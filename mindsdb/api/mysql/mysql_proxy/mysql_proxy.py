"""
*******************************************************
 * Copyright (C) 2017 MindsDB Inc. <copyright@mindsdb.com>
 *
 * This file is part of MindsDB Server.
 *
 * MindsDB Server can not be copied and/or distributed without the express
 * permission of MindsDB Inc
 *******************************************************
"""


import os
import sys
import socketserver as SocketServer
import ssl
import re
import traceback
import json
import atexit
import tempfile
import datetime
import socket
import struct
from collections import OrderedDict
from functools import partial
import select
import base64
import time

import moz_sql_parser as sql_parser

from mindsdb.utilities.wizards import make_ssl_cert

from mindsdb.api.mysql.mysql_proxy.data_types.mysql_packet import Packet
from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import SessionController
from mindsdb.api.mysql.mysql_proxy.datahub import init_datahub
from mindsdb.api.mysql.mysql_proxy.classes.client_capabilities import ClentCapabilities
from mindsdb.api.mysql.mysql_proxy.classes.server_capabilities import server_capabilities
from mindsdb.api.mysql.mysql_proxy.classes.sql_statement_parser import SqlStatementParser, SQL_PARAMETER, SQL_DEFAULT
from mindsdb.api.mysql.mysql_proxy.utilities import log
from mindsdb.api.mysql.mysql_proxy.external_libs.mysql_scramble import scramble as scramble_func

from mindsdb.api.mysql.mysql_proxy.classes.sql_query import (
    SQLQuery,
    NotImplementedError,
    SqlError
)

from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import (
    getConstName,
    CHARSET_NUMBERS,
    ERR,
    COMMANDS,
    TYPES,
    SERVER_VARIABLES,
    DEFAULT_AUTH_METHOD,
    SERVER_STATUS,
    FIELD_FLAG,
    CAPABILITIES
)

from mindsdb.api.mysql.mysql_proxy.data_types.mysql_packets import (
    ErrPacket,
    HandshakePacket,
    FastAuthFail,
    PasswordAnswer,
    HandshakeResponsePacket,
    OkPacket,
    SwitchOutPacket,
    SwitchOutResponse,
    CommandPacket,
    ColumnCountPacket,
    ColumnDefenitionPacket,
    ResultsetRowPacket,
    EofPacket,
    STMTPrepareHeaderPacket,
    BinaryResultsetRowPacket
)

from mindsdb.interfaces.datastore.datastore import DataStore
from mindsdb.interfaces.model.model_interface import ModelInterface as NativeInterface
from mindsdb.interfaces.custom.custom_models import CustomModels
from mindsdb.interfaces.ai_table.ai_table import AITable_store


connection_id = 0

default_store = None
mdb = None
custom_models = None
datahub = None
config = None
ai_table = None


def check_auth(username, password, scramble_func, salt, config):
    '''
    '''
    try:
        hardcoded_user = config['api']['mysql']['user']
        hardcoded_password = config['api']['mysql']['password']
        hardcoded_password_hash = scramble_func(hardcoded_password, salt)
        hardcoded_password = hardcoded_password.encode()
        integrations_names = config['integrations'].keys()

        if password is None:
            password = ''
        if isinstance(password, str):
            password = password.encode()

        integration = None
        integration_type = None
        extracted_username = username
        for integration_name in integrations_names:
            if username == f'{hardcoded_user}_{integration_name}':
                extracted_username = hardcoded_user
                integration = integration_name
                integration_type = config['integrations'][integration]['type']

        if extracted_username != hardcoded_user:
            log.warning(f'Check auth, user={username}: user mismatch')
            return {
                'success': False
            }

        if password != hardcoded_password and password != hardcoded_password_hash:
            log.warning(f'check auth, user={username}: password mismatch')
            return {
                'success': False
            }

        log.info(f'Check auth, user={username}: Ok')
        return {
            'success': True,
            'username': extracted_username,
            'integration': integration,
            'integration_type': integration_type
        }
    except Exception as e:
        log.error(f'Check auth, user={username}: ERROR')
        log.error(e)
        log.error(traceback.format_exc())


class MysqlProxy(SocketServer.BaseRequestHandler):
    """
    The Main Server controller class
    """

    charset = 'utf8'

    charset_text_type = CHARSET_NUMBERS['utf8_general_ci']

    session = None

    client_capabilities = None

    @staticmethod
    def server_close(srv):
        srv.server_close()

    def init_session(self, company_id=None):
        global connection_id
        log.debug('New connection [{ip}:{port}]'.format(
            ip=self.client_address[0], port=self.client_address[1]))
        log.debug(self.__dict__)

        connection_id += 1

        self.session = SessionController(company_id=company_id)

        if hasattr(self.server, 'salt') and isinstance(self.server.salt, str):
            self.salt = self.server.salt
        else:
            self.salt = base64.b64encode(os.urandom(15)).decode()

        self.socket = self.request
        self.count = 0  # next packet number
        self.connection_id = connection_id
        self.logging = log

        self.current_transaction = None

        log.debug('session salt: {salt}'.format(salt=self.salt))

    def handshake(self):
        def switch_auth(method='mysql_native_password'):
            self.packet(SwitchOutPacket, seed=self.salt, method=method).send()
            switch_out_answer = self.packet(SwitchOutResponse)
            switch_out_answer.get()
            password = switch_out_answer.password
            if method == 'mysql_native_password' and len(password) == 0:
                password = scramble_func('', self.salt)
            return password

        def get_fast_auth_password():
            log.debug('Asking for fast auth password')
            self.packet(FastAuthFail).send()
            password_answer = self.packet(PasswordAnswer)
            password_answer.get()
            try:
                password = password_answer.password.value.decode()
            except Exception:
                log.warning('error: no password in Fast Auth answer')
                self.packet(ErrPacket, err_code=ERR.ER_PASSWORD_NO_MATCH, msg='Is not password in connection query.').send()
                return None
            return password

        username = None
        password = None

        log.debug('send HandshakePacket')
        self.packet(HandshakePacket).send()

        handshake_resp = self.packet(HandshakeResponsePacket)
        handshake_resp.get()
        if handshake_resp.length == 0:
            log.warning('HandshakeResponsePacket empty')
            self.packet(OkPacket).send()
            return False
        self.client_capabilities = ClentCapabilities(handshake_resp.capabilities.value)

        client_auth_plugin = handshake_resp.client_auth_plugin.value.decode()

        self.session.is_ssl = False

        if handshake_resp.type == 'SSLRequest':
            log.debug('switch to SSL')
            self.session.is_ssl = True

            ssl_context = ssl.SSLContext()
            ssl_context.load_cert_chain(self.server.cert_path)
            ssl_socket = ssl_context.wrap_socket(
                self.socket,
                server_side=True,
                do_handshake_on_connect=True
            )

            self.socket = ssl_socket
            handshake_resp = self.packet(HandshakeResponsePacket)
            handshake_resp.get()
            client_auth_plugin = handshake_resp.client_auth_plugin.value.decode()

        username = handshake_resp.username.value.decode()

        if client_auth_plugin != DEFAULT_AUTH_METHOD:
            if client_auth_plugin == 'mysql_native_password':
                password = switch_auth('mysql_native_password')
            else:
                new_method = 'caching_sha2_password' if client_auth_plugin == 'caching_sha2_password' else 'mysql_native_password'

                if new_method == 'caching_sha2_password' and self.session.is_ssl is False:
                    log.warning(f'Check auth, user={username}, ssl={self.session.is_ssl}, auth_method={client_auth_plugin}: '
                                'error: cant switch to caching_sha2_password without SSL')
                    self.packet(ErrPacket, err_code=ERR.ER_PASSWORD_NO_MATCH, msg='caching_sha2_password without SSL not supported').send()
                    return False

                log.debug(f'Check auth, user={username}, ssl={self.session.is_ssl}, auth_method={client_auth_plugin}: '
                          f'switch auth method to {new_method}')
                password = switch_auth(new_method)

                if new_method == 'caching_sha2_password':
                    if password == b'\x00':
                        password = ''
                    else:
                        password = get_fast_auth_password()
        elif 'caching_sha2_password' in client_auth_plugin:
            log.debug(f'Check auth, user={username}, ssl={self.session.is_ssl}, auth_method={client_auth_plugin}: '
                     'check auth using caching_sha2_password')
            password = handshake_resp.enc_password.value
            if password == b'\x00':
                password = ''
            else:
                if self.session.is_ssl:
                    password = get_fast_auth_password()
                else:
                    password = switch_auth()
        elif 'mysql_native_password' in client_auth_plugin:
            log.debug(f'Check auth, user={username}, ssl={self.session.is_ssl}, auth_method={client_auth_plugin}: '
                      'check auth using mysql_native_password')
            password = handshake_resp.enc_password.value
        else:
            log.debug(f'Check auth, user={username}, ssl={self.session.is_ssl}, auth_method={client_auth_plugin}: '
                      'unknown method, possible ERROR. Try to switch to mysql_native_password')
            password = switch_auth('mysql_native_password')

        try:
            self.session.database = handshake_resp.database.value.decode()
        except Exception:
            self.session.database = None
        log.debug(f'Check auth, user={username}, ssl={self.session.is_ssl}, auth_method={client_auth_plugin}: '
                  f'connecting to database {self.session.database}')

        auth_data = self.server.check_auth(username, password, scramble_func, self.salt)
        if auth_data['success']:
            self.session.username = auth_data['username']
            self.session.auth = True
            self.session.integration = auth_data['integration']
            self.session.integration_type = auth_data['integration_type']
            self.packet(OkPacket).send()
            return True
        else:
            self.packet(ErrPacket, err_code=ERR.ER_PASSWORD_NO_MATCH, msg=f'Access denied for user {username}').send()
            log.warning(f'Access denied for user {username}')
            return False

    def sendPackageGroup(self, packages):
        string = b''.join([x.accum() for x in packages])
        self.socket.sendall(string)

    def answerVersionComment(self):
        packages = []
        packages += self.getTabelPackets(
            columns=[{
                'table_name': '',
                'name': '@@version_comment',
                'type': TYPES.MYSQL_TYPE_VAR_STRING
            }],
            data=[{'@@version_comment': '(MindsDB)'}]
        )
        if self.client_capabilities.DEPRECATE_EOF is True:
            packages.append(self.packet(OkPacket, eof=True))
        else:
            packages.append(self.packet(EofPacket))
        self.sendPackageGroup(packages)

    def answerVersion(self):
        packages = []
        packages += self.getTabelPackets(
            columns=[{
                'table_name': '',
                'name': '@@version',
                'type': TYPES.MYSQL_TYPE_VAR_STRING
            }],
            data=[{'@@version': '0.1'}]
        )
        packages.append(self.packet(OkPacket, eof=True))
        self.sendPackageGroup(packages)

    def answerTableQuery(self, query):
        packages = []
        packages += self.getTabelPackets(
            columns=query.columns,
            data=query.result
        )
        packages.append(self.packet(OkPacket, eof=True))
        self.sendPackageGroup(packages)

    def insert_predictor_answer(self, insert):
        ''' Start learn new predictor.
            Parameters:
             - insert - dict with keys as columns of mindsb.predictors table.
        '''
        global mdb, default_store, config, custom_models

        for key in insert.keys():
            if insert[key] is SQL_DEFAULT:
                insert[key] = None  # all default values at this moment is null (None)

        is_external_datasource = isinstance(insert.get('external_datasource'), str) and len(insert['external_datasource']) > 0
        is_select_data_query = isinstance(insert.get('select_data_query'), str) and len(insert['select_data_query']) > 0

        if is_external_datasource and is_select_data_query:
            self.packet(
                ErrPacket,
                err_code=ERR.ER_WRONG_ARGUMENTS,
                msg="'external_datasource' and 'select_data_query' should not be used in one query"
            ).send()
            return
        elif is_external_datasource is False and is_select_data_query is False:
            self.packet(
                ErrPacket,
                err_code=ERR.ER_WRONG_ARGUMENTS,
                msg="in query should be 'external_datasource' or 'select_data_query'"
            ).send()
            return

        models = mdb.get_models()
        if insert['name'] in [x['name'] for x in models]:
            self.packet(
                ErrPacket,
                err_code=ERR.ER_WRONG_ARGUMENTS,
                msg=f"predictor with name '{insert['name']}'' already exists"
            ).send()
            return

        kwargs = {}
        if isinstance(insert.get('training_options'), str) \
                and len(insert['training_options']) > 0:
            try:
                kwargs = json.loads(insert['training_options'])
            except Exception:
                self.packet(
                    ErrPacket,
                    err_code=ERR.ER_WRONG_ARGUMENTS,
                    msg='training_options should be in valid JSON string'
                ).send()
                return

        if is_select_data_query:
            integration = self.session.integration
            if isinstance(integration, str) is False or len(integration) == 0:
                self.packet(
                    ErrPacket,
                    err_code=ERR.ER_WRONG_ARGUMENTS,
                    msg='select_data_query can be used only in query from database'
                ).send()
                return
            insert['select_data_query'] = insert['select_data_query'].replace(r"\'", "'")
            ds, ds_name = default_store.save_datasource(insert['name'], integration, {'query': insert['select_data_query']})
        elif is_external_datasource:
            ds = default_store.get_datasource_obj(insert['external_datasource'], raw=True)
            ds_name = insert['external_datasource']

        insert['predict'] = [x.strip() for x in insert['predict'].split(',')]

        ds_data = default_store.get_datasource(ds_name)
        ds_columns = [x['name'] for x in ds_data['columns']]
        for col in insert['predict']:
            if col not in ds_columns:
                if is_select_data_query:
                    default_store.delete_datasource(ds_name)
                raise Exception(f"Column '{col}' not exists")

        if insert['name'] in [x['name'] for x in custom_models.get_models()]:
            custom_models.learn(insert['name'], ds, insert['predict'], ds_data['id'], kwargs)
        else:
            mdb.learn(insert['name'], ds, insert['predict'], ds_data['id'], kwargs)

        self.packet(OkPacket).send()

    def answer_create_ai_table(self, struct):
        global mdb, default_store, config, ai_table

        table = ai_table.get_ai_table(struct['ai_table_name'])
        if table is not None:
            raise Exception(f"AT Table with name {struct['ai_table_name']} already exists")

        # check predictor exists
        models = mdb.get_models()
        models_names = [x['name'] for x in models]
        if struct['predictor_name'] not in models_names:
            raise Exception(f"Predictor with name {struct['predictor_name']} not exists")

        # check integration exists
        if struct['integration_name'] not in config['integrations']:
            raise Exception(f"Integration with name {struct['integration_name']} not exists")

        ai_table.add(
            name=struct['ai_table_name'],
            integration_name=struct['integration_name'],
            integration_query=struct['integration_query'],
            query_fields=struct['query_fields'],
            predictor_name=struct['predictor_name'],
            predictor_fields=struct['predictor_fields']
        )

        self.packet(OkPacket).send()

    def answer_create_predictor(self, struct):
        global mdb, default_store, config

        if struct['integration_name'] not in config['integrations'].keys():
            # use first integration by default
            struct['integration_name'] = list(config['integrations'].keys())[0]

        is_temp_ds = False
        ds_name = struct.get('datasource_name')
        if ds_name is None:
            ds_name = f'temp_ds_{int(time.time()*100)}'
            is_temp_ds = True

        ds, ds_name = default_store.save_datasource(ds_name, struct['integration_name'], {'query': struct['select']})
        ds_data = default_store.get_datasource(ds_name)

        # TODO add alias here
        predict = [x['name'] for x in struct['predict']]

        timeseries_settings = {}
        for w in ['order_by', 'group_by', 'window']:
            if w in struct:
                timeseries_settings[w] = struct.get(w)

        kwargs = struct.get('using', {})
        if len(timeseries_settings) > 0:
            if 'timeseries_settings' not in kwargs:
                kwargs['timeseries_settings'] = timeseries_settings
            else:
                kwargs['timeseries_settings'].update(timeseries_settings)

        mdb.learn(struct['predictor_name'], ds, predict, ds_data['id'], kwargs)

        if is_temp_ds:
            default_store.delete_datasource(ds_name)

        self.packet(OkPacket).send()

    def delete_predictor_sql(self, sql):
        global datahub

        fake_sql = sql.strip(' ')
        fake_sql = 'select name ' + fake_sql[len('delete '):]
        query = SQLQuery(fake_sql, integration=self.session.integration, database=self.session.database)

        result = query.fetch(datahub)

        if result['success'] is False:
            self.packet(
                ErrPacket,
                err_code=result['error_code'],
                msg=result['msg']
            ).send()
            return

        predictors_names = [x[0] for x in result['result']]

        if len(predictors_names) == 0:
            raise NotImplementedError('nothing to delete')

        for predictor_name in predictors_names:
            datahub['mindsdb'].delete_predictor(predictor_name)

    def handle_custom_command(self, command):
        command = command.strip(' ;').split()

        if command[0].lower() == 'delete' and command[1].lower() == 'predictor':
            if len(command) != 3:
                self.packet(
                    ErrPacket,
                    err_code=ERR.ER_SYNTAX_ERROR,
                    msg="wrong syntax of 'DELETE PREDICTOR {NAME}' command"
                ).send()
                return
            predictor_name = command[2]
            self.delete_predictor_sql(f"delete from mindsdb.predictors where name = '{predictor_name}'")
            self.packet(OkPacket).send()
            return

        self.packet(
            ErrPacket,
            err_code=ERR.ER_SYNTAX_ERROR,
            msg="at this moment only 'delete predictor' command supported"
        ).send()

    def answer_stmt_prepare(self, statement):
        sql = statement.sql
        stmt_id = self.session.register_stmt(statement)
        prepared_stmt = self.session.prepared_stmts[stmt_id]

        if statement.keyword == 'insert':
            prepared_stmt['type'] = 'insert'

            struct = statement.struct
            if struct['table'] not in ['predictors', 'commands']:
                raise Exception("Only parametrized insert into 'predictors' or 'commands' supported at this moment")

            columns_str = ','.join([f'`{col}`' for col in struct['columns']])
            query = SQLQuery(
                f'select {columns_str} from mindsdb.{struct["table"]}',
                integration=self.session.integration,
                database=self.session.database
            )
            num_params = struct['values'].count(SQL_PARAMETER)
            num_columns = len(struct['values']) - num_params

            if num_columns != 0:
                raise Exception("At this moment supported only insert where all values is parameters.")

            columns_def = []
            for col in query.columns:
                columns_def.append(dict(
                    database='',
                    table_alias='',
                    table_name='',
                    alias='',
                    name='?',
                    type=TYPES.MYSQL_TYPE_VAR_STRING,
                    charset=CHARSET_NUMBERS['binary']
                ))
        elif statement.keyword == 'select' and statement.ends_with('for update'):
            # postgres when execute "delete from mindsdb.predictors where name = 'x'" sends for it prepare statement:
            # 'select name from mindsdb.predictors where name = 'x' FOR UPDATE;'
            # and after it send prepare for delete query.
            prepared_stmt['type'] = 'lock'
            statement.cut_from_tail('for update')
            query = SQLQuery(statement.sql, integration=self.session.integration, database=self.session.database)
            num_columns = len(query.columns)
            num_params = 0
            columns_def = query.columns
            for col in columns_def:
                col['charset'] = CHARSET_NUMBERS['utf8_general_ci']

        elif statement.keyword == 'delete':
            prepared_stmt['type'] = 'delete'

            fake_sql = sql.replace('?', '"?"')
            fake_sql = 'select name ' + fake_sql[len('delete '):]
            query = SQLQuery(fake_sql, integration=self.session.integration, database=self.session.database)
            num_columns = 0
            num_params = sql.count('?')
            columns_def = []
            for i in range(num_params):
                columns_def.append(dict(
                    database='',
                    table_alias='',
                    table_name='',
                    alias='?',
                    name='',
                    type=TYPES.MYSQL_TYPE_VAR_STRING,
                    charset=CHARSET_NUMBERS['utf8_general_ci'],
                    flags=sum([FIELD_FLAG.BINARY_COLLATION])
                ))

        elif statement.keyword == 'select':
            prepared_stmt['type'] = 'select'
            query = SQLQuery(sql, integration=self.session.integration, database=self.session.database)
            num_columns = len(query.columns)
            num_params = 0
            columns_def = query.columns
        else:
            raise SqlError(f"Only 'SELECT' and 'INSERT' statements supported. Got: {sql}")

        packages = [
            self.packet(
                STMTPrepareHeaderPacket,
                stmt_id=stmt_id,
                num_columns=num_columns,
                num_params=num_params
            )
        ]

        packages.extend(
            self._get_column_defenition_packets(columns_def)
        )

        if self.client_capabilities.DEPRECATE_EOF is False:
            status = sum([SERVER_STATUS.SERVER_STATUS_AUTOCOMMIT])
            packages.append(self.packet(EofPacket, status=status))

        self.sendPackageGroup(packages)

    def answer_stmt_execute(self, stmt_id, parameters):
        prepared_stmt = self.session.prepared_stmts[stmt_id]
        if prepared_stmt['type'] == 'select':
            sql = prepared_stmt['statement'].sql
            query = SQLQuery(sql, integration=self.session.integration, database=self.session.database)

            columns = query.columns
            packages = [self.packet(ColumnCountPacket, count=len(columns))]
            packages.extend(self._get_column_defenition_packets(columns))

            if self.client_capabilities.DEPRECATE_EOF is True:
                packages.append(self.packet(OkPacket, eof=True, status=0x0062))
            else:
                packages.append(self.packet(EofPacket, status=0x0062))
            self.sendPackageGroup(packages)
        elif prepared_stmt['type'] == 'insert':
            statement = prepared_stmt['statement']
            struct = statement.struct

            insert_dict = OrderedDict(zip(struct['columns'], struct['values']))
            if len(parameters) != len(insert_dict):
                raise SqlError(f"For INSERT statement got {len(parameters)} parameters, but should be {len(insert_dict)}")

            for i, col in enumerate(insert_dict.keys()):
                insert_dict[col] = parameters[i]

            sql = statement.sql
            table = struct['table'].lower()
            database = struct['database'].lower() if isinstance(struct['database'], str) else None
            if table == 'commands' \
                    and (database == 'mindsdb' or database is None and self.session.database == 'mindsdb'):
                if len(insert_dict) != 1 or 'command' not in insert_dict:
                    self.packet(ErrPacket, err_code=ERR.ER_WRONG_ARGUMENTS, msg="Error: only 'command' should be inserted in mindsdb.commands").send()
                    return
                self.handle_custom_command(insert_dict['command'])
            elif table == 'predictors' \
                    and (database == 'mindsdb' or database is None and self.session.database == 'mindsdb'):
                self.insert_predictor_answer(insert_dict)
            else:
                raise NotImplementedError("Only 'insert into predictors' and 'insert into commands' implemented")
        elif prepared_stmt['type'] == 'lock':
            sql = prepared_stmt['statement'].sql
            query = SQLQuery(sql, integration=self.session.integration, database=self.session.database)

            columns = query.columns
            packages = [self.packet(ColumnCountPacket, count=len(columns))]
            packages.extend(self._get_column_defenition_packets(columns))

            status = sum([
                SERVER_STATUS.SERVER_STATUS_AUTOCOMMIT,
                SERVER_STATUS.SERVER_STATUS_CURSOR_EXISTS,
            ])

            if self.client_capabilities.DEPRECATE_EOF is True:
                packages.append(self.packet(OkPacket, eof=True, status=status))
            else:
                packages.append(self.packet(EofPacket, status=status))
            self.sendPackageGroup(packages)
        elif prepared_stmt['type'] == 'delete':
            if len(parameters) == 0:
                raise SqlError("Delete statement must content 'where' filter")
            sql = prepared_stmt['statement'].sql
            sql = sql[:sql.find('?')] + f"'{parameters[0]}'"
            self.delete_predictor_sql(sql)
            self.packet(OkPacket, affected_rows=1).send()
        else:
            raise NotImplementedError(f"Unknown statement type: {prepared_stmt['type']}")

    def answer_stmt_fetch(self, stmt_id, limit=100000):
        global datahub
        prepared_stmt = self.session.prepared_stmts[stmt_id]
        sql = prepared_stmt['statement'].sql
        fetched = prepared_stmt['fetched']
        query = SQLQuery(sql, integration=self.session.integration, database=self.session.database)

        result = query.fetch(datahub)

        if result['success'] is False:
            self.packet(
                ErrPacket,
                err_code=result['error_code'],
                msg=result['msg']
            ).send()
            return

        packages = []
        columns = query.columns
        for row in query.result[fetched:limit]:
            packages.append(
                self.packet(BinaryResultsetRowPacket, data=row, columns=columns)
            )

        prepared_stmt['fetched'] += len(query.result[fetched:limit])

        if len(query.result) <= limit:
            status = sum([
                SERVER_STATUS.SERVER_STATUS_AUTOCOMMIT,
                SERVER_STATUS.SERVER_STATUS_LAST_ROW_SENT,
            ])
        else:
            status = sum([
                SERVER_STATUS.SERVER_STATUS_AUTOCOMMIT,
                SERVER_STATUS.SERVER_STATUS_CURSOR_EXISTS,
            ])
        packages.append(self.packet(EofPacket, status=status))  # what should be if CLIENT_DEPRECATE_EOF?

        self.sendPackageGroup(packages)

    def answer_stmt_close(self, stmt_id):
        self.session.unregister_stmt(stmt_id)

    def answer_explain_table(self, sql):
        parts = sql.split(' ')
        table = parts[1].lower()
        if table == 'predictors' or table == 'mindsdb.predictors':
            self.answer_explain_predictors()
        elif table == 'commands' or table == 'mindsdb.commands':
            self.answer_explain_commands()
        else:
            raise NotImplementedError("Only 'EXPLAIN predictors' and 'EXPLAIN commands' supported")

    def _get_explain_columns(self):
        return [{
            'database': 'information_schema',
            'table_name': 'COLUMNS',
            'name': 'COLUMN_NAME',
            'alias': 'Field',
            'type': TYPES.MYSQL_TYPE_VAR_STRING,
            'charset': self.charset_text_type,
            'flags': sum([FIELD_FLAG.NOT_NULL])
        }, {
            'database': 'information_schema',
            'table_name': 'COLUMNS',
            'name': 'COLUMN_TYPE',
            'alias': 'Type',
            'type': TYPES.MYSQL_TYPE_BLOB,
            'charset': self.charset_text_type,
            'flags': sum([
                FIELD_FLAG.NOT_NULL,
                FIELD_FLAG.BLOB
            ])
        }, {
            'database': 'information_schema',
            'table_name': 'COLUMNS',
            'name': 'IS_NULLABLE',
            'alias': 'Null',
            'type': TYPES.MYSQL_TYPE_VAR_STRING,
            'charset': self.charset_text_type,
            'flags': sum([FIELD_FLAG.NOT_NULL])
        }, {
            'database': 'information_schema',
            'table_name': 'COLUMNS',
            'name': 'COLUMN_KEY',
            'alias': 'Key',
            'type': TYPES.MYSQL_TYPE_VAR_STRING,
            'charset': self.charset_text_type,
            'flags': sum([FIELD_FLAG.NOT_NULL])
        }, {
            'database': 'information_schema',
            'table_name': 'COLUMNS',
            'name': 'COLUMN_DEFAULT',
            'alias': 'Default',
            'type': TYPES.MYSQL_TYPE_BLOB,
            'charset': self.charset_text_type,
            'flags': sum([FIELD_FLAG.BLOB])
        }, {
            'database': 'information_schema',
            'table_name': 'COLUMNS',
            'name': 'EXTRA',
            'alias': 'Extra',
            'type': TYPES.MYSQL_TYPE_VAR_STRING,
            'charset': self.charset_text_type,
            'flags': sum([FIELD_FLAG.NOT_NULL])
        }]

    def answer_explain_predictors(self):
        status = sum([
            SERVER_STATUS.SERVER_STATUS_AUTOCOMMIT,
            SERVER_STATUS.SERVER_QUERY_NO_INDEX_USED,
        ])

        packages = self.getTabelPackets(
            columns=self._get_explain_columns(),
            # [Field, Type, Null, Key, Default, Extra]
            data=[
                ['name', 'varchar(255)', 'NO', 'PRI', None, ''],
                ['status', 'varchar(255)', 'YES', '', None, ''],
                ['accuracy', 'varchar(255)', 'YES', '', None, ''],
                ['predict', 'varchar(255)', 'YES', '', None, ''],
                ['select_data_query', 'varchar(255)', 'YES', '', None, ''],
                ['external_datasource', 'varchar(255)', 'YES', '', None, ''],
                ['training_options', 'varchar(255)', 'YES', '', None, ''],
            ],
            status=status
        )

        if self.client_capabilities.DEPRECATE_EOF is False:
            packages.append(self.packet(EofPacket, status=status))
        else:
            packages.append(self.packet(OkPacket, eof=True, status=status))

        self.sendPackageGroup(packages)

    def answer_explain_commands(self):
        status = sum([
            SERVER_STATUS.SERVER_STATUS_AUTOCOMMIT,
            SERVER_STATUS.SERVER_QUERY_NO_INDEX_USED,
        ])

        packages = self.getTabelPackets(
            columns=self._get_explain_columns(),
            data=[
                # [Field, Type, Null, Key, Default, Extra]
                ['command', 'varchar(255)', 'NO', 'PRI', None, '']
            ],
            status=status
        )

        if self.client_capabilities.DEPRECATE_EOF is False:
            packages.append(self.packet(EofPacket, status=status))
        else:
            packages.append(self.packet(OkPacket, eof=True, status=status))

        self.sendPackageGroup(packages)

    def queryAnswer(self, sql):
        statement = SqlStatementParser(sql)

        sql = statement.sql
        sql_lower = sql.lower()
        sql_lower = sql_lower.replace('`', '')

        keyword = statement.keyword
        struct = statement.struct

        if keyword == 'show':
            if 'show databases' in sql_lower:
                sql = 'select schema_name as Database from information_schema.SCHEMATA;'
                statement = SqlStatementParser(sql)
                sql_lower = statement.sql.lower()
                keyword = statement.keyword
                struct = statement.struct
            elif 'tables' in sql_lower:
                if sql_lower == 'show tables':
                    schema = 'mindsdb'
                elif 'show tables from' in sql_lower:
                    schema = re.findall(r'show\s+tables\s+from\s+(\S*)', sql_lower)[0]
                elif 'show full tables from' in sql_lower:
                    schema = re.findall(r'show\s+full\s+tables\s+from\s+(\S*)', sql_lower)[0]
                sql = f"select table_name as Tables_in_{schema} from INFORMATION_SCHEMA.TABLES WHERE table_schema = '{schema.upper()}' and table_type = 'BASE TABLE'"
                statement = SqlStatementParser(sql)
                sql_lower = statement.sql.lower()
                keyword = statement.keyword
                struct = statement.struct

        if keyword == 'start':
            # start transaction
            self.packet(OkPacket).send()
        elif keyword == 'set':
            if 'autocommit' in sql_lower:
                self.packet(OkPacket).send()
            elif 'set names' in sql_lower:
                # it can be "set names utf8"
                self.charset = re.findall(r"set\s+names\s+(\S*)", sql_lower)[0]
                self.charset_text_type = CHARSET_NUMBERS['utf8_general_ci']
                if self.charset == 'utf8mb4':
                    self.charset_text_type = CHARSET_NUMBERS['utf8mb4_general_ci']
                self.packet(
                    OkPacket,
                    state_track=[
                        ['character_set_client', self.charset],
                        ['character_set_connection', self.charset],
                        ['character_set_results', self.charset]
                    ]
                ).send()
            else:
                self.packet(OkPacket).send()
        elif keyword == 'use':
            self.session.database = sql_lower.split()[1].strip(' ;')
            self.packet(OkPacket).send()
        elif keyword == 'create_ai_table':
            self.answer_create_ai_table(struct)
        elif keyword == 'create_predictor':
            self.answer_create_predictor(struct)
        elif 'show warnings' in sql_lower:
            self.answerShowWarnings()
        elif 'show engines' in sql_lower:
            self.answerShowEngines()
        elif 'show charset' in sql_lower:
            self.answerShowCharset()
        elif 'show collation' in sql_lower:
            self.answerShowCollation()
        elif 'show table status' in sql_lower:
            self.answer_show_table_status(sql)
        elif keyword == 'delete' and \
                ('mindsdb.predictors' in sql_lower or self.session.database == 'mindsdb' and 'predictors' in sql_lower):
            self.delete_predictor_sql(sql)
            self.packet(OkPacket).send()
        elif keyword == 'insert' \
                and (struct['database'] == 'mindsdb' or struct['database'] is None and self.session.database == 'mindsdb') \
                and struct['table'] == 'commands':
            insert = OrderedDict(zip(struct['columns'], struct['values']))
            if len(insert) != 1 or 'command' not in insert:
                self.packet(ErrPacket, err_code=ERR.ER_WRONG_ARGUMENTS, msg="Error: only 'command' should be inserted in mindsdb.commands").send()
                return
            self.handle_custom_command(insert['command'])
        elif keyword == 'insert' \
                and (struct['database'] == 'mindsdb' or struct['database'] is None and self.session.database == 'mindsdb') \
                and struct['table'] == 'predictors':
            if len(struct['columns']) != len(struct['values']):
                # All clients what i saw convert queries from: "insert into a values (b)" to "insert into a (colb) values (b)"
                # If once it no happened, then this error will raise, and will need to add columns list definition.
                self.packet(
                    ErrPacket,
                    err_code=ERR.ER_WRONG_ARGUMENTS,
                    msg="Error: number of columns is not equal to number of inserted values."
                ).send()
                return
            insert_dict = OrderedDict(zip(struct['columns'], struct['values']))
            self.insert_predictor_answer(insert_dict)
        elif keyword in ('update', 'insert'):
            raise NotImplementedError('Update and Insert not implemented')
        elif keyword == 'alter' and ('disable keys' in sql_lower) or ('enable keys' in sql_lower):
            self.packet(OkPacket).send()
        elif keyword == 'select':
            if 'connection_id()' in sql_lower:
                self.answer_connection_id(sql)
                return
            if '@@' in sql_lower:
                self.answerVariables(sql)
                return
            if 'select 1' in sql_lower:
                self.answerSelect1(sql)
                return
            if 'database()' in sql_lower:
                self.answerSelectDatabase()
                return
            query = SQLQuery(sql, integration=self.session.integration, database=self.session.database)
            self.selectAnswer(query)
        elif keyword == 'rollback':
            self.packet(OkPacket).send()
        elif keyword == 'commit':
            self.packet(OkPacket).send()
        elif keyword == 'explain':
            self.answer_explain_table(sql)
        else:
            raise NotImplementedError('Action not implemented')

    def answer_show_table_status(self, sql):
        # NOTE at this moment parsed statement only like `SHOW TABLE STATUS LIKE 'table'`.
        # NOTE some columns has {'database': 'mysql'}, other not. That correct. This is how real DB sends messages.
        parts = sql.split(' ')
        if parts[3].lower() != 'like':
            raise NotImplementedError('Action not implemented')
        table = parts[4].strip("'")

        packages = []
        packages += self.getTabelPackets(
            columns=[{
                'database': 'mysql',
                'table_name': 'tables',
                'name': 'Name',
                'alias': 'Name',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': '',
                'table_name': 'tables',
                'name': 'Engine',
                'alias': 'Engine',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': '',
                'table_name': 'tables',
                'name': 'Version',
                'alias': 'Version',
                'type': TYPES.MYSQL_TYPE_LONGLONG,
                'charset': CHARSET_NUMBERS['binary']
            }, {
                'database': 'mysql',
                'table_name': 'tables',
                'name': 'Row_format',
                'alias': 'Row_format',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': '',
                'table_name': 'tables',
                'name': 'Rows',
                'alias': 'Rows',
                'type': TYPES.MYSQL_TYPE_LONGLONG,
                'charset': CHARSET_NUMBERS['binary']
            }, {
                'database': '',
                'table_name': 'tables',
                'name': 'Avg_row_length',
                'alias': 'Avg_row_length',
                'type': TYPES.MYSQL_TYPE_LONGLONG,
                'charset': CHARSET_NUMBERS['binary']
            }, {
                'database': '',
                'table_name': 'tables',
                'name': 'Data_length',
                'alias': 'Data_length',
                'type': TYPES.MYSQL_TYPE_LONGLONG,
                'charset': CHARSET_NUMBERS['binary']
            }, {
                'database': '',
                'table_name': 'tables',
                'name': 'Max_data_length',
                'alias': 'Max_data_length',
                'type': TYPES.MYSQL_TYPE_LONGLONG,
                'charset': CHARSET_NUMBERS['binary']
            }, {
                'database': '',
                'table_name': 'tables',
                'name': 'Index_length',
                'alias': 'Index_length',
                'type': TYPES.MYSQL_TYPE_LONGLONG,
                'charset': CHARSET_NUMBERS['binary']
            }, {
                'database': '',
                'table_name': 'tables',
                'name': 'Data_free',
                'alias': 'Data_free',
                'type': TYPES.MYSQL_TYPE_LONGLONG,
                'charset': CHARSET_NUMBERS['binary']
            }, {
                'database': '',
                'table_name': 'tables',
                'name': 'Auto_increment',
                'alias': 'Auto_increment',
                'type': TYPES.MYSQL_TYPE_LONGLONG,
                'charset': CHARSET_NUMBERS['binary']
            }, {
                'database': '',
                'table_name': 'tables',
                'name': 'Create_time',
                'alias': 'Create_time',
                'type': TYPES.MYSQL_TYPE_TIMESTAMP,
                'charset': CHARSET_NUMBERS['binary']
            }, {
                'database': '',
                'table_name': 'tables',
                'name': 'Update_time',
                'alias': 'Update_time',
                'type': TYPES.MYSQL_TYPE_TIMESTAMP,
                'charset': CHARSET_NUMBERS['binary']
            }, {
                'database': '',
                'table_name': 'tables',
                'name': 'Check_time',
                'alias': 'Check_time',
                'type': TYPES.MYSQL_TYPE_TIMESTAMP,
                'charset': CHARSET_NUMBERS['binary']
            }, {
                'database': 'mysql',
                'table_name': 'tables',
                'name': 'Collation',
                'alias': 'Collation',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': '',
                'table_name': 'tables',
                'name': 'Checksum',
                'alias': 'Checksum',
                'type': TYPES.MYSQL_TYPE_LONGLONG,
                'charset': CHARSET_NUMBERS['binary']
            }, {
                'database': '',
                'table_name': 'tables',
                'name': 'Create_options',
                'alias': 'Create_options',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': '',
                'table_name': 'tables',
                'name': 'Comment',
                'alias': 'Comment',
                'type': TYPES.MYSQL_TYPE_BLOB,
                'charset': self.charset_text_type
            }],
            data=[[
                table,          # Name
                'InnoDB',       # Engine
                10,             # Version
                'Dynamic',      # Row_format
                1,              # Rows
                16384,          # Avg_row_length
                16384,          # Data_length
                0,              # Max_data_length
                0,              # Index_length
                0,              # Data_free
                None,           # Auto_increment
                datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),  # Create_time
                datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),  # Update_time
                None,           # Check_time
                'utf8mb4_0900_ai_ci',   # Collation
                None,           # Checksum
                '',             # Create_options
                ''              # Comment
            ]]
        )
        packages.append(self.packet(OkPacket, eof=True, status=0x0002))
        self.sendPackageGroup(packages)

    def answerShowWarnings(self):
        packages = []
        packages += self.getTabelPackets(
            columns=[{
                'database': '',
                'table_name': '',
                'name': 'Level',
                'alias': 'Level',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': '',
                'table_name': '',
                'name': 'Code',
                'alias': 'Code',
                'type': TYPES.MYSQL_TYPE_LONG,
                'charset': CHARSET_NUMBERS['binary']
            }, {
                'database': '',
                'table_name': '',
                'name': 'Message',
                'alias': 'Message',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }],
            data=[]
        )
        packages.append(self.packet(OkPacket, eof=True, status=0x0002))
        self.sendPackageGroup(packages)

    def answerSelectDatabase(self):
        packages = []
        packages += self.getTabelPackets(
            columns=[{
                'database': '',
                'table_name': '',
                'name': 'database()',
                'alias': 'database()',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }],
            data=[
                [None]
            ]
        )
        packages.append(self.packet(OkPacket, eof=True, status=0x0000))
        self.sendPackageGroup(packages)

    def answerShowCollation(self):
        packages = []
        packages += self.getTabelPackets(
            columns=[{
                'database': 'information_schema',
                'table_name': 'COLLATIONS',
                'name': 'COLLATION_NAME',
                'alias': 'Collation',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': 'information_schema',
                'table_name': 'COLLATIONS',
                'name': 'CHARACTER_SET_NAME',
                'alias': 'Charset',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': 'information_schema',
                'table_name': 'COLLATIONS',
                'name': 'ID',
                'alias': 'Id',
                'type': TYPES.MYSQL_TYPE_LONGLONG,
                'charset': CHARSET_NUMBERS['binary']
            }, {
                'database': 'information_schema',
                'table_name': 'COLLATIONS',
                'name': 'IS_DEFAULT',
                'alias': 'Default',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': 'information_schema',
                'table_name': 'COLLATIONS',
                'name': 'IS_COMPILED',
                'alias': 'Compiled',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': 'information_schema',
                'table_name': 'COLLATIONS',
                'name': 'SORTLEN',
                'alias': 'Sortlen',
                'type': TYPES.MYSQL_TYPE_LONGLONG,
                'charset': CHARSET_NUMBERS['binary']
            }],
            data=[
                ['utf8_general_ci', 'utf8', 33, 'Yes', 'Yes', 1],
                ['latin1_swedish_ci', 'latin1', 8, 'Yes', 'Yes', 1]
            ]
        )
        packages.append(self.packet(OkPacket, eof=True, status=0x0002))
        self.sendPackageGroup(packages)

    def answerShowCharset(self):
        packages = []
        packages += self.getTabelPackets(
            columns=[{
                'database': 'information_schema',
                'table_name': 'CHARACTER_SETS',
                'name': 'CHARACTER_SET_NAME',
                'alias': 'Charset',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': 'information_schema',
                'table_name': 'CHARACTER_SETS',
                'name': 'DESCRIPTION',
                'alias': 'Description',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': 'information_schema',
                'table_name': 'CHARACTER_SETS',
                'name': 'DEFAULT_COLLATE_NAME',
                'alias': 'Default collation',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': 'information_schema',
                'table_name': 'CHARACTER_SETS',
                'name': 'MAXLEN',
                'alias': 'Maxlen',
                'type': TYPES.MYSQL_TYPE_LONGLONG,
                'charset': CHARSET_NUMBERS['binary']
            }],
            data=[
                ['utf8', 'UTF-8 Unicode', 'utf8_general_ci', 3],
                ['latin1', 'cp1252 West European', 'latin1_swedish_ci', 1]
            ]
        )
        packages.append(self.packet(OkPacket, eof=True, status=0x0002))
        self.sendPackageGroup(packages)

    def answerShowEngines(self):
        packages = []
        packages += self.getTabelPackets(
            columns=[{
                'database': 'information_schema',
                'table_name': 'ENGINES',
                'name': 'Engine',
                'alias': 'Engine',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': 'information_schema',
                'table_name': 'ENGINES',
                'name': 'Support',
                'alias': 'Support',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': 'information_schema',
                'table_name': 'ENGINES',
                'name': 'Comment',
                'alias': 'Comment',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': 'information_schema',
                'table_name': 'ENGINES',
                'name': 'Transactions',
                'alias': 'Transactions',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': 'information_schema',
                'table_name': 'ENGINES',
                'name': 'XA',
                'alias': 'XA',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }, {
                'database': 'information_schema',
                'table_name': 'ENGINES',
                'name': 'Savepoints',
                'alias': 'Savepoints',
                'type': TYPES.MYSQL_TYPE_VAR_STRING,
                'charset': self.charset_text_type
            }],
            data=[['InnoDB', 'DEFAULT', 'Supports transactions, row-level locking, and foreign keys', 'YES', 'YES', 'YES']]
        )
        packages.append(self.packet(OkPacket, eof=True, status=0x0002))
        self.sendPackageGroup(packages)

    def answerSelect1(self, sql):
        packages = []
        packages += self.getTabelPackets(
            columns=[{
                'table_name': '',
                'name': '',
                'alias': '1',
                'type': TYPES.MYSQL_TYPE_LONGLONG,
                'charset': CHARSET_NUMBERS['binary']
            }],
            data=[[1]]
        )
        packages.append(self.packet(OkPacket, eof=True, status=0x0002))
        self.sendPackageGroup(packages)

    def answer_connection_id(self, sql):
        packages = self.getTabelPackets(
            columns=[{
                'database': '',
                'table_name': '',
                'name': 'conn_id',
                'alias': 'conn_id',
                'type': TYPES.MYSQL_TYPE_LONG,
                'charset': CHARSET_NUMBERS['binary']
            }],
            data=[[96]]     # can be any UNIQUE number
        )
        packages.append(self.packet(OkPacket, eof=True, status=0x0002))
        self.sendPackageGroup(packages)

    def answerVariables(self, sql):
        if '@@version_comment' in sql.lower():
            self.answerVersionComment()
            return
        elif '@@version' in sql.lower():
            self.answerVersion()
            return
        p = sql_parser.parse(sql)
        select = p.get('select')
        if isinstance(select, dict):
            select = [select]
        columns = []
        row = []
        for s in select:
            fill_name = s.get('value')
            alias = s.get('name', fill_name)
            variable = SERVER_VARIABLES.get(fill_name)
            columns.append({
                'table_name': '',
                'name': '',
                'alias': alias,
                'type': variable[1],
                'charset': variable[2]
            })
            row.append(variable[0])

        packages = []
        packages += self.getTabelPackets(
            columns=columns,
            data=[row]
        )
        packages.append(self.packet(OkPacket, eof=True, status=0x0002))
        self.sendPackageGroup(packages)

    def selectAnswer(self, query):
        global datahub
        result = query.fetch(datahub)

        if result['success'] is False:
            self.packet(
                ErrPacket,
                err_code=result['error_code'],
                msg=result['msg']
            ).send()
            return

        self.answerTableQuery(query)

    def _get_column_defenition_packets(self, columns, data=[]):
        packets = []
        for i, column in enumerate(columns):
            table_name = column.get('table_name', 'table_name')
            column_name = column.get('name', 'column_name')
            column_alias = column.get('alias', column_name)
            flags = column.get('flags', 0)
            if self.session.integration_type == 'mssql':
                # mssql raise error if value more then this.
                length = 0x2000
            else:
                if len(data) == 0:
                    length = 0xffff
                else:
                    length = 1
                    for row in data:
                        if isinstance(row, dict):
                            length = max(len(str(row[column_alias])), length)
                        else:
                            length = max(len(str(row[i])), length)

            packets.append(
                self.packet(
                    ColumnDefenitionPacket,
                    schema=column.get('database', 'mindsdb_schema'),
                    table_alias=column.get('table_alias', table_name),
                    table_name=table_name,
                    column_alias=column_alias,
                    column_name=column_name,
                    column_type=column['type'],
                    charset=column.get('charset', CHARSET_NUMBERS["utf8_unicode_ci"]),
                    max_length=length,
                    flags=flags
                )
            )
        return packets

    def getTabelPackets(self, columns, data, status=0):
        # TODO remove columns order
        packets = [self.packet(ColumnCountPacket, count=len(columns))]
        packets.extend(self._get_column_defenition_packets(columns, data))

        if self.client_capabilities.DEPRECATE_EOF is False:
            packets.append(self.packet(EofPacket, status=status))

        packets += [self.packet(ResultsetRowPacket, data=x) for x in data]
        return packets

    def decode_utf(self, text):
        try:
            return text.decode('utf-8')
        except Exception as e:
            log.error(f'SQL contains non utf-8 values: {text}')
            self.packet(
                ErrPacket,
                err_code=ERR.ER_SYNTAX_ERROR,
                msg=str(e)
            ).send()
            raise

    def is_cloud_connection(self):
        ''' Determine source of connection. Must be call before handshake.
            Idea based on: real mysql connection does not send anything before server handshake, so
            soket should be in 'out' state. In opposite, clout connection sends '0000' right after
            connection. '0000' selected because in real mysql connection it should be lenght of package,
            and it can not be 0.
        '''
        if sys.platform != 'linux':
            return {
                'is_cloud': False
            }

        read_poller = select.poll()
        read_poller.register(self.request, select.POLLIN)
        events = read_poller.poll(0)

        if len(events) == 0:
            return {
                'is_cloud': False
            }

        first_byte = self.request.recv(4, socket.MSG_PEEK)
        if first_byte == b'\x00\x00\x00\x00':
            self.request.recv(4)
            client_capabilities = self.request.recv(8)
            client_capabilities = struct.unpack('L', client_capabilities)[0]

            company_id = self.request.recv(4)
            company_id = struct.unpack('I', company_id)[0]

            return {
                'is_cloud': True,
                'client_capabilities': client_capabilities,
                'company_id': company_id
            }

        return {
            'is_cloud': False
        }

    def handle(self):
        """
        Handle new incoming connections
        :return:
        """
        log.debug('handle new incoming connection')
        cloud_connection = self.is_cloud_connection()
        self.init_session(company_id=cloud_connection.get('company_id'))
        if cloud_connection['is_cloud'] is False:
            if self.handshake() is False:
                return
        else:
            self.client_capabilities = ClentCapabilities(cloud_connection['client_capabilities'])
            self.session.username = 'cloud'
            self.session.auth = True
            self.session.integration = None
            self.session.integration_type = None

        while True:
            log.debug('Got a new packet')
            p = self.packet(CommandPacket)

            try:
                success = p.get()
            except Exception:
                log.error('Session closed, on packet read error')
                log.error(traceback.format_exc())
                return

            if success is False:
                log.debug('Session closed by client')
                return

            log.debug('Command TYPE: {type}'.format(
                type=getConstName(COMMANDS, p.type.value)))

            try:
                if p.type.value == COMMANDS.COM_QUERY:
                    sql = self.decode_utf(p.sql.value)
                    sql = SqlStatementParser(sql).sql
                    log.debug(f'COM_QUERY: {sql}')
                    self.queryAnswer(sql)
                elif p.type.value == COMMANDS.COM_STMT_PREPARE:
                    # https://dev.mysql.com/doc/internals/en/com-stmt-prepare.html
                    sql = self.decode_utf(p.sql.value)
                    statement = SqlStatementParser(sql)
                    log.debug(f'COM_STMT_PREPARE: {statement.sql}')
                    self.answer_stmt_prepare(statement)
                elif p.type.value == COMMANDS.COM_STMT_EXECUTE:
                    self.answer_stmt_execute(p.stmt_id.value, p.parameters)
                elif p.type.value == COMMANDS.COM_STMT_FETCH:
                    self.answer_stmt_fetch(p.stmt_id.value, p.limit.value)
                elif p.type.value == COMMANDS.COM_STMT_CLOSE:
                    self.answer_stmt_close(p.stmt_id.value)
                elif p.type.value == COMMANDS.COM_QUIT:
                    log.debug('Session closed, on client disconnect')
                    self.session = None
                    break
                elif p.type.value == COMMANDS.COM_INIT_DB:
                    new_database = p.database.value.decode()
                    # That fix for bug in mssql: it keeps connection for a long time, but after some time mssql can
                    # send packet with COM_INIT_DB=null. In this case keep old database name as default.
                    if new_database != 'null':
                        self.session.database = new_database
                    self.packet(OkPacket).send()
                else:
                    log.warning('Command has no specific handler, return OK msg')
                    log.debug(str(p))
                    # p.pprintPacket() TODO: Make a version of print packet
                    # that sends it to debug isntead
                    self.packet(OkPacket).send()

            except Exception as e:
                log.error(
                    f'ERROR while executing query\n'
                    f'{traceback.format_exc()}\n'
                    f'{e}'
                )
                self.packet(
                    ErrPacket,
                    err_code=ERR.ER_SYNTAX_ERROR,
                    msg=str(e)
                ).send()

    def packet(self, packetClass=Packet, **kwargs):
        """
        Factory method for packets

        :param packetClass:
        :param kwargs:
        :return:
        """
        p = packetClass(socket=self.socket, seq=self.count, session=self.session, proxy=self, **kwargs)
        self.count = (self.count + 1) % 256
        return p

    @staticmethod
    def startProxy(_config):
        global default_store
        global mdb
        global datahub
        global config
        global custom_models
        global ai_table
        """
        Create a server and wait for incoming connections until Ctrl-C
        """
        config = _config

        cert_path = config['api']['mysql'].get('certificate_path')
        if cert_path is None or cert_path == '':
            cert_path = tempfile.mkstemp(prefix='mindsdb_cert_', text=True)[1]
            make_ssl_cert(cert_path)
            atexit.register(lambda: os.remove(cert_path))

        server_capabilities.set(
            CAPABILITIES.CLIENT_SSL,
            config['api']['mysql']['ssl']
        )

        ai_table = AITable_store()
        default_store = DataStore()
        mdb = NativeInterface()
        custom_models = CustomModels()
        datahub = init_datahub(config)

        host = config['api']['mysql']['host']
        port = int(config['api']['mysql']['port'])

        log.info(f'Starting MindsDB Mysql proxy server on tcp://{host}:{port}')

        SocketServer.TCPServer.allow_reuse_address = True
        server = SocketServer.ThreadingTCPServer((host, port), MysqlProxy)
        server.mindsdb_config = config
        server.check_auth = partial(check_auth, config=config)
        server.cert_path = cert_path

        atexit.register(MysqlProxy.server_close, srv=server)

        # Activate the server; this will keep running until you
        # interrupt the program with Ctrl-C
        log.info('Waiting for incoming connections...')
        server.serve_forever()
