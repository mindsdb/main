from contextlib import closing
import mysql.connector

from mindsdb.utilities.subtypes import DATA_SUBTYPES
from mindsdb.integrations.base import Integration
from mindsdb.utilities.log import log


class MySQLConnectionChecker:
    def __init__(self, **kwargs):
        self.host = kwargs.get('host')
        self.port = kwargs.get('port')
        self.user = kwargs.get('user')
        self.password = kwargs.get('password')

    def _get_connnection(self):
        return mysql.connector.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password
        )

    def check_connection(self):
        try:
            con = self._get_connnection()
            with closing(con) as con:
                connected = con.is_connected()
        except Exception:
            connected = False
        return connected


class MySQL(Integration, MySQLConnectionChecker):
    def __init__(self, config, name, db_info):
        super().__init__(config, name)
        self.user = db_info.get('user')
        self.password = db_info.get('password')
        self.host = db_info.get('host')
        self.port = db_info.get('port')

    def _to_mysql_table(self, stats, predicted_cols, columns):
        subtype_map = {
            DATA_SUBTYPES.INT: 'int',
            DATA_SUBTYPES.FLOAT: 'double',
            DATA_SUBTYPES.BINARY: 'bool',
            DATA_SUBTYPES.DATE: 'Date',
            DATA_SUBTYPES.TIMESTAMP: 'Datetime',
            DATA_SUBTYPES.SINGLE: 'VARCHAR(500)',
            DATA_SUBTYPES.MULTIPLE: 'VARCHAR(500)',
            DATA_SUBTYPES.TAGS: 'VARCHAR(500)',
            DATA_SUBTYPES.IMAGE: 'VARCHAR(500)',
            DATA_SUBTYPES.VIDEO: 'VARCHAR(500)',
            DATA_SUBTYPES.AUDIO: 'VARCHAR(500)',
            DATA_SUBTYPES.SHORT: 'VARCHAR(500)',
            DATA_SUBTYPES.RICH: 'VARCHAR(500)',
            DATA_SUBTYPES.ARRAY: 'VARCHAR(500)'
        }

        column_declaration = []
        for name in columns:
            try:
                col_subtype = stats[name]['typing']['data_subtype']
                new_type = subtype_map[col_subtype]
                column_declaration.append(f' `{name}` {new_type} ')
                if name in predicted_cols:
                    column_declaration.append(f' `{name}_original` {new_type} ')
            except Exception as e:
                log.error(f'Error: can not determine mysql data type for column {name}: {e}')

        return column_declaration

    def _escape_table_name(self, name):
        return '`' + name.replace('`', '``') + '`'

    def _query(self, query):
        con = self._get_connnection()
        with closing(con) as con:
            cur = con.cursor(dictionary=True, buffered=True)
            cur.execute(query)
            res = True
            try:
                res = cur.fetchall()
            except Exception:
                pass
            con.commit()

        return res

    def _get_connect_string(self, table):
        user = f"{self.config['api']['mysql']['user']}_{self.name}"
        password = self.config['api']['mysql']['password']
        host = self.config['api']['mysql']['host']
        port = self.config['api']['mysql']['port']

        if password is None or password == '':
            connect = f'mysql://{user}@{host}:{port}/mindsdb/{table}'
        else:
            connect = f'mysql://{user}:{password}@{host}:{port}/mindsdb/{table}'

        return connect

    def setup(self):
        self._query(f'DROP DATABASE IF EXISTS {self.mindsdb_database}')
        self._query(f'CREATE DATABASE IF NOT EXISTS {self.mindsdb_database}')

        connect = self._get_connect_string('predictors')

        q = f"""
            CREATE TABLE IF NOT EXISTS {self.mindsdb_database}.predictors (
                name VARCHAR(500),
                status VARCHAR(500),
                accuracy VARCHAR(500),
                predict VARCHAR(500),
                select_data_query VARCHAR(500),
                external_datasource VARCHAR(500),
                training_options VARCHAR(500),
                key name_key (name)
            ) ENGINE=FEDERATED CHARSET=utf8 CONNECTION='{connect}';
        """
        self._query(q)

        connect = self._get_connect_string('commands')

        q = f"""
            CREATE TABLE IF NOT EXISTS {self.mindsdb_database}.commands (
                command VARCHAR(500),
                key command_key (command)
            ) ENGINE=FEDERATED CHARSET=utf8 CONNECTION='{connect}';
        """
        self._query(q)

    def register_predictors(self, model_data_arr):
        for model_meta in model_data_arr:
            name = model_meta['name']
            columns_sql = ','.join(self._to_mysql_table(model_meta['data_analysis_v2'], model_meta['predict'], model_meta['columns']))
            columns_sql += ',`when_data` varchar(500)'
            columns_sql += ',`select_data_query` varchar(500)'
            columns_sql += ',`external_datasource` varchar(500)'
            for col in model_meta['predict']:
                columns_sql += f',`{col}_confidence` double'
                if model_meta['data_analysis_v2'][col]['typing']['data_type'] == 'Numeric':
                    columns_sql += f',`{col}_min` double'
                    columns_sql += f',`{col}_max` double'
                columns_sql += f',`{col}_explain` varchar(500)'

            connect = self._get_connect_string(name)

            q = f"""
                CREATE TABLE {self.mindsdb_database}.{self._escape_table_name(name)} (
                    {columns_sql},
                    index when_data_index (when_data),
                    index select_data_query_index (select_data_query),
                    index external_datasource_index (external_datasource)
                ) ENGINE=FEDERATED CHARSET=utf8 CONNECTION='{connect}';
            """
            self._query(q)

    def unregister_predictor(self, name):
        q = f"""
            drop table if exists {self.mindsdb_database}.{self._escape_table_name(name)};
        """
        self._query(q)
