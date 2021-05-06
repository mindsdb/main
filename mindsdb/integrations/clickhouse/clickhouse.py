import requests

from mindsdb.utilities.subtypes import DATA_SUBTYPES
from mindsdb.integrations.base import Integration
from mindsdb.utilities.log import log


class ClickhouseConnectionChecker:
    def __init__(self, **kwargs):
        self.host = kwargs.get("host")
        self.port = kwargs.get("port")
        self.user = kwargs.get("user")
        self.password = kwargs.get("password")

    def check_connection(self):
        try:
            res = requests.post(f"http://{self.host}:{self.port}",
                                data="select 1;",
                                params={'user': self.user, 'password': self.password})
            connected = res.status_code == 200
        except Exception:
            connected = False
        return connected


class Clickhouse(Integration, ClickhouseConnectionChecker):
    def __init__(self, config, name, db_info):
        super().__init__(config, name)
        self.user = db_info.get('user', 'default')
        self.password = db_info.get('password', None)
        self.host = db_info.get('host')
        self.port = db_info.get('port')

    def _to_clickhouse_table(self, stats, predicted_cols, columns):
        subtype_map = {
            DATA_SUBTYPES.INT: 'Nullable(Int64)',
            DATA_SUBTYPES.FLOAT: 'Nullable(Float64)',
            DATA_SUBTYPES.BINARY: 'Nullable(UInt8)',
            DATA_SUBTYPES.DATE: 'Nullable(Date)',
            DATA_SUBTYPES.TIMESTAMP: 'Nullable(Datetime)',
            DATA_SUBTYPES.SINGLE: 'Nullable(String)',
            DATA_SUBTYPES.MULTIPLE: 'Nullable(String)',
            DATA_SUBTYPES.TAGS: 'Nullable(String)',
            DATA_SUBTYPES.IMAGE: 'Nullable(String)',
            DATA_SUBTYPES.VIDEO: 'Nullable(String)',
            DATA_SUBTYPES.AUDIO: 'Nullable(String)',
            DATA_SUBTYPES.SHORT: 'Nullable(String)',
            DATA_SUBTYPES.RICH: 'Nullable(String)',
            DATA_SUBTYPES.ARRAY: 'Nullable(String)'
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
                log.error(f'Error: can not determine clickhouse data type for column {name}: {e}')

        return column_declaration

    def _query(self, query):
        params = {'user': self.user}

        if self.password is not None:
            params['password'] = self.password

        host = self.host
        port = self.port

        response = requests.post(f'http://{host}:{port}', data=query, params=params)

        if response.status_code != 200:
            raise Exception(f'Error: {response.content}\nQuery:{query}')

        return response

    def _get_mysql_user(self):
        return f"{self.config['api']['mysql']['user']}_{self.name}"

    def _escape_table_name(self, name):
        return '`' + name.replace('`', '\\`') + '`'

    def setup(self):
        self._query(f'DROP DATABASE IF EXISTS {self.mindsdb_database}')
        self._query(f'CREATE DATABASE IF NOT EXISTS {self.mindsdb_database}')

        msqyl_conn = self.config['api']['mysql']['host'] + ':' + str(self.config['api']['mysql']['port'])
        msqyl_pass = self.config['api']['mysql']['password']
        msqyl_user = self._get_mysql_user()

        q = f"""
            CREATE TABLE IF NOT EXISTS {self.mindsdb_database}.predictors (
                name String,
                status String,
                accuracy String,
                predict String,
                select_data_query String,
                external_datasource String,
                training_options String
                ) ENGINE=MySQL('{msqyl_conn}', 'mindsdb', 'predictors', '{msqyl_user}', '{msqyl_pass}')
        """
        self._query(q)
        q = f"""
            CREATE TABLE IF NOT EXISTS {self.mindsdb_database}.commands (
                command String
            ) ENGINE=MySQL('{msqyl_conn}', 'mindsdb', 'commands', '{msqyl_user}', '{msqyl_pass}')
        """
        self._query(q)

    def register_predictors(self, model_data_arr):
        for model_meta in model_data_arr:
            name = self._escape_table_name(model_meta['name'])

            columns_sql = ','.join(self._to_clickhouse_table(model_meta['data_analysis_v2'], model_meta['predict'], model_meta['columns']))
            columns_sql += ',`when_data` Nullable(String)'
            columns_sql += ',`select_data_query` Nullable(String)'
            columns_sql += ',`external_datasource` Nullable(String)'
            for col in model_meta['predict']:
                columns_sql += f',`{col}_confidence` Nullable(Float64)'

                if model_meta['data_analysis_v2'][col]['typing']['data_type'] == 'Numeric':
                    columns_sql += f',`{col}_min` Nullable(Float64)'
                    columns_sql += f',`{col}_max` Nullable(Float64)'
                columns_sql += f',`{col}_explain` Nullable(String)'

            msqyl_conn = self.config['api']['mysql']['host'] + ':' + str(self.config['api']['mysql']['port'])
            msqyl_pass = self.config['api']['mysql']['password']
            msqyl_user = self._get_mysql_user()

            q = f"""
                CREATE TABLE {self.mindsdb_database}.{name}
                ({columns_sql}
                ) ENGINE=MySQL('{msqyl_conn}', 'mindsdb', {name}, '{msqyl_user}', '{msqyl_pass}')
            """
            self._query(q)

    def unregister_predictor(self, name):
        q = f"""
            drop table if exists {self.mindsdb_database}.{self._escape_table_name(name)};
        """
        self._query(q)

    # def check_connection(self):
    #     try:
    #         res = requests.post(f"http://{self.host}:{self.port}",
    #                             data="select 1;",
    #                             params={'user': self.user, 'password': self.password})
    #         connected = res.status_code == 200
    #     except Exception:
    #         connected = False
    #     return connected
