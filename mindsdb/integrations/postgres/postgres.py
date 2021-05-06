from contextlib import closing
import pg8000

from mindsdb.utilities.subtypes import DATA_SUBTYPES
from mindsdb.integrations.base import Integration
from mindsdb.utilities.log import log


class PostgreSQLConnectionChecker:
    def __init__(self, **kwargs):
        self.host = kwargs.get('host')
        self.port = kwargs.get('port')
        self.user = kwargs.get('user')
        self.password = kwargs.get('password')
        self.database = kwargs.get('database', 'postgres')

    def _get_connection(self):
        return pg8000.connect(
            database=self.database,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )

    def check_connection(self):
        try:
            con = self._get_connection()
            with closing(con) as con:
                con.run('select 1;')
            connected = True
        except Exception:
            connected = False
        return connected


class PostgreSQL(Integration, PostgreSQLConnectionChecker):
    def __init__(self, config, name, db_info):
        super().__init__(config, name)
        self.user = db_info.get('user')
        self.password = db_info.get('password')
        self.host = db_info.get('host')
        self.port = db_info.get('port')
        self.database = db_info.get('database', 'postgres')

    def _to_postgres_table(self, stats, predicted_cols, columns):
        subtype_map = {
            DATA_SUBTYPES.INT: ' int8',
            DATA_SUBTYPES.FLOAT: 'float8',
            DATA_SUBTYPES.BINARY: 'bool',
            DATA_SUBTYPES.DATE: 'date',
            DATA_SUBTYPES.TIMESTAMP: 'timestamp',
            DATA_SUBTYPES.SINGLE: 'text',
            DATA_SUBTYPES.MULTIPLE: 'text',
            DATA_SUBTYPES.TAGS: 'text',
            DATA_SUBTYPES.IMAGE: 'text',
            DATA_SUBTYPES.VIDEO: 'text',
            DATA_SUBTYPES.AUDIO: 'text',
            DATA_SUBTYPES.SHORT: 'text',
            DATA_SUBTYPES.RICH: 'text',
            DATA_SUBTYPES.ARRAY: 'text'
        }

        column_declaration = []
        for name in columns:
            try:
                col_subtype = stats[name]['typing']['data_subtype']
                new_type = subtype_map[col_subtype]
                column_declaration.append(f' "{name}" {new_type} ')
                if name in predicted_cols:
                    column_declaration.append(f' "{name}_original" {new_type} ')
            except Exception as e:
                log.error(f'Error: can not determine postgres data type for column {name}: {e}')

        return column_declaration

    def _escape_table_name(self, name):
        return '"' + name.replace('"', '""') + '"'

    def _query(self, query):
        con = self._get_connection()
        with closing(con) as con:

            cur = con.cursor()
            res = True
            cur.execute(query)

            try:
                rows = cur.fetchall()
                keys = [k[0] if isinstance(k[0], str) else k[0].decode('ascii') for k in cur.description]
                res = [dict(zip(keys, row)) for row in rows]
            except Exception:
                pass

            con.commit()

        return res

    def setup(self):
        user = f"{self.config['api']['mysql']['user']}_{self.name}"
        password = self.config['api']['mysql']['password']
        host = self.config['api']['mysql']['host']
        port = self.config['api']['mysql']['port']

        try:
            self._query('''
                DO $$
                begin
                    if not exists (SELECT 1 FROM pg_extension where extname = 'mysql_fdw') then
                        CREATE EXTENSION mysql_fdw;
                    end if;
                END
                $$;
            ''')
        except Exception:
            print('Error: cant find or activate mysql_fdw extension for PostgreSQL.')

        self._query(f'DROP SCHEMA IF EXISTS {self.mindsdb_database} CASCADE')

        self._query(f"DROP USER MAPPING IF EXISTS FOR {self.user} SERVER server_{self.mindsdb_database}")

        self._query(f'DROP SERVER IF EXISTS server_{self.mindsdb_database} CASCADE')

        self._query(f'''
            CREATE SERVER server_{self.mindsdb_database}
                FOREIGN DATA WRAPPER mysql_fdw
                OPTIONS (host '{host}', port '{port}');
        ''')

        self._query(f'''
           CREATE USER MAPPING FOR {self.user}
                SERVER server_{self.mindsdb_database}
                OPTIONS (username '{user}', password '{password}');
        ''')

        self._query(f'CREATE SCHEMA {self.mindsdb_database}')

        q = f"""
            CREATE FOREIGN TABLE IF NOT EXISTS {self.mindsdb_database}.predictors (
                name text,
                status text,
                accuracy text,
                predict text,
                select_data_query text,
                external_datasource text,
                training_options text
            )
            SERVER server_{self.mindsdb_database}
            OPTIONS (dbname 'mindsdb', table_name 'predictors');
        """
        self._query(q)

        q = f"""
            CREATE FOREIGN TABLE IF NOT EXISTS {self.mindsdb_database}.commands (
                command text
            ) SERVER server_{self.mindsdb_database}
            OPTIONS (dbname 'mindsdb', table_name 'commands');
        """
        self._query(q)

    def register_predictors(self, model_data_arr):
        for model_meta in model_data_arr:
            name = model_meta['name']
            data_analysis_v2 = model_meta['data_analysis_v2']
            columns_sql = ','.join(self._to_postgres_table(data_analysis_v2, model_meta['predict'], model_meta['columns']))
            columns_sql += ',"select_data_query" text'
            columns_sql += ',"external_datasource" text'
            for col in model_meta['predict']:
                columns_sql += f',"{col}_confidence" float8'
                if data_analysis_v2[col]['typing']['data_type'] == 'Numeric':
                    columns_sql += f',"{col}_min" float8'
                    columns_sql += f',"{col}_max" float8'
                columns_sql += f',"{col}_explain" text'

            q = f"""
                CREATE FOREIGN TABLE {self.mindsdb_database}.{self._escape_table_name(name)} (
                    {columns_sql}
                ) SERVER server_{self.mindsdb_database}
                OPTIONS (dbname 'mindsdb', table_name '{name}');
            """
            self._query(q)

    def unregister_predictor(self, name):
        q = f"""
            DROP FOREIGN TABLE IF EXISTS {self.mindsdb_database}.{self._escape_table_name(name)};
        """
        self._query(q)
