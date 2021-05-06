import pytds
from mindsdb.integrations.base import Integration


class MSSQLConnectionChecker:
    def __init__(self, **kwargs):
        self.host = kwargs.get('host')
        self.port = kwargs.get('port')
        self.user = kwargs.get('user')
        self.password = kwargs.get('password')

    def _get_connnection(self):
        return pytds.connect(
            user=self.user,
            password=self.password,
            dsn=self.host,
            port=self.port,
            as_dict=True,
            autocommit=True  # .commit() doesn't work
        )

    def check_connection(self):
        try:
            conn = self._get_connnection()
            conn.close()
            connected = True
        except Exception:
            connected = False

        return connected


class MSSQL(Integration, MSSQLConnectionChecker):
    def __init__(self, config, name, db_info):
        super().__init__(config, name)
        self.db_info = db_info
        self.user = db_info.get('user')
        self.password = db_info.get('password', None)
        self.host = db_info.get('host')
        self.port = db_info.get('port')

    def _query(self, query, fetch=False):
        conn = self._get_connnection()
        cur = conn.cursor()
        cur.execute(query)
        res = True
        if fetch:
            res = cur.fetchall()
        cur.close()
        conn.close()
        return res

    def setup(self):
        integration = self.db_info
        driver_name = integration.get('odbc_driver_name', 'MySQL ODBC 8.0 Unicode Driver')
        servers = self._query('exec sp_linkedservers;', fetch=True)
        servers = [x['SRV_NAME'] for x in servers]
        if self.mindsdb_database in servers:
            self._query(f"exec sp_dropserver @server = N'{self.mindsdb_database}';")
        mysql = self.config['api']['mysql']
        self._query(f'''
            exec sp_addlinkedserver
                @server = N'{self.mindsdb_database}'
                ,@srvproduct=N'MySQL'
                ,@provider=N'MSDASQL'
                ,@provstr=N'DRIVER={{{driver_name}}}; SERVER={mysql['host']}; PORT={mysql['port']}; DATABASE=mindsdb; USER={mysql['user']}_{self.name}; {('PASSWORD=' + mysql['password'] + ';') if len(mysql['password']) > 0 else ''} OPTION=3;';
        ''')
        try:
            self._query(f"exec sp_serveroption @server='{self.mindsdb_database}', @optname='rpc', @optvalue='true'")
            self._query(f"exec sp_serveroption @server='{self.mindsdb_database}', @optname='rpc out', @optvalue='true'")
        except Exception:
            # nothing critical if server options not setted. Only 'four part' notation will not work.
            print('MSSQL integration: failed to set server options.')

    def register_predictors(self, model_data_arr):
        pass

    def unregister_predictor(self, name):
        pass
