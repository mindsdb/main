from mindsdb.integrations.clickhouse.clickhouse import Clickhouse
from mindsdb.integrations.postgres.postgres import PostgreSQL
from mindsdb.integrations.mariadb.mariadb import Mariadb
from mindsdb.integrations.mysql.mysql import MySQL
from mindsdb.integrations.mssql.mssql import MSSQL
from mindsdb.integrations.mongodb.mongodb import MongoDB
from mindsdb.integrations.redis.redisdb import Redis
from mindsdb.integrations.kafka.kafkadb import Kafka

from mindsdb.utilities.log import log as logger
from mindsdb.utilities.config import Config
from mindsdb.interfaces.database.integrations import get_db_integration, get_db_integrations


class DatabaseWrapper():
    known_dbs = {'clickhouse': Clickhouse,
                 'mariadb': Mariadb,
                 'mysql': MySQL,
                 'postgres': PostgreSQL,
                 'mssql': MSSQL,
                 'mongodb': MongoDB,
                 'redis': Redis,
                 'kafka': Kafka}

    def __init__(self, company_id):
        self.config = Config()
        self.company_id = company_id

    def setup_integration(self, db_alias):
        try:
            # If this is the name of an integration
            integration = self._get_integration(db_alias)
            if integration is False:
                raise Exception(f'Unkonw database integration type for: {db_alias}')
            if integration is not True:
                integration.setup()
        except Exception as e:
            logger.warning('Failed to integrate with database ' + db_alias + f', error: {e}')

    def _get_integration(self, db_alias):
        integration = get_db_integration(db_alias, self.company_id)
        if integration:
            db_type = integration['type']
            if db_type in self.known_dbs:
                return self.known_dbs[db_type](self.config, db_alias, integration)
            logger.warning(f'Uknown integration type: {db_type} for database called: {db_alias}')
            return False
        return True

    def _get_integrations(self, publish=False):
        all_integrations = get_db_integrations(self.company_id)
        if publish is True:
            all_integrations = [x for x, y in get_db_integrations(self.company_id).items() if y.get('publish') is True]
        else:
            all_integrations = [x for x in get_db_integrations(self.company_id)]
        integrations = [self._get_integration(x) for x in all_integrations]
        integrations = [x for x in integrations if x is not True and x is not False]
        return integrations

    def register_predictors(self, model_data_arr, integration_name=None):
        if integration_name is None:
            integrations = self._get_integrations(publish=True)
        else:
            integration = self._get_integration(integration_name)
            integrations = [] if isinstance(integration, bool) else [integration]

        for integration in integrations:
            if integration.check_connection():
                try:
                    integration.register_predictors(model_data_arr)
                except Exception as e:
                    logger.warning(f"Error {e} when trying to register predictor to {integration.name}. Predictor wouldn't be registred.")
            else:
                logger.warning(f"There is no connection to {integration.name}. Predictor wouldn't be registred.")

    def unregister_predictor(self, name):
        for integration in self._get_integrations(publish=True):
            # FIXME
            # !!! Integrations from config.json add to db on each start!!!!
            if '@@@@@' in name:
                name = name.split('@@@@@')[1]
            if integration.check_connection():
                integration.unregister_predictor(name)
            else:
                logger.warning(f"There is no connection to {integration.name}. predictor wouldn't be unregistred")

    def check_connections(self):
        connections = {}
        for integration in self._get_integrations():
            connections[integration.name] = integration.check_connection()

        return connections
