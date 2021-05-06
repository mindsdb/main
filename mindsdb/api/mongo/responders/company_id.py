from mindsdb.api.mongo.classes import Responder
from mindsdb.interfaces.datastore.datastore import DataStoreWrapper
from mindsdb.interfaces.model.model_interface import ModelInterfaceWrapper


class Responce(Responder):
    def when(self, query):
        return 'company_id' in query

    def result(self, query, request_env, mindsdb_env, session):
        company_id = query.get('company_id')
        need_response = query.get('need_response', False)

        mindsdb_env['company_id'] = company_id
        mindsdb_env['data_store'] = DataStoreWrapper(
            data_store=mindsdb_env['data_store'],
            company_id=company_id
        )
        mindsdb_env['mindsdb_native'] = ModelInterfaceWrapper(
            model_interface=mindsdb_env['mindsdb_native'],
            company_id=company_id
        )

        if need_response:
            return {'ok': 1}

        return None


responder = Responce()
