from mindsdb.interfaces.storage.db import session, AITable
from mindsdb.utilities.log import log


class AITableStore():
    def __init__(self, company_id=None):
        self.company_id = company_id

    def is_ai_table(self, name):
        record = self.get_ai_table(name.lower())
        return record is not None

    def get_ai_table(self, name):
        ''' get particular ai table
        '''
        aitable_record = session.query(AITable).filter_by(company_id=self.company_id, name=name.lower()).first()
        return aitable_record

    def get_ai_tables(self):
        ''' get list of ai tables
        '''
        aitable_records = [x.__dict__ for x in session.query(AITable).filter_by(company_id=self.company_id)]
        return aitable_records

    def add(self, name, integration_name, integration_query, query_fields, predictor_name, predictor_fields):
        ai_table_record = AITable(
            name=name.lower(),
            integration_name=integration_name,
            integration_query=integration_query,
            query_fields=query_fields,
            predictor_name=predictor_name,
            predictor_columns=predictor_fields,
            company_id=self.company_id
        )
        session.add(ai_table_record)
        session.commit()

    def query(self, name, where=None):
        ''' query to ai table
        '''
        pass
