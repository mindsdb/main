from mindsdb.interfaces.storage.db import session
from mindsdb.interfaces.storage.db import Integration
from copy import deepcopy


def add_db_integration(name, data, company_id):
    if 'database_name' not in data:
        data['database_name'] = name
    if 'publish' not in data:
        data['publish'] = True

    integration_record = Integration(name=name, data=data, company_id=company_id)
    session.add(integration_record)
    session.commit()

def modify_db_integration(name, data, company_id):
    integration_record = session.query(Integration).filter_by(company_id=company_id, name=name).first()
    old_data = deepcopy(integration_record.data)
    for k in old_data:
        if k not in data:
            data[k] = old_data[k]

    integration_record.data = data
    session.commit()

def remove_db_integration(name, company_id):
    session.query(Integration).filter_by(company_id=company_id, name=name).delete()
    session.commit()

def get_db_integration(name, company_id, sensitive_info=True):
    integration_record = session.query(Integration).filter_by(company_id=company_id, name=name).first()
    if integration_record is None or integration_record.data is None:
        return None
    data = deepcopy(integration_record.data)
    if data.get('password', None) is None:
        data['password'] = ''
    data['date_last_update'] = deepcopy(integration_record.updated_at)

    if not sensitive_info:
        data['password'] = None

    return data

def get_db_integrations(company_id, sensitive_info=True):
    integration_records = session.query(Integration).filter_by(company_id=company_id).all()
    integration_dict = {}
    for record in integration_records:
        if record is None or record.data is None:
            continue
        data = record.data
        if data.get('password', None) is None:
            data['password'] = ''
        data['date_last_update'] = deepcopy(record.updated_at)
        if not sensitive_info:
            data['password'] = None
        integration_dict[record.name] = data
    return integration_dict
