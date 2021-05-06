import unittest
import inspect
from pathlib import Path
import json
import requests

from pymongo import MongoClient

from common import (
    CONFIG_PATH,
    HTTP_API_ROOT,
    run_environment
)

from http_test_helpers import (
    get_predictors_names_list,
    get_datasources_names,
    get_integrations_names
)

config = {}

CID_A = 1
CID_B = 2


def get_mongo_predictors(company_id):
    client = MongoClient(host='127.0.0.1', port=int(config['api']['mongodb']['port']))
    client.admin.command({'company_id': company_id, 'need_response': 1})
    return [x['name'] for x in client.mindsdb.predictors.find()]


class CompanyIndependentTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        run_environment(
            apis=['http', 'mongodb']
        )

        config.update(
            json.loads(
                Path(CONFIG_PATH).read_text()
            )
        )

    def test_1_initial_state_http(self):
        print(f'\nExecuting {inspect.stack()[0].function}')

        # is no ds
        datasources_a = get_datasources_names(company_id=CID_A)
        datasources_b = get_datasources_names(company_id=CID_B)
        self.assertTrue(len(datasources_a) == 0)
        self.assertTrue(len(datasources_b) == 0)

        # is no predictors
        predictors_a = get_predictors_names_list(company_id=CID_A)
        predictors_b = get_predictors_names_list(company_id=CID_A)
        self.assertTrue(len(predictors_a) == 0)
        self.assertTrue(len(predictors_b) == 0)

        # is no integrations
        integrations_a = get_integrations_names(company_id=CID_A)
        integrations_b = get_integrations_names(company_id=CID_B)
        self.assertTrue(len(integrations_a) == 0)
        self.assertTrue(len(integrations_b) == 0)

    def test_2_add_integration_http(self):
        print(f'\nExecuting {inspect.stack()[0].function}')

        test_integration_data = {}
        test_integration_data.update(config['integrations']['default_postgres'])
        test_integration_data['publish'] = False

        res = requests.put(
            f'{HTTP_API_ROOT}/config/integrations/test_integration_a',
            json={'params': test_integration_data},
            headers={'company-id': f'{CID_A}'}
        )
        self.assertTrue(res.status_code == 200)

        integrations_a = get_integrations_names(company_id=CID_A)
        self.assertTrue(len(integrations_a) == 1 and integrations_a[0] == 'test_integration_a')

        integrations_b = get_integrations_names(company_id=CID_B)
        self.assertTrue(len(integrations_b) == 0)

        res = requests.put(
            f'{HTTP_API_ROOT}/config/integrations/test_integration_b',
            json={'params': test_integration_data},
            headers={'company-id': f'{CID_B}'}
        )
        self.assertTrue(res.status_code == 200)

        integrations_a = get_integrations_names(company_id=CID_A)
        self.assertTrue(len(integrations_a) == 1 and integrations_a[0] == 'test_integration_a')

        integrations_b = get_integrations_names(company_id=CID_B)
        self.assertTrue(len(integrations_b) == 1 and integrations_b[0] == 'test_integration_b')

    def test_3_add_datasources_http(self):
        print(f'\nExecuting {inspect.stack()[0].function}')

        params = {
            'name': 'test_ds_a',
            'query': 'select sqft, rental_price from test_data.home_rentals limit 20;',
            'integration_id': 'test_integration_a'
        }

        url = f'{HTTP_API_ROOT}/datasources/test_ds_a'
        res = requests.put(url, json=params, headers={'company-id': f'{CID_A}'})
        self.assertTrue(res.status_code == 200)

        datasources_a = get_datasources_names(company_id=CID_A)
        datasources_b = get_datasources_names(company_id=CID_B)
        self.assertTrue(len(datasources_a) == 1 and datasources_a[0] == 'test_ds_a')
        self.assertTrue(len(datasources_b) == 0)

        params = {
            'name': 'test_ds_b',
            'query': 'select sqft, rental_price from test_data.home_rentals limit 20;',
            'integration_id': 'test_integration_b'
        }

        url = f'{HTTP_API_ROOT}/datasources/test_ds_b'
        res = requests.put(url, json=params, headers={'company-id': f'{CID_B}'})
        self.assertTrue(res.status_code == 200)

        datasources_a = get_datasources_names(company_id=CID_A)
        datasources_b = get_datasources_names(company_id=CID_B)
        self.assertTrue(len(datasources_a) == 1 and datasources_a[0] == 'test_ds_a')
        self.assertTrue(len(datasources_b) == 1 and datasources_b[0] == 'test_ds_b')

    def test_4_add_predictors_http(self):
        params = {
            'data_source_name': 'test_ds_a',
            'to_predict': 'rental_price',
            'kwargs': {
                'stop_training_in_x_seconds': 5,
                'join_learn_process': True
            }
        }
        url = f'{HTTP_API_ROOT}/predictors/test_p_a'
        res = requests.put(url, json=params, headers={'company-id': f'{CID_A}'})
        self.assertTrue(res.status_code == 200)

        predictors_a = get_predictors_names_list(company_id=CID_A)
        predictors_b = get_predictors_names_list(company_id=CID_B)
        self.assertTrue(len(predictors_a) == 1 and predictors_a[0] == 'test_p_a')
        self.assertTrue(len(predictors_b) == 0)

        mongo_predictors_a = get_mongo_predictors(company_id=CID_A)
        mongo_predictors_b = get_mongo_predictors(company_id=CID_B)
        self.assertTrue(len(mongo_predictors_a) == 1 and mongo_predictors_a[0] == 'test_p_a')
        self.assertTrue(len(mongo_predictors_b) == 0)

        params = {
            'data_source_name': 'test_ds_a',
            'to_predict': 'rental_price',
            'kwargs': {
                'stop_training_in_x_seconds': 5,
                'join_learn_process': True
            }
        }
        url = f'{HTTP_API_ROOT}/predictors/test_p_b'
        res = requests.put(url, json=params, headers={'company-id': f'{CID_B}'})
        # shld not able create predictor from foreign ds
        self.assertTrue(res.status_code != 200)

        predictors_a = get_predictors_names_list(company_id=CID_A)
        predictors_b = get_predictors_names_list(company_id=CID_B)
        self.assertTrue(len(predictors_a) == 1 and predictors_a[0] == 'test_p_a')
        self.assertTrue(len(predictors_b) == 0)

        mongo_predictors_a = get_mongo_predictors(company_id=CID_A)
        mongo_predictors_b = get_mongo_predictors(company_id=CID_B)
        self.assertTrue(len(mongo_predictors_a) == 1 and mongo_predictors_a[0] == 'test_p_a')
        self.assertTrue(len(mongo_predictors_b) == 0)

        params = {
            'data_source_name': 'test_ds_b',
            'to_predict': 'rental_price',
            'kwargs': {
                'stop_training_in_x_seconds': 5,
                'join_learn_process': True
            }
        }
        url = f'{HTTP_API_ROOT}/predictors/test_p_b'
        res = requests.put(url, json=params, headers={'company-id': f'{CID_B}'})
        self.assertTrue(res.status_code == 200)

        predictors_a = get_predictors_names_list(company_id=CID_A)
        predictors_b = get_predictors_names_list(company_id=CID_B)
        self.assertTrue(len(predictors_a) == 1 and predictors_a[0] == 'test_p_a')
        self.assertTrue(len(predictors_b) == 1 and predictors_b[0] == 'test_p_b')

        mongo_predictors_a = get_mongo_predictors(company_id=CID_A)
        mongo_predictors_b = get_mongo_predictors(company_id=CID_B)
        self.assertTrue(len(mongo_predictors_a) == 1 and mongo_predictors_a[0] == 'test_p_a')
        self.assertTrue(len(mongo_predictors_b) == 1 and mongo_predictors_b[0] == 'test_p_b')

    def test_5_add_predictors_mongo(self):
        client = MongoClient(host='127.0.0.1', port=int(config['api']['mongodb']['port']))
        client.admin.command({'company_id': CID_A, 'need_response': 1})
        client.mindsdb.predictors.insert_one({
            'name': 'test_mon_p_a',
            'predict': 'rental_price',
            'external_datasource': 'test_ds_a',
            'training_options': {
                'join_learn_process': True,
                'stop_training_in_x_seconds': 3
            }
        })

        mongo_predictors_a = get_mongo_predictors(company_id=CID_A)
        mongo_predictors_b = get_mongo_predictors(company_id=CID_B)
        self.assertTrue(len(mongo_predictors_a) == 2 and mongo_predictors_a[1] == 'test_mon_p_a')
        self.assertTrue(len(mongo_predictors_b) == 1 and mongo_predictors_b[0] == 'test_p_b')

        client = MongoClient(host='127.0.0.1', port=int(config['api']['mongodb']['port']))
        client.admin.command({'company_id': CID_A, 'need_response': 1})
        client.mindsdb.predictors.delete_one({'name': 'test_p_a'})

        mongo_predictors_a = get_mongo_predictors(company_id=CID_A)
        mongo_predictors_b = get_mongo_predictors(company_id=CID_B)
        self.assertTrue(len(mongo_predictors_a) == 1 and mongo_predictors_a[0] == 'test_mon_p_a')
        self.assertTrue(len(mongo_predictors_b) == 1 and mongo_predictors_b[0] == 'test_p_b')

        predictors_a = get_predictors_names_list(company_id=CID_A)
        predictors_b = get_predictors_names_list(company_id=CID_B)
        self.assertTrue(len(predictors_a) == 1 and predictors_a[0] == 'test_mon_p_a')
        self.assertTrue(len(predictors_b) == 1 and predictors_b[0] == 'test_p_b')


if __name__ == "__main__":
    try:
        unittest.main(failfast=True)
        print('Tests passed!')
    except Exception as e:
        print(f'Tests Failed!\n{e}')
