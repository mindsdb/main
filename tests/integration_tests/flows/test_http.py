import os
import unittest
from random import randint
from pathlib import Path
from uuid import uuid1
import json

import requests

from common import (
    CONFIG_PATH,
    run_environment
)

rand = randint(0, pow(10, 12))
ds_name = f'hr_ds_{rand}'
pred_name = f'hr_predictor_{rand}'
root = 'http://localhost:47334/api'


class HTTPTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        run_environment(
            apis=['http'],
            override_config={
                'integrations': {
                    'default_mariadb': {
                        'publish': True
                    },
                    'default_clickhouse': {
                        'publish': True
                    }
                }
            }
        )

        cls.config = json.loads(
            Path(CONFIG_PATH).read_text()
        )

        cls.initial_integrations_names = list(cls.config['integrations'].keys())

    def test_1_config(self):
        res = requests.get(f'{root}/config/integrations')
        assert res.status_code == 200
        res = res.json()
        assert isinstance(res['integrations'], list)

        test_integration_data = {'publish': False, 'host': 'test', 'type': 'clickhouse', 'port': 8123, 'user': 'default', 'password': '123'}
        res = requests.put(f'{root}/config/integrations/test_integration', json={'params': test_integration_data})
        assert res.status_code == 200

        res = requests.get(f'{root}/config/integrations/test_integration')
        assert res.status_code == 200
        test_integration = res.json()
        print(test_integration)
        assert len(test_integration) == 8

        for k in test_integration_data:
            if k != 'password':
                assert test_integration[k] == test_integration_data[k]

        for name in ['test_integration']:
            # Get the original
            res = requests.get(f'{root}/config/integrations/{name}')
            assert res.status_code == 200

            integration = res.json()
            for k in ['publish', 'host', 'port', 'type', 'user']:
                assert k in integration
                assert integration[k] is not None
            assert integration.get('password') is None

            # Modify it
            res = requests.post(
                f'{root}/config/integrations/{name}',
                json={'params': {'user': 'dr.Who'}}
            )

            res = requests.get(f'{root}/config/integrations/{name}')
            assert res.status_code == 200
            modified_integration = res.json()

            assert modified_integration['password'] is None
            assert modified_integration['user'] == 'dr.Who'
            for k in integration:
                if k not in ['password', 'date_last_update', 'user']:
                    assert modified_integration[k] == integration[k]

            # Put the original values back in\
            del integration['password']
            res = requests.post(f'{root}/config/integrations/{name}', json={'params': integration})
            res = requests.get(f'{root}/config/integrations/{name}')
            assert res.status_code == 200
            modified_integration = res.json()
            for k in integration:
                if k != 'date_last_update':
                    assert modified_integration[k] == integration[k]

        res = requests.delete(f'{root}/config/integrations/test_integration')
        assert res.status_code == 200

        res = requests.get(f'{root}/config/integrations/test_integration')
        assert res.status_code != 200

    def test_2_put_ds(self):
        # PUT datasource
        params = {
            'name': ds_name,
            'source_type': 'url',
            'source': 'https://raw.githubusercontent.com/mindsdb/mindsdb-examples/master/classics/home_rentals/dataset/train.csv'
        }
        url = f'{root}/datasources/{ds_name}'
        res = requests.put(url, json=params)
        assert res.status_code == 200

        db_ds_name = ds_name + '_db'
        params = {
            'name': db_ds_name,
            'query': 'SELECT arrayJoin([1,2,3]) as a, arrayJoin([1,2,3,4,5,6,7,8]) as b',
            'integration_id': 'default_clickhouse'
        }

        url = f'{root}/datasources/{db_ds_name}'
        res = requests.put(url, json=params)
        assert res.status_code == 200
        ds_data = res.json()

        assert ds_data['source_type'] == 'default_clickhouse'
        assert ds_data['row_count'] == 3 * 8

    def test_3_analyze(self):
        response = requests.get(f'{root}/datasources/{ds_name}/analyze')
        assert response.status_code == 200

    def test_3_put_predictor(self):
        # PUT predictor
        params = {
            'data_source_name': ds_name,
            'to_predict': 'rental_price',
            'kwargs': {
                'stop_training_in_x_seconds': 20,
                'join_learn_process': True
            }
        }
        url = f'{root}/predictors/{pred_name}'
        res = requests.put(url, json=params)
        assert res.status_code == 200

        # POST predictions
        params = {
            'when': {'sqft': 500}
        }
        url = f'{root}/predictors/{pred_name}/predict'
        res = requests.post(url, json=params)
        assert res.status_code == 200
        assert isinstance(res.json()[0]['rental_price']['predicted_value'], float)

    def test_4_datasources(self):
        """
        Call list datasources endpoint
        THEN check the response is success
        """
        response = requests.get(f'{root}/datasources/')
        assert response.status_code == 200

    def test_5_datasource_not_found(self):
        """
        Call unexisting datasource
        then check the response is NOT FOUND
        """
        response = requests.get(f'{root}/datasources/dummy_source')
        assert response.status_code == 404

    def test_6_utils(self):
        """
        Call utilities ping endpoint
        THEN check the response is success
        """

        response = requests.get(f'{root}/util/ping')
        assert response.status_code == 200


        response = requests.get(f'{root}/config/vars')
        assert response.status_code == 200

    def test_7_predictors(self):
        """
        Call list predictors endpoint
        THEN check the response is success
        """
        response = requests.get(f'{root}/predictors/')
        assert response.status_code == 200

    def test_8_predictor_not_found(self):
        """
        Call unexisting predictor
        then check the response is NOT FOUND
        """
        response = requests.get(f'{root}/predictors/dummy_predictor')
        assert response.status_code == 404

    def test_9_gui_is_served(self):
        """
        GUI downloaded and available
        """
        response = requests.get('http://localhost:47334/')
        assert response.status_code == 200
        assert response.content.decode().find('<head>') > 0

    def test__10_ds_from_unexist_integration(self):
        """
        Call telemetry enabled
        then check the response is status 200
        """
        ds_name = f"ds_{uuid1()}"
        data = {"integration_id": f'unexists_integration_{uuid1()}',
                "name": ds_name,
                "query": "select * from test_data.any_data limit 100;"}
        response = requests.put(f'{root}/datasources/{ds_name}', json=data)
        assert response.status_code == 400, f"expected 400 but got {response.status_code}, {response.text}"


if __name__ == '__main__':
    unittest.main(failfast=True)
