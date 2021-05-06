import unittest
from pathlib import Path
import json

from pymongo import MongoClient

from common import (
    MINDSDB_DATABASE,
    CONFIG_PATH,
    check_prediction_values,
    run_environment
)

# +++ define test data
TEST_DATASET = 'concrete_strength'

DB_TYPES_MAP = {
    int: 'int',
    float: 'float',
    str: 'text'
}

TO_PREDICT = {
    'concrete_strength': float,
    'cement': float
}
CONDITION = {
    'water': 162.5,
    'age': 28
}
# ---

TEST_DATA_TABLE = TEST_DATASET
TEST_PREDICTOR_NAME = f'{TEST_DATASET}_predictor'
EXTERNAL_DS_NAME = f'{TEST_DATASET}_external'

config = {}


class MongoTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        run_environment(
            apis=['mongodb'],
            override_config={
                'integrations': {
                    'default_mongodb': {
                        'publish': True
                    }
                }, 'permanent_storage': {
                    'location': 'local'
                }
            }
        )

        config.update(
            json.loads(
                Path(CONFIG_PATH).read_text()
            )
        )

        connection = config['api']['mongodb']
        kwargs = {}
        if len(connection.get('password', '')) > 0:
            kwargs['password'] = connection.get('password')
        if len(connection.get('user', '')) > 0:
            kwargs['username'] = connection.get('user')
        cls.mongo_mindsdb = MongoClient(
            host=connection['host'],
            port=int(connection.get('port', 47336)),
            **kwargs
        )

        integraton = config['integrations']['default_mongodb']
        cls.mongo_server = MongoClient(
            host=integraton['host'],
            port=integraton.get('port', 27017),
            username=integraton['user'],
            password=integraton['password']
        )

    def test_1_entitys_exists(self):
        collections = list(self.mongo_mindsdb[MINDSDB_DATABASE].list_collection_names())

        self.assertTrue(len(collections) == 2)
        self.assertTrue('predictors' in collections)
        self.assertTrue('commands' in collections)

        predictors = list(self.mongo_mindsdb[MINDSDB_DATABASE]['predictors'].find())
        self.assertTrue(len(predictors) == 0)

        tests_records_count = self.mongo_server['test_data'][TEST_DATASET].count_documents({})
        self.assertTrue(tests_records_count > 0)

    def test_2_learn_predictor(self):
        mindsdb = self.mongo_mindsdb[MINDSDB_DATABASE]
        mindsdb.predictors.insert_one({
            'name': TEST_PREDICTOR_NAME,
            'predict': list(TO_PREDICT.keys()),
            'connection': 'default_mongodb',
            'select_data_query': {
                'database': 'test_data',
                'collection': TEST_DATASET,
                'find': {}
            },
            'training_options': {
                'join_learn_process': True,
                'stop_training_in_x_seconds': 3
            }
        })

        predictors = list(mindsdb.predictors.find())
        self.assertTrue(TEST_PREDICTOR_NAME in [x['name'] for x in predictors])
        predictor_record = [x for x in predictors if x['name'] == TEST_PREDICTOR_NAME][0]
        self.assertTrue(predictor_record['status'] == 'complete')

        mindsdb_collections = mindsdb.list_collection_names()
        self.assertTrue(TEST_PREDICTOR_NAME in mindsdb_collections)

    def test_3_predict(self):
        mindsdb = self.mongo_mindsdb[MINDSDB_DATABASE]

        result = mindsdb[TEST_PREDICTOR_NAME].find_one(CONDITION)

        self.assertIsInstance(result, dict)
        self.assertTrue(check_prediction_values(result, TO_PREDICT))

    def test_4_remove(self):
        mindsdb = self.mongo_mindsdb[MINDSDB_DATABASE]

        mindsdb.predictors.delete_one({'name': TEST_PREDICTOR_NAME})

        predictors = list(mindsdb.predictors.find())
        self.assertTrue(TEST_PREDICTOR_NAME not in [x['name'] for x in predictors])

        mindsdb_collections = mindsdb.list_collection_names()
        self.assertTrue(TEST_PREDICTOR_NAME not in mindsdb_collections)


if __name__ == "__main__":
    try:
        unittest.main(failfast=True)
        print('Tests passed!')
    except Exception as e:
        print(f'Tests Failed!\n{e}')
