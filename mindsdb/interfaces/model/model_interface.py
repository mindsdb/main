# @TODO, replace with arrow later: https://mirai-solutions.ch/news/2020/06/11/apache-arrow-flight-tutorial/
import xmlrpc
import xmlrpc.client
import time
import pickle

from mindsdb.utilities.log import log
import pyarrow.flight as fl


class ModelInterfaceRPC():
    def __init__(self):
        for _ in range(10):
            try:
                time.sleep(3)
                self.client = fl.connect("grpc://localhost:19329")
                res = self._action('ping')
                assert self._loads(res)
                return
            except Exception:
                import traceback
                print(traceback.format_exc())
                log.info('Wating for native RPC server to start')
        raise Exception('Unable to connect to RPC server')

    def _action(self, act_name, **kwargs):
        action = fl.Action(act_name, pickle.dumps(kwargs))
        return self.client.do_action(action)

    def _loads(self, res):
        return pickle.loads(next(iter(res)).body.to_pybytes())

    def create(self, name):
        self._action('create', name=name)

    def learn(self, name, from_data, to_predict, datasource_id, kwargs={}):
        self._action('learn', name=name, from_data=from_data, to_predict=to_predict, datasource_id=datasource_id, kwargs=kwargs)

    def predict(self, name, pred_format, when_data=None, kwargs={}):
        res = self._action('predict', name=name, pred_format=pred_format, when_data=when_data, kwargs=kwargs)
        return self._loads(res)

    def analyse_dataset(self, ds):
        res = self._action('analyse_dataset', ds=ds)
        return self._loads(res)

    def get_model_data(self, name, db_fix=True):
        res = self._action('get_model_data', name=name, db_fix=db_fix)
        return self._loads(res)

    def get_models(self):
        res = self._action('get_models')
        return self._loads(res)

    def delete_model(self, name):
        self._action('delete_model', name=name)

    def update_model(self, name):
        return 'Model updating is no available in this version of mindsdb'


try:
    from mindsdb_worker.cluster.ray_interface import ModelInterfaceRay
    import ray
    ray.init(ignore_reinit_error=True)
    ModelInterface = ModelInterfaceRay
    ray_based = True
except Exception as e:
    ModelInterface = ModelInterfaceRPC
    ray_based = False
