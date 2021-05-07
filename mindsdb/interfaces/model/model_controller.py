# @TODO, replace with arrow later: https://mirai-solutions.ch/news/2020/06/11/apache-arrow-flight-tutorial/
import xmlrpc
from xmlrpc.server import SimpleXMLRPCServer
from dateutil.parser import parse as parse_datetime
import os
import pickle
from pathlib import Path
import psutil
import datetime
from copy import deepcopy
import time
from contextlib import contextmanager

import pandas as pd
import mindsdb_datasources

from mindsdb.utilities.fs import create_directory, create_process_mark, delete_process_mark
from mindsdb.interfaces.database.database import DatabaseWrapper
from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage.fs import FsSotre
from mindsdb.utilities.log import log
import pyarrow as pa
import pyarrow.flight as fl


class ModelController():
    def __init__(self, ray_based):
        self.config = Config()
        self.fs_store = FsSotre()
        self.company_id = os.environ.get('MINDSDB_COMPANY_ID', None)
        self.dbw = DatabaseWrapper()
        self.predictor_cache = {}
        self.ray_based = ray_based

    def _invalidate_cached_predictors(self):
        from mindsdb_datasources import FileDS, ClickhouseDS, MariaDS, MySqlDS, PostgresDS, MSSQLDS, MongoDS, SnowflakeDS, AthenaDS
        import mindsdb_native
        from mindsdb_native import F
        from mindsdb_native.libs.constants.mindsdb import DATA_SUBTYPES
        from mindsdb.interfaces.storage.db import session, Predictor


        # @TODO: Cache will become stale if the respective NativeInterface is not invoked yet a bunch of predictors remained cached, no matter where we invoke it. In practice shouldn't be a big issue though
        for predictor_name in list(self.predictor_cache.keys()):
            if (datetime.datetime.now() - self.predictor_cache[predictor_name]['created']).total_seconds() > 1200:
                del self.predictor_cache[predictor_name]

    def _lock_predictor(self, id, mode='write'):
        from mindsdb.interfaces.storage.db import session, Semaphor

        while True:
            semaphor_record = session.query(Semaphor).filter_by(company_id=self.company_id, entity_id=id, entity_type='predictor').first()
            if semaphor_record is not None:
                if mode == 'read' and semaphor_record.action == 'read':
                    return True
            try:
                semaphor_record = Semaphor(company_id=self.company_id, entity_id=id, entity_type='predictor', action=mode)
                session.add(semaphor_record)
                session.commit()
                return True
            except Excpetion as e:
                pass
            time.sleep(1)


    def _unlock_predictor(self, id):
        from mindsdb.interfaces.storage.db import session, Semaphor
        semaphor_record = session.query(Semaphor).filter_by(company_id=self.company_id, entity_id=id, entity_type='predictor').first()
        if semaphor_record is not None:
            session.delete(semaphor_record)
            session.commit()

    @contextmanager
    def _lock_context(self, id, mode='write'):
        try:
            self._lock_predictor(mode)
            yield True
        finally:
            self._unlock_predictor(id)

    def _setup_for_creation(self, name):
        from mindsdb_datasources import FileDS, ClickhouseDS, MariaDS, MySqlDS, PostgresDS, MSSQLDS, MongoDS, SnowflakeDS, AthenaDS
        import mindsdb_native
        from mindsdb_native import F
        from mindsdb_native.libs.constants.mindsdb import DATA_SUBTYPES
        from mindsdb.interfaces.storage.db import session, Predictor


        if name in self.predictor_cache:
            del self.predictor_cache[name]
        # Here for no particular reason, because we want to run this sometimes but not too often
        self._invalidate_cached_predictors()

        predictor_dir = Path(self.config.paths['predictors']).joinpath(name)
        create_directory(predictor_dir)
        predictor_record = Predictor(company_id=self.company_id, name=name, is_custom=False)

        session.add(predictor_record)
        session.commit()

    def _try_outdate_db_status(self, predictor_record):
        from mindsdb_native import __version__ as native_version
        from mindsdb import __version__ as mindsdb_version
        from mindsdb.interfaces.storage.db import session

        if predictor_record.update_status == 'update_failed':
            return predictor_record

        if predictor_record.native_version != native_version:
            predictor_record.update_status = 'available'
        if predictor_record.mindsdb_version != mindsdb_version:
            predictor_record.update_status = 'available'

        session.commit()
        return predictor_record

    def _update_db_status(self, predictor_record):
        from mindsdb_native import __version__ as native_version
        from mindsdb import __version__ as mindsdb_version
        from mindsdb.interfaces.storage.db import session

        predictor_record.native_version = native_version
        predictor_record.mindsdb_version = mindsdb_version
        predictor_record.update_status = 'up_to_date'

        session.commit()
        return predictor_record

    def create(self, name):
        from mindsdb_datasources import FileDS, ClickhouseDS, MariaDS, MySqlDS, PostgresDS, MSSQLDS, MongoDS, SnowflakeDS, AthenaDS
        import mindsdb_native
        from mindsdb_native import F
        from mindsdb_native.libs.constants.mindsdb import DATA_SUBTYPES
        from mindsdb.interfaces.storage.db import session, Predictor


        self._setup_for_creation(name)
        predictor = mindsdb_native.Predictor(name=name, run_env={'trigger': 'mindsdb'})
        return predictor

    def learn(self, name, from_data, to_predict, datasource_id, kwargs={}):
        from mindsdb.interfaces.model.learn_process import LearnProcess, run_learn

        create_process_mark('learn')

        join_learn_process = kwargs.get('join_learn_process', False)
        if 'join_learn_process' in kwargs:
            del kwargs['join_learn_process']

        self._setup_for_creation(name)

        if self.ray_based:
            run_learn(name, from_data, to_predict, kwargs, datasource_id)
        else:
            p = LearnProcess(name, from_data, to_predict, kwargs, datasource_id)
            p.start()
            if join_learn_process is True:
                p.join()
                if p.exitcode != 0:
                    delete_process_mark('learn')
                    raise Exception('Learning process failed !')

        delete_process_mark('learn')
        return 0

    def predict(self, name, pred_format, when_data=None, kwargs={}):
        from mindsdb_datasources import (FileDS, ClickhouseDS, MariaDS,
                                         MySqlDS, PostgresDS, MSSQLDS, MongoDS,
                                         SnowflakeDS, AthenaDS)
        import mindsdb_native
        from mindsdb.interfaces.storage.db import session, Predictor

        create_process_mark('predict')

        if name not in self.predictor_cache:
            # Clear the cache entirely if we have less than 1.2 GB left
            if psutil.virtual_memory().available < 1.2 * pow(10, 9):
                self.predictor_cache = {}

            predictor_record = Predictor.query.filter_by(company_id=self.company_id, name=name, is_custom=False).first()
            if predictor_record.data['status'] == 'complete':
                self.fs_store.get(name, f'predictor_{self.company_id}_{predictor_record.id}', self.config['paths']['predictors'])
                self.predictor_cache[name] = {
                    'predictor': mindsdb_native.Predictor(name=name, run_env={'trigger': 'mindsdb'}),
                    'created': datetime.datetime.now()
                }

        if isinstance(when_data, dict) and 'kwargs' in when_data and 'args' in when_data:
            data_source = getattr(mindsdb_datasources, when_data['class'])(*when_data['args'], **when_data['kwargs'])
        else:
            # @TODO: Replace with Datasource
            try:
                data_source = pd.DataFrame(when_data)
            except Exception:
                data_source = when_data

        predictions = self.predictor_cache[name]['predictor'].predict(
            when_data=data_source,
            **kwargs
        )
        if pred_format == 'explain' or pred_format == 'new_explain':
            predictions = [p.explain() for p in predictions]
        elif pred_format == 'dict':
            predictions = [p.as_dict() for p in predictions]
        elif pred_format == 'dict&explain':
            predictions = [[p.as_dict() for p in predictions], [p.explain() for p in predictions]]
        else:
            delete_process_mark('predict')
            raise Exception(f'Unkown predictions format: {pred_format}')

        delete_process_mark('predict')

        return predictions

    def analyse_dataset(self, ds):
        from mindsdb_datasources import FileDS, ClickhouseDS, MariaDS, MySqlDS, PostgresDS, MSSQLDS, MongoDS, SnowflakeDS, AthenaDS
        from mindsdb_native import F

        create_process_mark('analyse')

        ds = eval(ds['class'])(*ds['args'], **ds['kwargs'])
        analysis = F.analyse_dataset(ds)

        delete_process_mark('analyse')
        return analysis

    def get_model_data(self, name, db_fix=True):
        from mindsdb_native import F
        from mindsdb_native.libs.constants.mindsdb import DATA_SUBTYPES
        from mindsdb.interfaces.storage.db import session, Predictor

        predictor_record = Predictor.query.filter_by(company_id=self.company_id, name=name, is_custom=False).first()
        predictor_record = self._try_outdate_db_status(predictor_record)
        model = predictor_record.data
        if model is None or model['status'] == 'training':
            try:
                self.fs_store.get(name, f'predictor_{self.company_id}_{predictor_record.id}', self.config['paths']['predictors'])
                new_model_data = F.get_model_data(name)
            except Exception:
                new_model_data = None

            if predictor_record.data is None or (new_model_data is not None and len(new_model_data) > len(predictor_record.data)):
                predictor_record.data = new_model_data
                model = new_model_data
                session.commit()

        # Make some corrections for databases not to break when dealing with empty columns
        if db_fix:
            data_analysis = model['data_analysis_v2']
            for column in model['columns']:
                analysis = data_analysis.get(column)
                if isinstance(analysis, dict) and (len(analysis) == 0 or analysis.get('empty', {}).get('is_empty', False)):
                    data_analysis[column]['typing'] = {
                        'data_subtype': DATA_SUBTYPES.INT
                    }

        model['created_at'] = str(parse_datetime(str(predictor_record.created_at).split('.')[0]))
        model['updated_at'] = str(parse_datetime(str(predictor_record.updated_at).split('.')[0]))
        model['predict'] = predictor_record.to_predict
        model['update'] = predictor_record.update_status
        return model

    def get_models(self):
        from mindsdb.interfaces.storage.db import session, Predictor

        models = []
        predictor_records = Predictor.query.filter_by(company_id=self.company_id, is_custom=False)
        predictor_names = [
            x.name for x in predictor_records
        ]
        for model_name in predictor_names:
            try:
                model_data = self.get_model_data(model_name, db_fix=False)
                reduced_model_data = {}

                for k in ['name', 'version', 'is_active', 'predict', 'status', 'current_phase', 'accuracy', 'data_source', 'update']:
                    reduced_model_data[k] = model_data.get(k, None)

                for k in ['train_end_at', 'updated_at', 'created_at']:
                    reduced_model_data[k] = model_data.get(k, None)
                    if reduced_model_data[k] is not None:
                        try:
                            reduced_model_data[k] = parse_datetime(str(reduced_model_data[k]).split('.')[0])
                        except Exception as e:
                            # @TODO Does this ever happen
                            log.error(f'Date parsing exception while parsing: {k} in get_models: ', e)
                            reduced_model_data[k] = parse_datetime(str(reduced_model_data[k]))

                models.append(reduced_model_data)
            except Exception as e:
                log.error(f"Can't list data for model: '{model_name}' when calling `get_models(), error: {e}`")
        return models

    def delete_model(self, name):
        from mindsdb_native import F
        from mindsdb_native.libs.constants.mindsdb import DATA_SUBTYPES
        from mindsdb.interfaces.storage.db import session, Predictor

        predictor_record = Predictor.query.filter_by(company_id=self.company_id, name=name, is_custom=False).first()
        id = predictor_record.id
        session.delete(predictor_record)
        session.commit()
        F.delete_model(name)
        self.dbw.unregister_predictor(name)
        self.fs_store.delete(f'predictor_{self.company_id}_{id}')
        return 0

    def update_model(self, name):
        from mindsdb_native import F
        from mindsdb_worker.updater.update_model import update_model
        from mindsdb.interfaces.storage.db import session, Predictor
        from mindsdb.interfaces.datastore.datastore import DataStore

        try:
            predictor_record = Predictor.query.filter_by(company_id=self.company_id, name=name, is_custom=False).first()
            predictor_record.update_status = 'updating'
            session.commit()
            update_model(name, self.delete_model, F.delete_model, self.learn, self._lock_context, self.company_id, self.config['paths']['predictors'], predictor_record, self.fs_store, DataStore())

            predictor_record = self._update_db_status(predictor_record)
        except Exception as e:
            log.error(e)
            predictor_record.update_status = 'update_failed'
            session.commit()
            return str(e)


try:
    from mindsdb_worker.cluster.ray_controller import ray_ify
    import ray
    ray.init(ignore_reinit_error=True)
    ModelController = ray_ify(ModelController)
except Exception as e:
    print(e)
    pass


def ping(): return True


class FlightServer(fl.FlightServerBase):
    def __init__(self, location="grpc://localhost:19329", **kwargs):
        super(FlightServer, self).__init__(location, **kwargs)

        self.controller = ModelController(False)

        self.actions = {
            'create': self.controller.create,
            'learn': self.controller.learn,
            'predict': self.controller.predict,
            'analyse_dataset': self.controller.analyse_dataset,
            'get_model_data': self.controller.get_model_data,
            'get_models': self.controller.get_models,
            'delete_model': self.controller.delete_model,
            'ping': ping,
        }

    def do_action(self, context, action):
        if action.type not in self.actions:
            raise ValueError('Unknown action')
        else:
            kwargs = pickle.loads(action.body)
            obj = self.actions[action.type](**kwargs)
            buf = pa.py_buffer(pickle.dumps(obj))
            res = pa.flight.Result(buf)
            yield res


def start():
    server = FlightServer("grpc://localhost:19329")
    server.serve()
