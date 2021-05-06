import os
import logging
import tempfile
from pathlib import Path

import torch.multiprocessing as mp

from mindsdb.__about__ import __version__ as mindsdb_version
from mindsdb.interfaces.database.database import DatabaseWrapper
from mindsdb.interfaces.storage.db import session, Predictor
from mindsdb.interfaces.storage.fs import FsSotre
from mindsdb.utilities.config import Config
from mindsdb.utilities.fs import create_process_mark, delete_process_mark


ctx = mp.get_context('spawn')


def create_learn_mark():
    if os.name == 'posix':
        p = Path(tempfile.gettempdir()).joinpath('mindsdb/learn_processes/')
        p.mkdir(parents=True, exist_ok=True)
        p.joinpath(f'{os.getpid()}').touch()


def delete_learn_mark():
    if os.name == 'posix':
        p = Path(tempfile.gettempdir()).joinpath('mindsdb/learn_processes/').joinpath(f'{os.getpid()}')
        if p.exists():
            p.unlink()


def run_learn(name, db_name, from_data, to_predict, kwargs, datasource_id, company_id):
    import mindsdb_native
    import mindsdb_datasources
    import mindsdb
    import torch
    import gc

    if 'join_learn_process' in kwargs:
        del kwargs['join_learn_process']

    create_process_mark('learn')

    config = Config()
    fs_store = FsSotre()
    mdb = mindsdb_native.Predictor(name=name, run_env={'trigger': 'mindsdb'})

    predictor_record = Predictor.query.filter_by(company_id=company_id, name=db_name).first()
    predictor_record.datasource_id = datasource_id
    predictor_record.to_predict = to_predict
    predictor_record.native_version = mindsdb_native.__version__
    predictor_record.mindsdb_version = mindsdb_version
    predictor_record.learn_args = {
        'to_predict': to_predict,
        'kwargs': kwargs
    }
    predictor_record.data = {
        'name': db_name,
        'status': 'training'
    }
    session.commit()

    to_predict = to_predict if isinstance(to_predict, list) else [to_predict]
    data_source = getattr(mindsdb_datasources, from_data['class'])(*from_data['args'], **from_data['kwargs'])
    try:
        mdb.learn(
            from_data=data_source,
            to_predict=to_predict,
            **kwargs
        )

    except Exception as e:
        log = logging.getLogger('mindsdb.main')
        log.error(f'Predictor learn error: {e}')
        predictor_record.data = {
            'name': db_name,
            'status': 'error'
        }
        session.commit()
        delete_process_mark('learn')
        return

    fs_store.put(name, f'predictor_{company_id}_{predictor_record.id}', config['paths']['predictors'])

    model_data = mindsdb_native.F.get_model_data(name)

    try:
        torch.cuda.empty_cache()
    except Exception as e:
        pass
    gc.collect()

    predictor_record = Predictor.query.filter_by(company_id=company_id, name=db_name).first()
    predictor_record.data = model_data
    session.commit()

    model_data['name'] = db_name
    DatabaseWrapper(company_id).register_predictors([model_data])
    delete_process_mark('learn')


class LearnProcess(ctx.Process):
    daemon = True

    def __init__(self, *args):
        super(LearnProcess, self).__init__(args=args)

    def run(self):
        '''
        running at subprocess due to
        ValueError: signal only works in main thread

        this is work for celery worker here?
        '''
        run_learn(*self._args)
