import os
import shutil
import importlib
import json
import sys

import mindsdb_datasources
import pandas as pd

from mindsdb.interfaces.database.database import DatabaseWrapper
from mindsdb.interfaces.model.model_interface import ModelInterface as NativeInterface
from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage.db import session, Predictor
from mindsdb.interfaces.storage.fs import FsSotre


class CustomModels():
    def __init__(self, company_id=None):
        self.config = Config()
        self.fs_store = FsSotre()
        self.company_id = company_id
        self.dbw = DatabaseWrapper(None)
        self.storage_dir = self.config['paths']['custom_models']
        os.makedirs(self.storage_dir, exist_ok=True)
        self.model_cache = {}
        self.mindsdb_native = NativeInterface()

    def _dir(self, name):
        return str(os.path.join(self.storage_dir, name))

    def _internal_load(self, name):
        self.fs_store.get(name, f'custom_model_{self.company_id}_{name}', self.storage_dir)
        sys.path.insert(0, self._dir(name))
        module = __import__(name)

        try:
            model = module.Model.load(os.path.join(self._dir(name), 'model.pickle'))
        except Exception as e:
            model = module.Model()
            model.initialize_column_types()
            if hasattr(model, 'setup'):
                model.setup()

        self.model_cache[name] = model

        return model

    def learn(self, name, from_data, to_predict, datasource_id, kwargs={}):
        model_data = self.get_model_data(name)
        model_data['status'] = 'training'
        self.save_model_data(name, model_data)

        to_predict = to_predict if isinstance(to_predict,list) else [to_predict]

        data_source = getattr(mindsdb_datasources, from_data['class'])(*from_data['args'], **from_data['kwargs'])
        data_frame = data_source.df
        model = self._internal_load(name)
        model.to_predict = to_predict

        model_data = self.get_model_data(name)
        model_data['predict'] = model.to_predict
        self.save_model_data(name, model_data)

        data_analysis = self.mindsdb_native.analyse_dataset(data_source)['data_analysis_v2']

        model_data = self.get_model_data(name)
        model_data['data_analysis_v2'] = data_analysis
        self.save_model_data(name, model_data)

        model.fit(data_frame, to_predict, data_analysis, kwargs)

        model.save(os.path.join(self._dir(name), 'model.pickle'))
        self.model_cache[name] = model

        model_data = self.get_model_data(name)
        model_data['status'] = 'completed'
        model_data['columns'] = list(data_analysis.keys())
        self.save_model_data(name, model_data)
        self.fs_store.put(name, f'custom_model_{self.company_id}_{name}', self.storage_dir)

        self.dbw.unregister_predictor(name)
        self.dbw.register_predictors([self.get_model_data(name)])

    def predict(self, name, when_data=None, from_data=None, kwargs=None):
        self.fs_store.get(name, f'custom_model_{self.company_id}_{name}', self.storage_dir)
        if kwargs is None:
            kwargs = {}
        if from_data is not None:
            if isinstance(from_data, dict):
                data_source = getattr(mindsdb_datasources, from_data['class'])(*from_data['args'],
                                                                          **from_data['kwargs'])
            # assume that particular instance of any DataSource class is provided
            else:
                data_source = from_data
            data_frame = data_source.df
        elif when_data is not None:
            if isinstance(when_data, dict):
                for k in when_data:
                    when_data[k] = [when_data[k]]
                data_frame = pd.DataFrame(when_data)
            else:
                data_frame = pd.DataFrame(when_data)

        model = self._internal_load(name)
        predictions = model.predict(data_frame, kwargs)

        pred_arr = []
        for i in range(len(predictions)):
            pred_arr.append({})
            pred_arr[-1] = {}
            for col in predictions.columns:
                pred_arr[-1][col] = {}
                pred_arr[-1][col]['predicted_value'] = predictions[col].iloc[i]

        return pred_arr

    def get_model_data(self, name):
        predictor_record = Predictor.query.filter_by(company_id=self.company_id, name=name, is_custom=True).first()
        return predictor_record.data

    def save_model_data(self, name, data):
        predictor_record = Predictor.query.filter_by(company_id=self.company_id, name=name, is_custom=True).first()
        if predictor_record is None:
            predictor_record = Predictor(company_id=self.company_id, name=name, is_custom=True, data=data)
            session.add(predictor_record)
        else:
            predictor_record.data = data
        session.commit()

    def get_models(self):
        predictor_names = [
            x.name for x in Predictor.query.filter_by(company_id=self.company_id, is_custom=True)
        ]
        models = []
        for name in predictor_names:
            models.append(self.get_model_data(name))

        return models

    def delete_model(self, name):
        Predictor.query.filter_by(company_id=self.company_id, name=name, is_custom=True).delete()
        session.commit()
        shutil.rmtree(self._dir(name))
        self.dbw.unregister_predictor(name)
        self.fs_store.delete(f'custom_model_{self.company_id}_{name}')

    def rename_model(self, name, new_name):
        self.fs_store.get(name, f'custom_model_{self.company_id}_{name}', self.storage_dir)

        self.dbw.unregister_predictor(name)
        shutil.move(self._dir(name), self._dir(new_name))
        shutil.move(os.path.join(self._dir(new_name) + f'{name}.py'), os.path.join(self._dir(new_name) ,f'{new_name}.py'))

        predictor_record = Predictor.query.filter_by(company_id=self.company_id, name=name, is_custom=True).first()
        predictor_record.name = new_name
        session.commit()

        self.dbw.register_predictors([self.get_model_data(new_name)])

        self.fs_store.put(name, f'custom_model_{self.company_id}_{new_name}', self.storage_dir)
        self.fs_store.delete(f'custom_model_{self.company_id}_{name}')

    def export_model(self, name):
        shutil.make_archive(base_name=name, format='zip', root_dir=self._dir(name))
        return str(self._dir(name)) + '.zip'

    def load_model(self, fpath, name, trained_status):
        shutil.unpack_archive(fpath, self._dir(name), 'zip')
        shutil.move( os.path.join(self._dir(name), 'model.py') ,  os.path.join(self._dir(name), f'{name}.py') )
        model = self._internal_load(name)
        model.to_predict = model.to_predict if isinstance(model.to_predict,list) else [model.to_predict]
        self.save_model_data(name,{
            'name': name
            ,'data_analysis_v2': model.column_type_map
            ,'predict': model.to_predict
            ,'status': trained_status
            ,'is_custom': True
            ,'columns': list(model.column_type_map.keys())
        })

        with open(os.path.join(self._dir(name), '__init__.py') , 'w') as fp:
            fp.write('')

        self.fs_store.put(name, f'custom_model_{self.company_id}_{name}', self.storage_dir)

        if trained_status == 'trained':
            self.dbw.register_predictors([self.get_model_data(name)])
