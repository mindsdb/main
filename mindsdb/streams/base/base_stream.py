import os
from threading import Event
from mindsdb.interfaces.model.model_interface import ModelInterface as NativeInterface


class StreamTypes:
    timeseries = "timeseries"


class BaseStream:
    def __init__(self):
        self.format_flag = 'dict'
        self.stop_event = Event()
        self.company_id = os.environ.get('MINDSDB_COMPANY_ID', None)
        self.target = None
        self.window = None
        self.gb = None
        self.dt = None
        self.native_interface = NativeInterface()

    @staticmethod
    def get_ts_settings(predict_record):
        ts_settings = predict_record.learn_args['kwargs'].get('timeseries_settings', None)
        if ts_settings is None:
            raise Exception(f"Attempt to use {predict_record.name} predictor (not TS type) as timeseries predictor.")
        ts_settings['to_predict'] = predict_record.learn_args['to_predict']
        return ts_settings

    @staticmethod
    def is_anomaly(prediction):
        for key in prediction:
            if "anomaly" in key and prediction[key] is not None:
                return True
        return False
