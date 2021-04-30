import json
from threading import Thread, Event
import walrus

from mindsdb.utilities.log import log
from mindsdb.streams.base.base_stream import StreamTypes, BaseStream
from mindsdb.interfaces.storage.db import session
from mindsdb.interfaces.storage.db import Predictor as DBPredictor
from mindsdb.interfaces.model.model_interface import ModelInterface as NativeInterface


class RedisStream(Thread, BaseStream):
    def __init__(self, name, connection_info, advanced_info, stream_in, stream_out, stream_anomaly, predictor, _type):
        self.stream_name = name
        self.connection_info = connection_info
        self.connection_info.update(advanced_info)
        self.predictor = predictor
        self.client = self._get_client()
        self.stream_in_name = stream_in
        self.stream_out_name = stream_out
        self.stream_in = self.client.Stream(stream_in)
        self.stream_out = self.client.Stream(stream_out)
        self.stream_anomaly = self.client.Stream(stream_anomaly)
        self._type = _type

        BaseStream.__init__(self)
        if self._type.lower() == StreamTypes.timeseries:
            super().__init__(target=RedisStream.make_timeseries_predictions, args=(self,))
        else:
            super().__init__(target=RedisStream.make_predictions, args=(self,))

    def _get_client(self):
        return walrus.Database(**self.connection_info)

    def predict(self, stream_in, timeseries_mode=False):
        predict_info = stream_in.read(block=0)
        when_list = []
        for record in predict_info:
            record_id = record[0]
            raw_when_data = record[1]
            when_data = self.decode(raw_when_data)
            if timeseries_mode:
                if self.target not in when_data:
                    when_data['make_predictions'] = False
                else:
                    when_data['make_predictions'] = True
                when_list.append(when_data)
            else:
                result = self.native_interface.predict(self.predictor, self.format_flag, when_data=when_data)
                log.error(f"STREAM: got {result}")
                for res in result:
                    in_json = json.dumps(res)
                    stream_out = self.stream_anomaly if self.is_anomaly(res) else self.stream_out
                    stream_out.add({'prediction': in_json})
                stream_in.delete(record_id)

        if timeseries_mode:
            result = self.native_interface.predict(self.predictor, self.format_flag, when_data=when_list)
            log.error(f"TIMESERIES STREAM: got {result}")
            for res in result:
                in_json = json.dumps(res)
                stream_out = self.stream_anomaly if self.is_anomaly(res) else self.stream_out
                stream_out.add({'prediction': in_json})
            stream_in.trim(len(stream_in) - 1, approximate=False)


    def make_prediction_from_cache(self, cache):
        log.error("STREAM: in make_prediction_from_cache")
        if len(cache) >= self.window:
            log.error(f"STREAM: make_prediction_from_cache - len(cache) = {len(cache)}")
            self.predict(cache, timeseries_mode=True)

    def make_timeseries_predictions(self):
        log.error("STREAM: running 'make_timeseries_predictions'")
        predict_record = session.query(DBPredictor).filter_by(company_id=self.company_id, name=self.predictor).first()
        if predict_record is None:
            log.error(f"Error creating stream: requested predictor {self.predictor} is not exist")
            return
        ts_settings = self.get_ts_settings(predict_record)
        log.error(f"STREAM TS_SETTINGS: {ts_settings}")
        self.target = ts_settings['to_predict'][0]
        self.window = ts_settings['window']
        self.gb = ts_settings['group_by'][0]
        self.dt = ts_settings['order_by'][0]


        while not self.stop_event.wait(0.5):
            predict_info = self.stream_in.read()
            for record in predict_info:
                record_id = record[0]
                raw_when_data = record[1]
                when_data = self.decode(raw_when_data)
                log.error(f"STREAM: next record have read from {self.stream_in.key}: {when_data}")
                self.to_cache(when_data)
                self.stream_in.delete(record_id)
        session.close()

    def to_cache(self, record):
        gb_val = record[self.gb]
        cache = self.client.Stream(f"{self.stream_name}.cache.{gb_val}")
        log.error(f"STREAM: cache {cache.key} has been created")
        self.make_prediction_from_cache(cache)
        self.handle_record(cache, record)
        self.make_prediction_from_cache(cache)
        log.error("STREAM in cache: current iteration has done.")

    def handle_record(self, cache, record):
        log.error(f"STREAM: handling cache {cache.key} and {record} record.")
        records = cache.read()
        records = [self.decode(x[1]) for x in records]
        log.error(f"STREAM: read {records} from cache.")
        records.append(record)
        records = self.sort_cache(records)
        log.error(f"STREAM: after updating and sorting - {records}.")
        cache.trim(0, approximate=False)
        for rec in records:
            cache.add(rec)
        log.error(f"STREAM: finish updating {cache.key}")

    def sort_cache(self, cache):
        return sorted(cache, key=lambda x: x[self.dt])

    def make_predictions(self):
        predict_record = session.query(DBPredictor).filter_by(company_id=self.company_id, name=self.predictor).first()
        if predict_record is None:
            log.error(f"Error creating stream: requested predictor {self.predictor} is not exist")
            return

        while not self.stop_event.wait(0.5):
            predict_info = self.stream_in.read()
            for record in predict_info:
                record_id = record[0]
                raw_when_data = record[1]
                when_data = self.decode(raw_when_data)

                result = self.native_interface.predict(self.predictor, self.format_flag, when_data=when_data)
                log.error(f"STREAM: got {result}")
                for res in result:
                    in_json = json.dumps(res)
                    self.stream_out.add({"prediction": in_json})
                self.stream_in.delete(record_id)

        session.close()
        log.error("STREAM: stopping...")

    def decode(self, redis_data):
        decoded = {}
        for k in redis_data:
            decoded[k.decode('utf8')] = redis_data[k].decode('utf8')
        return decoded
