from threading import Thread
import walrus
from mindsdb.integrations.base import StreamIntegration
from mindsdb.streams.redis.redis_stream import RedisStream
from mindsdb.streams.base.base_stream import StreamTypes

from mindsdb.interfaces.storage.db import session, Stream
from mindsdb.interfaces.database.integrations import get_db_integration


class RedisConnectionChecker:
    def __init__(self, **kwargs):
        self.connection_info = kwargs.get("connection", {})
        self.advanced_info = kwargs.get("advanced", {})
        self.connection_params = {}
        self.connection_params.update(self.connection_info)
        self.connection_params.update(self.advanced_info)

    def _get_connection(self):
        return walrus.Database(**self.connection_params)

    def check_connection(self):
        client = self._get_connection()
        try:
            client.dbsize()
            return True
        except Exception:
            return False


class Redis(StreamIntegration, RedisConnectionChecker):
    """Redis Integration which is more a Streams factory
    than classical Integration."""
    def __init__(self, config, name):
        StreamIntegration.__init__(self, config, name)
        integration_info = get_db_integration(self.name, self.company_id)

        self.connection_info = integration_info.get("connection", {})
        self.advanced_info = integration_info.get("advanced", {})
        self.connection_params = {}
        self.connection_params.update(self.connection_info)
        self.connection_params.update(self.advanced_info)
        self.client = self._get_connection()


        self.control_stream_name = integration_info.get('stream')
        self.control_stream = self.client.Stream(self.control_stream_name)

    def start(self):
        Thread(target=Redis.work, args=(self, )).start()

    def start_stored_streams(self):
        existed_streams = session.query(Stream).filter_by(company_id=self.company_id, integration=self.name)

        for stream in existed_streams:
            to_launch = self.get_stream_from_db(stream)
            if stream.name not in self.streams:
                params = {"integration": stream.integration,
                          "predictor": stream.predictor,
                          "stream_in": stream.stream_in,
                          "stream_out": stream.stream_out,
                          "type": stream._type}

                self.log.error(f"Integration {self.name} - launching from db : {params}")
                to_launch.start()
                self.streams[stream.name] = to_launch.stop_event

    def work(self):
        """Creates a Streams by receiving initial information from control stream."""
        self.log.error(f"INTEGRATION HAS BEEN CREATED: {self.connection_params}")
        self.log.error(f"Integration {self.name}: start listening {self.control_stream_name} redis stream")
        while not self.stop_event.wait(0.5):
            try:
                # break if no record about this integration has found in db
                if not self.exist_in_db():
                    self.delete_all_streams()
                    break
                # First, lets check that there are no new records in db, created via HTTP API for e.g.
                self.start_stored_streams()
                # stop streams which have been deleted from db.
                self.stop_deleted_streams()
                # Checking new incoming records(requests) for new stream.
                # Could't use blocking reading here, because this loop must
                # also check new records in db (created via HTTP api for e.g)
                recs = self.control_stream.read()
                for r in recs:
                    r_id = r[0]
                    binary_r = r[1]
                    stream_params = self._decode(binary_r)
                    if "input_stream" not in stream_params or "output_stream" not in stream_params:
                        self.delete_stream(stream_params['predictor'])
                        self.control_stream.delete(r_id)
                        continue

                    stream_params['name'] = f"{self.name}_{stream_params['predictor']}"
                    stream = self.get_stream_from_kwargs(**stream_params)
                    self.log.error(f"Integration {self.name}: creating stream: {stream_params}")
                    stream.start()
                    # store created stream in database
                    self.store_stream(stream)
                    # need to delete record which steam is already created
                    self.control_stream.delete(r_id)
            except Exception as e:
                self.log.error(f"Integration {self.name}: {e}")
                raise e

        # received exit event
        self.log.error(f"Integration {self.name}: exiting...")
        self.client.close()
        self.stop_streams()
        session.close()

    def store_stream(self, stream):
        """Stories a created stream."""
        stream_rec = Stream(name=stream.stream_name, connection_params=self.connection_info, advanced_params=self.advanced_info,
                            _type=stream._type, predictor=stream.predictor,
                            integration=self.name, company_id=self.company_id,
                            stream_in=stream.stream_in_name, stream_out=stream.stream_out_name, stream_anomaly=stream.stream_anomaly_name)
        session.add(stream_rec)
        session.commit()
        self.streams[stream.stream_name] = stream.stop_event


    def get_stream_from_kwargs(self, **kwargs):
        name = kwargs.get('name')
        stream_in = kwargs.get('input_stream')
        stream_out = kwargs.get('output_stream')
        stream_anomaly = kwargs.get('anomaly_stream', stream_out)
        predictor_name = kwargs.get('predictor')
        stream_type = kwargs.get('type', 'forecast')
        return RedisStream(name, self.connection_info, self.advanced_info,
                           stream_in, stream_out, stream_anomaly, predictor_name,
                           stream_type)

    def _decode(self, b_dict):
        """convert binary key/value into strings"""
        decoded = {}
        if not isinstance(b_dict, dict):
            self.log.error(f"Integration {self.name}: got unexpected data format from redis control stream {self.control_stream_name}: {b_dict}")
            return {}
        for k in b_dict:
            decoded[k.decode('utf8')] = b_dict[k].decode('utf8')
        return decoded
