import json
import kafka

from threading import Thread
from mindsdb.integrations.base import StreamIntegration
from mindsdb.streams.kafka.kafka_stream import KafkaStream
from mindsdb.interfaces.storage.db import session, Stream
from mindsdb.interfaces.database.integrations import get_db_integration


class KafkaConnectionChecker:
    def __init__(self, **kwargs):
        self.connection_info = kwargs.get('connection')
        self.advanced_info = kwargs.get('advanced', {}).get('common', {})
        self.connection_params = {}
        self.connection_params.update(self.connection_info)
        self.connection_params.update(self.advanced_info)

    def _get_connection(self):
        return kafka.KafkaAdminClient(**self.connection_params)
    def check_connection(self):
        try:
            client = self._get_connection()
            client.close()
            return True
        except Exception:
            return False


class Kafka(StreamIntegration, KafkaConnectionChecker):
    def __init__(self, config, name):
        StreamIntegration.__init__(self, config, name)
        integration_info = get_db_integration(self.name, self.company_id)
        self.connection_info = integration_info.get('connection')
        self.advanced_info = integration_info.get('advanced', {})
        self.advanced_common = self.advanced_info.get('common', {})
        self.connection_params = {}
        self.connection_params.update(self.connection_info)
        self.connection_params.update(self.advanced_common)

        self.control_topic_name = integration_info.get('topic', None)
        self.client = self._get_connection()

    def start(self):
        Thread(target=Kafka.work, args=(self, )).start()

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
        if self.control_topic_name is not None:
            self.consumer = kafka.KafkaConsumer(**self.connection_params, **self.advanced_info.get('consumer', {}))

            self.consumer.subscribe([self.control_topic_name])
            self.log.error(f"Integration {self.name}: subscribed  to {self.control_topic_name} kafka topic")
        else:
            self.consumer = None
            self.log.error(f"Integration {self.name}: worked mode - DB only.")

        while not self.stop_event.wait(0.5):
            try:
                # break if no record about this integration has found in db
                if not self.exist_in_db():
                    self.delete_all_streams()
                    break
                self.start_stored_streams()
                self.stop_deleted_streams()
                if self.consumer is not None:
                    try:
                        msg_str = next(self.consumer)

                        stream_params = json.loads(msg_str.value)
                        stream = self.get_stream_from_kwargs(**stream_params)
                        stream.start()
                        # store created stream in database
                        self.store_stream(stream)
                    except StopIteration:
                        pass
            except Exception as e:
                self.log.error(f"Integration {self.name}: {e}")

        # received exit event
        self.consumer.close()
        self.stop_streams()
        session.close()
        self.log.error(f"Integration {self.name}: exiting...")

    def store_stream(self, stream):
        """Stories a created stream."""
        stream_rec = Stream(name=stream.stream_name, connection_params=self.connection_params, advanced_params=self.advanced_info,
                            _type=stream._type, predictor=stream.predictor,
                            integration=self.name, company_id=self.company_id,
                            stream_in=stream.stream_in_name, stream_out=stream.stream_out_name,
                            stream_anomaly=stream.stream_anomaly_name)
        session.add(stream_rec)
        session.commit()
        self.streams[stream.stream_name] = stream.stop_event

    def get_stream_from_kwargs(self, **kwargs):
        name = kwargs.get('name')
        topic_in = kwargs.get('input_stream')
        topic_out = kwargs.get('output_stream')
        topic_anomaly = kwargs.get('anomaly_stream', topic_out)
        predictor_name = kwargs.get('predictor')
        stream_type = kwargs.get('type', 'forecast')
        return KafkaStream(name, self.connection_params, self.advanced_info,
                           topic_in, topic_out, topic_anomaly,
                           predictor_name, stream_type)
