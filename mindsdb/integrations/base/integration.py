import os
from abc import ABC, abstractmethod
from mindsdb.utilities.config import Config, STOP_THREADS_EVENT
from mindsdb.interfaces.storage.db import session, Stream
from mindsdb.utilities.log import log

class Integration(ABC):
    def __init__(self, config, name):
        self.config = config
        self.name = name
        self.mindsdb_database = config['api']['mysql']['database']

    @abstractmethod
    def setup(self):
        pass

    @abstractmethod
    def _query(self, query, fetch=False):
        pass

    @abstractmethod
    def register_predictors(self, model_data_arr):
        pass

    @abstractmethod
    def unregister_predictor(self, name):
        pass


class StreamIntegration(Integration):
    """Base abstract class for stream integrations like redis and kafka."""
    def __init__(self, config, name):
        Integration.__init__(self, config, name)
        self.streams = {}
        self.company_id = os.environ.get('MINDSDB_COMPANY_ID', None)
        self.stop_event = STOP_THREADS_EVENT
        self.log = log

    def get_stream_from_db(self, db_record):
        kwargs = {"type": db_record._type,
                  "name": db_record.name,
                  "predictor": db_record.predictor,
                  "input_stream": db_record.stream_in,
                  "output_stream": db_record.stream_out,
                  "anomaly_stream": db_record.stream_anomaly}
        return self.get_stream_from_kwargs(**kwargs)

    def exist_in_db(self):
        return self.name in Config()['integrations']

    def delete_stream(self, predictor):
        """Deletes stream from database and stops it work by
        setting up a special threading.Event flag."""
        stream_name = f"{self.name}_{predictor}"
        self.log.debug(f"Integration {self.name}: deleting {stream_name}")
        session.query(Stream).filter_by(company_id=self.company_id, integration=self.name, name=stream_name).delete()
        session.commit()
        if stream_name in self.streams:
            self.streams[stream_name].set()
            del self.streams[stream_name]

    def delete_all_streams(self):
        for stream in self.streams.copy():
            self.streams[stream].set()
            del self.streams[stream]
        session.query(Stream).filter_by(company_id=self.company_id, integration=self.name).delete()
        session.commit()

    def stop_streams(self):
        for stream in self.streams:
            self.streams[stream].set()

    def stop_deleted_streams(self):
        existed_streams = session.query(Stream).filter_by(company_id=self.company_id, integration=self.name)
        actual_streams = [x.name for x in existed_streams]


        for stream in self.streams.copy():
            if stream not in actual_streams:
                # this stream is still running but it has been deleted from database.
                # need to stop it.
                self.log.error(f"INTEGRATION {self.name}: deleting {stream} stream.")
                self.streams[stream].set()
                del self.streams[stream]

    def setup(self):
        """Launches streams stored in db and
        Launches a worker in separate thread which waits
        data from control stream and creates a particular Streams."""

        self.start_stored_streams()
        # launch worker which reads control steam and spawn streams
        self.start()

    @abstractmethod
    def start_stored_streams(self):
        pass

    @abstractmethod
    def work(self):
        pass

    @abstractmethod
    def start(self):
        pass

    def _query(self, query, fetch=False):
        pass

    def register_predictors(self, model_data_arr):
        pass

    def unregister_predictor(self, name):
        pass
