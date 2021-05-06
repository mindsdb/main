from mindsdb.utilities.config import Config
from mindsdb.api.mongo.server import run_server
from mindsdb.utilities.log import initialize_log


def start(verbose=False):
    config = Config()

    initialize_log(config, 'mongodb', wrap_print=True)

    run_server(config)
