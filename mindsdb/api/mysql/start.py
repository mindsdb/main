from mindsdb.api.mysql.mysql_proxy.mysql_proxy import MysqlProxy
from mindsdb.utilities.config import Config
from mindsdb.utilities.log import initialize_log


def start(verbose=False):
    config = Config()

    initialize_log(config, 'mysql', wrap_print=True)

    MysqlProxy.startProxy()
