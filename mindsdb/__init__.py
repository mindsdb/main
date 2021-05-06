import os
import sys
import json

from mindsdb.__about__ import __package_name__ as name, __version__   # noqa
from mindsdb.utilities.fs import get_or_create_data_dir, create_dirs_recursive
from mindsdb.utilities.functions import args_parse, is_notebook
from mindsdb.__about__ import __version__ as mindsdb_version
from mindsdb.utilities.telemetry import telemetry_file_exists, disable_telemetry

is_ray_worker = False
if '-ray' in str(sys.argv):
    is_ray_worker = True

if not is_ray_worker:
    try:
        if not is_notebook():
            args = args_parse()
        else:
            args = None
    except Exception:
        # This fials in some notebooks ... check above for is_notebook is still needed because even if the exception is caught trying to read the arg still leads to failure in other notebooks... notebooks a
        args = None

    # ---- CHECK SYSTEM ----
    if not (sys.version_info[0] >= 3 and sys.version_info[1] >= 6):
        print("""
    MindsDB server requires Python >= 3.6 to run

    Once you have Python 3.6 installed you can tun mindsdb as follows:

    1. create and activate venv:
    python3.6 -m venv venv
    source venv/bin/activate

    2. install MindsDB:
    pip3 install mindsdb

    3. Run MindsDB
    python3.6 -m mindsdb

    More instructions in https://docs.mindsdb.com
        """)
        exit(1)

    # --- VERSION MODE ----
    if args is not None and args.version:
        print(f'MindsDB {mindsdb_version}')
        sys.exit(0)

    # --- MODULE OR LIBRARY IMPORT MODE ----
    if args is not None and args.config is not None:
        config_path = args.config
        with open(config_path, 'r') as fp:
            user_config = json.load(fp)
    else:
        user_config = {}
        config_path = 'absent'
    os.environ['MINDSDB_CONFIG_PATH'] = config_path

    if 'storage_dir' in user_config:
        root_storage_dir = user_config['storage_dir']
        os.environ['MINDSDB_STORAGE_DIR'] = root_storage_dir
    elif os.environ.get('MINDSDB_STORAGE_DIR') is not None:
        root_storage_dir = os.environ['MINDSDB_STORAGE_DIR']
    else:
        root_storage_dir = get_or_create_data_dir()
        os.environ['MINDSDB_STORAGE_DIR'] = root_storage_dir

    if os.path.isdir(root_storage_dir) is False:
        os.makedirs(root_storage_dir)

    if 'storage_db' in user_config:
        os.environ['MINDSDB_DB_CON'] = user_config['storage_db']
    elif os.environ.get('MINDSDB_DB_CON', '') == '':
        os.environ['MINDSDB_DB_CON'] = 'sqlite:///' + os.path.join(os.environ['MINDSDB_STORAGE_DIR'], 'mindsdb.sqlite3.db') + '?check_same_thread=False&timeout=30'

    from mindsdb.utilities.config import Config
    mindsdb_config = Config()
    create_dirs_recursive(mindsdb_config['paths'])

    os.environ['DEFAULT_LOG_LEVEL'] = os.environ.get('DEFAULT_LOG_LEVEL', 'ERROR')
    os.environ['LIGHTWOOD_LOG_LEVEL'] = os.environ.get('LIGHTWOOD_LOG_LEVEL', 'ERROR')
    os.environ['MINDSDB_STORAGE_PATH'] = mindsdb_config['paths']['predictors']

    if telemetry_file_exists(mindsdb_config['storage_dir']):
        os.environ['CHECK_FOR_UPDATES'] = '0'
        print('\n x telemetry disabled! \n')
    elif os.getenv('CHECK_FOR_UPDATES', '1').lower() in ['0', 'false', 'False'] or mindsdb_config.get('cloud', False):
        disable_telemetry(mindsdb_config['storage_dir'])
        print('\n x telemetry disabled \n')
    else:
        print('\n ✓ telemetry enabled \n')


    from mindsdb.interfaces.custom.model_interface import ModelInterface
