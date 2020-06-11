import argparse
import importlib
import atexit
from torch.multiprocessing import Process, get_start_method, set_start_method
import os
import traceback
import time
import sys

from mindsdb_server.utilities import config

print(f'Main call under name {__name__}')

def close_api_gracefully(p_arr):
    for p in p_arr:
        sys.stdout.flush()
        p.terminate()
        p.join()
        sys.stdout.flush()
        try:
            os.system('fuser -k 3306/tcp')
        except:
            pass

        try:
            os.system('fuser -k 47334/tcp')
            sys.stdout.flush()
        except:
            pass
        sys.stdout.flush()


#set_start_method('spawn')
print('Launching torch.multiprocessing: ' + str(get_start_method()))
#exit()
parser = argparse.ArgumentParser(description='CL argument for mindsdb server')
parser.add_argument('--api', type=str, default='http,mysql')
parser.add_argument('--config', type=str, default='/etc/mindsdb/config.json')

args = parser.parse_args()
config.merge(args.config)
config.special()
api_arr = args.api.split(',')

p_arr = []

try:
    os.system('fuser -k 3306/tcp')
except:
    pass

try:
    os.system('fuser -k 47334/tcp')
    sys.stdout.flush()
except:
    pass

for api in api_arr:
    print(api_arr)
    print(f'\n\n\n{api}\n\n\n')
    print(f'Starting Mindsdb {api} API !')
    try:
        start = importlib.import_module(f'mindsdb_server.api.{api}.start')
        p = Process(target=start.start)
        p.start()
        p_arr.append(p)
        print(f'Started Mindsdb {api} API ! <clap, clap, clap>')
    except Exception as e:
        close_api_gracefully(p_arr)
        print(f'Failed to start {api} API with exception {e}')
        print(traceback.format_exc())
        exit()

#while True:
#    time.sleep(1)
#    sys.stdout.flush()

atexit.register(close_api_gracefully, p_arr=p_arr)

for p in p_arr:
    p.join()