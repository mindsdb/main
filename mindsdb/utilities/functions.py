import argparse
import datetime


def args_parse():
    parser = argparse.ArgumentParser(description='CL argument for mindsdb server')
    parser.add_argument('--api', type=str, default=None)
    parser.add_argument('--config', type=str, default=None)
    parser.add_argument('--verbose', action='store_true')
    parser.add_argument('--no_studio', action='store_true')
    parser.add_argument('-v', '--version', action='store_true')
    return parser.parse_args()

def cast_row_types(row, field_types):
    '''
    '''
    keys = [x for x in row.keys() if x in field_types]
    for key in keys:
        t = field_types[key]
        if t == 'Timestamp' and isinstance(row[key], (int, float)):
            timestamp = datetime.datetime.utcfromtimestamp(row[key])
            row[key] = timestamp.strftime('%Y-%m-%d %H:%M:%S')
        elif t == 'Date' and isinstance(row[key], (int, float)):
            timestamp = datetime.datetime.utcfromtimestamp(row[key])
            row[key] = timestamp.strftime('%Y-%m-%d')
        elif t == 'Int' and isinstance(row[key], (int, float, str)):
            try:
                print(f'cast {row[key]} to {int(row[key])}')
                row[key] = int(row[key])
            except Exception:
                pass

def is_notebook():
    try:
        shell = get_ipython().__class__.__name__
        if shell == 'ZMQInteractiveShell':
            return True   # Jupyter notebook or qtconsole
        elif shell == 'TerminalInteractiveShell':
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False      # Probably standard Python interpreter
