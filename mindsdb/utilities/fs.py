import inspect
import os
from pathlib import Path
import tempfile
from appdirs import user_data_dir


def create_directory(path):
    path = Path(path)
    path.mkdir(mode=0o777, exist_ok=True, parents=True)


def get_root_path():
    mindsdb_path = user_data_dir('mindsdb', 'mindsdb')
    return os.path.join(mindsdb_path, 'var/')


def get_or_create_data_dir():
    data_dir = user_data_dir('mindsdb', 'mindsdb')
    mindsdb_data_dir = os.path.join(data_dir, 'var/')

    if os.path.exists(mindsdb_data_dir) is False:
        create_directory(mindsdb_data_dir)

    try:
        assert os.path.exists(mindsdb_data_dir)
        assert os.access(mindsdb_data_dir, os.W_OK) is True
    except Exception:
        raise Exception('MindsDB storage directory does not exist and could not be created')

    return mindsdb_data_dir


def create_dirs_recursive(path):
    if isinstance(path, dict):
        for p in path.values():
            create_dirs_recursive(p)
    elif isinstance(path, str):
        create_directory(path)
    else:
        raise ValueError(f'Wrong path: {path}')


def create_process_mark(folder='learn'):
    if os.name == 'posix':
        p = Path(tempfile.gettempdir()).joinpath(f'mindsdb/processes/{folder}/')
        p.mkdir(parents=True, exist_ok=True)
        p.joinpath(f'{os.getpid()}').touch()


def delete_process_mark(folder='learn'):
    if os.name == 'posix':
        p = Path(tempfile.gettempdir()).joinpath(f'mindsdb/processes/{folder}/').joinpath(f'{os.getpid()}')
        if p.exists():
            p.unlink()
