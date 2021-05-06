import json
import shutil
import os

import pandas as pd

import mindsdb_datasources
from mindsdb.__about__ import __version__ as mindsdb_version
from mindsdb.interfaces.model.model_interface import ModelInterface as NativeInterface
from mindsdb_datasources import FileDS, ClickhouseDS, MariaDS, MySqlDS, PostgresDS, MSSQLDS, MongoDS, SnowflakeDS, AthenaDS
from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage.db import session, Datasource, Semaphor
from mindsdb.interfaces.storage.fs import FsSotre
from mindsdb.utilities.log import log
from mindsdb.interfaces.database.integrations import get_db_integration


class DataStoreWrapper(object):
    def __init__(self, data_store, company_id=None):
        self.company_id = company_id
        self.data_store = data_store

    def __getattr__(self, name):
        def wrapper(*args, **kwargs):
            if kwargs.get('company_id') is None:
                kwargs['company_id'] = self.company_id
            return getattr(self.data_store, name)(*args, **kwargs)
        return wrapper


class DataStore():
    def __init__(self):
        self.config = Config()
        self.fs_store = FsSotre()
        self.dir = self.config['paths']['datasources']
        self.mindsdb_native = NativeInterface()

    def get_analysis(self, name, company_id=None):
        datasource_record = session.query(Datasource).filter_by(company_id=company_id, name=name).first()
        if datasource_record.analysis is None:
            return None
        analysis = json.loads(datasource_record.analysis)
        return analysis

    def start_analysis(self, name, company_id=None):
        datasource_record = session.query(Datasource).filter_by(company_id=company_id, name=name).first()
        if datasource_record.analysis is not None:
            return None
        semaphor_record = session.query(Semaphor).filter_by(company_id=company_id, entity_id=datasource_record.id, entity_type='datasource').first()
        if semaphor_record is None:
            semaphor_record = Semaphor(company_id=company_id, entity_id=datasource_record.id, entity_type='datasource', action='write')
            session.add(semaphor_record)
            session.commit()
        else:
            return
        try:
            analysis = self.mindsdb_native.analyse_dataset(ds=self.get_datasource_obj(name, raw=True, company_id=company_id), company_id=company_id)
            datasource_record = session.query(Datasource).filter_by(company_id=company_id, name=name).first()
            datasource_record.analysis = json.dumps(analysis)
            session.commit()
        except Exception as e:
            log.error(e)
        finally:
            semaphor_record = session.query(Semaphor).filter_by(company_id=company_id, entity_id=datasource_record.id, entity_type='datasource').first()
            session.delete(semaphor_record)
            session.commit()

    def get_datasources(self, name=None, company_id=None):
        datasource_arr = []
        if name is not None:
            datasource_record_arr = session.query(Datasource).filter_by(company_id=company_id, name=name)
        else:
            datasource_record_arr = session.query(Datasource).filter_by(company_id=company_id)
        for datasource_record in datasource_record_arr:
            try:
                if datasource_record.data is None:
                    continue
                datasource = json.loads(datasource_record.data)
                datasource['created_at'] = datasource_record.created_at
                datasource['updated_at'] = datasource_record.updated_at
                datasource['name'] = datasource_record.name
                datasource['id'] = datasource_record.id
                datasource_arr.append(datasource)
            except Exception as e:
                log.error(e)
        return datasource_arr

    def get_data(self, name, where=None, limit=None, offset=None, company_id=None):
        offset = 0 if offset is None else offset
        ds = self.get_datasource_obj(name, company_id=company_id)

        if limit is not None:
            # @TODO Add `offset` to the `filter` method of the datasource and get rid of `offset`
            filtered_ds = ds.filter(where=where, limit=limit + offset).iloc[offset:]
        else:
            filtered_ds = ds.filter(where=where)

        filtered_ds = filtered_ds.where(pd.notnull(filtered_ds), None)
        data = filtered_ds.to_dict(orient='records')
        return {
            'data': data,
            'rowcount': len(ds),
            'columns_names': filtered_ds.columns
        }

    def get_datasource(self, name, company_id=None):
        datasource_arr = self.get_datasources(name, company_id=company_id)
        if len(datasource_arr) == 1:
            return datasource_arr[0]
        # @TODO: Remove when db swithc is more stable, this should never happen, but good santiy check while this is kinda buggy
        elif len(datasource_arr) > 1:
            log.error('Two or more datasource with the same name, (', len(datasource_arr), ') | Full list: ', datasource_arr)
            raise Exception('Two or more datasource with the same name')
        return None

    def delete_datasource(self, name, company_id=None):
        datasource_record = Datasource.query.filter_by(company_id=company_id, name=name).first()
        session.delete(datasource_record)
        session.commit()
        self.fs_store.delete(f'datasource_{company_id}_{datasource_record.id}')
        try:
            shutil.rmtree(os.path.join(self.dir, f'{company_id}@@@@@{name}'))
        except Exception:
            pass

    def save_datasource(self, name, source_type, source, file_path=None, company_id=None):
        if source_type == 'file' and (file_path is None):
            raise Exception('`file_path` argument required when source_type == "file"')

        datasource_record = session.query(Datasource).filter_by(company_id=company_id, name=name).first()
        while datasource_record is not None:
            raise Exception(f'Datasource with name {name} already exists')

        try:
            datasource_record = Datasource(
                company_id=company_id,
                name=name,
                datasources_version=mindsdb_datasources.__version__,
                mindsdb_version=mindsdb_version
            )
            session.add(datasource_record)
            session.commit()
            datasource_record = session.query(Datasource).filter_by(company_id=company_id, name=name).first()

            ds_meta_dir = os.path.join(self.dir, f'{company_id}@@@@@{name}')
            os.mkdir(ds_meta_dir)

            if source_type == 'file':
                source = os.path.join(ds_meta_dir, source)
                shutil.move(file_path, source)
                ds = FileDS(source)

                creation_info = {
                    'class': 'FileDS',
                    'args': [source],
                    'kwargs': {}
                }

            elif get_db_integration(source_type, company_id) is not None:
                integration = get_db_integration(source_type, company_id)

                ds_class_map = {
                    'clickhouse': ClickhouseDS,
                    'mariadb': MariaDS,
                    'mysql': MySqlDS,
                    'postgres': PostgresDS,
                    'mssql': MSSQLDS,
                    'mongodb': MongoDS,
                    'snowflake': SnowflakeDS,
                    'athena': AthenaDS
                }

                try:
                    dsClass = ds_class_map[integration['type']]
                except KeyError:
                    raise KeyError(f"Unknown DS type: {source_type}, type is {integration['type']}")

                if dsClass is None:
                    raise Exception(f'Unsupported datasource: {source_type}, please install required dependencies!')

                if integration['type'] in ['clickhouse']:
                    creation_info = {
                        'class': dsClass.__name__,
                        'args': [],
                        'kwargs': {
                            'query': source['query'],
                            'user': integration['user'],
                            'password': integration['password'],
                            'host': integration['host'],
                            'port': integration['port']
                        }
                    }
                    ds = dsClass(**creation_info['kwargs'])

                elif integration['type'] in ['mssql', 'postgres', 'mariadb', 'mysql']:
                    creation_info = {
                        'class': dsClass.__name__,
                        'args': [],
                        'kwargs': {
                            'query': source['query'],
                            'user': integration['user'],
                            'password': integration['password'],
                            'host': integration['host'],
                            'port': integration['port']
                        }
                    }

                    if 'database' in integration:
                        creation_info['kwargs']['database'] = integration['database']

                    if 'database' in source:
                        creation_info['kwargs']['database'] = source['database']

                    ds = dsClass(**creation_info['kwargs'])

                elif integration['type'] == 'snowflake':
                    creation_info = {
                        'class': dsClass.__name__,
                        'args': [],
                        'kwargs': {
                            'query': source['query'],
                            'schema': source['schema'],
                            'warehouse': source['warehouse'],
                            'database': source['database'],
                            'host': integration['host'],
                            'password': integration['password'],
                            'user': integration['user'],
                            'account': integration['account']
                        }
                    }

                    ds = dsClass(**creation_info['kwargs'])

                elif integration['type'] == 'mongodb':
                    if isinstance(source['find'], str):
                        source['find'] = json.loads(source['find'])
                    creation_info = {
                        'class': dsClass.__name__,
                        'args': [],
                        'kwargs': {
                            'database': source['database'],
                            'collection': source['collection'],
                            'query': source['find'],
                            'user': integration['user'],
                            'password': integration['password'],
                            'host': integration['host'],
                            'port': integration['port']
                        }
                    }

                    ds = dsClass(**creation_info['kwargs'])

                elif integration['type'] == 'athena':
                    creation_info = {
                        'class': dsClass.__name__,
                        'args': [],
                        'kwargs': {
                            'query': source['query'],
                            'staging_dir': source['staging_dir'],
                            'database': source['database'],
                            'access_key': source['access_key'],
                            'secret_key': source['secret_key'],
                            'region_name': source['region_name']
                        }
                    }

                    ds = dsClass(**creation_info['kwargs'])
            else:
                # This probably only happens for urls
                ds = FileDS(source)
                creation_info = {
                    'class': 'FileDS',
                    'args': [source],
                    'kwargs': {}
                }

            df = ds.df

            if '' in df.columns or len(df.columns) != len(set(df.columns)):
                shutil.rmtree(ds_meta_dir)
                raise Exception('Each column in datasource must have unique non-empty name')

            datasource_record.creation_info = json.dumps(creation_info)
            datasource_record.data = json.dumps({
                'source_type': source_type,
                'source': source,
                'row_count': len(df),
                'columns': [dict(name=x) for x in list(df.keys())]
            })

            self.fs_store.put(f'{company_id}@@@@@{name}', f'datasource_{company_id}_{datasource_record.id}', self.dir)
            session.commit()

        except Exception as e:
            log.error(f'Error creating datasource {name}, exception: {e}')
            try:
                self.delete_datasource(name, company_id=company_id)
            except Exception:
                pass
            raise e

        return self.get_datasource_obj(name, raw=True, company_id=company_id), name

    def get_datasource_obj(self, name, raw=False, id=None, company_id=None):
        try:
            datasource_record = session.query(Datasource).filter_by(company_id=company_id, name=name).first()

            self.fs_store.get(f'{company_id}@@@@@{name}', f'datasource_{company_id}_{datasource_record.id}', self.dir)
            creation_info = json.loads(datasource_record.creation_info)
            if raw:
                return creation_info
            else:
                return eval(creation_info['class'])(*creation_info['args'], **creation_info['kwargs'])
        except Exception as e:
            log.error(f'Error getting datasource {name}, exception: {e}')
            return None
