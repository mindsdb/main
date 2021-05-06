import os
import threading
import tempfile
import re
import multipart

import mindsdb
from dateutil.parser import parse
from flask import request, send_file
from flask_restx import Resource, abort     # 'abort' using to return errors as json: {'message': 'error text'}
from flask import current_app as ca

from mindsdb.utilities.log import log
from mindsdb.api.http.namespaces.configs.datasources import ns_conf
from mindsdb.api.http.namespaces.entitites.datasources.datasource import (
    datasource_metadata,
    put_datasource_params
)
from mindsdb.api.http.namespaces.entitites.datasources.datasource_data import (
    get_datasource_rows_params,
    datasource_rows_metadata
)
from mindsdb.api.http.namespaces.entitites.datasources.datasource_files import (
    put_datasource_file_params
)
from mindsdb.api.http.namespaces.entitites.datasources.datasource_missed_files import (
    datasource_missed_files_metadata,
    get_datasource_missed_files_params
)
from mindsdb.interfaces.database.integrations import get_db_integration


def parse_filter(key, value):
    result = re.search(r'filter(_*.*)\[(.*)\]', key)
    operator = result.groups()[0].strip('_') or 'like'
    field = result.groups()[1]
    operators_map = {
        'like': 'like',
        'in': 'in',
        'nin': 'not in',
        'gt': '>',
        'lt': '<',
        'gte': '>=',
        'lte': '<=',
        'eq': '=',
        'neq': '!='
    }
    if operator not in operators_map:
        return None
    operator = operators_map[operator]
    return [field, operator, value]


@ns_conf.route('/')
class DatasourcesList(Resource):
    @ns_conf.doc('get_datasources_list')
    @ns_conf.marshal_list_with(datasource_metadata)
    def get(self):
        '''List all datasources'''
        return request.default_store.get_datasources()


@ns_conf.route('/<name>')
@ns_conf.param('name', 'Datasource name')
class Datasource(Resource):
    @ns_conf.doc('get_datasource')
    @ns_conf.marshal_with(datasource_metadata)
    def get(self, name):
        '''return datasource metadata'''
        ds = request.default_store.get_datasource(name)
        if ds is not None:
            return ds
        return '', 404

    @ns_conf.doc('delete_datasource')
    def delete(self, name):
        '''delete datasource'''
        try:
            request.default_store.delete_datasource(name)
        except Exception as e:
            log.error(e)
            abort(400, str(e))
        return '', 200

    @ns_conf.doc('put_datasource', params=put_datasource_params)
    @ns_conf.marshal_with(datasource_metadata)
    def put(self, name):
        '''add new datasource'''
        data = {}

        def on_field(field):
            name = field.field_name.decode()
            value = field.value.decode()
            data[name] = value

        file_object = None

        def on_file(file):
            nonlocal file_object
            data['file'] = file.file_name.decode()
            file_object = file.file_object

        temp_dir_path = tempfile.mkdtemp(prefix='datasource_file_')

        if request.headers['Content-Type'].startswith('multipart/form-data'):
            parser = multipart.create_form_parser(
                headers=request.headers,
                on_field=on_field,
                on_file=on_file,
                config={
                    'UPLOAD_DIR': temp_dir_path.encode(),    # bytes required
                    'UPLOAD_KEEP_FILENAME': True,
                    'UPLOAD_KEEP_EXTENSIONS': True,
                    'MAX_MEMORY_FILE_SIZE': 0
                }
            )

            while True:
                chunk = request.stream.read(8192)
                if not chunk:
                    break
                parser.write(chunk)
            parser.finalize()
            parser.close()

            if file_object is not None and not file_object.closed:
                file_object.close()
        else:
            data = request.json

        if 'query' in data:
            integration_id = request.json['integration_id']
            integration = get_db_integration(integration_id, request.company_id)
            if integration is None:
                abort(400, f"{integration_id} integration doesn't exist")

            if integration['type'] == 'mongodb':
                data['find'] = data['query']

            ds_obj, ds_name = request.default_store.save_datasource(name, integration_id, data)
            os.rmdir(temp_dir_path)
            return request.default_store.get_datasource(ds_name)

        ds_name = data['name'] if 'name' in data else name
        source = data['source'] if 'source' in data else name
        source_type = data['source_type']

        if source_type == 'file':
            file_path = os.path.join(temp_dir_path, data['file'])
        else:
            file_path = None

        ds_obj, ds_name = request.default_store.save_datasource(ds_name, source_type, source, file_path)
        os.rmdir(temp_dir_path)

        return request.default_store.get_datasource(ds_name)


def analyzing_thread(name, default_store):
    try:
        from mindsdb.interfaces.storage.db import session
        analysis = default_store.start_analysis(name)
        session.close()
    except Exception as e:
        log.error(e)


@ns_conf.route('/<name>/analyze')
@ns_conf.param('name', 'Datasource name')
class Analyze(Resource):
    @ns_conf.doc('analyse_dataset')
    def get(self, name):
        analysis = request.default_store.get_analysis(name)
        if analysis is not None:
            return analysis, 200


        ds = request.default_store.get_datasource(name)
        if ds is None:
            log.error('No valid datasource given')
            abort(400, 'No valid datasource given')

        x = threading.Thread(target=analyzing_thread, args=(name, request.default_store))
        x.start()
        return {'status': 'analyzing'}, 200


@ns_conf.route('/<name>/analyze_refresh')
@ns_conf.param('name', 'Datasource name')
class Analyze2(Resource):
    @ns_conf.doc('analyze_refresh_dataset')
    def get(self, name):
        analysis = request.default_store.get_analysis(name)
        if analysis is not None:
            return analysis, 200

        ds = request.default_store.get_datasource(name)
        if ds is None:
            log.error('No valid datasource given')
            abort(400, 'No valid datasource given')

        x = threading.Thread(target=analyzing_thread, args=(name, request.default_store))
        x.start()
        return {'status': 'analyzing'}, 200


@ns_conf.route('/<name>/data/')
@ns_conf.param('name', 'Datasource name')
class DatasourceData(Resource):
    @ns_conf.doc('get_datasource_data', params=get_datasource_rows_params)
    @ns_conf.marshal_with(datasource_rows_metadata)
    def get(self, name):
        '''return data rows'''
        ds = request.default_store.get_datasource(name)
        if ds is None:
            abort(400, 'No valid datasource given')

        params = {
            'page[size]': None,
            'page[offset]': None
        }
        where = []
        for key, value in request.args.items():
            if key == 'page[size]':
                params['page[size]'] = int(value)
            if key == 'page[offset]':
                params['page[offset]'] = int(value)
            elif key.startswith('filter'):
                param = parse_filter(key, value)
                if param is None:
                    abort(400, f'Not valid filter "{key}"')
                where.append(param)

        data_dict = request.default_store.get_data(name, where, params['page[size]'], params['page[offset]'])
        return data_dict, 200


@ns_conf.route('/<name>/download')
@ns_conf.param('name', 'Datasource name')
class DatasourceMissedFilesDownload(Resource):
    @ns_conf.doc('get_datasource_download')
    def get(self, name):
        '''download uploaded file'''
        ds = request.default_store.get_datasource(name)
        if not ds:
            abort(404, "{} not found".format(name))
        if not os.path.exists(ds['source']):
            abort(404, "{} not found".format(name))

        return send_file(os.path.abspath(ds['source']), as_attachment=True)
