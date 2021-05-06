from bson.int64 import Int64
from collections import OrderedDict

from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers
from mindsdb.interfaces.database.integrations import get_db_integrations


class Responce(Responder):
    when = {'find': helpers.is_true}

    def result(self, query, request_env, mindsdb_env, session):
        models = mindsdb_env['mindsdb_native'].get_models()
        model_names = [x['name'] for x in models]
        table = query['find']
        where_data = query.get('filter', {})
        if table == 'predictors':
            data = [{
                'name': x['name'],
                'status': x['status'],
                'accuracy': str(x['accuracy']) if x['accuracy'] is not None else None,
                'predict': ', '.join(x['predict']),
                'select_data_query': '',
                'external_datasource': '',
                'training_options': ''
            } for x in models]
        elif table in model_names:
            # prediction
            model = mindsdb_env['mindsdb_native'].get_model_data(name=query['find'])

            columns = []
            columns += model['columns']
            columns += [f'{x}_original' for x in model['predict']]
            for col in model['predict']:
                if model['data_analysis_v2'][col]['typing']['data_type'] == 'Numeric':
                    columns += [f"{col}_min", f"{col}_max"]
                columns += [f"{col}_confidence"]
                columns += [f"{col}_explain"]

            columns += ['when_data', 'select_data_query', 'external_datasource']

            where_data_list = where_data if isinstance(where_data, list) else [where_data]
            for statement in where_data_list:
                if isinstance(statement, dict):
                    for key in statement:
                        if key not in columns:
                            columns.append(key)

            datasource = where_data
            if 'select_data_query' in where_data:
                integrations = get_db_integrations(mindsdb_env['company_id']).keys()
                connection = where_data.get('connection')
                if connection is None:
                    if 'default_mongodb' in integrations:
                        connection = 'default_mongodb'
                    else:
                        for integration in integrations:
                            if integration.startswith('mongodb_'):
                                connection = integration
                                break

                if connection is None:
                    raise Exception("Can't find connection from which fetch data")

                ds_name = 'temp'

                ds, ds_name = mindsdb_env['data_store'].save_datasource(
                    name=ds_name,
                    source_type=connection,
                    source=where_data['select_data_query']
                )
                datasource = mindsdb_env['data_store'].get_datasource_obj(ds_name, raw=True)


            if 'external_datasource' in where_data:
                ds_name = where_data['external_datasource']
                if mindsdb_env['data_store'].get_datasource(ds_name) is None:
                    raise Exception(f"Datasource {ds_name} not exists")
                datasource = mindsdb_env['data_store'].get_datasource_obj(ds_name, raw=True)

            if isinstance(datasource, OrderedDict):
                datasource = dict(datasource)

            prediction = mindsdb_env['mindsdb_native'].predict(table, 'dict&explain', when_data=datasource)
            if 'select_data_query' in where_data:
                mindsdb_env['data_store'].delete_datasource(ds_name)

            pred_dict_arr, explanations = prediction

            predicted_columns = model['predict']

            data = []
            keys = [k for k in pred_dict_arr[0] if k in columns]
            min_max_keys = []
            for col in predicted_columns:
                if model['data_analysis_v2'][col]['typing']['data_type'] == 'Numeric':
                    min_max_keys.append(col)

            for i in range(len(pred_dict_arr)):
                row = {}
                explanation = explanations[i]
                for key in keys:
                    row[key] = pred_dict_arr[i][key]

                for key in predicted_columns:
                    row[key + '_confidence'] = explanation[key]['confidence']
                    row[key + '_explain'] = explanation[key]
                for key in min_max_keys:
                    row[key + '_min'] = min(explanation[key]['confidence_interval'])
                    row[key + '_max'] = max(explanation[key]['confidence_interval'])
                data.append(row)

        else:
            # probably wrong table name. Mongo in this case returns empty data
            data = []

        if 'projection' in query and len(data) > 0:
            true_filter = []
            false_filter = []
            for key, value in query['projection'].items():
                if helpers.is_true(value):
                    true_filter.append(key)
                else:
                    false_filter.append(key)

            keys = list(data[0].keys())
            del_id = '_id' in false_filter
            if len(true_filter) > 0:
                for row in data:
                    for key in keys:
                        if key != '_id':
                            if key not in true_filter:
                                del row[key]
                        elif del_id:
                            del row[key]
            else:
                for row in data:
                    for key in false_filter:
                        if key in row:
                            del row[key]

        db = mindsdb_env['config']['api']['mongodb']['database']

        cursor = {
            'id': Int64(0),
            'ns': f"{db}.$cmd.{query['find']}",
            'firstBatch': data
        }
        return {
            'cursor': cursor,
            'ok': 1
        }


responder = Responce()
