import time

import pandas as pd
from moz_sql_parser import format

from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.datanode import DataNode


class IntegrationDataNode(DataNode):
    type = 'integration'

    def __init__(self, integration_name, data_store):
        self.integration_name = integration_name
        self.data_store = data_store

    def getType(self):
        return self.type

    def getTables(self):
        return []

    def hasTable(self, tableName):
        return True

    def getTableColumns(self, tableName):
        return []

    def select(self, table=None, columns=None, where=None, where_data=None, order_by=None, group_by=None, came_from=None):
        has_where = isinstance(where, (dict, list)) and len(where) > 0

        if isinstance(where, dict):
            where = [where]

        if isinstance(where, list):
            for el in where:
                if isinstance(el, dict):
                    for key in el:
                        if isinstance(el[key], list) and len(el[key]) > 0 and isinstance(el[key][0], str) and '.' in el[key][0]:
                            el[key][0] = el[key][0][el[key][0].find('.') + 1:]
            where = {'and': where}

        format_data = {
            'from': table,
            'select': columns
        }
        if has_where:
            format_data['where'] = where

        query = format(format_data)

        ds, ds_name = self.data_store.save_datasource(f'temp_ds_{int(time.time()*100)}', self.integration_name, {'query': query})
        dso = self.data_store.get_datasource_obj(ds_name)

        data = dso.df.to_dict(orient='records')

        for column_name in dso.df.columns:
            if pd.core.dtypes.common.is_datetime_or_timedelta_dtype(dso.df[column_name]):
                pass_data = dso.df[column_name].dt.to_pydatetime()
                for i, rec in enumerate(data):
                    rec[column_name] = pass_data[i].timestamp()

        self.data_store.delete_datasource(ds_name)

        return data
