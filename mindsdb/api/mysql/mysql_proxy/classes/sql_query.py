"""
*******************************************************
 * Copyright (C) 2017 MindsDB Inc. <copyright@mindsdb.com>
 *
 * This file is part of MindsDB Server.
 *
 * MindsDB Server can not be copied and/or distributed without the express
 * permission of MindsDB Inc
 *******************************************************
"""

import re
import traceback

from moz_sql_parser import parse

from mindsdb.api.mysql.mysql_proxy.classes.com_operators import join_keywords, binary_ops, unary_ops, operator_map
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import TYPES
from mindsdb.api.mysql.mysql_proxy.utilities import log
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import ERR
from mindsdb.interfaces.ai_table.ai_table import AITableStore


class TableWithoutDatasourceException(Exception):
    def __init__(self, tableName='?'):
        Exception.__init__(self, f'Each table in FROM statement mush have explicit specified datasource. Table {tableName} hasnt.')


class UndefinedColumnTableException(Exception):
    def __init__(self, columnName='?'):
        Exception.__init__(self, f'Cant find confirm table for column {columnName}')


class DuplicateTableNameException(Exception):
    def __init__(self, tableName='?'):
        Exception.__init__(self, f'Duplicate table name {tableName}')


class NotImplementedError(Exception):
    pass


class SqlError(Exception):
    pass


class SQLQuery():
    raw = ''
    struct = {}
    result = None

    def __init__(self, sql, integration=None, database=None):
        # parse
        self.integration = integration
        self.database = database

        self.ai_table = None

        # 'offset x, y' - specific just for mysql, parser dont understand it
        sql = re.sub(r'\n?limit([\n\d\s]*),([\n\d\s]*)', ' limit \g<1> offset \g<1> ', sql)

        self.raw = sql
        self._parseQuery(sql)

        # prepare
        self._prepareQuery()

    def fetch(self, datahub, view='list'):
        try:
            self.datahub = datahub
            self._fetchData()
            data = self._processData()
            if view == 'dict':
                self.result = self._makeDictResultVeiw(data)
            elif view == 'list':
                self.result = self._makeListResultVeiw(data)
        except (TableWithoutDatasourceException,
                UndefinedColumnTableException,
                DuplicateTableNameException,
                NotImplementedError,
                SqlError,
                Exception) as e:
            # TODO determine different codes for errors types
            log.error(
                f'ERROR while fetching data for query: {self.raw}\n'
                f'{traceback.format_exc()}\n'
                f'{e}'
            )
            return {
                'success': False,
                'error_code': ERR.ER_SYNTAX_ERROR,
                'msg': str(e)
            }

        return {
            'success': True,
            'result': self.result
        }

    def _format_from_statement(self, s):
        """ parser can return FROM statement in different views:
            `... from xxx.zzz` -> 'xxx.zzz'
            `... from xxx.zzz a` -> {'value': 'xxx.zzz', 'name': 'a'}
            `... from xxx.yyy a left join xxx.zzz b on a.id = b.id`
                    -> [{'value': 'xxx.yyy', 'name': 'a'},
                        {'left join': {'name': 'b', 'value': 'xxx.zzz'}, 'on': {'eq': ['a.id', 'b.id']}}]
            This function do:
                1. replace string view 'xxx.zzz' to {'value': 'xxx.zzz', 'name': 'zzz'}
                2. if exists db info, then replace 'zzz' to 'db.zzz'
                3. if database marks (as _clickhouse or _mariadb) in datasource name, than do:
                    {'value': 'xxx.zzz_mariadb', 'name': 'a'}
                    -> {'value': 'xxx.zzz', 'name': 'a', source: 'mariadb'}
        """
        database = self.database
        if isinstance(s, str):
            if '.' in s:
                s = {
                    'name': s.split('.')[-1],
                    'value': s
                }
            elif database is not None:
                s = {
                    'name': s,
                    'value': f'{database}.{s}'
                }
            else:
                raise SqlError('table without datasource %s ' % s)
        elif isinstance(s, dict):
            # TODO case when dot in table name
            if 'value' in s and 'name' in s:
                if '.' not in s['value'] and database is not None:
                    s['value'] = f"{database}.{s['value']}"
                elif '.' not in s['value']:
                    raise SqlError('table without datasource %s ' % s['value'])
            elif 'left join' in s:
                s['left join'] = self._format_from_statement(s['left join'])
            elif 'right join' in s:
                s['right join'] = self._format_from_statement(s['right join'])
            elif 'join' in s:
                s['join'] = self._format_from_statement(s['join'])
            else:
                raise SqlError('Something wrong in query parsing process')
        return s

    def _parseQuery(self, sql):
        self.struct = parse(sql)

        if 'limit' in self.struct:
            limit = self.struct.get('limit')
            if isinstance(limit, int) is False:
                raise SqlError('LIMIT must be integer')
            if limit < 0:
                raise SqlError('LIMIT must not be negative')

        selectStatement = self.struct.get('select')
        if isinstance(selectStatement, dict):
            self.struct['select'] = [selectStatement]

        fromStatements = self.struct.get('from')
        if isinstance(fromStatements, list) is False:
            fromStatements = [fromStatements]

        self.struct['from'] = [self._format_from_statement(x) for x in fromStatements]

        orderby = self.struct.get('orderby')
        if isinstance(orderby, dict):
            self.struct['orderby'] = [orderby]

    def _prepareQuery(self):
        # prepare "from" statement
        from_statement = self.struct.get('from')

        # prepared selected tables
        self.tables_select = []

        if from_statement:
            for statement in from_statement:
                join = None
                table = None

                if isinstance(statement, dict) and 'on' in statement:
                    # maybe join
                    join_type = None
                    for join in join_keywords:
                        if join in statement:
                            join_type = join
                            break

                    if join_type is None:
                        raise NotImplementedError('Unknown join type')

                    table = statement[join_type]
                    join = dict(
                        type=join_type,
                        value=statement['on']
                    )
                else:
                    table = statement

                table_alias = None
                if isinstance(table, dict):
                    table_alias = table.get('name')
                    table = table['value']

                self.tables_select.append(dict(
                    name=table,
                    alias=table_alias,
                    join=join
                ))

        # create tables index
        self.tables_index = {}

        for table in self.tables_select:
            table_rec = dict(
                name=table['name'],     # true name of table in database
                fields=[],              # list of used field
                mongo_query=[],         # conditions that dedicated to table
                conditions=[]
            )
            # self.tables_sequence.append(table['name'])

            self.tables_index[table['name']] = table_rec
            if table['alias'] is not None:
                self.tables_index[table['alias']] = table_rec

        # analyze FROM statement:
        # - get tables fields
        for table in self.tables_select:
            join = table['join']
            if join:
                ret = self._analyse_condition(self._condition_get_tables, join['value'])
                # add field to tables fields
                for table_name, field_name in ret['fields']:
                    self._add_table_field(table_name, field_name)

            if join:
                ret = self._analyse_condition(self._condition_make_comand_stack, join['value'])

                commands = None
                if ret['type'] == 'commands':
                    commands = ret['commands']
                join['commands'] = commands

        # analyze SELECT statement:
        #  - get select representation
        #  - get tables fields
        select_statement = self.struct.get('select')
        if select_statement is None:
            raise NotImplementedError('need "select" statement to analyze')

        self.select_columns = []

        # if meet wildcard character without specified table, then extrapolate it on all known tables
        extended_select_statement = []
        for column in select_statement:
            if column == '*':
                for table in self.tables_select:
                    extended_select_statement.append({'value': '{table}.*'.format(table=table['alias'])})
            else:
                extended_select_statement.append(column)
        select_statement = extended_select_statement

        for column in select_statement:
            field = column['value']
            if isinstance(field, dict):
                # TODO implement functions and literals in select
                raise NotImplementedError('column must be field %s' % field)

            # mysql variables
            if field.startswith('@@'):
                table_name = None
                field_name = field
            else:
                table_name, field_name = self._get_field(field)

            caption = column.get('name', field_name)

            self.select_columns.append(dict(
                table=table_name,
                field=field_name,
                caption=caption
            ))

            # add field to tables fields
            self._add_table_field(table_name, field_name)

        # analyze WHERE statement:
        # - get tables fields
        # - get tables conditions
        where_statement = self.struct.get('where')
        self.where_conditions = []
        if where_statement is not None:
            if not isinstance(where_statement, dict):
                raise NotImplementedError('unknown filter: %s' % where_statement)
            key = list(where_statement.keys())[0]
            # trying to get sets of conditions for every table (independent from other tables)
            # current strategy: split condition by AND or take it whole

            if key == 'and':
                conditions = where_statement[key]
            else:
                conditions = [where_statement]

            for condition in conditions:
                ret = self._analyse_condition(self._condition_get_tables, condition)
                tables = ret['tables']
                # only 1 table takes part in condition
                if len(tables) == 1:
                    table_name = next(iter(tables))

                    mongo_condition = self._analyse_condition(self._condition_make_mongo_query, condition)
                    if 'query' in mongo_condition:
                        self.tables_index[table_name]['mongo_query'].append(mongo_condition['query'])
                        self.tables_index[table_name]['conditions'].append(condition)

                # add field to tables fields
                for table_name, field_name in ret['fields']:
                    self._add_table_field(table_name, field_name)

                ret = self._analyse_condition(self._condition_make_comand_stack, condition)
                if ret['type'] == 'commands':
                    self.where_conditions.append(dict(
                        tables=tables,
                        commands=ret['commands']
                    ))

        # analyze ORDER statement:
        # - get sort rules
        order_statement = self.struct.get('orderby')
        self.order_rules = []
        if order_statement is not None:
            for item in order_statement:
                direction = item.get('sort', 'asc')
                field = item['value']
                if isinstance(field, int):
                    if field > len(self.select_columns) or field < 1:
                        raise SqlError('Not found sorting field %s' % field)
                    fieldx = self.select_columns[field - 1]
                    field = dict(
                        table=fieldx['table'],
                        field=fieldx['field']
                    )
                else:
                    fieldx = self._get_field(field)
                    field = dict(
                        table=fieldx[0],
                        field=fieldx[1]
                    )
                field['direction'] = direction
                self.order_rules.append(field)

        self.order_rules.reverse()  # sorting processed in reverse order
        # print(self.order_rules)

    def _fetchData(self):
        self.table_data = {}

        prev_table_name = None

        # TODO calculate statements for join
        for tablenum, table in enumerate(self.tables_select):

            full_table_name = table['name']

            parts = full_table_name.split('.')
            if len(parts) == 3:
                full_table_name = '.'.join([parts[1], parts[2]])
                self.tables_index[full_table_name] = self.tables_index['.'.join(parts)]
            dn_name = parts[0]
            table_name = '.'.join(parts[1:])

            dn = self.datahub.get(dn_name)

            if dn is None:
                raise SqlError('unknown datasource %s ' % dn_name)

            if not dn.hasTable(table_name):
                raise SqlError('table not found in datasource %s ' % full_table_name)

            table_columns = dn.getTableColumns(table_name)

            table_info = self.tables_index[full_table_name]
            fields = table_info['fields']

            # FIXME removed for 'integration JOIN predictor'
            # for f in fields:
            #     if f.lower() not in [x.lower() for x in table_columns] and f != '*':
            #         raise SqlError('column %s not found in table %s ' % (f, full_table_name))

            # wildcard ? replace it
            if '*' in fields:
                fields = table_columns
                self.tables_index[full_table_name]['fields'] = fields
            new_select_columns = []
            for column in self.select_columns:
                if column['field'] == '*' and table['name'] == column['table']:
                    new_select_columns += [{
                        'caption': x,
                        'field': x,
                        'table': column['table']
                    } for x in table_columns]
                else:
                    new_select_columns.append(column)
            self.select_columns = new_select_columns

            condition = self._mongo_query_and(table_info['mongo_query'])

            # TODO if left join and missing in condition and table_num > 0 - no apply condition
            if tablenum > 0 \
               and isinstance(table['join'], dict) \
               and table['join']['type'] == 'left join':
                condition = {}

            if 'external_datasource' in condition \
                    and isinstance(condition['external_datasource']['$eq'], str) \
                    and condition['external_datasource']['$eq'] != '':
                external_datasource = condition['external_datasource']['$eq']
                result = []
                if 'select ' not in external_datasource.lower():
                    external_datasource = f'select * from {external_datasource}'
                query = SQLQuery(external_datasource, database='datasource', integration=self.integration)
                result = query.fetch(self.datahub, view='dict')
                if result['success'] is False:
                    raise Exception(result['msg'])
                data = dn.select(
                    table=table_name,
                    columns=fields,
                    where=condition,
                    where_data=result['result'],
                    came_from=table.get('source')
                )
            elif tablenum > 0 \
                    and isinstance(table['join'], dict) \
                    and table['join']['type'] in ['left join', 'join'] \
                    and dn.type == 'mindsdb':
                data = dn.select(
                    table=table_name,
                    columns=fields,
                    where=condition,
                    where_data=self.table_data[prev_table_name],
                    came_from=self.integration
                )
            else:
                data = dn.select(
                    table=table_name,
                    columns=fields,
                    where=condition if dn.getType() != 'integration' else table_info['conditions'],
                    came_from=self.integration
                )

            self.table_data[full_table_name] = data

            prev_table_name = full_table_name

    def _is_wildcard_join(self, commands):
        # FIXME that for 'table JOIN predictor'
        return True
        # if len(commands) == 3 \
        #    and commands[2].get('op') == 'eq' \
        #    and commands[1].get('field') == '*' \
        #    and commands[0].get('field') == '*':
        #     return True
        # return False

    def _resolveTableData(self, table_name):
        # if isinstance(self.table_data[table_name], ObjectID):
        #     self.table_data[table_name] = list(ray.get(self.table_data[table_name]))

        # FIXME that for 'integration JOIN predictoir'
        if len(table_name.split('.')) == 3:
            table_name = table_name[table_name.find('.') + 1: ]

        self.table_data[table_name] = list(self.table_data[table_name])
        return self.table_data[table_name]

    def _processData(self):
        # do join with "on" filter
        data = []
        table1_name = self.tables_select[0]['name']

        # FIXME that for 'integration JOIN predictoir'
        if len(table1_name.split('.')) == 3:
            self._resolveTableData(table1_name[table1_name.find('.') + 1: ])
            for row in self.table_data[table1_name[table1_name.find('.') + 1: ]]:
                data.append({table1_name: row})
        else:
            self._resolveTableData(table1_name)
            for row in self.table_data[table1_name]:
                data.append({table1_name: row})

        for i in range(1, len(self.tables_select)):
            table2 = self.tables_select[i]

            table2_join = table2['join']
            table2_name = table2['name']

            data2 = []
            for i, record in enumerate(data):
                is_joined = False
                self._resolveTableData(table2_name)
                for j, row2 in enumerate(self.table_data[table2_name]):
                    record2 = {k: v for k, v in record.items()}  # copy 1 layer
                    record2[table2_name] = row2

                    is_wildcard_join = table2_join is not None and self._is_wildcard_join(table2_join['commands'])
                    if is_wildcard_join:
                        if i == j:
                            data2.append(record2)
                            is_joined = True
                    elif (
                        table2_join is None or (
                            self._command_stack_eval(
                                table2_join['commands'],
                                record2
                            )
                        )
                    ):
                        data2.append(record2)
                        is_joined = True

                # TODO other types of join
                # LEFT JOIN
                if table2_join is not None and table2_join['type'] == "left join" and not is_joined:
                    # add empty row
                    row2 = {}
                    for field in self.tables_index[table2_name]['fields']:
                        row2[field] = None

                    record2 = {k: v for k, v in record.items()}
                    record2[table2_name] = row2
                    data2.append(record2)

            data = data2

        # do "where" filter
        if self.where_conditions:
            data2 = []
            for record in data:
                success = True

                tables = list(self.where_conditions[0]['tables'])
                db = (self.database or '').lower()

                # @TODO: Figure out why initalization is so low, for now only initialize it when needed
                # We should probably just pass this to the SQLQuery the same way we do integrations, databases, etc
                if self.ai_table is None:
                    self.ai_table = AITableStore()

                if False and len(tables) == 1 and (
                        tables[0].lower() in ['mindsdb.predictors', 'mindsdb.commands'] \
                        or db == 'mindsdb' and tables[0].lower() in ['predictors', 'commands']
                    ) is False and self.ai_table.is_ai_table(tables[0]) is False:
                    success = True
                else:
                    for cond in self.where_conditions:
                        if not self._command_stack_eval(cond['commands'], record):
                            success = False
                            break

                if success:
                    data2.append(record)
            data = data2

        # TODO do grouping

        # do ordering
        def gen_key_f(table, field):
            def fnc(record):
                return record[table][field]
            return fnc

        for rule in self.order_rules:
            reverse = (rule['direction'] == 'desc')
            data.sort(key=gen_key_f(rule['table'], rule['field']), reverse=reverse)

        # limit
        if 'limit' in self.struct:
            limit = max(self.struct.get('limit'), 0)
            data = data[:limit]

        return data

    def _makeDictResultVeiw(self, data):
        result = []

        for record in data:
            row = {}
            for col in self.columns:
                col_name = f"{col['database']}.{col['table_name']}"

                # FIXME that only for 'integration JOIN predictor'
                if col_name not in record:
                    col_name = [x for x in record.keys() if col_name in x][0]

                table_record = record[col_name]
                row[col['name']] = table_record[col['name']]
            result.append(row)

        return result

    def _makeListResultVeiw(self, data):
        result = []

        for record in data:
            row = []
            for col in self.columns:
                col_name = f"{col['database']}.{col['table_name']}"

                # FIXME that only for 'integration JOIN predictor'
                if col_name not in record:
                    col_name = [x for x in record.keys() if col_name in x][0]

                table_record = record[col_name]
                val = table_record[col['name']]
                row.append(val)
            result.append(row)

        return result

    def _add_table_field(self, table_name, field_name):
        fields = self.tables_index[table_name]['fields']
        # NOTE will be better append all fields?
        if field_name not in fields:
            fields.append(field_name)

    def _condition_get_tables(self, cache, field=None, value=None, operation=None, ret=None):
        if 'tables' not in cache:
            cache['tables'] = set()
        if 'fields' not in cache:
            cache['fields'] = []

        if field is not None:
            table_name, field_name = self._get_field(field)
            cache['tables'].add(table_name)
            cache['fields'].append([table_name, field_name])

        return cache

    def _command_stack_eval(self, commands, record):

        results = []
        for el in commands:
            elype = el['type']

            if elype == 'field':
                val = record[el['table']][el['field']]
                results.append(val)
            elif elype == 'value':
                results.append(el['value'])
            elif elype == 'op':
                op = el['op']
                length = el['len']

                args = results[-length:]
                results = results[:-length]

                opfunc = operator_map[op]
                ret = opfunc(*args)

                results.append(ret)
            else:
                raise NotImplementedError('unknown command %s' % elype)

        if len(results) != 1:
            raise SqlError('something wrong')
        return results.pop()

    def _condition_make_comand_stack(self, cache, field=None, value=None, operation=None, ret=None):
        if value is not None:
            return dict(
                type='value',
                value=value
            )

        if field:
            table_name, field_name = self._get_field(field)
            return dict(
                type='field',
                table=table_name,
                field=field_name,
            )

        if operation:
            if operation in unary_ops:
                l = 1
                ret = [ret]
            else:
                l = len(ret)

            commands = []
            for ret1 in ret:
                if ret1['type'] == 'commands':
                    commands.extend(ret1['commands'])

                else:
                    commands.append(ret1)

            commands.append(dict(
                type='op',
                op=operation,
                len=l
            ))
            return dict(
                type='commands',
                commands=commands
            )

    def _condition_make_mongo_query(self, cache, field=None, value=None, operation=None, ret=None):
        # current mongo query gen strategy:
        # make simple query field : simple filter

        # TODO: 'in' operator. maybe already works
        if operation is not None:
            query = None

            if operation == 'in':
                if len(ret) != 2:
                    raise Exception()  # TODO make exception
                    return {}
                op = '$in'
                field = ret[0].get('field')
                value = ret[1].get('value')
                query = {field: {op: value}}

            elif operation in ('missing', 'exists'):
                if ret.get('field') is not None:
                    if operation == 'missing':
                        op = '$eq'
                    else:
                        op = '$ne'

                    query = {ret['field']: {op: None}}

            elif operation in ('gt', 'lt', 'eq', 'neq'):
                # only with 1 key and 1 value
                if len(ret) != 2:
                    return {}
                    # raise SqlError('must be 2 fields %s ' % ret)
                field, value = ret
                if field.get('field') is None:
                    # swap
                    field, value = value, field

                if field.get('field') is None or value.get('value') is None:
                    return {}
                    # raise SqlError('must be 1 key and 1 value: %s ' % ret)

                ops = {
                    'gt': '$gt',
                    'lt': '$lt',
                    'gte': '$gte',
                    'lte': '$lte',
                    'eq': '$eq',
                    'neq': '$ne',
                }
                op = ops[operation]

                query = {field['field']: {op: value['value']}}

            elif operation in ('or', 'and'):
                # only subquery
                is_all_query = True
                ret_queries = []
                for q in ret:
                    if 'query' not in q:
                        is_all_query = False
                        break
                    ret_queries.append(q['query'])

                if is_all_query:
                    if operation == 'and':
                        query = self._mongo_query_and(ret_queries)

                    elif operation == 'or':
                        query = {'$or': ret_queries}

            if query:
                return dict(query=query)
            else:
                return {}

        if field is not None:
            _, field = self._get_field(field)

        return dict(
            field=field,
            value=value
        )

    def _mongo_query_and(self, queries):
        if len(queries) == 1:
            return queries[0]
        query = {}
        for q in queries:
            for k, v in q.items():
                if k not in query:
                    query[k] = {}
                if isinstance(v, list):
                    # TODO check exists of k in query, may be it should be update
                    query[k] = v
                else:
                    query[k].update(v)
        return query

    def _analyse_condition(self, fnc, condition, cache=None):
        if cache is None:
            cache = {}

        if isinstance(condition, str):
            # this is field
            return fnc(cache, field=condition)

        elif not isinstance(condition, dict):
            # this is value: int or float or list
            return fnc(cache, value=condition)
        else:
            # condition is array with one key
            key = list(condition.keys())[0]
            value = condition[key]

            if key == 'literal':
                # this is value: string
                return fnc(cache, value=value)
            elif key in unary_ops:
                # this is unary op
                ret = self._analyse_condition(fnc, value, cache)
                return fnc(cache, operation=key, ret=ret)
            elif key in binary_ops:
                # this is binary op
                ret_list = []
                for v in value:
                    ret = self._analyse_condition(fnc, v, cache)
                    ret_list.append(ret)

                return fnc(cache, operation=key, ret=ret_list)
            else:
                raise NotImplementedError('unknown operator: %s' % key)

    def _get_field(self, field):
        # get field destination
        field_name = field
        table_name = ''
        for table in self.tables_select:
            alias_prefix = f"{table['alias']}."
            table_prefix = f"{table['name'].split('.')[-1]}."
            if field.startswith(alias_prefix):
                table_name = table['name']
                field_name = field_name[len(alias_prefix):]
                break
            elif field.startswith(table_prefix):
                table_name = table['name']
                field_name = field_name[len(table_prefix):]
                break

        if table_name == '':
            if len(self.tables_select) > 1:
                raise UndefinedColumnTableException('Unable find table: %s' % field)
            table_name = self.tables_select[0]['name']
        else:
            table_name = self.tables_index[table_name]['name']

        return table_name, field_name

    @property
    def columns(self):
        result = []
        for column in self.select_columns:
            parts = column['table'].split('.')
            if len(parts) == 2:
                dn_name, table_name = parts
            elif len(parts) == 3:
                integration_name, dn_name, table_name = parts
            else:
                raise UndefinedColumnTableException('Unable find table: %s' % column['table'])
            result.append({
                'database': dn_name,
                'table_name': table_name,
                'name': column['field'],
                'alias': column['caption'],
                # NOTE all work with text-type, but if/when wanted change types to real,
                # it will need to check all types casts in BinaryResultsetRowPacket
                'type': TYPES.MYSQL_TYPE_VAR_STRING
            })
        return result
