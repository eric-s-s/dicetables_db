import sqlite3 as lite
from mongo_dicetables.connections.baseconnection import BaseConnection


# todo OMFG REFACTOR

class NonExistentColumnError(ValueError):
    pass


class SQLConnection(BaseConnection):
    def __init__(self, db_path, table_name):
        self._path = db_path
        self._connection = lite.connect(self._path)
        self._cursor = self._connection.cursor()
        self._collection = table_name
        if self._no_such_collection():
            self._set_up()
        self._in_memory = InMemoryInformation(self._cursor, self._collection)

    def _no_such_collection(self):
        command = 'pragma table_info([{}]);'.format(self._collection)
        return not self._cursor.execute(command).fetchall()

    def _set_up(self):
        command = "CREATE TABLE [{}] (_id INTEGER, PRIMARY KEY(_id))".format(self._collection)
        self._cursor.execute(command)

    def get_info(self):
        out = {
            'db': self._path,
            'collections': self._in_memory.tables,
            'current_collection': self._collection,
            'indices': self._in_memory.indices
        }
        return out

    @property
    def cursor(self):
        return self._cursor

    def is_collection_empty(self):
        if self._no_such_collection():
            return True
        command = "select count(*) from {}".format(self._collection)
        answer = self._cursor.execute(command).fetchall()
        return not answer[0][0]

    def find(self, params_dict=None, projection=None):
        inclusion_list = self._get_result_columns(projection)
        command, values = self._get_command_and_values(params_dict, inclusion_list)
        results = self._cursor.execute(command, values).fetchall()

        to_check = [make_dict(inclusion_list, value_list) for value_list in results]
        return [element for element in to_check if element is not None]

    def find_one(self, params_dict=None, projection=None):
        inclusion_list = self._get_result_columns(projection)
        command, values = self._get_command_and_values(params_dict, inclusion_list)
        results = self._cursor.execute(command, values).fetchone()
        if not results:
            return None
        return make_dict(inclusion_list, results)

    def _get_command_and_values(self, params_dict, inclusion_list):
        safe_col_names = ['[{}]'.format(col) for col in inclusion_list]
        select_string = ', '.join(safe_col_names)
        if not select_string:
            select_string = 'null'
        command_start = 'select {} from [{}]'.format(select_string, self._collection)
        try:
            command_where, values = self._get_search_params(params_dict)
        except NonExistentColumnError:
            command_start = 'select null from [{}]'.format(self._collection)
            command_where = ''
            values = []
        return command_start + command_where, values

    def _get_result_columns(self, projection):
        if not projection:
            return self._in_memory.columns
        projection_type = self._get_projection_type(projection)
        if projection_type == 'error':
            raise ValueError('Projection cannot have a mix of inclusion and exclusion.')
        elif projection_type == 'include':
            try:
                to_include = self._get_list_from_included(projection)
            except NonExistentColumnError:
                to_include = []
        else:
            to_include = self._get_list_from_excluded(projection)
        return to_include

    @staticmethod
    def _get_projection_type(projection):
        bool_list = [bool(value) for value in projection.values()]
        if True in bool_list:
            if False in bool_list:
                return 'error'
            else:
                return 'include'
        return 'exclude'

    def _get_list_from_included(self, projection):
        return [col for col in projection if self._in_memory.has_column(col)]

    def _get_list_from_excluded(self, projection):
        all_cols = self._in_memory.columns
        return [col for col in all_cols if col not in projection]

    def _get_search_params(self, params_dict):
        if params_dict is None:
            params_dict = {}
        self._raise_column_error(params_dict)
        where_vals = []
        values = []
        for col, inequality_info in params_dict.items():
            inequality_str, value = self._get_inequality_data(inequality_info)
            where_vals.append('[{}]{}?'.format(col, inequality_str))
            values.append(value)
        if not values:
            return '', []
        where_string = ' where ' + ' and '.join(where_vals)
        return where_string, values

    @staticmethod
    def _get_inequality_data(inequality_info):
        inequalities = {'$lt': '<', '$lte': '<=', '$gt': '>', '$gte': '>=', '$ne': '<>'}
        if isinstance(inequality_info, dict):
            key, value = next(iter(inequality_info.items()))
            inequality_str = inequalities[key]
        else:
            inequality_str = '='
            value = inequality_info
        return inequality_str, value

    def _raise_column_error(self, dictionary):
        if any(not self._in_memory.has_column(column) for column in dictionary):
            raise NonExistentColumnError

    def insert(self, document):
        id_to_return = self._in_memory.get_new_id()
        self._update_columns(document)
        values_str = '?, '
        values = [id_to_return]
        command = 'insert into [{}] (_id, '.format(self._collection)
        for col, value in document.items():
            command += '[{}], '.format(col)
            values_str += '?, '
            values.append(value)
        command = '{}) values({})'.format(command.rstrip(', '), values_str.rstrip(', '))
        self._cursor.execute(command, values)
        return id_to_return

    def _update_columns(self, document):
        for column, value in document.items():
            if not self._in_memory.has_column(column):
                self._add_column(column, value)

    def _add_column(self, column, value):
        if isinstance(value, int):
            type_str = 'INTEGER'
            default = 0
        elif isinstance(value, str):
            type_str = 'TEXT'
            default = ''
        else:
            type_str = 'BLOB'
            default = None

        command = 'alter table [{}] add column [{}] {}'.format(self._collection, column, type_str)
        if default is not None:
            command += ' default {}'.format(default)
        self._cursor.execute(command)
        self._in_memory.add_column(column)

    def drop_collection(self):
        self._drop_indices()
        self._cursor.execute('DROP TABLE if exists [{}]'.format(self._collection))
        self._in_memory.drop_table()

    def reset_collection(self):
        self.drop_collection()
        self._set_up()
        self._in_memory.refresh_information()

    def _drop_indices(self):
        for index in self._in_memory.indices:
            index_name = '&'.join(index)
            command = 'drop index [{}]'.format(index_name)
            self._cursor.execute(command)
        self._in_memory.refresh_indices()

    def close(self):
        if self._connection:
            self._connection.commit()
            self._connection.close()

        self._in_memory = None
        self._collection = None
        self._connection = None
        self._cursor = None

    @staticmethod
    def get_id_string(id_obj):
        return str(id_obj)

    @staticmethod
    def get_id_object(id_string):
        return int(id_string)

    def create_index(self, columns_tuple):
        safe_col_names = ['[{}]'.format(col_name) for col_name in columns_tuple]
        values = ', '.join(safe_col_names)
        name = '&'.join(columns_tuple)
        command = "create index [{}] on [{}] ({})".format(name, self._collection, values)
        self._update_columns(dict.fromkeys(columns_tuple, (1, )))
        self._cursor.execute(command)
        self._in_memory.add_index(columns_tuple)

    def has_index(self, columns_tuple):
        return self._in_memory.has_index(columns_tuple)


class InMemoryInformation(object):
    def __init__(self, cursor, collection_name):
        self._cursor = cursor
        self._collection = collection_name
        self._tables = None
        self._col_names = None
        self._id_generator = None
        self._indices = None
        self.refresh_information()

    def refresh_information(self):
        self.refresh_tables()
        self.refresh_columns()
        self.refresh_indices()
        self._id_generator = self._get_id_generator()

    def refresh_tables(self):
        self._cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        self._tables = sorted([element[0] for element in self._cursor.fetchall()])

    def refresh_columns(self):
        self._cursor.execute("PRAGMA table_info([{}])".format(self._collection))
        data = self._cursor.fetchall()
        self._col_names = [col_data[1] for col_data in data]

    def refresh_indices(self):
        self._cursor.execute("select * from sqlite_master where type='index'")
        data = self._cursor.fetchall()
        indices = []
        for index_data in data:
            if index_data[2] == self._collection:
                index_name = index_data[1]
                indices.append(tuple(index_name.split('&')))
        self._indices = sorted(indices)

    def _get_id_generator(self):
        def id_generator(available_now):
            currently_available = available_now
            while True:
                yield currently_available
                currently_available += 1

        first_available = self._get_next_id()
        return id_generator(first_available)

    def _get_next_id(self):
        command = 'select max(_id) from {}'.format(self._collection)
        db_max = self._cursor.execute(command).fetchone()[0]
        if db_max is None:
            return 0
        return db_max + 1

    def has_column(self, col_name):
        return col_name in self._col_names

    def has_table(self, table_name):
        return table_name in self._tables

    def has_index(self, columns_tuple):
        return columns_tuple in self._indices

    @property
    def tables(self):
        return self._tables[:]

    @property
    def columns(self):
        return self._col_names[:]

    @property
    def indices(self):
        return self._indices[:]

    def get_new_id(self):
        return next(self._id_generator)

    def add_table(self, table_name):
        if not self.has_table(table_name):
            self._tables.append(table_name)
            self._tables.sort()

    def add_column(self, col_name):
        if not self.has_column(col_name):
            self._col_names.append(col_name)

    def add_index(self, columns_tuple):
        if not self.has_index(columns_tuple):
            self._indices.append(columns_tuple)
            self._indices.sort()

    def drop_table(self):
        self._indices = []
        self._col_names = []
        if self._collection in self._tables:
            del self._tables[self._tables.index(self._collection)]


def get_index_data(index_name):
    string_list = index_name.split('&')
    return tuple(string_list)


def make_dict(keys, values):
    if all(value is None for value in values):
        return None
    return {key: val for key, val in zip(keys, values)}




"""select priority0.*, priority1.die, priority1.number
from priority0 left outer join priority1 on priority0.id = priority1.id
where priority0.die = 'Die(4)' and priority1.die is not NULL

select all the stuff
from priority0
left outer join priority1 on priority0.id = priority1.id
left outer join priority2 on priotity0.id = priority2.id

where
priority0.die = 'Die(4)' and 10 < priority0.number <100
and priority1.die

mongodb notes
import pymongo
from bson.binary import Binary

{'my_data': Binary(some bytes)}


insert({key: val})
find/find_one({key: exact_vale, key: {'$lte': val}}, return_only {key: 1} not {key: 0}
return dict()

"""
