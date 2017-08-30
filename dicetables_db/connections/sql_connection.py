import sqlite3 as lite

from dicetables_db.connections.baseconnection import BaseConnection


class SQLConnection(BaseConnection):
    def __init__(self, db_path, collection_name):
        self._path = db_path
        self._collection = collection_name

        self._connection = lite.connect(self._path, detect_types=lite.PARSE_DECLTYPES)
        lite.register_adapter(self.id_class(), self.id_class().to_string)

        self._cursor = self._connection.cursor()

        self._set_up()
        self._in_memory = InMemoryInformation(self)

    def _set_up(self):
        command = "CREATE TABLE IF NOT EXISTS [{}] (_id {}, PRIMARY KEY(_id))".format(self._collection,
                                                                                      self.id_class().__name__)
        self._cursor.execute(command)

    def get_info(self):
        out = {
            'db': self._path,
            'collections': self._in_memory.collections,
            'current_collection': self._collection,
            'indices': self._in_memory.indices
        }
        return out

    @property
    def cursor(self):
        return self._cursor

    @property
    def collection(self):
        return self._collection

    def is_collection_empty(self):
        command = 'PRAGMA TABLE_INFO([{}]);'.format(self._collection)
        table_info = self._cursor.execute(command).fetchone()
        if table_info is None:
            return True

        count_entries = "SELECT COUNT(*) FROM {}".format(self._collection)
        entries = self._cursor.execute(count_entries).fetchone()[0]
        return entries == 0

    def find(self, params_dict=None, projection=None):
        keys_list = self._get_columns_list(projection)
        command, values = self._get_command_and_values(params_dict, keys_list)
        values_lists = self._cursor.execute(command, values).fetchall()

        to_check = [self._make_dict(keys_list, value_list) for value_list in values_lists]
        return [element for element in to_check if element is not None]

    def find_one(self, params_dict=None, projection=None):
        keys_list = self._get_columns_list(projection)
        command, values = self._get_command_and_values(params_dict, keys_list)
        values_list = self._cursor.execute(command, values).fetchone()
        if not values_list:
            return None
        return self._make_dict(keys_list, values_list)

    def _get_columns_list(self, projection):
        if not projection:
            return self._in_memory.columns
        projection_uses_inclusion = self._does_projection_use_inclusion(projection)
        if projection_uses_inclusion:
            return self._get_columns_by_inclusion(projection)
        return self._get_columns_by_exlusion(projection)

    @staticmethod
    def _does_projection_use_inclusion(projection):
        bool_list = [bool(value) for value in projection.values()]
        if True in bool_list and False in bool_list:
            raise ValueError('Projection cannot have a mix of inclusion and exclusion.')
        return bool_list[0]

    def _get_columns_by_inclusion(self, projection):
        return [col for col in projection if self._in_memory.has_column(col)]

    def _get_columns_by_exlusion(self, projection):
        all_cols = self._in_memory.columns
        return [col for col in all_cols if col not in projection]

    def _get_command_and_values(self, params_dict, columns_list):
        if self._has_non_existent_columns(params_dict) or not columns_list:
            return 'SELECT NULL FROM [{}]'.format(self._collection), []

        select_statement = self._get_select_statement(columns_list)
        where_statement, values = self._get_statement_and_values_for_where(params_dict)
        return select_statement + where_statement, values

    def _has_non_existent_columns(self, params_dict):
        if not params_dict:
            return False
        return any(not self._in_memory.has_column(key) for key in params_dict)

    def _get_select_statement(self, columns_list):
        safe_col_names = ['[{}]'.format(col) for col in columns_list]
        select_string = ', '.join(safe_col_names)
        command_start = 'SELECT {} FROM [{}]'.format(select_string, self._collection)
        return command_start

    def _get_statement_and_values_for_where(self, params_dict):
        if not params_dict:
            return '', []
        where_vals = []
        values = []
        for col, inequality_info in params_dict.items():
            inequality_str, value = self._get_inequality_data(inequality_info)
            where_vals.append('[{}]{}?'.format(col, inequality_str))
            values.append(value)

        where_string = ' WHERE ' + ' AND '.join(where_vals)
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

    def _make_dict(self, keys, values):
        if all(value is None for value in values):
            return None
        answer = {key: val for key, val in zip(keys, values)}
        self._change_id_key(answer)
        return answer

    def _change_id_key(self, answer):
        if '_id' in answer:
            answer['_id'] = self.id_class().from_string(answer['_id'])

    def insert(self, document):
        self._update_columns(document)
        id_to_return = self.id_class().new()
        command, values = self._insert_command_and_values(document, id_to_return)
        self._cursor.execute(command, values)
        return id_to_return

    def _insert_command_and_values(self, document, id_to_return):
        values_str = '?, '
        values = [id_to_return]
        command = 'INSERT INTO [{}] (_id, '.format(self._collection)
        for col, value in document.items():
            command += '[{}], '.format(col)
            values_str += '?, '
            values.append(value)
        command = '{}) VALUES({})'.format(command.rstrip(', '), values_str.rstrip(', '))
        return command, values

    def _update_columns(self, document):
        for column, value in sorted(document.items()):
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

        command = 'ALTER TABLE [{}] ADD COLUMN [{}] {}'.format(self._collection, column, type_str)
        if default is not None:
            command += ' DEFAULT {!r}'.format(default)
        self._cursor.execute(command)
        self._in_memory.add_column(column)

    def drop_collection(self):
        self._drop_indices()
        self._cursor.execute('DROP TABLE IF EXISTS [{}]'.format(self._collection))
        self._in_memory.drop_collection()

    def reset_collection(self):
        self.drop_collection()
        self._set_up()
        self._in_memory.refresh_information()

    def _drop_indices(self):
        for index in self._in_memory.indices:
            index_name = '&'.join(index)
            command = 'DROP INDEX [{}]'.format(index_name)
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

    def create_index(self, columns_tuple):
        new_column_type = object
        self._update_columns(dict.fromkeys(columns_tuple, new_column_type))

        safe_col_names = ['[{}]'.format(col_name) for col_name in columns_tuple]

        index_values = ', '.join(safe_col_names)
        index_name = '&'.join(columns_tuple)
        command = "CREATE INDEX [{}] ON [{}] ({})".format(index_name, self._collection, index_values)

        self._cursor.execute(command)
        self._in_memory.add_index(columns_tuple)

    def has_index(self, columns_tuple):
        return self._in_memory.has_index(columns_tuple)

    # TODO flesh out these two methods and test them.  just basic idea. need serious
    def save_to_target(self, db_name):
        """
        this is just notes.  this is a super buggy idea and will only work with a bit of tweaking.
        """
        commands = self._connection.iterdump()
        target_db = lite.connect(db_name)
        cursor = target_db.cursor()
        for command in commands:
            cursor.execute(command)
        target_db.commit()
        target_db.close()

    def load_from_target(self, db_name):
        """
        this is just notes.  this is a super buggy idea and will only work with a bit of tweaking.
        """
        self.drop_collection()
        db_connect = lite.connect(db_name)
        commands = db_connect.iterdump()

        for command in commands:
            self.cursor.execute(command)
        self._in_memory.refresh_information()


class InMemoryInformation(object):
    def __init__(self, connection):
        self._cursor = connection.cursor
        self._collection = connection.collection
        self._collections = None
        self._col_names = None
        self._indices = None
        self.refresh_information()

    def refresh_information(self):
        self.refresh_collections()
        self.refresh_columns()
        self.refresh_indices()

    def refresh_collections(self):
        self._cursor.execute("SELECT name FROM sqlite_master WHERE TYPE='table';")
        self._collections = sorted([element[0] for element in self._cursor.fetchall()])

    def refresh_columns(self):
        self._cursor.execute("PRAGMA table_info([{}])".format(self._collection))
        data = self._cursor.fetchall()
        self._col_names = [col_data[1] for col_data in data]

    def refresh_indices(self):
        self._cursor.execute("SELECT * FROM sqlite_master WHERE TYPE='index';")
        data = self._cursor.fetchall()
        indices = []
        for index_data in data:
            if index_data[2] == self._collection:
                index_name = index_data[1]
                if 'autoindex' in index_name:
                    continue
                indices.append(tuple(index_name.split('&')))
        self._indices = sorted(indices)

    def has_column(self, col_name):
        return col_name in self._col_names

    def has_collection(self, table_name):
        return table_name in self._collections

    def has_index(self, columns_tuple):
        return columns_tuple in self._indices

    @property
    def collections(self):
        return self._collections[:]

    @property
    def columns(self):
        return self._col_names[:]

    @property
    def indices(self):
        return self._indices[:]

    def add_collection(self, table_name):
        if not self.has_collection(table_name):
            self._collections.append(table_name)
            self._collections.sort()

    def add_column(self, col_name):
        if not self.has_column(col_name):
            self._col_names.append(col_name)

    def add_index(self, columns_tuple):
        if not self.has_index(columns_tuple):
            self._indices.append(columns_tuple)
            self._indices.sort()

    def drop_collection(self):
        self._indices = []
        self._col_names = []
        if self._collection in self._collections:
            del self._collections[self._collections.index(self._collection)]
