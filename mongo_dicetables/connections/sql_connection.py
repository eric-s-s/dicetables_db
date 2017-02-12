import sqlite3 as lite


class SQLConnection(object):
    def __init__(self, db_path, table_name):
        self._path = db_path
        self._connection = None
        self._cursor = None
        self.start_up()
        if self.is_empty():
            self._set_up()
        if not self.has_master() or not self.is_master_correct():
            self.abort()
            raise ValueError('wrong type of database')

    @property
    def cursor(self):
        return self._cursor

    def has_master(self):
        return 'master' in self.get_tables()

    def is_master_correct(self):
        columns = self.get_table_data('master')
        if len(columns) < 4:
            return False
        """col number, name, type, can_null, default, is_primary_key"""
        col0 = (0, 'id', 'INTEGER', 0, None, 1)
        col1 = (1, 'serialized', 'BLOB', 0, None, 0)
        col2 = (2, 'score', 'INTEGER', 0, None, 0)
        col3 = (3, 'group', 'TEXT', 0, None, 0)
        if col0 != columns[0] or col1 != columns[1] or col2 != columns[2] or col3 != columns[3]:
            return False
        return True

    def is_empty(self):
        return not self.get_tables()

    def get_tables(self):
        self._cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        return [element[0] for element in self._cursor.fetchall()]

    def get_table_data(self, table_name):
        self._cursor.execute("PRAGMA table_info([{}])".format(table_name))
        return self._cursor.fetchall()

    def _set_up(self):
        command = "CREATE TABLE master (id INTEGER, serialized BLOB, score INTEGER, [group] TEXT, PRIMARY KEY(id))"
        self._cursor.execute(command)

    def reset_table(self):
        for table in self.get_tables():
            self._cursor.execute('DROP TABLE [{}]'.format(table))
        self._set_up()

    def start_up(self):
        self._connection = lite.connect(self._path)
        self._cursor = self._connection.cursor()

    def abort(self):
        self._connection.close()

    def shut_down(self):
        self._connection.commit()
        self._connection.close()


class InMemoryInformation(object):
    def __init__(self, connection):
        self._conn = connection
        self._tables = None
        self._die_names = None
        self._next_available_id = None
        self.refresh_information()

    def refresh_information(self):
        self._tables = self._conn.get_tables()
        self._die_names = self._get_die_names()
        self._next_available_id = self._get_next_id()

    def _get_die_names(self):
        data = self._conn.get_table_data('master')
        return [col_data[1] for col_data in data[4:]]

    def _get_next_id(self):
        db_max = self._conn.cursor.execute('select max(id) from master').fetchone()[0]
        if db_max is None:
            return 0
        return db_max + 1

    def has_die_column(self, die_name):
        return die_name in self._die_names

    def has_table(self, table_name):
        return table_name in self._tables

    @property
    def available_id(self):
        return self._next_available_id

    @property
    def tables(self):
        return self._tables[:]

    @property
    def dice(self):
        return self._die_names[:]

    def increment_id(self):
        self._next_available_id += 1

    def add_table(self, table_name):
        if not self.has_table(table_name):
            self._tables.append(table_name)

    def add_die_column(self, die_name):
        if not self.has_die_column(die_name):
            self._die_names.append(die_name)





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
