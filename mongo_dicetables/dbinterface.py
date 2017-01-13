import pickle
import sqlite3 as lite
from itertools import combinations
import dicetables as dt


class Connection(object):
    def __init__(self, path_name):
        self._path = path_name
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
        if len(columns) < 3:
            return False
        """col number, name, type, can_null, default, is_primary_key"""
        col0 = (0, 'id', 'INTEGER', 0, None, 1)
        col1 = (1, 'bytes', 'BLOB', 0, None, 0)
        col2 = (2, 'dice_score', 'INTEGER', 0, None, 0)
        if col0 != columns[0] or col1 != columns[1] or col2 != columns[2]:
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
        self._cursor.execute("CREATE TABLE master (id INTEGER, [bytes] BLOB, dice_score INTEGER, PRIMARY KEY(id))")

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
        return [col_data[1] for col_data in data[3:]]

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


def adapt_dice_table(dice_table):
    return pickle.dumps(dice_table)


lite.register_adapter(dt.DiceTable, adapt_dice_table)


class ConnectionCommandInterface(object):
    def __init__(self, connection):
        self._conn = connection
        self.info = InMemoryInformation(connection)

    def add_table(self, table):
        die_names = [repr(die_num[0]) for die_num in table.get_list()]
        types_table_name = self._update_types_tables(die_names)
        self._update_master_cols(die_names)
        command1, values1 = self._get_command_for_master(table)
        self._conn.cursor.execute(command1, values1)
        command2, values2 = self._get_command_for_types_table(table.get_list(), types_table_name)
        self._conn.cursor.execute(command2, values2)
        self.info.increment_id()

    def _update_types_tables(self, die_names):
        new_table_name = '&'.join(die_names)
        if not self.info.has_table(new_table_name):
            command = 'CREATE TABLE [{}] (id INTEGER'.format(new_table_name)
            for die_repr in die_names:
                command += ', [{}] INTEGER'.format(die_repr)
            command += ')'
            self._conn.cursor.execute(command)
            self.info.add_table(new_table_name)
        return new_table_name

    def _update_master_cols(self, die_names):
        for die_repr in die_names:
            if not self.info.has_die_column(die_repr):
                command = 'ALTER TABLE master ADD COLUMN [{}] INTEGER DEFAULT 0'.format(die_repr)
                self._conn.cursor.execute(command)
                self.info.add_die_column(die_repr)

    def _get_command_for_master(self, dice_table):
        command = 'INSERT INTO master (id, [bytes], dice_score'
        values = [self.info.available_id, dice_table, get_dice_score(dice_table.get_list())]
        for die, num in dice_table.get_list():
            command += ', [{!r}]'.format(die)
            values.append(num)
        command += ') VALUES(?, ?, ?'
        command += ', ?'*len(dice_table.get_list()) + ')'
        return command, tuple(values)

    def _get_command_for_types_table(self, dice_list, types_table_name):
        command = 'INSERT INTO [{}] (id'.format(types_table_name)
        values = [self.info.available_id]
        for die, num in dice_list:
            command += ', [{!r}]'.format(die)
            values.append(num)
        command += ') VALUES(?'
        command += ', ?'*len(dice_list) + ')'
        return command, tuple(values)

    def find_nearest_table(self, dice_list):
        acceptable_score_ratio = 0.80
        input_dice_score = float(get_dice_score(dice_list))

        combos_generator = generate_table_names(dice_list)
        table_names = next(combos_generator)

        id_number = None
        highest_score = 0
        dice_score_ratio = 0
        while table_names != [''] and dice_score_ratio < acceptable_score_ratio:
            safe_table_names = self._remove_nonexistent_tables(table_names)
            for table in safe_table_names:
                command, values = self._get_command_for_search(dice_list, table)
                result = self._conn.cursor.execute(command, values).fetchone()
                if result:
                    new_id, new_score = result
                    if new_score > highest_score:
                        id_number = new_id
                        highest_score = new_score
                        dice_score_ratio = float(new_score) / input_dice_score
            table_names = next(combos_generator)
        return id_number

    def _remove_nonexistent_tables(self, table_names):
        return [name for name in table_names if self.info.has_table(name)]

    def _get_command_for_search(self, dice_list, table):
        command = ('SELECT master.id, max(master.dice_score) FROM master JOIN [{}]\n'.format(table) +
                   'ON master.id = [{}].id\n'.format(table) +
                   'WHERE master.dice_score <= ?')
        values = [get_dice_score(dice_list)]
        safe_dice_list = self._remove_nonexistent_dice(dice_list)
        for die, num in safe_dice_list:
            command += '\nAND master.[{!r}] <= ?'.format(die)
            values.append(num)
        return command, tuple(values)

    def _remove_nonexistent_dice(self, dice_list):
        return [die_num for die_num in dice_list if self.info.has_die_column(repr(die_num[0]))]

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


"""


def get_combos(lst):
    r = len(lst)
    while r >= 0:
        yield list(combinations(lst, r))
        r -= 1


def generate_table_names(dice_list):
    dice_names_generator = get_combos([repr(die_num[0]) for die_num in dice_list])
    while True:
        name_groups = next(dice_names_generator)
        yield ['&'.join(die_group) for die_group in name_groups]


def get_dice_score(dice_list):
    score = 0
    for die, num in dice_list:
        size = die.get_size()
        if die.get_weight() > size:
            size += 1
        score += size * num
    return score
