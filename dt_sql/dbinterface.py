import pickle
import sqlite3 as lite
from itertools import combinations
import dicetables as dt


class DBConnect(object):
    def __init__(self, location, add_path=True):
        if add_path:
            location = 'E:/work/database/' + location
        self._file_name = location
        self._connection = None
        self._cursor = None
        self.start_up()
        if self.is_empty():
            self._set_up()
        if not self.has_dicetables() or not self.is_dicetables_correct():
            self.abort()
            raise ValueError('wrong type of database')

    @property
    def cursor(self):
        return self._cursor

    def has_dicetables(self):
        return 'dicetables' in self.get_tables()

    def is_dicetables_correct(self):
        columns = self.get_table_data('dicetables')
        if len(columns) != 2:
            return False
        """col number, name, type, can_null, default, is_primary_key"""
        col0 = (0, 'id', 'INTEGER', 0, None, 1)
        col1 = (1, 'dt', 'BLOB', 0, None, 0)
        if col0 != columns[0] or col1 != columns[1]:
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

    def get_table_data_all(self):
        out = []
        for table_name in self.get_tables():
            out.append((table_name, self.get_table_data(table_name)))
        return out

    def _set_up(self):
        self._cursor.execute("CREATE TABLE dicetables (id INTEGER, dt BLOB, PRIMARY KEY(id))")

    def reset_table(self):
        for table in self.get_tables():
            self._cursor.execute('DROP TABLE [{}]'.format(table))
        self._set_up()

    def start_up(self):
        self._connection = lite.connect(self._file_name)
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
        data = self._conn.get_table_data('dicetables')
        return [col_data[1] for col_data in data[2:]]

    def _get_next_id(self):
        db_max = self._conn.cursor.execute('select max(id) from dicetables').fetchone()[0]
        if db_max is None:
            return 0
        return db_max + 1

    def has_die(self, die_name):
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

    def add_die(self, die_name):
        if not self.has_die(die_name):
            self._die_names.append(die_name)


def adapt_dice_table(dice_table):
    return pickle.dumps(dice_table)


lite.register_adapter(dt.DiceTable, adapt_dice_table)


class DiceTableInjector(object):
    def __init__(self, connection):
        self.conn = connection
        self.info = InMemoryInformation(connection)

    @property
    def cursor(self):
        return self.conn.cursor

    def add_table(self, table):
        die_names = [repr(die_num[0]) for die_num in table.get_list()]
        types_table_name = self._update_tables(die_names)
        self._update_dicetables_cols(die_names)
        command1, values1 = self._get_dicetables_command(table)
        self.conn.cursor.execute(command1, values1)
        command2, values2 = self._get_types_table_command(table.get_list(), types_table_name)
        self.conn.cursor.execute(command2, values2)
        self.info.increment_id()

    def _update_tables(self, die_names):
        new_table_name = '&'.join(die_names)
        if not self.info.has_table(new_table_name):
            command = 'CREATE TABLE [{}] (id INTEGER'.format(new_table_name)
            for die_repr in die_names:
                command += ', [{}] INTEGER'.format(die_repr)
            command += ')'
            self.cursor.execute(command)
            self.info.add_table(new_table_name)
        return new_table_name

    def _update_dicetables_cols(self, die_names):
        for die_repr in die_names:
            if not self.info.has_die(die_repr):
                command = 'ALTER TABLE dicetables ADD COLUMN [{}] INTEGER DEFAULT 0'.format(die_repr)
                self.cursor.execute(command)
                self.info.add_die(die_repr)

    def _get_dicetables_command(self, dice_table):
        command = 'INSERT INTO dicetables (id, dt'
        values = [self.info.available_id, dice_table]
        for die, num in dice_table.get_list():
            command += ', [{!r}]'.format(die)
            values.append(num)
        command += ') VALUES(?, ?'
        command += ', ?'*len(dice_table.get_list()) + ')'
        return command, tuple(values)

    def _get_types_table_command(self, dice_list, types_table_name):
        command = 'INSERT INTO [{}] (id'.format(types_table_name)
        values = [self.info.available_id]
        for die, num in dice_list:
            command += ', [{!r}]'.format(die)
            values.append(num)
        command += ') VALUES(?'
        command += ', ?'*len(dice_list) + ')'
        print(command, values)
        return command, tuple(values)

# class DiceTableRetriever(object):
#     def __init__(self, connector):
#         self.conn = connector
#
#     def get_db_priorities(self):
#         return len(self.conn.get_tables()) - 1
#
#     def get_candidates(self, dice_list):
#         db_priorities = self.get_db_priorities()
#         if db_priorities == 0:
#             return []
#         priority_list = get_priority_list(dice_list)
#         priority0 = priority_list[0]
#         command_select = "SELECT priority0.id, priority0.number"
#         command_join = " from\n  priority0"
#         command_where = "\nwhere\n  priority0.die = '{0[0]!r}' and priority0.number <= {0[1]}".format(priority0)
#         for all_priorities in range(1, db_priorities):
#             command_join += "\n  left outer join priority{0} on priority{1}.id = priority{0}.id".format(
#                 all_priorities, all_priorities - 1
#             )
#         for included_priority in range(1, len(priority_list)):
#             command_select += ", priority{}.number".format(included_priority)
#             new_where = ("\n  and (priority{0}.die = '{1[0]!r}' and priority{0}.number <= {1[1]} or" +
#                          "\n       priority{0}.die is NULL)")
#             command_where += new_where.format(included_priority, priority_list[included_priority])
#         for excluded_priority in range(len(priority_list), db_priorities):
#             command_where += '\n  and priority{}.die is NULL'.format(excluded_priority)
#         command = command_select + command_join + command_where
#         print(command)
#         self.conn.cursor.execute(command)
#         return self.conn.cursor.fetchall()



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
"""


def get_combos(lst):
    r = len(lst)
    while r >= 0:
        yield list(combinations(lst, r))
        r -= 1



