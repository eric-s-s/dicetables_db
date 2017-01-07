import pickle
import sqlite3 as lite
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
        col1 = (0, 'id', 'INTEGER', 0, None, 1)
        col2 = (1, 'dt', 'BLOB', 0, None, 0)
        if col1 != columns[0] or col2 != columns[1]:
            return False
        return True

    def is_empty(self):
        return not self.get_tables()

    def get_tables(self):
        self._cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        return [element[0] for element in self._cursor.fetchall()]

    def get_table_data(self, table_name):
        self._cursor.execute("PRAGMA table_info({})".format(table_name))
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
            self._cursor.execute('DROP TABLE {}'.format(table))
        self._set_up()

    def start_up(self):
        self._connection = lite.connect(self._file_name)
        self._cursor = self._connection.cursor()

    def abort(self):
        self._connection.close()

    def shut_down(self):
        self._connection.commit()
        self._connection.close()


def adapt_dice_table(dice_table):
    return pickle.dumps(dice_table)


lite.register_adapter(dt.DiceTable, adapt_dice_table)


class DiceTableInjector(object):
    def __init__(self, connector):
        self.conn = connector
        self.priorities = len(connector.get_tables()) - 1
        self._id_to_use = 0
        db_max = self.conn.cursor.execute('select max(id) from dicetables').fetchone()[0]
        if db_max is not None:
            self._id_to_use = db_max + 1

    @property
    def current_id(self):
        return self._id_to_use

    @property
    def cursor(self):
        return self.conn.cursor

    def add_table(self, table):
        self.conn.cursor.execute('INSERT INTO dicetables (id, dt) VALUES(?, ?)', (self._id_to_use, table))
        priorities = create_priority_list(table)
        self._update_priorities(len(priorities))
        for priority, die_number in enumerate(priorities):
            die, number = die_number
            command = 'INSERT INTO priority{} (die, number, id) VALUES(?, ?, ?)'.format(priority)
            self.conn.cursor.execute(command, (repr(die), number, self._id_to_use))
        self._id_to_use += 1

    def _update_priorities(self, new_size):
        if self.priorities < new_size:
            for priority_to_add in range(self.priorities, new_size):
                command = "CREATE TABLE priority{} (die TEXT, number INTEGER, id INTEGER)".format(priority_to_add)
                self.conn.cursor.execute(command)
            self.priorities = new_size


class DiceTableRetriever(object):
    def __init__(self, connector):
        self.conn = connector
        self.priorities = len(connector.get_tables()) - 1

    def get_candidates(self, dice_list):
        priority_list = get_priority_list(dice_list)



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


class Searcher(object):
    def __init__(self, connector):
        self.conn = connector

    def get_table_list(self, table):
        command = 'select '


def create_priority_list(table):
    return get_priority_list(table.get_list())


def get_priority_list(die_number_list):
    die_number_list.sort(key=prioritize, reverse=True)
    return die_number_list


def prioritize(die_num_tuple):
    die, num = die_num_tuple
    return die.get_size()**2 * num**2 + die.get_weight()



