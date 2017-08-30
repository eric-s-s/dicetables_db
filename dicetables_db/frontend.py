"""
the front-end that handles all requests and returns completed tables
"""
import dicetables as dt
from threading import Thread
from dicetables_db.insertandretrieve import DiceTableInsertionAndRetrieval
from dicetables_db.connections.mongodb_connection import MongoDBConnection
from dicetables_db.connections.sql_connection import SQLConnection


class TableCreator(object):
    def __init__(self, db_interface: DiceTableInsertionAndRetrieval):
        self._interface = db_interface

    @classmethod
    def create_for_mongo_db(cls, db_name, collection_name, ip='localhost', port=27017):
        db_interface = create_insert_retrieve('mongo_db', db_name, collection_name, ip, port)
        return cls(db_interface)

    @classmethod
    def create_for_sql(cls, db_path, collection_name):
        db_interface = create_insert_retrieve('SQL', db_path, collection_name)
        return cls(db_interface)

    def get_nearest_table(self, dice_list):
        id_ = self._interface.find_nearest_table(dice_list)
        if id_ is None:
            return dt.DiceTable.new()
        else:
            return self._interface.get_table(id_)

    def generate_table(self, dice_list):
        retrieved = self.get_nearest_table(dice_list)
        if retrieved.get_list() != dice_list:
            return make_up_difference(retrieved, dice_list)
        return retrieved, []

    def save(self, dice_table):
        if not is_new_table(dice_table) and not self._interface.has_table(dice_table):
            self._interface.add_table(dice_table)


def make_up_difference(dice_table: dt.DiceTable, dice_list):
    step = 25
    to_save = []
    for die, number in dice_list:
        add_step = max(1, step // die.get_size())
        while dice_table.number_of_dice(die) <= number - add_step:
            dice_table = dice_table.add_die(die, add_step)
            to_save.append(dice_table)
        end_bit = number - dice_table.number_of_dice(die)
        if end_bit != 0:
            dice_table = dice_table.add_die(die, end_bit)
    return dice_table, to_save


def is_new_table(dice_table):
    return dice_table == dt.DiceTable.new()


def create_insert_retrieve(type_, db_loc, collection_name, ip='localhost', port=27017):
    """

    :param type_: 'mongo_db', 'SQL'
    :param db_loc: db_name and possibly path 'test_db'
    :param collection_name: 'my_table', 'dicetables_collection'
    :param ip: 'localhost'
    :param port: 27017
    :return: connection
    """
    conn = None
    if type_ == 'mongo_db':
        conn = MongoDBConnection(db_loc, collection_name, ip, port)
    if type_ == 'SQL':
        conn = SQLConnection(db_loc, collection_name)
    return DiceTableInsertionAndRetrieval(conn)


class ServerTalker(object):
    def __init__(self, table_creator: TableCreator):
        self._creator = table_creator

    def get_table(self, die_str_numer_list):
        original_list = parse_list(die_str_numer_list)
        modifier, query_list = get_mod_and_new_list(original_list)
        pre_return_table, to_save = self._creator.generate_table(query_list)
        thread = Thread(target=self.save_tables, args=(to_save,))
        thread.start()
        to_return = pre_return_table
        if modifier:
            to_return = to_return.add_die(dt.Modifier(modifier))
        return to_return

    def save_tables(self, table_list):
        for table in table_list:
            self._creator.save(table)


def parse_list(die_number_list):
    parser = dt.Parser()
    return [(parser.parse_die(die_str), number) for die_str, number in die_number_list]


def get_mod_and_new_list(dice_list):
    modifier = 0
    new_list = []
    for die, number in dice_list:
        if type(die) is dt.ModWeightedDie:
            modifier += number * die.get_modifier()
            new_die = dt.WeightedDie(die.get_raw_dict())
        elif type(die) is dt.ModDie:
            modifier += number * die.get_modifier()
            new_die = dt.Die(die.get_size())
        else:
            new_die = die

        if type(new_die) is dt.Modifier:
            modifier += new_die.get_modifier() * number
        else:
            new_list.append((new_die, number))

    return modifier, new_list



