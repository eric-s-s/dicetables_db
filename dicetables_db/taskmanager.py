from threading import Thread
from typing import List, Tuple

import dicetables as dt

from dicetables_db.connections.sql_connection import SQLConnection
from dicetables_db.connections.mongodb_connection import MongoDBConnection
from insertandretrieve import DiceTableInsertionAndRetrieval


from tasktools import extract_modifiers, TableGenerator, is_new_table


class TaskManager(object):
    def __init__(self, insert_retrieve: DiceTableInsertionAndRetrieval) -> None:
        self._insert_retrieve = insert_retrieve
        self._table_generator = None

    # TODO testing convenience. out of scope
    @classmethod
    def create_for_mongo_db(cls, db_name, collection_name, ip='localhost', port=27017) -> 'TaskManager':
        db_interface = create_insert_retrieve('mongo_db', db_name, collection_name, ip, port)
        return cls(db_interface)

    @classmethod
    def create_for_sql(cls, db_path, collection_name) -> 'TaskManager':
        db_interface = create_insert_retrieve('SQL', db_path, collection_name)
        return cls(db_interface)

    def extract_modifiers(self, dice_list: list) -> Tuple[int, list]:
        return extract_modifiers(dice_list)

    def get_closest_from_database(self, dice_list: list) -> dt.DiceTable:
        id_ = self._insert_retrieve.find_nearest_table(dice_list)
        if id_ is None:
            return dt.DiceTable.new()

        return self._insert_retrieve.get_table(id_)

    def get_tables_to_save(self, current_table: dt.DiceTable, dice_list: list) -> List[dt.DiceTable]:
        pass

    def get_final_table(self, closest_table: dt.DiceTable, dice_list: list) -> dt.DiceTable:
        pass

    def apply_modifier(self, table: dt.DiceTable, modifier: int) -> dt.DiceTable:
        pass

    def save_table_list(self, table_list: list):
        for table in table_list:
            if not is_new_table(table) and not self._insert_retrieve.has_table(table):
                self._insert_retrieve.add_table(table)

    def process_request(self, dice_list: list) -> dt.DiceTable:
        modifier, new_list = self.extract_modifiers(dice_list)
        closest = self.get_closest_from_database(new_list)

        tables_to_save = self.get_tables_to_save(closest, new_list)

        thread = Thread(target=self.save_table_list, args=(tables_to_save,))
        thread.start()

        raw_final_table = self.get_final_table(tables_to_save[-1], new_list)
        with_modifier = self.apply_modifier(raw_final_table, modifier)
        return dt.DiceTable(with_modifier.get_dict(), dt.DiceRecord(dict(dice_list)))


def create_insert_retrieve(type_: str, db_loc: str, collection_name: str, ip='localhost', port=27017
                           ) -> DiceTableInsertionAndRetrieval:
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
