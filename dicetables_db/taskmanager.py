from threading import Thread
from typing import List, Tuple

from dicetables import DiceRecord, DiceTable

from dicetables_db.connections.sql_connection import SQLConnection
from dicetables_db.connections.mongodb_connection import MongoDBConnection
from insertandretrieve import DiceTableInsertionAndRetrieval


from tasktools import extract_modifiers, TableGenerator, is_new_table


class TaskManager(object):
    def __init__(self, insert_retrieve: DiceTableInsertionAndRetrieval) -> None:
        self._insert_retrieve = insert_retrieve
        self._table_generator = None

    def extract_modifiers(self, dice_record: DiceRecord) -> Tuple[int, DiceRecord]:
        return extract_modifiers(dice_record)

    def get_closest_from_database(self, dice_record: DiceRecord) -> DiceTable:
        dice_list = sorted(dice_record.get_dict().items())
        id_ = self._insert_retrieve.find_nearest_table(dice_list)
        if id_ is None:
            return DiceTable.new()

        return self._insert_retrieve.get_table(id_)

    def get_tables_to_save(self, current_table: DiceTable, dice_record: DiceRecord) -> List[DiceTable]:
        pass

    def get_final_table(self, closest_table: DiceTable, dice_record: DiceRecord) -> DiceTable:
        pass

    def apply_modifier(self, table: DiceTable, modifier: int) -> DiceTable:
        pass

    def save_table_list(self, table_list: list):
        for table in table_list:
            if not is_new_table(table) and not self._insert_retrieve.has_table(table):
                self._insert_retrieve.add_table(table)

    def process_request(self, dice_record: DiceRecord) -> DiceTable:
        modifier, new_record = self.extract_modifiers(dice_record)
        closest = self.get_closest_from_database(new_record)

        tables_to_save = self.get_tables_to_save(closest, new_record)

        thread = Thread(target=self.save_table_list, args=(tables_to_save,))
        thread.start()

        raw_final_table = self.get_final_table(tables_to_save[-1], new_record)
        with_modifier = self.apply_modifier(raw_final_table, modifier)
        return DiceTable(with_modifier.get_dict(), dice_record)
