from typing import List, Tuple

import dicetables as dt

from insertandretrieve import DiceTableInsertionAndRetrieval


from tasktools import *


class TaskManager(object):
    def __init__(self, insert_retrieve: DiceTableInsertionAndRetrieval):
        self._insert_retrieve = insert_retrieve
        self._table_generator = None

    def do_modifier_things(self, dice_list: List) -> Tuple[int, list]:
        raise NotImplementedError

    def get_closest_from_database(self, dice_list: List) -> dt.DiceTable:
        id_ = self._insert_retrieve.find_nearest_table(dice_list)
        return self._insert_retrieve.get_table(id_)

    def get_tables_to_save(self, current_table, dice_list):
        pass

    def get_final_table(self, closes_table, dice_list):
        pass

    def apply_modifier(self, table, modifier):
        pass

    def save_table_list(self, table_list):
        pass

    


