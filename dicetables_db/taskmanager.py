from queue import Queue

from dicetables import DiceRecord, DiceTable

from dicetables_db.tools.tasktools import TableGenerator, is_new_table, extract_modifiers, apply_modifier
from dicetables_db.insertandretrieve import DiceTableInsertionAndRetrieval


class TaskManager(object):
    def __init__(self, insert_retrieve: DiceTableInsertionAndRetrieval, step_size=30) -> None:
        self._insert_retrieve = insert_retrieve
        self._step_size = step_size
        self.save_queue = []

    @property
    def step_size(self):
        return self._step_size

    def get_closest_from_database(self, dice_record: DiceRecord) -> DiceTable:
        dice_list = sorted(dice_record.get_dict().items())
        id_ = self._insert_retrieve.find_nearest_table(dice_list)
        if id_ is None:
            return DiceTable.new()

        return self._insert_retrieve.get_table(id_)

    def save_table_list(self, table_list: list):
        for table in table_list:
            if not is_new_table(table) and not self._insert_retrieve.has_table(table):
                self._insert_retrieve.add_table(table)

    def process_request(self, dice_record: DiceRecord, updater_queue: Queue = None) -> DiceTable:

        modifier, new_record = extract_modifiers(dice_record)

        if new_record == DiceRecord.new():
            closest = DiceTable.new()
        else:
            closest = self.get_closest_from_database(new_record)

        table_generator = TableGenerator(new_record)
        tables_to_save = table_generator.create_save_list(closest, self.step_size, updater_queue)

        if not tables_to_save:
            intermediate_table = closest
        else:
            intermediate_table = tables_to_save[-1]

        self.save_table_list(tables_to_save)

        raw_final_table = table_generator.create_target_table(intermediate_table)
        table_with_modifier = apply_modifier(raw_final_table, modifier)

        answer = DiceTable(table_with_modifier.get_dict(), dice_record)

        return answer
