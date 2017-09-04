from threading import Thread

from dicetables import DiceRecord, DiceTable

from insertandretrieve import DiceTableInsertionAndRetrieval


from tasktools import TableGenerator, is_new_table, extract_modifiers, apply_modifier


class TaskManager(object):
    def __init__(self, insert_retrieve: DiceTableInsertionAndRetrieval) -> None:
        self._insert_retrieve = insert_retrieve
        self.step_size = 30

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

    def process_request(self, dice_record: DiceRecord) -> DiceTable:
        modifier, new_record = extract_modifiers(dice_record)
        closest = self.get_closest_from_database(new_record)

        table_generator = TableGenerator(new_record)
        tables_to_save = table_generator.create_save_list(closest, self.step_size)

        thread = Thread(target=self.save_table_list, args=(tables_to_save,))
        thread.start()

        raw_final_table = table_generator.create_target_table(tables_to_save[-1])
        table_with_modifier = apply_modifier(raw_final_table, modifier)

        answer = DiceTable(table_with_modifier.get_dict(), dice_record)

        return answer
