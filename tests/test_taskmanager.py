from threading import active_count
from unittest import TestCase

from dicetables import DiceTable, Die, DiceRecord

from dicetables_db.taskmanager import TaskManager
from dicetables_db.insertandretrieve import DiceTableInsertionAndRetrieval
from dicetables_db.connections.sql_connection import SQLConnection


class TestTaskManager(TestCase):

    def setUp(self):
        self.connection = SQLConnection(':memory:', 'test')
        self.insert_retrieve = DiceTableInsertionAndRetrieval(self.connection)
        self.task_manager = TaskManager(self.insert_retrieve)

    def test_get_closest_from_database_no_matches_in_database(self):
        table = DiceTable.new().add_die(Die(10))
        self.insert_retrieve.add_table(table)
        self.assertTrue(self.insert_retrieve.has_table(table))

        empty_table = self.task_manager.get_closest_from_database(DiceRecord({Die(5): 2}))
        self.assertEqual(empty_table, DiceTable.new())

    def test_get_closest_from_database_no_matches_database_table_too_big(self):
        table = DiceTable.new().add_die(Die(10), 2)
        self.insert_retrieve.add_table(table)

        target = DiceRecord({Die(10): 1})

        empty_table = self.task_manager.get_closest_from_database(target)
        self.assertEqual(empty_table, DiceTable.new())

    def test_get_closest_from_database_exact_match(self):
        one_d10 = DiceTable.new().add_die(Die(10), 1)
        two_d10 = DiceTable.new().add_die(Die(10), 2)
        self.insert_retrieve.add_table(one_d10)
        self.insert_retrieve.add_table(two_d10)

        target_1 = DiceRecord({Die(10): 1})
        target_2 = DiceRecord({Die(10): 2})

        answer_1 = self.task_manager.get_closest_from_database(target_1)
        answer_2 = self.task_manager.get_closest_from_database(target_2)
        self.assertEqual(answer_1, one_d10)
        self.assertEqual(answer_2, two_d10)

    def test_get_closest_from_database_partial_match(self):
        one_d5 = DiceTable.new().add_die(Die(5), 1)
        two_d5 = DiceTable.new().add_die(Die(5), 2)
        one_d10 = DiceTable.new().add_die(Die(10), 1)
        two_d10 = DiceTable.new().add_die(Die(10), 2)
        one_d5_one_d10 = DiceTable.new().add_die(Die(5)).add_die(Die(10))
        self.insert_retrieve.add_table(one_d5)
        self.insert_retrieve.add_table(two_d5)
        self.insert_retrieve.add_table(one_d10)
        self.insert_retrieve.add_table(two_d10)
        self.insert_retrieve.add_table(one_d5_one_d10)

        target_1 = DiceRecord({Die(10): 2, Die(5): 1})
        target_2 = DiceRecord({Die(10): 1, Die(5): 2})
        target_3 = DiceRecord({Die(10): 2, Die(5): 2})

        answer_1 = self.task_manager.get_closest_from_database(target_1)
        answer_2 = self.task_manager.get_closest_from_database(target_2)
        answer_3 = self.task_manager.get_closest_from_database(target_3)
        self.assertEqual(answer_1, two_d10)
        self.assertEqual(answer_2, one_d5_one_d10)
        self.assertEqual(answer_3, two_d10)

    def test_save_table_list_does_not_save_empty_table(self):
        empty = DiceTable.new()
        non_empty = empty.add_die(Die(3))

        self.task_manager.save_table_list([empty, non_empty])

        self.assertTrue(self.insert_retrieve.has_table(non_empty))
        all_docs = self.connection.find()
        self.assertTrue(len(all_docs), 1)  # has_table(empty) never actually checks database.

    def test_save_table_list_does_not_save_same_table_twice(self):
        table = DiceTable.new().add_die(Die(4))
        same_table = DiceTable.new().add_die(Die(4))
        self.task_manager.save_table_list([table, same_table])
        self.assertTrue(self.insert_retrieve.has_table(table))
        self.assertTrue(self.insert_retrieve.has_table(same_table))

        all_docs = self.connection.find()
        self.assertEqual(len(all_docs), 1)

    def test_save_table_list_saves_all_the_tables(self):
        to_save = []
        for dice_num in range(1, 6):
            to_save.append(DiceTable.new().add_die(Die(2), dice_num))

        self.task_manager.save_table_list(to_save)
        self.assertEqual(len(self.connection.find()), len(to_save))
        for table in to_save:
            self.assertTrue(self.insert_retrieve.has_table(table))

    def test_process_request_threading(self):
        big_request = DiceRecord({Die(6): 500})
        answer = self.task_manager.process_request(big_request)
        for _ in range(5):
            print(active_count())

        #  TODO threading issue with sqlite. connection created in main thread cannot be accessed in other thread.
        #  TODO perhaps no threading.
