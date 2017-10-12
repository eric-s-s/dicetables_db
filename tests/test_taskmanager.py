import unittest
from queue import Queue
from time import clock

from dicetables import (DiceTable, DiceRecord, Modifier, Die, ModDie, WeightedDie, ModWeightedDie,
                        StrongDie, Exploding, ExplodingOn)

from dicetables_db.connections.sql_connection import SQLConnection
from dicetables_db.taskmanager import TaskManager
from dicetables_db.insertandretrieve import DiceTableInsertionAndRetrieval


class TestTaskManager(unittest.TestCase):

    def setUp(self):
        self.connection = SQLConnection(':memory:', 'test')
        self.insert_retrieve = DiceTableInsertionAndRetrieval(self.connection)
        self.task_manager = TaskManager(self.insert_retrieve)

    def test_init(self):
        manager = TaskManager(self.insert_retrieve)
        self.assertEqual(manager.step_size, 30)

        manager = TaskManager(self.insert_retrieve, 40)
        self.assertEqual(manager.step_size, 40)
        self.assertIs(manager._insert_retrieve, self.insert_retrieve)

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

    def test_process_request_empty_record(self):
        q = Queue()
        request = DiceRecord.new()
        answer = self.task_manager.process_request(request, q)
        self.assertEqual(answer, DiceTable.new())
        self.assertEqual(q.get(), 'STOP')

    def test_process_request_returns_correct_table_too_small_to_save(self):
        request = DiceRecord({Die(5): 2, Die(3): 2})
        answer = self.task_manager.process_request(request)
        expected = DiceTable.new().add_die(Die(5), 2).add_die(Die(3), 2)

        self.assertEqual(answer, expected)
        self.assertEqual(self.connection.find(), [])

    def test_process_request_returns_correct_table_partly_too_small_to_save(self):
        request = DiceRecord({Die(5): 2, Die(3): 50})
        answer = self.task_manager.process_request(request)
        expected = DiceTable.new().add_die(Die(5), 2).add_die(Die(3), 50)

        self.assertEqual(answer, expected)

    def test_process_request_returns_correct_table_intermediate_is_exact(self):
        request = DiceRecord({Die(5): 6, Die(3): 50})
        answer = self.task_manager.process_request(request)
        expected = DiceTable.new().add_die(Die(5), 6).add_die(Die(3), 50)

        self.assertEqual(answer, expected)
        self.assertTrue(self.insert_retrieve.has_table(answer))

    def test_process_request_saves_intermediary_tables(self):
        request = DiceRecord({Die(3): 20, Die(5): 12})
        answer = self.task_manager.process_request(request)

        self.assertTrue(self.task_manager.step_size, 30)

        saved = [DiceTable.new().add_die(Die(3), 10),
                 DiceTable.new().add_die(Die(3), 20),
                 DiceTable.new().add_die(Die(3), 20).add_die(Die(5), 6),
                 DiceTable.new().add_die(Die(3), 20).add_die(Die(5), 12)]

        for table in saved:
            self.assertTrue(self.insert_retrieve.has_table(table))
        self.assertEqual(len(self.connection.find()), 4)

        self.assertEqual(answer, saved[-1])

    def test_process_request_returns_correct_table_with_modifiers(self):
        modifier = Modifier(10)
        mod_die = ModDie(5, 2)
        mod_weighted = ModWeightedDie({1: 2, 3: 4}, -2)

        request = DiceRecord({modifier: 6, mod_die: 2, mod_weighted: 3})
        answer = self.task_manager.process_request(request)

        expected = DiceTable.new().add_die(modifier, 6).add_die(mod_die, 2).add_die(mod_weighted, 3)

        self.assertEqual(answer, expected)

    def test_process_request_saves_intermediary_tables_without_their_modifiers(self):
        modifier = Modifier(10)
        mod_die = ModDie(5, 2)
        mod_weighted = ModWeightedDie({1: 2, 2: 5, 3: 4}, -2)

        die = Die(5)
        weighted = WeightedDie({1: 2, 2: 5, 3: 4})

        request = DiceRecord({modifier: 6, mod_die: 12, mod_weighted: 20})
        self.task_manager.process_request(request)

        self.assertTrue(self.task_manager.step_size, 30)

        saved = [DiceTable.new().add_die(weighted, 10),
                 DiceTable.new().add_die(weighted, 20),
                 DiceTable.new().add_die(weighted, 20).add_die(die, 6),
                 DiceTable.new().add_die(weighted, 20).add_die(die, 12)]

        for table in saved:
            self.assertTrue(self.insert_retrieve.has_table(table))
        self.assertEqual(len(self.connection.find()), 4)

    def test_process_request_regression_test_check_times(self):
        big_request_one = DiceRecord({Die(6): 500})
        big_request_two = DiceRecord({ModDie(6, 5): 500})

        start = clock()
        self.task_manager.process_request(big_request_one)
        first_request_time = clock() - start

        number_of_saved_tables = len(self.connection.find())

        start = clock()
        self.task_manager.process_request(big_request_two)
        second_request_time = clock() - start

        self.assertEqual(len(self.connection.find()), number_of_saved_tables)

        self.assertTrue(first_request_time > 10 * second_request_time)

    def test_process_request_all_die_types_below_step_size(self):
        weighted_dict = {1: 2, 3: 4}

        modifier = Modifier(5)
        die = Die(5)
        mod_die = ModDie(5, -10)
        weighted = WeightedDie(weighted_dict)
        mod_weighted = ModWeightedDie(weighted_dict, 4)
        strong = StrongDie(die, 2)
        exploding = Exploding(die, explosions=1)
        exploding_on = ExplodingOn(die, (1,), explosions=1)

        request = DiceRecord({modifier: 1, die: 1, mod_die: 1, weighted: 1, mod_weighted: 1,
                              strong: 1, exploding: 1, exploding_on: 1})
        expected = DiceTable.new().add_die(modifier).add_die(die).add_die(mod_die).add_die(weighted)
        expected = expected.add_die(mod_weighted).add_die(strong).add_die(exploding).add_die(exploding_on)

        answer = self.task_manager.process_request(request)

        self.assertEqual(expected, answer)
        self.assertEqual(self.connection.find(), [])

    def test_process_request_with_queue_no_tables_saved(self):
        q = Queue()
        request = DiceRecord({Die(6): 4})
        answer = self.task_manager.process_request(request, update_queue=q)

        self.assertEqual(answer, DiceTable.new().add_die(Die(6), 4))

        self.assertEqual(q.get(), 'STOP')

    def test_process_request_with_queue(self):
        q = Queue()
        request = DiceRecord({Die(6): 20})
        answer = self.task_manager.process_request(request, update_queue=q)

        self.assertEqual(answer, DiceTable.new().add_die(Die(6), 20))

        expected_reprs = ['<DiceTable containing [5D6]>', '<DiceTable containing [10D6]>',
                          '<DiceTable containing [15D6]>', '<DiceTable containing [20D6]>', ]
        for table_repr in expected_reprs:
            self.assertEqual(q.get(), table_repr)
        self.assertEqual(q.get(), 'STOP')

    def test_process_request_with_queue_no_tables_saved_because_already_saved(self):
        initial_queue = Queue()
        request = DiceRecord({Die(6): 10})
        answer = self.task_manager.process_request(request, update_queue=initial_queue)
        expected = DiceTable.new().add_die(Die(6), 10)

        self.assertEqual(answer, expected)

        expected_reprs = ['<DiceTable containing [5D6]>', '<DiceTable containing [10D6]>']
        for table_repr in expected_reprs:
            self.assertEqual(initial_queue.get(), table_repr)
        self.assertEqual(initial_queue.get(), 'STOP')

        second_queue = Queue()
        second_answer = self.task_manager.process_request(request, update_queue=second_queue)

        self.assertEqual(second_queue.get(), 'STOP')
        self.assertEqual(second_answer, expected)

    def test_process_request_each_die_type(self):
        all_dice = [Modifier(3), Die(3), ModDie(3, -1), WeightedDie({1: 2, 2: 1}), ModWeightedDie({1: 2, 2: 1}, 4),
                    StrongDie(Die(3), 3), Exploding(Die(3), explosions=1), ExplodingOn(Die(3), (1, ), explosions=1)]

        add_times = 2
        one_step = TaskManager(self.insert_retrieve, step_size=1)

        for die in all_dice:
            request = DiceRecord.new().add_die(die, add_times)
            answer = one_step.process_request(request)
            expected = DiceTable.new().add_die(die, add_times)
            self.assertEqual(answer, expected)

    def test_process_request_all_die_types_above_step_size_and_gets_from_database(self):
        weighted_dict = {1: 2, 3: 4}

        modifier = Modifier(5)
        die = Die(5)
        mod_die = ModDie(5, -10)
        weighted = WeightedDie(weighted_dict)
        mod_weighted = ModWeightedDie(weighted_dict, 4)
        strong = StrongDie(die, 2)
        exploding = Exploding(die, explosions=1)
        exploding_on = ExplodingOn(die, (1,), explosions=1)

        request = DiceRecord({modifier: 10, die: 10, mod_die: 10, weighted: 10, mod_weighted: 10,
                              strong: 10, exploding: 10, exploding_on: 10})

        expected = DiceTable.new().add_die(modifier, 10).add_die(die, 10).add_die(mod_die, 10).add_die(weighted, 10)
        expected = expected.add_die(mod_weighted, 10).add_die(strong, 10)
        expected = expected.add_die(exploding, 10).add_die(exploding_on, 10)

        one_step = TaskManager(self.insert_retrieve, step_size=1)

        initial_queue = Queue()
        initial_answer = one_step.process_request(request, initial_queue)

        initial_db_size = len(self.connection.find())

        second_queue = Queue()
        second_answer = one_step.process_request(request, second_queue)

        second_db_size = len(self.connection.find())

        self.assertEqual(expected, initial_answer, second_answer)
        self.assertEqual(initial_db_size, second_db_size, 70)

        self.assertEqual(initial_queue.qsize(), 71)
        self.assertEqual(second_queue.qsize(), 1)


if __name__ == '__main__':
    unittest.main()
