import pickle
import unittest

import dicetables as dt

from dt_sql import dbinterface as dbi


class TestToolFuncs(unittest.TestCase):
    def test_get_combos_empty(self):
        self.assertEqual(next(dbi.get_combos([])), [()])

    def test_get_combos_one_element(self):
        to_test = dbi.get_combos([1])
        self.assertEqual(next(to_test), [(1,)])
        self.assertEqual(next(to_test), [()])

    def test_get_combos_many_elements(self):
        to_test = dbi.get_combos([1, 2, 3])
        self.assertEqual(next(to_test), [(1, 2, 3)])
        self.assertEqual(next(to_test), [(1, 2), (1, 3), (2, 3)])
        self.assertEqual(next(to_test), [(1,), (2,), (3,)])
        self.assertEqual(next(to_test), [()])

    def test_generate_table_names(self):
        dice_list = [(dt.Die(1), 2), (dt.Die(2), 3), (dt.Die(3), 2)]
        to_test = dbi.generate_table_names(dice_list)
        self.assertEqual(next(to_test), ['Die(1)&Die(2)&Die(3)'])
        self.assertEqual(next(to_test), ['Die(1)&Die(2)', 'Die(1)&Die(3)', 'Die(2)&Die(3)'])
        self.assertEqual(next(to_test), ['Die(1)', 'Die(2)', 'Die(3)'])
        self.assertEqual(next(to_test), [''])

    def test_get_dice_score_empty(self):
        self.assertEqual(dbi.get_dice_score([]), 0)

    def test_get_dice_score_one(self):
        self.assertEqual(dbi.get_dice_score([(dt.ModDie(3, 10), 4)]), 12)

    def test_get_dice_score_non_zero_weight_normal(self):
        self.assertEqual(dbi.get_dice_score([(dt.WeightedDie({1: 1, 5: 6}), 4)]), 24)  # size + 1

    def test_get_dice_score_non_zero_weight_too_low(self):
        self.assertEqual(dbi.get_dice_score([(dt.WeightedDie({1: 1, 5: 1}), 4)]), 20)  # size not + 1

    def test_get_dice_score_multiple(self):
        self.assertEqual(dbi.get_dice_score([(dt.WeightedDie({1: 1, 5: 6}), 4), (dt.ModDie(3, 10), 4)]), 36)


class TestConnection(unittest.TestCase):
    connection = dbi.Connection(':memory:')

    def setUp(self):
        self.connection.reset_table()

    def tearDown(self):
        pass

    def test_has_master_true(self):
        self.assertTrue(self.connection.has_master())

    def test_has_master_false(self):
        self.connection.cursor.execute('drop table master;')
        self.assertFalse(self.connection.has_master())

    def test_is_master_correct_false_by_col_num(self):
        self.connection.cursor.execute('drop table master')
        self.connection.cursor.execute('create table master (id TEXT, bytes BLOB)')
        self.assertFalse(self.connection.is_master_correct())

    def test_is_master_correct_false_by_col_info(self):
        self.connection.cursor.execute('drop table master')
        self.connection.cursor.execute('create table master (id TEXT, bytes TEXT)')
        self.assertFalse(self.connection.is_master_correct())

    def test_is_empty_true(self):
        self.connection.cursor.execute('drop table master')
        self.assertTrue(self.connection.is_empty())

    def test_is_empty_false(self):
        self.assertFalse(self.connection.is_empty())

    def test_get_tables_default(self):
        self.assertEqual(self.connection.get_tables(), ['master'])

    def test_get_tables_with_more_tables(self):
        self.connection.cursor.execute('create table bob (a TEXT)')
        self.connection.cursor.execute('create table rob (a TEXT)')
        self.assertEqual(self.connection.get_tables(), ['master', 'bob', 'rob'])

    def test_get_table_data(self):
        answer = [(0, 'a', 'TEXT', 0, None, 0)]
        self.connection.cursor.execute('create table bob (a TEXT)')
        self.assertEqual(self.connection.get_table_data('bob'), answer)

    def test_reset_table(self):
        self.connection.cursor.execute('create table bob (a TEXT)')
        self.connection.cursor.execute('insert into master (id, bytes) values(?, ?)', (5, 'hello'))
        self.connection.reset_table()
        self.assertEqual(self.connection.get_tables(), ['master'])
        self.assertEqual(self.connection.cursor.execute('select * from master').fetchall(), [])


class TestDiceTableInjector(unittest.TestCase):
    connection = dbi.Connection(':memory:')

    def setUp(self):
        self.connection.reset_table()
        self.injector = dbi.DiceTableInjector(self.connection)

    def tearDown(self):
        del self.injector

    def test_InMemoryInformation_init_empty(self):
        info = dbi.InMemoryInformation(self.connection)
        self.assertEqual(info.available_id, 0)
        self.assertEqual(info.tables, ['master'])
        self.assertEqual(info.dice, [])

    def test_InMemoryInformation_init_non_empty(self):
        self.connection.cursor.execute('create table bob (a TEXT)')
        self.connection.cursor.execute('insert into master (id, bytes) values(?, ?)', (100, 'hi'))
        self.connection.cursor.execute('alter table master add column [Die(2)] INTEGER DEFAULT 0')
        self.connection.cursor.execute('alter table master add column [Die(1)] INTEGER DEFAULT 0')

        info = dbi.InMemoryInformation(self.connection)
        self.assertEqual(info.tables, ['master', 'bob'])
        self.assertEqual(info.available_id, 101)
        self.assertEqual(info.dice, ['Die(2)', 'Die(1)'])

    def test_InMemoryInformation_refresh_information(self):
        info = dbi.InMemoryInformation(self.connection)
        self.connection.cursor.execute('create table bob (a TEXT)')
        self.connection.cursor.execute('insert into master (id, bytes) values(?, ?)', (100, 'hi'))
        self.connection.cursor.execute('alter table master add column [Die(2)] INTEGER DEFAULT 0')
        self.connection.cursor.execute('alter table master add column [Die(1)] INTEGER DEFAULT 0')

        info.refresh_information()
        self.assertEqual(info.tables, ['master', 'bob'])
        self.assertEqual(info.available_id, 101)
        self.assertEqual(info.dice, ['Die(2)', 'Die(1)'])

    def test_InMemoryInformation_properties_do_not_mutate(self):
        info = dbi.InMemoryInformation(self.connection)
        info_tables = info.tables
        info_dice = info.dice
        info_tables.append(5)
        info_dice.append(5)

        self.assertEqual(info.tables, ['master'])
        self.assertEqual(info.dice, [])

    def test_InMemoryInformation_has_die_column_true(self):
        self.connection.cursor.execute('alter table master add column [Die(1)] INTEGER DEFAULT 0')
        self.assertTrue(dbi.InMemoryInformation(self.connection).has_die_column('Die(1)'))

    def test_InMemoryInformation_has_die_column_false(self):
        self.connection.cursor.execute('alter table master add column [Die(1)] INTEGER DEFAULT 0')
        self.assertFalse(dbi.InMemoryInformation(self.connection).has_die_column('Die(2)'))

    def test_InMemoryInformation_has_table_true(self):
        self.assertTrue(dbi.InMemoryInformation(self.connection).has_table('master'))

    def test_InMemoryInformation_has_table_false(self):
        self.assertFalse(dbi.InMemoryInformation(self.connection).has_table('nonsense'))

    def test_InMemoryInformation_add_table_not_already_present(self):
        info = dbi.InMemoryInformation(self.connection)
        info.add_table('new_table')
        self.assertEqual(info.tables, ['master', 'new_table'])

    def test_InMemoryInformation_add_table_already_present(self):
        info = dbi.InMemoryInformation(self.connection)
        info.add_table('master')
        self.assertEqual(info.tables, ['master'])

    def test_InMemoryInformation_add_die_column_not_already_present(self):
        info = dbi.InMemoryInformation(self.connection)
        info.add_die_column('Die(3)')
        self.assertEqual(info.dice, ['Die(3)'])

    def test_InMemoryInformation_add_die_column_already_present(self):
        info = dbi.InMemoryInformation(self.connection)
        info.add_die_column('Die(3)')
        info.add_die_column('Die(3)')
        self.assertEqual(info.dice, ['Die(3)'])

    def test_InMemoryInformation_increment_id(self):
        info = dbi.InMemoryInformation(self.connection)
        info.increment_id()
        self.assertEqual(info.available_id, 1)
        info.increment_id()
        self.assertEqual(info.available_id, 2)

    def test_add_table_updates_info(self):
        dice_table = dt.DiceTable.new().add_die(dt.Die(2), 2)
        self.injector.add_table(dice_table)

        col_names = [info[1] for info in self.connection.get_table_data('master')]
        self.assertEqual(col_names, ['id', 'bytes', 'dice_score', 'Die(2)'])
        self.assertEqual(self.connection.get_tables(), ['master', 'Die(2)'])

        self.assertEqual(self.injector.info.dice, ['Die(2)'])
        self.assertEqual(self.injector.info.available_id, 1)
        self.assertEqual(self.injector.info.tables, ['master', 'Die(2)'])

    def test_add_table_does_not_create_new_dice_cols_if_same_die_types(self):
        dice_table = dt.DiceTable.new().add_die(dt.Die(2), 2)
        other_table = dt.DiceTable.new().add_die(dt.Die(2), 3)
        self.injector.add_table(dice_table)
        self.injector.add_table(other_table)
        col_names = [info[1] for info in self.connection.get_table_data('master')]
        self.assertEqual(col_names, ['id', 'bytes', 'dice_score', 'Die(2)'])
        self.assertEqual(self.injector.info.dice, ['Die(2)'])
        self.assertEqual(self.injector.info.tables, ['master', 'Die(2)'])

    def test_add_table_does_create_new_dice_cols_and_tables_if_new_die_types(self):
        dice_table = dt.DiceTable.new().add_die(dt.Die(2), 2)
        other_table = dt.DiceTable.new().add_die(dt.Die(2), 3).add_die(dt.Die(1))
        self.injector.add_table(dice_table)
        self.injector.add_table(other_table)

        master_col_names = [info[1] for info in self.connection.get_table_data('master')]
        die_1_die_2_col_names = [info[1] for info in self.connection.get_table_data('Die(1)&Die(2)')]
        self.assertEqual(master_col_names, ['id', 'bytes', 'dice_score', 'Die(2)', 'Die(1)'])
        self.assertEqual(die_1_die_2_col_names, ['id', 'Die(1)', 'Die(2)'])

        self.assertEqual(self.injector.info.dice, ['Die(2)', 'Die(1)'])
        self.assertEqual(self.injector.info.tables, ['master', 'Die(2)', 'Die(1)&Die(2)'])

    def test_add_table_puts_correct_data_in_types_tables(self):
        dice_table0 = dt.DiceTable.new().add_die(dt.Die(2), 2)
        dice_table1 = dt.DiceTable.new().add_die(dt.Die(2), 3).add_die(dt.Die(1))
        dice_table2 = dt.DiceTable.new().add_die(dt.Die(2), 2).add_die(dt.Die(1))
        self.injector.add_table(dice_table0)
        self.injector.add_table(dice_table1)
        self.injector.add_table(dice_table2)
        die_2 = self.connection.cursor.execute('select * from [Die(2)]').fetchall()
        die_1_die_2 = self.connection.cursor.execute('select * from [Die(1)&Die(2)]').fetchall()
        self.assertEqual(die_2, [(0, 2)])
        self.assertEqual(die_1_die_2, [(1, 1, 3), (2, 1, 2)])

    def test_add_table_puts_correct_data_in_master_table(self):
        dice_table0 = dt.DiceTable.new().add_die(dt.Die(2), 2)
        dice_table1 = dt.DiceTable.new().add_die(dt.Die(2), 3).add_die(dt.Die(1))
        dice_table2 = dt.DiceTable.new().add_die(dt.Die(2), 2).add_die(dt.Die(1))
        self.injector.add_table(dice_table0)
        self.injector.add_table(dice_table1)
        self.injector.add_table(dice_table2)
        data = self.connection.cursor.execute('select [id], [Die(1)], [Die(2)] from master').fetchall()
        dice_tables = self.connection.cursor.execute('select [bytes] from master').fetchall()
        self.assertEqual(data, [(0, 0, 2), (1, 1, 3), (2, 1, 2)])
        self.assertEqual(dice_table0.get_dict(), pickle.loads(dice_tables[0][0]).get_dict())
        self.assertEqual(dice_table1.get_dict(), pickle.loads(dice_tables[1][0]).get_dict())
        self.assertEqual(dice_table2.get_dict(), pickle.loads(dice_tables[2][0]).get_dict())

    def test_find_nearest_table_no_match(self):
        dice_list = [(dt.Die(1), 1)]
        self.assertIsNone(self.injector.find_nearest_table(dice_list))

    def test_find_nearest_table_perfect_match_one_die(self):
        dice_table = dt.DiceTable.new().add_die(dt.Die(2))
        self.injector.add_table(dice_table)
        self.assertEqual(self.injector.find_nearest_table(dice_table.get_list()), 0)

    def test_find_nearest_table_perfect_match_multi_die(self):
        dice_table = dt.DiceTable.new().add_die(dt.Die(2)).add_die(dt.Die(3)).add_die(dt.Die(4))
        self.injector.add_table(dt.DiceTable.new().add_die(dt.Die(2)))
        self.injector.add_table(dice_table)
        self.assertEqual(self.injector.find_nearest_table(dice_table.get_list()), 1)


# todo bad test only check score!
    def test_find_nearest_table_nearest_match_multi_die(self):
        dice_table0 = dt.DiceTable.new().add_die(dt.Die(2))
        dice_table1 = dt.DiceTable.new().add_die(dt.Die(2)).add_die(dt.Die(3)).add_die(dt.Die(4))
        test_list0 = dice_table0.add_die(dt.Die(10)).get_list()
        test_list1 = dice_table1.add_die(dt.Die(2)).get_list()
        self.injector.add_table(dice_table0)
        self.injector.add_table(dice_table1)
        self.assertEqual(self.injector.find_nearest_table(test_list0), 0)
        self.assertEqual(self.injector.find_nearest_table(test_list1), 1)

# class TestTableRetriever(unittest.TestCase):
#     connection = dbi.Connection(':memory:', add_path=False)
#
#     def setUp(self):
#         self.connection.reset_table()
#         self.injector = dbi.DiceTableInjector(self.connection)
#         self.retriever = dbi.DiceTableRetriever(self.connection)
#
#     def tearDown(self):
#         del self.injector
#         del self.retriever
#
#     def test_get_candidates_empty_db_returns_empty_list(self):
#         table = dt.DiceTable.new().add_die(dt.Die(3))
#         self.assertEqual(self.retriever.get_candidates(table.get_list()), [])
#
#     def test_get_candidates_no_match_returns_empty_list(self):
#         table = dt.DiceTable.new().add_die(dt.Die(3))
#         self.injector.add_table(dt.DiceTable.new().add_die(dt.Die(2)))
#         self.assertEqual(self.retriever.get_candidates(table.get_list()), [])
#
#     def test_get_candidates_one_match_one_priority(self):
#         table = dt.DiceTable.new().add_die(dt.Die(3))
#         self.injector.add_table(table)  # id 0
#         self.injector.add_table(dt.DiceTable.new().add_die(dt.Die(2)))  # id 1
#         self.assertEqual(self.retriever.get_candidates(table.get_list()), [(0, 1)])
#
#     def test_get_candidates_many_matches_one_priority(self):
#         table = dt.DiceTable.new().add_die(dt.Die(3), 5)
#         self.injector.add_table(table)  # id 0
#         self.injector.add_table(dt.DiceTable.new().add_die(dt.Die(2)))  # id 1
#         self.injector.add_table(dt.DiceTable.new().add_die(dt.Die(3), 3))  # id 2
#         self.injector.add_table(dt.DiceTable.new().add_die(dt.Die(4)))  # id 3
#         self.injector.add_table(dt.DiceTable.new().add_die(dt.Die(3), 1))  # id 4
#         self.assertEqual(self.retriever.get_candidates(table.get_list()), [(0, 5), (2, 3), (4, 1)])
#
#     def test_get_candidates_many_matches_one_priority_does_not_include_matches_with_more_priorities(self):
#         table = dt.DiceTable.new().add_die(dt.Die(3), 5)
#         self.injector.add_table(table)  # id 0
#         self.injector.add_table(dt.DiceTable.new().add_die(dt.Die(2)))  # id 1
#         self.injector.add_table(dt.DiceTable.new().add_die(dt.Die(3), 3).add_die(dt.Die(2)))  # id 2
#         self.injector.add_table(dt.DiceTable.new().add_die(dt.Die(4)))  # id 3
#         self.injector.add_table(dt.DiceTable.new().add_die(dt.Die(3), 1))  # id 4
#         self.assertEqual(self.retriever.get_candidates(table.get_list()), [(0, 5), (4, 1)])
#
#     def test_get_candidates_many_matches_two_priorities(self):
#         table = dt.DiceTable.new().add_die(dt.Die(3), 5).add_die(dt.Die(2), 2)
#         self.injector.add_table(table)  # id 0 YES
#         self.injector.add_table(dt.DiceTable.new().add_die(dt.Die(2), 10).add_die(dt.Die(3)))  # id 1 NO
#         self.injector.add_table(dt.DiceTable.new().add_die(dt.Die(3), 3).add_die(dt.Die(2)))  # id 2 YES
#         self.injector.add_table(dt.DiceTable.new().add_die(dt.Die(3), 2).add_die(dt.Die(4)))  # id 3 NO
#         self.injector.add_table(dt.DiceTable.new().add_die(dt.Die(3), 1))  # id 4 YES
#         self.assertEqual(self.retriever.get_candidates(table.get_list()),
#                          [(0, 5, 2), (2, 3, 1), (4, 1, None)])
#
#     def test_get_candidates_many_matches_two_priorities_three_priorities_total(self):
#         table = dt.DiceTable.new().add_die(dt.Die(3), 5).add_die(dt.Die(2), 2)
#         self.injector.add_table(table)  # id 0 YES
#         self.injector.add_table(dt.DiceTable.new().add_die(dt.Die(2), 10).add_die(dt.Die(3)))  # id 1 NO
#         self.injector.add_table(dt.DiceTable.new().add_die(dt.Die(3), 3).add_die(dt.Die(2)))  # id 2 YES
#         self.injector.add_table(dt.DiceTable.new().add_die(dt.Die(3), 2).add_die(dt.Die(4)))  # id 3 NO
#         self.injector.add_table(dt.DiceTable.new().add_die(dt.Die(3), 1))  # id 4 YES
#         three_types = dt.DiceTable.new().add_die(dt.Die(3), 3).add_die(dt.Die(2)).add_die(dt.Die(1))
#         self.injector.add_table(three_types)  # id 5 NO
#         self.assertEqual(self.retriever.get_candidates(table.get_list()),
#                          [(0, 5, 2), (2, 3, 1), (4, 1, None)])
#
#     def test_get_candidates_out_of_order_matches(self):
#         table = dt.DiceTable.new().add_die(dt.Die(3), 5).add_die(dt.Die(2), 4)
#         reverse_match = dt.DiceTable.new().add_die(dt.Die(2), 4).add_die(dt.Die(3))
#         missing_first_match = dt.DiceTable.new().add_die(dt.Die(2), 4)
#         self.injector.add_table(reverse_match)
#         self.injector.add_table(missing_first_match)
#         self.assertEqual(self.retriever.get_candidates(table.get_list()),
#                          [(0, None, 4), (1, 1, 4)])



if __name__ == '__main__':
    unittest.main()
