import unittest
import pickle
import dbinterface as dbi
import dicetables as dt


class TestDBConnect(unittest.TestCase):
    test_dbi = dbi.DBConnect(':memory:', add_path=False)

    def setUp(self):
        self.test_dbi.reset_table()

    def tearDown(self):
        pass

    def test_has_dicetables_true(self):
        self.assertTrue(self.test_dbi.has_dicetables())

    def test_has_dicetables_false(self):
        self.test_dbi.cursor.execute('drop table dicetables;')
        self.assertFalse(self.test_dbi.has_dicetables())

    def test_is_dicetables_correct_false_by_col_num(self):
        self.test_dbi.cursor.execute('ALTER TABLE dicetables ADD oops TEXT')
        self.assertFalse(self.test_dbi.is_dicetables_correct())

    def test_is_dicetables_correct_false_by_col_info(self):
        self.test_dbi.cursor.execute('drop table dicetables')
        self.test_dbi.cursor.execute('create table dicetable (id TEXT, dt TEXT)')
        self.assertFalse(self.test_dbi.is_dicetables_correct())

    def test_is_empty_true(self):
        self.test_dbi.cursor.execute('drop table dicetables')
        self.assertTrue(self.test_dbi.is_empty())

    def test_is_empty_false(self):
        self.assertFalse(self.test_dbi.is_empty())

    def test_get_tables_default(self):
        self.assertEqual(self.test_dbi.get_tables(), ['dicetables'])

    def test_get_tables_with_more_tables(self):
        self.test_dbi.cursor.execute('create table bob (a TEXT)')
        self.test_dbi.cursor.execute('create table rob (a TEXT)')
        self.assertEqual(self.test_dbi.get_tables(), ['dicetables', 'bob', 'rob'])

    def test_get_table_data(self):
        answer = [(0, 'a', 'TEXT', 0, None, 0)]
        self.test_dbi.cursor.execute('create table bob (a TEXT)')
        self.assertEqual(self.test_dbi.get_table_data('bob'), answer)

    def test_get_table_data_all(self):
        answer_bob = [(0, 'a', 'TEXT', 0, None, 0)]
        answer_dt = [(0, 'id', 'INTEGER', 0, None, 1), (1, 'dt', 'BLOB', 0, None, 0)]
        self.test_dbi.cursor.execute('create table bob (a TEXT)')
        self.assertEqual(self.test_dbi.get_table_data_all(), [('dicetables', answer_dt), ('bob', answer_bob)])

    def test_reset_table(self):
        self.test_dbi.cursor.execute('create table bob (a TEXT)')
        self.test_dbi.cursor.execute('insert into dicetables (id, dt) values(?, ?)', (5, 'hello'))
        self.test_dbi.reset_table()
        self.assertEqual(self.test_dbi.get_tables(), ['dicetables'])
        self.assertEqual(self.test_dbi.cursor.execute('select * from dicetables').fetchall(), [])


class TestDiceTableInjector(unittest.TestCase):
    connection = dbi.DBConnect(':memory:', add_path=False)

    def setUp(self):
        self.connection.reset_table()
        self.injector = dbi.DiceTableInjector(self.connection)

    def tearDown(self):
        del self.injector

    def test_init_empty(self):
        self.assertEqual(self.injector.current_id, 0)
        self.assertEqual(self.injector.priorities, 0)

    def test_init_non_empty(self):
        self.connection.cursor.execute('create table bob (a TEXT)')
        self.connection.cursor.execute('insert into dicetables (id, dt) values(?, ?)', (100, 'hi'))
        test = dbi.DiceTableInjector(self.connection)
        self.assertEqual(test.priorities, 1)
        self.assertEqual(test.current_id, 101)

    def test_add_table_adds_a_priority_table_and_adds_table(self):
        dice_table = dt.DiceTable.new().add_die(dt.Die(2), 2)

        self.injector.add_table(dice_table)

        self.assertEqual(self.connection.get_tables(), ['dicetables', 'priority0'])

        priority0 = self.connection.cursor.execute('select * from priority0').fetchall()
        stored_tables = self.connection.cursor.execute('select * from dicetables').fetchall()
        self.assertEqual(len(stored_tables), 1)
        self.assertEqual(len(priority0), 1)

        id_num, table_bytes = stored_tables[0]
        retrieved_table = pickle.loads(table_bytes)

        self.assertEqual(id_num, 0)
        self.assertEqual(retrieved_table.get_dict(), dice_table.get_dict())
        self.assertEqual(retrieved_table.get_list(), dice_table.get_list())
        self.assertEqual(priority0[0], ('Die(2)', 2, 0))

    def test_add_table_add_does_not_add_new_priority_table(self):
        dice_table1 = dt.DiceTable.new().add_die(dt.Die(2), 1)
        dice_table2 = dt.DiceTable.new().add_die(dt.Die(4), 1)

        self.injector.add_table(dice_table1)
        self.injector.add_table(dice_table2)

        self.assertEqual(self.connection.get_tables(), ['dicetables', 'priority0'])

        priority0 = self.connection.cursor.execute('select * from priority0').fetchall()
        stored_tables = self.connection.cursor.execute('select * from dicetables').fetchall()
        self.assertEqual(len(stored_tables), 2)
        self.assertEqual(len(priority0), 2)

        self.assertEqual(priority0[0], ('Die(2)', 1, 0))
        self.assertEqual(priority0[1], ('Die(4)', 1, 1))

    def test_add_table_multiple_priority_tables(self):
        dice_table1 = dt.DiceTable.new().add_die(dt.Die(2), 1)
        dice_table2 = dt.DiceTable.new().add_die(dt.Die(4), 1).add_die(dt.Die(5), 2).add_die(dt.Die(6), 3)

        self.injector.add_table(dice_table1)
        self.injector.add_table(dice_table2)

        self.assertEqual(self.connection.get_tables(), ['dicetables', 'priority0', 'priority1', 'priority2'])

        priority0 = self.connection.cursor.execute('select * from priority0').fetchall()
        priority1 = self.connection.cursor.execute('select * from priority1').fetchall()
        priority2 = self.connection.cursor.execute('select * from priority2').fetchall()
        self.assertEqual(len(priority0), 2)

        self.assertEqual(priority0, [('Die(2)', 1, 0), ('Die(6)', 3, 1)])
        self.assertEqual(priority1, [('Die(5)', 2, 1)])
        self.assertEqual(priority2, [('Die(4)', 1, 1)])

        self.assertEqual(self.injector.priorities, 3)


    def test_prioritize(self):
        test = (dt.WeightedDie({1: 2, 2: 3}), 10)
        self.assertEqual(dbi.prioritize(test), 2**2 * 100 + 5)

    def test_create_priority_list(self):
        d2 = dt.Die(2)
        d3_plus = dt.WeightedDie({1: 2, 3: 4})
        d3 = dt.Die(3)
        d4 = dt.Die(4)
        """
        algorithm is diesize**2 * times**2 + dieweight
        """
        test = dt.DiceTable.new().add_die(d3_plus, 4)  # priority 9 * 16 + 6  = 150
        test = test.add_die(d3, 4)  # priority 9 * 16 = 144
        test = test.add_die(d2, 5)  # priority 4 * 25 = 100
        test = test.add_die(d4, 2)  # priority 16 * 4 = 64
        expected = [(d3_plus, 4), (d3, 4), (d2, 5), (d4, 2)]
        self.assertEqual(dbi.create_priority_list(test), expected)

    def test_create_priority_list_one_type(self):
        test = dt.DiceTable.new().add_die(dt.Die(2), 3)
        self.assertEqual(dbi.create_priority_list(test), [(dt.Die(2), 3)])


if __name__ == '__main__':
    unittest.main()
