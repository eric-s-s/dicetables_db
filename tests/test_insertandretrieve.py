import unittest

import dicetables as dt

import dicetables_db.insertandretrieve as dbi
from dicetables_db.dbprep import Serializer
from dicetables_db.connections.sql_connection import SQLConnection
from tests.connections.test_baseconnection import MockConnection
from dicetables_db.connections.mongodb_connection import MongoDBConnection


class TestDBInterface(unittest.TestCase):
    @staticmethod
    def get_connection():
        return MockConnection('test_collection')

    def setUp(self):
        self.connection = self.get_connection()
        self.interface = dbi.DiceTableInsertionAndRetrieval(self.connection)
        self.interface.reset()

    def tearDown(self):
        self.connection.close()

    def test_connection_info(self):
        self.assertEqual(self.interface.connection_info, self.connection.get_info())

    def test_init_creates_index(self):
        new_conn = SQLConnection(':memory:', 'another_collection')
        self.assertEqual(new_conn.get_info()['indices'], [])
        dbi.DiceTableInsertionAndRetrieval(new_conn)

        self.assertEqual(new_conn.get_info()['indices'], [('group', 'score')])

    def test_has_required_index_true(self):
        self.assertTrue(self.interface.has_required_index())

    def test_has_required_index_false(self):
        self.connection.reset_collection()
        self.assertFalse(self.interface.has_required_index())

    def test_reset_creates_index(self):
        self.connection.reset_collection()
        self.interface.reset()
        self.assertTrue(self.interface.has_required_index())

    def test_has_table_true(self):
        self.interface.add_table(dt.DiceTable.new().add_die(dt.Die(3)))
        self.assertTrue(self.interface.has_table(dt.DiceTable.new().add_die(dt.Die(3))))

    def test_has_table_false(self):
        self.assertFalse(self.interface.has_table(dt.DiceTable.new().add_die(dt.Die(3))))

    def test_add_table_return_document_id(self):
        doc_id = self.interface.add_table(dt.DiceTable.new().add_die(dt.Die(1)))
        document = self.connection.find_one()
        doc_id = document['_id']
        self.assertEqual(doc_id, doc_id)

    def test_add_table_adds_correctly(self):
        table = dt.DiceTable.new().add_die(dt.Die(2))
        doc_id = self.interface.add_table(table)
        table_data = Serializer.serialize(table)
        expected = {'_id': doc_id, 'group': 'Die(2)', 'serialized': table_data, 'score': 2, 'Die(2)': 1}
        document = self.connection.find_one()
        self.assertEqual(document, expected)

    def test_find_nearest_table_no_match(self):
        dice_list = [(dt.Die(1), 1)]
        self.assertIsNone(self.interface.find_nearest_table(dice_list))

    def test_find_nearest_table_perfect_match_one_die(self):
        dice_table = dt.DiceTable.new().add_die(dt.Die(2))
        doc_id = self.interface.add_table(dice_table)
        self.assertEqual(self.interface.find_nearest_table(dice_table.get_list()), doc_id)

    def test_find_nearest_table_perfect_match_multi_die(self):
        dice_table = dt.DiceTable.new().add_die(dt.Die(2)).add_die(dt.Die(3)).add_die(dt.Die(4))
        same_score_not_it = dt.DiceTable.new().add_die(dt.Die(2), 2).add_die(dt.Die(3))
        self.interface.add_table(dt.DiceTable.new().add_die(dt.Die(2)))
        doc_id = self.interface.add_table(dice_table)
        self.interface.add_table(same_score_not_it)
        self.assertEqual(self.interface.find_nearest_table(dice_table.get_list()), doc_id)

    def test_find_nearest_table_nearest_match_multi_die(self):
        dice_table0 = dt.DiceTable.new().add_die(dt.Die(2))
        dice_table1 = dt.DiceTable.new().add_die(dt.Die(2)).add_die(dt.Die(3)).add_die(dt.Die(4))
        test_list0 = dice_table0.add_die(dt.Die(10)).get_list()
        test_list1 = dice_table1.add_die(dt.Die(2)).get_list()
        dice_table0_id = self.interface.add_table(dice_table0)
        dice_table1_id = self.interface.add_table(dice_table1)
        self.assertEqual(self.interface.find_nearest_table(test_list0), dice_table0_id)
        self.assertEqual(self.interface.find_nearest_table(test_list1), dice_table1_id)

    def test_find_nearest_table_scores_same_group_same(self):
        dice_table0 = dt.DiceTable.new().add_die(dt.Die(2), 3).add_die(dt.Die(3)).add_die(dt.Die(4))
        dice_table1 = dt.DiceTable.new().add_die(dt.Die(2)).add_die(dt.Die(3)).add_die(dt.Die(4), 2)
        test_list1 = [(dt.Die(2), 1), (dt.Die(3), 2), (dt.Die(4), 10), (dt.Die(10), 100)]
        self.interface.add_table(dice_table0)
        dice_table1_id = self.interface.add_table(dice_table1)
        self.assertEqual(self.interface.find_nearest_table(test_list1), dice_table1_id)

    def test_get_table(self):
        table = dt.DiceTable.new().add_die(dt.Die(2))
        doc_id = self.interface.add_table(table)
        new_table = self.interface.get_table(doc_id)
        self.assertEqual(new_table, table)

    def test_Finder_get_exact_match_returns_None(self):
        finder = dbi.Finder(self.connection, [(dt.Die(2), 1)])
        self.assertIsNone(finder.get_exact_match())

    def test_Finder_get_exact_match_returns_correct_id(self):
        finder = dbi.Finder(self.connection, [(dt.Die(2), 1)])
        doc_id = self.interface.add_table(dt.DiceTable.new().add_die(dt.Die(2)))
        self.assertEqual(finder.get_exact_match(), doc_id)

    def test_Finder_find_nearest_table_no_match(self):
        dice_list = [(dt.Die(1), 1)]
        finder = dbi.Finder(self.connection, dice_list)
        self.assertIsNone(finder.find_nearest_table())

    def test_Finder_find_nearest_table_perfect_match_one_die(self):
        dice_table = dt.DiceTable.new().add_die(dt.Die(2))
        doc_id = self.interface.add_table(dice_table)
        finder = dbi.Finder(self.connection, dice_table.get_list())
        self.assertEqual(finder.find_nearest_table(), doc_id)

    def test_Finder_find_nearest_table_perfect_match_multi_die(self):
        exact_match = dt.DiceTable.new().add_die(dt.Die(2)).add_die(dt.Die(3)).add_die(dt.Die(4))
        finder = dbi.Finder(self.connection, exact_match.get_list())

        same_score_not_it = dt.DiceTable.new().add_die(dt.Die(2), 2).add_die(dt.Die(3))
        self.interface.add_table(dt.DiceTable.new().add_die(dt.Die(2)))
        doc_id = self.interface.add_table(exact_match)
        self.interface.add_table(same_score_not_it)

        self.assertEqual(finder.find_nearest_table(), doc_id)

    def test_Finder_find_nearest_table_nearest_match_multi_die(self):
        dice_table0 = dt.DiceTable.new().add_die(dt.Die(2))
        dice_table1 = dt.DiceTable.new().add_die(dt.Die(2)).add_die(dt.Die(3)).add_die(dt.Die(4))
        test_list0 = dice_table0.add_die(dt.Die(10)).get_list()
        test_list1 = dice_table1.add_die(dt.Die(2)).get_list()

        finder0 = dbi.Finder(self.connection, test_list0)
        finder1 = dbi.Finder(self.connection, test_list1)

        dice_table0_id = self.interface.add_table(dice_table0)
        dice_table1_id = self.interface.add_table(dice_table1)

        self.assertEqual(finder0.find_nearest_table(), dice_table0_id)
        self.assertEqual(finder1.find_nearest_table(), dice_table1_id)

    def test_Finder_find_nearest_table_scores_same_group_same(self):
        dice_table0 = dt.DiceTable.new().add_die(dt.Die(2), 3).add_die(dt.Die(3)).add_die(dt.Die(4))
        dice_table1 = dt.DiceTable.new().add_die(dt.Die(2)).add_die(dt.Die(3)).add_die(dt.Die(4), 2)
        test_list1 = [(dt.Die(2), 1), (dt.Die(3), 2), (dt.Die(4), 10), (dt.Die(10), 100)]

        finder1 = dbi.Finder(self.connection, test_list1)

        self.interface.add_table(dice_table0)
        dice_table1_id = self.interface.add_table(dice_table1)

        self.assertEqual(finder1.find_nearest_table(), dice_table1_id)


class TestDBInterfaceWithSQL(TestDBInterface):
    @staticmethod
    def get_connection():
        return SQLConnection(':memory:', 'test_collection')


class TestDBInterfaceWithMongoDB(TestDBInterface):
    @staticmethod
    def get_connection():
        return MongoDBConnection('test', 'test_collection')


if __name__ == '__main__':
    unittest.main()
