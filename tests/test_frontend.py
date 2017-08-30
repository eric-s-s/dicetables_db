import unittest

from dicetables_db.frontend import (TableCreator, make_up_difference, is_new_table, create_insert_retrieve,
                                    ServerTalker, parse_list, get_mod_and_new_list)
from dicetables_db.insertandretrieve import DiceTableInsertionAndRetrieval
from dicetables_db.connections.mongodb_connection import MongoDBConnection
from dicetables_db.connections.sql_connection import SQLConnection

class TestFrontEnd(unittest.TestCase):
    def test_table_creator(self):
        TableCreator.create_for_mongo_db('test', 'test_db')


#
#
# class MockInterface(object):
#     dice_tables = (dt.DiceTable.new().add_die(dt.Die(2)),
#                    dt.DiceTable.new().add_die(dt.Die(2), 10),
#                    dt.DiceTable.new().add_die(dt.Die(3)),
#                    dt.DiceTable.new().add_die(dt.Die(3), 10)
#                    )
#
#     def __init__(self):
#         self._table_dict = {}
#         self.reset()
#
#     def reset(self):
#         self._table_dict = {id_num: dice_table for id_num, dice_table in enumerate(self.dice_tables)}
#
#     def has_table(self, dice_table):
#         return dice_table in self._table_dict.values()
#
#     def add_table(self, dice_table):
#         new_id = max(self._table_dict.keys()) + 1
#         self._table_dict[new_id] = dice_table
#
#     @staticmethod
#     def find_nearest_table(dice_list):
#         just_dice = [pair[0] for pair in dice_list]
#         if dt.Die(2) in just_dice:
#             return '0'
#         if dt.Die(3) in just_dice:
#             return '2'
#         return None
#
#     def get_table(self, id_str):
#         return self._table_dict[int(id_str)]
#
#
# class TestFrontEnd(unittest.TestCase):
#     # connection = dbi.MongoDBConnection('test_db', 'test_collection')
#     # interface = dbi.ConnectionCommandInterface(connection)
#
#     def setUp(self):
#         self.manager = TableCreator(MockInterface())
#
#     def tearDown(self):
#         del self.manager
#
#     @unittest.skip('soon')
#     def test_save_current_will_not_add_empty(self):
#         self.reset_interface()
#         all_info = list(self.connection.find())
#         self.assertEqual(len(all_info), 4)
#
#         self.manager.save_current()
#         self.assertEqual(len(list(self.connection.find())), 4)
#
#     @unittest.skip('soon')
#     def test_save_current_will_not_add_duplicate(self):
#         self.reset_interface()
#
#         self.manager.add_die(dt.Die(3), 1)
#         self.manager.save_current()
#         self.assertEqual(len(list(self.connection.find())), 4)
#
#     @unittest.skip('soon')
#     def test_save_current_will_add_new(self):
#         self.reset_interface()
#
#         self.manager.add_die(dt.Die(5), 1)
#         self.manager.save_current()
#         self.assertEqual(len(list(self.connection.find())), 5)
#
if __name__ == "__main__":
    unittest.main()
