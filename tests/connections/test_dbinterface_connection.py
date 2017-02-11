import unittest

import tests.connections.test_baseconnection as tbc
import mongo_dicetables.connections.mongodb_connection as mg


class TestNew(tbc.TestBaseConnection):
    connection = mg.Connection('test_db', 'test')

    def new_connection(self, collection_name):
        connection_class = self.connection.__class__
        return connection_class('test_db', collection_name)



# import unittest
# from bson.objectid import ObjectId
# from pymongo.cursor import Cursor
#
# import mongo_dicetables.dbinterface as dbi
#
#
# """
# HEY DUMMY!  DID YOU FORGET TO RUN "mongod" IN BASH? DON'T FORGET!
# """
#
#
# class TestConnection(unittest.TestCase):
#     connection = dbi.Connection('test_db', 'test_collection')
#
#     def setUp(self):
#         self.connection.reset_database()
#
#     def tearDown(self):
#         pass
#
#     def test_connection_info(self):
#         self.assertEqual(self.connection.connection_info, ('test_db', 'test_collection', 'localhost', '27017'))
#
#     def test_collection_info_empty(self):
#         self.assertEqual(self.connection.collection_info(), {})
#
#     def test_collection_info_non_empty(self):
#         self.connection.insert({'a': 1})
#         self.assertEqual(list(self.connection.collection_info().keys()), ['_id_'])
#
#     def test_collection_info_AND_create_index_on_collection(self):
#         self.connection.create_index_on_collection([('hi', dbi.ASCENDING), ('there', dbi.ASCENDING)])
#         self.connection.insert({'a': 1})
#         index_names = sorted(self.connection.collection_info().keys())
#         self.assertEqual(index_names, ['_id_', 'hi_1_there_1'])
#
#     def test_client_info_empty_db_and_collection(self):
#         self.assertNotIn('test_db', self.connection.client_info())
#
#     def test_client_info_non_empty_db(self):
#         self.connection.insert({'a': 1})
#         self.assertIn('test_db', self.connection.client_info())
#
#     def test_db_info_empty_db(self):
#         self.assertEqual([], self.connection.db_info())
#
#     def test_db_info_non_empty_db(self):
#         other_conn = dbi.Connection('test_db', 'other_collection')
#         other_conn.insert({'b': 2})
#         self.connection.insert({'a': 1})
#         self.assertEqual(['other_collection', 'test_collection'], self.connection.db_info())
#
#     def test_reset_db(self):
#         other_conn = dbi.Connection('test_db', 'other_collection')
#         other_conn.insert({'b': 2})
#         self.connection.insert({'a': 1})
#         self.connection.reset_database()
#         self.assertEqual([], self.connection.db_info())
#
#     def test_reset_collection(self):
#         other_conn = dbi.Connection('test_db', 'other_collection')
#         other_conn.insert({'b': 2})
#         self.connection.insert({'a': 1})
#         self.connection.reset_collection()
#         self.assertEqual(['other_collection'], self.connection.db_info())
#
#     def test_insert_returns_ObjectId(self):
#         obj_id = self.connection.insert({'a': 1})
#         self.assertIsInstance(obj_id, ObjectId)
#
#     def test_find_no_params_empty(self):
#         self.assertEqual(list(self.connection.find()), [])
#
#     def test_find_no_params_and_insert_works_as_expected(self):
#         obj_id = self.connection.insert({'a': 1})
#         cursor = self.connection.find()
#         self.assertIsInstance(cursor, Cursor)
#         self.assertEqual(list(cursor), [{'a': 1, '_id': obj_id}])
#
#     def test_find_with_params_empty_result(self):
#         self.connection.insert({'a': 1})
#         cursor = self.connection.find({'a': 2})
#         self.assertEqual(list(cursor), [])
#         cursor = self.connection.find({'b': 2})
#         self.assertEqual(list(cursor), [])
#
#     def test_find_with_params_and_non_empty_result(self):
#         obj_id = self.connection.insert({'a': 1})
#         self.connection.insert({'a': 3})
#         cursor = self.connection.find({'a': {'$lte': 2}})
#         self.assertEqual(list(cursor), [{'a': 1, '_id': obj_id}])
#
#     def test_find_with_restrictions(self):
#         self.connection.insert({'a': 1})
#         self.connection.insert({'a': 3, 'b': 'will not appear'})
#         cursor = self.connection.find(None, {'a': 1, '_id': 0})
#         self.assertEqual(list(cursor), [{'a': 1}, {'a': 3}])
#
#     def test_find_one_no_params_empty(self):
#         self.assertIsNone(self.connection.find_one())
#
#     def test_find_one_no_params(self):
#         obj_id = self.connection.insert({'a': 1})
#         answer = self.connection.find_one()
#         self.assertEqual(answer, {'a': 1, '_id': obj_id})
#
#     def test_find_one_with_params_empty_result(self):
#         self.connection.insert({'a': 1})
#         answer = self.connection.find_one({'a': 2})
#         self.assertIsNone(answer)
#         answer = self.connection.find_one({'b': 2})
#         self.assertIsNone(answer)
#
#     def test_find_one_with_params_and_non_empty_result(self):
#         obj_id = self.connection.insert({'a': 1})
#         self.connection.insert({'a': 3})
#         answer = self.connection.find_one({'a': {'$lte': 2}})
#         self.assertEqual(answer, {'a': 1, '_id': obj_id})
#
#     def test_find_one_with_restrictions(self):
#         self.connection.insert({'a': 1})
#         self.connection.insert({'a': 3, 'b': 'will not appear'})
#         answer = self.connection.find_one(None, {'a': 1, '_id': 0})
#         self.assertEqual(answer, {'a': 1})
#
#     def test_get_id_string(self):
#         obj_id = self.connection.insert({'a': 1})
#         obj_id_str = dbi.get_id_string(obj_id)
#         self.assertEqual(len(obj_id_str), 24)
#
#     def test_get_id_object(self):
#         obj_id = self.connection.insert({'a': 1})
#         obj_id_str = dbi.get_id_string(obj_id)
#         new_obj_id = dbi.get_id_object(obj_id_str)
#         self.assertNotEqual(obj_id, obj_id_str)
#         self.assertEqual(obj_id, new_obj_id)
