import unittest

import os

import tests.connections.test_baseconnection as tbc
from dicetables_db.connections.sql_connection import SQLConnection, InMemoryInformation


class TestSQLConnection(tbc.TestBaseConnection):
    connection = SQLConnection(':memory:', 'test')
    current_connections = []
    delete_test_dot_db = False

    @staticmethod
    def remove_test_db():
        if os.path.isfile('test.db'):
            os.remove('test.db')

    def new_persistent_connection(self, collection_name):
        connection_class = self.connection.__class__
        out = connection_class('test.db', collection_name)
        self.current_connections.append(out)
        self.delete_test_dot_db = True
        return out

    def new_connection(self, collection_name):
        connection_class = self.connection.__class__
        out = connection_class(':memory:', collection_name)
        self.current_connections.append(out)
        return out

    @classmethod
    def setUpClass(cls):
        cls.remove_test_db()

    @classmethod
    def tearDownClass(cls):
        cls.remove_test_db()

    def setUp(self):
        self.connection = self.new_connection('test')

    def tearDown(self):
        if self.delete_test_dot_db:
            self.empty_database()
            self.delete_test_dot_db = False
        for connection in self.current_connections:
            connection.close()
        self.current_connections = []

    def test_1_get_info(self):
        expected = {
            'db': ':memory:',
            'collections': ['test'],
            'current_collection': 'test',
            'indices': []
        }
        self.assertEqual(self.connection.get_info(), expected)

    def test_2_get_info_new_connection(self):
        expected = {
            'db': 'test.db',
            'collections': ['bob', 'test'],
            'current_collection': 'bob',
            'indices': [('foo',), ('foo', 'bar')]
        }
        self.new_persistent_connection('test')

        new_connection = self.new_persistent_connection('bob')
        new_connection.create_index(('foo',))
        new_connection.create_index(('foo', 'bar'))
        self.assertEqual(new_connection.get_info(), expected)

    def test_48_data_persistence(self):

        connection_1 = self.new_persistent_connection('new_test')
        doc_id = connection_1.insert({'a': 1})
        connection_1.create_index(('a', ))
        connection_1.close()

        connection_2 = self.new_persistent_connection('new_test')
        self.assertTrue(connection_2.has_index(('a', )))
        self.assertEqual(connection_2.find_one(), {'_id': doc_id, 'a': 1})


class AdditionalSQLTests(unittest.TestCase):
    def setUp(self):
        self.connection = SQLConnection(':memory:', 'test')
        self.in_memory = InMemoryInformation(self.connection)

    def tearDown(self):
        del self.in_memory
        self.connection.close()
        del self.connection

    def test_new_columns_default_integer(self):
        self.connection.insert({'a': 1, 'b': 1})
        doc_id = self.connection.insert({'a': 2})
        self.assertEqual(self.connection.find_one({'_id': doc_id}), {'_id': doc_id, 'a': 2, 'b': 0})

    def test_new_columns_default_text(self):
        self.connection.insert({'a': 1, 'b': 'hello'})
        doc_id = self.connection.insert({'a': 2})
        self.assertEqual(self.connection.find_one({'_id': doc_id}), {'_id': doc_id, 'a': 2, 'b': ''})

    def test_new_columns_default_blob(self):
        self.connection.insert({'a': 1, 'b': self.connection.id_class().new()})
        doc_id = self.connection.insert({'a': 2})
        self.assertEqual(self.connection.find_one({'_id': doc_id}), {'_id': doc_id, 'a': 2, 'b': None})

    def test_InMemoryInformation_refresh_columns(self):
        self.assertEqual(self.in_memory.columns, ['_id'])
        self.connection.insert({'a': 1})
        self.in_memory.refresh_columns()
        self.assertEqual(self.in_memory.columns, ['_id', 'a'])

    def test_InMemoryInformation_refresh_collections(self):
        connection_1 = SQLConnection('test.db', 'foo')
        in_memory = InMemoryInformation(connection_1)
        connection_2 = SQLConnection('test.db', 'bar')
        self.assertEqual(in_memory.collections, ['foo'])
        in_memory.refresh_collections()
        self.assertEqual(in_memory.collections, ['bar', 'foo'])

        connection_1.drop_collection()
        connection_2.drop_collection()
        connection_1.close()
        connection_2.close()

    def test_InMemoryInformation_refresh_indices(self):
        self.assertEqual(self.in_memory.indices, [])
        self.connection.create_index(('a', 'b'))
        self.in_memory.refresh_indices()
        self.assertEqual(self.in_memory.indices, [('a', 'b')])

    def test_InMemoryInformation_refresh_indices_ignores_any_autoindex(self):
        true_index_data = self.connection.cursor.execute("select * from sqlite_master where type='index';").fetchall()
        true_index_names = [index[1] for index in true_index_data if index[2] == 'test']
        self.assertEqual(true_index_names, ['sqlite_autoindex_test_1'])

    def test_InMemoryInformation_refresh_information(self):
        connection_1 = SQLConnection('test.db', 'foo')
        in_memory = InMemoryInformation(connection_1)
        self.assertEqual(in_memory.columns, ['_id'])
        self.assertEqual(in_memory.collections, ['foo'])
        self.assertEqual(in_memory.indices, [])

        connection_1.create_index(('a', 'b'))
        connection_2 = SQLConnection('test.db', 'bar')
        in_memory.refresh_information()

        self.assertEqual(in_memory.indices, [('a', 'b')])
        self.assertEqual(in_memory.collections, ['bar', 'foo'])
        self.assertEqual(in_memory.columns, ['_id', 'a', 'b'])

        connection_1.drop_collection()
        connection_2.drop_collection()
        connection_1.close()
        connection_2.close()

    def test_InMemoryInformation_add_column(self):
        self.in_memory.add_column('a')
        self.assertEqual(self.in_memory.columns, ['_id', 'a'])

    def test_InMemoryInformation_add_collection(self):
        self.in_memory.add_collection('hello')
        self.assertEqual(self.in_memory.collections, ['hello', 'test'])

    def test_InMemoryInformation_add_index(self):
        self.in_memory.add_index(('hello', ))
        self.assertEqual(self.in_memory.indices, [('hello', )])

    def test_InMemoryInformation_has_index_t_and_f(self):
        self.in_memory.add_index(('a', 'b'))
        self.assertTrue(self.in_memory.has_index(('a', 'b')))
        self.assertFalse(self.in_memory.has_index(('b', 'a')))

    def test_InMemoryInformation_has_column_t_and_f(self):
        self.assertTrue(self.in_memory.has_column('_id'))
        self.assertFalse(self.in_memory.has_column('a'))

    def test_InMemoryInformation_has_collection_t_and_f(self):
        self.assertTrue(self.in_memory.has_collection('test'))
        self.assertFalse(self.in_memory.has_collection('a'))

    def test_InMemoryDropCollection(self):
        self.in_memory.add_index(('a', ))
        self.in_memory.add_column('b')
        self.in_memory.add_collection('will_still_exist')
        self.in_memory.drop_collection()
        self.assertEqual(self.in_memory.columns, [])
        self.assertEqual(self.in_memory.indices, [])
        self.assertEqual(self.in_memory.collections, ['will_still_exist'])

if __name__ == '__main__':
    unittest.main()
