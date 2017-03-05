import unittest

import os

import tests.connections.test_baseconnection as tbc
from dicetables_db.connections.sql_connection import SQLConnection


class TestNew(tbc.TestBaseConnection):
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

    def test_46_data_persistence(self):

        connection_1 = self.new_persistent_connection('new_test')
        id_obj = connection_1.insert({'a': 1})
        connection_1.create_index(('a', ))
        connection_1.close()

        connection_2 = self.new_persistent_connection('new_test')
        self.assertTrue(connection_2.has_index(('a', )))
        self.assertEqual(connection_2.find_one(), {'_id': id_obj, 'a': 1})


if __name__ == '__main__':
    unittest.main()
