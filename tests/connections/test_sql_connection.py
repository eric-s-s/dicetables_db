import unittest

import os

import tests.connections.test_baseconnection as tbc
from mongo_dicetables.connections.sql_connection import SQLConnection


class TestNew(tbc.TestBaseConnection):
    connection = SQLConnection(':memory:', 'test')

    @staticmethod
    def remove_test_db():
        if os.path.isfile('test.db'):
            os.remove('test.db')

    def new_connection(self, collection_name, test_db=False):
        connection_class = self.connection.__class__
        if test_db:
            return connection_class('test.db', collection_name)
        else:
            return connection_class(':memory:', collection_name)

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
        self.remove_test_db()
        first_connection = self.new_connection('test', test_db=True)

        new_connection = self.new_connection('bob', test_db=True)
        new_connection.create_index(('foo',))
        new_connection.create_index(('foo', 'bar'))
        self.assertEqual(new_connection.get_info(), expected)

        first_connection.close()
        new_connection.close()
        self.remove_test_db()

    def test_34_get_id_object(self):
        id_obj = self.connection.insert({'a': 1})
        id_str = self.connection.get_id_string(id_obj)
        new_id_obj = self.connection.get_id_object(id_str)

        self.assertIsInstance(new_id_obj, id_obj.__class__)
        self.assertEqual(new_id_obj, id_obj)

    def test_43_data_persistence(self):
        connection_1 = self.new_connection('new_test', test_db=True)
        id_obj = connection_1.insert({'a': 1})
        connection_1.create_index(('a', ))
        connection_1.close()

        connection_2 = self.new_connection('new_test', test_db=True)
        self.assertTrue(connection_2.has_index(('a', )))
        self.assertEqual(connection_2.find_one(), {'_id': id_obj, 'a': 1})



if __name__ == '__main__':
    unittest.main()
