import unittest

import tests.connections.test_baseconnection as tbc
import dicetables_db.connections.mongodb_connection as mg


class TestNew(tbc.TestBaseConnection):
    connection_class = mg.MongoDBConnection

    def new_connection(self, collection_name):
        connection_class = self.connection_class
        return connection_class('test_db', collection_name)

    """
    from "http://api.mongodb.com/python/current/tutorial.html"
    'An important note about collections (and databases) in MongoDB is that they are created lazily - none of the above
    commands have actually performed any operations on the MongoDB server. Collections and databases are created when
    the first document is inserted into them.'

    hence, an empty collection will not show up in get_info()
    """
    def test_1_get_info(self):
        expected = {
            'db': 'test_db',
            'collections': [],
            'current_collection': 'test',
            'indices': [],
            'ip': 'localhost',
            'port': '27017'
        }
        self.assertEqual(self.connection.get_info(), expected)

    def test_2_get_info_new_connection(self):
        expected = {
            'db': 'test_db',
            'collections': ['bob'],
            'current_collection': 'bob',
            'indices': [('foo',), ('foo', 'bar')],
            'ip': 'localhost',
            'port': '27017'
        }
        new_connection = self.new_connection('bob')
        new_connection.create_index(('foo',))
        new_connection.create_index(('foo', 'bar'))
        self.assertEqual(new_connection.get_info(), expected)

    def test_9_reset_collection_puts_collection_back_into_db_after_removal(self):
        self.connection.drop_collection()
        self.connection.reset_collection()
        self.assertEqual(self.connection.get_info()['collections'], [])

    def test_5_reset_collection_still_in_db(self):
        self.connection.reset_collection()
        self.assertEqual([], self.connection.get_info()['collections'])


class AdditionalMongoDBTests(unittest.TestCase):
    def test_projection_id_pos_neg(self):
        pass

if __name__ == '__main__':
    unittest.main()
