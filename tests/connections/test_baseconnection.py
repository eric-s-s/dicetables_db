import unittest

from mongo_dicetables.connections.baseconnection import BaseConnection

MOCK_DATABASE = []


class MockConnection(BaseConnection):
    def __init__(self, collection_name):
        self.collection_name = collection_name
        self._insert_collection_into_db()
        self._collection = []
        self._index = []

    def _insert_collection_into_db(self):
        global MOCK_DATABASE
        if self.collection_name not in MOCK_DATABASE:
            MOCK_DATABASE.append(self.collection_name)

    def get_info(self):
        info = {
            'db': 'test_db',
            'collections': MOCK_DATABASE[:],
            'current_collection': self.collection_name,
            'indices': self._index[:]
        }
        return info

    def is_collection_empty(self):
        return not self._collection

    def reset_collection(self):
        self._collection = []
        self._index = []
        self._insert_collection_into_db()

    def drop_collection(self):
        self.reset_collection()
        global MOCK_DATABASE
        index = MOCK_DATABASE.index(self.collection_name)
        del MOCK_DATABASE[index]

    def find(self, params_dict=None, restrictions=None):
        out = []
        for document in self._collection:
            if fits_search(document, params_dict):
                new = get_new_document(document, restrictions)
                out.append(new)
        return out

    def find_one(self, params_dict=None, restrictions=None):
        for document in self._collection:
            if fits_search(document, params_dict):
                new = get_new_document(document, restrictions)
                return new
        return None

    def insert(self, document):
        self._collection.append(document)

    @staticmethod
    def get_id_string(id_obj):
        return str(id_obj)

    @staticmethod
    def get_id_object(id_string):
        return int(id_string)

    def create_index(self, columns_tuple):
        self._index.append(columns_tuple)

    def has_index(self, columns_tuple):
        return columns_tuple in self._index


def get_new_document(document, restrictions):
    if restrictions:
        new = {}
        for key, value in restrictions.items():
            if value:
                new[key] = document[key]
    else:
        new = document.copy()
    return new


def fits_search(document, params_dict):
    should_add_bool = True
    for key, value in params_dict.items():
        if not should_add_bool:
            break

        if isinstance(value, dict):
            should_add_bool = is_inequality_true(document[key], value)
        else:
            should_add_bool = (document[key] == value)

    return should_add_bool


def is_inequality_true(value, inequality_dict):
    inequality_str, limiter = list(inequality_dict.values())[0]
    inequalities = {
        '$lt': value.__lt__,
        '$lte': value.__le__,
        '$gt': value.__gt__,
        '$gte': value.__ge__,
        '$ne': value.__ne__
    }
    operator = inequalities[inequality_str]
    return operator(limiter)


class TestBaseConnection(unittest.TestCase):
    connection = MockConnection('test')

    def populate_db(self):
        self.connection.reset_collection()
        for _id in range(10):
            document = dict.fromkeys(['a', 'b', 'c'], _id % 3)
            document['_id'] = _id
            self.connection.insert(document)

    def new_connection(self, *params):
        connection_class = self.connection.__class__
        return connection_class(*params)

    def empty_database(self):
        collections = self.connection.get_info()['collections']
        for collection_name in collections:
            to_drop = self.new_connection(collection_name)
            to_drop.drop_collection()

    def setUp(self):
        self.empty_database()
        self.connection.reset_collection()

    def test_get_info(self):
        expected = {
            'db': 'test_db',
            'collections': ['test'],
            'current_collection': 'test',
            'indices': []
        }
        self.assertEqual(self.connection.get_info(), expected)

    def test_get_info_new_connection(self):
        expected = {
            'db': 'test_db',
            'collections': ['test', 'bob'],
            'current_collection': 'bob',
            'indices': [('foo',), ('foo', 'bar')]
        }
        new_connection = self.new_connection('bob')
        new_connection.create_index(('foo',))
        new_connection.create_index(('foo', 'bar'))
        self.assertEqual(new_connection.get_info(), expected)

    def test_is_collection_empty_true(self):
        self.assertTrue(self.connection.is_collection_empty())

    def test_is_collection_empty_false(self):
        self.populate_db()
        self.assertFalse(self.connection.is_collection_empty())

    def test_reset_collection_still_in_db(self):
        self.connection.reset_collection()
        self.assertEqual(['test'], self.connection.get_info()['collections'])

    def test_reset_collection_removes_contents_and_indices(self):
        self.populate_db()
        self.connection.create_index(('foo', ))

        self.assertFalse(self.connection.is_collection_empty())
        self.assertTrue(self.connection.get_info()['indices'])

        self.connection.reset_collection()

        self.assertTrue(self.connection.is_collection_empty())
        self.assertFalse(self.connection.get_info()['indices'])

    def test_reset_collection_multiple_collections_only_empties_requested(self):
        document = {'a': 1}
        new_connection = self.new_connection('bob')
        self.connection.insert(document)
        new_connection.insert(document)
        self.assertFalse(self.connection.is_collection_empty())
        self.assertFalse(new_connection.is_collection_empty())

        self.connection.reset_collection()

        self.assertTrue(self.connection.is_collection_empty())
        self.assertFalse(new_connection.is_collection_empty())

    def test_drop_collection_resets_and_removes_from_db(self):
        self.populate_db()
        self.connection.create_index(('foo',))

        self.connection.drop_collection()
        expected_info = {
            'db': 'test_db',
            'collections': [],
            'current_collection': 'test',
            'indices': []
        }

        self.assertTrue(self.connection.is_collection_empty())
        self.assertEqual(self.connection.get_info(), expected_info)

    def test_reset_collection_puts_collection_back_into_db_after_removal(self):
        self.connection.drop_collection()
        self.connection.reset_collection()
        self.assertEqual(self.connection.get_info()['collections'], ['test'])

    def test_find_one(self):
        pass

    def test_find(self):
        pass

    def test_insert(self):
        pass

    def test_get_id_string(self):
        pass

    def test_get_id_object(self):
        pass

    def test_create_index(self):
        pass

    def test_has_index(self):
        pass

if __name__ == '__main__':
    unittest.main()
