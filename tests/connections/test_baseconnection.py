import unittest

from mongo_dicetables.connections.baseconnection import BaseConnection

MOCK_DATABASE = {}


class MockConnection(BaseConnection):
    def __init__(self, collection_name):
        self.collection_name = collection_name
        self._insert_collection_into_db()
        self._index = []

    def _insert_collection_into_db(self):
        global MOCK_DATABASE
        if self.collection_name not in MOCK_DATABASE:
            MOCK_DATABASE[self.collection_name] = []

    def get_info(self):
        info = {
            'db': 'test_db',
            'collections': sorted(MOCK_DATABASE.keys()),
            'current_collection': self.collection_name,
            'indices': sorted(self._index)
        }
        return info

    def is_collection_empty(self):
        return not MOCK_DATABASE.get(self.collection_name, [])

    def reset_collection(self):
        global MOCK_DATABASE
        MOCK_DATABASE[self.collection_name] = []
        self._index = []

    def drop_collection(self):
        self.reset_collection()
        global MOCK_DATABASE
        del MOCK_DATABASE[self.collection_name]

    def find(self, params_dict=None, restrictions=None):
        out = []
        collection = MOCK_DATABASE[self.collection_name]
        for document in collection:
            if fits_search(document, params_dict):
                new = get_new_document(document, restrictions)
                out.append(new)
        return out

    def find_one(self, params_dict=None, restrictions=None):
        collection = MOCK_DATABASE[self.collection_name]
        for document in collection:
            if fits_search(document, params_dict):
                new = get_new_document(document, restrictions)
                return new
        return None

    def insert(self, document):
        new_id = self._get_next_id()
        document['_id'] = new_id
        global MOCK_DATABASE
        MOCK_DATABASE[self.collection_name].append(document.copy())
        return new_id

    def _get_next_id(self):
        collection = MOCK_DATABASE[self.collection_name]
        if not collection:
            return MockObjId(0)
        highest_num = max([doc['_id'] for doc in collection]).number
        return MockObjId(highest_num + 1)

    @staticmethod
    def get_id_string(id_obj):
        return str(id_obj)

    @staticmethod
    def get_id_object(id_string):
        return MockObjId(int(id_string))

    def create_index(self, columns_tuple):
        self._index.append(columns_tuple)

    def has_index(self, columns_tuple):
        return columns_tuple in self._index


class MockObjId(object):
    def __init__(self, number):
        self.number = number

    def __str__(self):
        return str(self.number)

    def __eq__(self, other):
        return self.number == other.number

    def __ne__(self, other):
        return not self == other

    def __lt__(self, other):
        return self.number < other.number

    def __gt__(self, other):
        return self.number > other.number

    def __le__(self, other):
        return self.number <= other.number

    def __ge__(self, other):
        return self.number >= other.number


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
    if not params_dict:
        return True

    should_add_bool = True
    for key, value in params_dict.items():
        if key not in document.keys():
            return False
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
        out = []
        self.connection.reset_collection()
        for document in self.generate_document_list():
            obj_id = self.connection.insert(document)
            document['_id'] = obj_id
            out.append(document)
        return out

    @staticmethod
    def generate_document_list():
        out = []
        for _id in range(10):
            document = dict.fromkeys(['a', 'b', 'c'], _id % 3)
            out.append(document)
        return out

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
            'collections': ['bob', 'test'],
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

        self.assertTrue(self.connection.is_collection_empty())
        self.assertEqual(self.connection.get_info()['collections'], [])
        self.assertEqual(self.connection.get_info()['indices'], [])

    def test_reset_collection_puts_collection_back_into_db_after_removal(self):
        self.connection.drop_collection()
        self.connection.reset_collection()
        self.assertEqual(self.connection.get_info()['collections'], ['test'])

    def test_insert(self):
        document = {'a': 1, 'b': 2}
        self.connection.insert(document)
        returned = self.connection.find_one()
        self.assertEqual(document, returned)
        self.assertIsNot(document, returned)

    def test_insert_mutating_original_is_safe(self):
        document = {'a': 1, 'b': 2}
        obj_id = self.connection.insert(document)
        document['a'] = 100

        expected = {'a': 1, 'b': 2, '_id': obj_id}

        self.assertEqual(expected, self.connection.find_one())

    def test_find_one_no_params_empty_collection(self):
        self.assertIsNone(self.connection.find_one())

    def test_find_one_has_params_empty_collection(self):
        self.assertIsNone(self.connection.find_one({'a': 1}))

    def test_find_one_has_restrictions_empty_collection(self):
        self.assertIsNone(self.connection.find_one(restrictions={'a': 1}))

    def test_find_one_no_params_non_empty_collection(self):
        document_list = self.populate_db()
        result = self.connection.find_one()

        self.assertIn(result, document_list)

    def test_find_one_has_params_non_empty_collection(self):
        target_id = self.connection.insert({'a': 0, 'b': 0, 'c': 0})
        expected = {'_id': target_id, 'a': 0, 'b': 0, 'c': 0}

        self.connection.insert({'a': 0, 'b': 0, 'c': 0})
        self.connection.insert({'a': 1, 'b': 0, 'c': 0})

        self.assertEqual(self.connection.find_one({'_id': target_id}), expected)
        self.assertEqual(self.connection.find_one({'_id': target_id, 'a': 0, 'b': 0}), expected)

    def test_find_one_has_params_many_possible_answers(self):
        all_docs = self.populate_db()
        expected_list = []
        for doc in all_docs:
            if (doc['a'], doc['b'], doc['c']) == (1, 1, 1):
                expected_list.append(doc)

        self.assertIn(self.connection.find_one({'a': 1}), expected_list)
        self.assertIn(self.connection.find_one({'a': 1, 'b': 1}), expected_list)

    def test_find_one_no_match_by_value(self):
        self.populate_db()
        self.assertIsNone(self.connection.find_one({'a': 1, 'b': 2}))

    def test_find_one_no_match_by_key(self):
        self.populate_db()
        self.assertIsNone(self.connection.find_one({'a': 1, 'd': 1}))

    def test_find_one_restrictions(self):
        obj_id = self.connection.insert({'a': 0, 'b': 0, 'c': 0})
        self.assertEqual(self.connection.find_one({'_id': obj_id}, {'_id': 1, 'b': 1}),
                         {'_id': obj_id, 'b': 0})

    def test_find_no_params_empty_connection(self):
        results = list(self.connection.find())
        self.assertEqual([], results)

    def test_find_no_matches(self):
        self.populate_db()
        result = list(self.connection.find({'a': 1, 'b': 2}))
        self.assertEqual(result, [])

    def test_find_no_params(self):
        docs = self.populate_db()
        results = list(self.connection.find())
        self.assertEqual(len(docs), len(results))
        for document in docs:
            self.assertIn(document, results)

    def test_find_with_params(self):
        docs = self.populate_db()
        expected = []
        for document in docs:
            if (document['a'], document['b']) == (1, 1):
                expected.append(document)

        results = list(self.connection.find({'a': 1, 'b': 1}))
        self.assertEqual(len(results), len(expected))
        for document in expected:
            self.assertIn(document, results)

    def test_find_with_restrictions(self):
        self.populate_db()
        results = list(self.connection.find({'a': 1}, {'_id': 0, 'a': 1, 'c': 1}))
        expected = [{'a': 1, 'c': 1}] * 3
        self.assertEqual(results, expected)


# todo create inequality tests and get_id test.  insert tests should be explicit about returning new ID

    def test_get_id_string(self):
        pass

    def test_get_id_object(self):
        pass

    def test_has_index_true(self):
        self.connection.create_index(('a', 'b'))
        self.assertTrue(self.connection.has_index(('a', 'b')))

    def test_has_index_false_wrong_index(self):
        self.connection.create_index(('a', 'b'))
        self.assertFalse(self.connection.has_index(('a',)))

    def test_has_index_false_no_indices(self):
        self.assertFalse(self.connection.has_index(('a', 'b')))

    def test_create_index(self):
        pass


if __name__ == '__main__':
    unittest.main()
