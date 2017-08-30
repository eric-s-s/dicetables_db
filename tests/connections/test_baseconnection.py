import unittest
from operator import lt, le, gt, ge, ne


from dicetables_db.connections.baseconnection import BaseConnection
from dicetables_db.tools.serializer import Serializer
from dicetables_db.tools.documentid import DocumentId

MOCK_DATABASE = {}


class MockConnection(BaseConnection):
    def __init__(self, collection_name):
        self.collection_name = collection_name
        self._insert_collection_into_db()

    def _insert_collection_into_db(self):
        global MOCK_DATABASE
        if self.collection_name not in MOCK_DATABASE:
            MOCK_DATABASE[self.collection_name] = {'docs': [], 'indices': []}

    def get_info(self):
        info = {
            'db': 'test_db',
            'collections': sorted(MOCK_DATABASE.keys()),
            'current_collection': self.collection_name,
            'indices': sorted(self._indices_pointer())
        }
        return info

    def _documents_pointer(self):
        return self._collection_pointer()['docs']

    def _indices_pointer(self):
        return self._collection_pointer()['indices']

    def _collection_pointer(self):
        if self.collection_name is None:
            return None
        global MOCK_DATABASE
        return MOCK_DATABASE.get(self.collection_name, {'docs': [], 'indices': []})

    def is_collection_empty(self):
        return not self._documents_pointer()

    def reset_collection(self):
        if self.collection_name is not None:
            global MOCK_DATABASE
            MOCK_DATABASE[self.collection_name] = {'docs': [], 'indices': []}

    def drop_collection(self):
        global MOCK_DATABASE
        del MOCK_DATABASE[self.collection_name]

    def close(self):
        self.collection_name = None

    def find(self, params_dict=None, projection=None):
        out = []
        raise_error_for_bad_projection(projection)
        for document in self._documents_pointer():
            if fits_search(document, params_dict):
                new = get_new_document(document, projection)
                out.append(new)
        return out

    def find_one(self, params_dict=None, projection=None):
        raise_error_for_bad_projection(projection)
        for document in self._documents_pointer():
            if fits_search(document, params_dict):
                new = get_new_document(document, projection)
                return new
        return None

    def insert(self, document):
        new_id = self.id_class().new()
        to_insert = document.copy()
        to_insert['_id'] = new_id
        self._documents_pointer().append(to_insert)
        return new_id

    def create_index(self, columns_tuple):
        self._indices_pointer().append(columns_tuple)

    def has_index(self, columns_tuple):
        return columns_tuple in self._indices_pointer()


def raise_error_for_bad_projection(projection):
    if projection:
        bool_values = [bool(value) for value in projection.values()]
        if True in bool_values and False in bool_values:
            raise ValueError('Projection cannot have a mix of inclusion and exclusion.')


def get_new_document(document, projection):
    if projection:
        if 1 in projection.values():
            new = get_included(document, projection)
        else:
            new = remove_excluded(document, projection)
    else:
        new = document.copy()
    return new


def get_included(document, projection):
    new = {}
    for key, value in projection.items():
        if key in document and value:
            new[key] = document[key]
    return new


def remove_excluded(document, projection):
    new = document.copy()
    for key, value in projection.items():
        if not value:
            del new[key]
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
    inequality_str, limiter = list(inequality_dict.items())[0]
    inequalities = {
        '$lt': lt,
        '$lte': le,
        '$gt': gt,
        '$gte': ge,
        '$ne': ne
    }
    operator = inequalities[inequality_str]
    return operator(value, limiter)


class TestBaseConnection(unittest.TestCase):
    connection_class = MockConnection

    def populate_db(self):
        out = []
        self.connection.reset_collection()
        for document in self.generate_document_list():
            doc_id = self.connection.insert(document)
            document['_id'] = doc_id
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
        connection_class = self.connection_class
        return connection_class(*params)

    def empty_database(self):
        collections = self.connection.get_info()['collections']
        for collection_name in collections:
            to_drop = self.new_connection(collection_name)
            to_drop.drop_collection()

    def setUp(self):
        self.connection = self.new_connection('test')
        self.empty_database()
        self.connection.reset_collection()

    def tearDown(self):
        self.connection.close()

    def test_1_get_info(self):
        expected = {
            'db': 'test_db',
            'collections': ['test'],
            'current_collection': 'test',
            'indices': []
        }
        self.assertEqual(self.connection.get_info(), expected)

    def test_2_get_info_new_connection(self):
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

    def test_3_is_collection_empty_true(self):
        self.assertTrue(self.connection.is_collection_empty())

    def test_4_is_collection_empty_false(self):
        self.populate_db()
        self.assertFalse(self.connection.is_collection_empty())

    def test_5_reset_collection_still_in_db(self):
        self.connection.reset_collection()
        self.assertEqual(['test'], self.connection.get_info()['collections'])

    def test_6_reset_collection_removes_contents_and_indices(self):
        self.populate_db()
        self.connection.create_index(('foo', ))

        self.assertFalse(self.connection.is_collection_empty())
        self.assertNotEqual(self.connection.get_info()['indices'], [])

        self.connection.reset_collection()

        self.assertTrue(self.connection.is_collection_empty())
        self.assertEqual(self.connection.get_info()['indices'], [])

    def test_7_reset_collection_multiple_collections_only_empties_requested(self):
        document = {'a': 1}
        new_connection = self.new_connection('bob')
        self.connection.insert(document)
        new_connection.insert(document)
        self.assertFalse(self.connection.is_collection_empty())
        self.assertFalse(new_connection.is_collection_empty())

        self.connection.reset_collection()

        self.assertTrue(self.connection.is_collection_empty())
        self.assertFalse(new_connection.is_collection_empty())

    def test_8_drop_collection_resets_and_removes_from_db(self):
        self.populate_db()
        self.connection.create_index(('foo',))

        self.connection.drop_collection()
        self.assertTrue(self.connection.is_collection_empty())
        self.assertEqual(self.connection.get_info()['collections'], [])
        self.assertEqual(self.connection.get_info()['indices'], [])

    def test_9_reset_collection_puts_collection_back_into_db_after_removal(self):
        self.connection.drop_collection()
        self.connection.reset_collection()
        self.assertEqual(self.connection.get_info()['collections'], ['test'])

    def test_10_close_connection_cannot_connect_anymore(self):
        self.connection.close()
        self.assertRaises(Exception, self.connection.find)
        self.assertRaises(Exception, self.connection.find_one)
        self.assertRaises(Exception, self.connection.insert, {'a': 1})
        self.assertRaises(Exception, self.connection.drop_collection)

    def test_11_id_class__class_method(self):
        self.assertEqual(self.connection.id_class(), DocumentId)

    def test_12_insert_returns_document_id(self):
        doc_id = self.connection.insert({'a': 1})
        self.assertEqual(type(doc_id), self.connection.id_class())

    def test_13_insert(self):
        original = {'a': 1, 'b': 2}
        doc_id = self.connection.insert(original)
        original['_id'] = doc_id
        original_plus_id = self.connection.find_one()
        self.assertEqual(original, original_plus_id)
        self.assertIsNot(original, original_plus_id)

    def test_14_insert_does_not_mutate_original(self):
        original = {'a': 1, 'b': 2}
        self.connection.insert(original)
        original_plus_id = self.connection.find_one()
        self.assertNotEqual(original, original_plus_id)

    def test_15_insert_creates_unique_ids(self):
        id_list = []
        for _ in range(10):
            doc_id = self.connection.insert({'a': 1})
            self.assertNotIn(doc_id, id_list)
            id_list.append(doc_id)

    def test_16_insert_serialized_data(self):
        original = 'a'
        serialized = Serializer.serialize(original)
        self.assertNotEqual(original, serialized)

        doc_id = self.connection.insert({'a': serialized})
        document = self.connection.find_one({'_id': doc_id})

        self.assertEqual(Serializer.deserialize(document['a']), 'a')

    def test_17_insert_mutating_original_is_safe(self):
        document = {'a': 1, 'b': 2}
        doc_id = self.connection.insert(document)
        document['a'] = 100

        expected = {'a': 1, 'b': 2, '_id': doc_id}

        self.assertEqual(expected, self.connection.find_one())

    def test_18_find_one_no_params_empty_collection(self):
        self.assertIsNone(self.connection.find_one())

    def test_19_find_one_has_params_empty_collection(self):
        self.assertIsNone(self.connection.find_one({'a': 1}))

    def test_20_find_one_has_projection_empty_collection(self):
        self.assertIsNone(self.connection.find_one(projection={'a': 1}))

    def test_21_find_one_no_params_non_empty_collection(self):
        document_list = self.populate_db()
        result = self.connection.find_one()

        self.assertIn(result, document_list)

    def test_22_find_one_has_params_non_empty_collection(self):
        target_id = self.connection.insert({'a': 0, 'b': 0, 'c': 0})
        expected = {'_id': target_id, 'a': 0, 'b': 0, 'c': 0}

        self.connection.insert({'a': 0, 'b': 0, 'c': 0})
        self.connection.insert({'a': 1, 'b': 0, 'c': 0})

        self.assertEqual(self.connection.find_one({'_id': target_id}), expected)
        self.assertEqual(self.connection.find_one({'_id': target_id, 'a': 0, 'b': 0}), expected)

    def test_23_find_one_has_params_many_possible_answers(self):
        all_docs = self.populate_db()
        expected_list = []
        for doc in all_docs:
            if (doc['a'], doc['b'], doc['c']) == (1, 1, 1):
                expected_list.append(doc)

        self.assertIn(self.connection.find_one({'a': 1}), expected_list)
        self.assertIn(self.connection.find_one({'a': 1, 'b': 1}), expected_list)

    def test_24_find_one_no_match_by_value(self):
        self.populate_db()
        self.assertIsNone(self.connection.find_one({'a': 1, 'b': 2}))

    def test_25_find_one_no_match_by_key(self):
        self.populate_db()
        self.assertIsNone(self.connection.find_one({'a': 1, 'd': 1}))

    def test_26_find_one_inclusion_out_of_range(self):
        self.populate_db()
        self.assertEqual({'a': 0}, self.connection.find_one({'b': 0}, {'a': 1, 'x': 1}))

    def test_27_find_one_projection(self):
        doc_id = self.connection.insert({'a': 0, 'b': 0, 'c': 0})
        self.assertEqual(self.connection.find_one({'_id': doc_id}, {'_id': 1, 'b': 1}),
                         {'_id': doc_id, 'b': 0})

    def test_28_find_one_using_id(self):
        doc_id = self.connection.insert({'a': 1})
        result = self.connection.find_one({'_id': doc_id})
        self.assertEqual(result, {'_id': doc_id, 'a': 1})

    def test_29_find_no_params_empty_connection(self):
        results = list(self.connection.find())
        self.assertEqual([], results)

    def test_30_find_no_matches_by_value(self):
        self.populate_db()
        result = list(self.connection.find({'a': 1, 'b': 2}))
        self.assertEqual(result, [])

    def test_31_find_no_match_by_key(self):
        self.populate_db()
        self.assertEqual([], list(self.connection.find({'a': 1, 'd': 1})))

    def test_32_find_inclusion_out_of_range(self):
        self.populate_db()
        expected = [{'a': 0}, {'a': 0}, {'a': 0}, {'a': 0}]
        self.assertEqual(expected, list(self.connection.find({'b': 0}, {'a': 1, 'x': 1})))

    def test_33_find_no_params(self):
        docs = self.populate_db()
        results = list(self.connection.find())
        self.assertEqual(len(docs), len(results))
        for document in docs:
            self.assertIn(document, results)

    def test_34_find_with_params(self):
        docs = self.populate_db()
        expected = []
        for document in docs:
            if (document['a'], document['b']) == (1, 1):
                expected.append(document)

        results = list(self.connection.find({'a': 1, 'b': 1}))
        self.assertEqual(len(results), len(expected))
        for document in expected:
            self.assertIn(document, results)

    def test_35_find_with_projection_inclusion(self):
        self.populate_db()
        results = list(self.connection.find({'a': 1}, {'a': 1, 'c': 1}))
        expected = [{'a': 1, 'c': 1}] * 3
        self.assertEqual(results, expected)

    def test_36_find_with_projection_exclusion(self):
        self.populate_db()
        results = list(self.connection.find({'a': 1}, {'_id': 0, 'b': 0}))
        expected = [{'a': 1, 'c': 1}] * 3
        self.assertEqual(results, expected)

    def test_37_find_with_projection_raises_error_with_inclusion_and_exclusion(self):
        self.assertRaises(ValueError, self.connection.find, {'a': 1}, {'a': 1, 'b': 0})
        self.assertRaises(ValueError, self.connection.find_one, {'a': 1}, {'a': 1, 'b': 0})

    def test_38_projection_id_is_not_special_case(self):
        doc_id = self.connection.insert({'a': 1, 'b': 1})
        just_a = self.connection.find_one(projection={'a': 1})
        a_and_id = self.connection.find_one(projection={'a': 1, '_id': 1})
        id_and_b = self.connection.find_one(projection={'a': 0})
        self.assertEqual(just_a, {'a': 1})
        self.assertEqual(a_and_id, {'_id': doc_id, 'a': 1})
        self.assertEqual(id_and_b, {'_id': doc_id, 'b': 1})

    def test_39_find_using_id_value(self):
        doc_id = self.connection.insert({'a': 1})
        result = self.connection.find({'_id': doc_id})
        self.assertEqual(result, [{'_id': doc_id, 'a': 1}])

    def test_40_has_index_true(self):
        self.connection.create_index(('a', 'b'))
        self.assertTrue(self.connection.has_index(('a', 'b')))

    def test_41_has_index_false_wrong_index(self):
        self.connection.create_index(('a', 'b'))
        self.assertFalse(self.connection.has_index(('a',)))

    def test_42_has_index_false_no_indices(self):
        self.assertFalse(self.connection.has_index(('a', 'b')))

    def test_43_lt_syntax_with_find(self):
        self.populate_db()
        results = list(self.connection.find({'a': {'$lt': 1}}, {'a': 1, 'b': 1}))
        expected = [{'a': 0, 'b': 0}] * 4
        self.assertEqual(results, expected)

    def test_44_lte_syntax_with_find(self):
        self.populate_db()
        results = list(self.connection.find({'a': {'$lte': 1}}, {'a': 1}))

        results_zero = [element for element in results if element == {'a': 0}]
        results_one = [element for element in results if element == {'a': 1}]
        self.assertEqual(len(results_one), 3)
        self.assertEqual(len(results_zero), 4)
        self.assertEqual(len(results), 7)

    def test_45_gt_syntax_with_find(self):
        self.populate_db()
        results = list(self.connection.find({'a': {'$gt': 1}}, {'a': 1}))
        self.assertEqual(results, [{'a': 2}] * 3)

    def test_46_gte_syntax_with_find(self):
        self.populate_db()
        results = list(self.connection.find({'a': {'$gte': 2}}, {'a': 1}))
        self.assertEqual(results, [{'a': 2}] * 3)

    def test_47_ne_syntax_with_find(self):
        self.populate_db()
        results = list(self.connection.find({'a': {'$ne': 1}}, {'a': 1}))

        results_zero = [element for element in results if element == {'a': 0}]
        results_two = [element for element in results if element == {'a': 2}]
        self.assertEqual(len(results_two), 3)
        self.assertEqual(len(results_zero), 4)
        self.assertEqual(len(results), 7)

    def test_48_data_persistence(self):
        connection_1 = self.new_connection('new_test')
        doc_id = connection_1.insert({'a': 1})
        connection_1.create_index(('a', ))
        connection_1.close()

        connection_2 = self.new_connection('new_test')
        self.assertTrue(connection_2.has_index(('a', )))
        self.assertEqual(connection_2.find_one(), {'_id': doc_id, 'a': 1})


if __name__ == '__main__':
    unittest.main()
