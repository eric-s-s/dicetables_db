from pymongo import MongoClient, ASCENDING
from bson.objectid import ObjectId
import mongo_dicetables.dbprep as prep


class Connection(object):
    def __init__(self, db_name, collection_name, ip='localhost', port=27017):
        self._client = MongoClient(ip, port)
        self._db = self._client[db_name]
        self._collection = self._db[collection_name]

    def collection_info(self):
        if not self.db_info():
            return {}
        return self._collection.index_information()

    def db_info(self):
        return self._db.collection_names()

    def client_info(self):
        return self._client.database_names()

    def reset_collection(self):
        self._db.drop_collection(self._collection.name)

    def reset_database(self):
        self._client.drop_database(self._db.name)

    def find(self, params_dict=None, restrictions=None):
        """

        :return: iterable of results
        """
        return self._collection.find(params_dict, restrictions)

    def find_one(self, params_dict=None, restrictions=None):
        return self._collection.find_one(params_dict, restrictions)

    def insert(self, document):
        """

        :return: ObjectId
        """
        obj_id = self._collection.insert_one(document).inserted_id
        return obj_id

    def create_index_on_collection(self, name_order_pairs):
        self._collection.create_index(name_order_pairs)


def get_id_string(id_object):
    return str(id_object)


def get_id_object(id_string):
    return ObjectId(id_string)


class ConnectionCommandInterface(object):
    def __init__(self, connection):
        self._conn = connection
        if not self.has_required_index():
            self._create_required_index()

    def has_required_index(self):
        answer = self._conn.collection_info()
        return 'group_1_score_1' in answer.keys()

    def _create_required_index(self):
        self._conn.create_index_on_collection([('group', ASCENDING), ('score', ASCENDING)])

    def reset(self):
        self._conn.reset_collection()
        self._create_required_index()

    def add_table(self, table):
        adder = prep.PrepDiceTable(table)
        obj_id = self._conn.insert(adder.get_dict())
        return get_id_string(obj_id)

    def find_nearest_table(self, dice_list):
        finder = Finder(self._conn, dice_list)
        obj_id = finder.get_exact_match()
        if not obj_id:
            obj_id = finder.find_nearest_table()

        if not obj_id:
            return None
        return get_id_string(obj_id)

    def get_table(self, id_str):
        obj_id = get_id_object(id_str)
        data = self._conn.find_one({'_id': obj_id}, {'_id': 0, 'serialized': 1})
        return prep.Serializer.deserialize(data['serialized'])


class Finder(object):
    """
    all searches return ObjectId
    """
    def __init__(self, connection, dice_list):
        self._conn = connection
        self._param_maker = prep.SearchParams(dice_list)
        self._param_score = self._param_maker.get_score()

        self._obj_id = None
        self._highest_found_score = 0

    def get_exact_match(self):
        query_dict = self._get_query_dict_for_exact()
        obj_id_in_dict = self._conn.find_one(query_dict, {'_id': 1})
        if obj_id_in_dict:
            return obj_id_in_dict['_id']
        return None

    def _get_query_dict_for_exact(self):
        group, dice_dict = next(self._param_maker.get_search_params())[0]
        dice_dict['group'] = group
        dice_dict['score'] = self._param_score
        return dice_dict

    def find_nearest_table(self):
        for search_param in self._param_maker.get_search_params():
            if self._is_close_enough():
                break
            candidates = self._get_list_of_candidates(search_param)
            if candidates:
                biggest_score = max(candidates, key=lambda dictionary: dictionary['score'])
                self._update_id_and_highest_score(biggest_score)

        if self._obj_id is None:
            return None
        return self._obj_id

    def _is_close_enough(self):
        close_enough = 0.8
        return (self._highest_found_score / float(self._param_score)) >= close_enough

    def _get_list_of_candidates(self, group_dice_dict_list):
        out = []
        for group, dice_dict in group_dice_dict_list:
            query_dict = self._get_query_dict_for_nearest(group, dice_dict)
            cursor = self._conn.find(query_dict, {'_id': 1, 'score': 1})
            out += list(cursor)
        return out

    def _get_query_dict_for_nearest(self, group, dice_dict):
        output_dict = {'group': group, 'score': {'$lte': self._param_score}}
        for die_repr, num in dice_dict.items():
            output_dict[die_repr] = {'$lte': num}
        return output_dict

    def _update_id_and_highest_score(self, score_id_dict):
        if score_id_dict['score'] > self._highest_found_score:
            self._obj_id = score_id_dict['_id']
            self._highest_found_score = score_id_dict['score']

