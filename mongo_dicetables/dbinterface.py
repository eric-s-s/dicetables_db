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
        ex: {'score': {'$lte': 10}, 'group': 'Die(1)', 'Die(1)': {'$lte': 3}}, {'_id': 1, 'score': 1} < won't show other

        :return: iterable of results
        """
        return self._collection.find(params_dict, restrictions)

    def find_one(self, params_dict=None, restrictions=None):
        return self._collection.find_one(params_dict, restrictions)

    def insert(self, document):
        """
        ex: {'score': 5, 'serialized': somebytes}


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

    def find_nearest_table(self, dice_dict):
        acceptable_score_ratio = 0.80
        finder = prep.RetrieveDiceTable(dice_dict)

        dice_dict_score = finder.get_score()

        id_number = None
        highest_score = 0
        dice_score_ratio = 0
        for search_param in finder.search_params:
            if dice_score_ratio > acceptable_score_ratio:
                break
            for group, dice_dict in search_param:
                query_dict = self._get_query_dict(dice_dict, group, dice_dict_score)
                dict_list = list(self._conn.find(query_dict, {'_id': 1, 'score': 1}))
                if dict_list:

                    candidate = max(dict_list, key=lambda dictionary: dictionary['score'])
                    new_score = candidate['score']
                    new_id = candidate['_id']

                    if new_score > highest_score:
                        id_number = new_id
                        highest_score = new_score
                        dice_score_ratio = float(new_score) / float(dice_dict_score)
        if id_number is None:
            return None
        return get_id_string(id_number)

    @staticmethod
    def _get_query_dict(dice_dict, group, score):
        output_dict = {'group': group, 'score': {'$lte': score}}
        for die_repr, num in dice_dict.items():
            output_dict[die_repr] = {'$lte': num}
        return output_dict

    def get_table(self, id_str):
        obj_id = get_id_object(id_str)
        data = self._conn.find_one({'_id': obj_id}, {'_id': 0, 'serialized': 1})
        return prep.Serializer.deserialize(data['serialized'])

