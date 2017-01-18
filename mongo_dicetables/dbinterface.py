from pymongo import MongoClient
from bson.objectid import ObjectId
import mongo_dicetables.dbprep as prep


class Connection(object):
    def __init__(self, db_name, collection_name, ip='localhost', port=27017):
        self._client = MongoClient(ip, port)
        self._db = self._client[db_name]
        self._collection = self._db[collection_name]

    def reset_collection(self):
        self._db.drop_collection(self._collection.name)

    def reset_database(self):
        self._client.drop_database(self._db.name)

    def find(self, params_dict, restrictions=None):
        """
        ex: {'score': {'$lte': 10}, 'group': 'Die(1)', 'Die(1)': {'$lte': 3}}, {'_id': 1, 'score': 1} < won't show other

        :return: iterable of results
        """
        return self._collection.find(params_dict, restrictions)

    def find_one(self, params_dict, restrictions=None):
        return self._collection.find_one(params_dict, restrictions)

    def insert(self, document):
        """
        ex: {'score': 5, 'serialized': somebytes}


        :return: ObjectId
        """
        return self._collection.insert_one(document).inserted_id


def get_id_string(id_object):
    return str(id_object)


def get_id_object(id_string):
    return ObjectId(id_string)


class ConnectionCommandInterface(object):
    def __init__(self, connection):
        self._conn = connection

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

"""select priority0.*, priority1.die, priority1.number
from priority0 left outer join priority1 on priority0.id = priority1.id
where priority0.die = 'Die(4)' and priority1.die is not NULL

select all the stuff
from priority0
left outer join priority1 on priority0.id = priority1.id
left outer join priority2 on priotity0.id = priority2.id

where
priority0.die = 'Die(4)' and 10 < priority0.number <100
and priority1.die

mongodb notes
import pymongo
from bson.binary import Binary

{'my_data': Binary(some bytes)}


"""
