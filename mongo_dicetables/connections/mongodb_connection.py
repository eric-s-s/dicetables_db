from bson.objectid import ObjectId


from connections.baseconnection import BaseConnection
from pymongo import MongoClient, ASCENDING


class MongoDBConnection(BaseConnection):
    def __init__(self, db_name, collection_name, ip='localhost', port=27017):
        self._client = MongoClient(ip, port)
        self._db = self._client[db_name]
        self._collection = self._db[collection_name]
        self._params_storage = (db_name, collection_name, ip, str(port))
        self._place_holder = None

    def is_collection_empty(self):
        print('check collection: ', self._collection.count())
        print('check db: ', self._db.collection_names())
        return not self._collection.count()

    def get_info(self):
        indices = self._get_indices()
        info = {
            'db': self._params_storage[0],
            'collections': self._db.collection_names(),
            'current_collection': self._params_storage[1],
            'indices': indices
        }
        return info

    def _get_indices(self):
        out = []
        if self._collection.name in self._db.collection_names():
            index_info = self._collection.index_information()
            use_keys = [key for key in self._collection.index_information() if key != '_id_']
            for key in use_keys:
                columns = [pair[0] for pair in index_info[key]['key']]
                out.append(tuple(columns))
                out.sort()
        return out

    def reset_collection(self):
        self.drop_collection()

    def drop_collection(self):
        self._db.drop_collection(self._collection.name)

    def find(self, params_dict=None, restrictions=None):
        """

        :return: iterable of results
        """
        result = self._collection.find(params_dict, restrictions)
        return result

    def find_one(self, params_dict=None, restrictions=None):
        result = self._collection.find_one(params_dict, restrictions)
        return result

    def insert(self, document):
        """

        :return: ObjectId
        """
        obj_id = self._collection.insert_one(document).inserted_id
        return obj_id

    def create_index(self, column_tuple):
        params = [(column_name, ASCENDING) for column_name in column_tuple]
        self._collection.create_index(params)

    def has_index(self, columns_tuple):
        indices = self.get_info()['indices']
        return columns_tuple in indices

    @staticmethod
    def get_id_string(id_object):
        return str(id_object)

    @staticmethod
    def get_id_object(id_string):
        return ObjectId(id_string)