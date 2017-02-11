from bson.objectid import ObjectId


from connections.baseconnection import BaseConnection
from pymongo import MongoClient, ASCENDING


class Connection(BaseConnection):
    def __init__(self, db_name, collection_name, ip='localhost', port=27017):
        self._client = MongoClient(ip, port)
        self._db = self._client[db_name]
        self._collection = self._db[collection_name]
        self._params_storage = (db_name, collection_name, ip, str(port))

    def is_collection_empty(self):
        pass

    def get_info(self):
        info = {
            'db': self._params_storage[0],
            'collections': self._db.collection_names(),
            'current_collection': self._params_storage[1],
            'indices': [] #self._collection.index_information()
        }
        return info

    @property
    def connection_info(self):
        return self._params_storage

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

    def drop_collection(self):
        self.reset_collection()

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

    def create_index(self, column_tuple):
        params = [(column_name, ASCENDING) for column_name in column_tuple]
        self._collection.create_index(params)

    def has_index(self, columns_tuple):
        to_join = []
        for column_name in columns_tuple:
            to_join.append(column_name + '_1')
        index_string = '_'.join(to_join)

        answer = self.collection_info()
        return index_string in answer.keys()

    @staticmethod
    def get_id_string(id_object):
        return str(id_object)

    @staticmethod
    def get_id_object(id_string):
        return ObjectId(id_string)