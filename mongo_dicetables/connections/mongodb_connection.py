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
        return not self._collection.count()

    def get_info(self):
        indices = self._get_indices()
        info = {
            'db': self._db.name,
            'collections': self._db.collection_names(),
            'current_collection': self._collection.name,
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

    def close(self):
        if self._client:
            self._client.close()
        self._client = None
        self._collection = None
        self._db = None

    def find(self, params_dict=None, projection=None):
        """

        :return: iterable of results
        """
        self._raise_error_for_bad_projection(projection)
        new_projection = self._make_consistent_projection_api(projection)
        result = self._collection.find(params_dict, new_projection)
        return result

    def find_one(self, params_dict=None, projection=None):
        self._raise_error_for_bad_projection(projection)
        new_projection = self._make_consistent_projection_api(projection)
        result = self._collection.find_one(params_dict, new_projection)
        return result

    @staticmethod
    def _raise_error_for_bad_projection(projection):
        if projection:
            bool_values = [bool(value) for value in projection.values()]
            if True in bool_values and False in bool_values:
                raise ValueError('Projection cannot have a mix of inclusion and exclusion.')

    @staticmethod
    def _make_consistent_projection_api(projection):
        if not projection:
            return None
        new_projection = projection.copy()
        if 1 in new_projection.values():
            if '_id' not in new_projection.keys():
                new_projection['_id'] = 0
        return new_projection

    def insert(self, document):
        """

        :return: ObjectId
        """
        to_insert = document.copy()
        obj_id = self._collection.insert_one(to_insert).inserted_id
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
