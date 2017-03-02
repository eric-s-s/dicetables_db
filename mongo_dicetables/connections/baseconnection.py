from bson.objectid import ObjectId

class BaseConnection(object):

    def get_info(self):
        """

        :return: {'db': name_or_file_path,
                  'collections': [all collection/table names],
                  'current collection': name of collection in use,
                  'indices': [('col', 'col'), ('col',) ...]
        """
        raise NotImplementedError

    def is_collection_empty(self):
        raise NotImplementedError

    def find(self, params_dict=None, restrictions=None):
        """

        :return: iterable of document dictionaries
        """
        raise NotImplementedError

    def find_one(self, params_dict=None, restrictions=None):
        """

        :return: document dictionary
        """
        raise NotImplementedError

    def insert(self, document):
        """

        :return: id_object
        """
        raise NotImplementedError

    def reset_collection(self):
        raise NotImplementedError

    def drop_collection(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    @staticmethod
    def get_id_string(id_obj):
        return str(id_obj)

    @staticmethod
    def get_id_object(id_string):
        return ObjectId(id_string)

    def create_index(self, columns_tuple):
        raise NotImplementedError

    def has_index(self, columns_tuple):
        raise NotImplementedError
    """
    SELECT * FROM sqlite_master WHERE type = 'index';
    """
