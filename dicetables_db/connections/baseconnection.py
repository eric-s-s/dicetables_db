from dicetables_db.tools.documentid import DocumentId


class BaseConnection(object):

    @classmethod
    def id_class(cls):
        return DocumentId

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

        :return: document dictionary or None
        """
        raise NotImplementedError

    def insert(self, document):
        """

        :return: instance of self.id_class()
        """
        raise NotImplementedError

    def reset_collection(self):
        raise NotImplementedError

    def drop_collection(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def create_index(self, columns_tuple):
        raise NotImplementedError

    def has_index(self, columns_tuple):
        raise NotImplementedError
