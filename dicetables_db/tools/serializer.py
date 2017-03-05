import pickle


class Serializer(object):
    @staticmethod
    def serialize(thing):
        return pickle.dumps(thing)

    @staticmethod
    def deserialize(data):
        return pickle.loads(data)
