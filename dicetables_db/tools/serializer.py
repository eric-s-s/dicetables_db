import pickle


class Serializer(object):
    @staticmethod
    def serialize(thing) -> bytes:
        return pickle.dumps(thing)

    @staticmethod
    def deserialize(data: bytes):
        return pickle.loads(data)
