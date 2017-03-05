from bson.objectid import ObjectId


class DocumentId(object):
    def __init__(self, obj_id):
        self._id = obj_id

    @classmethod
    def new(cls):
        return cls(ObjectId())

    @classmethod
    def from_string(cls, input_string):
        obj_id = ObjectId(input_string)
        return cls(obj_id)

    @classmethod
    def from_bson_id(cls, bson_id):
        return cls(bson_id)

    def to_bson_id(self):
        return self._id

    def to_string(self):
        return str(self)

    def __str__(self):
        return str(self._id)

    def __repr__(self):
        return 'ObjectId.from_string({})'.format(self)

    def __eq__(self, other):
        if not isinstance(other, DocumentId):
            return False
        return self._id == other.to_bson_id()

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        return self._id < other.to_bson_id()

    def __le__(self, other):
        return self._id <= other.to_bson_id()

    def __gt__(self, other):
        return self._id > other.to_bson_id()

    def __ge__(self, other):
        return self._id >= other.to_bson_id()
