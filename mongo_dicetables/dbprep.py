import pickle
from itertools import combinations


class PrepDiceTable(object):
    def __init__(self, dice_table):
        input_list = dice_table.get_list()
        if not input_list:
            raise ValueError('DiceTable may not be empty.')
        self._serialized = Serializer.serialize(dice_table)
        self._score = get_score(input_list)
        self._label_list = get_label_list(input_list)

    def get_score(self):
        return self._score

    def get_serialized(self):
        return self._serialized

    def get_label_list(self):
        return self._label_list[:]

    def get_group_list(self):
        return [repr_num[0] for repr_num in self._label_list]

    def get_group(self):
        return '&'.join(self.get_group_list())

    def get_dict(self):
        output = {'group': self.get_group(), 'score': self._score, 'serialized': self._serialized}
        for die_repr, num in self._label_list:
            output[die_repr] = num
        return output


class RetrieveDiceTable(object):
    def __init__(self, dice_list):
        if not dice_list:
            raise ValueError('List may not be empty')
        self._score = get_score(dice_list)
        self._labels = get_label_list(dice_list)
        self.search_params = self._search_generator()

    def _search_generator(self):
        elements_in_group = len(self._labels)
        while elements_in_group > 0:
            out = []

            for repr_num_tuple in combinations(self._labels, elements_in_group):
                group = [repr_num[0] for repr_num in repr_num_tuple]
                group_string = '&'.join(group)
                out.append((group_string, dict(repr_num_tuple)))
            elements_in_group -= 1
            yield out

    def get_score(self):
        return self._score

    @staticmethod
    def deserialize(serialized_data):
        return Serializer.deserialize(serialized_data)


class Serializer(object):
    @staticmethod
    def serialize(thing):
        return pickle.dumps(thing)

    @staticmethod
    def deserialize(data):
        return pickle.loads(data)


def get_score(dice_list):
    score = 0
    for die, num in dice_list:
        size = die.get_size()
        if die.get_weight() > size:
            size += 1
        score += size * num
    return score


def get_label_list(dice_list):
    return [(repr(die), num) for die, num in dice_list]
