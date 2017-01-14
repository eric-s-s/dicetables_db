import pickle
from itertools import combinations
import dicetables as dt


class PrepDiceTable(object):
    def __init__(self, dice_table, group_as_list=True):
        input_list = dice_table.get_list()
        self._serialized = pickle.dumps(dice_table)
        self._score = get_score(input_list)
        self._label_list = get_label_list(input_list)
        self._key_as_list = group_as_list

    def get_score(self):
        return self._score

    def get_serialized(self):
        return self._serialized

    def get_label_list(self):
        return self._label_list[:]

    def get_group(self):
        if self._key_as_list:
            return self.get_group_as_list()
        else:
            return self.get_group_as_string()

    def get_group_as_list(self):
        return [repr_num[0] for repr_num in self._label_list]

    def get_group_as_string(self):
        return '&'.join(self.get_group_as_list())

    def get_dict(self):
        output = {'group': self.get_group(), 'score': self._score, 'serialized': self._serialized}
        for die_repr, num in self._label_list:
            output[die_repr] = num
        return output


class RetrieveDiceTable(object):
    def __init__(self, dice_list):
        self._score = get_score(dice_list)
        self._labels = get_label_list(dice_list)


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


def get_key_and_list_pairs(dice_list, depth):
    names_and_numbers = [(repr(die), num) for die, num in dice_list]
    r_value = len(dice_list) - depth
    if r_value <= 0:
        return []
    generator = combinations(names_and_numbers, r_value)
    out = []
    for dice_tuple in generator:
        key = [die_num[0] for die_num in dice_tuple]
        out.append((key, list(dice_tuple)))
    return out

