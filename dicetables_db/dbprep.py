from itertools import combinations
from typing import List, Tuple

from dicetables_db.tools.serializer import Serializer


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

    def get_label_list(self) -> List[Tuple[str, int]]:
        return self._label_list[:]

    def get_group_list(self) -> List[str]:
        return [repr_num[0] for repr_num in self._label_list]

    def get_group(self) -> str:
        return '&'.join(self.get_group_list())

    def get_dict(self):
        output = {'group': self.get_group(), 'score': self._score, 'serialized': self._serialized}
        for die_repr, num in self._label_list:
            output[die_repr] = num
        return output


class SearchParams(object):
    def __init__(self, dice_list):
        if not dice_list:
            raise ValueError('List may not be empty')
        self._score = get_score(dice_list)
        self._labels = get_label_list(dice_list)

    def get_search_params(self):
        return self._search_generator()

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

    def get_score(self) -> int:
        return self._score


def get_score(dice_list: list) -> int:
    score = 0
    for die, num in dice_list:
        size = die.get_size()
        if die.get_weight() > size:
            size += 1
        score += size * num
    return score


def get_label_list(dice_list) -> List[Tuple[str, int]]:
    return [(repr(die), num) for die, num in dice_list]
