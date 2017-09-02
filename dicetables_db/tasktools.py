from typing import Tuple

from dicetables import DiceTable, DiceRecord, Modifier, ModDie, ModWeightedDie, Die, WeightedDie


class TableGenerator(object):
    def __init__(self, dice_table: DiceTable) -> None:
        self.table = dice_table


def extract_modifiers(dice_list: list) -> Tuple[int, list]:
    record = DiceRecord(dict(dice_list))
    modifier = 0
    for die, num in dice_list:
        if isinstance(die, (Modifier, ModDie, ModWeightedDie)):
            record = record.remove_die(die, num)
            modifier += die.get_modifier() * num

        if isinstance(die, ModDie):
            new_die = Die(die.get_size())
            record = record.add_die(new_die, num)

        if isinstance(die, ModWeightedDie):
            new_die = WeightedDie(die.get_raw_dict())
            record = record.add_die(new_die, num)

    return modifier, sorted(record.get_dict().items())


def is_new_table(table: DiceTable) -> bool:
    return table == DiceTable.new()
