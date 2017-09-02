from typing import Tuple

from dicetables import DiceTable, DiceRecord, Modifier, ModDie, ModWeightedDie, Die, WeightedDie


class TableGenerator(object):
    def __init__(self, dice_table: DiceTable) -> None:
        self.table = dice_table


def extract_modifiers(dice_record: DiceRecord) -> Tuple[int, DiceRecord]:
    new_record = dice_record
    modifier = 0
    for die, num in dice_record.get_dict().items():
        if isinstance(die, (Modifier, ModDie, ModWeightedDie)):
            new_record = new_record.remove_die(die, num)
            modifier += die.get_modifier() * num

        if isinstance(die, ModDie):
            new_die = Die(die.get_size())
            new_record = new_record.add_die(new_die, num)

        if isinstance(die, ModWeightedDie):
            new_die = WeightedDie(die.get_raw_dict())
            new_record = new_record.add_die(new_die, num)

    return modifier, new_record


def is_new_table(table: DiceTable) -> bool:
    return table == DiceTable.new()
