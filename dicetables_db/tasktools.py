from typing import Tuple, List

from dicetables import DiceTable, DiceRecord, Modifier, ModDie, ModWeightedDie, Die, WeightedDie
from dicetables.eventsbases.protodie import ProtoDie


class TableGenerator(object):
    def __init__(self, target_record: DiceRecord) -> None:
        self._target = target_record

    def create_save_list(self, initial_table: DiceTable, step_size: int) -> List[DiceTable]:
        saves = []
        newest_table = initial_table
        ordered_list = sorted(self._target.get_dict().items())
        for die, num in ordered_list:
            die_step = get_die_step(die, step_size)
            current_die_number = newest_table.number_of_dice(die)
            target_die_number = self._target.get_number(die)

            add_times = (target_die_number - current_die_number) // die_step
            for _ in range(add_times):
                newest_table = newest_table.add_die(die, die_step)
                saves.append(newest_table)

        return saves

    def create_target_table(self, initial_table: DiceTable) -> DiceTable:
        new_table = initial_table
        for die, number in self._target.get_dict().items():
            to_add = number - new_table.number_of_dice(die)
            new_table = new_table.add_die(die, to_add)
        return new_table


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


def apply_modifier(initial_table: DiceTable, modifier: int) -> DiceTable:
    return initial_table.add_die(Modifier(modifier))


def is_new_table(table: DiceTable) -> bool:
    return table == DiceTable.new()


def get_die_step(die: ProtoDie, step_size: int) -> int:
    dict_len = len(die.get_dict())
    return max(1, step_size // dict_len)
