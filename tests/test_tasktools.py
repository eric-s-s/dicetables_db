from unittest import TestCase

from dicetables import (DiceRecord, DiceTable, 
                        Die, ModDie, WeightedDie, ModWeightedDie,
                        StrongDie, Exploding, ExplodingOn, Modifier)
from dicetables_db.tasktools import extract_modifiers, is_new_table, TableGenerator


class TestTaskTool(TestCase):

    def test_place_holder(self):
        table_generator = TableGenerator(DiceTable.new())
        self.assertIsInstance(table_generator, TableGenerator)

    def test_is_new_table(self):
        new = DiceTable.new()
        not_new = new.add_die(Die(3))

        self.assertTrue(is_new_table(new))
        self.assertFalse(is_new_table(not_new))

    def test_extract_modifiers_no_modifiers(self):
        die = Die(6)
        weighted = WeightedDie({1: 2, 3: 4})
        dice = DiceRecord({die: 3, weighted: 4, StrongDie(die, 3): 5, ExplodingOn(die, (1, )): 6,
                           Exploding(weighted): 10})
        self.assertEqual(extract_modifiers(dice), (0, dice))

    def test_extract_modifiers_cannot_extract_from_nested_dice(self):
        mod = Modifier(6)
        mod_die = ModDie(5, -1)
        mod_weighted = ModWeightedDie({1: 2, 3: 4}, 3)
        dice = DiceRecord({StrongDie(mod, 5): 10, ExplodingOn(mod_die, (1,)): 10, Exploding(mod_weighted): 10})
        self.assertEqual(extract_modifiers(dice), (0, dice))

    def test_extract_modifiers_on_Modifier(self):
        dice = DiceRecord({Modifier(5): 3})
        self.assertEqual(extract_modifiers(dice), (15, DiceRecord.new()))

        dice = DiceRecord({Modifier(-5): 3})
        self.assertEqual(extract_modifiers(dice), (-15, DiceRecord.new()))

    def test_extract_modifiers_on_ModDie(self):
        dice = DiceRecord({ModDie(6, 5): 4})
        self.assertEqual(extract_modifiers(dice), (20, DiceRecord({Die(6): 4})))

        dice = DiceRecord({ModDie(6, -5): 4})
        self.assertEqual(extract_modifiers(dice), (-20, DiceRecord({Die(6): 4})))

    def test_extract_modifiers_on_ModWeightedDie(self):
        die_dict = {roll: roll % 3 for roll in range(1, 10)}

        dice = DiceRecord({ModWeightedDie(die_dict, 6): 5})
        self.assertEqual(extract_modifiers(dice), (30, DiceRecord({WeightedDie(die_dict): 5})))

        dice = DiceRecord({ModWeightedDie(die_dict, -6): 5})
        self.assertEqual(extract_modifiers(dice), (-30, DiceRecord({WeightedDie(die_dict): 5})))

    def test_extract_modifiers_many_modifier_dice(self):
        die_dict = {roll: roll % 3 for roll in range(1, 10)}
        dice = DiceRecord({ModDie(6, -5): 4, ModWeightedDie(die_dict, 6): 5, Modifier(-5): 3})
        new_dice = DiceRecord({Die(6): 4, WeightedDie(die_dict): 5})
        self.assertEqual(extract_modifiers(dice), (-20 + 30 + -15, new_dice))

    def test_extract_modifiers_many_dice(self):
        die_dict = {roll: roll % 3 for roll in range(1, 10)}
        dice = DiceRecord({ModDie(6, -5): 4, Die(7): 3, ModWeightedDie(die_dict, 6): 5, WeightedDie({1: 2}): 10,
                           Modifier(-5): 3, StrongDie(Die(6), 2): 11})

        dice_no_mods = DiceRecord({Die(6): 4, Die(7): 3, WeightedDie(die_dict): 5,
                                   WeightedDie({1: 2}): 10, StrongDie(Die(6), 2): 11})

        self.assertEqual(extract_modifiers(dice), (-20 + 30 + -15, dice_no_mods))

    def test_extract_modifiers_combines_new_die_with_same_modless_die(self):
        die_dict = {1: 2, 3: 4}
        different_dict = {1: 3, 2: 1}
        size = 6
        different_size = 7

        die = Die(size)
        die_pos = ModDie(size, 5)
        die_neg = ModDie(size, -3)

        weighted = WeightedDie(die_dict)
        weighted_pos = ModWeightedDie(die_dict, 4)
        weighted_neg = ModWeightedDie(die_dict, -9)

        different_die = Die(different_size)
        different_weighted = WeightedDie(different_dict)

        dice = DiceRecord({die: 2, die_pos: 3, die_neg: 4,
                           weighted: 5, weighted_pos: 6, weighted_neg: 7,
                           different_die: 8, different_weighted: 9})
        new_dice = DiceRecord({die: 2 + 3 + 4, weighted: 5 + 6 + 7, different_die: 8, different_weighted: 9})

        self.assertEqual(extract_modifiers(dice), (15 + -12 + 24 + -63, new_dice))
