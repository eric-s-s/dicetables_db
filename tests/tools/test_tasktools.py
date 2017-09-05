from queue import Queue
from unittest import TestCase

from dicetables import (DiceRecord, DiceTable, DiceRecordError,
                        Die, ModDie, WeightedDie, ModWeightedDie,
                        StrongDie, Exploding, ExplodingOn, Modifier)

from dicetables_db.tools.tasktools import extract_modifiers, apply_modifier, is_new_table, get_die_step, TableGenerator


class TestTaskTool(TestCase):
    def test_get_die_step_perfect_fit(self):
        self.assertEqual(get_die_step(Die(6), 24), 4)
        also_six = StrongDie(ModDie(6, -1), 5)
        self.assertEqual(get_die_step(also_six, 24), 4)

    def test_get_die_step_imperfect_fit(self):
        self.assertEqual(get_die_step(Die(7), 24), 3)
        also_seven = StrongDie(WeightedDie({roll: roll for roll in range(1, 8)}), 3)
        self.assertEqual(get_die_step(also_seven, 24), 3)

    def test_get_die_step_based_on_get_dict_size(self):
        two_get_dict = WeightedDie({1: 2, 5: 9})
        four_get_dict = ModWeightedDie({1: 2, 3: 4, 4: 5, 5: 6}, -2)
        nine_get_dict = Exploding(Die(5), explosions=1)

        self.assertEqual(two_get_dict.get_size(), four_get_dict.get_size(), nine_get_dict.get_size())

        self.assertEqual(get_die_step(two_get_dict, 4), 2)
        self.assertEqual(get_die_step(two_get_dict, 5), 2)

        self.assertEqual(get_die_step(four_get_dict, 8), 2)
        self.assertEqual(get_die_step(four_get_dict, 11), 2)

        self.assertEqual(get_die_step(nine_get_dict, 18), 2)
        self.assertEqual(get_die_step(nine_get_dict, 26), 2)

    def test_get_die_step_is_always_minimum_of_one(self):
        step_size = 4
        self.assertEqual(get_die_step(Die(2), step_size), 2)
        for die_size in range(3, 20):
            self.assertEqual(get_die_step(Die(die_size), step_size), 1)

    def test_is_new_table(self):
        new = DiceTable.new()
        not_new = new.add_die(Die(3))

        self.assertTrue(is_new_table(new))
        self.assertFalse(is_new_table(not_new))

    def test_TableGenerator_empty_record(self):
        table_generator = TableGenerator(DiceRecord.new())

        initial_table = DiceTable.new()
        q = Queue()

        self.assertEqual(table_generator.create_save_list(initial_table, 20), [])

        table_generator.create_save_list(initial_table, 20, q)
        self.assertEqual(q.get(), 'STOP')

        self.assertEqual(table_generator.create_target_table(initial_table), DiceTable.new())

    def test_TableGenerator_create_save_list_hits_target(self):
        initial = DiceTable.new()
        target = DiceRecord({Die(5): 6})
        step_size = 10
        save_list = TableGenerator(target).create_save_list(initial, step_size)
        expected = [initial.add_die(Die(5), 2), initial.add_die(Die(5), 4), initial.add_die(Die(5), 6)]
        self.assertEqual(save_list, expected)
        self.assertEqual(save_list[-1].dice_data(), target)

    def test_TableGenerator_create_save_list_does_not_hit_target(self):
        new = DiceTable.new()
        initial = new.add_die(Die(5))
        self.assertEqual(initial.number_of_dice(Die(5)), 1)
        target = DiceRecord({Die(5): 6})
        step_size = 10
        save_list = TableGenerator(target).create_save_list(initial, step_size)
        expected = [new.add_die(Die(5), 3), new.add_die(Die(5), 5)]
        self.assertEqual(save_list, expected)
        self.assertEqual(save_list[-1].dice_data(), DiceRecord({Die(5): 5}))

    def test_TableGenerator_crate_save_list_LIMITATION_initial_contains_dice_not_in_target(self):
        initial = DiceTable.new().add_die(ModDie(5, 10))
        target = DiceRecord({Die(5): 4})
        step_size = 10
        save_list = TableGenerator(target).create_save_list(initial, step_size)
        expected = [initial.add_die(Die(5), 2), initial.add_die(Die(5), 4)]
        self.assertEqual(save_list, expected)
        self.assertEqual(save_list[-1].number_of_dice(Die(5)), target.get_number(Die(5)))
        self.assertNotEqual(save_list[-1].dice_data(), target)

    def test_TableGenerator_crate_save_list_LIMITATION_initial_contains_dice_higher_than_target(self):
        initial = DiceTable.new().add_die(Die(5), 10).add_die(Die(2), 2)
        target = DiceRecord({Die(2): 3, Die(5): 4})
        step_size = 1
        save_list = TableGenerator(target).create_save_list(initial, step_size)
        expected = [initial.add_die(Die(2))]
        self.assertEqual(save_list, expected)

        self.assertEqual(save_list[-1].dice_data(), DiceRecord({Die(2): 3, Die(5): 10}))

    def test_TableGenerator_create_save_list_multiple_dice_hit_target_smallest_dice_filled_first(self):
        initial = DiceTable.new()
        target = DiceRecord({Die(2): 4, Die(4): 2})
        step_size = 4
        two_d2 = initial.add_die(Die(2), 2)
        four_d2 = two_d2.add_die(Die(2), 2)
        four_d2_one_d4 = four_d2.add_die(Die(4))
        four_d2_two_d4 = four_d2_one_d4.add_die(Die(4))

        save_list = TableGenerator(target).create_save_list(initial, step_size)
        expected = [two_d2, four_d2, four_d2_one_d4, four_d2_two_d4]

        self.assertEqual(save_list, expected)
        self.assertEqual(save_list[-1].dice_data(), target)

    def test_TableGenerator_create_save_list_one_type_of_dice_already_at_target(self):
        target = DiceRecord({Die(2): 4, Die(4): 2})
        step_size = 4

        initial = DiceTable.new().add_die(Die(2), 4)
        four_d2_one_d4 = initial.add_die(Die(4))
        four_d2_two_d4 = four_d2_one_d4.add_die(Die(4))
        save_list = TableGenerator(target).create_save_list(initial, step_size)
        expected = [four_d2_one_d4, four_d2_two_d4]

        self.assertEqual(save_list, expected)
        self.assertEqual(save_list[-1].dice_data(), target)

    def test_TableGenerator_create_save_list_empty_return(self):
        target = DiceRecord({Die(2): 4, Die(4): 2})
        step_size = 4

        initial = DiceTable.new().add_die(Die(2), 3).add_die(Die(4), 2)
        self.assertEqual(TableGenerator(target).create_save_list(initial, step_size), [])

    def test_TableGenerator_create_save_list_die_much_larger_than_step_size(self):
        target = DiceRecord({Die(2): 4, Die(40): 2})
        step_size = 4
        initial = DiceTable.new().add_die(Die(2), 3)

        expected = [initial.add_die(Die(40)), initial.add_die(Die(40), 2)]
        save_list = TableGenerator(target).create_save_list(initial, step_size)

        self.assertEqual(expected, save_list)
        self.assertEqual(save_list[-1].dice_data(), DiceRecord({Die(2): 3, Die(40): 2}))

    def test_TableGenerator_create_save_list_with_queue_empty_return(self):
        q = Queue()
        target = DiceRecord({Die(6): 11})
        initial = DiceTable.new().add_die(Die(6), 10)

        TableGenerator(target).create_save_list(initial, 30, q)

        self.assertEqual(q.get(), 'STOP')

    def test_TableGenerator_create_save_list_with_queue(self):
        q = Queue()
        target = DiceRecord({Die(6): 8})
        initial = DiceTable.new().add_die(Die(6), 5)

        TableGenerator(target).create_save_list(initial, 1, q)

        expected = ['<DiceTable containing [6D6]>', '<DiceTable containing [7D6]>', '<DiceTable containing [8D6]>']
        for repr_str in expected:
            value = q.get()
            self.assertEqual(value, repr_str)

        self.assertEqual(q.get(), 'STOP')


    def test_TableGenerator_create_target_table_initial_is_target(self):
        target = DiceRecord({Die(2): 4, Die(4): 3})
        initial = DiceTable.new().add_die(Die(2), 4).add_die(Die(4), 3)

        self.assertEqual(initial, TableGenerator(target).create_target_table(initial))

    def test_TableGenerator_create_target_table_initial_is_new(self):
        target = DiceRecord({Die(2): 4, Die(4): 3})
        expected = DiceTable.new().add_die(Die(2), 4).add_die(Die(4), 3)

        self.assertEqual(expected, TableGenerator(target).create_target_table(DiceTable.new()))

    def test_TableGenerator_create_target_table_initial_is_intermediary(self):
        target = DiceRecord({Die(2): 4, Die(4): 3})
        initial = DiceTable.new().add_die(Die(2)).add_die(Die(4))
        self.assertEqual(initial.dice_data(), DiceRecord({Die(2): 1, Die(4): 1}))

        expected = DiceTable.new().add_die(Die(2), 4).add_die(Die(4), 3)

        self.assertEqual(expected, TableGenerator(target).create_target_table(initial))

    def test_TableGenerator_create_target_table_LIMITATION_initial_contains_dice_not_in_target(self):
        target = DiceRecord({Die(2): 4, Die(4): 3})
        initial = DiceTable.new().add_die(Die(2)).add_die(Die(5))
        self.assertEqual(initial.dice_data(), DiceRecord({Die(2): 1, Die(5): 1}))

        expected = DiceTable.new().add_die(Die(2), 4).add_die(Die(4), 3)

        self.assertNotEqual(expected, TableGenerator(target).create_target_table(initial))

    def test_TableGenerator_create_target_table_FAILS_when_initial_has_value_higher_than_target(self):
        target = DiceRecord({Die(2): 4, Die(4): 3})
        initial = DiceTable.new().add_die(Die(2)).add_die(Die(4), 5)
        self.assertEqual(initial.dice_data(), DiceRecord({Die(2): 1, Die(4): 5}))

        self.assertRaises(DiceRecordError, TableGenerator(target).create_target_table, initial)

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

    def test_apply_modifier_zero_mod(self):
        table = DiceTable.new().add_die(Die(2)).add_die(Die(3))
        self.assertEqual(table.get_dict(), {2: 1, 3: 2, 4: 2, 5: 1})

        no_mod = apply_modifier(table, 0)
        self.assertEqual(no_mod.get_dict(), table.get_dict())
        self.assertNotEqual(no_mod, table)
        self.assertEqual(no_mod.dice_data(), DiceRecord({Die(2): 1, Die(3): 1, Modifier(0): 1}))

    def test_apply_modifier_positive_mod(self):
        table = DiceTable.new().add_die(Die(2)).add_die(Die(3))
        self.assertEqual(table.get_dict(), {2: 1, 3: 2, 4: 2, 5: 1})

        pos_mod = apply_modifier(table, 2)
        expected_dict = {4: 1, 5: 2, 6: 2, 7: 1}

        self.assertEqual(pos_mod.get_dict(), expected_dict)
        self.assertEqual(pos_mod.dice_data(), DiceRecord({Die(2): 1, Die(3): 1, Modifier(2): 1}))

    def test_apply_modifier_negative_mod(self):
        table = DiceTable.new().add_die(Die(2)).add_die(Die(3))
        self.assertEqual(table.get_dict(), {2: 1, 3: 2, 4: 2, 5: 1})

        pos_mod = apply_modifier(table, -3)
        expected_dict = {-1: 1, 0: 2, 1: 2, 2: 1}

        self.assertEqual(pos_mod.get_dict(), expected_dict)
        self.assertEqual(pos_mod.dice_data(), DiceRecord({Die(2): 1, Die(3): 1, Modifier(-3): 1}))

    def test_apply_modifier_used_against_extract_modifier(self):
        initial_table = DiceTable.new().add_die(ModDie(2, -1), 5).add_die(ModWeightedDie({1: 2, 3: 4}, 2), 2)
        modifier, new_record = extract_modifiers(initial_table.dice_data())

        no_mods = DiceTable.new().add_die(Die(2), 5).add_die(WeightedDie({1: 2, 3: 4}), 2)
        self.assertEqual(no_mods.dice_data(), new_record)
        self.assertEqual(modifier, -1)

        like_initial = apply_modifier(no_mods, modifier)
        self.assertEqual(like_initial.get_dict(), initial_table.get_dict())
        self.assertNotEqual(like_initial.dice_data(), initial_table.dice_data())
