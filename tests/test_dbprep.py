import unittest
import pickle
import dicetables as dt

import mongo_dicetables.dbprep as prep


class TestToolFuncs(unittest.TestCase):
    
    def test_get_score_empty(self):
        self.assertEqual(prep.get_score([]), 0)

    def test_get_score_one(self):
        self.assertEqual(prep.get_score([(dt.ModDie(3, 10), 4)]), 12)

    def test_get_score_non_zero_weight_normal(self):
        self.assertEqual(prep.get_score([(dt.WeightedDie({1: 1, 5: 6}), 4)]), 24)  # size + 1

    def test_get_score_non_zero_weight_too_low(self):
        self.assertEqual(prep.get_score([(dt.WeightedDie({1: 1, 5: 1}), 4)]), 20)  # size not + 1

    def test_get_score_multiple(self):
        self.assertEqual(prep.get_score([(dt.WeightedDie({1: 1, 5: 6}), 4), (dt.ModDie(3, 10), 4)]), 36)

    def test_get_label_list(self):
        self.assertEqual(prep.get_label_list([]), [])
        self.assertEqual(prep.get_label_list([(dt.Die(3), 2)]), [('Die(3)', 2)])
        self.assertEqual(prep.get_label_list([(dt.Die(3), 2), (dt.WeightedDie({2: 2}), 1)]),
                         [('Die(3)', 2), ('WeightedDie({1: 0, 2: 2})', 1)])

    def test_PrepDiceTable_init(self):
        table = dt.DiceTable.new().add_die(dt.Die(2))
        prepped = prep.PrepDiceTable(table)
        self.assertEqual(prepped.get_label_list(), [('Die(2)', 1)])
        self.assertEqual(prepped.get_score(), 2)
        self.assertEqual(prepped.get_serialized(), pickle.dumps(table))

    def test_PrepDiceTable_get_label_list_does_not_mutate_original(self):
        prepped = prep.PrepDiceTable(dt.DiceTable.new().add_die(dt.Die(2)))
        to_mutate = prepped.get_label_list()
        to_mutate[0] = 'oops'
        self.assertEqual(prepped.get_label_list(), [('Die(2)', 1)])

    def test_PrepDiceTable_get_group_as_list(self):
        table = dt.DiceTable.new().add_die(dt.Die(2)).add_die(dt.Die(3))
        prepped = prep.PrepDiceTable(table)
        self.assertEqual(prepped.get_group_as_list(), ['Die(2)', 'Die(3)'])

    def test_PrepDiceTable_get_group_as_string(self):
        table = dt.DiceTable.new().add_die(dt.Die(2)).add_die(dt.Die(3))
        prepped = prep.PrepDiceTable(table)
        self.assertEqual(prepped.get_group_as_string(), 'Die(2)&Die(3)')

    def test_PrepDiceTable_group_as_list_boolean_through_get_group(self):
        table = dt.DiceTable.new().add_die(dt.Die(2)).add_die(dt.Die(3))
        p_true = prep.PrepDiceTable(table)
        p_false = prep.PrepDiceTable(table, False)

        self.assertEqual(p_true.get_group(), ['Die(2)', 'Die(3)'])
        self.assertEqual(p_false.get_group(), 'Die(2)&Die(3)')

    def test_PrepDiceTable_get_dict_group_as_list_true(self):
        table = dt.DiceTable.new().add_die(dt.Die(2)).add_die(dt.Die(3))
        prepped = prep.PrepDiceTable(table)
        expected = {'group': ['Die(2)', 'Die(3)'],
                    'score': 5,
                    'serialized': pickle.dumps(table),
                    'Die(2)': 1,
                    'Die(3)': 1}
        self.assertEqual(prepped.get_dict(), expected)

    def test_PrepDiceTable_get_dict_group_as_list_false(self):
        table = dt.DiceTable.new().add_die(dt.Die(2)).add_die(dt.Die(3))
        prepped = prep.PrepDiceTable(table, False)
        expected = {'group': 'Die(2)&Die(3)',
                    'score': 5,
                    'serialized': pickle.dumps(table),
                    'Die(2)': 1,
                    'Die(3)': 1}
        self.assertEqual(prepped.get_dict(), expected)




