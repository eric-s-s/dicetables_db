import unittest

import dicetables as dt

import dicetables_db.tools.dbprep as prep
from dicetables_db.tools.serializer import Serializer


class TestDBPrep(unittest.TestCase):

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
        self.assertEqual(prepped.get_serialized(), Serializer.serialize(table))

    def test_PrepDiceTable_disallows_empty_table(self):
        self.assertRaises(ValueError, prep.PrepDiceTable, dt.DiceTable.new())

    def test_PrepDiceTable_get_label_list_does_not_mutate_original(self):
        prepped = prep.PrepDiceTable(dt.DiceTable.new().add_die(dt.Die(2)))
        to_mutate = prepped.get_label_list()
        to_mutate[0] = 'oops'
        self.assertEqual(prepped.get_label_list(), [('Die(2)', 1)])

    def test_PrepDiceTable_get_group_list(self):
        table = dt.DiceTable.new().add_die(dt.Die(2)).add_die(dt.Die(3))
        prepped = prep.PrepDiceTable(table)
        self.assertEqual(prepped.get_group_list(), ['Die(2)', 'Die(3)'])

    def test_PrepDiceTable_get_group(self):
        table = dt.DiceTable.new().add_die(dt.Die(2)).add_die(dt.Die(3))
        prepped = prep.PrepDiceTable(table)
        self.assertEqual(prepped.get_group(), 'Die(2)&Die(3)')

    def test_PrepDiceTable_get_dict(self):
        table = dt.DiceTable.new().add_die(dt.Die(2)).add_die(dt.Die(3))
        prepped = prep.PrepDiceTable(table)
        expected = {'group': 'Die(2)&Die(3)',
                    'score': 5,
                    'serialized': Serializer.serialize(table),
                    'Die(2)': 1,
                    'Die(3)': 1}
        self.assertEqual(prepped.get_dict(), expected)

    def test_SearchParams_init_creates_score(self):
        table_list = [(dt.Die(2), 2), (dt.Die(3), 1)]
        retriever = prep.SearchParams(table_list)
        self.assertEqual(retriever.get_score(), (4 + 3))

    def test_SearchParams_init_disallows_empty_list(self):
        self.assertRaises(ValueError, prep.SearchParams, [])

    def test_SearchParams_get_search_params(self):
        table_list = [(dt.Die(1), 4), (dt.Die(2), 2), (dt.Die(3), 1)]
        retriever = prep.SearchParams(table_list)
        searcher = retriever.get_search_params()
        self.assertEqual(next(searcher), [('Die(1)&Die(2)&Die(3)',
                                          {'Die(1)': 4, 'Die(2)': 2, 'Die(3)': 1})])
        self.assertEqual(next(searcher),
                         [('Die(1)&Die(2)', {'Die(1)': 4, 'Die(2)': 2}),
                          ('Die(1)&Die(3)', {'Die(1)': 4, 'Die(3)': 1}),
                          ('Die(2)&Die(3)', {'Die(2)': 2, 'Die(3)': 1})]
                         )
        self.assertEqual(next(searcher),
                         [('Die(1)', {'Die(1)': 4}),
                          ('Die(2)', {'Die(2)': 2}),
                          ('Die(3)', {'Die(3)': 1})]
                         )
        self.assertRaises(StopIteration, next, searcher)

    def test_SearchParams_get_search_params_can_have_two_independent_generators(self):
        table_list = [(dt.Die(1), 4), (dt.Die(2), 2), (dt.Die(3), 1)]
        retriever = prep.SearchParams(table_list)
        searcher = retriever.get_search_params()
        other_searcher = retriever.get_search_params()
        self.assertEqual(next(searcher), [('Die(1)&Die(2)&Die(3)',
                                          {'Die(1)': 4, 'Die(2)': 2, 'Die(3)': 1})])

        self.assertEqual(next(other_searcher), [('Die(1)&Die(2)&Die(3)',
                                                 {'Die(1)': 4, 'Die(2)': 2, 'Die(3)': 1})])

        self.assertEqual(next(searcher),
                         [('Die(1)&Die(2)', {'Die(1)': 4, 'Die(2)': 2}),
                          ('Die(1)&Die(3)', {'Die(1)': 4, 'Die(3)': 1}),
                          ('Die(2)&Die(3)', {'Die(2)': 2, 'Die(3)': 1})]
                         )
        self.assertEqual(next(searcher),
                         [('Die(1)', {'Die(1)': 4}),
                          ('Die(2)', {'Die(2)': 2}),
                          ('Die(3)', {'Die(3)': 1})]
                         )
        self.assertRaises(StopIteration, next, searcher)

        self.assertEqual(next(other_searcher),
                         [('Die(1)&Die(2)', {'Die(1)': 4, 'Die(2)': 2}),
                          ('Die(1)&Die(3)', {'Die(1)': 4, 'Die(3)': 1}),
                          ('Die(2)&Die(3)', {'Die(2)': 2, 'Die(3)': 1})]
                         )
        self.assertEqual(next(other_searcher),
                         [('Die(1)', {'Die(1)': 4}),
                          ('Die(2)', {'Die(2)': 2}),
                          ('Die(3)', {'Die(3)': 1})]
                         )
        self.assertRaises(StopIteration, next, other_searcher)

    def test_SearchParams_get_search_params_stop_iteration(self):
        table_list = [(dt.Die(1), 4), (dt.Die(2), 2), (dt.Die(3), 1)]
        retriever = prep.SearchParams(table_list)
        result = [lst for lst in retriever.get_search_params()]
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], [('Die(1)&Die(2)&Die(3)', {'Die(1)': 4, 'Die(2)': 2, 'Die(3)': 1})])
        self.assertEqual(result[1],
                         [('Die(1)&Die(2)', {'Die(1)': 4, 'Die(2)': 2}),
                          ('Die(1)&Die(3)', {'Die(1)': 4, 'Die(3)': 1}),
                          ('Die(2)&Die(3)', {'Die(2)': 2, 'Die(3)': 1})]
                         )
        self.assertEqual(result[2],
                         [('Die(1)', {'Die(1)': 4}),
                          ('Die(2)', {'Die(2)': 2}),
                          ('Die(3)', {'Die(3)': 1})]
                         )


if __name__ == "__main__":
    unittest.main()
