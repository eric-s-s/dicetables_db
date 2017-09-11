from string import printable
import unittest

from dicetables import (DiceRecord, DiceTable, EventsCalculations, Parser, ParseError, LimitsError, InvalidEventsError,
                        Die, ModDie, WeightedDie, ModWeightedDie, StrongDie, Exploding, ExplodingOn, Modifier)
from dicetables_db.requesthandler import RequestHandler
from dicetables_db.connections.mongodb_connection import MongoDBConnection
from dicetables_db.connections.sql_connection import SQLConnection


class TestRequestHandler(unittest.TestCase):
    def setUp(self):
        self.handler = RequestHandler.using_SQL(':memory:', 'test')

    def test_using_sql(self):
        handler = RequestHandler.using_SQL(':memory:', 'test')
        self.assertIsInstance(handler, RequestHandler)
        conn = handler._conn
        self.assertIsInstance(conn, SQLConnection)
        self.assertEqual(conn.get_info()['current_collection'], 'test')
        self.assertEqual(conn.get_info()['db'], ':memory:')

    def test_using_mongo_db(self):
        "connecting is slow."
        handler = RequestHandler.using_mongo_db('test_db', 'test', port=27017)
        self.assertIsInstance(handler, RequestHandler)
        conn = handler._conn
        self.assertIsInstance(conn, MongoDBConnection)
        self.assertEqual(conn.get_info()['current_collection'], 'test')
        self.assertEqual(conn.get_info()['db'], 'test_db')
        self.assertEqual(conn.get_info()['ip'], 'localhost')
        self.assertEqual(conn.get_info()['port'], '27017')

    def test_get_table(self):
        self.assertEqual(self.handler.get_table(), DiceTable.new())
        self.handler._table = DiceTable.new().add_die(Die(6), 2).add_die(Die(5))
        self.assertEqual(self.handler.get_table(), DiceTable.new().add_die(Die(6), 2).add_die(Die(5)))

    def test_request_dice_table_construction(self):
        self.handler.request_dice_table_construction('2*Die(5) & 1*Die(4)')
        self.assertEqual(self.handler.get_table(), DiceTable.new().add_die(Die(5), 2).add_die(Die(4)))

    def test_request_dice_table_construction_single_dice_do_not_need_number(self):
        self.handler.request_dice_table_construction('2*Die(5) & Die(4)')
        self.assertEqual(self.handler.get_table(), DiceTable.new().add_die(Die(5), 2).add_die(Die(4)))

    def test_request_dice_table_construction_all_dice_singly(self):
        all_dice = [Die(die_size=2), ModDie(2, modifier=-1), WeightedDie(dictionary_input={3: 4, 5: 6, 7: 8, 9: 0}),
                    ModWeightedDie({1: 2, 3: 4}, 0), StrongDie(input_die=Die(2), multiplier=2),
                    Exploding(Die(2), explosions=1), ExplodingOn(Die(3), explodes_on=(1, 2)), Modifier(modifier=-100)]

        for die in all_dice:
            self.handler.request_dice_table_construction('2 * {!r}'.format(die))
            self.assertEqual(self.handler.get_table(), DiceTable.new().add_die(die, 2))

    def test_request_dice_table_construction_all_kwargs(self):
        all_dice = ['ModDie(die_size=2, modifier=-1)', 'WeightedDie(dictionary_input={3: 4, 5: 6, 7: 8, 9: 0})',
                    'ModWeightedDie(dictionary_input={3: 4, 5: 6, 7: 8, 9: 0}, modifier=-3)',
                    'StrongDie(input_die=Die(2), multiplier=2)', 'Modifier(modifier=-100)',
                    'Exploding(Die(2), explosions=1)', 'ExplodingOn(Die(3), explodes_on=(1, 2))']

        for die_str in all_dice:
            die = Parser().parse_die(die_str)
            self.handler.request_dice_table_construction('2 * {}'.format(die_str))
            self.assertEqual(self.handler.get_table(), DiceTable.new().add_die(die, 2))

    def test_request_dice_table_construction_mixed_case(self):
        request = 'dIe(DiE_sIzE=3)'
        self.handler.request_dice_table_construction(request)
        self.assertEqual(self.handler.get_table(), DiceTable.new().add_die(Die(3)))

    def assert_dice_table_construction_all_dice_as_pairs(self, num_delimiter, pairs_delimiter):
        all_dice = [Die(die_size=2), ModDie(2, modifier=-1), WeightedDie(dictionary_input={3: 4, 5: 6, 7: 8, 9: 0}),
                    ModWeightedDie({1: 2, 3: 4}, 0), StrongDie(input_die=Die(2), multiplier=2),
                    Exploding(Die(2), explosions=1), ExplodingOn(Die(3), explodes_on=(1, 2)), Modifier(modifier=-100)]

        all_strs = ['Die(die_size=2)', 'ModDie(2, modifier=-1)',
                    'WeightedDie(dictionary_input={3: 4, 5: 6, 7: 8, 9: 0})', 'ModWeightedDie({1: 2, 3: 4}, 0)',
                    'StrongDie(input_die=Die(2), multiplier=2)',
                    'Exploding(Die(2), explosions=1)', 'ExplodingOn(Die(3), explodes_on=(1, 2))',
                    'Modifier(modifier=-100)']

        for index in range(len(all_dice)):
            die_1 = all_dice[index - 1]
            str_1 = all_strs[index - 1]

            die_2 = all_dice[index]
            str_2 = all_strs[index]

            request_str = '2 {num_delimiter} {str_1} {pairs_delimiter} 3 {num_delimiter} {str_2}'.format(
                num_delimiter=num_delimiter, pairs_delimiter=pairs_delimiter, str_1=str_1, str_2=str_2)

            self.handler.request_dice_table_construction(request_str,
                                                         num_delimiter=num_delimiter, pairs_delimiter=pairs_delimiter)

            self.assertEqual(self.handler.get_table(),
                             DiceTable.new().add_die(die_1, 2).add_die(die_2, 3))

    def test_request_dice_table_construction_all_dice_as_pairs_default_delimiters(self):
        self.assert_dice_table_construction_all_dice_as_pairs(num_delimiter='*', pairs_delimiter='&')

    def test_disallowed_delimiters_raise_value_error(self):
        expected_allowed = "!\"#$%&'*+./;<>?@\\^`|~\t\n\r"
        answer = ""
        for char in printable:
            try:
                self.handler.request_dice_table_construction('Die(6)', num_delimiter=char)
                answer += char
            except ValueError as e:
                self.assertTrue(e.args[0].startswith('Delimiters may not be'))
        self.assertEqual(expected_allowed, answer)

    def test_request_dice_table_construction_with_all_allowed_delimiters(self):
        allowed = "!\"#$%&'*+./;<>?@\\^`|~\t\n\r"

        for delimiter_index in range(len(allowed)):
            num_delimiter = allowed[delimiter_index - 1]
            pairs_delimiter = allowed[delimiter_index]
            self.assert_dice_table_construction_all_dice_as_pairs(num_delimiter=num_delimiter,
                                                                  pairs_delimiter=pairs_delimiter)

    def test_request_dice_table_construction_each_error_raised(self):
        instructions = '2*Die(5) & *Die(4)'
        self.assertRaises(ValueError, self.handler.request_dice_table_construction, instructions)

        instructions = '3 die(3)'
        self.assertRaises(SyntaxError, self.handler.request_dice_table_construction, instructions)

        instructions = '3 * die("a")'
        self.assertRaises(AttributeError, self.handler.request_dice_table_construction, instructions)

        instructions = 'didfde(3)'
        self.assertRaises(ParseError, self.handler.request_dice_table_construction, instructions)

        instructions = 'die(1, 2, 3)'
        self.assertRaises(IndexError, self.handler.request_dice_table_construction, instructions)

        instructions = 'die(30000)'
        self.assertRaises(LimitsError, self.handler.request_dice_table_construction, instructions)

        instructions = 'die(-1)'
        self.assertRaises(InvalidEventsError, self.handler.request_dice_table_construction, instructions)

    def test_request_dice_table_construction_errors(self):
        errors = (ValueError, SyntaxError, AttributeError, IndexError, ParseError, LimitsError, InvalidEventsError)
        instructions = ['* Die(4)', '3 die(3)', '3 & die(3)', 'Die(4) * 3 * Die(5)', '4 $ die(5)',
                        '2 * die(5) $ 4 * die(6)', 'die("a")', 'die(5', 'die(5000)', 'notadie(5)',
                        'die(1, 2, 3)', 'WeightedDie({1, 2})', 'WeightedDie({-1: 1})', 'Die(-1)',
                        'WeightedDie({1: -1})']
        for instruction in instructions:
            self.assertRaises(errors, self.handler.request_dice_table_construction, instruction)
