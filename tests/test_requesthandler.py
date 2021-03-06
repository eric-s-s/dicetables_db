from queue import Queue
from string import printable
import unittest

from dicetables import (DiceTable, DetailedDiceTable, DiceRecord, Parser,
                        ParseError, LimitsError, InvalidEventsError, DiceRecordError,
                        Die, ModDie, WeightedDie, ModWeightedDie, StrongDie, Exploding, ExplodingOn, Modifier)

from dicetables_db.connections.mongodb_connection import MongoDBConnection
from dicetables_db.connections.sql_connection import SQLConnection

from dicetables_db.requesthandler import RequestHandler, make_dict


class TestRequestHandler(unittest.TestCase):
    def setUp(self):
        self.handler = RequestHandler.using_SQL(':memory:', 'test')

    def tearDown(self):
        self.handler.close_connection()

    def test_using_sql(self):
        handler = RequestHandler.using_SQL(':memory:', 'test')
        self.assertIsInstance(handler, RequestHandler)
        conn = handler._conn
        self.assertIsInstance(conn, SQLConnection)
        self.assertEqual(conn.get_info()['current_collection'], 'test')
        self.assertEqual(conn.get_info()['db'], ':memory:')

    def test_using_mongo_db(self):
        handler = RequestHandler.using_mongo_db('test_db', 'test', port=27017)
        self.assertIsInstance(handler, RequestHandler)
        conn = handler._conn
        self.assertIsInstance(conn, MongoDBConnection)
        self.assertEqual(conn.get_info()['current_collection'], 'test')
        self.assertEqual(conn.get_info()['db'], 'test_db')
        self.assertEqual(conn.get_info()['ip'], 'localhost')
        self.assertEqual(conn.get_info()['port'], '27017')

    def test_init_default_max_score(self):
        self.assertEqual(self.handler._max_dice_value, 12000)
        new_handler = RequestHandler.using_mongo_db('test_db', 'test', max_dice_value=10)
        self.assertEqual(new_handler._max_dice_value, 10)
        new_handler.close_connection()

        new_handler = RequestHandler.using_SQL(':memory:', 'test', max_dice_value=100)
        self.assertEqual(new_handler._max_dice_value, 100)
        new_handler.close_connection()

    def test_get_table(self):
        self.assertEqual(self.handler.get_table(), DiceTable.new())
        self.handler._table = DiceTable.new().add_die(Die(6), 2).add_die(Die(5))
        self.assertEqual(self.handler.get_table(), DiceTable.new().add_die(Die(6), 2).add_die(Die(5)))

    def test_request_dice_table_empty_string(self):
        self.handler.request_dice_table_construction('')
        self.assertEqual(self.handler.get_table(), DiceTable.new())

    def test_request_dice_table_empty_string_queue(self):
        q = Queue()
        self.handler.request_dice_table_construction('', q)
        self.assertEqual(q.get(), 'STOP')

    def test_request_dice_table_only_whitespace(self):
        self.handler.request_dice_table_construction('   ')
        self.assertEqual(self.handler.get_table(), DiceTable.new())

    def test_request_dice_table_construction_request_exceeds_max_dice_value(self):
        handler = RequestHandler.using_SQL(':memory:', 'test', max_dice_value=12)
        handler.request_dice_table_construction('2*Die(6)')
        handler.request_dice_table_construction('6*WeightedDie({1: 2, 2: 10})')
        with self.assertRaises(ValueError) as cm:
            handler.request_dice_table_construction('1*Die(6)&1*Die(7)')
        self.assertEqual(cm.exception.args[0], 'The sum of all max(die_size, len(die_dict))*die_number must be <= 12')

    def test_request_dice_table_construction_exceed_max_dice_value_based_on_max_of_dict_len_and_die_size(self):
        handler = RequestHandler.using_SQL(':memory:', 'test', max_dice_value=12)

        self.assertEqual(len(Exploding(Die(4)).get_dict()), 10)
        handler.request_dice_table_construction('Exploding(Die(4))')
        self.assertEqual(len(Exploding(Die(5)).get_dict()), 13)
        self.assertEqual(Exploding(Die(5)).get_size(), 5)
        self.assertRaises(ValueError, handler.request_dice_table_construction, 'Exploding(Die(5))')

        self.assertEqual(WeightedDie({1: 1, 12: 1}).get_size(), 12)
        handler.request_dice_table_construction('WeightedDie({1: 1, 12: 1})')
        self.assertEqual(len(WeightedDie({1: 1, 13: 1}).get_dict()), 2)
        self.assertEqual(WeightedDie({1: 1, 13: 1}).get_size(), 13)
        self.assertRaises(ValueError, handler.request_dice_table_construction, 'WeightedDie({1: 1, 13: 1})')

    def test_request_dice_table_construction(self):
        self.handler.request_dice_table_construction('2*Die(5) & 1*Die(4)')
        self.assertEqual(self.handler.get_table(), DiceTable.new().add_die(Die(5), 2).add_die(Die(4)))

    def test_request_dice_table_construction_leading_and_trailing_whitespace(self):
        self.handler.request_dice_table_construction('   2  *  Die( 5 )   &   1  *  Die( 4 )   ')
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

        instructions = '-2*die(2)'
        self.assertRaises(DiceRecordError, self.handler.request_dice_table_construction, instructions)

    def test_request_dice_table_construction_errors(self):
        errors = (ValueError, SyntaxError, AttributeError, IndexError,
                  ParseError, LimitsError, InvalidEventsError, DiceRecordError)
        instructions = ['* Die(4)', '3 die(3)', '3 & die(3)', 'Die(4) * 3 * Die(5)', '4 $ die(5)',
                        '2 * die(5) $ 4 * die(6)', 'die("a")', 'die(5', 'die(5000)', 'notadie(5)',
                        'die(1, 2, 3)', 'WeightedDie({1, 2})', 'WeightedDie({-1: 1})', 'Die(-1)',
                        'WeightedDie({1: -1})', '-2*Die(2)']
        for instruction in instructions:
            self.assertRaises(errors, self.handler.request_dice_table_construction, instruction)

    def test_request_dice_table_construction_with_update_queue(self):
        q = Queue()
        self.handler.request_dice_table_construction('20*Die(6)', q)
        expected = ['<DiceTable containing [5D6]>', '<DiceTable containing [10D6]>', '<DiceTable containing [15D6]>',
                    '<DiceTable containing [20D6]>']
        for value in expected:
            self.assertEqual(value, q.get())
        self.assertEqual(q.get(), 'STOP')

    def test_close_connection(self):
        self.handler.close_connection()
        self.assertRaises(AttributeError, self.handler.request_dice_table_construction, '1*Die(6)')

    def test_make_dict_simple_table(self):
        answer = make_dict(DiceTable.new().add_die(Die(4)))
        expected = {
            'name': '<DiceTable containing [1D4]>',
            'diceStr': 'Die(4): 1',
            'data': [(1, 2, 3, 4), (25.0, 25.0, 25.0, 25.0)],
            'tableString': '1: 1\n2: 1\n3: 1\n4: 1\n',
            'forSciNum': {1: ['1.00000', '0'], 2: ['1.00000', '0'], 3: ['1.00000', '0'], 4: ['1.00000', '0']},
            'range': (1, 4),
            'mean': 2.5,
            'stddev': 1.118
        }

        self.assertEqual(answer, expected)

    def test_make_dict_large_number_table(self):
        table = DiceTable({1: 1, 2: 99**1000}, DiceRecord.new())
        answer = make_dict(table)
        expected = {
            'data': [(1, 2), (0.0, 100.0)],
            'forSciNum': {1: ['1.00000', '0'], 2: ['4.31712', '1995']},
            'mean': 2.0,
            'range': (1, 2),
            'name': '<DiceTable containing []>',
            'diceStr': '',
            'stddev': 0.0,
            'tableString': '1: 1\n2: 4.317e+1995\n'
        }

        self.assertEqual(answer, expected)

    def test_make_dict_complex_table(self):
        table = DiceTable.new().add_die(WeightedDie({1: 1, 2: 99}), 3).add_die(Die(3), 4)
        answer = make_dict(table)
        expected = {
            'name': '<DiceTable containing [3D2  W:100, 4D3]>',
            'diceStr': 'WeightedDie({1: 1, 2: 99}): 3\nDie(3): 4',
            'data': [
                (7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18),
                (1.234567901234568e-06, 0.0003716049382716049, 0.03777901234567901, 1.3467864197530863,
                 5.16049012345679, 12.566786419753084, 19.861979012345678, 23.34457160493827, 19.53086790123457,
                 12.124566666666665, 4.8279, 1.1978999999999997)
            ],
            'tableString': (' 7: 1\n' +
                            ' 8: 301\n' +
                            ' 9: 30,601\n' +
                            '10: 1,090,897\n' +
                            '11: 4,179,997\n' +
                            '12: 1.018e+7\n' +
                            '13: 1.609e+7\n' +
                            '14: 1.891e+7\n' +
                            '15: 1.582e+7\n' +
                            '16: 9,820,899\n' +
                            '17: 3,910,599\n' +
                            '18: 970,299\n'),
            'forSciNum': {
                7: ['1.00000', '0'],
                8: ['3.01000', '2'],
                9: ['3.06010', '4'],
                10: ['1.09090', '6'],
                11: ['4.18000', '6'],
                12: ['1.01791', '7'],
                13: ['1.60882', '7'],
                14: ['1.89091', '7'],
                15: ['1.58200', '7'],
                16: ['9.82090', '6'],
                17: ['3.91060', '6'],
                18: ['9.70299', '5']},
            'range': (7, 18),
            'mean': 13.97,
            'stddev': 1.642
        }
        self.assertEqual(answer, expected)

    def test_make_dict_mean_and_stddev_rounding(self):
        table = DetailedDiceTable.new().add_die(WeightedDie({1: 1, 2: 2}))
        answer = make_dict(table)
        self.assertEqual(table.calc.mean(), 1.6666666666666667)
        self.assertEqual(answer['mean'], 1.667)

        self.assertEqual(table.calc.stddev(3), 0.471)
        self.assertEqual(answer['stddev'], 0.471)

    def test_get_response_error_response(self):
        instructions = '2*Die(5) & *Die(4)'
        response = self.handler.get_response(instructions)
        self.assertEqual(response,
                         {"error": "invalid literal for int() with base 10: ' '", "type": "ValueError"})

        instructions = '3 die(3)'
        response = self.handler.get_response(instructions)
        self.assertEqual(response,
                         {'error': 'invalid syntax', 'type': 'SyntaxError'})

        instructions = '3 * die("a")'
        response = self.handler.get_response(instructions)
        self.assertEqual(response,
                         {'error': "'Str' object has no attribute 'n'", 'type': 'AttributeError'})

        instructions = 'didfde(3)'
        response = self.handler.get_response(instructions)
        self.assertEqual(response,
                         {'error': 'Die class: <didfde> not recognized by parser.', 'type': 'ParseError'})

        instructions = 'die(1, 2, 3)'
        response = self.handler.get_response(instructions)
        self.assertEqual(response,
                         {'error': 'tuple index out of range', 'type': 'IndexError'})

        instructions = 'die(30000)'
        response = self.handler.get_response(instructions)
        self.assertEqual(response,
                         {'error': 'Max die_size: 500', 'type': 'LimitsError'})

        instructions = 'die(-1)'
        response = self.handler.get_response(instructions)
        self.assertEqual(response,
                         {'error': 'events may not be empty. a good alternative is the identity - {0: 1}.',
                          'type': 'InvalidEventsError'})

        instructions = '-2*die(2)'
        response = self.handler.get_response(instructions)
        self.assertEqual(response,
                         {'error': 'Tried to add_die or remove_die with a negative number.', 'type': 'DiceRecordError'})

    def test_get_response_empty_string_and_whitespace(self):
        q = Queue()
        empty_str_answer = self.handler.get_response('', q)
        self.assertEqual(q.get(), 'STOP')

        empty_response = {'data': [(0,), (100.0,)],
                          'forSciNum': {0: ['1.00000', '0']},
                          'mean': 0.0,
                          'range': (0, 0),
                          'name': '<DiceTable containing []>',
                          'diceStr': '',
                          'stddev': 0.0,
                          'tableString': '0: 1\n'}
        self.assertEqual(empty_str_answer, empty_response)

        whitespace_str_answer = self.handler.get_response('   ')
        self.assertEqual(whitespace_str_answer, empty_response)

    def test_get_response_no_error(self):
        table = DiceTable.new().add_die(Die(6), 10).add_die(Die(3), 12)
        expected = make_dict(table)
        instructions = '10*Die(6)&12*Die(3)'
        reverse_instructions = '12*Die(3)&10*Die(6)'
        self.assertEqual(self.handler.get_response(instructions), expected)
        self.assertEqual(self.handler.get_response(reverse_instructions), expected)

    def test_get_response_is_connecting_to_database(self):
        instructions = '10*Die(6)'
        self.handler.get_response(instructions)
        answer = self.handler._conn.find()
        expected = [
            {'group': 'Die(6)', 'score': 30, 'Die(6)': 5},
            {'group': 'Die(6)', 'score': 60, 'Die(6)': 10}
        ]
        for index, partial_document in enumerate(expected):
            answer_document = answer[index]
            for key in partial_document:
                self.assertEqual(answer_document[key], partial_document[key])

    def test_get_response_with_queue(self):
        q = Queue()
        instructions = '10*Die(6)'
        expected = make_dict(DiceTable.new().add_die(Die(6), 10))
        answer = self.handler.get_response(instructions, q)
        self.assertEqual(expected, answer)
        expected_queue = ['<DiceTable containing [5D6]>', '<DiceTable containing [10D6]>', 'STOP']
        for element in expected_queue:
            self.assertEqual(q.get(), element)
