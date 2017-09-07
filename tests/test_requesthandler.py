from string import digits, ascii_letters
import unittest

from dicetables import DiceRecord, DiceTable, Die, ParseError, LimitsError, InvalidEventsError
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

