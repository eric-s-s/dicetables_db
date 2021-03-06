from queue import Queue
import string

from dicetables_db.connections.sql_connection import SQLConnection
from dicetables_db.connections.mongodb_connection import MongoDBConnection
from dicetables_db.connections.baseconnection import BaseConnection

from dicetables_db.insertandretrieve import DiceTableInsertionAndRetrieval
from dicetables_db.taskmanager import TaskManager

from dicetables import (Parser, DiceTable, DiceRecord, EventsCalculations,
                        ParseError, LimitsError, InvalidEventsError, DiceRecordError)


class RequestHandler(object):
    def __init__(self, connection: BaseConnection, max_dice_value=12000) -> None:
        self._conn = connection
        self._task_manager = TaskManager(DiceTableInsertionAndRetrieval(self._conn))
        self._table = DiceTable.new()
        self._parser = Parser(ignore_case=True)
        self._max_dice_value = max_dice_value

    @classmethod
    def using_SQL(cls, db_path, collection_name, max_dice_value=12000):
        return cls(SQLConnection(db_path, collection_name), max_dice_value=max_dice_value)

    @classmethod
    def using_mongo_db(cls, db_name, collection_name, ip='localhost', port=27017, max_dice_value=12000):
        return cls(MongoDBConnection(db_name, collection_name, ip, port), max_dice_value=max_dice_value)

    def request_dice_table_construction(self, instructions: str, update_queue: Queue = None,
                                        num_delimiter: str = '*', pairs_delimiter: str = '&') -> None:

        self._raise_error_for_bad_delimiter(num_delimiter, pairs_delimiter)

        record = DiceRecord.new()

        if instructions.strip() == '':
            number_die_pairs = []
        else:
            number_die_pairs = instructions.split(pairs_delimiter)

        for pair in number_die_pairs:
            if num_delimiter not in pair:
                number = 1
                die = pair
            else:
                num, die = pair.split(num_delimiter)
                number = int(num)
            die = self._parser.parse_die_within_limits(die)
            record = record.add_die(die, number)

        self._check_record_against_max_dice_value(record)

        self._table = self._task_manager.process_request(record, update_queue=update_queue)

    @staticmethod
    def _raise_error_for_bad_delimiter(num_delimiter, pairs_delimiter):
        reserved_characters = '_[]{}(),: -=\x0b\x0c' + string.digits + string.ascii_letters
        if num_delimiter in reserved_characters or pairs_delimiter in reserved_characters:
            raise ValueError('Delimiters may not be {!r}'.format(reserved_characters))

    def _check_record_against_max_dice_value(self, record):
        if sum(
                (max(len(die.get_dict()), die.get_size()) * number) for die, number in record.get_dict().items()
        ) > self._max_dice_value:
            raise ValueError('The sum of all max(die_size, len(die_dict))*die_number must be <= {}'
                             .format(self._max_dice_value))

    def get_table(self):
        return self._table

    def close_connection(self):
        self._conn.close()

    def get_response(self, input_str, update_queue=None):
        errors = (ValueError, SyntaxError, AttributeError, IndexError,
                  ParseError, LimitsError, InvalidEventsError, DiceRecordError)

        try:
            self.request_dice_table_construction(input_str, update_queue)
            return make_dict(self._table)
        except errors as e:
            return {'error': e.args[0], 'type': e.__class__.__name__}


def make_dict(dice_table: DiceTable):
    calc = EventsCalculations(dice_table)
    out = dict()
    out['diceStr'] = '\n'.join(['{!r}: {}'.format(die, number) for die, number in dice_table.get_list()])
    out['name'] = repr(dice_table)

    out['data'] = calc.percentage_axes()
    out['tableString'] = calc.full_table_string()

    for_scinum_lst = [el.split(': ') for el in calc.full_table_string(6, -1).split('\n')[:-1]]
    for_scinum_dict = {int(pair[0]): pair[1].split('e+') for pair in for_scinum_lst}

    out['forSciNum'] = for_scinum_dict

    out['range'] = calc.info.events_range()
    out['mean'] = round(calc.mean(), 3)
    out['stddev'] = calc.stddev(3)
    return out
