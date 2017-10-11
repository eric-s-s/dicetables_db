import string

from dicetables_db.connections.sql_connection import SQLConnection
from dicetables_db.connections.mongodb_connection import MongoDBConnection
from dicetables_db.connections.baseconnection import BaseConnection

from insertandretrieve import DiceTableInsertionAndRetrieval
from taskmanager import TaskManager

from dicetables import (Parser, ParseError, DiceRecordError, InvalidEventsError, LimitsError,
                        DiceTable, DiceRecord, EventsCalculations)


class RequestHandler(object):
    def __init__(self, connection: BaseConnection) -> None:
        self._conn = connection
        self._task_manager = TaskManager(DiceTableInsertionAndRetrieval(self._conn))
        self._table = DiceTable.new()
        self._parser = Parser(ignore_case=True)

    @classmethod
    def using_SQL(cls, db_path, collection_name):
        return cls(SQLConnection(db_path, collection_name))

    @classmethod
    def using_mongo_db(cls, db_name, collection_name, ip='localhost', port=27017):
        return cls(MongoDBConnection(db_name, collection_name, ip, port))

    def request_dice_table_construction(self, instructions: str,
                                        num_delimiter: str = '*', pairs_delimiter: str = '&') -> None:

        reserved_characters = '_[]{}(),: -=\x0b\x0c' + string.digits + string.ascii_letters
        if num_delimiter in reserved_characters or pairs_delimiter in reserved_characters:
            raise ValueError('Delimiters may not be {!r}'.format(reserved_characters))

        record = DiceRecord.new()
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

        self._table = self._task_manager.process_request(record)

    def get_table(self):
        return self._table

