"""
the front-end that handles all requests and returns completed tables
"""
import dicetables as dt
from dicetables_db import insertandretrieve as dbi


class TableManagement(object):
    def __init__(self, interface):
        self._interface = interface

    @classmethod
    def create_for_mongo_db(cls, db_name, collection_name, ip='localhost', port=27017):
        connection = dbi.Connection(db_name, collection_name, ip=ip,  port=port)
        interface = dbi.DiceTableInsertionAndRetrieval(connection)
        return cls(interface)

    def save(self, dice_table):
        if not is_new_table(dice_table) and not self._interface.has_collection(dice_table):
            self._interface.add_collection(dice_table)


def is_new_table(dice_table):
    return dice_table == dt.DiceTable.new()
