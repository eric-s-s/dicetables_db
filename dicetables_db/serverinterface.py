import json

from dicetables import DiceTable, EventsCalculations
from dicetables_db.requesthandler import RequestHandler


def make_dict(dice_table: DiceTable):
    calc = EventsCalculations(dice_table)
    out = dict()
    out['repr'] = repr(dice_table).replace('Detailed', '')

    out['data'] = calc.percentage_axes()
    out['tableString'] = calc.full_table_string()

    for_scinum_lst = [el.split(': ') for el in calc.full_table_string(6, -1).split('\n')[:-1]]
    for_scinum_dict = {int(pair[0]): pair[1].split('e+') for pair in for_scinum_lst}

    out['forSciNum'] = for_scinum_dict

    out['range'] = calc.info.events_range()
    out['mean'] = calc.mean()
    out['stddev'] = calc.stddev()
    return json.dumps(out)


class StandIn(object):
    def __init__(self):
        self._handler = RequestHandler.using_mongo_db('try', 'tables')

    def get_response(self, input_str):
        try:
            table = self._handler.request_dice_table_construction(input_str)
            return make_dict(table)
        except BaseException as e:
            return json.dumps({'error': e.args[0], 'type': e.__class__.__name__})
