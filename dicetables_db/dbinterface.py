import dicetables_db.dbprep as prep
from dicetables_db.tools.serializer import Serializer


class ConnectionCommandInterface(object):
    def __init__(self, connection):
        self._conn = connection
        if not self.has_required_index():
            self._create_required_index()

    @property
    def connection_info(self):
        return self._conn.get_info()

    def has_required_index(self):
        return self._conn.has_index(('group', 'score'))

    def _create_required_index(self):
        self._conn.create_index(('group', 'score'))

    def reset(self):
        self._conn.reset_collection()
        self._create_required_index()

    def has_table(self, table):
        finder = Finder(self._conn, table.get_list())
        return finder.get_exact_match() is not None

    def add_table(self, table):
        adder = prep.PrepDiceTable(table)
        obj_id = self._conn.insert(adder.get_dict())
        return self._conn.get_id_string(obj_id)

    def find_nearest_table(self, dice_list):
        finder = Finder(self._conn, dice_list)
        obj_id = finder.get_exact_match()
        if obj_id is None:
            obj_id = finder.find_nearest_table()

        if obj_id is None:
            return None
        return self._conn.get_id_string(obj_id)

    def get_table(self, id_str):
        obj_id = self._conn.get_id_object(id_str)
        data = self._conn.find_one({'_id': obj_id}, {'serialized': 1})
        return Serializer.deserialize(data['serialized'])


class Finder(object):
    """
    all searches return ObjectId
    """
    def __init__(self, connection, dice_list):
        self._conn = connection
        self._param_maker = prep.SearchParams(dice_list)
        self._param_score = self._param_maker.get_score()

        self._obj_id = None
        self._highest_found_score = 0

    def get_exact_match(self):
        query_dict = self._get_query_dict_for_exact()
        obj_id_in_dict = self._conn.find_one(query_dict, {'_id': 1})
        if obj_id_in_dict is None:
            return None
        return obj_id_in_dict['_id']

    def _get_query_dict_for_exact(self):
        group, dice_dict = next(self._param_maker.get_search_params())[0]
        dice_dict['group'] = group
        dice_dict['score'] = self._param_score
        return dice_dict

    def find_nearest_table(self):
        for search_param in self._param_maker.get_search_params():
            if self._is_close_enough():
                break
            candidates = self._get_list_of_candidates(search_param)
            if candidates:
                biggest_score = max(candidates, key=lambda dictionary: dictionary['score'])
                self._update_id_and_highest_score(biggest_score)

        if self._obj_id is None:
            return None
        return self._obj_id

    def _is_close_enough(self):
        close_enough = 0.8
        return (self._highest_found_score / float(self._param_score)) >= close_enough

    def _get_list_of_candidates(self, group_dice_dict_list):
        out = []
        for group, dice_dict in group_dice_dict_list:
            query_dict = self._get_query_dict_for_nearest(group, dice_dict)
            cursor = self._conn.find(query_dict, {'_id': 1, 'score': 1})
            out += list(cursor)
        return out

    def _get_query_dict_for_nearest(self, group, dice_dict):
        output_dict = {'group': group, 'score': {'$lte': self._param_score}}
        for die_repr, num in dice_dict.items():
            output_dict[die_repr] = {'$lte': num}
        return output_dict

    def _update_id_and_highest_score(self, score_id_dict):
        if score_id_dict['score'] > self._highest_found_score:
            self._obj_id = score_id_dict['_id']
            self._highest_found_score = score_id_dict['score']

