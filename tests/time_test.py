
import unittest
from os import remove, path

import dicetables as dt

from dt_sql import dbinterface as dbi


class TimeTest(unittest.TestCase):

    connection = dbi.Connection('timer.db')

    # @classmethod
    # def setUpClass(cls):
    #     cls.connection = dbi.Connection('timer.db')


    @classmethod
    def tearDownClass(cls):
        cls.connection.shut_down()

    def __init__(self, *args, **kwargs):
        self.command = dbi.ConnectionCommandInterface(self.connection)
        super(TimeTest, self).__init__(*args, **kwargs)

    def test_something(self):
        # self.assertEqual(self.connection.get_tables(), ['master'])
        pass


def clear_file(file_name):
    print('removing file')
    if path.isfile(file_name):
        remove(file_name)


# def setUpModule():
#     clear_file('timer.db')


def tearDownModule():
    clear_file('timer.db')


if __name__ == '__main__':
    unittest.main()
