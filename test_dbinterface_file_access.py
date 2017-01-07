import unittest
from os import remove, path
import dbinterface as dbi


class TestDBConnectInit(unittest.TestCase):

    def test_non_existent_table(self):
        file_name = 'E:/work/database/does_not_exist.db'
        clear_file(file_name)
        new = dbi.DBConnect('does_not_exist.db')
        self.assertTrue(path.isfile(file_name))
        new.abort()

    def test_non_existent_table_full_path_name(self):
        file_name = 'E:/work/database/does_not_exist.db'
        clear_file(file_name)
        new = dbi.DBConnect(file_name, add_path=False)
        self.assertTrue(path.isfile(file_name))
        new.abort()

    def test_new_table_inits_with_dicetables(self):
        file_name = 'E:/work/database/does_not_exist.db'
        clear_file(file_name)
        new = dbi.DBConnect(file_name, add_path=False)
        self.assertEqual(new.get_tables(), ['dicetables'])
        new.abort()

    def test_non_new_db_does_not_remove_data(self):
        file_name = 'E:/work/database/has_data.db'
        clear_file(file_name)
        test_db = dbi.DBConnect(file_name, False)
        test_db.cursor.execute('create table bob (rnd INTEGER)')
        test_db.cursor.execute('insert into bob (rnd) values(?)', (5,))
        test_db.shut_down()
        test_db = dbi.DBConnect(file_name, False)
        self.assertEqual(test_db.get_tables(), ['dicetables', 'bob'])
        test_db.cursor.execute('select * from bob')
        self.assertEqual(test_db.cursor.fetchall(), [(5, )])
        test_db.abort()

    def test_db_non_empty_missing_dicetables_raises_error(self):
        file_name = 'E:/work/database/no_dicetable.db'
        clear_file(file_name)
        new = dbi.DBConnect(file_name, False)
        new.cursor.execute('create table bob (rnd INTEGER)')
        new.cursor.execute('drop table dicetables')
        new.shut_down()
        self.assertRaises(ValueError, dbi.DBConnect, file_name, False)

    def test_db_non_empty_wrong_dicetables_raises_error(self):
        file_name = 'E:/work/database/bad_dicetable.db'
        clear_file(file_name)
        new = dbi.DBConnect(file_name, False)
        new.cursor.execute('create table bob (rnd INTEGER)')
        new.cursor.execute('drop table dicetables')
        new.cursor.execute('create table dicetable (oops INTEGER, damn BLOB)')
        new.shut_down()
        self.assertRaises(ValueError, dbi.DBConnect, file_name, False)

    @unittest.expectedFailure
    def test_db_abort(self):
        file_name = 'E:/work/database/bad_dicetable.db'
        clear_file(file_name)
        new = dbi.DBConnect(file_name, False)
        new.cursor.execute('create table bob (rnd INTEGER)')
        new.abort()
        new.start_up()
        self.assertEqual(new.get_tables(), ['dicetables'])


def clear_file(file_name):
    if path.isfile(file_name):
        remove(file_name)


if __name__ == '__main__':
    unittest.main()