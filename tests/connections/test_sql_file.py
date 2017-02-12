# import unittest
# from os import remove, path
#
# from connections import sql_connection as interface
#
#
# class TestDBConnectInit(unittest.TestCase):
#
#     def test_non_existent_table(self):
#         file_name = 'E:/work/database/does_not_exist.db'
#         clear_file(file_name)
#         new = interface.MongoDBConnection(file_name)
#         self.assertTrue(path.isfile(file_name))
#         new.abort()
#
#     def test_new_table_inits_with_master(self):
#         file_name = 'E:/work/database/does_not_exist.db'
#         clear_file(file_name)
#         new = interface.MongoDBConnection(file_name)
#         self.assertEqual(new.get_tables(), ['master'])
#         new.abort()
#
#     def test_non_new_db_does_not_remove_data(self):
#         file_name = 'E:/work/database/has_data.db'
#         clear_file(file_name)
#         test_db = interface.MongoDBConnection(file_name)
#         test_db.cursor.execute('create table bob (rnd INTEGER)')
#         test_db.cursor.execute('insert into bob (rnd) values(?)', (5,))
#         test_db.shut_down()
#         test_db = interface.MongoDBConnection(file_name)
#         self.assertEqual(test_db.get_tables(), ['master', 'bob'])
#         test_db.cursor.execute('select * from bob')
#         self.assertEqual(test_db.cursor.fetchall(), [(5, )])
#         test_db.abort()
#
#     def test_db_non_empty_missing_master_raises_error(self):
#         file_name = 'E:/work/database/no_dicetable.db'
#         clear_file(file_name)
#         new = interface.MongoDBConnection(file_name)
#         new.cursor.execute('create table bob (rnd INTEGER)')
#         new.cursor.execute('drop table master')
#         new.shut_down()
#         self.assertRaises(ValueError, interface.MongoDBConnection, file_name)
#
#     def test_db_non_empty_wrong_master_raises_error(self):
#         file_name = 'E:/work/database/bad_dicetable.db'
#         clear_file(file_name)
#         new = interface.MongoDBConnection(file_name)
#         new.cursor.execute('create table bob (rnd INTEGER)')
#         new.cursor.execute('drop table master')
#         new.cursor.execute('create table master (oops INTEGER, damn BLOB)')
#         new.shut_down()
#         self.assertRaises(ValueError, interface.MongoDBConnection, file_name)
#
#     @unittest.expectedFailure
#     def test_db_abort(self):
#         file_name = 'E:/work/database/bad_dicetable.db'
#         clear_file(file_name)
#         new = interface.MongoDBConnection(file_name)
#         new.cursor.execute('create table bob (rnd INTEGER)')
#         new.abort()
#         new.start_up()
#         self.assertEqual(new.get_tables(), ['master'])
#
#
# def clear_file(file_name):
#     if path.isfile(file_name):
#         remove(file_name)
#
#
# if __name__ == '__main__':
#     unittest.main()