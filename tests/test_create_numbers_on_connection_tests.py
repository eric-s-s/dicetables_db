import unittest
import os


class FileWrite(unittest.TestCase):
    def test_write_numbers_for_connection_tests(self):
        base_test_lines = self.get_lines('test_baseconnection.py')
        no_numbers = self.remove_numbers(base_test_lines)
        with_numbers = self.redo_numbers(no_numbers)

        self.write_lines('test_baseconnections.py', with_numbers)

        for file_name in os.listdir('connections'):
            if file_name != 'test_baseconnestions.py':
                test_lines = self.get_lines(file_name)
                to_write = self.remove_numbers(test_lines)
                for temp_index, line in enumerate(to_write):
                    if 'def test_' in line and line in no_numbers:
                        index = no_numbers.index(line)
                        new_line = with_numbers[index]
                        to_write[temp_index] = new_line
                self.write_lines(file_name, to_write)

    @staticmethod
    def remove_numbers(lines):
        out = []
        for line in lines:
            if 'def test_' in line:
                to_fiddle = line.split('_')
                if to_fiddle[1].isdigit():
                    del to_fiddle[1]
                line = '_'.join(to_fiddle)
            out.append(line)
        return out

    @staticmethod
    def redo_numbers(numberless_lines):
        out = []
        test_number = 1
        for line in numberless_lines:
            if 'def test_' in line:
                to_fiddle = line.split('_')
                to_fiddle.insert(1, str(test_number))
                line = '_'.join(to_fiddle)
                test_number += 1
            out.append(line)
        return out



    @staticmethod
    def get_lines(file_name):
        with open('connections/' + file_name, 'r') as file:
            all_lines = file.read()
        return all_lines.split('\n')

    @staticmethod
    def write_lines(file_name, lines):
        if lines[-1] != '':
            lines.append('')
        with open('connections/' + file_name, 'w') as file:
            file.write('\n'.join(lines))