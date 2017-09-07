import os

"""
quick and dirty file_writer to number tests.

problem: easily find which tests failed in module that inherit from test_baseconnection.py

solution: it searches the files in tests/connections.
it removes numbering system from lines with 'def test_' in them. it sequentially numbers lines in
test_baseconnection.py with 'def test_' in them. it then searches for matching lines in other files
in tests/connections and changes them to match. it writes to original files.

"""


def write_numbers_for_connection_tests():
    if not os.path.isfile('connections/test_baseconnection.py'):
        print('number writing failed')
        return None

    base_test_lines = get_lines('test_baseconnection.py')
    no_numbers = remove_numbers(base_test_lines)
    with_numbers = redo_numbers(no_numbers)

    write_lines('test_baseconnection.py', with_numbers)

    for file_name in os.listdir('connections'):
        if file_name != 'test_baseconnestion.py' and file_name != '__pycache__':
            copy_numbered_names(file_name, no_numbers, with_numbers)


def copy_numbered_names(file_name, no_numbers, with_numbers):
    test_lines = get_lines(file_name)
    to_write = remove_numbers(test_lines)
    for temp_index, line in enumerate(to_write):
        if 'def test_' in line and line in no_numbers:
            index = no_numbers.index(line)
            new_line = with_numbers[index]
            to_write[temp_index] = new_line
    write_lines(file_name, to_write)


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


def get_lines(file_name):
    with open('connections/' + file_name, 'r') as file:
        all_lines = file.read()
    return all_lines.split('\n')


def write_lines(file_name, lines):
    if lines[-1] != '':
        lines.append('')
    with open('connections/' + file_name, 'w') as file:
        file.write('\n'.join(lines))

if __name__ == '__main__':
    write_numbers_for_connection_tests()
