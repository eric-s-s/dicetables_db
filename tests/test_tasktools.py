from unittest import TestCase

import dicetables as dt

from dicetables_db.tasktools import extract_modifiers, TableGenerator


class TestTaskTool(TestCase):
    def test_place_holder(self):
        table_generator = TableGenerator()
        self.assertIsInstance(table_generator, TableGenerator)
