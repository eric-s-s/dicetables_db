from unittest import TestCase

import dicetables as dt

from dicetables_db.taskmanager import TaskManager
from dicetables_db.insertandretrieve import DiceTableInsertionAndRetrieval
from dicetables_db.connections.sql_connection import SQLConnection


class TestTaskManager(TestCase):

    def setUp(self):
        self.insert_retrieve = DiceTableInsertionAndRetrieval(SQLConnection(':memory:', 'test'))
        self.task_manager = TaskManager(self.insert_retrieve)
