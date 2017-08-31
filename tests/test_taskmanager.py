from unittest import TestCase

import dicetables as dt

from dicetables_db.taskmanager import TaskManager, create_insert_retrieve


class TestTaskManager(TestCase):
    def test_place_hold(self):
        task_manager = TaskManager.create_for_sql(':memory:', 'test_collection')
        self.assertIsInstance(task_manager, TaskManager)
