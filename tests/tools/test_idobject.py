import unittest
from bson.objectid import ObjectId

from dicetables_db.tools.idobject import IdObject


class TestIdObject(unittest.TestCase):

    def test_new_constructor(self):
        new = IdObject.new()
        self.assertEqual(new.__class__, IdObject)

    def test_to_string(self):
        self.assertIsInstance(IdObject.new().to_string(), str)

    def test_to_bson_id(self):
        self.assertIsInstance(IdObject.new().to_bson_id(), ObjectId)

    def test_from_string_constructor(self):
        to_test = IdObject.new()
        expected_equal = IdObject.from_string(to_test.to_string())
        self.assertEqual(to_test, expected_equal)

    def test_from_bson_id_constructor(self):
        to_test = IdObject.new()
        expected_equal = IdObject.from_bson_id(to_test.to_bson_id())
        self.assertEqual(to_test, expected_equal)

    def test__eq__false(self):
        ne_1 = IdObject.new()
        ne_2 = IdObject.new()
        ne_3 = 'hello'
        self.assertFalse(ne_1.__eq__(ne_2))
        self.assertFalse(ne_1.__eq__(ne_3))

    def test__ne__true(self):
        ne_1 = IdObject.new()
        ne_2 = IdObject.new()
        ne_3 = 'hello'
        self.assertTrue(ne_1.__ne__(ne_2))
        self.assertTrue(ne_1.__ne__(ne_3))

    def test__ne__false(self):
        eq_1 = IdObject.new()
        eq_2 = IdObject.from_bson_id(eq_1.to_bson_id())
        self.assertFalse(eq_1.__ne__(eq_2))

    def test__lt__true(self):
        small = IdObject.new()
        large = IdObject.new()
        self.assertTrue(small.__lt__(large))

    def test__lt__false(self):
        small = IdObject.new()
        small_2 = IdObject.from_bson_id(small.to_bson_id())
        large = IdObject.new()
        self.assertFalse(large.__lt__(small))
        self.assertFalse(small.__lt__(small_2))

    def test__le__true(self):
        small = IdObject.new()
        small_2 = IdObject.from_bson_id(small.to_bson_id())
        large = IdObject.new()
        self.assertTrue(small.__le__(large))
        self.assertTrue(small.__le__(small_2))

    def test__le__false(self):
        small = IdObject.new()
        large = IdObject.new()
        self.assertFalse(large.__le__(small))

    def test__gt__true(self):
        small = IdObject.new()
        large = IdObject.new()
        self.assertTrue(large.__gt__(small))

    def test__gt__false(self):
        small = IdObject.new()
        small_2 = IdObject.from_bson_id(small.to_bson_id())
        large = IdObject.new()
        self.assertFalse(small.__gt__(large))
        self.assertFalse(small.__gt__(small_2))

    def test__ge__true(self):
        small = IdObject.new()
        small_2 = IdObject.from_bson_id(small.to_bson_id())
        large = IdObject.new()
        self.assertTrue(large.__ge__(small))
        self.assertTrue(small.__ge__(small_2))

    def test__ge__false(self):
        small = IdObject.new()
        large = IdObject.new()
        self.assertFalse(small.__ge__(large))

    def test__repr__(self):
        new = IdObject.new()
        new_str = new.to_string()
        self.assertEqual('ObjectId.from_string({})'.format(new_str), new.__repr__())

    def test__str__(self):
        new = IdObject.new()
        new_str = new.to_string()
        self.assertEqual(new_str, new.__str__())

if __name__ == '__main__':
    unittest.main()
