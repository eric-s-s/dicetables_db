# import unittest
# import pickle
#
# from mongo_dicetables.serializer import Serializer
#
#
# class TestDBPrep(unittest.TestCase):
#
#     def test_Serializer_serialize(self):
#         self.assertEqual(Serializer.serialize(12), pickle.dumps(12))
#
#     def test_Serializer_deserialize(self):
#         data = Serializer.serialize(12)
#         self.assertNotEqual(data, 12)
#         self.assertEqual(Serializer.deserialize(data), 12)
