import unittest
from shop import find_item
class T(unittest.TestCase):
    def test_find(self):
        self.assertEqual(find_item("milk").name, "Milk")
        self.assertEqual(find_item("MILK").name, "Milk")
        self.assertIsNone(find_item("caviar"))
