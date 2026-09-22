import unittest
from shop import apply_discount
class T(unittest.TestCase):
    def test_discount_subtracts(self):
        self.assertEqual(apply_discount(1000, 10), 900)
        self.assertEqual(apply_discount(999, 0), 999)
