import unittest
from shop import format_price
class T(unittest.TestCase):
    def test_pads_cents(self):
        self.assertEqual(format_price(105), "$1.05")
        self.assertEqual(format_price(1000), "$10.00")
        self.assertEqual(format_price(7), "$0.07")
