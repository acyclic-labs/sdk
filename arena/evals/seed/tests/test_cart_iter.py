import unittest
from shop import Cart, CATALOG
class T(unittest.TestCase):
    def test_iter_and_len(self):
        c = Cart(); c.add(CATALOG["apple"], 2); c.add(CATALOG["milk"])
        self.assertEqual(len(c), 3)
        self.assertEqual(sorted(sku for sku, qty in c), ["apple", "milk"])
