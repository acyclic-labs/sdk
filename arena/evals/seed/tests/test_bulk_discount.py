import unittest
from shop import Cart, CATALOG, bulk_discount_cents
class T(unittest.TestCase):
    def test_bulk(self):
        c = Cart(); c.add(CATALOG["apple"], 10)   # 500 cents, 10 units -> 5% off = 25
        self.assertEqual(bulk_discount_cents(c), 25)
        c2 = Cart(); c2.add(CATALOG["apple"], 9)
        self.assertEqual(bulk_discount_cents(c2), 0)
        c3 = Cart(); c3.add(CATALOG["bread"], 10); c3.add(CATALOG["apple"], 10)  # 2500+500 -> 5% of 3000 = 150
        self.assertEqual(bulk_discount_cents(c3), 150)
