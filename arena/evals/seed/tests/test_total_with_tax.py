import unittest
from shop import Cart, CATALOG, total_cents
class T(unittest.TestCase):
    def test_tax(self):
        c = Cart(); c.add(CATALOG["bread"], 2)   # 500
        self.assertEqual(total_cents(c, tax_percent=10), 550)
        self.assertEqual(total_cents(c, tax_percent=0), 500)
        self.assertEqual(total_cents(c, tax_percent=7), 535)
