import unittest
from shop import Cart, CATALOG, subtotal_cents


class BaseTests(unittest.TestCase):
    def test_add_and_count(self):
        c = Cart(); c.add(CATALOG["apple"], 3); c.add(CATALOG["milk"])
        self.assertEqual(c.count(), 4)

    def test_subtotal(self):
        c = Cart(); c.add(CATALOG["apple"], 2); c.add(CATALOG["bread"])
        self.assertEqual(subtotal_cents(c), 350)
