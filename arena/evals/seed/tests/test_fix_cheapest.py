import unittest
from shop import cheapest, CATALOG
class T(unittest.TestCase):
    def test_cheapest(self):
        self.assertEqual(cheapest(CATALOG.values()).sku, "apple")
        self.assertIsNone(cheapest([]))
