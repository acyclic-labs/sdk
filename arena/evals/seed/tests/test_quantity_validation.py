import unittest
from shop import Cart, CATALOG
class T(unittest.TestCase):
    def test_rejects_bad_qty(self):
        c = Cart()
        for bad in (0, -1):
            with self.assertRaises(ValueError): c.add(CATALOG["apple"], bad)
        c.add(CATALOG["apple"], 2); self.assertEqual(c.count(), 2)
