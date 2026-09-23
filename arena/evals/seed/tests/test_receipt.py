import unittest
from shop import Cart, CATALOG, receipt
class T(unittest.TestCase):
    def test_receipt_lines(self):
        c = Cart(); c.add(CATALOG["apple"], 2); c.add(CATALOG["bread"])
        r = receipt(c)
        self.assertIn("Apple x2", r); self.assertIn("Bread x1", r)
        self.assertTrue(r.strip().endswith("$3.50"), r)
