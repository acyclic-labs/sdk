import unittest
from shop import Cart, CATALOG
class T(unittest.TestCase):
    def test_remove_missing_is_noop(self):
        c = Cart(); c.add(CATALOG["apple"]); c.remove("nope"); c.remove("apple"); c.remove("apple")
        self.assertEqual(c.count(), 0)
