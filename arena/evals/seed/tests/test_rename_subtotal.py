import unittest, shop
class T(unittest.TestCase):
    def test_renamed(self):
        self.assertTrue(hasattr(shop, "cart_subtotal_cents"))
        self.assertFalse(hasattr(shop, "subtotal_cents"))
        c = shop.Cart(); c.add(shop.CATALOG["milk"], 2)
        self.assertEqual(shop.cart_subtotal_cents(c), 240)
