import unittest, shop
class T(unittest.TestCase):
    def test_public_functions_documented(self):
        for name in ("subtotal_cents", "apply_discount", "format_price", "cheapest"):
            self.assertTrue((getattr(shop, name).__doc__ or "").strip(), name)
