"""A tiny shop: items, a cart, and pricing. Deliberately small so tasks are checkable."""
from dataclasses import dataclass, field


@dataclass
class Item:
    sku: str
    name: str
    price_cents: int


@dataclass
class Cart:
    lines: dict = field(default_factory=dict)  # sku -> quantity

    def add(self, item: Item, qty: int = 1) -> None:
        self.lines[item.sku] = self.lines.get(item.sku, 0) + qty

    def remove(self, sku: str) -> None:
        del self.lines[sku]

    def count(self) -> int:
        return sum(self.lines.values())


CATALOG = {
    "apple": Item("apple", "Apple", 50),
    "bread": Item("bread", "Bread", 250),
    "milk": Item("milk", "Milk", 120),
}


def subtotal_cents(cart: Cart) -> int:
    total = 0
    for sku, qty in cart.lines.items():
        total += CATALOG[sku].price_cents * qty
    return total


def apply_discount(cents: int, percent: int) -> int:
    # BUG: discount is added instead of subtracted
    return cents + cents * percent // 100


def format_price(cents: int) -> str:
    return f"${cents // 100}.{cents % 100}"


def cheapest(items):
    best = None
    for it in items:
        if best is None or it.price_cents > best.price_cents:
            best = it
    return best
