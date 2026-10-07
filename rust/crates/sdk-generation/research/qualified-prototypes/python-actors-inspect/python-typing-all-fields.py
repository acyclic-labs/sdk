"""Static type fixture for every generated data-carrying enum field."""

from acyclic_actors_uniffi import SubscriptionSpec, SubscriptionStart, SubscriptionStartValue


def cursor_spec() -> SubscriptionSpec:
    start: SubscriptionStartValue = SubscriptionStart.CURSOR(42)
    return SubscriptionSpec("sub-cursor", "/stream", start, True)


def current_head_spec() -> SubscriptionSpec:
    start: SubscriptionStartValue = SubscriptionStart.CURRENT_HEAD(True)
    return SubscriptionSpec("sub-current-head", "/stream", start, False)


def accepts_every_variant(value: SubscriptionStartValue) -> SubscriptionStartValue:
    return value


cursor = accepts_every_variant(SubscriptionStart.CURSOR(1))
current_head = accepts_every_variant(SubscriptionStart.CURRENT_HEAD(False))
