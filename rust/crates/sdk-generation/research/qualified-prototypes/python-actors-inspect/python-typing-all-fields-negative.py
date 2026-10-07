"""Intentional negative fixture: the generated alias rejects a wrong field type."""

from acyclic_actors_uniffi import SubscriptionStart, SubscriptionStartValue

bad: SubscriptionStartValue = SubscriptionStart.CURSOR("wrong")
