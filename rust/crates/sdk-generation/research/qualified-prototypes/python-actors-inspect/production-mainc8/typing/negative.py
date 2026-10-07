"""Deliberate type errors for the current mainc8 producer output."""

from actors_c8.acyclic_actors import (
    ActorId, ActorLimits, Binding, CodeSha256, PositiveU64, Start,
    SubscriptionStart,
)

wrong_actor_brand: ActorId = CodeSha256.from_value(bytes(range(32)))
wrong_digest_brand: CodeSha256 = ActorId.from_value("actor-a")
wrong_limit_brand: PositiveU64 = ActorId.from_value("actor-a")
wrong_limits: ActorLimits = ActorLimits(
    handler_timeout_millis=1,
    memory_bytes=PositiveU64.from_value(2), checkpoint_bytes=PositiveU64.from_value(3),
)
wrong_head: SubscriptionStart = SubscriptionStart(start=Start.CURRENT_HEAD(False))
wrong_cursor: SubscriptionStart = SubscriptionStart(start=Start.CURSOR("not a cursor"))
wrong_presence: Binding = Binding(
    name="handler", capability=None, resource="https://example.test",
)
readonly_limits = ActorLimits(
    handler_timeout_millis=PositiveU64.from_value(1),
    memory_bytes=PositiveU64.from_value(2), checkpoint_bytes=PositiveU64.from_value(3),
)
readonly_limits.memory_bytes = PositiveU64.from_value(4)
