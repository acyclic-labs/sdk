"""Intentional type errors for the actual03bb producer output."""

from acyclic_actors import (
    ActorLimits,
    ActorObservation,
    Start,
    StartValue,
    SubscriptionObservation,
    SubscriptionStart,
)

bad_cursor: StartValue = Start.CURSOR("wrong")
bad_head: StartValue = Start.CURRENT_HEAD(1)
bad_start: SubscriptionStart = SubscriptionStart(start=Start.CURSOR("wrong"))
bad_limits: ActorLimits = ActorLimits(handler_timeout_millis=0, memory_bytes="wrong", checkpoint_bytes=1)
bad_observation: SubscriptionObservation = SubscriptionObservation(
    subscription_id="events",
    stream_path="events/input",
    state="ACTIVE",
    delivered_cursor="wrong",
    completed_cursor=0,
    recoverable_cursor=1,
    placement_anchor=True,
    retry_count=2,
    failure_code="",
    failed_cursor=None,
)
bad_actor: ActorObservation = ActorObservation(
    actor_id="actor-a",
    code_sha256=b"short",
    home_region="home",
    state="ACTIVE",
    subscriptions=[],
    checkpoint_unix_millis=None,
    checkpoint_epoch="wrong",
    configuration_revision=1,
)
