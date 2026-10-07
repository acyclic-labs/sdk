"""Positive static typing cases for the generated Rust-owned Python facade.

Run a maintained checker against this file with the generated module on the
checker path. This file intentionally exercises nominal IDs, fixed bytes,
unsigned values, optional fields, records, enums, and request constructors.
"""
from acyclic_actors_uniffi import (
    ActorId,
    ActorLimits,
    Binding,
    CodeSha256,
    CreateActorRequest,
    Header,
    InvokeActorRequest,
    InvokeActorResponse,
    SubscriptionSpec,
    SubscriptionStart,
)

actor_id: ActorId = ActorId("actor-a")
digest: CodeSha256 = CodeSha256(bytes(range(32)))
binding: Binding = Binding("handler", "http", "https://example.test")
limits: ActorLimits = ActorLimits(1, 2, 3)
start: SubscriptionStart = SubscriptionStart.CURSOR(1)
subscription: SubscriptionSpec = SubscriptionSpec(
    "events",
    "events/input",
    start,
    True,
)
create: CreateActorRequest = CreateActorRequest(
    digest,
    "home",
    [binding],
    limits,
    [subscription],
    "create-key",
)
headers: list[Header] = [Header(name="content-type", value="application/json")]
invoke: InvokeActorRequest = InvokeActorRequest(
    actor_id,
    "POST",
    "/invoke",
    b"request-body",
    headers,
)
response: InvokeActorResponse = InvokeActorResponse(status=201, body=b"", headers=headers)
status: int = response.status
body: bytes = response.body
