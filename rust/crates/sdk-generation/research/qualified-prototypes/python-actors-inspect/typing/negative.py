"""Deliberately invalid cases for a maintained static type checker.

A checker should report each marked argument/assignment. This file is never
executed; it is a negative qualification fixture for generated annotations.
"""
from acyclic_actors_uniffi import (
    ActorId,
    ActorLimits,
    CodeSha256,
    Header,
    InvokeActorRequest,
    InvokeActorResponse,
)

bad_actor_id: ActorId = ActorId(42)
bad_digest: CodeSha256 = CodeSha256("not bytes")
bad_limits: ActorLimits = ActorLimits("one", 2, 3)
bad_headers: list[Header] = [("content-type", "application/json")]
bad_request = InvokeActorRequest(
    ActorId("actor-a"),
    "POST",
    "/invoke",
    "not bytes",
    bad_headers,
)
bad_response: int = InvokeActorResponse(status=201, body=b"", headers=[])
