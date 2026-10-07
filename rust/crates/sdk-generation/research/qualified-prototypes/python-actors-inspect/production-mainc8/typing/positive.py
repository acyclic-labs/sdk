"""Positive typing coverage for the current mainc8 producer output."""

from actors_c8.acyclic_actors import (
    ActorId, ActorLimits, ActorObservation, ActorState, AddSubscriptionRequest,
    Binding, CheckpointActorRequest, CodeSha256, CreateActorRequest,
    CurrentHeadMarker, ErrorCode, Header, InspectActorRequest,
    InvokeActorRequest, InvokeActorResponse, RemoveSubscriptionRequest,
    ResumeSubscriptionRequest, ServiceError, Start, SubscriptionObservation,
    SubscriptionSpec, SubscriptionStart, SubscriptionState, UpdateActorRequest,
    PositiveU64,
)

actor_id: ActorId = ActorId.from_value("actor-a")
digest: CodeSha256 = CodeSha256.from_value(bytes(range(32)))
one: PositiveU64 = PositiveU64.from_value(1)
limits: ActorLimits = ActorLimits(
    handler_timeout_millis=one,
    memory_bytes=PositiveU64.from_value(2),
    checkpoint_bytes=PositiveU64.from_value(3),
)
binding: Binding = Binding(name="handler", capability="http", resource="https://example.test")
cursor_start: SubscriptionStart = SubscriptionStart(start=Start.CURSOR(9007199254740993))
head_start: SubscriptionStart = SubscriptionStart(
    start=Start.CURRENT_HEAD(CurrentHeadMarker.from_value(True)),
)
subscription: SubscriptionSpec = SubscriptionSpec(
    subscription_id="events", stream_path="events/input", start=cursor_start,
    placement_anchor=True,
)
observation: SubscriptionObservation = SubscriptionObservation(
    subscription_id="events", stream_path="events/input", state=SubscriptionState.ACTIVE,
    delivered_cursor=9007199254740993, completed_cursor=9007199254740992,
    recoverable_cursor=9007199254740991, placement_anchor=True, retry_count=0,
    failure_code="", failed_cursor=None,
)
actor: ActorObservation = ActorObservation(
    actor_id=actor_id, code_sha256=digest, home_region="home", state=ActorState.ACTIVE,
    subscriptions=[observation], checkpoint_unix_millis=None, checkpoint_epoch=4,
    configuration_revision=7,
)
create: CreateActorRequest = CreateActorRequest(
    code_sha256=digest, home_region="home", bindings=[binding], limits=limits,
    subscriptions=[subscription], idempotency_key="create-key",
)
update: UpdateActorRequest = UpdateActorRequest(
    actor_id=actor_id, code_sha256=digest, bindings=[binding], limits=limits,
    expected_configuration_revision=7, idempotency_key="update-key",
)
inspect: InspectActorRequest = InspectActorRequest(actor_id=actor_id)
add: AddSubscriptionRequest = AddSubscriptionRequest(
    actor_id=actor_id, subscription=subscription, idempotency_key="add-key",
)
remove: RemoveSubscriptionRequest = RemoveSubscriptionRequest(
    actor_id=actor_id, subscription_id="events", idempotency_key="remove-key",
)
resume: ResumeSubscriptionRequest = ResumeSubscriptionRequest(
    actor_id=actor_id, subscription_id="events", idempotency_key="resume-key",
)
checkpoint: CheckpointActorRequest = CheckpointActorRequest(
    actor_id=actor_id, idempotency_key="checkpoint-key",
)
invoke: InvokeActorRequest = InvokeActorRequest(
    actor_id=actor_id, method="POST", url="/invoke", body=b"request-body",
    headers=[Header(name="content-type", value="application/json")],
)
response: InvokeActorResponse = InvokeActorResponse(status=201, body=b"response-body", headers=[])
service_error: ServiceError = ServiceError(code=ErrorCode.INVALID_ARGUMENT, message="invalid")

_all_values = (actor, create, update, inspect, add, remove, resume, checkpoint,
               invoke, response, service_error, head_start)
