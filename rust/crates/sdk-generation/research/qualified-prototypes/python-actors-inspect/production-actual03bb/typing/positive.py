"""Static typing coverage for the actual03bb producer output."""

from acyclic_actors import (
    ActorId,
    ActorLimits,
    ActorObservation,
    ActorState,
    AddSubscriptionRequest,
    AddSubscriptionResponse,
    Binding,
    CheckpointActorRequest,
    CheckpointActorResponse,
    CodeSha256,
    CreateActorRequest,
    CreateActorResponse,
    ErrorCode,
    Header,
    InspectActorRequest,
    InspectActorResponse,
    InvokeActorRequest,
    InvokeActorResponse,
    RemoveSubscriptionRequest,
    RemoveSubscriptionResponse,
    ResumeSubscriptionRequest,
    ResumeSubscriptionResponse,
    ServiceError,
    Start,
    StartValue,
    SubscriptionObservation,
    SubscriptionSpec,
    SubscriptionStart,
    SubscriptionState,
    UpdateActorRequest,
    UpdateActorResponse,
)

MAX_U64: int = 2**64 - 1
actor_id: ActorId = "actor-a"
digest: CodeSha256 = bytes(range(32))
binding: Binding = Binding(name="handler", capability="http", resource="https://example.test")
limits: ActorLimits = ActorLimits(
    handler_timeout_millis=MAX_U64,
    memory_bytes=1,
    checkpoint_bytes=MAX_U64,
)
cursor_start: StartValue = Start.CURSOR(MAX_U64)
current_head_start: StartValue = Start.CURRENT_HEAD(True)
cursor_message: SubscriptionStart = SubscriptionStart(start=cursor_start)
head_message: SubscriptionStart = SubscriptionStart(start=current_head_start)
subscription: SubscriptionSpec = SubscriptionSpec(
    subscription_id="events",
    stream_path="events/input",
    start=cursor_message,
    placement_anchor=True,
)
observation: SubscriptionObservation = SubscriptionObservation(
    subscription_id="events",
    stream_path="events/input",
    state=SubscriptionState.ACTIVE,
    delivered_cursor=MAX_U64,
    completed_cursor=0,
    recoverable_cursor=1,
    placement_anchor=True,
    retry_count=2,
    failure_code="",
    failed_cursor=None,
)
actor: ActorObservation = ActorObservation(
    actor_id=actor_id,
    code_sha256=digest,
    home_region="home",
    state=ActorState.ACTIVE,
    subscriptions=[observation],
    checkpoint_unix_millis=None,
    checkpoint_epoch=MAX_U64,
    configuration_revision=MAX_U64,
)
create: CreateActorRequest = CreateActorRequest(
    code_sha256=digest,
    home_region="home",
    bindings=[binding],
    limits=limits,
    subscriptions=[subscription],
    idempotency_key="create-key",
)
update: UpdateActorRequest = UpdateActorRequest(
    actor_id=actor_id,
    code_sha256=digest,
    bindings=[binding],
    limits=limits,
    expected_configuration_revision=MAX_U64,
    idempotency_key="update-key",
)
inspect: InspectActorRequest = InspectActorRequest(actor_id=actor_id)
add: AddSubscriptionRequest = AddSubscriptionRequest(
    actor_id=actor_id,
    subscription=subscription,
    idempotency_key="add-key",
)
remove: RemoveSubscriptionRequest = RemoveSubscriptionRequest(
    actor_id=actor_id,
    subscription_id="events",
    idempotency_key="remove-key",
)
resume: ResumeSubscriptionRequest = ResumeSubscriptionRequest(
    actor_id=actor_id,
    subscription_id="events",
    idempotency_key="resume-key",
)
checkpoint: CheckpointActorRequest = CheckpointActorRequest(
    actor_id=actor_id,
    idempotency_key="checkpoint-key",
)
headers: list[Header] = [Header(name="content-type", value="application/json")]
invoke: InvokeActorRequest = InvokeActorRequest(
    actor_id=actor_id,
    method="POST",
    url="/invoke",
    body=b"request-body",
    headers=headers,
)
create_result: CreateActorResponse = CreateActorResponse(actor=actor)
update_result: UpdateActorResponse = UpdateActorResponse(actor=None)
inspect_result: InspectActorResponse = InspectActorResponse(actor=actor)
add_result: AddSubscriptionResponse = AddSubscriptionResponse(actor=actor)
remove_result: RemoveSubscriptionResponse = RemoveSubscriptionResponse(actor=None)
resume_result: ResumeSubscriptionResponse = ResumeSubscriptionResponse(actor=actor)
checkpoint_result: CheckpointActorResponse = CheckpointActorResponse(actor=None)
invoke_result: InvokeActorResponse = InvokeActorResponse(status=201, body=b"", headers=headers)
error: ServiceError = ServiceError(code=ErrorCode.INVALID_ARGUMENT, message="bad request")
