//! Rust-owned Actors v1 contract.
//!
//! Protify derives the prost wire implementation and the complete protobuf
//! schema from these declarations. The tonic service facade below remains a
//! generated transport adapter and consumes these same message types.

use protify::*;

proto_package!(ACTORS_PACKAGE, name = "acyclic.actors.v1", files = [ACTORS_FILE]);
define_proto_file!(
    ACTORS_FILE,
    name = "actors/v1/actors.proto",
    package = ACTORS_PACKAGE,
    messages = [
        Binding,
        ActorLimits,
        SubscriptionStart,
        SubscriptionSpec,
        SubscriptionObservation,
        ActorObservation,
        CreateActorRequest,
        CreateActorResponse,
        UpdateActorRequest,
        UpdateActorResponse,
        InspectActorRequest,
        InspectActorResponse,
        AddSubscriptionRequest,
        AddSubscriptionResponse,
        RemoveSubscriptionRequest,
        RemoveSubscriptionResponse,
        ResumeSubscriptionRequest,
        ResumeSubscriptionResponse,
        CheckpointActorRequest,
        CheckpointActorResponse,
        Header,
        InvokeActorRequest,
        InvokeActorResponse,
        Error,
    ],
    enums = [SubscriptionState, ActorState, ErrorCode],
    services = [ActorsService],
);

#[proto_message]
pub struct Binding {
    #[proto(tag = 1)]
    pub name: String,
    #[proto(tag = 2)]
    pub capability: String,
    #[proto(tag = 3)]
    pub resource: String,
}

#[proto_message]
pub struct ActorLimits {
    #[proto(tag = 1)]
    pub handler_timeout_millis: u64,
    #[proto(tag = 2)]
    pub memory_bytes: u64,
    #[proto(tag = 3)]
    pub checkpoint_bytes: u64,
}

pub mod subscription_start {
    use super::*;

    #[proto_oneof]
    pub enum Start {
        #[proto(tag = 1)]
        Cursor(u64),
        #[proto(tag = 2)]
        CurrentHead(bool),
    }
}

#[proto_message]
pub struct SubscriptionStart {
    #[proto(oneof(tags(1, 2)))]
    pub start: Option<subscription_start::Start>,
}

#[proto_message]
pub struct SubscriptionSpec {
    #[proto(tag = 1)]
    pub subscription_id: String,
    #[proto(tag = 2)]
    pub stream_path: String,
    #[proto(tag = 3, message)]
    pub start: Option<SubscriptionStart>,
    #[proto(tag = 4)]
    pub placement_anchor: bool,
}

#[proto_enum]
pub enum SubscriptionState {
    Unspecified = 0,
    Active = 1,
    Paused = 2,
}

#[proto_message]
pub struct SubscriptionObservation {
    #[proto(tag = 1)]
    pub subscription_id: String,
    #[proto(tag = 2)]
    pub stream_path: String,
    #[proto(tag = 3, enum_(SubscriptionState))]
    pub state: i32,
    #[proto(tag = 4)]
    pub delivered_cursor: u64,
    #[proto(tag = 5)]
    pub completed_cursor: u64,
    #[proto(tag = 6)]
    pub recoverable_cursor: u64,
    #[proto(tag = 7)]
    pub placement_anchor: bool,
    #[proto(tag = 8)]
    pub retry_count: u32,
    #[proto(tag = 9)]
    pub failure_code: String,
    #[proto(tag = 10)]
    pub failed_cursor: Option<u64>,
}

#[proto_enum]
pub enum ActorState {
    Unspecified = 0,
    Active = 1,
    Hibernated = 2,
    Paused = 3,
}

#[proto_message]
pub struct ActorObservation {
    #[proto(tag = 1)]
    pub actor_id: String,
    #[proto(tag = 2, bytes)]
    pub code_sha256: Bytes,
    #[proto(tag = 3)]
    pub home_region: String,
    #[proto(tag = 4, enum_(ActorState))]
    pub state: i32,
    #[proto(tag = 5, repeated(message))]
    pub subscriptions: Vec<SubscriptionObservation>,
    #[proto(tag = 6)]
    pub checkpoint_unix_millis: Option<u64>,
    #[proto(tag = 7)]
    pub checkpoint_epoch: u64,
    #[proto(tag = 8)]
    pub configuration_revision: u64,
}

#[proto_message]
pub struct CreateActorRequest {
    #[proto(tag = 1, bytes)]
    pub code_sha256: Bytes,
    #[proto(tag = 2)]
    pub home_region: String,
    #[proto(tag = 3, repeated(message))]
    pub bindings: Vec<Binding>,
    #[proto(tag = 4, message)]
    pub limits: Option<ActorLimits>,
    #[proto(tag = 5, repeated(message))]
    pub subscriptions: Vec<SubscriptionSpec>,
    #[proto(tag = 6)]
    pub idempotency_key: String,
}

#[proto_message]
pub struct CreateActorResponse {
    #[proto(tag = 1, message)]
    pub actor: Option<ActorObservation>,
}

/// Full configuration replacement with CAS. The new code's checkpoint schema
/// must be compatible or explicitly migrated before activation; failure keeps
/// the previous version active. Paused subscriptions stay paused until resumed.
#[proto_message]
pub struct UpdateActorRequest {
    #[proto(tag = 1)]
    pub actor_id: String,
    #[proto(tag = 2, bytes)]
    pub code_sha256: Bytes,
    #[proto(tag = 3, repeated(message))]
    pub bindings: Vec<Binding>,
    #[proto(tag = 4, message)]
    pub limits: Option<ActorLimits>,
    #[proto(tag = 5)]
    pub expected_configuration_revision: u64,
    #[proto(tag = 6)]
    pub idempotency_key: String,
}

#[proto_message]
pub struct UpdateActorResponse {
    #[proto(tag = 1, message)]
    pub actor: Option<ActorObservation>,
}

#[proto_message]
pub struct InspectActorRequest {
    #[proto(tag = 1)]
    pub actor_id: String,
}

#[proto_message]
pub struct InspectActorResponse {
    #[proto(tag = 1, message)]
    pub actor: Option<ActorObservation>,
}

#[proto_message]
pub struct AddSubscriptionRequest {
    #[proto(tag = 1)]
    pub actor_id: String,
    #[proto(tag = 2, message)]
    pub subscription: Option<SubscriptionSpec>,
    #[proto(tag = 3)]
    pub idempotency_key: String,
}

#[proto_message]
pub struct AddSubscriptionResponse {
    #[proto(tag = 1, message)]
    pub actor: Option<ActorObservation>,
}

#[proto_message]
pub struct RemoveSubscriptionRequest {
    #[proto(tag = 1)]
    pub actor_id: String,
    #[proto(tag = 2)]
    pub subscription_id: String,
    #[proto(tag = 3)]
    pub idempotency_key: String,
}

#[proto_message]
pub struct RemoveSubscriptionResponse {
    #[proto(tag = 1, message)]
    pub actor: Option<ActorObservation>,
}

/// Resumption may replay a previously delivered record and duplicate external effects.
#[proto_message]
pub struct ResumeSubscriptionRequest {
    #[proto(tag = 1)]
    pub actor_id: String,
    #[proto(tag = 2)]
    pub subscription_id: String,
    #[proto(tag = 3)]
    pub idempotency_key: String,
}

#[proto_message]
pub struct ResumeSubscriptionResponse {
    #[proto(tag = 1, message)]
    pub actor: Option<ActorObservation>,
}

#[proto_message]
pub struct CheckpointActorRequest {
    #[proto(tag = 1)]
    pub actor_id: String,
    #[proto(tag = 2)]
    pub idempotency_key: String,
}

#[proto_message]
pub struct CheckpointActorResponse {
    #[proto(tag = 1, message)]
    pub actor: Option<ActorObservation>,
}

#[proto_message]
pub struct Header {
    #[proto(tag = 1)]
    pub name: String,
    #[proto(tag = 2)]
    pub value: String,
}

/// Invocation is not an implicit Stream append or persistence guarantee.
#[proto_message]
pub struct InvokeActorRequest {
    #[proto(tag = 1)]
    pub actor_id: String,
    #[proto(tag = 2)]
    pub method: String,
    #[proto(tag = 3)]
    pub url: String,
    #[proto(tag = 4, bytes)]
    pub body: Bytes,
    #[proto(tag = 5, repeated(message))]
    pub headers: Vec<Header>,
}

#[proto_message]
pub struct InvokeActorResponse {
    #[proto(tag = 1)]
    pub status: u32,
    #[proto(tag = 2, bytes)]
    pub body: Bytes,
    #[proto(tag = 3, repeated(message))]
    pub headers: Vec<Header>,
}

#[proto_enum]
pub enum ErrorCode {
    Unspecified = 0,
    InvalidArgument = 1,
    CapabilityDenied = 2,
    CapabilityExpired = 3,
    ActorNotFound = 4,
    SubscriptionNotFound = 5,
    IdempotencyMismatch = 6,
    Conflict = 7,
    AdmissionDenied = 8,
    CheckpointFailed = 9,
    DependencyUnavailable = 10,
}

#[proto_message]
pub struct Error {
    #[proto(tag = 1, enum_(ErrorCode))]
    pub code: i32,
    #[proto(tag = 2)]
    pub message: String,
}

#[proto_service]
pub enum ActorsService {
    CreateActor {
        request: CreateActorRequest,
        response: CreateActorResponse,
    },
    UpdateActor {
        request: UpdateActorRequest,
        response: UpdateActorResponse,
    },
    InspectActor {
        request: InspectActorRequest,
        response: InspectActorResponse,
    },
    AddSubscription {
        request: AddSubscriptionRequest,
        response: AddSubscriptionResponse,
    },
    RemoveSubscription {
        request: RemoveSubscriptionRequest,
        response: RemoveSubscriptionResponse,
    },
    ResumeSubscription {
        request: ResumeSubscriptionRequest,
        response: ResumeSubscriptionResponse,
    },
    CheckpointActor {
        request: CheckpointActorRequest,
        response: CheckpointActorResponse,
    },
    InvokeActor {
        request: InvokeActorRequest,
        response: InvokeActorResponse,
    },
}

include!("generated/acyclic.actors.v1.tonic.rs");
