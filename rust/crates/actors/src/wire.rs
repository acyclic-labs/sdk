//! Rust-owned Actors v1 contract.
//!
//! Protify derives the prost wire implementation and the complete protobuf
//! schema from these declarations. The tonic service facade below remains a
//! generated transport adapter and consumes these same message types.

use protify::*;
use ts_rs::TS;

proto_package!(
    ACTORS_PACKAGE,
    name = "acyclic.actors.v1",
    files = [ACTORS_FILE]
);
define_proto_file!(
    ACTORS_FILE,
    name = "actors/v1/actors.proto",
    package = ACTORS_PACKAGE,
    options =
        [proto_option!("go_package" => "github.com/acyclic-labs/sdk/go/gen/actors/v1;actorsv1")],
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

/// Renders the canonical Actors protobuf input for the maintained prost/tonic
/// build. The rendered file is an intermediate artifact; these Rust
/// declarations remain the contract authority.
pub fn render_proto_files(root: impl AsRef<std::path::Path>) -> std::io::Result<()> {
    let root = root.as_ref();
    std::fs::create_dir_all(root.join("actors/v1"))?;
    ACTORS_PACKAGE::get_package().render_files(root)
}

#[proto_message]
/// A named capability binding made available to an actor.
pub struct Binding {
    #[proto(tag = 1)]
    /// Name used to refer to this binding.
    pub name: String,
    #[proto(tag = 2)]
    /// Capability supplied by this binding.
    pub capability: String,
    #[proto(tag = 3)]
    /// Resource associated with the capability.
    pub resource: String,
}

#[proto_message]
/// Resource and execution limits associated with an actor.
pub struct ActorLimits {
    #[proto(tag = 1)]
    /// Handler duration, in milliseconds.
    pub handler_timeout_millis: u64,
    #[proto(tag = 2)]
    /// Memory size, in bytes.
    pub memory_bytes: u64,
    #[proto(tag = 3)]
    /// Checkpoint size, in bytes.
    pub checkpoint_bytes: u64,
}

/// Starting position options for a subscription.
pub mod subscription_start {
    use super::*;

    #[proto_oneof]
    #[derive(Copy)]
    /// Position from which a subscription starts.
    pub enum Start {
        #[proto(tag = 1)]
        /// Start at the supplied stream cursor.
        Cursor(u64),
        #[proto(tag = 2)]
        /// Boolean selector for starting at the current stream head.
        CurrentHead(bool),
    }
}

#[proto_message]
/// Starting position used when creating a subscription.
pub struct SubscriptionStart {
    #[proto(oneof(tags(1, 2)))]
    /// Selected subscription start position.
    pub start: Option<subscription_start::Start>,
}

#[proto_message]
/// Configuration for one actor subscription.
pub struct SubscriptionSpec {
    #[proto(tag = 1)]
    /// Subscription identity.
    pub subscription_id: String,
    #[proto(tag = 2)]
    /// Stream path to subscribe to.
    pub stream_path: String,
    #[proto(tag = 3, message)]
    /// Optional position from which delivery starts.
    pub start: Option<SubscriptionStart>,
    #[proto(tag = 4)]
    /// Whether the subscription uses the placement anchor.
    pub placement_anchor: bool,
}

#[proto_enum]
/// Current state of an actor subscription.
pub enum SubscriptionState {
    /// No subscription state was specified.
    Unspecified = 0,
    /// The subscription is active.
    Active = 1,
    /// The subscription is paused.
    Paused = 2,
}

#[proto_message]
/// Observed state and delivery cursors for a subscription.
pub struct SubscriptionObservation {
    #[proto(tag = 1)]
    /// Subscription identity.
    pub subscription_id: String,
    #[proto(tag = 2)]
    /// Stream path being observed.
    pub stream_path: String,
    #[proto(tag = 3, enum_(SubscriptionState))]
    /// Encoded [`SubscriptionState`].
    pub state: i32,
    #[proto(tag = 4)]
    /// Cursor of the latest delivered record.
    pub delivered_cursor: u64,
    #[proto(tag = 5)]
    /// Cursor of the latest completed record.
    pub completed_cursor: u64,
    #[proto(tag = 6)]
    /// Cursor from which delivery can be recovered.
    pub recoverable_cursor: u64,
    #[proto(tag = 7)]
    /// Whether the subscription uses the placement anchor.
    pub placement_anchor: bool,
    #[proto(tag = 8)]
    /// Number of delivery retries.
    pub retry_count: u32,
    #[proto(tag = 9)]
    /// Code describing the latest failure.
    pub failure_code: String,
    #[proto(tag = 10)]
    /// Cursor of the failed record, when one is available.
    pub failed_cursor: Option<u64>,
}

#[proto_enum]
/// Current lifecycle state of an actor.
pub enum ActorState {
    /// No actor state was specified.
    Unspecified = 0,
    /// The actor is active.
    Active = 1,
    /// The actor is hibernated.
    Hibernated = 2,
    /// The actor is paused.
    Paused = 3,
}

#[proto_message]
/// Observed actor state and subscription data.
pub struct ActorObservation {
    #[proto(tag = 1)]
    /// Actor identity.
    pub actor_id: String,
    #[proto(tag = 2, bytes)]
    /// SHA-256 digest of the actor code.
    pub code_sha256: Bytes,
    #[proto(tag = 3)]
    /// Region in which the actor is hosted.
    pub home_region: String,
    #[proto(tag = 4, enum_(ActorState))]
    /// Encoded [`ActorState`].
    pub state: i32,
    #[proto(tag = 5, repeated(message))]
    /// Observed subscriptions belonging to the actor.
    pub subscriptions: Vec<SubscriptionObservation>,
    #[proto(tag = 6)]
    /// Unix timestamp in milliseconds of the latest checkpoint, when known.
    pub checkpoint_unix_millis: Option<u64>,
    #[proto(tag = 7)]
    /// Epoch associated with the latest checkpoint.
    pub checkpoint_epoch: u64,
    #[proto(tag = 8)]
    /// Configuration revision represented by this observation.
    pub configuration_revision: u64,
}

#[proto_message]
/// Request to create an actor.
pub struct CreateActorRequest {
    #[proto(tag = 1, bytes)]
    /// SHA-256 digest of the actor code.
    pub code_sha256: Bytes,
    #[proto(tag = 2)]
    /// Region in which the actor should be hosted.
    pub home_region: String,
    #[proto(tag = 3, repeated(message))]
    /// Capability bindings for the actor.
    pub bindings: Vec<Binding>,
    #[proto(tag = 4, message)]
    /// Resource and execution limits for the actor.
    pub limits: Option<ActorLimits>,
    #[proto(tag = 5, repeated(message))]
    /// Subscriptions to create with the actor.
    pub subscriptions: Vec<SubscriptionSpec>,
    #[proto(tag = 6)]
    /// Key used to make the create operation idempotent.
    pub idempotency_key: String,
}

#[proto_message]
/// Response from actor creation.
pub struct CreateActorResponse {
    #[proto(tag = 1, message)]
    /// Created actor observation, when creation produced one.
    pub actor: Option<ActorObservation>,
}

/// Full configuration replacement with CAS. The new code's checkpoint schema
/// must be compatible or explicitly migrated before activation; failure keeps
/// the previous version active. Paused subscriptions stay paused until resumed.
#[proto_message]
pub struct UpdateActorRequest {
    #[proto(tag = 1)]
    /// Actor identity to update.
    pub actor_id: String,
    #[proto(tag = 2, bytes)]
    /// SHA-256 digest of the replacement actor code.
    pub code_sha256: Bytes,
    #[proto(tag = 3, repeated(message))]
    /// Replacement capability bindings.
    pub bindings: Vec<Binding>,
    #[proto(tag = 4, message)]
    /// Replacement resource and execution limits.
    pub limits: Option<ActorLimits>,
    #[proto(tag = 5)]
    /// Configuration revision that the update expects to replace.
    pub expected_configuration_revision: u64,
    #[proto(tag = 6)]
    /// Key used to make the update operation idempotent.
    pub idempotency_key: String,
}

#[proto_message]
/// Response from an actor update.
pub struct UpdateActorResponse {
    #[proto(tag = 1, message)]
    /// Updated actor observation, when the update produced one.
    pub actor: Option<ActorObservation>,
}

#[proto_message]
/// Request to inspect an actor.
pub struct InspectActorRequest {
    #[proto(tag = 1)]
    /// Actor identity to inspect.
    pub actor_id: String,
}

#[proto_message]
/// Response from actor inspection.
pub struct InspectActorResponse {
    #[proto(tag = 1, message)]
    /// Inspected actor observation, when one was found.
    pub actor: Option<ActorObservation>,
}

#[proto_message]
/// Request to add a subscription to an actor.
pub struct AddSubscriptionRequest {
    #[proto(tag = 1)]
    /// Actor identity receiving the subscription.
    pub actor_id: String,
    #[proto(tag = 2, message)]
    /// Subscription to add.
    pub subscription: Option<SubscriptionSpec>,
    #[proto(tag = 3)]
    /// Key used to make the add operation idempotent.
    pub idempotency_key: String,
}

#[proto_message]
/// Response from adding a subscription.
pub struct AddSubscriptionResponse {
    #[proto(tag = 1, message)]
    /// Actor observation after the subscription change, when one was produced.
    pub actor: Option<ActorObservation>,
}

#[proto_message]
/// Request to remove a subscription from an actor.
pub struct RemoveSubscriptionRequest {
    #[proto(tag = 1)]
    /// Actor identity owning the subscription.
    pub actor_id: String,
    #[proto(tag = 2)]
    /// Subscription identity to remove.
    pub subscription_id: String,
    #[proto(tag = 3)]
    /// Key used to make the remove operation idempotent.
    pub idempotency_key: String,
}

#[proto_message]
/// Response from removing a subscription.
pub struct RemoveSubscriptionResponse {
    #[proto(tag = 1, message)]
    /// Actor observation after the subscription change, when one was produced.
    pub actor: Option<ActorObservation>,
}

/// Resumption may replay a previously delivered record and duplicate external effects.
#[proto_message]
pub struct ResumeSubscriptionRequest {
    #[proto(tag = 1)]
    /// Actor identity owning the subscription.
    pub actor_id: String,
    #[proto(tag = 2)]
    /// Subscription identity to resume.
    pub subscription_id: String,
    #[proto(tag = 3)]
    /// Key used to make the resume operation idempotent.
    pub idempotency_key: String,
}

#[proto_message]
/// Response from resuming a subscription.
pub struct ResumeSubscriptionResponse {
    #[proto(tag = 1, message)]
    /// Actor observation after the subscription change, when one was produced.
    pub actor: Option<ActorObservation>,
}

#[proto_message]
/// Request to checkpoint an actor.
pub struct CheckpointActorRequest {
    #[proto(tag = 1)]
    /// Actor identity to checkpoint.
    pub actor_id: String,
    #[proto(tag = 2)]
    /// Key used to make the checkpoint operation idempotent.
    pub idempotency_key: String,
}

#[proto_message]
/// Response from checkpointing an actor.
pub struct CheckpointActorResponse {
    #[proto(tag = 1, message)]
    /// Actor observation after checkpointing, when one was produced.
    pub actor: Option<ActorObservation>,
}

#[derive(Eq, TS)]
#[proto_message]
/// Name/value metadata sent with an actor invocation.
pub struct Header {
    #[proto(tag = 1)]
    /// Header name.
    pub name: String,
    #[proto(tag = 2)]
    /// Header value.
    pub value: String,
}

/// Invocation is not an implicit Stream append or persistence guarantee.
#[proto_message]
pub struct InvokeActorRequest {
    #[proto(tag = 1)]
    /// Actor identity receiving the invocation.
    pub actor_id: String,
    #[proto(tag = 2)]
    /// Method used for the invocation.
    pub method: String,
    #[proto(tag = 3)]
    /// URL supplied for the invocation.
    pub url: String,
    #[proto(tag = 4, bytes)]
    /// Request body bytes.
    pub body: Bytes,
    #[proto(tag = 5, repeated(message))]
    /// Request headers.
    pub headers: Vec<Header>,
}

#[proto_message]
/// Response returned from an actor invocation.
pub struct InvokeActorResponse {
    #[proto(tag = 1)]
    /// Response status code.
    pub status: u32,
    #[proto(tag = 2, bytes)]
    /// Response body bytes.
    pub body: Bytes,
    #[proto(tag = 3, repeated(message))]
    /// Response headers.
    pub headers: Vec<Header>,
}

#[proto_enum]
/// Error classifications returned by the actor service.
pub enum ErrorCode {
    /// No error classification was specified.
    Unspecified = 0,
    /// One or more request values are invalid.
    InvalidArgument = 1,
    /// The requested capability was denied.
    CapabilityDenied = 2,
    /// The requested capability has expired.
    CapabilityExpired = 3,
    /// The requested actor was not found.
    ActorNotFound = 4,
    /// The requested subscription was not found.
    SubscriptionNotFound = 5,
    /// An idempotency key did not match the existing operation.
    IdempotencyMismatch = 6,
    /// The request conflicts with current actor state.
    Conflict = 7,
    /// The request was denied during admission.
    AdmissionDenied = 8,
    /// The actor checkpoint failed.
    CheckpointFailed = 9,
    /// A dependency required by the operation is unavailable.
    DependencyUnavailable = 10,
}

#[proto_message]
/// Structured error returned by the actor service.
pub struct Error {
    #[proto(tag = 1, enum_(ErrorCode))]
    /// Encoded [`ErrorCode`] classification.
    pub code: i32,
    #[proto(tag = 2)]
    /// Human-readable error message.
    pub message: String,
}

#[proto_service]
/// Operations exposed by the Actors service.
pub enum ActorsService {
    /// Creates an actor.
    CreateActor {
        /// Request payload for actor creation.
        request: CreateActorRequest,
        /// Response payload from actor creation.
        response: CreateActorResponse,
    },
    /// Replaces an actor configuration.
    UpdateActor {
        /// Request payload for the actor update.
        request: UpdateActorRequest,
        /// Response payload from the actor update.
        response: UpdateActorResponse,
    },
    /// Inspects an actor.
    InspectActor {
        /// Request payload for actor inspection.
        request: InspectActorRequest,
        /// Response payload from actor inspection.
        response: InspectActorResponse,
    },
    /// Adds a subscription to an actor.
    AddSubscription {
        /// Request payload for adding a subscription.
        request: AddSubscriptionRequest,
        /// Response payload from adding a subscription.
        response: AddSubscriptionResponse,
    },
    /// Removes a subscription from an actor.
    RemoveSubscription {
        /// Request payload for removing a subscription.
        request: RemoveSubscriptionRequest,
        /// Response payload from removing a subscription.
        response: RemoveSubscriptionResponse,
    },
    /// Resumes a subscription.
    ResumeSubscription {
        /// Request payload for resuming a subscription.
        request: ResumeSubscriptionRequest,
        /// Response payload from resuming a subscription.
        response: ResumeSubscriptionResponse,
    },
    /// Checkpoints an actor.
    CheckpointActor {
        /// Request payload for checkpointing an actor.
        request: CheckpointActorRequest,
        /// Response payload from checkpointing an actor.
        response: CheckpointActorResponse,
    },
    /// Invokes an actor.
    InvokeActor {
        /// Request payload for invoking an actor.
        request: InvokeActorRequest,
        /// Response payload from invoking an actor.
        response: InvokeActorResponse,
    },
}

include!("generated/acyclic.actors.v1.tonic.rs");
