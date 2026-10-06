//! Rust-owned semantic projections for the Actors contract.
//!
//! This module is intentionally small while the generator integration is being
//! wired by the owning generation task. It gives the first complete vertical
//! operation (`InvokeActor`) a typed Rust boundary and derives its static
//! TypeScript declaration from the same Rust definitions. Transport behavior
//! remains in [`crate::grpc`] and [`crate::http`].

use crate::wire;
use ts_rs::TS;

/// A non-empty Actor identity.
#[derive(Clone, Debug, Eq, Hash, PartialEq, TS)]
#[ts(type = "string & { readonly __brand: unique symbol }")]
pub struct ActorId(String);

/// An exact, non-zero SHA-256 digest as used by the existing Actor validators.
#[derive(Clone, Debug, Eq, Hash, PartialEq, TS)]
#[ts(type = "Uint8Array & { readonly __brand: unique symbol; readonly __length: 32 }")]
pub struct CodeSha256([u8; 32]);

/// Failure while constructing a semantic value from customer or wire input.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DomainError {
    /// Actor validators require an actor identity to be present.
    #[error("actor_id must not be empty")]
    EmptyActorId,
    /// Actor validators require exactly 32 bytes with at least one non-zero byte.
    #[error("code_sha256 must contain 32 bytes and must not be all zero")]
    InvalidCodeSha256,
    /// A request failed the canonical wire-level admission validator.
    #[error("Actors request failed canonical contract validation: {0}")]
    Contract(#[from] crate::ContractError),
    /// A response carried an enum value outside the published set.
    #[error("unknown ActorState value {0}")]
    UnknownActorState(i32),
    /// A response carried an enum value outside the published set.
    #[error("unknown SubscriptionState value {0}")]
    UnknownSubscriptionState(i32),
    /// A service error carried an enum value outside the published set.
    #[error("unknown ErrorCode value {0}")]
    UnknownErrorCode(i32),
    /// A response or request omitted a message required by the contract.
    #[error("required Actors message is absent")]
    MissingMessage,
    /// A semantic subscription could not be represented without changing wire data.
    #[error("subscription is malformed")]
    InvalidSubscription,
    /// A semantic binding could not be represented without changing wire data.
    #[error("binding is malformed")]
    InvalidBinding,
}

impl ActorId {
    /// Constructs an Actor identity using the existing `is_empty` contract rule.
    pub fn new(value: String) -> Result<Self, DomainError> {
        if value.is_empty() {
            return Err(DomainError::EmptyActorId);
        }
        Ok(Self(value))
    }

    /// Returns the wire spelling without changing it.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ActorId {
    type Error = DomainError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<&ActorId> for String {
    fn from(value: &ActorId) -> Self {
        value.0.clone()
    }
}

impl CodeSha256 {
    /// Constructs a digest with the exact predicate used by `crate::validate_create`.
    pub fn new(value: Vec<u8>) -> Result<Self, DomainError> {
        if !valid_code_sha256(&value) {
            return Err(DomainError::InvalidCodeSha256);
        }
        let value: [u8; 32] = value
            .try_into()
            .map_err(|_| DomainError::InvalidCodeSha256)?;
        Ok(Self(value))
    }

    /// Returns the exact wire bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    fn from_validated(value: Vec<u8>) -> Result<Self, DomainError> {
        Ok(Self(
            value
                .try_into()
                .map_err(|_| DomainError::InvalidCodeSha256)?,
        ))
    }
}

/// The shared digest predicate used by the existing Actor validators.
pub(crate) fn valid_code_sha256(value: &[u8]) -> bool {
    value.len() == 32 && value.iter().any(|byte| *byte != 0)
}

impl TryFrom<Vec<u8>> for CodeSha256 {
    type Error = DomainError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<&CodeSha256> for Vec<u8> {
    fn from(value: &CodeSha256) -> Self {
        value.0.to_vec()
    }
}

/// The canonical wire header. The wire owner will attach `ts-rs` metadata to
/// this type; keeping an alias here avoids a second Rust header model.
pub type Header = wire::Header;

/// Resource binding admitted by the canonical create/update validators.
#[derive(Clone, Debug, Eq, PartialEq, TS)]
pub struct Binding {
    name: String,
    capability: String,
    resource: String,
}

impl TryFrom<wire::Binding> for Binding {
    type Error = DomainError;

    fn try_from(value: wire::Binding) -> Result<Self, Self::Error> {
        if value.name.is_empty() || value.capability.is_empty() || value.resource.is_empty() {
            return Err(DomainError::InvalidBinding);
        }
        Ok(Self::from_validated(value))
    }
}

impl Binding {
    fn from_validated(value: wire::Binding) -> Self {
        Self {
            name: value.name,
            capability: value.capability,
            resource: value.resource,
        }
    }
}

impl From<Binding> for wire::Binding {
    fn from(value: Binding) -> Self {
        Self {
            name: value.name,
            capability: value.capability,
            resource: value.resource,
        }
    }
}

/// Positive limits admitted by the canonical create/update validators.
#[derive(Clone, Copy, Debug, Eq, PartialEq, TS)]
pub struct ActorLimits {
    #[ts(type = "bigint")]
    handler_timeout_millis: u64,
    #[ts(type = "bigint")]
    memory_bytes: u64,
    #[ts(type = "bigint")]
    checkpoint_bytes: u64,
}

impl TryFrom<wire::ActorLimits> for ActorLimits {
    type Error = DomainError;

    fn try_from(value: wire::ActorLimits) -> Result<Self, Self::Error> {
        if value.handler_timeout_millis == 0
            || value.memory_bytes == 0
            || value.checkpoint_bytes == 0
        {
            return Err(DomainError::Contract(crate::ContractError::InvalidArgument));
        }
        Ok(Self::from_validated(value))
    }
}

impl ActorLimits {
    fn from_validated(value: wire::ActorLimits) -> Self {
        Self {
            handler_timeout_millis: value.handler_timeout_millis,
            memory_bytes: value.memory_bytes,
            checkpoint_bytes: value.checkpoint_bytes,
        }
    }
}

impl From<ActorLimits> for wire::ActorLimits {
    fn from(value: ActorLimits) -> Self {
        Self {
            handler_timeout_millis: value.handler_timeout_millis,
            memory_bytes: value.memory_bytes,
            checkpoint_bytes: value.checkpoint_bytes,
        }
    }
}

/// The published oneof is retained exactly, including cursor zero and the
/// boolean payload of `CurrentHead`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, TS)]
pub enum SubscriptionStart {
    /// Start at the exact u64 cursor, including cursor zero.
    Cursor {
        #[ts(type = "bigint")]
        cursor: u64,
    },
    /// Start at the service's current head, preserving the wire boolean.
    CurrentHead { current_head: bool },
}

impl TryFrom<wire::SubscriptionStart> for SubscriptionStart {
    type Error = DomainError;

    fn try_from(value: wire::SubscriptionStart) -> Result<Self, Self::Error> {
        match value.start {
            Some(wire::subscription_start::Start::Cursor(cursor)) => Ok(Self::Cursor { cursor }),
            Some(wire::subscription_start::Start::CurrentHead(current_head)) => {
                Ok(Self::CurrentHead { current_head })
            }
            None => Err(DomainError::InvalidSubscription),
        }
    }
}

impl From<SubscriptionStart> for wire::SubscriptionStart {
    fn from(value: SubscriptionStart) -> Self {
        Self {
            start: Some(match value {
                SubscriptionStart::Cursor { cursor } => {
                    wire::subscription_start::Start::Cursor(cursor)
                }
                SubscriptionStart::CurrentHead { current_head } => {
                    wire::subscription_start::Start::CurrentHead(current_head)
                }
            }),
        }
    }
}

/// A subscription admitted by `validate_create` or `validate_add_subscription`.
#[derive(Clone, Debug, Eq, PartialEq, TS)]
pub struct SubscriptionSpec {
    subscription_id: String,
    stream_path: String,
    start: SubscriptionStart,
    placement_anchor: bool,
}

impl TryFrom<wire::SubscriptionSpec> for SubscriptionSpec {
    type Error = DomainError;

    fn try_from(value: wire::SubscriptionSpec) -> Result<Self, Self::Error> {
        if value.subscription_id.is_empty() || value.stream_path.is_empty() {
            return Err(DomainError::InvalidSubscription);
        }
        let start_wire = value
            .start
            .as_ref()
            .ok_or(DomainError::InvalidSubscription)?;
        if !matches!(
            start_wire.start.as_ref(),
            Some(wire::subscription_start::Start::Cursor(_))
                | Some(wire::subscription_start::Start::CurrentHead(true))
        ) {
            return Err(DomainError::InvalidSubscription);
        }
        let start = start_wire.clone().try_into()?;
        Ok(Self::from_validated(value, start))
    }
}

impl SubscriptionSpec {
    fn from_validated_wire(value: wire::SubscriptionSpec) -> Result<Self, DomainError> {
        let start = match value.start.as_ref().and_then(|start| start.start.as_ref()) {
            Some(wire::subscription_start::Start::Cursor(cursor)) => {
                SubscriptionStart::Cursor { cursor: *cursor }
            }
            Some(wire::subscription_start::Start::CurrentHead(current_head)) => {
                SubscriptionStart::CurrentHead {
                    current_head: *current_head,
                }
            }
            None => return Err(DomainError::InvalidSubscription),
        };
        Ok(Self::from_validated(value, start))
    }

    fn from_validated(value: wire::SubscriptionSpec, start: SubscriptionStart) -> Self {
        Self {
            subscription_id: value.subscription_id,
            stream_path: value.stream_path,
            start,
            placement_anchor: value.placement_anchor,
        }
    }
}

impl From<SubscriptionSpec> for wire::SubscriptionSpec {
    fn from(value: SubscriptionSpec) -> Self {
        Self {
            subscription_id: value.subscription_id,
            stream_path: value.stream_path,
            start: Some(value.start.into()),
            placement_anchor: value.placement_anchor,
        }
    }
}

/// Known subscription states. Unknown protobuf integers are rejected rather
/// than normalized to `Unspecified`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, TS)]
pub enum SubscriptionState {
    /// No subscription state was specified by the service.
    Unspecified,
    /// The subscription is active.
    Active,
    /// The subscription is paused.
    Paused,
}

impl TryFrom<i32> for SubscriptionState {
    type Error = DomainError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Unspecified),
            1 => Ok(Self::Active),
            2 => Ok(Self::Paused),
            other => Err(DomainError::UnknownSubscriptionState(other)),
        }
    }
}

/// Known Actor states. Unknown protobuf integers remain observable errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq, TS)]
pub enum ActorState {
    /// No Actor state was specified by the service.
    Unspecified,
    /// The Actor is active.
    Active,
    /// The Actor is hibernated.
    Hibernated,
    /// The Actor is paused.
    Paused,
}

/// Published service error codes. Unknown numeric values stay visible through
/// `DomainError::UnknownErrorCode` instead of being coerced to `Unspecified`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, TS)]
pub enum ErrorCode {
    /// No service error code was specified.
    Unspecified,
    /// The request was invalid.
    InvalidArgument,
    /// The requested capability was denied.
    CapabilityDenied,
    /// The requested capability expired.
    CapabilityExpired,
    /// The Actor could not be found.
    ActorNotFound,
    /// The subscription could not be found.
    SubscriptionNotFound,
    /// The idempotency key did not match the original request.
    IdempotencyMismatch,
    /// The request conflicted with current state.
    Conflict,
    /// Admission was denied.
    AdmissionDenied,
    /// Checkpoint creation failed.
    CheckpointFailed,
    /// A required dependency was unavailable.
    DependencyUnavailable,
}

impl TryFrom<i32> for ErrorCode {
    type Error = DomainError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Unspecified),
            1 => Ok(Self::InvalidArgument),
            2 => Ok(Self::CapabilityDenied),
            3 => Ok(Self::CapabilityExpired),
            4 => Ok(Self::ActorNotFound),
            5 => Ok(Self::SubscriptionNotFound),
            6 => Ok(Self::IdempotencyMismatch),
            7 => Ok(Self::Conflict),
            8 => Ok(Self::AdmissionDenied),
            9 => Ok(Self::CheckpointFailed),
            10 => Ok(Self::DependencyUnavailable),
            other => Err(DomainError::UnknownErrorCode(other)),
        }
    }
}

/// A service error with a typed known code and lossless message.
#[derive(Clone, Debug, Eq, PartialEq, TS)]
pub struct ServiceError {
    code: ErrorCode,
    message: String,
}

impl TryFrom<wire::Error> for ServiceError {
    type Error = DomainError;

    fn try_from(value: wire::Error) -> Result<Self, Self::Error> {
        Ok(Self {
            code: value.code.try_into()?,
            message: value.message,
        })
    }
}

impl TryFrom<i32> for ActorState {
    type Error = DomainError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Unspecified),
            1 => Ok(Self::Active),
            2 => Ok(Self::Hibernated),
            3 => Ok(Self::Paused),
            other => Err(DomainError::UnknownActorState(other)),
        }
    }
}

/// Lossless semantic view of a subscription observation.
#[derive(Clone, Debug, Eq, PartialEq, TS)]
pub struct SubscriptionObservation {
    subscription_id: String,
    stream_path: String,
    state: SubscriptionState,
    #[ts(type = "bigint")]
    delivered_cursor: u64,
    #[ts(type = "bigint")]
    completed_cursor: u64,
    #[ts(type = "bigint")]
    recoverable_cursor: u64,
    placement_anchor: bool,
    retry_count: u32,
    failure_code: String,
    #[ts(type = "bigint | undefined")]
    failed_cursor: Option<u64>,
}

impl TryFrom<wire::SubscriptionObservation> for SubscriptionObservation {
    type Error = DomainError;

    fn try_from(value: wire::SubscriptionObservation) -> Result<Self, Self::Error> {
        Ok(Self {
            subscription_id: value.subscription_id,
            stream_path: value.stream_path,
            state: value.state.try_into()?,
            delivered_cursor: value.delivered_cursor,
            completed_cursor: value.completed_cursor,
            recoverable_cursor: value.recoverable_cursor,
            placement_anchor: value.placement_anchor,
            retry_count: value.retry_count,
            failure_code: value.failure_code,
            failed_cursor: value.failed_cursor,
        })
    }
}

/// Lossless semantic view of a server Actor observation.
#[derive(Clone, Debug, Eq, PartialEq, TS)]
pub struct ActorObservation {
    actor_id: ActorId,
    code_sha256: CodeSha256,
    home_region: String,
    state: ActorState,
    subscriptions: Vec<SubscriptionObservation>,
    #[ts(type = "bigint | undefined")]
    checkpoint_unix_millis: Option<u64>,
    #[ts(type = "bigint")]
    checkpoint_epoch: u64,
    #[ts(type = "bigint")]
    configuration_revision: u64,
}

impl TryFrom<wire::ActorObservation> for ActorObservation {
    type Error = DomainError;

    fn try_from(value: wire::ActorObservation) -> Result<Self, Self::Error> {
        Ok(Self {
            actor_id: value.actor_id.try_into()?,
            code_sha256: value.code_sha256.to_vec().try_into()?,
            home_region: value.home_region,
            state: value.state.try_into()?,
            subscriptions: value
                .subscriptions
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            checkpoint_unix_millis: value.checkpoint_unix_millis,
            checkpoint_epoch: value.checkpoint_epoch,
            configuration_revision: value.configuration_revision,
        })
    }
}

fn actor_response(
    value: Option<wire::ActorObservation>,
) -> Result<Option<ActorObservation>, DomainError> {
    value.map(TryInto::try_into).transpose()
}

macro_rules! actor_response_type {
    ($name:ident, $wire:ident) => {
        #[derive(Clone, Debug, Eq, PartialEq, TS)]
        #[doc = "Typed response preserving the optional server Actor observation."]
        pub struct $name {
            actor: Option<ActorObservation>,
        }

        impl TryFrom<wire::$wire> for $name {
            type Error = DomainError;

            fn try_from(value: wire::$wire) -> Result<Self, Self::Error> {
                Ok(Self {
                    actor: actor_response(value.actor)?,
                })
            }
        }
    };
}

actor_response_type!(CreateActorResponse, CreateActorResponse);
actor_response_type!(UpdateActorResponse, UpdateActorResponse);
actor_response_type!(InspectActorResponse, InspectActorResponse);
actor_response_type!(AddSubscriptionResponse, AddSubscriptionResponse);
actor_response_type!(RemoveSubscriptionResponse, RemoveSubscriptionResponse);
actor_response_type!(ResumeSubscriptionResponse, ResumeSubscriptionResponse);
actor_response_type!(CheckpointActorResponse, CheckpointActorResponse);

/// Create request after the canonical admission validator has run.
#[derive(Clone, Debug, Eq, PartialEq, TS)]
pub struct CreateActorRequest {
    code_sha256: CodeSha256,
    home_region: String,
    bindings: Vec<Binding>,
    limits: ActorLimits,
    subscriptions: Vec<SubscriptionSpec>,
    idempotency_key: String,
}

impl TryFrom<wire::CreateActorRequest> for CreateActorRequest {
    type Error = DomainError;

    fn try_from(value: wire::CreateActorRequest) -> Result<Self, Self::Error> {
        crate::validate_create(&value)?;
        Ok(Self {
            code_sha256: CodeSha256::from_validated(value.code_sha256.to_vec())?,
            home_region: value.home_region,
            bindings: value
                .bindings
                .into_iter()
                .map(Binding::from_validated)
                .collect(),
            limits: ActorLimits::from_validated(value.limits.ok_or(DomainError::MissingMessage)?),
            subscriptions: value
                .subscriptions
                .into_iter()
                .map(SubscriptionSpec::from_validated_wire)
                .collect::<Result<_, _>>()?,
            idempotency_key: value.idempotency_key,
        })
    }
}

impl From<CreateActorRequest> for wire::CreateActorRequest {
    fn from(value: CreateActorRequest) -> Self {
        Self {
            code_sha256: value.code_sha256.as_bytes().to_vec().into(),
            home_region: value.home_region,
            bindings: value.bindings.into_iter().map(Into::into).collect(),
            limits: Some(value.limits.into()),
            subscriptions: value.subscriptions.into_iter().map(Into::into).collect(),
            idempotency_key: value.idempotency_key,
        }
    }
}

/// Update request after the canonical admission validator has run.
#[derive(Clone, Debug, Eq, PartialEq, TS)]
pub struct UpdateActorRequest {
    actor_id: ActorId,
    code_sha256: CodeSha256,
    bindings: Vec<Binding>,
    limits: ActorLimits,
    #[ts(type = "bigint")]
    expected_configuration_revision: u64,
    idempotency_key: String,
}

impl TryFrom<wire::UpdateActorRequest> for UpdateActorRequest {
    type Error = DomainError;

    fn try_from(value: wire::UpdateActorRequest) -> Result<Self, Self::Error> {
        crate::validate_update(&value)?;
        Ok(Self {
            actor_id: value.actor_id.try_into()?,
            code_sha256: CodeSha256::from_validated(value.code_sha256.to_vec())?,
            bindings: value
                .bindings
                .into_iter()
                .map(Binding::from_validated)
                .collect(),
            limits: ActorLimits::from_validated(value.limits.ok_or(DomainError::MissingMessage)?),
            expected_configuration_revision: value.expected_configuration_revision,
            idempotency_key: value.idempotency_key,
        })
    }
}

impl From<UpdateActorRequest> for wire::UpdateActorRequest {
    fn from(value: UpdateActorRequest) -> Self {
        Self {
            actor_id: value.actor_id.0,
            code_sha256: value.code_sha256.as_bytes().to_vec().into(),
            bindings: value.bindings.into_iter().map(Into::into).collect(),
            limits: Some(value.limits.into()),
            expected_configuration_revision: value.expected_configuration_revision,
            idempotency_key: value.idempotency_key,
        }
    }
}

/// Inspect currently has no additional wire validator; only the semantic
/// Actor identity rule is applied by this conversion.
#[derive(Clone, Debug, Eq, PartialEq, TS)]
pub struct InspectActorRequest {
    actor_id: ActorId,
}

impl TryFrom<wire::InspectActorRequest> for InspectActorRequest {
    type Error = DomainError;

    fn try_from(value: wire::InspectActorRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            actor_id: value.actor_id.try_into()?,
        })
    }
}

impl From<InspectActorRequest> for wire::InspectActorRequest {
    fn from(value: InspectActorRequest) -> Self {
        Self {
            actor_id: value.actor_id.0,
        }
    }
}

/// Add subscription request after the canonical admission validator has run.
#[derive(Clone, Debug, Eq, PartialEq, TS)]
pub struct AddSubscriptionRequest {
    actor_id: ActorId,
    subscription: SubscriptionSpec,
    idempotency_key: String,
}

impl TryFrom<wire::AddSubscriptionRequest> for AddSubscriptionRequest {
    type Error = DomainError;

    fn try_from(value: wire::AddSubscriptionRequest) -> Result<Self, Self::Error> {
        crate::validate_add_subscription(&value)?;
        Ok(Self {
            actor_id: value.actor_id.try_into()?,
            subscription: SubscriptionSpec::from_validated_wire(
                value.subscription.ok_or(DomainError::MissingMessage)?,
            )?,
            idempotency_key: value.idempotency_key,
        })
    }
}

impl From<AddSubscriptionRequest> for wire::AddSubscriptionRequest {
    fn from(value: AddSubscriptionRequest) -> Self {
        Self {
            actor_id: value.actor_id.0,
            subscription: Some(value.subscription.into()),
            idempotency_key: value.idempotency_key,
        }
    }
}

/// Removal has no canonical validator yet, so subscription and idempotency
/// strings remain unbranded and are carried exactly as received.
#[derive(Clone, Debug, Eq, PartialEq, TS)]
pub struct RemoveSubscriptionRequest {
    actor_id: ActorId,
    subscription_id: String,
    idempotency_key: String,
}

impl TryFrom<wire::RemoveSubscriptionRequest> for RemoveSubscriptionRequest {
    type Error = DomainError;

    fn try_from(value: wire::RemoveSubscriptionRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            actor_id: value.actor_id.try_into()?,
            subscription_id: value.subscription_id,
            idempotency_key: value.idempotency_key,
        })
    }
}

impl From<RemoveSubscriptionRequest> for wire::RemoveSubscriptionRequest {
    fn from(value: RemoveSubscriptionRequest) -> Self {
        Self {
            actor_id: value.actor_id.0,
            subscription_id: value.subscription_id,
            idempotency_key: value.idempotency_key,
        }
    }
}

/// Resume has no canonical validator yet; the wire strings stay unbranded.
#[derive(Clone, Debug, Eq, PartialEq, TS)]
pub struct ResumeSubscriptionRequest {
    actor_id: ActorId,
    subscription_id: String,
    idempotency_key: String,
}

impl TryFrom<wire::ResumeSubscriptionRequest> for ResumeSubscriptionRequest {
    type Error = DomainError;

    fn try_from(value: wire::ResumeSubscriptionRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            actor_id: value.actor_id.try_into()?,
            subscription_id: value.subscription_id,
            idempotency_key: value.idempotency_key,
        })
    }
}

impl From<ResumeSubscriptionRequest> for wire::ResumeSubscriptionRequest {
    fn from(value: ResumeSubscriptionRequest) -> Self {
        Self {
            actor_id: value.actor_id.0,
            subscription_id: value.subscription_id,
            idempotency_key: value.idempotency_key,
        }
    }
}

/// Checkpoint has no canonical validator yet; only the Actor identity rule is
/// applied and the idempotency key remains an ordinary string.
#[derive(Clone, Debug, Eq, PartialEq, TS)]
pub struct CheckpointActorRequest {
    actor_id: ActorId,
    idempotency_key: String,
}

impl TryFrom<wire::CheckpointActorRequest> for CheckpointActorRequest {
    type Error = DomainError;

    fn try_from(value: wire::CheckpointActorRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            actor_id: value.actor_id.try_into()?,
            idempotency_key: value.idempotency_key,
        })
    }
}

impl From<CheckpointActorRequest> for wire::CheckpointActorRequest {
    fn from(value: CheckpointActorRequest) -> Self {
        Self {
            actor_id: value.actor_id.0,
            idempotency_key: value.idempotency_key,
        }
    }
}

/// Typed invocation request. Method, URL, headers, and body preserve the
/// existing wire contract without adding new validation rules.
#[derive(Clone, Debug, PartialEq, TS)]
pub struct InvokeActorRequest {
    actor_id: ActorId,
    method: String,
    url: String,
    #[ts(type = "Uint8Array")]
    body: Vec<u8>,
    #[ts(type = "Array<{ name: string; value: string }>")]
    headers: Vec<Header>,
}

impl TryFrom<wire::InvokeActorRequest> for InvokeActorRequest {
    type Error = DomainError;

    fn try_from(value: wire::InvokeActorRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            actor_id: value.actor_id.try_into()?,
            method: value.method,
            url: value.url,
            body: value.body.to_vec(),
            headers: value.headers,
        })
    }
}

impl From<InvokeActorRequest> for wire::InvokeActorRequest {
    fn from(value: InvokeActorRequest) -> Self {
        Self {
            actor_id: value.actor_id.0,
            method: value.method,
            url: value.url,
            body: value.body.into(),
            headers: value.headers,
        }
    }
}

/// Typed invocation response with byte-preserving body and headers.
#[derive(Clone, Debug, PartialEq, TS)]
pub struct InvokeActorResponse {
    status: u32,
    #[ts(type = "Uint8Array")]
    body: Vec<u8>,
    #[ts(type = "Array<{ name: string; value: string }>")]
    headers: Vec<Header>,
}

impl From<wire::InvokeActorResponse> for InvokeActorResponse {
    fn from(value: wire::InvokeActorResponse) -> Self {
        Self {
            status: value.status,
            body: value.body.to_vec(),
            headers: value.headers,
        }
    }
}

impl From<InvokeActorResponse> for wire::InvokeActorResponse {
    fn from(value: InvokeActorResponse) -> Self {
        Self {
            status: value.status,
            body: value.body.into(),
            headers: value.headers,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actor_id_matches_existing_presence_rule() {
        assert_eq!(ActorId::new(String::new()), Err(DomainError::EmptyActorId));
        assert_eq!(
            ActorId::new("  ".into()).map(|value| value.as_str().to_owned()),
            Ok("  ")
        );
    }

    #[test]
    fn digest_matches_existing_create_predicate() {
        assert_eq!(
            CodeSha256::new(vec![0; 31]),
            Err(DomainError::InvalidCodeSha256)
        );
        assert_eq!(
            CodeSha256::new(vec![0; 32]),
            Err(DomainError::InvalidCodeSha256)
        );
        assert!(CodeSha256::new(vec![1; 32]).is_ok());
    }

    #[test]
    fn invoke_round_trips_without_rewriting_wire_fields() {
        let wire = wire::InvokeActorRequest {
            actor_id: "actor-1".into(),
            method: String::new(),
            url: String::new(),
            body: vec![0, 1, 2],
            headers: vec![wire::Header {
                name: "x-test".into(),
                value: "ok".into(),
            }],
        };
        let typed = InvokeActorRequest::try_from(wire.clone()).ok();
        assert!(typed.is_some());
        let Some(typed) = typed else {
            return;
        };
        let round_trip: wire::InvokeActorRequest = typed.into();
        assert_eq!(round_trip, wire);
    }

    #[test]
    fn invoke_rejects_only_the_existing_actor_id_presence_violation() {
        let wire = wire::InvokeActorRequest {
            actor_id: String::new(),
            method: String::new(),
            url: String::new(),
            body: Vec::new(),
            headers: Vec::new(),
        };
        assert_eq!(
            InvokeActorRequest::try_from(wire),
            Err(DomainError::EmptyActorId)
        );
    }

    #[test]
    fn response_enum_unknown_values_are_not_normalized() {
        let observation = wire::ActorObservation {
            actor_id: "actor-1".into(),
            code_sha256: vec![1; 32],
            home_region: "eu".into(),
            state: 99,
            subscriptions: Vec::new(),
            checkpoint_unix_millis: None,
            checkpoint_epoch: 0,
            configuration_revision: 0,
        };
        assert_eq!(
            ActorObservation::try_from(observation),
            Err(DomainError::UnknownActorState(99))
        );
        assert_eq!(
            SubscriptionState::try_from(99),
            Err(DomainError::UnknownSubscriptionState(99))
        );
        assert_eq!(
            ErrorCode::try_from(99),
            Err(DomainError::UnknownErrorCode(99))
        );
    }

    #[test]
    fn response_digest_keeps_the_nonzero_admission_rule() {
        let observation = wire::ActorObservation {
            actor_id: "actor-1".into(),
            code_sha256: vec![0; 32],
            home_region: "eu".into(),
            state: 1,
            subscriptions: Vec::new(),
            checkpoint_unix_millis: None,
            checkpoint_epoch: 0,
            configuration_revision: 0,
        };
        assert_eq!(
            ActorObservation::try_from(observation),
            Err(DomainError::InvalidCodeSha256)
        );
    }

    #[test]
    fn subscription_projection_keeps_canonical_current_head_rule() {
        let invalid = wire::SubscriptionSpec {
            subscription_id: "events".into(),
            stream_path: "agents/a/events".into(),
            start: Some(wire::SubscriptionStart {
                start: Some(wire::subscription_start::Start::CurrentHead(false)),
            }),
            placement_anchor: false,
        };
        assert_eq!(
            SubscriptionSpec::try_from(invalid),
            Err(DomainError::InvalidSubscription)
        );
    }
}
