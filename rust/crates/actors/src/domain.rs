// Rust-owned semantic projections for the Actors contract.
//
// This module provides typed semantic boundaries for all eight Actors
// operations (create, update, inspect, add/remove/resume subscription,
// checkpoint, and invoke), preserving the canonical wire validators and
// lossless enum/presence rules. Transport behavior remains in
// [`crate::grpc`] and [`crate::http`].

use std::{num::NonZeroU64, path::Path};

use crate::wire;
use crate::contract::ActorsFile;

use ts_rs::{Config, ExportError, TS, TypeVisitor};

#[cfg(kani)]
#[path = "domain/formal_proofs.rs"]
mod formal_proofs;

/// A non-empty Actor identity.
#[derive(Clone, Debug, Eq, Hash, PartialEq, TS)]
#[ts(export_to = "actors/ActorId.ts")]
#[ts(type = "string & { readonly __brand: unique symbol }")]
pub struct ActorId(String);


/// An exact, non-zero SHA-256 digest as used by the existing Actor validators.
#[derive(Clone, Debug, Eq, Hash, PartialEq, TS)]
#[ts(export_to = "actors/CodeSha256.ts")]
#[ts(type = "import(\"../../readonly.js\").ReadonlyBytes & { readonly __brand: unique symbol; readonly __length: 32 }")]
pub struct CodeSha256([u8; 32]);


/// A strictly positive unsigned 64-bit value.
///
/// The wire contract remains a raw `u64`; this nominal type is used only at
/// semantic boundaries where zero is not admitted.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, TS)]
#[ts(export_to = "actors/PositiveU64.ts")]
#[ts(type = "bigint & { readonly __brand: unique symbol }")]
pub struct PositiveU64(NonZeroU64);


/// Failure while constructing a semantic value from customer or wire input.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, thiserror::Error)]
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
    #[default]
    #[error("required Actors message is absent")]
    MissingMessage,
    /// A semantic subscription could not be represented without changing wire data.
    #[error("subscription is malformed")]
    InvalidSubscription,
    /// A semantic binding could not be represented without changing wire data.
    #[error("binding is malformed")]
    InvalidBinding,
}

impl DomainError {
    /// Returns the stable error code shared by every SDK bridge.
    #[must_use]
    pub const fn code_name(&self) -> &'static str {
        match self {
            Self::EmptyActorId => "empty_actor_id",
            Self::InvalidCodeSha256 => "invalid_code_sha256",
            Self::Contract(error) => error.code_name(),
            Self::UnknownActorState(_) => "unknown_actor_state",
            Self::UnknownSubscriptionState(_) => "unknown_subscription_state",
            Self::UnknownErrorCode(_) => "unknown_error_code",
            Self::MissingMessage => "missing_message",
            Self::InvalidSubscription => "invalid_subscription",
            Self::InvalidBinding => "invalid_binding",
        }
    }
}

fn parse_subscription_state(value: i32) -> Result<SubscriptionState, DomainError> {
    SubscriptionState::try_from(value)
}

fn encode_subscription_state(value: SubscriptionState) -> i32 {
    value as i32
}

fn parse_actor_state(value: i32) -> Result<ActorState, DomainError> {
    ActorState::try_from(value)
}

fn encode_actor_state(value: ActorState) -> i32 {
    value as i32
}

fn parse_error_code(value: i32) -> Result<ErrorCode, DomainError> {
    ErrorCode::try_from(value)
}

fn encode_error_code(value: ErrorCode) -> i32 {
    value as i32
}

impl From<std::convert::Infallible> for DomainError {
    fn from(value: std::convert::Infallible) -> Self { match value {} }
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

impl From<ActorId> for String {
    fn from(value: ActorId) -> Self {
        value.0
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

}

impl PositiveU64 {
    /// Constructs a positive value using the canonical semantic predicate.
    pub fn new(value: u64) -> Result<Self, DomainError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(DomainError::Contract(crate::ContractError::InvalidArgument))
    }

    /// Returns the exact unsigned wire value.
    #[must_use]
    pub fn get(self) -> u64 { self.0.get() }
}

impl TryFrom<u64> for PositiveU64 {
    type Error = DomainError;

    fn try_from(value: u64) -> Result<Self, Self::Error> { Self::new(value) }
}

impl From<PositiveU64> for u64 {
    fn from(value: PositiveU64) -> Self { value.get() }
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



impl From<CodeSha256> for Vec<u8> {
    fn from(value: CodeSha256) -> Self {
        value.0.to_vec()
    }
}

/// A lossless invocation header.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]

#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/Header.ts")]
#[ts(rename_all = "camelCase")]
pub struct Header {
    #[wire(tag = 1)]
    name: String,
    #[wire(tag = 2)]
    value: String,
}

impl Header {
    /// Creates a header without changing its wire spelling.
    #[must_use]
    pub fn new(name: String, value: String) -> Self {
        Self { name, value }
    }

    /// Returns the header name.
    #[must_use]
    pub fn name(&self) -> &str { &self.name }

    /// Returns the header value.
    #[must_use]
    pub fn value(&self) -> &str { &self.value }
}

/// Resource binding admitted by the canonical create/update validators.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = Binding::validate_from_proto)]

#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/Binding.ts")]
#[ts(rename_all = "camelCase")]
pub struct Binding {
    #[wire(tag = 1)]

    name: String,
    #[wire(tag = 2)]
    capability: String,
    #[wire(tag = 3)]
    resource: String,
}



impl Binding {
    /// Constructs a binding while applying the same non-empty admission rule
    /// used by the canonical create and update validators.
    pub fn new(name: String, capability: String, resource: String) -> Result<Self, DomainError> {
        let binding = Self {
            name,
            capability,
            resource,
        };
        binding.validate_admission()?;
        Ok(binding)
    }

    fn validate_admission(&self) -> Result<(), DomainError> {
        if self.name.is_empty() || self.capability.is_empty() || self.resource.is_empty() {
            return Err(DomainError::InvalidBinding);
        }
        Ok(())
    }

    /// Re-checks a value admitted from an untrusted protobuf wire message
    /// through the canonical constructor predicate.
    fn validate_from_proto(&self) -> Result<(), DomainError> {
        self.validate_admission()
    }

    /// Returns the binding name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the capability name.
    #[must_use]
    pub fn capability(&self) -> &str {
        &self.capability
    }

    /// Returns the bound resource name.
    #[must_use]
    pub fn resource(&self) -> &str {
        &self.resource
    }

}



/// Positive limits admitted by the canonical create/update validators.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]

#[derive(Clone, Copy, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/ActorLimits.ts")]
#[ts(rename_all = "camelCase")]
pub struct ActorLimits {
    #[wire(tag = 1, uint64)]

    handler_timeout_millis: PositiveU64,
    #[wire(tag = 2, uint64)]
    memory_bytes: PositiveU64,
    #[wire(tag = 3, uint64)]
    checkpoint_bytes: PositiveU64,
}



impl ActorLimits {
    /// Constructs positive limits using the canonical admission predicate.
    pub fn new(
        handler_timeout_millis: u64,
        memory_bytes: u64,
        checkpoint_bytes: u64,
    ) -> Result<Self, DomainError> {
        Ok(Self {
            handler_timeout_millis: handler_timeout_millis.try_into()?,
            memory_bytes: memory_bytes.try_into()?,
            checkpoint_bytes: checkpoint_bytes.try_into()?,
        })
    }

    /// Returns the handler timeout in milliseconds.
    #[must_use]
    pub fn handler_timeout_millis(&self) -> u64 {
        self.handler_timeout_millis.get()
    }

    /// Returns the memory limit in bytes.
    #[must_use]
    pub fn memory_bytes(&self) -> u64 {
        self.memory_bytes.get()
    }

    /// Returns the checkpoint limit in bytes.
    #[must_use]
    pub fn checkpoint_bytes(&self) -> u64 {
        self.checkpoint_bytes.get()
    }

}
/// The published subscription-start message owns its oneof declaration.
/// Contract derivation generates the wire shadow and fallible ingress from this semantic
/// declaration, preserving cursor zero and the current-head boolean payload.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]

#[derive(Clone, Copy, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/SubscriptionStart.ts")]
pub struct SubscriptionStart {
    #[wire(oneof = "1,2", from = parse_subscription_start)]
    #[ts(inline)]
    start: subscription_start::Start,
}

fn parse_subscription_start(
    value: Option<subscription_start::StartProto>,
) -> Result<subscription_start::Start, DomainError> {
    match value {
        Some(subscription_start::StartProto::Cursor(cursor)) => {
            Ok(subscription_start::Start::Cursor(cursor))
        }
        Some(subscription_start::StartProto::CurrentHead(true)) => {
            Ok(subscription_start::Start::CurrentHead(
                subscription_start::CurrentHeadMarker::new(),
            ))
        }
        Some(subscription_start::StartProto::CurrentHead(false)) | None => {
            Err(DomainError::InvalidSubscription)
        }
    }
}

pub mod subscription_start {
    use super::*;

    /// The only valid semantic payload for the current-head selector. The
    /// protobuf wire representation remains a bool for descriptor stability,
    /// while this type makes `false` unrepresentable after ingress validation.
    #[derive(Clone, Copy, Debug, Eq, PartialEq, TS)]
    #[ts(type = "true", export_to = "actors/CurrentHeadMarker.ts")]
    pub struct CurrentHeadMarker;

    impl Default for CurrentHeadMarker {
        fn default() -> Self {
            Self::new()
        }
    }

    impl CurrentHeadMarker {
        #[must_use]
        pub const fn new() -> Self {
            Self
        }
    }

    impl TryFrom<bool> for CurrentHeadMarker {
        type Error = DomainError;

        fn try_from(value: bool) -> Result<Self, Self::Error> {
            value.then_some(Self).ok_or(DomainError::InvalidSubscription)
        }
    }

    impl From<CurrentHeadMarker> for bool {
        fn from(_: CurrentHeadMarker) -> Self {
            true
        }
    }


    /// The semantic oneof for [`super::SubscriptionStart`].
    #[acyclic_contract_derive::oneof(error = DomainError, file = ActorsFile)]
    #[derive(Clone, Copy, Debug, Eq, PartialEq, TS)]
    #[ts(tag = "case", content = "value", rename_all = "camelCase")]
    pub enum Start {
        /// Start at the exact stream cursor, including cursor zero.
        #[wire(tag = 1)]
        Cursor(u64),
        /// Start at the service's current head, preserving the wire boolean.
        #[wire(tag = 2, bool)]
        CurrentHead(CurrentHeadMarker),
    }
}

impl SubscriptionStart {
    /// Creates a cursor-based start selector.
    #[must_use]
    pub fn cursor(cursor: u64) -> Self {
        Self {
            start: subscription_start::Start::Cursor(cursor),
        }
    }

    /// Creates a current-head start selector.
    #[must_use]
    pub fn current_head() -> Self {
        Self {
            start: subscription_start::Start::CurrentHead(
                subscription_start::CurrentHeadMarker::new(),
            ),
        }
    }

    /// Returns the cursor payload when this start selects an explicit cursor.
    #[must_use]
    pub fn cursor_value(&self) -> Option<u64> {
        match self.start {
            subscription_start::Start::Cursor(cursor) => Some(cursor),
            subscription_start::Start::CurrentHead(_) => None,
        }
    }

    /// Returns the current-head payload when this start selects current head.
    #[must_use]
    pub fn current_head_value(&self) -> Option<bool> {
        match self.start {
            subscription_start::Start::Cursor(_) => None,
            subscription_start::Start::CurrentHead(_) => Some(true),
        }
    }
}





/// A subscription admitted by `validate_create` or `validate_add_subscription`.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = SubscriptionSpec::validate_from_proto)]

#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/SubscriptionSpec.ts")]
#[ts(rename_all = "camelCase")]
pub struct SubscriptionSpec {
    #[wire(tag = 1)]

    subscription_id: String,
    #[wire(tag = 2)]
    stream_path: String,
    #[wire(tag = 3, message)]
    start: SubscriptionStart,
    #[wire(tag = 4)]
    placement_anchor: bool,
}



impl SubscriptionSpec {
    /// Constructs a subscription using the same start and presence rules as
    /// the canonical create and add-subscription validators.
    pub fn new(
        subscription_id: String,
        stream_path: String,
        start: SubscriptionStart,
        placement_anchor: bool,
    ) -> Result<Self, DomainError> {
        let subscription = Self {
            subscription_id,
            stream_path,
            start,
            placement_anchor,
        };
        subscription.validate_admission()?;
        Ok(subscription)
    }

    fn validate_admission(&self) -> Result<(), DomainError> {
        if self.subscription_id.is_empty()
            || self.stream_path.is_empty()
            || (self.start.cursor_value().is_none()
                && self.start.current_head_value() != Some(true))
        {
            return Err(DomainError::InvalidSubscription);
        }
        Ok(())
    }

    /// Re-checks a value admitted from an untrusted protobuf wire message
    /// through the canonical constructor predicate.
    fn validate_from_proto(&self) -> Result<(), DomainError> {
        self.validate_admission()
    }

    /// Returns the subscription identifier.
    #[must_use]
    pub fn subscription_id(&self) -> &str {
        &self.subscription_id
    }

    /// Returns the subscription stream path.
    #[must_use]
    pub fn stream_path(&self) -> &str {
        &self.stream_path
    }

    /// Returns the validated start selector.
    #[must_use]
    pub fn start(&self) -> &SubscriptionStart {
        &self.start
    }

    /// Returns whether this subscription is the placement anchor.
    #[must_use]
    pub fn placement_anchor(&self) -> bool {
        self.placement_anchor
    }

}



/// Known subscription states. Unknown protobuf integers are rejected rather
/// than normalized to `Unspecified`.
#[acyclic_contract_derive::enumeration(error = DomainError, unknown = DomainError::UnknownSubscriptionState)]

#[derive(TS)]
#[ts(export_to = "actors/SubscriptionState.ts")]
#[ts(repr(enum))]
pub enum SubscriptionState {
    /// No subscription state was specified by the service.
    Unspecified = 0,
    /// The subscription is active.
    Active = 1,
    /// The subscription is paused.
    Paused = 2,
}


/// Known Actor states. Unknown protobuf integers remain observable errors.
#[acyclic_contract_derive::enumeration(error = DomainError, unknown = DomainError::UnknownActorState)]

#[derive(TS)]
#[ts(export_to = "actors/ActorState.ts")]
#[ts(repr(enum))]
pub enum ActorState {
    /// No Actor state was specified by the service.
    Unspecified = 0,
    /// The Actor is active.
    Active = 1,
    /// The Actor is hibernated.
    Hibernated = 2,
    /// The Actor is paused.
    Paused = 3,
}

/// Published service error codes. Unknown numeric values stay visible through
/// `DomainError::UnknownErrorCode` instead of being coerced to `Unspecified`.
#[acyclic_contract_derive::enumeration(error = DomainError, unknown = DomainError::UnknownErrorCode)]

#[derive(TS)]
#[ts(export_to = "actors/ErrorCode.ts")]
#[ts(repr(enum))]
pub enum ErrorCode {
    /// No service error code was specified.
    Unspecified = 0,
    /// The request was invalid.
    InvalidArgument = 1,
    /// The requested capability was denied.
    CapabilityDenied = 2,
    /// The requested capability expired.
    CapabilityExpired = 3,
    /// The Actor could not be found.
    ActorNotFound = 4,
    /// The subscription could not be found.
    SubscriptionNotFound = 5,
    /// The idempotency key did not match the original request.
    IdempotencyMismatch = 6,
    /// The request conflicted with current state.
    Conflict = 7,
    /// Admission was denied.
    AdmissionDenied = 8,
    /// Checkpoint creation failed.
    CheckpointFailed = 9,
    /// A required dependency was unavailable.
    DependencyUnavailable = 10,
}


/// A service error with a typed known code and lossless message.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, name = "Error")]

#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/ServiceError.ts")]
#[ts(rename_all = "camelCase")]
pub struct ServiceError {
    #[wire(tag = 1, enumeration = ErrorCode, from = parse_error_code, into = encode_error_code)]

    code: ErrorCode,
    #[wire(tag = 2)]
    message: String,
}

impl ServiceError {
    /// Creates a service error with its typed code and lossless message.
    #[must_use]
    pub fn new(code: ErrorCode, message: String) -> Self {
        Self { code, message }
    }

    /// Returns the typed service error code.
    #[must_use]
    pub fn code(&self) -> ErrorCode {
        self.code
    }

    /// Returns the service error message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}







/// Lossless semantic view of a subscription observation.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]

#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/SubscriptionObservation.ts")]
#[ts(rename_all = "camelCase")]
pub struct SubscriptionObservation {
    #[wire(tag = 1)]

    subscription_id: String,
    #[wire(tag = 2)]
    stream_path: String,
    #[wire(tag = 3, enumeration = SubscriptionState, from = parse_subscription_state, into = encode_subscription_state)]
    state: SubscriptionState,
    #[ts(type = "bigint")]
    #[wire(tag = 4)]
    delivered_cursor: u64,
    #[ts(type = "bigint")]
    #[wire(tag = 5)]
    completed_cursor: u64,
    #[ts(type = "bigint")]
    #[wire(tag = 6)]
    recoverable_cursor: u64,
    #[wire(tag = 7)]
    placement_anchor: bool,
    #[wire(tag = 8)]
    retry_count: u32,
    #[wire(tag = 9)]
    failure_code: String,
    #[ts(optional)]
    #[wire(tag = 10)]
    failed_cursor: Option<u64>,
}

impl SubscriptionObservation {
    /// Returns the subscription identifier.
    #[must_use]
    pub fn subscription_id(&self) -> &str { &self.subscription_id }
    /// Returns the stream path.
    #[must_use]
    pub fn stream_path(&self) -> &str { &self.stream_path }
    /// Returns the typed subscription state.
    #[must_use]
    pub fn state(&self) -> SubscriptionState { self.state }
    /// Returns the delivered cursor without narrowing its `u64` range.
    #[must_use]
    pub fn delivered_cursor(&self) -> u64 { self.delivered_cursor }
    /// Returns the completed cursor without narrowing its `u64` range.
    #[must_use]
    pub fn completed_cursor(&self) -> u64 { self.completed_cursor }
    /// Returns the recoverable cursor without narrowing its `u64` range.
    #[must_use]
    pub fn recoverable_cursor(&self) -> u64 { self.recoverable_cursor }
    /// Returns whether this subscription is the placement anchor.
    #[must_use]
    pub fn placement_anchor(&self) -> bool { self.placement_anchor }
    /// Returns the number of recorded retries.
    #[must_use]
    pub fn retry_count(&self) -> u32 { self.retry_count }
    /// Returns the lossless failure code spelling.
    #[must_use]
    pub fn failure_code(&self) -> &str { &self.failure_code }
    /// Returns the optional failed cursor.
    #[must_use]
    pub fn failed_cursor(&self) -> Option<u64> { self.failed_cursor }
}





/// Lossless semantic view of a server Actor observation.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]

#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/ActorObservation.ts")]
#[ts(rename_all = "camelCase")]
pub struct ActorObservation {
    #[wire(tag = 1, string)]

    actor_id: ActorId,
    #[wire(tag = 2, bytes)]
    code_sha256: CodeSha256,
    #[wire(tag = 3)]
    home_region: String,
    #[wire(tag = 4, enumeration = ActorState, from = parse_actor_state, into = encode_actor_state)]
    state: ActorState,
    #[wire(tag = 5, message)]
    subscriptions: Vec<SubscriptionObservation>,
    #[ts(optional)]
    #[wire(tag = 6)]
    checkpoint_unix_millis: Option<u64>,
    #[ts(type = "bigint")]
    #[wire(tag = 7)]
    checkpoint_epoch: u64,
    #[ts(type = "bigint")]
    #[wire(tag = 8)]
    configuration_revision: u64,
}

impl ActorObservation {
    /// Returns the validated Actor identity.
    #[must_use]
    pub fn actor_id(&self) -> &ActorId { &self.actor_id }
    /// Returns the validated code digest.
    #[must_use]
    pub fn code_sha256(&self) -> &CodeSha256 { &self.code_sha256 }
    /// Returns the home region spelling.
    #[must_use]
    pub fn home_region(&self) -> &str { &self.home_region }
    /// Returns the typed Actor state.
    #[must_use]
    pub fn state(&self) -> ActorState { self.state }
    /// Returns the observed subscriptions in wire order.
    #[must_use]
    pub fn subscriptions(&self) -> &[SubscriptionObservation] { &self.subscriptions }
    /// Returns the optional checkpoint timestamp in Unix milliseconds.
    #[must_use]
    pub fn checkpoint_unix_millis(&self) -> Option<u64> { self.checkpoint_unix_millis }
    /// Returns the checkpoint epoch.
    #[must_use]
    pub fn checkpoint_epoch(&self) -> u64 { self.checkpoint_epoch }
    /// Returns the configuration revision.
    #[must_use]
    pub fn configuration_revision(&self) -> u64 { self.configuration_revision }
}





macro_rules! actor_response_type {
    ($name:ident, $wire:ident) => {
	        #[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]

        #[derive(Clone, Debug, Eq, PartialEq, TS)]
        #[ts(export_to = concat!("actors/", stringify!($name), ".ts"))]
        #[ts(rename_all = "camelCase")]
        #[doc = "Typed response preserving the optional server Actor observation."]
        pub struct $name {
            #[wire(tag = 1, message)]
            #[ts(optional)]
            actor: Option<ActorObservation>,
        }

        impl $name {
            /// Returns the optional Actor observation carried by this response.
            #[must_use]
            pub fn actor(&self) -> Option<&ActorObservation> {
                self.actor.as_ref()
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
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = CreateActorRequest::validate_from_proto)]

#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/CreateActorRequest.ts")]
#[ts(rename_all = "camelCase")]
pub struct CreateActorRequest {
    #[wire(tag = 1, bytes)]

    code_sha256: CodeSha256,
    #[wire(tag = 2)]
    home_region: String,
    #[wire(tag = 3, message)]
    bindings: Vec<Binding>,
    #[wire(tag = 4, message)]
    limits: ActorLimits,
    #[wire(tag = 5, message)]
    subscriptions: Vec<SubscriptionSpec>,
    #[wire(tag = 6)]
    idempotency_key: String,
}

impl CreateActorRequest {
    /// Builds and validates a create request through the canonical admission path.
    pub fn new(
        code_sha256: CodeSha256,
        home_region: String,
        bindings: Vec<Binding>,
        limits: ActorLimits,
        subscriptions: Vec<SubscriptionSpec>,
        idempotency_key: String,
    ) -> Result<Self, DomainError> {
        let wire = wire::CreateActorRequest {
            code_sha256: code_sha256.as_bytes().to_vec(),
            home_region,
            bindings: bindings.into_iter().map(Into::into).collect(),
            limits: Some(limits.into()),
            subscriptions: subscriptions.into_iter().map(Into::into).collect(),
            idempotency_key,
        };
        Self::try_from(wire)
    }

    /// Re-runs the canonical request validator after converting untrusted wire
    /// fields into their semantic Rust representation. This deliberately uses
    /// the infallible semantic-to-wire projection rather than recursively
    /// invoking `TryFrom`, so wire-only collection and idempotency rules are
    /// shared with the public constructor.
    fn validate_from_proto(&self) -> Result<(), DomainError> {
        let wire: wire::CreateActorRequest = self.clone().into();
        crate::validate_create(&wire).map_err(DomainError::Contract)
    }

    /// Returns the code digest.
    #[must_use]
    pub fn code_sha256(&self) -> &CodeSha256 { &self.code_sha256 }
    /// Returns the home region.
    #[must_use]
    pub fn home_region(&self) -> &str { &self.home_region }
    /// Returns bindings in their request order.
    #[must_use]
    pub fn bindings(&self) -> &[Binding] { &self.bindings }
    /// Returns the validated resource limits.
    #[must_use]
    pub fn limits(&self) -> &ActorLimits { &self.limits }
    /// Returns subscriptions in their request order.
    #[must_use]
    pub fn subscriptions(&self) -> &[SubscriptionSpec] { &self.subscriptions }
    /// Returns the idempotency key.
    #[must_use]
    pub fn idempotency_key(&self) -> &str { &self.idempotency_key }
}





/// Update request after the canonical admission validator has run.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = UpdateActorRequest::validate_from_proto)]

#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/UpdateActorRequest.ts")]
#[ts(rename_all = "camelCase")]
pub struct UpdateActorRequest {
    #[wire(tag = 1, string)]

    actor_id: ActorId,
    #[wire(tag = 2, bytes)]
    code_sha256: CodeSha256,
    #[wire(tag = 3, message)]
    bindings: Vec<Binding>,
    #[wire(tag = 4, message)]
    limits: ActorLimits,
    #[ts(type = "bigint")]
    #[wire(tag = 5)]
    expected_configuration_revision: u64,
    #[wire(tag = 6)]
    idempotency_key: String,
}

impl UpdateActorRequest {
    /// Builds and validates an update request through the canonical admission path.
    pub fn new(
        actor_id: ActorId,
        code_sha256: CodeSha256,
        bindings: Vec<Binding>,
        limits: ActorLimits,
        expected_configuration_revision: u64,
        idempotency_key: String,
    ) -> Result<Self, DomainError> {
        let wire = wire::UpdateActorRequest {
            actor_id: actor_id.as_str().to_owned(),
            code_sha256: code_sha256.as_bytes().to_vec(),
            bindings: bindings.into_iter().map(Into::into).collect(),
            limits: Some(limits.into()),
            expected_configuration_revision,
            idempotency_key,
        };
        Self::try_from(wire)
    }

    /// Re-runs the canonical request validator without recursively invoking
    /// the semantic `TryFrom` implementation.
    fn validate_from_proto(&self) -> Result<(), DomainError> {
        let wire: wire::UpdateActorRequest = self.clone().into();
        crate::validate_update(&wire).map_err(DomainError::Contract)
    }

    /// Returns the Actor identity.
    #[must_use]
    pub fn actor_id(&self) -> &ActorId { &self.actor_id }
    /// Returns the code digest.
    #[must_use]
    pub fn code_sha256(&self) -> &CodeSha256 { &self.code_sha256 }
    /// Returns bindings in their request order.
    #[must_use]
    pub fn bindings(&self) -> &[Binding] { &self.bindings }
    /// Returns the validated resource limits.
    #[must_use]
    pub fn limits(&self) -> &ActorLimits { &self.limits }
    /// Returns the expected compare-and-replace revision.
    #[must_use]
    pub fn expected_configuration_revision(&self) -> u64 {
        self.expected_configuration_revision
    }
    /// Returns the idempotency key.
    #[must_use]
    pub fn idempotency_key(&self) -> &str { &self.idempotency_key }
}





/// Inspect currently has no additional wire validator; only the semantic
/// Actor identity rule is applied by this conversion.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]

#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/InspectActorRequest.ts")]
#[ts(rename_all = "camelCase")]
pub struct InspectActorRequest {
    #[wire(tag = 1, string)]

    actor_id: ActorId,
}

impl InspectActorRequest {
    /// Creates an inspect request for a validated Actor identity.
    #[must_use]
    pub fn new(actor_id: ActorId) -> Self { Self { actor_id } }
    /// Returns the Actor identity.
    #[must_use]
    pub fn actor_id(&self) -> &ActorId { &self.actor_id }
}





/// Add subscription request after the canonical admission validator has run.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = AddSubscriptionRequest::validate_from_proto)]

#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/AddSubscriptionRequest.ts")]
#[ts(rename_all = "camelCase")]
pub struct AddSubscriptionRequest {
    #[wire(tag = 1, string)]

    actor_id: ActorId,
    #[wire(tag = 2, message)]
    subscription: SubscriptionSpec,
    #[wire(tag = 3)]
    idempotency_key: String,
}

impl AddSubscriptionRequest {
    /// Builds and validates an add-subscription request through the canonical path.
    pub fn new(
        actor_id: ActorId,
        subscription: SubscriptionSpec,
        idempotency_key: String,
    ) -> Result<Self, DomainError> {
        let wire = wire::AddSubscriptionRequest {
            actor_id: actor_id.as_str().to_owned(),
            subscription: Some(subscription.into()),
            idempotency_key,
        };
        Self::try_from(wire)
    }

    /// Re-runs the canonical request validator without recursively invoking
    /// the semantic `TryFrom` implementation.
    fn validate_from_proto(&self) -> Result<(), DomainError> {
        let wire: wire::AddSubscriptionRequest = self.clone().into();
        crate::validate_add_subscription(&wire).map_err(DomainError::Contract)
    }

    /// Returns the Actor identity.
    #[must_use]
    pub fn actor_id(&self) -> &ActorId { &self.actor_id }
    /// Returns the validated subscription.
    #[must_use]
    pub fn subscription(&self) -> &SubscriptionSpec { &self.subscription }
    /// Returns the idempotency key.
    #[must_use]
    pub fn idempotency_key(&self) -> &str { &self.idempotency_key }
}





/// Removal has no canonical validator yet, so subscription and idempotency
/// strings remain unbranded and are carried exactly as received.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]

#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/RemoveSubscriptionRequest.ts")]
#[ts(rename_all = "camelCase")]
pub struct RemoveSubscriptionRequest {
    #[wire(tag = 1, string)]

    actor_id: ActorId,
    #[wire(tag = 2)]
    subscription_id: String,
    #[wire(tag = 3)]
    idempotency_key: String,
}

impl RemoveSubscriptionRequest {
    /// Creates a removal request; no additional canonical validator exists yet.
    #[must_use]
    pub fn new(actor_id: ActorId, subscription_id: String, idempotency_key: String) -> Self {
        Self {
            actor_id,
            subscription_id,
            idempotency_key,
        }
    }
    /// Returns the Actor identity.
    #[must_use]
    pub fn actor_id(&self) -> &ActorId { &self.actor_id }
    /// Returns the subscription identifier.
    #[must_use]
    pub fn subscription_id(&self) -> &str { &self.subscription_id }
    /// Returns the idempotency key.
    #[must_use]
    pub fn idempotency_key(&self) -> &str { &self.idempotency_key }
}





/// Resume has no canonical validator yet; the wire strings stay unbranded.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]

#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/ResumeSubscriptionRequest.ts")]
#[ts(rename_all = "camelCase")]
pub struct ResumeSubscriptionRequest {
    #[wire(tag = 1, string)]

    actor_id: ActorId,
    #[wire(tag = 2)]
    subscription_id: String,
    #[wire(tag = 3)]
    idempotency_key: String,
}

impl ResumeSubscriptionRequest {
    /// Creates a resume request; no additional canonical validator exists yet.
    #[must_use]
    pub fn new(actor_id: ActorId, subscription_id: String, idempotency_key: String) -> Self {
        Self {
            actor_id,
            subscription_id,
            idempotency_key,
        }
    }
    /// Returns the Actor identity.
    #[must_use]
    pub fn actor_id(&self) -> &ActorId { &self.actor_id }
    /// Returns the subscription identifier.
    #[must_use]
    pub fn subscription_id(&self) -> &str { &self.subscription_id }
    /// Returns the idempotency key.
    #[must_use]
    pub fn idempotency_key(&self) -> &str { &self.idempotency_key }
}





/// Checkpoint has no canonical validator yet; only the Actor identity rule is
/// applied and the idempotency key remains an ordinary string.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]

#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/CheckpointActorRequest.ts")]
#[ts(rename_all = "camelCase")]
pub struct CheckpointActorRequest {
    #[wire(tag = 1, string)]

    actor_id: ActorId,
    #[wire(tag = 2)]
    idempotency_key: String,
}

impl CheckpointActorRequest {
    /// Creates a checkpoint request for a validated Actor identity.
    #[must_use]
    pub fn new(actor_id: ActorId, idempotency_key: String) -> Self {
        Self {
            actor_id,
            idempotency_key,
        }
    }
    /// Returns the Actor identity.
    #[must_use]
    pub fn actor_id(&self) -> &ActorId { &self.actor_id }
    /// Returns the idempotency key.
    #[must_use]
    pub fn idempotency_key(&self) -> &str { &self.idempotency_key }
}





/// Typed invocation request. Method, URL, headers, and body preserve the
/// existing wire contract without adding new validation rules.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]

#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/InvokeActorRequest.ts")]
#[ts(rename_all = "camelCase")]
pub struct InvokeActorRequest {
    #[wire(tag = 1, string)]
    actor_id: ActorId,
    #[wire(tag = 2)]
    method: String,
    #[wire(tag = 3)]
    url: String,
    #[wire(tag = 4, bytes)]
    #[ts(type = "import(\"../../readonly.js\").ReadonlyBytes")]
    body: Vec<u8>,
    #[wire(tag = 5, message)]
    headers: Vec<Header>,
}

impl InvokeActorRequest {
    /// Creates an invocation request while preserving bytes and header order.
    #[must_use]
    pub fn new(
        actor_id: ActorId,
        method: String,
        url: String,
        body: Vec<u8>,
        headers: Vec<Header>,
    ) -> Self {
        Self {
            actor_id,
            method,
            url,
            body,
            headers,
        }
    }
    /// Returns the Actor identity.
    #[must_use]
    pub fn actor_id(&self) -> &ActorId { &self.actor_id }
    /// Returns the invocation method.
    #[must_use]
    pub fn method(&self) -> &str { &self.method }
    /// Returns the invocation URL.
    #[must_use]
    pub fn url(&self) -> &str { &self.url }
    /// Returns the invocation body bytes.
    #[must_use]
    pub fn body(&self) -> &[u8] { &self.body }
    /// Returns headers in their original order.
    #[must_use]
    pub fn headers(&self) -> &[Header] { &self.headers }
}





/// Typed invocation response with byte-preserving body and headers.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]

#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/InvokeActorResponse.ts")]
#[ts(rename_all = "camelCase")]
pub struct InvokeActorResponse {
    #[wire(tag = 1)]
    status: u32,
    #[wire(tag = 2, bytes)]
    #[ts(type = "import(\"../../readonly.js\").ReadonlyBytes")]
    body: Vec<u8>,
    #[wire(tag = 3, message)]
    headers: Vec<Header>,
}

impl InvokeActorResponse {
    /// Returns the HTTP-like status code.
    #[must_use]
    pub fn status(&self) -> u32 { self.status }
    /// Returns the response body bytes.
    #[must_use]
    pub fn body(&self) -> &[u8] { &self.body }
    /// Returns headers in their original order.
    #[must_use]
    pub fn headers(&self) -> &[Header] { &self.headers }
}

// The semantic export registry is shared by ts-rs and the SDK facade generator.
// ts-rs supplies the emitted identifier while its visitor recursively discovers
// dependencies, so renamed or newly nested Rust declarations follow the same
// executable export path automatically.
macro_rules! typescript_message_roots {
    ($callback:ident) => {
        $callback!(
            ActorObservation,
            CreateActorRequest,
            UpdateActorRequest,
            InspectActorRequest,
            AddSubscriptionRequest,
            RemoveSubscriptionRequest,
            ResumeSubscriptionRequest,
            CheckpointActorRequest,
            InvokeActorRequest,
            CreateActorResponse,
            UpdateActorResponse,
            InspectActorResponse,
            AddSubscriptionResponse,
            RemoveSubscriptionResponse,
            ResumeSubscriptionResponse,
            CheckpointActorResponse,
            InvokeActorResponse,
        )
    };
}
macro_rules! typescript_roots {
    ($callback:ident) => {
        $callback!(
            subscription_start::CurrentHeadMarker,
            PositiveU64,
            ErrorCode,
            ServiceError,
        );
        typescript_message_roots!($callback);
    };
}

struct TypeScriptExportVisitor<'a> {
    config: &'a Config,
    names: Vec<String>,
    seen: std::collections::HashSet<std::any::TypeId>,
}

impl TypeVisitor for TypeScriptExportVisitor<'_> {
    fn visit<T: TS + 'static + ?Sized>(&mut self) {
        if !self.seen.insert(std::any::TypeId::of::<T>()) {
            return;
        }
        if T::output_path().is_some() {
            self.names.push(T::ident(self.config));
        }
        T::visit_dependencies(self);
    }
}

/// Names emitted by the executable Rust semantic export registry, including
/// recursively discovered ts-rs declarations.
pub fn typescript_export_names(config: &Config) -> Vec<String> {
    let mut visitor = TypeScriptExportVisitor {
        config,
        names: Vec::new(),
        seen: std::collections::HashSet::new(),
    };
    macro_rules! visit_roots {
        ($($root:ty),+ $(,)?) => {
            $(visitor.visit::<$root>();)+
        };
    }
    typescript_roots!(visit_roots);
    visitor.names.sort();
    visitor.names
}

/// Actual exported names and compiler-linked wire/semantic identities.
pub fn typescript_export_metadata(config: &Config) -> (Vec<String>, Vec<(String, String)>) {
    fn paired<D, W>(config: &Config) -> (String, String)
    where D: TS + TryFrom<W>, W: prost::Name + prost::Message + From<D> {
        (W::full_name(), D::ident(config))
    }
    let mut messages = Vec::new();
    macro_rules! roots {
        ($($root:ident),+ $(,)?) => {
            $(messages.push(paired::<$root, crate::wire::$root>(config));)+
        };
    }
    typescript_message_roots!(roots);
    (typescript_export_names(config), messages)
}

// ts-rs derives numeric unions from Rust discriminants with repr(enum), but its
// file export emits a runtime enum. Keep the existing type-only public surface.
fn export_numeric_union<T: TS + 'static>(path: &Path, config: &Config) -> Result<(), ExportError> {
    let generated = T::export_to_string(config)?;
    let declaration = format!("{}\n", T::decl(config));
    let prefix = generated.strip_suffix(&declaration).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "ts-rs enum declaration suffix is absent")
    })?;
    let output = T::output_path().ok_or(ExportError::CannotBeExported(std::any::type_name::<T>()))?;
    std::fs::write(
        path.join(output),
        format!("{prefix}type {} = {};\n", T::ident(config), T::inline(config)),
    )?;
    Ok(())
}

/// Export all public Actors request and response declarations and their
/// recursively discovered semantic dependencies.
///
/// The output directory is supplied by the SDK generator so generation never
/// writes into the Rust source tree. `ts-rs` keeps dependency traversal and
/// import paths derived from these Rust types.
pub fn export_typescript(path: impl AsRef<Path>) -> Result<(), ExportError> {
    let config = Config::default()
        .with_out_dir(path.as_ref())
        .with_import_extension(Some("js"));

    macro_rules! export_roots {
        ($($root:ty),+ $(,)?) => {
            $(<$root as TS>::export_all(&config)?;)+
        };
    }

    typescript_roots!(export_roots);
    export_numeric_union::<SubscriptionState>(path.as_ref(), &config)?;
    export_numeric_union::<ActorState>(path.as_ref(), &config)?;
    export_numeric_union::<ErrorCode>(path.as_ref(), &config)?;
    Ok(())
}

#[cfg(test)]
#[allow(
    clippy::indexing_slicing,
    clippy::too_many_lines,
    reason = "boundary tests use explicit indexed fixtures and grouped validation cases"
)]
mod tests {
    use super::*;

    #[test]
    fn actor_id_matches_existing_presence_rule() {
        assert_eq!(ActorId::new(String::new()), Err(DomainError::EmptyActorId));
        assert_eq!(
            ActorId::new("  ".into()).map(|value| value.as_str().to_owned()),
            Ok(String::from("  "))
        );
    }

    #[test]
    fn typescript_export_includes_error_roots_with_esm_imports() {
        let output = std::env::temp_dir().join(format!(
            "acyclic-actors-typescript-export-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&output);

        export_typescript(&output).expect("Actors TypeScript export should succeed");

        let service_error =
            std::fs::read_to_string(output.join("actors/ServiceError.ts")).expect("ServiceError");
        assert!(service_error.contains("from \"./ErrorCode.js\""));
        for name in ["ActorState", "SubscriptionState", "ErrorCode"] {
            let declaration = std::fs::read_to_string(output.join(format!("actors/{name}.ts")))
                .expect("numeric enum declaration");
            assert!(declaration.contains(&format!("export type {name} = ")));
            assert!(!declaration.contains("export enum "));
        }
        let current_head = std::fs::read_to_string(output.join("actors/CurrentHeadMarker.ts"))
            .expect("CurrentHeadMarker");
        assert!(current_head.contains("true"));

        std::fs::remove_dir_all(output).expect("remove temporary export");
    }

    #[test]
    fn typescript_export_names_follow_nested_ts_rs_metadata() {
        let names = typescript_export_names(&Config::default());

        assert!(names.windows(2).all(|pair| pair[0] < pair[1]));
        for name in [
            "ActorObservation",
            "ActorState",
            "CurrentHeadMarker",
            "SubscriptionObservation",
            "SubscriptionState",
        ] {
            assert!(names.iter().any(|candidate| candidate == name), "{name}");
        }
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
    fn positive_u64_is_the_single_limits_admission_predicate() {
        let invalid = DomainError::Contract(crate::ContractError::InvalidArgument);
        assert_eq!(PositiveU64::new(0), Err(invalid));
        assert_eq!(PositiveU64::try_from(0), Err(invalid));

        let one = PositiveU64::new(1).expect("one is positive");
        let max = PositiveU64::new(u64::MAX).expect("u64::MAX is representable");
        assert_eq!(one.get(), 1);
        assert_eq!(max.get(), u64::MAX);

        let wire = wire::ActorLimits {
            handler_timeout_millis: 1,
            memory_bytes: u64::MAX,
            checkpoint_bytes: 4096,
        };
        let limits = ActorLimits::try_from(wire.clone()).expect("positive wire limits");
        assert_eq!(limits.handler_timeout_millis(), 1);
        assert_eq!(limits.memory_bytes(), u64::MAX);
        assert_eq!(limits.checkpoint_bytes(), 4096);
        assert_eq!(wire::ActorLimits::from(limits), wire);

        let invalid_wire = wire::ActorLimits {
            handler_timeout_millis: 0,
            memory_bytes: 1,
            checkpoint_bytes: 1,
        };
        assert_eq!(ActorLimits::try_from(invalid_wire), Err(invalid));
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
        let invalid_start = wire::SubscriptionStart {
            start: Some(wire::subscription_start::Start::CurrentHead(false)),
        };
        assert_eq!(
            SubscriptionStart::try_from(invalid_start),
            Err(DomainError::InvalidSubscription)
        );

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

    #[test]
    fn wire_ingress_rechecks_container_constructor_predicates() {
        let invalid_binding = wire::Binding {
            name: String::new(),
            capability: "read".into(),
            resource: "bucket/a".into(),
        };
        assert_eq!(
            Binding::try_from(invalid_binding),
            Err(DomainError::InvalidBinding)
        );

        let invalid_subscription = wire::SubscriptionSpec {
            subscription_id: String::new(),
            stream_path: "events/input".into(),
            start: Some(wire::SubscriptionStart {
                start: Some(wire::subscription_start::Start::Cursor(0)),
            }),
            placement_anchor: false,
        };
        assert_eq!(
            SubscriptionSpec::try_from(invalid_subscription),
            Err(DomainError::InvalidSubscription)
        );
    }

    #[test]
    fn wire_ingress_rechecks_request_admission_predicates() {
        let subscription = wire::SubscriptionSpec {
            subscription_id: "events".into(),
            stream_path: "agents/a/events".into(),
            start: Some(wire::SubscriptionStart {
                start: Some(wire::subscription_start::Start::Cursor(0)),
            }),
            placement_anchor: false,
        };
        let binding = wire::Binding {
            name: "storage".into(),
            capability: "read".into(),
            resource: "bucket/a".into(),
        };
        let create = wire::CreateActorRequest {
            code_sha256: vec![1; 32],
            home_region: "eu".into(),
            bindings: vec![binding.clone()],
            limits: Some(wire::ActorLimits {
                handler_timeout_millis: 1,
                memory_bytes: 2,
                checkpoint_bytes: 3,
            }),
            subscriptions: vec![subscription.clone()],
            idempotency_key: "create-1".into(),
        };

        let mut duplicate = create.clone();
        duplicate.subscriptions.push(subscription.clone());
        assert_eq!(
            CreateActorRequest::try_from(duplicate),
            Err(DomainError::Contract(crate::ContractError::DuplicateName))
        );

        let mut multiple_anchors = create.clone();
        multiple_anchors.subscriptions[0].placement_anchor = true;
        let mut second_anchor = subscription;
        second_anchor.subscription_id = "other-events".into();
        second_anchor.placement_anchor = true;
        multiple_anchors.subscriptions.push(second_anchor);
        assert_eq!(
            CreateActorRequest::try_from(multiple_anchors),
            Err(DomainError::Contract(crate::ContractError::InvalidArgument))
        );

        let mut invalid_idempotency = create.clone();
        invalid_idempotency.idempotency_key.clear();
        assert_eq!(
            CreateActorRequest::try_from(invalid_idempotency),
            Err(DomainError::Contract(crate::ContractError::InvalidArgument))
        );

        let mut over_limit = create;
        over_limit
            .bindings
            .extend((0..=crate::MAX_BINDINGS).map(|index| wire::Binding {
                name: format!("binding-{index}"),
                capability: "read".into(),
                resource: "bucket/a".into(),
            }));
        assert_eq!(
            CreateActorRequest::try_from(over_limit),
            Err(DomainError::Contract(crate::ContractError::LimitExceeded))
        );

        let update = wire::UpdateActorRequest {
            actor_id: "actor-1".into(),
            code_sha256: vec![1; 32],
            bindings: vec![binding.clone()],
            limits: Some(wire::ActorLimits {
                handler_timeout_millis: 1,
                memory_bytes: 2,
                checkpoint_bytes: 3,
            }),
            expected_configuration_revision: 1,
            idempotency_key: "update-1".into(),
        };
        let mut duplicate_bindings = update.clone();
        duplicate_bindings.bindings.push(binding);
        assert_eq!(
            UpdateActorRequest::try_from(duplicate_bindings),
            Err(DomainError::Contract(crate::ContractError::DuplicateName))
        );

        let mut invalid_update_idempotency = update;
        invalid_update_idempotency.idempotency_key.clear();
        assert_eq!(
            UpdateActorRequest::try_from(invalid_update_idempotency),
            Err(DomainError::Contract(crate::ContractError::InvalidArgument))
        );

        let add = wire::AddSubscriptionRequest {
            actor_id: "actor-1".into(),
            subscription: Some(wire::SubscriptionSpec {
                subscription_id: "events".into(),
                stream_path: "agents/a/events".into(),
                start: Some(wire::SubscriptionStart {
                    start: Some(wire::subscription_start::Start::CurrentHead(true)),
                }),
                placement_anchor: false,
            }),
            idempotency_key: "add-1".into(),
        };
        let mut invalid_add_idempotency = add;
        invalid_add_idempotency.idempotency_key.clear();
        assert_eq!(
            AddSubscriptionRequest::try_from(invalid_add_idempotency),
            Err(DomainError::Contract(crate::ContractError::InvalidArgument))
        );
    }

    #[test]
    fn public_create_constructor_round_trips_and_uses_canonical_validation() {
        let digest = CodeSha256::new(vec![1; 32]).expect("valid digest");
        let binding = Binding::new("storage".into(), "read".into(), "bucket/a".into())
            .expect("valid binding");
        let limits = ActorLimits::new(1, u64::MAX, 4096).expect("positive limits");
        let subscription = SubscriptionSpec::new(
            "events".into(),
            "agents/a/events".into(),
            SubscriptionStart::cursor(u64::MAX),
            false,
        )
        .expect("valid subscription");
        let request = CreateActorRequest::new(
            digest.clone(),
            "eu".into(),
            vec![binding.clone()],
            limits,
            vec![subscription.clone()],
            "create-1".into(),
        )
        .expect("valid create request");
        let wire: wire::CreateActorRequest = request.clone().into();
        let decoded = CreateActorRequest::try_from(wire).expect("canonical decode");
        assert_eq!(decoded, request);
        assert_eq!(decoded.code_sha256(), &digest);
        assert_eq!(decoded.bindings(), &[binding]);
        assert_eq!(decoded.subscriptions(), std::slice::from_ref(&subscription));

        let duplicate = CreateActorRequest::new(
            digest,
            "eu".into(),
            Vec::new(),
            ActorLimits::new(1, 1, 1).expect("positive limits"),
            vec![subscription.clone(), subscription],
            "create-2".into(),
        );
        assert_eq!(
            duplicate,
            Err(DomainError::Contract(crate::ContractError::DuplicateName))
        );
    }

    #[test]
    fn public_update_and_add_constructors_preserve_wire_inverse() {
        let actor_id = ActorId::new("actor-1".into()).expect("valid actor id");
        let digest = CodeSha256::new(vec![2; 32]).expect("valid digest");
        let limits = ActorLimits::new(1, 2, 3).expect("positive limits");
        let update = UpdateActorRequest::new(
            actor_id.clone(),
            digest,
            Vec::new(),
            limits,
            u64::MAX,
            "update-1".into(),
        )
        .expect("valid update request");
        let update_wire: wire::UpdateActorRequest = update.clone().into();
        assert_eq!(UpdateActorRequest::try_from(update_wire), Ok(update));

        let subscription = SubscriptionSpec::new(
            "events".into(),
            "agents/a/events".into(),
            SubscriptionStart::current_head(),
            false,
        )
        .expect("valid subscription");
        let add = AddSubscriptionRequest::new(actor_id, subscription, "add-1".into())
            .expect("valid add request");
        let add_wire: wire::AddSubscriptionRequest = add.clone().into();
        assert_eq!(AddSubscriptionRequest::try_from(add_wire), Ok(add));

        assert_eq!(
            SubscriptionStart::try_from(wire::SubscriptionStart {
                start: Some(wire::subscription_start::Start::CurrentHead(false)),
            }),
            Err(DomainError::InvalidSubscription)
        );
    }
}
