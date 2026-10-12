use super::*;

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
    pub fn new(name: String, value: String) -> Self {
        Self { name, value }
    }

    /// Returns the header name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the header value.
    pub fn value(&self) -> &str {
        &self.value
    }
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
    fn validate_from_proto(&self) -> Result<(), DomainError> {
        self.validate_admission()
    }

    /// Returns the binding name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the capability name.
    pub fn capability(&self) -> &str {
        &self.capability
    }

    /// Returns the bound resource name.
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
    pub fn handler_timeout_millis(&self) -> u64 {
        self.handler_timeout_millis.get()
    }

    /// Returns the memory limit in bytes.
    pub fn memory_bytes(&self) -> u64 {
        self.memory_bytes.get()
    }

    /// Returns the checkpoint limit in bytes.
    pub fn checkpoint_bytes(&self) -> u64 {
        self.checkpoint_bytes.get()
    }
}
/// The published subscription-start message owns its oneof declaration.
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
        Some(subscription_start::StartProto::CurrentHead(true)) => Ok(
            subscription_start::Start::CurrentHead(subscription_start::CurrentHeadMarker::new()),
        ),
        Some(subscription_start::StartProto::CurrentHead(false)) | None => {
            Err(DomainError::InvalidSubscription)
        }
    }
}

pub mod subscription_start {
    use super::*;

    /// The only valid semantic payload for the current-head selector. The
    #[derive(Clone, Copy, Debug, Eq, PartialEq, TS)]
    #[ts(type = "true", export_to = "actors/CurrentHeadMarker.ts")]
    pub struct CurrentHeadMarker;

    impl Default for CurrentHeadMarker {
        fn default() -> Self {
            Self::new()
        }
    }

    impl CurrentHeadMarker {
        pub const fn new() -> Self {
            Self
        }
    }

    impl TryFrom<bool> for CurrentHeadMarker {
        type Error = DomainError;

        fn try_from(value: bool) -> Result<Self, Self::Error> {
            value
                .then_some(Self)
                .ok_or(DomainError::InvalidSubscription)
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
    pub fn cursor(cursor: u64) -> Self {
        Self {
            start: subscription_start::Start::Cursor(cursor),
        }
    }

    /// Creates a current-head start selector.
    pub fn current_head() -> Self {
        Self {
            start: subscription_start::Start::CurrentHead(
                subscription_start::CurrentHeadMarker::new(),
            ),
        }
    }

    /// Returns the cursor payload when this start selects an explicit cursor.
    pub fn cursor_value(&self) -> Option<u64> {
        match self.start {
            subscription_start::Start::Cursor(cursor) => Some(cursor),
            subscription_start::Start::CurrentHead(_) => None,
        }
    }

    /// Returns the current-head payload when this start selects current head.
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
    fn validate_from_proto(&self) -> Result<(), DomainError> {
        self.validate_admission()
    }

    /// Returns the subscription identifier.
    pub fn subscription_id(&self) -> &str {
        &self.subscription_id
    }

    /// Returns the subscription stream path.
    pub fn stream_path(&self) -> &str {
        &self.stream_path
    }

    /// Returns the validated start selector.
    pub fn start(&self) -> &SubscriptionStart {
        &self.start
    }

    /// Returns whether this subscription is the placement anchor.
    pub fn placement_anchor(&self) -> bool {
        self.placement_anchor
    }
}

/// Known subscription states. Unknown protobuf integers are rejected rather
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
    #[wire(tag = 1, enumeration)]
    code: ErrorCode,
    #[wire(tag = 2)]
    message: String,
}

impl ServiceError {
    /// Creates a service error with its typed code and lossless message.
    pub fn new(code: ErrorCode, message: String) -> Self {
        Self { code, message }
    }

    /// Returns the typed service error code.
    pub fn code(&self) -> ErrorCode {
        self.code
    }

    /// Returns the service error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}
