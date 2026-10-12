use super::*;

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
