//! Admission rules shared by the Actors semantic schema and host.
use std::collections::HashSet;

/// Check the public Actors idempotency-key grammar.
pub fn valid_idempotency_key(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256
}

/// Reject malformed Actors idempotency keys.
pub fn validate_idempotency_key(value: &str) -> Result<(), ContractError> {
    valid_idempotency_key(value)
        .then_some(())
        .ok_or(ContractError::InvalidArgument)
}

/// Invalid customer-authored Actors request.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ContractError {
    /// A required field is absent or malformed.
    #[error("required field is absent or malformed")]
    InvalidArgument,
    /// A subscription or binding name occurs more than once.
    #[error("duplicate subscription or binding name")]
    DuplicateName,
}

impl ContractError {
    /// Returns the stable error code shared by every SDK bridge.
    pub const fn code_name(self) -> &'static str {
        match self {
            Self::InvalidArgument => "invalid_argument",
            Self::DuplicateName => "duplicate_name",
        }
    }
}

impl From<std::convert::Infallible> for ContractError {
    fn from(value: std::convert::Infallible) -> Self {
        match value {}
    }
}

/// Check the deployment jurisdiction for the requested workload.
pub fn valid_jurisdiction(jurisdiction: &str, worker: bool) -> bool {
    if worker {
        jurisdiction.is_empty()
    } else {
        matches!(jurisdiction, "" | "eu" | "us" | "fedramp")
    }
}

/// Check the subscription name and its idempotency key.
pub fn validate_subscription_identity(subscription: &str, key: &str) -> Result<(), ContractError> {
    if subscription.is_empty() {
        return Err(ContractError::InvalidArgument);
    }
    validate_idempotency_key(key)
}

/// Require distinct actors, a valid key, and distinct subscription names.
pub fn validate_fork_identity<'a>(
    parent: &str,
    child: &str,
    key: &str,
    mut subscriptions: impl Iterator<Item = &'a str>,
) -> Result<(), ContractError> {
    if parent == child {
        return Err(ContractError::InvalidArgument);
    }
    validate_idempotency_key(key)?;
    let mut seen = HashSet::new();
    subscriptions
        .all(|id| seen.insert(id))
        .then_some(())
        .ok_or(ContractError::DuplicateName)
}
