//! Transport-independent public contract types shared by every SDK family.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use uuid::Uuid;

/// A durable identity assigned before an operation is admitted.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct OperationId(Uuid);

impl OperationId {
    /// Creates a fresh operation identity.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for OperationId {
    fn default() -> Self {
        Self::new()
    }
}

/// A caller-selected key used to reconcile uncertain admission.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct IdempotencyKey(pub String);

/// Admission result returned once for every submitted input.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Admission<T> {
    /// Work was accepted and has an addressable identity.
    Accepted(T),
    /// Work was rejected before execution.
    Rejected {
        /// Stable, user-presentable rejection reason.
        reason: String,
    },
    /// The caller must reconcile the existing identity before retrying.
    Indeterminate {
        /// Existing identity that must be reconciled.
        operation_id: OperationId,
    },
}

/// Terminal outcome of an admitted operation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Outcome<T> {
    /// Work completed successfully.
    Succeeded(T),
    /// Work failed after admission.
    Failed {
        /// Stable failure description.
        message: String,
    },
    /// Work was cancelled.
    Cancelled,
    /// Completion is uncertain; observe this identity instead of duplicating it.
    Indeterminate {
        /// Existing identity that must be observed.
        operation_id: OperationId,
    },
}

/// Stable protocol identity used during compatibility handshakes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProtocolIdentity {
    /// Semantic protocol version.
    pub version: String,
    /// Digest of the canonical descriptor set.
    pub descriptor_digest: String,
}

/// A deterministic set of provider capabilities.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Capabilities(BTreeSet<String>);

impl Capabilities {
    /// Builds a capability set.
    pub fn new(values: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self(values.into_iter().map(Into::into).collect())
    }

    /// Returns whether the named capability is present.
    #[must_use]
    pub fn contains(&self, value: &str) -> bool {
        self.0.contains(value)
    }
}

/// Errors shared across provider boundaries.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum Error {
    /// The requested resource does not exist.
    #[error("resource not found: {0}")]
    NotFound(String),
    /// A compare-and-swap or publication precondition failed.
    #[error("conflict: {0}")]
    Conflict(String),
    /// The provider cannot satisfy a required capability.
    #[error("unsupported capability: {0}")]
    Unsupported(String),
    /// The request was invalid.
    #[error("invalid request: {0}")]
    Invalid(String),
}

/// SDK result type.
pub type Result<T> = std::result::Result<T, Error>;
