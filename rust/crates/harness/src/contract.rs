//! Common harness identities and operation outcomes.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use uuid::Uuid;

macro_rules! uuid_id {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(
            Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Creates a fresh identity.
            #[must_use]
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }

            /// Creates a deterministic identity from bytes.
            #[must_use]
            pub const fn from_bytes(bytes: [u8; 16]) -> Self {
                Self(Uuid::from_bytes(bytes))
            }

            /// Parses a canonical UUID.
            pub fn parse(value: &str) -> Result<Self> {
                Uuid::parse_str(value)
                    .map(Self)
                    .map_err(|error| Error::Invalid(error.to_string()))
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }
        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

uuid_id!(AgentId, "Identity of one agent definition.");
uuid_id!(ConversationId, "Identity of one durable conversation.");
uuid_id!(SessionId, "Identity of one live conversation session.");
uuid_id!(TurnId, "Identity of one user-driven turn.");
uuid_id!(TaskId, "Identity of one general task.");
uuid_id!(
    OperationId,
    "Identity assigned before an operation is admitted."
);
uuid_id!(EffectId, "Identity of one external effect.");
uuid_id!(
    EffectAttemptId,
    "Identity of one dispatch attempt for an effect."
);
uuid_id!(InteractionId, "Identity of one typed open interaction.");

/// Caller-selected identity used to reconcile uncertain admission.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IdempotencyKey(pub String);

impl IdempotencyKey {
    /// Creates a bounded non-empty caller retry identity.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
            return Err(Error::Invalid("idempotency key is invalid".into()));
        }
        Ok(Self(value))
    }

    /// Returns the validated string representation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One admission result per submitted input.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Admission<T> {
    /// Work was accepted.
    Accepted(T),
    /// Work was rejected before execution.
    Rejected {
        /// Stable human-readable rejection reason.
        reason: String,
    },
    /// Existing work must be reconciled before retrying.
    Indeterminate {
        /// Operation whose admission must be reconciled.
        operation_id: OperationId,
    },
}

/// Terminal outcome of admitted work.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Outcome<T> {
    /// Work completed successfully.
    Succeeded(T),
    /// Work failed after admission.
    Failed {
        /// Stable human-readable failure description.
        message: String,
    },
    /// Work was cancelled.
    Cancelled,
    /// Completion is uncertain.
    Indeterminate {
        /// Operation whose completion must be reconciled.
        operation_id: OperationId,
    },
}

/// Stable wire identity used during compatibility handshakes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProtocolIdentity {
    /// Semantic protocol version.
    pub version: String,
    /// Digest of the canonical descriptor set.
    pub descriptor_digest: String,
}

/// Deterministic capability set.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Capabilities(BTreeSet<String>);

impl Capabilities {
    /// Builds a capability set.
    pub fn new(values: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self(values.into_iter().map(Into::into).collect())
    }

    /// Returns whether a capability is present.
    #[must_use]
    pub fn contains(&self, value: &str) -> bool {
        self.0.contains(value)
    }

    /// Returns whether this set attenuates an ancestor set.
    #[must_use]
    pub fn is_subset_of(&self, ancestor: &Self) -> bool {
        self.0.is_subset(&ancestor.0)
    }

    /// Iterates in canonical lexical order.
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(String::as_str)
    }

    /// Returns the deterministic intersection of two grants.
    #[must_use]
    pub fn intersect(&self, other: &Self) -> Self {
        Self(self.0.intersection(&other.0).cloned().collect())
    }

    /// Removes explicit denials from this grant.
    #[must_use]
    pub fn without(&self, denied: &Self) -> Self {
        Self(self.0.difference(&denied.0).cloned().collect())
    }
}

/// One explicit authority policy layer; grants intersect and denials always win.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuthorityPolicy {
    /// Capabilities this layer permits.
    pub grants: Capabilities,
    /// Capabilities this layer explicitly denies.
    pub denies: Capabilities,
}

/// Ordered authority-resolution level from the runtime root to one invocation.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorityLevel {
    /// Runtime-wide policy.
    Runtime,
    /// Agent policy.
    Agent,
    /// Conversation policy.
    Conversation,
    /// Session policy.
    Session,
    /// Turn policy.
    Turn,
    /// Durable task policy.
    Task,
    /// Exact invocation policy.
    Invocation,
}

/// One named layer in an explicit authority chain.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PolicyLayer {
    /// Hierarchy position.
    pub level: AuthorityLevel,
    /// Grants and denials contributed by this level.
    pub policy: AuthorityPolicy,
}

/// Resolves runtime → invocation policy layers without ambient authority.
#[must_use]
pub fn resolve_policies(layers: &[AuthorityPolicy]) -> Capabilities {
    let Some(first) = layers.first() else {
        return Capabilities::default();
    };
    let granted = layers
        .iter()
        .skip(1)
        .fold(first.grants.clone(), |current, layer| {
            current.intersect(&layer.grants)
        });
    let denied = Capabilities::new(layers.iter().flat_map(|layer| layer.denies.iter()));
    granted.without(&denied)
}

/// Validates hierarchy order and resolves the exact effective grant.
pub fn resolve_policy_layers(layers: &[PolicyLayer]) -> Result<Capabilities> {
    if layers
        .first()
        .is_none_or(|layer| layer.level != AuthorityLevel::Runtime)
        || layers.windows(2).any(|pair| pair[0].level >= pair[1].level)
    {
        return Err(Error::Invalid(
            "policy layers must start at runtime and be strictly ordered".into(),
        ));
    }
    Ok(resolve_policies(
        &layers
            .iter()
            .map(|layer| layer.policy.clone())
            .collect::<Vec<_>>(),
    ))
}

/// Errors shared by harness surfaces.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum Error {
    /// The requested resource does not exist.
    #[error("resource not found: {0}")]
    NotFound(String),
    /// An immutable identity or revision precondition failed.
    #[error("conflict: {0}")]
    Conflict(String),
    /// A required capability is unavailable.
    #[error("unsupported capability: {0}")]
    Unsupported(String),
    /// The request is malformed.
    #[error("invalid request: {0}")]
    Invalid(String),
    /// The caller lacks authority.
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    /// Durable provider operation failed.
    #[error("storage failure: {0}")]
    Storage(String),
    /// Durable admission may have committed and must be reconciled.
    #[error("operation outcome is indeterminate: {0}")]
    Indeterminate(OperationId),
}

/// Harness result type.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_grants_intersect_and_any_deny_wins() {
        let resolved = resolve_policies(&[
            AuthorityPolicy {
                grants: Capabilities::new(["read", "write", "shell"]),
                denies: Capabilities::new(["shell"]),
            },
            AuthorityPolicy {
                grants: Capabilities::new(["read", "write"]),
                denies: Capabilities::new(["write"]),
            },
        ]);
        assert!(resolved.contains("read"));
        assert!(!resolved.contains("write"));
        assert!(!resolved.contains("shell"));
    }

    #[test]
    fn named_policy_layers_require_canonical_hierarchy_order() {
        let policy = AuthorityPolicy {
            grants: Capabilities::new(["read"]),
            denies: Capabilities::default(),
        };
        assert!(
            resolve_policy_layers(&[
                PolicyLayer {
                    level: AuthorityLevel::Runtime,
                    policy: policy.clone()
                },
                PolicyLayer {
                    level: AuthorityLevel::Invocation,
                    policy
                },
            ])
            .is_ok()
        );
        assert!(
            resolve_policy_layers(&[PolicyLayer {
                level: AuthorityLevel::Agent,
                policy: AuthorityPolicy::default()
            },])
            .is_err()
        );
    }
}
