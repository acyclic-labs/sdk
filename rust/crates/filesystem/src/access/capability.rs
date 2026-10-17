use super::{Action, InvalidPolicy, ResourcePattern};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Prefix of an owner-issued exact-file read capability.
pub const FILE_READ_PREFIX: &str = "file:read:";
/// Prefix of an owner-issued subtree read capability.
pub const DIRECTORY_READ_PREFIX: &str = "directory:read:";
/// Structured filesystem grants carried in harness signed scopes.
pub const SCOPE_PREFIX: &str = "filesystem:";

/// Whole-volume content operations in harness signed scopes.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VolumeOperation {
    /// Resolve exact file bytes.
    Read,
    /// Create a new file version.
    Write,
}

impl VolumeOperation {
    /// Canonical signed-scope vocabulary, shared by every adapter.
    pub const fn capability_prefix(self) -> &'static str {
        match self {
            Self::Read => "volume:read:",
            Self::Write => "volume:write:",
        }
    }
}

/// One resource-bound grant inside a signed scope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Grant {
    /// Granted actions.
    pub actions: BTreeSet<Action>,
    /// Granted resource set.
    pub resource: ResourcePattern,
}

/// Unsigned payload of a host-verified signed scope, intersected with policy.
/// Each delegation adds a ceiling. No path-pattern subset heuristic is needed:
/// the complete chain remains an intersection, even for overlapping globs.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Capability {
    /// Every layer must grant the operation. Empty means no authority.
    pub layers: Vec<Vec<Grant>>,
    /// Exclusive Unix expiry for the entire chain.
    pub expires_at: Option<u64>,
}

impl Capability {
    /// Encode the complete grant as one capability in an existing signed scope.
    pub fn to_scope_label(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self).map(|json| format!("{SCOPE_PREFIX}{json}"))
    }

    /// Decode a filesystem grant. Authentication remains the scope verifier's job.
    pub fn from_scope_label(label: &str) -> Result<Option<Self>, serde_json::Error> {
        label
            .strip_prefix(SCOPE_PREFIX)
            .map(serde_json::from_str)
            .transpose()
    }

    /// Structural proof of narrowing: retain all parent ceilings, and never
    /// extend their expiry. This intentionally rejects unproven glob rewrites.
    #[must_use]
    pub fn is_attenuation_of(&self, parent: &Self) -> bool {
        self.validate().is_ok()
            && parent.validate().is_ok()
            && if parent.layers.is_empty() {
                self.layers.is_empty() || self.layers.iter().any(Vec::is_empty)
            } else {
                self.layers.starts_with(&parent.layers)
            }
            && parent
                .expires_at
                .is_none_or(|expiry| self.expires_at.is_some_and(|child| child <= expiry))
    }

    /// Add a delegation ceiling. Consumes the parent without copying its grants.
    /// A later requested expiry cannot extend the parent's expiry.
    #[must_use]
    pub fn attenuate(mut self, grants: Vec<Grant>, expires_at: Option<u64>) -> Self {
        // An empty parent must remain empty authority, not become a root grant.
        if self.layers.is_empty() {
            self.layers.push(Vec::new());
        }
        self.layers.push(grants);
        self.expires_at = match (self.expires_at, expires_at) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        self
    }

    /// Check encoding and glob validity before accepting a signed payload.
    pub fn validate(&self) -> Result<(), InvalidPolicy> {
        for grant in self.layers.iter().flatten() {
            grant.resource.validate()?;
        }
        Ok(())
    }
}
