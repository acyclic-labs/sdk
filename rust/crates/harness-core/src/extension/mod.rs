//! Native extension identities and retained admissions.
use crate::{
    Error, Result,
    core::{ExtensionAdmission, ExtensionDependency, Reducer, validate_extension_name},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};
mod registry;
mod runtime;
pub use registry::{ExtensionLease, ExtensionRegistry};
pub use runtime::ExtensionRuntime;
#[cfg(test)]
mod tests;

/// Exact executable identity for one installed extension version.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ExtensionIdentity {
    /// Namespaced extension name.
    pub name: String,
    /// Positive extension version.
    pub version: u32,
    /// Digest of the linked implementation and its executable contract.
    pub digest: [u8; 32],
}

impl ExtensionIdentity {
    /// Creates and validates an exact executable identity.
    pub fn new(name: impl Into<String>, version: u32, digest: [u8; 32]) -> Result<Self> {
        let identity = Self {
            name: name.into(),
            version,
            digest,
        };
        identity.validate()?;
        Ok(identity)
    }

    /// Validates the identity without consulting an installation registry.
    pub fn validate(&self) -> Result<()> {
        validate_extension_name(&self.name)?;
        if self.version == 0 || self.digest == [0; 32] {
            return Err(Error::Invalid(
                "extension version or digest is empty".into(),
            ));
        }
        Ok(())
    }
}

/// Process-local implementation retained by admitted operations.
pub trait NativeExtension: Send + Sync {
    /// Immutable executable identity.
    fn identity(&self) -> ExtensionIdentity;
    /// Exact installed dependencies.
    fn dependencies(&self) -> Vec<ExtensionDependency> {
        Vec::new()
    }
    /// Called after removal when no admission retains the implementation.
    fn dispose(&self) {}
}
