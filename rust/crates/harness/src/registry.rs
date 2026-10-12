//! Immutable component implementation identities.

pub(crate) use crate::contract::validate_component_label;
use serde::{Deserialize, Serialize};

/// Immutable component implementation identity.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ComponentIdentity {
    /// Stable local or namespaced logical name.
    pub name: String,
    /// Semantic implementation version.
    pub version: String,
    /// Implementation/schema digest.
    pub digest: [u8; 32],
}
