//! Immutable provenance references retained by agent and extension records.
use super::*;

/// Reference to an exact event in another aggregate.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub struct EventReference {
    /// Referenced authority.
    pub authority: Authority,
    /// Exact referenced revision.
    pub revision: u64,
}

crate::core::ownership::owned_struct!(EventReference {
    authority,
    revision
});
