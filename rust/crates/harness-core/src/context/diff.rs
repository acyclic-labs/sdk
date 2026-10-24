//! Exact-path publications from an authorized immutable volume query.
use super::PinnedContextPath;
use crate::resources::GenerationRef;
use serde::{Deserialize, Serialize};

/// One exact path in an authoritative volume query delta, relative to its volume.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiffPath {
    /// Canonical volume-relative path, including any declared subtree boundary.
    pub path: String,
    /// Pinned baseline content; absent for a path created since the fork base.
    pub base: Option<PinnedContextPath>,
    /// Pinned current content; absent for a removed path.
    pub current: Option<PinnedContextPath>,
    /// Largest immutable endpoint length from the authorized query's metadata.
    /// Older publishers may omit it; their bounded reads still enforce limits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub largest_file_bytes: Option<u64>,
    /// True certifies equal immutable content, independent of chunk layout.
    /// False also covers comparisons omitted under the query's finite allowance.
    /// The path is still published so consumers remove a previously changed row.
    #[serde(default)]
    pub matches_base: bool,
}

/// A pushed volume query publication. It carries only paths affected by this
/// publication, while each path's baseline stays pinned to the fork base.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiffPublication {
    /// Previous query generation; a mismatch requires snapshot recovery.
    pub previous: GenerationRef,
    /// Exact published generation, including for deletion-only publications.
    pub generation: GenerationRef,
    /// All affected paths, including both names of a rename and hard-link aliases.
    pub paths: Vec<DiffPath>,
}
