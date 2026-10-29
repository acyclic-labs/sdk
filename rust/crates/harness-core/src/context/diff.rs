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

/// Captured file status, independent of any terminal renderer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(rename_all = "snake_case")]
pub enum DiffKind {
    /// Path did not exist at the fork base.
    Added,
    /// Path no longer exists.
    Removed,
    /// Both endpoints exist and differ.
    Modified,
}

/// Exclusive position in one exact diff projection. A publication invalidates
/// its old positions; callers reopen at the new generation rather than mixing
/// old file offsets with a new patch.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
#[serde(deny_unknown_fields)]
pub struct DiffCursor {
    /// Digest of the exact authorized volume/base/publication projection.
    pub publication: [u8; 32],
    /// Exact volume-relative file at this position.
    pub path: String,
    /// None means the file header has not been delivered yet.
    pub offset: Option<usize>,
}

/// A visible fragment of one file, suitable for any frontend. Metadata remains
/// the whole file's; only `patch` is a fragment. No endpoint is read to page a
/// projection, and no preceding file or patch prefix is scanned again.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
#[serde(deny_unknown_fields)]
pub struct DiffFilePage {
    /// Exact projection that produced this fragment, including terminal pages.
    pub publication: [u8; 32],
    /// Exact volume-relative path.
    pub path: String,
    /// Change at the pinned endpoints.
    pub kind: DiffKind,
    /// Whole-file line counts, independent of this visible fragment.
    pub added: u64,
    /// Whole-file line counts, independent of this visible fragment.
    pub removed: u64,
    /// Binary files have no text patch.
    pub binary: bool,
    /// Content omitted before reading under the query's file-byte allowance.
    pub omitted_bytes: Option<u64>,
    /// Whether this file's header was delivered on an earlier page.
    pub continued: bool,
    /// At most the requested UTF-8 patch bytes and visible rows.
    pub patch: String,
    /// Exact exclusive continuation, or completion of this projection.
    pub next: Option<DiffCursor>,
}
