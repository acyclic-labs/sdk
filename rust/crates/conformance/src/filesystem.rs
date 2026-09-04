//! Generated workload-shape coverage for backend and platform qualification.
//!
//! The current manifest generator does not pretend that an infinite
//! operation-history space can be enumerated. It exhausts every declared level
//! and every pair of independent dimensions. Executable backend runners,
//! receipts, model histories, fuzzing, crash cuts, and bounded stress orchestration remain
//! release-blocking work recorded in the guarantee ledger.

use serde::Serialize;
use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

/// One named dimension of filesystem workload behavior.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct Dimension {
    /// Stable machine-readable dimension name.
    pub name: &'static str,
    /// Stable machine-readable equivalence classes and boundaries.
    pub levels: &'static [&'static str],
}

/// Canonical workload dimensions. Adding a level automatically expands all
/// single-axis and pairwise qualification cases.
pub const DIMENSIONS: &[Dimension] = &[
    Dimension {
        name: "file_layout",
        levels: &[
            "empty",
            "tiny",
            "inline_63_bytes",
            "inline_64_bytes",
            "inline_65_bytes",
            "chunk_minus_one",
            "chunk_exact",
            "chunk_plus_one",
            "large_sequential",
            "large_random",
            "hole_dominant",
            "allocated_zero",
            "alternating_sparse",
            "maximum_logical_size",
        ],
    },
    Dimension {
        name: "file_kind",
        levels: &[
            "regular",
            "directory",
            "symlink",
            "hardlink_alias",
            "fifo",
            "socket",
            "block_device",
            "character_device",
            "windows_reparse_point",
            "unsupported_explicit_rejection",
        ],
    },
    Dimension {
        name: "payload_shape",
        levels: &[
            "all_zero",
            "low_entropy",
            "high_entropy",
            "repeated_chunks",
            "shared_prefix",
            "shared_suffix",
            "embedded_nul",
            "text_lf",
            "text_crlf",
            "invalid_text_bytes",
        ],
    },
    Dimension {
        name: "file_access_pattern",
        levels: &[
            "sequential_forward",
            "sequential_reverse",
            "uniform_random",
            "skewed_random",
            "strided",
            "overlapping",
            "unaligned",
            "boundary_crossing",
            "repeated_hot_range",
        ],
    },
    Dimension {
        name: "access_api_shape",
        levels: &[
            "scalar_buffered",
            "vectored",
            "streaming_pull",
            "streaming_push",
            "memory_mapped_read",
            "private_mapped_write",
            "direct_io_aligned",
            "direct_io_unaligned_rejection",
            "async_single",
            "async_pipeline",
            "cancelled_in_flight",
        ],
    },
    Dimension {
        name: "api_semantics",
        levels: &[
            "path_no_follow",
            "path_follow",
            "fd_relative",
            "directory_handle_cursor",
            "open_then_rename",
            "open_then_unlink",
            "synchronous_completion",
            "asynchronous_immediate",
            "asynchronous_suspended",
            "retry_same_identity",
            "retry_identity_conflict",
        ],
    },
    Dimension {
        name: "working_set_shape",
        levels: &[
            "fits_inline",
            "fits_one_page",
            "fits_memory_cache",
            "fits_local_disk_cache",
            "exceeds_cache",
            "single_hotspot",
            "zipf_hotset",
            "uniform",
            "stream_once",
            "cyclic_scan",
        ],
    },
    Dimension {
        name: "tree_layout",
        levels: &[
            "empty",
            "single",
            "shallow_wide",
            "deep_narrow",
            "mixed",
            "million_entry",
            "metadata_dense",
            "hardlink_dense",
            "symlink_dense",
            "case_unicode_adversarial",
        ],
    },
    Dimension {
        name: "path_shape",
        levels: &[
            "root",
            "one_component",
            "deep",
            "maximum_depth",
            "minimum_name",
            "maximum_name",
            "common_prefix_names",
            "case_collision",
            "unicode_normalization_collision",
            "reserved_platform_name",
            "raw_posix_name",
            "traversal_attempt",
        ],
    },
    Dimension {
        name: "repository_shape",
        levels: &[
            "unversioned_workspace",
            "small_source_repo",
            "many_tiny_files_monorepo",
            "deep_package_monorepo",
            "large_binary_repo",
            "model_checkpoint_tree",
            "generated_build_tree",
            "dependency_cache",
            "git_worktree",
            "jj_working_copy",
            "rename_heavy_history",
            "mixed_source_binary_sparse",
        ],
    },
    Dimension {
        name: "operation",
        levels: &[
            "open",
            "close",
            "lookup",
            "batch_lookup",
            "stat",
            "batch_stat",
            "list_page",
            "walk",
            "read_range",
            "read_sequential",
            "query_sparse_ranges",
            "hash_content",
            "prefetch",
            "readlink",
            "map_read",
            "map_write",
            "create",
            "mkdir",
            "symlink",
            "overwrite",
            "append",
            "truncate",
            "allocate",
            "hole_punch",
            "clone",
            "copy_range",
            "link",
            "unlink",
            "rmdir",
            "rename",
            "metadata_change",
            "attribute_get",
            "attribute_set",
            "attribute_list",
            "attribute_remove",
            "flush",
            "sync",
            "watch",
            "checkpoint",
            "commit",
            "diff",
            "merge",
            "rebase",
            "refresh",
            "discard",
            "export",
            "import",
            "materialize",
            "capture",
            "mount",
            "unmount",
            "lock_range",
            "unlock_range",
            "garbage_collect",
            "recover",
        ],
    },
    Dimension {
        name: "operation_mix",
        levels: &[
            "single",
            "homogeneous_batch",
            "mixed_batch",
            "burst",
            "long_history",
        ],
    },
    Dimension {
        name: "operation_sequence",
        levels: &[
            "single",
            "create_write_read",
            "open_rename_read",
            "open_unlink_read",
            "checkpoint_branch_merge",
            "refresh_safe_rebase",
            "refresh_conflicting_rebase",
            "export_import_roundtrip",
            "cancel_then_retry",
            "crash_before_object_publication",
            "crash_after_objects_before_authority",
            "ambiguous_commit_then_replay",
            "concurrent_generation_cas",
            "speculative_residency",
            "speculative_promotion",
        ],
    },
    Dimension {
        name: "transaction_shape",
        levels: &[
            "single_path",
            "shared_prefix_batch",
            "disjoint_path_batch",
            "mixed_namespace_content_metadata",
            "large_atomic_batch",
            "semantic_noop_batch",
            "precondition_failure_early",
            "precondition_failure_late",
            "retry_same_operation",
            "retry_conflicting_operation",
        ],
    },
    Dimension {
        name: "mutation_geometry",
        levels: &[
            "front_insert",
            "middle_insert",
            "tail_append",
            "front_delete",
            "middle_delete",
            "tail_truncate",
            "same_leaf_many_edits",
            "cross_leaf_edits",
            "split_boundary",
            "merge_boundary",
            "tree_height_growth",
            "tree_height_shrink",
            "overlapping_ranges",
            "disjoint_ranges",
        ],
    },
    Dimension {
        name: "handle_lifetime",
        levels: &[
            "single_call",
            "short_lived",
            "long_lived",
            "reopened",
            "renamed_while_open",
            "unlinked_while_open",
            "generation_advanced",
            "mount_fenced",
            "cancelled",
            "abandoned_after_crash",
        ],
    },
    Dimension {
        name: "operation_size",
        levels: &[
            "zero",
            "one",
            "page_minus_one",
            "page_exact",
            "page_plus_one",
            "chunk_minus_one",
            "chunk_exact",
            "chunk_plus_one",
            "multi_page",
            "maximum_admitted",
            "maximum_plus_one_rejection",
        ],
    },
    Dimension {
        name: "locality",
        levels: &[
            "same_path",
            "same_subtree",
            "disjoint_subtrees",
            "whole_volume",
        ],
    },
    Dimension {
        name: "cache_state",
        levels: &["empty", "cold", "warm", "skewed_hot", "thrashing"],
    },
    Dimension {
        name: "cache_mechanics",
        levels: &[
            "disabled",
            "single_hit",
            "single_miss",
            "coalesced_miss",
            "capacity_minus_one",
            "capacity_exact",
            "capacity_plus_one",
            "lru_eviction",
            "generation_invalidation",
            "authority_fence_invalidation",
        ],
    },
    Dimension {
        name: "storage_locality",
        levels: &[
            "same_process_memory",
            "same_process_disk",
            "same_machine_service",
            "local_network",
            "wide_area",
            "offline_cached",
            "temporarily_unavailable",
        ],
    },
    Dimension {
        name: "storage_media",
        levels: &[
            "dram",
            "nvme",
            "ssd",
            "hdd",
            "network_block",
            "object_store",
            "browser_indexeddb",
            "browser_opfs",
        ],
    },
    Dimension {
        name: "storage_geometry",
        levels: &[
            "default",
            "small_sector",
            "large_sector",
            "small_block",
            "large_block",
            "unaligned_range",
            "queue_depth_one",
            "queue_depth_saturated",
            "reflink_available",
            "reflink_unavailable",
            "sparse_available",
            "sparse_unavailable",
        ],
    },
    Dimension {
        name: "resource_pressure",
        levels: &[
            "idle",
            "memory_constrained",
            "io_constrained",
            "cpu_constrained",
            "descriptor_constrained",
            "quota_near_limit",
            "gc_pressure",
            "cancellation_pressure",
        ],
    },
    Dimension {
        name: "execution_pressure",
        levels: &[
            "none",
            "one_descriptor_remaining",
            "descriptor_exhausted",
            "one_byte_memory_remaining",
            "allocator_fragmented",
            "queue_depth_one",
            "queue_saturated",
            "numa_local",
            "numa_remote",
            "thermal_throttled",
        ],
    },
    Dimension {
        name: "concurrency",
        levels: &[
            "single",
            "readers",
            "disjoint_writers",
            "contended_writers",
            "read_write",
            "multi_process",
            "multi_tab",
        ],
    },
    Dimension {
        name: "consistency",
        levels: &["pinned", "tracking_safe", "live", "manual_refresh"],
    },
    Dimension {
        name: "volume_access",
        levels: &["read_only", "writable"],
    },
    Dimension {
        name: "writer_authority",
        levels: &[
            "exclusive_writer",
            "optimistic_generation_cas",
            "serialized_authority",
        ],
    },
    Dimension {
        name: "checkout_behavior",
        levels: &["private_cow_overlay", "direct_serialized_live_mutation"],
    },
    Dimension {
        name: "volume_lifecycle",
        levels: &["ephemeral", "durable"],
    },
    Dimension {
        name: "volume_topology",
        levels: &[
            "single_private",
            "single_shared",
            "multiple_disjoint",
            "nested_mounts",
            "many_workspaces",
            "workspace_plus_scratches",
        ],
    },
    Dimension {
        name: "repository_history",
        levels: &[
            "fresh",
            "linear_deep",
            "branching",
            "merge_heavy",
            "rewrite_heavy",
            "rename_heavy",
            "create_delete_churn",
        ],
    },
    Dimension {
        name: "durability_state",
        levels: &[
            "clean",
            "objects_pending",
            "authority_pending",
            "ambiguous_barrier",
            "torn_tail",
            "snapshot_plus_tail",
            "compaction_boundary",
            "orphan_objects",
            "corrupt_object",
            "corrupt_authority_record",
        ],
    },
    Dimension {
        name: "backend",
        levels: &[
            "memory",
            "local",
            "indexeddb",
            "indexeddb_opfs",
            "simulated_remote",
        ],
    },
    Dimension {
        name: "backend_capability_profile",
        levels: &[
            "minimal_event_and_object",
            "conditional_append",
            "bounded_batch_replay",
            "wake_notifications",
            "batch_exists",
            "batch_object_io",
            "authenticated_range_read",
            "multi_range_read",
            "streaming_backpressure",
            "shared_immutable_view",
            "ownership_transfer",
            "strict_durability",
            "sparse_native",
            "clone_offload",
        ],
    },
    Dimension {
        name: "platform_profile",
        levels: &[
            "portable",
            "posix",
            "windows",
            "browser",
            "linux_fuse",
            "macos_nfs",
            "windows_projfs",
        ],
    },
    Dimension {
        name: "platform_filesystem",
        levels: &[
            "portable_model",
            "windows_ntfs",
            "windows_refs",
            "linux_ext4",
            "linux_xfs",
            "linux_btrfs",
            "macos_apfs",
            "browser_indexeddb",
            "browser_opfs",
            "network_filesystem",
        ],
    },
    Dimension {
        name: "consumer_surface",
        levels: &["rust", "wasm", "native_mount", "daemon"],
    },
    Dimension {
        name: "failure",
        levels: &[
            "none",
            "cancel",
            "deadline",
            "quota",
            "short_read",
            "partial_write",
            "corruption",
            "stale_read",
            "delayed_visibility",
            "duplicate_response",
            "ambiguous_commit",
            "fence_change",
            "process_kill",
            "machine_restart",
        ],
    },
];

/// One deterministic pairwise qualification case. Unselected dimensions use
/// their first baseline level.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct WorkloadCase {
    /// Level index for every entry in [`DIMENSIONS`].
    pub levels: Vec<usize>,
}

/// Locked BLAKE3 identity of the ordered taxonomy and generated case sequence.
pub const WORKLOAD_CORPUS_DIGEST: &str =
    "b1e9d1d438350fcc849459e2cf8d4b56c315113de9598872a81e3fb1eb0c1c78";

/// Returns the locked corpus identity used by indexed selectors.
#[must_use]
pub fn workload_corpus_digest() -> String {
    WORKLOAD_CORPUS_DIGEST.to_owned()
}

#[cfg(test)]
fn compute_workload_corpus_digest() -> String {
    let mut hasher = blake3::Hasher::new();
    digest_component(&mut hasher, b"acyclic-fs-pairwise-workload-corpus-v2");
    digest_component(&mut hasher, b"pairwise-btreeset-generator-v1");
    for dimension in DIMENSIONS {
        digest_component(&mut hasher, dimension.name.as_bytes());
        for level in dimension.levels {
            digest_component(&mut hasher, level.as_bytes());
        }
        digest_component(&mut hasher, b"dimension-end");
    }
    for case in pairwise_cases() {
        for level in case.levels {
            digest_component(
                &mut hasher,
                &u64::try_from(level).unwrap_or(u64::MAX).to_le_bytes(),
            );
        }
        digest_component(&mut hasher, b"case-end");
    }
    hasher.finalize().to_hex().to_string()
}

#[cfg(test)]
fn digest_component(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&u64::try_from(value.len()).unwrap_or(u64::MAX).to_le_bytes());
    hasher.update(value);
}

/// One canonical filter over the deterministic workload corpus.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum WorkloadSelector {
    /// Select exactly one zero-based position bound to one corpus identity.
    Case {
        /// BLAKE3 identity of the exact taxonomy that defines the index.
        corpus: String,
        /// Zero-based position in the canonical generated corpus.
        index: usize,
    },
    /// Select every case containing one named dimension level.
    Level {
        /// Stable dimension name from [`DIMENSIONS`].
        dimension: &'static str,
        /// Stable level name from the selected dimension.
        level: &'static str,
    },
}

impl fmt::Display for WorkloadSelector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Case { corpus, index } => write!(formatter, "case:{corpus}:{index}"),
            Self::Level { dimension, level } => write!(formatter, "{dimension}={level}"),
        }
    }
}

/// Typed rejection for malformed, contradictory, or empty workload selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SelectorError {
    /// The selector does not use `case:<corpus-digest>:<index>` or
    /// `<dimension>=<level>`.
    Malformed(String),
    /// The dimension name is not declared by [`DIMENSIONS`].
    UnknownDimension(String),
    /// The level name is not declared by the selected dimension.
    UnknownLevel {
        /// Declared dimension whose level was not found.
        dimension: String,
        /// Rejected level name.
        level: String,
    },
    /// The zero-based case index is outside the generated corpus.
    UnknownCase(usize),
    /// An indexed selector names another corpus identity.
    CorpusMismatch {
        /// Identity required by this build.
        expected: String,
        /// Identity supplied by the selector.
        actual: String,
    },
    /// One canonical selector was supplied more than once.
    Duplicate(String),
    /// Two filters constrain the same dimension or case identity differently.
    Contradictory(String),
    /// No generated case satisfies the complete filter.
    EmptySelection,
    /// A generated or selected result would exceed an admitted bound.
    LimitExceeded {
        /// Bounded resource or result class.
        what: &'static str,
        /// Caller-admitted maximum.
        limit: usize,
        /// Required amount known before the rejected growth.
        actual: usize,
    },
}

impl fmt::Display for SelectorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed(value) => write!(formatter, "malformed workload selector: {value}"),
            Self::UnknownDimension(value) => {
                write!(formatter, "unknown workload dimension: {value}")
            }
            Self::UnknownLevel { dimension, level } => {
                write!(
                    formatter,
                    "unknown level {level} for workload dimension {dimension}"
                )
            }
            Self::UnknownCase(index) => write!(formatter, "unknown workload case: {index}"),
            Self::CorpusMismatch { expected, actual } => write!(
                formatter,
                "workload selector corpus mismatch: expected {expected}, received {actual}"
            ),
            Self::Duplicate(value) => write!(formatter, "duplicate workload selector: {value}"),
            Self::Contradictory(value) => {
                write!(formatter, "contradictory workload selector: {value}")
            }
            Self::EmptySelection => formatter.write_str("workload selectors matched no cases"),
            Self::LimitExceeded {
                what,
                limit,
                actual,
            } => write!(
                formatter,
                "workload {what} requires {actual}, exceeding admitted limit {limit}"
            ),
        }
    }
}

impl std::error::Error for SelectorError {}

impl FromStr for WorkloadSelector {
    type Err = SelectorError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if let Some(case) = value.strip_prefix("case:") {
            let Some((corpus, index)) = case.split_once(':') else {
                return Err(SelectorError::Malformed(value.to_owned()));
            };
            if index.is_empty()
                || (index.len() > 1 && index.starts_with('0'))
                || !index.bytes().all(|byte| byte.is_ascii_digit())
            {
                return Err(SelectorError::Malformed(value.to_owned()));
            }
            if corpus.len() != 64
                || !corpus
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Err(SelectorError::Malformed(value.to_owned()));
            }
            return index
                .parse()
                .map(|index| Self::Case {
                    corpus: corpus.to_owned(),
                    index,
                })
                .map_err(|_| SelectorError::Malformed(value.to_owned()));
        }
        let Some((dimension_name, level_name)) = value.split_once('=') else {
            return Err(SelectorError::Malformed(value.to_owned()));
        };
        if dimension_name.is_empty() || level_name.is_empty() || level_name.contains('=') {
            return Err(SelectorError::Malformed(value.to_owned()));
        }
        let dimension = DIMENSIONS
            .iter()
            .find(|candidate| candidate.name == dimension_name)
            .ok_or_else(|| SelectorError::UnknownDimension(dimension_name.to_owned()))?;
        let level = dimension
            .levels
            .iter()
            .copied()
            .find(|candidate| *candidate == level_name)
            .ok_or_else(|| SelectorError::UnknownLevel {
                dimension: dimension_name.to_owned(),
                level: level_name.to_owned(),
            })?;
        Ok(Self::Level {
            dimension: dimension.name,
            level,
        })
    }
}

/// Typed rejection from the shared `--selector <value>` CLI grammar.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SelectorArgumentError {
    /// Arguments were empty, incomplete, or used another flag.
    Usage,
    /// More than one case plus one selector per dimension was supplied.
    TooMany,
    /// One selector value was malformed or unknown.
    Selector(SelectorError),
}

impl fmt::Display for SelectorArgumentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage => formatter
                .write_str("usage: --selector <case:corpus-digest:index|dimension=level> [...]"),
            Self::TooMany => formatter
                .write_str("workload selector count exceeds one case plus one per dimension"),
            Self::Selector(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for SelectorArgumentError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Selector(error) => Some(error),
            Self::Usage | Self::TooMany => None,
        }
    }
}

/// Parses the shared bounded repeated-selector command-line grammar.
///
/// # Errors
///
/// Returns a typed error for empty/incomplete arguments, another flag, too
/// many selectors, or a malformed selector value.
pub fn parse_workload_selector_arguments(
    arguments: impl IntoIterator<Item = String>,
) -> Result<Vec<WorkloadSelector>, SelectorArgumentError> {
    let mut arguments = arguments.into_iter();
    let mut selectors = Vec::with_capacity(DIMENSIONS.len() + 1);
    while let Some(flag) = arguments.next() {
        if flag != "--selector" {
            return Err(SelectorArgumentError::Usage);
        }
        let value = arguments.next().ok_or(SelectorArgumentError::Usage)?;
        if selectors.len() == DIMENSIONS.len() + 1 {
            return Err(SelectorArgumentError::TooMany);
        }
        selectors.push(value.parse().map_err(SelectorArgumentError::Selector)?);
    }
    if selectors.is_empty() {
        return Err(SelectorArgumentError::Usage);
    }
    Ok(selectors)
}

/// Target complexity and plan constraints for one operation equivalence class.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct OperationContract {
    /// Operation level from the `operation` dimension.
    pub operation: &'static str,
    /// Dominant admitted work expressed using documented workload variables.
    pub dominant_work: &'static str,
    /// Required physical-plan properties or accelerations.
    pub required_plans: &'static [&'static str],
    /// Behavior that must never occur implicitly.
    pub forbidden: &'static [&'static str],
}

/// One mandatory higher-order interaction family beyond the pairwise floor.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct InteractionFamily {
    /// Stable receipt and runner name.
    pub name: &'static str,
    /// Exactly three independently varied dimension names.
    pub dimensions: [&'static str; 3],
}

/// Targeted three-way families where pairwise selection cannot expose the
/// relevant planner, durability, cache-pressure, or platform interaction.
pub const TARGETED_INTERACTIONS: &[InteractionFamily] = &[
    InteractionFamily {
        name: "operation_shape_backend",
        dimensions: ["operation", "repository_shape", "backend"],
    },
    InteractionFamily {
        name: "operation_failure_durability",
        dimensions: ["operation", "failure", "durability_state"],
    },
    InteractionFamily {
        name: "api_cache_pressure",
        dimensions: ["api_semantics", "cache_mechanics", "execution_pressure"],
    },
    InteractionFamily {
        name: "platform_capability_consumer",
        dimensions: [
            "platform_filesystem",
            "backend_capability_profile",
            "consumer_surface",
        ],
    },
    InteractionFamily {
        name: "sequence_concurrency_consistency",
        dimensions: ["operation_sequence", "concurrency", "consistency"],
    },
    InteractionFamily {
        name: "access_geometry_working_set",
        dimensions: [
            "file_access_pattern",
            "storage_geometry",
            "working_set_shape",
        ],
    },
];

/// One deterministic Cartesian case from a mandatory three-way interaction
/// family. Level indices address the dimensions named by the selected family
/// in their declared order.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct TargetedInteractionCase {
    /// Zero-based family position in [`TARGETED_INTERACTIONS`].
    pub family_index: usize,
    /// One level index for each of the family's three dimensions.
    pub levels: [usize; 3],
}

/// Human-readable resolution of one targeted interaction case. The ordinal is
/// globally stable only within the locked corpus digest.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct ResolvedTargetedInteractionCase {
    /// Zero-based position in [`targeted_interaction_cases`].
    pub ordinal: usize,
    /// Stable targeted family name.
    pub family: &'static str,
    /// Exact dimension and level selected at each family position.
    pub dimensions: [TargetedDimensionLevel; 3],
}

/// One resolved dimension-level fact in a targeted interaction case.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct TargetedDimensionLevel {
    /// Stable dimension name.
    pub dimension: &'static str,
    /// Stable selected level name.
    pub level: &'static str,
}

/// Fail-closed limits and deterministic modulo shard for targeted execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TargetedInteractionShard {
    /// Zero-based shard selected from `shard_count`.
    pub shard_index: usize,
    /// Total number of shards partitioning global corpus ordinals.
    pub shard_count: usize,
    /// Maximum selected cases admitted before allocation.
    pub maximum_cases: usize,
}

/// Typed rejection from targeted family resolution or sharding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TargetedInteractionSelectionError {
    /// Requested family is not declared by [`TARGETED_INTERACTIONS`].
    UnknownFamily(String),
    /// Shard count was zero or the index was outside the declared count.
    InvalidShard {
        /// Rejected zero-based shard index.
        index: usize,
        /// Declared shard count.
        count: usize,
    },
    /// The selected shard exceeds the caller-admitted case limit.
    LimitExceeded {
        /// Caller-admitted selected case count.
        limit: usize,
        /// First required count above the limit.
        actual: usize,
    },
    /// An internally generated case did not resolve against the taxonomy.
    InvalidCorpusCase {
        /// Global corpus ordinal that could not be resolved.
        ordinal: usize,
    },
}

impl fmt::Display for TargetedInteractionSelectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownFamily(family) => {
                write!(formatter, "unknown targeted interaction family: {family}")
            }
            Self::InvalidShard { index, count } => {
                write!(formatter, "invalid targeted shard {index} of {count}")
            }
            Self::LimitExceeded { limit, actual } => write!(
                formatter,
                "targeted interaction shard requires {actual} cases, exceeding {limit}"
            ),
            Self::InvalidCorpusCase { ordinal } => {
                write!(formatter, "targeted interaction case {ordinal} is invalid")
            }
        }
    }
}

impl std::error::Error for TargetedInteractionSelectionError {}

/// Locked BLAKE3 identity of every targeted three-way Cartesian case.
pub const TARGETED_INTERACTION_CORPUS_DIGEST: &str =
    "64f53d9363ac120394e7a88df81566e38c566b5da31f07b4759d385c8c352e59";

/// Returns every mandatory targeted interaction in stable family and level
/// order. Unlike the broad pairwise corpus, each named family is the complete
/// Cartesian product of all levels in its three dimensions.
#[must_use]
pub fn targeted_interaction_cases() -> Vec<TargetedInteractionCase> {
    let mut cases = Vec::new();
    for (family_index, family) in TARGETED_INTERACTIONS.iter().enumerate() {
        let dimensions = family.dimensions.map(|name| {
            DIMENSIONS
                .iter()
                .find(|dimension| dimension.name == name)
                .map_or(&[][..], |dimension| dimension.levels)
        });
        for first in 0..dimensions[0].len() {
            for second in 0..dimensions[1].len() {
                for third in 0..dimensions[2].len() {
                    cases.push(TargetedInteractionCase {
                        family_index,
                        levels: [first, second, third],
                    });
                }
            }
        }
    }
    cases
}

/// Resolves and shards targeted cases without changing their global ordinals.
/// A family filter is exact and sharding always uses `ordinal % shard_count`,
/// so independently executed receipts can be recomposed without ambiguity.
///
/// # Errors
///
/// Returns a typed error for an unknown family, invalid shard, selected count
/// above the caller's bound, or an inconsistent generated corpus case.
pub fn select_targeted_interaction_cases(
    family: Option<&str>,
    shard: TargetedInteractionShard,
) -> Result<Vec<ResolvedTargetedInteractionCase>, TargetedInteractionSelectionError> {
    if shard.shard_count == 0 || shard.shard_index >= shard.shard_count {
        return Err(TargetedInteractionSelectionError::InvalidShard {
            index: shard.shard_index,
            count: shard.shard_count,
        });
    }
    if let Some(family) = family
        && !TARGETED_INTERACTIONS
            .iter()
            .any(|candidate| candidate.name == family)
    {
        return Err(TargetedInteractionSelectionError::UnknownFamily(
            family.to_owned(),
        ));
    }
    let mut selected = Vec::new();
    for (ordinal, case) in targeted_interaction_cases().into_iter().enumerate() {
        let resolved = resolve_targeted_interaction_case(ordinal, case)?;
        if family.is_none_or(|family| resolved.family == family)
            && ordinal % shard.shard_count == shard.shard_index
        {
            let actual = selected.len().saturating_add(1);
            if actual > shard.maximum_cases {
                return Err(TargetedInteractionSelectionError::LimitExceeded {
                    limit: shard.maximum_cases,
                    actual,
                });
            }
            selected.push(resolved);
        }
    }
    Ok(selected)
}

fn resolve_targeted_interaction_case(
    ordinal: usize,
    case: TargetedInteractionCase,
) -> Result<ResolvedTargetedInteractionCase, TargetedInteractionSelectionError> {
    let family = TARGETED_INTERACTIONS
        .get(case.family_index)
        .ok_or(TargetedInteractionSelectionError::InvalidCorpusCase { ordinal })?;
    let mut dimensions = [TargetedDimensionLevel {
        dimension: "",
        level: "",
    }; 3];
    for (position, resolved_dimension) in dimensions.iter_mut().enumerate() {
        let dimension_name = family.dimensions[position];
        let dimension = DIMENSIONS
            .iter()
            .find(|candidate| candidate.name == dimension_name)
            .ok_or(TargetedInteractionSelectionError::InvalidCorpusCase { ordinal })?;
        let level = dimension
            .levels
            .get(case.levels[position])
            .ok_or(TargetedInteractionSelectionError::InvalidCorpusCase { ordinal })?;
        *resolved_dimension = TargetedDimensionLevel {
            dimension: dimension_name,
            level,
        };
    }
    Ok(ResolvedTargetedInteractionCase {
        ordinal,
        family: family.name,
        dimensions,
    })
}

#[cfg(test)]
fn compute_targeted_interaction_corpus_digest() -> String {
    let mut hasher = blake3::Hasher::new();
    targeted_digest_component(&mut hasher, b"acyclic-fs-targeted-interaction-corpus-v1");
    for family in TARGETED_INTERACTIONS {
        targeted_digest_component(&mut hasher, family.name.as_bytes());
        for dimension_name in family.dimensions {
            targeted_digest_component(&mut hasher, dimension_name.as_bytes());
            if let Some(dimension) = DIMENSIONS
                .iter()
                .find(|dimension| dimension.name == dimension_name)
            {
                for level in dimension.levels {
                    targeted_digest_component(&mut hasher, level.as_bytes());
                }
            }
        }
        targeted_digest_component(&mut hasher, b"family-end");
    }
    for case in targeted_interaction_cases() {
        hasher.update(
            &u64::try_from(case.family_index)
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        for level in case.levels {
            hasher.update(&u64::try_from(level).unwrap_or(u64::MAX).to_le_bytes());
        }
        targeted_digest_component(&mut hasher, b"case-end");
    }
    hasher.finalize().to_hex().to_string()
}

#[cfg(test)]
fn targeted_digest_component(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&u64::try_from(value.len()).unwrap_or(u64::MAX).to_le_bytes());
    hasher.update(value);
}

const NO_SCAN: &[&str] = &["full_tree_scan", "full_file_materialization"];
const NO_UNBOUNDED: &[&str] = &[
    "unbounded_allocation",
    "unbounded_backend_request",
    "silent_semantic_degradation",
];

/// Machine-owned target contracts for every declared operation shape.
pub const OPERATION_CONTRACTS: &[OperationContract] = &[
    contract(
        "open",
        "O(path_depth + selected_metadata_frontier)",
        &["generation_fenced_handle", "lazy_data_access"],
        NO_SCAN,
    ),
    contract(
        "close",
        "O(handle_dirty_state)",
        &["bounded_dirty_flush", "idempotent_handle_release"],
        NO_UNBOUNDED,
    ),
    contract(
        "lookup",
        "O(path_depth * page_frontier)",
        &["authenticated_frontier", "binary_page_search"],
        NO_SCAN,
    ),
    contract(
        "batch_lookup",
        "O(shared_path_frontiers + requested_paths * log(page_items))",
        &["prefix_compiled_batch", "single_flight_shared_pages"],
        &["one_root_walk_per_path", "full_tree_scan"],
    ),
    contract(
        "stat",
        "O(path_depth + file_table_height)",
        &["authenticated_frontier", "path_independent_file_record"],
        NO_SCAN,
    ),
    contract(
        "batch_stat",
        "O(shared_path_frontiers + distinct_file_table_frontiers)",
        &["prefix_compiled_batch", "grouped_file_id_lookup"],
        &["one_file_table_walk_per_path", "full_tree_scan"],
    ),
    contract(
        "list_page",
        "O(path_depth + intersecting_pages + output_items)",
        &["bounded_cursor", "continuation_witness"],
        NO_SCAN,
    ),
    contract(
        "walk",
        "O(selected_namespace_pages + output_items)",
        &["bounded_streaming_cursor", "subtree_selection_pruning"],
        &["unbounded_result_materialization", "implicit_content_read"],
    ),
    contract(
        "read_range",
        "O(extent_height + intersecting_extents + requested_bytes)",
        &["extent_frontier", "range_object_reads", "zero_synthesis"],
        NO_SCAN,
    ),
    contract(
        "read_sequential",
        "O(intersecting_extents + requested_bytes)",
        &[
            "vectored_ranges",
            "bounded_readahead",
            "zero_copy_when_proved",
        ],
        NO_UNBOUNDED,
    ),
    contract(
        "query_sparse_ranges",
        "O(extent_height + intersecting_extents + output_ranges)",
        &["extent_frontier", "bounded_range_cursor"],
        &["blob_body_read", "hole_materialization"],
    ),
    contract(
        "hash_content",
        "O(1) authenticated witness; O(intersecting_extents + logical_bytes) streamed fallback",
        &["layout_independent_digest", "bounded_streaming_hash"],
        &["whole_file_materialization", "layout_dependent_result"],
    ),
    contract(
        "prefetch",
        "O(selected_frontiers + admitted_range_bytes)",
        &[
            "bounded_cancellable_hint",
            "generation_fenced_cache_admission",
        ],
        &["semantic_result_change", "unbounded_readahead"],
    ),
    contract(
        "readlink",
        "O(path_depth + link_bytes)",
        &["bounded_link_payload", "no_implicit_link_follow"],
        NO_SCAN,
    ),
    contract(
        "map_read",
        "O(intersecting_pages + faulted_bytes)",
        &["generation_fenced_mapping", "bounded_fault_handler"],
        &["whole_file_materialization", "unfenced_stale_mapping"],
    ),
    contract(
        "map_write",
        "O(dirtied_pages + committed_bytes)",
        &["private_cow_mapping", "bounded_dirty_page_capture"],
        &[
            "implicit_cross_generation_write",
            "unbounded_dirty_tracking",
        ],
    ),
    contract(
        "create",
        "O(path_depth + file_table_height)",
        &["shared_prefix_plan", "persistent_path_copy"],
        NO_SCAN,
    ),
    contract(
        "mkdir",
        "O(path_depth + file_table_height)",
        &["shared_prefix_plan", "persistent_path_copy"],
        NO_SCAN,
    ),
    contract(
        "symlink",
        "O(path_depth + link_bytes)",
        &["bounded_link_payload", "persistent_path_copy"],
        &["implicit_target_resolution", "full_tree_scan"],
    ),
    contract(
        "overwrite",
        "O(path_depth + touched_extent_frontiers + input_bytes)",
        &["normalized_extent_intervals", "persistent_path_copy"],
        NO_SCAN,
    ),
    contract(
        "append",
        "O(path_depth + final_extent_frontier + input_bytes)",
        &["last_frontier_growth", "streaming_blob_build"],
        NO_SCAN,
    ),
    contract(
        "truncate",
        "O(path_depth + intersecting_frontier)",
        &["subtree_bound_drop", "no_discarded_subtree_read"],
        NO_SCAN,
    ),
    contract(
        "allocate",
        "O(path_depth + touched_extent_frontiers)",
        &["allocated_zero_extent", "qualified_native_preallocation"],
        &["logical_zero_materialization", "full_file_materialization"],
    ),
    contract(
        "hole_punch",
        "O(path_depth + touched_extent_frontiers)",
        &["normalized_extent_intervals", "hole_coalescing"],
        NO_SCAN,
    ),
    contract(
        "clone",
        "O(path_depth + touched_extent_frontiers)",
        &["immutable_extent_reference", "qualified_native_clone"],
        &[
            "content_copy_without_explicit_plan",
            "full_file_materialization",
        ],
    ),
    contract(
        "copy_range",
        "O(source_frontiers + destination_frontiers + copied_bytes)",
        &["immutable_reference_when_safe", "bounded_streaming_copy"],
        &["whole_file_materialization", "unbounded_copy_buffer"],
    ),
    contract(
        "link",
        "O(source_path_depth + destination_path_depth)",
        &["path_independent_file_id", "shared_prefix_plan"],
        NO_SCAN,
    ),
    contract(
        "unlink",
        "O(path_depth + file_table_height)",
        &["persistent_path_copy", "deferred_reachability_gc"],
        NO_SCAN,
    ),
    contract(
        "rmdir",
        "O(path_depth + bounded_empty_proof)",
        &[
            "authenticated_empty_directory_proof",
            "persistent_path_copy",
        ],
        NO_SCAN,
    ),
    contract(
        "rename",
        "O(source_depth + destination_depth)",
        &["one_volume_atomic_batch", "shared_prefix_plan"],
        &["cross_volume_rename", "full_tree_scan"],
    ),
    contract(
        "metadata_change",
        "O(path_depth + file_table_height)",
        &["immutable_metadata_object", "persistent_path_copy"],
        NO_SCAN,
    ),
    contract(
        "attribute_get",
        "O(path_depth + attribute_frontier + output_bytes)",
        &["bounded_attribute_lookup", "range_object_reads"],
        NO_SCAN,
    ),
    contract(
        "attribute_set",
        "O(path_depth + attribute_frontier + input_bytes)",
        &["byte_adaptive_attribute_pages", "persistent_path_copy"],
        NO_SCAN,
    ),
    contract(
        "attribute_list",
        "O(path_depth + intersecting_pages + output_items)",
        &["bounded_attribute_cursor", "continuation_witness"],
        NO_SCAN,
    ),
    contract(
        "attribute_remove",
        "O(path_depth + attribute_frontier)",
        &["persistent_path_copy", "empty_root_reuse"],
        NO_SCAN,
    ),
    contract(
        "flush",
        "O(handle_dirty_state)",
        &["bounded_dirty_flush", "idempotent_retry"],
        NO_UNBOUNDED,
    ),
    contract(
        "sync",
        "O(pending_durable_objects + one_authority_barrier)",
        &["closure_before_authority", "explicit_durability_barrier"],
        &["ack_before_durable_closure", "unbounded_pending_queue"],
    ),
    contract(
        "watch",
        "O(authority_delta + selected_notifications)",
        &["persistent_cursor", "bounded_coalescing"],
        &["polling_full_tree_scan", "notification_as_authority"],
    ),
    contract(
        "checkpoint",
        "O(1) unchanged; O(1) new generation root",
        &["root_identity_reuse", "single_generation_object"],
        NO_SCAN,
    ),
    contract(
        "commit",
        "O(generation_closure + one_authority_append)",
        &[
            "durable_closure_before_cas",
            "idempotent_authority_operation",
        ],
        NO_UNBOUNDED,
    ),
    contract(
        "diff",
        "O(changed_authenticated_frontiers + output)",
        &["equal_root_short_circuit", "subtree_identity_skip"],
        NO_SCAN,
    ),
    contract(
        "merge",
        "O(changed_frontiers + conflicts + output)",
        &["three_way_subtree_identity", "sparse_replay"],
        NO_SCAN,
    ),
    contract(
        "rebase",
        "O(1) equal root; O(observed_regions + sparse_mutations)",
        &["observation_proofs", "subtree_identity_reuse"],
        NO_SCAN,
    ),
    contract(
        "refresh",
        "O(1) pinned/manual; O(observed_regions) tracking",
        &["authority_cursor", "observation_safe_rebase"],
        NO_SCAN,
    ),
    contract(
        "discard",
        "O(sparse_overlay_state)",
        &["immutable_base_reuse", "pin_release"],
        NO_SCAN,
    ),
    contract(
        "export",
        "O(reachable_objects + archive_bytes)",
        &["streaming_archive", "digest_dedup", "bounded_backpressure"],
        NO_UNBOUNDED,
    ),
    contract(
        "import",
        "O(archive_objects + archive_bytes)",
        &["streaming_verify_before_admit", "create_if_absent"],
        NO_UNBOUNDED,
    ),
    contract(
        "materialize",
        "O(selected_files + selected_bytes)",
        &[
            "bounded_parallel_ranges",
            "qualified_sparse_and_clone_paths",
        ],
        NO_UNBOUNDED,
    ),
    contract(
        "capture",
        "O(change_journal + changed_bytes)",
        &["platform_change_cursor", "bounded_rescan_on_typed_plan"],
        &["silent_whole_volume_rescan", "unbounded_allocation"],
    ),
    contract(
        "mount",
        "O(mount_count * path_depth)",
        &[
            "deterministic_longest_prefix_index",
            "lazy_authenticated_demand",
        ],
        NO_SCAN,
    ),
    contract(
        "unmount",
        "O(owned_handles + dirty_sparse_state)",
        &["stale_handle_fence", "bounded_drain"],
        NO_UNBOUNDED,
    ),
    contract(
        "lock_range",
        "O(log(active_handle_ranges))",
        &["mount_epoch_fence", "bounded_interval_index"],
        &["durable_lock_as_filesystem_truth", "unbounded_wait"],
    ),
    contract(
        "unlock_range",
        "O(log(active_handle_ranges))",
        &["idempotent_handle_release", "bounded_waiter_wake"],
        &["cross_epoch_unlock", "unbounded_wake_fanout"],
    ),
    contract(
        "garbage_collect",
        "O(reachable_objects + collection_candidates)",
        &["ref_and_pin_mark", "incremental_sweep_cursor"],
        NO_UNBOUNDED,
    ),
    contract(
        "recover",
        "O(bounded_log_tail + snapshot_delta)",
        &[
            "checksummed_hash_chain",
            "durable_snapshot",
            "bounded_replay",
        ],
        NO_UNBOUNDED,
    ),
];

const fn contract(
    operation: &'static str,
    dominant_work: &'static str,
    required_plans: &'static [&'static str],
    forbidden: &'static [&'static str],
) -> OperationContract {
    OperationContract {
        operation,
        dominant_work,
        required_plans,
        forbidden,
    }
}

#[derive(Serialize)]
struct WorkloadManifest<'a> {
    schema: &'static str,
    dimensions: &'a [Dimension],
    operation_contracts: &'a [OperationContract],
    targeted_interactions: &'a [InteractionFamily],
}

/// Renders the stable workload taxonomy and operation contracts as JSON.
///
/// # Errors
///
/// Returns a serialization error if the machine-readable manifest cannot be
/// represented by the pinned JSON implementation.
pub fn manifest_json() -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(&WorkloadManifest {
        schema: "acyclic-fs-workload-manifest-v1",
        dimensions: DIMENSIONS,
        operation_contracts: OPERATION_CONTRACTS,
        targeted_interactions: TARGETED_INTERACTIONS,
    })
}

/// Generates complete pairwise coverage over all declared dimension levels.
#[must_use]
pub fn pairwise_cases() -> Vec<WorkloadCase> {
    let mut cases = BTreeSet::new();
    for first_dimension in 0..DIMENSIONS.len() {
        for second_dimension in first_dimension + 1..DIMENSIONS.len() {
            for first_level in 0..DIMENSIONS[first_dimension].levels.len() {
                for second_level in 0..DIMENSIONS[second_dimension].levels.len() {
                    let mut levels = vec![0; DIMENSIONS.len()];
                    levels[first_dimension] = first_level;
                    levels[second_dimension] = second_level;
                    cases.insert(levels);
                }
            }
        }
    }
    cases
        .into_iter()
        .map(|levels| WorkloadCase { levels })
        .collect()
}

/// One generated case paired with its stable corpus position.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SelectedWorkloadCase {
    /// Zero-based position in [`pairwise_cases`].
    pub index: usize,
    /// Complete level vector in canonical dimension order.
    pub case: WorkloadCase,
}

/// Explicit bounds for corpus generation, filtering, and serialization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SelectionLimits {
    /// Maximum raw pairwise cases admitted before generation.
    pub maximum_generated_cases: usize,
    /// Maximum matching cases retained in one result.
    pub maximum_selected_cases: usize,
    /// Maximum encoded selected-corpus JSON bytes.
    pub maximum_json_bytes: usize,
}

impl Default for SelectionLimits {
    fn default() -> Self {
        Self {
            maximum_generated_cases: 1_000_000,
            maximum_selected_cases: 1_000_000,
            maximum_json_bytes: 16 * 1024 * 1024,
        }
    }
}

/// Selects deterministic corpus cases using a canonical conjunctive filter.
///
/// An empty selector list selects the entire corpus. At most one case identity
/// and one level per dimension may be supplied. Results remain in canonical
/// corpus order.
///
/// # Errors
///
/// Returns a typed error for duplicate/contradictory filters, an out-of-range
/// case identity, or a complete filter that matches no generated case.
pub fn select_workload_cases(
    selectors: &[WorkloadSelector],
    limits: SelectionLimits,
) -> Result<Vec<SelectedWorkloadCase>, SelectorError> {
    let mut canonical = BTreeSet::new();
    let mut selected_case = None;
    let mut selected_levels = BTreeSet::new();
    for selector in selectors {
        let rendered = selector.to_string();
        if !canonical.insert(rendered.clone()) {
            return Err(SelectorError::Duplicate(rendered));
        }
        match selector {
            WorkloadSelector::Case { corpus, index } => {
                let expected = workload_corpus_digest();
                if *corpus != expected {
                    return Err(SelectorError::CorpusMismatch {
                        expected,
                        actual: corpus.clone(),
                    });
                }
                if let Some(prior) = selected_case.replace(*index) {
                    return Err(SelectorError::Contradictory(format!(
                        "case indices {prior} and {index}"
                    )));
                }
            }
            WorkloadSelector::Level { dimension, level } => {
                let definition = DIMENSIONS
                    .iter()
                    .find(|candidate| candidate.name == *dimension)
                    .ok_or_else(|| SelectorError::UnknownDimension((*dimension).to_owned()))?;
                if !definition.levels.contains(level) {
                    return Err(SelectorError::UnknownLevel {
                        dimension: (*dimension).to_owned(),
                        level: (*level).to_owned(),
                    });
                }
                if let Some((_, prior)) = selected_levels
                    .iter()
                    .find(|(candidate, _)| candidate == dimension)
                {
                    return Err(SelectorError::Contradictory(format!(
                        "{dimension}={prior} and {dimension}={level}"
                    )));
                }
                selected_levels.insert((*dimension, *level));
            }
        }
    }

    let generated_upper_bound = pairwise_case_upper_bound()?;
    if generated_upper_bound > limits.maximum_generated_cases {
        return Err(SelectorError::LimitExceeded {
            what: "generated case count",
            limit: limits.maximum_generated_cases,
            actual: generated_upper_bound,
        });
    }
    let cases = pairwise_cases();
    if let Some(index) = selected_case
        && index >= cases.len()
    {
        return Err(SelectorError::UnknownCase(index));
    }
    let mut selected = Vec::new();
    for (index, case) in cases.into_iter().enumerate() {
        let matches = selected_case.is_none_or(|selected| selected == index)
            && selected_levels.iter().all(|(dimension, level)| {
                let dimension_index = DIMENSIONS
                    .iter()
                    .position(|candidate| candidate.name == *dimension);
                dimension_index.is_some_and(|dimension_index| {
                    let level_index = DIMENSIONS[dimension_index]
                        .levels
                        .iter()
                        .position(|candidate| candidate == level);
                    level_index
                        .is_some_and(|level_index| case.levels[dimension_index] == level_index)
                })
            });
        if matches {
            let actual = selected.len().saturating_add(1);
            if actual > limits.maximum_selected_cases {
                return Err(SelectorError::LimitExceeded {
                    what: "selected case count",
                    limit: limits.maximum_selected_cases,
                    actual,
                });
            }
            selected.push(SelectedWorkloadCase { index, case });
        }
    }
    if selected.is_empty() {
        return Err(SelectorError::EmptySelection);
    }
    Ok(selected)
}

fn pairwise_case_upper_bound() -> Result<usize, SelectorError> {
    let mut total = 0_usize;
    for (first, first_dimension) in DIMENSIONS.iter().enumerate() {
        for second_dimension in DIMENSIONS.iter().skip(first + 1) {
            let pair = first_dimension
                .levels
                .len()
                .checked_mul(second_dimension.levels.len())
                .ok_or(SelectorError::LimitExceeded {
                    what: "generated case count",
                    limit: usize::MAX,
                    actual: usize::MAX,
                })?;
            total = total
                .checked_add(pair)
                .ok_or(SelectorError::LimitExceeded {
                    what: "generated case count",
                    limit: usize::MAX,
                    actual: usize::MAX,
                })?;
        }
    }
    Ok(total)
}

#[derive(Serialize)]
struct WorkloadCorpus {
    schema: &'static str,
    corpus_digest: String,
    dimension_order: Vec<&'static str>,
    cases: Vec<WorkloadCase>,
}

#[derive(Serialize)]
struct TargetedInteractionCorpus {
    schema: &'static str,
    corpus_digest: &'static str,
    families: &'static [InteractionFamily],
    cases: Vec<TargetedInteractionCase>,
}

#[derive(Serialize)]
struct SelectedWorkloadCorpus {
    schema: &'static str,
    corpus_digest: String,
    selectors: Vec<String>,
    dimension_order: Vec<&'static str>,
    cases: Vec<SelectedWorkloadCase>,
}

/// Typed failure from selecting or encoding a bounded corpus subset.
#[derive(Debug)]
pub enum SelectedCorpusError {
    /// Selection failed before serialization.
    Selection(SelectorError),
    /// The pinned JSON serializer rejected the selected value.
    Serialization(serde_json::Error),
    /// Encoded JSON crossed the admitted byte ceiling.
    OutputLimit {
        /// Caller-admitted encoded byte maximum.
        limit: usize,
    },
}

impl fmt::Display for SelectedCorpusError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Selection(error) => error.fmt(formatter),
            Self::Serialization(error) => error.fmt(formatter),
            Self::OutputLimit { limit } => {
                write!(formatter, "selected workload JSON exceeds {limit} bytes")
            }
        }
    }
}

impl std::error::Error for SelectedCorpusError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Selection(error) => Some(error),
            Self::Serialization(error) => Some(error),
            Self::OutputLimit { .. } => None,
        }
    }
}

impl From<SelectorError> for SelectedCorpusError {
    fn from(value: SelectorError) -> Self {
        Self::Selection(value)
    }
}

struct BoundedJsonWriter {
    bytes: Box<[u8]>,
    written: usize,
    exceeded: bool,
}

impl std::io::Write for BoundedJsonWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let required = self.written.checked_add(bytes.len());
        if required.is_none_or(|required| required > self.bytes.len()) {
            self.exceeded = true;
            return Err(std::io::Error::other(
                "selected workload JSON limit exceeded",
            ));
        }
        let required = required.unwrap_or(self.bytes.len());
        self.bytes[self.written..required].copy_from_slice(bytes);
        self.written = required;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Renders the deterministic duplicate-free pairwise workload corpus as JSON.
///
/// Every case contains one level index for every entry in [`DIMENSIONS`]. The
/// separate dimension order makes selectors compact while retaining a stable,
/// independently checkable meaning.
///
/// # Errors
///
/// Returns a serialization error if the generated corpus cannot be represented
/// by the pinned JSON implementation.
pub fn workload_corpus_json() -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(&WorkloadCorpus {
        schema: "acyclic-fs-pairwise-workload-corpus-v2",
        corpus_digest: workload_corpus_digest(),
        dimension_order: DIMENSIONS.iter().map(|dimension| dimension.name).collect(),
        cases: pairwise_cases(),
    })
}

/// Renders the complete deterministic Cartesian corpus for every mandatory
/// targeted three-way family.
///
/// # Errors
///
/// Returns a serialization error if the corpus cannot be represented by the
/// pinned JSON implementation.
pub fn targeted_interaction_corpus_json() -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(&TargetedInteractionCorpus {
        schema: "acyclic-fs-targeted-interaction-corpus-v1",
        corpus_digest: TARGETED_INTERACTION_CORPUS_DIGEST,
        families: TARGETED_INTERACTIONS,
        cases: targeted_interaction_cases(),
    })
}

/// Renders one canonical selected subset of the generated workload corpus.
///
/// # Errors
///
/// Returns a typed selection error for invalid filters and a serialization
/// error if the selected corpus cannot be represented by the pinned JSON
/// implementation.
pub fn selected_workload_corpus_json(
    selectors: &[WorkloadSelector],
    limits: SelectionLimits,
) -> Result<String, SelectedCorpusError> {
    let cases = select_workload_cases(selectors, limits)?;
    let mut canonical: Vec<_> = selectors.iter().map(ToString::to_string).collect();
    canonical.sort_unstable();
    let mut writer = BoundedJsonWriter {
        bytes: vec![0; limits.maximum_json_bytes].into_boxed_slice(),
        written: 0,
        exceeded: false,
    };
    let encoded = serde_json::to_writer_pretty(
        &mut writer,
        &SelectedWorkloadCorpus {
            schema: "acyclic-fs-selected-workload-corpus-v1",
            corpus_digest: workload_corpus_digest(),
            selectors: canonical,
            dimension_order: DIMENSIONS.iter().map(|dimension| dimension.name).collect(),
            cases,
        },
    );
    if writer.exceeded {
        return Err(SelectedCorpusError::OutputLimit {
            limit: limits.maximum_json_bytes,
        });
    }
    encoded.map_err(SelectedCorpusError::Serialization)?;
    let written = writer.written;
    let mut bytes = writer.bytes.into_vec();
    bytes.truncate(written);
    String::from_utf8(bytes).map_err(|error| {
        SelectedCorpusError::Serialization(serde_json::Error::io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            error,
        )))
    })
}

/// Emits canonical language-neutral content-range dependency vectors.
///
/// # Errors
///
/// Returns the canonical kernel's bounded evidence error, allocation failure,
/// or JSON serialization failure.
pub fn dependency_vectors_json() -> Result<String, Box<dyn std::error::Error>> {
    let cases = [
        ("single-zero", vec![0_u8]),
        ("hello-utf8", b"hello".to_vec()),
        ("embedded-zero", b"left\0right".to_vec()),
        ("byte-sequence-0-31", (0_u8..32).collect()),
    ];
    let mut vectors = Vec::new();
    vectors.try_reserve_exact(cases.len())?;
    for (name, bytes) in cases {
        let receipt = acyclic_fs::kernel::capture_content_range_bytes(
            &bytes,
            u64::try_from(bytes.len())?,
            acyclic_fs::WorkBudget::UNBOUNDED,
        )?;
        let acyclic_fs::kernel::DependencyState::Present(digest) = receipt.value else {
            return Err("content range evidence was unexpectedly absent".into());
        };
        vectors.push(serde_json::json!({
            "name": name,
            "logical_bytes_hex": hex::encode(&bytes),
            "logical_length": bytes.len(),
            "digest_hex": hex::encode(digest.into_bytes()),
            "bytes_hashed": receipt.work.bytes_hashed,
        }));
    }
    Ok(serde_json::to_string_pretty(&serde_json::json!({
        "schema": "acyclic-fs-dependency-content-range-v1",
        "algorithm": "BLAKE3-256",
        "domain_utf8_hex": hex::encode(b"acyclic-fs-dependency-content-range-v1\0"),
        "length_encoding": "u64-le",
        "cases": vectors,
    }))?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_level_and_every_pair_is_generated() {
        let cases = pairwise_cases();
        assert!(!cases.is_empty());
        for (dimension, definition) in DIMENSIONS.iter().enumerate() {
            for level in 0..definition.levels.len() {
                let mut levels = vec![0; DIMENSIONS.len()];
                levels[dimension] = level;
                assert!(cases.binary_search(&WorkloadCase { levels }).is_ok());
            }
        }
        for (first, first_dimension) in DIMENSIONS.iter().enumerate() {
            for (second, second_dimension) in DIMENSIONS.iter().enumerate().skip(first + 1) {
                for first_level in 0..first_dimension.levels.len() {
                    for second_level in 0..second_dimension.levels.len() {
                        let mut levels = vec![0; DIMENSIONS.len()];
                        levels[first] = first_level;
                        levels[second] = second_level;
                        assert!(cases.binary_search(&WorkloadCase { levels }).is_ok());
                    }
                }
            }
        }
    }

    #[test]
    fn generated_corpus_is_stable_and_duplicate_free() -> Result<(), Box<dyn std::error::Error>> {
        let first = pairwise_cases();
        let second = pairwise_cases();
        assert_eq!(first, second);
        assert!(first.windows(2).all(|window| window[0] < window[1]));
        let json = workload_corpus_json()?;
        let value: serde_json::Value = serde_json::from_str(&json)?;
        assert_eq!(value["schema"], "acyclic-fs-pairwise-workload-corpus-v2");
        assert_eq!(
            value["corpus_digest"],
            "b1e9d1d438350fcc849459e2cf8d4b56c315113de9598872a81e3fb1eb0c1c78"
        );
        assert_eq!(compute_workload_corpus_digest(), WORKLOAD_CORPUS_DIGEST);
        assert_eq!(value["cases"].as_array().map(Vec::len), Some(first.len()));
        Ok(())
    }

    #[test]
    fn dependency_vectors_are_locked_to_the_canonical_kernel()
    -> Result<(), Box<dyn std::error::Error>> {
        let generated = dependency_vectors_json()?;
        let tracked = include_str!("../vectors/filesystem/dependency-content-range-v1.json");
        assert_eq!(format!("{generated}\n"), tracked.replace("\r\n", "\n"));
        Ok(())
    }

    #[test]
    fn selectors_are_canonical_and_filter_without_reordering() -> Result<(), SelectorError> {
        let digest = workload_corpus_digest();
        let case_text = format!("case:{digest}:0");
        let case: WorkloadSelector = case_text.parse()?;
        let backend: WorkloadSelector = "backend=memory".parse()?;
        assert_eq!(case.to_string(), case_text);
        assert_eq!(backend.to_string(), "backend=memory");

        let selected =
            select_workload_cases(std::slice::from_ref(&case), SelectionLimits::default())?;
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].index, 0);
        assert_eq!(selected[0].case, pairwise_cases()[0]);

        let memory =
            select_workload_cases(std::slice::from_ref(&backend), SelectionLimits::default())?;
        let backend_index = DIMENSIONS
            .iter()
            .position(|dimension| dimension.name == "backend")
            .ok_or_else(|| SelectorError::UnknownDimension("backend".to_owned()))?;
        assert!(
            memory
                .windows(2)
                .all(|window| window[0].index < window[1].index)
        );
        assert!(
            memory
                .iter()
                .all(|selected| selected.case.levels[backend_index] == 0)
        );
        let json = selected_workload_corpus_json(&[backend, case], SelectionLimits::default())
            .map_err(|error| SelectorError::Malformed(error.to_string()))?;
        let value: serde_json::Value = serde_json::from_str(&json)
            .map_err(|error| SelectorError::Malformed(error.to_string()))?;
        assert_eq!(value["schema"], "acyclic-fs-selected-workload-corpus-v1");
        assert_eq!(value["corpus_digest"], digest);
        assert_eq!(value["selectors"][0], "backend=memory");
        assert_eq!(value["selectors"][1], case_text);
        assert_eq!(value["cases"].as_array().map(Vec::len), Some(1));
        assert!(matches!(
            selected_workload_corpus_json(
                &["backend=memory".parse()?],
                SelectionLimits {
                    maximum_json_bytes: 1,
                    ..SelectionLimits::default()
                },
            ),
            Err(SelectedCorpusError::OutputLimit { limit: 1 })
        ));
        Ok(())
    }

    #[test]
    fn selector_rejections_are_typed_and_total() -> Result<(), SelectorError> {
        assert_eq!(
            "missing".parse::<WorkloadSelector>(),
            Err(SelectorError::Malformed("missing".to_owned()))
        );
        assert_eq!(
            "case:00".parse::<WorkloadSelector>(),
            Err(SelectorError::Malformed("case:00".to_owned()))
        );
        assert_eq!(
            "unknown=value".parse::<WorkloadSelector>(),
            Err(SelectorError::UnknownDimension("unknown".to_owned()))
        );
        assert_eq!(
            "backend=unknown".parse::<WorkloadSelector>(),
            Err(SelectorError::UnknownLevel {
                dimension: "backend".to_owned(),
                level: "unknown".to_owned(),
            })
        );
        let digest = workload_corpus_digest();
        let case_text = format!("case:{digest}:0");
        let case: WorkloadSelector = case_text.parse()?;
        assert_eq!(
            select_workload_cases(&[case.clone(), case], SelectionLimits::default()),
            Err(SelectorError::Duplicate(case_text))
        );
        let memory: WorkloadSelector = "backend=memory".parse()?;
        let local: WorkloadSelector = "backend=local".parse()?;
        assert!(matches!(
            select_workload_cases(&[memory, local], SelectionLimits::default()),
            Err(SelectorError::Contradictory(_))
        ));
        let stale: WorkloadSelector = format!("case:{}:0", "0".repeat(64)).parse()?;
        assert!(matches!(
            select_workload_cases(&[stale], SelectionLimits::default()),
            Err(SelectorError::CorpusMismatch { .. })
        ));
        assert!(matches!(
            select_workload_cases(
                &[WorkloadSelector::Level {
                    dimension: "not-declared",
                    level: "none",
                }],
                SelectionLimits::default(),
            ),
            Err(SelectorError::UnknownDimension(_))
        ));
        assert!(matches!(
            select_workload_cases(
                &[],
                SelectionLimits {
                    maximum_generated_cases: 0,
                    ..SelectionLimits::default()
                },
            ),
            Err(SelectorError::LimitExceeded { .. })
        ));
        let exact: WorkloadSelector = format!("case:{WORKLOAD_CORPUS_DIGEST}:0").parse()?;
        assert!(matches!(
            select_workload_cases(
                &[exact],
                SelectionLimits {
                    maximum_generated_cases: 0,
                    ..SelectionLimits::default()
                },
            ),
            Err(SelectorError::LimitExceeded { .. })
        ));
        assert_eq!(
            select_workload_cases(
                &[WorkloadSelector::Case {
                    corpus: digest,
                    index: usize::MAX,
                }],
                SelectionLimits::default(),
            ),
            Err(SelectorError::UnknownCase(usize::MAX))
        );
        Ok(())
    }

    #[test]
    fn selector_argument_grammar_is_shared_bounded_and_exact() -> Result<(), SelectorError> {
        let parsed = parse_workload_selector_arguments([
            "--selector".to_owned(),
            "backend=memory".to_owned(),
        ])
        .map_err(|error| SelectorError::Malformed(error.to_string()))?;
        assert_eq!(parsed, ["backend=memory".parse()?]);
        assert_eq!(
            parse_workload_selector_arguments(Vec::<String>::new()),
            Err(SelectorArgumentError::Usage)
        );
        assert_eq!(
            parse_workload_selector_arguments(["--other".to_owned(), "backend=memory".to_owned()]),
            Err(SelectorArgumentError::Usage)
        );
        let mut excessive = Vec::new();
        for _ in 0..=DIMENSIONS.len() + 1 {
            excessive.push("--selector".to_owned());
            excessive.push("backend=memory".to_owned());
        }
        assert_eq!(
            parse_workload_selector_arguments(excessive),
            Err(SelectorArgumentError::TooMany)
        );
        Ok(())
    }

    #[test]
    fn bounded_json_writer_never_exceeds_its_fixed_allocation() -> std::io::Result<()> {
        let limit = 80 * 1024;
        let mut writer = BoundedJsonWriter {
            bytes: vec![0; limit].into_boxed_slice(),
            written: 0,
            exceeded: false,
        };
        std::io::Write::write_all(&mut writer, &vec![7; 70 * 1024])?;
        assert_eq!(writer.bytes.len(), limit);
        assert_eq!(writer.written, 70 * 1024);
        assert!(!writer.exceeded);
        assert!(std::io::Write::write_all(&mut writer, &vec![8; 11 * 1024]).is_err());
        assert_eq!(writer.bytes.len(), limit);
        assert_eq!(writer.written, 70 * 1024);
        assert!(writer.exceeded);
        Ok(())
    }

    #[test]
    fn names_are_stable_and_unique() {
        for (index, dimension) in DIMENSIONS.iter().enumerate() {
            assert!(!dimension.name.is_empty());
            assert!(!dimension.levels.is_empty());
            assert!(
                !DIMENSIONS[..index]
                    .iter()
                    .any(|prior| prior.name == dimension.name)
            );
            for (level, name) in dimension.levels.iter().enumerate() {
                assert!(!name.is_empty());
                assert!(!dimension.levels[..level].contains(name));
            }
        }
    }

    #[test]
    fn every_operation_has_one_nonempty_complexity_contract() {
        let operation = DIMENSIONS
            .iter()
            .find(|dimension| dimension.name == "operation")
            .map(|dimension| dimension.levels)
            .unwrap_or_default();
        assert_eq!(operation.len(), OPERATION_CONTRACTS.len());
        for name in operation {
            let matching: Vec<_> = OPERATION_CONTRACTS
                .iter()
                .filter(|contract| contract.operation == *name)
                .collect();
            assert_eq!(matching.len(), 1);
            assert!(!matching[0].dominant_work.is_empty());
            assert!(!matching[0].required_plans.is_empty());
            assert!(!matching[0].forbidden.is_empty());
        }
    }

    #[test]
    fn targeted_interactions_reference_three_distinct_dimensions() {
        for (index, family) in TARGETED_INTERACTIONS.iter().enumerate() {
            assert!(!family.name.is_empty());
            assert!(
                !TARGETED_INTERACTIONS[..index]
                    .iter()
                    .any(|prior| prior.name == family.name)
            );
            for (position, dimension) in family.dimensions.iter().enumerate() {
                assert!(
                    DIMENSIONS
                        .iter()
                        .any(|candidate| candidate.name == *dimension)
                );
                assert!(!family.dimensions[..position].contains(dimension));
            }
        }
    }

    #[test]
    fn targeted_interaction_corpus_is_complete_stable_and_duplicate_free() {
        let cases = targeted_interaction_cases();
        let expected = TARGETED_INTERACTIONS
            .iter()
            .map(|family| {
                family
                    .dimensions
                    .iter()
                    .map(|name| {
                        DIMENSIONS
                            .iter()
                            .find(|dimension| dimension.name == *name)
                            .map_or(0, |dimension| dimension.levels.len())
                    })
                    .product::<usize>()
            })
            .sum::<usize>();
        assert_eq!(cases.len(), expected);
        assert_eq!(
            cases.iter().copied().collect::<BTreeSet<_>>().len(),
            expected
        );
        assert_eq!(
            compute_targeted_interaction_corpus_digest(),
            TARGETED_INTERACTION_CORPUS_DIGEST
        );
    }

    #[test]
    fn targeted_interaction_json_is_bound_to_the_locked_corpus()
    -> Result<(), Box<dyn std::error::Error>> {
        let value: serde_json::Value = serde_json::from_str(&targeted_interaction_corpus_json()?)?;
        assert_eq!(value["schema"], "acyclic-fs-targeted-interaction-corpus-v1");
        assert_eq!(value["corpus_digest"], TARGETED_INTERACTION_CORPUS_DIGEST);
        assert_eq!(
            value["families"].as_array().map(Vec::len),
            Some(TARGETED_INTERACTIONS.len())
        );
        assert_eq!(
            value["cases"].as_array().map(Vec::len),
            Some(targeted_interaction_cases().len())
        );
        Ok(())
    }

    #[test]
    fn targeted_interaction_shards_are_bounded_exact_and_recomposable()
    -> Result<(), Box<dyn std::error::Error>> {
        let complete = select_targeted_interaction_cases(
            None,
            TargetedInteractionShard {
                shard_index: 0,
                shard_count: 1,
                maximum_cases: 20_000,
            },
        )?;
        assert_eq!(complete.len(), 14_160);
        let mut recomposed = Vec::new();
        for shard_index in 0..17 {
            recomposed.extend(select_targeted_interaction_cases(
                None,
                TargetedInteractionShard {
                    shard_index,
                    shard_count: 17,
                    maximum_cases: 1_000,
                },
            )?);
        }
        recomposed.sort_unstable_by_key(|case| case.ordinal);
        assert_eq!(recomposed, complete);

        let family = select_targeted_interaction_cases(
            Some("sequence_concurrency_consistency"),
            TargetedInteractionShard {
                shard_index: 0,
                shard_count: 1,
                maximum_cases: 500,
            },
        )?;
        assert_eq!(family.len(), 15 * 7 * 4);
        assert!(family.iter().all(|case| {
            case.family == "sequence_concurrency_consistency"
                && case.dimensions[0].dimension == "operation_sequence"
                && case.dimensions[1].dimension == "concurrency"
                && case.dimensions[2].dimension == "consistency"
        }));
        Ok(())
    }

    #[test]
    fn targeted_interaction_selection_rejects_before_unbounded_growth() {
        assert_eq!(
            select_targeted_interaction_cases(
                None,
                TargetedInteractionShard {
                    shard_index: 1,
                    shard_count: 1,
                    maximum_cases: usize::MAX,
                },
            ),
            Err(TargetedInteractionSelectionError::InvalidShard { index: 1, count: 1 })
        );
        assert_eq!(
            select_targeted_interaction_cases(
                Some("missing"),
                TargetedInteractionShard {
                    shard_index: 0,
                    shard_count: 1,
                    maximum_cases: usize::MAX,
                },
            ),
            Err(TargetedInteractionSelectionError::UnknownFamily(
                "missing".to_owned()
            ))
        );
        assert_eq!(
            select_targeted_interaction_cases(
                None,
                TargetedInteractionShard {
                    shard_index: 0,
                    shard_count: 1,
                    maximum_cases: 0,
                },
            ),
            Err(TargetedInteractionSelectionError::LimitExceeded {
                limit: 0,
                actual: 1,
            })
        );
    }

    #[test]
    fn machine_manifest_is_stable_and_complete() -> Result<(), Box<dyn std::error::Error>> {
        let manifest = manifest_json()?;
        let value: serde_json::Value = serde_json::from_str(&manifest)?;
        assert_eq!(value["schema"], "acyclic-fs-workload-manifest-v1");
        assert_eq!(
            value["dimensions"].as_array().map(Vec::len),
            Some(DIMENSIONS.len())
        );
        assert_eq!(
            value["operation_contracts"].as_array().map(Vec::len),
            Some(OPERATION_CONTRACTS.len())
        );
        assert_eq!(
            value["targeted_interactions"].as_array().map(Vec::len),
            Some(TARGETED_INTERACTIONS.len())
        );
        Ok(())
    }
}
