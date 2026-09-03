//! Explicit, backend-independent volume and checkout semantics.

use crate::foundation::GenerationId;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Filesystem behavior profile requested by a volume.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilesystemProfile {
    /// Semantics shared by native and browser implementations.
    Portable,
    /// POSIX-oriented names, links, ownership, and mode behavior.
    Posix,
    /// Windows-oriented names, reparse points, streams, and security metadata.
    Windows,
    /// Browser-safe semantics with no host mount assumptions.
    Browser,
}

/// Whether a mounted checkout admits mutations.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessMode {
    /// Reads only.
    ReadOnly,
    /// Reads and admitted mutations.
    ReadWrite,
}

/// How a checkout observes newer published generations.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsistencyMode {
    /// The selected generation never advances implicitly.
    Pinned,
    /// Advance only when observation and mutation proofs establish safety.
    TrackingSafe,
    /// Receive generation changes and apply them at safe boundaries.
    Live,
    /// Report newer heads and require an explicit caller refresh.
    Manual,
}

/// How concurrent writers are admitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConcurrencyMode {
    /// Every writable head checkout atomically advances the authority epoch;
    /// opening a replacement writer permanently fences all prior writers.
    ExclusiveWriter,
    /// Writers publish against an expected immutable generation and receive a
    /// typed conflict instead of implicit retry or merge.
    Optimistic,
    /// One authority orders every admitted direct-live mutation; safe disjoint
    /// races may rebase and retry within the caller's explicit bounds.
    SerializedAuthority,
}

/// Persistence expectation of a volume.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Lifecycle {
    /// State may disappear when its owning runtime ends.
    Ephemeral,
    /// Acknowledged state must survive restart under the backend contract.
    Durable,
}

/// Where checkout mutations accumulate before publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationMode {
    /// Mutations accumulate in a private sparse copy-on-write overlay.
    PrivateOverlay,
    /// Mutations are submitted directly to a serialized live authority.
    DirectLive,
    /// The checkout cannot mutate.
    None,
}

/// Case comparison policy for logical names.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaseSensitivity {
    /// Compare canonical name bytes exactly.
    Sensitive,
    /// Use the selected platform profile's case-folding rules.
    ProfileFolded,
}

/// Unicode normalization policy applied at the parsing boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnicodePolicy {
    /// Preserve admitted scalar sequences exactly.
    Preserve,
    /// Require canonical NFC input and reject non-normalized names.
    RequireNfc,
}

/// Hard limits enforced before allocation, traversal, or mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VolumeLimits {
    /// Maximum encoded path bytes.
    pub maximum_path_bytes: u32,
    /// Maximum encoded bytes in one path component.
    pub maximum_component_bytes: u32,
    /// Maximum path components.
    pub maximum_path_depth: u16,
    /// Maximum canonical bytes in one immutable object.
    pub maximum_object_bytes: u64,
    /// Maximum mutations admitted in one atomic batch.
    pub maximum_mutations_per_batch: u32,
    /// Maximum exact paths admitted in one shared lookup/stat batch.
    pub maximum_paths_per_batch: u32,
    /// Maximum distinct observation and mutation regions retained by one checkout.
    pub maximum_checkout_dependencies: u32,
    /// Maximum directory entries returned in one page.
    pub maximum_directory_page_entries: u32,
    /// Maximum authenticated B+tree levels followed by one operation.
    pub maximum_page_height: u16,
    /// Maximum bytes returned by one range request.
    pub maximum_read_bytes: u64,
    /// Maximum path-independent files authenticated in one generation.
    pub maximum_files_per_generation: u64,
    /// Maximum distinct immutable objects authenticated in one generation.
    pub maximum_objects_per_generation: u64,
    /// Maximum cumulative canonical object bytes in one generation closure.
    pub maximum_generation_bytes: u64,
}

impl Default for VolumeLimits {
    fn default() -> Self {
        Self {
            maximum_path_bytes: 32 * 1024,
            maximum_component_bytes: 255,
            maximum_path_depth: 1_024,
            maximum_object_bytes: 64 * 1024 * 1024,
            maximum_mutations_per_batch: 2_048,
            maximum_paths_per_batch: 65_536,
            maximum_checkout_dependencies: 262_144,
            maximum_directory_page_entries: 1_024,
            maximum_page_height: 64,
            maximum_read_bytes: 16 * 1024 * 1024,
            maximum_files_per_generation: 16 * 1024 * 1024,
            maximum_objects_per_generation: 64 * 1024 * 1024,
            maximum_generation_bytes: 1024 * 1024 * 1024 * 1024,
        }
    }
}

/// Complete immutable volume configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VolumeConfig {
    /// Requested filesystem semantics.
    pub profile: FilesystemProfile,
    /// Writer coordination contract.
    pub concurrency: ConcurrencyMode,
    /// Persistence expectation.
    pub lifecycle: Lifecycle,
    /// Logical name comparison policy.
    pub case_sensitivity: CaseSensitivity,
    /// Unicode admission policy.
    pub unicode: UnicodePolicy,
    /// Whether symbolic links are representable.
    pub symbolic_links: bool,
    /// Whether multiple names may share one stable file record.
    pub hard_links: bool,
    /// Whether holes and allocated-zero extents remain distinct.
    pub sparse_files: bool,
    /// Mandatory resource and work bounds.
    pub limits: VolumeLimits,
}

impl VolumeConfig {
    /// Constructs the fully portable profile with explicit persistence.
    #[must_use]
    pub fn portable(lifecycle: Lifecycle) -> Self {
        Self {
            profile: FilesystemProfile::Portable,
            concurrency: ConcurrencyMode::Optimistic,
            lifecycle,
            case_sensitivity: CaseSensitivity::Sensitive,
            unicode: UnicodePolicy::Preserve,
            symbolic_links: true,
            hard_links: true,
            sparse_files: true,
            limits: VolumeLimits::default(),
        }
    }

    /// Validates all limits and cross-field semantic requirements.
    ///
    /// # Errors
    ///
    /// Returns a stable configuration error before any backend is opened.
    pub fn validate(self) -> Result<Self, VolumeConfigError> {
        let limits = self.limits;
        if limits.maximum_path_bytes == 0
            || limits.maximum_component_bytes == 0
            || limits.maximum_path_depth == 0
            || limits.maximum_object_bytes == 0
            || limits.maximum_mutations_per_batch == 0
            || limits.maximum_paths_per_batch == 0
            || limits.maximum_checkout_dependencies == 0
            || limits.maximum_directory_page_entries == 0
            || limits.maximum_page_height == 0
            || limits.maximum_read_bytes == 0
            || limits.maximum_files_per_generation == 0
            || limits.maximum_objects_per_generation == 0
            || limits.maximum_generation_bytes == 0
        {
            return Err(VolumeConfigError::ZeroLimit);
        }
        if limits.maximum_component_bytes > limits.maximum_path_bytes {
            return Err(VolumeConfigError::ComponentExceedsPath);
        }
        if limits.maximum_directory_page_entries < 2 {
            return Err(VolumeConfigError::InsufficientPageFanout);
        }
        Ok(self)
    }
}

/// Generation selected when a checkout opens.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GenerationSelector {
    /// Resolve the volume head once at checkout admission.
    Head,
    /// Open one exact immutable generation.
    Exact(GenerationId),
}

/// Complete checkout behavior chosen by a consumer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckoutMode {
    /// Whether mutations are admitted.
    pub access: AccessMode,
    /// How newer generations are observed.
    pub consistency: ConsistencyMode,
    /// Where admitted mutations accumulate.
    pub mutations: MutationMode,
}

impl CheckoutMode {
    /// Immutable exact-generation reader.
    #[must_use]
    pub const fn read_only_pinned() -> Self {
        Self {
            access: AccessMode::ReadOnly,
            consistency: ConsistencyMode::Pinned,
            mutations: MutationMode::None,
        }
    }

    /// Optimistic writable sparse overlay with observation-safe tracking.
    #[must_use]
    pub const fn tracking_transaction() -> Self {
        Self {
            access: AccessMode::ReadWrite,
            consistency: ConsistencyMode::TrackingSafe,
            mutations: MutationMode::PrivateOverlay,
        }
    }

    /// Validates cross-field checkout behavior.
    ///
    /// # Errors
    ///
    /// Rejects writable/read-only contradictions and direct-live use without
    /// live consistency.
    pub const fn validate(self) -> Result<Self, CheckoutModeError> {
        match (self.access, self.mutations, self.consistency) {
            (AccessMode::ReadOnly, MutationMode::None, _)
            | (AccessMode::ReadWrite, MutationMode::DirectLive, ConsistencyMode::Live)
            | (AccessMode::ReadWrite, MutationMode::PrivateOverlay, _) => Ok(self),
            (AccessMode::ReadOnly, _, _) => Err(CheckoutModeError::ReadOnlyMutation),
            (AccessMode::ReadWrite, MutationMode::None, _) => {
                Err(CheckoutModeError::WritableWithoutMutationMode)
            }
            (AccessMode::ReadWrite, MutationMode::DirectLive, _) => {
                Err(CheckoutModeError::DirectRequiresLive)
            }
        }
    }
}

/// Volume configuration failures.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum VolumeConfigError {
    /// Every work or allocation limit must be positive.
    #[error("volume limits must be non-zero")]
    ZeroLimit,
    /// One component cannot be larger than the complete admitted path.
    #[error("maximum component bytes exceeds maximum path bytes")]
    ComponentExceedsPath,
    /// Persistent trees require at least two entries per internal page to make progress.
    #[error("maximum directory page entries must be at least two")]
    InsufficientPageFanout,
}

/// Checkout configuration failures.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CheckoutModeError {
    /// A read-only checkout selected a mutation mode.
    #[error("read-only checkout cannot admit mutations")]
    ReadOnlyMutation,
    /// A writable checkout omitted its mutation mode.
    #[error("writable checkout requires a mutation mode")]
    WritableWithoutMutationMode,
    /// Direct mutation requires a live serialized view.
    #[error("direct mutation requires live consistency")]
    DirectRequiresLive,
}

#[cfg(test)]
#[path = "tests/model.rs"]
mod tests;
