//! Exact no-follow namespace-path resolution over authenticated frontiers.

use super::allocation::{KernelAccounting, KernelOwnedAllocation};
use super::file_read::ReadOwned;
use super::{
    AuthenticatedProbeError, DecodeLimits, Dependency, DependencyRegion, DependencyState, FileKind,
    FilePayload, FileRecord, FileRecordReadError, LogicalName, NamespacePath, NamespacePathError,
    TreeReadError,
};
use crate::async_storage::{self, AsyncObjectStore};
use crate::cancellation::CancellationToken;
use crate::foundation::usize_to_u64;
use crate::lock_order::MutexGuard;
#[cfg(test)]
use crate::model::FilesystemProfile;
use crate::model::{VolumeConfig, VolumeConfigError};
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{
    ObjectFailure, ObjectId, ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectStoreError,
};
use crate::sync::Mutex;
use acyclic_stream::OwnedBytes;
use std::cmp::Ordering;
use std::mem::size_of;
use thiserror::Error;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum CaptureRecords {
    None,
    Terminal,
    Prefixes,
}

/// Exact path lookup result. Absence is authenticated and not an error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PathLookup {
    /// Terminal path-independent record, or authenticated absence.
    pub record: Option<FileRecord>,
    /// Immediate parent directory record for a non-root terminal result.
    pub parent: Option<FileRecord>,
    /// Number of requested components resolved before terminal absence.
    pub resolved_components: u16,
    /// Exact file-table, namespace-page, backend, copy, and allocation work.
    pub work: WorkCounters,
}

/// One exact path result plus the canonical regions observed while resolving it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedPathLookup {
    /// Ordinary no-follow result.
    pub lookup: PathLookup,
    /// Positive namespace edges, terminal record, or exact negative edge.
    pub dependencies: Vec<Dependency>,
}

/// A shared path batch and its observed canonical regions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedPathBatch {
    /// Ordinary batch result.
    pub lookup: PathBatchLookup,
    /// Positive namespace edges and terminal records, or exact negative edges.
    pub dependencies: Vec<Dependency>,
}

/// One original-order result from a shared no-follow path batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PathBatchEntry {
    /// Terminal path-independent record, or authenticated absence.
    pub record: Option<FileRecord>,
    /// Immediate parent directory record for a non-root terminal result.
    pub parent: Option<FileRecord>,
    /// Number of requested components resolved before terminal absence.
    pub resolved_components: u16,
}

/// Original-order results and one non-duplicated operation receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PathBatchLookup {
    /// Exactly one result for every requested path, preserving duplicates.
    pub entries: Vec<PathBatchEntry>,
    /// Logical result-vector capacity retained by the caller.
    pub retained_allocation_bytes: u64,
    /// Exact shared-prefix, file-table, backend, copy, and allocation work.
    pub work: WorkCounters,
}

/// Path resolution failure retaining all completed work.
pub type PathLookupFailure = OperationFailure<PathLookupError>;

/// Fail-closed no-follow path resolution errors.
#[derive(Debug, Error)]
pub enum PathLookupError {
    /// Volume limits or semantics are invalid.
    #[error(transparent)]
    Config(#[from] VolumeConfigError),
    /// A shared path batch must contain at least one query.
    #[error("path lookup batch is empty")]
    EmptyBatch,
    /// A shared path batch exceeds the volume's explicit query bound.
    #[error("path lookup batch exceeds the volume limit")]
    TooManyPaths,
    /// The path was not admitted under the supplied volume bounds.
    #[error("namespace path exceeds the volume's admitted bounds")]
    PathBounds,
    /// A component encoding is not representable by this volume profile.
    #[error("namespace component encoding is unsupported by the volume profile")]
    UnsupportedNameEncoding,
    /// Operation-scoped acceleration metadata could not be allocated.
    #[error("path lookup acceleration allocation failed")]
    AllocationFailed,
    /// The authenticated generation root has no root file record.
    #[error("generation root file record is missing")]
    MissingRootRecord,
    /// A non-directory occurred before the terminal component.
    #[error("path traversal encountered a non-directory")]
    NotDirectory,
    /// Namespace binding kind and file-record kind disagree.
    #[error("namespace binding kind does not match its file record")]
    KindMismatch,
    /// File-table traversal failed.
    #[error(transparent)]
    FileTable(#[from] FileRecordReadError),
    /// Directory traversal failed.
    #[error(transparent)]
    Tree(#[from] TreeReadError),
    /// A bounded prefix path could not be represented.
    #[error(transparent)]
    NamespacePath(#[from] NamespacePathError),
    /// Canonical observation-state construction failed.
    #[error(transparent)]
    Dependency(#[from] AuthenticatedProbeError),
    /// Exact work overflowed or exceeded the request budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

mod memory;
use memory::*;
mod cache;
use cache::*;
mod provider;
mod single;
pub(crate) use single::resolve_path_with_accounting;
mod batch;
pub(crate) use batch::PathQueries;
use batch::*;
mod api;
pub(crate) use api::PathBatchMode;
pub use api::{
    lookup_path, lookup_path_refs, lookup_paths, observe_path_async, observe_paths_async,
};
pub(crate) use api::{
    lookup_path_async_with_accounting, lookup_paths_with_accounting,
    observe_path_async_with_accounting, observe_paths_async_with_accounting,
};

#[cfg(all(test, feature = "memory"))]
#[path = "../tests/path_access/mod.rs"]
mod tests;
pub(super) fn path_error(error: crate::FsError) -> PathLookupError {
    match error {
        crate::FsError::Path(error) => error,
        crate::FsError::Config(error) => PathLookupError::Config(error),
        crate::FsError::FileRecord(error) => PathLookupError::FileTable(error),
        crate::FsError::Object(ObjectStoreError::Work(error)) => PathLookupError::Work(error),
        crate::FsError::Object(error) => PathLookupError::Tree(TreeReadError::Storage(error)),
        crate::FsError::Work(error) => PathLookupError::Work(error),
        crate::FsError::Cancelled(_) => PathLookupError::Tree(TreeReadError::Cancelled),
        #[cfg(not(target_arch = "wasm32"))]
        crate::FsError::NativeKernel(error) => {
            PathLookupError::Tree(TreeReadError::Storage(ObjectStoreError::Native(error)))
        }
        crate::FsError::Decode(error) => PathLookupError::Tree(TreeReadError::Decode(error)),
        _ => PathLookupError::AllocationFailed,
    }
}

fn validate_path(
    path: &NamespacePath,
    config: VolumeConfig,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), crate::FsError> {
    config.validate()?;
    if path.encoded_bytes() > config.limits.maximum_path_bytes
        || path.depth() > usize::from(config.limits.maximum_path_depth)
    {
        return Err(PathLookupError::PathBounds.into());
    }
    let maximum_component_bytes =
        usize::try_from(config.limits.maximum_component_bytes).unwrap_or(usize::MAX);
    for component in path.components() {
        let (too_long, admitted_encoding) = examine_item(accounting, None, || {
            (
                component.as_bytes().len() > maximum_component_bytes,
                component.encoding().is_admitted_in_profile(config.profile),
            )
        })?;
        if too_long {
            return Err(PathLookupError::PathBounds.into());
        }
        if !admitted_encoding {
            return Err(PathLookupError::UnsupportedNameEncoding.into());
        }
    }
    Ok(())
}

fn maximum_cache_entries_for_components(
    components: usize,
    config: VolumeConfig,
) -> Result<usize, PathLookupFailure> {
    let component_frontiers = components
        .checked_mul(2)
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| OperationFailure::before_work(PathLookupError::Work(WorkError::Overflow)))?;
    let maximum = component_frontiers
        .checked_mul(usize::from(config.limits.maximum_page_height))
        .ok_or_else(|| OperationFailure::before_work(PathLookupError::Work(WorkError::Overflow)))?;
    Ok(maximum
        .min(usize::try_from(config.limits.maximum_objects_per_generation).unwrap_or(usize::MAX)))
}
