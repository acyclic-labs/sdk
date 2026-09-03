//! Exact no-follow namespace-path resolution over authenticated frontiers.

use super::allocation::{AllocationError, AllocationLedger};
use super::probe::{capture_directory_name_state, capture_file_record_state};
use super::{
    AuthenticatedProbeError, DecodeLimits, Dependency, DependencyRegion, DependencyState, FileKind,
    FilePayload, FileRecord, FileRecordReadError, LogicalName, NamespacePath, NamespacePathError,
    TreeReadError, lookup_file_record_async, lookup_file_records_async, lookup_tree_entries_async,
    lookup_tree_entry_async,
};
use crate::async_storage::{self, AsyncObjectStore};
use crate::cancellation::CancellationToken;
use crate::model::{FilesystemProfile, VolumeConfig, VolumeConfigError};
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{
    ObjectFailure, ObjectId, ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectReceipt,
    ObjectResult, ObjectStoreError,
};
use bytes::Bytes;
use std::cmp::Ordering;
use std::mem::size_of;
use std::sync::{Mutex, MutexGuard};
use thiserror::Error;

struct CachedObject {
    object_id: ObjectId,
    bytes: Bytes,
}

struct CacheState {
    slots: Vec<Option<CachedObject>>,
    entry_count: usize,
    retained_owned_bytes: u64,
    external_resident_bytes: u64,
}

struct OperationReadCache<'a, S> {
    store: &'a S,
    state: Mutex<CacheState>,
    maximum_entries: usize,
    metadata_bytes: u64,
}

impl<'a, S> OperationReadCache<'a, S> {
    fn new(
        store: &'a S,
        maximum_entries: usize,
        budget: WorkBudget,
    ) -> Result<(Self, WorkCounters), PathLookupFailure> {
        let slot_count = maximum_entries
            .checked_mul(2)
            .and_then(usize::checked_next_power_of_two)
            .ok_or_else(|| {
                OperationFailure::before_work(PathLookupError::Work(WorkError::Overflow))
            })?;
        let requested = u64::try_from(slot_count)
            .unwrap_or(u64::MAX)
            .checked_mul(u64::try_from(size_of::<Option<CachedObject>>()).unwrap_or(u64::MAX))
            .ok_or_else(|| {
                OperationFailure::before_work(PathLookupError::Work(WorkError::Overflow))
            })?;
        WorkCounters {
            allocation_operations: 1,
            peak_allocation_bytes: requested,
            ..WorkCounters::default()
        }
        .verify(budget)
        .map_err(|error| OperationFailure::before_work(error.into()))?;
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(slot_count)
            .map_err(|_| OperationFailure::before_work(PathLookupError::AllocationFailed))?;
        slots.resize_with(slot_count, || None);
        let metadata_bytes = u64::try_from(slots.capacity())
            .unwrap_or(u64::MAX)
            .checked_mul(u64::try_from(size_of::<Option<CachedObject>>()).unwrap_or(u64::MAX))
            .ok_or_else(|| {
                OperationFailure::before_work(PathLookupError::Work(WorkError::Overflow))
            })?;
        let work = WorkCounters {
            allocation_operations: 1,
            peak_allocation_bytes: metadata_bytes,
            ..WorkCounters::default()
        };
        work.verify(budget)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        Ok((
            Self {
                store,
                state: Mutex::new(CacheState {
                    slots,
                    entry_count: 0,
                    retained_owned_bytes: 0,
                    external_resident_bytes: 0,
                }),
                maximum_entries,
                metadata_bytes,
            },
            work,
        ))
    }

    fn probe(
        &self,
        object_id: ObjectId,
    ) -> Result<(Option<ObjectRead>, usize, u64), ObjectFailure> {
        let state = self.lock_state();
        let mask = state.slots.len() - 1;
        let mut index = object_hash(object_id) & mask;
        let mut examined = 0_u64;
        for _ in 0..state.slots.len() {
            examined = examined.saturating_add(1);
            match &state.slots[index] {
                Some(entry) if entry.object_id == object_id => {
                    return Ok((
                        Some(ObjectRead {
                            bytes: entry.bytes.clone(),
                            retention: ObjectReadRetention::Shared,
                        }),
                        index,
                        examined,
                    ));
                }
                Some(_) => index = (index + 1) & mask,
                None => return Ok((None, index, examined)),
            }
        }
        Err(ObjectFailure::new(
            ObjectStoreError::Corrupt,
            WorkCounters {
                items_examined: examined,
                ..WorkCounters::default()
            },
        ))
    }

    fn resident_bytes(&self) -> Result<u64, ObjectFailure> {
        let state = self.lock_state();
        self.metadata_bytes
            .checked_add(state.external_resident_bytes)
            .ok_or_else(|| ObjectFailure::before_work(ObjectStoreError::Work(WorkError::Overflow)))?
            .checked_add(state.retained_owned_bytes)
            .ok_or_else(|| ObjectFailure::before_work(ObjectStoreError::Work(WorkError::Overflow)))
    }

    fn add_external_resident_bytes(&self, bytes: u64) -> Result<u64, WorkError> {
        let mut state = self.lock_state();
        state.external_resident_bytes = state
            .external_resident_bytes
            .checked_add(bytes)
            .ok_or(WorkError::Overflow)?;
        Ok(state.external_resident_bytes)
    }

    fn lock_state(&self) -> MutexGuard<'_, CacheState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

fn object_hash(object_id: ObjectId) -> usize {
    let mut hash = usize::from(object_id.kind.canonical_tag());
    for byte in object_id.digest.as_bytes() {
        hash ^= usize::from(*byte);
        hash = hash.wrapping_mul(16_777_619);
    }
    hash
}

fn compare_object_id(left: ObjectId, right: ObjectId) -> Ordering {
    left.kind
        .canonical_tag()
        .cmp(&right.kind.canonical_tag())
        .then_with(|| left.digest.cmp(&right.digest))
}

fn budget_after_resident(
    mut budget: WorkBudget,
    resident: u64,
) -> Result<WorkBudget, ObjectFailure> {
    budget.peak_allocation_bytes = budget
        .peak_allocation_bytes
        .checked_sub(resident)
        .ok_or_else(|| {
            ObjectFailure::before_work(ObjectStoreError::Work(WorkError::BudgetExceeded {
                counter: "peak_allocation_bytes",
                observed: resident,
                maximum: budget.peak_allocation_bytes,
            }))
        })?;
    Ok(budget)
}

fn merge_backend_peak(
    prior: WorkCounters,
    mut backend: WorkCounters,
    resident: u64,
    budget: WorkBudget,
) -> Result<WorkCounters, ObjectFailure> {
    let simultaneous_peak = resident
        .checked_add(backend.peak_allocation_bytes)
        .ok_or_else(|| ObjectFailure::new(ObjectStoreError::Work(WorkError::Overflow), prior))?;
    backend.peak_allocation_bytes = 0;
    let mut work = prior
        .checked_add(backend)
        .map_err(|error| ObjectFailure::new(error.into(), prior))?;
    work.peak_allocation_bytes = work.peak_allocation_bytes.max(simultaneous_peak);
    work.verify(budget)
        .map_err(|error| ObjectFailure::new(error.into(), work))?;
    Ok(work)
}

fn merge_backend_failure(
    prior: WorkCounters,
    mut backend: WorkCounters,
    resident: u64,
    error: ObjectStoreError,
) -> ObjectFailure {
    let Some(simultaneous_peak) = resident.checked_add(backend.peak_allocation_bytes) else {
        return ObjectFailure::new(ObjectStoreError::Work(WorkError::Overflow), prior);
    };
    backend.peak_allocation_bytes = 0;
    let Ok(mut work) = prior.checked_add(backend) else {
        return ObjectFailure::new(ObjectStoreError::Work(WorkError::Overflow), prior);
    };
    work.peak_allocation_bytes = work.peak_allocation_bytes.max(simultaneous_peak);
    ObjectFailure::new(error, work)
}

impl<S: AsyncObjectStore> AsyncObjectStore for OperationReadCache<'_, S> {
    fn decoded_cache_get(
        &self,
        key: async_storage::DecodedCacheKey,
    ) -> Result<Option<async_storage::DecodedCacheValue>, ObjectStoreError> {
        self.store.decoded_cache_get(key)
    }

    fn decoded_cache_admit(
        &self,
        key: async_storage::DecodedCacheKey,
        value: async_storage::DecodedCacheValue,
    ) -> Result<async_storage::DecodedCacheAdmission, ObjectStoreError> {
        self.store.decoded_cache_admit(key, value)
    }

    async fn put(
        &self,
        object_id: ObjectId,
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        let resident = self.resident_bytes()?;
        let backend_budget = budget_after_resident(budget, resident)?;
        let receipt =
            AsyncObjectStore::put(self.store, object_id, bytes, backend_budget, cancellation)
                .await
                .map_err(|failure| {
                    merge_backend_failure(
                        WorkCounters::default(),
                        *failure.work,
                        resident,
                        failure.error,
                    )
                })?;
        let work = merge_backend_peak(WorkCounters::default(), receipt.work, resident, budget)?;
        Ok(ObjectReceipt { value: (), work })
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        let (hit, vacant_slot, examined) = self.probe(object_id)?;
        let hit_work = WorkCounters {
            items_examined: examined,
            ..WorkCounters::default()
        };
        if let Some(value) = hit {
            let observed = u64::try_from(value.bytes.len()).unwrap_or(u64::MAX);
            if observed > maximum_bytes {
                return Err(ObjectFailure::new(
                    ObjectStoreError::TooLarge {
                        observed,
                        maximum: maximum_bytes,
                    },
                    hit_work,
                ));
            }
            hit_work
                .verify(budget)
                .map_err(|error| ObjectFailure::new(error.into(), hit_work))?;
            return Ok(ObjectReceipt {
                value,
                work: hit_work,
            });
        }

        let resident = self.resident_bytes()?;
        let backend_budget = budget_after_resident(budget, resident)?;
        let receipt = AsyncObjectStore::read(
            self.store,
            object_id,
            maximum_bytes,
            backend_budget,
            cancellation,
        )
        .await
        .map_err(|failure| {
            merge_backend_failure(hit_work, *failure.work, resident, failure.error)
        })?;
        let owned_bytes = match receipt.value.retention {
            ObjectReadRetention::Shared => 0,
            ObjectReadRetention::Owned { logical_bytes } => logical_bytes,
        };
        let work = merge_backend_peak(hit_work, receipt.work, resident, budget)?;
        let mut state = self.lock_state();
        if state.entry_count < self.maximum_entries && state.slots[vacant_slot].is_none() {
            state.slots[vacant_slot] = Some(CachedObject {
                object_id,
                bytes: receipt.value.bytes.clone(),
            });
            state.entry_count += 1;
            state.retained_owned_bytes = state
                .retained_owned_bytes
                .checked_add(owned_bytes)
                .ok_or_else(|| {
                    ObjectFailure::new(ObjectStoreError::Work(WorkError::Overflow), work)
                })?;
        }
        Ok(ObjectReceipt {
            value: receipt.value,
            work,
        })
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        async_storage::read_many_sequential_async(self, requests, budget, cancellation).await
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        let (hit, _, examined) = self.probe(object_id)?;
        if hit.is_some() {
            let work = WorkCounters {
                items_examined: examined,
                ..WorkCounters::default()
            };
            work.verify(budget)
                .map_err(|error| ObjectFailure::before_work(error.into()))?;
            return Ok(ObjectReceipt { value: true, work });
        }
        let prior = WorkCounters {
            items_examined: examined,
            ..WorkCounters::default()
        };
        let resident = self.resident_bytes()?;
        let remaining = prior
            .remaining(budget)
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        let backend_budget = budget_after_resident(remaining, resident)?;
        let receipt =
            AsyncObjectStore::contains(self.store, object_id, backend_budget, cancellation)
                .await
                .map_err(|failure| {
                    merge_backend_failure(prior, *failure.work, resident, failure.error)
                })?;
        let work = merge_backend_peak(prior, receipt.work, resident, budget)?;
        Ok(ObjectReceipt {
            value: receipt.value,
            work,
        })
    }
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

#[derive(Clone, Copy)]
struct PendingBinding {
    path_index: usize,
    file_id: crate::foundation::FileId,
    kind: FileKind,
}

#[derive(Clone, Copy)]
struct ActivePath {
    path_index: usize,
    directory: ObjectId,
}

#[derive(Clone, Copy)]
enum PathQueries<'a> {
    Owned(&'a [NamespacePath]),
    Borrowed(&'a [&'a NamespacePath]),
}

impl<'a> PathQueries<'a> {
    const fn len(self) -> usize {
        match self {
            Self::Owned(paths) => paths.len(),
            Self::Borrowed(paths) => paths.len(),
        }
    }

    const fn is_empty(self) -> bool {
        self.len() == 0
    }

    fn get(self, index: usize) -> &'a NamespacePath {
        match self {
            Self::Owned(paths) => &paths[index],
            Self::Borrowed(paths) => paths[index],
        }
    }
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

/// Resolves one exact path without following terminal or intermediate links.
///
/// This synchronous surface drives the same asynchronous composition used by
/// browser and remote stores. It reads only the file-table and directory
/// frontiers required by the requested path and never reads file bodies or
/// enumerates unrelated namespace subtrees.
///
/// # Errors
///
/// Rejects unsupported name semantics/encodings before backend work, malformed
/// authenticated routing, kind mismatches, non-directory intermediates,
/// storage/cancellation failures, and work outside the admitted budget.
pub fn lookup_path<S: crate::ImmediateObjectStore>(
    store: &S,
    generation: &super::GenerationRoot,
    path: &NamespacePath,
    config: VolumeConfig,
    budget: WorkBudget,
) -> Result<PathLookup, PathLookupFailure> {
    let cancellation = CancellationToken::new();
    async_storage::poll_immediate(lookup_path_async(
        store,
        generation,
        path,
        config,
        budget,
        &cancellation,
    ))
}

/// Asynchronously resolves one exact no-follow path through authenticated frontiers.
///
/// # Errors
///
/// Returns the same typed failures and exact receipts as [`lookup_path`], plus
/// cooperative cancellation from the object-store boundary.
pub async fn lookup_path_async<S: AsyncObjectStore>(
    store: &S,
    generation: &super::GenerationRoot,
    path: &NamespacePath,
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<PathLookup, PathLookupFailure> {
    cancellation.check().map_err(|_| {
        OperationFailure::before_work(PathLookupError::Tree(TreeReadError::Cancelled))
    })?;
    validate_path(path, config)?;
    let limits = decode_limits(config);
    let maximum_cache_entries = maximum_cache_entries(path, config)?;
    let (cache, mut work) = OperationReadCache::new(store, maximum_cache_entries, budget)?;
    let root = lookup_file_record_async(
        &cache,
        generation.file_table,
        generation.root_file_id,
        limits,
        remaining(work, budget)?,
        cancellation,
    )
    .await
    .map_err(|failure| failure.map_with_prior_work(work, PathLookupError::FileTable))?;
    work = add(work, root.work)?;
    let mut current = root
        .record
        .ok_or_else(|| OperationFailure::new(PathLookupError::MissingRootRecord, work))?;
    if path.is_root() {
        return Ok(PathLookup {
            record: Some(current),
            parent: None,
            resolved_components: 0,
            work,
        });
    }

    let mut parent = None;
    for (index, component) in path.components().iter().enumerate() {
        parent = Some(current);
        let FilePayload::Directory { entries } = current.payload else {
            return Err(OperationFailure::new(PathLookupError::NotDirectory, work));
        };
        if current.kind != FileKind::Directory {
            return Err(OperationFailure::new(PathLookupError::KindMismatch, work));
        }
        let binding = lookup_tree_entry_async(
            &cache,
            entries,
            component,
            limits,
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, PathLookupError::Tree))?;
        work = add(work, binding.work)?;
        let Some(binding) = binding.entry else {
            return Ok(PathLookup {
                record: None,
                parent,
                resolved_components: u16::try_from(index).unwrap_or(u16::MAX),
                work,
            });
        };
        let record = lookup_file_record_async(
            &cache,
            generation.file_table,
            binding.file_id,
            limits,
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, PathLookupError::FileTable))?;
        work = add(work, record.work)?;
        current = record
            .record
            .ok_or_else(|| OperationFailure::new(PathLookupError::KindMismatch, work))?;
        if current.kind != binding.kind {
            return Err(OperationFailure::new(PathLookupError::KindMismatch, work));
        }
    }
    Ok(PathLookup {
        record: Some(current),
        parent,
        resolved_components: u16::try_from(path.depth()).unwrap_or(u16::MAX),
        work,
    })
}

/// Resolves one path and captures the exact semantic regions needed for a safe rebase.
///
/// Resolution walks the path once through an operation-local authenticated
/// object cache, so work is proportional to depth and no prefix paths are
/// constructed or rescanned.
/// Negative lookups capture the first absent edge; positive lookups capture
/// every namespace edge and the terminal path-independent record.
///
/// # Errors
///
/// Returns the same measured failures as [`lookup_path_async`], plus canonical
/// dependency-state construction failures.
pub async fn observe_path_async<S: AsyncObjectStore>(
    store: &S,
    generation: &super::GenerationRoot,
    path: &NamespacePath,
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<ObservedPathLookup, PathLookupFailure> {
    observe_path_with_terminal_async(store, generation, path, config, budget, cancellation, true)
        .await
}

/// Resolves one path while retaining namespace-edge observations but omitting
/// the terminal whole-record observation.
///
/// This is the internal primitive used by metadata, content-range, and
/// directory-page reads. Those operations append their narrower terminal
/// region after resolution, so unrelated changes to the same file record do
/// not create false rebase conflicts. Exact negative lookups remain captured.
pub(crate) async fn observe_path_edges_async<S: AsyncObjectStore>(
    store: &S,
    generation: &super::GenerationRoot,
    path: &NamespacePath,
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<ObservedPathLookup, PathLookupFailure> {
    observe_path_with_terminal_async(store, generation, path, config, budget, cancellation, false)
        .await
}

async fn observe_path_with_terminal_async<S: AsyncObjectStore>(
    store: &S,
    generation: &super::GenerationRoot,
    path: &NamespacePath,
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    capture_terminal: bool,
) -> Result<ObservedPathLookup, PathLookupFailure> {
    cancellation.check().map_err(|_| {
        OperationFailure::before_work(PathLookupError::Tree(TreeReadError::Cancelled))
    })?;
    validate_path(path, config)?;

    let limits = decode_limits(config);
    let maximum_cache_entries = maximum_cache_entries(path, config)?;
    let (cache, mut work) = OperationReadCache::new(store, maximum_cache_entries, budget)?;
    let mut allocations = AllocationLedger::default();
    let mut dependencies = reserve_fixed::<Dependency>(
        path.depth().saturating_add(1),
        &mut allocations,
        &mut work,
        budget,
    )?;
    let dependency_vector_bytes = allocations.live_bytes();
    cache
        .add_external_resident_bytes(dependency_vector_bytes)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    let combined_resident = cache
        .resident_bytes()
        .map_err(|failure| OperationFailure::new(map_cache_error(failure.error), work))?;
    work.peak_allocation_bytes = work.peak_allocation_bytes.max(combined_resident);
    work.verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;

    let root = lookup_file_record_async(
        &cache,
        generation.file_table,
        generation.root_file_id,
        limits,
        remaining(work, budget)?,
        cancellation,
    )
    .await
    .map_err(|failure| failure.map_with_prior_work(work, PathLookupError::FileTable))?;
    work = add(work, root.work)?;
    let current = root
        .record
        .ok_or_else(|| OperationFailure::new(PathLookupError::MissingRootRecord, work))?;
    if path.is_root() {
        if !capture_terminal {
            return Ok(ObservedPathLookup {
                lookup: PathLookup {
                    record: Some(current),
                    parent: None,
                    resolved_components: 0,
                    work,
                },
                dependencies,
            });
        }
        let state =
            capture_file_record_state(current, remaining(work, budget)?, WorkCounters::default())
                .map_err(|failure| failure.map_with_prior_work(work, PathLookupError::Dependency))?;
        work = add(work, state.work)?;
        dependencies.push(Dependency {
            region: DependencyRegion::FileRecord(current.file_id),
            expected: state.value,
        });
        return Ok(ObservedPathLookup {
            lookup: PathLookup {
                record: Some(current),
                parent: None,
                resolved_components: 0,
                work,
            },
            dependencies,
        });
    }

    observe_descendants(
        ObserveContext {
            cache: &cache,
            generation,
            path,
            limits,
            budget,
            cancellation,
            capture_terminal,
        },
        current,
        dependencies,
        dependency_vector_bytes,
        work,
    )
    .await
}

struct ObserveContext<'a, S> {
    cache: &'a OperationReadCache<'a, S>,
    generation: &'a super::GenerationRoot,
    path: &'a NamespacePath,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &'a CancellationToken,
    capture_terminal: bool,
}

#[allow(clippy::too_many_lines)]
async fn observe_descendants<S: AsyncObjectStore>(
    context: ObserveContext<'_, S>,
    mut current: FileRecord,
    mut dependencies: Vec<Dependency>,
    mut external_resident_bytes: u64,
    mut work: WorkCounters,
) -> Result<ObservedPathLookup, PathLookupFailure> {
    let mut parent = None;
    for (index, component) in context.path.components().iter().enumerate() {
        let copied = copy_observation_name(
            context.cache,
            component,
            context.limits.maximum_name_bytes,
            external_resident_bytes,
            work,
            context.budget,
        )?;
        let dependency_name = copied.name;
        external_resident_bytes = copied.external_resident_bytes;
        work = copied.work;
        parent = Some(current);
        let FilePayload::Directory { entries } = current.payload else {
            return Err(OperationFailure::new(PathLookupError::NotDirectory, work));
        };
        if current.kind != FileKind::Directory {
            return Err(OperationFailure::new(PathLookupError::KindMismatch, work));
        }
        let binding = lookup_tree_entry_async(
            context.cache,
            entries,
            component,
            context.limits,
            remaining(work, context.budget)?,
            context.cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, PathLookupError::Tree))?;
        work = add(work, binding.work)?;
        let Some(binding) = binding.entry else {
            dependencies.push(Dependency {
                region: DependencyRegion::DirectoryName {
                    directory_id: current.file_id,
                    name: dependency_name,
                },
                expected: DependencyState::Absent,
            });
            return Ok(ObservedPathLookup {
                lookup: PathLookup {
                    record: None,
                    parent,
                    resolved_components: u16::try_from(index).unwrap_or(u16::MAX),
                    work,
                },
                dependencies,
            });
        };
        let state = capture_directory_name_state(
            &binding,
            remaining(work, context.budget)?,
            WorkCounters::default(),
        )
        .map_err(|failure| failure.map_with_prior_work(work, PathLookupError::Dependency))?;
        work = add(work, state.work)?;
        let binding_file_id = binding.file_id;
        let binding_kind = binding.kind;
        dependencies.push(Dependency {
            region: DependencyRegion::DirectoryName {
                directory_id: current.file_id,
                name: dependency_name,
            },
            expected: state.value,
        });
        let record = lookup_file_record_async(
            context.cache,
            context.generation.file_table,
            binding_file_id,
            context.limits,
            remaining(work, context.budget)?,
            context.cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, PathLookupError::FileTable))?;
        work = add(work, record.work)?;
        current = record
            .record
            .ok_or_else(|| OperationFailure::new(PathLookupError::KindMismatch, work))?;
        if current.kind != binding_kind {
            return Err(OperationFailure::new(PathLookupError::KindMismatch, work));
        }
    }
    if context.capture_terminal {
        let state = capture_file_record_state(
            current,
            remaining(work, context.budget)?,
            WorkCounters::default(),
        )
        .map_err(|failure| failure.map_with_prior_work(work, PathLookupError::Dependency))?;
        work = add(work, state.work)?;
        dependencies.push(Dependency {
            region: DependencyRegion::FileRecord(current.file_id),
            expected: state.value,
        });
    }
    Ok(ObservedPathLookup {
        lookup: PathLookup {
            record: Some(current),
            parent,
            resolved_components: u16::try_from(context.path.depth()).unwrap_or(u16::MAX),
            work,
        },
        dependencies,
    })
}

/// Resolves a non-empty exact path batch through shared authenticated prefixes.
///
/// Input order and duplicates are preserved. Paths sharing a directory prefix
/// use one directory-frontier batch, while all bindings discovered at one depth
/// use one shared file-table batch. No file body or unrelated subtree is read.
///
/// # Errors
///
/// Returns the same fail-closed semantic and storage errors as [`lookup_path`],
/// plus explicit empty/excessive batch and allocation failures.
pub fn lookup_paths<S: crate::ImmediateObjectStore>(
    store: &S,
    generation: &super::GenerationRoot,
    paths: &[NamespacePath],
    config: VolumeConfig,
    budget: WorkBudget,
) -> Result<PathBatchLookup, PathLookupFailure> {
    let cancellation = CancellationToken::new();
    async_storage::poll_immediate(lookup_paths_async(
        store,
        generation,
        paths,
        config,
        budget,
        &cancellation,
    ))
}

/// Asynchronously resolves a non-empty no-follow path batch with shared work.
///
/// # Errors
///
/// Returns exact operation-wide work on cancellation, malformed authenticated
/// routing, semantic mismatch, storage failure, or resource-bound rejection.
pub async fn lookup_paths_async<S: AsyncObjectStore>(
    store: &S,
    generation: &super::GenerationRoot,
    paths: &[NamespacePath],
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<PathBatchLookup, PathLookupFailure> {
    lookup_path_queries_async(
        store,
        generation,
        PathQueries::Owned(paths),
        config,
        budget,
        cancellation,
    )
    .await
}

/// Resolves borrowed paths without first cloning their names into an owned batch.
///
/// This is the mutation-planner and latency-sensitive batch surface. Input
/// order, duplicates, semantics, and receipts are identical to [`lookup_paths`].
///
/// # Errors
///
/// Returns the same bounded, fail-closed outcomes as [`lookup_paths_async`].
pub async fn lookup_path_refs_async<S: AsyncObjectStore>(
    store: &S,
    generation: &super::GenerationRoot,
    paths: &[&NamespacePath],
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<PathBatchLookup, PathLookupFailure> {
    lookup_path_queries_async(
        store,
        generation,
        PathQueries::Borrowed(paths),
        config,
        budget,
        cancellation,
    )
    .await
}

/// Synchronously resolves borrowed paths without constructing owned path copies.
///
/// # Errors
///
/// Returns the same bounded, fail-closed outcomes as [`lookup_paths`].
pub fn lookup_path_refs<S: crate::ImmediateObjectStore>(
    store: &S,
    generation: &super::GenerationRoot,
    paths: &[&NamespacePath],
    config: VolumeConfig,
    budget: WorkBudget,
) -> Result<PathBatchLookup, PathLookupFailure> {
    async_storage::poll_immediate(lookup_path_refs_async(
        store,
        generation,
        paths,
        config,
        budget,
        &CancellationToken::new(),
    ))
}

#[allow(clippy::too_many_lines)]
async fn lookup_path_queries_async<S: AsyncObjectStore>(
    store: &S,
    generation: &super::GenerationRoot,
    paths: PathQueries<'_>,
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<PathBatchLookup, PathLookupFailure> {
    cancellation.check().map_err(|_| {
        OperationFailure::before_work(PathLookupError::Tree(TreeReadError::Cancelled))
    })?;
    config
        .validate()
        .map_err(|error| OperationFailure::before_work(error.into()))?;
    if paths.is_empty() {
        return Err(OperationFailure::before_work(PathLookupError::EmptyBatch));
    }
    if u32::try_from(paths.len()).unwrap_or(u32::MAX) > config.limits.maximum_paths_per_batch {
        return Err(OperationFailure::before_work(PathLookupError::TooManyPaths));
    }
    for index in 0..paths.len() {
        validate_path(paths.get(index), config)?;
    }

    let total_components = (0..paths.len())
        .try_fold(0_usize, |total, index| {
            total
                .checked_add(paths.get(index).depth())
                .ok_or(PathLookupError::Work(WorkError::Overflow))
        })
        .map_err(OperationFailure::before_work)?;
    let cache_entries = maximum_cache_entries_for_components(total_components, config)?;
    let (cache, mut work) = OperationReadCache::new(store, cache_entries, budget)?;
    let mut allocations = AllocationLedger::default();
    allocations
        .claim_bytes(cache.metadata_bytes, 0, &mut work, budget)
        .map_err(|error| allocation_failure(error, work))?;

    let count = paths.len();
    let mut entries = reserve_fixed::<PathBatchEntry>(count, &mut allocations, &mut work, budget)?;
    let mut current =
        reserve_fixed::<Option<FileRecord>>(count, &mut allocations, &mut work, budget)?;
    let mut done = reserve_fixed::<u8>(count, &mut allocations, &mut work, budget)?;
    let mut active = reserve_fixed::<ActivePath>(count, &mut allocations, &mut work, budget)?;
    let mut names = reserve_fixed::<LogicalName>(count, &mut allocations, &mut work, budget)?;
    let mut query_indices = reserve_fixed::<usize>(count, &mut allocations, &mut work, budget)?;
    let mut pending = reserve_fixed::<PendingBinding>(count, &mut allocations, &mut work, budget)?;
    let mut file_ids =
        reserve_fixed::<crate::foundation::FileId>(count, &mut allocations, &mut work, budget)?;
    entries.resize(
        count,
        PathBatchEntry {
            record: None,
            parent: None,
            resolved_components: 0,
        },
    );
    current.resize(count, None);
    done.resize(count, 0);
    let limits = decode_limits(config);
    let root = lookup_file_record_async(
        &cache,
        generation.file_table,
        generation.root_file_id,
        limits,
        batch_sub_budget(work, budget, orchestration_live(&cache, allocations)?)?,
        cancellation,
    )
    .await
    .map_err(|failure| {
        merge_batch_failure(
            work,
            *failure.work,
            orchestration_live(&cache, allocations).unwrap_or(u64::MAX),
            PathLookupError::FileTable(failure.error),
        )
    })?;
    work = merge_batch_work(
        work,
        root.work,
        orchestration_live(&cache, allocations)?,
        budget,
    )?;
    let root_record = root
        .record
        .ok_or_else(|| OperationFailure::new(PathLookupError::MissingRootRecord, work))?;
    for index in 0..paths.len() {
        let path = paths.get(index);
        if path.is_root() {
            entries[index].record = Some(root_record);
            done[index] = 1;
        } else {
            current[index] = Some(root_record);
        }
    }

    let maximum_depth = (0..paths.len())
        .map(|index| paths.get(index).depth())
        .max()
        .unwrap_or(0);
    for depth in 0..maximum_depth {
        pending.clear();
        active.clear();
        for index in 0..paths.len() {
            if done[index] != 0 || paths.get(index).depth() <= depth {
                continue;
            }
            let FilePayload::Directory { entries: directory } = current[index]
                .ok_or_else(|| OperationFailure::new(PathLookupError::KindMismatch, work))?
                .payload
            else {
                return Err(OperationFailure::new(PathLookupError::NotDirectory, work));
            };
            active.push(ActivePath {
                path_index: index,
                directory,
            });
        }
        let comparisons = std::cell::Cell::new(0_u64);
        active.sort_unstable_by(|left, right| {
            comparisons.set(comparisons.get().saturating_add(1));
            compare_object_id(left.directory, right.directory)
                .then_with(|| left.path_index.cmp(&right.path_index))
        });
        work = work
            .checked_add(WorkCounters {
                items_examined: comparisons.get(),
                ..WorkCounters::default()
            })
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        work.verify(budget)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        let mut cursor = 0_usize;
        while cursor < active.len() {
            let directory = active[cursor].directory;
            let group_start = cursor;
            cursor += 1;
            while cursor < active.len() && active[cursor].directory == directory {
                cursor += 1;
            }
            names.clear();
            query_indices.clear();
            for item in &active[group_start..cursor] {
                query_indices.push(item.path_index);
            }
            let mut nested_bytes = 0_u64;
            for index in &query_indices {
                let source = &paths.get(*index).components()[depth];
                let (name, owned_bytes) = copy_batch_name(
                    source,
                    config.limits.maximum_component_bytes,
                    &mut allocations,
                    &mut work,
                    budget,
                )?;
                nested_bytes = nested_bytes.checked_add(owned_bytes).ok_or_else(|| {
                    OperationFailure::new(PathLookupError::Work(WorkError::Overflow), work)
                })?;
                names.push(name);
            }
            let looked_up = lookup_tree_entries_async(
                &cache,
                directory,
                &names,
                config.limits.maximum_paths_per_batch,
                limits,
                batch_sub_budget(work, budget, orchestration_live(&cache, allocations)?)?,
                cancellation,
            )
            .await
            .map_err(|failure| {
                merge_batch_failure(
                    work,
                    *failure.work,
                    orchestration_live(&cache, allocations).unwrap_or(u64::MAX),
                    PathLookupError::Tree(failure.error),
                )
            })?;
            work = merge_batch_work(
                work,
                looked_up.work,
                orchestration_live(&cache, allocations)?,
                budget,
            )?;
            for (index, binding) in query_indices.iter().zip(looked_up.entries) {
                if let Some(binding) = binding {
                    pending.push(PendingBinding {
                        path_index: *index,
                        file_id: binding.file_id,
                        kind: binding.kind,
                    });
                } else {
                    entries[*index] = PathBatchEntry {
                        record: None,
                        parent: current[*index],
                        resolved_components: u16::try_from(depth).unwrap_or(u16::MAX),
                    };
                    done[*index] = 1;
                }
            }
            names.clear();
            allocations
                .release(nested_bytes)
                .map_err(|error| allocation_failure(error, work))?;
        }
        if pending.is_empty() {
            continue;
        }
        file_ids.clear();
        file_ids.extend(pending.iter().map(|binding| binding.file_id));
        let records = lookup_file_records_async(
            &cache,
            generation.file_table,
            &file_ids,
            config.limits.maximum_paths_per_batch,
            limits,
            batch_sub_budget(work, budget, orchestration_live(&cache, allocations)?)?,
            cancellation,
        )
        .await
        .map_err(|failure| {
            merge_batch_failure(
                work,
                *failure.work,
                orchestration_live(&cache, allocations).unwrap_or(u64::MAX),
                PathLookupError::FileTable(failure.error),
            )
        })?;
        work = merge_batch_work(
            work,
            records.work,
            orchestration_live(&cache, allocations)?,
            budget,
        )?;
        for (binding, record) in pending.iter().zip(records.records) {
            let record =
                record.ok_or_else(|| OperationFailure::new(PathLookupError::KindMismatch, work))?;
            if record.kind != binding.kind {
                return Err(OperationFailure::new(PathLookupError::KindMismatch, work));
            }
            let next_depth = depth + 1;
            if next_depth == paths.get(binding.path_index).depth() {
                entries[binding.path_index] = PathBatchEntry {
                    record: Some(record),
                    parent: current[binding.path_index],
                    resolved_components: u16::try_from(next_depth).unwrap_or(u16::MAX),
                };
                done[binding.path_index] = 1;
                current[binding.path_index] = None;
            } else {
                current[binding.path_index] = Some(record);
            }
        }
    }
    if done.contains(&0) {
        return Err(OperationFailure::new(PathLookupError::KindMismatch, work));
    }
    let retained_allocation_bytes = u64::try_from(entries.capacity())
        .unwrap_or(u64::MAX)
        .checked_mul(u64::try_from(size_of::<PathBatchEntry>()).unwrap_or(u64::MAX))
        .ok_or_else(|| OperationFailure::new(PathLookupError::Work(WorkError::Overflow), work))?;
    Ok(PathBatchLookup {
        entries,
        retained_allocation_bytes,
        work,
    })
}

fn validate_path(path: &NamespacePath, config: VolumeConfig) -> Result<(), PathLookupFailure> {
    config
        .validate()
        .map_err(|error| OperationFailure::before_work(error.into()))?;
    if path.encoded_bytes() > config.limits.maximum_path_bytes
        || path.depth() > usize::from(config.limits.maximum_path_depth)
    {
        return Err(OperationFailure::before_work(PathLookupError::PathBounds));
    }
    let maximum_component_bytes =
        usize::try_from(config.limits.maximum_component_bytes).unwrap_or(usize::MAX);
    for component in path.components() {
        if component.as_bytes().len() > maximum_component_bytes {
            return Err(OperationFailure::before_work(PathLookupError::PathBounds));
        }
        if !matches!(
            (config.profile, component.encoding()),
            (
                FilesystemProfile::Portable | FilesystemProfile::Browser,
                super::NameEncoding::Utf8
            ) | (
                FilesystemProfile::Posix,
                super::NameEncoding::Utf8 | super::NameEncoding::PosixBytes
            ) | (
                FilesystemProfile::Windows,
                super::NameEncoding::Utf8 | super::NameEncoding::WindowsUtf16Le
            )
        ) {
            return Err(OperationFailure::before_work(
                PathLookupError::UnsupportedNameEncoding,
            ));
        }
    }
    Ok(())
}

fn decode_limits(config: VolumeConfig) -> DecodeLimits {
    DecodeLimits {
        maximum_object_bytes: config.limits.maximum_object_bytes,
        maximum_name_bytes: config.limits.maximum_component_bytes,
        maximum_page_items: config.limits.maximum_directory_page_entries,
        maximum_page_bytes: u32::try_from(config.limits.maximum_object_bytes).unwrap_or(u32::MAX),
        maximum_page_height: config.limits.maximum_page_height,
        maximum_visited_pages: u32::try_from(config.limits.maximum_objects_per_generation)
            .unwrap_or(u32::MAX),
    }
}

fn reserve_fixed<T>(
    count: usize,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<Vec<T>, PathLookupFailure> {
    let bytes = allocations
        .claim_elements::<T>(count, work, budget)
        .map_err(|error| allocation_failure(error, *work))?;
    let mut values = Vec::new();
    if values.try_reserve_exact(count).is_err() {
        allocations
            .release(bytes)
            .map_err(|error| allocation_failure(error, *work))?;
        return Err(OperationFailure::new(
            PathLookupError::AllocationFailed,
            *work,
        ));
    }
    Ok(values)
}

fn copy_batch_name(
    source: &LogicalName,
    maximum_bytes: u32,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<(LogicalName, u64), PathLookupFailure> {
    let bytes = crate::foundation::usize_to_u64(source.as_bytes().len());
    let attempted = work
        .checked_add(WorkCounters {
            allocation_operations: 1,
            ..WorkCounters::default()
        })
        .map_err(|error| OperationFailure::new(error.into(), *work))?;
    attempted
        .verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), *work))?;
    *work = attempted;

    let mut copied = Vec::new();
    copied
        .try_reserve_exact(source.as_bytes().len())
        .map_err(|_| OperationFailure::new(PathLookupError::AllocationFailed, *work))?;
    allocations
        .claim_bytes(bytes, 0, work, budget)
        .map_err(|error| allocation_failure(error, *work))?;
    let copied_work = work
        .checked_add(WorkCounters {
            bytes_copied: bytes,
            ..WorkCounters::default()
        })
        .map_err(|error| OperationFailure::new(error.into(), *work))?;
    copied_work
        .verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), *work))?;
    copied.extend_from_slice(source.as_bytes());
    *work = copied_work;
    let name = LogicalName::new(source.encoding(), copied, maximum_bytes)
        .map_err(|_| OperationFailure::new(PathLookupError::KindMismatch, *work))?;
    Ok((name, bytes))
}

struct CopiedObservationName {
    name: LogicalName,
    external_resident_bytes: u64,
    work: WorkCounters,
}

fn copy_observation_name<S>(
    cache: &OperationReadCache<'_, S>,
    source: &LogicalName,
    maximum_bytes: u32,
    external_resident_bytes: u64,
    work: WorkCounters,
    budget: WorkBudget,
) -> Result<CopiedObservationName, PathLookupFailure> {
    let bytes = crate::foundation::usize_to_u64(source.as_bytes().len());
    let next_external = external_resident_bytes
        .checked_add(bytes)
        .ok_or_else(|| OperationFailure::new(PathLookupError::Work(WorkError::Overflow), work))?;
    let resident_before = cache
        .resident_bytes()
        .map_err(|failure| OperationFailure::new(map_cache_error(failure.error), work))?;
    let simultaneous = resident_before
        .checked_add(bytes)
        .ok_or_else(|| OperationFailure::new(PathLookupError::Work(WorkError::Overflow), work))?;
    let allocation_attempt = work
        .checked_add(WorkCounters {
            allocation_operations: 1,
            ..WorkCounters::default()
        })
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    let mut admitted = allocation_attempt;
    admitted.peak_allocation_bytes = admitted.peak_allocation_bytes.max(simultaneous);
    admitted
        .verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;

    let mut copied = Vec::new();
    copied
        .try_reserve_exact(source.as_bytes().len())
        .map_err(|_| {
            OperationFailure::new(PathLookupError::AllocationFailed, allocation_attempt)
        })?;
    let copied_work = admitted
        .checked_add(WorkCounters {
            bytes_copied: bytes,
            ..WorkCounters::default()
        })
        .map_err(|error| OperationFailure::new(error.into(), admitted))?;
    copied_work
        .verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), admitted))?;
    copied.extend_from_slice(source.as_bytes());
    let name = LogicalName::new(source.encoding(), copied, maximum_bytes)
        .map_err(|_| OperationFailure::new(PathLookupError::KindMismatch, copied_work))?;
    let retained = cache
        .add_external_resident_bytes(bytes)
        .map_err(|error| OperationFailure::new(error.into(), copied_work))?;
    if retained != next_external {
        return Err(OperationFailure::new(
            PathLookupError::Work(WorkError::Overflow),
            copied_work,
        ));
    }
    Ok(CopiedObservationName {
        name,
        external_resident_bytes: retained,
        work: copied_work,
    })
}

fn allocation_failure(error: AllocationError, work: WorkCounters) -> PathLookupFailure {
    match error {
        AllocationError::Work(error) => OperationFailure::new(error.into(), work),
        AllocationError::Overflow | AllocationError::ReleaseInvariant => {
            OperationFailure::new(PathLookupError::Work(WorkError::Overflow), work)
        }
        AllocationError::InvalidCapacity
        | AllocationError::CapacityExceeded
        | AllocationError::AllocationFailed => {
            OperationFailure::new(PathLookupError::AllocationFailed, work)
        }
    }
}

fn map_cache_error(error: ObjectStoreError) -> PathLookupError {
    match error {
        ObjectStoreError::Work(error) => PathLookupError::Work(error),
        error => PathLookupError::Tree(TreeReadError::Storage(error)),
    }
}

fn orchestration_live<S>(
    cache: &OperationReadCache<'_, S>,
    allocations: AllocationLedger,
) -> Result<u64, PathLookupFailure> {
    allocations
        .live_bytes()
        .checked_sub(cache.metadata_bytes)
        .ok_or_else(|| OperationFailure::before_work(PathLookupError::Work(WorkError::Overflow)))
}

fn batch_sub_budget(
    work: WorkCounters,
    budget: WorkBudget,
    orchestration_live: u64,
) -> Result<WorkBudget, PathLookupFailure> {
    let mut remaining = remaining(work, budget)?;
    remaining.peak_allocation_bytes = remaining
        .peak_allocation_bytes
        .checked_sub(orchestration_live)
        .ok_or_else(|| {
            OperationFailure::new(
                PathLookupError::Work(WorkError::BudgetExceeded {
                    counter: "peak_allocation_bytes",
                    observed: orchestration_live,
                    maximum: remaining.peak_allocation_bytes,
                }),
                work,
            )
        })?;
    Ok(remaining)
}

fn merge_batch_work(
    prior: WorkCounters,
    mut nested: WorkCounters,
    orchestration_live: u64,
    budget: WorkBudget,
) -> Result<WorkCounters, PathLookupFailure> {
    let simultaneous_peak = orchestration_live
        .checked_add(nested.peak_allocation_bytes)
        .ok_or_else(|| OperationFailure::new(PathLookupError::Work(WorkError::Overflow), prior))?;
    nested.peak_allocation_bytes = 0;
    let mut merged = prior
        .checked_add(nested)
        .map_err(|error| OperationFailure::new(error.into(), prior))?;
    merged.peak_allocation_bytes = merged.peak_allocation_bytes.max(simultaneous_peak);
    merged
        .verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), merged))?;
    Ok(merged)
}

fn merge_batch_failure(
    prior: WorkCounters,
    mut nested: WorkCounters,
    orchestration_live: u64,
    error: PathLookupError,
) -> PathLookupFailure {
    let Some(simultaneous_peak) = orchestration_live.checked_add(nested.peak_allocation_bytes)
    else {
        return OperationFailure::new(PathLookupError::Work(WorkError::Overflow), prior);
    };
    nested.peak_allocation_bytes = 0;
    let Ok(mut merged) = prior.checked_add(nested) else {
        return OperationFailure::new(PathLookupError::Work(WorkError::Overflow), prior);
    };
    merged.peak_allocation_bytes = merged.peak_allocation_bytes.max(simultaneous_peak);
    OperationFailure::new(error, merged)
}

fn maximum_cache_entries(
    path: &NamespacePath,
    config: VolumeConfig,
) -> Result<usize, PathLookupFailure> {
    maximum_cache_entries_for_components(path.depth(), config)
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

fn add(left: WorkCounters, right: WorkCounters) -> Result<WorkCounters, PathLookupFailure> {
    left.checked_add(right)
        .map_err(|error| OperationFailure::new(error.into(), left))
}

fn remaining(work: WorkCounters, budget: WorkBudget) -> Result<WorkBudget, PathLookupFailure> {
    work.remaining(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/path_access.rs"]
mod tests;
