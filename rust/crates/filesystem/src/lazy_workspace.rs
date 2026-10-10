//! Source-backed sparse workspaces.
//!
//! A lazy workspace keeps unresolved host state outside the authenticated
//! authored checkout. Forking copies only one immutable overlay identifier;
//! paths are observed individually through [`DemandSource`].

use crate::demand::{
    DemandError, DemandSource, SourceCursor, SourceDirectoryEntry, SourceNode, SourceNodeKind,
    SourceReference, SourceVersion,
};
use crate::heap_future::{in_heap, in_heap_admitted, in_heap_measured};
use crate::kernel::{
    FileKind, FileMetadata, FilePayload, FileRecord, LogicalName, MetadataField, NamespacePath,
};
use crate::model::VolumeConfig;
use crate::path::PortablePath;
use crate::record_store::MAXIMUM_CAS_ATTEMPTS;
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, AuthoredMutation, CancellationToken, FileId,
    ForkOptions, Fs, IdempotencyKey, OperationReceipt, TransactionCommit, WorkBudget, WorkCounters,
    Workspace, WorkspaceDirectoryEntry, WorkspaceError, WorkspaceExtentKind, WorkspaceId,
    WorkspaceStat,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use thiserror::Error;

#[cfg(target_arch = "wasm32")]
type LazyFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;
#[cfg(not(target_arch = "wasm32"))]
type LazyFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub(crate) const LAZY_STATE_SCHEMA: u32 = 8;

const LAZY_SNAPSHOT_DOMAIN: &[u8] = b"acyclic-fs-lazy-snapshot-v1\0";

/// Stable logical identity of one constant-size lazy tree snapshot.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct LazySnapshotId([u8; 32]);

impl LazySnapshotId {
    /// Canonical digest bytes.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Constant-size reference to an authored generation plus unresolved source state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LazySnapshotRef {
    /// Stable logical identity over every represented component.
    pub id: LazySnapshotId,
    /// Workspace authenticating authored copy-on-write state.
    pub workspace_id: WorkspaceId,
    /// Immutable authored generation.
    pub authored_generation: crate::GenerationId,
    /// Live source identity and invalidation epoch.
    pub source: SourceReference,
    /// Immutable first-observation/tombstone overlay root.
    pub overlay: LazyOverlayId,
    /// Immutable source-identity shadow root.
    pub shadows: LazyShadowId,
}

impl LazySnapshotRef {
    pub(crate) fn new(
        workspace_id: WorkspaceId,
        authored_generation: crate::GenerationId,
        source: SourceReference,
        overlay: LazyOverlayId,
        shadows: LazyShadowId,
    ) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(LAZY_SNAPSHOT_DOMAIN);
        hasher.update(&workspace_id.into_bytes());
        hasher.update(authored_generation.digest().as_bytes());
        hasher.update(&source.identity);
        hasher.update(&source.epoch.to_le_bytes());
        hasher.update(&overlay.into_bytes());
        hasher.update(&shadows.into_bytes());
        Self {
            id: LazySnapshotId(*hasher.finalize().as_bytes()),
            workspace_id,
            authored_generation,
            source,
            overlay,
            shadows,
        }
    }
}

/// Content address of one immutable observed/tombstone overlay.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct LazyOverlayId([u8; 32]);

impl LazyOverlayId {
    /// Reconstructs a persisted overlay identifier.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Stable raw identifier.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Durable progress of one original lazy fork.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum LazyForkPhase {
    /// The source snapshot was frozen before filesystem creation.
    Prepared,
    /// Original filesystem creation was authenticated before readiness.
    Committed,
}

/// Immutable original fork evidence retained by the companion binding.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LazyForkOrigin {
    /// The original filesystem fork publication operation.
    pub operation_id: crate::OperationId,
    /// The complete first frozen source snapshot, never a refreshed parent.
    pub snapshot: LazySnapshotRef,
    /// Actual immutable source-volume decode limits retained for collection.
    pub config: VolumeConfig,
    /// Prepared metadata cannot be used as ready workspace authority.
    pub phase: LazyForkPhase,
}

fn deserialize_fork_origin<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<LazyForkOrigin>, D::Error> {
    Option::<LazyForkOrigin>::deserialize(deserializer)
}

/// Durable constant-size binding for one sparse workspace.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LazyWorkspaceState {
    /// Serialization schema.
    pub schema_version: u32,
    /// Monotonic CAS revision.
    pub revision: u64,
    /// Workspace whose authored generation supplies the writable overlay.
    pub workspace_id: WorkspaceId,
    /// Direct sparse parent, when this state was produced by a fork.
    pub parent_workspace_id: Option<WorkspaceId>,
    /// Original pre-creation fork freeze, retained across child observations.
    #[serde(deserialize_with = "deserialize_fork_origin")]
    pub fork_origin: Option<LazyForkOrigin>,
    /// Provider identity and invalidation epoch pinned at attach time.
    pub source: SourceReference,
    /// Immutable observations and tombstones visible to this context.
    pub overlay: LazyOverlayId,
    /// Persistent source identity to latest authored record index.
    pub shadows: LazyShadowId,
    /// Prepared authored removal recovered atomically when the workspace reopens.
    pub pending_remove: Option<PendingLazyRemove>,
}

impl LazyWorkspaceState {
    /// Authenticates an original root allocation without rebinding its source.
    ///
    /// Later observation epochs may advance independently of the immutable
    /// allocation; a fork, identity replacement or regressed epoch cannot.
    ///
    /// # Errors
    ///
    /// Rejects an incompatible schema, workspace, parent or retained source.
    pub fn validate_root_binding(
        &self,
        workspace_id: WorkspaceId,
        original_source: SourceReference,
    ) -> Result<(), LazyWorkspaceError> {
        if self.schema_version != LAZY_STATE_SCHEMA
            || self.workspace_id != workspace_id
            || self.parent_workspace_id.is_some()
            || self.fork_origin.is_some()
            || self.source.identity != original_source.identity
            || self.source.epoch < original_source.epoch
        {
            return Err(LazyWorkspaceError::StaleSource);
        }
        Ok(())
    }

    fn require_ready_fork(&self) -> Result<(), LazyWorkspaceError> {
        if self.fork_origin.is_some_and(|origin| origin.phase == LazyForkPhase::Prepared) {
            return Err(LazyWorkspaceError::Concurrent);
        }
        Ok(())
    }
}

/// Content address of one immutable source-identity shadow index node.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct LazyShadowId([u8; 32]);

impl LazyShadowId {
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    pub(crate) const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Stable raw identifier.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Persistent treap mapping a source-derived identity to its latest authored record.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum LazyShadow {
    /// Shared empty index root.
    #[default]
    Empty,
    /// One identity record and immutable child indexes.
    Node {
        /// Source-derived stable identity used as the search key.
        file_id: FileId,
        /// Deterministic treap priority.
        priority: u64,
        /// Canonically encoded immutable file record.
        record: Vec<u8>,
        /// Projected scalar metadata authenticated with the record.
        metadata: Box<crate::WorkspaceMetadata>,
        /// Identities ordered before this one.
        left: LazyShadowId,
        /// Identities ordered after this one.
        right: LazyShadowId,
    },
}

impl LazyShadow {
    fn id(&self) -> Result<LazyShadowId, LazyWorkspaceError> {
        Ok(LazyShadowId(hash_lazy_value(self, None)?))
    }

    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        match self {
            Self::Empty => 0,
            Self::Node { record, .. } => crate::foundation::usize_to_u64(
                record.capacity() + std::mem::size_of::<crate::WorkspaceMetadata>(),
            ),
        }
    }

    pub(crate) fn clone_measured(
        &self,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<Self, LazyWorkspaceError> {
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        let prior_live = allocations.live_bytes();
        let result = (|| {
            match self {
                Self::Empty => Ok(Self::Empty),
                Self::Node { file_id, priority, record, metadata, left, right } => {
                    let record = clone_lazy_bytes(record, allocations, work, budget)?;
                    allocations.claim_elements::<crate::WorkspaceMetadata>(1, work, budget)
                        .map_err(lazy_allocation_error)?;
                    charge_lazy_copy(crate::foundation::usize_to_u64(std::mem::size_of::<crate::WorkspaceMetadata>()), work, budget)?;
                    let metadata = Box::new(**metadata);
                    Ok(Self::Node { file_id: *file_id, priority: *priority, record, metadata, left: *left, right: *right })
                }
            }
        })();
        if result.is_err() {
            allocations.release(allocations.live_bytes() - prior_live).map_err(lazy_allocation_error)?;
        }
        result
    }
}

/// Durable intent that makes authored removal and source tombstoning recoverable.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PendingLazyRemove {
    /// Overlay visible before the removal began.
    pub prior_overlay: LazyOverlayId,
    /// Identity records visible before the removal began.
    pub prior_shadows: LazyShadowId,
    /// Overlay prepared for the same durable publication decision.
    pub prepared_overlay: LazyOverlayId,
    /// Whether recovery must consult a durable authored operation.
    pub kind: PendingLazyRemoveKind,
}

/// Durable decision needed to recover a prepared lazy removal.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum PendingLazyRemoveKind {
    /// The removed binding existed only in the lazy source.
    SourceOnly,
    /// An authored removal is identified by this stable publication key.
    Authored {
        /// Idempotency key persisted before the authored transaction begins.
        idempotency_key: IdempotencyKey,
    },
    /// Mounted removals staged with the checkout and published at its boundary.
    Mounted {
        /// The checkout publication whose result decides this overlay.
        idempotency_key: IdempotencyKey,
    },
}

/// One immutable node in the persistent source-knowledge index.
///
/// The deterministic treap keeps attach and fork O(1), mutations O(log n),
/// and exact lookups O(log n) without rewriting the complete observed set.
#[allow(
    clippy::large_enum_variant,
    reason = "boxing the Node payload would change this public enum's pattern-matching API"
)]
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum LazyOverlay {
    /// Shared empty index root.
    #[default]
    Empty,
    /// One path fact and its immutable child indexes.
    Node {
        /// Portable absolute path used as the search key.
        path: String,
        /// Deterministic heap priority derived from `path`.
        priority: u64,
        /// Source observation or authored source tombstone.
        change: LazyOverlayChange,
        /// Keys ordered before this path.
        left: LazyOverlayId,
        /// Keys ordered after this path.
        right: LazyOverlayId,
    },
}

/// What removing a path records: a deletion of the source version it held,
/// or only hiding a path the view never owned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Removal {
    Deleted,
    Hidden,
}

/// One fact introduced by a lazy overlay delta.
#[allow(
    clippy::large_enum_variant,
    reason = "boxing the Observe payload would change this public enum's pattern-matching API"
)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum LazyOverlayChange {
    /// First exact observation of one demanded path.
    Observe {
        /// Source generation against which this fact was authenticated.
        source: SourceReference,
        /// Immutable source fact.
        node: SourceNode,
    },
    /// Authored removal of a source path or subtree. `removed` is the source
    /// version the removal deleted; `None` only hides a path the view never
    /// owned (one an ancestor gained after the fork), which is no deletion.
    Tombstone {
        /// Source version deleted, or `None` for a hidden path.
        removed: Option<SourceVersion>,
    },
}

impl LazyOverlay {
    fn id(&self) -> Result<LazyOverlayId, LazyWorkspaceError> {
        Ok(LazyOverlayId(hash_lazy_value(self, None)?))
    }

    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        match self {
            Self::Empty => 0,
            Self::Node { path, .. } => crate::foundation::usize_to_u64(path.capacity()),
        }
    }

    pub(crate) fn clone_measured(
        &self,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<Self, LazyWorkspaceError> {
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        let prior_live = allocations.live_bytes();
        let result = (|| {
            match self {
                Self::Empty => Ok(Self::Empty),
                Self::Node { path, priority, change, left, right } => {
                    let path = String::from_utf8(clone_lazy_bytes(path.as_bytes(), allocations, work, budget)?)
                        .expect("copying a String preserves UTF-8");
                    Ok(Self::Node { path, priority: *priority, change: change.clone(), left: *left, right: *right })
                }
            }
        })();
        if result.is_err() {
            allocations.release(allocations.live_bytes() - prior_live).map_err(lazy_allocation_error)?;
        }
        result
    }
}

fn lazy_allocation_error(error: crate::kernel::allocation::AllocationError) -> LazyWorkspaceError {
    match error {
        crate::kernel::allocation::AllocationError::Work(error) => error.into(),
        _ => LazyWorkspaceError::Store("lazy owned allocation failed".to_owned()),
    }
}

fn charge_lazy_copy(bytes: u64, work: &mut WorkCounters, budget: WorkBudget) -> Result<(), LazyWorkspaceError> {
    let next = work.checked_add(WorkCounters { bytes_copied: bytes, ..WorkCounters::default() })?;
    next.verify(budget)?;
    *work = next;
    Ok(())
}

fn charge_lazy_hash(bytes: u64, work: &mut WorkCounters, budget: WorkBudget) -> Result<(), LazyWorkspaceError> {
    let next = work.checked_add(WorkCounters { bytes_hashed: bytes, ..WorkCounters::default() })?;
    next.verify(budget)?;
    *work = next;
    Ok(())
}

fn clone_lazy_bytes(
    bytes: &[u8],
    allocations: &mut crate::kernel::allocation::AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<Vec<u8>, LazyWorkspaceError> {
    allocations.claim_elements::<u8>(bytes.len(), work, budget).map_err(lazy_allocation_error)?;
    let mut copied = Vec::new();
    copied.try_reserve_exact(bytes.len()).map_err(|_| LazyWorkspaceError::Store("lazy byte allocation failed".to_owned()))?;
    let excess = copied.capacity().saturating_sub(bytes.len());
    if excess != 0 {
        allocations.claim_bytes(crate::foundation::usize_to_u64(excess), 0, work, WorkBudget::UNBOUNDED)
            .map_err(lazy_allocation_error)?;
        work.verify(budget)?;
    }
    charge_lazy_copy(crate::foundation::usize_to_u64(bytes.len()), work, budget)?;
    copied.extend_from_slice(bytes);
    Ok(copied)
}

/// Durable companion storage for constant-size lazy bindings and immutable overlays.
///
/// Statically dispatched futures avoid allocating an erased future before
/// a bounded provider operation admits its work.
pub trait LazyWorkspaceStore: Clone + Send + Sync + 'static {
    /// Backend error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Loads one workspace binding.
    fn load_lazy_workspace(
        &self,
        workspace_id: WorkspaceId,
    ) -> impl Future<Output = Result<Option<LazyWorkspaceState>, Self::Error>> + Send;

    /// Loads one binding with actual provider work and owned-allocation bounds.
    fn load_lazy_workspace_measured(
        &self,
        workspace_id: WorkspaceId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<OperationReceipt<Option<LazyWorkspaceState>>, LazyWorkspaceError>> + Send;

    /// Replaces one binding only at the expected revision (`0` means absent).
    fn compare_and_swap_lazy_workspace(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: LazyWorkspaceState,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    /// Replaces one binding with actual provider work and owned-allocation bounds.
    fn compare_and_swap_lazy_workspace_measured(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: LazyWorkspaceState,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<OperationReceipt<bool>, LazyWorkspaceError>> + Send;

    /// Loads an immutable content-addressed overlay.
    fn load_lazy_overlay(
        &self,
        overlay: LazyOverlayId,
    ) -> impl Future<Output = Result<Option<LazyOverlay>, Self::Error>> + Send;

    /// Loads one immutable overlay with actual provider work and owned-allocation bounds.
    fn load_lazy_overlay_measured(
        &self,
        overlay: LazyOverlayId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<OperationReceipt<Option<LazyOverlay>>, LazyWorkspaceError>> + Send;

    /// Stores an immutable overlay, idempotently rejecting digest mismatches.
    fn put_lazy_overlay(
        &self,
        overlay: LazyOverlayId,
        value: LazyOverlay,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Stores one immutable overlay under actual provider work and allocation bounds.
    fn put_lazy_overlay_measured(
        &self,
        overlay: LazyOverlayId,
        value: LazyOverlay,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<OperationReceipt<()>, LazyWorkspaceError>> + Send;

    /// Loads one immutable source-identity shadow node.
    fn load_lazy_shadow(
        &self,
        shadow: LazyShadowId,
    ) -> impl Future<Output = Result<Option<LazyShadow>, Self::Error>> + Send;

    /// Loads one immutable shadow with actual provider work and owned-allocation bounds.
    fn load_lazy_shadow_measured(
        &self,
        shadow: LazyShadowId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<OperationReceipt<Option<LazyShadow>>, LazyWorkspaceError>> + Send;

    /// Stores one immutable source-identity shadow node.
    fn put_lazy_shadow(
        &self,
        shadow: LazyShadowId,
        value: LazyShadow,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Stores one immutable shadow under actual provider work and allocation bounds.
    fn put_lazy_shadow_measured(
        &self,
        shadow: LazyShadowId,
        value: LazyShadow,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<OperationReceipt<()>, LazyWorkspaceError>> + Send;
}

/// In-memory companion store for deterministic tests and embedded use.
#[derive(Clone, Default)]
pub struct MemoryLazyWorkspaceStore {
    inner: Arc<Mutex<MemoryLazyWorkspaceState>>,
}

#[derive(Default)]
struct MemoryLazyWorkspaceState {
    workspaces: Vec<(WorkspaceId, LazyWorkspaceState)>,
    overlays: Vec<(LazyOverlayId, LazyOverlay)>,
    shadows: Vec<(LazyShadowId, LazyShadow)>,
}

fn memory_lazy_work_error(error: impl std::fmt::Display) -> LazyWorkspaceError {
    LazyWorkspaceError::Work(error.to_string())
}

fn memory_lazy_check(cancellation: &CancellationToken) -> Result<(), LazyWorkspaceError> {
    cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)
}

fn memory_lazy_search<K: Ord, V>(
    table: &[(K, V)],
    key: &K,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<Result<usize, usize>, LazyWorkspaceError> {
    let mut lower = 0;
    let mut upper = table.len();
    while lower < upper {
        memory_lazy_check(cancellation)?;
        work.charge_items(1, &budget).map_err(memory_lazy_work_error)?;
        let middle = lower + (upper - lower) / 2;
        let entry = table.get(middle).ok_or(LazyWorkspaceError::Concurrent)?;
        match entry.0.cmp(key) {
            std::cmp::Ordering::Less => lower = middle + 1,
            std::cmp::Ordering::Greater => upper = middle,
            std::cmp::Ordering::Equal => return Ok(Ok(middle)),
        }
    }
    Ok(Err(lower))
}

fn memory_lazy_copy(
    bytes: usize,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<(), LazyWorkspaceError> {
    work.charge(
        &WorkCounters {
            bytes_copied: crate::foundation::usize_to_u64(bytes),
            ..WorkCounters::UNCHARGED
        },
        &budget,
    ).map_err(memory_lazy_work_error)
}

/// Admit all copying and simultaneous old/new capacities before moving values.
/// The table owns the values directly; growing it never clones their heaps.
fn memory_lazy_insert<K, V>(
    table: &mut Vec<(K, V)>,
    index: usize,
    entry: (K, V),
    allocations: &mut crate::kernel::allocation::AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), LazyWorkspaceError> {
    memory_lazy_check(cancellation)?;
    let element_bytes = std::mem::size_of::<(K, V)>();
    let shifted = table.len().checked_sub(index).ok_or(LazyWorkspaceError::Concurrent)?;
    let moved = if table.len() == table.capacity() { table.len() } else { shifted };
    let bytes = moved.checked_add(1).and_then(|count| count.checked_mul(element_bytes))
        .ok_or(LazyWorkspaceError::TooLarge)?;
    memory_lazy_copy(bytes, work, budget)?;
    work.charge_items(crate::foundation::usize_to_u64(moved), &budget)
        .map_err(memory_lazy_work_error)?;
    if table.len() < table.capacity() {
        memory_lazy_check(cancellation)?;
        table.insert(index, entry);
        return Ok(());
    }
    let old_bytes = table.capacity().checked_mul(element_bytes)
        .ok_or(LazyWorkspaceError::TooLarge)?;
    allocations.claim_bytes(crate::foundation::usize_to_u64(old_bytes), 0, work, budget)
        .map_err(memory_lazy_work_error)?;
    let capacity = table.capacity().checked_mul(2).ok_or(LazyWorkspaceError::TooLarge)?
        .max(table.len().checked_add(1).ok_or(LazyWorkspaceError::TooLarge)?);
    allocations.claim_elements::<(K, V)>(capacity, work, budget)
        .map_err(memory_lazy_work_error)?;
    let mut grown = Vec::new();
    grown.try_reserve_exact(capacity).map_err(memory_lazy_work_error)?;
    // reserve_exact may return extra capacity. Reconcile it before semantic mutation.
    let extra = grown.capacity().checked_sub(capacity).ok_or(LazyWorkspaceError::Concurrent)?;
    if extra != 0 {
        let extra_bytes = extra.checked_mul(element_bytes).ok_or(LazyWorkspaceError::TooLarge)?;
        allocations.claim_bytes(
            crate::foundation::usize_to_u64(extra_bytes), 0, work, WorkBudget::UNBOUNDED,
        ).map_err(memory_lazy_work_error)?;
        work.remaining(budget).map_err(memory_lazy_work_error)?;
    }
    memory_lazy_check(cancellation)?;
    let mut entry = Some(entry);
    for (position, existing) in table.drain(..).enumerate() {
        if position == index {
            if let Some(value) = entry.take() {
                grown.push(value);
            }
        }
        grown.push(existing);
    }
    if let Some(value) = entry {
        grown.push(value);
    }
    *table = grown;
    allocations.release(crate::foundation::usize_to_u64(old_bytes))
        .map_err(memory_lazy_work_error)?;
    Ok(())
}

fn memory_lazy_digest<T: Serialize>(
    value: &T,
    expected: [u8; 32],
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), LazyWorkspaceError> {
    let remaining = work.remaining(budget).map_err(memory_lazy_work_error)?;
    let receipt = hash_lazy_value_measured(value, remaining, cancellation)?;
    work.charge(&receipt.work, &budget).map_err(memory_lazy_work_error)?;
    if receipt.value != expected {
        return Err(LazyWorkspaceError::Store("content-addressed lazy node digest mismatch".into()));
    }
    Ok(())
}

impl MemoryLazyWorkspaceStore {
    fn measured<T>(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        operation: impl FnOnce(
            &mut MemoryLazyWorkspaceState,
            &mut crate::kernel::allocation::AllocationLedger,
            &mut WorkCounters,
        ) -> Result<T, LazyWorkspaceError>,
    ) -> Result<OperationReceipt<T>, LazyWorkspaceError> {
        let mut work = WorkCounters::default();
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let result = (|| {
            memory_lazy_check(cancellation)?;
            work.charge_items(1, &budget).map_err(memory_lazy_work_error)?;
            let mut state = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            memory_lazy_check(cancellation)?;
            operation(&mut state, &mut allocations, &mut work)
        })();
        result.map(|value| OperationReceipt { value, work })
            .map_err(|error| error.with_prior_work(work))
    }
}

impl LazyWorkspaceStore for MemoryLazyWorkspaceStore {
    type Error = LazyWorkspaceError;

    async fn load_lazy_workspace(
        &self, workspace_id: WorkspaceId,
    ) -> Result<Option<LazyWorkspaceState>, Self::Error> {
        self.load_lazy_workspace_measured(workspace_id, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await.map(|receipt| receipt.value)
    }

    async fn load_lazy_workspace_measured(
        &self, workspace_id: WorkspaceId, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Option<LazyWorkspaceState>>, LazyWorkspaceError> {
        self.measured(budget, cancellation, |state, _, work| {
            let Ok(index) = memory_lazy_search(&state.workspaces, &workspace_id, work, budget, cancellation)? else {
                return Ok(None);
            };
            let value = &state.workspaces.get(index).ok_or(LazyWorkspaceError::Concurrent)?.1;
            memory_lazy_copy(std::mem::size_of::<LazyWorkspaceState>(), work, budget)?;
            Ok(Some(value.clone()))
        })
    }

    async fn compare_and_swap_lazy_workspace(
        &self, workspace_id: WorkspaceId, expected_revision: u64, replacement: LazyWorkspaceState,
    ) -> Result<bool, Self::Error> {
        self.compare_and_swap_lazy_workspace_measured(
            workspace_id, expected_revision, replacement, WorkBudget::UNBOUNDED, &CancellationToken::new(),
        ).await.map(|receipt| receipt.value)
    }

    async fn compare_and_swap_lazy_workspace_measured(
        &self, workspace_id: WorkspaceId, expected_revision: u64, replacement: LazyWorkspaceState,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<bool>, LazyWorkspaceError> {
        self.measured(budget, cancellation, |state, allocations, work| {
            let position = memory_lazy_search(&state.workspaces, &workspace_id, work, budget, cancellation)?;
            let current = match position {
                Ok(index) => Some(&state.workspaces.get(index).ok_or(LazyWorkspaceError::Concurrent)?.1),
                Err(_) => None,
            };
            if current.map_or(0, |state| state.revision) != expected_revision {
                return Ok(false);
            }
            let prior_origin = current.and_then(|state| state.fork_origin);
            if prior_origin.is_some() || replacement.fork_origin.is_some() {
                work.charge_items(1, &budget).map_err(memory_lazy_work_error)?;
                let Some(origin) = replacement.fork_origin else { return Ok(false); };
                if replacement.schema_version != LAZY_STATE_SCHEMA
                    || replacement.workspace_id != workspace_id
                    || replacement.parent_workspace_id != Some(origin.snapshot.workspace_id)
                    || origin.snapshot.workspace_id == workspace_id
                {
                    return Ok(false);
                }
                if let Some(prior) = prior_origin {
                    let current = current.expect("retained origin has a binding");
                    if current.schema_version != replacement.schema_version
                        || current.workspace_id != workspace_id
                        || current.parent_workspace_id != replacement.parent_workspace_id
                        || prior.operation_id != origin.operation_id
                        || prior.config != origin.config
                        || prior.snapshot != origin.snapshot
                        || (prior.phase == LazyForkPhase::Committed
                            && origin.phase != LazyForkPhase::Committed)
                    {
                        return Ok(false);
                    }
                } else if current.is_some() || expected_revision != 0 || replacement.revision != 1
                    || origin.phase != LazyForkPhase::Prepared
                {
                    return Ok(false);
                }
                if prior_origin.is_none_or(|prior| prior.phase == LazyForkPhase::Prepared)
                    && (replacement.source != origin.snapshot.source
                        || replacement.overlay != origin.snapshot.overlay
                        || replacement.shadows != origin.snapshot.shadows
                        || replacement.pending_remove.is_some())
                {
                    return Ok(false);
                }
                if prior_origin.is_none() {
                    // The mutex also excludes source updates until insertion. Retries
                    // and later child epochs never authenticate a mutable parent again.
                    let Ok(parent_index) = memory_lazy_search(
                        &state.workspaces, &origin.snapshot.workspace_id, work, budget, cancellation,
                    )? else { return Ok(false); };
                    let parent = &state.workspaces.get(parent_index).ok_or(LazyWorkspaceError::Concurrent)?.1;
                    if parent.schema_version != replacement.schema_version
                        || parent.workspace_id != origin.snapshot.workspace_id
                        || parent.source != origin.snapshot.source
                        || parent.overlay != origin.snapshot.overlay
                        || parent.shadows != origin.snapshot.shadows
                        || parent.fork_origin.is_some_and(|origin| origin.phase == LazyForkPhase::Prepared)
                    {
                        return Ok(false);
                    }
                }
            }
            match position {
                Ok(index) => {
                    memory_lazy_copy(std::mem::size_of::<LazyWorkspaceState>(), work, budget)?;
                    memory_lazy_check(cancellation)?;
                    state.workspaces.get_mut(index).ok_or(LazyWorkspaceError::Concurrent)?.1 = replacement;
                }
                Err(index) => memory_lazy_insert(
                    &mut state.workspaces, index, (workspace_id, replacement), allocations, work, budget, cancellation,
                )?,
            }
            Ok(true)
        })
    }

    async fn load_lazy_overlay(&self, overlay: LazyOverlayId) -> Result<Option<LazyOverlay>, Self::Error> {
        self.load_lazy_overlay_measured(overlay, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await.map(|receipt| receipt.value)
    }

    async fn load_lazy_overlay_measured(
        &self, overlay: LazyOverlayId, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Option<LazyOverlay>>, LazyWorkspaceError> {
        self.measured(budget, cancellation, |state, allocations, work| {
            let Ok(index) = memory_lazy_search(&state.overlays, &overlay, work, budget, cancellation)? else {
                return Ok(None);
            };
            let value = &state.overlays.get(index).ok_or(LazyWorkspaceError::Concurrent)?.1;
            memory_lazy_digest(value, overlay.0, work, budget, cancellation)?;
            Ok(Some(value.clone_measured(allocations, work, budget, cancellation)?))
        })
    }

    async fn put_lazy_overlay(&self, overlay: LazyOverlayId, value: LazyOverlay) -> Result<(), Self::Error> {
        self.put_lazy_overlay_measured(overlay, value, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await.map(|receipt| receipt.value)
    }

    async fn put_lazy_overlay_measured(
        &self, overlay: LazyOverlayId, value: LazyOverlay, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<()>, LazyWorkspaceError> {
        self.measured(budget, cancellation, |state, allocations, work| {
            allocations.claim_bytes(value.logical_owned_bytes(), 0, work, budget).map_err(memory_lazy_work_error)?;
            memory_lazy_digest(&value, overlay.0, work, budget, cancellation)?;
            match memory_lazy_search(&state.overlays, &overlay, work, budget, cancellation)? {
                Ok(index) => {
                    let existing = &state.overlays.get(index).ok_or(LazyWorkspaceError::Concurrent)?.1;
                    let examined = existing.logical_owned_bytes().checked_add(value.logical_owned_bytes())
                        .and_then(|bytes| bytes.checked_add(crate::foundation::usize_to_u64(std::mem::size_of::<LazyOverlay>())))
                        .ok_or(LazyWorkspaceError::TooLarge)?;
                    work.charge_items(examined, &budget).map_err(memory_lazy_work_error)?;
                    memory_lazy_check(cancellation)?;
                    if existing != &value {
                        return Err(LazyWorkspaceError::Store("content-addressed overlay collision".into()));
                    }
                }
                Err(index) => memory_lazy_insert(
                    &mut state.overlays, index, (overlay, value), allocations, work, budget, cancellation,
                )?,
            }
            Ok(())
        })
    }

    async fn load_lazy_shadow(&self, shadow: LazyShadowId) -> Result<Option<LazyShadow>, Self::Error> {
        self.load_lazy_shadow_measured(shadow, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await.map(|receipt| receipt.value)
    }

    async fn load_lazy_shadow_measured(
        &self, shadow: LazyShadowId, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Option<LazyShadow>>, LazyWorkspaceError> {
        self.measured(budget, cancellation, |state, allocations, work| {
            let Ok(index) = memory_lazy_search(&state.shadows, &shadow, work, budget, cancellation)? else {
                return Ok(None);
            };
            let value = &state.shadows.get(index).ok_or(LazyWorkspaceError::Concurrent)?.1;
            memory_lazy_digest(value, shadow.0, work, budget, cancellation)?;
            Ok(Some(value.clone_measured(allocations, work, budget, cancellation)?))
        })
    }

    async fn put_lazy_shadow(&self, shadow: LazyShadowId, value: LazyShadow) -> Result<(), Self::Error> {
        self.put_lazy_shadow_measured(shadow, value, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await.map(|receipt| receipt.value)
    }

    async fn put_lazy_shadow_measured(
        &self, shadow: LazyShadowId, value: LazyShadow, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<()>, LazyWorkspaceError> {
        self.measured(budget, cancellation, |state, allocations, work| {
            allocations.claim_bytes(value.logical_owned_bytes(), 0, work, budget).map_err(memory_lazy_work_error)?;
            memory_lazy_digest(&value, shadow.0, work, budget, cancellation)?;
            match memory_lazy_search(&state.shadows, &shadow, work, budget, cancellation)? {
                Ok(index) => {
                    let existing = &state.shadows.get(index).ok_or(LazyWorkspaceError::Concurrent)?.1;
                    let examined = existing.logical_owned_bytes().checked_add(value.logical_owned_bytes())
                        .and_then(|bytes| bytes.checked_add(crate::foundation::usize_to_u64(std::mem::size_of::<LazyShadow>())))
                        .ok_or(LazyWorkspaceError::TooLarge)?;
                    work.charge_items(examined, &budget).map_err(memory_lazy_work_error)?;
                    memory_lazy_check(cancellation)?;
                    if existing != &value {
                        return Err(LazyWorkspaceError::Store("content-addressed shadow collision".into()));
                    }
                }
                Err(index) => memory_lazy_insert(
                    &mut state.shadows, index, (shadow, value), allocations, work, budget, cancellation,
                )?,
            }
            Ok(())
        })
    }
}

struct ImmutableTreapNode<K, V, I> {
    key: K,
    priority: u64,
    value: V,
    left: I,
    right: I,
}

trait ImmutableTreap: Sync {
    type Id: Copy + Send + Sync;
    type Key: Ord + Send + Sync;
    type Value: Send + Sync;

    fn load(
        &self,
        id: Self::Id,
    ) -> impl Future<Output = Result<Option<ImmutableTreapNode<Self::Key, Self::Value, Self::Id>>, LazyWorkspaceError>> + Send;

    fn store(
        &self,
        node: ImmutableTreapNode<Self::Key, Self::Value, Self::Id>,
    ) -> impl Future<Output = Result<Self::Id, LazyWorkspaceError>> + Send;

    fn priority(key: &Self::Key) -> u64;
    fn priority_bytes(key: &Self::Key) -> u64;
    fn owned_bytes(node: &ImmutableTreapNode<Self::Key, Self::Value, Self::Id>) -> u64;
    fn value_owned_bytes(value: &Self::Value) -> u64;
    fn work(&self) -> Option<&LazyTreapWork<'_>>;
}

async fn insert_immutable_treap<T: ImmutableTreap>(
    treap: &T,
    mut root: T::Id,
    key: T::Key,
    value: T::Value,
) -> Result<T::Id, LazyWorkspaceError> {
    // The canonical rotations are unchanged; an admitted spine replaces one
    // heap-allocated recursive future for every ancestor.
    let _scope = treap.work().map(TreapAllocationScope::new);
    let mut pending = ImmutableTreapNode {
        priority: 0, key, value, left: root, right: root,
    };
    if let Some(work) = treap.work() {
        work.adopt(T::owned_bytes(&pending))?;
    }
    let mut spine = Vec::new();
    let mut inserted = loop {
        let Some(current) = treap.load(root).await? else {
            if let Some(work) = treap.work() {
                work.charge(WorkCounters { bytes_hashed: T::priority_bytes(&pending.key), ..WorkCounters::default() })?;
            }
            pending.priority = T::priority(&pending.key);
            pending.left = root;
            pending.right = root;
            break treap.store(pending).await?;
        };
        if let Some(work) = treap.work() {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
        }
        let ordering = pending.key.cmp(&current.key);
        if ordering == std::cmp::Ordering::Equal {
            pending.left = current.left;
            pending.right = current.right;
            pending.priority = current.priority;
            if let Some(work) = treap.work() {
                work.release(T::owned_bytes(&current))?;
            }
            drop(current);
            break treap.store(pending).await?;
        }
        let descend_left = ordering == std::cmp::Ordering::Less;
        root = if descend_left { current.left } else { current.right };
        if let Some(work) = treap.work() {
            work.ensure_vec_push(&mut spine)?;
        }
        spine.push((current, descend_left));
    };
    while let Some((current, descend_left)) = spine.pop() {
        let child = treap.load(inserted).await?.ok_or_else(|| {
            LazyWorkspaceError::Store("inserted lazy index node is absent".to_owned())
        })?;
        if child.priority > current.priority {
            let rotated = treap.store(if descend_left {
                ImmutableTreapNode { left: child.right, ..current }
            } else {
                ImmutableTreapNode { right: child.left, ..current }
            }).await?;
            inserted = treap.store(if descend_left {
                ImmutableTreapNode { right: rotated, ..child }
            } else {
                ImmutableTreapNode { left: rotated, ..child }
            }).await?;
        } else {
            if let Some(work) = treap.work() {
                work.release(T::owned_bytes(&child))?;
            }
            drop(child);
            inserted = treap.store(if descend_left {
                ImmutableTreapNode { left: inserted, ..current }
            } else {
                ImmutableTreapNode { right: inserted, ..current }
            }).await?;
        }
    }
    Ok(inserted)
}

async fn immutable_treap_value<T: ImmutableTreap>(
    treap: &T,
    mut root: T::Id,
    key: &T::Key,
) -> Result<Option<T::Value>, LazyWorkspaceError> {
    loop {
        let Some(node) = treap.load(root).await? else {
            return Ok(None);
        };
        match key.cmp(&node.key) {
            std::cmp::Ordering::Equal => {
                if let Some(work) = treap.work() {
                    work.release(T::owned_bytes(&node) - T::value_owned_bytes(&node.value))?;
                }
                return Ok(Some(node.value));
            }
            std::cmp::Ordering::Less => root = node.left,
            std::cmp::Ordering::Greater => root = node.right,
        }
        if let Some(work) = treap.work() {
            work.release(T::owned_bytes(&node))?;
        }
    }
}

/// Invocation-local accounting for the existing immutable treap algorithm.
/// This is neither authority nor durable state: the companion store and its
/// existing operation decision remain the recovery source.
struct LazyTreapWork<'a> {
    work: Mutex<WorkCounters>,
    allocations: Mutex<crate::kernel::allocation::AllocationLedger>,
    budget: WorkBudget,
    cancellation: &'a CancellationToken,
}

impl<'a> LazyTreapWork<'a> {
    fn new(budget: WorkBudget, cancellation: &'a CancellationToken) -> Self {
        Self {
            work: Mutex::new(WorkCounters::default()),
            allocations: Mutex::new(crate::kernel::allocation::AllocationLedger::default()),
            budget, cancellation,
        }
    }

    fn spent(&self) -> WorkCounters {
        *self.work.lock().expect("lazy treap work lock poisoned")
    }

    fn live_bytes(&self) -> u64 {
        self.allocations.lock().expect("lazy treap allocation lock poisoned").live_bytes()
    }

    fn remaining(&self) -> Result<WorkBudget, LazyWorkspaceError> {
        let mut prior = self.spent();
        prior.peak_allocation_bytes = 0;
        let mut budget = remaining_work(prior, self.budget)?;
        budget.peak_allocation_bytes = self.budget.peak_allocation_bytes
            .checked_sub(self.live_bytes()).ok_or_else(|| LazyWorkspaceError::Work("lazy retained allocation exceeds budget".to_owned()))?;
        Ok(budget)
    }

    fn accept<T>(&self, result: Result<OperationReceipt<T>, LazyWorkspaceError>) -> Result<T, LazyWorkspaceError> {
        let (result, additional) = match result {
            Ok(receipt) => (Ok(receipt.value), receipt.work),
            Err(error) => {
                let failure = error.into_measured_failure();
                (Err(failure.error), *failure.work)
            }
        };
        let live = self.live_bytes();
        let mut work = self.work.lock().expect("lazy treap work lock poisoned");
        *work = work.with_backend(additional, live)?;
        work.verify(self.budget)?;
        result
    }

    fn charge(&self, additional: WorkCounters) -> Result<(), LazyWorkspaceError> {
        self.cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        let mut work = self.work.lock().expect("lazy treap work lock poisoned");
        let next = work.checked_add(additional)?;
        next.verify(self.budget)?;
        *work = next;
        Ok(())
    }

    fn adopt(&self, bytes: u64) -> Result<(), LazyWorkspaceError> {
        let mut allocations = self.allocations.lock().expect("lazy treap allocation lock poisoned");
        let mut work = self.work.lock().expect("lazy treap work lock poisoned");
        allocations.claim_bytes(bytes, 0, &mut work, self.budget).map_err(lazy_allocation_error)
    }

    fn allocate(&self, bytes: u64) -> Result<(), LazyWorkspaceError> {
        self.cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        let mut allocations = self.allocations.lock().expect("lazy treap allocation lock poisoned");
        let mut work = self.work.lock().expect("lazy treap work lock poisoned");
        allocations.claim_bytes(bytes, 1, &mut work, self.budget).map_err(lazy_allocation_error)
    }

    fn canonical_path_owned(
        &self,
        path: &str,
        limits: crate::VolumeLimits,
    ) -> Result<String, LazyWorkspaceError> {
        self.cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        PortablePath::validate(path, limits)
            .map_err(|error| LazyWorkspaceError::Workspace(error.to_string()))?;
        let mut allocations = self.allocations.lock().expect("lazy treap allocation lock poisoned");
        let mut work = self.work.lock().expect("lazy treap work lock poisoned");
        let bytes = clone_lazy_bytes(path.as_bytes(), &mut allocations, &mut work, self.budget)?;
        String::from_utf8(bytes).map_err(|error| LazyWorkspaceError::Workspace(error.to_string()))
    }

    fn release(&self, bytes: u64) -> Result<(), LazyWorkspaceError> {
        self.allocations.lock().expect("lazy treap allocation lock poisoned")
            .release(bytes).map_err(lazy_allocation_error)
    }

    fn ensure_vec_push<T>(&self, values: &mut Vec<T>) -> Result<(), LazyWorkspaceError> {
        if values.len() < values.capacity() {
            return Ok(());
        }
        self.cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        let old_capacity = values.capacity();
        let next_capacity = old_capacity.checked_mul(2).map(|value| value.max(1))
            .ok_or(crate::WorkError::Overflow)?;
        let mut allocations = self.allocations.lock().expect("lazy treap allocation lock poisoned");
        let mut work = self.work.lock().expect("lazy treap work lock poisoned");
        allocations.claim_elements::<T>(next_capacity, &mut work, self.budget).map_err(lazy_allocation_error)?;
        let mut replacement = Vec::new();
        replacement.try_reserve_exact(next_capacity)
            .map_err(|_| LazyWorkspaceError::Store("lazy treap spine allocation failed".to_owned()))?;
        let excess = replacement.capacity().checked_sub(next_capacity).ok_or(crate::WorkError::Overflow)?
            .checked_mul(std::mem::size_of::<T>()).ok_or(crate::WorkError::Overflow)?;
        if excess != 0 {
            allocations.claim_bytes(crate::foundation::usize_to_u64(excess), 0, &mut work, WorkBudget::UNBOUNDED)
                .map_err(lazy_allocation_error)?;
            work.verify(self.budget)?;
        }
        charge_lazy_copy(crate::foundation::usize_to_u64(values.len() * std::mem::size_of::<T>()), &mut work, self.budget)?;
        replacement.append(values);
        *values = replacement;
        allocations.release(crate::foundation::usize_to_u64(old_capacity * std::mem::size_of::<T>()))
            .map_err(lazy_allocation_error)?;
        Ok(())
    }
}

struct TreapAllocationScope<'a, 'b> {
    work: &'a LazyTreapWork<'b>,
    prior_live: u64,
}

impl<'a, 'b> TreapAllocationScope<'a, 'b> {
    fn new(work: &'a LazyTreapWork<'b>) -> Self {
        Self { work, prior_live: work.live_bytes() }
    }
}

impl Drop for TreapAllocationScope<'_, '_> {
    fn drop(&mut self) {
        self.work.release(self.work.live_bytes().checked_sub(self.prior_live)
            .expect("treap only releases allocations it adopted"))
            .expect("treap allocation scope releases its remaining claims");
    }
}

struct LazyHashWriter<'a> {
    hasher: blake3::Hasher,
    work: Option<&'a LazyTreapWork<'a>>,
    failure: Option<LazyWorkspaceError>,
}

impl std::io::Write for LazyHashWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if let Some(work) = self.work {
            let bytes = crate::foundation::usize_to_u64(bytes.len());
            if let Err(error) = work.charge(WorkCounters {
                bytes_encoded: bytes, bytes_hashed: bytes, ..WorkCounters::default()
            }) {
                self.failure = Some(error);
                return Err(std::io::ErrorKind::Other.into());
            }
        }
        self.hasher.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

fn hash_lazy_value<T: Serialize>(value: &T, work: Option<&LazyTreapWork<'_>>) -> Result<[u8; 32], LazyWorkspaceError> {
    let mut writer = LazyHashWriter { hasher: blake3::Hasher::new(), work, failure: None };
    let result = serde_json::to_writer(&mut writer, value);
    if let Some(error) = writer.failure {
        return Err(error);
    }
    result.map_err(|error| LazyWorkspaceError::Store(error.to_string()))?;
    Ok(*writer.hasher.finalize().as_bytes())
}

pub(crate) fn hash_lazy_value_measured<T: Serialize>(
    value: &T,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<[u8; 32]>, LazyWorkspaceError> {
    let work = LazyTreapWork::new(budget, cancellation);
    let result = hash_lazy_value(value, Some(&work));
    let spent = work.spent();
    result.map(|value| OperationReceipt { value, work: spent }).map_err(|error| error.with_prior_work(spent))
}

struct OverlayTreap<'a, S>(&'a S, Option<&'a LazyTreapWork<'a>>);

impl<S: LazyWorkspaceStore> ImmutableTreap for OverlayTreap<'_, S> {
    type Id = LazyOverlayId;
    type Key = String;
    type Value = LazyOverlayChange;

    async fn load(
        &self,
        id: Self::Id,
    ) -> Result<Option<ImmutableTreapNode<Self::Key, Self::Value, Self::Id>>, LazyWorkspaceError>
    {
        let value = if let Some(work) = self.1 {
            work.accept(self.0.load_lazy_overlay_measured(
                id, work.remaining()?, work.cancellation,
            ).await)?
        } else {
            self.0.load_lazy_overlay(id).await.map_err(store_error)?
        };
        if let (Some(work), Some(value)) = (self.1, value.as_ref()) {
            work.adopt(value.logical_owned_bytes())?;
        }
        match value {
            Some(LazyOverlay::Empty) => Ok(None),
            Some(LazyOverlay::Node {
                path,
                priority,
                change,
                left,
                right,
            }) => Ok(Some(ImmutableTreapNode {
                key: path,
                priority,
                value: change,
                left,
                right,
            })),
            None => Err(LazyWorkspaceError::Store(
                "lazy overlay is absent".to_owned(),
            )),
        }
    }

    async fn store(
        &self,
        node: ImmutableTreapNode<Self::Key, Self::Value, Self::Id>,
    ) -> Result<Self::Id, LazyWorkspaceError> {
        let overlay = LazyOverlay::Node {
            path: node.key,
            priority: node.priority,
            change: node.value,
            left: node.left,
            right: node.right,
        };
        let owned_bytes = overlay.logical_owned_bytes();
        let mut transferred = false;
        let result = async {
            let id = LazyOverlayId(hash_lazy_value(&overlay, self.1)?);
            if let Some(work) = self.1 {
                work.release(owned_bytes)?;
                transferred = true;
                work.accept(self.0.put_lazy_overlay_measured(
                    id, overlay, work.remaining()?, work.cancellation,
                ).await)?;
            } else {
                self.0.put_lazy_overlay(id, overlay).await.map_err(store_error)?;
            }
            Ok(id)
        }.await;
        if !transferred && let Some(work) = self.1 {
            work.release(owned_bytes)?;
        }
        result
    }

    fn priority(key: &Self::Key) -> u64 {
        path_priority(key)
    }

    fn priority_bytes(key: &Self::Key) -> u64 { crate::foundation::usize_to_u64(key.len()) }
    fn owned_bytes(node: &ImmutableTreapNode<Self::Key, Self::Value, Self::Id>) -> u64 {
        crate::foundation::usize_to_u64(node.key.capacity())
    }
    fn value_owned_bytes(_: &Self::Value) -> u64 { 0 }
    fn work(&self) -> Option<&LazyTreapWork<'_>> { self.1 }
}

#[derive(Clone)]
struct ShadowValue {
    record: Vec<u8>,
    metadata: Box<crate::WorkspaceMetadata>,
}

struct ShadowTreap<'a, S>(&'a S, Option<&'a LazyTreapWork<'a>>);

impl<S: LazyWorkspaceStore> ImmutableTreap for ShadowTreap<'_, S> {
    type Id = LazyShadowId;
    type Key = FileId;
    type Value = ShadowValue;

    async fn load(
        &self,
        id: Self::Id,
    ) -> Result<Option<ImmutableTreapNode<Self::Key, Self::Value, Self::Id>>, LazyWorkspaceError>
    {
        let value = if let Some(work) = self.1 {
            work.accept(self.0.load_lazy_shadow_measured(
                id, work.remaining()?, work.cancellation,
            ).await)?
        } else {
            self.0.load_lazy_shadow(id).await.map_err(store_error)?
        };
        if let (Some(work), Some(value)) = (self.1, value.as_ref()) {
            work.adopt(value.logical_owned_bytes())?;
        }
        match value {
            Some(LazyShadow::Empty) => Ok(None),
            Some(LazyShadow::Node {
                file_id,
                priority,
                record,
                metadata,
                left,
                right,
            }) => Ok(Some(ImmutableTreapNode {
                key: file_id,
                priority,
                value: ShadowValue { record, metadata },
                left,
                right,
            })),
            None => Err(LazyWorkspaceError::Store(
                "lazy identity shadow is absent".to_owned(),
            )),
        }
    }

    async fn store(
        &self,
        node: ImmutableTreapNode<Self::Key, Self::Value, Self::Id>,
    ) -> Result<Self::Id, LazyWorkspaceError> {
        let shadow = LazyShadow::Node {
            file_id: node.key,
            priority: node.priority,
            record: node.value.record,
            metadata: node.value.metadata,
            left: node.left,
            right: node.right,
        };
        let owned_bytes = shadow.logical_owned_bytes();
        let mut transferred = false;
        let result = async {
            let id = LazyShadowId(hash_lazy_value(&shadow, self.1)?);
            if let Some(work) = self.1 {
                work.release(owned_bytes)?;
                transferred = true;
                work.accept(self.0.put_lazy_shadow_measured(
                    id, shadow, work.remaining()?, work.cancellation,
                ).await)?;
            } else {
                self.0.put_lazy_shadow(id, shadow).await.map_err(store_error)?;
            }
            Ok(id)
        }.await;
        if !transferred && let Some(work) = self.1 {
            work.release(owned_bytes)?;
        }
        result
    }

    fn priority(key: &Self::Key) -> u64 {
        shadow_priority(*key)
    }

    fn priority_bytes(_: &Self::Key) -> u64 { crate::foundation::usize_to_u64(std::mem::size_of::<FileId>()) }
    fn owned_bytes(node: &ImmutableTreapNode<Self::Key, Self::Value, Self::Id>) -> u64 {
        Self::value_owned_bytes(&node.value)
    }
    fn value_owned_bytes(value: &Self::Value) -> u64 {
        crate::foundation::usize_to_u64(value.record.capacity() + std::mem::size_of::<crate::WorkspaceMetadata>())
    }
    fn work(&self) -> Option<&LazyTreapWork<'_>> { self.1 }
}

/// Facts an inspection's caller already established, so it need not repeat them.
#[derive(Clone, Copy, Default)]
struct InspectBasis<'a> {
    /// State the caller read; otherwise the current state is loaded.
    state: Option<&'a LazyWorkspaceState>,
    /// Exact authored generation supplied by a frozen readonly caller.
    authored_generation: Option<crate::GenerationId>,
    /// The caller's authored view already lacks the path.
    authored_absent: bool,
    /// Node a listing observed for the path after excluding tombstones.
    listed: Option<SourceNode>,
    /// Keep an observation pinned to an earlier source epoch.
    retain_pinned_observation: bool,
    /// The overlay records no fact for the path, as its listing proved.
    unobserved: bool,
}

/// A source-backed workspace whose unresolved paths remain outside its authored generation.
pub struct LazyWorkspace<A, O, D, S> {
    workspace: Workspace<A, O>,
    source: Arc<D>,
    store: S,
}

impl<A, O, D, S> Clone for LazyWorkspace<A, O, D, S>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        Self {
            workspace: self.workspace.clone(),
            source: Arc::clone(&self.source),
            store: self.store.clone(),
        }
    }
}

/// Path facts returned without forcing unrelated source state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LazyStat {
    /// Observed kind.
    pub kind: SourceNodeKind,
    /// Observed logical bytes when meaningful.
    pub logical_bytes: Option<u64>,
    /// Whether the authored checkout overrides the source.
    pub authored: bool,
}

/// Complete facts returned from either authored state or a source observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LazyLookup {
    /// Authenticated authored state overrides the source.
    Authored {
        /// Existing authored binding used to serve this path. This may differ
        /// from the requested source alias until that alias is promoted.
        path: String,
        /// Authenticated authored facts for the shared identity.
        stat: WorkspaceStat,
    },
    /// Latest authored state retained after the final promoted binding was removed.
    Shadow {
        /// Path-independent immutable file record.
        record: FileRecord,
        /// Projected scalar metadata retained with the record.
        metadata: crate::WorkspaceMetadata,
        /// Source-native alias count evidence.
        source_link_count: u64,
    },
    /// Immutable first-observation source state.
    Source(SourceNode),
}

impl LazyLookup {
    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        match self {
            Self::Authored { path, .. } => path.capacity() as u64,
            Self::Shadow { .. } | Self::Source(_) => 0,
        }
    }

    /// Projects the canonical kind and logical size without representation identities.
    #[must_use]
    pub fn stat(&self) -> LazyStat {
        match self {
            Self::Authored { stat, .. } => LazyStat {
                kind: source_kind(stat.kind), logical_bytes: stat.logical_bytes, authored: true,
            },
            Self::Shadow { record, .. } => LazyStat {
                kind: source_kind(record.kind), logical_bytes: record_logical_bytes(*record), authored: true,
            },
            Self::Source(node) => LazyStat {
                kind: node.kind, logical_bytes: node.logical_bytes, authored: false,
            },
        }
    }

    /// Projects the same scalar metadata authored promotion preserves.
    ///
    /// Storage representation and source versions are not modification facts.
    #[must_use]
    pub fn metadata(&self) -> crate::WorkspaceMetadata {
        match self {
            Self::Authored { stat, .. } => stat.metadata,
            Self::Shadow { metadata, .. } => *metadata,
            Self::Source(node) => crate::WorkspaceMetadata::from_engine(source_file_metadata(node.metadata)),
        }
    }
}

struct ResolvedLazyLookup {
    lookup: LazyLookup,
    source: Option<SourceReference>,
}

struct PinnedSourceNode {
    source: SourceReference,
    node: SourceNode,
}

/// Streams one version-pinned demanded file into the SDK blob builder while
/// preserving the demand provider's exact work receipt.
struct DemandBlobSource<'a, D> {
    provider: &'a D,
    source: SourceReference,
    path: NamespacePath,
    version: SourceVersion,
    offset: u64,
    length: u64,
    work: Option<WorkCounters>,
    failure: Option<DemandError>,
}

impl<D: DemandSource> crate::kernel::AsyncBlobSource for DemandBlobSource<'_, D> {
    async fn read<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        budget: WorkBudget,
        cancellation: &'a CancellationToken,
    ) -> std::io::Result<usize> {
        let remaining = self.length.saturating_sub(self.offset);
        if remaining == 0 {
            self.work = Some(WorkCounters::default());
            return Ok(0);
        }
        let requested = remaining.min(u64::try_from(destination.len()).unwrap_or(u64::MAX));
        let receipt = self
            .provider
            .read_range(
                self.source,
                &self.path,
                self.version,
                self.offset,
                requested,
                budget,
                cancellation,
            )
            .await;
        let receipt = match receipt {
            Ok(receipt) => receipt,
            Err(failure) => {
                self.work = Some(*failure.work);
                self.failure = Some(failure.error);
                return Err(std::io::Error::other("version-pinned demand read failed"));
            }
        };
        self.work = Some(receipt.work);
        let chunk = receipt.value;
        if chunk.is_empty() || u64::try_from(chunk.len()).unwrap_or(u64::MAX) > requested {
            self.failure = Some(DemandError::StaleVersion);
            return Err(std::io::Error::other(
                "version-pinned demand read was incomplete",
            ));
        }
        self.offset = self
            .offset
            .checked_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| std::io::Error::other("demand source offset overflow"))?;
        destination
            .get_mut(..chunk.len())
            .ok_or_else(|| std::io::Error::other("demand source exceeded read bound"))?
            .copy_from_slice(&chunk);
        Ok(chunk.len())
    }

    fn take_work(&mut self) -> Option<WorkCounters> {
        self.work.take()
    }
}

/// Sparse seek target for a lazily sourced regular file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LazySeekTarget {
    /// First represented data byte.
    Data,
    /// First hole byte, including the logical end of a dense source file.
    Hole,
}

/// One merged directory entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LazyDirectoryEntry {
    /// Exact profile-aware child name.
    pub name: LogicalName,
    /// Observed or authored kind.
    pub kind: SourceNodeKind,
    /// Whether the authored checkout overrides the source.
    pub authored: bool,
    /// The source node observed while listing, for entries the source supplies.
    pub source: Option<SourceNode>,
    /// Whether the overlay records no fact for the entry, which the listing
    /// proved of its whole directory: the listed node is then its node.
    pub unobserved: bool,
}

/// Opaque bounded continuation for a merged source/authored directory scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LazyDirectoryCursor {
    source: SourceReference,
    overlay: LazyOverlayId,
    directory: NamespacePath,
    phase: LazyDirectoryPhase,
    snapshot: Option<LazySnapshotId>,
}

impl LazyDirectoryCursor {
    /// Encodes this original continuation into one bounded canonical owned buffer.
    pub fn encode_measured(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<Vec<u8>>, LazyWorkspaceError> {
        const DOMAIN: &[u8] = b"acyclic-fs-lazy-directory-cursor-v1\0";
        let mut work = WorkCounters::UNCHARGED;
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let result = (|| {
            cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
            let passes = if matches!(&self.phase, LazyDirectoryPhase::Source(Some(_))) { 2 } else { 1 };
            work.charge_items(crate::foundation::usize_to_u64(self.directory.depth()) * passes, &budget)?;
            let directory_bytes = self.directory.cursor_encoded_len().map_err(directory_namespace_error)?;
            let continuation_bytes = match &self.phase {
                LazyDirectoryPhase::Source(Some(cursor)) => cursor.cursor_encoded_len().map_err(LazyWorkspaceError::from)?,
                LazyDirectoryPhase::Authored(Some(name)) => name.cursor_encoded_len().map_err(directory_namespace_error)?,
                LazyDirectoryPhase::Source(None) | LazyDirectoryPhase::Authored(None) => 0,
            };
            let size = DOMAIN.len().checked_add(2 + 24 + 32 + 1 + 1)
                .and_then(|size| size.checked_add(if self.snapshot.is_some() { 32 } else { 0 }))
                .and_then(|size| size.checked_add(directory_bytes))
                .and_then(|size| size.checked_add(continuation_bytes)).ok_or_else(counter_overflow)?;
            allocations.claim_elements::<u8>(size, &mut work, budget).map_err(lazy_allocation_error)?;
            work.charge(&WorkCounters {
                bytes_encoded: crate::foundation::usize_to_u64(DOMAIN.len() + 2 + 24 + 32 + 1 + 1
                    + if self.snapshot.is_some() { 32 } else { 0 }),
                ..WorkCounters::UNCHARGED
            }, &budget)?;
            let mut encoder = crate::kernel::Encoder::with_exact_capacity(DOMAIN, 1, size)
                .map_err(|_| LazyWorkspaceError::StaleCursor)?;
            encoder.fixed(&self.source.identity);
            encoder.u64(self.source.epoch);
            encoder.fixed(&self.overlay.into_bytes());
            match self.snapshot {
                None => encoder.u8(0),
                Some(snapshot) => { encoder.u8(1); encoder.fixed(&snapshot.into_bytes()); }
            }
            self.directory.encode_cursor_into(&mut encoder, &mut work, budget, cancellation)
                .map_err(directory_namespace_error)?;
            match &self.phase {
                LazyDirectoryPhase::Source(None) => encoder.u8(0),
                LazyDirectoryPhase::Source(Some(cursor)) => {
                    encoder.u8(1);
                    cursor.encode_cursor_into(&mut encoder, &mut work, budget, cancellation)
                        .map_err(LazyWorkspaceError::from)?;
                }
                LazyDirectoryPhase::Authored(None) => encoder.u8(2),
                LazyDirectoryPhase::Authored(Some(name)) => {
                    encoder.u8(3);
                    name.encode_cursor_into(&mut encoder, &mut work, budget, cancellation)
                        .map_err(directory_namespace_error)?;
                }
            }
            cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
            let bytes = encoder.finish();
            if bytes.len() != size { return Err(LazyWorkspaceError::StaleCursor); }
            work.charge(&WorkCounters { output_bytes: bytes.len() as u64, ..WorkCounters::UNCHARGED }, &budget)?;
            Ok(bytes)
        })();
        result.map(|value| OperationReceipt { value, work })
            .map_err(|error: LazyWorkspaceError| error.into_measured_failure()
                .map_with_prior_work(work, std::convert::identity))
    }

    /// Decodes a bounded continuation using the actual workspace profile and limits.
    pub fn decode_measured(
        bytes: &[u8],
        profile: crate::model::FilesystemProfile,
        limits: crate::model::VolumeLimits,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<Self>, LazyWorkspaceError> {
        const DOMAIN: &[u8] = b"acyclic-fs-lazy-directory-cursor-v1\0";
        let mut work = WorkCounters::UNCHARGED;
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let result = (|| {
            cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
            work.charge(&WorkCounters {
                source_bytes_read: crate::foundation::usize_to_u64(bytes.len()),
                ..WorkCounters::UNCHARGED
            }, &budget)?;
            let mut decoder = crate::kernel::Decoder::new(bytes, DOMAIN, 1, budget.source_bytes_read.min(limits.maximum_object_bytes))
                .map_err(|_| LazyWorkspaceError::StaleCursor)?;
            let source = SourceReference {
                identity: decoder.fixed().map_err(|_| LazyWorkspaceError::StaleCursor)?,
                epoch: decoder.u64().map_err(|_| LazyWorkspaceError::StaleCursor)?,
            };
            let overlay = LazyOverlayId::from_bytes(decoder.fixed().map_err(|_| LazyWorkspaceError::StaleCursor)?);
            let snapshot = match decoder.u8().map_err(|_| LazyWorkspaceError::StaleCursor)? {
                0 => None,
                1 => Some(LazySnapshotId(decoder.fixed().map_err(|_| LazyWorkspaceError::StaleCursor)?)),
                _ => return Err(LazyWorkspaceError::StaleCursor),
            };
            let directory = NamespacePath::decode_cursor_with_ledger(
                &mut decoder, profile, limits, &mut allocations, &mut work, budget, cancellation,
            ).map_err(directory_namespace_error)?;
            let phase = match decoder.u8().map_err(|_| LazyWorkspaceError::StaleCursor)? {
                0 => LazyDirectoryPhase::Source(None),
                1 => LazyDirectoryPhase::Source(Some(SourceCursor::decode_cursor_with_ledger(
                    &mut decoder, source, &directory, profile, limits,
                    &mut allocations, &mut work, budget, cancellation,
                ).map_err(LazyWorkspaceError::from)?)),
                2 => LazyDirectoryPhase::Authored(None),
                3 => LazyDirectoryPhase::Authored(Some(LogicalName::decode_cursor_with_ledger(
                    &mut decoder, profile, limits.maximum_component_bytes,
                    &mut allocations, &mut work, budget, cancellation,
                ).map_err(directory_namespace_error)?)),
                _ => return Err(LazyWorkspaceError::StaleCursor),
            };
            decoder.finish().map_err(|_| LazyWorkspaceError::StaleCursor)?;
            cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
            Ok(Self { source, overlay, directory, phase, snapshot })
        })();
        result.map(|value| OperationReceipt { value, work })
            .map_err(|error: LazyWorkspaceError| error.into_measured_failure()
                .map_with_prior_work(work, std::convert::identity))
    }

    pub(crate) fn retained_bytes(&self) -> Result<u64, LazyWorkspaceError> {
        let continuation = match &self.phase {
            LazyDirectoryPhase::Source(Some(cursor)) => cursor.retained_bytes(),
            LazyDirectoryPhase::Authored(Some(name)) => crate::foundation::usize_to_u64(name.retained_bytes()),
            LazyDirectoryPhase::Source(None) | LazyDirectoryPhase::Authored(None) => 0,
        };
        self.directory.retained_bytes().checked_add(continuation).ok_or_else(counter_overflow)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum LazyDirectoryPhase {
    Source(Option<SourceCursor>),
    Authored(Option<LogicalName>),
}

/// One bounded page from the merged lazy directory view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LazyDirectoryPage {
    /// At most the requested number of visible entries.
    pub entries: Vec<LazyDirectoryEntry>,
    /// Opaque continuation, absent after the authored phase is exhausted.
    pub next: Option<LazyDirectoryCursor>,
}

/// Fail-closed lazy workspace errors.
#[derive(Debug, Error)]
pub enum LazyWorkspaceError {
    /// Authenticated workspace operation failed.
    #[error("workspace operation failed: {0}")]
    Workspace(String),
    /// Demand provider operation failed.
    #[error("source demand failed: {0}")]
    Demand(#[from] DemandError),
    /// Companion state failed.
    #[error("lazy state failed: {0}")]
    Store(String),
    /// Durable state changed too frequently to complete a bounded retry.
    #[error("lazy state changed concurrently")]
    Concurrent,
    /// Provider identity or invalidation epoch no longer matches the binding.
    #[error("lazy source binding is stale")]
    StaleSource,
    /// Requested path is absent after applying authored tombstones.
    #[error("path is absent")]
    NotFound,
    /// Requested subtree root is not a directory.
    #[error("path is not a directory")]
    NotDirectory,
    /// Requested operation requires a regular file.
    #[error("path is not a regular file")]
    NotRegularFile,
    /// Requested read exceeds the explicit bound.
    #[error("file exceeds the requested read bound")]
    TooLarge,
    /// A directory page must request at least one entry.
    #[error("directory page bound must be positive")]
    InvalidPageBound,
    /// Directory view changed after the supplied cursor was issued.
    #[error("lazy directory cursor is stale")]
    StaleCursor,
    /// The path no longer names the object the caller resolved.
    #[error("lazy filesystem object identity is stale")]
    StaleIdentity,
    /// The source fact cannot be represented by this workspace profile.
    #[error("source node cannot be represented by the authored workspace")]
    UnsupportedNode,
    /// Removing the last promoted binding would resurrect stale source bytes
    /// through an unresolved hard-link alias.
    #[error("source hard-link aliases must be promoted before removing the final authored binding")]
    UnresolvedHardLinks,
    /// Exact traversal was cancelled before every path was captured.
    #[error("lazy exact scan was cancelled")]
    Cancelled,
    /// Exact traversal exceeded an explicit work budget.
    #[error("lazy exact scan exceeded its work budget: {0}")]
    Work(String),
    /// Recoverable failure retaining the exact work of its nested engine.
    #[error("{0}")]
    SpentWork(Box<crate::OperationFailure<Self>>),
}

impl LazyWorkspaceError {
    /// Borrows the original semantic failure without discarding its work receipt.
    #[must_use]
    pub fn semantic_error(&self) -> &Self {
        match self {
            Self::SpentWork(failure) => failure.error.semantic_error(),
            error => error,
        }
    }

    /// Exact work retained by a failed demand, publication, or companion IO.
    #[must_use]
    pub fn spent_work(&self) -> WorkCounters {
        match self {
            Self::SpentWork(failure) => *failure.work,
            _ => WorkCounters::default(),
        }
    }

    /// Moves the original failure and counters into a measured producer result.
    pub fn into_measured_failure(self) -> crate::OperationFailure<Self> {
        match self {
            Self::SpentWork(failure) => *failure,
            error => crate::OperationFailure::before_work(error),
        }
    }

    fn with_prior_work(self, prior: WorkCounters) -> Self {
        let failure = self.into_measured_failure()
            .map_with_prior_work(prior, |error| error);
        if *failure.work == WorkCounters::default() {
            failure.error
        } else {
            Self::SpentWork(Box::new(failure))
        }
    }
}

#[derive(Clone, Copy)]
struct ExactificationSource(SourceReference);

impl LazySnapshotRef {
    fn exactification_source(
        self,
        live_source: SourceReference,
    ) -> Result<ExactificationSource, LazyWorkspaceError> {
        if self.source.identity != live_source.identity {
            return Err(LazyWorkspaceError::StaleSource);
        }
        Ok(ExactificationSource(live_source))
    }

    fn exactification_state(
        self,
        workspace_id: WorkspaceId,
        parent_workspace_id: WorkspaceId,
        source: ExactificationSource,
    ) -> LazyWorkspaceState {
        LazyWorkspaceState {
            schema_version: LAZY_STATE_SCHEMA,
            revision: 1,
            workspace_id,
            parent_workspace_id: Some(parent_workspace_id),
            fork_origin: None,
            // Observed overlay records retain their pinned source epochs. This
            // binding is deliberately live for paths unresolved at capture.
            source: source.0,
            overlay: self.overlay,
            shadows: self.shadows,
            pending_remove: None,
        }
    }
}

impl<A, O, D, S> LazyWorkspace<A, O, D, S>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    /// Creates an empty authored workspace and binds a source without demanding any path.
    pub async fn attach(
        fs: &Fs<A, O>,
        name: impl AsRef<str>,
        source: Arc<D>,
        store: S,
    ) -> Result<Self, LazyWorkspaceError> {
        let workspace = fs.create_workspace(name).await.map_err(workspace_error)?;
        Self::attach_workspace(workspace, source, store).await
    }

    /// Creates an authored workspace with exact filesystem semantics and binds
    /// a source without demanding any path.
    pub async fn attach_with_config(
        fs: &Fs<A, O>,
        name: impl AsRef<str>,
        source: Arc<D>,
        store: S,
        config: VolumeConfig,
    ) -> Result<Self, LazyWorkspaceError> {
        let workspace = fs
            .create_workspace_with_config(name, config)
            .await
            .map_err(workspace_error)?;
        Self::attach_workspace(workspace, source, store).await
    }

    /// Creates or recovers one original lazy attachment without tree demand.
    /// Returns its immutable initial generation separately from mutable lazy
    /// observations. Logical companion-store requests use the existing meter;
    /// this does not exactify, hydrate overlays or recover pending removals.
    #[allow(clippy::too_many_arguments, reason = "explicit original creation and finite provider bounds")]
    pub async fn attach_with_operation_measured(
        fs: &Fs<A, O>,
        name: impl AsRef<str>,
        source: Arc<D>,
        store: S,
        config: VolumeConfig,
        operation_id: crate::OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<(Self, crate::Generation<A, O>)>, LazyWorkspaceError> {
        let reference = source.reference();
        let created = fs
            .create_workspace_with_operation_measured(name, config, operation_id, budget, cancellation)
            .await
            .map_err(|failure| failure.map_with_prior_work(WorkCounters::default(), workspace_error))?;
        let mut work = created.work;
        let result = async {
            let workspace = created.value.workspace().clone();
            let loaded = store.load_lazy_workspace_measured(
                workspace.id(), remaining_work(work, budget)?, cancellation,
            ).await?;
            work = account_work(work, loaded.work, budget)?;
            let existing = loaded.value;
            if let Some(state) = existing {
                state.require_ready_fork()?;
                if state.schema_version != LAZY_STATE_SCHEMA
                    || state.workspace_id != workspace.id()
                    || state.source.identity != reference.identity
                {
                    return Err(LazyWorkspaceError::StaleSource);
                }
            } else {
                let overlay = LazyOverlay::default();
                let hashed = hash_lazy_value_measured(&overlay, remaining_work(work, budget)?, cancellation)?;
                work = account_work(work, hashed.work, budget)?;
                let overlay_id = LazyOverlayId(hashed.value);
                let stored = store.put_lazy_overlay_measured(
                    overlay_id, overlay, remaining_work(work, budget)?, cancellation,
                ).await?;
                work = account_work(work, stored.work, budget)?;
                let shadows = LazyShadow::default();
                let hashed = hash_lazy_value_measured(&shadows, remaining_work(work, budget)?, cancellation)?;
                work = account_work(work, hashed.work, budget)?;
                let shadow_id = LazyShadowId(hashed.value);
                let stored = store.put_lazy_shadow_measured(
                    shadow_id, shadows, remaining_work(work, budget)?, cancellation,
                ).await?;
                work = account_work(work, stored.work, budget)?;
                let state = LazyWorkspaceState {
                    schema_version: LAZY_STATE_SCHEMA,
                    revision: 1,
                    workspace_id: workspace.id(),
                    parent_workspace_id: None,
                    fork_origin: None,
                    source: reference,
                    overlay: overlay_id,
                    shadows: shadow_id,
                    pending_remove: None,
                };
                let swapped = store.compare_and_swap_lazy_workspace_measured(
                    workspace.id(), 0, state, remaining_work(work, budget)?, cancellation,
                ).await?;
                work = account_work(work, swapped.work, budget)?;
                if !swapped.value
                {
                    return Err(LazyWorkspaceError::Concurrent);
                }
            }
            cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
            if source.reference() != reference {
                return Err(LazyWorkspaceError::StaleSource);
            }
            Ok((Self { workspace, source, store }, created.value))
        }.await;
        result
            .map(|value| OperationReceipt { value, work })
            .map_err(|error: LazyWorkspaceError| error.into_measured_failure().map_with_prior_work(work, |error| error))
    }

    async fn attach_workspace(
        workspace: Workspace<A, O>,
        source: Arc<D>,
        store: S,
    ) -> Result<Self, LazyWorkspaceError> {
        let overlay = LazyOverlay::default();
        let overlay_id = overlay.id()?;
        store
            .put_lazy_overlay(overlay_id, overlay)
            .await
            .map_err(store_error)?;
        let shadows = LazyShadow::default();
        let shadow_id = shadows.id()?;
        store
            .put_lazy_shadow(shadow_id, shadows)
            .await
            .map_err(store_error)?;
        let state = LazyWorkspaceState {
            schema_version: LAZY_STATE_SCHEMA,
            revision: 1,
            workspace_id: workspace.id(),
            parent_workspace_id: None,
            fork_origin: None,
            source: source.reference(),
            overlay: overlay_id,
            shadows: shadow_id,
            pending_remove: None,
        };
        let existing = store
            .load_lazy_workspace(workspace.id())
            .await
            .map_err(store_error)?;
        match existing {
            Some(existing)
                if existing.source == state.source
                    && existing.schema_version == LAZY_STATE_SCHEMA => {
                        existing.require_ready_fork()?;
                    }
            Some(_) => return Err(LazyWorkspaceError::StaleSource),
            None => {
                if !store
                    .compare_and_swap_lazy_workspace(workspace.id(), 0, state)
                    .await
                    .map_err(store_error)?
                {
                    return Err(LazyWorkspaceError::Concurrent);
                }
            }
        }
        let lazy = Self {
            workspace,
            source,
            store,
        };
        lazy.recover_pending_remove().await?;
        Ok(lazy)
    }

    /// Reopens `workspace`'s binding, or binds it to `source` afresh when no
    /// binding is durable.
    ///
    /// A binding is absent only when its attach never became durable. Every
    /// later change to the workspace's binding is ordered after the attach, so
    /// nothing durable was built on a lost one and binding afresh is exact.
    pub async fn open_or_attach(
        workspace: Workspace<A, O>,
        source: Arc<D>,
        store: S,
    ) -> Result<Self, LazyWorkspaceError> {
        if store
            .load_lazy_workspace(workspace.id())
            .await
            .map_err(store_error)?
            .is_none()
        {
            return Self::attach_workspace(workspace, source, store).await;
        }
        Self::open(workspace, source, store).await
    }

    /// Reopens an existing sparse workspace without demanding source state.
    pub async fn open(
        workspace: Workspace<A, O>,
        source: Arc<D>,
        store: S,
    ) -> Result<Self, LazyWorkspaceError> {
        let state = store
            .load_lazy_workspace(workspace.id())
            .await
            .map_err(store_error)?
            .ok_or_else(|| LazyWorkspaceError::Store("lazy binding is absent".to_owned()))?;
        state.require_ready_fork()?;
        if state.schema_version != LAZY_STATE_SCHEMA
            || state.workspace_id != workspace.id()
            || state.source.identity != source.reference().identity
        {
            return Err(LazyWorkspaceError::StaleSource);
        }
        if store
            .load_lazy_overlay(state.overlay)
            .await
            .map_err(store_error)?
            .is_none()
        {
            return Err(LazyWorkspaceError::Store(
                "lazy overlay is absent".to_owned(),
            ));
        }
        if store
            .load_lazy_shadow(state.shadows)
            .await
            .map_err(store_error)?
            .is_none()
        {
            return Err(LazyWorkspaceError::Store(
                "lazy identity shadow is absent".to_owned(),
            ));
        }
        let lazy = Self {
            workspace,
            source,
            store,
        };
        lazy.recover_pending_remove().await?;
        lazy.rebind_source().await?;
        Ok(lazy)
    }

    /// Observes an existing binding and reconstructs its lazy handle read-only.
    ///
    /// The caller supplies the source capability authenticated against its
    /// retained physical identity. This observes only the constant-size binding:
    /// it never creates one, hydrates nodes, recovers removals or rebinds epochs.
    ///
    /// # Errors
    ///
    /// Retains actual provider work on failure and rejects a binding whose
    /// schema, workspace or exact source reference differs from the capability.
    pub async fn resume_measured(
        workspace: Workspace<A, O>,
        source: Arc<D>,
        store: S,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<Option<Self>>, LazyWorkspaceError> {
        let mut work = WorkCounters::UNCHARGED;
        let result = async {
            cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
            let reference = source.reference();
            let loaded = store.load_lazy_workspace_measured(
                workspace.id(), remaining_work(work, budget)?, cancellation,
            ).await?;
            work = account_work(work, loaded.work, budget)?;
            cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
            let Some(state) = loaded.value else {
                return Ok(None);
            };
            state.require_ready_fork()?;
            if state.schema_version != LAZY_STATE_SCHEMA
                || state.workspace_id != workspace.id()
                || state.source != reference
            {
                return Err(LazyWorkspaceError::StaleSource);
            }
            cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
            if source.reference() != reference {
                return Err(LazyWorkspaceError::StaleSource);
            }
            Ok(Some(Self { workspace, source, store }))
        }.await;
        result
            .map(|value| OperationReceipt { value, work })
            .map_err(|error: LazyWorkspaceError| {
                error.into_measured_failure().map_with_prior_work(work, |error| error)
            })
    }

    /// Activates an existing root binding after read-only allocation observation.
    ///
    /// Authenticates the original source and existing immutable roots, then
    /// advances only its observational epoch through the canonical rebind CAS.
    /// It never attaches a fresh binding or recovers a pending authored removal.
    ///
    /// # Errors
    ///
    /// Rejects replaced or regressed source identity and absent immutable roots.
    /// All store and rebind failures retain the complete measured prefix.
    pub async fn resume_root_observation_measured(
        workspace: Workspace<A, O>,
        source: Arc<D>,
        store: S,
        expected_source: SourceReference,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<Self>, LazyWorkspaceError> {
        let mut work = WorkCounters::UNCHARGED;
        let result = async {
            cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
            let loaded = store.load_lazy_workspace_measured(
                workspace.id(), remaining_work(work, budget)?, cancellation,
            ).await?;
            work = account_work(work, loaded.work, budget)?;
            let state = loaded.value
                .ok_or_else(|| LazyWorkspaceError::Store("lazy binding is absent".to_owned()))?;
            state.validate_root_binding(workspace.id(), expected_source)?;
            let live = source.reference();
            if live.identity != expected_source.identity || live.epoch < state.source.epoch {
                return Err(LazyWorkspaceError::StaleSource);
            }
            let overlay = store.load_lazy_overlay_measured(
                state.overlay, remaining_work(work, budget)?, cancellation,
            ).await?;
            work = account_work(work, overlay.work, budget)?;
            let overlay_present = overlay.value.is_some();
            drop(overlay.value);
            if !overlay_present {
                return Err(LazyWorkspaceError::Store("lazy overlay is absent".to_owned()));
            }
            let shadow = store.load_lazy_shadow_measured(
                state.shadows, remaining_work(work, budget)?, cancellation,
            ).await?;
            work = account_work(work, shadow.work, budget)?;
            let shadow_present = shadow.value.is_some();
            drop(shadow.value);
            if !shadow_present {
                return Err(LazyWorkspaceError::Store("lazy identity shadow is absent".to_owned()));
            }
            cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
            let lazy = Self { workspace, source, store };
            let rebound = lazy.rebind_source_measured(
                remaining_work(work, budget)?, cancellation,
            ).await.map_err(|failure| lazy_operation_failure(failure, |error| error))?;
            work = account_work(work, rebound.work, budget)?;
            Ok(lazy)
        }.await;
        result
            .map(|value| OperationReceipt { value, work })
            .map_err(|error: LazyWorkspaceError| {
                error.into_measured_failure().map_with_prior_work(work, |error| error)
            })
    }

    /// Authored SDK workspace used by merge, lineage, and publication primitives.
    #[must_use]
    pub const fn workspace(&self) -> &Workspace<A, O> {
        &self.workspace
    }

    /// Reopens another workspace bound to the same source capability.
    ///
    /// This is the constant-size branch/resume primitive for adapters that
    /// already resolved the destination workspace through durable lineage.
    pub async fn open_related(&self, workspace: Workspace<A, O>) -> Result<Self, LazyWorkspaceError>
    where
        S: Clone,
    {
        Self::open(workspace, Arc::clone(&self.source), self.store.clone()).await
    }

    /// Captures the complete logical lazy-tree identity without enumeration.
    pub async fn snapshot(&self) -> Result<LazySnapshotRef, LazyWorkspaceError> {
        let state = self.state().await?;
        let authored_generation = self.workspace.head().await.map_err(workspace_error)?.id();
        Ok(LazySnapshotRef::new(
            self.workspace.id(),
            authored_generation,
            state.source,
            state.overlay,
            state.shadows,
        ))
    }

    /// Captures the same complete lazy identity using bounded existing stores.
    pub async fn snapshot_measured(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<LazySnapshotRef>, LazyWorkspaceError> {
        let mut work = WorkCounters::default();
        let result = async {
            let state = self.state_measured(budget, cancellation).await?;
            work = state.work;
            let head = self.workspace.head_measured(remaining_work(work, budget)?, cancellation)
                .await.map_err(measured_workspace_error)?;
            work = account_work(work, head.work, budget)?;
            cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
            if self.source.reference() != state.value.source {
                return Err(LazyWorkspaceError::StaleSource);
            }
            let prospective = work.checked_add(WorkCounters {
                bytes_hashed: crate::foundation::usize_to_u64(LAZY_SNAPSHOT_DOMAIN.len()) + 136,
                ..WorkCounters::default()
            })?;
            prospective.verify(budget)?;
            work = prospective;
            let snapshot = LazySnapshotRef::new(
                self.workspace.id(), head.value.id(), state.value.source,
                state.value.overlay, state.value.shadows,
            );
            cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
            Ok(snapshot)
        }.await;
        result.map(|value| OperationReceipt { value, work })
            .map_err(|error| error.with_prior_work(work).into_measured_failure())
    }

    /// Loads the constant-size durable source/lineage binding.
    pub async fn binding(&self) -> Result<LazyWorkspaceState, LazyWorkspaceError> {
        self.state().await
    }

    /// Forks from the first durable source freeze, including after a lost acknowledgement.
    pub async fn fork(
        &self,
        destination: impl AsRef<str>,
        idempotency_key: IdempotencyKey,
    ) -> Result<Self, LazyWorkspaceError> {
        self.fork_measured(destination, idempotency_key, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    /// Uses the original companion freeze and the canonical filesystem fork producer.
    pub async fn fork_measured(
        &self, destination: impl AsRef<str>, idempotency_key: IdempotencyKey,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<Self>, LazyWorkspaceError> {
        self.fork_with_snapshot_measured(destination, idempotency_key, budget, cancellation)
            .await.map(|receipt| OperationReceipt { value: receipt.value.0, work: receipt.work })
    }

    /// Returns the original child snapshot, never a subsequently advanced child head.
    pub async fn fork_with_snapshot_measured(
        &self, destination: impl AsRef<str>, idempotency_key: IdempotencyKey,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<(Self, LazySnapshotRef)>, LazyWorkspaceError> {
        let meter = LazyTreapWork::new(budget, cancellation);
        let result = async {
            cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
            let (name, id) = meter.accept(self.workspace.volume.fs.workspace_name_identity_measured(
                destination, meter.remaining()?, cancellation,
            ).map_err(measured_workspace_error))?;
            let name_bytes = name.retained_bytes();
            meter.adopt(name_bytes)?;
            let origin = self.prepare_fork_origin_at(id, idempotency_key.operation_id(), None, &meter).await?;
            let generation = meter.accept(self.workspace.generation_measured(
                origin.snapshot.authored_generation, meter.remaining()?, cancellation,
            ).await.map_err(measured_workspace_error))?;
            // The owned destination transfers into the filesystem producer's ledger.
            meter.release(name_bytes)?;
            let workspace = meter.accept(self.workspace.volume.fs.fork_workspace_measured(
                name, &generation, idempotency_key, None, meter.remaining()?, cancellation,
            ).await.map_err(measured_workspace_error))?;
            meter.adopt(workspace.name.retained_bytes())?;
            let original = meter.accept(self.workspace.volume.fs.workspace_for_creation_operation_measured(
                workspace.name.as_str(), workspace.volume.config, origin.operation_id,
                meter.remaining()?, cancellation,
            ).await.map_err(measured_workspace_error))?.ok_or(LazyWorkspaceError::Concurrent)?;
            let child_snapshot = self.commit_prepared_fork_at(
                &workspace, original.id(), origin, &meter,
            ).await?;
            Ok((Self { workspace, source: Arc::clone(&self.source), store: self.store.clone() }, child_snapshot))
        }.await;
        result.map(|value| OperationReceipt { value, work: meter.spent() })
            .map_err(|error| crate::OperationFailure::new(error, meter.spent()))
    }

    /// Retains a qualified source freeze before another owner creates its filesystem project.
    /// Prepared companion metadata is not a ready workspace or a grant.
    pub async fn prepare_fork_snapshot_measured(
        &self, destination: impl AsRef<str>, frozen: LazySnapshotRef,
        original_operation_id: crate::OperationId, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<LazySnapshotRef>, LazyWorkspaceError> {
        let meter = LazyTreapWork::new(budget, cancellation);
        let result = async {
            cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
            let (name, id) = meter.accept(self.workspace.volume.fs.workspace_name_identity_measured(
                destination, meter.remaining()?, cancellation,
            ).map_err(measured_workspace_error))?;
            meter.adopt(name.retained_bytes())?;
            let origin = self.prepare_fork_origin_at(id, original_operation_id, Some(frozen), &meter).await?;
            Ok(origin.snapshot)
        }.await;
        result.map(|value| OperationReceipt { value, work: meter.spent() })
            .map_err(|error| crate::OperationFailure::new(error, meter.spent()))
    }

    /// Binds the same already-created project after authenticating its immutable creation.
    /// The original source freeze is never replaced by a current parent observation.
    pub async fn bind_prepared_fork_measured(
        &self, frozen: LazySnapshotRef, created_child: Workspace<A, O>,
        original_child_generation: crate::GenerationId, original_operation_id: crate::OperationId,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<(Self, LazySnapshotRef)>, LazyWorkspaceError> {
        let meter = LazyTreapWork::new(budget, cancellation);
        let result = async {
            cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
            if !self.workspace.volume.fs.same_deployment(&created_child.volume.fs) {
                return Err(workspace_error(WorkspaceError::ForeignGeneration));
            }
            meter.adopt(created_child.name.retained_bytes())?;
            let state = meter.accept(self.store.load_lazy_workspace_measured(
                created_child.id(), meter.remaining()?, cancellation,
            ).await)?.ok_or(LazyWorkspaceError::Concurrent)?;
            let origin = self.validate_fork_origin(&state, created_child.id(), original_operation_id, Some(frozen), &meter)?;
            let observed = meter.accept(self.workspace.volume.fs.workspace_for_creation_operation_measured(
                created_child.name.as_str(), created_child.volume.config, original_operation_id,
                meter.remaining()?, cancellation,
            ).await.map_err(measured_workspace_error))?.ok_or(LazyWorkspaceError::Concurrent)?;
            if observed.id() != original_child_generation || observed.workspace_id() != created_child.id() {
                return Err(LazyWorkspaceError::Concurrent);
            }
            let child_snapshot = self.commit_prepared_fork_at(
                &created_child, original_child_generation, origin, &meter,
            ).await?;
            Ok((Self { workspace: created_child, source: Arc::clone(&self.source), store: self.store.clone() }, child_snapshot))
        }.await;
        result.map(|value| OperationReceipt { value, work: meter.spent() })
            .map_err(|error| crate::OperationFailure::new(error, meter.spent()))
    }

    fn validate_fork_origin(
        &self, state: &LazyWorkspaceState, destination: WorkspaceId,
        original_operation_id: crate::OperationId, qualified: Option<LazySnapshotRef>,
        meter: &LazyTreapWork<'_>,
    ) -> Result<LazyForkOrigin, LazyWorkspaceError> {
        let origin = state.fork_origin.ok_or(LazyWorkspaceError::Concurrent)?;
        if state.schema_version != LAZY_STATE_SCHEMA || state.workspace_id != destination
            || state.parent_workspace_id != Some(self.workspace.id())
            || origin.operation_id != original_operation_id
            || origin.config != self.workspace.volume.config
            || origin.snapshot.workspace_id != self.workspace.id()
            || origin.snapshot.source.identity != self.source.reference().identity
            || qualified.is_some_and(|snapshot| snapshot != origin.snapshot)
        {
            return Err(LazyWorkspaceError::Concurrent);
        }
        meter.charge(WorkCounters {
            bytes_hashed: crate::foundation::usize_to_u64(LAZY_SNAPSHOT_DOMAIN.len()) + 136,
            ..WorkCounters::UNCHARGED
        })?;
        if LazySnapshotRef::new(origin.snapshot.workspace_id, origin.snapshot.authored_generation,
            origin.snapshot.source, origin.snapshot.overlay, origin.snapshot.shadows) != origin.snapshot
        {
            return Err(LazyWorkspaceError::StaleSource);
        }
        if origin.phase == LazyForkPhase::Prepared && (state.source != origin.snapshot.source
            || state.overlay != origin.snapshot.overlay || state.shadows != origin.snapshot.shadows
            || state.pending_remove.is_some())
        {
            return Err(LazyWorkspaceError::Concurrent);
        }
        Ok(origin)
    }

    async fn prepare_fork_origin_at(
        &self, destination: WorkspaceId, operation_id: crate::OperationId,
        qualified: Option<LazySnapshotRef>, meter: &LazyTreapWork<'_>,
    ) -> Result<LazyForkOrigin, LazyWorkspaceError> {
        if destination == self.workspace.id() { return Err(LazyWorkspaceError::Concurrent); }
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            meter.cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
            let loaded = meter.accept(self.store.load_lazy_workspace_measured(
                destination, meter.remaining()?, meter.cancellation,
            ).await)?;
            if let Some(state) = loaded {
                return self.validate_fork_origin(&state, destination, operation_id, qualified, meter);
            }
            let snapshot = if let Some(snapshot) = qualified { snapshot } else {
                meter.accept(self.snapshot_measured(meter.remaining()?, meter.cancellation).await
                    .map_err(|failure| lazy_operation_failure(failure, |error| error)))?
            };
            meter.accept(self.validate_snapshot_measured(snapshot, meter.remaining()?, meter.cancellation).await
                .map_err(|failure| lazy_operation_failure(failure, |error| error)))?;
            let origin = LazyForkOrigin {
                operation_id, snapshot, config: self.workspace.volume.config, phase: LazyForkPhase::Prepared,
            };
            let state = LazyWorkspaceState {
                schema_version: LAZY_STATE_SCHEMA, revision: 1, workspace_id: destination,
                parent_workspace_id: Some(snapshot.workspace_id), fork_origin: Some(origin),
                source: snapshot.source, overlay: snapshot.overlay, shadows: snapshot.shadows, pending_remove: None,
            };
            // Reuse SDK publication custody: the actual closure is protected from
            // concurrent sweeping until the FIRST companion freeze becomes durable.
            let hold = meter.accept(self.workspace.volume.fs.make_generation_durable_measured(
                &self.workspace.volume, snapshot.authored_generation, meter.remaining()?, meter.cancellation,
            ).await.map_err(measured_workspace_error))?;
            let swapped = meter.accept(self.store.compare_and_swap_lazy_workspace_measured(
                destination, 0, state, meter.remaining()?, meter.cancellation,
            ).await)?;
            drop(hold);
            if swapped { return Ok(origin); }
            // A qualified capture never substitutes a refreshed parent. A canonical
            // first attempt may recapture only while no durable freeze exists.
            if qualified.is_some() {
                let state = meter.accept(self.store.load_lazy_workspace_measured(
                    destination, meter.remaining()?, meter.cancellation,
                ).await)?.ok_or(LazyWorkspaceError::Concurrent)?;
                return self.validate_fork_origin(&state, destination, operation_id, qualified, meter);
            }
        }
        Err(LazyWorkspaceError::Concurrent)
    }

    async fn commit_prepared_fork_at(
        &self, workspace: &Workspace<A, O>, original_generation: crate::GenerationId,
        origin: LazyForkOrigin, meter: &LazyTreapWork<'_>,
    ) -> Result<LazySnapshotRef, LazyWorkspaceError> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            meter.cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
            let state = meter.accept(self.store.load_lazy_workspace_measured(
                workspace.id(), meter.remaining()?, meter.cancellation,
            ).await)?.ok_or(LazyWorkspaceError::Concurrent)?;
            let existing = self.validate_fork_origin(
                &state, workspace.id(), origin.operation_id, Some(origin.snapshot), meter,
            )?;
            if existing.phase == LazyForkPhase::Committed {
                meter.charge(WorkCounters {
                    bytes_hashed: crate::foundation::usize_to_u64(LAZY_SNAPSHOT_DOMAIN.len()) + 136,
                    ..WorkCounters::UNCHARGED
                })?;
                return Ok(LazySnapshotRef::new(workspace.id(), original_generation,
                    origin.snapshot.source, origin.snapshot.overlay, origin.snapshot.shadows));
            }
            let revision = state.revision.checked_add(1).ok_or(LazyWorkspaceError::Concurrent)?;
            let committed = LazyWorkspaceState {
                revision, fork_origin: Some(LazyForkOrigin { phase: LazyForkPhase::Committed, ..origin }), ..state
            };
            if meter.accept(self.store.compare_and_swap_lazy_workspace_measured(
                workspace.id(), revision - 1, committed, meter.remaining()?, meter.cancellation,
            ).await)? {
                meter.charge(WorkCounters {
                    bytes_hashed: crate::foundation::usize_to_u64(LAZY_SNAPSHOT_DOMAIN.len()) + 136,
                    ..WorkCounters::UNCHARGED
                })?;
                return Ok(LazySnapshotRef::new(workspace.id(), original_generation,
                    origin.snapshot.source, origin.snapshot.overlay, origin.snapshot.shadows));
            }
        }
        Err(LazyWorkspaceError::Concurrent)
    }

    /// Materializes an immutable lazy snapshot in a deterministic scratch fork.
    ///
    /// Previously observed records remain pinned to the snapshot's source epoch;
    /// unresolved paths are observed from the injected live source when first
    /// demanded. The owning workspace is never moved or rewritten.
    pub async fn exactify_snapshot(
        &self,
        snapshot: LazySnapshotRef,
        destination: impl AsRef<str>,
        idempotency_key: IdempotencyKey,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<crate::Generation<A, O>>, LazyWorkspaceError> {
        cancellation
            .check()
            .map_err(|_| LazyWorkspaceError::Cancelled)?;
        if snapshot.workspace_id != self.workspace.id() {
            return Err(LazyWorkspaceError::StaleSource);
        }
        let exactification_source = snapshot.exactification_source(self.source.reference())?;
        let overlay = self
            .store
            .load_lazy_overlay_measured(snapshot.overlay, budget, cancellation)
            .await?;
        let mut work = overlay.work;
        let shadow = self
            .store
            .load_lazy_shadow_measured(
                snapshot.shadows,
                remaining_work(work, budget)?,
                cancellation,
            )
            .await?;
        work = account_work(work, shadow.work, budget)?;
        if overlay.value.is_none() || shadow.value.is_none() {
            return Err(LazyWorkspaceError::Store(
                "lazy snapshot index is absent".to_owned(),
            ));
        }
        let generation = self
            .workspace
            .generation_measured(
                snapshot.authored_generation,
                remaining_work(work, budget)?,
                cancellation,
            )
            .await.map_err(measured_workspace_error)?;
        work = account_work(work, generation.work, budget)?;
        let workspace = self
            .workspace
            .fork_measured(
                destination,
                ForkOptions::from_generation(generation.value, idempotency_key),
                remaining_work(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| measured_workspace_error(failure).with_prior_work(work))?;
        work = account_work(work, workspace.work, budget)?;
        let workspace = workspace.value;
        let state = snapshot.exactification_state(
            workspace.id(),
            self.workspace.id(),
            exactification_source,
        );
        let inserted = self
            .store
            .compare_and_swap_lazy_workspace_measured(
                workspace.id(),
                0,
                state.clone(),
                remaining_work(work, budget)?,
                cancellation,
            )
            .await?;
        work = account_work(work, inserted.work, budget)?;
        if !inserted.value {
            let existing = self
                .store
                .load_lazy_workspace_measured(
                    workspace.id(),
                    remaining_work(work, budget)?,
                    cancellation,
                )
                .await?;
            work = account_work(work, existing.work, budget)?;
            let existing = existing.value.ok_or(LazyWorkspaceError::Concurrent)?;
            if existing.parent_workspace_id != state.parent_workspace_id
                || existing.source != state.source
                || existing.overlay != state.overlay
                || existing.shadows != state.shadows
            {
                return Err(LazyWorkspaceError::Concurrent);
            }
        }
        let exactified = Self {
            workspace,
            source: Arc::clone(&self.source),
            store: self.store.clone(),
        }
        .exactify(remaining_work(work, budget)?, cancellation)
        .await?;
        work = account_work(work, exactified.work, budget)?;
        Ok(OperationReceipt {
            value: exactified.value,
            work,
        })
    }

    /// Captures only selected frozen source regions, then projects selected
    /// imported records/removals through the same guarded SDK transactions.
    /// Unselected authored records remain frozen; unselected source stays lazy.
    pub async fn exactify_snapshot_selected_at_generation_measured<F, Fut>(
        &self,
        frozen: LazySnapshotRef,
        imported: &crate::Generation<A, O>,
        paths: &[String],
        destination: impl AsRef<str>,
        baseline_key: IdempotencyKey,
        candidate_key: IdempotencyKey,
        permit: crate::PublicationPermit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        mut before_publication: F,
    ) -> crate::MeasuredResult<OperationReceipt<(crate::Generation<A, O>, crate::Generation<A, O>)>, LazyWorkspaceError>
    where
        F: FnMut(WorkBudget) -> Fut + Send,
        Fut: Future<Output = crate::FsResult<()>> + Send,
    {
        in_heap_measured(move |budget| async move {
            let work = LazyTreapWork::new(budget, cancellation);
            let result = async {
                {
                    let mut counters = work.work.lock().expect("lazy treap work lock poisoned");
                    self.readonly_snapshot_state(frozen, true, &mut counters, budget, cancellation)?;
                }
                if imported.workspace_id() != self.workspace.id() {
                    return Err(LazyWorkspaceError::StaleSource);
                }
                for path in paths {
                    cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
                    PortablePath::validate(path, self.workspace.limits())
                        .map_err(|error| LazyWorkspaceError::Workspace(error.to_string()))?;
                }
                let mut seal_guard = |allowance| {
                    let owner = before_publication(allowance);
                    async move {
                        cancellation.check().map_err(|error| crate::OperationFailure::before_work(crate::FsError::from(error)))?;
                        if self.source.reference() != frozen.source {
                            return Err(crate::OperationFailure::before_work(crate::FsError::SourceSnapshotStale));
                        }
                        let receipt = owner.await?;
                        cancellation.check().map_err(|error| crate::OperationFailure::new(crate::FsError::from(error), receipt.work))?;
                        if self.source.reference() != frozen.source {
                            return Err(crate::OperationFailure::new(crate::FsError::SourceSnapshotStale, receipt.work));
                        }
                        Ok(receipt)
                    }
                };
                let generation = work.accept(self.workspace.generation_measured(
                    frozen.authored_generation, work.remaining()?, cancellation,
                ).await.map_err(measured_workspace_error))?;
                let workspace = work.accept(self.workspace.fork_measured_guarded(
                    destination, ForkOptions::from_generation(generation, baseline_key),
                    permit, work.remaining()?, cancellation, &mut seal_guard,
                ).await.map_err(measured_workspace_error))?;
                let name_bytes = workspace.name.retained_bytes();
                work.adopt(name_bytes)?;
                for (index, path) in paths.iter().enumerate() {
                    let mut covered = false;
                    for (other_index, other) in paths.iter().enumerate() {
                        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
                        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                        if (path == other && other_index < index)
                            || (path != other && (other == "/"
                                || path.strip_prefix(other.as_str()).is_some_and(|suffix| suffix.starts_with('/')))) {
                            covered = true;
                            break;
                        }
                    }
                    if covered { continue; }
                    work.accept(self.exactify_selected_guarded(
                        &workspace, path, Some((self, frozen)), work.remaining()?,
                        cancellation, permit, &mut seal_guard,
                    ).await)?;
                }
                let baseline = work.accept(workspace.head_measured(work.remaining()?, cancellation)
                    .await.map_err(measured_workspace_error))?;
                let restored = work.accept(workspace.restore_paths_from_with_permit_measured_guarded(
                    imported, paths, baseline.id(), candidate_key, permit,
                    work.remaining()?, cancellation, &mut seal_guard,
                ).await.map_err(measured_workspace_error))?;
                let candidate = match restored {
                    TransactionCommit::Committed(generation)
                    | TransactionCommit::AlreadyCommitted(generation) => generation,
                    TransactionCommit::Conflict { .. } | TransactionCommit::Fenced =>
                        return Err(LazyWorkspaceError::StaleIdentity),
                    TransactionCommit::IdempotencyConflict => return Err(LazyWorkspaceError::Concurrent),
                };
                let publication_bytes = candidate.publication_retained_bytes();
                work.adopt(publication_bytes)?;
                work.release(publication_bytes)?;
                work.release(name_bytes)?;
                Ok((baseline, candidate))
            }.await;
            let spent = work.spent();
            result.map(|value| OperationReceipt { value, work: spent })
                .map_err(|error: LazyWorkspaceError| error.with_prior_work(spent).into_measured_failure())
        }, budget, cancellation, LazyWorkspaceError::Cancelled).await
    }

    /// Advances this binding to the provider's current invalidation epoch
    /// without enumerating the source. Prior overlay roots remain immutable;
    /// observations are refreshed on demand in the new epoch while authored
    /// state and tombstones remain shared.
    pub async fn rebind_source(&self) -> Result<LazyWorkspaceState, LazyWorkspaceError> {
        self.rebind_source_measured(WorkBudget::UNBOUNDED, &CancellationToken::new()).await
            .map(|receipt| receipt.value)
            .map_err(|failure| lazy_operation_failure(failure, |error| error))
    }

    /// Advances only observational epoch state through the original companion CAS.
    pub async fn rebind_source_measured(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<LazyWorkspaceState>, LazyWorkspaceError> {
        let mut work = WorkCounters::default();
        let result = async {
        let source = self.source.reference();
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let loaded = self.store.load_lazy_workspace_measured(
                self.workspace.id(), remaining_work(work, budget)?, cancellation,
            ).await?;
            work = account_work(work, loaded.work, budget)?;
            let state = loaded.value
                .ok_or_else(|| LazyWorkspaceError::Store("lazy binding is absent".to_owned()))?;
            state.require_ready_fork()?;
            if state.schema_version != LAZY_STATE_SCHEMA
                || state.workspace_id != self.workspace.id()
                || state.source.identity != source.identity
            {
                return Err(LazyWorkspaceError::StaleSource);
            }
            if state.pending_remove.is_some() {
                return Err(LazyWorkspaceError::Concurrent);
            }
            if state.source == source {
                return Ok(state);
            }
            let replacement = LazyWorkspaceState {
                revision: state.revision.saturating_add(1),
                source,
                ..state.clone()
            };
            let replaced = self.store.compare_and_swap_lazy_workspace_measured(
                self.workspace.id(), state.revision, replacement.clone(),
                remaining_work(work, budget)?, cancellation,
            ).await?;
            work = account_work(work, replaced.work, budget)?;
            if replaced.value
            {
                return Ok(replacement);
            }
        }
        Err(LazyWorkspaceError::Concurrent)
        }.await;
        result.map(|value| OperationReceipt { value, work })
            .map_err(|error| error.with_prior_work(work).into_measured_failure())
    }

    /// Observes one path without reading file content.
    pub async fn stat(&self, path: &str) -> Result<LazyStat, LazyWorkspaceError> {
        let cancellation = CancellationToken::new();
        self.stat_measured(path, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map(|receipt| receipt.value)
    }

    async fn stat_measured(
        &self,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<LazyStat>, LazyWorkspaceError> {
        let receipt = self.lookup_measured(path, budget, cancellation).await?;
        let value = receipt.value.lookup.stat();
        Ok(OperationReceipt {
            value,
            work: receipt.work,
        })
    }

    /// Returns complete demanded facts without reading regular-file content.
    pub async fn lookup(&self, path: &str) -> Result<LazyLookup, LazyWorkspaceError> {
        let cancellation = CancellationToken::new();
        self.lookup_with_budget(path, WorkBudget::UNBOUNDED, &cancellation)
            .await.map(|receipt| receipt.value)
            .map_err(|failure| lazy_operation_failure(failure, std::convert::identity))
    }

    /// Resolves one path using the canonical bounded observation engine.
    pub async fn lookup_with_budget(
        &self,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<LazyLookup>, LazyWorkspaceError> {
        self.lookup_measured(path, budget, cancellation).await
            .map(|receipt| OperationReceipt { value: receipt.value.lookup, work: receipt.work })
            .map_err(LazyWorkspaceError::into_measured_failure)
    }

    async fn lookup_measured(
        &self,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<ResolvedLazyLookup>, LazyWorkspaceError> {
        in_heap_measured(move |budget| async move {
            let mut work = WorkCounters::default();
            let result = async {
            let authored = self
                .workspace.stat_optional_measured(path, budget, cancellation).await.map_err(measured_workspace_error)?;
            work = authored.work;
            if let Some(stat) = authored.value {
                let paths = LazyTreapWork::new(remaining_work(work, budget)?, cancellation);
                let path = paths.canonical_path_owned(path, self.workspace.limits())
                    .map_err(|error| error.with_prior_work(paths.spent()))?;
                work = account_work(work, paths.spent(), budget)?;
                return Ok(OperationReceipt {
                    value: ResolvedLazyLookup {
                        lookup: LazyLookup::Authored {
                            path,
                            stat,
                        },
                        source: None,
                    },
                    work,
                });
            }
            let state_receipt = self
                .state_measured(remaining_work(work, budget)?, cancellation)
                .await?;
            work = account_work(work, state_receipt.work, budget)?;
            let state = state_receipt.value;
            let tombstoned = self
                .tombstoned_measured(
                    state.overlay,
                    path,
                    remaining_work(work, budget)?,
                    cancellation,
                )
                .await?;
            work = account_work(work, tombstoned.work, budget)?;
            if tombstoned.value {
                return Err(LazyWorkspaceError::NotFound);
            }
            let observed = self.observe_measured(path, state, remaining_work(work, budget)?, cancellation).await?;
            work = account_work(work, observed.work, budget)?;
            let (source, node) = observed.value;
            let authored = self
                .authored_alias_measured(&node, remaining_work(work, budget)?, cancellation)
                .await?;
            work = account_work(work, authored.work, budget)?;
            if let Some(authored) = authored.value {
                return Ok(OperationReceipt {
                    value: ResolvedLazyLookup {
                        lookup: authored,
                        source: None,
                    },
                    work,
                });
            }
            Ok(OperationReceipt {
                value: ResolvedLazyLookup {
                    lookup: LazyLookup::Source(node),
                    source: Some(source),
                },
                work,
            })
            }.await;
            result.map_err(|error: LazyWorkspaceError| error.with_prior_work(work).into_measured_failure())
        }, budget, cancellation, LazyWorkspaceError::Cancelled)
        .await.map_err(|failure| lazy_operation_failure(failure, std::convert::identity))
    }

    /// Returns current facts without extending the immutable observation index.
    ///
    /// Directory projection uses this after receiving a source page so that
    /// gathering entry metadata cannot invalidate the page's continuation.
    pub async fn inspect(&self, path: &str) -> Result<LazyLookup, LazyWorkspaceError> {
        let cancellation = CancellationToken::new();
        self.inspect_measured(path, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map(|receipt| receipt.value.lookup)
    }

    async fn inspect_measured(
        &self,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<ResolvedLazyLookup>, LazyWorkspaceError> {
        self.inspect_with(path, InspectBasis::default(), budget, cancellation)
            .await
    }

    async fn inspect_snapshot_measured(
        &self,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<ResolvedLazyLookup>, LazyWorkspaceError> {
        let basis = InspectBasis {
            retain_pinned_observation: true,
            ..InspectBasis::default()
        };
        self.inspect_with(path, basis, budget, cancellation).await
    }

    async fn inspect_snapshot_after_authored_absence(
        &self,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<ResolvedLazyLookup>, LazyWorkspaceError> {
        let basis = InspectBasis {
            retain_pinned_observation: true,
            authored_absent: true,
            ..InspectBasis::default()
        };
        self.inspect_with(path, basis, budget, cancellation).await
    }

    /// Resolves `path` for a mount whose own checkout already lacks it.
    ///
    /// The mount's checkout, not the published head, is its authored view, so
    /// the head is not consulted again. `state` pins the snapshot a directory
    /// page was listed against; otherwise the current state is read.
    #[cfg(any(feature = "native-mount", test))]
    pub(crate) async fn inspect_unauthored(
        &self,
        path: &str,
        state: Option<&LazyWorkspaceState>,
    ) -> Result<(LazyLookup, Option<SourceReference>), LazyWorkspaceError> {
        self.inspect_unauthored_measured(path, state, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await.map(|receipt| receipt.value)
            .map_err(|failure| lazy_operation_failure(failure, std::convert::identity))
    }

    #[cfg(any(feature = "native-mount", test))]
    pub(crate) async fn inspect_unauthored_measured(
        &self,
        path: &str,
        state: Option<&LazyWorkspaceState>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<(LazyLookup, Option<SourceReference>)>, LazyWorkspaceError> {
        in_heap_measured(move |budget| async move {
            let basis = InspectBasis { state, authored_absent: true, ..InspectBasis::default() };
            self.inspect_with(path, basis, budget, cancellation).await
                .map(|receipt| OperationReceipt {
                    value: (receipt.value.lookup, receipt.value.source), work: receipt.work,
                })
                .map_err(LazyWorkspaceError::into_measured_failure)
        }, budget, cancellation, LazyWorkspaceError::Cancelled).await
    }

    /// The source view lookups currently resolve in.
    #[cfg(feature = "native-mount")]
    pub(crate) fn source_reference(&self) -> SourceReference {
        self.source.reference()
    }

    /// What the source itself names at `path` in the view `source`, read
    /// afresh and bypassing the overlay.
    #[cfg(feature = "native-mount")]
    pub(crate) async fn source_lookup(
        &self,
        source: SourceReference,
        path: &str,
    ) -> Result<Option<SourceNode>, LazyWorkspaceError> {
        self.source
            .lookup(
                source,
                &self.namespace_path(path)?,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value).map_err(|failure| lazy_operation_failure(failure, LazyWorkspaceError::from))
    }

    /// Resolves a source entry of a page listed against `state`, reusing the
    /// node the listing observed. The listing already excluded tombstoned and
    /// authored names, so neither the source nor the tombstones are consulted.
    #[cfg(feature = "native-mount")]
    pub(crate) async fn inspect_listed(
        &self,
        state: &LazyWorkspaceState,
        path: &str,
        node: SourceNode,
        unobserved: bool,
    ) -> Result<(LazyLookup, Option<SourceReference>), LazyWorkspaceError> {
        self.inspect_listed_measured(
            state, path, node, unobserved, WorkBudget::UNBOUNDED, &CancellationToken::new(),
        ).await.map(|receipt| receipt.value)
            .map_err(|failure| lazy_operation_failure(failure, std::convert::identity))
    }

    #[cfg(feature = "native-mount")]
    pub(crate) async fn inspect_listed_measured(
        &self,
        state: &LazyWorkspaceState,
        path: &str,
        node: SourceNode,
        unobserved: bool,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<(LazyLookup, Option<SourceReference>)>, LazyWorkspaceError> {
        in_heap_measured(move |budget| async move {
            let basis = InspectBasis {
                state: Some(state), authored_absent: true, listed: Some(node), unobserved,
                ..InspectBasis::default()
            };
            self.inspect_with(path, basis, budget, cancellation).await
                .map(|receipt| OperationReceipt {
                    value: (receipt.value.lookup, receipt.value.source), work: receipt.work,
                })
                .map_err(LazyWorkspaceError::into_measured_failure)
        }, budget, cancellation, LazyWorkspaceError::Cancelled).await
    }

    /// The source node `path` names in `state`: a pinned observation, the node
    /// a listing supplied, or a fresh source lookup, never a tombstoned path.
    async fn source_node_in(
        &self,
        state: &LazyWorkspaceState,
        path: &str,
        basis: InspectBasis<'_>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<(SourceReference, SourceNode)>, LazyWorkspaceError> {
        let fact = if basis.unobserved && basis.listed.is_some() {
            OperationReceipt {
                value: None,
                work: WorkCounters::default(),
            }
        } else {
            self.overlay_fact_measured(state.overlay, path, budget, cancellation)
                .await?
        };
        let mut work = fact.work;
        let observed = match fact.value {
            Some(LazyOverlayChange::Tombstone { .. }) => return Err(LazyWorkspaceError::NotFound.with_prior_work(work)),
            Some(LazyOverlayChange::Observe { source, node })
                if basis.retain_pinned_observation || source == state.source =>
            {
                Some((source, node))
            }
            Some(LazyOverlayChange::Observe { .. }) | None => None,
        };
        let (source, node) = if let Some(observed) = observed {
            observed
        } else if let Some(node) = basis.listed {
            (state.source, node)
        } else {
            if basis.authored_generation.is_some() && self.source.reference() != state.source {
                return Err(LazyWorkspaceError::StaleSource.with_prior_work(work));
            }
            let receipt = self.source_request_measured(path, remaining_work(work, budget)?, cancellation, |path, budget| async move { self.source.lookup(state.source, &path, budget, cancellation).await }).await.map_err(|error| error.with_prior_work(work))?;
            work = account_work(work, receipt.work, budget)?;
            (
                state.source,
                receipt.value.ok_or_else(|| LazyWorkspaceError::NotFound.with_prior_work(work))?,
            )
        };
        if basis.listed.is_none() {
            let tombstoned = self
                .tombstoned_measured(
                    state.overlay,
                    path,
                    remaining_work(work, budget)?,
                    cancellation,
                )
                .await.map_err(|error| error.with_prior_work(work))?;
            work = account_work(work, tombstoned.work, budget)?;
            if tombstoned.value {
                return Err(LazyWorkspaceError::NotFound.with_prior_work(work));
            }
        }
        Ok(OperationReceipt {
            value: (source, node),
            work,
        })
    }

    async fn inspect_with(
        &self,
        path: &str,
        basis: InspectBasis<'_>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<ResolvedLazyLookup>, LazyWorkspaceError> {
        let mut work = WorkCounters::default();
        let result = async {
        let authored = if basis.authored_absent {
            OperationReceipt {
                value: None,
                work: WorkCounters::default(),
            }
        } else if let Some(generation) = basis.authored_generation {
            self.authored_stat_at_measured(generation, path, budget, cancellation).await?
        } else {
            self.workspace.stat_optional_measured(path, budget, cancellation).await.map_err(measured_workspace_error)?
        };
        work = authored.work;
        if let Some(stat) = authored.value {
            let paths = LazyTreapWork::new(remaining_work(work, budget)?, cancellation);
            let path = paths.canonical_path_owned(path, self.workspace.limits())
                .map_err(|error| error.with_prior_work(paths.spent()))?;
            work = account_work(work, paths.spent(), budget)?;
            return Ok(OperationReceipt {
                value: ResolvedLazyLookup {
                    lookup: LazyLookup::Authored {
                        path,
                        stat,
                    },
                    source: None,
                },
                work,
            });
        }
        let loaded_state;
        let state = if let Some(state) = basis.state {
            state
        } else {
            let receipt = self
                .state_measured(remaining_work(work, budget)?, cancellation)
                .await?;
            work = account_work(work, receipt.work, budget)?;
            loaded_state = receipt.value;
            &loaded_state
        };
        let resolved = self
            .source_node_in(
                state,
                path,
                basis,
                remaining_work(work, budget)?,
                cancellation,
            )
            .await?;
        work = account_work(work, resolved.work, budget)?;
        let (source, node) = resolved.value;
        let authored = self
            .authored_alias_at_measured(&node, Some(state), basis.authored_generation,
                remaining_work(work, budget)?, cancellation)
            .await?;
        work = account_work(work, authored.work, budget)?;
        if let Some(authored) = authored.value {
            return Ok(OperationReceipt {
                value: ResolvedLazyLookup {
                    lookup: authored,
                    source: None,
                },
                work,
            });
        }
        Ok(OperationReceipt {
            value: ResolvedLazyLookup {
                lookup: LazyLookup::Source(node),
                source: Some(source),
            },
            work,
        })
        }.await;
        result.map_err(|error: LazyWorkspaceError| error.with_prior_work(work))
    }

    /// Returns a stable SDK identity for an unresolved source object.
    ///
    /// Aliases carrying the same source-native identity map to one `FileId`,
    /// independent of path, so mount lookups preserve hard-link topology.
    pub fn source_file_id(&self, node: &SourceNode) -> FileId {
        self.source_file_id_of(&node.file_identity)
    }

    fn source_file_id_measured(
        &self,
        node: &SourceNode,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<FileId, LazyWorkspaceError> {
        let receipt = self.source_file_id_of_measured(
            &node.file_identity, remaining_work(*work, budget)?, cancellation,
        ).map_err(|failure| lazy_operation_failure(failure, std::convert::identity))?;
        *work = account_work(*work, receipt.work, budget)?;
        Ok(receipt.value)
    }

    /// The identity of the source node whose [`SourceNode::file_identity`]
    /// is `file_identity`.
    pub(crate) fn source_file_id_of(&self, file_identity: &[u8; 32]) -> FileId {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"acyclic-fs-lazy-source-file-id-v1\0");
        hasher.update(&self.source.reference().identity);
        hasher.update(file_identity);
        let digest = hasher.finalize();
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(&digest.as_bytes()[..16]);
        FileId::from_bytes(bytes)
    }

    pub(crate) fn source_file_id_of_measured(
        &self,
        file_identity: &[u8; 32],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<FileId>, LazyWorkspaceError> {
        let mut work = WorkCounters::default();
        let result = (|| {
            let additional = WorkCounters {
                bytes_hashed: (b"acyclic-fs-lazy-source-file-id-v1\0".len()
                    + self.source.reference().identity.len() + file_identity.len()) as u64,
                ..WorkCounters::default()
            };
            work.admit(&additional, &budget)?;
            cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
            let file_id = self.source_file_id_of(file_identity);
            work.charge(&additional, &budget)?;
            Ok(file_id)
        })();
        result.map(|value| OperationReceipt { value, work })
            .map_err(|error| crate::OperationFailure::new(error, work))
    }

    /// Starts the source reporting its changes to `sink`; `None` when it
    /// cannot (see [`DemandSource::watch`]).
    #[cfg(feature = "native-mount")]
    pub(crate) fn watch_source(
        &self,
        sink: Arc<dyn crate::demand::SourceChangeSink>,
    ) -> Result<Option<Box<dyn crate::demand::SourceWatch>>, LazyWorkspaceError> {
        self.watch_source_measured(sink, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map(|receipt| receipt.value)
            .map_err(|failure| lazy_operation_failure(failure, std::convert::identity))
    }

    #[cfg(feature = "native-mount")]
    pub(crate) fn watch_source_measured(
        &self,
        sink: Arc<dyn crate::demand::SourceChangeSink>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<Option<Box<dyn crate::demand::SourceWatch>>>, LazyWorkspaceError> {
        self.source.watch(sink, budget, cancellation)
            .map_err(|failure| failure.map_with_prior_work(WorkCounters::default(), LazyWorkspaceError::from))
    }

    /// Direct identity of the resolved fact. Mounted directory identity is a
    /// separate projection rule in `stable_file_id_for_lookup`.
    fn direct_file_id_for_lookup(&self, lookup: &LazyLookup) -> FileId {
        match lookup {
            LazyLookup::Source(node) => self.source_file_id(node),
            LazyLookup::Authored { stat, .. } => stat.file_id,
            LazyLookup::Shadow { record, .. } => record.file_id,
        }
    }

    #[cfg(any(
        feature = "native-mount",
        all(test, feature = "native-watch", not(target_arch = "wasm32"))
    ))]
    pub(crate) async fn stable_file_id_for_lookup(
        &self,
        path: &str,
        lookup: &LazyLookup,
    ) -> Result<FileId, LazyWorkspaceError> {
        self.stable_file_id_for_lookup_measured(path, lookup, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await.map(|receipt| receipt.value)
            .map_err(|failure| lazy_operation_failure(failure, std::convert::identity))
    }

    #[cfg(any(
        feature = "native-mount",
        all(test, feature = "native-watch", not(target_arch = "wasm32"))
    ))]
    pub(crate) async fn stable_file_id_for_lookup_measured(
        &self,
        path: &str,
        lookup: &LazyLookup,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<FileId>, LazyWorkspaceError> {
        let mut work = WorkCounters::default();
        let result = async {
            cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
            match lookup {
                LazyLookup::Source(node) => self.source_file_id_measured(node, &mut work, budget, cancellation),
                LazyLookup::Shadow { record, .. } => Ok(record.file_id),
                LazyLookup::Authored { stat, .. } if stat.kind == FileKind::Directory => {
                    let state = self.state_measured(remaining_work(work, budget)?, cancellation).await?;
                    work = account_work(work, state.work, budget)?;
                    let fact = self.overlay_fact_measured(state.value.overlay, path, remaining_work(work, budget)?, cancellation).await?;
                    work = account_work(work, fact.work, budget)?;
                    match fact.value {
                        Some(LazyOverlayChange::Observe { node, .. }) if node.kind == SourceNodeKind::Directory =>
                            self.source_file_id_measured(&node, &mut work, budget, cancellation),
                        Some(LazyOverlayChange::Observe { .. })
                        | Some(LazyOverlayChange::Tombstone { .. })
                        | None => Ok(stat.file_id),
                    }
                }
                LazyLookup::Authored { stat, .. } => Ok(stat.file_id),
            }
        }.await;
        result.map(|value| OperationReceipt { value, work })
            .map_err(|error: LazyWorkspaceError| error.with_prior_work(work).into_measured_failure())
    }

    async fn authored_alias_measured(
        &self,
        node: &SourceNode,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Option<LazyLookup>>, LazyWorkspaceError> {
        self.authored_alias_at_measured(node, None, None, budget, cancellation).await
    }

    async fn authored_alias_at_measured(
        &self,
        node: &SourceNode,
        pinned_state: Option<&LazyWorkspaceState>,
        authored_generation: Option<crate::GenerationId>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Option<LazyLookup>>, LazyWorkspaceError> {
        in_heap_measured(move |budget| async move {
            let mut work = WorkCounters::default();
            let result = async {
            // Only source objects with multiple directory bindings can resolve
            // through an authored alias. Avoid the generation-wide reverse-path
            // query for ordinary files and directories; on large compiler trees
            // that otherwise creates one derived path-index entry per stat.
            if node.kind == SourceNodeKind::Directory || node.link_count == Some(1) {
                return Ok(OperationReceipt {
                    value: None,
                    work: WorkCounters::default(),
                });
            }
            let file_id = self.source_file_id_measured(node, &mut work, budget, cancellation)?;
            let loaded_state;
            let state = if let Some(state) = pinned_state {
                state
            } else {
                let loaded = self.state_measured(remaining_work(work, budget)?, cancellation).await?;
                work = account_work(work, loaded.work, budget)?;
                loaded_state = loaded.value;
                &loaded_state
            };
            let generation = if let Some(id) = authored_generation {
                let opened = self.workspace.generation_measured(id, remaining_work(work, budget)?, cancellation)
                    .await.map_err(measured_workspace_error)?;
                work = account_work(work, opened.work, budget)?;
                Some(opened.value)
            } else {
                let head = self.workspace.head_with_file_record_measured(
                    file_id, remaining_work(work, budget)?, cancellation,
                ).await.map_err(measured_workspace_error)?;
                work = account_work(work, head.work, budget)?;
                let (generation, held) = head.value;
                held.then_some(generation)
            };
            let path = if let Some(generation) = generation {
                let paths = generation.paths_for_file_id_measured(
                    file_id, u32::MAX, remaining_work(work, budget)?, cancellation,
                ).await.map_err(measured_workspace_error)?;
                work = account_work(work, paths.work, budget)?;
                paths.value.into_iter().next()
            } else {
                None
            };
            if let Some(path) = path {
                let stat = if let Some(id) = authored_generation {
                    self.authored_stat_at_measured(id, &path, remaining_work(work, budget)?, cancellation).await?
                } else {
                    self.workspace.stat_optional_measured(&path, remaining_work(work, budget)?, cancellation)
                        .await.map_err(measured_workspace_error)?
                };
                work = account_work(work, stat.work, budget)?;
                let mut stat = stat.value.ok_or(LazyWorkspaceError::Concurrent)?;
                if let Some(source_links) = node.link_count {
                    stat.link_count = stat.link_count.max(source_links);
                }
                return Ok(OperationReceipt {
                    value: Some(LazyLookup::Authored { path, stat }),
                    work,
                });
            }
            let shadow = self
                .shadow_record_measured(
                    state.shadows,
                    file_id,
                    remaining_work(work, budget)?,
                    cancellation,
                )
                .await?;
            work = account_work(work, shadow.work, budget)?;
            let Some((record, metadata)) = shadow.value else {
                return Ok(OperationReceipt { value: None, work });
            };
            Ok(OperationReceipt {
                value: Some(LazyLookup::Shadow {
                    record,
                    metadata,
                    source_link_count: node.link_count.unwrap_or(1),
                }),
                work,
            })
            }.await;
            result.map_err(|error: LazyWorkspaceError| error.with_prior_work(work).into_measured_failure())
        }, budget, cancellation, LazyWorkspaceError::Cancelled)
        .await.map_err(|failure| lazy_operation_failure(failure, std::convert::identity))
    }

    /// Resolves authored presence through the canonical exact-generation stat engine.
    async fn authored_stat_at_measured(
        &self,
        id: crate::GenerationId,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Option<WorkspaceStat>>, LazyWorkspaceError> {
        let opened = self.workspace.generation_measured(id, budget, cancellation)
            .await.map_err(measured_workspace_error)?;
        let prior = opened.work;
        match opened.value.stat_measured(path, remaining_work(prior, budget)?, cancellation).await {
            Ok(receipt) => Ok(OperationReceipt {
                value: Some(receipt.value), work: account_work(prior, receipt.work, budget)?,
            }),
            Err(failure) if matches!(failure.error, WorkspaceError::NotFound) => Ok(OperationReceipt {
                value: None, work: account_work(prior, *failure.work, budget)?,
            }),
            Err(failure) => Err(measured_workspace_error(failure).with_prior_work(prior)),
        }
    }

    /// Authenticates the complete frozen reference without consulting current lazy state.
    fn readonly_snapshot_state(
        &self,
        snapshot: LazySnapshotRef,
        require_exact_epoch: bool,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<LazyWorkspaceState, LazyWorkspaceError> {
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        let live = self.source.reference();
        if snapshot.workspace_id != self.workspace.id() || live.identity != snapshot.source.identity
            || (require_exact_epoch && live.epoch != snapshot.source.epoch) {
            return Err(LazyWorkspaceError::StaleSource);
        }
        let prospective = work.checked_add(WorkCounters {
            bytes_hashed: crate::foundation::usize_to_u64(LAZY_SNAPSHOT_DOMAIN.len()) + 136,
            ..WorkCounters::UNCHARGED
        })?;
        prospective.verify(budget)?;
        *work = prospective;
        if snapshot != LazySnapshotRef::new(
            snapshot.workspace_id, snapshot.authored_generation, snapshot.source,
            snapshot.overlay, snapshot.shadows,
        ) {
            return Err(LazyWorkspaceError::StaleSource);
        }
        Ok(LazyWorkspaceState {
            schema_version: LAZY_STATE_SCHEMA,
            revision: 0,
            workspace_id: snapshot.workspace_id,
            parent_workspace_id: None,
            fork_origin: None,
            source: snapshot.source,
            overlay: snapshot.overlay,
            shadows: snapshot.shadows,
            pending_remove: None,
        })
    }

    /// Authenticates retained immutable snapshot metadata without refreshing its parent.
    ///
    /// A changed live epoch does not invalidate original fork metadata. Reading
    /// unresolved content still requires the exact frozen epoch.
    pub async fn validate_snapshot_measured(
        &self,
        frozen: LazySnapshotRef,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<()>, LazyWorkspaceError> {
        in_heap_measured(move |budget| async move {
            let mut work = WorkCounters::UNCHARGED;
            let result = async {
                self.readonly_snapshot_state(frozen, false, &mut work, budget, cancellation)?;
                let generation = self.workspace.generation_measured(
                    frozen.authored_generation, remaining_work(work, budget)?, cancellation,
                ).await.map_err(measured_workspace_error)?;
                work = account_work(work, generation.work, budget)?;
                drop(generation.value);
                let overlay = self.store.load_lazy_overlay_measured(
                    frozen.overlay, remaining_work(work, budget)?, cancellation,
                ).await?;
                work = account_work(work, overlay.work, budget)?;
                let present = overlay.value.is_some();
                drop(overlay.value);
                if !present { return Err(LazyWorkspaceError::Store("lazy overlay is absent".to_owned())); }
                let shadow = self.store.load_lazy_shadow_measured(
                    frozen.shadows, remaining_work(work, budget)?, cancellation,
                ).await?;
                work = account_work(work, shadow.work, budget)?;
                let present = shadow.value.is_some();
                drop(shadow.value);
                if !present { return Err(LazyWorkspaceError::Store("lazy identity shadow is absent".to_owned())); }
                cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
                if self.source.reference().identity != frozen.source.identity { return Err(LazyWorkspaceError::StaleSource); }
                Ok(())
            }.await;
            result.map(|value| OperationReceipt { value, work })
                .map_err(|error: LazyWorkspaceError| error.into_measured_failure()
                    .map_with_prior_work(work, std::convert::identity))
        }, budget, cancellation, LazyWorkspaceError::Cancelled).await
    }

    /// Resolves exact frozen metadata without observing or hydrating the lazy parent.
    pub async fn lookup_at_snapshot_measured(
        &self,
        snapshot: LazySnapshotRef,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<LazyLookup>, LazyWorkspaceError> {
        in_heap_measured(move |budget| async move {
            let mut work = WorkCounters::UNCHARGED;
            let result = async {
                let state = self.readonly_snapshot_state(snapshot, false, &mut work, budget, cancellation)?;
                let resolved = self.inspect_with(path, InspectBasis {
                    state: Some(&state),
                    authored_generation: Some(snapshot.authored_generation),
                    retain_pinned_observation: true,
                    ..InspectBasis::default()
                }, remaining_work(work, budget)?, cancellation).await?;
                work = account_work(work, resolved.work, budget)?;
                cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
                if matches!(&resolved.value.lookup, LazyLookup::Source(_))
                    && self.source.reference() != snapshot.source {
                    return Err(LazyWorkspaceError::StaleSource);
                }
                Ok(resolved.value.lookup)
            }.await;
            result.map(|value| OperationReceipt { value, work })
                .map_err(|error: LazyWorkspaceError| error.into_measured_failure()
                    .map_with_prior_work(work, std::convert::identity))
        }, budget, cancellation, LazyWorkspaceError::Cancelled).await
    }

    /// Reads a complete regular file from exactly this frozen authored/source view.
    ///
    /// No observation, hydration, or state publication occurs. An unresolved
    /// native source read additionally requires the snapshot's exact source epoch.
    pub async fn read_at_snapshot_measured(
        &self,
        snapshot: LazySnapshotRef,
        path: &str,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<Bytes>, LazyWorkspaceError> {
        in_heap_measured(move |budget| async move {
            let mut work = WorkCounters::UNCHARGED;
            let result = async {
                let state = self.readonly_snapshot_state(snapshot, false, &mut work, budget, cancellation)?;
                let lookup = self.inspect_with(path, InspectBasis {
                    state: Some(&state),
                    authored_generation: Some(snapshot.authored_generation),
                    retain_pinned_observation: true,
                    ..InspectBasis::default()
                }, remaining_work(work, budget)?, cancellation).await?;
                work = account_work(work, lookup.work, budget)?;
                let length = match &lookup.value.lookup {
                    LazyLookup::Authored { stat, .. } if stat.kind == FileKind::Regular => stat.logical_bytes,
                    LazyLookup::Shadow { record, .. } if record.kind == FileKind::Regular => record_logical_bytes(*record),
                    LazyLookup::Source(node) if node.kind == SourceNodeKind::RegularFile => node.logical_bytes,
                    _ => return Err(LazyWorkspaceError::NotRegularFile),
                }.ok_or(LazyWorkspaceError::NotRegularFile)?;
                if length > maximum_bytes { return Err(LazyWorkspaceError::TooLarge); }
                let mut allocations = crate::kernel::allocation::AllocationLedger::default();
                allocations.claim_bytes(lookup.value.lookup.logical_owned_bytes(), 0, &mut work, budget)
                    .map_err(lazy_allocation_error)?;
                let source_required = matches!(&lookup.value.lookup, LazyLookup::Source(_));
                if source_required && self.source.reference() != snapshot.source {
                    return Err(LazyWorkspaceError::StaleSource);
                }
                let read = self.read_resolved_range_measured(
                    &lookup.value.lookup, lookup.value.source, Some(snapshot.authored_generation), path, 0, length,
                    directory_remaining_work(work, &allocations, budget)?, cancellation,
                ).await.map_err(|failure| nested_lazy_failure(
                    lazy_operation_failure(failure, std::convert::identity), allocations.live_bytes(),
                ))?;
                work = account_nested_with_live_memory(work, read.work, allocations.live_bytes(), budget)?;
                cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
                if source_required && self.source.reference() != snapshot.source {
                    return Err(LazyWorkspaceError::StaleSource);
                }
                Ok(read.value.bytes)
            }.await;
            result.map(|value| OperationReceipt { value, work })
                .map_err(|error: LazyWorkspaceError| error.into_measured_failure()
                    .map_with_prior_work(work, std::convert::identity))
        }, budget, cancellation, LazyWorkspaceError::Cancelled).await
    }

    /// Reads one bounded range from the original frozen view without hydrating it.
    #[allow(clippy::too_many_arguments, reason = "bounded frozen range request")]
    pub async fn read_range_at_snapshot_measured(
        &self,
        snapshot: LazySnapshotRef,
        path: &str,
        offset: u64,
        length: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<Bytes>, LazyWorkspaceError> {
        in_heap_measured(move |budget| async move {
            let mut work = WorkCounters::UNCHARGED;
            let result = async {
                let state = self.readonly_snapshot_state(snapshot, false, &mut work, budget, cancellation)?;
                let lookup = self.inspect_with(path, InspectBasis {
                    state: Some(&state),
                    authored_generation: Some(snapshot.authored_generation),
                    retain_pinned_observation: true,
                    ..InspectBasis::default()
                }, remaining_work(work, budget)?, cancellation).await?;
                work = account_work(work, lookup.work, budget)?;
                let mut allocations = crate::kernel::allocation::AllocationLedger::default();
                allocations.claim_bytes(lookup.value.lookup.logical_owned_bytes(), 0, &mut work, budget)
                    .map_err(lazy_allocation_error)?;
                let source_required = matches!(&lookup.value.lookup, LazyLookup::Source(_));
                if source_required && self.source.reference() != snapshot.source {
                    return Err(LazyWorkspaceError::StaleSource);
                }
                let read = self.read_resolved_range_measured(
                    &lookup.value.lookup, lookup.value.source, Some(snapshot.authored_generation), path, offset, length,
                    directory_remaining_work(work, &allocations, budget)?, cancellation,
                ).await.map_err(|failure| nested_lazy_failure(
                    lazy_operation_failure(failure, std::convert::identity), allocations.live_bytes(),
                ))?;
                work = account_nested_with_live_memory(work, read.work, allocations.live_bytes(), budget)?;
                cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
                if source_required && self.source.reference() != snapshot.source {
                    return Err(LazyWorkspaceError::StaleSource);
                }
                Ok(read.value.bytes)
            }.await;
            result.map(|value| OperationReceipt { value, work })
                .map_err(|error: LazyWorkspaceError| error.into_measured_failure()
                    .map_with_prior_work(work, std::convert::identity))
        }, budget, cancellation, LazyWorkspaceError::Cancelled).await
    }

    /// Reads an exact frozen symbolic-link target without following or hydrating it.
    pub async fn read_link_at_snapshot_measured(
        &self,
        snapshot: LazySnapshotRef,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<Bytes>, LazyWorkspaceError> {
        in_heap_measured(move |budget| async move {
            let mut work = WorkCounters::UNCHARGED;
            let result = async {
                let state = self.readonly_snapshot_state(snapshot, false, &mut work, budget, cancellation)?;
                let lookup = self.inspect_with(path, InspectBasis {
                    state: Some(&state),
                    authored_generation: Some(snapshot.authored_generation),
                    retain_pinned_observation: true,
                    ..InspectBasis::default()
                }, remaining_work(work, budget)?, cancellation).await?;
                work = account_work(work, lookup.work, budget)?;
                let mut allocations = crate::kernel::allocation::AllocationLedger::default();
                allocations.claim_bytes(lookup.value.lookup.logical_owned_bytes(), 0, &mut work, budget)
                    .map_err(lazy_allocation_error)?;
                let source_required = matches!(&lookup.value.lookup, LazyLookup::Source(_));
                if source_required && self.source.reference() != snapshot.source {
                    return Err(LazyWorkspaceError::StaleSource);
                }
                let target = self.read_resolved_link_measured(
                    &lookup.value.lookup, lookup.value.source, Some(snapshot.authored_generation), path,
                    directory_remaining_work(work, &allocations, budget)?, cancellation,
                ).await.map_err(|failure| nested_lazy_failure(
                    lazy_operation_failure(failure, std::convert::identity), allocations.live_bytes(),
                ))?;
                work = account_nested_with_live_memory(work, target.work, allocations.live_bytes(), budget)?;
                cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
                if source_required && self.source.reference() != snapshot.source {
                    return Err(LazyWorkspaceError::StaleSource);
                }
                Ok(target.value.bytes)
            }.await;
            result.map(|value| OperationReceipt { value, work })
                .map_err(|error: LazyWorkspaceError| error.into_measured_failure()
                    .map_with_prior_work(work, std::convert::identity))
        }, budget, cancellation, LazyWorkspaceError::Cancelled).await
    }

    /// Reads one exact range, demanding no unrelated content.
    pub async fn read_range(
        &self,
        path: &str,
        offset: u64,
        length: u64,
    ) -> Result<Bytes, LazyWorkspaceError> {
        let cancellation = CancellationToken::new();
        self.read_range_measured(path, offset, length, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| lazy_operation_failure(failure, std::convert::identity))
    }

    /// Opens the inspected regular source file for repeated reads, each of
    /// which proves `node` is still the file's current version.
    #[cfg(any(feature = "native-mount", test))]
    pub(crate) async fn open_source_file(
        &self,
        path: &str,
        source: SourceReference,
        node: SourceNode,
    ) -> Result<Box<dyn crate::demand::DemandFile>, LazyWorkspaceError> {
        self.open_source_file_measured(
            path, source, node, WorkBudget::UNBOUNDED, &CancellationToken::new(),
        ).await.map(|receipt| receipt.value)
            .map_err(|failure| lazy_operation_failure(failure, std::convert::identity))
    }

    #[cfg(any(feature = "native-mount", test))]
    pub(crate) async fn open_source_file_measured(
        &self,
        path: &str,
        source: SourceReference,
        node: SourceNode,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<Box<dyn crate::demand::DemandFile>>, LazyWorkspaceError> {
        if node.kind != SourceNodeKind::RegularFile {
            return Err(crate::OperationFailure::before_work(LazyWorkspaceError::NotRegularFile));
        }
        self.source_request_measured(path, budget, cancellation, |path, budget| async move {
            self.source.open_file(source, &path, node.version, budget, cancellation).await
        }).await.map_err(LazyWorkspaceError::into_measured_failure)
    }

    async fn read_range_measured(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<Bytes>, LazyWorkspaceError> {
        let mut work = WorkCounters::default();
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let result = async {
            let lookup = accept_directory_work(
                self.lookup_measured(path, budget, cancellation).await, &mut work, &allocations,
            )?;
            allocations.claim_bytes(lookup.lookup.logical_owned_bytes(), 0, &mut work, budget)
                .map_err(lazy_allocation_error)?;
            let receipt = self.read_resolved_range_measured(
                &lookup.lookup, lookup.source, None, path, offset, length,
                directory_remaining_work(work, &allocations, budget)?, cancellation,
            ).await.map_err(|failure| lazy_operation_failure(failure, std::convert::identity));
            let output = accept_directory_work(receipt, &mut work, &allocations)?;
            work.verify(budget)?;
            Ok(output.bytes)
        }.await;
        result.map(|value| OperationReceipt { value, work })
            .map_err(|error| crate::OperationFailure::new(error, work))
    }

    #[allow(clippy::too_many_arguments, reason = "shares the canonical range engine with frozen readonly reads")]
    async fn read_resolved_range_measured(
        &self,
        lookup: &LazyLookup,
        source: Option<SourceReference>,
        authored_generation: Option<crate::GenerationId>,
        path: &str,
        offset: u64,
        length: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<crate::kernel::OwnedSourceChunk>, LazyWorkspaceError> {
        let mut work = WorkCounters::default();
        let allocations = crate::kernel::allocation::AllocationLedger::default();
        let result = async {
            let output = match lookup {
                LazyLookup::Authored { path: authored_path, .. } => {
                    if let Some(id) = authored_generation {
                        let generation = accept_directory_work(
                            self.workspace.generation_measured(id, budget, cancellation).await
                                .map_err(measured_workspace_error),
                            &mut work, &allocations,
                        )?;
                        let receipt = generation.read_range_owned_measured(
                            authored_path, offset, length,
                            directory_remaining_work(work, &allocations, budget)?, cancellation,
                        ).await.map_err(measured_workspace_error);
                        accept_directory_work(receipt, &mut work, &allocations)?
                    } else {
                        let receipt = self.workspace.read_range_owned_measured(
                            authored_path, offset, length, budget, cancellation,
                        ).await.map_err(measured_workspace_error);
                        accept_directory_work(receipt, &mut work, &allocations)?
                    }
                }
                LazyLookup::Shadow { record, .. } => {
                    if record.kind != FileKind::Regular { return Err(LazyWorkspaceError::NotRegularFile); }
                    let file = accept_directory_work(
                        self.workspace.detached_record_measured(*record, budget, cancellation)
                            .map_err(measured_workspace_error),
                        &mut work, &allocations,
                    )?;
                    let read = file.read_range(crate::ByteRange { offset, length },
                        directory_remaining_work(work, &allocations, budget)?, cancellation,
                    ).await.map_err(|failure| lazy_operation_failure(failure, |error| workspace_error(error.into())));
                    let read = accept_directory_work(read, &mut work, &allocations)?;
                    let logical_owned_bytes = read.logical_owned_bytes();
                    crate::kernel::OwnedSourceChunk { bytes: read.bytes, logical_owned_bytes }
                }
                LazyLookup::Source(node) => {
                    if node.kind != SourceNodeKind::RegularFile { return Err(LazyWorkspaceError::NotRegularFile); }
                    let source = source.ok_or(LazyWorkspaceError::Concurrent)?;
                    let receipt = self.source_request_measured(path, budget, cancellation, |path, budget| async move {
                        self.source.read_range(source, &path, node.version, offset, length, budget, cancellation).await
                    }).await;
                    accept_directory_work(receipt, &mut work, &allocations)?
                }
            };
            work.verify(budget)?;
            Ok(output)
        }.await;
        result.map(|value| OperationReceipt { value, work })
            .map_err(|error| crate::OperationFailure::new(error, work))
    }

    #[cfg(feature = "native-mount")]
    pub(crate) async fn read_unauthored_range_owned_measured(
        &self, lookup: &LazyLookup, source: Option<SourceReference>, path: &str,
        offset: u64, length: u64, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<crate::kernel::OwnedSourceChunk>, LazyWorkspaceError> {
        self.read_resolved_range_measured(lookup, source, None, path, offset, length, budget, cancellation).await
    }

    /// Reads a complete regular file under an explicit byte bound.
    pub async fn read(&self, path: &str, maximum_bytes: u64) -> Result<Bytes, LazyWorkspaceError> {
        let stat = self.stat(path).await?;
        let length = stat
            .logical_bytes
            .ok_or(LazyWorkspaceError::NotRegularFile)?;
        if length > maximum_bytes {
            return Err(LazyWorkspaceError::TooLarge);
        }
        self.read_range(path, 0, length).await
    }

    /// Reads one exact symbolic-link target without following it.
    pub async fn read_link(&self, path: &str) -> Result<Bytes, LazyWorkspaceError> {
        let cancellation = CancellationToken::new();
        self.read_link_measured(path, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| lazy_operation_failure(failure, std::convert::identity))
    }

    async fn read_link_measured(
        &self,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<Bytes>, LazyWorkspaceError> {
        let mut work = WorkCounters::default();
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let result = async {
            let lookup = accept_directory_work(
                self.lookup_measured(path, budget, cancellation).await, &mut work, &allocations,
            )?;
            allocations.claim_bytes(lookup.lookup.logical_owned_bytes(), 0, &mut work, budget)
                .map_err(lazy_allocation_error)?;
            let receipt = self.read_resolved_link_measured(
                &lookup.lookup, lookup.source, None, path,
                directory_remaining_work(work, &allocations, budget)?, cancellation,
            ).await.map_err(|failure| lazy_operation_failure(failure, std::convert::identity));
            let output = accept_directory_work(receipt, &mut work, &allocations)?;
            work.verify(budget)?;
            Ok(output.bytes)
        }.await;
        result.map(|value| OperationReceipt { value, work })
            .map_err(|error| crate::OperationFailure::new(error, work))
    }

    async fn read_resolved_link_measured(
        &self,
        lookup: &LazyLookup,
        source: Option<SourceReference>,
        authored_generation: Option<crate::GenerationId>,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<crate::kernel::OwnedSourceChunk>, LazyWorkspaceError> {
        let mut work = WorkCounters::default();
        let allocations = crate::kernel::allocation::AllocationLedger::default();
        let result = async {
            let output = match lookup {
                LazyLookup::Authored { path: authored_path, stat } => {
                    if stat.kind != FileKind::SymbolicLink { return Err(LazyWorkspaceError::NotRegularFile); }
                    if let Some(id) = authored_generation {
                        let generation = accept_directory_work(
                            self.workspace.generation_measured(id, budget, cancellation).await
                                .map_err(measured_workspace_error),
                            &mut work, &allocations,
                        )?;
                        let receipt = generation.read_symbolic_link_owned_measured(
                            authored_path, directory_remaining_work(work, &allocations, budget)?, cancellation,
                        ).await.map_err(measured_workspace_error);
                        accept_directory_work(receipt, &mut work, &allocations)?
                    } else {
                        let receipt = self.workspace.read_symbolic_link_owned_measured(
                            authored_path, budget, cancellation,
                        ).await.map_err(measured_workspace_error);
                        accept_directory_work(receipt, &mut work, &allocations)?
                    }
                }
                LazyLookup::Shadow { record, .. } => {
                    let file = accept_directory_work(
                        self.workspace.detached_record_measured(*record, budget, cancellation)
                            .map_err(measured_workspace_error),
                        &mut work, &allocations,
                    )?;
                    let receipt = file.read_symbolic_link_owned(
                        directory_remaining_work(work, &allocations, budget)?, cancellation,
                    ).await.map_err(|failure| lazy_operation_failure(failure, |error| workspace_error(error.into())));
                    accept_directory_work(receipt, &mut work, &allocations)?
                }
                LazyLookup::Source(node) => {
                    if node.kind != SourceNodeKind::SymbolicLink { return Err(LazyWorkspaceError::NotRegularFile); }
                    let source = source.ok_or(LazyWorkspaceError::Concurrent)?;
                    let receipt = self.source_request_measured(path, budget, cancellation, |path, budget| async move {
                        self.source.read_link(source, &path, node.version, budget, cancellation).await
                    }).await;
                    accept_directory_work(receipt, &mut work, &allocations)?
                }
            };
            work.verify(budget)?;
            Ok(output)
        }.await;
        result.map(|value| OperationReceipt { value, work })
            .map_err(|error| crate::OperationFailure::new(error, work))
    }

    #[cfg(feature = "native-mount")]
    pub(crate) async fn read_unauthored_link_owned_measured(
        &self, lookup: &LazyLookup, source: Option<SourceReference>, path: &str,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<crate::kernel::OwnedSourceChunk>, LazyWorkspaceError> {
        self.read_resolved_link_measured(lookup, source, None, path, budget, cancellation).await
    }

    /// Finds a sparse boundary without demanding file contents.
    ///
    /// Authored files use their authenticated extent tree. Unresolved native
    /// sources currently expose only dense length/version facts, so their
    /// exact portable representation is data through EOF followed by a hole.
    pub async fn seek(
        &self,
        path: &str,
        offset: u64,
        target: LazySeekTarget,
    ) -> Result<Option<u64>, LazyWorkspaceError> {
        match self.lookup(path).await? {
            LazyLookup::Authored {
                path: authored_path,
                stat,
            } => {
                if stat.kind != FileKind::Regular {
                    return Err(LazyWorkspaceError::NotRegularFile);
                }
                let length = stat
                    .logical_bytes
                    .ok_or(LazyWorkspaceError::NotRegularFile)?;
                if offset >= length {
                    return Ok(None);
                }
                let plan = self
                    .workspace
                    .plan_extents(&authored_path, offset, length - offset, 65_536)
                    .await
                    .map_err(workspace_error)?;
                let found = plan.spans.into_iter().find(|span| match target {
                    LazySeekTarget::Data => span.kind != WorkspaceExtentKind::Hole,
                    LazySeekTarget::Hole => span.kind == WorkspaceExtentKind::Hole,
                });
                Ok(found
                    .map(|span| span.offset)
                    .or_else(|| (target == LazySeekTarget::Hole).then_some(length)))
            }
            LazyLookup::Shadow { record, .. } => {
                if record.kind != FileKind::Regular {
                    return Err(LazyWorkspaceError::NotRegularFile);
                }
                self.workspace
                    .detached_record(record)
                    .seek(
                        offset,
                        match target {
                            LazySeekTarget::Data => crate::kernel::ExtentSeekTarget::Data,
                            LazySeekTarget::Hole => crate::kernel::ExtentSeekTarget::Hole,
                        },
                        crate::WorkBudget::UNBOUNDED,
                        &crate::CancellationToken::new(),
                    )
                    .await
                    .map(|receipt| receipt.value).map_err(|failure| lazy_operation_failure(failure, |error| LazyWorkspaceError::Workspace(error.to_string())))
            }
            LazyLookup::Source(node) => {
                if node.kind != SourceNodeKind::RegularFile {
                    return Err(LazyWorkspaceError::NotRegularFile);
                }
                let length = node
                    .logical_bytes
                    .ok_or(LazyWorkspaceError::NotRegularFile)?;
                if offset >= length {
                    return Ok(None);
                }
                Ok(Some(match target {
                    LazySeekTarget::Data => offset,
                    LazySeekTarget::Hole => length,
                }))
            }
        }
    }

    /// Promotes exactly one demanded source node into authored copy-on-write state.
    /// Directories remain shallow and regular files are fetched in bounded ranges.
    pub async fn promote(
        &self,
        path: &str,
        maximum_bytes: u64,
        idempotency_key: IdempotencyKey,
    ) -> Result<TransactionCommit<A, O>, LazyWorkspaceError> {
        self.promote_with_permit(
            path,
            maximum_bytes,
            idempotency_key,
            crate::PublicationPermit::Unrestricted,
        )
        .await
    }

    #[allow(
        clippy::too_many_lines,
        reason = "populates every source node kind in one transaction so the pinned source fact is checked once"
    )]
    async fn populate_source_node_measured(
        &self,
        transaction: &mut crate::Transaction<A, O>,
        requested: &str,
        pinned: &PinnedSourceNode,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<WorkCounters, LazyWorkspaceError> {
        in_heap_measured(move |budget| async move { let mut work = WorkCounters::default(); let result = async { let node = &pinned.node; let source = pinned.source; match node.kind {
            SourceNodeKind::Directory => {
                let applied = transaction.create_directory_measured(requested,
                remaining_work(work, budget)?,
                cancellation,).await.map_err(measured_workspace_error)?;
                work = account_work(work, applied, budget)?;
            }
            SourceNodeKind::RegularFile => {
                work = self
                    .populate_regular_source_node(
                        transaction,
                        requested,
                        pinned,
                        maximum_bytes,
                        budget,
                        cancellation,
                    )
                    .await?;
            }
            SourceNodeKind::SymbolicLink => {
                let receipt = self.source_request_measured(requested, remaining_work(work, budget)?, cancellation, |path, budget| async move { self.source.read_link(source, &path, node.version, budget, cancellation).await }).await?;
                work = account_work(work, receipt.work, budget)?;
                let applied = transaction.create_symbolic_link_measured(requested,
                receipt.value,
                remaining_work(work, budget)?,
                cancellation,).await.map_err(measured_workspace_error)?;
                work = account_work(work, applied, budget)?;
            }
            SourceNodeKind::Fifo
            | SourceNodeKind::Socket
            | SourceNodeKind::CharacterDevice
            | SourceNodeKind::BlockDevice => {
                let kind = match node.kind {
                    SourceNodeKind::Fifo => FileKind::Fifo,
                    SourceNodeKind::Socket => FileKind::Socket,
                    SourceNodeKind::CharacterDevice => FileKind::CharacterDevice,
                    SourceNodeKind::BlockDevice => FileKind::BlockDevice,
                    _ => unreachable!(),
                };
                let applied = transaction.create_special_measured(requested,
                kind,
                node.device,
                remaining_work(work, budget)?,
                cancellation,).await.map_err(measured_workspace_error)?;
                work = account_work(work, applied, budget)?;
            }
            SourceNodeKind::Unsupported => return Err(LazyWorkspaceError::UnsupportedNode),
        }
        let applied = transaction.set_metadata_measured(requested,
        source_file_metadata(node.metadata),
        remaining_work(work, budget)?,
        cancellation,).await.map_err(measured_workspace_error)?;
        work = account_work(work, applied, budget)?;
        if node.kind != SourceNodeKind::Directory {
            let applied = transaction.preserve_file_identity_measured(requested,
            self.source_file_id_measured(node, &mut work, budget, cancellation)?,
            remaining_work(work, budget)?,
            cancellation,).await.map_err(measured_workspace_error)?;
            work = account_work(work, applied, budget)?;
        }
        Ok(work) }.await; result.map(|work| OperationReceipt { value: (), work })
            .map_err(|error: LazyWorkspaceError| error.with_prior_work(work).into_measured_failure())
        }, budget, cancellation, LazyWorkspaceError::Cancelled)
        .await.map(|receipt| receipt.work)
            .map_err(|failure| lazy_operation_failure(failure, std::convert::identity))
    }

    async fn populate_regular_source_node(
        &self,
        transaction: &mut crate::Transaction<A, O>,
        requested: &str,
        pinned: &PinnedSourceNode,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<WorkCounters, LazyWorkspaceError> {
        in_heap_measured(move |budget| async move { let mut work = WorkCounters::default(); let result = async { let node = &pinned.node; let source = pinned.source; let length = node
            .logical_bytes
            .ok_or(LazyWorkspaceError::NotRegularFile)?; if length > maximum_bytes { return Err(LazyWorkspaceError::TooLarge); } let applied = transaction.create_file_measured(requested, Bytes::new(), budget, cancellation).await.map_err(measured_workspace_error)?; work = account_work(WorkCounters::default(), applied, budget)?; let mut offset = 0_u64;
        while offset < length {
            let requested_bytes = (length - offset).min(1024 * 1024);
            let receipt = self.source_request_measured(requested, remaining_work(work, budget)?, cancellation, |path, budget| async move { self.source.read_range(source, &path, node.version, offset, requested_bytes, budget, cancellation).await }).await?;
            work = account_work(work, receipt.work, budget)?;
            let chunk = receipt.value;
            if chunk.is_empty() {
                return Err(LazyWorkspaceError::Demand(DemandError::StaleVersion));
            }
            let chunk_length =
                u64::try_from(chunk.len()).map_err(|_| LazyWorkspaceError::TooLarge)?;
            let applied = transaction.write_range_measured(requested,
            offset,
            chunk,
            remaining_work(work, budget)?,
            cancellation,).await.map_err(measured_workspace_error)?;
            work = account_work(work, applied, budget)?;
            offset = offset
                .checked_add(chunk_length)
                .ok_or(LazyWorkspaceError::TooLarge)?;
        }
        Ok(work) }.await; result.map(|work| OperationReceipt { value: (), work })
            .map_err(|error: LazyWorkspaceError| error.with_prior_work(work).into_measured_failure())
        }, budget, cancellation, LazyWorkspaceError::Cancelled)
        .await.map(|receipt| receipt.work)
            .map_err(|failure| lazy_operation_failure(failure, std::convert::identity))
    }

    async fn stage_regular_source_content(
        &self,
        transaction: &crate::Transaction<A, O>,
        requested: &str,
        pinned: &PinnedSourceNode,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<crate::StagedContent>, LazyWorkspaceError> {
        let mut work = WorkCounters::default();
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let result = async {
            let length = pinned.node.logical_bytes.ok_or(LazyWorkspaceError::NotRegularFile)?;
            let path = NamespacePath::parse_in_profile_with_ledger(
                requested, self.workspace.profile(), self.workspace.limits(),
                &mut allocations, &mut work, budget, cancellation,
            ).map_err(|error| LazyWorkspaceError::Workspace(error.to_string()))?;
            let live_bytes = allocations.live_bytes();
            let mut source = DemandBlobSource {
                provider: self.source.as_ref(),
                source: pinned.source,
                path,
                version: pinned.node.version,
                offset: 0,
                length,
                work: None,
                failure: None,
            };
            let mut remaining = remaining_work(work, budget)?;
            remaining.peak_allocation_bytes = remaining.peak_allocation_bytes
                .checked_sub(live_bytes).ok_or_else(counter_overflow)?;
            let staged = transaction.stage_content_measured(
                &mut source, length.max(1), remaining, cancellation,
            ).await;
            let value = match staged {
                Ok(receipt) => {
                    work = work.with_backend(receipt.work, live_bytes)?;
                    receipt.value
                }
                Err(failure) => {
                    work = work.with_backend(*failure.work, live_bytes)?;
                    return Err(source.failure.take().map_or_else(
                        || workspace_error(failure.error), LazyWorkspaceError::Demand,
                    ));
                }
            };
            drop(source);
            allocations.release(live_bytes)
                .map_err(|error| LazyWorkspaceError::Work(error.to_string()))?;
            work.verify(budget)?;
            if value.logical_bytes() != length {
                return Err(LazyWorkspaceError::Demand(DemandError::StaleVersion));
            }
            Ok(value)
        }.await;
        result.map(|value| OperationReceipt { value, work })
            .map_err(|error: LazyWorkspaceError| error.with_prior_work(work))
    }

    async fn commit_promotion(
        &self,
        mut transaction: crate::Transaction<A, O>,
        permit: crate::PublicationPermit,
        mut work: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<TransactionCommit<A, O>>, LazyWorkspaceError> {
        let receipt = transaction
            .commit_with_permit_measured(permit, remaining_work(work, budget)?, cancellation)
            .await
            .map_err(measured_workspace_error)?;
        work = account_work(work, receipt.work, budget)?;
        Ok(OperationReceipt {
            value: receipt.value,
            work,
        })
    }

    async fn existing_promotion(
        &self,
        mut work: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<TransactionCommit<A, O>>, LazyWorkspaceError> {
        let head = self
            .workspace
            .head_measured(remaining_work(work, budget)?, cancellation)
            .await.map_err(measured_workspace_error)?;
        work = account_work(work, head.work, budget)?;
        Ok(OperationReceipt {
            value: TransactionCommit::AlreadyCommitted(head.value),
            work,
        })
    }

    async fn commit_exactify_batch_guarded<F, Fut>(
        &self,
        mut transaction: crate::Transaction<A, O>,
        permit: crate::PublicationPermit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        before_publication: &mut F,
    ) -> Result<OperationReceipt<crate::Generation<A, O>>, LazyWorkspaceError>
    where
        F: FnMut(WorkBudget) -> Fut + Send,
        Fut: Future<Output = crate::FsResult<()>> + Send,
    {
        let receipt = transaction.commit_with_permit_measured_guarded(
            permit, budget, cancellation, before_publication,
        ).await.map_err(measured_workspace_error)?;
        match receipt.value {
            TransactionCommit::Committed(generation)
            | TransactionCommit::AlreadyCommitted(generation) => Ok(OperationReceipt {
                value: generation,
                work: receipt.work,
            }),
            TransactionCommit::Conflict { .. }
            | TransactionCommit::Fenced
            | TransactionCommit::IdempotencyConflict => Err(LazyWorkspaceError::Concurrent),
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the pending batch, its byte accounting, work, budget, and cancellation are all mutated by one flush"
    )]
    async fn flush_exactify_source_batch(
        &self,
        transaction: &mut crate::Transaction<A, O>,
        pending: &mut Vec<AuthoredMutation>,
        pending_bytes: &mut u64,
        retained_bytes: &mut u64,
        work: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<WorkCounters, LazyWorkspaceError> {
        if pending.is_empty() {
            return Ok(work);
        }
        let operations = std::mem::take(pending);
        *retained_bytes = retained_bytes.checked_sub(*pending_bytes)
            .ok_or_else(counter_overflow)?;
        *pending_bytes = 0;
        let applied = transaction.apply_authored_bulk_measured(
            operations, remaining_exactify_work(work, budget, *retained_bytes)?, cancellation,
        ).await.map_err(|mut failure| {
            match failure.work.peak_allocation_bytes.checked_add(*retained_bytes) {
                Some(peak) => {
                    failure.work.peak_allocation_bytes = peak;
                    measured_workspace_error(failure)
                }
                None => LazyWorkspaceError::from(crate::WorkError::Overflow)
                    .with_prior_work(*failure.work),
            }
        })?;
        let work = account_nested_with_live_memory(work, applied, *retained_bytes, budget)?;
        Ok(work)
    }

    /// Promotes one demanded node under an authority-side publication permit.
    pub async fn promote_with_permit(
        &self,
        path: &str,
        maximum_bytes: u64,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
    ) -> Result<TransactionCommit<A, O>, LazyWorkspaceError> {
        let cancellation = CancellationToken::new();
        self.promote_with_permit_measured(
            path,
            maximum_bytes,
            idempotency_key,
            permit,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map(|receipt| receipt.value)
    }

    async fn promote_with_permit_measured(
        &self,
        path: &str,
        maximum_bytes: u64,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<TransactionCommit<A, O>>, LazyWorkspaceError> {
        let work = LazyTreapWork::new(budget, cancellation);
        let result = async {
            let requested = work.canonical_path_owned(path, self.workspace.limits())?;
            let lookup = work.accept(self.lookup_measured(
                &requested, work.remaining()?, cancellation,
            ).await)?;
            work.accept(self.promote_resolved_with_permit_measured(
                &requested,
                lookup,
                maximum_bytes,
                idempotency_key,
                permit,
                WorkCounters::default(),
                work.remaining()?,
                cancellation,
            ).await)
        }.await;
        let spent = work.spent();
        result.map(|value| OperationReceipt { value, work: spent })
            .map_err(|error: LazyWorkspaceError| error.with_prior_work(spent))
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "carries the resolved lookup, byte bound, idempotency key, permit, work, budget, and cancellation of one promotion"
    )]
    async fn promote_resolved_with_permit_measured(
        &self,
        requested: &str,
        lookup: ResolvedLazyLookup,
        maximum_bytes: u64,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
        work: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<TransactionCommit<A, O>>, LazyWorkspaceError> {
        let parent = LazyTreapWork::new(budget, cancellation);
        *parent.work.lock().expect("lazy treap work lock poisoned") = work;
        let pinned_source = lookup.source;
        let result = async {
            work.verify(budget)?;
            let lookup_bytes = match &lookup.lookup {
                LazyLookup::Authored { path, .. } => crate::foundation::usize_to_u64(path.capacity()),
                _ => 0,
            };
            parent.adopt(lookup_bytes)?;
            parent.accept(self.validate_promotion_source_measured(
                pinned_source, parent.remaining()?, cancellation,
            ).await)?;
            let mut transaction = parent.accept(self.workspace.begin_transaction_measured(
                idempotency_key, parent.remaining()?, cancellation,
            ).await.map_err(measured_workspace_error))?;
            if let Some(directory) = parent_path(requested) {
                let applied = transaction.create_dir_all_measured(
                    directory, parent.remaining()?, cancellation,
                ).await.map_err(measured_workspace_error);
                parent.accept(applied.map(|work| OperationReceipt { value: (), work }))?;
            }
            // The application body receives this owned path and adopts it.
            parent.release(lookup_bytes)?;
            let applied = parent.accept(self.apply_resolved_promotion_measured(
                &mut transaction, requested, lookup, maximum_bytes,
                parent.remaining()?, cancellation,
            ).await)?;
            parent.accept(self.validate_promotion_source_measured(
                pinned_source, parent.remaining()?, cancellation,
            ).await)?;
            if !applied {
                return parent.accept(self.existing_promotion(
                    WorkCounters::default(), parent.remaining()?, cancellation,
                ).await);
            }
            parent.accept(self.commit_promotion(
                transaction, permit, WorkCounters::default(), parent.remaining()?, cancellation,
            ).await)
        }.await;
        let spent = parent.spent();
        result.map(|value| OperationReceipt { value, work: spent })
            .map_err(|error: LazyWorkspaceError| error.with_prior_work(spent))
    }

    // These checks fence the resolved source epoch, not external host mutation
    // atomically with the SDK's existing authority publication transaction.
    async fn validate_promotion_source_measured(
        &self,
        source: Option<SourceReference>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<()>, LazyWorkspaceError> {
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        let Some(source) = source else {
            return Ok(OperationReceipt { value: (), work: WorkCounters::default() });
        };
        if self.source.reference() != source { return Err(LazyWorkspaceError::StaleSource); }
        let state = self.state_measured(budget, cancellation).await?;
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled.with_prior_work(state.work))?;
        if state.value.source != source || self.source.reference() != source {
            return Err(LazyWorkspaceError::StaleSource.with_prior_work(state.work));
        }
        Ok(OperationReceipt { value: (), work: state.work })
    }

    async fn validate_staging_source_measured(
        &self,
        snapshot: Option<LazySnapshotRef>,
        source: Option<SourceReference>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<()>, LazyWorkspaceError> {
        let Some(snapshot) = snapshot else {
            return self.validate_promotion_source_measured(source, budget, cancellation).await;
        };
        let mut work = WorkCounters::default();
        self.readonly_snapshot_state(snapshot, true, &mut work, budget, cancellation)
            .map(|_| OperationReceipt { value: (), work })
            .map_err(|error| error.with_prior_work(work))
    }

    /// Applies one already-resolved source fact to an unpublished candidate.
    /// Callers choose the publication boundary; the source semantics stay here.
    async fn apply_resolved_promotion_measured(
        &self,
        transaction: &mut crate::Transaction<A, O>,
        requested: &str,
        lookup: ResolvedLazyLookup,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<bool>, LazyWorkspaceError> {
        self.apply_resolved_promotion_with_snapshot_measured(
            transaction, requested, lookup, maximum_bytes, None, budget, cancellation,
        ).await
    }

    async fn apply_resolved_promotion_with_snapshot_measured(
        &self,
        transaction: &mut crate::Transaction<A, O>,
        requested: &str,
        lookup: ResolvedLazyLookup,
        maximum_bytes: u64,
        snapshot: Option<LazySnapshotRef>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<bool>, LazyWorkspaceError> {
        in_heap_measured(move |budget| async move { let mut work = WorkCounters::default(); let result = async { let ResolvedLazyLookup { lookup, source } = lookup; let pinned = match lookup {
            LazyLookup::Source(node) => PinnedSourceNode {
                source: source.ok_or(LazyWorkspaceError::Concurrent)?,
                node,
            },
            LazyLookup::Authored {
                path: authored_path,
                ..
            } => {
                let paths = LazyTreapWork::new(remaining_work(work, budget)?, cancellation);
                paths.adopt(crate::foundation::usize_to_u64(authored_path.capacity()))
                    .map_err(|error| error.with_prior_work(paths.spent()))?;
                if authored_path == requested {
                    work = account_work(work, paths.spent(), budget)?;
                    return Ok(OperationReceipt { value: false, work });
                }
                let applied = transaction.hard_link_measured(
                    &authored_path, requested, paths.remaining()?, cancellation,
                ).await.map_err(measured_workspace_error);
                paths.accept(applied.map(|work| OperationReceipt { value: (), work }))
                    .map_err(|error| error.with_prior_work(paths.spent()))?;
                drop(authored_path);
                paths.release(paths.live_bytes())?;
                work = account_work(work, paths.spent(), budget)?;
                return Ok(OperationReceipt { value: true, work });
            }
            LazyLookup::Shadow { record, .. } => {
                let applied = transaction.restore_record_measured(requested,
                record,
                remaining_work(work, budget)?,
                cancellation,).await.map_err(measured_workspace_error)?;
                work = account_work(work, applied, budget)?;
                return Ok(OperationReceipt { value: true, work });
            }
        };
        let checked = self.validate_staging_source_measured(
            snapshot, Some(pinned.source), remaining_work(work, budget)?, cancellation,
        ).await?;
        work = account_work(work, checked.work, budget)?;
        let populated = self
            .populate_source_node_measured(
                transaction,
                requested,
                &pinned,
                maximum_bytes,
                remaining_work(work, budget)?,
                cancellation,
            )
            .await?;
        work = account_work(work, populated, budget)?;
        let checked = self.validate_staging_source_measured(
            snapshot, Some(pinned.source), remaining_work(work, budget)?, cancellation,
        ).await?;
        work = account_work(work, checked.work, budget)?;
        Ok(OperationReceipt { value: true, work }) }.await;
            result.map_err(|error: LazyWorkspaceError| error.with_prior_work(work).into_measured_failure())
        }, budget, cancellation, LazyWorkspaceError::Cancelled)
        .await.map_err(|failure| lazy_operation_failure(failure, std::convert::identity))
    }

    /// Promotes one source node and accepts only a durable successful outcome.
    ///
    /// Mount adapters use this boundary so a typed conflict can never advance
    /// their authored view as though promotion succeeded.
    pub async fn promote_exact(
        &self,
        path: &str,
        expected_source: FileId,
        maximum_bytes: u64,
        idempotency_key: IdempotencyKey,
    ) -> Result<crate::Generation<A, O>, LazyWorkspaceError> {
        self.promote_exact_measured(
            path, expected_source, maximum_bytes, idempotency_key,
            WorkBudget::UNBOUNDED, &CancellationToken::new(),
        ).await.map(|receipt| receipt.value)
            .map_err(|failure| lazy_operation_failure(failure, std::convert::identity))
    }

    /// Promotes the exact resolved identity through the canonical bounded transaction.
    pub async fn promote_exact_measured(
        &self,
        path: &str,
        expected_source: FileId,
        maximum_bytes: u64,
        idempotency_key: IdempotencyKey,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<crate::Generation<A, O>>, LazyWorkspaceError> {
        in_heap_measured(move |budget| async move {
            let work = LazyTreapWork::new(budget, cancellation);
            let result = async {
                let requested = work.canonical_path_owned(path, self.workspace.limits())?;
                let lookup = work.accept(self.lookup_measured(&requested, work.remaining()?, cancellation).await)?;
                let lookup_bytes = match &lookup.lookup {
                    LazyLookup::Authored { path, .. } => crate::foundation::usize_to_u64(path.capacity()),
                    _ => 0,
                };
                work.adopt(lookup_bytes)?;
                let actual = match &lookup.lookup {
                    LazyLookup::Source(node) => {
                        let mut spent = WorkCounters::default();
                        let result = self.source_file_id_measured(node, &mut spent, work.remaining()?, cancellation);
                        work.accept(result.map(|value| OperationReceipt { value, work: spent })
                            .map_err(|error| error.with_prior_work(spent)))?
                    }
                    LazyLookup::Authored { stat, .. } => stat.file_id,
                    LazyLookup::Shadow { record, .. } => record.file_id,
                };
                if actual != expected_source { return Err(LazyWorkspaceError::StaleIdentity); }
                if matches!(&lookup.lookup, LazyLookup::Authored { path, .. } if path == &requested) {
                    return work.accept(self.workspace.head_measured(work.remaining()?, cancellation)
                        .await.map_err(measured_workspace_error));
                }
                // Transfer the resolved owned path; do not observe a replacement identity.
                work.release(lookup_bytes)?;
                let committed = work.accept(self.promote_resolved_with_permit_measured(
                    &requested, lookup, maximum_bytes, idempotency_key,
                    crate::PublicationPermit::Unrestricted, WorkCounters::default(),
                    work.remaining()?, cancellation,
                ).await)?;
                match committed {
                    TransactionCommit::Committed(generation)
                    | TransactionCommit::AlreadyCommitted(generation) => Ok(generation),
                    TransactionCommit::Conflict { .. } | TransactionCommit::Fenced =>
                        Err(LazyWorkspaceError::StaleIdentity),
                    TransactionCommit::IdempotencyConflict => Err(LazyWorkspaceError::Concurrent),
                }
            }.await;
            let spent = work.spent();
            result.map(|value| OperationReceipt { value, work: spent })
                .map_err(|error: LazyWorkspaceError| error.with_prior_work(spent).into_measured_failure())
        }, budget, cancellation, LazyWorkspaceError::Cancelled).await
    }

    /// Promotes one pinned source fact into an unpublished mount candidate.
    /// The caller owns the only eventual workspace publication; promotion
    /// itself must never advance HEAD ahead of dirty mounted mutations.
    #[cfg(feature = "native-mount")]
    pub(crate) async fn stage_exact_into_checkout(
        &self,
        checkout: &mut crate::Checkout<A, O>,
        path: &str,
        expected_source: FileId,
        maximum_bytes: u64,
        cancellation: &CancellationToken,
    ) -> Result<bool, LazyWorkspaceError> {
        self.stage_exact_into_checkout_measured(
            checkout, path, expected_source, maximum_bytes, WorkBudget::UNBOUNDED, cancellation,
        ).await.map(|receipt| receipt.value)
            .map_err(|failure| lazy_operation_failure(failure, std::convert::identity))
    }

    #[cfg(feature = "native-mount")]
    pub(crate) async fn stage_exact_into_checkout_measured(
        &self,
        checkout: &mut crate::Checkout<A, O>,
        path: &str,
        expected_source: FileId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<bool>, LazyWorkspaceError> {
        self.stage_into_checkout_measured(
            checkout, None, path, expected_source, maximum_bytes, budget, cancellation,
        ).await
    }

    #[cfg(feature = "native-mount")]
    pub(crate) async fn stage_at_snapshot_into_checkout_measured(
        &self,
        checkout: &mut crate::Checkout<A, O>,
        snapshot: LazySnapshotRef,
        path: &str,
        expected_source: FileId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<bool>, LazyWorkspaceError> {
        self.stage_into_checkout_measured(
            checkout, Some(snapshot), path, expected_source, maximum_bytes, budget, cancellation,
        ).await
    }

    #[cfg(feature = "native-mount")]
    async fn stage_into_checkout_measured(
        &self,
        checkout: &mut crate::Checkout<A, O>,
        snapshot: Option<LazySnapshotRef>,
        path: &str,
        expected_source: FileId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<bool>, LazyWorkspaceError> {
        in_heap_measured(move |budget| async move {
            let work = LazyTreapWork::new(budget, cancellation);
            let result = async {
                let requested = work.canonical_path_owned(path, self.workspace.limits())?;
                let lookup = if let Some(snapshot) = snapshot {
                    let state = {
                        let mut counters = work.work.lock().expect("lazy treap work lock poisoned");
                        self.readonly_snapshot_state(snapshot, true, &mut counters, budget, cancellation)?
                    };
                    work.accept(self.inspect_with(&requested, InspectBasis {
                        state: Some(&state),
                        authored_generation: Some(snapshot.authored_generation),
                        retain_pinned_observation: true,
                        ..InspectBasis::default()
                    }, work.remaining()?, cancellation).await)?
                } else {
                    work.accept(self.lookup_measured(&requested, work.remaining()?, cancellation).await)?
                };
                let pinned_source = lookup.source;
                let lookup_bytes = match &lookup.lookup {
                    LazyLookup::Authored { path, .. } => crate::foundation::usize_to_u64(path.capacity()),
                    _ => 0,
                };
                work.adopt(lookup_bytes)?;
                work.accept(self.validate_staging_source_measured(
                    snapshot, pinned_source, work.remaining()?, cancellation,
                ).await)?;
                let actual = match &lookup.lookup {
                    LazyLookup::Source(node) => {
                        let mut spent = WorkCounters::default();
                        let result = self.source_file_id_measured(node, &mut spent, work.remaining()?, cancellation);
                        work.accept(result.map(|value| OperationReceipt { value, work: spent }))?
                    }
                    LazyLookup::Authored { stat, .. } => stat.file_id,
                    LazyLookup::Shadow { record, .. } => record.file_id,
                };
                if actual != expected_source { return Err(LazyWorkspaceError::StaleIdentity); }
                let candidate_path = {
                    let mut allocations = work.allocations.lock().expect("lazy treap allocation lock poisoned");
                    let mut counters = work.work.lock().expect("lazy treap work lock poisoned");
                    NamespacePath::parse_in_profile_with_ledger(
                        &requested, self.workspace.profile(), self.workspace.limits(),
                        &mut allocations, &mut counters, budget, cancellation,
                    ).map_err(directory_namespace_error)?
                };
                let candidate = work.accept(checkout.lookup_no_follow(&candidate_path, work.remaining()?, cancellation)
                    .await.map_err(|failure| lazy_operation_failure(failure, |error| workspace_error(error.into()))))?;
                let candidate_path_bytes = candidate_path.retained_bytes();
                drop(candidate_path);
                work.release(candidate_path_bytes)?;
                if let Some(record) = candidate.record {
                    return (record.file_id == expected_source).then_some(false)
                        .ok_or(LazyWorkspaceError::StaleIdentity);
                }
                if matches!(&lookup.lookup, LazyLookup::Authored { path, .. } if path == &requested) {
                    return Err(LazyWorkspaceError::StaleIdentity);
                }
                let existing_identity = if matches!(&lookup.lookup,
                    LazyLookup::Source(node) if node.kind != SourceNodeKind::Directory) {
                    let found = checkout.read_file_record_by_id(expected_source, work.remaining()?, cancellation).await;
                    work.accept(match found {
                        Ok(receipt) => Ok(OperationReceipt { value: Some(receipt.value), work: receipt.work }),
                        Err(failure) if matches!(failure.error, crate::FsError::NotFound) =>
                            Ok(OperationReceipt { value: None, work: *failure.work }),
                        Err(failure) => Err(lazy_operation_failure(failure, |error| workspace_error(error.into()))),
                    })?
                } else { None };
                let candidate = work.accept(checkout.private_candidate_measured(work.remaining()?, cancellation)
                    .map_err(|failure| lazy_operation_failure(failure, |error| workspace_error(error.into()))))?;
                let candidate_bytes = candidate.logical_owned_bytes();
                work.adopt(candidate_bytes)?;
                let mut transaction = crate::Transaction::for_checkout_candidate(&self.workspace, candidate);
                if let Some(parent) = parent_path(&requested) {
                    let applied = transaction.create_dir_all_measured(parent, work.remaining()?, cancellation)
                        .await.map_err(measured_workspace_error);
                    work.accept(applied.map(|work| OperationReceipt { value: (), work }))?;
                }
                let changed = if let Some(record) = existing_identity {
                    let applied = transaction.restore_record_measured(
                        &requested, record, work.remaining()?, cancellation,
                    ).await.map_err(measured_workspace_error);
                    work.accept(applied.map(|work| OperationReceipt { value: (), work }))?;
                    true
                } else {
                    work.release(lookup_bytes)?;
                    work.accept(self.apply_resolved_promotion_with_snapshot_measured(
                        &mut transaction, &requested, lookup, maximum_bytes, snapshot, work.remaining()?, cancellation,
                    ).await)?
                };
                work.accept(self.validate_staging_source_measured(
                    snapshot, pinned_source, work.remaining()?, cancellation,
                ).await)?;
                if changed {
                    let candidate = transaction.into_checkout_candidate();
                    let actual_bytes = candidate.logical_owned_bytes();
                    work.release(candidate_bytes)?;
                    work.adopt(actual_bytes)?;
                    *checkout = candidate;
                    // The caller's mounted ownership ledger receives this handle.
                    work.release(actual_bytes)?;
                } else {
                    drop(transaction);
                    work.release(candidate_bytes)?;
                }
                Ok(changed)
            }.await;
            let spent = work.spent();
            result.map(|value| OperationReceipt { value, work: spent })
                .map_err(|error: LazyWorkspaceError| error.with_prior_work(spent).into_measured_failure())
        }, budget, cancellation, LazyWorkspaceError::Cancelled).await
    }

    /// Returns one bounded page combining unresolved source entries with the
    /// authored overlay. Source-native order is followed by authored order;
    /// authored entries replace same-name source entries and tombstones hide
    /// source entries without forcing unrelated subtrees.
    pub async fn list_directory(
        &self,
        path: &str,
        cursor: Option<LazyDirectoryCursor>,
        maximum_entries: u32,
    ) -> Result<LazyDirectoryPage, LazyWorkspaceError> {
        let cancellation = CancellationToken::new();
        self.list_directory_measured(
            path,
            cursor,
            maximum_entries,
            WorkBudget::UNBOUNDED,
            &cancellation,
            None,
            None,
        )
        .await
        .map(|receipt| receipt.value)
    }

    /// Reads the same merged sparse directory against an unpublished mounted
    /// checkout candidate instead of the workspace's last committed head.
    #[cfg(feature = "native-mount")]
    ///
    /// Returns the state the page was listed against, so callers resolve its
    /// entries with [`Self::inspect_unauthored`] from the same snapshot.
    pub(crate) async fn list_directory_in_checkout(
        &self,
        checkout: &mut crate::Checkout<A, O>,
        path: &str,
        cursor: Option<LazyDirectoryCursor>,
        maximum_entries: u32,
        mounted_mask: Option<(&[String], bool)>,
    ) -> Result<(LazyDirectoryPage, LazyWorkspaceState), LazyWorkspaceError> {
        self.list_directory_in_checkout_measured(
            checkout, path, cursor, maximum_entries, mounted_mask,
            WorkBudget::UNBOUNDED, &CancellationToken::new(),
        ).await.map(|receipt| receipt.value)
            .map_err(|failure| lazy_operation_failure(failure, std::convert::identity))
    }

    #[cfg(feature = "native-mount")]
    #[allow(clippy::too_many_arguments, reason = "forwards the original mounted directory request and meter")]
    pub(crate) async fn list_directory_in_checkout_measured(
        &self,
        checkout: &mut crate::Checkout<A, O>,
        path: &str,
        cursor: Option<LazyDirectoryCursor>,
        maximum_entries: u32,
        mounted_mask: Option<(&[String], bool)>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<(LazyDirectoryPage, LazyWorkspaceState)>, LazyWorkspaceError> {
        in_heap_measured(move |budget| async move {
            let state = self.state_measured(budget, cancellation).await
                .map_err(LazyWorkspaceError::into_measured_failure)?;
            self.list_directory_at(
                &state.value, state.work, path, cursor, maximum_entries, budget,
                cancellation, Some(checkout), mounted_mask, None, None,
            ).await.map(|page| OperationReceipt { value: (page.value, state.value), work: page.work })
                .map_err(LazyWorkspaceError::into_measured_failure)
        }, budget, cancellation, LazyWorkspaceError::Cancelled).await
    }

    /// A mounted replacement must not inherit entries from an unrelated
    /// source directory at the same path.
    #[allow(clippy::too_many_arguments, reason = "shares the directory projection's actual live allocation with every nested lookup")]
    async fn mounted_directory_replaces_source(
        &self,
        path: &str,
        directory: &NamespacePath,
        source: SourceReference,
        overlay: LazyOverlayId,
        checkout: &mut crate::Checkout<A, O>,
        work: &mut WorkCounters,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<MountedDirectory, LazyWorkspaceError> {
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        let (mut future, future_bytes) = in_heap_admitted(
            (&mut *work, &mut *allocations),
            |(work, allocations), bytes| allocations
                .claim_bytes(bytes, u64::from(bytes != 0), work, budget)
                .map_err(lazy_allocation_error),
            move |(work, allocations)| async move {
            let candidate = accept_directory_work(
                checkout.lookup_no_follow(directory, directory_remaining_work(*work, allocations, budget)?, cancellation)
                    .await.map_err(|failure| lazy_operation_failure(failure, |error| LazyWorkspaceError::Workspace(error.to_string()))),
                work, allocations,
            )?;
            let tombstoned = accept_directory_work(
                self.tombstoned_measured(overlay, path, directory_remaining_work(*work, allocations, budget)?, cancellation).await,
                work, allocations,
            )?;
            let replaces_source = match candidate.record {
                None if tombstoned => return Err(LazyWorkspaceError::NotFound),
                Some(_) if tombstoned => true,
                Some(record) => {
                    let source = accept_directory_work(
                        self.source.lookup(source, directory, directory_remaining_work(*work, allocations, budget)?, cancellation)
                            .await.map_err(|failure| lazy_operation_failure(failure, LazyWorkspaceError::from)),
                        work, allocations,
                    )?;
                    match source {
                        None => true,
                        Some(node) if self.source_file_id_measured(&node, work, budget, cancellation)? == record.file_id => false,
                        Some(_) => {
                            let head = accept_directory_work(
                                self.workspace.head_measured(directory_remaining_work(*work, allocations, budget)?, cancellation)
                                    .await.map_err(measured_workspace_error),
                                work, allocations,
                            )?;
                            let committed = accept_directory_work(
                                head.lookup_paths(std::slice::from_ref(directory), directory_remaining_work(*work, allocations, budget)?, cancellation)
                                    .await.map_err(|failure| lazy_operation_failure(failure, |error| LazyWorkspaceError::Workspace(error.to_string()))),
                                work, allocations,
                            )?;
                            committed.first().copied().flatten().is_none_or(|committed| committed.file_id != record.file_id)
                        }
                    }
                }
                None => false,
            };
            Ok(MountedDirectory { replaces_source, authored: candidate.record.is_some() })
            },
        )?;
        let result = future.as_mut().await;
        drop(future);
        allocations.release(future_bytes).map_err(lazy_allocation_error)?;
        result
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "forwards the page request plus the optional mounted checkout and mask to list_directory_at"
    )]
    async fn list_directory_measured(
        &self,
        path: &str,
        cursor: Option<LazyDirectoryCursor>,
        maximum_entries: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        mounted: Option<&mut crate::Checkout<A, O>>,
        mounted_mask: Option<(&[String], bool)>,
    ) -> Result<OperationReceipt<LazyDirectoryPage>, LazyWorkspaceError> {
        let state = self.state_measured(budget, cancellation).await?;
        self.list_directory_at(
            &state.value,
            state.work,
            path,
            cursor,
            maximum_entries,
            budget,
            cancellation,
            mounted,
            mounted_mask,
            None,
            None,
        )
        .await
    }

    /// Lists one bounded page from the exact frozen authored, overlay, and source view.
    ///
    /// Unlike observation, this readonly projection never updates lazy state.
    #[allow(clippy::too_many_arguments, reason = "bounded frozen directory request")]
    pub async fn list_directory_at_snapshot_measured(
        &self,
        snapshot: LazySnapshotRef,
        path: &str,
        cursor: Option<LazyDirectoryCursor>,
        maximum_entries: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<LazyDirectoryPage>, LazyWorkspaceError> {
        in_heap_measured(move |budget| async move {
            let mut work = WorkCounters::UNCHARGED;
            let result = async {
                let source_required = cursor.as_ref().is_none_or(|cursor|
                    matches!(&cursor.phase, LazyDirectoryPhase::Source(_)));
                let state = self.readonly_snapshot_state(snapshot, source_required, &mut work, budget, cancellation)?;
                let generation = self.workspace.generation_measured(
                    snapshot.authored_generation, remaining_work(work, budget)?, cancellation,
                ).await.map_err(measured_workspace_error)?;
                work = account_work(work, generation.work, budget)?;
                let page = self.list_directory_at(
                    &state, WorkCounters::UNCHARGED, path, cursor, maximum_entries,
                    remaining_work(work, budget)?, cancellation, None, None, Some(&generation.value), Some(snapshot.id),
                ).await?;
                work = account_work(work, page.work, budget)?;
                cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
                if source_required && self.source.reference() != snapshot.source {
                    return Err(LazyWorkspaceError::StaleSource);
                }
                Ok(page.value)
            }.await;
            result.map(|value| OperationReceipt { value, work })
                .map_err(|error: LazyWorkspaceError| error.into_measured_failure()
                    .map_with_prior_work(work, std::convert::identity))
        }, budget, cancellation, LazyWorkspaceError::Cancelled).await
    }

    #[allow(clippy::too_many_lines, clippy::too_many_arguments,
        reason = "source and authored phases share one actual projection ledger and publication boundary")]
    async fn list_directory_at(
        &self, state: &LazyWorkspaceState, work: WorkCounters, path: &str,
        cursor: Option<LazyDirectoryCursor>, maximum_entries: u32, budget: WorkBudget,
        cancellation: &CancellationToken, mut mounted: Option<&mut crate::Checkout<A, O>>,
        mounted_mask: Option<(&[String], bool)>,
        authored_generation: Option<&crate::Generation<A, O>>,
        snapshot_id: Option<LazySnapshotId>,
    ) -> Result<OperationReceipt<LazyDirectoryPage>, LazyWorkspaceError> {
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled.with_prior_work(work))?;
        let (mut future, future_bytes) = in_heap_admitted(
            (work, crate::kernel::allocation::AllocationLedger::default()),
            |(work, allocations), bytes| allocations
                .claim_bytes(bytes, u64::from(bytes != 0), work, budget)
                .map_err(|error| lazy_allocation_error(error).with_prior_work(*work)),
            move |(mut work, mut allocations)| async move {
            let result = async {
                cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
                if maximum_entries == 0 { return Err(LazyWorkspaceError::InvalidPageBound); }
                if let Some(cursor) = &cursor {
                    allocations.claim_bytes(cursor.directory.retained_bytes(), 0, &mut work, budget)
                        .map_err(lazy_allocation_error)?;
                    if let LazyDirectoryPhase::Authored(Some(after)) = &cursor.phase {
                        allocations.claim_bytes(after.retained_bytes() as u64, 0, &mut work, budget)
                            .map_err(lazy_allocation_error)?;
                    }
                    if let LazyDirectoryPhase::Source(Some(source)) = &cursor.phase {
                        allocations.claim_bytes(source.retained_bytes(), 0, &mut work, budget)
                            .map_err(lazy_allocation_error)?;
                    }
                }
                let directory = NamespacePath::parse_in_profile_with_ledger(
                    path, self.workspace.profile(), self.workspace.limits(),
                    &mut allocations, &mut work, budget, cancellation,
                ).map_err(directory_namespace_error)?;
                let mut phase = if let Some(cursor) = cursor {
                    if cursor.snapshot != snapshot_id || cursor.source != state.source || cursor.overlay != state.overlay
                        || cursor.directory.depth() != directory.depth()
                        || cursor.directory.encoded_bytes() != directory.encoded_bytes() {
                        return Err(LazyWorkspaceError::StaleCursor);
                    }
                    for (held, requested) in cursor.directory.components().iter().zip(directory.components()) {
                        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
                        work.charge_items(1, &budget)?;
                        if held != requested { return Err(LazyWorkspaceError::StaleCursor); }
                    }
                    allocations.release(cursor.directory.retained_bytes()).map_err(lazy_allocation_error)?;
                    cursor.phase
                } else { LazyDirectoryPhase::Source(None) };
                if mounted_mask.is_some_and(|(_, opaque)| opaque)
                    && matches!(phase, LazyDirectoryPhase::Source(None)) {
                    phase = LazyDirectoryPhase::Authored(None);
                }
                let mut directory_authored = None;
                if matches!(phase, LazyDirectoryPhase::Source(None)) && !directory.is_root()
                    && let Some(checkout) = mounted.as_deref_mut() {
                    let candidate = self.mounted_directory_replaces_source(
                        path, &directory, state.source, state.overlay, checkout,
                        &mut work, &mut allocations, budget, cancellation,
                    ).await?;
                    directory_authored = Some(candidate.authored);
                    if candidate.replaces_source { phase = LazyDirectoryPhase::Authored(None); }
                }
                let mut source_absent = false;
                loop {
                    cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
                    match phase {
                        LazyDirectoryPhase::Source(source_cursor) => {
                            if let Some(cursor) = &source_cursor {
                                // Ownership of this continuation transfers into the provider future.
                                allocations.release(cursor.retained_bytes()).map_err(lazy_allocation_error)?;
                            }
                            let response = self.source.list_page(
                                state.source, &directory, source_cursor, maximum_entries,
                                directory_remaining_work(work, &allocations, budget)?, cancellation,
                            ).await.map_err(|failure| lazy_operation_failure(failure, LazyWorkspaceError::from));
                            let mut page = match accept_directory_work(response, &mut work, &allocations) {
                                Ok(page) => page,
                                Err(error) if matches!(error.semantic_error(),
                                    LazyWorkspaceError::Demand(DemandError::Absent | DemandError::NotDirectory)) => {
                                    source_absent = true;
                                    phase = LazyDirectoryPhase::Authored(None);
                                    continue;
                                }
                                Err(error) => return Err(error),
                            };
                            let cleanup = page.publication.cleanup_budget();
                            let projection_budget = remaining_work(cleanup, budget)?;
                            let projected = async {
                                work.admit(&cleanup, &budget)?;
                                let budget = projection_budget;
                                if page.entries.len() > maximum_entries as usize {
                                    return Err(LazyWorkspaceError::Work("source directory page exceeded its requested bound".to_owned()));
                                }
                                cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
                                let raw_bytes = page.retained_bytes();
                                allocations.claim_bytes(raw_bytes, 0, &mut work, budget).map_err(lazy_allocation_error)?;
                                let count = page.entries.len();
                                let mut slots = directory_vec::<(bool, Option<usize>)>(count, &mut allocations, &mut work, budget, cancellation)?;
                                let directory_tombstoned = accept_directory_work(
                                    self.tombstoned_measured(state.overlay, path,
                                        directory_remaining_work(work, &allocations, budget)?, cancellation).await,
                                    &mut work, &allocations,
                                )?;
                                let facts_beneath = accept_directory_work(
                                    self.overlay_has_beneath(state.overlay, path,
                                        directory_remaining_work(work, &allocations, budget)?, cancellation).await,
                                    &mut work, &allocations,
                                )?;
                                let authored_directory = match directory_authored {
                                    Some(authored) => authored,
                                    None => match mounted.as_deref_mut() {
                                        Some(checkout) if !directory.is_root() => {
                                            let record = accept_directory_work(
                                                checkout.lookup_no_follow(&directory,
                                                    directory_remaining_work(work, &allocations, budget)?, cancellation)
                                                    .await.map_err(|failure| lazy_operation_failure(failure,
                                                        |error| LazyWorkspaceError::Workspace(error.to_string()))),
                                                &mut work, &allocations,
                                            )?;
                                            record.record.is_some()
                                        }
                                        _ => true,
                                    },
                                };
                                let capacity = if authored_directory && !directory_tombstoned { count } else { 0 };
                                let mut source_paths = directory_vec::<NamespacePath>(capacity, &mut allocations, &mut work, budget, cancellation)?;
                                for entry in page.entries.iter() {
                                    cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
                                    work.charge_items(1, &budget)?;
                                    if directory_tombstoned {
                                        slots.push((true, None));
                                        continue;
                                    }
                                    let exact_encoding = matches!((self.workspace.profile(), entry.name.encoding()),
                                        (crate::model::FilesystemProfile::Portable | crate::model::FilesystemProfile::Browser, crate::kernel::NameEncoding::Utf8)
                                        | (crate::model::FilesystemProfile::Posix, crate::kernel::NameEncoding::PosixBytes)
                                        | (crate::model::FilesystemProfile::Windows, crate::kernel::NameEncoding::WindowsUtf16Le));
                                    let child = if facts_beneath || mounted_mask.is_some() || !exact_encoding {
                                        directory_child_path(path, &entry.name, &mut allocations, &mut work, budget, cancellation)?
                                    } else { None };
                                    let hidden = match (mounted_mask, child.as_deref()) {
                                        (Some((paths, _)), Some(child)) => directory_mask_contains(paths, child, &mut work, budget, cancellation)?,
                                        _ => false,
                                    };
                                    let hidden = if !hidden && facts_beneath && let Some(child) = child.as_deref() {
                                        crate::path::PortablePath::validate(child, self.workspace.limits())
                                            .map_err(|error| LazyWorkspaceError::Workspace(error.to_string()))?;
                                        let fact = accept_directory_work(
                                            self.overlay_fact_measured(state.overlay, child,
                                                directory_remaining_work(work, &allocations, budget)?, cancellation).await,
                                            &mut work, &allocations,
                                        )?;
                                        matches!(fact, Some(LazyOverlayChange::Tombstone { .. }))
                                    } else { hidden };
                                    if hidden {
                                        slots.push((true, None));
                                    } else if authored_directory {
                                        let child_path = if exact_encoding {
                                            Some(directory.child_with_ledger(&entry.name, self.workspace.limits(),
                                                &mut allocations, &mut work, budget, cancellation).map_err(directory_namespace_error)?)
                                        } else if let Some(child) = child.as_deref() {
                                            Some(NamespacePath::parse_in_profile_with_ledger(
                                                child, self.workspace.profile(), self.workspace.limits(),
                                                &mut allocations, &mut work, budget, cancellation,
                                            ).map_err(directory_namespace_error)?)
                                        } else { None };
                                        if let Some(child_path) = child_path {
                                            slots.push((false, Some(source_paths.len())));
                                            source_paths.push(child_path);
                                        } else { slots.push((false, None)); }
                                    } else { slots.push((false, None)); }
                                    if let Some(child) = child {
                                        let bytes = child.capacity() as u64;
                                        drop(child);
                                        allocations.release(bytes).map_err(lazy_allocation_error)?;
                                    }
                                }
                                let mut authored = directory_vec::<bool>(source_paths.len(), &mut allocations, &mut work, budget, cancellation)?;
                                authored.resize(source_paths.len(), false);
                                if authored_directory && !source_paths.is_empty() {
                                    let head;
                                    let generation = if mounted.is_some() { None }
                                    else if let Some(generation) = authored_generation { Some(generation) }
                                    else {
                                        head = accept_directory_work(
                                            self.workspace.head_measured(directory_remaining_work(work, &allocations, budget)?, cancellation)
                                                .await.map_err(measured_workspace_error),
                                            &mut work, &allocations,
                                        )?;
                                        Some(&head)
                                    };
                                    let maximum = (self.workspace.limits().maximum_paths_per_batch as usize).max(1);
                                    let mut offset = 0;
                                    for chunk in source_paths.chunks(maximum) {
                                        let target = authored.get_mut(offset..offset + chunk.len())
                                            .ok_or(LazyWorkspaceError::Concurrent)?;
                                        if let Some(checkout) = mounted.as_deref_mut() {
                                            let lookup = accept_directory_work(
                                                checkout.lookup_batch_no_follow(chunk,
                                                    directory_remaining_work(work, &allocations, budget)?, cancellation)
                                                    .await.map_err(|failure| lazy_operation_failure(failure,
                                                        |error| LazyWorkspaceError::Workspace(error.to_string()))),
                                                &mut work, &allocations,
                                            )?;
                                            for (slot, entry) in target.iter_mut().zip(lookup.entries) {
                                                cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
                                                work.charge_items(1, &budget)?;
                                                *slot = entry.record.is_some();
                                            }
                                        } else {
                                            let records = accept_directory_work(
                                                generation.as_ref().ok_or(LazyWorkspaceError::Concurrent)?
                                                    .lookup_paths(chunk, directory_remaining_work(work, &allocations, budget)?, cancellation)
                                                    .await.map_err(|failure| lazy_operation_failure(failure,
                                                        |error| LazyWorkspaceError::Workspace(error.to_string()))),
                                                &mut work, &allocations,
                                            )?;
                                            for (slot, record) in target.iter_mut().zip(records) {
                                                cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
                                                work.charge_items(1, &budget)?;
                                                *slot = record.is_some();
                                            }
                                        }
                                        offset += chunk.len();
                                    }
                                }
                                let path_bytes = source_paths.iter().try_fold(
                                    source_paths.capacity().checked_mul(std::mem::size_of::<NamespacePath>())
                                        .map(|bytes| bytes as u64).ok_or_else(counter_overflow)?,
                                    |bytes, path| bytes.checked_add(path.retained_bytes()).ok_or_else(counter_overflow),
                                )?;
                                drop(source_paths);
                                allocations.release(path_bytes).map_err(lazy_allocation_error)?;
                                work.charge_items(count as u64, &budget)?;
                                let visible = slots.iter().filter(|(hidden, index)| !*hidden
                                    && !index.is_some_and(|index| authored.get(index).copied().unwrap_or(false))).count();
                                let mut entries = directory_vec::<LazyDirectoryEntry>(visible, &mut allocations, &mut work, budget, cancellation)?;
                                work.charge_items(count as u64, &budget)?;
                                for (entry, (hidden, index)) in page.entries.iter().zip(slots) {
                                    if hidden || index.is_some_and(|index| authored.get(index).copied().unwrap_or(false)) { continue; }
                                    let name = directory_clone_name(&entry.name, &mut allocations, &mut work, budget, cancellation)?;
                                    entries.push(LazyDirectoryEntry {
                                        name, kind: entry.node.kind, authored: false,
                                        source: Some(entry.node), unobserved: !facts_beneath,
                                    });
                                }
                                let next = match page.next.take() {
                                    Some(next) => Some(LazyDirectoryPhase::Source(Some(next))),
                                    None if authored_directory => Some(LazyDirectoryPhase::Authored(None)),
                                    None => None,
                                }.map(|phase| LazyDirectoryCursor {
                                    source: state.source, overlay: state.overlay, directory, phase, snapshot: snapshot_id,
                                });
                                Ok(LazyDirectoryPage { entries, next })
                            }.await;
                            let outcome = match projected {
                                Ok(projected) => {
                                    let accepted = page.publication.accept(
                                        directory_remaining_work(work, &allocations, budget)?, cancellation,
                                    ).await.map_err(|failure| lazy_operation_failure(failure, LazyWorkspaceError::from));
                                    accept_directory_work(accepted, &mut work, &allocations).map(|()| projected)
                                }
                                Err(error) => Err(error),
                            };
                            if outcome.is_err() {
                                let aborted = page.publication.abort(
                                    directory_remaining_work(work, &allocations, budget)?,
                                ).await.map_err(|failure| lazy_operation_failure(failure, LazyWorkspaceError::from));
                                accept_directory_work(aborted, &mut work, &allocations)?;
                            }
                            return outcome;
                        }
                        LazyDirectoryPhase::Authored(after) => {
                            let (entries, next_name) = if let Some(checkout) = mounted.as_deref_mut() {
                                let response = checkout.list_directory(&directory, after.as_ref(), maximum_entries,
                                    directory_remaining_work(work, &allocations, budget)?, cancellation).await
                                    .map_err(|failure| lazy_operation_failure(failure, |error| match error {
                                        crate::FsError::NotFound => LazyWorkspaceError::NotFound,
                                        error => LazyWorkspaceError::Workspace(error.to_string()),
                                    }));
                                let page = match accept_directory_work(response, &mut work, &allocations) {
                                    Ok(page) => page,
                                    Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::NotFound) => {
                                        if source_absent { return Err(error); }
                                        return Ok(LazyDirectoryPage { entries: Vec::new(), next: None });
                                    }
                                    Err(error) => return Err(error),
                                };
                                let count = page.entries.len();
                                if count > maximum_entries as usize { return Err(LazyWorkspaceError::InvalidPageBound); }
                                let raw_bytes = retained_directory_page_bytes(page.entries.capacity(), page.entries.as_slice(),
                                    page.entries.iter().map(|entry| entry.name.retained_bytes()))?;
                                allocations.claim_bytes(raw_bytes, 0, &mut work, budget).map_err(lazy_allocation_error)?;
                                let next_name = if page.has_more { page.entries.last().map(|entry|
                                    directory_clone_name(&entry.name, &mut allocations, &mut work, budget, cancellation)).transpose()? } else { None };
                                let mut entries = directory_vec::<LazyDirectoryEntry>(count, &mut allocations, &mut work, budget, cancellation)?;
                                work.charge_items(count as u64, &budget)?;
                                entries.extend(page.entries.into_iter().map(|entry| LazyDirectoryEntry {
                                    name: entry.name, kind: source_kind(entry.kind), authored: true, source: None, unobserved: false,
                                }));
                                (entries, next_name)
                            } else {
                                let remaining = directory_remaining_work(work, &allocations, budget)?;
                                let response = if let Some(generation) = authored_generation {
                                    generation.list_directory_measured(path, after.as_ref(), maximum_entries, remaining, cancellation).await
                                } else {
                                    self.workspace.list_directory_measured(path, after.as_ref(), maximum_entries, remaining, cancellation).await
                                }.map_err(|failure| lazy_operation_failure(failure, |error| match error {
                                    WorkspaceError::NotFound => LazyWorkspaceError::NotFound,
                                    error => workspace_error(error),
                                }));
                                let page = match accept_directory_work(response, &mut work, &allocations) {
                                    Ok(page) => page,
                                    Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::NotFound) => {
                                        if source_absent { return Err(error); }
                                        return Ok(LazyDirectoryPage { entries: Vec::new(), next: None });
                                    }
                                    Err(error) => return Err(error),
                                };
                                let count = page.entries.len();
                                if count > maximum_entries as usize { return Err(LazyWorkspaceError::InvalidPageBound); }
                                let raw_bytes = retained_directory_page_bytes(page.entries.capacity(), page.entries.as_slice(),
                                    page.entries.iter().map(|entry| entry.name.retained_bytes()))?;
                                allocations.claim_bytes(raw_bytes, 0, &mut work, budget).map_err(lazy_allocation_error)?;
                                let next_name = if page.has_more { page.entries.last().map(|entry|
                                    directory_clone_name(&entry.name, &mut allocations, &mut work, budget, cancellation)).transpose()? } else { None };
                                let mut entries = directory_vec::<LazyDirectoryEntry>(count, &mut allocations, &mut work, budget, cancellation)?;
                                work.charge_items(count as u64, &budget)?;
                                entries.extend(page.entries.into_iter().map(authored_entry));
                                (entries, next_name)
                            };
                            cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
                            let next = next_name.map(|name| LazyDirectoryCursor {
                                source: state.source, overlay: state.overlay, directory,
                                phase: LazyDirectoryPhase::Authored(Some(name)),
                                snapshot: snapshot_id,
                            });
                            return Ok(LazyDirectoryPage { entries, next });
                        }
                    }
                }
            }.await;
            let result = result.map(|value| OperationReceipt { value, work })
                .map_err(|error: LazyWorkspaceError| error.with_prior_work(work));
            (result, allocations, work)
            },
        )?;
        let (result, mut allocations, work) = future.as_mut().await;
        drop(future);
        allocations.release(future_bytes)
            .map_err(|error| lazy_allocation_error(error).with_prior_work(work))?;
        result
    }

    /// Captures every currently visible path into an exact authored generation.
    ///
    /// Enumeration completes before promotion begins so authored overlay
    /// changes cannot invalidate source cursors. Budget exhaustion and
    /// cancellation are explicit errors; callers must never present a partial
    /// capture as exact. A retry safely continues from already promoted paths.
    pub async fn exactify(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<crate::Generation<A, O>>, LazyWorkspaceError> {
        self.exactify_with_permit(budget, cancellation, crate::PublicationPermit::Unrestricted)
            .await
    }

    /// Exactifies under the supplied operation-window or reservation permit.
    pub async fn exactify_with_permit(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        permit: crate::PublicationPermit,
    ) -> Result<OperationReceipt<crate::Generation<A, O>>, LazyWorkspaceError> {
        self.exactify_selected_with_permit("/", budget, cancellation, permit)
            .await
    }

    /// Captures one visible subtree into authored state without visiting its
    /// siblings. The selected directory and every descendant become exact;
    /// unresolved paths outside it retain their normal lazy semantics.
    pub async fn exactify_subtree(
        &self,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<crate::Generation<A, O>>, LazyWorkspaceError> {
        self.exactify_subtree_with_permit(
            path,
            budget,
            cancellation,
            crate::PublicationPermit::Unrestricted,
        )
        .await
    }

    /// Captures one visible subtree into authored state without visiting its
    /// siblings. The selected directory and every descendant become exact;
    /// unresolved paths outside it retain their normal lazy semantics.
    pub async fn exactify_subtree_with_permit(
        &self,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        permit: crate::PublicationPermit,
    ) -> Result<OperationReceipt<crate::Generation<A, O>>, LazyWorkspaceError> {
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        PortablePath::validate(path, self.workspace.limits())
            .map_err(|error| LazyWorkspaceError::Workspace(error.to_string()))?;
        let root = path;
        let selected = self.lookup_measured(&root, budget, cancellation).await?;
        if !matches!(
            selected.value.lookup,
            LazyLookup::Authored {
                stat: crate::WorkspaceStat {
                    kind: FileKind::Directory,
                    ..
                },
                ..
            } | LazyLookup::Source(SourceNode {
                kind: SourceNodeKind::Directory,
                ..
            }) | LazyLookup::Shadow {
                record: FileRecord {
                    kind: FileKind::Directory,
                    ..
                },
                ..
            }
        ) {
            return Err(LazyWorkspaceError::NotDirectory.with_prior_work(selected.work));
        }
        drop(selected.value);
        let exact = self
            .exactify_selected_with_permit(
                &root,
                remaining_work(selected.work, budget)
                    .map_err(|error| error.with_prior_work(selected.work))?,
                cancellation,
                permit,
            )
            .await.map_err(|error| error.with_prior_work(selected.work))?;
        Ok(OperationReceipt {
            value: exact.value,
            work: account_work(selected.work, exact.work, budget)
                .map_err(|error| error.with_prior_work(selected.work))?,
        })
    }

    #[allow(
        clippy::too_many_lines,
        clippy::cognitive_complexity,
        reason = "exactifies the selected subtree as one ordered traversal that batches mutations under one permit"
    )]
    async fn exactify_selected_with_permit(
        &self,
        root: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        permit: crate::PublicationPermit,
    ) -> Result<OperationReceipt<crate::Generation<A, O>>, LazyWorkspaceError> {
        self.exactify_selected_guarded(
            &self.workspace, root, None, budget, cancellation, permit,
            &mut |_| async { Ok(OperationReceipt { value: (), work: WorkCounters::default() }) },
        ).await
    }

    async fn exactify_selected_guarded<F, Fut>(
        &self,
        workspace: &Workspace<A, O>,
        root: &str,
        read_view: Option<(&Self, LazySnapshotRef)>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        permit: crate::PublicationPermit,
        before_publication: &mut F,
    ) -> Result<OperationReceipt<crate::Generation<A, O>>, LazyWorkspaceError>
    where
        F: FnMut(WorkBudget) -> Fut + Send,
        Fut: Future<Output = crate::FsResult<()>> + Send,
    {
        in_heap_measured(move |budget| async move {
        let mut work = WorkCounters::default();
        let result = async {
        cancellation
            .check()
            .map_err(|_| LazyWorkspaceError::Cancelled)?;
        let state = if let Some((origin, snapshot)) = read_view {
            origin.readonly_snapshot_state(snapshot, true, &mut work, budget, cancellation)?
        } else {
            let state = self.state_measured(budget, cancellation).await?;
            work = state.work;
            state.value
        };
        let projection = read_view.map_or(self, |(origin, _)| origin);
        let head = workspace
            .head_measured(remaining_work(work, budget)?, cancellation)
            .await.map_err(measured_workspace_error)?;
        work = account_work(work, head.work, budget)?;
        let mut pinned_generation = head.value;
        let capture = if let Some((_, snapshot)) = read_view {
            snapshot.id
        } else {
            charge_lazy_hash(
                crate::foundation::usize_to_u64(LAZY_SNAPSHOT_DOMAIN.len()) + 136, &mut work, budget,
            )?;
            LazySnapshotRef::new(self.workspace.id(), pinned_generation.id(),
                state.source, state.overlay, state.shadows).id
        };
        let mut retained_bytes = 0_u64;
        let mut directories = Vec::new();
        let mut paths = Vec::new();
        let root_key = root;
        let is_directory = if let Some((origin, snapshot)) = read_view {
            let selected = origin.inspect_with(root, InspectBasis {
                state: Some(&state), authored_generation: Some(snapshot.authored_generation),
                retain_pinned_observation: true, ..InspectBasis::default()
            }, remaining_work(work, budget)?, cancellation).await;
            let selected = match selected {
                Ok(receipt) => {
                    work = account_work(work, receipt.work, budget)?;
                    Some(receipt.value)
                }
                Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::NotFound) => {
                    work = account_work(work, error.spent_work(), budget)?;
                    None
                }
                Err(error) => return Err(error),
            };
            let Some(selected) = selected else {
                return Ok(OperationReceipt { value: pinned_generation, work });
            };
            matches!(selected.lookup,
                LazyLookup::Authored { stat: crate::WorkspaceStat { kind: FileKind::Directory, .. }, .. }
                | LazyLookup::Source(SourceNode { kind: SourceNodeKind::Directory, .. })
                | LazyLookup::Shadow { record: FileRecord { kind: FileKind::Directory, .. }, .. })
        } else { true };
        if is_directory {
            reserve_exactify_slot(&mut directories, &mut retained_bytes, &mut work, budget)?;
            let directory = clone_exactify_string(root, &mut retained_bytes, &mut work, budget)?;
            directories.push(directory);
        }
        if root != "/" {
            reserve_exactify_slot(&mut paths, &mut retained_bytes, &mut work, budget)?;
            let path = clone_exactify_string(root, &mut retained_bytes, &mut work, budget)?;
            paths.push(path);
        }
        let mut next_directory = 0_usize;
        while next_directory < directories.len() {
            cancellation
                .check()
                .map_err(|_| LazyWorkspaceError::Cancelled)?;
            let directory = directories
                .get_mut(next_directory)
                .map(std::mem::take)
                .ok_or(LazyWorkspaceError::Concurrent)?;
            next_directory += 1;
            let mut cursor: Option<LazyDirectoryCursor> = None;
            loop {
                cancellation
                    .check()
                    .map_err(|_| LazyWorkspaceError::Cancelled)?;
                if let Some(cursor) = &cursor {
                    retained_bytes = retained_bytes.checked_sub(cursor.retained_bytes()?)
                        .ok_or_else(counter_overflow)?;
                }
                let receipt = if let Some((origin, snapshot)) = read_view {
                    origin.list_directory_at_snapshot_measured(
                        snapshot, &directory, cursor, 1_024,
                        remaining_exactify_work(work, budget, retained_bytes)?, cancellation,
                    ).await.map_err(|failure| lazy_operation_failure(failure, std::convert::identity))
                } else {
                    self.list_directory_measured(&directory, cursor, 1_024,
                        remaining_exactify_work(work, budget, retained_bytes)?, cancellation, None, None).await
                }
                    .map_err(|error| exactify_error(
                        "enumerate", &directory, nested_lazy_failure(error, retained_bytes),
                    ))?;
                work = account_nested_with_live_memory(work, receipt.work, retained_bytes, budget)?;
                let page = receipt.value;
                let page_bytes = retained_directory_page_bytes(
                    page.entries.capacity(), &page.entries,
                    page.entries.iter().map(|entry| entry.name.retained_bytes()),
                )?;
                let slot_bytes = crate::foundation::usize_to_u64(page.entries.capacity()
                    .checked_mul(std::mem::size_of::<LazyDirectoryEntry>())
                    .ok_or_else(counter_overflow)?);
                let cursor_bytes = page.next.as_ref().map(LazyDirectoryCursor::retained_bytes)
                    .transpose()?.unwrap_or(0);
                retained_bytes = retained_bytes.checked_add(page_bytes)
                    .and_then(|value| value.checked_add(cursor_bytes)).ok_or_else(counter_overflow)?;
                work.peak_allocation_bytes = work.peak_allocation_bytes.max(retained_bytes);
                work.verify(budget)?;
                for entry in page.entries {
                    cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
                    work.charge_items(1, &budget)?;
                    let name_bytes = crate::foundation::usize_to_u64(entry.name.retained_bytes());
                    let path = exactify_child_path(
                        &directory, &entry.name, &mut retained_bytes, &mut work, budget,
                    )?;
                    drop(entry.name);
                    retained_bytes = retained_bytes.checked_sub(name_bytes)
                        .ok_or_else(counter_overflow)?;
                    if entry.kind == SourceNodeKind::Directory {
                        reserve_exactify_slot(
                            &mut directories,
                            &mut retained_bytes,
                            &mut work,
                            budget,
                        )?;
                        let child_directory = clone_exactify_string(
                            &path, &mut retained_bytes, &mut work, budget,
                        )?;
                        directories.push(child_directory);
                    }
                    reserve_exactify_slot(&mut paths, &mut retained_bytes, &mut work, budget)?;
                    paths.push(path);
                }
                retained_bytes = retained_bytes.checked_sub(slot_bytes).ok_or_else(counter_overflow)?;
                cursor = page.next;
                if cursor.is_none() {
                    break;
                }
            }
            retained_bytes = retained_bytes
                .checked_sub(u64::try_from(directory.capacity()).unwrap_or(u64::MAX))
                .ok_or_else(counter_overflow)?;
        }
        let directory_vector_bytes = exactify_path_vector_bytes(&directories)?;
        retained_bytes = retained_bytes
            .checked_sub(directory_vector_bytes)
            .ok_or_else(counter_overflow)?;
        drop(directories);
        let path_vector_bytes = exactify_path_vector_bytes(&paths)?;
        sort_exactify_paths(&mut paths, &mut work, budget, cancellation)?;
        let limits = self.workspace.limits();
        // A path can add an ancestor proof for every component. Keep each
        // unpublished candidate well inside the volume dependency ceiling.
        let dependency_paths = usize::try_from(limits.maximum_checkout_dependencies)
            .unwrap_or(usize::MAX)
            / (usize::from(limits.maximum_path_depth) + 4);
        let mutation_paths =
            usize::try_from(limits.maximum_mutations_per_batch).unwrap_or(usize::MAX) / 4;
        let maximum_batch_paths = dependency_paths.min(mutation_paths).max(1);
        let mut seen_sources: Vec<(FileId, String)> = Vec::new();
        seen_sources = exactify_vec(
            maximum_batch_paths.min(paths.len()), &mut retained_bytes, &mut work, budget, cancellation,
        )?;
        let seen_vector_bytes = u64::try_from(
            seen_sources
                .capacity()
                .checked_mul(std::mem::size_of::<(FileId, String)>())
                .ok_or_else(counter_overflow)?,
        )
        .map_err(|_| counter_overflow())?;
        let mut transaction = None;
        let mut batch_paths = 0_usize;
        let mut batch_dirty = false;
        let mut batch_index = 0_u64;
        let mut pending_sources = Vec::new();
        let mut pending_source_bytes = 0_u64;
        let mut authored_batch: Vec<Option<FileRecord>> = Vec::new();
        let mut authored_batch_bytes = 0_u64;
        for index in 0..paths.len() {
            cancellation
                .check()
                .map_err(|_| LazyWorkspaceError::Cancelled)?;
            if batch_paths == maximum_batch_paths {
                if batch_dirty {
                    work = self
                        .flush_exactify_source_batch(
                            transaction.as_mut().ok_or(LazyWorkspaceError::Concurrent)?,
                            &mut pending_sources,
                            &mut pending_source_bytes,
                            &mut retained_bytes,
                            work,
                            budget,
                            cancellation,
                        )
                        .await?;
                    let current = transaction.take().ok_or(LazyWorkspaceError::Concurrent)?;
                    let committed = self
                        .commit_exactify_batch_guarded(
                            current,
                            permit,
                            remaining_exactify_work(work, budget, retained_bytes)?,
                            cancellation,
                            before_publication,
                        )
                        .await.map_err(|error| nested_lazy_failure(error, retained_bytes))?;
                    work = account_nested_with_live_memory(
                        work,
                        committed.work,
                        retained_bytes,
                        budget,
                    )?;
                    pinned_generation = committed.value;
                }
                transaction = None;
                batch_dirty = false;
                batch_paths = 0;
                batch_index = batch_index.checked_add(1).ok_or_else(counter_overflow)?;
                for (_, retained) in seen_sources.drain(..) {
                    retained_bytes = retained_bytes
                        .checked_sub(u64::try_from(retained.capacity()).unwrap_or(u64::MAX))
                        .ok_or_else(counter_overflow)?;
                }
            }
            if batch_paths == 0 {
                let end = index.saturating_add(maximum_batch_paths).min(paths.len());
                retained_bytes = retained_bytes.checked_sub(authored_batch_bytes).ok_or_else(counter_overflow)?;
                drop(std::mem::take(&mut authored_batch));
                authored_batch_bytes = 0;
                let mut namespace_paths = exactify_vec(
                    end - index, &mut retained_bytes, &mut work, budget, cancellation,
                )?;
                let mut namespace_bytes = crate::foundation::usize_to_u64(namespace_paths.capacity()
                    .checked_mul(std::mem::size_of::<NamespacePath>()).ok_or_else(counter_overflow)?);
                for path in paths.get(index..end).ok_or(LazyWorkspaceError::Concurrent)? {
                    let namespace = exactify_namespace_path(
                        path, self.workspace.profile(), self.workspace.limits(),
                        &mut retained_bytes, &mut work, budget, cancellation,
                    )?;
                    namespace_bytes = namespace_bytes.checked_add(namespace.retained_bytes())
                        .ok_or_else(counter_overflow)?;
                    namespace_paths.push(namespace);
                }
                let found = pinned_generation
                    .lookup_paths(
                        &namespace_paths,
                        remaining_exactify_work(work, budget, retained_bytes)?,
                        cancellation,
                    )
                    .await.map_err(|failure| nested_lazy_failure(
                        lazy_operation_failure(failure, |error| LazyWorkspaceError::Workspace(error.to_string())),
                        retained_bytes,
                    ))?;
                work = account_nested_with_live_memory(work, found.work, retained_bytes, budget)?;
                authored_batch = found.value;
                authored_batch_bytes = crate::foundation::usize_to_u64(authored_batch.capacity()
                    .checked_mul(std::mem::size_of::<Option<FileRecord>>()).ok_or_else(counter_overflow)?);
                retained_bytes = retained_bytes.checked_add(authored_batch_bytes).ok_or_else(counter_overflow)?;
                work.peak_allocation_bytes = work.peak_allocation_bytes.max(retained_bytes);
                work.verify(budget)?;
                drop(namespace_paths);
                retained_bytes = retained_bytes.checked_sub(namespace_bytes).ok_or_else(counter_overflow)?;
            }
            let authored_absent = authored_batch
                .get(batch_paths).ok_or(LazyWorkspaceError::Concurrent)?.is_none();
            let path = std::mem::take(paths.get_mut(index).ok_or(LazyWorkspaceError::Concurrent)?);
            let receipt = if let Some((origin, snapshot)) = read_view {
                origin.inspect_with(&path, InspectBasis {
                    state: Some(&state), authored_generation: Some(snapshot.authored_generation),
                    retain_pinned_observation: true, ..InspectBasis::default()
                }, remaining_exactify_work(work, budget, retained_bytes)?, cancellation).await
            } else if authored_absent {
                self.inspect_snapshot_after_authored_absence(
                    &path,
                    remaining_exactify_work(work, budget, retained_bytes)?,
                    cancellation,
                )
                .await
            } else {
                self.inspect_snapshot_measured(
                    &path, remaining_exactify_work(work, budget, retained_bytes)?, cancellation,
                )
                    .await
            }
            .map_err(|error| exactify_error("inspect", &path, nested_lazy_failure(error, retained_bytes)))?;
            work = account_nested_with_live_memory(work, receipt.work, retained_bytes, budget)?;
            let lookup = receipt.value;
            if matches!(lookup.lookup, LazyLookup::Source(_)) {
                work.materializations = work
                    .materializations
                    .checked_add(1)
                    .ok_or_else(counter_overflow)?;
                work.verify(budget)?;
            }
            if transaction.is_none() {
                charge_lazy_hash(
                    crate::foundation::usize_to_u64(b"acyclic-fs-lazy-exactify-batch-v1\0".len())
                        + 32 + crate::foundation::usize_to_u64(root_key.len()) + 8,
                    &mut work, budget,
                )?;
                let begun = workspace
                    .begin_pinned_transaction_measured(
                        &pinned_generation,
                        exactify_batch_key(capture, root_key, batch_index),
                        remaining_exactify_work(work, budget, retained_bytes)?,
                        cancellation,
                    )
                    .await
                    .map_err(|failure| nested_lazy_failure(measured_workspace_error(failure), retained_bytes))?;
                work = account_nested_with_live_memory(work, begun.work, retained_bytes, budget)?;
                transaction = Some(begun.value);
                if let Some(parent) = parent_path(root_key) {
                    let prepared = transaction
                        .as_mut()
                        .ok_or(LazyWorkspaceError::Concurrent)?
                        .create_dir_all_measured(
                            parent,
                            remaining_exactify_work(work, budget, retained_bytes)?,
                            cancellation,
                        )
                        .await
                        .map_err(|failure| nested_lazy_failure(measured_workspace_error(failure), retained_bytes))?;
                    work = account_nested_with_live_memory(work, prepared, retained_bytes, budget)?;
                }
            }
            let source_id = match &lookup.lookup {
                LazyLookup::Source(node)
                    if node.kind == SourceNodeKind::RegularFile && node.link_count != Some(1) =>
                {
                    Some(self.source_file_id_measured(node, &mut work, budget, cancellation)?)
                }
                _ => None,
            };
            let alias = if let Some(id) = source_id {
                let mut found = None;
                for (seen, alias) in &seen_sources {
                    cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
                    work.charge_items(1, &budget)?;
                    if *seen == id {
                        found = Some(alias);
                        break;
                    }
                }
                found
            } else {
                None
            };
            if let Some(alias) = alias {
                work = self
                    .flush_exactify_source_batch(
                        transaction.as_mut().ok_or(LazyWorkspaceError::Concurrent)?,
                        &mut pending_sources,
                        &mut pending_source_bytes,
                        &mut retained_bytes,
                        work,
                        budget,
                        cancellation,
                    )
                    .await?;
                let linked = transaction
                    .as_mut()
                    .ok_or(LazyWorkspaceError::Concurrent)?.hard_link_measured(
                        alias, &path, remaining_exactify_work(work, budget, retained_bytes)?, cancellation,
                    ).await.map_err(measured_workspace_error)
                    .map_err(|error| exactify_error("link", &path, nested_lazy_failure(error, retained_bytes)))?;
                work = account_nested_with_live_memory(work, linked, retained_bytes, budget)?;
                batch_dirty = true;
                retained_bytes = retained_bytes
                    .checked_sub(u64::try_from(path.capacity()).unwrap_or(u64::MAX))
                    .ok_or_else(counter_overflow)?;
            } else if let LazyLookup::Source(node) = &lookup.lookup
                && node.kind == SourceNodeKind::RegularFile
            {
                let pinned = PinnedSourceNode {
                    source: lookup.source.ok_or(LazyWorkspaceError::Concurrent)?,
                    node: *node,
                };
                let staged = projection
                    .stage_regular_source_content(
                        transaction.as_ref().ok_or(LazyWorkspaceError::Concurrent)?,
                        &path,
                        &pinned,
                        remaining_exactify_work(work, budget, retained_bytes)?,
                        cancellation,
                    )
                    .await
                    .map_err(|error| exactify_error("stage", &path, nested_lazy_failure(error, retained_bytes)))?;
                work = account_nested_with_live_memory(work, staged.work, retained_bytes, budget)?;
                let namespace_path = exactify_namespace_path(
                    &path, self.workspace.profile(), self.workspace.limits(),
                    &mut retained_bytes, &mut work, budget, cancellation,
                )?;
                queue_exactify_mutation(
                    &mut pending_sources,
                    AuthoredMutation::CreateFileFromContent {
                        path: namespace_path,
                        content: staged.value,
                        metadata: source_file_metadata(node.metadata),
                        file_id: Some(match source_id {
                            Some(id) => id,
                            None => self.source_file_id_measured(node, &mut work, budget, cancellation)?,
                        }),
                    },
                    &mut pending_source_bytes,
                    &mut retained_bytes,
                    &mut work,
                    budget,
                )?;
                batch_dirty = true;
                if let Some(id) = source_id {
                    seen_sources.push((id, path));
                } else {
                    retained_bytes = retained_bytes
                        .checked_sub(u64::try_from(path.capacity()).unwrap_or(u64::MAX))
                        .ok_or_else(counter_overflow)?;
                }
            } else if let LazyLookup::Source(node) = &lookup.lookup
                && node.kind == SourceNodeKind::Directory
            {
                let namespace_path = exactify_namespace_path(
                    &path, self.workspace.profile(), self.workspace.limits(),
                    &mut retained_bytes, &mut work, budget, cancellation,
                )?;
                queue_exactify_mutation(
                    &mut pending_sources,
                    AuthoredMutation::CreateDirectory {
                        path: namespace_path,
                        metadata: source_file_metadata(node.metadata),
                    },
                    &mut pending_source_bytes,
                    &mut retained_bytes,
                    &mut work,
                    budget,
                )?;
                batch_dirty = true;
                retained_bytes = retained_bytes
                    .checked_sub(u64::try_from(path.capacity()).unwrap_or(u64::MAX))
                    .ok_or_else(counter_overflow)?;
            } else {
                work = self
                    .flush_exactify_source_batch(
                        transaction.as_mut().ok_or(LazyWorkspaceError::Concurrent)?,
                        &mut pending_sources,
                        &mut pending_source_bytes,
                        &mut retained_bytes,
                        work,
                        budget,
                        cancellation,
                    )
                    .await?;
                let applied = projection
                    .apply_resolved_promotion_with_snapshot_measured(
                        transaction.as_mut().ok_or(LazyWorkspaceError::Concurrent)?,
                        &path,
                        lookup,
                        budget.source_bytes_read,
                        read_view.map(|(_, snapshot)| snapshot),
                        remaining_exactify_work(work, budget, retained_bytes)?,
                        cancellation,
                    )
                    .await
                    .map_err(|error| exactify_error("promote", &path, nested_lazy_failure(error, retained_bytes)))?;
                work = account_nested_with_live_memory(work, applied.work, retained_bytes, budget)?;
                batch_dirty |= applied.value;
                if let Some(id) = source_id.filter(|_| applied.value) {
                    seen_sources.push((id, path));
                } else {
                    retained_bytes = retained_bytes
                        .checked_sub(u64::try_from(path.capacity()).unwrap_or(u64::MAX))
                        .ok_or_else(counter_overflow)?;
                }
            }
            batch_paths += 1;
        }
        if batch_dirty {
            work = self
                .flush_exactify_source_batch(
                    transaction.as_mut().ok_or(LazyWorkspaceError::Concurrent)?,
                    &mut pending_sources,
                    &mut pending_source_bytes,
                    &mut retained_bytes,
                    work,
                    budget,
                    cancellation,
                )
                .await?;
            let current = transaction.take().ok_or(LazyWorkspaceError::Concurrent)?;
            let committed = self
                .commit_exactify_batch_guarded(
                    current, permit, remaining_exactify_work(work, budget, retained_bytes)?, cancellation,
                    before_publication,
                ).await.map_err(|error| nested_lazy_failure(error, retained_bytes))?;
            work = account_nested_with_live_memory(work, committed.work, retained_bytes, budget)?;
            pinned_generation = committed.value;
        }
        for (_, retained) in seen_sources.drain(..) {
            retained_bytes = retained_bytes
                .checked_sub(u64::try_from(retained.capacity()).unwrap_or(u64::MAX))
                .ok_or_else(counter_overflow)?;
        }
        retained_bytes = retained_bytes
            .checked_sub(seen_vector_bytes)
            .ok_or_else(counter_overflow)?;
        drop(seen_sources);
        drop(paths);
        drop(transaction);
        retained_bytes = retained_bytes
            .checked_sub(path_vector_bytes)
            .ok_or_else(counter_overflow)?;
        drop(authored_batch);
        retained_bytes = retained_bytes.checked_sub(authored_batch_bytes).ok_or_else(counter_overflow)?;
        debug_assert_eq!(retained_bytes, 0);
        let head = workspace
            .head_measured(remaining_work(work, budget)?, cancellation)
            .await.map_err(measured_workspace_error)
            .map_err(|error| exactify_error("load head", "/", error))?;
        work = account_work(work, head.work, budget)?;
        if head.value.id() != pinned_generation.id() {
            return Err(LazyWorkspaceError::Concurrent);
        }
        Ok(OperationReceipt {
            value: head.value,
            work,
        })
        }.await;
        result.map_err(|error: LazyWorkspaceError| error.with_prior_work(work).into_measured_failure())
        }, budget, cancellation, LazyWorkspaceError::Cancelled).await
            .map_err(|failure| lazy_operation_failure(failure, std::convert::identity))
    }

    /// Creates or replaces one complete authored file.
    ///
    /// Authored state always wins over the source overlay, so no companion
    /// metadata mutation is needed. An inherited tombstone deliberately stays
    /// attached to the underlying source fact: deleting the authored
    /// replacement later must not resurrect the source file.
    pub async fn write(
        &self,
        path: &str,
        bytes: Bytes,
    ) -> Result<TransactionCommit<A, O>, LazyWorkspaceError> {
        let path = self.canonical_path(path)?;
        let source_identity = match self.lookup(&path).await {
            Ok(LazyLookup::Source(node)) => Some(self.source_file_id(&node)),
            Ok(LazyLookup::Authored { stat, .. }) => {
                self.promote_exact(&path, stat.file_id, u64::MAX, IdempotencyKey::new())
                    .await?;
                None
            }
            Ok(LazyLookup::Shadow { record, .. }) => {
                self.promote_exact(&path, record.file_id, u64::MAX, IdempotencyKey::new())
                    .await?;
                None
            }
            Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::NotFound) => None,
            Err(error) => return Err(error),
        };
        let mut transaction = self
            .workspace
            .begin_transaction(IdempotencyKey::new())
            .await
            .map_err(workspace_error)?;
        if let Some(parent) = parent_path(&path) {
            transaction
                .create_dir_all(parent)
                .await
                .map_err(workspace_error)?;
        }
        transaction
            .write(&path, bytes)
            .await
            .map_err(workspace_error)?;
        if let Some(file_id) = source_identity {
            transaction
                .preserve_file_identity(&path, file_id)
                .await
                .map_err(workspace_error)?;
        }
        transaction.commit().await.map_err(workspace_error)
    }

    /// Prepares mounted whiteouts and identity updates under the checkout's
    /// exact publication identity. The caller must hold the mount mutation gate
    /// through checkout seal and then call `recover_pending_remove`.
    #[cfg(feature = "native-mount")]
    pub(crate) async fn prepare_mount_removals(
        &self,
        paths: &[String],
        identity_records: &[(FileRecord, FileMetadata)],
        idempotency_key: IdempotencyKey,
    ) -> Result<(), LazyWorkspaceError> {
        self.prepare_mount_removals_measured(
            paths, identity_records, idempotency_key,
            WorkBudget::UNBOUNDED, &CancellationToken::new(),
        ).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    #[cfg(feature = "native-mount")]
    pub(crate) async fn prepare_mount_removals_measured(
        &self,
        paths: &[String],
        identity_records: &[(FileRecord, FileMetadata)],
        idempotency_key: IdempotencyKey,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<()>, LazyWorkspaceError> {
        let work = LazyTreapWork::new(budget, cancellation);
        let result = async {
        if paths.is_empty() && identity_records.is_empty() {
            cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
            return Ok(());
        }
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let state = work.accept(self.state_measured(work.remaining()?, cancellation).await)?;
            let mut overlay = state.overlay;
            let mut shadows = state.shadows;
            for path in paths {
                let path = work.canonical_path_owned(path, self.workspace.limits())?;
                let fact = work.accept(self.overlay_fact_measured(
                    state.overlay, &path, work.remaining()?, cancellation,
                ).await)?;
                let removed = match fact {
                    Some(LazyOverlayChange::Observe { node, .. }) => Some(node.version),
                    Some(LazyOverlayChange::Tombstone { removed }) => removed,
                    None => {
                        let lookup = self.source_request_measured(
                            &path, work.remaining()?, cancellation,
                            |namespace, remaining| async move {
                                self.source.lookup(
                                    state.source, &namespace, remaining, cancellation,
                                ).await
                            },
                        ).await;
                        match work.accept(lookup) {
                            Ok(node) => node.map(|node| node.version),
                            Err(error) if matches!(error.semantic_error(),
                                LazyWorkspaceError::Demand(DemandError::Absent | DemandError::NotDirectory)) => None,
                            Err(error) => return Err(error),
                        }
                    }
                };
                // The by-value treap key adopts this existing buffer.
                work.release(crate::foundation::usize_to_u64(path.capacity()))?;
                overlay = insert_immutable_treap(
                    &OverlayTreap(&self.store, Some(&work)), overlay,
                    path, LazyOverlayChange::Tombstone { removed },
                ).await?;
            }
            for (record, metadata) in identity_records {
                let _hold = work.accept(self.workspace.make_records_durable_measured(
                    std::slice::from_ref(record), work.remaining()?, cancellation,
                ).await.map_err(|failure| lazy_operation_failure(failure, workspace_error)))?;
                let encoded = work.accept(crate::kernel::encode_file_record_measured(
                    *record, work.remaining()?, cancellation,
                ).map_err(|failure| lazy_operation_failure(
                    failure, |error| LazyWorkspaceError::Store(error.to_string()),
                )))?;
                let record_bytes = crate::foundation::usize_to_u64(encoded.capacity());
                work.adopt(record_bytes)?;
                let metadata_bytes = crate::foundation::usize_to_u64(
                    std::mem::size_of::<crate::WorkspaceMetadata>(),
                );
                work.allocate(metadata_bytes)?;
                let value = ShadowValue {
                    record: encoded,
                    metadata: Box::new(crate::WorkspaceMetadata::from_engine(*metadata)),
                };
                // Transfer the owned value's claim to the canonical treap.
                work.release(record_bytes.checked_add(metadata_bytes)
                    .ok_or(crate::WorkError::Overflow)?)?;
                shadows = insert_immutable_treap(
                    &ShadowTreap(&self.store, Some(&work)), shadows, record.file_id, value,
                ).await?;
            }
            let prepared = LazyWorkspaceState {
                revision: state.revision.saturating_add(1),
                overlay,
                shadows,
                pending_remove: Some(PendingLazyRemove {
                    prior_overlay: state.overlay,
                    prior_shadows: state.shadows,
                    prepared_overlay: overlay,
                    kind: PendingLazyRemoveKind::Mounted { idempotency_key },
                }),
                ..state.clone()
            };
            if work.accept(self.store.compare_and_swap_lazy_workspace_measured(
                self.workspace.id(), state.revision, prepared,
                work.remaining()?, cancellation,
            ).await)? {
                return Ok(());
            }
        }
        Err(LazyWorkspaceError::Concurrent)
        }.await;
        let spent = work.spent();
        result.map(|value| OperationReceipt { value: value, work: spent })
            .map_err(|error| error.with_prior_work(spent).into_measured_failure())
    }

    /// Reads the durable decision for a prepared mounted overlay without
    /// finalizing it. An ambiguously acknowledged checkout must first retry
    /// its exact operation ID so its private candidate adopts the commit.
    #[cfg(feature = "native-mount")]
    pub(crate) async fn mount_removal_publication(
        &self,
    ) -> Result<Option<bool>, LazyWorkspaceError> {
        self.mount_removal_publication_measured(
            WorkBudget::UNBOUNDED, &CancellationToken::new(),
        ).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    #[cfg(feature = "native-mount")]
    pub(crate) async fn mount_removal_publication_measured(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<Option<bool>>, LazyWorkspaceError> {
        let work = LazyTreapWork::new(budget, cancellation);
        let result = async {
        let state = work.accept(self.store.load_lazy_workspace_measured(
            self.workspace.id(), work.remaining()?, cancellation,
        ).await)?.ok_or_else(|| LazyWorkspaceError::Store("lazy binding is absent".to_owned()))?;
        let Some(PendingLazyRemove {
            kind: PendingLazyRemoveKind::Mounted { idempotency_key },
            ..
        }) = state.pending_remove
        else {
            return Ok(None);
        };
        let committed = work.accept(self.workspace.operation_generation_measured(
            idempotency_key, work.remaining()?, cancellation,
        ).await.map_err(|failure| lazy_operation_failure(failure, workspace_error)))?.is_some();
        Ok(Some(committed))
        }.await;
        let spent = work.spent();
        result.map(|value| OperationReceipt { value: value, work: spent })
            .map_err(|error| error.with_prior_work(spent).into_measured_failure())
    }

    /// Resolves a prepared mounted overlay from the checkout's durable result.
    #[cfg(feature = "native-mount")]
    pub(crate) async fn finish_mount_removals(&self) -> Result<bool, LazyWorkspaceError> {
        self.finish_mount_removals_measured(
            WorkBudget::UNBOUNDED, &CancellationToken::new(),
        ).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    #[cfg(feature = "native-mount")]
    pub(crate) async fn finish_mount_removals_measured(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<bool>, LazyWorkspaceError> {
        let decision = self.mount_removal_publication_measured(budget, cancellation).await?;
        let Some(committed) = decision.value else {
            return Ok(OperationReceipt { value: false, work: decision.work });
        };
        let recovered = self.recover_pending_remove_measured(
            remaining_work(decision.work, budget).map_err(|error|
                error.with_prior_work(decision.work).into_measured_failure())?,
            cancellation,
        ).await.map_err(|failure| failure.map_with_prior_work(decision.work, |error| error))?;
        let work = decision.work.checked_add(recovered.work).map_err(|error|
            crate::OperationFailure::new(LazyWorkspaceError::from(error), decision.work))?;
        Ok(OperationReceipt { value: committed, work: work })
    }

    /// Source paths this view deleted, sorted, each with the source version
    /// it deleted. No authored generation holds these, so a merge cannot
    /// carry them; its caller applies each to the parent while the source
    /// still holds that version. Hidden paths are not listed.
    ///
    /// # Errors
    ///
    /// Returns store failures, or a work failure past `maximum` tombstones.
    pub async fn source_tombstones(
        &self,
        maximum: usize,
    ) -> Result<Vec<(String, SourceVersion)>, LazyWorkspaceError> {
        let state = self.state().await?;
        let mut pending = vec![state.overlay];
        let mut tombstones = Vec::new();
        while let Some(id) = pending.pop() {
            let Some(LazyOverlay::Node {
                path,
                change,
                left,
                right,
                ..
            }) = self
                .store
                .load_lazy_overlay(id)
                .await
                .map_err(store_error)?
            else {
                continue;
            };
            if let LazyOverlayChange::Tombstone {
                removed: Some(version),
            } = change
            {
                if tombstones.len() == maximum {
                    return Err(LazyWorkspaceError::Work(format!(
                        "more than {maximum} source tombstones"
                    )));
                }
                tombstones.push((path, version));
            }
            pending.push(left);
            pending.push(right);
        }
        tombstones.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(tombstones)
    }

    /// Removes one path without confusing authored deletion with unresolved source state.
    pub async fn remove(&self, path: &str) -> Result<(), LazyWorkspaceError> {
        self.remove_if(path, None).await
    }

    /// Hides a path this view only reads from the source and never owned
    /// (one an ancestor gained after the fork): it reads as absent, but is
    /// no deletion of this view's, so no merge carries it anywhere.
    ///
    /// # Errors
    ///
    /// Returns the same failures as [`Self::remove`].
    pub async fn hide_source_path(&self, path: &str) -> Result<(), LazyWorkspaceError> {
        self.remove_recording(path, None, Removal::Hidden).await
    }

    /// The source version the current source holds at `path`, bypassing
    /// this view's own observations; `None` when the source has no node there.
    ///
    /// # Errors
    ///
    /// Returns path, state, or source failures.
    pub async fn current_source_version(
        &self,
        path: &str,
    ) -> Result<Option<SourceVersion>, LazyWorkspaceError> {
        let path = self.namespace_path(path)?;
        let state = self.state().await?;
        self.source.lookup(state.source, &path, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value.map(|node| node.version))
            .or_else(|failure| match failure.error {
                DemandError::Absent | DemandError::NotDirectory => Ok(None),
                error => Err(LazyWorkspaceError::from(error)),
            })
    }

    /// The source version removing `path` deletes: this view's observation
    /// of it, or else the source's current node.
    async fn removed_source_version(
        &self,
        state: &LazyWorkspaceState,
        path: &str,
    ) -> Result<Option<SourceVersion>, LazyWorkspaceError> {
        match self.overlay_fact(state.overlay, path).await? {
            Some(LazyOverlayChange::Observe { node, .. }) => return Ok(Some(node.version)),
            Some(LazyOverlayChange::Tombstone { removed }) => return Ok(removed),
            None => {}
        }
        self.source.lookup(state.source, &self.namespace_path(path)?, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value.map(|node| node.version))
            .or_else(|failure| match failure.error {
                DemandError::Absent | DemandError::NotDirectory => Ok(None),
                error => Err(LazyWorkspaceError::from(error)),
            })
    }

    /// Removes one path only if it still has the caller's resolved identity.
    pub async fn remove_if(
        &self,
        path: &str,
        expected: Option<FileId>,
    ) -> Result<(), LazyWorkspaceError> {
        self.remove_recording(path, expected, Removal::Deleted)
            .await
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one removal records the overlay tombstone for every source and authored node kind"
    )]
    async fn remove_recording(
        &self,
        path: &str,
        expected: Option<FileId>,
        removal: Removal,
    ) -> Result<(), LazyWorkspaceError> {
        let path = self.canonical_path(path)?;
        let mut resolved = self.lookup(&path).await?;
        if let LazyLookup::Authored {
            path: authored_path,
            stat,
        } = &resolved
            && authored_path != &path
        {
            self.promote_exact(&path, stat.file_id, u64::MAX, IdempotencyKey::new())
                .await?;
            resolved = self.lookup(&path).await?;
        }
        let actual = self.direct_file_id_for_lookup(&resolved);
        if let Some(expected) = expected
            && actual != expected
        {
            return Err(LazyWorkspaceError::StaleIdentity);
        }
        let removal_kind = if matches!(resolved, LazyLookup::Authored { .. }) {
            PendingLazyRemoveKind::Authored {
                idempotency_key: IdempotencyKey::new(),
            }
        } else {
            PendingLazyRemoveKind::SourceOnly
        };
        let mut prepared = None;
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let state = self.state().await?;
            let shadows = if let LazyLookup::Authored { stat, .. } = &resolved {
                let record = self
                    .workspace
                    .record_by_id(stat.file_id)
                    .await
                    .map_err(workspace_error)?;
                self.insert_shadow(state.shadows, stat.file_id, record, stat.metadata)
                    .await?
            } else {
                state.shadows
            };
            let removed = match (removal, &resolved) {
                (Removal::Hidden, _) => None,
                (Removal::Deleted, LazyLookup::Source(node)) => Some(node.version),
                (Removal::Deleted, _) => self.removed_source_version(&state, &path).await?,
            };
            let tombstone_overlay = self
                .insert_overlay(
                    state.overlay,
                    path.clone(),
                    LazyOverlayChange::Tombstone { removed },
                )
                .await?;
            let pending_remove = PendingLazyRemove {
                prior_overlay: state.overlay,
                prior_shadows: state.shadows,
                prepared_overlay: tombstone_overlay,
                kind: removal_kind,
            };
            let replacement = LazyWorkspaceState {
                revision: state.revision.saturating_add(1),
                overlay: tombstone_overlay,
                shadows,
                pending_remove: Some(pending_remove),
                ..state.clone()
            };
            if self
                .store
                .compare_and_swap_lazy_workspace(
                    self.workspace.id(),
                    state.revision,
                    replacement.clone(),
                )
                .await
                .map_err(store_error)?
            {
                prepared = Some(replacement);
                break;
            }
        }
        let prepared = prepared.ok_or(LazyWorkspaceError::Concurrent)?;

        let removal = if let PendingLazyRemoveKind::Authored { idempotency_key } = removal_kind {
            let mut transaction = self
                .workspace
                .begin_transaction(idempotency_key)
                .await
                .map_err(workspace_error)?;
            match transaction.remove_if(&path, actual).await {
                Ok(()) => transaction.commit().await,
                Err(error) => Err(error),
            }
        } else {
            Ok(TransactionCommit::AlreadyCommitted(
                self.workspace.head().await.map_err(workspace_error)?,
            ))
        };
        let prior_overlay = prepared
            .pending_remove
            .as_ref()
            .ok_or(LazyWorkspaceError::Concurrent)?
            .prior_overlay;
        let (overlay, result) = match removal {
            Ok(TransactionCommit::Committed(_))
            | Ok(TransactionCommit::AlreadyCommitted(_))
            | Err(WorkspaceError::NotFound) => (prepared.overlay, Ok(())),
            Ok(TransactionCommit::Conflict { .. }) | Ok(TransactionCommit::Fenced) => {
                (prior_overlay, Err(LazyWorkspaceError::StaleIdentity))
            }
            Ok(TransactionCommit::IdempotencyConflict) => {
                (prior_overlay, Err(LazyWorkspaceError::Concurrent))
            }
            Err(error) => (prior_overlay, Err(workspace_error(error))),
        };
        let replacement = LazyWorkspaceState {
            revision: prepared.revision.saturating_add(1),
            overlay,
            shadows: if result.is_ok() {
                prepared.shadows
            } else {
                prepared
                    .pending_remove
                    .as_ref()
                    .ok_or(LazyWorkspaceError::Concurrent)?
                    .prior_shadows
            },
            pending_remove: None,
            ..prepared.clone()
        };
        if !self
            .store
            .compare_and_swap_lazy_workspace(self.workspace.id(), prepared.revision, replacement)
            .await
            .map_err(store_error)?
        {
            return Err(LazyWorkspaceError::Concurrent);
        }
        result
    }

    async fn state(&self) -> Result<LazyWorkspaceState, LazyWorkspaceError> {
        let state = self
            .store
            .load_lazy_workspace(self.workspace.id())
            .await
            .map_err(store_error)?
            .ok_or_else(|| LazyWorkspaceError::Store("lazy binding is absent".to_owned()))?;
        state.require_ready_fork()?;
        if state.schema_version != LAZY_STATE_SCHEMA
            || state.workspace_id != self.workspace.id()
            || state.source != self.source.reference()
        {
            return Err(LazyWorkspaceError::StaleSource);
        }
        if state.pending_remove.is_some() {
            return Err(LazyWorkspaceError::Concurrent);
        }
        Ok(state)
    }

    async fn state_measured(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<LazyWorkspaceState>, LazyWorkspaceError> {
        let mut work = WorkCounters::default();
        let result = async {
        let receipt = self
            .store
            .load_lazy_workspace_measured(self.workspace.id(), budget, cancellation)
            .await?;
        work = receipt.work;
        let mut state = receipt
            .value
            .ok_or_else(|| LazyWorkspaceError::Store("lazy binding is absent".to_owned()))?;
        state.require_ready_fork()?;
        if state.schema_version != LAZY_STATE_SCHEMA || state.workspace_id != self.workspace.id() {
            return Err(LazyWorkspaceError::StaleSource);
        }
        let live = self.source.reference();
        if state.source != live {
            // Every view of one shared source follows its epoch: the same
            // source, only advanced (a refresh of the physical root), is
            // adopted here rather than failing each view bound before it.
            if state.source.identity != live.identity || state.source.epoch > live.epoch {
                return Err(LazyWorkspaceError::StaleSource);
            }
            let rebound = self.rebind_source_measured(remaining_work(work, budget)?, cancellation)
                .await.map_err(|failure| lazy_operation_failure(failure, |error| error))?;
            work = account_work(work, rebound.work, budget)?;
            state = rebound.value;
        }
        if state.pending_remove.is_some() {
            return Err(LazyWorkspaceError::Concurrent);
        }
        Ok(OperationReceipt {
            value: state,
            work,
        })
        }.await;
        result.map_err(|error| error.with_prior_work(work))
    }

    async fn recover_pending_remove(&self) -> Result<(), LazyWorkspaceError> {
        self.recover_pending_remove_measured(
            WorkBudget::UNBOUNDED, &CancellationToken::new(),
        ).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    async fn recover_pending_remove_measured(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<()>, LazyWorkspaceError> {
        let work = LazyTreapWork::new(budget, cancellation);
        let result = async {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let state = work.accept(self.store.load_lazy_workspace_measured(
                self.workspace.id(), work.remaining()?, cancellation,
            ).await)?.ok_or_else(|| LazyWorkspaceError::Store("lazy binding is absent".to_owned()))?;
            if state.schema_version != LAZY_STATE_SCHEMA
                || state.workspace_id != self.workspace.id()
                || state.source.identity != self.source.reference().identity
            {
                return Err(LazyWorkspaceError::StaleSource);
            }
            let Some(pending) = state.pending_remove.as_ref() else {
                return Ok(());
            };
            let committed = match pending.kind {
                PendingLazyRemoveKind::SourceOnly => true,
                PendingLazyRemoveKind::Authored { idempotency_key }
                | PendingLazyRemoveKind::Mounted { idempotency_key } => work.accept(
                    self.workspace.operation_generation_measured(
                        idempotency_key, work.remaining()?, cancellation,
                    ).await.map_err(|failure| lazy_operation_failure(failure, workspace_error))
                )?.is_some(),
            };
            let replacement = LazyWorkspaceState {
                revision: state.revision.saturating_add(1),
                overlay: if committed {
                    pending.prepared_overlay
                } else {
                    pending.prior_overlay
                },
                shadows: if committed {
                    state.shadows
                } else {
                    pending.prior_shadows
                },
                pending_remove: None,
                ..state.clone()
            };
            if work.accept(self.store.compare_and_swap_lazy_workspace_measured(
                self.workspace.id(), state.revision, replacement,
                work.remaining()?, cancellation,
            ).await)? {
                return Ok(());
            }
        }
        Err(LazyWorkspaceError::Concurrent)
        }.await;
        let spent = work.spent();
        result.map(|value| OperationReceipt { value: value, work: spent })
            .map_err(|error| error.with_prior_work(spent).into_measured_failure())
    }

    async fn observe_measured(
        &self,
        path: &str,
        state: LazyWorkspaceState,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<(SourceReference, SourceNode)>, LazyWorkspaceError> {
        let work = LazyTreapWork::new(budget, cancellation);
        let result = async {
            let path = work.canonical_path_owned(path, self.workspace.limits())?;
            let fact = work.accept(self.overlay_fact_measured(
                state.overlay, &path, work.remaining()?, cancellation,
            ).await)?;
            match fact {
                Some(LazyOverlayChange::Observe { source, node }) if source == state.source =>
                    return Ok((source, node)),
                Some(LazyOverlayChange::Tombstone { .. }) => return Err(LazyWorkspaceError::NotFound),
                Some(LazyOverlayChange::Observe { .. }) | None => {}
            }
            // Transfer the existing allocation to the receiving operation;
            // it adopts capacity without charging another allocation/copy.
            work.release(crate::foundation::usize_to_u64(path.capacity()))?;
            work.accept(self.observe_live_measured(path, state, work.remaining()?, cancellation).await)
        }.await;
        let spent = work.spent();
        result.map(|value| OperationReceipt { value, work: spent })
            .map_err(|error| error.with_prior_work(spent))
    }

    async fn observe_live_measured(
        &self,
        path: String,
        state: LazyWorkspaceState,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<(SourceReference, SourceNode)>, LazyWorkspaceError> {
        let work = LazyTreapWork::new(budget, cancellation);
        let result = async {
            work.adopt(crate::foundation::usize_to_u64(path.capacity()))?;
            let prior_live = work.live_bytes();
            let namespace = {
                let mut allocations = work.allocations.lock().expect("lazy treap allocation lock poisoned");
                let mut counters = work.work.lock().expect("lazy treap work lock poisoned");
                NamespacePath::parse_in_profile_with_ledger(
                    &path, self.workspace.profile(), self.workspace.limits(),
                    &mut allocations, &mut counters, budget, cancellation,
                ).map_err(directory_namespace_error)?
            };
            let namespace_bytes = work.live_bytes().checked_sub(prior_live)
                .ok_or(crate::WorkError::Overflow)?;
            let node = work.accept(self.source.lookup(
                state.source, &namespace, work.remaining()?, cancellation,
            ).await.map_err(|failure| lazy_operation_failure(failure, std::convert::identity)))?;
            drop(namespace);
            work.release(namespace_bytes)?;
            let node = node.ok_or(LazyWorkspaceError::NotFound)?;
            work.release(crate::foundation::usize_to_u64(path.capacity()))?;
            work.accept(self.append_overlay_measured(
                path, LazyOverlayChange::Observe { source: state.source, node },
                work.remaining()?, cancellation,
            ).await)?;
            Ok((state.source, node))
        }.await;
        let spent = work.spent();
        result.map(|value| OperationReceipt { value, work: spent })
            .map_err(|error: LazyWorkspaceError| error.with_prior_work(spent))
    }

    async fn append_overlay_measured(
        &self,
        path: String,
        change: LazyOverlayChange,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<()>, LazyWorkspaceError> {
        let work = LazyTreapWork::new(budget, cancellation);
        let result = async {
        work.adopt(crate::foundation::usize_to_u64(path.capacity()))?;
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let state = work.accept(self.state_measured(work.remaining()?, cancellation).await)?;
            let copied_path = work.canonical_path_owned(&path, self.workspace.limits())?;
            // The treap adopts this moved key in its own allocation scope.
            work.release(crate::foundation::usize_to_u64(copied_path.capacity()))?;
            let overlay_id = insert_immutable_treap(
                &OverlayTreap(&self.store, Some(&work)), state.overlay,
                copied_path, change.clone(),
            ).await?;
            let replacement = LazyWorkspaceState {
                revision: state.revision.saturating_add(1),
                overlay: overlay_id,
                ..state.clone()
            };
            if work.accept(self.store.compare_and_swap_lazy_workspace_measured(
                self.workspace.id(), state.revision, replacement,
                work.remaining()?, cancellation,
            ).await)? {
                return Ok(());
            }
        }
        Err(LazyWorkspaceError::Concurrent)
        }.await;
        let spent = work.spent();
        result.map(|value| OperationReceipt { value, work: spent })
            .map_err(|error| error.with_prior_work(spent))
    }

    fn insert_overlay<'a>(
        &'a self,
        root: LazyOverlayId,
        path: String,
        change: LazyOverlayChange,
    ) -> LazyFuture<'a, Result<LazyOverlayId, LazyWorkspaceError>> {
        Box::pin(async move {
            insert_immutable_treap(&OverlayTreap(&self.store, None), root, path, change).await
        })
    }

    async fn overlay_fact(
        &self,
        root: LazyOverlayId,
        path: &str,
    ) -> Result<Option<LazyOverlayChange>, LazyWorkspaceError> {
        immutable_treap_value(&OverlayTreap(&self.store, None), root, &path.to_owned()).await
    }

    async fn overlay_fact_measured(
        &self,
        mut root: LazyOverlayId,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Option<LazyOverlayChange>>, LazyWorkspaceError> {
        let mut work = WorkCounters::default();
        let result = async {
        loop {
            let receipt = self
                .store
                .load_lazy_overlay_measured(root, remaining_work(work, budget)?, cancellation)
                .await?;
            work = account_work(work, receipt.work, budget)?;
            let Some(node) = receipt.value else {
                return Ok(OperationReceipt { value: None, work });
            };
            let LazyOverlay::Node {
                path: node_path,
                change,
                left,
                right,
                ..
            } = node
            else {
                return Ok(OperationReceipt { value: None, work });
            };
            match path.cmp(node_path.as_str()) {
                std::cmp::Ordering::Equal => {
                    return Ok(OperationReceipt {
                        value: Some(change),
                        work,
                    });
                }
                std::cmp::Ordering::Less => root = left,
                std::cmp::Ordering::Greater => root = right,
            }
        }
        }.await;
        result.map_err(|error| error.with_prior_work(work))
    }

    /// Whether the overlay records a fact for any path strictly beneath
    /// `directory`: such paths are exactly those after `directory/` in the
    /// overlay's order that start with it, so one descent to the first of
    /// them decides.
    async fn overlay_has_beneath(
        &self,
        mut root: LazyOverlayId,
        directory: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<bool>, LazyWorkspaceError> {
        let work = LazyTreapWork::new(budget, cancellation);
        let result = async {
            cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
            PortablePath::validate(directory, self.workspace.limits())
                .map_err(|error| LazyWorkspaceError::Workspace(error.to_string()))?;
            // Compare the borrowed directory plus its separator rather than
            // allocating a canonical copy and a second prefix.
            let prefix = || directory.bytes().chain((directory != "/").then_some(b'/'));
            let mut first_after = false;
            loop {
                let node = work.accept(self.store.load_lazy_overlay_measured(
                    root, work.remaining()?, cancellation,
                ).await)?;
                let Some(LazyOverlay::Node { path, left, right, .. }) = node else {
                    break;
                };
                if path.bytes().cmp(prefix()) != std::cmp::Ordering::Less {
                    first_after = path.strip_prefix(directory).is_some_and(|suffix|
                        directory == "/" || suffix.starts_with('/'));
                    root = left;
                } else {
                    root = right;
                }
            }
            Ok(first_after)
        }.await;
        let spent = work.spent();
        result.map(|value| OperationReceipt { value, work: spent })
            .map_err(|error: LazyWorkspaceError| error.with_prior_work(spent))
    }

    async fn shadow_record_measured(
        &self,
        mut root: LazyShadowId,
        file_id: FileId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Option<(FileRecord, crate::WorkspaceMetadata)>>, LazyWorkspaceError>
    {
        let mut work = WorkCounters::default();
        let result = async {
        loop {
            let receipt = self
                .store
                .load_lazy_shadow_measured(root, remaining_work(work, budget)?, cancellation)
                .await?;
            work = account_work(work, receipt.work, budget)?;
            let Some(node) = receipt.value else {
                return Ok(OperationReceipt { value: None, work });
            };
            let LazyShadow::Node {
                file_id: node_file_id,
                record,
                metadata,
                left,
                right,
                ..
            } = node
            else {
                return Ok(OperationReceipt { value: None, work });
            };
            match file_id.cmp(&node_file_id) {
                std::cmp::Ordering::Equal => {
                    let decoded = WorkCounters {
                        items_examined: 1,
                        ..WorkCounters::default()
                    };
                    let next = work.checked_add(decoded)?;
                    next.verify(budget)?;
                    work = next;
                    let record = crate::kernel::decode_file_record(&record)
                        .map_err(|error| LazyWorkspaceError::Store(error.to_string()))?;
                    return Ok(OperationReceipt {
                        value: Some((record, *metadata)),
                        work,
                    });
                }
                std::cmp::Ordering::Less => root = left,
                std::cmp::Ordering::Greater => root = right,
            }
        }
        }.await;
        result.map_err(|error: LazyWorkspaceError| error.with_prior_work(work))
    }

    fn insert_shadow<'a>(
        &'a self,
        root: LazyShadowId,
        file_id: FileId,
        record: FileRecord,
        metadata: crate::WorkspaceMetadata,
    ) -> LazyFuture<'a, Result<LazyShadowId, LazyWorkspaceError>> {
        Box::pin(async move {
            // A shadow outlives the checkout whose objects it names, which
            // publication need not flush: make them durable first, so no
            // durable shadow ever names content a crash could lose.
            // Held until the shadow is written, so no collection sweeps what
            // it names in between.
            let _hold = self
                .workspace
                .make_records_durable(std::slice::from_ref(&record))
                .await
                .map_err(workspace_error)?;
            insert_immutable_treap(
                &ShadowTreap(&self.store, None),
                root,
                file_id,
                ShadowValue {
                    record: crate::kernel::encode_file_record(record)
                        .map_err(|error| LazyWorkspaceError::Store(error.to_string()))?,
                    metadata: Box::new(metadata),
                },
            )
            .await
        })
    }

    async fn tombstoned_measured(
        &self,
        overlay_id: LazyOverlayId,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<bool>, LazyWorkspaceError> {
        let work = LazyTreapWork::new(budget, cancellation);
        let result = async {
            cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
            PortablePath::validate(path, self.workspace.limits())
                .map_err(|error| LazyWorkspaceError::Workspace(error.to_string()))?;
            let root = work.accept(self.overlay_fact_measured(
                overlay_id, "/", work.remaining()?, cancellation,
            ).await)?;
            if matches!(root, Some(LazyOverlayChange::Tombstone { .. })) {
                return Ok(true);
            }
            // Canonical ancestors are borrowed UTF-8 prefixes of the input;
            // no owned canonical or scratch path is needed for this search.
            for end in path.match_indices('/').map(|(index, _)| index).skip(1)
                .chain(std::iter::once(path.len())).filter(|end| *end > 1)
            {
                let fact = work.accept(self.overlay_fact_measured(
                    overlay_id, &path[..end], work.remaining()?, cancellation,
                ).await)?;
                if matches!(fact, Some(LazyOverlayChange::Tombstone { .. })) {
                    return Ok(true);
                }
            }
            Ok(false)
        }.await;
        let spent = work.spent();
        result.map(|value| OperationReceipt { value, work: spent })
            .map_err(|error: LazyWorkspaceError| error.with_prior_work(spent))
    }

    fn canonical_path(&self, path: &str) -> Result<String, LazyWorkspaceError> {
        PortablePath::parse(path, self.workspace.limits())
            .map(|path| path.as_str().to_owned())
            .map_err(|error| LazyWorkspaceError::Workspace(error.to_string()))
    }

    pub(crate) fn namespace_path(&self, path: &str) -> Result<NamespacePath, LazyWorkspaceError> {
        let portable = PortablePath::parse(path, self.workspace.limits())
            .map_err(|error| LazyWorkspaceError::Workspace(error.to_string()))?;
        NamespacePath::from_portable_in_profile(
            &portable,
            self.workspace.profile(),
            self.workspace.limits(),
        )
        .map_err(|error| LazyWorkspaceError::Workspace(error.to_string()))
    }

    /// Shares the canonical path ledger with the actual demand request. The
    /// temporary exact path stays live across the provider future, not just
    /// across parsing.
    async fn source_request_measured<T, F, Fut>(
        &self,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        request: F,
    ) -> Result<OperationReceipt<T>, LazyWorkspaceError>
    where
        F: FnOnce(NamespacePath, WorkBudget) -> Fut,
        Fut: Future<Output = crate::demand::DemandResult<T>>,
    {
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let mut work = WorkCounters::default();
        let result = async {
            let path = NamespacePath::parse_in_profile_with_ledger(
                path, self.workspace.profile(), self.workspace.limits(),
                &mut allocations, &mut work, budget, cancellation,
            ).map_err(directory_namespace_error)?;
            let live_bytes = allocations.live_bytes();
            let mut remaining = remaining_work(work, budget)?;
            remaining.peak_allocation_bytes = remaining.peak_allocation_bytes
                .checked_sub(live_bytes).ok_or_else(counter_overflow)?;
            let (value, additional) = match request(path, remaining).await {
                Ok(receipt) => (Ok(receipt.value), receipt.work),
                Err(failure) => (Err(LazyWorkspaceError::from(failure.error)), *failure.work),
            };
            work = work.with_backend(additional, live_bytes)?;
            allocations.release(live_bytes)
                .map_err(lazy_allocation_error)?;
            work.verify(budget)?;
            value
        }.await;
        result.map(|value| OperationReceipt { value, work })
            .map_err(|error: LazyWorkspaceError| error.with_prior_work(work))
    }
}

/// What a mounted checkout holds for a listed directory.
struct MountedDirectory {
    /// Whether the checkout's directory stands in for the source's.
    replaces_source: bool,
    /// Whether the checkout holds the directory at all.
    authored: bool,
}

fn store_error(error: impl std::fmt::Display) -> LazyWorkspaceError {
    LazyWorkspaceError::Store(error.to_string())
}


impl From<crate::WorkError> for LazyWorkspaceError {
    fn from(error: crate::WorkError) -> Self {
        Self::Demand(DemandError::Work(error))
    }
}

fn counter_overflow() -> LazyWorkspaceError {
    crate::WorkError::Overflow.into()
}

fn workspace_error(error: WorkspaceError) -> LazyWorkspaceError {
    match error {
        WorkspaceError::StaleIdentity => LazyWorkspaceError::StaleIdentity,
        WorkspaceError::StaleSource => LazyWorkspaceError::StaleSource,
        WorkspaceError::Cancelled(_) => LazyWorkspaceError::Cancelled,
        WorkspaceError::Work(error) => error.into(),
        error => LazyWorkspaceError::Workspace(error.to_string()),
    }
}

fn measured_workspace_error(failure: crate::OperationFailure<WorkspaceError>) -> LazyWorkspaceError {
    lazy_operation_failure(failure, workspace_error)
}

fn lazy_operation_failure<E>(
    failure: crate::OperationFailure<E>,
    map: impl FnOnce(E) -> LazyWorkspaceError,
) -> LazyWorkspaceError {
    let failure = failure.map_with_prior_work(WorkCounters::default(), map);
    if *failure.work == WorkCounters::default() {
        failure.error
    } else {
        LazyWorkspaceError::SpentWork(Box::new(failure))
    }
}

fn source_kind(kind: crate::kernel::FileKind) -> SourceNodeKind {
    match kind {
        crate::kernel::FileKind::Directory => SourceNodeKind::Directory,
        crate::kernel::FileKind::Regular => SourceNodeKind::RegularFile,
        crate::kernel::FileKind::SymbolicLink => SourceNodeKind::SymbolicLink,
        crate::kernel::FileKind::Fifo => SourceNodeKind::Fifo,
        crate::kernel::FileKind::Socket => SourceNodeKind::Socket,
        crate::kernel::FileKind::CharacterDevice => SourceNodeKind::CharacterDevice,
        crate::kernel::FileKind::BlockDevice => SourceNodeKind::BlockDevice,
        crate::kernel::FileKind::ReparsePoint | crate::kernel::FileKind::MountBoundary => {
            SourceNodeKind::Unsupported
        }
    }
}

fn record_logical_bytes(record: FileRecord) -> Option<u64> {
    match record.payload {
        FilePayload::InlineRegular(bytes) => u64::try_from(bytes.as_bytes().len()).ok(),
        FilePayload::Regular { logical_bytes, .. }
        | FilePayload::SymbolicLink {
            target_bytes: logical_bytes,
            ..
        }
        | FilePayload::ReparsePoint {
            payload_bytes: logical_bytes,
            ..
        } => Some(logical_bytes),
        FilePayload::Directory { .. } | FilePayload::Empty | FilePayload::Device { .. } => None,
    }
}

fn source_file_metadata(source: crate::demand::SourceMetadata) -> FileMetadata {
    let time = |value: Option<i128>| {
        value
            .and_then(|value| i64::try_from(value).ok())
            .map_or(MetadataField::Unavailable, MetadataField::Value)
    };
    FileMetadata {
        posix_mode: metadata_u32(source.posix_mode),
        posix_uid: metadata_u32(source.posix_uid),
        posix_gid: metadata_u32(source.posix_gid),
        posix_flags: metadata_u64(source.posix_flags),
        windows_attributes: metadata_u32(source.windows_attributes),
        created_ns: time(source.created_ns),
        modified_ns: time(source.modified_ns),
        accessed_ns: time(source.accessed_ns),
        changed_ns: time(source.changed_ns),
        ..FileMetadata::default()
    }
}

fn metadata_u32(value: Option<u32>) -> MetadataField<u32> {
    value.map_or(MetadataField::Unavailable, MetadataField::Value)
}

fn metadata_u64(value: Option<u64>) -> MetadataField<u64> {
    value.map_or(MetadataField::Unavailable, MetadataField::Value)
}

fn path_priority(path: &str) -> u64 {
    let digest = blake3::hash(path.as_bytes());
    let mut bytes = [0_u8; 8];
    if let Some(prefix) = digest.as_bytes().get(..8) {
        bytes.copy_from_slice(prefix);
    }
    u64::from_be_bytes(bytes)
}

fn shadow_priority(file_id: FileId) -> u64 {
    let digest = blake3::hash(&file_id.into_bytes());
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest.as_bytes()[..8]);
    u64::from_le_bytes(bytes)
}

fn parent_path(path: &str) -> Option<&str> {
    let index = path.rfind('/')?;
    if index == 0 {
        Some("/")
    } else {
        path.get(..index)
    }
}

fn authored_entry(entry: WorkspaceDirectoryEntry) -> LazyDirectoryEntry {
    LazyDirectoryEntry {
        name: entry.name,
        kind: source_kind(entry.kind),
        authored: true,
        source: None,
        unobserved: false,
    }
}

/// Joins `name`, decoded by its declared encoding, onto a portable parent path.
pub(crate) fn logical_child_path(parent: &str, name: &LogicalName) -> Option<String> {
    let component = name.unicode_text()?;
    Some(if parent == "/" {
        format!("/{component}")
    } else {
        format!("{parent}/{component}")
    })
}

fn exactify_path_order(left: &str, right: &str) -> std::cmp::Ordering {
    left.matches('/')
        .count()
        .cmp(&right.matches('/').count())
        .then_with(|| left.cmp(right))
}

fn sort_exactify_paths(
    values: &mut [String],
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), LazyWorkspaceError> {
    fn compare(
        left: &str, right: &str, work: &mut WorkCounters,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> Result<std::cmp::Ordering, LazyWorkspaceError> {
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        work.charge_items(1, &budget)?;
        Ok(exactify_path_order(left, right))
    }
    fn swap(
        values: &mut [String], left: usize, right: usize,
        work: &mut WorkCounters, budget: WorkBudget,
    ) -> Result<(), LazyWorkspaceError> {
        charge_lazy_copy(crate::foundation::usize_to_u64(
            2 * std::mem::size_of::<String>(),
        ), work, budget)?;
        values.swap(left, right);
        Ok(())
    }
    fn sift(
        values: &mut [String], mut root: usize, end: usize,
        work: &mut WorkCounters, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> Result<(), LazyWorkspaceError> {
        while root < end / 2 {
            let mut child = root * 2 + 1;
            if child + 1 < end
                && compare(&values[child], &values[child + 1], work, budget, cancellation)?.is_lt() {
                child += 1;
            }
            if !compare(&values[root], &values[child], work, budget, cancellation)?.is_lt() {
                break;
            }
            swap(values, root, child, work, budget)?;
            root = child;
        }
        Ok(())
    }
    cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
    let length = values.len();
    for root in (0..length / 2).rev() {
        sift(values, root, length, work, budget, cancellation)?;
    }
    for end in (1..length).rev() {
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        swap(values, 0, end, work, budget)?;
        sift(values, 0, end, work, budget, cancellation)?;
    }
    Ok(())
}

fn account_work(
    current: WorkCounters,
    additional: WorkCounters,
    budget: WorkBudget,
) -> Result<WorkCounters, LazyWorkspaceError> {
    let combined = current.checked_add(additional)?;
    combined.verify(budget).map_err(|error|
        LazyWorkspaceError::from(error).with_prior_work(additional))?;
    Ok(combined)
}

fn account_nested_with_live_memory(
    current: WorkCounters,
    nested: WorkCounters,
    live_bytes: u64,
    budget: WorkBudget,
) -> Result<WorkCounters, LazyWorkspaceError> {
    let combined = current.with_backend(nested, live_bytes)?;
    combined.verify(budget).map_err(|error| {
        let mut spent = nested;
        spent.peak_allocation_bytes = combined.peak_allocation_bytes;
        LazyWorkspaceError::from(error).with_prior_work(spent)
    })?;
    Ok(combined)
}

fn queue_exactify_mutation(
    pending: &mut Vec<AuthoredMutation>,
    mutation: AuthoredMutation,
    pending_bytes: &mut u64,
    retained_bytes: &mut u64,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<(), LazyWorkspaceError> {
    let path_bytes = mutation.logical_owned_bytes();
    let old_bytes = crate::foundation::usize_to_u64(pending.capacity()
        .checked_mul(std::mem::size_of::<AuthoredMutation>()).ok_or_else(counter_overflow)?);
    reserve_exactify_slot(pending, retained_bytes, work, budget)?;
    let new_bytes = crate::foundation::usize_to_u64(pending.capacity()
        .checked_mul(std::mem::size_of::<AuthoredMutation>()).ok_or_else(counter_overflow)?);
    *pending_bytes = pending_bytes.checked_add(path_bytes)
        .and_then(|value| value.checked_add(new_bytes.checked_sub(old_bytes)?))
        .ok_or_else(counter_overflow)?;
    // The path was admitted by exactify_namespace_path; only the new
    // container storage is added to the containing live total here.
    pending.push(mutation);
    Ok(())
}

fn reserve_exactify_slot<T>(
    values: &mut Vec<T>,
    retained_bytes: &mut u64,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<(), LazyWorkspaceError> {
    if values.len() < values.capacity() {
        return Ok(());
    }
    let width = std::mem::size_of::<T>();
    let old_bytes = crate::foundation::usize_to_u64(
        values.capacity().checked_mul(width).ok_or_else(counter_overflow)?,
    );
    let target = values.capacity().checked_mul(2).map(|value| value.max(1))
        .ok_or_else(counter_overflow)?;
    let copied = crate::foundation::usize_to_u64(
        values.len().checked_mul(width).ok_or_else(counter_overflow)?,
    );
    work.admit(&WorkCounters { bytes_copied: copied, ..WorkCounters::default() }, &budget)?;
    let mut allocations = crate::kernel::allocation::AllocationLedger::default();
    allocations.claim_bytes(*retained_bytes, 0, work, budget).map_err(lazy_allocation_error)?;
    allocations.claim_elements::<T>(target, work, budget).map_err(lazy_allocation_error)?;
    let mut replacement = Vec::new();
    replacement.try_reserve_exact(target)
        .map_err(|_| LazyWorkspaceError::Store("exactify container allocation failed".to_owned()))?;
    let excess = replacement.capacity().checked_sub(target)
        .and_then(|value| value.checked_mul(width)).ok_or_else(counter_overflow)?;
    if excess != 0 {
        allocations.claim_bytes(crate::foundation::usize_to_u64(excess), 0,
            work, WorkBudget::UNBOUNDED).map_err(lazy_allocation_error)?;
        work.verify(budget)?;
    }
    charge_lazy_copy(copied, work, budget)?;
    replacement.append(values);
    *values = replacement;
    allocations.release(old_bytes).map_err(lazy_allocation_error)?;
    *retained_bytes = allocations.live_bytes();
    Ok(())
}


fn retained_directory_page_bytes<T>(
    capacity: usize,
    _entries: &[T],
    mut names: impl Iterator<Item = usize>,
) -> Result<u64, LazyWorkspaceError> {
    let slots = capacity
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(counter_overflow)?;
    let names = names
        .try_fold(0_usize, |total, bytes| total.checked_add(bytes))
        .ok_or_else(counter_overflow)?;
    u64::try_from(slots.checked_add(names).ok_or_else(counter_overflow)?)
        .map_err(|_| counter_overflow())
}

fn directory_remaining_work(
    work: WorkCounters,
    allocations: &crate::kernel::allocation::AllocationLedger,
    budget: WorkBudget,
) -> Result<WorkBudget, LazyWorkspaceError> {
    let mut remaining = remaining_work(work, budget)?;
    remaining.peak_allocation_bytes = remaining.peak_allocation_bytes
        .checked_sub(allocations.live_bytes()).ok_or_else(counter_overflow)?;
    Ok(remaining)
}

fn accept_directory_work<T>(
    result: Result<OperationReceipt<T>, LazyWorkspaceError>,
    work: &mut WorkCounters,
    allocations: &crate::kernel::allocation::AllocationLedger,
) -> Result<T, LazyWorkspaceError> {
    let (value, spent) = match result {
        Ok(receipt) => (Ok(receipt.value), receipt.work),
        Err(error) => {
            let failure = error.into_measured_failure();
            (Err(failure.error), *failure.work)
        }
    };
    *work = work.with_backend(spent, allocations.live_bytes())?;
    value
}

fn directory_vec<T>(
    count: usize,
    allocations: &mut crate::kernel::allocation::AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<Vec<T>, LazyWorkspaceError> {
    cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
    let bytes = allocations.claim_elements::<T>(count, work, budget).map_err(lazy_allocation_error)?;
    let mut values = Vec::new();
    if values.try_reserve_exact(count).is_err() {
        allocations.release(bytes).map_err(lazy_allocation_error)?;
        return Err(LazyWorkspaceError::Work("directory projection allocation failed".to_owned()));
    }
    Ok(values)
}

fn directory_namespace_error(error: crate::kernel::NamespacePathError) -> LazyWorkspaceError {
    match error {
        crate::kernel::NamespacePathError::Cancelled => LazyWorkspaceError::Cancelled,
        crate::kernel::NamespacePathError::Work(error) => error.into(),
        error => LazyWorkspaceError::Workspace(error.to_string()),
    }
}

fn directory_mask_contains(
    paths: &[String],
    child: &str,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<bool, LazyWorkspaceError> {
    let mut lower = 0;
    let mut upper = paths.len();
    while lower < upper {
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        work.charge_items(1, &budget)?;
        let middle = lower + (upper - lower) / 2;
        let path = paths.get(middle).ok_or(LazyWorkspaceError::Concurrent)?;
        match path.as_str().cmp(child) {
            std::cmp::Ordering::Less => lower = middle + 1,
            std::cmp::Ordering::Greater => upper = middle,
            std::cmp::Ordering::Equal => return Ok(true),
        }
    }
    Ok(false)
}

fn directory_child_path(
    parent: &str,
    name: &LogicalName,
    allocations: &mut crate::kernel::allocation::AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<Option<String>, LazyWorkspaceError> {
    cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
    work.charge_items(1, &budget)?;
    let Some(name_bytes) = name.unicode_utf8_bytes() else { return Ok(None); };
    let parent_bytes = if parent == "/" { 0 } else { parent.len() };
    let length = parent_bytes.checked_add(1).and_then(|length| length.checked_add(name_bytes))
        .ok_or_else(counter_overflow)?;
    let delta = if name.encoding() == crate::kernel::NameEncoding::WindowsUtf16Le {
        WorkCounters { bytes_copied: (parent_bytes + 1) as u64, bytes_encoded: name_bytes as u64, ..WorkCounters::default() }
    } else {
        WorkCounters { bytes_copied: length as u64, ..WorkCounters::default() }
    };
    work.admit(&delta, &budget)?;
    allocations.claim_elements::<u8>(length, work, budget).map_err(lazy_allocation_error)?;
    let mut child = String::new();
    child.try_reserve_exact(length)
        .map_err(|_| LazyWorkspaceError::Work("directory child allocation failed".to_owned()))?;
    if parent_bytes != 0 { child.push_str(parent); }
    child.push('/');
    name.append_unicode(&mut child).map_err(|error| LazyWorkspaceError::Workspace(error.to_string()))?;
    work.charge(&delta, &budget)?;
    Ok(Some(child))
}

fn directory_clone_name(
    name: &LogicalName,
    allocations: &mut crate::kernel::allocation::AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<LogicalName, LazyWorkspaceError> {
    let length = name.as_bytes().len();
    let delta = WorkCounters { bytes_copied: length as u64, ..WorkCounters::default() };
    work.admit(&delta, &budget)?;
    let mut bytes = directory_vec::<u8>(length, allocations, work, budget, cancellation)?;
    bytes.extend_from_slice(name.as_bytes());
    work.charge(&delta, &budget)?;
    LogicalName::new(name.encoding(), bytes, u32::MAX)
        .map_err(|error| LazyWorkspaceError::Workspace(error.to_string()))
}


fn exactify_path_vector_bytes(paths: &Vec<String>) -> Result<u64, LazyWorkspaceError> {
    u64::try_from(paths.capacity())
        .ok()
        .and_then(|count| count.checked_mul(u64::try_from(std::mem::size_of::<String>()).ok()?))
        .ok_or_else(counter_overflow)
}

fn clone_exactify_string(
    value: &str,
    retained_bytes: &mut u64,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<String, LazyWorkspaceError> {
    let mut allocations = crate::kernel::allocation::AllocationLedger::default();
    allocations.claim_bytes(*retained_bytes, 0, work, budget).map_err(lazy_allocation_error)?;
    let bytes = clone_lazy_bytes(value.as_bytes(), &mut allocations, work, budget)?;
    *retained_bytes = allocations.live_bytes();
    String::from_utf8(bytes).map_err(|error| LazyWorkspaceError::Workspace(error.to_string()))
}

fn exactify_child_path(
    directory: &str,
    name: &LogicalName,
    retained_bytes: &mut u64,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<String, LazyWorkspaceError> {
    let prefix = if directory == "/" { "" } else { directory };
    let name_bytes = name.unicode_utf8_bytes().ok_or(LazyWorkspaceError::UnsupportedNode)?;
    let bytes = prefix.len().checked_add(1).and_then(|value| value.checked_add(name_bytes))
        .ok_or_else(counter_overflow)?;
    let mut allocations = crate::kernel::allocation::AllocationLedger::default();
    allocations.claim_bytes(*retained_bytes, 0, work, budget).map_err(lazy_allocation_error)?;
    allocations.claim_elements::<u8>(bytes, work, budget).map_err(lazy_allocation_error)?;
    let mut result = String::new();
    result.try_reserve_exact(bytes)
        .map_err(|_| LazyWorkspaceError::Store("exactify path allocation failed".to_owned()))?;
    if result.capacity() > bytes {
        allocations.claim_bytes(crate::foundation::usize_to_u64(result.capacity() - bytes),
            0, work, WorkBudget::UNBOUNDED).map_err(lazy_allocation_error)?;
        work.verify(budget)?;
    }
    charge_lazy_copy(crate::foundation::usize_to_u64(bytes), work, budget)?;
    result.push_str(prefix);
    result.push('/');
    name.append_unicode(&mut result).map_err(|_| LazyWorkspaceError::UnsupportedNode)?;
    *retained_bytes = allocations.live_bytes();
    Ok(result)
}

fn remaining_exactify_work(
    work: WorkCounters,
    budget: WorkBudget,
    live_bytes: u64,
) -> Result<WorkBudget, LazyWorkspaceError> {
    let mut remaining = remaining_work(work, budget)?;
    remaining.peak_allocation_bytes = remaining.peak_allocation_bytes
        .checked_sub(live_bytes).ok_or_else(counter_overflow)?;
    Ok(remaining)
}

fn exactify_namespace_path(
    path: &str,
    profile: crate::FilesystemProfile,
    limits: crate::VolumeLimits,
    retained_bytes: &mut u64,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<NamespacePath, LazyWorkspaceError> {
    let mut allocations = crate::kernel::allocation::AllocationLedger::default();
    allocations.claim_bytes(*retained_bytes, 0, work, budget).map_err(lazy_allocation_error)?;
    let result = NamespacePath::parse_in_profile_with_ledger(
        path, profile, limits, &mut allocations, work, budget, cancellation,
    ).map_err(|error| LazyWorkspaceError::Workspace(error.to_string()))?;
    *retained_bytes = allocations.live_bytes();
    Ok(result)
}

fn exactify_vec<T>(
    count: usize,
    retained_bytes: &mut u64,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<Vec<T>, LazyWorkspaceError> {
    let mut allocations = crate::kernel::allocation::AllocationLedger::default();
    allocations.claim_bytes(*retained_bytes, 0, work, budget).map_err(lazy_allocation_error)?;
    let result = directory_vec(count, &mut allocations, work, budget, cancellation)?;
    *retained_bytes = allocations.live_bytes();
    Ok(result)
}

fn nested_lazy_failure(error: LazyWorkspaceError, live_bytes: u64) -> LazyWorkspaceError {
    let mut failure = error.into_measured_failure();
    match failure.work.peak_allocation_bytes.checked_add(live_bytes) {
        Some(peak) => {
            failure.work.peak_allocation_bytes = peak;
            lazy_operation_failure(failure, std::convert::identity)
        }
        None => LazyWorkspaceError::from(crate::WorkError::Overflow).with_prior_work(*failure.work),
    }
}

fn remaining_work(
    current: WorkCounters,
    budget: WorkBudget,
) -> Result<WorkBudget, LazyWorkspaceError> {
    current
        .remaining(budget)
        .map_err(LazyWorkspaceError::from)
}

fn exactify_error(stage: &str, path: &str, error: LazyWorkspaceError) -> LazyWorkspaceError {
    match error {
        LazyWorkspaceError::SpentWork(failure) => {
            let failure = *failure;
            lazy_operation_failure(
                failure,
                |error| exactify_error(stage, path, error),
            )
        }
        LazyWorkspaceError::Cancelled | LazyWorkspaceError::Demand(DemandError::Cancelled) => {
            LazyWorkspaceError::Cancelled
        }
        error @ LazyWorkspaceError::Work(_) => error,
        error => {
            LazyWorkspaceError::Workspace(format!("exactify could not {stage} {path}: {error}"))
        }
    }
}

fn exactify_batch_key(snapshot_id: LazySnapshotId, root: &str, index: u64) -> IdempotencyKey {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-fs-lazy-exactify-batch-v1\0");
    hasher.update(&snapshot_id.into_bytes());
    hasher.update(root.as_bytes());
    hasher.update(&index.to_le_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    IdempotencyKey::from_bytes(bytes)
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::demand::{
        DemandFile, DemandResult, SourceCursor, SourceDirectoryEntry, SourceDirectoryPage,
        SourceMetadata, SourceVersion,
    };
    use crate::kernel::allocation::AllocationLedger;
    use crate::kernel::NameEncoding;
    use crate::performance::{OperationFailure, OperationReceipt, WorkCounters};
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

    #[test]
    fn canonical_path_admission_tracks_actual_capacity_and_transfer() {
        let cancellation = CancellationToken::new();
        let limits = crate::VolumeLimits::default();
        let path = "/actual-owned-path";
        let parent = LazyTreapWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let owned = parent.canonical_path_owned(path, limits).expect("admitted path");
        let capacity = crate::foundation::usize_to_u64(owned.capacity());
        assert_eq!(parent.spent().bytes_copied, path.len() as u64);
        assert_eq!(parent.spent().allocation_operations, 1);
        assert_eq!(parent.live_bytes(), capacity);
        assert_eq!(parent.spent().peak_allocation_bytes, capacity);
        parent.release(capacity).expect("transfer claim");
        let child = LazyTreapWork::new(parent.remaining().expect("remaining"), &cancellation);
        child.adopt(capacity).expect("adopt moved capacity");
        assert_eq!(child.spent().allocation_operations, 0);
        assert_eq!(child.spent().bytes_copied, 0);
        assert_eq!(child.live_bytes(), capacity);
        drop(owned);
        child.release(capacity).expect("release dropped path");
        assert_eq!(child.live_bytes(), 0);
        assert_eq!(child.remaining().expect("released budget").peak_allocation_bytes, u64::MAX);

        let mut bounded = limits;
        bounded.maximum_path_bytes = 16;
        let oversized = "/".to_owned() + &"x".repeat(16);
        let denied = LazyTreapWork::new(WorkBudget::UNBOUNDED, &cancellation);
        assert!(denied.canonical_path_owned(&oversized, bounded).is_err());
        assert_eq!(denied.spent(), WorkCounters::default());
        assert_eq!(denied.live_bytes(), 0);
    }

    #[tokio::test]
    async fn measured_canonical_paths_reject_before_source_work() {
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"content")));
        let root = LazyWorkspace::attach(
            &Fs::memory(), "path-admission-controls", Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        ).await.expect("attach");
        let state = root.state().await.expect("state");
        let path = "/file.txt";
        let cancellation = CancellationToken::new();
        let mut allocation_denied = WorkBudget::UNBOUNDED;
        allocation_denied.allocation_operations = 0;
        let mut copy_denied = WorkBudget::UNBOUNDED;
        copy_denied.bytes_copied = 0;
        let mut capacity_denied = WorkBudget::UNBOUNDED;
        capacity_denied.peak_allocation_bytes = path.len() as u64 - 1;
        for budget in [allocation_denied, copy_denied, capacity_denied] {
            let failure = root.observe_measured(path, state.clone(), budget, &cancellation)
                .await.err().expect("path rejected before lookup");
            assert!(matches!(failure.semantic_error(), LazyWorkspaceError::Work(_)));
            assert_eq!(failure.spent_work().bytes_copied, 0);
            if budget.bytes_copied == 0 {
                assert_eq!(failure.spent_work().allocation_operations, 1);
                assert!(failure.spent_work().peak_allocation_bytes >= path.len() as u64);
            } else {
                assert_eq!(failure.spent_work(), WorkCounters::default());
            }
            let failure = root.promote_with_permit_measured(
                path, 64, IdempotencyKey::from_bytes([0xd7; 16]),
                crate::PublicationPermit::Unrestricted, budget, &cancellation,
            ).await.err().expect("requested path rejected before promotion");
            assert!(matches!(failure.semantic_error(), LazyWorkspaceError::Work(_)));
            assert_eq!(failure.spent_work().bytes_copied, 0);
        }
        let mut child_denied = WorkBudget::UNBOUNDED;
        child_denied.items_examined = 0;
        let failure = root.observe_measured(
            path, state.clone(), child_denied, &cancellation,
        ).await.err().expect("overlay lookup rejected after path construction");
        assert!(matches!(failure.semantic_error(), LazyWorkspaceError::Work(_)));
        assert_eq!(failure.spent_work().bytes_copied, path.len() as u64);
        assert_eq!(failure.spent_work().allocation_operations, 1);
        assert!(failure.spent_work().peak_allocation_bytes >= path.len() as u64);
        let oversized = "/".to_owned()
            + &"x".repeat(root.workspace.limits().maximum_path_bytes as usize);
        let failure = root.observe_measured(
            &oversized, state.clone(), WorkBudget::UNBOUNDED, &cancellation,
        ).await.err().expect("oversized canonical path");
        assert!(matches!(failure.semantic_error(), LazyWorkspaceError::Workspace(_)));
        assert_eq!(failure.spent_work(), WorkCounters::default());
        cancellation.cancel();
        let failure = root.observe_measured(
            path, state, WorkBudget::UNBOUNDED, &cancellation,
        ).await.err().expect("cancelled path construction");
        assert!(matches!(failure.semantic_error(), LazyWorkspaceError::Cancelled));
        assert_eq!(failure.spent_work(), WorkCounters::default());
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 0);
        assert_eq!(source.counts.pages.load(Ordering::Relaxed), 0);
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn measured_authored_lookup_admits_the_returned_path_clone() {
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"content")));
        let root = LazyWorkspace::attach(
            &Fs::memory(), "authored-path-controls", Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        ).await.expect("attach");
        let path = "/file.txt";
        root.workspace().write(path, Bytes::from_static(b"authored")).await.expect("write");
        let cancellation = CancellationToken::new();
        let stat = root.workspace.stat_optional_measured(
            path, WorkBudget::UNBOUNDED, &cancellation,
        ).await.expect("authored stat");
        let inspected = root.inspect_measured(path, WorkBudget::UNBOUNDED, &cancellation)
            .await.expect("authored inspect");
        let LazyLookup::Authored { path: owned, .. } = inspected.value.lookup else {
            panic!("authored path expected");
        };
        assert_eq!(owned, path);
        assert_eq!(inspected.work.bytes_copied, stat.work.bytes_copied + path.len() as u64);
        assert_eq!(inspected.work.allocation_operations, stat.work.allocation_operations + 1);
        assert_eq!(inspected.work.peak_allocation_bytes,
            stat.work.peak_allocation_bytes.max(owned.capacity() as u64));
        let looked_up = root.lookup_measured(path, WorkBudget::UNBOUNDED, &cancellation)
            .await.expect("authored lookup");
        assert!(looked_up.work.bytes_copied >= stat.work.bytes_copied + path.len() as u64);
        assert!(looked_up.work.allocation_operations >= stat.work.allocation_operations + 1);

        let mut denied = WorkBudget::UNBOUNDED;
        denied.bytes_copied = stat.work.bytes_copied;
        let failure = root.inspect_measured(path, denied, &cancellation)
            .await.err().expect("reject returned path copy");
        assert!(matches!(failure.semantic_error(), LazyWorkspaceError::Work(_)));
        assert_eq!(failure.spent_work().bytes_copied, stat.work.bytes_copied);
        assert_eq!(failure.spent_work().allocation_operations, stat.work.allocation_operations + 1);
        assert!(failure.spent_work().peak_allocation_bytes >= path.len() as u64);
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 0);
    }

    /// A deliberately modeled one-I/O source, not a native-provider receipt.
    /// All fixture-owned search, hash, result, and buffer work is admitted too.
    struct SourceFixtureMeter {
        budget: WorkBudget,
        work: WorkCounters,
        allocations: AllocationLedger,
    }

    impl SourceFixtureMeter {
        fn begin(budget: WorkBudget, cancellation: &CancellationToken) -> Result<Self, OperationFailure<DemandError>> {
            let mut meter = Self { budget, work: WorkCounters::default(), allocations: AllocationLedger::default() };
            meter.check(cancellation)?;
            meter.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
            Ok(meter)
        }

        fn failure(&self, error: DemandError) -> OperationFailure<DemandError> {
            OperationFailure::new(error, self.work)
        }

        fn check(&self, cancellation: &CancellationToken) -> Result<(), OperationFailure<DemandError>> {
            cancellation.check().map_err(|_| self.failure(DemandError::Cancelled))
        }

        fn charge(&mut self, delta: WorkCounters) -> Result<(), OperationFailure<DemandError>> {
            self.work.charge(&delta, &self.budget)
                .map_err(|error| self.failure(error.into()))
        }

        fn allocate<T>(&mut self, count: usize) -> Result<(), OperationFailure<DemandError>> {
            self.allocations.claim_elements::<T>(count, &mut self.work, self.budget)
                .map(|_| ())
                .map_err(|error| self.failure(error.into_work_or(DemandError::WorkerUnavailable)))
        }

        fn receipt<T>(self, value: T) -> DemandResult<T> {
            Ok(OperationReceipt { value, work: self.work })
        }

        fn range(&mut self, bytes: &Bytes, offset: u64, length: u64) -> Result<Bytes, OperationFailure<DemandError>> {
            let start = usize::try_from(offset).unwrap_or(usize::MAX).min(bytes.len());
            let end = start.saturating_add(usize::try_from(length).unwrap_or(usize::MAX)).min(bytes.len());
            let length = (end - start) as u64;
            self.charge(WorkCounters { source_bytes_read: length, ..WorkCounters::default() })?;
            let value = bytes.slice(start..end);
            Ok(value)
        }
    }

    #[derive(Default)]
    struct SourceCounts {
        lookups: AtomicUsize,
        pages: AtomicUsize,
        ranges: AtomicUsize,
    }

    struct CountingSource {
        identity: [u8; 16],
        epoch: AtomicU64,
        node: SourceNode,
        versions: Mutex<BTreeMap<u64, Bytes>>,
        directory_entries: usize,
        counts: SourceCounts,
        cancel_on_page: AtomicBool,
        cancel_on_range: AtomicBool,
    }

    impl CountingSource {
        fn new(bytes: Bytes) -> Self {
            // Promote owned Bytes backing during fixture setup, not a callback.
            let shared = bytes.clone();
            drop(bytes);
            let bytes = shared;
            let logical_bytes = bytes.len() as u64;
            let versions = BTreeMap::from([(3, bytes)]);
            Self {
                identity: [7; 16],
                epoch: AtomicU64::new(3),
                node: SourceNode {
                    kind: SourceNodeKind::RegularFile,
                    file_identity: [8; 32],
                    link_count: Some(1),
                    device: None,
                    logical_bytes: Some(logical_bytes),
                    version: SourceVersion([9; 32]),
                    metadata: SourceMetadata::default(),
                },
                versions: Mutex::new(versions),
                directory_entries: 1,
                counts: SourceCounts::default(),
                cancel_on_page: AtomicBool::new(false),
                cancel_on_range: AtomicBool::new(false),
            }
        }

        fn resolve(
            &self,
            source: SourceReference,
            expected: Option<SourceVersion>,
            meter: &mut SourceFixtureMeter,
        ) -> Result<(SourceNode, Bytes), OperationFailure<DemandError>> {
            if source.identity != self.identity {
                return Err(meter.failure(DemandError::StaleSource));
            }
            let versions = self.versions.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            for (epoch, bytes) in versions.iter() {
                meter.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                if *epoch != source.epoch {
                    continue;
                }
                meter.charge(WorkCounters { bytes_hashed: bytes.len() as u64, ..WorkCounters::default() })?;
                let node = SourceNode {
                    logical_bytes: Some(bytes.len() as u64),
                    version: SourceVersion(*blake3::hash(bytes).as_bytes()),
                    ..self.node
                };
                if expected.is_some_and(|version| version != node.version) {
                    return Err(meter.failure(DemandError::StaleVersion));
                }
                return Ok((node, bytes.clone()));
            }
            Err(meter.failure(DemandError::StaleSource))
        }

        fn invalidate(&self) {
            let prior = self.epoch.fetch_add(1, Ordering::Relaxed);
            let mut versions = self
                .versions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let bytes = versions.get(&prior).cloned().unwrap_or_default();
            versions.insert(prior + 1, bytes);
        }

        fn replace(&self, bytes: Bytes) {
            let shared = bytes.clone();
            drop(bytes);
            let bytes = shared;
            let epoch = self.epoch.fetch_add(1, Ordering::Relaxed) + 1;
            self.versions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(epoch, bytes);
        }

        fn with_directory_entries(mut self, entries: usize) -> Self {
            self.directory_entries = entries;
            self
        }

        fn cancel_next_page(&self) {
            self.cancel_on_page.store(true, Ordering::Relaxed);
        }

        #[cfg(feature = "native-mount")]
        fn cancel_next_range(&self) {
            self.cancel_on_range.store(true, Ordering::Relaxed);
        }
    }

    /// One source version's bytes, pinned when the file was opened.
    struct PinnedFile(Bytes);

    impl DemandFile for PinnedFile {
        fn read_range(
            &self,
            offset: u64,
            length: u64,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> DemandResult<Bytes> {
            let mut meter = SourceFixtureMeter::begin(budget, cancellation)?;
            let value = meter.range(&self.0, offset, length)?;
            meter.check(cancellation)?;
            meter.charge(WorkCounters { output_bytes: value.len() as u64, ..WorkCounters::default() })?;
            meter.receipt(value)
        }
    }

    impl DemandSource for CountingSource {
        fn reference(&self) -> SourceReference {
            SourceReference {
                identity: self.identity,
                epoch: self.epoch.load(Ordering::Relaxed),
            }
        }

        async fn lookup(
            &self,
            source: SourceReference,
            _path: &NamespacePath,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> DemandResult<Option<SourceNode>> {
            let mut meter = SourceFixtureMeter::begin(budget, cancellation)?;
            self.counts.lookups.fetch_add(1, Ordering::Relaxed);
            let (node, _) = self.resolve(source, None, &mut meter)?;
            meter.charge(WorkCounters { items_returned: 1, ..WorkCounters::default() })?;
            meter.check(cancellation)?;
            meter.receipt(Some(node))
        }

        async fn list_page(
            &self,
            source: SourceReference,
            _directory: &NamespacePath,
            cursor: Option<SourceCursor>,
            maximum_entries: u32,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> DemandResult<SourceDirectoryPage> {
            let mut meter = SourceFixtureMeter::begin(budget, cancellation)?;
            self.counts.pages.fetch_add(1, Ordering::Relaxed);
            if self.cancel_on_page.swap(false, Ordering::Relaxed) {
                cancellation.cancel();
                return Err(meter.failure(DemandError::Cancelled));
            }
            if cursor.is_some() {
                return Err(meter.failure(DemandError::StaleCursor));
            }
            if self.directory_entries > maximum_entries as usize {
                return Err(meter.failure(DemandError::InvalidRequest));
            }
            let start = 0;
            let (node, _) = self.resolve(source, None, &mut meter)?;
            let end = self.directory_entries;
            meter.allocate::<SourceDirectoryEntry>(end - start)?;
            let mut entries = Vec::with_capacity(end - start);
            for index in start..end {
                meter.check(cancellation)?;
                meter.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                let mut digits = 1_usize;
                let mut quotient = index;
                while quotient >= 10 {
                    quotient /= 10;
                    digits += 1;
                }
                let length = if self.directory_entries == 1 { 8 } else { 9 + digits.max(4) };
                meter.allocate::<u8>(length)?;
                meter.charge(WorkCounters { bytes_copied: length as u64, ..WorkCounters::default() })?;
                let mut name = Vec::with_capacity(length);
                if self.directory_entries == 1 {
                    name.extend_from_slice(b"file.txt");
                } else {
                    use std::io::Write as _;
                    write!(&mut name, "file-{index:04}.txt").expect("bounded name buffer");
                }
                meter.charge(WorkCounters { items_returned: 1, ..WorkCounters::default() })?;
                entries.push(SourceDirectoryEntry {
                    name: LogicalName::new(NameEncoding::Utf8, name, 255).expect("name"),
                    node,
                });
            }
            meter.receipt(SourceDirectoryPage::stateless(entries, None, SourceVersion([4; 32])))
        }

        async fn read_range(
            &self,
            source: SourceReference,
            _path: &NamespacePath,
            expected: SourceVersion,
            offset: u64,
            length: u64,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> DemandResult<Bytes> {
            let mut meter = SourceFixtureMeter::begin(budget, cancellation)?;
            self.counts.ranges.fetch_add(1, Ordering::Relaxed);
            let (_, bytes) = self.resolve(source, Some(expected), &mut meter)?;
            let value = meter.range(&bytes, offset, length)?;
            if self.cancel_on_range.swap(false, Ordering::Relaxed) {
                cancellation.cancel();
            }
            meter.check(cancellation)?;
            meter.charge(WorkCounters { output_bytes: value.len() as u64, ..WorkCounters::default() })?;
            meter.receipt(value)
        }

        async fn open_file(
            &self,
            source: SourceReference,
            _path: &NamespacePath,
            expected: SourceVersion,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> DemandResult<Box<dyn DemandFile>> {
            let mut meter = SourceFixtureMeter::begin(budget, cancellation)?;
            let (_, bytes) = self.resolve(source, Some(expected), &mut meter)?;
            meter.allocate::<PinnedFile>(1)?;
            meter.check(cancellation)?;
            meter.receipt(Box::new(PinnedFile(bytes)) as Box<dyn DemandFile>)
        }

        async fn read_link(
            &self,
            _source: SourceReference,
            _path: &NamespacePath,
            _expected: SourceVersion,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> DemandResult<Bytes> {
            let meter = SourceFixtureMeter::begin(budget, cancellation)?;
            Err(meter.failure(DemandError::InvalidRequest))
        }
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    struct CancelAfterNativePage {
        inner: crate::demand::native::NativeDemandSource,
        cancel: AtomicBool,
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    impl DemandSource for CancelAfterNativePage {
        fn reference(&self) -> SourceReference { self.inner.reference() }

        async fn lookup(&self, source: SourceReference, path: &NamespacePath,
            budget: WorkBudget, token: &CancellationToken) -> DemandResult<Option<SourceNode>> {
            self.inner.lookup(source, path, budget, token).await
        }

        async fn list_page(&self, source: SourceReference, path: &NamespacePath,
            cursor: Option<SourceCursor>, maximum: u32, budget: WorkBudget,
            token: &CancellationToken) -> DemandResult<SourceDirectoryPage> {
            let receipt = self.inner.list_page(source, path, cursor, maximum, budget, token).await?;
            if self.cancel.swap(false, Ordering::Relaxed) { token.cancel(); }
            Ok(receipt)
        }

        async fn read_range(&self, source: SourceReference, path: &NamespacePath,
            version: SourceVersion, offset: u64, length: u64, budget: WorkBudget,
            token: &CancellationToken) -> DemandResult<Bytes> {
            self.inner.read_range(source, path, version, offset, length, budget, token).await
        }

        async fn open_file(&self, source: SourceReference, path: &NamespacePath,
            version: SourceVersion, budget: WorkBudget, token: &CancellationToken)
            -> DemandResult<Box<dyn DemandFile>> {
            self.inner.open_file(source, path, version, budget, token).await
        }

        async fn read_link(&self, source: SourceReference, path: &NamespacePath,
            version: SourceVersion, budget: WorkBudget, token: &CancellationToken) -> DemandResult<Bytes> {
            self.inner.read_link(source, path, version, budget, token).await
        }
    }

    fn finite_fixture_budget() -> WorkBudget {
        let mut value = serde_json::to_value(WorkBudget::UNBOUNDED).expect("budget fields");
        for allowance in value.as_object_mut().expect("budget object").values_mut() {
            *allowance = serde_json::json!(100_000_000_u64);
        }
        serde_json::from_value(value).expect("finite budget")
    }

    #[test]
    fn finite_lazy_hash_reports_exact_streaming_prefixes() {
        let value = LazyOverlay::default();
        let budget = finite_fixture_budget();
        let token = CancellationToken::new();
        let receipt = hash_lazy_value_measured(&value, budget, &token).expect("bounded hash");
        let bytes = serde_json::to_vec(&value).expect("canonical JSON").len() as u64;
        assert_eq!(receipt.work, WorkCounters {
            bytes_hashed: bytes, bytes_encoded: bytes, ..WorkCounters::default()
        });
        let mut denied = budget;
        denied.bytes_hashed = bytes - 1;
        let failure = hash_lazy_value_measured(&value, denied, &token).expect_err("deny final hash write");
        assert!(matches!(failure.semantic_error(), LazyWorkspaceError::Work(_)));
        assert!(failure.spent_work().bytes_hashed > 0);
        assert_eq!(failure.spent_work().bytes_encoded, failure.spent_work().bytes_hashed);
        failure.spent_work().verify(denied).expect("streaming prefix");
        token.cancel();
        let failure = hash_lazy_value_measured(&value, budget, &token).expect_err("cancel before hash");
        assert!(matches!(failure.semantic_error(), LazyWorkspaceError::Cancelled));
        assert_eq!(failure.spent_work(), WorkCounters::default());
    }

    #[tokio::test]
    async fn finite_memory_cas_rejections_leave_the_actual_binding_unchanged() {
        let fs = Fs::memory();
        let store = MemoryLazyWorkspaceStore::default();
        let root = LazyWorkspace::attach(
            &fs, "finite-companion-cas", Arc::new(CountingSource::new(Bytes::from_static(b"source"))), store.clone(),
        ).await.expect("attach");
        let original = root.state().await.expect("binding");
        let mut replacement = original.clone();
        replacement.revision += 1;
        replacement.source.epoch += 1;
        let budget = finite_fixture_budget();
        let token = CancellationToken::new();
        let mut denied = budget;
        denied.bytes_copied = 0;
        let failure = store.compare_and_swap_lazy_workspace_measured(
            root.workspace().id(), original.revision, replacement.clone(), denied, &token,
        ).await.expect_err("deny replacement copy");
        assert!(matches!(failure.semantic_error(), LazyWorkspaceError::Work(_)));
        assert_eq!(failure.spent_work(), WorkCounters { items_examined: 1, ..WorkCounters::default() });
        assert_eq!(store.load_lazy_workspace(root.workspace().id()).await.expect("load"), Some(original.clone()));
        token.cancel();
        let failure = store.compare_and_swap_lazy_workspace_measured(
            root.workspace().id(), original.revision, replacement, budget, &token,
        ).await.expect_err("cancel replacement");
        assert_eq!(failure.spent_work(), WorkCounters::default());
        assert_eq!(store.load_lazy_workspace(root.workspace().id()).await.expect("load"), Some(original));
    }

    #[tokio::test]
    async fn finite_treap_lookup_rejections_retain_real_node_receipts() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs, "finite-overlay-descent", Arc::clone(&source), MemoryLazyWorkspaceStore::default(),
        ).await.expect("attach");
        for index in 0..64 {
            root.stat(&format!("/observed-{index:02}")).await.expect("observe node");
        }
        let state = root.state().await.expect("state");
        let budget = finite_fixture_budget();
        let token = CancellationToken::new();
        let receipt = root.overlay_fact_measured(state.overlay, "/observed-00", budget, &token)
            .await.expect("bounded descent");
        assert!(matches!(receipt.value, Some(LazyOverlayChange::Observe { .. })));
        assert!(receipt.work.items_examined > 0);
        assert!(receipt.work.bytes_hashed > 0);
        assert!(receipt.work.allocation_operations > 0);
        receipt.work.verify(budget).expect("finite descent");
        let source_lookups = source.counts.lookups.load(Ordering::Relaxed);
        let mut denied = budget;
        denied.bytes_hashed = receipt.work.bytes_hashed - 1;
        let failure = root.overlay_fact_measured(state.overlay, "/observed-00", denied, &token)
            .await.expect_err("deny final node authentication");
        assert!(matches!(failure.semantic_error(), LazyWorkspaceError::Work(_)));
        assert!(failure.spent_work().items_examined > 0);
        failure.spent_work().verify(denied).expect("authenticated node prefix");
        let mut denied = budget;
        denied.allocation_operations = 0;
        let failure = root.overlay_fact_measured(state.overlay, "/observed-00", denied, &token)
            .await.expect_err("deny loaded node ownership");
        assert_eq!(failure.spent_work().allocation_operations, 0);
        assert!(failure.spent_work().bytes_hashed > 0);
        failure.spent_work().verify(denied).expect("before node clone prefix");
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), source_lookups);
        assert_eq!(root.state().await.expect("unchanged state"), state);
    }

    #[tokio::test]
    async fn modeled_source_callbacks_admit_work_before_io_and_keep_started_prefixes() {
        let source = CountingSource::new(Bytes::from_static(b"source"));
        let path = NamespacePath::new(
            vec![LogicalName::new(NameEncoding::Utf8, b"file.txt".to_vec(), 255).expect("name")],
            16, 4096,
        ).expect("path");
        let token = CancellationToken::new();
        let budget = finite_fixture_budget();
        let mut denied = budget;
        denied.backend_read_operations = 0;
        let failure = source.lookup(source.reference(), &path, denied, &token).await.expect_err("deny modeled IO");
        assert!(matches!(failure.error, DemandError::Work(_)));
        assert_eq!(*failure.work, WorkCounters::default());
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 0);

        let lookup = source.lookup(source.reference(), &path, budget, &token).await.expect("lookup");
        assert_eq!(lookup.work, WorkCounters {
            backend_read_operations: 1, items_examined: 1, bytes_hashed: 6, items_returned: 1,
            ..WorkCounters::default()
        });
        let node = lookup.value.expect("node");
        let mut denied = budget;
        denied.bytes_hashed = 5;
        let failure = source.lookup(source.reference(), &path, denied, &token).await.expect_err("deny hash before hashing");
        assert_eq!(*failure.work, WorkCounters {
            backend_read_operations: 1, items_examined: 1, ..WorkCounters::default()
        });
        failure.work.verify(denied).expect("admitted prefix");

        let page = source.list_page(source.reference(), &path, None, 1, budget, &token).await.expect("page");
        assert_eq!(page.work.allocation_operations, 2);
        assert_eq!(page.work.bytes_copied, 8);
        let mut denied = budget;
        denied.peak_allocation_bytes = page.work.peak_allocation_bytes - 1;
        let failure = source.list_page(source.reference(), &path, None, 1, denied, &token).await.expect_err("deny name allocation");
        assert_eq!(failure.work.backend_read_operations, 1);
        assert_eq!(failure.work.allocation_operations, 1);
        assert_eq!(failure.work.bytes_copied, 0);
        failure.work.verify(denied).expect("page prefix");

        let mut denied = budget;
        denied.allocation_operations = 0;
        let failure = source.open_file(source.reference(), &path, node.version, denied, &token)
            .await.err().expect("deny pinned file box");
        assert_eq!(*failure.work, WorkCounters {
            backend_read_operations: 1, items_examined: 1, bytes_hashed: 6, ..WorkCounters::default()
        });
        let file = source.open_file(source.reference(), &path, node.version, budget, &token).await.expect("open");
        assert_eq!(file.work.allocation_operations, 1);
        let mut denied = budget;
        denied.output_bytes = 0;
        let failure = file.value.read_range(0, 6, denied, &token).expect_err("deny output after modeled read");
        assert_eq!(*failure.work, WorkCounters {
            backend_read_operations: 1, source_bytes_read: 6, ..WorkCounters::default()
        });
        failure.work.verify(denied).expect("read prefix");
        token.cancel();
        let failure = file.value.read_range(0, 6, budget, &token).expect_err("cancel before read");
        assert_eq!(*failure.work, WorkCounters::default());
        assert!(matches!(failure.error, DemandError::Cancelled));
    }

    #[tokio::test]
    async fn finite_sparse_authored_and_detached_shadow_reads_use_the_real_engine() {
        let fs = Fs::memory();
        let mut source = CountingSource::new(Bytes::from_static(b"source"));
        source.node.link_count = Some(2);
        let source = Arc::new(source);
        let root = LazyWorkspace::attach(
            &fs, "finite-sparse-shadow", Arc::clone(&source), MemoryLazyWorkspaceStore::default(),
        ).await.expect("attach");
        root.write("/a", Bytes::from_static(b"head")).await.expect("authored identity");
        let mut transaction = root.workspace().begin_transaction(IdempotencyKey::from_bytes([0xD1; 16]))
            .await.expect("transaction");
        transaction.resize("/a", 4096).await.expect("sparse length");
        transaction.write_range("/a", 4093, Bytes::from_static(b"end")).await.expect("tail extent");
        assert!(matches!(transaction.commit().await.expect("commit"), TransactionCommit::Committed(_)));
        let budget = finite_fixture_budget();
        let token = CancellationToken::new();
        let authored = root.read_range_measured("/a", 0, 4096, budget, &token).await.expect("authored sparse read");
        assert_eq!(&authored.value[..4], b"head");
        assert!(authored.value[4..4093].iter().all(|byte| *byte == 0));
        assert_eq!(&authored.value[4093..], b"end");
        assert_eq!(authored.work.source_bytes_read, 0);
        assert_eq!(authored.work.output_bytes, 4096);
        authored.work.verify(budget).expect("finite authored receipt");
        let mut denied = budget;
        denied.output_bytes = 4095;
        let failure = root.read_range_measured("/a", 0, 4096, denied, &token).await.expect_err("deny authored output");
        assert!(matches!(failure.error.semantic_error(), LazyWorkspaceError::Demand(DemandError::Work(_))));
        assert_eq!(failure.work.output_bytes, 0);
        failure.work.verify(denied).expect("authored rejection prefix");

        root.remove("/a").await.expect("detach last authored alias");
        let shadow = root.read_range_measured("/b", 0, 4096, budget, &token).await.expect("detached sparse shadow");
        assert_eq!(shadow.value, authored.value);
        assert_eq!(shadow.work.source_bytes_read, 0);
        assert_eq!(shadow.work.output_bytes, 4096);
        assert!(shadow.work.backend_read_operations > 0);
        shadow.work.verify(budget).expect("finite shadow receipt");
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
        let mut denied = budget;
        denied.output_bytes = 4095;
        let failure = root.read_range_measured("/b", 0, 4096, denied, &token).await.expect_err("deny shadow output");
        assert!(matches!(failure.error.semantic_error(), LazyWorkspaceError::Demand(DemandError::Work(_))));
        assert_eq!(failure.work.output_bytes, 0);
        failure.work.verify(denied).expect("shadow rejection prefix");
        token.cancel();
        let failure = root.read_range_measured("/b", 0, 4096, budget, &token).await.expect_err("pre-cancelled shadow");
        assert!(matches!(failure.error.semantic_error(), LazyWorkspaceError::Cancelled));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    #[tokio::test]
    async fn finite_lazy_source_reads_preserve_source_failure_work() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs, "finite-source-read", Arc::clone(&source), MemoryLazyWorkspaceStore::default(),
        ).await.expect("attach");
        let budget = finite_fixture_budget();
        let token = CancellationToken::new();
        root.stat("/file.txt").await.expect("observe source");
        let receipt = root.read_range_measured("/file.txt", 0, 6, budget, &token).await.expect("finite source read");
        assert_eq!(receipt.value.as_ref(), b"source");
        assert_eq!(receipt.work.source_bytes_read, 6);
        receipt.work.verify(budget).expect("finite source receipt");
        let mut denied = budget;
        denied.source_bytes_read = 5;
        let failure = root.read_range_measured("/file.txt", 0, 6, denied, &token).await.expect_err("deny source bytes before reading");
        assert!(matches!(failure.error.semantic_error(), LazyWorkspaceError::Demand(DemandError::Work(_))));
        assert_eq!(failure.work.source_bytes_read, 0);
        assert_eq!(failure.work.output_bytes, 0);
        assert!(failure.work.backend_read_operations > 0);
        failure.work.verify(denied).expect("source rejection prefix");

        source.cancel_on_range.store(true, Ordering::Relaxed);
        let failure = root.read_range_measured("/file.txt", 0, 6, budget, &token).await.expect_err("cancel after modeled source read");
        assert!(matches!(failure.error.semantic_error(), LazyWorkspaceError::Demand(DemandError::Cancelled)));
        assert_eq!(failure.work.source_bytes_read, 6);
        assert_eq!(failure.work.output_bytes, 0);
        failure.work.verify(budget).expect("cancelled read prefix");
        let failure = root.read_range_measured("/file.txt", 0, 6, budget, &token).await.expect_err("cancel before any lazy work");
        assert_eq!(*failure.work, WorkCounters::default());
    }

    #[tokio::test]
    async fn public_bounded_lookup_and_exact_promotion_deny_before_source_and_keep_quota_prefix() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs, "public-bounded-lazy", Arc::clone(&source), MemoryLazyWorkspaceStore::default(),
        ).await.expect("attach");
        let lookup = root.lookup("/file.txt").await.expect("observe source");
        let expected = root.direct_file_id_for_lookup(&lookup);
        let before = root.workspace.head().await.expect("initial head").id();
        let lookups = source.counts.lookups.load(Ordering::Relaxed);
        let token = CancellationToken::new();
        let failure = root.lookup_with_budget("/file.txt", WorkBudget::default(), &token)
            .await.expect_err("future denied before source");
        assert_eq!(*failure.work, WorkCounters::default());
        let failure = root.promote_exact_measured(
            "/file.txt", expected, 6, IdempotencyKey::new(), WorkBudget::default(), &token,
        ).await.expect_err("promotion future denied");
        assert_eq!(*failure.work, WorkCounters::default());
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), lookups);
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);

        let budget = finite_fixture_budget();
        let failure = root.promote_exact_measured(
            "/file.txt", expected, 5, IdempotencyKey::new(), budget, &token,
        ).await.expect_err("actual logical bytes exceed promotion quota");
        assert!(matches!(failure.error.semantic_error(), LazyWorkspaceError::TooLarge));
        assert!(failure.work.allocation_operations > 0);
        assert!(failure.work.bytes_hashed > 0);
        assert_eq!(failure.work.source_bytes_read, 0);
        failure.work.verify(budget).expect("complete quota failure prefix");
        assert_eq!(root.workspace.head().await.expect("unpublished head").id(), before);
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn exact_promotion_rejects_the_original_resolved_epoch_without_refreshing_it() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs, "resolved-source-epoch", Arc::clone(&source), MemoryLazyWorkspaceStore::default(),
        ).await.expect("attach");
        let budget = finite_fixture_budget();
        let token = CancellationToken::new();
        let lookup = root.lookup_measured("/file.txt", budget, &token).await.expect("original lookup");
        let prior = lookup.work;
        let pinned_source = lookup.value.source.expect("resolved source pin");
        let before = root.workspace.head().await.expect("initial head").id();
        source.replace(Bytes::from_static(b"replacement"));
        assert_ne!(source.reference(), pinned_source);
        let failure = root.promote_resolved_with_permit_measured(
            "/file.txt", lookup.value, 64, IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted, prior, budget, &token,
        ).await.expect_err("resolved epoch must not be silently refreshed");
        assert!(matches!(failure.semantic_error(), LazyWorkspaceError::StaleSource));
        assert_eq!(failure.spent_work(), prior);
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
        assert_eq!(root.workspace.head().await.expect("unpublished head").id(), before);
    }

    /// A view bound before its shared source advanced (a refresh of the
    /// physical root) follows the new epoch on its next access instead of
    /// failing: every fork of a root reads through one source.
    #[tokio::test]
    async fn a_view_follows_its_shared_source_to_a_newer_epoch() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "follows",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        root.stat("/observed.txt").await.expect("observe");
        source.invalidate();
        assert_eq!(
            root.stat("/new.txt").await.expect("follows").logical_bytes,
            Some(6)
        );
        assert_eq!(
            root.snapshot().await.expect("snapshot").source,
            source.reference()
        );
    }

    #[tokio::test]
    async fn source_tombstones_list_only_removed_source_paths() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "tombstones",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        root.stat("/kept.txt").await.expect("observe kept");
        root.stat("/removed.txt").await.expect("observe removed");
        root.remove("/removed.txt")
            .await
            .expect("remove source path");
        root.remove("/also-removed.txt")
            .await
            .expect("remove unobserved source path");
        root.stat("/hidden.txt").await.expect("observe hidden");
        root.hide_source_path("/hidden.txt")
            .await
            .expect("hide source path");
        assert!(root.stat("/hidden.txt").await.is_err());
        let tombstones = root.source_tombstones(16).await.expect("tombstones");
        assert_eq!(
            tombstones
                .iter()
                .map(|(path, _)| path.as_str())
                .collect::<Vec<_>>(),
            ["/also-removed.txt", "/removed.txt"]
        );
        let current = root
            .current_source_version("/removed.txt")
            .await
            .expect("current version");
        assert_eq!(current, tombstones.get(1).map(|(_, version)| *version));
        assert!(matches!(
            root.source_tombstones(1).await,
            Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::Work(_))
        ));
    }

    #[tokio::test]
    async fn exactify_threads_cancellation_into_nested_source_work() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"content")));
        let root = LazyWorkspace::attach(
            &fs,
            "cancelled-exactify",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let cancellation = CancellationToken::new();
        source.cancel_next_page();

        let failure = root.exactify(WorkBudget::UNBOUNDED, &cancellation).await.err().expect("source cancellation");
        assert!(matches!(failure.semantic_error(), LazyWorkspaceError::Cancelled));
        assert!(failure.spent_work().backend_read_operations >= 1);
        assert!(cancellation.is_cancelled());
        assert_eq!(source.counts.pages.load(Ordering::Relaxed), 1);
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 0);
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn exactify_budget_exhaustion_cannot_publish_the_current_path() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"content")));
        let root = LazyWorkspace::attach(
            &fs,
            "budgeted-exactify",
            source,
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let before = root
            .workspace()
            .head()
            .await
            .expect("head before exactify")
            .id();
        let mut budget = WorkBudget::UNBOUNDED;
        budget.authority_records_appended = 0;

        assert!(matches!(
            root.exactify(budget, &CancellationToken::new()).await,
            Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::Workspace(_))
        ));
        assert_eq!(
            root.workspace()
                .head()
                .await
                .expect("head after rejection")
                .id(),
            before
        );
        assert!(matches!(
            root.workspace().stat("/file.txt").await,
            Err(WorkspaceError::NotFound)
        ));
    }

    #[tokio::test]
    async fn exactify_snapshot_rejects_before_destination_publication() {
        let fs = Fs::memory();
        let root = LazyWorkspace::attach(
            &fs,
            "snapshot-source",
            Arc::new(CountingSource::new(Bytes::from_static(b"content"))),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let snapshot = root.snapshot().await.expect("snapshot");

        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let failure = root.exactify_snapshot(
            snapshot, "cancelled-destination", IdempotencyKey::from_bytes([0x91; 16]),
            WorkBudget::UNBOUNDED, &cancelled,
        ).await.err().expect("pre-cancelled destination");
        assert!(matches!(failure.semantic_error(), LazyWorkspaceError::Cancelled));
        assert_eq!(failure.spent_work(), WorkCounters::default());
        assert!(fs.open_workspace("cancelled-destination").await.is_err());

        let failure = root.exactify_snapshot(
            snapshot, "budget-destination", IdempotencyKey::from_bytes([0x92; 16]),
            WorkBudget::default(), &CancellationToken::new(),
        ).await.err().expect("destination budget exhaustion");
        assert!(matches!(failure.semantic_error(), LazyWorkspaceError::Work(_)));
        failure.spent_work().verify(WorkBudget::default()).expect("admitted destination prefix");
        assert!(fs.open_workspace("budget-destination").await.is_err());
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn cancelled_native_page_projection_retries_the_original_held_page() {
        let directory = tempfile::tempdir().expect("native source");
        for name in ["a", "b", "c"] {
            std::fs::write(directory.path().join(name), name.as_bytes()).expect("source file");
        }
        let source = Arc::new(CancelAfterNativePage {
            inner: crate::demand::native::NativeDemandSource::open(
                directory.path(), crate::model::FilesystemProfile::Portable,
                crate::model::VolumeLimits::default(),
            ).await.expect("native provider"),
            cancel: AtomicBool::new(true),
        });
        let root = LazyWorkspace::attach(
            &Fs::memory(), "cancelled-page-projection", source, MemoryLazyWorkspaceStore::default(),
        ).await.expect("attach");
        let budget = finite_fixture_budget();
        let token = CancellationToken::new();
        let failure = root.list_directory_measured("/", None, 1, budget, &token, None, None)
            .await.err().expect("cancel after real raw page");
        assert!(matches!(failure.semantic_error(), LazyWorkspaceError::Cancelled));
        assert_eq!(failure.spent_work().source_entries_visited, 1);
        assert!(failure.spent_work().backend_read_operations > 0);
        failure.spent_work().verify(budget).expect("started native page prefix");

        let token = CancellationToken::new();
        let mut cursor = None;
        let mut names = Vec::new();
        loop {
            let receipt = root.list_directory_measured("/", cursor, 1, budget, &token, None, None)
                .await.expect("retry held reader");
            receipt.work.verify(budget).expect("bounded projected page");
            names.extend(receipt.value.entries.into_iter().map(|entry| entry.name.as_bytes().to_vec()));
            cursor = receipt.value.next;
            if cursor.is_none() { break; }
        }
        names.sort();
        assert_eq!(names, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn native_page_projection_allocation_denial_keeps_the_raw_page_recoverable() {
        let directory = tempfile::tempdir().expect("native source");
        for name in ["a", "b", "c"] {
            std::fs::write(directory.path().join(name), name.as_bytes()).expect("source file");
        }
        let profile = crate::model::FilesystemProfile::Portable;
        let limits = crate::model::VolumeLimits::default();
        let baseline = crate::demand::native::NativeDemandSource::open(directory.path(), profile, limits)
            .await.expect("baseline real provider");
        let source = Arc::new(crate::demand::native::NativeDemandSource::open(directory.path(), profile, limits)
            .await.expect("projected real provider"));
        let root = LazyWorkspace::attach(
            &Fs::memory(), "native-page-allocation-retry", source, MemoryLazyWorkspaceStore::default(),
        ).await.expect("attach");
        let budget = finite_fixture_budget();
        let token = CancellationToken::new();
        let path = root.namespace_path("/").expect("root path");
        let raw = baseline.list_page(baseline.reference(), &path, None, 1, budget, &token)
            .await.expect("real raw page");
        assert_eq!(raw.value.entries.len(), 1);
        raw.work.verify(budget).expect("bounded raw page");
        let mut denied = budget;
        denied.allocation_operations = raw.work.allocation_operations;
        let mut raw_page = raw.value;
        raw_page.publication.abort(budget).await.expect("abort baseline publication")
            .work.verify(budget).expect("actual baseline cleanup");
        let failure = root.list_directory_measured("/", None, 1, denied, &token, None, None)
            .await.err().expect("deny first consumer allocation after real raw page");
        assert!(matches!(failure.semantic_error(), LazyWorkspaceError::Work(_)));
        assert_eq!(failure.spent_work().allocation_operations, denied.allocation_operations);
        assert!(failure.spent_work().source_entries_visited > 0);
        failure.spent_work().verify(denied).expect("actual producer and abort prefix");

        let mut cursor = None;
        let mut names = Vec::new();
        loop {
            let receipt = root.list_directory_measured("/", cursor, 1, budget, &token, None, None)
                .await.expect("retry original native cursor");
            receipt.work.verify(budget).expect("bounded publication");
            names.extend(receipt.value.entries.into_iter().map(|entry| entry.name.as_bytes().to_vec()));
            cursor = receipt.value.next;
            if cursor.is_none() { break; }
        }
        names.sort();
        assert_eq!(names, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[tokio::test]
    async fn finite_nonempty_authored_page_projection_keeps_cursor_and_failure_prefix() {
        let root = LazyWorkspace::attach(
            &Fs::memory(), "finite-authored-page",
            Arc::new(CountingSource::new(Bytes::from_static(b"source")).with_directory_entries(0)),
            MemoryLazyWorkspaceStore::default(),
        ).await.expect("attach");
        root.workspace().write_text("/a", "a").await.expect("authored a");
        root.workspace().write_text("/b", "b").await.expect("authored b");
        let budget = finite_fixture_budget();
        let token = CancellationToken::new();
        let source = root.list_directory_measured("/", None, 1, budget, &token, None, None)
            .await.expect("empty source phase");
        assert!(source.value.entries.is_empty());
        let cursor = source.value.next.expect("authored phase");
        let baseline = root.list_directory_measured("/", Some(cursor.clone()), 1, budget, &token, None, None)
            .await.expect("nonempty authored phase");
        assert_eq!(baseline.value.entries.len(), 1);
        assert!(baseline.value.entries.iter().all(|entry| entry.authored));
        assert!(baseline.value.next.is_some());
        assert!(baseline.work.allocation_operations > 0);
        baseline.work.verify(budget).expect("actual authored receipt");
        let mut denied = budget;
        denied.peak_allocation_bytes = baseline.work.peak_allocation_bytes - 1;
        let failure = root.list_directory_measured("/", Some(cursor.clone()), 1, denied, &token, None, None)
            .await.err().expect("deny owned page overlap");
        assert!(matches!(failure.semantic_error(), LazyWorkspaceError::Work(_)));
        failure.spent_work().verify(denied).expect("actual admitted authored prefix");
        let retried = root.list_directory_measured("/", Some(cursor), 1, budget, &token, None, None)
            .await.expect("retry unchanged authored cursor");
        assert_eq!(retried.value.entries, baseline.value.entries);
    }

    #[tokio::test]
    async fn full_directory_page_peak_is_admitted_before_projection() {
        let fs = Fs::memory();
        let root = LazyWorkspace::attach(
            &fs,
            "page-memory",
            Arc::new(
                CountingSource::new(Bytes::from_static(b"content")).with_directory_entries(1_024),
            ),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let receipt = root
            .list_directory_measured(
                "/",
                None,
                1_024,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
                None,
                None,
            )
            .await
            .expect("measured page");
        assert_eq!(receipt.value.entries.len(), 1_024);
        assert!(receipt.work.allocation_operations >= 1);

        let mut insufficient = WorkBudget::UNBOUNDED;
        insufficient.peak_allocation_bytes = receipt.work.peak_allocation_bytes - 1;
        let failure = root.list_directory_measured(
            "/", None, 1_024, insufficient, &CancellationToken::new(), None, None,
        ).await.err().expect("full page allocation denial");
        assert!(matches!(failure.semantic_error(), LazyWorkspaceError::Work(_)));
        assert!(failure.spent_work().backend_read_operations > 0);
        failure.spent_work().verify(insufficient).expect("admitted page prefix");
    }

    #[tokio::test]
    async fn attach_open_and_fork_demand_no_source_work() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"abcdef")));
        let store = MemoryLazyWorkspaceStore::default();
        let root = LazyWorkspace::attach(&fs, "root", Arc::clone(&source), store.clone())
            .await
            .expect("attach");
        let reopened = LazyWorkspace::open(
            fs.open_workspace("root").await.expect("workspace"),
            Arc::clone(&source),
            store.clone(),
        )
        .await
        .expect("open");
        let child = reopened
            .fork("child", IdempotencyKey::from_bytes([5; 16]))
            .await
            .expect("fork");
        let first_snapshot = child.snapshot().await.expect("snapshot");
        let second_snapshot = child.snapshot().await.expect("repeat snapshot");
        let resumed_child = root
            .open_related(fs.open_workspace("child").await.expect("child workspace"))
            .await
            .expect("resume related child");
        let sibling = root
            .fork("sibling", IdempotencyKey::from_bytes([6; 16]))
            .await
            .expect("sibling fork");
        let resumed_sibling = resumed_child
            .open_related(
                fs.open_workspace("sibling")
                    .await
                    .expect("sibling workspace"),
            )
            .await
            .expect("constant-work branch switch");
        assert_eq!(
            sibling.snapshot().await.expect("sibling snapshot"),
            resumed_sibling
                .snapshot()
                .await
                .expect("resumed sibling snapshot")
        );

        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 0);
        assert_eq!(source.counts.pages.load(Ordering::Relaxed), 0);
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
        assert_eq!(
            child
                .state()
                .await
                .expect("child state")
                .parent_workspace_id,
            Some(root.workspace().id())
        );
        assert_eq!(first_snapshot, second_snapshot);
    }

    #[cfg(feature = "native-mount")]
    #[tokio::test]
    async fn mounted_promotion_cancels_midstream_without_installing_a_partial_candidate() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from(vec![
            0x5a;
            2 * 1024 * 1024
        ])));
        let root = LazyWorkspace::attach(
            &fs,
            "cancelled-mount-promotion",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let head = root.workspace().head().await.expect("head").id();
        let mut checkout = root
            .workspace()
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
            )
            .await
            .expect("checkout");
        let LazyLookup::Source(node) = root.lookup("/file.txt").await.expect("source") else {
            panic!("expected unresolved source file");
        };
        let cancellation = CancellationToken::new();
        source.cancel_next_range();
        assert!(matches!(
            root.stage_exact_into_checkout(
                &mut checkout,
                "/file.txt",
                root.source_file_id(&node),
                u64::MAX,
                &cancellation,
            )
            .await,
            Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::Cancelled)
        ));
        assert!(cancellation.is_cancelled());
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 1);
        assert_eq!(root.workspace().head().await.expect("head").id(), head);
        assert!(
            checkout
                .lookup_no_follow(
                    &root.namespace_path("/file.txt").expect("path"),
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .expect("candidate lookup")
                .value
                .record
                .is_none()
        );
    }

    #[cfg(feature = "native-mount")]
    #[tokio::test]
    async fn frozen_staging_captures_source_only_in_the_private_candidate() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs, "frozen-stage", Arc::clone(&source), MemoryLazyWorkspaceStore::default(),
        ).await.expect("attach");
        let token = CancellationToken::new();
        let budget = finite_fixture_budget();
        let snapshot = root.snapshot_measured(budget, &token).await.expect("frozen origin").value;
        let before = root.state().await.expect("original companion state");
        let lookup = root.lookup_at_snapshot_measured(snapshot, "/file.txt", budget, &token)
            .await.expect("frozen source lookup").value;
        let expected = root.direct_file_id_for_lookup(&lookup);
        let mut checkout = root.workspace().checkout(
            crate::model::GenerationSelector::Exact(snapshot.authored_generation),
            crate::model::CheckoutMode::tracking_transaction(),
        ).await.expect("private exact generation");
        let failure = root.stage_at_snapshot_into_checkout_measured(
            &mut checkout, snapshot, "/file.txt", expected, 6, WorkBudget::default(), &token,
        ).await.expect_err("zero future permit");
        assert_eq!(*failure.work, WorkCounters::default());
        assert_eq!(root.state().await.expect("unchanged rejected companion"), before);
        let staged = root.stage_at_snapshot_into_checkout_measured(
            &mut checkout, snapshot, "/file.txt", expected, 6, budget, &token,
        ).await.expect("capture frozen source in candidate");
        assert!(staged.value);
        assert_eq!(staged.work.source_bytes_read, 6);
        staged.work.verify(budget).expect("actual finite capture receipt");
        let record = checkout.lookup_no_follow(
            &root.namespace_path("/file.txt").expect("path"), budget, &token,
        ).await.expect("actual staged record").value.record.expect("captured record");
        assert_eq!(record.file_id, expected);
        assert_eq!(root.state().await.expect("unchanged companion"), before);
        assert_eq!(root.workspace().head().await.expect("unpublished head").id(), snapshot.authored_generation);
    }

    #[cfg(feature = "native-mount")]
    #[tokio::test]
    async fn mounted_promotion_stays_in_the_dirty_candidate_until_one_publication() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "staged-mount-promotion",
            source,
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let original_head = root.workspace().head().await.expect("head").id();
        let mut checkout = root
            .workspace()
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
            )
            .await
            .expect("mount checkout");
        let other = root.namespace_path("/other.txt").expect("other path");
        checkout
            .apply_authored_transaction(
                vec![AuthoredMutation::CreateFile {
                    path: other.clone(),
                    bytes: Bytes::from_static(b"dirty"),
                    metadata: FileMetadata::default(),
                }],
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .expect("dirty mounted write");
        let LazyLookup::Source(node) = root.lookup("/file.txt").await.expect("source") else {
            panic!("expected unresolved source file");
        };
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(matches!(
            root.stage_exact_into_checkout(
                &mut checkout,
                "/file.txt",
                root.source_file_id(&node),
                u64::MAX,
                &cancelled,
            )
            .await,
            Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::Cancelled)
        ));
        assert!(
            root.stage_exact_into_checkout(
                &mut checkout,
                "/file.txt",
                root.source_file_id(&node),
                u64::MAX,
                &CancellationToken::new(),
            )
            .await
            .expect("stage source in dirty mount")
        );
        assert!(
            !root
                .stage_exact_into_checkout(
                    &mut checkout,
                    "/file.txt",
                    root.source_file_id(&node),
                    u64::MAX,
                    &CancellationToken::new(),
                )
                .await
                .expect("already staged source identity")
        );
        assert_eq!(
            root.workspace()
                .head()
                .await
                .expect("unpublished head")
                .id(),
            original_head
        );
        let file = root.namespace_path("/file.txt").expect("file path");
        for path in [&other, &file] {
            assert!(
                checkout
                    .lookup_no_follow(path, WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .expect("candidate lookup")
                    .value
                    .record
                    .is_some()
            );
        }
        checkout
            .commit(
                crate::OperationId::new(),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .expect("publish both changes once");
        assert_ne!(
            root.workspace().head().await.expect("published head").id(),
            original_head
        );
    }

    #[cfg(feature = "native-mount")]
    #[tokio::test]
    async fn mounted_alias_promotions_preserve_source_hard_links_before_publication() {
        let fs = Fs::memory();
        let mut source = CountingSource::new(Bytes::from_static(b"shared"));
        source.node.link_count = Some(2);
        let root = LazyWorkspace::attach(
            &fs,
            "staged-hard-link-promotions",
            Arc::new(source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let mut checkout = root
            .workspace()
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
            )
            .await
            .expect("mount checkout");
        for path in ["/a", "/b"] {
            let LazyLookup::Source(node) = root.lookup(path).await.expect("source alias") else {
                panic!("expected unresolved source alias");
            };
            assert!(
                root.stage_exact_into_checkout(
                    &mut checkout,
                    path,
                    root.source_file_id(&node),
                    u64::MAX,
                    &CancellationToken::new(),
                )
                .await
                .expect("stage source alias")
            );
        }
        let first = checkout
            .lookup_no_follow(
                &root.namespace_path("/a").expect("first path"),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .expect("first alias")
            .value
            .record
            .expect("first record");
        let second = checkout
            .lookup_no_follow(
                &root.namespace_path("/b").expect("second path"),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .expect("second alias")
            .value
            .record
            .expect("second record");
        assert_eq!(first.file_id, second.file_id);
        assert_eq!(first.link_count, 2);
        assert_eq!(second.link_count, 2);
    }

    #[cfg(feature = "native-mount")]
    #[tokio::test]
    async fn mounted_directory_pages_include_unpublished_checkout_bindings() {
        let fs = Fs::memory();
        let root = LazyWorkspace::attach(
            &fs,
            "mounted-directory-candidate",
            Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let mut checkout = root
            .workspace()
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
            )
            .await
            .expect("mount checkout");
        checkout
            .apply_authored_transaction(
                vec![AuthoredMutation::CreateFile {
                    path: root.namespace_path("/new.txt").expect("new path"),
                    bytes: Bytes::from_static(b"new"),
                    metadata: FileMetadata::default(),
                }],
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .expect("unpublished file");
        let source_page = root
            .list_directory_in_checkout(&mut checkout, "/", None, 16, None)
            .await
            .map(|(page, _)| page)
            .expect("source page");
        assert_eq!(source_page.entries.len(), 1);
        assert!(!source_page.entries[0].authored);
        let authored_page = root
            .list_directory_in_checkout(&mut checkout, "/", source_page.next, 16, None)
            .await
            .map(|(page, _)| page)
            .expect("authored page");
        assert_eq!(authored_page.entries.len(), 1);
        assert!(authored_page.entries[0].authored);
        assert_eq!(authored_page.entries[0].name.as_bytes(), b"new.txt");
        assert!(authored_page.next.is_none());
    }

    #[cfg(all(
        feature = "native-mount",
        feature = "native-watch",
        not(target_arch = "wasm32")
    ))]
    #[tokio::test]
    async fn mounted_replacement_directory_does_not_expose_obsolete_source_children() {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{FilesystemProfile, VolumeLimits};

        let directory = tempfile::tempdir().expect("temporary source");
        std::fs::create_dir(directory.path().join("d")).expect("source directory");
        std::fs::write(directory.path().join("d").join("old.txt"), b"old").expect("source child");
        let source = Arc::new(
            NativeDemandSource::open(
                directory.path(),
                FilesystemProfile::Portable,
                VolumeLimits::default(),
            )
            .await
            .expect("source"),
        );
        let fs = Fs::memory();
        let root = LazyWorkspace::attach(
            &fs,
            "mounted-directory-replacement",
            source,
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let mut checkout = root
            .workspace()
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
            )
            .await
            .expect("mount checkout");
        assert_eq!(
            root.list_directory_in_checkout(&mut checkout, "/d", None, 16, None)
                .await
                .expect("source directory")
                .0
                .entries
                .len(),
            1
        );
        checkout
            .apply_authored_transaction(
                vec![AuthoredMutation::CreateDirectory {
                    path: root.namespace_path("/d").expect("replacement path"),
                    metadata: FileMetadata::default(),
                }],
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .expect("replace with empty authored directory");
        let page = root
            .list_directory_in_checkout(&mut checkout, "/d", None, 16, None)
            .await
            .map(|(page, _)| page)
            .expect("replacement directory");
        assert!(page.entries.is_empty());
        assert!(page.next.is_none());
    }

    #[tokio::test]
    async fn source_only_remove_does_not_advance_the_authored_head() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "source-only-remove-effect",
            source,
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let original_head = root.workspace().head().await.expect("head").id();
        let LazyLookup::Source(node) = root.lookup("/file.txt").await.expect("source") else {
            panic!("expected source file");
        };
        root.remove_if("/file.txt", Some(root.source_file_id(&node)))
            .await
            .expect("source-only tombstone");
        assert_eq!(
            root.workspace().head().await.expect("unchanged head").id(),
            original_head
        );
        assert!(matches!(
            root.lookup("/file.txt").await,
            Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::NotFound)
        ));
    }

    #[cfg(feature = "native-mount")]
    #[tokio::test]
    async fn mounted_whiteout_follows_the_checkout_publication_decision() {
        let fs = Fs::memory();
        let root = LazyWorkspace::attach(
            &fs,
            "mounted-whiteout-publication",
            Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let paths = vec!["/file.txt".to_owned()];
        let aborted = IdempotencyKey::new();
        root.prepare_mount_removals(&paths, &[], aborted)
            .await
            .expect("prepare uncommitted whiteout");
        assert!(!root.finish_mount_removals().await.expect("rollback"));
        assert!(matches!(
            root.lookup("/file.txt").await.expect("source remains"),
            LazyLookup::Source(_)
        ));

        let committed = IdempotencyKey::new();
        root.prepare_mount_removals(&paths, &[], committed)
            .await
            .expect("prepare committed whiteout");
        let mut checkout = root
            .workspace()
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
            )
            .await
            .expect("checkout");
        assert!(matches!(
            checkout
                .commit_with_permit_even_if_clean(
                    committed.operation_id(),
                    crate::PublicationPermit::Unrestricted,
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .expect("publish unchanged authored root")
                .value,
            crate::CheckoutCommitOutcome::Committed { .. }
        ));
        assert!(root.finish_mount_removals().await.expect("finalize"));
        assert!(matches!(
            root.lookup("/file.txt").await,
            Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::NotFound)
        ));
    }

    /// A mounted removal shadows an identity whose content only an
    /// unpublished checkout staged; publication then flushes a generation
    /// without it. The shadow must never outlive that content across a
    /// power loss, which drops everything staged and unflushed.
    #[cfg(all(feature = "native-mount", feature = "local", any(unix, windows)))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_shadowed_identity_keeps_its_content_across_power_loss() {
        let directory = tempfile::tempdir().expect("tempdir");
        let fs_root = directory.path().join("fs");
        let state_root = directory.path().join("state");
        let content = Bytes::from(vec![0x5a; 256 * 1024]);
        let cancellation = CancellationToken::new();
        let removal = IdempotencyKey::new();
        let path = NamespacePath::new(
            vec![LogicalName::new(NameEncoding::Utf8, b"removed.bin".to_vec(), 255).expect("name")],
            crate::model::VolumeLimits::default(),
        )
        .expect("path");
        let shadowed;
        {
            let fs = Fs::local(crate::LocalOptions::new(&fs_root))
                .await
                .expect("fs");
            let released = fs.local_root_release_barrier().expect("release barrier");
            let root = LazyWorkspace::attach(
                &fs,
                "power-loss-shadow",
                Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
                crate::core_state::LocalCoreStateStore::new(&state_root),
            )
            .await
            .expect("attach");
            // A mounted write stages content in the checkout, then the file
            // is removed before any publication, as `rm` after a write does.
            let mut checkout = root
                .workspace()
                .checkout(
                    crate::model::GenerationSelector::Head,
                    crate::model::CheckoutMode::tracking_transaction(),
                )
                .await
                .expect("checkout");
            checkout
                .create_file(
                    path.clone(),
                    content.clone(),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
                .expect("stage content");
            let identity = checkout
                .lookup_no_follow_with_metadata(&path, WorkBudget::UNBOUNDED, &cancellation)
                .await
                .expect("lookup")
                .value
                .expect("staged file");
            checkout
                .remove(path.clone(), None, WorkBudget::UNBOUNDED, &cancellation)
                .await
                .expect("remove");
            root.prepare_mount_removals(&[], &[(identity.record, identity.metadata)], removal)
                .await
                .expect("shadow the removed identity");
            checkout
                .commit_with_permit_even_if_clean(
                    removal.operation_id(),
                    crate::PublicationPermit::Unrestricted,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
                .expect("publish without the removed file");
            assert!(root.finish_mount_removals().await.expect("finish"));
            shadowed = identity.record;
            // Power loss: nothing staged survives the engine.
            drop((checkout, root, fs));
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                while !released.is_released() {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("release");
        }
        let fs = Fs::local(crate::LocalOptions::new(&fs_root))
            .await
            .expect("reopen");
        let reopened = LazyWorkspace::open(
            fs.open_workspace("power-loss-shadow")
                .await
                .expect("workspace"),
            Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
            crate::core_state::LocalCoreStateStore::new(&state_root),
        )
        .await
        .expect("reopen lazy");
        let state = reopened.state().await.expect("state");
        let shadow = reopened
            .shadow_record_measured(
                state.shadows,
                shadowed.file_id,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .expect("shadow lookup")
            .value
            .expect("the removed identity stays shadowed");
        let read = reopened
            .workspace()
            .detached_record(shadow.0)
            .read_range(
                crate::ByteRange {
                    offset: 0,
                    length: u64::try_from(content.len()).expect("length"),
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .expect("the shadowed identity's content survived");
        assert_eq!(read.value.bytes, content);
    }

    #[tokio::test]
    async fn exact_observation_is_shared_and_content_is_range_demanded() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"abcdef")));
        let store = MemoryLazyWorkspaceStore::default();
        let root = LazyWorkspace::attach(&fs, "root", Arc::clone(&source), store)
            .await
            .expect("attach");

        assert_eq!(
            root.stat("/deep/file.txt")
                .await
                .expect("stat")
                .logical_bytes,
            Some(6)
        );
        assert_eq!(
            root.stat("/deep/file.txt")
                .await
                .expect("cached stat")
                .logical_bytes,
            Some(6)
        );
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 1);
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
        assert_eq!(
            root.read_range("/deep/file.txt", 2, 3)
                .await
                .expect("range"),
            Bytes::from_static(b"cde")
        );
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 1);
        assert_eq!(source.counts.pages.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn inspection_is_transient_and_resolved_reads_pin_the_source_version() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"before")));
        let root = LazyWorkspace::attach(
            &fs,
            "transient-inspection",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let before = root.state().await.expect("state before inspection");

        let (lookup, source_reference) = root
            .inspect_unauthored("/file.txt", None)
            .await
            .expect("inspect source file");
        let LazyLookup::Source(node) = lookup else {
            panic!("inspection must resolve the source file");
        };
        assert_eq!(root.state().await.expect("state after inspection"), before);

        source.replace(Bytes::from_static(b"after"));
        assert_eq!(
            root.open_source_file(
                "/file.txt",
                source_reference.expect("source reference"),
                node,
            )
            .await
            .expect("open pinned version")
            .read_range(0, 6, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .expect("read pinned version")
            .value.bytes,
            Bytes::from_static(b"before")
        );

        root.rebind_source().await.expect("rebind source");
        let LazyLookup::Source(rebound) = root.inspect("/file.txt").await.expect("reinspect")
        else {
            panic!("rebound inspection must resolve the source file");
        };
        assert_ne!(rebound.version, node.version);
    }

    #[tokio::test]
    async fn independent_alias_promotions_preserve_source_hard_links() {
        let fs = Fs::memory();
        let mut source = CountingSource::new(Bytes::from_static(b"shared"));
        source.node.link_count = Some(2);
        source.node.metadata.posix_mode = Some(0o100640);
        source.node.metadata.modified_ns = Some(123_456);
        let source = Arc::new(source);
        let root = LazyWorkspace::attach(
            &fs,
            "hard-link-promotions",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");

        root.promote("/a", 64, IdempotencyKey::from_bytes([0x71; 16]))
            .await
            .expect("promote first alias");
        root.promote("/b", 64, IdempotencyKey::from_bytes([0x72; 16]))
            .await
            .expect("promote second alias");
        let first = root.workspace().stat("/a").await.expect("first stat");
        let second = root.workspace().stat("/b").await.expect("second stat");
        assert_eq!(first.file_id, second.file_id);
        assert_eq!(first.file_id, root.source_file_id(&source.node));
        assert_eq!(first.link_count, 2);
        assert_eq!(second.link_count, 2);
        assert_eq!(first.metadata.posix_mode, Some(0o100640));
        assert_eq!(second.metadata.modified_ns, Some(123_456));

        root.write("/a", Bytes::from_static(b"changed"))
            .await
            .expect("mutate alias");
        assert_eq!(
            root.workspace()
                .read("/b", 64)
                .await
                .expect("read other alias")
                .as_ref(),
            b"changed"
        );
    }

    #[tokio::test]
    async fn unresolved_alias_with_unknown_link_count_reads_latest_authored_identity() {
        let fs = Fs::memory();
        let mut source = CountingSource::new(Bytes::from_static(b"source"));
        source.node.link_count = None;
        let source = Arc::new(source);
        let root = LazyWorkspace::attach(
            &fs,
            "hard-link-late-alias",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");

        root.promote("/a", 64, IdempotencyKey::from_bytes([0x73; 16]))
            .await
            .expect("promote first alias");
        root.write("/a", Bytes::from_static(b"changed"))
            .await
            .expect("mutate first alias");
        let source_reads = source.counts.ranges.load(Ordering::Relaxed);
        assert_eq!(
            root.read("/b", 64).await.expect("read alias").as_ref(),
            b"changed"
        );
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), source_reads);

        root.promote("/b", 64, IdempotencyKey::from_bytes([0x74; 16]))
            .await
            .expect("promote late alias");
        let first = root.workspace().stat("/a").await.expect("first stat");
        let second = root.workspace().stat("/b").await.expect("second stat");
        assert_eq!(first.file_id, second.file_id);
        assert_eq!(first.file_id, root.source_file_id(&source.node));
        assert_eq!(
            root.read("/b", 64)
                .await
                .expect("read promoted alias")
                .as_ref(),
            b"changed"
        );
    }

    #[tokio::test]
    async fn identity_shadow_survives_last_promoted_alias_removal() {
        let fs = Fs::memory();
        let mut source = CountingSource::new(Bytes::from_static(b"source"));
        source.node.link_count = Some(2);
        let source = Arc::new(source);
        let store = MemoryLazyWorkspaceStore::default();
        let root =
            LazyWorkspace::attach(&fs, "hard-link-shadow", Arc::clone(&source), store.clone())
                .await
                .expect("attach");

        root.write("/a", Bytes::from_static(b"changed"))
            .await
            .expect("direct identity-preserving write");
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
        root.remove("/a").await.expect("remove promoted alias");
        assert_eq!(
            root.read("/b", 64)
                .await
                .expect("read shadow alias")
                .as_ref(),
            b"changed"
        );

        let reopened = LazyWorkspace::open(
            fs.open_workspace("hard-link-shadow")
                .await
                .expect("workspace"),
            source,
            store,
        )
        .await
        .expect("reopen");
        assert_eq!(
            reopened
                .read("/b", 64)
                .await
                .expect("reopened shadow")
                .as_ref(),
            b"changed"
        );
    }

    #[tokio::test]
    async fn remove_if_rejects_a_stale_source_identity() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "stale-remove",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let stale = FileId::from_bytes([0xff; 16]);
        assert!(matches!(
            root.remove_if("/file.txt", Some(stale)).await,
            Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::StaleIdentity)
        ));
        assert!(root.stat("/file.txt").await.is_ok());
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn metadata_inspection_does_not_stale_a_native_directory_cursor() {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{FilesystemProfile, VolumeLimits};

        let directory = tempfile::tempdir().expect("temporary source");
        std::fs::write(directory.path().join("a.txt"), b"a").expect("first file");
        std::fs::write(directory.path().join("b.txt"), b"b").expect("second file");
        let source = Arc::new(
            NativeDemandSource::open(
                directory.path(),
                FilesystemProfile::Portable,
                VolumeLimits::default(),
            )
            .await
            .expect("source"),
        );
        let fs = Fs::memory();
        let root = LazyWorkspace::attach(
            &fs,
            "paged-source",
            source,
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let first = root.list_directory("/", None, 1).await.expect("first page");
        let name = std::str::from_utf8(first.entries[0].name.as_bytes()).expect("portable name");
        root.inspect(&format!("/{name}"))
            .await
            .expect("entry metadata");
        let second = root
            .list_directory("/", first.next, 1)
            .await
            .expect("second page remains valid");
        assert_eq!(second.entries.len(), 1);
    }

    #[tokio::test]
    async fn unresolved_aliases_have_one_stable_identity_and_dense_seek_needs_no_content() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"abcdef")));
        let root = LazyWorkspace::attach(
            &fs,
            "root",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let first = match root.lookup("/first").await.expect("first") {
            LazyLookup::Source(node) => node,
            LazyLookup::Authored { .. } | LazyLookup::Shadow { .. } => {
                panic!("source lookup expected")
            }
        };
        let second = match root.lookup("/second").await.expect("second") {
            LazyLookup::Source(node) => node,
            LazyLookup::Authored { .. } | LazyLookup::Shadow { .. } => {
                panic!("source lookup expected")
            }
        };
        assert_eq!(root.source_file_id(&first), root.source_file_id(&second));
        assert_eq!(
            root.seek("/first", 2, LazySeekTarget::Data)
                .await
                .expect("seek data"),
            Some(2)
        );
        assert_eq!(
            root.seek("/first", 2, LazySeekTarget::Hole)
                .await
                .expect("seek hole"),
            Some(6)
        );
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn promotion_demands_only_the_selected_file_and_is_idempotent() {
        let fs = Fs::memory();
        let bytes = Bytes::from(vec![b'x'; 1024 * 1024 + 17]);
        let source = Arc::new(CountingSource::new(bytes.clone()));
        let root = LazyWorkspace::attach(
            &fs,
            "root",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let key = IdempotencyKey::from_bytes([21; 16]);

        let first = root
            .promote("/deep/file.txt", bytes.len() as u64, key)
            .await
            .expect("promote");
        assert!(matches!(first, TransactionCommit::Committed(_)));
        assert_eq!(
            root.workspace()
                .read("/deep/file.txt", bytes.len() as u64)
                .await
                .expect("authored content"),
            bytes
        );
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 1);
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 2);
        assert_eq!(source.counts.pages.load(Ordering::Relaxed), 0);

        let second = root
            .promote("/deep/file.txt", 0, key)
            .await
            .expect("idempotent promotion");
        assert!(matches!(second, TransactionCommit::AlreadyCommitted(_)));
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 1);
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 2);
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn promoted_source_directory_keeps_its_identity_and_lazy_children() {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{FilesystemProfile, VolumeLimits};

        let directory = tempfile::tempdir().expect("temporary source");
        std::fs::create_dir(directory.path().join("repo")).expect("source directory");
        std::fs::write(
            directory.path().join("repo/HEAD"),
            b"ref: refs/heads/main\n",
        )
        .expect("source child");
        let source = Arc::new(
            NativeDemandSource::open(
                directory.path(),
                FilesystemProfile::Portable,
                VolumeLimits::default(),
            )
            .await
            .expect("source"),
        );
        let root = LazyWorkspace::attach(
            &Fs::memory(),
            "directory-identity",
            source,
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let source_node = match root.lookup("/repo").await.expect("source directory") {
            LazyLookup::Source(node) => node,
            LazyLookup::Authored { .. } | LazyLookup::Shadow { .. } => {
                panic!("expected source directory")
            }
        };
        let source_id = root.source_file_id(&source_node);

        root.promote("/repo", 0, IdempotencyKey::from_bytes([0x52; 16]))
            .await
            .expect("promote directory");

        let promoted = root.lookup("/repo").await.expect("promoted directory");
        assert_eq!(
            root.stable_file_id_for_lookup("/repo", &promoted)
                .await
                .expect("stable projection identity"),
            source_id
        );
        assert_eq!(
            root.read("/repo/HEAD", 64).await.expect("lazy child"),
            Bytes::from_static(b"ref: refs/heads/main\n")
        );
    }

    #[tokio::test]
    async fn exact_promotion_accepts_an_identity_preserving_direct_write() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "root",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let source_node = match root.lookup("/file.txt").await.expect("source lookup") {
            LazyLookup::Source(node) => node,
            LazyLookup::Authored { .. } | LazyLookup::Shadow { .. } => {
                panic!("expected source node")
            }
        };
        let expected_source = root.source_file_id(&source_node);
        root.write("/file.txt", Bytes::from_static(b"replacement"))
            .await
            .expect("authored replacement");

        root.promote_exact(
            "/file.txt",
            expected_source,
            64,
            IdempotencyKey::from_bytes([0x73; 16]),
        )
        .await
        .expect("identity-preserving write is already promoted");
        assert_eq!(
            root.read("/file.txt", 64)
                .await
                .expect("replacement remains"),
            Bytes::from_static(b"replacement")
        );
    }

    #[tokio::test]
    async fn authored_state_and_tombstones_mask_the_source() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "root",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");

        root.write("/file.txt", Bytes::from_static(b"authored"))
            .await
            .expect("write");
        assert_eq!(
            root.read("/file.txt", 32).await.expect("authored read"),
            Bytes::from_static(b"authored")
        );
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 1);
        root.remove("/file.txt").await.expect("remove");
        assert!(matches!(
            root.stat("/file.txt").await,
            Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::NotFound)
        ));
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn lazy_lookup_scalar_projection_does_not_report_promotion_as_modification() {
        let mut source = CountingSource::new(Bytes::from_static(b"source"));
        source.node.metadata.posix_mode = Some(0o100640);
        source.node.metadata.posix_uid = Some(42);
        source.node.metadata.modified_ns = Some(123_456);
        let root = LazyWorkspace::attach(
            &Fs::memory(), "lazy-semantic-projection", Arc::new(source), MemoryLazyWorkspaceStore::default(),
        ).await.expect("attach");
        let before = root.inspect("/file.txt").await.expect("original source metadata");
        root.promote("/file.txt", 64, IdempotencyKey::from_bytes([0x77; 16]))
            .await.expect("representation-only promotion");
        let after = root.inspect("/file.txt").await.expect("authored metadata");
        assert!(!before.stat().authored);
        assert!(after.stat().authored);
        assert_eq!(before.stat().kind, after.stat().kind);
        assert_eq!(before.stat().logical_bytes, after.stat().logical_bytes);
        assert_eq!(before.metadata(), after.metadata());
    }

    #[tokio::test]
    async fn frozen_cursor_codec_preserves_authored_continuation_and_failed_work() {
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &Fs::memory(), "frozen-cursor-codec", source, MemoryLazyWorkspaceStore::default(),
        ).await.expect("attach");
        root.write("/a", Bytes::from_static(b"a")).await.expect("first authored");
        root.write("/b", Bytes::from_static(b"b")).await.expect("second authored");
        let snapshot = root.snapshot().await.expect("freeze");
        let budget = finite_fixture_budget();
        let token = CancellationToken::new();
        let source = root.list_directory_at_snapshot_measured(snapshot, "/", None, 1, budget, &token)
            .await.expect("source page");
        let authored = root.list_directory_at_snapshot_measured(snapshot, "/", source.value.next, 1, budget, &token)
            .await.expect("first authored page");
        let cursor = authored.value.next.expect("authored continuation");
        let encoded = cursor.encode_measured(budget, &token).expect("bounded canonical encoding");
        encoded.work.verify(budget).expect("bounded encoding work");
        let decoded = LazyDirectoryCursor::decode_measured(
            &encoded.value, root.workspace().profile(), root.workspace().limits(), budget, &token,
        ).expect("bounded canonical decoding");
        decoded.work.verify(budget).expect("bounded decoding work");
        assert_eq!(decoded.value, cursor);
        let next = root.list_directory_at_snapshot_measured(snapshot, "/", Some(decoded.value), 1, budget, &token)
            .await.expect("resume original authored cursor");
        assert_eq!(next.value.entries[0].name.as_bytes(), b"b");
        let mut malformed = encoded.value.clone();
        malformed.push(0);
        let failure = LazyDirectoryCursor::decode_measured(
            &malformed, root.workspace().profile(), root.workspace().limits(), budget, &token,
        ).expect_err("reject trailing wire bytes");
        assert!(matches!(failure.error.semantic_error(), LazyWorkspaceError::StaleCursor));
        assert_eq!(failure.work.source_bytes_read, malformed.len() as u64);
        assert!(failure.work.allocation_operations > 0);
        let mut denied = budget;
        denied.peak_allocation_bytes = decoded.work.peak_allocation_bytes - 1;
        let failure = LazyDirectoryCursor::decode_measured(
            &encoded.value, root.workspace().profile(), root.workspace().limits(), denied, &token,
        ).expect_err("deny decoded authored-name allocation");
        assert!(matches!(failure.error.semantic_error(), LazyWorkspaceError::Work(_)));
        failure.work.verify(denied).expect("retained admitted decoder prefix");
        let mut denied = budget;
        denied.allocation_operations = 0;
        let failure = cursor.encode_measured(denied, &token).expect_err("deny output allocation before creation");
        assert!(matches!(failure.error.semantic_error(), LazyWorkspaceError::Work(_)));
        assert_eq!(failure.work.allocation_operations, 0);
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn frozen_cursor_codec_resumes_original_native_provider_token() {
        let directory = tempfile::tempdir().expect("native source");
        for name in ["a", "b", "c"] {
            std::fs::write(directory.path().join(name), name.as_bytes()).expect("source file");
        }
        let source = Arc::new(crate::demand::native::NativeDemandSource::open(
            directory.path(), crate::model::FilesystemProfile::Portable, crate::model::VolumeLimits::default(),
        ).await.expect("native source"));
        let root = LazyWorkspace::attach(
            &Fs::memory(), "frozen-native-cursor-codec", source, MemoryLazyWorkspaceStore::default(),
        ).await.expect("attach");
        let snapshot = root.snapshot().await.expect("freeze");
        let budget = finite_fixture_budget();
        let token = CancellationToken::new();
        let mut cursor = None;
        let mut names = Vec::new();
        loop {
            let page = root.list_directory_at_snapshot_measured(snapshot, "/", cursor, 1, budget, &token)
                .await.expect("bounded original native page");
            names.extend(page.value.entries.iter().map(|entry| entry.name.as_bytes().to_vec()));
            cursor = match page.value.next {
                None => break,
                Some(next) => {
                    let encoded = next.encode_measured(budget, &token).expect("encode original token");
                    let decoded = LazyDirectoryCursor::decode_measured(
                        &encoded.value, root.workspace().profile(), root.workspace().limits(), budget, &token,
                    ).expect("decode original token");
                    assert_eq!(decoded.value, next);
                    Some(decoded.value)
                }
            };
        }
        names.sort();
        assert_eq!(names, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        assert_eq!(root.snapshot().await.expect("no observation or authored publication"), snapshot);
    }

    #[tokio::test]
    async fn frozen_symbolic_link_read_uses_original_authored_target() {
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &Fs::memory(), "frozen-authored-link", source, MemoryLazyWorkspaceStore::default(),
        ).await.expect("attach");
        let mut original = root.workspace().begin_transaction(IdempotencyKey::from_bytes([0x75; 16]))
            .await.expect("original transaction");
        original.create_symbolic_link("/link", Bytes::from_static(b"original")).await.expect("original target");
        original.commit().await.expect("publish original link");
        let snapshot = root.snapshot().await.expect("freeze original target");
        let mut advanced = root.workspace().begin_transaction(IdempotencyKey::from_bytes([0x76; 16]))
            .await.expect("advance transaction");
        advanced.remove("/link").await.expect("remove original binding");
        advanced.create_symbolic_link("/link", Bytes::from_static(b"advanced")).await.expect("new target");
        advanced.commit().await.expect("publish advanced target");
        let current = root.snapshot().await.expect("advanced current");
        let budget = finite_fixture_budget();
        let token = CancellationToken::new();
        let read = root.read_link_at_snapshot_measured(snapshot, "/link", budget, &token)
            .await.expect("original frozen target");
        assert_eq!(read.value.as_ref(), b"original");
        read.work.verify(budget).expect("bounded exact target");
        let failure = root.read_at_snapshot_measured(snapshot, "/link", 64, budget, &token)
            .await.expect_err("symbolic link is not a regular file");
        assert!(matches!(failure.error.semantic_error(), LazyWorkspaceError::NotRegularFile));
        assert!(failure.work.items_examined > 0);
        assert_eq!(root.snapshot().await.expect("readonly state unchanged"), current);
    }

    #[cfg(all(feature = "native-watch", unix, not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn frozen_native_symbolic_link_read_never_follows_or_hydrates() {
        let directory = tempfile::tempdir().expect("native source");
        std::os::unix::fs::symlink("missing-target", directory.path().join("link")).expect("dangling link");
        let source = Arc::new(crate::demand::native::NativeDemandSource::open(
            directory.path(), crate::model::FilesystemProfile::Portable, crate::model::VolumeLimits::default(),
        ).await.expect("native source"));
        let root = LazyWorkspace::attach(
            &Fs::memory(), "frozen-native-link", source, MemoryLazyWorkspaceStore::default(),
        ).await.expect("attach");
        let snapshot = root.snapshot().await.expect("freeze unresolved link");
        let budget = finite_fixture_budget();
        let token = CancellationToken::new();
        let lookup = root.lookup_at_snapshot_measured(snapshot, "/link", budget, &token)
            .await.expect("unresolved link metadata");
        assert_eq!(lookup.value.stat().kind, SourceNodeKind::SymbolicLink);
        assert!(!lookup.value.stat().authored);
        let read = root.read_link_at_snapshot_measured(snapshot, "/link", budget, &token)
            .await.expect("dangling link target without following");
        assert_eq!(read.value.as_ref(), b"missing-target");
        read.work.verify(budget).expect("bounded native target");
        assert_eq!(root.snapshot().await.expect("no hydration or observation publication"), snapshot);
    }

    #[tokio::test]
    async fn frozen_readonly_snapshot_retains_authored_overlay_and_unresolved_source() {
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &Fs::memory(), "frozen-readonly", Arc::clone(&source), MemoryLazyWorkspaceStore::default(),
        ).await.expect("attach");
        root.write("/authored.txt", Bytes::from_static(b"original")).await.expect("original authored");
        root.remove("/hidden.txt").await.expect("original tombstone");
        let snapshot = root.snapshot().await.expect("original frozen reference");
        root.write("/authored.txt", Bytes::from_static(b"advanced")).await.expect("advance authored");
        root.remove("/file.txt").await.expect("advance overlay");
        let current = root.snapshot().await.expect("advanced state");
        assert_ne!(current.authored_generation, snapshot.authored_generation);
        assert_ne!(current.overlay, snapshot.overlay);
        let budget = finite_fixture_budget();
        let token = CancellationToken::new();
        for (path, expected) in [("/authored.txt", b"original".as_slice()), ("/file.txt", b"source".as_slice())] {
            let read = root.read_at_snapshot_measured(snapshot, path, 64, budget, &token)
                .await.expect("frozen exact contents");
            assert_eq!(read.value.as_ref(), expected);
            read.work.verify(budget).expect("bounded frozen read");
            let range = root.read_range_at_snapshot_measured(snapshot, path, 2, 3, budget, &token)
                .await.expect("bounded original frozen range");
            assert_eq!(range.value.as_ref(), &expected[2..5]);
            range.work.verify(budget).expect("bounded frozen range work");
        }
        let source_lookup = root.lookup_at_snapshot_measured(snapshot, "/file.txt", budget, &token)
            .await.expect("original unresolved metadata");
        assert!(matches!(source_lookup.value, LazyLookup::Source(_)));
        let authored_lookup = root.lookup_at_snapshot_measured(snapshot, "/authored.txt", budget, &token)
            .await.expect("original authored metadata");
        assert!(matches!(authored_lookup.value, LazyLookup::Authored { .. }));
        let absent = root.read_at_snapshot_measured(snapshot, "/hidden.txt", 64, budget, &token)
            .await.expect_err("original tombstone");
        assert!(matches!(absent.error.semantic_error(), LazyWorkspaceError::NotFound));
        assert!(absent.work.items_examined > 0);
        let source_page = root.list_directory_at_snapshot_measured(snapshot, "/", None, 16, budget, &token)
            .await.expect("original unresolved source page");
        assert_eq!(source_page.value.entries.len(), 1);
        assert_eq!(source_page.value.entries[0].name.as_bytes(), b"file.txt");
        assert!(!source_page.value.entries[0].authored);
        source_page.work.verify(budget).expect("bounded source page");
        let changed_generation = LazySnapshotRef::new(
            snapshot.workspace_id, current.authored_generation, snapshot.source, snapshot.overlay, snapshot.shadows,
        );
        let wrong_cursor = root.list_directory_at_snapshot_measured(
            changed_generation, "/", source_page.value.next.clone(), 16, budget, &token,
        ).await.expect_err("continuation belongs to exact original snapshot");
        assert!(matches!(wrong_cursor.error.semantic_error(), LazyWorkspaceError::StaleCursor));
        let authored_page = root.list_directory_at_snapshot_measured(
            snapshot, "/", source_page.value.next, 16, budget, &token,
        ).await.expect("original authored page");
        assert_eq!(authored_page.value.entries.len(), 1);
        assert_eq!(authored_page.value.entries[0].name.as_bytes(), b"authored.txt");
        assert!(authored_page.value.entries[0].authored);
        authored_page.work.verify(budget).expect("bounded authored page");
        assert_eq!(root.snapshot().await.expect("readonly state unchanged"), current);
    }

    #[tokio::test]
    async fn frozen_authored_and_captured_shadow_reads_survive_unrelated_source_epoch() {
        let mut source = CountingSource::new(Bytes::from_static(b"source"));
        source.node.link_count = Some(2);
        let source = Arc::new(source);
        let root = LazyWorkspace::attach(
            &Fs::memory(), "frozen-authored-epoch", Arc::clone(&source), MemoryLazyWorkspaceStore::default(),
        ).await.expect("attach");
        root.write("/a", Bytes::from_static(b"retained")).await.expect("authored content");
        let authored_snapshot = root.snapshot().await.expect("freeze authored generation");
        let budget = finite_fixture_budget();
        let token = CancellationToken::new();
        let source_page = root.list_directory_at_snapshot_measured(authored_snapshot, "/", None, 16, budget, &token)
            .await.expect("original source phase");
        let authored_cursor = source_page.value.next.expect("held original authored phase");
        root.remove("/a").await.expect("retain detached identity shadow");
        assert!(matches!(root.lookup("/b").await.expect("capture alias identity"), LazyLookup::Shadow { .. }));
        let shadow_snapshot = root.snapshot().await.expect("freeze captured shadow");
        let binding = root.store.load_lazy_workspace(root.workspace().id()).await.expect("original companion state");
        let lookups = source.counts.lookups.load(Ordering::Relaxed);
        let ranges = source.counts.ranges.load(Ordering::Relaxed);
        let pages = source.counts.pages.load(Ordering::Relaxed);
        source.invalidate();
        for (snapshot, path) in [(authored_snapshot, "/a"), (shadow_snapshot, "/b")] {
            let lookup = root.lookup_at_snapshot_measured(snapshot, path, budget, &token)
                .await.expect("immutable authored metadata independent of live epoch");
            assert!(lookup.value.stat().authored);
            let read = root.read_at_snapshot_measured(snapshot, path, 64, budget, &token)
                .await.expect("immutable authored or captured shadow bytes");
            assert_eq!(read.value.as_ref(), b"retained");
            assert_eq!(read.work.source_bytes_read, 0);
            read.work.verify(budget).expect("bounded immutable content");
            let range = root.read_range_at_snapshot_measured(snapshot, path, 1, 3, budget, &token)
                .await.expect("immutable frozen range");
            assert_eq!(range.value.as_ref(), b"eta");
            assert_eq!(range.work.source_bytes_read, 0);
        }
        let authored_page = root.list_directory_at_snapshot_measured(
            authored_snapshot, "/", Some(authored_cursor), 16, budget, &token,
        ).await.expect("held authored-only continuation has no source participation");
        assert_eq!(authored_page.value.entries[0].name.as_bytes(), b"a");
        assert!(authored_page.value.entries[0].authored);
        let failure = root.read_at_snapshot_measured(shadow_snapshot, "/unresolved", 64, budget, &token)
            .await.expect_err("unobserved historical source still fails closed");
        assert!(matches!(failure.error.semantic_error(), LazyWorkspaceError::StaleSource));
        let failure = root.list_directory_at_snapshot_measured(authored_snapshot, "/", None, 16, budget, &token)
            .await.expect_err("initial source-directory phase cannot substitute new epoch");
        assert!(matches!(failure.error.semantic_error(), LazyWorkspaceError::StaleSource));
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), lookups);
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), ranges);
        assert_eq!(source.counts.pages.load(Ordering::Relaxed), pages);
        assert_eq!(root.store.load_lazy_workspace(root.workspace().id()).await.expect("readonly companion state"), binding);
    }

    #[tokio::test]
    async fn frozen_readonly_snapshot_rejects_epoch_and_reference_conflicts() {
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &Fs::memory(), "frozen-conflict", Arc::clone(&source), MemoryLazyWorkspaceStore::default(),
        ).await.expect("attach");
        let snapshot = root.snapshot().await.expect("freeze");
        let budget = finite_fixture_budget();
        let token = CancellationToken::new();
        let mut forged = snapshot;
        forged.overlay = LazyOverlayId::from_bytes([0x71; 32]);
        let failure = root.read_at_snapshot_measured(forged, "/file.txt", 64, budget, &token)
            .await.expect_err("unauthenticated snapshot id");
        assert!(matches!(failure.error.semantic_error(), LazyWorkspaceError::StaleSource));
        assert!(failure.work.bytes_hashed > 0);
        let replaced_identity = LazySnapshotRef::new(
            snapshot.workspace_id, snapshot.authored_generation,
            SourceReference { identity: [0x73; 16], epoch: snapshot.source.epoch },
            snapshot.overlay, snapshot.shadows,
        );
        let identity_failure = root.validate_snapshot_measured(replaced_identity, budget, &token)
            .await.expect_err("original physical source identity differs");
        assert!(matches!(identity_failure.error.semantic_error(), LazyWorkspaceError::StaleSource));
        source.invalidate();
        let validated = root.validate_snapshot_measured(snapshot, budget, &token)
            .await.expect("original immutable metadata survives live epoch advancement");
        validated.work.verify(budget).expect("bounded retained root authentication");
        assert!(validated.work.bytes_hashed > failure.work.bytes_hashed);
        let missing_root = LazySnapshotRef::new(
            snapshot.workspace_id, snapshot.authored_generation, snapshot.source,
            LazyOverlayId::from_bytes([0x72; 32]), snapshot.shadows,
        );
        let invalid = root.validate_snapshot_measured(missing_root, budget, &token)
            .await.expect_err("digest alone cannot authenticate absent overlay root");
        assert!(matches!(invalid.error.semantic_error(), LazyWorkspaceError::Store(_)));
        assert!(invalid.work.items_examined > 0);
        invalid.work.verify(budget).expect("retained generation and missing root prefix");
        let missing_shadow = LazySnapshotRef::new(
            snapshot.workspace_id, snapshot.authored_generation, snapshot.source,
            snapshot.overlay, LazyShadowId::from_bytes([0x74; 32]),
        );
        let invalid = root.validate_snapshot_measured(missing_shadow, budget, &token)
            .await.expect_err("digest alone cannot authenticate absent shadow root");
        assert!(matches!(invalid.error.semantic_error(), LazyWorkspaceError::Store(_)));
        invalid.work.verify(budget).expect("retained authored, overlay, and missing shadow prefix");
        let failure = root.read_at_snapshot_measured(snapshot, "/file.txt", 64, budget, &token)
            .await.expect_err("live epoch differs");
        assert!(matches!(failure.error.semantic_error(), LazyWorkspaceError::StaleSource));
        let failure = root.list_directory_at_snapshot_measured(snapshot, "/", None, 16, budget, &token)
            .await.expect_err("live epoch differs for listing");
        assert!(matches!(failure.error.semantic_error(), LazyWorkspaceError::StaleSource));
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 0);
        assert_eq!(source.counts.pages.load(Ordering::Relaxed), 0);
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn frozen_readonly_failures_retain_read_and_list_producer_work() {
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &Fs::memory(), "frozen-failures", Arc::clone(&source), MemoryLazyWorkspaceStore::default(),
        ).await.expect("attach");
        let snapshot = root.snapshot().await.expect("freeze");
        let budget = finite_fixture_budget();
        let token = CancellationToken::new();
        let too_large = root.read_at_snapshot_measured(snapshot, "/file.txt", 1, budget, &token)
            .await.expect_err("whole file bound");
        assert!(matches!(too_large.error.semantic_error(), LazyWorkspaceError::TooLarge));
        assert!(too_large.work.bytes_hashed > 0);
        assert!(too_large.work.items_examined > 0);
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
        source.cancel_on_range.store(true, Ordering::Relaxed);
        let cancelled = root.read_at_snapshot_measured(snapshot, "/file.txt", 64, budget, &token)
            .await.expect_err("cancel after content demand");
        assert!(matches!(cancelled.error.semantic_error(),
            LazyWorkspaceError::Cancelled | LazyWorkspaceError::Demand(DemandError::Cancelled)));
        assert!(cancelled.work.bytes_hashed > 0);
        assert_eq!(cancelled.work.source_bytes_read, 6);
        assert_eq!(cancelled.work.output_bytes, 0);
        cancelled.work.verify(budget).expect("retained cancelled prefix");
        let token = CancellationToken::new();
        let page = root.list_directory_at_snapshot_measured(snapshot, "/", None, 16, budget, &token)
            .await.expect("baseline page");
        let mut denied = budget;
        denied.peak_allocation_bytes = page.work.peak_allocation_bytes - 1;
        let failed = root.list_directory_at_snapshot_measured(snapshot, "/", None, 16, denied, &token)
            .await.expect_err("deny finite page allocation");
        assert!(matches!(failed.error.semantic_error(), LazyWorkspaceError::Work(_)));
        assert!(failed.work.bytes_hashed > 0);
        assert!(failed.work.allocation_operations > 0);
        failed.work.verify(denied).expect("admitted failed list prefix");
        assert_eq!(root.snapshot().await.expect("no readonly observation publication"), snapshot);
    }

    #[tokio::test]
    async fn source_epoch_can_rebind_without_enumeration() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "root",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        root.stat("/observed.txt").await.expect("observe");
        let before = root.snapshot().await.expect("snapshot before rebind");
        source.invalidate();
        let rebound = root.rebind_source().await.expect("rebind");
        assert_eq!(rebound.source, source.reference());
        assert_eq!(source.counts.pages.load(Ordering::Relaxed), 0);
        assert_eq!(
            root.stat("/observed.txt")
                .await
                .expect("cached")
                .logical_bytes,
            Some(6)
        );
        let after = root.snapshot().await.expect("snapshot after refresh");
        assert_ne!(after, before);
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn snapshot_exactification_uses_live_epoch_for_unresolved_paths() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "root",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let snapshot = root.snapshot().await.expect("snapshot before rebind");

        source.replace(Bytes::from_static(b"changed"));
        root.rebind_source().await.expect("rebind");
        let exact = root
            .exactify_snapshot(
                snapshot,
                "historical-exact",
                IdempotencyKey::from_bytes([23; 16]),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .expect("exactify historical snapshot");

        assert_eq!(
            exact
                .value
                .read("/file.txt", 32)
                .await
                .expect("unresolved file from live epoch"),
            Bytes::from_static(b"changed")
        );
    }

    #[tokio::test]
    async fn snapshot_exactification_keeps_observed_paths_on_the_pinned_epoch() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "root",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        root.stat("/file.txt").await.expect("observe old epoch");
        let snapshot = root.snapshot().await.expect("snapshot after observation");

        source.replace(Bytes::from_static(b"changed"));
        root.rebind_source().await.expect("rebind");
        let exact = root
            .exactify_snapshot(
                snapshot,
                "pinned-exact",
                IdempotencyKey::from_bytes([24; 16]),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .expect("exactify pinned observation");

        assert_eq!(
            exact
                .value
                .read("/file.txt", 32)
                .await
                .expect("observed file from pinned epoch"),
            Bytes::from_static(b"source")
        );
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn native_source_rebind_refreshes_an_observed_file() {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{FilesystemProfile, VolumeLimits};

        let directory = tempfile::tempdir().expect("temporary source");
        let path = directory.path().join("file.txt");
        std::fs::write(&path, b"before").expect("initial file");
        let source = Arc::new(
            NativeDemandSource::open(
                directory.path(),
                FilesystemProfile::Portable,
                VolumeLimits::default(),
            )
            .await
            .expect("source"),
        );
        let fs = Fs::memory();
        let root = LazyWorkspace::attach(
            &fs,
            "native-rebind",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        assert_eq!(
            root.read("/file.txt", 64).await.expect("initial read"),
            Bytes::from_static(b"before")
        );
        let before = root.snapshot().await.expect("snapshot before change");

        std::fs::write(&path, b"after").expect("external write");
        source.invalidate();
        root.rebind_source().await.expect("rebind source");

        assert_eq!(
            root.read("/file.txt", 64).await.expect("refreshed read"),
            Bytes::from_static(b"after")
        );
        assert_ne!(
            root.snapshot().await.expect("snapshot after change"),
            before
        );
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn exactify_is_bounded_and_returns_only_a_complete_generation() {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{FilesystemProfile, VolumeLimits};

        let directory = tempfile::tempdir().expect("temporary source");
        std::fs::create_dir(directory.path().join("nested")).expect("nested directory");
        std::fs::write(directory.path().join("a.txt"), b"alpha").expect("first file");
        std::fs::write(directory.path().join("nested").join("b.txt"), b"beta")
            .expect("second file");
        let source = Arc::new(
            NativeDemandSource::open(
                directory.path(),
                FilesystemProfile::Portable,
                VolumeLimits::default(),
            )
            .await
            .expect("source"),
        );
        let fs = Fs::memory();
        let root =
            LazyWorkspace::attach(&fs, "exactify", source, MemoryLazyWorkspaceStore::default())
                .await
                .expect("attach");
        let mut insufficient = WorkBudget::UNBOUNDED;
        insufficient.source_entries_visited = 1;
        assert!(matches!(
            root.exactify(insufficient, &CancellationToken::new()).await,
            Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::Work(_))
        ));

        let exact = root
            .exactify(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .expect("exact capture");
        assert_eq!(
            exact
                .value
                .read("/nested/b.txt", 16)
                .await
                .expect("captured file"),
            Bytes::from_static(b"beta")
        );
        assert_eq!(exact.work.source_entries_visited, 3);
        assert_eq!(
            exact.work.source_bytes_read, 9,
            "version-pinned demand is read once and measured by the blob builder"
        );
    }

    #[tokio::test]
    async fn exactify_subtree_preflights_lookup_budget_and_original_cancellation() {
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &Fs::memory(),
            "subtree-preflight",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let lookups = source.counts.lookups.load(Ordering::Relaxed);
        let mut budget = WorkBudget::UNBOUNDED;
        budget.backend_read_operations = 0;
        let failure = root.exactify_subtree_with_permit(
            "/selected", budget, &CancellationToken::new(), crate::PublicationPermit::Unrestricted,
        ).await.err().expect("deny source lookup IO");
        assert!(matches!(failure.semantic_error(), LazyWorkspaceError::Work(_)));
        assert_eq!(failure.spent_work().backend_read_operations, 0);
        failure.spent_work().verify(budget).expect("preflight prefix");
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let failure = root.exactify_subtree_with_permit(
            "/selected", WorkBudget::UNBOUNDED, &cancelled, crate::PublicationPermit::Unrestricted,
        ).await.err().expect("pre-cancelled subtree");
        assert!(matches!(failure.semantic_error(), LazyWorkspaceError::Cancelled));
        assert_eq!(failure.spent_work(), WorkCounters::default());
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), lookups);
        assert_eq!(source.counts.pages.load(Ordering::Relaxed), 0);
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn exactify_subtree_keeps_siblings_lazy() {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{FilesystemProfile, VolumeLimits};

        let directory = tempfile::tempdir().expect("temporary source");
        std::fs::create_dir(directory.path().join("hot")).expect("hot directory");
        std::fs::create_dir(directory.path().join("cold")).expect("cold directory");
        std::fs::write(directory.path().join("hot/file.txt"), b"hot").expect("hot file");
        std::fs::write(directory.path().join("cold/file.txt"), b"cold").expect("cold file");
        let source = Arc::new(
            NativeDemandSource::open(
                directory.path(),
                FilesystemProfile::Portable,
                VolumeLimits::default(),
            )
            .await
            .expect("source"),
        );
        let root = LazyWorkspace::attach(
            &Fs::memory(),
            "subtree-exactify",
            source,
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");

        let exact = root
            .exactify_subtree_with_permit(
                "/hot",
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
                crate::PublicationPermit::Unrestricted,
            )
            .await
            .expect("capture hot subtree");
        assert_eq!(
            exact
                .value
                .read("/hot/file.txt", 16)
                .await
                .expect("hot content"),
            Bytes::from_static(b"hot")
        );
        #[cfg(feature = "native-mount")]
        {
            let staged = tempfile::tempdir().expect("empty native stage");
            exact
                .value
                .materialize_path(
                    "/hot",
                    &crate::MaterializeOptions::native(staged.path()),
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .expect("materialize only pinned hot subtree");
            assert_eq!(
                std::fs::read(staged.path().join("hot/file.txt")).expect("staged hot file"),
                b"hot"
            );
            assert!(!staged.path().join("cold").exists());
        }
        assert!(matches!(
            exact.value.read("/cold/file.txt", 16).await,
            Err(WorkspaceError::NotFound)
        ));
        assert!(matches!(
            root.lookup("/cold/file.txt")
                .await
                .expect("cold remains lazy"),
            LazyLookup::Source(_)
        ));
        assert!(matches!(
            root.exactify_subtree_with_permit(
                "/hot/file.txt",
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
                crate::PublicationPermit::Unrestricted,
            )
            .await,
            Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::NotDirectory)
        ));
    }

    #[cfg(all(windows, feature = "native-watch"))]
    #[tokio::test]
    async fn source_page_batch_lookup_transcodes_windows_names_to_workspace_profile() {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{FilesystemProfile, VolumeLimits};

        let directory = tempfile::tempdir().expect("temporary source");
        std::fs::write(directory.path().join("café.txt"), b"source").expect("source file");
        let source = Arc::new(
            NativeDemandSource::open(
                directory.path(),
                FilesystemProfile::Windows,
                VolumeLimits::default(),
            )
            .await
            .expect("Windows source"),
        );
        let root = LazyWorkspace::attach(
            &Fs::memory(),
            "windows-source-portable-authored",
            source,
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach portable authored workspace");
        assert_eq!(root.workspace().profile(), FilesystemProfile::Portable);
        let exact = root
            .exactify(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .expect("exactify Windows source into portable authored workspace");
        assert_eq!(
            exact
                .value
                .read("/café.txt", 16)
                .await
                .expect("captured file"),
            Bytes::from_static(b"source")
        );
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn exactify_preserves_hard_links_across_batch_boundaries() {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{FilesystemProfile, VolumeLimits};

        let directory = tempfile::tempdir().expect("temporary source");
        let source_path = directory.path().join("a000");
        std::fs::write(&source_path, b"shared").expect("source file");
        for index in 1..520 {
            std::fs::hard_link(&source_path, directory.path().join(format!("a{index:03}")))
                .expect("source hard link");
        }
        let source = Arc::new(
            NativeDemandSource::open(
                directory.path(),
                FilesystemProfile::Portable,
                VolumeLimits::default(),
            )
            .await
            .expect("source"),
        );
        let root = LazyWorkspace::attach(
            &Fs::memory(),
            "hard-link-batches",
            source,
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let exact = root
            .exactify(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .expect("exactify all batches");
        let first = exact.value.stat("/a000").await.expect("first stat");
        let last = exact.value.stat("/a519").await.expect("last stat");
        assert_eq!(first.file_id, last.file_id);
        assert_eq!(last.link_count, 520);
    }

    #[tokio::test]
    async fn directory_pages_emit_authored_entries_once_after_source_phase() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "root",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        root.write("/file.txt", Bytes::from_static(b"authored"))
            .await
            .expect("write");

        let source_page = root
            .list_directory("/", None, 16)
            .await
            .expect("source phase");
        assert!(source_page.entries.is_empty());
        let authored_page = root
            .list_directory("/", source_page.next, 16)
            .await
            .expect("authored phase");
        assert_eq!(authored_page.entries.len(), 1);
        assert!(authored_page.entries[0].authored);
        assert!(authored_page.next.is_none());
    }

    #[tokio::test]
    async fn directory_cursor_rejects_overlay_changes_between_pages() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "root",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let first = root
            .list_directory("/", None, 16)
            .await
            .expect("first page");
        root.stat("/newly-observed.txt").await.expect("observe");

        assert!(matches!(
            root.list_directory("/", first.next, 16).await,
            Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::StaleCursor)
        ));
    }

    #[tokio::test]
    async fn failed_authored_remove_rolls_back_source_tombstone() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "root",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        root.write("/directory/file.txt", Bytes::from_static(b"authored"))
            .await
            .expect("write");
        let before = root.state().await.expect("state");

        assert!(matches!(
            root.remove("/directory").await,
            Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::Workspace(_))
        ));
        let after = root.state().await.expect("state after failure");
        assert_eq!(after.overlay, before.overlay);
        assert_eq!(after.shadows, before.shadows);
        assert!(after.pending_remove.is_none());
        assert!(root.stat("/directory").await.expect("directory").authored);
    }

    #[tokio::test]
    async fn open_recovers_remove_intent_from_authored_state() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let store = MemoryLazyWorkspaceStore::default();
        let root = LazyWorkspace::attach(&fs, "root", Arc::clone(&source), store.clone())
            .await
            .expect("attach");
        root.write("/file.txt", Bytes::from_static(b"authored"))
            .await
            .expect("write");
        let prior = root.state().await.expect("state");
        let LazyLookup::Authored { stat, .. } = root.lookup("/file.txt").await.expect("authored")
        else {
            panic!("expected authored file");
        };
        let record = root
            .workspace()
            .record_by_id(stat.file_id)
            .await
            .expect("authored record");
        let staged_shadows = root
            .insert_shadow(prior.shadows, stat.file_id, record, stat.metadata)
            .await
            .expect("staged identity shadow");
        assert_ne!(staged_shadows, prior.shadows);
        let tombstone = root
            .insert_overlay(
                prior.overlay,
                "/file.txt".to_owned(),
                LazyOverlayChange::Tombstone { removed: None },
            )
            .await
            .expect("tombstone");
        let first_key = IdempotencyKey::from_bytes([0x31; 16]);
        let pending = LazyWorkspaceState {
            revision: prior.revision + 1,
            overlay: tombstone,
            shadows: staged_shadows,
            pending_remove: Some(PendingLazyRemove {
                prior_overlay: prior.overlay,
                prior_shadows: prior.shadows,
                prepared_overlay: tombstone,
                kind: PendingLazyRemoveKind::Authored {
                    idempotency_key: first_key,
                },
            }),
            ..prior.clone()
        };
        assert!(
            store
                .compare_and_swap_lazy_workspace(root.workspace().id(), prior.revision, pending)
                .await
                .expect("prepare")
        );

        let reopened = LazyWorkspace::open(
            fs.open_workspace("root").await.expect("workspace"),
            Arc::clone(&source),
            store.clone(),
        )
        .await
        .expect("recover before physical remove");
        let recovered = reopened.state().await.expect("recovered state");
        assert_eq!(recovered.overlay, prior.overlay);
        assert_eq!(recovered.shadows, prior.shadows);
        assert!(recovered.pending_remove.is_none());

        let committed_key = IdempotencyKey::from_bytes([0x32; 16]);
        let prepared = LazyWorkspaceState {
            revision: recovered.revision + 1,
            overlay: tombstone,
            shadows: staged_shadows,
            pending_remove: Some(PendingLazyRemove {
                prior_overlay: recovered.overlay,
                prior_shadows: recovered.shadows,
                prepared_overlay: tombstone,
                kind: PendingLazyRemoveKind::Authored {
                    idempotency_key: committed_key,
                },
            }),
            ..recovered.clone()
        };
        assert!(
            store
                .compare_and_swap_lazy_workspace(
                    reopened.workspace().id(),
                    recovered.revision,
                    prepared
                )
                .await
                .expect("prepare second")
        );
        let mut removal = reopened
            .workspace()
            .begin_transaction(committed_key)
            .await
            .expect("removal transaction");
        removal.remove("/file.txt").await.expect("stage remove");
        assert!(matches!(
            removal.commit().await.expect("physical remove"),
            TransactionCommit::Committed(_)
        ));

        let finalized = LazyWorkspace::open(
            fs.open_workspace("root").await.expect("workspace"),
            Arc::clone(&source),
            store,
        )
        .await
        .expect("recover after physical remove");
        let final_state = finalized.state().await.expect("final state");
        assert_eq!(final_state.overlay, tombstone);
        assert_eq!(final_state.shadows, staged_shadows);
        assert!(matches!(
            finalized.stat("/file.txt").await,
            Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::NotFound)
        ));
    }

    #[tokio::test]
    async fn source_only_remove_recovery_keeps_tombstone_across_a_competing_create() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let store = MemoryLazyWorkspaceStore::default();
        let root = LazyWorkspace::attach(&fs, "root", Arc::clone(&source), store.clone())
            .await
            .expect("attach");
        root.stat("/file.txt").await.expect("observe source");
        let prior = root.state().await.expect("state");
        let tombstone = root
            .insert_overlay(
                prior.overlay,
                "/file.txt".to_owned(),
                LazyOverlayChange::Tombstone { removed: None },
            )
            .await
            .expect("tombstone");
        let pending = LazyWorkspaceState {
            revision: prior.revision + 1,
            overlay: tombstone,
            pending_remove: Some(PendingLazyRemove {
                prior_overlay: prior.overlay,
                prior_shadows: prior.shadows,
                prepared_overlay: tombstone,
                kind: PendingLazyRemoveKind::SourceOnly,
            }),
            ..prior.clone()
        };
        assert!(
            store
                .compare_and_swap_lazy_workspace(root.workspace().id(), prior.revision, pending)
                .await
                .expect("prepare")
        );
        root.workspace()
            .write("/file.txt", Bytes::from_static(b"authored"))
            .await
            .expect("competing authored create");

        let reopened = LazyWorkspace::open(
            fs.open_workspace("root").await.expect("workspace"),
            Arc::clone(&source),
            store,
        )
        .await
        .expect("recover source removal");
        assert_eq!(reopened.state().await.expect("state").overlay, tombstone);
        assert_eq!(
            reopened
                .read("/file.txt", 64)
                .await
                .expect("authored replacement"),
            Bytes::from_static(b"authored")
        );
        reopened.remove("/file.txt").await.expect("remove authored");
        assert!(matches!(
            reopened.stat("/file.txt").await,
            Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::NotFound)
        ));
    }

    #[tokio::test]
    async fn observation_index_stays_balanced_and_nodes_stay_constant_size() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let store = MemoryLazyWorkspaceStore::default();
        let root = LazyWorkspace::attach(&fs, "root", Arc::clone(&source), store.clone())
            .await
            .expect("attach");
        for index in 0..1_000 {
            root.stat(&format!("/paths/{index:04}.txt"))
                .await
                .expect("observe");
        }
        let root_id = root.state().await.expect("state").overlay;
        let state = store
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        fn depth(overlays: &BTreeMap<LazyOverlayId, LazyOverlay>, id: LazyOverlayId) -> usize {
            match &overlays[&id] {
                LazyOverlay::Empty => 0,
                LazyOverlay::Node { left, right, .. } => {
                    1 + depth(overlays, *left).max(depth(overlays, *right))
                }
            }
        }
        assert!(depth(&state.overlays, root_id) < 64);
        assert!(state.overlays.values().all(|overlay| {
            serde_json::to_vec(overlay)
                .expect("encode overlay node")
                .len()
                < 2_048
        }));
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 1_000);
    }

    #[test]
    fn million_path_exactify_capture_memory_is_exactly_admitted() {
        const PATHS: u64 = 1_000_000;

        fn capture(budget: WorkBudget) -> Result<(WorkCounters, u64), LazyWorkspaceError> {
            let mut paths = Vec::new();
            let mut retained = 0_u64;
            let mut work = WorkCounters::default();
            for index in 0..PATHS {
                let path = format!("/wide/{index:06}.txt");
                reserve_exactify_slot(&mut paths, &mut retained, &mut work, budget)?;
                let path = clone_exactify_string(&path, &mut retained, &mut work, budget)?;
                paths.push(path);
            }
            let expected = exactify_path_vector_bytes(&paths)?
                + paths
                    .iter()
                    .map(|path| u64::try_from(path.capacity()).unwrap_or(u64::MAX))
                    .sum::<u64>();
            assert_eq!(retained, expected);
            Ok((work, retained))
        }

        let (work, retained) = capture(WorkBudget::UNBOUNDED).expect("unbounded capture");
        assert!(work.peak_allocation_bytes >= retained);
        assert!(work.allocation_operations >= PATHS);
        let mut insufficient = WorkBudget::UNBOUNDED;
        insufficient.peak_allocation_bytes = work.peak_allocation_bytes - 1;
        assert!(matches!(
            capture(insufficient),
            Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::Work(_))
        ));
    }

    fn memory_prepared_origin(
        snapshot: LazySnapshotRef,
        config: VolumeConfig,
        workspace_id: WorkspaceId,
    ) -> LazyWorkspaceState {
        LazyWorkspaceState {
            schema_version: LAZY_STATE_SCHEMA,
            revision: 1,
            workspace_id,
            parent_workspace_id: Some(snapshot.workspace_id),
            fork_origin: Some(LazyForkOrigin {
                operation_id: crate::OperationId::from_bytes([0xc0; 16]),
                snapshot,
                config,
                phase: LazyForkPhase::Prepared,
            }),
            source: snapshot.source,
            overlay: snapshot.overlay,
            shadows: snapshot.shadows,
            pending_remove: None,
        }
    }

    #[tokio::test]
    async fn memory_first_origin_rejects_missing_parent_and_concurrent_source_changes() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let store = MemoryLazyWorkspaceStore::default();
        let root = LazyWorkspace::attach(&fs, "first-origin-parent", source, store.clone())
            .await.expect("parent");
        root.stat("/file.txt").await.expect("source observation");
        let parent = root.state().await.expect("parent binding");
        let snapshot = root.snapshot().await.expect("original snapshot");
        let prepared = memory_prepared_origin(
            snapshot, root.workspace().config(), WorkspaceId::from_bytes([0xc1; 16]),
        );
        let cancellation = CancellationToken::new();
        let missing = MemoryLazyWorkspaceStore::default().compare_and_swap_lazy_workspace_measured(
            prepared.workspace_id, 0, prepared.clone(), WorkBudget::UNBOUNDED, &cancellation,
        ).await.expect("absent source is a CAS miss");
        assert!(!missing.value);
        assert!(missing.work.items_examined > 0);

        let mut committed_first = prepared.clone();
        committed_first.fork_origin.as_mut().expect("origin").phase = LazyForkPhase::Committed;
        let mut wrong_schema = prepared.clone();
        wrong_schema.schema_version -= 1;
        let mut wrong_workspace = prepared.clone();
        wrong_workspace.workspace_id = WorkspaceId::from_bytes([0xc2; 16]);
        let mut wrong_parent = prepared.clone();
        wrong_parent.parent_workspace_id = Some(WorkspaceId::from_bytes([0xc3; 16]));
        let mut wrong_source = prepared.clone();
        wrong_source.source.epoch += 1;
        let mut wrong_overlay = prepared.clone();
        wrong_overlay.overlay = LazyOverlay::Empty.id().expect("empty overlay");
        let mut wrong_shadows = prepared.clone();
        wrong_shadows.shadows = LazyShadowId([0xc4; 32]);
        let mut pending_remove = prepared.clone();
        pending_remove.pending_remove = Some(PendingLazyRemove {
            prior_overlay: prepared.overlay,
            prior_shadows: prepared.shadows,
            prepared_overlay: prepared.overlay,
            kind: PendingLazyRemoveKind::SourceOnly,
        });
        for replacement in [
            committed_first, wrong_schema, wrong_workspace, wrong_parent,
            wrong_source, wrong_overlay, wrong_shadows, pending_remove,
        ] {
            assert!(!store.compare_and_swap_lazy_workspace(
                prepared.workspace_id, 0, replacement,
            ).await.expect("noncanonical first origin"));
        }

        let updated = LazyWorkspaceState {
            revision: parent.revision + 1,
            source: SourceReference { epoch: parent.source.epoch + 1, ..parent.source },
            overlay: LazyOverlay::Empty.id().expect("empty overlay"),
            shadows: LazyShadow::Empty.id().expect("empty shadows"),
            ..parent.clone()
        };
        let updater = store.clone();
        let update = updated.clone();
        let expected = parent.revision;
        assert!(tokio::spawn(async move {
            updater.compare_and_swap_lazy_workspace(update.workspace_id, expected, update)
                .await.expect("concurrent source update")
        }).await.expect("update task"));
        let stale = store.compare_and_swap_lazy_workspace_measured(
            prepared.workspace_id, 0, prepared.clone(), WorkBudget::UNBOUNDED, &cancellation,
        ).await.expect("stale snapshot is a CAS miss");
        assert!(!stale.value);
        assert!(stale.work.items_examined > missing.work.items_examined);
        assert_eq!(stale.work.bytes_copied, 0);
        let denied = WorkBudget {
            items_examined: stale.work.items_examined - 1,
            ..WorkBudget::UNBOUNDED
        };
        let failure = store.compare_and_swap_lazy_workspace_measured(
            prepared.workspace_id, 0, prepared.clone(), denied, &cancellation,
        ).await.expect_err("deny the additional source-binding examination");
        assert!(matches!(failure.semantic_error(), LazyWorkspaceError::Work(_)));
        assert_eq!(failure.spent_work().items_examined, denied.items_examined);
        assert_eq!(failure.spent_work().bytes_copied, 0);
        assert!(store.load_lazy_workspace(prepared.workspace_id).await.expect("child").is_none());

        let refreshed_snapshot = LazySnapshotRef::new(
            updated.workspace_id, snapshot.authored_generation, updated.source, updated.overlay, updated.shadows,
        );
        let refreshed = memory_prepared_origin(refreshed_snapshot, root.workspace().config(), prepared.workspace_id);
        assert!(store.compare_and_swap_lazy_workspace(
            refreshed.workspace_id, 0, refreshed,
        ).await.expect("current source binding"));
    }

    #[tokio::test]
    async fn memory_original_freeze_retries_and_commit_ignore_later_parent_epochs() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let store = MemoryLazyWorkspaceStore::default();
        let root = LazyWorkspace::attach(&fs, "original-origin-parent", Arc::clone(&source), store.clone())
            .await.expect("parent");
        let parent = root.state().await.expect("parent state");
        let snapshot = root.snapshot().await.expect("original snapshot");
        let child_workspace = fs.create_workspace("prepared-origin-child").await.expect("child workspace");
        let prepared = memory_prepared_origin(snapshot, root.workspace().config(), child_workspace.id());
        let revision_only = LazyWorkspaceState { revision: parent.revision + 1, ..parent.clone() };
        assert!(store.compare_and_swap_lazy_workspace(
            parent.workspace_id, parent.revision, revision_only.clone(),
        ).await.expect("parent revision is not frozen evidence"));
        let _ = store.compare_and_swap_lazy_workspace(
            prepared.workspace_id, 0, prepared.clone(),
        ).await.expect("first durable freeze with lost acknowledgement");
        let recovered = store.load_lazy_workspace(prepared.workspace_id).await.expect("recover original").expect("original");
        assert_eq!(recovered, prepared);
        let denied_open = LazyWorkspace::open(child_workspace.clone(), Arc::clone(&source), store.clone())
            .await.err().expect("prepared binding is not workspace authority");
        assert!(matches!(denied_open.semantic_error(), LazyWorkspaceError::Concurrent));
        let descendant_snapshot = LazySnapshotRef::new(
            prepared.workspace_id, child_workspace.head().await.expect("head").id(),
            prepared.source, prepared.overlay, prepared.shadows,
        );
        let descendant = memory_prepared_origin(
            descendant_snapshot, child_workspace.config(), WorkspaceId::from_bytes([0xc5; 16]),
        );
        assert!(!store.compare_and_swap_lazy_workspace(
            descendant.workspace_id, 0, descendant,
        ).await.expect("prepared parent cannot authorize another freeze"));

        let updated = LazyWorkspaceState {
            revision: revision_only.revision + 1,
            source: SourceReference { epoch: parent.source.epoch + 1, ..parent.source },
            ..revision_only.clone()
        };
        assert!(store.compare_and_swap_lazy_workspace(
            parent.workspace_id, revision_only.revision, updated.clone(),
        ).await.expect("advance parent after original freeze"));
        assert!(!store.compare_and_swap_lazy_workspace(
            prepared.workspace_id, 0, prepared.clone(),
        ).await.expect("lost acknowledgement retry"));
        let retry = LazyWorkspaceState { revision: prepared.revision + 1, ..prepared.clone() };
        let cancellation = CancellationToken::new();
        let mut child_only_work = WorkCounters::default();
        child_only_work.charge_items(1, &WorkBudget::UNBOUNDED).expect("mutex admission");
        {
            let state = store.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            assert!(memory_lazy_search(
                &state.workspaces, &retry.workspace_id, &mut child_only_work,
                WorkBudget::UNBOUNDED, &cancellation,
            ).expect("child lookup").is_ok());
        }
        child_only_work.charge_items(1, &WorkBudget::UNBOUNDED).expect("origin examination");
        memory_lazy_copy(
            std::mem::size_of::<LazyWorkspaceState>(), &mut child_only_work, WorkBudget::UNBOUNDED,
        ).expect("replacement transfer");
        let no_parent_reads = WorkBudget {
            items_examined: child_only_work.items_examined,
            ..WorkBudget::UNBOUNDED
        };
        let retried = store.compare_and_swap_lazy_workspace_measured(
            retry.workspace_id, prepared.revision, retry.clone(), no_parent_reads, &cancellation,
        ).await.expect("same immutable prepared origin never rereads parent");
        assert!(retried.value);
        assert_eq!(retried.work, child_only_work);
        let mut committed = retry.clone();
        committed.revision += 1;
        committed.fork_origin.as_mut().expect("origin").phase = LazyForkPhase::Committed;
        let committed_receipt = store.compare_and_swap_lazy_workspace_measured(
            committed.workspace_id, retry.revision, committed.clone(), no_parent_reads, &cancellation,
        ).await.expect("commit immutable original without parent reads");
        assert!(committed_receipt.value);
        assert_eq!(committed_receipt.work, child_only_work);
        let child = LazyWorkspaceState {
            revision: committed.revision + 1,
            source: updated.source,
            ..committed.clone()
        };
        let child_receipt = store.compare_and_swap_lazy_workspace_measured(
            child.workspace_id, committed.revision, child.clone(), no_parent_reads, &cancellation,
        ).await.expect("child observation epoch can advance independently without parent reads");
        assert!(child_receipt.value);
        assert_eq!(child_receipt.work, child_only_work);
        assert_eq!(child.fork_origin.expect("origin").snapshot, snapshot);

        let next = LazyWorkspaceState { revision: child.revision + 1, ..child.clone() };
        let mut removed = next.clone();
        removed.fork_origin = None;
        let mut new_operation = next.clone();
        new_operation.fork_origin.as_mut().expect("origin").operation_id = crate::OperationId::from_bytes([0xc6; 16]);
        let mut new_config = next.clone();
        new_config.fork_origin.as_mut().expect("origin").config.limits.maximum_read_bytes += 1;
        let mut new_snapshot = next.clone();
        new_snapshot.fork_origin.as_mut().expect("origin").snapshot.source.epoch += 1;
        let mut new_parent = next.clone();
        new_parent.parent_workspace_id = Some(WorkspaceId::from_bytes([0xc7; 16]));
        let mut regressed = next;
        regressed.fork_origin.as_mut().expect("origin").phase = LazyForkPhase::Prepared;
        for replacement in [removed, new_operation, new_config, new_snapshot, new_parent, regressed] {
            assert!(!store.compare_and_swap_lazy_workspace(
                child.workspace_id, child.revision, replacement,
            ).await.expect("original fork evidence is immutable"));
        }
        assert_eq!(store.load_lazy_workspace(child.workspace_id).await.expect("unchanged child"), Some(child));
    }
}
