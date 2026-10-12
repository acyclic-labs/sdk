//! Source-backed sparse workspaces.

use acyclic_stream::ProviderTask;
use crate::demand::{
    DemandError, DemandSource, SourceCursor, SourceNode, SourceNodeKind,
    SourceReference, SourceVersion,
};
use crate::heap_future::{in_heap_admitted, in_heap_measured};
use crate::kernel::{
    FileKind, FileMetadata, FilePayload, FileRecord, LogicalName, MetadataField, NamespacePath,
};
use crate::model::VolumeConfig;
use crate::path::PortablePath;
use crate::record_store::MAXIMUM_CAS_ATTEMPTS;
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, AuthoredMutation, CancellationToken, FileId,
    ForkOptions, Fs, FsError, IdempotencyKey, OperationReceipt, TransactionCommit, WorkBudget, WorkCounters,
    Workspace, WorkspaceDirectoryEntry, WorkspaceError, WorkspaceExtentKind, WorkspaceId,
    WorkspaceStat,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use thiserror::Error;

#[cfg(target_arch = "wasm32")]
type LazyFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;
#[cfg(not(target_arch = "wasm32"))]
type LazyFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub(crate) const LAZY_STATE_SCHEMA: u32 = 10;

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

/// A borrowed-path member of one complete lazy upload candidate.
#[derive(Debug)]
pub enum LazyWriteMutation<'path> {
    /// Creates every missing directory through the canonical transaction.
    CreateDirectory {
        /// Canonical customer path, borrowed from the caller's original plan.
        path: &'path str,
    },
    /// Creates or replaces a complete file while preserving its lazy identity.
    PutFile {
        /// Canonical customer path, borrowed from the caller's original plan.
        path: &'path str,
        /// Complete original immutable payload.
        bytes: acyclic_stream::OwnedBytes,
    },
}

/// Authenticated selected source inputs for the canonical lazy import engine.
#[derive(Debug)]
pub struct SelectedImportPlan {
    pub(crate) source: LazySnapshotRef,
    pub(crate) source_config: VolumeConfig,
    pub(crate) source_used: bool,
    pub(crate) roots: Vec<String>,
    pub(crate) entries: Vec<SelectedImportRecord>,
    logical_bytes: u64,
    maximum_regular_file_bytes: u64,
    logical_owned_bytes: u64,
}

#[derive(Debug)]
pub(crate) struct SelectedImportRecord {
    pub(crate) path: String,
    pub(crate) record: Option<(FileRecord, FileMetadata)>,
}

impl SelectedImportPlan {
    /// Total selected regular-file logical length, including selected aliases.
    #[must_use]
    pub const fn logical_bytes(&self) -> u64 {
        self.logical_bytes
    }

    /// Largest represented selected regular-file logical length.
    #[must_use]
    pub const fn maximum_regular_file_bytes(&self) -> u64 {
        self.maximum_regular_file_bytes
    }

    /// Actual owned name and vector backing retained by this plan.
    #[must_use]
    pub const fn logical_owned_bytes(&self) -> u64 {
        self.logical_owned_bytes
    }

    pub(crate) fn input_digest(
        &self, destination: LazySnapshotRef, work: &LazyTreapWork<'_>,
    ) -> Result<crate::Digest, LazyWorkspaceError> {
        let mut hasher = blake3::Hasher::new();
        let domain = b"acyclic-fs-selected-import-v1\0";
        work.charge(WorkCounters {
            bytes_hashed: domain.len() as u64 + 64, ..WorkCounters::UNCHARGED
        })?;
        hasher.update(domain);
        hasher.update(&destination.id.into_bytes());
        hasher.update(&self.source.id.into_bytes());
        for root in &self.roots {
            work.charge(WorkCounters {
                items_examined: 1,
                bytes_hashed: 8u64.checked_add(crate::foundation::usize_to_u64(root.len()))
                    .ok_or_else(counter_overflow)?,
                ..WorkCounters::UNCHARGED
            })?;
            hasher.update(&(root.len() as u64).to_le_bytes());
            hasher.update(root.as_bytes());
        }
        Ok(crate::Digest::from_bytes(*hasher.finalize().as_bytes()))
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

/// Immutable SDK snapshot data, not a permission or an unforgeable grant.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OriginalQualifiedSnapshot {
    pub(crate) context: NativeSnapshotContext,
    pub(crate) generation: crate::GenerationId,
}

impl OriginalQualifiedSnapshot {
    /// Actual immutable authored generation; no logical ID is converted.
    #[must_use]
    pub const fn generation(&self) -> crate::GenerationId { self.generation }

    /// Actual SDK workspace represented by this snapshot data.
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId { self.context.workspace_id() }

    /// Actual immutable decoding profile and limits.
    #[must_use]
    pub const fn volume_config(&self) -> VolumeConfig { self.context.config() }

    /// Whether this snapshot retains genuine unresolved source metadata.
    #[must_use]
    pub const fn is_lazy(&self) -> bool { matches!(self.context, NativeSnapshotContext::Lazy(_)) }

    /// Projects the exact stored source cut with its actual authored generation.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn lazy_snapshot_with_native_work(
        self, original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<Option<LazySnapshotRef>, crate::FsError> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(original, cancellation);
        accounting.check()?;
        let NativeSnapshotContext::Lazy(cut) = self.context else { return Ok(None); };
        let hashing = WorkCounters {
            bytes_hashed: (LAZY_SNAPSHOT_DOMAIN.len() + 136) as u64,
            bytes_copied: std::mem::size_of::<LazySnapshotRef>() as u64, ..WorkCounters::UNCHARGED
        };
        accounting.admit(hashing)?;
        let snapshot = LazySnapshotRef::new(
            cut.workspace_id, self.generation, cut.source, cut.overlay, cut.shadows,
        );
        accounting.record_completed(hashing)?;
        Ok(Some(snapshot))
    }
}

/// Actual same-engine sync result.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NativePublicationSnapshot {
    pub(crate) operation: Option<crate::OperationId>,
    pub(crate) snapshot: OriginalQualifiedSnapshot,
}

impl NativePublicationSnapshot {
    /// The genuinely selected SDK DATA operation, if one actually published.
    #[must_use]
    pub const fn operation(&self) -> Option<crate::OperationId> { self.operation }

    /// Immutable full SDK qualification associated with the actual result.
    #[must_use]
    pub const fn snapshot(&self) -> OriginalQualifiedSnapshot { self.snapshot }

    pub(crate) const fn unchanged(context: NativeSnapshotContext, generation: crate::GenerationId) -> Self {
        Self { operation: None, snapshot: OriginalQualifiedSnapshot { context, generation } }
    }

    pub(crate) const fn published(
        context: NativeSnapshotContext, operation: crate::OperationId, generation: crate::GenerationId,
    ) -> Self {
        Self { operation: Some(operation), snapshot: OriginalQualifiedSnapshot { context, generation } }
    }
}

/// Exact post-prepare source/overlay/shadow cut, captured before the DATA CAS.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LazyPublicationCut {
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) config: VolumeConfig,
    pub(crate) source: SourceReference,
    pub(crate) overlay: LazyOverlayId,
    pub(crate) shadows: LazyShadowId,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum NativeSnapshotContext {
    Authored { workspace_id: WorkspaceId, config: VolumeConfig },
    Lazy(LazyPublicationCut),
}

impl NativeSnapshotContext {
    pub(crate) const fn workspace_id(self) -> WorkspaceId {
        match self {
            Self::Authored { workspace_id, .. } => workspace_id,
            Self::Lazy(cut) => cut.workspace_id,
        }
    }
    pub(crate) const fn config(self) -> VolumeConfig {
        match self {
            Self::Authored { config, .. } => config,
            Self::Lazy(cut) => cut.config,
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

/// Original selected publication evidence retained in the existing companion.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompletedSelectedImport {
    /// Supplied SDK publication identity.
    pub operation: crate::OperationId,
    /// Actual finished logical snapshot.
    pub snapshot: LazySnapshotRef,
    /// Original destination checkpoint.
    pub destination: LazySnapshotRef,
    /// Authenticated original source freeze.
    pub source: LazySnapshotRef,
    /// Selected input fingerprint, not an object-store root.
    pub input: crate::Digest,
    /// Actual destination volume configuration.
    pub config: VolumeConfig,
    /// Actual source freeze volume configuration.
    pub source_config: VolumeConfig,
}

/// Exact current candidate hint in the existing companion.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PendingLazyWrite {
    /// Original SDK publication operation.
    pub operation: crate::OperationId,
    /// Authentic original authored/source/overlay/shadow freeze.
    pub basis: LazySnapshotRef,
    /// Canonical digest of the complete ordered mutation input.
    pub input: crate::Digest,
    /// Actual immutable workspace decode context, including profile and limits.
    pub config: VolumeConfig,
    /// Permit bound into the original canonical publication fingerprint.
    pub permit: crate::PublicationPermit,
}

/// Latest finished snapshot hint, not an archive or historical authority.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompletedLazyWrite {
    /// Exact original candidate and preconditions.
    pub prepared: PendingLazyWrite,
    /// Genuine snapshot from the original committed generation.
    pub snapshot: LazySnapshotRef,
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
    #[serde(deserialize_with = "Option::<LazyForkOrigin>::deserialize")]
    pub fork_origin: Option<LazyForkOrigin>,
    /// Provider identity and invalidation epoch pinned at attach time.
    pub source: SourceReference,
    /// Immutable observations and tombstones visible to this context.
    pub overlay: LazyOverlayId,
    /// Persistent source identity to latest authored record index.
    pub shadows: LazyShadowId,
    /// Prepared authored removal recovered atomically when the workspace reopens.
    pub pending_remove: Option<PendingLazyRemove>,
    /// Latest original selected publication, never reconstructed from the head.
    #[serde(deserialize_with = "Option::<CompletedSelectedImport>::deserialize")]
    pub completed_selected_import: Option<CompletedSelectedImport>,
    /// Exact original prepared write, retained across an uncertain DATA ACK.
    #[serde(deserialize_with = "Option::<PendingLazyWrite>::deserialize")]
    pub pending_write: Option<PendingLazyWrite>,
    /// Latest finished write; unavailable older witnesses remain indeterminate.
    #[serde(deserialize_with = "Option::<CompletedLazyWrite>::deserialize")]
    pub completed_write: Option<CompletedLazyWrite>,
}

impl LazyWorkspaceState {
    fn new(workspace_id: WorkspaceId, source: SourceReference, overlay: LazyOverlayId, shadows: LazyShadowId) -> Self {
        Self { schema_version: LAZY_STATE_SCHEMA, revision: 1, workspace_id, source, overlay, shadows,
            parent_workspace_id: None, fork_origin: None, completed_selected_import: None,
            pending_remove: None, pending_write: None, completed_write: None }
    }

    /// Authenticates an original root allocation without rebinding its source.
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

    fn clone_with_accounting(
        &self, accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Self, crate::kernel::allocation::KernelOwnedAllocation), LazyWorkspaceError> {
        accounting.check().map_err(lazy_cursor_filesystem_error)?;
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        match self {
            Self::Empty => accounting.claim_owned(0, 0).map(|allocation| (Self::Empty, allocation))
                .map_err(lazy_cursor_filesystem_error),
            Self::Node { file_id, priority, record, metadata, left, right } => {
                let copied = WorkCounters { bytes_copied: record.len() as u64, ..WorkCounters::UNCHARGED };
                accounting.admit(copied).map_err(lazy_cursor_filesystem_error)?;
                let (record_buffer, allocation) = directory_vec_with_accounting::<u8>(
                    record.len(), accounting, cancellation,
                )?;
                let mut owned = (record_buffer, allocation);
                owned.0.extend_from_slice(record);
                accounting.record_completed(copied).map_err(lazy_cursor_filesystem_error)?;
                let copied = WorkCounters { bytes_copied: std::mem::size_of::<crate::WorkspaceMetadata>() as u64,
                    ..WorkCounters::UNCHARGED };
                accounting.admit(copied).map_err(lazy_cursor_filesystem_error)?;
                let mut allocation = accounting.claim_owned(copied.bytes_copied, 1)
                    .map_err(lazy_cursor_filesystem_error)?;
                owned.1.absorb(&mut allocation).map_err(lazy_cursor_filesystem_error)?;
                let metadata = Box::new(**metadata);
                let owned = (Self::Node { file_id: *file_id, priority: *priority, record: owned.0,
                    metadata, left: *left, right: *right }, owned.1);
                accounting.record_completed(copied).map_err(lazy_cursor_filesystem_error)?;
                Ok(owned)
            }
        }
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
#[allow(clippy::large_enum_variant)]
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

/// What removing a path records: a deletion of the source version it held, or only hiding a path the view never owned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Removal {
    Deleted,
    Hidden,
}

/// One fact introduced by a lazy overlay delta.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum LazyOverlayChange {
    /// First exact observation of one demanded path.
    Observe {
        /// Source generation against which this fact was authenticated.
        source: SourceReference,
        /// Immutable source fact.
        node: SourceNode,
    },
    /// Authored removal of a source path or subtree.
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

    fn clone_with_accounting(
        &self, accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Self, crate::kernel::allocation::KernelOwnedAllocation), LazyWorkspaceError> {
        accounting.check().map_err(lazy_cursor_filesystem_error)?;
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        match self {
            Self::Empty => accounting.claim_owned(0, 0).map(|allocation| (Self::Empty, allocation))
                .map_err(lazy_cursor_filesystem_error),
            Self::Node { path, priority, change, left, right } => {
                let (path, allocation) = clone_lazy_string_with_accounting(path, accounting, cancellation)?;
                Ok((Self::Node { path, priority: *priority, change: change.clone(),
                    left: *left, right: *right }, allocation))
            }
        }
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

fn lazy_reservation_error(error: std::collections::TryReserveError) -> LazyWorkspaceError {
    #[cfg(not(target_arch = "wasm32"))]
    { LazyWorkspaceError::NativeWork(crate::materializer::NativeTreeMaterializationError::AllocationFailed(error)) }
    #[cfg(target_arch = "wasm32")]
    { let _ = error; LazyWorkspaceError::NamespacePath(crate::kernel::NamespacePathError::AllocationFailed) }
}

fn clone_lazy_string(
    value: &str, allocations: &mut crate::kernel::allocation::AllocationLedger,
    work: &mut WorkCounters, budget: WorkBudget,
) -> Result<String, LazyWorkspaceError> {
    allocations.claim_elements::<u8>(value.len(), work, budget).map_err(lazy_allocation_error)?;
    let mut copied = String::new();
    copied.try_reserve_exact(value.len()).map_err(lazy_reservation_error)?;
    let excess = copied.capacity().checked_sub(value.len()).ok_or(crate::WorkError::Overflow)?;
    if excess != 0 {
        allocations.claim_bytes(crate::foundation::usize_to_u64(excess), 0, work, WorkBudget::UNBOUNDED)
            .map_err(lazy_allocation_error)?;
        work.verify(budget)?;
    }
    charge_lazy_copy(crate::foundation::usize_to_u64(value.len()), work, budget)?;
    copied.push_str(value);
    Ok(copied)
}

fn clone_lazy_string_with_accounting(
    value: &str, accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(String, crate::kernel::allocation::KernelOwnedAllocation), LazyWorkspaceError> {
    accounting.check().map_err(lazy_cursor_filesystem_error)?;
    cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
    let copied = WorkCounters { bytes_copied: value.len() as u64, ..WorkCounters::UNCHARGED };
    accounting.admit(copied).map_err(lazy_cursor_filesystem_error)?;
    let allocation = accounting.claim_owned(value.len() as u64, u64::from(!value.is_empty()))
        .map_err(lazy_cursor_filesystem_error)?;
    let mut owned = (String::new(), allocation);
    owned.0.try_reserve_exact(value.len()).map_err(|error| {
        #[cfg(not(target_arch = "wasm32"))]
        if accounting.native_work().is_some() {
            return LazyWorkspaceError::NativeWork(crate::NativeTreeMaterializationError::AllocationFailed(error));
        }
        lazy_reservation_error(error)
    })?;
    accounting.reconcile_owned(&mut owned.1, value.len() as u64, owned.0.capacity() as u64)
        .map_err(lazy_cursor_filesystem_error)?;
    owned.0.push_str(value);
    accounting.record_completed(copied).map_err(lazy_cursor_filesystem_error)?;
    Ok(owned)
}

#[cfg(not(target_arch = "wasm32"))]
/// A native core-state payload retaining its original allocation custody.
#[derive(Debug)]
pub struct NativeOwnedCoreState<T> {
    value: T,
    allocation: crate::materializer::NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl<T> NativeOwnedCoreState<T> {
    /// Transfers an already admitted payload and its original guard.
    pub fn from_parts(value: T, allocation: crate::materializer::NativeAllocation) -> Self {
        Self { value, allocation }
    }

    /// Borrows the actual stored payload without cloning it.
    pub fn as_state(&self) -> &T { &self.value }

    /// Transfers both the payload and its custody to another physical owner.
    pub fn into_parts(self) -> (T, crate::materializer::NativeAllocation) {
        (self.value, self.allocation)
    }
}

/// Durable companion storage for constant-size lazy bindings and immutable overlays.
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

    /// Loads the actual payload on the caller's original native allocation ledger.
    #[cfg(not(target_arch = "wasm32"))]
    fn load_lazy_workspace_with_native_work(
        &self, _workspace_id: WorkspaceId,
        _work: &crate::materializer::NativeMaterializationWork,
    ) -> impl Future<Output = Result<Option<NativeOwnedCoreState<LazyWorkspaceState>>, LazyWorkspaceError>> + Send {
        async { Err(LazyWorkspaceError::NativeUnsupported) }
    }

    /// Performs the same revision CAS using the original native invocation.
    #[cfg(not(target_arch = "wasm32"))]
    fn compare_and_swap_lazy_workspace_with_native_work(
        &self, _workspace_id: WorkspaceId, _expected_revision: u64,
        _replacement: LazyWorkspaceState,
        _work: &crate::materializer::NativeMaterializationWork,
    ) -> impl Future<Output = Result<bool, LazyWorkspaceError>> + Send {
        async { Err(LazyWorkspaceError::NativeUnsupported) }
    }

    /// Loads one immutable overlay with its original physical custody.
    #[cfg(not(target_arch = "wasm32"))]
    fn load_lazy_overlay_with_native_work(
        &self, _overlay: LazyOverlayId,
        _work: &crate::materializer::NativeMaterializationWork,
    ) -> impl Future<Output = Result<Option<NativeOwnedCoreState<LazyOverlay>>, LazyWorkspaceError>> + Send {
        async { Err(LazyWorkspaceError::NativeUnsupported) }
    }

    /// Transfers the original admitted overlay to its canonical content-addressed store.
    #[cfg(not(target_arch = "wasm32"))]
    fn put_lazy_overlay_with_native_work(
        &self, _overlay: LazyOverlayId, _value: NativeOwnedCoreState<LazyOverlay>,
        _work: &crate::materializer::NativeMaterializationWork,
    ) -> impl Future<Output = Result<(), LazyWorkspaceError>> + Send {
        async { Err(LazyWorkspaceError::NativeUnsupported) }
    }

    /// Loads one immutable shadow with its original physical custody.
    #[cfg(not(target_arch = "wasm32"))]
    fn load_lazy_shadow_with_native_work(
        &self, _shadow: LazyShadowId,
        _work: &crate::materializer::NativeMaterializationWork,
    ) -> impl Future<Output = Result<Option<NativeOwnedCoreState<LazyShadow>>, LazyWorkspaceError>> + Send {
        async { Err(LazyWorkspaceError::NativeUnsupported) }
    }

    /// Transfers the original admitted shadow to its canonical content-addressed store.
    #[cfg(not(target_arch = "wasm32"))]
    fn put_lazy_shadow_with_native_work(
        &self, _shadow: LazyShadowId, _value: NativeOwnedCoreState<LazyShadow>,
        _work: &crate::materializer::NativeMaterializationWork,
    ) -> impl Future<Output = Result<(), LazyWorkspaceError>> + Send {
        async { Err(LazyWorkspaceError::NativeUnsupported) }
    }
}

/// In-memory companion store for deterministic tests and embedded use.
#[derive(Clone, Default)]
pub struct MemoryLazyWorkspaceStore {
    inner: Arc<Mutex<MemoryLazyWorkspaceState>>,
}

#[derive(Default)]
struct MemoryLazyWorkspaceState {
    workspaces: Vec<(WorkspaceId, LazyWorkspaceState)>,
    overlays: Vec<(LazyOverlayId, MemoryLazyResident<LazyOverlay>)>,
    shadows: Vec<(LazyShadowId, MemoryLazyResident<LazyShadow>)>,
    #[cfg(not(target_arch = "wasm32"))]
    workspace_allocation: Option<crate::NativeAllocation>,
    #[cfg(not(target_arch = "wasm32"))]
    overlay_allocation: Option<crate::NativeAllocation>,
    #[cfg(not(target_arch = "wasm32"))]
    shadow_allocation: Option<crate::NativeAllocation>,
}

/// Payload first, original custody last: resident heaps retain their actual owner.
struct MemoryLazyResident<T> {
    value: T,
    #[cfg(not(target_arch = "wasm32"))]
    _allocation: Option<crate::NativeAllocation>,
}

impl<T> MemoryLazyResident<T> {
    fn ordinary(value: T) -> Self {
        Self {
            value,
            #[cfg(not(target_arch = "wasm32"))]
            _allocation: None,
        }
    }
}

pub(crate) trait MemoryLazyNode: Serialize + Eq {
    fn owned_bytes(&self) -> u64;
    fn clone_accounted(
        &self, accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Self, crate::kernel::allocation::KernelOwnedAllocation), LazyWorkspaceError> where Self: Sized;
}

impl MemoryLazyNode for LazyOverlay {
    fn owned_bytes(&self) -> u64 { self.logical_owned_bytes() }
    fn clone_accounted(
        &self, accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Self, crate::kernel::allocation::KernelOwnedAllocation), LazyWorkspaceError> {
        self.clone_with_accounting(accounting, cancellation)
    }
}

impl MemoryLazyNode for LazyShadow {
    fn owned_bytes(&self) -> u64 { self.logical_owned_bytes() }
    fn clone_accounted(
        &self, accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Self, crate::kernel::allocation::KernelOwnedAllocation), LazyWorkspaceError> {
        self.clone_with_accounting(accounting, cancellation)
    }
}

pub(crate) fn clone_lazy_node<T: MemoryLazyNode>(
    value: &T, allocations: &mut crate::kernel::allocation::AllocationLedger,
    work: &mut WorkCounters, budget: WorkBudget, cancellation: &CancellationToken,
) -> Result<T, LazyWorkspaceError> {
    let prior_live = allocations.live_bytes();
    let mut accounting = crate::kernel::allocation::KernelAccounting::borrowed_standalone(
        allocations, work, budget, cancellation,
    );
    let result = value.clone_accounted(&mut accounting, cancellation).map(|(value, _)| value);
    if result.is_err() {
        allocations.release(allocations.live_bytes() - prior_live).map_err(lazy_allocation_error)?;
    }
    result
}

fn memory_lazy_allocation_error(error: crate::kernel::allocation::AllocationError) -> LazyWorkspaceError {
    error.into_work_or(DemandError::WorkerUnavailable).into()
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
    memory_lazy_search_with_visit(table, key, || {
        memory_lazy_check(cancellation)?;
        work.charge_items(1, &budget).map_err(LazyWorkspaceError::from)
    })
}

fn memory_lazy_search_with_visit<K: Ord, V>(
    table: &[(K, V)], key: &K, mut visit: impl FnMut() -> Result<(), LazyWorkspaceError>,
) -> Result<Result<usize, usize>, LazyWorkspaceError> {
    let mut lower = 0;
    let mut upper = table.len();
    while lower < upper {
        visit()?;
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

/// Admit all copying and simultaneous old/new capacities before moving values.
fn memory_lazy_insert<K, V>(
    table: &mut Vec<(K, V)>,
    index: usize,
    entry: &mut Option<(K, V)>,
    allocations: &mut crate::kernel::allocation::AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    count_old_table: bool,
) -> Result<(), LazyWorkspaceError> {
    let old_bytes = if count_old_table && table.len() == table.capacity() {
        table.capacity().checked_mul(std::mem::size_of::<(K, V)>())
            .ok_or(LazyWorkspaceError::TooLarge)? as u64
    } else { 0 };
    if old_bytes != 0 {
        allocations.claim_bytes(old_bytes, 0, work, budget).map_err(memory_lazy_allocation_error)?;
    }
    let old_capacity = table.capacity();
    let mut accounting = crate::kernel::allocation::KernelAccounting::borrowed_standalone(
        allocations, work, budget, cancellation,
    );
    let result = memory_lazy_insert_with_accounting(
        table, index, entry, &mut accounting, cancellation,
        #[cfg(not(target_arch = "wasm32"))]
        None,
    );
    if old_bytes != 0 && table.capacity() != old_capacity {
        allocations.release(old_bytes).map_err(memory_lazy_allocation_error)?;
    }
    result
}

fn memory_lazy_insert_with_accounting<K, V>(
    table: &mut Vec<(K, V)>, index: usize, entry: &mut Option<(K, V)>,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>, cancellation: &CancellationToken,
    #[cfg(not(target_arch = "wasm32"))]
    mut native_table: Option<&mut Option<crate::NativeAllocation>>,
) -> Result<(), LazyWorkspaceError> {
    accounting.check().map_err(lazy_cursor_filesystem_error)?;
    memory_lazy_check(cancellation)?;
    let element_bytes = std::mem::size_of::<(K, V)>();
    let shifted = table.len().checked_sub(index).ok_or(LazyWorkspaceError::Concurrent)?;
    let moved = if table.len() == table.capacity() { table.len() } else { shifted };
    let bytes = moved.checked_add(1).and_then(|count| count.checked_mul(element_bytes))
        .ok_or(LazyWorkspaceError::TooLarge)?;
    let copied = WorkCounters {
        bytes_copied: crate::foundation::usize_to_u64(bytes),
        items_examined: crate::foundation::usize_to_u64(moved),
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(copied).map_err(lazy_cursor_filesystem_error)?;
    if table.len() < table.capacity() {
        memory_lazy_check(cancellation)?;
        table.insert(index, entry.take().ok_or(LazyWorkspaceError::Concurrent)?);
        accounting.record_completed(copied).map_err(lazy_cursor_filesystem_error)?;
        return Ok(());
    }
    let capacity = table.capacity().checked_mul(2).ok_or(LazyWorkspaceError::TooLarge)?
        .max(table.len().checked_add(1).ok_or(LazyWorkspaceError::TooLarge)?);
    let mut owned = directory_vec_with_accounting::<(K, V)>(capacity, accounting, cancellation)?;
    accounting.check().map_err(lazy_cursor_filesystem_error)?;
    memory_lazy_check(cancellation)?;
    let mut entry = Some(entry.take().ok_or(LazyWorkspaceError::Concurrent)?);
    for (position, existing) in table.drain(..).enumerate() {
        if position == index {
            if let Some(value) = entry.take() {
                owned.0.push(value);
            }
        }
        owned.0.push(existing);
    }
    if let Some(value) = entry {
        owned.0.push(value);
    }
    let old = std::mem::replace(table, owned.0);
    drop(old);
    #[cfg(not(target_arch = "wasm32"))]
    if let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) = owned.1 {
        let owner = native_table.as_mut().expect("original memory table supplies its canonical physical custody");
        **owner = Some(allocation);
    }
    accounting.record_completed(copied).map_err(lazy_cursor_filesystem_error)
}

fn memory_lazy_digest<T: Serialize>(
    value: &T,
    expected: [u8; 32],
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), LazyWorkspaceError> {
    let digest = hash_lazy_value_with_counters(value, work, budget, cancellation)?;
    if digest != expected {
        return Err(LazyWorkspaceError::Store("content-addressed lazy node digest mismatch".into()));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn memory_lazy_load_node<K: Ord, V: MemoryLazyNode>(
    table: &[(K, MemoryLazyResident<V>)], key: &K, digest: [u8; 32],
    allocations: &mut crate::kernel::allocation::AllocationLedger,
    work: &mut WorkCounters, budget: WorkBudget, cancellation: &CancellationToken,
) -> Result<Option<V>, LazyWorkspaceError> {
    let Ok(index) = memory_lazy_search(table, key, work, budget, cancellation)? else {
        return Ok(None);
    };
    let value = &table.get(index).ok_or(LazyWorkspaceError::Concurrent)?.1.value;
    memory_lazy_digest(value, digest, work, budget, cancellation)?;
    Ok(Some(clone_lazy_node(value, allocations, work, budget, cancellation)?))
}

#[allow(clippy::too_many_arguments)]
fn memory_lazy_put_node<K: Ord, V: MemoryLazyNode>(
    table: &mut Vec<(K, MemoryLazyResident<V>)>,
    entry: &mut Option<(K, MemoryLazyResident<V>)>, digest: [u8; 32],
    allocations: &mut crate::kernel::allocation::AllocationLedger,
    work: &mut WorkCounters, budget: WorkBudget, cancellation: &CancellationToken,
    count_old_table: bool,
) -> Result<(), LazyWorkspaceError> {
    let (key, resident) = entry.as_ref().ok_or(LazyWorkspaceError::Concurrent)?;
    let value = &resident.value;
    memory_lazy_digest(value, digest, work, budget, cancellation)?;
    let position = memory_lazy_put_position(table, key, value, |count| {
        memory_lazy_check(cancellation)?;
        work.charge_items(count, &budget).map_err(LazyWorkspaceError::from)
    }, || LazyWorkspaceError::Store("content-addressed lazy node collision".into()))?;
    if let Some(index) = position {
        memory_lazy_insert(table, index, entry, allocations, work, budget, cancellation, count_old_table)?;
    }
    Ok(())
}

fn memory_lazy_put_position<K: Ord, V: MemoryLazyNode>(
    table: &[(K, MemoryLazyResident<V>)], key: &K, value: &V,
    mut visit: impl FnMut(u64) -> Result<(), LazyWorkspaceError>,
    collision: impl FnOnce() -> LazyWorkspaceError,
) -> Result<Option<usize>, LazyWorkspaceError> {
    match memory_lazy_search_with_visit(table, key, || visit(1))? {
        Ok(index) => {
            let existing = &table.get(index).ok_or(LazyWorkspaceError::Concurrent)?.1.value;
            let examined = existing.owned_bytes().checked_add(value.owned_bytes())
                .and_then(|bytes| bytes.checked_add(std::mem::size_of::<V>() as u64))
                .ok_or(LazyWorkspaceError::TooLarge)?;
            visit(examined)?;
            if existing != value { return Err(collision()); }
            Ok(None)
        }
        Err(index) => Ok(Some(index)),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn memory_lazy_native_load_node<K: Ord, V: MemoryLazyNode>(
    table: &[(K, MemoryLazyResident<V>)], key: &K, digest: [u8; 32],
    original: &crate::NativeMaterializationWork,
) -> Result<Option<NativeOwnedCoreState<V>>, LazyWorkspaceError> {
    original.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
    let Ok(index) = memory_lazy_search_with_visit(table, key, || original.charge(
        WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED },
    ).map_err(Into::into))? else { return Ok(None); };
    let value = &table.get(index).ok_or(LazyWorkspaceError::Concurrent)?.1.value;
    if hash_lazy_value_with_native_work(value, original)? != digest {
        return Err(LazyWorkspaceError::NativeRecordIntegrity);
    }
    let mut accounting = crate::kernel::allocation::KernelAccounting::native(original, original.cancellation());
    let (value, allocation) = value.clone_accounted(&mut accounting, original.cancellation())?;
    match allocation {
        crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) =>
            Ok(Some(NativeOwnedCoreState::from_parts(value, allocation))),
        crate::kernel::allocation::KernelOwnedAllocation::Standalone => {
            drop(value);
            Err(LazyWorkspaceError::NativeLedgerMismatch)
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn memory_lazy_native_put_node<K: Ord, V: MemoryLazyNode>(
    table: &mut Vec<(K, MemoryLazyResident<V>)>,
    table_allocation: &mut Option<crate::NativeAllocation>,
    key: K, value: NativeOwnedCoreState<V>, digest: [u8; 32],
    original: &crate::NativeMaterializationWork,
) -> Result<(), LazyWorkspaceError> {
    if !value.allocation.matches_work(original) {
        return Err(LazyWorkspaceError::NativeLedgerMismatch);
    }
    let (value, allocation) = value.into_parts();
    let mut entry = Some((key, MemoryLazyResident { value, _allocation: Some(allocation) }));
    original.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
    let (key, value) = entry.as_ref().ok_or(LazyWorkspaceError::Concurrent)?;
    if hash_lazy_value_with_native_work(&value.value, original)? != digest {
        return Err(LazyWorkspaceError::NativeRecordIntegrity);
    }
    let position = memory_lazy_put_position(table, key, &value.value, |count| original.charge(
        WorkCounters { items_examined: count, ..WorkCounters::UNCHARGED },
    ).map_err(Into::into), || LazyWorkspaceError::NativeRecordIntegrity)?;
    if let Some(index) = position {
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(original, original.cancellation());
        memory_lazy_insert_with_accounting(
            table, index, &mut entry, &mut accounting, original.cancellation(), Some(table_allocation),
        )?;
    }
    // Drop incoming data and its guard outside the resident lock.
    drop(entry);
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
            work.charge_items(1, &budget).map_err(LazyWorkspaceError::from)?;
            let mut state = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            memory_lazy_check(cancellation)?;
            operation(&mut state, &mut allocations, &mut work)
        })();
        result.map(|value| OperationReceipt { value, work })
            .map_err(|error| error.with_prior_work(work))
    }

    fn cas(
        state: &mut MemoryLazyWorkspaceState, workspace_id: WorkspaceId, expected_revision: u64,
        replacement: LazyWorkspaceState, accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken, count_old_table: bool,
    ) -> Result<bool, LazyWorkspaceError> {
            let position = memory_lazy_search_with_visit(&state.workspaces, &workspace_id, ||
                accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })
                    .map_err(lazy_cursor_filesystem_error))?;
            let current = match position {
                Ok(index) => Some(&state.workspaces.get(index).ok_or(LazyWorkspaceError::Concurrent)?.1),
                Err(_) => None,
            };
            if current.map_or(0, |state| state.revision) != expected_revision {
                return Ok(false);
            }
            let prior_origin = current.and_then(|state| state.fork_origin);
            if prior_origin.is_some() || replacement.fork_origin.is_some() {
                accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })
                    .map_err(lazy_cursor_filesystem_error)?;
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
                    // The mutex excludes source updates until insertion.
                    let Ok(parent_index) = memory_lazy_search_with_visit(
                        &state.workspaces, &origin.snapshot.workspace_id, ||
                            accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })
                                .map_err(lazy_cursor_filesystem_error),
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
                    memory_lazy_check(cancellation)?;
                    accounting.check().map_err(lazy_cursor_filesystem_error)?;
                    state.workspaces.get_mut(index).ok_or(LazyWorkspaceError::Concurrent)?.1 = replacement;
                }
                Err(index) => match accounting {
                    crate::kernel::allocation::KernelAccounting::BorrowedStandalone {
                        allocations, work, budget, ..
                    } => memory_lazy_insert(
                        &mut state.workspaces, index, &mut Some((workspace_id, replacement)),
                        allocations, work, *budget, cancellation, count_old_table,
                    )?,
                    #[cfg(not(target_arch = "wasm32"))]
                    crate::kernel::allocation::KernelAccounting::NativeOriginal { .. } =>
                        memory_lazy_insert_with_accounting(
                            &mut state.workspaces, index, &mut Some((workspace_id, replacement)),
                            accounting, cancellation, Some(&mut state.workspace_allocation),
                        )?,
                    crate::kernel::allocation::KernelAccounting::Standalone { .. } =>
                        unreachable!("memory CAS uses borrowed ordinary or original native accounting"),
                },
            }
            Ok(true)
    }

}

macro_rules! memory_lazy_node_methods {
    ($id:ty, $node:ty, $table:ident, $custody:ident,
     $load:ident, $load_measured:ident, $load_native:ident,
     $put:ident, $put_measured:ident, $put_native:ident) => {
    async fn $load(&self, key: $id) -> Result<Option<$node>, Self::Error> {
        self.$load_measured(key, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await.map(|receipt| receipt.value)
    }

    async fn $load_measured(
        &self, key: $id, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Option<$node>>, LazyWorkspaceError> {
        self.measured(budget, cancellation, |state, allocations, work| {
            memory_lazy_load_node(
                &state.$table, &key, key.0, allocations, work, budget, cancellation,
            )
        })
    }

    async fn $put(&self, key: $id, value: $node) -> Result<(), Self::Error> {
        self.$put_measured(key, value, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await.map(|receipt| receipt.value)
    }

    async fn $put_measured(
        &self, key: $id, value: $node, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<()>, LazyWorkspaceError> {
        self.measured(budget, cancellation, |state, allocations, work| {
            allocations.claim_bytes(value.logical_owned_bytes(), 0, work, budget).map_err(memory_lazy_allocation_error)?;
            #[cfg(not(target_arch = "wasm32"))]
            let old_capacity = state.$table.capacity();
            memory_lazy_put_node(
                &mut state.$table, &mut Some((key, MemoryLazyResident::ordinary(value))),
                key.0, allocations, work, budget, cancellation, true,
            )?;
            #[cfg(not(target_arch = "wasm32"))]
            if state.$table.capacity() != old_capacity {
                state.$custody = None;
            }
            Ok(())
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn $load_native(
        &self, key: $id, original: &crate::NativeMaterializationWork,
    ) -> Result<Option<NativeOwnedCoreState<$node>>, LazyWorkspaceError> {
        original.admit(WorkCounters::UNCHARGED)?;
        let state = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        memory_lazy_native_load_node(&state.$table, &key, key.0, original)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn $put_native(
        &self, key: $id, value: NativeOwnedCoreState<$node>,
        original: &crate::NativeMaterializationWork,
    ) -> Result<(), LazyWorkspaceError> {
        original.admit(WorkCounters::UNCHARGED)?;
        let mut state = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let MemoryLazyWorkspaceState { $table, $custody, .. } = &mut *state;
        memory_lazy_native_put_node($table, $custody, key, value, key.0, original)
    }
    };
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
            #[cfg(not(target_arch = "wasm32"))]
            let old_capacity = state.workspaces.capacity();
            let mut accounting = crate::kernel::allocation::KernelAccounting::borrowed_standalone(
                allocations, work, budget, cancellation,
            );
            let value = Self::cas(
                state, workspace_id, expected_revision, replacement, &mut accounting, cancellation, true,
            )?;
            #[cfg(not(target_arch = "wasm32"))]
            if state.workspaces.capacity() != old_capacity {
                state.workspace_allocation = None;
            }
            Ok(value)
        })
    }

    memory_lazy_node_methods!(LazyOverlayId, LazyOverlay, overlays, overlay_allocation,
        load_lazy_overlay, load_lazy_overlay_measured, load_lazy_overlay_with_native_work,
        put_lazy_overlay, put_lazy_overlay_measured, put_lazy_overlay_with_native_work);
    memory_lazy_node_methods!(LazyShadowId, LazyShadow, shadows, shadow_allocation,
        load_lazy_shadow, load_lazy_shadow_measured, load_lazy_shadow_with_native_work,
        put_lazy_shadow, put_lazy_shadow_measured, put_lazy_shadow_with_native_work);

    #[cfg(not(target_arch = "wasm32"))]
    async fn load_lazy_workspace_with_native_work(
        &self, workspace_id: WorkspaceId, original: &crate::NativeMaterializationWork,
    ) -> Result<Option<NativeOwnedCoreState<LazyWorkspaceState>>, LazyWorkspaceError> {
        original.admit(WorkCounters::UNCHARGED)?;
        let state = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        original.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        let position = memory_lazy_search_with_visit(&state.workspaces, &workspace_id, || original.charge(
            WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED },
        ).map_err(Into::into))?;
        let allocation = original.claim_allocation(0, 0, WorkCounters::UNCHARGED)?;
        let value = if let Ok(index) = position {
            let copied = WorkCounters { bytes_copied: std::mem::size_of::<LazyWorkspaceState>() as u64,
                ..WorkCounters::UNCHARGED };
            original.admit(copied)?;
            let value = state.workspaces.get(index).ok_or(LazyWorkspaceError::Concurrent)?.1.clone();
            original.record_admitted(copied)?;
            Some(value)
        } else { None };
        drop(state);
        Ok(value.map(|value| NativeOwnedCoreState::from_parts(value, allocation)))

    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn compare_and_swap_lazy_workspace_with_native_work(
        &self, workspace_id: WorkspaceId, expected_revision: u64,
        replacement: LazyWorkspaceState, original: &crate::NativeMaterializationWork,
    ) -> Result<bool, LazyWorkspaceError> {
        original.admit(WorkCounters::UNCHARGED)?;
        let mut state = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        original.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        let copied = WorkCounters { bytes_copied: std::mem::size_of::<LazyWorkspaceState>() as u64,
            ..WorkCounters::UNCHARGED };
        original.admit(copied)?;
        let replacement = replacement.clone();
        original.record_admitted(copied)?;
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(original, original.cancellation());
        Self::cas(
            &mut state, workspace_id, expected_revision, replacement, &mut accounting, original.cancellation(), false,
        )

    }

}

struct ImmutableTreapNode<K, V, I> {
    key: K,
    priority: u64,
    value: V,
    left: I,
    right: I,
    #[cfg(not(target_arch = "wasm32"))]
    allocation: Option<crate::materializer::NativeAllocation>,
}

impl<K, V, I> ImmutableTreapNode<K, V, I> {
    fn has_native_allocation(&self) -> bool {
        #[cfg(not(target_arch = "wasm32"))]
        { self.allocation.is_some() }
        #[cfg(target_arch = "wasm32")]
        { false }
    }
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
    fn work(&self) -> Option<&LazyTreapWork<'_>>;
}

async fn insert_immutable_treap<T: ImmutableTreap>(
    treap: &T,
    mut root: T::Id,
    key: T::Key,
    value: T::Value,
) -> Result<T::Id, LazyWorkspaceError> {
    // The admitted spine replaces recursive treap descent.
    let _scope = treap.work().map(TreapAllocationScope::new);
    let mut pending = ImmutableTreapNode {
        priority: 0, key, value, left: root, right: root,
        #[cfg(not(target_arch = "wasm32"))]
        allocation: None,
    };
    if let Some(work) = treap.work() {
        #[cfg(not(target_arch = "wasm32"))]
        { pending.allocation = work.take_native_owned(T::owned_bytes(&pending))?; }
        if !pending.has_native_allocation() { work.adopt(T::owned_bytes(&pending))?; }
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
            let owned = if current.has_native_allocation() { 0 } else { T::owned_bytes(&current) };
            drop(current);
            if let Some(work) = treap.work() { work.release(owned)?; }
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
            let owned = if child.has_native_allocation() { 0 } else { T::owned_bytes(&child) };
            drop(child);
            if let Some(work) = treap.work() { work.release(owned)?; }
            inserted = treap.store(if descend_left {
                ImmutableTreapNode { left: inserted, ..current }
            } else {
                ImmutableTreapNode { right: inserted, ..current }
            }).await?;
        }
    }
    Ok(inserted)
}

enum LazyTreapMeter {
    Standalone {
        work: Mutex<WorkCounters>,
        allocations: Mutex<crate::kernel::allocation::AllocationLedger>,
        budget: WorkBudget,
    },
    #[cfg(not(target_arch = "wasm32"))]
    NativeOriginal {
        original: crate::materializer::NativeMaterializationWork,
        scratch: Mutex<crate::materializer::NativeAllocation>,
    },
}

impl<A, O> LazyReadonlyNode<(crate::Generation<A, O>, crate::DurableCommit)> {

    fn into_generation(self) -> crate::Generation<A, O> {
        match self {
            Self::Portable((generation, record)) => {
                drop(record);
                generation
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(value) => {
                let ((generation, record), allocation) = value.into_parts();
                drop(record);
                drop(allocation);
                generation
            }
        }
    }
}

macro_rules! lazy_dispatch {
    ($work:expr, $portable:expr, |$original:ident| $native:expr) => {
        match &$work.meter {
            LazyTreapMeter::Standalone { .. } => $portable,
            #[cfg(not(target_arch = "wasm32"))]
            LazyTreapMeter::NativeOriginal { original: $original, .. } => $native,
        }
    };
}

macro_rules! selected_checkout_query {
    ($checkout:expr, $path:expr, $accounting:expr, $cancellation:expr, $ordinary:ident, $native:ident) => {
        match &$accounting.meter {
            LazyTreapMeter::Standalone { .. } => $accounting.accept(
                $checkout.$ordinary($path, $accounting.remaining()?, $cancellation).await
                    .map_err(|failure| lazy_operation_failure(failure, |error| workspace_error(error.into()))),
            ),
            #[cfg(not(target_arch = "wasm32"))]
            LazyTreapMeter::NativeOriginal { original, .. } =>
                $checkout.$native($path, original, $cancellation).await.map_err(native_filesystem_error),
        }
    };
}

/// Accounting context for bounded lazy workspace operations.
pub struct LazyTreapWork<'a> {
    meter: LazyTreapMeter,
    cancellation: &'a CancellationToken,
}

impl<'a> LazyTreapWork<'a> {
    /// Starts ordinary accounting with the caller's budget and cancellation.
    pub fn new(budget: WorkBudget, cancellation: &'a CancellationToken) -> Self {
        Self::new_with_prior(budget, cancellation, WorkCounters::UNCHARGED)
    }

    fn new_with_prior(
        budget: WorkBudget, cancellation: &'a CancellationToken, prior: WorkCounters,
    ) -> Self {
        Self {
            meter: LazyTreapMeter::Standalone {
                work: Mutex::new(prior),
                allocations: Mutex::new(crate::kernel::allocation::AllocationLedger::default()),
                budget,
            },
            cancellation,
        }
    }

    /// Uses the caller's original native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn native_with_work(
        original: &crate::materializer::NativeMaterializationWork,
        cancellation: &'a CancellationToken,
    ) -> Result<Self, LazyWorkspaceError> {
        let original = original.clone();
        original.admit(WorkCounters::UNCHARGED)?;
        let scratch = original.claim_allocation(0, 0, crate::WorkCounters::UNCHARGED)?;
        Ok(Self {
            meter: LazyTreapMeter::NativeOriginal {
                original,
                scratch: Mutex::new(scratch),
            },
            cancellation,
        })
    }

    fn with_allocation_state<T>(
        &self,
        operation: impl FnOnce(
            &mut crate::kernel::allocation::AllocationLedger,
            &mut WorkCounters, WorkBudget, &CancellationToken,
        ) -> Result<T, LazyWorkspaceError>,
    ) -> Result<T, LazyWorkspaceError> {
        match &self.meter {
            LazyTreapMeter::Standalone { work, allocations, budget } => {
                let mut allocations = allocations.lock().expect("lazy treap allocation lock poisoned");
                let mut work = work.lock().expect("lazy treap work lock poisoned");
                operation(&mut allocations, &mut work, *budget, self.cancellation)
            }
            #[cfg(not(target_arch = "wasm32"))]
            LazyTreapMeter::NativeOriginal { .. } =>
                unreachable!("native operations use the original kernel accounting"),
        }
    }

    fn with_kernel<T>(
        &self, operation: impl FnOnce(
            &mut crate::kernel::allocation::KernelAccounting<'_>, &CancellationToken,
        ) -> Result<T, LazyWorkspaceError>,
    ) -> Result<T, LazyWorkspaceError> {
        lazy_dispatch!{self, self.with_allocation_state(
                |allocations, work, budget, cancellation| operation(
                    &mut crate::kernel::allocation::KernelAccounting::borrowed_standalone(
                        allocations, work, budget, cancellation,
                    ), cancellation,
                ),
            ),
            |original| operation(
                &mut crate::kernel::allocation::KernelAccounting::native(original, self.cancellation),
                self.cancellation,
            )
        }
    }

    fn produce<T>(
        &self, operation: impl FnOnce(
            &mut crate::kernel::allocation::KernelAccounting<'_>, &CancellationToken,
        ) -> Result<(T, crate::kernel::allocation::KernelOwnedAllocation), LazyWorkspaceError>,
    ) -> Result<T, LazyWorkspaceError> {
        let (value, allocation) = self.with_kernel(operation)?;
        #[cfg(target_arch = "wasm32")]
        drop(allocation);
        #[cfg(not(target_arch = "wasm32"))]
        if let LazyTreapMeter::NativeOriginal { scratch, .. } = &self.meter {
            let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(mut allocation) = allocation else {
                drop(value);
                return Err(LazyWorkspaceError::NativeLedgerMismatch);
            };
            let absorbed = scratch.lock().expect("lazy treap scratch lock poisoned").absorb(&mut allocation);
            if let Err(error) = absorbed {
                drop(value);
                drop(allocation);
                return Err(error.into());
            }
        }
        Ok(value)
    }

    fn spent(&self) -> WorkCounters {
        match &self.meter {
            LazyTreapMeter::Standalone { work, .. } =>
                *work.lock().expect("lazy treap work lock poisoned"),
            #[cfg(not(target_arch = "wasm32"))]
            LazyTreapMeter::NativeOriginal { original, .. } =>
                original.native_read_meter().with_original_leaf(|work| work.work),
        }
    }

    /// Returns the result with this context's completed work.
    pub fn measured<T, E, R>(
        &self, result: Result<T, E>, map_error: impl FnOnce(E, WorkCounters) -> R,
    ) -> Result<OperationReceipt<T>, R> {
        let work = self.spent();
        result.map(|value| OperationReceipt { value, work }).map_err(|error| map_error(error, work))
    }

    fn live_bytes(&self) -> u64 {
        match &self.meter {
            LazyTreapMeter::Standalone { allocations, .. } =>
                allocations.lock().expect("lazy treap allocation lock poisoned").live_bytes(),
            #[cfg(not(target_arch = "wasm32"))]
            LazyTreapMeter::NativeOriginal { scratch, .. } =>
                scratch.lock().expect("lazy treap scratch lock poisoned").bytes(),
        }
    }

    fn remaining(&self) -> Result<WorkBudget, LazyWorkspaceError> {
        match &self.meter {
            LazyTreapMeter::Standalone { budget, .. } => {
                let mut prior = self.spent();
                prior.peak_allocation_bytes = 0;
                let mut remaining = remaining_work(prior, *budget, 0)?;
                remaining.peak_allocation_bytes = budget.peak_allocation_bytes
                    .checked_sub(self.live_bytes()).ok_or_else(counter_overflow)?;
                Ok(remaining)
            }
            #[cfg(not(target_arch = "wasm32"))]
            LazyTreapMeter::NativeOriginal { original, .. } =>
                original.remaining().map_err(Into::into),
        }
    }

    fn accept<T>(&self, result: Result<OperationReceipt<T>, LazyWorkspaceError>) -> Result<T, LazyWorkspaceError> {
        let (result, additional) = match result {
            Ok(receipt) => (Ok(receipt.value), receipt.work),
            Err(error) => {
                let failure = error.into_measured_failure();
                (Err(failure.error), *failure.work)
            }
        };
        let recorded = match &self.meter {
            LazyTreapMeter::Standalone { work, budget, .. } => {
                let live = self.live_bytes();
                let mut work = work.lock().expect("lazy treap work lock poisoned");
                *work = work.with_backend(additional, live)?;
                work.verify(*budget).map_err(Into::into)
            }
            #[cfg(not(target_arch = "wasm32"))]
            LazyTreapMeter::NativeOriginal { original, .. } =>
                original.native_read_meter().with_original_leaf(|work| work.merge(additional)).map_err(Into::into),
        };
        match result {
            Err(error) => Err(error),
            Ok(value) => recorded.map(|()| value),
        }
    }

    fn charge(&self, additional: WorkCounters) -> Result<(), LazyWorkspaceError> {
        self.cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        match &self.meter {
            LazyTreapMeter::Standalone { work, budget, .. } => {
                let mut work = work.lock().expect("lazy treap work lock poisoned");
                let next = work.checked_add(additional)?;
                next.verify(*budget)?;
                *work = next;
                Ok(())
            }
            #[cfg(not(target_arch = "wasm32"))]
            LazyTreapMeter::NativeOriginal { original, .. } =>
                original.admit(additional).map_err(Into::into),
        }
    }

    fn retain_fresh_checkout<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &self, checkout: &crate::Checkout<A, O>,
    ) -> Result<u64, LazyWorkspaceError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let LazyTreapMeter::NativeOriginal { original, .. } = &self.meter {
            original.admit(WorkCounters::UNCHARGED)?;
            return Ok(0);
        }
        let token = checkout.private_control_custody();
        let bytes = token.logical_owned_bytes()
            .checked_add(token.retained_payload_bytes())
            .ok_or_else(counter_overflow)?;
        self.adopt(bytes)?;
        Ok(bytes)
    }

    fn adopt(&self, bytes: u64) -> Result<(), LazyWorkspaceError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let LazyTreapMeter::NativeOriginal { original, scratch } = &self.meter {
            let mut allocation = original.retain_owned(bytes)?;
            scratch.lock().expect("lazy treap scratch lock poisoned").absorb(&mut allocation)?;
            return Ok(());
        }
        self.with_allocation_state(|allocations, work, budget, _|
            allocations.claim_bytes(bytes, 0, work, budget).map_err(lazy_allocation_error))
    }

    fn adopt_input(&self, bytes: u64) -> Result<(), LazyWorkspaceError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let LazyTreapMeter::NativeOriginal { .. } = &self.meter {
            return self.adopt(bytes);
        }
        self.with_allocation_state(|allocations, work, budget, cancellation| {
            allocations.claim_bytes(bytes, 0, work, WorkBudget::UNBOUNDED)
                .map_err(lazy_allocation_error)?;
            cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
            work.verify(budget)?;
            Ok(())
        })
    }

    fn allocate(&self, bytes: u64) -> Result<(), LazyWorkspaceError> {
        self.cancellation.check().map_err(|_| {
            #[cfg(not(target_arch = "wasm32"))]
            if let LazyTreapMeter::NativeOriginal { .. } = &self.meter {
                return LazyWorkspaceError::NativeWork(
                    crate::NativeTreeMaterializationError::Cancelled);
            }
            LazyWorkspaceError::Cancelled
        })?;
        #[cfg(not(target_arch = "wasm32"))]
        if let LazyTreapMeter::NativeOriginal { original, scratch } = &self.meter {
            let mut allocation = original.claim(bytes)?;
            scratch.lock().expect("lazy treap scratch lock poisoned").absorb(&mut allocation)?;
            return Ok(());
        }
        self.with_allocation_state(|allocations, work, budget, _|
            allocations.claim_bytes(bytes, u64::from(bytes != 0), work, budget)
                .map_err(lazy_allocation_error))
    }

    fn canonical_path_owned(
        &self,
        path: &str,
        limits: crate::VolumeLimits,
    ) -> Result<String, LazyWorkspaceError> {
        self.cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        PortablePath::validate(path, limits)
            .map_err(|error| LazyWorkspaceError::NamespacePath(crate::kernel::NamespacePathError::Path(error)))?;
        self.produce(|accounting, cancellation|
            clone_lazy_string_with_accounting(path, accounting, cancellation))
    }

    fn namespace_owned(
        &self,
        path: &str,
        profile: crate::model::FilesystemProfile,
        limits: crate::VolumeLimits,
    ) -> Result<NamespacePath, LazyWorkspaceError> {
        self.produce(|accounting, cancellation|
            NamespacePath::parse_in_profile_with_accounting(
                path, profile, limits, accounting, cancellation,
            ).map_err(lazy_cursor_filesystem_error))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn take_native_owned(
        &self, bytes: u64,
    ) -> Result<Option<crate::materializer::NativeAllocation>, LazyWorkspaceError> {
        match &self.meter {
            LazyTreapMeter::Standalone { .. } => Ok(None),
            LazyTreapMeter::NativeOriginal { scratch, .. } => Ok(Some(
                scratch.lock().expect("lazy treap scratch lock poisoned").split_off(bytes)?,
            )),
        }
    }

    fn transfer_to_treap(&self, bytes: u64) -> Result<(), LazyWorkspaceError> {
        #[cfg(not(target_arch = "wasm32"))]
        if matches!(self.meter, LazyTreapMeter::NativeOriginal { .. }) {
            return Ok(());
        }
        self.release(bytes)
    }

    fn release(&self, bytes: u64) -> Result<(), LazyWorkspaceError> {
        match &self.meter {
            LazyTreapMeter::Standalone { allocations, .. } =>
                allocations.lock().expect("lazy treap allocation lock poisoned")
                    .release(bytes).map_err(lazy_allocation_error),
            #[cfg(not(target_arch = "wasm32"))]
            LazyTreapMeter::NativeOriginal { scratch, .. } => {
                scratch.lock().expect("lazy treap scratch lock poisoned").release(bytes)?;
                Ok(())
            }
        }
    }

    fn ensure_vec_push<T>(&self, values: &mut Vec<T>) -> Result<(), LazyWorkspaceError> {
        if values.len() < values.capacity() {
            return Ok(());
        }
        self.cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let LazyTreapMeter::NativeOriginal { original, scratch } = &self.meter {
            let old_bytes = crate::foundation::usize_to_u64(values.capacity())
                .checked_mul(std::mem::size_of::<T>() as u64).ok_or(crate::WorkError::Overflow)?;
            let next_capacity = values.capacity().checked_mul(2).map(|value| value.max(1))
                .ok_or(crate::WorkError::Overflow)?;
            let copied = WorkCounters {
                bytes_copied: (values.len() as u64).checked_mul(std::mem::size_of::<T>() as u64)
                    .ok_or(crate::WorkError::Overflow)?, ..WorkCounters::UNCHARGED
            };
            original.admit(copied)?;
            let mut accounting = crate::kernel::allocation::KernelAccounting::native(original, self.cancellation);
            let (replacement, allocation) = directory_vec_with_accounting::<T>(
                next_capacity, &mut accounting, self.cancellation,
            )?;
            let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) = allocation else {
                drop(replacement);
                return Err(LazyWorkspaceError::NativeLedgerMismatch);
            };
            let mut owned = (replacement, allocation);
            let absorbed = scratch.lock().expect("lazy treap scratch lock poisoned").absorb(&mut owned.1);
            if let Err(error) = absorbed { drop(owned); return Err(error.into()); }
            owned.0.append(values);
            let old = std::mem::replace(values, owned.0);
            drop(old);
            let recorded = original.record_admitted(copied).map_err(LazyWorkspaceError::from);
            let released = self.release(old_bytes);
            recorded.and(released)?;
            return Ok(());
        }
        let old_capacity = values.capacity();
        let next_capacity = old_capacity.checked_mul(2).map(|value| value.max(1))
            .ok_or(crate::WorkError::Overflow)?;
        self.with_allocation_state(|allocations, work, budget, _| {
            allocations.claim_elements::<T>(next_capacity, work, budget).map_err(lazy_allocation_error)?;
            let mut replacement = Vec::new();
            replacement.try_reserve_exact(next_capacity)
                .map_err(lazy_reservation_error)?;
            let excess = replacement.capacity().checked_sub(next_capacity).ok_or(crate::WorkError::Overflow)?
                .checked_mul(std::mem::size_of::<T>()).ok_or(crate::WorkError::Overflow)?;
            if excess != 0 {
                allocations.claim_bytes(crate::foundation::usize_to_u64(excess), 0, work, WorkBudget::UNBOUNDED)
                    .map_err(lazy_allocation_error)?;
                work.verify(budget)?;
            }
            charge_lazy_copy(crate::foundation::usize_to_u64(values.len() * std::mem::size_of::<T>()), work, budget)?;
            replacement.append(values);
            *values = replacement;
            allocations.release(crate::foundation::usize_to_u64(old_capacity * std::mem::size_of::<T>()))
                .map_err(lazy_allocation_error)?;
            Ok(())
        })
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

struct LazyHashWriter<F> {
    hasher: blake3::Hasher,
    charge: F,
    failure: Option<LazyWorkspaceError>,
}

impl<F: FnMut(&[u8]) -> Result<(), LazyWorkspaceError>> std::io::Write for LazyHashWriter<F> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if let Err(error) = (self.charge)(bytes) {
            self.failure = Some(error);
            return Err(std::io::ErrorKind::Other.into());
        }
        self.hasher.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

fn hash_lazy_value<T: Serialize>(value: &T, work: Option<&LazyTreapWork<'_>>) -> Result<[u8; 32], LazyWorkspaceError> {
    finish_lazy_hash(value, |bytes| work.map_or(Ok(()), |work| {
        let bytes = crate::foundation::usize_to_u64(bytes.len());
        work.charge(WorkCounters { bytes_encoded: bytes, bytes_hashed: bytes, ..WorkCounters::UNCHARGED })
    }))
}

fn finish_lazy_hash<T: Serialize>(
    value: &T, charge: impl FnMut(&[u8]) -> Result<(), LazyWorkspaceError>,
) -> Result<[u8; 32], LazyWorkspaceError> {
    let mut writer = LazyHashWriter { hasher: blake3::Hasher::new(), charge, failure: None };
    let result = serde_json::to_writer(&mut writer, value);
    if let Some(error) = writer.failure { return Err(error); }
    result.map_err(|error| {
        #[cfg(not(target_arch = "wasm32"))]
        { LazyWorkspaceError::NativeRecordJson(error, None) }
        #[cfg(target_arch = "wasm32")]
        { LazyWorkspaceError::Store(error.to_string()) }
    })?;
    Ok(*writer.hasher.finalize().as_bytes())
}

pub(crate) fn hash_lazy_value_with_counters<T: Serialize>(
    value: &T, work: &mut WorkCounters, budget: WorkBudget, cancellation: &CancellationToken,
) -> Result<[u8; 32], LazyWorkspaceError> {
    cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
    finish_lazy_hash(value, |bytes| {
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        let bytes = crate::foundation::usize_to_u64(bytes.len());
        work.charge(&WorkCounters { bytes_encoded: bytes, bytes_hashed: bytes, ..WorkCounters::UNCHARGED }, &budget)
            .map_err(LazyWorkspaceError::from)
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn hash_lazy_value_with_native_work<T: Serialize>(
    value: &T, original: &crate::NativeMaterializationWork,
) -> Result<[u8; 32], LazyWorkspaceError> {
    original.admit(WorkCounters::UNCHARGED)?;
    finish_lazy_hash(value, |bytes| original.charge(WorkCounters {
        bytes_encoded: bytes.len() as u64, bytes_hashed: bytes.len() as u64, ..WorkCounters::UNCHARGED
    }).map_err(Into::into))
}

macro_rules! lazy_treap_io {
    ($id:ident, $load:ident, $measured_load:ident, $native_load:ident,
        $put:ident, $measured_put:ident, $native_put:ident, $missing:literal,
        |$value:ident, $allocation:ident| $decode:expr, |$node:ident| $encode:expr) => {
        async fn load(&self, id: Self::Id)
            -> Result<Option<ImmutableTreapNode<Self::Key, Self::Value, Self::Id>>, LazyWorkspaceError> {
            let decode = |$value, #[cfg(not(target_arch = "wasm32"))] $allocation| $decode;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = self.1
                && let LazyTreapMeter::NativeOriginal { original, .. } = &work.meter {
                let owned = self.0.$native_load(id, original).await?.ok_or(LazyWorkspaceError::StaleSource)?;
                let (value, allocation) = owned.into_parts();
                return decode(value, Some(allocation));
            }
            let value = if let Some(work) = self.1 {
                work.accept(self.0.$measured_load(id, work.remaining()?, work.cancellation).await)?
            } else { self.0.$load(id).await.map_err(store_error)? };
            if let (Some(work), Some(value)) = (self.1, value.as_ref()) {
                work.adopt(value.logical_owned_bytes())?;
            }
            let value = value.ok_or_else(|| LazyWorkspaceError::Store($missing.to_owned()))?;
            decode(value, #[cfg(not(target_arch = "wasm32"))] None)
        }

        async fn store(&self, $node: ImmutableTreapNode<Self::Key, Self::Value, Self::Id>)
            -> Result<Self::Id, LazyWorkspaceError> {
            #[cfg(not(target_arch = "wasm32"))]
            let allocation = $node.allocation;
            let value = $encode;
            let owned_bytes = value.logical_owned_bytes();
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = self.1
                && let LazyTreapMeter::NativeOriginal { original, .. } = &work.meter {
                let value = NativeOwnedCoreState::from_parts(
                    value, allocation.ok_or(LazyWorkspaceError::NativeLedgerMismatch)?,
                );
                let id = $id(hash_lazy_value(value.as_state(), self.1)?);
                self.0.$native_put(id, value, original).await?;
                return Ok(id);
            }
            let mut transferred = false;
            let result = async {
                let id = $id(hash_lazy_value(&value, self.1)?);
                if let Some(work) = self.1 {
                    work.release(owned_bytes)?;
                    transferred = true;
                    work.accept(self.0.$measured_put(id, value, work.remaining()?, work.cancellation).await)?;
                } else { self.0.$put(id, value).await.map_err(store_error)?; }
                Ok(id)
            }.await;
            if !transferred && let Some(work) = self.1 { work.release(owned_bytes)?; }
            result
        }
    };
}

struct OverlayTreap<'a, S>(&'a S, Option<&'a LazyTreapWork<'a>>);

impl<S: LazyWorkspaceStore> ImmutableTreap for OverlayTreap<'_, S> {
    type Id = LazyOverlayId;
    type Key = String;
    type Value = LazyOverlayChange;

    lazy_treap_io!(LazyOverlayId, load_lazy_overlay, load_lazy_overlay_measured, load_lazy_overlay_with_native_work,
        put_lazy_overlay, put_lazy_overlay_measured, put_lazy_overlay_with_native_work, "lazy overlay is absent",
        |value, allocation| match value {
            LazyOverlay::Empty => {
                #[cfg(not(target_arch = "wasm32"))]
                drop(allocation);
                Ok(None)
            },
            LazyOverlay::Node { path, priority, change, left, right } => Ok(Some(ImmutableTreapNode {
                key: path, priority, value: change, left, right,
                #[cfg(not(target_arch = "wasm32"))]
                allocation,
            })),
        },
        |node| LazyOverlay::Node { path: node.key, priority: node.priority, change: node.value, left: node.left, right: node.right });

    fn priority(key: &Self::Key) -> u64 {
        u64::from_be_bytes(lazy_priority_bytes(key.as_bytes()))
    }

    fn priority_bytes(key: &Self::Key) -> u64 { crate::foundation::usize_to_u64(key.len()) }
    fn owned_bytes(node: &ImmutableTreapNode<Self::Key, Self::Value, Self::Id>) -> u64 {
        crate::foundation::usize_to_u64(node.key.capacity())
    }
    fn work(&self) -> Option<&LazyTreapWork<'_>> { self.1 }
}

struct ShadowValue {
    record: Vec<u8>,
    metadata: Box<crate::WorkspaceMetadata>,
}

struct ShadowTreap<'a, S>(&'a S, Option<&'a LazyTreapWork<'a>>);

impl<S: LazyWorkspaceStore> ImmutableTreap for ShadowTreap<'_, S> {
    type Id = LazyShadowId;
    type Key = FileId;
    type Value = ShadowValue;

    lazy_treap_io!(LazyShadowId, load_lazy_shadow, load_lazy_shadow_measured, load_lazy_shadow_with_native_work,
        put_lazy_shadow, put_lazy_shadow_measured, put_lazy_shadow_with_native_work, "lazy identity shadow is absent",
        |value, allocation| match value {
            LazyShadow::Empty => {
                #[cfg(not(target_arch = "wasm32"))]
                drop(allocation);
                Ok(None)
            },
            LazyShadow::Node { file_id, priority, record, metadata, left, right } => Ok(Some(ImmutableTreapNode {
                key: file_id, priority,
                value: ShadowValue { record, metadata }, left, right,
                #[cfg(not(target_arch = "wasm32"))]
                allocation,
            })),
        },
        |node| LazyShadow::Node { file_id: node.key, priority: node.priority, record: node.value.record,
            metadata: node.value.metadata, left: node.left, right: node.right });

    fn priority(key: &Self::Key) -> u64 {
        u64::from_le_bytes(lazy_priority_bytes(&key.into_bytes()))
    }

    fn priority_bytes(_: &Self::Key) -> u64 { crate::foundation::usize_to_u64(std::mem::size_of::<FileId>()) }
    fn owned_bytes(node: &ImmutableTreapNode<Self::Key, Self::Value, Self::Id>) -> u64 {
        crate::foundation::usize_to_u64(node.value.record.capacity() + std::mem::size_of::<crate::WorkspaceMetadata>())
    }
    fn work(&self) -> Option<&LazyTreapWork<'_>> { self.1 }
}

/// Facts an inspection's caller already established, so it need not repeat them.
#[derive(Clone, Copy, Default)]
pub(crate) struct InspectBasis<'a> {
    /// State the caller read; otherwise the current state is loaded.
    pub(crate) state: Option<&'a LazyWorkspaceState>,
    /// Exact authored generation supplied by a frozen readonly caller.
    pub(crate) authored_generation: Option<crate::GenerationId>,
    /// Marks actual native lookup participation, including native absence queries.
    pub(crate) source_used: Option<&'a std::sync::atomic::AtomicBool>,
    /// The caller's authored view already lacks the path.
    pub(crate) authored_absent: bool,
    /// Node a listing observed for the path after excluding tombstones.
    pub(crate) listed: Option<SourceNode>,
    /// Keep an observation pinned to an earlier source epoch.
    pub(crate) retain_pinned_observation: bool,
    /// The overlay records no fact for the path, as its listing proved.
    pub(crate) unobserved: bool,
}

pub(crate) struct SelectedImportEffects {
    pub(crate) paths: Vec<String>,
    pub(crate) records: Vec<(FileRecord, FileMetadata)>,
    pub(crate) changes: Vec<NamespacePath>,
    pub(crate) input: crate::Digest,
    pub(crate) logical_owned_bytes: u64,
    #[cfg(not(target_arch = "wasm32"))]
    allocation: Option<crate::materializer::NativeAllocation>,
}

struct SelectedImportInput {
    plan: SelectedImportPlan,
    #[cfg(not(target_arch = "wasm32"))]
    allocation: Option<crate::materializer::NativeAllocation>,
}

pub(crate) struct SelectedImportPublication<A, O> {
    checkout: crate::Checkout<A, O>,
    destination: LazySnapshotRef,
    plan: SelectedImportPlan,
    operation: crate::OperationId,
    effects: SelectedImportEffects,
    prepared: bool,
    published: Option<crate::GenerationId>,
    // The frozen plan physically retires before its original payload guard.
    #[cfg(not(target_arch = "wasm32"))]
    plan_allocation: Option<crate::materializer::NativeAllocation>,
}

pub(crate) struct SelectedImportControl<A, O> {
    busy: std::sync::atomic::AtomicBool,
    retained: Mutex<Option<SelectedImportPublication<A, O>>>,
    #[cfg(not(target_arch = "wasm32"))]
    allocation: Option<crate::materializer::NativeAllocation>,
}

impl<A, O> SelectedImportControl<A, O> {
    pub(super) fn new() -> Self {
        Self {
            busy: std::sync::atomic::AtomicBool::new(false),
            retained: Mutex::new(None),
            #[cfg(not(target_arch = "wasm32"))]
            allocation: None,
        }
    }

}

struct SelectedImportCustody<'a, A, O> {
    control: &'a SelectedImportControl<A, O>,
    retained: Option<SelectedImportPublication<A, O>>,
}

impl<A, O> Drop for SelectedImportCustody<'_, A, O> {
    fn drop(&mut self) {
        *self.control.retained.lock().expect("selected import custody lock poisoned")
            = self.retained.take();
        self.control.busy.store(false, std::sync::atomic::Ordering::Release);
    }
}

fn new_selected_import_control<A, O>(
    meter: &LazyTreapWork<'_>,
) -> Result<Arc<SelectedImportControl<A, O>>, LazyWorkspaceError> {
    let bytes = std::mem::size_of::<SelectedImportControl<A, O>>() as u64;
    meter.allocate(bytes)?;
    let mut control = SelectedImportControl::new();
    #[cfg(not(target_arch = "wasm32"))]
    { control.allocation = meter.take_native_owned(bytes)?; }
    Ok(Arc::new(control))
}

/// A source-backed workspace whose unresolved paths remain outside its authored generation.
pub struct LazyWorkspace<A, O, D, S> {
    workspace: Workspace<A, O>,
    source: Arc<D>,
    store: S,
    selected_publication: Arc<SelectedImportControl<A, O>>,
    #[cfg(not(target_arch = "wasm32"))]
    allocation: Option<crate::materializer::NativeAllocation>,
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
            selected_publication: Arc::clone(&self.selected_publication),
            #[cfg(not(target_arch = "wasm32"))]
            allocation: None,
        }
    }
}

impl<A, O, D, S> LazyWorkspace<A, O, D, S> {
    /// Moves this handle into an original-root-funded Arc.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_arc_with_native_work(
        mut self, original: &crate::materializer::NativeMaterializationWork,
    ) -> Result<Arc<Self>, LazyWorkspaceError> {
        drop(self.allocation.take());
        self.allocation = Some(original.claim_allocation(
            std::mem::size_of::<Self>() as u64, 1, crate::WorkCounters::UNCHARGED)?);
        Ok(Arc::new(self))
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
        /// Existing authored binding used to serve this path.
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

#[cfg(not(target_arch = "wasm32"))]
fn authored_record_stat(record: FileRecord, metadata: FileMetadata) -> WorkspaceStat {
    let logical_bytes = record_logical_bytes(record);
    WorkspaceStat {
        file_id: record.file_id, kind: record.kind, link_count: record.link_count,
        logical_bytes, metadata: crate::WorkspaceMetadata::from_engine(metadata),
    }
}

enum SelectedLookupPhase<A: AsyncAuthorityStore, O: AsyncObjectStore> {
    Portable(LazyLookup, std::marker::PhantomData<fn() -> (A, O)>),
    #[cfg(not(target_arch = "wasm32"))]
    Native((ResolvedLazyLookup, NativeInspectState<A, O>)),
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> SelectedLookupPhase<A, O> {
    fn lookup(&self) -> &LazyLookup {
        match self {
            Self::Portable(lookup, _) => lookup,
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(frozen) => &frozen.0.lookup,
        }
    }

    fn portable_owned_bytes(&self) -> u64 {
        match self {
            Self::Portable(lookup, _) => lookup.logical_owned_bytes(),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(_) => 0,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeInspectState<A: AsyncAuthorityStore, O: AsyncObjectStore> {
    reader: crate::PinnedReader<A, O>,
    record: Option<FileRecord>,
    metadata: Option<FileMetadata>,
    allocation: crate::materializer::NativeAllocation,
}

enum LazyInspectBackend<'a, A: AsyncAuthorityStore, O: AsyncObjectStore> {
    Measured(std::marker::PhantomData<(&'a (), A, O)>),
    #[cfg(not(target_arch = "wasm32"))]
    Native(&'a mut NativeInspectState<A, O>),
}

pub(crate) enum LazyReadonlyAccounting<'a> {
    Portable {
        work: WorkCounters,
        budget: WorkBudget,
        cancellation: &'a CancellationToken,
    },
    #[cfg(not(target_arch = "wasm32"))]
    Native {
        original: &'a crate::materializer::NativeMaterializationWork,
        cancellation: &'a CancellationToken,
    },
}

impl LazyReadonlyAccounting<'_> {
    pub(crate) fn finish_measured<T>(self, result: Result<T, LazyWorkspaceError>)
        -> Result<OperationReceipt<T>, LazyWorkspaceError>
    {
        match self {
            Self::Portable { work, .. } => result
                .map(|value| OperationReceipt { value, work })
                .map_err(|error| error.with_prior_work(work)),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { .. } => unreachable!("original work cannot become a standalone receipt"),
        }
    }

    fn check(&self) -> Result<(), LazyWorkspaceError> {
        match self {
            Self::Portable { cancellation, .. } =>
                cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { original, cancellation } => {
                original.admit(WorkCounters::UNCHARGED)?;
                cancellation.check().map_err(|_| LazyWorkspaceError::NativeWork(
                    crate::NativeTreeMaterializationError::Cancelled))
            }
        }
    }

    fn charge(&mut self, delta: WorkCounters) -> Result<(), LazyWorkspaceError> {
        self.check()?;
        match self {
            Self::Portable { work, budget, .. } => {
                work.charge(&delta, budget)?;
                Ok(())
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { original, .. } => original.charge(delta).map_err(Into::into),
        }
    }

    fn decode_error(&self, error: crate::kernel::CanonicalDecodeError) -> LazyWorkspaceError {
        match self {
            Self::Portable { .. } => LazyWorkspaceError::Store(error.to_string()),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { .. } => native_filesystem_error(error.into()),
        }
    }

    fn validate_path(&self, path: &str, limits: crate::VolumeLimits)
        -> Result<(), LazyWorkspaceError>
    {
        self.check()?;
        PortablePath::validate(path, limits).map(|_| ()).map_err(|error| match self {
            Self::Portable { .. } => LazyWorkspaceError::Workspace(error.to_string()),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { .. } => LazyWorkspaceError::NamespacePath(
                crate::kernel::NamespacePathError::Path(error)),
        })
    }
}

/// A lazy value retaining its original native allocation when present.
pub enum LazyReadonlyNode<T> {
    /// Value produced by ordinary accounting.
    Portable(T),
    #[cfg(not(target_arch = "wasm32"))]
    /// Value and its original native allocation.
    Native(NativeOwnedCoreState<T>),
}

impl<T> LazyReadonlyNode<T> {
    fn as_state_mut(&mut self) -> &mut T {
        match self {
            Self::Portable(value) => value,
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(value) => &mut value.value,
        }
    }

    /// Borrows the value while retaining its allocation.
    pub fn as_state(&self) -> &T {
        match self {
            Self::Portable(value) => value,
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(value) => value.as_state(),
        }
    }
}

enum FrozenRead {
    Regular(u64),
    Range(crate::ByteRange),
    Link,
}

enum LazyPromotionMutation<'a> {
    Directory,
    Parents,
    File(acyclic_stream::OwnedBytes),
    Link(crate::kernel::OwnedSourceChunk),
    Special(FileKind, Option<(u32, u32)>),
    Metadata(FileMetadata),
    Identity(FileId),
    Restore(FileRecord),
    HardLink(&'a str),
    Write(u64, crate::kernel::OwnedSourceChunk),
}

/// Streams one version-pinned demanded file into the SDK blob builder while preserving the demand provider's exact work receipt.
struct DemandBlobSource<'a, D> {
    provider: &'a D,
    source: SourceReference,
    path: NamespacePath,
    version: SourceVersion,
    offset: u64,
    length: u64,
    work: Option<WorkCounters>,
    failure: Option<DemandError>,
    source_used: Option<&'a std::sync::atomic::AtomicBool>,
    expected_source: Option<SourceReference>,
    #[cfg(not(target_arch = "wasm32"))]
    failure_storage: Option<crate::kernel::PreparedBlobDemandFailure>,
}

#[cfg(not(target_arch = "wasm32"))]
impl<D> DemandBlobSource<'_, D> {
    fn original_source_error(&mut self, error: DemandError) -> crate::FsError {
        let storage = self.failure_storage.take().expect("native source failure storage is prepared");
        crate::kernel::BlobBuildError::Demand(storage.failed(error)).into()
    }
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
        if self.expected_source.is_some_and(|expected| self.provider.reference() != expected) {
            self.work = Some(WorkCounters::default());
            self.failure = Some(DemandError::StaleSource);
            return Err(std::io::Error::other("frozen demand source epoch changed"));
        }
        if let Some(used) = self.source_used {
            used.store(true, std::sync::atomic::Ordering::Release);
        }
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
        if self.expected_source.is_some_and(|expected| self.provider.reference() != expected) {
            self.failure = Some(DemandError::StaleSource);
            return Err(std::io::Error::other("frozen demand source epoch changed"));
        }
        let chunk = receipt.value;
        if chunk.bytes.is_empty() || u64::try_from(chunk.bytes.len()).unwrap_or(u64::MAX) > requested {
            self.failure = Some(DemandError::StaleVersion);
            return Err(std::io::Error::other(
                "version-pinned demand read was incomplete",
            ));
        }
        self.offset = self
            .offset
            .checked_add(u64::try_from(chunk.bytes.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| std::io::Error::other("demand source offset overflow"))?;
        destination
            .get_mut(..chunk.bytes.len())
            .ok_or_else(|| std::io::Error::other("demand source exceeded read bound"))?
            .copy_from_slice(&chunk.bytes);
        Ok(chunk.bytes.len())
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_with_native_work<'a>(
        &'a mut self, destination: &'a mut [u8],
        original: &'a crate::NativeMaterializationWork, cancellation: &'a CancellationToken,
    ) -> Result<usize, crate::FsError> {
        original.admit(WorkCounters::UNCHARGED)?;
        cancellation.check()?;
        let remaining = self.length.saturating_sub(self.offset);
        if remaining == 0 || destination.is_empty() { return Ok(0); }
        if let Some(storage) = &self.failure_storage {
            if !storage.shares_ledger(original) {
                return Err(crate::NativeTreeMaterializationError::InvalidLayout.into());
            }
        } else {
            self.failure_storage = Some(crate::kernel::PreparedBlobDemandFailure::new(original)?);
        }
        let requested = remaining.min(u64::try_from(destination.len()).map_err(|_| crate::WorkError::Overflow)?);
        if self.expected_source.is_some_and(|expected| self.provider.reference() != expected) {
            return Err(self.original_source_error(DemandError::StaleSource));
        }
        // Reserve the destination copy before source I/O.
        let copy = original.reserve_work(crate::WorkCounters { bytes_copied: requested, ..crate::WorkCounters::UNCHARGED })?;
        let before = original.receipt();
        let result = self.provider.read_range(
            self.source, &self.path, self.version, self.offset, requested, original, cancellation,
        ).await;
        let after = original.receipt();
        if (after.source_bytes_read > before.source_bytes_read
            || after.backend_read_operations > before.backend_read_operations)
            && let Some(used) = self.source_used
        {
            used.store(true, std::sync::atomic::Ordering::Release);
        }
        let chunk = match result {
            Ok(chunk) => chunk,
            Err(error) => return Err(self.original_source_error(error)),
        };
        if self.expected_source.is_some_and(|expected| self.provider.reference() != expected) {
            return Err(self.original_source_error(DemandError::StaleSource));
        }
        let count = u64::try_from(chunk.bytes.len()).map_err(|_| crate::WorkError::Overflow)?;
        if count == 0 || count > requested {
            return Err(self.original_source_error(DemandError::StaleVersion));
        }
        let next_offset = self.offset.checked_add(count).ok_or(crate::WorkError::Overflow)?;
        original.admit(WorkCounters::UNCHARGED)?;
        cancellation.check()?;
        let Some(destination) = destination.get_mut(..chunk.bytes.len()) else {
            return Err(self.original_source_error(DemandError::StaleVersion));
        };
        destination.copy_from_slice(&chunk.bytes);
        { let mut copy = copy; copy.record_completed(crate::WorkCounters { bytes_copied: count, ..crate::WorkCounters::UNCHARGED }) }?;
        self.offset = next_offset;
        Ok(chunk.bytes.len())
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
    /// Whether the overlay records no fact for the entry, which the listing proved of its whole directory: the listed node is then its node.
    pub unobserved: bool,
}

enum ExactifyEntries {
    Lazy(std::vec::IntoIter<LazyDirectoryEntry>),
    Authored(std::vec::IntoIter<WorkspaceDirectoryEntry>),
}

impl Iterator for ExactifyEntries {
    type Item = (LogicalName, bool);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Lazy(entries) => entries.next().map(|entry|
                (entry.name, entry.kind == SourceNodeKind::Directory)),
            Self::Authored(entries) => entries.next().map(|entry|
                (entry.name, entry.kind == FileKind::Directory)),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Default)]
struct DirectoryNativeOwnership {
    input_borrowed: bool,
    allocation: Option<crate::materializer::NativeAllocation>,
}

#[cfg(not(target_arch = "wasm32"))]
impl DirectoryNativeOwnership {
    fn mark_input_borrowed(&mut self) { self.input_borrowed = true; }

    fn unclaimed_retained_bytes(&self, bytes: u64) -> u64 {
        if self.input_borrowed || self.allocation.is_some() { 0 } else { bytes }
    }

    fn attach_admitted(
        &mut self, bytes: u64, aggregate: &mut crate::materializer::NativeAllocation,
    ) -> Result<(), crate::materializer::NativeTreeMaterializationError> {
        if !self.input_borrowed && self.allocation.is_none() {
            self.allocation = Some(aggregate.split_off(bytes)?);
        }
        Ok(())
    }
}

/// Opaque bounded continuation for a merged source/authored directory scan.
pub struct LazyDirectoryCursor {
    source: SourceReference,
    overlay: LazyOverlayId,
    directory: NamespacePath,
    phase: LazyDirectoryPhase,
    snapshot: Option<LazySnapshotId>,
    #[cfg(not(target_arch = "wasm32"))]
    native_directory_ownership: DirectoryNativeOwnership,
}

impl Clone for LazyDirectoryCursor {
    fn clone(&self) -> Self {
        Self {
            source: self.source, overlay: self.overlay, directory: self.directory.clone(),
            phase: self.phase.clone(), snapshot: self.snapshot,
            #[cfg(not(target_arch = "wasm32"))]
            native_directory_ownership: DirectoryNativeOwnership::default(),
        }
    }
}

impl std::fmt::Debug for LazyDirectoryCursor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("LazyDirectoryCursor").field("source", &self.source)
            .field("overlay", &self.overlay).field("directory", &self.directory)
            .field("phase", &self.phase).field("snapshot", &self.snapshot).finish()
    }
}

impl PartialEq for LazyDirectoryCursor {
    fn eq(&self, other: &Self) -> bool {
        self.source == other.source && self.overlay == other.overlay
            && self.directory == other.directory && self.phase == other.phase && self.snapshot == other.snapshot
    }
}

impl Eq for LazyDirectoryCursor {}

fn lazy_cursor_filesystem_error(error: crate::FsError) -> LazyWorkspaceError {
    match error {
        crate::FsError::Work(error) => LazyWorkspaceError::Demand(DemandError::Work(error)),
        crate::FsError::Cancelled(_) => LazyWorkspaceError::Cancelled,
        error => {
            #[cfg(not(target_arch = "wasm32"))]
            { native_filesystem_error(error) }
            #[cfg(target_arch = "wasm32")]
            { LazyWorkspaceError::Filesystem(error) }
        }
    }
}

fn lazy_cursor_wire_error(error: crate::FsError) -> LazyWorkspaceError {
    match error {
        crate::FsError::Decode(_) => LazyWorkspaceError::StaleCursor,
        error => lazy_cursor_filesystem_error(error),
    }
}

fn lazy_cursor_reservation_filesystem_error(error: std::collections::TryReserveError) -> crate::FsError {
    #[cfg(not(target_arch = "wasm32"))]
    { crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::AllocationFailed(error)) }
    #[cfg(target_arch = "wasm32")]
    { let _ = error; crate::kernel::CanonicalDecodeError::AllocationFailed.into() }
}

fn lazy_cursor_fixed_read_step(
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    step: crate::kernel::ReadAllocationStep,
) -> Result<(), crate::FsError> {
    use crate::kernel::ReadAllocationStep;
    match step {
        ReadAllocationStep::BeforeCopy(bytes) => accounting.admit(WorkCounters {
            bytes_copied: bytes, ..WorkCounters::UNCHARGED
        }),
        ReadAllocationStep::Copied(bytes) => accounting.record_completed(WorkCounters {
            bytes_copied: bytes, ..WorkCounters::UNCHARGED
        }),
        ReadAllocationStep::BeforeHash(bytes) => accounting.admit(WorkCounters {
            bytes_hashed: bytes, ..WorkCounters::UNCHARGED
        }),
        ReadAllocationStep::Hashed(bytes) => accounting.record_completed(WorkCounters {
            bytes_hashed: bytes, ..WorkCounters::UNCHARGED
        }),
        ReadAllocationStep::BeforeItem => accounting.admit(WorkCounters {
            items_examined: 1, ..WorkCounters::UNCHARGED
        }),
        ReadAllocationStep::Item => accounting.record_completed(WorkCounters {
            items_examined: 1, ..WorkCounters::UNCHARGED
        }),
        ReadAllocationStep::Allocate { .. } | ReadAllocationStep::Capacity { .. }
        | ReadAllocationStep::ReserveFailed(_) =>
            unreachable!("fixed lazy-cursor fields never allocate"),
    }
}

impl LazyDirectoryCursor {
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn mark_input_borrowed(&mut self) {
        self.native_directory_ownership.mark_input_borrowed();
        match &mut self.phase {
            LazyDirectoryPhase::Source(Some(cursor)) => cursor.mark_input_borrowed(),
            LazyDirectoryPhase::Authored(Some(after)) => after.native_ownership.mark_input_borrowed(),
            LazyDirectoryPhase::Source(None) | LazyDirectoryPhase::Authored(None) => {}
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn unclaimed_retained_bytes(&self) -> Result<u64, LazyWorkspaceError> {
        let directory = self.native_directory_ownership.unclaimed_retained_bytes(self.directory.retained_bytes());
        let continuation = match &self.phase {
            LazyDirectoryPhase::Source(Some(cursor)) => cursor.unclaimed_retained_bytes(),
            LazyDirectoryPhase::Authored(Some(after)) => after.native_ownership
                .unclaimed_retained_bytes(after.name.retained_bytes() as u64),
            LazyDirectoryPhase::Source(None) | LazyDirectoryPhase::Authored(None) => 0,
        };
        directory.checked_add(continuation).ok_or_else(counter_overflow)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn attach_native_custody(
        &mut self,
        allocation: &mut crate::materializer::NativeAllocation,
    ) -> Result<(), crate::materializer::NativeTreeMaterializationError> {
        self.native_directory_ownership.attach_admitted(self.directory.retained_bytes(), allocation)?;
        if let LazyDirectoryPhase::Authored(Some(after)) = &mut self.phase {
            after.native_ownership.attach_admitted(after.name.retained_bytes() as u64, allocation)?;
        }
        // SourceCursor already carries its producer's actual original guard.
        Ok(())
    }

    /// Encodes this original continuation into one bounded canonical owned buffer.
    pub fn encode_measured(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<Vec<u8>>, LazyWorkspaceError> {
        let mut work = WorkCounters::UNCHARGED;
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let mut accounting = crate::kernel::allocation::KernelAccounting::borrowed_standalone(
            &mut allocations, &mut work, budget, cancellation,
        );
        let result = self.encode_with_accounting(&mut accounting, cancellation);
        drop(accounting);
        result.map(|(value, _)| OperationReceipt { value, work })
            .map_err(|error| error.into_measured_failure()
                .map_with_prior_work(work, std::convert::identity))
    }

    fn encode_with_accounting(
        &self,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Vec<u8>, crate::kernel::allocation::KernelOwnedAllocation), LazyWorkspaceError> {
            const DOMAIN: &[u8] = b"acyclic-fs-lazy-directory-cursor-v1\0";
        let mut allocation = accounting.claim_owned(0, 0).map_err(lazy_cursor_filesystem_error)?;
        let mut owned_bytes = 0;
        let result = (|| {
            accounting.check().map_err(lazy_cursor_filesystem_error)?;
            let passes = if matches!(&self.phase, LazyDirectoryPhase::Source(Some(_))) { 2 } else { 1 };
            accounting.charge(WorkCounters {
                items_examined: crate::foundation::usize_to_u64(self.directory.depth()) * passes,
                ..WorkCounters::UNCHARGED
            }).map_err(lazy_cursor_filesystem_error)?;
            let directory_bytes = self.directory.cursor_encoded_len().map_err(directory_namespace_error)?;
            let continuation_bytes = match &self.phase {
                LazyDirectoryPhase::Source(Some(cursor)) => cursor.cursor_encoded_len().map_err(LazyWorkspaceError::from)?,
                LazyDirectoryPhase::Authored(Some(name)) => name.cursor_encoded_len().map_err(directory_namespace_error)?,
                LazyDirectoryPhase::Source(None) | LazyDirectoryPhase::Authored(None) => 0,
            };
            let snapshot_bytes = if self.snapshot.is_some() { 32 } else { 0 };
            let size = DOMAIN.len().checked_add(2 + 24 + 32 + 1 + 1)
                .and_then(|size| size.checked_add(snapshot_bytes))
                .and_then(|size| size.checked_add(directory_bytes))
                .and_then(|size| size.checked_add(continuation_bytes)).ok_or_else(counter_overflow)?;
            let mut encoder = lazy_encoder(DOMAIN, size, accounting, &mut allocation, &mut owned_bytes, false).map_err(lazy_cursor_filesystem_error)?;
            let fixed = WorkCounters {
                bytes_encoded: (24 + 32 + 1 + snapshot_bytes) as u64,
                bytes_copied: (16 + 32 + snapshot_bytes) as u64,
                ..WorkCounters::UNCHARGED
            };
            accounting.admit(fixed).map_err(lazy_cursor_filesystem_error)?;
            encoder.fixed(&self.source.identity);
            encoder.u64(self.source.epoch);
            encoder.fixed(&self.overlay.into_bytes());
            match self.snapshot {
                None => encoder.u8(0),
                Some(snapshot) => { encoder.u8(1); encoder.fixed(&snapshot.into_bytes()); }
            }
            accounting.record_completed(fixed).map_err(lazy_cursor_filesystem_error)?;
            self.directory.encode_cursor_into_with_accounting(&mut encoder, accounting, cancellation)
                .map_err(lazy_cursor_filesystem_error)?;
            let tag = WorkCounters { bytes_encoded: 1, ..WorkCounters::UNCHARGED };
            accounting.admit(tag).map_err(lazy_cursor_filesystem_error)?;
            encoder.u8(match &self.phase {
                LazyDirectoryPhase::Source(None) => 0,
                LazyDirectoryPhase::Source(Some(_)) => 1,
                LazyDirectoryPhase::Authored(None) => 2,
                LazyDirectoryPhase::Authored(Some(_)) => 3,
            });
            accounting.record_completed(tag).map_err(lazy_cursor_filesystem_error)?;
            match &self.phase {
                LazyDirectoryPhase::Source(Some(cursor)) => cursor
                    .encode_cursor_into(&mut encoder, accounting, cancellation)
                    .map_err(LazyWorkspaceError::from)?,
                LazyDirectoryPhase::Authored(Some(after)) => after.name
                    .encode_cursor_into_with_accounting(&mut encoder, accounting, cancellation)
                    .map_err(lazy_cursor_filesystem_error)?,
                LazyDirectoryPhase::Source(None) | LazyDirectoryPhase::Authored(None) => {}
            }
            accounting.check().map_err(lazy_cursor_filesystem_error)?;
            let bytes = encoder.finish();
            if bytes.len() != size { return Err(LazyWorkspaceError::StaleCursor); }
            accounting.charge(WorkCounters {
                output_bytes: crate::foundation::usize_to_u64(bytes.len()), ..WorkCounters::UNCHARGED
            }).map_err(lazy_cursor_filesystem_error)?;
            Ok(bytes)
        })();
        match result {
            Ok(bytes) => Ok((bytes, allocation)),
            Err(error) => {
                accounting.release_owned(&mut allocation, owned_bytes).map_err(lazy_cursor_filesystem_error)?;
                Err(error)
            }
        }
    }

    /// Decodes a bounded continuation using the actual workspace profile and limits.
    pub fn decode_measured(
        bytes: &[u8],
        profile: crate::model::FilesystemProfile,
        limits: crate::model::VolumeLimits,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<Self>, LazyWorkspaceError> {
        let mut work = WorkCounters::UNCHARGED;
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let mut accounting = crate::kernel::allocation::KernelAccounting::borrowed_standalone(
            &mut allocations, &mut work, budget, cancellation,
        );
        let result = Self::decode_with_accounting(bytes, profile, limits, &mut accounting, cancellation);
        drop(accounting);
        result.map(|(value, _)| OperationReceipt { value, work })
            .map_err(|error| error.into_measured_failure()
                .map_with_prior_work(work, std::convert::identity))
    }

    fn decode_with_accounting(
        bytes: &[u8],
        profile: crate::model::FilesystemProfile,
        limits: crate::model::VolumeLimits,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Self, crate::kernel::allocation::KernelOwnedAllocation), LazyWorkspaceError> {
        const DOMAIN: &[u8] = b"acyclic-fs-lazy-directory-cursor-v1\0";
        let mut allocation = accounting.claim_owned(0, 0).map_err(lazy_cursor_filesystem_error)?;
        let mut owned_bytes = 0;
        let result = (|| {
            let maximum_bytes = accounting.remaining().map_err(lazy_cursor_filesystem_error)?
                .source_bytes_read.min(limits.maximum_object_bytes);
            accounting.charge(WorkCounters {
                source_bytes_read: crate::foundation::usize_to_u64(bytes.len()),
                ..WorkCounters::UNCHARGED
            }).map_err(lazy_cursor_filesystem_error)?;
            let mut decoder = crate::kernel::Decoder::new(
                bytes, DOMAIN, 1, maximum_bytes,
                &mut |step| lazy_cursor_fixed_read_step(accounting, step),
            ).map_err(lazy_cursor_wire_error)?;
            let source = SourceReference {
                identity: decoder.fixed(&mut |step| lazy_cursor_fixed_read_step(accounting, step))
                    .map_err(lazy_cursor_wire_error)?,
                epoch: decoder.u64(&mut |step| lazy_cursor_fixed_read_step(accounting, step))
                    .map_err(lazy_cursor_wire_error)?,
            };
            let overlay = LazyOverlayId::from_bytes(decoder
                .fixed(&mut |step| lazy_cursor_fixed_read_step(accounting, step))
                .map_err(lazy_cursor_wire_error)?);
            let snapshot = match decoder
                .u8(&mut |step| lazy_cursor_fixed_read_step(accounting, step))
                .map_err(lazy_cursor_wire_error)? {
                0 => None,
                1 => Some(LazySnapshotId(decoder
                    .fixed(&mut |step| lazy_cursor_fixed_read_step(accounting, step))
                    .map_err(lazy_cursor_wire_error)?)),
                _ => return Err(LazyWorkspaceError::StaleCursor),
            };
            let (directory, mut directory_allocation) = NamespacePath::decode_cursor_with_accounting(
                &mut decoder, profile, limits, accounting, cancellation,
            ).map_err(lazy_cursor_filesystem_error)?;
            if let Err(error) = allocation.absorb(&mut directory_allocation) {
                drop(directory);
                drop(directory_allocation);
                return Err(lazy_cursor_filesystem_error(error));
            }
            owned_bytes = directory.retained_bytes();
            drop(directory_allocation);
            let phase = match decoder
                .u8(&mut |step| lazy_cursor_fixed_read_step(accounting, step))
                .map_err(lazy_cursor_wire_error)? {
                0 => LazyDirectoryPhase::Source(None),
                1 => {
                    let (cursor, mut cursor_allocation) = SourceCursor::decode_cursor(
                        &mut decoder, source, &directory, profile, limits, accounting, cancellation,
                    ).map_err(LazyWorkspaceError::from)?;
                    let next_owned = match owned_bytes.checked_add(cursor.retained_bytes()) {
                        Some(bytes) => bytes,
                        None => {
                            let bytes = cursor.retained_bytes();
                            drop(cursor);
                            accounting.release_owned(&mut cursor_allocation, bytes)
                                .map_err(lazy_cursor_filesystem_error)?;
                            return Err(counter_overflow());
                        }
                    };
                    if let Err(error) = allocation.absorb(&mut cursor_allocation) {
                        drop(cursor);
                        drop(cursor_allocation);
                        return Err(lazy_cursor_filesystem_error(error));
                    }
                    owned_bytes = next_owned;
                    drop(cursor_allocation);
                    LazyDirectoryPhase::Source(Some(cursor))
                }
                2 => LazyDirectoryPhase::Authored(None),
                3 => {
                    let (name, mut name_allocation) = LogicalName::decode_cursor_with_accounting(
                        &mut decoder, profile, limits.maximum_component_bytes, accounting, cancellation,
                    ).map_err(lazy_cursor_filesystem_error)?;
                    let next_owned = match owned_bytes.checked_add(name.retained_bytes() as u64) {
                        Some(bytes) => bytes,
                        None => {
                            let bytes = name.retained_bytes() as u64;
                            drop(name);
                            accounting.release_owned(&mut name_allocation, bytes)
                                .map_err(lazy_cursor_filesystem_error)?;
                            return Err(counter_overflow());
                        }
                    };
                    if let Err(error) = allocation.absorb(&mut name_allocation) {
                        drop(name);
                        drop(name_allocation);
                        return Err(lazy_cursor_filesystem_error(error));
                    }
                    owned_bytes = next_owned;
                    drop(name_allocation);
                    LazyDirectoryPhase::Authored(Some(LazyDirectoryAfter::new(name)))
                }
                _ => return Err(LazyWorkspaceError::StaleCursor),
            };
            decoder.finish().map_err(|_| LazyWorkspaceError::StaleCursor)?;
            accounting.check().map_err(lazy_cursor_filesystem_error)?;
            Ok(Self { source, overlay, directory, phase, snapshot,
                #[cfg(not(target_arch = "wasm32"))]
                native_directory_ownership: DirectoryNativeOwnership::default(),
            })
        })();
        match result {
            Ok(cursor) => Ok((cursor, allocation)),
            Err(error) => {
                accounting.release_owned(&mut allocation, owned_bytes).map_err(lazy_cursor_filesystem_error)?;
                Err(error)
            }
        }
    }

    /// Decodes on the original ledger using the workspace's actual profile and limits.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn decode_with_native_work(
        bytes: &[u8],
        profile: crate::model::FilesystemProfile,
        limits: crate::model::VolumeLimits,
        original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(Self, crate::NativeAllocation), (LazyWorkspaceError, Option<crate::NativeAllocation>)> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(original, cancellation);
        match Self::decode_with_accounting(bytes, profile, limits, &mut accounting, cancellation) {
            Ok((mut cursor, crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(mut allocation))) => {
                let attached = cursor.attach_native_custody(&mut allocation).and_then(|()| {
                    if let LazyDirectoryPhase::Source(Some(source)) = &mut cursor.phase {
                        source.attach_native_allocation(&mut allocation)?;
                    }
                    Ok(())
                });
                if let Err(error) = attached {
                    drop(cursor);
                    drop(allocation);
                    return Err((LazyWorkspaceError::NativeWork(error), None));
                }
                Ok((cursor, allocation))
            }
            Ok((cursor, crate::kernel::allocation::KernelOwnedAllocation::Standalone)) => {
                drop(cursor);
                Err((LazyWorkspaceError::OriginalSnapshotUnavailable, None))
            }
            Err(LazyWorkspaceError::Demand(DemandError::Work(error))) => Err((LazyWorkspaceError::NativeWork(
                crate::NativeTreeMaterializationError::Work(error)), None)),
            Err(LazyWorkspaceError::Cancelled) => Err((LazyWorkspaceError::NativeWork(
                crate::NativeTreeMaterializationError::Cancelled), None)),
            Err(error) => Err(error.into_native_failure()),
        }
    }

    fn claimable_directory_bytes(&self) -> u64 {
        let bytes = self.directory.retained_bytes();
        #[cfg(not(target_arch = "wasm32"))]
        { self.native_directory_ownership.unclaimed_retained_bytes(bytes) }
        #[cfg(target_arch = "wasm32")]
        { bytes }
    }

    fn claimable_source_cursor_bytes(cursor: &SourceCursor) -> u64 {
        #[cfg(not(target_arch = "wasm32"))]
        { cursor.unclaimed_retained_bytes() }
        #[cfg(target_arch = "wasm32")]
        { cursor.retained_bytes() }
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
    Authored(Option<LazyDirectoryAfter>),
}

struct LazyDirectoryAfter {
    name: LogicalName,
    #[cfg(not(target_arch = "wasm32"))]
    native_ownership: DirectoryNativeOwnership,
}

impl LazyDirectoryAfter {
    fn new(name: LogicalName) -> Self {
        Self { name,
            #[cfg(not(target_arch = "wasm32"))]
            native_ownership: DirectoryNativeOwnership::default(),
        }
    }
}

impl std::ops::Deref for LazyDirectoryAfter {
    type Target = LogicalName;
    fn deref(&self) -> &Self::Target { &self.name }
}

impl Clone for LazyDirectoryAfter {
    fn clone(&self) -> Self { Self::new(self.name.clone()) }
}

impl std::fmt::Debug for LazyDirectoryAfter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.name, formatter)
    }
}

impl PartialEq for LazyDirectoryAfter {
    fn eq(&self, other: &Self) -> bool { self.name == other.name }
}

impl Eq for LazyDirectoryAfter {}

/// One bounded page from the merged lazy directory view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LazyDirectoryPage {
    /// At most the requested number of visible entries.
    pub entries: Vec<LazyDirectoryEntry>,
    /// Opaque continuation, absent after the authored phase is exhausted.
    pub next: Option<LazyDirectoryCursor>,
}

/// An original SDK failure and its remaining native diagnostic custody.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Error)]
#[error("{source}")]
pub struct NativeLazyWorkspaceCause {
    /// The complete original cause, including its intrinsic SDK guards.
    #[source]
    pub source: WorkspaceError,
    /// Extra diagnostic ownership retires only after the source is dropped.
    pub custody: Option<crate::materializer::NativeAllocation>,
}

/// A source-page primary failure in its preadmitted physical slot.
pub struct LazyDirectoryCleanupCause {
    primary: Box<Option<LazyWorkspaceError>>,
    work: WorkCounters,
    prefix_error: Option<crate::WorkError>,
    prefix_prior: WorkCounters,
    #[cfg(not(target_arch = "wasm32"))]
    _custody: Option<crate::materializer::NativeAllocation>,
}

impl LazyDirectoryCleanupCause {
    fn allocation_bytes() -> u64 {
        std::mem::size_of::<Option<LazyWorkspaceError>>() as u64
    }

    fn new(
        primary: Box<Option<LazyWorkspaceError>>,
        #[cfg(not(target_arch = "wasm32"))]
        custody: Option<crate::materializer::NativeAllocation>,
    ) -> Self {
        Self {
            primary,
            work: WorkCounters::UNCHARGED,
            prefix_error: None,
            prefix_prior: WorkCounters::UNCHARGED,
            #[cfg(not(target_arch = "wasm32"))]
            _custody: custody,
        }
    }

    fn store(&mut self, error: LazyWorkspaceError) {
        assert!(self.primary.is_none(), "source-page primary slot is occupied");
        self.work = error.spent_work();
        *self.primary = Some(error);
    }

    fn primary(&self) -> &LazyWorkspaceError {
        self.primary.as_ref().as_ref().expect("completed source-page primary slot")
    }

    fn into_primary(mut self) -> LazyWorkspaceError {
        self.primary.take().expect("completed source-page primary slot")
    }

    fn into_cleanup(
        self,
        cleanup: DemandError,
        publication: Option<Box<dyn crate::demand::SourceDirectoryPublication>>,
    ) -> LazyWorkspaceError {
        LazyWorkspaceError::SourcePageCleanup { cleanup, publication, primary: self }
    }
}

impl std::fmt::Debug for LazyDirectoryCleanupCause {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(error) = &self.prefix_error {
            formatter.debug_struct("LazyDirectoryCleanupCause")
                .field("prefix_error", error)
                .field("prefix_prior", &self.prefix_prior)
                .field("primary_work", &self.work)
                .field("primary", self.primary())
                .finish()
        } else {
            std::fmt::Debug::fmt(self.primary(), formatter)
        }
    }
}

impl std::fmt::Display for LazyDirectoryCleanupCause {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(error) = &self.prefix_error {
            write!(formatter, "source-page work prefixes cannot be combined: {error}; primary: {}",
                self.primary())
        } else {
            std::fmt::Display::fmt(self.primary(), formatter)
        }
    }
}

impl std::error::Error for LazyDirectoryCleanupCause {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.prefix_error {
            Some(error) => Some(error),
            None => Some(self.primary()),
        }
    }
}

/// Fail-closed lazy workspace errors.
#[derive(Debug, Error)]
pub enum LazyWorkspaceError {
    /// Authenticated workspace operation failed.
    #[error("workspace operation failed: {0}")]
    Workspace(String),
    /// Original workspace cause retained without allocating a diagnostic.
    #[error(transparent)]
    WorkspaceCause(WorkspaceError),
    /// The actual workspace failure and a subsequent prefix-recording invariant failure remain distinct; neither replaces the other.
    #[error("workspace operation failed: {source}; recording its prefix failed: {accounting}")]
    WorkspacePrefixFailure {
        /// Original SDK cause, including its intrinsic payload custody.
        #[source]
        source: WorkspaceError,
        /// Actual checked-accounting or poison cause, not a synthetic cancellation.
        accounting: WorkspaceError,
    },
    /// Original native SDK cause and its actual remaining allocation custody.
    #[cfg(not(target_arch = "wasm32"))]
    #[error(transparent)]
    NativeWorkspaceCause(NativeLazyWorkspaceCause),
    /// Canonical filesystem failure retains its original typed cause.
    #[error(transparent)]
    Filesystem(crate::FsError),
    /// Canonical namespace validation retains its original typed cause.
    #[error(transparent)]
    NamespacePath(crate::kernel::NamespacePathError),
    /// Original native operation denial or failure, retained without formatting.
    #[cfg(not(target_arch = "wasm32"))]
    #[error(transparent)]
    NativeWork(#[from] crate::materializer::NativeTreeMaterializationError),
    /// Original accounting-state construction failed before a heap existed.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("native lazy accounting construction failed: {cause}")]
    NativeConstruction {
        /// Original constructor denial or cancellation.
        #[source]
        cause: crate::materializer::NativeTreeMaterializationError,
        /// Actual constructor prefix, including only completed admissions.
        work: WorkCounters,
    },
    /// Demand provider operation failed.
    #[error("source demand failed: {0}")]
    Demand(#[from] DemandError),
    /// Source-page processing and its actual publication abort both failed.
    #[error("source-page processing failed: {primary}; publication abort failed: {cleanup}")]
    SourcePageCleanup {
        /// The original typed SDK abort cause, including intrinsic custody.
        cleanup: DemandError,
        /// The original publication owner retained by the failed abort.
        publication: Option<Box<dyn crate::demand::SourceDirectoryPublication>>,
        /// The full primary failure; its box claim retires last.
        #[source]
        primary: LazyDirectoryCleanupCause,
    },
    /// Companion state failed.
    #[error("lazy state failed: {0}")]
    Store(String),
    /// Durable state changed too frequently to complete a bounded retry.
    #[error("lazy state changed concurrently")]
    Concurrent,
    /// The SDK acknowledgement lacks the original logical snapshot evidence.
    #[error("original selected-import snapshot is unavailable")]
    OriginalSnapshotUnavailable,
    /// DATA is durable but no authentic original logical witness is available.
    #[error("original selected-import operation {operation:?} is indeterminate")]
    SelectedImportIndeterminate {
        /// The original identity; a caller must not relaunch against the head.
        operation: crate::OperationId,
    },
    /// DATA is authentic but its exact original lazy write proof is unavailable.
    #[error("original lazy write operation {operation:?} is indeterminate")]
    WriteIndeterminate {
        /// Original identity, never permission to relaunch from HEAD.
        operation: crate::OperationId,
    },
    /// An existing original operation has different frozen input or basis.
    #[error("original lazy write operation {operation:?} has different input")]
    WriteInputConflict {
        /// The authentic original operation, not an inferred authority.
        operation: crate::OperationId,
    },
    /// The original selected seal returned a semantic publication decision.
    #[error("selected import publication was not committed: {0:?}")]
    SelectedPublication(crate::CheckoutCommitOutcome),
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
    /// Exact traversal was cancelled before every path was captured.
    #[error("lazy exact scan was cancelled")]
    Cancelled,
    /// Exact traversal exceeded an explicit work budget.
    #[error("lazy exact scan exceeded its work budget: {0}")]
    Work(String),
    /// Recoverable failure retaining the exact work of its nested engine.
    #[error("{0}")]
    SpentWork(Box<crate::OperationFailure<Self>>),
    /// This provider does not implement the original native accounting families.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("lazy state provider does not support original native work")]
    NativeUnsupported,
    /// A transferred payload belongs to a different original native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("native lazy payload belongs to a different original ledger")]
    NativeLedgerMismatch,
    /// Actual native record IO failure, retaining its owned cause.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("{0}")]
    NativeRecordIo(#[source] std::io::Error, Option<crate::materializer::NativeAllocation>),
    /// Actual native record decoding failure, retaining its owned cause.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("{0}")]
    NativeRecordJson(#[source] serde_json::Error, Option<crate::materializer::NativeAllocation>),
    /// The original record lock exceeded its transaction deadline.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("native lazy record lock timed out: {}", .0.display())]
    NativeRecordLockTimeout(std::path::PathBuf, Option<crate::materializer::NativeAllocation>),
    /// A different original owner holds the record namespace.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("native lazy record ownership conflict: {}", .0.display())]
    NativeRecordOwnershipConflict(std::path::PathBuf, Option<crate::materializer::NativeAllocation>),
    /// The original nonterminal journal cannot be discarded.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("native lazy materialization is still in progress")]
    NativeRecordMaterializationInProgress,
    /// The record's actual content does not match its canonical address.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("native lazy record integrity failure")]
    NativeRecordIntegrity,
    /// The existing durable context transaction was incompatible.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("native lazy context transaction is incompatible: {0}")]
    NativeRecordContextTransaction(String, Option<crate::materializer::NativeAllocation>),
    /// The original log contains an intact frame that cannot be decoded.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("native lazy record log is corrupt: {}", .0.display())]
    NativeRecordCorrupt(std::path::PathBuf, Option<crate::materializer::NativeAllocation>),
}

impl LazyWorkspaceError {
    /// Moves original native diagnostic custody without allocating or changing its cause.
    #[cfg(not(target_arch = "wasm32"))]
    #[must_use]
    pub fn into_native_failure(mut self) -> (Self, Option<crate::materializer::NativeAllocation>) {
        let allocation = self.take_native_failure_allocation();
        (self, allocation)
    }

    /// Fuses paid diagnostic custody with an existing original frame token.
    #[cfg(not(target_arch = "wasm32"))]
    #[must_use]
    pub fn into_native_failure_with_frame(
        mut self, mut frame: Option<crate::materializer::NativeAllocation>,
    ) -> (Self, Option<crate::materializer::NativeAllocation>) {
        if let Some(active_frame) = frame.as_mut() {
            if let Some(paid) = self.native_failure_allocation_mut()
                && let Some(allocation) = paid.as_mut()
                && active_frame.absorb(allocation).is_ok()
            {
                // Successful absorption emptied this field without freeing data.
                drop(paid.take());
            }
            (self, frame)
        } else {
            self.into_native_failure()
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn restore_native_failure_custody(
        mut self, mut allocation: Option<crate::materializer::NativeAllocation>,
    ) -> Self {
        if let Some(paid) = self.native_failure_allocation_mut()
            && paid.is_none()
        {
            *paid = allocation.take();
        }
        self
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_failure_allocation_mut(
        &mut self,
    ) -> Option<&mut Option<crate::materializer::NativeAllocation>> {
        match self {
            Self::NativeRecordIo(_, allocation)
            | Self::NativeRecordJson(_, allocation)
            | Self::NativeRecordLockTimeout(_, allocation)
            | Self::NativeRecordOwnershipConflict(_, allocation)
            | Self::NativeRecordContextTransaction(_, allocation)
            | Self::NativeRecordCorrupt(_, allocation) => Some(allocation),
            Self::NativeWork(error) => error.native_failure_allocation_mut(),
            Self::NativeWorkspaceCause(cause) => Some(&mut cause.custody),
            Self::Filesystem(error)
            | Self::WorkspaceCause(WorkspaceError::Filesystem(error)) => {
                Self::filesystem_native_failure_allocation_mut(error)
            }
            Self::Demand(error) => Self::demand_native_failure_allocation_mut(error),
            Self::SpentWork(failure) => failure.error.native_failure_allocation_mut(),
            _ => None,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn filesystem_native_failure_allocation_mut(
        error: &mut FsError,
    ) -> Option<&mut Option<crate::materializer::NativeAllocation>> {
        match error {
            FsError::NativeKernel(error) => error.native_failure_allocation_mut(),
            FsError::BlobBuild(crate::kernel::BlobBuildError::Context(error)) => {
                Self::filesystem_native_failure_allocation_mut(error)
            }
            FsError::BlobBuild(crate::kernel::BlobBuildError::Demand(error)) => {
                Self::demand_native_failure_allocation_mut(error.error_mut())
            }
            _ => None,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn demand_native_failure_allocation_mut(
        error: &mut DemandError,
    ) -> Option<&mut Option<crate::materializer::NativeAllocation>> {
        match error {
            #[cfg(feature = "native-mount")]
            DemandError::Mount(error) => match error {
                crate::native_mount::MountSourceError::Filesystem(error)
                | crate::native_mount::MountSourceError::WorkspaceCause(
                    WorkspaceError::Filesystem(error),
                ) => Self::filesystem_native_failure_allocation_mut(error),
                error => error.native_allocation_mut(),
            },
            _ => None,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn take_native_failure_allocation(&mut self) -> Option<crate::materializer::NativeAllocation> {
        self.native_failure_allocation_mut().and_then(Option::take)
    }

    /// Borrows the original semantic failure without discarding its work receipt.
    #[must_use]
    pub fn semantic_error(&self) -> &Self {
        match self {
            Self::SpentWork(failure) => failure.error.semantic_error(),
            Self::SourcePageCleanup { primary, .. } if primary.prefix_error.is_none() =>
                primary.primary().semantic_error(),
            error => error,
        }
    }

    /// Exact work retained by a failed demand, publication, or companion IO.
    #[must_use]
    pub fn spent_work(&self) -> WorkCounters {
        match self {
            Self::SpentWork(failure) => *failure.work,
            Self::SourcePageCleanup { primary, .. } => {
                if primary.prefix_error.is_some() { primary.prefix_prior } else { primary.work }
            }
            _ => WorkCounters::default(),
        }
    }

    /// Moves the original failure and counters into a measured producer result.
    pub fn into_measured_failure(self) -> crate::OperationFailure<Self> {
        match self {
            Self::SpentWork(failure) => *failure,
            error @ Self::SourcePageCleanup { .. } => {
                let work = error.spent_work();
                crate::OperationFailure::new(error, work)
            }
            error => crate::OperationFailure::before_work(error),
        }
    }

    /// Adds prior work while preserving cleanup failures.
    pub fn with_prior_work(mut self, prior: WorkCounters) -> Self {
        if let Self::SourcePageCleanup { primary, .. } = &mut self {
            match prior.checked_add(primary.work) {
                Ok(work) => primary.work = work,
                Err(error) => {
                    primary.prefix_error = Some(error);
                    primary.prefix_prior = prior;
                }
            }
            return self;
        }
        let failure = self.into_measured_failure()
            .map_with_prior_work(prior, |error| error);
        if *failure.work == WorkCounters::default() {
            failure.error
        } else {
            Self::SpentWork(Box::new(failure))
        }
    }
}

impl LazySnapshotRef {

    fn exactification_state(
        self,
        workspace_id: WorkspaceId,
        parent_workspace_id: WorkspaceId,
        source: SourceReference,
    ) -> LazyWorkspaceState {
        LazyWorkspaceState { parent_workspace_id: Some(parent_workspace_id),
            ..LazyWorkspaceState::new(workspace_id, source, self.overlay, self.shadows) }
    }
}

macro_rules! read_lazy_content {
    ($lazy:expr, $path:expr, $range:expr, $budget:expr, $cancellation:expr) => {{
        let mut work = WorkCounters::default();
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let result = async {
            let lookup = accept_directory_work(
                $lazy.lookup_measured($path, $budget, $cancellation).await, &mut work, &allocations,
            )?;
            allocations.claim_bytes(lookup.lookup.logical_owned_bytes(), 0, &mut work, $budget)
                .map_err(lazy_allocation_error)?;
            let receipt = $lazy.read_resolved(&lookup.lookup, lookup.source, None, $path, $range, remaining_work(work, $budget, allocations.live_bytes())?, $cancellation).await.map_err(|failure| lazy_operation_failure(failure, std::convert::identity));
            let output = accept_directory_work(receipt, &mut work, &allocations)?;
            work.verify($budget)?;
            Ok(output.bytes)
        }.await;
        result.map(|value| OperationReceipt { value, work })
            .map_err(|error| crate::OperationFailure::new(error, work))

    }};
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
        let workspace = fs.create_workspace(name, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(workspace_error)?;
        Self::attach_workspace(workspace, source, store).await
    }

    /// Creates an authored workspace with exact filesystem semantics and binds a source without demanding any path.
    pub async fn attach_with_config(
        fs: &Fs<A, O>,
        name: impl AsRef<str>,
        source: Arc<D>,
        store: S,
        config: VolumeConfig,
    ) -> Result<Self, LazyWorkspaceError> {
        let workspace = fs
            .create_workspace_with_config(name, config, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
            .map_err(workspace_error)?;
        Self::attach_workspace(workspace, source, store).await
    }

    /// Creates or recovers one original lazy attachment without tree demand.
    #[allow(clippy::too_many_arguments)]
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
            .create_workspace_with_operation(name, config, operation_id, budget, cancellation)
            .await
            .map_err(|failure| failure.map_with_prior_work(WorkCounters::default(), workspace_error))?;
        let meter = LazyTreapWork::new_with_prior(budget, cancellation, created.work);
        let result = Self::attach_created_with_accounting(
            created.value, source, store, reference, &meter, cancellation,
        ).await;
        meter.measured(result, |error, work| crate::OperationFailure::new(error, work))
    }

    /// Creates the original operation and binds its companion on the same native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::too_many_arguments)]
    pub async fn attach_with_operation_with_native_work(
        fs: &Fs<A, O>, name: &str, source: Arc<D>, store: S,
        config: VolumeConfig, operation_id: crate::OperationId,
        original: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(Self, crate::Generation<A, O>),
        (LazyWorkspaceError, Option<crate::materializer::NativeAllocation>)> {
        original.admit(WorkCounters::UNCHARGED)
            .map_err(|error| (LazyWorkspaceError::NativeWork(error), None))?;
        let reference = source.reference();
        let generation = fs.create_workspace_with_operation_with_native_work(
            name, config, operation_id, original, cancellation,
        ).await.map_err(|(error, allocation)| (LazyWorkspaceError::WorkspaceCause(error), allocation))?;
        let meter = LazyTreapWork::native_with_work(original, cancellation)
            .map_err(LazyWorkspaceError::into_native_failure)?;
        Self::attach_created_with_accounting(
            generation, source, store, reference, &meter, cancellation,
        ).await.map_err(LazyWorkspaceError::into_native_failure)
    }

    async fn attach_created_with_accounting(
        generation: crate::Generation<A, O>, source: Arc<D>, store: S,
        reference: SourceReference, meter: &LazyTreapWork<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Self, crate::Generation<A, O>), LazyWorkspaceError> {
        if matches!(meter.meter, LazyTreapMeter::Standalone { .. }) {
            meter.adopt_input(generation.workspace().name.retained_bytes()
                .checked_add(generation.publication_retained_bytes())
                .ok_or(crate::WorkError::Overflow)?)?;
        }
        let workspace = generation.workspace().clone();
        let existing = lazy_dispatch!{meter, meter.accept(store.load_lazy_workspace_measured(
                workspace.id(), meter.remaining()?, cancellation,
            ).await)?.map(LazyReadonlyNode::Portable),
            |original| {
                store.load_lazy_workspace_with_native_work(
                    workspace.id(), original,
                ).await?.map(LazyReadonlyNode::Native)
            }
        };
        if let Some(state) = existing {
            let state = state.as_state();
            state.require_ready_fork()?;
            if state.schema_version != LAZY_STATE_SCHEMA
                || state.workspace_id != workspace.id()
                || state.source.identity != reference.identity
            {
                return Err(LazyWorkspaceError::StaleSource);
            }
        } else {
            let (overlay, shadows) = Self::initialize_empty_roots_with_accounting(&store, meter, cancellation).await?;
            let state = LazyWorkspaceState::new(workspace.id(), reference, overlay, shadows);
            let swapped = lazy_dispatch!{meter, meter.accept(store.compare_and_swap_lazy_workspace_measured(
                    workspace.id(), 0, state, meter.remaining()?, cancellation,
                ).await)?,
                |original| {
                    store.compare_and_swap_lazy_workspace_with_native_work(
                        workspace.id(), 0, state, original,
                    ).await?
                }
            };
            if !swapped {
                return Err(LazyWorkspaceError::Concurrent);
            }
        }
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        if source.reference() != reference {
            return Err(LazyWorkspaceError::StaleSource);
        }
        let selected_publication = new_selected_import_control(meter)?;
        Ok((Self { workspace, source, store, selected_publication,
            #[cfg(not(target_arch = "wasm32"))]
            allocation: None,
        }, generation))
    }

    async fn initialize_empty_roots_with_accounting(
        store: &S, meter: &LazyTreapWork<'_>, cancellation: &CancellationToken,
    ) -> Result<(LazyOverlayId, LazyShadowId), LazyWorkspaceError> {
        meter.charge(WorkCounters::UNCHARGED)?;
        let overlay = LazyOverlay::default();
        let overlay_id = LazyOverlayId(hash_lazy_value(&overlay, Some(meter))?);
        let shadow_id;
        lazy_dispatch!{meter, {
                meter.accept(store.put_lazy_overlay_measured(
                    overlay_id, overlay, meter.remaining()?, cancellation,
                ).await)?;
                let shadows = LazyShadow::default();
                shadow_id = LazyShadowId(hash_lazy_value(&shadows, Some(meter))?);
                meter.accept(store.put_lazy_shadow_measured(
                    shadow_id, shadows, meter.remaining()?, cancellation,
                ).await)?;
            },
            |original| {
                let (overlay, allocation) = original.allocations(
                    |_, _, _, _| Ok::<_, LazyWorkspaceError>(overlay),
                )?;
                store.put_lazy_overlay_with_native_work(
                    overlay_id, NativeOwnedCoreState::from_parts(overlay, allocation), original,
                ).await?;
                let (shadows, allocation) = original.allocations(
                    |_, _, _, _| Ok::<_, LazyWorkspaceError>(LazyShadow::default()),
                )?;
                shadow_id = LazyShadowId(hash_lazy_value(&shadows, Some(meter))?);
                store.put_lazy_shadow_with_native_work(
                    shadow_id, NativeOwnedCoreState::from_parts(shadows, allocation), original,
                ).await?;
            }
        }
        Ok((overlay_id, shadow_id))
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
        let state = LazyWorkspaceState::new(workspace.id(), source.reference(), overlay_id, shadow_id);
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
        let cancellation = CancellationToken::new();
        let meter = LazyTreapWork::new(WorkBudget::UNBOUNDED, &cancellation);
        meter.adopt_input(workspace.name.retained_bytes())?;
        let selected_publication = new_selected_import_control(&meter)?;
        let lazy = Self {
            workspace,
            source,
            store,
            selected_publication,
            #[cfg(not(target_arch = "wasm32"))]
            allocation: None,
        };
        lazy.recover_pending_remove(&meter).await.map_err(|error| error.with_prior_work(meter.spent()).into_measured_failure().error)?;
        Ok(lazy)
    }

    /// Reopens `workspace`'s binding, or binds it to `source` afresh when no binding is durable.
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
        let cancellation = CancellationToken::new();
        let meter = LazyTreapWork::new(WorkBudget::UNBOUNDED, &cancellation);
        meter.adopt_input(workspace.name.retained_bytes())?;
        let selected_publication = new_selected_import_control(&meter)?;
        let lazy = Self {
            workspace,
            source,
            store,
            selected_publication,
            #[cfg(not(target_arch = "wasm32"))]
            allocation: None,
        };
        lazy.recover_pending_remove(&meter).await.map_err(|error| error.with_prior_work(meter.spent()).into_measured_failure().error)?;
        lazy.rebind_source(&meter).await?;
        Ok(lazy)
    }

    /// Authored SDK workspace used by merge, lineage, and publication primitives.
    #[must_use]
    pub const fn workspace(&self) -> &Workspace<A, O> {
        &self.workspace
    }

    /// Reopens another workspace bound to the same source capability.
    pub async fn open_related(&self, workspace: Workspace<A, O>) -> Result<Self, LazyWorkspaceError>
    where
        S: Clone,
    {
        Self::open(workspace, Arc::clone(&self.source), self.store.clone()).await
    }

    /// Captures the logical lazy-tree identity on the caller's accounting context.
    pub async fn snapshot(&self, work: &LazyTreapWork<'_>) -> Result<LazySnapshotRef, LazyWorkspaceError> {
        let cancellation = work.cancellation;
        match &work.meter {
            #[cfg(not(target_arch = "wasm32"))]
            LazyTreapMeter::NativeOriginal { original, .. } => {
                let owned = self.state(work).await?;
                self.snapshot_from_state(owned.as_state(), original).await
            }
            LazyTreapMeter::Standalone { .. } => {
            let phase = self.state(work).await?; let state = phase.as_state();
            let head = work.accept(self.workspace.head(work.remaining()?, cancellation)
                .await.map_err(measured_workspace_error))?;
            cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
            if self.source.reference() != state.source {
                return Err(LazyWorkspaceError::StaleSource);
            }
            work.charge(WorkCounters {
                bytes_hashed: crate::foundation::usize_to_u64(LAZY_SNAPSHOT_DOMAIN.len()) + 136,
                ..WorkCounters::default()
            })?;
            let snapshot = LazySnapshotRef::new(
                self.workspace.id(), head.id(), state.source, state.overlay, state.shadows,
            );
            cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
            Ok(snapshot)
            }
        }
    }

    /// Captures genuine current SDK snapshot data without rebinding a source or mutating its companion.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn publication_snapshot_with_native_work(
        &self, original: &crate::materializer::NativeMaterializationWork,
    ) -> Result<NativePublicationSnapshot, LazyWorkspaceError> {
        let cancellation = original.cancellation();
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        original.native_read_meter().with_original_leaf(|work|
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }))?;
        let owned = self.store.load_lazy_workspace_with_native_work(self.workspace.id(), original)
            .await?.ok_or(LazyWorkspaceError::StaleSource)?;
        let state = owned.as_state();
        state.require_ready_fork()?;
        if state.schema_version != LAZY_STATE_SCHEMA || state.workspace_id != self.workspace.id()
            || state.source != self.source.reference()
        {
            return Err(LazyWorkspaceError::StaleSource);
        }
        if state.pending_remove.is_some() || state.pending_write.is_some() {
            return Err(LazyWorkspaceError::Concurrent);
        }
        let snapshot = self.snapshot_from_state(state, original).await?;
        drop(owned);
        original.admit(WorkCounters::UNCHARGED)?;
        if self.source.reference() != snapshot.source { return Err(LazyWorkspaceError::StaleSource); }
        let copied = WorkCounters {
            bytes_copied: std::mem::size_of::<NativePublicationSnapshot>() as u64,
            ..WorkCounters::UNCHARGED
        };
        original.admit(copied)?;
        let qualification = NativePublicationSnapshot::unchanged(
            NativeSnapshotContext::Lazy(LazyPublicationCut {
                workspace_id: snapshot.workspace_id, config: self.workspace.volume_config(),
                source: snapshot.source, overlay: snapshot.overlay, shadows: snapshot.shadows,
            }),
            snapshot.authored_generation,
        );
        original.spent(copied)?;
        original.admit(WorkCounters::UNCHARGED)?;
        if self.source.reference() != snapshot.source { return Err(LazyWorkspaceError::StaleSource); }
        Ok(qualification)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn snapshot_from_state(
        &self, state: &LazyWorkspaceState, original: &crate::materializer::NativeMaterializationWork,
    ) -> Result<LazySnapshotRef, LazyWorkspaceError> {
        let cancellation = original.cancellation();
        let head = self.workspace.head(
            original, cancellation,
        ).await.map_err(|error| native_workspace_error_pair((error, None)))?;
        original.native_read_meter().with_original_leaf(|work| work.check())?;
        if self.source.reference() != state.source { return Err(LazyWorkspaceError::StaleSource); }
        original.native_read_meter().with_original_leaf(|work| work.charge(WorkCounters {
            bytes_hashed: (LAZY_SNAPSHOT_DOMAIN.len() + self.workspace.id().into_bytes().len()
                + head.id().digest().as_bytes().len() + state.source.identity.len()
                + state.source.epoch.to_le_bytes().len() + state.overlay.into_bytes().len()
                + state.shadows.into_bytes().len()) as u64,
            bytes_copied: std::mem::size_of::<LazySnapshotRef>() as u64,
            ..WorkCounters::UNCHARGED
        }))?;
        let snapshot = LazySnapshotRef::new(
            self.workspace.id(), head.id(), state.source, state.overlay, state.shadows,
        );
        original.native_read_meter().with_original_leaf(|work| work.check())?;
        if self.source.reference() != state.source { return Err(LazyWorkspaceError::StaleSource); }
        drop(head);
        Ok(snapshot)
    }

    /// Loads the constant-size durable source/lineage binding.
    pub async fn binding(&self) -> Result<LazyWorkspaceState, LazyWorkspaceError> {
        self.inspect_state(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("ordinary inspection uses ordinary funding") })
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
            let (name, id) = meter.accept(self.workspace.volume.fs.workspace_name_identity(
                destination, meter.remaining()?, cancellation,
            ).map_err(measured_workspace_error))?;
            let name_bytes = name.retained_bytes();
            meter.adopt(name_bytes)?;
            let origin = self.prepare_fork_origin_at(id, idempotency_key.operation_id(), None, &meter).await?;
            let generation = meter.accept(self.workspace.generation(
                origin.snapshot.authored_generation, meter.remaining()?, cancellation,
            ).await.map_err(measured_workspace_error))?;
            let generation_bytes = generation.publication_retained_bytes();
            meter.adopt_input(generation_bytes)?;
            // The owned destination transfers into the filesystem producer's ledger.
            meter.release(name_bytes)?;
            let workspace = meter.accept(self.workspace.volume.fs.fork_workspace::<_, fn(WorkBudget) -> std::future::Ready<crate::FsResult<()>>, _>(
                name, &generation, idempotency_key, None, crate::PublicationPermit::Unrestricted, meter.remaining()?, cancellation, None,
            ).await.map_err(measured_workspace_error))?;
            drop(generation);
            meter.release(generation_bytes)?;
            meter.adopt(workspace.name.retained_bytes())?;
            let original = meter.accept(self.workspace.volume.fs.workspace_for_creation_operation(
                workspace.name.as_str(), workspace.volume.config, origin.operation_id,
                meter.remaining()?, cancellation,
            ).await.map_err(measured_workspace_error))?.ok_or(LazyWorkspaceError::Concurrent)?;
            let original_bytes = original.publication_retained_bytes();
            meter.adopt_input(original_bytes)?;
            let child_snapshot = self.commit_prepared_fork_at(
                &workspace, original.id(), origin, &meter,
            ).await?;
            drop(original);
            meter.release(original_bytes)?;
            let selected_publication = new_selected_import_control(&meter)?;
            Ok((Self { workspace, source: Arc::clone(&self.source), store: self.store.clone(), selected_publication,
                #[cfg(not(target_arch = "wasm32"))]
                allocation: None,
            }, child_snapshot))
        }.await;
        meter.measured(result, |error, work| crate::OperationFailure::new(error, work))
    }

    /// Retains a qualified source freeze before its filesystem project is created.
    pub async fn prepare_fork_snapshot(
        &self, destination: impl AsRef<str>, frozen: LazySnapshotRef,
        original_operation_id: crate::OperationId, meter: &LazyTreapWork<'_>,
    ) -> Result<LazySnapshotRef, LazyWorkspaceError> {
        meter.cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        let (name, id) = lazy_dispatch!{meter, {
                let named = meter.accept(self.workspace.volume.fs.workspace_name_identity(
                    destination, meter.remaining()?, meter.cancellation,
                ).map_err(measured_workspace_error))?;
                meter.adopt(named.0.retained_bytes())?;
                named
            },
            |original| self.workspace.volume.fs.workspace_name_identity_with_native_work(
                destination, original, meter.cancellation,
            ).map_err(native_workspace_error_pair)?
        };
        let origin = self.prepare_fork_origin_at(id, original_operation_id, Some(frozen), meter).await?;
        drop(name);
        Ok(origin.snapshot)
    }

    /// Binds an already-created project after authenticating its immutable creation.
    pub async fn bind_prepared_fork(
        &self, frozen: LazySnapshotRef, created_child: Workspace<A, O>,
        original_child_generation: crate::GenerationId, original_operation_id: crate::OperationId,
        meter: &LazyTreapWork<'_>,
    ) -> Result<(Self, LazySnapshotRef), LazyWorkspaceError> {
        if matches!(meter.meter, LazyTreapMeter::Standalone { .. }) {
            meter.adopt_input(created_child.name.retained_bytes())?;
        }
        meter.cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        if !self.workspace.volume.fs.same_deployment(&created_child.volume.fs) {
            return Err(match &meter.meter {
                LazyTreapMeter::Standalone { .. } => workspace_error(WorkspaceError::ForeignGeneration),
                #[cfg(not(target_arch = "wasm32"))]
                LazyTreapMeter::NativeOriginal { .. } => native_workspace_error(WorkspaceError::ForeignGeneration),
            });
        }
        let state = self.load_binding_at(created_child.id(), meter, meter.cancellation).await?
            .ok_or(LazyWorkspaceError::Concurrent)?;
        let origin = self.validate_fork_origin(
            state.as_state(), created_child.id(), original_operation_id, Some(frozen), meter,
        )?;
        let observed = lazy_dispatch!{meter, meter.accept(self.workspace.volume.fs.workspace_for_creation_operation(
                created_child.name.as_str(), created_child.volume.config, original_operation_id,
                meter.remaining()?, meter.cancellation,
            ).await.map_err(measured_workspace_error))?,
            |original| self.workspace.volume.fs.workspace_for_creation_operation_with_native_work(
                created_child.name.as_str(), created_child.volume.config, original_operation_id,
                original, meter.cancellation,
            ).await.map_err(native_workspace_error_pair)?
        }.ok_or(LazyWorkspaceError::Concurrent)?;
        let observed_bytes = observed.publication_retained_bytes();
        if matches!(meter.meter, LazyTreapMeter::Standalone { .. }) {
            meter.adopt_input(observed_bytes)?;
        }
        if observed.id() != original_child_generation || observed.workspace_id() != created_child.id() {
            return Err(LazyWorkspaceError::Concurrent);
        }
        drop(observed);
        if matches!(meter.meter, LazyTreapMeter::Standalone { .. }) {
            meter.release(observed_bytes)?;
        }
        let child_snapshot = self.commit_prepared_fork_at(
            &created_child, original_child_generation, origin, meter,
        ).await?;
        let selected_publication = new_selected_import_control(meter)?;
        Ok((Self { workspace: created_child, source: Arc::clone(&self.source), store: self.store.clone(), selected_publication,
            #[cfg(not(target_arch = "wasm32"))]
            allocation: None,
        }, child_snapshot))
    }

    async fn load_binding_at(
        &self, destination: WorkspaceId, meter: &LazyTreapWork<'_>, cancellation: &CancellationToken,
    ) -> Result<Option<LazyReadonlyNode<LazyWorkspaceState>>, LazyWorkspaceError> {
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        lazy_dispatch!{meter, meter.accept(self.store.load_lazy_workspace_measured(
                destination, meter.remaining()?, cancellation,
            ).await).map(|state| state.map(LazyReadonlyNode::Portable)),
            |original| self.store.load_lazy_workspace_with_native_work(destination, original).await
            .map(|state| state.map(LazyReadonlyNode::Native))
        }
    }

    async fn swap_binding_at(
        &self, destination: WorkspaceId, revision: u64, state: LazyWorkspaceState,
        meter: &LazyTreapWork<'_>, cancellation: &CancellationToken,
    ) -> Result<bool, LazyWorkspaceError> {
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        lazy_dispatch!{meter, meter.accept(self.store.compare_and_swap_lazy_workspace_measured(
                destination, revision, state, meter.remaining()?, cancellation,
            ).await),
            |original| self.store.compare_and_swap_lazy_workspace_with_native_work(
                destination, revision, state, original,
            ).await
        }
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
            let loaded = self.load_binding_at(destination, meter, meter.cancellation).await?;
            if let Some(state) = loaded {
                return self.validate_fork_origin(state.as_state(), destination, operation_id, qualified, meter);
            }
            let snapshot = if let Some(snapshot) = qualified { snapshot } else {
                self.snapshot(meter).await?
            };
            self.validate_snapshot(snapshot, meter).await?;
            let origin = LazyForkOrigin {
                operation_id, snapshot, config: self.workspace.volume.config, phase: LazyForkPhase::Prepared,
            };
            let state = LazyWorkspaceState { parent_workspace_id: Some(snapshot.workspace_id), fork_origin: Some(origin),
                ..LazyWorkspaceState::new(destination, snapshot.source, snapshot.overlay, snapshot.shadows) };
            let hold = lazy_dispatch!{meter, meter.accept(self.workspace.volume.fs.make_generation_durable(
                    &self.workspace.volume, snapshot.authored_generation, meter.remaining()?, meter.cancellation,
                ).await.map_err(measured_workspace_error))?,
                |original| self.workspace.volume.fs.make_generation_durable_with_native_work(
                    &self.workspace.volume, snapshot.authored_generation, original, meter.cancellation,
                ).await.map_err(native_workspace_error_pair)?
            };
            let swapped = self.swap_binding_at(destination, 0, state, meter, meter.cancellation).await?;
            drop(hold);
            if swapped { return Ok(origin); }
            if qualified.is_some() {
                let state = self.load_binding_at(destination, meter, meter.cancellation).await?
                    .ok_or(LazyWorkspaceError::Concurrent)?;
                return self.validate_fork_origin(state.as_state(), destination, operation_id, qualified, meter);
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
            // Native decoded data must retire before its actual allocation guard.
            #[cfg(not(target_arch = "wasm32"))]
            let mut state_allocation = None;
            let state = match self.load_binding_at(workspace.id(), meter, meter.cancellation).await?
                .ok_or(LazyWorkspaceError::Concurrent)? {
                LazyReadonlyNode::Portable(state) => state,
                #[cfg(not(target_arch = "wasm32"))]
                LazyReadonlyNode::Native(owned) => {
                    let (state, allocation) = owned.into_parts();
                    state_allocation = Some(allocation);
                    state
                }
            };
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
            let swapped = self.swap_binding_at(workspace.id(), revision - 1, committed, meter, meter.cancellation).await?;
            #[cfg(not(target_arch = "wasm32"))]
            drop(state_allocation);
            if swapped {
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
        let exactification_source = self.source.reference();
        if snapshot.source.identity != exactification_source.identity { return Err(LazyWorkspaceError::StaleSource); }
        let overlay = self
            .store
            .load_lazy_overlay_measured(snapshot.overlay, budget, cancellation)
            .await?;
        let mut work = overlay.work;
        let shadow = self
            .store
            .load_lazy_shadow_measured(
                snapshot.shadows,
                remaining_work(work, budget, 0)?,
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
            .generation(
                snapshot.authored_generation,
                remaining_work(work, budget, 0)?,
                cancellation,
            )
            .await.map_err(measured_workspace_error)?;
        work = account_work(work, generation.work, budget)?;
        let workspace = self
            .workspace
            .fork(
                destination,
                ForkOptions::from_generation(generation.value, idempotency_key),
                crate::PublicationPermit::Unrestricted, remaining_work(work, budget, 0)?,
                cancellation, |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })),
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
                remaining_work(work, budget, 0)?,
                cancellation,
            )
            .await?;
        work = account_work(work, inserted.work, budget)?;
        if !inserted.value {
            let existing = self
                .store
                .load_lazy_workspace_measured(
                    workspace.id(),
                    remaining_work(work, budget, 0)?,
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
        let control_meter = LazyTreapWork::new(remaining_work(work, budget, 0)?, cancellation);
        let selected_publication = new_selected_import_control(&control_meter)?;
        work = account_work(work, control_meter.spent(), budget)?;
        let control_bytes = control_meter.live_bytes();
        let mut remaining = remaining_work(work, budget, 0)?;
        remaining.peak_allocation_bytes = remaining.peak_allocation_bytes
            .checked_sub(control_bytes).ok_or_else(counter_overflow)?;
        let exactified = Self {
            workspace,
            source: Arc::clone(&self.source),
            store: self.store.clone(),
            selected_publication,
            #[cfg(not(target_arch = "wasm32"))]
            allocation: None,
        }
        .exactify("/", remaining, cancellation, crate::PublicationPermit::Unrestricted)
        .await?;
        control_meter.release(control_bytes)?;
        work = work.with_backend(exactified.work, control_bytes)?;
        work.verify(budget)?;
        Ok(OperationReceipt {
            value: exactified.value,
            work,
        })
    }

    /// Captures only selected frozen source regions, then projects selected imported records/removals through the same guarded SDK transactions.
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
            let source_used = std::sync::atomic::AtomicBool::new(false);
            let result = async {
                work.with_kernel(|accounting, _| self.readonly_snapshot_state(frozen, false, accounting))?;
                if !self.workspace.volume.fs.same_deployment(&imported.workspace.volume.fs) {
                    return Err(workspace_error(WorkspaceError::ForeignGeneration));
                }
                for path in paths {
                    cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
                    PortablePath::validate(path, self.workspace.limits())
                        .map_err(|error| LazyWorkspaceError::Workspace(error.to_string()))?;
                }
                let source_is_current = || {
                    let current = self.source.reference();
                    current.identity == frozen.source.identity
                        && (!source_used.load(std::sync::atomic::Ordering::Acquire)
                            || current.epoch == frozen.source.epoch)
                };
                let mut seal_guard = |allowance| {
                    let owner = before_publication(allowance);
                    let source_is_current = &source_is_current;
                    async move {
                        cancellation.check().map_err(|error| crate::OperationFailure::before_work(crate::FsError::from(error)))?;
                        if !source_is_current() {
                            return Err(crate::OperationFailure::before_work(crate::FsError::SourceSnapshotStale));
                        }
                        let receipt = owner.await?;
                        cancellation.check().map_err(|error| crate::OperationFailure::new(crate::FsError::from(error), receipt.work))?;
                        if !source_is_current() {
                            return Err(crate::OperationFailure::new(crate::FsError::SourceSnapshotStale, receipt.work));
                        }
                        Ok(receipt)
                    }
                };
                let generation = work.accept(self.workspace.generation(
                    frozen.authored_generation, work.remaining()?, cancellation,
                ).await.map_err(measured_workspace_error))?;
                let workspace = work.accept(self.workspace.fork(
                    destination, ForkOptions::from_generation(generation, baseline_key),
                    permit, work.remaining()?, cancellation, &mut seal_guard,
                ).await.map_err(measured_workspace_error))?;
                let name_bytes = workspace.name.retained_bytes();
                work.adopt(name_bytes)?;
                for (index, path) in paths.iter().enumerate() {
                    let covered = work.with_kernel(|accounting, cancellation|
                        exactify_root_is_covered(paths, index, accounting, cancellation))?;
                    if covered { continue; }
                    work.accept(self.exactify_selected_guarded(
                        &workspace, path, Some((self, frozen)), work.remaining()?,
                        cancellation, permit, &mut seal_guard, Some(&source_used),
                    ).await)?;
                }
                let baseline = work.accept(workspace.head(work.remaining()?, cancellation)
                    .await.map_err(measured_workspace_error))?;
                let accounting = LazyTreapWork::new(work.remaining()?, cancellation);
                let result = self.selected_paths(paths, None, Some(&baseline), &accounting).await;
                let mut removed = work.accept(accounting.measured(result, |error, spent| error.with_prior_work(spent)).map(|receipt| OperationReceipt { value: receipt.value.0, work: receipt.work }))?;
                work.adopt(exactify_paths_retained_bytes(&removed)?)?;
                let accounting = LazyTreapWork::new(work.remaining()?, cancellation);
                let result = self.selected_paths(paths, None, Some(imported), &accounting).await;
                let present = work.accept(accounting.measured(result, |error, spent| error.with_prior_work(spent)).map(|receipt| OperationReceipt { value: receipt.value.0, work: receipt.work }))?;
                work.adopt(exactify_paths_retained_bytes(&present)?)?;
                filter_selected_removals(&mut removed, &present, &work, cancellation, ||
                    cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled))?;
                let maximum = exactify_batch_paths(self.workspace.limits());
                let mut candidate = baseline.clone();
                let mut publication_bytes = 0_u64;
                let mut batch_index = 0_u64;
                for records in [&removed, &present] {
                    for batch in records.chunks(maximum) {
                        if batch_index != 0 {
                            work.charge(WorkCounters {
                                bytes_hashed: (SELECTED_RESTORE_BATCH_DOMAIN.len() + 16 + 8) as u64,
                                ..WorkCounters::default()
                            })?;
                        }
                        let key = selected_restore_batch_key(candidate_key, batch_index);
                        let restored = work.accept(workspace.restore_selected_paths_from(
                            imported, batch, candidate.id(), key, permit,
                            work.remaining()?, cancellation, &mut seal_guard,
                        ).await.map_err(measured_workspace_error))?;
                        let next = match restored {
                            TransactionCommit::Committed(generation)
                            | TransactionCommit::AlreadyCommitted(generation) => generation,
                            TransactionCommit::Conflict { .. } | TransactionCommit::Fenced =>
                                return Err(LazyWorkspaceError::StaleIdentity),
                            TransactionCommit::IdempotencyConflict => return Err(LazyWorkspaceError::Concurrent),
                        };
                        let shared = shares_exactify_publication(&candidate, &next);
                        let next_bytes = next.publication_retained_bytes();
                        if !shared { work.adopt(next_bytes)?; }
                        candidate = next;
                        if !shared { work.release(publication_bytes)?; }
                        publication_bytes = next_bytes;
                        batch_index = batch_index.checked_add(1).ok_or_else(counter_overflow)?;
                    }
                }
                let removed_bytes = exactify_paths_retained_bytes(&removed)?;
                let present_bytes = exactify_paths_retained_bytes(&present)?;
                drop(removed);
                drop(present);
                work.release(removed_bytes)?;
                work.release(present_bytes)?;
                work.release(publication_bytes)?;
                work.release(name_bytes)?;
                Ok((baseline, candidate))
            }.await;
            work.measured(result, |error: LazyWorkspaceError, spent| error.with_prior_work(spent).into_measured_failure())
        }, budget, cancellation, LazyWorkspaceError::Cancelled, None).await
    }

    /// Projects selected regions of the original immutable cut on the caller's native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn exactify_snapshot_selected_at_generation_with_native_work_guarded<E, F, Fut>(
        &self, frozen: LazySnapshotRef, imported: &crate::Generation<A, O>,
        paths: &[String], destination: &str, baseline_key: IdempotencyKey,
        candidate_key: IdempotencyKey, permit: crate::PublicationPermit,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
        before_publication: F,
    ) -> Result<(crate::Generation<A, O>, crate::Generation<A, O>), E>
    where
        D: 'static,
        E: From<LazyWorkspaceError> + From<crate::FsError> + From<WorkspaceError> + Send + 'static,
        F: for<'work> FnMut(&'work crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        let work = LazyTreapWork::native_with_work(original, cancellation)?;
        in_heap_lazy(&work, || async {
            self.validate_snapshot(frozen, &work).await?;
            if !self.workspace.volume.fs.same_deployment(&imported.workspace.volume.fs) {
                return Err(E::from(native_workspace_error(WorkspaceError::ForeignGeneration)));
            }
            let mut accounting = ExactifyCollectorWork::Native { work: &work, original };
            for path in paths {
                accounting.check(cancellation)?;
                PortablePath::validate(path, self.workspace.limits())
                    .map_err(|error| LazyWorkspaceError::NamespacePath(
                        crate::kernel::NamespacePathError::Path(error)))?;
            }
            let source_used = std::sync::atomic::AtomicBool::new(false);
            let source_is_current = || {
                let current = self.source.reference();
                current.identity == frozen.source.identity
                    && (!source_used.load(std::sync::atomic::Ordering::Acquire)
                        || current.epoch == frozen.source.epoch)
            };
            let guard_source = Arc::clone(&self.source);
            let guard_original = original.clone();
            let guard_cancellation = cancellation.clone();
            let mut owned_guard_builder = move |check_epoch: bool| {
                let mut owner_factory = before_publication.clone();
                let source = Arc::clone(&guard_source);
                let original = guard_original.clone();
                let cancellation = guard_cancellation.clone();
                move |_: &crate::NativeMaterializationWork| {
                    let owner = owner_factory(&original);
                    let source = Arc::clone(&source);
                    let original = original.clone();
                    let cancellation = cancellation.clone();
                    async move {
                        original.admit(WorkCounters::UNCHARGED)
                            .map_err(crate::FsError::from).map_err(E::from)?;
                        cancellation.check().map_err(crate::FsError::from).map_err(E::from)?;
                        let current = source.reference();
                        if current.identity != frozen.source.identity
                            || (check_epoch && current.epoch != frozen.source.epoch) {
                            return Err(E::from(crate::FsError::SourceSnapshotStale));
                        }
                        owner.await?;
                        original.admit(WorkCounters::UNCHARGED)
                            .map_err(crate::FsError::from).map_err(E::from)?;
                        cancellation.check().map_err(crate::FsError::from).map_err(E::from)?;
                        let current = source.reference();
                        if current.identity != frozen.source.identity
                            || (check_epoch && current.epoch != frozen.source.epoch) {
                            return Err(E::from(crate::FsError::SourceSnapshotStale));
                        }
                        Ok(())
                    }
                }
            };
            let mut fork_guard = owned_guard_builder(
                source_used.load(std::sync::atomic::Ordering::Acquire),
            );
            let generation = self.workspace.generation(
                frozen.authored_generation, original, cancellation,
            ).await.map_err(|error| native_workspace_error_pair((error, None)))?;
            let workspace = self.workspace.fork_with_permit_guarded_with_native_work(
                destination, ForkOptions::from_generation(generation, baseline_key),
                permit, original, cancellation, &mut fork_guard,
            ).await?;
            let mut baseline = workspace.operation_generation(
                baseline_key, original, cancellation,
            ).await.map_err(|error| native_workspace_error_pair((error, None)))?
                .ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?;
            let current = workspace.head(original, cancellation)
                .await.map_err(|error| native_workspace_error_pair((error, None)))?;
            if current.id() != baseline.id() { return Err(E::from(LazyWorkspaceError::Concurrent)); }
            drop(current);
            for (index, path) in paths.iter().enumerate() {
                if accounting.with_kernel(cancellation, |accounting|
                    exactify_root_is_covered(paths, index, accounting, cancellation))? {
                    continue;
                }
                let mut publication = NativeOwnedBuilder { builder: &mut owned_guard_builder, original };
                baseline = self.exactify_selected_with_accounting(
                    &workspace, path, Some((self, frozen)), cancellation, permit,
                    &mut publication, Some(&source_used), None, &mut accounting,
                ).await?;
            }
            let (mut removed, _) = self.collect_selected_paths_with_accounting(
                paths, None, Some(&baseline), &mut accounting, cancellation,
            ).await?;
            let (present, _) = self.collect_selected_paths_with_accounting(
                paths, None, Some(imported), &mut accounting, cancellation,
            ).await?;
            filter_selected_removals(&mut removed, &present, &work, cancellation, ||
                accounting.check(cancellation))?;
            let mut candidate = baseline.clone();
            let mut batch_index = 0_u64;
            for records in [&removed, &present] {
                for batch in records.chunks(exactify_batch_paths(self.workspace.limits())) {
                    if batch_index != 0 {
                        work.charge(WorkCounters { bytes_hashed:
                            (SELECTED_RESTORE_BATCH_DOMAIN.len() + 16 + 8) as u64,
                            ..WorkCounters::UNCHARGED })?;
                    }
                    let key = selected_restore_batch_key(candidate_key, batch_index);
                    let mut restore_guard = owned_guard_builder(
                        source_used.load(std::sync::atomic::Ordering::Acquire),
                    );
                    candidate = match workspace.restore_selected_paths_from_with_permit_guarded_with_native_work(
                        imported, batch, candidate.id(), key, permit, original,
                        cancellation, &mut restore_guard,
                    ).await? {
                        TransactionCommit::Committed(next) | TransactionCommit::AlreadyCommitted(next) => next,
                        TransactionCommit::Conflict { .. } | TransactionCommit::Fenced =>
                            return Err(E::from(LazyWorkspaceError::StaleIdentity)),
                        TransactionCommit::IdempotencyConflict =>
                            return Err(E::from(LazyWorkspaceError::Concurrent)),
                    };
                    batch_index = batch_index.checked_add(1).ok_or_else(counter_overflow)?;
                }
            }
            let removed_bytes = exactify_paths_retained_bytes(&removed)?;
            let present_bytes = exactify_paths_retained_bytes(&present)?;
            drop(removed);
            drop(present);
            accounting.release(removed_bytes)?;
            accounting.release(present_bytes)?;
            accounting.check(cancellation)?;
            if !source_is_current() { return Err(E::from(LazyWorkspaceError::StaleSource)); }
            Ok((baseline, candidate))
        }).await
    }

    /// Advances the source epoch through the companion CAS without enumeration.
    pub async fn rebind_source(
        &self, work: &LazyTreapWork<'_>,
    ) -> Result<LazyReadonlyNode<LazyWorkspaceState>, LazyWorkspaceError> {
        let cancellation = work.cancellation;
        let source = self.source.reference();
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            let mut phase = self.load_binding_at(self.workspace.id(), work, cancellation).await?
                .ok_or(LazyWorkspaceError::StaleSource)?;
            let state = phase.as_state();
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
                return Ok(phase);
            }
            let replacement = LazyWorkspaceState {
                revision: state.revision.saturating_add(1),
                source,
                ..state.clone()
            };
            if self.swap_binding_at(self.workspace.id(), state.revision, replacement, work, cancellation).await? {
                let state = phase.as_state_mut();
                state.revision = state.revision.saturating_add(1);
                state.source = source;
                return Ok(phase);
            }
        }
        Err(LazyWorkspaceError::Concurrent)
    }

    /// Observes one path without reading file content.
    pub async fn stat(&self, path: &str) -> Result<LazyStat, LazyWorkspaceError> {
        let cancellation = CancellationToken::new();
        self.lookup_measured(path, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map(|receipt| receipt.value.lookup.stat())
    }

    /// Returns complete demanded facts without reading regular-file content.
    pub async fn lookup(&self, path: &str) -> Result<LazyLookup, LazyWorkspaceError> {
        let cancellation = CancellationToken::new();
        self.lookup_measured(path, WorkBudget::UNBOUNDED, &cancellation)
            .await.map(|receipt| receipt.value.lookup)
            .map_err(LazyWorkspaceError::into_measured_failure)
            .map_err(|failure| lazy_operation_failure(failure, std::convert::identity))
    }

    async fn lookup_measured(
        &self,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<ResolvedLazyLookup>, LazyWorkspaceError> {
        in_heap_measured(move |budget| async move {
            let mut accounting = LazyReadonlyAccounting::Portable {
                work: WorkCounters::UNCHARGED, budget, cancellation,
            };
            let mut backend = LazyInspectBackend::Measured(std::marker::PhantomData);
            let result = self.lookup_with_accounting(path, &mut accounting, &mut backend).await;
            accounting.finish_measured(result).map_err(LazyWorkspaceError::into_measured_failure)
        }, budget, cancellation, LazyWorkspaceError::Cancelled, None)
        .await.map_err(|failure| lazy_operation_failure(failure, std::convert::identity))
    }

    /// Resolves and publishes observations on the caller's original native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn lookup_with_native_work(
        &self, path: &str, original: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(LazyLookup, crate::materializer::NativeAllocation), LazyWorkspaceError> {
        self.lookup_resolved_with_native_work(path, original, cancellation).await
            .map(|(resolved, allocation)| (resolved.lookup, allocation))
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn lookup_resolved_with_native_work(
        &self, path: &str, original: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(ResolvedLazyLookup, crate::materializer::NativeAllocation), LazyWorkspaceError> {
        let (resolved, NativeInspectState { reader, allocation, .. }) =
            self.lookup_frozen(path, original, cancellation).await?;
        drop(reader);
        Ok((resolved, allocation))
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn lookup_frozen(
        &self, path: &str, original: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(ResolvedLazyLookup, NativeInspectState<A, O>), LazyWorkspaceError> {
        let frame = LazyTreapWork::native_with_work(original, cancellation)?;
        in_heap_lazy::<_, LazyWorkspaceError, _>(&frame, move || async move {
                let head = self.workspace.head(
                    original, cancellation,
                ).await.map_err(|error| native_workspace_error_pair((error, None)))?;
                let reader = self.workspace.readonly_pinned_reader_with_native_work(
                    head.id(), &original, cancellation,
                ).await.map_err(native_workspace_error)?;
                drop(head);
                let (_, allocation) = original.allocations(
                    |_, _, _, _| Ok::<(), LazyWorkspaceError>(()),
                )?;
                let mut context = NativeInspectState {
                    reader, record: None, metadata: None, allocation,
                };
                let mut accounting = LazyReadonlyAccounting::Native { original: &original, cancellation };
                let result = {
                    let mut backend = LazyInspectBackend::Native(&mut context);
                    self.lookup_with_accounting(path, &mut accounting, &mut backend).await
                }.and_then(|resolved| {
                    accounting.check()?;
                    if resolved.source.is_some_and(|source| self.source.reference() != source) {
                        return Err(LazyWorkspaceError::StaleSource);
                    }
                    Ok(resolved)
                });
                let NativeInspectState { reader, record, metadata, allocation } = context;
                match result {
                    Ok(resolved) => Ok((resolved, NativeInspectState { reader, record, metadata, allocation })),
                    Err(error) => {
                        drop(reader);
                        drop(allocation);
                        Err(error)
                    }
                }
        }).await.map_err(|error| match error {
            LazyWorkspaceError::Demand(DemandError::Work(error)) => LazyWorkspaceError::NativeWork(
                crate::NativeTreeMaterializationError::Work(error)),
            LazyWorkspaceError::Cancelled => LazyWorkspaceError::NativeWork(
                crate::NativeTreeMaterializationError::Cancelled),
            error => error,
        })
    }

    async fn lookup_with_accounting(
        &self, path: &str, accounting: &mut LazyReadonlyAccounting<'_>,
        backend: &mut LazyInspectBackend<'_, A, O>,
    ) -> Result<ResolvedLazyLookup, LazyWorkspaceError> {
        if let Some(lookup) = self.authored_lookup_with_accounting(
            path, InspectBasis::default(), accounting, backend,
        ).await? {
            return Ok(ResolvedLazyLookup { lookup, source: None });
        }
        let phase = match accounting {
            LazyReadonlyAccounting::Portable { work, budget, cancellation } => {
                let receipt = { let state_work = LazyTreapWork::new(remaining_work(*work, *budget, 0)?, cancellation); let result = self.state(&state_work).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("standalone state keeps its accounting owner") }); state_work.measured(result, |error, spent| error.with_prior_work(spent)) }?;
                *work = account_work(*work, receipt.work, *budget)?;
                LazyReadonlyNode::Portable(receipt.value)
            }
            #[cfg(not(target_arch = "wasm32"))]
            LazyReadonlyAccounting::Native { original, cancellation } =>
                self.state(&LazyTreapWork::native_with_work(original, cancellation)?).await?,
        };
        let state = phase.as_state();
        if self.tombstoned(state.overlay, path, accounting).await? {
            return Err(LazyWorkspaceError::NotFound);
        }
        let (source, node) = match accounting {
            LazyReadonlyAccounting::Portable { work, budget, cancellation } => {
                let receipt = (async { let observations = LazyTreapWork::new(remaining_work(*work, *budget, 0)?, cancellation); let result = self.observe(path, state, &observations).await; observations.measured(result, |error, spent| error.with_prior_work(spent)) }).await?;
                *work = account_work(*work, receipt.work, *budget)?;
                receipt.value
            }
            #[cfg(not(target_arch = "wasm32"))]
            LazyReadonlyAccounting::Native { original, cancellation } => {
                    let observations = LazyTreapWork::native_with_work(original, cancellation)?;
                    self.observe(path, state, &observations).await?
            }
        };
        let authored = match accounting {
            LazyReadonlyAccounting::Portable { work, budget, cancellation } => {
                let receipt = self.authored_alias_at_measured(
                    &node, None, None, remaining_work(*work, *budget, 0)?, cancellation,
                ).await?;
                *work = account_work(*work, receipt.work, *budget)?;
                receipt.value
            }
            #[cfg(not(target_arch = "wasm32"))]
            LazyReadonlyAccounting::Native { original, cancellation } => {
                    if node.kind == SourceNodeKind::Directory || node.link_count == Some(1) {
                        None
                    } else if let LazyInspectBackend::Native(context) = backend {
                        let head = self.workspace.head(
                            *original, cancellation,
                        ).await.map_err(|error| native_workspace_error_pair((error, None)))?;
                        let reader = self.workspace.readonly_pinned_reader_with_native_work(
                            head.id(), original, cancellation,
                        ).await.map_err(native_workspace_error)?;
                        drop(head);
                        let previous = std::mem::replace(&mut context.reader, reader);
                        drop(previous);
                        let current = self.state(&LazyTreapWork::native_with_work(original, cancellation)?).await?;
                        self.authored_alias_with_work(
                            &node, current.as_state(), context, original, cancellation,
                        ).await?
                    } else {
                        return Err(LazyWorkspaceError::OriginalSnapshotUnavailable);
                    }
            }
        };
        let aliased = authored.is_some();
        Ok(ResolvedLazyLookup {
            lookup: authored.unwrap_or(LazyLookup::Source(node)),
            source: if aliased { None } else { Some(source) },
        })
    }

    /// Returns current facts without extending the immutable observation index.
    pub async fn inspect(&self, path: &str) -> Result<LazyLookup, LazyWorkspaceError> {
        let cancellation = CancellationToken::new();
        self.inspect_with(path, InspectBasis::default(), WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map(|receipt| receipt.value.lookup)
    }

    /// Resolves `path` for a mount whose own checkout already lacks it.
    #[cfg(any(feature = "native-mount", test, not(target_arch = "wasm32")))]
    pub(crate) async fn inspect_unauthored(
        &self, path: &str, basis: InspectBasis<'_>,
        work: &LazyTreapWork<'_>,
    ) -> Result<LazyReadonlyNode<(LazyLookup, Option<SourceReference>)>, LazyWorkspaceError> {
        let basis = InspectBasis { authored_absent: true, ..basis };
        let cancellation = work.cancellation;
        in_heap_lazy::<_, LazyWorkspaceError, _>(work, || async {
        match &work.meter {
            LazyTreapMeter::Standalone { .. } => {
                let resolved = work.accept(self.inspect_with(path, basis, work.remaining()?, cancellation).await)?;
                Ok(LazyReadonlyNode::Portable((resolved.lookup, resolved.source)))
            }
            #[cfg(not(target_arch = "wasm32"))]
            LazyTreapMeter::NativeOriginal { original, .. } => {
            original.admit(WorkCounters::UNCHARGED)?;
            cancellation.check().map_err(|_| LazyWorkspaceError::NativeWork(
                crate::NativeTreeMaterializationError::Cancelled))?;
            let loaded;
            let state = if let Some(state) = basis.state {
                state
            } else {
                loaded = self.store.load_lazy_workspace_with_native_work(
                    self.workspace.id(), original,
                ).await?.ok_or(LazyWorkspaceError::StaleSource)?;
                loaded.as_state()
            };
            if state.workspace_id != self.workspace.id()
                || state.source.identity != self.source.reference().identity
            {
                return Err(LazyWorkspaceError::StaleSource);
            }
            let reader = self.directory_head_reader(original, cancellation).await?;
            let ((), allocation) = original.allocations(
                |_, _, _, _| Ok::<_, LazyWorkspaceError>(()),
            )?;
            let mut context = NativeInspectState { reader, record: None, metadata: None, allocation };
            let mut accounting = LazyReadonlyAccounting::Native { original, cancellation };
            let result = {
                let mut backend = LazyInspectBackend::Native(&mut context);
                self.inspect_with_accounting(path, InspectBasis {
                    state: Some(state), ..basis
                }, &mut accounting, &mut backend).await
            }.and_then(|resolved| { accounting.check()?; Ok(resolved) });
            let NativeInspectState { reader, allocation, .. } = context;
            drop(reader);
            match result {
                Ok(resolved) => Ok(LazyReadonlyNode::Native(NativeOwnedCoreState::from_parts(
                    (resolved.lookup, resolved.source), allocation,
                ))),
                Err(error) => { drop(allocation); Err(error) }
            }
            }
        }
        }).await
    }

    /// The source view lookups currently resolve in.
    #[cfg(feature = "native-mount")]
    pub(crate) fn source_reference(&self) -> SourceReference {
        self.source.reference()
    }

    /// What the source itself names at `path` in the view `source`, read afresh and bypassing the overlay.
    #[cfg(feature = "native-mount")]
    pub(crate) async fn source_lookup(
        &self,
        source: SourceReference,
        path: &str,
    ) -> Result<Option<SourceNode>, LazyWorkspaceError> {
        self.source_lookup_measured(source, path, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await.map(|receipt| receipt.value)
            .map_err(|failure| lazy_operation_failure(failure, std::convert::identity))
    }

    #[cfg(feature = "native-mount")]
    pub(crate) async fn source_lookup_measured(
        &self, source: SourceReference, path: &str, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<Option<SourceNode>>, LazyWorkspaceError> {
        self.source_request_measured(path, budget, cancellation, |path, budget| async move {
            self.source.lookup(source, &path, budget, cancellation).await
        }).await.map_err(LazyWorkspaceError::into_measured_failure)
    }

    /// The source node `path` names in `state`: a pinned observation, the node a listing supplied, or a fresh source lookup, never a tombstoned path.
    fn source_node_in<'a, 'accounting, 'backend>(
        &'a self,
        state: &'a LazyWorkspaceState,
        path: &'a str,
        basis: InspectBasis<'a>,
        accounting: &'a mut LazyReadonlyAccounting<'accounting>,
        backend: &'a mut LazyInspectBackend<'backend, A, O>,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<(SourceReference, SourceNode), LazyWorkspaceError>> {
        Box::pin(async move {
            let fact = if basis.unobserved && basis.listed.is_some() {
                None
            } else {
                self.overlay_fact(state.overlay, path, accounting).await?
            };
            let observed = match fact {
                Some(LazyOverlayChange::Tombstone { .. }) => return Err(LazyWorkspaceError::NotFound),
                Some(LazyOverlayChange::Observe { source, node })
                    if basis.retain_pinned_observation || source == state.source => Some((source, node)),
                Some(LazyOverlayChange::Observe { .. }) | None => None,
            };
            let (source, node) = if let Some(observed) = observed {
                observed
            } else if let Some(node) = basis.listed {
                (state.source, node)
            } else {
                accounting.check()?;
                if basis.authored_generation.is_some() && self.source.reference() != state.source {
                    return Err(LazyWorkspaceError::StaleSource);
                }
                let node = match accounting {
                    LazyReadonlyAccounting::Portable { work, budget, cancellation } => {
                        let cancellation = *cancellation;
                        let receipt = self.source_request_measured(
                            path, remaining_work(*work, *budget, 0)?, cancellation, |path, budget| async move {
                                if let Some(source_used) = basis.source_used {
                                    source_used.store(true, std::sync::atomic::Ordering::Release);
                                }
                                self.source.lookup(state.source, &path, budget, cancellation).await
                            },
                        ).await?;
                        *work = account_work(*work, receipt.work, *budget)?;
                        receipt.value
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    LazyReadonlyAccounting::Native { original, cancellation } => {
                            let original = *original;
                            let result = self.source_request_with_work(
                                path, state.source, original, cancellation, |path| async move {
                                    if let Some(source_used) = basis.source_used {
                                        source_used.store(true, std::sync::atomic::Ordering::Release);
                                    }
                                    self.source.lookup(state.source, &path, original, cancellation).await
                                },
                            ).await;
                            match result {
                                Ok((node, mut allocation)) => {
                                    if let LazyInspectBackend::Native(context) = backend {
                                        context.allocation.absorb(&mut allocation)?;
                                    }
                                    node
                                }
                                Err((error, allocation)) => {
                                    if let (Some(mut allocation), LazyInspectBackend::Native(context)) =
                                        (allocation, backend)
                                    {
                                        context.allocation.absorb(&mut allocation)?;
                                    }
                                    return Err(error);
                                }
                            }
                    }
                };
                if basis.authored_generation.is_some() && self.source.reference() != state.source {
                    return Err(LazyWorkspaceError::StaleSource);
                }
                (state.source, node.ok_or(LazyWorkspaceError::NotFound)?)
            };
            if basis.listed.is_none()
                && self.tombstoned(state.overlay, path, accounting).await?
            {
                return Err(LazyWorkspaceError::NotFound);
            }
            Ok((source, node))
        })
    }

    async fn inspect_with(
        &self,
        path: &str,
        basis: InspectBasis<'_>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<ResolvedLazyLookup>, LazyWorkspaceError> {
        let mut accounting = LazyReadonlyAccounting::Portable {
            work: WorkCounters::UNCHARGED, budget, cancellation,
        };
        let mut backend = LazyInspectBackend::Measured(std::marker::PhantomData);
        let result = self.inspect_with_accounting(path, basis, &mut accounting, &mut backend).await;
        accounting.finish_measured(result)
    }

    async fn inspect_with_accounting(
        &self,
        path: &str,
        basis: InspectBasis<'_>,
        accounting: &mut LazyReadonlyAccounting<'_>,
        backend: &mut LazyInspectBackend<'_, A, O>,
    ) -> Result<ResolvedLazyLookup, LazyWorkspaceError> {
        accounting.check()?;
        if !basis.authored_absent {
            if let Some(lookup) = self.authored_lookup_with_accounting(path, basis, accounting, backend).await? {
                return Ok(ResolvedLazyLookup { lookup, source: None });
            }
        }
        let loaded_state;
        let state = if let Some(state) = basis.state {
            state
        } else {
            loaded_state = match accounting {
                LazyReadonlyAccounting::Portable { work, budget, cancellation } => {
                    let receipt = { let state_work = LazyTreapWork::new(remaining_work(*work, *budget, 0)?, cancellation); let result = self.state(&state_work).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("standalone state keeps its accounting owner") }); state_work.measured(result, |error, spent| error.with_prior_work(spent)) }?;
                    *work = account_work(*work, receipt.work, *budget)?;
                    receipt.value
                }
                #[cfg(not(target_arch = "wasm32"))]
                LazyReadonlyAccounting::Native { .. } =>
                    return Err(LazyWorkspaceError::OriginalSnapshotUnavailable),
            };
            &loaded_state
        };
        let (source, node) = self.source_node_in(state, path, basis, accounting, backend).await?;
        if let Some(lookup) = self.authored_alias_with_accounting(&node, state, basis, accounting, backend).await? {
            return Ok(ResolvedLazyLookup { lookup, source: None });
        }
        Ok(ResolvedLazyLookup { lookup: LazyLookup::Source(node), source: Some(source) })
    }

    async fn authored_lookup_with_accounting(
        &self, path: &str, basis: InspectBasis<'_>,
        accounting: &mut LazyReadonlyAccounting<'_>,
        backend: &mut LazyInspectBackend<'_, A, O>,
    ) -> Result<Option<LazyLookup>, LazyWorkspaceError> {
        match accounting {
            LazyReadonlyAccounting::Portable { work, budget, cancellation } => {
                let authored = if let Some(generation) = basis.authored_generation {
                    self.authored_stat_at_measured(
                        generation, path, remaining_work(*work, *budget, 0)?, cancellation,
                    ).await?
                } else {
                    self.workspace.stat_optional_measured(
                        path, remaining_work(*work, *budget, 0)?, cancellation,
                    ).await.map_err(measured_workspace_error)?
                };
                *work = account_work(*work, authored.work, *budget)?;
                let Some(stat) = authored.value else { return Ok(None); };
                let paths = LazyTreapWork::new(remaining_work(*work, *budget, 0)?, cancellation);
                let path = paths.canonical_path_owned(path, self.workspace.limits())
                    .map_err(|error| error.with_prior_work(paths.spent()))?;
                *work = account_work(*work, paths.spent(), *budget)?;
                Ok(Some(LazyLookup::Authored { path, stat }))
            }
            #[cfg(not(target_arch = "wasm32"))]
            LazyReadonlyAccounting::Native { original, cancellation } => {
                if let LazyInspectBackend::Native(context) = backend {
                    let (namespace, mut allocation) = self.source_namespace_with_work(path, original, cancellation)?;
                    let result = context.reader.lookup_record_with_native_work(
                        &namespace, original, cancellation,
                    ).await.map_err(native_filesystem_error);
                    drop(namespace);
                    let released = allocation.release(allocation.bytes());
                    if released.is_err() {
                        context.allocation.absorb(&mut allocation)?;
                    }
                    let record = result?;
                    released?;
                    let Some(record) = record else { return Ok(None); };
                    let metadata = context.reader.read_record_metadata(
                        record, &**original, cancellation,
                    ).await.map_err(native_filesystem_error)?;
                    let (path, mut allocation) = self.authored_path_with_work(path, original, cancellation)?;
                    if let Err(error) = context.allocation.absorb(&mut allocation) {
                        drop(path);
                        drop(allocation);
                        return Err(LazyWorkspaceError::NativeWork(error));
                    }
                    context.record = Some(record);
                    context.metadata = Some(metadata);
                    return Ok(Some(LazyLookup::Authored {
                        path, stat: authored_record_stat(record, metadata),
                    }));

                }
                let _ = (original, cancellation, backend);
                Err(LazyWorkspaceError::OriginalSnapshotUnavailable)
            }
        }
    }

    async fn authored_alias_with_accounting(
        &self, node: &SourceNode, state: &LazyWorkspaceState, basis: InspectBasis<'_>,
        accounting: &mut LazyReadonlyAccounting<'_>,
        backend: &mut LazyInspectBackend<'_, A, O>,
    ) -> Result<Option<LazyLookup>, LazyWorkspaceError> {
        match accounting {
            LazyReadonlyAccounting::Portable { work, budget, cancellation } => {
                let receipt = self.authored_alias_at_measured(
                    node, Some(state), basis.authored_generation,
                    remaining_work(*work, *budget, 0)?, cancellation,
                ).await?;
                *work = account_work(*work, receipt.work, *budget)?;
                Ok(receipt.value)
            }
            #[cfg(not(target_arch = "wasm32"))]
            LazyReadonlyAccounting::Native { original, cancellation } => {
                if let LazyInspectBackend::Native(context) = backend {
                    return self.authored_alias_with_work(node, state, context, original, cancellation).await;
                }
                let _ = (original, cancellation, backend);
                Err(LazyWorkspaceError::OriginalSnapshotUnavailable)
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn authored_path_with_work(
        &self, path: &str, original: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(String, crate::materializer::NativeAllocation), LazyWorkspaceError> {
        original.admit(WorkCounters::UNCHARGED)?;
        cancellation.check().map_err(|_| LazyWorkspaceError::NativeWork(
            crate::NativeTreeMaterializationError::Cancelled))?;
        let paths = LazyTreapWork::native_with_work(original, cancellation)?;
        let path = paths.canonical_path_owned(path, self.workspace.limits())?;
        let allocation = paths.take_native_owned(
            crate::foundation::usize_to_u64(path.capacity()))?
            .ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?;
        Ok((path, allocation))
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn authored_alias_with_work(
        &self, node: &SourceNode, state: &LazyWorkspaceState,
        context: &mut NativeInspectState<A, O>,
        original: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<Option<LazyLookup>, LazyWorkspaceError> {
        original.admit(WorkCounters::UNCHARGED)?;
        cancellation.check().map_err(|_| LazyWorkspaceError::NativeWork(
            crate::NativeTreeMaterializationError::Cancelled))?;
        if node.kind == SourceNodeKind::Directory || node.link_count == Some(1) {
            return Ok(None);
        }
        let file_id = self.source_file_id(&node.file_identity,
            &mut crate::kernel::allocation::KernelAccounting::native(original, cancellation))?;
        // This bound counts examined namespace entries, not matching aliases.
        let path = context.reader.first_path_for_file_id_with_native_work(
            file_id, u32::MAX, original, cancellation,
        ).await.map_err(native_workspace_error)?
            .map(|(path, allocation)| NativeOwnedCoreState::from_parts(path, allocation));
        if let Some(path) = path {
            let record = context.reader.file_record_by_id_with_native_work(
                file_id, original, cancellation,
            ).await.map_err(native_filesystem_error)?
                .ok_or(LazyWorkspaceError::Concurrent)?;
            let metadata = context.reader.read_record_metadata(
                record, original, cancellation,
            ).await.map_err(native_filesystem_error)?;
            let mut stat = authored_record_stat(record, metadata);
            if let Some(source_links) = node.link_count {
                stat.link_count = stat.link_count.max(source_links);
            }
            let (path, mut allocation) = path.into_parts();
            if let Err(error) = context.allocation.absorb(&mut allocation) {
                drop(path);
                drop(allocation);
                return Err(LazyWorkspaceError::NativeWork(error));
            }
            context.record = Some(record);
            context.metadata = Some(metadata);
            return Ok(Some(LazyLookup::Authored { path, stat }));
        }
        let mut readonly = LazyReadonlyAccounting::Native { original, cancellation };
        let Some((record, metadata)) = self.shadow_record(
            state.shadows, file_id, &mut readonly,
        ).await? else { return Ok(None); };
        let actual_metadata = context.reader.read_record_metadata(
            record, original, cancellation,
        ).await.map_err(native_filesystem_error)?;
        context.record = Some(record);
        context.metadata = Some(actual_metadata);
        Ok(Some(LazyLookup::Shadow {
            record, metadata, source_link_count: node.link_count.unwrap_or(1),
        }))

    }

    /// Returns the source identity on the caller's accounting context.
    pub(crate) fn source_file_id(
        &self, file_identity: &[u8; 32], accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    ) -> Result<FileId, LazyWorkspaceError> {
        use crate::kernel::allocation::KernelAccounting;
        let (work, budget, cancellation) = match accounting {
            KernelAccounting::Standalone { work, budget, cancellation, .. } => (work, *budget, *cancellation),
            KernelAccounting::BorrowedStandalone { work, budget, cancellation, .. } => (&mut **work, *budget, *cancellation),
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work: original, cancellation } => {
                original.admit(WorkCounters::UNCHARGED)?;
                cancellation.check().map_err(|_| LazyWorkspaceError::NativeWork(
                    crate::NativeTreeMaterializationError::Cancelled))?;
                let source_identity = self.source.reference().identity;
                let hashed = WorkCounters { bytes_hashed: (b"acyclic-fs-lazy-source-file-id-v1\0".len()
                    + source_identity.len() + file_identity.len()) as u64, ..WorkCounters::UNCHARGED };
                cancellation.check().map_err(|_| LazyWorkspaceError::NativeWork(
                    crate::NativeTreeMaterializationError::Cancelled))?;
                original.charge(hashed)?;
                let file_id = Self::source_file_id_at_identity(file_identity, &source_identity);
                original.admit(WorkCounters::UNCHARGED)?;
                cancellation.check().map_err(|_| LazyWorkspaceError::NativeWork(
                    crate::NativeTreeMaterializationError::Cancelled))?;
                if self.source.reference().identity != source_identity { return Err(LazyWorkspaceError::StaleSource); }
                return Ok(file_id);
            }
        };
        let allowance = remaining_work(*work, budget, 0)?;
        let mut spent = WorkCounters::UNCHARGED;
        let hashed = WorkCounters { bytes_hashed: (b"acyclic-fs-lazy-source-file-id-v1\0".len()
            + self.source.reference().identity.len() + file_identity.len()) as u64, ..WorkCounters::UNCHARGED };
        spent.admit(&hashed, &allowance)?;
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        let file_id = Self::source_file_id_at_identity(file_identity, &self.source.reference().identity);
        spent.charge(&hashed, &allowance)?;
        *work = account_work(*work, spent, budget)?;
        Ok(file_id)
    }

    fn source_file_id_at_identity(file_identity: &[u8; 32], source_identity: &[u8; 16]) -> FileId {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"acyclic-fs-lazy-source-file-id-v1\0");
        hasher.update(source_identity);
        hasher.update(file_identity);
        let digest = hasher.finalize();
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(&digest.as_bytes()[..16]);
        FileId::from_bytes(bytes)
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

    /// Starts the existing source watcher with the original admitted work.
    #[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
    pub(crate) fn watch_source_with_native_work(
        &self,
        sink: Arc<dyn crate::demand::SourceChangeSink>,
        work: crate::demand::NativeSourceWork<'_>,
    ) -> Result<Option<Box<dyn crate::demand::SourceWatch>>, LazyWorkspaceError> {
        let original = work.materialization_work()?;
        self.source.watch(sink, &original, original.cancellation()).map_err(LazyWorkspaceError::from)
    }

    /// Direct identity of the resolved fact.
    fn direct_file_id_for_lookup(&self, lookup: &LazyLookup) -> FileId {
        match lookup {
            LazyLookup::Source(node) => Self::source_file_id_at_identity(&node.file_identity, &self.source.reference().identity),
            LazyLookup::Authored { stat, .. } => stat.file_id,
            LazyLookup::Shadow { record, .. } => record.file_id,
        }
    }

    #[cfg(any(feature = "native-mount", not(target_arch = "wasm32")))]
    pub(crate) async fn stable_file_id_for_lookup(
        &self, path: &str, lookup: &LazyLookup,
        accounting: &mut LazyReadonlyAccounting<'_>,
    ) -> Result<FileId, LazyWorkspaceError> {
        accounting.check()?;
        let identity = match lookup {
            LazyLookup::Source(node) => node.file_identity,
            LazyLookup::Shadow { record, .. } => return Ok(record.file_id),
            LazyLookup::Authored { stat, .. } if stat.kind == FileKind::Directory => {
                let phase = match accounting {
                    LazyReadonlyAccounting::Portable { work, budget, cancellation } => {
                        let state = { let state_work = LazyTreapWork::new(remaining_work(*work, *budget, 0)?, cancellation); let result = self.state(&state_work).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("standalone state keeps its accounting owner") }); state_work.measured(result, |error, spent| error.with_prior_work(spent)) }?;
                        *work = account_work(*work, state.work, *budget)?;
                        LazyReadonlyNode::Portable(state.value)
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    LazyReadonlyAccounting::Native { original, .. } => {
                        let state = self.store.load_lazy_workspace_with_native_work(
                            self.workspace.id(), original,
                        ).await?.ok_or(LazyWorkspaceError::StaleSource)?;
                        if state.as_state().workspace_id != self.workspace.id()
                            || state.as_state().source.identity != self.source.reference().identity
                        {
                            return Err(LazyWorkspaceError::StaleSource);
                        }
                        LazyReadonlyNode::Native(state)
                    }
                };
                let fact = self.overlay_fact(phase.as_state().overlay, path, accounting).await?;
                match fact {
                    Some(LazyOverlayChange::Observe { node, .. }) if node.kind == SourceNodeKind::Directory =>
                        node.file_identity,
                    Some(LazyOverlayChange::Observe { .. })
                    | Some(LazyOverlayChange::Tombstone { .. })
                    | None => return Ok(stat.file_id),
                }
            }
            LazyLookup::Authored { stat, .. } => return Ok(stat.file_id),
        };
        accounting.check()?;
        match accounting {
            LazyReadonlyAccounting::Portable { work, budget, cancellation } =>
                self.source_file_id(&identity, &mut crate::kernel::allocation::KernelAccounting::borrowed_standalone(
                    &mut crate::kernel::allocation::AllocationLedger::default(), work, *budget, cancellation)),
            #[cfg(not(target_arch = "wasm32"))]
            LazyReadonlyAccounting::Native { original, cancellation } =>
                self.source_file_id(&identity, &mut crate::kernel::allocation::KernelAccounting::native(original, cancellation)),
        }
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
            if node.kind == SourceNodeKind::Directory || node.link_count == Some(1) {
                return Ok(OperationReceipt {
                    value: None,
                    work: WorkCounters::default(),
                });
            }
            let file_id = self.source_file_id(&node.file_identity,
                &mut crate::kernel::allocation::KernelAccounting::borrowed_standalone(
                    &mut crate::kernel::allocation::AllocationLedger::default(), &mut work, budget, cancellation))?;
            let loaded_state;
            let state = if let Some(state) = pinned_state {
                state
            } else {
                let loaded = { let state_work = LazyTreapWork::new(remaining_work(work, budget, 0)?, cancellation); let result = self.state(&state_work).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("standalone state keeps its accounting owner") }); state_work.measured(result, |error, spent| error.with_prior_work(spent)) }?;
                work = account_work(work, loaded.work, budget)?;
                loaded_state = loaded.value;
                &loaded_state
            };
            let generation = if let Some(id) = authored_generation {
                let opened = self.workspace.generation(id, remaining_work(work, budget, 0)?, cancellation)
                    .await.map_err(measured_workspace_error)?;
                work = account_work(work, opened.work, budget)?;
                Some(opened.value)
            } else {
                let head = self.workspace.head_with_file_record_measured(
                    file_id, remaining_work(work, budget, 0)?, cancellation,
                ).await.map_err(measured_workspace_error)?;
                work = account_work(work, head.work, budget)?;
                let (generation, held) = head.value;
                held.then_some(generation)
            };
            let path = if let Some(generation) = generation {
                let paths = generation.paths_for_file_id(
                    file_id, u32::MAX, remaining_work(work, budget, 0)?, cancellation,
                ).await.map_err(measured_workspace_error)?;
                work = account_work(work, paths.work, budget)?;
                paths.value.into_iter().next()
            } else {
                None
            };
            if let Some(path) = path {
                let stat = if let Some(id) = authored_generation {
                    self.authored_stat_at_measured(id, &path, remaining_work(work, budget, 0)?, cancellation).await?
                } else {
                    self.workspace.stat_optional_measured(&path, remaining_work(work, budget, 0)?, cancellation)
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
            let shadow = { let mut accounting = LazyReadonlyAccounting::Portable { work: WorkCounters::UNCHARGED, budget: remaining_work(work, budget, 0)?, cancellation }; let result = self.shadow_record(state.shadows, file_id, &mut accounting).await; accounting.finish_measured(result) }?;
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
        }, budget, cancellation, LazyWorkspaceError::Cancelled, None)
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
        let opened = self.workspace.generation(id, budget, cancellation)
            .await.map_err(measured_workspace_error)?;
        let prior = opened.work;
        match opened.value.stat(path, remaining_work(prior, budget, 0)?, cancellation).await {
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
        &self, snapshot: LazySnapshotRef, require_exact_epoch: bool,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    ) -> Result<LazyWorkspaceState, LazyWorkspaceError> {
        use crate::kernel::allocation::KernelAccounting;
        let cancellation = match &*accounting {
            KernelAccounting::Standalone { cancellation, .. }
            | KernelAccounting::BorrowedStandalone { cancellation, .. } => *cancellation,
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work: original, cancellation } => {
                original.admit(WorkCounters::UNCHARGED)?;
                cancellation.check().map_err(|_| LazyWorkspaceError::NativeWork(
                    crate::NativeTreeMaterializationError::Cancelled))?;
                *cancellation
            }
        };
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        let live = self.source.reference();
        if snapshot.workspace_id != self.workspace.id() || live.identity != snapshot.source.identity
            || (require_exact_epoch && live.epoch != snapshot.source.epoch) {
            return Err(LazyWorkspaceError::StaleSource);
        }
        let delta = WorkCounters {
            bytes_hashed: crate::foundation::usize_to_u64(LAZY_SNAPSHOT_DOMAIN.len()) + 136,
            ..WorkCounters::UNCHARGED
        };
        match accounting {
            KernelAccounting::Standalone { work, budget, .. } => work.charge(&delta, budget)?,
            KernelAccounting::BorrowedStandalone { work, budget, .. } => work.charge(&delta, budget)?,
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work: original, .. } => original.charge(delta)?,
        }
        if snapshot != LazySnapshotRef::new(
            snapshot.workspace_id, snapshot.authored_generation, snapshot.source,
            snapshot.overlay, snapshot.shadows,
        ) {
            return Err(LazyWorkspaceError::StaleSource);
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let KernelAccounting::NativeOriginal { work: original, .. } = &*accounting {
            original.admit(WorkCounters::UNCHARGED)?;
            cancellation.check().map_err(|_| LazyWorkspaceError::NativeWork(
                crate::materializer::NativeTreeMaterializationError::Cancelled))?;
        }
        Ok(LazyWorkspaceState { revision: 0,
            ..LazyWorkspaceState::new(snapshot.workspace_id, snapshot.source, snapshot.overlay, snapshot.shadows) })
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn inspect_at_snapshot_with_work(
        &self,
        snapshot: LazySnapshotRef,
        path: &str,
        original: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
        source_used: &std::sync::atomic::AtomicBool,
    ) -> Result<(ResolvedLazyLookup, NativeInspectState<A, O>),
        (LazyWorkspaceError, Option<crate::materializer::NativeAllocation>)>
    {
        let frame = LazyTreapWork::native_with_work(original, cancellation)
            .map_err(|error| (error, None))?;
        in_heap_lazy::<_, LazyWorkspaceError, _>(&frame, || async {
            Ok(async {
        let state = self.readonly_snapshot_state(snapshot, false,
            &mut crate::kernel::allocation::KernelAccounting::native(original, cancellation))
            .map_err(|error| (error, None))?;
        let reader = self.workspace.readonly_pinned_reader_with_native_work(
            snapshot.authored_generation, original, cancellation,
        ).await.map_err(|error| (native_workspace_error(error), None))?;
        let mut accounting = LazyReadonlyAccounting::Native { original, cancellation };
        // Authenticate the actual retained roots, including an unused empty root.
        self.load_overlay_with_readonly_accounting(state.overlay, &mut accounting).await
            .map_err(|error| (error, None))?;
        self.load_shadow_with_readonly_accounting(state.shadows, &mut accounting).await
            .map_err(|error| (error, None))?;
        let (_, allocation) = original.allocations(
            |_, _, _, _| Ok::<(), LazyWorkspaceError>(()),
        ).map_err(|error| (error, None))?;
        let mut context = NativeInspectState {
            reader, record: None, metadata: None, allocation,
        };
        let result = {
            let mut backend = LazyInspectBackend::Native(&mut context);
            self.inspect_with_accounting(path, InspectBasis {
                state: Some(&state),
                authored_generation: Some(snapshot.authored_generation),
                source_used: Some(source_used),
                retain_pinned_observation: true,
                ..InspectBasis::default()
            }, &mut accounting, &mut backend).await
        }.and_then(|resolved| {
            accounting.check()?;
            let live = self.source.reference();
            if live.identity != snapshot.source.identity
                || (source_used.load(std::sync::atomic::Ordering::Acquire)
                    && live != snapshot.source)
            {
                return Err(LazyWorkspaceError::StaleSource);
            }
            Ok(resolved)
        });
        let NativeInspectState { reader, record, metadata, mut allocation } = context;
        match result {
            Ok(resolved) => Ok((resolved, NativeInspectState { reader, record, metadata, allocation })),
            Err(error) => {
                drop(reader);
                // The original failure remains primary; failed retirement retains custody.
                let _ = allocation.release(allocation.bytes());
                Err((error, Some(allocation)))
            }
        }
            }.await)
        }).await.map_err(|error| (error, None))?
    }

    /// Authenticates frozen roots without refreshing the parent.
    pub async fn validate_snapshot(
        &self, frozen: LazySnapshotRef, work: &LazyTreapWork<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let cancellation = work.cancellation;
        in_heap_lazy::<_, LazyWorkspaceError, _>(work, || async {
        match &work.meter {
            #[cfg(not(target_arch = "wasm32"))]
            LazyTreapMeter::NativeOriginal { original, .. } => {
                    self.readonly_snapshot_state(frozen, false,
                        &mut crate::kernel::allocation::KernelAccounting::native(original, cancellation))?;
                    let reader = self.workspace.readonly_pinned_reader_with_native_work(
                        frozen.authored_generation, original, cancellation,
                    ).await.map_err(native_workspace_error)?;
                    drop(reader);
                    let overlay = self.store.load_lazy_overlay_with_native_work(frozen.overlay, original).await?;
                    let present = overlay.is_some();
                    drop(overlay);
                    if !present { return Err(LazyWorkspaceError::OriginalSnapshotUnavailable); }
                    let shadow = self.store.load_lazy_shadow_with_native_work(frozen.shadows, original).await?;
                    let present = shadow.is_some();
                    drop(shadow);
                    if !present { return Err(LazyWorkspaceError::OriginalSnapshotUnavailable); }
                    original.charge(WorkCounters::UNCHARGED)?;
                    if self.source.reference().identity != frozen.source.identity {
                        return Err(LazyWorkspaceError::StaleSource);
                    }
                    Ok(())
            }
            LazyTreapMeter::Standalone { .. } => {
                work.with_kernel(|accounting, _| self.readonly_snapshot_state(frozen, false, accounting).map(|_| ()))?;
                let generation = work.accept(self.workspace.generation(
                    frozen.authored_generation, work.remaining()?, cancellation,
                ).await.map_err(|failure| lazy_operation_failure(failure, LazyWorkspaceError::WorkspaceCause)))?;
                drop(generation);
                let overlay = work.accept(self.store.load_lazy_overlay_measured(
                    frozen.overlay, work.remaining()?, cancellation,
                ).await)?;
                let present = overlay.is_some();
                drop(overlay);
                if !present { return Err(LazyWorkspaceError::Store("lazy overlay is absent".to_owned())); }
                let shadow = work.accept(self.store.load_lazy_shadow_measured(
                    frozen.shadows, work.remaining()?, cancellation,
                ).await)?;
                let present = shadow.is_some();
                drop(shadow);
                if !present { return Err(LazyWorkspaceError::Store("lazy identity shadow is absent".to_owned())); }
                work.charge(WorkCounters::UNCHARGED)?;
                if self.source.reference().identity != frozen.source.identity {
                    return Err(LazyWorkspaceError::StaleSource);
                }
                Ok(())
            }
        }
        }).await
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
            let source_used = std::sync::atomic::AtomicBool::new(false);
            let result = async {
                let state = self.readonly_snapshot_state(snapshot, false, &mut crate::kernel::allocation::KernelAccounting::borrowed_standalone(
                    &mut crate::kernel::allocation::AllocationLedger::default(), &mut work, budget, cancellation))?;
                let resolved = self.inspect_with(path, InspectBasis {
                    state: Some(&state),
                    authored_generation: Some(snapshot.authored_generation),
                    retain_pinned_observation: true,
                    source_used: Some(&source_used),
                    ..InspectBasis::default()
                }, remaining_work(work, budget, 0)?, cancellation).await?;
                work = account_work(work, resolved.work, budget)?;
                cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
                if source_used.load(std::sync::atomic::Ordering::Acquire)
                    && self.source.reference() != snapshot.source {
                    return Err(LazyWorkspaceError::StaleSource);
                }
                Ok(resolved.value.lookup)
            }.await;
            result.map(|value| OperationReceipt { value, work })
                .map_err(|error: LazyWorkspaceError| error.into_measured_failure()
                    .map_with_prior_work(work, std::convert::identity))
        }, budget, cancellation, LazyWorkspaceError::Cancelled, None).await
    }

    /// Looks up exactly the frozen view using the caller's original allocation ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn lookup_at_snapshot_with_work(
        &self, snapshot: LazySnapshotRef, path: &str,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(LazyLookup, crate::NativeAllocation),
        (LazyWorkspaceError, Option<crate::NativeAllocation>)> {
        let source_used = std::sync::atomic::AtomicBool::new(false);
        let frozen = self.inspect_at_snapshot_with_work(
            snapshot, path, original, cancellation, &source_used,
        ).await.map_err(|(error, allocation)| error.into_native_failure_with_frame(allocation))?;
        let checked = original.admit(WorkCounters::UNCHARGED)
            .map_err(LazyWorkspaceError::NativeWork)
            .and_then(|()| cancellation.check().map_err(|_| LazyWorkspaceError::NativeWork(
                crate::NativeTreeMaterializationError::Cancelled)))
            .and_then(|()| {
                if source_used.load(std::sync::atomic::Ordering::Acquire)
                    && self.source.reference() != snapshot.source {
                    Err(LazyWorkspaceError::StaleSource)
                } else { Ok(()) }
            });
        match checked {
            Ok(()) => {
                let (resolved, NativeInspectState { reader, allocation, .. }) = frozen;
                drop(reader);
                Ok((resolved.lookup, allocation))
            }
            Err(error) => finish_native_frozen_read(frozen, Err(error)),
        }
    }

    /// Reads the exact frozen regular file on the caller's original ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn read_at_snapshot_with_work(
        &self, snapshot: LazySnapshotRef, path: &str, maximum_bytes: u64,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<acyclic_stream::OwnedBytes,
        (LazyWorkspaceError, Option<crate::NativeAllocation>)> {
        let frame = LazyTreapWork::native_with_work(original, cancellation)
            .map_err(|error| (error, None))?;
        in_heap_lazy::<_, LazyWorkspaceError, _>(&frame, || async {
            Ok(async {
        let source_used = std::sync::atomic::AtomicBool::new(false);
        let frozen = self.inspect_at_snapshot_with_work(
            snapshot, path, original, cancellation, &source_used,
        ).await.map_err(|(error, allocation)| error.into_native_failure_with_frame(allocation))?;
        let result = async {
            let source_required = source_used.load(std::sync::atomic::Ordering::Acquire)
                || matches!(&frozen.0.lookup, LazyLookup::Source(_));
            if source_required && self.source.reference() != snapshot.source {
                return Err(LazyWorkspaceError::StaleSource);
            }
            let read = {
                let stat = frozen.0.lookup.stat();
                if stat.kind != SourceNodeKind::RegularFile {
                    return Err(LazyWorkspaceError::NotRegularFile);
                }
                let length = stat.logical_bytes.ok_or(LazyWorkspaceError::NotRegularFile)?;
                if length > maximum_bytes { return Err(LazyWorkspaceError::TooLarge); }
                self.read_resolved_with_work(&frozen.0.lookup, frozen.0.source, Some(&frozen.1.reader), frozen.1.record, path, Some(crate::ByteRange { offset: 0, length: length }), u64::MAX, original, cancellation).await
            };
            let chunk = match read {
                Ok(chunk) => chunk,
                Err((error, metadata)) => { drop(metadata); return Err(error); }
            };
            original.admit(WorkCounters::UNCHARGED)?;
            cancellation.check().map_err(|_| LazyWorkspaceError::NativeWork(
                crate::NativeTreeMaterializationError::Cancelled))?;
            if source_required && self.source.reference() != snapshot.source {
                return Err(LazyWorkspaceError::StaleSource);
            }
            Ok(chunk)
        }.await;
        let chunk = finish_native_frozen_read(frozen, result)?;
        let (bytes, _, _) = chunk.into_owned_parts();
        Ok(bytes)
            }.await)
        }).await.map_err(|error| (error, None))?
    }

    /// Reads a complete regular file from exactly this frozen authored/source view.
    pub async fn read_at_snapshot_measured(
        &self,
        snapshot: LazySnapshotRef,
        path: &str,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<acyclic_stream::OwnedBytes>, LazyWorkspaceError> {
        self.read_snapshot(snapshot, path, FrozenRead::Regular(maximum_bytes), budget, cancellation).await
    }

    /// Reads one bounded range from the original frozen view without hydrating it.
    #[allow(clippy::too_many_arguments)]
    pub async fn read_range_at_snapshot_measured(
        &self,
        snapshot: LazySnapshotRef,
        path: &str,
        offset: u64,
        length: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<acyclic_stream::OwnedBytes>, LazyWorkspaceError> {
        self.read_snapshot(snapshot, path, FrozenRead::Range(crate::ByteRange { offset, length }), budget, cancellation).await
    }

    /// Reads an exact frozen symbolic-link target without following or hydrating it.
    pub async fn read_link_at_snapshot_measured(
        &self,
        snapshot: LazySnapshotRef,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<acyclic_stream::OwnedBytes>, LazyWorkspaceError> {
        self.read_snapshot(snapshot, path, FrozenRead::Link, budget, cancellation).await
    }

    async fn read_snapshot(
        &self, snapshot: LazySnapshotRef, path: &str, request: FrozenRead,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<acyclic_stream::OwnedBytes>, LazyWorkspaceError> {
        in_heap_measured(move |budget| async move {
            let mut work = WorkCounters::UNCHARGED;
            let result = async {
                let source_used = std::sync::atomic::AtomicBool::new(false);
                let state = self.readonly_snapshot_state(snapshot, false, &mut crate::kernel::allocation::KernelAccounting::borrowed_standalone(
                    &mut crate::kernel::allocation::AllocationLedger::default(), &mut work, budget, cancellation))?;
                let lookup = self.inspect_with(path, InspectBasis {
                    state: Some(&state),
                    authored_generation: Some(snapshot.authored_generation),
                    retain_pinned_observation: true,
                    source_used: Some(&source_used),
                    ..InspectBasis::default()
                }, remaining_work(work, budget, 0)?, cancellation).await?;
                work = account_work(work, lookup.work, budget)?;
                let request = match request {
                    FrozenRead::Regular(maximum_bytes) => {
                        let length = match &lookup.value.lookup {
                            LazyLookup::Authored { stat, .. } if stat.kind == FileKind::Regular => stat.logical_bytes,
                            LazyLookup::Shadow { record, .. } if record.kind == FileKind::Regular => record_logical_bytes(*record),
                            LazyLookup::Source(node) if node.kind == SourceNodeKind::RegularFile => node.logical_bytes,
                            _ => return Err(LazyWorkspaceError::NotRegularFile),
                        }.ok_or(LazyWorkspaceError::NotRegularFile)?;
                        if length > maximum_bytes { return Err(LazyWorkspaceError::TooLarge); }
                        FrozenRead::Range(crate::ByteRange { offset: 0, length })
                    }
                    request => request,
                };
                let mut allocations = crate::kernel::allocation::AllocationLedger::default();
                allocations.claim_bytes(lookup.value.lookup.logical_owned_bytes(), 0, &mut work, budget)
                    .map_err(lazy_allocation_error)?;
                let source_required = source_used.load(std::sync::atomic::Ordering::Acquire)
                    || matches!(&lookup.value.lookup, LazyLookup::Source(_));
                if source_required && self.source.reference() != snapshot.source {
                    return Err(LazyWorkspaceError::StaleSource);
                }
                let read = match request {
                    FrozenRead::Range(range) => self.read_resolved(&lookup.value.lookup, lookup.value.source, Some(snapshot.authored_generation), path, Some(crate::ByteRange { offset: range.offset, length: range.length }), remaining_work(work, budget, allocations.live_bytes())?, cancellation).await,
                    FrozenRead::Link => self.read_resolved(&lookup.value.lookup, lookup.value.source, Some(snapshot.authored_generation), path, None, remaining_work(work, budget, allocations.live_bytes())?, cancellation).await,
                    FrozenRead::Regular(_) => unreachable!("complete reads resolve their range before admission"),
                }.map_err(|failure| nested_lazy_failure(
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
        }, budget, cancellation, LazyWorkspaceError::Cancelled, None).await
    }

    /// Reads one exact range, demanding no unrelated content.
    pub async fn read_range(
        &self,
        path: &str,
        offset: u64,
        length: u64,
    ) -> Result<acyclic_stream::OwnedBytes, LazyWorkspaceError> {
        let cancellation = CancellationToken::new();
        self.read_range_measured(path, offset, length, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| lazy_operation_failure(failure, std::convert::identity))
    }

    /// Opens the inspected regular source file for repeated reads, each of which proves `node` is still the file's current version.
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

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn open_source_file_with_native_work(
        &self, path: &str, source: SourceReference, node: SourceNode,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(Box<dyn crate::demand::DemandFile>, crate::NativeAllocation),
        (LazyWorkspaceError, Option<crate::NativeAllocation>)> {
        if node.kind != SourceNodeKind::RegularFile {
            return Err((LazyWorkspaceError::NotRegularFile, None));
        }
        self.source_request_with_work(path, source, original, cancellation, |path| async move {
            self.source.open_file(
                source, &path, node.version,
                original, cancellation).await
        }).await
    }

    async fn read_range_measured(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<acyclic_stream::OwnedBytes>, LazyWorkspaceError> {

        read_lazy_content!(self, path, Some(crate::ByteRange { offset, length }), budget, cancellation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_resolved_with_work(
        &self, lookup: &LazyLookup, source: Option<SourceReference>, reader: Option<&crate::PinnedReader<A, O>>,
        authored_record: Option<FileRecord>, path: &str, range: Option<crate::ByteRange>, maximum_bytes: u64,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<crate::kernel::OwnedSourceChunk, (LazyWorkspaceError, Option<crate::NativeAllocation>)> {
        original.admit(WorkCounters::UNCHARGED).map_err(|error| (LazyWorkspaceError::NativeWork(error), None))?;
        cancellation.check().map_err(|_| (LazyWorkspaceError::NativeWork(
            crate::NativeTreeMaterializationError::Cancelled), None))?;
        let kind = if range.is_some() { FileKind::Regular } else { FileKind::SymbolicLink };
        let record = match lookup {
            LazyLookup::Authored { .. } => authored_record.ok_or((LazyWorkspaceError::Concurrent, None))?,
            LazyLookup::Shadow { record, .. } => *record,
            LazyLookup::Source(node) => {
                if node.kind != source_kind(kind) { return Err((LazyWorkspaceError::NotRegularFile, None)); }
                if range.is_none() && node.logical_bytes.is_some_and(|length| length > maximum_bytes) {
                    return Err((LazyWorkspaceError::TooLarge, None));
                }
                let source = source.ok_or((LazyWorkspaceError::Concurrent, None))?;
                return match range {
                    Some(range) => self.read_source_range(path, source, node.version, range.offset, range.length, original, cancellation).await,
                    None => self.read_source_link(path, source, node.version, maximum_bytes, original, cancellation).await,
                };
            }
        };
        if record.kind != kind { return Err((LazyWorkspaceError::NotRegularFile, None)); }
        if range.is_none() && matches!(record.payload, FilePayload::SymbolicLink { target_bytes, .. } if target_bytes > maximum_bytes) {
            return Err((LazyWorkspaceError::TooLarge, None));
        }
        let reader = reader.ok_or((LazyWorkspaceError::OriginalSnapshotUnavailable, None))?;
        match range {
            Some(range) => reader.read_record_range(record, range, original, cancellation).await
                .map(|read| crate::kernel::OwnedSourceChunk::intrinsic(read.bytes, read.logical_owned_bytes)),
            None => reader.read_record_symbolic_link_owned_with_native_work(record, original, cancellation).await,
        }.map_err(|error| (native_filesystem_error(error), None))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn read_unauthored(
        &self, lookup: &LazyLookup, source: Option<SourceReference>, path: &str,
        range: Option<crate::ByteRange>, maximum_bytes: u64, work: &LazyTreapWork<'_>,
    ) -> Result<crate::kernel::OwnedSourceChunk,
        (LazyWorkspaceError, Option<crate::NativeAllocation>)> {
        let cancellation = work.cancellation;
        match &work.meter {
            LazyTreapMeter::Standalone { .. } => work.accept(self.read_resolved(
                lookup, source, None, path, range, work.remaining().map_err(|error| (error, None))?, cancellation,
            ).await.map_err(|failure| lazy_operation_failure(failure, std::convert::identity)))
                .map_err(|error| (error, None)),
            LazyTreapMeter::NativeOriginal { original, .. } => {
                let kind = if range.is_some() { FileKind::Regular } else { FileKind::SymbolicLink };
                if matches!(lookup, LazyLookup::Shadow { record, .. } if record.kind != kind) {
                    return Err((LazyWorkspaceError::NotRegularFile, None));
                }
                let reader = if matches!(lookup, LazyLookup::Shadow { .. }) {
                    let head = self.workspace.head(original, cancellation)
                        .await.map_err(|error| (error, None)).map_err(|error| (native_workspace_error_pair(error), None))?;
                    Some(self.workspace.readonly_pinned_reader_with_native_work(head.id(), original, cancellation)
                        .await.map_err(|error| (native_workspace_error(error), None))?)
                } else { None };
                self.read_resolved_with_work(lookup, source, reader.as_ref(), None, path,
                    range, maximum_bytes, original, cancellation).await
            }
        }
    }

    async fn read_resolved(
        &self, lookup: &LazyLookup, source: Option<SourceReference>,
        authored_generation: Option<crate::GenerationId>, path: &str, range: Option<crate::ByteRange>,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<crate::kernel::OwnedSourceChunk>, LazyWorkspaceError> {
        let mut work = WorkCounters::default();
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let result = async {
            macro_rules! read_authored {
                ($owner:expr, $path:expr, $budget:expr) => {
                    match range {
                        Some(range) => $owner.read_range(
                            $path, range.offset, range.length, $budget, cancellation,
                        ).await,
                        None => $owner.read_symbolic_link($path, $budget, cancellation).await,
                    }.map_err(measured_workspace_error)
                };
            }
            let output = match lookup {
                LazyLookup::Authored { path: authored_path, stat } => {
                    if range.is_none() && stat.kind != FileKind::SymbolicLink {
                        return Err(LazyWorkspaceError::NotRegularFile);
                    }
                    let receipt = if let Some(id) = authored_generation {
                        let generation = accept_directory_work(
                            self.workspace.generation(id, budget, cancellation).await
                                .map_err(measured_workspace_error), &mut work, &allocations,
                        )?;
                        read_authored!(generation, authored_path, remaining_work(work, budget, allocations.live_bytes())?)
                    } else { read_authored!(self.workspace, authored_path, budget) };
                    accept_directory_work(receipt, &mut work, &allocations)?
                }
                LazyLookup::Shadow { record, .. } => {
                    if range.is_some() && record.kind != FileKind::Regular {
                        return Err(LazyWorkspaceError::NotRegularFile);
                    }
                    let file = accept_directory_work(
                        crate::DetachedFile::from_record(self.workspace.volume.clone(), *record, budget, cancellation)
                            .map_err(|failure| measured_workspace_error(failure.map_error(WorkspaceError::from))), &mut work, &allocations,
                    )?;
                    let hold_bytes = file.logical_owned_bytes();
                    allocations.claim_bytes(hold_bytes, 0, &mut work, budget).map_err(lazy_allocation_error)?;
                    let read = async {
                        let budget = remaining_work(work, budget, allocations.live_bytes())?;
                        let receipt = match range {
                            Some(range) => file.read_range(range, budget, cancellation).await.map(|receipt| {
                                let logical_owned_bytes = receipt.value.logical_owned_bytes();
                                OperationReceipt { value: crate::kernel::OwnedSourceChunk::claimed(
                                    receipt.value.bytes, logical_owned_bytes,
                                ), work: receipt.work }
                            }),
                            None => file.read_symbolic_link(budget, cancellation).await,
                        }.map_err(|failure| lazy_operation_failure(failure, |error| workspace_error(error.into())));
                        accept_directory_work(receipt, &mut work, &allocations)
                    }.await;
                    drop(file);
                    allocations.release(hold_bytes).map_err(lazy_allocation_error)?;
                    read?
                }
                LazyLookup::Source(node) => {
                    let kind = if range.is_some() { SourceNodeKind::RegularFile } else { SourceNodeKind::SymbolicLink };
                    if node.kind != kind { return Err(LazyWorkspaceError::NotRegularFile); }
                    let source = source.ok_or(LazyWorkspaceError::Concurrent)?;
                    let receipt = self.source_request_measured(path, budget, cancellation, |path, budget| async move {
                        match range {
                            Some(range) => self.source.read_range(source, &path, node.version, range.offset, range.length, budget, cancellation).await,
                            None => self.source.read_link(source, &path, node.version, u64::MAX, budget, cancellation).await,
                        }
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

    /// Reads a complete regular file under an explicit byte bound.
    pub async fn read(&self, path: &str, maximum_bytes: u64) -> Result<acyclic_stream::OwnedBytes, LazyWorkspaceError> {
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
    pub async fn read_link(&self, path: &str) -> Result<acyclic_stream::OwnedBytes, LazyWorkspaceError> {
        let cancellation = CancellationToken::new();
        read_lazy_content!(self, path, None, WorkBudget::UNBOUNDED, &cancellation)
            .map(|receipt| receipt.value)
            .map_err(|failure| lazy_operation_failure(failure, std::convert::identity))
    }

    /// Finds a sparse boundary without demanding file contents.
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
                crate::DetachedFile::from_record(self.workspace.volume.clone(), record, WorkBudget::UNBOUNDED, &CancellationToken::new()).expect("unbounded detached hold preserves a validated record").value
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

    async fn populate_source_node_with_accounting(
        &self, transaction: &mut crate::Transaction<A, O>, requested: &str,
        pinned: &(SourceReference, SourceNode), maximum_bytes: u64, work: &LazyTreapWork<'_>,
        cancellation: &CancellationToken,
        source_used: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<(), LazyWorkspaceError> {
        in_heap_lazy::<_, LazyWorkspaceError, _>(work, move || async move {
                let node = &pinned.1;
                match node.kind {
                    SourceNodeKind::Directory => self.promotion_mutation_with_accounting(
                        transaction, requested, LazyPromotionMutation::Directory, work, cancellation.clone(),
                    ).await?,
                    SourceNodeKind::RegularFile => self.populate_regular_source_node(
                        transaction, requested, pinned, maximum_bytes, work, cancellation, source_used,
                    ).await?,
                    SourceNodeKind::SymbolicLink => {
                        if let Some(used) = source_used {
                            used.store(true, std::sync::atomic::Ordering::Release);
                        }
                        let chunk = self.promotion_source_chunk_with_accounting(
                            requested, pinned, None, u64::MAX, work, cancellation,
                        ).await?;
                        self.promotion_mutation_with_accounting(
                            transaction, requested, LazyPromotionMutation::Link(chunk), work, cancellation.clone(),
                        ).await?;
                    }
                    SourceNodeKind::Fifo | SourceNodeKind::Socket
                    | SourceNodeKind::CharacterDevice | SourceNodeKind::BlockDevice => {
                        let kind = match node.kind {
                            SourceNodeKind::Fifo => FileKind::Fifo,
                            SourceNodeKind::Socket => FileKind::Socket,
                            SourceNodeKind::CharacterDevice => FileKind::CharacterDevice,
                            SourceNodeKind::BlockDevice => FileKind::BlockDevice,
                            _ => unreachable!(),
                        };
                        self.promotion_mutation_with_accounting(
                            transaction, requested, LazyPromotionMutation::Special(kind, node.device),
                            work, cancellation.clone(),
                        ).await?;
                    }
                    SourceNodeKind::Unsupported => return Err(LazyWorkspaceError::UnsupportedNode),
                }
                self.promotion_mutation_with_accounting(
                    transaction, requested, LazyPromotionMutation::Metadata(source_file_metadata(node.metadata)),
                    work, cancellation.clone(),
                ).await?;
                if node.kind != SourceNodeKind::Directory {
                    let identity = work.with_kernel(|accounting, _| self.source_file_id(&node.file_identity, accounting))?;
                    self.promotion_mutation_with_accounting(
                        transaction, requested, LazyPromotionMutation::Identity(identity), work, cancellation.clone(),
                    ).await?;
                }
                Ok(())
        }).await
    }

    async fn populate_regular_source_node(
        &self, transaction: &mut crate::Transaction<A, O>, requested: &str,
        pinned: &(SourceReference, SourceNode), maximum_bytes: u64, work: &LazyTreapWork<'_>,
        cancellation: &CancellationToken,
        source_used: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<(), LazyWorkspaceError> {
        in_heap_lazy::<_, LazyWorkspaceError, _>(work, move || async move {
                let length = pinned.1.logical_bytes.ok_or(LazyWorkspaceError::NotRegularFile)?;
                if length > maximum_bytes { return Err(LazyWorkspaceError::TooLarge); }
                self.promotion_mutation_with_accounting(
                    transaction, requested, LazyPromotionMutation::File(acyclic_stream::OwnedBytes::new()), work, cancellation.clone(),
                ).await?;
                let mut offset = 0_u64;
                while offset < length {
                    let requested_bytes = (length - offset).min(1024 * 1024);
                    if let Some(used) = source_used {
                        used.store(true, std::sync::atomic::Ordering::Release);
                    }
                    let chunk = self.promotion_source_chunk_with_accounting(
                        requested, pinned, Some(offset), requested_bytes, work, cancellation,
                    ).await?;
                    if chunk.bytes.is_empty() {
                        return Err(LazyWorkspaceError::Demand(DemandError::StaleVersion));
                    }
                    let chunk_length = crate::foundation::usize_to_u64(chunk.bytes.len());
                    self.promotion_mutation_with_accounting(
                        transaction, requested, LazyPromotionMutation::Write(offset, chunk), work, cancellation.clone(),
                    ).await?;
                    offset = offset.checked_add(chunk_length).ok_or(LazyWorkspaceError::TooLarge)?;
                }
                Ok(())
        }).await
    }

    async fn promotion_transaction_with_accounting(
        &self, key: IdempotencyKey, work: &LazyTreapWork<'_>, cancellation: &CancellationToken,
    ) -> Result<crate::Transaction<A, O>, LazyWorkspaceError> {
        in_heap_lazy::<_, LazyWorkspaceError, _>(work, move || async move {
            lazy_dispatch!{work, work.accept(
                    self.workspace.begin_transaction(key, work.remaining()?, cancellation)
                        .await.map_err(measured_workspace_error),
                ),
                |original| {
                    self.workspace.begin_transaction(
                        key, original, cancellation,
                    ).await.map_err(|error| native_workspace_error_pair((error, None)))
                }
            }
        }).await
    }

    async fn promotion_mutation_with_accounting(
        &self, transaction: &mut crate::Transaction<A, O>, path: &str,
        operation: LazyPromotionMutation<'_>, work: &LazyTreapWork<'_>, cancellation: CancellationToken,
    ) -> Result<(), LazyWorkspaceError> {
        in_heap_lazy::<_, LazyWorkspaceError, _>(work, move || async move {
            // Resolve the ordinary allowance before consuming mutation inputs.
            let budget = if matches!(work.meter, LazyTreapMeter::Standalone { .. }) {
                Some(work.remaining()?)
            } else { None };
            macro_rules! apply {
                ($operation:ident, $($argument:expr),+ $(,)?) => {
                    lazy_dispatch!{work, work.accept(
                            transaction.$operation($($argument,)+ budget.expect("ordinary allowance"), &cancellation)
                                .await.map_err(measured_workspace_error),
                        ),
                        |original| transaction.$operation($($argument,)+ original, &cancellation).await
                        .map_err(native_workspace_error)
                    }
                };
            }
            match operation {
                LazyPromotionMutation::Directory => apply!(create_directory, path),
                LazyPromotionMutation::Parents => apply!(create_dir_all, path),
                LazyPromotionMutation::File(bytes) => apply!(create_file, path, bytes, crate::kernel::FileMetadata::default()),
                LazyPromotionMutation::Special(kind, device) => apply!(create_special, path, kind, device),
                LazyPromotionMutation::Metadata(metadata) => apply!(set_metadata, path, metadata),
                LazyPromotionMutation::Identity(identity) => apply!(preserve_file_identity, path, identity),
                LazyPromotionMutation::Restore(record) => apply!(restore_record, path, record),
                LazyPromotionMutation::HardLink(source) => apply!(hard_link, source, path),
                LazyPromotionMutation::Link(mut chunk) => {
                    let applied = apply!(create_symbolic_link, path, std::mem::take(&mut chunk.bytes));
                    drop(chunk);
                    applied
                }
                LazyPromotionMutation::Write(offset, mut chunk) => {
                    let applied = apply!(write_range, path, offset, std::mem::take(&mut chunk.bytes));
                    drop(chunk);
                    applied
                }
            }
        }).await
    }

    async fn commit_promotion_with_accounting(
        &self, transaction: &mut crate::Transaction<A, O>, permit: crate::PublicationPermit,
        work: &LazyTreapWork<'_>, cancellation: &CancellationToken,
    ) -> Result<TransactionCommit<A, O>, LazyWorkspaceError> {
        in_heap_lazy::<_, LazyWorkspaceError, _>(work, move || async move {
            lazy_dispatch!{work, work.accept(
                    transaction.commit(None, permit, work.remaining()?, cancellation, |_| async { Ok::<_, crate::OperationFailure<WorkspaceError>>(OperationReceipt { value: (), work: WorkCounters::UNCHARGED }) })
                        .await.map_err(measured_workspace_error),
                ),
                |original| {
                    transaction.commit_with_native_work(
                        None, permit, original, cancellation, &mut |_: &crate::NativeMaterializationWork| std::future::ready(Ok::<(), WorkspaceError>(())),
                    ).await.map_err(native_workspace_error)
                }
            }
        }).await
    }

    async fn promotion_source_chunk_with_accounting(
        &self, requested: &str, pinned: &(SourceReference, SourceNode), offset: Option<u64>, maximum_bytes: u64,
        work: &LazyTreapWork<'_>, cancellation: &CancellationToken,
    ) -> Result<crate::kernel::OwnedSourceChunk, LazyWorkspaceError> {
        in_heap_lazy::<_, LazyWorkspaceError, _>(work, move || async move {
            lazy_dispatch!{work, work.accept(self.source_request_measured(
                    requested, work.remaining()?, cancellation, |path, budget| async move {
                        match offset {
                            Some(offset) => self.source.read_range(
                                pinned.0, &path, pinned.1.version, offset, maximum_bytes, budget, cancellation,
                            ).await,
                            None => self.source.read_link(pinned.0, &path, pinned.1.version, u64::MAX, budget, cancellation).await,
                        }
                    },
                ).await),
                |original| {
                    let read = match offset {
                        Some(offset) => self.read_source_range(
                            requested, pinned.0, pinned.1.version, offset, maximum_bytes, original, cancellation,
                        ).await,
                        None => self.read_source_link(
                            requested, pinned.0, pinned.1.version, maximum_bytes, original, cancellation,
                        ).await,
                    };
                    match read {
                        Ok(chunk) => Ok(chunk),
                        Err((error, metadata_custody)) => { drop(metadata_custody); Err(error) }
                    }
                }
            }
        }).await
    }

    async fn validate_promotion_source_with_accounting(
        &self, snapshot: Option<LazySnapshotRef>, source: Option<SourceReference>,
        work: &LazyTreapWork<'_>, cancellation: &CancellationToken,
    ) -> Result<(), LazyWorkspaceError> {
        lazy_dispatch!{work, work.accept(self.validate_promotion_source(
                snapshot, source, work.remaining()?, cancellation,
            ).await),
            |original| {
                original.admit(WorkCounters::UNCHARGED)?;
                cancellation.check().map_err(|_| LazyWorkspaceError::NativeWork(
                    crate::NativeTreeMaterializationError::Cancelled))?;
                if let Some(snapshot) = snapshot {
                    self.readonly_snapshot_state(snapshot, source.is_some(),
                        &mut crate::kernel::allocation::KernelAccounting::native(original, cancellation))?;
                    return Ok(());
                }
                let Some(source) = source else { return Ok(()); };
                if self.source.reference() != source { return Err(LazyWorkspaceError::StaleSource); }
                let state = self.state(&LazyTreapWork::native_with_work(original, cancellation)?).await?;
                original.admit(WorkCounters::UNCHARGED)?;
                cancellation.check().map_err(|_| LazyWorkspaceError::NativeWork(
                    crate::NativeTreeMaterializationError::Cancelled))?;
                if state.as_state().source != source || self.source.reference() != source {
                    return Err(LazyWorkspaceError::StaleSource);
                }
                Ok(())
            }
        }
    }

    async fn promotion_authored_record_with_accounting(
        &self, snapshot: LazySnapshotRef, path: &str, work: &LazyTreapWork<'_>,
        cancellation: &CancellationToken,
    ) -> Result<FileRecord, LazyWorkspaceError> {
        let scope = TreapAllocationScope::new(work);
        let namespace = work.namespace_owned(path, self.workspace.profile(), self.workspace.limits())?;
        let result = lazy_dispatch!{work, {
                let generation = work.accept(self.workspace.generation(
                    snapshot.authored_generation, work.remaining()?, cancellation,
                ).await.map_err(measured_workspace_error))?;
                let records = work.accept(generation.lookup_paths(
                    std::slice::from_ref(&namespace), work.remaining()?, cancellation,
                ).await.map_err(|failure| lazy_operation_failure(failure,
                    |error| workspace_error(error.into()))))?;
                let bytes = crate::foundation::usize_to_u64(records.capacity())
                    .checked_mul(std::mem::size_of::<Option<FileRecord>>() as u64).ok_or_else(counter_overflow)?;
                work.adopt(bytes)?;
                let record = records.first().copied().flatten().ok_or(LazyWorkspaceError::NotFound);
                drop(records);
                work.release(bytes)?;
                record
            },
            |original| {
                let reader = self.workspace.readonly_pinned_reader_with_native_work(
                    snapshot.authored_generation, original, cancellation,
                ).await.map_err(native_workspace_error)?;
                reader.lookup_record_with_native_work(&namespace, original, cancellation).await
                    .map_err(native_filesystem_error)?.ok_or(LazyWorkspaceError::NotFound)
            }
        };
        drop(namespace);
        drop(scope);
        result
    }

    async fn stage_regular_source_content(
        &self,
        transaction: &crate::Transaction<A, O>,
        requested: &str,
        pinned: &(SourceReference, SourceNode),
        budget: WorkBudget,
        cancellation: &CancellationToken,
        snapshot: Option<LazySnapshotRef>,
        source_used: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<OperationReceipt<crate::StagedContent>, LazyWorkspaceError> {
        let mut work = WorkCounters::default();
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let result = async {
            let length = pinned.1.logical_bytes.ok_or(LazyWorkspaceError::NotRegularFile)?;
            let path = NamespacePath::parse_in_profile_with_ledger(
                requested, self.workspace.profile(), self.workspace.limits(),
                &mut allocations, &mut work, budget, cancellation,
            ).map_err(|error| LazyWorkspaceError::Workspace(error.to_string()))?;
            let live_bytes = allocations.live_bytes();
            let mut source = DemandBlobSource {
                provider: self.source.as_ref(),
                source: pinned.0,
                path,
                version: pinned.1.version,
                offset: 0,
                length,
                work: None,
                failure: None,
                source_used,
                expected_source: snapshot.map(|snapshot| snapshot.source),
                #[cfg(not(target_arch = "wasm32"))]
                failure_storage: None,
            };
            let mut remaining = remaining_work(work, budget, 0)?;
            remaining.peak_allocation_bytes = remaining.peak_allocation_bytes
                .checked_sub(live_bytes).ok_or_else(counter_overflow)?;
            let staged = transaction.stage_content(
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

    #[cfg(not(target_arch = "wasm32"))]
    async fn stage_regular_source_content_with_native_work(
        &self,
        transaction: &crate::Transaction<A, O>,
        requested: &str,
        pinned: &(SourceReference, SourceNode),
        original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
        snapshot: Option<LazySnapshotRef>,
        source_used: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<crate::StagedContent, LazyWorkspaceError> {
        let work = LazyTreapWork::native_with_work(original, cancellation)?;
        in_heap_lazy::<_, LazyWorkspaceError, _>(&work, || async {
            let length = pinned.1.logical_bytes.ok_or(LazyWorkspaceError::NotRegularFile)?;
            let scope = TreapAllocationScope::new(&work);
            let path = work.namespace_owned(requested, self.workspace.profile(), self.workspace.limits())?;
            let mut source = DemandBlobSource {
                provider: self.source.as_ref(), source: pinned.0, path,
                version: pinned.1.version, offset: 0, length,
                work: None, failure: None, source_used,
                expected_source: snapshot.map(|snapshot| snapshot.source),
                failure_storage: None,
            };
            let staged = transaction.stage_content(
                &mut source, length.max(1), original, cancellation,
            ).await.map_err(native_workspace_error);
            drop(source);
            drop(scope);
            let value = staged?;
            if value.logical_bytes() != length {
                return Err(LazyWorkspaceError::Demand(DemandError::StaleVersion));
            }
            Ok(value)
        }).await
    }

    async fn commit_exactify_batch_guarded_with_accounting<F, Fut>(
        &self,
        mut transaction: crate::Transaction<A, O>,
        permit: crate::PublicationPermit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        before_publication: &mut F,
    ) -> Result<OperationReceipt<crate::Generation<A, O>>, LazyWorkspaceError>
    where
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::FsResult<()>> + ProviderTask,
    {
        let mut guard = |allowance| {
            let outcome = before_publication(allowance);
            async move {
                outcome.await.map_err(|failure| failure.map_error(WorkspaceError::from))
            }
        };
        let receipt = transaction.commit(None, permit, budget, cancellation, &mut guard).await.map_err(measured_workspace_error)?;
        Self::exactify_batch_commit_result(receipt.value, receipt.work, true)
    }

    fn exactify_batch_commit_result(
        value: TransactionCommit<A, O>, work: WorkCounters, portable: bool,
    ) -> Result<OperationReceipt<crate::Generation<A, O>>, LazyWorkspaceError> {
        match value {
            TransactionCommit::Committed(generation)
            | TransactionCommit::AlreadyCommitted(generation) => Ok(OperationReceipt {
                value: generation, work,
            }),
            TransactionCommit::Conflict { .. }
            | TransactionCommit::Fenced
            | TransactionCommit::IdempotencyConflict => {
                let error = LazyWorkspaceError::Concurrent;
                Err(if portable { error.with_prior_work(work) } else { error })
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn flush_exactify_source_batch_with_accounting(
        &self,
        transaction: &mut crate::Transaction<A, O>,
        pending: &mut Vec<AuthoredMutation>,
        pending_bytes: &mut u64,
        retained_bytes: &mut u64,
        work: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        #[cfg(not(target_arch = "wasm32"))]
        original: Option<&crate::NativeMaterializationWork>,
        #[cfg(not(target_arch = "wasm32"))]
        pending_allocation: Option<&mut crate::NativeAllocation>,
    ) -> Result<WorkCounters, LazyWorkspaceError> {
        if pending.is_empty() {
            return Ok(work);
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = original {
            let frame = LazyTreapWork::native_with_work(original, cancellation)?;
            in_heap_lazy::<_, LazyWorkspaceError, _>(&frame, || async {
                let allocation = pending_allocation.ok_or(LazyWorkspaceError::NativeLedgerMismatch)?;
                let paid = allocation.split_off(allocation.bytes())?;
                let operations = std::mem::take(pending);
                *pending_bytes = 0;
                transaction.apply_authored_bulk_with_native_work(
                    operations, paid, original, cancellation,
                ).await.map_err(native_workspace_error)
            }).await?;
            return Ok(work);
        }
        let operations = std::mem::take(pending);
        *retained_bytes = retained_bytes.checked_sub(*pending_bytes)
            .ok_or_else(counter_overflow)?;
        *pending_bytes = 0;
        let applied = transaction.apply_authored_bulk_measured(
            operations, remaining_work(work, budget, *retained_bytes)?, cancellation,
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
        work.measured(result, |error: LazyWorkspaceError, spent| error.with_prior_work(spent))
    }

    #[allow(clippy::too_many_arguments)]
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
        let parent = LazyTreapWork::new_with_prior(budget, cancellation, work);
        let result = async {
            work.verify(budget)?;
            let lookup_bytes = match &lookup.lookup {
                LazyLookup::Authored { path, .. } => crate::foundation::usize_to_u64(path.capacity()),
                _ => 0,
            };
            parent.adopt(lookup_bytes)?;
            self.promote_resolved_with_permit_with_accounting(
                requested, &lookup, maximum_bytes, idempotency_key, permit, &parent, cancellation,
            ).await
        }.await;
        parent.measured(result, |error: LazyWorkspaceError, spent| error.with_prior_work(spent))
    }

    async fn promote_resolved_with_permit_with_accounting(
        &self, requested: &str, lookup: &ResolvedLazyLookup, maximum_bytes: u64,
        idempotency_key: IdempotencyKey, permit: crate::PublicationPermit,
        work: &LazyTreapWork<'_>, cancellation: &CancellationToken,
    ) -> Result<TransactionCommit<A, O>, LazyWorkspaceError> {
        in_heap_lazy::<_, LazyWorkspaceError, _>(work, move || async move {
            self.validate_promotion_source_with_accounting(None, lookup.source, work, cancellation).await?;
            let mut transaction = self.promotion_transaction_with_accounting(idempotency_key, work, cancellation).await?;
            if let Some(directory) = parent_path(requested) {
                self.promotion_mutation_with_accounting(
                    &mut transaction, directory, LazyPromotionMutation::Parents, work, cancellation.clone(),
                ).await?;
            }
            let applied = self.apply_resolved_promotion(&mut transaction, requested, lookup, maximum_bytes, None, work, None).await?;
            self.validate_promotion_source_with_accounting(None, lookup.source, work, cancellation).await?;
            if !applied {
                return self.promotion_head(work).await
                    .map(TransactionCommit::AlreadyCommitted);
            }
            self.commit_promotion_with_accounting(&mut transaction, permit, work, cancellation).await
        }).await
    }

    async fn validate_promotion_source(
        &self, snapshot: Option<LazySnapshotRef>, source: Option<SourceReference>,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<()>, LazyWorkspaceError> {
        if let Some(snapshot) = snapshot {
            let mut work = WorkCounters::default();
            return self.readonly_snapshot_state(snapshot, source.is_some(),
                &mut crate::kernel::allocation::KernelAccounting::borrowed_standalone(
                    &mut crate::kernel::allocation::AllocationLedger::default(), &mut work, budget, cancellation))
                .map(|_| OperationReceipt { value: (), work })
                .map_err(|error| error.with_prior_work(work));
        }
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        let Some(source) = source else {
            return Ok(OperationReceipt { value: (), work: WorkCounters::default() });
        };
        if self.source.reference() != source { return Err(LazyWorkspaceError::StaleSource); }
        let state = { let state_work = LazyTreapWork::new(budget, cancellation); let result = self.state(&state_work).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("standalone state keeps its accounting owner") }); state_work.measured(result, |error, spent| error.with_prior_work(spent)) }?;
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled.with_prior_work(state.work))?;
        if state.value.source != source || self.source.reference() != source {
            return Err(LazyWorkspaceError::StaleSource.with_prior_work(state.work));
        }
        Ok(OperationReceipt { value: (), work: state.work })
    }

    async fn apply_resolved_promotion(
        &self, transaction: &mut crate::Transaction<A, O>, requested: &str,
        lookup: &ResolvedLazyLookup, maximum_bytes: u64, snapshot: Option<LazySnapshotRef>,
        work: &LazyTreapWork<'_>, source_used: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<bool, LazyWorkspaceError> {
        let cancellation = work.cancellation;
        in_heap_lazy::<_, LazyWorkspaceError, _>(work, move || async move {
            let ResolvedLazyLookup { lookup, source } = lookup;
            let pinned = match lookup {
                LazyLookup::Source(node) => (source.ok_or(LazyWorkspaceError::Concurrent)?, *node),
                LazyLookup::Authored { path: authored_path, .. } => {
                    if let Some(snapshot) = snapshot {
                        let record = self.promotion_authored_record_with_accounting(
                            snapshot, authored_path, work, cancellation,
                        ).await?;
                        self.promotion_mutation_with_accounting(
                            transaction, requested, LazyPromotionMutation::Restore(record), work, cancellation.clone(),
                        ).await?;
                        return Ok(true);
                    }
                    if authored_path == requested { return Ok(false); }
                    self.promotion_mutation_with_accounting(
                        transaction, requested, LazyPromotionMutation::HardLink(authored_path), work, cancellation.clone(),
                    ).await?;
                    return Ok(true);
                }
                LazyLookup::Shadow { record, .. } => {
                    self.promotion_mutation_with_accounting(
                        transaction, requested, LazyPromotionMutation::Restore(*record), work, cancellation.clone(),
                    ).await?;
                    return Ok(true);
                }
            };
            let require_source = snapshot.is_none()
                || (pinned.1.kind == SourceNodeKind::RegularFile && pinned.1.logical_bytes.unwrap_or(0) != 0)
                || pinned.1.kind == SourceNodeKind::SymbolicLink;
            let checked_source = require_source.then_some(pinned.0);
            self.validate_promotion_source_with_accounting(snapshot, checked_source, work, cancellation).await?;
            self.populate_source_node_with_accounting(
                transaction, requested, &pinned, maximum_bytes, work, cancellation, source_used,
            ).await?;
            self.validate_promotion_source_with_accounting(snapshot, checked_source, work, cancellation).await?;
            Ok(true)
        }).await
    }

    /// Promotes the exact identity and accepts only a durable successful outcome.
    pub async fn promote_exact(
        &self, path: &str, expected_source: FileId, maximum_bytes: u64,
        idempotency_key: IdempotencyKey, work: &LazyTreapWork<'_>,
    ) -> Result<crate::Generation<A, O>, LazyWorkspaceError> {
        let cancellation = work.cancellation;
        in_heap_lazy::<_, LazyWorkspaceError, _>(work, move || async move {
            let scope = TreapAllocationScope::new(work);
            let requested = work.canonical_path_owned(path, self.workspace.limits())?;
            let lookup = self.promotion_lookup(&requested, work).await?;
            let resolved = lookup.as_state();
            let actual = match &resolved.lookup {
                LazyLookup::Source(node) => work.with_kernel(|accounting, _| self.source_file_id(&node.file_identity, accounting))?,
                LazyLookup::Authored { stat, .. } => stat.file_id,
                LazyLookup::Shadow { record, .. } => record.file_id,
            };
            if actual != expected_source { return Err(LazyWorkspaceError::StaleIdentity); }
            if matches!(&resolved.lookup, LazyLookup::Authored { path, .. } if path == &requested) {
                return self.promotion_head(work).await;
            }
            let committed = self.promote_resolved_with_permit_with_accounting(
                &requested, resolved, maximum_bytes, idempotency_key,
                crate::PublicationPermit::Unrestricted, work, cancellation,
            ).await?;
            drop(lookup);
            drop(requested);
            drop(scope);
            match committed {
                TransactionCommit::Committed(generation)
                | TransactionCommit::AlreadyCommitted(generation) => Ok(generation),
                TransactionCommit::Conflict { .. } | TransactionCommit::Fenced =>
                    Err(LazyWorkspaceError::StaleIdentity),
                TransactionCommit::IdempotencyConflict => Err(LazyWorkspaceError::Concurrent),
            }
        }).await
    }

    async fn promotion_lookup(
        &self, path: &str, work: &LazyTreapWork<'_>,
    ) -> Result<LazyReadonlyNode<ResolvedLazyLookup>, LazyWorkspaceError> {
        let cancellation = work.cancellation;
        lazy_dispatch!{work, {
                let lookup = work.accept(self.lookup_measured(path, work.remaining()?, cancellation).await)?;
                let bytes = match &lookup.lookup {
                    LazyLookup::Authored { path, .. } => crate::foundation::usize_to_u64(path.capacity()),
                    _ => 0,
                };
                work.adopt(bytes)?;
                Ok(LazyReadonlyNode::Portable(lookup))
            },
            |original| {
                let (lookup, allocation) = self.lookup_resolved_with_native_work(path, original, cancellation).await?;
                Ok(LazyReadonlyNode::Native(NativeOwnedCoreState::from_parts(lookup, allocation)))
            }
        }
    }

    async fn promotion_head(
        &self, work: &LazyTreapWork<'_>,
    ) -> Result<crate::Generation<A, O>, LazyWorkspaceError> {
        let cancellation = work.cancellation;
        lazy_dispatch!{work, work.accept(
                self.workspace.head(work.remaining()?, cancellation).await.map_err(measured_workspace_error),
            ),
            |original| {
                self.workspace.head(original, cancellation).await.map_err(|error| native_workspace_error_pair((error, None)))
            }
        }
    }

    /// Stages one live or frozen source identity in the caller's checkout.
    pub(crate) async fn stage_into_checkout(
        &self, checkout: &mut crate::Checkout<A, O>, snapshot: Option<LazySnapshotRef>,
        path: &str, expected_source: FileId, maximum_bytes: u64,
        work: &LazyTreapWork<'_>,
    ) -> Result<bool, LazyWorkspaceError> {
        let cancellation = work.cancellation;
        in_heap_lazy::<_, LazyWorkspaceError, _>(work, move || async move {
            let _scope = TreapAllocationScope::new(work);
            let source_used = std::sync::atomic::AtomicBool::new(false);
                let requested = work.canonical_path_owned(path, self.workspace.limits())?;
                #[cfg(not(target_arch = "wasm32"))]
                let frozen = match &work.meter {
                    LazyTreapMeter::NativeOriginal { original, .. } => {
                        Some(match snapshot {
                            Some(snapshot) => self.inspect_at_snapshot_with_work(
                                snapshot, &requested, original, cancellation, &source_used,
                            ).await.map_err(|(error, custody)| { drop(custody); error })?,
                            None => self.lookup_frozen(
                                &requested, original, cancellation,
                            ).await?,
                        })
                    }
                    LazyTreapMeter::Standalone { .. } => None,
                };
                let mut portable_lookup = if matches!(&work.meter, LazyTreapMeter::Standalone { .. }) {
                    Some(if let Some(snapshot) = snapshot {
                    let state = work.with_kernel(|accounting, _| self.readonly_snapshot_state(snapshot, false, accounting))?;
                    work.accept(self.inspect_with(&requested, InspectBasis {
                        state: Some(&state),
                        authored_generation: Some(snapshot.authored_generation),
                        retain_pinned_observation: true,
                        source_used: Some(&source_used),
                        ..InspectBasis::default()
                    }, work.remaining()?, cancellation).await)?
                } else {
                    work.accept(self.lookup_measured(&requested, work.remaining()?, cancellation).await)?
                    })
                } else { None };
                let portable_accounting = portable_lookup.is_some();
                #[cfg(not(target_arch = "wasm32"))]
                let lookup = portable_lookup.as_ref().or_else(|| frozen.as_ref().map(|value| &value.0))
                    .ok_or(LazyWorkspaceError::Concurrent)?;
                #[cfg(target_arch = "wasm32")]
                let lookup = portable_lookup.as_ref().ok_or(LazyWorkspaceError::Concurrent)?;
                let pinned_source = lookup.source;
                let lookup_bytes = if portable_lookup.is_some() {
                    match &lookup.lookup {
                        LazyLookup::Authored { path, .. } => crate::foundation::usize_to_u64(path.capacity()),
                        _ => 0,
                    }
                } else { 0 };
                if portable_accounting { work.adopt(lookup_bytes)?; }
                let requires_data = matches!(&lookup.lookup,
                    LazyLookup::Source(node) if node.kind == SourceNodeKind::SymbolicLink
                        || (node.kind == SourceNodeKind::RegularFile && node.logical_bytes.unwrap_or(0) != 0));
                let check_source = if snapshot.is_none() || requires_data
                    || source_used.load(std::sync::atomic::Ordering::Acquire) { pinned_source } else { None };
                self.validate_promotion_source_with_accounting(
                    snapshot, check_source, work, cancellation,
                ).await?;
                let actual = match &lookup.lookup {
                    LazyLookup::Source(node) => {
                        if portable_accounting {
                            let mut spent = WorkCounters::default();
                            let result = self.source_file_id(&node.file_identity,
                                &mut crate::kernel::allocation::KernelAccounting::borrowed_standalone(
                                    &mut crate::kernel::allocation::AllocationLedger::default(), &mut spent, work.remaining()?, cancellation));
                            work.accept(result.map(|value| OperationReceipt { value, work: spent }))?
                        } else {
                            work.with_kernel(|accounting, _| self.source_file_id(&node.file_identity, accounting))?
                        }
                    }
                    LazyLookup::Authored { stat, .. } => stat.file_id,
                    LazyLookup::Shadow { record, .. } => record.file_id,
                };
                if actual != expected_source { return Err(LazyWorkspaceError::StaleIdentity); }
                let candidate_path = work.namespace_owned(
                    &requested, self.workspace.profile(), self.workspace.limits(),
                )?;
                let candidate = selected_checkout_query!(checkout, &candidate_path, work, cancellation, lookup_no_follow, lookup_no_follow)?;
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
                    lazy_dispatch!{work, {
                            let found = checkout.read_file_record_by_id(expected_source, work.remaining()?, cancellation).await;
                            work.accept(match found {
                                Ok(receipt) => Ok(OperationReceipt { value: Some(receipt.value), work: receipt.work }),
                                Err(failure) if matches!(failure.error, crate::FsError::NotFound) =>
                                    Ok(OperationReceipt { value: None, work: *failure.work }),
                                Err(failure) => Err(lazy_operation_failure(failure, |error| workspace_error(error.into()))),
                            })?
                        },
                        |original| {
                            match checkout.read_file_record_by_id(
                                expected_source, original, cancellation,
                            ).await {
                                Ok(record) => Some(record),
                                Err(crate::FsError::NotFound) => None,
                                Err(error) => return Err(native_filesystem_error(error)),
                            }
                        }
                    }
                } else { None };
                let candidate = lazy_dispatch!{work, work.accept(
                        checkout.private_candidate(work.remaining()?, cancellation)
                            .map_err(|failure| lazy_operation_failure(failure, |error| workspace_error(error.into()))),
                    )?,
                    |original| {
                        checkout.private_candidate(
                            original, cancellation,
                        ).map_err(native_filesystem_error)?
                    }
                };
                let candidate_bytes = if portable_lookup.is_some() { candidate.logical_owned_bytes() } else { 0 };
                if portable_accounting { work.adopt(candidate_bytes)?; }
                let mut transaction = lazy_dispatch!{work, crate::Transaction::for_checkout_candidate(&self.workspace, candidate),
                    |original| crate::Transaction::for_checkout_candidate_with_native_work(
                        &self.workspace, candidate, original, cancellation,
                    ).map_err(native_workspace_error)?
                };
                if let Some(parent) = parent_path(&requested) {
                    self.promotion_mutation_with_accounting(
                        &mut transaction, parent, LazyPromotionMutation::Parents, work, cancellation.clone(),
                    ).await?;
                }
                let changed = if let Some(record) = existing_identity {
                    self.promotion_mutation_with_accounting(
                        &mut transaction, &requested, LazyPromotionMutation::Restore(record), work, cancellation.clone(),
                    ).await?;
                    true
                } else {
                    let scope = portable_accounting.then(|| TreapAllocationScope::new(work));
                    let applied = self.apply_resolved_promotion(&mut transaction, &requested, lookup, maximum_bytes, snapshot, work, Some(&source_used)).await;
                    drop(scope);
                    if let Some(portable) = portable_lookup.take() {
                        drop(portable);
                        work.release(lookup_bytes)?;
                    }
                    applied?
                };
                self.validate_promotion_source_with_accounting(
                    snapshot, if snapshot.is_none() || source_used.load(std::sync::atomic::Ordering::Acquire) {
                        pinned_source.or(snapshot.map(|snapshot| snapshot.source))
                    } else { None }, work, cancellation,
                ).await?;
                if changed {
                    let candidate = transaction.into_checkout_candidate();
                    let actual_bytes = if portable_accounting { candidate.logical_owned_bytes() } else { 0 };
                    if portable_accounting {
                        work.release(candidate_bytes)?;
                        work.adopt(actual_bytes)?;
                    }
                    *checkout = candidate;
                    if portable_accounting { work.release(actual_bytes)?; }
                } else {
                    drop(transaction);
                    if portable_accounting { work.release(candidate_bytes)?; }
                }
                Ok(changed)
        }).await
    }

    /// Lists an unpublished mounted checkout with the source overlay.
    #[cfg(feature = "native-mount")]
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn list_directory_in_checkout(
        &self,
        checkout: &mut crate::Checkout<A, O>,
        path: &str,
        cursor: Option<LazyDirectoryCursor>,
        maximum_entries: u32,
        mounted_mask: Option<(&[String], bool)>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<OperationReceipt<(LazyDirectoryPage, LazyWorkspaceState)>, LazyWorkspaceError> {
        #[cfg(not(target_arch = "wasm32"))]
        let cursor = cursor.map(|mut cursor| { cursor.mark_input_borrowed(); cursor });
        in_heap_measured(move |budget| async move {
            let state = { let state_work = LazyTreapWork::new(budget, cancellation); let result = self.state(&state_work).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("standalone state keeps its accounting owner") }); state_work.measured(result, |error, spent| error.with_prior_work(spent)) }
                .map_err(LazyWorkspaceError::into_measured_failure)?;
            self.list_directory_at(
                &state.value, state.work, path, cursor, maximum_entries, budget,
                cancellation, Some(checkout), mounted_mask, None, None, None,
            ).await.map(|page| OperationReceipt { value: (page.value, state.value), work: page.work })
                .map_err(LazyWorkspaceError::into_measured_failure)
        }, budget, cancellation, LazyWorkspaceError::Cancelled, None).await
    }

    async fn directory_tombstoned(
        &self, root: LazyOverlayId, path: &str,
        projection: &mut DirectoryProjectionWork<'_>, cancellation: &CancellationToken,
    ) -> Result<bool, LazyWorkspaceError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = projection.native_original() {
            return self.tombstoned(root, path,
                &mut LazyReadonlyAccounting::Native { original, cancellation }).await;
        }
        let mut accounting = LazyReadonlyAccounting::Portable {
            work: WorkCounters::UNCHARGED, budget: projection.remaining()?, cancellation,
        };
        let result = self.tombstoned(root, path, &mut accounting).await;
        projection.accept(accounting.finish_measured(result))
    }

    async fn directory_has_overlay_beneath(
        &self, root: LazyOverlayId, path: &str,
        projection: &mut DirectoryProjectionWork<'_>, cancellation: &CancellationToken,
    ) -> Result<bool, LazyWorkspaceError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = projection.native_original() {
            return self.overlay_has_beneath(root, path,
                &mut LazyReadonlyAccounting::Native { original, cancellation }).await;
        }
        let mut accounting = LazyReadonlyAccounting::Portable {
            work: WorkCounters::UNCHARGED, budget: projection.remaining()?, cancellation,
        };
        let result = self.overlay_has_beneath(root, path, &mut accounting).await;
        projection.accept(accounting.finish_measured(result))
    }

    async fn directory_overlay_fact(
        &self, root: LazyOverlayId, path: &str,
        projection: &mut DirectoryProjectionWork<'_>, cancellation: &CancellationToken,
    ) -> Result<Option<LazyOverlayChange>, LazyWorkspaceError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = projection.native_original() {
            let mut accounting = LazyReadonlyAccounting::Native { original, cancellation };
            accounting.validate_path(path, self.workspace.limits())?;
            return self.overlay_fact(root, path, &mut accounting).await;
        }
        PortablePath::validate(path, self.workspace.limits())
            .map_err(|error| LazyWorkspaceError::Workspace(error.to_string()))?;
        let mut accounting = LazyReadonlyAccounting::Portable {
            work: WorkCounters::UNCHARGED, budget: projection.remaining()?, cancellation,
        };
        let result = self.overlay_fact(root, path, &mut accounting).await;
        projection.accept(accounting.finish_measured(result))
    }

    async fn directory_mounted_record(
        &self, checkout: &mut crate::Checkout<A, O>, path: &NamespacePath,
        projection: &mut DirectoryProjectionWork<'_>, cancellation: &CancellationToken,
    ) -> Result<Option<FileRecord>, LazyWorkspaceError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = projection.native_original() {
            return checkout.lookup_no_follow(path, original, cancellation)
                .await.map(|lookup| lookup.record).map_err(native_filesystem_error);
        }
        let result = checkout.lookup_no_follow(path, projection.remaining()?, cancellation)
            .await.map_err(|failure| lazy_operation_failure(failure,
                |error| LazyWorkspaceError::Workspace(error.to_string())));
        projection.accept(result).map(|lookup| lookup.record)
    }

    #[allow(clippy::too_many_arguments)]
    async fn directory_authored_entries(
        &self, path: &str, directory: &NamespacePath, after: Option<&LogicalName>,
        maximum_entries: u32, mounted: Option<&mut crate::Checkout<A, O>>,
        generation: Option<&crate::Generation<A, O>>,
        #[cfg(not(target_arch = "wasm32"))]
        reader: Option<&crate::PinnedReader<A, O>>,
        projection: &mut DirectoryProjectionWork<'_>, cancellation: &CancellationToken,
    ) -> Result<(DirectoryAuthoredEntries, bool), LazyWorkspaceError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = projection.native_original() {
            let (entries, has_more, mut allocation) = if let Some(checkout) = mounted {
                let (page, allocation) = checkout.list_directory_with_native_work(
                    directory, after, maximum_entries, original, cancellation,
                ).await.map_err(native_filesystem_error)?;
                (DirectoryAuthoredEntries::Mounted(page.entries), page.has_more, allocation)
            } else {
                let reader = reader.ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?;
                let (page, allocation) = reader.list_directory_records_with_native_work(
                    directory, after, maximum_entries, original, cancellation,
                ).await.map_err(native_filesystem_error)?;
                (DirectoryAuthoredEntries::Original(page.entries), page.has_more, allocation)
            };
            if let Err(error) = projection.absorb(&mut allocation) {
                drop(entries);
                drop(allocation);
                return Err(error);
            }
            return Ok((entries, has_more));
        }
        if let Some(checkout) = mounted {
            let result = checkout.list_directory(directory, after, maximum_entries,
                projection.remaining()?, cancellation).await
                .map_err(|failure| lazy_operation_failure(failure, |error| match error {
                    crate::FsError::NotFound => LazyWorkspaceError::NotFound,
                    error => LazyWorkspaceError::Workspace(error.to_string()),
                }));
            let page = projection.accept(result)?;
            Ok((DirectoryAuthoredEntries::Mounted(page.entries), page.has_more))
        } else {
            let remaining = projection.remaining()?;
            let result = if let Some(generation) = generation {
                generation.list_directory(path, after, maximum_entries, remaining, cancellation).await
            } else {
                self.workspace.list_directory(path, after, maximum_entries, remaining, cancellation).await
            }.map_err(|failure| lazy_operation_failure(failure, workspace_error));
            let page = projection.accept(result)?;
            Ok((DirectoryAuthoredEntries::Workspace(page.entries), page.has_more))
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn directory_head_reader(
        &self, original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<crate::PinnedReader<A, O>, LazyWorkspaceError> {
        let checkout = self.workspace.checkout(
            crate::model::GenerationSelector::Head, crate::model::CheckoutMode::read_only_pinned(),
            original, cancellation,
        ).await.map_err(native_workspace_error)?;
        checkout.into_pinned_reader_with_native_work(original, cancellation)
            .map_err(native_filesystem_error)
    }

    /// A mounted replacement must not inherit entries from an unrelated source directory at the same path.
    #[allow(clippy::too_many_arguments)]
    async fn mounted_directory_replaces_source(
        &self,
        path: &str,
        directory: &NamespacePath,
        source: SourceReference,
        overlay: LazyOverlayId,
        checkout: &mut crate::Checkout<A, O>,
        projection: &mut DirectoryProjectionWork<'_>,
        cancellation: &CancellationToken,
    ) -> Result<MountedDirectory, LazyWorkspaceError> {
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        let (mut future, future_bytes) = in_heap_admitted(
            &mut *projection,
            |projection, bytes| projection.claim(bytes, u64::from(bytes != 0)),
            move |projection| async move {
            let candidate = self.directory_mounted_record(checkout, directory, projection, cancellation).await?;
            let tombstoned = self.directory_tombstoned(overlay, path, projection, cancellation).await?;
            let replaces_source = match candidate {
                None if tombstoned => return Err(LazyWorkspaceError::NotFound),
                Some(_) if tombstoned => true,
                Some(record) => {
                    let source = projection.source_lookup(self.source.as_ref(), source, directory, cancellation).await?;
                    match source {
                        None => true,
                        Some(node) if match projection {
                            DirectoryProjectionWork::Portable { work, budget, cancellation, .. } =>
                                self.source_file_id(&node.file_identity, &mut crate::kernel::allocation::KernelAccounting::borrowed_standalone(
                                    &mut crate::kernel::allocation::AllocationLedger::default(), work, *budget, cancellation))?,
                            #[cfg(not(target_arch = "wasm32"))]
                            DirectoryProjectionWork::Native { original, cancellation, .. } =>
                                self.source_file_id(&node.file_identity, &mut crate::kernel::allocation::KernelAccounting::native(original, cancellation))?,
                        } == record.file_id => false,
                        Some(_) => {
                            #[cfg(not(target_arch = "wasm32"))]
                            if let Some(original) = projection.native_original() {
                                let reader = self.directory_head_reader(original, cancellation).await?;
                                let committed = reader.lookup_record_with_native_work(directory, original, cancellation)
                                    .await.map_err(native_filesystem_error)?;
                                return Ok(MountedDirectory {
                                    replaces_source: committed.is_none_or(|committed| committed.file_id != record.file_id),
                                    authored: true,
                                });
                            }
                            let head = { let result = self.workspace.head(projection.remaining()?, cancellation)
                                    .await.map_err(measured_workspace_error); projection.accept(result) }?;
                            let committed = { let result = head.lookup_paths(std::slice::from_ref(directory), projection.remaining()?, cancellation)
                                    .await.map_err(|failure| lazy_operation_failure(failure, |error| LazyWorkspaceError::Workspace(error.to_string()))); projection.accept(result) }?;
                            committed.first().copied().flatten().is_none_or(|committed| committed.file_id != record.file_id)
                        }
                    }
                }
                None => false,
            };
            Ok(MountedDirectory { replaces_source, authored: candidate.is_some() })
            },
        )?;
        let result = future.as_mut().await;
        drop(future);
        projection.release(future_bytes)?;
        result
    }

    /// Lists one bounded page combining source entries and the authored overlay.
    pub async fn list_directory(
        &self,
        path: &str,
        cursor: Option<LazyDirectoryCursor>,
        maximum_entries: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<LazyDirectoryPage>, LazyWorkspaceError> {
        let state = { let state_work = LazyTreapWork::new(budget, cancellation); let result = self.state(&state_work).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("standalone state keeps its accounting owner") }); state_work.measured(result, |error, spent| error.with_prior_work(spent)) }?;
        self.list_directory_at(
            &state.value,
            state.work,
            path,
            cursor,
            maximum_entries,
            budget,
            cancellation,
            None,
            None,
            None,
            None,
            None,
        )
        .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn list_directory_at_snapshot_observed_with_work<'a>(
        &'a self, snapshot: LazySnapshotRef, path: &str, cursor: Option<LazyDirectoryCursor>,
        maximum_entries: u32, original: &'a crate::NativeMaterializationWork,
        cancellation: &'a CancellationToken, source_used: &'a std::sync::atomic::AtomicBool,
    ) -> impl std::future::Future<Output = Result<(LazyDirectoryPage, crate::NativeAllocation),
        (LazyWorkspaceError, Option<crate::NativeAllocation>)>>
        + acyclic_stream::ProviderTask + 'a + use<'a, A, O, D, S>
    {
        let path = clone_lazy_string_with_accounting(
            path, &mut crate::kernel::allocation::KernelAccounting::native(original, cancellation),
            cancellation,
        ).map_err(|error| (error, None));
        async move {
            let path = path?;
            let result = async {
                let source_required = cursor.as_ref().is_none_or(|cursor|
                    matches!(&cursor.phase, LazyDirectoryPhase::Source(_)));
                let state = self.readonly_snapshot_state(
                    snapshot, source_required, &mut crate::kernel::allocation::KernelAccounting::native(original, cancellation),
                )?;
                let reader = self.workspace.readonly_pinned_reader_with_native_work(
                    snapshot.authored_generation, original, cancellation,
                ).await.map_err(native_workspace_error)?;
                let mut accounting = LazyReadonlyAccounting::Native { original, cancellation };
                let overlay = self.load_overlay_with_readonly_accounting(state.overlay, &mut accounting).await?;
                let shadow = self.load_shadow_with_readonly_accounting(state.shadows, &mut accounting).await?;
                drop(overlay);
                drop(shadow);
                let used = std::sync::atomic::AtomicBool::new(false);
                let cursor = cursor.map(|mut cursor| { cursor.mark_input_borrowed(); cursor });
                let projection = DirectoryProjectionWork::native(original, cancellation)?;
                let result = self.list_directory_with_projection(
                    &state, projection, &path.0, cursor, maximum_entries, cancellation,
                    None, None, None, Some(snapshot.id), Some(&used), Some(&reader),
                ).await;
                if used.load(std::sync::atomic::Ordering::Acquire) {
                    source_used.store(true, std::sync::atomic::Ordering::Release);
                }
                let page = result?;
                let live = self.source.reference();
                let conflict = if live.identity != snapshot.source.identity {
                    Some(LazyWorkspaceError::StaleSource)
                } else if used.load(std::sync::atomic::Ordering::Acquire)
                    && live.epoch != snapshot.source.epoch
                {
                    Some(LazyWorkspaceError::StaleSource)
                } else {
                    None
                };
                if let Some(error) = conflict {
                    drop(page);
                    return Err(error);
                }
                drop(reader);
                match page {
                    DirectoryProjectionPage::Native(NativeLazyDirectoryPage { page, allocation }) =>
                        Ok((page, allocation)),
                    DirectoryProjectionPage::Portable(_) =>
                        unreachable!("original frozen listing retains original allocation custody"),
                }
            }.await;
            drop(path);
            result.map_err(|error| (error, None))
        }
    }

    /// Lists one bounded page from the frozen authored, overlay, and source view.
    #[allow(clippy::too_many_arguments)]
    pub async fn list_directory_at_snapshot(
        &self,
        snapshot: LazySnapshotRef,
        path: &str,
        cursor: Option<LazyDirectoryCursor>,
        maximum_entries: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        source_used: Option<&std::sync::atomic::AtomicBool>,
    ) -> crate::MeasuredResult<OperationReceipt<LazyDirectoryPage>, LazyWorkspaceError> {
        in_heap_measured(move |budget| async move {
            let mut work = WorkCounters::UNCHARGED;
            let result = async {
                let source_required = cursor.as_ref().is_none_or(|cursor|
                    matches!(&cursor.phase, LazyDirectoryPhase::Source(_)));
                let state = self.readonly_snapshot_state(snapshot, source_required, &mut crate::kernel::allocation::KernelAccounting::borrowed_standalone(
                    &mut crate::kernel::allocation::AllocationLedger::default(), &mut work, budget, cancellation))?;
                let generation = self.workspace.generation(
                    snapshot.authored_generation, remaining_work(work, budget, 0)?, cancellation,
                ).await.map_err(measured_workspace_error)?;
                work = account_work(work, generation.work, budget)?;
                let page = self.list_directory_at(
                    &state, WorkCounters::UNCHARGED, path, cursor, maximum_entries,
                    remaining_work(work, budget, 0)?, cancellation, None, None, Some(&generation.value), Some(snapshot.id), source_used,
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
        }, budget, cancellation, LazyWorkspaceError::Cancelled, None).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn list_directory_at(
        &self, state: &LazyWorkspaceState, work: WorkCounters, path: &str,
        cursor: Option<LazyDirectoryCursor>, maximum_entries: u32, budget: WorkBudget,
        cancellation: &CancellationToken, mounted: Option<&mut crate::Checkout<A, O>>,
        mounted_mask: Option<(&[String], bool)>, authored_generation: Option<&crate::Generation<A, O>>,
        snapshot_id: Option<LazySnapshotId>, source_used: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<OperationReceipt<LazyDirectoryPage>, LazyWorkspaceError> {
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled.with_prior_work(work))?;
        let projection = DirectoryProjectionWork::Portable {
            work, allocations: crate::kernel::allocation::AllocationLedger::default(), budget, cancellation,
        };
        match self.list_directory_with_projection(
            state, projection, path, cursor, maximum_entries, cancellation,
            mounted, mounted_mask, authored_generation, snapshot_id, source_used,
            #[cfg(not(target_arch = "wasm32"))]
            None,
        ).await? {
            DirectoryProjectionPage::Portable(receipt) => Ok(receipt),
            #[cfg(not(target_arch = "wasm32"))]
            DirectoryProjectionPage::Native(_) => unreachable!("portable directory retains its original work mode"),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn list_directory_in_state_with_native_work(
        &self, state: &LazyWorkspaceState, path: &str, cursor: Option<LazyDirectoryCursor>,
        maximum_entries: u32, original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken, mounted: Option<&mut crate::Checkout<A, O>>,
        mounted_mask: Option<(&[String], bool)>, source_used: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<NativeLazyDirectoryPage, LazyWorkspaceError> {
        let cursor = cursor.map(|mut cursor| { cursor.mark_input_borrowed(); cursor });
        let projection = DirectoryProjectionWork::native(original, cancellation)?;
        match self.list_directory_with_projection(
            state, projection, path, cursor, maximum_entries, cancellation,
            mounted, mounted_mask, None, None, source_used,
            None,
        ).await? {
            DirectoryProjectionPage::Native(page) => Ok(page),
            DirectoryProjectionPage::Portable(_) => unreachable!("native directory retains the original work mode"),
        }
    }

    #[allow(clippy::too_many_lines, clippy::too_many_arguments)]
        async fn list_directory_with_projection(
            &self, state: &LazyWorkspaceState, projection: DirectoryProjectionWork<'_>, path: &str,
            cursor: Option<LazyDirectoryCursor>, maximum_entries: u32,
            cancellation: &CancellationToken, mut mounted: Option<&mut crate::Checkout<A, O>>,
            mounted_mask: Option<(&[String], bool)>,
            authored_generation: Option<&crate::Generation<A, O>>,
            snapshot_id: Option<LazySnapshotId>,
            source_used: Option<&std::sync::atomic::AtomicBool>,
            #[cfg(not(target_arch = "wasm32"))]
            authored_reader: Option<&crate::PinnedReader<A, O>>,
        ) -> Result<DirectoryProjectionPage, LazyWorkspaceError> {
            let (mut future, future_bytes) = in_heap_admitted(
                projection,
                |projection, bytes| projection.claim_frame(bytes),
                move |mut projection| async move {
                let result = async {
                #[cfg(not(target_arch = "wasm32"))]
                let mut current_native_reader = None;
                    cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
                    if maximum_entries == 0 { return Err(LazyWorkspaceError::InvalidPageBound); }
                    if let Some(cursor) = &cursor {
                        projection.claim(cursor.claimable_directory_bytes(), 0)?;
                        if let LazyDirectoryPhase::Authored(Some(after)) = &cursor.phase {
                            let bytes = after.name.retained_bytes() as u64;
                            #[cfg(not(target_arch = "wasm32"))]
                            let bytes = after.native_ownership.unclaimed_retained_bytes(bytes);
                            projection.claim(bytes, 0)?;
                        }
                        if let LazyDirectoryPhase::Source(Some(source)) = &cursor.phase {
                            projection.claim(LazyDirectoryCursor::claimable_source_cursor_bytes(source), 0)?;
                        }
                    }
                    let directory = projection.produce(|accounting, cancellation| NamespacePath::parse_in_profile_with_accounting(path, self.workspace.profile(), self.workspace.limits(), accounting, cancellation).map_err(lazy_cursor_filesystem_error))?;
                    let mut phase = if let Some(cursor) = cursor {
                        if cursor.snapshot != snapshot_id || cursor.source != state.source || cursor.overlay != state.overlay
                            || cursor.directory.depth() != directory.depth()
                            || cursor.directory.encoded_bytes() != directory.encoded_bytes() {
                            return Err(LazyWorkspaceError::StaleCursor);
                        }
                        for (held, requested) in cursor.directory.components().iter().zip(directory.components()) {
                            cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
                            projection.items(1)?;
                            if held != requested { return Err(LazyWorkspaceError::StaleCursor); }
                        }
                        let bytes = cursor.claimable_directory_bytes();
                        drop(cursor.directory);
                        projection.release(bytes)?;
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
                            &mut projection, cancellation,
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
                                    projection.release(LazyDirectoryCursor::claimable_source_cursor_bytes(cursor))?;
                                }
                                let mut cleanup_slot = projection.reserve_source_cleanup()?;
                                if let Some(source_used) = source_used {
                                    source_used.store(true, std::sync::atomic::Ordering::Release);
                                }
                                let mut page = match projection.source_page(self.source.as_ref(), state.source, &directory, source_cursor,
                                maximum_entries, cancellation,).await {
                                    Ok(page) => page,
                                    Err(error) if matches!(error.semantic_error(),
                                        LazyWorkspaceError::Demand(DemandError::Absent | DemandError::NotDirectory)) => {
                                        projection.retire_source_cleanup(cleanup_slot)?;
                                        source_absent = true;
                                        phase = LazyDirectoryPhase::Authored(None);
                                        continue;
                                    }
                                    Err(error) => {
                                        projection.retire_source_cleanup(cleanup_slot)?;
                                        return Err(error);
                                    }
                                };
                                let cleanup = page.publication.as_ref().map_or(WorkCounters::UNCHARGED, |guard| guard.cleanup_budget());
                                let mut reservation = None;
                                let projected = async {
                                    let mounted_mask = (move || mounted_mask)();
                                    reservation = Some(projection.reserve_publication(cleanup)?);
                                    if page.entries.len() > maximum_entries as usize {
                                        return Err(LazyWorkspaceError::InvalidPageBound);
                                    }
                                    cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
                                    let raw_bytes = page.retained_bytes();
                                    let already_owned = page.next.as_ref().map_or(0, |cursor|
                                        cursor.retained_bytes() - LazyDirectoryCursor::claimable_source_cursor_bytes(cursor));
                                    let raw_bytes = raw_bytes.checked_sub(already_owned).ok_or_else(counter_overflow)?;
                                    if matches!(projection, DirectoryProjectionWork::Portable { .. }) {
                                        projection.claim(raw_bytes, 0)?;
                                    }
                                    let count = page.entries.len();
                                    let mut slots = projection.vec::<(bool, Option<usize>)>(count)?;
                                    let directory_tombstoned = self.directory_tombstoned(
                                        state.overlay, path, &mut projection, cancellation).await?;
                                    let facts_beneath = self.directory_has_overlay_beneath(
                                        state.overlay, path, &mut projection, cancellation).await?;
                                    let authored_directory = match directory_authored {
                                        Some(authored) => authored,
                                        None => match mounted.as_deref_mut() {
                                            Some(checkout) if !directory.is_root() => {
                                                self.directory_mounted_record(
                                                    checkout, &directory, &mut projection, cancellation,
                                                ).await?.is_some()
                                            }
                                            _ => true,
                                        },
                                    };
                                    let capacity = if authored_directory && !directory_tombstoned { count } else { 0 };
                                    let mut source_paths = projection.vec::<NamespacePath>(capacity)?;
                                    for entry in page.entries.iter() {
                                        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
                                        projection.items(1)?;
                                        if directory_tombstoned {
                                            slots.push((true, None));
                                            continue;
                                        }
                                        let exact_encoding = matches!((self.workspace.profile(), entry.name().encoding()),
                                            (crate::model::FilesystemProfile::Portable | crate::model::FilesystemProfile::Browser, crate::kernel::NameEncoding::Utf8)
                                            | (crate::model::FilesystemProfile::Posix, crate::kernel::NameEncoding::PosixBytes)
                                            | (crate::model::FilesystemProfile::Windows, crate::kernel::NameEncoding::WindowsUtf16Le));
                                        let child = if facts_beneath || mounted_mask.is_some() || !exact_encoding {
                                            projection.child_text(path, entry.name())?
                                        } else { None };
                                        let hidden = match (mounted_mask, child.as_deref()) {
                                            (Some((paths, _)), Some(child)) => projection.mask_contains(paths, child)?,
                                            _ => false,
                                        };
                                        let hidden = if !hidden && facts_beneath && let Some(child) = child.as_deref() {
                                            let fact = self.directory_overlay_fact(
                                                state.overlay, child, &mut projection, cancellation).await?;
                                            matches!(fact, Some(LazyOverlayChange::Tombstone { .. }))
                                        } else { hidden };
                                        if hidden {
                                            slots.push((true, None));
                                        } else if authored_directory {
                                            let child_path = if exact_encoding {
                                                Some(projection.produce(|accounting, cancellation| directory.child_with_accounting(entry.name(), self.workspace.limits(), accounting, cancellation).map_err(lazy_cursor_filesystem_error))?)
                                            } else if let Some(child) = child.as_deref() {
                                                Some(projection.produce(|accounting, cancellation| NamespacePath::parse_in_profile_with_accounting(child, self.workspace.profile(), self.workspace.limits(), accounting, cancellation).map_err(lazy_cursor_filesystem_error))?)
                                            } else { None };
                                            if let Some(child_path) = child_path {
                                                slots.push((false, Some(source_paths.len())));
                                                source_paths.push(child_path);
                                            } else { slots.push((false, None)); }
                                        } else { slots.push((false, None)); }
                                        if let Some(child) = child {
                                            let bytes = child.capacity() as u64;
                                            drop(child);
                                            projection.release(bytes)?;
                                        }
                                    }
                                    let mut authored = projection.vec::<bool>(source_paths.len())?;
                                    authored.resize(source_paths.len(), false);
                                    if authored_directory && !source_paths.is_empty() {
                                        let head;
                                        let generation = if mounted.is_some() || projection.is_native() { None }
                                        else if let Some(generation) = authored_generation { Some(generation) }
                                        else {
                                            head = { let result = self.workspace.head(projection.remaining()?, cancellation)
                                                    .await.map_err(measured_workspace_error); projection.accept(result) }?;
                                            Some(&head)
                                        };
                                        let maximum = (self.workspace.limits().maximum_paths_per_batch as usize).max(1);
                                        let mut offset = 0;
                                        for chunk in source_paths.chunks(maximum) {
                                            let target = authored.get_mut(offset..offset + chunk.len())
                                                .ok_or(LazyWorkspaceError::Concurrent)?;
                                            #[cfg(not(target_arch = "wasm32"))]
                                            if let Some(original) = projection.native_original() {
                                                if mounted.is_none() && authored_reader.is_none() && current_native_reader.is_none() {
                                                    current_native_reader = Some(self.directory_head_reader(original, cancellation).await?);
                                                }
                                                for (slot, path) in target.iter_mut().zip(chunk) {
                                                    let record = if let Some(checkout) = mounted.as_deref_mut() {
                                                        self.directory_mounted_record(checkout, path, &mut projection, cancellation).await?
                                                    } else {
                                                        authored_reader.or(current_native_reader.as_ref())
                                                            .ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?
                                                            .lookup_record_with_native_work(path, original, cancellation)
                                                            .await.map_err(native_filesystem_error)?
                                                    };
                                                    projection.items(1)?;
                                                    *slot = record.is_some();
                                                }
                                                offset += chunk.len();
                                                continue;
                                            }
                                            if let Some(checkout) = mounted.as_deref_mut() {
                                                let lookup = { let result = checkout.lookup_batch_no_follow(chunk,
                                                        projection.remaining()?, cancellation)
                                                        .await.map_err(|failure| lazy_operation_failure(failure,
                                                            |error| LazyWorkspaceError::Workspace(error.to_string()))); projection.accept(result) }?;
                                                for (slot, entry) in target.iter_mut().zip(lookup.entries) {
                                                    cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
                                                    projection.items(1)?;
                                                    *slot = entry.record.is_some();
                                                }
                                            } else {
                                                let records = { let result = generation.as_ref().ok_or(LazyWorkspaceError::Concurrent)?
                                                        .lookup_paths(chunk, projection.remaining()?, cancellation)
                                                        .await.map_err(|failure| lazy_operation_failure(failure,
                                                            |error| LazyWorkspaceError::Workspace(error.to_string()))); projection.accept(result) }?;
                                                for (slot, record) in target.iter_mut().zip(records) {
                                                    cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
                                                    projection.items(1)?;
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
                                    projection.release(path_bytes)?;
                                    projection.items(count as u64)?;
                                    let visible = slots.iter().filter(|(hidden, index)| !*hidden
                                        && !index.is_some_and(|index| authored.get(index).copied().unwrap_or(false))).count();
                                    let mut entries = projection.vec::<LazyDirectoryEntry>(visible)?;
                                    projection.items(count as u64)?;
                                    let slot_bytes = (slots.capacity() * std::mem::size_of::<(bool, Option<usize>)>()) as u64;
                                    for (entry, (hidden, index)) in page.entries.iter().zip(slots) {
                                        if hidden || index.is_some_and(|index| authored.get(index).copied().unwrap_or(false)) { continue; }
                                        let name = projection.clone_name(entry.name())?;
                                        entries.push(LazyDirectoryEntry {
                                            name, kind: entry.node.kind, authored: false,
                                            source: Some(entry.node), unobserved: !facts_beneath,
                                        });
                                    }
                                    projection.release(slot_bytes)?;
                                    let authored_bytes = (authored.capacity() * std::mem::size_of::<bool>()) as u64;
                                    drop(authored);
                                    projection.release(authored_bytes)?;
                                    let next = match page.next.take() {
                                        Some(next) => Some(LazyDirectoryPhase::Source(Some(next))),
                                        None if authored_directory => Some(LazyDirectoryPhase::Authored(None)),
                                        None => None,
                                    }.map(|phase| LazyDirectoryCursor {
                                        source: state.source, overlay: state.overlay, directory, phase, snapshot: snapshot_id,
                                        #[cfg(not(target_arch = "wasm32"))]
                                        native_directory_ownership: DirectoryNativeOwnership::default(),
                                    });
                                    projection.prepare_page(LazyDirectoryPage { entries, next })
                                }.await;
                                if let Some(reservation) = reservation {
                                    projection.restore_publication(reservation);
                                }
                                let outcome = match projected {
                                    Ok(projected) => projection.accept_publication(&mut page.publication, cancellation).await
                                        .map(|()| projected),
                                    Err(error) => Err(error),
                                };
                                return match outcome {
                                    Ok(projected) => {
                                        projection.retire_source_cleanup(cleanup_slot)?;
                                        Ok(projected)
                                    }
                                    Err(primary) => {
                                        cleanup_slot.store(primary);
                                        match projection.abort_publication(&mut page.publication) {
                                            Ok(()) => {
                                                let primary = cleanup_slot.into_primary();
                                                projection.retire_source_cleanup_bytes()?;
                                                Err(primary)
                                            }
                                            Err(cleanup) => Err(cleanup_slot.into_cleanup(
                                                cleanup,
                                                std::mem::replace(&mut page.publication,
                                                    None),
                                            )),
                                        }
                                    }
                                };
                            }
                            LazyDirectoryPhase::Authored(after) => {
                                #[cfg(not(target_arch = "wasm32"))]
                                if let Some(original) = projection.native_original()
                                    && mounted.is_none() && authored_reader.is_none() && current_native_reader.is_none()
                                {
                                    current_native_reader = Some(self.directory_head_reader(original, cancellation).await?);
                                }
                                let response = self.directory_authored_entries(
                                    path, &directory, after.as_deref(), maximum_entries,
                                    mounted.as_deref_mut(), authored_generation,
                                    #[cfg(not(target_arch = "wasm32"))]
                                    authored_reader.or(current_native_reader.as_ref()),
                                    &mut projection, cancellation,
                                ).await;
                                let (raw, has_more) = match response {
                                    Ok(page) => page,
                                    Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::NotFound) => {
                                        if source_absent { return Err(error); }
                                        return projection.prepare_page(LazyDirectoryPage { entries: Vec::new(), next: None });
                                    }
                                    Err(error) => return Err(error),
                                };
                                let count = raw.len();
                                if count > maximum_entries as usize { return Err(LazyWorkspaceError::InvalidPageBound); }
                                if !projection.is_native() {
                                    projection.claim(raw.retained_bytes(true)?, 0)?;
                                }
                                let raw_slots = raw.retained_bytes(false)?;
                                let next_name = if has_more {
                                    raw.last_name().map(|name| projection.clone_name(name)).transpose()?
                                } else { None };
                                let mut entries = projection.vec::<LazyDirectoryEntry>(count)?;
                                projection.items(count as u64)?;
                                raw.append_into(&mut entries);
                                projection.release(raw_slots)?;
                                let after_bytes = after.as_ref().map_or(0, |after| {
                                    let bytes = after.name.retained_bytes() as u64;
                                    #[cfg(not(target_arch = "wasm32"))]
                                    let bytes = after.native_ownership.unclaimed_retained_bytes(bytes);
                                    bytes
                                });
                                drop(after);
                                projection.release(after_bytes)?;
                                cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
                                let next = next_name.map(|name| LazyDirectoryCursor {
                                    source: state.source, overlay: state.overlay, directory,
                                    phase: LazyDirectoryPhase::Authored(Some(LazyDirectoryAfter::new(name))),
                                    snapshot: snapshot_id,
                                    #[cfg(not(target_arch = "wasm32"))]
                                    native_directory_ownership: DirectoryNativeOwnership::default(),
                                });
                                return projection.prepare_page(LazyDirectoryPage { entries, next });
                            }
                        }
                    }
                }.await;
                (result, projection)
                },
            )?;
            let (result, mut projection) = future.as_mut().await;
            drop(future);
            projection.release(future_bytes)?;
            projection.finish(result)
        }

    /// Captures one visible subtree into authored state without visiting its siblings.
    pub async fn exactify(
        &self,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        permit: crate::PublicationPermit,
    ) -> Result<OperationReceipt<crate::Generation<A, O>>, LazyWorkspaceError> {
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        PortablePath::validate(path, self.workspace.limits())
            .map_err(|error| LazyWorkspaceError::Workspace(error.to_string()))?;
        let prior = if path == "/" { WorkCounters::UNCHARGED } else {
            let selected = self.lookup_measured(path, budget, cancellation).await?;
            if selected.value.lookup.stat().kind != SourceNodeKind::Directory {
                return Err(LazyWorkspaceError::NotDirectory.with_prior_work(selected.work));
            }
            drop(selected.value);
            selected.work
        };
        let exact = self.exactify_selected_guarded(
            &self.workspace, path, None,
            remaining_work(prior, budget, 0).map_err(|error| error.with_prior_work(prior))?,
            cancellation, permit,
            &mut |_| async { Ok(OperationReceipt { value: (), work: WorkCounters::UNCHARGED }) }, None,
        ).await.map_err(|error| error.with_prior_work(prior))?;
        Ok(OperationReceipt {
            value: exact.value,
            work: account_work(prior, exact.work, budget).map_err(|error| error.with_prior_work(prior))?,
        })
    }

    /// Exactifies a live subtree on the supplied original native ledger and permit.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn exactify_subtree_with_native_work(
        &self, path: &str, permit: crate::PublicationPermit,
        original: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<crate::Generation<A, O>, LazyWorkspaceError>
    where D: 'static,
    {
        let work = LazyTreapWork::native_with_work(original, cancellation)?;
        in_heap_lazy::<_, LazyWorkspaceError, _>(&work, || async {
            original.admit(WorkCounters::UNCHARGED)?;
            cancellation.check().map_err(|_| LazyWorkspaceError::NativeWork(
                crate::NativeTreeMaterializationError::Cancelled))?;
            let source = self.source.reference();
            let (selected, allocation) = self.lookup_resolved_with_native_work(
                path, original, cancellation,
            ).await?;
            let is_directory = selected.lookup.stat().kind == SourceNodeKind::Directory;
            drop(selected);
            drop(allocation);
            if !is_directory { return Err(LazyWorkspaceError::NotDirectory); }
            if self.source.reference() != source { return Err(LazyWorkspaceError::StaleSource); }
            let source_used = std::sync::atomic::AtomicBool::new(false);
            let source_owner = Arc::clone(&self.source);
            let mut builder = |_current_source_used: bool| {
                let source_owner = Arc::clone(&source_owner);
                let original = original.clone();
                let cancellation = cancellation.clone();
                move |_: &crate::NativeMaterializationWork| {
                    let source_owner = Arc::clone(&source_owner);
                    let original = original.clone();
                    let cancellation = cancellation.clone();
                    async move {
                        original.admit(WorkCounters::UNCHARGED)?;
                        cancellation.check().map_err(|_| LazyWorkspaceError::NativeWork(
                            crate::NativeTreeMaterializationError::Cancelled))?;
                        if source_owner.reference() != source {
                            return Err(LazyWorkspaceError::StaleSource);
                        }
                        Ok(())
                    }
                }
            };
            let mut publication = NativeOwnedBuilder { builder: &mut builder, original };
            self.exactify_selected_with_accounting(
                &self.workspace, path, None, cancellation, permit, &mut publication,
                Some(&source_used), Some(source),
                &mut ExactifyCollectorWork::Native { work: &work, original },
            ).await
        }).await
    }

    async fn collect_selected_paths_with_accounting(
        &self,
        roots: &[String],
        snapshot: Option<LazySnapshotRef>,
        generation: Option<&crate::Generation<A, O>>,
        accounting: &mut ExactifyCollectorWork<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Vec<String>, bool), LazyWorkspaceError> {
        let source_used = std::sync::atomic::AtomicBool::new(false);
        let state = match (snapshot, &mut *accounting) {
            (Some(snapshot), ExactifyCollectorWork::Portable { work, budget, .. }) =>
                Some(self.readonly_snapshot_state(snapshot, false, &mut crate::kernel::allocation::KernelAccounting::borrowed_standalone(
                    &mut crate::kernel::allocation::AllocationLedger::default(), work, *budget, cancellation))?),
            #[cfg(not(target_arch = "wasm32"))]
            (Some(snapshot), ExactifyCollectorWork::Native { original, .. }) =>
                Some(self.readonly_snapshot_state(snapshot, false, &mut crate::kernel::allocation::KernelAccounting::native(original, cancellation))?),
            (None, _) => None,
        };
        let mut paths = Vec::new();
        for (index, root) in roots.iter().enumerate() {
            if accounting.with_kernel(cancellation, |kernel|
                exactify_root_is_covered(roots, index, kernel, cancellation))? {
                continue;
            }
            let mut collected = self.collect_exactify_paths_with_accounting(
                root, snapshot.zip(state.as_ref()).map(|(snapshot, state)| (self, snapshot, state)),
                generation, Some(&source_used), accounting, cancellation, None,
            ).await?;
            let slots = exactify_path_vector_bytes(&collected)?;
            for path in collected.drain(..) {
                accounting.reserve(&mut paths)?;
                paths.push(path);
            }
            drop(collected);
            accounting.release(slots)?;
        }
        accounting.with_kernel(cancellation, |kernel|
            sort_exactify_paths_with_accounting(&mut paths, kernel, cancellation))?;
        accounting.dedup(&mut paths, cancellation)?;
        let used = source_used.load(std::sync::atomic::Ordering::Acquire);
        if let Some(snapshot) = snapshot {
            let current = self.source.reference();
            if current.identity != snapshot.source.identity || (used && current.epoch != snapshot.source.epoch) {
                return Err(LazyWorkspaceError::StaleSource);
            }
        }
        Ok((paths, used))
    }

    fn collect_exactify_paths_with_accounting<'a, 'work>(
        &'a self,
        root: &'a str,
        read_view: Option<(&'a Self, LazySnapshotRef, &'a LazyWorkspaceState)>,
        authored: Option<&'a crate::Generation<A, O>>,
        source_used: Option<&'a std::sync::atomic::AtomicBool>,
        accounting: &'a mut ExactifyCollectorWork<'work>,
        cancellation: &'a CancellationToken,
        live_state: Option<&'a LazyWorkspaceState>,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<Vec<String>, LazyWorkspaceError>> {
        Box::pin(async move {
            let is_directory = if let Some(generation) = authored {
                let response = generation.stat(root, accounting.remaining()?, cancellation).await;
                match response {
                    Ok(receipt) => accounting.accept(Ok(receipt))?.kind == FileKind::Directory,
                    Err(failure) if matches!(failure.error, WorkspaceError::NotFound) => {
                        accounting.record(*failure.work)?;
                        return Ok(Vec::new());
                    }
                    Err(failure) => return Err(accounting.nested_error(measured_workspace_error(failure))),
                }
            } else if let Some((origin, snapshot, state)) = read_view {
                #[cfg(target_arch = "wasm32")]
                let lookup: Option<bool> = None;
                #[cfg(not(target_arch = "wasm32"))]
                let lookup = match accounting.original() {
                    Some(original) => {
                        let used = source_used.ok_or(LazyWorkspaceError::NativeLedgerMismatch)?;
                        match origin.inspect_at_snapshot_with_work(
                            snapshot, root, original, cancellation, used,
                        ).await {
                            Ok(frozen) => {
                                let is_directory = frozen.0.lookup.stat().kind == SourceNodeKind::Directory;
                                drop(frozen);
                                Some(is_directory)
                            }
                            Err((error, allocation)) => {
                                drop(allocation);
                                if matches!(error.semantic_error(), LazyWorkspaceError::NotFound) {
                                    return Ok(Vec::new());
                                }
                                return Err(error);
                            }
                        }
                    }
                    _ => None,
                };
                if let Some(is_directory) = lookup { is_directory } else {
                    let response = origin.inspect_with(root, InspectBasis {
                        state: Some(state), authored_generation: Some(snapshot.authored_generation),
                        retain_pinned_observation: true, source_used, ..InspectBasis::default()
                    }, accounting.remaining()?, cancellation).await;
                    match response {
                        Ok(receipt) => accounting.accept(Ok(receipt))?.lookup.stat().kind == SourceNodeKind::Directory,
                        Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::NotFound) => {
                            accounting.record(error.spent_work())?;
                            return Ok(Vec::new());
                        }
                        Err(error) => return Err(accounting.nested_error(error)),
                    }
                }
            } else { true };
            let mut directories = Vec::new();
            let mut paths = Vec::new();
            if is_directory {
                accounting.reserve(&mut directories)?;
                directories.push(accounting.clone_string(root, cancellation)?);
            }
            if root != "/" || authored.is_some() {
                accounting.reserve(&mut paths)?;
                paths.push(accounting.clone_string(root, cancellation)?);
            }
            let mut next_directory = 0_usize;
            while next_directory < directories.len() {
                accounting.check(cancellation)?;
                let directory = std::mem::take(&mut directories[next_directory]);
                next_directory += 1;
                let mut cursor: Option<LazyDirectoryCursor> = None;
                let mut authored_cursor: Option<LogicalName> = None;
                loop {
                    accounting.check(cancellation)?;
                    // Drop physical entries before their allocation guard.
                    #[cfg(not(target_arch = "wasm32"))]
                    let mut page_allocation = None;
                    let (entries, slot_bytes, next, authored_next) = if let Some(generation) = authored {
                        let response = generation.list_directory(
                            &directory, authored_cursor.as_ref(), 1_024, accounting.remaining()?, cancellation,
                        ).await.map_err(|failure| accounting.nested_error(measured_workspace_error(failure)));
                        let page = accounting.accept(response)?;
                        if let Some(previous) = authored_cursor.take() {
                            let bytes = previous.retained_bytes() as u64;
                            drop(previous);
                            accounting.release(bytes)?;
                        }
                        let page_bytes = retained_directory_page_bytes(
                            &page.entries,
                            page.entries.iter().map(|entry| entry.name.retained_bytes()),
                        )?;
                        accounting.retain_page(page_bytes)?;
                        let next = if page.has_more {
                            let name = page.entries.last().ok_or(LazyWorkspaceError::Concurrent)?;
                            Some(accounting.clone_name(&name.name, cancellation)?)
                        } else { None };
                        let slots = crate::foundation::usize_to_u64(page.entries.capacity()
                            .checked_mul(std::mem::size_of::<WorkspaceDirectoryEntry>()).ok_or_else(counter_overflow)?);
                        (ExactifyEntries::Authored(page.entries.into_iter()), slots, None, next)
                    } else {
                        accounting.release_portable_cursor(cursor.as_ref())?;
                        #[cfg(not(target_arch = "wasm32"))]
                        let native_page = if let Some(original) = accounting.original() {
                            let used = source_used.ok_or(LazyWorkspaceError::NativeLedgerMismatch)?;
                            let (page, allocation) = if let Some((origin, snapshot, _)) = read_view {
                                origin.list_directory_at_snapshot_observed_with_work(
                                    snapshot, &directory, cursor.take(), 1_024, original, cancellation, used,
                                ).await.map_err(|(error, allocation)| { drop(allocation); error })?
                            } else {
                                let state = live_state.ok_or(LazyWorkspaceError::NativeLedgerMismatch)?;
                                self.check_exactify_live_source(Some(state.source), state.source)?;
                                let NativeLazyDirectoryPage { page, allocation } =
                                    self.list_directory_in_state_with_native_work(
                                        state, &directory, cursor.take(), 1_024, original,
                                        cancellation, None, None, Some(used),
                                    ).await?;
                                self.check_exactify_live_source(Some(state.source), state.source)?;
                                (page, allocation)
                            };
                            page_allocation = Some(allocation);
                            Some(page)
                        } else { None };
                        #[cfg(target_arch = "wasm32")]
                        let native_page: Option<LazyDirectoryPage> = None;
                        let page = if let Some(page) = native_page { page } else {
                            let response = if let Some((origin, snapshot, _)) = read_view {
                                let allowance = accounting.remaining()?;
                                origin.list_directory_at_snapshot(
                                    snapshot, &directory, cursor.take(), 1_024, allowance, cancellation, source_used,
                                ).await.map_err(|failure| lazy_operation_failure(failure, std::convert::identity))
                            } else {
                                self.list_directory(
                                    &directory, cursor.take(), 1_024, accounting.remaining()?,
                                    cancellation,
                                ).await
                            }.map_err(|error| exactify_error(accounting.nested_error(error)));
                            accounting.accept(response)?
                        };
                        let page_bytes = retained_directory_page_bytes(
                            &page.entries,
                            page.entries.iter().map(|entry| entry.name.retained_bytes()),
                        )?;
                        let cursor_bytes = page.next.as_ref().map(LazyDirectoryCursor::retained_bytes)
                            .transpose()?.unwrap_or(0);
                        accounting.retain_page(page_bytes.checked_add(cursor_bytes).ok_or_else(counter_overflow)?)?;
                        let slots = crate::foundation::usize_to_u64(page.entries.capacity()
                            .checked_mul(std::mem::size_of::<LazyDirectoryEntry>()).ok_or_else(counter_overflow)?);
                        (ExactifyEntries::Lazy(page.entries.into_iter()), slots, page.next, None)
                    };
                    for (name, is_directory) in entries {
                        accounting.check(cancellation)?;
                        accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }, cancellation)?;
                        let name_bytes = name.retained_bytes() as u64;
                        let path = accounting.child_path(&directory, &name, cancellation)?;
                        drop(name);
                        accounting.release_page_bytes(name_bytes)?;
                        if is_directory {
                            accounting.reserve(&mut directories)?;
                            directories.push(accounting.clone_string(&path, cancellation)?);
                        }
                        accounting.reserve(&mut paths)?;
                        paths.push(path);
                    }
                    accounting.release_page_bytes(slot_bytes)?;
                    #[cfg(not(target_arch = "wasm32"))]
                    drop(page_allocation);
                    cursor = next;
                    authored_cursor = authored_next;
                    if cursor.is_none() && authored_cursor.is_none() { break; }
                }
                let bytes = directory.capacity() as u64;
                drop(directory);
                accounting.release(bytes)?;
            }
            let directory_bytes = exactify_path_vector_bytes(&directories)?;
            drop(directories);
            accounting.release(directory_bytes)?;
            Ok(paths)
        })
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
        source_used: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<OperationReceipt<crate::Generation<A, O>>, LazyWorkspaceError>
    where
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::FsResult<()>> + ProviderTask,
    {
        in_heap_measured(move |budget| async move {
            let mut work = WorkCounters::default();
            let mut retained_bytes = 0;
            let result = self.exactify_selected_with_accounting(
                workspace, root, read_view, cancellation, permit, before_publication, source_used,
                None, &mut ExactifyCollectorWork::Portable {
                    retained_bytes: &mut retained_bytes, work: &mut work, budget,
                },
            ).await;
            result.map(|value| OperationReceipt { value, work })
                .map_err(|error: LazyWorkspaceError| error.with_prior_work(work).into_measured_failure())
        }, budget, cancellation, LazyWorkspaceError::Cancelled, None).await
            .map_err(|failure| lazy_operation_failure(failure, std::convert::identity))
    }

    async fn exactify_selected_with_accounting<P>(
        &self, workspace: &Workspace<A, O>, root: &str,
        read_view: Option<(&Self, LazySnapshotRef)>, cancellation: &CancellationToken,
        permit: crate::PublicationPermit, publication: &mut P,
        source_used: Option<&std::sync::atomic::AtomicBool>, live_source: Option<SourceReference>,
        accounting: &mut ExactifyCollectorWork<'_>,
    ) -> Result<crate::Generation<A, O>, P::Error>
    where
        P: ExactifyPublication<A, O>,
    {
        accounting.check(cancellation)?;
        let state = match &mut *accounting {
            ExactifyCollectorWork::Portable { work, budget, .. } => {
                if let Some((origin, snapshot)) = read_view {
                    LazyReadonlyNode::Portable(origin.readonly_snapshot_state(
                        snapshot, false, &mut crate::kernel::allocation::KernelAccounting::borrowed_standalone(
                            &mut crate::kernel::allocation::AllocationLedger::default(), work, *budget, cancellation),
                    )?)
                } else {
                    let receipt = { let state_work = LazyTreapWork::new(*budget, cancellation); let result = self.state(&state_work).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("standalone state keeps its accounting owner") }); state_work.measured(result, |error, spent| error.with_prior_work(spent)) }?;
                    **work = receipt.work;
                    LazyReadonlyNode::Portable(receipt.value)
                }
            }
            #[cfg(not(target_arch = "wasm32"))]
            ExactifyCollectorWork::Native { original, .. } => {
                if let Some((origin, snapshot)) = read_view {
                    LazyReadonlyNode::Portable(origin.readonly_snapshot_state(
                        snapshot, false, &mut crate::kernel::allocation::KernelAccounting::native(original, cancellation),
                    )?)
                } else {
                    self.state(&LazyTreapWork::native_with_work(original, cancellation)?).await?
                }
            }
        };
        let state = state.as_state();
        self.check_exactify_live_source(live_source, state.source)?;
        let projection = read_view.map_or(self, |(origin, _)| origin);
        let mut pinned_generation = self.exactify_head_with_accounting(
            workspace, accounting, cancellation,
        ).await?;
        let capture = if let Some((_, snapshot)) = read_view {
            snapshot.id
        } else {
            accounting.charge(WorkCounters {
                bytes_hashed: crate::foundation::usize_to_u64(LAZY_SNAPSHOT_DOMAIN.len()) + 136,
                ..WorkCounters::UNCHARGED
            }, cancellation)?;
            LazySnapshotRef::new(self.workspace.id(), pinned_generation.id(),
                state.source, state.overlay, state.shadows).id
        };
        let mut paths = self.collect_exactify_paths_with_accounting(
            root, read_view.map(|(origin, snapshot)| (origin, snapshot, state)),
            None, source_used, accounting, cancellation, Some(state),
        ).await?;
        self.check_exactify_live_source(live_source, state.source)?;
        let path_vector_bytes = exactify_path_vector_bytes(&paths)?;
        accounting.with_kernel(cancellation, |kernel|
            sort_exactify_paths_with_accounting(&mut paths, kernel, cancellation))?;
        let maximum_batch_paths = exactify_batch_paths(self.workspace.limits());
        let mut seen_sources: Vec<(FileId, String)> =
            accounting.vector(maximum_batch_paths.min(paths.len()), cancellation)?;
        let seen_vector_bytes = crate::foundation::usize_to_u64(seen_sources.capacity()
            .checked_mul(std::mem::size_of::<(FileId, String)>()).ok_or_else(counter_overflow)?);
        let mut transaction = None;
        let mut batch_paths = 0_usize;
        let mut batch_dirty = false;
        let mut batch_index = 0_u64;
        let mut pending_sources = Vec::new();
        let mut pending_source_bytes = 0_u64;
        let mut authored_batch: Vec<Option<FileRecord>> = Vec::new();
        let mut authored_batch_bytes = 0_u64;
        for index in 0..paths.len() {
            accounting.check(cancellation)?;
            self.check_exactify_live_source(live_source, state.source)?;
            if batch_paths == maximum_batch_paths {
                if batch_dirty {
                    self.exactify_flush_with_accounting(
                        transaction.as_mut().ok_or(LazyWorkspaceError::Concurrent)?,
                        &mut pending_sources, &mut pending_source_bytes, accounting, cancellation,
                    ).await?;
                    let committed = publication.commit(
                        self,
                        transaction.take().ok_or(LazyWorkspaceError::Concurrent)?, permit,
                        source_used.is_some_and(|used| used.load(std::sync::atomic::Ordering::Acquire)), accounting, cancellation,
                    ).await?;
                    accounting.replace_generation(&mut pinned_generation, committed)?;
                }
                transaction = None;
                batch_dirty = false;
                batch_paths = 0;
                batch_index = batch_index.checked_add(1).ok_or_else(counter_overflow)?;
                for (_, retained) in seen_sources.drain(..) {
                    let bytes = crate::foundation::usize_to_u64(retained.capacity());
                    drop(retained);
                    accounting.release(bytes)?;
                }
            }
            if batch_paths == 0 {
                let end = index.saturating_add(maximum_batch_paths).min(paths.len());
                drop(std::mem::take(&mut authored_batch));
                accounting.release(authored_batch_bytes)?;
                authored_batch = self.exactify_authored_batch_with_accounting(
                    &pinned_generation,
                    paths.get(index..end).ok_or(LazyWorkspaceError::Concurrent)?,
                    accounting, cancellation,
                ).await?;
                authored_batch_bytes = crate::foundation::usize_to_u64(authored_batch.capacity()
                    .checked_mul(std::mem::size_of::<Option<FileRecord>>()).ok_or_else(counter_overflow)?);
            }
            let authored_absent = authored_batch
                .get(batch_paths).ok_or(LazyWorkspaceError::Concurrent)?.is_none();
            let path = std::mem::take(paths.get_mut(index).ok_or(LazyWorkspaceError::Concurrent)?);
            let lookup = self.exactify_inspect_with_accounting(
                &path, read_view, state, authored_absent, source_used, accounting, cancellation,
            ).await.map_err(|error| accounting.context_error(error))?;
            let resolved = lookup.as_state();
            self.check_exactify_live_source(live_source, state.source)?;
            self.check_exactify_live_source(live_source, resolved.source.unwrap_or(state.source))?;
            if matches!(resolved.lookup, LazyLookup::Source(_)) {
                accounting.charge(WorkCounters { materializations: 1, ..WorkCounters::UNCHARGED }, cancellation)?;
            }
            if transaction.is_none() {
                accounting.charge(WorkCounters {
                    bytes_hashed: crate::foundation::usize_to_u64(b"acyclic-fs-lazy-exactify-batch-v1\0".len())
                        + 32 + crate::foundation::usize_to_u64(root.len()) + 8,
                    ..WorkCounters::UNCHARGED
                }, cancellation)?;
                transaction = Some(self.exactify_begin_with_accounting(
                    workspace, &pinned_generation, exactify_batch_key(capture, root, batch_index),
                    parent_path(root), accounting, cancellation,
                ).await?);
            }
            let source_id = match &resolved.lookup {
                LazyLookup::Source(node)
                    if node.kind == SourceNodeKind::RegularFile && node.link_count != Some(1) =>
                    Some(self.exactify_source_id_with_accounting(node, accounting, cancellation)?),
                _ => None,
            };
            let alias = if let Some(id) = source_id {
                let mut found = None;
                for (seen, alias) in &seen_sources {
                    accounting.check(cancellation)?;
                    accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }, cancellation)?;
                    if *seen == id { found = Some(alias); break; }
                }
                found
            } else { None };
            let current = transaction.as_mut().ok_or(LazyWorkspaceError::Concurrent)?;
            let keep_path = if let Some(alias) = alias {
                self.exactify_flush_with_accounting(
                    current, &mut pending_sources, &mut pending_source_bytes, accounting, cancellation,
                ).await?;
                match &mut *accounting {
                    ExactifyCollectorWork::Portable { .. } => {
                        let linked = current.hard_link(
                            alias, &path, accounting.remaining()?, cancellation,
                        ).await.map(|receipt| receipt.work).map_err(measured_workspace_error)
                            .map_err(|error| exactify_error(accounting.nested_error(error)))?;
                        accounting.record(linked)?;
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    ExactifyCollectorWork::Native { original, .. } => {
                        current.hard_link(alias, &path, *original, cancellation)
                            .await.map_err(native_workspace_error)?;
                    }
                }
                batch_dirty = true;
                false
            } else if let LazyLookup::Source(node) = &resolved.lookup
                && matches!(node.kind, SourceNodeKind::RegularFile | SourceNodeKind::Directory)
            {
                let content = if node.kind == SourceNodeKind::RegularFile {
                    let pinned = (resolved.source.ok_or(LazyWorkspaceError::Concurrent)?, *node);
                    let staged = match &mut *accounting {
                        ExactifyCollectorWork::Portable { .. } => {
                            let receipt = projection.stage_regular_source_content(
                                current, &path, &pinned, accounting.remaining()?, cancellation,
                                read_view.map(|(_, snapshot)| snapshot), source_used,
                            ).await.map_err(|error| accounting.context_error(accounting.nested_error(error)))?;
                            accounting.accept(Ok(receipt))?
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        ExactifyCollectorWork::Native { original, .. } =>
                            projection.stage_regular_source_content_with_native_work(
                                current, &path, &pinned, original, cancellation,
                                read_view.map(|(_, snapshot)| snapshot), source_used,
                            ).await?,
                    };
                    self.check_exactify_live_source(live_source, state.source)?;
                    Some(staged)
                } else { None };
                let file_id = if content.is_some() {
                    Some(match source_id {
                        Some(id) => id,
                        None => self.exactify_source_id_with_accounting(node, accounting, cancellation)?,
                    })
                } else { None };
                let namespace = accounting.namespace(
                    &path, self.workspace.profile(), self.workspace.limits(), cancellation,
                )?;
                let paid_bytes = namespace.retained_bytes();
                let mutation = if let Some(content) = content {
                    AuthoredMutation::CreateFileFromContent {
                        path: namespace, content, metadata: source_file_metadata(node.metadata), file_id,
                    }
                } else {
                    AuthoredMutation::CreateDirectory {
                        path: namespace, metadata: source_file_metadata(node.metadata),
                    }
                };
                accounting.queue(&mut pending_sources, mutation, paid_bytes, &mut pending_source_bytes)?;
                batch_dirty = true;
                source_id.is_some()
            } else {
                self.exactify_flush_with_accounting(
                    current, &mut pending_sources, &mut pending_source_bytes, accounting, cancellation,
                ).await?;
                let applied = match &mut *accounting {
                    ExactifyCollectorWork::Portable { budget, .. } => {
                        let maximum_bytes = budget.source_bytes_read;
                        let work = LazyTreapWork::new(accounting.remaining()?, cancellation);
                        let lookup_bytes = match &resolved.lookup {
                            LazyLookup::Authored { path, .. } => crate::foundation::usize_to_u64(path.capacity()),
                            _ => 0,
                        };
                        let result = async {
                            work.adopt(lookup_bytes)?;
                            projection.apply_resolved_promotion(current, &path, resolved, maximum_bytes, read_view.map(|(_, snapshot)| snapshot), &work, source_used).await
                        }.await;
                        let spent = work.spent();
                        let value = result.map_err(|error: LazyWorkspaceError|
                            accounting.context_error(accounting.nested_error(error.with_prior_work(spent))))?;
                        accounting.record(spent)?;
                        value
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    ExactifyCollectorWork::Native { work, original } =>
                        projection.apply_resolved_promotion(current, &path, resolved, original.native_read_meter()
                                .with_original_leaf(|meter| meter.budget.source_bytes_read), read_view.map(|(_, snapshot)| snapshot), work, source_used).await?,
                };
                batch_dirty |= applied;
                source_id.is_some() && applied
            };
            drop(lookup);
            if keep_path {
                seen_sources.push((source_id.ok_or(LazyWorkspaceError::Concurrent)?, path));
            } else {
                let bytes = crate::foundation::usize_to_u64(path.capacity());
                drop(path);
                accounting.release(bytes)?;
            }
            batch_paths += 1;
        }
        if batch_dirty {
            self.exactify_flush_with_accounting(
                transaction.as_mut().ok_or(LazyWorkspaceError::Concurrent)?,
                &mut pending_sources, &mut pending_source_bytes, accounting, cancellation,
            ).await?;
            let committed = publication.commit(
                        self,
                transaction.take().ok_or(LazyWorkspaceError::Concurrent)?, permit,
                source_used.is_some_and(|used| used.load(std::sync::atomic::Ordering::Acquire)), accounting, cancellation,
            ).await?;
            accounting.replace_generation(&mut pinned_generation, committed)?;
        }
        for (_, retained) in seen_sources.drain(..) {
            let bytes = crate::foundation::usize_to_u64(retained.capacity());
            drop(retained);
            accounting.release(bytes)?;
        }
        drop(seen_sources);
        accounting.release(seen_vector_bytes)?;
        drop(paths);
        accounting.release(path_vector_bytes)?;
        drop(transaction);
        drop(authored_batch);
        accounting.release(authored_batch_bytes)?;
        let expected_generation = pinned_generation.id();
        let publication_bytes = pinned_generation.publication_retained_bytes();
        drop(pinned_generation);
        accounting.release_page_bytes(publication_bytes)?;
        let head = self.exactify_head_with_accounting(workspace, accounting, cancellation).await
            .map_err(|error| accounting.context_error(error))?;
        self.check_exactify_live_source(live_source, state.source)?;
        if head.id() != expected_generation { return Err(LazyWorkspaceError::Concurrent.into()); }
        Ok(head)
    }

    fn check_exactify_live_source(
        &self, expected: Option<SourceReference>, state_source: SourceReference,
    ) -> Result<(), LazyWorkspaceError> {
        if let Some(expected) = expected
            && (state_source != expected || self.source.reference() != expected)
        {
            return Err(LazyWorkspaceError::StaleSource);
        }
        Ok(())
    }

    async fn exactify_head_with_accounting(
        &self, workspace: &Workspace<A, O>, accounting: &mut ExactifyCollectorWork<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::Generation<A, O>, LazyWorkspaceError> {
        match &mut *accounting {
            ExactifyCollectorWork::Portable { .. } => {
                let receipt = workspace.head(accounting.remaining()?, cancellation).await
                    .map_err(measured_workspace_error)?;
                accounting.accept(Ok(receipt))
            }
            #[cfg(not(target_arch = "wasm32"))]
            ExactifyCollectorWork::Native { original, .. } =>
                workspace.head(*original, cancellation)
                    .await.map_err(|error| native_workspace_error_pair((error, None))),
        }
    }

    async fn exactify_authored_batch_with_accounting(
        &self, generation: &crate::Generation<A, O>, paths: &[String],
        accounting: &mut ExactifyCollectorWork<'_>, cancellation: &CancellationToken,
    ) -> Result<Vec<Option<FileRecord>>, LazyWorkspaceError> {
        let mut namespaces = accounting.vector(paths.len(), cancellation)?;
        let mut namespace_bytes = crate::foundation::usize_to_u64(namespaces.capacity()
            .checked_mul(std::mem::size_of::<NamespacePath>()).ok_or_else(counter_overflow)?);
        for path in paths {
            let namespace = accounting.namespace(
                path, self.workspace.profile(), self.workspace.limits(), cancellation,
            )?;
            namespace_bytes = namespace_bytes.checked_add(namespace.retained_bytes()).ok_or_else(counter_overflow)?;
            namespaces.push(namespace);
        }
        let found = match &mut *accounting {
            ExactifyCollectorWork::Portable { .. } => {
                let receipt = generation.lookup_paths(&namespaces, accounting.remaining()?, cancellation)
                    .await.map_err(|failure| accounting.nested_error(lazy_operation_failure(
                        failure, |error| LazyWorkspaceError::Workspace(error.to_string()),
                    )))?;
                let values = accounting.accept(Ok(receipt))?;
                let bytes = crate::foundation::usize_to_u64(values.capacity()
                    .checked_mul(std::mem::size_of::<Option<FileRecord>>()).ok_or_else(counter_overflow)?);
                accounting.retain_page(bytes)?;
                values
            }
            #[cfg(not(target_arch = "wasm32"))]
            ExactifyCollectorWork::Native { original, .. } => {
                let original = *original;
                let reader = self.workspace.readonly_pinned_reader_with_native_work(
                    generation.id(), original, cancellation,
                ).await.map_err(native_workspace_error)?;
                let mut values = accounting.vector(namespaces.len(), cancellation)?;
                for namespace in &namespaces {
                    values.push(reader.lookup_record_with_native_work(namespace, original, cancellation)
                        .await.map_err(native_filesystem_error)?);
                }
                values
            }
        };
        drop(namespaces);
        accounting.release(namespace_bytes)?;
        Ok(found)
    }

    async fn exactify_inspect_with_accounting(
        &self, path: &str, read_view: Option<(&Self, LazySnapshotRef)>,
        state: &LazyWorkspaceState, authored_absent: bool,
        source_used: Option<&std::sync::atomic::AtomicBool>,
        accounting: &mut ExactifyCollectorWork<'_>, cancellation: &CancellationToken,
    ) -> Result<LazyReadonlyNode<ResolvedLazyLookup>, LazyWorkspaceError> {
        match &mut *accounting {
            ExactifyCollectorWork::Portable { .. } => {
                let response = if let Some((origin, snapshot)) = read_view {
                    origin.inspect_with(path, InspectBasis {
                        state: Some(state), authored_generation: Some(snapshot.authored_generation),
                        retain_pinned_observation: true, source_used, ..InspectBasis::default()
                    }, accounting.remaining()?, cancellation).await
                } else {
                    self.inspect_with(path, InspectBasis {
                        retain_pinned_observation: true, authored_absent, ..InspectBasis::default()
                    }, accounting.remaining()?, cancellation).await
                }.map_err(|error| accounting.nested_error(error))?;
                Ok(LazyReadonlyNode::Portable(accounting.accept(Ok(response))?))
            }
            #[cfg(not(target_arch = "wasm32"))]
            ExactifyCollectorWork::Native { work, original } => {
                let original = *original;
                in_heap_lazy::<_, LazyWorkspaceError, _>(work, || async {
                    if let Some((origin, snapshot)) = read_view {
                        let frozen = origin.inspect_at_snapshot_with_work(
                            snapshot, path, original, cancellation,
                            source_used.ok_or(LazyWorkspaceError::NativeLedgerMismatch)?,
                        ).await.map_err(|(error, allocation)| { drop(allocation); error })?;
                        let (resolved, NativeInspectState { reader, allocation, .. }) = frozen;
                        drop(reader);
                        return Ok(LazyReadonlyNode::Native(NativeOwnedCoreState::from_parts(
                            resolved, allocation,
                        )));
                    }
                    let current = self.state(&LazyTreapWork::native_with_work(original, cancellation)?).await?;
                    self.check_exactify_live_source(Some(state.source), current.as_state().source)?;
                    let head = self.workspace.head(
                        original, cancellation,
                    ).await.map_err(|error| native_workspace_error_pair((error, None)))?;
                    let reader = self.workspace.readonly_pinned_reader_with_native_work(
                        head.id(), original, cancellation,
                    ).await.map_err(native_workspace_error)?;
                    drop(head);
                    let (_, allocation) = original.allocations(
                        |_, _, _, _| Ok::<(), LazyWorkspaceError>(()),
                    )?;
                    let mut context = NativeInspectState {
                        reader, record: None, metadata: None, allocation,
                    };
                    let result = {
                        let mut backend = LazyInspectBackend::Native(&mut context);
                        let mut readonly = LazyReadonlyAccounting::Native { original, cancellation };
                        self.inspect_with_accounting(path, InspectBasis {
                            state: Some(current.as_state()), authored_absent,
                            retain_pinned_observation: true, source_used, ..InspectBasis::default()
                        }, &mut readonly, &mut backend).await
                    }.and_then(|resolved| {
                        self.check_exactify_live_source(Some(state.source), current.as_state().source)?;
                        Ok(resolved)
                    });
                    let NativeInspectState { reader, allocation, .. } = context;
                    drop(reader);
                    match result {
                        Ok(resolved) => Ok(LazyReadonlyNode::Native(
                            NativeOwnedCoreState::from_parts(resolved, allocation),
                        )),
                        Err(error) => { drop(allocation); Err(error) }
                    }
                }).await
            }
        }
    }

    async fn exactify_begin_with_accounting(
        &self, workspace: &Workspace<A, O>, generation: &crate::Generation<A, O>,
        key: IdempotencyKey, parent: Option<&str>, accounting: &mut ExactifyCollectorWork<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::Transaction<A, O>, LazyWorkspaceError> {
        match &mut *accounting {
            ExactifyCollectorWork::Portable { .. } => {
                let receipt = workspace.begin_pinned_transaction(
                    generation, key, accounting.remaining()?, cancellation,
                ).await.map_err(|failure| accounting.nested_error(measured_workspace_error(failure)))?;
                let mut transaction = accounting.accept(Ok(receipt))?;
                if let Some(parent) = parent {
                    let additional = transaction.create_dir_all(
                        parent, accounting.remaining()?, cancellation,
                    ).await.map(|receipt| receipt.work).map_err(|failure| accounting.nested_error(measured_workspace_error(failure)))?;
                    accounting.record(additional)?;
                }
                Ok(transaction)
            }
            #[cfg(not(target_arch = "wasm32"))]
            ExactifyCollectorWork::Native { original, .. } => {
                let mut transaction = workspace.begin_pinned_transaction(
                    generation, key, *original, cancellation,
                ).await.map_err(|error| native_workspace_error_pair((error, None)))?;
                if let Some(parent) = parent {
                    transaction.create_dir_all(parent, *original, cancellation)
                        .await.map_err(native_workspace_error)?;
                }
                Ok(transaction)
            }
        }
    }

    fn exactify_source_id_with_accounting(
        &self, node: &SourceNode, accounting: &mut ExactifyCollectorWork<'_>,
        cancellation: &CancellationToken,
    ) -> Result<FileId, LazyWorkspaceError> {
        match accounting {
            ExactifyCollectorWork::Portable { work, budget, .. } =>
                self.source_file_id(&node.file_identity, &mut crate::kernel::allocation::KernelAccounting::borrowed_standalone(
                    &mut crate::kernel::allocation::AllocationLedger::default(), work, *budget, cancellation)),
            #[cfg(not(target_arch = "wasm32"))]
            ExactifyCollectorWork::Native { original, .. } =>
                self.source_file_id(&node.file_identity, &mut crate::kernel::allocation::KernelAccounting::native(original, cancellation)),
        }
    }

    async fn exactify_flush_with_accounting(
        &self, transaction: &mut crate::Transaction<A, O>, pending: &mut Vec<AuthoredMutation>,
        pending_bytes: &mut u64, accounting: &mut ExactifyCollectorWork<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), LazyWorkspaceError> {
        match accounting {
            ExactifyCollectorWork::Portable { retained_bytes, work, budget } => {
                **work = self.flush_exactify_source_batch_with_accounting(
                    transaction, pending, pending_bytes, retained_bytes, **work, *budget, cancellation,
                    #[cfg(not(target_arch = "wasm32"))]
                    None,
                    #[cfg(not(target_arch = "wasm32"))]
                    None,
                ).await?;
                Ok(())
            }
            #[cfg(not(target_arch = "wasm32"))]
            ExactifyCollectorWork::Native { work, original } => {
                if pending.is_empty() { return Ok(()); }
                let mut allocation = work.take_native_owned(*pending_bytes)?
                    .ok_or(LazyWorkspaceError::NativeLedgerMismatch)?;
                let result = self.flush_exactify_source_batch_with_accounting(
                    transaction, pending, pending_bytes, &mut 0, WorkCounters::UNCHARGED,
                    WorkBudget::UNBOUNDED, cancellation, Some(original), Some(&mut allocation),
                ).await;
                if result.is_err() { drop(std::mem::take(pending)); }
                drop(allocation);
                result.map(|_| ())
            }
        }
    }

    /// Creates or replaces one complete authored file using the same candidate compiler as the frozen guarded writer.
    pub async fn write(
        &self, path: &str, bytes: Bytes,
    ) -> Result<TransactionCommit<A, O>, LazyWorkspaceError> {
        let cancellation = CancellationToken::new();
        let work = LazyTreapWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let mut callback = |_| async {
            Ok(OperationReceipt {
                value: (), work: WorkCounters::UNCHARGED,
            })
        };
        self.write_batch(None, std::iter::once(LazyWriteMutation::PutFile { path, bytes: bytes.into() }), IdempotencyKey::new(), crate::PublicationPermit::Unrestricted, &work, &mut callback).await.map(|(commit, _)| commit)
    }

    /// Publishes a frozen write batch with its upload receipt.
    #[allow(clippy::too_many_arguments)]
    pub async fn write_at_snapshot_guarded<'path, E, F, Fut, I>(
        &self, basis: LazySnapshotRef, mutations: I, key: IdempotencyKey,
        permit: crate::PublicationPermit, cancellation: &CancellationToken, before_mutation: &mut F,
    ) -> Result<(TransactionCommit<A, O>, Result<LazySnapshotRef, E>), E>
    where
        E: From<LazyWorkspaceError> + Send,
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
        I: IntoIterator<Item = LazyWriteMutation<'path>> + Send,
        I::IntoIter: Send,
    {
        let mut guard = |_| {
            let future = before_mutation();
            async move {
                future.await.map(|()| OperationReceipt { value: (), work: WorkCounters::UNCHARGED })
                    .map_err(crate::OperationFailure::before_work)
            }
        };
        self.write_at_snapshot_measured_guarded(
            basis, mutations, key, permit, WorkBudget::UNBOUNDED, cancellation, &mut guard,
        ).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    /// The same whole candidate with ordinary measured owner callbacks.
    #[allow(clippy::too_many_arguments)]
    pub async fn write_at_snapshot_measured_guarded<'path, E, F, Fut, I>(
        &self, basis: LazySnapshotRef, mutations: I, key: IdempotencyKey,
        permit: crate::PublicationPermit, budget: WorkBudget,
        cancellation: &CancellationToken, before_mutation: &mut F,
    ) -> crate::MeasuredResult<OperationReceipt<(
        TransactionCommit<A, O>, Result<LazySnapshotRef, E>,
    )>, E>
    where
        E: From<LazyWorkspaceError> + Send,
        F: FnMut(WorkBudget) -> Fut + Send,
        Fut: Future<Output = crate::MeasuredResult<OperationReceipt<()>, E>> + Send,
        I: IntoIterator<Item = LazyWriteMutation<'path>> + Send,
        I::IntoIter: Send,
    {
        let work = LazyTreapWork::new(budget, cancellation);
        let result = self.write_batch(Some(basis), mutations.into_iter(), key, permit, &work, before_mutation).await;
        work.measured(result, |error, spent| crate::OperationFailure::new(error, spent))
    }

    /// Original-native whole-candidate publication.
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::too_many_arguments)]
    pub async fn write_at_snapshot_with_native_work_guarded<'path, E, F, Fut, I>(
        self: &Arc<Self>, basis: LazySnapshotRef, mutations: I, key: IdempotencyKey,
        permit: crate::PublicationPermit, original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken, before_mutation: &mut F,
    ) -> Result<(TransactionCommit<A, O>, Result<LazySnapshotRef, E>), E>
    where
        E: From<LazyWorkspaceError> + Send + 'static,
        F: for<'work> FnMut(&'work crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        I: IntoIterator<Item = LazyWriteMutation<'path>> + Send,
        I::IntoIter: Send,
        A: 'static, O: 'static, S: 'static,
    {
        let work = LazyTreapWork::native_with_work(original, cancellation)?;
        let mut guard = self.write_native_guard(before_mutation, original, cancellation)?;
        self.write_batch(Some(basis), mutations.into_iter(), key, permit, &work, &mut guard).await
    }

    /// Resolves an authenticated original write from its durable publication.
    #[allow(clippy::too_many_arguments)]
    pub async fn original_write_snapshot_guarded<'path, E, F, Fut, I>(
        &self, basis: LazySnapshotRef, mutations: I, key: IdempotencyKey,
        cancellation: &CancellationToken, before_mutation: &mut F,
    ) -> Result<Option<(crate::Generation<A, O>, Result<LazySnapshotRef, E>)>, E>
    where
        E: From<LazyWorkspaceError> + Send,
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
        I: IntoIterator<Item = LazyWriteMutation<'path>> + Send,
        I::IntoIter: Send,
    {
        let mut guard = |_| {
            let future = before_mutation();
            async move {
                future.await.map(|()| OperationReceipt { value: (), work: WorkCounters::UNCHARGED })
                    .map_err(crate::OperationFailure::before_work)
            }
        };
        let work = LazyTreapWork::new(WorkBudget::UNBOUNDED, cancellation);
        self.resolve_original_write(basis, mutations.into_iter(), key, &work, &mut guard).await
    }

    /// Original-ledger lookup of the same immutable write proof.
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::too_many_arguments)]
    pub async fn original_write_snapshot_with_native_work_guarded<'path, E, F, Fut, I>(
        self: &Arc<Self>, basis: LazySnapshotRef, mutations: I, key: IdempotencyKey,
        original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken, before_mutation: &mut F,
    ) -> Result<Option<(crate::Generation<A, O>, Result<LazySnapshotRef, E>)>, E>
    where
        E: From<LazyWorkspaceError> + Send + 'static,
        F: for<'work> FnMut(&'work crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        I: IntoIterator<Item = LazyWriteMutation<'path>> + Send,
        I::IntoIter: Send,
        A: 'static, O: 'static, S: 'static,
    {
        let work = LazyTreapWork::native_with_work(original, cancellation)?;
        let mut guard = self.write_native_guard(before_mutation, original, cancellation)?;
        self.resolve_original_write(basis, mutations.into_iter(), key, &work, &mut guard).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn write_native_guard<F: Clone>(
        self: &Arc<Self>, callback: &F, original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<OwnedNativeWriteGuard<A, O, D, S, F>, LazyWorkspaceError> {
        original.admit(WorkCounters {
            bytes_copied: std::mem::size_of::<F>() as u64 + std::mem::size_of::<Arc<Self>>() as u64
                + std::mem::size_of::<crate::NativeMaterializationWork>() as u64
                + std::mem::size_of::<CancellationToken>() as u64,
            ..WorkCounters::UNCHARGED
        })?;
        Ok(OwnedNativeWriteGuard {
            lazy: Arc::clone(self), callback: callback.clone(),
            original: original.clone(), cancellation: cancellation.clone(),
        })
    }

    async fn write_original_record<E, F>(
        &self, key: IdempotencyKey, work: &LazyTreapWork<'_>,
        before_mutation: &mut F,
    ) -> Result<Option<LazyReadonlyNode<(crate::Generation<A, O>, crate::DurableCommit)>>, E>
    where
        E: From<LazyWorkspaceError> + Send,
        F: LazyWriteGuard<A, O, D, S, Error = E>,
    {
        let cancellation = work.cancellation;
        before_mutation.check(work).await?;
        lazy_dispatch!{work, accept_lazy_write_receipt(
                work, self.workspace.operation_generation_record_measured(
                    key, work.remaining()?, cancellation,
                ).await.map_err(|failure| failure.map_error(|error|
                    E::from(LazyWorkspaceError::WorkspaceCause(error)))),
            ).map(|value| value.map(LazyReadonlyNode::Portable)),
            |original| {
                let (value, allocation) = self.workspace.operation_generation_record_with_native_work(
                    key.operation_id(), original, cancellation,
                ).await.map_err(|error| E::from(native_workspace_error_pair(error)))?;
                Ok(match value {
                    Some(value) => Some(LazyReadonlyNode::Native(
                        NativeOwnedCoreState::from_parts(value, allocation),
                    )),
                    None => { drop(allocation); None }
                })
            }
        }
    }

    async fn resolve_original_write<'path, E, F, I>(
        &self, basis: LazySnapshotRef, mutations: I, key: IdempotencyKey,
        work: &LazyTreapWork<'_>, before_mutation: &mut F,
    ) -> Result<Option<(crate::Generation<A, O>, Result<LazySnapshotRef, E>)>, E>
    where
        E: From<LazyWorkspaceError> + Send,
        F: LazyWriteGuard<A, O, D, S, Error = E>,
        I: Iterator<Item = LazyWriteMutation<'path>> + Send,
    {
        in_heap_lazy(work, || async {
            let Some(original) = self.write_original_record(key, work, before_mutation).await? else { return Ok(None); };
            self.write_reconcile_original(basis, mutations, key, original, work, before_mutation).await.map(Some)
        }).await
    }

    async fn write_reconcile_original<'path, E, F, I>(
        &self, basis: LazySnapshotRef, mutations: I, key: IdempotencyKey,
        original: LazyReadonlyNode<(crate::Generation<A, O>, crate::DurableCommit)>, work: &LazyTreapWork<'_>,
        before_mutation: &mut F,
    ) -> Result<(crate::Generation<A, O>, Result<LazySnapshotRef, E>), E>
    where
        E: From<LazyWorkspaceError> + Send,
        F: LazyWriteGuard<A, O, D, S, Error = E>,
        I: Iterator<Item = LazyWriteMutation<'path>> + Send,
    {
        let authenticated = self.write_authenticate_original_record_with_accounting(&original, work);
        let generation = original.into_generation();
        let proof = async {
            let authenticated = authenticated?;
            let saved = authenticated.0;
            if saved.basis != basis {
                return Err(E::from(LazyWorkspaceError::WriteInputConflict { operation: key.operation_id() }));
            }
            self.validate_snapshot(saved.basis, work).await?;
            let mut hasher = lazy_write_input_hasher(work)?;
            let mut count = 0_u64;
            for mutation in mutations {
                let scope = TreapAllocationScope::new(work);
                let (path, bytes) = lazy_write_mutation_parts(mutation, work)?;
                let requested = work.canonical_path_owned(path, self.workspace.limits())?;
                lazy_write_hash_mutation(&mut hasher, &mut count, &requested, bytes.as_ref(), work)?;
                drop(bytes);
                drop(requested);
                drop(scope);
            }
            let input = lazy_write_finish_input(hasher, count, work)?;
            if saved.input != input {
                return Err(E::from(LazyWorkspaceError::WriteInputConflict { operation: key.operation_id() }));
            }
            self.write_settle_original(authenticated, work, before_mutation).await
        }.await;
        Ok((generation, proof))
    }

    async fn write_prepare_original<E, F>(
        &self, prepared: PendingLazyWrite, work: &LazyTreapWork<'_>,
        before_mutation: &mut F,
    ) -> Result<(), E>
    where
        E: From<LazyWorkspaceError> + Send,
        F: LazyWriteGuard<A, O, D, S, Error = E>,
    {
        let cancellation = work.cancellation;
        loop {
            self.write_owner(prepared.basis, work, before_mutation).await?;
            let state = self.load_binding_at(self.workspace.id(), work, cancellation).await?
                .ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?;
            if let Some(pending) = state.as_state().pending_write {
                if pending == prepared { return Ok(()); }
                return Err(E::from(if pending.operation == prepared.operation {
                    LazyWorkspaceError::WriteInputConflict { operation: prepared.operation }
                } else { LazyWorkspaceError::Concurrent }));
            }
            let revision = state.as_state().revision;
            work.charge(WorkCounters {
                bytes_copied: std::mem::size_of::<LazyWorkspaceState>() as u64,
                ..WorkCounters::UNCHARGED
            })?;
            let mut replacement = state.as_state().clone();
            replacement.revision = revision.checked_add(1).ok_or(LazyWorkspaceError::Concurrent)?;
            replacement.pending_write = Some(prepared);
            drop(state);
            self.write_owner(prepared.basis, work, before_mutation).await?;
            if self.swap_binding_at(self.workspace.id(), revision, replacement, work, cancellation).await? {
                return Ok(());
            }
        }
    }

    async fn write_prepared_basis(
        &self, prepared: PendingLazyWrite, work: &LazyTreapWork<'_>,
        ) -> Result<(), LazyWorkspaceError> {
        let cancellation = work.cancellation;
        self.write_basis(prepared.basis, work).await?;
        let state = self.load_binding_at(self.workspace.id(), work, cancellation).await?
            .ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?;
        if state.as_state().pending_write != Some(prepared) {
            return Err(LazyWorkspaceError::WriteIndeterminate { operation: prepared.operation });
        }
        Ok(())
    }

    fn write_authenticate_original_record_with_accounting(
        &self, original: &LazyReadonlyNode<(crate::Generation<A, O>, crate::DurableCommit)>, work: &LazyTreapWork<'_>,
    ) -> Result<(PendingLazyWrite, crate::GenerationId), LazyWorkspaceError> {
        let (generation, commit) = original.as_state();
        let operation = commit.operation_id;
        let authenticate = |accounting: &mut crate::kernel::allocation::KernelAccounting<'_>| {
            let publication = crate::kernel::decode_publication_evidence_with_accounting(
                &commit.payload, commit.payload.len() as u64, accounting,
            ).map_err(lazy_cursor_filesystem_error)?;
            let evidence = publication.original_evidence.ok_or(
                LazyWorkspaceError::WriteIndeterminate { operation },
            )?;
            let prepared = lazy_write_decode_original_with_accounting(evidence, operation, accounting)?;
            if prepared.config != self.workspace.volume_config()
                || prepared.basis.workspace_id != self.workspace.id()
                || publication.publication.volume_id != self.workspace.id().volume_id()
                || publication.publication.generation_root.digest != generation.id().digest()
            {
                return Err(LazyWorkspaceError::WriteInputConflict { operation });
            }
            let previous = commit.sequence.get().checked_sub(1)
                .ok_or(LazyWorkspaceError::WriteIndeterminate { operation })?;
            let context = lazy_write_context_with_accounting(prepared.basis, prepared.input, accounting)?;
            let context = crate::kernel::permit_context_with_accounting(
                Some(context), prepared.permit, accounting,
            ).map_err(lazy_cursor_filesystem_error)?
                .ok_or(LazyWorkspaceError::WriteIndeterminate { operation })?;
            let request = crate::kernel::PublishGenerationRequest {
                authority_id: self.workspace.authority_id(),
                volume_id: self.workspace.id().volume_id(),
                epoch: commit.epoch,
                expected: crate::Head {
                    epoch: commit.epoch, sequence: crate::Sequence::new(previous),
                    digest: commit.previous_digest,
                },
                operation_id: operation,
                generation_root: publication.publication.generation_root,
            };
            let expected = crate::kernel::contextual_publication_fingerprint_from_payload_with_accounting(
                request, context, &commit.payload, accounting,
            ).map_err(lazy_cursor_filesystem_error)?;
            if commit.fingerprint != expected {
                return Err(LazyWorkspaceError::WriteInputConflict { operation });
            }
            Ok((prepared, generation.id()))
        };
        work.with_kernel(|accounting, _| authenticate(accounting))
    }

    async fn write_settle_original<E, F>(
        &self, authenticated: (PendingLazyWrite, crate::GenerationId), work: &LazyTreapWork<'_>,
        before_mutation: &mut F,
    ) -> Result<LazySnapshotRef, E>
    where
        E: From<LazyWorkspaceError> + Send,
        F: LazyWriteGuard<A, O, D, S, Error = E>,
    {
        let cancellation = work.cancellation;
        let (prepared, generation) = authenticated;
        work.charge(WorkCounters {
            bytes_hashed: (LAZY_SNAPSHOT_DOMAIN.len() + 136) as u64,
            ..WorkCounters::UNCHARGED
        })?;
        let snapshot = LazySnapshotRef::new(
            prepared.basis.workspace_id, generation, prepared.basis.source,
            prepared.basis.overlay, prepared.basis.shadows,
        );
        self.validate_snapshot(snapshot, work).await?;
        loop {
            let state = self.load_binding_at(self.workspace.id(), work, cancellation).await?
                .ok_or(LazyWorkspaceError::WriteIndeterminate { operation: prepared.operation })?;
            if state.as_state().schema_version != LAZY_STATE_SCHEMA
                || state.as_state().workspace_id != prepared.basis.workspace_id
            {
                return Err(E::from(LazyWorkspaceError::WriteIndeterminate { operation: prepared.operation }));
            }
            if let Some(saved) = state.as_state().completed_write
                .filter(|saved| saved.prepared.operation == prepared.operation)
            {
                if saved.prepared != prepared || saved.snapshot != snapshot {
                    return Err(E::from(LazyWorkspaceError::WriteInputConflict { operation: prepared.operation }));
                }
                return Ok(saved.snapshot);
            }
            if state.as_state().pending_write != Some(prepared) {
                return Ok(snapshot);
            }
            let revision = state.as_state().revision;
            work.charge(WorkCounters {
                bytes_copied: std::mem::size_of::<LazyWorkspaceState>() as u64,
                ..WorkCounters::UNCHARGED
            })?;
            let mut replacement = state.as_state().clone();
            replacement.revision = revision.checked_add(1).ok_or(LazyWorkspaceError::Concurrent)?;
            replacement.pending_write = None;
            replacement.completed_write = Some(CompletedLazyWrite { prepared, snapshot });
            drop(state);
            // This CAS preserves concurrent roots and settles the pending write.
            before_mutation.check(work).await?;
            if self.swap_binding_at(self.workspace.id(), revision, replacement, work, cancellation).await? {
                return Ok(snapshot);
            }
        }
    }

    async fn write_abandon_original<E, F>(
        &self, prepared: PendingLazyWrite, work: &LazyTreapWork<'_>,
        before_mutation: &mut F,
    ) -> Result<(), E>
    where
        E: From<LazyWorkspaceError> + Send,
        F: LazyWriteGuard<A, O, D, S, Error = E>,
    {
        let cancellation = work.cancellation;
        loop {
            before_mutation.check(work).await?;
            let state = self.load_binding_at(self.workspace.id(), work, cancellation).await?
                .ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?;
            if state.as_state().pending_write != Some(prepared) { return Ok(()); }
            let revision = state.as_state().revision;
            work.charge(WorkCounters {
                bytes_copied: std::mem::size_of::<LazyWorkspaceState>() as u64,
                ..WorkCounters::UNCHARGED
            })?;
            let mut replacement = state.as_state().clone();
            replacement.revision = revision.checked_add(1).ok_or(LazyWorkspaceError::Concurrent)?;
            replacement.pending_write = None;
            drop(state);
            before_mutation.check(work).await?;
            if self.swap_binding_at(self.workspace.id(), revision, replacement, work, cancellation).await? {
                return Ok(());
            }
        }
    }

    async fn write_basis(
        &self, basis: LazySnapshotRef, work: &LazyTreapWork<'_>,
        ) -> Result<(), LazyWorkspaceError> {
        let cancellation = work.cancellation;
        self.validate_snapshot(basis, work).await?;
        let phase = self.load_binding_at(self.workspace.id(), work, cancellation).await?
            .ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?;
        let state = phase.as_state();
        if state.schema_version != LAZY_STATE_SCHEMA
            || state.workspace_id != basis.workspace_id || state.source != basis.source
            || state.overlay != basis.overlay || state.shadows != basis.shadows
            || state.pending_remove.is_some() || self.source.reference() != basis.source
        {
            return Err(LazyWorkspaceError::StaleIdentity);
        }
        Ok(())
    }

    async fn write_owner<E, F>(
        &self, basis: LazySnapshotRef, work: &LazyTreapWork<'_>,
        before_mutation: &mut F,
    ) -> Result<(), E>
    where
        E: From<LazyWorkspaceError> + Send,
        F: LazyWriteGuard<A, O, D, S, Error = E>,
    {
        self.write_basis(basis, work).await?;
        before_mutation.check(work).await?;
        self.write_basis(basis, work).await?;
        Ok(())
    }

    async fn write_batch<'path, E, F, I>(
        &self, basis: Option<LazySnapshotRef>, mutations: I,
        key: IdempotencyKey, permit: crate::PublicationPermit, work: &LazyTreapWork<'_>,
        before_mutation: &mut F,
    ) -> Result<(TransactionCommit<A, O>, Result<LazySnapshotRef, E>), E>
    where
        E: From<LazyWorkspaceError> + Send,
        F: LazyWriteGuard<A, O, D, S, Error = E>,
        I: Iterator<Item = LazyWriteMutation<'path>> + Send,
    {
        let cancellation = work.cancellation;
        in_heap_lazy(work, || async {
            let frozen_basis = basis.is_some();
            if let Some(basis) = basis
                && let Some(original) = self.write_original_record(key, work, before_mutation).await?
            {
                let (generation, proof) = self.write_reconcile_original(basis, mutations, key, original, work, before_mutation).await?;
                return Ok((TransactionCommit::AlreadyCommitted(generation), proof));
            }
            let mut input_hasher = if frozen_basis { Some(lazy_write_input_hasher(work)?) } else { None };
            let mut input_count = 0_u64;
            let mut captured = basis;
            let mut candidate = None;
            for mutation in mutations {
                let scope = TreapAllocationScope::new(work);
                let (path, bytes) = lazy_write_mutation_parts(mutation, work)?;
                let requested = work.canonical_path_owned(path, self.workspace.limits())?;
                if let Some(hasher) = &mut input_hasher {
                    lazy_write_hash_mutation(hasher, &mut input_count, &requested, bytes.as_ref(), work)?;
                }
                let ordinary_lookup = if !frozen_basis && bytes.is_some() {
                    match self.promotion_lookup(&requested, work).await {
                        Ok(lookup) => Some(lookup),
                        Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::NotFound) => None,
                        Err(error) => return Err(E::from(error)),
                    }
                } else { None };
                let basis = match captured {
                    Some(basis) => basis,
                    None => self.snapshot(work).await.map_err(E::from)?,
                };
                captured = Some(basis);
                self.write_basis(basis, work).await?;
                let frozen_lookup = if frozen_basis && bytes.is_some() {
                    match self.selected_lookup(basis, &requested, work).await {
                        Ok(lookup) => Some(lookup),
                        Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::NotFound) => None,
                        Err(error) => return Err(E::from(error)),
                    }
                } else { None };
                let lookup = ordinary_lookup.as_ref().map(|lookup| &lookup.as_state().lookup)
                    .or_else(|| frozen_lookup.as_ref().map(SelectedLookupPhase::lookup));
                if let Some(frozen) = &frozen_lookup {
                    let bytes = frozen.portable_owned_bytes();
                    if bytes != 0 { work.adopt_input(bytes)?; }
                }
                if candidate.is_none() {
                    let generation = lazy_dispatch!{work, accept_lazy_write_receipt(work,
                        self.workspace.generation(
                            basis.authored_generation, work.remaining()?, cancellation,
                        ).await.map_err(|failure| failure.map_error(|error|
                            E::from(LazyWorkspaceError::WorkspaceCause(error)))))?,
                        |original| self.workspace.generation(
                            basis.authored_generation, original, cancellation,
                        ).await.map_err(|error| native_workspace_error_pair((error, None)))?
                    };
                    candidate = Some(lazy_dispatch!{work, accept_lazy_write_receipt(work,
                        self.workspace.begin_pinned_transaction(
                            &generation, key, work.remaining()?, cancellation,
                        ).await.map_err(|failure| failure.map_error(|error|
                            E::from(LazyWorkspaceError::WorkspaceCause(error)))))?,
                        |original| self.workspace.begin_pinned_transaction(
                            &generation, key, original, cancellation,
                        ).await.map_err(|error| native_workspace_error_pair((error, None)))?
                    });
                    drop(generation);
                }
                let transaction = candidate.as_mut().ok_or(LazyWorkspaceError::Concurrent)?;
                let Some(bytes) = bytes else {
                    self.write_owner(basis, work, before_mutation).await?;
                    self.promotion_mutation_with_accounting(
                        transaction, &requested, LazyPromotionMutation::Parents, work, cancellation.clone(),
                    ).await?;
                    drop(frozen_lookup);
                    drop(ordinary_lookup);
                    drop(requested);
                    drop(scope);
                    continue;
                };
                if let Some(parent) = parent_path(&requested) {
                    self.write_owner(basis, work, before_mutation).await?;
                    self.promotion_mutation_with_accounting(
                        transaction, parent, LazyPromotionMutation::Parents, work, cancellation.clone(),
                    ).await?;
                }
                let identity = match lookup {
                    Some(LazyLookup::Source(node)) => Some(
                        work.with_kernel(|accounting, _| self.source_file_id(&node.file_identity, accounting))?),
                    Some(LazyLookup::Authored { path: authored, .. }) if authored != &requested => {
                        self.write_owner(basis, work, before_mutation).await?;
                        self.promotion_mutation_with_accounting(
                            transaction, &requested, LazyPromotionMutation::HardLink(authored), work, cancellation.clone(),
                        ).await?;
                        None
                    }
                    Some(LazyLookup::Shadow { record, .. }) => {
                        self.write_owner(basis, work, before_mutation).await?;
                        self.promotion_mutation_with_accounting(
                            transaction, &requested, LazyPromotionMutation::Restore(*record), work, cancellation.clone(),
                        ).await?;
                        None
                    }
                    _ => None,
                };
                self.write_owner(basis, work, before_mutation).await?;
                lazy_dispatch!{work, {
                        accept_lazy_write_receipt(work, transaction.write(
                            &requested, bytes, work.remaining()?, cancellation,
                        ).await
                            .map_err(|failure| failure.map_error(|error|
                                E::from(LazyWorkspaceError::WorkspaceCause(error)))))?;
                    },
                    |original| transaction.write(&requested, bytes, original, cancellation)
                    .await.map_err(|error| native_workspace_error_pair((error, None)))?
                }
                if let Some(identity) = identity {
                    self.write_owner(basis, work, before_mutation).await?;
                    self.promotion_mutation_with_accounting(
                        transaction, &requested, LazyPromotionMutation::Identity(identity), work, cancellation.clone(),
                    ).await?;
                }
                drop(frozen_lookup);
                drop(ordinary_lookup);
                drop(requested);
                drop(scope);
            }
            let basis = captured.ok_or(LazyWorkspaceError::WorkspaceCause(WorkspaceError::EmptyContentSet))?;
            let transaction = candidate.ok_or(LazyWorkspaceError::WorkspaceCause(WorkspaceError::EmptyContentSet))?;
            let evidence_scope = TreapAllocationScope::new(work);
            let mut operation_evidence = None;
            let mut context = None;
            let prepared = match input_hasher {
                Some(hasher) => {
                    let prepared = PendingLazyWrite {
                        operation: key.operation_id(), basis,
                        input: lazy_write_finish_input(hasher, input_count, work)?,
                        config: self.workspace.volume_config(), permit,
                    };
                    context = Some(lazy_write_context(basis, prepared.input, work)?);
                    operation_evidence = Some(lazy_write_original_bytes(prepared, work)?);
                    self.write_prepare_original(prepared, work, before_mutation).await?;
                    Some(prepared)
                }
                None => None,
            };
            let commit = before_mutation.commit(
                self, transaction, context, operation_evidence, basis, prepared,
                key.operation_id(), permit, work, cancellation,
            ).await?;
            let snapshot = match &commit {
                TransactionCommit::Committed(generation) | TransactionCommit::AlreadyCommitted(generation) => {
                    if let Some(prepared) = prepared {
                        async {
                            let generation = generation.id();
                            let original = self.write_original_record(IdempotencyKey::from_bytes(prepared.operation.into_bytes()), work, before_mutation).await?.ok_or(LazyWorkspaceError::WriteIndeterminate { operation: prepared.operation })?;
                            let authenticated = self.write_authenticate_original_record_with_accounting(&original, work)?;
                            drop(original);
                            if authenticated.0 != prepared || authenticated.1 != generation {
                                return Err(E::from(LazyWorkspaceError::WriteInputConflict { operation: prepared.operation }));
                            }
                            self.validate_snapshot(prepared.basis, work).await?;
                            self.write_settle_original(authenticated, work, before_mutation).await
                        }.await
                    } else {
                        async {
                            self.write_basis(basis, work).await?;
                            work.charge(WorkCounters {
                                bytes_hashed: (LAZY_SNAPSHOT_DOMAIN.len() + 136) as u64,
                                ..WorkCounters::UNCHARGED
                            })?;
                            let snapshot = LazySnapshotRef::new(
                                basis.workspace_id, generation.id(), basis.source, basis.overlay, basis.shadows,
                            );
                            self.validate_snapshot(snapshot, work).await?;
                            Ok(snapshot)
                        }.await
                    }
                }
                _ => {
                    if let Some(prepared) = prepared {
                        self.write_abandon_original(prepared, work, before_mutation).await?;
                    }
                    Err(E::from(LazyWorkspaceError::StaleIdentity))
                }
            };
            drop(evidence_scope);
            Ok((commit, snapshot))
        }).await
    }

    #[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
    pub(crate) async fn publication_cut(
        &self, operation: Option<crate::OperationId>, original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<LazyPublicationCut, LazyWorkspaceError> {
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        let work = LazyTreapWork::native_with_work(original, cancellation)?;
        in_heap_lazy::<_, LazyWorkspaceError, _>(&work, || async {
            let phase = self.load_binding_at(self.workspace.id(), &work, cancellation).await?
                .ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?;
            let state = phase.as_state();
            state.require_ready_fork()?;
            if state.schema_version != LAZY_STATE_SCHEMA || state.workspace_id != self.workspace.id()
                || state.source != self.source.reference()
            {
                return Err(LazyWorkspaceError::StaleIdentity);
            }
            match (operation, state.pending_remove.as_ref()) {
                (Some(operation), Some(pending))
                    if state.overlay == pending.prepared_overlay
                        && matches!(pending.kind, PendingLazyRemoveKind::Mounted { idempotency_key }
                            if idempotency_key.operation_id() == operation) => {}
                (None, None) => {}
                _ => return Err(LazyWorkspaceError::OriginalSnapshotUnavailable),
            }
            let copied = WorkCounters {
                bytes_copied: std::mem::size_of::<LazyPublicationCut>() as u64, ..WorkCounters::UNCHARGED
            };
            original.admit(copied)?;
            let cut = LazyPublicationCut {
                workspace_id: state.workspace_id, config: self.workspace.volume_config(),
                source: state.source, overlay: state.overlay, shadows: state.shadows,
            };
            original.spent(copied)?;
            drop(phase);
            work.charge(WorkCounters::UNCHARGED)?;
            let overlay = self.store.load_lazy_overlay_with_native_work(cut.overlay, original).await?;
            let present = overlay.is_some();
            drop(overlay);
            if !present { return Err(LazyWorkspaceError::OriginalSnapshotUnavailable); }
            work.charge(WorkCounters::UNCHARGED)?;
            let shadows = self.store.load_lazy_shadow_with_native_work(cut.shadows, original).await?;
            let present = shadows.is_some();
            drop(shadows);
            if !present { return Err(LazyWorkspaceError::OriginalSnapshotUnavailable); }
            work.charge(WorkCounters::UNCHARGED)?;
            if self.source.reference() != cut.source { return Err(LazyWorkspaceError::StaleSource); }
            Ok(cut)
        }).await
    }

    pub(crate) async fn prepare_mount_removals(
        &self, paths: &[String], selected_paths: &[String],
        identity_records: &[(FileRecord, FileMetadata)],
        idempotency_key: IdempotencyKey, work: &LazyTreapWork<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let cancellation = work.cancellation;
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            let phase = self.load_binding_at(self.workspace.id(), work, cancellation).await?
                .ok_or(LazyWorkspaceError::StaleIdentity)?;
            let state = phase.as_state();
            if state.schema_version != LAZY_STATE_SCHEMA
                || state.workspace_id != self.workspace.id()
                || state.source != self.source.reference()
            {
                return Err(LazyWorkspaceError::StaleSource);
            }
            if let Some(pending) = state.pending_remove.as_ref() {
                return match pending.kind {
                    PendingLazyRemoveKind::Mounted { idempotency_key: original }
                        if original == idempotency_key && state.overlay == pending.prepared_overlay => Ok(()),
                    _ => Err(LazyWorkspaceError::Concurrent),
                };
            }
            let mut overlay = state.overlay;
            let mut shadows = state.shadows;
            for path in paths.iter().chain(selected_paths) {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                let path = work.canonical_path_owned(path, self.workspace.limits())?;
                let fact = match &work.meter {
                    LazyTreapMeter::Standalone { .. } => {
                        let mut readonly = LazyReadonlyAccounting::Portable {
                            work: WorkCounters::UNCHARGED, budget: work.remaining()?, cancellation,
                        };
                        let result = self.overlay_fact(state.overlay, &path, &mut readonly).await;
                        work.accept(readonly.finish_measured(result))?
                    },
                    #[cfg(not(target_arch = "wasm32"))]
                    LazyTreapMeter::NativeOriginal { original, .. } => {
                        let mut readonly = LazyReadonlyAccounting::Native { original, cancellation };
                        self.overlay_fact(state.overlay, &path, &mut readonly).await?
                    }
                };
                let removed = match fact {
                    Some(LazyOverlayChange::Observe { node, .. }) => Some(node.version),
                    Some(LazyOverlayChange::Tombstone { removed }) => removed,
                    None => {
                        let lookup = match &work.meter {
                            LazyTreapMeter::Standalone { .. } => work.accept(self.source_request_measured(
                                &path, work.remaining()?, cancellation,
                                |namespace, remaining| async move {
                                    self.source.lookup(state.source, &namespace, remaining, cancellation).await
                                },
                            ).await),
                            #[cfg(not(target_arch = "wasm32"))]
                            LazyTreapMeter::NativeOriginal { original, scratch } => {
                                let native = original;
                                let result = self.source_request_with_work(
                                    &path, state.source, original, cancellation, |namespace| async move {
                                        self.source.lookup(
                                            state.source, &namespace, native, cancellation).await
                                    },
                                ).await;
                                match result {
                                    Ok((node, mut allocation)) => {
                                        scratch.lock().expect("lazy treap scratch lock poisoned").absorb(&mut allocation)?;
                                        Ok(node)
                                    }
                                    Err((error, allocation)) => {
                                        if let Some(mut allocation) = allocation {
                                            scratch.lock().expect("lazy treap scratch lock poisoned").absorb(&mut allocation)?;
                                        }
                                        Err(error)
                                    }
                                }
                            }
                        };
                        match lookup {
                            Ok(node) => node.map(|node| node.version),
                            Err(error) if matches!(error.semantic_error(),
                                LazyWorkspaceError::Demand(DemandError::Absent | DemandError::NotDirectory)) => None,
                            Err(error) => return Err(error),
                        }
                    }
                };
                work.transfer_to_treap(crate::foundation::usize_to_u64(path.capacity()))?;
                overlay = insert_immutable_treap(
                    &OverlayTreap(&self.store, Some(work)), overlay,
                    path, LazyOverlayChange::Tombstone { removed },
                ).await?;
            }
            for (record, metadata) in identity_records {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                let _hold = lazy_dispatch!{work, work.accept(self.workspace.volume.fs.make_records_durable(
                        self.workspace.volume.config, std::slice::from_ref(record), work.remaining()?, cancellation,
                    ).await.map_err(|failure| lazy_operation_failure(failure, |error| workspace_error(WorkspaceError::engine(error)))))?,
                    |original| {
                        self.workspace.volume.fs.make_records_durable_with_native_work(
                            self.workspace.volume.config, std::slice::from_ref(record), original, cancellation,
                        ).await.map_err(|error| native_workspace_error(error.into()))?
                    }
                };
                let encoded = match &work.meter {
                    LazyTreapMeter::Standalone { .. } => {
                        let encoded = work.accept(crate::kernel::encode::<crate::kernel::EncodeError>(work.remaining()?, Some(cancellation), 0, |observe| crate::kernel::encode_file_record(*record, observe, crate::kernel::EncodeError::AllocationFailed)).map_err(|failure| lazy_operation_failure(
                            failure, |error| LazyWorkspaceError::Store(error.to_string()),
                        )))?;
                        work.adopt(crate::foundation::usize_to_u64(encoded.capacity()))?;
                        encoded
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    LazyTreapMeter::NativeOriginal { original, scratch } => {
                        let mut accounting = crate::kernel::allocation::KernelAccounting::native(original, cancellation);
                        let (encoded, allocation) = crate::kernel::codec::encode_owned(&mut accounting, |observe, allocation_error| crate::kernel::encode_file_record(*record, observe, allocation_error)).map_err(native_filesystem_error)?;
                        let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(mut allocation) = allocation else {
                            drop(encoded);
                            return Err(LazyWorkspaceError::NativeLedgerMismatch);
                        };
                        if let Err(error) = scratch.lock().expect("lazy treap scratch lock poisoned").absorb(&mut allocation) {
                            drop(encoded);
                            return Err(error.into());
                        }
                        encoded
                    }
                };
                let record_bytes = crate::foundation::usize_to_u64(encoded.capacity());
                let metadata_bytes = crate::foundation::usize_to_u64(
                    std::mem::size_of::<crate::WorkspaceMetadata>(),
                );
                work.allocate(metadata_bytes)?;
                let value = ShadowValue {
                    record: encoded,
                    metadata: Box::new(crate::WorkspaceMetadata::from_engine(*metadata)),
                };
                work.transfer_to_treap(record_bytes.checked_add(metadata_bytes)
                    .ok_or(crate::WorkError::Overflow)?)?;
                shadows = insert_immutable_treap(
                    &ShadowTreap(&self.store, Some(work)), shadows, record.file_id, value,
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
            if self.swap_binding_at(self.workspace.id(), state.revision, prepared, work, cancellation).await? {
                return Ok(());
            }
        }
        Err(LazyWorkspaceError::Concurrent)
    }

    pub(crate) async fn mount_removal_publication(
        &self, work: &LazyTreapWork<'_>,
    ) -> Result<Option<bool>, LazyWorkspaceError> {
        let cancellation = work.cancellation;
        let phase = self.load_binding_at(self.workspace.id(), work, cancellation).await?
            .ok_or(LazyWorkspaceError::StaleSource)?;
        let Some(PendingLazyRemove {
            kind: PendingLazyRemoveKind::Mounted { idempotency_key }, ..
        }) = phase.as_state().pending_remove.as_ref() else { return Ok(None); };
        Ok(Some(self.operation_generation(*idempotency_key, work).await?.is_some()))
    }

    pub(crate) async fn finish_mount_removals(
        &self, work: &LazyTreapWork<'_>,
    ) -> Result<bool, LazyWorkspaceError> {
        let Some(committed) = self.mount_removal_publication(work).await? else { return Ok(false); };
        self.recover_pending_remove(work).await?;
        Ok(committed)
    }

    /// Source paths this view deleted, sorted, each with the source version it deleted.
    pub async fn source_tombstones(
        &self,
        maximum: usize,
    ) -> Result<Vec<(String, SourceVersion)>, LazyWorkspaceError> {
        let state = self.inspect_state(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("ordinary inspection uses ordinary funding") })?;
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

    /// Hides a path this view only reads from the source and never owned (one an ancestor gained after the fork): it reads as absent, but is no deletion of this view's, so no merge carries it anywhere.
    pub async fn hide_source_path(&self, path: &str) -> Result<(), LazyWorkspaceError> {
        self.remove_recording(path, None, Removal::Hidden).await
    }

    /// The source version the current source holds at `path`, bypassing this view's own observations; `None` when the source has no node there.
    pub async fn current_source_version(
        &self,
        path: &str,
    ) -> Result<Option<SourceVersion>, LazyWorkspaceError> {
        let path = self.namespace_path(path)?;
        let state = self.inspect_state(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("ordinary inspection uses ordinary funding") })?;
        self.source.lookup(state.source, &path, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value.map(|node| node.version))
            .or_else(|failure| match failure.error {
                DemandError::Absent | DemandError::NotDirectory => Ok(None),
                error => Err(LazyWorkspaceError::from(error)),
            })
    }

    /// The source version removing `path` deletes: this view's observation of it, or else the source's current node.
    async fn removed_source_version(
        &self,
        state: &LazyWorkspaceState,
        path: &str,
    ) -> Result<Option<SourceVersion>, LazyWorkspaceError> {
        let cancellation = CancellationToken::new();
        let mut accounting = LazyReadonlyAccounting::Portable {
            work: WorkCounters::UNCHARGED, budget: WorkBudget::UNBOUNDED, cancellation: &cancellation,
        };
        let result = self.overlay_fact(state.overlay, path, &mut accounting).await;
        match accounting.finish_measured(result)?.value {
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

    #[allow(clippy::too_many_lines)]
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
            let cancellation = CancellationToken::new();
            let work = LazyTreapWork::new(WorkBudget::UNBOUNDED, &cancellation);
            self.promote_exact(&path, stat.file_id, u64::MAX, IdempotencyKey::new(), &work)
                .await.map_err(|error| error.with_prior_work(work.spent()))?;
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
            let state = self.inspect_state(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("ordinary inspection uses ordinary funding") })?;
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
                .workspace.begin_transaction(idempotency_key, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
                .map_err(workspace_error)?;
            match transaction.remove_if(&path, actual).await {
                Ok(()) => transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error),
                Err(error) => Err(error),
            }
        } else {
            Ok(TransactionCommit::AlreadyCommitted(
                self.workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(workspace_error)?,
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

    async fn inspect_state(
        &self, work: &LazyTreapWork<'_>,
    ) -> Result<LazyReadonlyNode<LazyWorkspaceState>, LazyWorkspaceError> {
        let (phase, source) = self.ready_state(work).await?;
        if phase.as_state().source != source { return Err(LazyWorkspaceError::StaleSource); }
        if phase.as_state().pending_remove.is_some() { return Err(LazyWorkspaceError::Concurrent); }
        Ok(phase)
    }

    /// Reads the ready companion, rebinding an advanced source epoch when needed.
    pub(crate) async fn state(
        &self, work: &LazyTreapWork<'_>,
    ) -> Result<LazyReadonlyNode<LazyWorkspaceState>, LazyWorkspaceError> {
        let (phase, source) = self.ready_state(work).await?;
        if phase.as_state().pending_remove.is_some() { return Err(LazyWorkspaceError::Concurrent); }
        if phase.as_state().source == source { return Ok(phase); }
        drop(phase);
        self.rebind_source(work).await
    }

    async fn ready_state(
        &self, work: &LazyTreapWork<'_>,
    ) -> Result<(LazyReadonlyNode<LazyWorkspaceState>, SourceReference), LazyWorkspaceError> {
        #[cfg(not(target_arch = "wasm32"))]
        if matches!(work.meter, LazyTreapMeter::NativeOriginal { .. }) {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        }
        let phase = self.load_binding_at(self.workspace.id(), work, work.cancellation).await?
            .ok_or_else(|| LazyWorkspaceError::Store("lazy binding is absent".to_owned()))?;
        let state = phase.as_state();
        state.require_ready_fork()?;
        let source = self.source.reference();
        if state.schema_version != LAZY_STATE_SCHEMA || state.workspace_id != self.workspace.id()
            || state.source.identity != source.identity || state.source.epoch > source.epoch
        {
            return Err(LazyWorkspaceError::StaleSource);
        }
        Ok((phase, source))
    }

    async fn recover_pending_remove(
        &self, work: &LazyTreapWork<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let cancellation = work.cancellation;
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let phase = self.load_binding_at(self.workspace.id(), work, cancellation).await?
                .ok_or(LazyWorkspaceError::StaleSource)?;
            let state = phase.as_state();
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
                | PendingLazyRemoveKind::Mounted { idempotency_key } => self.operation_generation(idempotency_key, work)
                    .await?.is_some(),
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
            if self.swap_binding_at(self.workspace.id(), state.revision, replacement, work, cancellation).await? {
                return Ok(());
            }
        }
        Err(LazyWorkspaceError::Concurrent)
    }

    async fn observe(
        &self, path: &str, state: &LazyWorkspaceState,
        work: &LazyTreapWork<'_>,
    ) -> Result<(SourceReference, SourceNode), LazyWorkspaceError> {
        let cancellation = work.cancellation;
        let _scope = TreapAllocationScope::new(work);
        let path = work.canonical_path_owned(path, self.workspace.limits())?;
        let fact = match &work.meter {
            LazyTreapMeter::Standalone { .. } => {
                let mut accounting = LazyReadonlyAccounting::Portable {
                    work: WorkCounters::UNCHARGED, budget: work.remaining()?, cancellation,
                };
                let result = self.overlay_fact(state.overlay, &path, &mut accounting).await;
                work.accept(accounting.finish_measured(result))?
            },
            #[cfg(not(target_arch = "wasm32"))]
            LazyTreapMeter::NativeOriginal { original, .. } => {
                if self.source.reference() != state.source {
                    return Err(LazyWorkspaceError::StaleSource);
                }
                let mut accounting = LazyReadonlyAccounting::Native { original, cancellation };
                self.overlay_fact(state.overlay, &path, &mut accounting).await?
            }
        };
        match fact {
            Some(LazyOverlayChange::Observe { source, node }) if source == state.source => {
                #[cfg(not(target_arch = "wasm32"))]
                if matches!(&work.meter, LazyTreapMeter::NativeOriginal { .. }) {
                    let bytes = crate::foundation::usize_to_u64(path.capacity());
                    drop(path);
                    work.release(bytes)?;
                }
                return Ok((source, node));
            }
            Some(LazyOverlayChange::Tombstone { .. }) => return Err(LazyWorkspaceError::NotFound),
            Some(LazyOverlayChange::Observe { .. }) | None => {}
        }
        let node = lazy_dispatch!{work, {
                let prior_live = work.live_bytes();
                let namespace = work.namespace_owned(
                    &path, self.workspace.profile(), self.workspace.limits(),
                )?;
                let namespace_bytes = work.live_bytes().checked_sub(prior_live)
                    .ok_or(crate::WorkError::Overflow)?;
                let node = work.accept(self.source.lookup(
                    state.source, &namespace, work.remaining()?, cancellation,
                ).await.map_err(|failure| lazy_operation_failure(failure, LazyWorkspaceError::from)))?;
                drop(namespace);
                work.release(namespace_bytes)?;
                node
            },
            |original| {
                match self.source_request_with_work(
                    &path, state.source, original, cancellation, |namespace| async move {
                        self.source.lookup(
                            state.source, &namespace, original, cancellation).await
                    },
                ).await {
                    Ok((node, allocation)) => {
                        drop(allocation);
                        node
                    }
                    Err((error, allocation)) => {
                        drop(allocation);
                        return Err(error);
                    }
                }
            }
        };
        let node = node.ok_or(LazyWorkspaceError::NotFound)?;
        let change = LazyOverlayChange::Observe { source: state.source, node };
        let result = self.append_overlay(&path, change, work, cancellation).await;
        let bytes = crate::foundation::usize_to_u64(path.capacity());
        drop(path);
        let released = work.release(bytes);
        result?;
        released?;
        Ok((state.source, node))
    }

    async fn append_overlay(
        &self, path: &str, change: LazyOverlayChange,
        work: &LazyTreapWork<'_>, cancellation: &CancellationToken,
    ) -> Result<(), LazyWorkspaceError> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let phase = self.state(work).await?;
            let state = phase.as_state();
            #[cfg(not(target_arch = "wasm32"))]
            if let LazyTreapMeter::NativeOriginal { .. } = &work.meter
                && let LazyOverlayChange::Observe { source, .. } = &change
            {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                if state.source != *source || self.source.reference() != *source {
                    return Err(LazyWorkspaceError::StaleSource);
                }
            }
            let copied_path = work.canonical_path_owned(&path, self.workspace.limits())?;
            work.transfer_to_treap(crate::foundation::usize_to_u64(copied_path.capacity()))?;
            let overlay_id = insert_immutable_treap(
                &OverlayTreap(&self.store, Some(work)), state.overlay,
                copied_path, change.clone(),
            ).await?;
            let replacement = LazyWorkspaceState {
                revision: state.revision.saturating_add(1),
                overlay: overlay_id,
                ..state.clone()
            };
            #[cfg(not(target_arch = "wasm32"))]
            if let LazyTreapMeter::NativeOriginal { .. } = &work.meter
                && let LazyOverlayChange::Observe { source, .. } = &change
            {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                if self.source.reference() != *source {
                    return Err(LazyWorkspaceError::StaleSource);
                }
            }
            if self.swap_binding_at(self.workspace.id(), state.revision, replacement, work, cancellation).await? {
                return Ok(());
            }
        }
        Err(LazyWorkspaceError::Concurrent)
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

    async fn load_overlay_with_readonly_accounting(
        &self, root: LazyOverlayId, accounting: &mut LazyReadonlyAccounting<'_>,
    ) -> Result<Option<LazyReadonlyNode<LazyOverlay>>, LazyWorkspaceError> {
        accounting.check()?;
        match accounting {
            LazyReadonlyAccounting::Portable { work, budget, cancellation } => {
                let receipt = self.store.load_lazy_overlay_measured(
                    root, remaining_work(*work, *budget, 0)?, cancellation,
                ).await?;
                *work = account_work(*work, receipt.work, *budget)?;
                Ok(receipt.value.map(LazyReadonlyNode::Portable))
            }
            #[cfg(not(target_arch = "wasm32"))]
            LazyReadonlyAccounting::Native { original, .. } => {
                let node = self.store.load_lazy_overlay_with_native_work(root, original).await?
                    .ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?;
                Ok(Some(LazyReadonlyNode::Native(node)))
            }
        }
    }

    async fn overlay_fact(
        &self, mut root: LazyOverlayId, path: &str,
        accounting: &mut LazyReadonlyAccounting<'_>,
    ) -> Result<Option<LazyOverlayChange>, LazyWorkspaceError> {
        loop {
            let Some(node) = self.load_overlay_with_readonly_accounting(root, accounting).await? else {
                return Ok(None);
            };
            let LazyOverlay::Node { path: node_path, change, left, right, .. } = node.as_state()
                else { return Ok(None); };
            match path.cmp(node_path.as_str()) {
                std::cmp::Ordering::Equal => return Ok(Some(change.clone())),
                std::cmp::Ordering::Less => root = *left,
                std::cmp::Ordering::Greater => root = *right,
            }
        }
    }

    async fn overlay_has_beneath(
        &self, mut root: LazyOverlayId, directory: &str,
        accounting: &mut LazyReadonlyAccounting<'_>,
    ) -> Result<bool, LazyWorkspaceError> {
        accounting.check()?;
        accounting.validate_path(directory, self.workspace.limits())?;
        let prefix = || directory.bytes().chain((directory != "/").then_some(b'/'));
        let mut first_after = false;
        loop {
            let Some(node) = self.load_overlay_with_readonly_accounting(root, accounting).await?
                else { break; };
            let LazyOverlay::Node { path, left, right, .. } = node.as_state()
                else { break; };
            if path.bytes().cmp(prefix()) != std::cmp::Ordering::Less {
                first_after = path.strip_prefix(directory).is_some_and(|suffix|
                    directory == "/" || suffix.starts_with('/'));
                root = *left;
            } else {
                root = *right;
            }
        }
        Ok(first_after)
    }

    async fn load_shadow_with_readonly_accounting(
        &self, root: LazyShadowId, accounting: &mut LazyReadonlyAccounting<'_>,
    ) -> Result<Option<LazyReadonlyNode<LazyShadow>>, LazyWorkspaceError> {
        accounting.check()?;
        match accounting {
            LazyReadonlyAccounting::Portable { work, budget, cancellation } => {
                let receipt = self.store.load_lazy_shadow_measured(
                    root, remaining_work(*work, *budget, 0)?, cancellation,
                ).await?;
                *work = account_work(*work, receipt.work, *budget)?;
                Ok(receipt.value.map(LazyReadonlyNode::Portable))
            }
            #[cfg(not(target_arch = "wasm32"))]
            LazyReadonlyAccounting::Native { original, .. } => {
                let node = self.store.load_lazy_shadow_with_native_work(root, original).await?
                    .ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?;
                Ok(Some(LazyReadonlyNode::Native(node)))
            }
        }
    }

    async fn shadow_record(
        &self, mut root: LazyShadowId, file_id: FileId,
        accounting: &mut LazyReadonlyAccounting<'_>,
    ) -> Result<Option<(FileRecord, crate::WorkspaceMetadata)>, LazyWorkspaceError> {
        loop {
            let Some(node) = self.load_shadow_with_readonly_accounting(root, accounting).await? else {
                return Ok(None);
            };
            let LazyShadow::Node { file_id: node_file_id, record, metadata, left, right, .. } =
                node.as_state() else { return Ok(None); };
            match file_id.cmp(node_file_id) {
                std::cmp::Ordering::Equal => {
                    accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                    let record = crate::kernel::decode_file_record(record)
                        .map_err(|error| accounting.decode_error(error))?;
                    return Ok(Some((record, **metadata)));
                }
                std::cmp::Ordering::Less => root = *left,
                std::cmp::Ordering::Greater => root = *right,
            }
        }
    }

    fn insert_shadow<'a>(
        &'a self,
        root: LazyShadowId,
        file_id: FileId,
        record: FileRecord,
        metadata: crate::WorkspaceMetadata,
    ) -> LazyFuture<'a, Result<LazyShadowId, LazyWorkspaceError>> {
        Box::pin(async move {
            let _hold = self.workspace.volume.fs.make_records_durable(self.workspace.volume.config, std::slice::from_ref(&record), WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await.map(|receipt| receipt.value)
                .map_err(|failure| workspace_error(WorkspaceError::engine(failure.error)))?;
            insert_immutable_treap(
                &ShadowTreap(&self.store, None),
                root,
                file_id,
                ShadowValue {
                    record: crate::kernel::encode_file_record::<crate::kernel::EncodeError>(record, |_| Ok(()), crate::kernel::EncodeError::AllocationFailed)
                        .map_err(|error| LazyWorkspaceError::Store(error.to_string()))?,
                    metadata: Box::new(metadata),
                },
            )
            .await
        })
    }

    async fn tombstoned(
        &self, overlay_id: LazyOverlayId, path: &str,
        accounting: &mut LazyReadonlyAccounting<'_>,
    ) -> Result<bool, LazyWorkspaceError> {
        accounting.validate_path(path, self.workspace.limits())?;
        let root = self.overlay_fact(overlay_id, "/", accounting).await?;
        if matches!(root, Some(LazyOverlayChange::Tombstone { .. })) {
            return Ok(true);
        }
        for end in path.match_indices('/').map(|(index, _)| index).skip(1)
            .chain(std::iter::once(path.len())).filter(|end| *end > 1)
        {
            let fact = self.overlay_fact(overlay_id, &path[..end], accounting).await?;
            if matches!(fact, Some(LazyOverlayChange::Tombstone { .. })) {
                return Ok(true);
            }
        }
        Ok(false)
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

    #[cfg(not(target_arch = "wasm32"))]
    fn source_namespace_with_work(
        &self, path: &str, original: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(NamespacePath, crate::materializer::NativeAllocation), LazyWorkspaceError> {
        original.admit(WorkCounters::UNCHARGED)?;
        cancellation.check().map_err(|_| LazyWorkspaceError::NativeWork(
            crate::materializer::NativeTreeMaterializationError::Cancelled))?;
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(original, cancellation);
        let (namespace, allocation) = NamespacePath::parse_in_profile_with_accounting(
            path, self.workspace.profile(), self.workspace.limits(), &mut accounting, cancellation,
        ).map_err(native_filesystem_error)?;
        match allocation {
            crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) =>
                Ok((namespace, allocation)),
            crate::kernel::allocation::KernelOwnedAllocation::Standalone => {
                drop(namespace);
                Err(LazyWorkspaceError::NativeWork(
                    crate::NativeTreeMaterializationError::InvalidLayout))
            }
        }
    }

    /// The returned guard covers request metadata only.
    #[cfg(not(target_arch = "wasm32"))]
    fn source_request_with_work<'a, T, F, Fut>(
        &'a self, path: &'a str, source: SourceReference,
        original: &'a crate::materializer::NativeMaterializationWork,
        cancellation: &'a CancellationToken, run: F,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<(T, crate::materializer::NativeAllocation),
        (LazyWorkspaceError, Option<crate::materializer::NativeAllocation>)>>
    where
        F: FnOnce(NamespacePath) -> Fut + Send + 'a,
        Fut: Future<Output = Result<T, DemandError>> + Send + 'a,
    {
        Box::pin(async move {
            let frame = LazyTreapWork::native_with_work(original, cancellation)
                .map_err(|error| (error, None))?;
            in_heap_lazy::<_, LazyWorkspaceError, _>(&frame, || async {
                Ok(async {
            original.admit(WorkCounters::UNCHARGED)
                .map_err(|error| (LazyWorkspaceError::NativeWork(error), None))?;
            cancellation.check().map_err(|_| (LazyWorkspaceError::NativeWork(
                crate::materializer::NativeTreeMaterializationError::Cancelled), None))?;
            if self.source.reference() != source {
                return Err((LazyWorkspaceError::StaleSource, None));
            }
            let (namespace, mut custody) = self.source_namespace_with_work(path, original, cancellation)
                .map_err(|error| (error, None))?;
            // Drop the owned namespace before its allocation guard.
            let result = { run(namespace).await };
            let result = result.map_err(|error| match error {
                DemandError::Work(error) => LazyWorkspaceError::NativeWork(
                    crate::materializer::NativeTreeMaterializationError::Work(error)),
                DemandError::Cancelled => LazyWorkspaceError::NativeWork(
                    crate::materializer::NativeTreeMaterializationError::Cancelled),
                #[cfg(feature = "native-mount")]
                DemandError::Mount(crate::native_mount::MountSourceError::NativeWork(error)) =>
                    LazyWorkspaceError::NativeWork(error),
                #[cfg(feature = "native-mount")]
                DemandError::Mount(error) => match error.semantic_error() {
                    crate::native_mount::MountSourceError::Work(error) => LazyWorkspaceError::NativeWork(
                        crate::materializer::NativeTreeMaterializationError::Work(*error)),
                    crate::native_mount::MountSourceError::Cancelled => LazyWorkspaceError::NativeWork(
                        crate::materializer::NativeTreeMaterializationError::Cancelled),
                    crate::native_mount::MountSourceError::NativeWork(
                        crate::materializer::NativeTreeMaterializationError::Work(error)) =>
                        LazyWorkspaceError::NativeWork(crate::materializer::NativeTreeMaterializationError::Work(*error)),
                    crate::native_mount::MountSourceError::NativeWork(
                        crate::materializer::NativeTreeMaterializationError::Cancelled) =>
                        LazyWorkspaceError::NativeWork(crate::materializer::NativeTreeMaterializationError::Cancelled),
                    _ => LazyWorkspaceError::Demand(DemandError::Mount(error)),
                },
                error => LazyWorkspaceError::Demand(error),
        }).and_then(|value| {
            original.admit(WorkCounters::UNCHARGED)?;
            cancellation.check().map_err(|_| LazyWorkspaceError::NativeWork(
                crate::materializer::NativeTreeMaterializationError::Cancelled))?;
            if self.source.reference() != source {
                return Err(LazyWorkspaceError::StaleSource);
            }
            Ok(value)
        });
        if let Err(error) = custody.release(custody.bytes()) {
            drop(result);
            return Err((LazyWorkspaceError::NativeWork(error), Some(custody)));
        }
        match result {
            Ok(value) => Ok((value, custody)),
            Err(error) => Err((error, Some(custody))),
        }
            }.await)
        }).await.map_err(|error| (error, None))?
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::too_many_arguments)]
    async fn read_source_range(
        &self, path: &str, source: SourceReference, expected: SourceVersion,
        offset: u64, length: u64, original: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<crate::kernel::OwnedSourceChunk,
        (LazyWorkspaceError, Option<crate::materializer::NativeAllocation>)> {
        self.source_request_with_work(path, source, original, cancellation, |path| async move {
            self.source.read_range(source, &path, expected, offset, length,
                original, cancellation).await
        }).await.map(|(chunk, retired_metadata)| { drop(retired_metadata); chunk })
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_source_link(
        &self, path: &str, source: SourceReference, expected: SourceVersion, maximum_bytes: u64,
        original: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<crate::kernel::OwnedSourceChunk,
        (LazyWorkspaceError, Option<crate::materializer::NativeAllocation>)> {
        self.source_request_with_work(path, source, original, cancellation, |path| async move {
            self.source.read_link(source, &path, expected, maximum_bytes,
                original, cancellation).await
        }).await.map(|(chunk, retired_metadata)| { drop(retired_metadata); chunk })
    }

    async fn selected_lookup(
        &self, snapshot: LazySnapshotRef, path: &str,
        accounting: &LazyTreapWork<'_>,
    ) -> Result<SelectedLookupPhase<A, O>, LazyWorkspaceError> {
        let cancellation = accounting.cancellation;
        lazy_dispatch!{accounting, {
                let lookup = accounting.accept(self.lookup_at_snapshot_measured(
                    snapshot, path, accounting.remaining()?, cancellation,
                ).await.map_err(|failure| LazyWorkspaceError::SpentWork(Box::new(failure))))?;
                Ok(SelectedLookupPhase::Portable(lookup, std::marker::PhantomData))
            },
            |original| {
                let source_used = std::sync::atomic::AtomicBool::new(false);
                match self.inspect_at_snapshot_with_work(
                    snapshot, path, original, cancellation, &source_used,
                ).await {
                    Ok(frozen) => Ok(SelectedLookupPhase::Native(frozen)),
                    Err((error, allocation)) => Err(error.restore_native_failure_custody(allocation)),
                }
            }
        }
    }

    async fn compile_selected_record(
        &self, checkout: &crate::Checkout<A, O>, path: NamespacePath,
        record: FileRecord, accounting: &LazyTreapWork<'_>,
        ) -> Result<(crate::kernel::Mutation, FileId), LazyWorkspaceError> {
        let cancellation = accounting.cancellation;
        let bytes = path.retained_bytes();
        match &accounting.meter {
            LazyTreapMeter::Standalone { .. } => {
                accounting.release(bytes)?;
                let mut sdk = crate::kernel::allocation::KernelAccounting::standalone(accounting.remaining()?, cancellation);
                let result = async {
                    let allocation = sdk.claim_owned(bytes, 0)?;
                    checkout.compile_copied_record(crate::kernel::Owned::new(path, allocation, bytes), record, &mut sdk, cancellation).await.map(|owned| owned.value)
                }.await;
                let compiled = accounting.accept(crate::facade::measured(result, &sdk).map_err(|failure|
                    lazy_operation_failure(failure, |error| workspace_error(error.into()))))?;
                accounting.adopt(bytes)?;
                Ok(compiled)
            }
            #[cfg(not(target_arch = "wasm32"))]
            LazyTreapMeter::NativeOriginal { original, scratch } => {
                let allocation = accounting.take_native_owned(bytes)?
                    .ok_or(LazyWorkspaceError::NativeLedgerMismatch)?;
                let compiled = checkout.compile_copied_record(crate::kernel::Owned::new(path, crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation), bytes), record, &mut crate::kernel::allocation::KernelAccounting::native(original, cancellation), cancellation).await.map_err(native_filesystem_error)?;
                let crate::kernel::Owned { value: compiled, allocation: crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(mut allocation), .. } = compiled else { unreachable!("native copied records retain the original path allocation"); };
                let absorbed = scratch.lock().expect("lazy treap scratch lock poisoned")
                    .absorb(&mut allocation);
                if let Err(error) = absorbed {
                    drop(compiled);
                    drop(allocation);
                    return Err(error.into());
                }
                Ok(compiled)
            }
        }
    }

    async fn mutate_selected(
        &self, checkout: &mut crate::Checkout<A, O>,
        operations: Vec<crate::kernel::Mutation>, bytes: u64,
        accounting: &LazyTreapWork<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let cancellation = accounting.cancellation;
        if operations.is_empty() {
            drop(operations);
            return accounting.release(bytes);
        }
        lazy_dispatch!{accounting, {
                accounting.release(bytes)?;
                accounting.accept(checkout.mutate(
                    operations, accounting.remaining()?, cancellation,
                ).await.map_err(|failure|
                    lazy_operation_failure(failure, |error| workspace_error(error.into()))))?;
                Ok(())
            },
            |original| {
                let allocation = accounting.take_native_owned(bytes)?
                    .ok_or(LazyWorkspaceError::NativeLedgerMismatch)?;
                checkout.mutate_with_native_work(
                    operations, allocation, original, cancellation,
                ).await.map_err(native_filesystem_error)
            }
        }
    }

    pub(crate) async fn operation_generation(
        &self, key: IdempotencyKey, accounting: &LazyTreapWork<'_>,
    ) -> Result<Option<crate::GenerationId>, LazyWorkspaceError> {
        let cancellation = accounting.cancellation;
        let generation = lazy_dispatch!{accounting, accounting.accept(
                self.workspace.operation_generation(
                    key, accounting.remaining()?, cancellation,
                ).await.map_err(measured_workspace_error),
            )?,
            |original| {
                self.workspace.operation_generation(
                    key, original, cancellation,
                ).await.map_err(|error| native_workspace_error_pair((error, None)))?
            }
        };
        let id = generation.as_ref().map(|generation| generation.id());
        drop(generation);
        Ok(id)
    }

    async fn selected_checkout(
        &self, selector: crate::model::GenerationSelector, mode: crate::model::CheckoutMode,
        accounting: &LazyTreapWork<'_>,
    ) -> Result<crate::Checkout<A, O>, LazyWorkspaceError> {
        let cancellation = accounting.cancellation;
        lazy_dispatch!{accounting, accounting.accept(
                self.workspace.checkout(
                    selector, mode, accounting.remaining()?, cancellation,
                ).await.map_err(measured_workspace_error),
            ),
            |original| {
                self.workspace.checkout(
                    selector, mode, original, cancellation,
                ).await.map_err(native_workspace_error)
            }
        }
    }

    async fn selected_checkout_metadata(
        checkout: &mut crate::Checkout<A, O>, path: &NamespacePath,
        accounting: &LazyTreapWork<'_>,
    ) -> Result<Option<crate::PathMetadataLookup>, LazyWorkspaceError> {
        let cancellation = accounting.cancellation;
        match &accounting.meter {
            LazyTreapMeter::Standalone { .. } => accounting.accept(
                checkout.lookup_no_follow_with_metadata(path, accounting.remaining()?, cancellation).await
                    .map_err(|failure| lazy_operation_failure(
                        failure, |error| workspace_error(error.into()),
                    )),
            ),
            #[cfg(not(target_arch = "wasm32"))]
            LazyTreapMeter::NativeOriginal { original, .. } =>
                checkout.lookup_no_follow_with_metadata(path, original, cancellation)
                    .await.map_err(native_filesystem_error),
        }
    }

    async fn selected_paths(
        &self, roots: &[String], snapshot: Option<LazySnapshotRef>,
        generation: Option<&crate::Generation<A, O>>, accounting: &LazyTreapWork<'_>,
    ) -> Result<(Vec<String>, bool), LazyWorkspaceError> {
        let cancellation = accounting.cancellation;
        in_heap_lazy::<_, LazyWorkspaceError, _>(accounting, || async {
            match &accounting.meter {
                LazyTreapMeter::Standalone { .. } => {
                    let mut work = WorkCounters::UNCHARGED;
                    let mut retained_bytes = 0;
                    let result = self.collect_selected_paths_with_accounting(
                        roots, snapshot, generation, &mut ExactifyCollectorWork::Portable {
                            retained_bytes: &mut retained_bytes, work: &mut work,
                            budget: accounting.remaining()?,
                        }, cancellation,
                    ).await;
                    let (paths, used) = accounting.accept(result
                        .map(|value| OperationReceipt { value, work })
                        .map_err(|error| error.with_prior_work(work)))?;
                    accounting.adopt(exactify_paths_retained_bytes(&paths)?)?;
                    Ok((paths, used))
                }
                #[cfg(not(target_arch = "wasm32"))]
                LazyTreapMeter::NativeOriginal { original, scratch } => {
                    let scope = TreapAllocationScope::new(accounting);
                    let (paths, used) = self.collect_selected_paths_with_accounting(
                        roots, snapshot, generation,
                        &mut ExactifyCollectorWork::Native { work: accounting, original }, cancellation,
                    ).await?;
                    let mut allocation = accounting.take_native_owned(exactify_paths_retained_bytes(&paths)?)?
                        .ok_or(LazyWorkspaceError::NativeLedgerMismatch)?;
                    drop(scope);
                    if let Err(error) = scratch.lock().expect("lazy treap scratch lock poisoned").absorb(&mut allocation) {
                        drop(paths);
                        return Err(error.into());
                    }
                    Ok((paths, used))
                }
            }
        }).await
    }

    /// Prepares selected namespace replacement in the caller's actual private checkout.
    pub(crate) async fn prepare_selected_import_into_checkout(
        &self, destination: LazySnapshotRef, plan: &SelectedImportPlan,
        checkout: &mut crate::Checkout<A, O>, operation: crate::OperationId,
        accounting: &LazyTreapWork<'_>,
    ) -> Result<SelectedImportEffects, LazyWorkspaceError> {
        let cancellation = accounting.cancellation;
        let scratch = TreapAllocationScope::new(accounting);
        let prepare = || async move {
            macro_rules! stage_selected_lookup {
                ($checkout:expr, $snapshot:expr, $path:expr, $reject_authored:expr) => {{
                    let lookup = self.selected_lookup($snapshot, $path, accounting).await?;
                    let bytes = lookup.portable_owned_bytes();
                    accounting.adopt(bytes)?;
                    if $reject_authored && matches!(lookup.lookup(), LazyLookup::Authored { path: actual, .. } if actual == $path) {
                        return Err(LazyWorkspaceError::StaleIdentity);
                    }
                    let expected = self.selected_lookup_identity(lookup.lookup(), accounting)?;
                    self.stage_into_checkout(
                        $checkout, Some($snapshot), $path, expected, u64::MAX, accounting,
                    ).await?;
                    drop(lookup);
                    accounting.release(bytes)?;
                }};
            }
            if plan.source_used && self.source.reference() != plan.source.source {
                return Err(LazyWorkspaceError::StaleSource);
            }
            let current_snapshot = self.snapshot(accounting).await?;
            let (paths, _) = self.selected_paths(&plan.roots, Some(destination), None, accounting).await?;
            let mut baseline = self.selected_checkout(crate::model::GenerationSelector::Exact(destination.authored_generation), crate::model::CheckoutMode {
                    access: crate::model::AccessMode::ReadWrite,
                    consistency: crate::model::ConsistencyMode::Pinned,
                    mutations: crate::model::MutationMode::PrivateOverlay,
                }, accounting).await?;
            let baseline_bytes = accounting.retain_fresh_checkout(&baseline)?;
            #[cfg(not(target_arch = "wasm32"))]
            if let LazyTreapMeter::NativeOriginal { .. } = &accounting.meter {
                baseline.bind_authored_operation(operation);
                checkout.bind_authored_operation(operation);
            }
            for path in &paths {
                stage_selected_lookup!(&mut baseline, destination, path, false);
                let namespace = accounting.namespace_owned(
                    path, self.workspace.profile(), self.workspace.limits(),
                )?;
                let namespace_bytes = namespace.retained_bytes();
                let current = selected_checkout_query!(checkout, &namespace, accounting, cancellation, lookup_no_follow, lookup_no_follow)?;
                drop(namespace);
                accounting.release(namespace_bytes)?;
                if current.record.is_none() {
                    stage_selected_lookup!(checkout, current_snapshot, path, true);
                }
            }
            let mut records: Vec<(FileRecord, FileMetadata)> = Vec::new();
            for path in &paths {
                let namespace = accounting.namespace_owned(
                    path, self.workspace.profile(), self.workspace.limits(),
                )?;
                let namespace_bytes = namespace.retained_bytes();
                let before = selected_checkout_query!(&mut baseline, &namespace, accounting, cancellation, lookup_no_follow_with_metadata, lookup_no_follow_with_metadata)?
                    .ok_or(LazyWorkspaceError::StaleIdentity)?;
                let actual = selected_checkout_query!(checkout, &namespace, accounting, cancellation, lookup_no_follow_with_metadata, lookup_no_follow_with_metadata)?
                    .ok_or(LazyWorkspaceError::StaleIdentity)?;
                let mut comparable = actual.record;
                // Normalize link count for aliases outside the selected namespace.
                comparable.link_count = before.record.link_count;
                accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                if comparable != before.record || actual.metadata != before.metadata {
                    return Err(LazyWorkspaceError::StaleIdentity);
                }
                drop(namespace);
                accounting.release(namespace_bytes)?;
                let mut retained_identity = false;
                for (record, _) in &records {
                    accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                    if record.file_id == actual.record.file_id {
                        retained_identity = true;
                        break;
                    }
                }
                if !retained_identity {
                    accounting.ensure_vec_push(&mut records)?;
                    accounting.charge(WorkCounters {
                        bytes_copied: std::mem::size_of::<(FileRecord, FileMetadata)>() as u64,
                        ..WorkCounters::UNCHARGED
                    })?;
                    records.push((actual.record, actual.metadata));
                }
            }
            for entry in &plan.entries {
                let mut previously_present = false;
                for path in &paths {
                    accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                    if *path == entry.path { previously_present = true; break; }
                }
                if !previously_present {
                    let namespace = accounting.namespace_owned(
                        &entry.path, self.workspace.profile(), self.workspace.limits(),
                    )?;
                    let namespace_bytes = namespace.retained_bytes();
                    let before = Self::selected_checkout_metadata(&mut baseline, &namespace, accounting).await?
                        .ok_or(LazyWorkspaceError::StaleIdentity)?;
                    let actual = Self::selected_checkout_metadata(checkout, &namespace, accounting).await?
                        .ok_or(LazyWorkspaceError::StaleIdentity)?;
                    let mut comparable = actual.record;
                    comparable.link_count = before.record.link_count;
                    accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                    if comparable != before.record || actual.metadata != before.metadata {
                        return Err(LazyWorkspaceError::StaleIdentity);
                    }
                    drop(namespace);
                    accounting.release(namespace_bytes)?;
                }
            }
            drop(baseline);
            accounting.release(baseline_bytes)?;
            for entry in &plan.entries {
                if entry.record.is_none() { continue; }
                for (end, _) in entry.path.match_indices('/').skip(1) {
                    let parent = &entry.path[..end];
                    let mut selected_parent = false;
                    for selected in &plan.entries {
                        accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                        if selected.path == parent
                            && matches!(selected.record, Some((FileRecord {
                                kind: FileKind::Directory, ..
                            }, _)))
                        {
                            selected_parent = true;
                            break;
                        }
                    }
                    if selected_parent { continue; }
                    let namespace = accounting.namespace_owned(
                        parent, self.workspace.profile(), self.workspace.limits(),
                    )?;
                    let namespace_bytes = namespace.retained_bytes();
                    let existing = selected_checkout_query!(checkout, &namespace, accounting, cancellation, lookup_no_follow, lookup_no_follow)?;
                    if let Some(record) = existing.record {
                        if record.kind != FileKind::Directory {
                            return Err(LazyWorkspaceError::NotDirectory);
                        }
                    } else {
                        stage_selected_lookup!(checkout, current_snapshot, parent, false);
                        let staged = selected_checkout_query!(checkout, &namespace, accounting, cancellation, lookup_no_follow, lookup_no_follow)?;
                        if !matches!(staged.record, Some(FileRecord { kind: FileKind::Directory, .. })) {
                            return Err(LazyWorkspaceError::NotDirectory);
                        }
                    }
                    drop(namespace);
                    accounting.release(namespace_bytes)?;
                }
            }
            if let LazyTreapMeter::Standalone { .. } = &accounting.meter {
                checkout.bind_authored_operation(operation);
            }
            let mut operations = Vec::new();
            let mut changes = Vec::new();
            for path in paths.iter().rev() {
                if path == "/" { continue; }
                accounting.ensure_vec_push(&mut operations)?;
                accounting.ensure_vec_push(&mut changes)?;
                let namespace = accounting.namespace_owned(
                    path, self.workspace.profile(), self.workspace.limits(),
                )?;
                let changed = accounting.namespace_owned(
                    path, self.workspace.profile(), self.workspace.limits(),
                )?;
                operations.push(crate::kernel::Mutation::Remove {
                    path: namespace, expected_file_id: crate::kernel::MetadataField::Unavailable,
                });
                changes.push(changed);
                accounting.charge(WorkCounters {
                    bytes_copied: (std::mem::size_of::<crate::kernel::Mutation>()
                        + std::mem::size_of::<NamespacePath>()) as u64,
                    ..WorkCounters::UNCHARGED
                })?;
            }
            let mut identities: Vec<(FileId, &str)> = Vec::new();
            for entry in &plan.entries {
                let Some((record, _)) = entry.record else { continue; };
                accounting.ensure_vec_push(&mut operations)?;
                accounting.ensure_vec_push(&mut changes)?;
                let mut existing = None;
                if record.kind != FileKind::Directory {
                    for (identity, path) in &identities {
                        accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                        if *identity == record.file_id { existing = Some(*path); break; }
                    }
                }
                let namespace = accounting.namespace_owned(
                    &entry.path, self.workspace.profile(), self.workspace.limits(),
                )?;
                let mutation = if let Some(existing) = existing {
                    let source = accounting.namespace_owned(
                        existing, self.workspace.profile(), self.workspace.limits(),
                    )?;
                    crate::kernel::Mutation::Link { source, destination: namespace }
                } else {
                    let compiled = self.compile_selected_record(checkout, namespace, record, accounting).await?;
                    if record.kind != FileKind::Directory {
                        accounting.ensure_vec_push(&mut identities)?;
                        identities.push((record.file_id, entry.path.as_str()));
                    }
                    compiled.0
                };
                let changed = accounting.namespace_owned(
                    &entry.path, self.workspace.profile(), self.workspace.limits(),
                )?;
                accounting.charge(WorkCounters {
                    bytes_copied: (std::mem::size_of::<crate::kernel::Mutation>()
                        + std::mem::size_of::<NamespacePath>()) as u64,
                    ..WorkCounters::UNCHARGED
                })?;
                operations.push(mutation);
                changes.push(changed);
            }
            let identity_bytes = crate::foundation::usize_to_u64(identities.capacity())
                .checked_mul(std::mem::size_of::<(FileId, &str)>() as u64)
                .ok_or_else(counter_overflow)?;
            drop(identities);
            accounting.release(identity_bytes)?;
            let mut transferred_bytes = crate::foundation::usize_to_u64(operations.capacity())
                .checked_mul(std::mem::size_of::<crate::kernel::Mutation>() as u64)
                .ok_or_else(counter_overflow)?;
            for operation in &operations {
                transferred_bytes = transferred_bytes.checked_add(match operation {
                    crate::kernel::Mutation::Remove { path, .. }
                    | crate::kernel::Mutation::Create { path, .. } => path.retained_bytes(),
                    crate::kernel::Mutation::Link { source, destination } =>
                        source.retained_bytes().checked_add(destination.retained_bytes())
                            .ok_or_else(counter_overflow)?,
                    crate::kernel::Mutation::File { .. } => 0,
                    _ => unreachable!("selected compiler emits only canonical replacement mutations"),
                }).ok_or_else(counter_overflow)?;
            }
            let mut paths = paths;
            for root in &plan.roots {
                let mut present = false;
                for path in &paths {
                    accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                    if path == root { present = true; break; }
                }
                if !present {
                    accounting.ensure_vec_push(&mut paths)?;
                    paths.push(accounting.canonical_path_owned(root, self.workspace.limits())?);
                }
            }
            let input = plan.input_digest(destination, &accounting)?;
            self.mutate_selected(checkout, operations, transferred_bytes, accounting).await?;
            let mut logical_owned_bytes = (paths.capacity() as u64)
                .checked_mul(std::mem::size_of::<String>() as u64)
                .and_then(|bytes| bytes.checked_add((records.capacity() as u64)
                    .checked_mul(std::mem::size_of::<(FileRecord, FileMetadata)>() as u64)?))
                .and_then(|bytes| bytes.checked_add((changes.capacity() as u64)
                    .checked_mul(std::mem::size_of::<NamespacePath>() as u64)?))
                .ok_or(crate::WorkError::Overflow)?;
            for path in &paths {
                logical_owned_bytes = logical_owned_bytes.checked_add(path.capacity() as u64)
                    .ok_or(crate::WorkError::Overflow)?;
            }
            for path in &changes {
                logical_owned_bytes = logical_owned_bytes.checked_add(path.retained_bytes())
                    .ok_or(crate::WorkError::Overflow)?;
            }
            Ok(SelectedImportEffects {
                paths, records, changes,
                input,
                logical_owned_bytes,
                #[cfg(not(target_arch = "wasm32"))]
                allocation: None,
            })
        };
        let mut effects = match &accounting.meter {
            LazyTreapMeter::Standalone { .. } =>
                in_heap_lazy::<_, LazyWorkspaceError, _>(accounting, prepare).await?,
            #[cfg(not(target_arch = "wasm32"))]
            LazyTreapMeter::NativeOriginal { .. } => prepare().await?,
        };
        #[cfg(not(target_arch = "wasm32"))]
        if matches!(accounting.meter, LazyTreapMeter::NativeOriginal { .. }) {
            effects.allocation = accounting.take_native_owned(effects.logical_owned_bytes)?;
        }
        drop(scratch);
        if matches!(accounting.meter, LazyTreapMeter::Standalone { .. }) {
            accounting.adopt(effects.logical_owned_bytes)?;
        }
        Ok(effects)
    }

    fn selected_lookup_identity(
        &self,
        lookup: &LazyLookup,
        accounting: &LazyTreapWork<'_>,
    ) -> Result<FileId, LazyWorkspaceError> {
        match lookup {
            LazyLookup::Authored { stat, .. } => Ok(stat.file_id),
            LazyLookup::Shadow { record, .. } => Ok(record.file_id),
            LazyLookup::Source(node) => accounting.with_kernel(|kernel, _|
                self.source_file_id(&node.file_identity, kernel)),
        }
    }

    /// Publishes selected frozen records through the retained original SDK checkout and its existing companion removal operation.
    pub async fn import_selected_measured_guarded<F, Fut>(
        &self,
        destination: LazySnapshotRef,
        plan: SelectedImportPlan,
        operation: crate::OperationId,
        permit: crate::PublicationPermit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        mut before_publication: F,
    ) -> crate::MeasuredResult<OperationReceipt<LazySnapshotRef>, LazyWorkspaceError>
    where
        F: FnMut(WorkBudget) -> Fut + Send,
        Fut: Future<Output = crate::FsResult<()>> + Send,
    {
        in_heap_measured(move |budget| async move {
            let accounting = LazyTreapWork::new(budget, cancellation);
            let incoming = SelectedImportInput {
                plan,
                #[cfg(not(target_arch = "wasm32"))]
                allocation: None,
            };
            let result = self.import_selected_with_accounting_guarded(
                destination, incoming, operation, permit, &accounting, cancellation,
                &mut before_publication,
            ).await;
            accounting.measured(result, |error, work| crate::OperationFailure::new(error, work))
        }, budget, cancellation, LazyWorkspaceError::Cancelled, None).await
    }

    /// Consumes a real frozen plan and its original root/resource payload guard.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn import_selected_with_native_work_guarded<E, F, Fut>(
        &self,
        destination: LazySnapshotRef,
        plan: SelectedImportPlan,
        allocation: crate::materializer::NativeAllocation,
        operation: crate::OperationId,
        permit: crate::PublicationPermit,
        original: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
        mut before_publication: F,
    ) -> Result<LazySnapshotRef, E>
    where
        E: From<LazyWorkspaceError> + From<crate::FsError> + From<WorkspaceError> + Send + 'static,
        F: for<'work> FnMut(&'work crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        // Field order drops the transferred data before its guard.
        let incoming = SelectedImportInput { plan, allocation: Some(allocation) };
        let paid = incoming.allocation.as_ref().expect("native plan has custody");
        if !paid.work().shares_ledger(original)
            || !paid.work().shares_resource_allowance(original)
            || paid.bytes() < incoming.plan.logical_owned_bytes()
        {
            return Err(E::from(LazyWorkspaceError::NativeLedgerMismatch));
        }
        let accounting = LazyTreapWork::native_with_work(original, cancellation)?;
        in_heap_lazy(&accounting, || async {
            let mut publication = OwnedNativeImportGuard { callback: &mut before_publication, original };
            self.import_selected_with_accounting_guarded(
                destination, incoming, operation, permit, &accounting, cancellation,
                &mut publication,
            ).await
        }).await
    }

    async fn import_selected_with_accounting_guarded<P>(
        &self,
        destination: LazySnapshotRef,
        incoming: SelectedImportInput,
        operation: crate::OperationId,
        permit: crate::PublicationPermit,
        accounting: &LazyTreapWork<'_>,
        cancellation: &CancellationToken,
        publication: &mut P,
    ) -> Result<LazySnapshotRef, P::Error>
    where
        P: SelectedImportGuard<A, O>,
    {
        if self.selected_publication.busy.compare_exchange(
            false, true, std::sync::atomic::Ordering::Acquire,
            std::sync::atomic::Ordering::Relaxed,
        ).is_err() {
            return Err(LazyWorkspaceError::Concurrent.into());
        }
        let mut custody = SelectedImportCustody {
            control: &self.selected_publication,
            retained: self.selected_publication.retained.lock()
                .expect("selected import custody lock poisoned").take(),
        };
        let incoming_bytes = lazy_dispatch!{accounting, {
                let bytes = incoming.plan.logical_owned_bytes();
                accounting.adopt(bytes)?;
                bytes
            },
            |original| {
                if let Some(retained) = custody.retained.as_ref() {
                    let paid = retained.plan_allocation.as_ref()
                        .ok_or(LazyWorkspaceError::NativeLedgerMismatch)?;
                    if !paid.work().shares_ledger(original)
                        || !paid.work().shares_resource_allowance(original)
                    {
                        return Err(LazyWorkspaceError::NativeLedgerMismatch.into());
                    }
                }
                0
            }
        };
        let retained_bytes = match &accounting.meter {
            LazyTreapMeter::Standalone { .. } => {
                let bytes = custody.retained.as_ref().map(|retained|
                    retained.checkout.logical_owned_bytes()
                        .checked_add(retained.plan.logical_owned_bytes())
                        .and_then(|bytes| bytes.checked_add(retained.effects.logical_owned_bytes))
                        .ok_or_else(counter_overflow)).transpose()?.unwrap_or(0);
                accounting.adopt(bytes)?;
                bytes
            }
            #[cfg(not(target_arch = "wasm32"))]
            LazyTreapMeter::NativeOriginal { .. } => 0,
        };
        let key = IdempotencyKey::from_bytes(operation.into_bytes());
        let input = incoming.plan.input_digest(destination, accounting)?;
        if let Some(generation) = self.operation_generation(key, accounting).await? {
            let snapshot = self.selected_committed_snapshot(operation, generation, destination, incoming.plan.source, incoming.plan.source_config, input, accounting).await.map_err(|error| publication.map_lazy(error, operation))?;
            custody.retained = None;
            if retained_bytes != 0 { accounting.release(retained_bytes)?; }
            drop(incoming);
            if incoming_bytes != 0 { accounting.release(incoming_bytes)?; }
            return Ok(snapshot);
        }
        if let Some(retained) = custody.retained.as_ref() {
            accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            if retained.operation != operation || retained.destination != destination
                || retained.effects.input != input
            {
                return Err(LazyWorkspaceError::StaleIdentity.into());
            }
            drop(incoming);
            if incoming_bytes != 0 { accounting.release(incoming_bytes)?; }
        } else {
            let phase = self.load_binding_at(self.workspace.id(), accounting, cancellation).await?
                .ok_or(LazyWorkspaceError::StaleIdentity)?;
            if phase.as_state().pending_remove.is_some() {
                return Err(LazyWorkspaceError::Concurrent.into());
            }
            drop(phase);
            let mut checkout = self.selected_checkout(crate::model::GenerationSelector::Head, crate::model::CheckoutMode {
                    access: crate::model::AccessMode::ReadWrite,
                    consistency: crate::model::ConsistencyMode::TrackingSafe,
                    mutations: crate::model::MutationMode::PrivateOverlay,
                }, accounting).await?;
            accounting.retain_fresh_checkout(&checkout)?;
            let effects = self.prepare_selected_import_into_checkout(
                destination, &incoming.plan, &mut checkout, operation, accounting,
            ).await?;
            custody.retained = Some(SelectedImportPublication {
                checkout, destination, plan: incoming.plan, operation, effects,
                prepared: false, published: None,
                #[cfg(not(target_arch = "wasm32"))]
                plan_allocation: incoming.allocation,
            });
        }
        let retained = custody.retained.as_mut().expect("selected candidate is retained");
        if !retained.prepared {
            lazy_dispatch!{accounting, {
                    let rebase = accounting.accept(retained.checkout.rebase_head(
                        self.workspace.limits().maximum_checkout_dependencies,
                        accounting.remaining()?, cancellation,
                    ).await.map_err(|failure|
                        lazy_operation_failure(failure, |error| workspace_error(error.into()))))?;
                    if let crate::kernel::RebaseDecision::Conflicted { conflicts, truncated } = rebase {
                        return Err(LazyWorkspaceError::Filesystem(crate::FsError::LiveConflict {
                            conflicts, truncated,
                        }).into());
                    }
                },
                |original| {
                    let (rebase, allocation) = retained.checkout.rebase_head_with_native_work(
                        self.workspace.limits().maximum_checkout_dependencies,
                        original, cancellation,
                    ).await.map_err(native_filesystem_error)?;
                    if let crate::kernel::RebaseDecision::Conflicted { conflicts, truncated } = rebase {
                        return Err(native_workspace_error_pair((
                            WorkspaceError::Filesystem(crate::FsError::LiveConflict { conflicts, truncated }),
                            Some(allocation),
                        )).into());
                    }
                    drop(rebase);
                    drop(allocation);
                }
            }
            self.prepare_mount_removals(&retained.effects.paths, &[], &retained.effects.records, key, accounting).await?;
            retained.prepared = true;
        }
        accounting.charge(WorkCounters::UNCHARGED)?;
        let published = publication.commit(
            &mut retained.checkout, operation, permit, accounting, cancellation,
        ).await?;
        let generation = match published {
            crate::CheckoutCommitOutcome::Committed { generation_id, .. }
            | crate::CheckoutCommitOutcome::AlreadyCommitted { generation_id, .. } => generation_id,
            outcome => return Err(LazyWorkspaceError::SelectedPublication(outcome).into()),
        };
        retained.published = Some(generation);
        accounting.charge(WorkCounters::UNCHARGED)?;
        let snapshot = self.selected_committed_snapshot(operation, generation, retained.destination, retained.plan.source, retained.plan.source_config, retained.effects.input, accounting).await.map_err(|error| publication.map_lazy(error, operation))?;
        custody.retained = None;
        Ok(snapshot)
    }

    pub(crate) async fn selected_committed_snapshot(
        &self, operation: crate::OperationId, generation: crate::GenerationId,
        destination: LazySnapshotRef, source: LazySnapshotRef,
        source_config: VolumeConfig, input: crate::Digest,
        accounting: &LazyTreapWork<'_>,
    ) -> Result<LazySnapshotRef, LazyWorkspaceError> {
        let cancellation = accounting.cancellation;
            if destination.workspace_id != self.workspace.id() {
                return Err(LazyWorkspaceError::StaleIdentity);
            }
            for _ in 0..MAXIMUM_CAS_ATTEMPTS {
                let phase = self.load_binding_at(self.workspace.id(), accounting, cancellation).await?
                    .ok_or(LazyWorkspaceError::StaleIdentity)?;
                let state = phase.as_state();
                if let Some(completed) = state.completed_selected_import
                    && completed.operation == operation
                {
                    if completed.snapshot.authored_generation != generation
                        || completed.destination != destination || completed.source != source
                        || completed.source_config != source_config || completed.input != input
                        || completed.config != self.workspace.volume_config()
                    {
                        return Err(LazyWorkspaceError::StaleIdentity);
                    }
                    if matches!(state.pending_remove, Some(PendingLazyRemove {
                        kind: PendingLazyRemoveKind::Mounted { idempotency_key }, ..
                    }) if idempotency_key.operation_id() == operation) {
                        let finalized = self.finish_mount_removals(accounting).await?;
                        if !finalized { return Err(LazyWorkspaceError::Concurrent); }
                    }
                    return Ok(completed.snapshot);
                }
                if !matches!(state.pending_remove, Some(PendingLazyRemove {
                    kind: PendingLazyRemoveKind::Mounted { idempotency_key }, ..
                }) if idempotency_key.operation_id() == operation) {
                    return Err(LazyWorkspaceError::OriginalSnapshotUnavailable);
                }
                accounting.charge(WorkCounters {
                    bytes_hashed: (LAZY_SNAPSHOT_DOMAIN.len() + self.workspace.id().into_bytes().len()
                        + generation.digest().as_bytes().len() + state.source.identity.len()
                        + state.source.epoch.to_le_bytes().len() + state.overlay.into_bytes().len()
                        + state.shadows.into_bytes().len()) as u64,
                    ..WorkCounters::UNCHARGED
                })?;
                let snapshot = LazySnapshotRef::new(
                    self.workspace.id(), generation, state.source, state.overlay, state.shadows,
                );
                let replacement = LazyWorkspaceState {
                    revision: state.revision.saturating_add(1),
                    completed_selected_import: Some(CompletedSelectedImport {
                        operation, snapshot, destination, source, input,
                        config: self.workspace.volume_config(), source_config,
                    }),
                    ..state.clone()
                };
                if self.swap_binding_at(self.workspace.id(), state.revision, replacement, accounting, cancellation).await? {
                    let finalized = self.finish_mount_removals(accounting).await?;
                    if !finalized { return Err(LazyWorkspaceError::Concurrent); }
                    return Ok(snapshot);
                }
            }
            Err(LazyWorkspaceError::Concurrent)
    }

    #[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
    pub(crate) async fn selected_import_snapshot_for_generation(
        &self, operation: crate::OperationId, generation: crate::GenerationId,
        accounting: &LazyTreapWork<'_>,
    ) -> Result<LazySnapshotRef, LazyWorkspaceError> {
        let phase = self.load_binding_at(self.workspace.id(), accounting, accounting.cancellation).await?
            .ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?;
        let saved = phase.as_state().completed_selected_import
            .ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?;
        if saved.operation != operation || saved.snapshot.authored_generation != generation
            || saved.snapshot.workspace_id != self.workspace.id()
            || saved.destination.workspace_id != self.workspace.id()
            || saved.config != self.workspace.volume_config()
        {
            return Err(LazyWorkspaceError::OriginalSnapshotUnavailable);
        }
        Ok(saved.snapshot)
    }

    /// Returns only a proven original selected-import snapshot, never the head.
    pub async fn selected_import_snapshot(
        &self, operation: crate::OperationId, accounting: &LazyTreapWork<'_>,
    ) -> Result<Option<LazySnapshotRef>, LazyWorkspaceError> {
        let cancellation = accounting.cancellation;
        let query = || async {
            if self.selected_publication.busy.compare_exchange(
                false, true, std::sync::atomic::Ordering::Acquire,
                std::sync::atomic::Ordering::Relaxed,
            ).is_err() {
                return Err(LazyWorkspaceError::Concurrent);
            }
            let mut custody = SelectedImportCustody {
                control: &self.selected_publication,
                retained: self.selected_publication.retained.lock()
                    .expect("selected import custody lock poisoned").take(),
            };
            if let LazyTreapMeter::Standalone { .. } = &accounting.meter {
                let bytes = custody.retained.as_ref().map(|retained|
                    retained.checkout.logical_owned_bytes()
                        .checked_add(retained.plan.logical_owned_bytes())
                        .and_then(|bytes| bytes.checked_add(retained.effects.logical_owned_bytes))
                        .ok_or_else(counter_overflow)).transpose()?.unwrap_or(0);
                accounting.adopt(bytes)?;
            }
            let Some(generation) = self.operation_generation(IdempotencyKey::from_bytes(operation.into_bytes()), accounting).await? else { return Ok(None); };
            let phase = self.load_binding_at(self.workspace.id(), accounting, cancellation).await?
                .ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?;
            let saved = phase.as_state().completed_selected_import
                .filter(|saved| saved.operation == operation);
            let (destination, source, source_config, input) = if let Some(saved) = saved {
                if saved.snapshot.authored_generation != generation
                    || saved.snapshot.workspace_id != self.workspace.id()
                    || saved.destination.workspace_id != self.workspace.id()
                    || saved.config != self.workspace.volume_config()
                {
                    return Err(LazyWorkspaceError::OriginalSnapshotUnavailable);
                }
                (saved.destination, saved.source, saved.source_config, saved.input)
            } else {
                let retained = custody.retained.as_ref()
                    .filter(|retained| retained.operation == operation)
                    .ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?;
                #[cfg(not(target_arch = "wasm32"))]
                if let LazyTreapMeter::NativeOriginal { original, .. } = &accounting.meter {
                    let paid = retained.plan_allocation.as_ref()
                        .ok_or(LazyWorkspaceError::NativeLedgerMismatch)?;
                    if !paid.work().shares_ledger(original)
                        || !paid.work().shares_resource_allowance(original)
                    {
                        return Err(LazyWorkspaceError::NativeLedgerMismatch);
                    }
                }
                (retained.destination, retained.plan.source,
                    retained.plan.source_config, retained.effects.input)
            };
            drop(phase);
            let snapshot = self.selected_committed_snapshot(operation, generation, destination, source, source_config, input, accounting).await?;
            self.validate_snapshot(snapshot, accounting).await?;
            if custody.retained.as_ref().is_some_and(|retained| retained.operation == operation) {
                custody.retained = None;
            }
            Ok(Some(snapshot))
        };
        #[cfg(not(target_arch = "wasm32"))]
        if matches!(accounting.meter, LazyTreapMeter::NativeOriginal { .. }) {
            return in_heap_lazy::<_, LazyWorkspaceError, _>(accounting, query).await
                .map_err(|error| match error {
                    LazyWorkspaceError::OriginalSnapshotUnavailable =>
                        LazyWorkspaceError::SelectedImportIndeterminate { operation },
                    error => error,
                });
        }
        query().await
    }

    /// Freezes selected records while retaining any original native allocation.
    pub async fn prepare_selected_import(
        &self, snapshot: LazySnapshotRef, requested: &[NamespacePath],
        accounting: &LazyTreapWork<'_>,
    ) -> Result<LazyReadonlyNode<SelectedImportPlan>, LazyWorkspaceError> {
        let cancellation = accounting.cancellation;
        #[cfg(not(target_arch = "wasm32"))]
        let scratch = matches!(accounting.meter, LazyTreapMeter::NativeOriginal { .. })
            .then(|| TreapAllocationScope::new(accounting));
        let result = in_heap_lazy::<_, LazyWorkspaceError, _>(accounting, || async {
                let mut roots = Vec::new();
                for path in requested {
                    accounting.ensure_vec_push(&mut roots)?;
                    let text = accounting.produce(
                        |kernel, cancellation|
                            path.unicode_text_with_accounting(kernel, cancellation).map_err(WorkspaceError::from).map_err(|error| {
                                #[cfg(not(target_arch = "wasm32"))]
                                if let LazyTreapMeter::NativeOriginal { .. } = &accounting.meter {
                                    return native_workspace_error(error);
                                }
                                workspace_error(error)
                            }),
                    )?;
                    accounting.charge(WorkCounters {
                        bytes_copied: std::mem::size_of::<String>() as u64,
                        ..WorkCounters::UNCHARGED
                    })?;
                    roots.push(text);
                }
                let (paths, source_used) = self.selected_paths(&roots, Some(snapshot), None, accounting).await?;
                let path_slots = (paths.capacity() as u64)
                    .checked_mul(std::mem::size_of::<String>() as u64)
                    .ok_or_else(counter_overflow)?;
                let mut checkout = match &accounting.meter {
                    LazyTreapMeter::Standalone { .. } => Some(self.selected_checkout(crate::model::GenerationSelector::Exact(snapshot.authored_generation), crate::model::CheckoutMode {
                            access: crate::model::AccessMode::ReadOnly,
                            consistency: crate::model::ConsistencyMode::Pinned,
                            mutations: crate::model::MutationMode::None,
                        }, accounting).await?),
                    #[cfg(not(target_arch = "wasm32"))]
                    LazyTreapMeter::NativeOriginal { .. } => None,
                };
                let checkout_bytes = checkout.as_ref().map(|checkout|
                    accounting.retain_fresh_checkout(checkout)).transpose()?.unwrap_or(0);
                let mut entries = Vec::new();
                let mut logical_bytes = 0_u64;
                let mut maximum_regular_file_bytes = 0_u64;
                for path in paths {
                    accounting.ensure_vec_push(&mut entries)?;
                    let lookup = self.selected_lookup(snapshot, &path, accounting).await?;
                    let lookup_bytes = lookup.portable_owned_bytes();
                    accounting.adopt(lookup_bytes)?;
                    let record_and_metadata = match lookup {
                        SelectedLookupPhase::Portable(LazyLookup::Authored { path: lookup_path, stat }, _) => {
                            let checkout = checkout.as_mut().expect("portable selected reader is retained");
                            let record = accounting.accept(checkout.read_file_record_by_id(
                                stat.file_id, accounting.remaining()?, cancellation,
                            ).await.map_err(|failure|
                                lazy_operation_failure(failure, |error| workspace_error(WorkspaceError::from(error)))))?;
                            let metadata = accounting.accept(checkout.read_metadata_by_id(
                                stat.file_id, accounting.remaining()?, cancellation,
                            ).await.map_err(|failure|
                                lazy_operation_failure(failure, |error| workspace_error(WorkspaceError::from(error)))))?;
                            drop(lookup_path);
                            accounting.release(lookup_bytes)?;
                            (record, metadata)
                        }
                        SelectedLookupPhase::Portable(LazyLookup::Shadow { record, .. }, _) => {
                            let checkout = checkout.as_mut().expect("portable selected reader is retained");
                            let detached = accounting.accept(checkout.detached_from_record(
                                record, accounting.remaining()?, cancellation,
                            ).map_err(|failure|
                                lazy_operation_failure(failure, |error| workspace_error(WorkspaceError::from(error)))))?;
                            let detached_bytes = detached.logical_owned_bytes();
                            accounting.adopt(detached_bytes)?;
                            let metadata = accounting.accept(detached.read_metadata(
                                accounting.remaining()?, cancellation,
                            ).await.map_err(|failure|
                                lazy_operation_failure(failure, |error| workspace_error(WorkspaceError::from(error)))))?;
                            drop(detached);
                            accounting.release(detached_bytes)?;
                            (record, metadata)
                        }
                        SelectedLookupPhase::Portable(LazyLookup::Source(_), _) =>
                            return Err(LazyWorkspaceError::UnsupportedNode),
                        #[cfg(not(target_arch = "wasm32"))]
                        SelectedLookupPhase::Native(frozen) => (
                            frozen.1.record.ok_or(LazyWorkspaceError::UnsupportedNode)?,
                            frozen.1.metadata.ok_or(LazyWorkspaceError::UnsupportedNode)?,
                        ),
                    };
                    let record = record_and_metadata.0;
                    if record.kind == FileKind::Regular {
                        let size = record_logical_bytes(record)
                            .ok_or(LazyWorkspaceError::UnsupportedNode)?;
                        logical_bytes = logical_bytes.checked_add(size).ok_or_else(counter_overflow)?;
                        maximum_regular_file_bytes = maximum_regular_file_bytes.max(size);
                    }
                    accounting.charge(WorkCounters {
                        items_examined: 1,
                        bytes_copied: std::mem::size_of::<SelectedImportRecord>() as u64,
                        ..WorkCounters::UNCHARGED
                    })?;
                    entries.push(SelectedImportRecord { path, record: Some(record_and_metadata) });
                }
                accounting.release(path_slots)?;
                for root in &roots {
                    let mut found = false;
                    for entry in &entries {
                        accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                        if entry.path == *root {
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        accounting.ensure_vec_push(&mut entries)?;
                        let path = accounting.canonical_path_owned(root, self.workspace.limits())?;
                        accounting.charge(WorkCounters {
                            bytes_copied: std::mem::size_of::<SelectedImportRecord>() as u64,
                            ..WorkCounters::UNCHARGED
                        })?;
                        entries.push(SelectedImportRecord { path, record: None });
                    }
                }
                drop(checkout);
                accounting.release(checkout_bytes)?;
                let mut owned_bytes = (roots.capacity() as u64)
                    .checked_mul(std::mem::size_of::<String>() as u64)
                    .and_then(|bytes| bytes.checked_add((entries.capacity() as u64)
                        .checked_mul(std::mem::size_of::<SelectedImportRecord>() as u64)?))
                    .ok_or_else(counter_overflow)?;
                for root in &roots {
                    accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                    owned_bytes = owned_bytes.checked_add(root.capacity() as u64).ok_or_else(counter_overflow)?;
                }
                for entry in &entries {
                    accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                    owned_bytes = owned_bytes.checked_add(entry.path.capacity() as u64).ok_or_else(counter_overflow)?;
                }
                let plan = SelectedImportPlan {
                    source: snapshot, source_config: self.workspace.volume_config(),
                    source_used, roots, entries, logical_bytes,
                    maximum_regular_file_bytes,
                    logical_owned_bytes: owned_bytes,
                };
                #[cfg(not(target_arch = "wasm32"))]
                if matches!(accounting.meter, LazyTreapMeter::NativeOriginal { .. }) {
                    let allocation = accounting.take_native_owned(owned_bytes)?
                        .ok_or(LazyWorkspaceError::NativeLedgerMismatch)?;
                    return Ok(LazyReadonlyNode::Native(NativeOwnedCoreState::from_parts(plan, allocation)));
                }
                Ok(LazyReadonlyNode::Portable(plan))
        }).await;
        #[cfg(not(target_arch = "wasm32"))]
        drop(scratch);
        result
    }

    /// Shares the canonical path ledger with the actual demand request.
    fn source_request_measured<'a, T, F, Fut>(
        &'a self,
        path: &'a str,
        budget: WorkBudget,
        cancellation: &'a CancellationToken,
        request: F,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<OperationReceipt<T>, LazyWorkspaceError>>
    where
        F: FnOnce(NamespacePath, WorkBudget) -> Fut + acyclic_stream::ProviderTask + 'a,
        Fut: Future<Output = crate::demand::DemandResult<T>> + acyclic_stream::ProviderTask + 'a,
    {
        Box::pin(async move {
            let mut allocations = crate::kernel::allocation::AllocationLedger::default();
            let mut work = WorkCounters::default();
            let result = async {
                let path = NamespacePath::parse_in_profile_with_ledger(
                    path, self.workspace.profile(), self.workspace.limits(),
                    &mut allocations, &mut work, budget, cancellation,
                ).map_err(directory_namespace_error)?;
                let live_bytes = allocations.live_bytes();
                let mut remaining = remaining_work(work, budget, 0)?;
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
        })
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

impl From<crate::FsError> for LazyWorkspaceError {
    fn from(error: crate::FsError) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        { native_filesystem_error(error) }
        #[cfg(target_arch = "wasm32")]
        { Self::Filesystem(error) }
    }
}

impl From<WorkspaceError> for LazyWorkspaceError {
    fn from(error: WorkspaceError) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        { native_workspace_error(error) }
        #[cfg(target_arch = "wasm32")]
        { Self::WorkspaceCause(error) }
    }
}

fn counter_overflow() -> LazyWorkspaceError {
    crate::WorkError::Overflow.into()
}

#[cfg(not(target_arch = "wasm32"))]
fn finish_native_frozen_read<A: AsyncAuthorityStore, O: AsyncObjectStore, T>(
    frozen: (ResolvedLazyLookup, NativeInspectState<A, O>), result: Result<T, LazyWorkspaceError>,
) -> Result<T,
    (LazyWorkspaceError, Option<crate::NativeAllocation>)> {
    let (resolved, NativeInspectState { reader, mut allocation, .. }) = frozen;
    drop(resolved);
    drop(reader);
    if let Err(retirement) = allocation.release(allocation.bytes()) {
        return match result {
            Err(primary) => Err(primary.into_native_failure_with_frame(Some(allocation))),
            Ok(value) => {
                drop(value);
                Err(LazyWorkspaceError::NativeWork(retirement).into_native_failure_with_frame(Some(allocation)))
            }
        };
    }
    match result {
        Ok(value) => { drop(allocation); Ok(value) }
        Err(error) => Err(error.into_native_failure_with_frame(Some(allocation))),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn native_filesystem_error(error: crate::FsError) -> LazyWorkspaceError {
    match error {
        crate::FsError::NativeKernel(error) => LazyWorkspaceError::NativeWork(error),
        crate::FsError::Cancelled(_) => LazyWorkspaceError::NativeWork(
            crate::NativeTreeMaterializationError::Cancelled),
        crate::FsError::NotFound => LazyWorkspaceError::NotFound,
        error => LazyWorkspaceError::Filesystem(error),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn native_workspace_error_pair(
    (source, custody): (WorkspaceError, Option<crate::materializer::NativeAllocation>),
) -> LazyWorkspaceError {
    LazyWorkspaceError::NativeWorkspaceCause(NativeLazyWorkspaceCause { source, custody })
}

#[cfg(not(target_arch = "wasm32"))]
fn native_workspace_error(error: WorkspaceError) -> LazyWorkspaceError {
    match error {
        WorkspaceError::Filesystem(error) => native_filesystem_error(error),
        WorkspaceError::Work(error)
        | WorkspaceError::NamespacePath(crate::kernel::NamespacePathError::Work(error)) =>
            LazyWorkspaceError::NativeWork(crate::NativeTreeMaterializationError::Work(error)),
        WorkspaceError::Cancelled(_)
        | WorkspaceError::NamespacePath(crate::kernel::NamespacePathError::Cancelled) =>
            LazyWorkspaceError::NativeWork(crate::NativeTreeMaterializationError::Cancelled),
        WorkspaceError::NamespacePath(error) => LazyWorkspaceError::NamespacePath(error),
        WorkspaceError::StaleSource => LazyWorkspaceError::StaleSource,
        WorkspaceError::StaleIdentity => LazyWorkspaceError::StaleIdentity,
        WorkspaceError::NotFound => LazyWorkspaceError::NotFound,
        WorkspaceError::NotRegularFile => LazyWorkspaceError::NotRegularFile,
        WorkspaceError::NotDirectory => LazyWorkspaceError::NotDirectory,
        WorkspaceError::ReadLimitExceeded => LazyWorkspaceError::TooLarge,
        error => LazyWorkspaceError::WorkspaceCause(error),
    }
}

fn workspace_error(error: WorkspaceError) -> LazyWorkspaceError {
    match error {
        WorkspaceError::StaleIdentity => LazyWorkspaceError::StaleIdentity,
        WorkspaceError::StaleSource => LazyWorkspaceError::StaleSource,
        WorkspaceError::Cancelled(_) => LazyWorkspaceError::Cancelled,
        WorkspaceError::Work(error) => error.into(),
        WorkspaceError::Filesystem(error) => LazyWorkspaceError::Filesystem(error),
        WorkspaceError::NamespacePath(error) => LazyWorkspaceError::NamespacePath(error),
        error => LazyWorkspaceError::Workspace(error.to_string()),
    }
}

fn measured_workspace_error(failure: crate::OperationFailure<WorkspaceError>) -> LazyWorkspaceError {
    lazy_operation_failure(failure, workspace_error)
}

struct LazyHeapFrame<'a, 'b> {
    work: &'a LazyTreapWork<'b>,
    bytes: u64,
}

impl Drop for LazyHeapFrame<'_, '_> {
    fn drop(&mut self) {
        self.work.release(self.bytes)
            .expect("lazy future releases exactly its admitted box after drop");
    }
}

async fn in_heap_lazy<T, E, F>(
    work: &LazyTreapWork<'_>, create: impl FnOnce() -> F,
) -> Result<T, E>
where
    E: From<LazyWorkspaceError>,
    F: Future<Output = Result<T, E>>,
{
    let mut frame = LazyHeapFrame { work, bytes: 0 };
    let (mut future, _) = in_heap_admitted(
        &mut frame,
        |frame, bytes| {
            frame.work.allocate(bytes)?;
            frame.bytes = bytes;
            Ok::<(), LazyWorkspaceError>(())
        },
        |_| create(),
    )?;
    let result = future.as_mut().await;
    drop(future);
    drop(frame);
    result
}

pub(crate) fn lazy_operation_failure<E>(
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
        posix_mode: source.posix_mode.map_or(MetadataField::Unavailable, MetadataField::Value),
        posix_uid: source.posix_uid.map_or(MetadataField::Unavailable, MetadataField::Value),
        posix_gid: source.posix_gid.map_or(MetadataField::Unavailable, MetadataField::Value),
        posix_flags: source.posix_flags.map_or(MetadataField::Unavailable, MetadataField::Value),
        windows_attributes: source.windows_attributes.map_or(MetadataField::Unavailable, MetadataField::Value),
        created_ns: time(source.created_ns),
        modified_ns: time(source.modified_ns),
        accessed_ns: time(source.accessed_ns),
        changed_ns: time(source.changed_ns),
        ..FileMetadata::default()
    }
}

fn lazy_priority_bytes(key: &[u8]) -> [u8; 8] {
    blake3::hash(key).as_bytes()[..8].try_into().expect("eight-byte digest prefix")
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

const NATIVE_PUBLICATION_SNAPSHOT_DOMAIN: &[u8] = b"acyclic-fs-native-publication-snapshot-v1\0";
const NATIVE_PUBLICATION_CONTEXT_DOMAIN: &[u8] = b"acyclic-fs-native-publication-context-v1\0";
const NATIVE_PUBLICATION_FIXED_BYTES: usize = 33 + crate::kernel::CONFIG_ENCODED_BYTES;

fn native_publication_context_with_accounting(
    evidence: &[u8], accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<crate::Digest, crate::FsError> {
    let hashing = WorkCounters {
        bytes_hashed: (NATIVE_PUBLICATION_CONTEXT_DOMAIN.len() as u64)
            .checked_add(crate::foundation::usize_to_u64(evidence.len()))
            .ok_or(crate::WorkError::Overflow)?,
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(hashing)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(NATIVE_PUBLICATION_CONTEXT_DOMAIN);
    hasher.update(evidence);
    let context = crate::Digest::from_bytes(*hasher.finalize().as_bytes());
    accounting.record_completed(hashing)?;
    Ok(context)
}

fn native_publication_encode_with_accounting(
    context: NativeSnapshotContext, operation: crate::OperationId,
    permit: crate::PublicationPermit, accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<(crate::Digest, Vec<u8>, crate::kernel::allocation::KernelOwnedAllocation), crate::FsError> {
    let source_bytes = if matches!(context, NativeSnapshotContext::Lazy(_)) { 88 } else { 0 };
    let permit_bytes = lazy_permit_bytes(permit);
    let size = NATIVE_PUBLICATION_SNAPSHOT_DOMAIN.len() + 2
        + NATIVE_PUBLICATION_FIXED_BYTES + source_bytes + permit_bytes;
    let mut allocation = accounting.claim_owned(0, 0)?;
    let mut owned_bytes = 0;
    let result = (|| {
        let mut encoder = lazy_encoder(NATIVE_PUBLICATION_SNAPSHOT_DOMAIN, size, accounting, &mut allocation, &mut owned_bytes, true)?;
        let encoding = WorkCounters {
            bytes_encoded: (NATIVE_PUBLICATION_FIXED_BYTES + source_bytes + permit_bytes) as u64,
            bytes_copied: (NATIVE_PUBLICATION_FIXED_BYTES + source_bytes + permit_bytes) as u64,
            ..WorkCounters::UNCHARGED
        };
        accounting.admit(encoding)?;
        encoder.fixed(&operation.into_bytes());
        encoder.u8(u8::from(matches!(context, NativeSnapshotContext::Lazy(_))));
        encoder.fixed(&context.workspace_id().into_bytes());
        crate::kernel::encode_config(&mut encoder, context.config());
        if let NativeSnapshotContext::Lazy(cut) = context {
            encoder.fixed(&cut.source.identity);
            encoder.u64(cut.source.epoch);
            encoder.fixed(&cut.overlay.into_bytes());
            encoder.fixed(&cut.shadows.into_bytes());
        }
        encode_lazy_permit(&mut encoder, permit);
        accounting.record_completed(encoding)?;
        if encoder.remaining_capacity() != encoder.capacity().checked_sub(size).ok_or(crate::WorkError::Overflow)? {
            return Err(native_publication_invalid());
        }
        let bytes = encoder.finish();
        let context = native_publication_context_with_accounting(&bytes, accounting)?;
        Ok((context, bytes))
    })();
    match result {
        Ok((context, bytes)) => Ok((context, bytes, allocation)),
        Err(error) => {
            accounting.release_owned(&mut allocation, owned_bytes)?;
            Err(error)
        }
    }
}

/// Funds the same native DATA evidence before any vector or sharing allocation.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn native_publication_evidence_with_native_work(
    context: NativeSnapshotContext, operation: crate::OperationId,
    original_permit: crate::PublicationPermit, original: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
) -> Result<(crate::Digest, acyclic_stream::OwnedBytes), crate::FsError> {
    let mut accounting = crate::kernel::allocation::KernelAccounting::native(original, cancellation);
    let (context, bytes, custody) = native_publication_encode_with_accounting(
        context, operation, original_permit, &mut accounting,
    )?;
    let bytes = acyclic_stream::OwnedBytes::from_vec_with_guard(bytes, custody, original.runtime_meter_owner())
        .map_err(|failure| -> crate::FsError { match failure.into_error() {
            acyclic_stream::PreparedOwnedBytesError::Admission(error) => original.runtime_error(error).into(),
            acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized
                =>
                crate::NativeTreeMaterializationError::InvalidLayout.into(),
        } })?;
    Ok((context, bytes))
}

fn native_publication_invalid() -> crate::FsError {
    crate::kernel::CanonicalDecodeError::Invariant(crate::kernel::CanonicalInvariant::Static(
        "original native publication qualification differs",
    )).into()
}

const LAZY_WRITE_INPUT_DOMAIN: &[u8] = b"acyclic-fs-lazy-write-input-v1\0";
const LAZY_WRITE_CONTEXT_DOMAIN: &[u8] = b"acyclic-fs-lazy-write-context-v1\0";
const LAZY_WRITE_ORIGINAL_DOMAIN: &[u8] = b"acyclic-fs-lazy-write-original-v1\0";
const LAZY_WRITE_ORIGINAL_FIXED_BYTES: usize = 216 + crate::kernel::CONFIG_ENCODED_BYTES;
const LAZY_WRITE_ORIGINAL_MAX_BYTES: usize = LAZY_WRITE_ORIGINAL_DOMAIN.len() + 2
    + LAZY_WRITE_ORIGINAL_FIXED_BYTES + 73;

fn lazy_permit_bytes(permit: crate::PublicationPermit) -> usize {
    match permit {
        crate::PublicationPermit::Unrestricted => 1,
        crate::PublicationPermit::Lease { .. } => 57,
        crate::PublicationPermit::Reservation { .. } => 73,
    }
}

fn encode_lazy_permit(encoder: &mut crate::kernel::Encoder, permit: crate::PublicationPermit) {
    match permit {
        crate::PublicationPermit::Unrestricted => encoder.u8(0),
        crate::PublicationPermit::Lease { authority_id, workspace_id, lease_id, expires_at_millis } => {
            encoder.u8(1);
            encoder.fixed(&authority_id);
            encoder.fixed(&workspace_id);
            encoder.fixed(&lease_id);
            encoder.u64(expires_at_millis);
        }
        crate::PublicationPermit::Reservation { operation_id, gate_tail, expected } => {
            encoder.u8(2);
            encoder.fixed(&operation_id);
            encoder.u64(gate_tail);
            encoder.u64(expected.epoch.get());
            encoder.u64(expected.sequence.get());
            encoder.fixed(expected.digest.as_bytes());
        }
    }
}

fn lazy_encoder(
    domain: &[u8], size: usize, accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    allocation: &mut crate::kernel::allocation::KernelOwnedAllocation, owned_bytes: &mut u64,
    copy_header: bool,
) -> Result<crate::kernel::Encoder, crate::FsError> {
    use crate::kernel::EncodeAllocationStep;
    crate::kernel::Encoder::with_exact_capacity(domain, 1, size, |step| match step {
        EncodeAllocationStep::Reserve { capacity } => {
            let bytes = crate::foundation::usize_to_u64(capacity);
            let mut incoming = accounting.claim_owned(bytes, u64::from(capacity != 0))?;
            allocation.absorb(&mut incoming)?;
            *owned_bytes = bytes;
            Ok(())
        }
        EncodeAllocationStep::Capacity { admitted_bytes, actual_bytes } => {
            *owned_bytes = actual_bytes;
            accounting.reconcile_owned(allocation, admitted_bytes, actual_bytes)
        }
        EncodeAllocationStep::BeforeHeader { bytes } => accounting.admit(WorkCounters {
            bytes_encoded: bytes, bytes_copied: if copy_header { bytes } else { 0 }, ..WorkCounters::UNCHARGED
        }),
        EncodeAllocationStep::HeaderWritten { bytes } => accounting.record_completed(WorkCounters {
            bytes_encoded: bytes, bytes_copied: if copy_header { bytes } else { 0 }, ..WorkCounters::UNCHARGED
        }),
    }, lazy_cursor_reservation_filesystem_error)
}

fn lazy_write_encode_original_with_accounting(
    prepared: PendingLazyWrite, accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<(Vec<u8>, crate::kernel::allocation::KernelOwnedAllocation), LazyWorkspaceError> {
    let permit_bytes = lazy_permit_bytes(prepared.permit);
    let size = LAZY_WRITE_ORIGINAL_DOMAIN.len() + 2 + LAZY_WRITE_ORIGINAL_FIXED_BYTES + permit_bytes;
    let mut allocation = accounting.claim_owned(0, 0).map_err(lazy_cursor_filesystem_error)?;
    let mut owned_bytes = 0;
    let result = (|| {
        let mut encoder = lazy_encoder(LAZY_WRITE_ORIGINAL_DOMAIN, size, accounting, &mut allocation, &mut owned_bytes, false).map_err(lazy_cursor_filesystem_error)?;
        let encoding = WorkCounters {
            bytes_encoded: (LAZY_WRITE_ORIGINAL_FIXED_BYTES + permit_bytes) as u64,
            bytes_copied: (LAZY_WRITE_ORIGINAL_FIXED_BYTES + permit_bytes) as u64,
            ..WorkCounters::UNCHARGED
        };
        accounting.admit(encoding).map_err(lazy_cursor_filesystem_error)?;
        encoder.fixed(&prepared.operation.into_bytes());
        encoder.fixed(&prepared.basis.id.into_bytes());
        encoder.fixed(&prepared.basis.workspace_id.into_bytes());
        encoder.fixed(prepared.basis.authored_generation.digest().as_bytes());
        encoder.fixed(&prepared.basis.source.identity);
        encoder.u64(prepared.basis.source.epoch);
        encoder.fixed(&prepared.basis.overlay.into_bytes());
        encoder.fixed(&prepared.basis.shadows.into_bytes());
        encoder.fixed(prepared.input.as_bytes());
        crate::kernel::encode_config(&mut encoder, prepared.config);
        encode_lazy_permit(&mut encoder, prepared.permit);
        accounting.record_completed(encoding).map_err(lazy_cursor_filesystem_error)?;
        if encoder.remaining_capacity() != encoder.capacity().checked_sub(size).ok_or_else(counter_overflow)? {
            return Err(LazyWorkspaceError::WriteInputConflict { operation: prepared.operation });
        }
        Ok(encoder.finish())
    })();
    match result {
        Ok(bytes) => Ok((bytes, allocation)),
        Err(error) => {
            accounting.release_owned(&mut allocation, owned_bytes).map_err(lazy_cursor_filesystem_error)?;
            Err(error)
        }
    }
}

fn lazy_write_original_bytes(
    prepared: PendingLazyWrite, work: &LazyTreapWork<'_>,
) -> Result<acyclic_stream::OwnedBytes, LazyWorkspaceError> {
    lazy_dispatch!{work, work.with_allocation_state(
            |allocations, spent, budget, cancellation| {
                let mut accounting = crate::kernel::allocation::KernelAccounting::borrowed_standalone(
                    allocations, spent, budget, cancellation,
                );
                let (bytes, _) = lazy_write_encode_original_with_accounting(prepared, &mut accounting)?;
                let control = WorkCounters { allocation_operations: 1, ..WorkCounters::UNCHARGED };
                accounting.admit(control).map_err(lazy_cursor_filesystem_error)?;
                let bytes: acyclic_stream::OwnedBytes = bytes.into();
                drop(bytes.clone());
                accounting.record_completed(control).map_err(lazy_cursor_filesystem_error)?;
                Ok(bytes)
            },
        ),
        |original| {
            let mut accounting = crate::kernel::allocation::KernelAccounting::native(original, work.cancellation);
            let (bytes, custody) = lazy_write_encode_original_with_accounting(prepared, &mut accounting)?;
            acyclic_stream::OwnedBytes::from_vec_with_guard(bytes, custody, original.runtime_meter_owner())
                .map_err(|failure| match failure.into_error() {
                    acyclic_stream::PreparedOwnedBytesError::Admission(error) =>
                        native_filesystem_error(original.runtime_error(error).into()),
                    acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized
                    =>
                        native_filesystem_error(crate::NativeTreeMaterializationError::InvalidLayout.into()),
                })
        }
    }
}

fn lazy_write_decode_original_with_accounting(
    bytes: &[u8], expected_operation: crate::OperationId,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<PendingLazyWrite, LazyWorkspaceError> {
    let mut observe = |step| lazy_cursor_fixed_read_step(accounting, step);
    let mut decoder = crate::kernel::Decoder::new::<crate::FsError>(
        bytes, LAZY_WRITE_ORIGINAL_DOMAIN, 1, LAZY_WRITE_ORIGINAL_MAX_BYTES as u64, &mut observe,
    ).map_err(lazy_cursor_filesystem_error)?;
    let operation = crate::OperationId::from_bytes(
        decoder.fixed::<16, crate::FsError>(&mut observe).map_err(lazy_cursor_filesystem_error)?);
    let snapshot_id = LazySnapshotId(
        decoder.fixed::<32, crate::FsError>(&mut observe).map_err(lazy_cursor_filesystem_error)?);
    let workspace_id = WorkspaceId::from_bytes(
        decoder.fixed::<16, crate::FsError>(&mut observe).map_err(lazy_cursor_filesystem_error)?);
    let authored_generation = crate::GenerationId::new(crate::Digest::from_bytes(
        decoder.fixed::<32, crate::FsError>(&mut observe).map_err(lazy_cursor_filesystem_error)?));
    let source = SourceReference {
        identity: decoder.fixed::<16, crate::FsError>(&mut observe).map_err(lazy_cursor_filesystem_error)?,
        epoch: decoder.u64(&mut observe).map_err(lazy_cursor_filesystem_error)?,
    };
    let overlay = LazyOverlayId::from_bytes(
        decoder.fixed::<32, crate::FsError>(&mut observe).map_err(lazy_cursor_filesystem_error)?);
    let shadows = LazyShadowId(
        decoder.fixed::<32, crate::FsError>(&mut observe).map_err(lazy_cursor_filesystem_error)?);
    let input = crate::Digest::from_bytes(
        decoder.fixed::<32, crate::FsError>(&mut observe).map_err(lazy_cursor_filesystem_error)?);
    let config = crate::kernel::decode_config(&mut decoder, &mut observe)
        .map_err(lazy_cursor_filesystem_error)?;
    let permit = match decoder.u8(&mut observe).map_err(lazy_cursor_filesystem_error)? {
        0 => crate::PublicationPermit::Unrestricted,
        1 => crate::PublicationPermit::Lease {
            authority_id: decoder.fixed::<16, crate::FsError>(&mut observe).map_err(lazy_cursor_filesystem_error)?,
            workspace_id: decoder.fixed::<16, crate::FsError>(&mut observe).map_err(lazy_cursor_filesystem_error)?,
            lease_id: decoder.fixed::<16, crate::FsError>(&mut observe).map_err(lazy_cursor_filesystem_error)?,
            expires_at_millis: decoder.u64(&mut observe).map_err(lazy_cursor_filesystem_error)?,
        },
        2 => {
            let operation_id = decoder.fixed::<16, crate::FsError>(&mut observe).map_err(lazy_cursor_filesystem_error)?;
            let gate_tail = decoder.u64(&mut observe).map_err(lazy_cursor_filesystem_error)?;
            let epoch = crate::Epoch::new(decoder.u64(&mut observe).map_err(lazy_cursor_filesystem_error)?)
                .map_err(|_| LazyWorkspaceError::WriteInputConflict { operation: expected_operation })?;
            let sequence = crate::Sequence::new(
                decoder.u64(&mut observe).map_err(lazy_cursor_filesystem_error)?);
            let digest = crate::Digest::from_bytes(
                decoder.fixed::<32, crate::FsError>(&mut observe).map_err(lazy_cursor_filesystem_error)?);
            crate::PublicationPermit::Reservation {
                operation_id, gate_tail, expected: crate::Head { epoch, sequence, digest },
            }
        }
        tag => return Err(lazy_cursor_filesystem_error(crate::kernel::CanonicalDecodeError::UnknownTag {
            field: "original lazy write permit", tag,
        }.into())),
    };
    decoder.finish().map_err(|error| lazy_cursor_filesystem_error(error.into()))?;
    let hashing = WorkCounters {
        bytes_hashed: (LAZY_SNAPSHOT_DOMAIN.len() + 136) as u64, ..WorkCounters::UNCHARGED
    };
    accounting.admit(hashing).map_err(lazy_cursor_filesystem_error)?;
    let basis = LazySnapshotRef::new(workspace_id, authored_generation, source, overlay, shadows);
    accounting.record_completed(hashing).map_err(lazy_cursor_filesystem_error)?;
    if operation != expected_operation || basis.id != snapshot_id {
        return Err(LazyWorkspaceError::WriteInputConflict { operation: expected_operation });
    }
    Ok(PendingLazyWrite { operation, basis, input, config, permit })
}

fn lazy_write_mutation_parts<'path>(
    mutation: LazyWriteMutation<'path>, work: &LazyTreapWork<'_>,
) -> Result<(&'path str, Option<acyclic_stream::OwnedBytes>), LazyWorkspaceError> {
    let (path, bytes) = match mutation {
        LazyWriteMutation::CreateDirectory { path } => (path, None),
        LazyWriteMutation::PutFile { path, bytes } => (path, Some(bytes)),
    };
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(bytes) = &bytes
        && let LazyTreapMeter::NativeOriginal { original, .. } = &work.meter
        && !bytes.matches_meter(&original.runtime_meter_owner())
    {
        return Err(LazyWorkspaceError::NativeLedgerMismatch);
    }
    Ok((path, bytes))
}

fn lazy_write_input_hasher(work: &LazyTreapWork<'_>) -> Result<blake3::Hasher, LazyWorkspaceError> {
    work.charge(WorkCounters {
        bytes_hashed: LAZY_WRITE_INPUT_DOMAIN.len() as u64, ..WorkCounters::UNCHARGED
    })?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(LAZY_WRITE_INPUT_DOMAIN);
    Ok(hasher)
}

fn lazy_write_hash_mutation(
    hasher: &mut blake3::Hasher, count: &mut u64, path: &str,
    bytes: Option<&acyclic_stream::OwnedBytes>, work: &LazyTreapWork<'_>,
) -> Result<(), LazyWorkspaceError> {
    let path_bytes = crate::foundation::usize_to_u64(path.len());
    let data_bytes = bytes.map_or(0, |bytes| crate::foundation::usize_to_u64(bytes.len()));
    work.charge(WorkCounters {
        bytes_hashed: path_bytes.checked_add(data_bytes).and_then(|bytes| bytes.checked_add(17))
            .ok_or_else(counter_overflow)?,
        items_examined: 1, ..WorkCounters::UNCHARGED
    })?;
    *count = count.checked_add(1).ok_or_else(counter_overflow)?;
    hasher.update(&[u8::from(bytes.is_some())]);
    hasher.update(&path_bytes.to_be_bytes());
    hasher.update(path.as_bytes());
    hasher.update(&data_bytes.to_be_bytes());
    if let Some(bytes) = bytes { hasher.update(bytes); }
    Ok(())
}

fn lazy_write_finish_input(
    mut hasher: blake3::Hasher, count: u64, work: &LazyTreapWork<'_>,
) -> Result<crate::Digest, LazyWorkspaceError> {
    work.charge(WorkCounters { bytes_hashed: 8, ..WorkCounters::UNCHARGED })?;
    hasher.update(&count.to_be_bytes());
    Ok(crate::Digest::from_bytes(*hasher.finalize().as_bytes()))
}

fn lazy_write_context_with_accounting(
    basis: LazySnapshotRef, input: crate::Digest,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<crate::Digest, LazyWorkspaceError> {
    let hashing = WorkCounters {
        bytes_hashed: LAZY_WRITE_CONTEXT_DOMAIN.len() as u64 + 64, ..WorkCounters::UNCHARGED
    };
    accounting.admit(hashing).map_err(lazy_cursor_filesystem_error)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(LAZY_WRITE_CONTEXT_DOMAIN);
    hasher.update(&basis.id.into_bytes());
    hasher.update(input.as_bytes());
    let digest = crate::Digest::from_bytes(*hasher.finalize().as_bytes());
    accounting.record_completed(hashing).map_err(lazy_cursor_filesystem_error)?;
    Ok(digest)
}

fn lazy_write_context(
    basis: LazySnapshotRef, input: crate::Digest, work: &LazyTreapWork<'_>,
) -> Result<crate::Digest, LazyWorkspaceError> {
    work.with_kernel(|accounting, _| lazy_write_context_with_accounting(basis, input, accounting))
}

trait LazyWriteGuard<A, O, D, S>: Send
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    type Error: From<LazyWorkspaceError> + Send;

    fn check(
        &mut self, work: &LazyTreapWork<'_>,
    ) -> impl Future<Output = Result<(), Self::Error>> + ProviderTask;

    #[allow(clippy::too_many_arguments)]
    fn commit(
        &mut self, lazy: &LazyWorkspace<A, O, D, S>, transaction: crate::Transaction<A, O>,
        context: Option<crate::Digest>, operation_evidence: Option<acyclic_stream::OwnedBytes>,
        basis: LazySnapshotRef, prepared: Option<PendingLazyWrite>,
        operation: crate::OperationId, permit: crate::PublicationPermit, work: &LazyTreapWork<'_>,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<TransactionCommit<A, O>, Self::Error>> + ProviderTask;
}

impl<A, O, D, S, E, F, Fut> LazyWriteGuard<A, O, D, S> for F
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
    E: From<LazyWorkspaceError> + Send,
    F: FnMut(WorkBudget) -> Fut + Send,
    Fut: Future<Output = crate::MeasuredResult<OperationReceipt<()>, E>> + Send,
{
    type Error = E;

    async fn check(&mut self, work: &LazyTreapWork<'_>) -> Result<(), E> {
        accept_lazy_write_receipt(work, self(work.remaining()?).await)
    }

    async fn commit(
        &mut self, lazy: &LazyWorkspace<A, O, D, S>, mut transaction: crate::Transaction<A, O>,
        context: Option<crate::Digest>, operation_evidence: Option<acyclic_stream::OwnedBytes>,
        basis: LazySnapshotRef, prepared: Option<PendingLazyWrite>,
        operation: crate::OperationId, permit: crate::PublicationPermit, work: &LazyTreapWork<'_>,
        cancellation: &CancellationToken,
    ) -> Result<TransactionCommit<A, O>, E> {
        let mut guard = |allowance| {
            let owner = self(allowance);
            async move {
                let seal = LazyTreapWork::new(allowance, cancellation);
                let result: Result<(), E> = async {
                    if let Some(prepared) = prepared {
                        lazy.write_prepared_basis(prepared, &seal).await?;
                    } else {
                        lazy.write_basis(basis, &seal).await?;
                    }
                    accept_lazy_write_receipt(&seal, owner.await)?;
                    if let Some(prepared) = prepared {
                        lazy.write_prepared_basis(prepared, &seal).await?;
                    } else {
                        lazy.write_basis(basis, &seal).await?;
                    }
                    Ok(())
                }.await;
                let spent = seal.spent();
                result.map(|()| OperationReceipt { value: (), work: spent })
                    .map_err(|error| crate::OperationFailure::new(
                        LazyWriteSealError::Owner(error), spent,
                    ))
            }
        };
        let result = if let Some(context) = context {
            transaction.commit(Some((context, operation_evidence.ok_or(LazyWorkspaceError::WriteIndeterminate {
                    operation,
                })?, prepared.map_or(permit, |prepared| prepared.permit))), permit, work.remaining()?, cancellation, &mut guard).await
        } else {
            transaction.commit(None, permit, work.remaining()?, cancellation, &mut guard).await
        }.map_err(|failure| failure.map_error(LazyWriteSealError::into_owner));
        accept_lazy_write_receipt(work, result)
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct OwnedNativeWriteGuard<A, O, D, S, F> {
    lazy: Arc<LazyWorkspace<A, O, D, S>>,
    callback: F,
    original: crate::NativeMaterializationWork,
    cancellation: CancellationToken,
}

#[cfg(not(target_arch = "wasm32"))]
impl<A, O, D, S, E, F, Fut> LazyWriteGuard<A, O, D, S>
    for OwnedNativeWriteGuard<A, O, D, S, F>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore + 'static,
    E: From<LazyWorkspaceError> + Send + 'static,
    F: for<'work> FnMut(&'work crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
{
    type Error = E;

    async fn check(&mut self, work: &LazyTreapWork<'_>) -> Result<(), E> {
        let _ = work.remaining()?;
        (self.callback)(&self.original).await
    }

    async fn commit(
        &mut self, _lazy: &LazyWorkspace<A, O, D, S>, mut transaction: crate::Transaction<A, O>,
        context: Option<crate::Digest>, operation_evidence: Option<acyclic_stream::OwnedBytes>,
        basis: LazySnapshotRef, prepared: Option<PendingLazyWrite>,
        operation: crate::OperationId, permit: crate::PublicationPermit, _work: &LazyTreapWork<'_>,
        _cancellation: &CancellationToken,
    ) -> Result<TransactionCommit<A, O>, E> {
        let copy = WorkCounters {
            bytes_copied: std::mem::size_of::<F>() as u64
                + std::mem::size_of::<Arc<LazyWorkspace<A, O, D, S>>>() as u64
                + std::mem::size_of::<crate::NativeMaterializationWork>() as u64
                + std::mem::size_of::<CancellationToken>() as u64,
            ..WorkCounters::UNCHARGED
        };
        self.original.admit(copy).map_err(LazyWorkspaceError::from)?;
        let mut callback = self.callback.clone();
        let lazy = Arc::clone(&self.lazy);
        let original = self.original.clone();
        let cancellation = self.cancellation.clone();
        let invocation_copy = WorkCounters {
            bytes_copied: std::mem::size_of::<Arc<LazyWorkspace<A, O, D, S>>>() as u64
                + std::mem::size_of::<crate::NativeMaterializationWork>() as u64
                + std::mem::size_of::<CancellationToken>() as u64,
            ..WorkCounters::UNCHARGED
        };
        let mut guard = move |_: &crate::NativeMaterializationWork| {
            let state = original.admit(invocation_copy).map(|()| (
                callback(&original), Arc::clone(&lazy), original.clone(), cancellation.clone(),
            ));
            async move {
                let result: Result<(), E> = async {
                    let (owner, lazy, original, cancellation) = state.map_err(LazyWorkspaceError::from)?;
                    let seal = LazyTreapWork::native_with_work(&original, &cancellation)?;
                    in_heap_lazy(&seal, || async {
                        if let Some(prepared) = prepared {
                            lazy.write_prepared_basis(prepared, &seal).await?;
                        } else {
                            lazy.write_basis(basis, &seal).await?;
                        }
                        owner.await?;
                        if let Some(prepared) = prepared {
                            lazy.write_prepared_basis(prepared, &seal).await?;
                        } else {
                            lazy.write_basis(basis, &seal).await?;
                        }
                        Ok(())
                    }).await
                }.await;
                result.map_err(LazyWriteSealError::Owner)
            }
        };
        let result = if let Some(context) = context {
            transaction.commit_with_native_work(
                Some((context, operation_evidence.ok_or(LazyWorkspaceError::WriteIndeterminate {
                    operation,
                })?, prepared.map_or(permit, |prepared| prepared.permit))),
                permit, &self.original, &self.cancellation, &mut guard,
            ).await
        } else {
            transaction.commit_with_native_work(
                None, permit, &self.original, &self.cancellation, &mut guard,
            ).await
        };
        result.map_err(LazyWriteSealError::into_owner)
    }
}

enum LazyWriteSealError<E> {
    Owner(E),
    Lazy(LazyWorkspaceError),
}

impl<E> From<WorkspaceError> for LazyWriteSealError<E> {
    fn from(error: WorkspaceError) -> Self { Self::Lazy(LazyWorkspaceError::WorkspaceCause(error)) }
}

impl<E> From<crate::FsError> for LazyWriteSealError<E> {
    fn from(error: crate::FsError) -> Self { Self::Lazy(LazyWorkspaceError::Filesystem(error)) }
}

impl<E: From<LazyWorkspaceError>> LazyWriteSealError<E> {
    fn into_owner(self) -> E {
        match self {
            Self::Owner(error) => error,
            Self::Lazy(error) => E::from(error),
        }
    }
}

fn accept_lazy_write_receipt<T, E: From<LazyWorkspaceError>>(
    work: &LazyTreapWork<'_>, result: crate::MeasuredResult<OperationReceipt<T>, E>,
) -> Result<T, E> {
    let (result, additional) = match result {
        Ok(receipt) => (Ok(receipt.value), receipt.work),
        Err(failure) => (Err(failure.error), *failure.work),
    };
    let recorded = work.accept(Ok(OperationReceipt { value: (), work: additional }));
    match result {
        Err(error) => Err(error),
        Ok(value) => { recorded?; Ok(value) }
    }
}

trait SelectedImportGuard<A: AsyncAuthorityStore, O: AsyncObjectStore>: Send {
    type Error: From<LazyWorkspaceError> + Send;

    fn map_lazy(&self, error: LazyWorkspaceError, _operation: crate::OperationId) -> Self::Error {
        error.into()
    }

    fn commit(
        &mut self,
        checkout: &mut crate::Checkout<A, O>,
        operation: crate::OperationId,
        permit: crate::PublicationPermit,
        accounting: &LazyTreapWork<'_>,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<crate::CheckoutCommitOutcome, Self::Error>> + ProviderTask;
}

impl<A, O, F, Fut> SelectedImportGuard<A, O> for F
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    F: FnMut(WorkBudget) -> Fut + Send,
    Fut: Future<Output = crate::FsResult<()>> + Send,
{
    type Error = LazyWorkspaceError;

    async fn commit(
        &mut self,
        checkout: &mut crate::Checkout<A, O>,
        operation: crate::OperationId,
        permit: crate::PublicationPermit,
        accounting: &LazyTreapWork<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::CheckoutCommitOutcome, Self::Error> {
        accounting.accept(checkout.commit_even_if_clean(
            operation, None, None, None, permit, accounting.remaining()?, cancellation, self, None,
        ).await.map_err(|failure|
            lazy_operation_failure(failure, |error| workspace_error(error.into()))))
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct OwnedNativeImportGuard<'a, F> {
    callback: &'a mut F,
    original: &'a crate::NativeMaterializationWork,
}

#[cfg(not(target_arch = "wasm32"))]
impl<A, O, F, Fut, E> SelectedImportGuard<A, O> for OwnedNativeImportGuard<'_, F>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    E: From<LazyWorkspaceError> + From<crate::FsError> + Send + 'static,
    F: for<'work> FnMut(&'work crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
{
    type Error = E;

    fn map_lazy(&self, error: LazyWorkspaceError, operation: crate::OperationId) -> E {
        E::from(match error {
            LazyWorkspaceError::OriginalSnapshotUnavailable =>
                LazyWorkspaceError::SelectedImportIndeterminate { operation },
            error => error,
        })
    }

    async fn commit(
        &mut self,
        checkout: &mut crate::Checkout<A, O>,
        operation: crate::OperationId,
        permit: crate::PublicationPermit,
        _accounting: &LazyTreapWork<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::CheckoutCommitOutcome, Self::Error> {
        checkout.commit_even_if_clean_with_native_work(operation, permit, self.original, cancellation, self.callback, None, None, None, None).await
    }
}

trait ExactifyPublication<A: AsyncAuthorityStore, O: AsyncObjectStore>: ProviderTask {
    type Error: From<LazyWorkspaceError> + Send;

    fn commit<D: DemandSource + 'static, S: LazyWorkspaceStore>(
        &mut self,
        lazy: &LazyWorkspace<A, O, D, S>,
        transaction: crate::Transaction<A, O>,
        permit: crate::PublicationPermit,
        current_source_used: bool,
        accounting: &mut ExactifyCollectorWork<'_>,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<crate::Generation<A, O>, Self::Error>> + ProviderTask;
}

impl<A, O, F, Fut> ExactifyPublication<A, O> for F
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    F: FnMut(WorkBudget) -> Fut + ProviderTask,
    Fut: Future<Output = crate::FsResult<()>> + ProviderTask,
{
    type Error = LazyWorkspaceError;

    async fn commit<D: DemandSource + 'static, S: LazyWorkspaceStore>(
        &mut self,
        lazy: &LazyWorkspace<A, O, D, S>,
        transaction: crate::Transaction<A, O>,
        permit: crate::PublicationPermit,
        _current_source_used: bool,
        accounting: &mut ExactifyCollectorWork<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::Generation<A, O>, Self::Error> {
        let receipt = lazy.commit_exactify_batch_guarded_with_accounting(
            transaction, permit, accounting.remaining()?, cancellation, self,
        ).await.map_err(|error| accounting.nested_error(error))?;
        accounting.accept(Ok(receipt))
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeOwnedBuilder<'a, B> {
    builder: &'a mut B,
    original: &'a crate::NativeMaterializationWork,
}

#[cfg(not(target_arch = "wasm32"))]
impl<A, O, B, G, Fut, E> ExactifyPublication<A, O> for NativeOwnedBuilder<'_, B>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    B: FnMut(bool) -> G + Send,
    G: for<'work> FnMut(&'work crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: From<LazyWorkspaceError> + From<crate::FsError> + Send + 'static,
{
    type Error = E;

    async fn commit<D: DemandSource + 'static, S: LazyWorkspaceStore>(
        &mut self,
        _lazy: &LazyWorkspace<A, O, D, S>,
        mut transaction: crate::Transaction<A, O>,
        permit: crate::PublicationPermit,
        current_source_used: bool,
        _accounting: &mut ExactifyCollectorWork<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::Generation<A, O>, Self::Error> {
        let frame = LazyTreapWork::native_with_work(self.original, cancellation)?;
        in_heap_lazy(&frame, || async {
            let mut guard = (self.builder)(current_source_used);
            let value = transaction.commit_with_native_work(
                None, permit, self.original, cancellation, &mut guard,
            ).await?;
            LazyWorkspace::<A, O, D, S>::exactify_batch_commit_result(
                value, WorkCounters::UNCHARGED, false,
            ).map(|receipt| receipt.value).map_err(E::from)
        }).await
    }
}

enum ExactifyCollectorWork<'a> {
    Portable {
        retained_bytes: &'a mut u64,
        work: &'a mut WorkCounters,
        budget: WorkBudget,
    },
    #[cfg(not(target_arch = "wasm32"))]
    Native {
        work: &'a LazyTreapWork<'a>,
        original: &'a crate::materializer::NativeMaterializationWork,
    },
}

impl ExactifyCollectorWork<'_> {
    fn with_kernel<T>(
        &mut self, cancellation: &CancellationToken,
        operation: impl FnOnce(&mut crate::kernel::allocation::KernelAccounting<'_>)
            -> Result<T, LazyWorkspaceError>,
    ) -> Result<T, LazyWorkspaceError> {
        match self {
            Self::Portable { work, budget, .. } => {
                let mut allocations = crate::kernel::allocation::AllocationLedger::default();
                let mut accounting = crate::kernel::allocation::KernelAccounting::borrowed_standalone(
                    &mut allocations, work, *budget, cancellation,
                );
                operation(&mut accounting)
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { original, .. } => {
                let mut accounting = crate::kernel::allocation::KernelAccounting::native(original, cancellation);
                operation(&mut accounting)
            }
        }
    }

    fn charge(
        &mut self, delta: WorkCounters, cancellation: &CancellationToken,
    ) -> Result<(), LazyWorkspaceError> {
        self.with_kernel(cancellation, |accounting|
            accounting.charge(delta).map_err(lazy_cursor_filesystem_error))
    }

    fn produce<T>(
        &mut self, cancellation: &CancellationToken,
        run: impl FnOnce(&mut crate::kernel::allocation::KernelAccounting<'_>, &CancellationToken)
            -> Result<(T, crate::kernel::allocation::KernelOwnedAllocation), LazyWorkspaceError>,
    ) -> Result<T, LazyWorkspaceError> {
        match self {
            Self::Portable { retained_bytes, work, budget } => {
                let mut allocations = crate::kernel::allocation::AllocationLedger::default();
                allocations.claim_bytes(**retained_bytes, 0, work, *budget).map_err(lazy_allocation_error)?;
                let mut accounting = crate::kernel::allocation::KernelAccounting::borrowed_standalone(
                    &mut allocations, work, *budget, cancellation,
                );
                let result = run(&mut accounting, cancellation);
                drop(accounting);
                **retained_bytes = allocations.live_bytes();
                result.map(|(value, allocation)| { drop(allocation); value })
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { work, .. } => work.produce(run),
        }
    }

    fn clone_name(&mut self, name: &LogicalName, cancellation: &CancellationToken) -> Result<LogicalName, LazyWorkspaceError> {
        self.produce(cancellation, |accounting, cancellation|
            name.clone_with_accounting(accounting, cancellation).map_err(lazy_cursor_filesystem_error))
    }

    fn vector<T>(&mut self, count: usize, cancellation: &CancellationToken) -> Result<Vec<T>, LazyWorkspaceError> {
        self.produce(cancellation, |accounting, cancellation|
            directory_vec_with_accounting(count, accounting, cancellation))
    }

    fn namespace(
        &mut self, path: &str, profile: crate::model::FilesystemProfile,
        limits: crate::VolumeLimits, cancellation: &CancellationToken,
    ) -> Result<NamespacePath, LazyWorkspaceError> {
        self.produce(cancellation, |accounting, cancellation|
            NamespacePath::parse_in_profile_with_accounting(path, profile, limits, accounting, cancellation)
                .map_err(lazy_cursor_filesystem_error))
    }

    fn queue(
        &mut self, pending: &mut Vec<AuthoredMutation>, mutation: AuthoredMutation,
        paid_path_bytes: u64, pending_bytes: &mut u64,
    ) -> Result<(), LazyWorkspaceError> {
        let old_bytes = (pending.capacity() as u64).checked_mul(std::mem::size_of::<AuthoredMutation>() as u64)
            .ok_or_else(counter_overflow)?;
        self.reserve(pending)?;
        let new_bytes = (pending.capacity() as u64).checked_mul(std::mem::size_of::<AuthoredMutation>() as u64)
            .ok_or_else(counter_overflow)?;
        *pending_bytes = pending_bytes.checked_add(paid_path_bytes)
            .and_then(|bytes| bytes.checked_add(new_bytes.checked_sub(old_bytes)?))
            .ok_or_else(counter_overflow)?;
        pending.push(mutation);
        Ok(())
    }

    fn replace_generation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &mut self, current: &mut crate::Generation<A, O>, replacement: crate::Generation<A, O>,
    ) -> Result<(), LazyWorkspaceError> {
        match self {
            Self::Portable { retained_bytes, work, budget } =>
                replace_exactify_generation(current, replacement, retained_bytes, work, *budget),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { .. } => { *current = replacement; Ok(()) }
        }
    }

    fn context_error(&self, error: LazyWorkspaceError) -> LazyWorkspaceError {
        match self {
            Self::Portable { .. } => exactify_error(error),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { .. } => error,
        }
    }

    fn check(&mut self, cancellation: &CancellationToken) -> Result<(), LazyWorkspaceError> {
        match self {
            Self::Portable { .. } => cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { original, .. } => {
                original.admit(WorkCounters::UNCHARGED)?;
                cancellation.check().map_err(|_| LazyWorkspaceError::NativeWork(
                    crate::NativeTreeMaterializationError::Cancelled))
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn original(&self) -> Option<&crate::materializer::NativeMaterializationWork> {
        match self {
            Self::Native { original, .. } => Some(original),
            Self::Portable { .. } => None,
        }
    }

    fn remaining(&self) -> Result<WorkBudget, LazyWorkspaceError> {
        match self {
            Self::Portable { retained_bytes, work, budget } =>
                remaining_work(**work, *budget, **retained_bytes),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { .. } => Err(LazyWorkspaceError::NativeLedgerMismatch),
        }
    }

    fn record(&mut self, additional: WorkCounters) -> Result<(), LazyWorkspaceError> {
        match self {
            Self::Portable { retained_bytes, work, budget } => {
                **work = account_nested_with_live_memory(**work, additional, **retained_bytes, *budget)?;
                Ok(())
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { .. } => Err(LazyWorkspaceError::NativeLedgerMismatch),
        }
    }

    fn nested_error(&self, error: LazyWorkspaceError) -> LazyWorkspaceError {
        match self {
            Self::Portable { retained_bytes, .. } => nested_lazy_failure(error, **retained_bytes),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { .. } => error,
        }
    }

    fn accept<T>(&mut self, result: Result<OperationReceipt<T>, LazyWorkspaceError>) -> Result<T, LazyWorkspaceError> {
        let receipt = result?;
        self.record(receipt.work)?;
        Ok(receipt.value)
    }

    fn release(&mut self, bytes: u64) -> Result<(), LazyWorkspaceError> {
        match self {
            Self::Portable { retained_bytes, .. } => {
                **retained_bytes = retained_bytes.checked_sub(bytes).ok_or_else(counter_overflow)?;
                Ok(())
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { work, .. } => work.release(bytes),
        }
    }

    fn retain_page(&mut self, bytes: u64) -> Result<(), LazyWorkspaceError> {
        match self {
            Self::Portable { retained_bytes, work, budget } => {
                **retained_bytes = retained_bytes.checked_add(bytes).ok_or_else(counter_overflow)?;
                work.peak_allocation_bytes = work.peak_allocation_bytes.max(**retained_bytes);
                work.verify(*budget)?;
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { .. } => {}
        }
        Ok(())
    }

    fn release_page_bytes(&mut self, bytes: u64) -> Result<(), LazyWorkspaceError> {
        if matches!(self, Self::Portable { .. }) { self.release(bytes)?; }
        Ok(())
    }



    fn release_portable_cursor(&mut self, cursor: Option<&LazyDirectoryCursor>) -> Result<(), LazyWorkspaceError> {
        if matches!(self, Self::Portable { .. }) {
            if let Some(cursor) = cursor { self.release(cursor.retained_bytes()?)?; }
        }
        Ok(())
    }

    fn reserve<T>(&mut self, values: &mut Vec<T>) -> Result<(), LazyWorkspaceError> {
        match self {
            Self::Portable { retained_bytes, work, budget } =>
                reserve_exactify_slot(values, retained_bytes, work, *budget),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { work, .. } => work.ensure_vec_push(values),
        }
    }

    fn clone_string(&mut self, value: &str, cancellation: &CancellationToken) -> Result<String, LazyWorkspaceError> {
        self.produce(cancellation, |accounting, cancellation|
            clone_lazy_string_with_accounting(value, accounting, cancellation))
    }

    fn child_path(&mut self, directory: &str, name: &LogicalName, cancellation: &CancellationToken) -> Result<String, LazyWorkspaceError> {
        if name.unicode_utf8_bytes().is_none() { return Err(LazyWorkspaceError::UnsupportedNode); }
        self.produce(cancellation, |accounting, cancellation| {
            let (value, allocation) = directory_child_path_with_accounting(directory, name, accounting, cancellation)?;
            value.map(|value| (value, allocation)).ok_or(LazyWorkspaceError::UnsupportedNode)
        })
    }

    fn dedup(&mut self, paths: &mut Vec<String>, cancellation: &CancellationToken) -> Result<(), LazyWorkspaceError> {
        match self {
            Self::Portable { retained_bytes, work, budget } => {
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let mut accounting = crate::kernel::allocation::KernelAccounting::borrowed_standalone(
            &mut allocations, work, *budget, cancellation,
        );
        dedup_exactify_paths(paths, &mut accounting, cancellation, |bytes| {
            **retained_bytes = retained_bytes.checked_sub(bytes).ok_or_else(counter_overflow)?;
            Ok(())
        })
            },
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { work, original } => {
                let mut accounting = crate::kernel::allocation::KernelAccounting::native(original, cancellation);
                dedup_exactify_paths(paths, &mut accounting, cancellation, |bytes|
                    work.release(bytes))
            }
        }
    }
}

fn exactify_root_is_covered(
    roots: &[String], index: usize,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<bool, LazyWorkspaceError> {
    let root = roots.get(index).ok_or(LazyWorkspaceError::Concurrent)?;
    for (other_index, other) in roots.iter().enumerate() {
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })
            .map_err(lazy_cursor_filesystem_error)?;
        if (root == other && other_index < index)
            || (root != other && (other == "/"
                || root.strip_prefix(other.as_str()).is_some_and(|suffix| suffix.starts_with('/')))) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn dedup_exactify_paths(
    paths: &mut Vec<String>, accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken, mut release: impl FnMut(u64) -> Result<(), LazyWorkspaceError>,
) -> Result<(), LazyWorkspaceError> {
    let mut write = 0_usize;
    for read in 0..paths.len() {
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })
            .map_err(lazy_cursor_filesystem_error)?;
        if write != 0 && paths[write - 1] == paths[read] {
            let duplicate = std::mem::take(&mut paths[read]);
            let bytes = duplicate.capacity() as u64;
            drop(duplicate);
            release(bytes)?;
        } else {
            if read != write {
                let copied = WorkCounters { bytes_copied: (2 * std::mem::size_of::<String>()) as u64,
                    ..WorkCounters::UNCHARGED };
                accounting.admit(copied).map_err(lazy_cursor_filesystem_error)?;
                paths.swap(read, write);
                accounting.record_completed(copied).map_err(lazy_cursor_filesystem_error)?;
            }
            write += 1;
        }
    }
    paths.truncate(write);
    Ok(())
}

fn filter_selected_removals(
    removed: &mut Vec<String>, present: &[String], work: &LazyTreapWork<'_>,
    cancellation: &CancellationToken, mut check: impl FnMut() -> Result<(), LazyWorkspaceError>,
) -> Result<(), LazyWorkspaceError> {
    let mut write = 0;
    for read in 0..removed.len() {
        let exists = work.with_kernel(|accounting, _|
            exactify_contains_path(present, &removed[read], accounting, cancellation))?;
        if exists {
            let path = std::mem::take(&mut removed[read]);
            let bytes = path.capacity() as u64;
            drop(path);
            work.release(bytes)?;
        } else {
            if read != write {
                work.charge(WorkCounters { bytes_copied: (2 * std::mem::size_of::<String>()) as u64,
                    ..WorkCounters::UNCHARGED })?;
                removed.swap(read, write);
            }
            write += 1;
        }
    }
    removed.truncate(write);
    for left in 0..removed.len() / 2 {
        check()?;
        work.charge(WorkCounters { bytes_copied: (2 * std::mem::size_of::<String>()) as u64,
            ..WorkCounters::UNCHARGED })?;
        let right = removed.len() - left - 1;
        removed.swap(left, right);
    }
    Ok(())
}

fn exactify_contains_path(
    paths: &[String], path: &str,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<bool, LazyWorkspaceError> {
    let mut start = 0_usize;
    let mut end = paths.len();
    while start < end {
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })
            .map_err(lazy_cursor_filesystem_error)?;
        let middle = start + (end - start) / 2;
        match exactify_path_order(&paths[middle], path) {
            std::cmp::Ordering::Less => start = middle + 1,
            std::cmp::Ordering::Greater => end = middle,
            std::cmp::Ordering::Equal => return Ok(true),
        }
    }
    Ok(false)
}

fn exactify_path_order(left: &str, right: &str) -> std::cmp::Ordering {
    left.matches('/')
        .count()
        .cmp(&right.matches('/').count())
        .then_with(|| left.cmp(right))
}

fn sort_exactify_paths_with_accounting(
    values: &mut [String], accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(), LazyWorkspaceError> {
    fn compare(
        left: &str, right: &str, accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<std::cmp::Ordering, LazyWorkspaceError> {
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })
            .map_err(lazy_cursor_filesystem_error)?;
        Ok(exactify_path_order(left, right))
    }
    fn swap(
        values: &mut [String], left: usize, right: usize,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let delta = WorkCounters { bytes_copied: crate::foundation::usize_to_u64(
            2 * std::mem::size_of::<String>(),
        ), ..WorkCounters::UNCHARGED };
        accounting.admit(delta).map_err(lazy_cursor_filesystem_error)?;
        values.swap(left, right);
        accounting.record_completed(delta).map_err(lazy_cursor_filesystem_error)?;
        Ok(())
    }
    fn sift(
        values: &mut [String], mut root: usize, end: usize,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<(), LazyWorkspaceError> {
        while root < end / 2 {
            let mut child = root * 2 + 1;
            if child + 1 < end
                && compare(&values[child], &values[child + 1], accounting, cancellation)?.is_lt() {
                child += 1;
            }
            if !compare(&values[root], &values[child], accounting, cancellation)?.is_lt() {
                break;
            }
            swap(values, root, child, accounting)?;
            root = child;
        }
        Ok(())
    }
    cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
    let length = values.len();
    for root in (0..length / 2).rev() {
        sift(values, root, length, accounting, cancellation)?;
    }
    for end in (1..length).rev() {
        cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
        swap(values, 0, end, accounting)?;
        sift(values, 0, end, accounting, cancellation)?;
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
    let mut allocations = crate::kernel::allocation::AllocationLedger::default();
    allocations.claim_bytes(*retained_bytes, 0, work, budget).map_err(lazy_allocation_error)?;
    let target = values.capacity().checked_mul(2).map(|value| value.max(1))
        .ok_or_else(counter_overflow)?;
    let copied = crate::foundation::usize_to_u64(
        values.len().checked_mul(width).ok_or_else(counter_overflow)?,
    );
    work.admit(&WorkCounters { bytes_copied: copied, ..WorkCounters::default() }, &budget)?;
    allocations.claim_elements::<T>(target, work, budget).map_err(lazy_allocation_error)?;
    let mut replacement = Vec::new();
    replacement.try_reserve_exact(target).map_err(lazy_reservation_error)?;
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

pub(crate) fn retained_directory_page_bytes<T>(
    entries: &Vec<T>,
    mut names: impl Iterator<Item = usize>,
) -> Result<u64, LazyWorkspaceError> {
    let slots = entries.capacity()
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(counter_overflow)?;
    let names = names
        .try_fold(0_usize, |total, bytes| total.checked_add(bytes))
        .ok_or_else(counter_overflow)?;
    u64::try_from(slots.checked_add(names).ok_or_else(counter_overflow)?)
        .map_err(|_| counter_overflow())
}

enum DirectoryAuthoredEntries {
    Mounted(Vec<crate::kernel::TreeEntry>),
    Workspace(Vec<WorkspaceDirectoryEntry>),
    #[cfg(not(target_arch = "wasm32"))]
    Original(Vec<crate::DirectoryRecordEntry>),
}

impl DirectoryAuthoredEntries {
    fn len(&self) -> usize {
        match self {
            Self::Mounted(entries) => entries.len(),
            Self::Workspace(entries) => entries.len(),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Original(entries) => entries.len(),
        }
    }

    fn retained_bytes(&self, include_names: bool) -> Result<u64, LazyWorkspaceError> {
        macro_rules! bytes {
            ($entries:expr) => {
                retained_directory_page_bytes($entries,
                    $entries.iter().map(|entry| if include_names { entry.name.retained_bytes() } else { 0 }))
            };
        }
        match self {
            Self::Mounted(entries) => bytes!(entries),
            Self::Workspace(entries) => bytes!(entries),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Original(entries) => bytes!(entries),
        }
    }

    fn last_name(&self) -> Option<&LogicalName> {
        match self {
            Self::Mounted(entries) => entries.last().map(|entry| &entry.name),
            Self::Workspace(entries) => entries.last().map(|entry| &entry.name),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Original(entries) => entries.last().map(|entry| &entry.name),
        }
    }

    fn append_into(self, output: &mut Vec<LazyDirectoryEntry>) {
        match self {
            Self::Mounted(entries) => output.extend(entries.into_iter().map(|entry| LazyDirectoryEntry {
                name: entry.name, kind: source_kind(entry.kind), authored: true,
                source: None, unobserved: false,
            })),
            Self::Workspace(entries) => output.extend(entries.into_iter().map(authored_entry)),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Original(entries) => output.extend(entries.into_iter().map(|entry| LazyDirectoryEntry {
                name: entry.name, kind: source_kind(entry.record.kind), authored: true,
                source: None, unobserved: false,
            })),
        }
    }
}

enum DirectoryProjectionWork<'a> {
    Portable {
        work: WorkCounters,
        allocations: crate::kernel::allocation::AllocationLedger,
        budget: WorkBudget,
        cancellation: &'a CancellationToken,
    },
    #[cfg(not(target_arch = "wasm32"))]
    Native {
        original: &'a crate::materializer::NativeMaterializationWork,
        cancellation: &'a CancellationToken,
        allocation: crate::materializer::NativeAllocation,
    },
}

enum DirectoryProjectionPage {
    Portable(OperationReceipt<LazyDirectoryPage>),
    #[cfg(not(target_arch = "wasm32"))]
    Native(NativeLazyDirectoryPage),
}

enum DirectoryPublicationReservation {
    Portable(WorkBudget),
    #[cfg(not(target_arch = "wasm32"))]
    Native,
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct NativeLazyDirectoryPage {
    pub(crate) page: LazyDirectoryPage,
    pub(crate) allocation: crate::materializer::NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl std::ops::Deref for NativeLazyDirectoryPage {
    type Target = LazyDirectoryPage;
    fn deref(&self) -> &Self::Target { &self.page }
}

#[cfg(not(target_arch = "wasm32"))]
impl std::ops::DerefMut for NativeLazyDirectoryPage {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.page }
}

impl<'a> DirectoryProjectionWork<'a> {
    #[cfg(not(target_arch = "wasm32"))]
    fn native(
        original: &'a crate::NativeMaterializationWork,
        cancellation: &'a CancellationToken,
    ) -> Result<Self, LazyWorkspaceError> {
        original.admit(WorkCounters::UNCHARGED)?;
        cancellation.check().map_err(|_| LazyWorkspaceError::NativeWork(
            crate::NativeTreeMaterializationError::Cancelled))?;
        let allocation = original.claim_allocation(0, 0, crate::WorkCounters::UNCHARGED)?;
        Ok(Self::Native { original, cancellation, allocation })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_original(&self) -> Option<&'a crate::NativeMaterializationWork> {
        match self {
            Self::Native { original, .. } => Some(*original),
            Self::Portable { .. } => None,
        }
    }

    fn is_native(&self) -> bool {
        match self {
            Self::Portable { .. } => false,
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { .. } => true,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn absorb(&mut self, incoming: &mut crate::NativeAllocation) -> Result<(), LazyWorkspaceError> {
        match self {
            Self::Native { allocation, .. } => allocation.absorb(incoming).map_err(Into::into),
            Self::Portable { .. } => unreachable!("original query preserves original projection"),
        }
    }

    fn produce<T>(&mut self, run: impl FnOnce(
        &mut crate::kernel::allocation::KernelAccounting<'_>, &CancellationToken,
    ) -> Result<(T, crate::kernel::allocation::KernelOwnedAllocation), LazyWorkspaceError>)
    -> Result<T, LazyWorkspaceError> {
        match self {
            Self::Portable { work, allocations, budget, cancellation } => {
                let mut accounting = crate::kernel::allocation::KernelAccounting::borrowed_standalone(
                    allocations, work, *budget, cancellation,
                );
                run(&mut accounting, cancellation).map(|(value, _)| value)
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { original, cancellation, allocation } => {
                let mut accounting = crate::kernel::allocation::KernelAccounting::native(original, cancellation);
                let (value, custody) = run(&mut accounting, cancellation)?;
                match custody {
                    crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(mut owned) => {
                        if let Err(error) = allocation.absorb(&mut owned) {
                            drop(value);
                            drop(owned);
                            return Err(LazyWorkspaceError::NativeWork(error));
                        }
                        Ok(value)
                    }
                    crate::kernel::allocation::KernelOwnedAllocation::Standalone => {
                        drop(value);
                        Err(LazyWorkspaceError::OriginalSnapshotUnavailable)
                    }
                }
            }
        }
    }

    fn remaining(&mut self) -> Result<WorkBudget, LazyWorkspaceError> {
        match self {
            Self::Portable { work, allocations, budget, .. } =>
                remaining_work(*work, *budget, allocations.live_bytes()),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { original, .. } => original.remaining().map_err(Into::into),
        }
    }

    fn accept<T>(&mut self, result: Result<OperationReceipt<T>, LazyWorkspaceError>)
    -> Result<T, LazyWorkspaceError> {
        match self {
            Self::Portable { work, allocations, .. } => accept_directory_work(result, work, allocations),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { .. } =>
                unreachable!("original directory dispatch never accepts a standalone receipt"),
        }
    }

    fn claim(&mut self, bytes: u64, operations: u64) -> Result<(), LazyWorkspaceError> {
        self.produce(|accounting, _| accounting.claim_owned(bytes, operations)
            .map(|allocation| ((), allocation)).map_err(lazy_cursor_filesystem_error))
    }

    fn claim_frame(&mut self, bytes: u64) -> Result<(), LazyWorkspaceError> {
        let result = self.claim(bytes, u64::from(bytes != 0));
        match self {
            Self::Portable { work, .. } => result.map_err(|error| error.with_prior_work(*work)),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { .. } => result,
        }
    }

    fn release(&mut self, bytes: u64) -> Result<(), LazyWorkspaceError> {
        match self {
            Self::Portable { allocations, .. } => allocations.release(bytes).map_err(lazy_allocation_error),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { allocation, .. } => allocation.release(bytes).map_err(Into::into),
        }
    }

    fn items(&mut self, count: u64) -> Result<(), LazyWorkspaceError> {
        match self {
            Self::Portable { work, budget, cancellation, .. } => {
                cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
                work.charge_items(count, budget).map_err(Into::into)
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { original, cancellation, .. } => {
                cancellation.check().map_err(|_| LazyWorkspaceError::NativeWork(
                    crate::NativeTreeMaterializationError::Cancelled))?;
                original.charge(WorkCounters { items_examined: count, ..WorkCounters::UNCHARGED })
                    .map_err(Into::into)
            }
        }
    }

    fn vec<T>(&mut self, count: usize) -> Result<Vec<T>, LazyWorkspaceError> {
        self.produce(|accounting, cancellation|
            directory_vec_with_accounting(count, accounting, cancellation))
    }

    fn clone_name(&mut self, name: &LogicalName) -> Result<LogicalName, LazyWorkspaceError> {
        self.produce(|accounting, cancellation|
            name.clone_with_accounting(accounting, cancellation).map_err(lazy_cursor_filesystem_error))
    }

    fn child_text(&mut self, parent: &str, name: &LogicalName) -> Result<Option<String>, LazyWorkspaceError> {
        self.produce(|accounting, cancellation|
            directory_child_path_with_accounting(parent, name, accounting, cancellation))
    }

    fn mask_contains(&mut self, paths: &[String], child: &str) -> Result<bool, LazyWorkspaceError> {
        directory_mask_contains(paths, child, || self.items(1))
    }

    async fn source_lookup<S: DemandSource>(
        &mut self, source: &S, reference: SourceReference, path: &NamespacePath,
        cancellation: &CancellationToken,
    ) -> Result<Option<SourceNode>, LazyWorkspaceError> {
        match self {
            Self::Portable { .. } => {
                let remaining = self.remaining()?;
                let response = source.lookup(reference, path, remaining, cancellation)
                    .await.map_err(|failure| lazy_operation_failure(failure, LazyWorkspaceError::from));
                self.accept(response)
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { original, .. } => source.lookup(
                reference, path, *original, cancellation).await.map_err(LazyWorkspaceError::from),
        }
    }

    async fn source_page<S: DemandSource>(
        &mut self, source: &S, reference: SourceReference, directory: &NamespacePath,
        cursor: Option<SourceCursor>, maximum_entries: u32, cancellation: &CancellationToken,
    ) -> Result<crate::demand::SourceDirectoryPage, LazyWorkspaceError> {
        match self {
            Self::Portable { .. } => {
                let remaining = self.remaining()?;
                let response = source.list_page(reference, directory, cursor, maximum_entries, remaining, cancellation)
                    .await.map_err(|failure| lazy_operation_failure(failure, LazyWorkspaceError::from));
                self.accept(response)
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { original, .. } => source.list_page(
                reference, directory, cursor, maximum_entries, *original, cancellation).await.map_err(LazyWorkspaceError::from),
        }
    }

    fn reserve_publication(&mut self, cleanup: WorkCounters)
    -> Result<DirectoryPublicationReservation, LazyWorkspaceError> {
        match self {
            Self::Portable { work, budget, .. } => {
                work.admit(&cleanup, budget)?;
                let prior = *budget;
                *budget = remaining_work(cleanup, prior, 0)?;
                Ok(DirectoryPublicationReservation::Portable(prior))
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { .. } => Ok(DirectoryPublicationReservation::Native),
        }
    }

    fn restore_publication(&mut self, reservation: DirectoryPublicationReservation) {
        match (self, reservation) {
            (Self::Portable { budget, .. }, DirectoryPublicationReservation::Portable(prior)) => *budget = prior,
            #[cfg(not(target_arch = "wasm32"))]
            (Self::Native { .. }, DirectoryPublicationReservation::Native) => {}
            #[cfg(not(target_arch = "wasm32"))]
            _ => unreachable!("directory publication retains its original work mode"),
        }
    }

    async fn accept_publication(
        &mut self, publication: &mut Option<Box<dyn crate::demand::SourceDirectoryPublication>>, cancellation: &CancellationToken,
    ) -> Result<(), LazyWorkspaceError> {
        match self {
            Self::Portable { .. } => {
                let remaining = self.remaining()?;
                let response = crate::demand::accept_publication(publication, remaining, cancellation).await
                    .map_err(|failure| lazy_operation_failure(failure, LazyWorkspaceError::from));
                self.accept(response)
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { original, .. } => crate::demand::accept_publication(publication, *original, cancellation).await
                .map_err(LazyWorkspaceError::from),
        }
    }

    fn reserve_source_cleanup(&mut self) -> Result<LazyDirectoryCleanupCause, LazyWorkspaceError> {
        let bytes = LazyDirectoryCleanupCause::allocation_bytes();
        match self {
            Self::Portable { work, allocations, budget, cancellation } => {
                cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
                allocations.claim_bytes(bytes, 1, work, *budget).map_err(lazy_allocation_error)?;
                Ok(LazyDirectoryCleanupCause::new(
                    Box::new(None),
                    #[cfg(not(target_arch = "wasm32"))]
                    None,
                ))
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { original, cancellation, .. } => {
                original.admit(WorkCounters::UNCHARGED)?;
                cancellation.check().map_err(|_| LazyWorkspaceError::NativeWork(
                    crate::NativeTreeMaterializationError::Cancelled))?;
                let allocation = original.claim(bytes)?;
                Ok(LazyDirectoryCleanupCause::new(Box::new(None), Some(allocation)))
            }
        }
    }

    fn retire_source_cleanup(&mut self, slot: LazyDirectoryCleanupCause) -> Result<(), LazyWorkspaceError> {
        drop(slot);
        self.retire_source_cleanup_bytes()
    }

    fn retire_source_cleanup_bytes(&mut self) -> Result<(), LazyWorkspaceError> {
        match self {
            Self::Portable { allocations, .. } => allocations.release(
                LazyDirectoryCleanupCause::allocation_bytes(),
            ).map_err(lazy_allocation_error),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { .. } => Ok(()),
        }
    }

    fn abort_publication(&mut self, publication: &mut Option<Box<dyn crate::demand::SourceDirectoryPublication>>)
    -> Result<(), DemandError> {
        match self {
            Self::Portable { work, allocations, budget, .. } => {
                // Cleanup retires physical tokens without consulting cancellation.
                let mut remaining = work.remaining(*budget)?;
                remaining.peak_allocation_bytes = remaining.peak_allocation_bytes
                    .checked_sub(allocations.live_bytes()).ok_or(crate::WorkError::Overflow)?;
                let (answer, spent) = match crate::demand::abort_publication(publication, remaining) {
                    Ok(receipt) => (Ok(receipt.value), receipt.work),
                    Err(failure) => (Err(failure.error), *failure.work),
                };
                *work = work.with_backend(spent, allocations.live_bytes())?;
                answer
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { original, .. } => crate::demand::abort_publication(publication, *original),
        }
    }

    fn prepare_page(&mut self, mut page: LazyDirectoryPage)
    -> Result<DirectoryProjectionPage, LazyWorkspaceError> {
        match self {
            Self::Portable { work, .. } => Ok(DirectoryProjectionPage::Portable(OperationReceipt { value: page, work: *work })),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { allocation, .. } => {
                let entries_bytes = retained_directory_page_bytes(
                    &page.entries,
                    page.entries.iter().map(|entry| entry.name.retained_bytes()),
                )?;
                if let Some(cursor) = &mut page.next {
                    cursor.attach_native_custody(allocation)?;
                }
                let owned = allocation.split_off(entries_bytes)?;
                Ok(DirectoryProjectionPage::Native(NativeLazyDirectoryPage {
                    page, allocation: owned,
                }))
            }
        }
    }

    fn finish(&mut self, result: Result<DirectoryProjectionPage, LazyWorkspaceError>)
    -> Result<DirectoryProjectionPage, LazyWorkspaceError> {
        match self {
            Self::Portable { work, .. } => result
                .map(|page| match page {
                    DirectoryProjectionPage::Portable(mut receipt) => {
                        receipt.work = *work;
                        DirectoryProjectionPage::Portable(receipt)
                    },
                    #[cfg(not(target_arch = "wasm32"))]
                    DirectoryProjectionPage::Native(_) => unreachable!("portable directory preserves its work mode"),
                })
                .map_err(|error| error.with_prior_work(*work)),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { .. } => result,
        }
    }
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

fn directory_vec_with_accounting<T>(
    count: usize,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(Vec<T>, crate::kernel::allocation::KernelOwnedAllocation), LazyWorkspaceError> {
    accounting.check().map_err(lazy_cursor_filesystem_error)?;
    cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
    let requested = crate::foundation::usize_to_u64(count.checked_mul(std::mem::size_of::<T>())
        .ok_or_else(counter_overflow)?);
    let mut allocation = accounting.claim_owned(requested, u64::from(requested != 0))
        .map_err(lazy_cursor_filesystem_error)?;
    let mut values = Vec::new();
    if let Err(error) = values.try_reserve_exact(count) {
        drop(values);
        accounting.release_owned(&mut allocation, requested).map_err(lazy_cursor_filesystem_error)?;
        return Err(lazy_reservation_error(error));
    }
    let actual = crate::foundation::usize_to_u64(values.capacity().checked_mul(std::mem::size_of::<T>())
        .ok_or_else(counter_overflow)?);
    if let Err(error) = accounting.reconcile_owned(&mut allocation, requested, actual) {
        drop(values);
        accounting.release_owned(&mut allocation, actual).map_err(lazy_cursor_filesystem_error)?;
        return Err(lazy_cursor_filesystem_error(error));
    }
    Ok((values, allocation))
}

fn directory_namespace_error(error: crate::kernel::NamespacePathError) -> LazyWorkspaceError {
    match error {
        crate::kernel::NamespacePathError::Cancelled => LazyWorkspaceError::Cancelled,
        crate::kernel::NamespacePathError::Work(error) => error.into(),
        error => LazyWorkspaceError::NamespacePath(error),
    }
}

fn directory_mask_contains(
    paths: &[String],
    child: &str,
    mut visit: impl FnMut() -> Result<(), LazyWorkspaceError>,
) -> Result<bool, LazyWorkspaceError> {
    let mut lower = 0;
    let mut upper = paths.len();
    while lower < upper {
        visit()?;
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

fn directory_child_path_with_accounting(
    parent: &str,
    name: &LogicalName,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(Option<String>, crate::kernel::allocation::KernelOwnedAllocation), LazyWorkspaceError> {
    accounting.check().map_err(lazy_cursor_filesystem_error)?;
    cancellation.check().map_err(|_| LazyWorkspaceError::Cancelled)?;
    accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })
        .map_err(lazy_cursor_filesystem_error)?;
    let Some(name_bytes) = name.unicode_utf8_bytes() else {
        return accounting.claim_owned(0, 0).map(|allocation| (None, allocation))
            .map_err(lazy_cursor_filesystem_error);
    };
    let parent_bytes = if parent == "/" { 0 } else { parent.len() };
    let length = parent_bytes.checked_add(1).and_then(|length| length.checked_add(name_bytes))
        .ok_or_else(counter_overflow)?;
    let delta = if name.encoding() == crate::kernel::NameEncoding::WindowsUtf16Le {
        WorkCounters { bytes_copied: (parent_bytes + 1) as u64, bytes_encoded: name_bytes as u64, ..WorkCounters::UNCHARGED }
    } else {
        WorkCounters { bytes_copied: length as u64, ..WorkCounters::UNCHARGED }
    };
    accounting.admit(delta).map_err(lazy_cursor_filesystem_error)?;
    let mut allocation = accounting.claim_owned(length as u64, u64::from(length != 0))
        .map_err(lazy_cursor_filesystem_error)?;
    let mut owned_bytes = length as u64;
    let result = (|| {
        let mut child = String::new();
        child.try_reserve_exact(length).map_err(lazy_reservation_error)?;
        owned_bytes = child.capacity() as u64;
        accounting.reconcile_owned(&mut allocation, length as u64, owned_bytes)
            .map_err(lazy_cursor_filesystem_error)?;
        accounting.admit(delta).map_err(lazy_cursor_filesystem_error)?;
        if parent_bytes != 0 { child.push_str(parent); }
        child.push('/');
        name.append_unicode(&mut child).map_err(|error|
            directory_namespace_error(crate::kernel::NamespacePathError::Name(error)))?;
        accounting.record_completed(delta).map_err(lazy_cursor_filesystem_error)?;
        Ok(child)
    })();
    match result {
        Ok(child) => Ok((Some(child), allocation)),
        Err(error) => {
            accounting.release_owned(&mut allocation, owned_bytes).map_err(lazy_cursor_filesystem_error)?;
            Err(error)
        }
    }
}

fn exactify_path_vector_bytes(paths: &Vec<String>) -> Result<u64, LazyWorkspaceError> {
    u64::try_from(paths.capacity())
        .ok()
        .and_then(|count| count.checked_mul(u64::try_from(std::mem::size_of::<String>()).ok()?))
        .ok_or_else(counter_overflow)
}

fn exactify_paths_retained_bytes(paths: &Vec<String>) -> Result<u64, LazyWorkspaceError> {
    paths.iter().try_fold(exactify_path_vector_bytes(paths)?, |bytes, path|
        bytes.checked_add(path.capacity() as u64).ok_or_else(counter_overflow))
}

fn remaining_work(
    work: WorkCounters, budget: WorkBudget, live_bytes: u64,
) -> Result<WorkBudget, LazyWorkspaceError> {
    let mut remaining = work.remaining(budget)?;
    remaining.peak_allocation_bytes = remaining.peak_allocation_bytes
        .checked_sub(live_bytes).ok_or_else(counter_overflow)?;
    Ok(remaining)
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

fn exactify_error(error: LazyWorkspaceError) -> LazyWorkspaceError {
    match error {
        LazyWorkspaceError::SpentWork(failure) => lazy_operation_failure(*failure, exactify_error),
        LazyWorkspaceError::Cancelled | LazyWorkspaceError::Demand(DemandError::Cancelled) => {
            LazyWorkspaceError::Cancelled
        }
        error => error,
    }
}

fn exactify_batch_paths(limits: crate::model::VolumeLimits) -> usize {
    let dependency_paths = usize::try_from(limits.maximum_checkout_dependencies)
        .unwrap_or(usize::MAX) / (usize::from(limits.maximum_path_depth) + 4);
    let mutation_paths = usize::try_from(limits.maximum_mutations_per_batch)
        .unwrap_or(usize::MAX) / 4;
    dependency_paths.min(mutation_paths).max(1)
}

fn shares_exactify_publication<A, O>(
    old: &crate::Generation<A, O>,
    new: &crate::Generation<A, O>,
) -> bool {
    match (&old.publication_closure, &new.publication_closure) {
        (Some(old), Some(new)) => Arc::ptr_eq(old, new),
        _ => false,
    }
}

fn replace_exactify_generation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    old: &mut crate::Generation<A, O>,
    new: crate::Generation<A, O>,
    retained_bytes: &mut u64,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<(), LazyWorkspaceError> {
    let shared = shares_exactify_publication(old, &new);
    let old_bytes = old.publication_retained_bytes();
    let new_bytes = new.publication_retained_bytes();
    if !shared {
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        allocations.claim_bytes(*retained_bytes, 0, work, budget).map_err(lazy_allocation_error)?;
        allocations.claim_bytes(new_bytes, 0, work, budget).map_err(lazy_allocation_error)?;
        *retained_bytes = allocations.live_bytes();
    }
    *old = new;
    if !shared {
        *retained_bytes = retained_bytes.checked_sub(old_bytes).ok_or_else(counter_overflow)?;
    }
    Ok(())
}

const SELECTED_RESTORE_BATCH_DOMAIN: &[u8] = b"acyclic-fs-selected-restore-batch-v1\0";

fn selected_restore_batch_key(key: IdempotencyKey, index: u64) -> IdempotencyKey {
    if index == 0 { return key; }
    let mut hasher = blake3::Hasher::new();
    hasher.update(SELECTED_RESTORE_BATCH_DOMAIN);
    hasher.update(&key.into_bytes());
    hasher.update(&index.to_le_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    IdempotencyKey::from_bytes(bytes)
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

    const NATIVE_PUBLICATION_MAX_BYTES: usize =
        NATIVE_PUBLICATION_SNAPSHOT_DOMAIN.len() + 2 + NATIVE_PUBLICATION_FIXED_BYTES + 88 + 73;

    // Test the real production C encoder with the existing canonical cursor;
    fn native_publication_decode_with_accounting(
        evidence: &[u8], expected_operation: crate::OperationId,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    ) -> Result<(NativeSnapshotContext, crate::PublicationPermit), crate::FsError> {
        let mut observe = |step| lazy_cursor_fixed_read_step(accounting, step);
        let mut decoder = crate::kernel::Decoder::new::<crate::FsError>(
            evidence, NATIVE_PUBLICATION_SNAPSHOT_DOMAIN, 1, NATIVE_PUBLICATION_MAX_BYTES as u64, &mut observe,
        )?;
        let operation = crate::OperationId::from_bytes(decoder.fixed::<16, crate::FsError>(&mut observe)?);
        let kind = decoder.u8(&mut observe)?;
        let workspace_id = WorkspaceId::from_bytes(decoder.fixed::<16, crate::FsError>(&mut observe)?);
        let config = crate::kernel::decode_config(&mut decoder, &mut observe)?;
        let context = match kind {
            0 => NativeSnapshotContext::Authored { workspace_id, config },
            1 => NativeSnapshotContext::Lazy(LazyPublicationCut {
                workspace_id, config,
                source: SourceReference {
                    identity: decoder.fixed::<16, crate::FsError>(&mut observe)?,
                    epoch: decoder.u64(&mut observe)?,
                },
                overlay: LazyOverlayId::from_bytes(decoder.fixed::<32, crate::FsError>(&mut observe)?),
                shadows: LazyShadowId(decoder.fixed::<32, crate::FsError>(&mut observe)?),
            }),
            tag => return Err(crate::kernel::CanonicalDecodeError::UnknownTag {
                field: "original native publication snapshot", tag,
            }.into()),
        };
        let permit = match decoder.u8(&mut observe)? {
            0 => crate::PublicationPermit::Unrestricted,
            1 => crate::PublicationPermit::Lease {
                authority_id: decoder.fixed::<16, crate::FsError>(&mut observe)?,
                workspace_id: decoder.fixed::<16, crate::FsError>(&mut observe)?,
                lease_id: decoder.fixed::<16, crate::FsError>(&mut observe)?,
                expires_at_millis: decoder.u64(&mut observe)?,
            },
            2 => crate::PublicationPermit::Reservation {
                operation_id: decoder.fixed::<16, crate::FsError>(&mut observe)?,
                gate_tail: decoder.u64(&mut observe)?,
                expected: crate::Head {
                    epoch: crate::Epoch::new(decoder.u64(&mut observe)?).map_err(|_| native_publication_invalid())?,
                    sequence: crate::Sequence::new(decoder.u64(&mut observe)?),
                    digest: crate::Digest::from_bytes(decoder.fixed::<32, crate::FsError>(&mut observe)?),
                },
            },
            tag => return Err(crate::kernel::CanonicalDecodeError::UnknownTag {
                field: "original native publication permit", tag,
            }.into()),
        };
        decoder.finish()?;
        if operation != expected_operation { return Err(native_publication_invalid()); }
        Ok((context, permit))
    }

    /// A deliberately modeled one-I/O source, not a native-provider receipt.
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
        fn logical_owned_bytes(&self) -> u64 { 0 }

        fn read_range(
            &self, offset: u64, length: u64, accounting: &mut crate::kernel::KernelAccounting<'_>,
        ) -> Result<crate::kernel::OwnedSourceChunk, DemandError> {
            #[cfg(not(target_arch = "wasm32"))]
            if let crate::kernel::KernelAccounting::NativeOriginal { cancellation, .. } = accounting {
                cancellation.check().map_err(|_| DemandError::Cancelled)?;
            }
            let budget = crate::demand::ordinary_source_budget(accounting)?;
            let answer = (|| {
                let (_, _, _, cancellation) = accounting.standalone_parts().ok_or_else(|| OperationFailure::before_work(DemandError::NativeWorkUnavailable))?;
                let mut meter = SourceFixtureMeter::begin(budget, cancellation)?;
                let value = meter.range(&self.0, offset, length)?;
                meter.check(cancellation)?;
                meter.charge(WorkCounters { output_bytes: value.len() as u64, ..WorkCounters::default() })?;
                meter.receipt(crate::kernel::OwnedSourceChunk::claimed(value.into(), 0))
            })();
            crate::demand::accept_source_result(answer, accounting)
        }
    }

    impl DemandSource for CountingSource {
        fn reference(&self) -> SourceReference {
            SourceReference {
                identity: self.identity,
                epoch: self.epoch.load(Ordering::Relaxed),
            }
        }

        async fn lookup<W: crate::facade::FsWork + Send>(
            &self,
            source: SourceReference,
            _path: &NamespacePath,
            work: W,
            cancellation: &CancellationToken,
        ) -> W::Output<Option<SourceNode>, DemandError> {
            let mut accounting = work.accounting(cancellation);
            let result = async {
                let budget = crate::demand::ordinary_source_budget(&accounting)?;
                let answer = async {
                    let mut meter = SourceFixtureMeter::begin(budget, cancellation)?;
                    self.counts.lookups.fetch_add(1, Ordering::Relaxed);
                    let (node, _) = self.resolve(source, None, &mut meter)?;
                    meter.charge(WorkCounters { items_returned: 1, ..WorkCounters::default() })?;
                    meter.check(cancellation)?;
                    meter.receipt(Some(node))
                }.await;
                crate::demand::accept_source_result(answer, &mut accounting)
            }.await;
            work.finish(result, &accounting)
        }

        async fn list_page<W: crate::facade::FsWork + Send>(
            &self,
            source: SourceReference,
            _directory: &NamespacePath,
            cursor: Option<SourceCursor>,
            maximum_entries: u32,
            work: W,
            cancellation: &CancellationToken,
        ) -> W::Output<SourceDirectoryPage, DemandError> {
            let mut accounting = work.accounting(cancellation);
            let result = async {
                let budget = crate::demand::ordinary_source_budget(&accounting)?;
                let answer = async {
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
                        entries.push(SourceDirectoryEntry::new(
                            LogicalName::new(NameEncoding::Utf8, name, 255).expect("name"), node,
                        ));
                    }
                    meter.receipt(SourceDirectoryPage::stateless(entries, None, SourceVersion([4; 32])))
                }.await;
                crate::demand::accept_source_result(answer, &mut accounting)
            }.await;
            work.finish(result, &accounting)
        }

        async fn read_range<W: crate::facade::FsWork + Send>(
            &self,
            source: SourceReference,
            _path: &NamespacePath,
            expected: SourceVersion,
            offset: u64,
            length: u64,
            work: W,
            cancellation: &CancellationToken,
        ) -> W::Output<crate::kernel::OwnedSourceChunk, DemandError> {
            let mut accounting = work.accounting(cancellation);
            let result = async {
                let budget = crate::demand::ordinary_source_budget(&accounting)?;
                let answer = async {
                    let mut meter = SourceFixtureMeter::begin(budget, cancellation)?;
                    self.counts.ranges.fetch_add(1, Ordering::Relaxed);
                    let (_, bytes) = self.resolve(source, Some(expected), &mut meter)?;
                    let value = meter.range(&bytes, offset, length)?;
                    if self.cancel_on_range.swap(false, Ordering::Relaxed) {
                        cancellation.cancel();
                    }
                    meter.check(cancellation)?;
                    meter.charge(WorkCounters { output_bytes: value.len() as u64, ..WorkCounters::default() })?;
                    meter.receipt(crate::kernel::OwnedSourceChunk::claimed(value.into(), 0))
                }.await;
                crate::demand::accept_source_result(answer, &mut accounting)
            }.await;
            work.finish(result, &accounting)
        }

        async fn open_file<W: crate::facade::FsWork + Send>(
            &self,
            source: SourceReference,
            _path: &NamespacePath,
            expected: SourceVersion,
            work: W,
            cancellation: &CancellationToken,
        ) -> W::Output<Box<dyn DemandFile>, DemandError> {
            let mut accounting = work.accounting(cancellation);
            let result = async {
                let budget = crate::demand::ordinary_source_budget(&accounting)?;
                let answer = async {
                    let mut meter = SourceFixtureMeter::begin(budget, cancellation)?;
                    let (_, bytes) = self.resolve(source, Some(expected), &mut meter)?;
                    meter.allocate::<PinnedFile>(1)?;
                    meter.check(cancellation)?;
                    meter.receipt(Box::new(PinnedFile(bytes)) as Box<dyn DemandFile>)
                }.await;
                crate::demand::accept_source_result(answer, &mut accounting)
            }.await;
            work.finish(result, &accounting)
        }

        async fn read_link<W: crate::facade::FsWork + Send>(
            &self,
            _source: SourceReference,
            _path: &NamespacePath,
            _expected: SourceVersion, _maximum_bytes: u64,
            work: W,
            cancellation: &CancellationToken,
        ) -> W::Output<crate::kernel::OwnedSourceChunk, DemandError> {
            let mut accounting = work.accounting(cancellation);
            let result = async {
                let budget = crate::demand::ordinary_source_budget(&accounting)?;
                let answer = async {
                    let meter = SourceFixtureMeter::begin(budget, cancellation)?;
                    Err(meter.failure(DemandError::InvalidRequest))
                }.await;
                crate::demand::accept_source_result(answer, &mut accounting)
            }.await;
            work.finish(result, &accounting)
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

        async fn lookup<W: crate::facade::FsWork + Send>(&self, source: SourceReference, path: &NamespacePath,
            work: W, token: &CancellationToken) -> W::Output<Option<SourceNode>, DemandError> {
            let accounting = work.accounting(token);
            if let Err(error) = crate::demand::ordinary_source_budget(&accounting) { return work.finish(Err(error), &accounting); }
            drop(accounting);
            self.inner.lookup(source, path, work, token).await
        }

        async fn list_page<W: crate::facade::FsWork + Send>(&self, source: SourceReference, path: &NamespacePath,
            cursor: Option<SourceCursor>, maximum: u32, work: W,
            token: &CancellationToken) -> W::Output<SourceDirectoryPage, DemandError> {
            let mut accounting = work.accounting(token);
            let result = async {
                let budget = crate::demand::ordinary_source_budget(&accounting)?;
                let answer = async {
                    let receipt = self.inner.list_page(source, path, cursor, maximum, budget, token).await?;
                    if self.cancel.swap(false, Ordering::Relaxed) { token.cancel(); }
                    Ok(receipt)
                }.await;
                crate::demand::accept_source_result(answer, &mut accounting)
            }.await;
            work.finish(result, &accounting)
        }

        async fn read_range<W: crate::facade::FsWork + Send>(&self, source: SourceReference, path: &NamespacePath,
            version: SourceVersion, offset: u64, length: u64, work: W,
            token: &CancellationToken) -> W::Output<crate::kernel::OwnedSourceChunk, DemandError> {
            let accounting = work.accounting(token);
            if let Err(error) = crate::demand::ordinary_source_budget(&accounting) { return work.finish(Err(error), &accounting); }
            drop(accounting);
            self.inner.read_range(source, path, version, offset, length, work, token).await
        }

        async fn open_file<W: crate::facade::FsWork + Send>(&self, source: SourceReference, path: &NamespacePath,
            version: SourceVersion, work: W, token: &CancellationToken)
            -> W::Output<Box<dyn DemandFile>, DemandError> {
            let accounting = work.accounting(token);
            if let Err(error) = crate::demand::ordinary_source_budget(&accounting) { return work.finish(Err(error), &accounting); }
            drop(accounting);
            self.inner.open_file(source, path, version, work, token).await
        }

        async fn read_link<W: crate::facade::FsWork + Send>(&self, source: SourceReference, path: &NamespacePath,
            version: SourceVersion, maximum_bytes: u64, work: W, token: &CancellationToken) -> W::Output<crate::kernel::OwnedSourceChunk, DemandError> {
            let accounting = work.accounting(token);
            if let Err(error) = crate::demand::ordinary_source_budget(&accounting) { return work.finish(Err(error), &accounting); }
            drop(accounting);
            self.inner.read_link(source, path, version, maximum_bytes, work, token).await
        }
    }

    fn finite_fixture_budget() -> WorkBudget {
        let mut value = serde_json::to_value(WorkBudget::UNBOUNDED).expect("budget fields");
        for allowance in value.as_object_mut().expect("budget object").values_mut() {
            *allowance = serde_json::json!(100_000_000_u64);
        }
        serde_json::from_value(value).expect("finite budget")
    }

    #[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn original_selected_import_denial_lost_ack_and_retry_keep_exact_companion() {
        acyclic_native_runtime::initialize_native_io_services().expect("native services");
        let directory = tempfile::tempdir().expect("actual native source");
        std::fs::write(directory.path().join("outside-native"), b"untouched").expect("source input");
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(finite_fixture_budget(), &cancellation)
            .expect("original root");
        let source = Arc::new(crate::demand::native::NativeDemandSource::open_with_reference_and_native_work(
            directory.path(), crate::model::FilesystemProfile::Portable,
            crate::model::VolumeLimits::default(), SourceReference { identity: [0xa7; 16], epoch: 0 },
            None, &original,
        ).await.expect("stock original-native source"));
        let root = LazyWorkspace::attach(
            &Fs::memory(), "selected-import-original", source, MemoryLazyWorkspaceStore::default(),
        ).await.expect("real SDK root");
        root.write("/selected", Bytes::from_static(b"baseline")).await.expect("selected preimage");
        root.write("/outside-authored", Bytes::from_static(b"outside")).await.expect("outside preimage");
        let destination = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("authentic destination");
        let child = root.fork("selected-import-child", IdempotencyKey::from_bytes([0xa8; 16]))
            .await.expect("actual fork");
        child.write("/selected", Bytes::from_static(b"candidate")).await.expect("real candidate");
        let frozen = child.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("actual candidate snapshot");
        let selected = child.namespace_path("/selected").expect("selected grammar");
        let operation = crate::OperationId::from_bytes([0xa9; 16]);
        let (plan, allocation) = async { let accounting = LazyTreapWork::native_with_work(&original, (&original).cancellation())?; child.prepare_selected_import(frozen, std::slice::from_ref(&selected), &accounting).await }.await.map(|phase| match phase { LazyReadonlyNode::Native(owned) => owned.into_parts(), LazyReadonlyNode::Portable(_) => unreachable!("native plan retains native funding") }).map_err(LazyWorkspaceError::into_native_failure).expect("original-owned real plan");
        let before = original.receipt();
        let guard_original = original.clone();
        let denied = root.import_selected_with_native_work_guarded(
            destination, plan, allocation, operation, crate::PublicationPermit::Unrestricted,
            &original, &cancellation, move |work| {
                assert!(work.shares_ledger(&guard_original));
                async { Err::<(), LazyWorkspaceError>(crate::FsError::MutationNotAllowed.into()) }
            },
        ).await.expect_err("actual before-CAS guard denies");
        assert!(matches!(denied, LazyWorkspaceError::Filesystem(crate::FsError::MutationNotAllowed)));
        assert!(original.receipt().backend_write_operations > before.backend_write_operations);
        assert_eq!(root.workspace().head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("unchanged head").id(), destination.authored_generation);
        assert_eq!(async { let accounting = LazyTreapWork::native_with_work(&original, &cancellation)?; root.selected_import_snapshot(operation, &accounting).await }
            .await.expect("prepublication original query"), None);
        let (wrong_plan, wrong_allocation) = async { let accounting = LazyTreapWork::native_with_work(&original, (&original).cancellation())?; child.prepare_selected_import(frozen, std::slice::from_ref(&selected), &accounting).await }.await.map(|phase| match phase { LazyReadonlyNode::Native(owned) => owned.into_parts(), LazyReadonlyNode::Portable(_) => unreachable!("native plan retains native funding") }).map_err(LazyWorkspaceError::into_native_failure).expect("actual paid retry input");
        let other_resource = original.native_volume_work(finite_fixture_budget())
            .expect("same root but distinct resource allowance");
        let prefix = original.receipt();
        let wrong_guard = Arc::new(AtomicUsize::new(0));
        let wrong_guard_observer = Arc::clone(&wrong_guard);
        let mismatch = root.import_selected_with_native_work_guarded(
            destination, wrong_plan, wrong_allocation, operation, crate::PublicationPermit::Unrestricted,
            &other_resource, &cancellation, move |_| {
                wrong_guard_observer.fetch_add(1, Ordering::Relaxed);
                async { Ok::<(), LazyWorkspaceError>(()) }
            },
        ).await.expect_err("root identity alone does not transfer resource custody");
        assert!(matches!(mismatch, LazyWorkspaceError::NativeLedgerMismatch));
        assert_eq!(wrong_guard.load(Ordering::Relaxed), 0);
        assert_eq!(original.receipt(), prefix);
        let prepared = root.store.load_lazy_workspace(root.workspace().id()).await
            .expect("actual companion").expect("companion exists");
        assert!(prepared.pending_remove.is_some());
        assert!(prepared.completed_selected_import.is_none());

        // Exercise the real DATA/companion gap: the retained SDK checkout
        let mut retained = root.selected_publication.retained.lock()
            .expect("candidate custody").take().expect("retained actual checkout");
        let guard_original = original.clone();
        let outcome = retained.checkout.commit_even_if_clean_with_native_work(operation, crate::PublicationPermit::Unrestricted, &original, &cancellation, &mut move |work: &crate::NativeMaterializationWork| {
                assert!(work.shares_ledger(&guard_original));
                async { Ok::<(), crate::FsError>(()) }
            }, None, None, None, None).await.expect("actual original DATA CAS");
        let generation = match outcome {
            crate::CheckoutCommitOutcome::Committed { generation_id, .. } => generation_id,
            other => panic!("unexpected actual publication: {other:?}"),
        };
        assert!(retained.published.is_none());
        *root.selected_publication.retained.lock().expect("candidate custody") = Some(retained);
        let snapshot = async { let accounting = LazyTreapWork::native_with_work(&original, &cancellation)?; root.selected_import_snapshot(operation, &accounting).await }
            .await.expect("recover lost ACK from authentic original operation").expect("original snapshot");
        assert_eq!(snapshot.authored_generation, generation);
        let completed = root.store.load_lazy_workspace(root.workspace().id()).await
            .expect("final actual companion").expect("companion exists");
        assert!(completed.pending_remove.is_none());
        assert_eq!(completed.completed_selected_import.expect("durable original witness").snapshot, snapshot);
        assert_eq!(snapshot.overlay, completed.overlay);
        assert_eq!(snapshot.shadows, completed.shadows);
        assert_eq!(root.read("/selected", 64).await.expect("candidate body").as_ref(), b"candidate");
        assert_eq!(root.read("/outside-authored", 64).await.expect("outside body").as_ref(), b"outside");
        assert_eq!(std::fs::read(directory.path().join("outside-native")).expect("source remains"), b"untouched");

        root.write("/outside-authored", Bytes::from_static(b"later")).await.expect("later distinct head");
        assert_ne!(root.workspace().head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("later head").id(), generation);
        let (plan, allocation) = async { let accounting = LazyTreapWork::native_with_work(&original, (&original).cancellation())?; child.prepare_selected_import(frozen, std::slice::from_ref(&selected), &accounting).await }.await.map(|phase| match phase { LazyReadonlyNode::Native(owned) => owned.into_parts(), LazyReadonlyNode::Portable(_) => unreachable!("native plan retains native funding") }).map_err(LazyWorkspaceError::into_native_failure).expect("same original paid retry plan");
        let guards = Arc::new(AtomicUsize::new(0));
        let guard_observer = Arc::clone(&guards);
        let retry = root.import_selected_with_native_work_guarded(
            destination, plan, allocation, operation, crate::PublicationPermit::Unrestricted,
            &original, &cancellation, move |_| {
                guard_observer.fetch_add(1, Ordering::Relaxed);
                async { Ok::<(), LazyWorkspaceError>(()) }
            },
        ).await.expect("exact original operation retry");
        assert_eq!(retry, snapshot);
        assert_eq!(guards.load(Ordering::Relaxed), 0);
        assert_eq!(async { let accounting = LazyTreapWork::native_with_work(&original, &cancellation)?; root.selected_import_snapshot(operation, &accounting).await }
            .await.expect("durable original query after unrelated head"), Some(snapshot));
        original.receipt().verify(finite_fixture_budget()).expect("all completed original work retained");

        // Missing durable original evidence must never be reconstructed from
        let state = root.store.load_lazy_workspace(root.workspace().id()).await
            .expect("state").expect("state exists");
        assert!(root.store.compare_and_swap_lazy_workspace(
            root.workspace().id(), state.revision, LazyWorkspaceState {
                revision: state.revision + 1, completed_selected_import: None, ..state
            },
        ).await.expect("remove original witness control"));
        let unavailable = async { let accounting = LazyTreapWork::native_with_work(&original, &cancellation)?; root.selected_import_snapshot(operation, &accounting).await }
            .await.expect_err("no head adoption without original witness");
        assert!(matches!(unavailable, LazyWorkspaceError::SelectedImportIndeterminate { operation: saved }
            if saved == operation));
        assert_eq!(root.read("/outside-authored", 64).await.expect("later head unchanged").as_ref(), b"later");
    }

    #[tokio::test]
    async fn selected_immutable_authored_seed_survives_unrelated_native_epoch() {
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(&Fs::memory(), "selected-immutable-seed",
            Arc::clone(&source), MemoryLazyWorkspaceStore::default()).await.expect("attach");
        root.write("/authored", Bytes::from_static(b"frozen")).await.expect("original authored file");
        let frozen = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("freeze original");
        root.write("/authored", Bytes::from_static(b"imported")).await.expect("imported file");
        let imported = root.workspace().head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("imported generation");
        let before = root.store.load_lazy_workspace(root.workspace().id()).await.expect("companion");
        let lookups = source.counts.lookups.load(Ordering::Relaxed);
        let ranges = source.counts.ranges.load(Ordering::Relaxed);
        source.invalidate();
        let budget = finite_fixture_budget();
        let receipt = root.exactify_snapshot_selected_at_generation_measured(
            frozen, &imported, &["/authored".to_owned()], "selected-immutable-result",
            IdempotencyKey::from_bytes([0x83; 16]), IdempotencyKey::from_bytes([0x84; 16]),
            crate::PublicationPermit::Unrestricted, budget, &CancellationToken::new(),
            |_| async { Ok(OperationReceipt { value: (), work: WorkCounters::default() }) },
        ).await.expect("immutable source-independent capture");
        receipt.work.verify(budget).expect("actual bounded receipt");
        let (baseline, candidate) = receipt.value;
        assert_eq!(baseline.read("/authored", 64, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("frozen body").as_ref(), b"frozen");
        assert_eq!(candidate.read("/authored", 64, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("imported body").as_ref(), b"imported");
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), lookups);
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), ranges);
        assert_eq!(root.store.load_lazy_workspace(root.workspace().id()).await.expect("companion unchanged"), before);
        assert_eq!(root.workspace().head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("original head unchanged").id(), imported.id());
    }

    #[cfg(all(feature = "native-watch", feature = "native-mount", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn selected_native_subtree_captures_nonempty_baseline_imported_records_and_removals() {
        let directory = tempfile::tempdir().expect("native source");
        std::fs::create_dir_all(directory.path().join("hot")).expect("selected directory");
        std::fs::create_dir_all(directory.path().join("cold")).expect("outside directory");
        std::fs::write(directory.path().join("hot/a"), b"source-a").expect("original source data");
        std::fs::write(directory.path().join("hot/gone"), b"source-gone").expect("removed source data");
        std::fs::write(directory.path().join("cold/untouched"), b"outside-native").expect("outside source data");
        let source = Arc::new(crate::demand::native::NativeDemandSource::open(
            directory.path(), crate::model::FilesystemProfile::Portable, crate::model::VolumeLimits::default(), None, None,
        ).await.expect("actual native provider"));
        let root = LazyWorkspace::attach(&Fs::memory(), "selected-native-original",
            source, MemoryLazyWorkspaceStore::default()).await.expect("attach");
        root.write("/outside-authored", Bytes::from_static(b"frozen-outside")).await.expect("outside authored seed");
        let frozen = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("freeze actual original source");
        root.write("/hot/a", Bytes::from_static(b"imported-a")).await.expect("selected imported data");
        root.write("/hot/new", Bytes::from_static(b"imported-new")).await.expect("selected imported addition");
        root.remove("/hot/gone").await.expect("selected imported removal");
        root.write("/outside-authored", Bytes::from_static(b"outside-imported")).await.expect("unselected imported change");
        let imported = root.workspace().head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("imported head");
        let before = root.store.load_lazy_workspace(root.workspace().id()).await.expect("current companion");
        let budget = finite_fixture_budget();
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(budget, &cancellation)
            .expect("genuine original root");
        original.charge(WorkCounters { items_examined: 3, ..WorkCounters::UNCHARGED })
            .expect("retained caller prefix");
        let (baseline, candidate) = root.exactify_snapshot_selected_at_generation_with_native_work_guarded(
            frozen, &imported, &["/hot".to_owned()], "selected-native-result",
            IdempotencyKey::from_bytes([0x87; 16]), IdempotencyKey::from_bytes([0x88; 16]),
            crate::PublicationPermit::Unrestricted, &original, &cancellation,
            |work| {
                assert!(work.receipt().items_examined >= 3);
                async { Ok::<(), LazyWorkspaceError>(()) }
            },
        ).await.expect("actual selected baseline and candidate");
        original.receipt().verify(budget).expect("bounded original source and SDK work");
        assert!(original.receipt().source_bytes_read > 0);
        assert!(original.receipt().source_entries_visited > 0);
        assert_ne!(baseline.workspace_id(), root.workspace().id());
        assert_eq!(baseline.workspace_id(), candidate.workspace_id());
        assert_eq!(baseline.read("/hot/a", 64, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("nonempty baseline").as_ref(), b"source-a");
        assert_eq!(baseline.read("/hot/gone", 64, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("removed baseline").as_ref(), b"source-gone");
        assert_eq!(candidate.read("/hot/a", 64, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("candidate changed").as_ref(), b"imported-a");
        assert_eq!(candidate.read("/hot/new", 64, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("candidate added").as_ref(), b"imported-new");
        assert!(matches!(candidate.stat("/hot/gone", WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map_err(|failure| failure.error), Err(WorkspaceError::NotFound)));
        for generation in [&baseline, &candidate] {
            assert_eq!(generation.read("/outside-authored", 64, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("frozen outside authored").as_ref(), b"frozen-outside");
            assert!(matches!(generation.stat("/cold", WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map_err(|failure| failure.error), Err(WorkspaceError::NotFound)));
        }
        assert_eq!(root.workspace().head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("original head").id(), imported.id());
        assert_eq!(root.store.load_lazy_workspace(root.workspace().id()).await.expect("original companion"), before);
        assert_eq!(std::fs::read(directory.path().join("hot/a")).expect("original native source remains unchanged"), b"source-a");
        assert_eq!(std::fs::read(directory.path().join("cold/untouched")).expect("outside native remains unchanged"), b"outside-native");
    }

    #[cfg(all(feature = "native-watch", feature = "native-mount", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn original_native_selected_seal_and_budget_denials_preserve_the_frozen_root() {
        for mode in 0..3 {
            let directory = tempfile::tempdir().expect("actual native source");
            std::fs::write(directory.path().join("selected"), b"original-body").expect("source body");
            let source = Arc::new(crate::demand::native::NativeDemandSource::open(
                directory.path(), crate::model::FilesystemProfile::Portable,
                crate::model::VolumeLimits::default(), None, None,
            ).await.expect("native source"));
            let root = LazyWorkspace::attach(&Fs::memory(), "selected-original-denial",
                Arc::clone(&source), MemoryLazyWorkspaceStore::default()).await.expect("attach");
            root.write("/outside", Bytes::from_static(b"outside")).await.expect("outside seed");
            let frozen = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("immutable cut");
            let imported = root.workspace().head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("imported generation");
            let before = root.store.load_lazy_workspace(root.workspace().id()).await.expect("companion");
            let cancellation = CancellationToken::new();
            let mut budget = finite_fixture_budget();
            if mode == 2 { budget.bytes_hashed = 0; }
            let original = crate::NativeMaterializationWork::new(budget, &cancellation)
                .expect("original bounded authority");
            let guard_source = Arc::clone(&source);
            let guard_cancellation = cancellation.clone();
            let result = root.exactify_snapshot_selected_at_generation_with_native_work_guarded(
                frozen, &imported, &["/selected".to_owned()], "selected-original-denied",
                IdempotencyKey::from_bytes([0x91; 16]), IdempotencyKey::from_bytes([0x92; 16]),
                crate::PublicationPermit::Unrestricted, &original, &cancellation,
                move |work| {
                    if work.receipt().source_bytes_read != 0 {
                        if mode == 0 {
                            guard_source.invalidate(&mut crate::demand::NativeSourceWork::from_native_materialization_work(work)).expect("actual source epoch change");
                        } else {
                            guard_cancellation.cancel();
                        }
                    }
                    async { Ok::<(), LazyWorkspaceError>(()) }
                },
            ).await;
            assert!(result.is_err(), "original admission or seal must deny");
            let spent = original.receipt();
            spent.verify(budget).expect("real admitted prefix");
            if mode != 2 {
                assert!(spent.source_bytes_read > 0, "denial follows actual source work");
                assert!(spent.backend_write_operations > 0, "prepared SDK writes remain accounted");
            } else {
                assert_eq!(spent.source_bytes_read, 0);
            }
            assert_eq!(root.workspace().head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("unchanged original head").id(), imported.id());
            assert_eq!(root.store.load_lazy_workspace(root.workspace().id()).await.expect("unchanged companion"), before);
            assert_eq!(std::fs::read(directory.path().join("selected")).expect("source unchanged"), b"original-body");
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn original_selected_owner_guard_keeps_its_actual_typed_quota_cause() {
        #[derive(Debug)]
        enum OwnerCause {
            Filesystem(crate::FsError),
            Workspace(WorkspaceError),
            Lazy(LazyWorkspaceError),
            Quota(crate::NativeTreeMaterializationError),
        }
        impl From<crate::FsError> for OwnerCause {
            fn from(error: crate::FsError) -> Self { Self::Filesystem(error) }
        }
        impl From<WorkspaceError> for OwnerCause {
            fn from(error: WorkspaceError) -> Self { Self::Workspace(error) }
        }
        impl From<LazyWorkspaceError> for OwnerCause {
            fn from(error: LazyWorkspaceError) -> Self { Self::Lazy(error) }
        }

        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"unread")));
        let root = LazyWorkspace::attach(
            &fs, "selected-owner-cause", source, MemoryLazyWorkspaceStore::default(),
        ).await.expect("actual original workspace");
        root.write("/selected", Bytes::from_static(b"authored")).await.expect("authored seed");
        let frozen = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("actual frozen cut");
        let imported = root.workspace().head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("immutable imported generation");
        let cancellation = CancellationToken::new();
        let budget = finite_fixture_budget();
        let original = crate::NativeMaterializationWork::new(budget, &cancellation)
            .expect("actual bounded original work");
        original.charge(WorkCounters { items_examined: 3, ..WorkCounters::UNCHARGED })
            .expect("real caller prefix");
        let denied_hash = budget.bytes_hashed.checked_add(1).expect("finite fixture bound");
        let result = root.exactify_snapshot_selected_at_generation_with_native_work_guarded(
            frozen, &imported, &["/selected".to_owned()], "selected-owner-denied",
            IdempotencyKey::from_bytes([0xad; 16]), IdempotencyKey::from_bytes([0xae; 16]),
            crate::PublicationPermit::Unrestricted, &original, &cancellation,
            move |work| std::future::ready(work.admit(WorkCounters {
                bytes_hashed: denied_hash, ..WorkCounters::UNCHARGED
            }).map_err(OwnerCause::Quota)),
        ).await;
        match result {
            Err(OwnerCause::Quota(cause)) => { drop(cause); }
            Err(OwnerCause::Filesystem(cause)) => panic!("owner cause flattened into filesystem: {cause}"),
            Err(OwnerCause::Workspace(cause)) => panic!("owner cause flattened into workspace: {cause}"),
            Err(OwnerCause::Lazy(cause)) => panic!("owner cause flattened into lazy engine: {cause}"),
            Ok(_) => panic!("original owner admission must deny"),
        }
        assert!(original.receipt().items_examined >= 3);
        original.receipt().verify(budget).expect("exact admitted original prefix");
        assert_eq!(root.workspace().head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("unchanged original head").id(), imported.id());

        let selected = root.namespace_path("/selected").expect("selected original grammar");
        let (plan, allocation) = async { let accounting = LazyTreapWork::native_with_work(&original, (&original).cancellation())?; root.prepare_selected_import(frozen, std::slice::from_ref(&selected), &accounting).await }.await.map(|phase| match phase { LazyReadonlyNode::Native(owned) => owned.into_parts(), LazyReadonlyNode::Portable(_) => unreachable!("native plan retains native funding") }).map_err(LazyWorkspaceError::into_native_failure).expect("actual original funded plan");
        let prefix = original.receipt();
        let imported_result = root.import_selected_with_native_work_guarded(
            frozen, plan, allocation, crate::OperationId::from_bytes([0xaf; 16]),
            crate::PublicationPermit::Unrestricted, &original, &cancellation,
            move |work| std::future::ready(work.admit(WorkCounters {
                bytes_hashed: denied_hash, ..WorkCounters::UNCHARGED
            }).map_err(OwnerCause::Quota)),
        ).await;
        match imported_result {
            Err(OwnerCause::Quota(cause)) => { drop(cause); }
            Err(OwnerCause::Filesystem(cause)) => panic!("import owner cause flattened into filesystem: {cause}"),
            Err(OwnerCause::Workspace(cause)) => panic!("import owner cause flattened into workspace: {cause}"),
            Err(OwnerCause::Lazy(cause)) => panic!("import owner cause flattened into lazy engine: {cause}"),
            Ok(_) => panic!("original import owner admission must deny"),
        }
        assert!(original.receipt().items_examined >= prefix.items_examined);
        original.receipt().verify(budget).expect("whole original import prefix");
        assert_eq!(root.workspace().head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("unchanged import head").id(), imported.id());
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
        let before = root.workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("initial head").id();
        source.replace(Bytes::from_static(b"replacement"));
        assert_ne!(source.reference(), pinned_source);
        let failure = root.promote_resolved_with_permit_measured(
            "/file.txt", lookup.value, 64, IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted, prior, budget, &token,
        ).await.err().expect("resolved epoch must not be silently refreshed");
        assert!(matches!(failure.semantic_error(), LazyWorkspaceError::StaleSource));
        assert_eq!(failure.spent_work(), prior);
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
        assert_eq!(root.workspace.head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("unpublished head").id(), before);
    }

    /// A view bound before its shared source advanced (a refresh of the physical root) follows the new epoch on its next access instead of failing: every fork of a root reads through one source.
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
            root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("snapshot").source,
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

        let failure = root.exactify("/", WorkBudget::UNBOUNDED, &cancellation, crate::PublicationPermit::Unrestricted).await.err().expect("source cancellation");
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
            .workspace().head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
            .expect("head before exactify")
            .id();
        let mut budget = WorkBudget::UNBOUNDED;
        budget.authority_records_appended = 0;

        assert!(matches!(
            root.exactify("/", budget, &CancellationToken::new(), crate::PublicationPermit::Unrestricted).await,
            Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::Workspace(_))
        ));
        assert_eq!(
            root.workspace()
                .head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
                .expect("head after rejection")
                .id(),
            before
        );
        assert!(matches!(
            root.workspace().stat("/file.txt", WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error),
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
        let snapshot = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("snapshot");

        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let failure = root.exactify_snapshot(
            snapshot, "cancelled-destination", IdempotencyKey::from_bytes([0x91; 16]),
            WorkBudget::UNBOUNDED, &cancelled,
        ).await.err().expect("pre-cancelled destination");
        assert!(matches!(failure.semantic_error(), LazyWorkspaceError::Cancelled));
        assert_eq!(failure.spent_work(), WorkCounters::default());
        assert!(fs.open_workspace("cancelled-destination", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());

        let failure = root.exactify_snapshot(
            snapshot, "budget-destination", IdempotencyKey::from_bytes([0x92; 16]),
            WorkBudget::default(), &CancellationToken::new(),
        ).await.err().expect("destination budget exhaustion");
        assert!(matches!(failure.semantic_error(), LazyWorkspaceError::Work(_)));
        failure.spent_work().verify(WorkBudget::default()).expect("admitted destination prefix");
        assert!(fs.open_workspace("budget-destination", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
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
                crate::model::VolumeLimits::default(), None, None,
            ).await.expect("native provider"),
            cancel: AtomicBool::new(true),
        });
        let root = LazyWorkspace::attach(
            &Fs::memory(), "cancelled-page-projection", source, MemoryLazyWorkspaceStore::default(),
        ).await.expect("attach");
        let budget = finite_fixture_budget();
        let token = CancellationToken::new();
        let failure = root.list_directory("/", None, 1, budget, &token)
            .await.err().expect("cancel after real raw page");
        assert!(matches!(failure.semantic_error(), LazyWorkspaceError::Cancelled));
        assert_eq!(failure.spent_work().source_entries_visited, 1);
        assert!(failure.spent_work().backend_read_operations > 0);
        failure.spent_work().verify(budget).expect("started native page prefix");

        let token = CancellationToken::new();
        let mut cursor = None;
        let mut names = Vec::new();
        loop {
            let receipt = root.list_directory("/", cursor, 1, budget, &token)
                .await.expect("retry held reader");
            receipt.work.verify(budget).expect("bounded projected page");
            names.extend(receipt.value.entries.into_iter().map(|entry| entry.name.as_bytes().to_vec()));
            cursor = receipt.value.next;
            if cursor.is_none() { break; }
        }
        names.sort();
        assert_eq!(names, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[tokio::test]
    async fn authentic_prepared_fork_budget_retry_keeps_first_freeze_and_child_snapshot() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"original")));
        let store = MemoryLazyWorkspaceStore::default();
        let root = LazyWorkspace::attach(&fs, "authentic-parent", Arc::clone(&source), store.clone())
            .await.expect("attach");
        root.workspace().write_text("/authored", "original").await.expect("original head");
        let budget = finite_fixture_budget();
        let token = CancellationToken::new();
        let key = IdempotencyKey::from_bytes([0xd1; 16]);
        let frozen = root.snapshot(&LazyTreapWork::new(budget, &token)).await.expect("freeze");
        let prepared = root.prepare_fork_snapshot("authentic-child", frozen, key.operation_id(), &LazyTreapWork::new(budget, &token)).await.expect("durable first freeze");
        assert_eq!(prepared, frozen);
        assert!(fs.open_workspace("authentic-child", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
        let mut denied = budget;
        denied.backend_read_operations = 0;
        let failure = root.fork_with_snapshot_measured("authentic-child", key, denied, &token)
            .await.err().expect("budget failure after durable preparation");
        assert!(matches!(failure.error.semantic_error(), LazyWorkspaceError::Work(_)));
        assert_ne!(*failure.work, WorkCounters::UNCHARGED);
        failure.work.verify(denied).expect("admitted failure prefix");

        root.remove("/file.txt").await.expect("advance mutable parent overlay");
        root.workspace().write_text("/authored", "advanced").await.expect("advance parent head");
        source.replace(Bytes::from_static(b"advanced"));
        let advanced = root.snapshot(&LazyTreapWork::new(budget, &token)).await.expect("advanced freeze");
        assert_ne!(advanced, frozen);
        let receipt = root.fork_with_snapshot_measured("authentic-child", key, budget, &token)
            .await.expect("retry original preparation");
        receipt.work.verify(budget).expect("finite authentic fork");
        let (child, original_child) = receipt.value;
        assert_eq!(original_child.source, frozen.source);
        assert_eq!(original_child.overlay, frozen.overlay);
        assert_eq!(original_child.shadows, frozen.shadows);
        assert_eq!(child.workspace().read("/authored", child.workspace().limits().maximum_read_bytes, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("original authored view").as_ref(), b"original");
        assert!(matches!(child.lookup("/file.txt").await.expect("original source view"), LazyLookup::Source(_)));
        assert_eq!((child.read_range_measured("/file.txt", 0, 8, budget, &token).await.expect("frozen source bytes").value).as_ref(), b"original");

        child.remove("/file.txt").await.expect("advance committed child overlay");
        child.workspace().write_text("/authored", "child advanced").await.expect("advance child head");
        let current_child = child.snapshot(&LazyTreapWork::new(budget, &token)).await.expect("advanced child freeze");
        assert_ne!(current_child, original_child);
        let repeated = root.fork_with_snapshot_measured("authentic-child", key, budget, &token)
            .await.expect("retry committed fork");
        assert_eq!(repeated.value.1, original_child);
        assert_eq!(repeated.value.0.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("current child remains current"), current_child);
        assert_eq!(source.counts.pages.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn authentic_prepare_before_creation_binds_same_project_and_rejects_wrong_identity() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"unhydrated")));
        let store = MemoryLazyWorkspaceStore::default();
        let root = LazyWorkspace::attach(&fs, "bind-parent", Arc::clone(&source), store.clone())
            .await.expect("attach");
        let budget = finite_fixture_budget();
        let token = CancellationToken::new();
        let key = IdempotencyKey::from_bytes([0xd2; 16]);
        let frozen = root.snapshot(&LazyTreapWork::new(budget, &token)).await.expect("snapshot");
        root.prepare_fork_snapshot("bind-child", frozen, key.operation_id(), &LazyTreapWork::new(budget, &token))
            .await.expect("prepare before actual creation");
        assert!(fs.open_workspace("bind-child", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
        let config = root.workspace().volume.config();
        let created = fs.create_workspace_with_operation(
            "bind-child", config, key.operation_id(), budget, &token,
        ).await.expect("actual project creation").value;
        let child = fs.open_workspace("bind-child", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("same created project");
        let denied = root.open_related(child.clone()).await.err().expect("Prepared is not ready");
        assert!(matches!(denied.semantic_error(), LazyWorkspaceError::Concurrent));
        let wrong_operation = root.bind_prepared_fork(frozen, child.clone(), created.id(), IdempotencyKey::from_bytes([0xd3; 16]).operation_id(), &LazyTreapWork::new(budget, &token)).await.err().expect("wrong original operation");
        assert!(matches!(wrong_operation.semantic_error(), LazyWorkspaceError::Concurrent));
        let mut wrong_config = config;
        wrong_config.lifecycle = crate::model::Lifecycle::Durable;
        let rejected_config = fs.workspace_for_creation_operation(
            "bind-child", wrong_config, key.operation_id(), budget, &token,
        ).await.err().expect("wrong original creation configuration");
        assert_ne!(*rejected_config.work, WorkCounters::UNCHARGED);
        child.write_text("/advance", "later head").await.expect("advance before bind");
        let later = child.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("later head").id();
        assert_ne!(later, created.id());
        let wrong_seed = root.bind_prepared_fork(frozen, child.clone(), later, key.operation_id(), &LazyTreapWork::new(budget, &token)).await.err().expect("current head cannot replace original seed");
        assert!(matches!(wrong_seed.semantic_error(), LazyWorkspaceError::Concurrent));
        let bound = root.bind_prepared_fork(frozen, child.clone(), created.id(), key.operation_id(), &LazyTreapWork::new(budget, &token)).await.expect("bind same created project");
        assert_eq!(bound.0.workspace().id(), child.id());
        assert_eq!(bound.0.workspace().head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("unchanged current head").id(), later);
        assert_eq!(bound.1.authored_generation, created.id());
        assert_eq!(bound.1.source, frozen.source);
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 0);
        assert_eq!(source.counts.pages.load(Ordering::Relaxed), 0);
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
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
            fs.open_workspace("root", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("workspace"),
            Arc::clone(&source),
            store.clone(),
        )
        .await
        .expect("open");
        let child = reopened
            .fork("child", IdempotencyKey::from_bytes([5; 16]))
            .await
            .expect("fork");
        let first_snapshot = child.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("snapshot");
        let second_snapshot = child.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("repeat snapshot");
        let resumed_child = root
            .open_related(fs.open_workspace("child", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("child workspace"))
            .await
            .expect("resume related child");
        let sibling = root
            .fork("sibling", IdempotencyKey::from_bytes([6; 16]))
            .await
            .expect("sibling fork");
        let resumed_sibling = resumed_child
            .open_related(
                fs.open_workspace("sibling", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
                    .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
                    .expect("sibling workspace"),
            )
            .await
            .expect("constant-work branch switch");
        assert_eq!(
            sibling.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("sibling snapshot"),
            resumed_sibling
                .snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()))
                .await
                .expect("resumed sibling snapshot")
        );

        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 0);
        assert_eq!(source.counts.pages.load(Ordering::Relaxed), 0);
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
        assert_eq!(
            child
                .inspect_state(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("ordinary inspection uses ordinary funding") })
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
        let head = root.workspace().head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("head").id();
        let mut checkout = root
            .workspace().checkout(crate::model::GenerationSelector::Head, crate::model::CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
            .expect("checkout");
        let LazyLookup::Source(node) = root.lookup("/file.txt").await.expect("source") else {
            panic!("expected unresolved source file");
        };
        let cancellation = CancellationToken::new();
        source.cancel_next_range();
        assert!(matches!(
            { let stage_cancellation = &cancellation; let stage_work = LazyTreapWork::new(WorkBudget::UNBOUNDED, stage_cancellation); let result = root.stage_into_checkout(&mut checkout, None, "/file.txt", root.source_file_id(&node.file_identity, &mut crate::kernel::allocation::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())).expect("source identity"), u64::MAX, &stage_work).await; stage_work.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }.map(|receipt| receipt.value).map_err(|failure| lazy_operation_failure(failure, std::convert::identity)),
            Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::Cancelled)
        ));
        assert!(cancellation.is_cancelled());
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 1);
        assert_eq!(root.workspace().head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("head").id(), head);
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

    #[tokio::test]
    async fn frozen_staging_captures_source_only_in_the_private_candidate() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs, "frozen-stage", Arc::clone(&source), MemoryLazyWorkspaceStore::default(),
        ).await.expect("attach");
        let token = CancellationToken::new();
        let budget = finite_fixture_budget();
        let snapshot = root.snapshot(&LazyTreapWork::new(budget, &token)).await.expect("frozen origin");
        let before = root.inspect_state(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("ordinary inspection uses ordinary funding") }).expect("original companion state");
        let lookup = root.lookup_at_snapshot_measured(snapshot, "/file.txt", budget, &token)
            .await.expect("frozen source lookup").value;
        let expected = root.direct_file_id_for_lookup(&lookup);
        let mut checkout = root.workspace().checkout(crate::model::GenerationSelector::Exact(snapshot.authored_generation), crate::model::CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("private exact generation");
        let failure = { let stage_cancellation = &token; let stage_work = LazyTreapWork::new(WorkBudget::default(), stage_cancellation); let result = root.stage_into_checkout(&mut checkout, Some(snapshot), "/file.txt", expected, 6, &stage_work).await; stage_work.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }.expect_err("zero future permit");
        assert_eq!(*failure.work, WorkCounters::default());
        assert_eq!(root.inspect_state(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("ordinary inspection uses ordinary funding") }).expect("unchanged rejected companion"), before);
        let staged = { let stage_cancellation = &token; let stage_work = LazyTreapWork::new(budget, stage_cancellation); let result = root.stage_into_checkout(&mut checkout, Some(snapshot), "/file.txt", expected, 6, &stage_work).await; stage_work.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }.expect("capture frozen source in candidate");
        assert!(staged.value);
        assert_eq!(staged.work.source_bytes_read, 6);
        staged.work.verify(budget).expect("actual finite capture receipt");
        let record = checkout.lookup_no_follow(
            &root.namespace_path("/file.txt").expect("path"), budget, &token,
        ).await.expect("actual staged record").value.record.expect("captured record");
        assert_eq!(record.file_id, expected);
        assert_eq!(root.inspect_state(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("ordinary inspection uses ordinary funding") }).expect("unchanged companion"), before);
        assert_eq!(root.workspace().head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("unpublished head").id(), snapshot.authored_generation);
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
        let original_head = root.workspace().head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("head").id();
        let mut checkout = root
            .workspace().checkout(crate::model::GenerationSelector::Head, crate::model::CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
            .expect("mount checkout");
        let other = root.namespace_path("/other.txt").expect("other path");
        checkout
            .apply_authored_transaction(
                vec![AuthoredMutation::CreateFile {
                    path: other.clone(),
                    bytes: acyclic_stream::OwnedBytes::from_static(b"dirty"),
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
            { let stage_cancellation = &cancelled; let stage_work = LazyTreapWork::new(WorkBudget::UNBOUNDED, stage_cancellation); let result = root.stage_into_checkout(&mut checkout, None, "/file.txt", root.source_file_id(&node.file_identity, &mut crate::kernel::allocation::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())).expect("source identity"), u64::MAX, &stage_work).await; stage_work.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }.map(|receipt| receipt.value).map_err(|failure| lazy_operation_failure(failure, std::convert::identity)),
            Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::Cancelled)
        ));
        assert!(
            { let stage_cancellation = &CancellationToken::new(); let stage_work = LazyTreapWork::new(WorkBudget::UNBOUNDED, stage_cancellation); let result = root.stage_into_checkout(&mut checkout, None, "/file.txt", root.source_file_id(&node.file_identity, &mut crate::kernel::allocation::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())).expect("source identity"), u64::MAX, &stage_work).await; stage_work.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }.map(|receipt| receipt.value).map_err(|failure| lazy_operation_failure(failure, std::convert::identity))
            .expect("stage source in dirty mount")
        );
        assert!(
            !{ let stage_cancellation = &CancellationToken::new(); let stage_work = LazyTreapWork::new(WorkBudget::UNBOUNDED, stage_cancellation); let result = root.stage_into_checkout(&mut checkout, None, "/file.txt", root.source_file_id(&node.file_identity, &mut crate::kernel::allocation::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())).expect("source identity"), u64::MAX, &stage_work).await; stage_work.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }.map(|receipt| receipt.value).map_err(|failure| lazy_operation_failure(failure, std::convert::identity))
                .expect("already staged source identity")
        );
        assert_eq!(
            root.workspace()
                .head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
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
            .commit(crate::OperationId::new(), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &CancellationToken::new(), |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) })
            .await
            .expect("publish both changes once");
        assert_ne!(
            root.workspace().head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("published head").id(),
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
            .workspace().checkout(crate::model::GenerationSelector::Head, crate::model::CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
            .expect("mount checkout");
        for path in ["/a", "/b"] {
            let LazyLookup::Source(node) = root.lookup(path).await.expect("source alias") else {
                panic!("expected unresolved source alias");
            };
            assert!(
                { let stage_cancellation = &CancellationToken::new(); let stage_work = LazyTreapWork::new(WorkBudget::UNBOUNDED, stage_cancellation); let result = root.stage_into_checkout(&mut checkout, None, path, root.source_file_id(&node.file_identity, &mut crate::kernel::allocation::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())).expect("source identity"), u64::MAX, &stage_work).await; stage_work.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }.map(|receipt| receipt.value).map_err(|failure| lazy_operation_failure(failure, std::convert::identity))
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
            .workspace().checkout(crate::model::GenerationSelector::Head, crate::model::CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
            .expect("mount checkout");
        checkout
            .apply_authored_transaction(
                vec![AuthoredMutation::CreateFile {
                    path: root.namespace_path("/new.txt").expect("new path"),
                    bytes: acyclic_stream::OwnedBytes::from_static(b"new"),
                    metadata: FileMetadata::default(),
                }],
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .expect("unpublished file");
        let source_page = root
            .list_directory_in_checkout(&mut checkout, "/", None, 16, None, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value.0)
            .expect("source page");
        assert_eq!(source_page.entries.len(), 1);
        assert!(!source_page.entries[0].authored);
        let authored_page = root
            .list_directory_in_checkout(&mut checkout, "/", source_page.next, 16, None, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value.0)
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
                VolumeLimits::default(), None, None,
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
            .workspace().checkout(crate::model::GenerationSelector::Head, crate::model::CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
            .expect("mount checkout");
        assert_eq!(
            root.list_directory_in_checkout(&mut checkout, "/d", None, 16, None, WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await
                .expect("source directory")
                .value.0
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
            .list_directory_in_checkout(&mut checkout, "/d", None, 16, None, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value.0)
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
        let original_head = root.workspace().head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("head").id();
        let LazyLookup::Source(node) = root.lookup("/file.txt").await.expect("source") else {
            panic!("expected source file");
        };
        root.remove_if("/file.txt", Some(root.source_file_id(&node.file_identity, &mut crate::kernel::allocation::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())).expect("source identity")))
            .await
            .expect("source-only tombstone");
        assert_eq!(
            root.workspace().head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("unchanged head").id(),
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
        (async { let cancellation = &crate::CancellationToken::new(); let accounting = LazyTreapWork::new(crate::WorkBudget::UNBOUNDED, cancellation); let result = root.prepare_mount_removals(&paths, &[], &[], aborted, &accounting).await; accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
            .expect("prepare uncommitted whiteout");
        assert!(!(async { let cancellation = &crate::CancellationToken::new(); let accounting = LazyTreapWork::new(crate::WorkBudget::UNBOUNDED, cancellation); let result = root.finish_mount_removals(&accounting).await; accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("rollback"));
        assert!(matches!(
            root.lookup("/file.txt").await.expect("source remains"),
            LazyLookup::Source(_)
        ));

        let committed = IdempotencyKey::new();
        (async { let cancellation = &crate::CancellationToken::new(); let accounting = LazyTreapWork::new(crate::WorkBudget::UNBOUNDED, cancellation); let result = root.prepare_mount_removals(&paths, &[], &[], committed, &accounting).await; accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
            .expect("prepare committed whiteout");
        let mut checkout = root
            .workspace().checkout(crate::model::GenerationSelector::Head, crate::model::CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
            .expect("checkout");
        assert!(matches!(
            checkout
                .commit_even_if_clean(committed.operation_id(), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &CancellationToken::new(), &mut |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }, None)
                .await
                .expect("publish unchanged authored root")
                .value,
            crate::CheckoutCommitOutcome::Committed { .. }
        ));
        assert!((async { let cancellation = &crate::CancellationToken::new(); let accounting = LazyTreapWork::new(crate::WorkBudget::UNBOUNDED, cancellation); let result = root.finish_mount_removals(&accounting).await; accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("finalize"));
        assert!(matches!(
            root.lookup("/file.txt").await,
            Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::NotFound)
        ));
    }

    /// A mounted removal shadows an identity whose content only an unpublished checkout staged; publication then flushes a generation without it.
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
            let mut checkout = root
                .workspace().checkout(crate::model::GenerationSelector::Head, crate::model::CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
                .expect("checkout");
            checkout
                .create(crate::AuthoredMutation::CreateFile { path: path.clone(), bytes: (content.clone()).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)
                .await
                .expect("stage content");
            let identity = checkout
                .lookup_no_follow_with_metadata(&path, WorkBudget::UNBOUNDED, &cancellation)
                .await
                .expect("lookup")
                .value
                .expect("staged file");
            checkout
                .mutate(vec![crate::kernel::Mutation::Remove { path: path.clone(), expected_file_id: (None).map_or(crate::kernel::MetadataField::Unavailable, crate::kernel::MetadataField::Value) }], WorkBudget::UNBOUNDED, &cancellation)
                .await
                .expect("remove");
            (async { let cancellation = &crate::CancellationToken::new(); let accounting = LazyTreapWork::new(crate::WorkBudget::UNBOUNDED, cancellation); let result = root.prepare_mount_removals(&[], &[], &[(identity.record, identity.metadata)], removal, &accounting).await; accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
                .expect("shadow the removed identity");
            checkout
                .commit_even_if_clean(removal.operation_id(), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancellation, &mut |_| async { crate::FsResult::Ok(crate::FsReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }, None)
                .await
                .expect("publish without the removed file");
            assert!((async { let cancellation = &crate::CancellationToken::new(); let accounting = LazyTreapWork::new(crate::WorkBudget::UNBOUNDED, cancellation); let result = root.finish_mount_removals(&accounting).await; accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("finish"));
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
            fs.open_workspace("power-loss-shadow", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
                .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
                .expect("workspace"),
            Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
            crate::core_state::LocalCoreStateStore::new(&state_root),
        )
        .await
        .expect("reopen lazy");
        let state = reopened.inspect_state(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("ordinary inspection uses ordinary funding") }).expect("state");
        let shadow = { let mut accounting = LazyReadonlyAccounting::Portable { work: WorkCounters::UNCHARGED, budget: WorkBudget::UNBOUNDED, cancellation: &cancellation }; let result = reopened.shadow_record(state.shadows, shadowed.file_id, &mut accounting).await; accounting.finish_measured(result) }.expect("shadow lookup").value.expect("the removed identity stays shadowed");
        let read = crate::DetachedFile::from_record(reopened.workspace().volume.clone(), shadow.0, WorkBudget::UNBOUNDED, &cancellation).expect("validated shadow record").value.read_range(crate::ByteRange {
                    offset: 0,
                    length: u64::try_from(content.len()).expect("length"),
                }, WorkBudget::UNBOUNDED, &cancellation).await
            .expect("the shadowed identity's content survived");
        assert_eq!(read.value.bytes.as_ref(), content.as_ref());
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
        let before = root.inspect_state(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("ordinary inspection uses ordinary funding") }).expect("state before inspection");

        let inspected = root.inspect_unauthored("/file.txt", InspectBasis::default(),
            &LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("inspect source file");
        let (lookup, source_reference) = inspected.as_state().clone();
        let LazyLookup::Source(node) = lookup else {
            panic!("inspection must resolve the source file");
        };
        assert_eq!(root.inspect_state(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("ordinary inspection uses ordinary funding") }).expect("state after inspection"), before);

        source.replace(Bytes::from_static(b"after"));
        assert_eq!((root.open_source_file(
            "/file.txt",
            source_reference.expect("source reference"),
            node,
        )
        .await
        .expect("open pinned version")
        .read_range(0, 6, &mut crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new()))
        .expect("read pinned version")
        .bytes).as_ref(), b"before");

        root.rebind_source(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("ordinary rebind uses ordinary funding") }).expect("rebind source");
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
        let first = root.workspace().stat("/a", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("first stat");
        let second = root.workspace().stat("/b", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("second stat");
        assert_eq!(first.file_id, second.file_id);
        assert_eq!(first.file_id, root.source_file_id(&source.node.file_identity, &mut crate::kernel::allocation::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())).expect("source identity"));
        assert_eq!(first.link_count, 2);
        assert_eq!(second.link_count, 2);
        assert_eq!(first.metadata.posix_mode, Some(0o100640));
        assert_eq!(second.metadata.modified_ns, Some(123_456));

        root.write("/a", Bytes::from_static(b"changed"))
            .await
            .expect("mutate alias");
        assert_eq!(
            root.workspace()
                .read("/b", 64, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
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
        let first = root.workspace().stat("/a", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("first stat");
        let second = root.workspace().stat("/b", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("second stat");
        assert_eq!(first.file_id, second.file_id);
        assert_eq!(first.file_id, root.source_file_id(&source.node.file_identity, &mut crate::kernel::allocation::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())).expect("source identity"));
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
            fs.open_workspace("hard-link-shadow", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
                .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
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
                VolumeLimits::default(), None, None,
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
        let first = root.list_directory("/", None, 1, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).expect("first page");
        let name = std::str::from_utf8(first.entries[0].name.as_bytes()).expect("portable name");
        root.inspect(&format!("/{name}"))
            .await
            .expect("entry metadata");
        let second = root
            .list_directory("/", first.next, 1, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await.map(|receipt| receipt.value)
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
        assert_eq!(root.source_file_id(&first.file_identity, &mut crate::kernel::allocation::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())).expect("source identity"), root.source_file_id(&second.file_identity, &mut crate::kernel::allocation::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())).expect("source identity"));
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
                .read("/deep/file.txt", bytes.len() as u64, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
                .expect("authored content").as_ref(),
            bytes.as_ref()
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
                VolumeLimits::default(), None, None,
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
        let source_id = root.source_file_id(&source_node.file_identity, &mut crate::kernel::allocation::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())).expect("source identity");


        root.promote("/repo", 0, IdempotencyKey::from_bytes([0x52; 16]))
            .await
            .expect("promote directory");

        let promoted = root.lookup("/repo").await.expect("promoted directory");
        assert_eq!(
            root.stable_file_id_for_lookup("/repo", &promoted, &mut LazyReadonlyAccounting::Portable { work: WorkCounters::UNCHARGED, budget: WorkBudget::UNBOUNDED, cancellation: &CancellationToken::new() })
                .await
                .expect("stable projection identity"),
            source_id
        );
        assert_eq!((root.read("/repo/HEAD", 64).await.expect("lazy child")).as_ref(), b"ref: refs/heads/main\n");
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
        let expected_source = root.source_file_id(&source_node.file_identity, &mut crate::kernel::allocation::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())).expect("source identity");
        root.write("/file.txt", Bytes::from_static(b"replacement"))
            .await
            .expect("authored replacement");

        root.promote_exact(
            "/file.txt",
            expected_source,
            64,
            IdempotencyKey::from_bytes([0x73; 16]), &LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()),
        )
        .await
        .expect("identity-preserving write is already promoted");
        assert_eq!((root.read("/file.txt", 64)
            .await
            .expect("replacement remains")).as_ref(), b"replacement");
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
        assert_eq!((root.read("/file.txt", 32).await.expect("authored read")).as_ref(), b"authored");
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 1);
        root.remove("/file.txt").await.expect("remove");
        assert!(matches!(
            root.stat("/file.txt").await,
            Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::NotFound)
        ));
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn frozen_upload_batch_publishes_data_descriptor_and_receipt_in_one_actual_generation() {
        for deny_at_seal in [false, true] {
            let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
            let root = LazyWorkspace::attach(
                &Fs::memory(), "frozen-whole-upload", Arc::clone(&source),
                MemoryLazyWorkspaceStore::default(),
            ).await.expect("actual lazy root");
            let basis = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("exact captured upload basis");
            let token = CancellationToken::new();
            let calls = AtomicUsize::new(0);
            let mut owner = || {
                let index = calls.fetch_add(1, Ordering::Relaxed);
                async move {
                    if deny_at_seal && index == 13 { Err(LazyWorkspaceError::StaleIdentity) }
                    else { Ok(()) }
                }
            };
            let mutations = [
                LazyWriteMutation::CreateDirectory { path: "/data" },
                LazyWriteMutation::PutFile { path: "/file.txt", bytes: acyclic_stream::OwnedBytes::from_static(b"data") },
                LazyWriteMutation::CreateDirectory { path: "/.system/harness-uploads" },
                LazyWriteMutation::CreateDirectory { path: "/.system/harness-file-metadata" },
                LazyWriteMutation::PutFile {
                    path: "/.system/harness-file-metadata/file", bytes: acyclic_stream::OwnedBytes::from_static(b"descriptor"),
                },
                LazyWriteMutation::PutFile {
                    path: "/.system/harness-uploads/receipt", bytes: acyclic_stream::OwnedBytes::from_static(b"receipt"),
                },
            ];
            let result = root.write_at_snapshot_guarded(
                basis, mutations, IdempotencyKey::new(), crate::PublicationPermit::Unrestricted,
                &token, &mut owner,
            ).await;
            assert_eq!(calls.load(Ordering::Relaxed), if deny_at_seal { 14 } else { 16 },
                "actual members, protected lookup, prepared companion, DATA seal and final companion CAS");
            if deny_at_seal {
                assert!(matches!(result.err().expect("owner denial at actual CAS").semantic_error(),
                    LazyWorkspaceError::StaleIdentity));
                assert_eq!(root.workspace().head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("no upload member published").id(),
                    basis.authored_generation);
                assert!(root.workspace().stat("/.system/harness-uploads/receipt", WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
                assert!(root.workspace().stat("/.system/harness-file-metadata/file", WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
            } else {
                let (commit, proof) = result.expect("one original whole candidate");
                let generation = match commit {
                    TransactionCommit::Committed(generation) | TransactionCommit::AlreadyCommitted(generation) => generation,
                    _ => panic!("actual complete upload publication"),
                };
                assert_eq!(proof.expect("genuine whole candidate snapshot").authored_generation, generation.id());
                for (path, expected) in [
                    ("/file.txt", b"data".as_slice()),
                    ("/.system/harness-file-metadata/file", b"descriptor".as_slice()),
                    ("/.system/harness-uploads/receipt", b"receipt".as_slice()),
                ] {
                    assert_eq!(generation.read(path, 64, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("same physical generation").as_ref(), expected);
                }
                assert_eq!(root.workspace().stat("/file.txt", WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("original source file identity").file_id,
                    root.source_file_id(&source.node.file_identity, &mut crate::kernel::allocation::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())).expect("source identity"));
            }
            assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
        }
    }

    #[tokio::test]
    async fn frozen_writer_preserves_original_source_authored_and_shadow_identity() {
        for mode in 0..3 {
            let mut source = CountingSource::new(Bytes::from_static(b"source"));
            source.node.link_count = Some(2);
            let source = Arc::new(source);
            let root = LazyWorkspace::attach(
                &Fs::memory(), "frozen-write-identities", Arc::clone(&source),
                MemoryLazyWorkspaceStore::default(),
            ).await.expect("actual lazy workspace");
            if mode != 0 {
                root.write("/a", Bytes::from_static(b"authored")).await.expect("author original identity");
                if mode == 2 { root.remove("/a").await.expect("capture detached original shadow"); }
            }
            let lookup = root.lookup("/b").await.expect("capture exact source alias");
            assert!(matches!((&lookup, mode),
                (LazyLookup::Source(_), 0) | (LazyLookup::Authored { .. }, 1)
                    | (LazyLookup::Shadow { .. }, 2)));
            let basis = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("authentic freeze");
            let reads = source.counts.ranges.load(Ordering::Relaxed);
            let token = CancellationToken::new();
            let calls = AtomicUsize::new(0);
            let mut owner = || {
                calls.fetch_add(1, Ordering::Relaxed);
                async { Ok::<(), LazyWorkspaceError>(()) }
            };
            let (commit, proof) = root.write_at_snapshot_guarded(
            basis,
            std::iter::once(LazyWriteMutation::PutFile { path: "/b", bytes: (Bytes::from_static(b"replacement")).into() }),
            IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted,
            &token,
            &mut owner,
        ).await.expect("same guarded compiler");
            let generation = match commit {
                TransactionCommit::Committed(generation) | TransactionCommit::AlreadyCommitted(generation) => generation,
                _ => panic!("exact original candidate must publish"),
            };
            let published = proof.expect("actual final snapshot proof");
            assert_eq!(published.authored_generation, generation.id());
            assert_ne!(published.authored_generation, basis.authored_generation);
            assert_eq!((published.source, published.overlay, published.shadows),
                (basis.source, basis.overlay, basis.shadows));
            assert_eq!(root.workspace().stat("/b", WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("published identity").file_id,
                root.source_file_id(&source.node.file_identity, &mut crate::kernel::allocation::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())).expect("source identity"));
            assert_eq!(root.read_at_snapshot_measured(
                published, "/b", 64, finite_fixture_budget(), &token,
            ).await.expect("reader consumes actual final proof").value.as_ref(), b"replacement");
            assert_eq!(source.counts.ranges.load(Ordering::Relaxed), reads,
                "complete replacement never fetches the old source body");
            assert!(calls.load(Ordering::Relaxed) >= 3, "mutations and actual seal use owner");
            if mode == 1 {
                assert_eq!(root.read("/a", 64).await.expect("other authored hardlink").as_ref(), b"replacement");
            }
        }
    }

    #[tokio::test]
    async fn frozen_writer_rejects_stale_basis_and_preserves_concurrent_seal_edit() {
        for advance_at_seal in [false, true] {
            let root = LazyWorkspace::attach(
                &Fs::memory(), "frozen-write-concurrent",
                Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
                MemoryLazyWorkspaceStore::default(),
            ).await.expect("actual workspace");
            root.write("/target", Bytes::from_static(b"before")).await.expect("original authored target");
            let basis = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("first exact capture");
            if !advance_at_seal {
                root.workspace().write("/concurrent", Bytes::from_static(b"kept")).await.expect("concurrent head");
            }
            let calls = AtomicUsize::new(0);
            let token = CancellationToken::new();
            let mut owner = || {
                let index = calls.fetch_add(1, Ordering::Relaxed);
                let root = &root;
                async move {
                    if advance_at_seal && index == 5 {
                        root.workspace().write("/concurrent", Bytes::from_static(b"kept"))
                            .await.map_err(LazyWorkspaceError::WorkspaceCause)?;
                    }
                    Ok::<(), LazyWorkspaceError>(())
                }
            };
            let (commit, proof) = root.write_at_snapshot_guarded(
            basis,
            std::iter::once(LazyWriteMutation::PutFile { path: "/target", bytes: (Bytes::from_static(b"unpublished")).into() }),
            IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted,
            &token,
            &mut owner,
        ).await.expect("semantic stale CAS decision");
            assert!(matches!(commit, TransactionCommit::Conflict { .. }));
            assert!(proof.is_err());
            assert_eq!(root.read("/target", 64).await.expect("original target").as_ref(), b"before");
            assert_eq!(root.workspace().read("/concurrent", 64, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("concurrent edit retained").as_ref(), b"kept");
            if advance_at_seal {
                assert!(calls.load(Ordering::Relaxed) >= 3, "owner runs at real canonical seal");
            }
        }
    }

    #[tokio::test]
    async fn frozen_writer_original_key_recovers_only_the_exact_authenticated_basis() {
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &Fs::memory(), "frozen-write-original-key", Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        ).await.expect("actual workspace");
        root.write("/target", Bytes::from_static(b"before")).await.expect("original target");
        let basis = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("first authentic freeze");
        let key = IdempotencyKey::new();
        let token = CancellationToken::new();
        let mut owner = || async { Ok::<(), LazyWorkspaceError>(()) };
        let (first, proof) = root.write_at_snapshot_guarded(
            basis,
            std::iter::once(LazyWriteMutation::PutFile { path: "/target", bytes: (Bytes::from_static(b"published")).into() }),
            key,
            crate::PublicationPermit::Unrestricted,
            &token,
            &mut owner,
        ).await.expect("actual original publication");
        let original_generation = match first {
            TransactionCommit::Committed(generation) | TransactionCommit::AlreadyCommitted(generation) => generation.id(),
            _ => panic!("original publication"),
        };
        proof.expect("genuine first final snapshot");
        let (recovered, proof) = root.write_at_snapshot_guarded(
            basis,
            std::iter::once(LazyWriteMutation::PutFile { path: "/target", bytes: (Bytes::from_static(b"published")).into() }),
            key,
            crate::PublicationPermit::Unrestricted,
            &token,
            &mut owner,
        ).await.expect("same original candidate reconstructs lost ACK");
        assert!(matches!(&recovered, TransactionCommit::AlreadyCommitted(generation)
            if generation.id() == original_generation));
        assert_eq!(proof.expect("recovered exact roots").authored_generation, original_generation);
        for (path, payload) in [
            ("/target", Bytes::from_static(b"different")),
            ("/different-path", Bytes::from_static(b"published")),
        ] {
            let (actual, proof) = root.write_at_snapshot_guarded(
            basis,
            std::iter::once(LazyWriteMutation::PutFile { path: path, bytes: (payload).into() }),
            key,
            crate::PublicationPermit::Unrestricted,
            &token,
            &mut owner,
        ).await.expect("actual original operation is not absence on mismatched input");
            assert!(matches!(&actual, TransactionCommit::AlreadyCommitted(generation)
                if generation.id() == original_generation));
            assert!(matches!(proof.expect_err("full bytes and path bind the original candidate").semantic_error(),
                LazyWorkspaceError::WriteInputConflict { operation } if *operation == key.operation_id()));
        }
        let mut denied_owner = || async {
            Err::<(), LazyWorkspaceError>(LazyWorkspaceError::Filesystem(crate::FsError::MutationNotAllowed))
        };
        let uninspected = std::iter::from_fn(|| -> Option<LazyWriteMutation<'static>> {
            panic!("a foreign owner must not inspect or consume the protected candidate");
        });
        let denied = root.original_write_snapshot_guarded(
            basis, uninspected, key, &token, &mut denied_owner,
        ).await.err().expect("actual owner denies before protected lookup");
        assert!(matches!(denied.semantic_error(), LazyWorkspaceError::Filesystem(crate::FsError::MutationNotAllowed)));
        let absent_input = std::iter::from_fn(|| -> Option<LazyWriteMutation<'static>> {
            panic!("an authentic absent original must leave the caller's staging input untouched");
        });
        assert!(root.original_write_snapshot_guarded(
            basis, absent_input, IdempotencyKey::new(), &token, &mut owner,
        ).await.expect("authenticated absent original").is_none());
        source.invalidate();
        root.rebind_source(&LazyTreapWork::new(finite_fixture_budget(), &token)).await.expect("actual changed source binding");
        let other_basis = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("authentic but different source freeze");
        assert_ne!(other_basis.source, basis.source);
        let (rejected, proof) = root.write_at_snapshot_guarded(
            other_basis,
            std::iter::once(LazyWriteMutation::PutFile { path: "/target", bytes: (Bytes::from_static(b"published")).into() }),
            key,
            crate::PublicationPermit::Unrestricted,
            &token,
            &mut owner,
        ).await.expect("canonical original-operation mismatch decision");
        assert!(matches!(&rejected, TransactionCommit::AlreadyCommitted(generation)
            if generation.id() == original_generation));
        assert!(matches!(proof.expect_err("different immutable source must not replay").semantic_error(),
            LazyWorkspaceError::WriteInputConflict { operation } if *operation == key.operation_id()));
        assert_eq!(root.workspace().head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("original publication retained").id(), original_generation);
    }

    #[tokio::test]
    async fn frozen_writer_rejects_forged_snapshot_and_changed_original_source_before_mutation() {
        for change_source in [false, true] {
            let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
            let root = LazyWorkspace::attach(
                &Fs::memory(), "frozen-write-original-proof", Arc::clone(&source),
                MemoryLazyWorkspaceStore::default(),
            ).await.expect("actual source binding");
            let original = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("genuine original capture");
            let mut supplied = original;
            if change_source { source.invalidate(); }
            else { supplied.source.epoch = supplied.source.epoch.saturating_add(1); }
            let calls = AtomicUsize::new(0);
            let token = CancellationToken::new();
            let mut owner = || {
                calls.fetch_add(1, Ordering::Relaxed);
                async { Ok::<(), LazyWorkspaceError>(()) }
            };
            root.write_at_snapshot_guarded(
            supplied,
            std::iter::once(LazyWriteMutation::PutFile { path: "/file.txt", bytes: (Bytes::from_static(b"unpublished")).into() }),
            IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted,
            &token,
            &mut owner,
        ).await.err().expect("original snapshot identity is not caller-minted authority");
            assert_eq!(calls.load(Ordering::Relaxed), 1, "actual owner authorizes protected inspection, never a mutation");
            assert_eq!(root.workspace().head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("unchanged original authored head").id(),
                original.authored_generation);
            assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
        }
    }

    #[tokio::test]
    async fn frozen_writer_owner_denial_at_cas_cannot_publish_prepared_content() {
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &Fs::memory(), "frozen-write-owner", Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        ).await.expect("actual workspace");
        let basis = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("authentic initial basis");
        let calls = AtomicUsize::new(0);
        let token = CancellationToken::new();
        let mut owner = || {
            let index = calls.fetch_add(1, Ordering::Relaxed);
            async move {
                if index == 6 { Err(LazyWorkspaceError::StaleIdentity) }
                else { Ok(()) }
            }
        };
        let failure = root.write_at_snapshot_guarded(
            basis,
            std::iter::once(LazyWriteMutation::PutFile { path: "/file.txt", bytes: (Bytes::from_static(b"not-published")).into() }),
            IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted,
            &token,
            &mut owner,
        ).await.err().expect("actual publication owner denial");
        assert!(matches!(failure.semantic_error(), LazyWorkspaceError::StaleIdentity));
        assert_eq!(calls.load(Ordering::Relaxed), 7, "original lookup, parents/write/identity, companion and actual DATA CAS");
        assert_eq!(root.workspace().head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("unchanged authoritative head").id(), basis.authored_generation);
        assert_eq!(root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("unchanged exact companion"), basis);
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn frozen_writer_postpublish_companion_failure_retains_real_generation_and_cause() {
        #[derive(Clone, Default)]
        struct ProofFailureStore {
            actual: MemoryLazyWorkspaceStore,
            armed_reads: Arc<AtomicUsize>,
        }
        impl LazyWorkspaceStore for ProofFailureStore {
            type Error = LazyWorkspaceError;
            async fn load_lazy_workspace(&self, id: WorkspaceId) -> Result<Option<LazyWorkspaceState>, Self::Error> {
                self.actual.load_lazy_workspace(id).await
            }
            async fn load_lazy_workspace_measured(
                &self, id: WorkspaceId, budget: WorkBudget, token: &CancellationToken,
            ) -> Result<OperationReceipt<Option<LazyWorkspaceState>>, LazyWorkspaceError> {
                let actual = self.actual.load_lazy_workspace_measured(id, budget, token).await?;
                let reads = self.armed_reads.load(Ordering::Relaxed);
                if reads != 0 && self.armed_reads.fetch_add(1, Ordering::Relaxed) == 3 {
                    return Err(LazyWorkspaceError::Concurrent.with_prior_work(actual.work));
                }
                Ok(actual)
            }
            async fn compare_and_swap_lazy_workspace(
                &self, id: WorkspaceId, revision: u64, value: LazyWorkspaceState,
            ) -> Result<bool, Self::Error> {
                self.actual.compare_and_swap_lazy_workspace(id, revision, value).await
            }
            async fn compare_and_swap_lazy_workspace_measured(
                &self, id: WorkspaceId, revision: u64, value: LazyWorkspaceState,
                budget: WorkBudget, token: &CancellationToken,
            ) -> Result<OperationReceipt<bool>, LazyWorkspaceError> {
                self.actual.compare_and_swap_lazy_workspace_measured(id, revision, value, budget, token).await
            }
            async fn load_lazy_overlay(&self, id: LazyOverlayId) -> Result<Option<LazyOverlay>, Self::Error> {
                self.actual.load_lazy_overlay(id).await
            }
            async fn load_lazy_overlay_measured(
                &self, id: LazyOverlayId, budget: WorkBudget, token: &CancellationToken,
            ) -> Result<OperationReceipt<Option<LazyOverlay>>, LazyWorkspaceError> {
                self.actual.load_lazy_overlay_measured(id, budget, token).await
            }
            async fn put_lazy_overlay(&self, id: LazyOverlayId, value: LazyOverlay) -> Result<(), Self::Error> {
                self.actual.put_lazy_overlay(id, value).await
            }
            async fn put_lazy_overlay_measured(
                &self, id: LazyOverlayId, value: LazyOverlay, budget: WorkBudget, token: &CancellationToken,
            ) -> Result<OperationReceipt<()>, LazyWorkspaceError> {
                self.actual.put_lazy_overlay_measured(id, value, budget, token).await
            }
            async fn load_lazy_shadow(&self, id: LazyShadowId) -> Result<Option<LazyShadow>, Self::Error> {
                self.actual.load_lazy_shadow(id).await
            }
            async fn load_lazy_shadow_measured(
                &self, id: LazyShadowId, budget: WorkBudget, token: &CancellationToken,
            ) -> Result<OperationReceipt<Option<LazyShadow>>, LazyWorkspaceError> {
                self.actual.load_lazy_shadow_measured(id, budget, token).await
            }
            async fn put_lazy_shadow(&self, id: LazyShadowId, value: LazyShadow) -> Result<(), Self::Error> {
                self.actual.put_lazy_shadow(id, value).await
            }
            async fn put_lazy_shadow_measured(
                &self, id: LazyShadowId, value: LazyShadow, budget: WorkBudget, token: &CancellationToken,
            ) -> Result<OperationReceipt<()>, LazyWorkspaceError> {
                self.actual.put_lazy_shadow_measured(id, value, budget, token).await
            }
        }
        let store = ProofFailureStore::default();
        let root = LazyWorkspace::attach(
            &Fs::memory(), "frozen-write-postpublish",
            Arc::new(CountingSource::new(Bytes::from_static(b"source"))), store.clone(),
        ).await.expect("actual lazy companion");
        root.write("/target", Bytes::from_static(b"before")).await.expect("original target");
        let basis = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("actual capture");
        let key = IdempotencyKey::new();
        let calls = AtomicUsize::new(0);
        let token = CancellationToken::new();
        let mut owner = || {
            let index = calls.fetch_add(1, Ordering::Relaxed);
            if index == 5 { store.armed_reads.store(1, Ordering::Relaxed); }
            async { Ok::<(), LazyWorkspaceError>(()) }
        };
        let (commit, proof) = root.write_at_snapshot_guarded(
            basis,
            std::iter::once(LazyWriteMutation::PutFile { path: "/target", bytes: (Bytes::from_static(b"published")).into() }),
            key,
            crate::PublicationPermit::Unrestricted,
            &token,
            &mut owner,
        ).await.expect("published DATA must not be erased by proof failure");
        let generation = match commit {
            TransactionCommit::Committed(generation) | TransactionCommit::AlreadyCommitted(generation) => generation,
            _ => panic!("actual CAS has already published"),
        };
        assert!(matches!(proof.expect_err("postpublish proof genuinely unavailable").semantic_error(),
            LazyWorkspaceError::Concurrent));
        assert_eq!(root.workspace().head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("actual physical publication").id(), generation.id());
        assert_ne!(generation.id(), basis.authored_generation);
        assert_eq!(generation.read("/target", 64, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("actual committed bytes").as_ref(), b"published");
        assert_eq!(root.workspace().operation_generation(key, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
            .expect("authentic original operation").expect("durable ACK recovery").id(), generation.id());
        let pending = store.actual.load_lazy_workspace(root.workspace().id()).await
            .expect("real prepared companion").expect("existing binding").pending_write
            .expect("lost companion ACK retains original source evidence");
        assert_eq!(pending.operation, key.operation_id());
        assert_eq!(pending.basis, basis);
        store.armed_reads.store(0, Ordering::Relaxed);
        let workspace = root.workspace().clone();
        let source = Arc::clone(&root.source);
        drop(root);
        let reopened = LazyWorkspace::open(workspace, Arc::clone(&source), store.clone()).await
            .expect("cold original producer reopens the same companion and authority");
        reopened.workspace().write("/concurrent", Bytes::from_static(b"preserved")).await
            .expect("later actual authored edit");
        source.invalidate();
        reopened.rebind_source(&LazyTreapWork::new(finite_fixture_budget(), &token)).await
            .expect("actual later source epoch");
        let current = store.actual.load_lazy_workspace(reopened.workspace().id()).await
            .expect("later real companion").expect("binding");
        let mut recovery_owner = || async { Ok::<(), LazyWorkspaceError>(()) };
        let (original, proof) = reopened.original_write_snapshot_guarded(
            basis, std::iter::once(LazyWriteMutation::PutFile {
                path: "/target", bytes: acyclic_stream::OwnedBytes::from_static(b"published"),
            }), key, &token, &mut recovery_owner,
        ).await.expect("authenticate original authority before current-root comparison")
            .expect("actual original DATA exists");
        let recovered = proof.expect("genuine original pending companion finalization");
        assert_eq!(original.id(), generation.id());
        assert_eq!(recovered.authored_generation, generation.id());
        assert_eq!((recovered.source, recovered.overlay, recovered.shadows),
            (basis.source, basis.overlay, basis.shadows));
        let settled = store.actual.load_lazy_workspace(reopened.workspace().id()).await
            .expect("actual companion CAS").expect("binding");
        assert!(settled.pending_write.is_none());
        assert_eq!((settled.source, settled.overlay, settled.shadows),
            (current.source, current.overlay, current.shadows), "settlement cannot overwrite concurrent roots");
        assert_eq!(reopened.workspace().read("/concurrent", 64, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
            .expect("concurrent DATA remains").as_ref(), b"preserved");
        let head = reopened.workspace().head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("later head").id();
        let (ack, proof) = reopened.write_at_snapshot_guarded(
            basis,
            std::iter::once(LazyWriteMutation::PutFile { path: "/target", bytes: (Bytes::from_static(b"published")).into() }),
            key,
            crate::PublicationPermit::Unrestricted,
            &token,
            &mut recovery_owner,
        ).await.expect("warm original ACK never restages the stale source");
        assert!(matches!(&ack, TransactionCommit::AlreadyCommitted(actual) if actual.id() == original.id()));
        assert_eq!(proof.expect("exact stored final snapshot"), recovered);
        assert_eq!(reopened.workspace().head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("no retry publication").id(), head);
    }

    #[tokio::test]
    async fn original_write_metadata_codec_preserves_exact_context_and_all_historical_permit_forms() {
        let root = LazyWorkspace::attach(
            &Fs::memory(), "original-write-codec",
            Arc::new(CountingSource::new(Bytes::from_static(b"source"))), MemoryLazyWorkspaceStore::default(),
        ).await.expect("actual captured SDK context");
        let basis = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("genuine source and immutable roots");
        let operation = IdempotencyKey::new().operation_id();
        let permits = [
            (crate::PublicationPermit::Unrestricted, 333),
            (crate::PublicationPermit::Lease {
                authority_id: [1; 16], workspace_id: basis.workspace_id.into_bytes(),
                lease_id: [2; 16], expires_at_millis: 123,
            }, 389),
            (crate::PublicationPermit::Reservation {
                operation_id: [3; 16], gate_tail: 7,
                expected: crate::Head {
                    epoch: crate::Epoch::new(3).expect("valid historical scalar"),
                    sequence: crate::Sequence::new(4), digest: basis.authored_generation.digest(),
                },
            }, 405),
        ];
        let token = CancellationToken::new();
        let work = LazyTreapWork::new(finite_fixture_budget(), &token);
        for (permit, expected_len) in permits {
            let scope = TreapAllocationScope::new(&work);
            let prepared = PendingLazyWrite {
                operation, basis, input: crate::Digest::from_bytes([5; 32]),
                config: root.workspace().volume_config(), permit,
            };
            let bytes = lazy_write_original_bytes(prepared, &work).expect("fund before encoding and control promotion");
            assert_eq!(bytes.len(), expected_len);
            assert!(bytes.len() <= LAZY_WRITE_ORIGINAL_MAX_BYTES);
            let decode = |raw: &[u8], expected| work.with_allocation_state(
                |allocations, spent, budget, cancellation| {
                    let mut accounting = crate::kernel::allocation::KernelAccounting::borrowed_standalone(
                        allocations, spent, budget, cancellation,
                    );
                    lazy_write_decode_original_with_accounting(raw, expected, &mut accounting)
                },
            );
            assert_eq!(decode(bytes.as_ref(), operation).expect("exact canonical fixed decoder"), prepared);
            assert!(matches!(decode(bytes.as_ref(), IdempotencyKey::new().operation_id()).expect_err("operation is exact").semantic_error(),
                LazyWorkspaceError::WriteInputConflict { .. }));
            let mut wrong_snapshot = bytes.as_ref().to_vec();
            wrong_snapshot[LAZY_WRITE_ORIGINAL_DOMAIN.len() + 2 + 16] ^= 1;
            assert!(matches!(decode(&wrong_snapshot, operation).expect_err("snapshot ID is genuine captured evidence").semantic_error(),
                LazyWorkspaceError::WriteInputConflict { .. }));
            let mut trailing = bytes.as_ref().to_vec();
            trailing.push(0);
            assert!(decode(&trailing, operation).is_err(), "strict framing rejects trailing or over-bound input");
            drop(trailing);
            drop(wrong_snapshot);
            drop(bytes);
            drop(scope);
        }
    }

    #[tokio::test]
    async fn original_whole_write_recovers_after_a_later_frozen_write_replaces_latest_hint() {
        fn original_mutations() -> [LazyWriteMutation<'static>; 6] {
            [
                LazyWriteMutation::CreateDirectory { path: "/bundle" },
                LazyWriteMutation::PutFile { path: "/bundle/data", bytes: acyclic_stream::OwnedBytes::from_static(b"data") },
                LazyWriteMutation::PutFile { path: "/bundle/descriptor", bytes: acyclic_stream::OwnedBytes::from_static(b"descriptor") },
                LazyWriteMutation::PutFile { path: "/bundle/receipt", bytes: acyclic_stream::OwnedBytes::from_static(b"receipt") },
                LazyWriteMutation::PutFile { path: "/target", bytes: acyclic_stream::OwnedBytes::from_static(b"first") },
                LazyWriteMutation::PutFile { path: "/bundle/extra", bytes: acyclic_stream::OwnedBytes::from_static(b"extra") },
            ]
        }
        let simulation = crate::simulation::Simulation::default();
        let (authority, objects) = simulation.stores();
        let fs = Fs::new(authority, objects, crate::EmbeddedCapabilities::MEMORY);
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let store = MemoryLazyWorkspaceStore::default();
        let root = LazyWorkspace::attach(&fs, "historical-frozen-whole-write", Arc::clone(&source), store.clone())
            .await.expect("original SDK stores");
        root.write("/target", Bytes::from_static(b"before")).await.expect("preimage");
        let basis = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("authentic first basis");
        let token = CancellationToken::new();
        let first_key = IdempotencyKey::new();
        let second_key = IdempotencyKey::new();
        let mut owner = || async { Ok::<(), LazyWorkspaceError>(()) };
        let (first, first_proof) = root.write_at_snapshot_guarded(
            basis, original_mutations(), first_key, crate::PublicationPermit::Unrestricted, &token, &mut owner,
        ).await.expect("one real six-member DATA publication");
        let first = match first {
            TransactionCommit::Committed(generation) | TransactionCommit::AlreadyCommitted(generation) => generation,
            _ => panic!("first candidate must publish"),
        };
        let first_snapshot = first_proof.expect("actual captured first final roots");
        let (second, second_proof) = root.write_at_snapshot_guarded(
            first_snapshot,
            std::iter::once(LazyWriteMutation::PutFile { path: "/target", bytes: (Bytes::from_static(b"second")).into() }),
            second_key,
            crate::PublicationPermit::Unrestricted,
            &token,
            &mut owner,
        ).await.expect("later real frozen writer");
        let second = match second {
            TransactionCommit::Committed(generation) | TransactionCommit::AlreadyCommitted(generation) => generation,
            _ => panic!("second candidate must publish"),
        };
        second_proof.expect("genuine later final snapshot");
        let latest = store.load_lazy_workspace(root.workspace().id()).await.expect("same companion").expect("binding");
        assert_eq!(latest.completed_write.expect("single latest hint").prepared.operation, second_key.operation_id());
        assert!(latest.pending_write.is_none());
        let workspace = root.workspace().clone();
        drop(root);
        let reopened = LazyWorkspace::open(workspace, Arc::clone(&source), store.clone()).await
            .expect("cold original producer, without any operation registry");
        source.invalidate();
        reopened.rebind_source(&LazyTreapWork::new(finite_fixture_budget(), &token)).await.expect("later genuine source epoch");
        let current = store.load_lazy_workspace(reopened.workspace().id()).await.expect("current companion").expect("binding");
        let appends = simulation.trace().expect("actual trace").into_iter()
            .filter(|entry| entry.operation == crate::simulation::SimulationOperation::AuthorityAppend).count();
        let (original, proof) = reopened.original_write_snapshot_guarded(
            basis, original_mutations(), first_key, &token, &mut owner,
        ).await.expect("protected original DATA lookup").expect("original exists");
        assert_eq!(original.id(), first.id());
        assert_eq!(proof.expect("immutable original DATA C survives replacement"), first_snapshot);
        assert_ne!(original.id(), second.id(), "the current generation is not adopted");
        assert_eq!(original.read("/bundle/receipt", 64, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("original receipt").as_ref(), b"receipt");
        let (replayed, proof) = reopened.write_at_snapshot_guarded(
            basis, original_mutations(), first_key, crate::PublicationPermit::Unrestricted, &token, &mut owner,
        ).await.expect("whole-candidate replay never restages the old basis");
        assert!(matches!(&replayed, TransactionCommit::AlreadyCommitted(generation) if generation.id() == first.id()));
        assert_eq!(proof.expect("exact old captured snapshot"), first_snapshot);
        let mut changed = original_mutations();
        changed[3] = LazyWriteMutation::PutFile {
            path: "/bundle/receipt", bytes: acyclic_stream::OwnedBytes::from_static(b"wrong receipt"),
        };
        let (original, proof) = reopened.original_write_snapshot_guarded(
            basis, changed, first_key, &token, &mut owner,
        ).await.expect("a known original is never absence").expect("durable original");
        assert_eq!(original.id(), first.id());
        assert!(matches!(proof.expect_err("every whole-candidate member is bound").semantic_error(),
            LazyWorkspaceError::WriteInputConflict { operation } if *operation == first_key.operation_id()));
        let other_basis = reopened.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("genuine later source and authored roots");
        let uninspected = std::iter::from_fn(|| -> Option<LazyWriteMutation<'static>> {
            panic!("mismatched captured source must be rejected before input inspection");
        });
        let (original, proof) = reopened.original_write_snapshot_guarded(
            other_basis, uninspected, first_key, &token, &mut owner,
        ).await.expect("retain actual original on source mismatch").expect("original exists");
        assert_eq!(original.id(), first.id());
        assert!(matches!(proof.expect_err("other source is not original authority").semantic_error(),
            LazyWorkspaceError::WriteInputConflict { operation } if *operation == first_key.operation_id()));
        let after = store.load_lazy_workspace(reopened.workspace().id()).await.expect("unchanged current companion").expect("binding");
        assert_eq!((after.revision, after.source, after.overlay, after.shadows, after.completed_write, after.pending_write),
            (current.revision, current.source, current.overlay, current.shadows, current.completed_write, current.pending_write));
        assert_eq!(reopened.workspace().head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("later head retained").id(), second.id());
        assert_eq!(simulation.trace().expect("after genuine recovery").into_iter()
            .filter(|entry| entry.operation == crate::simulation::SimulationOperation::AuthorityAppend).count(), appends);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_original_write_recovers_immutable_data_evidence_after_latest_hint_replacement() {
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let store = MemoryLazyWorkspaceStore::default();
        let root = LazyWorkspace::attach(&Fs::memory(), "native-historical-original", Arc::clone(&source), store.clone())
            .await.expect("same stock SDK");
        root.write("/input", Bytes::from_static(b"funded")).await.expect("actual input");
        root.write("/target", Bytes::from_static(b"before")).await.expect("preimage");
        let basis = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("actual source snapshot");
        let token = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(finite_fixture_budget(), &token).expect("original ledger");
        let root = root.into_arc_with_native_work(&original).expect("funded actual root Arc");
        let bytes = root.read_at_snapshot_with_work(basis, "/input", 64, &original, &token).await.expect("intrinsic input");
        let first_key = IdempotencyKey::new();
        let second_key = IdempotencyKey::new();
        let owner_original = original.clone();
        let mut owner = move |work: &crate::NativeMaterializationWork| {
            assert!(work.shares_ledger(&owner_original));
            async { Ok::<(), LazyWorkspaceError>(()) }
        };
        let (first, proof) = root.write_at_snapshot_with_native_work_guarded(
            basis,
            std::iter::once(LazyWriteMutation::PutFile { path: "/target", bytes: (bytes.clone()).into() }),
            first_key,
            crate::PublicationPermit::Unrestricted,
            &original,
            &token,
            &mut owner,
        ).await.expect("native original DATA");
        let first = match first {
            TransactionCommit::Committed(generation) | TransactionCommit::AlreadyCommitted(generation) => generation,
            _ => panic!("first native publication"),
        };
        let first_snapshot = proof.expect("genuine first snapshot");
        let (_, proof) = root.write_at_snapshot_with_native_work_guarded(
            first_snapshot,
            std::iter::once(LazyWriteMutation::PutFile { path: "/next", bytes: (bytes.clone()).into() }),
            second_key,
            crate::PublicationPermit::Unrestricted,
            &original,
            &token,
            &mut owner,
        ).await.expect("later native frozen DATA");
        proof.expect("later genuine proof");
        let current = store.load_lazy_workspace(root.workspace().id()).await.expect("actual companion").expect("binding");
        assert_eq!(current.completed_write.expect("later latest hint").prepared.operation, second_key.operation_id());
        let workspace = root.workspace().clone();
        drop(root);
        let reopened = LazyWorkspace::open(workspace, source, store.clone()).await.expect("cold same engine");
        let reopened = reopened.into_arc_with_native_work(&original).expect("funded actual reopened root Arc");
        let before = original.receipt();
        let (recovered, proof) = reopened.original_write_snapshot_with_native_work_guarded(
            basis, std::iter::once(LazyWriteMutation::PutFile { path: "/target", bytes: bytes.clone() }),
            first_key, &original, &token, &mut owner,
        ).await.expect("same original native callbacks and borrowed DATA C").expect("durable first operation");
        assert_eq!(recovered.id(), first.id());
        assert_eq!(proof.expect("genuine historical proof"), first_snapshot);
        assert!(original.receipt().bytes_hashed > before.bytes_hashed, "real immutable envelope and whole input were hashed");
        let after = store.load_lazy_workspace(reopened.workspace().id()).await.expect("unchanged companion").expect("binding");
        assert_eq!((after.revision, after.completed_write, after.pending_write),
            (current.revision, current.completed_write, current.pending_write));
        assert_eq!(reopened.workspace().read("/next", 64, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("later edit preserved").as_ref(), b"funded");
    }

    #[tokio::test]
    async fn frozen_writer_actual_lost_data_ack_reopens_without_relaunching_old_source() {
        let simulation = crate::simulation::Simulation::default();
        let (authority, objects) = simulation.stores();
        let fs = Fs::new(authority, objects, crate::EmbeddedCapabilities::MEMORY);
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let store = MemoryLazyWorkspaceStore::default();
        let root = LazyWorkspace::attach(
            &fs, "actual-lost-write-ack", Arc::clone(&source), store.clone(),
        ).await.expect("same canonical simulated stock stores");
        root.write("/target", Bytes::from_static(b"before")).await.expect("preimage");
        let basis = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("original authentic source capture");
        let key = IdempotencyKey::new();
        let token = CancellationToken::new();
        simulation.schedule_next(
            crate::simulation::SimulationOperation::AuthorityAppend,
            crate::simulation::SimulationFault::AmbiguousAuthorityAppend,
        ).expect("genuine postappend lost acknowledgement");
        let mut owner = || async { Ok::<(), LazyWorkspaceError>(()) };
        root.write_at_snapshot_guarded(
            basis,
            std::iter::once(LazyWriteMutation::PutFile { path: "/target", bytes: (Bytes::from_static(b"published")).into() }),
            key,
            crate::PublicationPermit::Unrestricted,
            &token,
            &mut owner,
        ).await.err().expect("the real DATA append succeeds but its ACK is lost");
        let original = root.workspace().operation_generation(key, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
            .expect("protected original authority lookup").expect("DATA actually durable");
        assert_eq!(original.read("/target", 64, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("actual published bytes").as_ref(), b"published");
        let pending = store.load_lazy_workspace(root.workspace().id()).await
            .expect("same companion").expect("binding").pending_write.expect("original prepared evidence");
        assert_eq!(pending.operation, key.operation_id());
        let workspace = root.workspace().clone();
        drop(root);
        let reopened = LazyWorkspace::open(workspace, Arc::clone(&source), store.clone()).await
            .expect("cold actual SDK producer");
        reopened.workspace().write("/concurrent", Bytes::from_static(b"later")).await
            .expect("independent later publication");
        source.invalidate();
        reopened.rebind_source(&LazyTreapWork::new(finite_fixture_budget(), &token)).await
            .expect("actual current source changes");
        let current = reopened.workspace().head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("later head").id();
        let appends = simulation.trace().expect("actual bounded trace").into_iter()
            .filter(|entry| entry.operation == crate::simulation::SimulationOperation::AuthorityAppend).count();
        let (commit, proof) = reopened.write_at_snapshot_guarded(
            basis,
            std::iter::once(LazyWriteMutation::PutFile { path: "/target", bytes: (Bytes::from_static(b"published")).into() }),
            key,
            crate::PublicationPermit::Unrestricted,
            &token,
            &mut owner,
        ).await.expect("same original candidate reconciles before stale-basis staging");
        assert!(matches!(&commit, TransactionCommit::AlreadyCommitted(generation) if generation.id() == original.id()));
        assert_eq!(proof.expect("real original companion settlement").authored_generation, original.id());
        assert_eq!(reopened.workspace().head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("never publish another batch").id(), current);
        assert_eq!(simulation.trace().expect("trace after recovery").into_iter()
            .filter(|entry| entry.operation == crate::simulation::SimulationOperation::AuthorityAppend).count(), appends);
        assert_eq!(reopened.workspace().read("/concurrent", 64, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("later data preserved").as_ref(), b"later");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn original_writer_postdata_owner_denial_keeps_real_generation_and_pending_custody() {
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let store = MemoryLazyWorkspaceStore::default();
        let root = LazyWorkspace::attach(
            &Fs::memory(), "postdata-owner-denial", Arc::clone(&source), store.clone(),
        ).await.expect("actual stock SDK root");
        root.write("/target", Bytes::from_static(b"before")).await.expect("authored target");
        root.write("/input", Bytes::from_static(b"published")).await.expect("real source bytes");
        let basis = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("actual source capture");
        let token = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(finite_fixture_budget(), &token)
            .expect("genuine original ledger");
        let root = root.into_arc_with_native_work(&original).expect("funded actual root Arc");
        let bytes = root.read_at_snapshot_with_work(basis, "/input", 64, &original, &token).await
            .expect("SDK intrinsic input before writer allocation");
        let key = IdempotencyKey::new();
        let calls = Arc::new((AtomicUsize::new(0), original.claim_allocation(
            std::mem::size_of::<(AtomicUsize, crate::materializer::NativeAllocation)>() as u64, 1, crate::WorkCounters::UNCHARGED).expect("funded owner counter")));
        let owner_calls = Arc::clone(&calls);
        let owner_original = original.clone();
        let mut owner = move |work: &crate::NativeMaterializationWork| {
            assert!(work.shares_ledger(&owner_original));
            let index = owner_calls.0.fetch_add(1, Ordering::Relaxed);
            async move {
                if index == 7 { Err(LazyWorkspaceError::Filesystem(crate::FsError::MutationNotAllowed)) }
                else { Ok(()) }
            }
        };
        let (commit, proof) = root.write_at_snapshot_with_native_work_guarded(
            basis,
            std::iter::once(LazyWriteMutation::PutFile { path: "/target", bytes: (bytes.clone()).into() }),
            key,
            crate::PublicationPermit::Unrestricted,
            &original,
            &token,
            &mut owner,
        ).await.expect("known DATA generation survives a genuine postpublication owner denial");
        let generation = match commit {
            TransactionCommit::Committed(generation) | TransactionCommit::AlreadyCommitted(generation) => generation,
            _ => panic!("actual canonical DATA published"),
        };
        assert!(matches!(proof.expect_err("actual companion owner denies").semantic_error(),
            LazyWorkspaceError::Filesystem(crate::FsError::MutationNotAllowed)));
        assert_eq!(calls.0.load(Ordering::Relaxed), 8);
        let pending = store.load_lazy_workspace(root.workspace().id()).await
            .expect("real companion").expect("binding").pending_write.expect("actual pending witness");
        assert_eq!(pending.basis, basis);
        let before = original.receipt();
        let mut recovery_owner = |_: &crate::NativeMaterializationWork| async { Ok::<(), LazyWorkspaceError>(()) };
        let (actual, proof) = root.original_write_snapshot_with_native_work_guarded(
            basis, std::iter::once(LazyWriteMutation::PutFile { path: "/target", bytes: bytes.clone() }),
            key, &original, &token, &mut recovery_owner,
        ).await.expect("same original owner recovery").expect("actual original DATA");
        assert_eq!(actual.id(), generation.id());
        assert_eq!(proof.expect("genuine companion settlement").authored_generation, generation.id());
        assert!(original.receipt().bytes_hashed > before.bytes_hashed);
        assert!(store.load_lazy_workspace(root.workspace().id()).await.expect("settled").expect("binding").pending_write.is_none());
        assert_eq!(generation.read("/target", 64, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("actual bytes preserved").as_ref(), b"published");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn original_frozen_writer_denies_quota_cancellation_and_foreign_input_before_effect() {
        for mode in 0..3 {
            let root = LazyWorkspace::attach(
                &Fs::memory(), "original-write-denial",
                Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
                MemoryLazyWorkspaceStore::default(),
            ).await.expect("actual workspace");
            root.write("/target", Bytes::from_static(b"before")).await.expect("target");
            let basis = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("authentic basis");
            let token = CancellationToken::new();
            let original = crate::NativeMaterializationWork::new(finite_fixture_budget(), &token)
                .expect("original admission");
            let root = root.into_arc_with_native_work(&original).expect("funded actual root Arc");
            let calls = Arc::new((AtomicUsize::new(0), original.claim_allocation(
                std::mem::size_of::<(AtomicUsize, crate::materializer::NativeAllocation)>() as u64, 1, crate::WorkCounters::UNCHARGED).expect("funded owner counter")));
            let owner_calls = Arc::clone(&calls);
            let bytes = root.read_at_snapshot_with_work(
                basis, "/target", 64, &original, &token,
            ).await.expect("actual funded bytes");
            let view = if mode == 2 {
                crate::NativeMaterializationWork::new(finite_fixture_budget(), &token)
                    .expect("genuinely different input ledger")
            } else {
                let mut quota = finite_fixture_budget();
                if mode == 0 { quota.allocation_operations = 0; }
                original.native_volume_work(quota).expect("same-original resource view")
            };
            if mode == 1 { token.cancel(); }
            let retained_input = bytes.clone();
            let before = original.receipt();
            let mut owner = move |_: &crate::NativeMaterializationWork| {
                owner_calls.0.fetch_add(1, Ordering::Relaxed);
                async { Ok::<(), LazyWorkspaceError>(()) }
            };
            let failure = root.write_at_snapshot_with_native_work_guarded(
            basis,
            std::iter::once(LazyWriteMutation::PutFile { path: "/target", bytes: (bytes).into() }),
            IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted,
            &view,
            &token,
            &mut owner,
        ).await.err().expect("original native preeffect denial");
            if mode == 2 {
                assert!(matches!(failure.semantic_error(), LazyWorkspaceError::NativeLedgerMismatch));
            }
            assert_eq!(calls.0.load(Ordering::Relaxed), 0);
            assert_eq!(original.receipt(), before, "no fabricated native receipt or rebill");
            assert_eq!(root.workspace().head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("unchanged head").id(), basis.authored_generation);
            assert_eq!(root.read("/target", 64).await.expect("target retained").as_ref(), b"before");
            drop(retained_input);
        }
    }

    #[cfg(all(feature = "native-mount", unix, not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn original_native_link_reads_exact_opaque_target_without_following() {
        use std::os::unix::ffi::OsStringExt as _;
        let directory = tempfile::tempdir().expect("native source");
        let target = b"../missing-\xff";
        std::os::unix::fs::symlink(std::ffi::OsString::from_vec(target.to_vec()),
            directory.path().join("link")).expect("opaque dangling link");
        acyclic_native_runtime::initialize_native_io_services().expect("native fixture services");
        let token = CancellationToken::new();
        let original = crate::materializer::NativeMaterializationWork::new(finite_fixture_budget(), &token)
            .expect("original admitted work state");
        let source = Arc::new(crate::demand::native::NativeDemandSource::open_with_reference_and_native_work(
            directory.path(), crate::model::FilesystemProfile::Portable, crate::model::VolumeLimits::default(),
            SourceReference { identity: [0x57; 16], epoch: 0 }, None, &original,
        ).await.expect("same-original native source"));
        let root = LazyWorkspace::attach(
            &Fs::memory(), "original-native-link", source, MemoryLazyWorkspaceStore::default(),
        ).await.expect("attach");
        let reference = root.source.reference();
        let namespace = root.namespace_path("/link").expect("fixture namespace");
        let node = root.source.lookup(reference, &namespace, WorkBudget::UNBOUNDED, &token).await
            .expect("fixture metadata").value.expect("fixture link");
        drop(namespace);
        let (bytes, metadata) = root.read_source_link(
            "/link", reference, node.version, target.len() as u64, &original, &token,
        ).await.expect("same original link producer");
        assert_eq!(bytes.bytes.as_ref(), target);
        assert!(bytes.is_intrinsic());
        assert!(bytes.logical_owned_bytes >= target.len() as u64);
        assert_eq!(original.receipt().source_bytes_read, target.len() as u64);
        assert_eq!(metadata.bytes(), 0);
        drop(bytes);
        drop(metadata);
        token.cancel();
        let (error, custody) = root.read_source_link(
            "/link", reference, node.version, target.len() as u64, &original, &token,
        ).await.expect_err("original caller cancellation");
        assert!(matches!(error.semantic_error(), LazyWorkspaceError::NativeWork(
            crate::materializer::NativeTreeMaterializationError::Cancelled)));
        assert!(custody.is_none());
        assert_eq!(original.receipt().source_bytes_read, target.len() as u64);
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
        let snapshot = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("freeze");
        let budget = finite_fixture_budget();
        let token = CancellationToken::new();
        let source = root.list_directory_at_snapshot(snapshot, "/", None, 1, budget, &token, None)
            .await.expect("source page");
        let authored = root.list_directory_at_snapshot(snapshot, "/", source.value.next, 1, budget, &token, None)
            .await.expect("first authored page");
        let cursor = authored.value.next.expect("authored continuation");
        let encoded = cursor.encode_measured(budget, &token).expect("bounded canonical encoding");
        encoded.work.verify(budget).expect("bounded encoding work");
        let decoded = LazyDirectoryCursor::decode_measured(
            &encoded.value, root.workspace().profile(), root.workspace().limits(), budget, &token,
        ).expect("bounded canonical decoding");
        decoded.work.verify(budget).expect("bounded decoding work");
        assert_eq!(decoded.value, cursor);
        let next = root.list_directory_at_snapshot(snapshot, "/", Some(decoded.value), 1, budget, &token, None)
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
            directory.path(), crate::model::FilesystemProfile::Portable, crate::model::VolumeLimits::default(), None, None,
        ).await.expect("native source"));
        let root = LazyWorkspace::attach(
            &Fs::memory(), "frozen-native-cursor-codec", source, MemoryLazyWorkspaceStore::default(),
        ).await.expect("attach");
        let snapshot = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("freeze");
        let budget = finite_fixture_budget();
        let token = CancellationToken::new();
        let mut cursor = None;
        let mut names = Vec::new();
        loop {
            let page = root.list_directory_at_snapshot(snapshot, "/", cursor, 1, budget, &token, None)
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
        assert_eq!(root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("no observation or authored publication"), snapshot);
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn frozen_native_absence_query_marks_actual_source_participation() {
        let directory = tempfile::tempdir().expect("empty native source");
        let source = Arc::new(crate::demand::native::NativeDemandSource::open(
            directory.path(), crate::model::FilesystemProfile::Portable, crate::model::VolumeLimits::default(), None, None,
        ).await.expect("native source"));
        let root = LazyWorkspace::attach(
            &Fs::memory(), "frozen-native-absence", source, MemoryLazyWorkspaceStore::default(),
        ).await.expect("attach");
        let snapshot = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("freeze");
        let state = root.binding().await.expect("frozen companion state");
        let source_used = AtomicBool::new(false);
        let failure = root.inspect_with("/absent", InspectBasis {
            state: Some(&state),
            authored_generation: Some(snapshot.authored_generation),
            retain_pinned_observation: true,
            source_used: Some(&source_used),
            ..InspectBasis::default()
        }, finite_fixture_budget(), &CancellationToken::new()).await
            .err().expect("actual native absence");
        assert!(matches!(failure.semantic_error(), LazyWorkspaceError::NotFound));
        assert!(source_used.load(Ordering::Acquire));
        assert_eq!(root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("no absence observation publication"), snapshot);
    }

    #[tokio::test]
    async fn frozen_symbolic_link_read_uses_original_authored_target() {
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &Fs::memory(), "frozen-authored-link", source, MemoryLazyWorkspaceStore::default(),
        ).await.expect("attach");
        let mut original = root.workspace().begin_transaction(IdempotencyKey::from_bytes([0x75; 16]), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("original transaction");
        original.create_symbolic_link("/link", Bytes::from_static(b"original"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("original target");
        original.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("publish original link");
        let snapshot = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("freeze original target");
        let mut advanced = root.workspace().begin_transaction(IdempotencyKey::from_bytes([0x76; 16]), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("advance transaction");
        advanced.remove("/link", WorkBudget::UNBOUNDED, &CancellationToken::new()).await.expect("remove original binding");
        advanced.create_symbolic_link("/link", Bytes::from_static(b"advanced"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("new target");
        advanced.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("publish advanced target");
        let current = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("advanced current");
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
        assert_eq!(root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("readonly state unchanged"), current);
    }

    #[cfg(all(feature = "native-watch", unix, not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn frozen_native_symbolic_link_read_never_follows_or_hydrates() {
        let directory = tempfile::tempdir().expect("native source");
        std::os::unix::fs::symlink("missing-target", directory.path().join("link")).expect("dangling link");
        let source = Arc::new(crate::demand::native::NativeDemandSource::open(
            directory.path(), crate::model::FilesystemProfile::Portable, crate::model::VolumeLimits::default(), None, None,
        ).await.expect("native source"));
        let root = LazyWorkspace::attach(
            &Fs::memory(), "frozen-native-link", source, MemoryLazyWorkspaceStore::default(),
        ).await.expect("attach");
        let snapshot = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("freeze unresolved link");
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
        assert_eq!(root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("no hydration or observation publication"), snapshot);
    }

    #[tokio::test]
    async fn frozen_readonly_snapshot_retains_authored_overlay_and_unresolved_source() {
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &Fs::memory(), "frozen-readonly", Arc::clone(&source), MemoryLazyWorkspaceStore::default(),
        ).await.expect("attach");
        root.write("/authored.txt", Bytes::from_static(b"original")).await.expect("original authored");
        root.remove("/hidden.txt").await.expect("original tombstone");
        let snapshot = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("original frozen reference");
        root.write("/authored.txt", Bytes::from_static(b"advanced")).await.expect("advance authored");
        root.remove("/file.txt").await.expect("advance overlay");
        let current = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("advanced state");
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
        let source_page = root.list_directory_at_snapshot(snapshot, "/", None, 16, budget, &token, None)
            .await.expect("original unresolved source page");
        assert_eq!(source_page.value.entries.len(), 1);
        assert_eq!(source_page.value.entries[0].name.as_bytes(), b"file.txt");
        assert!(!source_page.value.entries[0].authored);
        source_page.work.verify(budget).expect("bounded source page");
        let changed_generation = LazySnapshotRef::new(
            snapshot.workspace_id, current.authored_generation, snapshot.source, snapshot.overlay, snapshot.shadows,
        );
        let wrong_cursor = root.list_directory_at_snapshot(
            changed_generation, "/", source_page.value.next.clone(), 16, budget, &token, None,
        ).await.expect_err("continuation belongs to exact original snapshot");
        assert!(matches!(wrong_cursor.error.semantic_error(), LazyWorkspaceError::StaleCursor));
        let authored_page = root.list_directory_at_snapshot(
            snapshot, "/", source_page.value.next, 16, budget, &token, None,
        ).await.expect("original authored page");
        assert_eq!(authored_page.value.entries.len(), 1);
        assert_eq!(authored_page.value.entries[0].name.as_bytes(), b"authored.txt");
        assert!(authored_page.value.entries[0].authored);
        authored_page.work.verify(budget).expect("bounded authored page");
        assert_eq!(root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("readonly state unchanged"), current);
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
        let authored_snapshot = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("freeze authored generation");
        let budget = finite_fixture_budget();
        let token = CancellationToken::new();
        let source_used = AtomicBool::new(false);
        let source_page = root.list_directory_at_snapshot(
            authored_snapshot, "/", None, 16, budget, &token, Some(&source_used),
        ).await.expect("original source phase");
        assert!(source_used.load(Ordering::Acquire));
        let authored_cursor = source_page.value.next.expect("held original authored phase");
        root.remove("/a").await.expect("retain detached identity shadow");
        let uncaptured = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("freeze before capturing source alias");
        let state = root.binding().await.expect("original frozen companion state");
        let source_used = AtomicBool::new(false);
        let resolved = root.inspect_with("/b", InspectBasis {
            state: Some(&state),
            authored_generation: Some(uncaptured.authored_generation),
            retain_pinned_observation: true,
            source_used: Some(&source_used),
            ..InspectBasis::default()
        }, budget, &token).await.expect("native identity query resolving detached shadow");
        assert!(matches!(resolved.value.lookup, LazyLookup::Shadow { .. }));
        assert!(source_used.load(Ordering::Acquire));
        assert!(matches!(root.lookup("/b").await.expect("capture alias identity"), LazyLookup::Shadow { .. }));
        let shadow_snapshot = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("freeze captured shadow");
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
        let source_used = AtomicBool::new(false);
        let authored_page = root.list_directory_at_snapshot(
            authored_snapshot, "/", Some(authored_cursor), 16, budget, &token, Some(&source_used),
        ).await.expect("held authored-only continuation has no source participation");
        assert!(!source_used.load(Ordering::Acquire));
        assert_eq!(authored_page.value.entries[0].name.as_bytes(), b"a");
        assert!(authored_page.value.entries[0].authored);
        let failure = root.read_at_snapshot_measured(shadow_snapshot, "/unresolved", 64, budget, &token)
            .await.expect_err("unobserved historical source still fails closed");
        assert!(matches!(failure.error.semantic_error(), LazyWorkspaceError::StaleSource));
        let failure = root.list_directory_at_snapshot(authored_snapshot, "/", None, 16, budget, &token, None)
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
        let snapshot = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("freeze");
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
        let identity_failure = root.validate_snapshot(replaced_identity, &LazyTreapWork::new(budget, &token))
            .await.expect_err("original physical source identity differs");
        assert!(matches!(identity_failure.semantic_error(), LazyWorkspaceError::StaleSource));
        source.invalidate();
        root.validate_snapshot(snapshot, &LazyTreapWork::new(budget, &token))
            .await.expect("original immutable metadata survives live epoch advancement");
        let missing_root = LazySnapshotRef::new(
            snapshot.workspace_id, snapshot.authored_generation, snapshot.source,
            LazyOverlayId::from_bytes([0x72; 32]), snapshot.shadows,
        );
        let invalid = root.validate_snapshot(missing_root, &LazyTreapWork::new(budget, &token))
            .await.expect_err("digest alone cannot authenticate absent overlay root");
        assert!(matches!(invalid.semantic_error(), LazyWorkspaceError::Store(_)));
        let missing_shadow = LazySnapshotRef::new(
            snapshot.workspace_id, snapshot.authored_generation, snapshot.source,
            snapshot.overlay, LazyShadowId([0x74; 32]),
        );
        let invalid = root.validate_snapshot(missing_shadow, &LazyTreapWork::new(budget, &token))
            .await.expect_err("digest alone cannot authenticate absent shadow root");
        assert!(matches!(invalid.semantic_error(), LazyWorkspaceError::Store(_)));
        let failure = root.read_at_snapshot_measured(snapshot, "/file.txt", 64, budget, &token)
            .await.expect_err("live epoch differs");
        assert!(matches!(failure.error.semantic_error(), LazyWorkspaceError::StaleSource));
        let failure = root.list_directory_at_snapshot(snapshot, "/", None, 16, budget, &token, None)
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
        let snapshot = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("freeze");
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
        let page = root.list_directory_at_snapshot(snapshot, "/", None, 16, budget, &token, None)
            .await.expect("baseline page");
        let mut denied = budget;
        denied.peak_allocation_bytes = page.work.peak_allocation_bytes - 1;
        let failed = root.list_directory_at_snapshot(snapshot, "/", None, 16, denied, &token, None)
            .await.expect_err("deny finite page allocation");
        assert!(matches!(failed.error.semantic_error(), LazyWorkspaceError::Work(_)));
        assert!(failed.work.bytes_hashed > 0);
        assert!(failed.work.allocation_operations > 0);
        failed.work.verify(denied).expect("admitted failed list prefix");
        assert_eq!(root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("no readonly observation publication"), snapshot);
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
        let before = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("snapshot before rebind");
        source.invalidate();
        let rebound = root.rebind_source(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("ordinary rebind uses ordinary funding") }).expect("rebind");
        assert_eq!(rebound.source, source.reference());
        assert_eq!(source.counts.pages.load(Ordering::Relaxed), 0);
        assert_eq!(
            root.stat("/observed.txt")
                .await
                .expect("cached")
                .logical_bytes,
            Some(6)
        );
        let after = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("snapshot after refresh");
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
        let snapshot = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("snapshot before rebind");

        source.replace(Bytes::from_static(b"changed"));
        root.rebind_source(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("ordinary rebind uses ordinary funding") }).expect("rebind");
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

        assert_eq!((exact
            .value
            .read("/file.txt", 32, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
            .expect("unresolved file from live epoch")).as_ref(), b"changed");
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
        let snapshot = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("snapshot after observation");

        source.replace(Bytes::from_static(b"changed"));
        root.rebind_source(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("ordinary rebind uses ordinary funding") }).expect("rebind");
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

        assert_eq!((exact
            .value
            .read("/file.txt", 32, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
            .expect("observed file from pinned epoch")).as_ref(), b"source");
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
                VolumeLimits::default(), None, None,
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
        assert_eq!((root.read("/file.txt", 64).await.expect("initial read")).as_ref(), b"before");
        let before = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("snapshot before change");

        std::fs::write(&path, b"after").expect("external write");
        source.invalidate(&mut crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).expect("source invalidation");
        root.rebind_source(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("ordinary rebind uses ordinary funding") }).expect("rebind source");

        assert_eq!((root.read("/file.txt", 64).await.expect("refreshed read")).as_ref(), b"after");
        assert_ne!(
            root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("snapshot after change"),
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
                VolumeLimits::default(), None, None,
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
            root.exactify("/", insufficient, &CancellationToken::new(), crate::PublicationPermit::Unrestricted).await,
            Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::Work(_))
        ));

        let exact = root
            .exactify("/", WorkBudget::UNBOUNDED, &CancellationToken::new(), crate::PublicationPermit::Unrestricted)
            .await
            .expect("exact capture");
        assert_eq!((exact
            .value
            .read("/nested/b.txt", 16, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
            .expect("captured file")).as_ref(), b"beta");
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
        let failure = root.exactify(
            "/selected", budget, &CancellationToken::new(), crate::PublicationPermit::Unrestricted,
        ).await.err().expect("deny source lookup IO");
        assert!(matches!(failure.semantic_error(), LazyWorkspaceError::Work(_)));
        assert_eq!(failure.spent_work().backend_read_operations, 0);
        failure.spent_work().verify(budget).expect("preflight prefix");
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let failure = root.exactify(
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
                VolumeLimits::default(), None, None,
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
            .exactify(
                "/hot",
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
                crate::PublicationPermit::Unrestricted,
            )
            .await
            .expect("capture hot subtree");
        assert_eq!((exact
            .value
            .read("/hot/file.txt", 16, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
            .expect("hot content")).as_ref(), b"hot");
        #[cfg(feature = "native-mount")]
        {
            let staged = tempfile::tempdir().expect("empty native stage");
            exact
                .value
                .materialize(
                    std::iter::once("/hot"),
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
            exact.value.read("/cold/file.txt", 16, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error),
            Err(WorkspaceError::NotFound)
        ));
        assert!(matches!(
            root.lookup("/cold/file.txt")
                .await
                .expect("cold remains lazy"),
            LazyLookup::Source(_)
        ));
        assert!(matches!(
            root.exactify(
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
        std::fs::write(directory.path().join("cafÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¾ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â¦ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â©.txt"), b"source").expect("source file");
        let source = Arc::new(
            NativeDemandSource::open(
                directory.path(),
                FilesystemProfile::Windows,
                VolumeLimits::default(), None, None,
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
            .exactify("/", WorkBudget::UNBOUNDED, &CancellationToken::new(), crate::PublicationPermit::Unrestricted)
            .await
            .expect("exactify Windows source into portable authored workspace");
        assert_eq!((exact
            .value
            .read("/cafÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¾ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â¦ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â©.txt", 16, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
            .expect("captured file")).as_ref(), b"source");
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
                VolumeLimits::default(), None, None,
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
            .exactify("/", WorkBudget::UNBOUNDED, &CancellationToken::new(), crate::PublicationPermit::Unrestricted)
            .await
            .expect("exactify all batches");
        let first = exact.value.stat("/a000", WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("first stat");
        let last = exact.value.stat("/a519", WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("last stat");
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
            .list_directory("/", None, 16, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await.map(|receipt| receipt.value)
            .expect("source phase");
        assert!(source_page.entries.is_empty());
        let authored_page = root
            .list_directory("/", source_page.next, 16, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await.map(|receipt| receipt.value)
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
            .list_directory("/", None, 16, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await.map(|receipt| receipt.value)
            .expect("first page");
        root.stat("/newly-observed.txt").await.expect("observe");

        assert!(matches!(
            root.list_directory("/", first.next, 16, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value),
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
        let before = root.inspect_state(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("ordinary inspection uses ordinary funding") }).expect("state");

        assert!(matches!(
            root.remove("/directory").await,
            Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::Workspace(_))
        ));
        let after = root.inspect_state(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("ordinary inspection uses ordinary funding") }).expect("state after failure");
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
        let prior = root.inspect_state(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("ordinary inspection uses ordinary funding") }).expect("state");
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
            fs.open_workspace("root", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("workspace"),
            Arc::clone(&source),
            store.clone(),
        )
        .await
        .expect("recover before physical remove");
        let recovered = reopened.inspect_state(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("ordinary inspection uses ordinary funding") }).expect("recovered state");
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
            .workspace().begin_transaction(committed_key, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
            .expect("removal transaction");
        removal.remove("/file.txt", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.expect("stage remove");
        assert!(matches!(
            removal.commit(None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(OperationReceipt { value: (), work: WorkCounters::UNCHARGED }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("physical remove"),
            TransactionCommit::Committed(_)
        ));

        let finalized = LazyWorkspace::open(
            fs.open_workspace("root", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("workspace"),
            Arc::clone(&source),
            store,
        )
        .await
        .expect("recover after physical remove");
        let final_state = finalized.inspect_state(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("ordinary inspection uses ordinary funding") }).expect("final state");
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
        let prior = root.inspect_state(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("ordinary inspection uses ordinary funding") }).expect("state");
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
            fs.open_workspace("root", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("workspace"),
            Arc::clone(&source),
            store,
        )
        .await
        .expect("recover source removal");
        assert_eq!(reopened.inspect_state(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("ordinary inspection uses ordinary funding") }).expect("state").overlay, tombstone);
        assert_eq!((reopened
            .read("/file.txt", 64)
            .await
            .expect("authored replacement")).as_ref(), b"authored");
        reopened.remove("/file.txt").await.expect("remove authored");
        assert!(matches!(
            reopened.stat("/file.txt").await,
            Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::NotFound)
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
            pending_write: None, completed_write: None, completed_selected_import: None,
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
        let parent = root.inspect_state(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("ordinary inspection uses ordinary funding") }).expect("parent binding");
        let snapshot = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("original snapshot");
        let prepared = memory_prepared_origin(
            snapshot, root.workspace().volume_config(), WorkspaceId::from_bytes([0xc1; 16]),
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
        let refreshed = memory_prepared_origin(refreshed_snapshot, root.workspace().volume_config(), prepared.workspace_id);
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
        let parent = root.inspect_state(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.map(|phase| match phase { LazyReadonlyNode::Portable(state) => state, #[cfg(not(target_arch = "wasm32"))] LazyReadonlyNode::Native(_) => unreachable!("ordinary inspection uses ordinary funding") }).expect("parent state");
        let snapshot = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("original snapshot");
        let child_workspace = fs.create_workspace("prepared-origin-child", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("child workspace");
        let prepared = memory_prepared_origin(snapshot, root.workspace().volume_config(), child_workspace.id());
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
            prepared.workspace_id, child_workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("head").id(),
            prepared.source, prepared.overlay, prepared.shadows,
        );
        let descendant = memory_prepared_origin(
            descendant_snapshot, child_workspace.volume_config(), WorkspaceId::from_bytes([0xc5; 16]),
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

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_publication_qualification_codec_preserves_whole_cut_and_historical_permit() {
        let root = LazyWorkspace::attach(
            &Fs::memory(), "native-publication-qualification",
            Arc::new(CountingSource::new(Bytes::from_static(b"source"))), MemoryLazyWorkspaceStore::default(),
        ).await.expect("actual SDK source");
        let basis = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("actual immutable SDK cut");
        let operation = IdempotencyKey::new().operation_id();
        let contexts = [
            NativeSnapshotContext::Authored {
                workspace_id: basis.workspace_id, config: root.workspace().volume_config(),
            },
            NativeSnapshotContext::Lazy(LazyPublicationCut {
                workspace_id: basis.workspace_id, config: root.workspace().volume_config(),
                source: basis.source, overlay: basis.overlay, shadows: basis.shadows,
            }),
        ];
        let permits = [
            crate::PublicationPermit::Unrestricted,
            crate::PublicationPermit::Lease {
                authority_id: root.workspace().authority_id().into_bytes(),
                workspace_id: basis.workspace_id.into_bytes(), lease_id: [2; 16], expires_at_millis: 123,
            },
            crate::PublicationPermit::Reservation {
                operation_id: [3; 16], gate_tail: 7,
                expected: crate::Head {
                    epoch: crate::Epoch::new(3).expect("historical epoch"),
                    sequence: crate::Sequence::new(4), digest: basis.authored_generation.digest(),
                },
            },
        ];
        let token = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(finite_fixture_budget(), &token).expect("original ledger");
        let prefix = original.live_allocation_bytes();
        for qualification in contexts {
            for historical_permit in permits {
                let (context, bytes) = native_publication_evidence_with_native_work(
                    qualification, operation, historical_permit, &original, &token,
                ).expect("funded canonical C and control");
                assert!(bytes.matches_meter(&original.runtime_meter_owner()));
                assert!(bytes.len() <= NATIVE_PUBLICATION_MAX_BYTES);
                let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &token);
                assert_eq!(native_publication_decode_with_accounting(
                    bytes.as_ref(), operation, &mut accounting,
                ).expect("strict whole-cut decoder"), (qualification, historical_permit));
                assert_eq!(native_publication_context_with_accounting(bytes.as_ref(), &mut accounting)
                    .expect("exact retained C"), context);
                assert!(native_publication_decode_with_accounting(
                    bytes.as_ref(), IdempotencyKey::new().operation_id(), &mut accounting,
                ).is_err(), "a task or permit operation cannot replace actual DATA");
                let mut trailing = bytes.as_ref().to_vec();
                trailing.push(0);
                assert!(native_publication_decode_with_accounting(&trailing, operation, &mut accounting).is_err());
                let mut wrong_domain = bytes.as_ref().to_vec();
                wrong_domain[0] ^= 1;
                assert!(native_publication_decode_with_accounting(&wrong_domain, operation, &mut accounting).is_err());
                let issued = NativePublicationSnapshot::published(qualification, operation, basis.authored_generation);
                let serialized = serde_json::to_vec(&issued).expect("opaque fixed metadata");
                assert_eq!(serde_json::from_slice::<NativePublicationSnapshot>(&serialized).expect("data roundtrip"), issued);
                assert_eq!(issued.snapshot().lazy_snapshot_with_native_work(&original, &token).expect("pure projection"),
                    if matches!(qualification, NativeSnapshotContext::Lazy(_)) { Some(basis) } else { None });
                drop(bytes);
                assert_eq!(original.live_allocation_bytes(), prefix, "encoded C drops before original custody");
            }
        }
        let denied = original.native_volume_work(WorkBudget {
            bytes_encoded: 0, ..WorkBudget::UNBOUNDED
        }).expect("same original scoped quota");
        assert!(native_publication_evidence_with_native_work(contexts[0], operation, permits[0], &denied, &token).is_err());
        assert_eq!(original.live_allocation_bytes(), prefix);
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let before = original.receipt();
        assert!(native_publication_evidence_with_native_work(contexts[1], operation, permits[0], &original, &cancelled).is_err());
        assert_eq!(original.receipt(), before, "cancellation before allocation or hashing");
        assert!(NativePublicationSnapshot::unchanged(contexts[0], basis.authored_generation)
            .snapshot().lazy_snapshot_with_native_work(&original, &cancelled).is_err());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_original_data_qualification_never_adopts_later_head_or_source() {
        let simulation = crate::simulation::Simulation::default();
        let (authority, objects) = simulation.stores();
        let fs = Fs::new(authority, objects, crate::EmbeddedCapabilities::MEMORY);
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let store = MemoryLazyWorkspaceStore::default();
        let root = LazyWorkspace::attach(&fs, "native-original-qualification", Arc::clone(&source), store.clone())
            .await.expect("same real canonical SDK engines");
        root.write("/input", Bytes::from_static(b"original")).await.expect("actual input");
        let basis = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("genuine SDK frozen basis");
        let cut = LazyPublicationCut {
            workspace_id: basis.workspace_id, config: root.workspace().volume_config(),
            source: basis.source, overlay: basis.overlay, shadows: basis.shadows,
        };
        let token = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(finite_fixture_budget(), &token).expect("original ledger");
        let bytes = root.read_at_snapshot_with_work(basis, "/input", 64, &original, &token)
            .await.expect("intrinsically funded native input");
        let generation = root.workspace().generation(basis.authored_generation, &original, &token)
            .await.expect("exact original generation, not a logical ID");
        let key = IdempotencyKey::new();
        let mut transaction = root.workspace().begin_pinned_transaction(
            &generation, key, &original, &token,
        ).await.expect("same existing canonical transaction");
        transaction.write("/published", bytes, &original, &token)
            .await.expect("same SDK staged mutation");
        let (context, evidence) = native_publication_evidence_with_native_work(
            NativeSnapshotContext::Lazy(cut), key.operation_id(), crate::PublicationPermit::Unrestricted,
            &original, &token,
        ).expect("complete frozen C before real publication");
        let mut owner = |_: &crate::NativeMaterializationWork| std::future::ready(Ok::<(), crate::FsError>(()));
        let published = transaction.commit_with_native_work(
            Some((context, evidence, crate::PublicationPermit::Unrestricted)),
            crate::PublicationPermit::Unrestricted, &original, &token, &mut owner,
        ).await.expect("actual canonical DATA CAS");
        let published = match published {
            TransactionCommit::Committed(generation) | TransactionCommit::AlreadyCommitted(generation) => generation,
            _ => panic!("real native DATA must publish"),
        };
        drop(transaction);
        root.workspace().write("/later", Bytes::from_static(b"later")).await.expect("independent later DATA");
        source.invalidate();
        root.rebind_source(&LazyTreapWork::new(finite_fixture_budget(), &token)).await.expect("later real source cut");
        let later = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("genuine later context");
        assert_ne!(later.authored_generation, published.id());
        assert_ne!(later.source, cut.source);
        let appends = simulation.trace().expect("real trace").into_iter()
            .filter(|entry| entry.operation == crate::simulation::SimulationOperation::AuthorityAppend).count();
        let companion = store.load_lazy_workspace(root.workspace().id()).await.expect("companion").expect("binding");
        let (observed, custody) = root.workspace().operation_generation_record_with_native_work(
            key.operation_id(), &original, &token,
        ).await.expect("original protected DATA and whole live provider custody");
        let (actual, record) = observed.expect("original operation exists");
        let allocation_count = original.receipt().allocation_operations;
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &token);
        let publication = crate::kernel::decode_publication_evidence_with_accounting(
            &record.payload, record.payload.len() as u64, &mut accounting,
        ).expect("real production canonical v2 decoder");
        assert_eq!(publication.publication.volume_id, root.workspace().id().volume_id());
        assert_eq!(record.operation_id, key.operation_id());
        let evidence = publication.original_evidence.expect("actual retained original C");
        let (retained_context, historical_permit) = native_publication_decode_with_accounting(
            evidence, key.operation_id(), &mut accounting,
        ).expect("real producer C is strict canonical data");
        assert_eq!(retained_context, NativeSnapshotContext::Lazy(cut));
        assert_eq!(historical_permit, crate::PublicationPermit::Unrestricted);
        let context = native_publication_context_with_accounting(evidence, &mut accounting).expect("actual retained C digest");
        let fingerprint_context = crate::kernel::permit_context_with_accounting(
            Some(context), historical_permit, &mut accounting,
        ).expect("original fingerprint permit is data").expect("qualified context");
        let request = crate::kernel::PublishGenerationRequest {
            authority_id: root.workspace().authority_id(), volume_id: root.workspace().id().volume_id(),
            epoch: record.epoch,
            expected: crate::Head {
                epoch: record.epoch, sequence: crate::Sequence::new(record.sequence.get() - 1),
                digest: record.previous_digest,
            },
            operation_id: key.operation_id(), generation_root: publication.publication.generation_root,
        };
        let fingerprint = crate::kernel::contextual_publication_fingerprint_from_payload_with_accounting(
            request, fingerprint_context, &record.payload, &mut accounting,
        ).expect("actual production whole-payload fingerprint");
        assert_eq!(fingerprint, record.fingerprint);
        let issued = NativePublicationSnapshot::published(
            retained_context, record.operation_id,
            crate::GenerationId::new(publication.publication.generation_root.digest),
        );
        assert_eq!(issued.snapshot().generation(), actual.id());
        assert_eq!(actual.id(), published.id());
        let expected = LazySnapshotRef::new(cut.workspace_id, published.id(), cut.source, cut.overlay, cut.shadows);
        assert_eq!(issued.snapshot().lazy_snapshot_with_native_work(&original, &token).expect("pure exact snapshot"), Some(expected));
        assert_ne!(retained_context, NativeSnapshotContext::Lazy(LazyPublicationCut { source: later.source, ..cut }),
            "later source is not original retained C");
        assert!(native_publication_decode_with_accounting(
            evidence, IdempotencyKey::new().operation_id(), &mut accounting,
        ).is_err(), "an unrelated operation cannot decode original C");
        let mut changed_payload = record.payload.as_ref().to_vec();
        let last = changed_payload.last_mut().expect("real nonempty canonical C");
        *last ^= 1;
        assert_ne!(crate::kernel::contextual_publication_fingerprint_from_payload_with_accounting(
            request, fingerprint_context, &changed_payload, &mut accounting,
        ).expect("real canonical fingerprint covers every retained C byte"), record.fingerprint);
        assert_eq!(original.receipt().allocation_operations, allocation_count, "pure C decode never allocates or copies the packet");
        drop(record);
        drop(custody);
        assert_eq!(store.load_lazy_workspace(root.workspace().id()).await.expect("unchanged companion").expect("binding").revision,
            companion.revision, "qualification decoder never finalizes companion state");
        assert_eq!(simulation.trace().expect("after pure lookup").into_iter()
            .filter(|entry| entry.operation == crate::simulation::SimulationOperation::AuthorityAppend).count(), appends);
    }

    #[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn native_source_publication_cut_captures_only_real_ready_original_companion() {
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let store = MemoryLazyWorkspaceStore::default();
        let root = LazyWorkspace::attach(&Fs::memory(), "native-real-cut", Arc::clone(&source), store.clone())
            .await.expect("actual canonical SDK stores");
        root.write("/input", Bytes::from_static(b"authored")).await.expect("actual identity and shadows");
        let basis = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("genuine full cut");
        let token = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(finite_fixture_budget(), &token).expect("original ledger");
        let current = root.publication_cut(None, &original, &token).await.expect("actual clean cut");
        assert_eq!((current.workspace_id, current.config, current.source, current.overlay, current.shadows),
            (basis.workspace_id, root.workspace().volume_config(), basis.source, basis.overlay, basis.shadows));
        let key = IdempotencyKey::new();
        let work = LazyTreapWork::native_with_work(&original, &token).expect("same original ledger");
        root.prepare_mount_removals(&[], &[], &[], key, &work).await
            .expect("same actual mounted companion preparation");
        assert!(root.publication_cut(None, &original, &token).await.is_err(),
            "unresolved original operation cannot be recaptured as a clean new publication");
        assert!(root.publication_cut(Some(IdempotencyKey::new().operation_id()), &original, &token)
            .await.is_err(), "companion labels for another operation are not original evidence");
        let frozen = root.publication_cut(Some(key.operation_id()), &original, &token)
            .await.expect("original exact prepared cut");
        assert_eq!(frozen, current, "an empty host delta does not replace genuine source or shadow metadata");
        let state = store.load_lazy_workspace(root.workspace().id()).await.expect("actual state").expect("binding");
        let prefix = original.live_allocation_bytes();
        let denied = original.native_volume_work(WorkBudget { items_examined: 0, ..WorkBudget::UNBOUNDED })
            .expect("same original scoped denial");
        assert!(root.publication_cut(Some(key.operation_id()), &denied, &token).await.is_err());
        assert_eq!(original.live_allocation_bytes(), prefix);
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let before = original.receipt();
        assert!(root.publication_cut(Some(key.operation_id()), &original, &cancelled).await.is_err());
        assert_eq!(original.receipt(), before, "cancelled capture allocates or copies nothing");
        source.invalidate();
        assert!(root.publication_cut(Some(key.operation_id()), &original, &token).await.is_err(),
            "changed live source cannot replace the genuinely prepared original epoch");
        assert_eq!(store.load_lazy_workspace(root.workspace().id()).await.expect("still actual state").expect("binding").revision,
            state.revision, "cut capture never writes or settles a companion");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn readonly_native_qualification_uses_actual_sdk_capture_without_source_rebind_or_publication() {
        let simulation = crate::simulation::Simulation::default();
        let (authority, objects) = simulation.stores();
        let fs = Fs::new(authority, objects, crate::EmbeddedCapabilities::MEMORY);
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let store = MemoryLazyWorkspaceStore::default();
        let root = LazyWorkspace::attach(&fs, "readonly-native-qualified", Arc::clone(&source), store.clone())
            .await.expect("actual SDK source and companion");
        root.write("/authored", Bytes::from_static(b"actual")).await.expect("real authored identity");
        let basis = root.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await.expect("genuine current immutable snapshot");
        let before = store.load_lazy_workspace(root.workspace().id()).await.expect("actual companion").expect("binding");
        let appends = simulation.trace().expect("real trace").into_iter()
            .filter(|entry| entry.operation == crate::simulation::SimulationOperation::AuthorityAppend).count();
        let token = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(finite_fixture_budget(), &token).expect("current READ scope");
        let qualification = root.publication_snapshot_with_native_work(&original).await
            .expect("real readonly SDK qualification");
        assert_eq!(qualification.operation(), None, "a readonly capture does not invent DATA");
        assert_eq!(qualification.snapshot().generation(), basis.authored_generation);
        assert_eq!(qualification.snapshot().workspace_id(), root.workspace().id());
        assert_eq!(qualification.snapshot().volume_config(), root.workspace().volume_config());
        assert_eq!(qualification.snapshot().lazy_snapshot_with_native_work(&original, &token).expect("same pure SDK projection"),
            Some(basis), "logical snapshot identity is retained, never used as a generation");
        let retained = original.live_allocation_bytes();
        let repeated = root.publication_snapshot_with_native_work(&original).await.expect("same actual readonly capture");
        assert_eq!(repeated, qualification);
        assert_eq!(original.live_allocation_bytes(), retained,
            "fixed output retains no retired decoded-state guard or phantom heap allocation");
        let after = store.load_lazy_workspace(root.workspace().id()).await.expect("unchanged companion").expect("binding");
        assert_eq!((after.revision, after.source, after.overlay, after.shadows),
            (before.revision, before.source, before.overlay, before.shadows));
        source.invalidate();
        assert!(matches!(root.publication_snapshot_with_native_work(&original).await
            .expect_err("readonly capture cannot rebind a newer source").semantic_error(), LazyWorkspaceError::StaleSource));
        let stale = store.load_lazy_workspace(root.workspace().id()).await.expect("still untouched companion").expect("binding");
        assert_eq!((stale.revision, stale.source, stale.overlay, stale.shadows),
            (before.revision, before.source, before.overlay, before.shadows));
        assert_eq!(simulation.trace().expect("after actual readonly captures").into_iter()
            .filter(|entry| entry.operation == crate::simulation::SimulationOperation::AuthorityAppend).count(), appends);
        let ordinary = root.snapshot(&LazyTreapWork::native_with_work(&original, &token).expect("native context")).await.expect("existing ordinary capture still rebinds");
        assert_eq!(ordinary.source, source.reference());
        let rebound = store.load_lazy_workspace(root.workspace().id()).await.expect("ordinary existing behavior").expect("binding");
        assert!(rebound.revision > before.revision);
    }
}
