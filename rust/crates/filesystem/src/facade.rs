//! Thin embedded `Fs`/`Volume`/`Checkout` composition over the two durable stores.

use acyclic_stream::ProviderTask;
use crate::async_storage::{AsyncAuthorityStore, AsyncObjectStore};
use crate::cancellation::{CancellationError, CancellationToken};
use crate::foundation::{
    AuthorityId, Digest, DurableCommit, Epoch, FileId, GenerationId, Head, OperationId, ProposedCommit,
    Sequence, VolumeId,
};
use crate::heap_future::in_heap;
use crate::kernel::{
    contextual_publication_fingerprint_with_accounting, list_attributes_async_with_accounting, lookup_file_record_async_with_accounting,
    lookup_path_async_with_accounting, lookup_paths, lookup_paths_async_with_accounting,
    merge_generation_with_borrowed_map_resolutions_with_accounting,
    merge_generation_with_borrowed_resolutions_with_accounting, observe_path_edges_async,
    plan_extent_range_with_accounting, prove_record_closure_async, read_blob_range_with_accounting,
    read_generation_root_with_accounting, retire_allocation, split_allocation, types,
};
#[cfg(not(target_arch = "wasm32"))]
use crate::kernel::{
    prove_generation_closure_with_native_work, prove_record_closure_with_native_work,
    publish_generation_async_guarded_with_native_work,
};

use crate::kernel::allocation::{AllocationLedger, KernelAccounting, KernelOwnedAllocation};
use crate::kernel::{Owned, ProofDataClaim, ProofDataCustody};
use crate::workspace::WorkspaceError;
use crate::foundation::usize_to_u64;
#[cfg(not(target_arch = "wasm32"))]
use crate::{NativeAllocation, NativeMaterializationWork, NativeTreeMaterializationError};
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
use crate::kernel::decode_retention_created;
use crate::kernel::{
    AsyncBlobSource, AttributeEntry, AttributeListError, AttributeListing, AttributeLookupError,
    AttributeMutation, AttributeMutationError, AttributeName, AttributePage, AuthenticatedGenerationProbe,
    AuthenticatedProbeError, BlobBuildError, BlobBuildOptions, BlobReadError, CanonicalDecodeError,
    CheckoutDependencies, CheckpointError, CheckpointRequest, ClosureError, ClosureLimits, DecodeLimits,
    Dependency, DependencyError, DependencyRegion, DependencyState, DirectoryPage, DirectoryReadError,
    ExtentPlan, ExtentRangeRequest, ExtentSeekRequest, ExtentSeekTarget, FileKind, FileMetadata,
    FileMutation, FilePayload, FileRangeRead, FileRangeReadError, FileRangeRequest, FileRecord,
    FileRecordReadError, FileTableMutationError, GenerationExportManifest, GenerationMutationError,
    GenerationRoot, GenerationTransferBatch, GenerationTransferError, InlineFileData, InlineFileDataError,
    LivePublicationObservation, LiveRetryAction, LiveRetryError, LiveRetryState, LogicalName,
    MergeConflictResolution, MergeConflictSide, MergeGenerationError, MergeGenerationOutcome,
    MergeGenerationRequest, MetadataField, Mutation, NameEncoding, NamespacePath, PathBatchLookup,
    PathLookup, PathLookupError, ProbeLimits, PublicationError, PublicationIntent, PublicationReceipt,
    PublishGenerationRequest, RebaseConflict, RebaseDecision, RebaseError, RegularMutation,
    RegularMutationError, RetentionCreated, RetentionCreatedError, RetentionKind, TransferCursor,
    TreeMutationError, TreePage, VolumeCreated, VolumeCreatedError, apply_regular_mutation_async,
    authenticate_generation_export_manifest_async, build_blob,
    build_generation_export_manifest_async, contextual_publication_fingerprint, decode_file_metadata,
    decode_volume_created, decode_workspace_deleted, export_generation_batch_async,
    import_generation_batch_async, list_attributes, lookup_file_record,
    lookup_file_records, merge_generation_async, plan_extent_range,
    prove_generation_closure, publish_generation, publish_generation_async_with_permit,
    read_blob_range, read_file_range_async, retention_authority_id, volume_authority_id,
};
#[cfg(test)]
use crate::kernel::{
    merge_directory_record_async as merge_directory_record,
};
pub use crate::kernel::{LiveMutationOutcome, MergeConflict};
#[cfg(all(
    feature = "local",
    feature = "native-watch",
    not(target_arch = "wasm32")
))]
use crate::kernel::{decode_source_volume, source_authority_id};
use crate::model::{
    AccessMode, CaseSensitivity, CheckoutMode, CheckoutModeError, ConcurrencyMode, ConsistencyMode,
    GenerationSelector, Lifecycle, MutationMode, UnicodePolicy, VolumeConfig, VolumeConfigError,
};
use crate::obs::Observe as _;
use crate::performance::{
    MeasuredResult, OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkError,
};
use crate::storage::{
    AppendOutcome, AuthorityStoreError, ByteRange, FenceOutcome, HashedObject,
    OBJECT_DIGEST_ENVELOPE_BYTES, ObjectId, ObjectKind, ObjectReadRequest,
    ObjectStoreError, PublicationPermit, ReplayLimit, object_digest,
};
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
use acyclic_native_runtime::OwnershipAnchor;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
use acyclic_objects::v1::ObjectsProvider as _;
use bytes::Bytes;
use futures::{StreamExt as _, stream};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::mem::size_of;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
use std::sync::{OnceLock, Weak};
use thiserror::Error;

/// Work supplied as a standalone budget or an original native invocation.
pub trait FsWork: Sync + ProviderTask {
    /// Result shape for this work source.
    type Output<T, E>;
    #[doc(hidden)]
    fn accounting<'a>(&'a self, cancellation: &'a CancellationToken) -> KernelAccounting<'a>;
    #[doc(hidden)]
    fn finish<T, E>(&self, result: Result<T, E>, accounting: &KernelAccounting<'_>) -> Self::Output<T, E>;
}

impl FsWork for WorkBudget {
    type Output<T, E> = MeasuredResult<FsReceipt<T>, E>;
    fn accounting<'a>(&'a self, cancellation: &'a CancellationToken) -> KernelAccounting<'a> {
        KernelAccounting::standalone(*self, cancellation)
    }
    fn finish<T, E>(&self, result: Result<T, E>, accounting: &KernelAccounting<'_>) -> Self::Output<T, E> {
        measured(result, accounting)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl FsWork for &NativeMaterializationWork {
    type Output<T, E> = Result<T, E>;
    fn accounting<'a>(&'a self, cancellation: &'a CancellationToken) -> KernelAccounting<'a> {
        KernelAccounting::native(self, cancellation)
    }
    fn finish<T, E>(&self, result: Result<T, E>, _: &KernelAccounting<'_>) -> Self::Output<T, E> { result }
}

const MAXIMUM_VOLUME_EVENT_BYTES: u64 = 4 * 1024;
const STAGED_METADATA_CACHE_ENTRIES: usize = 16;

struct AuthoredMetadataCache {
    entries: [Option<(FileMetadata, ObjectId)>; STAGED_METADATA_CACHE_ENTRIES],
    next: usize,
}

impl AuthoredMetadataCache {
    fn new() -> Self {
        Self {
            entries: [None; STAGED_METADATA_CACHE_ENTRIES],
            next: 0,
        }
    }

    fn get(&self, metadata: FileMetadata, accounting: &mut KernelAccounting<'_>) -> Result<Option<ObjectId>, FsError> {
        for entry in &self.entries {
            accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            if let Some((cached, object)) = entry { if *cached == metadata { return Ok(Some(*object)); } }
        }
        Ok(None)
    }

    fn remember(&mut self, metadata: FileMetadata, object: ObjectId) {
        if let Some(slot) = self.entries.get_mut(self.next) {
            *slot = Some((metadata, object));
        }
        self.next = (self.next + 1) % STAGED_METADATA_CACHE_ENTRIES;
    }
}
/// Directory-listing page size used to scan siblings for a case-folded
const CASE_FOLD_COLLISION_SCAN_PAGE: u32 = 256;
const ROOT_FILE_DOMAIN: &[u8] = b"acyclic-fs-root-file-v1\0";
const CREATION_OPERATION_DOMAIN: &[u8] = b"acyclic-fs-create-operation-v1\0";
const CREATION_FINGERPRINT_DOMAIN: &[u8] = b"acyclic-fs-create-fingerprint-v1\0";
const WORKSPACE_JOIN_OPERATION_DOMAIN: &[u8] = b"acyclic-fs-workspace-join-operation-v1\0";

mod fork_selection;

pub(crate) enum WorkspaceJoinOutcome {
    Applied(GenerationId),
    AlreadyApplied(GenerationId),
    Joined(GenerationId, Digest),
    AlreadyJoined(GenerationId, Digest),
    NoChanges(GenerationId),
    Stale(GenerationId),
    Conflicted(Vec<MergeConflict>, bool),
    Fenced,
    IdempotencyConflict,
}

pub(crate) struct WorkspaceJoinRequest<'a, A, O, R = BTreeMap<MergeConflict, MergeConflictResolution>> {
    pub target: &'a Volume<A, O>,
    pub base: WorkspaceCommonAncestor,
    pub source: &'a crate::Generation<A, O>,
    pub expected_target: GenerationId,
    pub expected_head: Head,
    pub history: crate::workspace::JoinHistory,
    pub operation_id: OperationId,
    pub permit: PublicationPermit,
    pub maximum_generations: u32,
    pub maximum_changes: u32,
    pub maximum_conflicts: u32,
    pub resolutions: R,
}

impl<A, O, R> WorkspaceJoinRequest<'_, A, O, R> {
    fn operation_context(&self, resolutions: Digest) -> Digest {
        workspace_join_context(
            self.source.workspace.volume.id,
            self.source.id,
            self.target.id,
            self.expected_target,
            self.base,
            self.history,
            self.maximum_generations,
            self.maximum_changes,
            self.maximum_conflicts,
            resolutions,
        )
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "join digest binds every checked Filesystem generation and limit"
)]
fn workspace_join_context(
    source_workspace: VolumeId,
    source_generation: GenerationId,
    target_workspace: VolumeId,
    expected_target: GenerationId,
    base: WorkspaceCommonAncestor,
    history: crate::workspace::JoinHistory,
    maximum_generations: u32,
    maximum_changes: u32,
    maximum_conflicts: u32,
    resolutions: Digest,
) -> Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(WORKSPACE_JOIN_OPERATION_DOMAIN);
    hasher.update(&source_workspace.into_bytes());
    hasher.update(source_generation.digest().as_bytes());
    hasher.update(&target_workspace.into_bytes());
    hasher.update(expected_target.digest().as_bytes());
    hasher.update(&base.volume_id.into_bytes());
    hasher.update(base.id.digest().as_bytes());
    hasher.update(&[match history {
        crate::workspace::JoinHistory::Merge => 1,
        crate::workspace::JoinHistory::Rebase => 2,
        crate::workspace::JoinHistory::Squash => 3,
        crate::workspace::JoinHistory::CherryPick => 4,
    }]);
    hasher.update(&maximum_generations.to_le_bytes());
    hasher.update(&maximum_changes.to_le_bytes());
    hasher.update(&maximum_conflicts.to_le_bytes());
    hasher.update(resolutions.as_bytes());
    Digest::from_bytes(*hasher.finalize().as_bytes())
}

#[derive(Clone, Copy)]
enum WorkspaceJoinResolutionView<'a> {
    Map(&'a BTreeMap<MergeConflict, MergeConflictResolution>), Slice(&'a [(MergeConflict, MergeConflictResolution)]),
}

trait WorkspaceJoinResolutions: Send + Sync { fn view(&self) -> WorkspaceJoinResolutionView<'_>; }

impl WorkspaceJoinResolutions for BTreeMap<MergeConflict, MergeConflictResolution> {
    fn view(&self) -> WorkspaceJoinResolutionView<'_> { WorkspaceJoinResolutionView::Map(self) }
}

impl WorkspaceJoinResolutions for &[(MergeConflict, MergeConflictResolution)] {
    fn view(&self) -> WorkspaceJoinResolutionView<'_> { WorkspaceJoinResolutionView::Slice(self) }
}

impl WorkspaceJoinResolutionView<'_> {
    fn visit(
        self, mut visit: impl FnMut(&MergeConflict, &MergeConflictResolution) -> Result<(), FsError>,
    ) -> Result<(), FsError> {
        match self {
            Self::Map(map) => { for (key, value) in map { visit(key, value)?; } }
            Self::Slice(slice) => { for (key, value) in slice { visit(key, value)?; } }
        }
        Ok(())
    }
}

fn hash_join_resolutions(
    resolutions: WorkspaceJoinResolutionView<'_>, accounting: &mut KernelAccounting<'_>,
) -> Result<Digest, FsError> {
    let mut hasher = blake3::Hasher::new();
    let domain = b"acyclic-fs-workspace-join-resolutions-v1\0";
    let prefix = WorkCounters { bytes_hashed: domain.len() as u64, ..WorkCounters::UNCHARGED };
    accounting.admit(prefix)?;
    hasher.update(domain);
    accounting.record_completed(prefix)?;
    resolutions.visit(|conflict, resolution| {
        let key_bytes = match conflict {
            MergeConflict::File(_) => 17, MergeConflict::Binding { name, .. } =>
                26_u64.checked_add(name.as_bytes().len() as u64).ok_or(WorkError::Overflow)?,
        };
        let resolution_bytes = match resolution {
            MergeConflictResolution::Select(_) => 2, MergeConflictResolution::File(None) | MergeConflictResolution::Binding(None) => 2,
            MergeConflictResolution::Binding(Some(_)) => 18, MergeConflictResolution::File(Some(record)) => {
                let payload_bytes = match record.payload {
                    FilePayload::InlineRegular(data) => 9 + data.as_bytes().len() as u64,
                    FilePayload::Regular { .. } | FilePayload::SymbolicLink { .. }
                    | FilePayload::ReparsePoint { .. } => 42, FilePayload::Directory { .. } => 34,
                    FilePayload::Empty => 1, FilePayload::Device { .. } => 9,
                };
                60 + payload_bytes
            }
        };
        let hashing = WorkCounters {
            bytes_hashed: key_bytes.checked_add(resolution_bytes).ok_or(WorkError::Overflow)?, items_examined: 1,..WorkCounters::UNCHARGED
        };
        accounting.admit(hashing)?;
        match conflict {
            MergeConflict::File(file_id) => {
                hasher.update(&[0]);
                hasher.update(&file_id.into_bytes());
            }
            MergeConflict::Binding { directory_id, name } => {
                hasher.update(&[1]);
                hasher.update(&directory_id.into_bytes());
                hasher.update(&[match name.encoding() {
                    NameEncoding::Utf8 => 0,
                    NameEncoding::PosixBytes => 1,
                    NameEncoding::WindowsUtf16Le => 2,
                }]);
                hasher.update(&(name.as_bytes().len() as u64).to_le_bytes());
                hasher.update(name.as_bytes());
            }
        }
        hash_merge_resolution(&mut hasher, resolution);
        accounting.record_completed(hashing)
    })?;
    Ok(Digest::from_bytes(*hasher.finalize().as_bytes()))
}

fn workspace_join_conflict_bytes(conflicts: &Vec<MergeConflict>) -> Result<u64, FsError> {
    let slots = conflicts.capacity().checked_mul(size_of::<MergeConflict>()).map(usize_to_u64).ok_or(WorkError::Overflow)?;
    conflicts.iter().try_fold(slots, |bytes, conflict| {
        let name = match conflict {
            MergeConflict::File(_) => 0, MergeConflict::Binding { name, .. } => name.retained_bytes() as u64,
        };
        bytes.checked_add(name).ok_or_else(|| WorkError::Overflow.into())
    })
}

fn workspace_join_scalar(
    outcome: WorkspaceJoinOutcome, accounting: &mut KernelAccounting<'_>,
) -> Result<Owned<WorkspaceJoinOutcome>, WorkspaceError> { Ok(Owned::new(outcome, accounting.claim_owned(0, 0)?, 0)) }

enum WorkspaceJoinPublished {
    Committed(GenerationId), AlreadyCommitted(GenerationId), Conflict, Fenced, IdempotencyConflict,
}

fn workspace_join_published(receipt: &crate::kernel::PublicationReceipt) -> WorkspaceJoinPublished {
    match &receipt.outcome {
        AppendOutcome::Committed(_) => WorkspaceJoinPublished::Committed(receipt.proof.generation_id),
        AppendOutcome::AlreadyCommitted(_) => WorkspaceJoinPublished::AlreadyCommitted(receipt.proof.generation_id),
        AppendOutcome::Conflict { .. } => WorkspaceJoinPublished::Conflict, AppendOutcome::Fenced { .. } => WorkspaceJoinPublished::Fenced,
        AppendOutcome::IdempotencyConflict { .. } => WorkspaceJoinPublished::IdempotencyConflict,
    }
}

trait WorkspaceJoinPublication<A: AsyncAuthorityStore, O: AsyncObjectStore>: Send {
    type Error: From<FsError> + From<WorkspaceError> + Send;
    fn publish(
        &mut self, objects: &O, authority: &A, request: PublishGenerationRequest,
        intent: PublicationIntent, limits: crate::kernel::ClosureLimits, accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<WorkspaceJoinPublished, Self::Error>> + acyclic_stream::ProviderTask;
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> WorkspaceJoinPublication<A, O> for () {
    type Error = WorkspaceError;
    async fn publish(
        &mut self, objects: &O, authority: &A, request: PublishGenerationRequest,
        intent: PublicationIntent, limits: crate::kernel::ClosureLimits, accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<WorkspaceJoinPublished, Self::Error> {
        let published = publish_generation_async_with_permit(
            objects, authority, request, intent, limits, accounting.remaining()?, cancellation,
        ).await;
        let spent = match &published { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
        merge_creation_backend_work(accounting, spent)?;
        published.map(|receipt| workspace_join_published(&receipt)).map_err(|failure| WorkspaceError::from(FsError::from(failure.error)))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<'original, A, O, F, Fut, E> WorkspaceJoinPublication<A, O> for (&'original NativeMaterializationWork, &mut F)
where
    A: AsyncAuthorityStore, O: AsyncObjectStore,
    F: for<'materialization> FnMut(&'materialization NativeMaterializationWork) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static, E: From<FsError> + From<WorkspaceError> + Send + 'static,
{
    type Error = E;
    async fn publish(
        &mut self, objects: &O, authority: &A, request: PublishGenerationRequest,
        intent: PublicationIntent, limits: crate::kernel::ClosureLimits, accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<WorkspaceJoinPublished, E> {
        let original = accounting.native_work().ok_or(FsError::MutationNotAllowed)?;
        if !original.shares_ledger(self.0) {
            return Err(E::from(FsError::from(NativeTreeMaterializationError::InvalidLayout)));
        }
        let (receipt, allocation) = publish_generation_async_guarded_with_native_work(
            objects, authority, request, intent, limits, self.0, cancellation, self.1, None, None,
        ).await?;
        let outcome = workspace_join_published(&receipt);
        // The kernel owns the sole preparation hook, publication hold and CAS.
        drop(receipt);
        drop(allocation);
        Ok(outcome)
    }
}

/// Restart-safe proof inputs for one committed workspace join. Verification
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinCommitWitness {
    /// Exact source workspace recorded by the join.
    pub source_workspace: VolumeId,
    /// Exact source generation recorded by the join.
    pub source_generation: GenerationId,
    /// Exact target workspace recorded by the join.
    pub target_workspace: VolumeId,
    /// CAS target observed when the join was planned.
    pub expected_target: GenerationId,
    pub(crate) base_workspace: VolumeId,
    pub(crate) base_generation: GenerationId,
    /// Exact history form used to publish the result.
    pub history: crate::workspace::JoinHistory,
    pub(crate) maximum_generations: u32,
    pub(crate) maximum_changes: u32,
    pub(crate) maximum_conflicts: u32,
    pub(crate) resolutions_digest: Digest,
    pub(crate) expected_head: Head,
    /// Stable Filesystem retry identity bound to the committed join.
    pub operation_id: OperationId,
    /// Immutable generation published by the join.
    pub result_generation: GenerationId,
}

impl JoinCommitWitness {
    fn operation_context(self) -> Digest {
        workspace_join_context(
            self.source_workspace,
            self.source_generation,
            self.target_workspace,
            self.expected_target,
            WorkspaceCommonAncestor {
                volume_id: self.base_workspace,
                id: self.base_generation,
            },
            self.history,
            self.maximum_generations,
            self.maximum_changes,
            self.maximum_conflicts,
            self.resolutions_digest,
        )
    }
}

fn hash_merge_resolution(hasher: &mut blake3::Hasher, resolution: &MergeConflictResolution) {
    let hash_side = |hasher: &mut blake3::Hasher, side: MergeConflictSide| {
        hasher.update(&[match side {
            MergeConflictSide::Base => 0,
            MergeConflictSide::Ours => 1,
            MergeConflictSide::Theirs => 2,
        }]);
    };
    match resolution {
        MergeConflictResolution::Select(side) => {
            hasher.update(&[0]);
            hash_side(hasher, *side);
        }
        MergeConflictResolution::File(record) => {
            hasher.update(&[1]);
            if let Some(record) = record {
                hasher.update(&[1]);
                hasher.update(&record.file_id.into_bytes());
                hasher.update(&[match record.kind {
                    FileKind::Regular => 0,
                    FileKind::Directory => 1,
                    FileKind::SymbolicLink => 2,
                    FileKind::Fifo => 3,
                    FileKind::Socket => 4,
                    FileKind::CharacterDevice => 5,
                    FileKind::BlockDevice => 6,
                    FileKind::ReparsePoint => 7,
                    FileKind::MountBoundary => 8,
                }]);
                hasher.update(&record.link_count.to_le_bytes());
                hash_object_id(hasher, record.metadata);
                hash_file_payload(hasher, record.payload);
            } else {
                hasher.update(&[0]);
            }
        }
        MergeConflictResolution::Binding(file_id) => {
            hasher.update(&[2]);
            if let Some(file_id) = file_id {
                hasher.update(&[1]);
                hasher.update(&file_id.into_bytes());
            } else {
                hasher.update(&[0]);
            }
        }
    }
}

fn hash_object_id(hasher: &mut blake3::Hasher, object: ObjectId) {
    hasher.update(&[object.kind.canonical_tag()]);
    hasher.update(object.digest.as_bytes());
}

fn hash_file_payload(hasher: &mut blake3::Hasher, payload: FilePayload) {
    match payload {
        FilePayload::InlineRegular(data) => {
            hasher.update(&[0]);
            hasher.update(&(data.as_bytes().len() as u64).to_le_bytes());
            hasher.update(data.as_bytes());
        }
        FilePayload::Regular {
            logical_bytes,
            extents,
        } => {
            hasher.update(&[1]);
            hasher.update(&logical_bytes.to_le_bytes());
            hash_object_id(hasher, extents);
        }
        FilePayload::Directory { entries } => {
            hasher.update(&[2]);
            hash_object_id(hasher, entries);
        }
        FilePayload::SymbolicLink {
            target_bytes,
            target,
        } => {
            hasher.update(&[3]);
            hasher.update(&target_bytes.to_le_bytes());
            hash_object_id(hasher, target);
        }
        FilePayload::Empty => {
            hasher.update(&[4]);
        }
        FilePayload::Device { major, minor } => {
            hasher.update(&[5]);
            hasher.update(&major.to_le_bytes());
            hasher.update(&minor.to_le_bytes());
        }
        FilePayload::ReparsePoint {
            payload_bytes,
            payload,
        } => {
            hasher.update(&[6]);
            hasher.update(&payload_bytes.to_le_bytes());
            hash_object_id(hasher, payload);
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct WorkspaceCommonAncestor {
    pub id: GenerationId,
    pub volume_id: VolumeId,
}

pub(crate) struct WorkspaceRebaseRequest<'a, A, O> {
    pub target: &'a Volume<A, O>,
    pub operation_id: OperationId,
    pub maximum_generations: u32,
    pub maximum_changes: u32,
    pub maximum_conflicts: u32,
}

/// Where a fork stands against the parent it was forked from.
struct RebaseLineage<A, O> {
    target_object: ObjectId,
    target_head: Head,
    target_root: GenerationRoot,
    /// The parent generation that the fork is based on.
    base_object: ObjectId,
    base_root: GenerationRoot,
    /// The parent.
    source: Volume<A, O>,
    /// The parent's head.
    source_object: ObjectId,
}

/// Capabilities that cannot be inferred from storage trait syntax alone.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EmbeddedCapabilities {
    /// Both authority and object acknowledgement survive backend restart.
    pub durable: bool,
}

impl EmbeddedCapabilities {
    /// Deterministic process-local memory profile.
    pub const MEMORY: Self = Self { durable: false };
}

/// Bounded self-contained local backend options.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalOptions {
    /// Durable root containing canonical Stream and Objects provider state.
    pub root: PathBuf,
    /// Durable hierarchical Stream provider limits.
    pub stream: acyclic_stream::LocalStreamLimits,
    /// Durable immutable Objects provider limits.
    pub objects: acyclic_objects::LocalObjectsLimits,
    /// Bounded disposable immutable-object accelerator shared by every handle.
    pub object_cache: crate::cache::ObjectCacheOptions,
}

/// Bounded reclamation result for the canonical local Stream/Objects composition.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LocalGarbageCollection {
    /// Immutable filesystem object versions examined.
    pub examined: u64,
    /// Unreachable immutable filesystem object versions removed.
    pub removed: u64,
    /// Physical body segments removed after logical reclamation.
    pub segments_removed: u64,
    /// Crash-left temporary body publications removed.
    pub temporary_files_removed: u64,
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
impl LocalOptions {
    /// Creates local options with the SDK's bounded backend defaults.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            stream: acyclic_stream::LocalStreamLimits::default(),
            objects: acyclic_objects::LocalObjectsLimits::default(),
            object_cache: crate::cache::ObjectCacheOptions::default(),
        }
    }
}

struct FsInner<A, O> {
    authority: A,
    objects: O,
    capabilities: EmbeddedCapabilities,
    workspace_namespace: [u8; 16],
    path_index: Arc<dyn crate::path_index::GenerationPathIndex>,
    // This field must remain last: Rust drops fields in declaration order, so every durable
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    _local_root_lifecycle: Option<OwnershipAnchor>,
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
struct LocalRootRegistration {
    options: LocalOptions,
    live: Weak<FsInner<LocalAuthorityBackend, LocalObjectBackend>>,
    opening: Arc<tokio::sync::Mutex<()>>,
    lifecycle: Arc<tokio::sync::Mutex<()>>,
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
type LocalRootRegistry = std::collections::BTreeMap<PathBuf, LocalRootRegistration>;

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
fn prepare_local_root_open(
    registry: &mut LocalRootRegistry,
    root: &Path,
    options: &LocalOptions,
) -> Arc<tokio::sync::Mutex<()>> {
    if let Some(registration) = registry.get(root) {
        return Arc::clone(&registration.opening);
    }
    let opening = Arc::new(tokio::sync::Mutex::new(()));
    let lifecycle = Arc::new(tokio::sync::Mutex::new(()));
    registry.insert(
        root.to_path_buf(),
        LocalRootRegistration {
            options: options.clone(),
            live: Weak::new(),
            opening: Arc::clone(&opening),
            lifecycle: Arc::clone(&lifecycle),
        },
    );
    opening
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
async fn run_local_initialization<T, F, I>(
    ownership: tokio::sync::OwnedMutexGuard<()>,
    span: crate::obs::OperationSpan,
    initialize: I,
) -> Result<T, FsError>
where
    T: Send + 'static,
    F: std::future::Future<Output = T> + Send + 'static,
    I: FnOnce(tokio::sync::OwnedMutexGuard<()>) -> F + Send + 'static,
{
    // Tokio detaches a spawned task when its JoinHandle is dropped. That is intentional here:
    tokio::spawn(async move {
        crate::obs::in_span(&span, async move { initialize(ownership).await }).await
    })
    .await
    .map_err(|_| FsError::LocalInitializationWorker)
}

/// Embedded filesystem composition handle.
pub struct Fs<A, O> {
    inner: Arc<FsInner<A, O>>,
}

impl<A, O> Clone for Fs<A, O> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<A, O> Fs<A, O> {
    /// Deployment guarantees supplied when this engine was composed.
    #[must_use]
    pub fn capabilities(&self) -> EmbeddedCapabilities {
        self.inner.capabilities
    }

    pub(crate) fn workspace_name_identity(
        &self, name: impl AsRef<str>, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> MeasuredResult<FsReceipt<(crate::WorkspaceName, crate::WorkspaceId)>, WorkspaceError> {

        let named = crate::WorkspaceName::new(name, budget, cancellation)?;
        let work = named.work;
        let derived = crate::WorkspaceId::derive(
            self.inner.workspace_namespace, &named.value,
            work.remaining(budget).map_err(|error| OperationFailure::new(WorkspaceError::Work(error), work))?,
            cancellation,
        ).map_err(|failure| failure.map_with_prior_work(work, |error| error))?;
        let work = work.checked_add(derived.work).map_err(|error| OperationFailure::new(WorkspaceError::Work(error), work))?;
        Ok(FsReceipt { value: (named.value, derived.value), work })
    }

    /// Derives the deployment-scoped stable identity for one canonical name.
    pub fn workspace_id(
        &self,
        name: impl AsRef<str>,
    ) -> Result<crate::WorkspaceId, crate::WorkspaceNameError> {
        self.workspace_name_identity(name, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map(|receipt| receipt.value.1).map_err(|failure| match failure.error {
                WorkspaceError::Name(error) => error, _ => crate::WorkspaceNameError::Allocation,
            })
    }

    pub(crate) fn same_deployment(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    /// Whether a workspace was acquired from this exact filesystem handle or its clones.
    #[must_use]
    pub fn owns_workspace(&self, workspace: &crate::Workspace<A, O>) -> bool {
        self.same_deployment(&workspace.volume.fs)
    }

    pub(crate) fn path_index(&self) -> &dyn crate::path_index::GenerationPathIndex {
        &*self.inner.path_index
    }

    pub(crate) fn authority(&self) -> &A {
        &self.inner.authority
    }

    /// Coordinates leases on an authority backend that stores windows atomically
    #[must_use]
    pub fn operation_windows(&self) -> crate::OperationWindowCoordinator<A>
    where
        A: crate::OperationWindowStore + Clone,
    {
        crate::OperationWindowCoordinator::new(self.inner.authority.clone())
    }

    pub(crate) fn objects(&self) -> &O {
        &self.inner.objects
    }
}

#[cfg(feature = "distributed")]
impl<P, O> Fs<crate::StreamAuthorityStore<P>, O> {
    /// Opens the SDK operation-window store on the exact Stream provider used
    #[must_use]
    pub fn operation_window_store(&self) -> crate::StreamOperationWindowStore<P> {
        crate::StreamOperationWindowStore::new(self.inner.authority.provider())
    }
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
impl<O> Fs<LocalAuthorityBackend, O> {
    /// Flushes the authority frames written under
    pub fn flush_deferred_authority(&self) -> Result<(), acyclic_stream::StreamError> {
        self.inner.authority.provider().flush()
    }
}

/// One independently configured volume handle reconstructed from authority.
pub struct Volume<A, O> {
    pub(crate) fs: Fs<A, O>,
    pub(crate) id: VolumeId,
    pub(crate) config: VolumeConfig,
}

impl<A, O> Clone for Volume<A, O> {
    fn clone(&self) -> Self {
        Self {
            fs: self.fs.clone(),
            id: self.id,
            config: self.config,
        }
    }
}

/// Immutable generation-fenced checkout admitted by the embedded facade.
pub struct Checkout<A, O> {
    volume: Volume<A, O>,
    generation_root: ObjectId,
    base_file_table: ObjectId,
    base_root: GenerationRoot,
    /// The base generation and the working tree, which a running collection
    root: crate::collection::CheckoutRoots,
    authority_head: Option<Head>,
    authored_operation_id: Option<OperationId>,
    live_operation_id: Option<OperationId>,
    last_commit: Option<LastCommit>,
    publication_closure: Option<PublicationClosure<A, O>>,
    prepared_merge_parent: Option<ObjectId>,
    dependencies: DependencyLedger,
    /// Exact newly owned proof payload admitted when this handle was constructed.
    proof_owned_bytes: u64,
    mode: CheckoutMode,
}

#[derive(Clone, Copy)]
enum CheckoutControlOrigin {
    Standalone,
    #[cfg(not(target_arch = "wasm32"))]
    NativeOriginal,
}

/// Tracking proof of one checkout, shared with that checkout's observers.
struct DependencyLedger {
    proof: Arc<Mutex<Proof>>,
    observing: bool,
    data_custody: Arc<ProofDataCustody>, accounting: parking_lot::Mutex<ProofDataAccounting>,
    // Checkout's base-root Vec drops before its ledger in declaration order.
    base_root_control: ProofDataClaim,
}

/// One checkout's tracking proof, with the base paths whose complete
#[derive(Clone)]
struct Proof {
    dependencies: CheckoutDependencies,
    observed_base: Option<ObjectId>,
    /// Each observed path, and whether its terminal record was observed.
    observed_paths: Option<Arc<ObservedPathMap>>, observed_data_bytes: u64, observed_clone_data_bytes: u64, observed_data_generation: u64,
    // Last: the actual Mutex payload keeps its claim across proof Arc aliases.
    control: ProofDataClaim,
}

#[derive(Clone)]
struct ObservedPathData { terminal: bool, data: ProofDataClaim, }

impl PartialEq for ObservedPathData { fn eq(&self, other: &Self) -> bool { self.terminal == other.terminal } }

impl Eq for ObservedPathData {}

/// One actual Arc payload; its keys and DATA retire before its control claim.
#[derive(Default)]
struct ObservedPathMap { entries: BTreeMap<NamespacePath, ObservedPathData>, control: ProofDataClaim, }

impl Clone for ObservedPathMap {
    fn clone(&self) -> Self { Self { entries: self.entries.clone(), control: ProofDataClaim::default() } }
}

impl PartialEq for ObservedPathMap { fn eq(&self, other: &Self) -> bool { self.entries == other.entries } }

impl Eq for ObservedPathMap {}

#[derive(Default)]
struct ProofDataAccounting { allocations: AllocationLedger, }

enum ProofDataAdmissionLedger<'a> {
    Local { accounting: &'a mut ProofDataAccounting, work: &'a mut WorkCounters, budget: WorkBudget, },
    #[cfg(not(target_arch = "wasm32"))]
    Original {
        original: &'a NativeMaterializationWork, staged: &'a mut NativeAllocation, pending: Option<NativeAllocation>,
        witness: crate::demand::NativeCheckoutControlWitness,
    },
}

struct ProofDataAdmission<'a> {
    ledger: ProofDataAdmissionLedger<'a>, owner: &'a Arc<ProofDataCustody>, cancellation: &'a CancellationToken,
}

impl ProofDataAdmission<'_> {
    fn reconcile(&mut self) -> Result<(), FsError> {
        let accounting = match &mut self.ledger {
            ProofDataAdmissionLedger::Local { accounting, .. } => accounting,
            #[cfg(not(target_arch = "wasm32"))]
            ProofDataAdmissionLedger::Original { .. } => return Ok(()),
        };
        let (retained, _) = self.owner.live_payload_counts();
        let live = accounting.allocations.live_bytes();
        if retained > live {
            // These exact same-producer payloads were admitted by their
            let mut mirrored = WorkCounters::default();
            accounting.allocations.claim_bytes(
                retained - live, 0, &mut mirrored, WorkBudget::UNBOUNDED,
            ).map_err(crate::collection::handle_allocation_error)
        } else { accounting.allocations.release(live - retained) .map_err(crate::collection::handle_allocation_error) }
    }

    fn admit(&mut self, bytes: u64, operations: u64, copied: u64, items: u64) -> Result<(), FsError> {
        self.cancellation.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let ProofDataAdmissionLedger::Original { original, pending, .. } = &mut self.ledger {
            let delta = WorkCounters { bytes_copied: copied, items_examined: items, ..WorkCounters::UNCHARGED };
            if operations != 0 || copied != 0 {
                if pending.as_ref().is_some_and(|guard| guard.bytes() != 0) {
                    return Err(FsError::NativeKernel(NativeTreeMaterializationError::InvalidLayout));
                }
                let allocation = original.claim_allocation(bytes, operations, delta)?;
                *pending = Some(allocation);
            } else { original.charge(delta)?; }
            return Ok(());
        }
        self.reconcile()?;
        let (accounting, caller_work, budget) = match &mut self.ledger {
            ProofDataAdmissionLedger::Local { accounting, work, budget } => { (accounting, work, *budget) }
            #[cfg(not(target_arch = "wasm32"))]
            ProofDataAdmissionLedger::Original { .. } => unreachable!("original admission returned"),
        };
        let (_, standing) = self.owner.live_payload_counts();
        let mut allocations = accounting.allocations;
        let mut work = **caller_work;
        work.charge_items(items, &budget)?;
        crate::collection::charge_handle_copy(copied, &mut work, budget)?;
        // Delivered DATA and constructor/control payloads already belong to
        let earlier_peak = work.peak_allocation_bytes;
        work.peak_allocation_bytes = 0;
        let external = self.owner.operation_cache_bytes();
        let local_live = allocations.live_bytes().checked_sub(standing)
            .and_then(|live| live.checked_add(bytes)).ok_or(WorkError::Overflow)?;
        let peak = local_live.checked_add(external).ok_or(WorkError::Overflow)?;
        if peak > budget.peak_allocation_bytes {
            return Err(WorkError::BudgetExceeded {
                counter: "peak_allocation_bytes", observed: peak, maximum: budget.peak_allocation_bytes,
            }.into());
        }
        let mut admission_budget = budget;
        admission_budget.peak_allocation_bytes = budget.peak_allocation_bytes
            .checked_sub(external).ok_or(WorkError::Overflow)?.saturating_add(standing);
        allocations.claim_bytes(bytes, operations, &mut work, admission_budget).map_err(crate::collection::handle_allocation_error)?;
        work.peak_allocation_bytes = earlier_peak.max(peak);
        work.verify(budget)?;
        self.owner.record_data_work(WorkCounters {
            items_examined: items, bytes_copied: copied, allocation_operations: operations, peak_allocation_bytes: peak,
            ..WorkCounters::default()
        })?;
        accounting.allocations = allocations;
        **caller_work = work;
        Ok(())
    }

    fn control_claim(&mut self, bytes: u64) -> Result<ProofDataClaim, FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let ProofDataAdmissionLedger::Original { pending, witness, .. } = &mut self.ledger {
            let pending = pending.as_mut().ok_or(FsError::NativeKernel(NativeTreeMaterializationError::InvalidLayout))?;
            let mut allocation = pending.split_off(bytes)?;
            return allocation.transfer_checkout_control(self.owner, witness).map_err(Into::into);
        }
        Ok(ProofDataClaim::pending_control(self.owner, bytes))
    }

    fn data_claim(&mut self, bytes: u64) -> Result<ProofDataClaim, FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let ProofDataAdmissionLedger::Original { pending, witness, .. } = &mut self.ledger {
            let pending = pending.as_mut().ok_or(FsError::NativeKernel(NativeTreeMaterializationError::InvalidLayout))?;
            return pending.transfer_checkout_data_bytes(bytes, self.owner, witness).map_err(Into::into);
        }
        Ok(ProofDataClaim::new(Some(self.owner), bytes))
    }
}

fn path_clone_data(path: &NamespacePath) -> (u64, u64) {
    let bytes = (path.components().len() * size_of::<LogicalName>()) as u64
        + path.components().iter().map(|name| name.as_bytes().len() as u64).sum::<u64>();
    let operations = u64::from(!path.components().is_empty())
        + path.components().iter().filter(|name| !name.as_bytes().is_empty()).count() as u64;
    (bytes, operations)
}

/// Inline custody observation, never an authority or durable allocation ID.
#[derive(Clone, Copy)]
pub(crate) struct ProofDataSnapshot { dependencies: (usize, u64, u64), paths: (usize, u64, u64), }

/// Exact copies admitted before an authoritative root replaces owned buffers.
struct PreparedCheckoutRoots {
    base: (GenerationRoot, ProofDataClaim), held: Option<(GenerationRoot, ProofDataClaim)>,
    producer: Option<Arc<ProofDataCustody>>, working_control: Option<ProofDataClaim>,
}

impl ProofDataSnapshot {
    pub(crate) fn total_bytes(self) -> Result<u64, WorkError> {
        self.dependencies.2.checked_add(self.paths.2).ok_or(WorkError::Overflow)
    }
}

impl Proof {
    fn new(dependencies: CheckoutDependencies) -> Self {
        Self::new_with_control(dependencies, ProofDataClaim::default())
    }

    fn new_with_control(dependencies: CheckoutDependencies, control: ProofDataClaim) -> Self {
        Self {
            dependencies,
            observed_base: None,
            observed_paths: Some(Arc::new(ObservedPathMap { entries: BTreeMap::new(), control })),
            observed_data_bytes: 0, observed_clone_data_bytes: 0, observed_data_generation: 0, control: ProofDataClaim::default(),
        }
    }

    fn clear(&mut self) {
        self.dependencies.clear();
        self.observed_paths = None;
        self.observed_base = None;
        self.observed_data_bytes = 0;
        self.observed_clone_data_bytes = 0;
        self.observed_data_generation = self.observed_data_generation.checked_add(1).expect("proof allocation generation exhausted");
    }

    /// Whether observing `path` against `base`, with its terminal when
    fn observes_path(&self, base: ObjectId, path: &NamespacePath, terminal: bool) -> bool {
        self.observed_base == Some(base)
            && self.observed_paths.as_deref().is_some_and(|observed| {
                observed.entries.get(path).is_some_and(|entry| entry.terminal || !terminal)
            })
    }

    /// Absorbs an observation through the same bounded canonical admission.
    fn observe_paths<'a>(
        &mut self,
        base: ObjectId,
        paths: impl IntoIterator<Item = &'a NamespacePath>,
        terminal: bool,
        dependencies: Vec<Dependency>,
        maximum_dependencies: u32,
        admission: &mut ProofDataAdmission<'_>,
    ) -> Result<(), FsError> {
        self.extend_observations(dependencies, maximum_dependencies, admission)?;
        let maximum_paths = usize::try_from(maximum_dependencies).unwrap_or(usize::MAX);
        if self.observed_base != Some(base) || self.observed_paths.is_none() {
            let generation = self.observed_data_generation.checked_add(1).ok_or(WorkError::Overflow)?;
            admission.admit(size_of::<ObservedPathMap>() as u64, 1, 0, 0)?;
            let control = admission.control_claim(size_of::<ObservedPathMap>() as u64)?;
            self.observed_paths = Some(Arc::new(ObservedPathMap { entries: BTreeMap::new(), control }));
            self.observed_base = Some(base);
            self.observed_data_bytes = 0;
            self.observed_clone_data_bytes = 0;
            self.observed_data_generation = generation;
            admission.reconcile()?;
        }
        for path in paths {
            admission.admit(0, 0, 0, 1)?;
            if self.observes_path(base, path, terminal) {
                continue;
            }
            let observed = self.observed_paths.as_deref().expect("the observed base owns a map");
            if !observed.entries.contains_key(path) && observed.entries.len() >= maximum_paths {
                let generation = self.observed_data_generation.checked_add(1).ok_or(WorkError::Overflow)?;
                admission.admit(size_of::<ObservedPathMap>() as u64, 1, 0, 0)?;
                let control = admission.control_claim(size_of::<ObservedPathMap>() as u64)?;
                self.observed_paths = Some(Arc::new(ObservedPathMap { entries: BTreeMap::new(), control }));
                self.observed_data_bytes = 0;
                self.observed_clone_data_bytes = 0;
                self.observed_data_generation = generation;
                admission.reconcile()?;
            }
            let observed_paths = self.observed_paths.as_mut().expect("the observed base owns a map");
            let copied = Arc::strong_count(observed_paths) > 1;
            let moved = !copied && Arc::weak_count(observed_paths) != 0;
            let generation = if copied || moved {
                self.observed_data_generation.checked_add(1).ok_or(WorkError::Overflow)?
            } else { self.observed_data_generation };
            let mut cloned_operations = 0_u64;
            if copied {
                for old in observed_paths.entries.keys() {
                    admission.admit(0, 0, 0, 1)?;
                    cloned_operations = cloned_operations.checked_add(path_clone_data(old).1).ok_or(WorkError::Overflow)?;
                }
            }
            if copied || moved {
                let (bytes, operations, copied_bytes) = if copied {
                    (
                        (size_of::<ObservedPathMap>() as u64).checked_add(self.observed_clone_data_bytes).ok_or(WorkError::Overflow)?,
                        cloned_operations.checked_add(1).ok_or(WorkError::Overflow)?, self.observed_clone_data_bytes,
                    )
                } else { (size_of::<ObservedPathMap>() as u64, 1, 0) };
                admission.admit(bytes, operations, copied_bytes, 0)?;
                let control = admission.control_claim(size_of::<ObservedPathMap>() as u64)?;
                let observed = Arc::make_mut(observed_paths);
                observed.control = control;
                if copied {
                    for (path, entry) in observed.entries.iter_mut() {
                        entry.data = admission.data_claim(path.retained_bytes())?;
                    }
                    self.observed_data_bytes = self.observed_clone_data_bytes;
                }
            }
            self.observed_data_generation = generation;
            let observed = Arc::make_mut(observed_paths);
            if let Some(entry) = observed.entries.get_mut(path) { entry.terminal |= terminal; continue; }
            let (bytes, operations) = path_clone_data(path);
            let retained = self.observed_data_bytes.checked_add(bytes).ok_or(WorkError::Overflow)?;
            let cloned = self.observed_clone_data_bytes.checked_add(bytes).ok_or(WorkError::Overflow)?;
            admission.admit(bytes, operations, bytes, 0)?;
            let path = path.clone();
            let data = admission.data_claim(path.retained_bytes())?;
            observed.entries.insert(path, ObservedPathData { terminal, data });
            self.observed_data_bytes = retained;
            self.observed_clone_data_bytes = cloned;
        }
        Ok(())
    }

    fn extend_observations(
        &mut self, dependencies: Vec<Dependency>, maximum_dependencies: u32, admission: &mut ProofDataAdmission<'_>,
    ) -> Result<(), FsError> {
        let extension = self.dependencies.prepare_observations(dependencies, maximum_dependencies)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let ProofDataAdmissionLedger::Original { original, staged, .. } = &mut admission.ledger {
            return self.dependencies.commit_with_original_data_custody(
                extension, admission.owner, staged, original, admission.cancellation,
            );
        }
        let owner = Arc::clone(admission.owner);
        self.dependencies.commit_with_data_custody(
            extension, Some(&owner), |bytes, operations, copied, items| {
                admission.admit(bytes, operations, copied, items)
            },
        )
    }
}

impl std::ops::Deref for Proof {
    type Target = CheckoutDependencies;

    fn deref(&self) -> &Self::Target {
        &self.dependencies
    }
}

impl std::ops::DerefMut for Proof {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.dependencies
    }
}

impl DependencyLedger {
    fn new(dependencies: CheckoutDependencies) -> Self {
        Self::sharing(Proof::new(dependencies))
    }

    /// Constructs already admitted native map and proof payloads from their
    #[cfg(not(target_arch = "wasm32"))]
    fn new_with_native_custody(
        dependencies: CheckoutDependencies, data_custody: Arc<ProofDataCustody>, proof_control: ProofDataClaim,
        paths_control: ProofDataClaim,
    ) -> Self {
        let mut proof = Proof::new_with_control(dependencies, paths_control);
        proof.control = proof_control;
        Self::sharing_with_custody(proof, data_custody)
    }

    fn sharing(proof: Proof) -> Self { Self::sharing_with_custody(proof, Arc::default()) }

    fn sharing_with_custody(proof: Proof, data_custody: Arc<ProofDataCustody>) -> Self {
        Self {
            proof: Arc::new(Mutex::new(proof)),
            observing: false,
            data_custody, accounting: parking_lot::Mutex::default(), base_root_control: ProofDataClaim::default(),
        }
    }

    fn proof(&self) -> MutexGuard<'_, Proof> {
        self.proof.lock().unwrap_or_else(PoisonError::into_inner)
    }

    #[cfg(feature = "native-mount")]
    fn observing_with_custody(&self, data_custody: Arc<ProofDataCustody>) -> Self {
        Self {
            proof: Arc::clone(&self.proof),
            observing: true,
            data_custody, accounting: parking_lot::Mutex::default(), base_root_control: ProofDataClaim::default(),
        }
    }

    /// Uses the already admitted token so its original owner can register
    fn independent_with_custody(&self, data_custody: Arc<ProofDataCustody>) -> Self {
        Self {
            proof: Arc::new(Mutex::new(self.proof().clone())), observing: false, data_custody,
            accounting: parking_lot::Mutex::default(), base_root_control: ProofDataClaim::default(),
        }
    }

    fn observe_paths<'a>(
        &self, base: ObjectId, paths: impl IntoIterator<Item = &'a NamespacePath>, terminal: bool,
        dependencies: Vec<Dependency>, maximum_dependencies: u32, work: &mut WorkCounters, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let mut proof = self.proof();
        let mut accounting = self.accounting.lock();
        proof.observe_paths(base, paths, terminal, dependencies, maximum_dependencies, &mut ProofDataAdmission {
            ledger: ProofDataAdmissionLedger::Local { accounting: &mut accounting, work, budget }, owner: &self.data_custody, cancellation,
        })
    }

    fn extend_observations(
        &self, dependencies: Vec<Dependency>, maximum_dependencies: u32, work: &mut WorkCounters, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let mut proof = self.proof();
        let mut accounting = self.accounting.lock();
        proof.extend_observations(dependencies, maximum_dependencies, &mut ProofDataAdmission {
            ledger: ProofDataAdmissionLedger::Local { accounting: &mut accounting, work, budget }, owner: &self.data_custody, cancellation,
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn observe_paths_with_native_work<'a>(
        &self, base: ObjectId, paths: impl IntoIterator<Item = &'a NamespacePath>, terminal: bool,
        dependencies: Vec<Dependency>, staged: &mut NativeAllocation, maximum_dependencies: u32,
        original: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let witness = original.checkout_control_sink(&self.data_custody)?;
        self.proof().observe_paths(
            base, paths, terminal, dependencies, maximum_dependencies, &mut ProofDataAdmission {
                ledger: ProofDataAdmissionLedger::Original { original, staged, pending: None, witness },
                owner: &self.data_custody, cancellation,
            },
        )
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn extend_observations_with_native_work(
        &self, dependencies: Vec<Dependency>, staged: &mut NativeAllocation, maximum_dependencies: u32,
        original: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let witness = original.checkout_control_sink(&self.data_custody)?;
        self.proof().extend_observations(
            dependencies, maximum_dependencies, &mut ProofDataAdmission {
                ledger: ProofDataAdmissionLedger::Original { original, staged, pending: None, witness },
                owner: &self.data_custody, cancellation,
            },
        )
    }

    fn copy_observation_name_with_accounting(
        &self, name: &LogicalName, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<(LogicalName, KernelOwnedAllocation), FsError> {
        cancellation.check()?;
        let bytes = name.as_bytes().len() as u64;
        let delta = WorkCounters { bytes_copied: bytes, items_examined: 1, ..WorkCounters::UNCHARGED };
        accounting.admit(delta)?;
        let allocation = accounting.claim_owned(bytes, u64::from(bytes != 0))?;
        let name = name.clone();
        let mut copied = (name, allocation);
        let actual = copied.0.retained_bytes() as u64;
        let capacity = accounting.reconcile_owned(&mut copied.1, bytes, actual);
        let completed = accounting.record_completed(delta);
        capacity?;
        completed?;
        cancellation.check()?;
        Ok((copied.0, copied.1))
    }


}

#[cfg(feature = "native-mount")]
impl<A, O> Checkout<A, O> {
    /// Reads the undelivered report without transferring its DATA custody.
    pub(crate) fn proof_data_pending_work(&self) -> WorkCounters {
        self.dependencies.data_custody.pending_data_work()
    }

    /// Drains only the undelivered receipt report, never the operative ledger.
    pub(crate) fn proof_data_take_completed_work(&self) -> WorkCounters {
        self.dependencies.data_custody.take_completed_data_work()
    }

    /// Custody control only; retaining this never retains a proof allocation.
    pub(crate) fn proof_data_custody(&self) -> Arc<ProofDataCustody> {
        Arc::clone(&self.dependencies.data_custody)
    }
}

/// Cheap immutable reader over one authenticated pinned checkout candidate.
pub struct PinnedReader<A, O> {
    volume: Volume<A, O>,
    generation_root: ObjectId,
    root: PinnedReaderRoot,
}

#[derive(Clone)]
enum PinnedReaderRoot {
    Owned(GenerationRoot),
    #[cfg(not(target_arch = "wasm32"))]
    FundedOwned(Arc<(GenerationRoot, NativeAllocation)>),
    #[cfg(not(target_arch = "wasm32"))]
    NativeCheckout(Arc<(crate::collection::CheckoutRoots, NativeAllocation)>),
}

impl std::ops::Deref for PinnedReaderRoot {
    type Target = GenerationRoot;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(root) => root,
            #[cfg(not(target_arch = "wasm32"))]
            Self::FundedOwned(carrier) => &carrier.0,
            #[cfg(not(target_arch = "wasm32"))]
            Self::NativeCheckout(carrier) => &carrier.0,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn native_pinned_allocation(allocation: KernelOwnedAllocation) -> Result<NativeAllocation, FsError> {
    match allocation {
        KernelOwnedAllocation::NativeOriginal(allocation) => Ok(allocation),
        KernelOwnedAllocation::Standalone => Err(FsError::NativeKernel(NativeTreeMaterializationError::InvalidLayout)),
    }
}

fn readonly_standalone_live(accounting: &KernelAccounting<'_>) -> Option<u64> {
    match accounting {
        KernelAccounting::Standalone { allocations, .. } =>
            Some(allocations.live_bytes()),
        KernelAccounting::BorrowedStandalone { allocations, .. } =>
            Some(allocations.live_bytes()),
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { .. } => None,
    }
}

fn readonly_cleanup_standalone(
    accounting: &mut KernelAccounting<'_>, before: Option<u64>, retained: u64,
) -> Result<(), FsError> {
    let Some(before) = before else { return Ok(()); };
    let release = |allocations: &mut AllocationLedger| {
        let keep = before.checked_add(retained).ok_or(WorkError::Overflow)?;
        let retired = allocations.live_bytes().checked_sub(keep).ok_or(WorkError::Overflow)?;
        allocations.release(retired).map_err(crate::collection::handle_allocation_error)
    };
    match accounting {
        KernelAccounting::Standalone { allocations, .. } => release(allocations),
        KernelAccounting::BorrowedStandalone { allocations, .. } => release(allocations),
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { .. } => Ok(()),
    }
}

fn readonly_absorb_allocation(
    target: &mut KernelOwnedAllocation, incoming: &mut KernelOwnedAllocation,
) -> Result<(), FsError> {
    match (target, incoming) {
        (KernelOwnedAllocation::Standalone, KernelOwnedAllocation::Standalone) => Ok(()),
        #[cfg(not(target_arch = "wasm32"))]
        (KernelOwnedAllocation::NativeOriginal(target), KernelOwnedAllocation::NativeOriginal(incoming)) =>
            target.absorb(incoming).map_err(Into::into),
        #[cfg(not(target_arch = "wasm32"))]
        _ => Err(FsError::NativeKernel(NativeTreeMaterializationError::InvalidLayout)),
    }
}

impl<A, O> Clone for PinnedReader<A, O> {
    fn clone(&self) -> Self {
        Self { volume: self.volume.clone(), generation_root: self.generation_root, root: self.root.clone(), }
    }
}

/// Public facts needed to plan reads without exposing generation-bound records.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileDescription {
    /// Terminal entry kind.
    pub kind: FileKind,
    /// Logical content length for regular files and symbolic links.
    pub logical_bytes: u64,
    /// Canonical metadata authenticated with the same pinned generation.
    pub metadata: FileMetadata,
}

/// One file resolved and authenticated by a specific pinned reader.
pub struct ResolvedFile<A, O> {
    reader: PinnedReader<A, O>,
    record: FileRecord,
    description: FileDescription,
}

impl<A, O> ResolvedFile<A, O> {
    /// Stable file identity authenticated with this handle.
    #[must_use]
    pub const fn file_id(&self) -> FileId {
        self.record.file_id
    }

    /// Public semantic facts authenticated with this handle.
    #[must_use]
    pub const fn description(&self) -> &FileDescription {
        &self.description
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> ResolvedFile<A, O> {
    /// Reads one logical regular-file range without another namespace lookup.
    pub async fn read_range<W: FsWork>(
        &self, range: ByteRange, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<FileRangeRead, FsError> {
        self.reader.read_record_range(self.record, range, budget, cancellation).await
    }

    /// Reads opaque symbolic-link target bytes without another namespace lookup.
    pub async fn read_symbolic_link<W: FsWork>(
        &self, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<acyclic_stream::OwnedBytes, FsError> {
        self.reader.read_symbolic_link_record(self.record, budget, cancellation).await
    }

    /// Plans sparse physical spans without another namespace lookup.
    pub async fn plan_extents(
        &self,
        range: ByteRange,
        maximum_spans: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Option<ExtentPlan>> {
        self.reader
            .plan_record_extents(self.record, range, maximum_spans, budget, cancellation)
            .await
    }



    /// Plans sparse spans on the same original ledger without another lookup.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn plan_extents_with_native_work(
        &self, range: ByteRange, maximum_spans: u32, work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<Option<(ExtentPlan, NativeAllocation)>, FsError> {
        self.reader.plan_record_extents_with_native_work(self.record, range, maximum_spans, work, cancellation).await
    }
}

/// One range requested from a reader-scoped resolved file.
pub struct ResolvedFileRangeReadRequest<'a, A, O> {
    /// Exact resolved file handle.
    pub file: &'a ResolvedFile<A, O>,
    /// Exact logical byte range returned.
    pub range: ByteRange,
}

/// One child returned from an authenticated directory page, resolved by the
pub struct ResolvedDirectoryEntry<A, O> {
    /// Exact canonical child name.
    pub name: LogicalName,
    /// Generation-scoped child handle with authenticated metadata.
    pub file: ResolvedFile<A, O>,
}

/// One bounded authenticated directory page with reader-scoped child handles.
pub struct ResolvedDirectoryPage<A, O> {
    /// Ordered children strictly after the supplied cursor.
    pub entries: Vec<ResolvedDirectoryEntry<A, O>>,
    /// Whether at least one additional child exists.
    pub has_more: bool,
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct LastCommit {
    operation_id: OperationId,
    generation_id: GenerationId,
    head: Head,
}

/// A generation whose complete closure was just proven.
#[derive(Clone)]
struct VerifiedForkSource {
    generation_root: ObjectId,
    /// Every object reachable from `generation_root`.
    closure: Vec<ObjectId>,
    /// The store's sweep count before the closure was proven.
    proven_at: u64,
}

// Single-use custody from the original authenticated publication.
pub(crate) type PublicationClosure<A, O> = Arc<Mutex<Option<VerifiedPublication<A, O>>>>;

pub(crate) struct VerifiedPublication<A, O> {
    filesystem: Fs<A, O>, generation_root: ObjectId, volume_id: VolumeId, limits: crate::kernel::ClosureLimits,
    objects: Vec<ObjectId>, proven_at: u64, _allocation: KernelOwnedAllocation,
}

impl<A, O> VerifiedPublication<A, O> {
    pub(crate) fn retained_bytes(&self) -> u64 { usize_to_u64(self.objects.capacity() * size_of::<ObjectId>()) }
}

/// Durable local authority backend with nonblocking native storage dispatch.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub type LocalAuthorityBackend =
    crate::distributed::StreamAuthorityStore<acyclic_stream::LocalStream>;

/// Durable local immutable-object backend with nonblocking native storage dispatch.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub type LocalObjectBackend = crate::staged_objects::StagedObjects<
    crate::cache::CachedObjectStore<
        crate::LogicalObjectStore<acyclic_objects::v1::local::LocalObjects>,
    >,
>;

/// Durable local filesystem composition with nonblocking native storage dispatch.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub type LocalFs = Fs<LocalAuthorityBackend, LocalObjectBackend>;

/// Durable operation-window store sharing the local filesystem's authority
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub type LocalOperationWindowStore = crate::StreamOperationWindowStore<acyclic_stream::LocalStream>;

/// Durable local volume handle with nonblocking native storage dispatch.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub type LocalVolume = Volume<LocalAuthorityBackend, LocalObjectBackend>;

/// Deterministic in-process composition over the exact public Stream and Objects providers.
#[cfg(all(feature = "memory", feature = "distributed"))]
pub type MemoryFs = Fs<MemoryAuthorityBackend, MemoryObjectBackend>;

#[cfg(all(feature = "memory", feature = "distributed"))]
/// Filesystem authority adapter backed by the public in-memory Stream provider.
pub type MemoryAuthorityBackend =
    crate::distributed::StreamAuthorityStore<acyclic_stream::MemoryStream>;

#[cfg(all(feature = "memory", feature = "distributed"))]
/// Filesystem object adapter backed by the public in-memory Objects provider.
pub type MemoryObjectBackend = crate::LogicalObjectStore<acyclic_objects::v1::MemoryObjects>;

#[cfg(all(test, feature = "memory", feature = "distributed"))]
pub(crate) type MemoryCheckout = Checkout<MemoryAuthorityBackend, MemoryObjectBackend>;

/// Ephemeral path-independent regular file retained by an open native handle.
pub struct DetachedFile<A, O> {
    volume: Volume<A, O>,
    record: FileRecord,
    /// Keeps what the record reaches from a running collection.
    _hold: Option<crate::collection::Hold>,
}

/// Successful facade operation with exact composed work.
pub type FsReceipt<T> = OperationReceipt<T>;

/// One within-volume zero-copy logical range clone request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileCloneRequest {
    /// Existing source regular file.
    pub source: NamespacePath,
    /// Inclusive source byte offset.
    pub source_offset: u64,
    /// Existing destination regular file.
    pub destination: NamespacePath,
    /// Inclusive destination byte offset.
    pub destination_offset: u64,
    /// Positive logical byte count.
    pub length: u64,
}

/// One regular-file content change. `F` addresses files: an exact
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ContentChange<F> {
    /// Replaces one range with caller bytes, preserving sparse untouched
    Write {
        /// Inclusive logical offset.
        offset: u64,
        /// Replacement bytes.
        bytes: acyclic_stream::OwnedBytes,
    },
    /// Changes logical length while preserving sparse semantics.
    Resize {
        /// New logical byte length.
        logical_bytes: u64,
    },
    /// Replaces one range with a hole or physically allocated zeros.
    ZeroRange {
        /// Replaced logical range.
        range: ByteRange,
        /// Preserve physical allocation instead of punching a hole.
        allocated: bool,
        /// Whether the replacement may extend logical file length.
        extend: bool,
    },
    /// Allocates sparse holes while preserving all existing content.
    Preallocate {
        /// Allocated logical range.
        range: ByteRange,
        /// Preserve logical file length.
        keep_size: bool,
    },
    /// Clones one range of another regular file by immutable extent
    CloneFrom {
        /// Existing source regular file.
        source: F,
        /// Inclusive source byte offset.
        source_offset: u64,
        /// Inclusive byte offset in the changed file.
        offset: u64,
        /// Positive logical byte count.
        length: u64,
    },
}

/// One independent change that [`Checkout::apply_group`] may apply together
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GroupedChange {
    /// Creates one regular file with the caller's fresh stable identity and
    CreateFile {
        /// New namespace path.
        path: NamespacePath,
        /// Exact cross-profile metadata.
        metadata: Box<FileMetadata>,
        /// Fresh stable identity the caller chose, as [`FileId::new`] mints.
        file_id: FileId,
        /// Complete initial bytes.
        bytes: acyclic_stream::OwnedBytes,
    },
    /// Changes the content of one regular file by stable identity.
    Content {
        /// Existing regular-file identity.
        file_id: FileId,
        /// The content change.
        change: ContentChange<FileId>,
        /// Whether the change also stamps the file's content-change times.
        times: ContentTimes,
    },
}

/// What one successful [`GroupedChange`] produced.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupedOutcome {
    /// The identity of the created file.
    Created(FileId),
    /// The content change applied.
    Changed,
}

/// Whether a content change also records its time in the changed file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContentTimes {
    /// Leaves the file's metadata unchanged.
    Preserve,
    /// Sets every represented modification and status-change time to this
    Stamp(i64),
}

/// One content change with its bytes already staged, addressed by `F`.
pub(crate) enum ContentOperation<F> {
    ValidateRegular,
    Write {
        offset: u64,
        length: u64,
        content: ObjectId,
    },
    Resize {
        logical_bytes: u64,
    },
    ZeroRange {
        range: ByteRange,
        allocated: bool,
        extend: bool,
    },
    Preallocate {
        range: ByteRange,
        keep_size: bool,
    },
    CloneFrom {
        source: F,
        source_offset: u64,
        offset: u64,
        length: u64,
    },
    SetMetadata {
        metadata: ObjectId,
    },
}

/// How a content change names the file whose current metadata it stamps.
pub(crate) enum ContentAddress<'a> {
    Path(&'a NamespacePath),
    File(FileId),
}

/// A way to address the regular file one content change applies to.
pub(crate) trait ContentFile: Sized {
    fn address(&self) -> ContentAddress<'_>;
    fn mutation(self, operation: ContentOperation<Self>) -> Mutation;
    fn clone_content_with_accounting(
        &self, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<Owned<Self>, FsError>;
}

fn content_input_path_bytes<F: ContentFile>(input: &(F, ContentChange<F>)) -> Result<u64, FsError> {
    let owned = |file: &F| match file.address() {
        ContentAddress::Path(path) => path.retained_bytes(), ContentAddress::File(_) => 0,
    };
    let source = match &input.1 { ContentChange::CloneFrom { source, .. } => owned(source), _ => 0, };
    owned(&input.0).checked_add(source).ok_or_else(|| WorkError::Overflow.into())
}

impl ContentFile for NamespacePath {
    fn address(&self) -> ContentAddress<'_> {
        ContentAddress::Path(self)
    }

    fn clone_content_with_accounting(
        &self, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<Owned<Self>, FsError> {
        let (path, allocation) = self.clone_with_accounting(accounting, cancellation)?;
        let bytes = path.retained_bytes();
        Ok(Owned::new(path, allocation, bytes))
    }

    fn mutation(self, operation: ContentOperation<Self>) -> Mutation {
        let path = self;
        match operation {
            ContentOperation::ValidateRegular => Mutation::ValidateRegular { path },
            ContentOperation::Write {
                offset,
                length,
                content,
            } => Mutation::Write {
                path,
                offset,
                length,
                content,
                content_offset: 0,
            },
            ContentOperation::Resize { logical_bytes } => Mutation::Resize {
                path,
                logical_bytes,
            },
            ContentOperation::ZeroRange {
                range,
                allocated,
                extend,
            } => Mutation::ZeroRange {
                path,
                offset: range.offset,
                length: range.length,
                allocated,
                extend,
            },
            ContentOperation::Preallocate { range, keep_size } => Mutation::Preallocate {
                path,
                offset: range.offset,
                length: range.length,
                keep_size,
            },
            ContentOperation::CloneFrom {
                source,
                source_offset,
                offset,
                length,
            } => Mutation::CloneRange {
                source,
                source_offset,
                destination: path,
                destination_offset: offset,
                length,
            },
            ContentOperation::SetMetadata { metadata } => Mutation::SetMetadata { path, metadata },
        }
    }
}

impl ContentFile for FileId {
    fn address(&self) -> ContentAddress<'_> {
        ContentAddress::File(*self)
    }

    fn clone_content_with_accounting(
        &self, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<Owned<Self>, FsError> {
        cancellation.check()?;
        let allocation = accounting.claim_owned(0, 0)?;
        Ok(Owned::new(*self, allocation, 0))
    }

    fn mutation(self, operation: ContentOperation<Self>) -> Mutation {
        let file_id = self;
        let mutation = match operation {
            ContentOperation::CloneFrom {
                source,
                source_offset,
                offset,
                length,
            } => {
                return Mutation::CloneFileRange {
                    source_file_id: source,
                    source_offset,
                    destination_file_id: file_id,
                    destination_offset: offset,
                    length,
                };
            }
            ContentOperation::ValidateRegular => FileMutation::ValidateRegular,
            ContentOperation::Write {
                offset,
                length,
                content,
            } => FileMutation::Write {
                offset,
                length,
                content,
                content_offset: 0,
            },
            ContentOperation::Resize { logical_bytes } => FileMutation::Resize { logical_bytes },
            ContentOperation::ZeroRange {
                range,
                allocated,
                extend,
            } => FileMutation::ZeroRange {
                offset: range.offset,
                length: range.length,
                allocated,
                extend,
            },
            ContentOperation::Preallocate { range, keep_size } => FileMutation::Preallocate {
                offset: range.offset,
                length: range.length,
                keep_size,
            },
            ContentOperation::SetMetadata { metadata } => FileMutation::SetMetadata { metadata },
        };
        Mutation::File { file_id, mutation }
    }
}

/// One caller-authored filesystem operation compiled into a single sparse,
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AuthoredMutation {
    /// Rebinds an existing non-directory object to a stable source identity.
    Reidentify {
        /// Existing namespace path.
        path: NamespacePath,
        /// Stable path-independent identity to retain.
        file_id: FileId,
    },
    /// Creates one regular file with exact initial bytes and metadata.
    CreateFile {
        /// New namespace path.
        path: NamespacePath,
        /// Complete initial bytes.
        bytes: acyclic_stream::OwnedBytes,
        /// Exact cross-profile metadata.
        metadata: FileMetadata,
    },
    /// Creates one regular file from already staged authenticated content.
    CreateFileFromContent {
        /// New namespace path.
        path: NamespacePath,
        /// Authenticated streamed content.
        content: StagedContent,
        /// Exact cross-profile metadata.
        metadata: FileMetadata,
        /// Optional stable source identity; the engine rejects an existing ID.
        file_id: Option<FileId>,
    },
    /// Creates one empty directory.
    CreateDirectory {
        /// New namespace path.
        path: NamespacePath,
        /// Exact cross-profile metadata.
        metadata: FileMetadata,
    },
    /// Creates one symbolic link with opaque target bytes.
    CreateSymbolicLink {
        /// New namespace path.
        path: NamespacePath,
        /// Exact uninterpreted target bytes.
        target: acyclic_stream::OwnedBytes,
        /// Exact cross-profile metadata.
        metadata: FileMetadata,
    },
    /// Creates one payload-free FIFO, socket, or mount boundary.
    CreateEmptySpecial {
        /// New namespace path.
        path: NamespacePath,
        /// Exact payload-free kind.
        kind: FileKind,
        /// Exact cross-profile metadata.
        metadata: FileMetadata,
    },
    /// Creates one POSIX character or block device.
    CreateDevice {
        /// New namespace path.
        path: NamespacePath,
        /// Character or block device kind.
        kind: FileKind,
        /// Native major identity.
        major: u32,
        /// Native minor identity.
        minor: u32,
        /// Exact cross-profile metadata.
        metadata: FileMetadata,
    },
    /// Creates one opaque Windows reparse point.
    CreateReparsePoint {
        /// New namespace path.
        path: NamespacePath,
        /// Opaque reparse payload.
        payload: acyclic_stream::OwnedBytes,
        /// Exact cross-profile metadata.
        metadata: FileMetadata,
    },
    /// Removes one namespace binding.
    Remove {
        /// Existing namespace path.
        path: NamespacePath,
        /// Optional exact path-independent identity precondition.
        expected_file_id: Option<FileId>,
    },
    /// Atomically moves one binding within the volume.
    Rename {
        /// Existing source path.
        source: NamespacePath,
        /// Destination path.
        destination: NamespacePath,
        /// Whether an existing destination may be replaced.
        replace: bool,
    },
    /// Creates a same-volume hard link.
    HardLink {
        /// Existing source path.
        source: NamespacePath,
        /// New destination path.
        destination: NamespacePath,
    },
    /// Replaces one regular-file range.
    Write {
        /// Existing regular-file path.
        path: NamespacePath,
        /// Inclusive logical offset.
        offset: u64,
        /// Exact replacement bytes.
        bytes: acyclic_stream::OwnedBytes,
    },
    /// Replaces one range from already staged authenticated content.
    WriteFromContent {
        /// Existing regular-file path.
        path: NamespacePath,
        /// Inclusive logical offset.
        offset: u64,
        /// Authenticated streamed content.
        content: StagedContent,
    },
    /// Replaces complete cross-profile metadata.
    SetMetadata {
        /// Existing path.
        path: NamespacePath,
        /// Exact replacement metadata.
        metadata: FileMetadata,
    },
    /// Changes logical regular-file length.
    Resize {
        /// Existing regular-file path.
        path: NamespacePath,
        /// New logical byte length.
        logical_bytes: u64,
    },
    /// Punches a hole or installs physically allocated zeros.
    ZeroRange {
        /// Existing regular-file path.
        path: NamespacePath,
        /// Exact logical range.
        range: ByteRange,
        /// Preserve physical allocation when true.
        allocated: bool,
        /// Permit logical file growth to the range end.
        extend: bool,
    },
    /// Allocates holes without replacing existing content.
    Preallocate {
        /// Existing regular-file path.
        path: NamespacePath,
        /// Exact allocation range.
        range: ByteRange,
        /// Preserve logical file length.
        keep_size: bool,
    },
    /// Clones an immutable logical range without reading content bytes.
    CloneRange(FileCloneRequest),
}

impl AuthoredMutation {
    /// Exact owned namespace-path buffer bytes transferred with this mutation.
    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        match self {
            Self::Rename { source, destination, .. }
            | Self::HardLink { source, destination } => { source.retained_bytes() + destination.retained_bytes() }
            Self::CloneRange(request) => { request.source.retained_bytes() + request.destination.retained_bytes() }
            Self::Reidentify { path, .. }
            | Self::CreateFile { path, .. }
            | Self::CreateFileFromContent { path, .. }
            | Self::CreateDirectory { path, .. }
            | Self::CreateSymbolicLink { path, .. }
            | Self::CreateEmptySpecial { path, .. }
            | Self::CreateDevice { path, .. }
            | Self::CreateReparsePoint { path, .. }
            | Self::Remove { path, .. }
            | Self::Write { path, .. }
            | Self::WriteFromContent { path, .. }
            | Self::SetMetadata { path, .. }
            | Self::Resize { path, .. }
            | Self::ZeroRange { path, .. }
            | Self::Preallocate { path, .. } => path.retained_bytes(),
        }
    }
}

fn authored_input_owned_bytes(authored: &[AuthoredMutation], capacity: usize) -> Result<u64, WorkError> {
    let vector_bytes = capacity.checked_mul(size_of::<AuthoredMutation>()).map(usize_to_u64).ok_or(WorkError::Overflow)?;
    authored.iter().try_fold(vector_bytes, |owned, mutation| {
        owned.checked_add(mutation.logical_owned_bytes()).ok_or(WorkError::Overflow)
    })
}

enum AuthoredTransactionInput<'a> {
    Owned { mutations: std::vec::IntoIter<AuthoredMutation>, allocation: KernelOwnedAllocation, capacity: usize, },
    Borrowed(&'a [AuthoredMutation]),
}

impl AuthoredTransactionInput<'_> {
    fn mutations(&self) -> &[AuthoredMutation] {
        match self { Self::Owned { mutations, .. } => mutations.as_slice(), Self::Borrowed(mutations) => mutations, }
    }

    fn capacity(&self) -> usize {
        match self { Self::Owned { capacity, .. } => *capacity, Self::Borrowed(mutations) => mutations.len(), }
    }

    fn retire(self, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        match self {
            Self::Owned { mutations, allocation, capacity } => {
                drop(mutations);
                let backing = match &allocation {
                    KernelOwnedAllocation::Standalone => { usize_to_u64(capacity * size_of::<AuthoredMutation>()) }
                    #[cfg(not(target_arch = "wasm32"))]
                    KernelOwnedAllocation::NativeOriginal(guard) => guard.bytes(),
                };
                retire_allocation(allocation, backing, accounting)
            }
            Self::Borrowed(_) => Ok(()),
        }
    }
}

fn mutation_path_owned_bytes(mutation: &Mutation) -> u64 {
    mutation.paths().map_or(0, |[first, second]| {
        let first_bytes = first.retained_bytes();
        if std::ptr::eq(first, second) { first_bytes } else { first_bytes + second.retained_bytes() }
    })
}

fn authored_mutation_operation_count(mutation: &AuthoredMutation) -> usize {
    match mutation {
        AuthoredMutation::CreateFile { bytes, .. } => {
            usize::from(bytes.len() > crate::kernel::MAXIMUM_INLINE_FILE_BYTES) + 1
        }
        AuthoredMutation::CreateFileFromContent { content, .. } => {
            usize::from(content.logical_bytes != 0) + 1
        }
        AuthoredMutation::Reidentify { .. }
        | AuthoredMutation::Write { .. }
        | AuthoredMutation::WriteFromContent { .. }
        | AuthoredMutation::CreateDirectory { .. }
        | AuthoredMutation::CreateSymbolicLink { .. }
        | AuthoredMutation::CreateEmptySpecial { .. }
        | AuthoredMutation::CreateDevice { .. }
        | AuthoredMutation::CreateReparsePoint { .. }
        | AuthoredMutation::Remove { .. }
        | AuthoredMutation::Rename { .. }
        | AuthoredMutation::HardLink { .. }
        | AuthoredMutation::SetMetadata { .. }
        | AuthoredMutation::Resize { .. }
        | AuthoredMutation::ZeroRange { .. }
        | AuthoredMutation::Preallocate { .. }
        | AuthoredMutation::CloneRange(_) => 1,
    }
}

#[cfg(not(target_arch = "wasm32"))]
type NameOperationsAllocation = NativeAllocation;
#[cfg(target_arch = "wasm32")]
type NameOperationsAllocation = ();

fn name_path_error(error: crate::kernel::NamespacePathError) -> FsError {
    match error {
        crate::kernel::NamespacePathError::Work(error) => FsError::Work(error),
        crate::kernel::NamespacePathError::Cancelled => FsError::Cancelled(CancellationError),
        error => FsError::Path(PathLookupError::NamespacePath(error)),
    }
}

fn name_cancellation<'a>(accounting: &KernelAccounting<'a>) -> &'a CancellationToken {
    match accounting {
        KernelAccounting::Standalone { cancellation, .. }
        | KernelAccounting::BorrowedStandalone { cancellation, .. } => cancellation,
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { cancellation, .. } => cancellation,
    }
}

fn name_fold_key(
    name: &LogicalName, accounting: &mut KernelAccounting<'_>,
) -> Result<(Vec<u8>, KernelOwnedAllocation), FsError> {
    let cancellation = name_cancellation(accounting);
    if name.encoding() == NameEncoding::Utf8 {
        let text = std::str::from_utf8(name.as_bytes()).map_err(|_| FsError::NonNormalizedName)?;
        return types::lowercase_unicode_with_accounting(text, accounting, cancellation);
    }
    let delta = WorkCounters { bytes_copied: name.as_bytes().len() as u64, ..WorkCounters::UNCHARGED };
    accounting.admit(delta)?;
    let requested = name.as_bytes().len() as u64;
    let mut allocation = accounting.claim_owned(requested, u64::from(requested != 0))?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(name.as_bytes().len()).map_err(|_| FsError::PendingMutationAllocationFailed)?;
    accounting.reconcile_owned(&mut allocation, requested, bytes.capacity() as u64)?;
    bytes.extend_from_slice(name.as_bytes());
    accounting.record_completed(delta)?;
    Ok((bytes, allocation))
}

fn name_fold_equal(
    left: &LogicalName, right: &LogicalName, accounting: &mut KernelAccounting<'_>,
) -> Result<bool, FsError> {
    let left = name_fold_key(left, accounting)?;
    let right = name_fold_key(right, accounting)?;
    let equal = left.0 == right.0;
    let left_bytes = left.0.capacity() as u64;
    retire_name_owned(left, left_bytes, accounting)?;
    let right_bytes = right.0.capacity() as u64;
    retire_name_owned(right, right_bytes, accounting)?;
    Ok(equal)
}

fn retire_name_owned<T>(
    owned: (T, KernelOwnedAllocation), bytes: u64, accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> { let (data, mut guard) = owned; drop(data); accounting.release_owned(&mut guard, bytes) }

fn pending_binding_path<'a>(
    components: &[LogicalName], pending: &'a [(NamespacePath, FileId)],
    case_sensitivity: CaseSensitivity,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Option<(&'a NamespacePath, FileId)>, FsError> {
    let Some((name, parent)) = components.split_last() else { return Ok(None); };
    for (path, id) in pending.iter().rev() {
        let Some((other_parent, other_name)) = path.split_last() else { continue; };
        if parent != other_parent { continue; }
        let same = match case_sensitivity {
            CaseSensitivity::Sensitive => name == other_name,
            CaseSensitivity::ProfileFolded => name_fold_equal(name, other_name, accounting)?,
        };
        if same { return Ok(Some((path, *id))); }
    }
    Ok(None)
}

fn removed_binding_id(
    path: &NamespacePath,
    removed: &[(NamespacePath, FileId)], accounting: &mut KernelAccounting<'_>,
) -> Result<Option<FileId>, FsError> {
    Ok(pending_binding_path(path.components(), removed, CaseSensitivity::ProfileFolded, accounting)? .map(|(_, id)| id))
}

/// Stable result positions for an authored transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthoredTransactionResult {
    /// One entry per authored operation; create operations contain their new
    pub created_file_ids: Vec<Option<FileId>>,
}

/// Stable result positions plus the terminal outcome of one authored live transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthoredLiveMutationResult {
    /// One entry per authored operation; create operations contain their new
    pub created_file_ids: Vec<Option<FileId>>,
    /// Terminal direct-live publication outcome.
    pub outcome: LiveMutationOutcome,
}

/// Authenticated content staged from a bounded stream without materializing the
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StagedContent {
    /// Root of the immutable chunk/index closure.
    root: ObjectId,
    /// Exact logical source length.
    logical_bytes: u64,
}

impl StagedContent {
    #[cfg(feature = "s3-http")]
    pub(crate) const fn from_canonical_parts(root: ObjectId, logical_bytes: u64) -> Self {
        Self {
            root,
            logical_bytes,
        }
    }

    /// Authenticated immutable blob-root identity created by this SDK.
    #[must_use]
    pub const fn root(&self) -> ObjectId {
        self.root
    }

    /// Exact staged logical byte length authenticated by the blob build.
    #[must_use]
    pub const fn logical_bytes(&self) -> u64 {
        self.logical_bytes
    }
}

/// One directory entry paired with its path-independent authenticated record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryRecordEntry {
    /// Exact canonical name.
    pub name: LogicalName,
    /// Complete child file record from the shared file-table frontier.
    pub record: FileRecord,
    /// Complete authenticated metadata referenced by `record`.
    pub metadata: FileMetadata,
}

/// One page-efficient directory listing for native projection callbacks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryRecordPage {
    /// Ordered entries strictly after the supplied cursor.
    pub entries: Vec<DirectoryRecordEntry>,
    /// Whether at least one additional entry exists.
    pub has_more: bool,
}

/// One bounded directory binding page requested from a [`PinnedReader`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryPageRequest {
    /// Exact directory namespace path.
    pub path: NamespacePath,
    /// Exclusive canonical name cursor.
    pub after: Option<LogicalName>,
    /// Maximum entries returned for this page.
    pub maximum_entries: u32,
}

/// One present no-follow path result with authenticated metadata decoded in
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PathMetadataLookup {
    /// Path-independent authenticated file record.
    pub record: FileRecord,
    /// Complete metadata referenced by `record`.
    pub metadata: FileMetadata,
}

/// Exact creation/replacement precondition for one named attribute write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NamedAttributeWriteMode {
    /// Insert when absent and replace when present.
    Upsert,
    /// Require authenticated absence.
    Create,
    /// Require authenticated presence.
    Replace,
}

/// Semantic result of publishing one private checkout overlay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckoutCommitOutcome {
    /// This call durably published the candidate generation.
    Committed {
        /// Published content-addressed generation.
        generation_id: GenerationId,
        /// Authority head produced by the durable append.
        head: Head,
    },
    /// The same operation identity and fingerprint was already durable.
    AlreadyCommitted {
        /// Previously published content-addressed generation.
        generation_id: GenerationId,
        /// Authority head at the original durable append.
        head: Head,
    },
    /// Another operation advanced the authority first.
    Conflict {
        /// Linearizable authority head observed by publication.
        actual: Head,
    },
    /// The checkout's writer epoch is stale.
    Fenced {
        /// Active authority epoch.
        actual_epoch: Epoch,
    },
    /// The operation identity was already bound to another fingerprint.
    IdempotencyConflict {
        /// Fingerprint durably bound to the reused operation identity.
        committed_fingerprint: Digest,
    },
}

/// Receipt-bearing facade result.
pub type FsResult<T> = MeasuredResult<FsReceipt<T>, FsError>;

/// One path-independent file-record change between immutable generations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileRecordChange {
    /// Stable file identity.
    pub file_id: FileId,
    /// Complete record in the earlier generation.
    pub before: Option<FileRecord>,
    /// Complete record in the later generation.
    pub after: Option<FileRecord>,
}

/// One exact name binding change within a stable directory identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryBindingChange {
    /// Stable parent directory identity.
    pub directory_id: FileId,
    /// Exact profile-specific component.
    pub name: LogicalName,
    /// Binding in the earlier generation.
    pub before: Option<crate::kernel::TreeEntry>,
    /// Binding in the later generation.
    pub after: Option<crate::kernel::TreeEntry>,
}

/// Bounded Merkle-aware semantic diff between two immutable generations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenerationDiff {
    /// Content, metadata, kind, link-count, or directory-root changes by file identity.
    pub files: Vec<FileRecordChange>,
    /// Added, removed, renamed, or retargeted names by parent directory identity.
    pub bindings: Vec<DirectoryBindingChange>,
    /// Additional changes exist beyond the caller's single total bound.
    pub truncated: bool,
}

/// Terminal result of preparing an unpublished two-parent merge generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MergePreparation {
    /// The checkout now holds this candidate and may commit or discard it.
    Prepared {
        /// Authenticated two-parent generation identity.
        generation_id: GenerationId,
    },
    /// No checkout state changed; exact conflicts are bounded by the caller.
    Conflicted {
        /// Stable conflicting regions.
        conflicts: Vec<MergeConflict>,
        /// Additional conflicts exceeded the supplied bound.
        truncated: bool,
    },
}

/// Fail-closed embedded facade errors.
#[derive(Debug, Error, strum::IntoStaticStr)]
pub enum FsError {
    /// Operation was cancelled before further facade work began.
    #[error(transparent)]
    Cancelled(#[from] CancellationError),
    /// A scoped original native accounting operation retained its typed cause.
    #[cfg(not(target_arch = "wasm32"))]
    #[error(transparent)]
    NativeKernel(#[from] NativeTreeMaterializationError),
    /// The caller's original frozen source identity or epoch diverged before sealing.
    #[error("frozen source snapshot is stale")]
    SourceSnapshotStale,
    /// The original native owner callback rejected the operation before publication.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("native owner guard rejected the operation")]
    NativeOwnerGuardRejected,
    /// An open handle's bounded proof, parent vector or collection-root registration could not allocate.
    #[error("filesystem handle allocation failed")]
    HandleAllocationFailed,
    /// The actual encoder buffer allocation failed without losing its allocator cause.
    #[error("filesystem encoding allocation failed: {0}")]
    EncodedAllocationFailed(#[source] std::collections::TryReserveError),
    /// Volume configuration is invalid.
    #[error(transparent)]
    Config(#[from] VolumeConfigError),
    /// Durable local Stream initialization or recovery failed.
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[error(transparent)]
    LocalStream(#[from] acyclic_stream::LocalStreamError),
    /// Durable local Objects initialization or recovery failed.
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[error(transparent)]
    LocalObjects(#[from] acyclic_objects::v1::local::LocalOpenError),
    /// The canonical filesystem Objects bucket could not be opened.
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[error("local filesystem Objects bucket initialization failed: {0}")]
    LocalObjectsBucket(acyclic_objects::v1::Error),
    /// Local provider root setup failed before either canonical provider opened.
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[error("local filesystem root setup failed: {0}")]
    LocalRoot(std::io::Error),
    /// The private spill file for pre-publication staging could not be created.
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[error("local staging spill file setup failed: {0}")]
    LocalStaging(std::io::Error),
    /// A live process-local engine already owns this root with different limits.
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[error("local filesystem root is already open with different options")]
    LocalOptionsConflict,
    /// The bounded local root initialization worker could not complete.
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[error("local filesystem initialization worker is unavailable")]
    LocalInitializationWorker,
    /// Immutable-object accelerator bounds are invalid.
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[error(transparent)]
    ObjectCacheConfig(#[from] crate::cache::ObjectCacheConfigError),
    /// Checkout behavior is contradictory.
    #[error(transparent)]
    CheckoutMode(#[from] CheckoutModeError),
    /// Requested lifecycle is not guaranteed by the selected backends.
    #[error("durable volume requested from non-durable embedded backends")]
    UnsupportedDurability,
    /// A new name collides with an existing sibling under the volume's
    #[error("name collides with an existing entry under the volume's case-folding policy")]
    NameCollision,
    /// A caller-derived identity already names different authenticated state.
    #[error("file identity collides with different authenticated content")]
    FileIdentityCollision,
    /// A new name is not canonical Unicode NFC, required by the volume's
    #[error("name is not normalized to NFC as required by the volume's unicode policy")]
    NonNormalizedName,
    /// The facade does not yet expose this checkout behavior.
    #[error("checkout behavior is not implemented by this facade")]
    UnsupportedCheckout,
    /// Direct-live mutation requires a serialized-authority volume.
    #[error("direct-live mutation requires serialized-authority concurrency")]
    LiveRequiresSerializedAuthority,
    /// Another writer changed the authority before exclusive ownership could be acquired.
    #[error("exclusive writer ownership raced with authority head {actual:?}")]
    ExclusiveWriterConflict {
        /// Linearizable head that defeated this acquisition.
        actual: Head,
    },
    /// A direct-live operation requires a positive safe-retry bound.
    #[error("direct-live retry bound must be positive")]
    ZeroLiveRetryLimit,
    /// Direct-live retry observations violated the canonical transition order.
    #[error(transparent)]
    LiveRetry(#[from] LiveRetryError),
    /// A direct-live checkout already has an unresolved candidate operation.
    #[error("direct-live checkout already has a pending operation")]
    PendingLiveMutation,
    /// Live advancement would cross an exact observed or mutated region.
    #[error("live advancement conflicts with an observed or mutated region")]
    LiveConflict {
        /// Bounded region-specific conflicts.
        conflicts: Vec<RebaseConflict>,
        /// Whether additional conflicts were omitted by the configured bound.
        truncated: bool,
    },
    /// Native live advancement keeps its exact conflict payload on the
    #[cfg(not(target_arch = "wasm32"))]
    #[error("live advancement conflicts with an observed or mutated region")]
    NativeLiveConflict {
        /// Bounded region-specific conflicts.
        conflicts: Vec<RebaseConflict>,
        /// Whether additional conflicts were omitted by the configured bound.
        truncated: bool,
        /// Retires only after conflict names and vector backing are destroyed.
        allocation: NativeAllocation,
    },
    /// Authority storage failed.
    #[error(transparent)]
    Authority(AuthorityStoreError),
    /// Immutable object storage failed.
    #[error(transparent)]
    Object(#[from] ObjectStoreError),
    /// Generation closure authentication failed.
    #[error(transparent)]
    Closure(#[from] ClosureError),
    /// Canonical filesystem data is malformed or unsupported.
    #[error(transparent)]
    Decode(#[from] CanonicalDecodeError),
    /// Canonical volume creation data is malformed or unsupported.
    #[error(transparent)]
    VolumeCreated(#[from] VolumeCreatedError),
    /// Canonical generation-retention fact is invalid.
    #[error(transparent)]
    RetentionCreated(#[from] RetentionCreatedError),
    /// Exact namespace-path resolution failed.
    #[error(transparent)]
    Path(#[from] PathLookupError),
    /// Bounded authenticated directory pagination failed.
    #[error(transparent)]
    Directory(#[from] DirectoryReadError),
    /// Bounded authenticated file-record pagination failed.
    #[error(transparent)]
    FileRecord(#[from] FileRecordReadError),
    /// Composed sparse regular-file range read failed.
    #[error(transparent)]
    FileRead(#[from] FileRangeReadError),
    /// Bounded authenticated blob construction failed.
    #[error(transparent)]
    BlobBuild(#[from] BlobBuildError),
    /// Authenticated blob-range reading failed.
    #[error(transparent)]
    BlobRead(#[from] BlobReadError),
    /// Authenticated named-attribute lookup failed.
    #[error(transparent)]
    AttributeLookup(#[from] AttributeLookupError),
    /// Authenticated named-attribute pagination failed.
    #[error(transparent)]
    AttributeList(#[from] AttributeListError),
    /// Sparse named-attribute path-copy failed.
    #[error(transparent)]
    AttributeMutation(#[from] AttributeMutationError),
    /// Tiny regular-file payload exceeded its canonical bound.
    #[error(transparent)]
    InlineFile(#[from] InlineFileDataError),
    /// Requested path is authentically absent.
    #[error("filesystem path does not exist")]
    NotFound,
    /// Requested operation requires a directory.
    #[error("filesystem path is not a directory")]
    NotDirectory,
    /// Requested operation requires a symbolic link.
    #[error("filesystem path is not a symbolic link")]
    NotSymbolicLink,
    /// Requested operation requires an opaque Windows reparse point.
    #[error("filesystem path is not a Windows reparse point")]
    NotReparsePoint,
    /// Special-file creation selected a kind incompatible with its payload.
    #[error("filesystem special-file kind is incompatible with the requested payload")]
    InvalidSpecialKind,
    /// The configured volume profile cannot represent this kind exactly.
    #[error("filesystem kind is unsupported by the configured volume profile")]
    UnsupportedFileKind,
    /// A directory binding and file-table record disagree or are missing.
    #[error("directory entry does not have a matching authenticated file record")]
    InvalidDirectoryRecord,
    /// Export manifest does not exactly match its authenticated closure.
    #[error("generation export manifest does not match the imported closure")]
    InvalidExportManifest,
    /// Resumable generation transfer validation or paging failed.
    #[error(transparent)]
    Transfer(#[from] GenerationTransferError),
    /// Checkout dependency capture failed.
    #[error(transparent)]
    Dependency(#[from] DependencyError),
    /// Exact authenticated dependency probing failed.
    #[error(transparent)]
    Probe(#[from] AuthenticatedProbeError),
    /// Observation-safe rebase classification failed.
    #[error(transparent)]
    Rebase(#[from] RebaseError<AuthenticatedProbeError>),
    /// One ordered generation mutation failed.
    #[error(transparent)]
    Mutation(#[from] GenerationMutationError),
    /// Path-independent open-file mutation failed.
    #[error(transparent)]
    DetachedMutation(#[from] RegularMutationError),
    /// Immutable checkpoint construction failed.
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
    /// Whole-closure generation publication failed.
    #[error(transparent)]
    Publication(#[from] PublicationError),
    /// Authority exists but has no volume creation fact.
    #[error("volume authority has no creation fact")]
    EmptyAuthority,
    /// Authority records do not contain the expected volume event.
    #[error("volume authority history has an invalid event shape")]
    InvalidAuthorityHistory,
    /// Authenticated data belongs to another volume.
    #[error("authenticated data belongs to another volume")]
    VolumeMismatch,
    /// Concurrent creation/publication produced a semantic conflict.
    #[error("volume creation was rejected by authority state")]
    CreationRejected,
    /// The named workspace has been terminally deleted.
    #[error("workspace is deleted")]
    WorkspaceDeleted,
    /// Writable overlays must be opened from the current authority head.
    #[error("writable private checkout requires a head selector")]
    WritableCheckoutRequiresHead,
    /// Mutation was requested from a checkout that cannot mutate.
    #[error("checkout does not admit private overlay mutation")]
    MutationNotAllowed,
    /// Commit was requested without a changed candidate generation.
    #[error("checkout has no pending mutation to commit")]
    NoPendingMutations,
    /// Explicit head refresh requires manual checkout consistency.
    #[error("checkout does not admit explicit head refresh")]
    RefreshNotAllowed,
    /// A dirty overlay must be safely rebased or discarded before refresh.
    #[error("pending overlay mutations require safe rebase before refresh")]
    PendingMutationsRequireRebase,
    /// Uncommitted operations exceed the volume's atomic replay bound.
    #[error("checkout pending mutation count exceeds the volume limit")]
    TooManyPendingMutations,
    /// Caller-supplied mutation storage exceeds the admitted persistent bound.
    #[error("mutation input capacity exceeds the volume limit")]
    ExcessiveMutationCapacity,
    /// Retaining a bounded replay log failed.
    #[error("pending mutation replay log allocation failed")]
    PendingMutationAllocationFailed,
    /// A prepared two-parent merge must be committed or discarded before mutation/rebase.
    #[error("prepared merge must be committed or discarded before further mutation")]
    PreparedMergePending,
    /// Diff roots or persistent page structure do not match the admitted format.
    #[error("generation diff structure is invalid")]
    InvalidDiff,
    /// Bounded diff result allocation failed.
    #[error("generation diff allocation failed")]
    DiffAllocationFailed,
    /// Bounded local live-set allocation failed before physical collection.
    #[error("local garbage-collection live-set allocation failed")]
    GarbageCollectionAllocationFailed,
    /// A three-way merge exceeded its admitted change frontier.
    #[error("three-way merge exceeds its change bound")]
    MergeChangeLimit,
    /// The selected second parent is not the current authority head.
    #[error("merge second parent must be the current authority head")]
    MergeParentNotHead,
    /// Sparse file-table merge application failed.
    #[error(transparent)]
    MergeFileTable(#[from] FileTableMutationError),
    /// Sparse directory-tree merge application failed.
    #[error(transparent)]
    MergeTree(#[from] TreeMutationError),
    /// Exact work overflowed or exceeded the admitted budget.
    #[error(transparent)]
    Work(#[from] WorkError),
    /// A completed standalone root read cannot be combined into one exact
    #[error("completed generation-root read work cannot be combined: {error}")]
    RootReadWorkMerge {
        prior: WorkCounters, backend: WorkCounters,
        #[source]
        error: WorkError,
    },
}

impl From<AuthorityStoreError> for FsError {
    fn from(error: AuthorityStoreError) -> Self {
        match error {
            // A deleted workspace's authority is released for good; reading
            AuthorityStoreError::Retired => Self::WorkspaceDeleted,
            error => Self::Authority(error),
        }
    }
}

impl<A, O> Fs<A, O> {
    /// Composes explicit authority and object backends without another state store.
    #[must_use]
    pub fn new(authority: A, objects: O, capabilities: EmbeddedCapabilities) -> Self {
        Self::new_in_namespace(authority, objects, capabilities, [0; 16])
    }

    /// Composes explicit backends in one stable workspace-identity namespace.
    #[must_use]
    pub fn new_in_namespace(
        authority: A,
        objects: O,
        capabilities: EmbeddedCapabilities,
        workspace_namespace: [u8; 16],
    ) -> Self {
        Self::new_with_path_index(
            authority,
            objects,
            capabilities,
            workspace_namespace,
            Arc::new(crate::path_index::MemoryGenerationPathIndex::default()),
            #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
            None,
        )
    }

    fn new_with_path_index(
        authority: A,
        objects: O,
        capabilities: EmbeddedCapabilities,
        workspace_namespace: [u8; 16],
        path_index: Arc<dyn crate::path_index::GenerationPathIndex>,
        #[cfg(all(feature = "local", not(target_arch = "wasm32")))] local_root_lifecycle: Option<
            OwnershipAnchor,
        >,
    ) -> Self {
        Self {
            inner: Arc::new(FsInner {
                authority,
                objects,
                capabilities,
                workspace_namespace,
                path_index,
                #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
                _local_root_lifecycle: local_root_lifecycle,
            }),
        }
    }
}

impl<A, S> Fs<A, crate::cache::CachedObjectStore<S>> {
    /// Returns exact current and cumulative observations from the disposable
    pub fn object_cache_stats(&self) -> Result<crate::cache::ObjectCacheStats, ObjectStoreError> {
        self.inner.objects.stats()
    }

    /// Discards all resident accelerated bytes without changing authority or
    pub fn clear_object_cache(&self) -> Result<(), ObjectStoreError> {
        self.inner.objects.clear()
    }
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
impl Fs<LocalAuthorityBackend, LocalObjectBackend> {
    /// Observes the disposable local immutable-object cache.
    pub fn object_cache_stats(&self) -> Result<crate::cache::ObjectCacheStats, ObjectStoreError> {
        self.inner.objects.inner().stats()
    }

    /// Drops cached bytes without changing persistent object storage.
    pub fn clear_object_cache(&self) -> Result<(), ObjectStoreError> {
        self.inner.objects.inner().clear()
    }
}

#[cfg(all(feature = "memory", feature = "distributed"))]
impl
    Fs<
        crate::distributed::StreamAuthorityStore<acyclic_stream::MemoryStream>,
        crate::LogicalObjectStore<acyclic_objects::v1::MemoryObjects>,
    >
{
    /// Creates the deterministic infrastructure-free composition from the exact public reference
    #[must_use]
    pub fn memory() -> Self {
        let stream = std::sync::Arc::new(acyclic_stream::MemoryStream::default());
        let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        Self::from_memory_providers(stream, std::sync::Arc::new(objects), bucket)
    }

    /// Composes Filesystem over caller-owned public reference providers.
    #[must_use]
    pub fn from_memory_providers(
        stream: std::sync::Arc<acyclic_stream::MemoryStream>,
        objects: std::sync::Arc<acyclic_objects::v1::MemoryObjects>,
        bucket: acyclic_objects::v1::wire::BucketRef,
    ) -> Self {
        Self::new(
            crate::distributed::StreamAuthorityStore::new(stream),
            crate::LogicalObjectStore::new(objects, bucket),
            EmbeddedCapabilities::MEMORY,
        )
    }
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
impl Fs<LocalAuthorityBackend, LocalObjectBackend> {
    /// Observes release of every provider derived from this local root.
    #[doc(hidden)]
    #[must_use]
    pub fn local_root_release_barrier(
        &self,
    ) -> Option<acyclic_native_runtime::OwnershipReleaseBarrier> {
        self.inner
            ._local_root_lifecycle
            .as_ref()
            .map(OwnershipAnchor::release_barrier)
    }

    /// Opens the infrastructure-free durable local composition.
    pub async fn local(options: LocalOptions) -> Result<Self, FsError> {
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.local",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
        );
        let worker_context = span.clone();
        crate::obs::outcome_on(
            &span,
            crate::obs::in_span(&span, async move {
                static REGISTRY: OnceLock<tokio::sync::Mutex<LocalRootRegistry>> = OnceLock::new();
                let requested_root = options.root.clone();
                let root = tokio::task::spawn_blocking(crate::obs::in_context(
                    worker_context.clone(),
                    move || {
                        std::fs::create_dir_all(&requested_root)?;
                        std::fs::canonicalize(requested_root)
                    },
                ))
                .await
                .map_err(|_| FsError::LocalInitializationWorker)?
                .map_err(FsError::LocalRoot)?;
                let mut options = options;
                options.root = root.clone();
                let registry =
                    REGISTRY.get_or_init(|| tokio::sync::Mutex::new(LocalRootRegistry::new()));
                let opening = {
                    let mut registrations = registry.lock().await;
                    if let Some(existing) = registrations.get(&root)
                        && let Some(inner) = existing.live.upgrade()
                    {
                        if existing.options != options {
                            return Err(FsError::LocalOptionsConflict);
                        }
                        return Ok(Self { inner });
                    }
                    prepare_local_root_open(&mut registrations, &root, &options)
                };
                // Opening a root may wait for old provider handles or native I/O to finish. Keep that
                let _opening = opening.lock().await;
                let lifecycle = {
                    let registrations = registry.lock().await;
                    let existing = registrations
                        .get(&root)
                        .ok_or(FsError::LocalInitializationWorker)?;
                    if let Some(inner) = existing.live.upgrade() {
                        if existing.options != options {
                            return Err(FsError::LocalOptionsConflict);
                        }
                        return Ok(Self { inner });
                    }
                    Arc::clone(&existing.lifecycle)
                };
                let ownership = lifecycle.lock_owned().await;
                let open_options = options.clone();
                let fs = run_local_initialization(
                    ownership,
                    worker_context.clone(),
                    move |ownership| async move {
                        let ownership = OwnershipAnchor::new(ownership);
                        Self::open_local_unshared(open_options, Some(ownership)).await
                    },
                )
                .await??;
                let mut registrations = registry.lock().await;
                let existing = registrations
                    .get_mut(&root)
                    .ok_or(FsError::LocalInitializationWorker)?;
                existing.options = options;
                existing.live = Arc::downgrade(&fs.inner);
                Ok(fs)
            })
            .await,
        )
    }

    async fn open_local_unshared(
        options: LocalOptions,
        lifecycle: Option<OwnershipAnchor>,
    ) -> Result<Self, FsError> {
        let LocalOptions {
            root,
            stream,
            objects,
            object_cache,
        } = options;
        // The Stream and Objects providers own disjoint directories, so their
        let open_stream = async {
            Ok::<_, FsError>(std::sync::Arc::new(match &lifecycle {
                Some(ownership) => {
                    acyclic_stream::LocalStream::open_with_ownership_anchor(
                        root.join("stream"),
                        stream,
                        ownership.clone(),
                    )
                    .await?
                }
                None => acyclic_stream::LocalStream::open(root.join("stream"), stream).await?,
            }))
        };
        let open_objects = async {
            let maximum_object_bytes = objects.maximum_object_bytes;
            let objects = std::sync::Arc::new(match &lifecycle {
                Some(ownership) => {
                    acyclic_objects::v1::local::LocalObjects::open(
                        root.join("objects"),
                        objects,
                        Some(ownership.clone()),
                    )
                    .await?
                }
                None => {
                    acyclic_objects::v1::local::LocalObjects::open(root.join("objects"), objects, None)
                        .await?
                }
            });
            let bucket = acyclic_objects::v1::wire::BucketRef {
                name: "filesystem-objects".to_owned(),
            };
            match objects
                .head_bucket(acyclic_objects::v1::wire::HeadBucketRequest {
                    bucket: Some(bucket.clone()),
                })
                .await
            {
                Ok(_) => {}
                Err(error) if error.code == acyclic_objects::v1::wire::ErrorCode::NotFound => {
                    objects
                        .create_bucket(acyclic_objects::v1::wire::CreateBucketRequest {
                            name: bucket.name.clone(),
                            mutation: None,
                        })
                        .await
                        .map_err(FsError::LocalObjectsBucket)?;
                }
                Err(error) => return Err(FsError::LocalObjectsBucket(error)),
            }
            let objects = crate::cache::CachedObjectStore::new(
                crate::LogicalObjectStore::new(objects, bucket)
                    .with_object_limit(maximum_object_bytes)?,
                object_cache,
            )?;
            crate::staged_objects::StagedObjects::open(objects, root.clone())
                .await
                .map_err(FsError::LocalStaging)
        };
        let (stream, objects) = futures::future::join(open_stream, open_objects).await;
        let (stream, objects) = (stream?, objects?);
        Ok(Self::new_with_path_index(
            crate::distributed::StreamAuthorityStore::new(stream),
            objects,
            EmbeddedCapabilities {
                durable: cfg!(any(unix, windows)),
            },
            [0; 16],
            Arc::new(crate::path_index::NativeGenerationPathIndex::new(
                root.join("generation-path-index-v1"),
            )),
            lifecycle,
        ))
    }

    /// Reclaims every stored object nothing live reaches, while the root
    pub async fn collect_local_garbage(
        &self,
        core_state: Option<&crate::LocalCoreStateStore>,
        cancellation: &CancellationToken,
    ) -> Result<LocalGarbageCollection, FsError> {
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.collect_local_garbage",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty
        );
        let result = crate::obs::in_span(&span, async move {
            if let Some(core_state) = core_state {
                let store_error = |error: crate::LocalCoreStateStoreError| {
                    crate::FsError::Object(crate::ObjectStoreError::Rejected(error.to_string()))
                };
                let records = core_state.workspace_records().await.map_err(store_error)?;
                let mut deleted = std::collections::BTreeSet::new();
                for workspace in records.keys() {
                    cancellation.check()?;
                    if self
                        .local_volume_deleted(workspace.volume_id(), cancellation)
                        .await?
                    {
                        deleted.insert(*workspace);
                    }
                }
                // Live descendants retain their deleted ancestors' lineage.
                let mut ancestors = std::collections::BTreeSet::new();
                for workspace in records.keys().filter(|id| !deleted.contains(id)) {
                    let mut pending = records.get(workspace).cloned().unwrap_or_default();
                    while let Some(parent) = pending.pop() {
                        if ancestors.insert(parent) {
                            pending.extend(records.get(&parent).cloned().unwrap_or_default());
                        }
                    }
                }
                for workspace in deleted.difference(&ancestors) {
                    core_state
                        .forget_workspace(*workspace)
                        .await
                        .map_err(store_error)?;
                }
                core_state.collect_lazy_nodes().await.map_err(store_error)?;
            }
            let collection = Arc::clone(self.inner.objects.collection().ok_or(FsError::Object(
                ObjectStoreError::Rejected("the local object store does not collect".to_owned()),
            ))?);
            let collecting = collection.begin().await;
            let candidates = self.local_object_candidates(cancellation).await?;
            let mut marker = crate::kernel::Marker::new(&self.inner.objects, cancellation);
            let shadow_limits = self
                .mark_local_authorities(&mut marker, cancellation)
                .await?;
            #[cfg(feature = "s3-http")]
            {
                let multipart = crate::active_s3_multipart_objects(
                    self.inner.authority.provider().as_ref(),
                    &self.inner.objects,
                    crate::FilesystemS3Limits::default(),
                    crate::S3MultipartRetentionLimits::default(),
                    WorkBudget::UNBOUNDED,
                    cancellation,
                )
                .await
                .map_err(|_| {
                    FsError::Object(ObjectStoreError::Rejected(
                        "active S3 multipart retention scan failed".to_owned(),
                    ))
                })?;
                marker.keep(multipart.value);
            }
            if let Some(core_state) = core_state {
                for (generation, config) in core_state.lazy_retained_generations().await.map_err(|error| {
                    FsError::Object(ObjectStoreError::Rejected(error.to_string()))
                })? {
                    cancellation.check()?;
                    marker.set_limits(DecodeLimits::for_volume(config));
                    marker.generation(generation).await.map_err(mark_error)?;
                }
                marker.set_limits(shadow_limits);
                for record in core_state.lazy_shadow_records().await.map_err(|error| {
                    FsError::Object(ObjectStoreError::Rejected(error.to_string()))
                })? {
                    marker.record(&record).await.map_err(mark_error)?;
                }
            }
            for held in collection.held() {
                match held {
                    crate::collection::Held::Checkout(held) => {
                        marker.set_limits(DecodeLimits::for_volume(held.config));
                        marker
                            .generation(GenerationId::new(held.base.digest))
                            .await
                            .map_err(mark_error)?;
                        marker.working(&held.working).await.map_err(mark_error)?;
                    }
                    crate::collection::Held::Record { record, config } => {
                        marker.set_limits(DecodeLimits::for_volume(config));
                        marker.record(&record).await.map_err(mark_error)?;
                    }
                }
            }
            marker.set_limits(shadow_limits);
            marker.lineage_roots().await.map_err(mark_error)?;
            let unmarked = candidates
                .into_iter()
                .filter(|(object, _)| !marker.marked().contains(object))
                .collect::<Vec<_>>();
            drop(marker);
            self.sweep_local_objects(&collecting, unmarked, cancellation)
                .await
        })
        .await;
        crate::obs::outcome_on(&span, result)
    }

    /// Every current filesystem key and its conditional-deletion validator.
    async fn local_object_candidates(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<Vec<(ObjectId, String)>, FsError> {
        let provider = self.inner.objects.inner().inner().provider();
        let bucket = self.inner.objects.inner().inner().bucket().clone();
        let mut continuation = String::new();
        let mut versions = Vec::new();
        loop {
            cancellation.check()?;
            let page = provider
                .list(acyclic_objects::v1::wire::ListObjectsRequest {
                    bucket: Some(bucket.clone()),
                    prefix: "fs/v1/".to_owned(),
                    page_size: 256,
                    continuation_token: continuation,
                    ..Default::default()
                })
                .await
                .map_err(FsError::LocalObjectsBucket)?;
            for entry in page.entries {
                let version = entry
                    .object
                    .ok_or(FsError::Object(ObjectStoreError::Corrupt))?;
                let object = crate::distributed::object_id_from_key(&entry.object_key)
                    .ok_or(FsError::Object(ObjectStoreError::Corrupt))?;
                versions.push((object, version.etag));
            }
            continuation = page.continuation_token;
            if continuation.is_empty() {
                return Ok(versions);
            }
        }
    }

    /// Deletes `unmarked` in batches, each under the collection gate, then
    async fn sweep_local_objects(
        &self,
        collecting: &crate::collection::Collecting,
        unmarked: Vec<(ObjectId, String)>,
        cancellation: &CancellationToken,
    ) -> Result<LocalGarbageCollection, FsError> {
        const BATCH: usize = 256;
        let provider = self.inner.objects.inner().inner().provider();
        let bucket = self.inner.objects.inner().inner().bucket().clone();
        let examined = u64::try_from(unmarked.len()).unwrap_or(u64::MAX);
        let mut candidates = unmarked
            .into_iter()
            .collect::<std::collections::HashMap<_, _>>();
        let mut removed = 0_u64;
        let objects = candidates.keys().copied().collect::<Vec<_>>();
        for batch in objects.chunks(BATCH) {
            cancellation.check()?;
            let (_gate, sweepable) = collecting.sweepable(batch.to_vec()).await;
            for object in sweepable {
                let Some(etag) = candidates.remove(&object) else {
                    continue;
                };
                collecting.sweeping(object);
                let deleted = provider
                    .delete(acyclic_objects::v1::wire::DeleteObjectRequest {
                        bucket: Some(bucket.clone()),
                        object_key: crate::distributed::object_key(object),
                        preconditions: Some(acyclic_objects::v1::wire::Preconditions {
                            condition: Some(
                                acyclic_objects::v1::wire::preconditions::Condition::IfMatch(etag),
                            ),
                        }),
                        mutation: None,
                    })
                    .await
                    .map_err(FsError::LocalObjectsBucket);
                // Also clear after an uncertain/partially completed sweep.
                self.inner.objects.inner().clear()?;
                if deleted?.existed {
                    removed = removed.saturating_add(1);
                }
            }
            // A cached copy of a swept object must not stand in for it.
            self.inner.objects.inner().clear()?;
        }
        let physical = provider
            .collect_garbage(u64::MAX)
            .await
            .map_err(FsError::LocalObjects)?;
        Ok(LocalGarbageCollection {
            examined,
            removed,
            segments_removed: physical.segments_removed,
            temporary_files_removed: physical.temporary_files_removed,
        })
    }

    /// Marks every live generation of every authority, releasing the
    async fn mark_local_authorities(
        &self,
        marker: &mut crate::kernel::Marker<'_, LocalObjectBackend>,
        cancellation: &CancellationToken,
    ) -> Result<DecodeLimits, FsError> {
        let mut widest = DecodeLimits::for_volume(VolumeConfig::portable(Lifecycle::Durable));
        let authorities = self
            .inner
            .authority
            .authorities(u32::MAX - 1)
            .await
            .map_err(FsError::from)?;
        for authority_id in authorities {
            match self
                .mark_local_authority(marker, authority_id, &mut widest, cancellation)
                .await
            {
                // Deleted and released since it was listed: nothing of it is
                Ok(()) | Err(FsError::WorkspaceDeleted) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(widest)
    }

    /// Marks one authority's live generations; see
    async fn mark_local_authority(
        &self,
        marker: &mut crate::kernel::Marker<'_, LocalObjectBackend>,
        authority_id: crate::foundation::AuthorityId,
        widest: &mut DecodeLimits,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        cancellation.check()?;
        let head = match self
            .inner
            .authority
            .head(authority_id, WorkBudget::UNBOUNDED, cancellation)
            .await
        {
            Ok(head) => head.value,
            // Released since it was listed.
            Err(failure) if matches!(failure.error, AuthorityStoreError::Retired) => return Ok(()),
            Err(failure) => return Err(failure.error.into()),
        };
        if head.sequence == Sequence::GENESIS {
            return Ok(());
        }
        let first = self
            .local_records(authority_id, Sequence::GENESIS, 1, cancellation)
            .await?
            .into_iter()
            .next()
            .ok_or(FsError::InvalidAuthorityHistory)?;
        if let Ok(created) = crate::kernel::decode_volume_created(&first.payload, MAXIMUM_VOLUME_EVENT_BYTES, &mut |_| Ok::<_, FsError>(())) {
            if volume_authority_id(created.volume_id) != authority_id {
                return Err(FsError::VolumeMismatch);
            }
            let limits = DecodeLimits::for_volume(created.config);
            *widest = widest_limits(*widest, limits);
            marker.set_limits(limits);
            self.mark_local_volume(marker, authority_id, created.volume_id, head, cancellation)
                .await?;
            return Ok(());
        }
        #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
        if let Ok(source_volume) = decode_source_volume(&first.payload, MAXIMUM_VOLUME_EVENT_BYTES)
        {
            if source_authority_id(source_volume) != authority_id {
                return Err(FsError::VolumeMismatch);
            }
            // Its facts name generations of its volume's own history.
            return Ok(());
        }
        let retained = decode_retention_created(&first.payload, MAXIMUM_VOLUME_EVENT_BYTES)?;
        // A retention lasts as long as the workspace it serves: a fork
        let owner = match retained.kind {
            RetentionKind::ForkBase => hex::decode(&retained.label)
                .ok()
                .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok())
                .map(VolumeId::from_bytes)
                .ok_or(FsError::InvalidAuthorityHistory)?,
            RetentionKind::Checkpoint | RetentionKind::Pin => retained.volume_id,
        };
        if self.local_volume_ended(owner, cancellation).await? {
            self.retire_local_authority(authority_id, cancellation)
                .await?;
            return Ok(());
        }
        let limits = DecodeLimits::for_volume(retained.config);
        *widest = widest_limits(*widest, limits);
        marker.set_limits(limits);
        marker
            .generation(GenerationId::new(retained.generation_root.digest))
            .await
            .map_err(mark_error)?;
        Ok(())
    }

    /// Marks every generation in one volume's history, or releases the
    async fn mark_local_volume(
        &self,
        marker: &mut crate::kernel::Marker<'_, LocalObjectBackend>,
        authority_id: crate::foundation::AuthorityId,
        volume_id: VolumeId,
        head: Head,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        const PAGE: u32 = 256;
        let latest = self
            .local_records(
                authority_id,
                Sequence::new(head.sequence.get().saturating_sub(1)),
                1,
                cancellation,
            )
            .await?;
        if let Some(latest) = latest.first()
            && let Ok(deleted) =
                decode_workspace_deleted(&latest.payload, MAXIMUM_VOLUME_EVENT_BYTES)
        {
            if deleted != volume_id {
                return Err(FsError::VolumeMismatch);
            }
            // A delete interrupted before it released the authority.
            return self
                .retire_local_authority(authority_id, cancellation)
                .await;
        }
        let mut after = Sequence::GENESIS;
        while after < head.sequence {
            let records = self
                .local_records(authority_id, after, PAGE, cancellation)
                .await?;
            let Some(last) = records.last() else {
                return Err(FsError::InvalidAuthorityHistory);
            };
            after = last.sequence;
            // Exactly the history as of the head read: a record appended
            for record in records
                .iter()
                .take_while(|record| record.sequence <= head.sequence)
            {
                let generation = generation_from_record(record, volume_id, WorkCounters::default())
                    .map_err(|failure| failure.error)?;
                marker
                    .generation(GenerationId::new(generation.digest))
                    .await
                    .map_err(mark_error)?;
            }
        }
        Ok(())
    }

    /// Up to `count` records of one authority after `after`.
    async fn local_records(
        &self,
        authority_id: crate::foundation::AuthorityId,
        after: Sequence,
        count: u32,
        cancellation: &CancellationToken,
    ) -> Result<Vec<crate::foundation::DurableCommit>, FsError> {
        Ok(self
            .inner
            .authority
            .replay(
                authority_id,
                after,
                ReplayLimit {
                    records: count,
                    payload_bytes: MAXIMUM_VOLUME_EVENT_BYTES.saturating_mul(u64::from(count)),
                },
                WorkBudget::UNBOUNDED,
                cancellation,
            )
            .await
            .map_err(|failure| FsError::from(failure.error))?
            .value)
    }

    /// Whether the workspace `volume_id` was deleted; one never created was
    pub(crate) async fn local_volume_deleted(
        &self,
        volume_id: VolumeId,
        cancellation: &CancellationToken,
    ) -> Result<bool, FsError> {
        match self
            .inner
            .authority
            .head(
                volume_authority_id(volume_id),
                WorkBudget::UNBOUNDED,
                cancellation,
            )
            .await
        {
            Err(failure) if matches!(failure.error, AuthorityStoreError::Missing) => Ok(false),
            _ => self.local_volume_ended(volume_id, cancellation).await,
        }
    }

    /// Whether the workspace `volume_id` ended: deleted, or never created.
    async fn local_volume_ended(
        &self,
        volume_id: VolumeId,
        cancellation: &CancellationToken,
    ) -> Result<bool, FsError> {
        let authority_id = volume_authority_id(volume_id);
        let head = match self
            .inner
            .authority
            .head(authority_id, WorkBudget::UNBOUNDED, cancellation)
            .await
        {
            Ok(head) => head.value,
            Err(failure)
                if matches!(
                    failure.error,
                    AuthorityStoreError::Retired | AuthorityStoreError::Missing
                ) =>
            {
                return Ok(true);
            }
            Err(failure) => return Err(failure.error.into()),
        };
        if head.sequence == Sequence::GENESIS {
            return Ok(false);
        }
        let latest = self
            .local_records(
                authority_id,
                Sequence::new(head.sequence.get().saturating_sub(1)),
                1,
                cancellation,
            )
            .await?;
        Ok(latest.first().is_some_and(|record| {
            decode_workspace_deleted(&record.payload, MAXIMUM_VOLUME_EVENT_BYTES)
                .is_ok_and(|deleted| deleted == volume_id)
        }))
    }

    async fn retire_local_authority(
        &self,
        authority_id: crate::foundation::AuthorityId,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        self.inner
            .authority
            .retire_authority(authority_id, WorkBudget::UNBOUNDED, cancellation)
            .await
            .map_err(|failure| FsError::from(failure.error))?;
        Ok(())
    }
}

/// Limits that admit every page either admits.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
fn widest_limits(left: DecodeLimits, right: DecodeLimits) -> DecodeLimits {
    DecodeLimits {
        maximum_object_bytes: left.maximum_object_bytes.max(right.maximum_object_bytes),
        maximum_name_bytes: left.maximum_name_bytes.max(right.maximum_name_bytes),
        maximum_page_items: left.maximum_page_items.max(right.maximum_page_items),
        maximum_page_bytes: left.maximum_page_bytes.max(right.maximum_page_bytes),
        maximum_page_height: left.maximum_page_height.max(right.maximum_page_height),
        maximum_visited_pages: left.maximum_visited_pages.max(right.maximum_visited_pages),
    }
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
fn mark_error(error: crate::kernel::MarkError) -> FsError {
    match error {
        crate::kernel::MarkError::Storage(error) => FsError::Object(error),
        crate::kernel::MarkError::Decode(reason) => FsError::Object(ObjectStoreError::Rejected(
            format!("a live object does not decode, so nothing was collected: {reason}"),
        )),
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    pub(crate) async fn observe_volume_operation(
        &self,
        volume_id: VolumeId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Option<DurableCommit>> {
        let receipt = self
            .inner
            .authority
            .find_operation(
                volume_authority_id(volume_id),
                operation_id,
                budget,
                cancellation,
            )
            .await
            .map_err(|failure| {
                OperationFailure::new(FsError::Authority(failure.error), *failure.work)
            })?;
        Ok(FsReceipt {
            value: receipt.value,
            work: receipt.work,
        })
    }

    pub(crate) async fn generation_parents<W: FsWork>(
        &self, volume: &Volume<A, O>, generation: GenerationId, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<(Vec<GenerationId>, KernelOwnedAllocation), WorkspaceError> {
        let mut accounting = budget.accounting(cancellation);
        let standalone = readonly_standalone_live(&accounting).is_some();
        let result = async {
            let decoded = read_generation_root_with_accounting(
                &self.inner.objects, ObjectId { kind: ObjectKind::GenerationRoot, digest: generation.digest() },
                volume.config, &mut accounting, cancellation,
            ).await.map_err(|error| if standalone { WorkspaceError::engine(error) } else { error.into() })?;
            if decoded.0.volume_id != volume.id { return Err(WorkspaceError::ForeignGeneration); }
            Ok((decoded.0.parents, decoded.1))
        }.await;
        budget.finish(result, &accounting)
    }

    /// Creates or recovers a named workspace under explicit work and cancellation.
    pub async fn create_workspace(
        &self, name: impl AsRef<str>, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> MeasuredResult<FsReceipt<crate::Workspace<A, O>>, WorkspaceError>
    {
        let lifecycle = if self.inner.capabilities.durable {
            Lifecycle::Durable
        } else {
            Lifecycle::Ephemeral
        };
        self.create_workspace_with_config(name, VolumeConfig::portable(lifecycle), budget, cancellation).await
    }


    /// Creates or recovers exact named semantics within one cumulative budget.
    pub async fn create_workspace_with_config(
        &self,
        name: impl AsRef<str>,
        config: VolumeConfig,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> MeasuredResult<FsReceipt<crate::Workspace<A, O>>, WorkspaceError>
    { self.create_workspace_with_config_operation(name, config, None, budget, cancellation) .await }

    /// Creates or recovers one original creation operation and its immutable
    pub async fn create_workspace_with_operation(
        &self,
        name: impl AsRef<str>,
        config: VolumeConfig, operation_id: OperationId, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> MeasuredResult<FsReceipt<crate::Generation<A, O>>, WorkspaceError>
    {

        let created = self.create_named_workspace(name, config, Some(operation_id), budget, cancellation).await?;
        let prior = created.work;
        let live = created.value.name.retained_bytes();
        let allowance = remaining_owned(prior, budget, live)
            .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
        let observed = self.read_creation_record(created.value.volume.id, allowance, cancellation).await
            .map_err(|failure| {
                merge_simultaneous_failure(prior, *failure.work, live, failure.error)
                    .map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from)
            })?;
        let creation = observed.value.0;
        let work = merge_simultaneous_work(prior, observed.work, live, budget)
            .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
        self.initial_workspace_generation(
            created.value, creation.initial_generation_root, work, budget, cancellation,
        ).await
    }

    /// Observes one original workspace creation without retrying its publication.
    pub async fn workspace_for_creation_operation(
        &self,
        name: impl AsRef<str>,
        config: VolumeConfig, original_operation_id: OperationId, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> MeasuredResult<FsReceipt<Option<crate::Generation<A, O>>>, WorkspaceError> {

        let named = self.workspace_name_identity(name, budget, cancellation)?;
        let (name, id) = named.value;
        let mut work = named.work;
        let live = name.retained_bytes();
        let allowance = remaining_owned(work, budget, live)
            .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
        let (creation, operation, nested) = match self.read_creation_record(
            id.volume_id(), allowance, cancellation,
        ).await {
            Ok(observed) => (observed.value.0, observed.value.1, observed.work), Err(failure) => {
                let failure = merge_simultaneous_failure(work, *failure.work, live, failure.error);
                if matches!(&failure.error, FsError::Authority(AuthorityStoreError::Missing)) {
                    return Ok(FsReceipt { value: None, work: *failure.work });
                }
                return Err(failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from));
            }
        };
        work = merge_simultaneous_work(work, nested, live, budget)
            .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
        authenticate_named_creation(
            &creation, operation, id.volume_id(), config, original_operation_id,
        ).map_err(|error| OperationFailure::new(WorkspaceError::from(error), work))?;
        validate_volume_capabilities(config, self.inner.capabilities)
            .map_err(|error| OperationFailure::new(WorkspaceError::from(error), work))?;
        let workspace = crate::Workspace {
            name, id, volume: Volume { fs: self.clone(), id: id.volume_id(), config },
            #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
            source: None,
        };
        self.initial_workspace_generation(
            workspace, creation.initial_generation_root, work, budget, cancellation,
        ).await.map(|receipt| FsReceipt { value: Some(receipt.value), work: receipt.work })
    }

    /// Creates or recovers the immutable initial generation on the original native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn create_workspace_with_operation_with_native_work(
        &self,
        name: &str, config: VolumeConfig, original_operation_id: OperationId, original: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<crate::Generation<A, O>, (WorkspaceError, Option<NativeAllocation>)> {
        let publication = |id, commit, _| async move {
            (self.inner.authority.create_authority_with_first_record_with_native_work(
                id, commit, original, cancellation,
            ).await.map_err(|error| WorkspaceError::from(FsError::from(error))), WorkCounters::UNCHARGED)
        };
        self.create_workspace_with_operation_with_native_work_value(
            name, config, original_operation_id, original, cancellation, publication,
        ).await.map_err(|error| (error, None))
    }

    /// Creates or recovers the immutable initial generation, sealing every real
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn create_workspace_with_operation_guarded_with_native_work<'original, E, F, Fut>(
        &self,
        name: &str,
        config: VolumeConfig,
        original_operation_id: OperationId, original: &'original NativeMaterializationWork,
        cancellation: &CancellationToken,
        before_publication: &mut F,
    ) -> Result<crate::Generation<A, O>, E>
    where
        E: From<FsError> + From<WorkspaceError> + Send + 'static,
        F: for<'m> FnMut(&'m NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        let publication = |id, commit, _| async move {
            (self.inner.authority.create_authority_with_first_record_guarded_with_native_work(
                id, commit, original, cancellation, before_publication,
            ).await, WorkCounters::UNCHARGED)
        };
        self.create_workspace_with_operation_with_native_work_value(
            name, config, original_operation_id, original, cancellation, publication,
        ).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn create_workspace_with_operation_with_native_work_value<E, P, Fut>(
        &self, name: &str, config: VolumeConfig, original_operation_id: OperationId,
        original: &NativeMaterializationWork, cancellation: &CancellationToken, publication: P,
    ) -> Result<crate::Generation<A, O>, E>
    where
        E: From<FsError> + From<WorkspaceError> + Send,
        P: FnOnce(crate::AuthorityId, ProposedCommit, WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = (Result<bool, E>, WorkCounters)> + ProviderTask,
    {

        cancellation.check().map_err(FsError::from)?;
        original.admit(WorkCounters::UNCHARGED).map_err(FsError::from)?;
        let name = crate::WorkspaceName::new(name, original, cancellation)?;
        let id = crate::WorkspaceId::derive(
            self.inner.workspace_namespace, &name, original, cancellation,
        )?;
        validate_volume_capabilities(config, self.inner.capabilities)?;
        let observed = self.read_creation_record(id.volume_id(), original, cancellation).await;
        let (creation, operation) = match observed {
            Ok(creation) => creation, Err(FsError::Authority(AuthorityStoreError::Missing)) => {
                let mut accounting = KernelAccounting::native(original, cancellation);
                let (_, initial_root) = self.create_volume_with_id_operation_with_accounting(
                    id.volume_id(), config, Some(original_operation_id), &mut accounting, cancellation, publication,
                ).await?;
                let observed = self.read_creation_record(
                    id.volume_id(), original, cancellation,
                ).await.map_err(|error| match error {
                    FsError::VolumeMismatch => WorkspaceError::from(FsError::CreationRejected), error => WorkspaceError::from(error),
                })?;
                if observed.0.initial_generation_root != initial_root {
                    return Err(WorkspaceError::from(FsError::CreationRejected).into());
                }
                observed
            }
            Err(FsError::VolumeMismatch) => return Err(WorkspaceError::from(FsError::CreationRejected).into()),
            Err(error) => return Err(WorkspaceError::from(error).into()),
        };
        authenticate_named_creation(&creation, operation, id.volume_id(), config, original_operation_id)?;
        let workspace = crate::Workspace {
            name, id, volume: Volume { fs: self.clone(), id: id.volume_id(), config },
            #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
            source: None,
        };
        self.initial_workspace_generation_with_native_work(
            workspace, creation.initial_generation_root, original, cancellation,
        ).await.map_err(Into::into)
    }

    /// Authenticates an original creation without publishing or reading its head.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn workspace_for_creation_operation_with_native_work(
        &self, name: &str, config: VolumeConfig, original_operation_id: OperationId,
        original: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<Option<crate::Generation<A, O>>, (WorkspaceError, Option<NativeAllocation>)> {
        let result: Result<_, WorkspaceError> = async {
            cancellation.check()?;
            original.admit(WorkCounters::UNCHARGED).map_err(FsError::from)?;
            let name = crate::WorkspaceName::new(name, original, cancellation)?;
            let id = crate::WorkspaceId::derive(
                self.inner.workspace_namespace, &name, original, cancellation,
            )?;
            let (creation, operation) = match self.read_creation_record(
                id.volume_id(), original, cancellation,
            ).await {
                Ok(creation) => creation, Err(FsError::Authority(AuthorityStoreError::Missing)) => return Ok(None),
                Err(FsError::VolumeMismatch) => return Err(WorkspaceError::from(FsError::CreationRejected)),
                Err(error) => return Err(WorkspaceError::from(error)),
            };
            authenticate_named_creation(&creation, operation, id.volume_id(), config, original_operation_id)?;
            validate_volume_capabilities(config, self.inner.capabilities)?;
            let workspace = crate::Workspace {
                name, id, volume: Volume { fs: self.clone(), id: id.volume_id(), config },
                #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
                source: None,
            };
            self.initial_workspace_generation_with_native_work(
                workspace, creation.initial_generation_root, original, cancellation,
            ).await.map(Some)
        }.await;
        result.map_err(|error| (error, None))
    }

    /// Opens the workspace authenticated by its immutable native creation record.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn open_workspace_with_native_work(
        &self, name: &str, original: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<crate::Workspace<A, O>, (WorkspaceError, Option<NativeAllocation>)> {
        let result: Result<_, WorkspaceError> = async {
            cancellation.check()?;
            original.admit(WorkCounters::UNCHARGED).map_err(FsError::from)?;
            let name = crate::WorkspaceName::new(name, original, cancellation)?;
            let id = crate::WorkspaceId::derive(
                self.inner.workspace_namespace, &name, original, cancellation,
            )?;
            let (creation, _) = self.read_creation_record(id.volume_id(), original, cancellation).await?;
            validate_volume_capabilities(creation.config, self.inner.capabilities)?;
            Ok(crate::Workspace {
                name, id, volume: Volume { fs: self.clone(), id: id.volume_id(), config: creation.config },
                #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
                source: None,
            })
        }.await;
        result.map_err(|error| (error, None))
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn initial_workspace_generation_with_native_work(
        &self, workspace: crate::Workspace<A, O>, generation_root: ObjectId, original: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<crate::Generation<A, O>, WorkspaceError> {
        let checkout = workspace.volume.checkout_with_native_work(
            GenerationSelector::Exact(GenerationId::new(generation_root.digest)), CheckoutMode::read_only_pinned(), original, cancellation,
        ).await?;
        Ok(crate::Generation { workspace, id: checkout.generation_id(), publication_closure: None, })
    }

    async fn initial_workspace_generation(
        &self, workspace: crate::Workspace<A, O>, generation_root: ObjectId, work: WorkCounters, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> MeasuredResult<FsReceipt<crate::Generation<A, O>>, WorkspaceError> {

        let live = workspace.name.retained_bytes();
        let allowance = remaining_owned(work, budget, live)
            .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
        let checkout = workspace.volume.checkout(
            GenerationSelector::Exact(GenerationId::new(generation_root.digest)), CheckoutMode::read_only_pinned(), allowance, cancellation,
        ).await.map_err(|failure| {
            merge_simultaneous_failure(work, *failure.work, live, failure.error)
                .map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from)
        })?;
        let work = merge_simultaneous_work(work, checkout.work, live, budget)
            .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
        Ok(FsReceipt {
            value: crate::Generation { workspace, id: checkout.value.generation_id(), publication_closure: None, }, work,
        })
    }

    pub(crate) async fn create_workspace_with_config_operation(
        &self, name: impl AsRef<str>, config: VolumeConfig, operation_id: Option<OperationId>, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> MeasuredResult<FsReceipt<crate::Workspace<A, O>>, WorkspaceError>
    {
        let span = crate::obs::span!(
            INFO, "acyclic.fs.create_workspace", outcome = crate::obs::Empty, error.kind = crate::obs::Empty,
            work.items = crate::obs::Empty, work.bytes = crate::obs::Empty, work.durability = crate::obs::Empty
        );

        crate::obs::in_span(&span, async move {
            let created = self.create_named_workspace(name, config, operation_id, budget, cancellation).await?;
            let work = created.work;
            let live = created.value.name.retained_bytes();
            let allowance = remaining_owned(work, budget, live)
                .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
            let head = created.value.volume.resolve_head_generation(allowance, cancellation).await.map_err(|failure| {
                    merge_simultaneous_failure(work, *failure.work, live, failure.error)
                        .map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from)
                })?;
            let work = merge_simultaneous_work(work, head.2, live, budget)
                .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
            Ok(FsReceipt { value: created.value, work, })
        }).await.observe_on(&span, "create_workspace")
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn workspace_name_identity_with_native_work(
        &self, name: impl AsRef<str>, original: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(crate::WorkspaceName, crate::WorkspaceId), (WorkspaceError, Option<NativeAllocation>)> {
        let result = (|| {
            let name = crate::WorkspaceName::new(name.as_ref(), original, cancellation)?;
            let id = crate::WorkspaceId::derive(
                self.inner.workspace_namespace, &name, original, cancellation,
            )?;
            Ok((name, id))
        })();
        result.map_err(|error| (error, None))
    }

    // Original creation recovery authenticates its immutable first record and
    async fn create_named_workspace(
        &self, name: impl AsRef<str>, config: VolumeConfig, operation_id: Option<OperationId>, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> MeasuredResult<FsReceipt<crate::Workspace<A, O>>, WorkspaceError>
    {

        let named = self.workspace_name_identity(name, budget, cancellation)?;
        let (name, id) = named.value;
        let prior = named.work;
        let live = name.retained_bytes();
        let allowance = remaining_owned(prior, budget, live)
            .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
        let created = self.create_volume_with_id_operation(id.volume_id(), config, operation_id, allowance, cancellation).await
            .map_err(|failure| {
                merge_simultaneous_failure(prior, *failure.work, live, failure.error)
                    .map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from)
            })?;
        let work = merge_simultaneous_work(prior, created.work, live, budget)
            .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
        Ok(FsReceipt {
            value: crate::Workspace {
                name, id, volume: created.value,
                #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
                source: None,
            }, work,
        })
    }


    /// Authenticates named workspace creation and its current head under one
    pub async fn open_workspace(
        &self, name: impl AsRef<str>, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> MeasuredResult<FsReceipt<crate::Workspace<A, O>>, WorkspaceError>
    {
        let span = crate::obs::span!(
            INFO, "acyclic.fs.open_workspace", outcome = crate::obs::Empty, error.kind = crate::obs::Empty,
            work.items = crate::obs::Empty, work.bytes = crate::obs::Empty, work.durability = crate::obs::Empty
        );

        crate::obs::in_span(&span, async move {
            let named = self.workspace_name_identity(name, budget, cancellation)?;
            let (name, id) = named.value;
            let prior = named.work;
            let live = name.retained_bytes();
            let allowance = remaining_owned(prior, budget, live)
                .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
            let opened = self.open_volume(id.volume_id(), allowance, cancellation).await.map_err(|failure| {
                    merge_simultaneous_failure(prior, *failure.work, live, failure.error)
                        .map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from)
                })?;
            let work = merge_simultaneous_work(prior, opened.work, live, budget)
                .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
            let allowance = remaining_owned(work, budget, live)
                .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
            let head = opened.value.resolve_head_generation(allowance, cancellation).await.map_err(|failure| {
                    merge_simultaneous_failure(work, *failure.work, live, failure.error)
                        .map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from)
                })?;
            let work = merge_simultaneous_work(work, head.2, live, budget)
                .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
            Ok(FsReceipt {
                value: crate::Workspace {
                    name, id, volume: opened.value,
                    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
                    source: None,
                }, work,
            })
        }).await.observe_on(&span, "open_workspace")
    }

    /// Terminally deletes one named workspace without requiring a live handle.
    pub async fn delete_workspace(
        &self, name: impl AsRef<str>, idempotency_key: crate::IdempotencyKey,
    ) -> Result<crate::workspace::WorkspaceDelete, WorkspaceError> {
        let span = crate::obs::span!(
            INFO, "acyclic.fs.delete_workspace", outcome = crate::obs::Empty, error.kind = crate::obs::Empty
        );
        let result = crate::obs::in_span(&span, async move {
            let (_, id) = self.workspace_name_identity(name, WorkBudget::UNBOUNDED, &CancellationToken::new())
                .map_err(|failure| failure.error)?.value;
            let volume = match self.open_volume(
                    id.volume_id(), WorkBudget::UNBOUNDED, &CancellationToken::new(),
                ).await
            {
                Ok(volume) => volume.value, Err(failure) if matches!(failure.error, FsError::WorkspaceDeleted) => {
                    return Ok(crate::workspace::WorkspaceDelete::AlreadyDeleted);
                }
                Err(failure) => return Err(WorkspaceError::engine(failure)),
            };
            self.delete_workspace_volume(&volume, idempotency_key.operation_id()).await
        }).await;
        crate::obs::outcome_on(&span, result)
    }

    /// Builds and stores the forked generation root for a new workspace, then
    async fn materialize_forked_generation_root(
        &self, source: &crate::Generation<A, O>, destination_id: crate::WorkspaceId, config: VolumeConfig,
        paths: Option<Vec<String>>, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> MeasuredResult<(VerifiedForkSource, ObjectId, WorkCounters), WorkspaceError> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = self.materialize_forked_generation_root_with_accounting(
            source, destination_id, config, paths, &mut accounting, cancellation,
        ).await;
        let work = accounting.spent();
        result.map(|(proof, source_root)| (proof.value, source_root, work)).map_err(|error| OperationFailure::new(error, work))
    }

    async fn materialize_forked_generation_root_with_accounting(
        &self, source: &crate::Generation<A, O>, destination_id: crate::WorkspaceId, config: VolumeConfig,
        paths: Option<Vec<String>>, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<(Owned<VerifiedForkSource>, ObjectId), WorkspaceError> {

        let path_bytes = paths.as_ref().map_or(Ok(0), |paths| crate::workspace::workspace_owned_strings_bytes(paths))?;
        let (path_allocation, admitted) = accounting.record_actual_owned(path_bytes)?;
        let mut paths = (paths, path_allocation);
        admitted?;
        accounting.check()?;
        let proven_at = self.inner.objects.collection_sweeps();
        let source_object = ObjectId {
            kind: ObjectKind::GenerationRoot, digest: source.id.digest(),
        };
        let (source_root, allocation) = read_generation_root_with_accounting(
            &self.inner.objects, source_object, config, accounting, cancellation,
        ).await?;
        let source_parent_bytes = root_parent_bytes(&source_root)?;
        let source_root = Owned::new(source_root, allocation, source_parent_bytes);
        if source_root.value.volume_id != source.workspace.volume.id { return Err(WorkspaceError::ForeignGeneration); }
        let file_table = if let Some(selected) = paths.0.take() {
            accounting.release_owned(&mut paths.1, path_bytes)?;
            self.select_fork_paths_with_accounting(
                source, &source_root.value, selected, accounting, cancellation,
            ).await?
        } else { source_root.value.file_table };
        let mut parents = Owned::<Vec<GenerationId>>::allocate(1, accounting)?;
        let copied = WorkCounters { bytes_copied: size_of::<GenerationId>() as u64, ..WorkCounters::UNCHARGED };
        accounting.admit(copied)?;
        parents.value.push(source.id);
        accounting.record_completed(copied)?;
        let required_features = source_root.value.continuation_features()
            | if file_table == source_root.value.file_table { 0 } else { GenerationRoot::FILTERED_FORK };
        let fork_root = GenerationRoot {
            volume_id: destination_id.volume_id(), root_file_id: source_root.value.root_file_id, file_table,
            parents: parents.value, required_features,
        };
        let fork_root = Owned::new(fork_root, parents.allocation, parents.bytes);
        source_root.retire(accounting)?;
        let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| crate::kernel::encode_generation_root(&fork_root.value, observe, allocation_error))?;
        let generation_root = self.put_encoded_with_accounting(
            ObjectKind::GenerationRoot, encoded, accounting, cancellation,
        ).await?;
        fork_root.retire(accounting)?;
        let proof = self.prove_fork_source_with_accounting(
            generation_root, proven_at, config, accounting, cancellation,
        ).await?;
        Ok((proof, source_object))
    }

    async fn prove_fork_source_with_accounting(
        &self, generation_root: ObjectId, proven_at: u64, config: VolumeConfig, accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Owned<VerifiedForkSource>, WorkspaceError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let (proof, mut allocation) = prove_generation_closure_with_native_work(
                &self.inner.objects, generation_root, closure_limits(config), original, cancellation,
            ).await?;
            let root_bytes = root_parent_bytes(&proof.root)?;
            let bytes = (proof.objects.capacity() as u64).checked_mul(size_of::<ObjectId>() as u64).ok_or(WorkError::Overflow)?;
            drop(proof.root);
            allocation.release(root_bytes).map_err(FsError::from)?;
            return Ok(Owned::new(VerifiedForkSource {
                generation_root, closure: proof.objects, proven_at,
            }, KernelOwnedAllocation::NativeOriginal(allocation), bytes));
        }
        let result = prove_generation_closure(
            &self.inner.objects, generation_root, closure_limits(config), accounting.remaining()?, cancellation,
        ).await;
        let work = match &result { Ok(proof) => proof.work, Err(failure) => *failure.work };
        merge_fork_backend_work(accounting, work)?;
        let proof = result.map_err(|failure| FsError::from(failure.error))?;
        drop(proof.root);
        let bytes = (proof.objects.capacity() as u64).checked_mul(size_of::<ObjectId>() as u64).ok_or(WorkError::Overflow)?;
        let allocation = accounting.claim_owned(bytes, 0)?;
        Ok(Owned::new(VerifiedForkSource { generation_root, closure: proof.objects, proven_at, }, allocation, bytes))
    }

    /// Forks one immutable generation, with an optional publication guard.
    pub(crate) async fn fork_workspace<W: FsWork, F, Fut>(
        &self, destination: crate::WorkspaceName, source: &crate::Generation<A, O>,
        idempotency_key: crate::IdempotencyKey, paths: Option<Vec<String>>,
        permit: PublicationPermit, budget: W, cancellation: &CancellationToken, guard: Option<&mut F>,
    ) -> W::Output<crate::Workspace<A, O>, WorkspaceError>
    where
        F: FnMut(WorkBudget) -> Fut + ProviderTask, Fut: Future<Output = FsResult<()>> + ProviderTask,
    {
        let mut accounting = budget.accounting(cancellation);
        let name_allocation = if matches!(accounting, KernelAccounting::Standalone { .. } | KernelAccounting::BorrowedStandalone { .. }) {
            accounting.record_actual_owned(destination.retained_bytes())
        } else { Ok((KernelOwnedAllocation::Standalone, Ok(()))) };
        #[cfg(not(target_arch = "wasm32"))]
        let original = accounting.native_work();
        let publication = |fork, permit, budget| async move {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = original {
                let committed = if guard.is_some() || permit != PublicationPermit::Unrestricted {
                    Err(WorkspaceError::from(FsError::MutationNotAllowed))
                } else {
                    self.inner.authority.commit_workspace_fork_with_native_work(
                        fork, original, cancellation,
                    ).await.map_err(|error| WorkspaceError::from(FsError::from(error)))
                };
                return (committed, WorkCounters::UNCHARGED);
            }
            let committed = match guard {
                Some(guard) => self.inner.authority.commit_workspace_fork_guarded(
                    fork, permit, budget, cancellation, guard,
                ).await,
                None => self.inner.authority.commit_workspace_fork(fork, budget, cancellation).await
                    .map(|receipt| FsReceipt { value: receipt.value, work: receipt.work })
                    .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, FsError::from)),
            };
            let work = match &committed { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
            (committed.map(|receipt| receipt.value).map_err(|failure| WorkspaceError::from(failure.error)), work)
        };
        let result = match name_allocation {
            Ok((allocation, admitted)) => {
                let destination = (destination, allocation);
                match admitted {
                    Ok(()) => self.fork_workspace_with_accounting(
                        destination.0, source, idempotency_key, paths, permit, &mut accounting, cancellation, publication,
                    ).await, Err(error) => Err(WorkspaceError::from(error)),
                }
            }
            Err(error) => Err(WorkspaceError::from(error)),
        };
        budget.finish(result, &accounting)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn fork_workspace_guarded_with_native_work<'original, E, F, Fut>(
        &self, destination: crate::WorkspaceName, source: &crate::Generation<A, O>,
        idempotency_key: crate::IdempotencyKey, paths: Option<Vec<String>>,
        permit: PublicationPermit, original: &'original NativeMaterializationWork,
        cancellation: &CancellationToken, before_publication: &mut F,
    ) -> Result<crate::Workspace<A, O>, E>
    where
        E: From<FsError> + From<WorkspaceError> + Send + 'static,
        F: for<'m> FnMut(&'m NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        let mut accounting = KernelAccounting::native(original, cancellation);
        let publication = |fork, permit, _| async move {
            (self.inner.authority.commit_workspace_fork_guarded_with_native_work(
                fork, permit, original, cancellation, before_publication,
            ).await, WorkCounters::UNCHARGED)
        };
        self.fork_workspace_with_accounting(
            destination, source, idempotency_key, paths, permit, &mut accounting, cancellation, publication,
        ).await
    }

    async fn fork_workspace_with_accounting<E, P, Fut>(
        &self, destination: crate::WorkspaceName, source: &crate::Generation<A, O>,
        idempotency_key: crate::IdempotencyKey, paths: Option<Vec<String>>, permit: PublicationPermit,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken, publication: P,
    ) -> Result<crate::Workspace<A, O>, E>
    where
        E: From<FsError> + From<WorkspaceError> + Send,
        P: FnOnce(crate::WorkspaceForkCommit, PublicationPermit, WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = (Result<crate::WorkspaceForkOutcome, E>, WorkCounters)> + ProviderTask,
    {

        let path_bytes = paths.as_ref().map_or(Ok(0), |paths| crate::workspace::workspace_owned_strings_bytes(paths))?;
        let (path_allocation, admitted) = accounting.record_actual_owned(path_bytes)?;
        let mut paths = (paths, path_allocation);
        admitted?;
        if !self.same_deployment(&source.workspace.volume.fs) { return Err(WorkspaceError::ForeignGeneration.into()); }
        accounting.check()?;
        let destination_id = self.derive_fork_workspace_id_with_accounting(&destination, accounting, cancellation)?;
        let volume_id = destination_id.volume_id();
        let source_volume = &source.workspace.volume;
        let config = source_volume.config;
        let selected = paths.0.take();
        accounting.release_owned(&mut paths.1, path_bytes)?;
        let (fork_root, source_root) = self.materialize_forked_generation_root_with_accounting(
            source, destination_id, config, selected, accounting, cancellation,
        ).await?;
        match self.read_fork_creation_with_accounting(volume_id, accounting, cancellation).await {
            Ok((created, operation)) => {
                if operation != idempotency_key.operation_id()
                    || created.volume_id != volume_id || created.config != config
                    || created.initial_generation_root != fork_root.value.generation_root
                { return Err(WorkspaceError::from(FsError::CreationRejected).into()); }
                fork_root.retire(accounting)?;
                return Ok(crate::Workspace {
                    name: destination, id: destination_id, volume: Volume { fs: self.clone(), id: volume_id, config },
                    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
                    source: None,
                });
            }
            Err(WorkspaceError::Filesystem(FsError::Authority(AuthorityStoreError::Missing))) =>
                accounting.check()?,
            Err(error) => return Err(error.into()),
        }
        let lineage = self.fork_lineage_with_accounting(source, accounting, cancellation).await?;
        let (retention, retained, creation, payload_bytes) = fork_records_with_accounting(
            source_volume, source_root, destination_id, fork_root.value.generation_root,
            idempotency_key.operation_id(), accounting, cancellation,
        )?;
        let fork = crate::WorkspaceForkCommit {
            lineage, destination: volume_authority_id(volume_id), creation, retention, retained,
        };
        let original_root = fork_root.value.generation_root;
        let outcome = match accounting {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work: original, .. } => {
                let original = *original;
                let drained = self.inner.objects.flush_before_publish_with_native_work(
                    crate::PublicationScope::Closure {
                        objects: &fork_root.value.closure, proven_at: fork_root.value.proven_at,
                    }, original, cancellation,
                ).await.map_err(FsError::from)?;
                accounting.check()?;
                let (outcome, _) = publication(fork, permit, accounting.remaining()?).await;
                drop(drained);
                outcome?
            }
            _ => {
                let drained = self.inner.objects.flush_before_publish(
                    crate::PublicationScope::Closure {
                        objects: &fork_root.value.closure, proven_at: fork_root.value.proven_at,
                    }, accounting.remaining()?, cancellation,
                ).await;
                let work = match &drained { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
                merge_fork_backend_work(accounting, work)?;
                let drained = drained.map_err(|failure| FsError::from(failure.error))?;
                let (committed, work) = publication(fork, permit, accounting.remaining()?).await;
                let merged = merge_fork_backend_work(accounting, work);
                let mut allocation = KernelOwnedAllocation::Standalone;
                accounting.release_owned(&mut allocation, payload_bytes)?;
                drop(drained);
                merged?;
                committed?
            }
        };
        match outcome {
            crate::WorkspaceForkOutcome::Committed => {}
            crate::WorkspaceForkOutcome::RetentionConflict => return Err(WorkspaceError::RetentionConflict.into()),
            crate::WorkspaceForkOutcome::CreationRejected =>
                return Err(WorkspaceError::from(FsError::CreationRejected).into()),
        }
        fork_root.retire(accounting)?;
        let (created, operation) = self.read_fork_creation_with_accounting(volume_id, accounting, cancellation).await?;
        if operation != idempotency_key.operation_id() || created.config != config
            || created.initial_generation_root != original_root
        { return Err(WorkspaceError::from(FsError::CreationRejected).into()); }
        Ok(crate::Workspace {
            name: destination, id: destination_id, volume: Volume { fs: self.clone(), id: volume_id, config },
            #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
            source: None,
        })
    }

    fn derive_fork_workspace_id_with_accounting(
        &self, destination: &crate::WorkspaceName, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<crate::WorkspaceId, WorkspaceError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            return crate::WorkspaceId::derive(
                self.inner.workspace_namespace, destination, original, cancellation,
            );
        }
        let derived = crate::WorkspaceId::derive(
            self.inner.workspace_namespace, destination, accounting.remaining()?, cancellation,
        );
        let work = match &derived { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
        merge_fork_backend_work(accounting, work)?;
        derived.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    async fn read_fork_creation_with_accounting(
        &self, volume_id: VolumeId, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<(VolumeCreated, OperationId), WorkspaceError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            return self.read_creation_record(volume_id, original, cancellation).await.map_err(WorkspaceError::from);
        }
        let read = self.read_creation_record(
            volume_id, accounting.remaining()?, cancellation,
        ).await;
        let work = match &read { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
        merge_fork_backend_work(accounting, work)?;
        read.map(|receipt| receipt.value).map_err(|failure| WorkspaceError::from(failure.error))
    }

    /// The source lineage a fork's authority starts from: the published
    async fn fork_lineage_with_accounting(
        &self, source: &crate::Generation<A, O>, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<Option<crate::GenerationForkSource>, WorkspaceError> {
        if !self.inner.authority.supports_generation_lineage_prefix() {
            return Ok(None);
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let head = source.workspace.volume.checkout_with_native_work(
                GenerationSelector::Head, CheckoutMode::read_only_pinned(), original, cancellation,
            ).await?;
            return Ok(Some(crate::GenerationForkSource {
                authority: volume_authority_id(source.workspace.volume.id),
                generation: source.id,
                lineage: if head.generation_id() == source.id {
                    crate::GenerationFork::PublishedPrefix
                } else { crate::GenerationFork::Independent },
            }));
        }
        let head = source.workspace.head(accounting.remaining()?, cancellation).await;
        let work = match &head { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
        merge_fork_backend_work(accounting, work)?;
        let head = head.map_err(|failure| failure.error)?;
        Ok(Some(crate::GenerationForkSource {
            authority: volume_authority_id(source.workspace.volume.id), generation: source.id,
            lineage: if head.value.id == source.id { crate::GenerationFork::PublishedPrefix }
                else { crate::GenerationFork::Independent },
        }))
    }

    pub(crate) async fn make_records_durable(
        &self,
        config: VolumeConfig,
        records: &[FileRecord],
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> MeasuredResult<FsReceipt<crate::PublicationHold>, FsError> {
        let proven_at = self.inner.objects.collection_sweeps();
        let (closure, work) = prove_record_closure_async(
            &self.inner.objects, records, closure_limits(config), budget, cancellation,
        ).await.map_err(|failure| failure.map_with_prior_work(WorkCounters::default(), FsError::from))?;
        let flushed = self.inner.objects.flush_before_publish(
            crate::PublicationScope::Closure { objects: &closure, proven_at }, remaining(work, budget)?, cancellation,
        ).await.map_err(|failure| failure.map_with_prior_work(work, FsError::from))?;
        let total = work.checked_add(flushed.work).map_err(|error| OperationFailure::new(FsError::from(error), work))?;
        Ok(FsReceipt { value: flushed.value, work: total })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn make_records_durable_with_native_work(
        &self, config: VolumeConfig, records: &[FileRecord], original: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<crate::PublicationHold, FsError> {
        original.admit(WorkCounters::UNCHARGED)?;
        cancellation.check()?;
        let proven_at = self.inner.objects.collection_sweeps();
        let (value, allocation) = prove_record_closure_with_native_work(
            &self.inner.objects, records, closure_limits(config), original, cancellation,
        ).await?;
        let closure = (value, allocation);
        self.inner.objects.flush_before_publish_with_native_work(
            crate::PublicationScope::Closure { objects: &closure.0, proven_at }, original, cancellation,
        ).await.map_err(FsError::from)
    }

    /// Holds the actual generation closure across an external durable publication.
    pub(crate) async fn make_generation_durable(
        &self, volume: &Volume<A, O>, generation: GenerationId, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> MeasuredResult<FsReceipt<crate::PublicationHold>, WorkspaceError> {

        let generation_root = ObjectId { kind: ObjectKind::GenerationRoot, digest: generation.digest() };
        let proven_at = self.inner.objects.collection_sweeps();
        let proof = prove_generation_closure(
            &self.inner.objects, generation_root, closure_limits(volume.config), budget, cancellation,
        ).await.map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::engine))?;
        let work = proof.work;
        if proof.root.volume_id != volume.id {
            return Err(OperationFailure::new(WorkspaceError::ForeignGeneration, work));
        }
        drop(proof.root);
        let live = (proof.objects.capacity() as u64).checked_mul(size_of::<ObjectId>() as u64)
            .ok_or_else(|| OperationFailure::new(WorkspaceError::Work(WorkError::Overflow), work))?;
        let flushed = self.inner.objects.flush_before_publish(
            crate::PublicationScope::Closure { objects: &proof.objects, proven_at },
            remaining_owned(work, budget, live).map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?,
            cancellation,
        ).await.map_err(|failure| merge_simultaneous_failure(work, *failure.work, live, FsError::Object(failure.error))
            .map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
        let work = merge_simultaneous_work(work, flushed.work, live, budget)
            .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
        Ok(FsReceipt { value: flushed.value, work })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn make_generation_durable_with_native_work(
        &self, volume: &Volume<A, O>, generation: GenerationId, original: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<crate::PublicationHold, (WorkspaceError, Option<NativeAllocation>)> {
        let result = async {
            let mut accounting = KernelAccounting::native(original, cancellation);
            accounting.check()?;
            if !self.same_deployment(&volume.fs) { return Err(WorkspaceError::ForeignGeneration); }
            let publication = self.retention_publication_with_accounting(
                volume, ObjectId { kind: ObjectKind::GenerationRoot, digest: generation.digest() }, &mut accounting, cancellation,
            ).await?;
            let held = self.inner.objects.flush_before_publish_with_native_work(
                crate::PublicationScope::Closure {
                    objects: &publication.value.objects, proven_at: publication.value.proven_at,
                }, original, cancellation,
            ).await.map_err(FsError::from)?;
            publication.retire(&mut accounting)?;
            Ok(held)
        }.await;
        result.map_err(|error| (error, None))
    }

    /// Durably retains one generation under a retention label.
    pub(crate) async fn retain_workspace_generation<W: FsWork>(
        &self, volume: &Volume<A, O>, generation_id: GenerationId, kind: RetentionKind, mut label: Owned<String>,
        publication: Option<VerifiedPublication<A, O>>, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                if label.bytes != label.value.capacity() as u64 || !matches!(&label.allocation,
                    KernelOwnedAllocation::NativeOriginal(allocation)
                        if allocation.bytes() == label.bytes && allocation.work().shares_ledger(original))
                { return Err(FsError::from(NativeTreeMaterializationError::InvalidLayout).into()); }
            } else {
                label.allocation = accounting.claim_owned(label.bytes, 0)?;
            }
            #[cfg(target_arch = "wasm32")]
            { label.allocation = accounting.claim_owned(label.bytes, 0)?; }
            self.retain_workspace_generation_with_accounting(
                volume, generation_id, kind, label, publication, &mut accounting, cancellation,
            ).await
        }.await;
        budget.finish(result, &accounting)
    }

    async fn retain_workspace_generation_with_accounting(
        &self, volume: &Volume<A, O>, generation_id: GenerationId, kind: RetentionKind, label: Owned<String>,
        publication: Option<VerifiedPublication<A, O>>, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        accounting.check()?;
        let generation_root = ObjectId { kind: ObjectKind::GenerationRoot, digest: generation_id.digest() };
        let hashed = WorkCounters {
            bytes_hashed: (b"acyclic-fs-retention-authority-v1\0".len() + 16 + 1 + 8 + label.value.len()) as u64,
            bytes_copied: 16,..WorkCounters::UNCHARGED
        };
        accounting.admit(hashed)?;
        let authority_id = retention_authority_id(volume.id, kind, &label.value);
        accounting.record_completed(hashed)?;
        let Owned { value: label, allocation, bytes } = label;
        let fact = Owned::new(
            RetentionCreated { volume_id: volume.id, kind, label, generation_root, config: volume.config }, allocation, bytes,
        );
        let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| crate::kernel::encode_retention_created(&fact.value, observe, allocation_error))?;
        let commit = creation_commit_with_accounting(
            OperationId::from_bytes(authority_id.into_bytes()), encoded, accounting,
        )?;
        fact.retire(accounting)?;
        let cached_bytes = publication.as_ref().map_or(0, VerifiedPublication::retained_bytes);
        let cached = if let Some(publication) = publication {
            let bytes = if accounting.standalone_parts().is_some() { cached_bytes } else { 0 };
            let allocation = accounting.claim_owned(bytes, 0)?;
            Some(Owned::new(publication, allocation, bytes))
        } else { None };
        let head = self.retention_head_with_accounting(authority_id, accounting, cancellation).await;
        match head {
            Ok(_) => {
                let found = self.retention_operation_with_accounting(
                    authority_id, commit.operation_id, accounting, cancellation,
                ).await?;
                if found.is_some_and(|(sequence, fingerprint)| sequence == Sequence::new(1)
                    && fingerprint == commit.fingerprint)
                { if let Some(cached) = cached { cached.retire(accounting)?; } return Ok(()); }
            }
            Err(FsError::Authority(AuthorityStoreError::Missing)) => {}
            Err(error) => return Err(error.into()),
        }
        let limits = closure_limits(volume.config);
        let cached = match cached {
            Some(proof) if self.same_deployment(&proof.value.filesystem)
                && proof.value.generation_root == generation_root && proof.value.volume_id == volume.id
                && proof.value.limits == limits
                && proof.value.proven_at == self.inner.objects.collection_sweeps() => Some(proof),
            Some(proof) => { proof.retire(accounting)?; None }
            None => None,
        };
        let publication = if let Some(proof) = cached { proof } else {
            self.retention_publication_with_accounting(volume, generation_root, accounting, cancellation).await?
        };
        let result = self.publish_retention_with_accounting(
            authority_id, commit, &publication.value, accounting, cancellation,
        ).await;
        publication.retire(accounting)?;
        if result? { Ok(()) } else { Err(WorkspaceError::RetentionConflict) }
    }

    async fn retention_head_with_accounting(
        &self, authority_id: AuthorityId, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<Head, FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            return self.inner.authority.head_with_native_work(authority_id, original, cancellation).await.map_err(Into::into);
        }
        let result = self.inner.authority.head(authority_id, accounting.remaining()?, cancellation).await;
        let spent = match &result { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
        merge_creation_backend_work(accounting, spent)?;
        result.map(|receipt| receipt.value).map_err(|failure| failure.error.into())
    }

    async fn retention_operation_with_accounting(
        &self, authority_id: AuthorityId, operation_id: OperationId, accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<(Sequence, Digest)>, FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let found = self.inner.authority.find_operation_with_native_work(
                authority_id, operation_id, original, cancellation,
            ).await?;
            return Ok(found.0.as_ref().map(|record| (record.sequence, record.fingerprint)));
        }
        let result = self.inner.authority.find_operation(
            authority_id, operation_id, accounting.remaining()?, cancellation,
        ).await;
        let spent = match &result { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
        merge_creation_backend_work(accounting, spent)?;
        result.map(|receipt| receipt.value.map(|record| (record.sequence, record.fingerprint))).map_err(|failure| failure.error.into())
    }

    async fn retention_publication_with_accounting(
        &self, volume: &Volume<A, O>, generation_root: ObjectId, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<Owned<VerifiedPublication<A, O>>, WorkspaceError> {
        let proven_at = self.inner.objects.collection_sweeps();
        let limits = closure_limits(volume.config);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let proof = prove_generation_closure_with_native_work(
                &self.inner.objects, generation_root, limits, original, cancellation,
            ).await?;
            if proof.0.root.volume_id != volume.id { return Err(WorkspaceError::ForeignGeneration); }
            let root_bytes = root_parent_bytes(&proof.0.root)?;
            let bytes = (proof.0.objects.capacity() as u64).checked_mul(size_of::<ObjectId>() as u64).ok_or(WorkError::Overflow)?;
            let (proof, allocation) = proof;
            drop(proof.root);
            let mut objects = (proof.objects, allocation);
            objects.1.release(root_bytes)?;
            let publication = VerifiedPublication {
                filesystem: self.clone(), generation_root, volume_id: volume.id, limits,
                objects: objects.0, proven_at, _allocation: KernelOwnedAllocation::Standalone,
            };
            return Ok(Owned::new(publication, KernelOwnedAllocation::NativeOriginal(objects.1), bytes));
        }
        let result = prove_generation_closure(
            &self.inner.objects, generation_root, limits, accounting.remaining()?, cancellation,
        ).await;
        let spent = match &result { Ok(proof) => proof.work, Err(failure) => *failure.work };
        merge_creation_backend_work(accounting, spent)?;
        let proof = result.map_err(|failure| FsError::from(failure.error))?;
        if proof.root.volume_id != volume.id { return Err(WorkspaceError::ForeignGeneration); }
        drop(proof.root);
        let bytes = (proof.objects.capacity() as u64).checked_mul(size_of::<ObjectId>() as u64).ok_or(WorkError::Overflow)?;
        let allocation = accounting.claim_owned(bytes, 0)?;
        Ok(Owned::new(VerifiedPublication {
            filesystem: self.clone(), generation_root, volume_id: volume.id, limits,
            objects: proof.objects, proven_at, _allocation: KernelOwnedAllocation::Standalone,
        }, allocation, bytes))
    }

    async fn publish_retention_with_accounting(
        &self, authority_id: AuthorityId, commit: ProposedCommit, publication: &VerifiedPublication<A, O>,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<bool, FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let _hold = self.inner.objects.flush_before_publish_with_native_work(
                crate::PublicationScope::Closure { objects: &publication.objects, proven_at: publication.proven_at },
                original, cancellation,
            ).await?;
            accounting.check()?;
            let created = self.inner.authority.create_authority_with_first_record_with_native_work(
                authority_id, commit, original, cancellation,
            ).await?;
            return Ok(created);
        }
        let drained = self.inner.objects.flush_before_publish(
            crate::PublicationScope::Closure { objects: &publication.objects, proven_at: publication.proven_at },
            accounting.remaining()?, cancellation,
        ).await;
        let spent = match &drained { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
        merge_creation_backend_work(accounting, spent)?;
        let _hold = drained.map_err(|failure| FsError::Object(failure.error))?.value;
        accounting.check()?;
        let created = self.inner.authority.create_authority_with_first_record(
            authority_id, commit, accounting.remaining()?, cancellation,
        ).await;
        let spent = match &created { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
        merge_creation_backend_work(accounting, spent)?;
        Ok(created.map_err(|failure| FsError::Authority(failure.error))?.value)
    }

    pub(crate) async fn delete_workspace_volume(
        &self,
        volume: &Volume<A, O>,
        operation_id: OperationId,
    ) -> Result<crate::workspace::WorkspaceDelete, WorkspaceError> {
        let cancellation = CancellationToken::new();
        let authority_id = volume_authority_id(volume.id);
        let head = match self
            .inner
            .authority
            .head(authority_id, WorkBudget::UNBOUNDED, &cancellation)
            .await
        {
            Ok(head) => head.value,
            Err(failure) if matches!(failure.error, AuthorityStoreError::Retired) => {
                return Ok(crate::workspace::WorkspaceDelete::AlreadyDeleted);
            }
            Err(failure) => return Err(WorkspaceError::engine(failure)),
        };
        if head.sequence == Sequence::GENESIS { return Err(WorkspaceError::engine(FsError::EmptyAuthority)); }
        let latest = self
            .inner
            .authority
            .replay(
                authority_id,
                Sequence::new(head.sequence.get().saturating_sub(1)),
                ReplayLimit {
                    records: 1,
                    payload_bytes: MAXIMUM_VOLUME_EVENT_BYTES,
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(WorkspaceError::engine)?;
        let latest = latest.value.first().ok_or_else(|| {
            WorkspaceError::engine(FsError::InvalidAuthorityHistory)
        })?;
        if let Ok(deleted) = decode_workspace_deleted(&latest.payload, MAXIMUM_VOLUME_EVENT_BYTES) {
            if deleted != volume.id {
                return Err(WorkspaceError::engine(
                    FsError::VolumeMismatch,
                ));
            }
            // A retry after the tombstone finishes what the delete began.
            self.retire_deleted_authority(authority_id, &cancellation)
                .await?;
            return Ok(crate::workspace::WorkspaceDelete::AlreadyDeleted);
        }
        generation_from_record(latest, volume.id, WorkCounters::default())
            .map_err(WorkspaceError::engine)?;
        let encoded = crate::kernel::encode::<FsError>(WorkBudget::UNBOUNDED, Some(&cancellation), 0, |observe| crate::kernel::encode_workspace_deleted(volume.id, observe, FsError::EncodedAllocationFailed))
            .map_err(|failure| WorkspaceError::engine(failure.error))?;
        let encoded = creation_commit(operation_id, encoded, WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|failure| WorkspaceError::engine(failure.error))?;
        let (commit, _) = encoded.value;
        let appended = self
            .inner
            .authority
            .compare_and_append(
                authority_id,
                head.epoch,
                head,
                commit,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(WorkspaceError::engine)?;
        Ok(match appended.value {
            AppendOutcome::Committed(_) | AppendOutcome::AlreadyCommitted(_) => {
                self.retire_deleted_authority(authority_id, &cancellation)
                    .await?;
                crate::workspace::WorkspaceDelete::Deleted
            }
            AppendOutcome::Conflict { .. } | AppendOutcome::Fenced { .. } => {
                crate::workspace::WorkspaceDelete::Conflict
            }
            AppendOutcome::IdempotencyConflict { .. } => {
                crate::workspace::WorkspaceDelete::IdempotencyConflict
            }
        })
    }

    /// Releases the authority of a workspace whose tombstone is durable:
    async fn retire_deleted_authority(
        &self,
        authority_id: crate::AuthorityId,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        self.inner
            .authority
            .retire_authority(authority_id, WorkBudget::UNBOUNDED, cancellation)
            .await
            .map_err(WorkspaceError::engine)?;
        Ok(())
    }

    pub(crate) async fn workspace_common_ancestor_with_accounting(
        &self,
        source: &crate::Generation<A, O>,
        target: &crate::Generation<A, O>,
        maximum_generations: u32,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<WorkspaceCommonAncestor, WorkspaceError> {
        if maximum_generations == 0
            || !Arc::ptr_eq(&self.inner, &source.workspace.volume.fs.inner)
            || !Arc::ptr_eq(&self.inner, &target.workspace.volume.fs.inner)
        {
            return Err(WorkspaceError::ForeignGeneration);
        }
        if source.workspace.volume.config != target.workspace.volume.config {
            return Err(WorkspaceError::IncompatibleWorkspace);
        }
        accounting.check()?;
        let (target_ancestors, target_truncated) = collect_generation_ancestors(
            &self.inner.objects,
            target.id,
            target.workspace.volume.config,
            maximum_generations,
            accounting, cancellation,
        ).await?;
        let result = find_first_generation_ancestor(
            &self.inner.objects,
            source.id,
            source.workspace.volume.config,
            maximum_generations,
            &target_ancestors.value,
            target_truncated,
            accounting, cancellation,
        ).await;
        target_ancestors.retire(accounting)?;
        result
    }

    /// Computes the immutable source-parent identity used by a true merge
    pub(crate) async fn workspace_normalized_join_parent(
        &self, source: &crate::Generation<A, O>, target: &crate::Generation<A, O>,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<GenerationId, WorkspaceError> {
        accounting.check()?;
        if !Arc::ptr_eq(&self.inner, &source.workspace.volume.fs.inner)
            || !Arc::ptr_eq(&self.inner, &target.workspace.volume.fs.inner)
            || source.workspace.volume.config != target.workspace.volume.config
        {
            return Err(WorkspaceError::IncompatibleWorkspace);
        }
        let source_root = read_generation_root_with_accounting(
            &self.inner.objects,
            ObjectId { kind: ObjectKind::GenerationRoot, digest: source.id.digest() },
            source.workspace.volume.config, accounting, cancellation,
        ).await?;
        let target_root = read_generation_root_with_accounting(
            &self.inner.objects, ObjectId { kind: ObjectKind::GenerationRoot, digest: target.id.digest() },
            target.workspace.volume.config, accounting, cancellation,
        ).await?;
        if source_root.0.volume_id != source.workspace.volume.id
            || target_root.0.volume_id != target.workspace.volume.id
            || source_root.0.root_file_id != target_root.0.root_file_id
        {
            return Err(WorkspaceError::IncompatibleWorkspace);
        }
        let normalized = (GenerationRoot {
            volume_id: target.workspace.volume.id,
            root_file_id: target_root.0.root_file_id, file_table: source_root.0.file_table,
            required_features: source_root.0.continuation_features(), parents: source_root.0.parents,
        }, source_root.1);
        let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| crate::kernel::encode_generation_root(&normalized.0, observe, allocation_error))?;
        let hashing = WorkCounters {
            bytes_hashed: (encoded.0.len() as u64).checked_add(OBJECT_DIGEST_ENVELOPE_BYTES)
                .ok_or(WorkError::Overflow)?,..WorkCounters::UNCHARGED
        };
        accounting.admit(hashing)?;
        let digest = object_digest(ObjectKind::GenerationRoot, &encoded.0);
        accounting.record_completed(hashing)?;
        Ok(GenerationId::new(digest))
    }

    pub(crate) async fn workspace_join_changes<W: FsWork>(
        &self, base: GenerationId, source: &crate::Generation<A, O>, maximum_changes: u32,
        budget: W, cancellation: &CancellationToken,
    ) -> W::Output<(GenerationDiff, KernelOwnedAllocation), WorkspaceError> {
        let mut accounting = budget.accounting(cancellation);
        let standalone = readonly_standalone_live(&accounting).is_some();
        let result = self.workspace_join_changes_with_accounting(
            base, source, maximum_changes, &mut accounting, cancellation,
        ).await.map_err(|error| match error {
            WorkspaceError::Filesystem(error) if standalone => WorkspaceError::engine(error), error => error,
        });
        budget.finish(result, &accounting)
    }

    async fn workspace_join_changes_with_accounting(
        &self, base: GenerationId, source: &crate::Generation<A, O>, maximum_changes: u32,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<(GenerationDiff, KernelOwnedAllocation), WorkspaceError> {

        if maximum_changes == 0 || !Arc::ptr_eq(&self.inner, &source.workspace.volume.fs.inner) {
            return Err(WorkspaceError::ForeignGeneration);
        }
        let config = source.workspace.volume.config;
        let (base_root, allocation) = read_generation_root_with_accounting(
            &self.inner.objects,
            ObjectId { kind: ObjectKind::GenerationRoot, digest: base.digest() }, config, accounting, cancellation,
        ).await.map_err(WorkspaceError::Filesystem)?;
        let bytes = usize_to_u64(base_root.parents.capacity() * size_of::<GenerationId>());
        let base_root = Owned::new(base_root, allocation, bytes);
        let (source_root, allocation) = read_generation_root_with_accounting(
            &self.inner.objects, ObjectId { kind: ObjectKind::GenerationRoot, digest: source.id.digest() },
            config, accounting, cancellation,
        ).await.map_err(WorkspaceError::Filesystem)?;
        let bytes = usize_to_u64(source_root.parents.capacity() * size_of::<GenerationId>());
        let source_root = Owned::new(source_root, allocation, bytes);
        let result = async {
            if source_root.value.volume_id != source.workspace.volume.id
                || base_root.value.root_file_id != source_root.value.root_file_id
            { return Err(WorkspaceError::IncompatibleWorkspace); }
            let (diff, allocation) = diff_generation_file_tables_with_accounting(
                &self.inner.objects, base_root.value.file_table, source_root.value.file_table,
                maximum_changes, config, accounting, cancellation,
            ).await.map_err(WorkspaceError::Filesystem)?;
            if diff.truncated { drop(diff); drop(allocation); return Err(WorkspaceError::JoinLimit); }
            Ok((diff, allocation))
        }.await;
        source_root.retire(accounting).map_err(WorkspaceError::Filesystem)?;
        base_root.retire(accounting).map_err(WorkspaceError::Filesystem)?;
        result
    }

    pub(crate) async fn workspace_head_state(
        &self,
        workspace: &Volume<A, O>,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<(GenerationId, Head), WorkspaceError> {
        accounting.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let (root, head) = workspace.resolve_head_generation_with_native_work(original, cancellation).await?;
            return Ok((GenerationId::new(root.digest), head));
        }
        let result = workspace.resolve_head_generation(accounting.remaining()?, cancellation).await;
        let backend = match &result { Ok((_, _, work)) => *work, Err(failure) => *failure.work, };
        let prior = accounting.spent();
        accounting.record_completed(backend).map_err(|error| match error {
            FsError::Work(WorkError::Overflow) => FsError::RootReadWorkMerge {
                prior, backend, error: WorkError::Overflow,
            }, error => error,
        })?;
        let (root, head, _) = result.map_err(|failure| failure.error)?;
        Ok((GenerationId::new(root.digest), head))
    }

    pub(crate) async fn workspace_operation_generation(
        &self, workspace: &Volume<A, O>, operation_id: OperationId, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<(Option<(GenerationId, DurableCommit)>, KernelOwnedAllocation), WorkspaceError> {
        let mut native = None;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            cancellation.check().map_err(FsError::from)?;
            let observed = self.inner.authority.find_operation_with_native_work(
                volume_authority_id(workspace.id), operation_id, original, cancellation,
            ).await.map_err(FsError::from)?;
            native = Some((observed.0, KernelOwnedAllocation::NativeOriginal(observed.1)));
        }
        let observed = match native {
            Some(observed) => observed, None => {
                let result = self.observe_volume_operation(
                    workspace.id, operation_id, accounting.remaining()?, cancellation,
                ).await;
                let spent = match &result { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
                let (_, work, _, _) = accounting.standalone_parts().expect("standalone operation accounting");
                work.try_add_assign(&spent).map_err(FsError::from)?;
                (result.map_err(|failure| WorkspaceError::from(failure.error))?.value, KernelOwnedAllocation::Standalone)
            }
        };
        let generation = observed.0.as_ref().map(|record| {
            generation_from_operation_record_value_with_accounting(record, workspace.id, operation_id, accounting)
                .map(|root| GenerationId::new(root.digest))
        }).transpose()?;
        Ok((generation.zip(observed.0), observed.1))
    }

    /// Verifies the exact publication identity of a committed workspace join.
    pub(crate) async fn verify_workspace_join_commit<W: FsWork>(
        &self, target: &Volume<A, O>, witness: &JoinCommitWitness, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<bool, WorkspaceError> {
        let mut accounting = budget.accounting(cancellation);
        let standalone = readonly_standalone_live(&accounting).is_some();
        let result = async {
            if !standalone { accounting.check()?; }
            if target.id != witness.target_workspace || witness.maximum_generations == 0
                || witness.maximum_changes == 0 || witness.maximum_conflicts == 0
                || witness.source_workspace == witness.target_workspace || witness.expected_target == witness.result_generation
            { return Ok(false); }
            let mut native = None;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                let observed = self.inner.authority.find_operation_with_native_work(
                    volume_authority_id(target.id), witness.operation_id, original, cancellation,
                ).await.map_err(FsError::from)?;
                native = Some((observed.0, KernelOwnedAllocation::NativeOriginal(observed.1)));
            }
            let observed = match native {
                Some(observed) => observed, None => {
                    let observed = self.observe_volume_operation(target.id, witness.operation_id, accounting.remaining()?, cancellation).await;
                    let completed = match &observed { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
                    *accounting.standalone_parts().expect("standalone join verification").1 = completed;
                    (observed.map_err(WorkspaceError::engine)?.value, KernelOwnedAllocation::Standalone)
                }
            };
            let Some(commit) = observed.0.as_ref() else { return Ok(false); };
            let generation = if standalone {
                generation_from_record(commit, target.id, accounting.spent()).map_err(WorkspaceError::engine)?
            } else { generation_from_record_value_with_accounting(commit, target.id, &mut accounting)? };
            if generation.digest != witness.result_generation.digest() { return Ok(false); }
            let hashing = WorkCounters {
                bytes_hashed: WORKSPACE_JOIN_OPERATION_DOMAIN.len() as u64 + 3 * 16 + 4 * 32 + 1 + 3 * 4,
                ..WorkCounters::UNCHARGED
            };
            if !standalone { accounting.admit(hashing)?; }
            let context = witness.operation_context();
            if !standalone { accounting.record_completed(hashing)?; }
            let request = PublishGenerationRequest {
                authority_id: volume_authority_id(target.id), volume_id: target.id, epoch: witness.expected_head.epoch,
                expected: witness.expected_head, operation_id: witness.operation_id,
                generation_root: ObjectId { kind: ObjectKind::GenerationRoot, digest: witness.result_generation.digest() },
            };
            let expected = if standalone { contextual_publication_fingerprint(request, context) } else {
                contextual_publication_fingerprint_with_accounting(request, context, &mut accounting)?
            };
            Ok(commit.fingerprint == expected)
        }.await;
        budget.finish(result, &accounting)
    }

    pub(crate) async fn workspace_conflict_record(
        &self,
        workspace: &Volume<A, O>,
        generation: GenerationId,
        file_id: FileId,
    ) -> Result<Option<FileRecord>, WorkspaceError> {
        let cancellation = CancellationToken::new();
        let generation_root = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: generation.digest(),
        };
        let (root, work) = read_generation_root(
            &self.inner.objects,
            generation_root,
            workspace.config,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(WorkspaceError::engine)?;
        let lookup = lookup_file_record(
            &self.inner.objects,
            root.file_table,
            file_id,
            DecodeLimits::for_volume(workspace.config),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(WorkspaceError::engine)?;
        let _ = work;
        Ok(lookup.value)
    }

    pub(crate) async fn workspace_conflict_bytes(
        &self,
        workspace: &Volume<A, O>,
        record: FileRecord,
    ) -> Result<Option<acyclic_stream::OwnedBytes>, WorkspaceError> {
        let logical_bytes = match record.payload {
            FilePayload::InlineRegular(data) => {
                return Ok(Some(acyclic_stream::OwnedBytes::from(data.as_bytes().to_vec())));
            }
            FilePayload::Regular { logical_bytes, .. } => logical_bytes,
            _ => return Ok(None),
        };
        if logical_bytes > workspace.config.limits.maximum_read_bytes {
            return Ok(None);
        }
        let read = read_file_range_async(
            &self.inner.objects,
            FileRangeRequest {
                record,
                range: ByteRange {
                    offset: 0,
                    length: logical_bytes,
                },
                maximum_spans: workspace.config.limits.maximum_directory_page_entries,
                limits: DecodeLimits::for_volume(workspace.config),
                budget: WorkBudget::UNBOUNDED,
            },
            &CancellationToken::new(),
        )
        .await
        .map_err(WorkspaceError::engine)?;
        Ok(Some(read.bytes))
    }

    /// Whether two single-link records hold one file state: the same kind,
    pub(crate) async fn same_file_state(
        &self,
        workspace: &Volume<A, O>,
        left: FileRecord,
        right: FileRecord,
    ) -> Result<bool, WorkspaceError> {
        if left.kind != right.kind
            || left.payload != right.payload
            || left.link_count != 1
            || right.link_count != 1
        {
            return Ok(false);
        }
        if left.metadata == right.metadata {
            return Ok(true);
        }
        let mut metadata = Vec::with_capacity(2);
        for object in [left.metadata, right.metadata] {
            let read = self
                .inner
                .objects
                .read(
                    object,
                    workspace.config.limits.maximum_object_bytes,
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map_err(WorkspaceError::engine)?;
            metadata.push(
                crate::kernel::decode_file_metadata(&read.value.bytes, DecodeLimits::for_volume(workspace.config), &mut |_| Ok::<_, FsError>(()))
                .map_err(WorkspaceError::engine)?
                .without_timestamps(),
            );
        }
        Ok(metadata.first() == metadata.get(1))
    }

    pub(crate) async fn workspace_conflict_metadata(
        &self,
        workspace: &Volume<A, O>,
        record: FileRecord,
    ) -> Result<Vec<u8>, WorkspaceError> {
        let read = self
            .inner
            .objects
            .read(
                record.metadata,
                workspace.config.limits.maximum_object_bytes,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(WorkspaceError::engine)?;
        crate::kernel::decode_file_metadata(&read.value.bytes, DecodeLimits::for_volume(workspace.config), &mut |_| Ok::<_, FsError>(()))
        .map_err(WorkspaceError::engine)?;
        Ok(read.value.bytes.to_vec())
    }

    pub(crate) async fn stage_merge_regular_record(
        &self,
        workspace: &Volume<A, O>,
        mut record: FileRecord,
        bytes: impl Into<acyclic_stream::OwnedBytes>,
    ) -> Result<FileRecord, WorkspaceError> {
        let bytes = bytes.into();
        if record.kind != FileKind::Regular
            || u64::try_from(bytes.len()).unwrap_or(u64::MAX)
                > workspace.config.limits.maximum_read_bytes
        {
            return Err(WorkspaceError::InvalidMergeResolution);
        }
        if bytes.len() <= crate::kernel::MAXIMUM_INLINE_FILE_BYTES {
            record.payload = FilePayload::InlineRegular(
                InlineFileData::new(&bytes).map_err(WorkspaceError::engine)?,
            );
            return Ok(record);
        }
        let maximum_blob_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX).max(1);
        let mut source = std::io::Cursor::new(bytes);
        let blob = build_blob(
            &self.inner.objects,
            &mut source,
            BlobBuildOptions {
                chunk_bytes: u32::try_from(
                    workspace
                        .config
                        .limits
                        .maximum_object_bytes
                        .min(1024 * 1024),
                )
                .unwrap_or(u32::MAX)
                .max(1),
                page_items: workspace.config.limits.maximum_directory_page_entries,
                page_bytes: u32::try_from(workspace.config.limits.maximum_object_bytes)
                    .unwrap_or(u32::MAX)
                    .max(1),
                maximum_blob_bytes,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(WorkspaceError::engine)?;
        let payload = apply_regular_mutation_async(
            &self.inner.objects,
            FilePayload::InlineRegular(
                InlineFileData::new(&[]).map_err(WorkspaceError::engine)?,
            ),
            RegularMutation::Write {
                offset: 0,
                length: blob.logical_bytes,
                content: blob.root,
                content_offset: 0,
            },
            workspace.config,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(WorkspaceError::engine)?;
        record.payload = payload.payload;
        Ok(record)
    }

    pub(crate) async fn stage_merge_metadata(
        &self,
        workspace: &Volume<A, O>,
        bytes: &[u8],
    ) -> Result<ObjectId, WorkspaceError> {
        let metadata = crate::kernel::decode_file_metadata(bytes, DecodeLimits::for_volume(workspace.config), &mut |_| Ok::<_, FsError>(())).map_err(WorkspaceError::engine)?;
        let cancellation = CancellationToken::new();
        let encoded = crate::kernel::encode::<FsError>(WorkBudget::UNBOUNDED, Some(&cancellation), 0, |observe| crate::kernel::encode_file_metadata(metadata, observe, FsError::EncodedAllocationFailed)).map_err(|failure| WorkspaceError::engine(failure.error))?;
        let (object, _) = self
            .put_encoded(ObjectKind::Metadata, encoded, WorkCounters::default(), WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(WorkspaceError::engine)?;
        Ok(object)
    }

    pub(crate) async fn restore_workspace_generation(
        &self,
        workspace: &Volume<A, O>,
        generation: GenerationId,
        expected: GenerationId,
        operation_id: OperationId,
        permit: PublicationPermit,
    ) -> Result<crate::WorkspaceRestore<GenerationId>, WorkspaceError> {
        let cancellation = CancellationToken::new();
        if let Some(recorded) = self
            .workspace_operation_generation(workspace, operation_id,
                &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation), &cancellation)
            .await?.0.map(|(generation, record)| { drop(record); generation })
        {
            return Ok(if recorded == generation {
                crate::WorkspaceRestore::AlreadyRestored(recorded)
            } else {
                crate::WorkspaceRestore::IdempotencyConflict
            });
        }
        let (current, authority_head, _) = workspace
            .resolve_head_generation(WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(WorkspaceError::engine)?;
        let current = GenerationId::new(current.digest);
        if current != expected {
            return Ok(crate::WorkspaceRestore::Stale(current));
        }
        if current == generation {
            return Ok(crate::WorkspaceRestore::Current(current));
        }
        let generation_root = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: generation.digest(),
        };
        let (root, _) = read_generation_root(
            &self.inner.objects,
            generation_root,
            workspace.config,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(WorkspaceError::engine)?;
        if root.volume_id != workspace.id {
            return Err(WorkspaceError::ForeignGeneration);
        }
        let publication = publish_generation_async_with_permit(
            &self.inner.objects,
            &self.inner.authority,
            PublishGenerationRequest {
                authority_id: volume_authority_id(workspace.id),
                volume_id: workspace.id,
                epoch: authority_head.epoch,
                expected: authority_head,
                operation_id,
                generation_root,
            },
            PublicationIntent::guarded(None, permit),
            closure_limits(workspace.config),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(WorkspaceError::engine)?;
        Ok(match publication.outcome {
            AppendOutcome::Committed(_) => {
                crate::WorkspaceRestore::Restored(publication.proof.generation_id)
            }
            AppendOutcome::AlreadyCommitted(_) => {
                crate::WorkspaceRestore::AlreadyRestored(
                    publication.proof.generation_id,
                )
            }
            AppendOutcome::Conflict { .. } => {
                let (actual, _, _) = workspace
                    .resolve_head_generation(WorkBudget::UNBOUNDED, &cancellation)
                    .await
                    .map_err(WorkspaceError::engine)?;
                crate::WorkspaceRestore::Stale(GenerationId::new(actual.digest))
            }
            AppendOutcome::Fenced { .. } => crate::WorkspaceRestore::Fenced,
            AppendOutcome::IdempotencyConflict { .. } => {
                crate::WorkspaceRestore::IdempotencyConflict
            }
        })
    }

    /// Resolves where a fork stands against the parent it was forked from:
    async fn rebase_lineage(
        &self,
        target: &Volume<A, O>,
        maximum_generations: u32,
        cancellation: &CancellationToken,
    ) -> Result<RebaseLineage<A, O>, WorkspaceError> {
        let (target_object, target_head, _) = target
            .resolve_head_generation(WorkBudget::UNBOUNDED, cancellation)
            .await
            .map_err(WorkspaceError::engine)?;
        let (target_root, _) = read_generation_root(
            &self.inner.objects,
            target_object,
            target.config,
            WorkBudget::UNBOUNDED,
            cancellation,
        )
        .await
        .map_err(WorkspaceError::engine)?;

        let mut cursor = target_object;
        let mut adopted = None;
        let mut filtered_baseline = None;
        for _ in 0..maximum_generations {
            let (root, _) = read_generation_root(
                &self.inner.objects,
                cursor,
                target.config,
                WorkBudget::UNBOUNDED,
                cancellation,
            )
            .await
            .map_err(WorkspaceError::engine)?;
            if root.volume_id != target.id {
                adopted = Some((cursor, root));
                break;
            }
            if filtered_baseline.is_none()
                && root.required_features & GenerationRoot::FILTERED_FORK != 0
            {
                filtered_baseline = Some(root.clone());
            }
            let Some(parent) = root.parents.first() else {
                return Err(WorkspaceError::NotFork);
            };
            cursor = ObjectId {
                kind: ObjectKind::GenerationRoot,
                digest: parent.digest(),
            };
        }
        let Some((base_object, base_root)) = adopted else {
            return Err(WorkspaceError::LineageLimit);
        };
        let source = self
            .open_volume(base_root.volume_id, WorkBudget::UNBOUNDED, cancellation)
            .await
            .map_err(WorkspaceError::engine)?
            .value;
        if source.config != target.config || base_root.root_file_id != target_root.root_file_id {
            return Err(WorkspaceError::IncompatibleWorkspace);
        }
        let (source_object, _, _) = source
            .resolve_head_generation(WorkBudget::UNBOUNDED, cancellation)
            .await
            .map_err(WorkspaceError::engine)?;
        Ok(RebaseLineage {
            target_object,
            target_head,
            target_root,
            base_object,
            base_root: filtered_baseline.unwrap_or(base_root),
            source,
            source_object,
        })
    }

    /// The parent head that a fork would rebase onto, or `None` when the fork
    pub(crate) async fn parent_advance(
        &self,
        target: &Volume<A, O>,
        maximum_generations: u32,
    ) -> Result<Option<GenerationId>, WorkspaceError> {
        let lineage = self
            .rebase_lineage(target, maximum_generations, &CancellationToken::new())
            .await?;
        Ok((lineage.source_object != lineage.base_object)
            .then(|| GenerationId::new(lineage.source_object.digest)))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one live rebase replays every candidate change against the advanced head under a single fence"
    )]
    pub(crate) async fn live_rebase_workspace(
        &self,
        request: WorkspaceRebaseRequest<'_, A, O>,
    ) -> Result<WorkspaceJoinOutcome, WorkspaceError> {
        let WorkspaceRebaseRequest {
            target,
            operation_id,
            maximum_generations,
            maximum_changes,
            maximum_conflicts,
        } = request;
        if maximum_generations == 0 || maximum_changes == 0 || maximum_conflicts == 0 {
            return Err(WorkspaceError::JoinLimit);
        }
        let cancellation = CancellationToken::new();
        let RebaseLineage {
            target_object,
            target_head: expected_head,
            target_root,
            base_object,
            base_root,
            source,
            source_object,
        } = self
            .rebase_lineage(target, maximum_generations, &cancellation)
            .await?;
        if source_object == base_object {
            return Ok(WorkspaceJoinOutcome::NoChanges(GenerationId::new(
                target_object.digest,
            )));
        }
        let (source_root, _) = read_generation_root(
            &self.inner.objects,
            source_object,
            target.config,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(WorkspaceError::engine)?;
        if source_root.volume_id != source.id
            || source_root.root_file_id != target_root.root_file_id
        {
            return Err(WorkspaceError::IncompatibleWorkspace);
        }

        let normalize = |root: &GenerationRoot| GenerationRoot {
            volume_id: target.id,
            root_file_id: target_root.root_file_id,
            file_table: root.file_table, parents: Vec::new(), required_features: root.continuation_features(),
        };
        let normalized_base = normalize(&base_root);
        let normalized_source = normalize(&source_root);
        drop(base_root);
        drop(source_root);
        let retained_root_bytes = root_parent_bytes(&target_root).map_err(WorkspaceError::engine)?;
        let (normalized_base_object, _) = self
            .put_generation_root(
                &normalized_base, WorkCounters::default(), WorkBudget::UNBOUNDED, &cancellation, retained_root_bytes,
            )
            .await
            .map_err(WorkspaceError::engine)?;
        let (normalized_source_object, _) = self
            .put_generation_root(
                &normalized_source, WorkCounters::default(), WorkBudget::UNBOUNDED, &cancellation, retained_root_bytes,
            )
            .await
            .map_err(WorkspaceError::engine)?;
        let merged = merge_generation_async(
            &self.inner.objects,
            MergeGenerationRequest {
                base_generation: normalized_base_object,
                base: normalized_base,
                ours_generation: Some(normalized_source_object), ours: normalized_source, theirs_generation: target_object,
                theirs: target_root, retain_theirs_parent: false,
                maximum_changes,
                maximum_conflicts,
                resolutions: BTreeMap::new(),
            },
            DecodeLimits::for_volume(target.config),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(WorkspaceError::engine)?;
        let merged_root = match merged.value {
            MergeGenerationOutcome::Conflicted {
                conflicts,
                truncated,
            } => return Ok(WorkspaceJoinOutcome::Conflicted(conflicts, truncated)),
            MergeGenerationOutcome::Prepared { root, .. } => root,
        };
        let parent_buffer = root_parent_buffer(
            &[GenerationId::new(source_object.digest)],
            root_parent_bytes(&merged_root).map_err(WorkspaceError::engine)?, WorkBudget::UNBOUNDED, &cancellation,
        ).map_err(WorkspaceError::engine)?;
        let rebased_root = GenerationRoot {
            volume_id: target.id, root_file_id: merged_root.root_file_id, file_table: merged_root.file_table, parents: parent_buffer.value,
            required_features: merged_root.continuation_features(),
        };
        drop(merged_root);
        let (candidate, _) = self.put_generation_root(
                &rebased_root, parent_buffer.work, WorkBudget::UNBOUNDED,
                &cancellation, root_parent_bytes(&rebased_root).map_err(WorkspaceError::engine)?,
            ).await.map_err(WorkspaceError::engine)?;
        let publication = publish_generation(
            &self.inner.objects,
            &self.inner.authority,
            PublishGenerationRequest {
                authority_id: volume_authority_id(target.id),
                volume_id: target.id,
                epoch: expected_head.epoch,
                expected: expected_head,
                operation_id,
                generation_root: candidate,
            },
            closure_limits(target.config),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(WorkspaceError::engine)?;
        Ok(match publication.outcome {
            AppendOutcome::Committed(_) => {
                WorkspaceJoinOutcome::Applied(publication.proof.generation_id)
            }
            AppendOutcome::AlreadyCommitted(_) => {
                WorkspaceJoinOutcome::AlreadyApplied(publication.proof.generation_id)
            }
            AppendOutcome::Conflict { .. } => {
                let (actual, _, _) = target
                    .resolve_head_generation(WorkBudget::UNBOUNDED, &cancellation)
                    .await
                    .map_err(WorkspaceError::engine)?;
                WorkspaceJoinOutcome::Stale(GenerationId::new(actual.digest))
            }
            AppendOutcome::Fenced { .. } => WorkspaceJoinOutcome::Fenced,
            AppendOutcome::IdempotencyConflict { .. } => WorkspaceJoinOutcome::IdempotencyConflict,
        })
    }

    pub(crate) async fn apply_workspace_join(
        &self, request: WorkspaceJoinRequest<'_, A, O>,
    ) -> Result<WorkspaceJoinOutcome, WorkspaceError> {
        let cancellation = CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        let output = self.apply_workspace_join_with_accounting(
            request, &mut accounting, &cancellation, &mut (),
        ).await?;
        Ok(output.value)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn apply_workspace_join_guarded_with_native_work<'original, F, Fut, E>(
        &self, request: WorkspaceJoinRequest<'_, A, O, &[(MergeConflict, MergeConflictResolution)]>,
        original: &'original NativeMaterializationWork, cancellation: &CancellationToken, before_publication: &mut F,
    ) -> Result<(WorkspaceJoinOutcome, NativeAllocation), E>
    where
        F: for<'materialization> FnMut(&'materialization NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static, E: From<FsError> + From<WorkspaceError> + Send + 'static,
    {
        let mut accounting = KernelAccounting::native(original, cancellation);
        let output = self.apply_workspace_join_with_accounting(
            request, &mut accounting, cancellation, &mut (original, before_publication),
        ).await?;
        match output.allocation {
            KernelOwnedAllocation::NativeOriginal(allocation) =>
                Ok((output.value, allocation)),
            KernelOwnedAllocation::Standalone =>
                unreachable!("native join retains original output custody"),
        }
    }

    async fn apply_workspace_join_with_accounting<R, P>(
        &self, request: WorkspaceJoinRequest<'_, A, O, R>, accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken, publication: &mut P,
    ) -> Result<Owned<WorkspaceJoinOutcome>, P::Error>
    where
        R: WorkspaceJoinResolutions, P: WorkspaceJoinPublication<A, O>,
    {
        use Owned;
        accounting.check()?;
        let resolutions_digest = hash_join_resolutions(request.resolutions.view(), accounting)?;
        let hashing = WorkCounters {
            bytes_hashed: WORKSPACE_JOIN_OPERATION_DOMAIN.len() as u64 + 189,..WorkCounters::UNCHARGED
        };
        accounting.admit(hashing)?;
        let operation_context = request.operation_context(resolutions_digest);
        accounting.record_completed(hashing)?;
        let WorkspaceJoinRequest {
            target, base, source, expected_target, expected_head, history, operation_id,
            permit, maximum_generations, maximum_changes, maximum_conflicts, resolutions,
        } = request;
        if maximum_generations == 0 || maximum_changes == 0 || maximum_conflicts == 0 {
            return Err(WorkspaceError::JoinLimit.into());
        }
        if !Arc::ptr_eq(&self.inner, &source.workspace.volume.fs.inner)
            || source.workspace.volume.config != target.config
        {
            return Err(WorkspaceError::IncompatibleWorkspace.into());
        }
        let current_target = self.workspace_head_state(target, accounting, cancellation).await?.0;
        let target_object = ObjectId { kind: ObjectKind::GenerationRoot, digest: expected_target.digest() };
        let base_object = ObjectId { kind: ObjectKind::GenerationRoot, digest: base.id.digest() };
        let source_object = ObjectId { kind: ObjectKind::GenerationRoot, digest: source.id.digest() };
        let mut base_root = self.read_join_root_with_accounting(base_object, target.config, accounting, cancellation).await?;
        let mut source_root = self.read_join_root_with_accounting(source_object, target.config, accounting, cancellation).await?;
        let target_root = self.read_join_root_with_accounting(target_object, target.config, accounting, cancellation).await?;
        let result = async {
            if source_root.value.volume_id != source.workspace.volume.id
                || target_root.value.volume_id != target.id
                || base_root.value.root_file_id != target_root.value.root_file_id
                || source_root.value.root_file_id != target_root.value.root_file_id
            { return Err(WorkspaceError::IncompatibleWorkspace.into()); }
            // Normalize scalar identity in place; each decoded parent vector keeps
            base_root.value.required_features = base_root.value.continuation_features();
            base_root.value.volume_id = target.id;
            source_root.value.required_features = source_root.value.continuation_features();
            source_root.value.volume_id = target.id;
            let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| crate::kernel::encode_generation_root(&base_root.value, observe, allocation_error))?;
            let normalized_base_object = self.put_encoded_with_accounting(
                ObjectKind::GenerationRoot, encoded, accounting, cancellation,
            ).await?;
            let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| crate::kernel::encode_generation_root(&source_root.value, observe, allocation_error))?;
            let normalized_source_object = self.put_encoded_with_accounting(
                ObjectKind::GenerationRoot, encoded, accounting, cancellation,
            ).await?;
            if normalized_source_object == target_object && current_target == expected_target {
                return workspace_join_scalar(WorkspaceJoinOutcome::NoChanges(current_target), accounting).map_err(P::Error::from);
            }
            let merge_request = crate::kernel::MergeGenerationBorrowedRequest {
                base_generation: normalized_base_object, base: &base_root.value,
                ours_generation: Some(target_object), ours: &target_root.value,
                theirs_generation: normalized_source_object, theirs: &source_root.value,
                retain_theirs_parent: history == crate::workspace::JoinHistory::Merge, maximum_changes, maximum_conflicts,
            };
            let (merged, allocation) = match resolutions.view() {
                WorkspaceJoinResolutionView::Map(map) =>
                    merge_generation_with_borrowed_map_resolutions_with_accounting(
                        &self.inner.objects, &merge_request, map, DecodeLimits::for_volume(target.config), accounting, cancellation,
                    ).await?,
                WorkspaceJoinResolutionView::Slice(slice) =>
                    merge_generation_with_borrowed_resolutions_with_accounting(
                        &self.inner.objects, merge_request, slice, DecodeLimits::for_volume(target.config), accounting, cancellation,
                    ).await?,
            };
            let (candidate, root) = match merged {
                MergeGenerationOutcome::Conflicted { conflicts, truncated } => {
                    let mut conflicts = Owned::new(
                        WorkspaceJoinOutcome::Conflicted(conflicts, truncated), allocation, 0,
                    );
                    if let WorkspaceJoinOutcome::Conflicted(values, _) = &conflicts.value {
                        conflicts.bytes = workspace_join_conflict_bytes(values)?;
                    }
                    return Ok(conflicts);
                }
                MergeGenerationOutcome::Prepared { generation_root, root, .. } => {
                    let mut root = Owned::new(root, allocation, 0);
                    root.bytes = root_parent_bytes(&root.value)?;
                    (generation_root, root)
                }
            };
            let unchanged = root.value.file_table == target_root.value.file_table;
            root.retire(accounting)?;
            if unchanged {
                return workspace_join_scalar(WorkspaceJoinOutcome::NoChanges(current_target), accounting).map_err(P::Error::from);
            }
            // This immutable normalized history parent is outside the candidate's
            let _source_hold = if history == crate::workspace::JoinHistory::Merge {
                Some(self.flush_join_parent_with_accounting(normalized_source_object, accounting, cancellation).await?)
            } else { None };
            let published = publication.publish(
                &self.inner.objects, &self.inner.authority, PublishGenerationRequest {
                    authority_id: volume_authority_id(target.id), volume_id: target.id,
                    epoch: expected_head.epoch, expected: expected_head, operation_id, generation_root: candidate,
                }, PublicationIntent::guarded(Some(operation_context), permit), closure_limits(target.config), accounting, cancellation,
            ).await?;
            let outcome = match published {
                WorkspaceJoinPublished::Committed(id) => WorkspaceJoinOutcome::Joined(id, resolutions_digest),
                WorkspaceJoinPublished::AlreadyCommitted(id) => WorkspaceJoinOutcome::AlreadyJoined(id, resolutions_digest),
                WorkspaceJoinPublished::Conflict => WorkspaceJoinOutcome::Stale(
                    self.workspace_head_state(target, accounting, cancellation).await?.0,
                ), WorkspaceJoinPublished::Fenced => WorkspaceJoinOutcome::Fenced,
                WorkspaceJoinPublished::IdempotencyConflict => WorkspaceJoinOutcome::IdempotencyConflict,
            };
            workspace_join_scalar(outcome, accounting).map_err(P::Error::from)
        }.await;
        target_root.retire(accounting)?;
        source_root.retire(accounting)?;
        base_root.retire(accounting)?;
        result
    }

    async fn read_join_root_with_accounting(
        &self, object: ObjectId, config: VolumeConfig, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<Owned<GenerationRoot>, FsError> {
        let (root, allocation) = read_generation_root_with_accounting(
            &self.inner.objects, object, config, accounting, cancellation,
        ).await?;
        let mut root = Owned::new(root, allocation, 0);
        root.bytes = root_parent_bytes(&root.value)?;
        Ok(root)
    }

    async fn flush_join_parent_with_accounting(
        &self, parent: ObjectId, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<crate::PublicationHold, FsError> {
        let objects = [parent];
        let scope = crate::PublicationScope::Closure { objects: &objects, proven_at: self.inner.objects.collection_sweeps() };
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            return self.inner.objects.flush_before_publish_with_native_work(scope, original, cancellation).await.map_err(Into::into);
        }
        let flushed = self.inner.objects.flush_before_publish(scope, accounting.remaining()?, cancellation).await;
        let spent = match &flushed { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
        merge_creation_backend_work(accounting, spent)?;
        flushed.map(|receipt| receipt.value).map_err(|failure| failure.error.into())
    }

    /// Executes one active local-residency prediction through this filesystem's
    pub async fn execute_residency(
        &self,
        speculator: &crate::ResidencySpeculator,
        permit: crate::ResidencyPermit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::storage::ObjectResult<u64> {
        crate::execute_residency(
            speculator,
            &self.inner.objects,
            permit,
            budget,
            cancellation,
        )
        .await
    }

    /// Creates a fresh volume identity and its authenticated empty generation.
    pub async fn create_volume(
        &self,
        config: VolumeConfig,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Volume<A, O>> {
        self.create_volume_with_id(VolumeId::new(), config, budget, cancellation)
            .await
    }

    /// Idempotently creates one caller-selected volume identity.
    pub async fn create_volume_with_id(
        &self,
        volume_id: VolumeId,
        config: VolumeConfig,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Volume<A, O>> {
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.create_volume",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
            work.items = crate::obs::Empty,
            work.bytes = crate::obs::Empty,
            work.durability = crate::obs::Empty,
            volume_id = crate::obs::hex(&volume_id.into_bytes()),
        );
        let result = crate::obs::in_span(
            &span,
            self.create_volume_with_id_operation(volume_id, config, None, budget, cancellation),
        )
        .await;
        result.observe_on(&span, "create_volume")
    }

    async fn create_volume_with_id_operation(
        &self,
        volume_id: VolumeId,
        config: VolumeConfig,
        operation_id: Option<OperationId>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Volume<A, O>> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = self.create_volume_with_id_operation_with_accounting(
            volume_id, config, operation_id, &mut accounting, cancellation, |id, commit, budget| async move {
                    let created = self.inner.authority.create_authority_with_first_record(
                        id, commit, budget, cancellation,
                    ).await;
                    let work = match &created { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
                    (created.map(|receipt| receipt.value).map_err(|failure| FsError::from(failure.error)), work)
                },
        ).await;
        let work = accounting.spent();
        result.map(|(value, _)| FsReceipt { value, work }).map_err(|error| OperationFailure::new(error, work))
    }

    async fn create_volume_with_id_operation_with_accounting<E, P, Fut>(
        &self, volume_id: VolumeId, config: VolumeConfig, operation_id: Option<OperationId>,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken, publication: P,
    ) -> Result<(Volume<A, O>, ObjectId), E>
    where
        E: From<FsError> + Send,
        P: FnOnce(crate::AuthorityId, ProposedCommit, WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = (Result<bool, E>, WorkCounters)> + ProviderTask,
    {
        accounting.check()?;
        validate_volume_capabilities(config, self.inner.capabilities)?;
        let proven_at = self.inner.objects.collection_sweeps();
        let produced = self.build_empty_generation_with_accounting(volume_id, config, accounting, cancellation).await?;
        let generation_root = produced[3];
        let creation = VolumeCreated { volume_id, config, initial_generation_root: generation_root };
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let proof = prove_generation_closure_with_native_work(
                &self.inner.objects, generation_root, closure_limits(config), original, cancellation,
            ).await.map_err(FsError::from)?;
            let volume = self.publish_volume_creation_with_accounting(
                creation, operation_id, &proof.0.objects, proven_at, accounting, cancellation, publication,
            ).await?;
            return Ok((volume, generation_root));
        }
        let proof = prove_generation_closure(
            &self.inner.objects, generation_root, closure_limits(config), accounting.remaining()?, cancellation,
        ).await;
        let spent = match &proof { Ok(proof) => proof.work, Err(failure) => *failure.work, };
        merge_creation_backend_work(accounting, spent)?;
        let proof = proof.map_err(|failure| FsError::from(failure.error))?;
        let closure_bytes = (proof.objects.capacity() as u64)
            .checked_mul(size_of::<ObjectId>() as u64).ok_or(FsError::from(WorkError::Overflow))?;
        let root_bytes = root_parent_bytes(&proof.root)?;
        let owned_bytes = closure_bytes.checked_add(root_bytes).ok_or(FsError::from(WorkError::Overflow))?;
        let mut allocation = accounting.claim_owned(owned_bytes, 0)?;
        let result = self.publish_volume_creation_with_accounting(
            creation, operation_id, &proof.objects, proven_at, accounting, cancellation, publication,
        ).await;
        drop(proof);
        accounting.release_owned(&mut allocation, owned_bytes)?;
        result.map(|volume| (volume, generation_root))
    }

    /// Opens one volume by reconstructing its immutable configuration from the
    pub async fn open_volume(
        &self,
        volume_id: VolumeId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Volume<A, O>> {
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.open_volume",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
            work.items = crate::obs::Empty,
            work.bytes = crate::obs::Empty,
            work.durability = crate::obs::Empty,
            volume_id = crate::obs::hex(&volume_id.into_bytes()),
        );
        let result = crate::obs::in_span(&span, async move {
            cancellation
                .check()
                .map_err(|error| OperationFailure::before_work(error.into()))?;
            let observed = self.read_creation_record(volume_id, budget, cancellation).await?;
            let creation = observed.value.0;
            let work = observed.work;
            validate_volume_capabilities(creation.config, self.inner.capabilities)
                .map_err(|error| OperationFailure::new(error, work))?;
            Ok(FsReceipt {
                value: Volume {
                    fs: self.clone(),
                    id: volume_id,
                    config: creation.config,
                },
                work,
            })
        })
        .await;
        result.observe_on(&span, "open_volume")
    }

    /// Reads one exact immutable object for resumable manifest transfer.
    pub async fn export_object(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<crate::storage::ObjectRead> {
        let receipt = self
            .inner
            .objects
            .read(object_id, maximum_bytes, budget, cancellation)
            .await
            .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
        Ok(FsReceipt {
            value: receipt.value,
            work: receipt.work,
        })
    }

    /// Idempotently imports one immutable object under its canonical identity.
    pub async fn import_object(
        &self,
        object_id: ObjectId,
        bytes: impl Into<acyclic_stream::OwnedBytes>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        let receipt = self
            .inner
            .objects
            .put(object_id, bytes.into(), budget, cancellation)
            .await
            .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
        Ok(FsReceipt {
            value: (),
            work: receipt.work,
        })
    }

    /// Reads one ordered resumable page from a canonical generation manifest.
    pub async fn export_generation_batch(
        &self,
        manifest: &GenerationExportManifest,
        cursor: TransferCursor,
        maximum_objects: u32,
        maximum_object_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<GenerationTransferBatch> {
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.export_generation_batch",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
            work.items = crate::obs::Empty,
            work.bytes = crate::obs::Empty,
            work.durability = crate::obs::Empty,
        );
        let result = crate::obs::in_span(
            &span,
            export_generation_batch_async(
                &self.inner.objects,
                manifest,
                cursor,
                maximum_objects,
                maximum_object_bytes,
                budget,
                cancellation,
            ),
        )
        .await
        .map_err(|failure| OperationFailure::new(map_transfer_error(failure.error), *failure.work));
        result.observe_on(&span, "export_generation_batch")
    }

    /// Idempotently imports one manifest-aligned page of immutable bodies.
    pub async fn import_generation_batch(
        &self,
        manifest: &GenerationExportManifest,
        cursor: TransferCursor,
        bodies: &[acyclic_stream::OwnedBytes],
        maximum_objects: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<TransferCursor> {
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.import_generation_batch",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
            work.items = crate::obs::Empty,
            work.bytes = crate::obs::Empty,
            work.durability = crate::obs::Empty,
        );
        let result = crate::obs::in_span(
            &span,
            import_generation_batch_async(
                &self.inner.objects,
                manifest,
                cursor,
                bodies,
                maximum_objects,
                budget,
                cancellation,
            ),
        )
        .await
        .map_err(|failure| OperationFailure::new(map_transfer_error(failure.error), *failure.work));
        result.observe_on(&span, "import_generation_batch")
    }

    /// Restores a volume authority from a fully imported immutable generation.
    pub async fn restore_volume(
        &self,
        manifest: &GenerationExportManifest,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Volume<A, O>> {
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.restore_volume",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
            work.items = crate::obs::Empty,
            work.bytes = crate::obs::Empty,
            work.durability = crate::obs::Empty,
        );
        let result = crate::obs::in_span(&span, async move {
            cancellation
                .check()
                .map_err(|error| OperationFailure::before_work(error.into()))?;
            validate_volume_capabilities(manifest.config, self.inner.capabilities)
                .map_err(OperationFailure::before_work)?;
            let proven_at = self.inner.objects.collection_sweeps();
            let proof = authenticate_generation_export_manifest_async(
                &self.inner.objects,
                manifest,
                closure_limits(manifest.config),
                budget,
                cancellation,
            )
            .await
            .map_err(|failure| {
                OperationFailure::new(map_transfer_error(failure.error), *failure.work)
            })?;
            let work = proof.work;
            drop(proof);
            self.publish_volume_creation(
                VolumeCreated {
                    volume_id: manifest.volume_id, config: manifest.config,
                    initial_generation_root: manifest.generation_root,
                }, Some(operation_id),
                &manifest.objects,
                0,
                proven_at,
                work,
                budget,
                cancellation,
            )
            .await
        })
        .await;
        result.observe_on(&span, "restore_volume")
    }

    /// Durably creates a volume's authority with its creation record, once
    async fn publish_volume_creation(
        &self,
        creation: VolumeCreated, operation_id: Option<OperationId>,
        closure: &[ObjectId],
        closure_owned_bytes: u64,
        proven_at: u64,
        mut work: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Volume<A, O>> {
        let mut allocations = AllocationLedger::default();
        allocations.claim_bytes(closure_owned_bytes, 0, &mut work, budget)
            .map_err(|error| OperationFailure::new(crate::collection::handle_allocation_error(error), work))?;
        let result = {
            let mut accounting = KernelAccounting::borrowed_standalone(
                &mut allocations, &mut work, budget, cancellation,
            );
            self.publish_volume_creation_with_accounting(
                creation, operation_id, closure, proven_at, &mut accounting, cancellation, |id, commit, budget| async move {
                    let created = self.inner.authority.create_authority_with_first_record(
                        id, commit, budget, cancellation,
                    ).await;
                    let work = match &created { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
                    (created.map(|receipt| receipt.value).map_err(|failure| FsError::from(failure.error)), work)
                },
            ).await
        };
        result.map(|value| FsReceipt { value, work }).map_err(|error| OperationFailure::new(error, work))
    }

    async fn publish_volume_creation_with_accounting<E, P, Fut>(
        &self, creation: VolumeCreated, operation_id: Option<OperationId>, closure: &[ObjectId], proven_at: u64, accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken, publication: P,
    ) -> Result<Volume<A, O>, E>
    where
        E: From<FsError> + Send,
        P: FnOnce(crate::AuthorityId, ProposedCommit, WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = (Result<bool, E>, WorkCounters)> + ProviderTask,
    {
        let VolumeCreated { volume_id, config, .. } = creation;
        accounting.check()?;
        let operation_id = match operation_id {
            Some(operation_id) => operation_id, None => {
                accounting.charge(identity_hash_work(CREATION_OPERATION_DOMAIN))?;
                OperationId::from_bytes(derived_digest(CREATION_OPERATION_DOMAIN, volume_id).as_bytes()[..16].try_into().expect("sixteen digest bytes"))
            }
        };
        let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| crate::kernel::encode_volume_created(creation, observe, allocation_error))?;
        let capacity = encoded.0.capacity() as u64;
        let commit = creation_commit_with_accounting(operation_id, encoded, accounting)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let drained = self.inner.objects.flush_before_publish_with_native_work(
                crate::PublicationScope::Closure { objects: closure, proven_at }, original, cancellation,
            ).await.map_err(FsError::from)?;
            accounting.check()?;
            let (created, _) = publication(volume_authority_id(volume_id), commit, accounting.remaining()?).await;
            let created = created?;
            if !created { return Err(FsError::CreationRejected.into()); }
            drop(drained);
            return Ok(Volume { fs: self.clone(), id: volume_id, config });
        }
        let drained = self.inner.objects.flush_before_publish(
            crate::PublicationScope::Closure { objects: closure, proven_at }, accounting.remaining()?, cancellation,
        ).await;
        let spent = match &drained { Ok(receipt) => receipt.work, Err(failure) => *failure.work, };
        merge_creation_backend_work(accounting, spent)?;
        let drained = drained.map_err(|failure| FsError::from(failure.error))?;
        accounting.check()?;
        let (created, spent) = publication(volume_authority_id(volume_id), commit, accounting.remaining()?).await;
        let merged = merge_creation_backend_work(accounting, spent);
        let mut allocation = KernelOwnedAllocation::Standalone;
        accounting.release_owned(&mut allocation, capacity)?;
        merged?;
        let created = created?;
        if !created { return Err(FsError::CreationRejected.into()); }
        drop(drained);
        Ok(Volume { fs: self.clone(), id: volume_id, config })
    }

    async fn read_creation_record<W: FsWork>(
        &self, volume_id: VolumeId, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<(VolumeCreated, OperationId), FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                accounting.check()?;
                let records = self.inner.authority.replay_with_native_work(
                    volume_authority_id(volume_id), Sequence::GENESIS,
                    ReplayLimit { records: 1, payload_bytes: MAXIMUM_VOLUME_EVENT_BYTES }, original, cancellation,
                ).await?;
                return decode_first_creation_record(volume_id, &records.0, &mut accounting);
            }
            let allowance = accounting.standalone_parts().expect("standalone creation read").2;
            let records = self.inner.authority.replay(
                volume_authority_id(volume_id), Sequence::GENESIS,
                ReplayLimit { records: 1, payload_bytes: MAXIMUM_VOLUME_EVENT_BYTES }, allowance, cancellation,
            ).await;
            let completed = match &records { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
            *accounting.standalone_parts().expect("standalone creation read").1 = completed;
            let records = records.map_err(|failure| FsError::from(failure.error))?;
            decode_first_creation_record(volume_id, &records.value, &mut accounting)
        }.await;
        budget.finish(result, &accounting)
    }

    async fn build_empty_generation_with_accounting(
        &self,
        volume_id: VolumeId,
        config: VolumeConfig,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<[ObjectId; 4], FsError> {
        let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| crate::kernel::encode_file_metadata(FileMetadata::default(), observe, allocation_error))?;
        let metadata = self.put_encoded_with_accounting(ObjectKind::Metadata, encoded, accounting, cancellation).await?;
        let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| crate::kernel::encode_tree_page(&TreePage::Leaf(Vec::new()), config.limits.maximum_directory_page_entries, observe, allocation_error))?;
        let tree = self.put_encoded_with_accounting(ObjectKind::TreePage, encoded, accounting, cancellation).await?;
        accounting.charge(identity_hash_work(ROOT_FILE_DOMAIN))?;
        let root_file_id = FileId::from_bytes(derived_digest(ROOT_FILE_DOMAIN, volume_id).as_bytes()[..16].try_into().expect("sixteen digest bytes"));
        let record = FileRecord {
            file_id: root_file_id,
            kind: FileKind::Directory,
            link_count: 1,
            metadata, payload: FilePayload::Directory { entries: tree },
        };
        let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| crate::kernel::encode_file_table_leaf_records(std::slice::from_ref(&record), config.limits.maximum_directory_page_entries, None, observe, allocation_error))?;
        let file_table = self.put_encoded_with_accounting(
            ObjectKind::FileTablePage, encoded, accounting, cancellation,
        ).await?;
        let root = GenerationRoot {
            volume_id, root_file_id, file_table, parents: Vec::new(), required_features: 0,
        };
        let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| crate::kernel::encode_generation_root(&root, observe, allocation_error))?;
        let generation_root = self.put_encoded_with_accounting(
            ObjectKind::GenerationRoot, encoded, accounting, cancellation,
        ).await?;
        Ok([metadata, tree, file_table, generation_root])
    }

    async fn put_encoded_with_accounting(
        &self, kind: ObjectKind, mut encoded: (Vec<u8>, KernelOwnedAllocation), accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<ObjectId, FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let allocation = match &mut encoded.1 {
                KernelOwnedAllocation::NativeOriginal(allocation) => allocation, KernelOwnedAllocation::Standalone =>
                    return Err(FsError::HandleAllocationFailed),
            };
            let hashed = HashedObject::from_native_vec(kind, &mut encoded.0, allocation, original, cancellation)?;
            let object = hashed.object_id();
            self.inner.objects.put_hashed_with_native_work(hashed, original, cancellation).await?;
            return Ok(object);
        }
        let capacity = encoded.0.capacity() as u64;
        let hashing = WorkCounters {
            bytes_hashed: (encoded.0.len() as u64).checked_add(OBJECT_DIGEST_ENVELOPE_BYTES)
                .ok_or(WorkError::Overflow)?,..WorkCounters::UNCHARGED
        };
        let owner = WorkCounters {
            allocation_operations: u64::from(encoded.0.capacity() != encoded.0.len()),..WorkCounters::UNCHARGED
        };
        accounting.admit(owner)?;
        accounting.admit(hashing)?;
        let (payload, allocation) = encoded;
        let bytes = acyclic_stream::OwnedBytes::from(payload);
        if let Err(error) = accounting.record_completed(owner) { drop(bytes); drop(allocation); return Err(error); }
        drop(allocation);
        let hashed = HashedObject::new(kind, bytes);
        accounting.record_completed(hashing)?;
        let object = hashed.object_id();
        let receipt = self.inner.objects.put_hashed(hashed, accounting.remaining()?, cancellation).await;
        let spent = match &receipt { Ok(receipt) => receipt.work, Err(failure) => *failure.work, };
        let (allocations, work, budget, _) = accounting.standalone_parts().ok_or(FsError::HandleAllocationFailed)?;
        *work = work.with_backend(spent, allocations.live_bytes())?;
        allocations.release(capacity).map_err(|error| error.into_work_or(FsError::PendingMutationAllocationFailed))?;
        work.verify(budget)?;
        receipt.map_err(|failure| FsError::from(failure.error))?;
        Ok(object)
    }

    /// Includes retained caller buffers across both canonical encoding and storage.
    async fn put_generation_root(
        &self, root: &GenerationRoot, mut work: WorkCounters, budget: WorkBudget, cancellation: &CancellationToken, live_owned_bytes: u64,
    ) -> Result<(ObjectId, WorkCounters), OperationFailure<FsError>> {
        cancellation.check().map_err(|error| OperationFailure::new(error.into(), work))?;
        let mut allocations = AllocationLedger::default();
        allocations.claim_bytes(live_owned_bytes, 0, &mut work, budget)
            .map_err(|error| OperationFailure::new(error.into_work_or(FsError::PendingMutationAllocationFailed), work))?;
        let child_budget = remaining_owned(work, budget, live_owned_bytes)?;
        let encoded = crate::kernel::encode::<FsError>(child_budget, Some(cancellation), 0, |observe| crate::kernel::encode_generation_root(root, observe, FsError::EncodedAllocationFailed))
            .map_err(|failure| merge_simultaneous_failure(work, *failure.work, live_owned_bytes, failure.error))?;
        let (object, child_work) = self.put_encoded(
            ObjectKind::GenerationRoot, encoded, WorkCounters::default(), child_budget, cancellation,
        ).await.map_err(|failure| merge_simultaneous_failure(work, *failure.work, live_owned_bytes, failure.error))?;
        Ok((object, merge_simultaneous_work(work, child_work, live_owned_bytes, budget)?))
    }

    async fn put_encoded(
        &self,
        kind: ObjectKind,
        encoded: FsReceipt<Vec<u8>>,
        work: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<(ObjectId, WorkCounters), OperationFailure<FsError>> {
        let work = add(work, encoded.work)?;
        cancellation.check().map_err(|error| OperationFailure::new(error.into(), work))?;
        let live_bytes = usize_to_u64(encoded.value.capacity());
        let length = usize_to_u64(encoded.value.len());
        let hashed_bytes = length.checked_add(OBJECT_DIGEST_ENVELOPE_BYTES)
            .ok_or_else(|| OperationFailure::new(FsError::Work(WorkError::Overflow), work))?;
        let hashing = WorkCounters {
            bytes_hashed: hashed_bytes,
            // Bytes 1.12 allocates its owner header exactly for spare-capacity
            allocation_operations: u64::from(encoded.value.capacity() != encoded.value.len()),
            ..WorkCounters::default()
        };
        let mut prospective = work;
        prospective.charge(&hashing, &budget)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        let hashed = HashedObject::new(kind, encoded.value.into());
        let object = hashed.object_id();
        let backend_budget = remaining_owned(prospective, budget, live_bytes)?;
        let receipt = self
            .inner
            .objects
            .put_hashed(hashed, backend_budget, cancellation)
            .await
            .map_err(|failure| {
                merge_simultaneous_failure(
                    prospective,
                    *failure.work,
                    live_bytes,
                    failure.error.into(),
                )
            })?;
        Ok((
            object,
            merge_simultaneous_work(prospective, receipt.work, live_bytes, budget)?,
        ))
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Volume<A, O> {
    /// Stable volume identity.
    #[must_use]
    pub const fn id(&self) -> VolumeId {
        self.id
    }

    /// Immutable volume configuration reconstructed from authority.
    #[must_use]
    pub const fn config(&self) -> VolumeConfig {
        self.config
    }

    /// Computes one bounded semantic diff without scanning equal Merkle subtrees.
    pub async fn diff_generations(
        &self,
        before: GenerationId,
        after: GenerationId,
        maximum_changes: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<GenerationDiff> {
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.diff_generations",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
            work.items = crate::obs::Empty,
            work.bytes = crate::obs::Empty,
            work.durability = crate::obs::Empty,
            volume_id = crate::obs::hex(&self.id.into_bytes()),
        );
        let result = crate::obs::in_span(&span, async move {
            if maximum_changes == 0 {
                return Err(OperationFailure::before_work(FsError::InvalidDiff));
            }
            if before == after {
                return Ok(FsReceipt {
                    value: GenerationDiff {
                        files: Vec::new(),
                        bindings: Vec::new(),
                        truncated: false,
                    },
                    work: WorkCounters::default(),
                });
            }
            let mut work = WorkCounters::default();
            let before_id = ObjectId {
                kind: ObjectKind::GenerationRoot,
                digest: before.digest(),
            };
            let after_id = ObjectId {
                kind: ObjectKind::GenerationRoot,
                digest: after.digest(),
            };
            let (before_root, before_work) = read_generation_root(
                &self.fs.inner.objects,
                before_id,
                self.config,
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
            work = add(work, before_work)?;
            let (after_root, after_work) = read_generation_root(
                &self.fs.inner.objects,
                after_id,
                self.config,
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
            work = add(work, after_work)?;
            if before_root.volume_id != self.id || after_root.volume_id != self.id {
                return Err(OperationFailure::new(FsError::VolumeMismatch, work));
            }
            diff_generation_file_tables(
                &self.fs.inner.objects,
                before_root.file_table,
                after_root.file_table,
                maximum_changes,
                self.config,
                work,
                budget,
                cancellation,
            )
            .await
        })
        .await;
        result.observe_on(&span, "diff_generations")
    }

    /// Resolves the exact generation root object and, for a writable
    async fn resolve_checkout_generation(
        &self,
        selector: GenerationSelector,
        mode: CheckoutMode,
        work: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<(ObjectId, Option<Head>, WorkCounters), OperationFailure<FsError>> {
        match selector {
            GenerationSelector::Exact(generation_id) => {
                let mut work = work;
                let authority_head = if mode.access == AccessMode::ReadWrite {
                    if mode.consistency != ConsistencyMode::TrackingSafe
                        || mode.mutations != MutationMode::PrivateOverlay
                    {
                        return Err(OperationFailure::before_work(
                            FsError::WritableCheckoutRequiresHead,
                        ));
                    }
                    let resolved = self
                        .resolve_head_generation(remaining(work, budget)?, cancellation)
                        .await
                        .map_err(|failure| {
                            failure.map_with_prior_work(work, std::convert::identity)
                        })?;
                    work = add(work, resolved.2)?;
                    Some(resolved.1)
                } else {
                    None
                };
                Ok((
                    ObjectId {
                        kind: ObjectKind::GenerationRoot,
                        digest: generation_id.digest(),
                    },
                    authority_head,
                    work,
                ))
            }
            GenerationSelector::Head => {
                let resolved = self
                    .resolve_head_generation(remaining(work, budget)?, cancellation)
                    .await
                    .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
                let work = add(work, resolved.2)?;
                Ok((resolved.0, Some(resolved.1), work))
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn resolve_checkout_generation_with_native_work(
        &self, selector: GenerationSelector, mode: CheckoutMode, work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(ObjectId, Option<Head>), FsError> {
        match selector {
            GenerationSelector::Exact(generation_id) => {
                let authority_head = if mode.access == AccessMode::ReadWrite {
                    if mode.consistency != ConsistencyMode::TrackingSafe
                        || mode.mutations != MutationMode::PrivateOverlay {
                        return Err(FsError::WritableCheckoutRequiresHead);
                    }
                    Some(self.resolve_head_generation_with_native_work(work, cancellation).await?.1)
                } else { None };
                Ok((ObjectId { kind: ObjectKind::GenerationRoot, digest: generation_id.digest(), }, authority_head))
            }
            GenerationSelector::Head => {
                let (root, head) = self.resolve_head_generation_with_native_work(work, cancellation).await?;
                Ok((root, Some(head)))
            }
        }
    }

    /// Opens one immutable pinned checkout after authenticating its bounded root.
    pub async fn checkout(
        &self,
        selector: GenerationSelector,
        mode: CheckoutMode,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Checkout<A, O>> {
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.checkout",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
            work.items = crate::obs::Empty,
            work.bytes = crate::obs::Empty,
            work.durability = crate::obs::Empty,
            volume_id = crate::obs::hex(&self.id.into_bytes()),
        );
        let result = crate::obs::in_span(&span, async move {
            cancellation
                .check()
                .map_err(|error| OperationFailure::before_work(error.into()))?;
            validate_checkout(mode, self.config)?;
            let mut work = WorkCounters::default();
            let (generation_root, mut authority_head, next_work) = self
                .resolve_checkout_generation(selector, mode, work, budget, cancellation)
                .await?;
            work = next_work;
            let (root, root_work) = read_generation_root(
                &self.fs.inner.objects,
                generation_root,
                self.config,
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
            work = add(work, root_work)?;
            if root.volume_id != self.id {
                return Err(OperationFailure::new(FsError::VolumeMismatch, work));
            }
            if mode.access == AccessMode::ReadWrite
                && self.config.concurrency == ConcurrencyMode::ExclusiveWriter
            {
                let expected = authority_head.ok_or_else(|| {
                    OperationFailure::new(FsError::WritableCheckoutRequiresHead, work)
                })?;
                let fenced = self
                    .fs
                    .inner
                    .authority
                    .fence(
                        volume_authority_id(self.id),
                        expected,
                        remaining(work, budget)?,
                        cancellation,
                    )
                    .await
                    .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
                work = add(work, fenced.work)?;
                authority_head = Some(match fenced.value {
                    FenceOutcome::Advanced(head) => head,
                    FenceOutcome::Conflict { actual } => {
                        return Err(OperationFailure::new(
                            FsError::ExclusiveWriterConflict { actual },
                            work,
                        ));
                    }
                });
            }
            let mut allocations = AllocationLedger::default();
            allocations.claim_bytes(
                usize_to_u64(root.parents.capacity() * size_of::<GenerationId>()), 0, &mut work, budget,
            ).map_err(|error| OperationFailure::new(crate::collection::handle_allocation_error(error), work))?;
            let captured_bytes = usize_to_u64(CheckoutDependencies::empty_logical_owned_bytes());
            allocations.claim_bytes(captured_bytes, 1, &mut work, budget)
                .map_err(|error| OperationFailure::new(crate::collection::handle_allocation_error(error), work))?;
            let dependencies = CheckoutDependencies::new(
                std::iter::empty(),
                std::iter::empty(),
                self.config.limits.maximum_checkout_dependencies,
            )
            .map_err(|error| OperationFailure::new(error.into(), work))?;
            let lock_and_paths_bytes = usize_to_u64(size_of::<Mutex<Proof>>() + size_of::<ObservedPathMap>());
            allocations.claim_bytes(lock_and_paths_bytes, 2, &mut work, budget)
                .map_err(|error| OperationFailure::new(crate::collection::handle_allocation_error(error), work))?;
            let custody_bytes = allocations.claim_elements::<ProofDataCustody>(
                1, &mut work, budget,
            ).map_err(|error| OperationFailure::new(crate::collection::handle_allocation_error(error), work))?;
            let mut dependencies = DependencyLedger::new(dependencies);
            dependencies.proof().control = ProofDataClaim::control(
                &dependencies.data_custody, usize_to_u64(size_of::<Mutex<Proof>>()),
            );
            assert!(dependencies.proof().dependencies.attach_fresh_control_custody(
                &dependencies.data_custody,
            ), "a freshly constructed checkout owns its unique empty dependency map");
            Arc::get_mut(dependencies.proof().observed_paths.as_mut().expect("a fresh checkout owns its observed map"))
                .expect("a freshly constructed checkout owns its unique empty observed map").control =
                ProofDataClaim::control(&dependencies.data_custody, usize_to_u64(size_of::<ObservedPathMap>()));
            let proof_owned_bytes = captured_bytes.checked_add(lock_and_paths_bytes).and_then(|bytes| bytes.checked_add(custody_bytes))
                .ok_or_else(|| OperationFailure::new(FsError::Work(WorkError::Overflow), work))?;
            let base_root = crate::collection::clone_generation_root(
                &root, &mut allocations, &mut work, budget, &cancellation,
            ).map_err(|error| OperationFailure::new(error, work))?;
            dependencies.base_root_control = ProofDataClaim::control(
                &dependencies.data_custody, root_parent_bytes(&base_root).map_err(|error| OperationFailure::new(error, work))?,
            );
            let mut root = crate::collection::CheckoutRoots::new_measured(
                self.fs.inner.objects.collection(), self.config, generation_root, root, &mut allocations, &mut work, budget, cancellation,
            ).map_err(|error| OperationFailure::new(error, work))?;
            root.attach_control_custody(&dependencies.data_custody);
            Ok(FsReceipt {
                value: Checkout {
                    volume: self.clone(),
                    generation_root,
                    base_file_table: root.file_table,
                    base_root, root,
                    authority_head,
                    authored_operation_id: None,
                    live_operation_id: None,
                    last_commit: None,
                    publication_closure: None,
                    prepared_merge_parent: None,
                    dependencies, proof_owned_bytes,
                    mode,
                },
                work,
            })
        })
        .await;
        result.observe_on(&span, "checkout")
    }

    /// Opens the authenticated checkout on the supplied original native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn checkout_with_native_work(
        &self, selector: GenerationSelector, mode: CheckoutMode, original: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<Checkout<A, O>, FsError> {
        cancellation.check()?;
        validate_checkout(mode, self.config).map_err(|failure| failure.error)?;
        let (generation_root, mut authority_head) = self
            .resolve_checkout_generation_with_native_work(selector, mode, original, cancellation).await?;
        let mut accounting = KernelAccounting::native(original, cancellation);
        let decoded = read_generation_root_with_accounting(
            &self.fs.inner.objects, generation_root, self.config, &mut accounting, cancellation,
        ).await?;
        let mut root_guard = match decoded.1 {
            KernelOwnedAllocation::NativeOriginal(guard) => guard, KernelOwnedAllocation::Standalone =>
                unreachable!("native root accounting returns its original allocation guard"),
        };
        let root = decoded.0;
        if root.volume_id != self.id { return Err(FsError::VolumeMismatch); }
        if mode.access == AccessMode::ReadWrite
            && self.config.concurrency == ConcurrencyMode::ExclusiveWriter {
            let expected = authority_head.ok_or(FsError::WritableCheckoutRequiresHead)?;
            authority_head = Some(match self.fs.inner.authority.fence_with_native_work(
                volume_authority_id(self.id), expected, original, cancellation,
            ).await? {
                FenceOutcome::Advanced(head) => head,
                FenceOutcome::Conflict { actual } => return Err(FsError::ExclusiveWriterConflict { actual }),
            });
        }
        cancellation.check()?;
        let mut token_header = original.claim_root_metadata(std::mem::size_of::<ProofDataCustody>() as u64)?;
        let custody_bytes = token_header.bytes();
        let custody = Arc::new(ProofDataCustody::default());
        let witness = original.register_native_checkout_control(&custody, &mut token_header)?;
        let working_control = root_guard.transfer_checkout_control(&custody, &witness)?;
        let working = (root, working_control);
        let captured_bytes = usize_to_u64(CheckoutDependencies::empty_logical_owned_bytes());
        cancellation.check()?;
        let mut captured_guard = original.claim_allocation(captured_bytes, 1, crate::WorkCounters::UNCHARGED)?;
        let mut captured_control = captured_guard.transfer_checkout_control(&custody, &witness)?;
        let mut captured = CheckoutDependencies::new(
            std::iter::empty(), std::iter::empty(), self.config.limits.maximum_checkout_dependencies,
        )?;
        assert!(captured.attach_fresh_paid_control(&mut captured_control),
            "a fresh checkout owns its unique empty dependency map");
        let proof_bytes = usize_to_u64(size_of::<Mutex<Proof>>());
        let paths_bytes = usize_to_u64(size_of::<ObservedPathMap>());
        cancellation.check()?;
        let mut proof_guard = original.claim_allocation(proof_bytes, 1, crate::WorkCounters::UNCHARGED)?;
        let mut paths_guard = original.claim_allocation(paths_bytes, 1, crate::WorkCounters::UNCHARGED)?;
        let proof_control = proof_guard.transfer_checkout_control(&custody, &witness)?;
        let paths_control = paths_guard.transfer_checkout_control(&custody, &witness)?;
        let mut dependencies = DependencyLedger::new_with_native_custody(
            captured, custody, proof_control, paths_control,
        );
        let (base_root, base_control) = crate::collection::clone_generation_root_native(
            &working.0, &mut accounting, &dependencies.data_custody,
        )?;
        dependencies.base_root_control = base_control;
        let root = crate::collection::CheckoutRoots::new_native_measured(
            self.fs.inner.objects.collection(), self.config, generation_root, working, &mut accounting, &dependencies.data_custody,
        )?;
        let proof_owned_bytes = captured_bytes.checked_add(proof_bytes).and_then(|bytes| bytes.checked_add(paths_bytes))
            .and_then(|bytes| bytes.checked_add(custody_bytes)).ok_or(WorkError::Overflow)?;
        accounting.check()?;
        Ok(Checkout {
            volume: self.clone(), generation_root, base_file_table: root.file_table,
            base_root, root, authority_head, authored_operation_id: None,
            live_operation_id: None, last_commit: None, publication_closure: None,
            prepared_merge_parent: None, dependencies, proof_owned_bytes, mode,
        })
    }

    async fn resolve_head_generation(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<(ObjectId, Head, WorkCounters), OperationFailure<FsError>> {
        async {
            let authority_id = volume_authority_id(self.id);
            let head = self
                .fs
                .inner
                .authority
                .head(authority_id, budget, cancellation)
                .await
                .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
            let mut work = head.work;
            if head.value.sequence == Sequence::GENESIS {
                return Err(OperationFailure::new(FsError::EmptyAuthority, work));
            }
            let after = Sequence::new(head.value.sequence.get().saturating_sub(1));
            let records = self
                .fs
                .inner
                .authority
                .replay(
                    authority_id,
                    after,
                    ReplayLimit {
                        records: 1,
                        payload_bytes: MAXIMUM_VOLUME_EVENT_BYTES,
                    },
                    remaining(work, budget)?,
                    cancellation,
                )
                .await
                .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
            work = add(work, records.work)?;
            let latest = records
                .value
                .first()
                .ok_or_else(|| OperationFailure::new(FsError::InvalidAuthorityHistory, work))?;
            let generation_root = generation_from_head_record_value(latest, self.id).map_err(|error| OperationFailure::new(error, work))?;
            Ok((generation_root, head.value, work))
        }
        .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn resolve_head_generation_with_native_work(
        &self, work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(ObjectId, Head), FsError> {
        cancellation.check()?;
        let authority_id = volume_authority_id(self.id);
        let head = self.fs.inner.authority.head_with_native_work(authority_id, work, cancellation).await?;
        if head.sequence == Sequence::GENESIS { return Err(FsError::EmptyAuthority); }
        let records = self.fs.inner.authority.replay_with_native_work(
            authority_id, Sequence::new(head.sequence.get().saturating_sub(1)),
            ReplayLimit { records: 1, payload_bytes: MAXIMUM_VOLUME_EVENT_BYTES }, work, cancellation,
        ).await?;
        let latest = records.0.first().ok_or(FsError::InvalidAuthorityHistory)?;
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        Ok((generation_from_head_record_value(latest, self.id)?, head))
    }
}

fn generation_from_head_record_value(record: &DurableCommit, volume_id: VolumeId) -> Result<ObjectId, FsError> {
    if let Ok(deleted) = decode_workspace_deleted(&record.payload, MAXIMUM_VOLUME_EVENT_BYTES) {
        if deleted != volume_id { return Err(FsError::VolumeMismatch); }
        return Err(FsError::WorkspaceDeleted);
    }
    generation_from_record_value(record, volume_id)
}

static UNBOUNDED_CHECKOUT_CANCELLATION: std::sync::LazyLock<CancellationToken> =
    std::sync::LazyLock::new(CancellationToken::new);

/// Sealed proof merge for the private candidate adoption fence. The snapshots
#[cfg(feature = "native-mount")]
pub(crate) struct PreparedCandidateAdoption {
    dependencies: CheckoutDependencies, live: CheckoutDependencies, candidate: CheckoutDependencies, retained_bytes: u64,
}

#[cfg(feature = "native-mount")]
impl PreparedCandidateAdoption { pub(crate) const fn retained_bytes(&self) -> u64 { self.retained_bytes } }

impl<A, O> Checkout<A, O> {
    /// Replaces this checkout with `candidate`, a private candidate begun
    #[cfg(feature = "native-mount")]
    pub(crate) fn adopt_candidate(&mut self, base: &Self, candidate: Self) -> bool {
        let Ok(receipt) = self.prepare_candidate_adoption(
            base, &candidate, WorkBudget::UNBOUNDED, &UNBOUNDED_CHECKOUT_CANCELLATION,
        ) else {
            return false;
        };
        let Some(prepared) = receipt.value else { return false; };
        self.adopt_prepared_candidate(base, candidate, prepared)
    }

    /// Whether [`Self::adopt_candidate`] would adopt `candidate`.
    #[cfg(feature = "native-mount")]
    pub(crate) fn admits_candidate(&self, base: &Self, candidate: &Self) -> bool {
        self.prepare_candidate_adoption(
            base, candidate, WorkBudget::UNBOUNDED, &UNBOUNDED_CHECKOUT_CANCELLATION,
        ).is_ok_and(|receipt| receipt.value.is_some())
    }

    /// Prepares the exact candidate proof under the original caller's limits.
    #[cfg(feature = "native-mount")]
    pub(crate) fn prepare_candidate_adoption<W: FsWork>(
        &self, base: &Self, candidate: &Self, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<Option<PreparedCandidateAdoption>, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = self.prepare_candidate_adoption_with_accounting(base, candidate, &mut accounting);
        budget.finish(result, &accounting)
    }

    #[cfg(feature = "native-mount")]
    fn prepare_candidate_adoption_with_accounting(
        &self, base: &Self, candidate: &Self, accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<PreparedCandidateAdoption>, FsError> {
        accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        if !self.candidate_base_unchanged(base) { return Ok(None); }
        // Only this candidate's original admitted producer can own NEW merge
        let producer = (candidate.dependencies.data_custody.physical_retained_bytes() != 0)
            .then_some(&candidate.dependencies.data_custody);
        let live = self.dependencies.proof().dependencies.clone();
        let candidate = candidate.dependencies.proof().dependencies.clone();
        let Some(dependencies) = candidate.merged_with_accounting(&live, producer, accounting)? else {
            return Ok(None);
        };
        let retained_bytes = if dependencies.same_capture(&candidate) { 0 } else {
            dependencies.logical_data().2.checked_add(
                CheckoutDependencies::empty_logical_owned_bytes() as u64,
            ).ok_or(WorkError::Overflow)?
        };
        Ok(Some(PreparedCandidateAdoption { dependencies, live, candidate, retained_bytes }))
    }

    /// Installs only the proof sealed by preparation. Both snapshots retain
    #[cfg(feature = "native-mount")]
    pub(crate) fn adopt_prepared_candidate(
        &mut self, base: &Self, candidate: Self, prepared: PreparedCandidateAdoption,
    ) -> bool {
        if !self.candidate_base_unchanged(base)
            || !self.dependencies.proof().dependencies.same_capture(&prepared.live)
        { return false; }
        {
            let mut proof = candidate.dependencies.proof();
            if !proof.dependencies.same_capture(&prepared.candidate) { return false; }
            // Retain the candidate's absorbed-walk memory: its sealed proof
            proof.dependencies = prepared.dependencies;
        }
        *self = candidate;
        true
    }

    #[cfg(feature = "native-mount")]
    fn candidate_base_unchanged(&self, base: &Self) -> bool {
        let Self {
            volume: _,
            generation_root,
            base_file_table,
            base_root,
            root,
            authority_head,
            authored_operation_id,
            live_operation_id,
            last_commit,
            publication_closure: _,
            prepared_merge_parent,
            dependencies: _, proof_owned_bytes: _,
            mode,
        } = self;
        *generation_root == base.generation_root
            && *base_file_table == base.base_file_table
            && *base_root == base.base_root
            && *root == base.root
            && *authority_head == base.authority_head
            && *authored_operation_id == base.authored_operation_id
            && *live_operation_id == base.live_operation_id
            && *last_commit == base.last_commit
            && *prepared_merge_parent == base.prepared_merge_parent
            && *mode == base.mode
    }

    /// Owning volume identity.
    #[must_use]
    pub const fn volume_id(&self) -> VolumeId {
        self.volume.id
    }

    /// Immutable owning-volume semantics and operation bounds.
    #[must_use]
    pub const fn volume_config(&self) -> VolumeConfig {
        self.volume.config
    }

    /// Exact immutable generation identity.
    #[must_use]
    pub const fn generation_id(&self) -> GenerationId {
        GenerationId::new(self.generation_root.digest)
    }

    /// Authenticated decoded generation root.
    #[must_use]
    pub fn root(&self) -> &GenerationRoot {
        &self.root
    }

    /// Admitted checkout behavior.
    #[must_use]
    pub const fn mode(&self) -> CheckoutMode {
        self.mode
    }

    /// Whether the private candidate differs from its immutable base.
    #[must_use]
    pub fn has_pending_mutations(&self) -> bool {
        self.root.file_table != self.base_file_table || self.prepared_merge_parent.is_some()
    }

    /// Whether reads retain exact observations for safe rebase and commit.
    fn tracks_observations(&self) -> bool {
        self.mode.consistency != ConsistencyMode::Pinned || self.dependencies.observing
    }

    /// Creates an immutable reader for this checkout's current private candidate.
    #[must_use]
    pub fn snapshot_reader(&self) -> PinnedReader<A, O> {
        PinnedReader {
            volume: self.volume.clone(),
            generation_root: self.generation_root,
            root: PinnedReaderRoot::Owned(self.root.clone()),
        }
    }

    /// Creates an immutable reader for a pinned checkout.
    pub fn pinned_reader(&self) -> Result<PinnedReader<A, O>, FsError> {
        if self.tracks_observations() {
            return Err(FsError::MutationNotAllowed);
        }
        Ok(self.snapshot_reader())
    }

    /// Funds only the legacy readonly root copy. It keeps standalone query
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn pinned_reader_for_materialization_with_work(
        &self, work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<PinnedReader<A, O>, FsError> {
        if self.tracks_observations() { return Err(FsError::MutationNotAllowed); }
        cancellation.check()?;
        work.admit(WorkCounters::UNCHARGED)?;
        let mut accounting = KernelAccounting::native(work, cancellation);
        let (root, allocation) = crate::collection::clone_generation_root_with_accounting(&self.root, &mut accounting)?;
        let mut carrier = (root, native_pinned_allocation(allocation)?);
        cancellation.check()?;
        carrier.1.grow(size_of::<(GenerationRoot, NativeAllocation)>() as u64)?;
        let root = PinnedReaderRoot::FundedOwned(Arc::new(carrier));
        Ok(PinnedReader { volume: self.volume.clone(), generation_root: self.generation_root, root, })
    }

    /// Clones only the registered original readonly roots and their exact hold.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn pinned_reader_with_native_work(
        &self, work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<PinnedReader<A, O>, FsError> {
        if self.tracks_observations() { return Err(FsError::MutationNotAllowed); }
        cancellation.check()?;
        work.admit(WorkCounters::UNCHARGED)?;
        if !self.dependencies.data_custody.is_native_original(work.native_read_meter())
            || !work.owns_native_checkout_token(&self.dependencies.data_custody)
            || !self.root.same_original_scope(work.native_read_meter())
        { return Err(FsError::NativeKernel(NativeTreeMaterializationError::InvalidLayout)); }
        let mut accounting = KernelAccounting::native(work, cancellation);
        let roots = self.root.replicate_with_accounting(&mut accounting, Some(&self.dependencies.data_custody))?;
        cancellation.check()?;
        let allocation = work.claim(size_of::<(crate::collection::CheckoutRoots, NativeAllocation)>() as u64)?;
        let root = PinnedReaderRoot::NativeCheckout(Arc::new((roots, allocation)));
        Ok(PinnedReader { volume: self.volume.clone(), generation_root: self.generation_root, root, })
    }

    /// Consumes an original-native checkout into a cheap cloneable readonly
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn into_pinned_reader_with_native_work(
        self, work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<PinnedReader<A, O>, FsError> {
        cancellation.check()?;
        if !self.dependencies.data_custody.is_native_original(work.native_read_meter())
            || !work.owns_native_checkout_token(&self.dependencies.data_custody)
            || !self.root.same_original_scope(work.native_read_meter())
        { return Err(FsError::NativeKernel(NativeTreeMaterializationError::InvalidLayout)); }
        let allocation = work.claim(size_of::<(crate::collection::CheckoutRoots, NativeAllocation)>() as u64)?;
        let carrier = Arc::new((self.root, allocation));
        Ok(PinnedReader {
            volume: self.volume, generation_root: self.generation_root, root: PinnedReaderRoot::NativeCheckout(carrier),
        })
    }

}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    /// Streams one bounded source into immutable authenticated chunks.
    pub async fn stage<R: AsyncBlobSource, W: FsWork>(
        &self, source: &mut R, maximum_source_bytes: u64, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<StagedContent, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let standalone = matches!(accounting, KernelAccounting::Standalone { .. } | KernelAccounting::BorrowedStandalone { .. });
        let read = stage_content_for_volume_with_accounting(
            &self.volume, source, maximum_source_bytes, &mut accounting, cancellation,
        );
        let result = if standalone { in_heap(move || async move { read.await }).await } else { read.await };
        budget.finish(result, &accounting)
    }
}

impl<A, O> PinnedReader<A, O> {
    /// Exact immutable generation identity this reader is pinned to.
    #[must_use]
    pub const fn generation_id(&self) -> GenerationId {
        GenerationId::new(self.generation_root.digest)
    }

    /// Immutable volume limits authenticated by the reader's constructor.
    #[must_use]
    pub const fn volume_config(&self) -> VolumeConfig { self.volume.config }

    /// Checks retained original custody without admitting work or granting rights.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn validate_native_work(&self, work: &NativeMaterializationWork) -> Result<(), FsError> {
        let PinnedReaderRoot::NativeCheckout(carrier) = &self.root else {
            return Err(FsError::NativeKernel(NativeTreeMaterializationError::InvalidLayout));
        };
        if !carrier.1.work().shares_ledger(work)
            || !carrier.0.same_original_scope(work.native_read_meter())
        { return Err(FsError::NativeKernel(NativeTreeMaterializationError::InvalidLayout)); }
        Ok(())
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> PinnedReader<A, O> {
    /// Looks up file records by stable identity, reading each distinct
    pub async fn file_records_by_id(
        &self,
        file_ids: &[FileId],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Vec<Option<FileRecord>>> {
        let lookup = lookup_file_records(
            &self.volume.fs.inner.objects,
            self.root.file_table,
            file_ids,
            self.volume.config.limits.maximum_paths_per_batch,
            DecodeLimits::for_volume(self.volume.config),
            budget,
            cancellation,
        )
        .await
        .map_err(|failure| {
            OperationFailure::new(FsError::FileRecord(failure.error), *failure.work)
        })?;
        Ok(FsReceipt {
            value: lookup.value,
            work: lookup.work,
        })
    }

    /// Looks up one authenticated stable identity on the original native journal.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn file_record_by_id_with_native_work(
        &self, file_id: FileId, work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<Option<FileRecord>, FsError> {
        self.validate_native_work(work)?;
        let mut accounting = KernelAccounting::native(work, cancellation);
        let (record, allocation) = lookup_file_record_async_with_accounting(
            &self.volume.fs.inner.objects, self.root.file_table, file_id, DecodeLimits::for_volume(self.volume.config),
            &mut accounting, cancellation, None,
        ).await?;
        drop(allocation);
        Ok(record)
    }

    /// Finds bounded sorted aliases using the canonical cached reverse traversal.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn paths_for_file_id_with_native_work(
        &self, file_id: FileId, maximum_entries: u32, work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(Vec<NamespacePath>, NativeAllocation), WorkspaceError> {
        cancellation.check()?;
        self.validate_native_work(work).map_err(WorkspaceError::Filesystem)?;
        let mut accounting = KernelAccounting::native(work, cancellation);
        let (paths, allocation) = crate::workspace::reverse_paths_to_file_root_with_accounting(
            &self.volume, &self.root, self.generation_root, file_id, maximum_entries, &mut accounting, cancellation,
        ).await?;
        Ok((paths, native_pinned_allocation(allocation) .map_err(WorkspaceError::Filesystem)?))
    }

    /// Projects only the first sorted alias; all reverse-query buffers keep
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn first_path_for_file_id_with_native_work(
        &self, file_id: FileId, maximum_entries: u32, work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<Option<(String, NativeAllocation)>, WorkspaceError> {
        let (value, allocation) = self.paths_for_file_id_with_native_work(
            file_id, maximum_entries, work, cancellation,
        ).await?;
        let paths = (value, allocation);
        let Some(path) = paths.0.first() else { return Ok(None); };
        let mut accounting = KernelAccounting::native(work, cancellation);
        let (value, allocation) = path.unicode_text_with_accounting(&mut accounting, cancellation)?;
        Ok(Some((value, native_pinned_allocation(allocation).map_err(WorkspaceError::Filesystem)?)))
    }

    /// Resolves an exact no-follow path on the original native journal.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn lookup_record_with_native_work(
        &self, path: &NamespacePath, work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<Option<FileRecord>, FsError> {
        self.validate_native_work(work)?;
        let mut accounting = KernelAccounting::native(work, cancellation);
        let lookup = lookup_path_async_with_accounting(
            &self.volume.fs.inner.objects, &self.root, path, self.volume.config, &mut accounting, cancellation, None,
        ).await?;
        Ok(lookup.record)
    }

    /// Reads canonical Copy metadata without following the terminal binding.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn read_metadata_with_native_work(
        &self, path: &NamespacePath, work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<FileMetadata, FsError> {
        let record = self.lookup_record_with_native_work(path, work, cancellation).await?.ok_or(FsError::NotFound)?;
        self.read_record_metadata(record, work, cancellation).await
    }

    /// Reads one authenticated record's range, preserving intrinsic byte custody.
    pub async fn read_record_range<W: FsWork>(
        &self, record: FileRecord, range: ByteRange, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<FileRangeRead, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let standalone = matches!(accounting, KernelAccounting::Standalone { .. } | KernelAccounting::BorrowedStandalone { .. });
        let result = async {
            if range.length > self.volume.config.limits.maximum_read_bytes {
                return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
            }
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() { self.validate_native_work(original)?; }
            crate::kernel::read_file_range_with_accounting(
                &self.volume.fs.inner.objects, FileRangeRequest {
                    record, range, maximum_spans: self.volume.config.limits.maximum_directory_page_entries,
                    limits: DecodeLimits::for_volume(self.volume.config), budget: accounting.remaining()?,
                }, &mut accounting, cancellation,
            ).await
        }.await.map_err(|error| if standalone {
            FsError::FileRead(match error {
                FsError::FileRead(error) => error,
                FsError::BlobRead(error) => FileRangeReadError::Blob(error),
                FsError::Work(error) => FileRangeReadError::Work(error),
                FsError::Cancelled(_) => FileRangeReadError::Cancelled,
                error => FileRangeReadError::Context(Box::new(error)),
            })
        } else { error });
        budget.finish(result, &accounting)
    }

    /// Plans actual sparse spans, returning the original-owned span vector.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn plan_record_extents_with_native_work(
        &self, record: FileRecord, range: ByteRange, maximum_spans: u32, work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<Option<(ExtentPlan, NativeAllocation)>, FsError> {
        if maximum_spans == 0 || range.length > self.volume.config.limits.maximum_generation_bytes {
            return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
        }
        self.validate_native_work(work)?;
        let mut accounting = KernelAccounting::native(work, cancellation);
        accounting.check()?;
        let (logical_bytes, root) = match record.payload {
            FilePayload::InlineRegular(data) => {
                validate_planned_file_range(usize_to_u64(data.as_bytes().len()), range)?;
                return Ok(None);
            }
            FilePayload::Regular { logical_bytes, extents } => (logical_bytes, extents),
            _ => return Err(FsError::FileRead(FileRangeReadError::NotRegular)),
        };
        let (plan, allocation) = plan_extent_range_with_accounting(
            &self.volume.fs.inner.objects, ExtentRangeRequest {
                root, file_size: logical_bytes, range, maximum_spans,
                limits: DecodeLimits::for_volume(self.volume.config), budget: accounting.remaining()?,
            }, &mut accounting, cancellation,
        ).await?;
        Ok(Some((plan, native_pinned_allocation(allocation)?)))
    }

    /// Reads an exact no-follow file range on the original native journal.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn read_file_range_with_native_work(
        &self, path: &NamespacePath, range: ByteRange, work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<FileRangeRead, FsError> {
        if range.length > self.volume.config.limits.maximum_read_bytes {
            return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
        }
        let record = self.lookup_record_with_native_work(path, work, cancellation).await?.ok_or(FsError::NotFound)?;
        self.read_record_range(record, range, work, cancellation).await
    }

    /// Reads the exact symbolic-link packet, including producer-measured capacity.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn read_record_symbolic_link_owned_with_native_work(
        &self, record: FileRecord, work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<crate::kernel::OwnedSourceChunk, FsError> {
        self.validate_native_work(work)?;
        let mut accounting = KernelAccounting::native(work, cancellation);
        read_symbolic_link_record_with_accounting(&self.volume, record, &mut accounting, cancellation).await
    }

    /// Reads an exact opaque symbolic-link body with intrinsic native custody.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn read_symbolic_link_with_native_work(
        &self, path: &NamespacePath, work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<acyclic_stream::OwnedBytes, FsError> {
        let record = self.lookup_record_with_native_work(path, work, cancellation).await?.ok_or(FsError::NotFound)?;
        self.read_symbolic_link_record(record, work, cancellation).await
    }

    /// Borrows this immutable root through the canonical original-funded tree pager.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn list_directory_with_native_work(
        &self, path: &NamespacePath, after: Option<&LogicalName>, maximum_entries: u32,
        work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(crate::kernel::DirectoryPage, NativeAllocation), FsError> {
        self.validate_native_work(work)?;
        let mut accounting = KernelAccounting::native(work, cancellation);
        let (page, allocation) = list_directory_to_file_root_with_accounting(
            &self.volume, &self.root, path, after, maximum_entries, &mut accounting, cancellation,
        ).await?;
        Ok((page, native_pinned_allocation(allocation)?))
    }

    /// Returns an exact cursor page and its original-owned names/vector claim.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn list_directory_records_with_native_work(
        &self, path: &NamespacePath, after: Option<&LogicalName>, maximum_entries: u32,
        work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(DirectoryRecordPage, NativeAllocation), FsError> {
        self.validate_native_work(work)?;
        let mut accounting = KernelAccounting::native(work, cancellation);
        let (page, allocation) = list_directory_records_to_file_root_with_accounting(
            &self.volume, &self.root, path, after, maximum_entries, &mut accounting, cancellation,
        ).await?;
        Ok((page, native_pinned_allocation(allocation)?))
    }

    /// Reads ordered named attributes, retaining original names/vector custody.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn read_named_attributes_with_native_work(
        &self, root: ObjectId, work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(Vec<(AttributeName, acyclic_stream::OwnedBytes)>, NativeAllocation), FsError> {
        self.validate_native_work(work)?;
        let mut accounting = KernelAccounting::native(work, cancellation);
        let mut attributes = Owned::<Vec<(AttributeName, acyclic_stream::OwnedBytes)>>::allocate(0, &mut accounting)?;
        let mut total = 0_u64;
        loop {
            let after = attributes.value.last().map(|(name, _)| name);
            let (value, allocation) = list_attributes_async_with_accounting(
                &self.volume.fs.inner.objects, root, after, 256,
                DecodeLimits::for_volume(self.volume.config), &mut accounting, cancellation, None,
            ).await?;
            let mut listing = (value, allocation);
            let has_more = listing.0.has_more;
            let retired = listing.0.entries.capacity().checked_mul(size_of::<crate::kernel::AttributeEntry>())
                .map(usize_to_u64).ok_or(WorkError::Overflow)?;
            attributes.allocation.absorb(&mut listing.1)?;
            for entry in listing.0.entries.drain(..) {
                total = total.saturating_add(entry.value_bytes);
                if total > self.volume.config.limits.maximum_read_bytes {
                    return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
                }
                attributes.ensure_for_push(usize::MAX, &mut accounting)?;
                let read = read_blob_range_with_accounting(
                    &self.volume.fs.inner.objects, entry.value, ByteRange { offset: 0, length: entry.value_bytes },
                    DecodeLimits::for_volume(self.volume.config), &mut accounting, cancellation,
                ).await?;
                let copied = WorkCounters {
                    items_examined: 1, bytes_copied: size_of::<(AttributeName, acyclic_stream::OwnedBytes)>() as u64,
                    ..WorkCounters::UNCHARGED
                };
                accounting.admit(copied)?;
                attributes.value.push((entry.name, read.bytes));
                accounting.record_completed(copied)?;
            }
            drop(listing);
            accounting.release_owned(&mut attributes.allocation, retired)?;
            if !has_more { break; }
        }
        Ok((attributes.value, native_pinned_allocation(attributes.allocation)?))
    }

    /// Reads resolved handles without another namespace traversal.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn read_resolved_ranges_with_native_work(
        &self, requests: &[ResolvedFileRangeReadRequest<'_, A, O>], concurrency: usize,
        work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(Vec<FileRangeRead>, NativeAllocation), FsError> {
        if concurrency == 0 || requests.len()
            > usize::try_from(self.volume.config.limits.maximum_paths_per_batch).unwrap_or(usize::MAX)
        { return Err(FsError::FileRead(FileRangeReadError::InvalidRange)); }
        self.validate_native_work(work)?;
        cancellation.check()?;
        work.admit(WorkCounters::UNCHARGED)?;
        let mut accounting = KernelAccounting::native(work, cancellation);
        let mut reads = Owned::<Vec<FileRangeRead>>::allocate(requests.len(), &mut accounting)?;
        for request in requests {
            let read = request.file.reader.read_record_range(
                request.file.record, request.range, work, cancellation,
            ).await?;
            let copied = WorkCounters {
                items_examined: 1, bytes_copied: usize_to_u64(size_of::<FileRangeRead>()),..WorkCounters::UNCHARGED
            };
            accounting.admit(copied)?;
            reads.value.push(read);
            accounting.record_completed(copied)?;
        }
        Ok((reads.value, native_pinned_allocation(reads.allocation)?))
    }

    /// Resolves ordered paths into native-reader handles for later range reads.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn resolve_file_ranges_with_native_work(
        &self, paths: &[NamespacePath], work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(Vec<Option<ResolvedFile<A, O>>>, NativeAllocation), FsError> {
        cancellation.check()?;
        self.validate_native_work(work)?;
        let mut accounting = KernelAccounting::native(work, cancellation);
        let lookup = lookup_paths_async_with_accounting(
            &self.volume.fs.inner.objects, &self.root, paths, self.volume.config, &mut accounting, cancellation, None,
        ).await?;
        let mut handles = Owned::<Vec<Option<ResolvedFile<A, O>>>>::allocate(
            lookup.value().entries.len(), &mut accounting,
        )?;
        for entry in &lookup.value().entries {
            let file = if let Some(record) = entry.record {
                let metadata = self.read_record_metadata(record, work, cancellation).await?;
                let copied = WorkCounters {
                    items_examined: 1, bytes_copied: usize_to_u64(size_of::<ResolvedFile<A, O>>()),..WorkCounters::UNCHARGED
                };
                accounting.admit(copied)?;
                let file = ResolvedFile {
                    reader: self.clone(), record, description: FileDescription {
                        kind: record.kind, logical_bytes: record_logical_bytes(record), metadata,
                    },
                };
                accounting.record_completed(copied)?;
                Some(file)
            } else { None };
            let copied = WorkCounters {
                bytes_copied: usize_to_u64(size_of::<Option<ResolvedFile<A, O>>>()),..WorkCounters::UNCHARGED
            };
            accounting.admit(copied)?;
            handles.value.push(file);
            accounting.record_completed(copied)?;
        }
        Ok((handles.value, native_pinned_allocation(handles.allocation)?))
    }

    async fn resolve_file_records(
        &self,
        paths: &[NamespacePath],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Vec<Option<FileRecord>>> {
        let lookup = lookup_paths(
            &self.volume.fs.inner.objects,
            &self.root,
            paths,
            self.volume.config,
            budget,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(WorkCounters::default(), FsError::Path))?;
        let mut records = Vec::new();
        records
            .try_reserve_exact(lookup.entries.len())
            .map_err(|_| {
                OperationFailure::new(FsError::PendingMutationAllocationFailed, lookup.work)
            })?;
        for entry in lookup.entries {
            records.push(entry.record);
        }
        Ok(FsReceipt {
            value: records,
            work: lookup.work,
        })
    }

    async fn list_directory_page(
        &self,
        request: DirectoryPageRequest,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<DirectoryPage> {
        list_directory_page(
            &self.volume,
            &self.root,
            &request.path,
            request.after.as_ref(),
            request.maximum_entries,
            budget,
            cancellation,
        )
        .await
    }

    /// Lists ordered directory binding pages with at most `concurrency`
    pub async fn list_directory_pages(
        &self,
        requests: &[DirectoryPageRequest],
        concurrency: usize,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Vec<DirectoryPage>> {
        if concurrency == 0
            || requests.len()
                > usize::try_from(self.volume.config.limits.maximum_paths_per_batch)
                    .unwrap_or(usize::MAX)
        {
            return Err(OperationFailure::before_work(FsError::Path(
                PathLookupError::TooManyPaths,
            )));
        }
        if budget != WorkBudget::UNBOUNDED {
            let mut work = WorkCounters::default();
            let mut ordered = Vec::new();
            ordered
                .try_reserve_exact(requests.len())
                .map_err(|_| OperationFailure::before_work(FsError::Work(WorkError::Overflow)))?;
            for request in requests.iter().cloned() {
                let receipt = self
                    .list_directory_page(request, remaining(work, budget)?, cancellation)
                    .await
                    .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
                work = add(work, receipt.work)?;
                ordered.push(receipt.value);
            }
            return Ok(FsReceipt {
                value: ordered,
                work,
            });
        }
        let results = stream::iter(
            requests
                .iter()
                .cloned()
                .enumerate()
                .map(|(index, request)| {
                    let reader = self.clone();
                    async move {
                        (
                            index,
                            reader
                                .list_directory_page(request, budget, cancellation)
                                .await,
                        )
                    }
                }),
        )
        .buffer_unordered(concurrency.min(requests.len().max(1)))
        .collect::<Vec<_>>()
        .await;
        order_batch_results(results, budget)
    }

    /// Resolves one bounded directory page into generation-scoped child
    pub async fn resolve_directory_page(
        &self,
        path: &NamespacePath,
        after: Option<&LogicalName>,
        maximum_entries: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<ResolvedDirectoryPage<A, O>> {
        let listing = list_directory_page(
            &self.volume,
            &self.root,
            path,
            after,
            maximum_entries,
            budget,
            cancellation,
        )
        .await?;
        let mut work = listing.work;
        let page = listing.value;
        let mut file_ids = Vec::new();
        file_ids
            .try_reserve_exact(page.entries.len())
            .map_err(|_| OperationFailure::new(FsError::Work(WorkError::Overflow), work))?;
        file_ids.extend(page.entries.iter().map(|entry| entry.file_id));
        let records = if file_ids.is_empty() {
            Vec::new()
        } else {
            let receipt = lookup_file_records(
                &self.volume.fs.inner.objects,
                self.root.file_table,
                &file_ids,
                maximum_entries,
                DecodeLimits::for_volume(self.volume.config),
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, FsError::FileRecord))?;
            work = add(work, receipt.work)?;
            receipt.value
        };
        let mut metadata_requests = Vec::new();
        metadata_requests
            .try_reserve_exact(records.len())
            .map_err(|_| OperationFailure::new(FsError::Work(WorkError::Overflow), work))?;
        for record in &records {
            let record = record
                .as_ref()
                .ok_or_else(|| OperationFailure::new(FsError::InvalidDirectoryRecord, work))?;
            metadata_requests.push(ObjectReadRequest {
                object_id: record.metadata,
                maximum_bytes: self.volume.config.limits.maximum_object_bytes,
            });
        }
        let metadata = if metadata_requests.is_empty() {
            Vec::new()
        } else {
            let receipt = self
                .volume
                .fs
                .inner
                .objects
                .read_many(&metadata_requests, remaining(work, budget)?, cancellation)
                .await
                .map_err(|failure| failure.map_with_prior_work(work, FsError::Object))?;
            work = add(work, receipt.work)?;
            receipt.value
        };
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(page.entries.len())
            .map_err(|_| OperationFailure::new(FsError::Work(WorkError::Overflow), work))?;
        for ((binding, record), metadata) in page.entries.into_iter().zip(records).zip(metadata) {
            let record = record
                .ok_or_else(|| OperationFailure::new(FsError::InvalidDirectoryRecord, work))?;
            if record.file_id != binding.file_id || record.kind != binding.kind {
                return Err(OperationFailure::new(FsError::InvalidDirectoryRecord, work));
            }
            let metadata =
                crate::kernel::decode_file_metadata(&metadata, DecodeLimits::for_volume(self.volume.config), &mut |_| Ok::<_, FsError>(()))
                    .map_err(|error| OperationFailure::new(error.into(), work))?;
            let logical_bytes = record_logical_bytes(record);
            entries.push(ResolvedDirectoryEntry {
                name: binding.name,
                file: ResolvedFile {
                    reader: self.clone(),
                    record,
                    description: FileDescription {
                        kind: record.kind,
                        logical_bytes,
                        metadata,
                    },
                },
            });
        }
        Ok(FsReceipt {
            value: ResolvedDirectoryPage {
                entries,
                has_more: page.has_more,
            },
            work,
        })
    }

    async fn read_symbolic_link_record<W: FsWork>(
        &self, record: FileRecord, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<acyclic_stream::OwnedBytes, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() { self.validate_native_work(original)?; }
            read_symbolic_link_record_with_accounting(&self.volume, record, &mut accounting, cancellation)
                .await.map(|read| read.bytes)
        }.await;
        budget.finish(result, &accounting)
    }

    /// Reads metadata for an already authenticated record.
    pub async fn read_record_metadata<W: FsWork>(
        &self, record: FileRecord, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<FileMetadata, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                self.validate_native_work(original)?;
                return read_record_metadata_to_file_root_with_accounting(
                    &self.volume, record, &mut accounting, cancellation,
                ).await;
            }
            let (_, _, budget, _) = accounting.standalone_parts().expect("standalone metadata read");
            let metadata = self.volume.fs.inner.objects.read(
                record.metadata, self.volume.config.limits.maximum_object_bytes, budget, cancellation,
            ).await;
            let completed = match &metadata { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
            *accounting.standalone_parts().expect("standalone metadata read").1 = completed;
            let metadata = metadata.map_err(|failure| FsError::Object(failure.error))?;
            crate::kernel::decode_file_metadata(&metadata.value, DecodeLimits::for_volume(self.volume.config), &mut |_| Ok::<_, FsError>(())).map_err(Into::into)
        }.await;
        budget.finish(result, &accounting)
    }

    async fn resolve_file(
        &self,
        path: &NamespacePath,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<FileRecord> {
        let lookup = observe_path_edges_async(
            &self.volume.fs.inner.objects,
            &self.root,
            path,
            self.volume.config,
            budget,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(WorkCounters::default(), FsError::Path))?;
        let work = lookup.lookup.work;
        let record = lookup
            .lookup
            .record
            .ok_or_else(|| OperationFailure::new(FsError::NotFound, work))?;
        Ok(FsReceipt {
            value: record,
            work,
        })
    }

    /// Plans sparse physical spans without reading content bodies.
    pub async fn plan_file_extents(
        &self,
        path: &NamespacePath,
        range: ByteRange,
        maximum_spans: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Option<ExtentPlan>> {
        let resolved = self.resolve_file(path, budget, cancellation).await?;
        let mut work = resolved.work;
        let plan = self
            .plan_record_extents(
                resolved.value,
                range,
                maximum_spans,
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, plan.work)?;
        Ok(FsReceipt {
            value: plan.value,
            work,
        })
    }

    async fn plan_record_extents(
        &self,
        record: FileRecord,
        range: ByteRange,
        maximum_spans: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Option<ExtentPlan>> {
        if maximum_spans == 0 || range.length > self.volume.config.limits.maximum_generation_bytes {
            return Err(OperationFailure::before_work(FsError::FileRead(
                FileRangeReadError::InvalidRange,
            )));
        }
        let mut work = WorkCounters::default();
        let (logical_bytes, extents) = match record.payload {
            FilePayload::InlineRegular(data) => {
                validate_planned_file_range(
                    u64::try_from(data.as_bytes().len()).unwrap_or(u64::MAX),
                    range,
                ).map_err(|error| OperationFailure::new(error, work))?;
                return Ok(FsReceipt { value: None, work });
            }
            FilePayload::Regular {
                logical_bytes,
                extents,
            } => (logical_bytes, extents),
            _ => {
                return Err(OperationFailure::new(
                    FsError::FileRead(FileRangeReadError::NotRegular),
                    work,
                ));
            }
        };
        let mut plan = plan_extent_range(
            &self.volume.fs.inner.objects,
            ExtentRangeRequest {
                root: extents,
                file_size: logical_bytes,
                range,
                maximum_spans,
                limits: DecodeLimits::for_volume(self.volume.config),
                budget: remaining(work, budget)?,
            },
            cancellation,
        )
        .await
        .map_err(|failure| {
            failure.map_with_prior_work(work, |error| {
                FsError::FileRead(FileRangeReadError::Extent(error))
            })
        })?;
        work = add(work, plan.work)?;
        plan.work = work;
        Ok(FsReceipt {
            value: Some(plan),
            work,
        })
    }

    /// Reads one exact logical range without mutating checkout state.
    pub async fn read_file_range(
        &self,
        path: &NamespacePath,
        range: ByteRange,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<FileRangeRead> {
        let span = crate::obs::span!(
            DEBUG,
            "acyclic.fs.pinned.read_file_range",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
            work.items = crate::obs::Empty,
            work.bytes = crate::obs::Empty,
            work.durability = crate::obs::Empty,
        );
        let result = crate::obs::in_span(&span, async move {
            if range.length > self.volume.config.limits.maximum_read_bytes {
                return Err(OperationFailure::before_work(FsError::FileRead(
                    FileRangeReadError::InvalidRange,
                )));
            }
            let resolved = self.resolve_file(path, budget, cancellation).await?;
            let mut work = resolved.work;
            let record = resolved.value;
            let read = read_file_range_async(
                &self.volume.fs.inner.objects,
                FileRangeRequest {
                    record,
                    range,
                    maximum_spans: self.volume.config.limits.maximum_directory_page_entries,
                    limits: DecodeLimits::for_volume(self.volume.config),
                    budget: remaining(work, budget)?,
                },
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, FsError::FileRead))?;
            work = add(work, read.work)?;
            Ok(FsReceipt { value: read, work })
        })
        .await;
        result.observe_on(&span, "pinned.read_file_range")
    }

    /// Reads exact opaque symbolic-link target bytes without following it.
    pub async fn read_symbolic_link(
        &self,
        path: &NamespacePath,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<acyclic_stream::OwnedBytes> {
        let resolved = self.resolve_file(path, budget, cancellation).await?;
        let mut work = resolved.work;
        let read = self
            .read_symbolic_link_record(resolved.value, remaining(work, budget)?, cancellation)
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, read.work)?;
        Ok(FsReceipt {
            value: read.value,
            work,
        })
    }

    /// Reads complete canonical metadata for one exact path.
    pub async fn read_metadata(
        &self,
        path: &NamespacePath,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<FileMetadata> {
        let resolved = self.resolve_file(path, budget, cancellation).await?;
        let mut work = resolved.work;
        let metadata = self
            .read_record_metadata(resolved.value, remaining(work, budget)?, cancellation)
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, metadata.work)?;
        Ok(FsReceipt {
            value: metadata.value,
            work,
        })
    }

    /// Reads every named attribute under one metadata attribute root, in
    pub async fn read_named_attributes(
        &self,
        root: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Vec<(AttributeName, acyclic_stream::OwnedBytes)>> {
        let mut work = WorkCounters::default();
        let mut attributes = Vec::new();
        let mut total = 0_u64;
        let mut after: Option<AttributeName> = None;
        loop {
            let listing = list_attributes(
                &self.volume.fs.inner.objects,
                root,
                after.as_ref(),
                256,
                DecodeLimits::for_volume(self.volume.config),
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, FsError::AttributeList))?;
            work = add(work, listing.work)?;
            for entry in listing.entries {
                total = total.saturating_add(entry.value_bytes);
                if total > self.volume.config.limits.maximum_read_bytes {
                    return Err(OperationFailure::new(
                        FsError::FileRead(FileRangeReadError::InvalidRange),
                        work,
                    ));
                }
                let read = read_blob_range(
                    &self.volume.fs.inner.objects,
                    entry.value,
                    ByteRange {
                        offset: 0,
                        length: entry.value_bytes,
                    },
                    DecodeLimits::for_volume(self.volume.config),
                    remaining(work, budget)?,
                    cancellation,
                )
                .await
                .map_err(|failure| failure.map_with_prior_work(work, FsError::BlobRead))?;
                work = add(work, read.work)?;
                after = Some(entry.name.clone());
                attributes.push((entry.name, read.bytes));
            }
            if !listing.has_more {
                break;
            }
        }
        Ok(FsReceipt {
            value: attributes,
            work,
        })
    }

    /// Describes an ordered path batch while sharing namespace and object traversal.
    pub async fn describe_files(
        &self,
        paths: &[NamespacePath],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Vec<Option<FileDescription>>> {
        let resolved = self.resolve_files(paths, budget, cancellation).await?;
        let descriptions = resolved
            .value
            .into_iter()
            .map(|file| file.map(|file| file.description))
            .collect();
        Ok(FsReceipt {
            value: descriptions,
            work: resolved.work,
        })
    }

    /// Resolves and describes paths once, returning reader-scoped handles for
    pub async fn resolve_files(
        &self,
        paths: &[NamespacePath],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Vec<Option<ResolvedFile<A, O>>>> {
        let resolved = self
            .resolve_file_records(paths, budget, cancellation)
            .await?;
        let mut work = resolved.work;
        let mut requests = Vec::new();
        requests
            .try_reserve_exact(resolved.value.len())
            .map_err(|_| OperationFailure::new(FsError::Work(WorkError::Overflow), work))?;
        requests.extend(
            resolved
                .value
                .iter()
                .flatten()
                .map(|record| ObjectReadRequest {
                    object_id: record.metadata,
                    maximum_bytes: self.volume.config.limits.maximum_object_bytes,
                }),
        );
        let reads = if requests.is_empty() {
            Vec::new()
        } else {
            let receipt = self
                .volume
                .fs
                .inner
                .objects
                .read_many(&requests, remaining(work, budget)?, cancellation)
                .await
                .map_err(|failure| failure.map_with_prior_work(work, FsError::Object))?;
            work = add(work, receipt.work)?;
            receipt.value
        };
        let mut reads = reads.into_iter();
        let mut handles = Vec::new();
        handles
            .try_reserve_exact(resolved.value.len())
            .map_err(|_| OperationFailure::new(FsError::Work(WorkError::Overflow), work))?;
        for record in resolved.value {
            let Some(record) = record else {
                handles.push(None);
                continue;
            };
            let bytes = reads
                .next()
                .ok_or_else(|| OperationFailure::new(FsError::Work(WorkError::Overflow), work))?;
            let metadata =
                crate::kernel::decode_file_metadata(&bytes, DecodeLimits::for_volume(self.volume.config), &mut |_| Ok::<_, FsError>(()))
                    .map_err(|error| OperationFailure::new(error.into(), work))?;
            let logical_bytes = record_logical_bytes(record);
            handles.push(Some(ResolvedFile {
                reader: self.clone(),
                record,
                description: FileDescription {
                    kind: record.kind,
                    logical_bytes,
                    metadata,
                },
            }));
        }
        Ok(FsReceipt {
            value: handles,
            work,
        })
    }

    /// Reads ranges from already resolved files with bounded concurrency and
    pub async fn read_resolved_ranges(
        &self,
        requests: &[ResolvedFileRangeReadRequest<'_, A, O>],
        concurrency: usize,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Vec<FileRangeRead>> {
        if concurrency == 0
            || requests.len()
                > usize::try_from(self.volume.config.limits.maximum_paths_per_batch)
                    .unwrap_or(usize::MAX)
        {
            return Err(OperationFailure::before_work(FsError::FileRead(
                FileRangeReadError::InvalidRange,
            )));
        }
        if budget != WorkBudget::UNBOUNDED {
            let mut work = WorkCounters::default();
            let mut reads = Vec::new();
            reads
                .try_reserve_exact(requests.len())
                .map_err(|_| OperationFailure::before_work(FsError::Work(WorkError::Overflow)))?;
            for request in requests {
                let read = request
                    .file
                    .read_range(request.range, remaining(work, budget)?, cancellation)
                    .await
                    .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
                work = add(work, read.work)?;
                reads.push(read.value);
            }
            return Ok(FsReceipt { value: reads, work });
        }
        let cancellation = cancellation.clone();
        let pending = requests
            .iter()
            .enumerate()
            .map(|(index, request)| {
                (
                    index,
                    request.file.reader.clone(),
                    request.file.record,
                    request.range,
                )
            })
            .collect::<Vec<_>>();
        let results = stream::iter(pending.into_iter().map(|(index, reader, record, range)| {
            let cancellation = cancellation.clone();
            async move {
                (
                    index,
                    reader
                        .read_record_range(record, range, budget, &cancellation)
                        .await,
                )
            }
        }))
        .buffer_unordered(concurrency.min(requests.len().max(1)))
        .collect::<Vec<_>>()
        .await;
        order_batch_results(results, budget)
    }
}

fn record_logical_bytes(record: FileRecord) -> u64 {
    match record.payload {
        FilePayload::InlineRegular(bytes) => {
            u64::try_from(bytes.as_bytes().len()).unwrap_or(u64::MAX)
        }
        FilePayload::Regular { logical_bytes, .. } => logical_bytes,
        FilePayload::SymbolicLink { target_bytes, .. } => target_bytes,
        _ => 0,
    }
}

async fn seek_record_extent_with_accounting<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>, record: FileRecord, offset: u64, target: ExtentSeekTarget,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<Option<u64>, FsError> {
    accounting.check()?;
    match record.payload {
        FilePayload::InlineRegular(data) => {
            let logical_bytes = data.as_bytes().len() as u64;
            if offset > logical_bytes { return Ok(None); }
            Ok(match target {
                ExtentSeekTarget::Data => (offset < logical_bytes).then_some(offset),
                ExtentSeekTarget::Hole => Some(logical_bytes),
            })
        }
        FilePayload::Regular { logical_bytes, extents } => {
            if offset > logical_bytes { return Ok(None); }
            let before = readonly_standalone_live(accounting);
            let result = crate::kernel::seek_extent_with_accounting(
                &volume.fs.inner.objects,
                ExtentSeekRequest {
                    root: extents, file_size: logical_bytes, offset, target,
                    limits: DecodeLimits::for_volume(volume.config), budget: accounting.remaining()?,
                }, accounting, cancellation,
            ).await;
            readonly_cleanup_standalone(accounting, before, 0)?;
            result
        }
        _ => Err(FsError::FileRead(FileRangeReadError::NotRegular)),
    }
}

async fn read_record_metadata_to_file_root_with_accounting<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>,
    record: FileRecord,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<FileMetadata, FsError> {
    let before = readonly_standalone_live(accounting);
    let result = async {
        let (read, _retained_bytes) = crate::kernel::read_object_with_accounting(
            &volume.fs.inner.objects, record.metadata, volume.config.limits.maximum_object_bytes,
            accounting, cancellation,
        ).await.map_err(|error| match error {
            FsError::BlobRead(BlobReadError::Storage(error)) => match error {
                #[cfg(not(target_arch = "wasm32"))]
                crate::ObjectStoreError::Native(error) => FsError::NativeKernel(error),
                error => FsError::Object(error),
            },
            error => error,
        })?;
        crate::kernel::decode_owned(accounting, |observe| crate::kernel::decode_file_metadata(&read.bytes, DecodeLimits::for_volume(volume.config), &mut |step| observe(step))).map(|(metadata, _)| metadata)
    }.await;
    readonly_cleanup_standalone(accounting, before, 0)?;
    result
}

async fn list_directory_to_file_root_with_accounting<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>,
    root: &GenerationRoot,
    path: &NamespacePath,
    after: Option<&LogicalName>,
    maximum_entries: u32,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(crate::kernel::DirectoryPage, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
    let before = readonly_standalone_live(accounting);
    let result = async {
        let record = crate::kernel::lookup_path_async_with_accounting(
            &volume.fs.inner.objects, root, path, volume.config, accounting, cancellation, None,
        )
            .await?.record.ok_or(FsError::NotFound)?;
        let FilePayload::Directory { entries } = record.payload else { return Err(FsError::NotDirectory); };
        if record.kind != FileKind::Directory { return Err(FsError::NotDirectory); }
        crate::kernel::list_tree_entries_async_with_accounting(
            &volume.fs.inner.objects, entries, after, maximum_entries, DecodeLimits::for_volume(volume.config),
            accounting, cancellation.clone(), None,
        ).await
    }.await;
    let retained = match &result {
        Ok((page, _)) if before.is_some() => page.entries.iter().try_fold(
            page.entries.capacity().checked_mul(size_of::<crate::kernel::TreeEntry>())
                .map(crate::foundation::usize_to_u64).ok_or(WorkError::Overflow)?,
            |bytes, entry| bytes.checked_add(entry.name.retained_bytes() as u64)
                .ok_or(FsError::Work(WorkError::Overflow)),
        )?,
        _ => 0,
    };
    readonly_cleanup_standalone(accounting, before, retained)?;
    result
}

pub(crate) async fn list_directory_records_to_file_root_with_accounting<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>,
    root: &GenerationRoot,
    path: &NamespacePath,
    after: Option<&LogicalName>,
    maximum_entries: u32,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(DirectoryRecordPage, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
    let before = readonly_standalone_live(accounting);
    let result = async {
        let (page, allocation) = list_directory_to_file_root_with_accounting(
            volume, root, path, after, maximum_entries, accounting, cancellation,
        ).await?;
        attach_directory_records_to_file_root_with_accounting(
            volume, root.file_table, page, allocation, maximum_entries, accounting, cancellation, None,
        ).await
    }.await;
    let retained = match &result {
        Ok((page, _)) if before.is_some() => directory_record_page_retained_bytes(page)?,
        _ => 0,
    };
    readonly_cleanup_standalone(accounting, before, retained)?;
    result
}

fn directory_record_page_retained_bytes(page: &DirectoryRecordPage) -> Result<u64, FsError> {
    let backing = page.entries.capacity().checked_mul(size_of::<DirectoryRecordEntry>())
        .map(crate::foundation::usize_to_u64).ok_or(WorkError::Overflow)?;
    page.entries.iter().try_fold(backing, |bytes, entry| {
        bytes.checked_add(entry.name.retained_bytes() as u64).ok_or(FsError::Work(WorkError::Overflow))
    })
}

async fn attach_directory_records_to_file_root_with_accounting<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>, file_table: ObjectId, page: DirectoryPage,
    allocation: crate::kernel::allocation::KernelOwnedAllocation,
    maximum_entries: u32, accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
    custody: Option<&crate::kernel::ProofDataCustody>,
) -> Result<(DirectoryRecordPage, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
    let mut source = (page, allocation);
    let count = source.0.entries.len();
    let mut ids = crate::kernel::Owned::<Vec<FileId>>::allocate(count, accounting)?;
    let mut output = crate::kernel::Owned::<Vec<DirectoryRecordEntry>>::allocate(count, accounting)?;
    let copied = WorkCounters {
        bytes_copied: count.checked_mul(size_of::<FileId>()).map(crate::foundation::usize_to_u64)
            .ok_or(WorkError::Overflow)?, ..WorkCounters::UNCHARGED
    };
    accounting.admit(copied)?;
    ids.value.extend(source.0.entries.iter().map(|entry| entry.file_id));
    accounting.record_completed(copied)?;
    source.1.absorb(&mut ids.allocation)?;
    source.1.absorb(&mut output.allocation)?;
    let mut buffers = ((source.0, ids.value, output.value), source.1);
    if count != 0 {
        let (value, allocation) = crate::kernel::lookup_file_records_async_with_accounting(
            &volume.fs.inner.objects, file_table, &buffers.0.1, maximum_entries,
            DecodeLimits::for_volume(volume.config), accounting, cancellation, custody,
        ).await?;
        let records = (value, allocation);
        let mut requests = crate::kernel::Owned::<Vec<ObjectReadRequest>>::allocate(count, accounting)?;
        for record in &records.0 {
            let record = record.ok_or(FsError::InvalidDirectoryRecord)?;
            let copied = WorkCounters {
                bytes_copied: size_of::<ObjectReadRequest>() as u64, ..WorkCounters::UNCHARGED
            };
            accounting.admit(copied)?;
            requests.value.push(ObjectReadRequest {
                object_id: record.metadata, maximum_bytes: volume.config.limits.maximum_object_bytes,
            });
            accounting.record_completed(copied)?;
        }
        let reads = crate::kernel::read_objects_with_accounting(
            &volume.fs.inner.objects, &requests.value, accounting, cancellation,
        ).await?;
        requests.retire(accounting)?;
        if reads.value.len() != count || records.0.len() != count {
            return Err(FsError::InvalidDirectoryRecord);
        }
        let (page, _, entries) = &mut buffers.0;
        for ((binding, record), metadata) in page.entries.drain(..).zip(&records.0).zip(&reads.value) {
            let record = record.ok_or(FsError::InvalidDirectoryRecord)?;
            if record.file_id != binding.file_id || record.kind != binding.kind {
                return Err(FsError::InvalidDirectoryRecord);
            }
            let metadata = crate::kernel::decode_owned(accounting, |observe| crate::kernel::decode_file_metadata(&metadata.bytes, DecodeLimits::for_volume(volume.config), &mut |step| observe(step))).map(|(metadata, _)| metadata)?;
            let copied = WorkCounters {
                items_examined: 1, bytes_copied: size_of::<DirectoryRecordEntry>() as u64,
                ..WorkCounters::UNCHARGED
            };
            accounting.admit(copied)?;
            entries.push(DirectoryRecordEntry { name: binding.name, record, metadata });
            accounting.record_completed(copied)?;
        }
    }
    let retired = buffers.0.0.entries.capacity().checked_mul(size_of::<crate::kernel::TreeEntry>())
        .and_then(|bytes| buffers.0.1.capacity().checked_mul(size_of::<FileId>())
            .and_then(|ids| bytes.checked_add(ids)))
        .map(crate::foundation::usize_to_u64).ok_or(WorkError::Overflow)?;
    let (page, file_ids, entries) = buffers.0;
    let has_more = page.has_more;
    drop(page.entries);
    drop(file_ids);
    let mut result = (DirectoryRecordPage { entries, has_more }, buffers.1);
    accounting.release_owned(&mut result.1, retired)?;
    Ok((result.0, result.1))
}

async fn list_directory_page<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>, root: &GenerationRoot, path: &NamespacePath,
    after: Option<&LogicalName>, maximum_entries: u32,
    budget: WorkBudget, cancellation: &CancellationToken,
) -> FsResult<DirectoryPage> {
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    let result = list_directory_to_file_root_with_accounting(
        volume, root, path, after, maximum_entries, &mut accounting, cancellation,
    ).await.map(|(mut page, allocation)| {
        drop(allocation);
        page.work = accounting.spent();
        page
    });
    measured(result, &accounting)
}

async fn read_named_attribute_to_file_root_with_accounting<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>, metadata: FileMetadata, name: &AttributeName,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<Option<crate::kernel::OwnedSourceChunk>, FsError> {
    let MetadataField::Value(root) = metadata.named_attributes else { return Ok(None); };
    let before = readonly_standalone_live(accounting);
    let result = async {
        let (value, allocation) = crate::kernel::lookup_attribute_async_with_accounting(
            &volume.fs.inner.objects, root, name, DecodeLimits::for_volume(volume.config),
            accounting, cancellation,
        ).await?;
        let lookup = (value, allocation);
        let entry = lookup.0.as_ref().map(|entry| (entry.value, entry.value_bytes));
        drop(lookup);
        readonly_cleanup_standalone(accounting, before, 0)?;
        let Some((value, value_bytes)) = entry else { return Ok(None); };
        if value_bytes > volume.config.limits.maximum_read_bytes {
            return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
        }
        let read = crate::kernel::read_blob_range_with_accounting(
            &volume.fs.inner.objects, value, ByteRange { offset: 0, length: value_bytes },
            DecodeLimits::for_volume(volume.config), accounting, cancellation,
        ).await?;
        let owned = read.logical_owned_bytes();
        #[cfg(not(target_arch = "wasm32"))]
        if accounting.native_work().is_some() {
            return Ok(Some(crate::kernel::OwnedSourceChunk::intrinsic(read.bytes, owned)));
        }
        Ok(Some(crate::kernel::OwnedSourceChunk::claimed(read.bytes, owned)))
    }.await;
    let retained = result.as_ref().ok().and_then(|chunk| chunk.as_ref())
        .map_or(0, |chunk| chunk.logical_owned_bytes);
    readonly_cleanup_standalone(accounting, before, retained)?;
    result
}

async fn list_named_attributes_to_file_root_with_accounting<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>, metadata: FileMetadata, after: Option<&AttributeName>, maximum_entries: u32,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<(AttributeListing, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
    let MetadataField::Value(root) = metadata.named_attributes else {
        return Ok((AttributeListing {
            entries: Vec::new(), has_more: false, next_residency: None, work: accounting.spent(),
        }, accounting.claim_owned(0, 0)?));
    };
    crate::kernel::list_attributes_async_with_accounting(
        &volume.fs.inner.objects, root, after, maximum_entries, DecodeLimits::for_volume(volume.config),
        accounting, cancellation, None,
    ).await
}

async fn read_symbolic_link_record_with_accounting<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>, record: FileRecord,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<crate::kernel::OwnedSourceChunk, FsError> {
    let FilePayload::SymbolicLink { target_bytes, target } = record.payload else {
        return Err(FsError::NotSymbolicLink);
    };
    if target_bytes > volume.config.limits.maximum_read_bytes {
        return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
    }
    accounting.check()?;
    if target_bytes == 0 {
        let bytes = crate::kernel::owned_read_bytes((Vec::new(), accounting.claim_owned(0, 0)?).into())?;
        #[cfg(not(target_arch = "wasm32"))]
        if accounting.native_work().is_some() {
            return Ok(crate::kernel::OwnedSourceChunk::intrinsic(bytes, 0));
        }
        return Ok(crate::kernel::OwnedSourceChunk::claimed(bytes, 0));
    }
    let before = readonly_standalone_live(accounting);
    let result = crate::kernel::read_blob_range_with_accounting(
        &volume.fs.inner.objects, target, ByteRange { offset: 0, length: target_bytes },
        DecodeLimits::for_volume(volume.config), accounting, cancellation,
    ).await;
    let retained = result.as_ref().map_or(0, |read| read.logical_owned_bytes());
    readonly_cleanup_standalone(accounting, before, retained)?;
    result.map(|read| {
        let owned = read.logical_owned_bytes();
        #[cfg(not(target_arch = "wasm32"))]
        if accounting.native_work().is_some() {
            return crate::kernel::OwnedSourceChunk::intrinsic(read.bytes, owned);
        }
        crate::kernel::OwnedSourceChunk::claimed(read.bytes, owned)
    })
}

fn order_batch_results<T>(
    mut results: Vec<(usize, FsResult<T>)>,
    budget: WorkBudget,
) -> FsResult<Vec<T>> {
    results.sort_unstable_by_key(|(index, _)| *index);
    let mut work = WorkCounters::default();
    let mut completed = Vec::new();
    completed
        .try_reserve_exact(results.len())
        .map_err(|_| OperationFailure::before_work(FsError::Work(WorkError::Overflow)))?;
    let mut first_failure = None;
    for (_, result) in results {
        match result {
            Ok(receipt) => {
                work = add(work, receipt.work)?;
                completed.push(receipt.value);
            }
            Err(failure) => {
                work = add(work, *failure.work)?;
                if first_failure.is_none() {
                    first_failure = Some(failure.error);
                }
            }
        }
    }
    work.verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    if let Some(error) = first_failure {
        return Err(OperationFailure::new(error, work));
    }
    Ok(FsReceipt {
        value: completed,
        work,
    })
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    /// Prepares one bounded two-parent merge against this checkout's immutable base.
    pub async fn prepare_merge(
        &mut self,
        theirs: GenerationId,
        maximum_changes: u32,
        maximum_conflicts: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<MergePreparation> {
        if self.mode.access != AccessMode::ReadWrite
            || self.mode.mutations != MutationMode::PrivateOverlay
        {
            return Err(OperationFailure::before_work(FsError::MutationNotAllowed));
        }
        if self.prepared_merge_parent.is_some() {
            return Err(OperationFailure::before_work(FsError::PreparedMergePending));
        }
        if maximum_changes == 0 || maximum_conflicts == 0 {
            return Err(OperationFailure::before_work(FsError::MergeChangeLimit));
        }
        let theirs_id = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: theirs.digest(),
        };
        let (current_head_root, current_head, mut work) = self
            .volume
            .resolve_head_generation(budget, cancellation)
            .await?;
        if current_head_root != theirs_id {
            return Err(OperationFailure::new(FsError::MergeParentNotHead, work));
        }
        let (theirs_root, theirs_work) = read_generation_root(
            &self.volume.fs.inner.objects,
            theirs_id,
            self.volume.config,
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, theirs_work)?;
        let merged = merge_generation_async(
            &self.volume.fs.inner.objects,
            MergeGenerationRequest {
                base_generation: self.root.base(),
                base: self.base_root.clone(),
                ours_generation: (self.root.file_table == self.base_file_table)
                    .then_some(self.generation_root),
                ours: self.root.clone(),
                theirs_generation: theirs_id,
                theirs: theirs_root,
                retain_theirs_parent: true,
                maximum_changes,
                maximum_conflicts,
                resolutions: BTreeMap::new(),
            },
            DecodeLimits::for_volume(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| map_merge_failure(failure, work))?;
        work = add(work, merged.work)?;
        match merged.value {
            MergeGenerationOutcome::Prepared {
                generation_root,
                root,
                generation_id,
            } => {
                self.root.set_working(root);
                self.generation_root = generation_root;
                self.prepared_merge_parent = Some(theirs_id);
                self.authority_head = Some(current_head);
                Ok(OperationReceipt {
                    value: MergePreparation::Prepared { generation_id },
                    work,
                })
            }
            MergeGenerationOutcome::Conflicted {
                conflicts,
                truncated,
            } => Ok(OperationReceipt {
                value: MergePreparation::Conflicted {
                    conflicts,
                    truncated,
                },
                work,
            }),
        }
    }

    /// Builds a deterministic complete manifest for the current immutable or
    pub async fn export_manifest(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<GenerationExportManifest> {
        let checkpoint = self.checkpoint_root(budget, cancellation).await?;
        let generation_root = checkpoint.value;
        let mut work = checkpoint.work;
        let manifest = build_generation_export_manifest_async(
            &self.volume.fs.inner.objects,
            self.volume.id,
            self.volume.config,
            generation_root,
            closure_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, map_transfer_error))?;
        work = add(work, manifest.work)?;
        Ok(FsReceipt {
            value: manifest.value,
            work,
        })
    }

    /// Writes the immutable generation-root checkpoint for the current sparse
    pub async fn checkpoint(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<GenerationId> {
        let checkpoint = self.checkpoint_root(budget, cancellation).await?;
        Ok(FsReceipt {
            value: GenerationId::new(checkpoint.value.digest),
            work: checkpoint.work,
        })
    }

    async fn checkpoint_root<W: FsWork>(
        &self, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<ObjectId, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = if self.prepared_merge_parent.is_some() || !self.has_pending_mutations() {
            Ok(self.generation_root)
        } else {
            crate::kernel::build_checkpoint_with_accounting(
                &self.volume.fs.inner.objects,
                CheckpointRequest {
                    base: self.root.base(), file_table: self.root.file_table, merge_parent: None,
                },
                DecodeLimits::for_volume(self.volume.config), &mut accounting, cancellation,
            ).await
        };
        budget.finish(result, &accounting)
    }

    /// Discards a private candidate and reauthenticates its immutable base.
    pub async fn discard(
        &mut self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        if self.mode.mutations == MutationMode::None {
            return Err(OperationFailure::before_work(FsError::MutationNotAllowed));
        }
        let (root, mut work) = read_generation_root(
            &self.volume.fs.inner.objects,
            self.root.base(),
            self.volume.config,
            budget,
            cancellation,
        )
        .await?;
        if root.volume_id != self.volume.id {
            return Err(OperationFailure::new(FsError::VolumeMismatch, work));
        }
        let live = root_parent_bytes(&root).map_err(|error| OperationFailure::new(error, work))?;
        let mut root_accounting = KernelAccounting::standalone(remaining_owned(work, budget, live)?, cancellation);
        let prepared = measured(self.prepare_root_replacement(
            &root, &root, &mut root_accounting, cancellation, None,
        ), &root_accounting)
            .map_err(|failure| merge_simultaneous_failure(work, *failure.work, live, failure.error))?;
        work = merge_simultaneous_work(work, prepared.work, live, budget)?;
        self.install_root_replacement(self.root.base(), root, prepared.value);
        self.base_file_table = self.root.file_table;
        self.generation_root = self.root.base();
        self.live_operation_id = None;
        self.prepared_merge_parent = None;
        self.dependencies.proof().clear();
        Ok(FsReceipt { value: (), work })
    }

    /// Explicitly advances one clean manual or tracking checkout to head.
    pub async fn refresh_head(
        &mut self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<GenerationId> {
        if !matches!(
            self.mode.consistency,
            ConsistencyMode::Manual | ConsistencyMode::TrackingSafe
        ) {
            return Err(OperationFailure::before_work(FsError::RefreshNotAllowed));
        }
        if self.has_pending_mutations() {
            return Err(OperationFailure::before_work(
                FsError::PendingMutationsRequireRebase,
            ));
        }
        let (generation_root, head, mut work) = self
            .volume
            .resolve_head_generation(budget, cancellation)
            .await?;
        if generation_root == self.generation_root {
            self.authority_head = Some(head);
            return Ok(FsReceipt {
                value: self.generation_id(),
                work,
            });
        }
        let (root, root_work) = read_generation_root(
            &self.volume.fs.inner.objects,
            generation_root,
            self.volume.config,
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, root_work)?;
        if root.volume_id != self.volume.id {
            return Err(OperationFailure::new(FsError::VolumeMismatch, work));
        }
        let live = root_parent_bytes(&root).map_err(|error| OperationFailure::new(error, work))?;
        let mut root_accounting = KernelAccounting::standalone(remaining_owned(work, budget, live)?, cancellation);
        let prepared = measured(self.prepare_root_replacement(
            &root, &root, &mut root_accounting, cancellation, None,
        ), &root_accounting)
            .map_err(|failure| merge_simultaneous_failure(work, *failure.work, live, failure.error))?;
        work = merge_simultaneous_work(work, prepared.work, live, budget)?;
        self.generation_root = generation_root;
        self.install_root_replacement(generation_root, root, prepared.value);
        self.base_file_table = self.root.file_table;
        self.authority_head = Some(head);
        self.dependencies.proof().clear();
        Ok(FsReceipt {
            value: self.generation_id(),
            work,
        })
    }

    /// Safely advances to the current head and rebases the private candidate.
    pub async fn rebase_head(
        &mut self,
        maximum_conflicts: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<RebaseDecision> {
        if self.live_operation_id.is_some() {
            return Err(OperationFailure::before_work(FsError::PendingLiveMutation));
        }
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
        let result = self.rebase_head_with_accounting(maximum_conflicts, &mut accounting, cancellation).await;
        let work = accounting.spent();
        result.map(|(value, allocation)| {
            drop(allocation);
            FsReceipt { value, work }
        }).map_err(|error| OperationFailure::new(error, work))
    }

    /// Reclassifies exact dependencies on the original native invocation.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn rebase_head_with_native_work(
        &mut self,
        maximum_conflicts: u32,
        original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(RebaseDecision, crate::NativeAllocation), FsError> {
        original.admit(WorkCounters::UNCHARGED)?;
        cancellation.check()?;
        if self.live_operation_id.is_some() { return Err(FsError::PendingLiveMutation); }
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(original, cancellation);
        let (decision, allocation) = self.rebase_head_with_accounting(
            maximum_conflicts, &mut accounting, cancellation,
        ).await?;
        match allocation {
            crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) => Ok((decision, allocation)),
            crate::kernel::allocation::KernelOwnedAllocation::Standalone =>
                unreachable!("native rebase preserves its original allocation custody"),
        }
    }

    async fn rebase_head_with_accounting(
        &mut self,
        maximum_conflicts: u32,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(RebaseDecision, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
        use crate::kernel::Owned;
        accounting.check()?;
        if self.prepared_merge_parent.is_some() { return Err(FsError::PreparedMergePending); }
        if !matches!(self.mode.consistency,
            ConsistencyMode::TrackingSafe | ConsistencyMode::Manual | ConsistencyMode::Live) {
            return Err(FsError::RefreshNotAllowed);
        }
        let (candidate_object, candidate_head) = {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                self.volume.resolve_head_generation_with_native_work(original, cancellation).await?
            } else {
                self.resolve_rebase_head_standalone(accounting, cancellation).await?
            }
            #[cfg(target_arch = "wasm32")]
            self.resolve_rebase_head_standalone(accounting, cancellation).await?
        };
        let (probe, probe_allocation) = AuthenticatedGenerationProbe::new_with_accounting(
            &self.volume.fs.inner.objects,
            probe_limits(self.volume.config),
            accounting,
        )?;
        let probe = (probe, probe_allocation);
        // The proof snapshot is an Arc share, not a copied map or name graph.
        let dependencies = self.dependencies.proof().dependencies.clone();
        let classified = crate::kernel::classify_rebase_with_accounting(
            &probe.0, GenerationId::new(self.root.base().digest),
            GenerationId::new(candidate_object.digest), &dependencies,
            maximum_conflicts, accounting, cancellation,
        ).await.map_err(|error| match error {
            crate::kernel::ClassifyRebaseFailure::Probe(error)
            | crate::kernel::ClassifyRebaseFailure::Accounting(error) => error,
            crate::kernel::ClassifyRebaseFailure::ZeroConflictLimit =>
                FsError::Rebase(RebaseError::ZeroConflictLimit),
        });
        let retired = probe.0.retire_with_accounting(accounting);
        drop(probe.1);
        retired?;
        let classified = classified?;
        if matches!(classified.0, RebaseDecision::Conflicted { .. }) { return Ok(classified); }
        if candidate_object == self.root.base() {
            cancellation.check()?;
            self.authority_head = Some(candidate_head);
            return Ok(classified);
        }
        let (candidate_root, allocation) = crate::kernel::read_generation_root_with_accounting(
            &self.volume.fs.inner.objects, candidate_object, self.volume.config, accounting, cancellation,
        ).await?;
        let candidate_bytes = root_parent_bytes(&candidate_root)?;
        let candidate = Owned::new(candidate_root, allocation, candidate_bytes);
        let outcome = async {
            if candidate.value.volume_id != self.volume.id { return Err(FsError::VolumeMismatch); }
            let candidate_file_table = candidate.value.file_table;
            let (root, allocation) = self.rebase_private_candidate_with_accounting(
                candidate_object, &candidate.value, maximum_conflicts, accounting, cancellation,
            ).await?;
            let bytes = root_parent_bytes(&root)?;
            let rebased = Owned::new(root, allocation, bytes);
            let staged = self.prepare_root_replacement(
                &candidate.value, &rebased.value, accounting, cancellation, None,
            );
            let prepared = match staged {
                Ok(prepared) => prepared,
                Err(error) => { rebased.retire(accounting)?; return Err(error); }
            };
            Ok::<_, FsError>((rebased, prepared, candidate_file_table))
        }.await;
        candidate.retire(accounting)?;
        let (mut rebased, mut prepared, candidate_file_table) = outcome?;
        if let Err(error) = cancellation.check() {
            rebased.retire(accounting)?;
            return Err(error.into());
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(guard) =
                &mut rebased.allocation else { unreachable!("native root has original custody"); };
            if let Err(error) = self.prepare_root_working_control_with_native_work(
                &rebased.value, guard, &mut prepared, original, cancellation,
            ) {
                rebased.retire(accounting)?;
                return Err(error);
            }
        }
        // Working-root transfer is the final fallible step. No operation
        self.install_root_replacement(candidate_object, rebased.value, prepared);
        self.base_file_table = candidate_file_table;
        self.generation_root = candidate_object;
        self.authority_head = Some(candidate_head);
        drop(rebased.allocation);
        Ok(classified)
    }

    async fn resolve_rebase_head_standalone(
        &self,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(ObjectId, Head), FsError> {
        match self.volume.resolve_head_generation(accounting.remaining()?, cancellation).await {
            Ok((root, head, work)) => {
                accounting.record_completed(work)?;
                Ok((root, head))
            }
            Err(failure) => {
                accounting.record_completed(*failure.work)?;
                Err(failure.error)
            }
        }
    }

    async fn rebase_private_candidate_with_accounting(
        &self,
        candidate_object: ObjectId,
        candidate_root: &GenerationRoot,
        maximum_conflicts: u32,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(GenerationRoot, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
        use crate::kernel::{clone_generation_root_with_accounting, retire_allocation};
        if self.root.file_table == self.base_file_table {
            return clone_generation_root_with_accounting(candidate_root, accounting);
        }
        let checkpoint = crate::kernel::build_checkpoint_with_accounting(
            &self.volume.fs.inner.objects,
            CheckpointRequest { base: self.root.base(), file_table: self.root.file_table, merge_parent: None },
            DecodeLimits::for_volume(self.volume.config), accounting, cancellation,
        ).await?;
        let base = clone_generation_root_with_accounting(&self.base_root, accounting)?;
        let ours = match clone_generation_root_with_accounting(candidate_root, accounting) {
            Ok(ours) => ours,
            Err(error) => {
                let bytes = root_parent_bytes(&base.0)?;
                drop(base.0);
                retire_allocation(base.1, bytes, accounting)?;
                return Err(error);
            }
        };
        let theirs = match clone_generation_root_with_accounting(&self.root, accounting) {
            Ok(theirs) => theirs,
            Err(error) => {
                let base_bytes = root_parent_bytes(&base.0)?;
                let ours_bytes = root_parent_bytes(&ours.0)?;
                drop(base.0);
                drop(ours.0);
                retire_allocation(base.1, base_bytes, accounting)?;
                retire_allocation(ours.1, ours_bytes, accounting)?;
                return Err(error);
            }
        };
        let input_bytes = [root_parent_bytes(&base.0)?, root_parent_bytes(&ours.0)?, root_parent_bytes(&theirs.0)?];
        let merged = crate::kernel::merge_generation_with_accounting(
            &self.volume.fs.inner.objects,
            MergeGenerationRequest {
                base_generation: self.root.base(), base: base.0,
                ours_generation: Some(candidate_object), ours: ours.0,
                theirs_generation: checkpoint, theirs: theirs.0,
                retain_theirs_parent: false, maximum_changes: u32::MAX,
                maximum_conflicts, resolutions: BTreeMap::new(),
            },
            DecodeLimits::for_volume(self.volume.config), accounting, cancellation,
        ).await;
        retire_allocation(base.1, input_bytes[0], accounting)?;
        retire_allocation(ours.1, input_bytes[1], accounting)?;
        retire_allocation(theirs.1, input_bytes[2], accounting)?;
        let (outcome, allocation) = merged?;
        match outcome {
            MergeGenerationOutcome::Prepared { root, .. } => Ok((root, allocation)),
            MergeGenerationOutcome::Conflicted { conflicts, .. } => {
                let bytes = conflicts.iter().try_fold(
                    crate::foundation::usize_to_u64(conflicts.capacity() * size_of::<crate::kernel::MergeConflict>()),
                    |bytes, conflict| {
                        let nested = match conflict {
                            crate::kernel::MergeConflict::Binding { name, .. } => name.retained_bytes(),
                            crate::kernel::MergeConflict::File(_) => 0,
                        };
                        bytes.checked_add(crate::foundation::usize_to_u64(nested)).ok_or(WorkError::Overflow)
                    },
                )?;
                drop(conflicts);
                retire_allocation(allocation, bytes, accounting)?;
                Err(FsError::InvalidDiff)
            }
        }
    }

    /// Explicitly performs the same observation-safe head advancement used
    pub async fn refresh_live(
        &mut self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<GenerationId> {
        if self.mode.consistency != ConsistencyMode::Live {
            return Err(OperationFailure::before_work(FsError::RefreshNotAllowed));
        }
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = self.synchronize_live_with_accounting(&mut accounting, cancellation).await
            .map(|()| self.generation_id());
        measured(result, &accounting)
    }

    pub(crate) async fn synchronize_live_with_accounting(
        &mut self,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        accounting.check()?;
        if self.mode.consistency != ConsistencyMode::Live { return Ok(()); }
        if self.live_operation_id.is_some() { return Err(FsError::PendingLiveMutation); }
        let (decision, allocation) = self.rebase_head_with_accounting(
            self.volume.config.limits.maximum_checkout_dependencies, accounting, cancellation,
        ).await?;
        match decision {
            RebaseDecision::Safe { .. } => { drop(allocation); Ok(()) }
            RebaseDecision::Conflicted { conflicts, truncated } => {
                match allocation {
                    crate::kernel::allocation::KernelOwnedAllocation::Standalone =>
                        Err(FsError::LiveConflict { conflicts, truncated }),
                    #[cfg(not(target_arch = "wasm32"))]
                    crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) =>
                        Err(FsError::NativeLiveConflict { conflicts, truncated, allocation }),
                }
            }
        }
    }

    async fn capture_mutation_dependencies_with_accounting(
        &self, operations: &[Mutation], accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::kernel::Owned<Vec<Dependency>>, FsError> {
        use crate::kernel::Owned;
        let mut dependencies = Owned::<Vec<Dependency>>::allocate(0, accounting)?;
        let mut exact_regions = Owned::<Vec<(Option<FileRecord>, DependencyRegion)>>::allocate(
            0, accounting,
        )?;
        for operation in operations {
            accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            let (first_record, second_record) = if let Some([first_path, second_path]) = operation.paths() {
                let first = crate::kernel::observe_path_async_with_accounting(
                    &self.volume.fs.inner.objects, &self.base_root, first_path,
                    self.volume.config, accounting, cancellation, None, false,
                ).await?;
                let first = first.into_parts();
                let first_record = first.0.lookup.record;
                let bytes = Self::mutation_dependency_bytes(&first.0.dependencies, accounting)?;
                Self::append_mutation_dependencies(
                    &mut dependencies, Owned::new(first.0.dependencies, first.1, bytes),
                    accounting,
                )?;
                let second_record = if second_path == first_path {
                    first_record
                } else {
                    let second = crate::kernel::observe_path_async_with_accounting(
                        &self.volume.fs.inner.objects, &self.base_root, second_path,
                        self.volume.config, accounting, cancellation, None, false,
                    ).await?;
                    let second = second.into_parts();
                    let second_record = second.0.lookup.record;
                    let bytes = Self::mutation_dependency_bytes(&second.0.dependencies, accounting)?;
                    Self::append_mutation_dependencies(
                        &mut dependencies, Owned::new(second.0.dependencies, second.1, bytes),
                        accounting,
                    )?;
                    second_record
                };
                (first_record, second_record)
            } else {
                let (first_id, second_id) = match operation {
                    Mutation::File { file_id, .. } => (Some(*file_id), None),
                    Mutation::CloneFileRange { source_file_id, destination_file_id, .. } =>
                        (Some(*source_file_id), Some(*destination_file_id)),
                    _ => (None, None),
                };
                let first = if let Some(file_id) = first_id {
                    let record = crate::kernel::lookup_file_record_async_with_accounting(
                        &self.volume.fs.inner.objects, self.base_root.file_table, file_id,
                        DecodeLimits::for_volume(self.volume.config), accounting, cancellation, None,
                    ).await?;
                    record.0
                } else { None };
                let second = if second_id == first_id {
                    first
                } else if let Some(file_id) = second_id {
                    let record = crate::kernel::lookup_file_record_async_with_accounting(
                        &self.volume.fs.inner.objects, self.base_root.file_table, file_id,
                        DecodeLimits::for_volume(self.volume.config), accounting, cancellation, None,
                    ).await?;
                    record.0
                } else { None };
                (first, second)
            };
            for region in exact_mutation_regions(operation, first_record, second_record)
                .into_iter().flatten()
            {
                exact_regions.ensure_for_push(usize::MAX, accounting)?;
                accounting.charge(WorkCounters {
                    bytes_copied: size_of::<(Option<FileRecord>, DependencyRegion)>() as u64,
                    ..WorkCounters::UNCHARGED
                })?;
                exact_regions.value.push(region);
            }
        }
        Self::subsume_mutation_record_dependencies(&mut exact_regions.value, accounting)?;
        let (probe, probe_allocation) = AuthenticatedGenerationProbe::new_with_accounting(
            &self.volume.fs.inner.objects, probe_limits(self.volume.config), accounting,
        )?;
        let probe = (probe, probe_allocation);
        let region_bytes = exact_regions.bytes;
        let captured = probe.0.capture_records_many_with_accounting(
            exact_regions.value, self.volume.config.limits.maximum_checkout_dependencies,
            accounting, cancellation, None,
        ).await;
        crate::kernel::retire_allocation(
            exact_regions.allocation, region_bytes, accounting,
        )?;
        probe.0.retire_with_accounting(accounting)?;
        drop(probe.1);
        let captured = captured?;
        let bytes = Self::mutation_dependency_bytes(&captured.0, accounting)?;
        Self::append_mutation_dependencies(
            &mut dependencies, Owned::new(captured.0, captured.1, bytes), accounting,
        )?;
        Ok(dependencies)
    }

    fn mutation_region_name_bytes(region: &DependencyRegion) -> u64 {
        match region {
            DependencyRegion::DirectoryName { name, .. }
            | DependencyRegion::DirectoryRange { after: Some(name), .. } => name.retained_bytes() as u64,
            _ => 0,
        }
    }

    fn mutation_dependency_bytes(
        values: &Vec<Dependency>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    ) -> Result<u64, FsError> {
        let mut bytes = (values.capacity() as u64).checked_mul(size_of::<Dependency>() as u64)
            .ok_or(WorkError::Overflow)?;
        for dependency in values {
            accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            bytes = bytes.checked_add(Self::mutation_region_name_bytes(&dependency.region))
                .ok_or(WorkError::Overflow)?;
        }
        Ok(bytes)
    }

    fn append_mutation_dependencies(
        destination: &mut crate::kernel::Owned<Vec<Dependency>>,
        mut incoming: crate::kernel::Owned<Vec<Dependency>>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        for dependency in incoming.value.drain(..) {
            destination.ensure_for_push(usize::MAX, accounting)?;
            let bytes = Self::mutation_region_name_bytes(&dependency.region);
            let allocation = crate::kernel::split_allocation(
                &mut incoming.allocation, bytes,
            )?;
            let mut owned = (dependency, allocation);
            destination.allocation.absorb(&mut owned.1)?;
            incoming.bytes = incoming.bytes.checked_sub(bytes).ok_or(WorkError::Overflow)?;
            destination.bytes = destination.bytes.checked_add(bytes).ok_or(WorkError::Overflow)?;
            accounting.charge(WorkCounters {
                bytes_copied: size_of::<Dependency>() as u64, ..WorkCounters::UNCHARGED
            })?;
            destination.value.push(owned.0);
        }
        incoming.retire(accounting)
    }

    fn subsume_mutation_record_dependencies(
        regions: &mut Vec<(Option<FileRecord>, DependencyRegion)>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        let mut records = crate::kernel::Owned::<Vec<FileId>>::allocate(
            regions.len(), accounting,
        )?;
        for (_, region) in regions.iter() {
            accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            if let DependencyRegion::FileRecord(file_id) = region {
                let position = crate::kernel::binary_search_with_accounting(
                    &records.value, accounting, |value| value.cmp(file_id),
                )?;
                if let Err(index) = position {
                    accounting.charge(WorkCounters {
                        bytes_copied: ((records.value.len() - index + 1) * size_of::<FileId>()) as u64,
                        ..WorkCounters::UNCHARGED
                    })?;
                    records.value.insert(index, *file_id);
                }
            }
        }
        let mut index = 0;
        while index < regions.len() {
            accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            let file_id = match &regions[index].1 {
                DependencyRegion::ContentRange { file_id, .. }
                | DependencyRegion::FileLength(file_id)
                | DependencyRegion::SparseSeek { file_id, .. } => Some(file_id),
                _ => None,
            };
            let removed = match file_id {
                Some(file_id) => crate::kernel::binary_search_with_accounting(
                    &records.value, accounting, |value| value.cmp(file_id),
                )?.is_ok(),
                None => false,
            };
            if removed {
                accounting.charge(WorkCounters {
                    bytes_copied: ((regions.len() - index - 1) * size_of::<(Option<FileRecord>, DependencyRegion)>()) as u64,
                    ..WorkCounters::UNCHARGED
                })?;
                regions.remove(index);
            } else {
                index += 1;
            }
        }
        records.retire(accounting)
    }

    /// Atomically applies one ordered mutation batch to this private checkout.
    pub async fn mutate(
        &mut self,
        operations: Vec<Mutation>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        crate::heap_future::in_heap_measured(move |budget| async move {
            if self.mode.access != AccessMode::ReadWrite
                || !matches!(
                    self.mode.mutations,
                    MutationMode::PrivateOverlay | MutationMode::DirectLive
                )
            {
                return Err(OperationFailure::before_work(FsError::MutationNotAllowed));
            }
            self.mutate_candidate(operations, budget, cancellation)
                .await
        }, budget, cancellation, FsError::Cancelled(CancellationError), None)
        .await
    }

    /// Applies and durably publishes one direct-live mutation operation.
    pub async fn mutate_live(
        &mut self,
        operations: Vec<Mutation>,
        operation_id: OperationId,
        maximum_attempts: u32,
        maximum_conflicts: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<LiveMutationOutcome> {
        self.validate_live_operation(maximum_attempts).map_err(OperationFailure::before_work)?;
        if self.has_pending_mutations() {
            return Err(OperationFailure::before_work(FsError::PendingLiveMutation));
        }
        let mutation = self
            .mutate_candidate(operations, budget, cancellation)
            .await?;
        let mut work = mutation.work;
        let publication = self
            .resume_live(
                operation_id,
                maximum_attempts,
                maximum_conflicts,
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, publication.work)?;
        Ok(FsReceipt {
            value: publication.value,
            work,
        })
    }

    /// Compiles, atomically applies, and durably publishes one authored live transaction.
    pub async fn apply_authored_live(
        &mut self,
        authored: Vec<AuthoredMutation>,
        operation_id: OperationId,
        maximum_attempts: u32,
        maximum_conflicts: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<AuthoredLiveMutationResult> {
        self.validate_live_operation(maximum_attempts).map_err(OperationFailure::before_work)?;
        if self.has_pending_mutations() {
            return Err(OperationFailure::before_work(FsError::PendingLiveMutation));
        }
        let transaction = self
            .apply_authored_transaction(authored, budget, cancellation)
            .await?;
        let mut work = transaction.work;
        let created_file_ids = transaction.value.created_file_ids;
        let publication = self
            .resume_live(
                operation_id,
                maximum_attempts,
                maximum_conflicts,
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, publication.work)?;
        Ok(FsReceipt {
            value: AuthoredLiveMutationResult {
                created_file_ids,
                outcome: publication.value,
            },
            work,
        })
    }

    /// Resumes publication of an unresolved direct-live candidate.
    pub async fn resume_live(
        &mut self,
        operation_id: OperationId,
        maximum_attempts: u32,
        maximum_conflicts: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<LiveMutationOutcome> {
        self.validate_live_operation(maximum_attempts).map_err(OperationFailure::before_work)?;
        if !self.has_pending_mutations() {
            return Err(OperationFailure::before_work(FsError::NoPendingMutations));
        }
        match self.live_operation_id {
            Some(retained) if retained != operation_id => {
                return Err(OperationFailure::before_work(FsError::PendingLiveMutation));
            }
            Some(_) => {}
            None => self.live_operation_id = Some(operation_id),
        }
        let publication = self
            .publish_live(
                operation_id,
                maximum_attempts,
                maximum_conflicts,
                budget,
                cancellation,
            )
            .await?;
        if matches!(
            publication.value,
            LiveMutationOutcome::Committed { .. }
                | LiveMutationOutcome::AlreadyCommitted { .. }
                | LiveMutationOutcome::IdempotencyConflict { .. }
        ) {
            self.live_operation_id = None;
        }
        Ok(publication)
    }

    /// Resumes the existing live retry controller on the original operation.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn resume_live_with_native_work<'a, E, F, Fut>(
        &mut self,
        operation_id: OperationId,
        maximum_attempts: u32,
        maximum_conflicts: u32,
        permit: PublicationPermit,
        original: &'a crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
        before_publication: &mut F,
        producer_custody: Option<&Arc<crate::kernel::ProofDataCustody>>,
        operation_context: Option<Digest>,
        operation_evidence: Option<acyclic_stream::OwnedBytes>,
        original_fingerprint_permit: Option<PublicationPermit>,
    ) -> Result<(LiveMutationOutcome, crate::NativeAllocation), E>
    where
        E: From<FsError> + Send + 'static,
        F: for<'materialization> FnMut(&'materialization crate::NativeMaterializationWork) -> Fut
            + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(original, cancellation);
        accounting.check().map_err(E::from)?;
        self.validate_live_operation(maximum_attempts).map_err(E::from)?;
        self.validate_native_work(original).map_err(E::from)?;
        if !self.has_pending_mutations() {
            return Err(E::from(FsError::NoPendingMutations));
        }
        match self.live_operation_id {
            Some(retained) if retained != operation_id =>
                return Err(E::from(FsError::PendingLiveMutation)),
            Some(_) => {}
            None => self.live_operation_id = Some(operation_id),
        }
        let mut state = LiveRetryState::new(maximum_attempts)
            .map_err(|error| E::from(FsError::LiveRetry(error)))?;
        let mut empty_allocation = accounting.claim_owned(0, 0).map_err(E::from)?;
        let (outcome, allocation) = loop {
            accounting.check().map_err(E::from)?;
            let mut before_publication = (*before_publication).clone();
            let publication = match self.retry_stale_commit_with_native_work(
                operation_id, permit, original, cancellation, &mut before_publication,
                producer_custody, operation_context, operation_evidence.clone(), original_fingerprint_permit,
            ).await? {
                Some(outcome) => outcome,
                None => self.publish_pending_with_native_work(
                    operation_id, operation_context, operation_evidence.clone(), original_fingerprint_permit,
                    permit, original, cancellation, &mut before_publication, producer_custody,
                ).await?,
            };
            if let Some(completed) = self.finish_live_retry(
                &mut state, publication, maximum_conflicts, &mut accounting, cancellation, &mut empty_allocation,
            ).await.map_err(E::from)? {
                break completed;
            }
        };
        if matches!(outcome,
            LiveMutationOutcome::Committed { .. }
                | LiveMutationOutcome::AlreadyCommitted { .. }
                | LiveMutationOutcome::IdempotencyConflict { .. })
        {
            self.live_operation_id = None;
        }
        match allocation {
            crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) =>
                Ok((outcome, allocation)),
            crate::kernel::allocation::KernelOwnedAllocation::Standalone =>
                unreachable!("native live outcomes preserve original allocation custody"),
        }
    }

    fn validate_live_operation(&self, maximum_attempts: u32) -> Result<(), FsError> {
        if self.mode.access != AccessMode::ReadWrite
            || self.mode.consistency != ConsistencyMode::Live
            || self.mode.mutations != MutationMode::DirectLive
        {
            return Err(FsError::MutationNotAllowed);
        }
        if maximum_attempts == 0 {
            return Err(FsError::ZeroLiveRetryLimit);
        }
        Ok(())
    }

    async fn publish_live(
        &mut self, operation_id: OperationId, maximum_attempts: u32, maximum_conflicts: u32,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> FsResult<LiveMutationOutcome> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            accounting.check()?;
            let mut state = LiveRetryState::new(maximum_attempts).map_err(FsError::LiveRetry)?;
            let mut empty_allocation = accounting.claim_owned(0, 0)?;
            loop {
                accounting.check()?;
                let publication = self.publish_pending(
                    operation_id, None, None, None, PublicationPermit::Unrestricted,
                    accounting.remaining()?, cancellation,
                    &mut |_| async { FsResult::Ok(FsReceipt { value: (), work: WorkCounters::UNCHARGED }) },
                ).await;
                let incoming = match &publication { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
                let (allocations, work, allowance, _) = accounting.standalone_parts()
                    .ok_or(FsError::PendingMutationAllocationFailed)?;
                *work = merge_simultaneous_work(*work, incoming, allocations.live_bytes(), allowance)
                    .map_err(|failure| failure.error)?;
                let publication = publication.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
                if let Some((outcome, allocation)) = self.finish_live_retry(
                    &mut state, publication, maximum_conflicts, &mut accounting, cancellation, &mut empty_allocation,
                ).await? {
                    drop(allocation);
                    return Ok(outcome);
                }
            }
        }.await;
        measured(result, &accounting)
    }

    async fn finish_live_retry(
        &mut self, state: &mut LiveRetryState, publication: CheckoutCommitOutcome, maximum_conflicts: u32,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
        empty_allocation: &mut KernelOwnedAllocation,
    ) -> Result<Option<(LiveMutationOutcome, KernelOwnedAllocation)>, FsError> {
        match state.observe_publication(live_publication_observation(publication)).map_err(FsError::LiveRetry)? {
            LiveRetryAction::Complete(outcome) => Ok(Some((
                outcome, std::mem::replace(empty_allocation, KernelOwnedAllocation::Standalone),
            ))),
            LiveRetryAction::Publish => Err(FsError::LiveRetry(LiveRetryError::InvalidTransition)),
            LiveRetryAction::Rebase => {
                let (decision, allocation) = self.rebase_head_with_accounting(
                    maximum_conflicts, accounting, cancellation,
                ).await?;
                match state.observe_rebase(decision).map_err(FsError::LiveRetry)? {
                    LiveRetryAction::Publish => { drop(allocation); Ok(None) }
                    LiveRetryAction::Complete(outcome) => Ok(Some((outcome, allocation))),
                    LiveRetryAction::Rebase => Err(FsError::LiveRetry(LiveRetryError::InvalidTransition)),
                }
            }
        }
    }

    async fn mutate_candidate(
        &mut self,
        operations: Vec<Mutation>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
        let result = self.mutate_candidate_with_accounting(
            operations, None, &mut accounting, cancellation,
        ).await;
        measured(result, &accounting)
    }

    /// Checks retained original custody, without admitting work or granting rights.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn validate_native_work(
        &self,
        original: &crate::NativeMaterializationWork,
    ) -> Result<(), FsError> {
        if !original.owns_native_checkout_token(&self.dependencies.data_custody)
            || !self.root.same_original_scope(original.native_read_meter())
            || !self.dependencies.base_root_control.is_native_original(
                original.native_read_meter(), root_parent_bytes(&self.base_root)?,
            )
        {
            return Err(FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout));
        }
        Ok(())
    }

    /// Transfers an already-paid operation vector to the canonical candidate
    pub(crate) async fn mutate_owned_with_accounting(
        &mut self,
        mut operations: crate::kernel::Owned<Vec<Mutation>>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        use crate::kernel::allocation::KernelOwnedAllocation;
        accounting.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        match (accounting.native_work(), &operations.allocation) {
            (Some(original), KernelOwnedAllocation::NativeOriginal(allocation)) => {
                if !allocation.work().shares_ledger(original) {
                    return Err(FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout));
                }
                self.validate_native_work(original)?;
            }
            (None, KernelOwnedAllocation::Standalone) => {}
            _ => return Err(FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout)),
        }
        if self.mode.access != AccessMode::ReadWrite
            || !matches!(self.mode.mutations, MutationMode::PrivateOverlay | MutationMode::DirectLive)
        {
            return Err(FsError::MutationNotAllowed);
        }
        self.mutate_candidate_with_accounting(
            operations.value, Some(&mut operations.allocation), accounting, cancellation,
        ).await
    }

    async fn mutate_candidate_with_accounting(
        &mut self,
        mut operations: Vec<Mutation>,
        operations_allocation: Option<&mut crate::kernel::allocation::KernelOwnedAllocation>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
        accounting.check()?;
        if self.live_operation_id.is_some() { return Err(FsError::PendingLiveMutation); }
        if self.prepared_merge_parent.is_some() { return Err(FsError::PreparedMergePending); }
        let maximum = usize::try_from(self.volume.config.limits.maximum_mutations_per_batch)
            .unwrap_or(usize::MAX);
        if operations.capacity() > maximum { return Err(FsError::ExcessiveMutationCapacity); }
        let incoming_bytes = Self::mutation_input_owned_bytes(&operations, accounting)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(KernelOwnedAllocation::NativeOriginal(guard)) = operations_allocation.as_deref() {
            if guard.bytes() != incoming_bytes {
                return Err(FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout));
            }
        }
        #[cfg(not(target_arch = "wasm32"))]
        if operations_allocation.is_none() && accounting.native_work().is_some() {
            return Err(FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout));
        }
        let mut ordinary_input_allocation;
        let operations_allocation = match operations_allocation {
            Some(allocation) => allocation,
            None => {
                ordinary_input_allocation = accounting.claim_owned(incoming_bytes, 0)?;
                &mut ordinary_input_allocation
            }
        };
        self.admit_mutation_names_with_accounting(
            &mut operations, operations_allocation, accounting, cancellation,
        ).await?;
        let captured = self.capture_mutation_dependencies_with_accounting(
            &operations, accounting, cancellation,
        ).await?;
        let extension = self.dependencies.proof().prepare_mutations(
            captured.value, self.volume.config.limits.maximum_checkout_dependencies,
        )?;
        let staged_names = extension.retained_name_bytes_with_step(|| {
            accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })
        })?;
        let mut staged = (extension, captured.allocation);
        accounting.release_owned(
            &mut staged.1, captured.bytes.checked_sub(staged_names).ok_or(WorkError::Overflow)?,
        )?;
        let final_input_bytes = Self::mutation_input_owned_bytes(&operations, accounting)?;
        let produced = crate::kernel::apply_generation_mutations_retaining_with_accounting(
            &self.volume.fs.inner.objects, &self.root, operations, self.volume.config,
            accounting, cancellation,
        ).await;
        let (receipt, returned_operations, allocation) = produced?;
        let mut working = (receipt.root, allocation);
        drop(returned_operations);
        accounting.release_owned(operations_allocation, final_input_bytes)?;
        if working.0.file_table == self.root.file_table {
            let bytes = root_parent_bytes(&working.0)?;
            drop(working.0);
            crate::kernel::retire_allocation(working.1, bytes, accounting)?;
            drop(staged.0);
            accounting.release_owned(&mut staged.1, staged_names)?;
            return Ok(());
        }
        accounting.charge(WorkCounters {
            bytes_copied: size_of::<CheckoutDependencies>() as u64, ..WorkCounters::UNCHARGED
        })?;
        let mut prepared_dependencies = self.dependencies.proof().dependencies.clone();
        let producer = &self.dependencies.data_custody;
        let held = match accounting {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { .. } =>
                self.root.prepare_held_root_native(&working.0, accounting, producer)?,
            _ => {
                let (allocations, work, budget, token) = accounting.standalone_parts()
                    .ok_or(FsError::PendingMutationAllocationFailed)?;
                token.check()?;
                self.root.prepare_held_root(&working.0, allocations, work, budget, token)?
                    .map(|root| {
                        let control = Self::root_control_claim(&root, Some(producer))?;
                        Ok::<_, FsError>((root, control))
                    }).transpose()?
            }
        };
        if working.0.file_table == self.base_file_table {
            prepared_dependencies.clear_mutations();
            drop(staged.0);
            accounting.release_owned(&mut staged.1, staged_names)?;
        } else {
            match accounting {
                #[cfg(not(target_arch = "wasm32"))]
                KernelAccounting::NativeOriginal { work: original, .. } => {
                    let KernelOwnedAllocation::NativeOriginal(allocation) = &mut staged.1 else {
                        unreachable!("original captured names retain original custody");
                    };
                    prepared_dependencies.commit_with_original_data_custody(
                        staged.0, producer, allocation, original, cancellation,
                    )?;
                    // Duplicate keys have already physically died in commit.
                    allocation.release(allocation.bytes())?;
                }
                _ => prepared_dependencies.commit_with_data_custody(
                    staged.0, Some(producer), |bytes, operations, copied, items| {
                        let delta = WorkCounters { bytes_copied: copied, items_examined: items,
                            ..WorkCounters::UNCHARGED };
                        accounting.admit(delta)?;
                        if operations != 0 || copied != 0 {
                            accounting.claim_owned(bytes, operations)?;
                        }
                        accounting.charge(delta)
                    },
                )?,
            }
        }
        let control = match &mut working.1 {
            #[cfg(not(target_arch = "wasm32"))]
            KernelOwnedAllocation::NativeOriginal(allocation) => {
                cancellation.check()?;
                let scope = allocation.work().checkout_control_sink(producer)?;
                allocation.transfer_checkout_control(producer, &scope)?
            }
            KernelOwnedAllocation::Standalone => Self::root_control_claim(&working.0, Some(producer))?,
        };
        // No fallible work follows the transfer into the actual root producer.
        self.root.set_prepared_working(working.0, control, held);
        self.dependencies.proof().dependencies = prepared_dependencies;
        Ok(())
    }

    fn mutation_input_owned_bytes(
        operations: &Vec<Mutation>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    ) -> Result<u64, FsError> {
        let mut bytes = (operations.capacity() as u64).checked_mul(size_of::<Mutation>() as u64)
            .ok_or(WorkError::Overflow)?;
        for operation in operations {
            accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            bytes = bytes.checked_add(mutation_path_owned_bytes(operation)).ok_or(WorkError::Overflow)?;
        }
        Ok(bytes)
    }

    async fn admit_mutation_names_with_accounting(
        &mut self,
        operations: &mut [Mutation],
        operations_allocation: &mut KernelOwnedAllocation,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        accounting.check()?;
        if self.volume.config.case_sensitivity != CaseSensitivity::ProfileFolded
            && self.volume.config.unicode != UnicodePolicy::RequireNfc
        {
            return Ok(());
        }
        let mut pending = Self::name_binding_vector(operations.len(), accounting)?;
        let mut removed = Self::name_binding_vector(operations.len(), accounting)?;
        for operation in operations {
            accounting.check()?;
            self.canonicalize_existing_mutation(
                operation, &pending.0, operations_allocation, accounting, cancellation,
            ).await?;
            self.canonicalize_new_mutation_parents(
                operation, &pending.0, operations_allocation, accounting, cancellation,
            ).await?;
            match operation {
                Mutation::Create { path, record } | Mutation::Restore { path, record } => {
                    let allowed = removed_binding_id(path, &removed.0, accounting)?;
                    let allowed_ids = allowed.as_ref().map_or(&[][..], std::slice::from_ref);
                    self.admit_binding_name(path, &pending.0, allowed_ids, accounting, cancellation).await?;
                    Self::push_name_binding(&mut pending, path, record.file_id, accounting)?;
                }
                Mutation::Link { source, destination } => {
                    let id = self.pending_or_existing_file_id(source, &pending.0, accounting, cancellation).await?;
                    let allowed = removed_binding_id(destination, &removed.0, accounting)?;
                    let allowed_ids = allowed.as_ref().map_or(&[][..], std::slice::from_ref);
                    self.admit_binding_name(destination, &pending.0, allowed_ids, accounting, cancellation).await?;
                    Self::push_name_binding(&mut pending, destination, id, accounting)?;
                }
                Mutation::Rename { source, destination, replace } => {
                    let id = self.pending_or_existing_file_id(source, &pending.0, accounting, cancellation).await?;
                    let removed_id = removed_binding_id(destination, &removed.0, accounting)?.filter(|other| *other != id);
                    let ids = [id, removed_id.unwrap_or(id)];
                    let allowed_ids = if removed_id.is_some() { &ids[..] } else { &ids[..1] };
                    self.admit_binding_name(destination, &pending.0, allowed_ids, accounting, cancellation).await?;
                    Self::retain_name_bindings(&mut pending, source, accounting)?;
                    if *replace {
                        Self::retain_name_bindings(&mut pending, destination, accounting)?;
                    }
                    Self::push_name_binding(&mut removed, source, id, accounting)?;
                    Self::push_name_binding(&mut pending, destination, id, accounting)?;
                }
                Mutation::Remove { path, .. } => {
                    if pending_binding_path(path.components(), &pending.0, self.volume.config.case_sensitivity, accounting)?.is_none() {
                        let id = self.pending_or_existing_file_id(path, &pending.0, accounting, cancellation).await?;
                        Self::push_name_binding(&mut removed, path, id, accounting)?;
                    }
                    Self::retain_name_bindings(&mut pending, path, accounting)?;
                }
                Mutation::SetMetadata { .. } | Mutation::Write { .. }
                | Mutation::ValidateRegular { .. } | Mutation::Resize { .. }
                | Mutation::ZeroRange { .. } | Mutation::Preallocate { .. }
                | Mutation::CloneRange { .. } | Mutation::File { .. }
                | Mutation::CloneFileRange { .. } => {}
            }
        }
        Ok(())
    }

    fn name_binding_vector(
        length: usize,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    ) -> Result<(Vec<(NamespacePath, FileId)>, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
        let requested = length.checked_mul(size_of::<(NamespacePath, FileId)>())
            .ok_or(WorkError::Overflow)? as u64;
        let mut allocation = accounting.claim_owned(requested, u64::from(length != 0))?;
        let mut values = Vec::new();
        values.try_reserve_exact(length).map_err(|_| FsError::PendingMutationAllocationFailed)?;
        let actual = values.capacity().checked_mul(size_of::<(NamespacePath, FileId)>())
            .ok_or(WorkError::Overflow)? as u64;
        accounting.reconcile_owned(&mut allocation, requested, actual)?;
        Ok((values, allocation))
    }

    fn push_name_binding(
        bindings: &mut (Vec<(NamespacePath, FileId)>, crate::kernel::allocation::KernelOwnedAllocation),
        path: &NamespacePath,
        id: FileId,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        let cancellation = name_cancellation(accounting);
        let mut copied = path.clone_with_accounting(accounting, cancellation)?;
        bindings.1.absorb(&mut copied.1)?;
        bindings.0.push((copied.0, id));
        Ok(())
    }

    fn retain_name_bindings(
        bindings: &mut (Vec<(NamespacePath, FileId)>, crate::kernel::allocation::KernelOwnedAllocation),
        removed: &NamespacePath,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        let bytes = bindings.0.iter().filter(|(path, _)| path == removed)
            .try_fold(0_u64, |total, (path, _)| total.checked_add(path.retained_bytes()).ok_or(WorkError::Overflow))?;
        bindings.0.retain(|(path, _)| path != removed);
        accounting.release_owned(&mut bindings.1, bytes)
    }

    fn replace_name_path(
        path: &mut NamespacePath,
        mut replacement: (NamespacePath, crate::kernel::allocation::KernelOwnedAllocation),
        operations_allocation: &mut KernelOwnedAllocation,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        let old_bytes = path.retained_bytes();
        operations_allocation.absorb(&mut replacement.1)?;
        drop(std::mem::replace(path, replacement.0));
        accounting.release_owned(operations_allocation, old_bytes)?;
        accounting.check()
    }

    async fn admit_binding_name(
        &mut self, path: &NamespacePath, pending: &[(NamespacePath, FileId)],
        allowed: &[FileId], accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let parent_pending = match path.split_last() {
            Some((parent, _)) => pending_binding_path(parent, pending,
                self.volume.config.case_sensitivity, accounting)?.is_some(),
            None => false,
        };
        self.admit_new_name_with_accounting(path, accounting, cancellation, allowed, parent_pending).await?;
        if self.volume.config.case_sensitivity == CaseSensitivity::ProfileFolded
            && let Some((parent, name)) = path.split_last()
        {
            for (other, id) in pending {
                if let Some((other_parent, other_name)) = other.split_last()
                    && parent == other_parent && name != other_name && !allowed.contains(id)
                    && name_fold_equal(name, other_name, accounting)?
                {
                    return Err(FsError::NameCollision);
                }
            }
        }
        Ok(())
    }

    async fn canonicalize_new_mutation_parents(
        &mut self,
        operation: &mut Mutation,
        pending: &[(NamespacePath, FileId)],
        operations_allocation: &mut KernelOwnedAllocation,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        if self.volume.config.case_sensitivity != CaseSensitivity::ProfileFolded { return Ok(()); }
        let (Mutation::Create { path, .. } | Mutation::Link { destination: path, .. }
            | Mutation::Rename { destination: path, .. }) = operation else { return Ok(()); };
        let Some((parent, name)) = path.split_last() else { return Ok(()); };
        if parent.is_empty() { return Ok(()); }
        let Some(resolved) = self.canonicalize_path_components(parent, pending, accounting, cancellation).await?
            else { return Ok(()); };
        let replacement = resolved.0.child_with_accounting(
            name, self.volume.config.limits, accounting, cancellation,
        )?;
        let bytes = resolved.0.retained_bytes();
        retire_name_owned(resolved, bytes, accounting)?;
        Self::replace_name_path(path, replacement, operations_allocation, accounting)
    }

    async fn canonicalize_existing_mutation(
        &mut self,
        operation: &mut Mutation,
        pending: &[(NamespacePath, FileId)],
        operations_allocation: &mut KernelOwnedAllocation,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        if self.volume.config.case_sensitivity != CaseSensitivity::ProfileFolded { return Ok(()); }
        match operation {
            Mutation::Remove { path, .. } | Mutation::SetMetadata { path, .. }
            | Mutation::Write { path, .. } | Mutation::ValidateRegular { path }
            | Mutation::Resize { path, .. } | Mutation::ZeroRange { path, .. }
            | Mutation::Preallocate { path, .. } => {
                let replacement = self.canonical_existing_path(path, pending, accounting, cancellation).await?;
                Self::replace_name_path(path, replacement, operations_allocation, accounting)?;
            }
            Mutation::Rename { source, .. } | Mutation::Link { source, .. } => {
                let replacement = self.canonical_existing_path(source, pending, accounting, cancellation).await?;
                Self::replace_name_path(source, replacement, operations_allocation, accounting)?;
            }
            Mutation::CloneRange { source, destination, .. } => {
                let replacement = self.canonical_existing_path(source, pending, accounting, cancellation).await?;
                Self::replace_name_path(source, replacement, operations_allocation, accounting)?;
                let replacement = self.canonical_existing_path(destination, pending, accounting, cancellation).await?;
                Self::replace_name_path(destination, replacement, operations_allocation, accounting)?;
            }
            Mutation::Create { .. } | Mutation::Restore { .. } | Mutation::File { .. }
            | Mutation::CloneFileRange { .. } => {}
        }
        Ok(())
    }

    async fn canonical_existing_path(
        &mut self,
        path: &NamespacePath,
        pending: &[(NamespacePath, FileId)],
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(NamespacePath, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
        if let Some((exact, _)) = pending_binding_path(path.components(), pending,
            self.volume.config.case_sensitivity, accounting)?
        {
            let token = name_cancellation(accounting);
            return exact.clone_with_accounting(accounting, token);
        }
        let exact = self.lookup_no_follow_at_current_with_accounting(path, accounting, cancellation, false).await?;
        if exact.0.record.is_some() {
            let token = name_cancellation(accounting);
            return path.clone_with_accounting(accounting, token);
        }
        if path.is_root() || self.volume.config.case_sensitivity != CaseSensitivity::ProfileFolded {
            return Err(FsError::NotFound);
        }
        self.canonicalize_path_components(path.components(), pending, accounting, cancellation).await?
            .ok_or(FsError::NotFound)
    }

    async fn canonicalize_path_components(
        &mut self,
        requested: &[LogicalName],
        pending: &[(NamespacePath, FileId)],
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<(NamespacePath, crate::kernel::allocation::KernelOwnedAllocation)>, FsError> {
        let allocation = accounting.claim_owned(0, 0)?;
        let mut canonical = (
            NamespacePath::new(Vec::new(), self.volume.config.limits).map_err(name_path_error)?,
            allocation,
        );
        for name in requested {
            accounting.check()?;
            let candidate = canonical.0.child_with_accounting(
                name, self.volume.config.limits, accounting, cancellation,
            )?;
            if let Some((exact, _)) = pending_binding_path(candidate.0.components(), pending,
                self.volume.config.case_sensitivity, accounting)?
            {
                let token = name_cancellation(accounting);
                let next = exact.clone_with_accounting(accounting, token)?;
                let bytes = canonical.0.retained_bytes();
                retire_name_owned(std::mem::replace(&mut canonical, next), bytes, accounting)?;
                let bytes = candidate.0.retained_bytes();
                retire_name_owned(candidate, bytes, accounting)?;
                continue;
            }
            let probe = self.lookup_no_follow_at_current_with_accounting(
                &candidate.0, accounting, cancellation, false,
            ).await?;
            if probe.0.record.is_some() {
                let bytes = canonical.0.retained_bytes();
                retire_name_owned(std::mem::replace(&mut canonical, candidate), bytes, accounting)?;
                continue;
            }
            let Some(exact_name) = self.find_case_folded_sibling_with_accounting(
                canonical.0.components(), name, accounting, cancellation, &[],
            ).await? else {
                let bytes = candidate.0.retained_bytes();
                retire_name_owned(candidate, bytes, accounting)?;
                let bytes = canonical.0.retained_bytes();
                retire_name_owned(canonical, bytes, accounting)?;
                return Ok(None);
            };
            let next = canonical.0.child_with_accounting(
                &exact_name.0, self.volume.config.limits, accounting, cancellation,
            )?;
            let bytes = canonical.0.retained_bytes();
            retire_name_owned(std::mem::replace(&mut canonical, next), bytes, accounting)?;
            let bytes = candidate.0.retained_bytes();
            retire_name_owned(candidate, bytes, accounting)?;
            let bytes = exact_name.0.retained_bytes() as u64;
            retire_name_owned(exact_name, bytes, accounting)?;
        }
        Ok(Some(canonical))
    }

    async fn pending_or_existing_file_id(
        &mut self,
        path: &NamespacePath,
        pending: &[(NamespacePath, FileId)],
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<FileId, FsError> {
        if let Some((_, id)) = pending_binding_path(path.components(), pending,
            self.volume.config.case_sensitivity, accounting)? {
            return Ok(id);
        }
        let lookup = self.lookup_no_follow_with_accounting(path, accounting, cancellation).await?;
        Ok(lookup.0.record.ok_or(FsError::NotFound)?.file_id)
    }

    /// Compiles and atomically applies one complete caller-authored transaction.
    pub async fn apply_authored_transaction(
        &mut self,
        authored: Vec<AuthoredMutation>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<AuthoredTransactionResult> {
        crate::heap_future::in_heap_measured(move |budget| async move {
                let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
                let result = async {
                    let input = Self::admit_authored_input(authored, &mut accounting)?;
                    let output = self.apply_authored_transaction_with_accounting(input, &mut accounting, cancellation).await?;
                    let crate::kernel::Owned { value, allocation, bytes } = output;
                    crate::kernel::retire_allocation(allocation, bytes, &mut accounting)?;
                    Ok(value)
                }.await;
                measured(result, &accounting)
            }, budget, cancellation, FsError::Cancelled(CancellationError), None).await
    }

    /// Applies a large ordered capture without exposing an intermediate candidate.
    pub(crate) async fn apply_authored_bulk_transaction(
        &mut self,
        authored: Vec<AuthoredMutation>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let input = Self::admit_authored_input(authored, &mut accounting)?;
            self.apply_authored_bulk_transaction_with_accounting(input, &mut accounting, cancellation).await
        }.await;
        measured(result, &accounting)
    }

    fn admit_authored_input(
        authored: Vec<AuthoredMutation>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    ) -> Result<crate::kernel::Owned<Vec<AuthoredMutation>>, FsError> {
        let bytes = authored_input_owned_bytes(&authored, authored.capacity())?;
        let allocation = accounting.claim_owned(bytes, 0)?;
        Ok(crate::kernel::Owned::new(authored, allocation, bytes))
    }

    /// Borrows producer-owned authored paths and opaque payloads on the original
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn apply_borrowed_authored_transaction_with_native_work(
        &mut self,
        authored: &[AuthoredMutation],
        slots_allocation: &crate::NativeAllocation,
        original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(AuthoredTransactionResult, crate::NativeAllocation), FsError> {
        let input = (authored, slots_allocation);
        self.validate_native_work(original)?;
        let slots = input.0.len().checked_mul(size_of::<AuthoredMutation>())
            .map(crate::foundation::usize_to_u64).ok_or(WorkError::Overflow)?;
        if input.1.bytes() < slots || !input.1.work().shares_ledger(original) {
            return Err(FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout));
        }
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(original, cancellation);
        let output = self.apply_authored_transaction_input_with_accounting(
            AuthoredTransactionInput::Borrowed(input.0), &mut accounting, cancellation,
        ).await?;
        let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) = output.allocation else {
            unreachable!("native authored result retains original custody")
        };
        Ok((output.value, allocation))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn validate_authored_input(
        &self, input: &crate::kernel::Owned<Vec<AuthoredMutation>>, accounting: &KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        if let Some(original) = accounting.native_work() {
            self.validate_native_work(original)?;
            let bytes = authored_input_owned_bytes(&input.value, input.value.capacity())?;
            let KernelOwnedAllocation::NativeOriginal(guard) = &input.allocation else {
                return Err(FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout));
            };
            if guard.bytes() < bytes || !guard.work().shares_ledger(original) {
                return Err(FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout));
            }
        }
        Ok(())
    }

    pub(crate) async fn apply_authored_transaction_with_accounting(
        &mut self,
        input: crate::kernel::Owned<Vec<AuthoredMutation>>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::kernel::Owned<AuthoredTransactionResult>, FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        self.validate_authored_input(&input, accounting)?;
        let capacity = input.value.capacity();
        self.apply_authored_transaction_input_with_accounting(
            AuthoredTransactionInput::Owned {
                mutations: input.value.into_iter(), allocation: input.allocation, capacity,
            },
            accounting, cancellation,
        ).await
    }

    async fn apply_authored_transaction_input_with_accounting(
        &mut self,
        mut input: AuthoredTransactionInput<'_>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::kernel::Owned<AuthoredTransactionResult>, FsError> {
        use crate::kernel::{Owned, split_allocation};
        accounting.check()?;
        let maximum = usize::try_from(self.volume.config.limits.maximum_mutations_per_batch).unwrap_or(usize::MAX);
        if input.mutations().len() > maximum || input.capacity() > maximum {
            return Err(FsError::TooManyPendingMutations);
        }
        let count = input.mutations().iter().try_fold(0_usize, |count, mutation| {
            count.checked_add(authored_mutation_operation_count(mutation))
        }).ok_or(FsError::TooManyPendingMutations)?;
        if count > maximum { return Err(FsError::TooManyPendingMutations); }
        let mut operations = Owned::<Vec<Mutation>>::allocate(count, accounting)?;
        let length = input.mutations().len();
        let mut created = Owned::<Vec<Option<FileId>>>::allocate(length, accounting)?;
        let mut metadata = AuthoredMetadataCache::new();
        for index in 0..length {
            let authored = match &mut input {
                AuthoredTransactionInput::Owned { mutations, allocation, .. } => {
                    let mutation = mutations.next().expect("preflight retained every authored mutation");
                    let bytes = mutation.logical_owned_bytes();
                    let allocation = split_allocation(allocation, bytes)?;
                    Owned::new(mutation, allocation, bytes)
                }
                AuthoredTransactionInput::Borrowed(mutations) => {
                    accounting.check()?;
                    clone_borrowed_authored_descriptor(&mutations[index], accounting)?
                }
            };
            let file_id = self.compile_authored_mutation_with_accounting(
                authored, &mut operations, &mut metadata, accounting, cancellation,
            ).await?;
            accounting.charge(WorkCounters {
                bytes_copied: crate::foundation::usize_to_u64(size_of::<Option<FileId>>()),
                ..WorkCounters::UNCHARGED
            })?;
            created.value.push(file_id);
        }
        input.retire(accounting)?;
        if operations.value.is_empty() {
            operations.retire(accounting)?;
        } else {
            if self.mode.access != AccessMode::ReadWrite
                || !matches!(self.mode.mutations, MutationMode::PrivateOverlay | MutationMode::DirectLive)
            { return Err(FsError::MutationNotAllowed); }
            self.mutate_owned_with_accounting(operations, accounting, cancellation).await?;
        }
        Ok(Owned::new(AuthoredTransactionResult { created_file_ids: created.value }, created.allocation, created.bytes))
    }

    pub(crate) async fn apply_authored_bulk_transaction_with_accounting(
        &mut self,
        input: crate::kernel::Owned<Vec<AuthoredMutation>>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        use crate::kernel::{Owned, retire_allocation, split_allocation};
        use crate::kernel::allocation::KernelOwnedAllocation;
        #[cfg(not(target_arch = "wasm32"))]
        self.validate_authored_input(&input, accounting)?;
        accounting.check()?;
        let maximum = usize::try_from(self.volume.config.limits.maximum_mutations_per_batch).unwrap_or(usize::MAX);
        if maximum == 0 { return Err(FsError::TooManyPendingMutations); }
        let mut candidate = self.private_candidate_with_accounting(accounting, cancellation)?;
        let backing = crate::foundation::usize_to_u64(input.value.capacity() * size_of::<AuthoredMutation>());
        let mut input = (input.value.into_iter(), input.allocation);
        let mut batch = Owned::<Vec<AuthoredMutation>>::allocate(0, accounting)?;
        let mut count = 0_usize;
        while let Some(mutation) = input.0.next() {
            let next = authored_mutation_operation_count(&mutation);
            if next > maximum { return Err(FsError::TooManyPendingMutations); }
            if !batch.value.is_empty() && (batch.value.len() == maximum || next > maximum - count) {
                let output = candidate.apply_authored_transaction_with_accounting(batch, accounting, cancellation).await?;
                output.retire(accounting)?;
                batch = Owned::<Vec<AuthoredMutation>>::allocate(0, accounting)?;
                count = 0;
            }
            batch.ensure_for_push(maximum, accounting)?;
            let bytes = mutation.logical_owned_bytes();
            let allocation = split_allocation(&mut input.1, bytes)?;
            let mut mutation = Owned::new(mutation, allocation, bytes);
            accounting.charge(WorkCounters {
                bytes_copied: crate::foundation::usize_to_u64(size_of::<AuthoredMutation>()),
                ..WorkCounters::UNCHARGED
            })?;
            batch.value.push(mutation.value);
            match (&mut batch.allocation, &mut mutation.allocation) {
                (KernelOwnedAllocation::Standalone, KernelOwnedAllocation::Standalone) => {}
                #[cfg(not(target_arch = "wasm32"))]
                (KernelOwnedAllocation::NativeOriginal(target), KernelOwnedAllocation::NativeOriginal(source)) => target.absorb(source)?,
                #[cfg(not(target_arch = "wasm32"))]
                _ => unreachable!("authored batches preserve one ledger"),
            }
            batch.bytes = batch.bytes.checked_add(bytes).ok_or(WorkError::Overflow)?;
            count += next;
        }
        drop(input.0);
        if batch.value.is_empty() {
            batch.retire(accounting)?;
        } else {
            candidate.apply_authored_transaction_with_accounting(batch, accounting, cancellation).await?.retire(accounting)?;
        }
        let backing = match &input.1 {
            KernelOwnedAllocation::Standalone => backing,
            #[cfg(not(target_arch = "wasm32"))]
            KernelOwnedAllocation::NativeOriginal(guard) => guard.bytes(),
        };
        retire_allocation(input.1, backing, accounting)?;
        *self = candidate;
        Ok(())
    }

    pub(crate) fn private_candidate<W: FsWork>(
        &self, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<Self, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = self.private_candidate_with_accounting(&mut accounting, cancellation);
        budget.finish(result, &accounting)
    }

    fn checkout_custody(
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<Arc<ProofDataCustody>, FsError> {
        cancellation.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let mut header = original.claim_root_metadata(std::mem::size_of::<ProofDataCustody>() as u64)?;
            let custody = Arc::new(ProofDataCustody::default());
            cancellation.check()?;
            original.register_native_checkout_control(&custody, &mut header)?;
            return Ok(custody);
        }
        let (allocations, work, budget, _) = accounting.standalone_parts()
            .ok_or(FsError::HandleAllocationFailed)?;
        allocations.claim_elements::<ProofDataCustody>(1, work, budget)
            .map_err(crate::collection::handle_allocation_error)?;
        Ok(Arc::new(ProofDataCustody::default()))
    }

    fn private_candidate_with_accounting(
        &self,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Self, FsError> {
        let custody = Self::checkout_custody(accounting, cancellation)?;
        let copied = WorkCounters {
            bytes_copied: crate::foundation::usize_to_u64(size_of::<Proof>()),
            ..WorkCounters::UNCHARGED
        };
        accounting.admit(copied)?;
        let proof_bytes = crate::foundation::usize_to_u64(size_of::<Mutex<Proof>>());
        // This guard is declared before the physical proof so every error
        let mut proof_allocation = accounting.claim_owned(proof_bytes, 1)?;
        // Immutable proof maps remain shared by the canonical Arc COW engine.
        let mut dependencies = self.dependencies.independent_with_custody(custody);
        accounting.record_completed(copied)?;
        dependencies.proof().control = match accounting {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { .. } => {
                let original = accounting.native_work()
                    .ok_or(FsError::HandleAllocationFailed)?;
                let scope = original.checkout_control_sink(&dependencies.data_custody)?;
                match &mut proof_allocation {
                    crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) =>
                        allocation.transfer_checkout_control(&dependencies.data_custody, &scope)?,
                    crate::kernel::allocation::KernelOwnedAllocation::Standalone =>
                        return Err(FsError::HandleAllocationFailed),
                }
            }
            _ => crate::kernel::ProofDataClaim::control(
                &dependencies.data_custody, proof_bytes,
            ),
        };
        let (base_root, base_control) = match accounting {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { .. } => crate::collection::clone_generation_root_native(
                &self.base_root, accounting, &dependencies.data_custody,
            )?,
            _ => {
                let (base_root, _) = crate::collection::clone_generation_root_with_accounting(
                    &self.base_root, accounting,
                )?;
                let control = crate::kernel::ProofDataClaim::control(
                    &dependencies.data_custody, root_parent_bytes(&base_root)?,
                );
                (base_root, control)
            }
        };
        dependencies.base_root_control = base_control;
        let root = match accounting {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { .. } => self.root.replicate_native_measured(
                accounting, &dependencies.data_custody,
            )?,
            _ => {
                let mut root = self.root.replicate_with_accounting(accounting, None)?;
                root.attach_control_custody(&dependencies.data_custody);
                root
            }
        };
        Ok(self.replica_with_roots(dependencies, self.mode, base_root, root))
    }

    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        self.proof_owned_bytes
            + crate::foundation::usize_to_u64(self.base_root.parents.capacity() * size_of::<GenerationId>())
            + self.root.logical_owned_bytes()
    }

    pub(crate) fn private_control_custody(&self) -> Arc<crate::kernel::ProofDataCustody> {
        Arc::clone(&self.dependencies.data_custody)
    }

    fn root_control_claim(
        root: &GenerationRoot,
        producer: Option<&Arc<crate::kernel::ProofDataCustody>>,
    ) -> Result<crate::kernel::ProofDataClaim, FsError> {
        match producer {
            Some(producer) => Ok(crate::kernel::ProofDataClaim::pending_control(producer, root_parent_bytes(root)?)),
            None => Ok(crate::kernel::ProofDataClaim::default()),
        }
    }

    fn prepare_root_replacement(
        &self, base: &GenerationRoot, working: &GenerationRoot,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
        producer: Option<&Arc<crate::kernel::ProofDataCustody>>,
    ) -> Result<PreparedCheckoutRoots, FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let producer = Arc::clone(producer.unwrap_or(&self.dependencies.data_custody));
            cancellation.check()?;
            original.cancellation().check()?;
            original.checkout_control_sink(&producer)?;
            let base = crate::collection::clone_generation_root_native(base, accounting, &producer)?;
            cancellation.check()?;
            let held = self.root.prepare_held_root_native(working, accounting, &producer)?;
            return Ok(PreparedCheckoutRoots { base, held, producer: Some(producer), working_control: None });
        }
        let producer = producer.or_else(|| {
            (self.dependencies.data_custody.physical_retained_bytes() != 0)
                .then_some(&self.dependencies.data_custody)
        }).map(Arc::clone);
        cancellation.check()?;
        let (allocations, work, bound, _) = accounting.standalone_parts()
            .ok_or(FsError::HandleAllocationFailed)?;
        let base = crate::collection::clone_generation_root(base, allocations, work, bound, cancellation)?;
        let control = Self::root_control_claim(&base, producer.as_ref())?;
        let base = (base, control);
        cancellation.check()?;
        let held = self.root.prepare_held_root(working, allocations, work, bound, cancellation)?
            .map(|root| {
                let control = Self::root_control_claim(&root, producer.as_ref())?;
                Ok::<_, FsError>((root, control))
            }).transpose()?;
        Ok(PreparedCheckoutRoots { base, held, producer, working_control: None })
    }

    /// Transfers only the authenticated working root's already-paid parent
    #[cfg(not(target_arch = "wasm32"))]
    fn prepare_root_working_control_with_native_work(
        &self,
        working: &GenerationRoot,
        guard: &mut crate::materializer::NativeAllocation,
        prepared: &mut PreparedCheckoutRoots,
        original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        cancellation.check()?;
        let bytes = root_parent_bytes(working)?;
        let producer = prepared.producer.as_ref().ok_or(FsError::HandleAllocationFailed)?;
        // Keep the paid guard outside all accounting leaves on failure.
        original.cancellation().check()?;
        let mut parents = guard.split_off(bytes)?;
        let result = parents.transfer_checkout_control_original(producer).map_err(FsError::from);
        match result {
            Ok(control) => { prepared.working_control = Some(control); Ok(()) }
            Err(error) => { guard.absorb(&mut parents)?; Err(error) }
        }
    }

    fn install_root_replacement(
        &mut self,
        generation: ObjectId,
        working: GenerationRoot,
        prepared: PreparedCheckoutRoots,
    ) {
        let control = prepared.working_control.unwrap_or_else(|| {
            Self::root_control_claim(&working, prepared.producer.as_ref())
                .expect("an admitted generation-root vector has a representable capacity")
        });
        self.root.set_prepared_root(generation, working, control, prepared.held);
        self.base_root = prepared.base.0;
        self.dependencies.base_root_control = prepared.base.1;
    }

    pub(crate) fn proof_data_bytes(
        &self, accounting: &mut KernelAccounting<'_>,
    ) -> Result<u64, FsError> {
        accounting.charge(WorkCounters { items_examined: 2, ..WorkCounters::UNCHARGED })?;
        let proof = self.dependencies.proof();
        proof.dependencies.logical_data().2.checked_add(proof.observed_data_bytes)
            .ok_or(FsError::Work(WorkError::Overflow))
    }

    #[cfg(feature = "native-mount")]
    pub(crate) fn into_unpublishable_capture_candidate(mut self) -> Self {
        self.mode = CheckoutMode {
            access: AccessMode::ReadWrite,
            consistency: ConsistencyMode::Pinned,
            mutations: MutationMode::PrivateOverlay,
        };
        self.authority_head = None;
        self
    }

    /// Constructs an observing alias with independently admitted roots.
    #[cfg(feature = "native-mount")]
    pub(crate) fn observer<W: FsWork>(
        &self, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<Self, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = (|| -> Result<Self, FsError> {
            let accounting = &mut accounting;
            cancellation.check()?;
            if !self.tracks_observations() {
                let mut checkout = self.private_candidate_with_accounting(accounting, cancellation)?;
                checkout.mode = CheckoutMode::read_only_pinned();
                return Ok(checkout);
            }
            let custody = Self::checkout_custody(accounting, cancellation)?;
            accounting.check()?;
            let mut dependencies = self.dependencies.observing_with_custody(custody);
            let base = match accounting {
                #[cfg(not(target_arch = "wasm32"))]
                KernelAccounting::NativeOriginal { .. } => crate::collection::clone_generation_root_native(
                    &self.base_root, accounting, &dependencies.data_custody,
                )?,
                _ => {
                    let root = crate::collection::clone_generation_root_with_accounting(&self.base_root, accounting)?;
                    if !matches!(root.1, crate::kernel::allocation::KernelOwnedAllocation::Standalone) {
                        return Err(FsError::HandleAllocationFailed);
                    }
                    let control = crate::kernel::ProofDataClaim::control(
                        &dependencies.data_custody, root_parent_bytes(&root.0)?,
                    );
                    (root.0, control)
                }
            };
            let root = match accounting {
                #[cfg(not(target_arch = "wasm32"))]
                KernelAccounting::NativeOriginal { .. } =>
                    self.root.replicate_native_measured(accounting, &dependencies.data_custody)?,
                _ => {
                    let mut root = self.root.replicate_with_accounting(accounting, None)?;
                    root.attach_control_custody(&dependencies.data_custody);
                    root
                }
            };
            dependencies.base_root_control = base.1;
            Ok(self.replica_with_roots(
                dependencies, CheckoutMode::read_only_pinned(), base.0, root,
            ))
        })();
        budget.finish(result, &accounting)
    }

    /// Diffs two candidates under the supplied work limits.
    #[cfg(feature = "native-mount")]
    pub(crate) async fn candidate_diff<W: FsWork>(
        &self, candidate: &Self, maximum_changes: u32, work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(GenerationDiff, KernelOwnedAllocation), FsError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            if candidate.volume.id != self.volume.id { return Err(FsError::VolumeMismatch); }
            let output = diff_generation_file_tables_with_accounting(
                &self.volume.fs.inner.objects, self.root.file_table, candidate.root.file_table,
                maximum_changes, self.volume.config, &mut accounting, cancellation,
            ).await?;
            #[cfg(not(target_arch = "wasm32"))]
            if accounting.native_work().is_some() && matches!(output.1, KernelOwnedAllocation::Standalone) {
                return Err(FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout));
            }
            Ok(output)
        }.await;
        work.finish(result, &accounting)
    }

    /// Constructs an isolated read-only candidate.
    #[cfg(feature = "native-mount")]
    pub(crate) fn inspector<W: FsWork>(
        &self, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<Self, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = self.private_candidate_with_accounting(&mut accounting, cancellation).map(|mut checkout| {
            checkout.mode = CheckoutMode::read_only_pinned();
            checkout
        });
        budget.finish(result, &accounting)
    }

    fn replica_with_roots(
        &self,
        dependencies: DependencyLedger,
        mode: CheckoutMode,
        base_root: GenerationRoot,
        root: crate::collection::CheckoutRoots,
    ) -> Self {
        Self {
            volume: self.volume.clone(),
            generation_root: self.generation_root,
            base_file_table: self.base_file_table,
            base_root,
            root,
            authority_head: self.authority_head,
            authored_operation_id: self.authored_operation_id,
            live_operation_id: self.live_operation_id,
            last_commit: self.last_commit,
            publication_closure: None,
            prepared_merge_parent: self.prepared_merge_parent,
            proof_owned_bytes: dependencies.data_custody.logical_owned_bytes()
                + if dependencies.observing { 0 } else { crate::foundation::usize_to_u64(size_of::<Mutex<Proof>>()) },
            dependencies,
            mode,
        }
    }

    /// Lowers every authored kind on the caller's existing allocation context.
    async fn compile_authored_mutation_with_accounting(
        &self,
        authored: crate::kernel::Owned<AuthoredMutation>,
        operations: &mut crate::kernel::Owned<Vec<Mutation>>,
        staged_metadata: &mut AuthoredMetadataCache,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<FileId>, FsError> {
        let crate::kernel::Owned { value, mut allocation, mut bytes } = authored;
        let path_bytes = value.logical_owned_bytes();
        let result = async {
            accounting.check()?;
            match value {
                AuthoredMutation::Reidentify { path, file_id } => {
                    let current = crate::kernel::lookup_path_async_with_accounting(
                        &self.volume.fs.inner.objects, &self.root, &path,
                        self.volume.config, accounting, cancellation, None,
                    ).await?.record.ok_or(FsError::NotFound)?;
                    if current.kind == FileKind::Directory {
                        return Err(FsError::InvalidSpecialKind);
                    }
                    if current.file_id == file_id {
                        return Ok(None);
                    }
                    let (existing, existing_allocation) =
                        crate::kernel::lookup_file_record_async_with_accounting(
                            &self.volume.fs.inner.objects, self.root.file_table, file_id,
                            DecodeLimits::for_volume(self.volume.config), accounting,
                            cancellation, Some(&self.dependencies.data_custody),
                        ).await?;
                    drop(existing_allocation);
                    if existing.is_some_and(|existing| {
                        existing.kind != current.kind
                            || existing.metadata != current.metadata
                            || existing.payload != current.payload
                    }) {
                        return Err(FsError::FileIdentityCollision);
                    }
                    push_authored_operation(operations, &mut allocation, &mut bytes, accounting,
                        Mutation::Restore { path, record: FileRecord {
                            file_id, link_count: 1, ..current
                        } })?;
                    Ok(None)
                }
                AuthoredMutation::CreateFile { path, bytes: content, metadata } => {
                    let file_id = self.authored_file_id_with_accounting(
                        FileKind::Regular, &path, accounting,
                    )?;
                    self.compile_create_file_with_accounting(
                        path, content, metadata, file_id, operations,
                        &mut allocation, &mut bytes, staged_metadata, accounting, cancellation,
                    ).await.map(Some)
                }
                AuthoredMutation::CreateFileFromContent { path, content, metadata, file_id } => {
                    let metadata = self.stage_authored_metadata_with_accounting(
                        metadata, staged_metadata, accounting, cancellation,
                    ).await?;
                    let file_id = match file_id {
                        Some(file_id) => file_id,
                        None => self.authored_file_id_with_accounting(
                            FileKind::Regular, &path, accounting,
                        )?,
                    };
                    self.compile_regular_content_with_accounting(
                        path, content, metadata, file_id, operations,
                        &mut allocation, &mut bytes, accounting,
                    )?;
                    Ok(Some(file_id))
                }
                AuthoredMutation::CreateDirectory { path, metadata } => {
                    let entries = self.stage_empty_tree_with_accounting(accounting, cancellation).await?;
                    self.compile_record_with_accounting(
                        operations, path, FileKind::Directory, FilePayload::Directory { entries },
                        metadata, &mut allocation, &mut bytes, staged_metadata, accounting,
                        cancellation,
                    ).await.map(Some)
                }
                AuthoredMutation::CreateSymbolicLink { path, target, metadata } => {
                    let content = self.stage_authored_blob_with_accounting(
                        target, accounting, cancellation,
                    ).await?;
                    self.compile_record_with_accounting(
                        operations, path, FileKind::SymbolicLink, FilePayload::SymbolicLink {
                            target_bytes: content.logical_bytes, target: content.root,
                        }, metadata, &mut allocation, &mut bytes, staged_metadata,
                        accounting, cancellation,
                    ).await.map(Some)
                }
                AuthoredMutation::CreateEmptySpecial { path, kind, metadata } => {
                    if !matches!(kind, FileKind::Fifo | FileKind::Socket | FileKind::MountBoundary) {
                        return Err(FsError::InvalidSpecialKind);
                    }
                    if !kind.is_supported_by_profile(self.volume.config.profile) {
                        return Err(FsError::UnsupportedFileKind);
                    }
                    self.compile_record_with_accounting(
                        operations, path, kind, FilePayload::Empty, metadata,
                        &mut allocation, &mut bytes, staged_metadata, accounting, cancellation,
                    ).await.map(Some)
                }
                AuthoredMutation::CreateDevice { path, kind, major, minor, metadata } => {
                    if !matches!(kind, FileKind::CharacterDevice | FileKind::BlockDevice) {
                        return Err(FsError::InvalidSpecialKind);
                    }
                    if !kind.is_supported_by_profile(self.volume.config.profile) {
                        return Err(FsError::UnsupportedFileKind);
                    }
                    self.compile_record_with_accounting(
                        operations, path, kind, FilePayload::Device { major, minor }, metadata,
                        &mut allocation, &mut bytes, staged_metadata, accounting, cancellation,
                    ).await.map(Some)
                }
                AuthoredMutation::CreateReparsePoint { path, payload, metadata } => {
                    if !FileKind::ReparsePoint.is_supported_by_profile(self.volume.config.profile) {
                        return Err(FsError::UnsupportedFileKind);
                    }
                    let content = self.stage_authored_blob_with_accounting(
                        payload, accounting, cancellation,
                    ).await?;
                    self.compile_record_with_accounting(
                        operations, path, FileKind::ReparsePoint, FilePayload::ReparsePoint {
                            payload_bytes: content.logical_bytes, payload: content.root,
                        }, metadata, &mut allocation, &mut bytes, staged_metadata,
                        accounting, cancellation,
                    ).await.map(Some)
                }
                AuthoredMutation::Remove { path, expected_file_id } => {
                    push_authored_operation(operations, &mut allocation, &mut bytes, accounting,
                        Mutation::Remove { path, expected_file_id: expected_file_id
                            .map_or(MetadataField::Unavailable, MetadataField::Value) })?;
                    Ok(None)
                }
                AuthoredMutation::Rename { source, destination, replace } => {
                    push_authored_operation(operations, &mut allocation, &mut bytes, accounting,
                        Mutation::Rename { source, destination, replace })?;
                    Ok(None)
                }
                AuthoredMutation::HardLink { source, destination } => {
                    push_authored_operation(operations, &mut allocation, &mut bytes, accounting,
                        Mutation::Link { source, destination })?;
                    Ok(None)
                }
                AuthoredMutation::Write { path, offset, bytes: content } => {
                    let mutation = if content.is_empty() {
                        drop(content);
                        Mutation::ValidateRegular { path }
                    } else {
                        let content = self.stage_authored_blob_with_accounting(
                            content, accounting, cancellation,
                        ).await?;
                        Mutation::Write { path, offset, length: content.logical_bytes,
                            content: content.root, content_offset: 0 }
                    };
                    push_authored_operation(operations, &mut allocation, &mut bytes, accounting, mutation)?;
                    Ok(None)
                }
                AuthoredMutation::WriteFromContent { path, offset, content } => {
                    let mutation = if content.logical_bytes == 0 {
                        Mutation::ValidateRegular { path }
                    } else {
                        Mutation::Write { path, offset, length: content.logical_bytes,
                            content: content.root, content_offset: 0 }
                    };
                    push_authored_operation(operations, &mut allocation, &mut bytes, accounting, mutation)?;
                    Ok(None)
                }
                AuthoredMutation::SetMetadata { path, metadata } => {
                    let metadata = self.stage_authored_metadata_with_accounting(
                        metadata, staged_metadata, accounting, cancellation,
                    ).await?;
                    let mutation = if path.is_root() {
                        // The root has a record but no namespace binding.
                        drop(path);
                        let remaining = bytes.checked_sub(path_bytes).ok_or(WorkError::Overflow)?;
                        let retired = crate::kernel::split_allocation(&mut allocation, path_bytes)?;
                        bytes = remaining;
                        crate::kernel::retire_allocation(retired, path_bytes, accounting)?;
                        Mutation::File { file_id: self.root.root_file_id,
                            mutation: FileMutation::SetMetadata { metadata } }
                    } else {
                        Mutation::SetMetadata { path, metadata }
                    };
                    push_authored_operation(operations, &mut allocation, &mut bytes, accounting, mutation)?;
                    Ok(None)
                }
                AuthoredMutation::Resize { path, logical_bytes } => {
                    push_authored_operation(operations, &mut allocation, &mut bytes, accounting,
                        Mutation::Resize { path, logical_bytes })?;
                    Ok(None)
                }
                AuthoredMutation::ZeroRange { path, range, allocated, extend } => {
                    push_authored_operation(operations, &mut allocation, &mut bytes, accounting,
                        Mutation::ZeroRange { path, offset: range.offset, length: range.length,
                            allocated, extend })?;
                    Ok(None)
                }
                AuthoredMutation::Preallocate { path, range, keep_size } => {
                    push_authored_operation(operations, &mut allocation, &mut bytes, accounting,
                        Mutation::Preallocate { path, offset: range.offset, length: range.length,
                            keep_size })?;
                    Ok(None)
                }
                AuthoredMutation::CloneRange(request) => {
                    push_authored_operation(operations, &mut allocation, &mut bytes, accounting,
                        Mutation::CloneRange { source: request.source, source_offset: request.source_offset,
                            destination: request.destination, destination_offset: request.destination_offset,
                            length: request.length })?;
                    Ok(None)
                }
            }
        }.await;
        // The exhaustive lowering future has dropped every non-transferred input.
        crate::kernel::retire_allocation(allocation, bytes, accounting)?;
        result
    }

    fn validate_profile_kind(
        &self,
        kind: FileKind,
        work: WorkCounters,
    ) -> Result<(), OperationFailure<FsError>> {
        if kind.is_supported_by_profile(self.volume.config.profile) {
            Ok(())
        } else {
            Err(OperationFailure::new(FsError::UnsupportedFileKind, work))
        }
    }

    async fn compile_record_with_accounting(
        &self,
        operations: &mut crate::kernel::Owned<Vec<Mutation>>,
        path: NamespacePath,
        kind: FileKind,
        payload: FilePayload,
        metadata: FileMetadata,
        allocation: &mut crate::kernel::allocation::KernelOwnedAllocation,
        bytes: &mut u64,
        staged_metadata: &mut AuthoredMetadataCache,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<FileId, FsError> {
        let metadata = self.stage_authored_metadata_with_accounting(
            metadata, staged_metadata, accounting, cancellation,
        ).await?;
        let file_id = self.authored_file_id_with_accounting(kind, &path, accounting)?;
        push_authored_operation(operations, allocation, bytes, accounting,
            Mutation::Create { path, record: FileRecord {
                file_id, kind, link_count: 1, metadata, payload,
            } })?;
        Ok(file_id)
    }

    pub(crate) fn bind_authored_operation(&mut self, operation_id: OperationId) {
        self.authored_operation_id = Some(operation_id);
    }

    /// The compiler's already-bound retry identity, not a publication grant.
    pub(crate) const fn bound_authored_operation_id(&self) -> Option<OperationId> {
        self.authored_operation_id
    }

    pub(crate) async fn compile_copied_record(
        &self, input: crate::kernel::Owned<NamespacePath>, mut record: FileRecord,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<crate::kernel::Owned<(Mutation, FileId)>, FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            match &input.allocation {
                KernelOwnedAllocation::NativeOriginal(allocation)
                    if allocation.work().shares_ledger(original)
                        && allocation.bytes() == input.value.retained_bytes() => {}
                _ => return Err(FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout)),
            }
        }
        let bytes = input.bytes;
        let mut input = crate::kernel::Owned::new(Some(input.value), input.allocation, bytes);
        let result = async {
            accounting.check()?;
            let path = input.value.take().expect("copied path is consumed once");
            if path.is_root() && record.kind != FileKind::Directory {
                return Err(FsError::UnsupportedFileKind);
            }
            accounting.charge(WorkCounters {
                bytes_copied: size_of::<Mutation>() as u64, ..WorkCounters::UNCHARGED
            })?;
            if !path.is_root() && record.kind == FileKind::Directory {
                let entries = self.stage_empty_tree_with_accounting(accounting, cancellation).await?;
                record.payload = FilePayload::Directory { entries };
            }
            if path.is_root() {
                let file_id = self.root.root_file_id;
                Ok((Mutation::File {
                    file_id, mutation: FileMutation::SetMetadata { metadata: record.metadata },
                }, file_id))
            } else {
                let file_id = self.authored_file_id_with_accounting(record.kind, &path, accounting)?;
                record.file_id = file_id;
                record.link_count = 1;
                Ok((Mutation::Create { path, record }, file_id))
            }
        }.await;
        result.map(|value| crate::kernel::Owned::new(value, input.allocation, bytes))
    }

    /// Copies inline bytes or stages the actual source before lowering content.
    async fn compile_create_file_with_accounting(
        &self,
        path: NamespacePath,
        content: acyclic_stream::OwnedBytes,
        metadata: FileMetadata,
        file_id: FileId,
        operations: &mut crate::kernel::Owned<Vec<Mutation>>,
        allocation: &mut crate::kernel::allocation::KernelOwnedAllocation,
        bytes: &mut u64,
        staged_metadata: &mut AuthoredMetadataCache,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<FileId, FsError> {
        if content.len() as u64 > self.volume.config.limits.maximum_read_bytes {
            return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
        }
        let metadata = self.stage_authored_metadata_with_accounting(
            metadata, staged_metadata, accounting, cancellation,
        ).await?;
        if content.len() <= crate::kernel::MAXIMUM_INLINE_FILE_BYTES {
            accounting.charge(WorkCounters {
                bytes_copied: content.len() as u64, ..WorkCounters::UNCHARGED
            })?;
            let payload = FilePayload::InlineRegular(InlineFileData::new(&content)?);
            drop(content);
            push_authored_operation(operations, allocation, bytes, accounting,
                Mutation::Create { path, record: FileRecord {
                    file_id, kind: FileKind::Regular, link_count: 1, metadata, payload,
                } })?;
        } else {
            let content = self.stage_authored_blob_with_accounting(
                content, accounting, cancellation,
            ).await?;
            self.compile_regular_content_with_accounting(
                path, content, metadata, file_id, operations, allocation, bytes, accounting,
            )?;
        }
        Ok(file_id)
    }

    fn compile_regular_content_with_accounting(
        &self,
        path: NamespacePath,
        content: StagedContent,
        metadata: ObjectId,
        file_id: FileId,
        operations: &mut crate::kernel::Owned<Vec<Mutation>>,
        allocation: &mut crate::kernel::allocation::KernelOwnedAllocation,
        bytes: &mut u64,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        let record = FileRecord {
            file_id, kind: FileKind::Regular, link_count: 1, metadata,
            payload: FilePayload::InlineRegular(InlineFileData::new(&[])?),
        };
        if content.logical_bytes == 0 {
            return push_authored_operation(operations, allocation, bytes, accounting,
                Mutation::Create { path, record });
        }
        let crate::kernel::Owned {
            value: creation_path, allocation: mut creation_allocation, bytes: mut creation_bytes,
        } = clone_authored_path(&path, accounting)?;
        let result = push_authored_operation(
            operations, &mut creation_allocation, &mut creation_bytes, accounting,
            Mutation::Create { path: creation_path, record },
        );
        // The pushed operation owns the cloned path, or the failed push dropped it.
        crate::kernel::retire_allocation(creation_allocation, creation_bytes, accounting)?;
        result?;
        push_authored_operation(operations, allocation, bytes, accounting,
            Mutation::Write { path, offset: 0, length: content.logical_bytes,
                content: content.root, content_offset: 0 })
    }

    async fn stage_authored_blob_with_accounting(
        &self,
        content: acyclic_stream::OwnedBytes,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<StagedContent, FsError> {
        if content.len() as u64 > self.volume.config.limits.maximum_read_bytes {
            return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
        }
        let mut source = std::io::Cursor::new(content.as_ref());
        let result = self.stage_content_with_accounting(
            &mut source, (content.len() as u64).max(1), accounting, cancellation,
        ).await;
        drop(source);
        drop(content);
        result
    }

    fn authored_file_id_with_accounting(
        &self,
        kind: FileKind,
        path: &NamespacePath,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    ) -> Result<FileId, FsError> {
        if let Some(operation) = self.authored_operation_id {
            let mut bytes = (b"acyclic-fs-authored-file-id-v1\0".len()
                + operation.into_bytes().len() + 1) as u64;
            for component in path.components() {
                accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                bytes = bytes.checked_add(1 + size_of::<u64>() as u64)
                    .and_then(|value| value.checked_add(component.as_bytes().len() as u64))
                    .ok_or(WorkError::Overflow)?;
            }
            accounting.charge(WorkCounters { bytes_hashed: bytes, ..WorkCounters::UNCHARGED })?;
        }
        Ok(self.authored_file_id(kind, path))
    }

    fn authored_file_id(&self, kind: FileKind, path: &NamespacePath) -> FileId {
        let Some(operation_id) = self.authored_operation_id else {
            return FileId::new();
        };
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"acyclic-fs-authored-file-id-v1\0");
        hasher.update(&operation_id.into_bytes());
        hasher.update(&[kind.tag()]);
        for component in path.components() {
            hasher.update(&[component.encoding().tag()]);
            hasher.update(
                &u64::try_from(component.as_bytes().len())
                    .unwrap_or(u64::MAX)
                    .to_le_bytes(),
            );
            hasher.update(component.as_bytes());
        }
        let digest = hasher.finalize();
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(&digest.as_bytes()[..16]);
        FileId::from_bytes(bytes)
    }

    async fn stage_metadata_with_accounting(
        &self,
        metadata: FileMetadata,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<ObjectId, FsError> {
        let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| crate::kernel::encode_file_metadata(metadata, observe, allocation_error))?;
        self.volume.fs.put_encoded_with_accounting(
            ObjectKind::Metadata, encoded, accounting, cancellation,
        ).await
    }

    async fn stage_authored_metadata_with_accounting(
        &self,
        metadata: FileMetadata,
        staged: &mut AuthoredMetadataCache,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<ObjectId, FsError> {
        accounting.check()?;
        if let Some(value) = staged.get(metadata, accounting)? {
            return Ok(value);
        }
        let value = self.stage_metadata_with_accounting(metadata, accounting, cancellation).await?;
        staged.remember(metadata, value);
        Ok(value)
    }

    async fn stage_empty_tree_with_accounting(
        &self,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<ObjectId, FsError> {
        let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| crate::kernel::encode_tree_page(&TreePage::Leaf(Vec::new()), self.volume.config.limits.maximum_directory_page_entries, observe, allocation_error))?;
        self.volume.fs.put_encoded_with_accounting(
            ObjectKind::TreePage, encoded, accounting, cancellation,
        ).await
    }

    async fn admit_new_name_with_accounting(
        &mut self,
        path: &NamespacePath,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
        allowed_file_ids: &[FileId],
        parent_pending: bool,
    ) -> Result<(), FsError> {
        accounting.check()?;
        let Some((parent, name)) = path.split_last() else { return Ok(()); };
        if self.volume.config.unicode == UnicodePolicy::RequireNfc
            && name.encoding() == NameEncoding::Utf8
            && !std::str::from_utf8(name.as_bytes()).is_ok_and(unicode_normalization::is_nfc)
        {
            return Err(FsError::NonNormalizedName);
        }
        if self.volume.config.case_sensitivity == CaseSensitivity::ProfileFolded
            && !parent_pending
            && self.find_case_folded_sibling_with_accounting(
                parent, name, accounting, cancellation, allowed_file_ids,
            ).await?.is_some()
        {
            return Err(FsError::NameCollision);
        }
        Ok(())
    }

    fn find_case_folded_sibling_with_accounting<'a, 'accounting>(
        &'a mut self,
        parent_components: &'a [LogicalName],
        target: &'a LogicalName,
        accounting: &'a mut crate::kernel::allocation::KernelAccounting<'accounting>,
        cancellation: &'a CancellationToken,
        allowed_file_ids: &'a [FileId],
    ) -> impl std::future::Future<Output = Result<Option<(LogicalName, crate::kernel::allocation::KernelOwnedAllocation)>, FsError>> + ProviderTask + 'a {
        async move {
            let mut frame_custody = None;
            let (future, frame_bytes) = crate::heap_future::in_heap_admitted(
                &mut *accounting,
                |context, bytes| {
                    frame_custody = Some(context.claim_owned(bytes, u64::from(bytes != 0))?);
                    Ok::<(), FsError>(())
                },
                |accounting| async move {
                    let allocation = accounting.claim_owned(0, 0)?;
                    let mut parent = (
                        NamespacePath::new(Vec::new(), self.volume.config.limits).map_err(name_path_error)?,
                        allocation,
                    );
                    for name in parent_components {
                        let next = parent.0.child_with_accounting(
                            name, self.volume.config.limits, accounting, cancellation,
                        )?;
                        let bytes = parent.0.retained_bytes();
                        retire_name_owned(std::mem::replace(&mut parent, next), bytes, accounting)?;
                    }
                    let folded_target = name_fold_key(target, accounting)?;
                    let mut after: Option<(LogicalName, crate::kernel::allocation::KernelOwnedAllocation)> = None;
                    let found = loop {
                        accounting.check()?;
                        let page = self.list_directory_with_accounting(
                            &parent.0, after.as_ref().map(|owned| &owned.0).map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                            CASE_FOLD_COLLISION_SCAN_PAGE, accounting, cancellation,
                        ).await?;
                        let mut found = None;
                        for entry in &page.0.entries {
                            let folded_entry = name_fold_key(&entry.name, accounting)?;
                            let collision = entry.name != *target && !allowed_file_ids.contains(&entry.file_id)
                                && folded_entry.0 == folded_target.0;
                            let bytes = folded_entry.0.capacity() as u64;
                            retire_name_owned(folded_entry, bytes, accounting)?;
                            if collision
                            {
                                found = Some(entry.name.clone_with_accounting(accounting, cancellation)?);
                                break;
                            }
                        }
                        let has_more = page.0.has_more;
                        if let Some(old) = after.take() {
                            let bytes = old.0.retained_bytes() as u64;
                            retire_name_owned(old, bytes, accounting)?;
                        }
                        after = match page.0.entries.last().filter(|_| has_more && found.is_none()) {
                            Some(entry) => Some(entry.name.clone_with_accounting(accounting, cancellation)?),
                            None => None,
                        };
                        let bytes = page.0.entries.iter().try_fold(
                            (page.0.entries.capacity() * size_of::<crate::kernel::TreeEntry>()) as u64,
                            |bytes, entry| bytes.checked_add(entry.name.retained_bytes() as u64).ok_or(WorkError::Overflow),
                        )?;
                        retire_name_owned(page, bytes, accounting)?;
                        if found.is_some() || !has_more { break found; }
                    };
                    let bytes = parent.0.retained_bytes();
                    retire_name_owned(parent, bytes, accounting)?;
                    let bytes = folded_target.0.capacity() as u64;
                    retire_name_owned(folded_target, bytes, accounting)?;
                    Ok(found)
                },
            )?;
            let mut future: acyclic_stream::BoxProviderFuture<'_, _> = future;
            let result = future.as_mut().await;
            drop(future);
            let mut frame_custody = frame_custody.ok_or(FsError::PendingMutationAllocationFailed)?;
            accounting.release_owned(&mut frame_custody, frame_bytes)?;
            result
        }
    }

    /// Creates one file through the atomic authored compiler.
    pub async fn create(
        &mut self,
        authored: AuthoredMutation,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<FileId> {
        match &authored {
            AuthoredMutation::CreateFile { bytes, .. }
            | AuthoredMutation::CreateSymbolicLink { target: bytes, .. }
                if bytes.len() as u64 > self.volume.config.limits.maximum_read_bytes =>
                return Err(OperationFailure::before_work(FsError::FileRead(FileRangeReadError::InvalidRange))),
            AuthoredMutation::CreateEmptySpecial { kind, .. } => {
                if !matches!(kind, FileKind::Fifo | FileKind::Socket | FileKind::MountBoundary) {
                    return Err(OperationFailure::before_work(FsError::InvalidSpecialKind));
                }
                self.validate_profile_kind(*kind, WorkCounters::UNCHARGED)?;
            }
            AuthoredMutation::CreateDevice { kind, .. } => {
                if !matches!(kind, FileKind::CharacterDevice | FileKind::BlockDevice) {
                    return Err(OperationFailure::before_work(FsError::InvalidSpecialKind));
                }
                self.validate_profile_kind(*kind, WorkCounters::UNCHARGED)?;
            }
            AuthoredMutation::CreateReparsePoint { payload, .. } => {
                self.validate_profile_kind(FileKind::ReparsePoint, WorkCounters::UNCHARGED)?;
                if payload.len() as u64 > self.volume.config.limits.maximum_read_bytes {
                    return Err(OperationFailure::before_work(FsError::FileRead(FileRangeReadError::InvalidRange)));
                }
            }
            AuthoredMutation::CreateFile { .. }
            | AuthoredMutation::CreateFileFromContent { .. }
            | AuthoredMutation::CreateDirectory { .. }
            | AuthoredMutation::CreateSymbolicLink { .. } => {}
            _ => return Err(OperationFailure::before_work(FsError::InvalidSpecialKind)),
        }
        self.apply_authored_transaction(vec![authored], budget, cancellation).await.map(|receipt| {
            FsReceipt {
                value: receipt.value.created_file_ids.into_iter().next().flatten()
                    .expect("one creation emits one file identity"),
                work: receipt.work,
            }
        })
    }

    /// Reads the same symbolic-link target, preserving producer-owned capacity.
    pub async fn read_symbolic_link<W: FsWork>(
        &mut self, path: &NamespacePath, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<crate::kernel::OwnedSourceChunk, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = self.read_symbolic_link_with_accounting(path, &mut accounting, cancellation).await;
        budget.finish(result, &accounting)
    }

    pub(crate) async fn read_symbolic_link_with_accounting(
        &mut self, path: &NamespacePath, accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::kernel::OwnedSourceChunk, FsError> {
        let (lookup, allocation) = self.lookup_no_follow_with_accounting(path, accounting, cancellation).await?;
        drop(allocation);
        let record = lookup.record.ok_or(FsError::NotFound)?;
        read_symbolic_link_record_with_accounting(&self.volume, record, accounting, cancellation).await
    }

    /// Reads exact opaque Windows reparse-point bytes without interpreting them.
    pub async fn read_reparse_point(
        &mut self,
        path: &NamespacePath,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<acyclic_stream::OwnedBytes> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let (lookup, allocation) = self.lookup_no_follow_with_accounting(
                path, &mut accounting, cancellation,
            ).await?;
            drop(allocation);
            let record = lookup.record.ok_or(FsError::NotFound)?;
            let FilePayload::ReparsePoint { payload_bytes, payload } = record.payload else {
                return Err(FsError::NotReparsePoint);
            };
            if payload_bytes > self.volume.config.limits.maximum_read_bytes {
                return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
            }
            if payload_bytes == 0 { return Ok(acyclic_stream::OwnedBytes::new()); }
            crate::kernel::read_blob_range_with_accounting(
                &self.volume.fs.inner.objects, payload, ByteRange { offset: 0, length: payload_bytes },
                DecodeLimits::for_volume(self.volume.config), &mut accounting, cancellation,
            ).await.map(|read| read.bytes)
        }.await;
        measured(result, &accounting)
    }

    /// Captures one regular file as an ephemeral path-independent open view.
    pub async fn detach_regular_file(
        &mut self,
        path: &NamespacePath,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<DetachedFile<A, O>> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let (lookup, allocation) = self.lookup_no_follow_with_accounting(
                path, &mut accounting, cancellation,
            ).await?;
            drop(allocation);
            let record = lookup.record.ok_or(FsError::NotFound)?;
            if record.kind != FileKind::Regular {
                return Err(FsError::FileRead(FileRangeReadError::NotRegular));
            }
            accounting.check()?;
            let hold = crate::Collection::hold_record_with_accounting(
                self.volume.fs.inner.objects.collection(), record, self.volume.config, &mut accounting,
            )?;
            Ok(DetachedFile { volume: self.volume.clone(), record, _hold: hold })
        }.await;
        measured(result, &accounting)
    }

    pub(crate) fn detached_from_record<W: FsWork>(
        &self,
        record: FileRecord,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<DetachedFile<A, O>, FsError> {
        DetachedFile::from_record(self.volume.clone(), record, budget, cancellation)
    }

    /// Reads one authenticated candidate file record by stable identity.
    pub async fn read_file_record_by_id<W: FsWork>(
        &mut self, file_id: FileId, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<FileRecord, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let before = readonly_standalone_live(&accounting);
        let result = async {
            let (record, allocation) = crate::kernel::lookup_file_record_async_with_accounting(
                &self.volume.fs.inner.objects, self.root.file_table, file_id,
                DecodeLimits::for_volume(self.volume.config), &mut accounting, cancellation,
                Some(&self.dependencies.data_custody),
            ).await?;
            drop(allocation);
            let record = record.ok_or(FsError::NotFound)?;
            if self.tracks_observations() {
                self.observe_identity_regions_with_accounting(
                    [Some(DependencyRegion::FileRecord(file_id)), None], &mut accounting, cancellation,
                ).await?;
            }
            Ok(record)
        }.await;
        let result = readonly_cleanup_standalone(&mut accounting, before, 0).and(result);
        budget.finish(result, &accounting)
    }

    /// Reads one exact logical range by stable file identity.
    pub async fn read_file_range_by_id<W: FsWork>(
        &mut self, file_id: FileId, range: ByteRange,
        budget: W, cancellation: &CancellationToken,
    ) -> W::Output<FileRangeRead, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            if range.length > self.volume.config.limits.maximum_read_bytes {
                return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
            }
            let record = self.lookup_file_record_by_id_with_accounting(file_id, &mut accounting, cancellation).await?
                .ok_or(FsError::NotFound)?;
            self.read_record_range_with_accounting(record, range, range, &mut accounting, cancellation).await
        }.await;
        let result = result.map(|mut value| { value.work = accounting.spent(); value });
        budget.finish(result, &accounting)
    }

    /// Reads at most `maximum_bytes`, clipping to the current logical EOF.
    pub async fn read_file_up_to_by_id<W: FsWork>(
        &mut self, file_id: FileId, offset: u64, maximum_bytes: u32,
        budget: W, cancellation: &CancellationToken,
    ) -> W::Output<FileRangeRead, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            let requested_range = ByteRange { offset, length: u64::from(maximum_bytes) };
            if requested_range.length > self.volume.config.limits.maximum_read_bytes {
                return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
            }
            let record = self.lookup_file_record_by_id_with_accounting(file_id, &mut accounting, cancellation).await?
                .ok_or(FsError::NotFound)?;
            if record.kind != FileKind::Regular { return Err(FsError::FileRead(FileRangeReadError::NotRegular)); }
            let logical_bytes = match record.payload {
                FilePayload::InlineRegular(data) => data.as_bytes().len() as u64,
                FilePayload::Regular { logical_bytes, .. } => logical_bytes,
                _ => return Err(FsError::FileRead(FileRangeReadError::NotRegular)),
            };
            let length = requested_range.length.min(logical_bytes.saturating_sub(offset));
            if length == 0 {
                if self.tracks_observations() && requested_range.length != 0 {
                    self.observe_base_regular_range_with_accounting(file_id, requested_range, &mut accounting, cancellation).await?;
                }
                let bytes = crate::kernel::owned_read_bytes((Vec::new(), accounting.claim_owned(0, 0)?).into())?;
                return Ok(FileRangeRead { bytes, logical_owned_bytes: 0, work: accounting.spent() });
            }
            self.read_record_range_with_accounting(record, ByteRange { offset, length }, requested_range, &mut accounting, cancellation).await
        }.await;
        let result = result.map(|mut value| { value.work = accounting.spent(); value });
        budget.finish(result, &accounting)
    }

    async fn read_record_range_with_accounting(
        &mut self, record: FileRecord, range: ByteRange, observed_range: ByteRange,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<FileRangeRead, FsError> {
        let before = readonly_standalone_live(accounting);
        let result = async {
            let mut read = crate::kernel::read_file_range_with_accounting(
                &self.volume.fs.inner.objects,
                FileRangeRequest {
                    record, range, maximum_spans: self.volume.config.limits.maximum_directory_page_entries,
                    limits: DecodeLimits::for_volume(self.volume.config), budget: accounting.remaining()?,
                },
                accounting, cancellation,
            ).await?;
            if self.tracks_observations() && observed_range.length != 0 {
                self.observe_base_regular_range_with_accounting(
                    record.file_id, observed_range, accounting, cancellation,
                ).await?;
            }
            read.work = accounting.spent();
            Ok(read)
        }.await;
        let retained = result.as_ref().map_or(0, |read| read.logical_owned_bytes);
        readonly_cleanup_standalone(accounting, before, retained)?;
        result
    }

    /// Plans one bounded sparse range by stable file identity without reading
    pub async fn plan_file_extents_by_id(
        &mut self,
        file_id: FileId,
        range: ByteRange,
        maximum_spans: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Option<ExtentPlan>> {
        if maximum_spans == 0 || range.length > self.volume.config.limits.maximum_generation_bytes {
            return Err(OperationFailure::before_work(FsError::FileRead(
                FileRangeReadError::InvalidRange,
            )));
        }
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let record = self.lookup_file_record_by_id_with_accounting(file_id, &mut accounting, cancellation).await?
                .ok_or(FsError::NotFound)?;
            self.plan_record_extents(record, range, maximum_spans, &mut accounting, cancellation).await
                .map(|(plan, allocation)| { drop(allocation); plan })
        }.await;
        measured(result, &accounting)
    }

    async fn plan_record_extents(
        &mut self, record: FileRecord, range: ByteRange, maximum_spans: u32,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<(Option<ExtentPlan>, KernelOwnedAllocation), FsError> {
        let mut planned = match record.payload {
            FilePayload::InlineRegular(data) => {
                validate_planned_file_range(data.as_bytes().len() as u64, range)?;
                (None, KernelOwnedAllocation::Standalone)
            }
            FilePayload::Regular { logical_bytes, extents } => {
                let (plan, allocation) = crate::kernel::plan_extent_range_with_accounting(
                    &self.volume.fs.inner.objects,
                    ExtentRangeRequest {
                        root: extents, file_size: logical_bytes, range, maximum_spans,
                        limits: DecodeLimits::for_volume(self.volume.config), budget: accounting.remaining()?,
                    },
                    accounting, cancellation,
                ).await?;
                (Some(plan), allocation)
            }
            _ => return Err(FsError::FileRead(FileRangeReadError::NotRegular)),
        };
        if self.tracks_observations() && range.length != 0 {
            self.observe_base_regular_range_with_accounting(record.file_id, range, accounting, cancellation).await?;
        }
        if let Some(plan) = &mut planned.0 { plan.work = accounting.spent(); }
        Ok(planned)
    }

    /// Finds the first sparse data or hole boundary by stable file identity.
    pub async fn seek_file_extent_by_id<W: FsWork>(
        &mut self, file_id: FileId, offset: u64, target: ExtentSeekTarget,
        budget: W, cancellation: &CancellationToken,
    ) -> W::Output<Option<u64>, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            let accounting = &mut accounting;
            let record = self.lookup_file_record_by_id_with_accounting(file_id, accounting, cancellation).await?
                .ok_or(FsError::NotFound)?;
            let value = seek_record_extent_with_accounting(&self.volume, record, offset, target, accounting, cancellation).await?;
            if self.tracks_observations() {
                self.observe_identity_regions_with_accounting(
                    [Some(DependencyRegion::SparseSeek { file_id, offset, target }), None], accounting, cancellation,
                ).await?;
            }
            Ok(value)
        }.await;
        budget.finish(result, &accounting)
    }

    /// Reads complete canonical metadata by stable file identity.
    pub async fn read_metadata_by_id<W: FsWork>(
        &mut self, file_id: FileId, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<FileMetadata, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = self.read_metadata_by_id_with_accounting(file_id, &mut accounting, cancellation).await;
        budget.finish(result, &accounting)
    }

    pub(crate) async fn read_metadata_by_id_with_accounting(
        &mut self, file_id: FileId,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<FileMetadata, FsError> {
        let before = readonly_standalone_live(accounting);
        let result = async {
            let record = self.lookup_file_record_by_id_with_accounting(file_id, accounting, cancellation).await?
                .ok_or(FsError::NotFound)?;
            let metadata = read_record_metadata_to_file_root_with_accounting(
                &self.volume, record, accounting, cancellation,
            ).await?;
            if self.tracks_observations() {
                self.observe_base_metadata_ids_with_accounting(
                    std::slice::from_ref(&file_id), accounting, cancellation,
                ).await?;
            }
            Ok(metadata)
        }.await;
        readonly_cleanup_standalone(accounting, before, 0)?;
        result
    }

    /// Reads one exact attribute while preserving its producer allocation custody.
    pub async fn read_named_attribute_by_id<W: FsWork>(
        &mut self, file_id: FileId, name: &AttributeName,
        budget: W, cancellation: &CancellationToken,
    ) -> W::Output<Option<crate::kernel::OwnedSourceChunk>, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            let accounting = &mut accounting;
            let metadata = self.read_metadata_by_id_with_accounting(file_id, accounting, cancellation).await?;
            read_named_attribute_to_file_root_with_accounting(&self.volume, metadata, name, accounting, cancellation).await
        }.await;
        budget.finish(result, &accounting)
    }

    /// Lists one bounded page of named attributes by stable file identity.
    pub async fn list_named_attributes_by_id(
        &mut self, file_id: FileId, after: Option<&AttributeName>, maximum_entries: u32,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> FsResult<AttributeListing> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
        let result = self.list_named_attributes_by_id_with_accounting(
            file_id, after, maximum_entries, &mut accounting, cancellation,
        ).await;
        let work = accounting.spent();
        result.map(|(mut value, allocation)| {
            drop(allocation); value.work = work; FsReceipt { value, work }
        }).map_err(|error| OperationFailure::new(error, work))
    }

    pub(crate) async fn list_named_attributes_by_id_with_accounting(
        &mut self, file_id: FileId, after: Option<&AttributeName>, maximum_entries: u32,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<(AttributeListing, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
        let metadata = self.read_metadata_by_id_with_accounting(file_id, accounting, cancellation).await?;
        list_named_attributes_to_file_root_with_accounting(&self.volume, metadata, after, maximum_entries, accounting, cancellation).await
    }

    /// Changes one exact named attribute atomically.
    pub async fn change_named_attribute_by_id<W: FsWork>(
        &mut self, file_id: FileId, name: &AttributeName, value: Option<(&[u8], NamedAttributeWriteMode)>,
        budget: W, cancellation: &CancellationToken,
    ) -> W::Output<(), FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() { self.validate_native_work(original)?; }
            let accounting = &mut accounting;
            let metadata = self.read_metadata_by_id_with_accounting(file_id, accounting, cancellation).await?;
            let metadata = Self::change_named_attribute_metadata_with_accounting(
                &self.volume, metadata, name, value, None, accounting, cancellation,
            ).await?;
            self.set_attributes_by_id_with_accounting(file_id, metadata, None, accounting, cancellation).await
        }.await;
        budget.finish(result, &accounting)
    }

    async fn change_named_attribute_metadata_with_accounting(
        volume: &Volume<A, O>, mut metadata: FileMetadata, name: &AttributeName,
        value: Option<(&[u8], NamedAttributeWriteMode)>,
        maximum_value_bytes: Option<u64>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<FileMetadata, FsError> {
        let root = match metadata.named_attributes {
            MetadataField::Value(root) => root,
            MetadataField::Unavailable if value.is_some() => {
                let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| crate::kernel::encode_attribute_page(&AttributePage::Leaf(Vec::new()), volume.config.limits.maximum_directory_page_entries, observe, allocation_error))?;
                volume.fs.put_encoded_with_accounting(ObjectKind::AttributePage, encoded, accounting, cancellation).await?
            },
            MetadataField::Unavailable => return Err(FsError::NotFound),
        };
        let (current, allocation) = crate::kernel::lookup_attribute_async_with_accounting(
            &volume.fs.inner.objects, root, name,
            DecodeLimits::for_volume(volume.config), accounting, cancellation,
        ).await?;
        let current_bytes = current.as_ref().map_or(0, |entry| crate::foundation::usize_to_u64(entry.name.retained_bytes()));
        let current = crate::kernel::Owned::new(current, allocation, current_bytes);
        let result = async {
            match (value.map(|(_, mode)| mode), current.value.as_ref()) {
                (Some(NamedAttributeWriteMode::Create), Some(_))
                | (Some(NamedAttributeWriteMode::Replace), None) => return Err(FsError::CreationRejected),
                (None, None) => return Err(FsError::NotFound),
                _ => {}
            }
            let blob = if let Some((bytes, _)) = value {
                let source_bytes = crate::foundation::usize_to_u64(bytes.len());
                if maximum_value_bytes.is_some_and(|maximum| source_bytes > maximum) {
                    return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
                }
                let mut source = std::io::Cursor::new(bytes);
                Some(stage_content_for_volume_with_accounting(
                    volume, &mut source, source_bytes.max(1), accounting, cancellation,
                ).await?)
            } else { None };
            let mut observer = crate::kernel::OriginalReadAccounting::new(accounting)?;
            let cloned = name.try_clone::<FsError>(
                DecodeLimits::for_volume(volume.config).maximum_name_bytes,
                &mut |step| observer.observe(step),
            );
            match cloned {
                Ok(name) => {
                    let (allocation, bytes) = observer.into_parts();
                    Ok((blob, crate::kernel::Owned::new(name, allocation, bytes)))
                }
                Err(error) => {
                    observer.release_failed()?;
                    Err(error)
                }
            }
        }.await;
        let (blob, name) = match result {
            Ok(value) => value,
            Err(error) => { current.retire(accounting)?; return Err(error); }
        };
        let crate::kernel::Owned { value: current, allocation: mut custody, bytes: current_bytes } = current;
        let crate::kernel::Owned { value: name, allocation: name_custody, bytes: name_bytes } = name;
        let mutation = match blob {
            Some(blob) => {
                let replacement = AttributeEntry { name, value_bytes: blob.logical_bytes, value: blob.root };
                match current {
                    Some(expected) => AttributeMutation::Replace { expected, replacement },
                    None => AttributeMutation::Insert(replacement),
                }
            }
            None => AttributeMutation::Remove { name, expected: current },
        };
        let mutation = crate::kernel::Owned::new(mutation, name_custody, name_bytes);
        let changed = crate::kernel::apply_attribute_mutations_with_accounting(
            &volume.fs.inner.objects, root, std::slice::from_ref(&mutation.value), 1,
            DecodeLimits::for_volume(volume.config), accounting, cancellation,
        ).await;
        mutation.retire(accounting)?;
        accounting.release_owned(&mut custody, current_bytes)?;
        metadata.named_attributes = MetadataField::Value(changed?.value);
        Ok(metadata)
    }

    /// Changes content by namespace path or stable file identity.
    pub async fn change_content<F: ContentFile, W: FsWork>(
        &mut self,
        file: F,
        change: ContentChange<F>,
        times: ContentTimes,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() { self.validate_native_work(original)?; }
            let input = (file, change);
            let bytes = content_input_path_bytes(&input)?;
            let allocation = accounting.claim_owned(bytes, 0)?;
            let compiled = self.compile_content_with_accounting(
                crate::kernel::Owned::new(input, allocation, bytes),
                times, &mut accounting, cancellation,
            ).await?;
            self.mutate_owned_with_accounting(compiled, &mut accounting, cancellation).await
        }.await;
        budget.finish(result, &accounting)
    }

    /// Input custody covers paths; opaque source custody stays with its caller
    pub(crate) async fn compile_content_with_accounting<F: ContentFile>(
        &mut self,
        input: crate::kernel::Owned<(F, ContentChange<F>)>,
        times: ContentTimes,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::kernel::Owned<Vec<Mutation>>, FsError> {
        let crate::kernel::Owned { value, allocation, bytes } = input;
        let mut input = (Some(value), allocation, bytes);
        let result = async {
            accounting.check()?;
            let mut operations = crate::kernel::Owned::allocate(
                1 + usize::from(matches!(times, ContentTimes::Stamp(_))), accounting,
            )?;
            let lowered = async {
                let (file, change) = input.0.take().expect("content input is consumed once");
                let operation = match change {
                    ContentChange::Write { bytes, .. } if bytes.is_empty() => ContentOperation::ValidateRegular,
                    ContentChange::Write { offset, bytes } => {
                        let content = self.stage_authored_blob_with_accounting(bytes, accounting, cancellation).await?;
                        ContentOperation::Write { offset, length: content.logical_bytes, content: content.root }
                    }
                    ContentChange::Resize { logical_bytes } => ContentOperation::Resize { logical_bytes },
                    ContentChange::ZeroRange { range, allocated, extend } => ContentOperation::ZeroRange { range, allocated, extend },
                    ContentChange::Preallocate { range, keep_size } => ContentOperation::Preallocate { range, keep_size },
                    ContentChange::CloneFrom { source, source_offset, offset, length } => ContentOperation::CloneFrom { source, source_offset, offset, length },
                };
                let mut stamped_metadata = None;
                if let ContentTimes::Stamp(at_ns) = times {
                    let mut metadata = match file.address() {
                        ContentAddress::Path(path) => self.read_metadata_with_accounting(path, accounting, cancellation).await?,
                        ContentAddress::File(file_id) => self.read_metadata_by_id_with_accounting(file_id, accounting, cancellation).await?,
                    };
                    if metadata.stamp_content_change(at_ns) {
                        stamped_metadata = Some(self.stage_metadata_with_accounting(metadata, accounting, cancellation).await?);
                    }
                }
                if let Some(metadata) = stamped_metadata {
                    let cloned = file.clone_content_with_accounting(accounting, cancellation)?;
                    let crate::kernel::Owned {
                        value: cloned, allocation: mut cloned_allocation, bytes: mut cloned_bytes,
                    } = cloned;
                    let mutation = cloned.mutation(operation);
                    // A clone operation also moves its original source path.
                    let source_bytes = mutation_path_owned_bytes(&mutation)
                        .checked_sub(cloned_bytes).ok_or(WorkError::Overflow)?;
                    let mut source_allocation = crate::kernel::split_allocation(&mut input.1, source_bytes)?;
                    input.2 = input.2.checked_sub(source_bytes).ok_or(WorkError::Overflow)?;
                    if let Err(error) = cloned_allocation.absorb(&mut source_allocation) {
                        drop(mutation);
                        crate::kernel::retire_allocation(source_allocation, source_bytes, accounting)?;
                        crate::kernel::retire_allocation(cloned_allocation, cloned_bytes, accounting)?;
                        return Err(error);
                    }
                    cloned_bytes = cloned_bytes.checked_add(source_bytes).ok_or(WorkError::Overflow)?;
                    let pushed = push_authored_operation(
                        &mut operations, &mut cloned_allocation, &mut cloned_bytes, accounting, mutation,
                    );
                    crate::kernel::retire_allocation(cloned_allocation, cloned_bytes, accounting)?;
                    pushed?;
                    push_authored_operation(
                        &mut operations, &mut input.1, &mut input.2, accounting,
                        file.mutation(ContentOperation::SetMetadata { metadata }),
                    )?;
                } else {
                    push_authored_operation(
                        &mut operations, &mut input.1, &mut input.2, accounting,
                        file.mutation(operation),
                    )?;
                }
                Ok::<_, FsError>(())
            }.await;
            if let Err(error) = lowered {
                operations.retire(accounting)?;
                return Err(error);
            }
            Ok(operations)
        }.await;
        drop(input.0);
        crate::kernel::retire_allocation(input.1, input.2, accounting)?;
        result
    }

    /// Applies independent changes as if one at a time in order, landing
    pub async fn apply_group(
        &mut self,
        changes: Vec<GroupedChange>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Vec<Result<GroupedOutcome, FsError>>> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
        let input = crate::kernel::Owned::new(
            changes, crate::kernel::allocation::KernelOwnedAllocation::Standalone, 0,
        );
        let result = self.apply_group_with_accounting(input, &mut accounting, cancellation).await;
        let work = accounting.spent();
        result.map(|output| FsReceipt { value: output.value, work })
            .map_err(|error| OperationFailure::new(error, work))
    }

    pub(crate) async fn apply_group_with_accounting(
        &mut self,
        input: crate::kernel::Owned<Vec<GroupedChange>>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::kernel::Owned<Vec<Result<GroupedOutcome, FsError>>>, FsError> {
        use crate::kernel::{Owned, split_allocation};
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let expected = input.value.capacity().checked_mul(size_of::<GroupedChange>())
                .ok_or(WorkError::Overflow)? as u64;
            let KernelOwnedAllocation::NativeOriginal(guard) = &input.allocation else {
                return Err(FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout));
            };
            if guard.bytes() != expected || !guard.work().shares_ledger(original)
                || !guard.work().shares_resource_allowance(original)
            {
                return Err(FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout));
            }
        }
        let mut compiled = Owned::<Vec<Result<(Owned<Vec<Mutation>>, GroupedOutcome), FsError>>>::allocate(input.value.len(), accounting)?;
        let mut output = Owned::<Vec<Result<GroupedOutcome, FsError>>>::allocate(input.value.len(), accounting)?;
        let mut metadata = AuthoredMetadataCache::new();
        let mut input = input;
        let result = async {
            // Only the input slots move into this guard. Original paths,
            for change in input.value.drain(..) {
                let guard = split_allocation(&mut input.allocation, 0)?;
                let change_input = Owned::new(change, guard, 0);
                let admitted = accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED });
                let change = match admitted {
                    Ok(()) => self.compile_grouped_with_accounting(change_input, &mut metadata, accounting, cancellation).await,
                    Err(error) => { change_input.retire(accounting)?; Err(error) }
                };
                compiled.value.push(change);
            }
            let applicable = compiled.value.iter().filter(|change| change.is_ok()).count();
            let mut combined_succeeded = false;
            if applicable > 1 {
                let combined = (|| {
                    let count = compiled.value.iter().flatten().try_fold(0usize, |count, (operations, _)| {
                        count.checked_add(operations.value.len()).ok_or(FsError::Work(WorkError::Overflow))
                    })?;
                    let mut combined = Owned::allocate(count, accounting)?;
                    let copied = (|| {
                        for (operations, _) in compiled.value.iter().flatten() {
                            for mutation in &operations.value {
                                let cloned = clone_grouped_mutation_with_accounting(mutation, accounting, cancellation)?;
                                combined.bytes = combined.bytes.checked_add(cloned.bytes).ok_or(WorkError::Overflow)?;
                                let mut guard = cloned.allocation;
                                readonly_absorb_allocation(&mut combined.allocation, &mut guard)?;
                                combined.value.push(cloned.value);
                            }
                        }
                        Ok::<(), FsError>(())
                    })();
                    if let Err(error) = copied { combined.retire(accounting)?; return Err(error); }
                    Ok(combined)
                })();
                if let Ok(operations) = combined {
                    combined_succeeded = self.mutate_owned_with_accounting(operations, accounting, cancellation).await.is_ok();
                }
            }
            for change in compiled.value.drain(..) {
                output.value.push(match change {
                    Ok((operations, outcome)) if combined_succeeded => {
                        operations.retire(accounting)?;
                        Ok(outcome)
                    }
                    Ok((operations, outcome)) => self.mutate_owned_with_accounting(operations, accounting, cancellation).await.map(|()| outcome),
                    Err(error) => Err(error),
                });
            }
            Ok::<(), FsError>(())
        }.await;
        input.retire(accounting)?;
        compiled.retire(accounting)?;
        if let Err(error) = result { output.retire(accounting)?; return Err(error); }
        Ok(output)
    }

    async fn compile_grouped_with_accounting(
        &mut self,
        input: crate::kernel::Owned<GroupedChange>,
        staged_metadata: &mut AuthoredMetadataCache,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(crate::kernel::Owned<Vec<Mutation>>, GroupedOutcome), FsError> {
        let crate::kernel::Owned { value: change, allocation, bytes } = input;
        match change {
            GroupedChange::CreateFile { path, metadata, file_id, bytes: content } => {
                accounting.charge(WorkCounters {
                    bytes_copied: size_of::<FileMetadata>() as u64, ..WorkCounters::UNCHARGED
                })?;
                let cloned = match clone_authored_path(&path, accounting) {
                    Ok(cloned) => cloned,
                    Err(error) => {
                        drop((path, metadata, content));
                        crate::kernel::retire_allocation(allocation, bytes, accounting)?;
                        return Err(error);
                    }
                };
                drop(path);
                crate::kernel::retire_allocation(allocation, bytes, accounting)?;
                let crate::kernel::Owned { value: path, mut allocation, mut bytes } = cloned;
                let count = 1 + usize::from(content.len() > crate::kernel::MAXIMUM_INLINE_FILE_BYTES);
                let mut operations = match crate::kernel::Owned::allocate(count, accounting) {
                    Ok(operations) => operations,
                    Err(error) => {
                        drop((path, metadata, content));
                        crate::kernel::retire_allocation(allocation, bytes, accounting)?;
                        return Err(error);
                    }
                };
                let result = self.compile_create_file_with_accounting(
                    path, content, *metadata, file_id, &mut operations, &mut allocation,
                    &mut bytes, staged_metadata, accounting, cancellation,
                ).await;
                drop(metadata);
                crate::kernel::retire_allocation(allocation, bytes, accounting)?;
                match result {
                    Ok(file_id) => Ok((operations, GroupedOutcome::Created(file_id))),
                    Err(error) => { operations.retire(accounting)?; Err(error) }
                }
            }
            GroupedChange::Content { file_id, change, times } => {
                let input = crate::kernel::Owned::new((file_id, change), allocation, bytes);
                self.compile_content_with_accounting(input, times, accounting, cancellation).await
                    .map(|operations| (operations, GroupedOutcome::Changed))
            }
        }
    }

    /// Replaces complete metadata and optionally logical size as one attached
    pub async fn set_attributes_by_id<W: FsWork>(
        &mut self, file_id: FileId, metadata: FileMetadata, logical_bytes: Option<u64>,
        budget: W, cancellation: &CancellationToken,
    ) -> W::Output<(), FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() { self.validate_native_work(original)?; }
            self.set_attributes_by_id_with_accounting(
            file_id, metadata, logical_bytes, &mut accounting, cancellation,
            ).await
        }.await;
        budget.finish(result, &accounting)
    }

    async fn set_attributes_by_id_with_accounting(
        &mut self, file_id: FileId, metadata: FileMetadata, logical_bytes: Option<u64>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let metadata = self.stage_metadata_with_accounting(metadata, accounting, cancellation).await?;
        let count = 1 + usize::from(logical_bytes.is_some());
        let mut operations = crate::kernel::Owned::<Vec<Mutation>>::allocate(count, accounting)?;
        let result = (|| -> Result<(), FsError> {
            accounting.admit(WorkCounters { bytes_copied: (count * size_of::<Mutation>()) as u64, ..WorkCounters::UNCHARGED })?;
            operations.value.push(Mutation::File { file_id, mutation: FileMutation::SetMetadata { metadata } });
            if let Some(logical_bytes) = logical_bytes {
                operations.value.push(Mutation::File { file_id, mutation: FileMutation::Resize { logical_bytes } });
            }
            accounting.record_completed(WorkCounters { bytes_copied: (count * size_of::<Mutation>()) as u64, ..WorkCounters::UNCHARGED })
        })();
        if let Err(error) = result { operations.retire(accounting)?; return Err(error); }
        self.mutate_owned_with_accounting(operations, accounting, cancellation).await
    }

    /// Replaces complete metadata and optionally logical size as one path transaction.
    pub async fn set_attributes(
        &mut self,
        path: NamespacePath,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let (lookup, allocation) = self.lookup_no_follow_with_accounting(
                &path, &mut accounting, cancellation,
            ).await?;
            drop(allocation);
            let file_id = lookup.record.ok_or(FsError::NotFound)?.file_id;
            self.set_attributes_by_id_with_accounting(
                file_id, metadata, logical_bytes, &mut accounting, cancellation,
            ).await
        }.await;
        measured(result, &accounting)
    }

    /// Streams caller content into immutable authenticated chunks without
    pub async fn stage_content<R: AsyncBlobSource>(
        &self,
        source: &mut R,
        maximum_source_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<StagedContent> {
        in_heap(move || async move {
            let mut accounting = KernelAccounting::standalone(budget, cancellation);
            let result = self.stage_content_with_accounting(
                source, maximum_source_bytes, &mut accounting, cancellation,
            ).await;
            measured(result, &accounting)
        }).await
    }

    pub(crate) async fn stage_content_with_accounting<R: AsyncBlobSource>(
        &self,
        source: &mut R,
        maximum_source_bytes: u64,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<StagedContent, FsError> {
        stage_content_for_volume_with_accounting(
            &self.volume, source, maximum_source_bytes, accounting, cancellation,
        ).await
    }

    /// Publishes this overlay with its permit, original context, and owner check.
    pub async fn commit<E, F, Fut>(
        &mut self, operation_id: OperationId, operation_context: Option<Digest>,
        operation_evidence: Option<acyclic_stream::OwnedBytes>,
        original_fingerprint_permit: Option<PublicationPermit>,
        permit: PublicationPermit, budget: WorkBudget, cancellation: &CancellationToken,
        mut before_publication: F,
    ) -> crate::MeasuredResult<FsReceipt<CheckoutCommitOutcome>, E>
    where
        E: From<FsError> + Send,
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::MeasuredResult<FsReceipt<()>, E>> + ProviderTask,
    {
        crate::heap_future::in_heap_measured_with_error(move |budget| async move {
                if self.mode.access != AccessMode::ReadWrite
                    || self.mode.mutations != MutationMode::PrivateOverlay
                {
                    return Err(OperationFailure::before_work(E::from(FsError::MutationNotAllowed)));
                }
                self.publish_pending(operation_id, operation_context, operation_evidence, original_fingerprint_permit, permit, budget, cancellation, &mut before_publication).await
            }, budget, cancellation, E::from(FsError::Cancelled(crate::CancellationError)), None, |error| E::from(FsError::from(error))).await
    }

    /// Seals the original companion operation even when its authored root is unchanged.
    pub(crate) async fn commit_even_if_clean<E, F, Fut>(
        &mut self,
        operation_id: OperationId,
        operation_context: Option<Digest>,
        operation_evidence: Option<acyclic_stream::OwnedBytes>,
        original_fingerprint_permit: Option<PublicationPermit>,
        permit: PublicationPermit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        before_publication: &mut F,
        producer_custody: Option<&Arc<crate::kernel::ProofDataCustody>>,
    ) -> crate::MeasuredResult<FsReceipt<CheckoutCommitOutcome>, E>
    where
        E: From<FsError> + Send,
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::MeasuredResult<FsReceipt<()>, E>> + ProviderTask,
    {
        crate::heap_future::in_heap_measured_with_error(move |budget| async move {
            if self.mode.access != AccessMode::ReadWrite
                || self.mode.mutations != MutationMode::PrivateOverlay
            {
                return Err(OperationFailure::before_work(E::from(FsError::MutationNotAllowed)));
            }
            let expected = self.authority_head
                .ok_or_else(|| OperationFailure::before_work(E::from(FsError::WritableCheckoutRequiresHead)))?;
            self.publish_pending_against(operation_id, operation_context, operation_evidence, original_fingerprint_permit, permit, expected, budget, cancellation, before_publication, None, producer_custody).await
        }, budget, cancellation, E::from(FsError::Cancelled(crate::CancellationError)), None, |error| E::from(FsError::from(error))).await
    }

    async fn publish_pending<E, F, Fut>(
        &mut self,
        operation_id: OperationId,
        operation_context: Option<Digest>,
        operation_evidence: Option<acyclic_stream::OwnedBytes>,
        original_fingerprint_permit: Option<PublicationPermit>,
        permit: PublicationPermit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        before_publication: &mut F,
    ) -> crate::MeasuredResult<FsReceipt<CheckoutCommitOutcome>, E>
    where
        E: From<FsError> + Send,
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::MeasuredResult<FsReceipt<()>, E>> + ProviderTask,
    {
        if !self.has_pending_mutations() && permit == PublicationPermit::Unrestricted
            && operation_context.is_none() && operation_evidence.is_none()
        {
            return self
                .resolve_clean_commit(operation_id, budget, cancellation)
                .await.map_err(|failure| failure.map_error(E::from));
        }
        let expected = self
            .authority_head
            .ok_or_else(|| OperationFailure::before_work(E::from(FsError::WritableCheckoutRequiresHead)))?;
        self.publish_pending_against(operation_id, operation_context, operation_evidence, original_fingerprint_permit, permit, expected, budget, cancellation, before_publication, None, None)
        .await
    }

    async fn publish_pending_against<E, F, Fut>(
        &mut self,
        operation_id: OperationId,
        operation_context: Option<Digest>,
        operation_evidence: Option<acyclic_stream::OwnedBytes>,
        original_fingerprint_permit: Option<PublicationPermit>,
        permit: PublicationPermit,
        expected: Head,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        before_publication: &mut F,
        resolved: Option<DurableCommit>,
        producer_custody: Option<&Arc<crate::kernel::ProofDataCustody>>,
    ) -> crate::MeasuredResult<FsReceipt<CheckoutCommitOutcome>, E>
    where
        E: From<FsError> + Send,
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::MeasuredResult<FsReceipt<()>, E>> + ProviderTask,
    {
        cancellation
            .check()
            .map_err(|error| OperationFailure::before_work(E::from(FsError::from(error))))?;
        self.publication_closure = None;
        let checkpoint = self.checkpoint_root(budget, cancellation).await
            .map_err(|failure| failure.map_error(E::from))?;
        let checkpoint_root = checkpoint.value;
        let mut work = checkpoint.work;
        let request = PublishGenerationRequest {
            authority_id: volume_authority_id(self.volume.id),
            volume_id: self.volume.id,
            epoch: expected.epoch,
            expected,
            operation_id,
            generation_root: checkpoint_root,
        };
        let custody_bytes = crate::foundation::usize_to_u64(
            std::mem::size_of::<Mutex<Option<VerifiedPublication<A, O>>>>()
                + 2 * std::mem::size_of::<usize>(),
        );
        let prospective = add(
            work,
            WorkCounters {
                allocation_operations: 1,
                peak_allocation_bytes: custody_bytes,
                ..WorkCounters::default()
            },
        ).map_err(|failure| failure.map_error(E::from))?;
        remaining(prospective, budget)
            .map_err(|failure| OperationFailure::new(E::from(failure.error), work))?;
        let publication_slot = Arc::new(Mutex::new(None));
        work = prospective;
        let mut publication_budget = remaining(work, budget)
            .map_err(|failure| failure.map_error(E::from))?;
        publication_budget.peak_allocation_bytes -= custody_bytes;
        let proven_at = self.volume.fs.inner.objects.collection_sweeps();
        let mut prepared_roots = None;
        let publication = {
            let mut prepare_roots = |root: &GenerationRoot, budget| {
                let mut accounting = KernelAccounting::standalone(budget, cancellation);
                let prepared = measured(self.prepare_root_replacement(
                    root, root, &mut accounting, cancellation, producer_custody,
                ), &accounting).map_err(|failure| failure.map_error(E::from))?;
                let bytes = root_parent_bytes(&prepared.value.base.0)
                    .and_then(|bytes| match &prepared.value.held {
                        Some((root, _)) => bytes.checked_add(root_parent_bytes(root)?)
                            .ok_or(FsError::Work(WorkError::Overflow)),
                        None => Ok(bytes),
                    })
                    .map_err(|error| OperationFailure::new(E::from(error), prepared.work))?;
                prepared_roots = Some(prepared.value);
                Ok(FsReceipt { value: bytes, work: prepared.work })
            };
            crate::kernel::publish_generation_async_guarded(
                &self.volume.fs.inner.objects,
                &self.volume.fs.inner.authority,
                request,
                PublicationIntent::guarded(operation_context, permit)
                    .with_original_evidence(operation_evidence.clone())
                    .with_original_fingerprint_permit(original_fingerprint_permit),
                closure_limits(self.volume.config),
                publication_budget,
                cancellation,
                before_publication,
                resolved,
                Some(&mut prepare_roots),
            ).await
        }
        .map_err(|mut failure| {
            *failure.work = work.with_backend(*failure.work, custody_bytes).unwrap_or(work);
            failure
        })?;
        drop(operation_evidence);
        work = merge_simultaneous_work(work, publication.work, custody_bytes, budget)
            .map_err(|failure| failure.map_error(E::from))?;
        let outcome = self.apply_publication(
            checkpoint_root,
            operation_id,
            (publication, KernelOwnedAllocation::Standalone),
            proven_at,
            publication_slot,
            prepared_roots,
        );
        Ok(FsReceipt {
            value: outcome,
            work,
        })
    }

    /// Publishes the unchanged root as the original companion head fence.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn commit_even_if_clean_with_native_work<'a, E, F, Fut>(
        &mut self,
        operation_id: OperationId,
        permit: PublicationPermit,
        original: &'a crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
        before_publication: &mut F,
        producer_custody: Option<&Arc<crate::kernel::ProofDataCustody>>,
        operation_context: Option<Digest>,
        operation_evidence: Option<acyclic_stream::OwnedBytes>,
        original_fingerprint_permit: Option<PublicationPermit>,
    ) -> Result<CheckoutCommitOutcome, E>
    where
        E: From<FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        if self.mode.access != AccessMode::ReadWrite || self.mode.mutations != MutationMode::PrivateOverlay {
            return Err(E::from(FsError::MutationNotAllowed));
        }
        let expected = self.authority_head.ok_or_else(|| E::from(FsError::WritableCheckoutRequiresHead))?;
        self.publish_pending_against_with_native_work(operation_id, operation_context, operation_evidence, original_fingerprint_permit, permit, expected, original, cancellation, before_publication, None, producer_custody).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn publish_pending_with_native_work<'a, E, F, Fut>(
        &mut self,
        operation_id: OperationId,
        operation_context: Option<Digest>,
        operation_evidence: Option<acyclic_stream::OwnedBytes>,
        original_fingerprint_permit: Option<PublicationPermit>,
        permit: PublicationPermit,
        original: &'a crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
        before_publication: &mut F,
        producer_custody: Option<&Arc<crate::kernel::ProofDataCustody>>,
    ) -> Result<CheckoutCommitOutcome, E>
    where
        E: From<FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        original.admit(WorkCounters::UNCHARGED).map_err(FsError::from).map_err(E::from)?;
        cancellation.check().map_err(FsError::from).map_err(E::from)?;
        if !self.has_pending_mutations() && permit == PublicationPermit::Unrestricted
            && operation_context.is_none() && operation_evidence.is_none()
        {
            return self.resolve_clean_commit(operation_id, original, cancellation)
                .await.map_err(E::from);
        }
        let expected = self.authority_head.ok_or_else(|| E::from(FsError::WritableCheckoutRequiresHead))?;
        self.publish_pending_against_with_native_work(operation_id, operation_context, operation_evidence, original_fingerprint_permit, permit, expected, original, cancellation, before_publication, None, producer_custody).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn publish_pending_against_with_native_work<'a, E, F, Fut>(
        &mut self,
        operation_id: OperationId,
        operation_context: Option<Digest>,
        operation_evidence: Option<acyclic_stream::OwnedBytes>,
        original_fingerprint_permit: Option<PublicationPermit>,
        permit: PublicationPermit,
        expected: Head,
        original: &'a crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
        before_publication: &mut F,
        resolved: Option<DurableCommit>,
        producer_custody: Option<&Arc<crate::kernel::ProofDataCustody>>,
    ) -> Result<CheckoutCommitOutcome, E>
    where
        E: From<FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        cancellation.check().map_err(FsError::from).map_err(E::from)?;
        let mut frame_custody = None;
        let create = || async move {
        original.admit(WorkCounters::UNCHARGED).map_err(FsError::from).map_err(E::from)?;
        cancellation.check().map_err(FsError::from).map_err(E::from)?;
        self.publication_closure = None;
        let checkpoint_root = self.checkpoint_root(original, cancellation).await.map_err(E::from)?;
        let request = PublishGenerationRequest {
            authority_id: volume_authority_id(self.volume.id), volume_id: self.volume.id,
            epoch: expected.epoch, expected, operation_id, generation_root: checkpoint_root,
        };
        let custody_bytes = crate::foundation::usize_to_u64(
            std::mem::size_of::<Mutex<Option<VerifiedPublication<A, O>>>>() + 2 * size_of::<usize>(),
        );
        // This actual slot is admitted before Arc's allocator. Its guard is
        let mut slot_allocation = original.claim(custody_bytes).map_err(FsError::from).map_err(E::from)?;
        let publication_slot = Arc::new(Mutex::new(None));
        let proven_at = self.volume.fs.inner.objects.collection_sweeps();
        let mut prepared_roots = None;
        let mut publication = {
            let mut prepare_roots = |root: &GenerationRoot, proof_guard: &mut crate::NativeAllocation, original: &crate::NativeMaterializationWork| {
                let mut prepared = self.prepare_root_replacement(
                    root, root, &mut KernelAccounting::native(original, cancellation), cancellation, producer_custody,
                ).map_err(E::from)?;
                self.prepare_root_working_control_with_native_work(
                    root, proof_guard, &mut prepared, original, cancellation,
                ).map_err(E::from)?;
                prepared_roots = Some(prepared);
                Ok(())
            };
            crate::kernel::publish_generation_async_guarded_with_native_work(
                &self.volume.fs.inner.objects, &self.volume.fs.inner.authority,
                request, PublicationIntent::guarded(operation_context, permit)
                    .with_original_evidence(operation_evidence.clone())
                    .with_original_fingerprint_permit(original_fingerprint_permit),
                closure_limits(self.volume.config), original, cancellation,
                before_publication, resolved, Some(&mut prepare_roots),
            ).await?
        };
        drop(operation_evidence);
        publication.1.absorb(&mut slot_allocation).map_err(FsError::from).map_err(E::from)?;
        let retained = (publication.0, KernelOwnedAllocation::NativeOriginal(publication.1));
        Ok(self.apply_publication(
            checkpoint_root, operation_id, retained, proven_at, publication_slot, prepared_roots,
        ))
        };
        let (mut future, _) = crate::heap_future::in_heap_admitted(
            (create, &mut frame_custody),
            |(_, custody), bytes| {
                **custody = Some(original.claim(bytes).map_err(FsError::from).map_err(E::from)?);
                Ok::<(), E>(())
            },
            |(create, _)| create(),
        )?;
        let result = future.as_mut().await;
        drop(future);
        drop(frame_custody);
        result
    }

    /// Resolves only the original durable operation. A lost acknowledgement
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn retry_stale_commit_with_native_work<'a, E, F, Fut>(
        &mut self,
        operation_id: OperationId,
        permit: PublicationPermit,
        original: &'a crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
        before_publication: &mut F,
        producer_custody: Option<&Arc<crate::kernel::ProofDataCustody>>,
        operation_context: Option<Digest>,
        operation_evidence: Option<acyclic_stream::OwnedBytes>,
        original_fingerprint_permit: Option<PublicationPermit>,
    ) -> Result<Option<CheckoutCommitOutcome>, E>
    where
        E: From<FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        let resolved = self.volume.fs.inner.authority.find_operation_with_native_work(
            volume_authority_id(self.volume.id), operation_id, original, cancellation,
        ).await.map_err(|error| E::from(match error {
            AuthorityStoreError::NativeOriginal(error) => FsError::NativeKernel(error),
            error => FsError::Authority(error),
        }))?;
        let Some(commit) = resolved.0 else { return Ok(None); };
        let previous_sequence = commit.sequence.get().checked_sub(1)
            .ok_or_else(|| E::from(FsError::InvalidAuthorityHistory))?;
        let expected = Head {
            epoch: commit.epoch, sequence: Sequence::new(previous_sequence), digest: commit.previous_digest,
        };
        self.publish_pending_against_with_native_work(operation_id, operation_context, operation_evidence, original_fingerprint_permit, permit, expected, original, cancellation, before_publication, Some(commit), producer_custody).await.map(Some)
    }

    pub(crate) fn publication_closure(
        &self,
        generation: GenerationId,
    ) -> Option<PublicationClosure<A, O>> {
        if self.generation_root.digest != generation.digest() {
            return None;
        }
        self.publication_closure.clone()
    }

    fn apply_publication(
        &mut self,
        checkpoint_root: ObjectId,
        operation_id: OperationId,
        mut retained: (PublicationReceipt, KernelOwnedAllocation),
        proven_at: u64,
        publication_slot: PublicationClosure<A, O>,
        mut prepared_roots: Option<PreparedCheckoutRoots>,
    ) -> CheckoutCommitOutcome {
        let generation_id = retained.0.proof.generation_id;
        self.publication_closure = if matches!(
            retained.0.outcome,
            AppendOutcome::Committed(_) | AppendOutcome::AlreadyCommitted(_)
        ) {
            *publication_slot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(VerifiedPublication {
                filesystem: self.volume.fs.clone(),
                generation_root: checkpoint_root,
                volume_id: retained.0.proof.root.volume_id,
                limits: closure_limits(self.volume.config),
                objects: std::mem::take(&mut retained.0.proof.objects),
                proven_at,
                _allocation: std::mem::replace(&mut retained.1, KernelOwnedAllocation::Standalone),
            });
            Some(publication_slot)
        } else {
            None
        };
        let outcome = match retained.0.outcome {
            AppendOutcome::Committed(commit) => CheckoutCommitOutcome::Committed {
                generation_id, head: durable_head(&commit),
            },
            AppendOutcome::AlreadyCommitted(commit) => CheckoutCommitOutcome::AlreadyCommitted {
                generation_id, head: durable_head(&commit),
            },
            AppendOutcome::Conflict { actual } => {
                drop(retained.0.proof);
                return CheckoutCommitOutcome::Conflict { actual };
            }
            AppendOutcome::Fenced { actual_epoch } => {
                drop(retained.0.proof);
                return CheckoutCommitOutcome::Fenced { actual_epoch };
            }
            AppendOutcome::IdempotencyConflict { committed_fingerprint } => {
                drop(retained.0.proof);
                return CheckoutCommitOutcome::IdempotencyConflict { committed_fingerprint };
            }
        };
        if let CheckoutCommitOutcome::Committed { head, .. }
            | CheckoutCommitOutcome::AlreadyCommitted { head, .. } = outcome
        {
            self.generation_root = checkpoint_root;
            self.install_root_replacement(
                checkpoint_root, retained.0.proof.root,
                prepared_roots.take().expect("publication prepared its owned root copies"),
            );
            self.base_file_table = self.root.file_table;
            self.authority_head = Some(head);
            self.prepared_merge_parent = None;
            self.dependencies.proof().clear();
            self.last_commit = Some(LastCommit { operation_id, generation_id, head });
        }
        outcome
    }

    pub(crate) async fn retry_stale_commit<E, F, Fut>(
        &mut self,
        operation_id: OperationId,
        permit: PublicationPermit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        before_publication: &mut F,
        producer_custody: Option<&Arc<crate::kernel::ProofDataCustody>>,
        operation_context: Option<Digest>,
        operation_evidence: Option<acyclic_stream::OwnedBytes>,
        original_fingerprint_permit: Option<PublicationPermit>,
    ) -> crate::MeasuredResult<FsReceipt<Option<CheckoutCommitOutcome>>, E>
    where
        E: From<FsError> + Send,
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::MeasuredResult<FsReceipt<()>, E>> + ProviderTask,
    {
        crate::heap_future::in_heap_measured_with_error(move |budget| async move {
            cancellation
                .check()
                .map_err(|error| OperationFailure::before_work(E::from(FsError::from(error))))?;
            let resolved = self
                .volume
                .fs
                .inner
                .authority
                .find_operation(
                    volume_authority_id(self.volume.id),
                    operation_id,
                    budget,
                    cancellation,
                )
                .await
                .map_err(|failure| {
                    OperationFailure::new(E::from(FsError::Authority(failure.error)), *failure.work)
                })?;
            let Some(commit) = resolved.value else {
                return Ok(FsReceipt {
                    value: None,
                    work: resolved.work,
                });
            };
            let previous_sequence = commit.sequence.get().checked_sub(1).ok_or_else(|| {
                OperationFailure::new(E::from(FsError::InvalidAuthorityHistory), resolved.work)
            })?;
            let expected = Head {
                epoch: commit.epoch,
                sequence: Sequence::new(previous_sequence),
                digest: commit.previous_digest,
            };
            let retried = self
                .publish_pending_against(operation_id, operation_context, operation_evidence, original_fingerprint_permit, permit, expected, remaining(resolved.work, budget).map_err(|failure| failure.map_error(E::from))?, cancellation, before_publication, Some(commit), producer_custody)
                .await
                .map_err(|mut failure| {
                    *failure.work = resolved.work.checked_add(*failure.work).unwrap_or(resolved.work);
                    failure
                })?;
            Ok(FsReceipt {
                value: Some(retried.value),
                work: add(resolved.work, retried.work).map_err(|failure| failure.map_error(E::from))?,
            })
        }, budget, cancellation, E::from(FsError::Cancelled(crate::CancellationError)), None, |error| E::from(FsError::from(error)))
        .await
    }

    async fn resolve_clean_commit<W: FsWork>(
        &self, operation_id: OperationId, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<CheckoutCommitOutcome, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            #[cfg(not(target_arch = "wasm32"))]
            if accounting.native_work().is_some() { accounting.check()?; }
            if let Some(last) = self.last_commit {
                if last.operation_id != operation_id { return Err(FsError::NoPendingMutations); }
                if last.generation_id == GenerationId::new(self.generation_root.digest) {
                    return Ok(CheckoutCommitOutcome::AlreadyCommitted {
                        generation_id: last.generation_id, head: last.head,
                    });
                }
            }
            accounting.check()?;
            let head = self.authority_head.ok_or(FsError::WritableCheckoutRequiresHead)?;
            let resolved = match &mut accounting {
                #[cfg(not(target_arch = "wasm32"))]
                KernelAccounting::NativeOriginal { work: original, .. } => {
                    let (value, allocation) = self.volume.fs.inner.authority.find_operation_with_native_work(
                        volume_authority_id(self.volume.id), operation_id, original, cancellation,
                    ).await.map_err(|error| match error {
                        AuthorityStoreError::NativeOriginal(error) => FsError::NativeKernel(error),
                        error => FsError::Authority(error),
                    })?;
                    (value, KernelOwnedAllocation::NativeOriginal(allocation))
                }
                _ => match self.volume.fs.inner.authority.find_operation(
                    volume_authority_id(self.volume.id), operation_id, accounting.remaining()?, cancellation,
                ).await {
                    Ok(receipt) => {
                        accounting.record_completed(receipt.work)?;
                        (receipt.value, KernelOwnedAllocation::Standalone)
                    }
                    Err(failure) => {
                        accounting.record_completed(*failure.work)?;
                        return Err(FsError::Authority(failure.error));
                    }
                },
            };
            let commit = resolved.0.as_ref().ok_or(FsError::NoPendingMutations)?;
            let generation_root = generation_from_record_value_with_accounting(
                commit, self.volume.id, &mut accounting,
            )?;
            Ok(if generation_root == self.generation_root {
                CheckoutCommitOutcome::AlreadyCommitted {
                    generation_id: GenerationId::new(generation_root.digest), head,
                }
            } else {
                CheckoutCommitOutcome::IdempotencyConflict { committed_fingerprint: commit.fingerprint }
            })
        }.await;
        budget.finish(result, &accounting)
    }

    /// Resolves an exact path without following links or scanning unrelated subtrees.
    pub async fn lookup_no_follow<W: FsWork>(
        &mut self,
        path: &NamespacePath,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<PathLookup, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = self.lookup_no_follow_observed_with_accounting(
            path, &mut accounting, cancellation, true,
        ).await;
        let work = accounting.spent();
        let result = result.map(|(mut lookup, allocation)| {
            drop(allocation);
            lookup.work = work;
            lookup
        });
        budget.finish(result, &accounting)
    }

    /// Resolves one exact path and decodes its authenticated metadata without
    pub async fn lookup_no_follow_with_metadata<W: FsWork>(
        &mut self, path: &NamespacePath, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<Option<PathMetadataLookup>, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = self.lookup_no_follow_with_metadata_with_accounting(
            path, &mut accounting, cancellation,
        ).await;
        budget.finish(result, &accounting)
    }

    pub(crate) async fn lookup_no_follow_with_metadata_with_accounting(
        &mut self, path: &NamespacePath,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<PathMetadataLookup>, FsError> {
        let (lookup, allocation) = self.lookup_no_follow_observed_with_accounting(
            path, accounting, cancellation, false,
        ).await?;
        drop(allocation);
        let Some(record) = lookup.record else { return Ok(None); };
        let metadata = read_record_metadata_to_file_root_with_accounting(
            &self.volume, record, accounting, cancellation,
        ).await?;
        if self.tracks_observations() {
            self.observe_base_metadata_ids_with_accounting(
                std::slice::from_ref(&record.file_id), accounting, cancellation,
            ).await?;
        }
        Ok(Some(PathMetadataLookup { record, metadata }))
    }

    pub(crate) async fn lookup_no_follow_with_accounting(
        &mut self,
        path: &NamespacePath,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(PathLookup, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
        self.lookup_no_follow_observed_with_accounting(path, accounting, cancellation, true).await
    }

    async fn lookup_no_follow_observed_with_accounting(
        &mut self,
        path: &NamespacePath,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
        capture_terminal: bool,
    ) -> Result<(PathLookup, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
        accounting.check()?;
        let before = readonly_standalone_live(accounting);
        let result = async {
            self.synchronize_live_with_accounting(accounting, cancellation).await?;
            let mut lookup = self.lookup_no_follow_at_current_with_accounting(
                path, accounting, cancellation, capture_terminal,
            ).await?;
            if lookup.0.record.is_none()
                && self.volume.config.case_sensitivity == CaseSensitivity::ProfileFolded
            {
                if let Some(folded) = self.resolve_profile_folded_path_with_accounting(
                    path, accounting, cancellation, capture_terminal,
                ).await? {
                    lookup = folded;
                }
            }
            lookup.0.work = accounting.spent();
            Ok::<_, FsError>(lookup)
        }.await;
        readonly_cleanup_standalone(accounting, before, 0)?;
        result.map(|(mut lookup, allocation)| {
            lookup.work = accounting.spent();
            (lookup, allocation)
        })
    }

    /// Retries an exact miss through the canonical profile-folded name engine.
    async fn resolve_profile_folded_path_with_accounting(
        &mut self,
        path: &NamespacePath,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
        capture_terminal: bool,
    ) -> Result<Option<(PathLookup, crate::kernel::allocation::KernelOwnedAllocation)>, FsError> {
        accounting.check()?;
        if path.is_root() { return Ok(None); }
        let before = readonly_standalone_live(accounting);
        let result = async {
            let Some((value, allocation)) = self.canonicalize_path_components(
                path.components(), &[], accounting, cancellation,
            ).await? else { return Ok(None); };
            let corrected = (value, allocation);
            let retried = self.lookup_no_follow_at_current_with_accounting(
                &corrected.0, accounting, cancellation, capture_terminal,
            ).await?;
            drop(corrected);
            Ok::<_, FsError>(Some(retried))
        }.await;
        readonly_cleanup_standalone(accounting, before, 0)?;
        result
    }

    pub(crate) async fn lookup_no_follow_at_current_with_accounting(
        &mut self,
        path: &NamespacePath,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
        capture_terminal: bool,
    ) -> Result<(PathLookup, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
        accounting.check()?;
        let before = readonly_standalone_live(accounting);
        let result = async {
            let lookup = if self.tracks_observations() {
                let candidate = if self.has_pending_mutations() {
                    Some(crate::kernel::lookup_path_async_with_accounting(
                        &self.volume.fs.inner.objects, &self.root, path, self.volume.config,
                        accounting, cancellation, Some(&self.dependencies.data_custody),
                    ).await?)
                } else { None };
                let base = self.base_root.file_table;
                if let Some(candidate) = candidate
                    && self.dependencies.proof().observes_path(base, path, capture_terminal)
                {
                    return Ok((candidate, accounting.claim_owned(0, 0)?));
                }
                let (value, allocation) = self.observe_base_for_candidate_with_accounting(
                    path, candidate.is_some(), capture_terminal, accounting, cancellation,
                ).await?;
                let mut observed = (value, allocation);
                let base_lookup = observed.0.lookup;
                let dependencies = std::mem::take(&mut observed.0.dependencies);
                self.observe_staged_paths_with_accounting(
                    base, [path], capture_terminal, dependencies, &mut observed.1,
                    accounting, cancellation,
                )?;
                drop(observed);
                candidate.unwrap_or(base_lookup)
            } else {
                crate::kernel::lookup_path_async_with_accounting(
                    &self.volume.fs.inner.objects, &self.root, path, self.volume.config,
                    accounting, cancellation, Some(&self.dependencies.data_custody),
                ).await?
            };
            Ok((lookup, accounting.claim_owned(0, 0)?))
        }.await;
        readonly_cleanup_standalone(accounting, before, 0)?;
        result.map(|(mut lookup, allocation)| {
            lookup.work = accounting.spent();
            (lookup, allocation)
        })
    }

    async fn observe_base_for_candidate_with_accounting(
        &self,
        path: &NamespacePath,
        has_candidate: bool,
        capture_terminal: bool,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(crate::kernel::ObservedPathLookup, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
        let observed = crate::kernel::observe_path_async_with_accounting(
            &self.volume.fs.inner.objects, &self.base_root, path, self.volume.config,
            accounting, cancellation, Some(&self.dependencies.data_custody), capture_terminal,
        ).await;
        match observed {
            Ok(observed) => Ok(observed.into_parts()),
            Err(FsError::Path(PathLookupError::NotDirectory)) if has_candidate =>
                crate::kernel::observe_path_async_with_accounting(
                    &self.volume.fs.inner.objects, &self.base_root, path, self.volume.config,
                    accounting, cancellation, Some(&self.dependencies.data_custody), false,
                ).await.map(|observed| observed.into_parts()),
            Err(error) => Err(error),
        }
    }

    /// Resolves a bounded path batch while sharing directory and file-table frontiers.
    pub async fn lookup_batch_no_follow(
        &mut self,
        paths: &[NamespacePath],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<PathBatchLookup> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
        let result = self.lookup_batch_no_follow_with_accounting(
            paths, false, &mut accounting, cancellation,
        ).await;
        let work = accounting.spent();
        result.map(|(mut lookup, allocation)| {
            drop(allocation);
            lookup.work = work;
            FsReceipt { value: lookup, work }
        }).map_err(|error| OperationFailure::new(error, work))
    }

    pub(crate) async fn lookup_batch_no_follow_with_accounting(
        &mut self, paths: &[NamespacePath], selected_restore: bool,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(PathBatchLookup, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
        if selected_restore && (self.mode.access != AccessMode::ReadOnly
            || self.mode.consistency != ConsistencyMode::Pinned
            || self.tracks_observations() || *self.root != self.base_root)
        { return Err(FsError::MutationNotAllowed); }
        accounting.check()?;
        let before = readonly_standalone_live(accounting);
        let result = async {
            self.synchronize_live_with_accounting(accounting, cancellation).await?;
            let config = self.volume.config;
            let base = self.base_root.file_table;
            if !self.tracks_observations() {
                let lookup = if selected_restore {
                    crate::kernel::lookup_paths_selected_async_with_accounting(
                        &self.volume.fs.inner.objects, &self.root, paths, config, accounting, cancellation,
                        Some(&self.dependencies.data_custody),
                    ).await?
                } else {
                    crate::kernel::lookup_paths_async_with_accounting(
                        &self.volume.fs.inner.objects, &self.root, paths, config, accounting, cancellation,
                        Some(&self.dependencies.data_custody),
                    ).await?
                };
                return Ok(lookup.into_parts());
            }
            // The unchanged root answers and observes in the same shared walk.
            if *self.root == self.base_root {
                let (value, allocation) = crate::kernel::observe_paths_async_with_accounting(
                    &self.volume.fs.inner.objects, &self.root, paths, config, accounting, cancellation,
                    Some(&self.dependencies.data_custody),
                ).await?.into_parts();
                let mut observed = (value, allocation);
                let dependencies = std::mem::take(&mut observed.0.dependencies);
                self.observe_staged_paths_with_accounting(
                    base, paths, true, dependencies, &mut observed.1, accounting, cancellation,
                )?;
                let bytes = observed.0.lookup.entries.capacity().checked_mul(size_of::<crate::kernel::PathBatchEntry>())
                    .map(crate::foundation::usize_to_u64).ok_or(WorkError::Overflow)?;
                let output_allocation = match &mut observed.1 {
                    crate::kernel::allocation::KernelOwnedAllocation::Standalone =>
                        crate::kernel::allocation::KernelOwnedAllocation::Standalone,
                    #[cfg(not(target_arch = "wasm32"))]
                    crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) =>
                        crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation.split_off(bytes)?),
                };
                let entries = std::mem::take(&mut observed.0.lookup.entries);
                let lookup = PathBatchLookup { entries, retained_allocation_bytes: bytes, work: accounting.spent() };
                drop(observed);
                return Ok((lookup, output_allocation));
            }
            let (value, allocation) = crate::kernel::lookup_paths_async_with_accounting(
                &self.volume.fs.inner.objects, &self.root, paths, config, accounting, cancellation,
                Some(&self.dependencies.data_custody),
            ).await?.into_parts();
            let lookup = (value, allocation);
            let mut unobserved = crate::kernel::Owned::<Vec<&NamespacePath>>::allocate(0, accounting)?;
            {
                let proof = self.dependencies.proof();
                for path in paths {
                    let examined = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
                    accounting.admit(examined)?;
                    let observed = proof.observes_path(base, path, true);
                    accounting.record_completed(examined)?;
                    if !observed {
                        unobserved.ensure_for_push(paths.len(), accounting)?;
                        let copied = WorkCounters {
                            bytes_copied: size_of::<&NamespacePath>() as u64, ..WorkCounters::UNCHARGED
                        };
                        accounting.admit(copied)?;
                        unobserved.value.push(path);
                        accounting.record_completed(copied)?;
                    }
                }
            }
            if !unobserved.value.is_empty() {
                let (value, allocation) = crate::kernel::observe_path_refs_async_with_accounting(
                    &self.volume.fs.inner.objects, &self.base_root, &unobserved.value, config,
                    accounting, cancellation, Some(&self.dependencies.data_custody),
                ).await?.into_parts();
                let mut observed = (value, allocation);
                let dependencies = std::mem::take(&mut observed.0.dependencies);
                unobserved.retire(accounting)?;
                self.observe_staged_paths_with_accounting(
                    base, paths, true, dependencies, &mut observed.1, accounting, cancellation,
                )?;
                drop(observed);
            } else {
                unobserved.retire(accounting)?;
            }
            Ok((lookup.0, lookup.1))
        }.await;
        let retained = result.as_ref().map_or(0, |(lookup, _)| {
            (lookup.entries.capacity() * size_of::<crate::kernel::PathBatchEntry>()) as u64
        });
        readonly_cleanup_standalone(accounting, before, retained)?;
        result.map(|(mut lookup, allocation)| { lookup.work = accounting.spent(); (lookup, allocation) })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn lookup_batch_no_follow_with_native_work(
        &mut self, paths: &[NamespacePath], work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(PathBatchLookup, crate::NativeAllocation), FsError> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(work, cancellation);
        let (lookup, allocation) = self.lookup_batch_no_follow_with_accounting(
            paths, false, &mut accounting, cancellation,
        ).await?;
        Ok((lookup, native_pinned_allocation(allocation)?))
    }

    fn observe_staged_paths_with_accounting<'p>(
        &mut self, base: ObjectId, paths: impl IntoIterator<Item = &'p NamespacePath>,
        capture_terminal: bool, dependencies: Vec<Dependency>,
        allocation: &mut crate::kernel::allocation::KernelOwnedAllocation,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(staged) = allocation else {
                return Err(FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout));
            };
            return self.dependencies.observe_paths_with_native_work(
                base, paths, capture_terminal, dependencies, staged,
                self.volume.config.limits.maximum_checkout_dependencies, original, cancellation,
            );
        }
        let (_, work, budget, cancellation) = accounting.standalone_parts()
            .ok_or(FsError::UnsupportedCheckout)?;
        self.dependencies.observe_paths(
            base, paths, capture_terminal, dependencies,
            self.volume.config.limits.maximum_checkout_dependencies, work, budget, cancellation,
        )
    }

    /// Returns one bounded authenticated page from a directory.
    pub async fn list_directory(
        &mut self,
        path: &NamespacePath,
        after: Option<&LogicalName>,
        maximum_entries: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<DirectoryPage> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
        let result = self.list_directory_with_accounting(
            path, after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded), maximum_entries, &mut accounting, cancellation,
        ).await;
        let work = accounting.spent();
        result.map(|(mut page, allocation)| {
            drop(allocation);
            page.work = work;
            FsReceipt { value: page, work }
        }).map_err(|error| OperationFailure::new(error, work))
    }

    pub(crate) async fn list_directory_with_accounting(
        &mut self,
        path: &NamespacePath,
        after: std::ops::Bound<&LogicalName>,
        maximum_entries: u32,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(DirectoryPage, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
        if matches!(after, std::ops::Bound::Included(_)) && self.tracks_observations() {
            return Err(FsError::UnsupportedCheckout);
        }
        accounting.check()?;
        let before = readonly_standalone_live(accounting);
        // The future is destroyed before its exact physical frame claim.
        let mut frame_allocation = None;
        let (mut future, frame_bytes) = crate::heap_future::in_heap_admitted(
            (&mut *self, path, after, &mut *accounting, cancellation),
            |(_, _, _, accounting, _), bytes| {
                frame_allocation = Some(accounting.claim_owned(bytes, u64::from(bytes != 0))?);
                Ok::<_, FsError>(())
            },
            |(this, path, after, accounting, cancellation)| async move {
                let (lookup, lookup_allocation) = this.lookup_no_follow_observed_with_accounting(
                    path, accounting, cancellation, false,
                ).await?;
                drop(lookup_allocation);
                let record = lookup.record.ok_or(FsError::NotFound)?;
                let FilePayload::Directory { entries } = record.payload else {
                    return Err(FsError::NotDirectory);
                };
                if record.kind != FileKind::Directory { return Err(FsError::NotDirectory); }
                let (page, allocation) = if let std::ops::Bound::Included(at) = after {
                    crate::kernel::list_tree_entries_at_or_after_async_with_accounting(
                        &this.volume.fs.inner.objects, entries, at, maximum_entries,
                        DecodeLimits::for_volume(this.volume.config), accounting, cancellation,
                        Some(&this.dependencies.data_custody),
                    ).await?
                } else {
                    crate::kernel::list_tree_entries_async_with_accounting(
                        &this.volume.fs.inner.objects, entries,
                        match after { std::ops::Bound::Excluded(name) => Some(name), _ => None }, maximum_entries,
                        DecodeLimits::for_volume(this.volume.config), accounting, cancellation.clone(),
                        Some(&this.dependencies.data_custody),
                    ).await?
                };
                let page = (page, allocation);
                if this.tracks_observations() {
                    let (cursor, allocation) = match after {
                        std::ops::Bound::Excluded(name) => {
                            let (name, allocation) = this.dependencies.copy_observation_name_with_accounting(
                                name, accounting, cancellation,
                            )?;
                            (Some(name), allocation)
                        }
                        std::ops::Bound::Unbounded => (None, accounting.claim_owned(0, 0)?),
                        std::ops::Bound::Included(_) => unreachable!("included cursors cannot track observations"),
                    };
                    let mut region = (DependencyRegion::DirectoryRange {
                            directory_id: record.file_id, after: cursor, maximum_entries,
                        }, allocation);
                    let (probe, allocation) = AuthenticatedGenerationProbe::new_with_accounting(
                        &this.volume.fs.inner.objects, probe_limits(this.volume.config), accounting,
                    )?;
                    let probe = (probe.with_data_custody(Some(&this.dependencies.data_custody)), allocation);
                    let state = probe.0.probe_async_with_accounting(
                        GenerationId::new(this.root.base().digest), &region.0, accounting, cancellation,
                    ).await?;
                    let (probe, allocation) = probe;
                    probe.retire_with_accounting(accounting)?;
                    drop(allocation);
                    let mut staged = crate::kernel::Owned::<Vec<Dependency>>::allocate(1, accounting)?;
                    staged.allocation.absorb(&mut region.1)?;
                    let copied = WorkCounters {
                        items_examined: 1, bytes_copied: size_of::<Dependency>() as u64,
                        ..WorkCounters::UNCHARGED
                    };
                    accounting.admit(copied)?;
                    staged.value.push(Dependency { region: region.0, expected: state });
                    accounting.record_completed(copied)?;
                    this.extend_observations_with_accounting(staged, accounting, cancellation)?;
                }
                Ok((page.0, page.1))
            },
        )?;
        let result = future.as_mut().await;
        drop(future);
        if let Some(mut allocation) = frame_allocation {
            accounting.release_owned(&mut allocation, frame_bytes)?;
        }
        let retained = result.as_ref().map_or(0, |(page, _)| {
            (page.entries.capacity() * size_of::<crate::kernel::TreeEntry>()) as u64
                + page.entries.iter().map(|entry| entry.name.retained_bytes() as u64).sum::<u64>()
        });
        readonly_cleanup_standalone(accounting, before, retained)?;
        result.map(|(mut page, allocation)| { page.work = accounting.spent(); (page, allocation) })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn list_directory_with_native_work(
        &mut self, path: &NamespacePath, after: Option<&LogicalName>, maximum_entries: u32,
        work: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(DirectoryPage, crate::NativeAllocation), FsError> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(work, cancellation);
        let (page, allocation) = self.list_directory_with_accounting(
            path, after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded), maximum_entries, &mut accounting, cancellation,
        ).await?;
        Ok((page, native_pinned_allocation(allocation)?))
    }

    /// Returns one directory page with all child records fetched in one shared
    pub async fn list_directory_records(
        &mut self,
        path: &NamespacePath,
        after: std::ops::Bound<&LogicalName>,
        maximum_entries: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<DirectoryRecordPage> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
        let result = self.list_directory_records_with_accounting(
            path, after, maximum_entries, &mut accounting, cancellation,
        ).await;
        let work = accounting.spent();
        result.map(|(value, allocation)| {
            drop(allocation);
            FsReceipt { value, work }
        }).map_err(|error| OperationFailure::new(error, work))
    }

    pub(crate) async fn list_directory_records_with_accounting(
        &mut self, path: &NamespacePath, after: std::ops::Bound<&LogicalName>,
        maximum_entries: u32, accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(DirectoryRecordPage, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
        let before = readonly_standalone_live(accounting);
        let result = async {
            let (page, allocation) = self.list_directory_with_accounting(
                path, after, maximum_entries, accounting, cancellation,
            ).await?;
            let (value, allocation) = attach_directory_records_to_file_root_with_accounting(
                &self.volume, self.root.file_table, page, allocation, maximum_entries, accounting, cancellation,
                Some(&self.dependencies.data_custody),
            ).await?;
            let page = (value, allocation);
            if self.tracks_observations() && !page.0.entries.is_empty() {
                let mut ids = crate::kernel::Owned::<Vec<FileId>>::allocate(page.0.entries.len(), accounting)?;
                let copied = WorkCounters {
                    bytes_copied: page.0.entries.len().checked_mul(size_of::<FileId>())
                        .map(crate::foundation::usize_to_u64).ok_or(WorkError::Overflow)?,
                    ..WorkCounters::UNCHARGED
                };
                accounting.admit(copied)?;
                ids.value.extend(page.0.entries.iter().map(|entry| entry.record.file_id));
                accounting.record_completed(copied)?;
                self.observe_base_metadata_ids_with_accounting(&ids.value, accounting, cancellation).await?;
                ids.retire(accounting)?;
            }
            Ok((page.0, page.1))
        }.await;
        let retained = match &result {
            Ok((page, _)) if before.is_some() => directory_record_page_retained_bytes(page)?,
            _ => 0,
        };
        readonly_cleanup_standalone(accounting, before, retained)?;
        result
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn list_directory_records_with_native_work(
        &mut self, path: &NamespacePath, after: Option<&LogicalName>, maximum_entries: u32,
        work: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(DirectoryRecordPage, crate::NativeAllocation), FsError> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(work, cancellation);
        let (page, allocation) = self.list_directory_records_with_accounting(
            path, after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded), maximum_entries, &mut accounting, cancellation,
        ).await?;
        Ok((page, native_pinned_allocation(allocation)?))
    }

    /// Reads complete canonical metadata for one exact path.
    pub async fn read_metadata<W: FsWork>(
        &mut self, path: &NamespacePath, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<FileMetadata, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = self.read_metadata_with_accounting(path, &mut accounting, cancellation).await;
        budget.finish(result, &accounting)
    }

    pub(crate) async fn read_metadata_with_accounting(
        &mut self, path: &NamespacePath,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<FileMetadata, FsError> {
        self.lookup_no_follow_with_metadata_with_accounting(path, accounting, cancellation).await?
            .map(|lookup| lookup.metadata).ok_or(FsError::NotFound)
    }

    /// Reads the same exact named attribute, preserving its producer's actual
    pub async fn read_named_attribute<W: FsWork>(
        &mut self, path: &NamespacePath, name: &AttributeName,
        budget: W, cancellation: &CancellationToken,
    ) -> W::Output<Option<crate::kernel::OwnedSourceChunk>, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            let accounting = &mut accounting;
            let metadata = self.read_metadata_with_accounting(path, accounting, cancellation).await?;
            read_named_attribute_to_file_root_with_accounting(&self.volume, metadata, name, accounting, cancellation).await
        }.await;
        budget.finish(result, &accounting)
    }

    /// Lists one bounded page of exact named-attribute identities without
    pub async fn list_named_attributes(
        &mut self, path: &NamespacePath, after: Option<&AttributeName>, maximum_entries: u32,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> FsResult<AttributeListing> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
        let result = self.list_named_attributes_with_accounting(path, after, maximum_entries, &mut accounting, cancellation).await;
        let work = accounting.spent();
        result.map(|(mut value, allocation)| {
            drop(allocation); value.work = work; FsReceipt { value, work }
        }).map_err(|error| OperationFailure::new(error, work))
    }

    pub(crate) async fn list_named_attributes_with_accounting(
        &mut self, path: &NamespacePath, after: Option<&AttributeName>, maximum_entries: u32,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<(AttributeListing, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
        let metadata = self.read_metadata_with_accounting(path, accounting, cancellation).await?;
        list_named_attributes_to_file_root_with_accounting(&self.volume, metadata, after, maximum_entries, accounting, cancellation).await
    }

    /// Changes one exact named attribute atomically.
    pub async fn change_named_attribute(
        &mut self, path: NamespacePath, name: &AttributeName,
        value: Option<(&[u8], NamedAttributeWriteMode)>,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> FsResult<()> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let lookup = self.lookup_no_follow_with_metadata_with_accounting(
                &path, &mut accounting, cancellation,
            ).await?.ok_or(FsError::NotFound)?;
            let metadata = Self::change_named_attribute_metadata_with_accounting(
                &self.volume, lookup.metadata, name, value,
                Some(self.volume.config.limits.maximum_read_bytes), &mut accounting, cancellation,
            ).await?;
            self.set_attributes_by_id_with_accounting(lookup.record.file_id, metadata, None, &mut accounting, cancellation).await
        }.await;
        measured(result, &accounting)
    }

    /// Plans the sparse physical representation of one exact regular-file range.
    pub async fn plan_file_extents(
        &mut self,
        path: &NamespacePath,
        range: ByteRange,
        maximum_spans: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Option<ExtentPlan>> {
        if maximum_spans == 0 || range.length > self.volume.config.limits.maximum_generation_bytes {
            return Err(OperationFailure::before_work(FsError::FileRead(
                FileRangeReadError::InvalidRange,
            )));
        }
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let (lookup, allocation) = self.lookup_no_follow_observed_with_accounting(
                path, &mut accounting, cancellation, false,
            ).await?;
            drop(allocation);
            let record = lookup.record.ok_or(FsError::NotFound)?;
            self.plan_record_extents(record, range, maximum_spans, &mut accounting, cancellation).await
                .map(|(plan, allocation)| { drop(allocation); plan })
        }.await;
        measured(result, &accounting)
    }

    /// Finds the first sparse data or hole boundary after one logical offset.
    pub async fn seek_file_extent(
        &mut self, path: &NamespacePath, offset: u64, target: ExtentSeekTarget,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> FsResult<Option<u64>> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let accounting = &mut accounting;
            let (lookup, allocation) = self.lookup_no_follow_observed_with_accounting(
                path, accounting, cancellation, false,
            ).await?;
            drop(allocation);
            let record = lookup.record.ok_or(FsError::NotFound)?;
            let value = seek_record_extent_with_accounting(&self.volume, record, offset, target, accounting, cancellation).await?;
            if self.tracks_observations() {
                self.observe_identity_regions_with_accounting(
                    [Some(DependencyRegion::SparseSeek { file_id: record.file_id, offset, target }), None],
                    accounting, cancellation,
                ).await?;
            }
            Ok(value)
        }.await;
        measured(result, &accounting)
    }

    /// Reads one exact logical regular-file range without materializing holes
    pub async fn read_file_range<W: FsWork>(
        &mut self,
        path: &NamespacePath,
        range: ByteRange,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<FileRangeRead, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = self.read_file_range_with_accounting(path, range, &mut accounting, cancellation).await;
        budget.finish(result, &accounting)
    }

    pub(crate) async fn read_file_range_with_accounting(
        &mut self, path: &NamespacePath, range: ByteRange,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<FileRangeRead, FsError> {
        if range.length > self.volume.config.limits.maximum_read_bytes {
            return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
        }
        let (lookup, allocation) = self.lookup_no_follow_observed_with_accounting(
            path, accounting, cancellation, false,
        ).await?;
        drop(allocation);
        let record = lookup.record.ok_or(FsError::NotFound)?;
        self.read_record_range_with_accounting(record, range, range, accounting, cancellation).await
    }

    async fn lookup_file_record_by_id_with_accounting(
        &self, file_id: FileId, accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<FileRecord>, FsError> {
        let before = readonly_standalone_live(accounting);
        let result = crate::kernel::lookup_file_record_async_with_accounting(
            &self.volume.fs.inner.objects, self.root.file_table, file_id,
            DecodeLimits::for_volume(self.volume.config), accounting, cancellation,
            Some(&self.dependencies.data_custody),
        ).await;
        readonly_cleanup_standalone(accounting, before, 0)?;
        result.map(|(record, allocation)| { drop(allocation); record })
    }

    async fn observe_identity_regions_with_accounting(
        &mut self, regions: [Option<DependencyRegion>; 2],
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let before = readonly_standalone_live(accounting);
        let result = async {
            let count = regions.iter().flatten().count();
            let mut requested = crate::kernel::Owned::<Vec<DependencyRegion>>::allocate(count, accounting)?;
            for region in regions.into_iter().flatten() {
                let copied = WorkCounters {
                    bytes_copied: size_of::<DependencyRegion>() as u64, ..WorkCounters::UNCHARGED
                };
                accounting.admit(copied)?;
                requested.value.push(region);
                accounting.record_completed(copied)?;
            }
            let (probe, allocation) = AuthenticatedGenerationProbe::new_with_accounting(
                &self.volume.fs.inner.objects, probe_limits(self.volume.config), accounting,
            )?;
            let probe = (probe.with_data_custody(Some(&self.dependencies.data_custody)), allocation);
            let input = std::mem::take(&mut requested.value);
            let captured = probe.0.capture_many_with_accounting(
                GenerationId::new(self.root.base().digest), input,
                self.volume.config.limits.maximum_checkout_dependencies, accounting, cancellation,
            ).await;
            // The canonical capture consumes the input backing before returning.
            requested.retire(accounting)?;
            let (value, allocation) = captured?;
            let bytes = value.capacity().checked_mul(size_of::<Dependency>())
                .map(crate::foundation::usize_to_u64).ok_or(WorkError::Overflow)?;
            let captured = crate::kernel::Owned::new(value, allocation, bytes);
            let (probe, allocation) = probe;
            probe.retire_with_accounting(accounting)?;
            drop(allocation);
            self.extend_observations_with_accounting(captured, accounting, cancellation)
        }.await;
        readonly_cleanup_standalone(accounting, before, 0)?;
        result
    }



    async fn observe_base_regular_range_with_accounting(
        &mut self, file_id: FileId, range: ByteRange,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let before = readonly_standalone_live(accounting);
        let result = async {
            let (record, allocation) = crate::kernel::lookup_file_record_async_with_accounting(
                &self.volume.fs.inner.objects, self.base_root.file_table, file_id,
                DecodeLimits::for_volume(self.volume.config), accounting, cancellation,
                Some(&self.dependencies.data_custody),
            ).await?;
            drop(allocation);
            self.observe_identity_regions_with_accounting(
                regular_range_regions(record, range.offset, range.length), accounting, cancellation,
            ).await
        }.await;
        readonly_cleanup_standalone(accounting, before, 0)?;
        result
    }

    async fn observe_base_metadata_ids_with_accounting(
        &mut self, file_ids: &[FileId],
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let before = readonly_standalone_live(accounting);
        let result = async {
            accounting.check()?;
            let selected = {
            let proof = self.dependencies.proof();
            let mut count = 0_usize;
            for file_id in file_ids {
                let examined = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
                accounting.admit(examined)?;
                count += usize::from(!proof.observes(&DependencyRegion::Metadata(*file_id)));
                accounting.record_completed(examined)?;
            }
            let mut selected = crate::kernel::Owned::<Vec<FileId>>::allocate(count, accounting)?;
            for file_id in file_ids {
                let examined = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
                accounting.admit(examined)?;
                let observed = proof.observes(&DependencyRegion::Metadata(*file_id));
                accounting.record_completed(examined)?;
                if !observed {
                    let copied = WorkCounters { bytes_copied: size_of::<FileId>() as u64, ..WorkCounters::UNCHARGED };
                    accounting.admit(copied)?;
                    selected.value.push(*file_id);
                    accounting.record_completed(copied)?;
                }
            }
                selected
            };
            if selected.value.is_empty() { return selected.retire(accounting); }
            let maximum = u32::try_from(selected.value.len()).unwrap_or(u32::MAX);
            let (records, allocation) = crate::kernel::lookup_file_records_async_with_accounting(
                &self.volume.fs.inner.objects, self.base_root.file_table, &selected.value, maximum,
                DecodeLimits::for_volume(self.volume.config), accounting, cancellation,
                Some(&self.dependencies.data_custody),
            ).await?;
            let records = (records, allocation);
            let mut dependencies = crate::kernel::Owned::<Vec<Dependency>>::allocate(selected.value.len(), accounting)?;
            for (file_id, record) in selected.value.iter().zip(&records.0) {
                let copied = WorkCounters {
                    items_examined: 1, bytes_copied: size_of::<Dependency>() as u64,
                    ..WorkCounters::UNCHARGED
                };
                accounting.admit(copied)?;
                dependencies.value.push(Dependency {
                    region: DependencyRegion::Metadata(*file_id),
                    expected: record.map_or(DependencyState::Absent, |record| DependencyState::Present(record.metadata.digest)),
                });
                accounting.record_completed(copied)?;
            }
            drop(records);
            selected.retire(accounting)?;
            self.extend_observations_with_accounting(dependencies, accounting, cancellation)
        }.await;
        readonly_cleanup_standalone(accounting, before, 0)?;
        result
    }

    fn extend_observations_with_accounting(
        &mut self,
        mut dependencies: crate::kernel::Owned<Vec<Dependency>>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let values = std::mem::take(&mut dependencies.value);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(staged) = &mut dependencies.allocation else {
                return Err(FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout));
            };
            return self.dependencies.extend_observations_with_native_work(
                values, staged, self.volume.config.limits.maximum_checkout_dependencies, original, cancellation,
            );
        }
        let (_, work, budget, cancellation) = accounting.standalone_parts()
            .ok_or(FsError::UnsupportedCheckout)?;
        self.dependencies.extend_observations(
            values, self.volume.config.limits.maximum_checkout_dependencies, work, budget, cancellation,
        )
    }
}

pub(crate) async fn stage_content_for_volume_with_accounting<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    R: AsyncBlobSource,
>(
    volume: &Volume<A, O>,
    source: &mut R,
    maximum_source_bytes: u64,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<StagedContent, FsError> {
    if maximum_source_bytes == 0
        || maximum_source_bytes > volume.config.limits.maximum_generation_bytes
    {
        return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
    }
    accounting.check()?;
    cancellation.check()?;
    let chunk_bytes = u32::try_from(volume.config.limits.maximum_object_bytes.min(1024 * 1024))
        .unwrap_or(u32::MAX)
        .max(1);
    let page_bytes = u32::try_from(volume.config.limits.maximum_object_bytes)
        .unwrap_or(u32::MAX)
        .max(1);
    let blob = crate::kernel::build_blob_with_accounting(
        &volume.fs.inner.objects,
        source,
        BlobBuildOptions {
            chunk_bytes,
            page_items: volume.config.limits.maximum_directory_page_entries,
            page_bytes,
            maximum_blob_bytes: maximum_source_bytes,
        },
        accounting,
        cancellation,
    ).await?;
    Ok(StagedContent { root: blob.root, logical_bytes: blob.logical_bytes })
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> DetachedFile<A, O> {

    pub(crate) fn from_record<W: FsWork>(
        volume: Volume<A, O>, record: FileRecord, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<Self, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = (|| -> Result<Self, FsError> {
            accounting.check()?;
            let hold = crate::Collection::hold_record_with_accounting(
                volume.fs.inner.objects.collection(), record, volume.config, &mut accounting,
            )?;
            Ok(Self { volume, record, _hold: hold })
        })();
        budget.finish(result, &accounting)
    }

    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        self._hold.as_ref().map_or(0, crate::collection::Hold::logical_owned_bytes)
    }

    #[cfg(feature = "native-mount")]
    pub(crate) const fn record(&self) -> FileRecord {
        self.record
    }

    /// Reads the same detached target while retaining producer-owned capacity.
    pub(crate) async fn read_symbolic_link<W: FsWork>(
        &self,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::kernel::OwnedSourceChunk, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = read_symbolic_link_record_with_accounting(
            &self.volume, self.record, &mut accounting, cancellation,
        ).await;
        budget.finish(result, &accounting)
    }

    /// Stable file identity retained across last-link removal.
    #[must_use]
    pub const fn file_id(&self) -> FileId {
        self.record.file_id
    }

    /// Current detached logical file length.
    #[must_use]
    pub fn logical_bytes(&self) -> u64 {
        if matches!(self.record.payload, FilePayload::InlineRegular(_) | FilePayload::Regular { .. }) {
            record_logical_bytes(self.record)
        } else { 0 }
    }

    /// Finds the first sparse data or hole boundary without a namespace binding.
    pub async fn seek<W: FsWork>(
        &self,
        offset: u64,
        target: ExtentSeekTarget,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<u64>, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = seek_record_extent_with_accounting(&self.volume, self.record, offset, target, &mut accounting, cancellation).await;
        budget.finish(result, &accounting)
    }

    /// Replaces complete metadata and optionally logical size as one detached
    pub async fn set_attributes<W: FsWork>(
        &mut self, metadata: FileMetadata, logical_bytes: Option<u64>,
        budget: W, cancellation: &CancellationToken,
    ) -> W::Output<(), FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = self.set_attributes_with_accounting(metadata, logical_bytes, &mut accounting, cancellation).await;
        budget.finish(result, &accounting)
    }

    async fn set_attributes_with_accounting(
        &mut self, metadata: FileMetadata, logical_bytes: Option<u64>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| crate::kernel::encode_file_metadata(metadata, observe, allocation_error))?;
        let metadata_id = self.volume.fs.put_encoded_with_accounting(
            ObjectKind::Metadata, encoded, accounting, cancellation,
        ).await?;
        let payload = if let Some(logical_bytes) = logical_bytes {
            crate::kernel::apply_regular_mutation_with_accounting(
                &self.volume.fs.inner.objects, self.record.payload,
                RegularMutation::Resize { logical_bytes }, self.volume.config, accounting, cancellation,
            ).await?.payload
        } else { self.record.payload };
        self.record.metadata = metadata_id;
        self.record.payload = payload;
        Ok(())
    }

    /// Reads one exact detached attribute with its producer allocation custody.
    pub async fn read_named_attribute<W: FsWork>(
        &self,
        name: &AttributeName,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<crate::kernel::OwnedSourceChunk>, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            let accounting = &mut accounting;
            let metadata = read_record_metadata_to_file_root_with_accounting(
                &self.volume, self.record, accounting, cancellation,
            ).await?;
            read_named_attribute_to_file_root_with_accounting(
                &self.volume, metadata, name, accounting, cancellation,
            ).await
        }.await;
        budget.finish(result, &accounting)
    }

    /// Lists one bounded page of detached named-attribute identities.
    pub async fn list_named_attributes(
        &self,
        after: Option<&AttributeName>,
        maximum_entries: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<AttributeListing> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
        let result = self.list_named_attributes_with_accounting(
            after, maximum_entries, &mut accounting, cancellation,
        ).await;
        let work = accounting.spent();
        result.map(|(value, _allocation)| FsReceipt { value, work })
            .map_err(|error| OperationFailure::new(error, work))
    }

    pub(crate) async fn list_named_attributes_with_accounting(
        &self, after: Option<&AttributeName>, maximum_entries: u32,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(AttributeListing, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
        let metadata = read_record_metadata_to_file_root_with_accounting(
            &self.volume, self.record, accounting, cancellation,
        ).await?;
        list_named_attributes_to_file_root_with_accounting(
            &self.volume, metadata, after, maximum_entries, accounting, cancellation,
        ).await
    }

    /// Changes one exact named attribute atomically.
    pub async fn change_named_attribute<W: FsWork>(
        &mut self, name: &AttributeName, value: Option<(&[u8], NamedAttributeWriteMode)>,
        budget: W, cancellation: &CancellationToken,
    ) -> W::Output<FileMetadata, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            let accounting = &mut accounting;
            let metadata = read_record_metadata_to_file_root_with_accounting(
                &self.volume, self.record, accounting, cancellation,
            ).await?;
            let metadata = Checkout::<A, O>::change_named_attribute_metadata_with_accounting(
                &self.volume, metadata, name, value, None, accounting, cancellation,
            ).await?;
            self.set_attributes_with_accounting(metadata, None, accounting, cancellation).await?;
            Ok(metadata)
        }.await;
        budget.finish(result, &accounting)
    }

    /// Reads complete authenticated metadata for this detached file.
    pub async fn read_metadata<W: FsWork>(
        &self,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<FileMetadata, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = read_record_metadata_to_file_root_with_accounting(
            &self.volume, self.record, &mut accounting, cancellation,
        ).await;
        budget.finish(result, &accounting)
    }

    /// Reads one bounded sparse range without requiring a namespace binding.
    pub async fn read_range<W: FsWork>(
        &self,
        range: ByteRange,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<FileRangeRead, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = self.read_range_with_accounting(range, &mut accounting, cancellation).await;
        budget.finish(result, &accounting)
    }

    async fn read_range_with_accounting(
        &self,
        range: ByteRange,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<FileRangeRead, FsError> {
        if range.length > self.volume.config.limits.maximum_read_bytes {
            return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
        }
        crate::kernel::read_file_range_with_accounting(
            &self.volume.fs.inner.objects,
            FileRangeRequest {
                record: self.record,
                range,
                maximum_spans: self.volume.config.limits.maximum_mutations_per_batch,
                limits: DecodeLimits::for_volume(self.volume.config),
                budget: accounting.remaining()?,
            },
            accounting,
            cancellation,
        ).await
    }

    /// Reads at most `maximum_bytes`, clipping to this detached record's EOF.
    pub async fn read_up_to(
        &self,
        offset: u64,
        maximum_bytes: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<FileRangeRead> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let accounting = &mut accounting;
            let maximum_bytes = u64::from(maximum_bytes);
            if maximum_bytes > self.volume.config.limits.maximum_read_bytes {
                return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
            }
            accounting.check()?;
            if self.record.kind != FileKind::Regular {
                return Err(FsError::FileRead(FileRangeReadError::NotRegular));
            }
            let logical_bytes = match self.record.payload {
                FilePayload::InlineRegular(data) => u64::try_from(data.as_bytes().len()).unwrap_or(u64::MAX),
                FilePayload::Regular { logical_bytes, .. } => logical_bytes,
                _ => return Err(FsError::FileRead(FileRangeReadError::NotRegular)),
            };
            let length = maximum_bytes.min(logical_bytes.saturating_sub(offset));
            if length == 0 {
                return Ok(FileRangeRead {
                    bytes: crate::kernel::owned_read_bytes((Vec::new(), accounting.claim_owned(0, 0)?).into())?,
                    logical_owned_bytes: 0,
                    work: accounting.spent(),
                });
            }
            self.read_range_with_accounting(ByteRange { offset, length }, accounting, cancellation).await
        }.await;
        measured(result, &accounting)
    }

    /// Applies one content change and, when `metadata` is given, replaces
    pub async fn change_content<W: FsWork>(
        &mut self,
        change: ContentChange<std::convert::Infallible>,
        metadata: Option<FileMetadata>,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            let accounting = &mut accounting;
            accounting.check()?;
            let mutation = match change {
                ContentChange::Write { bytes, .. } if bytes.is_empty() => None,
                ContentChange::Write { offset, bytes } => {
                    if bytes.len() as u64 > self.volume.config.limits.maximum_read_bytes {
                        return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
                    }
                    let mut source = std::io::Cursor::new(bytes.as_ref());
                    let content = stage_content_for_volume_with_accounting(
                        &self.volume, &mut source, (bytes.len() as u64).max(1),
                        accounting, cancellation,
                    ).await;
                    drop(source);
                    drop(bytes);
                    let content = content?;
                    Some(RegularMutation::Write {
                        offset, length: content.logical_bytes, content: content.root, content_offset: 0,
                    })
                }
                ContentChange::Resize { logical_bytes } => Some(RegularMutation::Resize { logical_bytes }),
                ContentChange::ZeroRange { range, allocated, extend } => Some(RegularMutation::ZeroRange {
                    offset: range.offset, length: range.length, allocated, extend,
                }),
                ContentChange::Preallocate { range, keep_size } => Some(RegularMutation::Preallocate {
                    offset: range.offset, length: range.length, keep_size,
                }),
                ContentChange::CloneFrom { source, .. } => match source {},
            };
            let payload = match mutation {
                Some(mutation) => crate::kernel::apply_regular_mutation_with_accounting(
                    &self.volume.fs.inner.objects, self.record.payload, mutation,
                    self.volume.config, accounting, cancellation,
                ).await?.payload,
                None => self.record.payload,
            };
            let metadata_id = match metadata {
                Some(metadata) => {
                    let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| crate::kernel::encode_file_metadata(metadata, observe, allocation_error))?;
                    self.volume.fs.put_encoded_with_accounting(
                        ObjectKind::Metadata, encoded, accounting, cancellation,
                    ).await?
                }
                None => self.record.metadata,
            };
            accounting.check()?;
            self.record.payload = payload;
            self.record.metadata = metadata_id;
            Ok(())
        }.await;
        budget.finish(result, &accounting)
    }

}

pub(crate) async fn read_generation_root<O: AsyncObjectStore>(
    objects: &O,
    generation_root: ObjectId,
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(GenerationRoot, WorkCounters), OperationFailure<FsError>> {
    let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
    let result = crate::kernel::read_generation_root_with_accounting(
        objects, generation_root, config, &mut accounting, cancellation,
    ).await;
    let work = accounting.spent();
    result.map(|(root, ownership)| {
        match ownership {
            crate::kernel::allocation::KernelOwnedAllocation::Standalone => (),
            #[cfg(not(target_arch = "wasm32"))]
            crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(_) => unreachable!(
                "standalone generation reading cannot create native custody",
            ),
        }
        (root, work)
    }).map_err(|error| OperationFailure::new(error, work))
}

fn exact_mutation_regions(
    mutation: &Mutation,
    first: Option<FileRecord>,
    second: Option<FileRecord>,
) -> [Option<(Option<FileRecord>, DependencyRegion)>; 4] {
    let mut regions = [None, None, None, None];
    match mutation {
        Mutation::Create { .. } | Mutation::Link { .. } => {}
        Mutation::Restore { .. } => {
            regions[0] =
                first.map(|record| (Some(record), DependencyRegion::FileRecord(record.file_id)));
        }
        Mutation::Remove { .. } => {
            regions[0] =
                first.map(|record| (Some(record), DependencyRegion::FileRecord(record.file_id)));
        }
        Mutation::Rename { .. } => {
            regions[0] =
                second.map(|record| (Some(record), DependencyRegion::FileRecord(record.file_id)));
        }
        Mutation::SetMetadata { .. } => {
            regions[0] =
                first.map(|record| (Some(record), DependencyRegion::Metadata(record.file_id)));
        }
        Mutation::Write { offset, length, .. } | Mutation::ZeroRange { offset, length, .. } => {
            let [content, file_length] = regular_range_regions(first, *offset, *length);
            regions[0] = content.map(|region| (first, region));
            regions[1] = file_length.map(|region| (first, region));
        }
        Mutation::ValidateRegular { .. } => {
            regions[0] =
                first.map(|record| (Some(record), DependencyRegion::FileRecord(record.file_id)));
        }
        Mutation::Preallocate { .. } => {
            regions[0] =
                first.map(|record| (Some(record), DependencyRegion::FileLength(record.file_id)));
        }
        Mutation::Resize { .. } => {
            // Resize can discard an arbitrarily large suffix. Until a compact
            regions[0] =
                first.map(|record| (Some(record), DependencyRegion::FileRecord(record.file_id)));
        }
        Mutation::CloneRange {
            source_offset,
            destination_offset,
            length,
            ..
        } => {
            let [source_content, source_length] =
                regular_range_regions(first, *source_offset, *length);
            let [destination_content, destination_length] =
                regular_range_regions(second, *destination_offset, *length);
            regions[0] = source_content.map(|region| (first, region));
            regions[1] = source_length.map(|region| (first, region));
            regions[2] = destination_content.map(|region| (second, region));
            regions[3] = destination_length.map(|region| (second, region));
        }
        Mutation::File { file_id, mutation } => match mutation {
            FileMutation::SetMetadata { .. } => {
                regions[0] = Some((first, DependencyRegion::Metadata(*file_id)));
            }
            FileMutation::Write { offset, length, .. }
            | FileMutation::ZeroRange { offset, length, .. } => {
                let [content, file_length] = regular_range_regions(first, *offset, *length);
                regions[0] = content.map(|region| (first, region));
                regions[1] = file_length.map(|region| (first, region));
            }
            FileMutation::Preallocate { .. } => {
                regions[0] = Some((first, DependencyRegion::FileLength(*file_id)));
            }
            FileMutation::ReplaceRecord { .. }
            | FileMutation::ValidateRegular
            | FileMutation::Resize { .. } => {
                regions[0] = Some((first, DependencyRegion::FileRecord(*file_id)));
            }
        },
        Mutation::CloneFileRange {
            source_file_id,
            source_offset,
            destination_file_id,
            destination_offset,
            length,
        } => {
            let [source_content, source_length] =
                regular_range_regions(first, *source_offset, *length);
            let [destination_content, destination_length] =
                regular_range_regions(second, *destination_offset, *length);
            regions[0] = source_content.map(|region| (first, region));
            regions[1] = source_length.map(|region| (first, region));
            regions[2] = destination_content.map(|region| (second, region));
            regions[3] = destination_length.map(|region| (second, region));
            debug_assert!(first.is_none_or(|record| record.file_id == *source_file_id));
            debug_assert!(second.is_none_or(|record| record.file_id == *destination_file_id));
        }
    }
    regions
}

fn regular_range_regions(
    record: Option<FileRecord>,
    offset: u64,
    length: u64,
) -> [Option<DependencyRegion>; 2] {
    let Some(record) = record else {
        return [None, None];
    };
    let logical_bytes = match record.payload {
        FilePayload::InlineRegular(data) => {
            u64::try_from(data.as_bytes().len()).unwrap_or(u64::MAX)
        }
        FilePayload::Regular { logical_bytes, .. } => logical_bytes,
        FilePayload::Directory { .. }
        | FilePayload::SymbolicLink { .. }
        | FilePayload::Device { .. }
        | FilePayload::Empty
        | FilePayload::ReparsePoint { .. } => {
            return [Some(DependencyRegion::FileRecord(record.file_id)), None];
        }
    };
    let overlap = logical_bytes.saturating_sub(offset).min(length);
    let content = (overlap != 0).then_some(DependencyRegion::ContentRange {
        file_id: record.file_id,
        offset,
        length: overlap,
    });
    let extends = offset
        .checked_add(length)
        .is_none_or(|end| end > logical_bytes);
    let file_length = extends.then_some(DependencyRegion::FileLength(record.file_id));
    [content, file_length]
}

fn validate_planned_file_range(
    logical_bytes: u64,
    range: ByteRange,
) -> Result<(), FsError> {
    let end = range.offset.checked_add(range.length)
        .ok_or(FsError::FileRead(FileRangeReadError::InvalidRange))?;
    if end > logical_bytes {
        return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
    }
    Ok(())
}

fn generation_from_record(
    record: &DurableCommit,
    volume_id: VolumeId,
    work: WorkCounters,
) -> Result<ObjectId, OperationFailure<FsError>> {
    generation_from_record_value(record, volume_id)
        .map_err(|error| OperationFailure::new(error, work))
}

fn generation_from_record_value(
    record: &DurableCommit,
    volume_id: VolumeId,
) -> Result<ObjectId, FsError> {
    let cancellation = CancellationToken::new();
    let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
        WorkBudget::UNBOUNDED, &cancellation,
    );
    generation_from_record_value_with_accounting(record, volume_id, &mut accounting)
}

fn generation_from_operation_record_value_with_accounting(
    record: &DurableCommit,
    volume_id: VolumeId,
    operation_id: OperationId,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<ObjectId, FsError> {
    accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
    if record.operation_id != operation_id {
        return Err(FsError::InvalidAuthorityHistory);
    }
    generation_from_record_value_with_accounting(record, volume_id, accounting)
}

fn generation_from_record_value_with_accounting(
    record: &DurableCommit,
    volume_id: VolumeId,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<ObjectId, FsError> {
    let mut observer = crate::kernel::OriginalReadAccounting::new(accounting)?;
    let item = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
    observer.admit(item)?;
    let initial = record.sequence == Sequence::new(1);
    observer.record_completed(item)?;
    let (record_volume, generation_root) = {
        let mut observe = |step| observer.observe(step);
        if initial {
            let creation = crate::kernel::decode_volume_created(&record.payload, MAXIMUM_VOLUME_EVENT_BYTES, &mut observe)?;
            (creation.volume_id, creation.initial_generation_root)
        } else {
            let publication = crate::kernel::decode_published_generation(
                &record.payload, MAXIMUM_VOLUME_EVENT_BYTES, &mut observe,
            )?;
            (publication.volume_id, publication.generation_root)
        }
    };
    drop(observer.into_allocation());
    if record_volume != volume_id {
        return Err(FsError::VolumeMismatch);
    }
    Ok(generation_root)
}

#[allow(
    clippy::too_many_arguments,
    reason = "the two file-table roots, change bound, config, prior work, budget, and cancellation are independent inputs"
)]
async fn diff_generation_file_tables<O: AsyncObjectStore>(
    objects: &O,
    before: ObjectId,
    after: ObjectId,
    maximum_changes: u32,
    config: VolumeConfig,
    mut work: WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> FsResult<GenerationDiff> {
    let mut allocations = crate::kernel::allocation::AllocationLedger::default();
    let mut accounting = crate::kernel::allocation::KernelAccounting::borrowed_standalone(
        &mut allocations, &mut work, budget, cancellation,
    );
    let result = diff_generation_file_tables_with_accounting(
        objects, before, after, maximum_changes, config, &mut accounting, cancellation,
    ).await;
    result.map(|(value, allocation)| {
        drop(allocation);
        OperationReceipt { value, work }
    }).map_err(|error| OperationFailure::new(error, work))
}

async fn diff_generation_file_tables_with_accounting<O: AsyncObjectStore>(
    objects: &O,
    before: ObjectId,
    after: ObjectId,
    maximum_changes: u32,
    config: VolumeConfig,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(GenerationDiff, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
    let (records, allocation) = crate::kernel::diff_file_records_with_accounting(
        objects, Some(before), Some(after), maximum_changes,
        DecodeLimits::for_volume(config), accounting, cancellation,
    ).await?;
    let record_bytes = records.retained_allocation_bytes;
    let mut records = crate::kernel::Owned::new(records, allocation, record_bytes);
    let maximum = usize::try_from(maximum_changes).unwrap_or(usize::MAX);
    let mut files = crate::kernel::Owned::<Vec<FileRecordChange>>::allocate(
        records.value.changes.len(), accounting,
    )?;
    let mut bindings = crate::kernel::Owned::<Vec<DirectoryBindingChange>>::allocate(0, accounting)?;
    let mut truncated = records.value.truncated;
    for change in records.value.changes.drain(..) {
        accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        if files.value.len() + bindings.value.len() >= maximum {
            truncated = true;
            break;
        }
        let before_entries = directory_entries(change.before);
        let after_entries = directory_entries(change.after);
        files.value.push(FileRecordChange {
            file_id: change.key, before: change.before, after: change.after,
        });
        if before_entries == after_entries { continue; }
        let remaining_changes = maximum.saturating_sub(files.value.len() + bindings.value.len());
        if remaining_changes == 0 {
            truncated = true;
            continue;
        }
        let (entries, allocation) = crate::kernel::diff_tree_entries_with_accounting(
            objects, before_entries, after_entries,
            u32::try_from(remaining_changes).unwrap_or(u32::MAX),
            DecodeLimits::for_volume(config), accounting, cancellation,
        ).await?;
        let bytes = entries.retained_allocation_bytes;
        let mut entries = crate::kernel::Owned::new(entries, allocation, bytes);
        truncated |= entries.value.truncated;
        let next_bytes = bindings.bytes.checked_add(bytes).ok_or(WorkError::Overflow)?;
        bindings.allocation.absorb(&mut entries.allocation)?;
        bindings.bytes = next_bytes;
        let buffer_bytes = crate::foundation::usize_to_u64(
            entries.value.changes.capacity().checked_mul(size_of::<crate::kernel::ValueChange<LogicalName, crate::kernel::TreeEntry>>())
                .ok_or(WorkError::Overflow)?,
        );
        for entry in entries.value.changes.drain(..) {
            accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            bindings.ensure_for_push(maximum, accounting)?;
            bindings.value.push(DirectoryBindingChange {
                directory_id: change.key, name: entry.key, before: entry.before, after: entry.after,
            });
        }
        // The original diff Vec is physically destroyed before retiring its
        drop(entries);
        accounting.release_owned(&mut bindings.allocation, buffer_bytes)?;
        bindings.bytes = bindings.bytes.checked_sub(buffer_bytes).ok_or(WorkError::Overflow)?;
    }
    records.retire(accounting)?;
    bindings.allocation.absorb(&mut files.allocation)?;
    let crate::kernel::Owned { value: files, allocation: files_allocation, .. } = files;
    let crate::kernel::Owned { value: bindings, allocation, .. } = bindings;
    drop(files_allocation);
    Ok((GenerationDiff { files, bindings, truncated }, allocation))
}

fn insert_lineage_value<T: Copy>(
    values: &mut crate::kernel::Owned<Vec<T>>,
    index: usize,
    value: T,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<(), FsError> {
    if values.value.len() == values.value.capacity() {
        let next = values.value.capacity().checked_mul(2)
            .map(|capacity| capacity.max(1)).ok_or(WorkError::Overflow)?;
        let mut replacement = crate::kernel::Owned::<Vec<T>>::allocate(next, accounting)?;
        let copied = WorkCounters {
            bytes_copied: crate::foundation::usize_to_u64(
                values.value.len().checked_mul(size_of::<T>()).ok_or(WorkError::Overflow)?,
            ),
            ..WorkCounters::UNCHARGED
        };
        if let Err(error) = accounting.admit(copied) {
            replacement.retire(accounting)?;
            return Err(error);
        }
        replacement.value.append(&mut values.value);
        let completed = accounting.record_completed(copied);
        let previous = std::mem::replace(values, replacement);
        previous.retire(accounting)?;
        completed?;
    }
    let copied = WorkCounters {
        bytes_copied: crate::foundation::usize_to_u64(
            (values.value.len() - index + 1).checked_mul(size_of::<T>())
                .ok_or(WorkError::Overflow)?,
        ),
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(copied)?;
    values.value.insert(index, value);
    accounting.record_completed(copied)
}

fn next_lineage_value<T: Copy>(
    pending: &[T],
    cursor: &mut usize,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<Option<T>, FsError> {
    let Some(value) = pending.get(*cursor) else { return Ok(None); };
    let copied = WorkCounters {
        items_examined: 1,
        bytes_copied: size_of::<T>() as u64,
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(copied)?;
    let value = *value;
    *cursor += 1;
    accounting.record_completed(copied)?;
    Ok(Some(value))
}

async fn collect_generation_ancestors<O: AsyncObjectStore>(
    objects: &O,
    start: GenerationId,
    config: VolumeConfig,
    maximum_generations: u32,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(crate::kernel::Owned<Vec<(GenerationId, Option<VolumeId>)>>, bool), crate::workspace::WorkspaceError> {
    let maximum = usize::try_from(maximum_generations).unwrap_or(usize::MAX);
    let mut visited = crate::kernel::Owned::<Vec<(GenerationId, Option<VolumeId>)>>::allocate(0, accounting)?;
    let mut pending = crate::kernel::Owned::<Vec<GenerationId>>::allocate(0, accounting)?;
    let mut cursor = 0;
    let result = async {
        insert_lineage_value(&mut pending, 0, start, accounting)?;
        while let Some(generation) = next_lineage_value(&pending.value, &mut cursor, accounting)? {
            let position = crate::kernel::binary_search_with_accounting(
                &visited.value, accounting, |candidate| candidate.0.cmp(&generation),
            )?;
            let Err(position) = position else { continue; };
            if visited.value.len() == maximum {
                // Retain the entire unread target frontier, not just the item
                while let Some(frontier) = next_lineage_value(&pending.value, &mut cursor, accounting)? {
                    if let Err(index) = crate::kernel::binary_search_with_accounting(
                        &visited.value, accounting, |candidate| candidate.0.cmp(&frontier),
                    )? {
                        insert_lineage_value(&mut visited, index, (frontier, None), accounting)?;
                    }
                }
                if let Err(index) = crate::kernel::binary_search_with_accounting(
                    &visited.value, accounting, |candidate| candidate.0.cmp(&generation),
                )? {
                    insert_lineage_value(&mut visited, index, (generation, None), accounting)?;
                }
                return Ok(true);
            }
            let decoded = crate::kernel::read_generation_root_with_accounting(
                objects,
                ObjectId { kind: ObjectKind::GenerationRoot, digest: generation.digest() },
                config, accounting, cancellation,
            ).await?;
            let bytes = root_parent_bytes(&decoded.0)?;
            let root = crate::kernel::Owned::new(decoded.0, decoded.1, bytes);
            let expanded = (|| -> Result<(), FsError> {
                insert_lineage_value(&mut visited, position, (generation, Some(root.value.volume_id)), accounting)?;
                for parent in &root.value.parents {
                    let index = pending.value.len();
                    insert_lineage_value(&mut pending, index, *parent, accounting)?;
                }
                Ok(())
            })();
            root.retire(accounting)?;
            expanded?;
        }
        Ok::<_, crate::workspace::WorkspaceError>(false)
    }.await;
    pending.retire(accounting)?;
    match result {
        Ok(truncated) => Ok((visited, truncated)),
        Err(error) => {
            visited.retire(accounting)?;
            Err(error)
        }
    }
}

async fn find_first_generation_ancestor<O: AsyncObjectStore>(
    objects: &O,
    start: GenerationId,
    config: VolumeConfig,
    maximum_generations: u32,
    candidates: &[(GenerationId, Option<VolumeId>)],
    candidates_truncated: bool,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<WorkspaceCommonAncestor, crate::workspace::WorkspaceError> {
    let maximum = usize::try_from(maximum_generations).unwrap_or(usize::MAX);
    let mut visited = crate::kernel::Owned::<Vec<GenerationId>>::allocate(0, accounting)?;
    let mut pending = crate::kernel::Owned::<Vec<(GenerationId, Option<WorkspaceCommonAncestor>)>>::allocate(0, accounting)?;
    let mut cursor = 0;
    let result = async {
        insert_lineage_value(&mut pending, 0, (start, None), accounting)?;
        while let Some((generation, baseline)) = next_lineage_value(&pending.value, &mut cursor, accounting)? {
            let candidate = crate::kernel::binary_search_with_accounting(
                candidates, accounting, |candidate| candidate.0.cmp(&generation),
            )?.ok().map(|index| candidates[index].1);
            if let Some(Some(volume_id)) = candidate {
                return Ok(baseline.unwrap_or(WorkspaceCommonAncestor { id: generation, volume_id }));
            }
            let is_candidate_frontier = matches!(candidate, Some(None));
            let position = crate::kernel::binary_search_with_accounting(
                &visited.value, accounting, |candidate| candidate.cmp(&generation),
            )?;
            let Err(position) = position else { continue; };
            if visited.value.len() == maximum {
                return Err(crate::workspace::WorkspaceError::LineageLimit);
            }
            let decoded = crate::kernel::read_generation_root_with_accounting(
                objects,
                ObjectId { kind: ObjectKind::GenerationRoot, digest: generation.digest() },
                config, accounting, cancellation,
            ).await?;
            let bytes = root_parent_bytes(&decoded.0)?;
            let root = crate::kernel::Owned::new(decoded.0, decoded.1, bytes);
            let expanded = (|| -> Result<Option<WorkspaceCommonAncestor>, FsError> {
                insert_lineage_value(&mut visited, position, generation, accounting)?;
                if is_candidate_frontier {
                    return Ok(Some(baseline.unwrap_or(WorkspaceCommonAncestor {
                        id: generation, volume_id: root.value.volume_id,
                    })));
                }
                // A filtered fork's initial namespace is its delta baseline.
                let boundary = if baseline.is_none()
                    && root.value.required_features & GenerationRoot::FILTERED_FORK != 0
                {
                    Some(WorkspaceCommonAncestor { id: generation, volume_id: root.value.volume_id })
                } else {
                    baseline
                };
                for parent in &root.value.parents {
                    let index = pending.value.len();
                    insert_lineage_value(&mut pending, index, (*parent, boundary), accounting)?;
                }
                Ok(None)
            })();
            root.retire(accounting)?;
            if let Some(ancestor) = expanded? { return Ok(ancestor); }
        }
        if candidates_truncated {
            Err(crate::workspace::WorkspaceError::LineageLimit)
        } else {
            Err(crate::workspace::WorkspaceError::NoCommonAncestor)
        }
    }.await;
    pending.retire(accounting)?;
    visited.retire(accounting)?;
    result
}

fn durable_head(commit: &DurableCommit) -> Head {
    Head {
        epoch: commit.epoch,
        sequence: commit.sequence,
        digest: commit.digest,
    }
}

fn validate_volume_capabilities(
    config: VolumeConfig,
    capabilities: EmbeddedCapabilities,
) -> Result<(), FsError> {
    config.validate()?;
    if config.lifecycle == Lifecycle::Durable && !capabilities.durable {
        return Err(FsError::UnsupportedDurability);
    }
    Ok(())
}

fn validate_checkout(
    mode: CheckoutMode,
    config: VolumeConfig,
) -> Result<(), OperationFailure<FsError>> {
    mode.validate().map_err(|error| OperationFailure::before_work(error.into()))?;
    if mode.mutations == MutationMode::DirectLive
        && config.concurrency != ConcurrencyMode::SerializedAuthority
    {
        return Err(OperationFailure::before_work(FsError::LiveRequiresSerializedAuthority));
    }
    Ok(())
}

fn closure_limits(config: VolumeConfig) -> ClosureLimits {
    ClosureLimits {
        decode: DecodeLimits::for_volume(config),
        maximum_objects: config.limits.maximum_objects_per_generation,
        maximum_files: config.limits.maximum_files_per_generation,
        maximum_object_bytes: config.limits.maximum_generation_bytes,
        profile: config.profile,
        symbolic_links: config.symbolic_links,
        hard_links: config.hard_links,
        sparse_files: config.sparse_files,
    }
}

fn directory_entries(record: Option<FileRecord>) -> Option<ObjectId> {
    let record = record?;
    match record.payload {
        FilePayload::Directory { entries } => Some(entries),
        _ => None,
    }
}

fn map_merge_failure(
    failure: OperationFailure<MergeGenerationError>,
    prior: WorkCounters,
) -> OperationFailure<FsError> {
    failure.map_with_prior_work(prior, |error| match error {
        MergeGenerationError::InvalidDiff => FsError::InvalidDiff,
        MergeGenerationError::ChangeLimit => FsError::MergeChangeLimit,
        MergeGenerationError::AllocationFailed => FsError::DiffAllocationFailed,
        MergeGenerationError::Object(error) => FsError::Object(error),
        MergeGenerationError::Decode(error) => FsError::Decode(error),
        MergeGenerationError::FileTable(error) => FsError::MergeFileTable(error),
        MergeGenerationError::Tree(error) => FsError::MergeTree(error),
        MergeGenerationError::ExtentRead(error) => {
            FsError::FileRead(FileRangeReadError::Extent(error))
        }
        MergeGenerationError::ExtentMutation(error) => {
            FsError::DetachedMutation(RegularMutationError::Extent(error))
        }
        MergeGenerationError::Checkpoint(error) => FsError::Checkpoint(error),
        MergeGenerationError::Cancelled(error) => FsError::Cancelled(error),
        MergeGenerationError::Work(error) => FsError::Work(error),
        MergeGenerationError::Kernel(error) => error,
    })
}

const fn live_publication_observation(
    outcome: CheckoutCommitOutcome,
) -> LivePublicationObservation {
    match outcome {
        CheckoutCommitOutcome::Committed {
            generation_id,
            head,
        } => LivePublicationObservation::Committed {
            generation_id,
            head,
        },
        CheckoutCommitOutcome::AlreadyCommitted {
            generation_id,
            head,
        } => LivePublicationObservation::AlreadyCommitted {
            generation_id,
            head,
        },
        CheckoutCommitOutcome::Conflict { actual } => {
            LivePublicationObservation::Conflict { actual }
        }
        CheckoutCommitOutcome::Fenced { actual_epoch } => {
            LivePublicationObservation::Fenced { actual_epoch }
        }
        CheckoutCommitOutcome::IdempotencyConflict {
            committed_fingerprint,
        } => LivePublicationObservation::IdempotencyConflict {
            committed_fingerprint,
        },
    }
}

fn probe_limits(config: VolumeConfig) -> ProbeLimits {
    ProbeLimits {
        decode: DecodeLimits::for_volume(config),
        maximum_cached_generations: 2,
        maximum_cached_records: config.limits.maximum_checkout_dependencies,
        maximum_extent_spans: config.limits.maximum_directory_page_entries,
        maximum_content_payload_bytes: config.limits.maximum_read_bytes,
        maximum_directory_entries: config.limits.maximum_directory_page_entries,
    }
}

fn root_parent_bytes(root: &GenerationRoot) -> Result<u64, FsError> {
    root.parents.capacity().checked_mul(size_of::<GenerationId>())
        .map(crate::foundation::usize_to_u64)
        .ok_or(FsError::Work(WorkError::Overflow))
}

fn root_parent_buffer(
    parents: &[GenerationId],
    live_owned_bytes: u64,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> MeasuredResult<OperationReceipt<Vec<GenerationId>>, FsError> {
    let mut work = WorkCounters::default();
    let result = (|| {
        cancellation.check()?;
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        allocations.claim_bytes(live_owned_bytes, 0, &mut work, budget)
            .map_err(|error| error.into_work_or(FsError::PendingMutationAllocationFailed))?;
        let mut values = Vec::new();
        if !parents.is_empty() {
            cancellation.check()?;
            let admitted = allocations.claim_elements::<GenerationId>(parents.len(), &mut work, budget)
                .map_err(|error| error.into_work_or(FsError::PendingMutationAllocationFailed))?;
            values.try_reserve_exact(parents.len()).map_err(|_| FsError::PendingMutationAllocationFailed)?;
            let actual = crate::foundation::usize_to_u64(values.capacity() * size_of::<GenerationId>());
            if actual > admitted {
                allocations.claim_bytes(actual - admitted, 0, &mut work, WorkBudget::UNBOUNDED)
                    .map_err(|error| error.into_work_or(FsError::PendingMutationAllocationFailed))?;
                work.verify(budget)?;
            }
        }
        let copy_bytes = parents.len().checked_mul(size_of::<GenerationId>())
            .map(crate::foundation::usize_to_u64).ok_or(FsError::Work(WorkError::Overflow))?;
        crate::collection::charge_handle_copy(copy_bytes, &mut work, budget)?;
        values.extend_from_slice(parents);
        Ok(values)
    })();
    result.map(|value| OperationReceipt { value, work })
        .map_err(|error| OperationFailure::new(error, work))
}

fn clone_grouped_mutation_with_accounting(
    mutation: &Mutation,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<crate::kernel::Owned<Mutation>, FsError> {
    accounting.charge(WorkCounters { bytes_copied: size_of::<Mutation>() as u64, ..WorkCounters::UNCHARGED })?;
    let mut allocation = accounting.claim_owned(0, 0)?;
    let mut bytes = 0_u64;
    let result = (|| {
    let mut path = |path: &NamespacePath| -> Result<NamespacePath, FsError> {
        let (cloned, mut guard) = path.clone_with_accounting(accounting, cancellation)?;
        let retained = cloned.retained_bytes();
        readonly_absorb_allocation(&mut allocation, &mut guard)?;
        bytes = bytes.checked_add(retained).ok_or(WorkError::Overflow)?;
        Ok(cloned)
    };
    let cloned = match mutation {
        Mutation::Create { path: source, record } => Mutation::Create { path: path(source)?, record: *record },
        Mutation::Restore { path: source, record } => Mutation::Restore { path: path(source)?, record: *record },
        Mutation::Remove { path: source, expected_file_id } => Mutation::Remove {
            path: path(source)?, expected_file_id: *expected_file_id,
        },
        Mutation::Rename { source, destination, replace } => Mutation::Rename {
            source: path(source)?, destination: path(destination)?, replace: *replace,
        },
        Mutation::Link { source, destination } => Mutation::Link {
            source: path(source)?, destination: path(destination)?,
        },
        Mutation::SetMetadata { path: source, metadata } => Mutation::SetMetadata {
            path: path(source)?, metadata: *metadata,
        },
        Mutation::Write { path: source, offset, length, content, content_offset } => Mutation::Write {
            path: path(source)?, offset: *offset, length: *length, content: *content, content_offset: *content_offset,
        },
        Mutation::ValidateRegular { path: source } => Mutation::ValidateRegular { path: path(source)? },
        Mutation::Resize { path: source, logical_bytes } => Mutation::Resize {
            path: path(source)?, logical_bytes: *logical_bytes,
        },
        Mutation::ZeroRange { path: source, offset, length, allocated, extend } => Mutation::ZeroRange {
            path: path(source)?, offset: *offset, length: *length, allocated: *allocated, extend: *extend,
        },
        Mutation::Preallocate { path: source, offset, length, keep_size } => Mutation::Preallocate {
            path: path(source)?, offset: *offset, length: *length, keep_size: *keep_size,
        },
        Mutation::CloneRange { source, source_offset, destination, destination_offset, length } => Mutation::CloneRange {
            source: path(source)?, source_offset: *source_offset, destination: path(destination)?,
            destination_offset: *destination_offset, length: *length,
        },
        Mutation::File { file_id, mutation } => Mutation::File { file_id: *file_id, mutation: mutation.clone() },
        Mutation::CloneFileRange { source_file_id, source_offset, destination_file_id, destination_offset, length } => Mutation::CloneFileRange {
            source_file_id: *source_file_id, source_offset: *source_offset, destination_file_id: *destination_file_id,
            destination_offset: *destination_offset, length: *length,
        },
    };
    Ok::<_, FsError>(cloned)
    })();
    match result {
        Ok(cloned) => Ok(crate::kernel::Owned::new(cloned, allocation, bytes)),
        Err(error) => {
            crate::kernel::retire_allocation(allocation, bytes, accounting)?;
            Err(error)
        }
    }
}

fn authenticate_named_creation(
    creation: &VolumeCreated,
    operation: OperationId,
    volume_id: VolumeId,
    config: VolumeConfig,
    original_operation_id: OperationId,
) -> Result<(), FsError> {
    if operation != original_operation_id || creation.volume_id != volume_id
        || creation.config != config
    {
        return Err(FsError::CreationRejected);
    }
    Ok(())
}

fn decode_first_creation_record(
    volume_id: VolumeId,
    records: &[crate::foundation::DurableCommit],
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<(VolumeCreated, OperationId), FsError> {
    accounting.check()?;
    let first = records.first().ok_or(FsError::EmptyAuthority)?;
    if first.sequence != Sequence::new(1) || first.previous_digest != Digest::ZERO {
        return Err(FsError::CreationRejected);
    }
    let hashed_bytes = (first.payload.len() as u64).checked_mul(2)
        .and_then(|bytes| bytes.checked_add(CREATION_FINGERPRINT_DOMAIN.len() as u64))
        .and_then(|bytes| bytes.checked_add(crate::foundation::AUTHORITY_COMMIT_DIGEST_ENVELOPE_BYTES))
        .ok_or(WorkError::Overflow)?;
    let hashing = WorkCounters { bytes_hashed: hashed_bytes, ..WorkCounters::UNCHARGED };
    accounting.admit(hashing)?;
    let fingerprint = creation_fingerprint(&first.payload);
    let digest = crate::foundation::authority_commit_digest(
        volume_authority_id(volume_id), first.epoch, first.sequence, first.operation_id,
        first.fingerprint, first.previous_digest, &first.payload,
    );
    accounting.record_completed(hashing)?;
    if first.fingerprint != fingerprint || first.digest != digest {
        return Err(FsError::CreationRejected);
    }
    let mut projection = crate::kernel::OriginalReadAccounting::new(accounting)?;
    let creation = crate::kernel::decode_volume_created::<FsError>(
        &first.payload, MAXIMUM_VOLUME_EVENT_BYTES, &mut |step| projection.observe(step),
    );
    projection.release_failed()?;
    let creation = creation?;
    if creation.volume_id != volume_id {
        return Err(FsError::VolumeMismatch);
    }
    Ok((creation, first.operation_id))
}

fn derived_digest(domain: &[u8], volume_id: VolumeId) -> Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(&volume_id.into_bytes());
    Digest::from_bytes(*hasher.finalize().as_bytes())
}

fn identity_hash_work(domain: &[u8]) -> WorkCounters {
    WorkCounters {
        bytes_hashed: u64::try_from(domain.len()).unwrap_or(u64::MAX) + 16,
        bytes_copied: 16,
        ..WorkCounters::default()
    }
}

/// A fork's two first records: the retention of its source generation, under
fn fork_records_with_accounting<A, O>(
    source: &Volume<A, O>,
    source_root: ObjectId,
    destination: crate::WorkspaceId,
    fork_root: ObjectId,
    operation_id: OperationId,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(crate::AuthorityId, ProposedCommit, ProposedCommit, u64), FsError> {
    accounting.check()?;
    cancellation.check()?;
    let mut label = crate::kernel::Owned::<Vec<u8>>::allocate(32, accounting)?;
    let encoding = WorkCounters { bytes_encoded: 32, ..WorkCounters::UNCHARGED };
    accounting.admit(encoding)?;
    label.value.resize(32, 0);
    hex::encode_to_slice(destination.into_bytes(), &mut label.value)
        .expect("sixteen-byte identity encodes to thirty-two hex bytes");
    accounting.record_completed(encoding)?;
    let text = String::from_utf8(label.value).expect("hex encoder produces ASCII");
    let label = crate::kernel::Owned::new(text, label.allocation, label.bytes);
    let hashing = WorkCounters {
        bytes_hashed: (b"acyclic-fs-retention-authority-v1\0".len() + 16 + 1 + 8 + label.value.len()) as u64,
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(hashing)?;
    let retention = retention_authority_id(source.id, RetentionKind::ForkBase, &label.value);
    accounting.record_completed(hashing)?;
    let fact = crate::kernel::Owned::new(RetentionCreated {
        volume_id: source.id, kind: RetentionKind::ForkBase, label: label.value,
        generation_root: source_root, config: source.config,
    }, label.allocation, label.bytes);
    let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| crate::kernel::encode_retention_created(&fact.value, observe, allocation_error))?;
    let retained_bytes = encoded.0.capacity() as u64;
    let retained = creation_commit_with_accounting(
        OperationId::from_bytes(retention.into_bytes()), encoded, accounting,
    )?;
    fact.retire(accounting)?;
    let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| crate::kernel::encode_volume_created(VolumeCreated {
        volume_id: destination.volume_id(), config: source.config, initial_generation_root: fork_root,
    }, observe, allocation_error))?;
    let creation_bytes = encoded.0.capacity() as u64;
    let creation = creation_commit_with_accounting(operation_id, encoded, accounting)?;
    let payload_bytes = retained_bytes.checked_add(creation_bytes).ok_or(WorkError::Overflow)?;
    Ok((retention, retained, creation, payload_bytes))
}

/// Transfers an already admitted canonical buffer without recharging its allocation.
fn creation_commit(
    operation_id: OperationId,
    encoded: OperationReceipt<Vec<u8>>,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> crate::MeasuredResult<OperationReceipt<(ProposedCommit, u64)>, FsError> {
    let capacity = encoded.value.capacity() as u64;
    let mut allocations = crate::kernel::allocation::AllocationLedger::default();
    let mut work = encoded.work;
    let result = {
        let mut accounting = crate::kernel::allocation::KernelAccounting::borrowed_standalone(
            &mut allocations, &mut work, budget, cancellation,
        );
        creation_commit_with_accounting(
            operation_id, (encoded.value, crate::kernel::allocation::KernelOwnedAllocation::Standalone),
            &mut accounting,
        )
    };
    result.map(|value| OperationReceipt { value: (value, capacity), work })
        .map_err(|error| OperationFailure::new(error, work))
}

fn creation_fingerprint(payload: &[u8]) -> Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(CREATION_FINGERPRINT_DOMAIN);
    hasher.update(payload);
    Digest::from_bytes(*hasher.finalize().as_bytes())
}

fn creation_commit_with_accounting(
    operation_id: OperationId,
    encoded: (Vec<u8>, crate::kernel::allocation::KernelOwnedAllocation),
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<ProposedCommit, FsError> {
    accounting.check()?;
    let hashing = WorkCounters {
        bytes_hashed: (CREATION_FINGERPRINT_DOMAIN.len() as u64).checked_add(encoded.0.len() as u64)
            .ok_or(WorkError::Overflow)?,
        ..WorkCounters::default()
    };
    accounting.admit(hashing)?;
    let fingerprint = creation_fingerprint(&encoded.0);
    accounting.record_completed(hashing)?;
    #[cfg(not(target_arch = "wasm32"))]
    if let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) = &encoded.1 {
        if allocation.bytes() != encoded.0.capacity() as u64
            || !accounting.native_work().is_some_and(|original| allocation.work().shares_ledger(original))
        {
            return Err(FsError::HandleAllocationFailed);
        }
        let control_bytes = crate::kernel::allocation::bytes_owner_control_bytes::<crate::kernel::Owned<Vec<u8>>>()?;
        let mut control = allocation.work().claim(control_bytes)?;
        let bytes = encoded.0.capacity() as u64;
        let mut payload = crate::kernel::Owned::new(encoded.0, encoded.1, bytes);
        let KernelOwnedAllocation::NativeOriginal(allocation) = &mut payload.allocation else {
            unreachable!("native encoded allocation was matched before moving");
        };
        allocation.absorb(&mut control)?;
        payload.bytes = allocation.bytes();
        let payload = Bytes::from_owner(payload);
        return Ok(ProposedCommit { operation_id, fingerprint, payload });
    }
    // Audited bytes 1.12: only a spare-capacity Vec allocates the private owner.
    let owner = WorkCounters {
        allocation_operations: u64::from(encoded.0.capacity() != encoded.0.len()),
        ..WorkCounters::default()
    };
    accounting.admit(owner)?;
    let payload = Bytes::from(encoded.0);
    accounting.record_completed(owner)?;
    Ok(ProposedCommit { operation_id, fingerprint, payload })
}

#[cfg(not(target_arch = "wasm32"))]
impl AsRef<[u8]> for crate::kernel::Owned<Vec<u8>> {
    fn as_ref(&self) -> &[u8] { &self.value }
}

fn merge_fork_backend_work(
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    backend: WorkCounters,
) -> Result<(), crate::workspace::WorkspaceError> {
    let (allocations, work, budget, _) = accounting.standalone_parts()
        .ok_or(FsError::HandleAllocationFailed)?;
    let prior = *work;
    *work = prior.with_backend(backend, allocations.live_bytes())
        .map_err(|error| crate::workspace::WorkspaceError::WorkMerge { prior, backend, error })?;
    work.verify(budget)?;
    Ok(())
}

fn merge_creation_backend_work(
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    spent: WorkCounters,
) -> Result<(), FsError> {
    let (allocations, work, budget, _) = accounting.standalone_parts()
        .ok_or(FsError::HandleAllocationFailed)?;
    *work = work.with_backend(spent, allocations.live_bytes())?;
    work.verify(budget)?;
    Ok(())
}

fn remaining_owned(
    work: WorkCounters,
    budget: WorkBudget,
    live_bytes: u64,
) -> Result<WorkBudget, OperationFailure<FsError>> {
    let mut remaining = work.remaining(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    // The ledger already admitted these live bytes within the same permit.
    remaining.peak_allocation_bytes -= live_bytes;
    Ok(remaining)
}

fn push_authored_operation(
    operations: &mut crate::kernel::Owned<Vec<Mutation>>,
    allocation: &mut crate::kernel::allocation::KernelOwnedAllocation,
    bytes: &mut u64,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    mutation: Mutation,
) -> Result<(), FsError> {
    if operations.value.len() == operations.value.capacity() {
        return Err(FsError::PendingMutationAllocationFailed);
    }
    accounting.charge(WorkCounters {
        bytes_copied: size_of::<Mutation>() as u64, ..WorkCounters::UNCHARGED
    })?;
    let path_bytes = mutation_path_owned_bytes(&mutation);
    let remaining = bytes.checked_sub(path_bytes).ok_or(WorkError::Overflow)?;
    let retained = operations.bytes.checked_add(path_bytes).ok_or(WorkError::Overflow)?;
    let mut mutation = crate::kernel::Owned::new(
        mutation, crate::kernel::split_allocation(allocation, path_bytes)?, path_bytes,
    );
    *bytes = remaining;
    let transferred = operations.allocation.absorb(&mut mutation.allocation);
    if let Err(error) = transferred {
        mutation.retire(accounting)?;
        return Err(error);
    }
    operations.value.push(mutation.value);
    operations.bytes = retained;
    Ok(())
}

fn clone_authored_path(
    path: &NamespacePath,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<crate::kernel::Owned<NamespacePath>, FsError> {
    let cancellation = match &*accounting {
        crate::kernel::allocation::KernelAccounting::Standalone { cancellation, .. }
        | crate::kernel::allocation::KernelAccounting::BorrowedStandalone { cancellation, .. } => *cancellation,
        #[cfg(not(target_arch = "wasm32"))]
        crate::kernel::allocation::KernelAccounting::NativeOriginal { cancellation, .. } => *cancellation,
    };
    let (path, allocation) = path.clone_with_accounting(accounting, cancellation)?;
    let bytes = path.retained_bytes();
    Ok(crate::kernel::Owned::new(path, allocation, bytes))
}

fn clone_borrowed_authored_descriptor(
    authored: &AuthoredMutation,
    accounting: &mut KernelAccounting<'_>,
) -> Result<crate::kernel::Owned<AuthoredMutation>, FsError> {
    use AuthoredMutation::*;
    let mut allocation = accounting.claim_owned(0, 0)?;
    let mut bytes = 0_u64;
    let result = (|| {
        let mut copy_path = |source: &NamespacePath| -> Result<NamespacePath, FsError> {
            let mut path = clone_authored_path(source, accounting)?;
            let Some(total) = bytes.checked_add(path.bytes) else {
                path.retire(accounting)?;
                return Err(WorkError::Overflow.into());
            };
            if let Err(error) = allocation.absorb(&mut path.allocation) {
                path.retire(accounting)?;
                return Err(error);
            }
            bytes = total;
            Ok(path.value)
        };
        Ok::<_, FsError>(match authored {
            Reidentify { path, file_id } => Reidentify { path: copy_path(path)?, file_id: *file_id },
            CreateFile { path, bytes, metadata } => CreateFile { path: copy_path(path)?, bytes: bytes.clone(), metadata: *metadata },
            CreateFileFromContent { path, content, metadata, file_id } =>
             CreateFileFromContent { path: copy_path(path)?, content: *content,
                    metadata: *metadata, file_id: *file_id },
            CreateDirectory { path, metadata } => CreateDirectory { path: copy_path(path)?, metadata: *metadata },
            CreateSymbolicLink { path, target, metadata } =>
             CreateSymbolicLink { path: copy_path(path)?, target: target.clone(), metadata: *metadata },
            CreateEmptySpecial { path, kind, metadata } =>
             CreateEmptySpecial { path: copy_path(path)?, kind: *kind, metadata: *metadata },
            CreateDevice { path, kind, major, minor, metadata } =>
             CreateDevice { path: copy_path(path)?, kind: *kind, major: *major, minor: *minor,
                    metadata: *metadata },
            CreateReparsePoint { path, payload, metadata } =>
             CreateReparsePoint { path: copy_path(path)?, payload: payload.clone(), metadata: *metadata },
            Remove { path, expected_file_id } => Remove { path: copy_path(path)?, expected_file_id: *expected_file_id },
            Rename { source, destination, replace } =>
             Rename { source: copy_path(source)?, destination: copy_path(destination)?, replace: *replace },
            HardLink { source, destination } =>
             HardLink { source: copy_path(source)?, destination: copy_path(destination)? },
            Write { path, offset, bytes } => Write { path: copy_path(path)?, offset: *offset, bytes: bytes.clone() },
            WriteFromContent { path, offset, content } =>
             WriteFromContent { path: copy_path(path)?, offset: *offset, content: *content },
            SetMetadata { path, metadata } => SetMetadata { path: copy_path(path)?, metadata: *metadata },
            Resize { path, logical_bytes } => Resize { path: copy_path(path)?, logical_bytes: *logical_bytes },
            ZeroRange { path, range, allocated, extend } =>
             ZeroRange { path: copy_path(path)?, range: *range, allocated: *allocated, extend: *extend },
            Preallocate { path, range, keep_size } =>
             Preallocate { path: copy_path(path)?, range: *range, keep_size: *keep_size },
            CloneRange(request) =>
            CloneRange(FileCloneRequest {
                    source: copy_path(&request.source)?, source_offset: request.source_offset,
                    destination: copy_path(&request.destination)?, destination_offset: request.destination_offset,
                    length: request.length,
                }),
    })
    })();
    match result {
        Ok(value) => Ok(crate::kernel::Owned::new(value, allocation, bytes)),
        Err(error) => {
            crate::kernel::retire_allocation(allocation, bytes, accounting)?;
            Err(error)
        }
    }
}

#[inline]
fn add(prior: WorkCounters, next: WorkCounters) -> Result<WorkCounters, OperationFailure<FsError>> {
    prior
        .checked_add(next)
        .map_err(|error| OperationFailure::new(error.into(), prior))
}

fn map_transfer_error(error: GenerationTransferError) -> FsError {
    match error {
        GenerationTransferError::Cancelled(error) => FsError::Cancelled(error),
        GenerationTransferError::Manifest(_) | GenerationTransferError::ManifestMismatch => {
            FsError::InvalidExportManifest
        }
        GenerationTransferError::Closure(error) => FsError::Closure(error),
        GenerationTransferError::Object(error) => FsError::Object(error),
        GenerationTransferError::Work(error) => FsError::Work(error),
        error @ (GenerationTransferError::InvalidCursor
        | GenerationTransferError::EmptyBatch
        | GenerationTransferError::TooManyObjects
        | GenerationTransferError::AllocationFailed) => FsError::Transfer(error),
    }
}

#[inline]
fn remaining(
    work: WorkCounters,
    budget: WorkBudget,
) -> Result<WorkBudget, OperationFailure<FsError>> {
    work.remaining(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))
}

fn merge_simultaneous_work(
    prior: WorkCounters,
    nested: WorkCounters,
    live_bytes: u64,
    budget: WorkBudget,
) -> Result<WorkCounters, OperationFailure<FsError>> {
    let merged = prior
        .with_backend(nested, live_bytes)
        .map_err(|error| OperationFailure::new(error.into(), prior))?;
    merged
        .verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), merged))?;
    Ok(merged)
}

fn merge_simultaneous_failure<E: From<WorkError>>(
    prior: WorkCounters,
    nested: WorkCounters,
    live_bytes: u64,
    error: E,
) -> OperationFailure<E> {
    match prior.with_backend(nested, live_bytes) {
        Ok(merged) => OperationFailure::new(error, merged),
        Err(overflow) => OperationFailure::new(overflow.into(), prior),
    }
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/facade.rs"]
mod tests;

#[inline]
pub(crate) fn measured<T, E>(result: Result<T, E>, accounting: &KernelAccounting<'_>) -> MeasuredResult<FsReceipt<T>, E> {
    let work = accounting.spent();
    result.map(|value| FsReceipt { value, work }).map_err(|error| OperationFailure::new(error, work))
}
