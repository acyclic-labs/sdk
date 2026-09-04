//! Thin embedded `Fs`/`Volume`/`Checkout` composition over the two durable stores.

use crate::async_storage::{AsyncAuthorityStore, AsyncObjectStore};
use crate::cancellation::{CancellationError, CancellationToken};
use crate::foundation::{
    Digest, DurableCommit, Epoch, FileId, GenerationId, Head, OperationId, ProposedCommit,
    Sequence, VolumeId,
};
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
use crate::kernel::decode_retention_created;
use crate::kernel::{
    AsyncBlobSource, AttributeEntry, AttributeListError, AttributeListing, AttributeLookupError,
    AttributeMutation, AttributeMutationError, AttributeName, AttributePage,
    AuthenticatedGenerationProbe, AuthenticatedProbeError, BlobBuildError, BlobBuildOptions,
    BlobReadError, CanonicalDecodeError, CheckoutDependencies, CheckpointError, CheckpointRequest,
    ClosureError, ClosureLimits, DecodeLimits, Dependency, DependencyError, DependencyRegion,
    DependencyState, DirectoryPage, DirectoryReadError, ExtentPlan, ExtentRangeRequest,
    ExtentSeekRequest, ExtentSeekTarget, FileKind, FileMetadata, FileMutation, FilePayload,
    FileRangeRead, FileRangeReadError, FileRangeRequest, FileRecord, FileRecordReadError,
    FileTableMutationError, FileTablePage, GenerationExportManifest, GenerationMutationError,
    GenerationRoot, GenerationTransferBatch, GenerationTransferError, InlineFileData,
    InlineFileDataError, LivePublicationObservation, LiveRetryAction, LiveRetryError,
    LiveRetryState, LogicalName, MAXIMUM_GENERATION_ROOT_BYTES, MergeGenerationError,
    MergeGenerationOutcome, MergeGenerationRequest, MetadataField, Mutation, NameEncoding,
    NamespacePath, PathBatchLookup, PathLookup, PathLookupError, PersistentDiffError, ProbeLimits,
    PublicationError, PublishGenerationRequest, RebaseConflict, RebaseDecision, RebaseError,
    RegularMutation, RegularMutationError, RetentionCreated, RetentionCreatedError, RetentionKind,
    TransferCursor, TreeMutationError, TreePage, VolumeCreated, VolumeCreatedError,
    apply_attribute_mutations_async, apply_generation_mutations_retaining_async,
    apply_regular_mutation_async, authenticate_generation_export_manifest_async, build_blob_async,
    build_checkpoint_async, build_generation_export_manifest_async, classify_rebase_async,
    decode_file_metadata, decode_published_generation, decode_volume_created,
    decode_workspace_deleted, diff_file_records_async, diff_tree_entries_async,
    encode_attribute_page, encode_file_metadata, encode_file_table_page, encode_generation_root,
    encode_retention_created, encode_tree_page, encode_volume_created, encode_workspace_deleted,
    export_generation_batch_async, generation_root_parent_count, import_generation_batch_async,
    list_attributes_async, list_tree_entries_async, lookup_attribute_async,
    lookup_file_record_async, lookup_file_records_async, merge_generation_async,
    plan_extent_range_async, prove_generation_closure_async, publish_generation_async,
    read_blob_range_async, read_file_range_async, retention_authority_id, seek_extent_async,
    volume_authority_id,
};
#[cfg(test)]
use crate::kernel::{
    FileTableMutation, TreeMutation, file_table_mutation,
    merge_directory_record_async as merge_directory_record, merge_file_fields, resolve_three,
    tree_mutation,
};
pub use crate::kernel::{LiveMutationOutcome, MergeConflict};
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
use crate::kernel::{decode_source_volume, source_authority_id};
use crate::model::{
    AccessMode, CaseSensitivity, CheckoutMode, CheckoutModeError, ConcurrencyMode, ConsistencyMode,
    GenerationSelector, Lifecycle, MutationMode, UnicodePolicy, VolumeConfig, VolumeConfigError,
    VolumeLimits,
};
use crate::performance::{
    MeasuredResult, OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkError,
};
use crate::storage::{
    AppendOutcome, AuthorityStoreError, ByteRange, CreateAuthorityOutcome, FenceOutcome,
    OBJECT_DIGEST_ENVELOPE_BYTES, ObjectId, ObjectKind, ObjectReadRequest, ObjectReadRetention,
    ObjectStoreError, ReplayLimit, object_digest,
};
use bytes::Bytes;
use std::collections::{BTreeSet, VecDeque};
use std::mem::size_of;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;

const MAXIMUM_VOLUME_EVENT_BYTES: u64 = 4 * 1024;
/// Directory-listing page size used to scan siblings for a case-folded
/// collision under `CaseSensitivity::ProfileFolded`. This makes admitting a
/// new name under that policy O(directory size) rather than O(1); the tree's
/// physical page ordering is exact-byte (see `kernel::tree`), so folded
/// comparison cannot be a binary search over it.
const CASE_FOLD_COLLISION_SCAN_PAGE: u32 = 256;
const ROOT_FILE_DOMAIN: &[u8] = b"acyclic-fs-root-file-v1\0";
const CREATION_OPERATION_DOMAIN: &[u8] = b"acyclic-fs-create-operation-v1\0";
const CREATION_FINGERPRINT_DOMAIN: &[u8] = b"acyclic-fs-create-fingerprint-v1\0";

pub(crate) enum WorkspaceJoinOutcome {
    Applied(GenerationId),
    AlreadyApplied(GenerationId),
    NoChanges(GenerationId),
    Stale(GenerationId),
    Conflicted(Vec<MergeConflict>, bool),
    Fenced,
    IdempotencyConflict,
}

pub(crate) struct WorkspaceJoinRequest<'a, A, O> {
    pub target: &'a Volume<A, O>,
    pub base: GenerationId,
    pub source: &'a crate::Generation<A, O>,
    pub expected_target: GenerationId,
    pub expected_head: Head,
    pub history: crate::workspace::JoinHistory,
    pub operation_id: OperationId,
    pub maximum_changes: u32,
    pub maximum_conflicts: u32,
}

pub(crate) struct WorkspaceRebaseRequest<'a, A, O> {
    pub target: &'a Volume<A, O>,
    pub operation_id: OperationId,
    pub maximum_generations: u32,
    pub maximum_changes: u32,
    pub maximum_conflicts: u32,
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
    /// Durable root containing `objects/` and `authorities/`.
    pub root: PathBuf,
    /// Maximum canonical bytes admitted for one immutable object.
    pub maximum_object_bytes: u64,
    /// Maximum canonical payload bytes admitted for one authority fact.
    pub maximum_authority_payload_bytes: u32,
    /// `SQLite` WAL pages between automatic bounded checkpoints.
    pub authority_checkpoint_pages: u32,
    /// Bounded worker pool used for all synchronous native storage calls.
    pub native_executor: crate::native_executor::NativeExecutorConfig,
    /// Bounded disposable immutable-object accelerator shared by every handle.
    pub object_cache: crate::cache::ObjectCacheOptions,
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
impl LocalOptions {
    /// Creates local options with the SDK's bounded backend defaults.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            maximum_object_bytes: 64 * 1024 * 1024,
            maximum_authority_payload_bytes: crate::local_authority::DEFAULT_MAX_PAYLOAD_BYTES,
            authority_checkpoint_pages: crate::local_authority::DEFAULT_CHECKPOINT_PAGES,
            native_executor: crate::native_executor::NativeExecutorConfig::default(),
            object_cache: crate::cache::ObjectCacheOptions::default(),
        }
    }
}

struct FsInner<A, O> {
    authority: A,
    objects: O,
    capabilities: EmbeddedCapabilities,
    workspace_namespace: [u8; 16],
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

    /// Derives the deployment-scoped stable identity for one canonical name.
    ///
    /// # Errors
    ///
    /// Rejects an invalid workspace name without storage access.
    pub fn workspace_id(
        &self,
        name: impl AsRef<str>,
    ) -> Result<crate::WorkspaceId, crate::WorkspaceNameError> {
        let name = crate::WorkspaceName::new(name)?;
        Ok(crate::WorkspaceId::derive(
            self.inner.workspace_namespace,
            &name,
        ))
    }

    pub(crate) fn same_deployment(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    pub(crate) fn authority(&self) -> &A {
        &self.inner.authority
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
    base_generation_root: ObjectId,
    generation_root: ObjectId,
    base_file_table: ObjectId,
    base_root: GenerationRoot,
    root: GenerationRoot,
    authority_head: Option<Head>,
    pending_operations: Vec<Mutation>,
    live_operation_id: Option<OperationId>,
    last_commit: Option<LastCommit>,
    prepared_merge_parent: Option<ObjectId>,
    dependencies: CheckoutDependencies,
    mode: CheckoutMode,
}

#[derive(Clone, Copy)]
struct LastCommit {
    operation_id: OperationId,
    generation_id: GenerationId,
    head: Head,
}

#[derive(Clone, Copy)]
struct VolumeCreation {
    volume_id: VolumeId,
    config: VolumeConfig,
    generation_root: ObjectId,
    operation_id: Option<OperationId>,
}

/// Durable local authority backend with nonblocking native storage dispatch.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub type LocalAuthorityBackend =
    crate::native_executor::NativeStore<crate::local_authority::LocalAuthorityStore>;

/// Durable local immutable-object backend with nonblocking native storage dispatch.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub type LocalObjectBackend = crate::cache::CachedObjectStore<
    crate::native_executor::NativeStore<crate::local::LocalObjectStore>,
>;

/// Durable local filesystem composition with nonblocking native storage dispatch.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub type LocalFs = Fs<LocalAuthorityBackend, LocalObjectBackend>;

/// Durable local volume handle with nonblocking native storage dispatch.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub type LocalVolume = Volume<LocalAuthorityBackend, LocalObjectBackend>;

/// Deterministic in-process composition over the exact public Stream and Objects providers.
#[cfg(all(
    feature = "memory",
    feature = "distributed",
    not(target_arch = "wasm32")
))]
pub type MemoryFs = Fs<MemoryAuthorityBackend, MemoryObjectBackend>;

#[cfg(all(
    feature = "memory",
    feature = "distributed",
    not(target_arch = "wasm32")
))]
/// Filesystem authority adapter backed by the public in-memory Stream provider.
pub type MemoryAuthorityBackend =
    crate::distributed::StreamAuthorityStore<acyclic_stream::MemoryStream>;

#[cfg(all(
    feature = "memory",
    feature = "distributed",
    not(target_arch = "wasm32")
))]
/// Filesystem object adapter backed by the public in-memory Objects provider.
pub type MemoryObjectBackend =
    crate::distributed::ProviderObjectStore<acyclic_objects::MemoryObjects>;

#[cfg(all(
    test,
    feature = "memory",
    feature = "distributed",
    not(target_arch = "wasm32")
))]
pub(crate) type MemoryCheckout = Checkout<MemoryAuthorityBackend, MemoryObjectBackend>;

/// Ephemeral path-independent regular file retained by an open native handle.
///
/// Detached files share immutable objects with their originating volume but
/// are never published into a generation. They exist only until the final
/// native handle closes, matching POSIX last-unlink lifetime semantics.
pub struct DetachedFile<A, O> {
    volume: Volume<A, O>,
    record: FileRecord,
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

/// One caller-authored filesystem operation compiled into a single sparse,
/// atomic checkout transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AuthoredMutation {
    /// Creates one regular file with exact initial bytes and metadata.
    CreateFile {
        /// New namespace path.
        path: NamespacePath,
        /// Complete initial bytes.
        bytes: Bytes,
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
        target: Bytes,
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
        payload: Bytes,
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
        bytes: Bytes,
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

fn authored_mutation_operation_count(mutation: &AuthoredMutation) -> usize {
    match mutation {
        AuthoredMutation::CreateFile { bytes, .. } => {
            usize::from(bytes.len() > crate::kernel::MAXIMUM_INLINE_FILE_BYTES) + 1
        }
        AuthoredMutation::CreateFileFromContent { content, .. } => {
            usize::from(content.logical_bytes != 0) + 1
        }
        AuthoredMutation::Write { .. }
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

fn pending_name_collision(
    path: &NamespacePath,
    pending: &[(NamespacePath, FileId)],
    allowed_file_ids: &[FileId],
) -> bool {
    let Some((parent, name)) = path.split_last() else {
        return false;
    };
    let folded = name.case_fold_key();
    pending.iter().any(|(pending_path, file_id)| {
        let Some((pending_parent, pending_name)) = pending_path.split_last() else {
            return false;
        };
        pending_parent == parent
            && pending_name != name
            && pending_name.case_fold_key() == folded
            && !allowed_file_ids.contains(file_id)
    })
}

fn pending_binding_path(
    path: &NamespacePath,
    pending: &[(NamespacePath, FileId)],
    case_sensitivity: CaseSensitivity,
) -> Option<(NamespacePath, FileId)> {
    let (parent, name) = path.split_last()?;
    pending.iter().rev().find_map(|(pending_path, file_id)| {
        let (pending_parent, pending_name) = pending_path.split_last()?;
        let same_name = match case_sensitivity {
            CaseSensitivity::Sensitive => pending_name == name,
            CaseSensitivity::ProfileFolded => pending_name.case_fold_key() == name.case_fold_key(),
        };
        (pending_parent == parent && same_name).then_some((pending_path.clone(), *file_id))
    })
}

fn pending_binding_id(
    path: &NamespacePath,
    pending: &[(NamespacePath, FileId)],
    case_sensitivity: CaseSensitivity,
) -> Option<FileId> {
    pending_binding_path(path, pending, case_sensitivity).map(|(_, file_id)| file_id)
}

fn pending_parent_exists(
    path: &NamespacePath,
    pending: &[(NamespacePath, FileId)],
    case_sensitivity: CaseSensitivity,
    limits: VolumeLimits,
) -> bool {
    let Some((parent, _)) = path.split_last() else {
        return false;
    };
    let parent_path = NamespacePath::new(parent.to_vec(), limits);
    parent_path
        .ok()
        .and_then(|value| pending_binding_id(&value, pending, case_sensitivity))
        .is_some()
}

fn removed_binding_id(path: &NamespacePath, removed: &[(NamespacePath, FileId)]) -> Option<FileId> {
    let (parent, name) = path.split_last()?;
    let folded = name.case_fold_key();
    removed.iter().rev().find_map(|(removed_path, file_id)| {
        let (removed_parent, removed_name) = removed_path.split_last()?;
        (removed_parent == parent && removed_name.case_fold_key() == folded).then_some(*file_id)
    })
}

/// Stable result positions for an authored transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthoredTransactionResult {
    /// One entry per authored operation; create operations contain their new
    /// path-independent identity and all other operations contain `None`.
    pub created_file_ids: Vec<Option<FileId>>,
}

/// Stable result positions plus the terminal outcome of one authored live transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthoredLiveMutationResult {
    /// One entry per authored operation; create operations contain their new
    /// path-independent identity and all other operations contain `None`.
    pub created_file_ids: Vec<Option<FileId>>,
    /// Terminal direct-live publication outcome.
    pub outcome: LiveMutationOutcome,
}

/// Authenticated content staged from a bounded stream without materializing the
/// complete file in memory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StagedContent {
    /// Root of the immutable chunk/index closure.
    root: ObjectId,
    /// Exact logical source length.
    logical_bytes: u64,
}

impl StagedContent {
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

/// One present no-follow path result with authenticated metadata decoded in
/// the same operation. `None` represents authenticated absence.
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
#[derive(Debug, Error)]
pub enum FsError {
    /// Operation was cancelled before further facade work began.
    #[error(transparent)]
    Cancelled(#[from] CancellationError),
    /// Volume configuration is invalid.
    #[error(transparent)]
    Config(#[from] VolumeConfigError),
    /// Native blocking storage execution could not be configured.
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[error(transparent)]
    NativeExecutor(#[from] crate::native_executor::NativeExecutorConfigError),
    /// Native storage execution failed before a backend receipt was available.
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[error(transparent)]
    NativeExecution(#[from] crate::native_executor::NativeExecutionError),
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
    /// `CaseSensitivity::ProfileFolded` policy once both are case-folded,
    /// even though their exact bytes differ.
    #[error("name collides with an existing entry under the volume's case-folding policy")]
    NameCollision,
    /// A new name is not canonical Unicode NFC, required by the volume's
    /// `UnicodePolicy::RequireNfc` policy.
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
    /// Authority storage failed.
    #[error(transparent)]
    Authority(#[from] AuthorityStoreError),
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
}

impl<A, O> Fs<A, O> {
    /// Composes explicit authority and object backends without another state store.
    #[must_use]
    pub fn new(authority: A, objects: O, capabilities: EmbeddedCapabilities) -> Self {
        Self::new_in_namespace(authority, objects, capabilities, [0; 16])
    }

    /// Composes explicit backends in one stable workspace-identity namespace.
    ///
    /// This is an engine-construction boundary. Customer constructors derive
    /// the namespace from their deployment rather than exposing it.
    #[must_use]
    pub fn new_in_namespace(
        authority: A,
        objects: O,
        capabilities: EmbeddedCapabilities,
        workspace_namespace: [u8; 16],
    ) -> Self {
        Self {
            inner: Arc::new(FsInner {
                authority,
                objects,
                capabilities,
                workspace_namespace,
            }),
        }
    }
}

impl<A, S> Fs<A, crate::cache::CachedObjectStore<S>> {
    /// Returns exact current and cumulative observations from the disposable
    /// immutable-object accelerator shared by every clone and volume handle.
    ///
    /// # Errors
    ///
    /// Fails closed if process-local accelerator synchronization was poisoned.
    pub fn object_cache_stats(&self) -> Result<crate::cache::ObjectCacheStats, ObjectStoreError> {
        self.inner.objects.stats()
    }

    /// Discards all resident accelerated bytes without changing authority or
    /// immutable backing storage. Concurrent reads remain correct and may
    /// repopulate the cache after this call.
    ///
    /// # Errors
    ///
    /// Fails closed if process-local accelerator synchronization was poisoned.
    pub fn clear_object_cache(&self) -> Result<(), ObjectStoreError> {
        self.inner.objects.clear()
    }
}

#[cfg(all(feature = "memory", target_arch = "wasm32"))]
impl Fs<crate::memory::MemoryAuthorityStore, crate::memory::MemoryObjectStore> {
    /// Creates the deterministic infrastructure-free memory composition.
    #[must_use]
    pub fn memory() -> Self {
        Self::new(
            crate::memory::MemoryAuthorityStore::default(),
            crate::memory::MemoryObjectStore::default(),
            EmbeddedCapabilities::MEMORY,
        )
    }

    /// Creates the deterministic infrastructure-free memory composition with
    /// an explicit immutable-object ceiling.
    ///
    /// # Errors
    ///
    /// Rejects a zero object bound before allocating backend state.
    pub fn memory_bounded(maximum_object_bytes: u64) -> Result<Self, FsError> {
        let objects = crate::memory::MemoryObjectStore::new(maximum_object_bytes)?;
        Ok(Self::new(
            crate::memory::MemoryAuthorityStore::default(),
            objects,
            EmbeddedCapabilities::MEMORY,
        ))
    }
}

#[cfg(all(
    feature = "memory",
    feature = "distributed",
    not(target_arch = "wasm32")
))]
impl
    Fs<
        crate::distributed::StreamAuthorityStore<acyclic_stream::MemoryStream>,
        crate::distributed::ProviderObjectStore<acyclic_objects::MemoryObjects>,
    >
{
    /// Creates the deterministic infrastructure-free composition from the exact public reference
    /// Stream and Objects providers used by hosted-service tests.
    #[must_use]
    pub fn memory() -> Self {
        let stream = std::sync::Arc::new(acyclic_stream::MemoryStream::default());
        let (objects, bucket) = acyclic_objects::MemoryObjects::with_default_bucket();
        Self::from_memory_providers(stream, std::sync::Arc::new(objects), bucket)
    }

    /// Creates the public-provider memory composition with an explicit aggregate object ceiling.
    ///
    /// # Errors
    ///
    /// Rejects a zero or process-unrepresentable object bound before allocating provider state.
    pub fn memory_bounded(maximum_object_bytes: u64) -> Result<Self, FsError> {
        let stream = std::sync::Arc::new(acyclic_stream::MemoryStream::default());
        let (objects, bucket) =
            acyclic_objects::MemoryObjects::with_bucket("acyclic-fs-memory", maximum_object_bytes)
                .map_err(|error| FsError::Object(ObjectStoreError::Rejected(error.to_string())))?;
        Ok(Self::from_memory_providers(
            stream,
            std::sync::Arc::new(objects),
            bucket,
        ))
    }

    /// Composes Filesystem over caller-owned public reference providers.
    ///
    /// Clones of the same providers may be retained to test cross-family behavior without any
    /// shadow filesystem storage implementation.
    #[must_use]
    pub fn from_memory_providers(
        stream: std::sync::Arc<acyclic_stream::MemoryStream>,
        objects: std::sync::Arc<acyclic_objects::MemoryObjects>,
        bucket: acyclic_objects::wire::BucketRef,
    ) -> Self {
        Self::new(
            crate::distributed::StreamAuthorityStore::new(stream),
            crate::distributed::ProviderObjectStore::new(objects, bucket),
            EmbeddedCapabilities::MEMORY,
        )
    }
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
impl
    Fs<
        crate::native_executor::NativeStore<crate::local_authority::LocalAuthorityStore>,
        crate::cache::CachedObjectStore<
            crate::native_executor::NativeStore<crate::local::LocalObjectStore>,
        >,
    >
{
    /// Opens the infrastructure-free durable local composition.
    ///
    /// # Errors
    ///
    /// Fails if limits are invalid or either durable backend cannot initialize.
    pub fn local(options: LocalOptions) -> Result<Self, FsError> {
        let LocalOptions {
            root,
            maximum_object_bytes,
            maximum_authority_payload_bytes,
            authority_checkpoint_pages,
            native_executor,
            object_cache,
        } = options;
        let executor = crate::native_executor::NativeExecutor::new(native_executor)?;
        let authority = crate::local_authority::LocalAuthorityStore::open(
            &root,
            crate::local_authority::LocalAuthorityConfig {
                max_payload_bytes: maximum_authority_payload_bytes,
                checkpoint_pages: authority_checkpoint_pages,
            },
        )?;
        let objects = crate::local::LocalObjectStore::open(&root, maximum_object_bytes)?;
        let objects = crate::cache::CachedObjectStore::new(
            crate::native_executor::NativeStore::new(objects, executor.clone()),
            object_cache,
        )?;
        Ok(Self::new(
            crate::native_executor::NativeStore::new(authority, executor),
            objects,
            EmbeddedCapabilities {
                durable: cfg!(any(unix, windows)),
            },
        ))
    }

    /// Reclaims unreachable local objects under an exclusive cross-process
    /// maintenance fence.
    ///
    /// Every normal local store holds a shared fence for its complete lifetime,
    /// so this operation fails while any embedded engine, checkout, mount, or
    /// direct local object-store consumer is open. It authenticates every
    /// authority head and complete generation closure before physical deletion.
    ///
    /// # Errors
    ///
    /// Returns a typed failure for an active local consumer, malformed authority
    /// history, incomplete/corrupt closure, cancellation, candidate/result
    /// bounds, storage failure, or work outside `budget`.
    pub async fn collect_local_garbage(
        options: LocalOptions,
        maximum_authorities: u32,
        maximum_candidates: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<crate::local::LocalGarbageCollection> {
        cancellation
            .check()
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        let executor = crate::native_executor::NativeExecutor::new(options.native_executor)
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        let root = options.root;
        let maximum_object_bytes = options.maximum_object_bytes;
        let object_cache = options.object_cache;
        let authority_config = crate::local_authority::LocalAuthorityConfig {
            max_payload_bytes: options.maximum_authority_payload_bytes,
            checkpoint_pages: options.authority_checkpoint_pages,
        };
        let (authority, objects) = executor
            .execute(cancellation, move || {
                let objects = crate::local::LocalObjectStore::open_for_maintenance(
                    &root,
                    maximum_object_bytes,
                )?;
                let authority =
                    crate::local_authority::LocalAuthorityStore::open(&root, authority_config)?;
                Ok::<_, FsError>((authority, objects))
            })
            .await
            .map_err(|error| OperationFailure::before_work(error.into()))?
            .map_err(OperationFailure::before_work)?;
        let objects = crate::cache::CachedObjectStore::new(
            crate::native_executor::NativeStore::new(objects, executor.clone()),
            object_cache,
        )
        .map_err(|error| OperationFailure::before_work(error.into()))?;
        let fs = Self::new(
            crate::native_executor::NativeStore::new(authority, executor.clone()),
            objects,
            EmbeddedCapabilities {
                durable: cfg!(any(unix, windows)),
            },
        );
        fs.collect_local_garbage_exclusive(
            maximum_authorities,
            maximum_candidates,
            budget,
            cancellation,
        )
        .await
    }

    async fn collect_local_garbage_exclusive(
        &self,
        maximum_authorities: u32,
        maximum_candidates: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<crate::local::LocalGarbageCollection> {
        let authorities = self
            .inner
            .authority
            .execute_backend(cancellation, move |authority| {
                authority.list_authorities(maximum_authorities, budget)
            })
            .await
            .map_err(|error| OperationFailure::before_work(error.into()))?
            .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
        let authority_live_bytes = u64::try_from(authorities.value.capacity())
            .unwrap_or(u64::MAX)
            .saturating_mul(
                u64::try_from(size_of::<crate::foundation::AuthorityId>()).unwrap_or(u64::MAX),
            );
        let mut work = authorities.work;
        let mut reachable = Vec::<ObjectId>::new();
        for authority_id in authorities.value {
            let Some((generation_root, config, next_work)) = self
                .local_authority_generation(
                    authority_id,
                    authority_live_bytes,
                    work,
                    budget,
                    cancellation,
                )
                .await?
            else {
                continue;
            };
            work = next_work;
            let live_bytes = authority_live_bytes.saturating_add(object_vec_bytes(&reachable));
            let proof = prove_generation_closure_async(
                &self.inner.objects,
                generation_root,
                closure_limits(config),
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
            work = merge_simultaneous_work(work, proof.work, live_bytes, budget)?;
            let incoming_bytes = object_vec_bytes(&proof.objects);
            merge_sorted_object_ids(
                &mut reachable,
                &proof.objects,
                incoming_bytes,
                authority_live_bytes,
                &mut work,
                budget,
            )?;
        }
        let collection_budget = remaining(work, budget)?;
        let collection_live_bytes = object_vec_bytes(&reachable);
        let collected = self
            .inner
            .objects
            .inner()
            .execute_backend(cancellation, move |objects| {
                objects.collect_garbage(&reachable, maximum_candidates, collection_budget)
            })
            .await
            .map_err(|error| OperationFailure::before_work(error.into()))?
            .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
        work = merge_simultaneous_work(
            work,
            collected.work,
            authority_live_bytes.saturating_add(collection_live_bytes),
            budget,
        )?;
        Ok(FsReceipt {
            value: collected.value,
            work,
        })
    }

    async fn local_authority_generation(
        &self,
        authority_id: crate::foundation::AuthorityId,
        retained_bytes: u64,
        mut work: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<Option<(ObjectId, VolumeConfig, WorkCounters)>, OperationFailure<FsError>> {
        cancellation
            .check()
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        let head = self
            .inner
            .authority
            .head(authority_id, remaining(work, budget)?, cancellation)
            .await
            .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
        work = merge_simultaneous_work(work, head.work, retained_bytes, budget)?;
        if head.value.sequence == Sequence::GENESIS {
            return Ok(None);
        }
        let creation = self
            .inner
            .authority
            .replay(
                authority_id,
                Sequence::GENESIS,
                ReplayLimit {
                    records: 1,
                    payload_bytes: MAXIMUM_VOLUME_EVENT_BYTES,
                },
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
        work = merge_simultaneous_work(work, creation.work, retained_bytes, budget)?;
        let first = creation
            .value
            .first()
            .ok_or_else(|| OperationFailure::new(FsError::InvalidAuthorityHistory, work))?;
        let Ok(created) = decode_volume_created(&first.payload, MAXIMUM_VOLUME_EVENT_BYTES) else {
            #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
            if let Ok(source_volume) =
                decode_source_volume(&first.payload, MAXIMUM_VOLUME_EVENT_BYTES)
            {
                if source_authority_id(source_volume) != authority_id {
                    return Err(OperationFailure::new(FsError::VolumeMismatch, work));
                }
                return Ok(None);
            }
            let retained = decode_retention_created(&first.payload, MAXIMUM_VOLUME_EVENT_BYTES)
                .map_err(|error| OperationFailure::new(error.into(), work))?;
            return Ok(Some((retained.generation_root, retained.config, work)));
        };
        if volume_authority_id(created.volume_id) != authority_id {
            return Err(OperationFailure::new(FsError::VolumeMismatch, work));
        }
        let generation_root = if head.value.sequence == Sequence::new(1) {
            created.initial_generation_root
        } else {
            let latest = self
                .inner
                .authority
                .replay(
                    authority_id,
                    Sequence::new(head.value.sequence.get().saturating_sub(1)),
                    ReplayLimit {
                        records: 1,
                        payload_bytes: MAXIMUM_VOLUME_EVENT_BYTES,
                    },
                    remaining(work, budget)?,
                    cancellation,
                )
                .await
                .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
            work = merge_simultaneous_work(work, latest.work, retained_bytes, budget)?;
            let record = latest
                .value
                .first()
                .ok_or_else(|| OperationFailure::new(FsError::InvalidAuthorityHistory, work))?;
            if let Ok(deleted) =
                decode_workspace_deleted(&record.payload, MAXIMUM_VOLUME_EVENT_BYTES)
            {
                if deleted != created.volume_id {
                    return Err(OperationFailure::new(FsError::VolumeMismatch, work));
                }
                return Ok(None);
            }
            generation_from_record(record, created.volume_id, work)?
        };
        Ok(Some((generation_root, created.config, work)))
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    pub(crate) async fn generation_parents(
        &self,
        volume: &Volume<A, O>,
        generation: GenerationId,
    ) -> Result<Vec<GenerationId>, crate::workspace::WorkspaceError> {
        let object = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: generation.digest(),
        };
        let (root, _) = read_generation_root(
            &self.inner.objects,
            object,
            volume.config,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(crate::workspace::WorkspaceError::engine)?;
        if root.volume_id != volume.id {
            return Err(crate::workspace::WorkspaceError::ForeignGeneration);
        }
        Ok(root.parents)
    }

    /// Creates or recovers one canonical named workspace with deployment-
    /// appropriate portable defaults.
    ///
    /// # Errors
    ///
    /// Rejects invalid names, incompatible durability, storage failures, or a
    /// pre-existing name with different immutable creation semantics.
    pub async fn create_workspace(
        &self,
        name: impl AsRef<str>,
    ) -> Result<crate::Workspace<A, O>, crate::workspace::WorkspaceError> {
        let lifecycle = if self.inner.capabilities.durable {
            Lifecycle::Durable
        } else {
            Lifecycle::Ephemeral
        };
        self.create_workspace_with_config(name, VolumeConfig::portable(lifecycle))
            .await
    }

    /// Creates or recovers one named workspace with exact filesystem semantics.
    ///
    /// # Errors
    ///
    /// Returns the same typed failures as [`Self::create_workspace`].
    pub async fn create_workspace_with_config(
        &self,
        name: impl AsRef<str>,
        config: VolumeConfig,
    ) -> Result<crate::Workspace<A, O>, crate::workspace::WorkspaceError> {
        let name = crate::WorkspaceName::new(name)?;
        let id = crate::WorkspaceId::derive(self.inner.workspace_namespace, &name);
        let volume = self
            .create_volume_with_id(
                id.volume_id(),
                config,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(crate::workspace::WorkspaceError::engine)?
            .value;
        volume
            .resolve_head_generation(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(crate::workspace::WorkspaceError::engine)?;
        Ok(crate::Workspace {
            name,
            id,
            volume,
            #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
            source: None,
        })
    }

    /// Opens one existing named workspace without a mutable catalog lookup.
    ///
    /// # Errors
    ///
    /// Rejects invalid or absent names and malformed or unsupported creation
    /// state.
    pub async fn open_workspace(
        &self,
        name: impl AsRef<str>,
    ) -> Result<crate::Workspace<A, O>, crate::workspace::WorkspaceError> {
        let name = crate::WorkspaceName::new(name)?;
        let id = crate::WorkspaceId::derive(self.inner.workspace_namespace, &name);
        let volume = self
            .open_volume(
                id.volume_id(),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(crate::workspace::WorkspaceError::engine)?
            .value;
        volume
            .resolve_head_generation(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(crate::workspace::WorkspaceError::engine)?;
        Ok(crate::Workspace {
            name,
            id,
            volume,
            #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
            source: None,
        })
    }

    /// Terminally deletes one named workspace without requiring a live handle.
    /// This stateless form preserves exact retry resolution after the durable
    /// tombstone makes ordinary workspace opening fail closed.
    ///
    /// # Errors
    ///
    /// Rejects invalid or absent names and returns typed storage failures.
    pub async fn delete_workspace(
        &self,
        name: impl AsRef<str>,
        idempotency_key: crate::IdempotencyKey,
    ) -> Result<crate::workspace::WorkspaceDelete, crate::workspace::WorkspaceError> {
        let name = crate::WorkspaceName::new(name)?;
        let id = crate::WorkspaceId::derive(self.inner.workspace_namespace, &name);
        let volume = self
            .open_volume(
                id.volume_id(),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(crate::workspace::WorkspaceError::engine)?
            .value;
        self.delete_workspace_volume(&volume, idempotency_key.operation_id())
            .await
    }

    /// Creates a new workspace at one exact immutable source generation while
    /// sharing the complete file table and every unchanged content object.
    ///
    /// Only one small generation root and one creation fact are new. The source
    /// and destination then publish independently.
    pub(crate) async fn fork_workspace(
        &self,
        destination: crate::WorkspaceName,
        source: &crate::Generation<A, O>,
        idempotency_key: crate::IdempotencyKey,
    ) -> Result<crate::Workspace<A, O>, crate::workspace::WorkspaceError> {
        if !Arc::ptr_eq(&self.inner, &source.workspace.volume.fs.inner) {
            return Err(crate::workspace::WorkspaceError::ForeignGeneration);
        }
        let destination_id =
            crate::WorkspaceId::derive(self.inner.workspace_namespace, &destination);
        let config = source.workspace.volume.config;
        let cancellation = CancellationToken::new();
        let source_object = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: source.id.digest(),
        };
        let (source_root, mut work) = read_generation_root(
            &self.inner.objects,
            source_object,
            config,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(crate::workspace::WorkspaceError::engine)?;
        if source_root.volume_id != source.workspace.volume.id {
            return Err(crate::workspace::WorkspaceError::ForeignGeneration);
        }
        let fork_root = GenerationRoot {
            volume_id: destination_id.volume_id(),
            root_file_id: source_root.root_file_id,
            file_table: source_root.file_table,
            parents: vec![source.id],
            required_features: source_root.required_features,
        };
        let encoded =
            encode_generation_root(&fork_root).map_err(crate::workspace::WorkspaceError::engine)?;
        let (generation_root, next_work) = self
            .put_encoded(
                ObjectKind::GenerationRoot,
                encoded,
                work,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(crate::workspace::WorkspaceError::engine)?;
        work = next_work;
        let proof = prove_generation_closure_async(
            &self.inner.objects,
            generation_root,
            closure_limits(config),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(crate::workspace::WorkspaceError::engine)?;
        work = add(work, proof.work).map_err(crate::workspace::WorkspaceError::engine)?;
        self.retain_workspace_generation(
            &source.workspace.volume,
            source.id,
            RetentionKind::ForkBase,
            hex::encode(destination_id.into_bytes()),
        )
        .await?;
        if self.inner.authority.supports_native_generation_fork() {
            let forked = self
                .inner
                .authority
                .fork_generation_authority(
                    volume_authority_id(source.workspace.volume.id),
                    source.id,
                    volume_authority_id(destination_id.volume_id()),
                    idempotency_key.operation_id(),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
                .map_err(crate::workspace::WorkspaceError::engine)?;
            work = add(work, forked.work).map_err(crate::workspace::WorkspaceError::engine)?;
        }
        let volume = self
            .publish_volume_creation(
                VolumeCreation {
                    volume_id: destination_id.volume_id(),
                    config,
                    generation_root,
                    operation_id: Some(idempotency_key.operation_id()),
                },
                work,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(crate::workspace::WorkspaceError::engine)?
            .value;
        volume
            .resolve_head_generation(WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(crate::workspace::WorkspaceError::engine)?;
        Ok(crate::Workspace {
            name: destination,
            id: destination_id,
            volume,
            #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
            source: None,
        })
    }

    pub(crate) async fn retain_workspace_generation(
        &self,
        volume: &Volume<A, O>,
        generation_id: GenerationId,
        kind: RetentionKind,
        label: String,
    ) -> Result<(), crate::workspace::WorkspaceError> {
        let cancellation = CancellationToken::new();
        let generation_root = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: generation_id.digest(),
        };
        let proof = prove_generation_closure_async(
            &self.inner.objects,
            generation_root,
            closure_limits(volume.config),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(crate::workspace::WorkspaceError::engine)?;
        if proof.root.volume_id != volume.id {
            return Err(crate::workspace::WorkspaceError::ForeignGeneration);
        }
        let authority_id = retention_authority_id(volume.id, kind, &label);
        let created = self
            .inner
            .authority
            .create_authority(
                authority_id,
                Epoch::GENESIS,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(crate::workspace::WorkspaceError::engine)?;
        let active_head = match created.value {
            CreateAuthorityOutcome::Created(head) | CreateAuthorityOutcome::Existing(head) => head,
        };
        let payload = encode_retention_created(&RetentionCreated {
            volume_id: volume.id,
            kind,
            label,
            generation_root,
            config: volume.config,
        })
        .map_err(crate::workspace::WorkspaceError::engine)?;
        let operation_id = OperationId::from_bytes(authority_id.into_bytes());
        let (commit, _) = creation_commit(operation_id, payload);
        let appended = self
            .inner
            .authority
            .compare_and_append(
                authority_id,
                active_head.epoch,
                Head::genesis(active_head.epoch),
                commit,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(crate::workspace::WorkspaceError::engine)?;
        match appended.value {
            AppendOutcome::Committed(_) | AppendOutcome::AlreadyCommitted(_) => Ok(()),
            AppendOutcome::Conflict { .. }
            | AppendOutcome::Fenced { .. }
            | AppendOutcome::IdempotencyConflict { .. } => {
                Err(crate::workspace::WorkspaceError::RetentionConflict)
            }
        }
    }

    pub(crate) async fn delete_workspace_volume(
        &self,
        volume: &Volume<A, O>,
        operation_id: OperationId,
    ) -> Result<crate::workspace::WorkspaceDelete, crate::workspace::WorkspaceError> {
        let cancellation = CancellationToken::new();
        let authority_id = volume_authority_id(volume.id);
        let head = self
            .inner
            .authority
            .head(authority_id, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(crate::workspace::WorkspaceError::engine)?
            .value;
        if head.sequence == Sequence::GENESIS {
            return Err(crate::workspace::WorkspaceError::engine(
                FsError::EmptyAuthority,
            ));
        }
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
            .map_err(crate::workspace::WorkspaceError::engine)?;
        let latest = latest.value.first().ok_or_else(|| {
            crate::workspace::WorkspaceError::engine(FsError::InvalidAuthorityHistory)
        })?;
        if let Ok(deleted) = decode_workspace_deleted(&latest.payload, MAXIMUM_VOLUME_EVENT_BYTES) {
            if deleted != volume.id {
                return Err(crate::workspace::WorkspaceError::engine(
                    FsError::VolumeMismatch,
                ));
            }
            return Ok(crate::workspace::WorkspaceDelete::AlreadyDeleted);
        }
        generation_from_record(latest, volume.id, WorkCounters::default())
            .map_err(crate::workspace::WorkspaceError::engine)?;
        let (commit, _) = creation_commit(operation_id, encode_workspace_deleted(volume.id));
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
            .map_err(crate::workspace::WorkspaceError::engine)?;
        Ok(match appended.value {
            AppendOutcome::Committed(_) | AppendOutcome::AlreadyCommitted(_) => {
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

    pub(crate) async fn workspace_common_ancestor(
        &self,
        source: &crate::Generation<A, O>,
        target: &crate::Generation<A, O>,
        maximum_generations: u32,
    ) -> Result<GenerationId, crate::workspace::WorkspaceError> {
        if maximum_generations == 0
            || !Arc::ptr_eq(&self.inner, &source.workspace.volume.fs.inner)
            || !Arc::ptr_eq(&self.inner, &target.workspace.volume.fs.inner)
        {
            return Err(crate::workspace::WorkspaceError::ForeignGeneration);
        }
        if source.workspace.volume.config != target.workspace.volume.config {
            return Err(crate::workspace::WorkspaceError::IncompatibleWorkspace);
        }
        let cancellation = CancellationToken::new();
        let target_ancestors = collect_generation_ancestors(
            &self.inner.objects,
            target.id,
            target.workspace.volume.config,
            maximum_generations,
            &cancellation,
        )
        .await?;
        find_first_generation_ancestor(
            &self.inner.objects,
            source.id,
            source.workspace.volume.config,
            maximum_generations,
            &target_ancestors,
            &cancellation,
        )
        .await
    }

    pub(crate) async fn workspace_head_state(
        &self,
        workspace: &Volume<A, O>,
    ) -> Result<(GenerationId, Head), crate::workspace::WorkspaceError> {
        let (root, head, _) = workspace
            .resolve_head_generation(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(crate::workspace::WorkspaceError::engine)?;
        Ok((GenerationId::new(root.digest), head))
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) async fn live_rebase_workspace(
        &self,
        request: WorkspaceRebaseRequest<'_, A, O>,
    ) -> Result<WorkspaceJoinOutcome, crate::workspace::WorkspaceError> {
        let WorkspaceRebaseRequest {
            target,
            operation_id,
            maximum_generations,
            maximum_changes,
            maximum_conflicts,
        } = request;
        if maximum_generations == 0 || maximum_changes == 0 || maximum_conflicts == 0 {
            return Err(crate::workspace::WorkspaceError::JoinLimit);
        }
        let cancellation = CancellationToken::new();
        let (target_object, expected_head, _) = target
            .resolve_head_generation(WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(crate::workspace::WorkspaceError::engine)?;
        let (target_root, _) = read_generation_root(
            &self.inner.objects,
            target_object,
            target.config,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(crate::workspace::WorkspaceError::engine)?;

        let mut cursor = target_object;
        let mut adopted = None;
        for _ in 0..maximum_generations {
            let (root, _) = read_generation_root(
                &self.inner.objects,
                cursor,
                target.config,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(crate::workspace::WorkspaceError::engine)?;
            if root.volume_id != target.id {
                adopted = Some((cursor, root));
                break;
            }
            let Some(parent) = root.parents.first() else {
                return Err(crate::workspace::WorkspaceError::NotFork);
            };
            cursor = ObjectId {
                kind: ObjectKind::GenerationRoot,
                digest: parent.digest(),
            };
        }
        let Some((base_object, base_root)) = adopted else {
            return Err(crate::workspace::WorkspaceError::LineageLimit);
        };
        let source = self
            .open_volume(base_root.volume_id, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(crate::workspace::WorkspaceError::engine)?
            .value;
        if source.config != target.config || base_root.root_file_id != target_root.root_file_id {
            return Err(crate::workspace::WorkspaceError::IncompatibleWorkspace);
        }
        let (source_object, _, _) = source
            .resolve_head_generation(WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(crate::workspace::WorkspaceError::engine)?;
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
        .map_err(crate::workspace::WorkspaceError::engine)?;
        if source_root.volume_id != source.id
            || source_root.root_file_id != target_root.root_file_id
        {
            return Err(crate::workspace::WorkspaceError::IncompatibleWorkspace);
        }

        let normalize = |root: &GenerationRoot| GenerationRoot {
            volume_id: target.id,
            root_file_id: target_root.root_file_id,
            file_table: root.file_table,
            parents: Vec::new(),
            required_features: root.required_features,
        };
        let normalized_base = normalize(&base_root);
        let normalized_source = normalize(&source_root);
        let (normalized_base_object, _) = self
            .put_encoded(
                ObjectKind::GenerationRoot,
                encode_generation_root(&normalized_base)
                    .map_err(crate::workspace::WorkspaceError::engine)?,
                WorkCounters::default(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(crate::workspace::WorkspaceError::engine)?;
        let (normalized_source_object, _) = self
            .put_encoded(
                ObjectKind::GenerationRoot,
                encode_generation_root(&normalized_source)
                    .map_err(crate::workspace::WorkspaceError::engine)?,
                WorkCounters::default(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(crate::workspace::WorkspaceError::engine)?;
        let merged = merge_generation_async(
            &self.inner.objects,
            MergeGenerationRequest {
                base_generation: normalized_base_object,
                base: normalized_base,
                ours_generation: Some(normalized_source_object),
                ours: normalized_source,
                theirs_generation: target_object,
                theirs: target_root,
                retain_theirs_parent: false,
                maximum_changes,
                maximum_conflicts,
            },
            decode_limits(target.config),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(crate::workspace::WorkspaceError::engine)?;
        let merged_root = match merged.value {
            MergeGenerationOutcome::Conflicted {
                conflicts,
                truncated,
            } => return Ok(WorkspaceJoinOutcome::Conflicted(conflicts, truncated)),
            MergeGenerationOutcome::Prepared { root, .. } => root,
        };
        let rebased_root = GenerationRoot {
            volume_id: target.id,
            root_file_id: merged_root.root_file_id,
            file_table: merged_root.file_table,
            parents: vec![GenerationId::new(source_object.digest)],
            required_features: merged_root.required_features,
        };
        let (candidate, _) = self
            .put_encoded(
                ObjectKind::GenerationRoot,
                encode_generation_root(&rebased_root)
                    .map_err(crate::workspace::WorkspaceError::engine)?,
                WorkCounters::default(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(crate::workspace::WorkspaceError::engine)?;
        let publication = publish_generation_async(
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
        .map_err(crate::workspace::WorkspaceError::engine)?;
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
                    .map_err(crate::workspace::WorkspaceError::engine)?;
                WorkspaceJoinOutcome::Stale(GenerationId::new(actual.digest))
            }
            AppendOutcome::Fenced { .. } => WorkspaceJoinOutcome::Fenced,
            AppendOutcome::IdempotencyConflict { .. } => WorkspaceJoinOutcome::IdempotencyConflict,
        })
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) async fn apply_workspace_join(
        &self,
        request: WorkspaceJoinRequest<'_, A, O>,
    ) -> Result<WorkspaceJoinOutcome, crate::workspace::WorkspaceError> {
        let WorkspaceJoinRequest {
            target,
            base,
            source,
            expected_target,
            expected_head,
            history,
            operation_id,
            maximum_changes,
            maximum_conflicts,
        } = request;
        if maximum_changes == 0 || maximum_conflicts == 0 {
            return Err(crate::workspace::WorkspaceError::JoinLimit);
        }
        if !Arc::ptr_eq(&self.inner, &source.workspace.volume.fs.inner)
            || source.workspace.volume.config != target.config
        {
            return Err(crate::workspace::WorkspaceError::IncompatibleWorkspace);
        }
        let cancellation = CancellationToken::new();
        let (current_target_object, _, _) = target
            .resolve_head_generation(WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(crate::workspace::WorkspaceError::engine)?;
        let current_target = GenerationId::new(current_target_object.digest);
        let target_object = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: expected_target.digest(),
        };
        let base_object = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: base.digest(),
        };
        let source_object = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: source.id.digest(),
        };
        let (base_root, _) = read_generation_root(
            &self.inner.objects,
            base_object,
            target.config,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(crate::workspace::WorkspaceError::engine)?;
        let (source_root, _) = read_generation_root(
            &self.inner.objects,
            source_object,
            target.config,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(crate::workspace::WorkspaceError::engine)?;
        let (target_root, _) = read_generation_root(
            &self.inner.objects,
            target_object,
            target.config,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(crate::workspace::WorkspaceError::engine)?;
        if source_root.volume_id != source.workspace.volume.id
            || target_root.volume_id != target.id
            || base_root.root_file_id != target_root.root_file_id
            || source_root.root_file_id != target_root.root_file_id
        {
            return Err(crate::workspace::WorkspaceError::IncompatibleWorkspace);
        }
        let normalized_base = GenerationRoot {
            volume_id: target.id,
            root_file_id: target_root.root_file_id,
            file_table: base_root.file_table,
            parents: base_root.parents,
            required_features: base_root.required_features,
        };
        let normalized_source = GenerationRoot {
            volume_id: target.id,
            root_file_id: target_root.root_file_id,
            file_table: source_root.file_table,
            parents: source_root.parents,
            required_features: source_root.required_features,
        };
        let (normalized_base_object, _) = self
            .put_encoded(
                ObjectKind::GenerationRoot,
                encode_generation_root(&normalized_base)
                    .map_err(crate::workspace::WorkspaceError::engine)?,
                WorkCounters::default(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(crate::workspace::WorkspaceError::engine)?;
        let (normalized_source_object, _) = self
            .put_encoded(
                ObjectKind::GenerationRoot,
                encode_generation_root(&normalized_source)
                    .map_err(crate::workspace::WorkspaceError::engine)?,
                WorkCounters::default(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(crate::workspace::WorkspaceError::engine)?;
        let merged = merge_generation_async(
            &self.inner.objects,
            MergeGenerationRequest {
                base_generation: normalized_base_object,
                base: normalized_base,
                ours_generation: Some(target_object),
                ours: target_root.clone(),
                theirs_generation: normalized_source_object,
                theirs: normalized_source,
                retain_theirs_parent: history == crate::workspace::JoinHistory::Merge,
                maximum_changes,
                maximum_conflicts,
            },
            decode_limits(target.config),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(crate::workspace::WorkspaceError::engine)?;
        let (candidate, merged_root) = match merged.value {
            MergeGenerationOutcome::Conflicted {
                conflicts,
                truncated,
            } => return Ok(WorkspaceJoinOutcome::Conflicted(conflicts, truncated)),
            MergeGenerationOutcome::Prepared {
                generation_root,
                root,
                ..
            } => (generation_root, root),
        };
        if merged_root.file_table == target_root.file_table {
            return Ok(WorkspaceJoinOutcome::NoChanges(current_target));
        }
        let publication = publish_generation_async(
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
        .map_err(crate::workspace::WorkspaceError::engine)?;
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
                    .map_err(crate::workspace::WorkspaceError::engine)?;
                WorkspaceJoinOutcome::Stale(GenerationId::new(actual.digest))
            }
            AppendOutcome::Fenced { .. } => WorkspaceJoinOutcome::Fenced,
            AppendOutcome::IdempotencyConflict { .. } => WorkspaceJoinOutcome::IdempotencyConflict,
        })
    }

    /// Executes one active local-residency prediction through this filesystem's
    /// ordinary authenticated object backend and shared cache.
    ///
    /// # Errors
    ///
    /// Returns a stale-token, cancellation, authentication, storage, or exact
    /// work-budget failure. It never changes filesystem authority.
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
    ///
    /// # Errors
    ///
    /// Returns a typed measured failure for invalid/unsupported semantics,
    /// cancellation, storage, authentication, authority conflict, or budget.
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
    ///
    /// # Errors
    ///
    /// Returns a typed measured failure for invalid/unsupported semantics,
    /// cancellation, storage, authentication, authority conflict, or budget.
    pub async fn create_volume_with_id(
        &self,
        volume_id: VolumeId,
        config: VolumeConfig,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Volume<A, O>> {
        cancellation
            .check()
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        validate_volume_capabilities(config, self.inner.capabilities)?;
        let (generation_root, mut work) = self
            .build_empty_generation(volume_id, config, budget, cancellation)
            .await?;
        let closure_limits = closure_limits(config);
        let remaining_budget = remaining(work, budget)?;
        let proof = prove_generation_closure_async(
            &self.inner.objects,
            generation_root,
            closure_limits,
            remaining_budget,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
        work = add(work, proof.work)?;
        self.publish_volume_creation(
            VolumeCreation {
                volume_id,
                config,
                generation_root,
                operation_id: None,
            },
            work,
            budget,
            cancellation,
        )
        .await
    }

    /// Opens one volume by reconstructing its immutable configuration from the
    /// first bounded authority fact.
    ///
    /// # Errors
    ///
    /// Returns a typed measured failure when authority is absent/corrupt, its
    /// creation fact is invalid, semantics are unsupported, or work is denied.
    pub async fn open_volume(
        &self,
        volume_id: VolumeId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Volume<A, O>> {
        cancellation
            .check()
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        let (creation, work) = self
            .read_creation(volume_id, WorkCounters::default(), budget, cancellation)
            .await?;
        validate_volume_capabilities(creation.config, self.inner.capabilities)
            .map_err(|failure| OperationFailure::new(failure.error, work))?;
        Ok(FsReceipt {
            value: Volume {
                fs: self.clone(),
                id: volume_id,
                config: creation.config,
            },
            work,
        })
    }

    /// Reads one exact immutable object for resumable manifest transfer.
    ///
    /// # Errors
    ///
    /// Returns typed identity, corruption, cancellation, backend, size, or
    /// work-budget failures. The object is always reauthenticated by storage.
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
    ///
    /// # Errors
    ///
    /// Rejects an identity/body mismatch before admitting visible authority and
    /// returns exact cancellation, backend, allocation, or work failures.
    pub async fn import_object(
        &self,
        object_id: ObjectId,
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        let receipt = self
            .inner
            .objects
            .put(object_id, bytes, budget, cancellation)
            .await
            .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
        Ok(FsReceipt {
            value: (),
            work: receipt.work,
        })
    }

    /// Reads one ordered resumable page from a canonical generation manifest.
    ///
    /// Backends with a physical multi-get primitive may service the complete
    /// page in one operation. The terminal cursor returns an empty terminal
    /// page without storage work.
    ///
    /// # Errors
    ///
    /// Returns typed manifest, cursor, cancellation, allocation, storage, or
    /// bounded-work failures with exact retained-buffer accounting.
    pub async fn export_generation_batch(
        &self,
        manifest: &GenerationExportManifest,
        cursor: TransferCursor,
        maximum_objects: u32,
        maximum_object_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<GenerationTransferBatch> {
        export_generation_batch_async(
            &self.inner.objects,
            manifest,
            cursor,
            maximum_objects,
            maximum_object_bytes,
            budget,
            cancellation,
        )
        .await
        .map_err(|failure| OperationFailure::new(map_transfer_error(failure.error), *failure.work))
    }

    /// Idempotently imports one manifest-aligned page of immutable bodies.
    ///
    /// A failed page may leave a valid immutable prefix; retrying the same
    /// cursor and bodies is exactly safe because every put authenticates its
    /// canonical object identity.
    ///
    /// # Errors
    ///
    /// Returns typed manifest, cursor/body bound, cancellation, storage, or
    /// exact-work failures while retaining imported-prefix work.
    pub async fn import_generation_batch(
        &self,
        manifest: &GenerationExportManifest,
        cursor: TransferCursor,
        bodies: &[Bytes],
        maximum_objects: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<TransferCursor> {
        import_generation_batch_async(
            &self.inner.objects,
            manifest,
            cursor,
            bodies,
            maximum_objects,
            budget,
            cancellation,
        )
        .await
        .map_err(|failure| OperationFailure::new(map_transfer_error(failure.error), *failure.work))
    }

    /// Restores a volume authority from a fully imported immutable generation.
    ///
    /// The complete closure is authenticated before the first authority fact
    /// can expose it. Retrying with the same caller-owned operation identity is
    /// idempotent; changing its input is rejected by authority publication.
    ///
    /// # Errors
    ///
    /// Returns typed configuration, volume-identity, closure, authority,
    /// cancellation, or bounded-work failures.
    pub async fn restore_volume(
        &self,
        manifest: &GenerationExportManifest,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Volume<A, O>> {
        cancellation
            .check()
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        validate_volume_capabilities(manifest.config, self.inner.capabilities)?;
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
        self.publish_volume_creation(
            VolumeCreation {
                volume_id: manifest.volume_id,
                config: manifest.config,
                generation_root: manifest.generation_root,
                operation_id: Some(operation_id),
            },
            work,
            budget,
            cancellation,
        )
        .await
    }

    async fn publish_volume_creation(
        &self,
        creation: VolumeCreation,
        mut work: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Volume<A, O>> {
        let VolumeCreation {
            volume_id,
            config,
            generation_root,
            operation_id,
        } = creation;
        let authority_id = volume_authority_id(volume_id);
        let created = self
            .inner
            .authority
            .create_authority(
                authority_id,
                Epoch::GENESIS,
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
        work = add(work, created.work)?;
        let active_head = match created.value {
            CreateAuthorityOutcome::Created(head) | CreateAuthorityOutcome::Existing(head) => head,
        };
        let event = encode_volume_created(VolumeCreated {
            volume_id,
            config,
            initial_generation_root: generation_root,
        })
        .map_err(|error| OperationFailure::new(error.into(), work))?;
        let operation_id = match operation_id {
            Some(operation_id) => operation_id,
            None => {
                let (operation_id, identity_work) = derived_operation_id(volume_id);
                work = add(work, identity_work)?;
                operation_id
            }
        };
        let (commit, encoding_work) = creation_commit(operation_id, event);
        work = add(work, encoding_work)?;
        work.verify(budget)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        let appended = self
            .inner
            .authority
            .compare_and_append(
                authority_id,
                active_head.epoch,
                Head::genesis(active_head.epoch),
                commit,
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
        work = add(work, appended.work)?;
        match appended.value {
            AppendOutcome::Committed(_) | AppendOutcome::AlreadyCommitted(_) => Ok(FsReceipt {
                value: Volume {
                    fs: self.clone(),
                    id: volume_id,
                    config,
                },
                work,
            }),
            AppendOutcome::Conflict { .. }
            | AppendOutcome::Fenced { .. }
            | AppendOutcome::IdempotencyConflict { .. } => {
                Err(OperationFailure::new(FsError::CreationRejected, work))
            }
        }
    }

    async fn read_creation(
        &self,
        volume_id: VolumeId,
        work: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<(VolumeCreated, WorkCounters), OperationFailure<FsError>> {
        let records = self
            .inner
            .authority
            .replay(
                volume_authority_id(volume_id),
                Sequence::GENESIS,
                ReplayLimit {
                    records: 1,
                    payload_bytes: MAXIMUM_VOLUME_EVENT_BYTES,
                },
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
        let combined = add(work, records.work)?;
        let first = records
            .value
            .first()
            .ok_or_else(|| OperationFailure::new(FsError::EmptyAuthority, combined))?;
        let creation = decode_volume_created(&first.payload, MAXIMUM_VOLUME_EVENT_BYTES)
            .map_err(|error| OperationFailure::new(error.into(), combined))?;
        if creation.volume_id != volume_id {
            return Err(OperationFailure::new(FsError::VolumeMismatch, combined));
        }
        Ok((creation, combined))
    }

    async fn build_empty_generation(
        &self,
        volume_id: VolumeId,
        config: VolumeConfig,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<(ObjectId, WorkCounters), OperationFailure<FsError>> {
        let mut work = WorkCounters::default();
        let metadata = self
            .put_encoded(
                ObjectKind::Metadata,
                encode_file_metadata(empty_metadata())
                    .map_err(|error| OperationFailure::new(error.into(), work))?,
                work,
                budget,
                cancellation,
            )
            .await?;
        work = metadata.1;
        let tree = self
            .put_encoded(
                ObjectKind::TreePage,
                encode_tree_page(
                    &TreePage::Leaf(Vec::new()),
                    config.limits.maximum_directory_page_entries,
                )
                .map_err(|error| OperationFailure::new(error.into(), work))?,
                work,
                budget,
                cancellation,
            )
            .await?;
        work = tree.1;
        let (root_file_id, identity_work) = derived_file_id(volume_id);
        work = add(work, identity_work)?;
        let records = FileTablePage::Leaf(vec![FileRecord {
            file_id: root_file_id,
            kind: FileKind::Directory,
            link_count: 1,
            metadata: metadata.0,
            payload: FilePayload::Directory { entries: tree.0 },
        }]);
        work = add(
            work,
            WorkCounters {
                allocation_operations: 1,
                peak_allocation_bytes: u64::try_from(size_of::<FileRecord>()).unwrap_or(u64::MAX),
                ..WorkCounters::default()
            },
        )?;
        let encoded_file_table =
            encode_file_table_page(&records, config.limits.maximum_directory_page_entries)
                .map_err(|error| OperationFailure::new(error.into(), work))?;
        drop(records);
        let file_table = self
            .put_encoded(
                ObjectKind::FileTablePage,
                encoded_file_table,
                work,
                budget,
                cancellation,
            )
            .await?;
        work = file_table.1;
        let root = GenerationRoot {
            volume_id,
            root_file_id,
            file_table: file_table.0,
            parents: Vec::new(),
            required_features: 0,
        };
        self.put_encoded(
            ObjectKind::GenerationRoot,
            encode_generation_root(&root)
                .map_err(|error| OperationFailure::new(error.into(), work))?,
            work,
            budget,
            cancellation,
        )
        .await
    }

    async fn put_encoded(
        &self,
        kind: ObjectKind,
        encoded: Vec<u8>,
        work: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<(ObjectId, WorkCounters), OperationFailure<FsError>> {
        let length = u64::try_from(encoded.len()).unwrap_or(u64::MAX);
        let semantic = WorkCounters {
            bytes_encoded: length,
            bytes_hashed: length.saturating_add(OBJECT_DIGEST_ENVELOPE_BYTES),
            allocation_operations: 1,
            peak_allocation_bytes: u64::try_from(encoded.capacity()).unwrap_or(u64::MAX),
            ..WorkCounters::default()
        };
        let prospective = add(work, semantic)?;
        prospective
            .verify(budget)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        let object = ObjectId {
            kind,
            digest: object_digest(kind, &encoded),
        };
        let live_bytes = semantic.peak_allocation_bytes;
        let mut backend_budget = remaining(prospective, budget)?;
        // `prospective.verify` above proves the encoded buffer fits the
        // operation-wide peak, so this subtraction is total.
        backend_budget.peak_allocation_bytes -= live_bytes;
        let receipt = self
            .inner
            .objects
            .put(object, Bytes::from(encoded), backend_budget, cancellation)
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
    ///
    /// File-record changes are keyed by stable identity, while namespace changes
    /// are keyed by stable parent-directory identity and exact logical name. This
    /// represents hard links and renames without inventing one canonical path.
    /// Equal generation roots return with zero backend work.
    ///
    /// # Errors
    ///
    /// Rejects zero bounds, foreign/malformed generations, storage corruption,
    /// cancellation, allocation failure, or work beyond the admitted budget.
    #[allow(clippy::too_many_lines)]
    pub async fn diff_generations(
        &self,
        before: GenerationId,
        after: GenerationId,
        maximum_changes: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<GenerationDiff> {
        if maximum_changes == 0 {
            return Err(OperationFailure::before_work(FsError::InvalidDiff));
        }
        if before == after {
            return Ok(OperationReceipt {
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
        let records = diff_file_records_async(
            &self.fs.inner.objects,
            Some(before_root.file_table),
            Some(after_root.file_table),
            maximum_changes,
            decode_limits(self.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| map_diff_failure(failure, work))?;
        work = add(work, records.work)?;
        let maximum = usize::try_from(maximum_changes).unwrap_or(usize::MAX);
        let mut files = Vec::new();
        files
            .try_reserve(records.changes.len())
            .map_err(|_| OperationFailure::new(FsError::DiffAllocationFailed, work))?;
        let mut bindings = Vec::new();
        let mut truncated = records.truncated;
        for change in records.changes {
            if files.len() + bindings.len() >= maximum {
                truncated = true;
                break;
            }
            let before_entries = directory_entries(change.before);
            let after_entries = directory_entries(change.after);
            files.push(FileRecordChange {
                file_id: change.key,
                before: change.before,
                after: change.after,
            });
            if before_entries == after_entries {
                continue;
            }
            let remaining_changes = maximum.saturating_sub(files.len() + bindings.len());
            if remaining_changes == 0 {
                truncated = true;
                continue;
            }
            let entries = diff_tree_entries_async(
                &self.fs.inner.objects,
                before_entries,
                after_entries,
                u32::try_from(remaining_changes).unwrap_or(u32::MAX),
                decode_limits(self.config),
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| map_diff_failure(failure, work))?;
            work = add(work, entries.work)?;
            truncated |= entries.truncated;
            bindings.extend(
                entries
                    .changes
                    .into_iter()
                    .map(|entry| DirectoryBindingChange {
                        directory_id: change.key,
                        name: entry.key,
                        before: entry.before,
                        after: entry.after,
                    }),
            );
        }
        Ok(OperationReceipt {
            value: GenerationDiff {
                files,
                bindings,
                truncated,
            },
            work,
        })
    }

    /// Opens one immutable pinned checkout after authenticating its bounded root.
    ///
    /// # Errors
    ///
    /// Returns a typed measured failure for unsupported modes, missing/corrupt
    /// generation roots, volume mismatch, authority inconsistency,
    /// cancellation, or work outside the admitted budget. Descendant objects
    /// are authenticated lazily by the exact operation that demands them.
    pub async fn checkout(
        &self,
        selector: GenerationSelector,
        mode: CheckoutMode,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Checkout<A, O>> {
        cancellation
            .check()
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        validate_checkout(mode, self.config)?;
        let mut work = WorkCounters::default();
        let (generation_root, mut authority_head) = match selector {
            GenerationSelector::Exact(generation_id) => {
                if mode.access == AccessMode::ReadWrite {
                    return Err(OperationFailure::before_work(
                        FsError::WritableCheckoutRequiresHead,
                    ));
                }
                (
                    ObjectId {
                        kind: ObjectKind::GenerationRoot,
                        digest: generation_id.digest(),
                    },
                    None,
                )
            }
            GenerationSelector::Head => {
                let resolved = self
                    .resolve_head_generation(remaining(work, budget)?, cancellation)
                    .await
                    .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
                work = add(work, resolved.2)?;
                (resolved.0, Some(resolved.1))
            }
        };
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
        let dependencies = CheckoutDependencies::new(
            std::iter::empty(),
            std::iter::empty(),
            self.config.limits.maximum_checkout_dependencies,
        )
        .map_err(|error| OperationFailure::new(error.into(), work))?;
        Ok(FsReceipt {
            value: Checkout {
                volume: self.clone(),
                base_generation_root: generation_root,
                generation_root,
                base_file_table: root.file_table,
                base_root: root.clone(),
                root,
                authority_head,
                pending_operations: Vec::new(),
                live_operation_id: None,
                last_commit: None,
                prepared_merge_parent: None,
                dependencies,
                mode,
            },
            work,
        })
    }

    async fn resolve_head_generation(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<(ObjectId, Head, WorkCounters), OperationFailure<FsError>> {
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
        if let Ok(deleted) = decode_workspace_deleted(&latest.payload, MAXIMUM_VOLUME_EVENT_BYTES) {
            if deleted != self.id {
                return Err(OperationFailure::new(FsError::VolumeMismatch, work));
            }
            return Err(OperationFailure::new(FsError::WorkspaceDeleted, work));
        }
        let generation_root = generation_from_record(latest, self.id, work)?;
        Ok((generation_root, head.value, work))
    }
}

impl<A, O> Checkout<A, O> {
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
    pub const fn root(&self) -> &GenerationRoot {
        &self.root
    }

    /// Admitted checkout behavior.
    #[must_use]
    pub const fn mode(&self) -> CheckoutMode {
        self.mode
    }

    /// Whether the private candidate differs from its immutable base.
    #[must_use]
    pub const fn has_pending_mutations(&self) -> bool {
        !self.pending_operations.is_empty() || self.prepared_merge_parent.is_some()
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    /// Prepares one bounded two-parent merge against this checkout's immutable base.
    ///
    /// Independent file identities merge without scanning equal Merkle subtrees.
    /// A conflict leaves the checkout unchanged; a successful result is an
    /// authenticated unpublished generation that must be committed or discarded.
    ///
    /// # Errors
    ///
    /// Rejects non-writable/prepared checkouts, foreign generations, truncated
    /// change frontiers, malformed storage, cancellation, or bounded-work failure.
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
                base_generation: self.base_generation_root,
                base: self.base_root.clone(),
                ours_generation: self
                    .pending_operations
                    .is_empty()
                    .then_some(self.generation_root),
                ours: self.root.clone(),
                theirs_generation: theirs_id,
                theirs: theirs_root,
                retain_theirs_parent: true,
                maximum_changes,
                maximum_conflicts,
            },
            decode_limits(self.volume.config),
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
                self.root = root;
                self.generation_root = generation_root;
                self.pending_operations.clear();
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
    /// sparse candidate generation without publishing authority.
    ///
    /// A dirty checkout first writes one unreachable checkpoint root; the
    /// complete closure is then authenticated and sorted. Object bodies remain
    /// independently transferable through [`Fs::export_object`].
    ///
    /// # Errors
    ///
    /// Returns typed checkpoint, closure, cancellation, storage, allocation,
    /// authentication, or bounded-work failures.
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
    /// candidate without publishing authority.
    ///
    /// A clean checkout returns its selected generation with zero object work.
    /// Repeated dirty checkpoints are content-addressed and therefore exactly
    /// idempotent. The checkout and its pending publication state are unchanged.
    ///
    /// # Errors
    ///
    /// Returns typed canonical encoding, storage, cancellation, allocation, or
    /// bounded-work failures.
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

    async fn checkpoint_root(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<ObjectId> {
        if self.prepared_merge_parent.is_some() || !self.has_pending_mutations() {
            return Ok(FsReceipt {
                value: self.generation_root,
                work: WorkCounters::default(),
            });
        }
        let checkpoint = build_checkpoint_async(
            &self.volume.fs.inner.objects,
            CheckpointRequest {
                base: self.base_generation_root,
                file_table: self.root.file_table,
                merge_parent: None,
            },
            decode_limits(self.volume.config),
            budget,
            cancellation,
        )
        .await
        .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
        Ok(FsReceipt {
            value: checkpoint.root,
            work: checkpoint.work,
        })
    }

    /// Discards a private candidate and reauthenticates its immutable base.
    ///
    /// # Errors
    ///
    /// Rejects non-overlay modes and returns exact cancellation, storage,
    /// decode, or budget failures without changing the checkout on failure.
    pub async fn discard(
        &mut self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        if self.mode.mutations == MutationMode::None {
            return Err(OperationFailure::before_work(FsError::MutationNotAllowed));
        }
        let (root, work) = read_generation_root(
            &self.volume.fs.inner.objects,
            self.base_generation_root,
            self.volume.config,
            budget,
            cancellation,
        )
        .await?;
        if root.volume_id != self.volume.id {
            return Err(OperationFailure::new(FsError::VolumeMismatch, work));
        }
        self.root = root;
        self.base_root = self.root.clone();
        self.base_file_table = self.root.file_table;
        self.generation_root = self.base_generation_root;
        self.pending_operations.clear();
        self.live_operation_id = None;
        self.prepared_merge_parent = None;
        self.dependencies.clear();
        Ok(FsReceipt { value: (), work })
    }

    /// Explicitly advances one clean manual checkout to the current head.
    ///
    /// Equal generations avoid an immutable-object read. Dirty overlays are
    /// retained and rejected because adopting them requires safe rebase.
    ///
    /// # Errors
    ///
    /// Rejects non-manual or dirty checkouts and returns exact authority,
    /// storage, authentication, cancellation, or budget failures.
    pub async fn refresh_head(
        &mut self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<GenerationId> {
        if self.mode.consistency != ConsistencyMode::Manual {
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
        self.base_generation_root = generation_root;
        self.generation_root = generation_root;
        self.root = root;
        self.base_root = self.root.clone();
        self.base_file_table = self.root.file_table;
        self.authority_head = Some(head);
        self.dependencies.clear();
        Ok(FsReceipt {
            value: self.generation_id(),
            work,
        })
    }

    /// Safely advances to the current head and sparsely replays private mutations.
    ///
    /// Only exact regions captured by reads and mutation preconditions are
    /// compared. A conflict leaves the checkout unchanged. A safe dirty rebase
    /// applies the retained ordered mutation log to the new immutable base.
    ///
    /// # Errors
    ///
    /// Returns measured authority, probe, decode, replay, cancellation, or
    /// budget failures without changing the checkout unless the full rebase succeeds.
    pub async fn rebase_head(
        &mut self,
        maximum_conflicts: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<RebaseDecision> {
        if self.live_operation_id.is_some() {
            return Err(OperationFailure::before_work(FsError::PendingLiveMutation));
        }
        self.rebase_head_for_live_retry(maximum_conflicts, budget, cancellation)
            .await
    }

    async fn rebase_head_for_live_retry(
        &mut self,
        maximum_conflicts: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<RebaseDecision> {
        if self.prepared_merge_parent.is_some() {
            return Err(OperationFailure::before_work(FsError::PreparedMergePending));
        }
        if !matches!(
            self.mode.consistency,
            ConsistencyMode::TrackingSafe | ConsistencyMode::Manual | ConsistencyMode::Live
        ) {
            return Err(OperationFailure::before_work(FsError::RefreshNotAllowed));
        }
        let (candidate_object, candidate_head, mut work) = self
            .volume
            .resolve_head_generation(budget, cancellation)
            .await?;
        let base = GenerationId::new(self.base_generation_root.digest);
        let candidate = GenerationId::new(candidate_object.digest);
        let probe = AuthenticatedGenerationProbe::new(
            &self.volume.fs.inner.objects,
            ProbeLimits {
                decode: decode_limits(self.volume.config),
                maximum_cached_generations: 2,
                maximum_cached_records: self.volume.config.limits.maximum_checkout_dependencies,
                maximum_extent_spans: self.volume.config.limits.maximum_directory_page_entries,
                maximum_content_payload_bytes: self.volume.config.limits.maximum_read_bytes,
                maximum_directory_entries: self.volume.config.limits.maximum_directory_page_entries,
            },
        )
        .map_err(|error| OperationFailure::new(FsError::Rebase(RebaseError::Probe(error)), work))?;
        let classification = classify_rebase_async(
            &probe,
            base,
            candidate,
            &self.dependencies,
            maximum_conflicts,
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::Rebase))?;
        work = add(work, classification.work)?;
        if matches!(classification.decision, RebaseDecision::Conflicted { .. }) {
            return Ok(FsReceipt {
                value: classification.decision,
                work,
            });
        }
        if candidate_object == self.base_generation_root {
            self.authority_head = Some(candidate_head);
            return Ok(FsReceipt {
                value: classification.decision,
                work,
            });
        }
        let (candidate_root, root_work) = read_generation_root(
            &self.volume.fs.inner.objects,
            candidate_object,
            self.volume.config,
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, root_work)?;
        if candidate_root.volume_id != self.volume.id {
            return Err(OperationFailure::new(FsError::VolumeMismatch, work));
        }
        let candidate_file_table = candidate_root.file_table;
        let candidate_base_root = candidate_root.clone();
        let rebased_root = if self.pending_operations.is_empty() {
            candidate_root
        } else {
            let replay = apply_generation_mutations_retaining_async(
                &self.volume.fs.inner.objects,
                &candidate_root,
                self.pending_operations.clone(),
                self.volume.config,
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, FsError::Mutation))?;
            work = add(work, replay.0.work)?;
            replay.0.root
        };
        self.base_generation_root = candidate_object;
        self.base_file_table = candidate_file_table;
        self.base_root = candidate_base_root;
        self.generation_root = candidate_object;
        self.root = rebased_root;
        self.authority_head = Some(candidate_head);
        if self.root.file_table == self.base_file_table {
            self.pending_operations.clear();
        }
        Ok(FsReceipt {
            value: classification.decision,
            work,
        })
    }

    /// Explicitly performs the same observation-safe head advancement used
    /// automatically before live reads.
    ///
    /// # Errors
    ///
    /// Rejects non-live checkouts and returns a typed region conflict rather
    /// than crossing any prior observation or pending mutation dependency.
    pub async fn refresh_live(
        &mut self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<GenerationId> {
        if self.mode.consistency != ConsistencyMode::Live {
            return Err(OperationFailure::before_work(FsError::RefreshNotAllowed));
        }
        let synchronized = self.synchronize_live(budget, cancellation).await?;
        Ok(FsReceipt {
            value: self.generation_id(),
            work: synchronized.work,
        })
    }

    async fn synchronize_live(
        &mut self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        if self.mode.consistency != ConsistencyMode::Live {
            return Ok(FsReceipt {
                value: (),
                work: WorkCounters::default(),
            });
        }
        let rebase = self
            .rebase_head(
                self.volume.config.limits.maximum_checkout_dependencies,
                budget,
                cancellation,
            )
            .await?;
        match rebase.value {
            RebaseDecision::Safe { .. } => Ok(FsReceipt {
                value: (),
                work: rebase.work,
            }),
            RebaseDecision::Conflicted {
                conflicts,
                truncated,
            } => Err(OperationFailure::new(
                FsError::LiveConflict {
                    conflicts,
                    truncated,
                },
                rebase.work,
            )),
        }
    }

    async fn capture_mutation_dependencies(
        &self,
        operations: &[Mutation],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Vec<Dependency>> {
        let mut work = WorkCounters::default();
        let mut dependencies = Vec::new();
        let mut exact_regions = Vec::new();
        exact_regions
            .try_reserve_exact(operations.len().saturating_mul(4))
            .map_err(|_| OperationFailure::before_work(FsError::PendingMutationAllocationFailed))?;
        for operation in operations {
            let Some([first_path, second_path]) = operation.paths() else {
                let records = self
                    .identity_base_records(operation, remaining(work, budget)?, cancellation)
                    .await
                    .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
                work = add(work, records.work)?;
                let (first_record, second_record) = records.value;
                for known_region in exact_mutation_regions(operation, first_record, second_record)
                    .into_iter()
                    .flatten()
                {
                    exact_regions.push(known_region);
                }
                continue;
            };
            let first = crate::kernel::observe_path_edges_async(
                &self.volume.fs.inner.objects,
                &self.base_root,
                first_path,
                self.volume.config,
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, FsError::Path))?;
            work = add(work, first.lookup.work)?;
            dependencies.extend(first.dependencies);
            let second_record = if second_path == first_path {
                first.lookup.record
            } else {
                let second = crate::kernel::observe_path_edges_async(
                    &self.volume.fs.inner.objects,
                    &self.base_root,
                    second_path,
                    self.volume.config,
                    remaining(work, budget)?,
                    cancellation,
                )
                .await
                .map_err(|failure| failure.map_with_prior_work(work, FsError::Path))?;
                work = add(work, second.lookup.work)?;
                dependencies.extend(second.dependencies);
                second.lookup.record
            };
            for known_region in
                exact_mutation_regions(operation, first.lookup.record, second_record)
                    .into_iter()
                    .flatten()
            {
                exact_regions.push(known_region);
            }
        }
        let captured = capture_known_dependencies_async(
            &self.volume.fs.inner.objects,
            self.volume.config,
            exact_regions,
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, captured.work)?;
        dependencies.extend(captured.value);
        Ok(FsReceipt {
            value: dependencies,
            work,
        })
    }

    async fn identity_base_records(
        &self,
        operation: &Mutation,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<(Option<FileRecord>, Option<FileRecord>)> {
        let (first_id, second_id) = match operation {
            Mutation::File { file_id, .. } => (Some(*file_id), None),
            Mutation::CloneFileRange {
                source_file_id,
                destination_file_id,
                ..
            } => (Some(*source_file_id), Some(*destination_file_id)),
            _ => (None, None),
        };
        let mut work = WorkCounters::default();
        let first_record = if let Some(file_id) = first_id {
            let lookup = lookup_file_record_async(
                &self.volume.fs.inner.objects,
                self.base_root.file_table,
                file_id,
                decode_limits(self.volume.config),
                budget,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, FsError::FileRecord))?;
            work = add(work, lookup.work)?;
            lookup.record
        } else {
            None
        };
        let second_record = if second_id == first_id {
            first_record
        } else if let Some(file_id) = second_id {
            let lookup = lookup_file_record_async(
                &self.volume.fs.inner.objects,
                self.base_root.file_table,
                file_id,
                decode_limits(self.volume.config),
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, FsError::FileRecord))?;
            work = add(work, lookup.work)?;
            lookup.record
        } else {
            None
        };
        Ok(FsReceipt {
            value: (first_record, second_record),
            work,
        })
    }

    /// Atomically applies one ordered mutation batch to this private checkout.
    ///
    /// The immutable base remains unchanged. A failed batch may leave harmless
    /// unreferenced objects but never replaces the checkout candidate.
    ///
    /// # Errors
    ///
    /// Rejects non-writable modes and returns exact planning, storage,
    /// cancellation, semantic, or work-budget failures.
    pub async fn mutate(
        &mut self,
        operations: Vec<Mutation>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
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
    }

    /// Applies and durably publishes one direct-live mutation operation.
    ///
    /// Authority races are automatically rebased only when every exact read
    /// and mutation dependency remains unchanged. The stable operation ID is
    /// reused across those safe retries. A region conflict or exhausted retry
    /// bound leaves the sparse candidate available to [`Self::resume_live`] or
    /// [`Self::discard`].
    ///
    /// # Errors
    ///
    /// Rejects non-live modes, an existing unresolved candidate, zero bounds,
    /// and returns exact mutation, rebase, publication, cancellation, storage,
    /// or work-budget failures.
    pub async fn mutate_live(
        &mut self,
        operations: Vec<Mutation>,
        operation_id: OperationId,
        maximum_attempts: u32,
        maximum_conflicts: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<LiveMutationOutcome> {
        self.validate_live_operation(maximum_attempts)?;
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
    ///
    /// This is the canonical byte-oriented direct-live orchestration boundary
    /// for language SDKs. It preserves generated file identities and one exact
    /// work receipt across content staging, sparse mutation, safe rebase, and
    /// authority publication.
    ///
    /// # Errors
    ///
    /// Returns the combined failures of [`Self::apply_authored_transaction`]
    /// and [`Self::resume_live`]. An unresolved publication retains the sparse
    /// candidate for an exact [`Self::resume_live`] retry.
    pub async fn apply_authored_live(
        &mut self,
        authored: Vec<AuthoredMutation>,
        operation_id: OperationId,
        maximum_attempts: u32,
        maximum_conflicts: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<AuthoredLiveMutationResult> {
        self.validate_live_operation(maximum_attempts)?;
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
    ///
    /// The caller must reuse the original operation ID after an indeterminate
    /// transport result. Exact authority idempotency resolves whether that
    /// operation was already durable.
    ///
    /// # Errors
    ///
    /// Returns the same failures as [`Self::mutate_live`] and rejects a clean
    /// checkout.
    pub async fn resume_live(
        &mut self,
        operation_id: OperationId,
        maximum_attempts: u32,
        maximum_conflicts: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<LiveMutationOutcome> {
        self.validate_live_operation(maximum_attempts)?;
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

    fn validate_live_operation(
        &self,
        maximum_attempts: u32,
    ) -> Result<(), OperationFailure<FsError>> {
        if self.mode.access != AccessMode::ReadWrite
            || self.mode.consistency != ConsistencyMode::Live
            || self.mode.mutations != MutationMode::DirectLive
        {
            return Err(OperationFailure::before_work(FsError::MutationNotAllowed));
        }
        if maximum_attempts == 0 {
            return Err(OperationFailure::before_work(FsError::ZeroLiveRetryLimit));
        }
        Ok(())
    }

    async fn publish_live(
        &mut self,
        operation_id: OperationId,
        maximum_attempts: u32,
        maximum_conflicts: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<LiveMutationOutcome> {
        let mut work = WorkCounters::default();
        let mut state = LiveRetryState::new(maximum_attempts)
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        loop {
            let publication = self
                .publish_pending(operation_id, remaining(work, budget)?, cancellation)
                .await
                .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
            work = add(work, publication.work)?;
            let action = state
                .observe_publication(live_publication_observation(publication.value))
                .map_err(|error| OperationFailure::new(error.into(), work))?;
            match action {
                LiveRetryAction::Complete(outcome) => {
                    return Ok(FsReceipt {
                        value: outcome,
                        work,
                    });
                }
                LiveRetryAction::Publish => {
                    return Err(OperationFailure::new(
                        FsError::LiveRetry(LiveRetryError::InvalidTransition),
                        work,
                    ));
                }
                LiveRetryAction::Rebase => {
                    let rebase = self
                        .rebase_head_for_live_retry(
                            maximum_conflicts,
                            remaining(work, budget)?,
                            cancellation,
                        )
                        .await
                        .map_err(|failure| {
                            failure.map_with_prior_work(work, std::convert::identity)
                        })?;
                    work = add(work, rebase.work)?;
                    match state
                        .observe_rebase(rebase.value)
                        .map_err(|error| OperationFailure::new(error.into(), work))?
                    {
                        LiveRetryAction::Publish => {}
                        LiveRetryAction::Complete(outcome) => {
                            return Ok(FsReceipt {
                                value: outcome,
                                work,
                            });
                        }
                        LiveRetryAction::Rebase => {
                            return Err(OperationFailure::new(
                                FsError::LiveRetry(LiveRetryError::InvalidTransition),
                                work,
                            ));
                        }
                    }
                }
            }
        }
    }

    async fn mutate_candidate(
        &mut self,
        mut operations: Vec<Mutation>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        if self.live_operation_id.is_some() {
            return Err(OperationFailure::before_work(FsError::PendingLiveMutation));
        }
        if self.prepared_merge_parent.is_some() {
            return Err(OperationFailure::before_work(FsError::PreparedMergePending));
        }
        let maximum = usize::try_from(self.volume.config.limits.maximum_mutations_per_batch)
            .unwrap_or(usize::MAX);
        if operations.capacity() > maximum {
            return Err(OperationFailure::before_work(
                FsError::ExcessiveMutationCapacity,
            ));
        }
        let pending_count = self
            .pending_operations
            .len()
            .checked_add(operations.len())
            .ok_or_else(|| OperationFailure::before_work(FsError::TooManyPendingMutations))?;
        if pending_count > maximum {
            return Err(OperationFailure::before_work(
                FsError::TooManyPendingMutations,
            ));
        }
        // Keep the public mutation futures small: folded-name admission is a
        // bounded, independently stateful planning phase and must not inflate
        // every SDK mutation future's stack frame.
        let admitted =
            Box::pin(self.admit_mutation_names(&mut operations, budget, cancellation)).await?;
        let captured = self
            .capture_mutation_dependencies(&operations, budget, cancellation)
            .await?;
        let dependency_work = add(admitted.work, captured.work)?;
        let mutation_dependencies = captured.value;
        let mut next_dependencies = self.dependencies.clone();
        next_dependencies
            .extend_mutations(
                mutation_dependencies,
                self.volume.config.limits.maximum_checkout_dependencies,
            )
            .map_err(|error| OperationFailure::new(error.into(), dependency_work))?;
        let prior_file_table = self.root.file_table;
        let (receipt, retained_operations) = apply_generation_mutations_retaining_async(
            &self.volume.fs.inner.objects,
            &self.root,
            operations,
            self.volume.config,
            remaining(dependency_work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(dependency_work, FsError::Mutation))?;
        let mut work = add(dependency_work, receipt.work)?;
        if receipt.root.file_table != prior_file_table {
            if receipt.root.file_table == self.base_file_table {
                next_dependencies.clear_mutations();
                self.pending_operations.clear();
            } else {
                work = retain_pending_operations(
                    &mut self.pending_operations,
                    retained_operations,
                    maximum,
                    work,
                    budget,
                )?;
            }
            self.dependencies = next_dependencies;
            self.root = receipt.root;
        }
        Ok(FsReceipt { value: (), work })
    }

    /// Admits every namespace-binding destination in one ordered mutation
    /// batch before the generation kernel runs. The kernel works on exact
    /// byte-ordered pages and therefore cannot discover folded siblings from
    /// its sparse endpoint lookups alone; this boundary owns that policy while
    /// retaining a zero-work fast path for the default sensitive profile.
    #[allow(clippy::too_many_lines)]
    async fn admit_mutation_names(
        &mut self,
        operations: &mut [Mutation],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        if self.volume.config.case_sensitivity != CaseSensitivity::ProfileFolded
            && self.volume.config.unicode != UnicodePolicy::RequireNfc
        {
            return Ok(FsReceipt {
                value: (),
                work: WorkCounters::default(),
            });
        }

        let mut work = WorkCounters::default();
        let mut pending: Vec<(NamespacePath, FileId)> = Vec::new();
        let mut removed: Vec<(NamespacePath, FileId)> = Vec::new();
        pending
            .try_reserve_exact(operations.len())
            .map_err(|_| OperationFailure::before_work(FsError::PendingMutationAllocationFailed))?;
        removed
            .try_reserve_exact(operations.len())
            .map_err(|_| OperationFailure::before_work(FsError::PendingMutationAllocationFailed))?;
        for operation in operations.iter_mut() {
            cancellation
                .check()
                .map_err(|error| OperationFailure::new(error.into(), work))?;
            let canonicalized = Box::pin(self.canonicalize_existing_mutation(
                operation,
                &pending,
                remaining(work, budget)?,
                cancellation,
            ))
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
            work = add(work, canonicalized.work)?;
            let parent_canonicalized = Box::pin(self.canonicalize_new_mutation_parents(
                operation,
                &pending,
                remaining(work, budget)?,
                cancellation,
            ))
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
            work = add(work, parent_canonicalized.work)?;
            match operation {
                Mutation::Create { path, record } => {
                    let allowed = removed_binding_id(path, &removed);
                    let allowed_ids: &[FileId] = match allowed {
                        Some(ref file_id) => std::slice::from_ref(file_id),
                        None => &[],
                    };
                    let parent_pending = pending_parent_exists(
                        path,
                        &pending,
                        self.volume.config.case_sensitivity,
                        self.volume.config.limits,
                    );
                    let admission = Box::pin(self.admit_new_name(
                        path,
                        remaining(work, budget)?,
                        cancellation,
                        allowed_ids,
                        parent_pending,
                    ))
                    .await
                    .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
                    work = add(work, admission.work)?;
                    if self.volume.config.case_sensitivity == CaseSensitivity::ProfileFolded
                        && pending_name_collision(path, &pending, allowed_ids)
                    {
                        return Err(OperationFailure::new(FsError::NameCollision, work));
                    }
                    pending.push((path.clone(), record.file_id));
                }
                Mutation::Link {
                    source,
                    destination,
                } => {
                    let source_id = self
                        .pending_or_existing_file_id(
                            source,
                            &pending,
                            remaining(work, budget)?,
                            cancellation,
                        )
                        .await
                        .map_err(|failure| {
                            failure.map_with_prior_work(work, std::convert::identity)
                        })?;
                    work = add(work, source_id.work)?;
                    let allowed = removed_binding_id(destination, &removed);
                    let allowed_ids: &[FileId] = match allowed {
                        Some(ref file_id) => std::slice::from_ref(file_id),
                        None => &[],
                    };
                    let parent_pending = pending_parent_exists(
                        destination,
                        &pending,
                        self.volume.config.case_sensitivity,
                        self.volume.config.limits,
                    );
                    let admission = Box::pin(self.admit_new_name(
                        destination,
                        remaining(work, budget)?,
                        cancellation,
                        allowed_ids,
                        parent_pending,
                    ))
                    .await
                    .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
                    work = add(work, admission.work)?;
                    if self.volume.config.case_sensitivity == CaseSensitivity::ProfileFolded
                        && pending_name_collision(destination, &pending, allowed_ids)
                    {
                        return Err(OperationFailure::new(FsError::NameCollision, work));
                    }
                    pending.push((destination.clone(), source_id.value));
                }
                Mutation::Rename {
                    source,
                    destination,
                    replace,
                } => {
                    let source_id = self
                        .pending_or_existing_file_id(
                            source,
                            &pending,
                            remaining(work, budget)?,
                            cancellation,
                        )
                        .await
                        .map_err(|failure| {
                            failure.map_with_prior_work(work, std::convert::identity)
                        })?;
                    work = add(work, source_id.work)?;
                    let mut allowed_ids = Vec::with_capacity(2);
                    allowed_ids.push(source_id.value);
                    if let Some(file_id) = removed_binding_id(destination, &removed)
                        && file_id != source_id.value
                    {
                        allowed_ids.push(file_id);
                    }
                    let parent_pending = pending_parent_exists(
                        destination,
                        &pending,
                        self.volume.config.case_sensitivity,
                        self.volume.config.limits,
                    );
                    let admission = Box::pin(self.admit_new_name(
                        destination,
                        remaining(work, budget)?,
                        cancellation,
                        &allowed_ids,
                        parent_pending,
                    ))
                    .await
                    .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
                    work = add(work, admission.work)?;
                    if self.volume.config.case_sensitivity == CaseSensitivity::ProfileFolded
                        && pending_name_collision(destination, &pending, &allowed_ids)
                    {
                        return Err(OperationFailure::new(FsError::NameCollision, work));
                    }
                    pending.retain(|(path, _)| path != source);
                    if *replace {
                        pending.retain(|(path, _)| path != destination);
                    }
                    removed.push((source.clone(), source_id.value));
                    pending.push((destination.clone(), source_id.value));
                }
                Mutation::Remove { path, .. } => {
                    if pending_binding_id(path, &pending, self.volume.config.case_sensitivity)
                        .is_none()
                    {
                        let removed_id = self
                            .pending_or_existing_file_id(
                                path,
                                &pending,
                                remaining(work, budget)?,
                                cancellation,
                            )
                            .await
                            .map_err(|failure| {
                                failure.map_with_prior_work(work, std::convert::identity)
                            })?;
                        work = add(work, removed_id.work)?;
                        removed.push((path.clone(), removed_id.value));
                    }
                    pending.retain(|(pending_path, _)| pending_path != path);
                }
                Mutation::SetMetadata { .. }
                | Mutation::Write { .. }
                | Mutation::ValidateRegular { .. }
                | Mutation::Resize { .. }
                | Mutation::ZeroRange { .. }
                | Mutation::Preallocate { .. }
                | Mutation::CloneRange { .. }
                | Mutation::File { .. }
                | Mutation::CloneFileRange { .. } => {}
            }
        }
        Ok(FsReceipt { value: (), work })
    }

    async fn canonicalize_new_mutation_parents(
        &mut self,
        operation: &mut Mutation,
        pending: &[(NamespacePath, FileId)],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        if self.volume.config.case_sensitivity != CaseSensitivity::ProfileFolded {
            return Ok(FsReceipt {
                value: (),
                work: WorkCounters::default(),
            });
        }
        let (Mutation::Create { path, .. }
        | Mutation::Link {
            destination: path, ..
        }
        | Mutation::Rename {
            destination: path, ..
        }) = operation
        else {
            return Ok(FsReceipt {
                value: (),
                work: WorkCounters::default(),
            });
        };
        let Some((parent, name)) = path.split_last() else {
            return Ok(FsReceipt {
                value: (),
                work: WorkCounters::default(),
            });
        };
        if parent.is_empty() {
            return Ok(FsReceipt {
                value: (),
                work: WorkCounters::default(),
            });
        }
        let resolved = self
            .canonicalize_path_components(parent, pending, budget, cancellation)
            .await?;
        let work = resolved.work;
        let Some(resolved_parent) = resolved.value else {
            return Ok(FsReceipt { value: (), work });
        };
        let mut components = resolved_parent.components().to_vec();
        components.push(name.clone());
        *path = NamespacePath::new(components, self.volume.config.limits).map_err(|error| {
            OperationFailure::new(FsError::Path(PathLookupError::NamespacePath(error)), work)
        })?;
        Ok(FsReceipt { value: (), work })
    }

    async fn canonicalize_existing_mutation(
        &mut self,
        operation: &mut Mutation,
        pending: &[(NamespacePath, FileId)],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        if self.volume.config.case_sensitivity != CaseSensitivity::ProfileFolded {
            return Ok(FsReceipt {
                value: (),
                work: WorkCounters::default(),
            });
        }
        let mut work = WorkCounters::default();
        let paths = match operation {
            Mutation::Remove { path, .. }
            | Mutation::SetMetadata { path, .. }
            | Mutation::Write { path, .. }
            | Mutation::ValidateRegular { path }
            | Mutation::Resize { path, .. }
            | Mutation::ZeroRange { path, .. }
            | Mutation::Preallocate { path, .. } => {
                let resolved = self
                    .canonical_existing_path(path, pending, remaining(work, budget)?, cancellation)
                    .await?;
                *path = resolved.value;
                work = add(work, resolved.work)?;
                return Ok(FsReceipt { value: (), work });
            }
            Mutation::Rename { source, .. } | Mutation::Link { source, .. } => {
                let resolved = self
                    .canonical_existing_path(
                        source,
                        pending,
                        remaining(work, budget)?,
                        cancellation,
                    )
                    .await?;
                *source = resolved.value;
                work = add(work, resolved.work)?;
                return Ok(FsReceipt { value: (), work });
            }
            Mutation::CloneRange {
                source,
                destination,
                ..
            } => Some((source, destination)),
            Mutation::Create { .. } | Mutation::File { .. } | Mutation::CloneFileRange { .. } => {
                None
            }
        };
        if let Some((source, destination)) = paths {
            let resolved_source = self
                .canonical_existing_path(source, pending, remaining(work, budget)?, cancellation)
                .await?;
            *source = resolved_source.value;
            work = add(work, resolved_source.work)?;
            let resolved_destination = self
                .canonical_existing_path(
                    destination,
                    pending,
                    remaining(work, budget)?,
                    cancellation,
                )
                .await?;
            *destination = resolved_destination.value;
            work = add(work, resolved_destination.work)?;
        }
        Ok(FsReceipt { value: (), work })
    }

    async fn canonical_existing_path(
        &mut self,
        path: &NamespacePath,
        pending: &[(NamespacePath, FileId)],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<NamespacePath> {
        if let Some((exact, _)) =
            pending_binding_path(path, pending, self.volume.config.case_sensitivity)
        {
            return Ok(FsReceipt {
                value: exact,
                work: WorkCounters::default(),
            });
        }
        let mut work = WorkCounters::default();
        let exact = self
            .lookup_no_follow_at_current(path, budget, cancellation, false)
            .await?;
        work = add(work, exact.work)?;
        if exact.value.record.is_some()
            || self.volume.config.case_sensitivity != CaseSensitivity::ProfileFolded
        {
            if exact.value.record.is_some() {
                return Ok(FsReceipt {
                    value: path.clone(),
                    work,
                });
            }
            return Err(OperationFailure::new(FsError::NotFound, work));
        }

        if path.is_root() {
            return Err(OperationFailure::new(FsError::NotFound, work));
        }
        let resolved = self
            .canonicalize_path_components(
                path.components(),
                pending,
                remaining(work, budget)?,
                cancellation,
            )
            .await?;
        work = add(work, resolved.work)?;
        let canonical = resolved
            .value
            .ok_or_else(|| OperationFailure::new(FsError::NotFound, work))?;
        Ok(FsReceipt {
            value: canonical,
            work,
        })
    }

    async fn canonicalize_path_components(
        &mut self,
        requested_components: &[LogicalName],
        pending: &[(NamespacePath, FileId)],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Option<NamespacePath>> {
        let mut work = WorkCounters::default();
        let mut canonical_components = Vec::new();
        for requested_component in requested_components {
            let candidate = NamespacePath::new(
                canonical_components
                    .iter()
                    .cloned()
                    .chain(std::iter::once(requested_component.clone()))
                    .collect(),
                self.volume.config.limits,
            )
            .map_err(|error| {
                OperationFailure::new(FsError::Path(PathLookupError::NamespacePath(error)), work)
            })?;
            if let Some((pending_path, _)) =
                pending_binding_path(&candidate, pending, self.volume.config.case_sensitivity)
            {
                canonical_components.push(
                    pending_path
                        .components()
                        .last()
                        .cloned()
                        .ok_or_else(|| OperationFailure::new(FsError::NotFound, work))?,
                );
                continue;
            }
            let probe = self
                .lookup_no_follow_at_current(
                    &candidate,
                    remaining(work, budget)?,
                    cancellation,
                    false,
                )
                .await?;
            work = add(work, probe.work)?;
            if probe.value.record.is_some() {
                canonical_components.push(requested_component.clone());
                continue;
            }
            let parent =
                NamespacePath::new(canonical_components.clone(), self.volume.config.limits)
                    .map_err(|error| {
                        OperationFailure::new(
                            FsError::Path(PathLookupError::NamespacePath(error)),
                            work,
                        )
                    })?;
            let siblings = self
                .find_case_folded_sibling(
                    parent.components(),
                    requested_component,
                    remaining(work, budget)?,
                    cancellation,
                    &[],
                )
                .await?;
            work = add(work, siblings.work)?;
            let Some(exact_name) = siblings.value else {
                return Ok(FsReceipt { value: None, work });
            };
            canonical_components.push(exact_name);
        }
        let canonical = NamespacePath::new(canonical_components, self.volume.config.limits)
            .map_err(|error| {
                OperationFailure::new(FsError::Path(PathLookupError::NamespacePath(error)), work)
            })?;
        Ok(FsReceipt {
            value: Some(canonical),
            work,
        })
    }

    async fn pending_or_existing_file_id(
        &mut self,
        path: &NamespacePath,
        pending: &[(NamespacePath, FileId)],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<FileId> {
        if let Some(file_id) =
            pending_binding_id(path, pending, self.volume.config.case_sensitivity)
        {
            return Ok(FsReceipt {
                value: file_id,
                work: WorkCounters::default(),
            });
        }
        let lookup = self.lookup_no_follow(path, budget, cancellation).await?;
        let work = lookup.work;
        let record = lookup
            .value
            .record
            .ok_or_else(|| OperationFailure::new(FsError::NotFound, work))?;
        Ok(FsReceipt {
            value: record.file_id,
            work,
        })
    }

    /// Compiles and atomically applies one complete caller-authored transaction.
    ///
    /// Immutable byte and metadata objects are admitted before the candidate
    /// root is replaced. Failure may leave unreachable immutable objects, but
    /// never a partially changed checkout candidate.
    ///
    /// # Errors
    ///
    /// Rejects excessive input, unsupported kinds, invalid ranges,
    /// non-writable checkouts, semantic conflicts, cancellation, storage
    /// failures, or work beyond the caller's exact budget.
    pub async fn apply_authored_transaction(
        &mut self,
        authored: Vec<AuthoredMutation>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<AuthoredTransactionResult> {
        let maximum = usize::try_from(self.volume.config.limits.maximum_mutations_per_batch)
            .unwrap_or(usize::MAX);
        let operation_count = authored
            .iter()
            .try_fold(0_usize, |count, mutation| {
                count.checked_add(authored_mutation_operation_count(mutation))
            })
            .ok_or_else(|| OperationFailure::before_work(FsError::TooManyPendingMutations))?;
        if authored.len() > maximum || authored.capacity() > maximum || operation_count > maximum {
            return Err(OperationFailure::before_work(
                FsError::TooManyPendingMutations,
            ));
        }
        let mut work = WorkCounters::default();
        let mut operations = Vec::new();
        let mut created_file_ids = Vec::new();
        operations
            .try_reserve_exact(operation_count)
            .map_err(|_| OperationFailure::before_work(FsError::PendingMutationAllocationFailed))?;
        created_file_ids
            .try_reserve_exact(authored.len())
            .map_err(|_| OperationFailure::before_work(FsError::PendingMutationAllocationFailed))?;

        for authored_mutation in authored {
            cancellation
                .check()
                .map_err(|error| OperationFailure::new(error.into(), work))?;
            let created = self
                .compile_authored_mutation(
                    authored_mutation,
                    &mut operations,
                    &mut work,
                    budget,
                    cancellation,
                )
                .await?;
            created_file_ids.push(created);
        }
        if operations.is_empty() {
            return Ok(FsReceipt {
                value: AuthoredTransactionResult { created_file_ids },
                work,
            });
        }
        let mutation = self
            .mutate(operations, remaining(work, budget)?, cancellation)
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, mutation.work)?;
        Ok(FsReceipt {
            value: AuthoredTransactionResult { created_file_ids },
            work,
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn compile_authored_mutation(
        &self,
        authored: AuthoredMutation,
        operations: &mut Vec<Mutation>,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<Option<FileId>, OperationFailure<FsError>> {
        match authored {
            AuthoredMutation::CreateFile {
                path,
                bytes,
                metadata,
            } => {
                if u64::try_from(bytes.len()).unwrap_or(u64::MAX)
                    > self.volume.config.limits.maximum_read_bytes
                {
                    return Err(OperationFailure::new(
                        FsError::FileRead(FileRangeReadError::InvalidRange),
                        *work,
                    ));
                }
                let metadata = self
                    .stage_metadata(metadata, remaining(*work, budget)?, cancellation)
                    .await
                    .map_err(|failure| {
                        failure.map_with_prior_work(*work, std::convert::identity)
                    })?;
                *work = add(*work, metadata.work)?;
                let file_id = FileId::new();
                if bytes.len() <= crate::kernel::MAXIMUM_INLINE_FILE_BYTES {
                    operations.push(Mutation::Create {
                        path,
                        record: FileRecord {
                            file_id,
                            kind: FileKind::Regular,
                            link_count: 1,
                            metadata: metadata.value,
                            payload: FilePayload::InlineRegular(
                                InlineFileData::new(&bytes)
                                    .map_err(|error| OperationFailure::new(error.into(), *work))?,
                            ),
                        },
                    });
                } else {
                    let blob = self
                        .stage_blob(bytes, remaining(*work, budget)?, cancellation)
                        .await
                        .map_err(|failure| {
                            failure.map_with_prior_work(*work, std::convert::identity)
                        })?;
                    *work = add(*work, blob.work)?;
                    operations.push(Mutation::Create {
                        path: path.clone(),
                        record: FileRecord {
                            file_id,
                            kind: FileKind::Regular,
                            link_count: 1,
                            metadata: metadata.value,
                            payload: FilePayload::InlineRegular(
                                InlineFileData::new(&[])
                                    .map_err(|error| OperationFailure::new(error.into(), *work))?,
                            ),
                        },
                    });
                    operations.push(Mutation::Write {
                        path,
                        offset: 0,
                        length: blob.value.logical_bytes,
                        content: blob.value.root,
                        content_offset: 0,
                    });
                }
                Ok(Some(file_id))
            }
            AuthoredMutation::CreateFileFromContent {
                path,
                content,
                metadata,
            } => {
                let metadata = self
                    .stage_metadata(metadata, remaining(*work, budget)?, cancellation)
                    .await
                    .map_err(|failure| {
                        failure.map_with_prior_work(*work, std::convert::identity)
                    })?;
                *work = add(*work, metadata.work)?;
                let file_id = FileId::new();
                operations.push(Mutation::Create {
                    path: path.clone(),
                    record: FileRecord {
                        file_id,
                        kind: FileKind::Regular,
                        link_count: 1,
                        metadata: metadata.value,
                        payload: FilePayload::InlineRegular(
                            InlineFileData::new(&[])
                                .map_err(|error| OperationFailure::new(error.into(), *work))?,
                        ),
                    },
                });
                if content.logical_bytes != 0 {
                    operations.push(Mutation::Write {
                        path,
                        offset: 0,
                        length: content.logical_bytes,
                        content: content.root,
                        content_offset: 0,
                    });
                }
                Ok(Some(file_id))
            }
            AuthoredMutation::CreateDirectory { path, metadata } => {
                let tree = self
                    .stage_empty_tree(remaining(*work, budget)?, cancellation)
                    .await
                    .map_err(|failure| {
                        failure.map_with_prior_work(*work, std::convert::identity)
                    })?;
                *work = add(*work, tree.work)?;
                self.compile_record(
                    operations,
                    path,
                    FileKind::Directory,
                    FilePayload::Directory {
                        entries: tree.value,
                    },
                    metadata,
                    work,
                    budget,
                    cancellation,
                )
                .await
                .map(Some)
            }
            AuthoredMutation::CreateSymbolicLink {
                path,
                target,
                metadata,
            } => {
                let blob = self
                    .stage_blob(target, remaining(*work, budget)?, cancellation)
                    .await
                    .map_err(|failure| {
                        failure.map_with_prior_work(*work, std::convert::identity)
                    })?;
                *work = add(*work, blob.work)?;
                self.compile_record(
                    operations,
                    path,
                    FileKind::SymbolicLink,
                    FilePayload::SymbolicLink {
                        target_bytes: blob.value.logical_bytes,
                        target: blob.value.root,
                    },
                    metadata,
                    work,
                    budget,
                    cancellation,
                )
                .await
                .map(Some)
            }
            AuthoredMutation::CreateEmptySpecial {
                path,
                kind,
                metadata,
            } => {
                if !matches!(
                    kind,
                    FileKind::Fifo | FileKind::Socket | FileKind::MountBoundary
                ) {
                    return Err(OperationFailure::new(FsError::InvalidSpecialKind, *work));
                }
                self.validate_profile_kind(kind, *work)?;
                self.compile_record(
                    operations,
                    path,
                    kind,
                    FilePayload::Empty,
                    metadata,
                    work,
                    budget,
                    cancellation,
                )
                .await
                .map(Some)
            }
            AuthoredMutation::CreateDevice {
                path,
                kind,
                major,
                minor,
                metadata,
            } => {
                if !matches!(kind, FileKind::CharacterDevice | FileKind::BlockDevice) {
                    return Err(OperationFailure::new(FsError::InvalidSpecialKind, *work));
                }
                self.validate_profile_kind(kind, *work)?;
                self.compile_record(
                    operations,
                    path,
                    kind,
                    FilePayload::Device { major, minor },
                    metadata,
                    work,
                    budget,
                    cancellation,
                )
                .await
                .map(Some)
            }
            AuthoredMutation::CreateReparsePoint {
                path,
                payload,
                metadata,
            } => {
                self.validate_profile_kind(FileKind::ReparsePoint, *work)?;
                let blob = self
                    .stage_blob(payload, remaining(*work, budget)?, cancellation)
                    .await
                    .map_err(|failure| {
                        failure.map_with_prior_work(*work, std::convert::identity)
                    })?;
                *work = add(*work, blob.work)?;
                self.compile_record(
                    operations,
                    path,
                    FileKind::ReparsePoint,
                    FilePayload::ReparsePoint {
                        payload_bytes: blob.value.logical_bytes,
                        payload: blob.value.root,
                    },
                    metadata,
                    work,
                    budget,
                    cancellation,
                )
                .await
                .map(Some)
            }
            AuthoredMutation::Remove {
                path,
                expected_file_id,
            } => {
                operations.push(Mutation::Remove {
                    path,
                    expected_file_id: expected_file_id
                        .map_or(MetadataField::Unavailable, MetadataField::Value),
                });
                Ok(None)
            }
            AuthoredMutation::Rename {
                source,
                destination,
                replace,
            } => {
                operations.push(Mutation::Rename {
                    source,
                    destination,
                    replace,
                });
                Ok(None)
            }
            AuthoredMutation::HardLink {
                source,
                destination,
            } => {
                operations.push(Mutation::Link {
                    source,
                    destination,
                });
                Ok(None)
            }
            AuthoredMutation::Write {
                path,
                offset,
                bytes,
            } => {
                if bytes.is_empty() {
                    operations.push(Mutation::ValidateRegular { path });
                } else {
                    let blob = self
                        .stage_blob(bytes, remaining(*work, budget)?, cancellation)
                        .await
                        .map_err(|failure| {
                            failure.map_with_prior_work(*work, std::convert::identity)
                        })?;
                    *work = add(*work, blob.work)?;
                    operations.push(Mutation::Write {
                        path,
                        offset,
                        length: blob.value.logical_bytes,
                        content: blob.value.root,
                        content_offset: 0,
                    });
                }
                Ok(None)
            }
            AuthoredMutation::WriteFromContent {
                path,
                offset,
                content,
            } => {
                if content.logical_bytes == 0 {
                    operations.push(Mutation::ValidateRegular { path });
                } else {
                    operations.push(Mutation::Write {
                        path,
                        offset,
                        length: content.logical_bytes,
                        content: content.root,
                        content_offset: 0,
                    });
                }
                Ok(None)
            }
            AuthoredMutation::SetMetadata { path, metadata } => {
                let metadata = self
                    .stage_metadata(metadata, remaining(*work, budget)?, cancellation)
                    .await
                    .map_err(|failure| {
                        failure.map_with_prior_work(*work, std::convert::identity)
                    })?;
                *work = add(*work, metadata.work)?;
                operations.push(Mutation::SetMetadata {
                    path,
                    metadata: metadata.value,
                });
                Ok(None)
            }
            AuthoredMutation::Resize {
                path,
                logical_bytes,
            } => {
                operations.push(Mutation::Resize {
                    path,
                    logical_bytes,
                });
                Ok(None)
            }
            AuthoredMutation::ZeroRange {
                path,
                range,
                allocated,
                extend,
            } => {
                operations.push(Mutation::ZeroRange {
                    path,
                    offset: range.offset,
                    length: range.length,
                    allocated,
                    extend,
                });
                Ok(None)
            }
            AuthoredMutation::Preallocate {
                path,
                range,
                keep_size,
            } => {
                operations.push(Mutation::Preallocate {
                    path,
                    offset: range.offset,
                    length: range.length,
                    keep_size,
                });
                Ok(None)
            }
            AuthoredMutation::CloneRange(request) => {
                operations.push(Mutation::CloneRange {
                    source: request.source,
                    source_offset: request.source_offset,
                    destination: request.destination,
                    destination_offset: request.destination_offset,
                    length: request.length,
                });
                Ok(None)
            }
        }
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

    #[allow(clippy::too_many_arguments)]
    async fn compile_record(
        &self,
        operations: &mut Vec<Mutation>,
        path: NamespacePath,
        kind: FileKind,
        payload: FilePayload,
        metadata: FileMetadata,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<FileId, OperationFailure<FsError>> {
        let staged = self
            .stage_metadata(metadata, remaining(*work, budget)?, cancellation)
            .await
            .map_err(|failure| failure.map_with_prior_work(*work, std::convert::identity))?;
        *work = add(*work, staged.work)?;
        let file_id = FileId::new();
        operations.push(Mutation::Create {
            path,
            record: FileRecord {
                file_id,
                kind,
                link_count: 1,
                metadata: staged.value,
                payload,
            },
        });
        Ok(file_id)
    }

    async fn stage_metadata(
        &self,
        metadata: FileMetadata,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<ObjectId> {
        let encoded = encode_file_metadata(metadata)
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        let (value, work) = self
            .volume
            .fs
            .put_encoded(
                ObjectKind::Metadata,
                encoded,
                WorkCounters::default(),
                budget,
                cancellation,
            )
            .await?;
        Ok(FsReceipt { value, work })
    }

    async fn stage_empty_tree(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<ObjectId> {
        let encoded = encode_tree_page(
            &TreePage::Leaf(Vec::new()),
            self.volume.config.limits.maximum_directory_page_entries,
        )
        .map_err(|error| OperationFailure::before_work(error.into()))?;
        let (value, work) = self
            .volume
            .fs
            .put_encoded(
                ObjectKind::TreePage,
                encoded,
                WorkCounters::default(),
                budget,
                cancellation,
            )
            .await?;
        Ok(FsReceipt { value, work })
    }

    async fn stage_empty_attributes(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<ObjectId> {
        let encoded = encode_attribute_page(
            &AttributePage::Leaf(Vec::new()),
            self.volume.config.limits.maximum_directory_page_entries,
        )
        .map_err(|error| OperationFailure::before_work(error.into()))?;
        let (value, work) = self
            .volume
            .fs
            .put_encoded(
                ObjectKind::AttributePage,
                encoded,
                WorkCounters::default(),
                budget,
                cancellation,
            )
            .await?;
        Ok(FsReceipt { value, work })
    }

    /// Rejects a new namespace binding that violates the volume's name
    /// policy before any mutation is attempted: a name that is not canonical
    /// NFC under `UnicodePolicy::RequireNfc`, or a name that collides with an
    /// existing sibling once case-folded under
    /// `CaseSensitivity::ProfileFolded`. A no-op under the default
    /// `Sensitive`/`Preserve` policy, and for the volume root (which has no
    /// siblings to collide with).
    async fn admit_new_name(
        &mut self,
        path: &NamespacePath,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        allowed_file_ids: &[FileId],
        parent_pending: bool,
    ) -> FsResult<()> {
        let mut work = WorkCounters::default();
        let Some((parent_components, name)) = path.split_last() else {
            return Ok(FsReceipt { value: (), work });
        };
        if self.volume.config.unicode == UnicodePolicy::RequireNfc
            && name.encoding() == NameEncoding::Utf8
        {
            let is_nfc =
                std::str::from_utf8(name.as_bytes()).is_ok_and(unicode_normalization::is_nfc);
            if !is_nfc {
                return Err(OperationFailure::new(FsError::NonNormalizedName, work));
            }
        }
        if self.volume.config.case_sensitivity == CaseSensitivity::ProfileFolded && !parent_pending
        {
            let found = self
                .find_case_folded_sibling(
                    parent_components,
                    name,
                    remaining(work, budget)?,
                    cancellation,
                    allowed_file_ids,
                )
                .await
                .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
            work = add(work, found.work)?;
            if found.value.is_some() {
                return Err(OperationFailure::new(FsError::NameCollision, work));
            }
        }
        Ok(FsReceipt { value: (), work })
    }

    /// Scans `parent`'s entries for one whose name is not byte-identical to
    /// `target` but shares its [`LogicalName::case_fold_key`], returning that
    /// sibling's exact canonical name. Used both to reject a colliding create
    /// ([`Self::admit_new_name`]) and to resolve a case-insensitive lookup
    /// that missed on an exact match.
    ///
    /// `parent_components` come from an already-admitted path's own prefix,
    /// so reconstructing it cannot exceed the same volume's bounds; if it
    /// somehow did, this returns `Ok(None)` rather than inventing an
    /// unrelated error — callers fall back to their own exact-match result.
    async fn find_case_folded_sibling(
        &mut self,
        parent_components: &[LogicalName],
        target: &LogicalName,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        allowed_file_ids: &[FileId],
    ) -> FsResult<Option<LogicalName>> {
        let mut work = WorkCounters::default();
        let Ok(parent) = NamespacePath::new(parent_components.to_vec(), self.volume.config.limits)
        else {
            return Ok(FsReceipt { value: None, work });
        };
        let folded_target = target.case_fold_key();
        let mut after: Option<LogicalName> = None;
        loop {
            // Boxed: `list_directory` resolves its own path through
            // `lookup_no_follow_observed`, which calls back into this
            // function on an exact-match miss, so the future is otherwise
            // unboundedly self-referential at compile time.
            let page = Box::pin(self.list_directory(
                &parent,
                after.as_ref(),
                CASE_FOLD_COLLISION_SCAN_PAGE,
                remaining(work, budget)?,
                cancellation,
            ))
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
            work = add(work, page.work)?;
            for entry in &page.value.entries {
                if entry.name != *target
                    && entry.name.case_fold_key() == folded_target
                    && !allowed_file_ids.contains(&entry.file_id)
                {
                    return Ok(FsReceipt {
                        value: Some(entry.name.clone()),
                        work,
                    });
                }
            }
            if !page.value.has_more {
                break;
            }
            after = page.value.entries.last().map(|entry| entry.name.clone());
        }
        Ok(FsReceipt { value: None, work })
    }

    /// Creates a regular file, embedding tiny content and sparsely staging
    /// larger content in one atomic checkout mutation batch.
    ///
    /// # Errors
    ///
    /// Returns measured path, identity, encoding, blob, mutation, storage,
    /// cancellation, allocation, or bounded-work failures.
    pub async fn create_file(
        &mut self,
        path: NamespacePath,
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<FileId> {
        cancellation
            .check()
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX)
            > self.volume.config.limits.maximum_read_bytes
        {
            return Err(OperationFailure::before_work(FsError::FileRead(
                FileRangeReadError::InvalidRange,
            )));
        }
        let mut work = WorkCounters::default();
        let metadata = self
            .volume
            .fs
            .put_encoded(
                ObjectKind::Metadata,
                encode_file_metadata(empty_metadata())
                    .map_err(|error| OperationFailure::new(error.into(), work))?,
                work,
                budget,
                cancellation,
            )
            .await?;
        work = metadata.1;
        let file_id = FileId::new();
        let mut operations = Vec::new();
        if bytes.len() <= crate::kernel::MAXIMUM_INLINE_FILE_BYTES {
            operations.push(Mutation::Create {
                path,
                record: FileRecord {
                    file_id,
                    kind: FileKind::Regular,
                    link_count: 1,
                    metadata: metadata.0,
                    payload: FilePayload::InlineRegular(
                        InlineFileData::new(&bytes)
                            .map_err(|error| OperationFailure::new(error.into(), work))?,
                    ),
                },
            });
        } else {
            let blob = self
                .stage_blob(bytes, remaining(work, budget)?, cancellation)
                .await
                .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
            work = add(work, blob.work)?;
            let length = blob.value.logical_bytes;
            operations.push(Mutation::Create {
                path: path.clone(),
                record: FileRecord {
                    file_id,
                    kind: FileKind::Regular,
                    link_count: 1,
                    metadata: metadata.0,
                    payload: FilePayload::InlineRegular(
                        InlineFileData::new(&[])
                            .map_err(|error| OperationFailure::new(error.into(), work))?,
                    ),
                },
            });
            operations.push(Mutation::Write {
                path,
                offset: 0,
                length,
                content: blob.value.root,
                content_offset: 0,
            });
        }
        let mutation = self
            .mutate(operations, remaining(work, budget)?, cancellation)
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, mutation.work)?;
        Ok(FsReceipt {
            value: file_id,
            work,
        })
    }

    /// Creates an empty directory without scanning its parent or siblings.
    ///
    /// # Errors
    ///
    /// Returns measured path, encoding, mutation, storage, cancellation,
    /// allocation, or bounded-work failures.
    pub async fn create_directory(
        &mut self,
        path: NamespacePath,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<FileId> {
        let mut work = WorkCounters::default();
        let metadata = self
            .volume
            .fs
            .put_encoded(
                ObjectKind::Metadata,
                encode_file_metadata(empty_metadata())
                    .map_err(|error| OperationFailure::new(error.into(), work))?,
                work,
                budget,
                cancellation,
            )
            .await?;
        work = metadata.1;
        let tree = self
            .volume
            .fs
            .put_encoded(
                ObjectKind::TreePage,
                encode_tree_page(
                    &TreePage::Leaf(Vec::new()),
                    self.volume.config.limits.maximum_directory_page_entries,
                )
                .map_err(|error| OperationFailure::new(error.into(), work))?,
                work,
                budget,
                cancellation,
            )
            .await?;
        work = tree.1;
        let file_id = FileId::new();
        let mutation = self
            .mutate(
                vec![Mutation::Create {
                    path,
                    record: FileRecord {
                        file_id,
                        kind: FileKind::Directory,
                        link_count: 1,
                        metadata: metadata.0,
                        payload: FilePayload::Directory { entries: tree.0 },
                    },
                }],
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, mutation.work)?;
        Ok(FsReceipt {
            value: file_id,
            work,
        })
    }

    /// Creates a symbolic link whose target bytes remain opaque and exact.
    ///
    /// # Errors
    ///
    /// Returns typed capability, path, blob, metadata, mutation, storage,
    /// cancellation, allocation, or bounded-work failures.
    pub async fn create_symbolic_link(
        &mut self,
        path: NamespacePath,
        target: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<FileId> {
        let blob = self.stage_blob(target, budget, cancellation).await?;
        let mut work = blob.work;
        let created = self
            .create_record(
                path,
                FileKind::SymbolicLink,
                FilePayload::SymbolicLink {
                    target_bytes: blob.value.logical_bytes,
                    target: blob.value.root,
                },
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, created.work)?;
        Ok(FsReceipt {
            value: created.value,
            work,
        })
    }

    /// Reads exact opaque symbolic-link target bytes without following it.
    ///
    /// # Errors
    ///
    /// Returns typed absence, kind, corruption, cancellation, storage,
    /// allocation, or bounded-work failures.
    pub async fn read_symbolic_link(
        &mut self,
        path: &NamespacePath,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Bytes> {
        let lookup = self.lookup_no_follow(path, budget, cancellation).await?;
        let mut work = lookup.work;
        let record = lookup
            .value
            .record
            .ok_or_else(|| OperationFailure::new(FsError::NotFound, work))?;
        let FilePayload::SymbolicLink {
            target_bytes,
            target,
        } = record.payload
        else {
            return Err(OperationFailure::new(FsError::NotSymbolicLink, work));
        };
        if target_bytes > self.volume.config.limits.maximum_read_bytes {
            return Err(OperationFailure::new(
                FsError::FileRead(FileRangeReadError::InvalidRange),
                work,
            ));
        }
        if target_bytes == 0 {
            return Ok(FsReceipt {
                value: Bytes::new(),
                work,
            });
        }
        let read = read_blob_range_async(
            &self.volume.fs.inner.objects,
            target,
            ByteRange {
                offset: 0,
                length: target_bytes,
            },
            decode_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::BlobRead))?;
        work = add(work, read.work)?;
        Ok(FsReceipt {
            value: read.bytes,
            work,
        })
    }

    /// Creates a FIFO, socket, or explicit mounted-volume boundary.
    ///
    /// # Errors
    ///
    /// Rejects payload-bearing kinds and returns typed metadata, mutation,
    /// storage, cancellation, or bounded-work failures.
    pub async fn create_empty_special(
        &mut self,
        path: NamespacePath,
        kind: FileKind,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<FileId> {
        if !matches!(
            kind,
            FileKind::Fifo | FileKind::Socket | FileKind::MountBoundary
        ) {
            return Err(OperationFailure::before_work(FsError::InvalidSpecialKind));
        }
        self.validate_profile_kind(kind, WorkCounters::default())?;
        self.create_record(path, kind, FilePayload::Empty, budget, cancellation)
            .await
    }

    /// Creates an exact POSIX character or block device identity.
    ///
    /// # Errors
    ///
    /// Rejects non-device kinds and returns typed metadata, mutation, storage,
    /// cancellation, or bounded-work failures.
    pub async fn create_device(
        &mut self,
        path: NamespacePath,
        kind: FileKind,
        major: u32,
        minor: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<FileId> {
        if !matches!(kind, FileKind::CharacterDevice | FileKind::BlockDevice) {
            return Err(OperationFailure::before_work(FsError::InvalidSpecialKind));
        }
        self.validate_profile_kind(kind, WorkCounters::default())?;
        self.create_record(
            path,
            kind,
            FilePayload::Device { major, minor },
            budget,
            cancellation,
        )
        .await
    }

    /// Creates an opaque Windows reparse-point payload.
    ///
    /// # Errors
    ///
    /// Returns typed path, blob, metadata, mutation, storage, cancellation,
    /// allocation, or bounded-work failures.
    pub async fn create_reparse_point(
        &mut self,
        path: NamespacePath,
        payload: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<FileId> {
        self.validate_profile_kind(FileKind::ReparsePoint, WorkCounters::default())?;
        let blob = self.stage_blob(payload, budget, cancellation).await?;
        let mut work = blob.work;
        let created = self
            .create_record(
                path,
                FileKind::ReparsePoint,
                FilePayload::ReparsePoint {
                    payload_bytes: blob.value.logical_bytes,
                    payload: blob.value.root,
                },
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, created.work)?;
        Ok(FsReceipt {
            value: created.value,
            work,
        })
    }

    /// Reads exact opaque Windows reparse-point bytes without interpreting them.
    ///
    /// # Errors
    ///
    /// Returns typed absence, kind, corruption, cancellation, storage,
    /// allocation, or bounded-work failures.
    pub async fn read_reparse_point(
        &mut self,
        path: &NamespacePath,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Bytes> {
        let lookup = self.lookup_no_follow(path, budget, cancellation).await?;
        let mut work = lookup.work;
        let record = lookup
            .value
            .record
            .ok_or_else(|| OperationFailure::new(FsError::NotFound, work))?;
        let FilePayload::ReparsePoint {
            payload_bytes,
            payload,
        } = record.payload
        else {
            return Err(OperationFailure::new(FsError::NotReparsePoint, work));
        };
        if payload_bytes > self.volume.config.limits.maximum_read_bytes {
            return Err(OperationFailure::new(
                FsError::FileRead(FileRangeReadError::InvalidRange),
                work,
            ));
        }
        if payload_bytes == 0 {
            return Ok(FsReceipt {
                value: Bytes::new(),
                work,
            });
        }
        let read = read_blob_range_async(
            &self.volume.fs.inner.objects,
            payload,
            ByteRange {
                offset: 0,
                length: payload_bytes,
            },
            decode_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::BlobRead))?;
        work = add(work, read.work)?;
        Ok(FsReceipt {
            value: read.bytes,
            work,
        })
    }

    async fn create_record(
        &mut self,
        path: NamespacePath,
        kind: FileKind,
        payload: FilePayload,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<FileId> {
        let mut work = WorkCounters::default();
        let metadata = self
            .volume
            .fs
            .put_encoded(
                ObjectKind::Metadata,
                encode_file_metadata(empty_metadata())
                    .map_err(|error| OperationFailure::new(error.into(), work))?,
                work,
                budget,
                cancellation,
            )
            .await?;
        work = metadata.1;
        let file_id = FileId::new();
        let mutation = self
            .mutate(
                vec![Mutation::Create {
                    path,
                    record: FileRecord {
                        file_id,
                        kind,
                        link_count: 1,
                        metadata: metadata.0,
                        payload,
                    },
                }],
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, mutation.work)?;
        Ok(FsReceipt {
            value: file_id,
            work,
        })
    }

    /// Replaces one file range with caller bytes, preserving sparse untouched ranges.
    ///
    /// # Errors
    ///
    /// Returns measured path, blob, mutation, storage, cancellation,
    /// allocation, or bounded-work failures.
    pub async fn write_file(
        &mut self,
        path: NamespacePath,
        offset: u64,
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        if bytes.is_empty() {
            return self
                .mutate(
                    vec![Mutation::ValidateRegular { path }],
                    budget,
                    cancellation,
                )
                .await;
        }
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX)
            > self.volume.config.limits.maximum_read_bytes
        {
            return Err(OperationFailure::before_work(FsError::FileRead(
                FileRangeReadError::InvalidRange,
            )));
        }
        let blob = self.stage_blob(bytes, budget, cancellation).await?;
        let mut work = blob.work;
        let mutation = self
            .mutate(
                vec![Mutation::Write {
                    path,
                    offset,
                    length: blob.value.logical_bytes,
                    content: blob.value.root,
                    content_offset: 0,
                }],
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, mutation.work)?;
        Ok(FsReceipt { value: (), work })
    }

    /// Captures one regular file as an ephemeral path-independent open view.
    ///
    /// The returned file shares authenticated immutable objects with this
    /// volume. Its later mutations are visible only through copies of this
    /// detached value and are never published into a generation.
    ///
    /// # Errors
    ///
    /// Returns typed absence, non-regular kind, path, storage, cancellation,
    /// dependency, or bounded-work failures.
    pub async fn detach_regular_file(
        &mut self,
        path: &NamespacePath,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<DetachedFile<A, O>> {
        let lookup = self
            .lookup_no_follow_observed(path, budget, cancellation, true)
            .await?;
        let record = lookup
            .value
            .record
            .ok_or_else(|| OperationFailure::new(FsError::NotFound, lookup.work))?;
        if record.kind != FileKind::Regular {
            return Err(OperationFailure::new(
                FsError::FileRead(FileRangeReadError::NotRegular),
                lookup.work,
            ));
        }
        Ok(FsReceipt {
            value: DetachedFile {
                volume: self.volume.clone(),
                record,
            },
            work: lookup.work,
        })
    }

    /// Reads one authenticated candidate file record by stable identity.
    ///
    /// Tracking modes retain a complete file-record dependency. More precise
    /// byte and metadata APIs below retain only their semantic regions.
    ///
    /// # Errors
    ///
    /// Returns typed absence, authentication, storage, cancellation,
    /// dependency, allocation, or bounded-work failures.
    pub async fn read_file_record_by_id(
        &mut self,
        file_id: FileId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<FileRecord> {
        let lookup = self
            .lookup_file_record_by_id(file_id, budget, cancellation)
            .await?;
        let mut work = lookup.work;
        let record = lookup
            .value
            .ok_or_else(|| OperationFailure::new(FsError::NotFound, work))?;
        if self.mode.consistency != ConsistencyMode::Pinned {
            work = self
                .observe_identity_region(
                    DependencyRegion::FileRecord(file_id),
                    work,
                    budget,
                    cancellation,
                )
                .await?;
        }
        Ok(FsReceipt {
            value: record,
            work,
        })
    }

    /// Reads one exact logical range by stable file identity.
    ///
    /// # Errors
    ///
    /// Returns measured absence, non-regular kind, range, authentication,
    /// dependency, cancellation, storage, allocation, or bounded-work failures.
    pub async fn read_file_range_by_id(
        &mut self,
        file_id: FileId,
        range: ByteRange,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<FileRangeRead> {
        if range.length > self.volume.config.limits.maximum_read_bytes {
            return Err(OperationFailure::before_work(FsError::FileRead(
                FileRangeReadError::InvalidRange,
            )));
        }
        let lookup = self
            .lookup_file_record_by_id(file_id, budget, cancellation)
            .await?;
        let mut work = lookup.work;
        let record = lookup
            .value
            .ok_or_else(|| OperationFailure::new(FsError::NotFound, work))?;
        let mut read = read_file_range_async(
            &self.volume.fs.inner.objects,
            FileRangeRequest {
                record,
                range,
                maximum_spans: self.volume.config.limits.maximum_directory_page_entries,
                limits: decode_limits(self.volume.config),
                budget: remaining(work, budget)?,
            },
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::FileRead))?;
        work = add(work, read.work)?;
        if self.mode.consistency != ConsistencyMode::Pinned && range.length != 0 {
            work = self
                .observe_base_regular_range(file_id, range, work, budget, cancellation)
                .await?;
        }
        read.work = work;
        Ok(FsReceipt { value: read, work })
    }

    /// Plans one bounded sparse range by stable file identity without reading
    /// content blobs or resolving namespace bindings.
    ///
    /// `None` identifies inline regular content. Tracking modes retain the
    /// exact byte-range dependency represented by the returned plan.
    ///
    /// # Errors
    ///
    /// Returns measured absence, non-regular kind, invalid bounds,
    /// authentication, dependency, cancellation, allocation, storage, or
    /// bounded-work failures.
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
        let lookup = self
            .lookup_file_record_by_id(file_id, budget, cancellation)
            .await?;
        let mut work = lookup.work;
        let record = lookup
            .value
            .ok_or_else(|| OperationFailure::new(FsError::NotFound, work))?;
        let (logical_bytes, extents) = match record.payload {
            FilePayload::InlineRegular(data) => {
                let logical_bytes = u64::try_from(data.as_bytes().len()).unwrap_or(u64::MAX);
                validate_planned_file_range(logical_bytes, range, work)?;
                if self.mode.consistency != ConsistencyMode::Pinned && range.length != 0 {
                    work = self
                        .observe_base_regular_range(file_id, range, work, budget, cancellation)
                        .await?;
                }
                return Ok(FsReceipt { value: None, work });
            }
            FilePayload::Regular {
                logical_bytes,
                extents,
            } => (logical_bytes, extents),
            FilePayload::Directory { .. }
            | FilePayload::SymbolicLink { .. }
            | FilePayload::Device { .. }
            | FilePayload::Empty
            | FilePayload::ReparsePoint { .. } => {
                return Err(OperationFailure::new(
                    FsError::FileRead(FileRangeReadError::NotRegular),
                    work,
                ));
            }
        };
        let mut plan = plan_extent_range_async(
            &self.volume.fs.inner.objects,
            ExtentRangeRequest {
                root: extents,
                file_size: logical_bytes,
                range,
                maximum_spans,
                limits: decode_limits(self.volume.config),
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
        if self.mode.consistency != ConsistencyMode::Pinned && range.length != 0 {
            work = self
                .observe_base_regular_range(file_id, range, work, budget, cancellation)
                .await?;
        }
        plan.work = work;
        Ok(FsReceipt {
            value: Some(plan),
            work,
        })
    }

    /// Finds the first sparse data or hole boundary by stable file identity.
    ///
    /// # Errors
    ///
    /// Returns measured absence, non-regular kind, malformed extent,
    /// dependency, cancellation, storage, allocation, or bounded-work failures.
    pub async fn seek_file_extent_by_id(
        &mut self,
        file_id: FileId,
        offset: u64,
        target: ExtentSeekTarget,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Option<u64>> {
        let lookup = self
            .lookup_file_record_by_id(file_id, budget, cancellation)
            .await?;
        let mut work = lookup.work;
        let record = lookup
            .value
            .ok_or_else(|| OperationFailure::new(FsError::NotFound, work))?;
        let value = match record.payload {
            FilePayload::InlineRegular(data) => {
                let logical_bytes = u64::try_from(data.as_bytes().len()).unwrap_or(u64::MAX);
                if offset > logical_bytes {
                    None
                } else {
                    match target {
                        ExtentSeekTarget::Data => (offset < logical_bytes).then_some(offset),
                        ExtentSeekTarget::Hole => Some(logical_bytes),
                    }
                }
            }
            FilePayload::Regular {
                logical_bytes,
                extents,
            } => {
                if offset > logical_bytes {
                    None
                } else {
                    let receipt = seek_extent_async(
                        &self.volume.fs.inner.objects,
                        ExtentSeekRequest {
                            root: extents,
                            file_size: logical_bytes,
                            offset,
                            target,
                            limits: decode_limits(self.volume.config),
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
                    work = add(work, receipt.work)?;
                    receipt.value
                }
            }
            FilePayload::Directory { .. }
            | FilePayload::SymbolicLink { .. }
            | FilePayload::Empty
            | FilePayload::Device { .. }
            | FilePayload::ReparsePoint { .. } => {
                return Err(OperationFailure::new(
                    FsError::FileRead(FileRangeReadError::NotRegular),
                    work,
                ));
            }
        };
        if self.mode.consistency != ConsistencyMode::Pinned {
            work = self
                .observe_identity_region(
                    DependencyRegion::SparseSeek {
                        file_id,
                        offset,
                        target,
                    },
                    work,
                    budget,
                    cancellation,
                )
                .await?;
        }
        Ok(FsReceipt { value, work })
    }

    /// Reads complete canonical metadata by stable file identity.
    ///
    /// # Errors
    ///
    /// Returns measured absence, authentication, decode, dependency, storage,
    /// cancellation, allocation, or bounded-work failures.
    pub async fn read_metadata_by_id(
        &mut self,
        file_id: FileId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<FileMetadata> {
        let lookup = self
            .lookup_file_record_by_id(file_id, budget, cancellation)
            .await?;
        let mut work = lookup.work;
        let record = lookup
            .value
            .ok_or_else(|| OperationFailure::new(FsError::NotFound, work))?;
        let metadata = self
            .volume
            .fs
            .inner
            .objects
            .read(
                record.metadata,
                self.volume.config.limits.maximum_object_bytes,
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, FsError::Object))?;
        work = add(work, metadata.work)?;
        let value = decode_file_metadata(&metadata.value, decode_limits(self.volume.config))
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        if self.mode.consistency != ConsistencyMode::Pinned {
            work = self
                .observe_base_metadata_ids(
                    std::slice::from_ref(&file_id),
                    work,
                    budget,
                    cancellation,
                )
                .await?;
        }
        Ok(FsReceipt { value, work })
    }

    /// Reads one exact named attribute by stable file identity.
    ///
    /// # Errors
    ///
    /// Returns measured metadata, attribute-tree, blob, storage,
    /// authentication, cancellation, allocation, or bounded-work failures.
    pub async fn read_named_attribute_by_id(
        &mut self,
        file_id: FileId,
        name: &AttributeName,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Option<Bytes>> {
        let metadata = self
            .read_metadata_by_id(file_id, budget, cancellation)
            .await?;
        let mut work = metadata.work;
        let MetadataField::Value(root) = metadata.value.named_attributes else {
            return Ok(FsReceipt { value: None, work });
        };
        let lookup = lookup_attribute_async(
            &self.volume.fs.inner.objects,
            root,
            name,
            decode_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::AttributeLookup))?;
        work = add(work, lookup.work)?;
        let Some(entry) = lookup.entry else {
            return Ok(FsReceipt { value: None, work });
        };
        if entry.value_bytes > self.volume.config.limits.maximum_read_bytes {
            return Err(OperationFailure::new(
                FsError::FileRead(FileRangeReadError::InvalidRange),
                work,
            ));
        }
        let read = read_blob_range_async(
            &self.volume.fs.inner.objects,
            entry.value,
            ByteRange {
                offset: 0,
                length: entry.value_bytes,
            },
            decode_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::BlobRead))?;
        work = add(work, read.work)?;
        Ok(FsReceipt {
            value: Some(read.bytes),
            work,
        })
    }

    /// Lists one bounded page of named attributes by stable file identity.
    ///
    /// # Errors
    ///
    /// Returns measured metadata, pagination, storage, authentication,
    /// cancellation, allocation, or bounded-work failures.
    pub async fn list_named_attributes_by_id(
        &mut self,
        file_id: FileId,
        after: Option<&AttributeName>,
        maximum_entries: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<AttributeListing> {
        let metadata = self
            .read_metadata_by_id(file_id, budget, cancellation)
            .await?;
        let mut work = metadata.work;
        let MetadataField::Value(root) = metadata.value.named_attributes else {
            let value = AttributeListing {
                entries: Vec::new(),
                has_more: false,
                next_residency: None,
                work,
            };
            return Ok(FsReceipt { value, work });
        };
        let mut listing = list_attributes_async(
            &self.volume.fs.inner.objects,
            root,
            after,
            maximum_entries,
            decode_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::AttributeList))?;
        work = add(work, listing.work)?;
        listing.work = work;
        Ok(FsReceipt {
            value: listing,
            work,
        })
    }

    /// Inserts or replaces one named attribute by stable file identity.
    ///
    /// # Errors
    ///
    /// Returns measured precondition, metadata, blob, attribute-tree, storage,
    /// mutation, cancellation, allocation, or bounded-work failures.
    pub async fn write_named_attribute_by_id(
        &mut self,
        file_id: FileId,
        name: AttributeName,
        value: Bytes,
        mode: NamedAttributeWriteMode,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        let metadata = self
            .read_metadata_by_id(file_id, budget, cancellation)
            .await?;
        let mut work = metadata.work;
        let mut metadata_value = metadata.value;
        let root = match metadata_value.named_attributes {
            MetadataField::Unavailable => {
                let empty = self
                    .stage_empty_attributes(remaining(work, budget)?, cancellation)
                    .await
                    .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
                work = add(work, empty.work)?;
                empty.value
            }
            MetadataField::Value(root) => root,
        };
        let current = lookup_attribute_async(
            &self.volume.fs.inner.objects,
            root,
            &name,
            decode_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::AttributeLookup))?;
        work = add(work, current.work)?;
        match (mode, current.entry.as_ref()) {
            (NamedAttributeWriteMode::Create, Some(_))
            | (NamedAttributeWriteMode::Replace, None) => {
                return Err(OperationFailure::new(FsError::CreationRejected, work));
            }
            _ => {}
        }
        let blob = self
            .stage_blob(value, remaining(work, budget)?, cancellation)
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, blob.work)?;
        let replacement = AttributeEntry {
            name,
            value_bytes: blob.value.logical_bytes,
            value: blob.value.root,
        };
        let mutation = match current.entry {
            None => AttributeMutation::Insert(replacement),
            Some(expected) => AttributeMutation::Replace {
                expected,
                replacement,
            },
        };
        let changed = apply_attribute_mutations_async(
            &self.volume.fs.inner.objects,
            root,
            vec![mutation],
            1,
            decode_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::AttributeMutation))?;
        work = add(work, changed.work)?;
        metadata_value.named_attributes = MetadataField::Value(changed.root);
        let changed_metadata = self
            .set_metadata_by_id(
                file_id,
                metadata_value,
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, changed_metadata.work)?;
        Ok(FsReceipt { value: (), work })
    }

    /// Removes one named attribute by stable file identity.
    ///
    /// # Errors
    ///
    /// Returns authenticated absence or the same measured failures as
    /// [`Self::write_named_attribute_by_id`].
    pub async fn remove_named_attribute_by_id(
        &mut self,
        file_id: FileId,
        name: AttributeName,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        let metadata = self
            .read_metadata_by_id(file_id, budget, cancellation)
            .await?;
        let mut work = metadata.work;
        let mut metadata_value = metadata.value;
        let MetadataField::Value(root) = metadata_value.named_attributes else {
            return Err(OperationFailure::new(FsError::NotFound, work));
        };
        let current = lookup_attribute_async(
            &self.volume.fs.inner.objects,
            root,
            &name,
            decode_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::AttributeLookup))?;
        work = add(work, current.work)?;
        let expected = current
            .entry
            .ok_or_else(|| OperationFailure::new(FsError::NotFound, work))?;
        let changed = apply_attribute_mutations_async(
            &self.volume.fs.inner.objects,
            root,
            vec![AttributeMutation::Remove {
                name,
                expected: Some(expected),
            }],
            1,
            decode_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::AttributeMutation))?;
        work = add(work, changed.work)?;
        metadata_value.named_attributes = MetadataField::Value(changed.root);
        let changed_metadata = self
            .set_metadata_by_id(
                file_id,
                metadata_value,
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, changed_metadata.work)?;
        Ok(FsReceipt { value: (), work })
    }

    /// Replaces one range of an attached regular file by stable identity.
    ///
    /// # Errors
    ///
    /// Returns measured blob, mutation, storage, cancellation, allocation, or
    /// bounded-work failures. Authenticated absence never scans the namespace.
    pub async fn write_file_by_id(
        &mut self,
        file_id: FileId,
        offset: u64,
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        if bytes.is_empty() {
            return self
                .mutate(
                    vec![Mutation::File {
                        file_id,
                        mutation: FileMutation::ValidateRegular,
                    }],
                    budget,
                    cancellation,
                )
                .await;
        }
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX)
            > self.volume.config.limits.maximum_read_bytes
        {
            return Err(OperationFailure::before_work(FsError::FileRead(
                FileRangeReadError::InvalidRange,
            )));
        }
        let blob = self.stage_blob(bytes, budget, cancellation).await?;
        let mut work = blob.work;
        let mutation = self
            .mutate(
                vec![Mutation::File {
                    file_id,
                    mutation: FileMutation::Write {
                        offset,
                        length: blob.value.logical_bytes,
                        content: blob.value.root,
                        content_offset: 0,
                    },
                }],
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, mutation.work)?;
        Ok(FsReceipt { value: (), work })
    }

    /// Replaces complete canonical metadata by stable identity.
    ///
    /// # Errors
    ///
    /// Returns measured encoding, storage, mutation, cancellation, allocation,
    /// or bounded-work failures.
    pub async fn set_metadata_by_id(
        &mut self,
        file_id: FileId,
        metadata: FileMetadata,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        let encoded = encode_file_metadata(metadata)
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        let (metadata, mut work) = self
            .volume
            .fs
            .put_encoded(
                ObjectKind::Metadata,
                encoded,
                WorkCounters::default(),
                budget,
                cancellation,
            )
            .await?;
        let mutation = self
            .mutate(
                vec![Mutation::File {
                    file_id,
                    mutation: FileMutation::SetMetadata { metadata },
                }],
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, mutation.work)?;
        Ok(FsReceipt { value: (), work })
    }

    /// Replaces complete metadata and optionally logical size as one attached
    /// file-table transaction.
    ///
    /// # Errors
    ///
    /// Returns measured encoding, storage, sparse mutation, cancellation,
    /// allocation, or bounded-work failures. The candidate record changes only
    /// if the complete ordered batch succeeds.
    pub async fn set_attributes_by_id(
        &mut self,
        file_id: FileId,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        let encoded = encode_file_metadata(metadata)
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        let (metadata, mut work) = self
            .volume
            .fs
            .put_encoded(
                ObjectKind::Metadata,
                encoded,
                WorkCounters::default(),
                budget,
                cancellation,
            )
            .await?;
        let mut operations = vec![Mutation::File {
            file_id,
            mutation: FileMutation::SetMetadata { metadata },
        }];
        if let Some(logical_bytes) = logical_bytes {
            operations.push(Mutation::File {
                file_id,
                mutation: FileMutation::Resize { logical_bytes },
            });
        }
        let mutation = self
            .mutate(operations, remaining(work, budget)?, cancellation)
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, mutation.work)?;
        Ok(FsReceipt { value: (), work })
    }

    /// Changes logical length by stable identity.
    ///
    /// # Errors
    ///
    /// Returns the same measured failures as [`Self::mutate`].
    pub async fn resize_file_by_id(
        &mut self,
        file_id: FileId,
        logical_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        self.mutate(
            vec![Mutation::File {
                file_id,
                mutation: FileMutation::Resize { logical_bytes },
            }],
            budget,
            cancellation,
        )
        .await
    }

    /// Punches a hole or writes allocated zeros by stable identity.
    ///
    /// # Errors
    ///
    /// Returns the same measured failures as [`Self::mutate`].
    pub async fn zero_file_range_by_id(
        &mut self,
        file_id: FileId,
        range: ByteRange,
        allocated: bool,
        extend: bool,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        self.mutate(
            vec![Mutation::File {
                file_id,
                mutation: FileMutation::ZeroRange {
                    offset: range.offset,
                    length: range.length,
                    allocated,
                    extend,
                },
            }],
            budget,
            cancellation,
        )
        .await
    }

    /// Allocates sparse holes by stable identity.
    ///
    /// # Errors
    ///
    /// Returns the same measured failures as [`Self::mutate`].
    pub async fn preallocate_file_by_id(
        &mut self,
        file_id: FileId,
        range: ByteRange,
        keep_size: bool,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        self.mutate(
            vec![Mutation::File {
                file_id,
                mutation: FileMutation::Preallocate {
                    offset: range.offset,
                    length: range.length,
                    keep_size,
                },
            }],
            budget,
            cancellation,
        )
        .await
    }

    /// Clones one sparse logical range between stable identities.
    ///
    /// # Errors
    ///
    /// Returns the same measured failures as [`Self::mutate`].
    #[allow(clippy::too_many_arguments)]
    pub async fn clone_file_range_by_id(
        &mut self,
        source_file_id: FileId,
        source_offset: u64,
        destination_file_id: FileId,
        destination_offset: u64,
        length: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        self.mutate(
            vec![Mutation::CloneFileRange {
                source_file_id,
                source_offset,
                destination_file_id,
                destination_offset,
                length,
            }],
            budget,
            cancellation,
        )
        .await
    }

    /// Removes one namespace binding with an optional exact identity precondition.
    ///
    /// # Errors
    ///
    /// Returns the same measured failures as [`Self::mutate`].
    pub async fn remove(
        &mut self,
        path: NamespacePath,
        expected_file_id: Option<FileId>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        self.mutate(
            vec![Mutation::Remove {
                path,
                expected_file_id: expected_file_id
                    .map_or(MetadataField::Unavailable, MetadataField::Value),
            }],
            budget,
            cancellation,
        )
        .await
    }

    /// Atomically renames one binding within this volume.
    ///
    /// # Errors
    ///
    /// Returns the same measured failures as [`Self::mutate`].
    pub async fn rename(
        &mut self,
        source: NamespacePath,
        destination: NamespacePath,
        replace: bool,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        self.mutate(
            vec![Mutation::Rename {
                source,
                destination,
                replace,
            }],
            budget,
            cancellation,
        )
        .await
    }

    /// Creates another binding to an existing non-directory file.
    ///
    /// # Errors
    ///
    /// Returns the same measured failures as [`Self::mutate`].
    pub async fn hard_link(
        &mut self,
        source: NamespacePath,
        destination: NamespacePath,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        self.mutate(
            vec![Mutation::Link {
                source,
                destination,
            }],
            budget,
            cancellation,
        )
        .await
    }

    /// Replaces the complete canonical metadata record for one existing file.
    ///
    /// The encoded metadata object is admitted before the checkout mutation;
    /// failed publication can leave only an unreachable immutable object.
    ///
    /// # Errors
    ///
    /// Returns measured metadata encoding, object-storage, cancellation,
    /// mutation, allocation, or bounded-work failures.
    pub async fn set_metadata(
        &mut self,
        path: NamespacePath,
        metadata: FileMetadata,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        let mut work = WorkCounters::default();
        let encoded = encode_file_metadata(metadata)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        let stored = self
            .volume
            .fs
            .put_encoded(ObjectKind::Metadata, encoded, work, budget, cancellation)
            .await?;
        work = stored.1;
        let mutation = self
            .mutate(
                vec![Mutation::SetMetadata {
                    path,
                    metadata: stored.0,
                }],
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, mutation.work)?;
        Ok(FsReceipt { value: (), work })
    }

    /// Replaces complete metadata and optionally logical size as one path transaction.
    ///
    /// Immutable metadata may be staged first, but the candidate changes only
    /// when both the metadata and optional sparse resize validate and apply.
    ///
    /// # Errors
    ///
    /// Returns measured encoding, storage, sparse mutation, cancellation,
    /// allocation, or bounded-work failures.
    pub async fn set_attributes(
        &mut self,
        path: NamespacePath,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        let lookup = self
            .lookup_no_follow_observed(&path, budget, cancellation, true)
            .await?;
        let mut work = lookup.work;
        let file_id = lookup
            .value
            .record
            .ok_or_else(|| OperationFailure::new(FsError::NotFound, work))?
            .file_id;
        let mutation = self
            .set_attributes_by_id(
                file_id,
                metadata,
                logical_bytes,
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, mutation.work)?;
        Ok(FsReceipt { value: (), work })
    }

    /// Changes a regular file's logical length while preserving sparse semantics.
    ///
    /// # Errors
    ///
    /// Returns the same measured failures as [`Self::mutate`].
    pub async fn resize_file(
        &mut self,
        path: NamespacePath,
        logical_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        self.mutate(
            vec![Mutation::Resize {
                path,
                logical_bytes,
            }],
            budget,
            cancellation,
        )
        .await
    }

    /// Punches a hole or preserves physical allocation as canonical zeros.
    ///
    /// # Errors
    ///
    /// Returns the same measured failures as [`Self::mutate`].
    pub async fn zero_file_range(
        &mut self,
        path: NamespacePath,
        range: ByteRange,
        allocated: bool,
        extend: bool,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        self.mutate(
            vec![Mutation::ZeroRange {
                path,
                offset: range.offset,
                length: range.length,
                allocated,
                extend,
            }],
            budget,
            cancellation,
        )
        .await
    }

    /// Allocates sparse holes while preserving all existing file content.
    ///
    /// With `keep_size = false`, the file grows to include the complete range.
    /// A keep-size request extending beyond EOF is rejected explicitly because
    /// invisible host allocation is not part of the portable authenticated format.
    ///
    /// # Errors
    ///
    /// Returns typed range, capability, sparse-mutation, storage, cancellation,
    /// or bounded-work failures.
    pub async fn preallocate_file(
        &mut self,
        path: NamespacePath,
        range: ByteRange,
        keep_size: bool,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        self.mutate(
            vec![Mutation::Preallocate {
                path,
                offset: range.offset,
                length: range.length,
                keep_size,
            }],
            budget,
            cancellation,
        )
        .await
    }

    /// Clones one logical range by immutable extent reference without reading bytes.
    ///
    /// # Errors
    ///
    /// Returns the same measured failures as [`Self::mutate`].
    pub async fn clone_file_range(
        &mut self,
        request: FileCloneRequest,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        self.mutate(
            vec![Mutation::CloneRange {
                source: request.source,
                source_offset: request.source_offset,
                destination: request.destination,
                destination_offset: request.destination_offset,
                length: request.length,
            }],
            budget,
            cancellation,
        )
        .await
    }

    /// Streams caller content into immutable authenticated chunks without
    /// materializing the complete source in memory.
    ///
    /// The returned reference can be used by
    /// [`AuthoredMutation::CreateFileFromContent`] or
    /// [`AuthoredMutation::WriteFromContent`] in one later atomic transaction.
    /// Unused staged content is harmless and collectible.
    ///
    /// # Errors
    ///
    /// Rejects zero or volume-exceeding source bounds and returns exact source,
    /// storage, cancellation, allocation, encoding, or work-budget failures.
    pub async fn stage_content<R: AsyncBlobSource>(
        &self,
        source: &mut R,
        maximum_source_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<StagedContent> {
        if maximum_source_bytes == 0
            || maximum_source_bytes > self.volume.config.limits.maximum_generation_bytes
        {
            return Err(OperationFailure::before_work(FsError::FileRead(
                FileRangeReadError::InvalidRange,
            )));
        }
        let blob = self
            .stage_blob_source(source, maximum_source_bytes, budget, cancellation)
            .await?;
        Ok(FsReceipt {
            value: StagedContent {
                root: blob.value.root,
                logical_bytes: blob.value.logical_bytes,
            },
            work: blob.work,
        })
    }

    async fn stage_blob(
        &self,
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<crate::kernel::BlobBuild> {
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX)
            > self.volume.config.limits.maximum_read_bytes
        {
            return Err(OperationFailure::before_work(FsError::FileRead(
                FileRangeReadError::InvalidRange,
            )));
        }
        let maximum_blob_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX).max(1);
        let mut source = std::io::Cursor::new(bytes);
        self.stage_blob_source(&mut source, maximum_blob_bytes, budget, cancellation)
            .await
    }

    async fn stage_blob_source<R: AsyncBlobSource>(
        &self,
        source: &mut R,
        maximum_blob_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<crate::kernel::BlobBuild> {
        let chunk_bytes = u32::try_from(
            self.volume
                .config
                .limits
                .maximum_object_bytes
                .min(1024 * 1024),
        )
        .unwrap_or(u32::MAX)
        .max(1);
        let page_bytes = u32::try_from(self.volume.config.limits.maximum_object_bytes)
            .unwrap_or(u32::MAX)
            .max(1);
        let blob = build_blob_async(
            &self.volume.fs.inner.objects,
            source,
            BlobBuildOptions {
                chunk_bytes,
                page_items: self.volume.config.limits.maximum_directory_page_entries,
                page_bytes,
                maximum_blob_bytes,
            },
            budget,
            cancellation,
        )
        .await
        .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
        Ok(FsReceipt {
            work: blob.work,
            value: blob,
        })
    }

    /// Checkpoints, authenticates, and conditionally publishes this overlay.
    ///
    /// The caller supplies a stable operation identity and must reuse it only
    /// for an exact retry. Semantic authority conflicts are returned as values;
    /// failed or rejected publication leaves the private overlay intact.
    ///
    /// # Errors
    ///
    /// Rejects clean/non-writable checkouts and returns exact checkpoint,
    /// closure, authority, cancellation, or budget failures.
    pub async fn commit(
        &mut self,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<CheckoutCommitOutcome> {
        if self.mode.access != AccessMode::ReadWrite
            || self.mode.mutations != MutationMode::PrivateOverlay
        {
            return Err(OperationFailure::before_work(FsError::MutationNotAllowed));
        }
        self.publish_pending(operation_id, budget, cancellation)
            .await
    }

    async fn publish_pending(
        &mut self,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<CheckoutCommitOutcome> {
        if !self.has_pending_mutations() {
            return self
                .resolve_clean_commit(operation_id, budget, cancellation)
                .await;
        }
        let expected = self
            .authority_head
            .ok_or_else(|| OperationFailure::before_work(FsError::WritableCheckoutRequiresHead))?;
        cancellation
            .check()
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        let checkpoint = self.checkpoint_root(budget, cancellation).await?;
        let checkpoint_root = checkpoint.value;
        let mut work = checkpoint.work;
        let publication = publish_generation_async(
            &self.volume.fs.inner.objects,
            &self.volume.fs.inner.authority,
            PublishGenerationRequest {
                authority_id: volume_authority_id(self.volume.id),
                volume_id: self.volume.id,
                epoch: expected.epoch,
                expected,
                operation_id,
                generation_root: checkpoint_root,
            },
            closure_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
        work = add(work, publication.work)?;
        let generation_id = publication.proof.generation_id;
        let outcome = match publication.outcome {
            AppendOutcome::Committed(commit) => {
                let head = durable_head(&commit);
                self.base_generation_root = checkpoint_root;
                self.generation_root = checkpoint_root;
                self.root = publication.proof.root;
                self.base_root = self.root.clone();
                self.base_file_table = self.root.file_table;
                self.authority_head = Some(head);
                self.pending_operations.clear();
                self.prepared_merge_parent = None;
                self.dependencies.clear();
                self.last_commit = Some(LastCommit {
                    operation_id,
                    generation_id,
                    head,
                });
                CheckoutCommitOutcome::Committed {
                    generation_id,
                    head,
                }
            }
            AppendOutcome::AlreadyCommitted(commit) => {
                let head = durable_head(&commit);
                self.base_generation_root = checkpoint_root;
                self.generation_root = checkpoint_root;
                self.root = publication.proof.root;
                self.base_root = self.root.clone();
                self.base_file_table = self.root.file_table;
                self.authority_head = Some(head);
                self.pending_operations.clear();
                self.prepared_merge_parent = None;
                self.dependencies.clear();
                self.last_commit = Some(LastCommit {
                    operation_id,
                    generation_id,
                    head,
                });
                CheckoutCommitOutcome::AlreadyCommitted {
                    generation_id,
                    head,
                }
            }
            AppendOutcome::Conflict { actual } => CheckoutCommitOutcome::Conflict { actual },
            AppendOutcome::Fenced { actual_epoch } => {
                CheckoutCommitOutcome::Fenced { actual_epoch }
            }
            AppendOutcome::IdempotencyConflict {
                committed_fingerprint,
            } => CheckoutCommitOutcome::IdempotencyConflict {
                committed_fingerprint,
            },
        };
        Ok(FsReceipt {
            value: outcome,
            work,
        })
    }

    async fn resolve_clean_commit(
        &self,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<CheckoutCommitOutcome> {
        if let Some(last) = self.last_commit {
            if last.operation_id != operation_id {
                return Err(OperationFailure::before_work(FsError::NoPendingMutations));
            }
            if last.generation_id == GenerationId::new(self.generation_root.digest) {
                return Ok(FsReceipt {
                    value: CheckoutCommitOutcome::AlreadyCommitted {
                        generation_id: last.generation_id,
                        head: last.head,
                    },
                    work: WorkCounters::default(),
                });
            }
        }
        cancellation
            .check()
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        let head = self
            .authority_head
            .ok_or_else(|| OperationFailure::before_work(FsError::WritableCheckoutRequiresHead))?;
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
                OperationFailure::new(FsError::Authority(failure.error), *failure.work)
            })?;
        let Some(commit) = resolved.value else {
            return Err(OperationFailure::new(
                FsError::NoPendingMutations,
                resolved.work,
            ));
        };
        let generation_root = generation_from_record(&commit, self.volume.id, resolved.work)?;
        let value = if generation_root == self.generation_root {
            CheckoutCommitOutcome::AlreadyCommitted {
                generation_id: GenerationId::new(generation_root.digest),
                head,
            }
        } else {
            CheckoutCommitOutcome::IdempotencyConflict {
                committed_fingerprint: commit.fingerprint,
            }
        };
        Ok(FsReceipt {
            value,
            work: resolved.work,
        })
    }

    /// Resolves an exact path without following links or scanning unrelated subtrees.
    ///
    /// # Errors
    ///
    /// Returns a measured failure for invalid profile semantics, malformed
    /// authenticated routing, missing root records, non-directory traversal,
    /// cancellation, backend failure, or budget exhaustion.
    pub async fn lookup_no_follow(
        &mut self,
        path: &NamespacePath,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<PathLookup> {
        self.lookup_no_follow_observed(path, budget, cancellation, true)
            .await
    }

    /// Resolves one exact path and decodes its authenticated metadata without
    /// repeating the namespace or file-table traversal.
    ///
    /// # Errors
    ///
    /// Returns the lookup failures plus bounded object-read or metadata-decode
    /// failures. Authenticated absence performs no metadata read.
    pub async fn lookup_no_follow_with_metadata(
        &mut self,
        path: &NamespacePath,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Option<PathMetadataLookup>> {
        let lookup = self
            .lookup_no_follow_observed(path, budget, cancellation, false)
            .await?;
        let mut work = lookup.work;
        let Some(record) = lookup.value.record else {
            return Ok(FsReceipt { value: None, work });
        };
        let metadata = self
            .volume
            .fs
            .inner
            .objects
            .read(
                record.metadata,
                self.volume.config.limits.maximum_object_bytes,
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, FsError::Object))?;
        work = add(work, metadata.work)?;
        let metadata = decode_file_metadata(&metadata.value, decode_limits(self.volume.config))
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        if self.mode.consistency != ConsistencyMode::Pinned {
            work = self
                .observe_base_metadata_ids(
                    std::slice::from_ref(&record.file_id),
                    work,
                    budget,
                    cancellation,
                )
                .await?;
        }
        Ok(FsReceipt {
            value: Some(PathMetadataLookup { record, metadata }),
            work,
        })
    }

    async fn lookup_no_follow_observed(
        &mut self,
        path: &NamespacePath,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        capture_terminal: bool,
    ) -> FsResult<PathLookup> {
        let synchronized = self.synchronize_live(budget, cancellation).await?;
        let prior = synchronized.work;
        let mut lookup = Box::pin(self.lookup_no_follow_at_current(
            path,
            remaining(prior, budget)?,
            cancellation,
            capture_terminal,
        ))
        .await
        .map_err(|failure| failure.map_with_prior_work(prior, std::convert::identity))?;
        let mut total = add(prior, lookup.work)?;
        if lookup.value.record.is_none()
            && self.volume.config.case_sensitivity == CaseSensitivity::ProfileFolded
        {
            // Boxed: this whole fallback (and everything it transitively
            // calls, including a retried lookup) is a cold path that would
            // otherwise inline into every caller's future, including ones
            // that never touch a `ProfileFolded` volume.
            let fallback = Box::pin(self.resolve_case_fold_fallback(
                path,
                remaining(total, budget)?,
                cancellation,
                capture_terminal,
            ))
            .await
            .map_err(|failure| failure.map_with_prior_work(total, std::convert::identity))?;
            total = add(total, fallback.work)?;
            if let Some(resolved) = fallback.value {
                lookup.value = resolved;
            }
        }
        lookup.work = total;
        lookup.value.work = total;
        Ok(lookup)
    }

    /// Retries an exact-match lookup miss under `CaseSensitivity::ProfileFolded`
    /// by resolving `path`'s terminal component against its case-folded
    /// siblings. Returns `Ok(None)` when `path` is the root (no siblings to
    /// fold against) or no folded match exists, leaving the original
    /// exact-match result in place.
    async fn resolve_case_fold_fallback(
        &mut self,
        path: &NamespacePath,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        capture_terminal: bool,
    ) -> FsResult<Option<PathLookup>> {
        let mut work = WorkCounters::default();
        if path.is_root() {
            return Ok(FsReceipt { value: None, work });
        }
        let resolved = Box::pin(self.canonicalize_path_components(
            path.components(),
            &[],
            budget,
            cancellation,
        ))
        .await
        .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, resolved.work)?;
        let Some(corrected) = resolved.value else {
            return Ok(FsReceipt { value: None, work });
        };
        let retried = self
            .lookup_no_follow_at_current(
                &corrected,
                remaining(work, budget)?,
                cancellation,
                capture_terminal,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, retried.work)?;
        Ok(FsReceipt {
            value: Some(retried.value),
            work,
        })
    }

    async fn lookup_no_follow_at_current(
        &mut self,
        path: &NamespacePath,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        capture_terminal: bool,
    ) -> FsResult<PathLookup> {
        let lookup = if self.mode.consistency == ConsistencyMode::Pinned {
            crate::kernel::lookup_path_async(
                &self.volume.fs.inner.objects,
                &self.root,
                path,
                self.volume.config,
                budget,
                cancellation,
            )
            .await
            .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?
        } else {
            let candidate = if self.has_pending_mutations() {
                Some(
                    crate::kernel::lookup_path_async(
                        &self.volume.fs.inner.objects,
                        &self.root,
                        path,
                        self.volume.config,
                        budget,
                        cancellation,
                    )
                    .await
                    .map_err(|failure| {
                        OperationFailure::new(failure.error.into(), *failure.work)
                    })?,
                )
            } else {
                None
            };
            let prior = candidate
                .as_ref()
                .map_or(WorkCounters::default(), |value| value.work);
            let observed = if capture_terminal {
                crate::kernel::observe_path_async(
                    &self.volume.fs.inner.objects,
                    &self.base_root,
                    path,
                    self.volume.config,
                    remaining(prior, budget)?,
                    cancellation,
                )
                .await
            } else {
                crate::kernel::observe_path_edges_async(
                    &self.volume.fs.inner.objects,
                    &self.base_root,
                    path,
                    self.volume.config,
                    remaining(prior, budget)?,
                    cancellation,
                )
                .await
            }
            .map_err(|failure| failure.map_with_prior_work(prior, FsError::Path))?;
            let combined = add(prior, observed.lookup.work)?;
            self.dependencies
                .extend_observations(
                    observed.dependencies,
                    self.volume.config.limits.maximum_checkout_dependencies,
                )
                .map_err(|error| OperationFailure::new(error.into(), combined))?;
            candidate.map_or(
                PathLookup {
                    work: combined,
                    ..observed.lookup
                },
                |value| PathLookup {
                    work: combined,
                    ..value
                },
            )
        };
        Ok(FsReceipt {
            work: lookup.work,
            value: lookup,
        })
    }

    /// Resolves a bounded path batch while sharing directory and file-table frontiers.
    ///
    /// Input order and duplicates are preserved. The operation never follows
    /// links, reads file bodies, or scans unrelated namespace subtrees.
    ///
    /// # Errors
    ///
    /// Returns the same measured fail-closed outcomes as [`Self::lookup_no_follow`],
    /// plus explicit empty and excessive-batch rejection.
    pub async fn lookup_batch_no_follow(
        &mut self,
        paths: &[NamespacePath],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<PathBatchLookup> {
        let synchronized = self.synchronize_live(budget, cancellation).await?;
        let prior = synchronized.work;
        let mut lookup = crate::kernel::lookup_paths_async(
            &self.volume.fs.inner.objects,
            &self.root,
            paths,
            self.volume.config,
            remaining(prior, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(prior, FsError::Path))?;
        lookup.work = add(prior, lookup.work)?;
        if self.mode.consistency != ConsistencyMode::Pinned {
            let mut work = lookup.work;
            for path in paths {
                let observed = crate::kernel::observe_path_async(
                    &self.volume.fs.inner.objects,
                    &self.base_root,
                    path,
                    self.volume.config,
                    remaining(work, budget)?,
                    cancellation,
                )
                .await
                .map_err(|failure| failure.map_with_prior_work(work, FsError::Path))?;
                work = add(work, observed.lookup.work)?;
                self.dependencies
                    .extend_observations(
                        observed.dependencies,
                        self.volume.config.limits.maximum_checkout_dependencies,
                    )
                    .map_err(|error| OperationFailure::new(error.into(), work))?;
            }
            lookup.work = work;
        }
        Ok(FsReceipt {
            work: lookup.work,
            value: lookup,
        })
    }

    /// Returns one bounded authenticated page from a directory.
    ///
    /// Tracking checkouts retain the exact cursor interval as an observation,
    /// preventing an automatic rebase from crossing inserted or removed names.
    ///
    /// # Errors
    ///
    /// Returns measured path, kind, pagination, dependency, storage,
    /// cancellation, or bounded-work failures.
    pub async fn list_directory(
        &mut self,
        path: &NamespacePath,
        after: Option<&LogicalName>,
        maximum_entries: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<DirectoryPage> {
        let lookup = self
            .lookup_no_follow_observed(path, budget, cancellation, false)
            .await?;
        let mut work = lookup.work;
        let record = lookup
            .value
            .record
            .ok_or_else(|| OperationFailure::new(FsError::NotFound, work))?;
        let FilePayload::Directory { entries } = record.payload else {
            return Err(OperationFailure::new(FsError::NotDirectory, work));
        };
        if record.kind != FileKind::Directory {
            return Err(OperationFailure::new(FsError::NotDirectory, work));
        }
        let mut page = list_tree_entries_async(
            &self.volume.fs.inner.objects,
            entries,
            after,
            maximum_entries,
            decode_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::Directory))?;
        work = add(work, page.work)?;
        if self.mode.consistency != ConsistencyMode::Pinned {
            let region = DependencyRegion::DirectoryRange {
                directory_id: record.file_id,
                after: after.cloned(),
                maximum_entries,
            };
            let probe = AuthenticatedGenerationProbe::new(
                &self.volume.fs.inner.objects,
                probe_limits(self.volume.config),
            )
            .map_err(|error| OperationFailure::new(error.into(), work))?;
            let state = probe
                .probe_async(
                    GenerationId::new(self.base_generation_root.digest),
                    &region,
                    remaining(work, budget)?,
                    cancellation,
                )
                .await
                .map_err(|failure| failure.map_with_prior_work(work, FsError::Probe))?;
            work = add(work, state.work)?;
            self.dependencies
                .extend_observations(
                    vec![Dependency {
                        region,
                        expected: state.value,
                    }],
                    self.volume.config.limits.maximum_checkout_dependencies,
                )
                .map_err(|error| OperationFailure::new(error.into(), work))?;
        }
        page.work = work;
        Ok(FsReceipt { value: page, work })
    }

    /// Returns one directory page with all child records fetched in one shared
    /// authenticated file-table batch.
    ///
    /// This is the native-mount enumeration surface: it avoids one path lookup
    /// per child while preserving the same cursor observation as
    /// [`Self::list_directory`].
    ///
    /// # Errors
    ///
    /// Returns the listing failures plus authenticated file-table failures or
    /// a typed namespace/file-table consistency error.
    pub async fn list_directory_records(
        &mut self,
        path: &NamespacePath,
        after: Option<&LogicalName>,
        maximum_entries: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<DirectoryRecordPage> {
        let listing = self
            .list_directory(path, after, maximum_entries, budget, cancellation)
            .await?;
        let mut work = listing.work;
        let page = listing.value;
        if page.entries.is_empty() {
            return Ok(FsReceipt {
                value: DirectoryRecordPage {
                    entries: Vec::new(),
                    has_more: page.has_more,
                },
                work,
            });
        }
        let retained = u64::try_from(page.entries.len())
            .unwrap_or(u64::MAX)
            .saturating_mul(u64::try_from(size_of::<FileId>()).unwrap_or(u64::MAX));
        work = add(
            work,
            WorkCounters {
                allocation_operations: 1,
                peak_allocation_bytes: retained,
                ..WorkCounters::default()
            },
        )?;
        work.verify(budget)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        let mut file_ids = Vec::new();
        file_ids
            .try_reserve_exact(page.entries.len())
            .map_err(|_| OperationFailure::new(FsError::PendingMutationAllocationFailed, work))?;
        file_ids.extend(page.entries.iter().map(|entry| entry.file_id));
        let records = lookup_file_records_async(
            &self.volume.fs.inner.objects,
            self.root.file_table,
            &file_ids,
            maximum_entries,
            decode_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::FileRecord))?;
        work = add(work, records.work)?;
        drop(file_ids);
        let mut named_records = Vec::new();
        named_records
            .try_reserve_exact(page.entries.len())
            .map_err(|_| OperationFailure::new(FsError::PendingMutationAllocationFailed, work))?;
        for (binding, record) in page.entries.into_iter().zip(records.records) {
            let record = record
                .ok_or_else(|| OperationFailure::new(FsError::InvalidDirectoryRecord, work))?;
            if record.file_id != binding.file_id || record.kind != binding.kind {
                return Err(OperationFailure::new(FsError::InvalidDirectoryRecord, work));
            }
            named_records.push((binding.name, record));
        }
        self.attach_directory_metadata(named_records, page.has_more, work, budget, cancellation)
            .await
    }

    async fn attach_directory_metadata(
        &mut self,
        named_records: Vec<(LogicalName, FileRecord)>,
        has_more: bool,
        mut work: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<DirectoryRecordPage> {
        let mut requests = Vec::new();
        requests
            .try_reserve_exact(named_records.len())
            .map_err(|_| OperationFailure::new(FsError::PendingMutationAllocationFailed, work))?;
        requests.extend(named_records.iter().map(|(_, record)| ObjectReadRequest {
            object_id: record.metadata,
            maximum_bytes: self.volume.config.limits.maximum_object_bytes,
        }));
        let metadata_reads = self
            .volume
            .fs
            .inner
            .objects
            .read_many(&requests, remaining(work, budget)?, cancellation)
            .await
            .map_err(|failure| failure.map_with_prior_work(work, FsError::Object))?;
        work = add(work, metadata_reads.work)?;
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(named_records.len())
            .map_err(|_| OperationFailure::new(FsError::PendingMutationAllocationFailed, work))?;
        for ((name, record), metadata) in named_records.into_iter().zip(metadata_reads.value) {
            let metadata = decode_file_metadata(&metadata, decode_limits(self.volume.config))
                .map_err(|error| OperationFailure::new(error.into(), work))?;
            entries.push(DirectoryRecordEntry {
                name,
                record,
                metadata,
            });
        }
        if self.mode.consistency != ConsistencyMode::Pinned && !entries.is_empty() {
            let mut file_ids = Vec::new();
            file_ids.try_reserve_exact(entries.len()).map_err(|_| {
                OperationFailure::new(FsError::PendingMutationAllocationFailed, work)
            })?;
            file_ids.extend(entries.iter().map(|entry| entry.record.file_id));
            work = self
                .observe_base_metadata_ids(&file_ids, work, budget, cancellation)
                .await?;
        }
        Ok(FsReceipt {
            value: DirectoryRecordPage { entries, has_more },
            work,
        })
    }

    /// Reads complete canonical metadata for one exact path.
    ///
    /// Tracking checkouts retain the metadata identity independently from file
    /// content, allowing unrelated content changes to rebase safely.
    ///
    /// # Errors
    ///
    /// Returns measured absence, path, decode, dependency, storage,
    /// cancellation, or bounded-work failures.
    pub async fn read_metadata(
        &mut self,
        path: &NamespacePath,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<FileMetadata> {
        let lookup = self
            .lookup_no_follow_with_metadata(path, budget, cancellation)
            .await?;
        let value = lookup
            .value
            .ok_or_else(|| OperationFailure::new(FsError::NotFound, lookup.work))?
            .metadata;
        Ok(FsReceipt {
            value,
            work: lookup.work,
        })
    }

    /// Reads one exact named attribute without reading unrelated attribute
    /// payloads or pages.
    ///
    /// # Errors
    ///
    /// Returns measured path, metadata, attribute-tree, blob, cancellation,
    /// storage, allocation, or bounded-work failures.
    pub async fn read_named_attribute(
        &mut self,
        path: &NamespacePath,
        name: &AttributeName,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Option<Bytes>> {
        let metadata = self.read_metadata(path, budget, cancellation).await?;
        let mut work = metadata.work;
        let MetadataField::Value(root) = metadata.value.named_attributes else {
            return Ok(FsReceipt { value: None, work });
        };
        let lookup = lookup_attribute_async(
            &self.volume.fs.inner.objects,
            root,
            name,
            decode_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::AttributeLookup))?;
        work = add(work, lookup.work)?;
        let Some(entry) = lookup.entry else {
            return Ok(FsReceipt { value: None, work });
        };
        if entry.value_bytes > self.volume.config.limits.maximum_read_bytes {
            return Err(OperationFailure::new(
                FsError::FileRead(FileRangeReadError::InvalidRange),
                work,
            ));
        }
        let read = read_blob_range_async(
            &self.volume.fs.inner.objects,
            entry.value,
            ByteRange {
                offset: 0,
                length: entry.value_bytes,
            },
            decode_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::BlobRead))?;
        work = add(work, read.work)?;
        Ok(FsReceipt {
            value: Some(read.bytes),
            work,
        })
    }

    /// Lists one bounded page of exact named-attribute identities without
    /// reading any attribute payload bytes.
    ///
    /// # Errors
    ///
    /// Returns measured path, metadata, pagination, cancellation, storage,
    /// allocation, or bounded-work failures.
    pub async fn list_named_attributes(
        &mut self,
        path: &NamespacePath,
        after: Option<&AttributeName>,
        maximum_entries: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<AttributeListing> {
        let metadata = self.read_metadata(path, budget, cancellation).await?;
        let mut work = metadata.work;
        let MetadataField::Value(root) = metadata.value.named_attributes else {
            return Ok(FsReceipt {
                value: AttributeListing {
                    entries: Vec::new(),
                    has_more: false,
                    next_residency: None,
                    work,
                },
                work,
            });
        };
        let mut listing = list_attributes_async(
            &self.volume.fs.inner.objects,
            root,
            after,
            maximum_entries,
            decode_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::AttributeList))?;
        work = add(work, listing.work)?;
        listing.work = work;
        Ok(FsReceipt {
            value: listing,
            work,
        })
    }

    /// Inserts or replaces one named attribute and atomically updates the
    /// path's metadata reference in the checkout candidate.
    ///
    /// Immutable payload and attribute pages are staged first; a rejected
    /// precondition can leave only unreachable authenticated objects.
    ///
    /// # Errors
    ///
    /// Returns measured precondition, path, blob, attribute-tree, metadata,
    /// mutation, cancellation, storage, allocation, or bounded-work failures.
    pub async fn write_named_attribute(
        &mut self,
        path: NamespacePath,
        name: AttributeName,
        value: Bytes,
        mode: NamedAttributeWriteMode,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        let metadata = self.read_metadata(&path, budget, cancellation).await?;
        let mut work = metadata.work;
        let mut metadata_value = metadata.value;
        let root = match metadata_value.named_attributes {
            MetadataField::Unavailable => {
                let empty = self
                    .stage_empty_attributes(remaining(work, budget)?, cancellation)
                    .await
                    .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
                work = add(work, empty.work)?;
                empty.value
            }
            MetadataField::Value(root) => root,
        };
        let current = lookup_attribute_async(
            &self.volume.fs.inner.objects,
            root,
            &name,
            decode_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::AttributeLookup))?;
        work = add(work, current.work)?;
        match (mode, current.entry.as_ref()) {
            (NamedAttributeWriteMode::Create, Some(_))
            | (NamedAttributeWriteMode::Replace, None) => {
                return Err(OperationFailure::new(FsError::CreationRejected, work));
            }
            _ => {}
        }
        let blob = self
            .stage_blob(value, remaining(work, budget)?, cancellation)
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, blob.work)?;
        let replacement = AttributeEntry {
            name,
            value_bytes: blob.value.logical_bytes,
            value: blob.value.root,
        };
        let mutation = match current.entry {
            None => AttributeMutation::Insert(replacement),
            Some(expected) => AttributeMutation::Replace {
                expected,
                replacement,
            },
        };
        let changed = apply_attribute_mutations_async(
            &self.volume.fs.inner.objects,
            root,
            vec![mutation],
            1,
            decode_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::AttributeMutation))?;
        work = add(work, changed.work)?;
        metadata_value.named_attributes = MetadataField::Value(changed.root);
        let changed_metadata = self
            .set_metadata(path, metadata_value, remaining(work, budget)?, cancellation)
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, changed_metadata.work)?;
        Ok(FsReceipt { value: (), work })
    }

    /// Removes one existing named attribute and atomically updates metadata.
    ///
    /// # Errors
    ///
    /// Returns [`FsError::NotFound`] for authenticated absence and otherwise
    /// the same bounded failures as [`Self::write_named_attribute`].
    pub async fn remove_named_attribute(
        &mut self,
        path: NamespacePath,
        name: AttributeName,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        let metadata = self.read_metadata(&path, budget, cancellation).await?;
        let mut work = metadata.work;
        let mut metadata_value = metadata.value;
        let MetadataField::Value(root) = metadata_value.named_attributes else {
            return Err(OperationFailure::new(FsError::NotFound, work));
        };
        let current = lookup_attribute_async(
            &self.volume.fs.inner.objects,
            root,
            &name,
            decode_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::AttributeLookup))?;
        work = add(work, current.work)?;
        let expected = current
            .entry
            .ok_or_else(|| OperationFailure::new(FsError::NotFound, work))?;
        let changed = apply_attribute_mutations_async(
            &self.volume.fs.inner.objects,
            root,
            vec![AttributeMutation::Remove {
                name,
                expected: Some(expected),
            }],
            1,
            decode_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::AttributeMutation))?;
        work = add(work, changed.work)?;
        metadata_value.named_attributes = MetadataField::Value(changed.root);
        let changed_metadata = self
            .set_metadata(path, metadata_value, remaining(work, budget)?, cancellation)
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, changed_metadata.work)?;
        Ok(FsReceipt { value: (), work })
    }

    /// Plans the sparse physical representation of one exact regular-file range.
    ///
    /// `None` identifies tiny inline content; callers should use
    /// [`Self::read_file_range`] for that bounded body. A returned plan contains
    /// holes, allocated-zero spans, and immutable-content spans without reading
    /// content blobs or unrelated extent pages.
    ///
    /// # Errors
    ///
    /// Returns measured path, kind, range, authentication, dependency,
    /// cancellation, storage, allocation, excessive-span, or bounded-work
    /// failures.
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
        let lookup = self
            .lookup_no_follow_observed(path, budget, cancellation, false)
            .await?;
        let mut work = lookup.work;
        let record = lookup
            .value
            .record
            .ok_or_else(|| OperationFailure::new(FsError::NotFound, work))?;
        let (logical_bytes, extents) = match record.payload {
            FilePayload::InlineRegular(data) => {
                let logical_bytes = u64::try_from(data.as_bytes().len()).unwrap_or(u64::MAX);
                validate_planned_file_range(logical_bytes, range, work)?;
                if self.mode.consistency != ConsistencyMode::Pinned && range.length != 0 {
                    work = self
                        .observe_base_regular_range(
                            record.file_id,
                            range,
                            work,
                            budget,
                            cancellation,
                        )
                        .await?;
                }
                return Ok(FsReceipt { value: None, work });
            }
            FilePayload::Regular {
                logical_bytes,
                extents,
            } => (logical_bytes, extents),
            FilePayload::Directory { .. }
            | FilePayload::SymbolicLink { .. }
            | FilePayload::Device { .. }
            | FilePayload::Empty
            | FilePayload::ReparsePoint { .. } => {
                return Err(OperationFailure::new(
                    FsError::FileRead(FileRangeReadError::NotRegular),
                    work,
                ));
            }
        };
        let mut plan = plan_extent_range_async(
            &self.volume.fs.inner.objects,
            ExtentRangeRequest {
                root: extents,
                file_size: logical_bytes,
                range,
                maximum_spans,
                limits: decode_limits(self.volume.config),
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
        if self.mode.consistency != ConsistencyMode::Pinned && range.length != 0 {
            work = self
                .observe_base_regular_range(record.file_id, range, work, budget, cancellation)
                .await?;
        }
        plan.work = work;
        Ok(FsReceipt {
            value: Some(plan),
            work,
        })
    }

    /// Finds the first sparse data or hole boundary after one logical offset.
    ///
    /// Namespace and extent roots are resolved once. The extent traversal then
    /// advances monotonically across authenticated leaf frontiers without
    /// reading file bodies or restarting at the root.
    ///
    /// # Errors
    ///
    /// Returns measured absence, non-regular kind, invalid offset, malformed
    /// extent graph, cancellation, backend, dependency, or bounded-work
    /// failures.
    pub async fn seek_file_extent(
        &mut self,
        path: &NamespacePath,
        offset: u64,
        target: ExtentSeekTarget,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Option<u64>> {
        let lookup = self
            .lookup_no_follow_observed(path, budget, cancellation, false)
            .await?;
        let mut work = lookup.work;
        let record = lookup
            .value
            .record
            .ok_or_else(|| OperationFailure::new(FsError::NotFound, work))?;
        let value = match record.payload {
            FilePayload::InlineRegular(data) => {
                let logical_bytes = u64::try_from(data.as_bytes().len()).unwrap_or(u64::MAX);
                if offset > logical_bytes {
                    None
                } else {
                    match target {
                        ExtentSeekTarget::Data => (offset < logical_bytes).then_some(offset),
                        ExtentSeekTarget::Hole => Some(logical_bytes),
                    }
                }
            }
            FilePayload::Regular {
                logical_bytes,
                extents,
            } => {
                if offset > logical_bytes {
                    None
                } else {
                    let receipt = seek_extent_async(
                        &self.volume.fs.inner.objects,
                        ExtentSeekRequest {
                            root: extents,
                            file_size: logical_bytes,
                            offset,
                            target,
                            limits: decode_limits(self.volume.config),
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
                    work = add(work, receipt.work)?;
                    receipt.value
                }
            }
            FilePayload::Directory { .. }
            | FilePayload::SymbolicLink { .. }
            | FilePayload::Empty
            | FilePayload::Device { .. }
            | FilePayload::ReparsePoint { .. } => {
                return Err(OperationFailure::new(
                    FsError::FileRead(FileRangeReadError::NotRegular),
                    work,
                ));
            }
        };
        if self.mode.consistency != ConsistencyMode::Pinned {
            let region = DependencyRegion::SparseSeek {
                file_id: record.file_id,
                offset,
                target,
            };
            let probe = AuthenticatedGenerationProbe::new(
                &self.volume.fs.inner.objects,
                probe_limits(self.volume.config),
            )
            .map_err(|error| OperationFailure::new(error.into(), work))?;
            let state = probe
                .probe_async(
                    GenerationId::new(self.base_generation_root.digest),
                    &region,
                    remaining(work, budget)?,
                    cancellation,
                )
                .await
                .map_err(|failure| failure.map_with_prior_work(work, FsError::Probe))?;
            work = add(work, state.work)?;
            self.dependencies
                .extend_observations(
                    vec![Dependency {
                        region,
                        expected: state.value,
                    }],
                    self.volume.config.limits.maximum_checkout_dependencies,
                )
                .map_err(|error| OperationFailure::new(error.into(), work))?;
        }
        Ok(FsReceipt { value, work })
    }

    /// Reads one exact logical regular-file range without materializing holes
    /// or unrelated extent and blob subtrees.
    ///
    /// # Errors
    ///
    /// Returns measured path, kind, range, authentication, dependency,
    /// cancellation, storage, allocation, or bounded-work failures.
    pub async fn read_file_range(
        &mut self,
        path: &NamespacePath,
        range: ByteRange,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<FileRangeRead> {
        if range.length > self.volume.config.limits.maximum_read_bytes {
            return Err(OperationFailure::before_work(FsError::FileRead(
                FileRangeReadError::InvalidRange,
            )));
        }
        let lookup = self
            .lookup_no_follow_observed(path, budget, cancellation, false)
            .await?;
        let mut work = lookup.work;
        let record = lookup
            .value
            .record
            .ok_or_else(|| OperationFailure::new(FsError::NotFound, work))?;
        let mut read = read_file_range_async(
            &self.volume.fs.inner.objects,
            FileRangeRequest {
                record,
                range,
                maximum_spans: self.volume.config.limits.maximum_directory_page_entries,
                limits: decode_limits(self.volume.config),
                budget: remaining(work, budget)?,
            },
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::FileRead))?;
        work = add(work, read.work)?;
        if self.mode.consistency != ConsistencyMode::Pinned && range.length != 0 {
            work = self
                .observe_base_regular_range(record.file_id, range, work, budget, cancellation)
                .await?;
        }
        read.work = work;
        Ok(FsReceipt { value: read, work })
    }
    async fn lookup_file_record_by_id(
        &self,
        file_id: FileId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Option<FileRecord>> {
        let lookup = lookup_file_record_async(
            &self.volume.fs.inner.objects,
            self.root.file_table,
            file_id,
            decode_limits(self.volume.config),
            budget,
            cancellation,
        )
        .await
        .map_err(|failure| {
            OperationFailure::new(FsError::FileRecord(failure.error), *failure.work)
        })?;
        Ok(FsReceipt {
            value: lookup.record,
            work: lookup.work,
        })
    }

    async fn observe_identity_region(
        &mut self,
        region: DependencyRegion,
        work: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<WorkCounters, OperationFailure<FsError>> {
        self.observe_identity_regions(vec![region], work, budget, cancellation)
            .await
    }

    async fn observe_identity_regions(
        &mut self,
        regions: Vec<DependencyRegion>,
        mut work: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<WorkCounters, OperationFailure<FsError>> {
        let captured = capture_dependencies_async(
            &self.volume.fs.inner.objects,
            &self.base_generation_root,
            self.volume.config,
            regions,
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, captured.work)?;
        self.dependencies
            .extend_observations(
                captured.value,
                self.volume.config.limits.maximum_checkout_dependencies,
            )
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        Ok(work)
    }

    async fn observe_base_regular_range(
        &mut self,
        file_id: FileId,
        range: ByteRange,
        mut work: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<WorkCounters, OperationFailure<FsError>> {
        let lookup = lookup_file_record_async(
            &self.volume.fs.inner.objects,
            self.base_root.file_table,
            file_id,
            decode_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::FileRecord))?;
        work = add(work, lookup.work)?;
        let regions = regular_range_regions(lookup.record, range.offset, range.length)
            .into_iter()
            .flatten()
            .collect();
        self.observe_identity_regions(regions, work, budget, cancellation)
            .await
    }

    async fn observe_base_metadata_ids(
        &mut self,
        file_ids: &[FileId],
        mut work: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<WorkCounters, OperationFailure<FsError>> {
        if file_ids.is_empty() {
            return Ok(work);
        }
        let maximum = u32::try_from(file_ids.len()).unwrap_or(u32::MAX);
        let records = lookup_file_records_async(
            &self.volume.fs.inner.objects,
            self.base_root.file_table,
            file_ids,
            maximum,
            decode_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::FileRecord))?;
        work = add(work, records.work)?;
        let mut dependencies = Vec::new();
        dependencies
            .try_reserve_exact(file_ids.len())
            .map_err(|_| OperationFailure::new(FsError::PendingMutationAllocationFailed, work))?;
        dependencies.extend(
            file_ids
                .iter()
                .zip(records.records)
                .map(|(file_id, record)| Dependency {
                    region: DependencyRegion::Metadata(*file_id),
                    expected: record.map_or(DependencyState::Absent, |record| {
                        DependencyState::Present(record.metadata.digest)
                    }),
                }),
        );
        self.dependencies
            .extend_observations(
                dependencies,
                self.volume.config.limits.maximum_checkout_dependencies,
            )
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        Ok(work)
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> DetachedFile<A, O> {
    /// Stable file identity retained across last-link removal.
    #[must_use]
    pub const fn file_id(&self) -> FileId {
        self.record.file_id
    }

    /// Current detached logical file length.
    #[must_use]
    pub fn logical_bytes(&self) -> u64 {
        match self.record.payload {
            FilePayload::InlineRegular(data) => {
                u64::try_from(data.as_bytes().len()).unwrap_or(u64::MAX)
            }
            FilePayload::Regular { logical_bytes, .. } => logical_bytes,
            _ => 0,
        }
    }

    /// Finds the first sparse data or hole boundary without a namespace binding.
    ///
    /// # Errors
    ///
    /// Returns measured malformed-extent, cancellation, backend, allocation,
    /// or bounded-work failures.
    pub async fn seek(
        &self,
        offset: u64,
        target: ExtentSeekTarget,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Option<u64>> {
        let value = match self.record.payload {
            FilePayload::InlineRegular(data) => {
                let logical_bytes = u64::try_from(data.as_bytes().len()).unwrap_or(u64::MAX);
                if offset > logical_bytes {
                    None
                } else {
                    match target {
                        ExtentSeekTarget::Data => (offset < logical_bytes).then_some(offset),
                        ExtentSeekTarget::Hole => Some(logical_bytes),
                    }
                }
            }
            FilePayload::Regular {
                logical_bytes,
                extents,
            } => {
                if offset > logical_bytes {
                    None
                } else {
                    return seek_extent_async(
                        &self.volume.fs.inner.objects,
                        ExtentSeekRequest {
                            root: extents,
                            file_size: logical_bytes,
                            offset,
                            target,
                            limits: decode_limits(self.volume.config),
                            budget,
                        },
                        cancellation,
                    )
                    .await
                    .map_err(|failure| {
                        OperationFailure::new(
                            FsError::FileRead(FileRangeReadError::Extent(failure.error)),
                            *failure.work,
                        )
                    });
                }
            }
            FilePayload::Directory { .. }
            | FilePayload::SymbolicLink { .. }
            | FilePayload::Empty
            | FilePayload::Device { .. }
            | FilePayload::ReparsePoint { .. } => {
                return Err(OperationFailure::before_work(FsError::FileRead(
                    FileRangeReadError::NotRegular,
                )));
            }
        };
        Ok(FsReceipt {
            value,
            work: WorkCounters::default(),
        })
    }

    /// Replaces complete metadata on this detached file only.
    ///
    /// # Errors
    ///
    /// Returns measured encoding, storage, cancellation, allocation, or
    /// bounded-work failures. A failed operation leaves the detached record
    /// unchanged.
    pub async fn set_metadata(
        &mut self,
        metadata: FileMetadata,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        let encoded = encode_file_metadata(metadata)
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        let (metadata, work) = self
            .volume
            .fs
            .put_encoded(
                ObjectKind::Metadata,
                encoded,
                WorkCounters::default(),
                budget,
                cancellation,
            )
            .await?;
        self.record.metadata = metadata;
        Ok(FsReceipt { value: (), work })
    }

    /// Replaces complete metadata and optionally logical size as one detached
    /// record transition.
    ///
    /// Immutable metadata bytes may be staged before validation finishes, but
    /// the visible detached record changes only after every requested update
    /// succeeds.
    ///
    /// # Errors
    ///
    /// Returns measured encoding, storage, sparse mutation, cancellation,
    /// allocation, or bounded-work failures.
    pub async fn set_attributes(
        &mut self,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        let encoded = encode_file_metadata(metadata)
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        let (metadata_id, mut work) = self
            .volume
            .fs
            .put_encoded(
                ObjectKind::Metadata,
                encoded,
                WorkCounters::default(),
                budget,
                cancellation,
            )
            .await?;
        let payload = if let Some(logical_bytes) = logical_bytes {
            let resized = apply_regular_mutation_async(
                &self.volume.fs.inner.objects,
                self.record.payload,
                RegularMutation::Resize { logical_bytes },
                self.volume.config,
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, FsError::DetachedMutation))?;
            work = add(work, resized.work)?;
            resized.payload
        } else {
            self.record.payload
        };
        self.record.metadata = metadata_id;
        self.record.payload = payload;
        Ok(FsReceipt { value: (), work })
    }

    /// Reads one exact named attribute from the detached metadata frontier.
    ///
    /// # Errors
    ///
    /// Returns measured metadata, attribute-tree, blob, storage,
    /// authentication, cancellation, allocation, or bounded-work failures.
    pub async fn read_named_attribute(
        &self,
        name: &AttributeName,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<Option<Bytes>> {
        let metadata = self.read_metadata(budget, cancellation).await?;
        let mut work = metadata.work;
        let MetadataField::Value(root) = metadata.value.named_attributes else {
            return Ok(FsReceipt { value: None, work });
        };
        let lookup = lookup_attribute_async(
            &self.volume.fs.inner.objects,
            root,
            name,
            decode_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::AttributeLookup))?;
        work = add(work, lookup.work)?;
        let Some(entry) = lookup.entry else {
            return Ok(FsReceipt { value: None, work });
        };
        if entry.value_bytes > self.volume.config.limits.maximum_read_bytes {
            return Err(OperationFailure::new(
                FsError::FileRead(FileRangeReadError::InvalidRange),
                work,
            ));
        }
        let read = read_blob_range_async(
            &self.volume.fs.inner.objects,
            entry.value,
            ByteRange {
                offset: 0,
                length: entry.value_bytes,
            },
            decode_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::BlobRead))?;
        work = add(work, read.work)?;
        Ok(FsReceipt {
            value: Some(read.bytes),
            work,
        })
    }

    /// Lists one bounded page of detached named-attribute identities.
    ///
    /// # Errors
    ///
    /// Returns measured metadata, pagination, storage, authentication,
    /// cancellation, allocation, or bounded-work failures.
    pub async fn list_named_attributes(
        &self,
        after: Option<&AttributeName>,
        maximum_entries: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<AttributeListing> {
        let metadata = self.read_metadata(budget, cancellation).await?;
        let mut work = metadata.work;
        let MetadataField::Value(root) = metadata.value.named_attributes else {
            let value = AttributeListing {
                entries: Vec::new(),
                has_more: false,
                next_residency: None,
                work,
            };
            return Ok(FsReceipt { value, work });
        };
        let mut listing = list_attributes_async(
            &self.volume.fs.inner.objects,
            root,
            after,
            maximum_entries,
            decode_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::AttributeList))?;
        work = add(work, listing.work)?;
        listing.work = work;
        Ok(FsReceipt {
            value: listing,
            work,
        })
    }

    /// Inserts or replaces one detached named attribute atomically.
    ///
    /// # Errors
    ///
    /// Returns measured precondition, metadata, blob, attribute-tree, storage,
    /// cancellation, allocation, or bounded-work failures.
    pub async fn write_named_attribute(
        &mut self,
        name: AttributeName,
        value: Bytes,
        mode: NamedAttributeWriteMode,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<FileMetadata> {
        let metadata = self.read_metadata(budget, cancellation).await?;
        let mut work = metadata.work;
        let mut metadata_value = metadata.value;
        let root = match metadata_value.named_attributes {
            MetadataField::Unavailable => {
                let encoded = encode_attribute_page(
                    &AttributePage::Leaf(Vec::new()),
                    self.volume.config.limits.maximum_directory_page_entries,
                )
                .map_err(|error| OperationFailure::new(error.into(), work))?;
                let stored = self
                    .volume
                    .fs
                    .put_encoded(
                        ObjectKind::AttributePage,
                        encoded,
                        work,
                        budget,
                        cancellation,
                    )
                    .await?;
                work = stored.1;
                stored.0
            }
            MetadataField::Value(root) => root,
        };
        let current = lookup_attribute_async(
            &self.volume.fs.inner.objects,
            root,
            &name,
            decode_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::AttributeLookup))?;
        work = add(work, current.work)?;
        match (mode, current.entry.as_ref()) {
            (NamedAttributeWriteMode::Create, Some(_))
            | (NamedAttributeWriteMode::Replace, None) => {
                return Err(OperationFailure::new(FsError::CreationRejected, work));
            }
            _ => {}
        }
        let blob = self
            .stage_blob(value, remaining(work, budget)?, cancellation)
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, blob.work)?;
        let replacement = AttributeEntry {
            name,
            value_bytes: blob.value.logical_bytes,
            value: blob.value.root,
        };
        let mutation = match current.entry {
            None => AttributeMutation::Insert(replacement),
            Some(expected) => AttributeMutation::Replace {
                expected,
                replacement,
            },
        };
        let changed = apply_attribute_mutations_async(
            &self.volume.fs.inner.objects,
            root,
            vec![mutation],
            1,
            decode_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::AttributeMutation))?;
        work = add(work, changed.work)?;
        metadata_value.named_attributes = MetadataField::Value(changed.root);
        let metadata = self
            .set_metadata(metadata_value, remaining(work, budget)?, cancellation)
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, metadata.work)?;
        Ok(FsReceipt {
            value: metadata_value,
            work,
        })
    }

    /// Removes one existing detached named attribute atomically.
    ///
    /// # Errors
    ///
    /// Returns authenticated absence or the same measured failures as
    /// [`Self::write_named_attribute`].
    pub async fn remove_named_attribute(
        &mut self,
        name: AttributeName,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<FileMetadata> {
        let metadata = self.read_metadata(budget, cancellation).await?;
        let mut work = metadata.work;
        let mut metadata_value = metadata.value;
        let MetadataField::Value(root) = metadata_value.named_attributes else {
            return Err(OperationFailure::new(FsError::NotFound, work));
        };
        let current = lookup_attribute_async(
            &self.volume.fs.inner.objects,
            root,
            &name,
            decode_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::AttributeLookup))?;
        work = add(work, current.work)?;
        let expected = current
            .entry
            .ok_or_else(|| OperationFailure::new(FsError::NotFound, work))?;
        let changed = apply_attribute_mutations_async(
            &self.volume.fs.inner.objects,
            root,
            vec![AttributeMutation::Remove {
                name,
                expected: Some(expected),
            }],
            1,
            decode_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, FsError::AttributeMutation))?;
        work = add(work, changed.work)?;
        metadata_value.named_attributes = MetadataField::Value(changed.root);
        let metadata = self
            .set_metadata(metadata_value, remaining(work, budget)?, cancellation)
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, metadata.work)?;
        Ok(FsReceipt {
            value: metadata_value,
            work,
        })
    }

    /// Reads complete authenticated metadata for this detached file.
    ///
    /// # Errors
    ///
    /// Returns measured storage, authentication, cancellation, allocation, or
    /// bounded-work failures.
    pub async fn read_metadata(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<FileMetadata> {
        let metadata = self
            .volume
            .fs
            .inner
            .objects
            .read(
                self.record.metadata,
                self.volume.config.limits.maximum_object_bytes,
                budget,
                cancellation,
            )
            .await
            .map_err(|failure| {
                OperationFailure::new(FsError::Object(failure.error), *failure.work)
            })?;
        let value = decode_file_metadata(&metadata.value, decode_limits(self.volume.config))
            .map_err(|error| OperationFailure::new(error.into(), metadata.work))?;
        Ok(FsReceipt {
            value,
            work: metadata.work,
        })
    }

    async fn stage_blob(
        &self,
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<crate::kernel::BlobBuild> {
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX)
            > self.volume.config.limits.maximum_read_bytes
        {
            return Err(OperationFailure::before_work(FsError::FileRead(
                FileRangeReadError::InvalidRange,
            )));
        }
        let maximum_blob_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX).max(1);
        let mut source = std::io::Cursor::new(bytes);
        let blob = build_blob_async(
            &self.volume.fs.inner.objects,
            &mut source,
            BlobBuildOptions {
                chunk_bytes: u32::try_from(
                    self.volume
                        .config
                        .limits
                        .maximum_object_bytes
                        .min(1024 * 1024),
                )
                .unwrap_or(u32::MAX)
                .max(1),
                page_items: self.volume.config.limits.maximum_directory_page_entries,
                page_bytes: u32::try_from(self.volume.config.limits.maximum_object_bytes)
                    .unwrap_or(u32::MAX)
                    .max(1),
                maximum_blob_bytes,
            },
            budget,
            cancellation,
        )
        .await
        .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
        Ok(FsReceipt {
            work: blob.work,
            value: blob,
        })
    }

    /// Reads one bounded sparse range without requiring a namespace binding.
    ///
    /// # Errors
    ///
    /// Returns typed range, storage, authentication, cancellation, allocation,
    /// or bounded-work failures.
    pub async fn read_range(
        &self,
        range: ByteRange,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<FileRangeRead> {
        if range.length > self.volume.config.limits.maximum_read_bytes {
            return Err(OperationFailure::before_work(FsError::FileRead(
                FileRangeReadError::InvalidRange,
            )));
        }
        let read = read_file_range_async(
            &self.volume.fs.inner.objects,
            FileRangeRequest {
                record: self.record,
                range,
                maximum_spans: self.volume.config.limits.maximum_mutations_per_batch,
                limits: decode_limits(self.volume.config),
                budget,
            },
            cancellation,
        )
        .await
        .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
        Ok(FsReceipt {
            work: read.work,
            value: read,
        })
    }

    /// Writes caller bytes into the detached sparse file.
    ///
    /// # Errors
    ///
    /// Returns typed range, blob, mutation, storage, cancellation, allocation,
    /// or bounded-work failures.
    pub async fn write_range(
        &mut self,
        offset: u64,
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        if bytes.is_empty() {
            return Ok(FsReceipt {
                value: (),
                work: WorkCounters::default(),
            });
        }
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX)
            > self.volume.config.limits.maximum_read_bytes
        {
            return Err(OperationFailure::before_work(FsError::FileRead(
                FileRangeReadError::InvalidRange,
            )));
        }
        let maximum_blob_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        let mut source = std::io::Cursor::new(bytes);
        let blob = build_blob_async(
            &self.volume.fs.inner.objects,
            &mut source,
            BlobBuildOptions {
                chunk_bytes: u32::try_from(
                    self.volume
                        .config
                        .limits
                        .maximum_object_bytes
                        .min(1024 * 1024),
                )
                .unwrap_or(u32::MAX)
                .max(1),
                page_items: self.volume.config.limits.maximum_directory_page_entries,
                page_bytes: u32::try_from(self.volume.config.limits.maximum_object_bytes)
                    .unwrap_or(u32::MAX)
                    .max(1),
                maximum_blob_bytes,
            },
            budget,
            cancellation,
        )
        .await
        .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
        let mut work = blob.work;
        let mutation = self
            .mutate_regular(
                RegularMutation::Write {
                    offset,
                    length: blob.logical_bytes,
                    content: blob.root,
                    content_offset: 0,
                },
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, mutation.work)?;
        Ok(FsReceipt { value: (), work })
    }

    /// Changes detached logical file length.
    ///
    /// # Errors
    ///
    /// Returns typed mutation, storage, cancellation, allocation, or work failures.
    pub async fn resize(
        &mut self,
        logical_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        self.mutate_regular(
            RegularMutation::Resize { logical_bytes },
            budget,
            cancellation,
        )
        .await
    }

    /// Replaces one detached range with a hole or allocated zeros.
    ///
    /// # Errors
    ///
    /// Returns typed mutation, storage, cancellation, allocation, or work failures.
    pub async fn zero_range(
        &mut self,
        range: ByteRange,
        allocated: bool,
        extend: bool,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        self.mutate_regular(
            RegularMutation::ZeroRange {
                offset: range.offset,
                length: range.length,
                allocated,
                extend,
            },
            budget,
            cancellation,
        )
        .await
    }

    /// Allocates detached sparse holes while preserving content.
    ///
    /// # Errors
    ///
    /// Returns typed unsupported keep-size, mutation, storage, cancellation,
    /// allocation, or work failures.
    pub async fn preallocate(
        &mut self,
        range: ByteRange,
        keep_size: bool,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        self.mutate_regular(
            RegularMutation::Preallocate {
                offset: range.offset,
                length: range.length,
                keep_size,
            },
            budget,
            cancellation,
        )
        .await
    }

    async fn mutate_regular(
        &mut self,
        mutation: RegularMutation,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        let receipt = apply_regular_mutation_async(
            &self.volume.fs.inner.objects,
            self.record.payload,
            mutation,
            self.volume.config,
            budget,
            cancellation,
        )
        .await
        .map_err(|failure| {
            OperationFailure::new(FsError::DetachedMutation(failure.error), *failure.work)
        })?;
        self.record.payload = receipt.payload;
        Ok(FsReceipt {
            value: (),
            work: receipt.work,
        })
    }
}

pub(crate) async fn read_generation_root<O: AsyncObjectStore>(
    objects: &O,
    generation_root: ObjectId,
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(GenerationRoot, WorkCounters), OperationFailure<FsError>> {
    cancellation
        .check()
        .map_err(|error| OperationFailure::before_work(error.into()))?;
    let receipt = objects
        .read(
            generation_root,
            MAXIMUM_GENERATION_ROOT_BYTES.min(config.limits.maximum_object_bytes),
            budget,
            cancellation,
        )
        .await
        .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
    let retained_bytes = match receipt.value.retention {
        ObjectReadRetention::Shared => 0,
        ObjectReadRetention::Owned { logical_bytes } => logical_bytes,
    };
    let decode = decode_limits(config);
    let parent_count = generation_root_parent_count(&receipt.value, decode)
        .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
    let parent_bytes = parent_count
        .checked_mul(size_of::<GenerationId>())
        .map(crate::foundation::usize_to_u64)
        .ok_or_else(|| OperationFailure::new(FsError::Work(WorkError::Overflow), receipt.work))?;
    let simultaneous = retained_bytes
        .checked_add(parent_bytes)
        .ok_or_else(|| OperationFailure::new(FsError::Work(WorkError::Overflow), receipt.work))?;
    let mut work = receipt
        .work
        .checked_add(WorkCounters {
            allocation_operations: u64::from(parent_count != 0),
            bytes_copied: parent_bytes,
            ..WorkCounters::default()
        })
        .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
    work.peak_allocation_bytes = work.peak_allocation_bytes.max(simultaneous);
    work.verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
    let root = crate::kernel::decode_generation_root(&receipt.value, decode)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    Ok((root, work))
}

fn exact_mutation_regions(
    mutation: &Mutation,
    first: Option<FileRecord>,
    second: Option<FileRecord>,
) -> [Option<(Option<FileRecord>, DependencyRegion)>; 4] {
    let mut regions = [None, None, None, None];
    match mutation {
        Mutation::Create { .. } | Mutation::Link { .. } => {}
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
            // authenticated extent-summary region exists, the complete record
            // is the only bounded proof that cannot miss delete-vs-modify.
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
            FileMutation::ValidateRegular | FileMutation::Resize { .. } => {
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
    work: WorkCounters,
) -> Result<(), OperationFailure<FsError>> {
    let end = range.offset.checked_add(range.length).ok_or_else(|| {
        OperationFailure::new(FsError::FileRead(FileRangeReadError::InvalidRange), work)
    })?;
    if end > logical_bytes {
        return Err(OperationFailure::new(
            FsError::FileRead(FileRangeReadError::InvalidRange),
            work,
        ));
    }
    Ok(())
}

async fn capture_dependencies_async<O: AsyncObjectStore>(
    objects: &O,
    generation_root: &ObjectId,
    config: VolumeConfig,
    regions: Vec<DependencyRegion>,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> FsResult<Vec<Dependency>> {
    let probe = AuthenticatedGenerationProbe::new(objects, probe_limits(config))
        .map_err(|error| OperationFailure::before_work(error.into()))?;
    probe
        .capture_many_async(
            GenerationId::new(generation_root.digest),
            regions,
            config.limits.maximum_checkout_dependencies,
            budget,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(WorkCounters::default(), FsError::Probe))
}

async fn capture_known_dependencies_async<O: AsyncObjectStore>(
    objects: &O,
    config: VolumeConfig,
    regions: Vec<(Option<FileRecord>, DependencyRegion)>,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> FsResult<Vec<Dependency>> {
    let probe = AuthenticatedGenerationProbe::new(objects, probe_limits(config))
        .map_err(|error| OperationFailure::before_work(error.into()))?;
    probe
        .capture_records_many_async(
            regions,
            config.limits.maximum_checkout_dependencies,
            budget,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(WorkCounters::default(), FsError::Probe))
}

fn generation_from_record(
    record: &DurableCommit,
    volume_id: VolumeId,
    work: WorkCounters,
) -> Result<ObjectId, OperationFailure<FsError>> {
    if record.sequence == Sequence::new(1) {
        let creation = decode_volume_created(&record.payload, MAXIMUM_VOLUME_EVENT_BYTES)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        if creation.volume_id != volume_id {
            return Err(OperationFailure::new(FsError::VolumeMismatch, work));
        }
        Ok(creation.initial_generation_root)
    } else {
        let publication = decode_published_generation(&record.payload, MAXIMUM_VOLUME_EVENT_BYTES)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        if publication.volume_id != volume_id {
            return Err(OperationFailure::new(FsError::VolumeMismatch, work));
        }
        Ok(publication.generation_root)
    }
}

async fn collect_generation_ancestors<O: AsyncObjectStore>(
    objects: &O,
    start: GenerationId,
    config: VolumeConfig,
    maximum_generations: u32,
    cancellation: &CancellationToken,
) -> Result<BTreeSet<GenerationId>, crate::workspace::WorkspaceError> {
    let maximum = usize::try_from(maximum_generations).unwrap_or(usize::MAX);
    let mut visited = BTreeSet::new();
    let mut pending = VecDeque::from([start]);
    while let Some(generation) = pending.pop_front() {
        if visited.contains(&generation) {
            continue;
        }
        if visited.len() == maximum {
            return Err(crate::workspace::WorkspaceError::LineageLimit);
        }
        let (root, _) = read_generation_root(
            objects,
            ObjectId {
                kind: ObjectKind::GenerationRoot,
                digest: generation.digest(),
            },
            config,
            WorkBudget::UNBOUNDED,
            cancellation,
        )
        .await
        .map_err(crate::workspace::WorkspaceError::engine)?;
        visited.insert(generation);
        pending.extend(root.parents);
    }
    Ok(visited)
}

async fn find_first_generation_ancestor<O: AsyncObjectStore>(
    objects: &O,
    start: GenerationId,
    config: VolumeConfig,
    maximum_generations: u32,
    candidates: &BTreeSet<GenerationId>,
    cancellation: &CancellationToken,
) -> Result<GenerationId, crate::workspace::WorkspaceError> {
    let maximum = usize::try_from(maximum_generations).unwrap_or(usize::MAX);
    let mut visited = BTreeSet::new();
    let mut pending = VecDeque::from([start]);
    while let Some(generation) = pending.pop_front() {
        if candidates.contains(&generation) {
            return Ok(generation);
        }
        if visited.contains(&generation) {
            continue;
        }
        if visited.len() == maximum {
            return Err(crate::workspace::WorkspaceError::LineageLimit);
        }
        let (root, _) = read_generation_root(
            objects,
            ObjectId {
                kind: ObjectKind::GenerationRoot,
                digest: generation.digest(),
            },
            config,
            WorkBudget::UNBOUNDED,
            cancellation,
        )
        .await
        .map_err(crate::workspace::WorkspaceError::engine)?;
        visited.insert(generation);
        pending.extend(root.parents);
    }
    Err(crate::workspace::WorkspaceError::NoCommonAncestor)
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
) -> Result<(), OperationFailure<FsError>> {
    config
        .validate()
        .map_err(|error| OperationFailure::before_work(error.into()))?;
    if config.lifecycle == Lifecycle::Durable && !capabilities.durable {
        return Err(OperationFailure::before_work(
            FsError::UnsupportedDurability,
        ));
    }
    Ok(())
}

fn validate_checkout(
    mode: CheckoutMode,
    config: VolumeConfig,
) -> Result<(), OperationFailure<FsError>> {
    mode.validate()
        .map_err(|error| OperationFailure::before_work(error.into()))?;
    match (mode.access, mode.consistency, mode.mutations) {
        (
            AccessMode::ReadOnly,
            ConsistencyMode::Pinned
            | ConsistencyMode::Manual
            | ConsistencyMode::TrackingSafe
            | ConsistencyMode::Live,
            MutationMode::None,
        )
        | (
            AccessMode::ReadWrite,
            ConsistencyMode::Pinned | ConsistencyMode::Manual | ConsistencyMode::TrackingSafe,
            MutationMode::PrivateOverlay,
        ) => Ok(()),
        (AccessMode::ReadWrite, ConsistencyMode::Live, MutationMode::DirectLive)
            if config.concurrency == ConcurrencyMode::SerializedAuthority =>
        {
            Ok(())
        }
        (AccessMode::ReadWrite, ConsistencyMode::Live, MutationMode::DirectLive) => Err(
            OperationFailure::before_work(FsError::LiveRequiresSerializedAuthority),
        ),
        _ => Err(OperationFailure::before_work(FsError::UnsupportedCheckout)),
    }
}

fn closure_limits(config: VolumeConfig) -> ClosureLimits {
    ClosureLimits {
        decode: decode_limits(config),
        maximum_objects: config.limits.maximum_objects_per_generation,
        maximum_files: config.limits.maximum_files_per_generation,
        maximum_object_bytes: config.limits.maximum_generation_bytes,
        profile: config.profile,
        symbolic_links: config.symbolic_links,
        hard_links: config.hard_links,
        sparse_files: config.sparse_files,
    }
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

fn directory_entries(record: Option<FileRecord>) -> Option<ObjectId> {
    let record = record?;
    match record.payload {
        FilePayload::Directory { entries } => Some(entries),
        _ => None,
    }
}

fn map_diff_failure(
    failure: OperationFailure<PersistentDiffError>,
    prior: WorkCounters,
) -> OperationFailure<FsError> {
    failure.map_with_prior_work(prior, |error| match error {
        PersistentDiffError::WrongRootKind | PersistentDiffError::InvalidLimit => {
            FsError::InvalidDiff
        }
        PersistentDiffError::AllocationFailed => FsError::DiffAllocationFailed,
        PersistentDiffError::Storage(error) => FsError::Object(error),
        PersistentDiffError::Decode(error) => FsError::Decode(error),
        PersistentDiffError::Work(error) => FsError::Work(error),
        PersistentDiffError::Cancelled => FsError::Cancelled(CancellationError),
    })
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
        decode: decode_limits(config),
        maximum_cached_generations: 2,
        maximum_cached_records: config.limits.maximum_checkout_dependencies,
        maximum_extent_spans: config.limits.maximum_directory_page_entries,
        maximum_content_payload_bytes: config.limits.maximum_read_bytes,
        maximum_directory_entries: config.limits.maximum_directory_page_entries,
    }
}

fn empty_metadata() -> FileMetadata {
    FileMetadata::default()
}

fn derived_file_id(volume_id: VolumeId) -> (FileId, WorkCounters) {
    let digest = derived_digest(ROOT_FILE_DOMAIN, volume_id);
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    (
        FileId::from_bytes(bytes),
        identity_hash_work(ROOT_FILE_DOMAIN),
    )
}

fn derived_operation_id(volume_id: VolumeId) -> (OperationId, WorkCounters) {
    let digest = derived_digest(CREATION_OPERATION_DOMAIN, volume_id);
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    (
        OperationId::from_bytes(bytes),
        identity_hash_work(CREATION_OPERATION_DOMAIN),
    )
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
        ..WorkCounters::default()
    }
}

fn creation_commit(operation_id: OperationId, payload: Vec<u8>) -> (ProposedCommit, WorkCounters) {
    let mut hasher = blake3::Hasher::new();
    hasher.update(CREATION_FINGERPRINT_DOMAIN);
    hasher.update(&payload);
    let length = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    let capacity = u64::try_from(payload.capacity()).unwrap_or(u64::MAX);
    (
        ProposedCommit {
            operation_id,
            fingerprint: Digest::from_bytes(*hasher.finalize().as_bytes()),
            payload: Bytes::from(payload),
        },
        WorkCounters {
            bytes_encoded: length,
            bytes_hashed: u64::try_from(CREATION_FINGERPRINT_DOMAIN.len())
                .unwrap_or(u64::MAX)
                .saturating_add(length),
            allocation_operations: 1,
            peak_allocation_bytes: capacity,
            ..WorkCounters::default()
        },
    )
}

fn retain_pending_operations(
    pending: &mut Vec<Mutation>,
    mut incoming: Vec<Mutation>,
    maximum: usize,
    work: WorkCounters,
    budget: WorkBudget,
) -> Result<WorkCounters, OperationFailure<FsError>> {
    if pending.is_empty() {
        *pending = incoming;
        return Ok(work);
    }
    let new_length = pending
        .len()
        .checked_add(incoming.len())
        .ok_or_else(|| OperationFailure::new(FsError::TooManyPendingMutations, work))?;
    if new_length > maximum {
        return Err(OperationFailure::new(
            FsError::TooManyPendingMutations,
            work,
        ));
    }
    let item_bytes = u64::try_from(size_of::<Mutation>())
        .map_err(|_| OperationFailure::new(FsError::Work(WorkError::Overflow), work))?;
    let incoming_bytes = u64::try_from(incoming.len())
        .ok()
        .and_then(|count| count.checked_mul(item_bytes))
        .ok_or_else(|| OperationFailure::new(FsError::Work(WorkError::Overflow), work))?;
    let mut delta = WorkCounters {
        bytes_copied: incoming_bytes,
        ..WorkCounters::default()
    };
    let mut peak = work.peak_allocation_bytes;
    if new_length > pending.capacity() {
        let doubled = pending.capacity().checked_mul(2).unwrap_or(maximum);
        let target = new_length.max(doubled.min(maximum));
        let old_bytes = u64::try_from(pending.capacity())
            .ok()
            .and_then(|count| count.checked_mul(item_bytes))
            .ok_or_else(|| OperationFailure::new(FsError::Work(WorkError::Overflow), work))?;
        let new_bytes = u64::try_from(target)
            .ok()
            .and_then(|count| count.checked_mul(item_bytes))
            .ok_or_else(|| OperationFailure::new(FsError::Work(WorkError::Overflow), work))?;
        let copied_existing = u64::try_from(pending.len())
            .ok()
            .and_then(|count| count.checked_mul(item_bytes))
            .ok_or_else(|| OperationFailure::new(FsError::Work(WorkError::Overflow), work))?;
        delta.allocation_operations = 1;
        delta.bytes_copied = delta
            .bytes_copied
            .checked_add(copied_existing)
            .ok_or_else(|| OperationFailure::new(FsError::Work(WorkError::Overflow), work))?;
        peak = peak.max(
            old_bytes
                .checked_add(new_bytes)
                .ok_or_else(|| OperationFailure::new(FsError::Work(WorkError::Overflow), work))?,
        );
        let mut attempted = work
            .checked_add(delta)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        attempted.peak_allocation_bytes = peak;
        attempted
            .verify(budget)
            .map_err(|error| OperationFailure::new(error.into(), attempted))?;
        pending
            .try_reserve_exact(target - pending.capacity())
            .map_err(|_| {
                OperationFailure::new(FsError::PendingMutationAllocationFailed, attempted)
            })?;
        pending.append(&mut incoming);
        return Ok(attempted);
    }
    let combined = work
        .checked_add(delta)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    combined
        .verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), combined))?;
    pending.append(&mut incoming);
    Ok(combined)
}

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

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
fn object_vec_bytes(objects: &Vec<ObjectId>) -> u64 {
    u64::try_from(objects.capacity())
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(size_of::<ObjectId>()).unwrap_or(u64::MAX))
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
fn merge_sorted_object_ids(
    reachable: &mut Vec<ObjectId>,
    incoming: &[ObjectId],
    incoming_bytes: u64,
    retained_bytes: u64,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<(), OperationFailure<FsError>> {
    let maximum_items = reachable
        .len()
        .checked_add(incoming.len())
        .ok_or_else(|| OperationFailure::new(FsError::Work(WorkError::Overflow), *work))?;
    let maximum_bytes = u64::try_from(maximum_items)
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(size_of::<ObjectId>()).unwrap_or(u64::MAX));
    let peak = retained_bytes
        .checked_add(object_vec_bytes(reachable))
        .and_then(|value| value.checked_add(incoming_bytes))
        .and_then(|value| value.checked_add(maximum_bytes))
        .ok_or_else(|| OperationFailure::new(FsError::Work(WorkError::Overflow), *work))?;
    let admission = work
        .checked_add(WorkCounters {
            items_examined: u64::try_from(maximum_items).unwrap_or(u64::MAX),
            bytes_copied: maximum_bytes,
            allocation_operations: u64::from(maximum_items != 0),
            peak_allocation_bytes: peak,
            ..WorkCounters::default()
        })
        .map_err(|error| OperationFailure::new(error.into(), *work))?;
    admission
        .verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), *work))?;
    let mut merged = Vec::new();
    merged
        .try_reserve_exact(maximum_items)
        .map_err(|_| OperationFailure::new(FsError::GarbageCollectionAllocationFailed, *work))?;
    let mut left_index = 0;
    let mut right_index = 0;
    while left_index < reachable.len() && right_index < incoming.len() {
        match reachable[left_index].cmp(&incoming[right_index]) {
            std::cmp::Ordering::Less => {
                merged.push(reachable[left_index]);
                left_index += 1;
            }
            std::cmp::Ordering::Greater => {
                merged.push(incoming[right_index]);
                right_index += 1;
            }
            std::cmp::Ordering::Equal => {
                merged.push(reachable[left_index]);
                left_index += 1;
                right_index += 1;
            }
        }
    }
    merged.extend_from_slice(&reachable[left_index..]);
    merged.extend_from_slice(&incoming[right_index..]);
    let copied_bytes = u64::try_from(merged.len())
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(size_of::<ObjectId>()).unwrap_or(u64::MAX));
    *work = work
        .checked_add(WorkCounters {
            items_examined: u64::try_from(maximum_items).unwrap_or(u64::MAX),
            bytes_copied: copied_bytes,
            allocation_operations: u64::from(maximum_items != 0),
            peak_allocation_bytes: peak,
            ..WorkCounters::default()
        })
        .map_err(|error| OperationFailure::new(error.into(), *work))?;
    *reachable = merged;
    Ok(())
}

fn remaining(
    work: WorkCounters,
    budget: WorkBudget,
) -> Result<WorkBudget, OperationFailure<FsError>> {
    work.remaining(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))
}

fn merge_simultaneous_work(
    prior: WorkCounters,
    mut nested: WorkCounters,
    live_bytes: u64,
    budget: WorkBudget,
) -> Result<WorkCounters, OperationFailure<FsError>> {
    let simultaneous_peak = live_bytes
        .checked_add(nested.peak_allocation_bytes)
        .ok_or_else(|| OperationFailure::new(FsError::Work(WorkError::Overflow), prior))?;
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

fn merge_simultaneous_failure(
    prior: WorkCounters,
    mut nested: WorkCounters,
    live_bytes: u64,
    error: FsError,
) -> OperationFailure<FsError> {
    let Some(simultaneous_peak) = live_bytes.checked_add(nested.peak_allocation_bytes) else {
        return OperationFailure::new(FsError::Work(WorkError::Overflow), prior);
    };
    nested.peak_allocation_bytes = 0;
    let Ok(mut merged) = prior.checked_add(nested) else {
        return OperationFailure::new(FsError::Work(WorkError::Overflow), prior);
    };
    merged.peak_allocation_bytes = merged.peak_allocation_bytes.max(simultaneous_peak);
    OperationFailure::new(error, merged)
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/facade.rs"]
mod tests;
