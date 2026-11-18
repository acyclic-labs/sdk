#![deny(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::indexing_slicing,
        clippy::cognitive_complexity,
        clippy::redundant_clone,
        reason = "tests index fixtures directly and spell out long scenario matrices"
    )
)]
#![doc = include_str!("../README.md")]
/// Shared canonical account-relative resource addressing.
pub use acyclic_auth::addressing;
#[cfg(not(target_arch = "wasm32"))]
pub use acyclic_fs_wire::{AUTHORIZATION, FsMethod, authorization};
#[cfg(test)]
#[cfg(all(not(target_arch = "wasm32")))]
acyclic_test_support::counting_allocator!();

#[cfg(test)]
extern crate self as acyclic_fs;

#[cfg(not(target_arch = "wasm32"))]
mod wire_service;
#[cfg(not(target_arch = "wasm32"))]
pub use wire_service::{
    CredentialGrant, CredentialGrantRequest, CredentialKind, DEFAULT_HOSTED_MAXIMUM_PAGE_ITEMS,
    DEFAULT_HOSTED_MAXIMUM_RESPONSE_BYTES, FILESYSTEM_PROTOCOL_VERSION, FilesystemCredentialIssuer,
    FilesystemNativeSources, FilesystemSourceProvider, FilesystemWireLimits, FilesystemWireService,
    HostedSourceInvalidation, HostedSourceResult, HostedSourceState, MountOwner, MountRefusal,
    admit_mount, mount_audience, mount_capability, mount_resource,
};

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn descriptor_digest() -> String {
    blake3::hash(acyclic_fs_wire::FILE_DESCRIPTOR_SET)
        .to_hex()
        .to_string()
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod test_native_io {
    pub(crate) fn initialize() {
        acyclic_native_runtime::initialize_native_io_services().expect("native I/O test services");
    }
}
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use test_native_io::initialize as initialize_test_native_io;

#[cfg(all(any(test, feature = "test-support"), windows))]
mod windows_test_filesystems;
/// Create a real Windows directory junction for native consumer controls.
#[cfg(all(feature = "test-support", windows))]
pub use windows_test_filesystems::junction as test_directory_junction;

pub mod access;
mod arc_layout;
pub use arc_layout::shared_allocation_bytes;
#[cfg(not(target_arch = "wasm32"))]
mod native_sync;
mod sync;
#[cfg(not(target_arch = "wasm32"))]
mod watch_queue;
pub use acyclic_stream::{
    OperationAttempt, OperationClaim, OperationClaims, OperationObservation, OperationTermination,
};

pub mod accounting;
#[cfg(test)]
#[path = "tests/accounting/mod.rs"]
mod accounting_tests;
pub mod async_storage;
pub mod binding_contract;
mod collection;
pub use collection::{Collection, PublicationHold};
pub mod cache;
pub mod cancellation;
pub mod compat_wire;
#[cfg(not(target_arch = "wasm32"))]
pub mod core_state;
pub mod mount_qualification;
#[cfg(not(target_arch = "wasm32"))]
pub mod package_install;
// Unit fixtures use the production memory and local compositions directly.
// cfg(test) supplies those implementations without a self dev-dependency.
pub mod demand;
/// Session filesystem payloads.
pub mod session;
pub use demand::DemandError;
pub mod distributed;
mod logical_objects;
pub use logical_objects::{LogicalObjectStore, ObjectSealer, PlainObjects};
pub mod facade;
pub mod foundation;
mod publication_changes;
pub use publication_changes::PublicationEntry;
pub use publication_changes::{
    DeltaOrigin, PublicationChanges, PublicationContext, PublicationDelta,
};
#[cfg(all(not(target_arch = "wasm32"), target_os = "macos"))]
mod fsevents;
pub mod git_compat;
mod heap_future;
#[doc(hidden)]
pub mod hosted_contract;
pub use hosted_contract::{MAX_BYTE_RESPONSE_ENVELOPE_BYTES, MINIMUM_HANDSHAKE_RESPONSE_BYTES};
mod identity;
#[cfg(kani)]
mod kani_proofs;
pub mod kernel;
pub mod lazy_workspace;
pub mod lineage;
mod lock_order;
pub mod materializer;
#[cfg(any(test, feature = "test-support"))]
pub mod memory;
use acyclic_fs_format::model;
mod model_names;
pub mod mount;
pub mod multi_root;
#[cfg(not(target_arch = "wasm32"))]
pub mod native_capture;
#[cfg(not(target_arch = "wasm32"))]
mod native_exchange;
#[cfg(not(target_arch = "wasm32"))]
#[doc(hidden)]
pub mod native_host;
#[cfg(all(
    not(target_arch = "wasm32"),
    any(
        all(windows, not(target_arch = "wasm32")),
        feature = "test-support",
        kani
    )
))]
mod native_ownership;
mod obs;
#[cfg(all(feature = "tooling", not(target_arch = "wasm32")))]
#[doc(hidden)]
pub mod tooling_contracts;
#[cfg(all(target_os = "linux", feature = "fuzzing"))]
#[doc(hidden)]
pub use acyclic_native_runtime::receive_packet_device_reply_for_test;
#[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
pub use native_ownership::codec_fuzz as native_ownership_codec;
#[cfg(not(target_arch = "wasm32"))]
pub mod native_mount;
#[cfg(not(target_arch = "wasm32"))]
mod native_name;
pub mod operation_window;
pub mod path;
#[cfg(not(test))]
use acyclic_fs_work::performance;
#[cfg(test)]
#[path = "tests/performance_scope.rs"]
mod performance;
mod record_store;
pub mod s3;
#[cfg(not(target_arch = "wasm32"))]
pub mod s3_multipart;
#[cfg(all(not(target_arch = "wasm32"), any(test, feature = "test-support")))]
pub mod simulation;
#[cfg(not(target_arch = "wasm32"))]
pub mod source;
#[cfg(not(target_arch = "wasm32"))]
mod source_watch;
pub mod speculation;
#[cfg(all(
    any(test, not(target_arch = "wasm32"), feature = "conformance"),
    not(target_arch = "wasm32")
))]
#[doc(hidden)]
pub mod staged_objects;
pub mod storage;
pub mod streams_record;
#[cfg(test)]
#[path = "tests/support.rs"]
pub(crate) mod test_support;
#[cfg(not(target_arch = "wasm32"))]
pub mod watch;
#[cfg(all(not(target_arch = "wasm32"), target_os = "windows"))]
mod windows_usn;
#[cfg(all(
    not(target_arch = "wasm32"),
    any(
        test,
        feature = "test-support",
        all(not(target_arch = "wasm32"), windows)
    )
))]
#[doc(hidden)]
pub mod windows_usn_codec;
pub mod workspace;
pub mod workspace_context;
pub mod workspace_context_wire;

use acyclic_fs_format::identity::{
    AuthorityId, CheckoutId, Digest, FileId, MountId, OperationId, VolumeId, WatchId,
};
use acyclic_fs_format::object_reference::{ObjectId, ObjectIdBytes, ObjectKind, ObjectStorage};
#[cfg(not(target_arch = "wasm32"))]
pub use acyclic_native_runtime::{RenameMode, durable_rename};
#[cfg(not(target_arch = "wasm32"))]
/// Scopes authority writes that survive a crash of this process but wait
/// for [`LocalFs::flush_deferred_authority`] to survive a power loss.
#[cfg(not(target_arch = "wasm32"))]
pub use acyclic_stream::deferring_durability as deferring_authority_durability;
#[cfg(not(target_arch = "wasm32"))]
pub use acyclic_stream::{LocalDurability as LocalStreamDurability, LocalStreamLimits};
#[cfg(not(target_arch = "wasm32"))]
pub use async_storage::NativeAuthorityGuard;
pub use async_storage::{
    AppendGroupOutcome, AsyncAuthorityStore, AsyncObjectStore, AuthorityAllocation, AuthorityError,
    AuthorityGroupCost, AuthorityGuard, AuthorityWork, DynObjectStore, EphemeralForkCommit,
    GenerationFork, GenerationForkSource, OwnedAuthorityGroup, OwnedAuthorityGuard,
    OwnedAuthorityTask, PolicyCut, PolicyGroup, PolicyOwner, PublicationScope, SharedObjectStore,
    WorkspaceForkCommit, WorkspaceForkOutcome, shared_objects,
};
pub use cache::{CachedObjectStore, ObjectCacheConfigError, ObjectCacheOptions, ObjectCacheStats};
pub use cancellation::{CancellationError, CancellationToken, Cancelled};
#[cfg(not(target_arch = "wasm32"))]
pub use core_state::{DeferredDurability, LocalCoreStateStore, LocalCoreStateStoreError};
pub use distributed::{
    AuthorityPlacement, FilesystemAppendPreparation, FilesystemCommit, FilesystemCommitOutcome,
    FilesystemCommitResult, PreparedFilesystemAppend, PreparedFilesystemCommit,
    StreamAuthorityStore,
};
pub use distributed::{StreamStateStoreCause, StreamStateStoreError, UnresolvedCommit};
pub use facade::{
    AuthoredLiveMutationResult, AuthoredMutation, AuthoredTransactionResult, Checkout,
    ContentAddress, ContentChange, ContentFile, ContentTimes, CreationAttribute, CreationOperation,
    DetachedFile, DirectoryBindingChange, DirectoryRecordEntry, DirectoryRecordPage,
    EmbeddedCapabilities, FileCloneRequest, FileDescription, FileRecordChange, Fs, FsError,
    FsResult, GenerationDiff, JoinCommitWitness, LiveMutationOutcome, LivePublicationObservation,
    MergeConflict, MergePreparation, NamedAttributeWriteMode, PathMetadataLookup, PinnedReader,
    ResolvedDirectoryEntry, ResolvedDirectoryPage, ResolvedFile, ResolvedFileRangeReadRequest,
    StagedContent, Volume, VolumeInitialization,
};
pub use facade::{
    CreateSpec, CreatedGeneration, ForkSpec, ForkedGeneration, PublishSpec, RestoreSpec,
    TransactionSpec,
};
#[cfg(not(target_arch = "wasm32"))]
pub use facade::{
    LocalAuthorityBackend, LocalFs, LocalGarbageCollection, LocalObjectBackend,
    LocalOperationWindowStore, LocalOptions, LocalVolume,
};
pub use facade::{MemoryAuthorityBackend, MemoryFs, MemoryObjectBackend};
pub use foundation::{
    AUTHORITY_COMMIT_DIGEST_ENVELOPE_BYTES, DurableCommit, Epoch, Head, ProposedCommit, Sequence,
    authority_commit_digest,
};
pub use git_compat::{
    GitBisectResult, GitBisectState, GitBlameLine, GitBranch, GitCaptureAuthenticationError,
    GitCaptureError, GitCaptureProof, GitCapturedGeneration, GitCommand, GitCommandOutput,
    GitCommit, GitCommitId, GitCompatError, GitCompatRepository, GitCompatRunError, GitCompatState,
    GitCompatStore, GitDiff, GitDiffCounts, GitDirtyState, GitFilesystemAction,
    GitFilesystemExecutor, GitFilesystemResult, GitGenerationRef, GitGrepMatch, GitGrepResult,
    GitIgnorePolicy, GitMergeResolution, GitPatchError, GitPathChange, GitPathStatus,
    GitPendingMutation, GitPendingTransition, GitPublicationRecord, GitRequest,
    GitRequestUnanswered, GitResetMode, GitStatus, GitTransitionId, GitTreeEntry, GitTreeRef,
    MemoryGitCompatStore, apply_git_patch, blame_git_generations, canonicalize_git_output_json,
    canonicalize_git_pending_transition_json, capture_git_compatible_generation,
    capture_git_compatible_generation_at, capture_git_compatible_generation_incremental,
    git_compatible_diff, git_compatible_diff_counts, git_generation_with_paths_from,
    git_merge_resolution, git_restore_paths, git_tracked_paths_after, grep_git_generation,
    parse_git_public_command, walk_git_tree,
};
pub use git_compat::{GitCompatCas, StreamGitCompatStore};

#[cfg(not(target_arch = "wasm32"))]
use acyclic_fs_native_contract::{NativeRootGeneration, NativeRootIdentity, NativeRootIncarnation};
pub use kernel::{
    DependencyRegion, DependencyState, DependencyUse, DirectoryPage, ExtentSeekTarget,
    GenerationExportManifest, GenerationExportManifestError, RebaseConflict, TreeEntry,
    decode_generation_export_manifest, encode_generation_export_manifest,
};
pub use lazy_workspace::{
    LazyDirectoryCursor, LazyDirectoryEntry, LazyDirectoryListing, LazyDirectoryNode,
    LazyDirectoryPage, LazyForkOrigin, LazyForkPhase, LazyLookup, LazyOverlay, LazyOverlayId,
    LazyShadow, LazyShadowId, LazySnapshotId, LazySnapshotRef, LazySourceRecord, LazyStat,
    LazyStoredRecord, LazyWorkspace, LazyWorkspaceError, LazyWorkspaceState, LazyWorkspaceStore,
    MemoryLazyWorkspaceStore, NativePublicationSnapshot, SelectedImportPlan,
};
pub use lineage::{
    MemoryWorkspaceLineageStore, MemoryWorkspaceLineageStoreError, WorkspaceGraph,
    WorkspaceLineageError, WorkspaceLineageRecord, WorkspaceLineageStore,
};
pub use materializer::{
    JournaledMaterializer, MaterializationBackend, MaterializationEdit, MaterializationError,
    MaterializationIntent, MaterializationJournal, MaterializationJournalCopyError,
    MaterializationJournalStore, MaterializationPhase, MaterializationPlan,
    MaterializationPreimage, MaterializationPreparationIdentity,
    MaterializationPreparationProgress, MaterializationPreparationSelection,
    MaterializationRecovery,
};
#[cfg(not(target_arch = "wasm32"))]
pub use materializer::{
    NATIVE_PREPARATION_ABSENT_IMAGE_PREFIX, NativeAllocation, NativeMaterializationWork,
    NativeStageUncertainty, NativeTreeMaterializationBackend, NativeTreeMaterializationError,
    NativeWorkReservation, validate_native_preparation_location,
};
#[cfg(not(target_arch = "wasm32"))]
pub use materializer::{
    NativeWorkspacePublication, NativeWorkspacePublicationError,
    load_native_materialization_journal, prepare_native_generation_transition,
    publish_native_generation_transition,
};
#[cfg(any(test, feature = "test-support"))]
pub use memory::{MemoryAuthorityStore, MemoryObjectStore};
use model::{FilesystemProfile, Lifecycle, VolumeConfig, VolumeLimits};
pub use mount::{
    MountError, MountedCheckout, MountedGeneration, MountedView, MountedViewBuilder,
    MountedViewSnapshot, RoutedCheckout,
};
pub use multi_root::{
    LineageMultiRootPublicationAuthorizationError, LineageMultiRootPublicationAuthorizer,
    MaterializingWorkspaceMultiRootPublisher, MaterializingWorkspaceMultiRootPublisherError,
    MemoryMultiRootPublicationStore, MemoryMultiRootPublicationStoreError, MultiRootConflictFinish,
    MultiRootFence, MultiRootMaterializer, MultiRootMergeCandidate, MultiRootMergePlan,
    MultiRootMergeRoot, MultiRootPublication, MultiRootPublicationAuthorizer,
    MultiRootPublicationCoordinator, MultiRootPublicationError, MultiRootPublicationPhase,
    MultiRootPublicationStore, MultiRootPublishRoot, MultiRootPublisher, Publication,
    WorkspaceMultiRootPublisher, WorkspaceMultiRootPublisherError, WorkspaceResolver,
};
#[cfg(not(target_arch = "wasm32"))]
pub use native_capture::{
    CaptureError, CaptureOptions, CapturePolicy, CaptureReceipt, HeldRoot, NativeDurability,
    NativeViewBaseline, SearchRoot, WatchCaptureReceipt, capture_baseline, capture_held_paths,
    capture_paths, capture_root_identity, capture_subtree, capture_watch_batch, file_identity,
    hold_root, hold_search_root, host_path_to_namespace, namespace_to_host_path,
};
#[cfg(not(target_arch = "wasm32"))]
pub use native_mount::{
    CheckoutMountSource, ContentSink, HostPathReplacement, HostPathRestore, LazyMount,
    MaterializationReceipt, MaterializeError, MaterializeOptions, Mount, MountAttributePage,
    MountDirectoryCursor, MountDirectoryEntry, MountDirectoryPage, MountFilesystem,
    MountLifecycleError, MountOpenFile, MountOptions, MountPublication, MountSourceError,
    MountSourceRetirement, MountSparseRange, MountSparseSpan, NativeBlockCloneAccelerationEvidence,
    NativeMountCleanupState, NativeMountControlPaths, NativeMountDrainProof, NativeMountError,
    NativeMountProbe, NativeMountRecoveryCleanupState, NativeMountRecoveryOwner,
    NativeMountSession, NativeMountWorkScope, NativeSourceIoLease,
    NativeSparseAccelerationEvidence, NativeStorageAccelerationError,
    NativeStorageAccelerationEvidence, NativeWorkingSet, SharedCheckout, SharedCheckoutState,
    SharedViewInstance, detach_native_mount_destination_after_crash, hold_shared_view_instance,
    initialize_native_mount_services, initialize_native_mount_services_async,
    initialize_native_source_services, initialize_native_source_services_async,
    materialize_checkout, materialize_checkout_paths, mount_native, mount_native_over_existing,
    native_mount_control_paths, prepare_native_mount_start, probe_native_mount,
    probe_native_storage_accelerations, reclaim_native_mount_destination_fence,
    recover_native_mount_destination, restore_checkout_host_path, seal_checkout,
    seal_checkout_with_permit, shared_view_namespace_contains, shared_view_root,
    shared_view_root_in,
};
#[cfg(all(not(target_arch = "wasm32"), target_os = "macos"))]
pub use native_mount::{DarwinRetirementFailure, DarwinTeardownFailure};
#[cfg(all(not(target_arch = "wasm32"), target_os = "linux"))]
pub use native_mount::{
    recover_disconnected_native_mount_destination, recover_disconnected_native_mount_destination_in,
};
pub use operation_window::{
    MemoryOperationWindowStore, OperationLease, OperationLeaseId, OperationReconcileLimits,
    OperationWindowCoordinator, OperationWindowError, OperationWindowFinish, OperationWindowLease,
    OperationWindowPhase, OperationWindowReconcile, OperationWindowSnapshot, OperationWindowStore,
    WorkspaceOperationFinish,
};
pub use operation_window::{StreamOperationWindowStore, StreamOperationWindowStoreError};
use performance::{
    MeasuredResult, OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkError,
    WorkObservationError,
};
pub use s3::{
    S3Error, S3List, S3ListCursor, S3ListOptions, S3MultipartOptions, S3MultipartUpload, S3Object,
    S3ObjectHead, S3Workspace,
};
#[cfg(all(not(target_arch = "wasm32"), any(test, feature = "test-support")))]
pub use simulation::{SimulatedStore, Simulation, SimulationFault};
#[cfg(not(target_arch = "wasm32"))]
pub use source::{ReconcileOutcome, Source, SourceError, SourceMode, SourceOptions, SourceState};
#[cfg(not(target_arch = "wasm32"))]
pub use source_watch::HeldRootWatch;
pub use speculation::{
    ObjectResidency, PromotionAdmission, PromotionCandidate, PromotionDestination,
    PromotionExecutor, PromotionMetrics, PromotionPlan, PromotionRejection, PromotionSpeculator,
    PromotionSpeculatorError, PromotionSpeculatorOptions, ResidencyAdmission, ResidencyCandidate,
    ResidencyHint, ResidencyMetrics, ResidencyPermit, ResidencyReason, ResidencyRejection,
    ResidencySpeculator, ResidencySpeculatorError, ResidencySpeculatorOptions,
    SpeculationController, SpeculationControllerError, SpeculationMetrics, SpeculationOptions,
    SpeculationPreemption, StorageLocationId, StorageTier, StorePromotionExecutor,
    StorePromotionExecutorError, execute_promotion, execute_residency,
};
pub use storage::{
    AppendOutcome, AuthorityFailure, AuthorityResult, AuthorityStoreError, ByteRange,
    CreateAuthorityOutcome, FenceOutcome, GuardedAppend, HashedObject, ObjectFailure, ObjectRead,
    ObjectReadRequest, ObjectReadRetention, ObjectResult, ObjectStoreError, PublicationPermit,
    PublicationReservation, ReplayLimit, ReservationOutcome, object_digest,
};
pub use streams_record::{
    STREAMS_AUTHORITY_RECORD_HEADER_BYTES, StreamsAuthorityRecord, StreamsAuthorityRecordError,
    StreamsDurableRecord,
};
#[cfg(not(target_arch = "wasm32"))]
pub use watch::{
    NativeWatch, NativeWatchCapabilities, NativeWatchCut, NativeWatchError, NativeWatchOptions,
    NativeWatchRescan, WatchBatch, WatchChange, WatchInvalidationReason, native_watch_capabilities,
};
#[cfg(all(not(target_arch = "wasm32"), target_os = "windows"))]
pub use windows_usn::{
    WindowsUsnCheckpoint, WindowsUsnContinuity, WindowsUsnDiscontinuity, WindowsUsnError,
    capture_windows_usn_checkpoint, validate_windows_usn_checkpoint,
};
pub use workspace::{
    ApplyOptions, ChangeSet, ChangedPath, ChangedPathEntry, ChangedPaths, DrivenJoinError,
    EphemeralFork, FileCondition, FileIdentityPaths, ForkOptions, Generation, IdempotencyKey,
    JoinApplication, JoinBuilder, JoinHistory, JoinOutcome, JoinPlan, OperationPublication,
    Transaction, TransactionCommit, TransactionRebase, Workspace, WorkspaceDelete, WorkspaceError,
    WorkspaceExtentKind, WorkspaceExtentPlan, WorkspaceExtentSpan, WorkspaceId, WorkspaceMetadata,
    WorkspaceName, WorkspaceNameError, WorkspacePathApply, WorkspacePathConflict, WorkspaceRebase,
    WorkspaceRestore, WorkspaceStat,
};
pub use workspace_context::{
    MemoryWorkspaceContextStore, MemoryWorkspaceContextStoreError, WorkspaceContext,
    WorkspaceContextDiscardOutcome, WorkspaceContextError, WorkspaceContextId,
    WorkspaceContextRegistry, WorkspaceContextRoot, WorkspaceContextState, WorkspaceContextStore,
    WorkspaceRootId, WorkspaceRoute, WorkspaceRouteKind,
};

#[cfg(feature = "browser")]
pub mod browser;

/// Shared backend qualification and checked on-demand filesystem workloads.
#[cfg(feature = "conformance")]
pub mod conformance;

#[cfg(not(target_arch = "wasm32"))]
mod native_future;
#[cfg(not(target_arch = "wasm32"))]
pub use native_future::NativeFutureBundle;

/// Optional original native work; `None` is unbounded.
#[cfg(not(target_arch = "wasm32"))]
pub type NativeWork<'a> = Option<&'a NativeMaterializationWork>;
/// Browser work has no native allocation ledger.
#[cfg(target_arch = "wasm32")]
pub type NativeWork<'a> = Option<&'a ()>;
/// Optional original native allocation custody.
#[cfg(not(target_arch = "wasm32"))]
pub type NativeCustody = Option<NativeAllocation>;
/// Browser work has no native allocation custody.
#[cfg(target_arch = "wasm32")]
pub type NativeCustody = Option<()>;
