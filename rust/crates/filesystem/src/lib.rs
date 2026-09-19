#![deny(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::indexing_slicing,
        clippy::cognitive_complexity,
        clippy::redundant_clone,
        clippy::too_many_lines
    )
)]
#![doc = include_str!("../README.md")]

/// Generated public gRPC schema and client/server bindings.
#[cfg(not(target_arch = "wasm32"))]
#[allow(missing_docs, clippy::all, clippy::pedantic, clippy::too_many_lines)]
pub mod wire {
    /// Shared operation and capability messages used by Filesystem.
    pub mod harness {
        /// Version 1 of the shared harness contract.
        pub mod v1 {
            include!("generated/acyclic/harness/v1/acyclic.harness.v1.rs");
        }
    }

    /// Filesystem messages and service definitions.
    pub mod filesystem {
        /// Version 2 of the public Filesystem contract.
        pub mod v2 {
            include!(concat!(env!("OUT_DIR"), "/acyclic.filesystem.v2.rs"));
        }

        /// Process-local daemon lifecycle and native-host operations.
        pub mod daemon {
            /// Version 2 of the daemon-only transport.
            pub mod v2 {
                include!(concat!(env!("OUT_DIR"), "/acyclic.filesystem.daemon.v2.rs"));
            }
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod wire_service;
#[cfg(not(target_arch = "wasm32"))]
pub use wire_service::{
    CredentialGrant, CredentialGrantRequest, CredentialKind, FilesystemCredentialIssuer,
    FilesystemSourceProvider, FilesystemWireLimits, FilesystemWireService,
    HostedSourceInvalidation, HostedSourceOperation, HostedSourceResult, HostedSourceScope,
    HostedSourceState,
};

/// Canonical public descriptor set used by compatibility and conformance gates.
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("generated/acyclic-filesystem-v2.bin");

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn descriptor_digest() -> String {
    blake3::hash(FILE_DESCRIPTOR_SET).to_hex().to_string()
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod public_contract_tests {
    use super::{FILE_DESCRIPTOR_SET, wire};

    #[test]
    fn generated_transport_and_descriptor_are_packaged() {
        assert!(!FILE_DESCRIPTOR_SET.is_empty());
        let _ = std::any::TypeId::of::<
            wire::filesystem::v2::filesystem_service_client::FilesystemServiceClient<
                tonic::transport::Channel,
            >,
        >();
    }
}

pub mod async_storage;
pub mod cache;
pub mod cancellation;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub mod core_state;
pub mod demand;
#[cfg(feature = "distributed")]
pub mod distributed;
pub mod facade;
pub mod foundation;
pub mod git_compat;
#[cfg(not(target_arch = "wasm32"))]
pub mod hosted;
pub mod kernel;
pub mod lineage;
pub mod materializer;
#[cfg(test)]
pub mod memory;
pub mod merge_driver;
pub mod model;
pub mod mount;
#[cfg(feature = "native-watch")]
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub mod native_capture;
#[cfg(not(target_arch = "wasm32"))]
pub mod native_exchange;
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
#[doc(hidden)]
pub mod native_host;
#[cfg(not(target_arch = "wasm32"))]
mod native_identity;
#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
pub mod native_mount;
#[cfg(feature = "native-watch")]
mod native_name;
pub mod notification;
pub mod operation_window;
pub mod path;
pub mod performance;
pub mod s3;
#[cfg(all(feature = "s3-http", not(target_arch = "wasm32")))]
pub mod s3_http;
#[cfg(feature = "memory")]
pub mod simulation;
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub mod source;
pub mod speculation;
pub mod storage;
pub mod streams_record;
#[cfg(all(test, feature = "memory"))]
#[path = "tests/support.rs"]
pub(crate) mod test_support;
pub mod text_merge;
#[cfg(feature = "native-watch")]
pub mod watch;
#[cfg(all(feature = "native-watch", target_os = "windows"))]
mod windows_usn;
pub mod workspace;

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub use acyclic_native_runtime::{RenameMode, durable_rename};
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub use acyclic_objects::{LocalDurability as LocalObjectsDurability, LocalObjectsLimits};
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub use acyclic_stream::{LocalDurability as LocalStreamDurability, LocalStreamLimits};
pub use async_storage::{
    AsyncAuthorityStore, AsyncObjectStore, GenerationFork, GenerationForkSource,
    ImmediateAuthorityStore, ImmediateObjectStore,
};
pub use cache::{CachedObjectStore, ObjectCacheConfigError, ObjectCacheOptions, ObjectCacheStats};
pub use cancellation::{CancellationError, CancellationToken, Cancelled};
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub use core_state::{LocalCoreStateStore, LocalCoreStateStoreError};
#[cfg(feature = "distributed")]
pub use distributed::{ProviderObjectStore, StreamAuthorityStore};
pub use facade::{
    AuthoredLiveMutationResult, AuthoredMutation, AuthoredTransactionResult, Checkout,
    CheckoutCommitOutcome, ContentStager, DetachedFile, DirectoryBindingChange,
    DirectoryPageRequest, DirectoryRecordEntry, DirectoryRecordPage, EmbeddedCapabilities,
    FileCloneRequest, FileDescription, FileRecordChange, Fs, FsError, FsReceipt, FsResult,
    GenerationDiff, LiveMutationOutcome, MergeConflict, MergePreparation, NamedAttributeWriteMode,
    PathMetadataLookup, PinnedReader, ResolvedDirectoryEntry, ResolvedDirectoryPage, ResolvedFile,
    ResolvedFileRangeReadRequest, StagedContent, Volume,
};
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub use facade::{
    LocalAuthorityBackend, LocalFs, LocalGarbageCollection, LocalObjectBackend, LocalOptions,
    LocalVolume,
};
#[cfg(all(feature = "memory", feature = "distributed"))]
pub use facade::{MemoryAuthorityBackend, MemoryFs, MemoryObjectBackend};
pub use foundation::{
    AUTHORITY_COMMIT_DIGEST_ENVELOPE_BYTES, AuthorityId, CheckoutId, Digest, DurableCommit, Epoch,
    FileId, GenerationId, Head, MountId, OperationId, ProposedCommit, Sequence, VolumeId, WatchId,
    authority_commit_digest,
};
pub use git_compat::{
    GitBisectResult, GitBisectState, GitBlameLine, GitBranch, GitCaptureError,
    GitCapturedGeneration, GitCommand, GitCommandOutput, GitCommit, GitCommitId, GitCompatError,
    GitCompatRepository, GitCompatRunError, GitCompatState, GitCompatStore, GitFilesystemAction,
    GitFilesystemExecutor, GitFilesystemResult, GitGenerationRef, GitGrepMatch, GitGrepResult,
    GitIgnorePolicy, GitObjectName, GitPatchError, GitPendingMutation, GitPendingTransition,
    GitResetMode, GitStatus, GitTransitionId, GitTreeEntry, MemoryGitCompatStore, apply_git_patch,
    blame_git_generations, capture_git_compatible_generation, grep_git_generation, walk_git_tree,
};
#[cfg(not(target_arch = "wasm32"))]
pub use hosted::{
    HostedFs, HostedFsError, HostedFsOptions, HostedGeneration, HostedS3Access,
    HostedS3AccessOptions, HostedTransaction, HostedWorkspace,
};
pub use kernel::{
    GenerationExportManifest, GenerationExportManifestError, decode_generation_export_manifest,
    encode_generation_export_manifest,
};
pub use lineage::{
    MemoryWorkspaceLineageStore, MemoryWorkspaceLineageStoreError, WorkspaceGraph,
    WorkspaceLineageError, WorkspaceLineageRecord, WorkspaceLineageStore,
};
pub use materializer::{
    JournaledMaterializer, MaterializationBackend, MaterializationEdit, MaterializationError,
    MaterializationJournal, MaterializationJournalStore, MaterializationPhase, MaterializationPlan,
    MaterializationPreimage, MaterializationRecovery, MemoryMaterializationJournalStore,
    MemoryMaterializationJournalStoreError,
};
#[cfg(not(target_arch = "wasm32"))]
pub use materializer::{NativeTreeMaterializationBackend, NativeTreeMaterializationError};
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub use materializer::{NativeTreePublicationError, publish_native_tree};
#[cfg(test)]
pub use memory::{MemoryAuthorityStore, MemoryObjectStore};
pub use merge_driver::{
    AttributeRule, CachedMergeResolution, ConflictKey, ConflictKind, ConflictSide, ConflictValue,
    ConflictView, DefaultTextMergeDriver, DriverError, DriverRegistrationError,
    MemoryMergeResolutionCache, MergeDriver, MergeDriverMode, MergeDriverRegistry, MergePlan,
    MergePlanResolutionError, MergeResolution, MergeResolutionCache, ResolutionKey,
    UnpublishedMergeCandidate, resolve_merge_plan,
};
pub use mount::{
    MountError, MountedCheckout, MountedGeneration, MountedView, MountedViewBuilder,
    MountedViewSnapshot, RoutedCheckout,
};
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub use native_capture::{
    CaptureError, CaptureOptions, CapturePolicy, CaptureReceipt, WatchCaptureReceipt,
    capture_baseline, capture_baseline_with_policy, capture_paths, capture_paths_with_policy,
    capture_root_identity, capture_subtree, capture_subtree_with_policy,
    capture_subtrees_with_policy, capture_watch_batch, capture_watch_batch_with_policy,
    host_path_to_namespace, namespace_to_host_path,
};
#[cfg(not(target_arch = "wasm32"))]
pub use native_exchange::{
    NativeExchangeError, NativeExchangeJournal, NativeExchangeOutcome, NativeExchangePhase,
    exchange_native_entries, prepare_native_exchange, publish_native_exchange,
    recover_native_exchange,
};
#[cfg(not(target_arch = "wasm32"))]
pub use native_identity::NativeRootIdentity;
#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
pub use native_mount::{
    CheckoutMountSource, MaterializationReceipt, MaterializeError, MaterializeOptions, Mount,
    MountAttributePage, MountDirectoryEntry, MountDirectoryPage, MountFilesystem,
    MountLifecycleError, MountLookup, MountNode, MountNodeKind, MountOpenFile, MountOptions,
    MountPath, MountPublication, MountRangeAllocation, MountSeekTarget, MountSourceError,
    MountSparseRange, MountSparseSpan, NativeBlockCloneAccelerationEvidence,
    NativeMountCapabilities, NativeMountError, NativeMountKind, NativeMountRequest,
    NativeMountSession, NativeMountSessionIsolation, NativeSparseAccelerationEvidence,
    NativeStorageAccelerationError, NativeStorageAccelerationEvidence, NativeStorageCapabilities,
    NativeStorageCapabilityError, RoutedMountSource, SharedCheckout, SharedCheckoutState,
    materialize_checkout, materialize_checkout_host_path, materialize_checkout_path, mount_native,
    mount_native_over_existing, probe_native_mount, probe_native_storage_accelerations,
    probe_native_storage_capabilities, reclaim_native_mount_destination_fence,
    reclaim_stale_native_mount_destination_fences, recover_native_mount_destination, seal_checkout,
};
pub use notification::{
    AsyncNotificationStore, ImmediateNotificationStore, MemoryNotificationStore, NotificationError,
    NotificationPoll, NotificationResult, NotificationStore,
};
pub use operation_window::{
    MemoryOperationWindowStore, OperationLease, OperationLeaseId, OperationReconcileLimits,
    OperationWindowCoordinator, OperationWindowError, OperationWindowFinish, OperationWindowLease,
    OperationWindowPhase, OperationWindowReconcile, OperationWindowSnapshot, OperationWindowStore,
    WorkspaceOperationFinish,
};
pub use performance::{
    MeasuredResult, OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkError,
};
pub use s3::{
    S3Error, S3List, S3ListCursor, S3ListOptions, S3MultipartOptions, S3MultipartUpload, S3Object,
    S3ObjectHead, S3Workspace,
};
#[cfg(all(feature = "s3-http", not(target_arch = "wasm32")))]
pub use s3_http::{
    FilesystemS3Adapter, FilesystemS3Authentication, FilesystemS3Limits, FilesystemS3Principal,
    FilesystemS3Resolver, S3MultipartRetentionLimits, active_s3_multipart_objects,
};
#[cfg(feature = "memory")]
pub use simulation::{
    ScheduledSimulationFault, SimulatedAuthorityStore, SimulatedObjectStore, Simulation,
    SimulationError, SimulationFault, SimulationOperation, SimulationOptions, SimulationTrace,
};
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub use source::{ReconcileOutcome, Source, SourceError, SourceMode, SourceOptions, SourceState};
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
    AppendOutcome, AuthorityFailure, AuthorityReceipt, AuthorityResult, AuthorityStore,
    AuthorityStoreError, ByteRange, CreateAuthorityOutcome, FenceOutcome,
    OBJECT_DIGEST_ENVELOPE_BYTES, ObjectFailure, ObjectId, ObjectKind, ObjectRead,
    ObjectReadRequest, ObjectReadRetention, ObjectReceipt, ObjectResult, ObjectStore,
    ObjectStoreError, ReplayLimit, object_digest,
};
pub use streams_record::{
    STREAMS_AUTHORITY_RECORD_HEADER_BYTES, StreamsAuthorityRecord, StreamsAuthorityRecordError,
    StreamsDurableRecord,
};
#[cfg(feature = "native-watch")]
pub use watch::{
    NativeWatch, NativeWatchBackend, NativeWatchCapabilities, NativeWatchError, NativeWatchOptions,
    WatchBatch, WatchChange, WatchEpoch, WatchInvalidationReason, WatchSequence,
    native_watch_capabilities,
};
#[cfg(all(feature = "native-watch", target_os = "windows"))]
pub use windows_usn::{
    WindowsUsnCheckpoint, WindowsUsnContinuity, WindowsUsnDiscontinuity, WindowsUsnError,
    capture_windows_usn_checkpoint, validate_windows_usn_checkpoint,
};
pub use workspace::{
    ApplyOptions, ChangeSet, ChangedPath, Checkpoint, DrivenJoinError, ForkOptions, Generation,
    GenerationPin, IdempotencyKey, JoinBuilder, JoinHistory, JoinOutcome, JoinPlan, Transaction,
    TransactionCommit, TransactionConflict, TransactionConflictRegion, TransactionDependencyUse,
    TransactionRebase, TransactionSparseSeek, Workspace, WorkspaceDelete, WorkspaceDirectoryEntry,
    WorkspaceDirectoryPage, WorkspaceError, WorkspaceExtentKind, WorkspaceExtentPlan,
    WorkspaceExtentSpan, WorkspaceId, WorkspaceMetadata, WorkspaceName, WorkspaceNameError,
    WorkspacePathApply, WorkspacePathConflict, WorkspaceRebase, WorkspaceRestore, WorkspaceStat,
    WorkspaceSync,
};
