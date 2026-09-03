#![deny(unsafe_code)]
//! Canonical Rust SDK for immutable, independently configured filesystem volumes.
//!
//! This crate owns its public contracts and first-party embedded backends so a
//! packaged consumer never depends on unpublished workspace crates. Optional
//! processes and language bindings depend on this crate, not the reverse.

/// Generated public gRPC schema and client/server bindings.
#[cfg(not(target_arch = "wasm32"))]
#[allow(missing_docs, clippy::all)]
pub mod wire {
    /// Shared operation and capability messages used by Filesystem.
    pub mod harness {
        /// Version 1 of the shared harness contract.
        pub mod v1 {
            include!("generated/acyclic.harness.v1.rs");
        }
    }

    /// Filesystem messages and service definitions.
    pub mod filesystem {
        /// Version 1 of the public Filesystem contract.
        pub mod v1 {
            include!("generated/acyclic.filesystem.v1.rs");
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod wire_service;
#[cfg(not(target_arch = "wasm32"))]
pub use wire_service::{
    CredentialGrant, CredentialGrantRequest, CredentialKind, FilesystemCredentialIssuer,
    FilesystemWireLimits, FilesystemWireService,
};

/// Canonical public descriptor set used by compatibility and conformance gates.
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("generated/acyclic-filesystem-v1.bin");

#[cfg(all(test, not(target_arch = "wasm32")))]
mod public_contract_tests {
    use super::{FILE_DESCRIPTOR_SET, wire};

    #[test]
    fn generated_transport_and_descriptor_are_packaged() {
        assert!(!FILE_DESCRIPTOR_SET.is_empty());
        let _ = std::any::TypeId::of::<
            wire::filesystem::v1::filesystem_service_client::FilesystemServiceClient<
                tonic::transport::Channel,
            >,
        >();
    }
}

pub mod async_storage;
pub mod cache;
pub mod cancellation;
#[cfg(all(feature = "distributed", not(target_arch = "wasm32")))]
pub mod distributed;
pub mod facade;
pub mod foundation;
pub mod kernel;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub mod local;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub mod local_authority;
#[cfg(feature = "memory")]
pub mod memory;
pub mod model;
pub mod mount;
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub mod native_capture;
#[cfg(all(feature = "native-executor", not(target_arch = "wasm32")))]
pub mod native_executor;
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
#[doc(hidden)]
pub mod native_host;
#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
pub mod native_mount;
pub mod notification;
pub mod path;
pub mod performance;
pub mod s3;
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
#[cfg(feature = "native-watch")]
pub mod watch;
pub mod workspace;

pub use async_storage::{
    AsyncAuthorityStore, AsyncObjectStore, ImmediateAuthorityStore, ImmediateObjectStore,
};
pub use cache::{CachedObjectStore, ObjectCacheConfigError, ObjectCacheOptions, ObjectCacheStats};
pub use cancellation::{CancellationError, CancellationToken, Cancelled};
#[cfg(all(feature = "distributed", not(target_arch = "wasm32")))]
pub use distributed::{ProviderObjectStore, StreamAuthorityStore};
pub use facade::{
    AuthoredLiveMutationResult, AuthoredMutation, AuthoredTransactionResult, Checkout,
    CheckoutCommitOutcome, DetachedFile, DirectoryBindingChange, DirectoryRecordEntry,
    DirectoryRecordPage, EmbeddedCapabilities, FileCloneRequest, FileRecordChange, Fs, FsError,
    FsReceipt, FsResult, GenerationDiff, LiveMutationOutcome, MergeConflict, MergePreparation,
    NamedAttributeWriteMode, PathMetadataLookup, StagedContent, Volume,
};
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub use facade::{LocalAuthorityBackend, LocalFs, LocalObjectBackend, LocalOptions, LocalVolume};
pub use foundation::{
    AUTHORITY_COMMIT_DIGEST_ENVELOPE_BYTES, AuthorityId, CheckoutId, Digest, DurableCommit, Epoch,
    FileId, GenerationId, Head, MountId, OperationId, ProposedCommit, Sequence, VolumeId, WatchId,
    authority_commit_digest,
};
pub use kernel::{
    GenerationExportManifest, GenerationExportManifestError, decode_generation_export_manifest,
    encode_generation_export_manifest,
};
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub use local::{LocalGarbageCollection, LocalObjectStore};
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub use local_authority::{LocalAuthorityConfig, LocalAuthorityStore};
#[cfg(feature = "memory")]
pub use memory::{MemoryAuthorityStore, MemoryObjectStore};
pub use mount::{
    MountError, MountedCheckout, MountedGeneration, MountedView, MountedViewBuilder,
    MountedViewSnapshot, RoutedCheckout,
};
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub use native_capture::{
    CaptureError, CaptureOptions, CaptureReceipt, WatchCaptureReceipt, capture_baseline,
    capture_paths, capture_root_identity, capture_watch_batch,
};
#[cfg(all(feature = "native-executor", not(target_arch = "wasm32")))]
pub use native_executor::{
    NativeExecutionError, NativeExecutor, NativeExecutorConfig, NativeExecutorConfigError,
    NativeStore,
};
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
    materialize_checkout, mount_native, probe_native_mount, probe_native_storage_accelerations,
    probe_native_storage_capabilities, reclaim_native_mount_destination_fence,
    reclaim_stale_native_mount_destination_fences, recover_native_mount_destination, seal_checkout,
};
#[cfg(all(feature = "native-mount", target_os = "windows"))]
pub use native_mount::{
    WindowsUsnCheckpoint, WindowsUsnContinuity, WindowsUsnDiscontinuity, WindowsUsnError,
    capture_windows_usn_checkpoint, validate_windows_usn_checkpoint,
};
pub use notification::{
    AsyncNotificationStore, ImmediateNotificationStore, MemoryNotificationStore, NotificationError,
    NotificationPoll, NotificationResult, NotificationStore,
};
pub use performance::{
    MeasuredResult, OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkError,
};
pub use s3::{
    S3Error, S3List, S3ListOptions, S3MultipartOptions, S3MultipartUpload, S3Object, S3ObjectHead,
    S3Workspace,
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
    NativeRootIdentity, NativeWatch, NativeWatchBackend, NativeWatchCapabilities, NativeWatchError,
    NativeWatchOptions, WatchBatch, WatchChange, WatchEpoch, WatchInvalidationReason,
    WatchSequence, native_watch_capabilities,
};
pub use workspace::{
    ApplyOptions, ChangeSet, Checkpoint, ForkOptions, Generation, GenerationPin, IdempotencyKey,
    JoinBuilder, JoinHistory, JoinOutcome, JoinPlan, Transaction, TransactionCommit,
    TransactionConflict, TransactionConflictRegion, TransactionDependencyUse, TransactionRebase,
    TransactionSparseSeek, Workspace, WorkspaceDelete, WorkspaceDirectoryEntry,
    WorkspaceDirectoryPage, WorkspaceError, WorkspaceExtentKind, WorkspaceExtentPlan,
    WorkspaceExtentSpan, WorkspaceId, WorkspaceMetadata, WorkspaceName, WorkspaceNameError,
    WorkspaceRebase, WorkspaceStat, WorkspaceSync,
};
