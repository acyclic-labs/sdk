macro_rules! unbounded_future {
    ($owner:expr, $operation:ident($($argument:expr),* $(,)?)) => {
        ($owner).$operation(
            $($argument,)*
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
    };
}

macro_rules! sdk_value {
    ($future:expr) => {
        ($future)
            .await
            .map_err(|failure| napi_error(failure.error))
            .map(|receipt| receipt.value)
    };
}

macro_rules! checkout_call {
    ($owner:ident, $checkout:ident, $operation:ident($($argument:expr),* $(,)?)) => {
        $checkout.$operation(
            $($argument,)*
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &$owner.cancellation,
        ).await.map_err(napi_error)
    };
}

macro_rules! checkout_mutation {
    ($owner:ident, $operation:ident($($argument:expr),* $(,)?)) => {{
        let mut checkout = Box::pin($owner.inner.lock()).await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        checkout.$operation(
            $($argument,)*
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &$owner.cancellation,
        ).await.map_err(napi_error)
    }};
}

mod authored;
pub use authored::*;
mod checkout;
pub use checkout::*;
mod context;
pub use context::*;
mod directory;
pub use directory::*;
mod engine;
pub use engine::*;
mod extents;
pub use extents::*;
mod history;
pub use history::*;
mod merge;
pub use merge::*;
mod metadata;
pub use metadata::*;
mod namespace;
mod native_view;
pub use native_view::*;
mod options;
pub use options::*;
mod publication;
pub use publication::*;
mod read;
pub use read::*;
mod records;
pub use records::*;
mod resolved;
pub use resolved::*;
mod source;
pub use source::*;
mod speculation;
pub use speculation::*;
#[cfg(test)]
mod tests;
mod transaction;
pub use transaction::*;
mod transfer;
pub use transfer::*;
mod volume;
pub use volume::*;
mod wire;
pub use wire::*;
mod workspace;
pub use workspace::*;
mod workspace_mount;
pub use workspace_mount::*;
mod write;

use acyclic_fs::kernel::{
    AttributeClass, AttributeName, DecodeLimits, ExtentKind, ExtentSeekTarget, FileKind,
    FileMetadata, FilePayload, FilePayloadKind, FileRecord, LogicalName, NameEncoding,
    NamespacePath, RebaseDecision, TransferCursor, TreeEntry, decode_file_metadata,
    encode_file_metadata,
};
use acyclic_fs::model::{
    AccessMode, CaseSensitivity, CheckoutMode, ConcurrencyMode, ConsistencyMode, FilesystemProfile,
    GenerationSelector, Lifecycle, MutationMode, UnicodePolicy, VolumeConfig, VolumeLimits,
};
use acyclic_fs::path::PortablePath;
use acyclic_fs::{
    ApplyOptions, AuthoredMutation, ByteRange, CancellationToken, ChangeSet, ConflictSide,
    DependencyRegion, DependencyState, DependencyUse, Digest, DirectoryPage, FileCloneRequest,
    FileId, ForkOptions, Generation, GenerationExportManifest, GenerationId, IdempotencyKey,
    JoinHistory, JoinOutcome, JoinPlan, LiveMutationOutcome, LivePublicationObservation,
    LocalAuthorityBackend, LocalCoreStateStore, LocalFs, LocalObjectBackend, LocalOptions,
    LocalVolume, MergeConflict, MergePreparation, NamedAttributeWriteMode,
    NativeWatch as FsNativeWatch, NativeWatchOptions, ObjectCacheOptions, ObjectId,
    ObjectReadRequest, ObjectResidency, OperationId, PromotionAdmission, PromotionDestination,
    PromotionSpeculatorOptions, RebaseConflict, ResidencyAdmission, ResidencyHint, ResidencyReason,
    ResidencySpeculatorOptions, ResolvedFile, SpeculationController, SpeculationOptions,
    StorageLocationId, StorageTier, Transaction, TransactionCommit, TransactionRebase, VolumeId,
    WatchBatch, WatchChange, WatchInvalidationReason, Workspace, WorkspaceDelete,
    WorkspaceExtentKind, WorkspaceExtentPlan, WorkspaceGraph, WorkspaceId, WorkspaceLineageRecord,
    WorkspaceMetadata, WorkspaceRebase, WorkspaceStat, decode_generation_export_manifest,
    encode_generation_export_manifest, native_watch_capabilities as sdk_native_watch_capabilities,
};
use acyclic_fs::{
    CaptureOptions, CaptureReceipt, CheckoutMountSource, MaterializeOptions, NativeMountRequest,
    NativeMountSession, SharedCheckout, WatchCaptureReceipt, capture_baseline, capture_paths,
    capture_root_identity, capture_watch_batch, materialize_checkout, mount_native,
    probe_native_mount, seal_checkout,
};
use acyclic_fs::{MountOptions, MountPublication};
use acyclic_fs::{ReconcileOutcome, SourceMode, SourceOptions, SourceState};
use napi::bindgen_prelude::{Array, AsyncTask, BigInt, Buffer, Error, PromiseRaw, Result, Status};
use napi::{Env, Task};
use napi_derive::napi;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
