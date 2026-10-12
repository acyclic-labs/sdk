//! One native callback adapter for every embedded checkout consumer.

use super::view_gate::{CallbackOwner, ViewGate, ViewReadLease, ViewWriteLease};
use super::view_ledger::{ViewChange, ViewEffect, ViewLedger, ViewStamp};
use super::{
    CaptureOptions, MountAttributePage, MountAttributeWriteMode, MountDirectoryEntry,
    MountDirectoryPage, MountFilesystem, MountLookup, MountNode, MountNodeKind, MountOpenFile,
    MountPath, MountPublication, MountRangeAllocation, MountSeekTarget, MountSourceError,
    MountSparseRange, NativeMountError, ViewObserver, capture_root_identity,
};
use crate::kernel::{
    AttributeClass, AttributeName, ExtentSeekTarget, FileKind, FileMetadata, FilePayload,
    FileRecord, GenerationMutationError, LogicalName, NameEncoding, NamespacePath, RebaseDecision,
};
use crate::model::{CaseSensitivity, FilesystemProfile, VolumeConfig, VolumeLimits};
use crate::native_capture::{
    MAX_NATIVE_EXACT_CAPTURE_PATHS, NativeViewBaseline, capture_paths_batched,
    capture_subtrees_with_native_work,
};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, AuthoredMutation, ByteRange, CancellationToken,
    Checkout, ContentChange, ContentTimes, DetachedFile, FileId, FsError, NamedAttributeWriteMode,
    NativeRootIdentity, ObjectId, OperationFailure, OperationId, VolumeId, WorkBudget, WorkError,
};
use bytes::Bytes;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::OnceLock;
use std::sync::PoisonError;
use std::sync::Weak;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

const CALLBACK_TIMEOUT: Duration = Duration::from_secs(30);
/// Diff bound for recording an installed candidate exactly; a larger effect
/// is recorded as unenumerated.
const MAXIMUM_EXACT_INSTALL_CHANGES: u32 = 65_536;
const MAX_NATIVE_SUBTREE_CAPTURE_PATHS: u32 = 4_000_000;

#[derive(Clone, Copy)]
pub(super) enum HostCaptureScope {
    ExactPaths,
    Subtree,
}

#[derive(Default)]
pub(super) struct CaptureSourceGate {
    cancelled: bool,
    producers: u64,
    retired: bool,
    namespace_bound: bool,
    ever_used: bool,
}

/// Blocking native callback adapter over one async canonical checkout.
pub struct CheckoutMountSource<A, O> {
    checkout: Arc<SharedCheckout<A, O>>,
    root: NamespacePath,
    limits: VolumeLimits,
    profile: FilesystemProfile,
    /// Whether the volume resolves every case of a name to one entry.
    folds_names: bool,
    cancellation: CancellationToken,
    capture_gate: Arc<StdMutex<CaptureSourceGate>>,
    capture_quiesced: Arc<std::sync::Condvar>,
    runtime: CallbackRuntime,
    native_original: Option<crate::NativeMaterializationWork>,
    native_io_original: Option<crate::NativeMaterializationWork>,
    native_callback_guard: Option<Arc<dyn super::NativeMountCallbackGuard>>,
    cleanup_credit: StdMutex<
        Option<(
            crate::materializer::NativeWorkReservation,
            crate::materializer::NativeWorkReservation,
        )>,
    >,
    _namespace_allocation: Option<crate::NativeAllocation>,
    _source_allocation: Option<crate::NativeAllocation>,
}

/// The current instant as a content-change time, in signed Unix-epoch nanoseconds.
fn content_change_time() -> Result<i64, MountSourceError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|since| i64::try_from(since.as_nanos()).ok())
        .ok_or_else(|| MountSourceError::Engine("the clock is outside file time".into()))
}

/// The content change one native range allocation requests.
fn range_allocation<F>(range: ByteRange, operation: MountRangeAllocation) -> ContentChange<F> {
    match operation {
        MountRangeAllocation::PunchHole => ContentChange::ZeroRange {
            range,
            allocated: false,
            extend: false,
        },
        MountRangeAllocation::ZeroRange { extend } => ContentChange::ZeroRange {
            range,
            allocated: true,
            extend,
        },
        MountRangeAllocation::Preallocate { keep_size } => {
            ContentChange::Preallocate { range, keep_size }
        }
    }
}

fn engine_error(error: impl std::fmt::Display) -> MountSourceError {
    MountSourceError::Engine(error.to_string().into())
}

pub(super) fn capture_io_error(error: std::io::Error) -> MountSourceError {
    match crate::demand::native_demand_error(error) {
        crate::DemandError::Io(error) => engine_error(error),
        error => super::lazy::mount_demand_error(error),
    }
}

fn facade_error(error: OperationFailure<FsError>) -> MountSourceError {
    fs_error(error.error)
}

fn fs_error(error: FsError) -> MountSourceError {
    match error {
        FsError::CreationRejected | FsError::Mutation(GenerationMutationError::AlreadyExists) => {
            MountSourceError::AlreadyExists
        }
        FsError::NotFound
        | FsError::Mutation(
            GenerationMutationError::MissingParent | GenerationMutationError::MissingSource,
        ) => MountSourceError::NotFound,
        FsError::Work(error) => MountSourceError::Work(error),
        FsError::NativeKernel(error) => MountSourceError::NativeWork(error),
        FsError::Cancelled(_) => MountSourceError::Cancelled,
        FsError::SourceSnapshotStale => MountSourceError::Stale,
        other => MountSourceError::Filesystem(other),
    }
}

impl From<FsError> for MountSourceError {
    fn from(error: FsError) -> Self {
        fs_error(error)
    }
}

fn boundary_budget() -> WorkBudget {
    crate::performance::BOUNDARY_BUDGET
}

mod runtime;
pub use runtime::initialize_native_mount_services;
use runtime::invoke;
pub(super) use runtime::{CallbackRuntime, PreparedCallbackWait};
mod source_io;
use source_io::CaptureProducer;
pub use source_io::NativeSourceIoLease;
#[cfg(windows)]
pub(super) use source_io::namespace_gate_is_fresh;
include!("detached.rs");
include!("attached.rs");
include!("checkout.rs");
include!("publication.rs");
include!("publishing.rs");
include!("publication_guard.rs");
include!("candidate.rs");
include!("installation.rs");
include!("creation.rs");
include!("mutations.rs");
include!("attributes.rs");
include!("bindings.rs");
include!("namespace.rs");
include!("history.rs");
include!("source.rs");
mod source_factory;
include!("work.rs");
include!("locks.rs");
include!("view_changes.rs");
include!("inputs.rs");
include!("native_names.rs");
mod capture;
mod directory;
mod file_handles;
include!("operations.rs");
mod records;
mod sparse;
use records::mount_node;
#[cfg(test)]
mod tests;
