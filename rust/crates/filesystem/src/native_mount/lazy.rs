//! Native projection of one demand-backed lazy workspace.

use super::adapter::{CallbackRuntime, CheckoutCandidate, CheckoutDetachedFile, InstallScope};
use super::view_gate::{
    ViewGate as SourceViewGate, ViewReadLease as SourceViewLease,
    ViewWriteLease as SourceMutationLease,
};
use super::view_ledger::{ViewChange, ViewEffect, ViewStamp};
use super::{
    CheckoutMountSource, ContentSink, MountAttributePage, MountAttributeWriteMode, MountContentPin,
    MountDirectoryEntry, MountDirectoryPage, MountFilesystem, MountLookup, MountNode,
    MountNodeKind, MountOpenFile, MountPath, MountRangeAllocation, MountSeekTarget,
    MountSourceError, ViewObserver, capture_root_identity,
};
use crate::LazySeekTarget;
use crate::demand::{
    DemandError, DemandFile, DemandSource, SourceChange, SourceChangeSink, SourceNode,
    SourceNodeKind, SourceReference, SourceWatch,
};
use crate::heap_future::in_heap;
use crate::kernel::{
    FileKind, FileMetadata, FileMutation, FilePayload, FileRecord, MetadataField, Mutation,
};
use crate::lazy_workspace::{InspectBasis, LazyReadonlyNode, LazyTreapWork};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, FileId, IdempotencyKey, LazyDirectoryCursor, LazyLookup,
    LazyWorkspace, LazyWorkspaceError, LazyWorkspaceStore, NativeRootIdentity, WorkBudget, WorkspaceMetadata,
};
use bytes::Bytes;
use acyclic_stream::OwnedBytes;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};

struct MountSyncWork<'w> {
    meter: super::adapter::NativeDemandCarrier<'w>,
    scratch_custody: Option<super::NativeMountAllocationCustody>,
    reserved_items: u64,
    observation: MountPublicationObservation,
    publication_snapshot: Option<crate::NativePublicationSnapshot>,
    accounting_failure: Option<MountSourceError>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum MountPublicationObservation { Ordinary, Qualified }

struct MountSyncBuffer<T: Default> {
    value: T,
    custody: Option<super::NativeMountAllocationCustody>,
    bytes: u64,
    native_allocation: Option<crate::NativeAllocation>,
}

impl<T: Default> MountSyncBuffer<T> {
    fn claim_additional(&mut self, bytes: u64, work: &mut MountSyncWork<'_>) -> Result<(), MountSourceError> {
        if bytes == 0 { return Ok(()); }
        let total = self.bytes.checked_add(bytes).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let custody = match &self.custody {
            Some(custody) => custody.clone(),
            None => {
                let custody = work.with_work(super::NativeMountAllocationCustody::new)
                    .map_err(mount_demand_error)?;
                self.custody = Some(custody.clone());
                custody
            }
        };
        work.with_work(|meter| custody.claim(bytes, meter)).map_err(mount_demand_error)?;
        self.bytes = total;
        Ok(())
    }

    fn absorb_buffer_guard<U: Default>(&mut self, other: &mut MountSyncBuffer<U>) -> Result<(), MountSourceError> {
        let total = self.bytes.checked_add(other.bytes).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        if let Some(other_allocation) = &mut other.native_allocation {
            if let Some(allocation) = &mut self.native_allocation {
                allocation.absorb(other_allocation).map_err(MountSourceError::NativeWork)?;
            } else {
                self.native_allocation = Some(other_allocation.split_off(other_allocation.bytes())
                    .map_err(MountSourceError::NativeWork)?);
            }
        }
        self.bytes = total;
        other.bytes = 0;
        Ok(())
    }

    fn take_native_parts(&mut self) -> Result<(T, crate::NativeAllocation), MountSourceError> {
        let allocation = self.native_allocation.take().ok_or(MountSourceError::NativeWork(
            crate::NativeTreeMaterializationError::MissingCustody,
        ))?;
        self.bytes = 0;
        Ok((std::mem::take(&mut self.value), allocation))
    }
}

impl MountSyncBuffer<Vec<String>> {
    fn push_cloned_text(&mut self, text: &str, work: &mut MountSyncWork<'_>) -> Result<(), MountSourceError> {
        let copied = crate::WorkCounters { bytes_copied: text.len() as u64, ..crate::WorkCounters::UNCHARGED };
        if let Some(original) = work.meter.original_native_work() {
            use super::adapter::CaptureInputScratch;
            let mut scratch = super::adapter::NativeCaptureScratchAllocation::new(&original)?;
            scratch.admit(copied)?;
            let mut bytes = scratch.vec::<u8>(text.len())?;
            bytes.extend_from_slice(text.as_bytes());
            scratch.record(copied)?;
            let mut owned = (String::from_utf8(bytes).expect("source String is UTF-8"), scratch.retain());
            let retained = owned.0.capacity() as u64;
            let total = self.bytes.checked_add(retained).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            if let Some(allocation) = &mut self.native_allocation {
                allocation.absorb(&mut owned.1).map_err(MountSourceError::NativeWork)?;
            } else {
                self.native_allocation = Some(owned.1.split_off(owned.1.bytes()).map_err(MountSourceError::NativeWork)?);
            }
            self.value.push(owned.0);
            self.bytes = total;
            Ok(())
        } else {
            work.admit(&copied)?;
            self.claim_additional(text.len() as u64, work)?;
            self.value.push(text.to_owned());
            work.record(copied).map_err(mount_demand_error)
        }
    }
}

impl<T: Default> std::ops::Deref for MountSyncBuffer<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target { &self.value }
}

impl<T: Default> std::ops::DerefMut for MountSyncBuffer<T> {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.value }
}

impl<T: Default> Drop for MountSyncBuffer<T> {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.value));
        if let Some(custody) = &self.custody {
            custody.retire(self.bytes).expect("mounted sync buffer ownership balances");
        }
    }
}

fn retain_sync_buffer<T: Default>(
    value: T, bytes: u64, work: &mut MountSyncWork<'_>,
) -> Result<MountSyncBuffer<T>, MountSourceError> {
    if bytes == 0 { return Ok(MountSyncBuffer { value, custody: None, bytes: 0, native_allocation: None }); }
    match work.retain_scratch(bytes) {
        Ok(custody) => Ok(MountSyncBuffer { value, custody: Some(custody), bytes, native_allocation: None }),
        Err(error) => {
            drop(value);
            work.release(bytes).map_err(mount_demand_error)?;
            Err(error)
        }
    }
}

impl<'w> MountSyncWork<'w> {
    fn new<A, O>(
        _source: &CheckoutMountSource<A, O>, mut meter: super::adapter::NativeDemandCarrier<'w>,
    ) -> Result<Self, MountSourceError> {
        meter.with_work(|work| work.check()).map_err(mount_demand_error)?;
        Ok(Self { meter, scratch_custody: None, reserved_items: 0,
            observation: MountPublicationObservation::Ordinary, publication_snapshot: None,
            accounting_failure: None })
    }

    fn with_work<T>(&mut self, operation: impl FnOnce(&mut crate::demand::DemandWork) -> T) -> T {
        self.meter.with_work(operation)
    }

    fn cancellation(&mut self) -> crate::CancellationToken { self.meter.cancellation() }

    fn retain_scratch(&mut self, bytes: u64) -> Result<super::NativeMountAllocationCustody, MountSourceError> {
        if self.scratch_custody.is_none() {
            let custody = self.meter.new_custody()?;
            self.scratch_custody = Some(custody);
        }
        let custody = self.scratch_custody.as_ref().expect("mounted scratch custody").clone();
        self.with_work(|work| custody.retain_admitted(bytes, work))
            .map_err(mount_demand_error)?;
        Ok(custody)
    }

    fn owned_vec<T>(&mut self, count: usize) -> Result<MountSyncBuffer<Vec<T>>, MountSourceError> {
        if let Some(original) = self.meter.original_native_work() {
            use super::adapter::CaptureInputScratch;
            let mut scratch = super::adapter::NativeCaptureScratchAllocation::new(&original)?;
            let value = scratch.vec::<T>(count)?;
            let bytes = value.capacity().checked_mul(std::mem::size_of::<T>())
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
            return Ok(MountSyncBuffer { value, custody: None, bytes, native_allocation: Some(scratch.retain()) });
        }
        let value = self.vec(count)?;
        let bytes = value.capacity().checked_mul(std::mem::size_of::<T>())
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
        retain_sync_buffer(value, bytes, self)
    }

    fn check(&mut self) -> Result<(), DemandError> { self.with_work(|work| work.check()) }
    fn remaining(&mut self) -> Result<crate::WorkBudget, DemandError> { self.with_work(|work| work.remaining()) }
    fn allocate(&mut self, bytes: u64) -> Result<(), DemandError> { self.with_work(|work| work.allocate(bytes)) }
    fn adopt(&mut self, bytes: u64) -> Result<(), DemandError> { self.with_work(|work| work.adopt(bytes)) }
    fn release(&mut self, bytes: u64) -> Result<(), DemandError> { self.with_work(|work| work.release(bytes)) }
    fn charge(&mut self, delta: crate::WorkCounters) -> Result<(), DemandError> { self.with_work(|work| work.charge(delta)) }
    fn record(&mut self, delta: crate::WorkCounters) -> Result<(), DemandError> { self.with_work(|work| work.record(delta)) }
    fn admit(&mut self, delta: &crate::WorkCounters) -> Result<(), MountSourceError> {
        if let Some(original) = self.meter.original_native_work() {
            original.admit(*delta).map_err(MountSourceError::NativeWork)
        } else {
            self.with_work(|work| work.work.admit(delta, &work.budget)).map_err(MountSourceError::Work)
        }
    }
    fn items(&mut self, count: usize) -> Result<(), MountSourceError> {
        self.with_work(|work| mount_items(work, count))
    }
    fn vec<T>(&mut self, count: usize) -> Result<Vec<T>, MountSourceError> {
        self.with_work(|work| mount_vec(count, work))
    }

    fn namespace_buffer(
        &mut self, path: &str, config: crate::VolumeConfig,
    ) -> Result<MountSyncBuffer<Option<crate::kernel::NamespacePath>>, MountSourceError> {
        if let Some(original) = self.meter.original_native_work() {
            let cancellation = self.cancellation();
            let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
            let owned = crate::kernel::NamespacePath::parse_in_profile_with_accounting(
                path, config.profile, config.limits, &mut accounting, &cancellation,
            ).map_err(MountSourceError::Filesystem)?;
            let allocation = match owned.1 {
                crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) => allocation,
                crate::kernel::allocation::KernelOwnedAllocation::Standalone => {
                    return Err(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::MissingCustody));
                }
            };
            let bytes = allocation.bytes();
            Ok(MountSyncBuffer { value: Some(owned.0), custody: None, bytes, native_allocation: Some(allocation) })
        } else {
            let namespace = self.with_work(|meter| mount_namespace(path, config, meter))?;
            let bytes = mount_namespace_bytes(&namespace);
            retain_sync_buffer(Some(namespace), bytes, self)
        }
    }
    fn accept<T, E>(
        &mut self, result: crate::MeasuredResult<crate::OperationReceipt<T>, E>,
        map_error: impl FnOnce(E) -> MountSourceError,
    ) -> Result<T, MountSourceError> {
        self.with_work(|work| mount_accept(work, result, map_error))
    }

    fn sdk_scope(&mut self) -> Result<(crate::WorkBudget, u64), MountSourceError> {
        self.with_work(|work| Ok((
            work.remaining().map_err(mount_demand_error)?, work.live_allocation_bytes(),
        )))
    }

    fn accept_sdk<T, E>(
        &mut self, result: crate::MeasuredResult<crate::OperationReceipt<T>, E>, prior_live: u64,
        map_error: impl FnOnce(E) -> MountSourceError,
    ) -> Result<T, MountSourceError> {
        self.meter.accept_with_prior_live(result, prior_live, map_error)
    }
    fn reserve_items(&mut self, count: u64) -> Result<(), MountSourceError> {
        let reserved = self.reserved_items.checked_add(count)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        self.with_work(|work| work.reserve(crate::materializer::NativeResourceCredit::Items, count)).map_err(mount_demand_error)?;
        self.reserved_items = reserved;
        Ok(())
    }
    fn restore_items(&mut self, count: u64) -> Result<(), MountSourceError> {
        let remaining = self.reserved_items.checked_sub(count)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        self.with_work(|work| work.refund(crate::materializer::NativeResourceCredit::Items, count).expect("original reservation is balanced"));
        self.reserved_items = remaining;
        Ok(())
    }
    fn complete_items(&mut self, count: u64) -> Result<(), MountSourceError> {
        let remaining = self.reserved_items.checked_sub(count)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        self.with_work(|work| work.complete(crate::materializer::NativeResourceCredit::Items, count, count)).map_err(mount_demand_error)?;
        self.reserved_items = remaining;
        Ok(())
    }

    async fn lock_checkout<'a, A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &mut self, source: &'a CheckoutMountSource<A, O>, cancellation: crate::CancellationToken,
    ) -> Result<super::adapter::SharedCheckoutGuard<'a, A, O>, MountSourceError> {
        source.shared_checkout().lock_with_carrier(
            &mut self.meter, &cancellation, Some(source.cancellation()),
        ).await
    }

    #[cfg(test)]
    async fn sync_authored<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &mut self, source: &CheckoutMountSource<A, O>, permit: crate::PublicationPermit,
        force: bool, cancellation: &crate::CancellationToken,
    ) -> Result<(), MountSourceError> {
        self.sync_authored_guarded(source, permit, force, cancellation,
            &mut source.sdk_publication_policy()).await
    }

    async fn sync_authored_guarded<A, O, F, Fut, E>(
        &mut self, source: &CheckoutMountSource<A, O>, permit: crate::PublicationPermit,
        force: bool, cancellation: &crate::CancellationToken, before_publication: &mut F,
    ) -> Result<(), E>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        source.sync_async_with_permit_carrier_guarded(
            permit, force, &mut self.meter, cancellation, before_publication,
        ).await
    }

    async fn sync_authored_context_guarded<A, O, F, Fut, E>(
        &mut self, source: &CheckoutMountSource<A, O>, permit: crate::PublicationPermit,
        force: bool, cancellation: &crate::CancellationToken, before_publication: &mut F,
        context: crate::lazy_workspace::NativeSnapshotContext,
    ) -> Result<(), E>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        let result = source.sync_async_with_permit_carrier_guarded_context_with_ack(
            permit, force, &mut self.meter, cancellation, before_publication,
            Some(context), &mut self.publication_snapshot,
        ).await;
        match result {
            Ok(snapshot) => self.publication_snapshot = Some(snapshot),
            Err((primary, accounting)) => {
                self.accounting_failure = accounting;
                return Err(primary);
            }
        }
        Ok(())
    }

    fn sync_authored_recovery_guarded<'a, A, O, F, Fut, E>(
        &'a mut self, source: &'a CheckoutMountSource<A, O>, permit: crate::PublicationPermit,
        cancellation: crate::CancellationToken, before_publication: &'a mut F,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        Box::pin(async move {
            if self.observation == MountPublicationObservation::Qualified
                && let Some(context) = source.retained_publication_context_with_carrier(
                    &mut self.meter, &cancellation,
                ).await?
            {
                self.sync_authored_context_guarded(
                    source, permit, true, &cancellation, before_publication, context,
                ).await
            } else {
                self.sync_authored_guarded(source, permit, true, &cancellation, before_publication).await
            }
        })
    }
}

impl Drop for MountSyncWork<'_> {
    fn drop(&mut self) {
        let reserved = std::mem::take(&mut self.reserved_items);
        self.meter.with_work(|work| {
            work.refund(crate::materializer::NativeResourceCredit::Items, reserved).expect("original reservation is balanced");
        });
    }
}

pub(super) fn mount_demand_error(error: DemandError) -> MountSourceError {
    match error {
        DemandError::Work(error) => MountSourceError::Work(error),
        DemandError::Cancelled => MountSourceError::Cancelled,
        DemandError::Mount(error) => error,
        DemandError::Io(error) => MountSourceError::Io(error, None),
        DemandError::Allocation(error) => MountSourceError::Allocation(error),
        DemandError::StaleSource | DemandError::StaleVersion | DemandError::StaleCursor =>
            MountSourceError::Stale,
        DemandError::Absent => MountSourceError::NotFound,
        DemandError::NotDirectory => MountSourceError::Filesystem(crate::FsError::NotDirectory),
        DemandError::WorkerUnavailable => MountSourceError::Engine(std::borrow::Cow::Borrowed("source demand failed: native source worker failed")),
        DemandError::NativeWorkUnavailable => MountSourceError::Engine(std::borrow::Cow::Borrowed("source demand failed: source does not support original native work custody")),
        DemandError::OriginalWorkPoisoned => MountSourceError::Engine(std::borrow::Cow::Borrowed("source demand failed: original native work state is poisoned")),
        DemandError::CursorBusy => MountSourceError::Engine(std::borrow::Cow::Borrowed("source demand failed: source directory cursor is in use")),
        DemandError::SourceUnavailable => MountSourceError::Engine(std::borrow::Cow::Borrowed("source demand failed: source root is unavailable")),
        DemandError::NotRegularFile => MountSourceError::Engine(std::borrow::Cow::Borrowed("source demand failed: source node is not a regular file")),
        DemandError::InvalidRequest => MountSourceError::Engine(std::borrow::Cow::Borrowed("source demand failed: source request is invalid")),
        #[cfg(all(target_os = "linux", any(feature = "native-mount", test)))]
        DemandError::LinuxMetadata(error) => MountSourceError::Metadata(error),
        #[cfg(all(target_os = "macos", any(feature = "native-mount", test)))]
        DemandError::MacMetadata(error) => MountSourceError::Metadata(error),
        #[cfg(all(windows, any(feature = "native-mount", test)))]
        DemandError::WindowsMetadata(error) => MountSourceError::Metadata(error),
        #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
        DemandError::SourceWatchStartup(owner) => MountSourceError::SourceWatchStartup(owner),
        #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
        DemandError::Observation(error) => mount_watch_error(error),
    }
}

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
fn mount_watch_error(error: crate::watch::NativeWatchError) -> MountSourceError {
    use crate::watch::NativeWatchError as WatchError;
    let reason = match error {
        WatchError::Io(error) => return MountSourceError::NativeWatchOriginalIo(error),
        WatchError::Backend(error) => return MountSourceError::NativeWatchOriginalBackend(error),
        WatchError::SourceStartup(owner) => return MountSourceError::SourceWatchStartup(owner),
        WatchError::SourceStartupFailed { cause, watch } =>
            return MountSourceError::NativeWatchSourceStartupFailed { cause, watch },
        WatchError::StartupFailed { cause, watch } =>
            return MountSourceError::NativeWatchStartupFailed { cause, watch },
        WatchError::NativeWork(error) => return MountSourceError::NativeWork(error),
        WatchError::Mount(error) => return error,
        #[cfg(all(any(target_os = "linux", target_os = "macos", windows), any(feature = "native-mount", test)))]
        WatchError::Metadata(error) => return MountSourceError::Metadata(error),
        WatchError::Cancelled(_) => return MountSourceError::Cancelled,
        WatchError::Work(error) => return MountSourceError::Work(error),
        WatchError::InvalidOptions => "filesystem engine failure: source demand failed: native watcher options are invalid",
        WatchError::BoundedObservationUnavailable => "filesystem engine failure: source demand failed: bounded native directory observation is unavailable",
        WatchError::RootIsNotDirectory => "filesystem engine failure: source demand failed: native watcher root is not a directory",
        WatchError::SymbolicLinkDirectory => "filesystem engine failure: source demand failed: native watcher directory traverses a symbolic link",
        WatchError::ObservedPathNotDirectory => "filesystem engine failure: source demand failed: native watcher observed path is not a directory",
        WatchError::UnrepresentablePath => "filesystem engine failure: source demand failed: native watcher directory is not representable on this host",
        WatchError::RootChanged => "filesystem engine failure: source demand failed: native watcher root changed identity",
        WatchError::StaleSource => "filesystem engine failure: source demand failed: source reference is stale",
        WatchError::StaleVersion => "filesystem engine failure: source demand failed: source node changed during the request",
        WatchError::StaleCursor => "filesystem engine failure: source demand failed: source directory cursor is expired or mismatched",
        WatchError::CursorBusy => "filesystem engine failure: source demand failed: source directory cursor is in use",
        WatchError::SourceUnavailable => "filesystem engine failure: source demand failed: source root is unavailable",
        WatchError::Absent => "filesystem engine failure: source demand failed: source path is absent",
        WatchError::NotRegularFile => "filesystem engine failure: source demand failed: source node is not a regular file",
        WatchError::NotDirectory => "filesystem engine failure: source demand failed: source node is not a directory",
        WatchError::InvalidRequest => "filesystem engine failure: source demand failed: source request is invalid",
        WatchError::WorkerUnavailable => "filesystem engine failure: source demand failed: native source worker failed",
        WatchError::NativeWorkUnavailable => "filesystem engine failure: source demand failed: source does not support original native work custody",
        WatchError::StatePoisoned => "filesystem engine failure: source demand failed: native watcher state is poisoned",
        WatchError::RescanAlreadyInProgress => "filesystem engine failure: source demand failed: native watcher baseline scan is already active",
        WatchError::NoRescanInProgress => "filesystem engine failure: source demand failed: native watcher baseline scan is not active",
        WatchError::RescanInProgress => "filesystem engine failure: source demand failed: native watcher baseline scan is in progress",
        WatchError::ZeroPollLimit => "filesystem engine failure: source demand failed: native watcher poll limit must be non-zero",
        WatchError::SequenceExhausted => "filesystem engine failure: source demand failed: native watcher sequence is exhausted",
    };
    MountSourceError::Engine(std::borrow::Cow::Borrowed(reason))
}

fn native_lazy_error(error: LazyWorkspaceError) -> MountSourceError {
    match error {
        LazyWorkspaceError::Demand(error) => mount_demand_error(error),
        LazyWorkspaceError::NotRegularFile => MountSourceError::Invalid(std::borrow::Cow::Borrowed("path is not a regular file")),
        LazyWorkspaceError::InvalidPageBound => MountSourceError::Invalid(std::borrow::Cow::Borrowed("directory page bound must be positive")),
        LazyWorkspaceError::UnsupportedNode => MountSourceError::Unsupported(std::borrow::Cow::Borrowed("source node cannot be represented by the authored workspace")),
        LazyWorkspaceError::TooLarge => MountSourceError::Engine(std::borrow::Cow::Borrowed("file exceeds the requested read bound")),
        error => lazy_error(error),
    }
}

fn accept_native_content(
    result: Result<crate::kernel::OwnedSourceChunk,
        (LazyWorkspaceError, Option<crate::NativeAllocation>)>,
) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
    match result {
        Ok(packet) => Ok(packet),
        Err((error, request)) => {
            let cause = native_lazy_error(error);
            drop(request);
            Err(cause)
        }
    }
}

fn source_callback_error(error: MountSourceError) -> DemandError {
    match error {
        MountSourceError::Work(error) => DemandError::Work(error),
        MountSourceError::Cancelled => DemandError::Cancelled,
        error => DemandError::Mount(error),
    }
}

pub(super) fn mount_wait_error(error: crate::cancellation::CancellationWaitError) -> MountSourceError {
    match error {
        crate::cancellation::CancellationWaitError::Work(error) => MountSourceError::Work(error),
        crate::cancellation::CancellationWaitError::AllocationFailed =>
            MountSourceError::Engine(std::borrow::Cow::Owned("cancellation waiter allocation failed".to_owned())),
        crate::cancellation::CancellationWaitError::Native(error) => MountSourceError::NativeWork(error),
    }
}

pub(super) fn mount_accept<T, E>(
    work: &mut crate::demand::DemandWork,
    result: crate::MeasuredResult<crate::OperationReceipt<T>, E>,
    map: impl FnOnce(E) -> MountSourceError,
) -> Result<T, MountSourceError> {
    match result {
        Ok(receipt) => {
            work.merge(receipt.work).map_err(mount_demand_error)?;
            Ok(receipt.value)
        }
        Err(failure) => {
            work.merge(*failure.work).map_err(mount_demand_error)?;
            Err(map(failure.error))
        }
    }
}

pub(super) fn mount_items(work: &mut crate::demand::DemandWork, count: usize) -> Result<(), MountSourceError> {
    work.charge(crate::WorkCounters {
        items_examined: count as u64,
        ..crate::WorkCounters::default()
    }).map_err(mount_demand_error)
}

pub(super) fn mount_vec<T>(
    count: usize,
    work: &mut crate::demand::DemandWork,
) -> Result<Vec<T>, MountSourceError> {
    let bytes = count.checked_mul(std::mem::size_of::<T>())
        .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
    work.allocate(bytes as u64).map_err(mount_demand_error)?;
    let mut values = Vec::new();
    if values.try_reserve_exact(count).is_err() {
        work.release(bytes as u64).map_err(mount_demand_error)?;
        return Err(MountSourceError::Engine(std::borrow::Cow::Owned("native preparation allocation failed".to_owned())));
    }
    Ok(values)
}

fn mount_path_text_measure(
    root: &str, path: &MountPath,
    mut examine: impl FnMut() -> Result<(), MountSourceError>,
    invalid: fn(&'static str) -> MountSourceError,
    unsupported: fn(&'static str) -> MountSourceError,
) -> Result<(usize, u64), MountSourceError> {
    let prefix = if path.components().is_empty() || root != "/" { root } else { "" };
    let mut length = prefix.len();
    let mut fill_items = 0_u64;
    for component in path.components() {
        examine()?;
        fill_items = fill_items.checked_add(1).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        length = length.checked_add(1).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        if cfg!(target_os = "windows") {
            let units = crate::kernel::types::utf16le_units(component)
                .ok_or_else(|| invalid("native UTF-16 name has an odd byte length"))?;
            for scalar in char::decode_utf16(units) {
                examine()?;
                fill_items = fill_items.checked_add(1).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
                let scalar = scalar.map_err(|_| unsupported(
                    "lazy portable mount cannot represent an unpaired UTF-16 name",
                ))?;
                length = length.checked_add(scalar.len_utf8())
                    .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            }
        } else {
            std::str::from_utf8(component).map_err(|_| unsupported(
                "lazy portable mount cannot represent a non-UTF-8 name",
            ))?;
            length = length.checked_add(component.len())
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        }
    }
    Ok((length, fill_items))
}

fn mount_path_text_fill(root: &str, path: &MountPath, mut bytes: Vec<u8>) -> String {
    let prefix = if path.components().is_empty() || root != "/" { root } else { "" };
    bytes.extend_from_slice(prefix.as_bytes());
    for component in path.components() {
        bytes.push(b'/');
        if cfg!(target_os = "windows") {
            let units = crate::kernel::types::utf16le_units(component)
                .expect("the immutable native name was validated before allocation");
            for scalar in char::decode_utf16(units) {
                let scalar = scalar.expect("the immutable UTF-16 scalar was validated before allocation");
                let mut encoded = [0_u8; 4];
                bytes.extend_from_slice(scalar.encode_utf8(&mut encoded).as_bytes());
            }
        } else {
            bytes.extend_from_slice(component);
        }
    }
    String::from_utf8(bytes).expect("the path contains only validated UTF-8")
}

fn mount_path_text(
    root: &str, path: &MountPath, work: &mut crate::demand::DemandWork,
) -> Result<String, MountSourceError> {
    let (length, fill_items) = mount_path_text_measure(
        root, path, || mount_items(work, 1),
        |text| MountSourceError::Invalid(std::borrow::Cow::Owned(text.to_owned())),
        |text| MountSourceError::Unsupported(std::borrow::Cow::Owned(text.to_owned())),
    )?;
    let copied = crate::WorkCounters {
        bytes_copied: length as u64, items_examined: fill_items,
        ..crate::WorkCounters::UNCHARGED
    };
    work.work.admit(&copied, &work.budget).map_err(MountSourceError::Work)?;
    let bytes = mount_vec::<u8>(length, work)?;
    let text = mount_path_text_fill(root, path, bytes);
    work.record(copied).map_err(mount_demand_error)?;
    Ok(text)
}

fn mount_path_text_with_carrier(
    root: &str, path: &MountPath,
    carrier: &mut super::adapter::NativeDemandCarrier<'_>,
) -> Result<MountOwnedBuffer<String>, MountSourceError> {
    let Some(original) = carrier.original_native_work() else {
        return carrier.with_work(|work| mount_path_text(root, path, work))
            .map(|text| MountOwnedBuffer { legacy_bytes: text.capacity() as u64, value: text, allocation: None });
    };
    let (length, fill_items) = mount_path_text_measure(
        root, path,
        || original.charge(crate::WorkCounters {
            items_examined: 1, ..crate::WorkCounters::UNCHARGED
        }).map_err(MountSourceError::NativeWork),
        |text| MountSourceError::Invalid(std::borrow::Cow::Borrowed(text)),
        |text| MountSourceError::Unsupported(std::borrow::Cow::Borrowed(text)),
    )?;
    let copied = original.reserve_work(crate::WorkCounters { bytes_copied: length as u64, ..crate::WorkCounters::UNCHARGED }).map_err(MountSourceError::NativeWork)?;
    let mut examined = original.reserve_work(crate::WorkCounters { items_examined: fill_items, ..crate::WorkCounters::UNCHARGED }).map_err(MountSourceError::NativeWork)?;
    let allocation = original.claim(length as u64, u64::from((length as u64) != 0), crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(length).map_err(MountSourceError::Allocation)?;
    let text = mount_path_text_fill(root, path, bytes);
    let recorded_copy = { let mut copy = copied; copy.record_completed(crate::WorkCounters { bytes_copied: length as u64, ..crate::WorkCounters::UNCHARGED }) }.map_err(MountSourceError::NativeWork);
    let recorded_items = examined.record_completed(crate::WorkCounters { items_examined: fill_items, ..crate::WorkCounters::UNCHARGED }).map_err(MountSourceError::NativeWork);
    recorded_copy.and(recorded_items)?;
    Ok(MountOwnedBuffer { value: text, legacy_bytes: 0, allocation: Some(allocation) })
}

fn mount_namespace(
    path: &str,
    config: crate::VolumeConfig,
    work: &mut crate::demand::DemandWork,
) -> Result<crate::kernel::NamespacePath, MountSourceError> {
    let mut owned_bytes = 0_u64;
    let prepared = (|| {
        if !path.starts_with('/') || (path != "/" && path.ends_with('/')) {
            return Err(MountSourceError::Invalid(std::borrow::Cow::Owned("noncanonical removal path".to_owned())));
        }
        let mut count = 0_usize;
        for name in path.split('/').skip(1) {
            if name.is_empty() && path == "/" { continue; }
            mount_items(work, 1)?;
            count = count.checked_add(1).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        }
        let mut names = mount_vec(count, work)?;
        owned_bytes = (names.capacity() * std::mem::size_of::<crate::kernel::LogicalName>()) as u64;
        for name in path.split('/').skip(1) {
            if name.is_empty() && path == "/" { continue; }
            mount_items(work, 1)?;
            let (encoding, length) = match config.profile {
                crate::FilesystemProfile::Portable | crate::FilesystemProfile::Browser =>
                    (crate::kernel::NameEncoding::Utf8, name.len()),
                crate::FilesystemProfile::Posix =>
                    (crate::kernel::NameEncoding::PosixBytes, name.len()),
                crate::FilesystemProfile::Windows =>
                    (crate::kernel::NameEncoding::WindowsUtf16Le, name.encode_utf16().count() * 2),
            };
            let mut bytes = mount_vec::<u8>(length, work)?;
            owned_bytes = owned_bytes.checked_add(bytes.capacity() as u64)
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            work.charge(crate::WorkCounters { bytes_copied: length as u64, ..crate::WorkCounters::default() })
                .map_err(mount_demand_error)?;
            if encoding == crate::kernel::NameEncoding::WindowsUtf16Le {
                bytes.extend(name.encode_utf16().flat_map(u16::to_le_bytes));
            } else {
                bytes.extend_from_slice(name.as_bytes());
            }
            names.push(crate::kernel::LogicalName::new(encoding, bytes, config.limits.maximum_component_bytes)
                .map_err(|error| MountSourceError::Invalid(std::borrow::Cow::Owned(error.to_string())))?);
        }
        crate::kernel::NamespacePath::new(names, config.limits)
            .map_err(|error| MountSourceError::Invalid(std::borrow::Cow::Owned(error.to_string())))
    })();
    if prepared.is_err() { work.release(owned_bytes).map_err(mount_demand_error)?; }
    prepared
}

pub(super) fn mount_namespace_bytes(path: &crate::kernel::NamespacePath) -> u64 {
    (path.depth() * std::mem::size_of::<crate::kernel::LogicalName>()) as u64
        + if path.is_root() { 0 } else { u64::from(path.encoded_bytes()) - path.depth() as u64 }
}

fn mount_effect_bytes(effect: &ViewEffect) -> u64 {
    (effect.nodes.capacity() * std::mem::size_of::<FileId>()
        + effect.names.capacity() * std::mem::size_of::<(crate::kernel::NamespacePath, crate::kernel::LogicalName)>()
        + (effect.subtrees.capacity() + effect.listings.capacity()) * std::mem::size_of::<crate::kernel::NamespacePath>()) as u64
        + effect.names.iter().map(|(path, name)| mount_namespace_bytes(path) + name.as_bytes().len() as u64).sum::<u64>()
        + effect.subtrees.iter().chain(&effect.listings).map(mount_namespace_bytes).sum::<u64>()
}

pub(super) fn mount_clone_namespace(
    names: &[crate::kernel::LogicalName],
    limits: crate::VolumeLimits,
    work: &mut crate::demand::DemandWork,
) -> Result<crate::kernel::NamespacePath, MountSourceError> {
    let mut owned_bytes = 0_u64;
    let prepared = (|| {
        let mut owned = mount_vec(names.len(), work)?;
        owned_bytes = (owned.capacity() * std::mem::size_of::<crate::kernel::LogicalName>()) as u64;
        for name in names {
            mount_items(work, 1)?;
            let copied = crate::WorkCounters {
                bytes_copied: name.as_bytes().len() as u64, ..crate::WorkCounters::default()
            };
            work.work.admit(&copied, &work.budget).map_err(MountSourceError::Work)?;
            work.allocate(name.as_bytes().len() as u64).map_err(mount_demand_error)?;
            let name = name.clone();
            owned_bytes = owned_bytes.checked_add(name.as_bytes().len() as u64)
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            work.record(copied).map_err(mount_demand_error)?;
            owned.push(name);
        }
        crate::kernel::NamespacePath::new(owned, limits)
            .map_err(|error| MountSourceError::Invalid(std::borrow::Cow::Owned(error.to_string())))
    })();
    if prepared.is_err() { work.release(owned_bytes).map_err(mount_demand_error)?; }
    prepared
}

pub(super) fn mount_engine_error(error: crate::FsError) -> MountSourceError {
    mount_bounded_error(&error).unwrap_or(MountSourceError::Filesystem(error))
}

fn mount_bounded_error(error: &(dyn std::error::Error + 'static)) -> Option<MountSourceError> {
    match crate::workspace::workspace_bounded_error(error) {
        Some(crate::WorkspaceError::Work(error)) => Some(MountSourceError::Work(error)),
        Some(crate::WorkspaceError::Cancelled(_)) => Some(MountSourceError::Cancelled),
        _ => None,
    }
}

fn mount_record_items(
    carrier: &mut super::adapter::NativeDemandCarrier<'_>, items: usize,
) -> Result<(), MountSourceError> {
    if let Some(original) = carrier.original_native_work() {
        original.charge(crate::WorkCounters { items_examined: items as u64, ..crate::WorkCounters::UNCHARGED })
            .map_err(MountSourceError::NativeWork)
    } else {
        carrier.with_work(|work| mount_items(work, items))
    }
}

fn mount_record_copy(
    carrier: &mut super::adapter::NativeDemandCarrier<'_>, bytes: u64, copy: impl FnOnce(),
) -> Result<(), MountSourceError> {
    if let Some(original) = carrier.original_native_work() {
        let reserved = original.reserve_work(crate::WorkCounters { bytes_copied: bytes, ..crate::WorkCounters::UNCHARGED }).map_err(MountSourceError::NativeWork)?;
        copy();
        { let mut copy = reserved; copy.record_completed(crate::WorkCounters { bytes_copied: bytes, ..crate::WorkCounters::UNCHARGED }) }.map_err(MountSourceError::NativeWork)
    } else {
        carrier.with_work(|work| work.charge(crate::WorkCounters { bytes_copied: bytes, ..crate::WorkCounters::UNCHARGED }))
            .map_err(mount_demand_error)?;
        copy();
        Ok(())
    }
}

fn mount_record_union<'a, I, J, K>(
    captured: I,
    candidate: J,
    dirty: K,
    mut output: Option<&mut Vec<(FileRecord, FileMetadata)>>,
    work: &mut super::adapter::NativeDemandCarrier<'_>,
) -> Result<usize, MountSourceError>
where
    I: ExactSizeIterator<Item = (&'a FileRecord, &'a FileMetadata)>,
    J: ExactSizeIterator<Item = (&'a FileRecord, &'a FileMetadata)>,
    K: ExactSizeIterator<Item = (&'a FileRecord, &'a FileMetadata)>,
{
    let (mut captured, mut candidate, mut dirty) =
        (captured.peekable(), candidate.peekable(), dirty.peekable());
    let mut count = 0_usize;
    loop {
        let present = usize::from(captured.len() != 0)
            + usize::from(candidate.len() != 0) + usize::from(dirty.len() != 0);
        if present == 0 { return Ok(count); }
        mount_record_items(work, present)?;
        let first = captured.peek().map(|(record, _)| record.file_id);
        let second = candidate.peek().map(|(record, _)| record.file_id);
        let third = dirty.peek().map(|(record, _)| record.file_id);
        let id = [first, second, third].into_iter().flatten().min().ok_or(MountSourceError::Stale)?;
        let mut selected = None;
        if first == Some(id) { selected = captured.next(); }
        if second == Some(id) { selected = candidate.next(); }
        if third == Some(id) { selected = dirty.next(); }
        if let Some(output) = &mut output {
            let (record, metadata) = selected.ok_or(MountSourceError::Stale)?;
            mount_record_copy(work, std::mem::size_of::<(FileRecord, FileMetadata)>() as u64,
                || output.push((*record, *metadata)))?;
        }
        count = count.checked_add(1).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
    }
}

fn mount_upsert_record(
    records: &mut Vec<(FileRecord, FileMetadata)>,
    record: (FileRecord, FileMetadata),
    work: &mut super::adapter::NativeDemandCarrier<'_>,
) -> Result<(), MountSourceError> {
    let (mut low, mut high) = (0, records.len());
    while low < high {
        mount_record_items(work, 1)?;
        let middle = low + (high - low) / 2;
        let existing = records.get(middle).ok_or(MountSourceError::Stale)?;
        match existing.0.file_id.cmp(&record.0.file_id) {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => {
                *records.get_mut(middle).ok_or(MountSourceError::Stale)? = record;
                return Ok(());
            }
        }
    }
    mount_record_copy(work, ((records.len() - low) * std::mem::size_of::<(FileRecord, FileMetadata)>()) as u64,
        || records.insert(low, record))?;
    Ok(())
}

const MAXIMUM_PROMOTION_BYTES: u64 = u64::MAX;
const MAXIMUM_LAZY_DIRECTORY_CURSORS: usize = 1_024;
const DIRECTORY_PROMOTION_PAGE: u32 = 256;
const MAXIMUM_REMEMBERED_RESOLUTIONS: usize = 65_536;
const MAXIMUM_PROMOTION_RETRIES: usize = 3;
const MAXIMUM_INTERRUPTED_LISTINGS: usize = 3;

async fn stage_mount_promotion<A, O, D, S>(
    lazy: &LazyWorkspace<A, O, D, S>,
    authored: &CheckoutMountSource<A, O>,
    path: &str,
    mounted: &MountPath,
    expected_source: FileId,
) -> Result<(), MountSourceError>
where
    A: AsyncAuthorityStore + Send + Sync,
    O: AsyncObjectStore + Send + Sync,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    let mut work = crate::demand::DemandWork::new(
        crate::WorkBudget::UNBOUNDED, authored.cancellation(),
    );
    stage_mount_promotion_with_carrier(
        lazy, authored, path, mounted, expected_source,
        &mut super::adapter::NativeDemandCarrier::Borrowed(&mut work),
    ).await
}

async fn stage_mount_promotion_with_carrier<A, O, D, S>(
    lazy: &LazyWorkspace<A, O, D, S>,
    authored: &CheckoutMountSource<A, O>,
    path: &str,
    mounted: &MountPath,
    expected_source: FileId,
    carrier: &mut super::adapter::NativeDemandCarrier<'_>,
) -> Result<(), MountSourceError>
where
    A: AsyncAuthorityStore + Send + Sync,
    O: AsyncObjectStore + Send + Sync,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    let mut work = MountSyncWork::new(authored, carrier.reborrow())?;
    let cancellation = work.cancellation();
    for _ in 0..MAXIMUM_PROMOTION_RETRIES {
        work.items(1)?;
        authored.cancellation().check().map_err(|_| MountSourceError::Stale)?;
        let (mut candidate, epoch, stamp) = authored.shared_checkout().prepare_native_phase_with_carrier(
            &mut work.meter, &cancellation, Some(authored.cancellation()),
        ).await?;
        let changed = if let Some(original) = work.meter.original_native_work() {
            lazy.stage_into_checkout(
                &mut candidate.checkout, None, path, expected_source, MAXIMUM_PROMOTION_BYTES,
                &LazyTreapWork::native_with_work(&original, &cancellation).map_err(native_lazy_error)?,
            ).await.map_err(native_lazy_error)?
        } else {
            let accounting = LazyTreapWork::new(work.remaining().map_err(mount_demand_error)?, &cancellation);
            let result = lazy.stage_into_checkout(
                &mut candidate.checkout, None, path, expected_source, MAXIMUM_PROMOTION_BYTES, &accounting,
            ).await;
            let result = accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure());
            let changed = work.accept(result, lazy_error)?;
            work.with_work(|work| work.retain_checkout_control(
                &candidate.checkout.private_control_custody(),
            )).map_err(mount_demand_error)?;
            changed
        };
        if !changed { return Ok(()); }
        let mut scratch = DirectoryCarrierScratch::new(&mut work.meter);
        let installed = promotion_effect_with_carrier(
            lazy, authored, &candidate, path, mounted, scratch.carrier,
        ).await?;
        scratch.track(installed.legacy_owned_bytes())?;
        let installed_root = authored.shared_checkout().finish_phase(
            candidate, epoch, stamp, scratch.carrier, &cancellation, Some(authored.cancellation()),
            |carrier| super::adapter::record_view_change(&authored.shared_checkout().ledger,
                &ViewChange::Effect(&installed), carrier,
            ), None, |publication, _| publication,
        ).await?;
        if installed_root.is_some() {
            return authored.shared_checkout().publish_mutation_with_carrier(
                scratch.carrier, &cancellation, Some(authored.cancellation()),
                authored.callback_guard(),
            ).await;
        }
    }
    Err(MountSourceError::Stale)
}

async fn promotion_effect_with_carrier<A, O, D, S>(
    lazy: &LazyWorkspace<A, O, D, S>,
    authored: &CheckoutMountSource<A, O>,
    candidate: &CheckoutCandidate<A, O>,
    path: &str,
    mounted: &MountPath,
    carrier: &mut super::adapter::NativeDemandCarrier<'_>,
) -> Result<MountOwnedBuffer<ViewEffect>, MountSourceError>
where
    A: AsyncAuthorityStore + Send + Sync,
    O: AsyncObjectStore + Send + Sync,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    let mut scratch = DirectoryCarrierScratch::new(carrier);
    let levels = mounted.components().len().checked_add(1)
        .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
    let node_slots = levels.checked_mul(2)
        .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
    let mut installed = ViewEffect::default();
    installed.nodes = scratch.vec::<FileId>(node_slots)?;
    installed.listings = scratch.vec::<crate::kernel::NamespacePath>(levels)?;
    let mut prefix = mount_components_with_carrier(mounted.components(), None, scratch.carrier)?;
    scratch.track(prefix.legacy_owned_bytes())?;
    let mut text = path;
    loop {
        scratch.carrier.with_work(|work| mount_items(work, 1))?;
        let after = authored.lookup_in_with_carrier(&candidate.checkout, &prefix, scratch.carrier).await?;
        let base = authored.lookup_in_with_carrier(&candidate.base, &prefix, scratch.carrier).await?;
        if base.is_none() && after.is_some() && !prefix.components().is_empty() {
            let parent = mount_components_with_carrier(
                &prefix.components()[..prefix.components().len() - 1], None, scratch.carrier,
            )?;
            scratch.track(parent.legacy_owned_bytes())?;
            let mut namespace = authored.namespace_path(&parent, scratch.carrier)?;
            if let Some(allocation) = &mut namespace.1 {
                scratch.absorb_native(allocation)?;
            } else {
                scratch.track(namespace.0.retained_bytes())?;
            }
            installed.listings.push(namespace.0);
            let bytes = parent.legacy_owned_bytes();
            drop(parent);
            scratch.release(bytes)?;
        }
        let before = match base {
            Some(lookup) => Some(lookup),
            None => {
                let cancellation = scratch.carrier.cancellation();
                if let Some(original) = scratch.carrier.original_native_work() {
                    match lazy.inspect_unauthored(text, InspectBasis::default(),
                        &LazyTreapWork::native_with_work(&original, &cancellation).map_err(native_lazy_error)?).await {
                        Ok(LazyReadonlyNode::Native(owned)) => {
                            let ((lookup, _), allocation) = owned.into_parts();
                            let lookup = MountOwnedBuffer { value: lookup, legacy_bytes: 0, allocation: Some(allocation) };
                            let file_id = lazy.stable_file_id_for_lookup(
                                text, &lookup.value, &mut crate::lazy_workspace::LazyReadonlyAccounting::Native { original: &original, cancellation: &cancellation },
                            ).await.map_err(native_lazy_error)?;
                            Some(mount_lookup_ref(&lookup.value, file_id))
                        }
                        Ok(_) => unreachable!("native inspection retains native funding"),
                        Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::NotFound) => None,
                        Err(error) => return Err(native_lazy_error(error)),
                    }
                } else {
                    let budget = scratch.carrier.with_work(|work| work.remaining()).map_err(mount_demand_error)?;
                    let accounting = LazyTreapWork::new(budget, &cancellation);
                    let result = lazy.inspect_unauthored(text, InspectBasis::default(), &accounting).await;
                    let result = accounting.measured(result, |error, work| error.with_prior_work(work).into_measured_failure());
                    match scratch.carrier.with_work(|work| mount_accept(work, result, lazy_error)) {
                        Ok(LazyReadonlyNode::Portable((lookup, _))) => {
                            let owned = lookup.logical_owned_bytes();
                            scratch.track(owned)?;
                            scratch.carrier.with_work(|work| work.record_adoption(owned, &None)).map_err(mount_demand_error)?;
                            let budget = scratch.carrier.with_work(|work| work.remaining()).map_err(mount_demand_error)?;
                            let mut accounting = crate::lazy_workspace::LazyReadonlyAccounting::Portable {
                                work: crate::WorkCounters::UNCHARGED, budget, cancellation: &cancellation,
                            };
                            let result = lazy.stable_file_id_for_lookup(text, &lookup, &mut accounting).await;
                            let result = accounting.finish_measured(result).map_err(LazyWorkspaceError::into_measured_failure);
                            let file_id = scratch.carrier.with_work(|work| mount_accept(work, result, lazy_error));
                            let projected = file_id.map(|file_id| mount_lookup(lookup, file_id));
                            scratch.release(owned)?;
                            Some(projected?)
                        }
                        Ok(_) => unreachable!("ordinary inspection uses ordinary funding"),
                        Err(MountSourceError::NotFound) => None,
                        Err(error) => return Err(error),
                    }
                }
            }
        };
        if before != after {
            let copies = ((u64::from(before.is_some()) + u64::from(after.is_some()))
                * std::mem::size_of::<FileId>() as u64) as u64;
            let copy = if let Some(original) = scratch.carrier.original_native_work() {
                Some(original.reserve_work(crate::WorkCounters { bytes_copied: copies, ..crate::WorkCounters::UNCHARGED }).map_err(MountSourceError::NativeWork)?)
            } else { None };
            installed.nodes.extend([before, after].into_iter().flatten().map(|lookup| lookup.node.file_id));
            if let Some(copy) = copy { { let mut copy = copy; copy.record_completed(crate::WorkCounters { bytes_copied: copies, ..crate::WorkCounters::UNCHARGED }) }.map_err(MountSourceError::NativeWork)?; }
        }
        let Some(component) = prefix.value.components.pop() else { break; };
        let bytes = component.capacity() as u64;
        drop(component);
        if let Some(allocation) = &mut prefix.allocation {
            allocation.release_deferred(bytes);
        } else {
            prefix.legacy_bytes -= bytes;
            scratch.release(bytes)?;
        }
        text = match text.rsplit_once('/') { Some(("", _)) | None => "/", Some((parent, _)) => parent };
    }
    let prefix_bytes = prefix.legacy_owned_bytes();
    drop(prefix);
    scratch.release(prefix_bytes)?;
    if let Some(original) = scratch.carrier.original_native_work() {
        original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
    }
    let bytes = scratch.bytes;
    scratch.keep(bytes)?;
    Ok(MountOwnedBuffer { value: installed, legacy_bytes: bytes, allocation: scratch.native_backing.take() })
}

struct DirectoryScratch<'a> {
    work: &'a mut crate::demand::DemandWork,
    bytes: u64,
}

impl<'a> DirectoryScratch<'a> {
    fn new(work: &'a mut crate::demand::DemandWork) -> Self { Self { work, bytes: 0 } }

    fn track(&mut self, bytes: u64) -> Result<(), MountSourceError> {
        self.bytes = self.bytes.checked_add(bytes)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        Ok(())
    }

    fn keep(&mut self, bytes: u64) -> Result<(), MountSourceError> {
        self.bytes = self.bytes.checked_sub(bytes)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        Ok(())
    }

    fn release(&mut self, bytes: u64) -> Result<(), MountSourceError> {
        self.keep(bytes)?;
        self.work.release(bytes).map_err(mount_demand_error)
    }
}

impl Drop for DirectoryScratch<'_> {
    fn drop(&mut self) {
        self.work.release(self.bytes).expect("directory scratch releases only its original allocations");
    }
}

struct DirectoryCarrierScratch<'a, 'w> {
    carrier: &'a mut super::adapter::NativeDemandCarrier<'w>,
    bytes: u64,
    reserved_items: u64,
    reserved_copies: u64,
    finalization: Option<(Box<Option<MountSourceError>>, Box<Option<crate::demand::DemandError>>, Option<crate::NativeAllocation>)>,
    native_backing: Option<crate::NativeAllocation>,
}

impl<'a, 'w> DirectoryCarrierScratch<'a, 'w> {
    fn new(carrier: &'a mut super::adapter::NativeDemandCarrier<'w>) -> Self {
        Self { carrier, bytes: 0, reserved_items: 0, reserved_copies: 0, finalization: None, native_backing: None }
    }

    fn track(&mut self, bytes: u64) -> Result<(), MountSourceError> {
        self.bytes = self.bytes.checked_add(bytes)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        Ok(())
    }

    fn keep(&mut self, bytes: u64) -> Result<(), MountSourceError> {
        self.bytes = self.bytes.checked_sub(bytes)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        Ok(())
    }

    fn release(&mut self, bytes: u64) -> Result<(), MountSourceError> {
        self.keep(bytes)?;
        self.carrier.with_work(|work| work.release(bytes)).map_err(mount_demand_error)
    }

    fn release_backing(&mut self, bytes: u64) -> Result<(), MountSourceError> {
        if self.carrier.original_native_work().is_some() {
            if let Some(allocation) = &mut self.native_backing {
                allocation.release_deferred(bytes);
            } else if bytes != 0 {
                return Err(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::MissingCustody));
            }
            Ok(())
        } else {
            self.release(bytes)
        }
    }

    fn absorb_native(&mut self, allocation: &mut crate::NativeAllocation) -> Result<(), MountSourceError> {
        if let Some(backing) = &mut self.native_backing {
            backing.absorb(allocation).map_err(MountSourceError::NativeWork)
        } else {
            self.native_backing = Some(allocation.split_off(allocation.bytes()).map_err(MountSourceError::NativeWork)?);
            Ok(())
        }
    }

    fn vec<T>(&mut self, count: usize) -> Result<Vec<T>, MountSourceError> {
        if let Some(original) = self.carrier.original_native_work() {
            use super::adapter::CaptureInputScratch;
            let mut scratch = super::adapter::NativeCaptureScratchAllocation::new(&original)?;
            let mut owned = (scratch.vec::<T>(count)?, scratch.retain());
            self.absorb_native(&mut owned.1)?;
            Ok(owned.0)
        } else {
            let values = self.carrier.with_work(|work| mount_vec::<T>(count, work))?;
            let bytes = values.capacity().checked_mul(std::mem::size_of::<T>())
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
            self.track(bytes)?;
            Ok(values)
        }
    }

    fn prepare_finalization(&mut self) -> Result<(), MountSourceError> {
        let bytes = (std::mem::size_of::<Option<MountSourceError>>()
            + std::mem::size_of::<Option<crate::demand::DemandError>>()) as u64;
        let allocation = if let Some(original) = self.carrier.original_native_work() {
            Some(original.claim(bytes, 2, crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?)
        } else {
            self.carrier.with_work(|work| work.claim_allocation(bytes, 2)).map_err(mount_demand_error)?;
            self.track(bytes)?;
            None
        };
        self.finalization = Some((Box::new(None), Box::new(None), allocation));
        Ok(())
    }

    fn finalization_error(
        &mut self, primary: MountSourceError, cleanup: crate::demand::DemandError,
    ) -> MountSourceError {
        let (mut slot, mut cleanup_slot, custody) = self.finalization.take()
            .expect("the original primary slot precedes every removal or rename effect");
        *slot = Some(primary);
        *cleanup_slot = Some(cleanup);
        if custody.is_none() {
            self.keep((std::mem::size_of::<Option<MountSourceError>>()
                + std::mem::size_of::<Option<crate::demand::DemandError>>()) as u64)
                .expect("the primary slot keeps only its original admitted allocation");
        }
        MountSourceError::MutationFinalization { primary: slot, cleanup: cleanup_slot, custody }
    }

    fn reserve_final_work(&mut self, delta: crate::WorkCounters) -> Result<(), MountSourceError> {
        let items = self.reserved_items.checked_add(delta.items_examined)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let copies = self.reserved_copies.checked_add(delta.bytes_copied)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        self.carrier.with_work(|work| {
            work.reserve(crate::materializer::NativeResourceCredit::Items, delta.items_examined).map_err(mount_demand_error)?;
            if let Err(error) = work.reserve(crate::materializer::NativeResourceCredit::Copy, delta.bytes_copied) {
                work.refund(crate::materializer::NativeResourceCredit::Items, delta.items_examined).expect("original reservation is balanced");
                return Err(mount_demand_error(error));
            }
            Ok::<_, MountSourceError>(())
        })?;
        self.reserved_items = items;
        self.reserved_copies = copies;
        Ok(())
    }


}

impl Drop for DirectoryCarrierScratch<'_, '_> {
    fn drop(&mut self) {
        let items = std::mem::take(&mut self.reserved_items);
        let copies = std::mem::take(&mut self.reserved_copies);
        self.carrier.with_work(|work| {
            work.refund(crate::materializer::NativeResourceCredit::Items, items).expect("original reservation is balanced");
            work.refund(crate::materializer::NativeResourceCredit::Copy, copies).expect("original reservation is balanced");
        });
        drop(self.finalization.take());
        let bytes = self.bytes;
        if bytes == 0 { return; }
        if self.carrier.original_native_work().is_some() {
            self.carrier.with_work(|work| {
                if let Err(error) = work.release(bytes) {
                    work.record_native_io_failure(error);
                    work.poison_native_original_work();
                }
            });
        } else {
            self.carrier.with_work(|work| work.release(bytes))
                .expect("directory scratch releases only its original allocations");
        }
    }
}

fn directory_removal_bytes(names: &Vec<String>) -> Result<u64, MountSourceError> {
    let backing = names.capacity().checked_mul(std::mem::size_of::<String>())
        .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
    names.iter().try_fold(backing, |bytes, name| bytes.checked_add(name.capacity() as u64)
        .ok_or(MountSourceError::Work(crate::WorkError::Overflow)))
}

fn directory_lazy_error(error: LazyWorkspaceError) -> MountSourceError {
    match error.semantic_error() {
        LazyWorkspaceError::Demand(DemandError::StaleVersion) => MountSourceError::Stale,
        _ => lazy_error(error),
    }
}

fn mount_components_with_scratch<S: super::adapter::CaptureInputScratch>(
    source: &[Vec<u8>], extra: Option<&[u8]>, scratch: &mut S,
) -> Result<MountPath, MountSourceError> {
    let count = source.len().checked_add(usize::from(extra.is_some()))
        .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
    let mut components = scratch.vec::<Vec<u8>>(count)?;
    for component in source.iter().map(Vec::as_slice).chain(extra.into_iter()) {
        scratch.items(1)?;
        let delta = crate::WorkCounters { bytes_copied: component.len() as u64,
            ..crate::WorkCounters::UNCHARGED };
        scratch.admit(delta)?;
        let mut copied = scratch.vec::<u8>(component.len())?;
        copied.extend_from_slice(component);
        scratch.record(delta)?;
        components.push(copied);
    }
    Ok(MountPath { components })
}

fn mount_components_with_work(
    source: &[Vec<u8>], extra: Option<&[u8]>, work: &mut crate::demand::DemandWork,
) -> Result<(MountPath, u64), MountSourceError> {
    let mut scratch = super::adapter::CaptureScratchAllocation::new(work);
    let path = mount_components_with_scratch(source, extra, &mut scratch)?;
    Ok((path, scratch.retain()))
}

struct MountOwnedBuffer<T> {
    value: T,
    legacy_bytes: u64,
    allocation: Option<crate::NativeAllocation>,
}

impl<T> std::ops::Deref for MountOwnedBuffer<T> {
    type Target = T;
    fn deref(&self) -> &T { &self.value }
}

impl<T> MountOwnedBuffer<T> {
    fn legacy_owned_bytes(&self) -> u64 {
        if self.allocation.is_some() { 0 } else { self.legacy_bytes }
    }

    fn into_parts(self) -> (T, Option<crate::NativeAllocation>, u64) {
        (self.value, self.allocation, self.legacy_bytes)
    }
}

fn mount_components_with_carrier(
    source: &[Vec<u8>], extra: Option<&[u8]>,
    carrier: &mut super::adapter::NativeDemandCarrier<'_>,
) -> Result<MountOwnedBuffer<MountPath>, MountSourceError> {
    if let Some(original) = carrier.original_native_work() {
        let mut scratch = super::adapter::NativeCaptureScratchAllocation::new(&original)?;
        let path = mount_components_with_scratch(source, extra, &mut scratch)?;
        Ok(MountOwnedBuffer { value: path, legacy_bytes: 0, allocation: Some(scratch.retain()) })
    } else {
        let (path, bytes) = carrier.with_work(|work| mount_components_with_work(source, extra, work))?;
        Ok(MountOwnedBuffer { value: path, legacy_bytes: bytes, allocation: None })
    }
}

fn mount_name_with_carrier(
    name: &crate::kernel::LogicalName,
    carrier: &mut super::adapter::NativeDemandCarrier<'_>,
) -> Result<MountOwnedBuffer<Vec<u8>>, MountSourceError> {
    if carrier.original_native_work().is_some() {
        let (name, allocation) = super::adapter::native_mount_name(name, carrier)?;
        Ok(MountOwnedBuffer { value: name, legacy_bytes: 0, allocation })
    } else {
        let name = super::adapter::native_mount_name(name, carrier)?.0;
        let bytes = name.capacity() as u64;
        Ok(MountOwnedBuffer { value: name, legacy_bytes: bytes, allocation: None })
    }
}

fn promotion_push<T>(
    values: &mut Vec<T>, value: T, scratch: &mut DirectoryCarrierScratch<'_, '_>,
) -> Result<(), MountSourceError> {
    let original = scratch.carrier.original_native_work();
    if let Some(original) = &original {
        original.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })
            .map_err(MountSourceError::NativeWork)?;
    } else {
        scratch.carrier.with_work(|work| mount_items(work, 1))?;
    }
    if values.len() == values.capacity() {
        let capacity = values.capacity().checked_mul(2).map(|capacity| capacity.max(1))
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let copied = values.len().checked_mul(std::mem::size_of::<T>())
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
        let delta = crate::WorkCounters { bytes_copied: copied, ..crate::WorkCounters::UNCHARGED };
        let mut replacement = if let Some(original) = &original {
            use super::adapter::CaptureInputScratch;
            original.admit(delta).map_err(MountSourceError::NativeWork)?;
            let mut allocation = super::adapter::NativeCaptureScratchAllocation::new(original)?;
            let values = allocation.vec::<T>(capacity)?;
            let mut allocation = allocation.retain();
            if let Some(backing) = &mut scratch.native_backing {
                backing.absorb(&mut allocation).map_err(MountSourceError::NativeWork)?;
            } else {
                scratch.native_backing = Some(allocation);
            }
            values
        } else {
            scratch.carrier.with_work(|work| work.work.admit(&delta, &work.budget))
                .map_err(MountSourceError::Work)?;
            let values = scratch.carrier.with_work(|work| mount_vec(capacity, work))?;
            let bytes = values.capacity().checked_mul(std::mem::size_of::<T>())
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
            scratch.track(bytes)?;
            values
        };
        replacement.append(values);
        if let Some(original) = &original {
            original.record_admitted(delta).map_err(MountSourceError::NativeWork)?;
        } else {
            scratch.carrier.with_work(|work| work.record(delta)).map_err(mount_demand_error)?;
        }
        let old = std::mem::replace(values, replacement);
        let bytes = old.capacity().checked_mul(std::mem::size_of::<T>())
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
        drop(old);
        scratch.release_backing(bytes)?;
    }
    values.push(value);
    Ok(())
}

struct StampedCursor<T> {
    generation: (u64, u64),
    cursor: T,
    native_cleanup: Option<crate::materializer::NativeWorkReservation>,
    _native_cleanup_allocation: Option<crate::NativeAllocation>,
}

struct CursorTable<T> {
    entries: Mutex<BTreeMap<u64, StampedCursor<T>>>,
    next: AtomicU64,
    retiring: AtomicU64,
    maximum: usize,
}

impl<T> CursorTable<T> {
    fn new(maximum: usize) -> Self {
        Self { entries: Mutex::new(BTreeMap::new()), next: AtomicU64::new(1),
            retiring: AtomicU64::new(0), maximum }
    }

    fn physically_empty(&self) -> Result<bool, MountSourceError> {
        let entries = self.entries.lock().map_err(|_| MountSourceError::Stale)?;
        Ok(entries.is_empty() && self.retiring.load(Ordering::Acquire) == 0)
    }

    fn next_token(&self) -> u64 {
        let mut token = self.next.fetch_add(1, Ordering::Relaxed);
        if token == 0 { token = self.next.fetch_add(1, Ordering::Relaxed); }
        token
    }

    fn take(
        &self,
        generation: (u64, u64),
        cursor: Option<&[u8]>,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<Option<T>, MountSourceError> {
        let Some(cursor) = cursor else { return Ok(None); };
        if let Some(original) = carrier.original_native_work() {
            original.charge(crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED
            }).map_err(MountSourceError::NativeWork)?;
            let bytes: [u8; 8] = cursor.try_into().map_err(|_| MountSourceError::Stale)?;
            original.charge(crate::WorkCounters {
                bytes_copied: 8, ..crate::WorkCounters::UNCHARGED
            }).map_err(MountSourceError::NativeWork)?;
            let stamped = self.entries.lock().map_err(|_| MountSourceError::Stale)?
                .remove(&u64::from_le_bytes(bytes)).ok_or(MountSourceError::Stale)?;
            if stamped.generation != generation { return Err(MountSourceError::Stale); }
            return Ok(Some(stamped.cursor));
        }
        let stamped = carrier.with_work(|work| {
            mount_items(work, 1)?;
            let delta = crate::WorkCounters { bytes_copied: 8, ..crate::WorkCounters::UNCHARGED };
            work.work.admit(&delta, &work.budget).map_err(MountSourceError::Work)?;
            let bytes: [u8; 8] = cursor.try_into().map_err(|_| MountSourceError::Stale)?;
            work.record(delta).map_err(mount_demand_error)?;
            self.entries.lock().map_err(|_| MountSourceError::Stale)?
                .remove(&u64::from_le_bytes(bytes)).ok_or(MountSourceError::Stale)
        })?;
        if stamped.generation != generation {
            drop(stamped);
            carrier.with_work(|work| work.record(crate::WorkCounters::UNCHARGED)).map_err(mount_demand_error)?;
            return Err(MountSourceError::Stale);
        }
        Ok(Some(stamped.cursor))
    }

    fn remember(
        &self,
        generation: (u64, u64),
        cursor: T,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<MountOwnedBuffer<Vec<u8>>, MountSourceError> {
        if let Some(original) = carrier.original_native_work() {
            use super::adapter::CaptureInputScratch;
            let native_cleanup = original.reserve_work(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED }).map_err(MountSourceError::NativeWork)?;
            let native_cleanup_allocation = original.claim(
                (std::mem::size_of::<Option<crate::materializer::NativeWorkReservation>>()
                    + std::mem::size_of::<Option<crate::NativeAllocation>>()) as u64, 0, crate::WorkCounters::UNCHARGED,
            ).map_err(MountSourceError::NativeWork)?;
            let mut scratch = super::adapter::NativeCaptureScratchAllocation::new(&original)?;
            let delta = crate::WorkCounters {
                bytes_copied: 8, ..crate::WorkCounters::UNCHARGED
            };
            scratch.admit(delta)?;
            let mut output = acyclic_native_runtime::NativeByteReservation::new(
                original.runtime_meter(), acyclic_native_runtime::NativeOperationKind::Write, 8,
            ).map_err(|error| MountSourceError::NativeWork(original.runtime_error(error)))?;
            let mut token_bytes = scratch.vec::<u8>(8)?;
            loop {
                let evicted = {
                    let mut entries = self.entries.lock().map_err(|_| MountSourceError::Stale)?;
                    if entries.len() < self.maximum || entries.is_empty() { None } else {
                        original.charge(crate::WorkCounters {
                            items_examined: 1, ..crate::WorkCounters::UNCHARGED
                        }).map_err(MountSourceError::NativeWork)?;
                        entries.pop_first()
                    }
                };
                let Some(evicted) = evicted else { break; };
                drop(evicted);
            }
            let replaced = {
                let mut entries = self.entries.lock().map_err(|_| MountSourceError::Stale)?;
                original.charge(crate::WorkCounters {
                    items_examined: 1, ..crate::WorkCounters::UNCHARGED
                }).map_err(MountSourceError::NativeWork)?;
                let token = self.next_token();
                let replaced = entries.insert(token, StampedCursor {
                    generation, cursor, native_cleanup: Some(native_cleanup),
                    _native_cleanup_allocation: Some(native_cleanup_allocation),
                });
                token_bytes.extend_from_slice(&token.to_le_bytes());
                replaced
            };
            drop(replaced);
            let recorded_copy = scratch.record(delta);
            let recorded_output = output.record(8)
                .map_err(|error| MountSourceError::NativeWork(original.runtime_error(error)));
            recorded_copy.and(recorded_output)?;
            return Ok(MountOwnedBuffer {
                value: token_bytes, legacy_bytes: 0, allocation: Some(scratch.retain()),
            });
        }
        let mut scratch = DirectoryCarrierScratch::new(carrier);
        let mut cursor = Some(cursor);
        let delta = crate::WorkCounters { bytes_copied: 8, output_bytes: 8, ..crate::WorkCounters::UNCHARGED };
        let mut token_bytes = scratch.carrier.with_work(|work| {
            work.work.admit(&delta, &work.budget).map_err(MountSourceError::Work)?;
            mount_vec::<u8>(8, work)
        })?;
        scratch.track(token_bytes.capacity() as u64)?;
        loop {
            let evicted = scratch.carrier.with_work(|work| {
                let mut entries = self.entries.lock().map_err(|_| MountSourceError::Stale)?;
                if entries.len() < self.maximum || entries.is_empty() { return Ok(None); }
                mount_items(work, 1)?;
                Ok::<_, MountSourceError>(entries.pop_first())
            })?;
            let Some(evicted) = evicted else { break; };
            drop(evicted);
            scratch.carrier.with_work(|work| work.record(crate::WorkCounters::UNCHARGED)).map_err(mount_demand_error)?;
        }
        let replaced = scratch.carrier.with_work(|work| {
            let mut entries = self.entries.lock().map_err(|_| MountSourceError::Stale)?;
            mount_items(work, 1)?;
            work.work.admit(&delta, &work.budget).map_err(MountSourceError::Work)?;
            let token = self.next_token();
            let replaced = entries.insert(token, StampedCursor {
                generation, cursor: cursor.take().expect("the new cursor is inserted once"),
                native_cleanup: None,
                _native_cleanup_allocation: None,
            });
            token_bytes.extend_from_slice(&token.to_le_bytes());
            work.record(delta).map_err(mount_demand_error)?;
            Ok::<_, MountSourceError>(replaced)
        })?;
        drop(replaced);
        scratch.carrier.with_work(|work| work.record(crate::WorkCounters::UNCHARGED)).map_err(mount_demand_error)?;
        scratch.keep(token_bytes.capacity() as u64)?;
        Ok(MountOwnedBuffer { value: token_bytes, legacy_bytes: 8, allocation: None })
    }

    fn clear_with_cleanup(
        &self, wait: Option<&crate::CancellationToken>,
    ) -> Result<(), MountSourceError> {
        loop {
            let removed = {
                let mut entries = self.entries.lock().map_err(|_| MountSourceError::Stale)?;
                let Some((_, first)) = entries.first_key_value() else {
                    return if self.retiring.load(Ordering::Acquire) == 0 { Ok(()) }
                        else { Err(MountSourceError::Stale) };
                };
                if let Some(wait) = wait { wait.check().map_err(|_| MountSourceError::Cancelled)?; }
                if first.native_cleanup.is_none() {
                    return Err(MountSourceError::NativeWork(
                        crate::NativeTreeMaterializationError::InvalidLayout,
                    ));
                }
                self.retiring.fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| count.checked_add(1))
                    .map_err(|_| MountSourceError::Work(crate::WorkError::Overflow))?;
                entries.pop_first().expect("accepted original cursor is present").1
            };
            let StampedCursor {
                cursor, mut native_cleanup, _native_cleanup_allocation, ..
            } = removed;
            drop(cursor);
            let completed = native_cleanup.as_mut().expect("birth owns its terminal visit")
                .record_completed(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED }).map_err(MountSourceError::NativeWork);
            drop(native_cleanup);
            drop(_native_cleanup_allocation);
            self.retiring.fetch_sub(1, Ordering::AcqRel);
            completed?;
        }
    }

    fn retire(
        &self,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        if let Some(original) = carrier.original_native_work() {
            return self.clear_with_cleanup(Some(original.cancellation()));
        }
        loop {
            let removed = carrier.with_work(|work| {
                let mut entries = self.entries.lock().map_err(|_| MountSourceError::Stale)?;
                if entries.is_empty() { return Ok(None); }
                mount_items(work, 1)?;
                Ok::<_, MountSourceError>(entries.pop_first())
            })?;
            let Some(removed) = removed else { return Ok(()); };
            drop(removed);
            carrier.with_work(|work| work.record(crate::WorkCounters::UNCHARGED)).map_err(mount_demand_error)?;
        }
    }

    fn clear(&self) {
        if let Ok(mut entries) = self.entries.lock() { entries.clear(); }
    }
}

type UnstagedIdentities = Arc<UnstagedTable>;

#[derive(Default)]
struct UnstagedTable(Mutex<HashMap<FileId, u64>>);

impl UnstagedTable {
    fn checked_at(&self, file_id: FileId, revision: u64) -> bool {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&file_id)
            == Some(&revision)
    }

    fn remember(&self, file_id: FileId, revision: u64) {
        let mut checked = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if checked.len() >= MAXIMUM_REMEMBERED_RESOLUTIONS {
            checked.clear();
        }
        checked.insert(file_id, revision);
    }
}

enum Resolution {
    Authored(MountLookup),
    Absent,
    Unauthored(LazyLookup, Option<SourceReference>, Option<crate::NativeAllocation>),
}

#[derive(Clone, Copy)]
struct Deferral {
    stamp: ViewStamp,
    source: SourceReference,
    node: Option<SourceNode>,
}

impl Deferral {
    fn resolution(self) -> Resolution {
        self.node.map_or(Resolution::Absent, |node| {
            Resolution::Unauthored(LazyLookup::Source(node), Some(self.source), None)
        })
    }
}

#[derive(Default)]
struct Resolutions(Mutex<HashMap<MountPath, Deferral>>);

impl Resolutions {
    fn get(&self, path: &MountPath) -> Option<Deferral> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(path)
            .copied()
    }

    fn remember(&self, path: &MountPath, deferral: Deferral) {
        let mut entries = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if entries.len() >= MAXIMUM_REMEMBERED_RESOLUTIONS {
            entries.clear();
        }
        entries.insert(path.clone(), deferral);
    }

    fn forget(&self, path: &MountPath) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(path);
    }
}

#[derive(Default)]
struct SourceAbsentDirectories(Mutex<HashMap<MountPath, (ViewStamp, SourceReference)>>);

impl SourceAbsentDirectories {
    fn get(&self, path: &MountPath) -> Option<(ViewStamp, SourceReference)> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(path)
            .copied()
    }

    fn remember(&self, path: &MountPath, stamp: ViewStamp, source: SourceReference) {
        let mut entries = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if entries.len() >= MAXIMUM_REMEMBERED_RESOLUTIONS {
            entries.clear();
        }
        entries.insert(path.clone(), (stamp, source));
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct MountedRemoval {
    file_id: FileId,
    operation: Option<crate::OperationId>,
}

struct MountedRemovalPath {
    text: String,
    custody: Option<super::NativeMountAllocationCustody>,
    bytes: u64,
    native_allocation: Option<crate::NativeAllocation>,
}

impl MountedRemovalPath {
    fn unmeasured(text: String) -> Self { Self { text, custody: None, bytes: 0, native_allocation: None } }
    fn capacity(&self) -> usize { self.text.capacity() }
}

impl std::ops::Deref for MountedRemovalPath {
    type Target = str;
    fn deref(&self) -> &str { &self.text }
}

impl std::borrow::Borrow<str> for MountedRemovalPath {
    fn borrow(&self) -> &str { &self.text }
}

impl std::borrow::Borrow<String> for MountedRemovalPath {
    fn borrow(&self) -> &String { &self.text }
}

impl PartialEq for MountedRemovalPath {
    fn eq(&self, other: &Self) -> bool { self.text == other.text }
}
impl Eq for MountedRemovalPath {}
impl PartialOrd for MountedRemovalPath {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}
impl Ord for MountedRemovalPath {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering { self.text.cmp(&other.text) }
}

impl Drop for MountedRemovalPath {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.text));
        if let Some(custody) = &self.custody {
            custody.retire(self.bytes).expect("the removed path drops its original owned payload first");
        }
    }
}

type CapturedRemovalData = ((FileRecord, FileMetadata), usize);

struct CapturedRemovalRecord {
    value: (FileRecord, FileMetadata),
    paths: usize,
    custody: Option<super::NativeMountAllocationCustody>,
    bytes: u64,
}

impl Drop for CapturedRemovalRecord {
    fn drop(&mut self) {
        if let Some(custody) = &self.custody {
            custody.retire(self.bytes).expect("captured removal data retires only its original owned bytes");
        }
    }
}

struct PreparedMountedRemoval {
    key: MountedRemovalPath,
    file_id: FileId,
    prior: Option<MountedRemoval>,
    old_paths: Option<usize>,
    new_paths: Option<usize>,
    changed: bool,
    final_work: crate::WorkCounters,
}

struct PreparedMountedIdentity<A, O> {
    record: FileRecord,
    metadata: FileMetadata,
    file: Arc<CheckoutDetachedFile<A, O>>,
    position: Option<usize>,
    paths: usize,
    captured: Option<CapturedRemovalRecord>,
    final_work: crate::WorkCounters,
}

struct PreparedMountedRebound {
    removed: Option<MountedRemoval>,
    paths: Option<usize>,
    final_work: crate::WorkCounters,
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct SelectedImportProof {
    checkpoint: crate::LazySnapshotRef,
    source: crate::LazySnapshotRef,
    source_config: crate::model::VolumeConfig,
    input: crate::Digest,
}

struct SelectedMountInput<'a> {
    destination: crate::LazySnapshotRef,
    plan: &'a crate::SelectedImportPlan,
    operation: crate::OperationId,
    original: &'a crate::NativeMaterializationWork,
}

#[derive(Clone, Copy)]
struct MountedPublicationCustody {
    operation: crate::OperationId,
    published: bool,
    selected: Option<SelectedImportProof>,
}

#[derive(Default)]
struct MountedRemovals {
    paths: BTreeMap<MountedRemovalPath, MountedRemoval>,
    records: BTreeMap<FileId, CapturedRemovalRecord>,
    publication: Option<MountedPublicationCustody>,
    epoch: u64,
    selected_inputs: Option<crate::lazy_workspace::SelectedImportEffects>,
}

struct SelectedInputsLoan<'a> {
    removals: &'a Mutex<MountedRemovals>,
    operation: crate::OperationId,
    value: Option<crate::lazy_workspace::SelectedImportEffects>,
}

impl<'a> SelectedInputsLoan<'a> {
    fn take(
        removals: &'a Mutex<MountedRemovals>, operation: crate::OperationId,
        work: &mut MountSyncWork<'_>,
    ) -> Result<Self, MountSourceError> {
        work.items(1)?;
        let mut retained = removals.lock().map_err(|_| MountSourceError::Stale)?;
        if !retained.publication.is_some_and(|custody| custody.operation == operation) {
            return Err(MountSourceError::Stale);
        }
        Ok(Self { removals, operation, value: retained.selected_inputs.take() })
    }

    fn discard(mut self) { drop(self.value.take()); }
}

impl Drop for SelectedInputsLoan<'_> {
    fn drop(&mut self) {
        let Some(value) = self.value.take() else { return; };
        let mut retained = self.removals.lock().unwrap_or_else(PoisonError::into_inner);
        if retained.publication.is_some_and(|custody| custody.operation == self.operation) {
            let displaced = retained.selected_inputs.replace(value);
            drop(retained);
            drop(displaced);
        } else {
            drop(retained);
            drop(value);
        }
    }
}

impl MountedRemovals {
    fn rebound(&mut self, path: &str) -> bool {
        let Some(removed) = self.paths.remove(path) else { return false; };
        if let Some(record) = self.records.get_mut(&removed.file_id) {
            record.paths = record.paths.checked_sub(1).expect("captured identity has a removal owner");
            if record.paths == 0 { self.records.remove(&removed.file_id); }
        }
        self.epoch = self.epoch.wrapping_add(1);
        true
    }
}

struct PendingDetachedIdentity<A, O> {
    file: crate::DetachedFile<A, O>,
    metadata: FileMetadata,
    epoch: u64,
    operation: Option<crate::OperationId>,
    // Declared last: the frozen physical collection hold drops first.
    _custody: super::NativeMountAllocationCustody,
}

fn retain_pending_detached_with_work<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    file: crate::DetachedFile<A, O>,
    metadata: FileMetadata,
    epoch: u64,
    operation: Option<crate::OperationId>,
    work: &mut crate::demand::DemandWork,
) -> Result<PendingDetachedIdentity<A, O>, MountSourceError> {
    let bytes = file.logical_owned_bytes();
    match super::NativeMountAllocationCustody::from_admitted(bytes, work) {
        Ok(custody) => Ok(PendingDetachedIdentity { file, metadata, epoch, operation, _custody: custody }),
        Err(error) => {
            drop(file);
            work.release(bytes).map_err(mount_demand_error)?;
            Err(mount_demand_error(error))
        }
    }
}

struct DetachedIdentity<A, O> {
    file: Arc<CheckoutDetachedFile<A, O>>,
    ready: bool,
    published_epoch: u64,
    pending: Option<PendingDetachedIdentity<A, O>>,
}

struct DetachedTable<A, O> {
    entries: Vec<(FileId, DetachedIdentity<A, O>)>,
    custody: Option<super::NativeMountAllocationCustody>,
    bytes: u64,
    native_allocation: Option<crate::NativeAllocation>,
}

impl<A, O> Default for DetachedTable<A, O> {
    fn default() -> Self { Self { entries: Vec::new(), custody: None, bytes: 0, native_allocation: None } }
}

impl<A, O> std::ops::Deref for DetachedTable<A, O> {
    type Target = Vec<(FileId, DetachedIdentity<A, O>)>;
    fn deref(&self) -> &Self::Target { &self.entries }
}

impl<A, O> std::ops::DerefMut for DetachedTable<A, O> {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.entries }
}

impl<A, O> Drop for DetachedTable<A, O> {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.entries));
        if let Some(custody) = &self.custody {
            custody.retire(self.bytes).expect("detached vector storage retires only after its actual drop");
        }
    }
}

type DetachedIdentities<A, O> = Arc<Mutex<DetachedTable<A, O>>>;
type OpenIdentityHandles = Arc<Mutex<BTreeMap<FileId, Vec<Weak<dyn MountOpenFile>>>>>;
type DirtyDetachedRecord = (FileId, FileRecord, FileMetadata, u64);

struct MountSyncInputs {
    paths: MountSyncBuffer<Vec<String>>,
    removed_ids: MountSyncBuffer<Vec<FileId>>,
    dirty: MountSyncBuffer<Vec<DirtyDetachedRecord>>,
}

struct PreparedMountSync<A, O> {
    candidate: CheckoutCandidate<A, O>,
    epoch: u64,
    stamp: ViewStamp,
    inputs: MountSyncInputs,
    records: MountSyncBuffer<Vec<(FileRecord, FileMetadata)>>,
}

fn detached_entry<A, O>(entries: &[(FileId, DetachedIdentity<A, O>)], id: FileId) -> Option<&DetachedIdentity<A, O>> {
    entries.binary_search_by_key(&id, |entry| entry.0).ok()
        .and_then(|position| entries.get(position)).map(|entry| &entry.1)
}

fn detached_position<A, O>(
    entries: &[(FileId, DetachedIdentity<A, O>)],
    id: FileId,
    work: &mut crate::demand::DemandWork,
) -> Result<Result<usize, usize>, MountSourceError> {
    let (mut low, mut high) = (0, entries.len());
    while low < high {
        mount_items(work, 1)?;
        let middle = low + (high - low) / 2;
        let entry = entries.get(middle).ok_or(MountSourceError::Stale)?;
        match entry.0.cmp(&id) {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => return Ok(Ok(middle)),
        }
    }
    Ok(Err(low))
}

struct SourceChangeRecorder<A, O, D, S> {
    lazy: Weak<LazyWorkspace<A, O, D, S>>,
    authored: Weak<CheckoutMountSource<A, O>>,
    constructor_inputs: Mutex<Option<SourceConstructorInputs>>,
}

struct SourceConstructorInputs {
    lazy: Arc<dyn std::any::Any + Send + Sync>,
    root: String,
    native_allocation: Option<crate::NativeAllocation>,
}

impl<A, O, D, S> SourceChangeSink for SourceChangeRecorder<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    fn source_changed(
        &self,
        changes: &[SourceChange],
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::demand::DemandResult<()> {
        let work = crate::NativeMaterializationWork::new(budget, cancellation)
            .map_err(|(error, work)| crate::OperationFailure::new(
                recorder_native_error(error), work,
            ))?;
        let result = self.record_changes(changes, &work, None);
        result.map(|value| crate::OperationReceipt { value, work: work.receipt() })
            .map_err(|error| crate::OperationFailure::new(error, work.receipt()))
    }

    fn source_changed_with_native_work(
        &self, changes: &[SourceChange], source: crate::demand::NativeSourceWork<'_>,
    ) -> Result<(), DemandError> {
        let work = source.materialization_work()?;
        self.record_changes(changes, &work, Some(source))
    }
}

fn recorder_native_error(error: crate::NativeTreeMaterializationError) -> DemandError {
    match error {
        crate::NativeTreeMaterializationError::Work(error) => DemandError::Work(error),
        crate::NativeTreeMaterializationError::Cancelled => DemandError::Cancelled,
        crate::NativeTreeMaterializationError::OriginalWorkPoisoned => DemandError::OriginalWorkPoisoned,
        crate::NativeTreeMaterializationError::AllocationFailed(error) => DemandError::Allocation(error),
        crate::NativeTreeMaterializationError::Io(error) => DemandError::Io(error),
        error => DemandError::Mount(MountSourceError::NativeWork(error)),
    }
}

fn recorder_projection_error(error: MountSourceError) -> DemandError {
    match error {
        MountSourceError::NativeWork(error) => recorder_native_error(error),
        MountSourceError::Allocation(error) => DemandError::Allocation(error),
        MountSourceError::Io(error, None) => DemandError::Io(error),
        error => source_callback_error(error),
    }
}

fn recorder_check(
    work: &crate::NativeMaterializationWork, source: Option<crate::demand::NativeSourceWork<'_>>,
) -> Result<(), DemandError> {
    if let Some(source) = source { crate::native_host::NativeHostWork::check(&source)?; }
    work.admit(crate::WorkCounters::UNCHARGED).map_err(recorder_native_error)
}

fn recorder_namespace(
    names: &[crate::kernel::LogicalName], limits: crate::VolumeLimits,
    work: &crate::NativeMaterializationWork, custody: &mut crate::NativeAllocation,
    source: Option<crate::demand::NativeSourceWork<'_>>,
) -> Result<crate::kernel::NamespacePath, DemandError> {
    use crate::kernel::codec::ReadAllocationStep;
    use crate::kernel::NamespacePathStep;
    crate::kernel::NamespacePath::names_with_steps(names, limits, work.cancellation(), &mut |step| {
        match step {
            NamespacePathStep::Admit(delta) => {
                recorder_check(work, source)?;
                work.admit(*delta).map_err(recorder_native_error)
            }
            NamespacePathStep::Charge(delta) => {
                recorder_check(work, source)?;
                work.charge(*delta).map_err(recorder_native_error)
            }
            NamespacePathStep::Performed(delta) => {
                work.record_admitted(*delta).map_err(recorder_native_error)?;
                recorder_check(work, source)
            }
            NamespacePathStep::Allocation(ReadAllocationStep::Allocate { bytes, operations }) => {
                recorder_check(work, source)?;
                let mut allocation = work.claim(bytes, operations, crate::WorkCounters::UNCHARGED).map_err(recorder_native_error)?;
                custody.absorb(&mut allocation).map_err(recorder_native_error)
            }
            NamespacePathStep::Allocation(ReadAllocationStep::Capacity { admitted_bytes, actual_bytes }) => {
                let actual = custody.bytes().checked_sub(admitted_bytes)
                    .and_then(|bytes| bytes.checked_add(actual_bytes))
                    .ok_or(DemandError::Work(crate::WorkError::Overflow))?;
                custody.reconcile_capacity(actual).map_err(recorder_native_error)?;
                recorder_check(work, source)
            }
            NamespacePathStep::Allocation(ReadAllocationStep::ReserveFailed(error)) =>
                Err(DemandError::Allocation(error)),
            NamespacePathStep::Allocation(ReadAllocationStep::BeforeCopy(bytes)) => {
                recorder_check(work, source)?;
                work.admit(crate::WorkCounters { bytes_copied: bytes, ..crate::WorkCounters::UNCHARGED })
                    .map_err(recorder_native_error)
            }
            NamespacePathStep::Allocation(ReadAllocationStep::Copied(bytes)) =>
                work.record_admitted(crate::WorkCounters { bytes_copied: bytes, ..crate::WorkCounters::UNCHARGED })
                    .map_err(recorder_native_error),
            NamespacePathStep::Allocation(ReadAllocationStep::BeforeHash(bytes)) => {
                recorder_check(work, source)?;
                work.admit(crate::WorkCounters { bytes_hashed: bytes, ..crate::WorkCounters::UNCHARGED })
                    .map_err(recorder_native_error)
            }
            NamespacePathStep::Allocation(ReadAllocationStep::Hashed(bytes)) =>
                work.record_admitted(crate::WorkCounters { bytes_hashed: bytes, ..crate::WorkCounters::UNCHARGED })
                    .map_err(recorder_native_error),
            NamespacePathStep::Allocation(ReadAllocationStep::BeforeItem) => {
                recorder_check(work, source)?;
                work.admit(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })
                    .map_err(recorder_native_error)
            }
            NamespacePathStep::Allocation(ReadAllocationStep::Item) =>
                work.record_admitted(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })
                    .map_err(recorder_native_error),
        }
    })
}

fn recorder_vec<T>(
    count: usize, work: &crate::NativeMaterializationWork, custody: &mut crate::NativeAllocation,
    source: Option<crate::demand::NativeSourceWork<'_>>,
) -> Result<Vec<T>, DemandError> {
    recorder_check(work, source)?;
    let bytes = count.checked_mul(std::mem::size_of::<T>())
        .ok_or(DemandError::Work(crate::WorkError::Overflow))? as u64;
    let mut allocation = work.claim(bytes, u64::from(bytes != 0), crate::WorkCounters::UNCHARGED)
        .map_err(recorder_native_error)?;
    let mut values = Vec::new();
    if let Err(error) = values.try_reserve_exact(count) {
        drop(values);
        return Err(DemandError::Allocation(error));
    }
    let actual = (values.capacity() * std::mem::size_of::<T>()) as u64;
    let completed = allocation.reconcile_capacity(actual).map_err(recorder_native_error);
    if let Err(error) = completed {
        drop(values);
        return Err(error);
    }
    if let Err(error) = custody.absorb(&mut allocation).map_err(recorder_native_error) {
        drop(values);
        return Err(error);
    }
    if let Err(error) = recorder_check(work, source) {
        drop(values);
        return Err(error);
    }
    Ok(values)
}

impl<A, O, D, S> SourceChangeRecorder<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    fn record_changes(
        &self, changes: &[SourceChange], work: &crate::NativeMaterializationWork,
        source: Option<crate::demand::NativeSourceWork<'_>>,
    ) -> Result<(), DemandError> {
        recorder_check(work, source)?;
        // A dropped view has no SDK state left to invalidate.
        let (Some(lazy), Some(authored)) = (self.lazy.upgrade(), self.authored.upgrade()) else {
            return Ok(());
        };
        authored.cancellation().check().map_err(|_| DemandError::Cancelled)?;
        let mut custody = work.claim(0, 0, crate::WorkCounters::UNCHARGED).map_err(recorder_native_error)?;
        // Declared after custody: every physical buffer dies before its guard.
        let mut effect = ViewEffect::default();
        let result = (|| {
            let (mut nodes, mut subtrees, mut listings) = (0_usize, 0_usize, 0_usize);
            for change in changes {
                recorder_check(work, source)?;
                work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })
                    .map_err(recorder_native_error)?;
                let count = match change {
                    SourceChange::Name(_) => Some((&mut subtrees, 1)),
                    SourceChange::Entry(path) => Some((&mut listings, 1 + usize::from(!path.is_root()))),
                    SourceChange::Node(_) => Some((&mut nodes, 1)),
                    SourceChange::Everything | SourceChange::Unconfirmed => None,
                };
                if let Some((count, additional)) = count {
                    *count = count.checked_add(additional)
                        .ok_or(DemandError::Work(crate::WorkError::Overflow))?;
                }
            }
            effect.nodes = recorder_vec(nodes, work, &mut custody, source)?;
            effect.subtrees = recorder_vec(subtrees, work, &mut custody, source)?;
            effect.listings = recorder_vec(listings, work, &mut custody, source)?;
            let mut unconfirmed = false;
            for change in changes {
                recorder_check(work, source)?;
                work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })
                    .map_err(recorder_native_error)?;
                match change {
                    SourceChange::Name(path) => effect.subtrees.push(recorder_namespace(
                        path.components(), lazy.workspace().limits(), work, &mut custody, source,
                    )?),
                    SourceChange::Entry(path) => {
                        if !path.is_root() {
                            effect.listings.push(recorder_namespace(
                                &path.components()[..path.depth() - 1], lazy.workspace().limits(), work, &mut custody, source,
                            )?);
                        }
                        effect.listings.push(recorder_namespace(
                            path.components(), lazy.workspace().limits(), work, &mut custody, source,
                        )?);
                    }
                    SourceChange::Node(identity) => effect.nodes.push(
                        lazy.source_file_id(identity, &mut crate::kernel::allocation::KernelAccounting::native(work, work.cancellation()))
                            .map_err(|error| recorder_projection_error(lazy_error(error)))?,
                    ),
                    SourceChange::Everything => effect.everything = true,
                    SourceChange::Unconfirmed => unconfirmed = true,
                }
                recorder_check(work, source)?;
            }
            recorder_check(work, source)?;
            authored.cancellation().check().map_err(|_| DemandError::Cancelled)?;
            if !effect.is_empty() {
                authored.record_projection_change_with_native_work(
                    &ViewChange::Effect(&effect), work,
                    || {
                        recorder_check(work, source).map_err(|error| MountSourceError::NativeWork(
                            crate::materializer::native_materialization_work_error(error),
                        ))?;
                        authored.cancellation().check().map_err(|_| MountSourceError::Cancelled)
                    },
                ).map_err(recorder_projection_error)?;
                recorder_check(work, source)?;
            }
            if unconfirmed {
                recorder_check(work, source)?;
                authored.cancellation().check().map_err(|_| DemandError::Cancelled)?;
                authored.record_projection_change_with_native_work(
                    &ViewChange::Unconfirmed, work,
                    || {
                        recorder_check(work, source).map_err(|error| MountSourceError::NativeWork(
                            crate::materializer::native_materialization_work_error(error),
                        ))?;
                        authored.cancellation().check().map_err(|_| MountSourceError::Cancelled)
                    },
                ).map_err(recorder_projection_error)?;
                recorder_check(work, source)?;
            }
            Ok(())
        })();
        drop(effect);
        drop(custody);
        result
    }
}

fn linked(node: &SourceNode) -> bool {
    node.kind != SourceNodeKind::Directory && node.link_count.is_some_and(|count| count > 1)
}

struct CaptureObserverDispatch {
    gate: usize,
    prior: *const CaptureObserverDispatch,
}

std::thread_local! {
    static CAPTURE_OBSERVER_DISPATCH: std::cell::Cell<*const CaptureObserverDispatch> =
        const { std::cell::Cell::new(std::ptr::null()) };
}

pub(super) fn with_capture_observer_dispatch<T>(
    gate: usize, dispatch: impl FnOnce() -> T,
) -> T {
    CAPTURE_OBSERVER_DISPATCH.with(|current| {
        struct Restore<'a>(&'a std::cell::Cell<*const CaptureObserverDispatch>, *const CaptureObserverDispatch);
        impl Drop for Restore<'_> {
            fn drop(&mut self) { self.0.set(self.1); }
        }
        let frame = CaptureObserverDispatch { gate, prior: current.get() };
        let _restore = Restore(current, frame.prior);
        current.set(std::ptr::from_ref(&frame));
        dispatch()
    })
}

#[allow(unsafe_code, reason = "thread-local links borrow synchronous dispatch frames until guard restoration")]
pub(super) fn is_current_capture_observer_dispatch(gate: usize) -> bool {
    CAPTURE_OBSERVER_DISPATCH.with(|current| {
        let mut at = current.get();
        while !at.is_null() {
            // SAFETY: each frame is on this thread's live synchronous stack;
            // its unwind guard restores the prior link before that frame dies.
            let frame = unsafe { &*at };
            if frame.gate == gate { return true; }
            at = frame.prior;
        }
        false
    })
}

/// Physical custody of the original source watcher and its constructor sink.
pub struct NativeMountSourceWatchOwner {
    watch: Option<Box<dyn SourceWatch>>,
    sink: Option<Arc<dyn SourceChangeSink>>,
    startup_failure: Option<Arc<crate::source_watch::SourceWatchStartupFailure>>,
    bytes: u64,
    custody: Option<super::NativeMountAllocationCustody>,
}

impl NativeMountSourceWatchOwner {
    /// Whether this original watcher has confirmed physical native quiescence.
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.startup_failure.as_ref().map_or_else(
            || self.watch.as_ref().is_none_or(|watch| watch.is_stopped()),
            |failure| failure.is_stopped(),
        )
    }

    /// Retires this original watch using its before-start cleanup credit.
    pub(super) fn stop_with_cleanup_wait(
        &self, wait: &crate::CancellationToken,
    ) -> Result<bool, MountSourceError> {
        self.stop_with_cleanup(Some(wait))
    }

    pub(super) fn stop_accepted_cleanup(&self) -> Result<bool, MountSourceError> {
        self.stop_with_cleanup(None)
    }

    fn stop_with_cleanup(
        &self, wait: Option<&crate::CancellationToken>,
    ) -> Result<bool, MountSourceError> {
        if self.is_stopped() { return Ok(false); }
        let result = if let Some(failure) = &self.startup_failure {
            match wait {
                Some(wait) => failure.stop_with_cleanup_wait(wait),
                None => failure.stop_accepted_cleanup(),
            }
        } else if let Some(watch) = &self.watch {
            match wait {
                Some(wait) => watch.stop_with_cleanup_wait(wait),
                None => watch.stop_accepted_cleanup(),
            }
        } else {
            return Ok(false);
        };
        result.map_err(mount_demand_error).map(|()| true)
    }

    /// Stops the original watch on the supplied carrier.
    pub(super) fn stop(
        &self, carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<bool, MountSourceError> {
        if self.custody.is_some()
            && self.watch.is_some()
            && !matches!(carrier, super::adapter::NativeDemandCarrier::Native(work)
                if work.resource_allowance().is_some()) {
            return self.stop_ordinary_with_carrier(carrier);
        }
        match carrier {
            super::adapter::NativeDemandCarrier::Borrowed(work) => {
                let loan = super::adapter::BorrowedNativeDemandMeter::new(work)?;
                loan.run(|original| self.stop_on_native_work(crate::demand::NativeSourceWork::new(original)))
            }
            super::adapter::NativeDemandCarrier::Shared(original) => self.stop_on_native_work(crate::demand::NativeSourceWork::new(original)),
            super::adapter::NativeDemandCarrier::Native(work) => {
                let original = work.clone();
                self.stop_on_native_work(
                    crate::demand::NativeSourceWork::from_native_materialization_work(&original))
            }
        }
    }

    fn stop_ordinary_with_carrier(
        &self, carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<bool, MountSourceError> {
        let (budget, cancellation, prior_live) = carrier.with_work(|work| {
            let budget = work.remaining().map_err(mount_demand_error)?;
            Ok::<_, MountSourceError>((budget, work.cancellation().clone(), work.live_allocation_bytes()))
        })?;
        if self.is_stopped() { return Ok(false); }
        let watch = self.watch.as_ref().expect("ordinary construction retains its original watch");
        let result = crate::demand::stop_watch(watch.as_ref(), budget, &cancellation);

        match result {
            Ok(receipt) => carrier.with_work(|work|
                work.merge_with_prior_live(receipt.work, prior_live))
                .map_err(mount_demand_error).map(|()| true),
            Err(failure) => {
                let _ = carrier.with_work(|work|
                    work.merge_with_prior_live(*failure.work, prior_live));
                Err(mount_demand_error(failure.error))
            }
        }
    }

    fn stop_on_native_work(
        &self, mut work: crate::demand::NativeSourceWork<'_>,
    ) -> Result<bool, MountSourceError> {
        crate::native_host::NativeHostWork::check(&mut work).map_err(mount_demand_error)?;
        if self.is_stopped() { return Ok(false); }
        let result = if let Some(failure) = &self.startup_failure {
            failure.stop_with_work(&mut work)
        } else if let Some(watch) = &self.watch {
            let original = work.materialization_work().map_err(mount_demand_error)?;
            return crate::demand::stop_watch(watch.as_ref(), &original, original.cancellation()).map_err(mount_demand_error).map(|()| true);
        } else {
            return Ok(false);
        };
        result.map_err(mount_demand_error).map(|()| true)
    }


}

impl std::ops::Deref for NativeMountSourceWatchOwner {
    type Target = dyn SourceWatch;
    fn deref(&self) -> &Self::Target {
        self.watch.as_deref().expect("published source watch retains its original owner")
    }
}

impl Drop for NativeMountSourceWatchOwner {
    fn drop(&mut self) {
        drop(self.watch.take());
        drop(self.sink.take());
        drop(self.startup_failure.take());
        if let Some(custody) = &self.custody {
            custody.retire(self.bytes)
                .expect("watch custody retires only after its physical owner and sink drop");
        }
    }
}

/// One native callback adapter over a source-backed sparse workspace.
pub struct LazyMountSource<A, O, D, S> {
    pub(super) lazy: Arc<LazyWorkspace<A, O, D, S>>,
    authored: Arc<CheckoutMountSource<A, O>>,
    root: String,
    runtime: Arc<CallbackRuntime>,
    resolutions: Resolutions,
    source_absent: SourceAbsentDirectories,
    authored_directories: Mutex<HashMap<MountPath, ViewStamp>>,
    unstaged: UnstagedIdentities,
    cursors: CursorTable<(ViewStamp, LazyDirectoryCursor)>,
    source_view: Arc<SourceViewGate>,
    removals: Mutex<MountedRemovals>,
    detached: DetachedIdentities<A, O>,
    open_sources: OpenIdentityHandles,
    source_watch: Option<NativeMountSourceWatchOwner>,
    linked: Mutex<LinkedIdentities>,
    native_allocation: Option<crate::NativeAllocation>,
}

impl<A, O, D, S> super::adapter::NativeSourceIoRetirement for LazyMountSource<A, O, D, S>
where Self: Send + Sync, CheckoutMountSource<A, O>: Send + Sync {
    fn release_native_source_io(&self) {
        super::adapter::NativeSourceIoRetirement::release_native_source_io(self.authored.as_ref());
    }
}

#[derive(Default)]
struct LinkedIdentities {
    values: Vec<FileId>,
    custody: Option<super::NativeMountAllocationCustody>,
}

impl Drop for LinkedIdentities {
    fn drop(&mut self) {
        let values = std::mem::take(&mut self.values);
        let bytes = (values.capacity() * std::mem::size_of::<FileId>()) as u64;
        drop(values);
        if let Some(custody) = self.custody.take() {
            custody.retire(bytes).expect("linked identity custody matches its physical buffer");
        }
    }
}

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    /// Composes lazy reads and the authored checkout on the supplied carrier.
    pub(super) fn new(
        lazy: Arc<LazyWorkspace<A, O, D, S>>,
        authored: Arc<CheckoutMountSource<A, O>>,
        root: String,
        mut carrier: super::adapter::NativeDemandCarrier<'_>,
        allocations: Option<(crate::NativeAllocation, &mut crate::NativeAllocation)>,
    ) -> Result<Self, super::NativeMountError> {
        let original = carrier.original_native_work();
        if let Some(original) = &original {
            original.admit(crate::WorkCounters::UNCHARGED)
                .map_err(|error| super::NativeMountError::Demand(
                    DemandError::Mount(MountSourceError::NativeWork(error))))?;
        }
        let valid = match (&original, &allocations) {
            (Some(original), Some((root, source))) =>
                root.work().shares_ledger(original) && source.work().shares_ledger(original)
                && root.work().shares_resource_allowance(original)
                && source.work().shares_resource_allowance(original)
                && root.bytes().checked_add(source.bytes()).is_some(),
            (None, None) => true,
            _ => false,
        };
        if !valid {
            return Err(super::NativeMountError::Demand(DemandError::Mount(
                MountSourceError::NativeWork(crate::NativeTreeMaterializationError::InvalidLayout))));
        }
        let (root_allocation, source_allocation) = allocations
            .map_or((None, None), |(root, source)| (Some(root), Some(source)));
        carrier.with_work(|work| work.check()).map_err(super::NativeMountError::Demand)?;
        // Exact source names cannot share a case-folding authored namespace.
        if authored.folds_names() {
            return Err(super::NativeMountError::ProfileUnavailable {
                profile: "case-folding", platform: "a lazy source projection",
            });
        }
        let runtime = CallbackRuntime::existing()?;
        let cancellation = carrier.cancellation();
        let mut retained = 0_u64;
        let result = (|| {
            macro_rules! owned_arc {
                ($value:expr) => {{
                    let value = $value;
                    if let Some(original) = &original {
                        original.claim(0, 1, crate::WorkCounters::UNCHARGED).map_err(|error|
                            super::NativeMountError::Demand(DemandError::Mount(MountSourceError::NativeWork(error))))?;
                    } else {
                        let bytes = std::mem::size_of_val(&value) as u64;
                        carrier.with_work(|work| work.allocate(bytes)).map_err(super::NativeMountError::Demand)?;
                        retained += bytes;
                    }
                    Arc::new(value)
                }}
            }
            let runtime = owned_arc!(runtime);
            let unstaged = owned_arc!(UnstagedTable::default());
            let source_view = owned_arc!(SourceViewGate::new());
            let detached = owned_arc!(Mutex::new(DetachedTable::default()));
            let open_sources = owned_arc!(Mutex::new(BTreeMap::new()));
            let sink_bytes = if original.is_some() { 0 } else {
                std::mem::size_of::<SourceChangeRecorder<A, O, D, S>>() as u64
            };
            let sink = owned_arc!(SourceChangeRecorder {
                lazy: Arc::downgrade(&lazy), authored: Arc::downgrade(&authored),
                constructor_inputs: Mutex::new(Some(SourceConstructorInputs {
                    lazy: Arc::clone(&lazy) as Arc<dyn std::any::Any + Send + Sync>,
                    root, native_allocation: root_allocation,
                })),
            });
            let custody = if original.is_some() { None } else {
                Some(carrier.with_work(super::NativeMountAllocationCustody::new)?)
            };
            let mut owner = NativeMountSourceWatchOwner {
                watch: None, sink: Some(Arc::clone(&sink) as Arc<dyn SourceChangeSink>),
                startup_failure: None, custody,
                bytes: 0,
            };
            if let Some(custody) = &owner.custody {
                carrier.with_work(|work| custody.retain_admitted(sink_bytes, work))?;
                owner.bytes = sink_bytes;
                retained -= sink_bytes;
            }
            if let Some(original) = &original {
                let sink = Arc::clone(owner.sink.as_ref().expect("preallocated source sink"));
                let watched = lazy.watch_source_with_native_work(sink,
                    crate::demand::NativeSourceWork::from_native_materialization_work(original));
                match watched {
                    Ok(watch) => {
                        owner.watch = watch;

                        let checked = crate::native_host::NativeHostWork::check(
                            &mut crate::demand::NativeSourceWork::from_native_materialization_work(original));
                        if let Err(cause) = checked {
                            if owner.watch.is_some() {
                                return Err(super::NativeMountError::SourceWatchStartupFailed { cause, owner });
                            }
                            return Err(super::NativeMountError::Demand(cause));
                        }
                    }
                    Err(LazyWorkspaceError::Demand(DemandError::SourceWatchStartup(failure))) => {

                        owner.startup_failure = Some(Arc::clone(&failure));
                        return Err(super::NativeMountError::SourceWatchStartupFailed {
                            cause: DemandError::SourceWatchStartup(failure), owner,
                        });
                    }
                    Err(LazyWorkspaceError::Demand(error)) =>
                        return Err(super::NativeMountError::Demand(error)),
                    Err(error) => return Err(super::NativeMountError::Source(lazy_error(error))),
                }
            } else {
                let prepared = carrier.with_work(|work| work.prepare_watch_custody())
                    .map_err(super::NativeMountError::Demand)?;
                let remaining = carrier.with_work(|work| work.remaining()
                    .map(|budget| (budget, work.live_allocation_bytes())));
                let (budget, prior_live) = match remaining {
                    Ok(remaining) => remaining,
                    Err(error) => {
                        carrier.with_work(|work| work.abandon_prepared_watch_custody(prepared));
                        return Err(super::NativeMountError::Demand(error));
                    }
                };
                let watched = lazy.watch_source_measured(
                    Arc::clone(owner.sink.as_ref().expect("preallocated source sink")), budget, &cancellation);
                match watched {
                    Ok(receipt) => {
                        owner.watch = receipt.value;

                        let merged = carrier.with_work(|work| work.merge_with_prior_live(receipt.work, prior_live));
                        if let Some(watch) = owner.watch.as_ref() {
                            let bytes = watch.logical_owned_bytes();
                            let adopted = carrier.with_work(|work| work.record_adoption(bytes, &None));
                            let retained_watch = carrier.with_work(|work|
                                owner.custody.as_ref().expect("ordinary receipt custody")
                                    .retain_completed_admitted(bytes, work)).map_err(DemandError::from);
                            if retained_watch.is_ok() {
                                owner.bytes = owner.bytes.checked_add(bytes)
                                    .expect("admitted watch custody has a representable physical byte total");
                            }
                            let backing = match watch.allocation_custody() {
                                Some(token) => carrier.with_work(|work| work.retain_prepared_watch_custody(prepared, &token)),
                                None => {
                                    carrier.with_work(|work| work.abandon_prepared_watch_custody(prepared));
                                    Ok(())
                                }
                            };
                            let completed = merged.and(adopted).and(retained_watch)
                                .and(backing).and(carrier.with_work(|work| work.check()));
                            if let Err(cause) = completed {
                                return Err(super::NativeMountError::SourceWatchStartupFailed { cause, owner });
                            }
                        } else {
                            carrier.with_work(|work| work.abandon_prepared_watch_custody(prepared));
                            merged.map_err(super::NativeMountError::Demand)?;
                        }
                    }
                    Err(failure) => {
                        carrier.with_work(|work| work.abandon_prepared_watch_custody(prepared));
                        let _ = carrier.with_work(|work| work.merge_with_prior_live(*failure.work, prior_live));
                        return match failure.error {
                            LazyWorkspaceError::Demand(DemandError::SourceWatchStartup(failure)) => {

                                owner.startup_failure = Some(Arc::clone(&failure));
                                Err(super::NativeMountError::SourceWatchStartupFailed {
                                    cause: DemandError::SourceWatchStartup(failure), owner,
                                })
                            }
                            error => Err(super::NativeMountError::Source(lazy_error(error))),
                        };
                    }
                }
            }
            let SourceConstructorInputs { lazy: retained_lazy, root, native_allocation } =
                sink.constructor_inputs.lock().unwrap_or_else(PoisonError::into_inner)
                    .take().expect("constructor retains its exact owned inputs");
            drop(retained_lazy);
            let source_watch = if owner.watch.is_some() { Some(owner) } else { drop(owner); None };
            Ok(Self {
                lazy, authored, root,
                runtime,
                resolutions: Resolutions::default(),
                source_absent: SourceAbsentDirectories::default(),
                authored_directories: Mutex::new(HashMap::new()),
                unstaged,
                cursors: CursorTable::new(MAXIMUM_LAZY_DIRECTORY_CURSORS),
                source_view,
                removals: Mutex::new(MountedRemovals::default()),
                detached,
                open_sources,
                source_watch,
                linked: Mutex::default(),
                native_allocation,
            })
        })();
        match result {
            Ok(mut source) => {
                if let Some(source_allocation) = source_allocation {
                    source.native_allocation.as_mut().expect("owned constructor root guard")
                        .absorb(source_allocation).expect("prechecked exact same-ledger storage transfer");
                }
                Ok(source)
            }
            Err(error) => {
                let _ = carrier.with_work(|work| work.release(retained));
                Err(error)
            }
        }
    }

    fn reported(&self, file_id: FileId) -> bool {
        !self
            .linked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values
            .binary_search(&file_id).is_ok()
    }

    fn reported_with_meter(&self, file_id: FileId, work: &crate::NativeMaterializationWork) -> Result<bool, MountSourceError> {
        let mut work = work.native_read_meter().lock();
        work.check().map_err(mount_demand_error)?;
        self.authored.cancellation().check().map_err(|_| MountSourceError::Stale)?;
        let linked = self.linked.lock().map_err(|_| MountSourceError::Stale)?;
        let mut low = 0;
        let mut high = linked.values.len();
        while low < high {
            mount_items(&mut work, 1)?;
            let middle = low + (high - low) / 2;
            match linked.values[middle].cmp(&file_id) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => return Ok(false),
            }
        }
        Ok(true)
    }

    fn source_observed(&self) -> bool {
        self.source_watch
            .as_ref()
            .is_some_and(|watch| watch.is_exact())
    }

    fn ledger_stamp(&self) -> Option<ViewStamp> {
        self.source_view
            .is_stable()
            .then(|| self.authored.view_stamp())
            .flatten()
    }

    pub(crate) fn physically_retired(&self) -> Result<bool, MountSourceError> {
        Ok(self.authored.capture_retired()
            && self.cursors.physically_empty()?
            && self.source_watch.as_ref().is_none_or(|watch| watch.is_stopped()))
    }

    pub(crate) fn cancel_with_cleanup_wait(
        &self, wait: &crate::CancellationToken,
    ) -> Result<(), MountSourceError> {
        self.cancel_with_cleanup(Some(wait))
    }

    pub(crate) fn cancel_accepted_cleanup(&self) -> Result<(), MountSourceError> {
        self.cancel_with_cleanup(None)
    }

    fn cancel_with_cleanup(
        &self, wait: Option<&crate::CancellationToken>,
    ) -> Result<(), MountSourceError> {
        if self.physically_retired()? { return Ok(()); }
        self.authored.cancel_cleanup(wait)?;
        self.cursors.clear_with_cleanup(wait)?;
        if let Some(watch) = &self.source_watch { watch.stop_with_cleanup(wait)?; }
        Ok(())
    }

    pub(crate) fn native_source_io_lease(
        self: &Arc<Self>,
    ) -> Result<super::adapter::NativeSourceIoLease, MountSourceError> {
        let owner: Arc<dyn super::adapter::NativeSourceIoRetirement> =
            Arc::clone(self) as Arc<dyn super::adapter::NativeSourceIoRetirement>;
        self.authored.native_source_io_lease_for_owner(owner)
    }

    pub(super) fn cancel(
        &self, carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        if let Some(original) = carrier.original_native_work() {
            return self.cancel_with_cleanup_wait(original.cancellation());
        }
        self.authored.cancel_with_carrier(carrier)?;
        self.cursors.retire(carrier)?;
        let Some(watch) = self.source_watch.as_ref() else { return Ok(()); };
        watch.stop(carrier).map(|_| ())
    }

    fn detached_by_id(
        &self,
        file_id: FileId,
    ) -> Result<Option<Arc<CheckoutDetachedFile<A, O>>>, MountSourceError> {
        let published = self.removals.lock().map_err(|_| MountSourceError::Stale)?.publication
            .filter(|custody| custody.published).map(|custody| custody.operation);
        let detached = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
        Ok(detached_entry(&detached, file_id)
            .filter(|entry| entry.ready || (published.is_some()
                && entry.pending.as_ref().is_some_and(|pending| pending.operation == published)))
            .map(|entry| Arc::clone(&entry.file)))
    }

    fn detached_by_id_with_work(
        &self,
        file_id: FileId,
        work: &mut crate::demand::DemandWork,
    ) -> Result<Option<Arc<CheckoutDetachedFile<A, O>>>, MountSourceError> {
        work.check().map_err(mount_demand_error)?;
        let published = self.removals.lock().map_err(|_| MountSourceError::Stale)?.publication
            .filter(|custody| custody.published).map(|custody| custody.operation);
        let detached = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
        let mut low = 0;
        let mut high = detached.len();
        while low < high {
            mount_items(work, 1)?;
            let middle = low + (high - low) / 2;
            match detached[middle].0.cmp(&file_id) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => {
                    let entry = &detached[middle].1;
                    return Ok((entry.ready || (published.is_some()
                        && entry.pending.as_ref().is_some_and(|pending| pending.operation == published)))
                        .then(|| Arc::clone(&entry.file)));
                }
            }
        }
        Ok(None)
    }

    async fn detached_identity(
        &self,
        named: MountLookup,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<Option<MountLookup>, MountSourceError> {
        let Some(file) = carrier.with_work(|work| self.detached_by_id_with_work(named.node.file_id, work))? else {
            return Ok(None);
        };
        let caller = carrier.cancellation();
        let (record, metadata, _) = file.snapshot(carrier, &caller).await?;
        let mut current = mount_lookup(LazyLookup::Shadow {
            record, metadata: WorkspaceMetadata::from_engine(metadata), source_link_count: named.node.link_count,
        }, named.node.file_id);
        current.node.link_count = named.node.link_count;
        Ok(Some(current))
    }

    fn remember_linked_with_work(
        &self,
        file_id: FileId,
        work: &mut crate::demand::DemandWork,
    ) -> Result<(), MountSourceError> {
        let mut linked = self.linked.lock().map_err(|_| MountSourceError::Stale)?;
        let mut low = 0;
        let mut high = linked.values.len();
        while low < high {
            mount_items(work, 1)?;
            let middle = low + (high - low) / 2;
            match linked.values[middle].cmp(&file_id) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => return Ok(()),
            }
        }
        let width = std::mem::size_of::<FileId>();
        let shifted = linked.values.len() - low;
        if linked.values.len() < linked.values.capacity() {
            let copied = crate::WorkCounters {
                bytes_copied: ((shifted + 1) * width) as u64, ..crate::WorkCounters::default()
            };
            work.work.admit(&copied, &work.budget).map_err(MountSourceError::Work)?;
            linked.values.insert(low, file_id);
            return work.record(copied).map_err(mount_demand_error);
        }
        let capacity = linked.values.len().checked_add(1)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let bytes = capacity.checked_mul(width)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
        let copied = crate::WorkCounters {
            bytes_copied: linked.values.len().checked_add(shifted).and_then(|value| value.checked_add(1))
                .and_then(|value| value.checked_mul(width))
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64,
            ..crate::WorkCounters::default()
        };
        work.work.admit(&copied, &work.budget).map_err(MountSourceError::Work)?;
        let custody = super::NativeMountAllocationCustody::new(work).map_err(mount_demand_error)?;
        custody.claim(bytes, work).map_err(mount_demand_error)?;
        let mut values = Vec::new();
        if let Err(error) = values.try_reserve_exact(capacity) {
            drop(values);
            custody.retire(bytes).map_err(MountSourceError::Work)?;
            return Err(MountSourceError::Allocation(error));
        }
        values.extend_from_slice(&linked.values);
        values.insert(low, file_id);
        if let Err(error) = work.record(copied) {
            drop(values);
            custody.retire(bytes).map_err(MountSourceError::Work)?;
            return Err(mount_demand_error(error));
        }
        let old = std::mem::replace(&mut linked.values, values);
        let old_bytes = (old.capacity() * width) as u64;
        let old_custody = linked.custody.replace(custody);
        drop(old);
        if let Some(custody) = old_custody {
            custody.retire(old_bytes).map_err(MountSourceError::Work)?;
        }
        Ok(())
    }

    fn has_open_source_handle_with_work(
        &self,
        file_id: FileId,
        work: &mut crate::demand::DemandWork,
    ) -> Result<bool, MountSourceError> {
        mount_items(work, 1)?;
        let open = self.open_sources.lock().map_err(|_| MountSourceError::Stale)?;
        let Some(handles) = open.get(&file_id) else { return Ok(false); };
        for handle in handles {
            mount_items(work, 1)?;
            if handle.strong_count() != 0 { return Ok(true); }
        }
        // Pruning expired handles is optional cache maintenance, not removal.
        Ok(false)
    }

    fn detached_for_path(
        &self,
        path: &MountPath,
    ) -> Result<Option<Arc<CheckoutDetachedFile<A, O>>>, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        if self
            .detached
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .is_empty()
        {
            return Ok(None);
        }
        if let Some(authored) = self.authored.lookup(path)? {
            return self.detached_by_id(authored.node.file_id);
        }
        let text = self.path(path)?;
        if self.is_removed(&text)? {
            return Err(MountSourceError::NotFound);
        }
        let file_id = self.wait(|| async {
            let lookup = self.lazy.lookup(&text).await.map_err(lazy_error)?;
            let mut accounting = crate::lazy_workspace::LazyReadonlyAccounting::Portable {
                work: crate::WorkCounters::UNCHARGED, budget: crate::WorkBudget::UNBOUNDED,
                cancellation: &crate::CancellationToken::new(),
            };
            let result = self.lazy.stable_file_id_for_lookup(&text, &lookup, &mut accounting).await;
            accounting.finish_measured(result).map(|receipt| receipt.value).map_err(lazy_error)
        })?;
        self.detached_by_id(file_id)
    }

    fn finish_detached_publication(
        &self,
        operation: crate::OperationId,
        work: &mut MountSyncWork<'_>,
    ) -> Result<(), MountSourceError> {
        let count = self.detached.lock().map_err(|_| MountSourceError::Stale)?.len();
        for position in 0..count {
            let (file_id, epoch, notify) = {
                let mut identities = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
                work.items(1)?;
                let (file_id, entry) = &mut identities[position];
                let Some(pending) = entry.pending.as_ref().filter(|pending| pending.operation == Some(operation)) else { continue; };
                if entry.ready {
                    entry.published_epoch = pending.epoch;
                    continue;
                }
                (*file_id, pending.epoch, !entry.ready)
            };
            work.reserve_items(1)?;
            if notify {
                super::adapter::record_view_change(&self.authored.shared_checkout().ledger,
                    &ViewChange::Node(file_id), &mut work.meter,
                )?;
            }
            let mut identities = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
            let (current_id, entry) = identities.get_mut(position).ok_or(MountSourceError::Stale)?;
            work.complete_items(1)?;
            if *current_id != file_id || !entry.pending.as_ref().is_some_and(|pending|
                pending.operation == Some(operation) && pending.epoch == epoch)
            {
                return Err(MountSourceError::Stale);
            }
            entry.ready = true;
            entry.published_epoch = epoch;
        }
        Ok(())
    }

    fn prune_detached(&self, work: &mut MountSyncWork) -> Result<(), MountSourceError> {
        self.prune_detached_operation(None, work)
    }

    fn prune_detached_operation(
        &self,
        operation: Option<crate::OperationId>,
        work: &mut MountSyncWork<'_>,
    ) -> Result<(), MountSourceError> {
        let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        let mut open = self.open_sources.lock().map_err(|_| MountSourceError::Stale)?;
        let mut handle_items = 0_u64;
        for handles in open.values() {
            work.items(1)?;
            handle_items = handle_items.checked_add(handles.len() as u64)
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        }
        let mut identities = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
        let cleanup = crate::WorkCounters {
            items_examined: handle_items.checked_add(identities.len() as u64)
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?,
            ..crate::WorkCounters::default()
        };
        work.check().map_err(mount_demand_error)?;
        work.admit(&cleanup)?;
        open.retain(|_, handles| {
            handles.retain(|handle| handle.strong_count() != 0);
            !handles.is_empty()
        });
        identities.retain_mut(|(id, entry)| {
            if operation.is_some() && entry.pending.as_ref().is_some_and(|pending| pending.operation == operation) {
                entry.pending = None;
            }
            removals.records.contains_key(id)
                || open.contains_key(id)
                || !entry.ready
                || entry.pending.is_some()
                || Arc::strong_count(&entry.file) != 1
                || entry.file.mutation_epoch_if_idle().map_or(true, |epoch| epoch > entry.published_epoch)
        });
        work.record(cleanup).map_err(mount_demand_error)?;
        Ok(())
    }

    fn abort_detached_publication(&self, work: &mut MountSyncWork) -> Result<(), MountSourceError> {
        let mut identities = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
        work.items(identities.len())?;
        identities.retain_mut(|(_, entry)| {
            if entry.pending.as_ref().is_some_and(|pending| pending.operation.is_none()) {
                entry.pending = None;
            }
            entry.ready || entry.pending.is_some()
        });
        work.record(crate::WorkCounters::UNCHARGED).map_err(mount_demand_error)?;
        Ok(())
    }

    async fn stage_detached_records_measured(
        &self,
        records: &[(FileRecord, FileMetadata)],
        dirty: &[DirtyDetachedRecord],
        work: &mut MountSyncWork<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        for (record, metadata) in records {
            work.items(1)?;
            let position = self.reserve_detached_slot_with_carrier(record.file_id, &mut work.meter)?;
            if let Some(position) = position {
                let file = self.authored.detached_file_from_record(
                    *record, *metadata, SourceViewGate::callback_owner(), &mut work.meter,
                ).await?;
                let mut identities = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
                work.charge(crate::WorkCounters {
                    bytes_copied: ((identities.len() - position) * std::mem::size_of::<(FileId, DetachedIdentity<A, O>)>()) as u64,
                    ..crate::WorkCounters::default()
                }).map_err(mount_demand_error)?;
                identities.insert(position, (record.file_id, DetachedIdentity {
                    file, ready: false, published_epoch: 0, pending: None,
                }));
            }
        }
        let mut dirty_position = 0_usize;
        for (record, metadata) in records {
            work.items(1)?;
            let dirty_epoch = if let Some((id, _, _, epoch)) = dirty.get(dirty_position) {
                work.items(1)?;
                if *id == record.file_id { dirty_position += 1; Some(*epoch) } else { None }
            } else { None };
            let frozen = self.authored.detached_identity_from_record_with_carrier(
                *record, SourceViewGate::callback_owner(), &mut work.meter, cancellation,
            ).await?;
            let mut pending = work.with_work(|meter| retain_pending_detached_with_work(
                frozen, *metadata, dirty_epoch.unwrap_or(0), None, meter,
            ))?;
            let mut identities = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
            let position = work.with_work(|meter| detached_position(&identities, record.file_id, meter))?
                .map_err(|_| MountSourceError::Stale)?;
            let entry = &mut identities.get_mut(position).ok_or(MountSourceError::Stale)?.1;
            if entry.pending.is_some() { return Err(MountSourceError::Stale); }
            if dirty_epoch.is_none() { pending.epoch = entry.published_epoch; }
            entry.pending = Some(pending);
        }
        Ok(())
    }

    #[cfg(test)]
    async fn recover_pending_mount_change(
        &self,
        permit: crate::PublicationPermit,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let mut original = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, self.authored.cancellation());
        let mut work = MountSyncWork::new(&self.authored, super::adapter::NativeDemandCarrier::Borrowed(&mut original))?;
        self.recover_pending_mount_change_measured(
            permit, &mut work, self.authored.cancellation(),
            &mut self.authored.sdk_publication_policy(),
        ).await
    }

    fn recover_pending_mount_change_measured<'a, 'work, F, Fut, E>(
        &'a self,
        permit: crate::PublicationPermit,
        work: &'a mut MountSyncWork<'work>,
        cancellation: &'a crate::CancellationToken,
        before_publication: &'a mut F,
    ) -> impl Future<Output = Result<(), E>> + Send + 'a
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        async move {
            let native = match &work.meter {
                super::adapter::NativeDemandCarrier::Shared(meter) =>
                    Some(crate::NativeMaterializationWork::from_meter(Arc::clone(meter))),
                super::adapter::NativeDemandCarrier::Native(original) => Some((*original).clone()),
                super::adapter::NativeDemandCarrier::Borrowed(_) => None,
            };
            work.items(1)?;
            let custody = self.removals.lock().map_err(|_| MountSourceError::Stale)?.publication;
            if let (Some(custody), Some(original)) = (custody, native.as_ref())
                && custody.selected.is_some()
                && let Some(generation) = self.lazy.operation_generation(crate::IdempotencyKey::from_bytes(custody.operation.into_bytes()), &LazyTreapWork::native_with_work(original, cancellation).map_err(native_lazy_error)?).await.map_err(native_lazy_error)?
            {
                return self.finish_acknowledged_mount_custody(
                    custody.operation, generation, work, original, cancellation,
                ).await?.ok_or(MountSourceError::Indeterminate).map(|_| ()).map_err(E::from);
            }
            let mut committed = if let Some(original) = native.as_ref() {
                (async { let accounting = LazyTreapWork::native_with_work(original, cancellation)?; self.lazy.mount_removal_publication(&accounting).await }).await.map_err(native_lazy_error)?
            } else {
                let (budget, prior_live) = work.sdk_scope()?;
                let result = (async { let accounting = LazyTreapWork::new(budget, cancellation); let result = self.lazy.mount_removal_publication(&accounting).await; accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }).await;
                work.accept_sdk(result, prior_live, lazy_error)?
            };
            if let Some(custody) = custody {
                if !custody.published {
                    if committed.is_none() {
                        let (paths, records) = self.frozen_mount_inputs(custody.operation, work)?;
                        let selected_inputs = SelectedInputsLoan::take(&self.removals, custody.operation, work)?;
                        if let Some(original) = native.as_ref() {
                            (async { let accounting = LazyTreapWork::native_with_work(original, cancellation)?; self.lazy.prepare_mount_removals(&paths.value, selected_inputs.value.as_ref().map_or(&[], |effects| effects.paths.as_slice()), &records.value, IdempotencyKey::from_bytes(custody.operation.into_bytes()), &accounting).await }).await.map_err(native_lazy_error)?;
                        } else {
                            let (budget, prior_live) = work.sdk_scope()?;
                            let result = (async { let accounting = LazyTreapWork::new(budget, cancellation); let result = self.lazy.prepare_mount_removals(&paths.value, &[], &records.value, IdempotencyKey::from_bytes(custody.operation.into_bytes()), &accounting).await; accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }).await;
                            work.accept_sdk(result, prior_live, lazy_error)?;
                        }
                        selected_inputs.discard();
                        drop(paths);
                        drop(records);
                        work.record(crate::WorkCounters::UNCHARGED).map_err(mount_demand_error)?;
                        committed = Some(false);
                    }
                    let retained = {
                        let mut checkout = work.lock_checkout(&self.authored, cancellation.clone()).await?;
                        checkout.has_retained_operation().then(|| checkout.retained_operation_id())
                    };
                    if retained == Some(custody.operation) {
                        work.sync_authored_recovery_guarded(
                            &self.authored, permit, cancellation.clone(), before_publication,
                        ).await?;
                    } else if committed != Some(true) {
                        return Err(E::from(MountSourceError::Stale));
                    }
                    self.mark_mount_published(custody.operation)?;
                }
                if let Some(original) = native.as_ref()
                    && custody.selected.is_some()
                {
                    let generation = self.lazy.operation_generation(crate::IdempotencyKey::from_bytes(custody.operation.into_bytes()), &LazyTreapWork::native_with_work(original, cancellation).map_err(native_lazy_error)?).await.map_err(native_lazy_error)?.ok_or(MountSourceError::Indeterminate)?;
                    return self.finish_acknowledged_mount_custody(
                        custody.operation, generation, work, original, cancellation,
                    ).await?.ok_or(MountSourceError::Indeterminate).map(|_| ()).map_err(E::from);
                }
                if committed.is_some() {
                    let finished = if let Some(original) = native.as_ref() {
                        (async { let accounting = LazyTreapWork::native_with_work(original, cancellation)?; self.lazy.finish_mount_removals(&accounting).await }).await.map_err(native_lazy_error)?
                    } else {
                        let (budget, prior_live) = work.sdk_scope()?;
                        let result = (async { let accounting = LazyTreapWork::new(budget, cancellation); let result = self.lazy.finish_mount_removals(&accounting).await; accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }).await;
                        work.accept_sdk(result, prior_live, lazy_error)?
                    };
                    if !finished { return Err(E::from(MountSourceError::Stale)); }
                }
                return self.finish_mount_custody(custody.operation, work).map_err(E::from);
            }
            let retained = work.lock_checkout(&self.authored, cancellation.clone()).await?.has_retained_operation();
            if retained {
                work.sync_authored_recovery_guarded(
                    &self.authored, permit, cancellation.clone(), before_publication,
                ).await?;
            }
            if let Some(committed) = committed {
                let published = if let Some(original) = native.as_ref() {
                    (async { let accounting = LazyTreapWork::native_with_work(original, cancellation)?; self.lazy.finish_mount_removals(&accounting).await }).await.map_err(native_lazy_error)?
                } else {
                    let (budget, prior_live) = work.sdk_scope()?;
                    let result = (async { let accounting = LazyTreapWork::new(budget, cancellation); let result = self.lazy.finish_mount_removals(&accounting).await; accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }).await;
                    work.accept_sdk(result, prior_live, lazy_error)?
                };
                if (committed || retained) && !published { return Err(E::from(MountSourceError::Stale)); }
            }
            self.abort_detached_publication(work).map_err(E::from)
        }
    }

    fn frozen_mount_inputs(
        &self,
        operation: crate::OperationId,
        work: &mut MountSyncWork<'_>,
    ) -> Result<(MountSyncBuffer<Vec<String>>, MountSyncBuffer<Vec<(FileRecord, FileMetadata)>>), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let paths = {
            let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
            work.items(removals.paths.len())?;
            let count = removals.paths.values().filter(|removed| removed.operation == Some(operation)).count();
            let mut paths = work.owned_vec::<String>(count)?;
            for (path, removed) in &removals.paths {
                work.items(1)?;
                if removed.operation != Some(operation) { continue; }
                paths.push_cloned_text(&path.text, work)?;
            }
            paths
        };
        let identities = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
        work.items(identities.len())?;
        let count = identities.iter().filter(|(_, entry)|
            entry.pending.as_ref().is_some_and(|pending| pending.operation == Some(operation))).count();
        let mut records = work.owned_vec(count)?;
        for (_, entry) in identities.iter() {
            work.items(1)?;
            if let Some(pending) = entry.pending.as_ref().filter(|pending| pending.operation == Some(operation)) {
                work.charge(crate::WorkCounters {
                    bytes_copied: std::mem::size_of::<(FileRecord, FileMetadata)>() as u64,
                    ..crate::WorkCounters::default()
                }).map_err(mount_demand_error)?;
                records.push((pending.file.record(), pending.metadata));
            }
        }
        Ok((paths, records))
    }

    fn mark_mount_published(&self, operation: crate::OperationId) -> Result<(), MountSourceError> {
        let mut removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        let custody = removals.publication.as_mut().filter(|custody| custody.operation == operation)
            .ok_or(MountSourceError::Stale)?;
        custody.published = true;
        Ok(())
    }

    fn finish_mount_custody(&self, operation: crate::OperationId, work: &mut MountSyncWork) -> Result<(), MountSourceError> {
        self.finish_detached_publication(operation, work)?;
        self.clear_mount_removals(operation, work)?;
        self.prune_detached_operation(Some(operation), work)?;
        let inputs = {
            let mut removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
            removals.publication = None;
            removals.selected_inputs.take()
        };
        drop(inputs);
        Ok(())
    }

    fn finish_acknowledged_mount_custody<'a, 'work>(
        &'a self, operation: crate::OperationId, generation: crate::GenerationId,
        work: &'a mut MountSyncWork<'work>, original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Option<crate::LazySnapshotRef>, MountSourceError>> + Send + 'a>>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let original = original.clone();
        let cancellation = cancellation.clone();
        Box::pin(async move {
            work.items(1)?;
            let custody = self.removals.lock().map_err(|_| MountSourceError::Stale)?.publication;
            let Some(custody) = custody else {
                return self.lazy.selected_import_snapshot_for_generation(operation, generation, &LazyTreapWork::native_with_work(&original, &cancellation).map_err(native_lazy_error)?).await.map(Some).map_err(native_lazy_error);
            };
            if custody.operation != operation { return Ok(None); }
            let Some(proof) = custody.selected else { return Ok(None); };
            if !custody.published {
                if !self.authored.shared_checkout().resolve_acknowledged_original_with_native_work(
                    operation, generation, &original, &cancellation, Some(self.authored.cancellation()),
                    self.authored.publication_guard(),
                ).await? {
                    return Ok(None);
                }
                self.mark_mount_published(operation)?;
            }
            let snapshot = self.lazy.selected_committed_snapshot(operation, generation, proof.checkpoint, proof.source, proof.source_config, proof.input, &LazyTreapWork::native_with_work(&original, &cancellation).map_err(native_lazy_error)?).await.map_err(native_lazy_error)?;
            self.finish_mount_custody(operation, work)?;
            Ok(Some(snapshot))
        })
    }

    pub(super) async fn publication_snapshot(
        &self, carrier: super::adapter::NativeDemandCarrier<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::NativePublicationSnapshot,
        (MountSourceError, Option<MountSourceError>)>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let (_, qualified) = self.capture_snapshot(
            carrier, cancellation, MountPublicationObservation::Qualified,
        ).await?;
        qualified.ok_or((MountSourceError::NativeWork(
            crate::NativeTreeMaterializationError::MissingCustody,
        ), None))
    }

    /// Reads the actual mounted lazy identity.
    pub(super) async fn snapshot(
        &self, carrier: super::adapter::NativeDemandCarrier<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::LazySnapshotRef, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        self.capture_snapshot(
            carrier, cancellation, MountPublicationObservation::Ordinary,
        ).await.map(|(snapshot, _)| snapshot).map_err(|(primary, _)| primary)
    }

    async fn capture_snapshot(
        &self, carrier: super::adapter::NativeDemandCarrier<'_>,
        cancellation: &crate::CancellationToken, policy: MountPublicationObservation,
    ) -> Result<(crate::LazySnapshotRef, Option<crate::NativePublicationSnapshot>),
        (MountSourceError, Option<MountSourceError>)>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let mut work = MountSyncWork::new(&self.authored, carrier)
            .map_err(|primary| (primary, None))?;
        let result = async {
            work.check().map_err(mount_demand_error)?;
            let _mutation = self.source_view.write_stable_with_carrier(
                None, &mut work.meter, cancellation, Some(self.authored.cancellation()),
            ).await?.into_mutation_phase();
            {
                let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
                work.items(1)?;
                if !removals.paths.is_empty() || removals.publication.is_some() {
                    return Err(MountSourceError::Stale);
                }
            }
            let detached = {
                let identities = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
                let mut detached = work.owned_vec(identities.len())?;
                for (_, entry) in identities.iter() {
                    work.items(1)?;
                    if !entry.ready || entry.pending.is_some() { return Err(MountSourceError::Stale); }
                    detached.push((Arc::clone(&entry.file), entry.published_epoch));
                }
                detached
            };
            for (file, published) in &detached.value {
                work.items(1)?;
                if file.snapshot(&mut work.meter, cancellation).await?.2 != *published {
                    return Err(MountSourceError::Stale);
                }
            }
            let checkout = work.lock_checkout(&self.authored, cancellation.clone()).await?;
            checkout.ensure_publication_resolved()?;
            if checkout.has_pending_mutations() || self.authored.captures_pending() {
                return Err(MountSourceError::Stale);
            }
            let generation = checkout.generation_id();
            drop(checkout);
            let mut qualified = None;
            let snapshot = if let Some(original) = work.meter.original_native_work() {
                if policy == MountPublicationObservation::Qualified {
                    let publication = self.lazy.publication_snapshot_with_native_work(&original)
                        .await.map_err(native_lazy_error)?;
                    let snapshot = publication.snapshot()
                        .lazy_snapshot_with_native_work(&original, cancellation)
                        .map_err(mount_engine_error)?
                        .ok_or(MountSourceError::Indeterminate)?;
                    qualified = Some(publication);
                    snapshot
                } else {
                    self.lazy.snapshot(&LazyTreapWork::native_with_work(&original, original.cancellation())
                        .map_err(native_lazy_error)?)
                        .await.map_err(native_lazy_error)?
                }
            } else {
                if policy == MountPublicationObservation::Qualified {
                    return Err(MountSourceError::NativeWork(
                        crate::NativeTreeMaterializationError::MissingCustody,
                    ));
                }
                let prior_live = work.with_work(|meter| meter.live_allocation_bytes());
                let accounting = LazyTreapWork::new(work.remaining().map_err(mount_demand_error)?, cancellation);
                let result = self.lazy.snapshot(&accounting).await;
                let receipt = accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure());
                work.meter.accept_with_prior_live(receipt, prior_live, lazy_error)?
            };
            let checkout = work.lock_checkout(&self.authored, cancellation.clone()).await?;
            checkout.ensure_publication_resolved()?;
            if checkout.has_pending_mutations() || self.authored.captures_pending()
                || checkout.generation_id() != generation
                || snapshot.authored_generation != generation {
                return Err(MountSourceError::Stale);
            }
            Ok((snapshot, qualified))
        }.await;
        let accounting = if policy == MountPublicationObservation::Qualified || result.is_ok() {
            work.record(crate::WorkCounters::UNCHARGED).map_err(mount_demand_error)
        } else {
            Ok(())
        };
        match (result, accounting) {
            (Ok(snapshot), Ok(())) => Ok(snapshot),
            (Ok(snapshot), Err(error)) => {
                drop(snapshot);
                Err((error, None))
            }
            (Err(primary), Ok(())) => Err((primary, None)),
            (Err(primary), Err(accounting)) => Err((primary, Some(accounting))),
        }
    }

    /// Publishes pending authored writes.
    pub async fn sync_async(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let _mutation = self.source_view.write_stable(None).await?.into_mutation_phase();
        self.sync_staged_with_permit(crate::PublicationPermit::Unrestricted)
            .await
    }

    /// Publishes the mounted candidate and removal journal with bounded work.
    pub async fn sync_async_with_permit(
        &self,
        permit: crate::PublicationPermit,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<()>, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let mut work = crate::demand::DemandWork::new(budget, cancellation);
        let result = {
            let mut carrier = super::adapter::NativeDemandCarrier::Borrowed(&mut work);
            let mut scratch = DirectoryCarrierScratch::new(&mut carrier);
            match scratch.prepare_finalization() {
                Err(error) => Err(error),
                Ok(()) => {
                    let mut before_publication = self.authored.sdk_publication_policy();
                    let result = self.sync_async_with_permit_carrier_guarded(
                        permit, scratch.carrier.reborrow(), cancellation, &mut before_publication,
                    ).await;
                    let finalized = scratch.carrier.with_work(|work| work.record(crate::WorkCounters::UNCHARGED));
                    match (result, finalized) {
                        (Ok(()), Ok(())) => Ok(()),
                        (Err(primary), Ok(())) => Err(primary),
                        (Ok(()), Err(cleanup)) => Err(mount_demand_error(cleanup)),
                        (Err(primary), Err(cleanup)) => Err(scratch.finalization_error(primary, cleanup)),
                    }
                }
            }
        };
        result.map(|value| crate::OperationReceipt { value, work: work.work })
            .map_err(|error| crate::OperationFailure::new(error, work.work))
    }

    pub(crate) async fn sync_async_with_permit_qualified_native_work_guarded_with_ack<F, Fut, E>(
        &self, permit: crate::PublicationPermit, original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
        acknowledged: &mut Option<crate::NativePublicationSnapshot>, mut before_publication: F,
    ) -> Result<crate::NativePublicationSnapshot, (E, Option<MountSourceError>)>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        let mut work = MountSyncWork::new(
            &self.authored, super::adapter::NativeDemandCarrier::Native(original),
        ).map_err(|error| (E::from(error), None))?;
        work.observation = MountPublicationObservation::Qualified;
        work.check().map_err(|error| (E::from(mount_demand_error(error)), None))?;
        let _mutation = self.source_view.write_stable_with_carrier(
            None, &mut work.meter, cancellation, Some(self.authored.cancellation()),
        ).await.map_err(|error| (E::from(error), None))?.into_mutation_phase();
        let result = self.sync_staged(
            permit, &mut work, cancellation, &mut before_publication, None,
        ).await;
        if let Some(snapshot) = work.publication_snapshot.filter(|snapshot| snapshot.operation().is_some()) {
            *acknowledged = Some(snapshot);
        }
        result.map_err(|primary| (primary, work.accounting_failure.take()))?;
        work.publication_snapshot.ok_or_else(|| (E::from(MountSourceError::Indeterminate), None))
    }

    async fn sync_async_with_permit_carrier_guarded<F, Fut, E>(
        &self, permit: crate::PublicationPermit,
        carrier: super::adapter::NativeDemandCarrier<'_>, cancellation: &crate::CancellationToken,
        before_publication: &mut F,
    ) -> Result<(), E>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        let mut work = MountSyncWork::new(&self.authored, carrier)?;
        work.check().map_err(mount_demand_error)?;
        let _mutation = self.source_view.write_stable_with_carrier(
            None, &mut work.meter, cancellation, Some(self.authored.cancellation()),
        ).await?.into_mutation_phase();
        self.sync_staged(permit, &mut work, cancellation, before_publication, None).await.map(|_| ())
    }

    /// Verifies the caller's original owner immediately before every SDK seal.
    pub async fn sync_async_with_permit_measured_guarded<F, Fut, E>(
        &self,
        permit: crate::PublicationPermit,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
        mut before_publication: F,
    ) -> crate::MeasuredResult<crate::OperationReceipt<()>, (E, Option<MountSourceError>)>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        let mut work = crate::demand::DemandWork::new(budget, cancellation);
        let result = async {
            super::adapter::BorrowedNativeDemandMeter::new(&mut work).map_err(E::from)?
                .run_async(|meter| async move {
                    self.sync_async_with_permit_carrier_guarded(
                        permit, super::adapter::NativeDemandCarrier::Shared(&meter), cancellation, &mut before_publication,
                    ).await
                }).await
        }.await;
        let finalized = work.record(crate::WorkCounters::UNCHARGED).map_err(mount_demand_error);
        match (result, finalized) {
            (Ok(value), Ok(())) => Ok(crate::OperationReceipt { value, work: work.work }),
            (Err(primary), Ok(())) => Err(crate::OperationFailure::new((primary, None), work.work)),
            (Ok(()), Err(cleanup)) => Err(crate::OperationFailure::new((E::from(cleanup), None), work.work)),
            (Err(primary), Err(cleanup)) => Err(crate::OperationFailure::new((primary, Some(cleanup)), work.work)),
        }
    }

    pub(super) async fn import_selected<F, Fut, E>(
        &self, destination: crate::LazySnapshotRef, plan: crate::SelectedImportPlan,
        operation: crate::OperationId, permit: crate::PublicationPermit,
        carrier: super::adapter::NativeDemandCarrier<'_>,
        cancellation: &crate::CancellationToken, mut before_publication: F,
    ) -> Result<crate::LazySnapshotRef, E>
    where
        A: AsyncAuthorityStore, O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        let original = carrier.original_native_work().ok_or_else(|| E::from(MountSourceError::NativeWork(
            crate::NativeTreeMaterializationError::MissingCustody,
        )))?;
        let mut work = MountSyncWork::new(&self.authored, carrier)?;
        work.check().map_err(mount_demand_error)?;
        let _mutation = self.source_view.write_stable_with_carrier(
            None, &mut work.meter, cancellation, Some(self.authored.cancellation()),
        ).await?.into_mutation_phase();
        self.sync_staged(
            permit, &mut work, cancellation, &mut before_publication,
            Some(SelectedMountInput { destination, plan: &plan, operation, original: &original }),
        ).await?.ok_or_else(|| E::from(MountSourceError::OriginalSnapshotUnavailable))
    }

    /// Reads the acknowledged selected publication.
    pub(crate) async fn selected_import_snapshot(
        &self,
        operation: crate::OperationId,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<Option<crate::LazySnapshotRef>, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let Some(generation) = self.lazy.operation_generation(crate::IdempotencyKey::from_bytes(operation.into_bytes()), &LazyTreapWork::native_with_work(original, cancellation).map_err(native_lazy_error)?).await.map_err(native_lazy_error)? else { return Ok(None); };
        let mut carrier = super::adapter::NativeDemandCarrier::Native(original);
        let _stable = self.source_view.write_stable_with_carrier(
            None, &mut carrier, cancellation, Some(self.authored.cancellation()),
        ).await?.into_mutation_phase();
        let mut work = MountSyncWork::new(&self.authored, carrier)?;
        self.finish_acknowledged_mount_custody(
            operation, generation, &mut work, original, cancellation,
        ).await?.map(Some).ok_or(MountSourceError::Indeterminate)
    }

    /// Publishes pending authored writes on the callback runtime.
    pub fn sync(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        let _mutation = self.mutation_lease(None)?;
        self.wait(|| async {
            self.sync_staged_with_permit(crate::PublicationPermit::Unrestricted)
                .await
        })
    }

    async fn sync_staged_with_permit(
        &self,
        permit: crate::PublicationPermit,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let mut original = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, self.authored.cancellation());
        let mut work = MountSyncWork::new(&self.authored, super::adapter::NativeDemandCarrier::Borrowed(&mut original))?;
        self.sync_staged(
            permit, &mut work, self.authored.cancellation(),
            &mut self.authored.sdk_publication_policy(), None,
        ).await.map(|_| ())
    }

    pub(super) async fn sync_staged<F, Fut, E>(
        &self,
        permit: crate::PublicationPermit,
        work: &mut MountSyncWork<'_>,
        cancellation: &crate::CancellationToken,
        before_publication: &mut F,
        selected: Option<SelectedMountInput<'_>>,
    ) -> Result<Option<crate::LazySnapshotRef>, E>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        let selected_proof = if let Some(input) = &selected {
            let generation = self.lazy.operation_generation(crate::IdempotencyKey::from_bytes(input.operation.into_bytes()), &LazyTreapWork::native_with_work(input.original, cancellation).map_err(native_lazy_error)?).await.map_err(native_lazy_error)?;
            let proof = SelectedImportProof {
                checkpoint: input.destination, source: input.plan.source,
                source_config: input.plan.source_config,
                input: input.plan.input_digest(
                    input.destination, &LazyTreapWork::native_with_work(
                        input.original, cancellation,
                    ).map_err(native_lazy_error)?,
                ).map_err(native_lazy_error)?,
            };
            let custody = self.removals.lock().map_err(|_| MountSourceError::Stale)?.publication;
            if let Some(custody) = custody
                && (custody.operation != input.operation || custody.selected != Some(proof))
            {
                return Err(E::from(MountSourceError::Stale));
            }
            if let Some(generation) = generation {
                if custody.is_some() {
                    return self.finish_acknowledged_mount_custody(
                        input.operation, generation, work, input.original, cancellation,
                    ).await.map_err(E::from);
                }
                return self.lazy.selected_committed_snapshot(input.operation, generation, proof.checkpoint, proof.source, proof.source_config, proof.input, &LazyTreapWork::native_with_work(input.original, cancellation).map_err(native_lazy_error)?).await.map(Some).map_err(native_lazy_error).map_err(E::from);
            }
            if custody.is_some() {
                self.recover_pending_mount_change_measured(
                    permit, work, cancellation, before_publication,
                ).await?;
                let generation = self.lazy.operation_generation(crate::IdempotencyKey::from_bytes(input.operation.into_bytes()), &LazyTreapWork::native_with_work(input.original, cancellation).map_err(native_lazy_error)?).await.map_err(native_lazy_error)?.ok_or(MountSourceError::Indeterminate)?;
                return self.finish_acknowledged_mount_custody(
                    input.operation, generation, work, input.original, cancellation,
                ).await.map_err(E::from);
            }
            Some(proof)
        } else { None };
        self.recover_pending_mount_change_measured(
            permit, work, cancellation, before_publication,
        ).await?;
        let inputs = self.snapshot_sync_inputs(work, cancellation.clone()).await?;
        if selected.is_none() && inputs.paths.value.is_empty() && inputs.dirty.value.is_empty() {
            if work.observation == MountPublicationObservation::Qualified {
                let original = work.meter.original_native_work().ok_or(MountSourceError::Indeterminate)?;
                let context = crate::lazy_workspace::NativeSnapshotContext::Lazy(
                    self.lazy.publication_cut(None, &original, cancellation)
                        .await.map_err(native_lazy_error)?,
                );
                work.sync_authored_context_guarded(
                    &self.authored, permit, false, cancellation, before_publication, context,
                ).await?;
            } else {
                work.sync_authored_guarded(
                    &self.authored, permit, false, cancellation, before_publication,
                ).await?;
            }
            self.prune_detached(work)?;
            return Ok(None);
        }
        let PreparedMountSync { mut candidate, epoch, stamp, inputs, mut records } =
            self.prepare_sync_candidate_from_inputs(inputs, work, cancellation).await?;
        let mut effects = if let Some(input) = &selected {
            let effects = (async { let accounting = LazyTreapWork::native_with_work(input.original, cancellation)?; self.lazy.prepare_selected_import_into_checkout(
                input.destination, input.plan, &mut candidate.checkout, input.operation, &accounting,
            ).await }).await.map_err(native_lazy_error)?;
            let original_records = records.value.iter().map(|(record, metadata)| (record, metadata));
            let selected_records = effects.records.iter().map(|(record, metadata)| (record, metadata));
            let count = mount_record_union(
                original_records.clone(), selected_records.clone(), std::iter::empty(), None, &mut work.meter,
            )?;
            let mut merged = work.owned_vec(count)?;
            mount_record_union(
                original_records, selected_records, std::iter::empty(), Some(&mut merged.value), &mut work.meter,
            )?;
            records = merged;
            Some(effects)
        } else { None };
        let paths = &inputs.paths.value;
        let dirty = &inputs.dirty.value;
        let operation_id = {
            let config = candidate.checkout.volume_config();
            let mut removed = work.owned_vec(paths.len())?;
            for path in paths {
                let mut namespace = work.namespace_buffer(path, config)?;
                removed.absorb_buffer_guard(&mut namespace)?;
                removed.value.push(namespace.value.take().expect("prepared removal namespace"));
            }
            let scope = effects.as_ref().map_or(InstallScope::Paths(&removed.value), |effects|
                InstallScope::CombinedPaths(&removed.value, &effects.changes));
            let (installed, allocation) = super::adapter::installed_changes(
                &candidate.base, &candidate.checkout, scope, &mut work.meter, cancellation,
            ).await?;
            let installed = if let Some(allocation) = allocation {
                let bytes = allocation.bytes();
                MountSyncBuffer { value: Some(installed), custody: None, bytes, native_allocation: Some(allocation) }
            } else {
                let installed_bytes = mount_effect_bytes(&installed);
                work.adopt(installed_bytes).map_err(mount_demand_error)?;
                retain_sync_buffer(Some(installed), installed_bytes, work)?
            };
            self.stage_detached_records_measured(&records.value, dirty, work, cancellation).await?;
            let custody_items = self.removals.lock().map_err(|_| MountSourceError::Stale)?.paths.len()
                .checked_add(self.detached.lock().map_err(|_| MountSourceError::Stale)?.len())
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
            let custody_work = crate::WorkCounters { items_examined: custody_items, ..crate::WorkCounters::default() };
            work.reserve_items(custody_items)?;
            let record = |carrier: &mut super::adapter::NativeDemandCarrier<'_>| {
                super::adapter::record_view_change(&self.authored.shared_checkout().ledger,
                    &ViewChange::Effect(installed.value.as_ref().expect("original mounted effects")), carrier,
                )
            };
            let installed_result = self.authored.shared_checkout().finish_phase(
                candidate, epoch, stamp, &mut work.meter, cancellation,
                Some(self.authored.cancellation()), record, selected.as_ref().map(|input| input.operation),
                |publication, state| (publication, state.retained_operation_id()),
            ).await;
            work.restore_items(custody_items)?;
            let operation = match installed_result {
                Ok(Some((_, operation))) => operation,
                Ok(None) => return Err(E::from(MountSourceError::Stale)),
                Err(error) => return Err(E::from(error)),
            };
            let displaced = {
                let mut removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
                for removed in removals.paths.values_mut() { removed.operation = Some(operation); }
                let mut identities = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
                for (_, entry) in identities.iter_mut() {
                    if let Some(pending) = &mut entry.pending { pending.operation = Some(operation); }
                }
                removals.publication = Some(MountedPublicationCustody {
                    operation, published: false, selected: selected_proof,
                });
                std::mem::replace(&mut removals.selected_inputs, effects.take())
            };
            drop(displaced);
            work.record(custody_work).map_err(mount_demand_error)?;
            drop(removed);
            drop(installed);
            work.record(crate::WorkCounters::UNCHARGED).map_err(mount_demand_error)?;
            operation
        };
        let native_owned = match &work.meter {
            super::adapter::NativeDemandCarrier::Shared(meter) =>
                Some(crate::NativeMaterializationWork::from_meter(Arc::clone(meter))),
            super::adapter::NativeDemandCarrier::Borrowed(_)
                | super::adapter::NativeDemandCarrier::Native(_) => None,
        };
        let native = match &work.meter {
            super::adapter::NativeDemandCarrier::Native(original) => Some(*original),
            _ => native_owned.as_ref(),
        };
        let selected_inputs = SelectedInputsLoan::take(&self.removals, operation_id, work)?;
        if let Some(original) = native {
            (async { let accounting = LazyTreapWork::native_with_work(original, cancellation)?; self.lazy.prepare_mount_removals(paths, selected_inputs.value.as_ref().map_or(&[], |effects| effects.paths.as_slice()), &records.value, IdempotencyKey::from_bytes(operation_id.into_bytes()), &accounting).await }).await.map_err(native_lazy_error)?;
        } else {
            let (budget, prior_live) = work.sdk_scope()?;
            let result = (async { let accounting = LazyTreapWork::new(budget, cancellation); let result = self.lazy.prepare_mount_removals(paths, &[], &records.value, IdempotencyKey::from_bytes(operation_id.into_bytes()), &accounting).await; accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }).await;
            work.accept_sdk(result, prior_live, lazy_error)?;
        }
        selected_inputs.discard();
        drop(inputs);
        drop(records);
        work.record(crate::WorkCounters::UNCHARGED).map_err(mount_demand_error)?;
        if work.observation == MountPublicationObservation::Qualified {
            let original = work.meter.original_native_work().ok_or(MountSourceError::Indeterminate)?;
            let context = crate::lazy_workspace::NativeSnapshotContext::Lazy(
                self.lazy.publication_cut(Some(operation_id), &original, cancellation)
                    .await.map_err(native_lazy_error)?,
            );
            work.sync_authored_context_guarded(
                &self.authored, permit, true, cancellation, before_publication, context,
            ).await?;
        } else {
            work.sync_authored_guarded(
                &self.authored, permit, true, cancellation, before_publication,
            ).await?;
        }
        self.mark_mount_published(operation_id)?;
        if let Some(input) = &selected {
            let generation = self.lazy.operation_generation(crate::IdempotencyKey::from_bytes(operation_id.into_bytes()), &LazyTreapWork::native_with_work(input.original, cancellation).map_err(native_lazy_error)?).await.map_err(native_lazy_error)?.ok_or(MountSourceError::Indeterminate)?;
            return self.finish_acknowledged_mount_custody(
                operation_id, generation, work, input.original, cancellation,
            ).await.map_err(E::from);
        }
        let finished = if let Some(original) = native {
            (async { let accounting = LazyTreapWork::native_with_work(original, cancellation)?; self.lazy.finish_mount_removals(&accounting).await }).await.map_err(native_lazy_error)?
        } else {
            let (budget, prior_live) = work.sdk_scope()?;
            let result = (async { let accounting = LazyTreapWork::new(budget, cancellation); let result = self.lazy.finish_mount_removals(&accounting).await; accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }).await;
            work.accept_sdk(result, prior_live, lazy_error)?
        };
        if !finished { return Err(E::from(MountSourceError::Stale)); }
        self.finish_mount_custody(operation_id, work)?;
        Ok(None)
    }

    fn clear_mount_removals(
        &self,
        operation: crate::OperationId,
        work: &mut MountSyncWork<'_>,
    ) -> Result<(), MountSourceError> {
        let mut removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        work.items(removals.paths.len())?;
        let removing = removals.paths.values().filter(|removed| removed.operation == Some(operation)).count();
        let cleanup = crate::WorkCounters {
            items_examined: (removals.paths.len() as u64).checked_add(removing as u64)
                .and_then(|items| items.checked_add(removals.records.len() as u64))
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?,
            ..crate::WorkCounters::default()
        };
        work.check().map_err(mount_demand_error)?;
        work.admit(&cleanup)?;
        let MountedRemovals { paths, records, .. } = &mut *removals;
        paths.retain(|_, removed| {
            if removed.operation != Some(operation) { return true; }
            if let Some(record) = records.get_mut(&removed.file_id) {
                record.paths = record.paths.checked_sub(1).expect("captured identity has a removal owner");
            }
            false
        });
        records.retain(|_, record| record.paths != 0);
        work.record(cleanup).map_err(mount_demand_error)?;
        Ok(())
    }

    async fn snapshot_sync_inputs(
        &self, work: &mut MountSyncWork<'_>, cancellation: crate::CancellationToken,
    ) -> Result<MountSyncInputs, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        drop(work.lock_checkout(&self.authored, cancellation.clone()).await?);
        let (paths, removed_ids) = {
            let state = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
            let mut paths = work.owned_vec::<String>(state.paths.len())?;
            let mut ids = work.owned_vec::<FileId>(state.paths.len())?;
            for (path, removed) in &state.paths {
                work.items(1)?;
                paths.push_cloned_text(&path.text, work)?;
                let copied = crate::WorkCounters {
                    bytes_copied: std::mem::size_of::<FileId>() as u64,
                    ..crate::WorkCounters::UNCHARGED
                };
                work.admit(&copied)?;
                ids.value.push(removed.file_id);
                work.record(copied).map_err(mount_demand_error)?;
            }
            (paths, ids)
        };
        let detached = {
            let state = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
            let mut files = work.owned_vec::<(FileId, Arc<CheckoutDetachedFile<A, O>>, u64)>(state.len())?;
            for (id, entry) in state.iter() {
                work.items(1)?;
                if entry.ready {
                    let copied = crate::WorkCounters {
                        bytes_copied: std::mem::size_of::<(FileId, Arc<CheckoutDetachedFile<A, O>>, u64)>() as u64,
                        ..crate::WorkCounters::UNCHARGED
                    };
                    work.admit(&copied)?;
                    files.value.push((*id, Arc::clone(&entry.file), entry.published_epoch));
                    work.record(copied).map_err(mount_demand_error)?;
                }
            }
            files
        };
        let mut dirty = work.owned_vec::<DirtyDetachedRecord>(0)?;
        for (id, file, published) in &detached.value {
            work.items(1)?;
            let (record, metadata, epoch) = file.snapshot(&mut work.meter, &cancellation).await?;
            if epoch > *published {
                if dirty.value.capacity() == 0 { dirty = work.owned_vec(detached.value.len())?; }
                let copied = crate::WorkCounters {
                    bytes_copied: std::mem::size_of::<DirtyDetachedRecord>() as u64,
                    ..crate::WorkCounters::UNCHARGED
                };
                work.admit(&copied)?;
                dirty.value.push((*id, record, metadata, epoch));
                work.record(copied).map_err(mount_demand_error)?;
            }
        }
        drop(detached);
        work.record(crate::WorkCounters::UNCHARGED).map_err(mount_demand_error)?;
        Ok(MountSyncInputs { paths, removed_ids, dirty })
    }

    async fn prepare_sync_candidate_from_inputs(
        &self, inputs: MountSyncInputs, work: &mut MountSyncWork<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<PreparedMountSync<A, O>, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let (candidate, epoch, stamp, identity_records) = self.prepare_removed_identity_candidate_measured(
            &inputs.paths.value, &inputs.removed_ids.value, &inputs.dirty.value, work, cancellation,
        ).await?;
        let records = {
            let captured = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
            let captured_records = captured.records.values().map(|entry| (&entry.value.0, &entry.value.1));
            let candidate_records = identity_records.value.iter().map(|(record, metadata)| (record, metadata));
            let dirty_records = inputs.dirty.value.iter().map(|(_, record, metadata, _)| (record, metadata));
            let count = mount_record_union(
                captured_records.clone(), candidate_records.clone(), dirty_records.clone(), None, &mut work.meter,
            )?;
            let mut records = work.owned_vec(count)?;
            mount_record_union(
                captured_records, candidate_records, dirty_records, Some(&mut records.value), &mut work.meter,
            )?;
            records
        };
        drop(identity_records);
        work.record(crate::WorkCounters::UNCHARGED).map_err(mount_demand_error)?;
        Ok(PreparedMountSync { candidate, epoch, stamp, inputs, records })
    }

    #[cfg(test)]
    async fn prepare_removed_identity_candidate(
        &self,
        removals: &BTreeMap<String, FileId>,
        dirty: &[DirtyDetachedRecord],
    ) -> Result<(crate::OperationId, CheckoutCandidate<A, O>, MountSyncBuffer<Vec<(FileRecord, FileMetadata)>>), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let paths = removals.keys().cloned().collect::<Vec<_>>();
        let ids = removals.values().copied().collect::<Vec<_>>();
        let mut original = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, self.authored.cancellation());
        let mut work = MountSyncWork::new(&self.authored, super::adapter::NativeDemandCarrier::Borrowed(&mut original))?;
        let (candidate, _, _, records) = self.prepare_removed_identity_candidate_measured(
            &paths, &ids, dirty, &mut work, self.authored.cancellation(),
        ).await?;
        let operation_id = self.authored.shared_checkout().lock().await.retained_operation_id();
        Ok((operation_id, candidate, records))
    }

    async fn prepare_removed_identity_candidate_measured(
        &self,
        paths: &[String],
        removed_ids: &[FileId],
        dirty: &[DirtyDetachedRecord],
        work: &mut MountSyncWork<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<(CheckoutCandidate<A, O>, u64, ViewStamp, MountSyncBuffer<Vec<(FileRecord, FileMetadata)>>), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let selected = self.removals.lock().map_err(|_| MountSourceError::Stale)?
            .publication.filter(|custody| custody.selected.is_some() && !custody.published);
        let (mut transaction, epoch, stamp) = self.authored.shared_checkout()
            .prepare_native_phase_for_operation_with_carrier(
                selected.map(|custody| custody.operation), &mut work.meter,
                cancellation, Some(self.authored.cancellation()),
            ).await?;
        let candidate = &mut transaction.checkout;
        let mut records = work.owned_vec::<(FileRecord, FileMetadata)>(paths.len())?;
        for (path, removed_id) in paths.iter().zip(removed_ids) {
            work.items(1)?;
            let mut namespace = work.namespace_buffer(path, candidate.volume_config())?;
            let lookup = if let Some(original) = work.meter.original_native_work() {
                candidate.lookup_no_follow_with_metadata(
                    namespace.value.as_ref().expect("original removal namespace"), &original, cancellation,
                ).await.map_err(mount_engine_error)?
            } else {
                let (budget, prior_live) = work.sdk_scope()?;
                let result = candidate.lookup_no_follow_with_metadata(
                    namespace.value.as_ref().expect("original removal namespace"), budget, cancellation,
                ).await;
                work.accept_sdk(result, prior_live, mount_engine_error)?
            };
            work.record(crate::WorkCounters::UNCHARGED).map_err(mount_demand_error)?;
            let Some(lookup) = lookup else {
                drop(namespace);
                continue;
            };
            if lookup.record.file_id != *removed_id {
                drop(namespace);
                continue;
            }
            if lookup.record.kind != FileKind::Regular { return Err(MountSourceError::Stale); }
            mount_upsert_record(&mut records.value, (lookup.record, lookup.metadata), &mut work.meter)?;
            if let Some(original) = work.meter.original_native_work() {
                let mut operations = work.owned_vec(1)?;
                operations.absorb_buffer_guard(&mut namespace)?;
                operations.value.push(Mutation::Remove {
                    path: namespace.value.take().expect("original removal namespace"),
                    expected_file_id: MetadataField::Value(*removed_id),
                });
                let (operations, allocation) = operations.take_native_parts()?;
                candidate.mutate_owned_with_accounting(crate::kernel::Owned { value: operations, bytes: allocation.bytes(), allocation: crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) }, &mut crate::kernel::allocation::KernelAccounting::native(&original, cancellation), cancellation)
                    .await.map_err(mount_engine_error)?;
            } else {
                let (budget, prior_live) = work.sdk_scope()?;
                let result = candidate.mutate(vec![crate::kernel::Mutation::Remove { path: namespace.value.take().expect("original removal namespace"), expected_file_id: (Some(*removed_id)).map_or(crate::kernel::MetadataField::Unavailable, crate::kernel::MetadataField::Value) }], budget, cancellation).await;
                work.accept_sdk(result, prior_live, mount_engine_error)?;
            }
            work.record(crate::WorkCounters::UNCHARGED).map_err(mount_demand_error)?;
            drop(namespace);
        }
        for index in 0..dirty.len() {
            let (file_id, record, _, _) = dirty[index];
            work.items(1)?;
            let result = if let Some(original) = work.meter.original_native_work() {
                candidate.read_file_record_by_id(file_id, &original, cancellation)
                    .await.map_err(mount_engine_error)
            } else {
                let (budget, prior_live) = work.sdk_scope()?;
                let result = candidate.read_file_record_by_id(file_id, budget, cancellation).await;
                work.accept_sdk(result, prior_live, mount_engine_error)
            };
            let live = match result {
                Ok(record) => Some(record),
                Err(error) if matches!(error.semantic_error(), MountSourceError::NotFound) => None,
                Err(error) => return Err(error),
            };
            work.record(crate::WorkCounters::UNCHARGED).map_err(mount_demand_error)?;
            if let Some(live) = live {
                if live.kind != record.kind { return Err(MountSourceError::Stale); }
                let mut replacements = work.owned_vec(1)?;
                replacements.value.push(Mutation::File {
                    file_id: file_id, mutation: FileMutation::ReplaceRecord { record: record },
                });
                if let Some(original) = work.meter.original_native_work() {
                    let (operations, allocation) = replacements.take_native_parts()?;
                    candidate.mutate_owned_with_accounting(crate::kernel::Owned { value: operations, bytes: allocation.bytes(), allocation: crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) }, &mut crate::kernel::allocation::KernelAccounting::native(&original, cancellation), cancellation)
                        .await.map_err(mount_engine_error)?;
                } else {
                    let (budget, prior_live) = work.sdk_scope()?;
                    let result = candidate.mutate(
                        std::mem::take(&mut replacements.value), budget, cancellation,
                    ).await;
                    work.accept_sdk(result, prior_live, mount_engine_error)?;
                }
                work.record(crate::WorkCounters::UNCHARGED).map_err(mount_demand_error)?;
                drop(replacements);
            }
        }
        Ok((transaction, epoch, stamp, records))
    }

    /// Advances the authored checkout after an external publication.
    pub async fn advance_to_head_async(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let _writer = self.source_view.write().await;
        if self.lazy.binding().await.is_ok() {
            let result = self.authored.advance_to_head_async().await;
            self.cursors.clear();
            return result;
        }
        self.source_view.begin_transition();
        let result = async {
            self.authored.advance_to_head_async().await?;
            self.lazy.rebind_source(&crate::lazy_workspace::LazyTreapWork::new(
                crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
            )).await.map_err(lazy_error)?;
            // Every unauthored node may read differently from the new epoch.
            self.authored
                .record_projection_change(&ViewChange::Everything);
            self.cursors.clear();
            Ok(())
        }
        .await;

        if result.is_ok() {
            self.source_view.finish_transition();
        }
        result
    }

    fn path(&self, path: &MountPath) -> Result<String, MountSourceError> {
        let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, self.authored.cancellation());
        mount_path_text(&self.root, path, &mut work)
    }

    fn removed_by(paths: &BTreeMap<MountedRemovalPath, MountedRemoval>, path: &str) -> bool {
        let mut current = path;
        loop {
            if paths.contains_key(current) {
                return true;
            }
            if current == "/" {
                return false;
            }
            current =
                current.rsplit_once('/').map_or(
                    "/",
                    |(parent, _)| if parent.is_empty() { "/" } else { parent },
                );
        }
    }

    fn is_removed(&self, path: &str) -> Result<bool, MountSourceError> {
        let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        Ok(Self::removed_by(&removals.paths, path))
    }

    fn removed_by_with_work(
        paths: &BTreeMap<MountedRemovalPath, MountedRemoval>, path: &str,
        work: &mut crate::demand::DemandWork,
    ) -> Result<bool, MountSourceError> {
        Self::removed_by_with_items(paths, path, || mount_items(work, 1))
    }

    fn removed_by_with_items(
        paths: &BTreeMap<MountedRemovalPath, MountedRemoval>, path: &str,
        mut visit: impl FnMut() -> Result<(), MountSourceError>,
    ) -> Result<bool, MountSourceError> {
        let mut current = path;
        loop {
            visit()?;
            if paths.contains_key(current) { return Ok(true); }
            if current == "/" { return Ok(false); }
            current = current.rsplit_once('/').map_or("/", |(parent, _)|
                if parent.is_empty() { "/" } else { parent });
        }
    }

    fn is_removed_with_work(
        &self, path: &str, work: &mut crate::demand::DemandWork,
    ) -> Result<bool, MountSourceError> {
        let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        Self::removed_by_with_work(&removals.paths, path, work)
    }

    fn removed_identity_with_work(
        &self, path: &str, file_id: FileId, work: &mut crate::demand::DemandWork,
    ) -> Result<bool, MountSourceError> {
        mount_items(work, 1)?;
        self.removed_identity(path, file_id)
    }

    fn removed_identity(
        &self, path: &str, file_id: FileId,
    ) -> Result<bool, MountSourceError> {
        let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        Ok(removals.paths.get(path).is_some_and(|removed| removed.file_id == file_id))
    }

    fn removed_identity_with_carrier(
        &self, path: &str, file_id: FileId,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<bool, MountSourceError> {
        if let Some(original) = carrier.original_native_work() {
            original.charge(crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED
            }).map_err(MountSourceError::NativeWork)?;
            self.removed_identity(path, file_id)
        } else {
            carrier.with_work(|work| self.removed_identity_with_work(path, file_id, work))
        }
    }

    fn removal_snapshot_with_work(
        &self, directory: &str, work: &mut crate::demand::DemandWork,
    ) -> Result<(Vec<String>, bool, u64), MountSourceError> {
        let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        let opaque = Self::removed_by_with_work(&removals.paths, directory, work)?;
        let mut scratch = super::adapter::CaptureScratchAllocation::new(work);
        let children = Self::removal_children_with_scratch(&removals.paths, directory, &mut scratch)?;
        scratch.retain();
        Ok((children, opaque, removals.epoch))
    }

    fn removal_snapshot_with_carrier(
        &self, directory: &str, carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<(MountOwnedBuffer<Vec<String>>, bool, u64), MountSourceError> {
        if let Some(original) = carrier.original_native_work() {
            let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
            let opaque = Self::removed_by_with_items(&removals.paths, directory, || original.charge(
                crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED },
            ).map_err(MountSourceError::NativeWork))?;
            let mut scratch = super::adapter::NativeCaptureScratchAllocation::new(&original)?;
            let children = Self::removal_children_with_scratch(&removals.paths, directory, &mut scratch)?;
            Ok((MountOwnedBuffer { value: children, legacy_bytes: 0,
                allocation: Some(scratch.retain()) }, opaque, removals.epoch))
        } else {
            let (children, opaque, epoch) = carrier.with_work(|work|
                self.removal_snapshot_with_work(directory, work))?;
            let bytes = directory_removal_bytes(&children)?;
            Ok((MountOwnedBuffer { value: children, legacy_bytes: bytes, allocation: None }, opaque, epoch))
        }
    }

    fn removal_children_with_scratch<C: super::adapter::CaptureInputScratch>(
        paths: &BTreeMap<MountedRemovalPath, MountedRemoval>, directory: &str, scratch: &mut C,
    ) -> Result<Vec<String>, MountSourceError> {
        let prefix: std::borrow::Cow<'_, str> = if directory == "/" {
            std::borrow::Cow::Borrowed("/")
        } else {
            let length = directory.len().checked_add(1)
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            let copied = crate::WorkCounters { bytes_copied: length as u64, ..crate::WorkCounters::UNCHARGED };
            scratch.admit(copied)?;
            let mut bytes = scratch.vec::<u8>(length)?;
            bytes.extend_from_slice(directory.as_bytes());
            bytes.push(b'/');
            scratch.record(copied)?;
            std::borrow::Cow::Owned(String::from_utf8(bytes).expect("Unicode removal prefix"))
        };
        let range = || paths.range::<str, _>((
            std::ops::Bound::Included(prefix.as_ref()), std::ops::Bound::Unbounded,
        ));
        let mut count = 0_usize;
        for (path, _) in range() {
            scratch.items(1)?;
            if !path.starts_with(prefix.as_ref()) { break; }
            if !path[prefix.len()..].contains('/') {
                count = count.checked_add(1).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            }
        }
        let mut children = scratch.vec::<String>(count)?;
        for (path, _) in range() {
            scratch.items(1)?;
            if !path.starts_with(prefix.as_ref()) { break; }
            if path[prefix.len()..].contains('/') { continue; }
            let copied = crate::WorkCounters { bytes_copied: path.len() as u64, ..crate::WorkCounters::UNCHARGED };
            scratch.admit(copied)?;
            let mut bytes = scratch.vec::<u8>(path.len())?;
            bytes.extend_from_slice(path.as_bytes());
            children.push(String::from_utf8(bytes).expect("Unicode removal path"));
            scratch.record(copied)?;
        }
        let prefix_bytes = match &prefix { std::borrow::Cow::Borrowed(_) => 0,
            std::borrow::Cow::Owned(prefix) => prefix.capacity() as u64 };
        drop(prefix);
        scratch.release(prefix_bytes)?;
        Ok(children)
    }

    async fn mutation_file_id_with_carrier(
        &self,
        text: &str,
        lookup: &LazyLookup,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<FileId, MountSourceError> {
        if let Some(original) = carrier.original_native_work() {
            let caller = carrier.cancellation();
            return self.lazy.stable_file_id_for_lookup(
                text, lookup, &mut crate::lazy_workspace::LazyReadonlyAccounting::Native { original: &original, cancellation: &caller },
            ).await.map_err(native_lazy_error);
        }
        let (budget, caller) = carrier.with_work(|work| {
            Ok::<_, MountSourceError>((
                work.remaining().map_err(mount_demand_error)?, work.cancellation().clone(),
            ))
        })?;
        let mut accounting = crate::lazy_workspace::LazyReadonlyAccounting::Portable {
            work: crate::WorkCounters::UNCHARGED, budget, cancellation: &caller,
        };
        let result = self.lazy.stable_file_id_for_lookup(text, lookup, &mut accounting).await;
        let result = accounting.finish_measured(result).map_err(LazyWorkspaceError::into_measured_failure);
        carrier.with_work(|work| mount_accept(work, result, lazy_error))
    }

    fn reserve_detached_slot_with_carrier(
        &self, file_id: FileId, carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<Option<usize>, MountSourceError> {
        let Some(original) = carrier.original_native_work() else {
            return carrier.with_work(|work| self.reserve_detached_slot_with_work(file_id, work));
        };
        let mut entries = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
        let position = match carrier.with_work(|work| detached_position(&entries, file_id, work))? {
            Ok(_) => return Ok(None),
            Err(position) => position,
        };
        if entries.len() < entries.capacity() { return Ok(Some(position)); }
        use super::adapter::CaptureInputScratch;
        let count = entries.len().checked_add(1).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let copied = entries.len().checked_mul(std::mem::size_of::<(FileId, DetachedIdentity<A, O>)>())
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
        let copy = original.reserve_work(crate::WorkCounters { bytes_copied: copied, ..crate::WorkCounters::UNCHARGED }).map_err(MountSourceError::NativeWork)?;
        let mut items = original.reserve_work(crate::WorkCounters { items_examined: entries.len() as u64, ..crate::WorkCounters::UNCHARGED }).map_err(MountSourceError::NativeWork)?;
        let mut scratch = super::adapter::NativeCaptureScratchAllocation::new(&original)?;
        let values = scratch.vec::<(FileId, DetachedIdentity<A, O>)>(count)?;
        let bytes = values.capacity().checked_mul(std::mem::size_of::<(FileId, DetachedIdentity<A, O>)>())
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
        let mut replacement = DetachedTable {
            entries: values, custody: None, bytes, native_allocation: Some(scratch.retain()),
        };
        let examined = entries.len() as u64;
        replacement.entries.append(&mut entries.entries);
        let old = std::mem::replace(&mut *entries, replacement);
        drop(old);
        let recorded_copy = { let mut copy = copy; copy.record_completed(crate::WorkCounters { bytes_copied: copied, ..crate::WorkCounters::UNCHARGED }) }.map_err(MountSourceError::NativeWork);
        let recorded_items = items.record_completed(crate::WorkCounters { items_examined: examined, ..crate::WorkCounters::UNCHARGED }).map_err(MountSourceError::NativeWork);
        recorded_copy.and(recorded_items)?;
        Ok(Some(position))
    }

    fn reserve_detached_slot_with_work(
        &self,
        file_id: FileId,
        work: &mut crate::demand::DemandWork,
    ) -> Result<Option<usize>, MountSourceError> {
        let mut entries = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
        let position = match detached_position(&entries, file_id, work)? {
            Ok(_) => return Ok(None),
            Err(position) => position,
        };
        if entries.len() < entries.capacity() { return Ok(Some(position)); }
        let mut scratch = DirectoryScratch::new(work);
        let custody = super::NativeMountAllocationCustody::new(scratch.work)
            .map_err(mount_demand_error)?;
        let count = entries.len().checked_add(1).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let copied = crate::WorkCounters {
            items_examined: entries.len() as u64,
            bytes_copied: (entries.len() * std::mem::size_of::<(FileId, DetachedIdentity<A, O>)>()) as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        scratch.work.work.admit(&copied, &scratch.work.budget).map_err(MountSourceError::Work)?;
        let replacement = mount_vec(count, scratch.work)?;
        let new_bytes = (replacement.capacity() * std::mem::size_of::<(FileId, DetachedIdentity<A, O>)>()) as u64;
        scratch.track(new_bytes)?;
        custody.retain_admitted(new_bytes, scratch.work).map_err(mount_demand_error)?;
        let mut replacement = DetachedTable { entries: replacement, custody: Some(custody), bytes: new_bytes, native_allocation: None };
        scratch.keep(new_bytes)?;
        replacement.entries.append(&mut entries.entries);
        let old = std::mem::replace(&mut *entries, replacement);
        drop(old);
        scratch.work.record(copied).map_err(mount_demand_error)?;
        Ok(Some(position))
    }

    async fn prepare_removed_identity_with_carrier(
        &self,
        lookup: MountLookup,
        owner: super::view_gate::CallbackOwner,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<PreparedMountedIdentity<A, O>, MountSourceError> {
        let mut scratch = DirectoryCarrierScratch::new(carrier);
        let record = self.authored.record_by_id(lookup.node.file_id, owner, scratch.carrier).await?;
        let existing = scratch.carrier.with_work(|work| {
            let entries = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
            Ok::<_, MountSourceError>(match detached_position(&entries, record.file_id, work)? {
                Ok(position) => Some(Arc::clone(&entries[position].1.file)),
                Err(_) => None,
            })
        })?;
        let file = if let Some(file) = existing { file } else {
            self.authored.detached_file_from_record(record, lookup.metadata, owner, scratch.carrier).await?
        };
        let position = self.reserve_detached_slot_with_carrier(record.file_id, scratch.carrier)?;
        let (paths, new_record, logical_bytes, final_work) = scratch.carrier.with_work(|work| {
            let entries = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
            let shift = position.map_or(0, |position| entries.len() - position);
            drop(entries);
            let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
            mount_items(work, 1)?;
            let (paths, new_record) = if let Some(existing) = removals.records.get(&record.file_id) {
                (existing.paths, false)
            } else {
                let mut paths = 0_usize;
                for removed in removals.paths.values() {
                    mount_items(work, 1)?;
                    if removed.file_id == record.file_id {
                        paths = paths.checked_add(1).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
                    }
                }
                (paths, true)
            };
            // Logical inline data only; no BTree node layout or allocator event.
            let logical_bytes = if new_record {
                (std::mem::size_of::<FileId>() + std::mem::size_of::<CapturedRemovalData>()) as u64
            } else { 0 };
            work.adopt(logical_bytes).map_err(mount_demand_error)?;
            let copied = if new_record { logical_bytes } else {
                std::mem::size_of::<(FileRecord, FileMetadata)>() as u64
            };
            let final_work = crate::WorkCounters {
                items_examined: 1 + u64::from(position.is_some()),
                bytes_copied: copied + (shift * std::mem::size_of::<(FileId, DetachedIdentity<A, O>)>()) as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            Ok::<_, MountSourceError>((paths, new_record, logical_bytes, final_work))
        })?;
        scratch.track(logical_bytes)?;
        let captured = if new_record {
            let custody = scratch.carrier.new_custody()?;
            scratch.carrier.with_work(|work| custody.retain_admitted(logical_bytes, work))
                .map_err(mount_demand_error)?;
            scratch.keep(logical_bytes)?;
            Some(CapturedRemovalRecord {
                value: (record, lookup.metadata), paths, custody: Some(custody), bytes: logical_bytes,
            })
        } else { None };
        super::adapter::record_view_change(&self.authored.shared_checkout().ledger, &ViewChange::Node(record.file_id), scratch.carrier)?;
        Ok(PreparedMountedIdentity {
            record, metadata: lookup.metadata, file, position, paths,
            captured, final_work,
        })
    }

    fn prepare_removed_path_with_carrier(
        &self,
        path: &MountPath,
        key: MountOwnedBuffer<String>,
        file_id: FileId,
        identity: Option<&PreparedMountedIdentity<A, O>>,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<PreparedMountedRemoval, MountSourceError> {
        let mut scratch = DirectoryCarrierScratch::new(carrier);
        let (prior, changed, old_paths, new_paths, logical_bytes, final_work) = scratch.carrier.with_work(|work| {
            let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
            mount_items(work, 1)?;
            let prior = removals.paths.get(key.value.as_str()).copied();
            let changed = prior.map(|prior| prior.file_id) != Some(file_id);
            let mut paths_for = |id| -> Result<Option<usize>, MountSourceError> {
                if let Some(identity) = identity.filter(|identity| identity.record.file_id == id) {
                    return Ok(Some(identity.paths));
                }
                mount_items(work, 1)?;
                Ok(removals.records.get(&id).map(|record| record.paths))
            };
            let old_paths = if changed {
                if let Some(prior) = prior {
                    paths_for(prior.file_id)?.map(|paths| paths.checked_sub(1)
                        .ok_or(MountSourceError::Stale)).transpose()?
                } else { None }
            } else { None };
            let new_paths = if changed {
                paths_for(file_id)?.map(|paths| paths.checked_add(1)
                    .ok_or(MountSourceError::Work(crate::WorkError::Overflow))).transpose()?
            } else { None };
            let logical_bytes = if prior.is_none() { std::mem::size_of::<MountedRemoval>() as u64 } else { 0 };
            work.adopt(logical_bytes).map_err(mount_demand_error)?;
            Ok::<_, MountSourceError>((prior, changed, old_paths, new_paths, logical_bytes, crate::WorkCounters {
                items_examined: 1 + u64::from(old_paths.is_some()) + u64::from(new_paths.is_some()),
                bytes_copied: std::mem::size_of::<MountedRemoval>() as u64,
                ..crate::WorkCounters::UNCHARGED
            }))
        })?;
        scratch.track(logical_bytes)?;
        let namespace = self.authored.namespace_path(path, scratch.carrier)?;
        if namespace.1.is_none() { scratch.track(namespace.0.retained_bytes())?; }
        super::adapter::record_view_change(&self.authored.shared_checkout().ledger, &ViewChange::Entry(&namespace.0, file_id), scratch.carrier)?;
        let bytes = key.legacy_owned_bytes().checked_add(logical_bytes)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let custody = scratch.carrier.new_custody()?;
        scratch.carrier.with_work(|work| custody.retain_admitted(bytes, work)).map_err(mount_demand_error)?;
        scratch.keep(logical_bytes)?;
        let key = MountedRemovalPath {
            text: key.value, custody: Some(custody), bytes, native_allocation: key.allocation,
        };
        Ok(PreparedMountedRemoval { key, file_id, prior, old_paths, new_paths, changed, final_work })
    }

    fn prepare_rebound_with_carrier(
        &self,
        key: &str,
        identity: Option<&PreparedMountedIdentity<A, O>>,
        removal: &PreparedMountedRemoval,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<PreparedMountedRebound, MountSourceError> {
        carrier.with_work(|work| {
            let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
            mount_items(work, 1)?;
            let removed = removals.paths.get_key_value(key);
            let removed = removed.map(|(_, removed)| *removed);
            let paths = if let Some(removed) = removed {
                let before = if removal.changed && removal.file_id == removed.file_id {
                    removal.new_paths
                } else if removal.changed && removal.prior.is_some_and(|prior| prior.file_id == removed.file_id) {
                    removal.old_paths.filter(|paths| *paths != 0)
                } else if let Some(identity) = identity.filter(|identity| identity.record.file_id == removed.file_id) {
                    Some(identity.paths)
                } else {
                    mount_items(work, 1)?;
                    removals.records.get(&removed.file_id).map(|record| record.paths)
                };
                before.map(|paths| paths.checked_sub(1).ok_or(MountSourceError::Stale)).transpose()?
            } else { None };
            Ok(PreparedMountedRebound { removed, paths, final_work: crate::WorkCounters {
                items_examined: u64::from(removed.is_some()) + u64::from(paths.is_some()),
                ..crate::WorkCounters::UNCHARGED
            } })
        })
    }

    fn commit_removed_with_carrier(
        &self,
        removal: PreparedMountedRemoval,
        identity: Option<PreparedMountedIdentity<A, O>>,
        rebound: Option<(&str, PreparedMountedRebound)>,
        scratch: &mut DirectoryCarrierScratch<'_, '_>,
    ) -> Result<(), MountSourceError> {
        let mut delta = removal.final_work;
        if let Some(identity) = &identity { delta = delta.checked_add(identity.final_work)?; }
        if let Some((_, rebound)) = &rebound { delta = delta.checked_add(rebound.final_work)?; }
        if delta.items_examined > scratch.reserved_items || delta.bytes_copied > scratch.reserved_copies {
            return Err(MountSourceError::Work(crate::WorkError::Overflow));
        }
        let mut completed = crate::WorkCounters::UNCHARGED;
        let result = (|| {
            if let Some(identity) = identity {
                let record_copies = if identity.captured.is_some() {
                    (std::mem::size_of::<FileId>() + std::mem::size_of::<CapturedRemovalData>()) as u64
                } else {
                    std::mem::size_of::<(FileRecord, FileMetadata)>() as u64
                };
                if let Some(position) = identity.position {
                    let mut detached = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
                    completed.items_examined += 1;
                    detached.insert(position, (
                        identity.record.file_id,
                        DetachedIdentity { file: identity.file, ready: true, published_epoch: 0, pending: None },
                    ));
                    completed.bytes_copied += identity.final_work.bytes_copied - record_copies;
                }
                let mut removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
                completed.items_examined += 1;
                if let Some(captured) = identity.captured {
                    removals.records.insert(identity.record.file_id, captured);
                } else {
                    removals.records.get_mut(&identity.record.file_id).ok_or(MountSourceError::Stale)?
                        .value = (identity.record, identity.metadata);
                }
                completed.bytes_copied += record_copies;
            }
            let mut removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
            completed.items_examined += 1;
            removals.paths.insert(removal.key, MountedRemoval { file_id: removal.file_id, operation: None });
            completed.bytes_copied += std::mem::size_of::<MountedRemoval>() as u64;
            if removal.changed {
                if let (Some(prior), Some(paths)) = (removal.prior, removal.old_paths) {
                    completed.items_examined += 1;
                    if paths == 0 { removals.records.remove(&prior.file_id); } else {
                        removals.records.get_mut(&prior.file_id).ok_or(MountSourceError::Stale)?.paths = paths;
                    }
                }
                if let Some(paths) = removal.new_paths {
                    completed.items_examined += 1;
                    removals.records.get_mut(&removal.file_id).ok_or(MountSourceError::Stale)?.paths = paths;
                }
                removals.epoch = removals.epoch.wrapping_add(1);
            }
            if let Some((key, rebound)) = &rebound {
                if let Some(removed) = rebound.removed {
                    completed.items_examined += 1;
                    removals.paths.remove(*key);
                    if let Some(paths) = rebound.paths {
                        completed.items_examined += 1;
                        if paths == 0 { removals.records.remove(&removed.file_id); } else {
                            removals.records.get_mut(&removed.file_id).ok_or(MountSourceError::Stale)?.paths = paths;
                        }
                    }
                    removals.epoch = removals.epoch.wrapping_add(1);
                }
            }
            Ok(())
        })();
        let items = std::mem::take(&mut scratch.reserved_items);
        let copies = std::mem::take(&mut scratch.reserved_copies);
        let accounted = scratch.carrier.with_work(|work| {
            let copied = work.complete(crate::materializer::NativeResourceCredit::Copy, copies, completed.bytes_copied);
            let examined = work.complete(crate::materializer::NativeResourceCredit::Items, completed.items_examined, completed.items_examined);
            work.refund(crate::materializer::NativeResourceCredit::Items, items - completed.items_examined).expect("original reservation is balanced");
            copied.and(examined)
        });
        match (result, accounted) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(primary), Ok(())) => Err(primary),
            (Ok(()), Err(cleanup)) => Err(mount_demand_error(cleanup)),
            (Err(primary), Err(cleanup)) => Err(scratch.finalization_error(primary, cleanup)),
        }
    }

    fn set_attributes_with_carrier(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        with_native_callback_carrier(&self.runtime, carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let _mutation = self.source_mutation_lease_with_carrier(None, carrier).await?;
                let owner = SourceViewGate::callback_owner();
                let mut scratch = DirectoryCarrierScratch::new(carrier);
                let detached_present = scratch.carrier.with_work(|work| {
                    mount_items(work, 1)?;
                    Ok::<_, MountSourceError>(!self.detached.lock()
                        .map_err(|_| MountSourceError::Stale)?.is_empty())
                })?;
                if detached_present {
                    let text = mount_path_text_with_carrier(&self.root, path, scratch.carrier)?;
                    scratch.track(text.legacy_owned_bytes())?;
                    let resolution = self.resolve(path, &text, owner, scratch.carrier).await?;
                    if let Some(file) = self.content_detached_with_carrier(&resolution, &text, scratch.carrier).await? {
                        return file.set_attributes_async_with_carrier(metadata, logical_bytes, scratch.carrier).await;
                    }
                }
                self.promote_with_carrier(path, owner, scratch.carrier).await?;
                self.authored.set_attributes_async_with_carrier(path, metadata, logical_bytes, scratch.carrier).await
            })
        })
    }

    fn record_rebound(&self, path: &str) -> Result<(), MountSourceError> {
        let mut removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        if removals.rebound(path) {
            self.cursors.clear();
        }
        Ok(())
    }

    async fn promote_with_carrier(
        &self, mounted: &MountPath, owner: super::view_gate::CallbackOwner,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        let mut scratch = DirectoryCarrierScratch::new(carrier);
        let path = mount_path_text_with_carrier(&self.root, mounted, scratch.carrier)?;
        scratch.track(path.legacy_owned_bytes())?;
        let resolution = self.resolve(
            mounted, &path, owner, scratch.carrier,
        ).await?;
        let (lookup, _, allocation) = match resolution {
            Resolution::Authored(_) => return Ok(()),
            Resolution::Absent => return Err(MountSourceError::NotFound),
            Resolution::Unauthored(lookup, source, allocation) => (lookup, source, allocation),
        };
        let lookup = MountOwnedBuffer { legacy_bytes: lookup.logical_owned_bytes(), value: lookup, allocation };
        let owned = lookup.legacy_owned_bytes();
        scratch.track(owned)?;
        scratch.carrier.with_work(|work| work.record_adoption(owned, &None)).map_err(mount_demand_error)?;
        let expected_source = self.mutation_file_id_with_carrier(
            &path, &lookup.value, scratch.carrier,
        ).await?;
        drop(lookup);
        scratch.release(owned)?;
        stage_mount_promotion_with_carrier(
            &self.lazy, &self.authored, &path, mounted, expected_source, scratch.carrier,
        ).await
    }

    async fn promote_parents_with_carrier(
        &self, path: &MountPath, owner: super::view_gate::CallbackOwner,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        for length in 1..path.components().len() {
            let mut scratch = DirectoryCarrierScratch::new(carrier);
            let parent = mount_components_with_carrier(
                &path.components()[..length], None, scratch.carrier,
            )?;
            scratch.track(parent.legacy_owned_bytes())?;
            self.promote_with_carrier(&parent, owner, scratch.carrier).await?;
        }
        Ok(())
    }

    async fn promote(&self, mounted: &MountPath) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
        D: DemandSource + 'static,
        S: LazyWorkspaceStore,
    {
        in_heap(move || async move {
            let mut work = crate::demand::DemandWork::new(
                crate::WorkBudget::UNBOUNDED, self.authored.cancellation(),
            );
            self.promote_with_carrier(
                mounted, SourceViewGate::callback_owner(),
                &mut super::adapter::NativeDemandCarrier::Borrowed(&mut work),
            ).await
        })
        .await
    }

    fn promote_parents_locked(&self, path: &MountPath) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
        D: DemandSource + 'static,
        S: LazyWorkspaceStore,
    {
        let components = path.components();
        let mut parent = MountPath::root();
        for component in components.iter().take(components.len().saturating_sub(1)) {
            parent = parent.child((*component).to_vec());
            if self.holds_authored_directory(&parent) {
                continue;
            }
            // Sampled first: a rebinding after it retires the confirmation.
            let stamp = self.ledger_stamp();
            if self.authored.lookup(&parent)?.is_none() {
                self.wait(|| async { self.promote(&parent).await })?;
            }
            if let Some(stamp) = stamp {
                let mut held = self
                    .authored_directories
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                if held.len() >= MAXIMUM_REMEMBERED_RESOLUTIONS {
                    held.clear();
                }
                held.insert(parent.clone(), stamp);
            }
        }
        Ok(())
    }

    async fn promote_subtree_with_carrier(
        &self, path: &MountPath, owner: super::view_gate::CallbackOwner,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        match carrier {
            super::adapter::NativeDemandCarrier::Native(original) =>
                self.promote_subtree_with_original_work(path, owner, original).await,
            super::adapter::NativeDemandCarrier::Shared(original) => {
                let original = crate::demand::OriginalDemandWork::from_meter(Arc::clone(original));
                let original = crate::NativeMaterializationWork::from_original_demand_work(&original)
                    .map_err(mount_demand_error)?;
                self.promote_subtree_with_original_work(path, owner, &original).await
            }
            super::adapter::NativeDemandCarrier::Borrowed(work) =>
                super::adapter::BorrowedNativeDemandMeter::new(work)?
                    .run_async(|original| async move {
                        let original = crate::demand::OriginalDemandWork::from_meter(original);
                        let original = crate::NativeMaterializationWork::from_original_demand_work(&original)
                            .map_err(mount_demand_error)?;
                        self.promote_subtree_with_original_work(path, owner, &original).await
                    }).await,
        }
    }

    async fn promote_subtree_with_original_work(
        &self, path: &MountPath, owner: super::view_gate::CallbackOwner,
        native: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        let original = native.clone();
        let cancellation = original.cancellation();
        original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        let mut carrier = super::adapter::NativeDemandCarrier::Native(native);
        let mut scratch = DirectoryCarrierScratch::new(&mut carrier);
        let root = mount_components_with_carrier(path.components(), None, scratch.carrier)?;
        let mut directories = Vec::new();
        promotion_push(&mut directories, root, &mut scratch)?;
        while let Some(directory) = directories.pop() {
            let text = mount_path_text_with_carrier(&self.root, &directory, scratch.carrier)?;
            let (removed, opaque, removal_epoch) =
                self.removal_snapshot_with_carrier(&text, scratch.carrier)?;
            let source_lease = self.source_read_lease_with_carrier(owner, None, scratch.carrier).await?;
            let stamp = ViewStamp::current();
            let mut unresolved = Vec::new();
            let mut cursor = None;
            loop {
                original.charge(crate::WorkCounters { items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED }).map_err(MountSourceError::NativeWork)?;
                let observation = self.authored.shared_checkout().observe(
                    owner, scratch.carrier, cancellation, Some(self.authored.cancellation()),
                ).await?;
                observation.ensure_publication_resolved()?;
                let state = self.lazy.state(&LazyTreapWork::native_with_work(&original, cancellation).map_err(native_lazy_error)?).await
                    .map_err(native_lazy_error)?;
                let mut page = self.lazy.list_directory_in_state_with_native_work(
                    state.as_state(), &text, cursor.take(), DIRECTORY_PROMOTION_PAGE,
                    &original, cancellation, Some(&mut observation.observer(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).expect("unbounded observer preserves a validated checkout").value),
                    Some((&removed, opaque)), None,
                ).await.map_err(native_lazy_error)?;
                for entry in &page.entries {
                    original.charge(crate::WorkCounters { items_examined: 1,
                        ..crate::WorkCounters::UNCHARGED }).map_err(MountSourceError::NativeWork)?;
                    if entry.authored { continue; }
                    let name = mount_name_with_carrier(&entry.name, scratch.carrier)?;
                    let child = mount_components_with_carrier(
                        directory.components(), Some(&name), scratch.carrier,
                    )?;
                    promotion_push(&mut unresolved,
                        (child, entry.kind == SourceNodeKind::Directory), &mut scratch)?;
                }
                cursor = page.next.take();
                if cursor.is_none() { break; }
            }
            if !self.authored.unchanged_since_with_carrier(&directory, None, stamp, scratch.carrier)? {
                return Err(MountSourceError::Stale);
            }
            let latest = self.removal_snapshot_with_carrier(&text, scratch.carrier)?;
            let unchanged = latest.2 == removal_epoch;
            drop(latest);
            if !unchanged { return Err(MountSourceError::Stale); }
            drop(source_lease);
            self.promote_with_carrier(&directory, owner, scratch.carrier).await?;
            for (child, is_directory) in unresolved.drain(..) {
                if is_directory {
                    promotion_push(&mut directories, child, &mut scratch)?;
                } else {
                    self.promote_with_carrier(&child, owner, scratch.carrier).await?;
                }
            }
            let unresolved_bytes = unresolved.capacity().checked_mul(
                std::mem::size_of::<(MountOwnedBuffer<MountPath>, bool)>(),
            ).ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
            drop(unresolved);
            scratch.release_backing(unresolved_bytes)?;
        }
        let directory_bytes = directories.capacity().checked_mul(
            std::mem::size_of::<MountOwnedBuffer<MountPath>>(),
        ).ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
        drop(directories);
        scratch.release_backing(directory_bytes)?;
        original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)
    }

    async fn lookup_pinned_in_view(
        &self,
        path: &MountPath,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<Option<(MountLookup, Option<MountContentPin>)>, MountSourceError> {
        let owner = SourceViewGate::callback_owner();
        let _lease = self.source_read_lease_with_carrier(owner, None, carrier).await?;
        let text = mount_path_text_with_carrier(&self.root, path, carrier)?;
        let result = async {
            let resolution = if carrier.original_native_work().is_none()
                && carrier.with_work(|work| work.budget == crate::WorkBudget::UNBOUNDED)
            {
                self.resolve_cached(path, &text, owner, carrier).await?
            } else { self.resolve(path, &text, owner, carrier).await? };
            match resolution {
                Resolution::Authored(lookup) =>
                    Ok(Some((self.project_authored(lookup, carrier).await?, None))),
                Resolution::Absent => Ok(None),
                Resolution::Unauthored(lookup, source, allocation) =>
                    self.project_unauthored(&text, lookup, source, allocation, carrier).await.map(Some),
            }
        }.await;
        let retained = text.legacy_owned_bytes();
        drop(text);
        carrier.with_work(|work| work.release(retained)).map_err(mount_demand_error)?;
        result
    }

    async fn resolve(
        &self,
        path: &MountPath,
        text: &str,
        owner: super::view_gate::CallbackOwner,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<Resolution, MountSourceError> {
        carrier.with_work(|work| work.check()).map_err(mount_demand_error)?;
        if let Some(lookup) = self.authored.lookup_async(path, owner, carrier).await? {
            if self.removed_identity_with_carrier(text, lookup.node.file_id, carrier)? {
                return Ok(Resolution::Absent);
            }
            return Ok(Resolution::Authored(lookup));
        }
        if carrier.with_work(|work| self.is_removed_with_work(text, work))? {
            return Ok(Resolution::Absent);
        }
        if carrier.with_work(|work| work.budget == crate::WorkBudget::UNBOUNDED)
            && path.parent().as_ref().is_some_and(|parent| self.source_lacks(parent))
        {
            return Ok(Resolution::Absent);
        }
        let caller = carrier.cancellation();
        if let Some(original) = carrier.original_native_work() {
            match self.lazy.inspect_unauthored(text, InspectBasis::default(),
                &LazyTreapWork::native_with_work(&original, &caller).map_err(lazy_error)?).await {
                Ok(LazyReadonlyNode::Native(owned)) => {
                    let ((lookup, source), allocation) = owned.into_parts();
                    Ok(Resolution::Unauthored(lookup, source, Some(allocation)))
                }
                Ok(_) => unreachable!("native inspection retains native funding"),
                Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::NotFound) =>
                    Ok(Resolution::Absent),
                Err(error) => Err(lazy_error(error)),
            }
        } else {
            let budget = carrier.with_work(|work| work.remaining()).map_err(mount_demand_error)?;
            let accounting = LazyTreapWork::new(budget, &caller);
            let result = self.lazy.inspect_unauthored(text, InspectBasis::default(), &accounting).await;
            let result = accounting.measured(result, |error, work| error.with_prior_work(work).into_measured_failure());
            match carrier.with_work(|work| mount_accept(work, result, lazy_error)) {
                Ok(LazyReadonlyNode::Portable((lookup, source))) => Ok(Resolution::Unauthored(lookup, source, None)),
                Ok(_) => unreachable!("ordinary inspection uses ordinary funding"),
                Err(MountSourceError::NotFound) => Ok(Resolution::Absent),
                Err(error) => Err(error),
            }
        }
    }

    async fn resolve_cached(
        &self,
        path: &MountPath,
        text: &str,
        owner: super::view_gate::CallbackOwner,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<Resolution, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        in_heap(move || async move {
            if let Some(deferral) = self.resolutions.get(path) {
                let file_id = deferral.node.map(|node| self.lazy.source_file_id(&node.file_identity,
                    &mut crate::kernel::allocation::KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()))).transpose().map_err(lazy_error)?;
                if deferral.source == self.lazy.source_reference()
                    && self.unchanged_since(path, file_id, deferral.stamp)
                {
                    if self.source_observed() {
                        return Ok(deferral.resolution());
                    }
                    match self.lazy.source_lookup(deferral.source, text).await {
                        Ok(node) if node == deferral.node => return Ok(deferral.resolution()),
                        // The source changed, or its view was invalidated.
                        Ok(_) => {}
                        Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::StaleSource) => {}
                        Err(error) => return Err(lazy_error(error)),
                    }
                }
            }
            let stamp = self.ledger_stamp();
            let source = self.lazy.source_reference();
            let resolution = self.resolve(path, text, owner, carrier).await?;
            if let Some(stamp) = stamp {
                match &resolution {
                    Resolution::Unauthored(LazyLookup::Source(node), Some(source), _) => {
                        self.resolutions.remember(path, Deferral {
                            stamp, source: *source, node: Some(*node),
                        });
                    }
                    Resolution::Absent if !self.is_removed(text)? => {
                        self.resolutions.remember(path, Deferral { stamp, source, node: None });
                        if let Some(parent) = path.parent() {
                            self.observe_source_absence(&parent, stamp, source).await?;
                        }
                    }
                    _ => {}
                }
            }
            Ok(resolution)
        })
        .await
    }

    fn holds_authored_directory(&self, directory: &MountPath) -> bool {
        self.authored_directories
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(directory)
            .copied()
            .is_some_and(|stamp| {
                self.source_view.is_stable()
                    && self.authored.binding_unchanged_since(directory, stamp)
            })
    }

    fn source_lacks(&self, directory: &MountPath) -> bool {
        self.source_observed()
            && self
                .source_absent
                .get(directory)
                .is_some_and(|(stamp, source)| {
                    source == self.lazy.source_reference()
                        && self.source_view.is_stable()
                        && self.authored.binding_unchanged_since(directory, stamp)
                })
    }

    async fn observe_source_absence(
        &self,
        directory: &MountPath,
        stamp: ViewStamp,
        source: SourceReference,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        if directory.components().is_empty()
            || !self.source_observed()
            || self.source_absent.get(directory).is_some()
        {
            return Ok(());
        }
        let text = self.path(directory)?;
        match self.lazy.source_lookup(source, &text).await {
            Ok(None) => self.source_absent.remember(directory, stamp, source),
            Ok(Some(_)) => {}
            Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::StaleSource) => {}
            Err(error) => return Err(lazy_error(error)),
        }
        Ok(())
    }

    async fn project_authored(
        &self,
        lookup: MountLookup,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<MountLookup, MountSourceError> {
        Ok(self.detached_identity(lookup, carrier).await?.unwrap_or(lookup))
    }

    async fn project_unauthored(
        &self,
        path: &str,
        lookup: LazyLookup,
        source: Option<SourceReference>,
        allocation: Option<crate::NativeAllocation>,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<(MountLookup, Option<MountContentPin>), MountSourceError> {
        let lookup = MountOwnedBuffer { legacy_bytes: lookup.logical_owned_bytes(), value: lookup, allocation };
        let retained = lookup.legacy_owned_bytes();
        if let Err(error) = carrier.with_work(|work| work.record_adoption(retained, &None)) {
            drop(lookup);
            carrier.with_work(|work| work.release(retained)).map_err(mount_demand_error)?;
            return Err(mount_demand_error(error));
        }
        let result = async {
        let file_id = self.mutation_file_id_with_carrier(path, &lookup.value, carrier).await?;
        if let LazyLookup::Source(node) = &lookup.value
            && linked(node)
        {
            carrier.with_work(|work| self.remember_linked_with_work(file_id, work))?;
        }
        let pin_source = match (&lookup.value, source) {
            (LazyLookup::Source(node), Some(source)) if node.kind == SourceNodeKind::RegularFile =>
                Some((source, *node)),
            _ => None,
        };
        let projected = mount_lookup_ref(&lookup.value, file_id);
        if let Some(current) = self.detached_identity(projected, carrier).await? {
            return Ok((current, None));
        }
        let pin = carrier.with_work(|work| pin_source
            .map(|(source, node)| source_content_pin_with_work(source, &node, work)).transpose())?;
        Ok((projected, pin))
        }.await;
        drop(lookup);
        carrier.with_work(|work| work.release(retained)).map_err(mount_demand_error)?;
        result
    }

    fn directory_page(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<MountDirectoryPage, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        with_native_callback_carrier(&self.runtime, carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || {
                self.directory_page_async(path, cursor, maximum_entries, carrier)
            })
        })
    }

    async fn directory_page_async(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<MountDirectoryPage, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        let mut scratch = DirectoryCarrierScratch::new(carrier);
        let text = mount_path_text_with_carrier(&self.root, path, scratch.carrier)?;
        scratch.track(text.legacy_owned_bytes())?;
        let owner = SourceViewGate::callback_owner();
        let lease = self.source_read_lease_with_carrier(owner, None, scratch.carrier).await?;
        let (removed_children, opaque, removal_epoch) =
            scratch.carrier.with_work(|work| self.removal_snapshot_with_work(&text, work))?;
        scratch.track(directory_removal_bytes(&removed_children)?)?;
        if opaque && self.authored.lookup_async(path, owner, scratch.carrier).await?.is_none() {
            return Err(MountSourceError::NotFound);
        }
        let generation = (lease.generation, removal_epoch);
        let held = self.cursors.take(generation, cursor, scratch.carrier)?;
        let (stamp, mut cursor) = match held {
            Some((stamp, cursor)) => (stamp, Some(cursor)),
            None => (ViewStamp::current(), None),
        };
        if let Some(cursor) = &mut cursor { cursor.mark_input_borrowed(); }
        if !self.authored.unchanged_since_with_carrier(path, None, stamp, scratch.carrier)? {
            return Err(MountSourceError::Stale);
        }
        let caller = scratch.carrier.cancellation();
        let original = scratch.carrier.original_native_work();
        let (mut page, state) = {
            let observation = self.authored.shared_checkout().observe(
                owner, scratch.carrier, &caller, Some(self.authored.cancellation()),
            ).await?;
            observation.ensure_publication_resolved()?;
            if let Some(original) = &original {
                let state = self.lazy.state(&LazyTreapWork::native_with_work(original, &caller).map_err(directory_lazy_error)?).await
                    .map_err(directory_lazy_error)?;
                let mut checkout = observation.observer(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
                    .expect("unbounded observer preserves a validated checkout").value;
                let page = self.lazy.list_directory_in_state_with_native_work(
                    state.as_state(), &text, cursor, maximum_entries, original, &caller,
                    Some(&mut checkout), Some((&removed_children, opaque)), None,
                ).await.map_err(directory_lazy_error)?;
                let LazyReadonlyNode::Native(state) = state else { unreachable!("native state retains native funding") };
                let (state, allocation) = state.into_parts();
                (MountOwnedBuffer { value: page.page, legacy_bytes: 0, allocation: Some(page.allocation) },
                    MountOwnedBuffer { value: state, legacy_bytes: 0, allocation: Some(allocation) })
            } else {
                let mut checkout = observation.observer(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
                    .expect("unbounded observer preserves a validated checkout").value;
                let budget = scratch.carrier.with_work(|work| work.remaining()).map_err(mount_demand_error)?;
                let result = self.lazy.list_directory_in_checkout(
                    &mut checkout, &text, cursor, maximum_entries, Some((&removed_children, opaque)), budget, &caller,
                ).await;
                let (page, state) = scratch.carrier.with_work(|work| mount_accept(work, result, directory_lazy_error))?;
                let page_bytes = crate::lazy_workspace::retained_directory_page_bytes(
                    &page.entries, page.entries.iter().map(|entry| entry.name.retained_bytes()),
                ).map_err(directory_lazy_error)?.checked_add(page.next.as_ref()
                    .map(LazyDirectoryCursor::retained_bytes).transpose().map_err(directory_lazy_error)?.unwrap_or(0))
                    .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
                let state_bytes = std::mem::size_of_val(&state) as u64;
                let bytes = page_bytes.checked_add(state_bytes)
                    .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
                scratch.track(bytes)?;
                scratch.carrier.with_work(|work| work.record_adoption(bytes, &None))
                    .map_err(mount_demand_error)?;
                (MountOwnedBuffer { value: page, legacy_bytes: page_bytes, allocation: None },
                    MountOwnedBuffer { value: state, legacy_bytes: state_bytes, allocation: None })
            }
        };
        let mut entries = scratch.vec::<MountDirectoryEntry>(page.value.entries.len())?;
        let output_storage = if scratch.native_backing.is_some() { 0 } else {
            (entries.capacity() * std::mem::size_of::<MountDirectoryEntry>()) as u64
        };
        let mut output_names = 0_u64;
        for entry in &page.value.entries {
            scratch.carrier.with_work(|work| mount_items(work, 1))?;
            let name = mount_name_with_carrier(&entry.name, scratch.carrier)?;
            let (name, mut allocation, name_bytes) = name.into_parts();
            if let Some(allocation) = &mut allocation { scratch.absorb_native(allocation)?; }
            scratch.track(name_bytes)?;
            let (mounted_child, mounted_bytes) = scratch.carrier.with_work(|work| mount_components_with_work(path.components(), Some(&name), work))?;
            scratch.track(mounted_bytes)?;
            let child = mount_path_text_with_carrier(&self.root, &mounted_child, scratch.carrier)?;
            scratch.track(child.legacy_owned_bytes())?;
            let (lookup, pin) = if let Some(node) = entry.source {
                let (lookup, source, allocation) = if let Some(original) = &original {
                    let phase = self.lazy.inspect_unauthored(&child,
                        InspectBasis { state: Some(&state.value), listed: Some(node), unobserved: entry.unobserved, ..InspectBasis::default() },
                        &LazyTreapWork::native_with_work(original, &caller).map_err(directory_lazy_error)?,
                    ).await.map_err(directory_lazy_error)?;
                    let LazyReadonlyNode::Native(owned) = phase else { unreachable!("native inspection retains native funding") };
                    let ((lookup, source), allocation) = owned.into_parts();
                    (lookup, source, Some(allocation))
                } else {
                    let budget = scratch.carrier.with_work(|work| work.remaining()).map_err(mount_demand_error)?;
                    let accounting = LazyTreapWork::new(budget, &caller);
                    let result = self.lazy.inspect_unauthored(&child,
                        InspectBasis { state: Some(&state.value), listed: Some(node), unobserved: entry.unobserved, ..InspectBasis::default() }, &accounting).await;
                    let result = accounting.measured(result, |error, work| error.with_prior_work(work).into_measured_failure());
                    let phase = scratch.carrier.with_work(|work| mount_accept(work, result, directory_lazy_error))?;
                    let LazyReadonlyNode::Portable((lookup, source)) = phase else { unreachable!("ordinary inspection uses ordinary funding") };
                    if self.source_view.is_stable() && let (LazyLookup::Source(node), Some(source)) = (&lookup, source) {
                        self.resolutions.remember(&mounted_child, Deferral { stamp, source, node: Some(*node) });
                    }
                    (lookup, source, None)
                };
                self.project_unauthored(&child, lookup, source, allocation, scratch.carrier).await?
            } else if let Some(authored) = self.authored.lookup_async(
                &mounted_child, owner, scratch.carrier,
            ).await? {
                let removed = self.removed_identity_with_carrier(&child, authored.node.file_id, scratch.carrier)?;
                if removed {
                    let name_capacity = name.capacity() as u64;
                    let child_bytes = child.legacy_owned_bytes();
                    drop(name); drop(child); drop(mounted_child);
                    if scratch.native_backing.is_some() { scratch.release_backing(name_capacity)?; }
                    scratch.release(name_bytes + child_bytes + mounted_bytes)?;
                    continue;
                }
                (self.project_authored(authored, scratch.carrier).await?, None)
            } else {
                let (lookup, source, allocation) = if let Some(original) = &original {
                    let phase = self.lazy.inspect_unauthored(&child,
                        InspectBasis { state: Some(&state.value), ..InspectBasis::default() },
                        &LazyTreapWork::native_with_work(original, &caller).map_err(directory_lazy_error)?,
                    ).await.map_err(directory_lazy_error)?;
                    let LazyReadonlyNode::Native(owned) = phase else { unreachable!("native inspection retains native funding") };
                    let ((lookup, source), allocation) = owned.into_parts();
                    (lookup, source, Some(allocation))
                } else {
                    let budget = scratch.carrier.with_work(|work| work.remaining()).map_err(mount_demand_error)?;
                    let accounting = LazyTreapWork::new(budget, &caller);
                    let result = self.lazy.inspect_unauthored(&child, InspectBasis { state: Some(&state.value), ..InspectBasis::default() }, &accounting).await;
                    let result = accounting.measured(result, |error, work| error.with_prior_work(work).into_measured_failure());
                    let phase = scratch.carrier.with_work(|work| mount_accept(work, result, directory_lazy_error))?;
                    let LazyReadonlyNode::Portable((lookup, source)) = phase else { unreachable!("ordinary inspection uses ordinary funding") };
                    (lookup, source, None)
                };
                self.project_unauthored(&child, lookup, source, allocation, scratch.carrier).await?
            };
            let child_bytes = child.legacy_owned_bytes();
            drop(child); drop(mounted_child);
            scratch.release(child_bytes + mounted_bytes)?;
            output_names = output_names.checked_add(name_bytes)
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            entries.push(MountDirectoryEntry { name, node: lookup.node, metadata: lookup.metadata, pin });
        }
        let unchanged = self.authored.unchanged_since_with_carrier(path, None, stamp, scratch.carrier)?;
        let latest = scratch.carrier.with_work(|work| self.removal_snapshot_with_work(&text, work))?;
        let latest_bytes = directory_removal_bytes(&latest.0)?;
        scratch.track(latest_bytes)?;
        let same_epoch = latest.2 == generation.1;
        drop(latest);
        scratch.release(latest_bytes)?;
        if !unchanged || !same_epoch { return Err(MountSourceError::Stale); }
        let mut cursor_bytes = 0;
        let next_cursor = if let Some(cursor) = page.value.next.take() {
            let token = self.cursors.remember(generation, (stamp, cursor), scratch.carrier)?;
            let (token, mut allocation, bytes) = token.into_parts();
            if let Some(allocation) = &mut allocation { scratch.absorb_native(allocation)?; }
            cursor_bytes = bytes;
            scratch.track(bytes)?;
            Some(token)
        } else { None };
        let lower_bytes = page.legacy_owned_bytes() + state.legacy_owned_bytes();
        drop(page); drop(state);
        scratch.release(lower_bytes)?;
        scratch.keep(output_storage + output_names + cursor_bytes)?;
        Ok(MountDirectoryPage::new(entries, next_cursor, scratch.native_backing.take()))
    }

    fn list_directory(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<MountDirectoryPage, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        let attempts = if cursor.is_some() { 1 } else { MAXIMUM_INTERRUPTED_LISTINGS };
        let mut page = Err(MountSourceError::Stale);
        for _ in 0..attempts {
            carrier.with_work(|work| mount_items(work, 1))?;
            page = self.directory_page(path, cursor, maximum_entries, carrier);
            if !matches!(page, Err(MountSourceError::Stale)) { break; }
        }
        let mut page = page?;
        while page.entries().is_empty() && page.next_cursor().is_some() {
            let (entries, next, mut ownership) = page.into_parts();
            let next = next.expect("the empty page has a continuation");
            let token_bytes = next.capacity() as u64;
            let native = ownership.has_native_custody();
            let token_ownership = match ownership.split_owned_payload(token_bytes) {
                Ok(token) => token,
                Err(error) => {
                    drop(entries);
                    drop(next);
                    drop(ownership);
                    return Err(MountSourceError::NativeWork(error));
                }
            };
            let storage = (entries.capacity() * std::mem::size_of::<MountDirectoryEntry>()) as u64;
            drop(entries);
            drop(ownership);
            if !native { carrier.with_work(|work| work.release(storage)).map_err(mount_demand_error)?; }
            let result = (|| {
                carrier.with_work(|work| mount_items(work, 1))?;
                self.directory_page(path, Some(&next), maximum_entries, carrier)
            })();
            drop(next);
            drop(token_ownership);
            if !native { carrier.with_work(|work| work.release(token_bytes)).map_err(mount_demand_error)?; }
            page = result?;
        }
        Ok(page)
    }

    fn capture_host_with_native_work(
        &self, source_root: &Path, paths: &[MountPath], expected_root_identity: NativeRootIdentity,
        scope: super::adapter::HostCaptureScope,
        original: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        original.admit(crate::WorkCounters::UNCHARGED)
            .map_err(MountSourceError::NativeWork)?;
        self.authored.cancellation().check().map_err(|_| MountSourceError::Stale)?;
        if paths.is_empty() { return Ok(()); }
        let mut carrier = super::adapter::NativeDemandCarrier::Native(original);
        with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let _mutation = self.source_mutation_lease_with_carrier(None, carrier).await?;
                let owner = SourceViewGate::callback_owner();
                for path in paths {
                    carrier.with_work(|work| mount_items(work, 1))?;
                    self.promote_parents_with_carrier(path, owner, carrier).await?;
                    match self.promote_with_carrier(path, owner, carrier).await {
                        Ok(()) | Err(MountSourceError::NotFound) => {}
                        Err(error) => return Err(error),
                    }
                }
                self.authored.capture_host(
                    source_root, paths, expected_root_identity, scope, None, original,
                ).await
            })
        })
    }

    async fn source_read_lease_with_carrier(
        &self, owner: super::view_gate::CallbackOwner, expected: Option<u64>,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<SourceViewLease, MountSourceError> {
        let cancellation = carrier.cancellation();
        self.source_view.read_for_callback_with_carrier(
            owner, expected, carrier, &cancellation, Some(self.authored.cancellation()),
        ).await
    }

    async fn source_mutation_lease_with_carrier(
        &self, expected: Option<u64>, carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<super::view_gate::ViewMutationPhase, MountSourceError> {
        let cancellation = carrier.cancellation();
        self.source_view.write_stable_with_carrier(
            expected, carrier, &cancellation, Some(self.authored.cancellation()),
        ).await.map(super::view_gate::ViewWriteLease::into_mutation_phase)
    }

    fn flush_with_carrier(
        &self, carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        with_native_callback_carrier(&self.runtime, carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let cancellation = carrier.cancellation();
                carrier.with_work(|work| work.check()).map_err(mount_demand_error)?;
                if !self.authored.shared_checkout().publishes_at_native_boundary_with_carrier(
                    carrier, &cancellation, Some(self.authored.cancellation()),
                ).await? { return Ok(()); }
                self.sync_async_with_permit_carrier_guarded(
                    crate::PublicationPermit::Unrestricted, carrier.reborrow(), &cancellation,
                    &mut self.authored.sdk_publication_policy(),
                ).await
            })
        })
    }

    fn fence_changes_with_carrier(
        &self, carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        carrier.with_work(|work| work.check()).map_err(mount_demand_error)?;
        if self.authored.cancellation().is_cancelled() { return Err(MountSourceError::Stale); }
        let Some(watch) = &self.source_watch else { return Ok(()); };
        let cancellation = carrier.cancellation();
        let budget = carrier.with_work(|work| work.remaining()).map_err(mount_demand_error)?;
        let result = crate::demand::fence_watch(&**watch, budget, &cancellation);
        carrier.with_work(|work| mount_accept(work, result, mount_demand_error))?;
        if self.authored.cancellation().is_cancelled() { return Err(MountSourceError::Stale); }
        Ok(())
    }

    fn acquire_binding_lease_with_carrier(
        &self, expected: Option<u64>, carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<Box<dyn Send>, MountSourceError> {
        with_native_callback_carrier(&self.runtime, carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let owner = SourceViewGate::callback_owner();
                let mut lease = self.source_read_lease_with_carrier(owner, expected, carrier).await?;
                let bytes = std::mem::size_of::<SourceViewLease>() as u64;
                let copied = crate::WorkCounters { bytes_copied: bytes, ..crate::WorkCounters::default() };
                carrier.with_work(|work| {
                    work.work.admit(&copied, &work.budget).map_err(MountSourceError::Work)?;
                    work.allocate(bytes).map_err(mount_demand_error)?;
                    if let Err(error) = lease.attach_owned_payload(bytes) {
                        work.release(bytes).map_err(mount_demand_error)?;
                        return Err(error);
                    }
                    Ok(())
                })?;
                let lease = Box::new(lease) as Box<dyn Send>;
                carrier.with_work(|work| work.record(copied)).map_err(mount_demand_error)?;
                Ok(lease)
            })
        })
    }

    fn native_attributes<T>(
        &self, path: &MountPath, work: &crate::NativeMaterializationWork,
        operation: impl FnOnce(Option<Arc<CheckoutDetachedFile<A, O>>>, bool) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
        let mut carrier = super::adapter::NativeDemandCarrier::Native(work);
        let owner = SourceViewGate::callback_owner();
        let (lease, detached, projected) = with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let lease = self.source_read_lease_with_carrier(owner, None, carrier).await?;
                let text = mount_path_text_with_carrier(&self.root, path, carrier)?;
                let resolution = self.resolve(path, &text, owner, carrier).await?;
                let detached = self.content_detached_with_carrier(&resolution, &text, carrier).await?;
                let projected = match resolution {
                    Resolution::Absent => return Err(MountSourceError::NotFound),
                    Resolution::Authored(_) => false,
                    Resolution::Unauthored(LazyLookup::Source(_), _, _) => true,
                    Resolution::Unauthored(_, _, _) if detached.is_some() => false,
                    Resolution::Unauthored(_, _, _) => return Err(MountSourceError::Unsupported(std::borrow::Cow::Owned("named attributes of an unpromoted shadow are not projected".to_owned()))),
                };
                Ok((lease, detached, projected))
            })
        })?;
        let result = operation(detached, projected);
        drop(lease);
        result
    }

    fn native_call<T>(&self, operation: impl FnOnce(&crate::NativeMaterializationWork) -> Result<T, MountSourceError>)
        -> Result<T, MountSourceError> {
        if let Some(work) = self.authored.native_original_work()? { return operation(&work); }
        let work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, self.authored.cancellation())
            .map_err(|(error, _)| MountSourceError::NativeWork(error))?;
        operation(&work)
    }

    fn native_mutation<T>(
        &self, path: &MountPath, source: Option<&MountPath>, promote: bool, rebound: bool,
        work: &crate::NativeMaterializationWork,
        operation: impl FnOnce(Option<Arc<CheckoutDetachedFile<A, O>>>) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
        let mut carrier = super::adapter::NativeDemandCarrier::Native(work);
        let owner = SourceViewGate::callback_owner();
        let (lease, text, detached) = with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let lease = self.source_mutation_lease_with_carrier(None, carrier).await?;
                let text = mount_path_text_with_carrier(&self.root, path, carrier)?;
                let detached = if promote {
                    let resolution = self.resolve(path, &text, owner, carrier).await?;
                    self.content_detached_with_carrier(&resolution, &text, carrier).await?
                } else { None };
                if let Some(source) = source { self.promote_with_carrier(source, owner, carrier).await?; }
                if promote {
                    if detached.is_none() { self.promote_with_carrier(path, owner, carrier).await?; }
                } else { self.promote_parents_with_carrier(path, owner, carrier).await?; }
                Ok((lease, text, detached))
            })
        })?;
        let original = work.clone();
        let delta = if rebound {
            carrier.with_work(|work| {
                mount_items(work, 1)?;
                let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
                let removed = removals.paths.contains_key(text.value.as_str());
                Ok::<_, MountSourceError>(crate::WorkCounters {
                    items_examined: 1 + u64::from(removed), ..crate::WorkCounters::UNCHARGED
                })
            })?
        } else { crate::WorkCounters::UNCHARGED };
        let mut completion = original.reserve_work(delta).map_err(MountSourceError::NativeWork)?;
        if rebound { self.cursors.retire(&mut carrier)?; }
        let result = operation(detached);
        let result = match result {
            Ok(value) if rebound => {
                let restored = self.record_rebound(&text);
                let recorded = completion.record_completed(delta).map_err(MountSourceError::NativeWork);
                restored.and(recorded).map(|()| value)
            }
            result => result,
        };
        let bytes = text.legacy_owned_bytes();
        drop(text);
        drop(lease);
        let released = carrier.with_work(|work| work.release(bytes)).map_err(mount_demand_error);
        match result { Ok(value) => released.map(|()| value), Err(error) => Err(error) }
    }

    fn wait<T: Send, F>(&self, create: impl FnOnce() -> F + Send) -> Result<T, MountSourceError>
    where
        F: std::future::Future<Output = Result<T, MountSourceError>>,
    {
        self.runtime.wait(create)
    }


}

pub(super) fn with_native_callback_carrier<T>(
    runtime: &CallbackRuntime, carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    run: impl FnOnce(&mut super::adapter::NativeDemandCarrier<'_>, Arc<super::adapter::CallbackWait>)
        -> Result<T, MountSourceError>,
) -> Result<T, MountSourceError> {
    let prepared = carrier.with_work(|work| runtime.prepare_wait(work))?;
    let bytes = prepared.logical_owned_bytes();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(carrier, prepared)));
    let release = carrier.with_work(|work| work.release(bytes)).map_err(mount_demand_error);
    let reconciled = carrier.with_work(|work| work.record(crate::WorkCounters::default()))
        .map_err(mount_demand_error);
    match result {
        Err(panic) => std::panic::resume_unwind(panic),
        Ok(Err(error)) => Err(error),
        Ok(Ok(value)) => release.and(reconciled).map(|()| value),
    }
}

struct LazyOpenFile<A, O, D, S> {
    lazy: Arc<LazyWorkspace<A, O, D, S>>,
    authored: Arc<CheckoutMountSource<A, O>>,
    runtime: Arc<CallbackRuntime>,
    path: String,
    mount_path: MountPath,
    expected_source: FileId,
    source_file: Box<dyn DemandFile>,
    source_node: SourceNode,
    source_generation: u64,
    source_view: Arc<SourceViewGate>,
    detached: DetachedIdentities<A, O>,
    open_sources: OpenIdentityHandles,
    promoted: Mutex<Option<Arc<dyn MountOpenFile>>>,
    unstaged: UnstagedIdentities,
    _input_allocation: Option<crate::NativeAllocation>,
}

struct ViewBoundOpenFile<A, O> {
    inner: Arc<dyn MountOpenFile>,
    runtime: Arc<CallbackRuntime>,
    source_view: Arc<SourceViewGate>,
    source_lifetime: crate::CancellationToken,
    detached: DetachedIdentities<A, O>,
    open_sources: OpenIdentityHandles,
    file_id: Option<FileId>,
    generation: u64,
}

impl<A, O, D, S> Drop for LazyOpenFile<A, O, D, S> {
    fn drop(&mut self) {
        untrack_open_handle(
            &self.open_sources,
            self.expected_source,
            self as *const Self as *const (),
        );
    }
}

impl<A, O> Drop for ViewBoundOpenFile<A, O> {
    fn drop(&mut self) {
        if let Some(file_id) = self.file_id {
            untrack_open_handle(
                &self.open_sources,
                file_id,
                self as *const Self as *const (),
            );
        }
    }
}

fn untrack_open_handle(handles: &OpenIdentityHandles, file_id: FileId, ptr: *const ()) {
    let Ok(mut open) = handles.lock() else {
        return;
    };
    if let Some(entries) = open.get_mut(&file_id) {
        entries.retain(|entry| entry.as_ptr() as *const () != ptr);
        if entries.is_empty() {
            open.remove(&file_id);
        }
    }
}

impl<A, O> ViewBoundOpenFile<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn current_file(
        &self,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        carrier.with_work(|work| {
            work.check().map_err(mount_demand_error)?;
            let Some(file_id) = self.file_id else { return Ok(Arc::clone(&self.inner)); };
            let detached = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
            let mut low = 0;
            let mut high = detached.len();
            while low < high {
                mount_items(work, 1)?;
                let middle = low + (high - low) / 2;
                match detached[middle].0.cmp(&file_id) {
                    std::cmp::Ordering::Less => low = middle + 1,
                    std::cmp::Ordering::Greater => high = middle,
                    std::cmp::Ordering::Equal => {
                        return Ok(if detached[middle].1.ready {
                            Arc::clone(&detached[middle].1.file) as Arc<dyn MountOpenFile>
                        } else { Arc::clone(&self.inner) });
                    }
                }
            }
            Ok(Arc::clone(&self.inner))
        })
    }

    fn with_view<T>(
        &self, mut carrier: super::adapter::NativeDemandCarrier<'_>,
        operation: impl FnOnce(&dyn MountOpenFile) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
        if matches!(carrier, super::adapter::NativeDemandCarrier::Native(_)) {
            return with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
                self.runtime.wait_prepared(prepared, || async {
                    let owner = SourceViewGate::callback_owner();
                    let caller = carrier.cancellation();
                    let _lease = self.source_view.read_for_callback_with_carrier(
                        owner, Some(self.generation), carrier, &caller, Some(&self.source_lifetime),
                    ).await?;
                    operation(self.current_file(carrier)?.as_ref())
                })
            });
        }
        let owner = SourceViewGate::callback_owner();
        let _lease = self.runtime.wait(|| {
            let source_view = Arc::clone(&self.source_view);
            let generation = self.generation;
            async move { source_view.read_for_callback(owner, Some(generation)).await }
        })?;
        operation(self.current_file(&mut carrier)?.as_ref())
    }

    fn with_mutation<T>(
        &self, mut carrier: super::adapter::NativeDemandCarrier<'_>,
        operation: impl FnOnce(&dyn MountOpenFile) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
        if matches!(carrier, super::adapter::NativeDemandCarrier::Native(_)) {
            return with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
                self.runtime.wait_prepared(prepared, || async {
                    let caller = carrier.cancellation();
                    let _lease = self.source_view.write_stable_with_carrier(
                        Some(self.generation), carrier, &caller, Some(&self.source_lifetime),
                    ).await?;
                    operation(self.current_file(carrier)?.as_ref())
                })
            });
        }
        let _lease = self.runtime.wait(|| {
            let source_view = Arc::clone(&self.source_view);
            let generation = self.generation;
            async move { source_view.write_stable(Some(generation)).await }
        })?;
        operation(self.current_file(&mut carrier)?.as_ref())
    }
}

impl<A, O> MountOpenFile for ViewBoundOpenFile<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn lookup_with_meter(&self, work: &crate::NativeMaterializationWork) -> Result<MountLookup, MountSourceError> {
        self.with_view(super::adapter::NativeDemandCarrier::Native(work), |file| file.lookup_with_meter(work))
    }

    fn read_range_with_meter(&self, offset: u64, length: u32, work: &crate::NativeMaterializationWork)
        -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        self.with_view(super::adapter::NativeDemandCarrier::Native(work), |file| file.read_range_with_meter(offset, length, work))
    }

    fn read_up_to_with_meter(&self, offset: u64, maximum_bytes: u32, work: &crate::NativeMaterializationWork)
        -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        self.with_view(super::adapter::NativeDemandCarrier::Native(work), |file| file.read_up_to_with_meter(offset, maximum_bytes, work))
    }

    fn seek_with_meter(&self, offset: u64, target: MountSeekTarget, work: &crate::NativeMaterializationWork)
        -> Result<Option<u64>, MountSourceError> {
        self.with_view(super::adapter::NativeDemandCarrier::Native(work), |file| file.seek_with_meter(offset, target, work))
    }

    fn write_range_with_meter(&self, offset: u64, bytes: crate::kernel::OwnedSourceChunk,
        work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_mutation(super::adapter::NativeDemandCarrier::Native(work), |file| file.write_range_with_meter(offset, bytes, work))
    }

    fn resize_with_meter(&self, logical_bytes: u64, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_mutation(super::adapter::NativeDemandCarrier::Native(work), |file| file.resize_with_meter(logical_bytes, work))
    }

    fn allocate_range_with_meter(&self, offset: u64, length: u64, operation: MountRangeAllocation,
        work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_mutation(super::adapter::NativeDemandCarrier::Native(work), |file| file.allocate_range_with_meter(offset, length, operation, work))
    }

    fn set_attributes(&self, metadata: FileMetadata, logical_bytes: Option<u64>,
        work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_mutation(super::adapter::NativeDemandCarrier::Native(work), |file| file.set_attributes(metadata, logical_bytes, work))
    }

    fn read_attribute(&self, name: &[u8], work: &crate::NativeMaterializationWork)
        -> Result<Option<crate::kernel::OwnedSourceChunk>, MountSourceError> {
        self.with_view(super::adapter::NativeDemandCarrier::Native(work), |file| file.read_attribute(name, work))
    }

    fn list_attributes_with_meter(&self, cursor: Option<&[u8]>, maximum_entries: u32,
        work: &crate::NativeMaterializationWork) -> Result<(MountAttributePage, crate::NativeAllocation), MountSourceError> {
        self.with_view(super::adapter::NativeDemandCarrier::Native(work), |file| file.list_attributes_with_meter(cursor, maximum_entries, work))
    }

    fn write_attribute(&self, name: &[u8], value: crate::kernel::OwnedSourceChunk,
        mode: MountAttributeWriteMode, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_mutation(super::adapter::NativeDemandCarrier::Native(work), |file| file.write_attribute(name, value, mode, work))
    }

    fn remove_attribute(&self, name: &[u8], work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_mutation(super::adapter::NativeDemandCarrier::Native(work), |file| file.remove_attribute(name, work))
    }

    fn settle(&self, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_mutation(super::adapter::NativeDemandCarrier::Native(work), |file| file.settle(work))
    }

    fn read_sparse_range(&self, offset: u64, length: u32, work: &crate::NativeMaterializationWork)
        -> Result<(super::MountSparseRange, crate::NativeAllocation), MountSourceError> {
        self.with_view(super::adapter::NativeDemandCarrier::Native(work), |file| file.read_sparse_range(offset, length, work))
    }

    fn write_sparse_range(&self, offset: u64, range: (super::MountSparseRange, crate::NativeAllocation),
        work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_mutation(super::adapter::NativeDemandCarrier::Native(work), |file| file.write_sparse_range(offset, range, work))
    }

    fn lookup(&self) -> Result<MountLookup, MountSourceError> {
        self.with_view(super::adapter::NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())), MountOpenFile::lookup)
    }

    fn read_range(&self, offset: u64, length: u32) -> Result<OwnedBytes, MountSourceError> {
        self.with_view(super::adapter::NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())), |file| file.read_range(offset, length))
    }

    fn read_up_to(&self, offset: u64, maximum_bytes: u32) -> Result<OwnedBytes, MountSourceError> {
        self.with_view(super::adapter::NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())), |file| file.read_up_to(offset, maximum_bytes))
    }

    fn seek(&self, offset: u64, target: MountSeekTarget) -> Result<Option<u64>, MountSourceError> {
        self.with_view(super::adapter::NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())), |file| file.seek(offset, target))
    }

    fn write_range(&self, offset: u64, bytes: Bytes) -> Result<(), MountSourceError> {
        self.with_mutation(super::adapter::NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())), |file| file.write_range(offset, bytes))
    }

    fn resize(&self, logical_bytes: u64) -> Result<(), MountSourceError> {
        self.with_mutation(super::adapter::NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())), |file| file.resize(logical_bytes))
    }

    fn allocate_range(
        &self,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        self.with_mutation(super::adapter::NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())), |file| file.allocate_range(offset, length, operation))
    }

    fn list_attributes(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        self.with_view(super::adapter::NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())), |file| file.list_attributes(cursor, maximum_entries))
    }

}

impl<A, O, D, S> LazyOpenFile<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    fn current_authored(&self) -> Result<Option<Arc<dyn MountOpenFile>>, MountSourceError> {
        let detached_file = {
            let detached = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
            detached_entry(&detached, self.expected_source)
                .filter(|entry| entry.ready)
                .map(|entry| Arc::clone(&entry.file))
        };
        if let Some(file) = detached_file {
            return Ok(Some(file));
        }
        let mut promoted = self.promoted.lock().map_err(|_| MountSourceError::Stale)?;
        if let Some(file) = promoted.as_ref() {
            return Ok(Some(Arc::clone(file)));
        }
        let revision = self.authored.shared_checkout().revision();
        if self.unstaged.checked_at(self.expected_source, revision) {
            return Ok(None);
        }
        let file = super::adapter::boundary(self.authored.cancellation(), |work| self.runtime.block_on_measured(|| async { let mut carrier = super::adapter::NativeDemandCarrier::Native(work); self.authored.attached_file_by_id(self.expected_source, SourceViewGate::callback_owner(), &mut carrier).await }, work.native_read_meter().as_ref()).map_err(super::adapter::capture_io_error)?.map(|handle| handle.map(|(handle, _)| handle)))?;
        if let Some(file) = file {
            *promoted = Some(Arc::clone(&file));
            return Ok(Some(file));
        }
        self.unstaged.remember(self.expected_source, revision);
        Ok(None)
    }

    async fn current_authored_with_carrier(
        &self, owner: super::view_gate::CallbackOwner,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<Option<(Arc<dyn MountOpenFile>, u64)>, MountSourceError> {
        carrier.with_work(|work| work.check()).map_err(mount_demand_error)?;
        {
            let detached = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
            let mut low = 0;
            let mut high = detached.len();
            while low < high {
                carrier.with_work(|work| mount_items(work, 1))?;
                let middle = low + (high - low) / 2;
                match detached[middle].0.cmp(&self.expected_source) {
                    std::cmp::Ordering::Less => low = middle + 1,
                    std::cmp::Ordering::Greater => high = middle,
                    std::cmp::Ordering::Equal => {
                        if detached[middle].1.ready {
                            return Ok(Some((detached[middle].1.file.clone(), 0)));
                        }
                        break;
                    }
                }
            }
        }
        {
            let promoted = self.promoted.lock().map_err(|_| MountSourceError::Stale)?;
            carrier.with_work(|work| mount_items(work, 1))?;
            if let Some(file) = promoted.as_ref() {
                return Ok(Some((Arc::clone(file), 0)));
            }
        }
        self.authored.attached_file_by_id(self.expected_source, owner, carrier).await
    }

    fn with_view_with_meter<T>(
        &self, work: &crate::NativeMaterializationWork, refresh: bool,
        operation: impl FnOnce(Option<&dyn MountOpenFile>, SourceNode,
            &mut super::adapter::NativeDemandCarrier<'_>) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
        let mut carrier = super::adapter::NativeDemandCarrier::Native(work);
        with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let owner = SourceViewGate::callback_owner();
                let caller = carrier.cancellation();
                let _lease = self.source_view.read_for_callback_with_carrier(
                    owner, Some(self.source_generation), carrier, &caller,
                    Some(self.authored.cancellation()),
                ).await?;
                let file = self.current_authored_with_carrier(owner, carrier).await?;
                let node = if file.is_none() && refresh {
                    let budget = carrier.with_work(|work| work.remaining()).map_err(mount_demand_error)?;
                    let result = self.lazy.source_lookup_measured(
                        self.lazy.source_reference(), &self.path, budget, &caller).await;
                    carrier.with_work(|work| mount_accept(work, result, lazy_error))?
                        .filter(|node| node.file_identity == self.source_node.file_identity)
                        .unwrap_or(self.source_node)
                } else { self.source_node };
                let bytes = file.as_ref().map_or(0, |(_, bytes)| *bytes);
                let result = operation(file.as_ref().map(|(file, _)| file.as_ref()), node, carrier);
                drop(file);
                carrier.with_work(|work| work.release(bytes)).map_err(mount_demand_error)?;
                result
            })
        })
    }

    fn with_authored_with_meter<T>(
        &self, work: &crate::NativeMaterializationWork,
        operation: impl FnOnce(&dyn MountOpenFile) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
        let mut carrier = super::adapter::NativeDemandCarrier::Native(work);
        with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let owner = SourceViewGate::callback_owner();
                let caller = carrier.cancellation();
                let _lease = self.source_view.write_stable_with_carrier(
                    Some(self.source_generation), carrier, &caller, Some(self.authored.cancellation()),
                ).await?;
                let file = match self.current_authored_with_carrier(owner, carrier).await? {
                    Some(file) => file,
                    None => {
                        stage_mount_promotion_with_carrier(&self.lazy, &self.authored, &self.path,
                            &self.mount_path, self.expected_source, carrier).await?;
                        self.authored.attached_file_by_id(
                            self.expected_source, owner, carrier).await?.ok_or(MountSourceError::Stale)?
                    }
                };
                let result = operation(file.0.as_ref());
                let bytes = file.1;
                drop(file);
                carrier.with_work(|work| work.release(bytes)).map_err(mount_demand_error)?;
                result
            })
        })
    }

    fn read_source(&self, offset: u64, length: u64) -> Result<OwnedBytes, MountSourceError> {
        let mut work = crate::demand::DemandWork::new(
            crate::WorkBudget::UNBOUNDED, self.authored.cancellation(),
        );
        self.read_source_with_carrier(offset, length,
            &mut super::adapter::NativeDemandCarrier::Borrowed(&mut work))
            .map(|output| output.bytes)
    }

    fn read_source_with_carrier(
        &self,
        offset: u64,
        length: u64,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        if let Some(original) = carrier.original_native_work() {
            return crate::demand::read_file(&*self.source_file, offset, length, &original, original.cancellation()).map_err(mount_demand_error);
        }
        let budget = carrier.with_work(|work| work.remaining()).map_err(mount_demand_error)?;
        let caller = carrier.cancellation();
        let read = crate::demand::read_file(&*self.source_file, offset, length, budget, &caller);
        let output = carrier.with_work(|work| mount_accept(work, read, mount_demand_error))?;
        let retained = output.logical_owned_bytes;
        if let Err(error) = carrier.with_work(|work| work.record_adoption(retained, &None)) {
            drop(output);
            carrier.with_work(|work| work.release(retained)).map_err(mount_demand_error)?;
            return Err(mount_demand_error(error));
        }
        Ok(output)
    }

    fn source_lease(&self) -> Result<SourceViewLease, MountSourceError> {
        let owner = SourceViewGate::callback_owner();
        self.runtime.wait(|| {
            let source_view = Arc::clone(&self.source_view);
            async move {
                source_view
                    .read_for_callback(owner, Some(self.source_generation))
                    .await
            }
        })
    }

    fn with_authored<T>(
        &self,
        operation: impl FnOnce(&dyn MountOpenFile) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
        let _lease = self.runtime.wait(|| {
            let source_view = Arc::clone(&self.source_view);
            async move { source_view.write_stable(Some(self.source_generation)).await }
        })?;
        if let Some(file) = self.current_authored()? {
            return operation(file.as_ref());
        }
        let mut promoted = self.promoted.lock().map_err(|_| MountSourceError::Stale)?;
        if promoted.is_none() {
            self.runtime.wait(|| async {
                stage_mount_promotion(
                    &self.lazy,
                    &self.authored,
                    &self.path,
                    &self.mount_path,
                    self.expected_source,
                )
                .await
            })?;
            *promoted = Some(self.authored.open_file(&self.mount_path)?);
        }
        let file = promoted.as_ref().ok_or(MountSourceError::Stale)?;
        operation(file.as_ref())
    }
}

impl<A, O, D, S> MountOpenFile for LazyOpenFile<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    fn lookup_with_meter(&self, work: &crate::NativeMaterializationWork) -> Result<MountLookup, MountSourceError> {
        self.with_view_with_meter(work, true, |file, node, carrier| {
            if let Some(file) = file { return file.lookup_with_meter(work); }
            let budget = carrier.with_work(|work| work.remaining()).map_err(mount_demand_error)?;
            let caller = carrier.cancellation();
            let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, &caller);
            let result = self.lazy.source_file_id(&node.file_identity, &mut accounting);
            let spent = accounting.spent();
            let result = result.map(|value| crate::OperationReceipt { value, work: spent })
                .map_err(|error| crate::OperationFailure::new(error, spent));
            let id = carrier.with_work(|work| mount_accept(work, result, lazy_error))?;
            Ok(mount_lookup(LazyLookup::Source(node), id))
        })
    }

    fn read_range_with_meter(&self, offset: u64, length: u32, work: &crate::NativeMaterializationWork)
        -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        self.with_view_with_meter(work, false, |file, _, carrier| match file {
            Some(file) => file.read_range_with_meter(offset, length, work),
            None => self.read_source_with_carrier(offset, u64::from(length), carrier),
        })
    }

    fn read_up_to_with_meter(&self, offset: u64, maximum_bytes: u32, work: &crate::NativeMaterializationWork)
        -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        self.with_view_with_meter(work, false, |file, node, carrier| {
            if let Some(file) = file { return file.read_up_to_with_meter(offset, maximum_bytes, work); }
            let length = match node.logical_bytes {
                Some(length) => length.saturating_sub(offset).min(u64::from(maximum_bytes)),
                None => return Err(mount_content_invalid_with_carrier(carrier, "source is not regular")?),
            };
            if length == 0 {
                return carrier.empty_source_chunk();
            }
            self.read_source_with_carrier(offset, length, carrier)
        })
    }

    fn seek_with_meter(&self, offset: u64, target: MountSeekTarget, work: &crate::NativeMaterializationWork)
        -> Result<Option<u64>, MountSourceError> {
        self.with_view_with_meter(work, false, |file, node, carrier| {
            if let Some(file) = file { return file.seek_with_meter(offset, target, work); }
            carrier.with_work(|work| mount_items(work, 1))?;
            let length = match node.logical_bytes {
                Some(length) => length,
                None => return Err(mount_content_invalid_with_carrier(carrier, "seek requires a regular file")?),
            };
            Ok((offset < length).then_some(match target { MountSeekTarget::Data => offset, MountSeekTarget::Hole => length }))
        })
    }

    fn write_range_with_meter(&self, offset: u64, bytes: crate::kernel::OwnedSourceChunk,
        work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_authored_with_meter(work, |file| file.write_range_with_meter(offset, bytes, work))
    }

    fn resize_with_meter(&self, logical_bytes: u64, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_authored_with_meter(work, |file| file.resize_with_meter(logical_bytes, work))
    }

    fn allocate_range_with_meter(&self, offset: u64, length: u64, operation: MountRangeAllocation,
        work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_authored_with_meter(work, |file| file.allocate_range_with_meter(offset, length, operation, work))
    }

    fn set_attributes(&self, metadata: FileMetadata, logical_bytes: Option<u64>,
        work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_authored_with_meter(work, |file| file.set_attributes(metadata, logical_bytes, work))
    }

    fn read_attribute(&self, name: &[u8], work: &crate::NativeMaterializationWork)
        -> Result<Option<crate::kernel::OwnedSourceChunk>, MountSourceError> {
        self.with_view_with_meter(work, false, |file, _, _| match file {
            Some(file) => file.read_attribute(name, work),
            None => Ok(None),
        })
    }

    fn list_attributes_with_meter(&self, cursor: Option<&[u8]>, maximum_entries: u32,
        work: &crate::NativeMaterializationWork) -> Result<(MountAttributePage, crate::NativeAllocation), MountSourceError> {
        self.with_view_with_meter(work, false, |file, _, _| match file {
            Some(file) => file.list_attributes_with_meter(cursor, maximum_entries, work),
            None => Ok((MountAttributePage { names: Vec::new(), next_cursor: None },
                work.claim(0, 0, crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?)),
        })
    }

    fn write_attribute(&self, name: &[u8], value: crate::kernel::OwnedSourceChunk,
        mode: MountAttributeWriteMode, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_authored_with_meter(work, |file| file.write_attribute(name, value, mode, work))
    }

    fn remove_attribute(&self, name: &[u8], work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_authored_with_meter(work, |file| file.remove_attribute(name, work))
    }

    fn settle(&self, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_view_with_meter(work, false, |file, _, _| match file {
            Some(file) => file.settle(work),
            None => Ok(()),
        })
    }

    fn read_sparse_range(&self, offset: u64, length: u32, work: &crate::NativeMaterializationWork)
        -> Result<(super::MountSparseRange, crate::NativeAllocation), MountSourceError> {
        self.with_view_with_meter(work, false, |file, node, carrier| {
            if let Some(file) = file { return file.read_sparse_range(offset, length, work); }
            let original = work.clone();
            let logical_bytes = u64::from(length);
            if offset.checked_add(logical_bytes).is_none() {
                return Err(MountSourceError::Invalid(std::borrow::Cow::Borrowed("sparse range overflows the file address space")));
            }
            let source_length = match node.logical_bytes {
                Some(length) => length,
                None => return Err(MountSourceError::Invalid(std::borrow::Cow::Borrowed("seek requires a regular file"))),
            };
            let data_length = source_length.saturating_sub(offset).min(logical_bytes);
            if data_length == 0 {
                return Ok((super::MountSparseRange { logical_bytes, spans: Vec::new() },
                    original.claim(0, 0, crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?));
            }
            let output = self.read_source_with_carrier(offset, data_length, carrier)?;
            if output.bytes.len() as u64 != data_length {
                return Err(MountSourceError::Stale);
            }
            if !output.is_intrinsic() {
                return Err(MountSourceError::NativeUnsupported);
            }
            original.charge(crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED
            }).map_err(MountSourceError::NativeWork)?;
            let counters = crate::WorkCounters { items_returned: 1, ..crate::WorkCounters::UNCHARGED };
            original.admit(counters).map_err(MountSourceError::NativeWork)?;
            let span_bytes = std::mem::size_of::<super::MountSparseSpan>() as u64;
            let mut packet = (
                Vec::<super::MountSparseSpan>::new(),
                original.claim(span_bytes, 1, crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?,
            );
            packet.0.try_reserve_exact(1).map_err(MountSourceError::Allocation)?;
            packet.1.reconcile_capacity(
                (packet.0.capacity() * std::mem::size_of::<super::MountSparseSpan>()) as u64,
            ).map_err(MountSourceError::NativeWork)?;
            let (bytes, _, _) = output.into_owned_parts();
            packet.0.push(super::MountSparseSpan {
                relative_offset: 0, bytes,
            });
            original.record_admitted(counters).map_err(MountSourceError::NativeWork)?;
            Ok((super::MountSparseRange { logical_bytes, spans: packet.0 }, packet.1))
        })
    }

    fn write_sparse_range(&self, offset: u64, range: (super::MountSparseRange, crate::NativeAllocation),
        work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.with_authored_with_meter(work, |file| file.write_sparse_range(offset, range, work))
    }

    fn lookup(&self) -> Result<MountLookup, MountSourceError> {
        let _lease = self.source_lease()?;
        if let Some(file) = self.current_authored()? {
            return file.lookup();
        }
        let current = self.runtime.wait(|| async {
            self.lazy
                .source_lookup(self.lazy.source_reference(), &self.path)
                .await
                .map_err(lazy_error)
        })?;
        let node = current
            .filter(|node| node.file_identity == self.source_node.file_identity)
            .unwrap_or(self.source_node);
        Ok(mount_lookup(
            LazyLookup::Source(node),
            self.lazy.source_file_id(&node.file_identity, &mut crate::kernel::allocation::KernelAccounting::standalone(
                crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())).map_err(lazy_error)?,
        ))
    }

    fn read_range(&self, offset: u64, length: u32) -> Result<OwnedBytes, MountSourceError> {
        let _lease = self.source_lease()?;
        if let Some(file) = self.current_authored()? {
            return file.read_range(offset, length);
        }
        self.read_source(offset, u64::from(length))
    }

    fn read_up_to(&self, offset: u64, maximum_bytes: u32) -> Result<OwnedBytes, MountSourceError> {
        let _lease = self.source_lease()?;
        if let Some(file) = self.current_authored()? {
            return file.read_up_to(offset, maximum_bytes);
        }
        let length = self
            .source_node
            .logical_bytes
            .ok_or_else(|| MountSourceError::Invalid(std::borrow::Cow::Owned("source is not regular".to_owned())))?
            .saturating_sub(offset)
            .min(u64::from(maximum_bytes));
        if length == 0 {
            return Ok(OwnedBytes::new());
        }
        self.read_source(offset, length)
    }

    fn seek(&self, offset: u64, target: MountSeekTarget) -> Result<Option<u64>, MountSourceError> {
        let _lease = self.source_lease()?;
        if let Some(file) = self.current_authored()? {
            return file.seek(offset, target);
        }
        let length = self
            .source_node
            .logical_bytes
            .ok_or_else(|| MountSourceError::Invalid(std::borrow::Cow::Owned("seek requires a regular file".to_owned())))?;
        if offset >= length {
            return Ok(None);
        }
        Ok(Some(match target {
            MountSeekTarget::Data => offset,
            MountSeekTarget::Hole => length,
        }))
    }

    fn write_range(&self, offset: u64, bytes: Bytes) -> Result<(), MountSourceError> {
        self.with_authored(|file| file.write_range(offset, bytes))
    }

    fn resize(&self, logical_bytes: u64) -> Result<(), MountSourceError> {
        self.with_authored(|file| file.resize(logical_bytes))
    }

    fn allocate_range(
        &self,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        self.with_authored(|file| file.allocate_range(offset, length, operation))
    }

    fn list_attributes(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        let _lease = self.source_lease()?;
        match self.current_authored()? {
            Some(file) => file.list_attributes(cursor, maximum_entries),
            None => Ok(MountAttributePage {
                names: Vec::new(),
                next_cursor: None,
            }),
        }
    }

}

impl<A, O, D, S> MountFilesystem for LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    fn native_namespace_lineage(&self) -> Option<super::NativeMountSourceLineage<'_>> {
        let mut lineage = self.authored.native_namespace_lineage()?;
        lineage.lazy_owner = Some(self.lazy.clone());
        lineage.lazy_root = Some(&self.root);
        Some(lineage)
    }

    fn native_callback_guard(&self) -> Option<Arc<dyn super::NativeMountCallbackGuard>> {
        self.authored.native_callback_guard()
    }

    fn native_original_work(&self) -> Result<Option<crate::NativeMaterializationWork>, MountSourceError> {
        self.authored.native_original_work()
    }

    fn supports_posix_named_attributes(&self) -> bool {
        self.authored.supports_posix_named_attributes()
    }

    fn flush_on_handle_close(&self) -> bool {
        false
    }

    fn flush_publishes(&self) -> bool {
        self.authored.flush_publishes()
    }

    fn view_is_stable(&self) -> bool {
        self.source_view.is_stable()
    }

    fn view_stamp(&self) -> Option<ViewStamp> {
        self.ledger_stamp().filter(|_| self.source_observed())
    }

    fn observe_view(&self, observer: Weak<dyn ViewObserver>) {
        self.authored.observe_view(observer)
    }

    fn observe_view_with_meter(
        &self, observer: Weak<dyn ViewObserver>, work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.authored.observe_view_with_meter(observer, work)
    }

    fn fence_changes(&self) -> Result<(), MountSourceError> {
        self.source_watch.as_ref().map_or(Ok(()), |watch| {
            crate::demand::fence_watch(&**watch, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).map(|receipt| receipt.value).map_err(|failure| lazy_error(failure.error.into()))
        })
    }

    fn fence_changes_with_meter(
        &self, work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.fence_changes_with_carrier(&mut super::adapter::NativeDemandCarrier::Native(work))
    }

    fn shutdown_with_meter(
        &self, work: &crate::NativeMaterializationWork,
    ) -> Result<bool, MountSourceError> {
        let changed = !self.physically_retired()?;
        self.cancel(&mut super::adapter::NativeDemandCarrier::Native(work))?;
        Ok(changed)
    }

    fn shutdown_with_cleanup_wait(
        &self, wait: &crate::CancellationToken,
    ) -> Result<bool, MountSourceError> {
        let changed = !self.physically_retired()?;
        self.cancel_with_cleanup_wait(wait)?;
        Ok(changed)
    }

    fn shutdown_accepted_cleanup(&self) -> Result<bool, MountSourceError> {
        let changed = !self.physically_retired()?;
        self.cancel_accepted_cleanup()?;
        Ok(changed)
    }

    fn unchanged_since(&self, path: &MountPath, file_id: Option<FileId>, stamp: ViewStamp) -> bool {
        self.source_view.is_stable()
            && file_id.is_none_or(|file_id| self.reported(file_id))
            && self.authored.unchanged_since(path, file_id, stamp)
    }

    fn view_is_stable_with_meter(&self, work: &crate::NativeMaterializationWork) -> Result<bool, MountSourceError> {
        let mut carrier = super::adapter::NativeDemandCarrier::Native(work);
        carrier.with_work(|work| mount_items(work, 1))?;
        Ok(self.source_view.is_stable())
    }

    fn view_stamp_with_meter(&self, work: &crate::NativeMaterializationWork) -> Result<Option<ViewStamp>, MountSourceError> {
        if !self.view_is_stable_with_meter(work)? || !self.source_observed() { return Ok(None); }
        self.authored.view_stamp_with_meter(work)
    }

    fn binding_epoch_with_meter(&self, work: &crate::NativeMaterializationWork) -> Result<Option<u64>, MountSourceError> {
        let mut carrier = super::adapter::NativeDemandCarrier::Native(work);
        carrier.with_work(|work| mount_items(work, 1))?;
        Ok(Some(self.source_view.generation()))
    }

    fn folded_path_with_meter(&self, path: &MountPath, work: &crate::NativeMaterializationWork)
        -> Result<Option<(MountPath, u64)>, MountSourceError> {
        self.authored.folded_path_with_meter(path, work)
    }

    fn unchanged_since_with_meter(&self, path: &MountPath, file_id: Option<FileId>, stamp: ViewStamp,
        work: &crate::NativeMaterializationWork) -> Result<bool, MountSourceError> {
        if !self.view_is_stable_with_meter(work)? { return Ok(false); }
        if let Some(file_id) = file_id { if !self.reported_with_meter(file_id, work)? { return Ok(false); } }
        self.authored.unchanged_since_with_meter(path, file_id, stamp, work)
    }

    fn reported_unchanged_since_with_meter(&self, path: &MountPath, file_id: Option<FileId>, stamp: ViewStamp,
        work: &crate::NativeMaterializationWork) -> Result<bool, MountSourceError> {
        if !self.view_is_stable_with_meter(work)? { return Ok(false); }
        if let Some(file_id) = file_id { if !self.reported_with_meter(file_id, work)? { return Ok(false); } }
        self.authored.reported_unchanged_since_with_meter(path, file_id, stamp, work)
    }

    fn node_unchanged_since_with_meter(&self, file_id: FileId, stamp: ViewStamp,
        work: &crate::NativeMaterializationWork) -> Result<bool, MountSourceError> {
        Ok(self.view_is_stable_with_meter(work)? && self.reported_with_meter(file_id, work)?
            && self.authored.node_unchanged_since_with_meter(file_id, stamp, work)?)
    }

    fn listing_unchanged_since_with_meter(&self, path: &MountPath, stamp: ViewStamp,
        work: &crate::NativeMaterializationWork) -> Result<bool, MountSourceError> {
        Ok(self.view_is_stable_with_meter(work)? && self.authored.listing_unchanged_since_with_meter(path, stamp, work)?)
    }

    fn reports_changes_to(&self, file_id: FileId) -> bool {
        self.source_observed() && self.reported(file_id)
    }

    fn reported_unchanged_since(
        &self,
        path: &MountPath,
        file_id: Option<FileId>,
        stamp: ViewStamp,
    ) -> bool {
        self.source_view.is_stable()
            && file_id.is_none_or(|file_id| self.reported(file_id))
            && self.authored.reported_unchanged_since(path, file_id, stamp)
    }

    fn node_unchanged_since(&self, file_id: FileId, stamp: ViewStamp) -> bool {
        self.source_view.is_stable()
            && self.reported(file_id)
            && self.authored.node_unchanged_since(file_id, stamp)
    }

    fn listing_unchanged_since(&self, path: &MountPath, stamp: ViewStamp) -> bool {
        self.source_view.is_stable() && self.authored.listing_unchanged_since(path, stamp)
    }

    fn binding_epoch(&self) -> Option<u64> {
        Some(self.source_view.generation())
    }

    fn acquire_view_lease(&self) -> Result<Box<dyn Send>, MountSourceError> {
        Ok(Box::new(self.view_lease(None)?))
    }

    fn acquire_binding_lease(
        &self,
        expected_epoch: Option<u64>,
    ) -> Result<Box<dyn Send>, MountSourceError> {
        self.view_lease(expected_epoch)
            .map(|lease| Box::new(lease) as Box<dyn Send>)
    }

    fn acquire_binding_lease_with_meter(
        &self, expected_epoch: Option<u64>, work: &crate::NativeMaterializationWork,
    ) -> Result<Box<dyn Send>, MountSourceError> {
        self.acquire_binding_lease_with_carrier(
            expected_epoch, &mut super::adapter::NativeDemandCarrier::Native(work),
        )
    }

    fn lookup(&self, path: &MountPath) -> Result<Option<MountLookup>, MountSourceError> {
        Ok(self.lookup_pinned(path)?.map(|(lookup, _)| lookup))
    }

    fn lookup_with_meter(
        &self, path: &MountPath, work: &crate::NativeMaterializationWork,
    ) -> Result<Option<MountLookup>, MountSourceError> {
        with_native_callback_carrier(&self.runtime,
            &mut super::adapter::NativeDemandCarrier::Native(work), |carrier, prepared| {
                self.runtime.wait_prepared(prepared, || async {
                    self.lookup_pinned_in_view(path, carrier).await
                        .map(|lookup| lookup.map(|(lookup, _)| lookup))
                })
            },
        )
    }

    fn lookup_pinned(
        &self, path: &MountPath,
    ) -> Result<Option<(MountLookup, Option<MountContentPin>)>, MountSourceError> {
        let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, self.authored.cancellation());
        with_native_callback_carrier(&self.runtime,
            &mut super::adapter::NativeDemandCarrier::Borrowed(&mut work), |carrier, prepared| {
                self.runtime.wait_prepared(prepared, || self.lookup_pinned_in_view(path, carrier))
            })
    }

    fn lookup_pinned_with_meter(
        &self, path: &MountPath, work: &crate::NativeMaterializationWork,
    ) -> Result<Option<(MountLookup, Option<MountContentPin>)>, MountSourceError> {
        with_native_callback_carrier(&self.runtime, &mut super::adapter::NativeDemandCarrier::Native(work), |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || self.lookup_pinned_in_view(path, carrier))
        })
    }

    fn open_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        self.native_call(|work| self.open_file_with_meter(path, work)).map(|(file, _)| file)
    }

    fn open_file_with_meter(&self, path: &MountPath, work: &crate::NativeMaterializationWork)
        -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
        self.open_proven(path, work).map(|(file, _)| file)
    }

    fn open_file_with_lookup(&self, path: &MountPath) -> Result<(Arc<dyn MountOpenFile>, MountLookup), MountSourceError> {
        self.native_call(|work| self.open_file_with_lookup_with_meter(path, work)).map(|((file, _), lookup)| (file, lookup))
    }

    fn open_file_with_lookup_with_meter(&self, path: &MountPath, work: &crate::NativeMaterializationWork)
        -> Result<((Arc<dyn MountOpenFile>, u64), MountLookup), MountSourceError> {
        let (file, proven) = self.open_proven(path, work)?;
        let lookup = match proven { Some(lookup) => lookup, None => file.0.lookup_with_meter(work)? };
        Ok((file, lookup))
    }

    fn detach_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        self.native_call(|work| self.detach_file_with_meter(path, work)).map(|(file, _)| file)
    }

    fn detach_file_with_meter(&self, path: &MountPath, work: &crate::NativeMaterializationWork)
        -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
        self.native_mutation(path, None, true, false, work, |detached| {
            let file = match detached {
                Some(file) => (file as Arc<dyn MountOpenFile>, 0),
                None => self.authored.detach_file_with_meter(path, work)?,
            };
            self.bind_open_file(file, None, self.source_view.generation(), work)
        })
    }

    fn read_link(
        &self, path: &MountPath, work: &crate::NativeMaterializationWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        with_native_callback_carrier(&self.runtime, &mut super::adapter::NativeDemandCarrier::Native(work), |_, prepared| {
            self.runtime.wait_prepared(prepared, || self.read_link_async(path, work))
        })
    }

    fn read_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u32,
    ) -> Result<OwnedBytes, MountSourceError> {
        let mut read = OwnedBytes::new();
        self.read_content(
            path,
            None,
            offset,
            u64::from(length),
            length,
            &mut |_, bytes| {
                read = bytes;
                Ok(())
            },
        )?;
        Ok(read)
    }

    fn read_range_with_meter(
        &self, path: &MountPath, offset: u64, length: u32, work: &crate::NativeMaterializationWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        let mut read = None;
        self.read_content_with_carrier(
            path, None, offset, u64::from(length), length,
            &mut |_, output| { read = Some(output); Ok(()) },
            &mut super::adapter::NativeDemandCarrier::Native(work),
        )?;
        match read {
            Some(read) => Ok(read),
            None => super::adapter::NativeDemandCarrier::Native(work).empty_source_chunk(),
        }
    }

    fn read_pinned(
        &self,
        path: &MountPath,
        pin: MountContentPin,
        offset: u64,
        length: u64,
        piece: u32,
        sink: &mut ContentSink<'_>,
    ) -> Result<(), MountSourceError> {
        self.read_content(path, Some(pin), offset, length, piece, sink)
    }

    fn read_pinned_with_meter(
        &self, path: &MountPath, pin: MountContentPin, offset: u64, length: u64,
        piece: u32, sink: &mut super::NativeMountContentSink<'_>, work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.read_content_with_carrier(path, Some(pin), offset, length, piece, sink,
            &mut super::adapter::NativeDemandCarrier::Native(work))
    }

    fn seek(
        &self,
        path: &MountPath,
        offset: u64,
        target: MountSeekTarget,
    ) -> Result<Option<u64>, MountSourceError> {
        let _lease = self.view_lease(None)?;
        if self.authored.lookup(path)?.is_some() {
            return self.authored.seek(path, offset, target);
        }
        let path = self.path(path)?;
        if self.is_removed(&path)? {
            return Err(MountSourceError::NotFound);
        }
        self.wait(|| async move {
            self.lazy
                .seek(
                    &path,
                    offset,
                    match target {
                        MountSeekTarget::Data => LazySeekTarget::Data,
                        MountSeekTarget::Hole => LazySeekTarget::Hole,
                    },
                )
                .await
                .map_err(lazy_error)
        })
    }

    fn read_directory(
        &self, path: &MountPath, cursor: Option<&[u8]>, maximum_entries: u32,
    ) -> Result<MountDirectoryPage, MountSourceError> {
        let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, self.authored.cancellation());
        self.list_directory(path, cursor, maximum_entries,
            &mut super::adapter::NativeDemandCarrier::Borrowed(&mut work))
    }

    fn read_directory_with_meter(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<MountDirectoryPage, MountSourceError> {
        self.list_directory(
            path, cursor, maximum_entries,
            &mut super::adapter::NativeDemandCarrier::Native(work),
        )
    }

    fn create_file(&self, path: &MountPath, metadata: FileMetadata, absent_since: Option<ViewStamp>,
        work: &crate::NativeMaterializationWork) -> Result<MountLookup, MountSourceError> {
        self.native_mutation(path, None, false, true, work, |_|
            self.authored.create_file(path, metadata, absent_since, work))
    }

    fn create_directory(&self, path: &MountPath, metadata: FileMetadata, work: &crate::NativeMaterializationWork)
        -> Result<MountLookup, MountSourceError> {
        self.native_mutation(path, None, false, true, work, |_| self.authored.create_directory(path, metadata, work))
    }

    fn create_symbolic_link(&self, path: &MountPath, target: crate::kernel::OwnedSourceChunk,
        metadata: FileMetadata, work: &crate::NativeMaterializationWork) -> Result<MountLookup, MountSourceError> {
        self.native_mutation(path, None, false, true, work, |_| self.authored.create_symbolic_link(path, target, metadata, work))
    }

    fn create_special(&self, path: &MountPath, kind: MountNodeKind, device: Option<(u32, u32)>,
        metadata: FileMetadata, work: &crate::NativeMaterializationWork) -> Result<MountLookup, MountSourceError> {
        self.native_mutation(path, None, false, true, work, |_| self.authored.create_special(path, kind, device, metadata, work))
    }

    fn hard_link(&self, source: &MountPath, destination: &MountPath, work: &crate::NativeMaterializationWork)
        -> Result<(), MountSourceError> {
        self.native_mutation(destination, Some(source), false, true, work, |_| self.authored.hard_link(source, destination, work))
    }

    fn clone_range_by_id(
        &self, _source_file_id: FileId, _source_offset: u64,
        _destination_file_id: FileId, _destination_offset: u64, _length: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        work.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        Err(MountSourceError::Unsupported(std::borrow::Cow::Borrowed("clone-by-identity requires path-backed promotion in a lazy mount")))
    }

    fn read_attribute(&self, path: &MountPath, name: &[u8], work: &crate::NativeMaterializationWork)
        -> Result<Option<crate::kernel::OwnedSourceChunk>, MountSourceError> {
        self.native_attributes(path, work, |file, projected| match file {
            Some(file) => file.read_attribute(name, work),
            None if projected => Ok(None),
            None => self.authored.read_attribute(path, name, work),
        })
    }

    fn list_attributes_with_meter(&self, path: &MountPath, cursor: Option<&[u8]>, maximum_entries: u32,
        work: &crate::NativeMaterializationWork) -> Result<(MountAttributePage, crate::NativeAllocation), MountSourceError> {
        self.native_attributes(path, work, |file, projected| match file {
            Some(file) => file.list_attributes_with_meter(cursor, maximum_entries, work),
            None if projected => Ok((MountAttributePage { names: Vec::new(), next_cursor: None },
                work.claim(0, 0, crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?)),
            None => self.authored.list_attributes_with_meter(path, cursor, maximum_entries, work),
        })
    }

    fn write_attribute(&self, path: &MountPath, name: &[u8], value: crate::kernel::OwnedSourceChunk,
        mode: MountAttributeWriteMode, work: &crate::NativeMaterializationWork) -> Result<(), MountSourceError> {
        self.native_mutation(path, None, true, false, work, |file| match file {
            Some(file) => file.write_attribute(name, value, mode, work),
            None => self.authored.write_attribute(path, name, value, mode, work),
        })
    }

    fn remove_attribute(&self, path: &MountPath, name: &[u8], work: &crate::NativeMaterializationWork)
        -> Result<(), MountSourceError> {
        self.native_mutation(path, None, true, false, work, |file| match file {
            Some(file) => file.remove_attribute(name, work),
            None => self.authored.remove_attribute(path, name, work),
        })
    }

    fn open_created(&self, path: &MountPath, created: &MountLookup) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        self.native_call(|work| self.open_created_with_meter(path, created, work)).map(|(file, _)| file)
    }

    fn open_created_with_meter(&self, path: &MountPath, created: &MountLookup, work: &crate::NativeMaterializationWork)
        -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
        let mut carrier = super::adapter::NativeDemandCarrier::Native(work);
        let owner = SourceViewGate::callback_owner();
        let lease = with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || self.source_read_lease_with_carrier(owner, None, carrier))
        })?;
        let file = self.authored.open_created_with_meter(path, created, work)?;
        self.bind_open_file(file, Some(created.node.file_id), lease.generation, work)
    }

    fn set_attributes(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.set_attributes_with_carrier(
            path, metadata, logical_bytes,
            &mut super::adapter::NativeDemandCarrier::Native(work),
        )
    }

    fn list_attributes(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        let _lease = self.view_lease(None)?;
        if let Some(file) = self.detached_for_path(path)? {
            return file.list_attributes(cursor, maximum_entries);
        }
        if self.is_source_projection(path)? {
            return Ok(MountAttributePage {
                names: Vec::new(),
                next_cursor: None,
            });
        }
        self.authored.list_attributes(path, cursor, maximum_entries)
    }


    fn write_range(
        &self,
        path: &MountPath,
        offset: u64,
        bytes: Bytes,
    ) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        if let Some(file) = self.detached_for_path(path)? {
            return file.write_range(offset, bytes);
        }
        self.promote_locked(path)?;
        self.authored.write_range(path, offset, bytes)
    }

    fn resize(&self, path: &MountPath, logical_bytes: u64) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        if let Some(file) = self.detached_for_path(path)? {
            return file.resize(logical_bytes);
        }
        self.promote_locked(path)?;
        self.authored.resize(path, logical_bytes)
    }

    fn allocate_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        if let Some(file) = self.detached_for_path(path)? {
            return file.allocate_range(offset, length, operation);
        }
        self.promote_locked(path)?;
        self.authored
            .allocate_range(path, offset, length, operation)
    }

    fn clone_range(
        &self,
        source: &MountPath,
        source_offset: u64,
        destination: &MountPath,
        destination_offset: u64,
        length: u64,
    ) -> Result<(), MountSourceError> {
        let _mutation = self.mutation_lease(None)?;
        if self.detached_for_path(source)?.is_some()
            || self.detached_for_path(destination)?.is_some()
        {
            return Err(MountSourceError::Unsupported(std::borrow::Cow::Owned("range cloning a detached identity requires identity-level clone support"
                    .to_owned(),)));
        }
        self.promote_locked(source)?;
        self.promote_locked(destination)?;
        self.authored.clone_range(
            source,
            source_offset,
            destination,
            destination_offset,
            length,
        )
    }

    fn remove(
        &self,
        path: &MountPath,
        expected: Option<FileId>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        let mut carrier = super::adapter::NativeDemandCarrier::Native(work);
        with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let _mutation = self.source_mutation_lease_with_carrier(None, carrier).await?;
                let owner = SourceViewGate::callback_owner();
                let mut scratch = DirectoryCarrierScratch::new(carrier);
                scratch.prepare_finalization()?;
                let text = mount_path_text_with_carrier(&self.root, path, scratch.carrier)?;
                scratch.track(text.legacy_owned_bytes())?;
                let resolution = self.resolve(path, &text, owner, scratch.carrier).await?;
                let (file_id, authored) = match resolution {
                    Resolution::Absent => return Err(MountSourceError::NotFound),
                    Resolution::Authored(lookup) => (lookup.node.file_id, Some(lookup)),
                    Resolution::Unauthored(lookup, _, allocation) => {
                        let owned = MountOwnedBuffer { legacy_bytes: lookup.logical_owned_bytes(), value: lookup, allocation };
                        (self.mutation_file_id_with_carrier(&text, &owned.value, scratch.carrier).await?, None)
                    }
                };
                if expected.is_some_and(|expected| expected != file_id) { return Err(MountSourceError::Stale); }
                let opened = scratch.carrier.with_work(|work| self.has_open_source_handle_with_work(file_id, work))?;
                let (remove_authored, identity) = if opened {
                    let lookup = if let Some(authored) = authored { authored } else {
                        self.promote_with_carrier(path, owner, scratch.carrier).await?;
                        let lookup = self.authored.lookup_async(path, owner, scratch.carrier).await?
                            .ok_or(MountSourceError::Stale)?;
                        if lookup.node.file_id != file_id { return Err(MountSourceError::Stale); }
                        lookup
                    };
                    let identity = self.prepare_removed_identity_with_carrier(lookup, owner, scratch.carrier).await?;
                    (true, Some(identity))
                } else { (authored.is_some(), None) };
                let removal = self.prepare_removed_path_with_carrier(path, text, file_id, identity.as_ref(), scratch.carrier)?;
                if removal.key.native_allocation.is_none() { scratch.keep(removal.key.capacity() as u64)?; }
                scratch.reserve_final_work(removal.final_work)?;
                if let Some(identity) = &identity { scratch.reserve_final_work(identity.final_work)?; }
                if remove_authored {
                    let guarded = if authored.is_some() { expected } else { Some(file_id) };
                    self.authored.remove_async_with_carrier(path, guarded, scratch.carrier).await?;
                } else {
                    scratch.carrier.with_work(|work| work.check()).map_err(mount_demand_error)?;
                }
                self.commit_removed_with_carrier(removal, identity, None, &mut scratch)
            })
        })
    }

    fn rename(
        &self,
        source: &MountPath,
        destination: &MountPath,
        replace: bool,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        let mut carrier = super::adapter::NativeDemandCarrier::Native(work);
        with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let _mutation = self.source_mutation_lease_with_carrier(None, carrier).await?;
                let owner = SourceViewGate::callback_owner();
                let mut scratch = DirectoryCarrierScratch::new(carrier);
                scratch.prepare_finalization()?;
                let source_text = mount_path_text_with_carrier(&self.root, source, scratch.carrier)?;
                scratch.track(source_text.legacy_owned_bytes())?;
                let destination_text = mount_path_text_with_carrier(&self.root, destination, scratch.carrier)?;
                scratch.track(destination_text.legacy_owned_bytes())?;
                let resolved = self.resolve(source, &source_text, owner, scratch.carrier).await?;
                if matches!(resolved, Resolution::Absent) { return Err(MountSourceError::NotFound); }
                if source_text.value == destination_text.value { return Ok(()); }
                if matches!(resolved, Resolution::Unauthored(LazyLookup::Source(SourceNode {
                    kind: SourceNodeKind::Directory, ..
                }), _, _)) {
                    self.promote_subtree_with_carrier(source, owner, scratch.carrier).await?;
                }
                self.promote_with_carrier(source, owner, scratch.carrier).await?;
                let source_id = self.authored.lookup_async(source, owner, scratch.carrier).await?
                    .ok_or(MountSourceError::Stale)?.node.file_id;
                self.promote_parents_with_carrier(destination, owner, scratch.carrier).await?;
                if self.authored.lookup_async(destination, owner, scratch.carrier).await?.is_none()
                    && !scratch.carrier.with_work(|work| self.is_removed_with_work(&destination_text, work))? {
                    match self.resolve(destination, &destination_text, owner, scratch.carrier).await? {
                        Resolution::Absent => {}
                        _ if replace => self.promote_with_carrier(destination, owner, scratch.carrier).await?,
                        _ => return Err(MountSourceError::AlreadyExists),
                    }
                }
                let identity = if replace {
                    if let Some(lookup) = self.authored.lookup_async(destination, owner, scratch.carrier).await? {
                        if lookup.node.file_id != source_id
                            && scratch.carrier.with_work(|work| self.has_open_source_handle_with_work(lookup.node.file_id, work))? {
                            let identity = self.prepare_removed_identity_with_carrier(lookup, owner, scratch.carrier).await?;
                            Some(identity)
                        } else { None }
                    } else { None }
                } else { None };
                let removal = self.prepare_removed_path_with_carrier(
                    source, source_text, source_id, identity.as_ref(), scratch.carrier,
                )?;
                if removal.key.native_allocation.is_none() { scratch.keep(removal.key.capacity() as u64)?; }
                let rebound = self.prepare_rebound_with_carrier(
                    &destination_text, identity.as_ref(), &removal, scratch.carrier,
                )?;
                scratch.reserve_final_work(removal.final_work)?;
                scratch.reserve_final_work(rebound.final_work)?;
                if let Some(identity) = &identity { scratch.reserve_final_work(identity.final_work)?; }
                self.authored.rename_async_with_carrier(source, destination, replace, scratch.carrier).await?;
                self.commit_removed_with_carrier(removal, identity, Some((&destination_text, rebound)), &mut scratch)
            })
        })
    }

    fn flush(
        &self, work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.flush_with_carrier(&mut super::adapter::NativeDemandCarrier::Native(work))
    }

    fn capture_host_paths_with_meter(
        &self, source_root: &Path, paths: &[MountPath],
        expected_root_identity: NativeRootIdentity, work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.capture_host_with_native_work(
            source_root, paths, expected_root_identity, super::adapter::HostCaptureScope::ExactPaths,
            work,
        )
    }

    fn capture_host_subtree_with_meter(
        &self, source_root: &Path, path: &MountPath,
        expected_root_identity: NativeRootIdentity, work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        self.capture_host_with_native_work(
            source_root, std::slice::from_ref(path), expected_root_identity,
            super::adapter::HostCaptureScope::Subtree, work,
        )
    }

    fn capture_host_path(
        &self,
        source_root: &Path,
        path: &MountPath,
    ) -> Result<(), MountSourceError> {
        self.capture_host_paths(source_root, std::slice::from_ref(path))
    }

    fn capture_host_paths(
        &self,
        source_root: &Path,
        paths: &[MountPath],
    ) -> Result<(), MountSourceError> {
        if paths.is_empty() {
            return Ok(());
        }
        let expected_root_identity = capture_root_identity(source_root)
            .map_err(|error| MountSourceError::Engine(std::borrow::Cow::Owned(error.to_string())))?;
        self.capture_host_paths_with_identity(source_root, paths, expected_root_identity)
    }

    fn capture_host_subtree(
        &self,
        source_root: &Path,
        path: &MountPath,
    ) -> Result<(), MountSourceError> {
        let expected_root_identity = capture_root_identity(source_root)
            .map_err(|error| MountSourceError::Engine(std::borrow::Cow::Owned(error.to_string())))?;
        let original = crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED, self.authored.cancellation(),
        ).map_err(|(error, _)| MountSourceError::NativeWork(error))?;
        self.capture_host_with_native_work(
            source_root, std::slice::from_ref(path), expected_root_identity,
            super::adapter::HostCaptureScope::Subtree, &original,
        )
    }
}

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    /// Records that another handle on the workspace changed each of
    pub(crate) fn changed_outside(&self, paths: &[String]) -> Result<(), MountSourceError> {
        let mut effect = ViewEffect::default();
        for path in paths {
            effect
                .subtrees
                .push(self.lazy.namespace_path(path).map_err(lazy_error)?);
        }
        if !effect.is_empty() {
            self.cursors.clear();
            self.authored
                .record_projection_change(&ViewChange::Effect(&effect));
        }
        Ok(())
    }

    pub(crate) fn capture_host_paths_with_identity(
        &self,
        source_root: &Path,
        paths: &[MountPath],
        expected_root_identity: NativeRootIdentity,
    ) -> Result<(), MountSourceError> {
        let original = crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED, self.authored.cancellation(),
        ).map_err(|(error, _)| MountSourceError::NativeWork(error))?;
        self.capture_host_with_native_work(
            source_root, paths, expected_root_identity, super::adapter::HostCaptureScope::ExactPaths,
            &original,
        )
    }

    fn open_proven(
        &self, path: &MountPath, work: &crate::NativeMaterializationWork,
    ) -> Result<((Arc<dyn MountOpenFile>, u64), Option<MountLookup>), MountSourceError> {
        let mut carrier = super::adapter::NativeDemandCarrier::Native(work);
        let path_text = mount_path_text_with_carrier(&self.root, path, &mut carrier)?;
        let owner = SourceViewGate::callback_owner();
        let (lease, resolution, source_file) = with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let lease = self.source_read_lease_with_carrier(owner, None, carrier).await?;
                let (resolution, source_file) = self.resolve_opened_with_carrier(
                    path, &path_text, owner, carrier,
                ).await?;
                Ok((lease, resolution, source_file))
            })
        })?;
        let generation = lease.generation;
        let lookup = match resolution {
            Resolution::Absent => return Err(MountSourceError::NotFound),
            Resolution::Authored(lookup) => {
                let detached = carrier.with_work(|work| self.detached_by_id_with_work(lookup.node.file_id, work))?;
                let file = match detached {
                    Some(file) => (file as Arc<dyn MountOpenFile>, 0),
                    None => self.authored.open_file_with_meter(path, work)?,
                };
                return self.bind_open_file(file, Some(lookup.node.file_id), generation, work)
                    .map(|file| (file, None));
            }
            Resolution::Unauthored(lookup, source, allocation) => {
                let lookup = MountOwnedBuffer { legacy_bytes: lookup.logical_owned_bytes(), value: lookup, allocation };
                match &lookup.value {
                    LazyLookup::Source(node) if node.kind == SourceNodeKind::RegularFile => {
                        let source_file = source.and(source_file).ok_or(MountSourceError::Stale)?;
                        let expected_source = self.lazy.source_file_id(&node.file_identity,
                            &mut crate::kernel::allocation::KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())).map_err(lazy_error)?;
                        let (mount_path, mut mount_allocation, _) = mount_components_with_carrier(
                            path.components(), None, &mut carrier,
                        )?.into_parts();
                        let MountOwnedBuffer { value: path, allocation: mut input_allocation, .. } = path_text;
                        if let (Some(input), Some(mount)) = (&mut input_allocation, &mut mount_allocation) {
                            input.absorb(mount).map_err(MountSourceError::NativeWork)?;
                        }
                        let bytes = std::mem::size_of::<LazyOpenFile<A, O, D, S>>() as u64;
                        carrier.with_work(|work| work.allocate(bytes)).map_err(mount_demand_error)?;
                        let file: Arc<dyn MountOpenFile> = Arc::new(LazyOpenFile {
                            lazy: Arc::clone(&self.lazy), authored: Arc::clone(&self.authored), runtime: Arc::clone(&self.runtime),
                            path, mount_path, expected_source, source_file, source_node: *node, source_generation: generation,
                            source_view: Arc::clone(&self.source_view), detached: Arc::clone(&self.detached),
                            open_sources: Arc::clone(&self.open_sources), promoted: Mutex::new(None), unstaged: Arc::clone(&self.unstaged),
                            _input_allocation: input_allocation,
                        });
                        self.open_sources.lock().map_err(|_| MountSourceError::Stale)?
                            .entry(expected_source).or_default().push(Arc::downgrade(&file));
                        return Ok(((file, bytes), Some(mount_lookup(LazyLookup::Source(*node), expected_source))));
                    }
                    LazyLookup::Source(_) => return Err(MountSourceError::Invalid(std::borrow::Cow::Owned("open requires a regular file".to_owned()))),
                    LazyLookup::Authored { stat, .. } => stat.file_id,
                    LazyLookup::Shadow { record, .. } => record.file_id,
                }
            }
        };
        let detached = carrier.with_work(|work| self.detached_by_id_with_work(lookup, work))?;
        if let Some(file) = detached {
            return self.bind_open_file((file, 0), Some(lookup), generation, work).map(|file| (file, None));
        }
        drop(lease);
        let mutation = with_native_callback_carrier(&self.runtime, &mut carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let lease = self.source_mutation_lease_with_carrier(Some(generation), carrier).await?;
                self.promote_with_carrier(path, owner, carrier).await?;
                Ok(lease)
            })
        })?;
        let file = self.authored.open_file_with_meter(path, work)?;
        let result = self.bind_open_file(file, Some(lookup), generation, work).map(|file| (file, None));
        drop(mutation);
        result
    }

    fn bind_open_file(
        &self, file: (Arc<dyn MountOpenFile>, u64), file_id: Option<FileId>, generation: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
        let bytes = std::mem::size_of::<ViewBoundOpenFile<A, O>>() as u64;
        work.native_read_meter().lock().allocate(bytes).map_err(mount_demand_error)?;
        let bound: Arc<dyn MountOpenFile> = Arc::new(ViewBoundOpenFile {
            inner: file.0, runtime: Arc::clone(&self.runtime), source_view: Arc::clone(&self.source_view),
            source_lifetime: self.authored.cancellation().clone(), detached: Arc::clone(&self.detached),
            open_sources: Arc::clone(&self.open_sources), file_id, generation,
        });
        if let Some(file_id) = file_id {
            self.open_sources.lock().map_err(|_| MountSourceError::Stale)?
                .entry(file_id).or_default().push(Arc::downgrade(&bound));
        }
        Ok((bound, bytes.checked_add(file.1).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?))
    }

    fn mutation_lease(
        &self,
        expected: Option<u64>,
    ) -> Result<SourceMutationLease, MountSourceError> {
        self.wait(|| {
            let source_view = Arc::clone(&self.source_view);
            async move { source_view.write_stable(expected).await }
        })
    }

    fn view_lease(&self, expected: Option<u64>) -> Result<SourceViewLease, MountSourceError> {
        let owner = SourceViewGate::callback_owner();
        self.wait(|| {
            let source_view = Arc::clone(&self.source_view);
            async move { source_view.read_for_callback(owner, expected).await }
        })
    }

    fn is_source_projection(&self, path: &MountPath) -> Result<bool, MountSourceError> {
        let text = self.path(path)?;
        let owner = SourceViewGate::callback_owner();
        self.wait(|| async move {
            if self.authored.lookup_async(path, owner, &mut super::adapter::NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(crate::performance::BOUNDARY_BUDGET, self.authored.cancellation()))).await?.is_some() {
                return Ok(false);
            }
            if self.is_removed(&text)? {
                return Err(MountSourceError::NotFound);
            }
            match self.lazy.inspect_unauthored(&text, InspectBasis::default(),
                &LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await {
                Ok(LazyReadonlyNode::Portable((LazyLookup::Source(_), _))) => Ok(true),
                Ok(_) => Err(MountSourceError::Unsupported(std::borrow::Cow::Owned("named attributes of an unpromoted shadow are not projected".to_owned(),))),
                Err(error) => Err(lazy_error(error)),
            }
        })
    }

    fn promote_locked(&self, path: &MountPath) -> Result<(), MountSourceError> {
        if self.authored.lookup(path)?.is_some() {
            return Ok(());
        }
        if self.is_removed(&self.path(path)?)? {
            return Err(MountSourceError::NotFound);
        }
        self.wait(|| self.promote(path))
    }

    fn read_content(
        &self,
        path: &MountPath,
        pin: Option<MountContentPin>,
        offset: u64,
        length: u64,
        piece: u32,
        sink: &mut ContentSink<'_>,
    ) -> Result<(), MountSourceError> {
        let mut work = crate::demand::DemandWork::new(
            crate::WorkBudget::UNBOUNDED, self.authored.cancellation(),
        );
        self.read_content_with_carrier(path, pin, offset, length, piece,
            &mut |offset, output| sink(offset, output.bytes),
            &mut super::adapter::NativeDemandCarrier::Borrowed(&mut work))
    }

    fn read_content_with_carrier(
        &self,
        path: &MountPath,
        pin: Option<MountContentPin>,
        offset: u64,
        length: u64,
        piece: u32,
        sink: &mut super::NativeMountContentSink<'_>,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        with_native_callback_carrier(&self.runtime, carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let owner = SourceViewGate::callback_owner();
                let _lease = self.source_read_lease_with_carrier(owner, None, carrier).await?;
                let text = mount_path_text_with_carrier(&self.root, path, carrier)?;
                let result = self.read_resolved_content_with_carrier(
                    path, &text, owner, pin, offset, length, piece, sink, carrier,
                ).await;
                let retained = text.legacy_owned_bytes();
                drop(text);
                carrier.with_work(|work| work.release(retained)).map_err(mount_demand_error)?;
                result
            })
        })
    }

    async fn content_detached_with_carrier(
        &self,
        resolution: &Resolution,
        text: &str,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<Option<Arc<CheckoutDetachedFile<A, O>>>, MountSourceError> {
        carrier.with_work(|work| work.check()).map_err(mount_demand_error)?;
        if self.detached.lock().map_err(|_| MountSourceError::Stale)?.is_empty() {
            return Ok(None);
        }
        let file_id = match resolution {
            Resolution::Absent => return Ok(None),
            Resolution::Authored(lookup) => lookup.node.file_id,
            Resolution::Unauthored(lookup, _, _) =>
                self.mutation_file_id_with_carrier(text, lookup, carrier).await?,
        };
        carrier.with_work(|work| self.detached_by_id_with_work(file_id, work))
    }

    async fn open_resolved_source_with_carrier(
        &self,
        text: &str,
        resolution: &Resolution,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<Option<Box<dyn DemandFile>>, MountSourceError> {
        let Resolution::Unauthored(LazyLookup::Source(node), Some(source), _) = resolution else {
            return Ok(None);
        };
        if node.kind != SourceNodeKind::RegularFile { return Ok(None); }
        let caller = carrier.cancellation();
        if let Some(original) = carrier.original_native_work() {
            return match self.lazy.open_source_file_with_native_work(
                text, *source, *node, &original, &caller,
            ).await {
                Ok((file, request)) => {
                    drop(request);
                    Ok(Some(file))
                }
                Err((error, request)) => {
                    let cause = native_lazy_error(error);
                    drop(request);
                    Err(cause)
                }
            };
        }
        let budget = carrier.with_work(|work| work.remaining()).map_err(mount_demand_error)?;
        let opened = self.lazy.open_source_file_measured(text, *source, *node, budget, &caller).await;
        let file = carrier.with_work(|work| mount_accept(work, opened, lazy_error))?;
        let retained = file.logical_owned_bytes();
        if let Err(error) = carrier.with_work(|work| work.record_adoption(retained, &None)) {
            drop(file);
            carrier.with_work(|work| work.release(retained)).map_err(mount_demand_error)?;
            return Err(mount_demand_error(error));
        }
        Ok(Some(file))
    }

    async fn resolve_opened_with_carrier(
        &self,
        path: &MountPath,
        text: &str,
        owner: super::view_gate::CallbackOwner,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<(Resolution, Option<Box<dyn DemandFile>>), MountSourceError> {
        let resolution = self.resolve(path, text, owner, carrier).await?;
        let retained = match &resolution {
            Resolution::Unauthored(lookup, _, allocation) =>
                if allocation.is_none() { lookup.logical_owned_bytes() } else { 0 },
            Resolution::Authored(_) | Resolution::Absent => 0,
        };
        if let Err(error) = carrier.with_work(|work| work.record_adoption(retained, &None)) {
            drop(resolution);
            carrier.with_work(|work| work.release(retained)).map_err(mount_demand_error)?;
            return Err(mount_demand_error(error));
        }
        match self.open_resolved_source_with_carrier(text, &resolution, carrier).await {
            Ok(file) => Ok((resolution, file)),
            Err(error) => {
                drop(resolution);
                carrier.with_work(|work| work.release(retained)).map_err(mount_demand_error)?;
                Err(error)
            }
        }
    }

    async fn read_resolved_content_with_carrier(
        &self,
        path: &MountPath,
        text: &str,
        owner: super::view_gate::CallbackOwner,
        pin: Option<MountContentPin>,
        offset: u64,
        length: u64,
        piece: u32,
        sink: &mut super::NativeMountContentSink<'_>,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        let resolution = self.resolve(path, text, owner, carrier).await?;
        let lookup_bytes = match &resolution {
            Resolution::Unauthored(lookup, _, allocation) =>
                if allocation.is_none() { lookup.logical_owned_bytes() } else { 0 },
            Resolution::Authored(_) | Resolution::Absent => 0,
        };
        if let Err(error) = carrier.with_work(|work| work.record_adoption(lookup_bytes, &None)) {
            drop(resolution);
            carrier.with_work(|work| work.release(lookup_bytes)).map_err(mount_demand_error)?;
            return Err(mount_demand_error(error));
        }
        let result = async {
            if matches!(resolution, Resolution::Absent) { return Err(MountSourceError::NotFound); }
            let detached = self.content_detached_with_carrier(&resolution, text, carrier).await?;
            if detached.is_none()
                && let Resolution::Unauthored(LazyLookup::Source(node), Some(source), _) = &resolution
                && let Some(pin) = pin
                && pin != carrier.with_work(|work| source_content_pin_with_work(*source, node, work))?
            {
                return Err(MountSourceError::Stale);
            }
            let source_file = if detached.is_none() {
                self.open_resolved_source_with_carrier(text, &resolution, carrier).await?
            } else { None };
            let result = async {
                let Some(end) = offset.checked_add(length) else {
                    return Err(mount_content_invalid_with_carrier(carrier, "content range overflows")?);
                };
                let mut next = offset;
                while next < end {
                    carrier.with_work(|work| work.check()).map_err(mount_demand_error)?;
                    self.authored.cancellation().check().map_err(|_| MountSourceError::Cancelled)?;
                    let wanted = u32::try_from(end - next).unwrap_or(u32::MAX).min(piece.max(1));
                    let output = if let Some(file) = &detached {
                        file.read_range_owned_async_with_carrier(next, wanted, carrier).await?
                    } else if let Some(file) = &source_file {
                        let caller = carrier.cancellation();
                        if let Some(original) = carrier.original_native_work() {
                            crate::demand::read_file(file.as_ref(), next, u64::from(wanted), &original, &caller).map_err(mount_demand_error)?
                        } else {
                            let budget = carrier.with_work(|work| work.remaining()).map_err(mount_demand_error)?;
                            let read = crate::demand::read_file(file.as_ref(), next, u64::from(wanted), budget, &caller);
                            let output = carrier.with_work(|work| mount_accept(work, read, mount_demand_error))?;
                            let retained = output.logical_owned_bytes;
                            if let Err(error) = carrier.with_work(|work| work.record_adoption(retained, &None)) {
                                drop(output);
                                carrier.with_work(|work| work.release(retained)).map_err(mount_demand_error)?;
                                return Err(mount_demand_error(error));
                            }
                            output
                        }
                    } else {
                        match &resolution {
                            Resolution::Authored(_) => self.authored.read_bytes(path, Some(crate::ByteRange { offset: next, length: u64::from(wanted) }), owner, carrier).await?,
                            Resolution::Unauthored(lookup, source, _) => {
                                let caller = carrier.cancellation();
                                if let Some(original) = carrier.original_native_work() {
                                    let read = self.lazy.read_unauthored(
                                        lookup, *source, text, Some(crate::ByteRange { offset: next, length: u64::from(wanted) }),
                                        u64::MAX, &LazyTreapWork::native_with_work(&original, &caller).map_err(native_lazy_error)?,
                                    ).await;
                                    accept_native_content(read)?
                                } else {
                                    let budget = carrier.with_work(|work| work.remaining()).map_err(mount_demand_error)?;
                                    let accounting = LazyTreapWork::new(budget, &caller);
                                    let read = self.lazy.read_unauthored(lookup, *source, text,
                                        Some(crate::ByteRange { offset: next, length: u64::from(wanted) }),
                                        u64::MAX, &accounting).await.map_err(|(error, _)| error);
                                    let read = accounting.measured(read, |error, work| error.with_prior_work(work).into_measured_failure());
                                    let output = carrier.with_work(|work| mount_accept(work, read, lazy_error))?;
                                    let retained = output.logical_owned_bytes;
                                    if let Err(error) = carrier.with_work(|work| work.record_adoption(retained, &None)) {
                                        drop(output);
                                        carrier.with_work(|work| work.release(retained)).map_err(mount_demand_error)?;
                                        return Err(mount_demand_error(error));
                                    }
                                    output
                                }
                            }
                            Resolution::Absent => return Err(MountSourceError::NotFound),
                        }
                    };
                    let read_length = output.bytes.len() as u64;
                    if read_length == 0 || read_length > u64::from(wanted) {
                        let retained = if output.is_intrinsic() { 0 } else { output.logical_owned_bytes };
                        drop(output);
                        carrier.with_work(|work| work.release(retained)).map_err(mount_demand_error)?;
                        if read_length == 0 { break; }
                        return Err(mount_content_invalid_with_carrier(carrier, "source returned more than was asked")?);
                    }
                    sink(next, output)?;
                    next += read_length;
                    if read_length < u64::from(wanted) { break; }
                }
                Ok(())
            }.await;
            if let Some(file) = source_file {
                let retained = if carrier.original_native_work().is_some() { 0 } else { file.logical_owned_bytes() };
                drop(file);
                carrier.with_work(|work| work.release(retained)).map_err(mount_demand_error)?;
            }
            result
        }.await;
        drop(resolution);
        carrier.with_work(|work| work.release(lookup_bytes)).map_err(mount_demand_error)?;
        result
    }

    async fn read_link_async(
        &self,
        path: &MountPath,
        original: &crate::NativeMaterializationWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        let mut carrier = super::adapter::NativeDemandCarrier::Native(original);
        let carrier = &mut carrier;
        let owner = SourceViewGate::callback_owner();
        let _lease = self.source_read_lease_with_carrier(owner, None, carrier).await?;
        let text = mount_path_text_with_carrier(&self.root, path, carrier)?;
        match self.resolve(path, &text, owner, carrier).await? {
            Resolution::Absent => Err(MountSourceError::NotFound),
            Resolution::Authored(_) => self.authored.read_bytes(path, None, owner, carrier).await,
            Resolution::Unauthored(lookup, source, allocation) => {
                let lookup = MountOwnedBuffer { legacy_bytes: lookup.logical_owned_bytes(), value: lookup, allocation };
                let maximum = original.remaining().map_err(MountSourceError::NativeWork)?.output_bytes;
                accept_native_content(self.lazy.read_unauthored(
                    &lookup.value, source, &text, None, maximum,
                    &LazyTreapWork::native_with_work(original, original.cancellation()).map_err(native_lazy_error)?,
                ).await)
            }
        }
    }
}

fn source_content_pin(source: SourceReference, node: &SourceNode) -> MountContentPin {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-fs-lazy-content-pin-v1\0");
    hasher.update(&source.identity);
    hasher.update(&node.file_identity);
    hasher.update(&node.version.0);
    MountContentPin(*hasher.finalize().as_bytes())
}

fn mount_content_invalid(
    work: &mut crate::demand::DemandWork,
    message: &'static str,
) -> Result<MountSourceError, MountSourceError> {
    let bytes = message.len() as u64;
    let copied = crate::WorkCounters { bytes_copied: bytes, ..crate::WorkCounters::default() };
    work.work.admit(&copied, &work.budget).map_err(MountSourceError::Work)?;
    work.allocate(bytes).map_err(mount_demand_error)?;
    let message = message.to_owned();
    if let Err(error) = work.record(copied) {
        drop(message);
        work.release(bytes).map_err(mount_demand_error)?;
        return Err(mount_demand_error(error));
    }
    Ok(MountSourceError::Invalid(std::borrow::Cow::Owned(message)))
}

fn mount_content_invalid_with_carrier(
    carrier: &mut super::adapter::NativeDemandCarrier<'_>, message: &'static str,
) -> Result<MountSourceError, MountSourceError> {
    if carrier.original_native_work().is_some() {
        Ok(MountSourceError::Invalid(std::borrow::Cow::Borrowed(message)))
    } else {
        carrier.with_work(|work| mount_content_invalid(work, message))
    }
}

fn source_content_pin_with_work(
    source: SourceReference,
    node: &SourceNode,
    work: &mut crate::demand::DemandWork,
) -> Result<MountContentPin, MountSourceError> {
    work.charge(crate::WorkCounters {
        bytes_hashed: (b"acyclic-fs-lazy-content-pin-v1\0".len()
            + source.identity.len() + node.file_identity.len() + node.version.0.len()) as u64,
        ..crate::WorkCounters::default()
    }).map_err(mount_demand_error)?;
    Ok(source_content_pin(source, node))
}

fn mount_lookup(lookup: LazyLookup, file_id: FileId) -> MountLookup {
    mount_lookup_ref(&lookup, file_id)
}

fn mount_lookup_ref(lookup: &LazyLookup, file_id: FileId) -> MountLookup {
    match lookup {
        LazyLookup::Authored { stat, .. } => MountLookup {
            node: MountNode {
                file_id,
                kind: file_kind(stat.kind),
                logical_bytes: stat.logical_bytes.unwrap_or(0),
                link_count: stat.link_count,
                device: None,
            },
            metadata: workspace_metadata(stat.metadata),
        },
        LazyLookup::Shadow {
            record,
            metadata,
            source_link_count,
        } => MountLookup {
            node: MountNode {
                file_id,
                kind: file_kind(record.kind),
                logical_bytes: record_logical_bytes(record.payload),
                link_count: *source_link_count,
                device: match record.payload {
                    FilePayload::Device { major, minor } => Some((major, minor)),
                    _ => None,
                },
            },
            metadata: workspace_metadata(*metadata),
        },
        LazyLookup::Source(node) => MountLookup {
            node: MountNode {
                file_id,
                kind: source_kind(node.kind),
                logical_bytes: node.logical_bytes.unwrap_or(0),
                link_count: node.link_count.unwrap_or(1),
                device: node.device,
            },
            metadata: source_metadata(node.metadata),
        },
    }
}

const fn source_kind(kind: SourceNodeKind) -> MountNodeKind {
    match kind {
        SourceNodeKind::RegularFile => MountNodeKind::Regular,
        SourceNodeKind::Directory => MountNodeKind::Directory,
        SourceNodeKind::SymbolicLink => MountNodeKind::SymbolicLink,
        SourceNodeKind::Fifo => MountNodeKind::Fifo,
        SourceNodeKind::Socket => MountNodeKind::Socket,
        SourceNodeKind::CharacterDevice => MountNodeKind::CharacterDevice,
        SourceNodeKind::BlockDevice => MountNodeKind::BlockDevice,
        SourceNodeKind::Unsupported => MountNodeKind::Unsupported,
    }
}

fn record_logical_bytes(payload: FilePayload) -> u64 {
    match payload {
        FilePayload::InlineRegular(bytes) => {
            u64::try_from(bytes.as_bytes().len()).unwrap_or(u64::MAX)
        }
        FilePayload::Regular { logical_bytes, .. }
        | FilePayload::SymbolicLink {
            target_bytes: logical_bytes,
            ..
        }
        | FilePayload::ReparsePoint {
            payload_bytes: logical_bytes,
            ..
        } => logical_bytes,
        FilePayload::Directory { .. } | FilePayload::Empty | FilePayload::Device { .. } => 0,
    }
}

const fn file_kind(kind: FileKind) -> MountNodeKind {
    match kind {
        FileKind::Regular => MountNodeKind::Regular,
        FileKind::Directory => MountNodeKind::Directory,
        FileKind::SymbolicLink => MountNodeKind::SymbolicLink,
        FileKind::Fifo => MountNodeKind::Fifo,
        FileKind::Socket => MountNodeKind::Socket,
        FileKind::CharacterDevice => MountNodeKind::CharacterDevice,
        FileKind::BlockDevice => MountNodeKind::BlockDevice,
        FileKind::ReparsePoint | FileKind::MountBoundary => MountNodeKind::Unsupported,
    }
}

fn workspace_metadata(metadata: WorkspaceMetadata) -> FileMetadata {
    FileMetadata {
        posix_mode: field(metadata.posix_mode),
        posix_uid: field(metadata.posix_uid),
        posix_gid: field(metadata.posix_gid),
        posix_flags: field(metadata.posix_flags),
        windows_attributes: field(metadata.windows_attributes),
        created_ns: field(metadata.created_ns),
        modified_ns: field(metadata.modified_ns),
        accessed_ns: field(metadata.accessed_ns),
        changed_ns: field(metadata.changed_ns),
        ..FileMetadata::default()
    }
}

fn source_metadata(metadata: crate::demand::SourceMetadata) -> FileMetadata {
    FileMetadata {
        posix_mode: field(metadata.posix_mode),
        posix_uid: field(metadata.posix_uid),
        posix_gid: field(metadata.posix_gid),
        posix_flags: field(metadata.posix_flags),
        windows_attributes: field(metadata.windows_attributes),
        created_ns: field(
            metadata
                .created_ns
                .and_then(|value| i64::try_from(value).ok()),
        ),
        modified_ns: field(
            metadata
                .modified_ns
                .and_then(|value| i64::try_from(value).ok()),
        ),
        accessed_ns: field(
            metadata
                .accessed_ns
                .and_then(|value| i64::try_from(value).ok()),
        ),
        changed_ns: field(
            metadata
                .changed_ns
                .and_then(|value| i64::try_from(value).ok()),
        ),
        ..FileMetadata::default()
    }
}

fn field<T>(value: Option<T>) -> MetadataField<T> {
    value.map_or(MetadataField::Unavailable, MetadataField::Value)
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "used as a map_err adapter, which hands over the error by value"
)]
fn lazy_error(error: LazyWorkspaceError) -> MountSourceError {
    let error = match error {
        LazyWorkspaceError::SpentWork(failure) => return lazy_error(failure.error),
        LazyWorkspaceError::OriginalSnapshotUnavailable =>
            return MountSourceError::OriginalSnapshotUnavailable,
        LazyWorkspaceError::SelectedPublication(outcome) =>
            return MountSourceError::SelectedPublication(outcome),
        LazyWorkspaceError::Demand(DemandError::Mount(error)) => return error,
        LazyWorkspaceError::Demand(DemandError::Io(error)) => return MountSourceError::Io(error, None),
        LazyWorkspaceError::NativeWork(error) => return MountSourceError::NativeWork(error),
        LazyWorkspaceError::NativeRecordIo(error, allocation) =>
            return MountSourceError::Io(error, allocation),
        LazyWorkspaceError::NativeRecordJson(error, allocation) =>
            return MountSourceError::NativeRecordJson(error, allocation),
        LazyWorkspaceError::NativeRecordLockTimeout(path, allocation) =>
            return MountSourceError::NativeRecordLockTimeout(path, allocation),
        LazyWorkspaceError::NativeRecordOwnershipConflict(path, allocation) =>
            return MountSourceError::NativeRecordOwnershipConflict(path, allocation),
        LazyWorkspaceError::NativeRecordMaterializationInProgress =>
            return MountSourceError::Indeterminate,
        LazyWorkspaceError::NativeRecordIntegrity => return MountSourceError::Engine(std::borrow::Cow::Borrowed("native record integrity verification failed")),
        LazyWorkspaceError::NativeRecordContextTransaction(message, allocation) =>
            return MountSourceError::NativeRecordContextTransaction(message, allocation),
        LazyWorkspaceError::NativeRecordCorrupt(path, allocation) =>
            return MountSourceError::NativeRecordCorrupt(path, allocation),
        LazyWorkspaceError::NativeLedgerMismatch => return MountSourceError::NativeWork(
            crate::materializer::NativeTreeMaterializationError::InvalidLayout,
        ),
        LazyWorkspaceError::NativeUnsupported => return MountSourceError::NativeUnsupported,
        LazyWorkspaceError::NativeConstruction { cause, .. } =>
            return MountSourceError::NativeWork(cause),
        LazyWorkspaceError::Filesystem(error) => {
            return mount_bounded_error(&error)
                .unwrap_or(MountSourceError::Filesystem(error));
        }
        LazyWorkspaceError::NamespacePath(crate::kernel::NamespacePathError::Work(error)) =>
            return MountSourceError::Work(error),
        LazyWorkspaceError::NamespacePath(crate::kernel::NamespacePathError::Cancelled) =>
            return MountSourceError::Cancelled,
        LazyWorkspaceError::NamespacePath(error) => return MountSourceError::NamespacePath(error),
        LazyWorkspaceError::WorkspaceCause(error) => return MountSourceError::WorkspaceCause(error),
        LazyWorkspaceError::WorkspacePrefixFailure { source, accounting } =>
            return MountSourceError::WorkspacePrefixFailure { source, accounting },
        LazyWorkspaceError::NativeWorkspaceCause(cause) =>
            return MountSourceError::NativeWorkspaceCause(cause),
        LazyWorkspaceError::Workspace(message) | LazyWorkspaceError::Store(message)
            | LazyWorkspaceError::Work(message) => return MountSourceError::Engine(std::borrow::Cow::Owned(message)),
        error => error,
    };
    match error.semantic_error() {
        LazyWorkspaceError::NotFound => MountSourceError::NotFound,
        LazyWorkspaceError::Cancelled | LazyWorkspaceError::Demand(DemandError::Cancelled) =>
            MountSourceError::Cancelled,
        LazyWorkspaceError::Demand(DemandError::Work(error)) => MountSourceError::Work(*error),
        LazyWorkspaceError::NotRegularFile | LazyWorkspaceError::InvalidPageBound => {
            MountSourceError::Invalid(std::borrow::Cow::Owned(error.to_string()))
        }
        LazyWorkspaceError::UnsupportedNode => {
            MountSourceError::Unsupported(std::borrow::Cow::Owned(error.to_string()))
        }
        // A held source file that changed since it opened is a stale handle.
        LazyWorkspaceError::StaleSource
        | LazyWorkspaceError::StaleCursor
        | LazyWorkspaceError::StaleIdentity
        | LazyWorkspaceError::Concurrent
        | LazyWorkspaceError::Demand(DemandError::StaleVersion) => MountSourceError::Stale,
        _ => MountSourceError::Engine(std::borrow::Cow::Owned(error.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listed_names_are_decoded_by_their_declared_encoding()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::kernel::{LogicalName, NameEncoding};

        let text = "dir-\u{e9}";
        let utf16 = text
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();
        let windows = LogicalName::new(NameEncoding::WindowsUtf16Le, utf16.clone(), 510)?;
        let portable = LogicalName::new(NameEncoding::Utf8, text.as_bytes().to_vec(), 255)?;
        if cfg!(windows) {
            for name in [&windows, &portable] {
                assert_eq!(super::super::adapter::native_mount_name(name, &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())))?.0, utf16);
            }
        } else {
            assert_eq!(
                super::super::adapter::native_mount_name(&portable, &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())))?.0,
                text.as_bytes()
            );
        }
        Ok(())
    }

    #[test]
    fn rebound_prunes_only_unreferenced_removed_identities()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::kernel::InlineFileData;
        use crate::{Digest, ObjectId, ObjectKind};

        let first = FileId::from_bytes([1; 16]);
        let second = FileId::from_bytes([2; 16]);
        let payload = InlineFileData::new(b"x")?;
        let record = |file_id| {
            (
                FileRecord {
                    file_id,
                    kind: FileKind::Regular,
                    link_count: 1,
                    metadata: ObjectId {
                        kind: ObjectKind::Metadata,
                        digest: Digest::from_bytes([0; 32]),
                    },
                    payload: FilePayload::InlineRegular(payload),
                },
                FileMetadata::default(),
            )
        };
        let mut removals = MountedRemovals {
            paths: BTreeMap::from([
                (MountedRemovalPath::unmeasured("/a".to_owned()), MountedRemoval { file_id: first, operation: None }),
                (MountedRemovalPath::unmeasured("/b".to_owned()), MountedRemoval { file_id: first, operation: None }),
                (MountedRemovalPath::unmeasured("/c".to_owned()), MountedRemoval { file_id: second, operation: None }),
            ]),
            records: BTreeMap::from([
                (first, CapturedRemovalRecord { value: record(first), paths: 2, custody: None, bytes: 0 }),
                (second, CapturedRemovalRecord { value: record(second), paths: 1, custody: None, bytes: 0 }),
            ]),
            publication: None,
            selected_inputs: None,
            epoch: 0,
        };
        assert!(removals.rebound("/a"));
        assert!(removals.records.contains_key(&first));
        assert!(removals.rebound("/b"));
        assert!(!removals.records.contains_key(&first));
        assert!(removals.records.contains_key(&second));
        assert!(removals.rebound("/c"));
        assert!(removals.records.is_empty());
        assert!(!removals.rebound("/c"));
        assert_eq!(removals.epoch, 3);
        Ok(())
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn rebound_does_not_publish_a_stale_removed_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{CheckoutMode, FilesystemProfile, GenerationSelector, VolumeLimits};
        use crate::native_mount::{MountPublication, SharedCheckout};
        use crate::{Fs, MemoryLazyWorkspaceStore};

        let source_root = tempfile::tempdir()?;
        std::fs::write(source_root.path().join("a"), b"old-a")?;
        std::fs::write(source_root.path().join("b"), b"old-b")?;
        let demand = Arc::new(
            NativeDemandSource::open(
                std::borrow::Cow::Owned(std::path::Path::new(source_root.path()).to_path_buf()),
                FilesystemProfile::Portable,
                VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
            )
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        );
        let fs = Fs::memory();
        let lazy = Arc::new(
            LazyWorkspace::attach(
                &fs,
                "mounted-rebound-identity",
                demand,
                MemoryLazyWorkspaceStore::default(),
            )
            .await?,
        );
        let checkout = lazy
            .workspace()
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let config = checkout.volume_config();
        let authored = Arc::new(CheckoutMountSource::new(
            Arc::new(SharedCheckout::with_publication(
                checkout,
                MountPublication::Manual,
            )),
            config,
        )?);
        let view = { CallbackRuntime::create()?; let cancellation = authored.cancellation().clone(); let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation); LazyMountSource::new(Arc::clone(&lazy), authored, "/".to_owned(), super::super::adapter::NativeDemandCarrier::Borrowed(&mut work), None) }?;
        let path = |name: &str| {
            MountPath::root().child(if cfg!(windows) {
                name.encode_utf16().flat_map(u16::to_le_bytes).collect()
            } else {
                name.as_bytes().to_vec()
            })
        };
        let a = path("a");
        let b = path("b");
        let _open_a = view.open_file(&a)?;
        let _open_b = view.open_file(&b)?;
        let old_a = view.lookup(&a)?.expect("source a").node.file_id;
        let old_b = view.lookup(&b)?.expect("source b").node.file_id;
        crate::native_mount::adapter::boundary(view.authored.cancellation(), |work| view.remove(&a, Some(old_a), work))?;
        view.native_call(|work| view.create_file(&a, FileMetadata::default(), None, work))?;
        view.write_range(&a, 0, Bytes::from_static(b"new-a"))?;
        crate::native_mount::adapter::boundary(view.authored.cancellation(), |work| view.remove(&b, Some(old_b), work))?;
        {
            let removals = view.removals.lock().map_err(|_| "poisoned removals")?;
            assert_eq!(removals.paths.len(), 1);
            assert_eq!(removals.records.len(), 1);
            assert!(removals.records.contains_key(&old_b));
        }
        view.sync_async().await?;
        assert_eq!(lazy.read_range("/a", 0, 5).await?.as_ref(), b"new-a");
        assert!(matches!(
            lazy.lookup("/b").await,
            Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::NotFound)
        ));
        Ok(())
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[derive(Default)]
    struct Recorded(Mutex<Vec<ViewStamp>>);

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    impl ViewObserver for Recorded {
        fn view_changed(&self, position: ViewStamp, _origin: crate::native_mount::ViewOrigin) {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(position);
        }
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    async fn recorded_view(
        source_root: &Path,
    ) -> Result<
        (
            Arc<crate::demand::native::NativeDemandSource>,
            LazyMountSource<
                crate::facade::MemoryAuthorityBackend,
                crate::facade::MemoryObjectBackend,
                crate::demand::native::NativeDemandSource,
                crate::MemoryLazyWorkspaceStore,
            >,
            Arc<Recorded>,
        ),
        Box<dyn std::error::Error>,
    > {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{CheckoutMode, FilesystemProfile, GenerationSelector, VolumeLimits};
        use crate::native_mount::{MountPublication, SharedCheckout};
        use crate::{Fs, MemoryLazyWorkspaceStore};

        std::fs::write(source_root.join("source-only"), b"source")?;
        let demand = Arc::new(
            NativeDemandSource::open(
                std::borrow::Cow::Owned(std::path::Path::new(source_root).to_path_buf()),
                FilesystemProfile::Portable,
                VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
            )
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        );
        let fs = Fs::memory();
        let lazy = Arc::new(
            LazyWorkspace::attach(
                &fs,
                "recorded",
                Arc::clone(&demand),
                MemoryLazyWorkspaceStore::default(),
            )
            .await?,
        );
        let checkout = lazy
            .workspace()
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let config = checkout.volume_config();
        let authored = Arc::new(CheckoutMountSource::new(
            Arc::new(SharedCheckout::with_publication(
                checkout,
                MountPublication::Manual,
            )),
            config,
        )?);
        let view = { CallbackRuntime::create()?; let cancellation = authored.cancellation().clone(); let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation); LazyMountSource::new(lazy, authored, "/".to_owned(), super::super::adapter::NativeDemandCarrier::Borrowed(&mut work), None) }?;
        let recorded = Arc::new(Recorded::default());
        let observer: Weak<Recorded> = Arc::downgrade(&recorded);
        view.observe_view(observer);
        Ok((demand, view, recorded))
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn bounded_mount_owner_guard_preserves_each_original_and_fresh_candidate()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let (_, view, _) = recorded_view(root.path()).await?;
        let view = Arc::new(view);
        let path = mounted(&["source-only"]);
        let held = view.open_file(&path)?;
        let identity = held.lookup()?.node.file_id;
        crate::native_mount::adapter::boundary(view.authored.cancellation(), |work| view.remove(&path, Some(identity), work))?;
        held.write_range(0, Bytes::from_static(b"epoch-one"))?;
        let cancellation = crate::CancellationToken::new();
        let inspections = Arc::new(std::sync::Mutex::new(Vec::new()));
        let inspect_owner = {
            let view = Arc::clone(&view);
            let inspections = Arc::clone(&inspections);
            move |operation: crate::OperationId, original: crate::NativeMaterializationWork, deny: bool| {
                let view = Arc::clone(&view);
                let inspections = Arc::clone(&inspections);
                async move {
                    let allowance = original.remaining().map_err(MountSourceError::NativeWork)?;
                    let prefix = original.receipt();
                    let workspace = view.lazy.workspace();
                    let inspected = ({ let __authority_args = (workspace.authority_id(), operation, &original, original.cancellation(),); let mut __authority_work = crate::AuthorityWork::native(__authority_args.2, __authority_args.3); let __authority_result = workspace.volume.fs.authority().publication_reservation(__authority_args.0, __authority_args.1, &mut __authority_work).await; __authority_result }).map_err(|error| MountSourceError::Filesystem(crate::FsError::Authority(error)))?;
                    assert!(inspected.is_none(), "unrestricted filesystem permit is not task authority");
                    let completed = original.receipt();
                    assert!(completed.authority_records_read > prefix.authority_records_read);
                    inspections.lock().expect("owner inspection evidence").push((
                        operation, allowance, completed.authority_records_read - prefix.authority_records_read,
                        completed.backend_read_operations - prefix.backend_read_operations,
                    ));
                    if deny { Err(MountSourceError::Filesystem(crate::FsError::MutationNotAllowed)) }
                    else { Ok(()) }
                }
            }
        };
        let rejected = Arc::new(std::sync::Mutex::new(Vec::new()));
        let failure = view.sync_async_with_permit_measured_guarded(
            crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &cancellation,
            {
                let view = Arc::clone(&view);
                let rejected = Arc::clone(&rejected);
                let inspect_owner = inspect_owner.clone();
                move |original: &crate::NativeMaterializationWork| {
                    let operation = view.removals.lock().expect("original custody").publication
                        .expect("original prepared publication").operation;
                    rejected.lock().expect("rejected operations").push(operation);
                    inspect_owner(operation, original.clone(), true)
                }
            },
        ).await.expect_err("reject after real journal preparation, before the seal");
        assert!(matches!(failure.error, (MountSourceError::Filesystem(crate::FsError::MutationNotAllowed), None)));
        assert!(failure.work.items_examined > 0);
        assert_eq!(failure.work.authority_records_appended, 0);
        let rejected = rejected.lock().map_err(|_| "rejected operations poisoned")?;
        assert_eq!(rejected.len(), 1);
        let original = rejected[0];
        drop(rejected);
        let rejected_prefix = inspections.lock().expect("owner inspection evidence")[0];
        assert!(failure.work.authority_records_read >= rejected_prefix.2);
        assert!(failure.work.backend_read_operations >= rejected_prefix.3);
        assert_eq!((async { let cancellation = &crate::CancellationToken::new(); let accounting = LazyTreapWork::new(crate::WorkBudget::UNBOUNDED, cancellation); let result = view.lazy.mount_removal_publication(&accounting).await; accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, Some(false));
        assert_eq!(view.authored.shared_checkout().lock().await.retained_operation_id(), original);
        let mut snapshot_work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
        let snapshot_failure = view.snapshot(
            super::super::adapter::NativeDemandCarrier::Borrowed(&mut snapshot_work), &cancellation,
        ).await.expect_err("an original prepared mounted operation is not a clean checkpoint");
        assert!(matches!(snapshot_failure, MountSourceError::Stale));
        assert!(snapshot_work.work.items_examined > 0);
        held.write_range(0, Bytes::from_static(b"epoch-two"))?;
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let failure = view.sync_async_with_permit_measured_guarded(
            crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &cancellation,
            {
                let view = Arc::clone(&view);
                let observed = Arc::clone(&observed);
                let inspect_owner = inspect_owner.clone();
                move |original: &crate::NativeMaterializationWork| {
                    let operation = view.removals.lock().expect("original custody").publication
                        .expect("original prepared publication").operation;
                    let deny = {
                        let mut observed = observed.lock().expect("observed operations");
                        observed.push(operation);
                        observed.len() != 1
                    };
                    inspect_owner(operation, original.clone(), deny)
                }
            },
        ).await.expect_err("fresh detached epoch requires another original-owner check");
        assert!(matches!(failure.error, (MountSourceError::Filesystem(crate::FsError::MutationNotAllowed), None)));
        assert!(failure.work.authority_records_appended > 0);
        let observed = observed.lock().map_err(|_| "observed operations poisoned")?;
        assert_eq!(observed.len(), 2);
        assert_eq!(observed[0], original);
        assert_ne!(observed[1], original);
        let next = observed[1];
        drop(observed);
        let evidence = inspections.lock().expect("owner inspection evidence");
        assert!(failure.work.authority_records_read >= evidence[1].2 + evidence[2].2);
        assert!(failure.work.backend_read_operations >= evidence[1].3 + evidence[2].3);
        drop(evidence);
        assert!(view.lazy.workspace().operation_generation(IdempotencyKey::from_bytes(original.into_bytes()), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.is_some());
        assert_eq!((async { let cancellation = &crate::CancellationToken::new(); let accounting = LazyTreapWork::new(crate::WorkBudget::UNBOUNDED, cancellation); let result = view.lazy.mount_removal_publication(&accounting).await; accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, Some(false));
        assert_eq!(view.authored.shared_checkout().lock().await.retained_operation_id(), next);
        {
            let identities = view.detached.lock().map_err(|_| "poisoned identities")?;
            let pending = detached_entry(&identities, identity).and_then(|entry| entry.pending.as_ref())
                .ok_or("missing original fresh-epoch custody")?;
            assert_eq!(pending.operation, Some(next));
        }
        let checks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let completed = view.sync_async_with_permit_measured_guarded(
            crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &cancellation,
            {
                let view = Arc::clone(&view);
                let checks = Arc::clone(&checks);
                move |original: &crate::NativeMaterializationWork| {
                    checks.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let operation = view.removals.lock().expect("original custody").publication
                        .expect("original prepared publication").operation;
                    assert_eq!(operation, next);
                    inspect_owner(operation, original.clone(), false)
                }
            },
        ).await.expect("the original and fresh prepared publications must both finish without cleanup failure");
        assert!(checks.load(std::sync::atomic::Ordering::Relaxed) > 0);
        assert!(completed.work.authority_records_appended > 0);
        assert!(view.lazy.workspace().operation_generation(IdempotencyKey::from_bytes(next.into_bytes()), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.is_some());
        assert_eq!((async { let cancellation = &crate::CancellationToken::new(); let accounting = LazyTreapWork::new(crate::WorkBudget::UNBOUNDED, cancellation); let result = view.lazy.mount_removal_publication(&accounting).await; accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, None);
        assert_eq!(held.read_range(0, 9)?.as_ref(), b"epoch-two");
        assert_eq!(held.lookup()?.node.file_id, identity);
        assert_eq!(std::fs::read(root.path().join("source-only"))?, b"source");
        let inspections = inspections.lock().map_err(|_| "poisoned inspection evidence")?;
        assert_eq!(inspections[0].0, original);
        assert_eq!(inspections[1].0, original);
        assert_eq!(inspections[2].0, next);
        assert!(inspections[2].1.authority_records_appended < inspections[1].1.authority_records_appended);
        assert!(completed.work.authority_records_read >= inspections[3].2);
        assert!(view.lookup(&path)?.is_none());
        Ok(())
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn bounded_mount_removal_keeps_original_candidate_until_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let (_, view, _) = recorded_view(root.path()).await?;
        let path = mounted(&["source-only"]);
        let open = view.open_file(&path)?;
        crate::native_mount::adapter::boundary(view.authored.cancellation(), |work| view.remove(&path, Some(open.lookup()?.node.file_id), work))?;
        open.write_range(0, Bytes::from_static(b"detached"))?;
        let cancellation = crate::CancellationToken::new();
        let failure = view.sync_async_with_permit(
            crate::PublicationPermit::Unrestricted,
            crate::WorkBudget { authority_records_appended: 0, ..crate::WorkBudget::UNBOUNDED },
            &cancellation,
        ).await.expect_err("the publication append must be denied");
        assert_eq!(failure.work.authority_records_appended, 0);
        assert!(failure.work.items_examined > 0);
        assert!(failure.work.backend_read_operations > 0);
        let operation = view.authored.shared_checkout().lock().await.retained_operation_id();
        assert_eq!((async { let cancellation = &crate::CancellationToken::new(); let accounting = LazyTreapWork::new(crate::WorkBudget::UNBOUNDED, cancellation); let result = view.lazy.mount_removal_publication(&accounting).await; accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, Some(false));
        let stopped = crate::CancellationToken::new();
        stopped.cancel();
        let cancelled = view.sync_async_with_permit(
            crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &stopped,
        ).await.expect_err("cancelled retry must not publish");
        assert!(matches!(cancelled.error, MountSourceError::Cancelled));
        assert_eq!(cancelled.work.authority_records_appended, 0);
        assert_eq!(view.authored.shared_checkout().lock().await.retained_operation_id(), operation);
        let receipt = view.sync_async_with_permit(
            crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &cancellation,
        ).await?;
        assert!(receipt.work.authority_records_appended > 0);
        assert!(view.lazy.workspace().operation_generation(
            IdempotencyKey::from_bytes(operation.into_bytes()),
        crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.is_some());
        assert!((async { let cancellation = &crate::CancellationToken::new(); let accounting = LazyTreapWork::new(crate::WorkBudget::UNBOUNDED, cancellation); let result = view.lazy.mount_removal_publication(&accounting).await; accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.is_none());
        assert!(matches!(view.lazy.lookup("/source-only").await,
            Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::NotFound)));
        assert_eq!(open.read_range(0, 8)?.as_ref(), b"detached");
        Ok(())
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn bounded_mount_failed_journal_cas_pins_the_original_detached_epoch()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::model::{CheckoutMode, FilesystemProfile, GenerationSelector, VolumeLimits};
        use crate::native_mount::{MountPublication, SharedCheckout};
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join("source-only"), b"source")?;
        let demand = Arc::new(crate::demand::native::NativeDemandSource::open(
            std::borrow::Cow::Owned(std::path::Path::new(root.path()).to_path_buf()), FilesystemProfile::Portable, VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
        ).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?);
        let fs = crate::Fs::memory();
        let store = crate::MemoryLazyWorkspaceStore::default();
        let lazy = Arc::new(LazyWorkspace::attach(
            &fs, "cas-custody", demand, store.clone(),
        ).await?);
        let checkout = lazy.workspace().checkout(
            GenerationSelector::Head, CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let config = checkout.volume_config();
        let authored = Arc::new(CheckoutMountSource::new(
            Arc::new(SharedCheckout::with_publication(checkout, MountPublication::Manual)), config,
        )?);
        let view = { CallbackRuntime::create()?; let cancellation = authored.cancellation().clone(); let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation); LazyMountSource::new(lazy, authored, "/".to_owned(), super::super::adapter::NativeDemandCarrier::Borrowed(&mut work), None) }?;
        let path = mounted(&["source-only"]);
        let held = view.open_file(&path)?;
        let identity = held.lookup()?.node.file_id;
        crate::native_mount::adapter::boundary(view.authored.cancellation(), |work| view.remove(&path, Some(identity), work))?;
        held.write_range(0, Bytes::from_static(b"epoch-one"))?;
        let file = view.detached_by_id(identity)?.ok_or("missing detached identity")?;
        let (original_record, _, original_epoch) = file.snapshot(&mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, view.authored.cancellation())), view.authored.cancellation()).await?;
        let cancellation = crate::CancellationToken::new();
        let failure = view.sync_async_with_permit(
            crate::PublicationPermit::Unrestricted,
            crate::WorkBudget { authority_records_appended: 0, ..crate::WorkBudget::UNBOUNDED },
            &cancellation,
        ).await.expect_err("retain the exact unpublished candidate");
        assert!(matches!(failure.error, MountSourceError::Work(_)));
        assert_eq!(failure.work.authority_records_appended, 0);
        let mut prepared = store.load_lazy_workspace(view.lazy.workspace().id()).await?
            .ok_or("missing actual prepared binding")?;
        let recovered = (async { let accounting = LazyTreapWork::new(crate::WorkBudget::UNBOUNDED, &cancellation); let result = view.lazy.finish_mount_removals(&accounting).await; accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }).await?;
        assert!(!recovered.value);
        let current = store.load_lazy_workspace(view.lazy.workspace().id()).await?
            .ok_or("missing recovered binding")?;
        assert!(current.pending_remove.is_none());
        prepared.revision = current.revision + 1;
        let denied = store.compare_and_swap_lazy_workspace_measured(
            view.lazy.workspace().id(), current.revision, prepared,
            crate::WorkBudget { bytes_copied: 0, ..crate::WorkBudget::UNBOUNDED }, &cancellation,
        ).await.expect_err("the original journal CAS state copy must be denied");
        assert!(matches!(denied.semantic_error(), LazyWorkspaceError::Work(_)));
        assert!(denied.spent_work().items_examined > 0);
        assert_eq!(denied.spent_work().bytes_copied, 0);
        assert_eq!((async { let cancellation = &crate::CancellationToken::new(); let accounting = LazyTreapWork::new(crate::WorkBudget::UNBOUNDED, cancellation); let result = view.lazy.mount_removal_publication(&accounting).await; accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, None);
        let original_operation = view.removals.lock().map_err(|_| "poisoned removals")?
            .publication.ok_or("missing original custody")?.operation;
        held.write_range(0, Bytes::from_static(b"epoch-two"))?;
        let (_, _, next_epoch) = file.snapshot(&mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, view.authored.cancellation())), view.authored.cancellation()).await?;
        assert!(next_epoch > original_epoch);
        {
            let identities = view.detached.lock().map_err(|_| "poisoned identities")?;
            let pending = detached_entry(&identities, identity).and_then(|entry| entry.pending.as_ref())
                .ok_or("missing frozen original epoch")?;
            assert_eq!(pending.operation, Some(original_operation));
            assert_eq!(pending.epoch, original_epoch);
            assert_eq!(pending.file.record(), original_record);
        }
        let completed = view.sync_async_with_permit(
            crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &cancellation,
        ).await?;
        assert!(completed.work.authority_records_appended >= 2);
        assert!(view.lazy.workspace().operation_generation(
            IdempotencyKey::from_bytes(original_operation.into_bytes()),
        crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.is_some());
        assert_eq!((async { let cancellation = &crate::CancellationToken::new(); let accounting = LazyTreapWork::new(crate::WorkBudget::UNBOUNDED, cancellation); let result = view.lazy.mount_removal_publication(&accounting).await; accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, None);
        {
            let identities = view.detached.lock().map_err(|_| "poisoned identities")?;
            let entry = detached_entry(&identities, identity).ok_or("missing live identity")?;
            assert_eq!(entry.published_epoch, next_epoch);
            assert!(entry.pending.is_none());
        }
        assert_eq!(held.read_range(0, 9)?.as_ref(), b"epoch-two");
        assert_eq!(held.lookup()?.node.file_id, identity);
        assert_eq!(std::fs::read(root.path().join("source-only"))?, b"source");
        assert!(view.lookup(&path)?.is_none());
        Ok(())
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn bounded_mount_cleanup_after_journal_clear_preserves_new_mutations()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join("next"), b"next")?;
        let (_, view, _) = recorded_view(root.path()).await?;
        let path = mounted(&["source-only"]);
        let next = mounted(&["next"]);
        let held = view.open_file(&path)?;
        let identity = held.lookup()?.node.file_id;
        crate::native_mount::adapter::boundary(view.authored.cancellation(), |work| view.remove(&path, Some(identity), work))?;
        held.write_range(0, Bytes::from_static(b"epoch-one"))?;
        let cancellation = crate::CancellationToken::new();
        view.sync_async_with_permit(
            crate::PublicationPermit::Unrestricted,
            crate::WorkBudget { authority_records_appended: 0, ..crate::WorkBudget::UNBOUNDED },
            &cancellation,
        ).await.expect_err("keep the original operation pending");
        let operation = view.removals.lock().map_err(|_| "poisoned removals")?
            .publication.ok_or("missing original custody")?.operation;
        let mut meter = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
        let mut work = MountSyncWork::new(
            &view.authored, super::super::adapter::NativeDemandCarrier::Borrowed(&mut meter),
        )?;
        work.sync_authored(&view.authored, crate::PublicationPermit::Unrestricted, true, &cancellation).await?;
        view.mark_mount_published(operation)?;
        let remaining = work.remaining().map_err(mount_demand_error)?;
        let result = (async { let accounting = LazyTreapWork::new(remaining, &cancellation); let result = view.lazy.finish_mount_removals(&accounting).await; accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }).await;
        assert!(work.accept(result, lazy_error)?);
        assert_eq!((async { let cancellation = &crate::CancellationToken::new(); let accounting = LazyTreapWork::new(crate::WorkBudget::UNBOUNDED, cancellation); let result = view.lazy.mount_removal_publication(&accounting).await; accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, None);
        let original_generation = view.lazy.workspace().operation_generation(IdempotencyKey::from_bytes(operation.into_bytes()), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.ok_or("original operation was not physically published")?;
        held.write_range(0, Bytes::from_static(b"epoch-two"))?;
        crate::native_mount::adapter::boundary(view.authored.cancellation(), |work| view.remove(&next, None, work))?;
        let before_cleanup = work.with_work(|work| {
            work.budget.items_examined = work.work.items_examined;
            work.work
        });
        assert!(matches!(view.finish_mount_custody(operation, &mut work), Err(MountSourceError::Work(_))));
        assert_eq!(work.with_work(|work| work.work), before_cleanup);
        drop(work);
        {
            let removals = view.removals.lock().map_err(|_| "poisoned removals")?;
            let custody = removals.publication.ok_or("published custody was discarded")?;
            assert_eq!(custody.operation, operation);
            assert!(custody.published);
            assert_eq!(removals.paths.get("/next").ok_or("new removal was lost")?.operation, None);
        }
        let reservation = view.lazy.workspace().reserve_publication(crate::OperationId::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let permit = crate::PublicationPermit::Reservation {
            operation_id: reservation.operation_id.into_bytes(),
            gate_tail: reservation.gate_tail,
            expected: reservation.expected,
        };
        let completed = view.sync_async_with_permit(
            permit, crate::WorkBudget::UNBOUNDED, &cancellation,
        ).await?;
        assert!(completed.work.authority_records_appended > 0);
        assert_eq!(view.lazy.workspace().operation_generation(
            IdempotencyKey::from_bytes(operation.into_bytes()),
        crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.map(|generation| generation.id()), Some(original_generation.id()));
        assert_eq!(held.read_range(0, 9)?.as_ref(), b"epoch-two");
        assert_eq!(held.lookup()?.node.file_id, identity);
        assert_eq!(std::fs::read(root.path().join("source-only"))?, b"source");
        assert_eq!(std::fs::read(root.path().join("next"))?, b"next");
        assert!(view.lookup(&next)?.is_none());
        assert!(view.removals.lock().map_err(|_| "poisoned removals")?.paths.is_empty());
        assert!(view.removals.lock().map_err(|_| "poisoned removals")?.publication.is_none());
        ({ let __authority_args = (reservation, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = view.lazy.workspace().release_publication(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        Ok(())
    }

    #[cfg(all(feature = "distributed", feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn bounded_mount_faults_recover_same_identity_with_current_permit()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::{Simulation, SimulationFault, SimulationOperation};
        use crate::model::{CheckoutMode, FilesystemProfile, GenerationSelector, VolumeLimits};
        use crate::native_mount::{MountPublication, SharedCheckout};
        for fault in [SimulationFault::PartialObjectRead, SimulationFault::AmbiguousAuthorityAppend] {
            let source_root = tempfile::tempdir()?;
            std::fs::write(source_root.path().join("a"), b"source")?;
            std::fs::hard_link(source_root.path().join("a"), source_root.path().join("b"))?;
            let demand = Arc::new(crate::demand::native::NativeDemandSource::open(
                std::borrow::Cow::Owned(std::path::Path::new(source_root.path()).to_path_buf()), FilesystemProfile::Portable, VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
            ).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?);
            let simulation = Simulation::default();
            let (authority, objects) = simulation.stores();
            let fs = crate::Fs::new(authority, objects, crate::EmbeddedCapabilities::MEMORY);
            let store = crate::MemoryLazyWorkspaceStore::default();
            let lazy = Arc::new(LazyWorkspace::attach(
                &fs, "bounded-mount-fault", Arc::clone(&demand), store.clone(),
            ).await?);
            let checkout = lazy.workspace().checkout(
                GenerationSelector::Head, CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            let config = checkout.volume_config();
            let authored = Arc::new(CheckoutMountSource::new(
                Arc::new(SharedCheckout::with_publication(checkout, MountPublication::Manual)), config,
            )?);
            let view = { CallbackRuntime::create()?; let cancellation = authored.cancellation().clone(); let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation); LazyMountSource::new(Arc::clone(&lazy), authored, "/".to_owned(), super::super::adapter::NativeDemandCarrier::Borrowed(&mut work), None) }?;
            let a = mounted(&["a"]);
            let b = mounted(&["b"]);
            let held = view.open_file(&a)?;
            let identity = held.lookup()?.node.file_id;
            crate::native_mount::adapter::boundary(view.authored.cancellation(), |work| view.remove(&a, Some(identity), work))?;
            held.write_range(0, Bytes::from_static(b"updated"))?;
            simulation.schedule_next(
                if fault == SimulationFault::AmbiguousAuthorityAppend {
                    SimulationOperation::AuthorityAppend
                } else { SimulationOperation::ObjectRead },
                fault,
            )?;
            let cancellation = crate::CancellationToken::new();
            let failure = view.sync_async_with_permit(
                crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &cancellation,
            ).await.expect_err("the real scheduled native publication fault must surface");
            assert!(failure.work.backend_read_operations > 0);
            assert!(failure.work.items_examined > 0);
            let original = if fault == SimulationFault::AmbiguousAuthorityAppend {
                assert!(failure.work.authority_records_appended > 0);
                Some(view.authored.shared_checkout().lock().await.retained_operation_id())
            } else { None };
            let acknowledged_generation = if let Some(operation) = original {
                Some(lazy.workspace().operation_generation(IdempotencyKey::from_bytes(operation.into_bytes()), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.ok_or("lost acknowledgement did not physically append the original operation")?)
            } else { None };
            let reservation = lazy.workspace().reserve_publication(crate::OperationId::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            let permit = crate::PublicationPermit::Reservation {
                operation_id: reservation.operation_id.into_bytes(),
                gate_tail: reservation.gate_tail,
                expected: reservation.expected,
            };
            let recovered = view.sync_async_with_permit(
                permit, crate::WorkBudget::UNBOUNDED, &cancellation,
            ).await?;
            assert!(recovered.work.backend_read_operations > 0);
            if let Some(operation) = original {
                assert_eq!(lazy.workspace().operation_generation(
                    IdempotencyKey::from_bytes(operation.into_bytes()),
                crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.map(|generation| generation.id()), acknowledged_generation.as_ref().map(|generation| generation.id()));
            }
            assert_eq!(view.read_range(&b, 0, 7)?.as_ref(), b"updated");
            assert_eq!(held.lookup()?.node.file_id, identity);
            assert_eq!(held.read_range(0, 7)?.as_ref(), b"updated");
            assert_eq!(view.lookup(&b)?.ok_or("surviving hard link was lost")?.node.file_id, identity);
            assert_eq!(std::fs::read(source_root.path().join("a"))?, b"source");
            assert_eq!(std::fs::read(source_root.path().join("b"))?, b"source");
            assert!(view.lookup(&a)?.is_none());
            ({ let __authority_args = (reservation, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = lazy.workspace().release_publication(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            drop(held);
            drop(view);
            let reopened = LazyWorkspace::open(
                fs.open_workspace("bounded-mount-fault", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, demand, store,
            ).await?;
            assert!(matches!(reopened.lookup("/a").await,
                Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::NotFound)));
            assert_eq!(reopened.read_range("/b", 0, 7).await?.as_ref(), b"updated");
        }
        Ok(())
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn an_advance_records_only_a_moved_head_or_source()
    -> Result<(), Box<dyn std::error::Error>> {
        let source_root = tempfile::tempdir()?;
        let (demand, view, recorded) = recorded_view(source_root.path()).await?;
        let changes = || recorded.0.lock().map(|positions| positions.len());
        let binding = view.binding_epoch();

        view.advance_to_head_async().await?;
        assert_eq!(changes().map_err(|_| "poisoned observer")?, 0);
        assert_eq!(
            view.binding_epoch(),
            binding,
            "an advance that moved nothing keeps every binding"
        );

        demand.invalidate(&mut crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()))?;
        view.advance_to_head_async().await?;
        assert!(
            changes().map_err(|_| "poisoned observer")? > 0,
            "a rebind to a new source epoch changes the view"
        );
        assert_ne!(view.binding_epoch(), binding);
        Ok(())
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn removing_a_source_only_file_supersedes_its_cached_lookups()
    -> Result<(), Box<dyn std::error::Error>> {
        let source_root = tempfile::tempdir()?;
        let (_, view, recorded) = recorded_view(source_root.path()).await?;

        let path = MountPath::root().child(if cfg!(windows) {
            "source-only"
                .encode_utf16()
                .flat_map(u16::to_le_bytes)
                .collect()
        } else {
            b"source-only".to_vec()
        });
        let stamp = view.view_stamp().ok_or("a lazy view carries stamps")?;
        let file_id = view.lookup(&path)?.ok_or("source-only file")?.node.file_id;
        assert!(view.unchanged_since(&path, Some(file_id), stamp));

        crate::native_mount::adapter::boundary(view.authored.cancellation(), |work| view.remove(&path, Some(file_id), work))?;

        assert_eq!(view.lookup(&path)?, None);
        assert!(!view.unchanged_since(&path, Some(file_id), stamp));
        assert!(!view.unchanged_since(&path, None, stamp));
        assert!(!view.unchanged_since(&MountPath::root(), None, stamp));
        let positions = recorded.0.lock().map_err(|_| "poisoned observer")?.clone();
        assert!(
            positions.iter().any(|position| *position > stamp),
            "observers learn of the removal"
        );
        Ok(())
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn manual_fsync_publishes_nothing_until_sync() -> Result<(), Box<dyn std::error::Error>> {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{CheckoutMode, FilesystemProfile, GenerationSelector, VolumeLimits};
        use crate::native_mount::{MountPublication, SharedCheckout};
        use crate::{Fs, MemoryLazyWorkspaceStore};

        let source_root = tempfile::tempdir()?;
        let demand = Arc::new(
            NativeDemandSource::open(
                std::borrow::Cow::Owned(std::path::Path::new(source_root.path()).to_path_buf()),
                FilesystemProfile::Portable,
                VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
            )
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        );
        let fs = Fs::memory();
        let lazy = Arc::new(
            LazyWorkspace::attach(
                &fs,
                "manual-fsync",
                demand,
                MemoryLazyWorkspaceStore::default(),
            )
            .await?,
        );
        let checkout = lazy
            .workspace()
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let config = checkout.volume_config();
        let authored = Arc::new(CheckoutMountSource::new(
            Arc::new(SharedCheckout::with_publication(
                checkout,
                MountPublication::Manual,
            )),
            config,
        )?);
        let view = { CallbackRuntime::create()?; let cancellation = authored.cancellation().clone(); let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation); LazyMountSource::new(Arc::clone(&lazy), authored, "/".to_owned(), super::super::adapter::NativeDemandCarrier::Borrowed(&mut work), None) }?;
        let written = MountPath::root().child(if cfg!(windows) {
            "written"
                .encode_utf16()
                .flat_map(u16::to_le_bytes)
                .collect()
        } else {
            b"written".to_vec()
        });
        view.native_call(|work| view.create_file(&written, FileMetadata::default(), None, work))?;
        view.write_range(&written, 0, Bytes::from_static(b"fsynced"))?;
        view.flush(&crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, view.authored.cancellation()).map_err(|(error, _)| MountSourceError::NativeWork(error))?)?;
        assert!(
            matches!(
                lazy.lookup("/written").await,
                Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::NotFound)
            ),
            "a native fsync under manual publication publishes nothing"
        );
        view.sync_async().await?;
        assert_eq!(
            lazy.read_range("/written", 0, 7).await?.as_ref(),
            b"fsynced"
        );
        Ok(())
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn replaced_source_identity_keeps_open_handle_and_surviving_alias()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{CheckoutMode, FilesystemProfile, GenerationSelector, VolumeLimits};
        use crate::native_mount::{MountPublication, SharedCheckout};
        use crate::{Fs, MemoryLazyWorkspaceStore};

        let source_root = tempfile::tempdir()?;
        std::fs::write(source_root.path().join("a"), b"source")?;
        std::fs::write(source_root.path().join("c"), b"target")?;
        std::fs::hard_link(source_root.path().join("c"), source_root.path().join("d"))?;
        let demand = Arc::new(
            NativeDemandSource::open(
                std::borrow::Cow::Owned(std::path::Path::new(source_root.path()).to_path_buf()),
                FilesystemProfile::Portable,
                VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
            )
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        );
        let fs = Fs::memory();
        let lazy = Arc::new(
            LazyWorkspace::attach(
                &fs,
                "mounted-identity-replace",
                demand,
                MemoryLazyWorkspaceStore::default(),
            )
            .await?,
        );
        let checkout = lazy
            .workspace()
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let config = checkout.volume_config();
        let authored = Arc::new(CheckoutMountSource::new(
            Arc::new(SharedCheckout::with_publication(
                checkout,
                MountPublication::Manual,
            )),
            config,
        )?);
        let view = { CallbackRuntime::create()?; let cancellation = authored.cancellation().clone(); let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation); LazyMountSource::new(Arc::clone(&lazy), authored, "/".to_owned(), super::super::adapter::NativeDemandCarrier::Borrowed(&mut work), None) }?;
        let mount_path = |name: &str| {
            let bytes = if cfg!(windows) {
                name.encode_utf16().flat_map(u16::to_le_bytes).collect()
            } else {
                name.as_bytes().to_vec()
            };
            MountPath::root().child(bytes)
        };
        let a = mounted(&["a"]);
        let c = mounted(&["c"]);
        let d = mounted(&["d"]);
        let old = view.open_file(&c)?;
        crate::native_mount::adapter::boundary(view.authored.cancellation(), |work| view.rename(&a, &c, true, work))?;
        old.write_range(0, Bytes::from_static(b"update"))?;
        assert_eq!(view.read_range(&c, 0, 6)?.as_ref(), b"source");
        assert_eq!(view.read_range(&d, 0, 6)?.as_ref(), b"update");
        view.sync_async().await?;
        assert_eq!(lazy.read_range("/c", 0, 6).await?.as_ref(), b"source");
        assert_eq!(lazy.read_range("/d", 0, 6).await?.as_ref(), b"update");
        old.write_range(0, Bytes::from_static(b"second!"))?;
        view.sync_async().await?;
        assert_eq!(lazy.read_range("/c", 0, 6).await?.as_ref(), b"source");
        assert_eq!(lazy.read_range("/d", 0, 7).await?.as_ref(), b"second!");
        view.allocate_range(&d, 0, 1, MountRangeAllocation::PunchHole)?;
        view.sync_async().await?;
        assert_eq!(lazy.read_range("/d", 0, 7).await?.as_ref(), b"\0econd!");
        let current = view.open_file(&c)?;
        let current_id = current.lookup()?.node.file_id;
        crate::native_mount::adapter::boundary(view.authored.cancellation(), |work| view.remove(&c, Some(current_id), work))?;
        current.write_range(0, Bytes::from_static(b"orphan"))?;
        assert_eq!(current.read_range(0, 6)?.as_ref(), b"orphan");
        let mut external = lazy
            .workspace().begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        external
            .write("/external", Bytes::from_static(b"unrelated"), WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await?;
        external.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        view.sync_async().await?;
        assert!(matches!(
            lazy.lookup("/c").await,
            Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::NotFound)
        ));
        Ok(())
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn conflicting_mount_removal_releases_publication_intent()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{CheckoutMode, FilesystemProfile, GenerationSelector, VolumeLimits};
        use crate::native_mount::{MountPublication, SharedCheckout};
        use crate::{Fs, MemoryLazyWorkspaceStore};

        let source_root = tempfile::tempdir()?;
        std::fs::write(source_root.path().join("file"), b"source")?;
        let demand = Arc::new(
            NativeDemandSource::open(
                std::borrow::Cow::Owned(std::path::Path::new(source_root.path()).to_path_buf()),
                FilesystemProfile::Portable,
                VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
            )
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        );
        let fs = Fs::memory();
        let lazy = Arc::new(
            LazyWorkspace::attach(
                &fs,
                "conflicting-mount-removal",
                demand,
                MemoryLazyWorkspaceStore::default(),
            )
            .await?,
        );
        let checkout = lazy
            .workspace()
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let config = checkout.volume_config();
        let authored = Arc::new(CheckoutMountSource::new(
            Arc::new(SharedCheckout::with_publication(
                checkout,
                MountPublication::Manual,
            )),
            config,
        )?);
        let view = { CallbackRuntime::create()?; let cancellation = authored.cancellation().clone(); let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation); LazyMountSource::new(Arc::clone(&lazy), authored, "/".to_owned(), super::super::adapter::NativeDemandCarrier::Borrowed(&mut work), None) }?;
        let path = MountPath::root().child(if cfg!(windows) {
            "file".encode_utf16().flat_map(u16::to_le_bytes).collect()
        } else {
            b"file".to_vec()
        });
        let file = view.open_file(&path)?;
        crate::native_mount::adapter::boundary(view.authored.cancellation(), |work| view.remove(&path, Some(file.lookup()?.node.file_id), work))?;
        let mut conflicting = lazy
            .workspace().begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        conflicting
            .write("/file", Bytes::from_static(b"external"), WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await?;
        conflicting.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        assert!(matches!(
            view.sync_async().await,
            Err(MountSourceError::Stale)
        ));
        assert!(
            !view
                .authored
                .shared_checkout()
                .lock()
                .await
                .has_retained_operation()
        );
        view.recover_pending_mount_change(crate::PublicationPermit::Unrestricted)
            .await?;
        assert!((async { let cancellation = &crate::CancellationToken::new(); let accounting = LazyTreapWork::new(crate::WorkBudget::UNBOUNDED, cancellation); let result = lazy.mount_removal_publication(&accounting).await; accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.is_none());
        Ok(())
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn remembered_resolutions_follow_every_change_to_the_view()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{CheckoutMode, FilesystemProfile, GenerationSelector, VolumeLimits};
        use crate::native_mount::{MountPublication, SharedCheckout};
        use crate::{Fs, MemoryLazyWorkspaceStore};

        let source_root = tempfile::tempdir()?;
        std::fs::write(source_root.path().join("a"), b"source")?;
        let demand = Arc::new(
            NativeDemandSource::open(
                std::borrow::Cow::Owned(std::path::Path::new(source_root.path()).to_path_buf()),
                FilesystemProfile::Portable,
                VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
            )
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        );
        let fs = Fs::memory();
        let lazy = Arc::new(
            LazyWorkspace::attach(
                &fs,
                "mounted-resolutions",
                demand,
                MemoryLazyWorkspaceStore::default(),
            )
            .await?,
        );
        let checkout = lazy
            .workspace()
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let config = checkout.volume_config();
        let authored = Arc::new(CheckoutMountSource::new(
            Arc::new(SharedCheckout::with_publication(
                checkout,
                MountPublication::Manual,
            )),
            config,
        )?);
        let view = { CallbackRuntime::create()?; let cancellation = authored.cancellation().clone(); let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation); LazyMountSource::new(Arc::clone(&lazy), authored, "/".to_owned(), super::super::adapter::NativeDemandCarrier::Borrowed(&mut work), None) }?;
        let mount_path = |name: &str| {
            let bytes = if cfg!(windows) {
                name.encode_utf16().flat_map(u16::to_le_bytes).collect()
            } else {
                name.as_bytes().to_vec()
            };
            MountPath::root().child(bytes)
        };
        let (a, b) = (mounted(&["a"]), mounted(&["b"]));

        // Remembered answers: a source node, and an absence.
        let source = view.lookup(&a)?.ok_or("source file absent")?;
        assert_eq!(view.lookup(&a)?, Some(source));
        assert_eq!(view.lookup(&b)?, None);
        assert_eq!(view.lookup(&b)?, None);

        // A create through the view replaces the remembered absence.
        let created = view.native_call(|work| view.create_file(&b, FileMetadata::default(), None, work))?;
        assert_eq!(
            view.lookup(&b)?.map(|lookup| lookup.node.file_id),
            Some(created.node.file_id)
        );

        // A write through a handle promotes the file; its answer follows.
        view.open_file(&a)?
            .write_range(0, Bytes::from_static(b"authored-longer"))?;
        assert_eq!(
            view.lookup(&a)?.map(|lookup| lookup.node.logical_bytes),
            Some(15)
        );

        // A removal replaces the remembered answer with an absence.
        crate::native_mount::adapter::boundary(view.authored.cancellation(), |work| view.remove(&a, None, work))?;
        assert_eq!(view.lookup(&a)?, None);
        assert!(matches!(
            view.open_file(&a),
            Err(MountSourceError::NotFound)
        ));

        // Names beneath a directory the source lacks are absent until created.
        let fresh = mounted(&["fresh"]);
        let inner = fresh.child(mounted(&["inner"]).components()[0].clone());
        assert_eq!(view.lookup(&fresh)?, None);
        view.native_call(|work| view.create_directory(&fresh, FileMetadata::default(), work))?;
        assert_eq!(view.lookup(&inner)?, None);
        let created = view.native_call(|work| view.create_file(&inner, FileMetadata::default(), None, work))?;
        assert_eq!(
            view.lookup(&inner)?.map(|lookup| lookup.node.file_id),
            Some(created.node.file_id)
        );

        std::fs::write(source_root.path().join("c"), b"first")?;
        let c = mounted(&["c"]);
        assert!(view.lookup(&c)?.is_some());
        std::fs::write(source_root.path().join("c"), b"second!")?;
        assert_eq!(view.open_file(&c)?.read_range(0, 7)?.as_ref(), b"second!");
        Ok(())
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn promotion_and_attribute_reads_leave_cached_lookups_current()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{CheckoutMode, FilesystemProfile, GenerationSelector, VolumeLimits};
        use crate::native_mount::{MountPublication, SharedCheckout};
        use crate::{Fs, MemoryLazyWorkspaceStore};

        let source_root = tempfile::tempdir()?;
        std::fs::create_dir(source_root.path().join("d"))?;
        std::fs::write(source_root.path().join("d").join("f"), b"source")?;
        std::fs::write(source_root.path().join("g"), b"other")?;
        let demand = Arc::new(
            NativeDemandSource::open(
                std::borrow::Cow::Owned(std::path::Path::new(source_root.path()).to_path_buf()),
                FilesystemProfile::Portable,
                VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
            )
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        );
        let fs = Fs::memory();
        let lazy = Arc::new(
            LazyWorkspace::attach(
                &fs,
                "mounted-promotion-effect",
                demand,
                MemoryLazyWorkspaceStore::default(),
            )
            .await?,
        );
        let checkout = lazy
            .workspace()
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let config = checkout.volume_config();
        let authored = Arc::new(CheckoutMountSource::new(
            Arc::new(SharedCheckout::with_publication(
                checkout,
                MountPublication::Manual,
            )),
            config,
        )?);
        let view = { CallbackRuntime::create()?; let cancellation = authored.cancellation().clone(); let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation); LazyMountSource::new(Arc::clone(&lazy), authored, "/".to_owned(), super::super::adapter::NativeDemandCarrier::Borrowed(&mut work), None) }?;
        let component = |name: &str| -> Vec<u8> {
            if cfg!(windows) {
                name.encode_utf16().flat_map(u16::to_le_bytes).collect()
            } else {
                name.as_bytes().to_vec()
            }
        };
        let directory = MountPath::root().child(component("d"));
        let file = directory.child(component("f"));
        let other = MountPath::root().child(component("g"));
        for path in [&directory, &file, &other] {
            view.lookup(path)?;
        }
        view.fence_changes()?;
        let before = [&directory, &file, &other].map(|path| view.lookup(path));
        let stamp = view.view_stamp().ok_or("lazy view has no stamp")?;

        // Reading attributes of a source file answers from the source.
        assert!(crate::native_mount::adapter::boundary(view.authored.cancellation(), |work| view.read_attribute(&file, b"user.absent", work))?.is_none());
        assert!(view.list_attributes(&file, None, 8)?.names.is_empty());
        assert!(view.authored.lookup(&file)?.is_none());

        view.promote_locked(&file)?;
        assert!(view.authored.lookup(&file)?.is_some());
        let after = [&directory, &file, &other].map(|path| view.lookup(path));
        for ((path, before), after) in [&directory, &file, &other].iter().zip(before).zip(after) {
            let (before, after) = (before?, after?);
            let file_id = after.map(|lookup| lookup.node.file_id);
            assert!(
                !view.unchanged_since(path, file_id, stamp) || before == after,
                "a promotion left a current cache answer stale for {path:?}"
            );
        }
        Ok(())
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn source_hard_link_open_unlink_write_preserves_surviving_alias()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{CheckoutMode, FilesystemProfile, GenerationSelector, VolumeLimits};
        use crate::native_mount::{MountPublication, SharedCheckout};
        use crate::{Fs, MemoryLazyWorkspaceStore};

        for (write_before_unlink, write_before_first_sync, promote_survivor) in [
            (false, false, false),
            (false, true, false),
            (true, true, false),
            (true, true, true),
        ] {
            let source_root = tempfile::tempdir()?;
            std::fs::write(source_root.path().join("a"), b"source")?;
            std::fs::hard_link(source_root.path().join("a"), source_root.path().join("b"))?;
            let demand = Arc::new(
                NativeDemandSource::open(
                    std::borrow::Cow::Owned(std::path::Path::new(source_root.path()).to_path_buf()),
                    FilesystemProfile::Portable,
                    VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
                )
                .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
            );
            let fs = Fs::memory();
            let lazy = Arc::new(
                LazyWorkspace::attach(
                    &fs,
                    "mounted-identity-unlink",
                    demand,
                    MemoryLazyWorkspaceStore::default(),
                )
                .await?,
            );
            let checkout = lazy
                .workspace()
                .checkout(
                    GenerationSelector::Head,
                    CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            let config = checkout.volume_config();
            let authored = Arc::new(CheckoutMountSource::new(
                Arc::new(SharedCheckout::with_publication(
                    checkout,
                    MountPublication::Manual,
                )),
                config,
            )?);
            let view = { CallbackRuntime::create()?; let cancellation = authored.cancellation().clone(); let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation); LazyMountSource::new(Arc::clone(&lazy), authored, "/".to_owned(), super::super::adapter::NativeDemandCarrier::Borrowed(&mut work), None) }?;
            let mount_path = |name: &str| {
                let bytes = if cfg!(windows) {
                    name.encode_utf16().flat_map(u16::to_le_bytes).collect()
                } else {
                    name.as_bytes().to_vec()
                };
                MountPath::root().child(bytes)
            };
            let a = mount_path("a");
            let b = mount_path("b");
            let open = view.open_file(&a)?;
            let identity = open.lookup()?.node.file_id;
            assert!(
                view.prepare_removed_identity_candidate(
                    &BTreeMap::from([("/\0".to_owned(), identity,)]),
                    &[]
                )
                .await
                .is_err()
            );
            assert!(
                !view
                    .authored
                    .shared_checkout()
                    .lock()
                    .await
                    .has_retained_operation()
            );
            assert_eq!(
                view.lookup(&b)?.expect("surviving alias").node.file_id,
                identity
            );
            if write_before_unlink {
                open.write_range(0, Bytes::from_static(b"prime!"))
                    .expect("pre-unlink write");
            }
            let existing_alias = if promote_survivor {
                {
                    let _mutation = view.mutation_lease(None)?;
                    view.promote_locked(&b)?;
                }
                assert!(view.authored.lookup(&b)?.is_some());
                Some(view.open_file(&b)?)
            } else {
                None
            };
            crate::native_mount::adapter::boundary(view.authored.cancellation(), |work| view.remove(&a, Some(identity), work))
                .expect("unlink source alias");
            if write_before_first_sync {
                open.write_range(0, Bytes::from_static(b"update"))
                    .expect("post-unlink write");
            }
            let first = if write_before_first_sync {
                b"update"
            } else {
                b"source"
            };
            assert!(view.lookup(&a)?.is_none(), "removed name must stay absent");
            assert_eq!(view.open_file(&b)?.read_range(0, 6)?.as_ref(), first);
            if let Some(alias) = &existing_alias {
                assert_eq!(alias.read_range(0, 6)?.as_ref(), first);
            }
            view.sync_async().await?;
            assert!(
                view.lookup(&a)?.is_none(),
                "sync must not restore removed name"
            );
            assert_eq!(view.open_file(&b)?.read_range(0, 6)?.as_ref(), first);
            if promote_survivor {
                assert_eq!(view.authored.read_range(&b, 0, 6)?.as_ref(), first);
            }
            assert!(matches!(
                lazy.lookup("/a").await,
                Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::NotFound)
            ));
            assert_eq!(lazy.read_range("/b", 0, 6).await?.as_ref(), first);
            open.write_range(0, Bytes::from_static(b"second!"))?;
            assert_eq!(view.lookup(&b)?.expect("alias").node.logical_bytes, 7);
            assert_eq!(view.read_range(&b, 0, 7)?.as_ref(), b"second!");
            assert_eq!(view.open_file(&b)?.read_range(0, 7)?.as_ref(), b"second!");
            if write_before_unlink {
                crate::native_mount::adapter::boundary(view.authored.cancellation(), |work| view.remove(&b, Some(identity), work))?;
                open.write_range(0, Bytes::from_static(b"orphan!"))?;
                assert_eq!(open.read_range(0, 7)?.as_ref(), b"orphan!");
                assert!(view.lookup(&b)?.is_none());
            }
            drop(existing_alias);
            drop(open);
            assert!(view.open_sources.lock().expect("open handles").is_empty());
            view.sync_async().await?;
            assert!(
                view.detached
                    .lock()
                    .expect("detached identities")
                    .is_empty()
            );
            if write_before_unlink {
                assert!(matches!(
                    lazy.lookup("/b").await,
                    Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::NotFound)
                ));
            } else {
                assert_eq!(lazy.read_range("/b", 0, 7).await?.as_ref(), b"second!");
                assert_eq!(view.open_file(&b)?.read_range(0, 7)?.as_ref(), b"second!");
            }
        }
        Ok(())
    }

    fn finite_directory_budget() -> crate::WorkBudget {
        crate::WorkBudget {
            authority_records_read: 100_000, authority_records_appended: 0,
            authority_bytes_read: 10_000_000, authority_bytes_written: 0,
            object_probes: 100_000, backend_read_operations: 100_000,
            backend_write_operations: 0, durability_operations: 0,
            page_reads: 100_000, page_writes: 0,
            object_bytes_read: 10_000_000, object_bytes_written: 0,
            bytes_hashed: 10_000_000, bytes_copied: 10_000_000, bytes_encoded: 10_000_000,
            source_bytes_read: 10_000_000, source_path_components: 100_000,
            source_entries_visited: 100_000, output_bytes: 10_000_000,
            items_examined: 100_000, items_returned: 100_000,
            allocation_operations: 100_000, peak_allocation_bytes: 10_000_000,
            materializations: 0,
        }
    }

    #[test]
    fn finite_cursor_admission_preserves_authority_on_denial()
    -> Result<(), Box<dyn std::error::Error>> {
        let cursors = CursorTable::new(1);
        let setup_cancellation = crate::CancellationToken::new();
        let mut setup_work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &setup_cancellation);
        let mut setup = super::super::adapter::NativeDemandCarrier::Borrowed(&mut setup_work);
        let first = cursors.remember((2, 1), 1_u8, &mut setup).map(|owned| owned.into_parts().0)?;
        let caller = crate::CancellationToken::new();
        let mut budget = finite_directory_budget();
        budget.allocation_operations = 0;
        let mut denied = crate::demand::DemandWork::new(budget, &caller);
        assert!(matches!(
            cursors.remember((2, 1), 2_u8, &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut denied)).map(|owned| owned.into_parts().0),
            Err(MountSourceError::Work(_))
        ));
        assert_eq!(denied.work.allocation_operations, 0);
        assert_eq!(cursors.take((2, 1), Some(&first), &mut setup)?, Some(1));

        let retained = cursors.remember((2, 1), 3_u8, &mut setup).map(|owned| owned.into_parts().0)?;
        let stopped = crate::CancellationToken::new();
        stopped.cancel();
        let mut cancelled = crate::demand::DemandWork::new(finite_directory_budget(), &stopped);
        assert!(matches!(
            cursors.take((2, 1), Some(&retained), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut cancelled)),
            Err(MountSourceError::Cancelled)
        ));
        assert_eq!(cursors.take((2, 1), Some(&retained), &mut setup)?, Some(3));

        let mut allowed = crate::demand::DemandWork::new(finite_directory_budget(), &caller);
        let token = cursors.remember((2, 1), 4_u8, &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut allowed)).map(|owned| owned.into_parts().0)?;
        assert_eq!(token.capacity(), 8);
        assert_eq!(allowed.work.allocation_operations, 1);
        assert_eq!(allowed.work.peak_allocation_bytes, 8);
        assert_eq!(cursors.take((2, 1), Some(&token), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut allowed))?, Some(4));
        let stale = cursors.remember((2, 1), 5_u8, &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut allowed)).map(|owned| owned.into_parts().0)?;
        assert!(matches!(
            cursors.take((2, 2), Some(&stale), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut allowed)),
            Err(MountSourceError::Stale)
        ));
        assert!(matches!(
            cursors.take((2, 1), Some(&stale), &mut setup),
            Err(MountSourceError::Stale)
        ));
        Ok(())
    }

    #[test]
    fn finite_cursor_retirement_preserves_unvisited_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        let cursors = CursorTable::new(3);
        let setup_cancellation = crate::CancellationToken::new();
        let mut setup_work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &setup_cancellation);
        let mut setup = super::super::adapter::NativeDemandCarrier::Borrowed(&mut setup_work);
        let first = cursors.remember((2, 1), vec![1_u8; 5], &mut setup).map(|owned| owned.into_parts().0)?;
        let second = cursors.remember((2, 1), vec![2_u8; 7], &mut setup).map(|owned| owned.into_parts().0)?;
        let caller = crate::CancellationToken::new();
        let mut budget = finite_directory_budget();
        budget.items_examined = 1;
        let mut denied = crate::demand::DemandWork::new(budget, &caller);
        assert!(matches!(cursors.retire(&mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut denied)), Err(MountSourceError::Work(_))));
        assert_eq!(denied.work.items_examined, 1);
        assert_eq!(denied.work.peak_allocation_bytes, 0);
        assert!(matches!(cursors.take((2, 1), Some(&first), &mut setup), Err(MountSourceError::Stale)));
        assert!(cursors.entries.lock().expect("cursor authority").contains_key(&u64::from_le_bytes(
            second.as_slice().try_into().expect("cursor token"),
        )));
        let mut retry = crate::demand::DemandWork::new(finite_directory_budget(), &caller);
        cursors.retire(&mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut retry))?;
        assert_eq!(retry.work.items_examined, 1);
        assert_eq!(retry.work.peak_allocation_bytes, 0);
        assert!(matches!(cursors.take((2, 1), Some(&second), &mut setup), Err(MountSourceError::Stale)));
        Ok(())
    }

    #[test]
    fn finite_cursor_payload_drop_never_holds_the_original_meter()
    -> Result<(), Box<dyn std::error::Error>> {
        struct Payload {
            meter: Arc<crate::demand::NativeDemandReadMeter>,
            drops: Arc<AtomicU64>,
        }
        impl Drop for Payload {
            fn drop(&mut self) {
                assert!(self.meter.0.try_lock().is_ok());
                self.drops.fetch_add(1, Ordering::Relaxed);
            }
        }
        let caller = crate::CancellationToken::new();
        let meter = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(
            crate::demand::DemandWork::new(finite_directory_budget(), &caller),
        )));
        let drops = Arc::new(AtomicU64::new(0));
        let payload = || Payload { meter: Arc::clone(&meter), drops: Arc::clone(&drops) };
        let cursors = CursorTable::new(1);
        let mut carrier = super::super::adapter::NativeDemandCarrier::Shared(&meter);
        let setup_cancellation = crate::CancellationToken::new();
        let mut setup_work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &setup_cancellation);
        let mut setup = super::super::adapter::NativeDemandCarrier::Borrowed(&mut setup_work);
        let stale = cursors.remember((2, 1), payload(), &mut setup).map(|owned| owned.into_parts().0)?;
        assert!(matches!(
            cursors.take((2, 2), Some(&stale), &mut carrier),
            Err(MountSourceError::Stale)
        ));
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        cursors.remember((2, 1), payload(), &mut setup).map(|owned| owned.into_parts().0)?;
        cursors.remember((2, 1), payload(), &mut carrier)?;
        assert_eq!(drops.load(Ordering::Relaxed), 2);
        let mut denied_budget = finite_directory_budget();
        denied_budget.allocation_operations = 0;
        let denied_meter = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(
            crate::demand::DemandWork::new(denied_budget, &caller),
        )));
        let mut denied_carrier = super::super::adapter::NativeDemandCarrier::Shared(&denied_meter);
        assert!(matches!(
            cursors.remember((2, 1), Payload {
                meter: Arc::clone(&denied_meter), drops: Arc::clone(&drops),
            }, &mut denied_carrier),
            Err(MountSourceError::Work(_))
        ));
        assert_eq!(drops.load(Ordering::Relaxed), 3);
        cursors.retire(&mut carrier)?;
        assert_eq!(drops.load(Ordering::Relaxed), 4);
        Ok(())
    }

    #[test]
    fn native_cursor_original_birth_credit_survives_root_stop_and_wait_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        struct Payload {
            table: std::sync::Weak<CursorTable<Payload>>,
            original: crate::NativeMaterializationWork,
            drops: Arc<AtomicU64>,
        }
        impl Drop for Payload {
            fn drop(&mut self) {
                let _actual_prefix = self.original.receipt();
                assert!(!self.table.upgrade().expect("live source table").physically_empty()
                    .expect("physical-drop observation"));
                self.drops.fetch_add(1, Ordering::AcqRel);
            }
        }
        let root_stop = crate::CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(finite_directory_budget(), &root_stop)
            .map_err(|(cause, _)| cause)?;
        let table = Arc::new(CursorTable::new(1));
        let drops = Arc::new(AtomicU64::new(0));
        let native = original.clone();
        let mut carrier = super::super::adapter::NativeDemandCarrier::Native(&native);
        let token = table.remember((2, 1), Payload {
            table: Arc::downgrade(&table), original: original.clone(), drops: Arc::clone(&drops),
        }, &mut carrier)?;
        drop(token);
        let before = original.receipt();
        let live_before = original.live_allocation_bytes();
        let stopped_wait = crate::CancellationToken::new();
        stopped_wait.cancel();
        assert!(matches!(table.clear_with_cleanup(Some(&stopped_wait)), Err(MountSourceError::Cancelled)));
        assert_eq!(drops.load(Ordering::Acquire), 0);
        assert_eq!(original.receipt(), before);
        root_stop.cancel();
        table.clear_with_cleanup(Some(&crate::CancellationToken::new()))?;
        assert_eq!(drops.load(Ordering::Acquire), 1);
        assert!(table.physically_empty()?);
        let completed = original.receipt();
        assert_eq!(completed.items_examined, before.items_examined + 1);
        assert_eq!(completed.allocation_operations, before.allocation_operations);
        assert!(original.live_allocation_bytes() < live_before);
        table.clear_with_cleanup(Some(&stopped_wait))?;
        assert_eq!(original.receipt(), completed);
        Ok(())
    }

    #[test]
    fn native_cursor_terminal_credit_denial_precedes_buffer_birth()
    -> Result<(), Box<dyn std::error::Error>> {
        let root_stop = crate::CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(
            crate::WorkBudget { items_examined: 0, ..finite_directory_budget() }, &root_stop,
        ).map_err(|(cause, _)| cause)?;
        let table = CursorTable::new(1);
        let native = original.clone();
        let mut carrier = super::super::adapter::NativeDemandCarrier::Native(&native);
        let before = original.receipt();
        assert!(matches!(table.remember((2, 1), 7_u8, &mut carrier),
            Err(MountSourceError::NativeWork(_))));
        assert!(table.physically_empty()?);
        assert_eq!(original.receipt(), before);
        Ok(())
    }

    #[test]
    fn finite_cursor_custody_survives_take_and_foreign_scope_until_payload_drop()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        std::fs::create_dir(source.path().join("dir"))?;
        std::fs::write(source.path().join("dir").join("a"), b"a")?;
        std::fs::write(source.path().join("dir").join("b"), b"b")?;
        let runtime = tokio::runtime::Runtime::new()?;
        let view = runtime.block_on(mounted_view(source.path(), "finite-cursor-custody"))?;
        let caller = crate::CancellationToken::new();
        let budget = finite_directory_budget();
        let original = native_control_work(budget, &caller)?;
        let context = original.clone();
        let mut native = super::super::adapter::NativeDemandCarrier::Native(&context);
        let page = view.read_directory_with_meter(
            &mounted(&["dir"]), None, 1,
            &original,
        )?;
        let token = page.next_cursor.as_deref().ok_or("missing continuation")?;
        let generation = view.cursors.entries.lock().map_err(|_| "cursor lock")?
            .values().next().ok_or("missing cursor")?.generation;
        original.admit(crate::WorkCounters::UNCHARGED)?;
        let before = original.remaining()?.peak_allocation_bytes;
        let held = view.cursors.take(generation, Some(token), &mut native)?
            .ok_or("missing owned cursor")?;
        assert!(held.1.retained_bytes()? > 0);
        assert_eq!(held.1.unclaimed_retained_bytes()?, 0);
        assert_eq!(original.remaining()?.peak_allocation_bytes, before);
        let setup_cancellation = crate::CancellationToken::new();
        let mut setup_work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &setup_cancellation);
        let mut setup = super::super::adapter::NativeDemandCarrier::Borrowed(&mut setup_work);
        let token = view.cursors.remember(generation, held, &mut setup).map(|owned| owned.into_parts().0)?;
        let mut foreign_budget = budget;
        foreign_budget.allocation_operations = 0;
        foreign_budget.peak_allocation_bytes = 0;
        let mut foreign = crate::demand::DemandWork::new(foreign_budget, &caller);
        let held = view.cursors.take(generation, Some(&token), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut foreign))?
            .ok_or("missing borrowed cursor")?;
        assert_eq!(foreign.work.allocation_operations, 0);
        assert_eq!(foreign.work.peak_allocation_bytes, 0);
        assert_eq!(original.remaining()?.peak_allocation_bytes, before);
        drop(held);
        original.admit(crate::WorkCounters::UNCHARGED)?;
        assert!(original.remaining()?.peak_allocation_bytes > before);
        original.receipt().verify(budget)?;
        Ok(())
    }

    #[test]
    fn finite_original_directory_frame_denial_precedes_the_source_callback()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        std::fs::create_dir(source.path().join("dir"))?;
        std::fs::write(source.path().join("dir").join("a"), b"a")?;
        let runtime = tokio::runtime::Runtime::new()?;
        let view = runtime.block_on(mounted_view(source.path(), "finite-original-directory-frame"))?;
        let state = runtime.block_on(async {
            let observation = view.authored.shared_checkout()
                .observe(SourceViewGate::callback_owner(), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())), &crate::CancellationToken::new(), None).await?;
            observation.ensure_publication_resolved()?;
            view.lazy.list_directory_in_checkout(
                &mut observation.observer(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).expect("unbounded observer preserves a validated checkout").value, "/dir", None, 1, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
            ).await.map(|receipt| receipt.value.1).map_err(|failure| lazy_error(crate::lazy_workspace::lazy_operation_failure(failure, std::convert::identity)))
        })?;
        let caller = crate::CancellationToken::new();
        let full_budget = finite_directory_budget();
        let root = native_control_work(full_budget, &caller)?;
        let budget = crate::WorkBudget { allocation_operations: 0, ..full_budget };
        let original = root.native_volume_work(budget)?;
        let before = original.receipt();
        let source_used = std::sync::atomic::AtomicBool::new(false);
        assert!(runtime.block_on(view.lazy.list_directory_in_state_with_native_work(
            &state, "/dir", None, 1, &original, &caller, None, None, Some(&source_used),
        )).is_err());
        assert!(!source_used.load(Ordering::Acquire));
        let work = original.receipt();
        assert_eq!(work.allocation_operations, before.allocation_operations);
        assert_eq!(work.backend_read_operations, 0);
        assert_eq!(work.source_entries_visited, 0);
        work.verify(full_budget)?;
        Ok(())
    }

    #[test]
    fn native_removed_cache_primary_slots_are_both_admitted_before_allocation()
    -> Result<(), Box<dyn std::error::Error>> {
        let caller = crate::CancellationToken::new();
        let root = native_control_work(finite_directory_budget(), &caller)?;
        let scoped = root.native_volume_work(crate::WorkBudget {
            allocation_operations: 1, ..finite_directory_budget()
        })?;
        let native = scoped.clone();
        let mut carrier = super::super::adapter::NativeDemandCarrier::Native(&native);
        let prefix = root.receipt();
        let mut scratch = DirectoryCarrierScratch::new(&mut carrier);
        assert!(scratch.prepare_finalization().is_err());
        assert!(scratch.finalization.is_none());
        assert_eq!(root.receipt(), prefix);
        Ok(())
    }

    #[test]
    fn native_removed_cache_partial_failure_retains_real_prefix_and_original_primary()
    -> Result<(), Box<dyn std::error::Error>> {
        for fail_accounting in [false, true] {
            let source = tempfile::tempdir()?;
            let runtime = tokio::runtime::Runtime::new()?;
            let view = runtime.block_on(mounted_view(source.path(), "native-removal-prefix"))?;
            let file_id = view.lookup(&MountPath::root())?.ok_or("missing mounted root")?.node.file_id;
            let caller = crate::CancellationToken::new();
            let original = native_control_work(finite_directory_budget(), &caller)?;
            let native = original.clone();
            let mut carrier = super::super::adapter::NativeDemandCarrier::Native(&native);
            let mut scratch = DirectoryCarrierScratch::new(&mut carrier);
            scratch.prepare_finalization()?;
            let mut text = mount_path_text_with_carrier("/", &mounted(&["removed"]), scratch.carrier)?;
            let mut entry = original.claim(std::mem::size_of::<MountedRemoval>() as u64, 0, crate::WorkCounters::UNCHARGED)?;
            text.allocation.as_mut().ok_or("native path has no custody")?.absorb(&mut entry)?;
            let final_work = crate::WorkCounters {
                items_examined: 2, bytes_copied: std::mem::size_of::<MountedRemoval>() as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            scratch.reserve_final_work(final_work)?;
            let removal = PreparedMountedRemoval {
                key: MountedRemovalPath { text: text.value, custody: None, bytes: 0, native_allocation: text.allocation },
                file_id, prior: None, old_paths: None, new_paths: Some(1), changed: true, final_work,
            };
            assert!(view.removals.lock().map_err(|_| "removals poisoned")?.records.is_empty());
            if fail_accounting {
                original.with_original_leaf(|work| work.budget.allocation_operations = 0);
            }
            let prefix = original.receipt();
            let error = view.commit_removed_with_carrier(removal, None, None, &mut scratch)
                .expect_err("the missing prepared record must retain its semantic failure");
            assert!(matches!(error.semantic_error(), MountSourceError::Stale));
            assert_eq!(matches!(error, MountSourceError::MutationFinalization { .. }), fail_accounting);
            if let MountSourceError::MutationFinalization { primary, cleanup, custody } = &error {
                assert!(primary.is_some());
                assert!(cleanup.is_some());
                assert!(custody.is_some());
            }
            let completed = original.receipt();
            assert_eq!(completed.items_examined - prefix.items_examined, 2);
            assert_eq!(completed.bytes_copied - prefix.bytes_copied, std::mem::size_of::<MountedRemoval>() as u64);
            assert_eq!(view.removals.lock().map_err(|_| "removals poisoned")?.paths.get("/removed").map(|entry| entry.file_id), Some(file_id));
        }
        Ok(())
    }

    #[test]
    fn native_detached_table_denial_precedes_growth_and_keeps_original_scope()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        let runtime = tokio::runtime::Runtime::new()?;
        let view = runtime.block_on(mounted_view(source.path(), "native-detached-table-denial"))?;
        let file_id = view.lookup(&MountPath::root())?.ok_or("missing mounted root")?.node.file_id;
        let caller = crate::CancellationToken::new();
        let root = native_control_work(finite_directory_budget(), &caller)?;
        let scoped = root.native_volume_work(crate::WorkBudget {
            allocation_operations: 0, ..finite_directory_budget()
        })?;
        let native = scoped.clone();
        let mut carrier = super::super::adapter::NativeDemandCarrier::Native(&native);
        let before = root.receipt();
        assert!(view.reserve_detached_slot_with_carrier(file_id, &mut carrier).is_err());
        let table = view.detached.lock().map_err(|_| "detached table poisoned")?;
        assert!(table.is_empty());
        assert_eq!(table.capacity(), 0);
        assert!(table.native_allocation.is_none());
        assert_eq!(root.receipt(), before);
        Ok(())
    }

    #[test]
    fn native_detached_table_borrowed_backing_keeps_original_physical_custody()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        let runtime = tokio::runtime::Runtime::new()?;
        let view = runtime.block_on(mounted_view(source.path(), "native-detached-table-custody"))?;
        let file_id = view.lookup(&MountPath::root())?.ok_or("missing mounted root")?.node.file_id;
        let caller = crate::CancellationToken::new();
        let root = native_control_work(finite_directory_budget(), &caller)?;
        let original = root.native_volume_work(finite_directory_budget())?;
        let native = original.clone();
        let mut carrier = super::super::adapter::NativeDemandCarrier::Native(&native);
        assert_eq!(view.reserve_detached_slot_with_carrier(file_id, &mut carrier)?, Some(0));
        let capacity = view.detached.lock().map_err(|_| "detached table poisoned")?.capacity();
        assert!(capacity > 0);
        let borrowed = root.native_volume_work(crate::WorkBudget {
            allocation_operations: 0, peak_allocation_bytes: 0, ..finite_directory_budget()
        })?;
        let borrowed_native = borrowed.clone();
        let mut borrowed_carrier = super::super::adapter::NativeDemandCarrier::Native(&borrowed_native);
        let before = original.remaining()?.peak_allocation_bytes;
        let prefix = root.receipt();
        assert_eq!(view.reserve_detached_slot_with_carrier(file_id, &mut borrowed_carrier)?, Some(0));
        assert_eq!(root.receipt(), prefix);
        let held = std::mem::take(&mut *view.detached.lock().map_err(|_| "detached table poisoned")?);
        assert_eq!(held.capacity(), capacity);
        assert!(held.native_allocation.is_some());
        assert_eq!(original.remaining()?.peak_allocation_bytes, before);
        drop(held);
        assert!(original.remaining()?.peak_allocation_bytes > before);
        root.receipt().verify(finite_directory_budget())?;
        Ok(())
    }

    #[test]
    fn finite_original_lower_pages_transfer_the_same_cursor_custody()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        std::fs::create_dir(source.path().join("dir"))?;
        std::fs::write(source.path().join("dir").join("a"), b"a")?;
        std::fs::write(source.path().join("dir").join("b"), b"b")?;
        let runtime = tokio::runtime::Runtime::new()?;
        let view = runtime.block_on(mounted_view(source.path(), "finite-original-lower-pages"))?;
        let state = runtime.block_on(async {
            let observation = view.authored.shared_checkout()
                .observe(SourceViewGate::callback_owner(), &mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())), &crate::CancellationToken::new(), None).await?;
            observation.ensure_publication_resolved()?;
            view.lazy.list_directory_in_checkout(
                &mut observation.observer(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).expect("unbounded observer preserves a validated checkout").value, "/dir", None, 1, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
            ).await.map(|receipt| receipt.value.1).map_err(|failure| lazy_error(crate::lazy_workspace::lazy_operation_failure(failure, std::convert::identity)))
        })?;
        let caller = crate::CancellationToken::new();
        let budget = finite_directory_budget();
        let original = native_control_work(budget, &caller)?;
        let source_used = std::sync::atomic::AtomicBool::new(false);
        let mut first = runtime.block_on(view.lazy.list_directory_in_state_with_native_work(
            &state, "/dir", None, 1, &original, &caller, None, None, Some(&source_used),
        ))?;
        assert_eq!(first.entries.len(), 1);
        assert!(first.entries[0].source.is_some());
        assert!(source_used.load(Ordering::Acquire));
        let cursor = first.next.take().ok_or("missing native lower continuation")?;
        assert_eq!(cursor.unclaimed_retained_bytes()?, 0);
        drop(first);
        let second = runtime.block_on(view.lazy.list_directory_in_state_with_native_work(
            &state, "/dir", Some(cursor), 1, &original, &caller, None, None, None,
        ))?;
        assert_eq!(second.entries.len(), 1);
        if let Some(cursor) = &second.next {
            assert_eq!(cursor.unclaimed_retained_bytes()?, 0);
        }
        drop(second);
        let work = original.receipt();
        assert!(work.allocation_operations > 0);
        assert!(work.source_entries_visited > 0);
        work.verify(budget)?;
        Ok(())
    }

    #[test]
    fn abandoned_directory_cursors_are_bounded_and_cleared() {
        let cursors = CursorTable::new(2);
        let setup_cancellation = crate::CancellationToken::new();
        let mut setup_work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &setup_cancellation);
        let mut setup = super::super::adapter::NativeDemandCarrier::Borrowed(&mut setup_work);
        let first = cursors.remember((2, 1), 1_u8, &mut setup).map(|owned| owned.into_parts().0).expect("first cursor");
        let second = cursors.remember((2, 1), 2_u8, &mut setup).map(|owned| owned.into_parts().0).expect("second cursor");
        let third = cursors.remember((2, 1), 3_u8, &mut setup).map(|owned| owned.into_parts().0).expect("third cursor");

        assert!(matches!(
            cursors.take((2, 1), Some(&first), &mut setup),
            Err(MountSourceError::Stale)
        ));
        assert_eq!(
            cursors.take((2, 1), Some(&second), &mut setup).expect("second token"),
            Some(2)
        );
        assert_eq!(
            cursors.take((2, 1), Some(&third), &mut setup).expect("third token"),
            Some(3)
        );

        let stale = cursors.remember((2, 1), 4_u8, &mut setup).map(|owned| owned.into_parts().0).expect("stale cursor");
        assert!(matches!(
            cursors.take((3, 1), Some(&stale), &mut setup),
            Err(MountSourceError::Stale)
        ));
        let retained = cursors.remember((2, 1), 5_u8, &mut setup).map(|owned| owned.into_parts().0).expect("retained cursor");
        cursors.clear();
        assert!(matches!(
            cursors.take((2, 1), Some(&retained), &mut setup),
            Err(MountSourceError::Stale)
        ));
    }

    #[tokio::test]
    async fn source_view_gate_serializes_rebinds_and_fences_old_handles() {
        let gate = Arc::new(SourceViewGate::new());
        let initial = Arc::clone(&gate).read(None).await.expect("initial lease");
        assert_eq!(initial.generation, SourceViewGate::INITIAL_GENERATION);

        let waiting_gate = Arc::clone(&gate);
        let writer = tokio::spawn(async move { waiting_gate.write().await });
        tokio::task::yield_now().await;
        assert!(
            !writer.is_finished(),
            "a rebind must wait for active readers"
        );
        drop(initial);

        let writer = writer.await.expect("writer task");
        gate.begin_transition();
        assert!(!gate.is_stable());
        gate.finish_transition();
        drop(writer);

        assert!(matches!(
            Arc::clone(&gate)
                .read(Some(SourceViewGate::INITIAL_GENERATION))
                .await,
            Err(MountSourceError::Stale)
        ));
        assert_eq!(
            Arc::clone(&gate)
                .read(None)
                .await
                .expect("new lease")
                .generation,
            SourceViewGate::INITIAL_GENERATION + 2
        );
    }

    #[tokio::test]
    async fn source_view_gate_excludes_authored_mutations_from_read_callbacks() {
        let gate = Arc::new(SourceViewGate::new());
        let reader = Arc::clone(&gate).read(None).await.expect("reader lease");
        let mutation_gate = Arc::clone(&gate);
        let mutation = tokio::spawn(async move {
            mutation_gate
                .write_stable(Some(SourceViewGate::INITIAL_GENERATION))
                .await
        });
        tokio::task::yield_now().await;
        assert!(
            !mutation.is_finished(),
            "an authored mutation must wait for active read callbacks"
        );
        drop(reader);

        let mutation = mutation
            .await
            .expect("mutation task")
            .expect("mutation lease");
        assert_eq!(mutation.generation, SourceViewGate::INITIAL_GENERATION);
        let reader_gate = Arc::clone(&gate);
        let reader = tokio::spawn(async move { reader_gate.read(None).await });
        tokio::task::yield_now().await;
        assert!(
            !reader.is_finished(),
            "a read callback must wait for an authored mutation"
        );
        drop(mutation);
        assert_eq!(
            reader
                .await
                .expect("reader task")
                .expect("reader lease")
                .generation,
            SourceViewGate::INITIAL_GENERATION
        );
    }

    #[tokio::test]
    async fn source_view_gate_allows_nested_callback_reads_while_writer_waits() {
        let gate = Arc::new(SourceViewGate::new());
        let owner = SourceViewGate::callback_owner();
        let outer = Arc::clone(&gate)
            .read_for_callback(owner, None)
            .await
            .expect("outer lease");
        let writer_gate = Arc::clone(&gate);
        let writer = tokio::spawn(async move { writer_gate.write().await });
        tokio::task::yield_now().await;
        assert!(!writer.is_finished(), "writer must wait for outer callback");

        let nested = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            Arc::clone(&gate).read_for_callback(owner, Some(outer.generation)),
        )
        .await
        .expect("a callback must not deadlock when its source method reacquires the view")
        .expect("nested lease");
        drop(nested);
        drop(outer);
        drop(writer.await.expect("writer task"));
    }

    #[tokio::test]
    async fn source_view_gate_gives_a_waiting_writer_priority_over_new_callbacks() {
        let gate = Arc::new(SourceViewGate::new());
        let active = Arc::clone(&gate).read(None).await.expect("active reader");
        let writer_gate = Arc::clone(&gate);
        let writer = tokio::spawn(async move { writer_gate.write().await });
        tokio::task::yield_now().await;
        assert!(!writer.is_finished(), "writer must wait for active reader");

        let later_gate = Arc::clone(&gate);
        let later = tokio::spawn(async move { later_gate.read(None).await });
        tokio::task::yield_now().await;
        assert!(
            !later.is_finished(),
            "a new callback bypassed a waiting writer"
        );

        drop(active);
        let writer = tokio::time::timeout(std::time::Duration::from_secs(1), writer)
            .await
            .expect("writer made no progress")
            .expect("writer task");
        assert!(!later.is_finished(), "reader crossed the active writer");
        drop(writer);
        drop(
            tokio::time::timeout(std::time::Duration::from_secs(1), later)
                .await
                .expect("later reader made no progress")
                .expect("reader task")
                .expect("reader lease"),
        );
    }

    #[cfg(all(feature = "native-watch", any(target_os = "linux", windows)))]
    #[tokio::test(flavor = "multi_thread")]
    async fn measured_source_shutdown_retains_the_same_native_watch_until_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let view = mounted_view(root.path(), "bounded-source-watch-shutdown").await?;
        let watch = view.source_watch.as_deref().ok_or("local source did not start its native watch")?;
        let original = std::ptr::from_ref(watch);
        assert!(watch.is_exact());

        let cancelled = crate::CancellationToken::new();
        cancelled.cancel();
        let mut cancelled_work = crate::demand::DemandWork::new(finite_directory_budget(), &cancelled);
        assert!(matches!(view.cancel(&mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut cancelled_work)), Err(MountSourceError::Cancelled)));
        assert!(!view.authored.cancellation().is_cancelled());
        assert!(watch.is_exact());

        let cleanup = crate::CancellationToken::new();
        let mut denied = crate::demand::DemandWork::new(finite_directory_budget(), &cleanup);
        assert!(matches!(view.cancel(&mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut denied)), Err(MountSourceError::Work(_))));
        assert!(view.authored.cancellation().is_cancelled());
        assert!(denied.work.items_examined > 0);
        assert_eq!(denied.work.backend_write_operations, 0);
        let mut loan_work = crate::demand::DemandWork::new(finite_directory_budget(), &cleanup);
        super::super::adapter::BorrowedNativeDemandMeter::new(&mut loan_work)?
            .run(|_| Ok::<(), MountSourceError>(()))?;
        assert_eq!(denied.work.allocation_operations, loan_work.work.allocation_operations);
        assert_eq!(denied.work.peak_allocation_bytes, loan_work.work.peak_allocation_bytes);
        assert!(watch.is_exact());
        assert!(!view.source_watch.as_ref().expect("retained watch owner").is_stopped());
        assert!(std::ptr::eq(original, std::ptr::from_ref(
            view.source_watch.as_deref().ok_or("failed cleanup discarded the original watch")?,
        )));

        let budget = crate::WorkBudget { backend_write_operations: 100_000, ..finite_directory_budget() };
        let mut retried = crate::demand::DemandWork::new(budget, &cleanup);
        view.cancel(&mut super::super::adapter::NativeDemandCarrier::Borrowed(&mut retried))?;
        assert!(retried.work.backend_read_operations > 0);
        assert!(retried.work.backend_write_operations > 0);
        assert!(!watch.is_exact(), "success requires actual native watch quiescence before Source Drop");
        assert!(view.source_watch.as_ref().expect("same completed watch owner").is_stopped());
        assert!(std::ptr::eq(original, std::ptr::from_ref(
            view.source_watch.as_deref().ok_or("successful stop discarded its live owner early")?,
        )));
        Ok(())
    }

    async fn mounted_view(
        source: &Path,
        name: &str,
    ) -> Result<
        LazyMountSource<
            impl AsyncAuthorityStore + Send + Sync + 'static,
            impl AsyncObjectStore + Send + Sync + 'static,
            crate::demand::native::NativeDemandSource,
            crate::MemoryLazyWorkspaceStore,
        >,
        Box<dyn std::error::Error>,
    > {
        mounted_view_of(Arc::new(native_source(source).await?), name).await
    }

    async fn native_source(
        source: &Path,
    ) -> Result<crate::demand::native::NativeDemandSource, Box<dyn std::error::Error>> {
        use crate::model::{FilesystemProfile, VolumeLimits};
        Ok(crate::demand::native::NativeDemandSource::open(
            std::borrow::Cow::Owned(std::path::Path::new(source).to_path_buf()),
            FilesystemProfile::Portable,
            VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
        )
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?)
    }

    async fn mounted_view_of<D: DemandSource + 'static>(
        demand: Arc<D>,
        name: &str,
    ) -> Result<
        LazyMountSource<
            impl AsyncAuthorityStore + Send + Sync + 'static,
            impl AsyncObjectStore + Send + Sync + 'static,
            D,
            crate::MemoryLazyWorkspaceStore,
        >,
        Box<dyn std::error::Error>,
    > {
        use crate::model::{CheckoutMode, GenerationSelector};
        use crate::native_mount::{MountPublication, SharedCheckout};
        use crate::{Fs, MemoryLazyWorkspaceStore};

        let lazy = Arc::new(
            LazyWorkspace::attach(
                &Fs::memory(),
                name,
                demand,
                MemoryLazyWorkspaceStore::default(),
            )
            .await?,
        );
        let checkout = lazy
            .workspace()
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let config = checkout.volume_config();
        let authored = Arc::new(CheckoutMountSource::new(
            Arc::new(SharedCheckout::with_publication(
                checkout,
                MountPublication::Manual,
            )),
            config,
        )?);
        Ok({ CallbackRuntime::create()?; let cancellation = authored.cancellation().clone(); let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation); LazyMountSource::new(lazy, authored, "/".to_owned(), super::super::adapter::NativeDemandCarrier::Borrowed(&mut work), None) }?)
    }

    fn mounted(names: &[&str]) -> MountPath {
        names.iter().fold(MountPath::root(), |path, name| {
            path.child(if cfg!(windows) {
                name.encode_utf16().flat_map(u16::to_le_bytes).collect()
            } else {
                name.as_bytes().to_vec()
            })
        })
    }

    fn finite_content_budget() -> crate::WorkBudget {
        crate::WorkBudget {
            authority_records_read: 10_000, authority_records_appended: 0,
            authority_bytes_read: 8_000_000, authority_bytes_written: 0,
            object_probes: 10_000, backend_read_operations: 10_000,
            backend_write_operations: 0, durability_operations: 0,
            page_reads: 10_000, page_writes: 0,
            object_bytes_read: 8_000_000, object_bytes_written: 0,
            bytes_hashed: 8_000_000, bytes_copied: 8_000_000, bytes_encoded: 8_000_000,
            source_bytes_read: 8_000_000, source_path_components: 10_000,
            source_entries_visited: 10_000, output_bytes: 8_000_000,
            items_examined: 100_000, items_returned: 10_000,
            allocation_operations: 100_000, peak_allocation_bytes: 8_000_000,
            materializations: 0,
        }
    }

    fn native_control_work(
        budget: crate::WorkBudget, cancellation: &crate::CancellationToken,
    ) -> Result<crate::NativeMaterializationWork, crate::NativeTreeMaterializationError> {
        crate::NativeMaterializationWork::new(budget, cancellation).map_err(|(error, _)| error)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mounted_readonly_qualification_preserves_actual_cut_and_rejects_dirty_or_stale_source()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::model::{CheckoutMode, GenerationSelector};
        use crate::native_mount::{MountPublication, SharedCheckout};
        use crate::{Fs, LazyWorkspaceStore, MemoryLazyWorkspaceStore};

        for mutation in 0..3 {
            let source_root = tempfile::tempdir()?;
            std::fs::write(source_root.path().join("file"), b"original")?;
            let demand = Arc::new(native_source(source_root.path()).await?);
            let store = MemoryLazyWorkspaceStore::default();
            let lazy = Arc::new(LazyWorkspace::attach(
                &Fs::memory(), &format!("mounted-readonly-{mutation}"),
                Arc::clone(&demand), store.clone(),
            ).await?);
            let checkout = lazy.workspace().checkout(
                GenerationSelector::Head, CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            let config = checkout.volume_config();
            let authored = Arc::new(CheckoutMountSource::new(
                Arc::new(SharedCheckout::with_publication(checkout, MountPublication::Manual)),
                config,
            )?);
            let view = { CallbackRuntime::create()?; let cancellation = authored.cancellation().clone(); let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation); LazyMountSource::new(Arc::clone(&lazy), authored, "/".to_owned(), super::super::adapter::NativeDemandCarrier::Borrowed(&mut work), None) }?;
            let basis = lazy.snapshot(&LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await?;
            let before = store.load_lazy_workspace(lazy.workspace().id()).await?
                .ok_or("missing original companion")?;
            let caller = crate::CancellationToken::new();
            let original = native_control_work(finite_content_budget(), &caller)?;
            let native = original.clone();
            let live_before = original.live_allocation_bytes();
            let publication = view.publication_snapshot(super::super::adapter::NativeDemandCarrier::Native(&native), &caller)
                .await.map_err(|(primary, _)| primary)?;
            assert_eq!(publication.operation(), None, "READ must not manufacture a DATA operation");
            assert_eq!(publication.snapshot().workspace_id(), lazy.workspace().id());
            assert_eq!(publication.snapshot().volume_config(), config);
            assert_eq!(publication.snapshot().generation(), basis.authored_generation);
            assert_eq!(publication.snapshot().lazy_snapshot_with_native_work(&original, &caller)?, Some(basis));
            assert_eq!(original.live_allocation_bytes(), live_before,
                "fixed qualification cannot retain already-dropped decoded companion data");
            let work = original.receipt();
            assert_eq!(work.authority_records_appended, 0);
            assert_eq!(work.backend_write_operations, 0);
            assert_eq!(work.durability_operations, 0);
            assert_eq!(work.object_bytes_written, 0);
            assert_eq!(work.page_writes, 0);
            let after = store.load_lazy_workspace(lazy.workspace().id()).await?
                .ok_or("missing readonly companion")?;
            assert_eq!((after.revision, after.source, after.overlay, after.shadows),
                (before.revision, before.source, before.overlay, before.shadows));

            match mutation {
                0 => { view.native_call(|work| view.create_directory(&mounted(&["pending"]), FileMetadata::default(), work))?; }
                1 => {
                    let path = mounted(&["file"]);
                    let held = view.open_file(&path)?;
                    crate::native_mount::adapter::boundary(view.authored.cancellation(), |work| view.remove(&path, Some(held.lookup()?.node.file_id), work))?;
                    held.write_range(0, Bytes::from_static(b"detached"))?;
                }
                _ => { demand.invalidate(&mut crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()))?; }
            }
            let stopped_snapshot = view.publication_snapshot(super::super::adapter::NativeDemandCarrier::Native(&native), &caller).await
                .expect_err("pending directories, detached writes and changed source epochs are not readonly checkpoints");
            assert!(matches!(stopped_snapshot, (MountSourceError::Stale, None)));
            let unchanged = store.load_lazy_workspace(lazy.workspace().id()).await?
                .ok_or("companion disappeared after readonly rejection")?;
            assert_eq!((unchanged.revision, unchanged.source, unchanged.overlay, unchanged.shadows),
                (before.revision, before.source, before.overlay, before.shadows),
                "a rejected READ cannot rebind or write the companion");
            assert_eq!(lazy.workspace().head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), basis.authored_generation);
            let work = original.receipt();
            assert_eq!(work.authority_records_appended, 0);
            assert_eq!(work.backend_write_operations, 0);
            assert_eq!(work.durability_operations, 0);
        }
        Ok(())
    }
    struct PublicationLifetimeGuard {
        lifetime: crate::CancellationToken,
        visits: Arc<std::sync::atomic::AtomicUsize>,
        authored_write: bool,
    }

    impl PublicationLifetimeGuard {
        fn verify<'a>(
            &'a self, work: &'a crate::NativeMaterializationWork, require_write: bool,
        ) -> Result<super::super::NativeMountCallbackFuture<'a>, super::super::NativeMountError> {
            if require_write && !self.authored_write {
                return Err(super::super::NativeMountError::Lifecycle(
                    "the installed fixture policy permits READ only",
                ));
            }
            super::super::NativeMountCallbackFuture::new_guarded(work, move || async move {
                work.charge(crate::WorkCounters {
                    items_examined: 1, ..crate::WorkCounters::UNCHARGED
                }).map_err(MountSourceError::NativeWork)?;
                self.visits.fetch_add(1, Ordering::AcqRel);
                self.lifetime.check().map_err(|_| MountSourceError::Stale)?;
                Ok::<_, MountSourceError>(())
            })
        }
    }

    impl super::super::NativeMountCallbackGuard for PublicationLifetimeGuard {
        fn check<'a>(
            &'a self, work: &'a crate::NativeMaterializationWork,
        ) -> Result<super::super::NativeMountCallbackFuture<'a>, super::super::NativeMountError> {
            self.verify(work, false)
        }

        fn check_write<'a>(
            &'a self, work: &'a crate::NativeMaterializationWork,
        ) -> Result<super::super::NativeMountCallbackFuture<'a>, super::super::NativeMountError> {
            self.verify(work, true)
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn installed_source_guard_denial_precedes_caller_guard_and_preserves_dirty_candidate()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        std::fs::write(source.path().join("file"), b"before")?;
        let mut view = mounted_view(source.path(), "installed-source-publication-guard").await?;
        let owner = crate::CancellationToken::new();
        let visits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        view.authored = Arc::new(Arc::try_unwrap(view.authored).map_err(|_| "authored adapter is shared")?.with_native_callback_guard(Arc::new(PublicationLifetimeGuard {
            lifetime: owner.clone(), visits: Arc::clone(&visits),
            authored_write: true,
        })));
        let held = view.open_file(&mounted(&["file"]))?;
        held.write_range(0, Bytes::from_static(b"after"))?;
        let caller = crate::CancellationToken::new();
        let original = native_control_work(crate::WorkBudget::UNBOUNDED, &caller)?;
        let caller_visits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let before = visits.load(Ordering::Acquire);
        owner.cancel();
        let result = view.authored.sync_async_with_permit_carrier_guarded(crate::PublicationPermit::Unrestricted, false, &mut super::super::adapter::NativeDemandCarrier::Native(&original), &caller, &mut
            {
                let caller_visits = Arc::clone(&caller_visits);
                move |work: &crate::NativeMaterializationWork| {
                    let work = work.clone();
                    let caller_visits = Arc::clone(&caller_visits);
                    async move {
                        caller_visits.fetch_add(1, Ordering::AcqRel);
                        work.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)
                    }
                }
            },
        ).await;
        assert!(matches!(result, Err(MountSourceError::NativeCallbackDenied { .. })));
        assert!(visits.load(Ordering::Acquire) > before);
        assert_eq!(caller_visits.load(Ordering::Acquire), 0);
        assert_eq!(original.receipt().authority_records_appended, 0);
        assert!(view.authored.shared_checkout().read_current_state().await.has_pending_mutations());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn installed_read_policy_cannot_publish_an_actual_dirty_candidate()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        std::fs::write(source.path().join("file"), b"before")?;
        let mut view = mounted_view(source.path(), "installed-read-publication-policy").await?;
        let held = view.open_file(&mounted(&["file"]))?;
        held.write_range(0, Bytes::from_static(b"after"))?;
        let visits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let guard: Arc<dyn super::super::NativeMountCallbackGuard> =
            Arc::new(PublicationLifetimeGuard {
                lifetime: crate::CancellationToken::new(),
                visits: Arc::clone(&visits),
                authored_write: false,
            });
        let caller = crate::CancellationToken::new();
        let original = native_control_work(crate::WorkBudget::UNBOUNDED, &caller)?;
        guard.check(&original)?.await?;
        assert_eq!(visits.load(Ordering::Acquire), 1);
        drop(held);
        view.authored = Arc::new(Arc::try_unwrap(view.authored).map_err(|_| "authored adapter is shared")?.with_native_callback_guard(guard));
        let caller_visits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let result = view.authored.sync_async_with_permit_carrier_guarded(crate::PublicationPermit::Unrestricted, false, &mut super::super::adapter::NativeDemandCarrier::Native(&original), &caller, &mut
            {
                let caller_visits = Arc::clone(&caller_visits);
                move |work: &crate::NativeMaterializationWork| {
                    let work = work.clone();
                    let caller_visits = Arc::clone(&caller_visits);
                    async move {
                        caller_visits.fetch_add(1, Ordering::AcqRel);
                        work.admit(crate::WorkCounters::UNCHARGED)
                            .map_err(MountSourceError::NativeWork)
                    }
                }
            },
        ).await;
        assert!(result.is_err());
        assert_eq!(caller_visits.load(Ordering::Acquire), 0);
        assert_eq!(visits.load(Ordering::Acquire), 1,
            "WRITE denial precedes the verification future, despite valid READ");
        assert_eq!(original.receipt().authority_records_appended, 0);
        assert!(view.authored.shared_checkout().read_current_state().await.has_pending_mutations());
        assert_eq!(std::fs::read(source.path().join("file"))?, b"before");
        Ok(())
    }

    #[tokio::test]
    async fn source_publication_guard_budget_denial_does_not_start_owned_guard_future()
    -> Result<(), Box<dyn std::error::Error>> {
        let caller = crate::CancellationToken::new();
        let root = native_control_work(crate::WorkBudget::UNBOUNDED, &caller)?;
        let original = root.native_volume_work(crate::WorkBudget {
            allocation_operations: 0, ..crate::WorkBudget::UNBOUNDED
        })?;
        let visits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let guard: Arc<dyn super::super::NativeMountCallbackGuard> = Arc::new(PublicationLifetimeGuard {
            lifetime: crate::CancellationToken::new(), visits: Arc::clone(&visits),
            authored_write: true,
        });
        let before = original.receipt();
        let mut publish = super::super::adapter::source_publication_guard(Some(guard), Some(caller.clone()));
        assert!(matches!(publish(&original).await, Err(MountSourceError::NativeWork(_))));
        assert_eq!(visits.load(Ordering::Acquire), 0);
        assert_eq!(original.receipt().allocation_operations, before.allocation_operations);
        assert_eq!(original.receipt().items_examined, before.items_examined);
        Ok(())
    }

    #[tokio::test]
    async fn sdk_only_source_policy_checks_both_original_accounting_and_source_lifetime()
    -> Result<(), Box<dyn std::error::Error>> {
        let caller = crate::CancellationToken::new();
        let original = native_control_work(crate::WorkBudget::UNBOUNDED, &caller)?;
        let source_lifetime = crate::CancellationToken::new();
        let mut publish = super::super::adapter::source_publication_guard(None, Some(source_lifetime.clone()));
        let before = original.receipt();
        publish(&original).await?;
        assert_eq!(original.receipt(), before);
        source_lifetime.cancel();
        assert!(matches!(publish(&original).await, Err(MountSourceError::Stale)));
        assert_eq!(original.receipt(), before);
        let mut original_policy = super::super::adapter::source_publication_guard(
            None, Some(crate::CancellationToken::new()),
        );
        caller.cancel();
        assert!(matches!(original_policy(&original).await, Err(MountSourceError::NativeWork(_))));
        assert_eq!(original.receipt(), before);
        Ok(())
    }

    #[test]
    fn finite_opened_source_reads_keep_original_meter_identity_and_cancellation()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        std::fs::write(source.path().join("file"), b"abcdefgh")?;
        let runtime = tokio::runtime::Runtime::new()?;
        let demand = Arc::new(Counted::new(runtime.block_on(native_source(source.path()))?, false));
        let view = runtime.block_on(mounted_view_of(Arc::clone(&demand), "finite-open-source"))?;
        let handle = view.open_file(&mounted(&["file"]))?;
        let opens = demand.opens.load(Ordering::Acquire);
        let caller = crate::CancellationToken::new();
        let meter = native_control_work(finite_content_budget(), &caller)?.native_read_meter().clone();
        let context = crate::NativeMaterializationWork::from_meter(Arc::clone(&meter));
        let identity = handle.lookup_with_meter(&context)?.node.file_id;
        assert_eq!(handle.read_range_with_meter(2, 3, &context)?.bytes.as_ref(), b"cde");
        assert_eq!(handle.read_up_to_with_meter(6, 8, &context)?.bytes.as_ref(), b"gh");
        assert_eq!(handle.seek_with_meter(2, MountSeekTarget::Data, &context)?, Some(2));
        assert_eq!(handle.seek_with_meter(2, MountSeekTarget::Hole, &context)?, Some(8));
        assert!(handle.read_attribute(b"absent", &context)?.is_none());
        assert!(handle.list_attributes_with_meter(None, 2, &context)?.0.names.is_empty());
        handle.settle(&context)?;
        let (sparse, owned) = handle.read_sparse_range(1, 3, &context)?;
        assert_eq!(sparse.logical_bytes, 3);
        assert_eq!(sparse.spans.len(), 1);
        assert_eq!(sparse.spans[0].relative_offset, 0);
        assert_eq!(sparse.spans[0].bytes.as_ref(), b"bcd");
        drop(sparse);
        drop(owned);
        assert_eq!(meter.lock().work.source_bytes_read, 8);
        assert_eq!(demand.opens.load(Ordering::Acquire), opens);
        std::fs::write(source.path().join("replacement"), b"other identity")?;
        std::fs::rename(source.path().join("replacement"), source.path().join("file"))?;
        let facts = handle.lookup_with_meter(&context)?;
        assert_eq!(facts.node.file_id, identity);
        assert_eq!(facts.node.logical_bytes, 8);
        caller.cancel();
        let failure = handle.read_range_with_meter(0, 1, &context).expect_err("same caller cancellation");
        assert!(matches!(failure.semantic_error(), MountSourceError::Cancelled));
        assert_eq!(meter.lock().work.source_bytes_read, 8);
        assert_eq!(demand.opens.load(Ordering::Acquire), opens);
        Ok(())
    }

    #[test]
    fn finite_opened_source_write_promotes_the_opened_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        std::fs::write(source.path().join("file"), b"abcdefgh")?;
        let runtime = tokio::runtime::Runtime::new()?;
        let view = runtime.block_on(mounted_view(source.path(), "finite-open-promotion"))?;
        let path = mounted(&["file"]);
        let handle = view.open_file(&path)?;
        let mut budget = finite_content_budget();
        budget.authority_records_appended = 10_000;
        budget.authority_bytes_written = 8_000_000;
        budget.backend_write_operations = 10_000;
        budget.durability_operations = 10_000;
        budget.page_writes = 10_000;
        budget.object_bytes_written = 8_000_000;
        budget.materializations = 100;
        let caller = crate::CancellationToken::new();
        let meter = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(
            crate::demand::DemandWork::new(budget, &caller),
        )));
        let context = crate::NativeMaterializationWork::from_meter(Arc::clone(&meter));
        let identity = handle.lookup_with_meter(&context)?.node.file_id;
        handle.write_range_with_meter(
            2, crate::kernel::OwnedSourceChunk::claimed(OwnedBytes::from_static(b"XY"), 0), &context,
        )?;
        assert_eq!(handle.lookup_with_meter(&context)?.node.file_id, identity);
        assert_eq!(handle.read_range_with_meter(0, 8, &context)?.bytes.as_ref(), b"abXYefgh");
        assert_eq!(std::fs::read(source.path().join("file"))?, b"abcdefgh");
        assert!(meter.lock().work.source_bytes_read >= 8);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn finite_linked_identity_claim_survives_handoff_until_cache_drop()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        std::fs::write(source.path().join("file"), b"linked")?;
        std::fs::hard_link(source.path().join("file"), source.path().join("alias"))?;
        let runtime = tokio::runtime::Runtime::new()?;
        let view = runtime.block_on(mounted_view(source.path(), "finite-linked-custody"))?;
        let caller = crate::CancellationToken::new();
        let meter = native_control_work(finite_content_budget(), &caller)?.native_read_meter().clone();
        let context = crate::NativeMaterializationWork::from_meter(Arc::clone(&meter));
        let lookup = view.lookup_pinned_with_meter(&mounted(&["file"]), &context)?.ok_or("missing linked file")?.0;
        let bytes = {
            let linked = view.linked.lock().map_err(|_| "linked authority poisoned")?;
            assert!(linked.values.binary_search(&lookup.node.file_id).is_ok());
            let mut work = meter.lock();
            assert!(linked.custody.as_ref().ok_or("missing custody")?.is_admitted(&mut work)?);
            (linked.values.capacity() * std::mem::size_of::<FileId>()) as u64
        };
        let before = {
            let mut work = meter.lock();
            work.check()?;
            work.live_allocation_bytes()
        };
        assert!(before >= bytes);
        drop(view);
        let after = {
            let mut work = meter.lock();
            work.check()?;
            work.live_allocation_bytes()
        };
        assert!(before - after >= bytes);
        Ok(())
    }

    #[test]
    fn shared_content_scope_uses_original_meter_outside_native_waits_and_sink()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        std::fs::write(source.path().join("file"), b"abcdefgh")?;
        let runtime = tokio::runtime::Runtime::new()?;
        let demand = Arc::new(Counted::new(runtime.block_on(native_source(source.path()))?, false));
        let view = runtime.block_on(mounted_view_of(Arc::clone(&demand), "shared-content-scope"))?;
        let link = mounted(&["link"]);
        super::super::adapter::boundary(view.authored.cancellation(), |work| view.create_symbolic_link(&link, crate::kernel::OwnedSourceChunk::claimed((Bytes::from_static(b"../target")).into(), 0), FileMetadata::default(), work))?;
        let caller = crate::CancellationToken::new();
        let meter = native_control_work(finite_content_budget(), &caller)?.native_read_meter().clone();
        let context = crate::NativeMaterializationWork::from_meter(Arc::clone(&meter));
        let path = mounted(&["file"]);
        let (_, pin) = view.lookup_pinned_with_meter(&path, &context)?.ok_or("missing source file")?;
        let pin = pin.ok_or("missing source content pin")?;
        let opened = demand.opens.load(Ordering::Acquire);
        let range = view.read_range_with_meter(&path, 2, 3, &context)?;
        assert_eq!(range.bytes.as_ref(), b"cde");
        assert!(range.logical_owned_bytes >= range.bytes.len() as u64);
        let target = view.read_link(&link, &context)?;
        assert_eq!(target.bytes.as_ref(), b"../target");
        let mut prefix = Vec::new();
        let result = view.read_pinned_with_meter(&path, pin, 0, 8, 2,
            &mut |offset, output| {
                assert!(meter.0.try_lock().is_ok(), "sink cannot run under the native work mutex");
                assert_eq!(offset, prefix.len() as u64);
                prefix.extend_from_slice(&output.bytes);
                caller.cancel();
                Ok(())
            }, &context,
        );
        assert!(matches!(result, Err(MountSourceError::Cancelled)));
        assert_eq!(prefix, b"ab");
        assert_eq!(demand.opens.load(Ordering::Acquire), opened + 2);
        assert_eq!(meter.lock().work.source_bytes_read, 5);
        let lookups = demand.lookups();
        let denied = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(
            crate::demand::DemandWork::new(
                crate::WorkBudget { allocation_operations: 0, ..finite_content_budget() },
                &crate::CancellationToken::new(),
            ),
        )));
        let denied_context = crate::NativeMaterializationWork::from_meter(denied);
        assert!(matches!(view.read_range_with_meter(&path, 0, 1, &denied_context),
            Err(MountSourceError::Work(_))));
        assert_eq!(demand.lookups(), lookups);
        Ok(())
    }

    #[test]
    fn finite_pinned_content_opens_once_and_preserves_sink_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        std::fs::write(source.path().join("file"), b"abcdefgh")?;
        let runtime = tokio::runtime::Runtime::new()?;
        let demand = Arc::new(Counted::new(runtime.block_on(native_source(source.path()))?, false));
        let view = runtime.block_on(mounted_view_of(Arc::clone(&demand), "finite-content-prefix"))?;
        let path = mounted(&["file"]);
        let caller = crate::CancellationToken::new();
        let mut lookup = native_control_work(finite_content_budget(), &caller)?;
        let (_, pin) = view.lookup_pinned_with_meter(
            &path, &lookup,
        )?.ok_or("missing source file")?;
        let pin = pin.ok_or("missing source content pin")?;
        let opened = demand.opens.load(Ordering::Acquire);
        let mut read = native_control_work(finite_content_budget(), &caller)?;
        let mut prefix = Vec::new();
        let mut pieces = 0;
        let result = view.read_pinned_with_meter(
            &path, pin, 0, 8, 2,
            &mut |offset, bytes| {
                assert_eq!(offset, prefix.len() as u64);
                assert!(bytes.bytes.len() <= 2);
                assert!(bytes.logical_owned_bytes >= bytes.bytes.len() as u64);
                if pieces == 2 { return Err(MountSourceError::Stale); }
                prefix.extend_from_slice(&bytes.bytes);
                pieces += 1;
                Ok(())
            },
            &read,
        );
        assert!(matches!(result, Err(MountSourceError::Stale)));
        assert_eq!(prefix, b"abcd");
        assert_eq!(demand.opens.load(Ordering::Acquire), opened + 1);
        assert_eq!(read.receipt().source_bytes_read, 6);
        assert!(read.receipt().backend_read_operations > 0);
        assert!(read.receipt().allocation_operations > 0);
        Ok(())
    }

    #[test]
    fn finite_pinned_content_keeps_original_cancellation_and_version()
    -> Result<(), Box<dyn std::error::Error>> {
        for cancel in [false, true] {
            let source = tempfile::tempdir()?;
            let native_path = source.path().join("file");
            std::fs::write(&native_path, b"abcdefgh")?;
            let runtime = tokio::runtime::Runtime::new()?;
            let demand = Arc::new(Counted::new(runtime.block_on(native_source(source.path()))?, false));
            let view = runtime.block_on(mounted_view_of(Arc::clone(&demand),
                if cancel { "finite-content-cancel" } else { "finite-content-version" }))?;
            let path = mounted(&["file"]);
            let caller = crate::CancellationToken::new();
            let mut lookup = native_control_work(finite_content_budget(), &caller)?;
            let (_, pin) = view.lookup_pinned_with_meter(
                &path, &lookup,
            )?.ok_or("missing source file")?;
            let pin = pin.ok_or("missing source content pin")?;
            let opened = demand.opens.load(Ordering::Acquire);
            let mut work = native_control_work(finite_content_budget(), &caller)?;
            let mut prefix = Vec::new();
            let result = view.read_pinned_with_meter(&path, pin, 0, 8, 2,
                &mut |offset, bytes| {
                    assert_eq!(offset, prefix.len() as u64);
                    prefix.extend_from_slice(&bytes.bytes);
                    if cancel { caller.cancel(); }
                    else { std::fs::write(&native_path, b"replacement")
                        .map_err(|error| MountSourceError::Engine(std::borrow::Cow::Owned(error.to_string())))?; }
                    Ok(())
                },
                &work,
            );
            if cancel { assert!(matches!(result, Err(MountSourceError::Cancelled))); }
            else { assert!(matches!(result, Err(MountSourceError::Stale))); }
            assert_eq!(prefix, b"ab");
            assert_eq!(demand.opens.load(Ordering::Acquire), opened + 1);
            assert_eq!(work.receipt().source_bytes_read, 2);
        }
        Ok(())
    }

    #[test]
    #[cfg(unix)]
    fn finite_source_link_reads_exact_target_without_content_open()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        std::os::unix::fs::symlink("../target", source.path().join("link"))?;
        let runtime = tokio::runtime::Runtime::new()?;
        let demand = Arc::new(Counted::new(runtime.block_on(native_source(source.path()))?, false));
        let view = runtime.block_on(mounted_view_of(Arc::clone(&demand), "finite-source-link"))?;
        let caller = crate::CancellationToken::new();
        let mut work = native_control_work(finite_content_budget(), &caller)?;
        let opened = demand.opens.load(Ordering::Acquire);
        let bytes = view.read_link(
            &mounted(&["link"]), &work,
        )?;
        assert_eq!(bytes.bytes.as_ref(), b"../target");
        assert_eq!(work.receipt().source_bytes_read, bytes.bytes.len() as u64);
        assert_eq!(demand.opens.load(Ordering::Acquire), opened);
        assert!(work.receipt().backend_read_operations > 0);
        assert!(work.receipt().allocation_operations > 0);
        Ok(())
    }

    #[test]
    fn finite_range_and_authored_link_read_actual_owned_payloads()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        std::fs::write(source.path().join("file"), b"abcdefgh")?;
        let runtime = tokio::runtime::Runtime::new()?;
        let demand = Arc::new(Counted::new(runtime.block_on(native_source(source.path()))?, false));
        let view = runtime.block_on(mounted_view_of(Arc::clone(&demand), "finite-content-payloads"))?;
        let caller = crate::CancellationToken::new();
        let opened = demand.opens.load(Ordering::Acquire);
        let mut range = native_control_work(finite_content_budget(), &caller)?;
        let bytes = view.read_range_with_meter(
            &mounted(&["file"]), 2, 3, &range,
        )?;
        assert_eq!(bytes.bytes.as_ref(), b"cde");
        assert!(bytes.logical_owned_bytes >= bytes.bytes.len() as u64);
        assert_eq!(range.receipt().source_bytes_read, 3);
        assert_eq!(demand.opens.load(Ordering::Acquire), opened + 1);
        let link = mounted(&["authored-link"]);
        let target = Bytes::from_static(b"../opaque-target\xff");
        super::super::adapter::boundary(view.authored.cancellation(), |work| view.create_symbolic_link(&link, crate::kernel::OwnedSourceChunk::claimed((target.clone()).into(), 0), FileMetadata::default(), work))?;
        let lookups = demand.lookups();
        let opened = demand.opens.load(Ordering::Acquire);
        let mut read = native_control_work(finite_content_budget(), &caller)?;
        let bytes = view.read_link(
            &link, &read,
        )?;
        assert_eq!(bytes.bytes.as_ref(), target.as_ref());
        assert_eq!(demand.lookups(), lookups);
        assert_eq!(demand.opens.load(Ordering::Acquire), opened);
        assert_eq!(read.receipt().source_bytes_read, 0);
        assert!(read.receipt().backend_read_operations > 0);
        assert!(read.receipt().items_examined > 0);
        Ok(())
    }

    #[test]
    fn finite_content_read_denies_before_source_and_retains_not_found_work()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        std::fs::write(source.path().join("file"), b"abcdefgh")?;
        let runtime = tokio::runtime::Runtime::new()?;
        let demand = Arc::new(Counted::new(runtime.block_on(native_source(source.path()))?, false));
        let view = runtime.block_on(mounted_view_of(Arc::clone(&demand), "finite-content-denial"))?;
        let caller = crate::CancellationToken::new();
        let original = native_control_work(finite_content_budget(), &caller)?;
        let denied = original.native_volume_work(
            crate::WorkBudget { allocation_operations: 0, ..finite_content_budget() },
        )?;
        let lookups = demand.lookups();
        let opens = demand.opens.load(Ordering::Acquire);
        assert!(matches!(view.read_range_with_meter(
            &mounted(&["file"]), 0, 2, &denied,
        ), Err(error) if matches!(error.semantic_error(),
            MountSourceError::Work(_) | MountSourceError::NativeWork(_)
            | MountSourceError::Filesystem(crate::FsError::NativeKernel(_)))));
        assert_eq!(demand.lookups(), lookups);
        assert_eq!(demand.opens.load(Ordering::Acquire), opens);
        let mut absent = native_control_work(finite_content_budget(), &caller)?;
        assert!(view.lookup_pinned_with_meter(
            &mounted(&["absent"]), &absent,
        )?.is_none());
        assert!(absent.receipt().backend_read_operations > 0);
        assert!(absent.receipt().items_examined > 0);
        Ok(())
    }

    #[test]
    fn lookups_follow_changes_made_to_the_source_outside_the_view()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        std::fs::write(source.path().join("a"), b"short")?;
        std::fs::create_dir(source.path().join("d"))?;
        let runtime = tokio::runtime::Runtime::new()?;
        let view = runtime.block_on(mounted_view(source.path(), "source-changes"))?;
        assert!(view.view_stamp().is_some(), "a local source is watched");
        let (a, d) = (mounted(&["a"]), mounted(&["d"]));
        let (created, beneath) = (mounted(&["d", "x"]), mounted(&["gone", "x"]));
        let size = |path: &MountPath| -> Result<Option<u64>, MountSourceError> {
            Ok(view.lookup(path)?.map(|lookup| lookup.node.logical_bytes))
        };
        for _ in 0..2 {
            assert_eq!(size(&a)?, Some(5));
            assert_eq!(size(&created)?, None);
            assert_eq!(size(&beneath)?, None);
        }

        std::fs::write(source.path().join("a"), b"much longer")?;
        std::fs::write(source.path().join("d/x"), b"x")?;
        std::fs::create_dir(source.path().join("gone"))?;
        std::fs::write(source.path().join("gone/x"), b"xy")?;
        view.fence_changes()?;
        assert_eq!(size(&a)?, Some(11));
        assert_eq!(size(&created)?, Some(1));
        assert_eq!(size(&beneath)?, Some(2));
        let listed = view.read_directory(&d, None, 64)?;
        assert_eq!(
            listed
                .entries
                .iter()
                .map(|entry| entry.name.clone())
                .collect::<Vec<_>>(),
            vec![created.components()[1].clone()]
        );

        std::fs::remove_file(source.path().join("a"))?;
        view.fence_changes()?;
        assert_eq!(size(&a)?, None);
        Ok(())
    }

    struct Counted<D> {
        inner: D,
        lookups: AtomicU64,
        opens: AtomicU64,
        watched: bool,
        reports: Arc<Mutex<Vec<SourceChange>>>,
    }

    impl<D> Counted<D> {
        fn new(inner: D, watched: bool) -> Self {
            Self {
                inner,
                lookups: AtomicU64::new(0),
                opens: AtomicU64::new(0),
                watched,
                reports: Arc::default(),
            }
        }

        fn lookups(&self) -> u64 {
            self.lookups.load(Ordering::Acquire)
        }

        #[cfg(target_os = "macos")]
        fn report_count(&self) -> usize {
            self.reports
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .len()
        }

        #[cfg(target_os = "macos")]
        fn reports_cover(&self, names: &[&[u8]]) -> bool {
            let reports = self.reports.lock().unwrap_or_else(PoisonError::into_inner);
            if reports
                .iter()
                .any(|change| matches!(change, SourceChange::Everything))
            {
                return true;
            }
            names.iter().all(|name| {
                reports.iter().any(|change| {
                    let (SourceChange::Name(path) | SourceChange::Entry(path)) = change else {
                        return false;
                    };
                    path.components()
                        .last()
                        .is_some_and(|component| component.as_bytes() == *name)
                })
            })
        }
    }

    struct CountedSink {
        inner: Arc<dyn SourceChangeSink>,
        reports: Arc<Mutex<Vec<SourceChange>>>,
    }

    impl SourceChangeSink for CountedSink {
        fn source_changed(
            &self,
            changes: &[SourceChange],
            budget: crate::WorkBudget,
            cancellation: &crate::CancellationToken,
        ) -> crate::demand::DemandResult<()> {
            let mut work = crate::demand::DemandWork::new(budget, cancellation);
            let result = (|| {
                work.check().map_err(mount_demand_error)?;
                let mut reports = self.reports.lock().unwrap_or_else(PoisonError::into_inner);
                let length = reports.len().checked_add(changes.len())
                    .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
                if length > reports.capacity() {
                    let growth = (length - reports.capacity()).checked_mul(std::mem::size_of::<SourceChange>())
                        .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
                    work.allocate(growth).map_err(mount_demand_error)?;
                    let additional = changes.len();
                    if reports.try_reserve_exact(additional).is_err() {
                        work.release(growth).map_err(mount_demand_error)?;
                        return Err(MountSourceError::Engine(std::borrow::Cow::Owned("counted change allocation failed".to_owned())));
                    }
                }
                for change in changes {
                    mount_items(&mut work, 1)?;
                    let owned = match change {
                        SourceChange::Name(path) | SourceChange::Entry(path) => {
                            let (_, ledger, counters, budget) = work.native_mount_admission_parts()
                                .map_err(mount_demand_error)?;
                            let copied = path.clone_with_ledger(ledger, counters, budget, cancellation)
                                .map_err(|error| match error {
                                    crate::kernel::NamespacePathError::Work(error) => MountSourceError::Work(error),
                                    crate::kernel::NamespacePathError::Cancelled => MountSourceError::Cancelled,
                                    error => MountSourceError::Invalid(std::borrow::Cow::Owned(error.to_string())),
                                })?;
                            if matches!(change, SourceChange::Name(_)) { SourceChange::Name(copied) }
                            else { SourceChange::Entry(copied) }
                        }
                        SourceChange::Node(identity) => {
                            work.charge(crate::WorkCounters {
                                bytes_copied: identity.len() as u64, ..crate::WorkCounters::default()
                            }).map_err(mount_demand_error)?;
                            SourceChange::Node(*identity)
                        }
                        SourceChange::Everything => SourceChange::Everything,
                        SourceChange::Unconfirmed => SourceChange::Unconfirmed,
                    };
                    reports.push(owned);
                }
                drop(reports);
                let remaining = work.remaining().map_err(mount_demand_error)?;
                mount_accept(&mut work, self.inner.source_changed(changes, remaining, cancellation), mount_demand_error)
            })();
            result.map(|value| crate::OperationReceipt { value, work: work.work })
                .map_err(|error| crate::OperationFailure::new(source_callback_error(error), work.work))
        }
    }

    impl<D: DemandSource> DemandSource for Counted<D> {
        fn reference(&self) -> SourceReference {
            self.inner.reference()
        }

        fn watch<W: crate::facade::FsWork + Send>(
            &self,
            sink: Arc<dyn SourceChangeSink>,
            funding: W,
            cancellation: &crate::CancellationToken,
        ) -> W::Output<Option<Box<dyn SourceWatch>>, DemandError> {
            let mut accounting = funding.accounting(cancellation);
            let result = (|| {
                let budget = crate::demand::ordinary_source_budget(&accounting)?;
                let answer = (|| {
                    let mut work = crate::demand::DemandWork::new(budget, cancellation);
                    let result = (|| {
                        work.check().map_err(mount_demand_error)?;
                        if !self.watched { return Ok(None); }
                        work.allocate(std::mem::size_of::<CountedSink>() as u64).map_err(mount_demand_error)?;
                        let sink = Arc::new(CountedSink { inner: sink, reports: Arc::clone(&self.reports) });
                        let remaining = work.remaining().map_err(mount_demand_error)?;
                        mount_accept(&mut work, self.inner.watch(sink, remaining, cancellation), mount_demand_error)
                })();
            result.map(|value| crate::OperationReceipt { value, work: work.work })
                .map_err(|error| crate::OperationFailure::new(source_callback_error(error), work.work))
        })();
                crate::demand::accept_source_result(answer, &mut accounting)
            })();
            funding.finish(result, &accounting)
        }

        async fn lookup<W: crate::facade::FsWork + Send>(
            &self,
            source: SourceReference,
            path: &crate::kernel::NamespacePath,
            work: W,
            cancellation: &crate::CancellationToken,
        ) -> W::Output<Option<SourceNode>, DemandError> {
            self.lookups.fetch_add(1, Ordering::AcqRel);
            self.inner.lookup(source, path, work, cancellation).await
        }

        async fn list_page<W: crate::facade::FsWork + Send>(
            &self,
            source: SourceReference,
            directory: &crate::kernel::NamespacePath,
            cursor: Option<crate::demand::SourceCursor>,
            maximum_entries: u32,
            work: W,
            cancellation: &crate::CancellationToken,
        ) -> W::Output<crate::demand::SourceDirectoryPage, DemandError> {
            self.inner
                .list_page(source, directory, cursor, maximum_entries, work, cancellation)
                .await
        }

        async fn read_range<W: crate::facade::FsWork + Send>(
            &self,
            source: SourceReference,
            path: &crate::kernel::NamespacePath,
            expected: crate::demand::SourceVersion,
            offset: u64,
            length: u64,
            work: W,
            cancellation: &crate::CancellationToken,
        ) -> W::Output<crate::kernel::OwnedSourceChunk, DemandError> {
            self.inner
                .read_range(source, path, expected, offset, length, work, cancellation)
                .await
        }

        async fn open_file<W: crate::facade::FsWork + Send>(
            &self,
            source: SourceReference,
            path: &crate::kernel::NamespacePath,
            expected: crate::demand::SourceVersion,
            work: W,
            cancellation: &crate::CancellationToken,
        ) -> W::Output<Box<dyn DemandFile>, DemandError> {
            self.opens.fetch_add(1, Ordering::AcqRel);
            self.inner
                .open_file(source, path, expected, work, cancellation)
                .await
        }

        async fn read_link<W: crate::facade::FsWork + Send>(
            &self,
            source: SourceReference,
            path: &crate::kernel::NamespacePath,
            expected: crate::demand::SourceVersion,
            maximum_bytes: u64,
            work: W,
            cancellation: &crate::CancellationToken,
        ) -> W::Output<crate::kernel::OwnedSourceChunk, DemandError> {
            self.inner
                .read_link(source, path, expected, maximum_bytes, work, cancellation)
                .await
        }
    }

    #[test]
    fn a_watched_source_is_read_again_only_for_what_it_reports_changed()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        let runtime = tokio::runtime::Runtime::new()?;
        let demand = Arc::new(Counted::new(
            runtime.block_on(native_source(source.path()))?,
            true,
        ));
        let view = runtime.block_on(mounted_view_of(Arc::clone(&demand), "watched-reads"))?;
        let (a, b, absent) = (mounted(&["a"]), mounted(&["b"]), mounted(&["absent"]));
        let size = |path: &MountPath| -> Result<Option<u64>, MountSourceError> {
            Ok(view.lookup(path)?.map(|lookup| lookup.node.logical_bytes))
        };

        std::fs::write(source.path().join("a"), b"short")?;
        std::fs::write(source.path().join("b"), b"other")?;
        #[cfg(target_os = "macos")]
        {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            let mut reports = demand.report_count();
            let mut quiet = 0;
            while quiet < 10 || !demand.reports_cover(&[b"a", b"b"]) {
                assert!(
                    std::time::Instant::now() < deadline,
                    "the startup notifications never settled"
                );
                std::thread::sleep(std::time::Duration::from_millis(10));
                let current = demand.report_count();
                if current == reports {
                    quiet += 1;
                } else {
                    reports = current;
                    quiet = 0;
                }
            }
        }
        #[cfg(not(target_os = "macos"))]
        view.fence_changes()?;

        assert_eq!(size(&a)?, Some(5));
        assert_eq!(size(&b)?, Some(5));
        assert_eq!(size(&absent)?, None);
        let read = demand.lookups();
        for _ in 0..3 {
            assert_eq!(size(&a)?, Some(5));
            assert_eq!(size(&b)?, Some(5));
            assert_eq!(size(&absent)?, None);
        }
        assert_eq!(demand.lookups(), read, "remembered answers read the source");

        std::fs::write(source.path().join("a"), b"much longer")?;
        std::fs::write(source.path().join("absent"), b"present")?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while (size(&a)?, size(&absent)?) != (Some(11), Some(7)) {
            assert!(
                std::time::Instant::now() < deadline,
                "the changes were never reported"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let reread = demand.lookups();
        assert!(reread > read, "a reported change reads the source again");
        assert_eq!(size(&b)?, Some(5));
        #[cfg(not(target_os = "macos"))]
        assert_eq!(
            demand.lookups(),
            reread,
            "an unchanged name is not read again"
        );
        Ok(())
    }

    #[test]
    fn a_source_that_lost_changes_is_read_again_in_full() -> Result<(), Box<dyn std::error::Error>>
    {
        let source = tempfile::tempdir()?;
        std::fs::write(source.path().join("a"), b"short")?;
        let runtime = tokio::runtime::Runtime::new()?;
        let demand = Arc::new(Counted::new(
            runtime.block_on(native_source(source.path()))?,
            true,
        ));
        let view = runtime.block_on(mounted_view_of(Arc::clone(&demand), "lost-changes"))?;
        let a = mounted(&["a"]);
        assert!(view.lookup(&a)?.is_some());
        let stamp = view.view_stamp().ok_or("a watched view carries stamps")?;
        let read = demand.lookups();
        SourceChangeRecorder {
            lazy: Arc::downgrade(&view.lazy),
            authored: Arc::downgrade(&view.authored),
            constructor_inputs: Mutex::new(None),
        }
        .source_changed(&[SourceChange::Everything], crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .map_err(|failure| failure.error)?;
        assert!(!view.unchanged_since(&a, None, stamp));
        assert!(view.lookup(&a)?.is_some());
        assert!(demand.lookups() > read, "everything is read again");
        Ok(())
    }

    async fn live_lazy_mount(
        source: &Path,
        destination: &Path,
        name: &str,
    ) -> Result<
        super::super::LazyMount<
            impl AsyncAuthorityStore + Send + Sync + 'static,
            impl AsyncObjectStore + Send + Sync + 'static,
            crate::demand::native::NativeDemandSource,
            crate::MemoryLazyWorkspaceStore,
        >,
        Box<dyn std::error::Error>,
    > {
        live_lazy_mount_of(open_native_source(source).await?, destination, name).await
    }

    #[cfg(unix)]
    #[derive(Clone, Copy, Debug)]
    enum Watched {
        Yes,
        No,
    }

    #[cfg(unix)]
    async fn live_lazy_mount_over(
        source: &Path,
        destination: &Path,
        name: &str,
        watched: Watched,
    ) -> Result<
        super::super::LazyMount<
            impl AsyncAuthorityStore + Send + Sync + 'static,
            impl AsyncObjectStore + Send + Sync + 'static,
            crate::demand::native::NativeDemandSource,
            crate::MemoryLazyWorkspaceStore,
        >,
        Box<dyn std::error::Error>,
    > {
        let demand = open_native_source(source).await?;
        let demand = match watched {
            Watched::Yes => demand,
            Watched::No => demand.unwatched(),
        };
        live_lazy_mount_of(demand, destination, name).await
    }

    async fn open_native_source(
        source: &Path,
    ) -> Result<crate::demand::native::NativeDemandSource, Box<dyn std::error::Error>> {
        let profile = if cfg!(windows) {
            crate::model::FilesystemProfile::Windows
        } else {
            crate::model::FilesystemProfile::Posix
        };
        Ok(crate::demand::native::NativeDemandSource::open(
            std::borrow::Cow::Owned(std::path::Path::new(source).to_path_buf()),
            profile,
            crate::model::VolumeLimits::default(), None, None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
        )
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?)
    }

    async fn live_lazy_mount_of(
        demand: crate::demand::native::NativeDemandSource,
        destination: &Path,
        name: &str,
    ) -> Result<
        super::super::LazyMount<
            impl AsyncAuthorityStore + Send + Sync + 'static,
            impl AsyncObjectStore + Send + Sync + 'static,
            crate::demand::native::NativeDemandSource,
            crate::MemoryLazyWorkspaceStore,
        >,
        Box<dyn std::error::Error>,
    > {
        use crate::model::{Lifecycle, VolumeConfig};
        use crate::native_mount::{MountOptions, MountPublication};
        use crate::{Fs, MemoryLazyWorkspaceStore};

        let lazy = LazyWorkspace::attach_with_config(
            &Fs::memory(),
            name,
            Arc::new(demand),
            MemoryLazyWorkspaceStore::default(),
            VolumeConfig::native(Lifecycle::Ephemeral),
        )
        .await?;
        std::fs::create_dir_all(destination)?;
        Ok(lazy
            .mount(
                destination,
                MountOptions::read_write().publication(MountPublication::Manual),
            )
            .await?)
    }

    fn sorted_names(directory: &Path) -> std::io::Result<Vec<std::ffi::OsString>> {
        let mut names = std::fs::read_dir(directory)?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<Result<Vec<_>, _>>()?;
        names.sort();
        Ok(names)
    }

    fn is_absent(path: &Path) -> bool {
        std::fs::symlink_metadata(path)
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "mounts a live native session; requires the host's native mount capability"]
    async fn live_mount_shows_changes_made_to_its_source() -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempfile::tempdir()?;
        let (source, mount) = (root.path().join("source"), root.path().join("mount"));
        std::fs::create_dir_all(source.join("d"))?;
        std::fs::write(source.join("a"), b"short")?;
        let live = live_lazy_mount(&source, &mount, "live-source-changes").await?;
        // Let the kernel cache every fact the changes below supersede.
        for _ in 0..2 {
            assert_eq!(std::fs::metadata(mount.join("a"))?.len(), 5);
            assert_eq!(std::fs::read(mount.join("a"))?, b"short");
            assert!(sorted_names(&mount.join("d"))?.is_empty());
            assert!(is_absent(&mount.join("d").join("x")));
            assert!(is_absent(&mount.join("gone")));
        }

        std::fs::write(source.join("a"), b"much longer")?;
        std::fs::write(source.join("d").join("x"), b"x")?;
        std::fs::create_dir(source.join("gone"))?;
        std::fs::write(source.join("gone").join("x"), b"xy")?;
        live.revalidate()?;
        assert_eq!(std::fs::metadata(mount.join("a"))?.len(), 11);
        assert_eq!(std::fs::read(mount.join("a"))?, b"much longer");
        assert_eq!(sorted_names(&mount.join("d"))?, ["x"]);
        assert_eq!(std::fs::metadata(mount.join("d").join("x"))?.len(), 1);
        assert_eq!(std::fs::read(mount.join("gone").join("x"))?, b"xy");

        std::fs::remove_file(source.join("a"))?;
        std::fs::rename(source.join("gone"), source.join("moved"))?;
        live.revalidate()?;
        assert!(is_absent(&mount.join("a")));
        assert!(is_absent(&mount.join("gone")));
        assert_eq!(std::fs::read(mount.join("moved").join("x"))?, b"xy");

        // Without revalidation, the source's report alone shows a change.
        std::fs::write(source.join("d").join("x"), b"reported")?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while std::fs::metadata(mount.join("d").join("x"))?.len() != 8 {
            assert!(
                std::time::Instant::now() < deadline,
                "a reported change never became visible"
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert_eq!(std::fs::read(mount.join("d").join("x"))?, b"reported");
        live.unmount().await?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "mounts a live native session; requires the host's native mount capability"]
    async fn live_mount_keeps_a_reused_identity_apart() -> Result<(), Box<dyn std::error::Error>> {
        for watched in [Watched::Yes, Watched::No] {
            keeps_a_reused_identity_apart(watched).await?;
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    async fn keeps_a_reused_identity_apart(
        watched: Watched,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};

        let root = tempfile::tempdir()?;
        let (source, mount) = (root.path().join("source"), root.path().join("mount"));
        std::fs::create_dir_all(&source)?;
        std::fs::write(source.join("removed"), b"old")?;
        let live = live_lazy_mount_over(&source, &mount, "live-reused-identity", watched).await?;
        assert_eq!(std::fs::metadata(mount.join("removed"))?.len(), 3);
        // Holds the removed name's inode without a file handle.
        let held = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_PATH)
            .open(mount.join("removed"))?;
        let identity = std::fs::metadata(source.join("removed"))?.ino();
        std::fs::remove_file(source.join("removed"))?;
        let mut reused = false;
        for index in 0..256 {
            let candidate = source.join(format!("created-{index}"));
            std::fs::write(&candidate, b"new file")?;
            if std::fs::metadata(&candidate)?.ino() == identity {
                std::fs::rename(&candidate, source.join("created"))?;
                reused = true;
                break;
            }
            std::fs::remove_file(&candidate)?;
        }
        if !reused {
            eprintln!("the host did not reuse the identity ({watched:?}); nothing to check");
            live.unmount().await?;
            return Ok(());
        }
        live.revalidate()?;
        let created = std::fs::metadata(mount.join("created"))?;
        assert_eq!(created.len(), 8);
        let mode = std::fs::metadata(source.join("created"))?
            .permissions()
            .mode();
        // A mode change through the held inode must not reach the new file.
        let _ = std::fs::set_permissions(
            format!("/proc/self/fd/{}", std::os::fd::AsRawFd::as_raw_fd(&held)),
            std::fs::Permissions::from_mode(0o600),
        );
        assert_eq!(
            std::fs::metadata(source.join("created"))?
                .permissions()
                .mode(),
            mode
        );
        assert_eq!(
            std::fs::metadata(mount.join("created"))?
                .permissions()
                .mode()
                & 0o777,
            mode & 0o777
        );
        drop(held);
        live.unmount().await?;
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "mounts a live native session; requires the host's native mount capability"]
    async fn live_mount_keeps_hard_links_one_file() -> Result<(), Box<dyn std::error::Error>> {
        for watched in [Watched::Yes, Watched::No] {
            keeps_hard_links_one_file(watched).await?;
        }
        Ok(())
    }

    #[cfg(unix)]
    async fn keeps_hard_links_one_file(watched: Watched) -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::MetadataExt as _;

        let root = tempfile::tempdir()?;
        let (source, mount) = (root.path().join("source"), root.path().join("mount"));
        let pairs = [("a", "d/a"), ("b", "d/b"), ("c", "l/c"), ("s/x", "s/y")];
        for directory in ["d", "l", "s"] {
            std::fs::create_dir_all(source.join(directory))?;
        }
        for (first, second) in pairs {
            std::fs::write(source.join(first), first.as_bytes())?;
            std::fs::hard_link(source.join(first), source.join(second))?;
        }
        let live = live_lazy_mount_over(&source, &mount, "live-hard-links", watched).await?;
        let same_file = |first: &str, second: &str| -> Result<(), Box<dyn std::error::Error>> {
            let _held = std::fs::File::open(mount.join(first))?;
            let (one, other) = (
                std::fs::metadata(mount.join(first))?,
                std::fs::metadata(mount.join(second))?,
            );
            assert_eq!(
                one.ino(),
                other.ino(),
                "{first} and {second} are one inode ({watched:?})"
            );
            assert_eq!(
                (one.nlink(), other.nlink()),
                (2, 2),
                "{first} and {second} ({watched:?})"
            );
            std::fs::write(mount.join(second), format!("through {second}"))?;
            assert_eq!(
                std::fs::read_to_string(mount.join(first))?,
                format!("through {second}")
            );
            std::fs::write(mount.join(first), format!("through {first}"))?;
            assert_eq!(
                std::fs::read_to_string(mount.join(second))?,
                format!("through {first}")
            );
            Ok(())
        };
        // One listing names both.
        assert_eq!(sorted_names(&mount.join("s"))?, ["x", "y"]);
        same_file("s/x", "s/y")?;
        // A listing names the inner one first.
        let listed = std::fs::read_dir(mount.join("l"))?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(listed, ["c"]);
        same_file("c", "l/c")?;
        // The outer name first, then the inner one.
        same_file("a", "d/a")?;
        // The inner name first, then the outer one.
        same_file("d/b", "b")?;
        // And every name still agrees after the source is revalidated.
        live.revalidate()?;
        for (first, second) in pairs {
            let _held = std::fs::File::open(mount.join(first))?;
            assert_eq!(
                std::fs::metadata(mount.join(first))?.ino(),
                std::fs::metadata(mount.join(second))?.ino(),
                "{first} and {second} after revalidation ({watched:?})"
            );
        }
        live.unmount().await?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "mounts a live native session; requires the host's native mount capability"]
    async fn live_mount_shows_writes_through_a_name_outside_its_source()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let (source, mount, outside) = (
            root.path().join("source"),
            root.path().join("mount"),
            root.path().join("outside"),
        );
        std::fs::create_dir_all(&source)?;
        std::fs::create_dir_all(&outside)?;
        std::fs::write(source.join("linked"), b"before")?;
        std::fs::hard_link(source.join("linked"), outside.join("alias"))?;
        let live = live_lazy_mount(&source, &mount, "live-linked-outside").await?;
        for _ in 0..3 {
            assert_eq!(std::fs::read(mount.join("linked"))?, b"before");
            assert_eq!(std::fs::metadata(mount.join("linked"))?.len(), 6);
        }
        // Written in place through the name outside the source: unreported.
        std::fs::OpenOptions::new()
            .write(true)
            .open(outside.join("alias"))
            .and_then(|mut file| std::io::Write::write_all(&mut file, b"AFTER!, longer"))?;
        assert_eq!(std::fs::metadata(mount.join("linked"))?.len(), 14);
        assert_eq!(std::fs::read(mount.join("linked"))?, b"AFTER!, longer");
        live.unmount().await?;
        Ok(())
    }

    #[cfg(not(target_os = "macos"))]
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "mounts a live native session; requires the host's native mount capability"]
    async fn live_mount_follows_concurrent_changes_to_its_source()
    -> Result<(), Box<dyn std::error::Error>> {
        const FILES: usize = 24;
        const WRITERS: usize = 4;
        const READERS: usize = 4;
        const CHANGES: usize = 150;

        fn version(name: usize, generation: usize) -> Vec<u8> {
            let mut bytes = format!("{name}:{generation}:").into_bytes();
            let length = 16 + (name * 7 + generation * 13) % 200;
            bytes.resize(length, b'.');
            let tail = format!(":{length}");
            bytes.truncate(length - tail.len());
            bytes.extend_from_slice(tail.as_bytes());
            bytes
        }
        fn whole(bytes: &[u8]) -> bool {
            std::str::from_utf8(bytes).is_ok_and(|text| {
                text.rsplit_once(':')
                    .and_then(|(_, length)| length.parse::<usize>().ok())
                    == Some(bytes.len())
            })
        }

        let root = tempfile::tempdir()?;
        let (source, mount) = (root.path().join("source"), root.path().join("mount"));
        std::fs::create_dir_all(&source)?;
        for name in 0..FILES {
            std::fs::write(source.join(format!("f{name}")), version(name, 0))?;
            std::fs::write(source.join(format!("g{name}")), version(name, 0))?;
        }
        let live = live_lazy_mount(&source, &mount, "live-concurrent-changes").await?;
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let torn = Arc::new(Mutex::new(Vec::new()));
        let readers = (0..READERS)
            .map(|reader| {
                let (mount, stop, torn) = (mount.clone(), Arc::clone(&stop), Arc::clone(&torn));
                std::thread::spawn(move || {
                    let mut turn = reader;
                    while !stop.load(Ordering::Acquire) {
                        turn += 1;
                        let replaced = mount.join(format!("f{}", turn % FILES));
                        let rewritten = mount.join(format!("g{}", turn % FILES));
                        let _ = std::fs::metadata(&replaced);
                        let _ = std::fs::metadata(&rewritten);
                        let _ = std::fs::read_dir(&mount).map(Iterator::count);
                        let _ = std::fs::read(&rewritten);
                        if let Ok(bytes) = std::fs::read(&replaced)
                            && !whole(&bytes)
                            && let Ok(mut torn) = torn.lock()
                        {
                            torn.push(String::from_utf8_lossy(&bytes).into_owned());
                        }
                    }
                })
            })
            .collect::<Vec<_>>();
        let writers = (0..WRITERS)
            .map(|writer| {
                let source = source.clone();
                std::thread::spawn(move || -> Result<(), String> {
                    let failed = |step: &str, path: &std::path::Path, error: std::io::Error| {
                        format!("{step} {}: {error}", path.display())
                    };
                    for change in 0..CHANGES {
                        let name = (writer + change * WRITERS) % FILES;
                        let path = source.join(format!("f{name}"));
                        match change % 3 {
                            // Replaced whole, as an editor saves.
                            0 | 2 => {
                                let staged = source.join(format!(".f{name}-{writer}"));
                                std::fs::write(&staged, version(name, change + 1))
                                    .map_err(|error| failed("staging", &staged, error))?;
                                std::fs::rename(&staged, &path)
                                    .map_err(|error| failed("renaming onto", &path, error))?;
                            }
                            _ => match std::fs::remove_file(&path) {
                                Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                                    return Err(failed("removing", &path, error));
                                }
                                _ => {}
                            },
                        }
                        let rewritten = source.join(format!("g{name}"));
                        std::fs::write(&rewritten, version(name, change + 1))
                            .map_err(|error| failed("rewriting", &rewritten, error))?;
                    }
                    Ok(())
                })
            })
            .collect::<Vec<_>>();
        for writer in writers {
            writer.join().map_err(|_| "writer panicked")??;
        }
        stop.store(true, Ordering::Release);
        for reader in readers {
            reader.join().map_err(|_| "reader panicked")?;
        }
        let torn = torn.lock().map_err(|_| "torn reads poisoned")?.clone();
        assert!(torn.is_empty(), "reads returned no whole version: {torn:?}");

        live.revalidate()?;
        assert_eq!(sorted_names(&mount)?, sorted_names(&source)?);
        for name in sorted_names(&source)? {
            let (native, mounted) = (source.join(&name), mount.join(&name));
            assert_eq!(
                std::fs::metadata(&mounted)?.len(),
                std::fs::metadata(&native)?.len(),
                "{name:?} differs in length"
            );
            assert_eq!(
                std::fs::read(&mounted)?,
                std::fs::read(&native)?,
                "{name:?} differs"
            );
        }
        live.unmount().await?;
        Ok(())
    }

    #[test]
    fn finite_subtree_rename_lists_whole_pages_before_promoting_and_keeps_source_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        std::fs::create_dir_all(source.path().join("tree/nested"))?;
        std::fs::write(source.path().join("tree/nested/leaf"), b"nested")?;
        for index in 0..=DIRECTORY_PROMOTION_PAGE {
            std::fs::write(source.path().join(format!("tree/file-{index:04}")), b"source")?;
        }
        let runtime = tokio::runtime::Runtime::new()?;
        let view = runtime.block_on(mounted_view(source.path(), "finite-subtree-rename"))?;
        let from = mounted(&["tree"]);
        let to = mounted(&["moved"]);
        let held = view.open_file(&mounted(&["tree", "nested", "leaf"]))?;
        let identity = held.lookup()?.node.file_id;
        let caller = crate::CancellationToken::new();
        let mut budget = finite_directory_budget();
        budget.authority_records_appended = 100_000;
        budget.authority_bytes_written = 64_000_000;
        budget.backend_write_operations = 100_000;
        budget.durability_operations = 100_000;
        budget.page_writes = 100_000;
        budget.object_bytes_written = 64_000_000;
        budget.bytes_copied = 64_000_000;
        budget.bytes_encoded = 64_000_000;
        budget.bytes_hashed = 64_000_000;
        budget.items_examined = 1_000_000;
        budget.peak_allocation_bytes = 64_000_000;
        budget.materializations = 10_000;
        let mut denied_budget = budget;
        denied_budget.allocation_operations = 0;
        let denied = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(
            crate::demand::DemandWork::new(denied_budget, &caller),
        )));
        let denied_context = crate::NativeMaterializationWork::from_meter(Arc::clone(&denied));
        assert!(matches!(
            view.rename(&from, &to, false, &denied_context),
            Err(MountSourceError::Work(_)),
        ));
        assert_eq!(denied.lock().work.allocation_operations, 0);
        assert_eq!(denied.lock().work.source_entries_visited, 0);
        assert!(view.authored.lookup(&to)?.is_none());
        let original = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(
            crate::demand::DemandWork::new(budget, &caller),
        )));
        let context = crate::NativeMaterializationWork::from_meter(Arc::clone(&original));
        view.rename(&from, &to, false, &context)?;
        assert!(view.lookup(&from)?.is_none());
        let moved = view.lookup(&mounted(&["moved", "nested", "leaf"]))?
            .ok_or("missing promoted nested leaf")?;
        assert_eq!(moved.node.file_id, identity);
        assert_eq!(held.lookup()?.node.file_id, identity);
        assert_eq!(held.read_range(0, 6)?.as_ref(), b"nested");
        for index in 0..=DIRECTORY_PROMOTION_PAGE {
            let name = format!("file-{index:04}");
            assert!(view.lookup(&mounted(&["moved", &name]))?.is_some());
            assert_eq!(std::fs::read(source.path().join("tree").join(name))?, b"source");
        }
        assert!(!source.path().join("moved").exists());
        assert_eq!(std::fs::read(source.path().join("tree/nested/leaf"))?, b"nested");
        assert!(original.lock().work.source_entries_visited > u64::from(DIRECTORY_PROMOTION_PAGE));
        assert!(original.lock().work.allocation_operations > 0);
        original.lock().work.verify(budget)?;
        Ok(())
    }

    #[test]
    fn shared_finite_directory_uses_original_meter_across_source_and_authored_pages()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        std::fs::write(source.path().join("a"), b"a")?;
        let runtime = tokio::runtime::Runtime::new()?;
        let view = runtime.block_on(mounted_view(source.path(), "shared-finite-directory"))?;
        view.native_call(|work| view.create_file(&mounted(&["b"]), FileMetadata::default(), None, work))?;
        let caller = crate::CancellationToken::new();
        let budget = finite_directory_budget();
        let meter = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(
            crate::demand::DemandWork::new(budget, &caller),
        )));
        let context = crate::NativeMaterializationWork::from_meter(Arc::clone(&meter));
        let mut cursor = None;
        let mut names = Vec::new();
        loop {
            let page = view.read_directory_with_meter(&MountPath::root(), cursor.as_deref(), 1, &context)?;
            assert!(!page.entries.is_empty() || page.next_cursor.is_none());
            for entry in page.entries { names.push(entry.name); }
            let Some(next) = page.next_cursor else { break; };
            cursor = Some(next);
        }
        names.sort();
        let mut expected = vec![mounted(&["a"]), mounted(&["b"])]
            .into_iter().map(|path| path.components().last().expect("child").clone()).collect::<Vec<_>>();
        expected.sort();
        assert_eq!(names, expected);
        let held = meter.lock();
        assert!(held.work.source_entries_visited > 0);
        assert!(held.work.items_examined > 0);
        assert!(held.work.allocation_operations > 0);
        held.work.verify(budget)?;
        drop(held);
        let mut denied_budget = budget;
        denied_budget.allocation_operations = 0;
        let denied_meter = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(
            crate::demand::DemandWork::new(denied_budget, &caller),
        )));
        let denied = crate::NativeMaterializationWork::from_meter(Arc::clone(&denied_meter));
        assert!(matches!(
            view.read_directory_with_meter(&MountPath::root(), None, 1, &denied),
            Err(MountSourceError::Work(_))
        ));
        assert_eq!(denied_meter.lock().work.source_entries_visited, 0);
        assert_eq!(denied_meter.lock().work.allocation_operations, 0);
        let stopped = crate::CancellationToken::new();
        stopped.cancel();
        let cancelled_meter = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(
            crate::demand::DemandWork::new(budget, &stopped),
        )));
        let cancelled = crate::NativeMaterializationWork::from_meter(Arc::clone(&cancelled_meter));
        assert!(matches!(
            view.read_directory_with_meter(&MountPath::root(), None, 1, &cancelled),
            Err(MountSourceError::Cancelled)
        ));
        assert_eq!(cancelled_meter.lock().work.source_entries_visited, 0);
        Ok(())
    }

    #[test]
    fn finite_directory_pages_merge_filter_and_reject_stale_continuations()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        std::fs::create_dir(source.path().join("empty"))?;
        std::fs::write(source.path().join("a"), b"a")?;
        std::fs::write(source.path().join("b"), b"b")?;
        let runtime = tokio::runtime::Runtime::new()?;
        let view = runtime.block_on(mounted_view(source.path(), "finite-directory-pages"))?;
        crate::native_mount::adapter::boundary(view.authored.cancellation(), |work| view.remove(&mounted(&["b"]), None, work))?;
        view.native_call(|work| view.create_file(&mounted(&["c"]), FileMetadata::default(), None, work))?;
        let caller = crate::CancellationToken::new();
        let budget = finite_directory_budget();
        assert!(budget.has_finite_allowances());
        let mut denied_budget = budget;
        denied_budget.allocation_operations = 0;
        let original = native_control_work(budget, &caller)?;
        let denied = original.native_volume_work(denied_budget)?;
        let denied_prefix = denied.receipt();
        assert!(matches!(
            view.read_directory_with_meter(
                &MountPath::root(), None, 1,
                &denied,
            ),
            Err(error) if matches!(error.semantic_error(),
                MountSourceError::Work(_) | MountSourceError::NativeWork(_)
                | MountSourceError::Filesystem(crate::FsError::NativeKernel(_)))
        ));
        assert_eq!(denied.receipt().allocation_operations, denied_prefix.allocation_operations);
        assert_eq!(denied.receipt().source_entries_visited, 0);
        let work = native_control_work(budget, &caller)?;
        let empty = view.read_directory_with_meter(
            &mounted(&["empty"]), None, 1, &work,
        )?;
        assert!(empty.entries.is_empty() && empty.next_cursor.is_none());
        let mut cursor = None;
        let mut names = Vec::new();
        loop {
            let page = view.read_directory_with_meter(
                &MountPath::root(), cursor.as_deref(), 1,
                &work,
            )?;
            assert!(!page.entries.is_empty() || page.next_cursor.is_none());
            for entry in page.entries { names.push(entry.name); }
            let Some(next) = page.next_cursor else { break; };
            cursor = Some(next);
        }
        names.sort();
        let mut expected = vec![mounted(&["a"]), mounted(&["c"]), mounted(&["empty"])]
            .into_iter().map(|path| path.components().last().expect("child").clone()).collect::<Vec<_>>();
        expected.sort();
        assert_eq!(names, expected);
        assert!(work.receipt().source_entries_visited > 0);
        assert!(work.receipt().allocation_operations > 0);
        work.receipt().verify(budget)?;

        let first = view.read_directory_with_meter(
            &MountPath::root(), None, 1, &work,
        )?;
        let next = first.next_cursor.expect("source phase has a continuation");
        view.native_call(|work| view.create_file(&mounted(&["d"]), FileMetadata::default(), None, work))?;
        assert!(matches!(
            view.read_directory_with_meter(
                &MountPath::root(), Some(&next), 1,
                &work,
            ),
            Err(MountSourceError::Stale)
        ));
        let stopped = crate::CancellationToken::new();
        let cancelled = native_control_work(budget, &stopped)?;
        stopped.cancel();
        assert!(matches!(
            view.read_directory_with_meter(
                &MountPath::root(), None, 1,
                &cancelled,
            ),
            Err(error) if matches!(error.semantic_error(), MountSourceError::Cancelled
                | MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Cancelled))
        ));
        Ok(())
    }

    #[test]
    fn an_empty_page_ends_its_listing() -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        std::fs::create_dir(source.path().join("empty"))?;
        std::fs::write(source.path().join("a"), b"a")?;
        let runtime = tokio::runtime::Runtime::new()?;
        let view = runtime.block_on(mounted_view(source.path(), "empty-pages"))?;
        let empty = view.read_directory(&mounted(&["empty"]), None, 64)?;
        assert!(empty.entries.is_empty());
        assert!(empty.next_cursor.is_none(), "an empty page continues");
        view.native_call(|work| view.create_file(&mounted(&["b"]), FileMetadata::default(), None, work))?;
        let mut cursor = None;
        let mut listed = 0;
        loop {
            let page = view.read_directory(&MountPath::root(), cursor.as_deref(), 1)?;
            assert!(
                !page.entries.is_empty() || page.next_cursor.is_none(),
                "an empty page continues"
            );
            listed += page.entries.len();
            let Some(next) = page.next_cursor else {
                break;
            };
            cursor = Some(next);
        }
        assert_eq!(listed, 3, "a, b, and empty");
        Ok(())
    }

    #[test]
    fn an_open_handle_reports_its_file_as_it_is_now() -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        std::fs::write(source.path().join("a"), b"short")?;
        let runtime = tokio::runtime::Runtime::new()?;
        let view = runtime.block_on(mounted_view(source.path(), "handle-facts"))?;
        let a = mounted(&["a"]);
        let handle = view.open_file(&a)?;
        assert_eq!(handle.lookup()?.node.logical_bytes, 5);
        let opened = handle.lookup()?.node.file_id;

        // Rewritten in place: the same file, with new facts.
        std::fs::write(source.path().join("a"), b"much longer")?;
        assert_eq!(handle.lookup()?.node.logical_bytes, 11);
        assert_eq!(handle.lookup()?.node.file_id, opened);

        // Replaced: the path names another file; the handle keeps its own.
        std::fs::write(source.path().join("b"), b"replacement!!")?;
        std::fs::rename(source.path().join("b"), source.path().join("a"))?;
        let facts = handle.lookup()?;
        assert_eq!(facts.node.file_id, opened);
        assert_eq!(facts.node.logical_bytes, 5);
        Ok(())
    }

    #[test]
    fn a_dropped_watched_view_releases_its_stores_at_once() -> Result<(), Box<dyn std::error::Error>>
    {
        use crate::model::{CheckoutMode, GenerationSelector};
        use crate::native_mount::{MountPublication, SharedCheckout};
        use crate::{Fs, LocalCoreStateStore};

        let source = tempfile::tempdir()?;
        std::fs::write(source.path().join("a"), b"a")?;
        let state = tempfile::tempdir()?;
        let runtime = tokio::runtime::Runtime::new()?;
        let fs = Fs::memory();
        for round in 0..24 {
            let store = LocalCoreStateStore::open_owned(state.path().join("core"))
                .map_err(|error| format!("round {round}: {error}"))?;
            let view = runtime.block_on(async {
                let lazy = Arc::new(
                    LazyWorkspace::attach(
                        &fs,
                        &format!("released-{round}"),
                        Arc::new(native_source(source.path()).await?),
                        store,
                    )
                    .await?,
                );
                let checkout = lazy
                    .workspace()
                    .checkout(
                        GenerationSelector::Head,
                        CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
                let config = checkout.volume_config();
                let authored = Arc::new(CheckoutMountSource::new(
                    Arc::new(SharedCheckout::with_publication(
                        checkout,
                        MountPublication::Manual,
                    )),
                    config,
                )?);
                Ok::<_, Box<dyn std::error::Error>>({ CallbackRuntime::create()?; let cancellation = authored.cancellation().clone(); let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation); LazyMountSource::new(lazy, authored, "/".to_owned(), super::super::adapter::NativeDemandCarrier::Borrowed(&mut work), None) }?)
            })?;
            assert!(view.view_stamp().is_some(), "a local source is watched");
            assert!(view.lookup(&mounted(&["a"]))?.is_some());
            // A report in delivery while the view drops.
            std::fs::write(source.path().join("a"), format!("round {round}"))?;
            drop(view);
        }
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn a_file_linked_outside_the_source_is_read_afresh() -> Result<(), Box<dyn std::error::Error>> {
        let parent = tempfile::tempdir()?;
        let (source, outside) = (parent.path().join("source"), parent.path().join("outside"));
        std::fs::create_dir_all(&source)?;
        std::fs::create_dir_all(&outside)?;
        std::fs::write(source.join("linked"), b"short")?;
        std::fs::hard_link(source.join("linked"), outside.join("alias"))?;
        std::fs::write(source.join("single"), b"single")?;
        let runtime = tokio::runtime::Runtime::new()?;
        let view = runtime.block_on(mounted_view(&source, "linked-outside"))?;
        let (linked_path, single) = (mounted(&["linked"]), mounted(&["single"]));
        let stamp = view.view_stamp().ok_or("a local source is watched")?;
        let linked_file = view.lookup(&linked_path)?.ok_or("linked")?.node;
        let single_file = view.lookup(&single)?.ok_or("single")?.node;
        assert_eq!(linked_file.logical_bytes, 5);
        assert!(!view.unchanged_since(&linked_path, Some(linked_file.file_id), stamp));
        assert!(!view.node_unchanged_since(linked_file.file_id, stamp));
        assert!(view.reports_changes_to(single_file.file_id));

        // Written in place through the name outside the source: unreported.
        std::fs::OpenOptions::new()
            .append(true)
            .open(outside.join("alias"))
            .and_then(|mut file| std::io::Write::write_all(&mut file, b" and longer"))?;
        assert_eq!(
            view.lookup(&linked_path)?
                .map(|lookup| lookup.node.logical_bytes),
            Some(16)
        );
        Ok(())
    }

    #[test]
    fn an_unwatched_source_is_read_afresh_and_never_cached()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        std::fs::write(source.path().join("a"), b"short")?;
        let runtime = tokio::runtime::Runtime::new()?;
        let demand = Arc::new(Counted::new(
            runtime.block_on(native_source(source.path()))?,
            false,
        ));
        let view = runtime.block_on(mounted_view_of(Arc::clone(&demand), "unwatched"))?;
        assert_eq!(view.view_stamp(), None, "an unwatched view is never cached");
        let (a, absent) = (mounted(&["a"]), mounted(&["absent"]));
        let size = |path: &MountPath| -> Result<Option<u64>, MountSourceError> {
            Ok(view.lookup(path)?.map(|lookup| lookup.node.logical_bytes))
        };
        assert_eq!(size(&a)?, Some(5));
        assert_eq!(size(&absent)?, None);
        std::fs::write(source.path().join("a"), b"much longer")?;
        std::fs::write(source.path().join("absent"), b"present")?;
        assert_eq!(size(&a)?, Some(11));
        assert_eq!(size(&absent)?, Some(7));
        Ok(())
    }

    #[test]
    fn a_promotion_changes_its_directory_listing() -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        std::fs::create_dir(source.path().join("d"))?;
        std::fs::write(source.path().join("d/f"), b"source")?;
        let runtime = tokio::runtime::Runtime::new()?;
        let view = runtime.block_on(mounted_view(source.path(), "listing-promotion"))?;
        let (d, f) = (mounted(&["d"]), mounted(&["d", "f"]));
        view.promote_locked(&d)?;
        let stamp = view.view_stamp().ok_or("the view has no stamp")?;
        view.promote_locked(&f)?;
        assert!(!view.unchanged_since(&d, None, stamp));
        Ok(())
    }
}
