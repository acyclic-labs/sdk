//! Composed bounded regular-file reads across inline and sparse representations.

use super::{BlobReadError, DecodeLimits, ExtentKind, ExtentRangeRequest, ExtentReadError,
    FileKind, FilePayload, FileRecord};
use super::allocation::{AllocationError, KernelAccounting, KernelOwnedAllocation};
use crate::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::foundation::usize_to_u64;
use crate::FsError;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::ByteRange;
use thiserror::Error;

/// Complete bounded request for one regular-file byte range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileRangeRequest {
    /// Authenticated file record selected by namespace lookup.
    pub record: FileRecord,
    /// Exact logical range returned.
    pub range: ByteRange,
    /// Maximum sparse spans admitted in the result plan.
    pub maximum_spans: u32,
    /// Canonical page and allocation limits.
    pub limits: DecodeLimits,
    /// Exact implementation-work ceiling.
    pub budget: WorkBudget,
}

/// Successful logical file range and exact physical work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileRangeRead {
    /// Exact requested bytes, with holes and allocated zeros represented as zero.
    pub bytes: acyclic_stream::OwnedBytes,
    /// Actual controlled output-buffer capacity retained by `bytes`.
    pub logical_owned_bytes: u64,
    /// Accounting-context snapshot of completed page, blob, copy, allocation, and output work.
    pub work: WorkCounters,
}

impl FileRangeRead {
    /// Exact logical payload capacity, measured by the producer before handoff.
    pub fn logical_owned_bytes(&self) -> u64 { self.logical_owned_bytes }
}

/// Reads one regular-file range without materializing untouched extents.
pub async fn read_file_range_async<S: AsyncObjectStore>(
    store: &S,
    request: FileRangeRequest,
    cancellation: &CancellationToken,
) -> Result<FileRangeRead, FileRangeReadFailure> {
    let mut accounting = KernelAccounting::standalone(request.budget, cancellation);
    read_file_range_with_accounting(store, request, &mut accounting, cancellation).await
        .map_err(|error| failed(file_error(error), accounting.spent()))
}

pub(crate) async fn read_file_range_with_accounting<S: AsyncObjectStore>(
    store: &S,
    request: FileRangeRequest,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<FileRangeRead, FsError> {
    accounting.check()?;
    if request.record.kind != FileKind::Regular {
        return Err(FileRangeReadError::NotRegular.into());
    }
    let completed = WorkCounters { output_bytes: request.range.length, items_returned: 1, ..WorkCounters::UNCHARGED };
    accounting.admit(completed)?;
    let output = match request.record.payload {
        FilePayload::InlineRegular(data) => {
            let end = request.range.offset.checked_add(request.range.length)
                .ok_or(FileRangeReadError::InvalidRange)?;
            let start = usize::try_from(request.range.offset)
                .map_err(|_| FileRangeReadError::InvalidRange)?;
            let end = usize::try_from(end).map_err(|_| FileRangeReadError::InvalidRange)?;
            let region = data.as_bytes().get(start..end).ok_or(FileRangeReadError::InvalidRange)?;
            let copied = WorkCounters { bytes_copied: request.range.length, ..WorkCounters::UNCHARGED };
            accounting.admit(copied)?;
            let mut output = allocate_read_buffer(region.len(), accounting)?;
            output.value.extend_from_slice(region);
            completed_read_work(accounting, copied)?;
            output
        }
        FilePayload::Regular { logical_bytes, extents } => {
            let plan = ReadOwned::from(super::range::plan_extent_range_with_accounting(
                store, ExtentRangeRequest {
                    root: extents, file_size: logical_bytes, range: request.range,
                    maximum_spans: request.maximum_spans, limits: request.limits,
                    budget: request.budget,
                }, accounting, cancellation,
            ).await?);
            let length = usize::try_from(request.range.length)
                .map_err(|_| FileRangeReadError::InvalidRange)?;
            let zeroed = WorkCounters { bytes_copied: request.range.length, ..WorkCounters::UNCHARGED };
            accounting.admit(zeroed)?;
            let mut output = allocate_read_buffer(length, accounting)?;
            output.value.resize(length, 0);
            completed_read_work(accounting, zeroed)?;
            for span in &plan.value.spans {
                let ExtentKind::Content { object, object_offset } = span.kind else { continue };
                let nested = super::blob::read_blob_range_for_copy_with_accounting(
                    store, object, ByteRange { offset: object_offset, length: span.length },
                    request.limits, accounting, cancellation,
                ).await?;
                let start = usize::try_from(span.offset - request.range.offset)
                    .map_err(|_| FileRangeReadError::InvalidRange)?;
                let end = start.checked_add(nested.bytes.len()).ok_or(FileRangeReadError::InvalidRange)?;
                let destination = output.value.get_mut(start..end).ok_or(FileRangeReadError::InvalidRange)?;
                let copied = WorkCounters { bytes_copied: span.length, ..WorkCounters::UNCHARGED };
                accounting.admit(copied)?;
                destination.copy_from_slice(&nested.bytes);
                completed_read_work(accounting, copied)?;
                let retained = nested.logical_owned_bytes;
                drop(nested);
                release_standalone_read_bytes(retained, accounting)?;
            }
            let retained = plan.value.retained_allocation_bytes;
            drop(plan.value);
            release_read_allocation(retained, plan.custody, accounting)?;
            output
        }
        _ => return Err(FileRangeReadError::NotRegular.into()),
    };
    let logical_owned_bytes = usize_to_u64(output.value.capacity());
    let bytes = owned_read_bytes(output)?;
    accounting.charge(completed)?;
    Ok(FileRangeRead { bytes, logical_owned_bytes, work: accounting.spent() })
}

fn file_error(error: FsError) -> FileRangeReadError {
    match error {
        FsError::FileRead(error) => error,
        FsError::BlobRead(error) => FileRangeReadError::Blob(error),
        FsError::Work(error) => FileRangeReadError::Work(error),
        FsError::Cancelled(_) => FileRangeReadError::Cancelled,
        error => FileRangeReadError::Context(Box::new(error)),
    }
}

pub(crate) fn completed_read_work(
    accounting: &mut KernelAccounting<'_>, delta: WorkCounters,
) -> Result<(), FsError> {
    accounting.record_completed(delta)
}

pub(crate) fn allocation_error(error: AllocationError) -> FsError {
    error.into_work_or(FsError::FileRead(FileRangeReadError::AllocationFailed))
}

pub(crate) struct ReadOwned<T> {
    pub(crate) value: T,
    pub(crate) custody: KernelOwnedAllocation,
}

impl<T> From<(T, KernelOwnedAllocation)> for ReadOwned<T> {
    fn from((value, custody): (T, KernelOwnedAllocation)) -> Self { Self { value, custody } }
}

impl<T> ReadOwned<T> {
    pub(crate) fn decoded((page, bytes, custody): (T, u64, KernelOwnedAllocation)) -> ReadOwned<(T, u64)> {
        ReadOwned { value: (page, bytes), custody }
    }
}

pub(crate) fn allocate_read_buffer(
    length: usize, accounting: &mut KernelAccounting<'_>,
) -> Result<ReadOwned<Vec<u8>>, FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    let native = accounting.native_work().is_some();
    let requested = usize_to_u64(length);
    let mut owner = ReadOwned {
        value: Vec::new(),
        custody: accounting.claim_owned(requested, u64::from(length != 0))?,
    };
    let result = (|| {
        owner.value.try_reserve_exact(length).map_err(|error| {
            #[cfg(not(target_arch = "wasm32"))]
            if native { return crate::NativeTreeMaterializationError::AllocationFailed(error).into(); }
            let _ = error;
            FsError::FileRead(FileRangeReadError::AllocationFailed)
        })?;
        accounting.reconcile_owned(&mut owner.custody, requested, usize_to_u64(owner.value.capacity()))
    })();
    match result {
        Ok(()) => Ok(owner),
        Err(error) => {
            let backing = requested.max(usize_to_u64(owner.value.capacity()));
            drop(owner.value);
            release_read_allocation(backing, owner.custody, accounting)?;
            Err(error)
        }
    }
}

pub(crate) fn absorb_read_custody(
    target: &mut KernelOwnedAllocation, incoming: KernelOwnedAllocation,
) -> Result<(), FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    match incoming {
        KernelOwnedAllocation::Standalone => {}
        KernelOwnedAllocation::NativeOriginal(mut incoming) => match target {
            KernelOwnedAllocation::Standalone => *target = KernelOwnedAllocation::NativeOriginal(incoming),
            KernelOwnedAllocation::NativeOriginal(target) => target.absorb(&mut incoming)?,
        },
    }
    #[cfg(target_arch = "wasm32")]
    let _ = (target, incoming);
    Ok(())
}

pub(crate) fn release_read_allocation(
    bytes: u64, mut custody: KernelOwnedAllocation, accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    match custody {
        KernelOwnedAllocation::Standalone => accounting.release_owned(&mut custody, bytes),
        #[cfg(not(target_arch = "wasm32"))]
        KernelOwnedAllocation::NativeOriginal(custody) => { drop(custody); Ok(()) }
    }
}

pub(crate) fn owned_read_bytes(owner: ReadOwned<Vec<u8>>) -> Result<acyclic_stream::OwnedBytes, FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    if let KernelOwnedAllocation::NativeOriginal(custody) = &owner.custody {
        let original = custody.work().clone();
        return acyclic_stream::OwnedBytes::from_vec_with_guard(
            owner.value, owner.custody, original.runtime_meter_owner(),
        ).map_err(|failure| match failure.into_error() {
            acyclic_stream::PreparedOwnedBytesError::Admission(error) => original.runtime_error(error).into(),
            acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized
            => {
                crate::NativeTreeMaterializationError::InvalidLayout.into()
            }
        });
    }
    Ok(owner.value.into())
}

pub(crate) async fn read_object_with_accounting<S: AsyncObjectStore>(
    store: &S, object: crate::ObjectId, maximum: u64, accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(crate::storage::ObjectRead, u64), FsError> {
    accounting.check()?;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = accounting.native_work() {
        return store.read_with_native_work(object, maximum, original, cancellation).await
            .map(|read| (read, 0))
            .map_err(read_store_error);
    }
    let result = store.read(object, maximum, accounting.remaining()?, cancellation).await;
    accept_standalone_object_read(result, accounting)
}

pub(crate) fn read_store_error(error: crate::storage::ObjectStoreError) -> FsError {
    #[cfg(not(target_arch = "wasm32"))]
    if let crate::storage::ObjectStoreError::Native(error) = error { return error.into(); }
    FsError::BlobRead(BlobReadError::Storage(error))
}

pub(crate) fn complete_standalone_backend(
    accounting: &mut KernelAccounting<'_>, completed: WorkCounters, retained: u64,
) -> Result<(), FsError> {
    record_standalone_backend(accounting, completed, retained).map_err(|(error, _)| error)
}

fn record_standalone_backend(
    accounting: &mut KernelAccounting<'_>, completed: WorkCounters, retained: u64,
) -> Result<(), (FsError, bool)> {
    let record = |allocations: &mut super::allocation::AllocationLedger, work: &mut WorkCounters, budget: WorkBudget| {
        *work = work.with_backend(completed, allocations.live_bytes())
            .map_err(|error| (FsError::from(error), false))?;
        allocations.record_owned_bytes(retained, work)
            .map_err(|error| (allocation_error(error), false))?;
        work.verify(budget).map_err(|error| (FsError::from(error), true))
    };
    match accounting {
        KernelAccounting::Standalone { allocations, work, budget, .. } => record(allocations, work, *budget),
        KernelAccounting::BorrowedStandalone { allocations, work, budget, .. } => record(allocations, work, *budget),
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { .. } => Err((crate::NativeTreeMaterializationError::InvalidLayout.into(), false)),
    }
}

pub(crate) fn accept_standalone_object_read(
    result: crate::storage::ObjectResult<crate::storage::ObjectRead>, accounting: &mut KernelAccounting<'_>,
) -> Result<(crate::storage::ObjectRead, u64), FsError> {
    let completed = match &result { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
    let retained = match &result {
        Ok(receipt) => match receipt.value.retention {
            crate::storage::ObjectReadRetention::Shared => 0,
            crate::storage::ObjectReadRetention::Owned { logical_bytes } => logical_bytes,
        },
        Err(_) => 0,
    };
    if let Err((error, registered)) = record_standalone_backend(accounting, completed, retained) {
        drop(result);
        if registered { release_standalone_read_bytes(retained, accounting)?; }
        return Err(error);
    }
    let receipt = result.map_err(|failure| read_store_error(failure.error))?;
    Ok((receipt.value, retained))
}

pub(crate) async fn read_objects_with_accounting<S: AsyncObjectStore>(
    store: &S,
    requests: &[crate::storage::ObjectReadRequest],
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<ReadOwned<Vec<crate::storage::ObjectRead>>, FsError> {
    accounting.check()?;
    cancellation.check()?;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = accounting.native_work() {
        return store.read_many_with_native_work(requests, original, cancellation).await
            .map(|batch| {
                let (value, custody) = batch;
                ReadOwned { value, custody: KernelOwnedAllocation::NativeOriginal(custody) }
            })
            .map_err(read_store_error);
    }
    let result = store.read_many(requests, accounting.remaining()?, cancellation).await;
    let completed = match &result {
        Ok(receipt) => receipt.work,
        Err(failure) => *failure.work,
    };
    let retained = match &result {
        Ok(receipt) => retained_object_reads_bytes(&receipt.value, receipt.value.capacity()),
        Err(_) => Ok(0),
    };
    let retained = match retained {
        Ok(retained) => retained,
        Err(error) => {
            let prefix = record_standalone_backend(accounting, completed, 0)
                .map_err(|(error, _)| error);
            drop(result);
            prefix?;
            return Err(error);
        }
    };
    if let Err((error, registered)) = record_standalone_backend(accounting, completed, retained) {
        drop(result);
        if registered { release_standalone_read_bytes(retained, accounting)?; }
        return Err(error);
    }
    let receipt = result.map_err(|failure| read_store_error(failure.error))?;
    Ok(ReadOwned { value: receipt.value, custody: KernelOwnedAllocation::Standalone })
}

pub(crate) fn retained_object_reads_bytes(
    reads: &[crate::storage::ObjectRead],
    capacity: usize,
) -> Result<u64, FsError> {
    let container = capacity.checked_mul(std::mem::size_of::<crate::storage::ObjectRead>())
        .map(usize_to_u64).ok_or(crate::WorkError::Overflow)?;
    reads.iter().try_fold(container, |total, read| {
        let bytes = match read.retention {
            crate::storage::ObjectReadRetention::Shared => 0,
            crate::storage::ObjectReadRetention::Owned { logical_bytes } => logical_bytes,
        };
        total.checked_add(bytes).ok_or_else(|| crate::WorkError::Overflow.into())
    })
}

pub(crate) struct OriginalReadAccounting<'a, 'operation> {
    accounting: &'a mut KernelAccounting<'operation>,
    allocation: KernelOwnedAllocation,
    owned_bytes: u64,
}

impl<'a, 'operation> OriginalReadAccounting<'a, 'operation> {
    pub(crate) fn new(accounting: &'a mut KernelAccounting<'operation>) -> Result<Self, FsError> {
        let allocation = accounting.claim_owned(0, 0)?;
        Ok(Self { accounting, allocation, owned_bytes: 0 })
    }

    pub(crate) fn observe(&mut self, step: super::codec::ReadAllocationStep) -> Result<(), FsError> {
        use super::codec::ReadAllocationStep;
        match step {
            ReadAllocationStep::Allocate { bytes, operations } => {
                let next = self.owned_bytes.checked_add(bytes).ok_or(WorkError::Overflow)?;
                let mut allocation = self.accounting.claim_owned(bytes, operations)?;
                self.allocation.absorb(&mut allocation)?;
                self.owned_bytes = next;
            }
            ReadAllocationStep::ReserveFailed(error) => {
                #[cfg(not(target_arch = "wasm32"))]
                if self.accounting.native_work().is_some() {
                    return Err(crate::NativeTreeMaterializationError::AllocationFailed(error).into());
                }
                drop(error);
                return Err(super::CanonicalDecodeError::AllocationFailed.into());
            }
            ReadAllocationStep::Capacity { admitted_bytes, actual_bytes } => {
                let next = self.owned_bytes.checked_sub(admitted_bytes)
                    .and_then(|bytes| bytes.checked_add(actual_bytes)).ok_or(WorkError::Overflow)?;
                let reconciled = self.accounting.reconcile_owned(&mut self.allocation, admitted_bytes, actual_bytes);
                self.owned_bytes = next;
                reconciled?;
            }
            ReadAllocationStep::BeforeCopy(bytes) => {
                self.accounting.check()?;
                self.accounting.admit(WorkCounters { bytes_copied: bytes, ..WorkCounters::UNCHARGED })?;
            }
            ReadAllocationStep::Copied(bytes) => {
                self.accounting.record_completed(WorkCounters { bytes_copied: bytes, ..WorkCounters::UNCHARGED })?;
            }
            ReadAllocationStep::BeforeHash(bytes) => {
                self.accounting.check()?;
                self.accounting.admit(WorkCounters { bytes_hashed: bytes, ..WorkCounters::UNCHARGED })?;
            }
            ReadAllocationStep::Hashed(bytes) => {
                self.accounting.record_completed(WorkCounters { bytes_hashed: bytes, ..WorkCounters::UNCHARGED })?;
                self.accounting.check()?;
            }
            ReadAllocationStep::BeforeItem => {
                self.accounting.check()?;
                self.accounting.admit(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            }
            ReadAllocationStep::Item => {
                self.accounting.record_completed(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            }
        }
        Ok(())
    }

    pub(crate) fn into_allocation(self) -> KernelOwnedAllocation { self.allocation }

    pub(crate) fn into_parts(self) -> (KernelOwnedAllocation, u64) {
        (self.allocation, self.owned_bytes)
    }

    pub(crate) fn admit(&mut self, delta: WorkCounters) -> Result<(), FsError> {
        self.accounting.check()?;
        self.accounting.admit(delta)
    }

    pub(crate) fn record_completed(&mut self, delta: WorkCounters) -> Result<(), FsError> {
        self.accounting.record_completed(delta)
    }

    pub(crate) fn release_failed(mut self) -> Result<(), FsError> {
        self.accounting.release_owned(&mut self.allocation, self.owned_bytes)
    }
}

pub(crate) fn reserve_decoded_items<T, E: From<super::CanonicalDecodeError>>(
    values: &mut Vec<T>, count: usize,
    observe: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
) -> Result<(), E> {
    let admitted = count.checked_mul(std::mem::size_of::<T>())
        .ok_or(super::CanonicalDecodeError::LengthOverflow)?;
    observe(super::codec::ReadAllocationStep::Allocate {
        bytes: usize_to_u64(admitted), operations: u64::from(count != 0),
    })?;
    if let Err(error) = values.try_reserve_exact(count) {
        observe(super::codec::ReadAllocationStep::ReserveFailed(error))?;
        return Err(super::CanonicalDecodeError::AllocationFailed.into());
    }
    let actual = values.capacity().checked_mul(std::mem::size_of::<T>())
        .ok_or(super::CanonicalDecodeError::LengthOverflow)?;
    observe(super::codec::ReadAllocationStep::Capacity {
        admitted_bytes: usize_to_u64(admitted), actual_bytes: usize_to_u64(actual),
    })?;
    Ok(())
}

pub(crate) fn release_standalone_read_bytes(
    bytes: u64, accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    if accounting.native_work().is_some() { return Ok(()); }
    release_read_allocation(bytes, KernelOwnedAllocation::Standalone, accounting)
}

pub(crate) fn cache_decoded_read<S: AsyncObjectStore, T: std::any::Any + Send + Sync>(
    store: &S, key: crate::async_storage::DecodedCacheKey,
    mut owner: ReadOwned<(T, u64)>, accounting: &mut KernelAccounting<'_>,
) -> Result<(crate::async_storage::DecodedCacheValue, u64), FsError> {
    let page_control = std::mem::size_of::<T>().checked_add(2 * std::mem::size_of::<usize>())
        .ok_or(WorkError::Overflow)?;
    #[cfg(not(target_arch = "wasm32"))]
    let guard_control = if accounting.native_work().is_some() {
        std::mem::size_of::<crate::NativeAllocation>() + 2 * std::mem::size_of::<usize>()
    } else { 0 };
    #[cfg(target_arch = "wasm32")]
    let guard_control = 0;
    let controls = page_control.checked_add(guard_control).ok_or(WorkError::Overflow)?;
    let controls_custody = accounting.claim_owned(usize_to_u64(controls), 1 + u64::from(guard_control != 0))?;
    absorb_read_custody(&mut owner.custody, controls_custody)?;
    let logical_bytes = owner.value.1.checked_add(usize_to_u64(controls)).ok_or(WorkError::Overflow)?;
    let value = std::sync::Arc::new(owner.value.0);
    #[cfg(not(target_arch = "wasm32"))]
    let native_custody = match owner.custody {
        KernelOwnedAllocation::Standalone => None,
        KernelOwnedAllocation::NativeOriginal(custody) => Some(std::sync::Arc::new(custody)),
    };
    let value = crate::async_storage::DecodedCacheValue {
        value, logical_bytes,
        #[cfg(not(target_arch = "wasm32"))]
        native_custody,
    };
    #[cfg(not(target_arch = "wasm32"))]
    let admitted = if let Some(original) = accounting.native_work() {
        store.decoded_cache_admit_with_native_work(key, value, original)
    } else { store.decoded_cache_admit(key, value) };
    #[cfg(target_arch = "wasm32")]
    let admitted = store.decoded_cache_admit(key, value);
    let value = match admitted.map_err(crate::kernel::file_read::read_store_error)? {
        crate::async_storage::DecodedCacheAdmission::Shared(value)
        | crate::async_storage::DecodedCacheAdmission::Uncached(value) => value,
    };
    Ok((value, logical_bytes))
}

/// Measured composed file-read failure.
pub type FileRangeReadFailure = OperationFailure<FileRangeReadError>;

/// Fail-closed regular-file range errors.
#[derive(Debug, Error)]
pub enum FileRangeReadError {
    /// The selected file is not regular.
    #[error("file range read requires a regular file")]
    NotRegular,
    /// Logical range overflows or exceeds file size.
    #[error("file range is invalid")]
    InvalidRange,
    /// Cooperative cancellation occurred before the next boundary.
    #[error("file range read was cancelled")]
    Cancelled,
    /// Exact output allocation was unavailable.
    #[error("file range output allocation failed")]
    AllocationFailed,
    /// Sparse extent planning failed.
    #[error(transparent)]
    Extent(#[from] ExtentReadError),
    /// Authenticated blob reading failed.
    #[error(transparent)]
    Blob(#[from] BlobReadError),
    /// Exact work overflowed or exceeded budget.
    #[error(transparent)]
    Work(#[from] WorkError),
    /// Shared original-ledger read boundary failed.
    #[error(transparent)]
    Context(Box<FsError>),
}

fn failed(error: FileRangeReadError, work: WorkCounters) -> FileRangeReadFailure {
    OperationFailure::new(error, work)
}

#[cfg(test)]
#[path = "tests/file_read.rs"]
mod tests;

pub(crate) fn decode_owned<T>(
    accounting: &mut KernelAccounting<'_>,
    create: impl FnOnce(&mut dyn FnMut(super::codec::ReadAllocationStep) -> Result<(), FsError>)
        -> Result<T, FsError>,
) -> Result<(T, KernelOwnedAllocation), FsError> {
    let mut observed = OriginalReadAccounting::new(accounting)?;
    match create(&mut |step| observed.observe(step)) {
        Ok(value) => Ok((value, observed.into_allocation())),
        Err(error) => { observed.release_failed()?; Err(error) }
    }
}
