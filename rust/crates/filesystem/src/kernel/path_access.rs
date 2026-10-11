//! Exact no-follow namespace-path resolution over authenticated frontiers.
//!
//! Standalone and native callers use the same lookup, observation and shared-prefix
//! batch machines. Native cache slots and returned dependency buffers retain the
//! original invocation guard; canonical OwnedBytes aliases retain their source
//! owner through every path-cache forwarding boundary.

use super::allocation::{KernelAccounting, KernelOwnedAllocation};
use super::{AuthenticatedProbeError, DecodeLimits, Dependency, DependencyRegion, DependencyState,
    FileKind, FilePayload, FileRecord, FileRecordReadError, LogicalName, NamespacePath,
    NamespacePathError, TreeReadError};
use crate::async_storage::{self, AsyncObjectStore};
use crate::cancellation::CancellationToken;
use crate::foundation::usize_to_u64;
use crate::model::{VolumeConfig, VolumeConfigError};
#[cfg(test)]
use crate::model::FilesystemProfile;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{ObjectFailure, ObjectId, ObjectRead, ObjectReadRequest,
    ObjectReadRetention, ObjectReceipt, ObjectResult, ObjectStoreError};
use acyclic_stream::OwnedBytes;
use std::cmp::Ordering;
use std::mem::size_of;
use std::ops::{Deref, DerefMut};
use std::sync::{Mutex, MutexGuard};
use thiserror::Error;

/// Data always precedes its allocation guard in physical destruction order.
struct GuardedVec<T> {
    values: Vec<T>,
    allocation: KernelOwnedAllocation,
}
impl<T> Deref for GuardedVec<T> {
    type Target = Vec<T>;
    fn deref(&self) -> &Self::Target { &self.values }
}
impl<T> DerefMut for GuardedVec<T> {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.values }
}

fn reserve_slots<T>(count: usize, accounting: &mut KernelAccounting<'_>, custody: Option<&super::ProofDataCustody>) -> Result<GuardedVec<T>, crate::FsError> {
    let requested = usize_to_u64(count).checked_mul(size_of::<T>() as u64).ok_or(WorkError::Overflow)?;
    let allocation = accounting.claim_owned(requested, u64::from(count != 0))?;
    let mut slots = GuardedVec { values: Vec::new(), allocation };
    if let Err(error) = slots.values.try_reserve_exact(count) {
        // Retain the allocator's actual cause on independent observation or
        // retirement refusal of this already-performed allocation attempt.
        if let Some(owner) = custody {
            let _ = owner.record_completed_leaf_work(WorkCounters {
                allocation_operations: u64::from(count != 0),
                peak_allocation_bytes: requested,
                ..WorkCounters::UNCHARGED
            });
        }
        #[cfg(not(target_arch = "wasm32"))]
        let error = if matches!(&slots.allocation, KernelOwnedAllocation::NativeOriginal(_)) {
            super::file_read::ReadDecodeError::AllocationFailed(error).into_original_fs_error()
        } else { crate::FsError::HandleAllocationFailed };
        #[cfg(target_arch = "wasm32")]
        let error = { let _ = error; crate::FsError::HandleAllocationFailed };
        let _ = accounting.release_owned(&mut slots.allocation, requested);
        return Err(error);
    }
    let actual = usize_to_u64(slots.capacity()).checked_mul(size_of::<T>() as u64).ok_or(WorkError::Overflow)?;
    // The buffer already exists: keep its actual guard even on a late refusal.
    let completed = accounting.reconcile_owned(&mut slots.allocation, requested, actual);
    let observed = custody.map_or(Ok(()), |owner| owner.record_completed_leaf_work(WorkCounters {
        allocation_operations: u64::from(count != 0),
        peak_allocation_bytes: actual,
        ..WorkCounters::UNCHARGED
    }).map_err(crate::FsError::from));
    completed?;
    observed?;
    Ok(slots)
}

/// Runs the SDK inspection outside original primitives, then commits its actual
/// prefix before any late cancellation or budget refusal can escape.
fn examine_item<T>(accounting: &mut KernelAccounting<'_>, custody: Option<&super::ProofDataCustody>,
    examine: impl FnOnce() -> T) -> Result<T, crate::FsError> {
    let delta = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
    accounting.admit(delta)?;
    let value = examine();
    let completed = accounting.record_completed(delta);
    let observed = custody.map_or(Ok(()), |owner| owner.record_completed_leaf_work(delta).map_err(crate::FsError::from));
    completed?;
    observed?;
    Ok(value)
}

/// Reports the existing short-frame primitive, never an adopted payload.
struct FrameObservation<'a> {
    owner: Option<&'a super::ProofDataCustody>,
    bytes: u64,
}
impl FrameObservation<'_> {
    fn admitted(&mut self, bytes: u64) -> Result<(), crate::FsError> {
        if let Some(owner) = self.owner {
            owner.record_heap_future_event(crate::heap_future::HeapFutureEvent::Admitted(WorkCounters {
                allocation_operations: u64::from(bytes != 0),
                peak_allocation_bytes: bytes,
                ..WorkCounters::UNCHARGED
            }))?;
            self.bytes = bytes;
        }
        Ok(())
    }
}
impl Drop for FrameObservation<'_> {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.filter(|_| self.bytes != 0) {
            owner.record_heap_future_event(crate::heap_future::HeapFutureEvent::Released(self.bytes))
                .expect("the actual path frame dies before its observation retires");
        }
    }
}

fn admit_path_frame(accounting: &mut KernelAccounting<'_>, bytes: u64, observation: &mut FrameObservation<'_>)
    -> Result<KernelOwnedAllocation, crate::FsError> {
    let guard = accounting.claim_owned(bytes, u64::from(bytes != 0))?;
    if let Err(error) = observation.admitted(bytes) {
        // No box was built: retire only the original admitted claim.
        drop(guard);
        release_standalone(accounting, bytes)?;
        return Err(error);
    }
    Ok(guard)
}

struct CachedObject { object_id: ObjectId, bytes: OwnedBytes }
struct CacheState {
    slots: GuardedVec<Option<CachedObject>>,
    entry_count: usize,
    retained_owned_bytes: u64,
    /// Standalone slots already present in the caller's allocation ledger.
    accounted_bytes: u64,
    accounted_slot_bytes: u64,
}
struct OperationReadCache<'a, S> {
    store: &'a S,
    state: Mutex<CacheState>,
    maximum_entries: usize,
    slot_limit: usize,
    data_custody: Option<&'a super::ProofDataCustody>,
}
const INITIAL_CACHE_SLOTS: usize = 16;
impl<'a, S> OperationReadCache<'a, S> {
    fn new(store: &'a S, maximum_entries: usize, accounting: &mut KernelAccounting<'_>,
        data_custody: Option<&'a super::ProofDataCustody>) -> Result<Self, crate::FsError> {
        let slot_limit = maximum_entries.checked_mul(2).and_then(usize::checked_next_power_of_two)
            .ok_or(WorkError::Overflow)?.max(1);
        usize_to_u64(slot_limit).checked_mul(size_of::<Option<CachedObject>>() as u64).ok_or(WorkError::Overflow)?;
        let mut slots = reserve_slots::<Option<CachedObject>>(slot_limit.min(INITIAL_CACHE_SLOTS), accounting, data_custody)?;
        let bytes = usize_to_u64(slots.capacity()).checked_mul(size_of::<Option<CachedObject>>() as u64).ok_or(WorkError::Overflow)?;
        slots.resize_with(slot_limit.min(INITIAL_CACHE_SLOTS), || None);
        if let Some(owner) = data_custody { owner.add_operation_cache_bytes(bytes)?; }
        Ok(Self { store, state: Mutex::new(CacheState { slots, entry_count: 0,
            retained_owned_bytes: 0, accounted_bytes: bytes, accounted_slot_bytes: bytes }), maximum_entries, slot_limit, data_custody })
    }
    fn lock_state(&self) -> MutexGuard<'_, CacheState> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn slot_bytes(state: &CacheState) -> Result<u64, WorkError> {
        usize_to_u64(state.slots.capacity()).checked_mul(size_of::<Option<CachedObject>>() as u64).ok_or(WorkError::Overflow)
    }
    fn extra_resident(&self) -> Result<u64, ObjectStoreError> {
        let state = self.lock_state();
        Self::slot_bytes(&state)?.checked_add(state.retained_owned_bytes)
            .and_then(|bytes| bytes.checked_sub(state.accounted_bytes)).ok_or(WorkError::Overflow.into())
    }
    /// Copies no backing bytes: native aliases retain the provider's physical owner.
    fn probe(&self, object_id: ObjectId, accounting: &mut KernelAccounting<'_>) -> Result<Option<ObjectRead>, crate::FsError> {
        let state = self.lock_state();
        Ok(probe_slots(&state.slots, object_id, accounting, self.data_custody)?.0)
    }
    /// The very same slot-growth and rehash leaves serve both accounting modes.
    fn admit(&self, object_id: ObjectId, bytes: &OwnedBytes, owned_bytes: u64,
        accounting: &mut KernelAccounting<'_>) -> Result<bool, crate::FsError> {
        let mut state = self.lock_state();
        if state.entry_count >= self.maximum_entries { return Ok(false); }
        if probe_slots(&state.slots, object_id, accounting, None)?.0.is_some() { return Ok(false); }
        if state.entry_count.saturating_mul(2) >= state.slots.len() && state.slots.len() < self.slot_limit {
            let length = state.slots.len().checked_mul(2).ok_or(WorkError::Overflow)?.min(self.slot_limit);
            let mut grown = reserve_slots::<Option<CachedObject>>(length, accounting, self.data_custody)?;
            grown.resize_with(length, || None);
            let old_bytes = Self::slot_bytes(&state)?;
            // Charge all rehash probes before moving each old entry. On failure,
            // the remaining original slots still own their untouched payloads.
            for entry in state.slots.iter_mut() {
                if let Some(value) = entry.as_ref() {
                    let vacant = probe_slots(&grown, value.object_id, accounting, None)?.1;
                    grown[vacant] = entry.take();
                }
            }
            let old = std::mem::replace(&mut state.slots, grown);
            drop(old);
            release_standalone(accounting, old_bytes.saturating_sub(state.accounted_slot_bytes))?;
            if let Some(owner) = self.data_custody {
                owner.add_operation_cache_bytes(Self::slot_bytes(&state)?.checked_sub(old_bytes).ok_or(WorkError::Overflow)?)?;
            }
        }
        let vacant = probe_slots(&state.slots, object_id, accounting, None)?.1;
        state.slots[vacant] = Some(CachedObject { object_id, bytes: bytes.clone() });
        state.entry_count += 1;
        state.retained_owned_bytes = state.retained_owned_bytes.checked_add(owned_bytes).ok_or(WorkError::Overflow)?;
        if let Some(owner) = self.data_custody { owner.add_operation_cache_bytes(owned_bytes)?; }
        Ok(true)
    }
    /// Reconciles only actual standalone backing residency, never native receipts.
    fn reconcile(&self, accounting: &mut KernelAccounting<'_>) -> Result<(), crate::FsError> {
        match accounting {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { .. } => Ok(()),
            _ => {
                let mut state = self.lock_state();
                let actual = Self::slot_bytes(&state)?.checked_add(state.retained_owned_bytes).ok_or(WorkError::Overflow)?;
                // Growth's standalone callback releases its own old-slot claim;
                // the caller retains the old baseline until this short leaf.
                let (allocations, work, budget, _) = accounting.standalone_parts().expect("native residency keeps its physical guards");
                if actual >= state.accounted_bytes {
                    allocations.record_owned_bytes(actual - state.accounted_bytes, work)
                        .map_err(|error| error.into_work_or(crate::FsError::HandleAllocationFailed))?;
                } else {
                    allocations.release(state.accounted_bytes - actual)
                        .map_err(|error| error.into_work_or(crate::FsError::HandleAllocationFailed))?;
                }
                work.verify(budget)?;
                state.accounted_bytes = actual;
                state.accounted_slot_bytes = Self::slot_bytes(&state)?;
                Ok(())
            }
        }
    }
}
impl<S> Drop for OperationReadCache<'_, S> {
    fn drop(&mut self) {
        let state = self.state.get_mut().unwrap_or_else(std::sync::PoisonError::into_inner);
        let bytes = Self::slot_bytes(state).and_then(|slots| slots.checked_add(state.retained_owned_bytes).ok_or(WorkError::Overflow))
            .expect("cache residency was admitted before retaining each payload");
        drop(std::mem::take(&mut state.slots.values));
        if let Some(owner) = self.data_custody { owner.release_operation_cache_bytes(bytes); }
    }
}
fn probe_slots(slots: &[Option<CachedObject>], object_id: ObjectId, accounting: &mut KernelAccounting<'_>,
    custody: Option<&super::ProofDataCustody>)
    -> Result<(Option<ObjectRead>, usize), crate::FsError> {
    let mask = slots.len().checked_sub(1).ok_or(ObjectStoreError::Corrupt)?;
    let mut index = object_hash(object_id) & mask;
    for _ in 0..slots.len() {
        let (found, next, vacant) = examine_item(accounting, custody, || match &slots[index] {
            Some(entry) if entry.object_id == object_id => (Some(ObjectRead {
                bytes: entry.bytes.clone(), retention: ObjectReadRetention::Shared }), index, false),
            Some(_) => (None, (index + 1) & mask, false),
            None => (None, index, true),
        })?;
        if found.is_some() || vacant { return Ok((found, index)); }
        index = next;
    }
    Err(ObjectStoreError::Corrupt.into())
}
fn object_hash(object_id: ObjectId) -> usize {
    let mut hash = usize::from(object_id.kind.canonical_tag());
    for byte in object_id.digest.as_bytes() {
        hash ^= usize::from(*byte);
        hash = hash.wrapping_mul(16_777_619);
    }
    hash
}
fn compare_object_id(left: &ObjectId, right: &ObjectId) -> Ordering {
    left.kind.canonical_tag().cmp(&right.kind.canonical_tag()).then_with(|| left.digest.cmp(&right.digest))
}
fn object_error(error: crate::FsError) -> ObjectStoreError {
    match error {
        crate::FsError::Object(error) => error,
        crate::FsError::Work(error) => ObjectStoreError::Work(error),
        crate::FsError::Cancelled(_) => ObjectStoreError::Cancelled,
        #[cfg(not(target_arch = "wasm32"))]
        crate::FsError::NativeKernel(error) => ObjectStoreError::Native(error),
        _ => ObjectStoreError::Io(std::io::ErrorKind::OutOfMemory.into()),
    }
}

/// A standalone provider callback subtracts only backing not already present
/// in its caller's original allocation ledger.
fn provider_remaining(accounting: &KernelAccounting<'_>, resident: u64) -> Result<WorkBudget, crate::FsError> {
    let mut remaining = accounting.remaining()?;
    remaining.peak_allocation_bytes = remaining.peak_allocation_bytes.checked_sub(resident).ok_or(
        WorkError::BudgetExceeded { counter: "peak_allocation_bytes", observed: resident, maximum: remaining.peak_allocation_bytes },
    )?;
    Ok(remaining)
}

/// Standalone receipts are composed at their actual callback boundary. Native
/// hooks below do not call this leaf: they already charge the original ledger.
fn compose_backend(accounting: &mut KernelAccounting<'_>, backend: WorkCounters, resident: u64) -> Result<(), crate::FsError> {
    let (_, work, budget, _) = accounting.standalone_parts().expect("native backend work is original, never composed");
    *work = work.with_backend(backend, resident)?;
    work.verify(budget)?;
    Ok(())
}
impl<S: AsyncObjectStore> AsyncObjectStore for OperationReadCache<'_, S> {
    fn decoded_cache_get(&self, key: async_storage::DecodedCacheKey)
        -> Result<Option<async_storage::DecodedCacheValue>, ObjectStoreError> { self.store.decoded_cache_get(key) }
    fn decoded_cache_admit(&self, key: async_storage::DecodedCacheKey, value: async_storage::DecodedCacheValue)
        -> Result<async_storage::DecodedCacheAdmission, ObjectStoreError> { self.store.decoded_cache_admit(key, value) }
    #[cfg(not(target_arch = "wasm32"))]
    fn decoded_cache_get_with_native_work(&self, key: async_storage::DecodedCacheKey, work: &crate::NativeMaterializationWork)
        -> Result<Option<async_storage::DecodedCacheValue>, ObjectStoreError> { self.store.decoded_cache_get_with_native_work(key, work) }
    #[cfg(not(target_arch = "wasm32"))]
    fn decoded_cache_admit_with_native_work(&self, key: async_storage::DecodedCacheKey, value: async_storage::DecodedCacheValue,
        work: &crate::NativeMaterializationWork) -> Result<async_storage::DecodedCacheAdmission, ObjectStoreError> {
        self.store.decoded_cache_admit_with_native_work(key, value, work)
    }
    async fn put(&self, object_id: ObjectId, bytes: OwnedBytes, budget: WorkBudget, cancellation: &CancellationToken) -> ObjectResult<()> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let resident = self.extra_resident()?;
            let remaining = provider_remaining(&accounting, resident)?;
            match self.store.put(object_id, bytes, remaining, cancellation).await {
                Ok(receipt) => compose_backend(&mut accounting, receipt.work, resident),
                Err(failure) => {
                    compose_backend(&mut accounting, *failure.work, resident)?;
                    Err(failure.error.into())
                }
            }
        }.await;
        let work = accounting.spent();
        result.map(|()| ObjectReceipt { value: (), work }).map_err(|error| ObjectFailure::new(object_error(error), work))
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn put_with_native_work(&self, object_id: ObjectId, bytes: OwnedBytes,
        work: &crate::NativeMaterializationWork, cancellation: &CancellationToken)
        -> Result<(), ObjectStoreError> {
        self.store.put_with_native_work(object_id, bytes, work, cancellation).await
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn put_hashed_with_native_work(&self, object: crate::storage::HashedObject,
        work: &crate::NativeMaterializationWork, cancellation: &CancellationToken)
        -> Result<(), ObjectStoreError> {
        self.store.put_hashed_with_native_work(object, work, cancellation).await
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn put_many_with_native_work(&self, writes: &[crate::storage::ObjectWrite],
        work: &crate::NativeMaterializationWork, cancellation: &CancellationToken)
        -> Result<(), ObjectStoreError> {
        self.store.put_many_with_native_work(writes, work, cancellation).await
    }
    async fn flush_before_publish(&self, scope: async_storage::PublicationScope<'_>,
        budget: WorkBudget, cancellation: &CancellationToken) -> ObjectResult<crate::PublicationHold> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let resident = self.extra_resident()?;
            let remaining = provider_remaining(&accounting, resident)?;
            match self.store.flush_before_publish(scope, remaining, cancellation).await {
                Ok(receipt) => {
                    compose_backend(&mut accounting, receipt.work, resident)?;
                    Ok(receipt.value)
                }
                Err(failure) => {
                    compose_backend(&mut accounting, *failure.work, resident)?;
                    Err(failure.error.into())
                }
            }
        }.await;
        let work = accounting.spent();
        result.map(|value| ObjectReceipt { value, work })
            .map_err(|error| ObjectFailure::new(object_error(error), work))
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn flush_before_publish_with_native_work(&self, scope: async_storage::PublicationScope<'_>,
        work: &crate::NativeMaterializationWork, cancellation: &CancellationToken)
        -> Result<crate::PublicationHold, ObjectStoreError> {
        self.store.flush_before_publish_with_native_work(scope, work, cancellation).await
    }
    async fn prepare_generation_publication(
        &self, request: &crate::kernel::PublishGenerationRequest,
        proof: &crate::kernel::GenerationProof, append: &crate::GuardedAppend,
        scope: crate::PublicationScope<'_>, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let resident = self.extra_resident()?;
            let remaining = provider_remaining(&accounting, resident)?;
            match self.store.prepare_generation_publication(
                request, proof, append, scope, remaining, cancellation,
            ).await {
                Ok(receipt) => compose_backend(&mut accounting, receipt.work, resident),
                Err(failure) => {
                    compose_backend(&mut accounting, *failure.work, resident)?;
                    Err(failure.error.into())
                }
            }
        }.await;
        let work = accounting.spent();
        result.map(|()| ObjectReceipt { value: (), work })
            .map_err(|error| ObjectFailure::new(object_error(error), work))
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn prepare_generation_publication_with_native_work(
        &self, request: &crate::kernel::PublishGenerationRequest,
        proof: &crate::kernel::GenerationProof, append: &crate::GuardedAppend,
        scope: crate::PublicationScope<'_>, original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        self.store.prepare_generation_publication_with_native_work(
            request, proof, append, scope, original, cancellation,
        ).await
    }
    fn collection(&self) -> Option<&std::sync::Arc<crate::Collection>> {
        self.store.collection()
    }
    fn collection_sweeps(&self) -> u64 {
        self.store.collection_sweeps()
    }
    async fn read(&self, object_id: ObjectId, maximum_bytes: u64, budget: WorkBudget,
        cancellation: &CancellationToken) -> ObjectResult<ObjectRead> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            if let Some(value) = self.probe(object_id, &mut accounting)? {
                if usize_to_u64(value.bytes.len()) > maximum_bytes {
                    return Err(ObjectStoreError::TooLarge { observed: usize_to_u64(value.bytes.len()), maximum: maximum_bytes }.into());
                }
                return Ok(value);
            }
            let resident = self.extra_resident()?;
            let remaining = provider_remaining(&accounting, resident)?;
            let receipt = self.store.read(object_id, maximum_bytes, remaining, cancellation).await;
            let receipt = match receipt {
                Ok(receipt) => receipt,
                Err(failure) => {
                    if let Some(owner) = self.data_custody { owner.record_completed_leaf_work(*failure.work)?; }
                    compose_backend(&mut accounting, *failure.work, resident)?;
                    return Err(failure.error.into());
                }
            };
            if let Some(owner) = self.data_custody { owner.record_completed_leaf_work(receipt.work)?; }
            compose_backend(&mut accounting, receipt.work, resident)?;
            let owned = match receipt.value.retention { ObjectReadRetention::Shared => 0, ObjectReadRetention::Owned { logical_bytes } => logical_bytes };
            // Resident backend aliases coexist with a new table during growth.
            let (allocations, work, _, _) = accounting.standalone_parts().expect("ordinary read owns its standalone projection");
            allocations.record_owned_bytes(resident.checked_add(owned).ok_or(WorkError::Overflow)?, work)
                .map_err(|error| error.into_work_or(crate::FsError::HandleAllocationFailed))?;
            let mut value = receipt.value;
            if self.admit(object_id, &value.bytes, owned, &mut accounting)? { value.retention = ObjectReadRetention::Shared; }
            Ok::<_, crate::FsError>(value)
        }.await;
        let work = accounting.spent();
        result.map(|value| ObjectReceipt { value, work }).map_err(|error| ObjectFailure::new(object_error(error), work))
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn read_with_native_work(&self, object_id: ObjectId, maximum_bytes: u64,
        work: &crate::NativeMaterializationWork, cancellation: &CancellationToken)
        -> Result<async_storage::NativeObjectRead, ObjectStoreError> {
        let mut accounting = KernelAccounting::native(work, cancellation);
        accounting.check().map_err(object_error)?;
        if let Some(value) = self.probe(object_id, &mut accounting).map_err(object_error)? {
            if usize_to_u64(value.bytes.len()) > maximum_bytes {
                return Err(ObjectStoreError::TooLarge { observed: usize_to_u64(value.bytes.len()), maximum: maximum_bytes });
            }
            return async_storage::NativeObjectRead::shared(value);
        }
        // No cache lock or original ledger borrow spans this backend await.
        let read = self.store.read_with_native_work(object_id, maximum_bytes, work, cancellation).await?;
        self.admit(object_id, &read.read().bytes, 0, &mut accounting).map_err(object_error)?;
        Ok(read)
    }
    async fn read_with_custody(&self, object_id: ObjectId, maximum_bytes: u64, budget: WorkBudget,
        cancellation: &CancellationToken, observation: Option<async_storage::ObjectReadObservation<'_>>) -> ObjectResult<ObjectRead> {
        let already_observed = observation.is_some_and(|observation| self.data_custody.is_some_and(|owner| observation.is_for(owner)));
        let result = self.read(object_id, maximum_bytes, budget, cancellation).await;
        match observation { Some(observation) if !already_observed => observation.complete(result), _ => result }
    }
    async fn read_many(&self, requests: &[ObjectReadRequest], budget: WorkBudget, cancellation: &CancellationToken) -> ObjectResult<Vec<ObjectRead>> {
        async_storage::read_many_sequential_async(self, requests, budget, cancellation).await
    }
    async fn contains(&self, object_id: ObjectId, budget: WorkBudget, cancellation: &CancellationToken) -> ObjectResult<bool> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            if self.probe(object_id, &mut accounting)?.is_some() { return Ok(true); }
            let resident = self.extra_resident()?;
            let remaining = provider_remaining(&accounting, resident)?;
            match self.store.contains(object_id, remaining, cancellation).await {
                Ok(receipt) => {
                    compose_backend(&mut accounting, receipt.work, resident)?;
                    Ok(receipt.value)
                }
                Err(failure) => {
                    compose_backend(&mut accounting, *failure.work, resident)?;
                    Err(failure.error.into())
                }
            }
        }.await;
        let work = accounting.spent();
        result.map(|value| ObjectReceipt { value, work }).map_err(|error| ObjectFailure::new(object_error(error), work))
    }
}

/// Exact path lookup result. Absence is authenticated and not an error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PathLookup {
    /// Terminal path-independent record, or authenticated absence.
    pub record: Option<FileRecord>,
    /// Immediate parent directory record for a non-root terminal result.
    pub parent: Option<FileRecord>,
    /// Number of requested components resolved before terminal absence.
    pub resolved_components: u16,
    /// Exact file-table, namespace-page, backend, copy, and allocation work.
    pub work: WorkCounters,
}

/// One exact path result plus the canonical regions observed while resolving it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedPathLookup {
    /// Ordinary no-follow result.
    pub lookup: PathLookup,
    /// Positive namespace edges, terminal record, or exact negative edge.
    pub dependencies: Vec<Dependency>,
}

/// A shared path batch plus the canonical regions observed while resolving
/// every path in it: exactly the union of what [`observe_path_async`]
/// captures for each path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedPathBatch {
    /// Ordinary batch result.
    pub lookup: PathBatchLookup,
    /// Positive namespace edges and terminal records, or exact negative edges.
    pub dependencies: Vec<Dependency>,
}

/// One original-order result from a shared no-follow path batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PathBatchEntry {
    /// Terminal path-independent record, or authenticated absence.
    pub record: Option<FileRecord>,
    /// Immediate parent directory record for a non-root terminal result.
    pub parent: Option<FileRecord>,
    /// Number of requested components resolved before terminal absence.
    pub resolved_components: u16,
}

/// Original-order results and one non-duplicated operation receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PathBatchLookup {
    /// Exactly one result for every requested path, preserving duplicates.
    pub entries: Vec<PathBatchEntry>,
    /// Logical result-vector capacity retained by the caller.
    pub retained_allocation_bytes: u64,
    /// Exact shared-prefix, file-table, backend, copy, and allocation work.
    pub work: WorkCounters,
}
/// Path resolution failure retaining all completed work.
pub type PathLookupFailure = OperationFailure<PathLookupError>;

/// Fail-closed no-follow path resolution errors.
#[derive(Debug, Error)]
pub enum PathLookupError {
    /// Volume limits or semantics are invalid.
    #[error(transparent)]
    Config(#[from] VolumeConfigError),
    /// A shared path batch must contain at least one query.
    #[error("path lookup batch is empty")]
    EmptyBatch,
    /// A shared path batch exceeds the volume's explicit query bound.
    #[error("path lookup batch exceeds the volume limit")]
    TooManyPaths,
    /// The path was not admitted under the supplied volume bounds.
    #[error("namespace path exceeds the volume's admitted bounds")]
    PathBounds,
    /// A component encoding is not representable by this volume profile.
    #[error("namespace component encoding is unsupported by the volume profile")]
    UnsupportedNameEncoding,
    /// Operation-scoped acceleration metadata could not be allocated.
    #[error("path lookup acceleration allocation failed")]
    AllocationFailed,
    /// The authenticated generation root has no root file record.
    #[error("generation root file record is missing")]
    MissingRootRecord,
    /// A non-directory occurred before the terminal component.
    #[error("path traversal encountered a non-directory")]
    NotDirectory,
    /// Namespace binding kind and file-record kind disagree.
    #[error("namespace binding kind does not match its file record")]
    KindMismatch,
    /// File-table traversal failed.
    #[error(transparent)]
    FileTable(#[from] FileRecordReadError),
    /// Directory traversal failed.
    #[error(transparent)]
    Tree(#[from] TreeReadError),
    /// A bounded prefix path could not be represented.
    #[error(transparent)]
    NamespacePath(#[from] NamespacePathError),
    /// Canonical observation-state construction failed.
    #[error(transparent)]
    Dependency(#[from] AuthenticatedProbeError),
    /// Exact work overflowed or exceeded the request budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

/// The payload's original allocation remains attached until its caller drops it.
/// Callers moving the value must carry the second `into_parts` component too.
pub(crate) struct AccountedPath<T> {
    value: T,
    allocation: KernelOwnedAllocation,
}
impl<T> AccountedPath<T> {
    pub(crate) fn value(&self) -> &T { &self.value }
    pub(crate) fn into_parts(self) -> (T, KernelOwnedAllocation) { (self.value, self.allocation) }
}
fn absorb_allocation(target: &mut KernelOwnedAllocation, incoming: &mut KernelOwnedAllocation) -> Result<(), crate::FsError> {
    match (target, incoming) {
        (KernelOwnedAllocation::Standalone, KernelOwnedAllocation::Standalone) => Ok(()),
        #[cfg(not(target_arch = "wasm32"))]
        (KernelOwnedAllocation::NativeOriginal(target), KernelOwnedAllocation::NativeOriginal(incoming)) => {
            target.absorb(incoming)?;
            Ok(())
        }
        #[cfg(not(target_arch = "wasm32"))]
        _ => Err(crate::NativeTreeMaterializationError::InvalidLayout.into()),
    }
}
fn release_standalone(accounting: &mut KernelAccounting<'_>, bytes: u64) -> Result<(), crate::FsError> {
    let ledger = match accounting {
        KernelAccounting::Standalone { allocations, .. } => allocations,
        KernelAccounting::BorrowedStandalone { allocations, .. } => &mut **allocations,
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { .. } => return Ok(()),
    };
    ledger.release(bytes).map_err(|error| error.into_work_or(crate::FsError::HandleAllocationFailed))
}
struct GuardedValue<T> { value: T, allocation: KernelOwnedAllocation }
fn drop_slots<T>(values: GuardedVec<T>, accounting: &mut KernelAccounting<'_>) -> Result<(), crate::FsError> {
    let bytes = match &values.allocation {
        KernelOwnedAllocation::Standalone => Some(usize_to_u64(values.capacity()).checked_mul(size_of::<T>() as u64).ok_or(WorkError::Overflow)?),
        #[cfg(not(target_arch = "wasm32"))]
        KernelOwnedAllocation::NativeOriginal(_) => None,
    };
    drop(values);
    if let Some(bytes) = bytes { release_standalone(accounting, bytes)?; }
    Ok(())
}
fn drop_bindings(values: GuardedVec<Option<super::TreeEntry>>, accounting: &mut KernelAccounting<'_>) -> Result<(), crate::FsError> {
    let bytes = match &values.allocation {
        KernelOwnedAllocation::Standalone => Some(usize_to_u64(values.capacity()).checked_mul(size_of::<Option<super::TreeEntry>>() as u64)
            .and_then(|bytes| values.iter().flatten().try_fold(bytes, |bytes, entry| bytes.checked_add(usize_to_u64(entry.name.retained_bytes()))))
            .ok_or(WorkError::Overflow)?),
        #[cfg(not(target_arch = "wasm32"))]
        KernelOwnedAllocation::NativeOriginal(_) => None,
    };
    drop(values);
    if let Some(bytes) = bytes { release_standalone(accounting, bytes)?; }
    Ok(())
}
fn drop_cache<S>(cache: OperationReadCache<'_, S>, accounting: &mut KernelAccounting<'_>) -> Result<(), crate::FsError> {
    match accounting {
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { .. } => { drop(cache); Ok(()) }
        _ => {
            let retained = cache.lock_state().accounted_bytes;
            drop(cache);
            release_standalone(accounting, retained)
        }
    }
}
fn copy_name(source: &LogicalName, maximum: u32, accounting: &mut KernelAccounting<'_>,
    custody: Option<&super::ProofDataCustody>) -> Result<(LogicalName, KernelOwnedAllocation), crate::FsError> {
    // Use the same canonical clone leaf, including its per-field observation.
    super::persistent_io::clone_key_with_accounting_and_custody::<super::tree_mutation::TreeFormat>(
        source, maximum, accounting, custody,
        custody.map_or(0, super::ProofDataCustody::operation_cache_bytes),
    )
}
pub(super) fn path_error(error: crate::FsError) -> PathLookupError {
    match error {
        crate::FsError::Path(error) => error,
        crate::FsError::Config(error) => PathLookupError::Config(error),
        crate::FsError::FileRecord(error) => PathLookupError::FileTable(error),
        crate::FsError::Object(ObjectStoreError::Work(error)) => PathLookupError::Work(error),
        crate::FsError::Object(error) => PathLookupError::Tree(TreeReadError::Storage(error)),
        crate::FsError::Work(error) => PathLookupError::Work(error),
        crate::FsError::Cancelled(_) => PathLookupError::Tree(TreeReadError::Cancelled),
        #[cfg(not(target_arch = "wasm32"))]
        crate::FsError::NativeKernel(error) => PathLookupError::Tree(TreeReadError::Storage(ObjectStoreError::Native(error))),
        crate::FsError::Decode(error) => PathLookupError::Tree(TreeReadError::Decode(error)),
        _ => PathLookupError::AllocationFailed,
    }
}

/// Resolves one exact path without following terminal or intermediate links.
///
/// This synchronous surface drives the same asynchronous composition used by
/// browser and remote stores. It reads only the file-table and directory
/// frontiers required by the requested path and never reads file bodies or
/// enumerates unrelated namespace subtrees.
///
/// # Errors
///
/// Rejects unsupported name semantics/encodings before backend work, malformed
/// authenticated routing, kind mismatches, non-directory intermediates,
/// storage/cancellation failures, and work outside the admitted budget.
pub fn lookup_path<S: crate::ImmediateObjectStore>(
    store: &S,
    generation: &super::GenerationRoot,
    path: &NamespacePath,
    config: VolumeConfig,
    budget: WorkBudget,
) -> Result<PathLookup, PathLookupFailure> {
    let cancellation = CancellationToken::new();
    async_storage::poll_immediate(lookup_path_async(
        store,
        generation,
        path,
        config,
        budget,
        &cancellation,
    ))
}

/// Asynchronously resolves one exact no-follow path through authenticated frontiers.
///
/// # Errors
///
/// Returns the same typed failures and exact receipts as [`lookup_path`], plus
/// cooperative cancellation from the object-store boundary.
pub async fn lookup_path_async<S: AsyncObjectStore>(
    store: &S,
    generation: &super::GenerationRoot,
    path: &NamespacePath,
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<PathLookup, PathLookupFailure> {
    lookup_path_async_with_custody(store, generation, path, config, budget, cancellation, None).await
}

/// Resolves one path and captures the exact semantic regions needed for a safe rebase.
///
/// Resolution walks the path once through an operation-local authenticated
/// object cache, so work is proportional to depth and no prefix paths are
/// constructed or rescanned.
/// Negative lookups capture the first absent edge; positive lookups capture
/// every namespace edge and the terminal path-independent record.
///
/// # Errors
///
/// Returns the same measured failures as [`lookup_path_async`], plus canonical
/// dependency-state construction failures.
pub async fn observe_path_async<S: AsyncObjectStore>(
    store: &S,
    generation: &super::GenerationRoot,
    path: &NamespacePath,
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<ObservedPathLookup, PathLookupFailure> {
    observe_path_with_terminal_async(store, generation, path, config, budget, cancellation, true, None)
        .await
}

/// Resolves one path while retaining namespace-edge observations but omitting
/// the terminal whole-record observation.
///
/// This is the internal primitive used by metadata, content-range, and
/// directory-page reads. Those operations append their narrower terminal
/// region after resolution, so unrelated changes to the same file record do
/// not create false rebase conflicts. Exact negative lookups remain captured.
pub(crate) async fn observe_path_edges_async<S: AsyncObjectStore>(
    store: &S,
    generation: &super::GenerationRoot,
    path: &NamespacePath,
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<ObservedPathLookup, PathLookupFailure> {
    observe_path_with_terminal_async(store, generation, path, config, budget, cancellation, false, None)
        .await
}

pub(crate) async fn observe_path_with_data_async<S: AsyncObjectStore>(
    store: &S,
    generation: &super::GenerationRoot,
    path: &NamespacePath,
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    terminal: bool,
    data_custody: &super::ProofDataCustody,
) -> Result<ObservedPathLookup, PathLookupFailure> {
    observe_path_with_terminal_async(
        store, generation, path, config, budget, cancellation, terminal, Some(data_custody),
    ).await
}

/// Resolves a non-empty exact path batch through shared authenticated prefixes.
///
/// Input order and duplicates are preserved. Paths sharing a directory prefix
/// use one directory-frontier batch, while all bindings discovered at one depth
/// use one shared file-table batch. No file body or unrelated subtree is read.
///
/// # Errors
///
/// Returns the same fail-closed semantic and storage errors as [`lookup_path`],
/// plus explicit empty/excessive batch and allocation failures.
pub fn lookup_paths<S: crate::ImmediateObjectStore>(
    store: &S,
    generation: &super::GenerationRoot,
    paths: &[NamespacePath],
    config: VolumeConfig,
    budget: WorkBudget,
) -> Result<PathBatchLookup, PathLookupFailure> {
    let cancellation = CancellationToken::new();
    async_storage::poll_immediate(lookup_paths_async(
        store,
        generation,
        paths,
        config,
        budget,
        &cancellation,
    ))
}

/// Asynchronously resolves a non-empty no-follow path batch with shared work.
///
/// # Errors
///
/// Returns exact operation-wide work on cancellation, malformed authenticated
/// routing, semantic mismatch, storage failure, or resource-bound rejection.
pub async fn lookup_paths_async<S: AsyncObjectStore>(
    store: &S,
    generation: &super::GenerationRoot,
    paths: &[NamespacePath],
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<PathBatchLookup, PathLookupFailure> {
    lookup_path_queries_async(
        store,
        generation,
        PathQueries::Owned(paths),
        false,
        config,
        budget,
        cancellation,
        None,
        PathBatchMode::NoFollow,
    )
    .await
    .map(|observed| observed.lookup)
}

/// Read-only selected restoration evidence over the same authenticated walk.
/// A missing or non-directory ancestor proves that its requested descendant is
/// absent. Ordinary no-follow lookup keeps its non-directory error semantics.
pub(crate) async fn lookup_paths_selected_async<S: AsyncObjectStore>(
    store: &S,
    generation: &super::GenerationRoot,
    paths: &[NamespacePath],
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<PathBatchLookup, PathLookupFailure> {
    lookup_path_queries_async(
        store, generation, PathQueries::Owned(paths), false, config, budget,
        cancellation, None, PathBatchMode::SelectedRestore,
    ).await.map(|observed| observed.lookup)
}

/// Resolves a non-empty no-follow path batch with shared work while
/// capturing, in the same walk, every region [`observe_path_async`] would
/// capture for each path.
///
/// # Errors
///
/// Returns the same bounded, fail-closed outcomes as [`lookup_paths_async`],
/// plus canonical dependency-state construction failures.
pub async fn observe_paths_async<S: AsyncObjectStore>(
    store: &S,
    generation: &super::GenerationRoot,
    paths: &[NamespacePath],
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<ObservedPathBatch, PathLookupFailure> {
    lookup_path_queries_async(
        store,
        generation,
        PathQueries::Owned(paths),
        true,
        config,
        budget,
        cancellation,
        None,
        PathBatchMode::NoFollow,
    )
    .await
}

pub(crate) async fn observe_paths_with_data_async<S: AsyncObjectStore>(
    store: &S,
    generation: &super::GenerationRoot,
    paths: &[NamespacePath],
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    data_custody: &super::ProofDataCustody,
) -> Result<ObservedPathBatch, PathLookupFailure> {
    lookup_path_queries_async(
        store, generation, PathQueries::Owned(paths), true, config, budget, cancellation, Some(data_custody),
        PathBatchMode::NoFollow,
    ).await
}

/// The same observation engine over borrowed path references.
pub(crate) async fn observe_path_refs_async<S: AsyncObjectStore>(
    store: &S,
    generation: &super::GenerationRoot,
    paths: &[&NamespacePath],
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    data_custody: &super::ProofDataCustody,
) -> Result<ObservedPathBatch, PathLookupFailure> {
    lookup_path_queries_async(
        store, generation, PathQueries::Borrowed(paths), true, config, budget, cancellation, Some(data_custody),
        PathBatchMode::NoFollow,
    ).await
}

/// Resolves borrowed paths without first cloning their names into an owned batch.
///
/// This is the mutation-planner and latency-sensitive batch surface. Input
/// order, duplicates, semantics, and receipts are identical to [`lookup_paths`].
///
/// # Errors
///
/// Returns the same bounded, fail-closed outcomes as [`lookup_paths_async`].
pub async fn lookup_path_refs_async<S: AsyncObjectStore>(
    store: &S,
    generation: &super::GenerationRoot,
    paths: &[&NamespacePath],
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<PathBatchLookup, PathLookupFailure> {
    lookup_path_queries_async(
        store,
        generation,
        PathQueries::Borrowed(paths),
        false,
        config,
        budget,
        cancellation,
        None,
        PathBatchMode::NoFollow,
    )
    .await
    .map(|observed| observed.lookup)
}

/// Synchronously resolves borrowed paths without constructing owned path copies.
///
/// # Errors
///
/// Returns the same bounded, fail-closed outcomes as [`lookup_paths`].
pub fn lookup_path_refs<S: crate::ImmediateObjectStore>(
    store: &S,
    generation: &super::GenerationRoot,
    paths: &[&NamespacePath],
    config: VolumeConfig,
    budget: WorkBudget,
) -> Result<PathBatchLookup, PathLookupFailure> {
    async_storage::poll_immediate(lookup_path_refs_async(
        store,
        generation,
        paths,
        config,
        budget,
        &CancellationToken::new(),
    ))
}

#[derive(Clone, Copy)]
enum PathBatchMode {
    NoFollow,
    SelectedRestore,
}


pub(crate) async fn lookup_path_async_with_custody<S: AsyncObjectStore>(
    store: &S, generation: &super::GenerationRoot, path: &NamespacePath, config: VolumeConfig,
    budget: WorkBudget, cancellation: &CancellationToken, data_custody: Option<&super::ProofDataCustody>,
) -> Result<PathLookup, PathLookupFailure> {
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    let result = lookup_path_async_with_accounting(store, generation, path, config, &mut accounting, cancellation, data_custody).await;
    result.map_err(|error| OperationFailure::new(path_error(error), accounting.spent()))
}

pub(crate) async fn lookup_path_async_with_accounting<S: AsyncObjectStore>(
    store: &S, generation: &super::GenerationRoot, path: &NamespacePath, config: VolumeConfig,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    data_custody: Option<&super::ProofDataCustody>,
) -> Result<PathLookup, crate::FsError> {
    let result = resolve_path_with_accounting(store, generation, path, config, accounting, cancellation, data_custody, false, false).await?;
    Ok(result.value.lookup)
}

pub(crate) async fn observe_path_async_with_accounting<S: AsyncObjectStore>(
    store: &S, generation: &super::GenerationRoot, path: &NamespacePath, config: VolumeConfig,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    data_custody: Option<&super::ProofDataCustody>, capture_terminal: bool,
) -> Result<AccountedPath<ObservedPathLookup>, crate::FsError> {
    resolve_path_with_accounting(store, generation, path, config, accounting, cancellation, data_custody, true, capture_terminal).await
}
async fn observe_path_with_terminal_async<S: AsyncObjectStore>(
    store: &S, generation: &super::GenerationRoot, path: &NamespacePath, config: VolumeConfig,
    budget: WorkBudget, cancellation: &CancellationToken, capture_terminal: bool,
    data_custody: Option<&super::ProofDataCustody>,
) -> Result<ObservedPathLookup, PathLookupFailure> {
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    observe_path_async_with_accounting(store, generation, path, config, &mut accounting, cancellation, data_custody, capture_terminal)
        .await.map(|result| result.value).map_err(|error| OperationFailure::new(path_error(error), accounting.spent()))
}

async fn resolve_path_with_accounting<S: AsyncObjectStore>(
    store: &S, generation: &super::GenerationRoot, path: &NamespacePath, config: VolumeConfig,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    data_custody: Option<&super::ProofDataCustody>, observe: bool, capture_terminal: bool,
) -> Result<AccountedPath<ObservedPathLookup>, crate::FsError> {
    accounting.check()?;
    validate_path(path, config, accounting)?;
    let cache_entries = maximum_cache_entries_for_components(path.depth(), config)
        .map_err(|failure| crate::FsError::Path(failure.error))?;
    let initial_live = standalone_live(accounting)?;
    let mut frame_guard = None;
    let mut frame_observation = FrameObservation { owner: data_custody, bytes: 0 };
    let (mut future, _) = crate::heap_future::in_heap_admitted(
        (&mut *accounting, store, generation, path, data_custody),
        |(accounting, ..), bytes| {
            frame_guard = Some(admit_path_frame(accounting, bytes, &mut frame_observation)?);
            Ok::<(), crate::FsError>(())
        },
        |(accounting, store, generation, path, data_custody)| async move {
            let limits = DecodeLimits::for_volume(config);
            let cache = OperationReadCache::new(store, cache_entries, accounting, data_custody)?;
            let mut dependencies = reserve_slots::<Dependency>(if observe { path.depth().saturating_add(1) } else { 0 }, accounting, data_custody)?;
            let root = super::file_table::lookup_file_record_async_with_accounting(&cache, generation.file_table,
                generation.root_file_id, limits, accounting, cancellation, data_custody).await?;
            cache.reconcile(accounting)?;
            let (root_record, root_guard) = root;
            let mut current = root_record.ok_or(PathLookupError::MissingRootRecord)?;
            drop(root_guard);
            let mut parent = None;
            let mut resolved = 0;
            let mut found = true;
            for (index, component) in path.components().iter().enumerate() {
                let payload = examine_item(accounting, None, || current.payload)?;
                parent = Some(current);
                let FilePayload::Directory { entries } = payload else {
                    if observe && !capture_terminal { found = false; break; }
                    return Err(PathLookupError::NotDirectory.into());
                };
                if current.kind != FileKind::Directory { return Err(PathLookupError::KindMismatch.into()); }
                let copied = if observe {
                    let (name, guard) = copy_name(component, limits.maximum_name_bytes, accounting, data_custody)?;
                    Some(GuardedValue { value: name, allocation: guard })
                } else { None };
                let (entry, entry_guard) = super::read::lookup_tree_entry_async_with_accounting(&cache, entries, component,
                    limits, accounting, cancellation, data_custody).await?;
                let entry = GuardedValue { value: entry, allocation: entry_guard };
                cache.reconcile(accounting)?;
                if let Some(copied) = copied {
                    let expected = match &entry.value {
                        Some(entry) => super::probe::capture_directory_name_state_with_accounting(entry, accounting, Some(cancellation), data_custody)?,
                        None => DependencyState::Absent,
                    };
                    let GuardedValue { value: name, allocation: mut guard } = copied;
                    // Original guards combine without copying data or charging again.
                    if let Err(error) = absorb_allocation(&mut dependencies.allocation, &mut guard) { drop(name); return Err(error); }
                    dependencies.push(Dependency { region: DependencyRegion::DirectoryName { directory_id: current.file_id, name }, expected });
                }
                let Some(binding) = entry.value.as_ref() else { found = false; resolved = index; drop(entry); break; };
                let file_id = binding.file_id;
                let kind = binding.kind;
                let entry_bytes = match &entry.allocation {
                    KernelOwnedAllocation::Standalone => Some(usize_to_u64(binding.name.retained_bytes())),
                    #[cfg(not(target_arch = "wasm32"))]
                    KernelOwnedAllocation::NativeOriginal(_) => None,
                };
                drop(entry);
                if let Some(bytes) = entry_bytes { release_standalone(accounting, bytes)?; }
                let (record, record_guard) = super::file_table::lookup_file_record_async_with_accounting(&cache, generation.file_table,
                    file_id, limits, accounting, cancellation, data_custody).await?;
                cache.reconcile(accounting)?;
                current = record.ok_or(PathLookupError::KindMismatch)?;
                drop(record_guard);
                if current.kind != kind { return Err(PathLookupError::KindMismatch.into()); }
                resolved = index + 1;
            }
            if observe && found && capture_terminal {
                let expected = super::probe::capture_file_record_state_with_accounting(current, accounting, Some(cancellation), data_custody)?;
                dependencies.push(Dependency { region: DependencyRegion::FileRecord(current.file_id), expected });
            }
            // Move result buffers first, then move their unchanged original guards.
            let GuardedVec { values: dependencies, allocation } = dependencies;
            let result = AccountedPath { value: ObservedPathLookup { lookup: PathLookup { record: found.then_some(current), parent,
                resolved_components: u16::try_from(resolved).unwrap_or(u16::MAX), work: accounting.spent() }, dependencies }, allocation };
            drop_cache(cache, accounting)?;
            Ok(result)
        },
    )?;
    let result = future.as_mut().await;
    drop(future);
    drop(frame_observation);
    drop(frame_guard);
    if initial_live.is_some() {
        let retained = match &result {
            Ok(result) => dependency_bytes(&result.value.dependencies, result.value.dependencies.capacity())?,
            Err(_) => 0,
        };
        cleanup_standalone(accounting, initial_live, retained)?;
    }
    result.map(|mut result| { result.value.lookup.work = accounting.spent(); result })
}

fn active_order(left: &ActivePath, right: &ActivePath, accounting: &mut KernelAccounting<'_>) -> Result<Ordering, crate::FsError> {
    examine_item(accounting, None, || compare_object_id(&left.directory, &right.directory)
        .then_with(|| left.path_index.cmp(&right.path_index)))
}
fn sift_active(values: &mut [ActivePath], mut root: usize, end: usize, accounting: &mut KernelAccounting<'_>) -> Result<(), crate::FsError> {
    loop {
        let Some(mut child) = root.checked_mul(2).and_then(|index| index.checked_add(1)).filter(|index| *index < end) else { return Ok(()); };
        if child + 1 < end && active_order(&values[child], &values[child + 1], accounting)? == Ordering::Less { child += 1; }
        if active_order(&values[root], &values[child], accounting)? != Ordering::Less { return Ok(()); }
        values.swap(root, child);
        root = child;
    }
}
fn sort_active(values: &mut [ActivePath], accounting: &mut KernelAccounting<'_>) -> Result<(), crate::FsError> {
    for root in (0..values.len() / 2).rev() { sift_active(values, root, values.len(), accounting)?; }
    for end in (1..values.len()).rev() {
        values.swap(0, end);
        sift_active(values, 0, end, accounting)?;
    }
    Ok(())
}
#[derive(Clone, Copy)]
struct PendingBinding { path_index: usize, file_id: crate::foundation::FileId, kind: FileKind }
#[derive(Clone, Copy)]
struct ActivePath { path_index: usize, directory: ObjectId }
#[derive(Clone, Copy)]
enum PathQueries<'a> { Owned(&'a [NamespacePath]), Borrowed(&'a [&'a NamespacePath]) }
impl<'a> PathQueries<'a> {
    fn len(self) -> usize { match self { Self::Owned(paths) => paths.len(), Self::Borrowed(paths) => paths.len() } }
    fn get(self, index: usize) -> &'a NamespacePath { match self { Self::Owned(paths) => &paths[index], Self::Borrowed(paths) => paths[index] } }
}

pub(crate) async fn lookup_paths_async_with_accounting<S: AsyncObjectStore>(
    store: &S, generation: &super::GenerationRoot, paths: &[NamespacePath], config: VolumeConfig,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken, custody: Option<&super::ProofDataCustody>,
) -> Result<AccountedPath<PathBatchLookup>, crate::FsError> {
    let AccountedPath { value, allocation } = path_queries_with_accounting(store, generation, PathQueries::Owned(paths), false,
        config, accounting, cancellation, custody, PathBatchMode::NoFollow).await?;
    Ok(AccountedPath { value: value.lookup, allocation })
}

/// Selected restoration uses the same guarded batch body and absence semantics.
pub(crate) async fn lookup_paths_selected_async_with_accounting<S: AsyncObjectStore>(
    store: &S, generation: &super::GenerationRoot, paths: &[NamespacePath], config: VolumeConfig,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken, custody: Option<&super::ProofDataCustody>,
) -> Result<AccountedPath<PathBatchLookup>, crate::FsError> {
    let AccountedPath { value, allocation } = path_queries_with_accounting(store, generation, PathQueries::Owned(paths), false,
        config, accounting, cancellation, custody, PathBatchMode::SelectedRestore).await?;
    Ok(AccountedPath { value: value.lookup, allocation })
}

pub(crate) async fn lookup_path_refs_async_with_accounting<S: AsyncObjectStore>(
    store: &S, generation: &super::GenerationRoot, paths: &[&NamespacePath], config: VolumeConfig,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken, custody: Option<&super::ProofDataCustody>,
) -> Result<AccountedPath<PathBatchLookup>, crate::FsError> {
    let AccountedPath { value, allocation } = path_queries_with_accounting(store, generation, PathQueries::Borrowed(paths), false,
        config, accounting, cancellation, custody, PathBatchMode::NoFollow).await?;
    Ok(AccountedPath { value: value.lookup, allocation })
}
pub(crate) async fn observe_paths_async_with_accounting<S: AsyncObjectStore>(
    store: &S, generation: &super::GenerationRoot, paths: &[NamespacePath], config: VolumeConfig,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken, custody: Option<&super::ProofDataCustody>,
) -> Result<AccountedPath<ObservedPathBatch>, crate::FsError> {
    path_queries_with_accounting(store, generation, PathQueries::Owned(paths), true, config, accounting, cancellation, custody,
        PathBatchMode::NoFollow).await
}

/// Borrowed observation preserves the existing batch's terminal capture.
pub(crate) async fn observe_path_refs_async_with_accounting<S: AsyncObjectStore>(
    store: &S, generation: &super::GenerationRoot, paths: &[&NamespacePath], config: VolumeConfig,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken, custody: Option<&super::ProofDataCustody>,
) -> Result<AccountedPath<ObservedPathBatch>, crate::FsError> {
    path_queries_with_accounting(store, generation, PathQueries::Borrowed(paths), true, config, accounting, cancellation, custody,
        PathBatchMode::NoFollow).await
}
async fn lookup_path_queries_async<S: AsyncObjectStore>(
    store: &S, generation: &super::GenerationRoot, paths: PathQueries<'_>, observe: bool, config: VolumeConfig,
    budget: WorkBudget, cancellation: &CancellationToken, data_custody: Option<&super::ProofDataCustody>, mode: PathBatchMode,
) -> Result<ObservedPathBatch, PathLookupFailure> {
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    path_queries_with_accounting(store, generation, paths, observe, config, &mut accounting, cancellation, data_custody, mode)
        .await.map(|result| result.value).map_err(|error| OperationFailure::new(path_error(error), accounting.spent()))
}
fn retire_name_bytes(allocation: &mut KernelOwnedAllocation, accounting: &mut KernelAccounting<'_>, bytes: u64) -> Result<(), crate::FsError> {
    match allocation {
        KernelOwnedAllocation::Standalone => release_standalone(accounting, bytes),
        #[cfg(not(target_arch = "wasm32"))]
        KernelOwnedAllocation::NativeOriginal(allocation) => allocation.release(bytes).map_err(Into::into),
    }
}
fn standalone_live(accounting: &mut KernelAccounting<'_>) -> Result<Option<u64>, crate::FsError> {
    Ok(accounting.standalone_parts().map(|(allocations, ..)| allocations.live_bytes()))
}
fn cleanup_standalone(accounting: &mut KernelAccounting<'_>, initial: Option<u64>, retained: u64) -> Result<(), crate::FsError> {
    if let Some(initial) = initial {
        let live = match accounting {
            KernelAccounting::Standalone { allocations, .. } => allocations.live_bytes(),
            KernelAccounting::BorrowedStandalone { allocations, .. } => allocations.live_bytes(),
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { .. } => return Ok(()),
        };
        let release = live.checked_sub(initial).and_then(|bytes| bytes.checked_sub(retained)).ok_or(WorkError::Overflow)?;
        release_standalone(accounting, release)?;
    }
    Ok(())
}
fn dependency_bytes(dependencies: &[Dependency], capacity: usize) -> Result<u64, WorkError> {
    dependencies.iter().try_fold(usize_to_u64(capacity).checked_mul(size_of::<Dependency>() as u64).ok_or(WorkError::Overflow)?,
        |bytes, dependency| bytes.checked_add(match &dependency.region {
            DependencyRegion::DirectoryName { name, .. } => usize_to_u64(name.retained_bytes()), _ => 0,
        }).ok_or(WorkError::Overflow))
}

async fn path_queries_with_accounting<S: AsyncObjectStore>(
    store: &S, generation: &super::GenerationRoot, paths: PathQueries<'_>, observe: bool, config: VolumeConfig,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    data_custody: Option<&super::ProofDataCustody>, mode: PathBatchMode,
) -> Result<AccountedPath<ObservedPathBatch>, crate::FsError> {
    accounting.check()?;
    config.validate()?;
    if paths.len() == 0 { return Err(PathLookupError::EmptyBatch.into()); }
    if u32::try_from(paths.len()).unwrap_or(u32::MAX) > config.limits.maximum_paths_per_batch { return Err(PathLookupError::TooManyPaths.into()); }
    let mut total_components = 0_usize;
    for index in 0..paths.len() {
        validate_path(paths.get(index), config, accounting)?;
        total_components = total_components.checked_add(paths.get(index).depth()).ok_or(WorkError::Overflow)?;
    }
    let cache_entries = maximum_cache_entries_for_components(total_components, config)
        .map_err(|failure| crate::FsError::Path(failure.error))?.min(4096);
    let initial_live = standalone_live(accounting)?;
    let mut frame_guard = None;
    let mut frame_observation = FrameObservation { owner: data_custody, bytes: 0 };
    let (mut future, _) = crate::heap_future::in_heap_admitted(
        (&mut *accounting, store, generation, paths, data_custody),
        |(accounting, ..), bytes| {
            frame_guard = Some(admit_path_frame(accounting, bytes, &mut frame_observation)?);
            Ok::<(), crate::FsError>(())
        },
        |(accounting, store, generation, paths, data_custody)| async move {
            let cache = OperationReadCache::new(store, cache_entries, accounting, data_custody)?;
            let count = paths.len();
            let limits = DecodeLimits::for_volume(config);
            let mut dependencies = reserve_slots::<Dependency>(if observe { total_components.checked_add(count).ok_or(WorkError::Overflow)? } else { 0 }, accounting, data_custody)?;
            let mut entries = reserve_slots::<PathBatchEntry>(count, accounting, data_custody)?;
            let mut current = reserve_slots::<Option<FileRecord>>(count, accounting, data_custody)?;
            let mut done = reserve_slots::<u8>(count, accounting, data_custody)?;
            let mut active = reserve_slots::<ActivePath>(count, accounting, data_custody)?;
            let mut names = reserve_slots::<LogicalName>(count, accounting, data_custody)?;
            let mut query_indices = reserve_slots::<usize>(count, accounting, data_custody)?;
            let mut pending = reserve_slots::<PendingBinding>(count, accounting, data_custody)?;
            let mut file_ids = reserve_slots::<crate::foundation::FileId>(count, accounting, data_custody)?;
            entries.resize(count, PathBatchEntry { record: None, parent: None, resolved_components: 0 });
            current.resize(count, None);
            done.resize(count, 0);
            let (root, root_guard) = super::file_table::lookup_file_record_async_with_accounting(&cache, generation.file_table,
                generation.root_file_id, limits, accounting, cancellation, data_custody).await?;
            cache.reconcile(accounting)?;
            let root = root.ok_or(PathLookupError::MissingRootRecord)?;
            drop(root_guard);
            for index in 0..count {
                let is_root = examine_item(accounting, None, || paths.get(index).is_root())?;
                if is_root {
                    entries[index].record = Some(root);
                    done[index] = 1;
                    if observe {
                        let expected = super::probe::capture_file_record_state_with_accounting(root, accounting, Some(cancellation), data_custody)?;
                        dependencies.push(Dependency { region: DependencyRegion::FileRecord(root.file_id), expected });
                    }
                } else { current[index] = Some(root); }
            }
            let maximum_depth = (0..count).map(|index| paths.get(index).depth()).max().unwrap_or(0);
            for depth in 0..maximum_depth {
                pending.clear();
                active.clear();
                for index in 0..count {
                    let finished = examine_item(accounting, None, || done[index] != 0 || paths.get(index).depth() <= depth)?;
                    if finished { continue; }
                    let FilePayload::Directory { entries: directory } = current[index].ok_or(PathLookupError::KindMismatch)?.payload else {
                        if matches!(mode, PathBatchMode::SelectedRestore) {
                            entries[index] = PathBatchEntry { record: None, parent: None, resolved_components: u16::try_from(depth).unwrap_or(u16::MAX) };
                            done[index] = 1;
                            continue;
                        }
                        return Err(PathLookupError::NotDirectory.into());
                    };
                    active.push(ActivePath { path_index: index, directory });
                }
                sort_active(&mut active, accounting)?;
                let mut cursor = 0;
                while cursor < active.len() {
                    let directory = active[cursor].directory;
                    let start = cursor;
                    cursor += 1;
                    while cursor < active.len() {
                        let same_directory = examine_item(accounting, None, || active[cursor].directory == directory)?;
                        if !same_directory { break; }
                        cursor += 1;
                    }
                    names.clear();
                    query_indices.clear();
                    let mut copied_bytes = 0_u64;
                    for item in &active[start..cursor] {
                        query_indices.push(item.path_index);
                        let (name, guard) = copy_name(&paths.get(item.path_index).components()[depth], limits.maximum_name_bytes, accounting, data_custody)?;
                        let copied = GuardedValue { value: name, allocation: guard };
                        copied_bytes = copied_bytes.checked_add(usize_to_u64(copied.value.retained_bytes())).ok_or(WorkError::Overflow)?;
                        let GuardedValue { value: name, allocation: mut guard } = copied;
                        let owner = if observe { &mut dependencies.allocation } else { &mut names.allocation };
                        if let Err(error) = absorb_allocation(owner, &mut guard) { drop(name); return Err(error); }
                        names.push(name);
                    }
                    let (bindings, bindings_guard) = super::read::lookup_tree_entries_async_with_accounting(&cache, directory, &names,
                        config.limits.maximum_paths_per_batch, limits, accounting, cancellation, data_custody).await?;
                    let bindings = GuardedVec { values: bindings, allocation: bindings_guard };
                    cache.reconcile(accounting)?;
                    for ((index, binding), name) in query_indices.iter().zip(bindings.iter()).zip(names.drain(..)) {
                        if observe {
                            let expected = match binding {
                                Some(binding) => super::probe::capture_directory_name_state_with_accounting(binding, accounting, Some(cancellation), data_custody)?,
                                None => DependencyState::Absent,
                            };
                            dependencies.push(Dependency { region: DependencyRegion::DirectoryName {
                                directory_id: current[*index].ok_or(PathLookupError::KindMismatch)?.file_id, name }, expected });
                        }
                        if let Some(binding) = binding {
                            pending.push(PendingBinding { path_index: *index, file_id: binding.file_id, kind: binding.kind });
                        } else {
                            entries[*index] = PathBatchEntry { record: None, parent: current[*index], resolved_components: u16::try_from(depth).unwrap_or(u16::MAX) };
                            done[*index] = 1;
                        }
                    }
                    drop_bindings(bindings, accounting)?;
                    if !observe { retire_name_bytes(&mut names.allocation, accounting, copied_bytes)?; }
                }
                if pending.is_empty() { continue; }
                file_ids.clear();
                file_ids.extend(pending.iter().map(|binding| binding.file_id));
                let (records, records_guard) = super::file_table::lookup_file_records_async_with_accounting(&cache, generation.file_table,
                    &file_ids, config.limits.maximum_paths_per_batch, limits, accounting, cancellation, data_custody).await?;
                let records = GuardedVec { values: records, allocation: records_guard };
                cache.reconcile(accounting)?;
                for (binding, record) in pending.iter().zip(records.iter().copied()) {
                    let record = record.ok_or(PathLookupError::KindMismatch)?;
                    if record.kind != binding.kind { return Err(PathLookupError::KindMismatch.into()); }
                    let next_depth = depth + 1;
                    if next_depth == paths.get(binding.path_index).depth() {
                        if observe {
                            let expected = super::probe::capture_file_record_state_with_accounting(record, accounting, Some(cancellation), data_custody)?;
                            dependencies.push(Dependency { region: DependencyRegion::FileRecord(record.file_id), expected });
                        }
                        entries[binding.path_index] = PathBatchEntry { record: Some(record), parent: current[binding.path_index],
                            resolved_components: u16::try_from(next_depth).unwrap_or(u16::MAX) };
                        done[binding.path_index] = 1;
                        current[binding.path_index] = None;
                    } else { current[binding.path_index] = Some(record); }
                }
                drop_slots(records, accounting)?;
            }
            if done.contains(&0) { return Err(PathLookupError::KindMismatch.into()); }
            let retained_allocation_bytes = usize_to_u64(entries.capacity()).checked_mul(size_of::<PathBatchEntry>() as u64).ok_or(WorkError::Overflow)?;
            // Scratch dies before guards; output arrays keep their exact guards.
            drop_slots(file_ids, accounting)?;
            drop_slots(pending, accounting)?;
            drop_slots(query_indices, accounting)?;
            drop_slots(names, accounting)?;
            drop_slots(active, accounting)?;
            drop_slots(done, accounting)?;
            drop_slots(current, accounting)?;
            let GuardedVec { values: dependencies, mut allocation } = dependencies;
            let GuardedVec { values: entries, allocation: mut entries_guard } = entries;
            if let Err(error) = absorb_allocation(&mut allocation, &mut entries_guard) { drop(entries); drop(dependencies); return Err(error); }
            let result = AccountedPath { value: ObservedPathBatch { lookup: PathBatchLookup { entries, retained_allocation_bytes, work: accounting.spent() }, dependencies }, allocation };
            drop_cache(cache, accounting)?;
            Ok(result)
        },
    )?;
    let result = future.as_mut().await;
    drop(future);
    drop(frame_observation);
    drop(frame_guard);
    if initial_live.is_some() {
        let retained = match &result {
            Ok(result) => result.value.lookup.retained_allocation_bytes.checked_add(
                dependency_bytes(&result.value.dependencies, result.value.dependencies.capacity())?).ok_or(WorkError::Overflow)?,
            Err(_) => 0,
        };
        cleanup_standalone(accounting, initial_live, retained)?;
    }
    result.map(|mut result| { result.value.lookup.work = accounting.spent(); result })
}

fn validate_path(path: &NamespacePath, config: VolumeConfig, accounting: &mut KernelAccounting<'_>) -> Result<(), crate::FsError> {
    config.validate()?;
    if path.encoded_bytes() > config.limits.maximum_path_bytes
        || path.depth() > usize::from(config.limits.maximum_path_depth)
    {
        return Err(PathLookupError::PathBounds.into());
    }
    let maximum_component_bytes =
        usize::try_from(config.limits.maximum_component_bytes).unwrap_or(usize::MAX);
    for component in path.components() {
        let (too_long, admitted_encoding) = examine_item(accounting, None, || (
            component.as_bytes().len() > maximum_component_bytes,
            component.encoding().is_admitted_in_profile(config.profile),
        ))?;
        if too_long { return Err(PathLookupError::PathBounds.into()); }
        if !admitted_encoding {
            return Err(PathLookupError::UnsupportedNameEncoding.into());
        }
    }
    Ok(())
}

fn maximum_cache_entries_for_components(
    components: usize,
    config: VolumeConfig,
) -> Result<usize, PathLookupFailure> {
    let component_frontiers = components
        .checked_mul(2)
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| OperationFailure::before_work(PathLookupError::Work(WorkError::Overflow)))?;
    let maximum = component_frontiers
        .checked_mul(usize::from(config.limits.maximum_page_height))
        .ok_or_else(|| OperationFailure::before_work(PathLookupError::Work(WorkError::Overflow)))?;
    Ok(maximum
        .min(usize::try_from(config.limits.maximum_objects_per_generation).unwrap_or(usize::MAX)))
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/path_access.rs"]
mod tests;
