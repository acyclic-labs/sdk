//! Bounded disposable immutable-object caching and single-flight reads.
//!
//! This layer owns no authority. Every entry is keyed by a canonical typed
//! object identity and may be discarded without changing filesystem results.
//!
//! Native reads borrow resident payloads and retain original custody in owned
//! byte aliases. Known flight/waiter controls are admitted before allocation;
//! opaque tree-node allocator metadata is not a logical DATA allocation.
//! Native writes forward the same original invocation and retain intrinsic
//! immutable byte aliases in memo entries; publication flush reaches the inner
//! staged or durable store rather than taking this decorator's default no-op.
//! Native cold frontiers remain genuine backend batches. Their concrete SDK
//! planning/result vector backings retain original custody through physical drop.

use crate::async_storage::{
    AsyncObjectStore, DecodedCacheAdmission, DecodedCacheKey, DecodedCacheValue,
};
use crate::cancellation::CancellationToken;
use crate::performance::{WorkBudget, WorkCounters};
use crate::storage::{
    ObjectFailure, ObjectId, ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectReceipt,
    ObjectResult, ObjectStoreError, ObjectWrite,
};
use bytes::Bytes;
#[cfg(not(target_arch = "wasm32"))]
use crate::async_storage::{NativeObjectRead, NativeObjectReadBatch};
#[cfg(not(target_arch = "wasm32"))]
use crate::materializer::{NativeAllocation, NativeMaterializationWork};
use std::collections::{BTreeMap, BTreeSet};
use std::future::{Future, poll_fn};
use std::mem::size_of;
use std::pin::pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use thiserror::Error;

/// Hard process-local bounds for one disposable immutable-object cache.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectCacheOptions {
    /// Maximum resident immutable objects.
    pub maximum_entries: u32,
    /// Maximum resident canonical object bytes.
    pub maximum_bytes: u64,
    /// Maximum distinct in-flight object identities.
    pub maximum_in_flight: u32,
    /// Maximum followers retained behind one in-flight read.
    pub maximum_waiters_per_object: u32,
}

impl Default for ObjectCacheOptions {
    fn default() -> Self {
        Self {
            maximum_entries: 4_096,
            maximum_bytes: 256 * 1024 * 1024,
            maximum_in_flight: 1_024,
            maximum_waiters_per_object: 1_024,
        }
    }
}

/// Invalid cache configuration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ObjectCacheConfigError {
    /// Every capacity must admit forward progress.
    #[error("every immutable-object cache bound must be positive")]
    ZeroBound,
}

impl ObjectCacheOptions {
    fn validate(self) -> Result<Self, ObjectCacheConfigError> {
        if self.maximum_entries == 0
            || self.maximum_bytes == 0
            || self.maximum_in_flight == 0
            || self.maximum_waiters_per_object == 0
        {
            return Err(ObjectCacheConfigError::ZeroBound);
        }
        Ok(self)
    }
}

/// Exact process-local accelerator observations.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ObjectCacheStats {
    /// Complete object reads served from resident immutable bytes.
    pub hits: u64,
    /// Authenticated decoded pages reused without canonical decode.
    pub decoded_hits: u64,
    /// Object reads requiring a backend request or in-flight wait.
    pub misses: u64,
    /// Reads that reused one concurrent backend result.
    pub coalesced_reads: u64,
    /// Entries removed by deterministic least-recently-used eviction.
    pub evictions: u64,
    /// Current resident entries.
    pub resident_entries: u64,
    /// Current canonical plus decoded logical bytes.
    pub resident_bytes: u64,
    /// Current resident canonical immutable objects.
    pub resident_canonical_objects: u64,
    /// Current resident canonical immutable-object bytes.
    pub resident_canonical_bytes: u64,
    /// Current resident authenticated decoded pages.
    pub resident_decoded_pages: u64,
    /// Current resident decoded-page logical bytes.
    pub resident_decoded_bytes: u64,
    /// Current distinct in-flight identities.
    pub in_flight: u64,
}

struct CacheEntry {
    bytes: Bytes,
    touched: u64,
}

struct DecodedCacheEntry {
    value: DecodedCacheValue,
    touched: u64,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum RecencyKey {
    Canonical(ObjectId),
    Decoded(DecodedCacheKey),
}

#[derive(Default)]
struct MutableStats {
    hits: u64,
    decoded_hits: u64,
    misses: u64,
    coalesced_reads: u64,
    evictions: u64,
}

struct CacheState {
    entries: BTreeMap<ObjectId, CacheEntry>,
    decoded: BTreeMap<DecodedCacheKey, DecodedCacheEntry>,
    recency: BTreeSet<(u64, RecencyKey)>,
    flights: BTreeMap<ObjectId, Arc<Flight>>,
    resident_bytes: u64,
    resident_canonical_bytes: u64,
    resident_decoded_bytes: u64,
    clock: u64,
    stats: MutableStats,
}

impl Default for CacheState {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            decoded: BTreeMap::new(),
            recency: BTreeSet::new(),
            flights: BTreeMap::new(),
            resident_bytes: 0,
            resident_canonical_bytes: 0,
            resident_decoded_bytes: 0,
            clock: 1,
            stats: MutableStats::default(),
        }
    }
}

struct Shared {
    options: ObjectCacheOptions,
    state: Mutex<CacheState>,
}

enum FlightState {
    Running(FlightWaiters),
    Complete(Option<Bytes>),
}

#[derive(Default)]
struct FlightWaiters {
    values: Vec<Waker>,
    #[cfg(not(target_arch = "wasm32"))]
    allocation: Option<NativeAllocation>,
}

struct Flight {
    state: Mutex<FlightState>,
    maximum_waiters: u32,
    #[cfg(not(target_arch = "wasm32"))]
    _allocation: Option<NativeAllocation>,
}

impl Flight {
    fn new(maximum_waiters: u32) -> Self {
        Self {
            state: Mutex::new(FlightState::Running(FlightWaiters::default())),
            maximum_waiters,
            #[cfg(not(target_arch = "wasm32"))]
            _allocation: None,
        }
    }

    fn finish(&self, bytes: Option<Bytes>) {
        let waiters = match self.state.lock() {
            Ok(mut state) => {
                if matches!(*state, FlightState::Complete(_)) { return; }
                match std::mem::replace(&mut *state, FlightState::Complete(bytes)) {
                    FlightState::Running(waiters) => waiters,
                    FlightState::Complete(_) => unreachable!(),
                }
            }
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                if matches!(*state, FlightState::Complete(_)) { return; }
                match std::mem::replace(&mut *state, FlightState::Complete(None)) {
                    FlightState::Running(waiters) => waiters,
                    FlightState::Complete(_) => return,
                }
            }
        };
        for waiter in waiters.values {
            waiter.wake();
        }
    }

    #[cfg(test)]
    fn poll(&self, context: &Context<'_>) -> Poll<Result<Option<Bytes>, ObjectStoreError>> {
        self.poll_with_context(context, #[cfg(not(target_arch = "wasm32"))] None)
    }

    fn poll_with_context(
        &self,
        context: &Context<'_>,
        #[cfg(not(target_arch = "wasm32"))] work: Option<&NativeMaterializationWork>,
    ) -> Poll<Result<Option<Bytes>, ObjectStoreError>> {
        let mut state = self.state.lock().map_err(|_| ObjectStoreError::Corrupt)?;
        match &mut *state {
            FlightState::Complete(bytes) => Poll::Ready(Ok(bytes.clone())),
            FlightState::Running(waiters) => {
                if waiters.values
                    .iter()
                    .any(|waiter| waiter.will_wake(context.waker()))
                {
                    return Poll::Pending;
                }
                if waiters.values.len() >= usize::try_from(self.maximum_waiters).unwrap_or(usize::MAX) {
                    return Poll::Ready(Err(ObjectStoreError::Rejected(
                        "immutable-object single-flight waiter bound exhausted".to_owned(),
                    )));
                }
                #[cfg(not(target_arch = "wasm32"))]
                if waiters.values.len() == waiters.values.capacity()
                    && (work.is_some() || waiters.allocation.is_some())
                {
                    // Only the concrete vector backing is SDK DATA. Build an
                    // empty replacement while loaning the original ledger; no
                    // waker or existing payload is dropped under that lock.
                    let capacity = waiters.values.len().saturating_mul(2).max(1)
                        .min(usize::try_from(self.maximum_waiters).unwrap_or(usize::MAX));
                    let mut replacement = FlightWaiters::default();
                    if let Some(work) = work {
                        let (values, allocation) = work.with_owned_allocations(|ledger, counters, budget, cancellation| {
                            cancellation.check().map_err(|_| crate::materializer::NativeTreeMaterializationError::Cancelled)?;
                            ledger.claim_elements::<Waker>(capacity, counters, budget)
                                .map_err(|error| error.into_work_or(
                                    crate::materializer::NativeTreeMaterializationError::Work(crate::WorkError::Overflow)))?;
                            let mut values = Vec::new();
                            values.try_reserve_exact(capacity).map_err(
                                crate::materializer::NativeTreeMaterializationError::AllocationFailed)?;
                            let excess = values.capacity().saturating_sub(capacity) * size_of::<Waker>();
                            ledger.record_owned_bytes(excess as u64, counters).map_err(|error| error.into_work_or(
                                crate::materializer::NativeTreeMaterializationError::Work(crate::WorkError::Overflow)))?;
                            Ok::<_, crate::materializer::NativeTreeMaterializationError>(values)
                        })?;
                        replacement = FlightWaiters { values, allocation: Some(allocation) };
                        // Record actual allocator excess before any late denial.
                        work.remaining()?;
                    } else {
                        replacement.values.try_reserve_exact(capacity).map_err(|error| ObjectStoreError::Native(
                            crate::materializer::NativeTreeMaterializationError::AllocationFailed(error)))?;
                    }
                    let copy = work.map(|work| work.reserve_copy((waiters.values.len() * size_of::<Waker>()) as u64)).transpose()?;
                    replacement.values.append(&mut waiters.values);
                    if let Some(copy) = copy { copy.record((replacement.values.len() * size_of::<Waker>()) as u64)?; }
                    // Vec storage is dropped before the custody field in this
                    // concrete owner, including failure and unwind paths.
                    drop(std::mem::replace(waiters, replacement));
                }
                waiters.values.push(context.waker().clone());
                Poll::Pending
            }
        }
    }
}

struct ActiveFlightGuard<'a, S> {
    store: &'a CachedObjectStore<S>,
    object_id: ObjectId,
    flight: Arc<Flight>,
    finished: bool,
}

impl<'a, S> ActiveFlightGuard<'a, S> {
    fn new(store: &'a CachedObjectStore<S>, object_id: ObjectId, flight: Arc<Flight>) -> Self {
        Self {
            store,
            object_id,
            flight,
            finished: false,
        }
    }

    fn finish(mut self, bytes: Option<Bytes>) -> Result<(), ObjectStoreError> {
        self.flight.finish(bytes);
        self.store.remove_flight(self.object_id, &self.flight)?;
        self.finished = true;
        Ok(())
    }
}

impl<S> Drop for ActiveFlightGuard<'_, S> {
    fn drop(&mut self) {
        if !self.finished {
            self.flight.finish(None);
            let _ = self.store.remove_flight(self.object_id, &self.flight);
        }
    }
}

enum ReadPlan {
    Hit(Bytes),
    Leader(Arc<Flight>),
    Follower(Arc<Flight>),
}

enum BatchSlot {
    Hit(Bytes),
    LocalLeader(usize),
    ExternalFollower(Arc<Flight>),
}

struct BatchLeader {
    object_id: ObjectId,
    maximum_bytes: u64,
    flight: Arc<Flight>,
}

struct BatchBuffer<T> {
    values: Vec<T>,
    #[cfg(not(target_arch = "wasm32"))]
    allocation: Option<NativeAllocation>,
}

impl<T> BatchBuffer<T> {
    fn new(
        count: usize,
        failure: &'static str,
        #[cfg(not(target_arch = "wasm32"))] native: Option<&NativeMaterializationWork>,
    ) -> Result<Self, ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(native) = native {
            // Only allocate an empty backing under the short original-ledger
            // loan. Existing byte/flight owners are moved outside that lock.
            let (values, allocation) = native.with_owned_allocations(|ledger, work, budget, cancellation| {
                cancellation.check().map_err(|_| crate::materializer::NativeTreeMaterializationError::Cancelled)?;
                ledger.claim_elements::<T>(count, work, budget).map_err(|error| error.into_work_or(
                    crate::materializer::NativeTreeMaterializationError::Work(crate::WorkError::Overflow)))?;
                let mut values = Vec::new();
                values.try_reserve_exact(count).map_err(
                    crate::materializer::NativeTreeMaterializationError::AllocationFailed)?;
                let excess = values.capacity().saturating_sub(count).checked_mul(size_of::<T>())
                    .ok_or(crate::WorkError::Overflow)?;
                ledger.record_owned_bytes(excess as u64, work).map_err(|error| error.into_work_or(
                    crate::materializer::NativeTreeMaterializationError::Work(crate::WorkError::Overflow)))?;
                Ok::<_, crate::materializer::NativeTreeMaterializationError>(values)
            })?;
            let owner = Self { values, allocation: Some(allocation) };
            native.remaining()?;
            return Ok(owner);
        }
        let mut values = Vec::new();
        values.try_reserve_exact(count).map_err(|_| ObjectStoreError::Rejected(failure.to_owned()))?;
        Ok(Self {
            values,
            #[cfg(not(target_arch = "wasm32"))]
            allocation: None,
        })
    }
}

struct PreparedBatch {
    slots: BatchBuffer<BatchSlot>,
    leaders: BatchBuffer<BatchLeader>,
    work: WorkCounters,
    planning_bytes: u64,
    result_bytes: u64,
    item_count: u64,
    shared: Arc<Shared>,
}

impl Drop for PreparedBatch {
    fn drop(&mut self) {
        for leader in &self.leaders.values {
            leader.flight.finish(None);
            if let Ok(mut state) = self.shared.state.lock()
                && state.flights.get(&leader.object_id)
                    .is_some_and(|flight| Arc::ptr_eq(flight, &leader.flight))
            {
                state.flights.remove(&leader.object_id);
            }
        }
    }
}

/// Async immutable-object accelerator over any conforming backend.
pub struct CachedObjectStore<S> {
    inner: S,
    shared: Arc<Shared>,
}

impl<S: Clone> Clone for CachedObjectStore<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<S> CachedObjectStore<S> {
    /// Wraps one backend with an independently bounded disposable cache.
    ///
    /// # Errors
    ///
    /// Rejects a zero bound.
    pub fn new(inner: S, options: ObjectCacheOptions) -> Result<Self, ObjectCacheConfigError> {
        Ok(Self {
            inner,
            shared: Arc::new(Shared {
                options: options.validate()?,
                state: Mutex::new(CacheState::default()),
            }),
        })
    }

    /// Returns the wrapped correctness backend.
    #[must_use]
    pub const fn inner(&self) -> &S {
        &self.inner
    }

    /// Returns exact current and cumulative accelerator observations.
    ///
    /// # Errors
    ///
    /// Fails closed if accelerator state was poisoned.
    pub fn stats(&self) -> Result<ObjectCacheStats, ObjectStoreError> {
        let state = self.lock_state()?;
        Ok(ObjectCacheStats {
            hits: state.stats.hits,
            decoded_hits: state.stats.decoded_hits,
            misses: state.stats.misses,
            coalesced_reads: state.stats.coalesced_reads,
            evictions: state.stats.evictions,
            resident_entries: u64::try_from(
                state.entries.len().saturating_add(state.decoded.len()),
            )
            .unwrap_or(u64::MAX),
            resident_bytes: state.resident_bytes,
            resident_canonical_objects: u64::try_from(state.entries.len()).unwrap_or(u64::MAX),
            resident_canonical_bytes: state.resident_canonical_bytes,
            resident_decoded_pages: u64::try_from(state.decoded.len()).unwrap_or(u64::MAX),
            resident_decoded_bytes: state.resident_decoded_bytes,
            in_flight: u64::try_from(state.flights.len()).unwrap_or(u64::MAX),
        })
    }

    /// Discards every resident byte. In-flight reads remain valid and may
    /// repopulate after this call.
    ///
    /// # Errors
    ///
    /// Fails closed if accelerator state was poisoned.
    pub fn clear(&self) -> Result<(), ObjectStoreError> {
        let mut state = self.lock_state()?;
        state.entries.clear();
        state.decoded.clear();
        state.recency.clear();
        state.resident_bytes = 0;
        state.resident_canonical_bytes = 0;
        state.resident_decoded_bytes = 0;
        Ok(())
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, CacheState>, ObjectStoreError> {
        self.shared
            .state
            .lock()
            .map_err(|_| ObjectStoreError::Corrupt)
    }

    fn read_plan_with_context(
        &self,
        object_id: ObjectId,
        #[cfg(not(target_arch = "wasm32"))] work: Option<&NativeMaterializationWork>,
    ) -> Result<ReadPlan, ObjectStoreError> {
        let mut state = self.lock_state()?;
        let tick = next_tick(&mut state)?;
        if let Some(entry) = state.entries.get_mut(&object_id) {
            let prior = entry.touched;
            entry.touched = tick;
            let bytes = entry.bytes.clone();
            state
                .recency
                .remove(&(prior, RecencyKey::Canonical(object_id)));
            state
                .recency
                .insert((tick, RecencyKey::Canonical(object_id)));
            state.stats.hits = increment(state.stats.hits)?;
            return Ok(ReadPlan::Hit(bytes));
        }
        state.stats.misses = increment(state.stats.misses)?;
        if let Some(flight) = state.flights.get(&object_id) {
            return Ok(ReadPlan::Follower(Arc::clone(flight)));
        }
        if state.flights.len()
            >= usize::try_from(self.shared.options.maximum_in_flight).unwrap_or(usize::MAX)
        {
            return Err(ObjectStoreError::Rejected(
                "immutable-object single-flight identity bound exhausted".to_owned(),
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        let allocation = work.map(|work| work.claim(size_of::<Flight>() as u64)).transpose()?;
        let flight = Flight::new(self.shared.options.maximum_waiters_per_object);
        #[cfg(not(target_arch = "wasm32"))]
        let flight = Flight { _allocation: allocation, ..flight };
        let flight = Arc::new(flight);
        state.flights.insert(object_id, Arc::clone(&flight));
        Ok(ReadPlan::Leader(flight))
    }

    fn cached_contains(&self, object_id: ObjectId) -> Result<bool, ObjectStoreError> {
        let mut state = self.lock_state()?;
        let tick = next_tick(&mut state)?;
        let Some(entry) = state.entries.get_mut(&object_id) else {
            state.stats.misses = increment(state.stats.misses)?;
            return Ok(false);
        };
        let prior = entry.touched;
        entry.touched = tick;
        state
            .recency
            .remove(&(prior, RecencyKey::Canonical(object_id)));
        state
            .recency
            .insert((tick, RecencyKey::Canonical(object_id)));
        state.stats.hits = increment(state.stats.hits)?;
        Ok(true)
    }

    fn insert(&self, object_id: ObjectId, bytes: Bytes) -> Result<bool, ObjectStoreError> {
        let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if length > self.shared.options.maximum_bytes {
            return Ok(false);
        }
        let mut state = self.lock_state()?;
        if let Some(existing) = state.entries.get(&object_id) {
            if existing.bytes != bytes {
                return Err(ObjectStoreError::Corrupt);
            }
            return Ok(true);
        }
        while state.entries.len().saturating_add(state.decoded.len())
            >= usize::try_from(self.shared.options.maximum_entries).unwrap_or(usize::MAX)
            || state
                .resident_bytes
                .checked_add(length)
                .is_none_or(|total| total > self.shared.options.maximum_bytes)
        {
            evict_oldest(&mut state)?;
        }
        let tick = next_tick(&mut state)?;
        state.resident_bytes = state
            .resident_bytes
            .checked_add(length)
            .ok_or(ObjectStoreError::Work(crate::WorkError::Overflow))?;
        state.resident_canonical_bytes = state
            .resident_canonical_bytes
            .checked_add(length)
            .ok_or(ObjectStoreError::Work(crate::WorkError::Overflow))?;
        state.entries.insert(
            object_id,
            CacheEntry {
                bytes,
                touched: tick,
            },
        );
        state
            .recency
            .insert((tick, RecencyKey::Canonical(object_id)));
        Ok(true)
    }

    fn decoded_get(
        &self,
        key: DecodedCacheKey,
    ) -> Result<Option<DecodedCacheValue>, ObjectStoreError> {
        let mut state = self.lock_state()?;
        let tick = next_tick(&mut state)?;
        let Some(entry) = state.decoded.get_mut(&key) else {
            return Ok(None);
        };
        let prior = entry.touched;
        entry.touched = tick;
        let value = entry.value.clone();
        state.recency.remove(&(prior, RecencyKey::Decoded(key)));
        state.recency.insert((tick, RecencyKey::Decoded(key)));
        state.stats.hits = increment(state.stats.hits)?;
        state.stats.decoded_hits = increment(state.stats.decoded_hits)?;
        Ok(Some(value))
    }

    fn decoded_admit(
        &self,
        key: DecodedCacheKey,
        value: DecodedCacheValue,
    ) -> Result<DecodedCacheAdmission, ObjectStoreError> {
        if value.logical_bytes > self.shared.options.maximum_bytes {
            return Ok(DecodedCacheAdmission::Uncached(value));
        }
        let mut state = self.lock_state()?;
        if let Some(existing) = state.decoded.get(&key) {
            return Ok(DecodedCacheAdmission::Shared(existing.value.clone()));
        }
        while state.entries.len().saturating_add(state.decoded.len())
            >= usize::try_from(self.shared.options.maximum_entries).unwrap_or(usize::MAX)
            || state
                .resident_bytes
                .checked_add(value.logical_bytes)
                .is_none_or(|total| total > self.shared.options.maximum_bytes)
        {
            evict_oldest(&mut state)?;
        }
        let tick = next_tick(&mut state)?;
        state.resident_bytes = state
            .resident_bytes
            .checked_add(value.logical_bytes)
            .ok_or(ObjectStoreError::Work(crate::WorkError::Overflow))?;
        state.resident_decoded_bytes = state
            .resident_decoded_bytes
            .checked_add(value.logical_bytes)
            .ok_or(ObjectStoreError::Work(crate::WorkError::Overflow))?;
        state.decoded.insert(
            key,
            DecodedCacheEntry {
                value: value.clone(),
                touched: tick,
            },
        );
        state.recency.insert((tick, RecencyKey::Decoded(key)));
        Ok(DecodedCacheAdmission::Shared(value))
    }

    fn remove_flight(
        &self,
        object_id: ObjectId,
        flight: &Arc<Flight>,
    ) -> Result<(), ObjectStoreError> {
        let mut state = self.lock_state()?;
        if state
            .flights
            .get(&object_id)
            .is_some_and(|active| Arc::ptr_eq(active, flight))
        {
            state.flights.remove(&object_id);
        }
        Ok(())
    }

    fn record_coalesced(&self) -> Result<(), ObjectStoreError> {
        let mut state = self.lock_state()?;
        state.stats.coalesced_reads = increment(state.stats.coalesced_reads)?;
        Ok(())
    }
}

fn increment(value: u64) -> Result<u64, ObjectStoreError> {
    value
        .checked_add(1)
        .ok_or(ObjectStoreError::Work(crate::WorkError::Overflow))
}

fn next_tick(state: &mut CacheState) -> Result<u64, ObjectStoreError> {
    let tick = state.clock;
    state.clock = increment(state.clock)?;
    Ok(tick)
}

fn evict_oldest(state: &mut CacheState) -> Result<(), ObjectStoreError> {
    let &(touched, key) = state.recency.first().ok_or(ObjectStoreError::Corrupt)?;
    state.recency.remove(&(touched, key));
    let removed_bytes = match key {
        RecencyKey::Canonical(object_id) => {
            let bytes = state
                .entries
                .remove(&object_id)
                .map(|entry| u64::try_from(entry.bytes.len()).unwrap_or(u64::MAX))
                .ok_or(ObjectStoreError::Corrupt)?;
            state.resident_canonical_bytes = state
                .resident_canonical_bytes
                .checked_sub(bytes)
                .ok_or(ObjectStoreError::Corrupt)?;
            bytes
        }
        RecencyKey::Decoded(decoded) => {
            let bytes = state
                .decoded
                .remove(&decoded)
                .map(|entry| entry.value.logical_bytes)
                .ok_or(ObjectStoreError::Corrupt)?;
            state.resident_decoded_bytes = state
                .resident_decoded_bytes
                .checked_sub(bytes)
                .ok_or(ObjectStoreError::Corrupt)?;
            bytes
        }
    };
    state.resident_bytes = state
        .resident_bytes
        .checked_sub(removed_bytes)
        .ok_or(ObjectStoreError::Corrupt)?;
    state.stats.evictions = increment(state.stats.evictions)?;
    Ok(())
}

fn cache_probe_work() -> WorkCounters {
    WorkCounters {
        object_probes: 1,
        items_examined: 1,
        ..WorkCounters::default()
    }
}

fn combine_failure(prior: WorkCounters, failure: ObjectFailure) -> ObjectFailure {
    match prior.checked_add(*failure.work) {
        Ok(work) => ObjectFailure::new(failure.error, work),
        Err(error) => ObjectFailure::new(error.into(), prior),
    }
}

fn shared_read(bytes: Bytes, work: WorkCounters) -> ObjectReceipt<ObjectRead> {
    ObjectReceipt {
        value: ObjectRead {
            bytes,
            retention: ObjectReadRetention::Shared,
        },
        work,
    }
}

#[cfg(test)]
async fn wait_for_flight(
    flight: &Flight,
    cancellation: &CancellationToken,
) -> Result<Option<Bytes>, ObjectStoreError> {
    wait_for_flight_with_context(flight, cancellation, #[cfg(not(target_arch = "wasm32"))] None).await
}

async fn wait_for_flight_with_context(
    flight: &Flight,
    cancellation: &CancellationToken,
    #[cfg(not(target_arch = "wasm32"))] work: Option<&NativeMaterializationWork>,
) -> Result<Option<Bytes>, ObjectStoreError> {
    let mut cancelled = pin!(cancellation.cancelled());
    poll_fn(|context| {
        if cancelled.as_mut().poll(context).is_ready() {
            return Poll::Ready(Err(ObjectStoreError::Cancelled));
        }
        flight.poll_with_context(context, #[cfg(not(target_arch = "wasm32"))] work)
    })
    .await
}

impl<S: AsyncObjectStore> CachedObjectStore<S> {
    async fn load_leader(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        prior: WorkCounters,
        flight: Arc<Flight>,
        #[cfg(not(target_arch = "wasm32"))] native: Option<&NativeMaterializationWork>,
    ) -> Result<ObjectReceipt<ObjectRead>, (ObjectStoreError, WorkCounters)> {
        let guard = ActiveFlightGuard::new(self, object_id, Arc::clone(&flight));
        let load = async {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(native) = native {
                return self.inner.read_with_native_work(object_id, maximum_bytes, native, cancellation)
                    .await.map(|read| ObjectReceipt { value: read.into_read(), work: WorkCounters::default() })
                    .map_err(|error| (error, prior));
            }
            let remaining = prior.remaining(budget).map_err(|error| (error.into(), prior))?;
            self.inner.read(object_id, maximum_bytes, remaining, cancellation).await
                .map_err(|failure| {
                    let work = prior.checked_add(*failure.work);
                    match work {
                        Ok(work) => (failure.error, work),
                        Err(error) => (error.into(), prior),
                    }
                })
        }.await;
        let receipt = match load {
            Ok(receipt) => receipt,
            Err(failure) => {
                guard.finish(None).map_err(|error| (error, prior))?;
                return Err(failure);
            }
        };
        let work = prior.checked_add(receipt.work).map_err(|error| (error.into(), prior))?;
        let bytes = receipt.value.bytes.clone();
        if self.insert(object_id, bytes.clone()).map_err(|error| (error, work))? {
            guard.finish(Some(bytes.clone())).map_err(|error| (error, work))?;
            return Ok(shared_read(bytes, work));
        }
        guard.finish(None).map_err(|error| (error, work))?;
        Ok(ObjectReceipt { value: receipt.value, work })
    }

    async fn read_engine(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        #[cfg(not(target_arch = "wasm32"))] native: Option<&NativeMaterializationWork>,
    ) -> Result<ObjectReceipt<ObjectRead>, (ObjectStoreError, WorkCounters)> {
        cancellation.check().map_err(|_| (ObjectStoreError::Cancelled, WorkCounters::default()))?;
        let mut prior = cache_probe_work();
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(native) = native {
            native.charge(prior).map_err(|error| (error.into(), WorkCounters::default()))?;
        } else {
            prior.verify(budget).map_err(|error| (error.into(), WorkCounters::default()))?;
        }
        #[cfg(target_arch = "wasm32")]
        prior.verify(budget).map_err(|error| (error.into(), WorkCounters::default()))?;
        loop {
            cancellation.check().map_err(|_| (ObjectStoreError::Cancelled, prior))?;
            let plan = self.read_plan_with_context(object_id, #[cfg(not(target_arch = "wasm32"))] native)
                .map_err(|error| (error, prior))?;
            let bytes = match plan {
                ReadPlan::Hit(bytes) => bytes,
                ReadPlan::Leader(flight) => return self.load_leader(
                    object_id, maximum_bytes, budget, cancellation, prior, flight,
                    #[cfg(not(target_arch = "wasm32"))] native,
                ).await,
                ReadPlan::Follower(flight) => {
                    let result = wait_for_flight_with_context(
                        &flight, cancellation, #[cfg(not(target_arch = "wasm32"))] native,
                    ).await.map_err(|error| (error, prior))?;
                    self.record_coalesced().map_err(|error| (error, prior))?;
                    if let Some(bytes) = result {
                        bytes
                    } else {
                        self.remove_flight(object_id, &flight).map_err(|error| (error, prior))?;
                        #[cfg(not(target_arch = "wasm32"))]
                        if let Some(native) = native {
                            native.charge(cache_probe_work()).map_err(|error| (error.into(), prior))?;
                        }
                        prior = prior.checked_add(cache_probe_work()).map_err(|error| (error.into(), prior))?;
                        #[cfg(not(target_arch = "wasm32"))]
                        if native.is_none() {
                            prior.verify(budget).map_err(|error| (error.into(), prior))?;
                        }
                        #[cfg(target_arch = "wasm32")]
                        prior.verify(budget).map_err(|error| (error.into(), prior))?;
                        continue;
                    }
                }
            };
            let observed = bytes.len() as u64;
            if observed > maximum_bytes {
                return Err((ObjectStoreError::TooLarge { observed, maximum: maximum_bytes }, prior));
            }
            return Ok(shared_read(bytes, prior));
        }
    }

    fn prepare_batch(
        &self, requests: &[ObjectReadRequest], budget: WorkBudget, cancellation: &CancellationToken,
        #[cfg(not(target_arch = "wasm32"))] native: Option<&NativeMaterializationWork>,
    ) -> Result<PreparedBatch, (ObjectStoreError, WorkCounters)> {
        cancellation.check().map_err(|_| (ObjectStoreError::Cancelled, WorkCounters::default()))?;
        if requests.is_empty() {
            return Err((ObjectStoreError::Rejected("object read batch is empty".to_owned()), WorkCounters::default()));
        }
        let item_count = requests.len() as u64;
        let result_bytes = item_count.checked_mul(size_of::<ObjectRead>() as u64)
            .ok_or((crate::WorkError::Overflow.into(), WorkCounters::default()))?;
        let planning_bytes = item_count.checked_mul(
            (size_of::<BatchSlot>() + size_of::<BatchLeader>() + size_of::<ObjectReadRequest>()) as u64,
        ).ok_or((crate::WorkError::Overflow.into(), WorkCounters::default()))?;
        let mut work = WorkCounters {
            object_probes: item_count, items_examined: item_count, items_returned: item_count,
            allocation_operations: 4,
            peak_allocation_bytes: planning_bytes.saturating_add(result_bytes),
            ..WorkCounters::default()
        };
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(native) = native {
            native.admit(WorkCounters { items_returned: item_count, ..WorkCounters::default() })
                .map_err(|error| (error.into(), WorkCounters::default()))?;
            native.charge(WorkCounters { object_probes: item_count, items_examined: item_count, ..WorkCounters::default() })
                .map_err(|error| (error.into(), WorkCounters::default()))?;
        } else {
            work.verify(budget).map_err(|error| (error.into(), WorkCounters::default()))?;
        }
        #[cfg(target_arch = "wasm32")]
        work.verify(budget).map_err(|error| (error.into(), WorkCounters::default()))?;
        work.items_returned = 0;
        let slots = BatchBuffer::new(requests.len(), "object cache batch plan allocation failed",
            #[cfg(not(target_arch = "wasm32"))] native,
        ).map_err(|error| (error, work))?;
        let leaders = BatchBuffer::new(requests.len(), "object cache leader allocation failed",
            #[cfg(not(target_arch = "wasm32"))] native,
        ).map_err(|error| (error, work))?;
        let mut prepared = PreparedBatch {
            slots, leaders, work, planning_bytes, result_bytes, item_count,
            shared: Arc::clone(&self.shared),
        };
        let mut local_leaders = BTreeMap::<ObjectId, usize>::new();
        for request in requests {
            let slot = self.prepare_batch_slot(request, &mut prepared.leaders.values, &mut local_leaders, work,
                #[cfg(not(target_arch = "wasm32"))] native,
            )?;
            prepared.slots.values.push(slot);
        }
        Ok(prepared)
    }

    fn prepare_batch_slot(
        &self, request: &ObjectReadRequest, leaders: &mut Vec<BatchLeader>,
        local_leaders: &mut BTreeMap<ObjectId, usize>, work: WorkCounters,
        #[cfg(not(target_arch = "wasm32"))] native: Option<&NativeMaterializationWork>,
    ) -> Result<BatchSlot, (ObjectStoreError, WorkCounters)> {
        match self.read_plan_with_context(request.object_id, #[cfg(not(target_arch = "wasm32"))] native)
            .map_err(|error| (error, work))?
        {
            ReadPlan::Hit(bytes) => Ok(BatchSlot::Hit(bytes)),
            ReadPlan::Leader(flight) => {
                let index = leaders.len();
                leaders.push(BatchLeader {
                    object_id: request.object_id, maximum_bytes: request.maximum_bytes, flight: Arc::clone(&flight),
                });
                local_leaders.insert(request.object_id, index);
                Ok(BatchSlot::LocalLeader(index))
            }
            ReadPlan::Follower(flight) => {
                if let Some(&index) = local_leaders.get(&request.object_id)
                    && leaders.get(index).is_some_and(|leader| Arc::ptr_eq(&leader.flight, &flight))
                {
                    if let Some(leader) = leaders.get_mut(index) {
                        leader.maximum_bytes = leader.maximum_bytes.max(request.maximum_bytes);
                    }
                    Ok(BatchSlot::LocalLeader(index))
                } else {
                    Ok(BatchSlot::ExternalFollower(flight))
                }
            }
        }
    }

    async fn load_batch_leaders(
        &self, prepared: &PreparedBatch, budget: WorkBudget, cancellation: &CancellationToken,
        #[cfg(not(target_arch = "wasm32"))] native: Option<&NativeMaterializationWork>,
    ) -> Result<ObjectReceipt<BatchBuffer<ObjectRead>>, (ObjectStoreError, WorkCounters)> {
        if prepared.leaders.values.is_empty() {
            return Ok(ObjectReceipt {
                value: BatchBuffer::new(0, "object cache backend batch allocation failed",
                    #[cfg(not(target_arch = "wasm32"))] native,
                ).map_err(|error| (error, prepared.work))?,
                work: prepared.work,
            });
        }
        let mut requests = BatchBuffer::new(prepared.leaders.values.len(), "object cache backend batch allocation failed",
            #[cfg(not(target_arch = "wasm32"))] native,
        ).map_err(|error| (error, prepared.work))?;
        #[cfg(not(target_arch = "wasm32"))]
        let copy = native.map(|native| native.reserve_copy(
            (prepared.leaders.values.len() * size_of::<ObjectReadRequest>()) as u64,
        )).transpose().map_err(|error| (error.into(), prepared.work))?;
        requests.values.extend(prepared.leaders.values.iter().map(|leader| ObjectReadRequest {
            object_id: leader.object_id, maximum_bytes: leader.maximum_bytes,
        }));
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(copy) = copy {
            copy.record((requests.values.len() * size_of::<ObjectReadRequest>()) as u64)
                .map_err(|error| (error.into(), prepared.work))?;
        }
        let mut guards = BatchBuffer::new(prepared.leaders.values.len(), "object cache flight guard allocation failed",
            #[cfg(not(target_arch = "wasm32"))] native,
        ).map_err(|error| (error, prepared.work))?;
        guards.values.extend(prepared.leaders.values.iter().map(|leader| Some(ActiveFlightGuard::new(
            self, leader.object_id, Arc::clone(&leader.flight),
        ))));
        let receipt = async {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(native) = native {
                let batch = self.inner.read_many_with_native_work(&requests.values, native, cancellation).await
                    .map_err(|error| (error, prepared.work))?;
                let (values, allocation) = batch.into_parts();
                return Ok(ObjectReceipt { value: BatchBuffer { values, allocation: Some(allocation) }, work: WorkCounters::default() });
            }
            let remaining = prepared.work.remaining(budget).map_err(|error| (error.into(), prepared.work))?;
            self.inner.read_many(&requests.values, remaining, cancellation).await
                .map(|receipt| ObjectReceipt {
                    value: BatchBuffer {
                        values: receipt.value,
                        #[cfg(not(target_arch = "wasm32"))]
                        allocation: None,
                    },
                    work: receipt.work,
                })
                .map_err(|failure| match prepared.work.checked_add(*failure.work) {
                    Ok(work) => (failure.error, work),
                    Err(error) => (error.into(), prepared.work),
                })
        }.await?;
        let mut work = prepared.work.checked_add(receipt.work).map_err(|error| (error.into(), prepared.work))?;
        let mut values = receipt.value;
        if values.values.len() != prepared.leaders.values.len() {
            return Err((ObjectStoreError::Corrupt, work));
        }
        for ((leader, value), guard) in prepared.leaders.values.iter()
            .zip(values.values.iter_mut()).zip(guards.values.iter_mut())
        {
            let bytes = value.bytes.clone();
            let retained = self.insert(leader.object_id, bytes.clone()).map_err(|error| (error, work))?;
            if retained { value.retention = ObjectReadRetention::Shared; }
            if let Some(guard) = guard.take() {
                guard.finish(retained.then_some(bytes)).map_err(|error| (error, work))?;
            }
        }
        work.peak_allocation_bytes = work.peak_allocation_bytes.max(
            prepared.planning_bytes.saturating_add(prepared.result_bytes),
        );
        Ok(ObjectReceipt { value: values, work })
    }

    async fn resolve_prepared_batch(
        &self, requests: &[ObjectReadRequest], mut prepared: PreparedBatch,
        leaders: ObjectReceipt<BatchBuffer<ObjectRead>>, budget: WorkBudget, cancellation: &CancellationToken,
        #[cfg(not(target_arch = "wasm32"))] native: Option<&NativeMaterializationWork>,
    ) -> Result<ObjectReceipt<BatchBuffer<ObjectRead>>, (ObjectStoreError, WorkCounters)> {
        let mut work = leaders.work;
        let mut values = BatchBuffer::new(requests.len(), "object cache result allocation failed",
            #[cfg(not(target_arch = "wasm32"))] native,
        ).map_err(|error| (error, work))?;
        for (request, slot) in requests.iter().zip(std::mem::take(&mut prepared.slots.values)) {
            cancellation.check().map_err(|_| (ObjectStoreError::Cancelled, work))?;
            let value = self.resolve_batch_slot(request, slot, &leaders.value.values, &mut work, budget, cancellation,
                #[cfg(not(target_arch = "wasm32"))] native,
            ).await?;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(native) = native {
                native.charge(WorkCounters { items_returned: 1, ..WorkCounters::default() })
                    .map_err(|error| (error.into(), work))?;
            }
            values.values.push(value);
        }
        work.items_returned = prepared.item_count;
        #[cfg(not(target_arch = "wasm32"))]
        if native.is_none() { work.verify(budget).map_err(|error| (error.into(), work))?; }
        #[cfg(target_arch = "wasm32")]
        work.verify(budget).map_err(|error| (error.into(), work))?;
        Ok(ObjectReceipt { value: values, work })
    }

    async fn resolve_batch_slot(
        &self, request: &ObjectReadRequest, slot: BatchSlot, leader_values: &[ObjectRead],
        work: &mut WorkCounters, budget: WorkBudget, cancellation: &CancellationToken,
        #[cfg(not(target_arch = "wasm32"))] native: Option<&NativeMaterializationWork>,
    ) -> Result<ObjectRead, (ObjectStoreError, WorkCounters)> {
        let bytes = match slot {
            BatchSlot::Hit(bytes) => bytes,
            BatchSlot::LocalLeader(index) => leader_values.get(index).ok_or((ObjectStoreError::Corrupt, *work))?.bytes.clone(),
            BatchSlot::ExternalFollower(flight) => {
                let result = wait_for_flight_with_context(&flight, cancellation,
                    #[cfg(not(target_arch = "wasm32"))] native,
                ).await.map_err(|error| (error, *work))?;
                self.record_coalesced().map_err(|error| (error, *work))?;
                if let Some(bytes) = result {
                    bytes
                } else {
                    self.remove_flight(request.object_id, &flight).map_err(|error| (error, *work))?;
                    #[cfg(not(target_arch = "wasm32"))]
                    let remaining = match native {
                        Some(native) => native.remaining().map_err(|error| (error.into(), *work))?,
                        None => work.remaining(budget).map_err(|error| (error.into(), *work))?,
                    };
                    #[cfg(target_arch = "wasm32")]
                    let remaining = work.remaining(budget).map_err(|error| (error.into(), *work))?;
                    let receipt = self.read_engine(request.object_id, request.maximum_bytes,
                        remaining, cancellation,
                        #[cfg(not(target_arch = "wasm32"))] native,
                    ).await.map_err(|(error, nested)| match work.checked_add(nested) {
                        Ok(work) => (error, work),
                        Err(error) => (error.into(), *work),
                    })?;
                    *work = work.checked_add(receipt.work).map_err(|error| (error.into(), *work))?;
                    return Ok(receipt.value);
                }
            }
        };
        let observed = bytes.len() as u64;
        if observed > request.maximum_bytes {
            return Err((ObjectStoreError::TooLarge { observed, maximum: request.maximum_bytes }, *work));
        }
        Ok(ObjectRead { bytes, retention: ObjectReadRetention::Shared })
    }

    async fn read_many_engine(
        &self, requests: &[ObjectReadRequest], budget: WorkBudget, cancellation: &CancellationToken,
        #[cfg(not(target_arch = "wasm32"))] native: Option<&NativeMaterializationWork>,
    ) -> Result<ObjectReceipt<BatchBuffer<ObjectRead>>, (ObjectStoreError, WorkCounters)> {
        let prepared = self.prepare_batch(requests, budget, cancellation,
            #[cfg(not(target_arch = "wasm32"))] native,
        )?;
        let leaders = self.load_batch_leaders(&prepared, budget, cancellation,
            #[cfg(not(target_arch = "wasm32"))] native,
        ).await?;
        self.resolve_prepared_batch(requests, prepared, leaders, budget, cancellation,
            #[cfg(not(target_arch = "wasm32"))] native,
        ).await
    }
}

impl<S: AsyncObjectStore> AsyncObjectStore for CachedObjectStore<S> {
    fn decoded_cache_get(
        &self,
        key: DecodedCacheKey,
    ) -> Result<Option<DecodedCacheValue>, ObjectStoreError> {
        self.decoded_get(key)
    }

    fn decoded_cache_admit(
        &self,
        key: DecodedCacheKey,
        value: DecodedCacheValue,
    ) -> Result<DecodedCacheAdmission, ObjectStoreError> {
        self.decoded_admit(key, value)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn decoded_cache_get_with_native_work(
        &self, key: DecodedCacheKey, work: &NativeMaterializationWork,
    ) -> Result<Option<DecodedCacheValue>, ObjectStoreError> {
        work.remaining()?;
        self.decoded_get(key)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn decoded_cache_admit_with_native_work(
        &self, key: DecodedCacheKey, value: DecodedCacheValue, work: &NativeMaterializationWork,
    ) -> Result<DecodedCacheAdmission, ObjectStoreError> {
        work.remaining()?;
        // The caller's decoded Arc already owns its original physical guard.
        self.decoded_admit(key, value)
    }

    async fn put(
        &self,
        object_id: ObjectId,
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let retained = bytes.clone();
        let receipt = self
            .inner
            .put(object_id, bytes, budget, cancellation)
            .await?;
        self.insert(object_id, retained)
            .map_err(|error| ObjectFailure::new(error, receipt.work))?;
        Ok(receipt)
    }

    async fn put_hashed(
        &self, object: crate::storage::HashedObject, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation.check().map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let object_id = object.object_id();
        let retained = object.bytes().clone();
        let receipt = self.inner.put_hashed(object, budget, cancellation).await?;
        self.insert(object_id, retained).map_err(|error| ObjectFailure::new(error, receipt.work))?;
        Ok(receipt)
    }

    async fn flush_before_publish(
        &self, scope: crate::async_storage::PublicationScope<'_>, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<crate::PublicationHold> {
        self.inner.flush_before_publish(scope, budget, cancellation).await
    }

    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let receipt = self.inner.put_many(writes, budget, cancellation).await?;
        for write in writes {
            self.insert(write.object_id, write.bytes.clone())
                .map_err(|error| ObjectFailure::new(error, receipt.work))?;
        }
        Ok(receipt)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn put_with_native_work(
        &self, object_id: ObjectId, bytes: Bytes, work: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        cancellation.check().map_err(|_| ObjectStoreError::Cancelled)?;
        work.admit(WorkCounters::UNCHARGED)?;
        let retained = bytes.clone();
        self.inner.put_with_native_work(object_id, bytes, work, cancellation).await?;
        // Resident aliases keep the original intrinsic physical byte owner.
        // Tree node implementation footprint is deliberately opaque.
        self.insert(object_id, retained)?;
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn put_hashed_with_native_work(
        &self, object: crate::storage::HashedObject, work: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        cancellation.check().map_err(|_| ObjectStoreError::Cancelled)?;
        work.admit(WorkCounters::UNCHARGED)?;
        let object_id = object.object_id();
        let retained = object.bytes().clone();
        self.inner.put_hashed_with_native_work(object, work, cancellation).await?;
        self.insert(object_id, retained)?;
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn put_many_with_native_work(
        &self, writes: &[ObjectWrite], work: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        cancellation.check().map_err(|_| ObjectStoreError::Cancelled)?;
        work.admit(WorkCounters::UNCHARGED)?;
        self.inner.put_many_with_native_work(writes, work, cancellation).await?;
        for write in writes { self.insert(write.object_id, write.bytes.clone())?; }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn flush_before_publish_with_native_work(
        &self, scope: crate::async_storage::PublicationScope<'_>, work: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<crate::PublicationHold, ObjectStoreError> {
        self.inner.flush_before_publish_with_native_work(scope, work, cancellation).await
    }

    async fn prepare_generation_publication(
        &self, request: &crate::kernel::PublishGenerationRequest,
        proof: &crate::kernel::GenerationProof, append: &crate::GuardedAppend,
        scope: crate::PublicationScope<'_>, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        self.inner.prepare_generation_publication(
            request, proof, append, scope, budget, cancellation,
        ).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn prepare_generation_publication_with_native_work(
        &self, request: &crate::kernel::PublishGenerationRequest,
        proof: &crate::kernel::GenerationProof, append: &crate::GuardedAppend,
        scope: crate::PublicationScope<'_>, original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        self.inner.prepare_generation_publication_with_native_work(
            request, proof, append, scope, original, cancellation,
        ).await
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        self.read_engine(object_id, maximum_bytes, budget, cancellation,
            #[cfg(not(target_arch = "wasm32"))] None,
        ).await.map_err(|(error, work)| ObjectFailure::new(error, work))
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_with_native_work(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        work: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<NativeObjectRead, ObjectStoreError> {
        let mut read = self.read_engine(object_id, maximum_bytes, work.remaining()?, cancellation, Some(work))
            .await.map_err(|(error, _)| error)?.value;
        // Genuine native backend owned bytes have custody in their Bytes owner;
        // cache and flight clones share that owner, not a second body claim.
        read.retention = ObjectReadRetention::Shared;
        NativeObjectRead::shared(read)
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        self.read_many_engine(requests, budget, cancellation,
            #[cfg(not(target_arch = "wasm32"))] None,
        ).await.map(|receipt| ObjectReceipt { value: receipt.value.values, work: receipt.work })
            .map_err(|(error, work)| ObjectFailure::new(error, work))
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_many_with_native_work(
        &self, requests: &[ObjectReadRequest], work: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<NativeObjectReadBatch, ObjectStoreError> {
        let receipt = self.read_many_engine(requests, work.remaining()?, cancellation, Some(work))
            .await.map_err(|(error, _)| error)?;
        let BatchBuffer { values, allocation } = receipt.value;
        let allocation = allocation.expect("native result vector owns its original backing guard");
        NativeObjectReadBatch::new(values, allocation)
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let prior = cache_probe_work();
        prior
            .verify(budget)
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        if self
            .cached_contains(object_id)
            .map_err(|error| ObjectFailure::new(error, prior))?
        {
            return Ok(ObjectReceipt {
                value: true,
                work: prior,
            });
        }
        let receipt = self
            .inner
            .contains(
                object_id,
                prior
                    .remaining(budget)
                    .map_err(|error| ObjectFailure::new(error.into(), prior))?,
                cancellation,
            )
            .await
            .map_err(|failure| combine_failure(prior, failure))?;
        Ok(ObjectReceipt {
            value: receipt.value,
            work: prior
                .checked_add(receipt.work)
                .map_err(|error| ObjectFailure::new(error.into(), prior))?,
        })
    }
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/cache.rs"]
mod tests;
