//! Bounded disposable immutable-object caching and single-flight reads.
//!
//! This layer owns no authority. Every entry is keyed by a canonical typed
//! object identity and may be discarded without changing filesystem results.

use crate::async_storage::{
    AsyncObjectStore, DecodedCacheAdmission, DecodedCacheKey, DecodedCacheValue,
};
use crate::cancellation::CancellationToken;
#[cfg(not(target_arch = "wasm32"))]
use crate::kernel::allocation::KernelAccounting;
use crate::kernel::{Owned, allocation::KernelOwnedAllocation};
#[cfg(not(target_arch = "wasm32"))]
use crate::materializer::{NativeAllocation, NativeMaterializationWork};
use crate::performance::{WorkBudget, WorkCounters};
use crate::storage::{
    ObjectFailure, ObjectId, ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectStoreError,
    ObjectWrite,
};
use acyclic_stream::OwnedBytes;
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
    bytes: OwnedBytes,
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
    Running(Owned<Vec<Waker>>),
    Complete(Option<OwnedBytes>),
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
            state: Mutex::new(FlightState::Running(Owned::new(
                Vec::new(),
                KernelOwnedAllocation::Standalone,
                0,
            ))),
            maximum_waiters,
            #[cfg(not(target_arch = "wasm32"))]
            _allocation: None,
        }
    }

    fn finish(&self, bytes: Option<OwnedBytes>) {
        let waiters = {
            let (mut state, bytes) = match self.state.lock() {
                Ok(state) => (state, bytes),
                Err(poisoned) => (poisoned.into_inner(), None),
            };
            if matches!(*state, FlightState::Complete(_)) {
                return;
            }
            match std::mem::replace(&mut *state, FlightState::Complete(bytes)) {
                FlightState::Running(waiters) => waiters,
                FlightState::Complete(_) => unreachable!(),
            }
        };
        for waiter in waiters.value {
            waiter.wake();
        }
    }

    fn poll_with_context(
        &self,
        context: &Context<'_>,
        #[cfg(not(target_arch = "wasm32"))] work: Option<&NativeMaterializationWork>,
    ) -> Poll<Result<Option<OwnedBytes>, ObjectStoreError>> {
        let mut state = self.state.lock().map_err(|_| ObjectStoreError::Corrupt)?;
        match &mut *state {
            FlightState::Complete(bytes) => Poll::Ready(Ok(bytes.clone())),
            FlightState::Running(waiters) => {
                if waiters
                    .value
                    .iter()
                    .any(|waiter| waiter.will_wake(context.waker()))
                {
                    return Poll::Pending;
                }
                if waiters.value.len()
                    >= usize::try_from(self.maximum_waiters).unwrap_or(usize::MAX)
                {
                    return Poll::Ready(Err(ObjectStoreError::Rejected(
                        "immutable-object single-flight waiter bound exhausted".to_owned(),
                    )));
                }
                #[cfg(not(target_arch = "wasm32"))]
                if waiters.value.len() == waiters.value.capacity()
                    && (work.is_some()
                        || matches!(waiters.allocation, KernelOwnedAllocation::NativeOriginal(_)))
                {
                    // Fund the actual replacement scope before entering its
                    // allocator; never construct or drop storage under a root loan.
                    let capacity = waiters
                        .value
                        .len()
                        .saturating_mul(2)
                        .max(1)
                        .min(usize::try_from(self.maximum_waiters).unwrap_or(usize::MAX));
                    let mut replacement =
                        allocate_vec(capacity, "immutable-object waiter allocation failed", work)?;
                    let copy = work
                        .map(|work| {
                            work.reserve_work(crate::WorkCounters {
                                bytes_copied: (waiters.value.len() * size_of::<Waker>()) as u64,
                                ..crate::WorkCounters::UNCHARGED
                            })
                        })
                        .transpose()?;
                    replacement.value.append(&mut waiters.value);
                    let copied = (replacement.value.len() * size_of::<Waker>()) as u64;
                    // Commit moved waiters before a late refusal, preserving all
                    // prior registrations. Free old storage before its guard.
                    drop(std::mem::replace(waiters, replacement));
                    if let Some(copy) = copy {
                        {
                            let copy = copy;
                            copy.record_completed(crate::WorkCounters {
                                bytes_copied: copied,
                                ..crate::WorkCounters::UNCHARGED
                            })
                        }?;
                    }
                    if let Some(work) = work {
                        KernelAccounting::native(work, work.cancellation())
                            .record_completed(WorkCounters::UNCHARGED)
                            .map_err(crate::async_storage::object_error)?;
                    }
                }
                waiters.value.push(context.waker().clone());
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

impl<S> ActiveFlightGuard<'_, S> {
    fn finish(mut self, bytes: Option<OwnedBytes>) -> Result<(), ObjectStoreError> {
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
    Hit(OwnedBytes),
    Leader(Arc<Flight>),
    Follower(Arc<Flight>),
}

enum BatchSlot {
    Hit(OwnedBytes),
    LocalLeader(usize),
    ExternalFollower(Arc<Flight>),
}

struct BatchLeader {
    object_id: ObjectId,
    maximum_bytes: u64,
    flight: Arc<Flight>,
}

fn allocate_vec<T>(
    count: usize,
    failure: &'static str,
    #[cfg(not(target_arch = "wasm32"))] native: Option<&NativeMaterializationWork>,
) -> Result<Owned<Vec<T>>, ObjectStoreError> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(native) = native {
        return Owned::allocate(
            count,
            &mut KernelAccounting::native(native, native.cancellation()),
        )
        .map_err(crate::async_storage::object_error);
    }
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| ObjectStoreError::Rejected(failure.to_owned()))?;
    let bytes = (values.capacity() * size_of::<T>()) as u64;
    Ok(Owned::new(values, KernelOwnedAllocation::Standalone, bytes))
}

struct PreparedBatch {
    slots: Owned<Vec<BatchSlot>>,
    leaders: Owned<Vec<BatchLeader>>,
    work: WorkCounters,
    planning_bytes: u64,
    result_bytes: u64,
    item_count: u64,
    shared: Arc<Shared>,
}

impl Drop for PreparedBatch {
    fn drop(&mut self) {
        for leader in &self.leaders.value {
            leader.flight.finish(None);
            if let Ok(mut state) = self.shared.state.lock()
                && state
                    .flights
                    .get(&leader.object_id)
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
        let allocation = work
            .map(|work| {
                let claim_bytes = size_of::<Flight>() as u64;
                work.claim(
                    claim_bytes,
                    u64::from(claim_bytes != 0),
                    crate::WorkCounters::UNCHARGED,
                )
            })
            .transpose()?;
        let flight = Flight::new(self.shared.options.maximum_waiters_per_object);
        #[cfg(not(target_arch = "wasm32"))]
        let flight = Flight {
            _allocation: allocation,
            ..flight
        };
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

    fn insert(&self, object_id: ObjectId, bytes: OwnedBytes) -> Result<bool, ObjectStoreError> {
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

fn shared_read(bytes: OwnedBytes, work: WorkCounters) -> crate::OperationReceipt<ObjectRead> {
    crate::OperationReceipt {
        value: ObjectRead {
            bytes,
            retention: ObjectReadRetention::Shared,
        },
        work,
    }
}

async fn wait_for_flight_with_context(
    flight: &Flight,
    cancellation: &CancellationToken,
    #[cfg(not(target_arch = "wasm32"))] work: Option<&NativeMaterializationWork>,
) -> Result<Option<OwnedBytes>, ObjectStoreError> {
    let mut cancelled = pin!(cancellation.cancelled());
    poll_fn(|context| {
        if cancelled.as_mut().poll(context).is_ready() {
            return Poll::Ready(Err(ObjectStoreError::Cancelled));
        }
        flight.poll_with_context(
            context,
            #[cfg(not(target_arch = "wasm32"))]
            work,
        )
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
    ) -> Result<crate::OperationReceipt<ObjectRead>, ObjectFailure> {
        let guard = ActiveFlightGuard {
            store: self,
            object_id,
            flight: Arc::clone(&flight),
            finished: false,
        };
        let load = async {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(native) = native {
                return {
                    let __object_args = (object_id, maximum_bytes, native, cancellation);
                    let mut __object_work =
                        crate::AuthorityWork::native(__object_args.2, __object_args.3);
                    self.inner
                        .read(__object_args.0, __object_args.1, &mut __object_work)
                        .await
                }
                .map(|read| crate::OperationReceipt {
                    value: read,
                    work: WorkCounters::default(),
                })
                .map_err(|error| ObjectFailure::new(error, prior));
            }
            let remaining = prior
                .remaining(budget)
                .map_err(|error| ObjectFailure::new(error.into(), prior))?;
            {
                let __object_args = (object_id, maximum_bytes, remaining, cancellation);
                let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
                let __object_result = self
                    .inner
                    .read(__object_args.0, __object_args.1, &mut __object_work)
                    .await;
                __object_work.receipt(__object_result)
            }
            .map_err(|failure| {
                let work = prior.checked_add(*failure.work);
                match work {
                    Ok(work) => ObjectFailure::new(failure.error, work),
                    Err(error) => ObjectFailure::new(error.into(), prior),
                }
            })
        }
        .await;
        let receipt = match load {
            Ok(receipt) => receipt,
            Err(failure) => {
                guard
                    .finish(None)
                    .map_err(|error| ObjectFailure::new(error, prior))?;
                return Err(failure);
            }
        };
        let work = prior
            .checked_add(receipt.work)
            .map_err(|error| ObjectFailure::new(error.into(), prior))?;
        let bytes = receipt.value.bytes.clone();
        if self
            .insert(object_id, bytes.clone())
            .map_err(|error| ObjectFailure::new(error, work))?
        {
            guard
                .finish(Some(bytes.clone()))
                .map_err(|error| ObjectFailure::new(error, work))?;
            return Ok(shared_read(bytes, work));
        }
        guard
            .finish(None)
            .map_err(|error| ObjectFailure::new(error, work))?;
        Ok(crate::OperationReceipt {
            value: receipt.value,
            work,
        })
    }

    async fn read_engine(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        #[cfg(not(target_arch = "wasm32"))] native: Option<&NativeMaterializationWork>,
    ) -> Result<crate::OperationReceipt<ObjectRead>, ObjectFailure> {
        cancellation.check().map_err(|_| {
            ObjectFailure::new(ObjectStoreError::Cancelled, WorkCounters::default())
        })?;
        let mut prior = cache_probe_work();
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(native) = native {
            native
                .charge(prior)
                .map_err(|error| ObjectFailure::new(error.into(), WorkCounters::default()))?;
        } else {
            prior
                .verify(budget)
                .map_err(|error| ObjectFailure::new(error.into(), WorkCounters::default()))?;
        }
        #[cfg(target_arch = "wasm32")]
        prior
            .verify(budget)
            .map_err(|error| ObjectFailure::new(error.into(), WorkCounters::default()))?;
        loop {
            cancellation
                .check()
                .map_err(|_| ObjectFailure::new(ObjectStoreError::Cancelled, prior))?;
            let plan = self
                .read_plan_with_context(
                    object_id,
                    #[cfg(not(target_arch = "wasm32"))]
                    native,
                )
                .map_err(|error| ObjectFailure::new(error, prior))?;
            let bytes = match plan {
                ReadPlan::Hit(bytes) => bytes,
                ReadPlan::Leader(flight) => {
                    return self
                        .load_leader(
                            object_id,
                            maximum_bytes,
                            budget,
                            cancellation,
                            prior,
                            flight,
                            #[cfg(not(target_arch = "wasm32"))]
                            native,
                        )
                        .await;
                }
                ReadPlan::Follower(flight) => {
                    let result = wait_for_flight_with_context(
                        &flight,
                        cancellation,
                        #[cfg(not(target_arch = "wasm32"))]
                        native,
                    )
                    .await
                    .map_err(|error| ObjectFailure::new(error, prior))?;
                    self.record_coalesced()
                        .map_err(|error| ObjectFailure::new(error, prior))?;
                    if let Some(bytes) = result {
                        bytes
                    } else {
                        self.remove_flight(object_id, &flight)
                            .map_err(|error| ObjectFailure::new(error, prior))?;
                        #[cfg(not(target_arch = "wasm32"))]
                        if let Some(native) = native {
                            native
                                .charge(cache_probe_work())
                                .map_err(|error| ObjectFailure::new(error.into(), prior))?;
                        }
                        prior = prior
                            .checked_add(cache_probe_work())
                            .map_err(|error| ObjectFailure::new(error.into(), prior))?;
                        #[cfg(not(target_arch = "wasm32"))]
                        if native.is_none() {
                            prior
                                .verify(budget)
                                .map_err(|error| ObjectFailure::new(error.into(), prior))?;
                        }
                        #[cfg(target_arch = "wasm32")]
                        prior
                            .verify(budget)
                            .map_err(|error| ObjectFailure::new(error.into(), prior))?;
                        continue;
                    }
                }
            };
            let observed = bytes.len() as u64;
            if observed > maximum_bytes {
                return Err(ObjectFailure::new(
                    ObjectStoreError::TooLarge {
                        observed,
                        maximum: maximum_bytes,
                    },
                    prior,
                ));
            }
            return Ok(shared_read(bytes, prior));
        }
    }

    fn prepare_batch(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
        #[cfg(not(target_arch = "wasm32"))] native: Option<&NativeMaterializationWork>,
    ) -> Result<PreparedBatch, ObjectFailure> {
        cancellation.check().map_err(|_| {
            ObjectFailure::new(ObjectStoreError::Cancelled, WorkCounters::default())
        })?;
        if requests.is_empty() {
            return Err(ObjectFailure::new(
                ObjectStoreError::Rejected("object read batch is empty".to_owned()),
                WorkCounters::default(),
            ));
        }
        let item_count = requests.len() as u64;
        let result_bytes = item_count
            .checked_mul(size_of::<ObjectRead>() as u64)
            .ok_or(ObjectFailure::new(
                crate::WorkError::Overflow.into(),
                WorkCounters::default(),
            ))?;
        let planning_bytes = item_count
            .checked_mul(
                (size_of::<BatchSlot>() + size_of::<BatchLeader>() + size_of::<ObjectReadRequest>())
                    as u64,
            )
            .ok_or(ObjectFailure::new(
                crate::WorkError::Overflow.into(),
                WorkCounters::default(),
            ))?;
        let mut work = WorkCounters {
            object_probes: item_count,
            items_examined: item_count,
            items_returned: item_count,
            allocation_operations: 4,
            peak_allocation_bytes: planning_bytes.saturating_add(result_bytes),
            ..WorkCounters::default()
        };
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(native) = native {
            native
                .admit(WorkCounters {
                    items_returned: item_count,
                    ..WorkCounters::default()
                })
                .map_err(|error| ObjectFailure::new(error.into(), WorkCounters::default()))?;
            native
                .charge(WorkCounters {
                    object_probes: item_count,
                    items_examined: item_count,
                    ..WorkCounters::default()
                })
                .map_err(|error| ObjectFailure::new(error.into(), WorkCounters::default()))?;
        } else {
            work.verify(budget)
                .map_err(|error| ObjectFailure::new(error.into(), WorkCounters::default()))?;
        }
        #[cfg(target_arch = "wasm32")]
        work.verify(budget)
            .map_err(|error| ObjectFailure::new(error.into(), WorkCounters::default()))?;
        work.items_returned = 0;
        let slots = allocate_vec(
            requests.len(),
            "object cache batch plan allocation failed",
            #[cfg(not(target_arch = "wasm32"))]
            native,
        )
        .map_err(|error| ObjectFailure::new(error, work))?;
        let leaders = allocate_vec(
            requests.len(),
            "object cache leader allocation failed",
            #[cfg(not(target_arch = "wasm32"))]
            native,
        )
        .map_err(|error| ObjectFailure::new(error, work))?;
        let mut prepared = PreparedBatch {
            slots,
            leaders,
            work,
            planning_bytes,
            result_bytes,
            item_count,
            shared: Arc::clone(&self.shared),
        };
        let mut local_leaders = BTreeMap::<ObjectId, usize>::new();
        for request in requests {
            let slot = self.prepare_batch_slot(
                request,
                &mut prepared.leaders.value,
                &mut local_leaders,
                work,
                #[cfg(not(target_arch = "wasm32"))]
                native,
            )?;
            prepared.slots.value.push(slot);
        }
        Ok(prepared)
    }

    fn prepare_batch_slot(
        &self,
        request: &ObjectReadRequest,
        leaders: &mut Vec<BatchLeader>,
        local_leaders: &mut BTreeMap<ObjectId, usize>,
        work: WorkCounters,
        #[cfg(not(target_arch = "wasm32"))] native: Option<&NativeMaterializationWork>,
    ) -> Result<BatchSlot, ObjectFailure> {
        match self
            .read_plan_with_context(
                request.object_id,
                #[cfg(not(target_arch = "wasm32"))]
                native,
            )
            .map_err(|error| ObjectFailure::new(error, work))?
        {
            ReadPlan::Hit(bytes) => Ok(BatchSlot::Hit(bytes)),
            ReadPlan::Leader(flight) => {
                let index = leaders.len();
                leaders.push(BatchLeader {
                    object_id: request.object_id,
                    maximum_bytes: request.maximum_bytes,
                    flight: Arc::clone(&flight),
                });
                local_leaders.insert(request.object_id, index);
                Ok(BatchSlot::LocalLeader(index))
            }
            ReadPlan::Follower(flight) => {
                if let Some(&index) = local_leaders.get(&request.object_id)
                    && leaders
                        .get(index)
                        .is_some_and(|leader| Arc::ptr_eq(&leader.flight, &flight))
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
        &self,
        prepared: &PreparedBatch,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        #[cfg(not(target_arch = "wasm32"))] native: Option<&NativeMaterializationWork>,
    ) -> Result<crate::OperationReceipt<Owned<Vec<ObjectRead>>>, ObjectFailure> {
        if prepared.leaders.value.is_empty() {
            return Ok(crate::OperationReceipt {
                value: allocate_vec(
                    0,
                    "object cache backend batch allocation failed",
                    #[cfg(not(target_arch = "wasm32"))]
                    native,
                )
                .map_err(|error| ObjectFailure::new(error, prepared.work))?,
                work: prepared.work,
            });
        }
        let mut requests = allocate_vec(
            prepared.leaders.value.len(),
            "object cache backend batch allocation failed",
            #[cfg(not(target_arch = "wasm32"))]
            native,
        )
        .map_err(|error| ObjectFailure::new(error, prepared.work))?;
        #[cfg(not(target_arch = "wasm32"))]
        let copy = native
            .map(|native| {
                native.reserve_work(crate::WorkCounters {
                    bytes_copied: (prepared.leaders.value.len() * size_of::<ObjectReadRequest>())
                        as u64,
                    ..crate::WorkCounters::UNCHARGED
                })
            })
            .transpose()
            .map_err(|error| ObjectFailure::new(error.into(), prepared.work))?;
        requests.value.extend(
            prepared
                .leaders
                .value
                .iter()
                .map(|leader| ObjectReadRequest {
                    object_id: leader.object_id,
                    maximum_bytes: leader.maximum_bytes,
                }),
        );
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(copy) = copy {
            {
                let copy = copy;
                copy.record_completed(crate::WorkCounters {
                    bytes_copied: (requests.value.len() * size_of::<ObjectReadRequest>()) as u64,
                    ..crate::WorkCounters::UNCHARGED
                })
            }
            .map_err(|error| ObjectFailure::new(error.into(), prepared.work))?;
        }
        let mut guards = allocate_vec(
            prepared.leaders.value.len(),
            "object cache flight guard allocation failed",
            #[cfg(not(target_arch = "wasm32"))]
            native,
        )
        .map_err(|error| ObjectFailure::new(error, prepared.work))?;
        guards
            .value
            .extend(prepared.leaders.value.iter().map(|leader| {
                Some(ActiveFlightGuard {
                    store: self,
                    object_id: leader.object_id,
                    flight: Arc::clone(&leader.flight),
                    finished: false,
                })
            }));
        let receipt = async {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(native) = native {
                let batch = {
                    let __object_args = (&requests.value, native, cancellation);
                    let mut __object_work =
                        crate::AuthorityWork::native(__object_args.1, __object_args.2);
                    let __object_result = self
                        .inner
                        .read_many(__object_args.0, &mut __object_work)
                        .await
                        .map(|(values, allocation)| {
                            (values, allocation.expect("native batch allocation"))
                        });
                    __object_result
                }
                .map_err(|error| ObjectFailure::new(error, prepared.work))?;
                let (values, allocation) = batch;
                let bytes = allocation.bytes();
                return Ok(crate::OperationReceipt {
                    value: Owned::new(
                        values,
                        KernelOwnedAllocation::NativeOriginal(allocation),
                        bytes,
                    ),
                    work: WorkCounters::default(),
                });
            }
            let remaining = prepared
                .work
                .remaining(budget)
                .map_err(|error| ObjectFailure::new(error.into(), prepared.work))?;
            {
                let __object_args = (&requests.value, remaining, cancellation);
                let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
                let __object_result = self
                    .inner
                    .read_many(__object_args.0, &mut __object_work)
                    .await
                    .map(|(values, _allocation)| values);
                __object_work.receipt(__object_result)
            }
            .map(|receipt| {
                let bytes = (receipt.value.capacity() * size_of::<ObjectRead>()) as u64;
                crate::OperationReceipt {
                    value: Owned::new(receipt.value, KernelOwnedAllocation::Standalone, bytes),
                    work: receipt.work,
                }
            })
            .map_err(|failure| match prepared.work.checked_add(*failure.work) {
                Ok(work) => ObjectFailure::new(failure.error, work),
                Err(error) => ObjectFailure::new(error.into(), prepared.work),
            })
        }
        .await?;
        let mut work = prepared
            .work
            .checked_add(receipt.work)
            .map_err(|error| ObjectFailure::new(error.into(), prepared.work))?;
        let mut values = receipt.value;
        if values.value.len() != prepared.leaders.value.len() {
            return Err(ObjectFailure::new(ObjectStoreError::Corrupt, work));
        }
        for ((leader, value), guard) in prepared
            .leaders
            .value
            .iter()
            .zip(values.value.iter_mut())
            .zip(guards.value.iter_mut())
        {
            let bytes = value.bytes.clone();
            let retained = self
                .insert(leader.object_id, bytes.clone())
                .map_err(|error| ObjectFailure::new(error, work))?;
            if retained {
                value.retention = ObjectReadRetention::Shared;
            }
            if let Some(guard) = guard.take() {
                guard
                    .finish(retained.then_some(bytes))
                    .map_err(|error| ObjectFailure::new(error, work))?;
            }
        }
        work.peak_allocation_bytes = work.peak_allocation_bytes.max(
            prepared
                .planning_bytes
                .saturating_add(prepared.result_bytes),
        );
        Ok(crate::OperationReceipt {
            value: values,
            work,
        })
    }

    async fn resolve_prepared_batch(
        &self,
        requests: &[ObjectReadRequest],
        mut prepared: PreparedBatch,
        leaders: crate::OperationReceipt<Owned<Vec<ObjectRead>>>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        #[cfg(not(target_arch = "wasm32"))] native: Option<&NativeMaterializationWork>,
    ) -> Result<crate::OperationReceipt<Owned<Vec<ObjectRead>>>, ObjectFailure> {
        let mut work = leaders.work;
        let mut values = allocate_vec(
            requests.len(),
            "object cache result allocation failed",
            #[cfg(not(target_arch = "wasm32"))]
            native,
        )
        .map_err(|error| ObjectFailure::new(error, work))?;
        for (request, slot) in requests
            .iter()
            .zip(std::mem::take(&mut prepared.slots.value))
        {
            cancellation
                .check()
                .map_err(|_| ObjectFailure::new(ObjectStoreError::Cancelled, work))?;
            let value = self
                .resolve_batch_slot(
                    request,
                    slot,
                    &leaders.value.value,
                    &mut work,
                    budget,
                    cancellation,
                    #[cfg(not(target_arch = "wasm32"))]
                    native,
                )
                .await?;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(native) = native {
                native
                    .charge(WorkCounters {
                        items_returned: 1,
                        ..WorkCounters::default()
                    })
                    .map_err(|error| ObjectFailure::new(error.into(), work))?;
            }
            values.value.push(value);
        }
        work.items_returned = prepared.item_count;
        #[cfg(not(target_arch = "wasm32"))]
        if native.is_none() {
            work.verify(budget)
                .map_err(|error| ObjectFailure::new(error.into(), work))?;
        }
        #[cfg(target_arch = "wasm32")]
        work.verify(budget)
            .map_err(|error| ObjectFailure::new(error.into(), work))?;
        Ok(crate::OperationReceipt {
            value: values,
            work,
        })
    }

    async fn resolve_batch_slot(
        &self,
        request: &ObjectReadRequest,
        slot: BatchSlot,
        leader_values: &[ObjectRead],
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        #[cfg(not(target_arch = "wasm32"))] native: Option<&NativeMaterializationWork>,
    ) -> Result<ObjectRead, ObjectFailure> {
        let bytes = match slot {
            BatchSlot::Hit(bytes) => bytes,
            BatchSlot::LocalLeader(index) => leader_values
                .get(index)
                .ok_or(ObjectFailure::new(ObjectStoreError::Corrupt, *work))?
                .bytes
                .clone(),
            BatchSlot::ExternalFollower(flight) => {
                let result = wait_for_flight_with_context(
                    &flight,
                    cancellation,
                    #[cfg(not(target_arch = "wasm32"))]
                    native,
                )
                .await
                .map_err(|error| ObjectFailure::new(error, *work))?;
                self.record_coalesced()
                    .map_err(|error| ObjectFailure::new(error, *work))?;
                if let Some(bytes) = result {
                    bytes
                } else {
                    self.remove_flight(request.object_id, &flight)
                        .map_err(|error| ObjectFailure::new(error, *work))?;
                    #[cfg(not(target_arch = "wasm32"))]
                    let remaining = match native {
                        Some(native) => native
                            .remaining()
                            .map_err(|error| ObjectFailure::new(error.into(), *work))?,
                        None => work
                            .remaining(budget)
                            .map_err(|error| ObjectFailure::new(error.into(), *work))?,
                    };
                    #[cfg(target_arch = "wasm32")]
                    let remaining = work
                        .remaining(budget)
                        .map_err(|error| ObjectFailure::new(error.into(), *work))?;
                    let receipt = self
                        .read_engine(
                            request.object_id,
                            request.maximum_bytes,
                            remaining,
                            cancellation,
                            #[cfg(not(target_arch = "wasm32"))]
                            native,
                        )
                        .await
                        .map_err(|failure| match work.checked_add(*failure.work) {
                            Ok(work) => ObjectFailure::new(failure.error, work),
                            Err(error) => ObjectFailure::new(error.into(), *work),
                        })?;
                    *work = work
                        .checked_add(receipt.work)
                        .map_err(|error| ObjectFailure::new(error.into(), *work))?;
                    return Ok(receipt.value);
                }
            }
        };
        let observed = bytes.len() as u64;
        if observed > request.maximum_bytes {
            return Err(ObjectFailure::new(
                ObjectStoreError::TooLarge {
                    observed,
                    maximum: request.maximum_bytes,
                },
                *work,
            ));
        }
        Ok(ObjectRead {
            bytes,
            retention: ObjectReadRetention::Shared,
        })
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

    fn collection(&self) -> Option<&Arc<crate::Collection>> {
        self.inner.collection()
    }

    fn collection_sweeps(&self) -> u64 {
        self.inner.collection_sweeps()
    }

    async fn historical_generation_evidence(
        &self,
        authority_id: crate::AuthorityId,
        record: &crate::DurableCommit,
        maximum_bytes: u64,
        maximum_objects: u64,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<Option<crate::kernel::HistoricalGenerationEvidence>, ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(ObjectStoreError::NativeUnavailable);
        }
        self.inner
            .historical_generation_evidence(
                authority_id,
                record,
                maximum_bytes,
                maximum_objects,
                context,
            )
            .await
    }

    async fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        context
            .accounting()
            .check()
            .map_err(crate::async_storage::object_error)?;
        let retained = bytes.clone();
        self.inner.put(object_id, bytes, context).await?;
        self.insert(object_id, retained)?;
        Ok(())
    }

    async fn put_hashed(
        &self,
        object: crate::storage::HashedObject,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        context
            .accounting()
            .check()
            .map_err(crate::async_storage::object_error)?;
        let object_id = object.object_id();
        let retained = object.bytes().clone();
        self.inner.put_hashed(object, context).await?;
        self.insert(object_id, retained)?;
        Ok(())
    }

    async fn flush_before_publish(
        &self,
        scope: crate::async_storage::PublicationScope<'_>,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::PublicationHold, ObjectStoreError> {
        self.inner.flush_before_publish(scope, context).await
    }

    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        context
            .accounting()
            .check()
            .map_err(crate::async_storage::object_error)?;
        self.inner.put_many(writes, context).await?;
        for write in writes {
            self.insert(write.object_id, write.bytes.clone())?;
        }
        Ok(())
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        let budget = context.read_budget()?;
        #[cfg(not(target_arch = "wasm32"))]
        let budget = match context.original {
            Some(work) => work.remaining()?,
            None => budget,
        };
        let result = self
            .read_engine(
                object_id,
                maximum_bytes,
                budget,
                context.cancellation,
                #[cfg(not(target_arch = "wasm32"))]
                context.original,
            )
            .await;
        let mut read = context.accept(result)?;
        if context.is_native() {
            read.retention = ObjectReadRetention::Shared;
        }
        Ok(read)
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        let budget = context.work.remaining(context.budget)?;
        #[cfg(not(target_arch = "wasm32"))]
        let budget = match context.original {
            Some(work) => work.remaining()?,
            None => budget,
        };
        #[cfg(not(target_arch = "wasm32"))]
        let native = context.original;
        let cancellation = context.cancellation;
        let result = async {
            let prepared = self.prepare_batch(
                requests,
                budget,
                cancellation,
                #[cfg(not(target_arch = "wasm32"))]
                native,
            )?;
            let leaders = self
                .load_batch_leaders(
                    &prepared,
                    budget,
                    cancellation,
                    #[cfg(not(target_arch = "wasm32"))]
                    native,
                )
                .await?;
            self.resolve_prepared_batch(
                requests,
                prepared,
                leaders,
                budget,
                cancellation,
                #[cfg(not(target_arch = "wasm32"))]
                native,
            )
            .await
        }
        .await
        .and_then(|receipt| {
                let Owned { value: values, allocation, .. } = receipt.value;
                #[cfg(not(target_arch = "wasm32"))]
                let allocation = match allocation {
                    KernelOwnedAllocation::NativeOriginal(allocation) => Some(allocation),
                    KernelOwnedAllocation::Standalone if !context.is_native() => None,
                    _ => return Err(ObjectFailure::new(ObjectStoreError::Corrupt, receipt.work)),
                };
                #[cfg(target_arch = "wasm32")]
                let allocation = None;
                Ok(crate::OperationReceipt {
                    value: (values, allocation),
                    work: receipt.work,
                })
            });
        context.accept(result)
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.is_native() {
            return Err(ObjectStoreError::NativeUnavailable);
        }
        context
            .accounting()
            .charge(cache_probe_work())
            .map_err(crate::async_storage::object_error)?;
        if self.cached_contains(object_id)? {
            return Ok(true);
        }
        self.inner.contains(object_id, context).await
    }
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/cache/mod.rs"]
mod tests;
