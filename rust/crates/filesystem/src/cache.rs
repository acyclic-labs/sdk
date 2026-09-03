//! Bounded disposable immutable-object caching and single-flight reads.
//!
//! This layer owns no authority. Every entry is keyed by a canonical typed
//! object identity and may be discarded without changing filesystem results.

use crate::async_storage::{
    AsyncObjectStore, DecodedCacheAdmission, DecodedCacheKey, DecodedCacheValue,
};
use crate::cancellation::CancellationToken;
use crate::performance::{WorkBudget, WorkCounters};
use crate::storage::{
    ObjectFailure, ObjectId, ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectReceipt,
    ObjectResult, ObjectStoreError,
};
use bytes::Bytes;
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
    Running(Vec<Waker>),
    Complete(Option<Bytes>),
}

struct Flight {
    state: Mutex<FlightState>,
    maximum_waiters: u32,
}

impl Flight {
    fn new(maximum_waiters: u32) -> Self {
        Self {
            state: Mutex::new(FlightState::Running(Vec::new())),
            maximum_waiters,
        }
    }

    fn finish(&self, bytes: Option<Bytes>) {
        let waiters = match self.state.lock() {
            Ok(mut state) => match std::mem::replace(&mut *state, FlightState::Complete(bytes)) {
                FlightState::Running(waiters) => waiters,
                FlightState::Complete(_) => return,
            },
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                match std::mem::replace(&mut *state, FlightState::Complete(None)) {
                    FlightState::Running(waiters) => waiters,
                    FlightState::Complete(_) => return,
                }
            }
        };
        for waiter in waiters {
            waiter.wake();
        }
    }

    fn poll(&self, context: &Context<'_>) -> Poll<Result<Option<Bytes>, ObjectStoreError>> {
        let mut state = self.state.lock().map_err(|_| ObjectStoreError::Corrupt)?;
        match &mut *state {
            FlightState::Complete(bytes) => Poll::Ready(Ok(bytes.clone())),
            FlightState::Running(waiters) => {
                if waiters
                    .iter()
                    .any(|waiter| waiter.will_wake(context.waker()))
                {
                    return Poll::Pending;
                }
                if waiters.len() >= usize::try_from(self.maximum_waiters).unwrap_or(usize::MAX) {
                    return Poll::Ready(Err(ObjectStoreError::Rejected(
                        "immutable-object single-flight waiter bound exhausted".to_owned(),
                    )));
                }
                waiters.push(context.waker().clone());
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

struct PreparedBatch {
    slots: Vec<BatchSlot>,
    leaders: Vec<BatchLeader>,
    work: WorkCounters,
    planning_bytes: u64,
    result_bytes: u64,
    item_count: u64,
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

    fn read_plan(&self, object_id: ObjectId) -> Result<ReadPlan, ObjectStoreError> {
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
        let flight = Arc::new(Flight::new(self.shared.options.maximum_waiters_per_object));
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

async fn wait_for_flight(
    flight: &Flight,
    cancellation: &CancellationToken,
) -> Result<Option<Bytes>, ObjectStoreError> {
    let mut cancelled = pin!(cancellation.cancelled());
    poll_fn(|context| {
        if cancelled.as_mut().poll(context).is_ready() {
            return Poll::Ready(Err(ObjectStoreError::Cancelled));
        }
        flight.poll(context)
    })
    .await
}

impl<S: AsyncObjectStore> CachedObjectStore<S> {
    fn validate_cached_read(
        _object_id: ObjectId,
        bytes: Bytes,
        maximum_bytes: u64,
        work: WorkCounters,
    ) -> ObjectResult<ObjectRead> {
        let observed = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if observed > maximum_bytes {
            return Err(ObjectFailure::new(
                ObjectStoreError::TooLarge {
                    observed,
                    maximum: maximum_bytes,
                },
                work,
            ));
        }
        Ok(shared_read(bytes, work))
    }

    async fn load_leader(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        prior: WorkCounters,
        flight: Arc<Flight>,
    ) -> ObjectResult<ObjectRead> {
        let guard = ActiveFlightGuard::new(self, object_id, Arc::clone(&flight));
        let receipt = match self
            .inner
            .read(
                object_id,
                maximum_bytes,
                prior
                    .remaining(budget)
                    .map_err(|error| ObjectFailure::new(error.into(), prior))?,
                cancellation,
            )
            .await
        {
            Ok(receipt) => receipt,
            Err(failure) => {
                guard
                    .finish(None)
                    .map_err(|error| ObjectFailure::new(error, prior))?;
                return Err(combine_failure(prior, failure));
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
        Ok(ObjectReceipt {
            value: receipt.value,
            work,
        })
    }

    fn prepare_batch(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<PreparedBatch, ObjectFailure> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        if requests.is_empty() {
            return Err(ObjectFailure::before_work(ObjectStoreError::Rejected(
                "object read batch is empty".to_owned(),
            )));
        }
        let item_count = u64::try_from(requests.len()).unwrap_or(u64::MAX);
        let result_bytes = item_count
            .checked_mul(u64::try_from(size_of::<ObjectRead>()).unwrap_or(u64::MAX))
            .ok_or_else(|| ObjectFailure::before_work(crate::WorkError::Overflow.into()))?;
        let planning_bytes = item_count
            .checked_mul(
                u64::try_from(
                    size_of::<BatchSlot>()
                        + size_of::<BatchLeader>()
                        + size_of::<ObjectReadRequest>(),
                )
                .unwrap_or(u64::MAX),
            )
            .ok_or_else(|| ObjectFailure::before_work(crate::WorkError::Overflow.into()))?;
        let mut work = WorkCounters {
            object_probes: item_count,
            items_examined: item_count,
            items_returned: item_count,
            allocation_operations: 4,
            peak_allocation_bytes: planning_bytes.saturating_add(result_bytes),
            ..WorkCounters::default()
        };
        work.verify(budget)
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        work.items_returned = 0;
        let mut slots = Vec::new();
        let mut leaders = Vec::new();
        let mut local_leaders = BTreeMap::<ObjectId, usize>::new();
        slots.try_reserve_exact(requests.len()).map_err(|_| {
            ObjectFailure::before_work(ObjectStoreError::Rejected(
                "object cache batch plan allocation failed".to_owned(),
            ))
        })?;
        leaders.try_reserve_exact(requests.len()).map_err(|_| {
            ObjectFailure::before_work(ObjectStoreError::Rejected(
                "object cache leader allocation failed".to_owned(),
            ))
        })?;
        for request in requests {
            match self.prepare_batch_slot(request, &mut leaders, &mut local_leaders, work) {
                Ok(slot) => slots.push(slot),
                Err(failure) => {
                    self.abandon_unstarted_leaders(&leaders);
                    return Err(failure);
                }
            }
        }
        Ok(PreparedBatch {
            slots,
            leaders,
            work,
            planning_bytes,
            result_bytes,
            item_count,
        })
    }

    fn prepare_batch_slot(
        &self,
        request: &ObjectReadRequest,
        leaders: &mut Vec<BatchLeader>,
        local_leaders: &mut BTreeMap<ObjectId, usize>,
        work: WorkCounters,
    ) -> Result<BatchSlot, ObjectFailure> {
        match self
            .read_plan(request.object_id)
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
                    && Arc::ptr_eq(&leaders[index].flight, &flight)
                {
                    leaders[index].maximum_bytes =
                        leaders[index].maximum_bytes.max(request.maximum_bytes);
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
    ) -> ObjectResult<Vec<ObjectRead>> {
        if prepared.leaders.is_empty() {
            return Ok(ObjectReceipt {
                value: Vec::new(),
                work: prepared.work,
            });
        }
        let mut requests = Vec::new();
        requests
            .try_reserve_exact(prepared.leaders.len())
            .map_err(|_| {
                ObjectFailure::new(
                    ObjectStoreError::Rejected(
                        "object cache backend batch allocation failed".to_owned(),
                    ),
                    prepared.work,
                )
            })?;
        requests.extend(prepared.leaders.iter().map(|leader| ObjectReadRequest {
            object_id: leader.object_id,
            maximum_bytes: leader.maximum_bytes,
        }));
        let mut guards = prepared
            .leaders
            .iter()
            .map(|leader| {
                Some(ActiveFlightGuard::new(
                    self,
                    leader.object_id,
                    Arc::clone(&leader.flight),
                ))
            })
            .collect::<Vec<_>>();
        let receipt = match self
            .inner
            .read_many(
                &requests,
                prepared
                    .work
                    .remaining(budget)
                    .map_err(|error| ObjectFailure::new(error.into(), prepared.work))?,
                cancellation,
            )
            .await
        {
            Ok(receipt) => receipt,
            Err(failure) => {
                Self::abort_batch_leaders(&mut guards);
                return Err(combine_failure(prepared.work, failure));
            }
        };
        let mut work = prepared
            .work
            .checked_add(receipt.work)
            .map_err(|error| ObjectFailure::new(error.into(), prepared.work))?;
        if receipt.value.len() != prepared.leaders.len() {
            Self::abort_batch_leaders(&mut guards);
            return Err(ObjectFailure::new(ObjectStoreError::Corrupt, work));
        }
        let mut values = receipt.value;
        for (index, leader) in prepared.leaders.iter().enumerate() {
            let bytes = values[index].bytes.clone();
            let retained = self
                .insert(leader.object_id, bytes.clone())
                .map_err(|error| ObjectFailure::new(error, work))?;
            if retained {
                values[index].retention = ObjectReadRetention::Shared;
            }
            if let Some(guard) = guards[index].take() {
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
        Ok(ObjectReceipt {
            value: values,
            work,
        })
    }

    fn abort_batch_leaders(guards: &mut [Option<ActiveFlightGuard<'_, S>>]) {
        for guard in guards {
            if let Some(guard) = guard.take() {
                let _ = guard.finish(None);
            }
        }
    }

    fn abandon_unstarted_leaders(&self, leaders: &[BatchLeader]) {
        for leader in leaders {
            leader.flight.finish(None);
            let _ = self.remove_flight(leader.object_id, &leader.flight);
        }
    }

    async fn resolve_prepared_batch(
        &self,
        requests: &[ObjectReadRequest],
        prepared: PreparedBatch,
        leaders: ObjectReceipt<Vec<ObjectRead>>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        let mut work = leaders.work;
        let mut values = Vec::new();
        values.try_reserve_exact(requests.len()).map_err(|_| {
            ObjectFailure::new(
                ObjectStoreError::Rejected("object cache result allocation failed".to_owned()),
                work,
            )
        })?;
        for (request, slot) in requests.iter().zip(prepared.slots) {
            cancellation
                .check()
                .map_err(|_| ObjectFailure::new(ObjectStoreError::Cancelled, work))?;
            let value = self
                .resolve_batch_slot(
                    request,
                    slot,
                    &leaders.value,
                    &mut work,
                    budget,
                    cancellation,
                )
                .await?;
            values.push(value);
        }
        work.items_returned = prepared.item_count;
        work.verify(budget)
            .map_err(|error| ObjectFailure::new(error.into(), work))?;
        Ok(ObjectReceipt {
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
    ) -> Result<ObjectRead, ObjectFailure> {
        let bytes = match slot {
            BatchSlot::Hit(bytes) => bytes,
            BatchSlot::LocalLeader(index) => leader_values
                .get(index)
                .ok_or_else(|| ObjectFailure::new(ObjectStoreError::Corrupt, *work))?
                .bytes
                .clone(),
            BatchSlot::ExternalFollower(flight) => {
                let result = wait_for_flight(&flight, cancellation)
                    .await
                    .map_err(|error| ObjectFailure::new(error, *work))?;
                self.record_coalesced()
                    .map_err(|error| ObjectFailure::new(error, *work))?;
                if let Some(bytes) = result {
                    bytes
                } else {
                    self.remove_flight(request.object_id, &flight)
                        .map_err(|error| ObjectFailure::new(error, *work))?;
                    let receipt = self
                        .read(
                            request.object_id,
                            request.maximum_bytes,
                            work.remaining(budget)
                                .map_err(|error| ObjectFailure::new(error.into(), *work))?,
                            cancellation,
                        )
                        .await
                        .map_err(|failure| combine_failure(*work, failure))?;
                    *work = work
                        .checked_add(receipt.work)
                        .map_err(|error| ObjectFailure::new(error.into(), *work))?;
                    return Ok(receipt.value);
                }
            }
        };
        Ok(Self::validate_cached_read(
            request.object_id,
            bytes,
            request.maximum_bytes,
            WorkCounters::default(),
        )?
        .value)
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

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let mut prior = cache_probe_work();
        prior
            .verify(budget)
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        loop {
            cancellation
                .check()
                .map_err(|_| ObjectFailure::new(ObjectStoreError::Cancelled, prior))?;
            match self
                .read_plan(object_id)
                .map_err(|error| ObjectFailure::new(error, prior))?
            {
                ReadPlan::Hit(bytes) => {
                    return Self::validate_cached_read(object_id, bytes, maximum_bytes, prior);
                }
                ReadPlan::Leader(flight) => {
                    return self
                        .load_leader(
                            object_id,
                            maximum_bytes,
                            budget,
                            cancellation,
                            prior,
                            flight,
                        )
                        .await;
                }
                ReadPlan::Follower(flight) => {
                    let result = wait_for_flight(&flight, cancellation)
                        .await
                        .map_err(|error| ObjectFailure::new(error, prior))?;
                    self.record_coalesced()
                        .map_err(|error| ObjectFailure::new(error, prior))?;
                    if let Some(bytes) = result {
                        return Self::validate_cached_read(object_id, bytes, maximum_bytes, prior);
                    }
                    self.remove_flight(object_id, &flight)
                        .map_err(|error| ObjectFailure::new(error, prior))?;
                    prior = prior
                        .checked_add(cache_probe_work())
                        .map_err(|error| ObjectFailure::new(error.into(), prior))?;
                    prior
                        .verify(budget)
                        .map_err(|error| ObjectFailure::new(error.into(), prior))?;
                }
            }
        }
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        let prepared = self.prepare_batch(requests, budget, cancellation)?;
        let leaders = self
            .load_batch_leaders(&prepared, budget, cancellation)
            .await?;
        self.resolve_prepared_batch(requests, prepared, leaders, budget, cancellation)
            .await
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
