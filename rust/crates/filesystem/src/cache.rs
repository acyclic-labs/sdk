//! Bounded disposable immutable-object caching and single-flight reads.
//!
//! This layer owns no authority. Every entry is keyed by a canonical typed
//! object identity and may be discarded without changing filesystem results.

#[cfg(all(test, feature = "memory"))]
use crate::WorkBudget;
use crate::async_storage::{
    AsyncObjectStore, DecodedCacheAdmission, DecodedCacheKey, DecodedCacheValue,
};
use crate::cancellation::CancellationToken;
#[cfg(not(target_arch = "wasm32"))]
use crate::kernel::allocation::KernelAccounting;
use crate::kernel::{Owned, allocation::KernelOwnedAllocation};
#[cfg(not(target_arch = "wasm32"))]
use crate::materializer::{NativeAllocation, NativeMaterializationWork};
use crate::performance::WorkCounters;
use crate::storage::{
    ObjectId, ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectStoreError, ObjectWrite,
};
use crate::sync::{Mutex, MutexGuard};
use acyclic_stream::OwnedBytes;
use std::collections::{BTreeMap, BTreeSet};
use std::future::{Future, poll_fn};
use std::mem::size_of;
use std::pin::pin;
use std::sync::Arc;
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

struct CacheState {
    entries: BTreeMap<ObjectId, CacheEntry>,
    decoded: BTreeMap<DecodedCacheKey, DecodedCacheEntry>,
    recency: BTreeSet<(u64, RecencyKey)>,
    flights: BTreeMap<ObjectId, Arc<Flight>>,
    resident_bytes: u64,
    resident_canonical_bytes: u64,
    resident_decoded_bytes: u64,
    clock: u64,
    hits: u64,
    decoded_hits: u64,
    misses: u64,
    coalesced_reads: u64,
    evictions: u64,
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
            hits: 0,
            decoded_hits: 0,
            misses: 0,
            coalesced_reads: 0,
            evictions: 0,
        }
    }
}

struct Shared {
    options: ObjectCacheOptions,
    // Cache/view precedes any retained payload custody; never held over provider I/O.
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

    fn poll(
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
                    let mut replacement = if let Some(work) = work {
                        Owned::allocate(
                            capacity,
                            &mut KernelAccounting::native(work, work.cancellation()),
                        )
                        .map_err(crate::async_storage::object_error)?
                    } else {
                        let mut values = Vec::new();
                        values.try_reserve_exact(capacity).map_err(|_| {
                            ObjectStoreError::Rejected(
                                "immutable-object waiter allocation failed".to_owned(),
                            )
                        })?;
                        let bytes = (values.capacity() * size_of::<Waker>()) as u64;
                        Owned::new(values, KernelOwnedAllocation::Standalone, bytes)
                    };
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
                        copy.record_completed(crate::WorkCounters {
                            bytes_copied: copied,
                            ..crate::WorkCounters::UNCHARGED
                        })?;
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
}

impl<S> ActiveFlightGuard<'_, S> {
    fn finish(self, bytes: Option<OwnedBytes>) -> Result<(), ObjectStoreError> {
        self.flight.finish(bytes);
        self.store.remove_flight(self.object_id, &self.flight)?;
        Ok(())
    }
}

impl<S> Drop for ActiveFlightGuard<'_, S> {
    fn drop(&mut self) {
        self.flight.finish(None);
        let _ = self.store.remove_flight(self.object_id, &self.flight);
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

struct PreparedBatch {
    slots: Owned<Vec<BatchSlot>>,
    leaders: Owned<Vec<BatchLeader>>,
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
    pub fn new(inner: S, options: ObjectCacheOptions) -> Result<Self, ObjectCacheConfigError> {
        Ok(Self::with_options(inner, options.validate()?))
    }

    fn with_options(inner: S, options: ObjectCacheOptions) -> Self {
        Self {
            inner,
            shared: Arc::new(Shared {
                options,
                state: Mutex::new(CacheState::default()),
            }),
        }
    }

    pub(crate) fn with_default_options(inner: S) -> Self {
        Self::with_options(inner, ObjectCacheOptions::default())
    }

    /// Returns the wrapped correctness backend.
    #[must_use]
    pub const fn inner(&self) -> &S {
        &self.inner
    }

    /// Returns exact current and cumulative accelerator observations.
    pub fn stats(&self) -> Result<ObjectCacheStats, ObjectStoreError> {
        let state = self.lock_state()?;
        Ok(ObjectCacheStats {
            hits: state.hits,
            decoded_hits: state.decoded_hits,
            misses: state.misses,
            coalesced_reads: state.coalesced_reads,
            evictions: state.evictions,
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

    fn read_plan(
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
            state.hits = increment(state.hits)?;
            return Ok(ReadPlan::Hit(bytes));
        }
        state.misses = increment(state.misses)?;
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
            state.misses = increment(state.misses)?;
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
        state.hits = increment(state.hits)?;
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

    pub(crate) fn decoded_get(
        &self,
        key: DecodedCacheKey,
    ) -> Result<Option<DecodedCacheValue>, ObjectStoreError> {
        self.decoded_get_matching::<true>(key)
    }

    fn decoded_get_matching<const MATCH_IDENTITY: bool>(
        &self,
        key: DecodedCacheKey,
    ) -> Result<Option<DecodedCacheValue>, ObjectStoreError> {
        let mut state = self.lock_state()?;
        let tick = next_tick(&mut state)?;
        let selected = if MATCH_IDENTITY {
            Some(key)
        } else {
            state.decoded.keys().next().copied()
        };
        let Some((key, entry)) =
            selected.and_then(|key| state.decoded.get_mut(&key).map(|entry| (key, entry)))
        else {
            return Ok(None);
        };
        let prior = entry.touched;
        entry.touched = tick;
        let value = entry.value.clone();
        state.recency.remove(&(prior, RecencyKey::Decoded(key)));
        state.recency.insert((tick, RecencyKey::Decoded(key)));
        state.hits = increment(state.hits)?;
        state.decoded_hits = increment(state.decoded_hits)?;
        Ok(Some(value))
    }

    pub(crate) fn decoded_admit(
        &self,
        key: DecodedCacheKey,
        value: DecodedCacheValue,
    ) -> Result<DecodedCacheAdmission, ObjectStoreError> {
        self.decoded_admit_bounded::<true>(key, value)
    }

    fn decoded_admit_bounded<const ENFORCE: bool>(
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
        while ENFORCE
            && (state.entries.len().saturating_add(state.decoded.len())
                >= usize::try_from(self.shared.options.maximum_entries).unwrap_or(usize::MAX)
                || state
                    .resident_bytes
                    .checked_add(value.logical_bytes)
                    .is_none_or(|total| total > self.shared.options.maximum_bytes))
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
        state.coalesced_reads = increment(state.coalesced_reads)?;
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
    state.evictions = increment(state.evictions)?;
    Ok(())
}

fn cache_probe_work() -> WorkCounters {
    WorkCounters {
        object_probes: 1,
        items_examined: 1,
        ..WorkCounters::default()
    }
}

async fn wait_for_flight(
    flight: &Flight,
    cancellation: &CancellationToken,
    #[cfg(not(target_arch = "wasm32"))] work: Option<&NativeMaterializationWork>,
) -> Result<Option<OwnedBytes>, ObjectStoreError> {
    let mut cancelled = pin!(cancellation.cancelled());
    poll_fn(|context| {
        if cancelled.as_mut().poll(context).is_ready() {
            return Poll::Ready(Err(ObjectStoreError::Cancelled));
        }
        flight.poll(
            context,
            #[cfg(not(target_arch = "wasm32"))]
            work,
        )
    })
    .await
}

fn shared_read(bytes: OwnedBytes, maximum: u64) -> Result<ObjectRead, ObjectStoreError> {
    let observed = bytes.len() as u64;
    if observed > maximum {
        return Err(ObjectStoreError::TooLarge { observed, maximum });
    }
    Ok(ObjectRead {
        bytes,
        retention: ObjectReadRetention::Shared,
    })
}

mod reads;

mod provider;

#[cfg(all(test, feature = "memory"))]
#[path = "tests/cache/mod.rs"]
mod tests;

#[cfg(all(test, acyclic_fs_loom))]
#[path = "cache/decoded_model.rs"]
mod decoded_model;
#[cfg(all(test, not(acyclic_fs_loom)))]
#[path = "cache/decoded_properties.rs"]
mod decoded_properties;
