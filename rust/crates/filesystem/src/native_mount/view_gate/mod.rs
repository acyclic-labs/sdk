use super::MountSourceError;
use crate::lock_order::{GATE, Mutex as RankedMutex};
use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError};
use tokio::sync::Notify;

mod admissions;
pub(crate) use admissions::ViewGateReadAdmissions;
use admissions::{gate_allocation_error, insert_reader_slot, reconcile_reader_registry};
mod leases;
use leases::WaitingWriter;
pub(super) use leases::{ViewMutationPhase, ViewReadLease, ViewWriteLease};
#[cfg(test)]
mod tests;

/// Internal callback reentrancy identity, never an OS or authority capability.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct CallbackOwner(u64);

static NEXT_CALLBACK_OWNER: AtomicU64 = AtomicU64::new(1);
thread_local! {
    static CALLBACK_OWNER: Cell<u64> = const { Cell::new(0) };
}

#[derive(Default)]
struct ViewState {
    readers: usize,
    reader_slots: Vec<Option<ReadSlot>>,
    occupied_slots: usize,
    writer: bool,
    mutation_phase: bool,
    waiting_writers: usize,
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
enum ViewReaderId {
    Callback(CallbackOwner),
    #[cfg(test)]
    Independent(u64),
}

enum ReadSlot {
    Owner {
        owner: ViewReaderId,
        readers: usize,
    },
    Meter {
        readers: usize,
        finished: u64,
        proof_alive: bool,
        payload_active: u64,
        payload_finished: u64,
    },
}

type ReaderSlots = (Option<usize>, Option<usize>, Option<usize>);

impl ViewState {
    fn release_empty_registry(&mut self, gate: &ViewGate) {
        if self.occupied_slots == 0 && self.reader_slots.capacity() != 0 {
            self.reader_slots = Vec::new();
            let advanced = gate.reader_generation.fetch_update(
                Ordering::AcqRel,
                Ordering::Acquire,
                |generation| generation.checked_add(1),
            );
            assert!(advanced.is_ok(), "admitted read registry generation");
        }
    }
}

/// Pins a coherent mount view across a native callback or authored mutation.
pub(super) struct ViewGate {
    state: RankedMutex<ViewState, GATE>,
    changed: Notify,
    generation: AtomicU64,
    reader_generation: AtomicU64,
    #[cfg(test)]
    next_reader: AtomicU64,
    // The gate's readers and wake state drop before its original custody.
    _native_allocation: Option<crate::NativeAllocation>,
}

impl ViewGate {
    pub(super) const INITIAL_GENERATION: u64 = 2;

    pub(super) fn new() -> Self {
        Self {
            state: RankedMutex::new(ViewState::default()),
            changed: Notify::new(),
            generation: AtomicU64::new(Self::INITIAL_GENERATION),
            reader_generation: AtomicU64::new(1),
            #[cfg(test)]
            next_reader: AtomicU64::new(1),
            _native_allocation: None,
        }
    }

    pub(super) fn new_with_native_work(
        original: &crate::NativeMaterializationWork,
    ) -> Result<Arc<Self>, MountSourceError> {
        let allocation = original
            .claim(
                std::mem::size_of::<Self>() as u64,
                1,
                crate::WorkCounters::UNCHARGED,
            )
            .map_err(MountSourceError::NativeWork)?;
        let mut value = Self::new();
        value._native_allocation = Some(allocation);
        Ok(Arc::new(value))
    }

    #[cfg(test)]
    pub(super) async fn read(
        self: &Arc<Self>,
        expected: Option<u64>,
    ) -> Result<ViewReadLease, MountSourceError> {
        let owner = ViewReaderId::Independent(self.next_reader.fetch_add(1, Ordering::Relaxed));
        self.read_as(owner, expected).await
    }

    /// Whether `owner`'s callback already holds a read of the view, which a
    /// writer it waited for would never see released.
    pub(super) fn reads_for_callback(&self, owner: CallbackOwner) -> bool {
        self.state.lock().unwrap_or_else(PoisonError::into_inner).reader_slots.iter()
            .any(|slot| matches!(slot, Some(ReadSlot::Owner { owner: current, readers }) if *current == ViewReaderId::Callback(owner) && *readers != 0))
    }

    pub(super) fn callback_owner() -> Result<CallbackOwner, MountSourceError> {
        CALLBACK_OWNER.with(|owner| {
            let mut id = owner.get();
            if id == 0 {
                id = NEXT_CALLBACK_OWNER
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
                    .map_err(|_| MountSourceError::Work(crate::WorkError::Overflow))?;
                owner.set(id);
            }
            Ok(CallbackOwner(id))
        })
    }

    pub(super) async fn read_for_callback(
        self: &Arc<Self>,
        owner: CallbackOwner,
        expected: Option<u64>,
    ) -> Result<ViewReadLease, MountSourceError> {
        self.read_as(ViewReaderId::Callback(owner), expected).await
    }

    async fn read_as(
        self: &Arc<Self>,
        owner: ViewReaderId,
        expected: Option<u64>,
    ) -> Result<ViewReadLease, MountSourceError> {
        let mut admissions = ViewGateReadAdmissions::default();
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let mut work = crate::WorkCounters::default();
        let mut budget = crate::WorkBudget::UNBOUNDED;
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(lease) = self.try_read_as(
                owner,
                expected,
                &mut admissions,
                &mut allocations,
                &mut work,
                &mut budget,
            )? {
                return Ok(lease);
            }
            notified.await;
        }
    }

    pub(super) fn try_read_for_callback_measured(
        self: &Arc<Self>,
        owner: CallbackOwner,
        expected: Option<u64>,
        admissions: &mut ViewGateReadAdmissions,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut crate::WorkCounters,
        budget: &mut crate::WorkBudget,
    ) -> Result<Option<ViewReadLease>, MountSourceError> {
        self.try_read_as(
            ViewReaderId::Callback(owner),
            expected,
            admissions,
            allocations,
            work,
            budget,
        )
    }

    pub(super) async fn read_for_callback_with_carrier(
        self: &Arc<Self>,
        owner: CallbackOwner,
        expected: Option<u64>,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
        cancellation: &crate::CancellationToken,
        lifetime: Option<&crate::CancellationToken>,
    ) -> Result<ViewReadLease, MountSourceError> {
        loop {
            if cancellation.is_cancelled() {
                return Err(MountSourceError::Cancelled);
            }
            if lifetime.is_some_and(crate::CancellationToken::is_cancelled) {
                return Err(MountSourceError::Stale);
            }
            carrier
                .with_work(|work| work.check())
                .map_err(MountSourceError::from)?;
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let admitted = carrier.with_work(|work| {
                let (result, budget) = {
                    let (admissions, allocations, counters, mut budget) = work
                        .admissions(|roots| &mut roots.view_gates)
                        .map_err(MountSourceError::from)?;
                    let result = self.try_read_for_callback_measured(
                        owner,
                        expected,
                        admissions,
                        allocations,
                        counters,
                        &mut budget,
                    );
                    (result, budget)
                };
                work.budget = budget;
                result
            })?;
            if let Some(lease) = admitted {
                return Ok(lease);
            }
            super::adapter::await_lock(notified, carrier, cancellation, lifetime).await?;
        }
    }

    fn try_read_as(
        self: &Arc<Self>,
        owner: ViewReaderId,
        expected: Option<u64>,
        admissions: &mut ViewGateReadAdmissions,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut crate::WorkCounters,
        budget: &mut crate::WorkBudget,
    ) -> Result<Option<ViewReadLease>, MountSourceError> {
        let entry = admissions.entry_mut(self, allocations, work, *budget)?;
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.writer {
            return Ok(None);
        }
        reconcile_reader_registry(entry, self, allocations)?;
        let (owner_slot, first_free, second_free) = state.read_slots(owner, work, budget)?;
        if state.waiting_writers != 0 && owner_slot.is_none() && !state.mutation_phase {
            return Ok(None);
        }
        let generation = self.generation.load(Ordering::Acquire);
        if !generation.is_multiple_of(2) || expected.is_some_and(|expected| expected != generation)
        {
            return Err(MountSourceError::Stale);
        }
        work.admit(
            &crate::WorkCounters {
                items_examined: 1,
                ..crate::WorkCounters::default()
            },
            budget,
        )
        .map_err(MountSourceError::Work)?;
        let mut grant_budget = *budget;
        grant_budget.items_examined = grant_budget
            .items_examined
            .checked_sub(1)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let next_readers = state
            .readers
            .checked_add(1)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let owner_readers = match owner_slot
            .and_then(|index| state.reader_slots.get(index).and_then(Option::as_ref))
        {
            Some(ReadSlot::Owner { readers, .. }) => readers.checked_add(1),
            _ => Some(1),
        }
        .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let (meter_readers, meter_finished) = match entry.slot {
            Some(index) => match state.reader_slots.get(index).and_then(Option::as_ref) {
                Some(ReadSlot::Meter {
                    readers,
                    finished,
                    proof_alive: true,
                    ..
                }) => (
                    readers
                        .checked_add(1)
                        .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?,
                    *finished,
                ),
                _ => return Err(MountSourceError::Work(crate::WorkError::Overflow)),
            },
            None => (1, 0),
        };
        meter_finished
            .checked_add(meter_readers as u64)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let creates = usize::from(owner_slot.is_none()) + usize::from(entry.slot.is_none());
        state
            .occupied_slots
            .checked_add(creates)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let free = usize::from(first_free.is_some()) + usize::from(second_free.is_some());
        let required = state
            .reader_slots
            .len()
            .checked_add(creates.saturating_sub(free))
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        state.grow_registry(self, entry, required, allocations, work, &grant_budget)?;
        let (owner_slot, cleanup_slot) = state.register_reader(
            entry,
            owner,
            (owner_slot, first_free, second_free),
            (next_readers, owner_readers, meter_readers),
        )?;
        *budget = grant_budget;
        Ok(Some(ViewReadLease {
            generation,
            gate: Arc::clone(self),
            owner_slot,
            cleanup_slot: Some(cleanup_slot),
            payload_bytes: 0,
        }))
    }

    pub(super) async fn write(self: &Arc<Self>) -> ViewWriteLease {
        self.acquire_writer().await
    }

    pub(super) async fn write_stable(
        self: &Arc<Self>,
        expected: Option<u64>,
    ) -> Result<ViewWriteLease, MountSourceError> {
        let lease = self.acquire_writer().await;
        if !lease.generation.is_multiple_of(2)
            || expected.is_some_and(|expected| expected != lease.generation)
        {
            return Err(MountSourceError::Stale);
        }
        Ok(lease)
    }

    pub(super) async fn write_stable_with_carrier(
        self: &Arc<Self>,
        expected: Option<u64>,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
        cancellation: &crate::CancellationToken,
        lifetime: Option<&crate::CancellationToken>,
    ) -> Result<ViewWriteLease, MountSourceError> {
        carrier
            .with_work(|work| work.check())
            .map_err(MountSourceError::from)?;
        super::adapter::await_lock(self.write_stable(expected), carrier, cancellation, lifetime)
            .await?
    }

    async fn acquire_writer(self: &Arc<Self>) -> ViewWriteLease {
        let mut waiter = WaitingWriter::register(self);
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
                if !state.writer && !state.mutation_phase && state.readers == 0 {
                    let generation = self.generation.load(Ordering::Acquire);
                    waiter.admit(&mut state);
                    state.writer = true;
                    return ViewWriteLease {
                        generation,
                        gate: Arc::clone(self),
                        armed: true,
                    };
                }
            }
            notified.await;
        }
    }

    pub(super) fn begin_transition(&self) {
        let generation = self.generation.load(Ordering::Acquire);
        if generation.is_multiple_of(2) {
            self.generation.store(generation + 1, Ordering::Release);
        }
    }

    pub(super) fn finish_transition(&self) {
        let generation = self.generation.load(Ordering::Acquire);
        debug_assert_eq!(generation % 2, 1);
        self.generation.store(generation + 1, Ordering::Release);
    }

    pub(super) fn is_stable(&self) -> bool {
        self.generation.load(Ordering::Acquire).is_multiple_of(2)
    }

    pub(super) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }
}

impl ViewState {
    fn read_slots(
        &self,
        owner: ViewReaderId,
        work: &mut crate::WorkCounters,
        budget: &crate::WorkBudget,
    ) -> Result<ReaderSlots, MountSourceError> {
        let mut owner_slot = None;
        let mut first_free = None;
        let mut second_free = None;
        for (index, slot) in self.reader_slots.iter().enumerate() {
            work.charge(
                &crate::WorkCounters {
                    items_examined: 1,
                    ..crate::WorkCounters::default()
                },
                budget,
            )
            .map_err(MountSourceError::Work)?;
            match slot {
                Some(ReadSlot::Owner { owner: current, .. }) if *current == owner => {
                    owner_slot = Some(index);
                }
                None if first_free.is_none() => first_free = Some(index),
                None if second_free.is_none() => second_free = Some(index),
                _ => {}
            }
        }
        Ok((owner_slot, first_free, second_free))
    }

    fn grow_registry(
        &mut self,
        gate: &ViewGate,
        entry: &mut admissions::ReadAdmission,
        required: usize,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut crate::WorkCounters,
        grant_budget: &crate::WorkBudget,
    ) -> Result<(), MountSourceError> {
        let backing_generation = gate.reader_generation.load(Ordering::Acquire);
        // Leave addressability for the eventual physical backing release too.
        backing_generation
            .checked_add(2)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        if required > self.reader_slots.capacity() {
            let capacity = self
                .reader_slots
                .capacity()
                .max(1)
                .checked_mul(2)
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?
                .max(required);
            let width = std::mem::size_of::<Option<ReadSlot>>();
            let bytes = capacity
                .checked_mul(width)
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?
                as u64;
            let old_bytes = entry.registry.map_or(0, |(_, bytes)| bytes);
            let copied = crate::WorkCounters {
                items_examined: self.reader_slots.len() as u64,
                bytes_copied: (self.reader_slots.len() * width) as u64,
                ..crate::WorkCounters::default()
            };
            work.admit(&copied, grant_budget)
                .map_err(MountSourceError::Work)?;
            allocations
                .claim_bytes(bytes, 1, work, *grant_budget)
                .map_err(gate_allocation_error)?;
            let mut replacement = Vec::new();
            if replacement.try_reserve_exact(capacity).is_err() {
                allocations.release(bytes).map_err(gate_allocation_error)?;
                return Err(MountSourceError::Engine(std::borrow::Cow::Owned(
                    "view reader allocation failed".into(),
                )));
            }
            replacement.append(&mut self.reader_slots);
            self.reader_slots = replacement;
            let next_generation = backing_generation + 1;
            gate.reader_generation
                .store(next_generation, Ordering::Release);
            entry.registry = Some((next_generation, bytes));
            work.charge(&copied, grant_budget)
                .map_err(MountSourceError::Work)?;
            allocations
                .release(old_bytes)
                .map_err(gate_allocation_error)?;
        }
        Ok(())
    }

    fn register_reader(
        &mut self,
        entry: &mut admissions::ReadAdmission,
        owner: ViewReaderId,
        slots: ReaderSlots,
        readers: (usize, usize, usize),
    ) -> Result<(usize, usize), MountSourceError> {
        let (owner_slot, mut first_free, mut second_free) = slots;
        let (next_readers, owner_readers, meter_readers) = readers;
        let owner_slot = if let Some(index) = owner_slot {
            index
        } else {
            insert_reader_slot(
                self,
                &mut first_free,
                &mut second_free,
                ReadSlot::Owner { owner, readers: 0 },
            )?
        };
        let cleanup_slot = if let Some(index) = entry.slot {
            index
        } else {
            let index = insert_reader_slot(
                self,
                &mut first_free,
                &mut second_free,
                ReadSlot::Meter {
                    readers: 0,
                    finished: 0,
                    proof_alive: true,
                    payload_active: 0,
                    payload_finished: 0,
                },
            )?;
            entry.slot = Some(index);
            index
        };
        let Some(ReadSlot::Owner { readers, .. }) = self
            .reader_slots
            .get_mut(owner_slot)
            .and_then(Option::as_mut)
        else {
            return Err(MountSourceError::Work(crate::WorkError::Overflow));
        };
        *readers = owner_readers;
        let Some(ReadSlot::Meter { readers, .. }) = self
            .reader_slots
            .get_mut(cleanup_slot)
            .and_then(Option::as_mut)
        else {
            return Err(MountSourceError::Work(crate::WorkError::Overflow));
        };
        *readers = meter_readers;
        self.readers = next_readers;
        Ok((owner_slot, cleanup_slot))
    }
}
