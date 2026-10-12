use super::MountSourceError;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::cell::Cell;
use tokio::sync::Notify;

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
    Owner { owner: ViewReaderId, readers: usize },
    Meter {
        readers: usize, finished: u64, proof_alive: bool,
        payload_active: u64, payload_finished: u64,
    },
}

impl ViewState {
    fn release_empty_registry(&mut self, gate: &ViewGate) {
        if self.occupied_slots == 0 && self.reader_slots.capacity() != 0 {
            self.reader_slots = Vec::new();
            gate.reader_generation.fetch_update(Ordering::AcqRel, Ordering::Acquire, |generation| {
                generation.checked_add(1)
            }).expect("admitted read registry generation");
        }
    }
}

struct ReadAdmission {
    gate: Arc<ViewGate>,
    slot: Option<usize>,
    registry: Option<(u64, u64)>,
    finished: u64,
    payload_finished: u64,
}

impl ReadAdmission {
    fn finished_payload(&self) -> u64 {
        self.slot.map_or(0, |index| {
            let state = self.gate.state.lock().unwrap_or_else(PoisonError::into_inner);
            match &state.reader_slots[index] {
                Some(ReadSlot::Meter { payload_finished, .. }) => *payload_finished,
                _ => unreachable!("live original read admission"),
            }
        })
    }
}

impl Drop for ReadAdmission {
    fn drop(&mut self) {
        if let Some(index) = self.slot {
            let mut state = self.gate.state.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(ReadSlot::Meter { readers, proof_alive, .. }) = &mut state.reader_slots[index] {
                *proof_alive = false;
                if *readers == 0 {
                    state.reader_slots[index] = None;
                    state.occupied_slots = state.occupied_slots.checked_sub(1)
                        .expect("owned read admission slot");
                }
            }
            state.release_empty_registry(&self.gate);
        }
    }
}

/// Original invocation custody of controlled callback registry backings.
/// No ledger or authority is created; this moves with the demand meter.
#[derive(Default)]
pub(crate) struct ViewGateReadAdmissions {
    first: Option<ReadAdmission>,
    more: Vec<ReadAdmission>,
    reconstructed: bool,
}

impl ViewGateReadAdmissions {
    pub(crate) fn budget_reconstructed(&mut self) { self.reconstructed = true; }

    fn entries(&self) -> impl Iterator<Item = &ReadAdmission> {
        self.first.iter().chain(self.more.iter())
    }

    pub(crate) fn freed_bytes(&self) -> u64 {
        self.entries().fold(0_u64, |freed, entry| {
            let registry = entry.registry.filter(|(generation, _)|
                *generation != entry.gate.reader_generation.load(Ordering::Acquire))
                .map_or(0, |(_, bytes)| bytes);
            let payload = entry.finished_payload().checked_sub(entry.payload_finished)
                .expect("original binding payload progress");
            freed.checked_add(registry).and_then(|freed| freed.checked_add(payload))
                .expect("owned original read allocations")
        })
    }

    pub(crate) fn reconcile(
        &mut self, allocations: &mut crate::kernel::allocation::AllocationLedger,
    ) -> Result<(), crate::WorkError> {
        for entry in self.first.iter_mut().chain(self.more.iter_mut()) {
            if let Some((generation, bytes)) = entry.registry
                && generation != entry.gate.reader_generation.load(Ordering::Acquire)
            {
                allocations.release(bytes).map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
                entry.registry = None;
            }
            let finished = entry.finished_payload();
            let bytes = finished.checked_sub(entry.payload_finished)
                .ok_or(crate::WorkError::Overflow)?;
            allocations.release(bytes).map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
            entry.payload_finished = finished;
        }
        Ok(())
    }

    pub(crate) fn reconcile_work(
        &mut self, work: &mut crate::WorkCounters, budget: &mut crate::WorkBudget,
    ) -> Result<(), crate::WorkError> {
        let mut completed = 0_u64;
        let mut active = 0_u64;
        for entry in self.first.iter_mut().chain(self.more.iter_mut()) {
            let Some(index) = entry.slot else { continue };
            let (readers, finished) = {
                let state = entry.gate.state.lock().unwrap_or_else(PoisonError::into_inner);
                match state.reader_slots.get(index).and_then(Option::as_ref) {
                    Some(ReadSlot::Meter { readers, finished, .. }) => (*readers, *finished),
                    _ => return Err(crate::WorkError::Overflow),
                }
            };
            let newly_finished = finished.checked_sub(entry.finished).ok_or(crate::WorkError::Overflow)?;
            // These physical releases already happened. Preserve their exact
            // prefix before applying any reconstructed or tighter ceiling.
            *work = work.checked_add(crate::WorkCounters {
                items_examined: newly_finished, ..crate::WorkCounters::default()
            })?;
            entry.finished = finished;
            completed = completed.checked_add(newly_finished).ok_or(crate::WorkError::Overflow)?;
            active = active.checked_add(readers as u64).ok_or(crate::WorkError::Overflow)?;
        }
        if self.reconstructed {
            work.admit(&crate::WorkCounters { items_examined: active, ..crate::WorkCounters::default() }, budget)?;
            budget.items_examined = budget.items_examined.checked_sub(active).ok_or(crate::WorkError::Overflow)?;
        } else {
            budget.items_examined = budget.items_examined.checked_add(completed).ok_or(crate::WorkError::Overflow)?;
        }
        self.reconstructed = false;
        work.verify(*budget)?;
        Ok(())
    }

    fn entry_mut(
        &mut self, gate: &Arc<ViewGate>,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut crate::WorkCounters, budget: crate::WorkBudget,
    ) -> Result<&mut ReadAdmission, MountSourceError> {
        let mut found = None;
        for (index, entry) in self.entries().enumerate() {
            work.charge(&crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() }, &budget)
                .map_err(MountSourceError::Work)?;
            if Arc::ptr_eq(&entry.gate, gate) { found = Some(index); break; }
        }
        if let Some(index) = found {
            return Ok(if index == 0 { self.first.as_mut().expect("first gate admission") } else { &mut self.more[index - 1] });
        }
        if self.first.is_none() {
            self.first = Some(ReadAdmission { gate: Arc::clone(gate), slot: None, registry: None, finished: 0, payload_finished: 0 });
            return Ok(self.first.as_mut().expect("new first admission"));
        }
        if self.more.len() == self.more.capacity() {
            let count = self.more.capacity().max(1).checked_mul(2)
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            let copied = crate::WorkCounters {
                items_examined: self.more.len() as u64,
                bytes_copied: (self.more.len() * std::mem::size_of::<ReadAdmission>()) as u64,
                ..crate::WorkCounters::default()
            };
            work.admit(&copied, &budget).map_err(MountSourceError::Work)?;
            let bytes = count.checked_mul(std::mem::size_of::<ReadAdmission>())
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
            allocations.claim_bytes(bytes, 1, work, budget).map_err(gate_allocation_error)?;
            let mut replacement = Vec::new();
            if replacement.try_reserve_exact(count).is_err() {
                allocations.release(bytes).map_err(gate_allocation_error)?;
                return Err(MountSourceError::Engine(std::borrow::Cow::Owned("view admission allocation failed".into())));
            }
            let old_bytes = (self.more.capacity() * std::mem::size_of::<ReadAdmission>()) as u64;
            replacement.append(&mut self.more);
            self.more = replacement;
            work.charge(&copied, &budget).map_err(MountSourceError::Work)?;
            allocations.release(old_bytes).map_err(gate_allocation_error)?;
        }
        self.more.push(ReadAdmission { gate: Arc::clone(gate), slot: None, registry: None, finished: 0, payload_finished: 0 });
        Ok(self.more.last_mut().expect("new gate admission"))
    }
}

fn gate_allocation_error(error: crate::kernel::allocation::AllocationError) -> MountSourceError {
    super::lazy::mount_engine_error(crate::collection::handle_allocation_error(error))
}

fn reconcile_reader_registry(
    entry: &mut ReadAdmission,
    gate: &ViewGate,
    allocations: &mut crate::kernel::allocation::AllocationLedger,
) -> Result<(), MountSourceError> {
    let generation = gate.reader_generation.load(Ordering::Acquire);
    if let Some((owned_generation, bytes)) = entry.registry
        && owned_generation != generation
    {
        allocations.release(bytes).map_err(gate_allocation_error)?;
        entry.registry = None;
    }
    if entry.registry.is_none() {
        entry.registry = Some((generation, 0));
    }
    Ok(())
}

fn insert_reader_slot(
    state: &mut ViewState,
    first_free: &mut Option<usize>,
    second_free: &mut Option<usize>,
    slot: ReadSlot,
) -> usize {
    let index = if let Some(index) = first_free.take() {
        *first_free = second_free.take();
        state.reader_slots[index] = Some(slot);
        index
    } else {
        let index = state.reader_slots.len();
        state.reader_slots.push(Some(slot));
        index
    };
    state.occupied_slots += 1;
    index
}

/// Pins a coherent mount view across a native callback or authored mutation.
pub(super) struct ViewGate {
    state: Mutex<ViewState>,
    changed: Notify,
    generation: AtomicU64,
    reader_generation: AtomicU64,
    #[cfg(test)]
    next_reader: AtomicU64,
    // The gate's readers and wake state drop before its original custody.
    _native_allocation: Option<crate::NativeAllocation>,
}

pub(super) struct ViewReadLease {
    pub(super) generation: u64,
    gate: Arc<ViewGate>,
    owner_slot: usize,
    cleanup_slot: Option<usize>,
    payload_bytes: u64,
}

pub(super) struct ViewWriteLease {
    pub(super) generation: u64,
    gate: Arc<ViewGate>,
    armed: bool,
}

pub(super) struct ViewMutationPhase {
    pub(super) generation: u64,
    gate: Arc<ViewGate>,
}

impl ViewWriteLease {
    pub(super) fn into_mutation_phase(mut self) -> ViewMutationPhase {
        let mut state = self.gate.state.lock().unwrap_or_else(PoisonError::into_inner);
        assert!(state.writer && !state.mutation_phase, "original exclusive view owner");
        state.mutation_phase = true;
        state.writer = false;
        self.armed = false;
        let phase = ViewMutationPhase { generation: self.generation, gate: Arc::clone(&self.gate) };
        drop(state);
        self.gate.changed.notify_waiters();
        phase
    }
}

impl Drop for ViewMutationPhase {
    fn drop(&mut self) {
        let mut state = self.gate.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.mutation_phase = false;
        drop(state);
        self.gate.changed.notify_waiters();
    }
}

struct WaitingWriter {
    gate: Arc<ViewGate>,
    registered: bool,
}

impl ViewReadLease {
    /// Transfers an already admitted visible owner payload to this original lease.
    pub(super) fn attach_owned_payload(&mut self, bytes: u64) -> Result<(), MountSourceError> {
        let mut state = self.gate.state.lock().unwrap_or_else(PoisonError::into_inner);
        let index = self.cleanup_slot.ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let Some(ReadSlot::Meter { payload_active, payload_finished, proof_alive: true, .. }) =
            &mut state.reader_slots[index]
        else { return Err(MountSourceError::Work(crate::WorkError::Overflow)); };
        let active = payload_active.checked_add(bytes)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        payload_finished.checked_add(active)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let owned = self.payload_bytes.checked_add(bytes)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        *payload_active = active;
        self.payload_bytes = owned;
        Ok(())
    }
}

impl Drop for ViewReadLease {
    fn drop(&mut self) {
        let mut state = self.gate.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.readers = state.readers.checked_sub(1).expect("owned view read");
        if let Some(ReadSlot::Owner { readers, .. }) = &mut state.reader_slots[self.owner_slot] {
            *readers = readers.checked_sub(1).expect("owned callback read");
            if *readers == 0 {
                state.reader_slots[self.owner_slot] = None;
                state.occupied_slots = state.occupied_slots.checked_sub(1).expect("owned callback slot");
            }
        }
        if let Some(index) = self.cleanup_slot
            && let Some(ReadSlot::Meter {
                readers, finished, proof_alive, payload_active, payload_finished,
            }) = &mut state.reader_slots[index]
        {
            *readers = readers.checked_sub(1).expect("owned view read cleanup");
            *finished = finished.checked_add(1).expect("admitted view read cleanup addressability");
            *payload_active = payload_active.checked_sub(self.payload_bytes)
                .expect("owned original binding payload");
            *payload_finished = payload_finished.checked_add(self.payload_bytes)
                .expect("admitted binding payload cleanup addressability");
            if *readers == 0 && !*proof_alive {
                state.reader_slots[index] = None;
                state.occupied_slots = state.occupied_slots.checked_sub(1).expect("owned cleanup slot");
            }
        }
        state.release_empty_registry(&self.gate);
        let wake = state.readers == 0;
        drop(state);
        if wake { self.gate.changed.notify_waiters(); }
    }
}

impl WaitingWriter {
    fn register(gate: &Arc<ViewGate>) -> Self {
        let mut state = gate.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.waiting_writers = state.waiting_writers.saturating_add(1);
        drop(state);
        Self {
            gate: Arc::clone(gate),
            registered: true,
        }
    }

    fn admit(&mut self, state: &mut ViewState) {
        state.waiting_writers = state.waiting_writers.saturating_sub(1);
        self.registered = false;
    }
}

impl Drop for WaitingWriter {
    fn drop(&mut self) {
        if self.registered {
            let mut state = self
                .gate
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            state.waiting_writers = state.waiting_writers.saturating_sub(1);
            drop(state);
            self.gate.changed.notify_waiters();
        }
    }
}

impl Drop for ViewWriteLease {
    fn drop(&mut self) {
        if !self.armed { return; }
        let mut state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.writer = false;
        drop(state);
        self.gate.changed.notify_waiters();
    }
}

impl ViewGate {
    pub(super) const INITIAL_GENERATION: u64 = 2;

    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(ViewState::default()),
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
        let allocation = original.claim(std::mem::size_of::<Self>() as u64, 1, crate::WorkCounters::UNCHARGED)
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

    pub(super) fn callback_owner() -> CallbackOwner {
        CALLBACK_OWNER.with(|owner| {
            let mut id = owner.get();
            if id == 0 {
                id = NEXT_CALLBACK_OWNER.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
                    .expect("callback identity addressability exhausted");
                owner.set(id);
            }
            CallbackOwner(id)
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
                owner, expected, &mut admissions, &mut allocations, &mut work, &mut budget,
            )? {
                return Ok(lease);
            }
            notified.await;
        }
    }


    pub(super) fn reads_for_callback_measured(
        self: &Arc<Self>,
        owner: CallbackOwner,
        admissions: &mut ViewGateReadAdmissions,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut crate::WorkCounters,
        budget: crate::WorkBudget,
    ) -> Result<bool, MountSourceError> {
        let entry = admissions.entry_mut(self, allocations, work, budget)?;
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        reconcile_reader_registry(entry, self, allocations)?;
        for slot in &state.reader_slots {
            work.charge(&crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() }, &budget)
                .map_err(MountSourceError::Work)?;
            if matches!(slot, Some(ReadSlot::Owner { owner: current, readers })
                if *current == ViewReaderId::Callback(owner) && *readers != 0)
            {
                return Ok(true);
            }
        }
        Ok(false)
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
        self.try_read_as(ViewReaderId::Callback(owner), expected, admissions, allocations, work, budget)
    }

    pub(super) async fn read_for_callback_with_work(
        self: &Arc<Self>, owner: CallbackOwner, expected: Option<u64>,
        work: &mut crate::demand::DemandWork,
        cancellation: &crate::CancellationToken, lifetime: Option<&crate::CancellationToken>,
    ) -> Result<ViewReadLease, MountSourceError> {
        self.read_for_callback_with_carrier(
            owner, expected, &mut super::adapter::NativeDemandCarrier::Borrowed(work),
            cancellation, lifetime,
        ).await
    }

    pub(super) async fn read_for_callback_with_meter(
        self: &Arc<Self>, owner: CallbackOwner, expected: Option<u64>,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
        cancellation: &crate::CancellationToken, lifetime: Option<&crate::CancellationToken>,
    ) -> Result<ViewReadLease, MountSourceError> {
        self.read_for_callback_with_carrier(
            owner, expected, &mut super::adapter::NativeDemandCarrier::Shared(meter),
            cancellation, lifetime,
        ).await
    }

    pub(super) async fn read_for_callback_with_carrier(
        self: &Arc<Self>, owner: CallbackOwner, expected: Option<u64>,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
        cancellation: &crate::CancellationToken, lifetime: Option<&crate::CancellationToken>,
    ) -> Result<ViewReadLease, MountSourceError> {
        loop {
            if cancellation.is_cancelled() { return Err(MountSourceError::Cancelled); }
            if lifetime.is_some_and(crate::CancellationToken::is_cancelled) {
                return Err(MountSourceError::Stale);
            }
            carrier.with_work(|work| work.check()).map_err(super::lazy::mount_demand_error)?;
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let admitted = carrier.with_work(|work| {
                let (result, budget) = {
                    let (admissions, allocations, counters, mut budget) =
                        work.admissions(|roots| &mut roots.view_gates).map_err(super::lazy::mount_demand_error)?;
                    let result = self.try_read_for_callback_measured(
                        owner, expected, admissions, allocations, counters, &mut budget,
                    );
                    (result, budget)
                };
                work.budget = budget;
                result
            })?;
            if let Some(lease) = admitted { return Ok(lease); }
            super::adapter::await_lock(
                notified, carrier, cancellation, lifetime,
            ).await?;
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
        if state.writer { return Ok(None); }
        reconcile_reader_registry(entry, self, allocations)?;
        let mut owner_slot = None;
        let mut first_free = None;
        let mut second_free = None;
        for (index, slot) in state.reader_slots.iter().enumerate() {
            work.charge(&crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() }, budget)
                .map_err(MountSourceError::Work)?;
            match slot {
                Some(ReadSlot::Owner { owner: current, .. }) if *current == owner => owner_slot = Some(index),
                None if first_free.is_none() => first_free = Some(index),
                None if second_free.is_none() => second_free = Some(index),
                _ => {}
            }
        }
        if state.waiting_writers != 0 && owner_slot.is_none() && !state.mutation_phase { return Ok(None); }
        let generation = self.generation.load(Ordering::Acquire);
        if !generation.is_multiple_of(2) || expected.is_some_and(|expected| expected != generation) {
            return Err(MountSourceError::Stale);
        }
        work.admit(&crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() }, budget)
            .map_err(MountSourceError::Work)?;
        let mut grant_budget = *budget;
        grant_budget.items_examined = grant_budget.items_examined.checked_sub(1)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let next_readers = state.readers.checked_add(1)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let owner_readers = match owner_slot.and_then(|index| state.reader_slots[index].as_ref()) {
            Some(ReadSlot::Owner { readers, .. }) => readers.checked_add(1),
            _ => Some(1),
        }.ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let (meter_readers, meter_finished) = match entry.slot {
            Some(index) => match state.reader_slots.get(index).and_then(Option::as_ref) {
                Some(ReadSlot::Meter { readers, finished, proof_alive: true, .. }) =>
                    (readers.checked_add(1).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?, *finished),
                _ => return Err(MountSourceError::Work(crate::WorkError::Overflow)),
            },
            None => (1, 0),
        };
        meter_finished.checked_add(meter_readers as u64)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let creates = usize::from(owner_slot.is_none()) + usize::from(entry.slot.is_none());
        state.occupied_slots.checked_add(creates)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let free = usize::from(first_free.is_some()) + usize::from(second_free.is_some());
        let required = state.reader_slots.len().checked_add(creates.saturating_sub(free))
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let backing_generation = self.reader_generation.load(Ordering::Acquire);
        // Leave addressability for the eventual physical backing release too.
        backing_generation.checked_add(2).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        if required > state.reader_slots.capacity() {
            let capacity = state.reader_slots.capacity().max(1).checked_mul(2)
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?.max(required);
            let width = std::mem::size_of::<Option<ReadSlot>>();
            let bytes = capacity.checked_mul(width)
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
            let old_bytes = entry.registry.map_or(0, |(_, bytes)| bytes);
            let copied = crate::WorkCounters {
                items_examined: state.reader_slots.len() as u64,
                bytes_copied: (state.reader_slots.len() * width) as u64,
                ..crate::WorkCounters::default()
            };
            work.admit(&copied, &grant_budget).map_err(MountSourceError::Work)?;
            allocations.claim_bytes(bytes, 1, work, grant_budget).map_err(gate_allocation_error)?;
            let mut replacement = Vec::new();
            if replacement.try_reserve_exact(capacity).is_err() {
                allocations.release(bytes).map_err(gate_allocation_error)?;
                return Err(MountSourceError::Engine(std::borrow::Cow::Owned("view reader allocation failed".into())));
            }
            replacement.append(&mut state.reader_slots);
            state.reader_slots = replacement;
            let next_generation = backing_generation + 1;
            self.reader_generation.store(next_generation, Ordering::Release);
            entry.registry = Some((next_generation, bytes));
            work.charge(&copied, &grant_budget).map_err(MountSourceError::Work)?;
            allocations.release(old_bytes).map_err(gate_allocation_error)?;
        }
        let owner_slot = owner_slot.unwrap_or_else(|| insert_reader_slot(
            &mut state, &mut first_free, &mut second_free, ReadSlot::Owner { owner, readers: 0 },
        ));
        let cleanup_slot = match entry.slot {
            Some(index) => index,
            None => {
                let index = insert_reader_slot(&mut state, &mut first_free, &mut second_free,
                    ReadSlot::Meter { readers: 0, finished: 0, proof_alive: true, payload_active: 0, payload_finished: 0 });
                entry.slot = Some(index);
                index
            }
        };
        if let Some(ReadSlot::Owner { readers, .. }) = &mut state.reader_slots[owner_slot] {
            *readers = owner_readers;
        }
        if let Some(ReadSlot::Meter { readers, .. }) = &mut state.reader_slots[cleanup_slot] {
            *readers = meter_readers;
        }
        state.readers = next_readers;
        *budget = grant_budget;
        Ok(Some(ViewReadLease {
            generation, gate: Arc::clone(self), owner_slot, cleanup_slot: Some(cleanup_slot), payload_bytes: 0,
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
        self: &Arc<Self>, expected: Option<u64>,
        carrier: &mut super::adapter::NativeDemandCarrier<'_>,
        cancellation: &crate::CancellationToken, lifetime: Option<&crate::CancellationToken>,
    ) -> Result<ViewWriteLease, MountSourceError> {
        carrier.with_work(|work| work.check()).map_err(super::lazy::mount_demand_error)?;
        super::adapter::await_lock(
            self.write_stable(expected), carrier, cancellation, lifetime,
        ).await?
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::demand::DemandWork;
    use crate::{CancellationToken, WorkBudget, WorkCounters};

    fn measured_read(
        gate: &Arc<ViewGate>,
        owner: CallbackOwner,
        work: &mut DemandWork,
    ) -> Result<ViewReadLease, MountSourceError> {
        work.check().map_err(super::super::lazy::mount_demand_error)?;
        let (result, budget) = {
            let (proofs, allocations, counters, mut budget) = work.admissions(|roots| &mut roots.view_gates)
                .map_err(super::super::lazy::mount_demand_error)?;
            let result = gate.try_read_for_callback_measured(
                owner, None, proofs, allocations, counters, &mut budget,
            );
            (result, budget)
        };
        work.budget = budget;
        result?.ok_or(MountSourceError::Stale)
    }

    #[test]
    fn reconstructed_full_budget_records_only_actual_cleanup() {
        let gate = Arc::new(ViewGate::new());
        let cancellation = CancellationToken::new();
        let original = WorkBudget::UNBOUNDED;
        let mut work = DemandWork::new(original, &cancellation);
        let lease = measured_read(&gate, CallbackOwner(1), &mut work).expect("actual read");
        assert_eq!(work.budget.items_examined, original.items_examined - 1);
        let before = work.work.items_examined;
        let (counters, allocations, roots) = work.into_current();
        let work = DemandWork::from_current(original, &cancellation, counters, allocations, roots);
        drop(lease);
        let receipt = work.finish(Ok(())).expect("actual cleanup under reconstructed full budget");
        assert_eq!(receipt.work.items_examined, before + 1);
        receipt.work.verify(original).expect("no artificial quota overflow");
        assert_eq!(gate.state.lock().expect("gate state").readers, 0);
    }

    #[test]
    fn native_queue_custody_preserves_non_lifo_cleanup_quota() {
        let gate = Arc::new(ViewGate::new());
        let cancellation = CancellationToken::new();
        let mut budget = WorkBudget::UNBOUNDED;
        budget.items_examined = 100;
        let mut work = DemandWork::new(budget, &cancellation);
        let first = measured_read(&gate, CallbackOwner(1), &mut work).expect("first actual read");
        let second = measured_read(&gate, CallbackOwner(2), &mut work).expect("second actual read");
        let before = work.work.items_examined;
        let mut queued = work.take_for_native_queue();
        assert_eq!(queued.budget.items_examined, budget.items_examined - 2);
        drop(first);
        let _ = queued.admissions(|roots| &mut roots.view_gates).expect("first actual cleanup");
        assert_eq!(queued.work.items_examined, before + 1);
        assert_eq!(queued.budget.items_examined, budget.items_examined - 1);
        drop(second);
        let _ = queued.admissions(|roots| &mut roots.view_gates).expect("second actual cleanup");
        assert_eq!(queued.work.items_examined, before + 2);
        assert_eq!(queued.budget.items_examined, budget.items_examined);
        queued.finish(Ok(())).expect("same original meter finishes");
        assert_eq!(gate.state.lock().expect("gate state").readers, 0);
    }

    #[test]
    fn denied_registry_allocation_and_copy_do_not_grant_reads() {
        let gate = Arc::new(ViewGate::new());
        let cancellation = CancellationToken::new();
        let mut budget = WorkBudget::UNBOUNDED;
        budget.allocation_operations = 0;
        let mut work = DemandWork::new(budget, &cancellation);
        assert!(matches!(
            measured_read(&gate, CallbackOwner(1), &mut work),
            Err(MountSourceError::Work(_)),
        ));
        assert_eq!(work.work.allocation_operations, 0);
        assert_eq!(gate.state.lock().expect("gate state").reader_slots.capacity(), 0);
        assert_eq!(gate.state.lock().expect("gate state").readers, 0);

        let mut budget = WorkBudget::UNBOUNDED;
        budget.bytes_copied = 0;
        let mut work = DemandWork::new(budget, &cancellation);
        let first = measured_read(&gate, CallbackOwner(1), &mut work).expect("initial registry");
        let allocations_before = work.work.allocation_operations;
        let generation = gate.reader_generation.load(Ordering::Acquire);
        assert!(matches!(
            measured_read(&gate, CallbackOwner(2), &mut work),
            Err(MountSourceError::Work(_)),
        ));
        assert_eq!(work.work.allocation_operations, allocations_before);
        assert_eq!(work.work.bytes_copied, 0);
        assert_eq!(gate.reader_generation.load(Ordering::Acquire), generation);
        assert_eq!(gate.state.lock().expect("gate state").readers, 1);
        drop(first);
        work.finish(Ok(())).expect("only admitted read releases");
    }

    #[tokio::test]
    async fn external_registry_growth_reconciles_original_backing_proof() {
        let gate = Arc::new(ViewGate::new());
        let cancellation = CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let first = measured_read(&gate, CallbackOwner(1), &mut work).expect("measured original read");
        assert!(work.live_allocation_bytes() > 0);
        let generation = gate.reader_generation.load(Ordering::Acquire);
        let independent = gate.read(None).await.expect("actual independent reader grows registry");
        assert!(gate.reader_generation.load(Ordering::Acquire) > generation);
        let _ = work.admissions(|roots| &mut roots.view_gates).expect("same root reconciles freed original backing");
        assert_eq!(work.live_allocation_bytes(), 0);
        drop(independent);
        let before = work.work.items_examined;
        drop(first);
        let receipt = work.finish(Ok(())).expect("original physical read cleanup");
        assert_eq!(receipt.work.items_examined, before + 1);
    }

    #[tokio::test]
    async fn measured_reentrant_reads_do_not_wait_for_queued_writer() {
        let gate = Arc::new(ViewGate::new());
        let cancellation = CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let owner = ViewGate::callback_owner();
        let outer = measured_read(&gate, owner, &mut work).expect("outer callback");
        let mut writer = std::pin::pin!(gate.write());
        assert!(futures::poll!(writer.as_mut()).is_pending());
        let nested = measured_read(&gate, owner, &mut work).expect("same original callback reenters");
        assert_eq!(gate.state.lock().expect("gate state").readers, 2);
        drop(outer);
        drop(nested);
        drop(writer.await);
        let before = work.work.items_examined;
        let receipt = work.finish(Ok(())).expect("two actual callback releases");
        assert_eq!(receipt.work.items_examined, before + 2);
    }

    #[test]
    fn tighter_reconstructed_budget_keeps_completed_cleanup_prefix() {
        let gate = Arc::new(ViewGate::new());
        let cancellation = CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let lease = measured_read(&gate, CallbackOwner(1), &mut work).expect("actual read");
        let before = work.work.items_examined;
        let (counters, allocations, roots) = work.into_current();
        let mut tighter = WorkBudget::UNBOUNDED;
        tighter.items_examined = before;
        let work = DemandWork::from_current(tighter, &cancellation, counters, allocations, roots);
        drop(lease);
        let failure = work.finish(Ok(())).expect_err("tighter ceiling cannot erase actual cleanup");
        assert_eq!(failure.work.items_examined, before + 1);
        assert!(matches!(failure.error, crate::demand::DemandError::Work(_)));
    }

    #[test]
    fn boxed_original_read_payload_survives_custody_until_physical_drop() {
        let gate = Arc::new(ViewGate::new());
        let cancellation = CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let mut lease = measured_read(&gate, CallbackOwner(1), &mut work).expect("actual read");
        let bytes = std::mem::size_of::<ViewReadLease>() as u64;
        work.claim_allocation(bytes, u64::from((bytes) != 0), crate::WorkCounters::UNCHARGED).expect("actual Box payload admission");
        lease.attach_owned_payload(bytes).expect("same original cleanup custody");
        let lease = Box::new(lease);
        let live = work.live_allocation_bytes();
        let before = work.work.items_examined;
        let (counters, allocations, roots) = work.into_current();
        let mut work = DemandWork::from_current(
            WorkBudget::UNBOUNDED, &cancellation, counters, allocations, roots,
        );
        work.check().expect("full budget reconstructs active original lease");
        assert_eq!(work.live_allocation_bytes(), live);
        assert_eq!(work.budget.items_examined, u64::MAX - 1);
        drop(lease);
        assert_eq!(work.live_allocation_bytes(), live - bytes);
        work.record(WorkCounters::default()).expect("actual Box and lease cleanup");
        assert_eq!(work.work.items_examined, before + 1);
        assert_eq!(work.budget.items_examined, u64::MAX);
        assert_eq!(work.live_allocation_bytes(), live - bytes);
    }

    #[test]
    fn independent_box_payloads_release_non_lifo_without_losing_quota() {
        let gate = Arc::new(ViewGate::new());
        let cancellation = CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let bytes = std::mem::size_of::<ViewReadLease>() as u64;
        let mut first = measured_read(&gate, CallbackOwner(1), &mut work).expect("first read");
        work.claim_allocation(bytes, u64::from((bytes) != 0), crate::WorkCounters::UNCHARGED).expect("first original Box");
        first.attach_owned_payload(bytes).expect("first payload custody");
        let first = Box::new(first);
        let mut second = measured_read(&gate, CallbackOwner(1), &mut work).expect("nested read");
        work.claim_allocation(bytes, u64::from((bytes) != 0), crate::WorkCounters::UNCHARGED).expect("second original Box");
        second.attach_owned_payload(bytes).expect("second payload custody");
        let second = Box::new(second);
        let live = work.live_allocation_bytes();
        let before = work.work.items_examined;
        drop(first);
        work.record(WorkCounters::default()).expect("first physical cleanup");
        assert_eq!(work.work.items_examined, before + 1);
        assert_eq!(work.live_allocation_bytes(), live - bytes);
        assert_eq!(work.budget.items_examined, u64::MAX - 1);
        drop(second);
        work.record(WorkCounters::default()).expect("second physical cleanup");
        assert_eq!(work.work.items_examined, before + 2);
        assert_eq!(work.live_allocation_bytes(), live - bytes * 2);
        assert_eq!(work.budget.items_examined, u64::MAX);
    }

    #[tokio::test]
    async fn measured_callback_wait_preserves_original_lifetime_cancellation() {
        let gate = Arc::new(ViewGate::new());
        let writer = gate.write().await;
        let cancellation = CancellationToken::new();
        let lifetime = CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        {
            let mut read = std::pin::pin!(gate.read_for_callback_with_work(
                CallbackOwner(1), None, &mut work, &cancellation, Some(&lifetime),
            ));
            assert!(futures::poll!(read.as_mut()).is_pending());
            lifetime.cancel();
            assert!(matches!(read.await, Err(MountSourceError::Stale)));
        }
        assert!(!cancellation.is_cancelled());
        drop(writer);
        work.record(WorkCounters::default()).expect("original wait cleanup");
        assert_eq!(gate.state.lock().expect("gate state").readers, 0);
    }

    #[tokio::test]
    async fn original_mutation_phase_allows_new_sdk_readers_but_not_later_writers() {
        let gate = Arc::new(ViewGate::new());
        let cancellation = CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let writer = gate.write_stable(None).await.expect("original writer");
        let phase = writer.into_mutation_phase();
        assert_eq!(phase.generation, ViewGate::INITIAL_GENERATION);
        assert!(!gate.state.lock().expect("original state").writer);
        let mut next = std::pin::pin!(gate.write_stable(None));
        assert!(futures::poll!(next.as_mut()).is_pending());
        let reader = gate.read_for_callback_with_work(
            CallbackOwner(10), Some(phase.generation), &mut work, &cancellation, None,
        ).await.expect("new SDK callback reads original frozen view");
        assert!(futures::poll!(next.as_mut()).is_pending());
        drop(phase);
        assert!(futures::poll!(next.as_mut()).is_pending(), "original reader still owns its lane");
        drop(reader);
        work.record(WorkCounters::UNCHARGED).expect("actual callback cleanup");
        drop(next.await.expect("later original writer"));
        assert_eq!(work.budget.items_examined, u64::MAX);
    }

    #[tokio::test]
    async fn foreign_existing_registry_capacity_remains_borrowed() {
        let gate = Arc::new(ViewGate::new());
        let first = gate.read_for_callback(CallbackOwner(1), None).await.expect("existing first reader");
        let second = gate.read_for_callback(CallbackOwner(2), None).await.expect("existing second reader");
        let spare = gate.read_for_callback(CallbackOwner(3), None).await.expect("existing spare reader");
        drop(spare);
        let cancellation = CancellationToken::new();
        let mut budget = WorkBudget::UNBOUNDED;
        budget.allocation_operations = 0;
        budget.peak_allocation_bytes = 0;
        let mut work = DemandWork::new(budget, &cancellation);
        let reader = measured_read(&gate, CallbackOwner(1), &mut work).expect("reuse borrowed original slots");
        assert_eq!(work.work.allocation_operations, 0);
        assert_eq!(work.live_allocation_bytes(), 0);
        drop(reader);
        work.record(WorkCounters::UNCHARGED).expect("only actual lease cleanup");
        drop(second);
        drop(first);
    }

    #[tokio::test]
    async fn shared_original_read_and_write_gate_preserve_custody_and_tokens() {
        let gate = Arc::new(ViewGate::new());
        let cancellation = CancellationToken::new();
        let meter = Arc::new(crate::demand::NativeDemandReadMeter(std::sync::Mutex::new(
            DemandWork::new(WorkBudget::UNBOUNDED, &cancellation),
        )));
        let lease = gate.read_for_callback_with_meter(
            CallbackOwner(1), None, &meter, &cancellation, None,
        ).await.expect("original shared read");
        let before = meter.lock().work.items_examined;
        let lifetime = CancellationToken::new();
        {
            let mut carrier = super::super::adapter::NativeDemandCarrier::Shared(&meter);
            let mut writer = std::pin::pin!(gate.write_stable_with_carrier(
                Some(ViewGate::INITIAL_GENERATION), &mut carrier, &cancellation, Some(&lifetime),
            ));
            assert!(futures::poll!(writer.as_mut()).is_pending());
            assert!(meter.0.try_lock().is_ok(), "pending writer retained the original work mutex");
            lifetime.cancel();
            assert!(matches!(writer.await, Err(MountSourceError::Stale)));
        }
        assert!(!cancellation.is_cancelled());
        assert_eq!(gate.state.lock().expect("original gate").waiting_writers, 0);
        drop(lease);
        meter.lock().record(WorkCounters::default()).expect("physical original cleanup");
        assert!(meter.lock().work.items_examined > before);
        assert_eq!(meter.lock().budget.items_examined, u64::MAX);
        let mut carrier = super::super::adapter::NativeDemandCarrier::Shared(&meter);
        let writer = gate.write_stable_with_carrier(
            Some(ViewGate::INITIAL_GENERATION), &mut carrier, &cancellation, None,
        ).await.expect("same original exclusive gate");
        assert!(gate.state.lock().expect("original gate").writer);
        drop(writer);
        assert!(!gate.state.lock().expect("original gate").writer);
    }
}
