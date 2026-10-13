use super::*;

pub(super) struct ReadAdmission {
    pub(super) gate: Arc<ViewGate>,
    pub(super) slot: Option<usize>,
    pub(super) registry: Option<(u64, u64)>,
    pub(super) finished: u64,
    pub(super) payload_finished: u64,
}

impl ReadAdmission {
    fn finished_payload(&self) -> u64 {
        self.slot.map_or(0, |index| {
            let state = self
                .gate
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            match &state.reader_slots[index] {
                Some(ReadSlot::Meter {
                    payload_finished, ..
                }) => *payload_finished,
                _ => unreachable!("live original read admission"),
            }
        })
    }
}

impl Drop for ReadAdmission {
    fn drop(&mut self) {
        if let Some(index) = self.slot {
            let mut state = self
                .gate
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(ReadSlot::Meter {
                readers,
                proof_alive,
                ..
            }) = &mut state.reader_slots[index]
            {
                *proof_alive = false;
                if *readers == 0 {
                    state.reader_slots[index] = None;
                    state.occupied_slots = state
                        .occupied_slots
                        .checked_sub(1)
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
}

impl ViewGateReadAdmissions {
    fn entries(&self) -> impl Iterator<Item = &ReadAdmission> {
        self.first.iter().chain(self.more.iter())
    }

    pub(crate) fn freed_bytes(&self) -> u64 {
        self.entries().fold(0_u64, |freed, entry| {
            let registry = entry
                .registry
                .filter(|(generation, _)| {
                    *generation != entry.gate.reader_generation.load(Ordering::Acquire)
                })
                .map_or(0, |(_, bytes)| bytes);
            let payload = entry
                .finished_payload()
                .checked_sub(entry.payload_finished)
                .expect("original binding payload progress");
            freed
                .checked_add(registry)
                .and_then(|freed| freed.checked_add(payload))
                .expect("owned original read allocations")
        })
    }

    pub(crate) fn reconcile(
        &mut self,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
    ) -> Result<(), crate::WorkError> {
        for entry in self.first.iter_mut().chain(self.more.iter_mut()) {
            if let Some((generation, bytes)) = entry.registry
                && generation != entry.gate.reader_generation.load(Ordering::Acquire)
            {
                allocations
                    .release(bytes)
                    .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
                entry.registry = None;
            }
            let finished = entry.finished_payload();
            let bytes = finished
                .checked_sub(entry.payload_finished)
                .ok_or(crate::WorkError::Overflow)?;
            allocations
                .release(bytes)
                .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
            entry.payload_finished = finished;
        }
        Ok(())
    }

    pub(crate) fn reconcile_work(
        &mut self,
        work: &mut crate::WorkCounters,
        budget: &mut crate::WorkBudget,
    ) -> Result<(), crate::WorkError> {
        let mut completed = 0_u64;
        for entry in self.first.iter_mut().chain(self.more.iter_mut()) {
            let Some(index) = entry.slot else { continue };
            let finished = {
                let state = entry
                    .gate
                    .state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                match state.reader_slots.get(index).and_then(Option::as_ref) {
                    Some(ReadSlot::Meter { finished, .. }) => *finished,
                    _ => return Err(crate::WorkError::Overflow),
                }
            };
            let newly_finished = finished
                .checked_sub(entry.finished)
                .ok_or(crate::WorkError::Overflow)?;
            // These physical releases already happened. Preserve their exact
            // prefix before applying any reconstructed or tighter ceiling.
            *work = work.checked_add(crate::WorkCounters {
                items_examined: newly_finished,
                ..crate::WorkCounters::default()
            })?;
            entry.finished = finished;
            completed = completed
                .checked_add(newly_finished)
                .ok_or(crate::WorkError::Overflow)?;
        }
        budget.items_examined = budget
            .items_examined
            .checked_add(completed)
            .ok_or(crate::WorkError::Overflow)?;
        work.verify(*budget)?;
        Ok(())
    }

    pub(super) fn entry_mut(
        &mut self,
        gate: &Arc<ViewGate>,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut crate::WorkCounters,
        budget: crate::WorkBudget,
    ) -> Result<&mut ReadAdmission, MountSourceError> {
        let mut found = None;
        for (index, entry) in self.entries().enumerate() {
            work.charge(
                &crate::WorkCounters {
                    items_examined: 1,
                    ..crate::WorkCounters::default()
                },
                &budget,
            )
            .map_err(MountSourceError::Work)?;
            if Arc::ptr_eq(&entry.gate, gate) {
                found = Some(index);
                break;
            }
        }
        if let Some(index) = found {
            return if index == 0 {
                self.first.as_mut()
            } else {
                self.more.get_mut(index - 1)
            }
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow));
        }
        if self.first.is_none() {
            return Ok(self.first.insert(ReadAdmission {
                gate: Arc::clone(gate),
                slot: None,
                registry: None,
                finished: 0,
                payload_finished: 0,
            }));
        }
        if self.more.len() == self.more.capacity() {
            let count = self
                .more
                .capacity()
                .max(1)
                .checked_mul(2)
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            let copied = crate::WorkCounters {
                items_examined: self.more.len() as u64,
                bytes_copied: (self.more.len() * std::mem::size_of::<ReadAdmission>()) as u64,
                ..crate::WorkCounters::default()
            };
            work.admit(&copied, &budget)
                .map_err(MountSourceError::Work)?;
            let bytes = count
                .checked_mul(std::mem::size_of::<ReadAdmission>())
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?
                as u64;
            allocations
                .claim_bytes(bytes, 1, work, budget)
                .map_err(gate_allocation_error)?;
            let mut replacement = Vec::new();
            if replacement.try_reserve_exact(count).is_err() {
                allocations.release(bytes).map_err(gate_allocation_error)?;
                return Err(MountSourceError::Engine(std::borrow::Cow::Owned(
                    "view admission allocation failed".into(),
                )));
            }
            let old_bytes = (self.more.capacity() * std::mem::size_of::<ReadAdmission>()) as u64;
            replacement.append(&mut self.more);
            self.more = replacement;
            work.charge(&copied, &budget)
                .map_err(MountSourceError::Work)?;
            allocations
                .release(old_bytes)
                .map_err(gate_allocation_error)?;
        }
        self.more.push(ReadAdmission {
            gate: Arc::clone(gate),
            slot: None,
            registry: None,
            finished: 0,
            payload_finished: 0,
        });
        self.more
            .last_mut()
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))
    }
}

pub(super) fn gate_allocation_error(
    error: crate::kernel::allocation::AllocationError,
) -> MountSourceError {
    super::super::lazy::mount_engine_error(crate::collection::handle_allocation_error(error))
}

pub(super) fn reconcile_reader_registry(
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

pub(super) fn insert_reader_slot(
    state: &mut ViewState,
    first_free: &mut Option<usize>,
    second_free: &mut Option<usize>,
    slot: ReadSlot,
) -> Result<usize, MountSourceError> {
    let index = if let Some(index) = first_free.take() {
        *first_free = second_free.take();
        *state
            .reader_slots
            .get_mut(index)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? = Some(slot);
        index
    } else {
        let index = state.reader_slots.len();
        state.reader_slots.push(Some(slot));
        index
    };
    state.occupied_slots += 1;
    Ok(index)
}
