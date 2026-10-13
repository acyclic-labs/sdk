use super::*;

pub(in crate::native_mount) struct ViewReadLease {
    pub(in crate::native_mount) generation: u64,
    pub(super) gate: Arc<ViewGate>,
    pub(super) owner_slot: usize,
    pub(super) cleanup_slot: Option<usize>,
    pub(super) payload_bytes: u64,
}

pub(in crate::native_mount) struct ViewWriteLease {
    pub(in crate::native_mount) generation: u64,
    pub(super) gate: Arc<ViewGate>,
    pub(super) armed: bool,
}

pub(in crate::native_mount) struct ViewMutationPhase {
    pub(in crate::native_mount) generation: u64,
    pub(super) gate: Arc<ViewGate>,
}

impl ViewWriteLease {
    pub(in crate::native_mount) fn into_mutation_phase(mut self) -> ViewMutationPhase {
        let mut state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        assert!(
            state.writer && !state.mutation_phase,
            "original exclusive view owner"
        );
        state.mutation_phase = true;
        state.writer = false;
        self.armed = false;
        let phase = ViewMutationPhase {
            generation: self.generation,
            gate: Arc::clone(&self.gate),
        };
        drop(state);
        self.gate.changed.notify_waiters();
        phase
    }
}

impl Drop for ViewMutationPhase {
    fn drop(&mut self) {
        let mut state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.mutation_phase = false;
        drop(state);
        self.gate.changed.notify_waiters();
    }
}

pub(super) struct WaitingWriter {
    pub(super) gate: Arc<ViewGate>,
    registered: bool,
}

impl ViewReadLease {
    /// Transfers an already admitted visible owner payload to this original lease.
    pub(in crate::native_mount) fn attach_owned_payload(
        &mut self,
        bytes: u64,
    ) -> Result<(), MountSourceError> {
        let mut state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let index = self
            .cleanup_slot
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let Some(ReadSlot::Meter {
            payload_active,
            payload_finished,
            proof_alive: true,
            ..
        }) = state.reader_slots.get_mut(index).and_then(Option::as_mut)
        else {
            return Err(MountSourceError::Work(crate::WorkError::Overflow));
        };
        let active = payload_active
            .checked_add(bytes)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        payload_finished
            .checked_add(active)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let owned = self
            .payload_bytes
            .checked_add(bytes)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        *payload_active = active;
        self.payload_bytes = owned;
        Ok(())
    }
}

impl Drop for ViewReadLease {
    fn drop(&mut self) {
        let mut state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.readers = state.readers.checked_sub(1).expect("owned view read");
        if let Some(ReadSlot::Owner { readers, .. }) = &mut state.reader_slots[self.owner_slot] {
            *readers = readers.checked_sub(1).expect("owned callback read");
            if *readers == 0 {
                state.reader_slots[self.owner_slot] = None;
                state.occupied_slots = state
                    .occupied_slots
                    .checked_sub(1)
                    .expect("owned callback slot");
            }
        }
        if let Some(index) = self.cleanup_slot
            && let Some(ReadSlot::Meter {
                readers,
                finished,
                proof_alive,
                payload_active,
                payload_finished,
            }) = &mut state.reader_slots[index]
        {
            *readers = readers.checked_sub(1).expect("owned view read cleanup");
            *finished = finished
                .checked_add(1)
                .expect("admitted view read cleanup addressability");
            *payload_active = payload_active
                .checked_sub(self.payload_bytes)
                .expect("owned original binding payload");
            *payload_finished = payload_finished
                .checked_add(self.payload_bytes)
                .expect("admitted binding payload cleanup addressability");
            if *readers == 0 && !*proof_alive {
                state.reader_slots[index] = None;
                state.occupied_slots = state
                    .occupied_slots
                    .checked_sub(1)
                    .expect("owned cleanup slot");
            }
        }
        state.release_empty_registry(&self.gate);
        let wake = state.readers == 0;
        drop(state);
        if wake {
            self.gate.changed.notify_waiters();
        }
    }
}

impl WaitingWriter {
    pub(super) fn register(gate: &Arc<ViewGate>) -> Self {
        let mut state = gate.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.waiting_writers = state.waiting_writers.saturating_add(1);
        drop(state);
        Self {
            gate: Arc::clone(gate),
            registered: true,
        }
    }

    pub(super) fn admit(&mut self, state: &mut ViewState) {
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
        if !self.armed {
            return;
        }
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
