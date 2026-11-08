use super::*;

pub(in crate::native_mount) struct ViewReadLease {
    pub(in crate::native_mount) generation: u64,
    pub(super) gate: Arc<ViewGate>,
    pub(super) key: [u8; 16],
    pub(super) owner: crate::kernel::allocation::retained::RetainedArc<ReadOwner>,
    pub(super) payload: Option<crate::kernel::allocation::retained::RetainedAllocation>,
    // Settles after the owner and payload have physically left this lease.
    pub(super) completion: crate::demand::ViewReadCompletion,
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
    pub(in crate::native_mount) fn fund_payload(
        &mut self,
        bytes: u64,
        carrier: &mut super::super::adapter::NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        let mut allocation = carrier.with_accounting(|accounting| {
            crate::kernel::allocation::retained::RetainedAllocation::claim(bytes, 1, accounting)
        })?;
        if let Some(payload) = &mut self.payload {
            payload
                .absorb(&mut allocation)
                .map_err(super::super::lazy::mount_engine_error)?;
        } else {
            self.payload = Some(allocation);
        }
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
        assert!(state.readers > 0, "owned view read");
        state.readers -= 1;
        let readers = self.owner.readers.load(Ordering::Relaxed);
        assert!(readers > 0, "owned callback read");
        self.owner.readers.store(readers - 1, Ordering::Relaxed);
        let retired = if readers == 1 {
            // The complete key identifies this still-live callback owner.
            // Retain the redundant parent too: even its funding must leave
            // the gate before retirement can reenter the original ledger.
            Some(
                state
                    .owners
                    .extract_prefix(&self.key, |_| {})
                    .unwrap_or_else(|_| unreachable!("complete reader key fits its index")),
            )
        } else {
            None
        };
        let wake = state.readers == 0;
        drop(state);
        // Original funding can reenter its ledger; release it outside the gate.
        drop(retired);
        self.completion.completed();
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
