use super::*;
use std::mem::size_of;

impl Scheduler {
    /// Applies a borrowed committed event on the optional history ledger.
    pub fn apply(
        &mut self,
        event: &SchedulerEvent,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        self.validate_event(event, work)?;
        if matches!(event, SchedulerEvent::Completed { .. })
            && self
                .operation(event.operation_id())
                .is_some_and(|state| state.phase == OperationPhase::Terminal)
        {
            return Ok(());
        }
        self.apply_event(event.clone_with_work(work)?, work)
    }
    fn apply_event(
        &mut self,
        event: SchedulerEvent,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        // Bytes observed for the copied event; the part the projection retains stays charged.
        let mut observed = if work.is_some() {
            event.logical_owned_bytes()?
        } else {
            0
        };
        let mut retained = 0;
        let mut child_slot = None;
        let mut cancellation = Vec::new();
        let prepared = (|| {
            retained = event.retained_clone_owned_bytes()?;
            if let SchedulerEvent::Declared { spec } = &event {
                let mut bytes = size_of::<(OperationId, OperationState)>() as u64;
                if spec.parent.is_some() {
                    bytes += size_of::<((OperationId, String), OperationId)>() as u64;
                }
                if let Some(io) = work.as_deref_mut() {
                    io.admit_allocation(bytes)?;
                    io.retain_owned(bytes)?;
                    observed += bytes;
                }
                retained += bytes;
                if let Some(parent) = &spec.parent {
                    let slot = match work.as_deref_mut() {
                        Some(io) => io.clone_text(&parent.slot)?,
                        None => parent.slot.clone(),
                    };
                    if work.is_some() {
                        observed += slot.capacity() as u64;
                    }
                    retained += slot.capacity() as u64;
                    child_slot = Some(slot);
                }
            }
            if let SchedulerEvent::CancellationRequested {
                operation_id,
                recursive,
            } = &event
            {
                cancellation = self.cancellation_frontier(*operation_id, *recursive, work)?;
                if work.is_some() {
                    observed += (cancellation.capacity() * size_of::<OperationId>()) as u64;
                }
            }
            self.reserve_completion(&event, &cancellation, work)
        })();
        if let Err(error) = prepared {
            if let Some(io) = work.as_deref_mut() {
                let _ = io.release_owned(observed);
            }
            return Err(error);
        }
        let id = event.operation_id();
        let parent = match &event {
            SchedulerEvent::Declared { spec } => {
                spec.parent.as_ref().map(|parent| parent.operation_id)
            }
            _ => None,
        };
        let cancelled = matches!(event, SchedulerEvent::CancellationRequested { .. });
        let phase = if matches!(event, SchedulerEvent::Admitted { .. }) {
            OperationPhase::Admitted
        } else {
            OperationPhase::WaitingForCapacity
        };
        macro_rules! state {
            ($id:expr) => {
                self.operations.get_mut(&$id).expect("validated operation")
            };
        }
        match event {
            SchedulerEvent::SessionConfigured { limits, .. } => {
                self.sessions.insert(id, limits);
            }
            SchedulerEvent::ModelDispatchClaimed {
                attempt_id,
                step,
                request_digest,
                ceiling,
                ..
            } => {
                self.model_claims
                    .entry(id)
                    .or_insert_with(|| TaskModelClaims {
                        ceiling,
                        attempts: BTreeMap::new(),
                    })
                    .attempts
                    .entry(attempt_id)
                    .or_default()
                    .insert(step, request_digest);
            }
            SchedulerEvent::Declared { spec } => {
                let spec = *spec;
                if let Some(parent) = &spec.parent {
                    self.child_slots.insert(
                        (
                            parent.operation_id,
                            child_slot.take().expect("prepared child slot"),
                        ),
                        id,
                    );
                }
                self.operations.insert(
                    id,
                    OperationState {
                        spec,
                        phase: OperationPhase::WaitingForDependencies,
                        reservation: None,
                        checkpoint: None,
                        workflow: None,
                        outcome: None,
                        cancellation_requested: false,
                        revision: 0,
                    },
                );
            }
            SchedulerEvent::WaitingForCapacity { .. } => {
                state!(id).phase = OperationPhase::WaitingForCapacity;
            }
            SchedulerEvent::Admitted { reservation, .. }
            | SchedulerEvent::PartiallyAdmitted { reservation, .. } => {
                let state = state!(id);
                state.reservation = Some(reservation);
                state.phase = phase;
            }
            SchedulerEvent::Rejected { reason, .. } => {
                let state = state!(id);
                state.reservation = None;
                state.phase = OperationPhase::Terminal;
                state.outcome = Some(Outcome::Failed { message: reason });
                self.completion_order.push(id);
            }
            SchedulerEvent::Started { .. } => state!(id).phase = OperationPhase::Running,
            SchedulerEvent::Checkpointed { checkpoint, .. } => {
                state!(id).checkpoint = Some(checkpoint);
            }
            SchedulerEvent::WaitingForChildren { .. } | SchedulerEvent::LeaseReleased { .. } => {
                let phase = if matches!(event, SchedulerEvent::WaitingForChildren { .. }) {
                    OperationPhase::WaitingForChildren
                } else {
                    OperationPhase::WaitingForCapacity
                };
                let state = state!(id);
                state.reservation = None;
                if state.cancellation_requested {
                    state.phase = OperationPhase::Terminal;
                    state.outcome = Some(Outcome::Cancelled);
                    self.completion_order.push(id);
                } else {
                    state.phase = phase;
                }
            }
            SchedulerEvent::WorkflowSuspended {
                workflow_revision,
                waiting_command,
                ..
            } => {
                let state = state!(id);
                state.workflow = Some(WorkflowSuspension {
                    revision: workflow_revision,
                    waiting_command,
                    input: None,
                });
                state.reservation = None;
                state.phase = OperationPhase::Suspended;
            }
            SchedulerEvent::WorkflowResumed { input, .. } => {
                let state = state!(id);
                state
                    .workflow
                    .as_mut()
                    .expect("validated resume slot")
                    .input = Some(input);
                state.phase = OperationPhase::WaitingForCapacity;
            }
            SchedulerEvent::ReconciliationResumed { .. } => {
                let state = state!(id);
                state.phase = OperationPhase::Running;
                state.outcome = None;
            }
            SchedulerEvent::CancellationRequested { .. } => {
                for target in &cancellation {
                    self.cancel_validated(*target);
                }
            }
            SchedulerEvent::Completed { outcome, .. } => {
                let state = state!(id);
                state.phase = if matches!(outcome, Outcome::Indeterminate { .. }) {
                    OperationPhase::Reconciling
                } else {
                    state.reservation = None;
                    OperationPhase::Terminal
                };
                state.outcome = Some(outcome);
                if state.phase == OperationPhase::Terminal && !self.completion_order.contains(&id) {
                    self.completion_order.push(id);
                }
            }
            SchedulerEvent::Orchestrated {
                outcome, cancel, ..
            } => {
                for child in cancel {
                    self.cancel_validated(child);
                }
                let state = state!(id);
                state.phase = if matches!(outcome, Outcome::Indeterminate { .. }) {
                    OperationPhase::Reconciling
                } else {
                    OperationPhase::Terminal
                };
                state.outcome = Some(outcome);
                state.reservation = None;
                if state.phase == OperationPhase::Terminal {
                    self.completion_order.push(id);
                }
            }
        }
        if !cancelled {
            state!(id).revision += 1;
            if let Some(parent) = parent {
                state!(parent).revision += 1;
            }
        }
        if let Some(io) = work.as_deref_mut() {
            io.release_owned(observed.saturating_sub(retained))?;
        }
        Ok(())
    }
    pub(super) fn cancellation_descendant(
        &self,
        mut candidate: OperationId,
        root: OperationId,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<bool> {
        let owner = self
            .operation(root)
            .map(|state| state.spec.owner.authority());
        while candidate != root {
            observe(work)?;
            let Some(state) = self.operation(candidate) else {
                return Ok(false);
            };
            if Some(state.spec.owner.authority()) != owner {
                return Ok(false);
            }
            let Some(parent) = &state.spec.parent else {
                return Ok(false);
            };
            candidate = parent.operation_id;
        }
        Ok(true)
    }
    fn cancel_validated(&mut self, id: OperationId) {
        let state = self.operations.get_mut(&id).expect("validated operation");
        if state.phase == OperationPhase::Terminal {
            return;
        }
        state.revision += 1;
        if !matches!(
            state.phase,
            OperationPhase::Running | OperationPhase::Reconciling
        ) && state.reservation.is_none()
        {
            state.phase = OperationPhase::Terminal;
            state.outcome = Some(Outcome::Cancelled);
            self.completion_order.push(id);
        } else {
            state.cancellation_requested = true;
        }
    }
    /// Reserves completion-order slots before mutation so pushes cannot fail.
    fn reserve_completion(
        &mut self,
        event: &SchedulerEvent,
        cancellation: &[OperationId],
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        let state = self.operation(event.operation_id());
        let mut extra = match event {
            SchedulerEvent::Rejected { .. } => 1,
            SchedulerEvent::WaitingForChildren { .. } | SchedulerEvent::LeaseReleased { .. } => {
                usize::from(state.is_some_and(|state| state.cancellation_requested))
            }
            SchedulerEvent::Completed { outcome, .. } => usize::from(
                !matches!(outcome, Outcome::Indeterminate { .. })
                    && state.is_some_and(|state| state.phase != OperationPhase::Terminal)
                    && !self.completion_order.contains(&event.operation_id()),
            ),
            SchedulerEvent::Orchestrated { outcome, .. } => {
                usize::from(!matches!(outcome, Outcome::Indeterminate { .. }))
            }
            _ => 0,
        };
        let cancel = match event {
            SchedulerEvent::CancellationRequested { .. } => cancellation,
            SchedulerEvent::Orchestrated { cancel, .. } => cancel.as_slice(),
            _ => &[],
        };
        for id in cancel {
            if self.operation(*id).is_some_and(|state| {
                !matches!(
                    state.phase,
                    OperationPhase::Terminal | OperationPhase::Running | OperationPhase::Reconciling
                ) && state.reservation.is_none()
            }) {
                extra += 1;
            }
        }
        let target = self
            .completion_order
            .len()
            .checked_add(extra)
            .ok_or_else(|| diagnostic(work, Invalid, "scheduler completion slots overflow"))?;
        if target <= self.completion_order.capacity() {
            return Ok(());
        }
        let bytes = target
            .checked_mul(size_of::<OperationId>())
            .ok_or_else(|| diagnostic(work, Invalid, "scheduler completion slots overflow"))?
            as u64;
        let copied = (self.completion_order.len() * size_of::<OperationId>()) as u64;
        if let Some(io) = work.as_deref_mut() {
            io.admit_owned(bytes)?;
            io.admit_copy(copied)?;
            io.admit_allocation(bytes)?;
            io.retain_owned(bytes)?;
            if let Err(error) = io.copy_bytes(copied) {
                let _ = io.release_owned(bytes);
                return Err(error);
            }
        }
        if let Err(cause) = self
            .completion_order
            .try_reserve_exact(target - self.completion_order.len())
        {
            return Err(match work.as_deref_mut() {
                Some(io) => {
                    let error = io.allocation_failure(cause);
                    let _ = io.release_owned(bytes);
                    error
                }
                None => Error::Storage(cause.to_string()),
            });
        }
        Ok(())
    }
}
