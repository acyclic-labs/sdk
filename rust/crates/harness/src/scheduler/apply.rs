use super::*;
use std::mem::size_of;

const DECLARATION: usize = 0;
const RESERVATION: usize = 1;
const CHECKPOINT: usize = 2;
const WORKFLOW: usize = 3;
const OUTCOME: usize = 4;

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
        // Guards precede temporary payloads; failures retire the event explicitly.
        let mut custody: crate::NativeCustody = None;
        let mut child_slot = None;
        let mut cancellation = Vec::new();
        let mut observer_owned = if work.is_some() {
            event.logical_owned_bytes()?
        } else {
            0
        };
        let mut payload_owned = 0;
        let mut retained_owned = 0;
        let prepared = (|| {
            payload_owned = event.retained_clone_owned_bytes()?;
            if let SchedulerEvent::Declared { spec } = &event {
                let bytes = size_of::<(OperationId, OperationState)>() as u64
                    + if spec.parent.is_some() {
                        size_of::<((OperationId, String), OperationId)>() as u64
                    } else {
                        0
                    };
                if let Some(io) = work.as_deref_mut() {
                    io.admit_allocation(bytes)?;
                    io.retain_owned(bytes)?;
                    observer_owned += bytes;
                }
                payload_owned += bytes;
            }
            retained_owned = payload_owned;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if !self.native_operations.contains_key(&event.operation_id())
                && (matches!(event, SchedulerEvent::Declared { .. }) || retained_owned != 0)
                && let Some(io) = work
                    .as_deref_mut()
                    .filter(|io| io.original_native_work().is_some())
            {
                let bytes =
                    size_of::<(OperationId, [Option<acyclic_fs::NativeAllocation>; 6])>() as u64;
                io.admit_allocation(bytes)?;
                io.retain_owned(bytes)?;
                observer_owned += bytes;
                retained_owned += bytes;
            }
            if let SchedulerEvent::Declared { spec } = &event
                && let Some(parent) = &spec.parent
            {
                let slot = match work.as_deref_mut() {
                    Some(io) => io.clone_text(&parent.slot)?,
                    None => parent.slot.clone(),
                };
                observer_owned = observer_owned
                    .checked_add(if work.is_some() {
                        slot.capacity() as u64
                    } else {
                        0
                    })
                    .ok_or_else(|| {
                        diagnostic(work, Invalid, "scheduler clone custody overflows")
                    })?;
                payload_owned += slot.capacity() as u64;
                retained_owned += slot.capacity() as u64;
                child_slot = Some(slot);
            }
            if let SchedulerEvent::CancellationRequested {
                operation_id,
                recursive,
            } = &event
            {
                cancellation = self.cancellation_frontier(*operation_id, *recursive, work)?;
                if work.is_some() {
                    observer_owned = observer_owned
                        .checked_add((cancellation.capacity() * size_of::<OperationId>()) as u64)
                        .ok_or_else(|| {
                            diagnostic(work, Invalid, "scheduler clone custody overflows")
                        })?;
                }
            }
            self.reserve_completion(&event, &cancellation, work)?;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(io) = work.as_deref_mut()
                && io.original_native_work().is_some()
            {
                custody = Some(io.take_native_custody(observer_owned)?);
                observer_owned = 0;
            }
            Ok(())
        })();
        if let Err(error) = prepared {
            drop(event);
            drop(child_slot);
            drop(cancellation);
            drop(custody);
            if let Some(io) = work.as_deref_mut() {
                let _ = io.release_owned(observer_owned);
            }
            return Err(error);
        }
        if let Some(io) = work.as_deref_mut()
            && custody.is_none()
        {
            observer_owned = observer_owned.checked_sub(retained_owned).ok_or_else(|| {
                diagnostic(
                    &Some(&mut *io),
                    Invalid,
                    "scheduler clone custody underflows",
                )
            })?;
        }
        // Every fallible admission is complete before mutation.
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
                self.install_native_field(id, DECLARATION, payload_owned, &mut custody);
            }
            SchedulerEvent::WaitingForCapacity { .. } => {
                self.operations
                    .get_mut(&id)
                    .expect("validated operation")
                    .phase = OperationPhase::WaitingForCapacity
            }
            SchedulerEvent::Admitted { reservation, .. }
            | SchedulerEvent::PartiallyAdmitted { reservation, .. } => {
                let state = self.operations.get_mut(&id).expect("validated operation");
                state.reservation = Some(reservation);
                state.phase = phase;
                self.install_native_field(id, RESERVATION, payload_owned, &mut custody);
            }
            SchedulerEvent::Rejected { reason, .. } => {
                let state = self.operations.get_mut(&id).expect("validated operation");
                state.reservation = None;
                state.phase = OperationPhase::Terminal;
                state.outcome = Some(Outcome::Failed { message: reason });
                self.completion_order.push(id);
                self.install_native_field(id, OUTCOME, payload_owned, &mut custody);
            }
            SchedulerEvent::Started { .. } => {
                self.operations
                    .get_mut(&id)
                    .expect("validated operation")
                    .phase = OperationPhase::Running
            }
            SchedulerEvent::Checkpointed { checkpoint, .. } => {
                self.operations
                    .get_mut(&id)
                    .expect("validated operation")
                    .checkpoint = Some(checkpoint);
                self.install_native_field(id, CHECKPOINT, payload_owned, &mut custody);
            }
            SchedulerEvent::WaitingForChildren { .. } | SchedulerEvent::LeaseReleased { .. } => {
                let phase = if matches!(event, SchedulerEvent::WaitingForChildren { .. }) {
                    OperationPhase::WaitingForChildren
                } else {
                    OperationPhase::WaitingForCapacity
                };
                let state = self.operations.get_mut(&id).expect("validated operation");
                state.reservation = None;
                if state.cancellation_requested {
                    state.phase = OperationPhase::Terminal;
                    state.outcome = Some(Outcome::Cancelled);
                    self.completion_order.push(id);
                } else {
                    state.phase = phase;
                }
                self.retire_native_fields(id);
            }
            SchedulerEvent::WorkflowSuspended {
                workflow_revision,
                waiting_command,
                ..
            } => {
                let state = self.operations.get_mut(&id).expect("validated operation");
                state.workflow = Some(WorkflowSuspension {
                    revision: workflow_revision,
                    waiting_command,
                    input: None,
                });
                state.reservation = None;
                state.phase = OperationPhase::Suspended;
                self.retire_native_fields(id);
            }
            SchedulerEvent::WorkflowResumed { input, .. } => {
                let state = self.operations.get_mut(&id).expect("validated operation");
                state
                    .workflow
                    .as_mut()
                    .expect("validated resume slot")
                    .input = Some(input);
                state.phase = OperationPhase::WaitingForCapacity;
                self.install_native_field(id, WORKFLOW, payload_owned, &mut custody);
            }
            SchedulerEvent::ReconciliationResumed { .. } => {
                let state = self.operations.get_mut(&id).expect("validated operation");
                state.phase = OperationPhase::Running;
                state.outcome = None;
                self.retire_native_fields(id);
            }
            SchedulerEvent::CancellationRequested { .. } => {
                for target in &cancellation {
                    self.cancel_validated(*target);
                }
            }
            SchedulerEvent::Completed { outcome, .. } => {
                let state = self.operations.get_mut(&id).expect("validated operation");
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
                self.install_native_field(id, OUTCOME, payload_owned, &mut custody);
                self.retire_native_fields(id);
            }
            SchedulerEvent::Orchestrated {
                outcome, cancel, ..
            } => {
                for child in cancel {
                    self.cancel_validated(child);
                }
                let state = self.operations.get_mut(&id).expect("validated operation");
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
                self.install_native_field(id, OUTCOME, payload_owned, &mut custody);
                self.retire_native_fields(id);
            }
        }
        if !cancelled {
            self.operations
                .get_mut(&id)
                .expect("validated operation")
                .revision += 1;
            if let Some(parent) = parent {
                self.operations
                    .get_mut(&parent)
                    .expect("validated parent")
                    .revision += 1;
            }
        }
        drop(child_slot);
        drop(cancellation);
        drop(custody);
        if let Some(io) = work.as_deref_mut() {
            io.release_owned(observer_owned)?;
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
        let state = self
            .operations
            .get_mut(&id)
            .expect("validated cancellation target");
        if state.phase == OperationPhase::Terminal {
            return;
        }
        let close = !matches!(
            state.phase,
            OperationPhase::Running | OperationPhase::Reconciling
        ) && state.reservation.is_none();
        state.revision += 1;
        if close {
            state.reservation = None;
            state.phase = OperationPhase::Terminal;
            state.outcome = Some(Outcome::Cancelled);
            self.completion_order.push(id);
        } else {
            state.cancellation_requested = true;
        }
        self.retire_native_fields(id);
    }
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
                state.phase != OperationPhase::Terminal
                    && !matches!(
                        state.phase,
                        OperationPhase::Running | OperationPhase::Reconciling
                    )
                    && state.reservation.is_none()
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
        if let Some(io) = work.as_deref_mut() {
            io.admit_owned(bytes)?;
            io.admit_copy((self.completion_order.len() * size_of::<OperationId>()) as u64)?;
            io.admit_allocation(bytes)?;
            io.retain_owned(bytes)?;
            if let Err(error) =
                io.copy_bytes((self.completion_order.len() * size_of::<OperationId>()) as u64)
            {
                let _ = io.release_owned(bytes);
                return Err(error);
            }
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let custody = match work.as_deref_mut() {
            Some(io) if io.original_native_work().is_some() => Some(io.take_native_custody(bytes)?),
            _ => None,
        };
        if let Err(cause) = self
            .completion_order
            .try_reserve_exact(target - self.completion_order.len())
        {
            let error = match work.as_deref_mut() {
                Some(io) => io.allocation_failure(cause),
                None => Error::Storage(cause.to_string()),
            };
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if custody.is_some() {
                return Err(error);
            }
            if let Some(io) = work.as_deref_mut() {
                let _ = io.release_owned(bytes);
            }
            return Err(error);
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        {
            self.native_completion = custody;
        }
        Ok(())
    }
    fn install_native_field(
        &mut self,
        id: OperationId,
        slot: usize,
        bytes: u64,
        incoming: &mut crate::NativeCustody,
    ) {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        {
            if incoming.is_none() && !self.native_operations.contains_key(&id) {
                return;
            }
            let bytes = if incoming.is_some() { bytes } else { 0 };
            if bytes == 0 && !self.native_operations.contains_key(&id) {
                return;
            }
            let metadata = if !self.native_operations.contains_key(&id) {
                incoming.as_mut().map(|guard| {
                    guard
                        .split_off(
                            size_of::<(OperationId, [Option<acyclic_fs::NativeAllocation>; 6])>()
                                as u64,
                        )
                        .expect("preclaimed guard-map tuple")
                })
            } else {
                None
            };
            let guard = incoming
                .as_mut()
                .map(|guard| guard.split_off(bytes).expect("preclaimed payload transfer"));
            let held = self.native_operations.entry(id).or_default();
            if metadata.is_some() {
                held[5] = metadata;
            }
            held[slot] = guard;
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = (id, slot, bytes, incoming);
    }
    fn retire_native_fields(&mut self, id: OperationId) {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(held) = self.native_operations.get_mut(&id) {
            let state = &self.operations[&id];
            if state.reservation.is_none() {
                held[RESERVATION] = None;
            }
            if state.checkpoint.is_none() {
                held[CHECKPOINT] = None;
            }
            if state
                .workflow
                .as_ref()
                .is_none_or(|slot| slot.input.is_none())
            {
                held[WORKFLOW] = None;
            }
            if state.outcome.as_ref().is_none_or(|outcome| {
                matches!(outcome, Outcome::Cancelled | Outcome::Indeterminate { .. })
            }) {
                held[OUTCOME] = None;
            }
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = id;
    }
}
