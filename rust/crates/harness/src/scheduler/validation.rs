use super::*;

impl Scheduler {
    pub(super) fn validated_operation(
        &self,
        id: OperationId,
        work: &Option<&mut dyn HistoryIo>,
    ) -> Result<&OperationState> {
        self.operation(id)
            .ok_or_else(|| diagnostic(work, NotFound, "scheduler operation"))
    }
    pub(crate) fn validate_event(
        &self,
        event: &SchedulerEvent,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        observe(work)?;
        let id = event.operation_id();
        let parent = match event {
            SchedulerEvent::Declared { spec } => {
                spec.parent.as_ref().map(|parent| parent.operation_id)
            }
            _ => None,
        };
        for id in std::iter::once(id).chain(parent) {
            if let Some(state) = self.operation(id) {
                revision(state.revision, work)?;
            }
        }
        if let SchedulerEvent::Orchestrated { cancel, .. } = event {
            for id in cancel {
                observe(work)?;
                if let Some(state) = self.operation(*id) {
                    revision(state.revision, work)?;
                }
            }
        }
        match event {
            SchedulerEvent::Declared { spec } => self.validate_declaration(spec, work)?,
            SchedulerEvent::SessionConfigured { limits, .. } => {
                limits.validate(work)?;
                let state = self.validated_operation(id, work)?;
                if state.spec.parent.is_some()
                    || state.reservation.is_some()
                    || state.cancellation_requested
                    || !matches!(
                        state.phase,
                        OperationPhase::WaitingForDependencies | OperationPhase::WaitingForCapacity
                    )
                    || self.children(id).next().is_some()
                {
                    return Err(diagnostic(
                        work,
                        Conflict,
                        "session must be configured before descendants or dispatch",
                    ));
                }
                self.require_session_policy(id, limits, work)?;
            }
            SchedulerEvent::ModelDispatchClaimed {
                attempt_id,
                step,
                request_digest,
                fence,
                ceiling,
                ..
            } => {
                require_execution_owner(self.validated_operation(id, work)?, fence, false, work)?;
                if *request_digest == [0; 32] || *ceiling == 0 {
                    return Err(diagnostic(
                        work,
                        Invalid,
                        "model dispatch claim is invalid or cancelled",
                    ));
                }
                let (root, _) = self.session_root(id, work)?;
                let limits = self.sessions.get(&root).ok_or_else(|| {
                    diagnostic(work, Unsupported, "session budget is not configured")
                })?;
                if *ceiling > limits.model_steps {
                    return Err(diagnostic(
                        work,
                        Unauthorized,
                        "task ceiling exceeds session",
                    ));
                }
                let claims = self.model_claims.get(&id);
                if claims.is_some_and(|claims| claims.ceiling != *ceiling) {
                    return Err(diagnostic(
                        work,
                        Unauthorized,
                        "task model ceiling differs from its admission",
                    ));
                }
                if let Some(previous) =
                    claims.and_then(|claims| claims.attempts.get(attempt_id)?.get(step))
                {
                    if previous != request_digest {
                        return Err(diagnostic(work, Conflict, "model attempt changed request"));
                    }
                } else {
                    let mut used = 0u64;
                    for (task, claims) in &self.model_claims {
                        observe(work)?;
                        if self.session_root(*task, work)?.0 == root {
                            used = used.checked_add(claims.used()).ok_or_else(|| {
                                diagnostic(work, Invalid, "model step accounting overflows")
                            })?;
                        }
                    }
                    if used >= limits.model_steps
                        || claims.map_or(0, TaskModelClaims::used) >= *ceiling
                    {
                        return Err(diagnostic(work, Invalid, "model step budget exhausted"));
                    }
                }
            }
            SchedulerEvent::WaitingForCapacity { .. } => {
                self.require_dependencies_ready(id, work)?;
                require_phase(
                    self.validated_operation(id, work)?,
                    OperationPhase::WaitingForDependencies,
                    work,
                )?;
            }
            SchedulerEvent::Admitted { reservation, .. }
            | SchedulerEvent::PartiallyAdmitted { reservation, .. } => {
                self.require_dependencies_ready(id, work)?;
                self.require_session_capacity(id, work)?;
                let state = self.validated_operation(id, work)?;
                if !matches!(
                    state.phase,
                    OperationPhase::WaitingForDependencies | OperationPhase::WaitingForCapacity
                ) || state.cancellation_requested
                {
                    return Err(diagnostic(
                        work,
                        Conflict,
                        "operation cannot be admitted in its current phase",
                    ));
                }
                match event {
                    SchedulerEvent::Admitted { .. } => {
                        if !covers(&reservation.admitted.0, &state.spec.resources, work)? {
                            return Err(diagnostic(
                                work,
                                Conflict,
                                "partial reservation cannot start an operation",
                            ));
                        }
                        if state
                            .reservation
                            .as_ref()
                            .is_some_and(|partial| !same_lease(partial, reservation))
                        {
                            return Err(diagnostic(
                                work,
                                Conflict,
                                "partial admission must be completed by the same reservation",
                            ));
                        }
                    }
                    _ => {
                        if !covers(&state.spec.resources.0, &reservation.admitted, work)?
                            || covers(&reservation.admitted.0, &state.spec.resources, work)?
                        {
                            return Err(diagnostic(
                                work,
                                Invalid,
                                "partial admission must be non-excessive and incomplete",
                            ));
                        }
                        if let Some(previous) = &state.reservation
                            && (!same_lease(previous, reservation)
                                || !covers(&reservation.admitted.0, &previous.admitted, work)?)
                        {
                            return Err(diagnostic(
                                work,
                                Conflict,
                                "partial reservation must advance the same lease",
                            ));
                        }
                    }
                }
            }
            SchedulerEvent::Rejected { reason, .. } => {
                observe(work)?;
                if reason.trim().is_empty() {
                    return Err(diagnostic(work, Invalid, "rejection reason is empty"));
                }
                let state = self.validated_operation(id, work)?;
                if !matches!(
                    state.phase,
                    OperationPhase::WaitingForDependencies
                        | OperationPhase::WaitingForCapacity
                        | OperationPhase::Admitted
                ) || state.reservation.is_some()
                {
                    return Err(diagnostic(
                        work,
                        Conflict,
                        "owned operation requires a fenced release",
                    ));
                }
            }
            SchedulerEvent::Started { fence, .. }
            | SchedulerEvent::Checkpointed { fence, .. }
            | SchedulerEvent::WaitingForChildren { fence, .. }
            | SchedulerEvent::ReconciliationResumed { fence, .. } => {
                let state = self.validated_operation(id, work)?;
                let expected = match event {
                    SchedulerEvent::Started { .. } => OperationPhase::Admitted,
                    SchedulerEvent::ReconciliationResumed { .. } => OperationPhase::Reconciling,
                    _ => OperationPhase::Running,
                };
                require_phase(state, expected, work)?;
                require_fence(state, fence, work)?;
                if matches!(
                    event,
                    SchedulerEvent::Started { .. } | SchedulerEvent::ReconciliationResumed { .. }
                ) && state.cancellation_requested
                {
                    return Err(diagnostic(
                        work,
                        Conflict,
                        "cancelled operation cannot start fresh work",
                    ));
                }
            }
            SchedulerEvent::WorkflowSuspended {
                fence,
                workflow_revision,
                ..
            } => {
                let state = self.validated_operation(id, work)?;
                require_execution_owner(state, fence, false, work)?;
                if *workflow_revision == 0
                    || state.workflow.as_ref().is_some_and(|slot| {
                        *workflow_revision < slot.revision
                            || (*workflow_revision == slot.revision && slot.input.is_none())
                    })
                {
                    return Err(diagnostic(
                        work,
                        Conflict,
                        "workflow suspension revision did not advance",
                    ));
                }
            }
            SchedulerEvent::WorkflowResumed {
                workflow_revision,
                input,
                ..
            } => {
                input.validate(work)?;
                let state = self.validated_operation(id, work)?;
                require_phase(state, OperationPhase::Suspended, work)?;
                if state.cancellation_requested
                    || state.workflow.as_ref().is_none_or(|slot| {
                        slot.revision != *workflow_revision || slot.input.is_some()
                    })
                {
                    return Err(diagnostic(
                        work,
                        Conflict,
                        "workflow resume slot is not available",
                    ));
                }
            }
            SchedulerEvent::LeaseReleased { fence, .. } => {
                let state = self.validated_operation(id, work)?;
                if !matches!(
                    state.phase,
                    OperationPhase::WaitingForCapacity
                        | OperationPhase::Admitted
                        | OperationPhase::Running
                ) {
                    return Err(diagnostic(
                        work,
                        Conflict,
                        "only an active lease can be released",
                    ));
                }
                require_fence(state, fence, work)?;
            }
            SchedulerEvent::CancellationRequested { recursive, .. } => {
                self.validated_operation(id, work)?;
                for (candidate, state) in &self.operations {
                    observe(work)?;
                    if *candidate == id
                        || (*recursive && self.cancellation_descendant(*candidate, id, work)?)
                    {
                        revision(state.revision, work)?;
                    }
                }
            }
            SchedulerEvent::Completed {
                outcome,
                fence,
                execution_duration_ns,
                ..
            } => {
                if execution_duration_ns.is_some()
                    && (fence.is_none() || matches!(outcome, Outcome::Indeterminate { .. }))
                {
                    return Err(diagnostic(
                        work,
                        Invalid,
                        "worker duration requires a fenced terminal completion",
                    ));
                }
                if let Outcome::Succeeded(file) = outcome {
                    file.validate(work)?;
                }
                let state = self.validated_operation(id, work)?;
                if state.phase == OperationPhase::Terminal {
                    if state.outcome.as_ref() == Some(outcome)
                        && execution_duration_ns.is_none()
                        && fence.is_none()
                    {
                        return Ok(());
                    }
                    return Err(diagnostic(
                        work,
                        Conflict,
                        "terminal operation has another outcome",
                    ));
                }
                if let Outcome::Indeterminate { operation_id } = outcome
                    && *operation_id != id
                {
                    return Err(diagnostic(
                        work,
                        Invalid,
                        "indeterminate outcome references another operation",
                    ));
                }
                if !matches!(
                    state.phase,
                    OperationPhase::Running | OperationPhase::Reconciling
                ) && !(state.cancellation_requested && matches!(outcome, Outcome::Cancelled))
                {
                    return Err(diagnostic(
                        work,
                        Conflict,
                        "operation cannot complete in its current phase",
                    ));
                }
                if state.reservation.is_some() {
                    require_fence(
                        state,
                        fence.as_ref().ok_or_else(|| {
                            diagnostic(
                                work,
                                Unauthorized,
                                "worker completion requires a lease fence",
                            )
                        })?,
                        work,
                    )?;
                } else if fence.is_some() {
                    return Err(diagnostic(work, Conflict, "completion fence is not active"));
                }
            }
            SchedulerEvent::Orchestrated {
                expected_revision,
                outcome,
                cancel,
                reducer,
                reduction_digest,
                ..
            } => {
                if let Outcome::Succeeded(file) = outcome {
                    file.validate(work)?;
                }
                let parent = self.validated_operation(id, work)?;
                if parent.revision != *expected_revision
                    || parent.phase != OperationPhase::WaitingForChildren
                    || parent.cancellation_requested
                {
                    return Err(diagnostic(work, Conflict, "stale orchestration decision"));
                }
                if !self.matches_orchestration(
                    id,
                    outcome,
                    cancel,
                    reducer.as_ref(),
                    *reduction_digest,
                    work,
                )? {
                    return Err(diagnostic(
                        work,
                        Conflict,
                        "orchestration decision no longer matches",
                    ));
                }
            }
        }
        Ok(())
    }
}

fn same_lease(left: &Reservation, right: &Reservation) -> bool {
    left.id == right.id && left.placement == right.placement
}
fn covers(
    held: &BTreeMap<String, u64>,
    request: &ResourceRequest,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<bool> {
    for (resource, amount) in &request.0 {
        observe(work)?;
        if held.get(resource).unwrap_or(&0) < amount {
            return Ok(false);
        }
    }
    Ok(true)
}
fn require_phase(
    state: &OperationState,
    expected: OperationPhase,
    work: &Option<&mut dyn HistoryIo>,
) -> Result<()> {
    if state.phase == expected {
        Ok(())
    } else {
        Err(diagnostic(
            work,
            Conflict,
            "operation phase differs from the required phase",
        ))
    }
}
pub(crate) fn require_execution_owner(
    state: &OperationState,
    fence: &LeaseFence,
    settlement: bool,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    if let Some(io) = work.as_deref_mut() {
        io.check_cancellation()?;
    }
    if settlement {
        if !matches!(
            state.phase,
            OperationPhase::Running | OperationPhase::Reconciling
        ) {
            return Err(diagnostic(work, Conflict, "task has no execution owner"));
        }
    } else {
        require_phase(state, OperationPhase::Running, work)?;
    }
    require_fence(state, fence, work)?;
    if state.cancellation_requested && !settlement {
        return Err(diagnostic(
            work,
            Conflict,
            "cancelled task cannot publish new execution work",
        ));
    }
    Ok(())
}
fn require_fence(
    state: &OperationState,
    fence: &LeaseFence,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    let reservation = state
        .reservation
        .as_ref()
        .ok_or_else(|| diagnostic(work, Conflict, "operation has no active lease"))?;
    observe(work)?;
    observe(work)?;
    if reservation.id != fence.reservation_id || reservation.placement != fence.placement {
        return Err(diagnostic(
            work,
            Unauthorized,
            "stale or foreign lease fence",
        ));
    }
    Ok(())
}
