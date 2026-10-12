use super::*;
use crate::contract::COMPONENT_LABEL_MAX_BYTES;

impl Scheduler {
    /// Plans a declaration with slot, dependency and session checks.
    pub fn declare(&self, spec: OperationSpec) -> Result<SchedulerEvent> {
        self.validate_declaration(&spec, &mut None)?;
        Ok(SchedulerEvent::Declared {
            spec: Box::new(spec),
        })
    }
    pub(crate) fn validate_session(
        &self,
        spec: &OperationSpec,
        limits: &SessionLimits,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        self.validate_declaration(spec, work)?;
        limits.validate(work)?;
        if spec.parent.is_some() || self.children(spec.operation_id).next().is_some() {
            return Err(diagnostic(
                work,
                Conflict,
                "session must be configured before descendants or dispatch",
            ));
        }
        self.require_session_policy(spec.operation_id, limits, work)
    }
    pub(super) fn require_session_policy(
        &self,
        id: OperationId,
        limits: &SessionLimits,
        work: &Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        if self
            .sessions
            .get(&id)
            .is_some_and(|existing| existing != limits)
        {
            return Err(diagnostic(
                work,
                Conflict,
                "session limits are already pinned",
            ));
        }
        Ok(())
    }
    pub(super) fn validate_declaration(
        &self,
        spec: &OperationSpec,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        observe(work)?;
        observe(work)?;
        if spec.entrypoint.name.trim().is_empty() || spec.entrypoint.version.trim().is_empty() {
            return Err(diagnostic(
                work,
                Invalid,
                "durable entrypoint name and version are required",
            ));
        }
        spec.state.validate(work)?;
        for (key, value) in &spec.placement {
            observe(work)?;
            observe(work)?;
            if key.is_empty()
                || key.len() > COMPONENT_LABEL_MAX_BYTES
                || value.len() > COMPONENT_LABEL_MAX_BYTES
                || key.chars().any(char::is_control)
                || value.chars().any(char::is_control)
            {
                return Err(diagnostic(work, Invalid, "placement labels are invalid"));
            }
        }
        crate::executor::compile_json_schema_with_work(
            &spec.entrypoint.result_schema,
            "entrypoint result",
            work,
        )?;
        if self.operations.contains_key(&spec.operation_id) {
            return Err(diagnostic(
                work,
                Conflict,
                "operation identity already exists",
            ));
        }
        for dependency in &spec.dependencies {
            if self.depends_on(*dependency, spec.operation_id, work)? {
                return Err(diagnostic(work, Conflict, "operation dependency cycle"));
            }
        }
        if let Some(parent) = &spec.parent {
            observe(work)?;
            if parent.slot.trim().is_empty()
                || parent.slot.len() > COMPONENT_LABEL_MAX_BYTES
                || parent.slot.chars().any(char::is_control)
            {
                return Err(diagnostic(
                    work,
                    Invalid,
                    "structured child slot is invalid",
                ));
            }
            let state = self.operation(parent.operation_id).ok_or_else(|| {
                diagnostic(
                    work,
                    Invalid,
                    "structured child requires an existing parent and slot",
                )
            })?;
            for ((id, slot), _) in self
                .child_slots
                .range((parent.operation_id, String::new())..)
            {
                observe(work)?;
                if *id != parent.operation_id || slot > &parent.slot {
                    break;
                }
                if slot == &parent.slot {
                    return Err(diagnostic(
                        work,
                        Conflict,
                        "parent child slot is already bound",
                    ));
                }
            }
            if state.phase == OperationPhase::Terminal || state.cancellation_requested {
                return Err(diagnostic(
                    work,
                    Conflict,
                    "terminal parent cannot accept children",
                ));
            }
            let (root, depth) = self.session_root(parent.operation_id, work)?;
            if let Some(limits) = self.sessions.get(&root) {
                let mut count = 0u64;
                for id in self.operations.keys() {
                    if self.session_root(*id, work)?.0 == root {
                        count += 1;
                    }
                }
                if depth >= limits.depth || count >= limits.total_tasks {
                    return Err(diagnostic(
                        work,
                        Invalid,
                        "session task or depth limit exhausted",
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn is_ready(
        &self,
        operation: &OperationState,
        capacity: &ResourceRequest,
        labels: &BTreeMap<String, String>,
        owner: Option<&Authority>,
    ) -> bool {
        owner.is_none_or(|owner| operation.spec.owner.authority() == owner)
            && matches!(
                operation.phase,
                OperationPhase::WaitingForDependencies | OperationPhase::WaitingForCapacity
            )
            && !operation.cancellation_requested
            && self.dependencies_succeeded(operation)
            && operation
                .spec
                .placement
                .iter()
                .all(|(key, value)| labels.get(key) == Some(value))
            && operation
                .spec
                .resources
                .0
                .iter()
                .all(|(resource, requested)| {
                    let held = operation
                        .reservation
                        .as_ref()
                        .and_then(|reservation| reservation.admitted.0.get(resource))
                        .copied()
                        .unwrap_or(0);
                    requested.saturating_sub(held) <= capacity.0.get(resource).copied().unwrap_or(0)
                })
            && self
                .require_session_capacity(operation.spec.operation_id, &mut None)
                .is_ok()
    }
    /// Iterates operations matching placement, ownership and available capacity.
    pub fn ready<'a>(
        &'a self,
        capacity: &'a ResourceRequest,
        labels: &'a BTreeMap<String, String>,
        owner: Option<&'a Authority>,
    ) -> impl Iterator<Item = OperationId> + 'a {
        self.operations
            .values()
            .filter(move |state| self.is_ready(state, capacity, labels, owner))
            .map(|state| state.spec.operation_id)
    }
    /// Subtracts every held reservation from one placement's advertised capacity.
    pub fn available_for(
        &self,
        placement: &str,
        advertised: &ResourceRequest,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<ResourceRequest> {
        for operation in self.operations.values() {
            observe(work)?;
            if let Some(reservation) = &operation.reservation {
                observe(work)?;
                if reservation.placement == placement {
                    for _ in reservation.admitted.0.keys() {
                        observe(work)?;
                    }
                }
            }
        }
        let mut available = advertised.clone_with_work(work)?;
        for reservation in self
            .operations
            .values()
            .filter_map(|state| state.reservation.as_ref())
            .filter(|reservation| reservation.placement == placement)
        {
            for (resource, amount) in &reservation.admitted.0 {
                if let Some(remaining) = available.0.get_mut(resource) {
                    *remaining = remaining.saturating_sub(*amount);
                }
            }
        }
        Ok(available)
    }
    fn dependency_blocked(&self, operation: &OperationState, owner: Option<&Authority>) -> bool {
        owner.is_none_or(|owner| operation.spec.owner.authority() == owner)
            && matches!(
                operation.phase,
                OperationPhase::WaitingForDependencies | OperationPhase::WaitingForCapacity
            )
            && operation.spec.dependencies.iter().any(|id| {
                self.operation(*id).is_some_and(|state| {
                    state.phase == OperationPhase::Terminal
                        && !matches!(state.outcome, Some(Outcome::Succeeded(_)))
                })
            })
    }
    pub(crate) fn operation_blocked_by_dependencies(
        &self,
        id: OperationId,
        owner: Option<&Authority>,
    ) -> bool {
        self.operation(id)
            .is_some_and(|state| self.dependency_blocked(state, owner))
    }
    /// Iterates owner-matched operations with terminal failed dependencies.
    pub fn blocked_by_dependencies<'a>(
        &'a self,
        owner: Option<&'a Authority>,
    ) -> impl Iterator<Item = OperationId> + 'a {
        self.operations
            .values()
            .filter(move |state| self.dependency_blocked(state, owner))
            .map(|state| state.spec.operation_id)
    }
    /// Returns the root-session ceilings for one operation.
    pub fn session_limits(
        &self,
        id: OperationId,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<SessionLimits> {
        let (root, _) = self.session_root(id, work)?;
        observe(work)?;
        self.sessions
            .get(&root)
            .copied()
            .ok_or_else(|| diagnostic(work, Unsupported, "session budget is not configured"))
    }
    #[cfg(feature = "filesystem")]
    pub(crate) fn require_model_claim(
        &self,
        task: OperationId,
        attempt: OperationId,
        step: u32,
        digest: &[u8; 32],
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        let reason = "model dispatch lacks its exact shared budget claim";
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        observe(work)?;
        let claims = self
            .model_claims
            .get(&task)
            .ok_or_else(|| diagnostic(work, Conflict, reason))?;
        observe(work)?;
        let attempt = claims
            .attempts
            .get(&attempt)
            .ok_or_else(|| diagnostic(work, Conflict, reason))?;
        observe(work)?;
        if attempt.get(&step) == Some(digest) {
            Ok(())
        } else {
            Err(diagnostic(work, Conflict, reason))
        }
    }
    pub(crate) fn session_root(
        &self,
        mut id: OperationId,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<(OperationId, u32)> {
        let mut depth = 0u32;
        let mut traversed = 0usize;
        loop {
            observe(work)?;
            let state = self
                .operation(id)
                .ok_or_else(|| diagnostic(work, NotFound, "session task"))?;
            traversed += 1;
            if traversed > self.operations.len() {
                return Err(diagnostic(
                    work,
                    Invalid,
                    "session task lineage contains a cycle",
                ));
            }
            let Some(parent) = &state.spec.parent else {
                return Ok((id, depth));
            };
            id = parent.operation_id;
            depth = depth
                .checked_add(1)
                .ok_or_else(|| diagnostic(work, Invalid, "task depth overflow"))?;
        }
    }
    pub(super) fn require_session_capacity(
        &self,
        id: OperationId,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        let (root, _) = self.session_root(id, work)?;
        let Some(limits) = self.sessions.get(&root) else {
            return Ok(());
        };
        if self
            .operation(id)
            .is_some_and(|state| state.reservation.is_some())
        {
            return Ok(());
        }
        let mut held = 0u64;
        for (candidate, state) in &self.operations {
            observe(work)?;
            if state.reservation.is_some() && self.session_root(*candidate, work)?.0 == root {
                held += 1;
            }
        }
        if held >= limits.active_tasks {
            return Err(diagnostic(
                work,
                Invalid,
                "session active task limit exhausted",
            ));
        }
        Ok(())
    }
    pub(super) fn require_dependencies_ready(
        &self,
        id: OperationId,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        let operation = self.validated_operation(id, work)?;
        for dependency in &operation.spec.dependencies {
            observe(work)?;
            if !matches!(
                self.operation(*dependency)
                    .and_then(|state| state.outcome.as_ref()),
                Some(Outcome::Succeeded(_))
            ) {
                return Err(diagnostic(
                    work,
                    Conflict,
                    "operation dependencies are not ready",
                ));
            }
        }
        Ok(())
    }
    fn dependencies_succeeded(&self, operation: &OperationState) -> bool {
        operation.spec.dependencies.iter().all(|id| {
            matches!(
                self.operation(*id).and_then(|state| state.outcome.as_ref()),
                Some(Outcome::Succeeded(_))
            )
        })
    }
    fn depends_on(
        &self,
        candidate: OperationId,
        target: OperationId,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<bool> {
        observe(work)?;
        if candidate == target {
            return Ok(true);
        }
        if let Some(state) = self.operation(candidate) {
            for dependency in &state.spec.dependencies {
                if self.depends_on(*dependency, target, work)? {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}
