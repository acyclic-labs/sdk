use super::*;

/// Pull worker capacity and placement identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(
    feature = "wasm",
    tsify(large_number_types_as_bigints, hashmap_as_object)
)]
pub struct Worker {
    /// Stable worker identity.
    pub id: String,
    /// Currently available logical capacity.
    pub available: ResourceRequest,
    /// Labels available for exact-match operation placement.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

/// Work atomically claimed from the coordinator.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
pub struct WorkLease {
    /// Complete immutable operation declaration.
    pub operation: OperationSpec,
    /// Pinned execution allocation.
    pub reservation: Reservation,
    /// Latest durable resumable checkpoint, if any.
    #[cfg_attr(
        feature = "wasm",
        tsify(type = "(WasmResourceRefWire & { kind: 'checkpoint' }) | null")
    )]
    pub checkpoint: Option<crate::resources::CheckpointRef>,
    /// Coordinator-observed revision associated with this reservation.
    pub operation_revision: u64,
}

/// Owner-scoped pull result.
pub enum WorkPull {
    /// No operation was admitted by this invocation.
    Idle,
    /// Coordinator publication was observed; execution must still verify it.
    Claimed(WorkLease),
    /// Publication or its observation failed.
    Unresolved {
        /// Exact attempted lease, including the reservation identity.
        lease: WorkLease,
        /// Publication/observation failure.
        error: Error,
    },
}

impl<P: StreamProvider> DistributedCoordinator<P> {
    /// Pulls and atomically admits one dependency- and resource-ready operation.
    pub async fn pull(&mut self, worker: &Worker) -> Result<Option<WorkLease>> {
        match self.pull_for(worker, None, None, &mut None).await? {
            WorkPull::Idle => Ok(None),
            WorkPull::Claimed(lease) => Ok(Some(lease)),
            WorkPull::Unresolved { error, .. } => Err(error),
        }
    }

    pub(crate) async fn pull_owned(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        worker: &Worker,
        operation_id: Option<OperationId>, work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<WorkPull> {
        verifier.verify_audience(owner, work)?;
        verifier.verify_scope_for_with_work(owner, scope, work)?;
        if !scope.capabilities().contains("operation:observe")
            || !scope.capabilities().contains("operation:declare")
        {
            return Err(coordinator_boundary(work, wire::ErrorCode::Unauthorized,
                "owned pull requires operation:declare and operation:observe"));
        }
        self.pull_for(worker, Some((owner, verifier)), operation_id, work).await
    }

    fn verify_pull_owner(
        &self, authorization: Option<(&Authority, &AuthorityVerifier)>, operation_id: OperationId,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        if let Some((owner, verifier)) = authorization {
            self.scheduler
                .operation(operation_id)
                .ok_or_else(|| coordinator_boundary(work, wire::ErrorCode::NotFound,
                "owned pull operation is absent"))?
                .spec
                .verify_owner(owner, verifier, work)?;
        }
        Ok(())
    }

    async fn pull_for(
        &mut self,
        worker: &Worker,
        authorization: Option<(&Authority, &AuthorityVerifier)>,
        target: Option<OperationId>, work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<WorkPull> {
        let owner = authorization.map(|(owner, _)| owner);
        self.refresh(work).await?;
        if worker.id.trim().is_empty() {
            return Err(coordinator_boundary(work, wire::ErrorCode::Invalid, "worker identity is empty"));
        }
        if let Some(target) = target {
            let state = self
                .scheduler
                .operation(target)
                .filter(|state| owner.is_some_and(|owner| state.spec.owner.authority() == owner))
                .ok_or_else(|| coordinator_boundary(work, wire::ErrorCode::NotFound,
                "owned pull target is absent"))?;
            self.verify_pull_owner(authorization, target, work)?;
            if state.reservation.is_some() || state.cancellation_requested {
                return Ok(WorkPull::Idle);
            }
        }
        let blocked = match target {
            Some(target) => self
                .scheduler
                .operation_blocked_by_dependencies(target, owner)
                .then_some(target),
            None => self.scheduler.blocked_by_dependencies(owner).next(),
        };
        if let Some(operation_id) = blocked {
            self.verify_pull_owner(authorization, operation_id, work)?;
            let mut custody = CodecQuota::new(work, 0)?;
            let key = formatted_text(
                "dependency-rejected:".len() + 36,
                format_args!("dependency-rejected:{operation_id}"), custody.work(),
            )?;
            if custody.work().is_some() { custody.adopt(key.capacity() as u64)?; }
            validate_idempotency_key(&key, custody.work())?;
            let key = IdempotencyKey(key);
            let reason = crate::core::copy_text("a required dependency did not succeed", &mut custody)?;
            self.apply( key, SchedulerEvent::Rejected { operation_id, reason }, custody.work(),
            ).await?;
            return Ok(WorkPull::Idle);
        }
        let mut custody = CodecQuota::new(work, 0)?;
        let available = self.scheduler.available_for(&worker.id, &worker.available, custody.work())?;
        let available_bytes = if custody.work().is_some() { available.logical_owned_bytes()? } else { 0 };
        custody.adopt(available_bytes)?;
        let selected = match target {
            Some(target) => self.scheduler.operation(target)
                .filter(|operation| self.scheduler.is_ready(operation, &available, &worker.labels, owner))
                .map(|_| target),
            None => self
                .scheduler
                .ready(&available, &worker.labels, owner).next(),
        };
        drop(available);
        custody.relinquish(available_bytes)?;
        let Some(operation_id) = selected else {
            return Ok(WorkPull::Idle);
        };
        self.verify_pull_owner(authorization, operation_id, custody.work())?;
        let state = self
            .scheduler
            .operation(operation_id)
            .ok_or_else(|| coordinator_boundary(custody.work(), wire::ErrorCode::NotFound,
                "selected owned operation is absent"))?;
        let operation_revision = state.revision.checked_add(1)
            .ok_or_else(|| coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
                "operation revision exhausted"))?;
        let operation = state.spec.clone_with_work(custody.work())?;
        if custody.work().is_some() { custody.adopt(operation.logical_owned_bytes()?)?; }
        let checkpoint = state.checkpoint.as_ref()
            .map(|checkpoint| checkpoint.clone_with_work(custody.work())).transpose()?;
        if custody.work().is_some()
            && let Some(checkpoint) = &checkpoint
        {
            custody.adopt(checkpoint.logical_owned_bytes()?)?;
        }
        let revision = next_revision(self.revision)?;
        let revision_digits = revision.checked_ilog10().unwrap_or(0) as usize + 1;
        let reservation_id = formatted_text(
            worker.id.len() + 1 + 36 + 1 + revision_digits,
            format_args!("{}:{operation_id}:{revision}", worker.id), custody.work(),
        )?;
        if custody.work().is_some() { custody.adopt(reservation_id.capacity() as u64)?; }
        let placement = crate::core::copy_text(&worker.id, &mut custody)?;
        let admitted = operation.resources.clone_with_work(custody.work())?;
        if custody.work().is_some() { custody.adopt(admitted.logical_owned_bytes()?)?; }
        let mut lease = WorkLease {
            operation,
            reservation: Reservation { id: reservation_id, placement, admitted },
            checkpoint,
            operation_revision,
        };
        let appended = {
            let mut attempt = CodecQuota::new(custody.work(), 0)?;
            let reservation = lease.reservation.clone_with_work(attempt.work())?;
            if attempt.work().is_some() { attempt.adopt(reservation.logical_owned_bytes()?)?; }
            let key = formatted_text(
                "pull:".len() + worker.id.len() + 1 + 36 + 1 + revision_digits,
                format_args!("pull:{}:{operation_id}:{revision}", worker.id), attempt.work(),
            )?;
            if attempt.work().is_some() { attempt.adopt(key.capacity() as u64)?; }
            validate_idempotency_key(&key, attempt.work())?;
            self.apply( IdempotencyKey(key),
                SchedulerEvent::Admitted { operation_id, reservation }, attempt.work(),
            ).await
        };
        if let Err(error) = appended {
            if custody.work().is_some() && error.is_native_failure() { return Err(error); }
            custody.transfer();
            return Ok(WorkPull::Unresolved { lease, error });
        }
        lease.operation_revision = self
            .scheduler
            .operation(operation_id)
            .map_or(0, |value| value.revision);
        custody.transfer();
        Ok(WorkPull::Claimed(lease))
    }

    /// Returns a crashed worker's exact lease to admission without losing its checkpoint.
    pub async fn release_lease(
        &mut self,
        lease: &WorkLease,
        idempotency_key: IdempotencyKey,
    ) -> Result<bool> {
        self.apply(idempotency_key, SchedulerEvent::LeaseReleased {
            operation_id: lease.operation.operation_id,
            fence: LeaseFence::from(&lease.reservation),
        }, &mut None).await
    }
}
