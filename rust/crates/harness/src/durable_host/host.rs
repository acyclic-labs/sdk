//! Existing task provider implementations.
use super::*;

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl<P: StreamProvider> DurableTaskHost for CoordinatorTaskHost<P> {
    async fn claim_model_dispatch(
        &self,
        task_id: TaskId,
        attempt_id: OperationId,
        step: u32,
        request_digest: [u8; 32],
        fence: crate::scheduler::LeaseFence,
    ) -> Result<()> {
        let operation_id = OperationId::from_bytes(task_id.into_bytes());
        let admission = self.admission(operation_id, &mut None).await?;
        if !admission.grants.contains("model:generate") {
            return Err(Error::Unauthorized(
                "task scope lacks model:generate".into(),
            ));
        }
        let scope = RuntimeScope::new(admission.grants, admission.limits)?
            .with_run_limits(admission.run_limits, &mut None)?;
        self.validate_parent_scope(admission.parent, &scope).await?;
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(&mut None).await?;
        let operation = coordinator.observe_operation(
            &self.owner,
            &self.owner_scope,
            &self.verifier,
            operation_id,
            &mut None,
        )?;
        crate::scheduler::require_execution_owner(operation, &fence, false, &mut None)?;
        let session = coordinator
            .scheduler()
            .session_limits(operation_id, &mut None)?;
        let ceiling = (scope.limits().model_steps as u64)
            .min(
                scope
                    .run_limits()
                    .max_steps
                    .map_or(u64::MAX, |value| value as u64),
            )
            .min(session.model_steps);
        let lease_digest = blake3::hash(&crate::contract::canonical_json_bytes(&fence)?);
        coordinator
            .apply(
                IdempotencyKey::new(format!(
                    "model-dispatch:{attempt_id}:{step}:{lease_digest}"
                ))?,
                crate::scheduler::SchedulerEvent::ModelDispatchClaimed {
                    operation_id,
                    attempt_id,
                    step,
                    request_digest,
                    fence,
                    ceiling,
                },
                &mut None,
            )
            .await?;
        Ok(())
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl<P: StreamProvider> crate::runtime::TaskSpawner for CoordinatorTaskHost<P> {
    fn policy_identity(&self) -> Option<ComponentIdentity> {
        TaskStateProvider::policy_identity(self)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn policy_identity_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<&ComponentIdentity>> {
        TaskStateProvider::policy_identity_with_native_work(self, work)
    }

    async fn reconcile_admission(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<(TaskId, TaskAdmissionRecord)>> {
        match self.admission(operation_id, &mut None).await {
            Ok(admission) => Ok(Some((
                TaskId::from_bytes(operation_id.into_bytes()),
                admission,
            ))),
            Err(Error::NotFound(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn reconcile_batch(
        &self,
        request: DurableBatchRequest,
    ) -> Result<Option<Vec<Admission<TaskId>>>> {
        self.validate_batch(&request)?;
        let Some(retained) = self.retained_batch(request.batch_id).await? else {
            return Ok(None);
        };
        if retained != request.canonical_value() {
            return Err(Error::Conflict(
                "batch identity belongs to another request".into(),
            ));
        }
        let cancelled = self.retained_batch_cancellation(request.batch_id).await?;
        if cancelled
            .as_ref()
            .is_some_and(|declaration| declaration.group_id != request.group_id)
        {
            return Err(Error::Conflict(
                "batch cancellation belongs to another group".into(),
            ));
        }
        let mut entries = Vec::with_capacity(request.inputs.len());
        for index in 0..request.inputs.len() {
            let operation_id = request.operation_id(index);
            let expected = request.member_admission(index)?;
            match self.admission(operation_id, &mut None).await {
                Ok(admission) if admission == expected => entries.push(Admission::Accepted(
                    TaskId::from_bytes(operation_id.into_bytes()),
                )),
                Ok(_) => {
                    return Err(Error::Conflict(
                        "batch member belongs to another admission".into(),
                    ));
                }
                Err(Error::NotFound(_)) if cancelled.is_some() => {
                    entries.push(Admission::Rejected {
                        reason: "batch cancellation preceded member admission".into(),
                    });
                }
                Err(Error::NotFound(_)) => {
                    entries.push(Admission::Indeterminate { operation_id });
                }
                Err(error) => return Err(error),
            }
        }
        Ok(Some(entries))
    }

    async fn load_batch(
        &self,
        batch_id: BatchId,
    ) -> Result<Option<DurableBatchRequest>> {
        let Some(value) = self.retained_batch(batch_id).await? else {
            return Ok(None);
        };
        let request = DurableBatchRequest::from_canonical_value(value)?;
        if request.batch_id != batch_id {
            return Err(Error::Conflict(
                "batch manifest has another identity".into(),
            ));
        }
        self.validate_batch(&request)?;
        Ok(Some(request))
    }

    async fn admit_batch(
        &self,
        request: DurableBatchRequest,
    ) -> Result<Vec<Admission<TaskId>>> {
        self.validate_batch(&request)?;
        self.validate_parent_scope(request.parent, &request.scope)
            .await?;
        self.commit_batch(&request).await?;
        let observed = self
            .reconcile_batch(request.clone())
            .await?
            .ok_or_else(|| Error::Storage("committed batch disappeared".into()))?;
        let mut entries = Vec::with_capacity(observed.len());
        for (index, admission) in observed.into_iter().enumerate() {
            match admission {
                Admission::Indeterminate { operation_id } => {
                    let member = request.member_admission(index)?;
                    if member.operation_id != operation_id {
                        return Err(Error::Conflict("batch member operation changed".into()));
                    }
                    if self
                        .retained_batch_cancellation(request.batch_id)
                        .await?
                        .is_some()
                    {
                        entries.push(Admission::Rejected {
                            reason: "batch cancellation preceded member admission".into(),
                        });
                        continue;
                    }
                    let admitted = self.admit(member).await?;
                    if self
                        .retained_batch_cancellation(request.batch_id)
                        .await?
                        .is_some()
                        && let Admission::Accepted(task_id) = &admitted
                    {
                        self.cancel(*task_id).await?;
                        entries.push(admitted);
                        continue;
                    }
                    entries.push(admitted);
                }
                other => entries.push(other),
            }
        }
        Ok(entries)
    }

    async fn cancel_batch(
        &self,
        batch_id: BatchId,
    ) -> Result<Option<BatchCancellationReport>> {
        let Some(request) = self.load_batch(batch_id).await? else {
            return Ok(None);
        };
        self.declare_batch_cancellation(&request).await?;
        let admissions = self
            .reconcile_batch(request.clone())
            .await?
            .ok_or_else(|| {
                Error::Storage("retained batch disappeared during cancellation".into())
            })?;
        let mut entries = Vec::with_capacity(admissions.len());
        for (index, admission) in admissions.into_iter().enumerate() {
            let status = match admission {
                Admission::Accepted(task_id) => match self.cancel(task_id).await {
                    Ok(()) => BatchCancellationStatus::Requested,
                    Err(Error::Indeterminate(operation_id)) => {
                        BatchCancellationStatus::Indeterminate { operation_id }
                    }
                    Err(error) => BatchCancellationStatus::Unresolved {
                        message: error.to_string(),
                    },
                },
                Admission::Rejected { reason } => {
                    BatchCancellationStatus::NotAdmitted { reason }
                }
                Admission::Indeterminate { operation_id } => {
                    BatchCancellationStatus::Indeterminate { operation_id }
                }
            };
            entries.push((InputKey { batch_id, index }, status));
        }
        Ok(Some(BatchCancellationReport {
            group_id: request.group_id,
            batch_id,
            entries,
        }))
    }

    async fn admit(
        &self,
        admission: TaskAdmissionRecord,
    ) -> Result<Admission<TaskId>> { self.admit_record(admission, None).await
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl<P: StreamProvider> TaskStateProvider for CoordinatorTaskHost<P> {
    fn verify_dispatch_owner<'a>(
        &'a self,
        task: TaskId,
        fence: &'a crate::scheduler::LeaseFence,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            #[cfg(feature = "filesystem")]
            {
                let mut history = io.map(crate::conversation::ContentIo::history_io);
                self.verify_owner(task, fence, false,
                    &mut history.as_mut().map(|history| history as &mut dyn crate::store::HistoryIo)).await
            }
            #[cfg(not(feature = "filesystem"))]
            self.verify_owner(task, fence, false, &mut None).await
        })
    }
    fn verify_execution_owner<'a>(
        &'a self,
        task: TaskId,
        fence: &'a crate::scheduler::LeaseFence,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            #[cfg(feature = "filesystem")]
            {
                let mut history = io.map(crate::conversation::ContentIo::history_io);
                self.verify_owner(task, fence, true,
                    &mut history.as_mut().map(|history| history as &mut dyn crate::store::HistoryIo)).await
            }
            #[cfg(not(feature = "filesystem"))]
            self.verify_owner(task, fence, true, &mut None).await
        })
    }

    async fn children(
        &self,
        parent: TaskId,
        expected_revision: Option<u64>,
        after_slot: Option<String>,
        maximum: usize,
    ) -> Result<TaskChildrenPage> {
        let (page, _) = self
            .coordinator
            .lock()
            .await
            .observe_children(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                OperationId::from_bytes(parent.into_bytes()),
                expected_revision,
                after_slot.as_deref(),
                maximum,
                &mut None,
            )
            .await?;
        Ok(page)
    }

    fn policy_identity(&self) -> Option<ComponentIdentity> {
        self.policy.as_ref().map(|policy| policy.identity())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn policy_identity_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<&ComponentIdentity>> {
        work.charge(acyclic_fs::WorkCounters::default())?;
        if self.policy.is_some() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "task state has no original native policy binding source",
            });
        }
        Ok(None)
    }

    async fn observe_admission(
        &self,
        task_id: TaskId,
    ) -> Result<TaskAdmissionRecord> {
        let operation_id = OperationId::from_bytes(task_id.into_bytes());
        self.admission(operation_id, &mut None).await
    }

    async fn resume_scope(
        &self,
        task_id: TaskId,
        operation_id: OperationId,
    ) -> Result<RuntimeScope> {
        if task_id.into_bytes() != operation_id.into_bytes() {
            return Err(Error::Unauthorized(
                "task identity does not match its admission".into(),
            ));
        }
        let admission = self.admission(operation_id, &mut None).await?;
        self.root_scope
            .narrow(admission.grants, admission.limits)?
            .with_run_limits(admission.run_limits, &mut None)?
            .with_replayed_extensions(admission.extensions)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn resume_scope_with_native_work<'a>(
        &'a self,
        task: TaskId,
        operation: OperationId,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (RuntimeScope, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            if task.into_bytes() != operation.into_bytes() {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "task identity does not match its admission",
                });
            }
            let io = crate::executor::PreparationIo::from_native_work(work)?;
            let admission = self
                .admission(operation, &mut Some(&mut io.history_io()))
                .await?;
            let (scope, retained) = self
                .authenticated_scope(&admission, &mut Some(&mut io.history_io()))
                .await?;
            drop(admission);
            Ok((scope, io.into_native_custody(retained)?))
        })
    }

    async fn outcome(&self, task_id: TaskId) -> Result<Option<Outcome<Value>>> {
        self.task_outcome(task_id, None).await
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn observe_admission_with_native_work<'a>(
        &'a self,
        task: TaskId,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (TaskAdmissionRecord, acyclic_fs::NativeAllocation)>>
    {
        crate::NativeFutureBundle::new(work, || async move {
            let io = crate::executor::PreparationIo::from_native_work(work)?;
            let record = self
                .admission(
                    OperationId::from_bytes(task.into_bytes()),
                    &mut Some(&mut io.history_io()),
                )
                .await?;
            let retained =
                std::mem::size_of::<TaskAdmissionRecord>() as u64 + record.logical_owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            Ok((record, custody))
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn outcome_with_native_work<'a>(
        &'a self,
        task: TaskId,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<Outcome<Value>>, acyclic_fs::NativeAllocation)>>
    {
        crate::NativeFutureBundle::new(work, || async move {
            let io = crate::executor::PreparationIo::from_native_work(work)?;
            let value = self.task_outcome(task, Some(&io)).await?;
            let retained = io.owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            Ok((value, custody))
        })
    }

    async fn cancel(&self, task_id: TaskId) -> Result<()> {
        let operation_id = OperationId::from_bytes(task_id.into_bytes());
        self.coordinator
            .lock()
            .await
            .cancel_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                operation_id,
                IdempotencyKey::new(format!("task-cancel:{operation_id}"))?,
                true,
                None,
                &mut None,
            )
            .await?;
        Ok(())
    }

    async fn send(
        &self,
        sender: TaskId,
        recipient: TaskId,
        message_id: OperationId,
        payload: FileRef,
    ) -> Result<()> {
        let bytes = self.prepare_mail(sender, recipient, message_id, payload, None).await?.0;
        self.publish_mail(sender, recipient, message_id, bytes, None, None).await
    }

    async fn inbox(
        &self,
        task_id: TaskId,
        after: u64,
        limit: usize,
    ) -> Result<Vec<InboxItem>> { self.inbox_page(task_id, after, limit, None).await
    }

    async fn wait_until(
        &self,
        task_id: TaskId,
        operation_id: OperationId,
        deadline_unix_ms: u64,
    ) -> Result<()> {
        if deadline_unix_ms == 0 {
            return Err(Error::Invalid("timer deadline is invalid".into()));
        }
        let admission = self
            .admission(OperationId::from_bytes(task_id.into_bytes()), &mut None)
            .await?;
        if !admission.grants.contains(capability::TIMER_WAIT) {
            return Err(Error::Unauthorized("task scope lacks timer:wait".into()));
        }
        let event = TimerEvent {
            task_id,
            operation_id,
            deadline_unix_ms,
        };
        let bytes = crate::contract::canonical_json_bytes(&event)?;
        let stream = self.timer_stream(task_id)?;
        loop {
            let (tail, found) = self
                .timer_state(task_id, operation_id, deadline_unix_ms, &mut None)
                .await?;
            if found {
                break;
            }
            if self
                .publish_control_at(
                    &stream,
                    "timers",
                    task_id,
                    operation_id,
                    &bytes,
                    Some(tail),
                )
                .await?
            {
                break;
            }
        }
        loop {
            let now = self.clock.now_unix_millis();
            if now >= deadline_unix_ms {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(
                deadline_unix_ms.saturating_sub(now).min(60_000),
            ))
            .await;
        }
    }
}
