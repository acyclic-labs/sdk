//! Existing task provider implementations.
use super::*;

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl<P: StreamProvider> DurableTaskHost for CoordinatorTaskHost<P> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<bool> {
        work.remaining()?;
        Ok(true)
    }

    async fn claim_model_dispatch(
        &self,
        task: TaskId,
        attempt: OperationId,
        step: u32,
        digest: [u8; 32],
        fence: crate::scheduler::LeaseFence,
    ) -> Result<()> {
        self.claim_model(task, attempt, step, digest, &fence, &mut None)
            .await
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn claim_model_dispatch_with_native_work<'a>(
        &'a self,
        task: TaskId,
        attempt: OperationId,
        step: u32,
        digest: [u8; 32],
        fence: &'a crate::scheduler::LeaseFence,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || async move {
            let mut io = crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(work)?,
            );
            self.claim_model(task, attempt, step, digest, fence, &mut Some(&mut io))
                .await
        })
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

    async fn admit_owned(
        &self,
        admission: TaskAdmissionRecord,
        fence: crate::scheduler::LeaseFence,
    ) -> Result<Admission<TaskId>> {
        let parent = admission
            .parent
            .ok_or_else(|| Error::Invalid("owned child admission has no parent".into()))?;
        self.verify_owner(parent, &fence, false, &mut None).await?;
        self.admit_record(admission, Some(fence)).await
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

    async fn load_batch(&self, batch_id: BatchId) -> Result<Option<DurableBatchRequest>> {
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

    async fn admit_batch(&self, request: DurableBatchRequest) -> Result<Vec<Admission<TaskId>>> {
        self.validate_batch(&request)?;
        self.validate_parent_scope(request.parent, &request.scope, &mut None)
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

    async fn cancel_batch(&self, batch_id: BatchId) -> Result<Option<BatchCancellationReport>> {
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
                Admission::Rejected { reason } => BatchCancellationStatus::NotAdmitted { reason },
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

    async fn admit(&self, admission: TaskAdmissionRecord) -> Result<Admission<TaskId>> {
        self.admit_record(admission, None).await
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
                self.verify_owner(
                    task,
                    fence,
                    false,
                    &mut history
                        .as_mut()
                        .map(|history| history as &mut dyn crate::store::HistoryIo),
                )
                .await
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
                self.verify_owner(
                    task,
                    fence,
                    true,
                    &mut history
                        .as_mut()
                        .map(|history| history as &mut dyn crate::store::HistoryIo),
                )
                .await
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
        work.remaining()?;
        if let Some(policy) = &self.policy {
            let (identity, custody) = policy.identity_with_native_work(work)?;
            let needed = (std::mem::size_of::<ComponentIdentity>() as u64)
                .checked_add(identity.name.capacity() as u64)
                .and_then(|bytes| bytes.checked_add(identity.version.capacity() as u64))
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "native policy identity custody overflows",
                })?;
            if !custody.work().shares_ledger(work) || custody.bytes() < needed {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "policy identity differs from its original native metadata custody",
                });
            }
            if self.policy_binding.as_ref() != Some(&identity) {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Conflict,
                    reason: "durable policy implementation changed after native binding",
                });
            }
        }
        Ok(self.policy_binding.as_ref())
    }

    async fn observe_admission(&self, task_id: TaskId) -> Result<TaskAdmissionRecord> {
        #[cfg(feature = "test-support")]
        crate::test_support::admission_work::observed();
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
        #[cfg(feature = "filesystem")]
        let scope = self.authenticated_scope(&admission, &mut None).await?.0;
        #[cfg(not(feature = "filesystem"))]
        let scope = {
            if !admission.resource_proofs.is_empty() {
                return Err(Error::Unsupported(
                    "created resource scope requires its filesystem provider".into(),
                ));
            }
            self.root_scope
                .narrow(admission.grants.clone(), admission.limits)?
                .with_run_limits(admission.run_limits, &mut None)?
        };
        scope.with_replayed_extensions(admission.extensions)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn resume_scope_with_native_work<'a>(
        &'a self,
        task: TaskId,
        operation: OperationId,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (RuntimeScope, acyclic_fs::NativeAllocation)>> {
        if task.into_bytes() != operation.into_bytes() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "task identity does not match its admission",
            });
        }
        crate::NativeFutureBundle::new(work, || async move {
            let io = crate::executor::PreparationIo::from_native_work(work)?;
            let admission = self
                .admission(operation, &mut Some(&mut io.history_io()))
                .await?;
            let admission_custody = io.take_native_custody(
                std::mem::size_of::<TaskAdmissionRecord>() as u64
                    + admission.logical_owned_bytes()?,
            )?;
            let (scope, _) = self
                .authenticated_scope(&admission, &mut Some(&mut io.history_io()))
                .await?;
            drop(admission);
            drop(admission_custody);
            let retained = io.owned_bytes()?;
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
        let admission = self
            .admission(OperationId::from_bytes(sender.into_bytes()), &mut None)
            .await?;
        let bytes = self
            .prepare_mail((sender, &admission), recipient, message_id, payload, None)
            .await?
            .0;
        self.publish_mail(
            (sender, &admission),
            recipient,
            message_id,
            bytes,
            None,
            None,
        )
        .await
    }

    async fn inbox(&self, task_id: TaskId, after: u64, limit: usize) -> Result<Vec<InboxItem>> {
        self.inbox_page(task_id, after, limit, None).await
    }
}
