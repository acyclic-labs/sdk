//! Existing task provider implementations.
use super::*;

impl<P: StreamProvider> DurableTaskHost for CoordinatorTaskHost<P> {
    fn verify_dispatch_owner<'a>(
        &'a self,
        task_id: TaskId,
        fence: crate::scheduler::LeaseFence,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.verify_owner(task_id, &fence, false).await })
    }

    fn verify_execution_owner<'a>(
        &'a self,
        task_id: TaskId,
        fence: crate::scheduler::LeaseFence,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.verify_owner(task_id, &fence, true).await })
    }

    fn claim_model_dispatch<'a>(
        &'a self,
        task_id: TaskId,
        attempt_id: OperationId,
        step: u32,
        request_digest: [u8; 32],
        fence: crate::scheduler::LeaseFence,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let operation_id = OperationId::from_bytes(task_id.into_bytes());
            let admission = self.admission(operation_id).await?;
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
            let operation = coordinator
                .observe_operation(
                    &self.owner,
                    &self.owner_scope,
                    &self.verifier,
                    operation_id,
                    &mut None,
                )?
                .clone();
            crate::scheduler::require_execution_owner(&operation, &fence, false, &mut None)?;
            let session = coordinator.scheduler().session_limits(operation_id)?;
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
        })
    }
}

impl<P: StreamProvider> crate::runtime::TaskSpawner for CoordinatorTaskHost<P> {
    fn policy_identity(&self) -> Option<ComponentIdentity> {
        TaskStateProvider::policy_identity(self)
    }

    fn supports_batch_cancellation(&self) -> bool {
        true
    }

    fn reconcile_admission<'a>(
        &'a self,
        operation_id: OperationId,
    ) -> BoxFuture<'a, Result<Option<(TaskId, TaskAdmissionRecord)>>> {
        Box::pin(async move {
            match self.admission(operation_id).await {
                Ok(admission) => Ok(Some((
                    TaskId::from_bytes(operation_id.into_bytes()),
                    admission,
                ))),
                Err(Error::NotFound(_)) => Ok(None),
                Err(error) => Err(error),
            }
        })
    }

    fn reconcile_batch<'a>(
        &'a self,
        request: DurableBatchRequest,
    ) -> BoxFuture<'a, Result<Option<Vec<Admission<TaskId>>>>> {
        Box::pin(async move {
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
                match self.admission(operation_id).await {
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
        })
    }

    fn load_batch<'a>(
        &'a self,
        batch_id: BatchId,
    ) -> BoxFuture<'a, Result<Option<DurableBatchRequest>>> {
        Box::pin(async move {
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
        })
    }

    fn admit_batch<'a>(
        &'a self,
        request: DurableBatchRequest,
    ) -> BoxFuture<'a, Result<Vec<Admission<TaskId>>>> {
        Box::pin(async move {
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
        })
    }

    fn cancel_batch<'a>(
        &'a self,
        batch_id: BatchId,
    ) -> BoxFuture<'a, Result<Option<BatchCancellationReport>>> {
        Box::pin(async move {
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
        })
    }

    fn admit<'a>(
        &'a self,
        admission: TaskAdmissionRecord,
    ) -> BoxFuture<'a, Result<Admission<TaskId>>> {
        Box::pin(async move { self.admit_record(admission, None).await })
    }
}

impl<P: StreamProvider> TaskStateProvider for CoordinatorTaskHost<P> {
    fn children<'a>(
        &'a self,
        parent: TaskId,
        expected_revision: Option<u64>,
        after_slot: Option<String>,
        maximum: usize,
    ) -> BoxFuture<'a, Result<TaskChildrenPage>> {
        Box::pin(async move {
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
        })
    }

    fn policy_identity(&self) -> Option<ComponentIdentity> {
        self.policy.as_ref().map(|policy| policy.identity())
    }

    fn observe_admission<'a>(
        &'a self,
        task_id: TaskId,
    ) -> BoxFuture<'a, Result<TaskAdmissionRecord>> {
        Box::pin(async move {
            let operation_id = OperationId::from_bytes(task_id.into_bytes());
            self.admission(operation_id).await
        })
    }

    fn resume_scope<'a>(
        &'a self,
        task_id: TaskId,
        operation_id: OperationId,
    ) -> BoxFuture<'a, Result<RuntimeScope>> {
        Box::pin(async move {
            if task_id.into_bytes() != operation_id.into_bytes() {
                return Err(Error::Unauthorized(
                    "task identity does not match its admission".into(),
                ));
            }
            let admission = self.admission(operation_id).await?;
            self.root_scope
                .narrow(admission.grants, admission.limits)?
                .with_run_limits(admission.run_limits, &mut None)?
                .with_replayed_extensions(admission.extensions)
        })
    }

    fn outcome<'a>(&'a self, task_id: TaskId) -> BoxFuture<'a, Result<Option<Outcome<Value>>>> {
        Box::pin(async move {
            let operation_id = OperationId::from_bytes(task_id.into_bytes());
            let observed = {
                let mut coordinator = self.coordinator.lock().await;
                coordinator.refresh(&mut None).await?;
                coordinator.observe_operation(
                    &self.owner,
                    &self.owner_scope,
                    &self.verifier,
                    operation_id,
                    &mut None,
                )?
            };
            match observed.outcome {
                Some(Outcome::Succeeded(reference)) => {
                    Ok(Some(Outcome::Succeeded(self.read_json(&reference).await?)))
                }
                Some(Outcome::Failed { message }) => Ok(Some(Outcome::Failed { message })),
                Some(Outcome::Cancelled) => Ok(Some(Outcome::Cancelled)),
                Some(Outcome::Indeterminate { operation_id }) => {
                    Ok(Some(Outcome::Indeterminate { operation_id }))
                }
                None => Ok(None),
            }
        })
    }

    fn cancel<'a>(&'a self, task_id: TaskId) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
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
                )
                .await?;
            Ok(())
        })
    }

    fn send<'a>(
        &'a self,
        sender: TaskId,
        recipient: TaskId,
        message_id: OperationId,
        payload: FileRef,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let bytes = self
                .mail_bytes(sender, recipient, message_id, payload)
                .await?;
            let mailbox = self.mailbox(recipient)?.clone();
            loop {
                let (tail, found) = self.mail_state(recipient, message_id, &bytes).await?;
                if found {
                    return Ok(());
                }
                if self
                    .publish_control_at(&mailbox, "mail", recipient, message_id, &bytes, Some(tail))
                    .await?
                {
                    return Ok(());
                }
            }
        })
    }

    fn inbox<'a>(
        &'a self,
        task_id: TaskId,
        after: u64,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<InboxItem>>> {
        Box::pin(async move {
            if limit == 0 {
                return Err(Error::Invalid("inbox page bound is invalid".into()));
            }
            let recipient_admission = self
                .admission(OperationId::from_bytes(task_id.into_bytes()))
                .await?;
            if !recipient_admission.grants.contains(capability::MAIL_READ) {
                return Err(Error::Unauthorized(
                    "recipient scope lacks mail:read".into(),
                ));
            }
            let mailbox = self.mailbox(task_id)?;
            let bounds = match mailbox.bounds().await {
                Ok(bounds) => bounds,
                Err(StreamError::NotFound) => return Ok(Vec::new()),
                Err(error) => return Err(Error::Storage(error.to_string())),
            };
            if after > bounds.tail {
                return Err(Error::Invalid("inbox cursor is beyond the tail".into()));
            }
            let page_limit = u32::try_from(limit.min(acyclic_stream::MAX_ITEMS))
                .map_err(|_| Error::Invalid("inbox page bound is invalid".into()))?;
            let page = match mailbox.read(after, page_limit).await {
                Ok(records) => records.try_collect::<Vec<_>>().await?,
                Err(StreamError::NotFound) => return Ok(Vec::new()),
                Err(error) => return Err(Error::Storage(error.to_string())),
            };
            if page.len() > limit {
                return Err(Error::Storage("inbox page exceeds requested limit".into()));
            }
            let mut expected = after;
            let mut items = Vec::with_capacity(page.len());
            for record in page {
                if record.sequence != expected {
                    return Err(Error::Storage("inbox replay sequence differs".into()));
                }
                expected = expected
                    .checked_add(1)
                    .ok_or_else(|| Error::Storage("inbox sequence exhausted".into()))?;

                let value: Value = crate::contract::json_from_slice(&record.value)
                    .map_err(|error| Error::Storage(error.to_string()))?;
                if crate::contract::canonical_json_bytes(&value)? != record.value.as_ref() {
                    return Err(Error::Storage("mail event is not canonical JSON".into()));
                }
                let event: MailEvent = serde_json::from_value(value)
                    .map_err(|error| Error::Storage(error.to_string()))?;
                event.payload.validate(&mut None)?;
                recipient_admission
                    .limits
                    .validate_file(&event.payload, &mut None)?;
                if !read_granted(&recipient_admission.grants, &event.payload)? {
                    return Err(Error::Unauthorized(
                        "mail history contains an unreadable payload".into(),
                    ));
                }
                self.admission(OperationId::from_bytes(event.sender.into_bytes()))
                    .await?;
                items.push(InboxItem {
                    task_id,
                    sequence: record
                        .sequence
                        .checked_add(1)
                        .ok_or_else(|| Error::Storage("inbox sequence exhausted".into()))?,
                    message_id: event.message_id.to_string(),
                    payload: event.payload,
                });
            }
            Ok(items)
        })
    }

    fn wait_until<'a>(
        &'a self,
        task_id: TaskId,
        operation_id: OperationId,
        deadline_unix_ms: u64,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if deadline_unix_ms == 0 {
                return Err(Error::Invalid("timer deadline is invalid".into()));
            }
            let admission = self
                .admission(OperationId::from_bytes(task_id.into_bytes()))
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
                    .timer_state(task_id, operation_id, deadline_unix_ms)
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
        })
    }
}
