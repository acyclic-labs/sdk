//! Main durable task operations.
use super::*;

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    /// Suspends the exact lease with optional atomic execution and workflow guards.
    #[allow(clippy::too_many_arguments)]
    pub async fn suspend_workflow(
        &self,
        task: TaskId,
        fence: &crate::scheduler::LeaseFence,
        revision: u64,
        waiting_command: Option<OperationId>,
        idle_execution: Option<(OperationId, u64)>,
        pending: Option<(&acyclic_stream::StreamPath, u64)>,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        if pending.is_some() && idle_execution.is_none() {
            return Err(crate::scheduler::diagnostic(
                work,
                crate::wire::ErrorCode::Invalid,
                "workflow tail guard requires its execution guard",
            ));
        }
        let operation_id = OperationId::from_bytes(task.into_bytes());
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        coordinator.observe_operation(
            &self.owner,
            &self.owner_scope,
            &self.verifier,
            operation_id,
            work,
        )?;
        let key = worker_key("suspend", operation_id, fence, revision, work)?;
        let event = crate::scheduler::SchedulerEvent::WorkflowSuspended {
            operation_id,
            fence: fence.clone_with_work(work)?,
            workflow_revision: revision,
            waiting_command,
        };
        if let Some((execution, tail)) = idle_execution {
            coordinator
                .apply_if_execution_quiescent(
                    key,
                    event,
                    execution,
                    tail,
                    pending.map(|(path, tail)| (path.as_str(), tail)),
                    work,
                )
                .await?;
        } else {
            coordinator.apply(key, event, work).await?;
        }
        let current = coordinator.observe_operation(
            &self.owner,
            &self.owner_scope,
            &self.verifier,
            operation_id,
            work,
        )?;
        if current.phase != crate::scheduler::OperationPhase::Suspended
            || current.cancellation_requested
            || current.workflow.as_ref().is_none_or(|slot| {
                slot.revision != revision || slot.waiting_command != waiting_command
            })
        {
            return Err(crate::scheduler::diagnostic(
                work,
                crate::wire::ErrorCode::Conflict,
                "workflow suspension receipt is no longer current",
            ));
        }
        Ok(())
    }

    /// One bounded source page, filtered by the current signed owner projection.
    #[cfg(feature = "filesystem")]
    pub(crate) async fn workflow_wait_page(
        &self,
        after_revision: u64,
        through_revision: Option<u64>,
        limit: u32,
    ) -> Result<(u64, u64, u32, Vec<TaskId>)> {
        if limit == 0 {
            return Err(Error::Invalid("wake page limit is out of bounds".into()));
        }
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(&mut None).await?;
        self.verifier.verify_audience(&self.owner, &mut None)?;
        self.verifier.verify(&self.owner_scope, &mut None)?;
        if !self
            .owner_scope
            .capabilities()
            .contains("operation:observe")
        {
            return Err(Error::Unauthorized(
                "wake discovery requires operation:observe".into(),
            ));
        }
        let through_revision = through_revision.unwrap_or(coordinator.revision());
        if after_revision > through_revision || through_revision > coordinator.revision() {
            return Err(Error::Invalid(
                "wake cursor exceeds its coordinator snapshot".into(),
            ));
        }
        let count = u32::try_from(u64::from(limit.min(64)).min(through_revision - after_revision))
            .map_err(|_| Error::Storage("wake page count exceeds its allowance".into()))?;
        let events = if count == 0 {
            Vec::new()
        } else {
            crate::distributed::read_coordinator_event_page(&self.stream, after_revision, count)
                .await?
        };
        if events.len() != count as usize {
            return Err(Error::Storage("wake snapshot has missing events".into()));
        }
        let mut tasks = BTreeSet::new();
        for event in events {
            if !matches!(
                event,
                crate::scheduler::SchedulerEvent::WorkflowSuspended {
                    waiting_command: Some(_),
                    ..
                }
            ) {
                continue;
            }
            let operation = coordinator
                .scheduler()
                .operation(event.operation_id())
                .ok_or_else(|| Error::Storage("wake event operation is absent".into()))?;
            if operation.spec.owner.authority() != &self.owner {
                continue;
            }
            let operation = coordinator.observe_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                event.operation_id(),
                &mut None,
            )?;
            if operation.phase == crate::scheduler::OperationPhase::Suspended
                && !operation.cancellation_requested
                && operation
                    .workflow
                    .as_ref()
                    .is_some_and(|slot| slot.waiting_command.is_some())
            {
                tasks.insert(TaskId::from_bytes(event.operation_id().into_bytes()));
            }
        }
        Ok((
            after_revision + u64::from(count),
            through_revision,
            count,
            tasks.into_iter().collect(),
        ))
    }

    /// Authenticated read of a passive slot; never claims execution ownership.
    #[cfg(feature = "filesystem")]
    pub(crate) async fn workflow_suspension(
        &self,
        task: TaskId,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<Option<crate::scheduler::WorkflowSuspension>> {
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        let operation = coordinator.observe_operation(
            &self.owner,
            &self.owner_scope,
            &self.verifier,
            OperationId::from_bytes(task.into_bytes()),
            work,
        )?;
        if operation.phase != crate::scheduler::OperationPhase::Suspended
            || operation.cancellation_requested
        {
            return Ok(None);
        }
        operation
            .workflow
            .as_ref()
            .map(|slot| slot.clone_with_work(work))
            .transpose()
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn workflow_stream(&self) -> Arc<P> {
        self.stream.clone()
    }

    /// Uses the existing owner-authenticated wake publication, with stable input.
    #[cfg(feature = "filesystem")]
    pub(crate) async fn wake_command(
        &self,
        task: TaskId,
        revision: u64,
        waiting: OperationId,
    ) -> Result<()> {
        let operation = OperationId::from_bytes(task.into_bytes());
        let key = format!("task-command-wake:{task}:{revision}:{waiting}");
        let input = self.payloads.stage(operation, &key, b"null", None)?.await?;
        let admission = self.admission(operation).await?;
        admission.limits.validate_file(&input, &mut None)?;
        if !read_granted(&admission.grants, &input)? {
            return Err(Error::Unauthorized(
                "task cannot read its command wake".into(),
            ));
        }
        self.read_json(&input).await?;
        self.coordinator
            .lock()
            .await
            .resume_workflow(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                operation,
                revision,
                input,
                IdempotencyKey::new(key)?,
                Some(waiting),
                &mut None,
            )
            .await?;
        Ok(())
    }

    /// Owner-authorized delivery into one exact durable workflow resume slot.
    pub async fn resume_workflow(
        &self,
        task: TaskId,
        revision: u64,
        input: FileRef,
        key: IdempotencyKey,
    ) -> Result<()> {
        let operation_id = OperationId::from_bytes(task.into_bytes());
        let admission = self.admission(operation_id).await?;
        admission.limits.validate_file(&input, &mut None)?;
        if !read_granted(&admission.grants, &input)? {
            return Err(Error::Unauthorized(
                "task cannot read its workflow resume input".into(),
            ));
        }
        self.read_json(&input).await?;
        self.coordinator
            .lock()
            .await
            .resume_workflow(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                operation_id,
                revision,
                input,
                key,
                None,
                &mut None,
            )
            .await?;
        Ok(())
    }

    /// Reads the retained input under the current lease. Its workflow revision
    /// lets recovery skip an input already consumed by a committed step.
    pub async fn workflow_input(
        &self,
        task: TaskId,
        fence: &crate::scheduler::LeaseFence,
    ) -> Result<Option<(u64, Value)>> {
        let operation_id = OperationId::from_bytes(task.into_bytes());
        let admission = self.admission(operation_id).await?;
        let slot = {
            let mut coordinator = self.coordinator.lock().await;
            coordinator.refresh(&mut None).await?;
            let operation = coordinator.observe_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                operation_id,
                &mut None,
            )?;
            crate::scheduler::require_execution_owner(operation, fence, false, &mut None)?;
            operation.workflow.clone()
        };
        let Some(slot) = slot else { return Ok(None) };
        let Some(input) = slot.input else {
            return Ok(None);
        };
        admission.limits.validate_file(&input, &mut None)?;
        if !read_granted(&admission.grants, &input)? {
            return Err(Error::Unauthorized(
                "task cannot read its retained workflow input".into(),
            ));
        }
        let value = self.read_json(&input).await?;
        self.verify_owner(task, fence, false).await?;
        Ok(Some((slot.revision, value)))
    }
}
