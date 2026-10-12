//! Main durable task operations.
use super::*;

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    /// Releases an owned slot after the worker durably retains its checkpoint
    /// and knows there is no uncertain active dispatch. No passive future is kept.
    pub async fn suspend_workflow(
        &self,
        task: TaskId,
        fence: crate::scheduler::LeaseFence,
        revision: u64,
    ) -> Result<()> {
        self.suspend_workflow_command(task, fence, revision, None)
            .await
    }

    /// Retains the exact passive command in the existing suspension slot.
    pub async fn suspend_workflow_command(
        &self,
        task: TaskId,
        fence: crate::scheduler::LeaseFence,
        revision: u64,
        waiting_command: Option<OperationId>,
    ) -> Result<()> {
        self.suspend_workflow_command_guarded(task, fence, revision, waiting_command, None)
            .await
    }

    /// Releases a passive command's slot only while its existing execution
    /// journal is empty, atomically with the scheduler publication. The caller
    /// supplies the exact execution identity; this is not an effects settlement.
    pub async fn suspend_workflow_command_if_execution_idle(
        &self,
        task: TaskId,
        fence: crate::scheduler::LeaseFence,
        revision: u64,
        command: OperationId,
        execution: OperationId,
    ) -> Result<()> {
        self.suspend_workflow_command_guarded(
            task,
            fence,
            revision,
            Some(command),
            Some((execution, 0)),
        )
        .await
    }

    pub(super) async fn suspend_workflow_command_guarded(
        &self,
        task: TaskId,
        fence: crate::scheduler::LeaseFence,
        revision: u64,
        waiting_command: Option<OperationId>,
        idle_execution: Option<(OperationId, u64)>,
    ) -> Result<()> {
        let operation_id = OperationId::from_bytes(task.into_bytes());
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh().await?;
        coordinator.observe_operation(
            &self.owner,
            &self.owner_scope,
            &self.verifier,
            operation_id,
        )?;
        let key = worker_key("suspend", operation_id, &fence, revision)?;
        let event = crate::scheduler::SchedulerEvent::WorkflowSuspended {
            operation_id,
            fence,
            workflow_revision: revision,
            waiting_command,
        };
        if let Some((execution, tail)) = idle_execution {
            coordinator
                .suspend_if_execution_idle(key, event, execution, tail)
                .await?;
        } else {
            coordinator.apply(operation_id, key, event).await?;
        }
        let current = coordinator.observe_operation(
            &self.owner,
            &self.owner_scope,
            &self.verifier,
            operation_id,
        )?;
        if current.phase != crate::scheduler::OperationPhase::Suspended
            || current.workflow.as_ref().is_none_or(|slot| {
                slot.revision != revision || slot.waiting_command != waiting_command
            })
        {
            return Err(Error::Conflict(
                "workflow suspension receipt is no longer current".into(),
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
    ) -> Result<WorkflowWaitPage> {
        if limit == 0 {
            return Err(Error::Invalid("wake page limit is out of bounds".into()));
        }
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh().await?;
        self.verifier.verify_audience(&self.owner)?;
        self.verifier.verify(&self.owner_scope)?;
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
                event.event,
                crate::scheduler::SchedulerEvent::WorkflowSuspended {
                    waiting_command: Some(_),
                    ..
                }
            ) {
                continue;
            }
            let operation = coordinator
                .scheduler()
                .operation(event.operation_id)
                .ok_or_else(|| Error::Storage("wake event operation is absent".into()))?;
            if operation.spec.owner.authority() != &self.owner {
                continue;
            }
            let operation = coordinator.observe_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                event.operation_id,
            )?;
            if operation.phase == crate::scheduler::OperationPhase::Suspended
                && !operation.cancellation_requested
                && operation
                    .workflow
                    .as_ref()
                    .is_some_and(|slot| slot.waiting_command.is_some())
            {
                tasks.insert(TaskId::from_bytes(event.operation_id.into_bytes()));
            }
        }
        Ok(WorkflowWaitPage {
            next_revision: after_revision + u64::from(count),
            through_revision,
            events_read: count,
            tasks: tasks.into_iter().collect(),
        })
    }

    /// Authenticated read of a passive slot; never claims execution ownership.
    #[cfg(feature = "filesystem")]
    pub(crate) async fn workflow_suspension(
        &self,
        task: TaskId,
    ) -> Result<Option<crate::scheduler::WorkflowSuspension>> {
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh().await?;
        let operation = coordinator.observe_operation(
            &self.owner,
            &self.owner_scope,
            &self.verifier,
            OperationId::from_bytes(task.into_bytes()),
        )?;
        if operation.phase != crate::scheduler::OperationPhase::Suspended
            || operation.cancellation_requested
        {
            return Ok(None);
        }
        Ok(operation.workflow)
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
        let input = self.payloads.stage(operation, &key, b"null").await?;
        let admission = self.admission(operation).await?;
        admission.limits.validate_file(&input)?;
        if !read_granted(&admission.grants, &input)? {
            return Err(Error::Unauthorized(
                "task cannot read its command wake".into(),
            ));
        }
        self.read_json(&input).await?;
        self.coordinator
            .lock()
            .await
            .resume_command(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                operation,
                revision,
                input,
                IdempotencyKey::new(key)?,
                waiting,
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
        admission.limits.validate_file(&input)?;
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
            coordinator.refresh().await?;
            let operation = coordinator.observe_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                operation_id,
            )?;
            crate::scheduler::require_execution_owner(&operation, fence, false)?;
            operation.workflow
        };
        let Some(slot) = slot else { return Ok(None) };
        let Some(input) = slot.input else {
            return Ok(None);
        };
        admission.limits.validate_file(&input)?;
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
