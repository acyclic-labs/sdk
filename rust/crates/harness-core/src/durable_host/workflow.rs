//! Main durable task operations.
use super::*;

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    /// Suspends the exact lease with optional atomic execution and workflow guards.
    pub async fn suspend_workflow<'a, 'w: 'a>(
        &'a self,
        task: TaskId,
        fence: &'a crate::scheduler::LeaseFence,
        waiting: (u64, Option<OperationId>),
        idle_execution: Option<(OperationId, u64)>,
        pending: Option<(&'a acyclic_stream::StreamPath, u64)>,
        work: &'a mut Option<&'w mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        let (revision, waiting_command) = waiting;
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
    #[doc(hidden)]
    pub async fn workflow_suspension(
        &self,
        task: TaskId,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<Option<crate::scheduler::WorkflowSuspension>> {
        self.read_workflow_suspension(task, false, work).await
    }

    /// Reads a retained receive cut after cancellation released execution ownership.
    #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
    #[doc(hidden)]
    pub async fn cancelled_workflow_suspension(
        &self,
        task: TaskId,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<Option<crate::scheduler::WorkflowSuspension>> {
        self.read_workflow_suspension(task, true, work).await
    }

    #[cfg(feature = "filesystem")]
    async fn read_workflow_suspension(
        &self,
        task: TaskId,
        cancelled: bool,
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
        let readable = if cancelled {
            operation.phase == crate::scheduler::OperationPhase::Terminal
                && matches!(operation.outcome, Some(Outcome::Cancelled))
                && operation.reservation.is_none()
        } else {
            operation.phase == crate::scheduler::OperationPhase::Suspended
                && !operation.cancellation_requested
        };
        if !readable {
            return Ok(None);
        }
        operation
            .workflow
            .as_ref()
            .map(|slot| slot.clone_with_work(work))
            .transpose()
    }

    #[cfg(feature = "filesystem")]
    #[doc(hidden)]
    pub fn workflow_stream(&self) -> Arc<P> {
        self.stream.clone()
    }

    /// Wakes the exact command wait with its stable null input.
    #[cfg(feature = "filesystem")]
    #[doc(hidden)]
    pub async fn wake_command(
        &self,
        task: TaskId,
        revision: u64,
        waiting: OperationId,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        let operation = OperationId::from_bytes(task.into_bytes());
        let text = crate::executor::preparation_format(
            format_args!("task-command-wake:{task}:{revision}:{waiting}"),
            work,
        )?;
        let io = operation_io(work)?;
        let input = self
            .payloads
            .stage(operation, &text, b"null", io.as_ref())?
            .await?;
        let admission = self.admission(operation, work).await?;
        admission.limits.validate_file(&input, work)?;
        if !self.read_json(&input, io.as_ref()).await?.is_null() {
            return Err(crate::scheduler::diagnostic(
                work,
                crate::wire::ErrorCode::Invalid,
                "host command wake is not null",
            ));
        }
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
                IdempotencyKey::new(text)?,
                Some(waiting),
                work,
            )
            .await?;
        Ok(())
    }

    /// Delivers owner-authorized input into the exact retained workflow slot.
    pub async fn resume_workflow(
        &self,
        task: TaskId,
        revision: u64,
        input: FileRef,
        key: IdempotencyKey,
        waiting: Option<OperationId>,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        let operation = OperationId::from_bytes(task.into_bytes());
        let io = operation_io(work)?;
        #[cfg(feature = "filesystem")]
        let mut admission_history = io.as_ref().map(|io| io.history_io());
        #[cfg(feature = "filesystem")]
        let mut admission_work = admission_history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::HistoryIo);
        #[cfg(not(feature = "filesystem"))]
        let mut admission_work = None;
        let admission = self.admission(operation, &mut admission_work).await?;
        admission.limits.validate_file(&input, work)?;
        if !read_granted(&admission.grants, &input)? {
            return Err(crate::scheduler::diagnostic(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "task cannot read workflow resume input",
            ));
        }
        self.read_json(&input, io.as_ref()).await?;
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
                key,
                waiting,
                work,
            )
            .await?;
        Ok(())
    }

    /// Reads the retained input under the original current lease and supplied IO.
    pub async fn workflow_input(
        &self,
        task: TaskId,
        fence: &crate::scheduler::LeaseFence,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<(u64, Value)>> {
        let operation = OperationId::from_bytes(task.into_bytes());
        #[cfg(feature = "filesystem")]
        let mut history = io.map(|io| io.history_io());
        #[cfg(feature = "filesystem")]
        let mut work = history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::HistoryIo);
        #[cfg(not(feature = "filesystem"))]
        let mut work: Option<&mut dyn crate::store::HistoryIo> = None;
        let mut quota = crate::wire_codec::CodecQuota::new(&mut work, 0)?;
        let slot = {
            let mut coordinator = self.coordinator.lock().await;
            coordinator.refresh(quota.work()).await?;
            let state = coordinator.observe_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                operation,
                quota.work(),
            )?;
            crate::scheduler::require_execution_owner(state, fence, false, quota.work())?;
            state
                .workflow
                .as_ref()
                .map(|slot| slot.clone_with_work(quota.work()))
                .transpose()?
        };
        let Some(slot) = slot else {
            return Ok(None);
        };
        quota.adopt(slot.logical_owned_bytes()?)?;
        let Some(input) = slot.input else {
            return Ok(None);
        };
        let admission = self.admission(operation, quota.work()).await?;
        quota.adopt(
            std::mem::size_of::<TaskAdmissionRecord>() as u64 + admission.logical_owned_bytes()?,
        )?;
        admission.limits.validate_file(&input, quota.work())?;
        if !read_granted(&admission.grants, &input)? {
            require_host_wake(
                input.descriptor(),
                slot.waiting_command.is_some(),
                quota.work(),
            )?;
        }
        let value = self.read_json(&input, io).await?;
        self.verify_owner(task, fence, false, quota.work()).await?;
        drop((admission, input));
        Ok(Some((slot.revision, value)))
    }
}

fn require_host_wake(
    descriptor: &crate::conversation::FileDescriptor,
    waiting: bool,
    work: &mut Option<&mut dyn crate::store::HistoryIo>,
) -> Result<()> {
    if !waiting || descriptor.byte_length() != 4 || descriptor.media_type() != "application/json" {
        return Err(crate::scheduler::diagnostic(
            work,
            crate::wire::ErrorCode::Unauthorized,
            "task cannot read retained workflow input",
        ));
    }
    // Only the fixed public host signal can cross this grant boundary.
    descriptor.verify(b"null", work)
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        // The exception accepts exactly the public null bytes for a command wait.
        #[test]
        fn host_wake_grant_exception_is_only_the_exact_null_signal(
            bytes in prop_oneof![Just(b"null".to_vec()), proptest::collection::vec(any::<u8>(), 0..17)],
            json in any::<bool>(), waiting in any::<bool>(),
        ) {
            let media = if json { "application/json" } else { "text/plain" };
            let descriptor = crate::conversation::FileDescriptor::from_bytes(&bytes, media, &mut None).unwrap();
            prop_assert_eq!(require_host_wake(&descriptor, waiting, &mut None).is_ok(),
                waiting && json && bytes == b"null");
        }
    }
}
