//! Workflow.
use super::*;

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    pub(crate) async fn suspend_workflow(
        &self,
        task: TaskId,
        fence: &crate::scheduler::LeaseFence,
        revision: u64,
        waiting_command: Option<OperationId>,
        idle_execution: Option<(OperationId, u64)>,
        pending: Option<(&acyclic_stream::StreamPath, u64)>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let fence = fence.clone_with_work(work)?;
        let operation_id = OperationId::from_bytes(task.into_bytes());
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        coordinator.observe_operation(
            self.verifier.audience(),
            &self.owner_scope,
            &self.verifier,
            operation_id,
            work,
        )?;
        let key = worker_key_with_work("suspend", operation_id, &fence, revision, work)?;
        let event = crate::scheduler::SchedulerEvent::WorkflowSuspended {
            operation_id,
            fence,
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
            self.verifier.audience(),
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
            return Err(admission_boundary(
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
            if operation.spec.owner.authority() != self.verifier.audience() {
                continue;
            }
            let operation = coordinator
                .observe_operation(
                    self.verifier.audience(),
                    &self.owner_scope,
                    &self.verifier,
                    event.operation_id(),
                    &mut None,
                )
                .cloned()?;
            if operation.phase == crate::scheduler::OperationPhase::Suspended
                && !operation.cancellation_requested
                && operation
                    .workflow
                    .as_ref()
                    .is_some_and(|slot| slot.waiting_command.is_some())
            {
                tasks.insert(crate::runtime::task_id_for_admission(event.operation_id()));
            }
        }
        Ok((
            after_revision + u64::from(count),
            through_revision,
            count,
            tasks.into_iter().collect(),
        ))
    }

    #[cfg(feature = "filesystem")]
    /// Reads the passive slot without claiming execution ownership.
    pub async fn workflow_suspension(
        &self,
        task: TaskId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Option<crate::scheduler::WorkflowSuspension>> {
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        let operation = coordinator.observe_operation(
            self.verifier.audience(),
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
    pub(crate) fn workflow_stream(&self) -> std::sync::Arc<P> {
        Arc::clone(&self.stream)
    }

    #[cfg(feature = "filesystem")]
    pub(crate) async fn wake_command(
        &self,
        task: TaskId,
        revision: u64,
        waiting: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let operation = OperationId::from_bytes(task.into_bytes());
        let mut key = String::new();
        if let Some(io) = work.as_deref_mut() {
            let digits = revision.checked_ilog10().map_or(1, |digits| digits + 1) as usize;
            let length = 92 + digits;
            io.admit_copy(length as u64)?;
            io.reserve_text(&mut key, length)?;
            io.copy_bytes(length as u64)?;
        }
        use std::fmt::Write as _;
        write!(&mut key, "task-command-wake:{task}:{revision}:{waiting}")
            .expect("writing into String");
        let input = self
            .stage_payload_with_work(operation, &key, b"null", work)
            .await?;
        let admission = self.admission_with_work(operation, work).await?;
        admission.limits.validate_file(&input, &mut None)?;
        if !crate::runtime::read_granted_with_work(&admission.grants, &input, work)? {
            return Err(admission_boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "task cannot read its command wake",
            ));
        }
        let (value, owned) = self.read_json(&input, work).await?;
        drop(value);
        if let Some(io) = work.as_deref_mut() {
            io.release_owned(owned)?;
        }
        self.coordinator
            .lock()
            .await
            .resume_workflow(
                self.verifier.audience(),
                &self.owner_scope,
                &self.verifier,
                operation,
                revision,
                input,
                IdempotencyKey::new(key)?,
                Some(waiting),
                work,
            )
            .await?;
        Ok(())
    }

    /// Delivers input into the exact owner-authorized workflow resume slot.
    pub async fn resume_workflow(
        &self,
        task: TaskId,
        revision: u64,
        input: FileRef,
        key: IdempotencyKey,
        native: crate::NativeWork<'_>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut io = native
            .map(|work| {
                crate::filesystem::PreparationIo::from_native_work(work)
                    .map(crate::store::HistoryWork::new)
            })
            .transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut work = io.as_mut().map(|io| io as &mut dyn crate::store::HistoryIo);
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let mut work: Option<&mut dyn crate::store::HistoryIo> = None;
        let _ = native;
        let operation_id = OperationId::from_bytes(task.into_bytes());
        let admission = self.admission_with_work(operation_id, &mut work).await?;
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
            if input.path().len() > admission.limits.path_bytes
                || input.descriptor().byte_length() > admission.limits.file_bytes
            {
                return Err(admission_boundary(
                    &mut work,
                    crate::wire::ErrorCode::Invalid,
                    "workflow resume input exceeds original task limits",
                ));
            }
        }
        admission.limits.validate_file(&input, &mut None)?;
        if !crate::runtime::read_granted_with_work(&admission.grants, &input, &mut work)? {
            return Err(admission_boundary(
                &mut work,
                crate::wire::ErrorCode::Unauthorized,
                "task cannot read its workflow resume input",
            ));
        }
        let (value, owned) = self.read_json(&input, &mut work).await?;
        drop(value);
        if let Some(io) = work.as_deref_mut() {
            io.release_owned(owned)?;
        }
        self.coordinator
            .lock()
            .await
            .resume_workflow(
                self.verifier.audience(),
                &self.owner_scope,
                &self.verifier,
                operation_id,
                revision,
                input,
                key,
                None,
                &mut work,
            )
            .await?;
        Ok(())
    }

    /// Reads the retained input under the current lease.
    pub async fn workflow_input(
        &self,
        task: TaskId,
        fence: &crate::scheduler::LeaseFence,
    ) -> Result<Option<(u64, Value)>> {
        self.workflow_input_with_work(task, fence, &mut None)
            .await
            .map(|(input, _)| input)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    /// Reads the retained input on its original native ledger.
    pub async fn workflow_input_with_native_work(
        &self,
        task: TaskId,
        fence: &crate::scheduler::LeaseFence,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Option<(u64, Value)>, acyclic_fs::NativeAllocation)> {
        let mut io = crate::filesystem::PreparationIo::from_native_work(work)
            .map(crate::store::HistoryWork::new)?;
        let metadata = std::mem::size_of::<Option<(u64, Value)>>() as u64;
        crate::store::history::HistoryIo::retain_owned(&mut io, metadata)?;
        let (input, bytes) = self
            .workflow_input_with_work(task, fence, &mut Some(&mut io))
            .await?;
        let custody =
            io.take_native_custody(bytes.checked_add(metadata).ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "workflow input custody overflows",
            })?)?;
        Ok((input, custody))
    }

    pub(super) async fn workflow_input_with_work(
        &self,
        task: TaskId,
        fence: &crate::scheduler::LeaseFence,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Option<(u64, Value)>, u64)> {
        let operation_id = OperationId::from_bytes(task.into_bytes());
        let admission = self.admission_with_work(operation_id, work).await?;
        let slot = {
            let mut coordinator = self.coordinator.lock().await;
            coordinator.refresh(work).await?;
            let operation = coordinator.observe_operation(
                self.verifier.audience(),
                &self.owner_scope,
                &self.verifier,
                operation_id,
                work,
            )?;
            crate::scheduler::require_execution_owner(operation, fence, false, &mut None)?;
            operation
                .workflow
                .as_ref()
                .map(|slot| slot.clone_with_work(work))
                .transpose()?
        };
        let Some(slot) = slot else {
            return Ok((None, 0));
        };
        let Some(input) = slot.input else {
            return Ok((None, 0));
        };
        admission.limits.validate_file(&input, &mut None)?;
        if !crate::runtime::read_granted_with_work(&admission.grants, &input, work)? {
            return Err(admission_boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "task cannot read its retained workflow input",
            ));
        }
        let (value, owned) = self.read_json(&input, work).await?;
        self.verify_owner(task, fence, false, work).await?;
        Ok((Some((slot.revision, value)), owned))
    }
}
