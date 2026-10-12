use super::*;

pub(super) fn validate_task_transition(
    output_schema: &Value,
    transition: &MachineTransition,
) -> Result<()> {
    if let MachineStatus::Completed { value } = &transition.status {
        validate_value(output_schema, value, "task output", None)?;
    }
    Ok(())
}

pub(super) fn task_original_work(
    io: Option<&crate::executor::PreparationIo>,
) -> Result<Option<&NativeWork>> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io {
        return io
            .original_native_work()
            .map(Some)
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "native workflow requires original work",
            });
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = io;
    Ok(None)
}

pub(super) fn task_machine_identity<'a>(
    machine: &'a dyn ResumableMachine,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<&'a MachineIdentity> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = task_original_work(io)? {
        return machine.identity_with_native_work(work);
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = io;
    Ok(machine.identity())
}

pub(super) fn inspect_task_metadata(
    io: Option<&crate::executor::PreparationIo>,
    items: u64,
) -> Result<()> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        use crate::store::history::HistoryIo as _;
        let mut observer = io.history_io();
        observer.check_cancellation()?;
        crate::store::history::HistoryIo::codec_charge(
            &mut observer,
            crate::contract::JsonCodecCharge::Inspect { items },
        )?;
    }
    #[cfg(not(feature = "filesystem"))]
    let _ = (io, items);
    Ok(())
}

pub(super) fn task_workflow_error(
    work: Option<&NativeWork>,
    code: crate::wire::ErrorCode,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.is_some() {
        return Error::NativeBoundary { code, reason };
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = work;
    match code {
        crate::wire::ErrorCode::Unsupported => Error::Unsupported(reason.into()),
        crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        crate::wire::ErrorCode::NotFound => Error::NotFound(reason.into()),
        crate::wire::ErrorCode::Invalid => Error::Invalid(reason.into()),
        _ => Error::Conflict(reason.into()),
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn native_task_open_value<T>(
    value: T,
    custody: acyclic_fs::NativeAllocation,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(T, Option<NativeCustody>)> {
    if !custody.matches_work(work) {
        drop(value);
        return Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
            custody: Some(custody),
        });
    }
    Ok((value, Some(custody)))
}

pub(super) async fn verify_task_execution_owner(
    host: &dyn DurableTaskHost,
    task: TaskId,
    fence: &crate::scheduler::LeaseFence,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = task_original_work(io)? {
        return host
            .verify_execution_owner_with_native_work(task, fence, work)?
            .await;
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = io;
    host.verify_execution_owner(task, fence.clone()).await
}

pub(super) async fn observe_task_open_admission(
    host: &dyn DurableTaskHost,
    task: TaskId,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(TaskAdmissionRecord, Option<NativeCustody>)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = task_original_work(io)? {
        let (value, custody) = host.observe_admission_with_native_work(task, work)?.await?;
        return native_task_open_value(value, custody, work);
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = io;
    Ok((host.observe_admission(task).await?, None))
}

pub(super) fn initialize_task_machine(
    machine: &dyn ResumableMachine,
    input: &Value,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(Value, Option<NativeCustody>)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = task_original_work(io)? {
        let (value, custody) = machine.initialize_with_native_work(input, work)?;
        return native_task_open_value(value, custody, work);
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = io;
    Ok((machine.initialize(input)?, None))
}

pub(super) async fn observe_workflow_open_admission(
    journal: &dyn WorkflowJournal,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(Option<WorkflowAdmission>, Option<NativeCustody>)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = task_original_work(io)? {
        let (value, custody) = journal.admission_with_native_work(work)?.await?;
        return native_task_open_value(value, custody, work);
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = io;
    Ok((journal.admission().await?, None))
}

pub(super) async fn admit_workflow_open(
    journal: &dyn WorkflowJournal,
    admission: WorkflowAdmission,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(WorkflowAdmission, Option<NativeCustody>)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = task_original_work(io)? {
        let (value, custody) = journal.admit_with_native_work(admission, work)?.await?;
        return native_task_open_value(value, custody, work);
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = io;
    Ok((journal.admit(admission).await?, None))
}

pub(super) async fn validate_latest_task_transition(
    workflow: &DurableWorkflowHost,
    schema: &Value,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = task_original_work(io)? {
        let (transition, custody) = workflow.latest_transition_with_native_work(work).await?;
        let latest = native_task_open_value(transition, custody, work)?;
        if let Some(transition) = &latest.0 {
            if let MachineStatus::Completed { value } = &transition.status {
                crate::contract::validate_json_schema_value(schema, value, "task output", io)?;
            }
        }
        return Ok(());
    }
    if let Some(transition) = workflow.latest_transition().await? {
        validate_task_transition(schema, &transition)?;
    }
    Ok(())
}

/// A registered task's current checkpoint and bounded workflow replay index.

pub struct ResumableTaskSession {
    pub(super) task_id: TaskId,
    pub(super) fence: crate::scheduler::LeaseFence,
    pub(super) host: Arc<dyn DurableTaskHost>,
    pub(super) workflow: DurableWorkflowHost,
    pub(super) output_schema: Value,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) native_context: Option<Arc<TaskContext>>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) native_metadata_custody: Option<acyclic_fs::NativeAllocation>,
}

impl ResumableTaskSession {
    /// Returns the owner-retained task identity.
    #[must_use]
    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// Returns the latest committed task checkpoint.
    #[must_use]
    pub const fn checkpoint(&self) -> &MachineCheckpoint {
        self.workflow.checkpoint()
    }

    /// Reads the latest committed outbox/status for recovery without advancing.
    pub async fn latest_transition(&self) -> Result<Option<MachineTransition>> {
        self.workflow.latest_transition().await
    }

    /// Returns the actual latest transition and its original retained guard.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn latest_transition_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Option<MachineTransition>, acyclic_fs::NativeAllocation)> {
        self.require_native_session(work)?;
        self.workflow.latest_transition_with_native_work(work).await
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) fn require_native_session(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.workflow.require_native_work()?;
        let context = self.native_context.as_ref().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "task session was not opened with native custody",
        })?;
        if !context.native_metadata_matches(work)
            || self
                .native_metadata_custody
                .as_ref()
                .is_none_or(|held| !held.matches_work(work))
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "task session belongs to another native ledger",
            });
        }
        Ok(())
    }

    /// Commits one deterministic task transition and its outbox.
    pub async fn step(
        &mut self,
        operation_id: OperationId,
        idempotency_key: crate::IdempotencyKey,
        input: Value,
    ) -> Result<MachineTransition> {
        Ok(self
            .step_with_io(operation_id, idempotency_key, input, None)
            .await?
            .0)
    }

    /// Consumes original response custody and retains it through state publication.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn step_with_native_work(
        &mut self,
        operation_id: OperationId,
        idempotency_key: crate::IdempotencyKey,
        input: Value,
        custody: acyclic_fs::NativeAllocation,
    ) -> Result<(MachineTransition, acyclic_fs::NativeAllocation)> {
        let mut input = (input, custody);
        self.require_native_session(input.1.work())?;
        let io = crate::filesystem::PreparationIo::from_native_work(input.1.work())?;
        let result = self
            .step_with_io(
                operation_id,
                idempotency_key,
                std::mem::take(&mut input.0),
                Some(&io),
            )
            .await;
        match result {
            Ok((transition, custody)) => {
                if let Some(mut guard) = custody {
                    input
                        .1
                        .absorb(&mut guard)
                        .map_err(|cause| Error::NativeWork {
                            cause,
                            custody: Some(guard),
                        })?;
                }
                Ok((transition, input.1))
            }
            Err(Error::NativeWork {
                cause,
                custody: incoming,
            }) => {
                if let Some(mut guard) = incoming {
                    if !input.1.work().shares_ledger(guard.work())
                        || input.1.absorb(&mut guard).is_err()
                    {
                        return Err(Error::NativeWork {
                            cause,
                            custody: Some(guard),
                        });
                    }
                }
                Err(Error::NativeWork {
                    cause,
                    custody: Some(input.1),
                })
            }
            Err(error) => Err(error),
        }
    }

    pub(super) async fn step_with_io(
        &mut self,
        operation_id: OperationId,
        idempotency_key: crate::IdempotencyKey,
        input: Value,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(MachineTransition, Option<NativeCustody>)> {
        self.verify_step_owner(io).await?;
        let output_schema = &self.output_schema;
        let check = |transition: &MachineTransition| {
            if io.is_some() {
                if let MachineStatus::Completed { value } = &transition.status {
                    crate::contract::validate_json_schema_value(
                        output_schema,
                        value,
                        "task output",
                        io,
                    )?;
                }
                Ok(())
            } else {
                validate_task_transition(output_schema, transition)
            }
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let output = if let Some(io) = io {
            let work = io.original_native_work().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "native task state requires original work",
            })?;
            let (transition, custody) = self
                .workflow
                .step_checked_with_native_work(operation_id, idempotency_key, input, work, check)
                .await?;
            (transition, Some(custody))
        } else {
            (
                self.workflow
                    .step(operation_id, idempotency_key, input, check)
                    .await?,
                None,
            )
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let output = (
            self.workflow
                .step(operation_id, idempotency_key, input, check)
                .await?,
            None,
        );
        self.verify_step_owner(io).await?;
        Ok(output)
    }

    pub(super) async fn verify_step_owner(
        &self,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = io {
            let work = io.original_native_work().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "native task ownership requires original work",
            })?;
            return self
                .host
                .verify_dispatch_owner_with_native_work(self.task_id, &self.fence, work)?
                .await;
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = io;
        self.host
            .verify_dispatch_owner(self.task_id, self.fence.clone())
            .await
    }
}
