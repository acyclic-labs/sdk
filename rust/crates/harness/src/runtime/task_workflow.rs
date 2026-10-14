use super::*;

pub(super) fn validate_task_transition(
    output_schema: &Value,
    transition: &MachineTransition,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    if let MachineStatus::Completed { value } = &transition.status {
        crate::executor::with_history_io(io, |work| {
            crate::executor::validate_json_schema_value(output_schema, value, "task output", work)
        })?;
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
    if io.is_some() {
        return Err(Error::Unsupported(
            "native workflow requires original work".into(),
        ));
    }
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

pub(super) async fn observe_task_open_admission(
    host: &dyn DurableTaskHost,
    task: TaskId,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<TaskAdmissionRecord> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = task_original_work(io)? {
        let (value, custody) = host.observe_admission_with_native_work(task, work)?.await?;
        return retain_task_value((value, Some(custody)), io);
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = io;
    host.observe_admission(task).await
}

pub(super) fn initialize_task_machine(
    machine: &dyn ResumableMachine,
    input: &Value,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<Value> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = task_original_work(io)? {
        let (value, custody) = machine.initialize_with_native_work(input, work)?;
        return retain_task_value((value, Some(custody)), io);
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = io;
    machine.initialize(input)
}

pub(super) fn retain_task_value<T>(
    value: (T, crate::NativeCustody),
    io: Option<&crate::executor::PreparationIo>,
) -> Result<T> {
    let _ = io;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = task_original_work(io)? {
        if value
            .1
            .as_ref()
            .is_none_or(|held| !held.work().shares_allocation_context(work))
        {
            drop(value.0);
            return Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                custody: value.1,
            });
        }
        let (Some(io), Some(custody)) = (io, value.1) else {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "original workflow custody is missing",
            });
        };
        io.absorb_native_custody(custody)?;
        return Ok(value.0);
    }
    let _ = io;
    Ok(value.0)
}

pub(super) async fn admit_workflow_open(
    journal: &dyn WorkflowJournal,
    admission: WorkflowAdmission,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<WorkflowAdmission> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = task_original_work(io)? {
        let (value, custody) = journal.admit_with_native_work(admission, work)?.await?;
        return retain_task_value((value, Some(custody)), io);
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = io;
    journal.admit(admission).await
}

pub(super) async fn validate_latest_task_transition(
    workflow: &DurableWorkflowHost,
    schema: &Value,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    let latest = match task_original_work(io)? {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        Some(work) => {
            let (value, custody) = workflow.latest_transition_with_native_work(work).await?;
            (retain_task_value((value, Some(custody)), io)?, None)
        }
        _ => (workflow.latest_transition().await?, None::<NativeCustody>),
    };
    if let Some(transition) = &latest.0 {
        validate_task_transition(schema, transition, io)?;
    }
    Ok(())
}

/// A registered task's current checkpoint and bounded replay index.
pub struct ResumableTaskSession {
    pub(super) task_id: TaskId,
    pub(super) fence: crate::scheduler::LeaseFence,
    pub(super) host: Arc<dyn DurableTaskHost>,
    pub(super) owner: Option<Arc<dyn OwnedTaskMailbox>>,
    pub(super) workflow: DurableWorkflowHost,
    pub(super) definition: Arc<dyn RegisteredTaskDefinition>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) native_metadata_custody: crate::NativeCustody,
}

impl ResumableTaskSession {
    /// Returns the owner-retained task identity.
    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// Returns the latest committed checkpoint.
    #[must_use]
    pub const fn checkpoint(&self) -> &MachineCheckpoint {
        self.workflow.checkpoint()
    }

    /// Reads the committed transition under the caller's original IO.
    pub async fn latest_transition(
        &self,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(Option<MachineTransition>, crate::NativeCustody)> {
        match task_original_work(io)? {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Some(work) => {
                self.require_native_session(work)?;
                let (value, custody) = self
                    .workflow
                    .latest_transition_with_native_work(work)
                    .await?;
                Ok((value, Some(custody)))
            }
            _ => Ok((self.workflow.latest_transition().await?, None)),
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn require_native_session(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        self.workflow.require_native_work()?;
        if self
            .native_metadata_custody
            .as_ref()
            .is_none_or(|held| !held.work().shares_allocation_context(work))
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "task session belongs to another native ledger",
            });
        }
        Ok(())
    }

    /// Commits one owner-checked transition and its outbox using the caller's IO.
    pub async fn step(
        &mut self,
        operation: OperationId,
        key: crate::IdempotencyKey,
        input: Value,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(MachineTransition, crate::NativeCustody)> {
        let native = task_original_work(io)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = native {
            self.require_native_session(work)?;
        }
        match &self.owner {
            Some(owner) => owner.verify_owner(io)?.await?,
            None => {
                self.host
                    .verify_dispatch_owner(self.task_id, &self.fence, io)?
                    .await?;
            }
        }
        let schema = self.definition.output_schema();
        let check =
            |transition: &MachineTransition| validate_task_transition(schema, transition, io);
        let transition = match native {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Some(work) => {
                let (value, custody) = self
                    .workflow
                    .step_checked_with_native_work(operation, key, input, work, check)
                    .await?;
                (value, Some(custody))
            }
            _ => (
                self.workflow.step(operation, key, input, check).await?,
                None,
            ),
        };
        match &self.owner {
            Some(owner) => owner.verify_owner(io)?.await?,
            None => {
                self.host
                    .verify_dispatch_owner(self.task_id, &self.fence, io)?
                    .await?;
            }
        }
        Ok(transition)
    }
}
