//! Common public provider for an already admitted native workspace incarnation.
use super::journal::{Admission, Journal};
use super::*;
use acyclic_stream::{StreamPath, StreamProvider};

/// Physical primitives. Authentication, selection, keyed settlement and
/// retained observations belong to the shared provider above this boundary.
#[async_trait]
pub(crate) trait NativeMachine: Send + Sync + 'static {
    async fn exec(&self, command: ExecCommand) -> Result<ExecStream, ProviderError>;
    async fn read(&self, access: FileAccess, limit: u32) -> Result<FileRead, ProviderError>;
    async fn write(
        &self,
        access: FileAccess,
        data: Vec<u8>,
        truncate: bool,
    ) -> Result<FileWrite, ProviderError>;
    async fn stop(&self) -> Result<(), ProviderError>;
    async fn wake(&self) -> Result<(), ProviderError>;
}

pub(crate) struct AdmittedMachine<B> {
    backend: B,
    observation: Mutex<MachineObservation>,
    journal: Journal,
}

#[derive(Serialize)]
enum Intent<'a> {
    Create(&'a CreateMachine),
    Attach(MachineId, &'a wire::selection::VolumeRef),
    Suspend(MachineId),
    Wake(MachineId),
    Destroy(MachineId),
    Policy(MachineId, SuspensionPolicy),
}

impl<B: NativeMachine> AdmittedMachine<B> {
    pub(crate) async fn open(
        backend: B,
        observation: MachineObservation,
        streams: Arc<dyn StreamProvider>,
        root: StreamPath,
    ) -> Result<Self, ProviderError> {
        let journal = Journal::new(streams, root, observation.id);
        let observation = journal.enroll(observation).await?;
        if observation.state != MachineState::Running {
            backend.stop().await?;
        }
        Ok(Self {
            backend,
            observation: Mutex::new(observation),
            journal,
        })
    }

    fn select(value: &MachineObservation, machine: MachineId) -> Result<(), ProviderError> {
        if value.id == machine {
            Ok(())
        } else {
            Err(ProviderError::NotFound(machine.to_string()))
        }
    }

    async fn change(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
        target: MachineState,
    ) -> Result<MutationOutcome, ProviderError> {
        let mut value = self.observation.lock().await;
        Self::select(&value, machine)?;
        let (intent, outcome) = match target {
            MachineState::Suspended => (
                Intent::Suspend(machine),
                MutationOutcome::Suspended(machine),
            ),
            MachineState::Running => (Intent::Wake(machine), MutationOutcome::Woken(machine)),
            MachineState::Destroyed => (
                Intent::Destroy(machine),
                MutationOutcome::MachineDestroyed(machine),
            ),
            _ => {
                return Err(ProviderError::Invalid(
                    "unknown native transition; recovery: use suspend, wake or destroy".into(),
                ));
            }
        };
        let digest = intent_digest(&(value.id, &intent))?;
        let mut pending = value.clone();
        pending.state = match target {
            MachineState::Running => MachineState::Waking,
            MachineState::Suspended => MachineState::Suspending,
            _ => MachineState::Destroying,
        };
        let admitted = match self
            .journal
            .begin(
                key,
                digest,
                (value.state != target).then_some(&pending),
                || {
                    if matches!(
                        value.state,
                        MachineState::Suspending
                            | MachineState::Waking
                            | MachineState::Destroying
                            | MachineState::Indeterminate
                            | MachineState::Failed
                    ) {
                        return Err(ProviderError::Indeterminate(key));
                    }
                    if value.state == MachineState::Destroyed && target != MachineState::Destroyed {
                        return Err(ProviderError::Conflict(
                "Machine incarnation is destroyed; recovery: retain its terminal destroy receipt"
                    .into(),
            ));
                    }
                    Ok(())
                },
            )
            .await?
        {
            Admission::Replay(original) => return Ok(original),
            Admission::Original(head) => head,
        };
        if value.state != target {
            value.state = match target {
                MachineState::Running => MachineState::Waking,
                MachineState::Suspended => MachineState::Suspending,
                _ => MachineState::Destroying,
            };
            let physical = if target == MachineState::Running {
                self.backend.wake().await
            } else {
                self.backend.stop().await
            };
            physical.map_err(|_| ProviderError::Indeterminate(key))?;
            value.state = target;
            value.changed_at_unix_ms = now()?;
        }
        self.journal
            .finish(key, digest, admitted, &value, outcome)
            .await
    }
}

#[async_trait]
impl<B: NativeMachine> MachinesProvider for AdmittedMachine<B> {
    fn assurance(&self) -> ProviderAssurance {
        ProviderAssurance::CustomerHosted
    }
    async fn exec(
        &self,
        machine: MachineId,
        command: ExecCommand,
    ) -> Result<ExecStream, ProviderError> {
        let value = self.observation.lock().await;
        Self::select(&value, machine)?;
        if value.state != MachineState::Running {
            return Err(ProviderError::Conflict(
                "Machine is not running; recovery: wake the original incarnation before executing"
                    .into(),
            ));
        }
        self.backend.exec(command).await
    }
    async fn read_file(
        &self,
        machine: MachineId,
        access: FileAccess,
        limit: u32,
    ) -> Result<FileRead, ProviderError> {
        let value = self.observation.lock().await;
        Self::select(&value, machine)?;
        if value.state == MachineState::Destroyed {
            return Err(ProviderError::NotFound(machine.to_string()));
        }
        drop(value);
        self.backend.read(access, limit).await
    }
    async fn write_file(
        &self,
        machine: MachineId,
        access: FileAccess,
        data: Vec<u8>,
        truncate: bool,
    ) -> Result<FileWrite, ProviderError> {
        let value = self.observation.lock().await;
        Self::select(&value, machine)?;
        if value.state != MachineState::Running {
            return Err(ProviderError::Conflict(
                "Machine is not running; recovery: wake the original incarnation before writing"
                    .into(),
            ));
        }
        drop(value);
        self.backend.write(access, data, truncate).await
    }
    async fn inspect_machine(
        &self,
        machine: MachineId,
    ) -> Result<MachineObservation, ProviderError> {
        let value = self.observation.lock().await;
        Self::select(&value, machine)?;
        Ok(value.clone())
    }
    async fn list_machines(
        &self,
        after: Option<MachineId>,
        limit: u32,
    ) -> Result<MachinePage, ProviderError> {
        if limit == 0 {
            return Err(ProviderError::Invalid(
                "empty Machine page credit; recovery: supply positive credit".into(),
            ));
        }
        let value = self.observation.lock().await;
        Ok(MachinePage {
            machines: if after.is_none_or(|after| after < value.id) {
                vec![value.clone()]
            } else {
                Vec::new()
            },
            next: None,
        })
    }
    async fn qualify_image(&self, image: Image) -> Result<ImageQualification, ProviderError> {
        let value = self.observation.lock().await;
        if image != value.contract.image {
            return Err(unsupported("replace the admitted host image"));
        }
        Ok(ImageQualification {
            image,
            capabilities: value.contract.capabilities.clone(),
            compatibility_revision: value.contract.compatibility_revision,
        })
    }
    async fn create(&self, request: CreateMachine) -> Result<MutationOutcome, ProviderError> {
        let value = self.observation.lock().await;
        let digest = intent_digest(&(value.id, &Intent::Create(&request)))?;
        // The local factory enrolls an existing workspace. It never fabricates
        // another filesystem view or an immutable image capture.
        if request
            .image
            .as_ref()
            .is_some_and(|image| image != &value.contract.image)
            || request.selected_volume_ref != value.contract.selected_volume_ref
            || request.network_policy_digest != value.contract.network_policy_digest
            || request.suspension != value.contract.suspension
            || request.expiration != value.contract.expiration
            || request.budgets != value.contract.budgets
            || request.compatibility != value.contract.compatibility
        {
            return Err(unsupported("change the enrolled workspace contract"));
        }
        match self
            .journal
            .begin(request.idempotency_key, digest, None, || {
                settled(&value, request.idempotency_key)
            })
            .await?
        {
            Admission::Replay(original) => Ok(original),
            Admission::Original(head) => {
                self.journal
                    .finish(
                        request.idempotency_key,
                        digest,
                        head,
                        &value,
                        MutationOutcome::Created(value.clone()),
                    )
                    .await
            }
        }
    }
    async fn attach_volume(
        &self,
        machine: MachineId,
        volume: wire::selection::VolumeRef,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        let value = self.observation.lock().await;
        Self::select(&value, machine)?;
        if value.contract.selected_volume_ref.as_ref() != Some(&volume) {
            return Err(unsupported("replace the admitted selected volume"));
        }
        let digest = intent_digest(&(value.id, &Intent::Attach(machine, &volume)))?;
        match self
            .journal
            .begin(key, digest, None, || settled(&value, key))
            .await?
        {
            Admission::Replay(original) => Ok(original),
            Admission::Original(head) => {
                self.journal
                    .finish(
                        key,
                        digest,
                        head,
                        &value,
                        MutationOutcome::VolumeAttached(value.clone()),
                    )
                    .await
            }
        }
    }
    async fn suspend(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.change(machine, key, MachineState::Suspended).await
    }
    async fn wake(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.change(machine, key, MachineState::Running).await
    }
    async fn destroy_machine(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.change(machine, key, MachineState::Destroyed).await
    }
    async fn set_suspension_policy(
        &self,
        machine: MachineId,
        policy: SuspensionPolicy,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        let value = self.observation.lock().await;
        Self::select(&value, machine)?;
        if policy != value.contract.suspension {
            return Err(unsupported("install an automatic suspension producer"));
        }
        let digest = intent_digest(&(value.id, &Intent::Policy(machine, policy)))?;
        match self
            .journal
            .begin(key, digest, None, || settled(&value, key))
            .await?
        {
            Admission::Replay(original) => Ok(original),
            Admission::Original(head) => {
                self.journal
                    .finish(
                        key,
                        digest,
                        head,
                        &value,
                        MutationOutcome::SuspensionPolicySet(machine, policy),
                    )
                    .await
            }
        }
    }
    async fn events(
        &self,
        machine: MachineId,
        after: Option<u64>,
        limit: u32,
    ) -> Result<EventPage, ProviderError> {
        Self::select(&*self.observation.lock().await, machine)?;
        self.journal.events(after, limit).await
    }
    async fn recover(&self, key: IdempotencyKey) -> Result<MutationOutcome, ProviderError> {
        self.journal.recover(key).await
    }
    async fn recover_operation(&self, key: IdempotencyKey) -> Result<OperationId, ProviderError> {
        let operation = OperationId(key.0);
        self.journal.inspect(operation).await?;
        Ok(operation)
    }
    async fn inspect_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationObservation, ProviderError> {
        self.journal.inspect(operation).await
    }
    async fn cancel(&self, operation: OperationId) -> Result<OperationObservation, ProviderError> {
        self.journal.inspect(operation).await
    }
    async fn watch_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationStream, ProviderError> {
        self.journal.watch(operation).await
    }
    async fn read_output(
        &self,
        _: MachineId,
        _: u64,
        _: u32,
    ) -> Result<MachineOutput, ProviderError> {
        Err(unsupported("replay an unretained console"))
    }
    async fn checkpoint(
        &self,
        _: MachineId,
        _: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        Err(unsupported("snapshot a mutable host workspace"))
    }
    async fn inspect_checkpoint(
        &self,
        _: CheckpointId,
    ) -> Result<CheckpointObservation, ProviderError> {
        Err(unsupported("inspect an uncreated snapshot"))
    }
    async fn fork(
        &self,
        _: CheckpointId,
        _: NonZeroU32,
        _: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        Err(unsupported(
            "fork without an immutable filesystem generation",
        ))
    }
    async fn destroy_checkpoint(
        &self,
        _: CheckpointId,
        _: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        Err(unsupported("destroy an uncreated snapshot"))
    }
    async fn usage(&self, _: MachineId, _: u64, _: u64) -> Result<UsageReceipt, ProviderError> {
        Err(unsupported("attest unmeasured host usage"))
    }
}

fn settled(value: &MachineObservation, key: IdempotencyKey) -> Result<(), ProviderError> {
    match value.state {
        MachineState::Running | MachineState::Suspended => Ok(()),
        MachineState::Destroyed => Err(ProviderError::Conflict(
            "Machine incarnation is destroyed; recovery: retain its original terminal receipt"
                .into(),
        )),
        _ => Err(ProviderError::Indeterminate(key)),
    }
}
fn unsupported(action: &str) -> ProviderError {
    ProviderError::Unsupported(format!(
        "local Machine cannot {action}; recovery: use the original admitted workspace or a provider with that primitive"
    ))
}

fn now() -> Result<u64, ProviderError> {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| ProviderError::Unavailable)?
            .as_millis(),
    )
    .map_err(|_| ProviderError::Unavailable)
}
