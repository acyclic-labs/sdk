//! One public Machines implementation over raw native primitives.
use super::*;
use std::sync::{Mutex, PoisonError};

/// Composition state; operation authority always remains the supplied Streams registry.
/// Staged boot credentials are ephemeral input, never an outcome/recovery cache.
pub struct BackendCore {
    operations: OperationRegistry,
    poll_interval: Duration,
    ready_timeout: Duration,
    assurance: ProviderAssurance,
    console: ConsoleHistory,
    boot_env: Mutex<BTreeMap<IdempotencyKey, BTreeMap<String, String>>>,
}

impl std::fmt::Debug for BackendCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let staged = self.boot_env.lock().unwrap_or_else(PoisonError::into_inner);
        let names: BTreeSet<&str> = staged
            .values()
            .flat_map(|env| env.keys().map(String::as_str))
            .collect();
        f.debug_struct("BackendCore")
            .field("operations", &self.operations)
            .field("staged_boot_envs", &staged.len())
            .field("staged_boot_env_names", &names)
            .finish_non_exhaustive()
    }
}

impl BackendCore {
    pub fn new(
        operations: OperationRegistry,
        poll_interval: Duration,
        ready_timeout: Duration,
        assurance: ProviderAssurance,
    ) -> Result<Self, ProviderError> {
        if poll_interval.is_zero() || ready_timeout.is_zero() {
            return Err(ProviderError::Invalid("Machines readiness admission failed; recovery: configure positive poll_interval and ready_timeout".into()));
        }
        Ok(Self {
            operations,
            poll_interval,
            ready_timeout,
            assurance,
            console: ConsoleHistory::default(),
            boot_env: Mutex::default(),
        })
    }
    pub fn operations(&self) -> &OperationRegistry {
        &self.operations
    }
    /// Ephemeral observed output, never an operation recovery source.
    pub fn console(&self) -> &ConsoleHistory {
        &self.console
    }
    /// Stage input for one create; every completed create attempt clears it.
    pub fn stage_boot_env(&self, key: IdempotencyKey, env: BTreeMap<String, String>) {
        self.boot_env
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(key, env);
    }
    pub fn staged_boot_env(&self, key: IdempotencyKey) -> Option<BTreeMap<String, String>> {
        self.boot_env
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&key)
            .cloned()
    }
    fn clear_boot_env(&self, key: IdempotencyKey) {
        self.boot_env
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&key);
    }
}

pub struct RawUsage {
    pub observation: MachineObservation,
    pub receipt: UsageReceipt,
}

/// Native wire reads and composition inputs. All lifecycle decisions and public
/// Machines methods have a single implementation below. A raw result is not a settlement.
#[async_trait]
pub trait Backend: RawMachines {
    fn core(&self) -> &BackendCore;
    fn now_unix_ms(&self) -> u64;
    async fn raw_qualify_image(&self, image: Image) -> Result<ImageQualification, ProviderError>;
    async fn raw_list(&self) -> Result<Vec<MachineObservation>, ProviderError>;
    /// Prepare a lazy command session without dispatching IO. Core registers its
    /// cancellation and WallAlarm owner before polling the first request.
    fn raw_exec(
        &self,
        machine: MachineId,
        native: Option<&NativeBinding>,
        command: ExecCommand,
    ) -> Result<ExecSession, ProviderError>;
    async fn raw_read_output(
        &self,
        machine: MachineId,
        native: Option<&NativeBinding>,
        offset: u64,
        limit: u32,
    ) -> Result<MachineOutput, ProviderError>;
    async fn raw_usage(
        &self,
        machine: MachineId,
        native: Option<&NativeBinding>,
        known: Option<&MachineObservation>,
        start_unix_ms: u64,
        end_unix_ms: u64,
    ) -> Result<RawUsage, ProviderError>;
}

#[async_trait]
impl<D: Backend> MachinesProvider for D {
    fn assurance(&self) -> ProviderAssurance {
        self.core().assurance
    }
    async fn exec(
        &self,
        machine: MachineId,
        command: ExecCommand,
    ) -> Result<ExecStream, ProviderError> {
        let (known, native) = self
            .core()
            .operations
            .bound_machine(machine, self.name())
            .await?;
        let session = self.raw_exec(machine, native.as_ref(), command.clone())?;
        let known = known.ok_or_else(|| ProviderError::NotFound(machine.to_string()))?;
        if known.state != MachineState::Running {
            return Err(ProviderError::Conflict(
                "guest command requires a running machine; recovery: inspect and wake the machine before dispatch".into(),
            ));
        }
        // Idle expiration is enforced by the native provider's activity policy;
        // fixed deadlines also bound this command's shared wall-clock owner.
        let expiry = match known.contract.expiration {
            ExpirationPolicy::Idle(_) => None,
            policy => expiration_deadline(policy, known.created_at_unix_ms)?,
        };
        owned_exec(
            machine,
            &command,
            expiry,
            self.core().console.clone(),
            session,
        )
    }
    async fn read_output(
        &self,
        machine: MachineId,
        offset: u64,
        limit: u32,
    ) -> Result<MachineOutput, ProviderError> {
        if limit == 0 || limit > MAX_OUTPUT_READ_BYTES {
            return Err(ProviderError::Invalid(
                "console read admission failed; recovery: request 1..65536 bytes".into(),
            ));
        }
        let native = self
            .core()
            .operations
            .native_binding(machine, self.name())
            .await?;
        self.raw_read_output(machine, native.as_ref(), offset, limit)
            .await
    }
    async fn qualify_image(&self, image: Image) -> Result<ImageQualification, ProviderError> {
        self.raw_qualify_image(image).await
    }

    async fn create(&self, request: CreateMachine) -> Result<MutationOutcome, ProviderError> {
        let key = request.idempotency_key;
        let outcome = self
            .core()
            .operations
            .create_machine(
                self,
                request,
                self.core().poll_interval,
                self.core().ready_timeout,
            )
            .await;
        self.core().clear_boot_env(key);
        outcome.map(|settled| settled.outcome)
    }

    async fn inspect_machine(
        &self,
        machine: MachineId,
    ) -> Result<MachineObservation, ProviderError> {
        self.core()
            .operations
            .inspect_machine(self, machine, self.now_unix_ms())
            .await
    }

    async fn list_machines(
        &self,
        after: Option<MachineId>,
        limit: u32,
    ) -> Result<MachinePage, ProviderError> {
        if limit == 0 || limit > MAX_PAGE_SIZE {
            return Err(ProviderError::Invalid(
                "machine page limit must be 1..=256".into(),
            ));
        }
        for observation in self.raw_list().await? {
            self.core()
                .operations
                .observe_machine(observation, self.now_unix_ms())
                .await?;
        }
        self.core().operations.machine_page(after, limit).await
    }

    async fn checkpoint(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.core()
            .operations
            .create_checkpoint(
                self,
                machine,
                key,
                self.core().poll_interval,
                self.core().ready_timeout,
                self.now_unix_ms(),
            )
            .await
            .map(|settled| settled.outcome)
    }

    async fn inspect_checkpoint(
        &self,
        checkpoint: CheckpointId,
    ) -> Result<CheckpointObservation, ProviderError> {
        self.core()
            .operations
            .inspect_checkpoint(self, checkpoint)
            .await
    }

    async fn fork(
        &self,
        checkpoint: CheckpointId,
        count: NonZeroU32,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.core()
            .operations
            .fork_checkpoint(
                self,
                checkpoint,
                count,
                key,
                self.core().poll_interval,
                self.core().ready_timeout,
            )
            .await
            .map(|settled| settled.outcome)
    }

    async fn fork_machine(
        &self,
        machine: MachineId,
        count: NonZeroU32,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.core()
            .operations
            .fork_machine(
                self,
                machine,
                count,
                key,
                self.core().poll_interval,
                self.core().ready_timeout,
                self.now_unix_ms(),
            )
            .await
            .map(|settled| settled.outcome)
    }

    async fn suspend(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.core()
            .operations
            .transition(
                self,
                machine,
                key,
                false,
                self.core().poll_interval,
                self.core().ready_timeout,
                self.now_unix_ms(),
            )
            .await
            .map(|settled| settled.outcome)
    }

    async fn wake(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.core()
            .operations
            .transition(
                self,
                machine,
                key,
                true,
                self.core().poll_interval,
                self.core().ready_timeout,
                self.now_unix_ms(),
            )
            .await
            .map(|settled| settled.outcome)
    }

    async fn set_suspension_policy(
        &self,
        machine: MachineId,
        policy: SuspensionPolicy,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.core()
            .operations
            .set_suspension_policy(self, machine, policy, key, self.now_unix_ms())
            .await
            .map(|settled| settled.outcome)
    }

    async fn destroy_machine(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.core()
            .operations
            .destroy_machine(self, machine, key, self.now_unix_ms())
            .await
            .map(|settled| settled.outcome)
    }

    async fn destroy_checkpoint(
        &self,
        checkpoint: CheckpointId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.core()
            .operations
            .destroy_checkpoint(self, checkpoint, key)
            .await
            .map(|settled| settled.outcome)
    }

    async fn events(
        &self,
        machine: MachineId,
        after_sequence: Option<u64>,
        limit: u32,
    ) -> Result<EventPage, ProviderError> {
        self.core()
            .operations
            .event_page(self, machine, after_sequence, limit, self.now_unix_ms())
            .await
    }

    async fn usage(
        &self,
        machine: MachineId,
        start_unix_ms: u64,
        end_unix_ms: u64,
    ) -> Result<UsageReceipt, ProviderError> {
        if start_unix_ms >= end_unix_ms {
            return Err(ProviderError::Invalid(
                "usage admission failed; recovery: request a non-empty interval".into(),
            ));
        }
        let (known, native) = self
            .core()
            .operations
            .bound_machine(machine, self.name())
            .await?;
        let usage = self
            .raw_usage(
                machine,
                native.as_ref(),
                known.as_ref(),
                start_unix_ms,
                end_unix_ms,
            )
            .await?;
        if usage.observation.id != machine
            || usage.receipt.machine != machine
            || usage.receipt.start_unix_ms != start_unix_ms
            || usage.receipt.end_unix_ms != end_unix_ms
        {
            return Err(ProviderError::Rejected("native usage identity mismatch; recovery: repair the provider mapping before retrying".into()));
        }
        self.core()
            .operations
            .observe_machine(usage.observation, self.now_unix_ms())
            .await?;
        Ok(usage.receipt)
    }

    async fn recover(&self, key: IdempotencyKey) -> Result<MutationOutcome, ProviderError> {
        Ok(self.core().operations.settled(key).await?.outcome)
    }

    async fn recover_operation(&self, key: IdempotencyKey) -> Result<OperationId, ProviderError> {
        self.core()
            .operations
            .record_for_key(key)
            .await?
            .map(|record| record.id)
            .ok_or_else(|| ProviderError::NotFound(key.to_string()))
    }

    async fn inspect_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationObservation, ProviderError> {
        self.core()
            .operations
            .inspect(operation)
            .await?
            .ok_or_else(|| ProviderError::NotFound(operation.to_string()))
    }

    async fn cancel(&self, operation: OperationId) -> Result<OperationObservation, ProviderError> {
        self.core().operations.cancel_with(self, operation).await
    }

    /// Current and future observations follow the canonical Streams cursor.
    async fn watch_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationStream, ProviderError> {
        self.core().operations.watch(operation).await
    }
}
