use super::*;

/// Public provider interface shared by deterministic and managed implementations.
#[async_trait]
pub trait MachinesProvider: Send + Sync {
    /// Describes the provider's actual security/durability boundary.
    fn assurance(&self) -> ProviderAssurance;
    async fn qualify_image(&self, image: Image) -> Result<ImageQualification, ProviderError>;
    async fn create(&self, request: CreateMachine) -> Result<MutationOutcome, ProviderError>;
    async fn inspect_machine(
        &self,
        machine: MachineId,
    ) -> Result<MachineObservation, ProviderError>;
    async fn list_machines(
        &self,
        after: Option<MachineId>,
        limit: u32,
    ) -> Result<MachinePage, ProviderError>;
    async fn checkpoint(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError>;
    async fn inspect_checkpoint(
        &self,
        checkpoint: CheckpointId,
    ) -> Result<CheckpointObservation, ProviderError>;
    async fn fork(
        &self,
        checkpoint: CheckpointId,
        count: NonZeroU32,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError>;
    /// Forks a running machine into `count` fresh children without an intermediate checkpoint, idempotently under `key`.
    async fn fork_machine(
        &self,
        machine: MachineId,
        count: NonZeroU32,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        let _ = (machine, count, key);
        Err(ProviderError::Unsupported(
            "this Machines provider does not support live fork".into(),
        ))
    }
    async fn suspend(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError>;
    async fn wake(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError>;
    async fn set_suspension_policy(
        &self,
        machine: MachineId,
        policy: SuspensionPolicy,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError>;
    async fn destroy_machine(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError>;
    async fn destroy_checkpoint(
        &self,
        checkpoint: CheckpointId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError>;
    async fn events(
        &self,
        machine: MachineId,
        after_sequence: Option<u64>,
        limit: u32,
    ) -> Result<EventPage, ProviderError>;
    async fn usage(
        &self,
        machine: MachineId,
        start_unix_ms: u64,
        end_unix_ms: u64,
    ) -> Result<UsageReceipt, ProviderError>;
    async fn recover(&self, key: IdempotencyKey) -> Result<MutationOutcome, ProviderError>;
    async fn recover_operation(&self, key: IdempotencyKey) -> Result<OperationId, ProviderError>;
    async fn inspect_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationObservation, ProviderError>;
    async fn cancel(&self, operation: OperationId) -> Result<OperationObservation, ProviderError>;
    async fn watch_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationStream, ProviderError>;
}

/// Canonical digest of a mutation intent, binding an idempotency key to it.
pub fn intent_digest<T: Serialize>(intent: &T) -> Result<[u8; 32], ProviderError> {
    let encoded = serde_json::to_vec(intent)
        .map_err(|error| ProviderError::Invalid(format!("intent cannot be encoded: {error}")))?;
    Ok(Sha256::digest(encoded).into())
}
