use super::*;

/// Public provider interface shared by deterministic and managed implementations.
/// Native providers retain thread-safe futures; browser providers retain their
/// original event-loop ownership through the shared primitive platform bound.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait MachinesProvider: acyclic_stream::ProviderPlatform {
    /// Describes the provider's actual security/durability boundary.
    fn assurance(&self) -> ProviderAssurance;
    /// Reads one native guest-file chunk, without shell execution or retries.
    async fn read_file(
        &self,
        machine: MachineId,
        access: FileAccess,
        limit: u32,
    ) -> Result<FileRead, ProviderError>;
    /// Writes one native chunk. Truncation is admitted only at offset zero.
    async fn write_file(
        &self,
        machine: MachineId,
        access: FileAccess,
        data: Vec<u8>,
        truncate: bool,
    ) -> Result<FileWrite, ProviderError>;
    /// Dispatches once. Lost replies and cancellation never authorize redispatch.
    async fn exec(
        &self,
        machine: MachineId,
        command: ExecCommand,
    ) -> Result<ExecStream, ProviderError>;
    /// Reads 1..65536 live console bytes without treating empty data as EOF.
    async fn read_output(
        &self,
        machine: MachineId,
        offset: u64,
        limit: u32,
    ) -> Result<MachineOutput, ProviderError>;

    async fn qualify_image(&self, image: Image) -> Result<ImageQualification, ProviderError>;
    async fn create(&self, request: CreateMachine) -> Result<MutationOutcome, ProviderError>;
    /// Admits the canonical selected volume under the original caller's scope
    /// and held physical incarnation. Settlement retains the original key.
    async fn attach_volume(
        &self,
        machine: MachineId,
        volume: wire::selection::VolumeRef,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError>;
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
    /// Forks a running or suspended machine into `count` fresh running children without an
    /// intermediate checkpoint, idempotently under `key`. A suspended source is its own
    /// checkpoint and stays suspended. Destroying a source refuses with
    /// [`Precondition::LiveForks`] while any of its forks lives.
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

mod client;
pub use client::*;
mod capture;
pub use capture::{CapturedExec, capture_exec};
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub(crate) mod admitted;
#[cfg(all(any(feature = "local", feature = "grpc"), not(target_arch = "wasm32")))]
pub(crate) mod deadline;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub(crate) mod journal;
