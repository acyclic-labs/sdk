use super::*;

pub(crate) mod events;

/// Public provider interface shared by deterministic and managed implementations.
/// Native providers retain thread-safe futures; browser providers retain their
/// original event-loop ownership through the shared primitive platform bound.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait MachinesProvider: acyclic_stream::ProviderPlatform {
    /// Describes the provider's actual security/durability boundary.
    fn assurance(&self) -> ProviderAssurance;
    /// Declares optional provider mechanisms without changing image capabilities.
    fn capability(&self, capability: ProviderCapability) -> Support {
        match capability {
            ProviderCapability::WholeFile | ProviderCapability::RequestResponseExec => {
                Support::Unsupported
            }
            _ => Support::Supported,
        }
    }
    /// One textual exchange with combined UTF-8 output, not binary channels.
    /// Missing terminal replies are indeterminate and never authorize redispatch.
    async fn exec_request(
        &self,
        machine: MachineId,
        command: ExecCommand,
    ) -> Result<TextExecResult, ProviderError> {
        let _ = (machine, command);
        unsupported("request/response exec")
    }
    /// Opens a lazy guest-file download under the original deadline. Dropping
    /// its stream cancels I/O. Range-only providers refuse this mechanism.
    async fn download_file(
        &self,
        machine: MachineId,
        access: FileAccess,
    ) -> Result<FileDownload, ProviderError> {
        let _ = (machine, access);
        unsupported("whole-file download")
    }
    /// Replaces one whole guest file, with no offset/short-write guarantee.
    async fn upload_file(
        &self,
        machine: MachineId,
        access: FileAccess,
        data: Vec<u8>,
    ) -> Result<(), ProviderError> {
        let _ = (machine, access, data);
        unsupported("whole-file upload")
    }
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
    /// Reads one live console chunk, normalizing larger positive hints to its
    /// physical width. Continue from the returned end; empty data is not EOF.
    async fn read_output(
        &self,
        machine: MachineId,
        offset: u64,
        limit: u32,
    ) -> Result<MachineOutput, ProviderError> {
        let _ = (machine, offset, limit);
        unsupported("read output")
    }

    async fn qualify_image(&self, image: Image) -> Result<ImageQualification, ProviderError>;
    async fn create(&self, request: CreateMachine) -> Result<MutationOutcome, ProviderError>;
    /// Admits the canonical selected volume under the original caller's scope
    /// and held physical incarnation. Settlement retains the original key.
    async fn attach_volume(
        &self,
        machine: MachineId,
        volume: wire::selection::VolumeRef,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        let _ = (machine, volume, key);
        unsupported("attach volume")
    }
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
    ) -> Result<MutationOutcome, ProviderError> {
        let _ = (machine, key);
        unsupported("checkpoint")
    }
    async fn inspect_checkpoint(
        &self,
        checkpoint: CheckpointId,
    ) -> Result<CheckpointObservation, ProviderError> {
        let _ = checkpoint;
        unsupported("inspect checkpoint")
    }
    async fn fork(
        &self,
        checkpoint: CheckpointId,
        count: NonZeroU32,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        let _ = (checkpoint, count, key);
        unsupported("fork")
    }
    /// Reads credited observations from the SAME original fork, including its
    /// partial children on failure. Every child is recorded at creation, before
    /// admission of the next child; terminal failure retains that exact cut.
    /// The cursor is metadata, not authority. Never reconstruct from inspections.
    async fn read_results(
        &self,
        cursor: crate::results::ResultCursor,
        credit: NonZeroU32,
    ) -> Result<crate::results::ResultPage, ProviderError> {
        let _ = (cursor, credit);
        unsupported("read results")
    }
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
    ) -> Result<MutationOutcome, ProviderError> {
        let _ = (machine, key);
        unsupported("suspend")
    }
    async fn wake(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        let _ = (machine, key);
        unsupported("wake")
    }
    async fn set_suspension_policy(
        &self,
        machine: MachineId,
        policy: SuspensionPolicy,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        let _ = (machine, policy, key);
        unsupported("set suspension policy")
    }
    async fn destroy_machine(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError>;
    async fn destroy_checkpoint(
        &self,
        checkpoint: CheckpointId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        let _ = (checkpoint, key);
        unsupported("destroy checkpoint")
    }
    async fn events(
        &self,
        machine: MachineId,
        after_sequence: Option<u64>,
        limit: u32,
    ) -> Result<EventPage, ProviderError> {
        let _ = (machine, after_sequence, limit);
        unsupported("events")
    }
    /// Replays retained events after `after_sequence`, then waits on the
    /// original publication producer. No inspections or periodic requests.
    /// A cursor past the retained tail or already past Destroyed is refused.
    async fn follow_events(
        &self,
        machine: MachineId,
        after_sequence: Option<u64>,
    ) -> Result<EventStream, ProviderError> {
        let _ = (machine, after_sequence);
        unsupported("follow events")
    }
    async fn usage(
        &self,
        machine: MachineId,
        start_unix_ms: u64,
        end_unix_ms: u64,
    ) -> Result<UsageReceipt, ProviderError> {
        let _ = (machine, start_unix_ms, end_unix_ms);
        unsupported("usage")
    }
    async fn recover(&self, key: IdempotencyKey) -> Result<MutationOutcome, ProviderError>;
    async fn recover_operation(&self, _key: IdempotencyKey) -> Result<OperationId, ProviderError> {
        unsupported("recover operation")
    }
    async fn inspect_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationObservation, ProviderError> {
        let _ = operation;
        unsupported("inspect operation")
    }
    async fn cancel(&self, _operation: OperationId) -> Result<OperationObservation, ProviderError> {
        unsupported("cancel")
    }
    async fn watch_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationStream, ProviderError> {
        let _ = operation;
        unsupported("watch operation")
    }
}

/// Canonical digest of a mutation intent, binding an idempotency key to it.
pub fn intent_digest<T: Serialize>(intent: &T) -> Result<[u8; 32], ProviderError> {
    let encoded = serde_json::to_vec(intent)
        .map_err(|error| ProviderError::Invalid(format!("intent cannot be encoded: {error}")))?;
    Ok(Sha256::digest(encoded).into())
}

/// Availability of an optional native mechanism.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Support {
    Supported,
    Unsupported,
}

/// Provider mechanisms, independently of image/runtime features.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderCapability {
    /// Separate binary stdout/stderr, stdin and terminal capture receipts.
    BinaryDuplexExec,
    /// One textual request with combined output and a terminal exit status.
    RequestResponseExec,
    RangeRead,
    RangeWrite,
    WholeFile,
    AttachVolume,
    Operations,
    Events,
    Usage,
    Checkpoint,
    SuspendResume,
    Destroy,
}

/// Terminal receipt of a provider's textual request/response execution API.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TextExecResult {
    /// Combined UTF-8 output; this mechanism does not preserve channel identity.
    pub output: String,
    /// Terminal exit status returned by the provider's execution API.
    pub exit_code: i32,
}

fn unsupported<T>(mechanism: &str) -> Result<T, ProviderError> {
    Err(ProviderError::Unsupported(format!(
        "provider has no {mechanism}; recovery: select a provider supporting this mechanism"
    )))
}

mod files;
pub use files::{read_file_at, write_file_at};
mod client;
pub use client::*;
mod capture;
pub use capture::{CapturedExec, capture_exec};
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod admitted;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod deadline;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod journal;
#[cfg(all(
    any(not(target_arch = "wasm32"), feature = "test-support"),
    not(target_arch = "wasm32")
))]
pub(crate) mod journal_codec;
