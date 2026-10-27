use super::*;

/// Public machine lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MachineState {
    Starting,
    Running,
    Suspending,
    Suspended,
    Waking,
    Destroying,
    Destroyed,
    Failed,
    Indeterminate,
}

/// Stable logical service endpoint.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Endpoint {
    pub name: String,
    pub uri: String,
}

/// Checked customer-visible machine observation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MachineObservation {
    pub id: MachineId,
    pub state: MachineState,
    pub contract: MachineContract,
    pub endpoints: Vec<Endpoint>,
    pub last_checkpoint: Option<CheckpointId>,
    pub created_at_unix_ms: u64,
    pub changed_at_unix_ms: u64,
}

/// Checked customer-visible checkpoint observation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CheckpointObservation {
    pub id: CheckpointId,
    pub source: MachineId,
    pub contract: MachineContract,
    pub forkable: bool,
    pub created_at_unix_ms: u64,
}

/// Bounded stable-cursor machine page.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MachinePage {
    pub machines: Vec<MachineObservation>,
    pub next: Option<MachineId>,
}

/// Customer-visible capacity pressure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Pressure {
    CustomerBudget,
    MachineLimit,
    ServiceSaturation,
}

/// Customer-visible event fact.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum EventFact {
    State(MachineState),
    Pressure(Pressure),
    CapacityChanged,
}

/// Ordered machine event.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MachineEvent {
    pub machine: MachineId,
    pub sequence: u64,
    pub observed_at_unix_ms: u64,
    pub fact: EventFact,
}

/// Bounded event page.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventPage {
    pub events: Vec<MachineEvent>,
    pub next_sequence: Option<u64>,
}

/// Immutable usage receipt over one half-open interval.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UsageReceipt {
    pub machine: MachineId,
    pub start_unix_ms: u64,
    pub end_unix_ms: u64,
    pub elastic_cpu_ns: u64,
    pub dedicated_cpu_ns: u64,
    pub private_resident_byte_seconds: u64,
    pub durable_private_bytes: u64,
    /// Commitment to the separately signed account-lineage storage receipt.
    pub lineage_receipt_sha256: [u8; 32],
    pub egress_bytes: u64,
    /// Provider-authenticated canonical receipt bytes; empty only for the in-memory provider.
    pub receipt: Vec<u8>,
}

/// Actual assurance supplied by a provider implementation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ProviderAssurance {
    /// Deterministic process-local behavior with no isolation or durability guarantee.
    ProcessLocalSimulation,
    /// Customer-hosted process execution with the provider's documented OS boundary.
    CustomerHosted,
    /// Acyclic-operated managed Machines service.
    ManagedService,
}

/// Terminal customer mutation outcome.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MutationOutcome {
    Created(MachineObservation),
    Checkpointed(CheckpointObservation),
    Forked(Vec<MachineObservation>),
    /// Outcome of [`MachinesProvider::fork_machine`]: the running source, the fidelity the children were forked at, and the children in index order.
    MachineForked {
        source: MachineId,
        fidelity: ForkFidelity,
        children: Vec<MachineObservation>,
    },
    Suspended(MachineId),
    Woken(MachineId),
    SuspensionPolicySet(MachineId, SuspensionPolicy),
    MachineDestroyed(MachineId),
    CheckpointDestroyed(CheckpointId),
}

/// Durable operation phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum OperationPhase {
    Pending,
    Succeeded,
    Cancelled,
    Indeterminate,
    Failed,
}

/// Operation observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OperationObservation {
    pub id: OperationId,
    pub phase: OperationPhase,
}

/// Bounded-memory stream of correlated operation observations.
pub type OperationStream =
    acyclic_stream::BoxProviderStream<'static, Result<OperationObservation, ProviderError>>;

/// One bounded guest command, dispatched once without an idempotency key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecCommand {
    pub(crate) argv: Vec<String>,
    pub(crate) timeout_ms: u32,
}

impl ExecCommand {
    /// Checks the protocol's argv and 1..60000 millisecond deadline bounds.
    pub fn new(argv: Vec<String>, timeout_ms: u32) -> Result<Self, ProviderError> {
        if argv.is_empty()
            || argv.len() > 64
            || argv.first().is_none_or(String::is_empty)
            || argv
                .iter()
                .any(|value| value.len() > 4096 || value.contains('\0'))
            || argv.iter().map(String::len).sum::<usize>() > 32768
            || !(1..=60000).contains(&timeout_ms)
        {
            return Err(ProviderError::Invalid("invalid guest command".into()));
        }
        Ok(Self { argv, timeout_ms })
    }

    #[must_use]
    pub fn argv(&self) -> &[String] {
        &self.argv
    }

    #[must_use]
    pub fn timeout_ms(&self) -> u32 {
        self.timeout_ms
    }
}

/// Merged stdout/stderr bytes or the single terminal guest exit code.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecOutput {
    Data(Vec<u8>),
    ExitCode(i32),
}

/// Disposable guest output view. Dropping it cancels the RPC, never retries it.
pub type ExecStream = acyclic_stream::BoxProviderStream<'static, Result<ExecOutput, ProviderError>>;

/// Bounded retained console bytes. A later offset exposes discarded bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MachineOutput {
    pub machine: MachineId,
    pub offset: u64,
    pub data: Vec<u8>,
}
