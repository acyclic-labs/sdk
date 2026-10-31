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
    /// Unreachable until its own device or guest reconnects; the provider cannot wake it.
    Offline,
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
    /// Settled original Machine contract with its exact selected volume.
    VolumeAttached(MachineObservation),
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

/// One guest command, dispatched once without an idempotency key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecCommand {
    pub(crate) argv: Vec<String>,
    pub(crate) timeout_ms: u64,
    pub(crate) cwd: Option<String>,
    pub(crate) stdin: Vec<u8>,
    pub(crate) expiry_unix_ms: Option<u64>,
    pub(crate) maximum_output_bytes: Option<u64>,
}

impl ExecCommand {
    /// Checks argv admission and a positive command deadline in milliseconds,
    /// which also bounds an implicit wake of an idle-suspended Machine.
    pub fn new(argv: Vec<String>, timeout_ms: u64) -> Result<Self, ProviderError> {
        if argv.is_empty()
            || argv.first().is_none_or(String::is_empty)
            || argv.iter().any(|value| value.contains('\0'))
            || timeout_ms == 0
        {
            return Err(ProviderError::Invalid("invalid guest command".into()));
        }
        Ok(Self {
            argv,
            timeout_ms,
            cwd: None,
            stdin: Vec::new(),
            expiry_unix_ms: None,
            maximum_output_bytes: None,
        })
    }

    /// Selects a guest working directory before dispatch.
    pub fn with_cwd(mut self, cwd: String) -> Result<Self, ProviderError> {
        if cwd.is_empty() || cwd.contains('\0') {
            return Err(ProviderError::Invalid(
                "guest cwd admission failed; recovery: supply a nonempty NUL-free directory".into(),
            ));
        }
        self.cwd = Some(cwd);
        Ok(self)
    }

    /// Supplies exact binary stdin followed by EOF, without environment variables.
    pub fn with_stdin(mut self, bytes: Vec<u8>) -> Self {
        self.stdin = bytes;
        self
    }

    /// Applies an authenticated authority's absolute expiration, including silence.
    pub fn with_expiry(mut self, expiry_unix_ms: u64) -> Self {
        self.expiry_unix_ms = Some(expiry_unix_ms);
        self
    }

    /// Bounds combined captured output. Zero allows no output bytes.
    pub fn with_output_limit(mut self, maximum_output_bytes: u64) -> Self {
        self.maximum_output_bytes = Some(maximum_output_bytes);
        self
    }

    pub fn cwd(&self) -> Option<&str> {
        self.cwd.as_deref()
    }
    pub fn stdin(&self) -> &[u8] {
        &self.stdin
    }
    pub fn expiry_unix_ms(&self) -> Option<u64> {
        self.expiry_unix_ms
    }
    pub fn maximum_output_bytes(&self) -> Option<u64> {
        self.maximum_output_bytes
    }

    #[must_use]
    pub fn argv(&self) -> &[String] {
        &self.argv
    }

    #[must_use]
    pub fn timeout_ms(&self) -> u64 {
        self.timeout_ms
    }
}

/// Exact stdout/stderr bytes followed by one terminal exit or stopped capture.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecOutput {
    Data(Vec<u8>),
    Stderr(Vec<u8>),
    ExitCode(i32),
    Stopped(ExecStop),
}

/// Why a guest command stopped before normal completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecStopKind {
    Timeout,
    Expired,
    OutputLimit,
    ControlStopped,
    CaptureFailed,
}

/// Original capture failure and whether the producer completed process cleanup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecStop {
    pub kind: ExecStopKind,
    pub message: String,
    pub cleanup_completed: bool,
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
