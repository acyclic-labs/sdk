use super::*;

/// Optional image/runtime capability.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Capability {
    ElasticCpu,
    ElasticMemory,
    LiveCheckpoint,
    LiveFork,
    SuspendResume,
    LiveMovement,
    /// [`MachinesProvider::fork_machine`] copies a running machine's persistent disk, but not its memory or processes.
    DiskFork,
}

/// What the children of one [`MachinesProvider::fork_machine`] inherited from their source.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum ForkFidelity {
    /// Children resume from the source's memory, processes, and disk at the fork instant.
    MemoryAndDisk,
    /// Children boot fresh over a copy of the source's persistent disk (provider-defined; see [`Capability::DiskFork`]) taken at one consistent instant.
    DiskOnly,
}

impl ForkFidelity {
    /// The fidelity a machine with `capabilities` forks at, or `None` when it cannot be forked live at all and the caller must fall back to checkpoint fork or a restart.
    pub fn for_capabilities(capabilities: &BTreeSet<Capability>) -> Option<Self> {
        if capabilities.contains(&Capability::LiveFork) {
            Some(Self::MemoryAndDisk)
        } else if capabilities.contains(&Capability::DiskFork) {
            Some(Self::DiskOnly)
        } else {
            None
        }
    }
}

/// Capability admission rule.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CompatibilityPolicy {
    /// Enable only the exact proven set.
    BestEffort,
    /// Reject unless every requested capability is proven.
    Require(BTreeSet<Capability>),
}

/// Automatic suspension policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SuspensionPolicy {
    Manual,
    AfterIdle(Duration),
}

/// Automatic destruction policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ExpirationPolicy {
    Never,
    MaxAge(Duration),
    AtUnixMs(u64),
    Idle(Duration),
}

/// Optional customer cost and concurrency limits.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Budgets {
    /// Spend ceiling in integer micros; zero delegates to account policy.
    pub spend_micros: u64,
    /// Concurrent-machine ceiling; zero delegates to account policy.
    pub concurrency: u32,
}

/// Shape-free create request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateMachine {
    /// Caller-retained mutation identity.
    pub idempotency_key: IdempotencyKey,
    /// Immutable image.
    pub image: Image,
    /// Capability policy.
    pub compatibility: CompatibilityPolicy,
    /// Suspension policy.
    pub suspension: SuspensionPolicy,
    /// Expiration policy.
    pub expiration: ExpirationPolicy,
    /// Commitment to a separately authorized network policy.
    pub network_policy_digest: [u8; 32],
    /// Cost and concurrency limits.
    pub budgets: Budgets,
}

impl CreateMachine {
    /// Creates a best-effort request with 15-second idle suspension and no expiry.
    pub fn new(
        idempotency_key: IdempotencyKey,
        image: Image,
        network_policy_digest: [u8; 32],
    ) -> Self {
        Self {
            idempotency_key,
            image,
            compatibility: CompatibilityPolicy::BestEffort,
            suspension: SuspensionPolicy::AfterIdle(DEFAULT_IDLE_SUSPEND),
            expiration: ExpirationPolicy::Never,
            network_policy_digest,
            budgets: Budgets::default(),
        }
    }
}

/// Exact public contract retained for one machine.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MachineContract {
    /// Immutable source image.
    pub image: Image,
    /// Exact enabled capabilities.
    pub capabilities: BTreeSet<Capability>,
    /// Original capability policy.
    pub compatibility: CompatibilityPolicy,
    /// Opaque customer compatibility revision.
    pub compatibility_revision: [u8; 32],
    /// Suspension policy.
    pub suspension: SuspensionPolicy,
    /// Expiration policy.
    pub expiration: ExpirationPolicy,
    /// Network-policy commitment.
    pub network_policy_digest: [u8; 32],
    /// Cost and concurrency limits.
    pub budgets: Budgets,
}

impl MachineContract {
    /// The fidelity [`MachinesProvider::fork_machine`] offers for a machine under this contract; `None` means live fork is unsupported for it.
    pub fn fork_fidelity(&self) -> Option<ForkFidelity> {
        ForkFidelity::for_capabilities(&self.capabilities)
    }
}

/// Qualification result for one immutable image.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ImageQualification {
    /// Qualified image.
    pub image: Image,
    /// Exact enabled capabilities.
    pub capabilities: BTreeSet<Capability>,
    /// Opaque customer compatibility revision.
    pub compatibility_revision: [u8; 32],
}
