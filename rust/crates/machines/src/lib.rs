#![doc = include_str!("../README.md")]
#![allow(
    missing_docs,
    reason = "field-level wire semantics are canonical in proto/machines/v1/machines.proto"
)]

use async_trait::async_trait;
use futures::{Stream, StreamExt, stream};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    num::NonZeroU32,
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;
use uuid::Uuid;

#[cfg(feature = "grpc")]
mod grpc;
#[cfg(feature = "grpc")]
mod obs;
#[cfg(feature = "grpc")]
pub use grpc::{GrpcProvider, Tls};

/// Generated revision-one public transport. Service implementations consume this module;
#[doc(hidden)]
pub mod wire {
    #![allow(missing_docs, reason = "generated from the documented public schema")]
    #![allow(
        clippy::all,
        clippy::pedantic,
        clippy::allow_attributes_without_reason,
        reason = "generated protobuf bindings"
    )]
    include!("generated/acyclic.machines.v1.rs");
}

/// Canonical public descriptor set.
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("generated/acyclic-machines-v1.bin");
/// Current public protocol major.
pub const PROTOCOL_MAJOR: u32 = 1;
/// Current public protocol minor.
pub const PROTOCOL_MINOR: u32 = 1;
/// Maximum machines returned in one page.
pub const MAX_PAGE_SIZE: u32 = 256;
/// Largest bearer credential, in bytes, that an SDK client accepts. It matches the
pub const MAX_BEARER_TOKEN_BYTES: usize = 12 * 1024;
/// Maximum children admitted by one fork request.
pub const MAX_FORK_CHILDREN: u32 = 1_024;
/// Maximum events returned in one page.
pub const MAX_EVENT_PAGE_SIZE: u32 = 1_024;
/// Default automatic idle suspension delay.
pub const DEFAULT_IDLE_SUSPEND: Duration = Duration::from_secs(15);

macro_rules! uuid_identity {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(
            Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
        )]
        pub struct $name(Uuid);

        impl $name {
            /// Generates a fresh identity.
            #[must_use]
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }

            /// Parses a retained non-nil UUID.
            pub fn parse(value: &str) -> Result<Self, IdentityError> {
                let value = Uuid::parse_str(value).map_err(|_| IdentityError)?;
                if value.is_nil() {
                    return Err(IdentityError);
                }
                Ok(Self(value))
            }

            /// Derives a deterministic, never-nil identity from a domain-separated
            #[must_use]
            pub fn from_digest(digest: [u8; 32]) -> Self {
                let mut bytes = [0_u8; 16];
                for (byte, value) in bytes.iter_mut().zip(digest) {
                    *byte = value;
                }
                Self(uuid::Builder::from_sha1_bytes(bytes).into_uuid())
            }

            /// Returns the canonical 16-byte UUID representation.
            #[must_use]
            pub fn as_bytes(self) -> [u8; 16] {
                *self.0.as_bytes()
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

/// Invalid public identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("identity must be a non-nil UUID")]
pub struct IdentityError;

uuid_identity!(
    IdempotencyKey,
    "Caller-retained identity for exactly one mutation intent."
);
uuid_identity!(OperationId, "Address of one admitted operation.");
uuid_identity!(MachineId, "Stable logical machine identity.");
uuid_identity!(CheckpointId, "Stable immutable checkpoint identity.");

/// Validated nonzero immutable image digest.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "[u8; 32]", into = "[u8; 32]")]
pub struct ImageDigest([u8; 32]);

impl ImageDigest {
    /// Constructs a checked SHA-256 digest.
    pub fn new(value: [u8; 32]) -> Result<Self, ProviderError> {
        if value == [0; 32] {
            return Err(ProviderError::Invalid("image digest cannot be zero".into()));
        }
        Ok(Self(value))
    }

    /// Returns the exact digest bytes.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl TryFrom<[u8; 32]> for ImageDigest {
    type Error = ProviderError;

    fn try_from(value: [u8; 32]) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ImageDigest> for [u8; 32] {
    fn from(value: ImageDigest) -> Self {
        value.0
    }
}

/// Canonical immutable OCI digest separator.
pub const MANAGED_OCI_REFERENCE_SEPARATOR: &str = "@sha256:";
/// Canonical hexadecimal character count for a SHA-256 digest.
pub const MANAGED_OCI_DIGEST_HEX_LENGTH: usize = 64;
/// Hexadecimal characters accepted by the canonical OCI parser.
pub const MANAGED_OCI_HEX_DIGITS: &str = "0123456789abcdefABCDEF";
/// Character used to identify the all-zero digest rejected by the parser.
pub const MANAGED_OCI_ZERO_DIGIT: char = '0';

/// Immutable machine image selection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Image {
    /// Digest parsed from an immutable OCI reference.
    ManagedOci(ImageDigest),
    /// Customer image content digest.
    Custom(ImageDigest),
    /// Existing immutable checkpoint.
    Checkpoint(CheckpointId),
}

impl Image {
    /// Creates a managed OCI image reference after checking immutable digest syntax.
    pub fn oci(reference: impl Into<String>) -> Result<Self, ProviderError> {
        let reference = reference.into();
        let Some((name, digest)) = reference.rsplit_once(MANAGED_OCI_REFERENCE_SEPARATOR) else {
            return Err(ProviderError::Invalid(
                "OCI image must contain @sha256:<digest>".into(),
            ));
        };
        if name.is_empty() || digest.len() != MANAGED_OCI_DIGEST_HEX_LENGTH {
            return Err(ProviderError::Invalid("OCI image digest is invalid".into()));
        }
        let bytes = hex::decode(digest)
            .map_err(|_| ProviderError::Invalid("OCI image digest is invalid".into()))?;
        let digest: [u8; 32] = bytes
            .try_into()
            .map_err(|_| ProviderError::Invalid("OCI image digest is invalid".into()))?;
        ImageDigest::new(digest).map(Self::ManagedOci)
    }

    /// Creates a managed image from an already resolved digest.
    pub fn managed(digest: [u8; 32]) -> Result<Self, ProviderError> {
        ImageDigest::new(digest).map(Self::ManagedOci)
    }

    /// Creates a custom image from its content digest.
    pub fn custom(digest: [u8; 32]) -> Result<Self, ProviderError> {
        ImageDigest::new(digest).map(Self::Custom)
    }
}

/// Optional image/runtime capability.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Capability {
    ElasticCpu,
    ElasticMemory,
    LiveCheckpoint,
    LiveFork,
    SuspendResume,
    LiveMovement,
    /// [`MachinesProvider::fork_machine`] copies a running machine's persistent disk, but not
    DiskFork,
}

/// What the children of one [`MachinesProvider::fork_machine`] inherited from their source.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum ForkFidelity {
    /// Children resume from the source's memory, processes, and disk at the fork instant.
    MemoryAndDisk,
    /// Children boot fresh over a copy of the source's persistent disk (provider-defined; see
    DiskOnly,
}

impl ForkFidelity {
    /// The fidelity a machine with `capabilities` forks at, or `None` when it cannot be
    #[must_use]
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
    #[must_use]
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
    /// The fidelity [`MachinesProvider::fork_machine`] offers for a machine under this
    #[must_use]
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
    /// Outcome of [`MachinesProvider::fork_machine`]: the running source, the fidelity the
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
    Pin<Box<dyn Stream<Item = Result<OperationObservation, ProviderError>> + Send + 'static>>;

/// Provider-boundary failure.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProviderError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("rejected: {0}")]
    Rejected(String),
    #[error("Machines service is unavailable")]
    Unavailable,
    #[error("operation is indeterminate: {0}")]
    Indeterminate(IdempotencyKey),
    #[error("operation observation is indeterminate; inspect operation {0}")]
    OperationIndeterminate(OperationId),
    #[error("operation failed")]
    Failed,
    #[error("operation cancelled")]
    Cancelled,
}

impl ProviderError {
    /// Stable, message-free name of this failure class.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::NotFound(_) => "not_found",
            Self::Conflict(_) => "conflict",
            Self::Unsupported(_) => "unsupported",
            Self::Invalid(_) => "invalid",
            Self::Rejected(_) => "rejected",
            Self::Unavailable => "unavailable",
            Self::Indeterminate(_) => "indeterminate",
            Self::OperationIndeterminate(_) => "operation_indeterminate",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

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
    /// Forks a running machine into `count` fresh children without an intermediate
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

/// Creates one machine and checks the provider's outcome kind.
pub async fn create_machine(provider: &dyn MachinesProvider, request: CreateMachine) -> Result<MachineId, ProviderError> {
    match provider.create(request).await? {
        MutationOutcome::Created(value) => Ok(value.id),
        _ => Err(ProviderError::Rejected("provider returned the wrong create outcome".into())),
    }
}

/// Checkpoints one machine and checks the provider's outcome kind.
pub async fn checkpoint_machine(provider: &dyn MachinesProvider, machine: MachineId, key: IdempotencyKey) -> Result<CheckpointId, ProviderError> {
    match provider.checkpoint(machine, key).await? {
        MutationOutcome::Checkpointed(value) => Ok(value.id),
        _ => Err(ProviderError::Rejected("provider returned the wrong checkpoint outcome".into())),
    }
}

/// Children of one live fork and the fidelity they were forked at.
#[derive(Clone)]
pub struct MachineFork {
    pub fidelity: ForkFidelity,
    pub children: Vec<MachineId>,
}

/// Forks one running machine and checks the returned source identity.
pub async fn fork_machine(provider: &dyn MachinesProvider, machine: MachineId, count: NonZeroU32, key: IdempotencyKey) -> Result<MachineFork, ProviderError> {
    match provider.fork_machine(machine, count, key).await? {
        MutationOutcome::MachineForked { source, fidelity, children } if source == machine => Ok(MachineFork {
            fidelity,
            children: children.into_iter().map(|value| value.id).collect(),
        }),
        _ => Err(ProviderError::Rejected("provider returned the wrong fork outcome".into())),
    }
}

/// Suspends one machine and checks the returned identity.
pub async fn suspend_machine(provider: &dyn MachinesProvider, machine: MachineId, key: IdempotencyKey) -> Result<(), ProviderError> {
    match provider.suspend(machine, key).await? {
        MutationOutcome::Suspended(id) if id == machine => Ok(()),
        _ => Err(ProviderError::Rejected("provider returned the wrong mutation outcome".into())),
    }
}

/// Wakes one machine and checks the returned identity.
pub async fn wake_machine(provider: &dyn MachinesProvider, machine: MachineId, key: IdempotencyKey) -> Result<(), ProviderError> {
    match provider.wake(machine, key).await? {
        MutationOutcome::Woken(id) if id == machine => Ok(()),
        _ => Err(ProviderError::Rejected("provider returned the wrong mutation outcome".into())),
    }
}

/// Sets a machine's suspension policy and checks the returned identity and policy.
pub async fn set_suspension_policy(provider: &dyn MachinesProvider, machine: MachineId, policy: SuspensionPolicy, key: IdempotencyKey) -> Result<(), ProviderError> {
    match provider.set_suspension_policy(machine, policy, key).await? {
        MutationOutcome::SuspensionPolicySet(id, returned) if id == machine && returned == policy => Ok(()),
        _ => Err(ProviderError::Rejected("provider returned the wrong policy outcome".into())),
    }
}

/// Destroys one machine and checks the returned identity.
pub async fn destroy_machine(provider: &dyn MachinesProvider, machine: MachineId, key: IdempotencyKey) -> Result<(), ProviderError> {
    match provider.destroy_machine(machine, key).await? {
        MutationOutcome::MachineDestroyed(id) if id == machine => Ok(()),
        _ => Err(ProviderError::Rejected("provider returned the wrong mutation outcome".into())),
    }
}

/// Forks an immutable checkpoint and checks the provider's outcome kind.
pub async fn fork_checkpoint(provider: &dyn MachinesProvider, checkpoint: CheckpointId, count: NonZeroU32, key: IdempotencyKey) -> Result<Vec<MachineId>, ProviderError> {
    match provider.fork(checkpoint, count, key).await? {
        MutationOutcome::Forked(values) => Ok(values.into_iter().map(|value| value.id).collect()),
        _ => Err(ProviderError::Rejected("provider returned the wrong fork outcome".into())),
    }
}

/// Destroys one checkpoint and checks the returned identity.
pub async fn destroy_checkpoint(provider: &dyn MachinesProvider, checkpoint: CheckpointId, key: IdempotencyKey) -> Result<(), ProviderError> {
    match provider.destroy_checkpoint(checkpoint, key).await? {
        MutationOutcome::CheckpointDestroyed(id) if id == checkpoint => Ok(()),
        _ => Err(ProviderError::Rejected("provider returned the wrong checkpoint outcome".into())),
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
enum MemoryIntent {
    Create(CreateMachine),
    Checkpoint(MachineId),
    Fork(CheckpointId, u32),
    ForkMachine(MachineId, u32),
    Suspend(MachineId),
    Wake(MachineId),
    Policy(MachineId, SuspensionPolicy),
    DestroyMachine(MachineId),
    DestroyCheckpoint(CheckpointId),
}

#[derive(Clone)]
struct Replay {
    digest: [u8; 32],
    outcome: MutationOutcome,
}

struct MemoryState {
    now: u64,
    machines: BTreeMap<MachineId, MachineObservation>,
    checkpoints: BTreeMap<CheckpointId, CheckpointObservation>,
    events: BTreeMap<MachineId, Vec<MachineEvent>>,
    replays: BTreeMap<IdempotencyKey, Replay>,
    operations: BTreeMap<OperationId, OperationObservation>,
    /// Live-fork child to its source.
    fork_sources: BTreeMap<MachineId, MachineId>,
}

impl Default for MemoryState {
    fn default() -> Self {
        Self {
            now: 1,
            machines: BTreeMap::new(),
            checkpoints: BTreeMap::new(),
            events: BTreeMap::new(),
            replays: BTreeMap::new(),
            operations: BTreeMap::new(),
            fork_sources: BTreeMap::new(),
        }
    }
}

/// Deterministic bounded process-local state-machine provider.
#[derive(Clone)]
pub struct SimulatedMachines {
    state: Arc<Mutex<MemoryState>>,
    capabilities: BTreeSet<Capability>,
}

impl Default for SimulatedMachines {
    /// Declares every capability.
    fn default() -> Self {
        Self::with_capabilities(BTreeSet::from([
            Capability::ElasticCpu,
            Capability::ElasticMemory,
            Capability::LiveCheckpoint,
            Capability::LiveFork,
            Capability::SuspendResume,
            Capability::LiveMovement,
            Capability::DiskFork,
        ]))
    }
}

impl SimulatedMachines {
    /// Simulates a provider that qualifies images with exactly `capabilities`, for example
    #[must_use]
    pub fn with_capabilities(capabilities: BTreeSet<Capability>) -> Self {
        Self {
            state: Arc::default(),
            capabilities,
        }
    }
    fn revision() -> [u8; 32] {
        Sha256::digest(b"acyclic-machines-memory-v1").into()
    }
    fn operation(key: IdempotencyKey) -> OperationId {
        OperationId::from_digest(derived_digest(b"operation", key, 0))
    }
    fn machine(key: IdempotencyKey, index: u32) -> MachineId {
        MachineId::from_digest(derived_digest(b"machine", key, index))
    }
    fn checkpoint_id(key: IdempotencyKey) -> CheckpointId {
        CheckpointId::from_digest(derived_digest(b"checkpoint", key, 0))
    }
    fn contract(request: &CreateMachine, capabilities: BTreeSet<Capability>) -> MachineContract {
        MachineContract {
            image: request.image.clone(),
            capabilities,
            compatibility: request.compatibility.clone(),
            compatibility_revision: Self::revision(),
            suspension: request.suspension,
            expiration: request.expiration,
            network_policy_digest: request.network_policy_digest,
            budgets: request.budgets,
        }
    }

    async fn apply<F>(
        &self,
        key: IdempotencyKey,
        intent: MemoryIntent,
        action: F,
    ) -> Result<MutationOutcome, ProviderError>
    where
        F: FnOnce(&mut MemoryState) -> Result<MutationOutcome, ProviderError>,
    {
        let digest = intent_digest(&intent)?;
        let mut state = self.state.lock().await;
        if let Some(replay) = state.replays.get(&key) {
            return if replay.digest == digest {
                Ok(replay.outcome.clone())
            } else {
                Err(ProviderError::Conflict(
                    "idempotency key is bound to another intent".into(),
                ))
            };
        }
        if state.replays.len() >= 4_096 {
            return Err(ProviderError::Rejected(
                "simulation operation limit reached".into(),
            ));
        }
        let outcome = action(&mut state)?;
        let operation = Self::operation(key);
        state.operations.insert(
            operation,
            OperationObservation {
                id: operation,
                phase: OperationPhase::Succeeded,
            },
        );
        state.replays.insert(
            key,
            Replay {
                digest,
                outcome: outcome.clone(),
            },
        );
        Ok(outcome)
    }
}

fn derived_digest(domain: &[u8], key: IdempotencyKey, index: u32) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(key.as_bytes());
    hash.update(index.to_be_bytes());
    hash.finalize().into()
}

/// Canonical digest of a mutation intent, binding an idempotency key to it.
pub fn intent_digest<T: Serialize>(intent: &T) -> Result<[u8; 32], ProviderError> {
    let encoded = serde_json::to_vec(intent)
        .map_err(|error| ProviderError::Invalid(format!("intent cannot be encoded: {error}")))?;
    Ok(Sha256::digest(encoded).into())
}

fn tick(state: &mut MemoryState) -> Result<u64, ProviderError> {
    state.now = state
        .now
        .checked_add(1)
        .ok_or_else(|| ProviderError::Rejected("simulation clock exhausted".into()))?;
    Ok(state.now)
}

fn event(
    state: &mut MemoryState,
    machine: MachineId,
    fact: EventFact,
    observed_at: u64,
) -> Result<(), ProviderError> {
    let values = state.events.entry(machine).or_default();
    if values.len() >= 4_096 {
        return Err(ProviderError::Rejected(
            "simulation event limit reached".into(),
        ));
    }
    let sequence = u64::try_from(values.len())
        .map_err(|_| ProviderError::Rejected("simulation event limit reached".into()))?
        + 1;
    values.push(MachineEvent {
        machine,
        sequence,
        observed_at_unix_ms: observed_at,
        fact,
    });
    Ok(())
}

#[async_trait]
impl MachinesProvider for SimulatedMachines {
    fn assurance(&self) -> ProviderAssurance {
        ProviderAssurance::ProcessLocalSimulation
    }
    async fn qualify_image(&self, image: Image) -> Result<ImageQualification, ProviderError> {
        Ok(ImageQualification {
            image,
            capabilities: self.capabilities.clone(),
            compatibility_revision: Self::revision(),
        })
    }
    async fn create(&self, request: CreateMachine) -> Result<MutationOutcome, ProviderError> {
        let key = request.idempotency_key;
        let intent = MemoryIntent::Create(request.clone());
        let capabilities = self.capabilities.clone();
        self.apply(key, intent, move |state| {
            if let CompatibilityPolicy::Require(required) = &request.compatibility
                && (required.is_empty() || !required.is_subset(&capabilities))
            {
                return Err(ProviderError::Unsupported(
                    "required image capabilities are unavailable".into(),
                ));
            }
            if state.machines.len() >= 1_024 {
                return Err(ProviderError::Rejected(
                    "simulation machine limit reached".into(),
                ));
            }
            let id = Self::machine(key, 0);
            let now = tick(state)?;
            let contract = Self::contract(&request, capabilities);
            let observation = MachineObservation {
                id,
                state: MachineState::Running,
                contract,
                endpoints: vec![Endpoint {
                    name: "default".into(),
                    uri: format!("memory://{id}"),
                }],
                last_checkpoint: None,
                created_at_unix_ms: now,
                changed_at_unix_ms: now,
            };
            state.machines.insert(id, observation.clone());
            event(state, id, EventFact::State(MachineState::Running), now)?;
            Ok(MutationOutcome::Created(observation))
        })
        .await
    }
    async fn inspect_machine(
        &self,
        machine: MachineId,
    ) -> Result<MachineObservation, ProviderError> {
        self.state
            .lock()
            .await
            .machines
            .get(&machine)
            .cloned()
            .ok_or_else(|| ProviderError::NotFound(machine.to_string()))
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
        let state = self.state.lock().await;
        let mut values = state
            .machines
            .values()
            .filter(|value| after.is_none_or(|cursor| value.id > cursor))
            .take(
                usize::try_from(limit)
                    .map_err(|_| ProviderError::Invalid("invalid page limit".into()))?
                    + 1,
            )
            .cloned()
            .collect::<Vec<_>>();
        let next = if values.len()
            > usize::try_from(limit)
                .map_err(|_| ProviderError::Invalid("invalid page limit".into()))?
        {
            values.pop();
            values.last().map(|value| value.id)
        } else {
            None
        };
        Ok(MachinePage {
            machines: values,
            next,
        })
    }
    async fn checkpoint(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(key, MemoryIntent::Checkpoint(machine), move |state| {
            let source = state
                .machines
                .get(&machine)
                .cloned()
                .ok_or_else(|| ProviderError::NotFound(machine.to_string()))?;
            if !matches!(
                source.state,
                MachineState::Running | MachineState::Suspended
            ) {
                return Err(ProviderError::Conflict(
                    "machine cannot be checkpointed in its current state".into(),
                ));
            }
            let id = Self::checkpoint_id(key);
            let now = tick(state)?;
            let value = CheckpointObservation {
                id,
                source: machine,
                contract: source.contract,
                forkable: true,
                created_at_unix_ms: now,
            };
            state.checkpoints.insert(id, value.clone());
            if let Some(machine_value) = state.machines.get_mut(&machine) {
                machine_value.last_checkpoint = Some(id);
                machine_value.changed_at_unix_ms = now;
            }
            Ok(MutationOutcome::Checkpointed(value))
        })
        .await
    }
    async fn inspect_checkpoint(
        &self,
        checkpoint: CheckpointId,
    ) -> Result<CheckpointObservation, ProviderError> {
        self.state
            .lock()
            .await
            .checkpoints
            .get(&checkpoint)
            .cloned()
            .ok_or_else(|| ProviderError::NotFound(checkpoint.to_string()))
    }
    async fn fork(
        &self,
        checkpoint: CheckpointId,
        count: NonZeroU32,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        let count_value = count.get();
        if count_value > MAX_FORK_CHILDREN {
            return Err(ProviderError::Invalid("fork count exceeds 1024".into()));
        }
        self.apply(
            key,
            MemoryIntent::Fork(checkpoint, count_value),
            move |state| {
                let source = state
                    .checkpoints
                    .get(&checkpoint)
                    .cloned()
                    .ok_or_else(|| ProviderError::NotFound(checkpoint.to_string()))?;
                if !source.forkable {
                    return Err(ProviderError::Conflict(
                        "checkpoint no longer accepts forks".into(),
                    ));
                }
                let add = usize::try_from(count_value)
                    .map_err(|_| ProviderError::Invalid("invalid fork count".into()))?;
                if state.machines.len().saturating_add(add) > 1_024 {
                    return Err(ProviderError::Rejected(
                        "simulation machine limit reached".into(),
                    ));
                }
                let mut children = Vec::with_capacity(add);
                for index in 0..count_value {
                    let id = Self::machine(key, index);
                    let now = tick(state)?;
                    let mut contract = source.contract.clone();
                    contract.image = Image::Checkpoint(checkpoint);
                    let value = MachineObservation {
                        id,
                        state: MachineState::Running,
                        contract,
                        endpoints: vec![Endpoint {
                            name: "default".into(),
                            uri: format!("memory://{id}"),
                        }],
                        last_checkpoint: Some(checkpoint),
                        created_at_unix_ms: now,
                        changed_at_unix_ms: now,
                    };
                    state.machines.insert(id, value.clone());
                    event(state, id, EventFact::State(MachineState::Running), now)?;
                    children.push(value);
                }
                Ok(MutationOutcome::Forked(children))
            },
        )
        .await
    }
    async fn fork_machine(
        &self,
        machine: MachineId,
        count: NonZeroU32,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        let count_value = count.get();
        if count_value > MAX_FORK_CHILDREN {
            return Err(ProviderError::Invalid("fork count exceeds 1024".into()));
        }
        self.apply(
            key,
            MemoryIntent::ForkMachine(machine, count_value),
            move |state| {
                let source = state
                    .machines
                    .get(&machine)
                    .cloned()
                    .ok_or_else(|| ProviderError::NotFound(machine.to_string()))?;
                let fidelity = source.contract.fork_fidelity().ok_or_else(|| {
                    ProviderError::Unsupported("machine contract does not declare live fork".into())
                })?;
                if source.state != MachineState::Running {
                    return Err(ProviderError::Conflict(
                        "only a running machine can be forked".into(),
                    ));
                }
                let add = usize::try_from(count_value)
                    .map_err(|_| ProviderError::Invalid("invalid fork count".into()))?;
                if state.machines.len().saturating_add(add) > 1_024 {
                    return Err(ProviderError::Rejected(
                        "simulation machine limit reached".into(),
                    ));
                }
                let mut children = Vec::with_capacity(add);
                for index in 0..count_value {
                    let id = Self::machine(key, index);
                    let now = tick(state)?;
                    let value = MachineObservation {
                        id,
                        state: MachineState::Running,
                        contract: source.contract.clone(),
                        endpoints: vec![Endpoint {
                            name: "default".into(),
                            uri: format!("memory://{id}"),
                        }],
                        last_checkpoint: None,
                        created_at_unix_ms: now,
                        changed_at_unix_ms: now,
                    };
                    state.machines.insert(id, value.clone());
                    state.fork_sources.insert(id, machine);
                    event(state, id, EventFact::State(MachineState::Running), now)?;
                    children.push(value);
                }
                Ok(MutationOutcome::MachineForked {
                    source: machine,
                    fidelity,
                    children,
                })
            },
        )
        .await
    }
    async fn suspend(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(key, MemoryIntent::Suspend(machine), move |state| {
            transition(
                state,
                machine,
                MachineState::Running,
                MachineState::Suspended,
                MutationOutcome::Suspended(machine),
            )
        })
        .await
    }
    async fn wake(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(key, MemoryIntent::Wake(machine), move |state| {
            transition(
                state,
                machine,
                MachineState::Suspended,
                MachineState::Running,
                MutationOutcome::Woken(machine),
            )
        })
        .await
    }
    async fn set_suspension_policy(
        &self,
        machine: MachineId,
        policy: SuspensionPolicy,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(key, MemoryIntent::Policy(machine, policy), move |state| {
            let value = state
                .machines
                .get(&machine)
                .ok_or_else(|| ProviderError::NotFound(machine.to_string()))?;
            if value.state == MachineState::Destroyed {
                return Err(ProviderError::Conflict(
                    "destroyed machine cannot change policy".into(),
                ));
            }
            let now = tick(state)?;
            let value = state
                .machines
                .get_mut(&machine)
                .ok_or_else(|| ProviderError::NotFound(machine.to_string()))?;
            value.contract.suspension = policy;
            value.changed_at_unix_ms = now;
            Ok(MutationOutcome::SuspensionPolicySet(machine, policy))
        })
        .await
    }
    async fn destroy_machine(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(key, MemoryIntent::DestroyMachine(machine), move |state| {
            let current = state
                .machines
                .get(&machine)
                .ok_or_else(|| ProviderError::NotFound(machine.to_string()))?
                .state;
            if current == MachineState::Destroyed {
                return Ok(MutationOutcome::MachineDestroyed(machine));
            }
            let live_children = state.fork_sources.iter().any(|(child, source)| {
                *source == machine
                    && state
                        .machines
                        .get(child)
                        .is_some_and(|value| value.state != MachineState::Destroyed)
            });
            if live_children {
                return Err(ProviderError::Conflict(
                    "machine has live-fork children; destroy them first".into(),
                ));
            }
            let now = tick(state)?;
            let value = state
                .machines
                .get_mut(&machine)
                .ok_or_else(|| ProviderError::NotFound(machine.to_string()))?;
            value.state = MachineState::Destroyed;
            value.changed_at_unix_ms = now;
            event(
                state,
                machine,
                EventFact::State(MachineState::Destroyed),
                now,
            )?;
            Ok(MutationOutcome::MachineDestroyed(machine))
        })
        .await
    }
    async fn destroy_checkpoint(
        &self,
        checkpoint: CheckpointId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(
            key,
            MemoryIntent::DestroyCheckpoint(checkpoint),
            move |state| {
                let value = state
                    .checkpoints
                    .get_mut(&checkpoint)
                    .ok_or_else(|| ProviderError::NotFound(checkpoint.to_string()))?;
                value.forkable = false;
                Ok(MutationOutcome::CheckpointDestroyed(checkpoint))
            },
        )
        .await
    }
    async fn events(
        &self,
        machine: MachineId,
        after_sequence: Option<u64>,
        limit: u32,
    ) -> Result<EventPage, ProviderError> {
        if limit == 0 || limit > MAX_EVENT_PAGE_SIZE {
            return Err(ProviderError::Invalid(
                "event page limit must be 1..=1024".into(),
            ));
        }
        let state = self.state.lock().await;
        if !state.machines.contains_key(&machine) {
            return Err(ProviderError::NotFound(machine.to_string()));
        }
        let limit = usize::try_from(limit)
            .map_err(|_| ProviderError::Invalid("invalid event limit".into()))?;
        let mut values = state
            .events
            .get(&machine)
            .into_iter()
            .flatten()
            .filter(|value| after_sequence.is_none_or(|cursor| value.sequence > cursor))
            .take(limit + 1)
            .cloned()
            .collect::<Vec<_>>();
        let next_sequence = if values.len() > limit {
            values.pop();
            values.last().map(|value| value.sequence)
        } else {
            None
        };
        Ok(EventPage {
            events: values,
            next_sequence,
        })
    }
    async fn usage(
        &self,
        machine: MachineId,
        start_unix_ms: u64,
        end_unix_ms: u64,
    ) -> Result<UsageReceipt, ProviderError> {
        if start_unix_ms >= end_unix_ms {
            return Err(ProviderError::Invalid(
                "usage interval must be non-empty".into(),
            ));
        }
        if !self.state.lock().await.machines.contains_key(&machine) {
            return Err(ProviderError::NotFound(machine.to_string()));
        }
        Ok(UsageReceipt {
            machine,
            start_unix_ms,
            end_unix_ms,
            elastic_cpu_ns: 0,
            dedicated_cpu_ns: 0,
            private_resident_byte_seconds: 0,
            durable_private_bytes: 0,
            lineage_receipt_sha256: [0; 32],
            egress_bytes: 0,
            receipt: Vec::new(),
        })
    }
    async fn recover(&self, key: IdempotencyKey) -> Result<MutationOutcome, ProviderError> {
        self.state
            .lock()
            .await
            .replays
            .get(&key)
            .map(|value| value.outcome.clone())
            .ok_or_else(|| ProviderError::NotFound(key.to_string()))
    }
    async fn recover_operation(&self, key: IdempotencyKey) -> Result<OperationId, ProviderError> {
        let operation = Self::operation(key);
        self.inspect_operation(operation)
            .await
            .map(|value| value.id)
    }
    async fn inspect_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationObservation, ProviderError> {
        self.state
            .lock()
            .await
            .operations
            .get(&operation)
            .copied()
            .ok_or_else(|| ProviderError::NotFound(operation.to_string()))
    }
    async fn cancel(&self, operation: OperationId) -> Result<OperationObservation, ProviderError> {
        let state = self.state.lock().await;
        let value = state
            .operations
            .get(&operation)
            .copied()
            .ok_or_else(|| ProviderError::NotFound(operation.to_string()))?;
        if value.phase == OperationPhase::Pending {
            Ok(OperationObservation {
                id: operation,
                phase: OperationPhase::Cancelled,
            })
        } else {
            Ok(value)
        }
    }
    async fn watch_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationStream, ProviderError> {
        let observation = self.inspect_operation(operation).await?;
        Ok(stream::once(async move { Ok(observation) }).boxed())
    }
}

fn transition(
    state: &mut MemoryState,
    machine: MachineId,
    required: MachineState,
    target: MachineState,
    outcome: MutationOutcome,
) -> Result<MutationOutcome, ProviderError> {
    let current = state
        .machines
        .get(&machine)
        .ok_or_else(|| ProviderError::NotFound(machine.to_string()))?
        .state;
    if current == target {
        return Ok(outcome);
    }
    if current != required {
        return Err(ProviderError::Conflict(
            "machine cannot perform that transition".into(),
        ));
    }
    let now = tick(state)?;
    let value = state
        .machines
        .get_mut(&machine)
        .ok_or_else(|| ProviderError::NotFound(machine.to_string()))?;
    value.state = target;
    value.changed_at_unix_ms = now;
    event(state, machine, EventFact::State(target), now)?;
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(key: IdempotencyKey) -> CreateMachine {
        CreateMachine::new(
            key,
            Image::custom([7; 32]).unwrap_or_else(|_| unreachable!()),
            [8; 32],
        )
    }

    #[tokio::test]
    async fn simulation_is_exactly_idempotent_and_rejects_key_rebinding() {
        let provider = SimulatedMachines::default();
        let key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000007")
            .unwrap_or_else(|_| unreachable!());
        let first = provider.create(request(key)).await;
        let second = provider.create(request(key)).await;
        assert_eq!(first, second);
        let mut changed = request(key);
        changed.network_policy_digest = [9; 32];
        assert!(matches!(
            provider.create(changed).await,
            Err(ProviderError::Conflict(_))
        ));
    }

    #[tokio::test]
    async fn failed_policy_mutation_does_not_advance_the_simulation_clock() {
        let provider = SimulatedMachines::default();
        let first = provider
            .create(request(IdempotencyKey::new()))
            .await
            .unwrap_or_else(|_| unreachable!());
        let MutationOutcome::Created(first) = first else {
            unreachable!()
        };
        assert_eq!(first.created_at_unix_ms, 2);
        assert!(matches!(
            provider
                .set_suspension_policy(
                    MachineId::new(),
                    SuspensionPolicy::Manual,
                    IdempotencyKey::new()
                )
                .await,
            Err(ProviderError::NotFound(_))
        ));
        let second = provider
            .create(request(IdempotencyKey::new()))
            .await
            .unwrap_or_else(|_| unreachable!());
        let MutationOutcome::Created(second) = second else {
            unreachable!()
        };
        assert_eq!(second.created_at_unix_ms, 3);
    }

    #[tokio::test]
    #[allow(
        clippy::indexing_slicing,
        reason = "indices are preceded by an assert_eq!(children.len(), 2), proving them in-bounds"
    )]
    async fn checkpoint_fork_lifetimes_and_endpoints_are_independent() {
        let provider = Arc::new(SimulatedMachines::default());
        let create_key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000008")
            .unwrap_or_else(|_| unreachable!());
        let machine = create_machine(provider.as_ref(), request(create_key))
            .await
            .unwrap_or_else(|_| unreachable!());
        let checkpoint_key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000009")
            .unwrap_or_else(|_| unreachable!());
        let checkpoint = checkpoint_machine(provider.as_ref(), machine, checkpoint_key)
            .await
            .unwrap_or_else(|_| unreachable!());
        let fork_key = IdempotencyKey::parse("00000000-0000-0000-0000-00000000000a")
            .unwrap_or_else(|_| unreachable!());
        let children = fork_checkpoint(provider.as_ref(), checkpoint, NonZeroU32::new(2).unwrap_or(NonZeroU32::MIN), fork_key)
            .await
            .unwrap_or_else(|_| unreachable!());
        assert_eq!(children.len(), 2);
        assert_ne!(children[0], children[1]);
        assert_ne!(children[0], machine);
        destroy_checkpoint(provider.as_ref(), checkpoint,
                IdempotencyKey::parse("00000000-0000-0000-0000-00000000000b")
                    .unwrap_or_else(|_| unreachable!()),
            )
            .await
            .unwrap_or_else(|_| unreachable!());
        assert!(provider.inspect_machine(machine).await.is_ok());
        assert!(provider.inspect_machine(children[0]).await.is_ok());
    }

    fn key(suffix: u8) -> IdempotencyKey {
        IdempotencyKey::parse(&format!("00000000-0000-0000-0000-0000000001{suffix:02x}"))
            .unwrap_or_else(|_| unreachable!())
    }

    fn two() -> NonZeroU32 {
        NonZeroU32::new(2).unwrap_or(NonZeroU32::MIN)
    }

    #[tokio::test]
    async fn live_fork_children_inherit_the_contract_and_replay_exactly() {
        let provider = Arc::new(SimulatedMachines::default());
        let parent = create_machine(provider.as_ref(), request(key(1)))
            .await
            .unwrap_or_else(|_| unreachable!());
        let parent_view = provider.inspect_machine(parent).await.unwrap_or_else(|_| unreachable!());
        let first = provider.fork_machine(parent, two(), key(2)).await;
        let Ok(MutationOutcome::MachineForked {
            source,
            fidelity,
            children,
        }) = first.clone()
        else {
            unreachable!("live fork must succeed: {first:?}")
        };
        assert_eq!(source, parent);
        assert_eq!(fidelity, ForkFidelity::MemoryAndDisk);
        assert_eq!(children.len(), 2);
        let ids = children
            .iter()
            .map(|child| child.id)
            .collect::<BTreeSet<_>>();
        assert_eq!(ids.len(), 2);
        assert!(!ids.contains(&parent));
        for child in &children {
            assert_eq!(child.state, MachineState::Running);
            assert_eq!(child.contract, parent_view.contract);
            assert_eq!(child.last_checkpoint, None);
            assert_ne!(child.endpoints, parent_view.endpoints);
        }
        assert_eq!(
            provider.fork_machine(parent, two(), key(2)).await,
            first
        );
        assert!(matches!(
            provider
                .fork_machine(parent, NonZeroU32::MIN, key(2))
                .await,
            Err(ProviderError::Conflict(_))
        ));
        assert_eq!(
            provider.inspect_machine(parent).await.map(|value| value.state),
            Ok(MachineState::Running)
        );

        // Recursive fork from one inspected child.
        let child = children.first().map(|value| value.id).unwrap_or_default();
        provider.inspect_machine(child).await.unwrap_or_else(|_| unreachable!());
        let grandchildren = fork_machine(provider.as_ref(), child, NonZeroU32::MIN, key(3))
            .await
            .unwrap_or_else(|_| unreachable!());
        assert_eq!(grandchildren.fidelity, ForkFidelity::MemoryAndDisk);
        assert_eq!(grandchildren.children.len(), 1);
    }

    #[tokio::test]
    async fn live_fork_sources_are_joined_after_their_children() {
        let provider = SimulatedMachines::default();
        let Ok(MutationOutcome::Created(parent)) = provider.create(request(key(0x10))).await else {
            unreachable!()
        };
        let Ok(MutationOutcome::MachineForked { children, .. }) =
            provider.fork_machine(parent.id, two(), key(0x11)).await
        else {
            unreachable!()
        };
        assert!(matches!(
            provider.destroy_machine(parent.id, key(0x12)).await,
            Err(ProviderError::Conflict(_))
        ));
        for (index, child) in (0x13_u8..).zip(&children) {
            assert_eq!(
                provider.destroy_machine(child.id, key(index)).await,
                Ok(MutationOutcome::MachineDestroyed(child.id))
            );
        }
        assert_eq!(
            provider.destroy_machine(parent.id, key(0x12)).await,
            Ok(MutationOutcome::MachineDestroyed(parent.id))
        );
    }

    #[tokio::test]
    async fn live_fork_requires_a_running_source() {
        let provider = SimulatedMachines::default();
        let Ok(MutationOutcome::Created(parent)) = provider.create(request(key(0x20))).await else {
            unreachable!()
        };
        provider
            .suspend(parent.id, key(0x21))
            .await
            .unwrap_or_else(|_| unreachable!());
        assert!(matches!(
            provider.fork_machine(parent.id, two(), key(0x22)).await,
            Err(ProviderError::Conflict(_))
        ));
        assert!(matches!(
            provider
                .fork_machine(MachineId::new(), two(), key(0x23))
                .await,
            Err(ProviderError::NotFound(_))
        ));
        let too_many = NonZeroU32::new(MAX_FORK_CHILDREN + 1).unwrap_or(NonZeroU32::MAX);
        assert!(matches!(
            provider.fork_machine(parent.id, too_many, key(0x24)).await,
            Err(ProviderError::Invalid(_))
        ));
    }

    #[tokio::test]
    async fn live_fork_fidelity_follows_the_declared_capability() {
        let disk_only =
            SimulatedMachines::with_capabilities(BTreeSet::from([Capability::DiskFork]));
        let Ok(MutationOutcome::Created(parent)) = disk_only.create(request(key(0x30))).await
        else {
            unreachable!()
        };
        assert_eq!(
            parent.contract.fork_fidelity(),
            Some(ForkFidelity::DiskOnly)
        );
        assert!(matches!(
            disk_only.fork_machine(parent.id, two(), key(0x31)).await,
            Ok(MutationOutcome::MachineForked {
                fidelity: ForkFidelity::DiskOnly,
                ..
            })
        ));

        let none = SimulatedMachines::with_capabilities(BTreeSet::new());
        let Ok(MutationOutcome::Created(parent)) = none.create(request(key(0x32))).await else {
            unreachable!()
        };
        assert_eq!(parent.contract.fork_fidelity(), None);
        assert!(matches!(
            none.fork_machine(parent.id, two(), key(0x33)).await,
            Err(ProviderError::Unsupported(_))
        ));
        // The rejected key stays free for a corrected intent.
        assert!(none.recover(key(0x33)).await.is_err());

        let mut require = request(key(0x34));
        require.compatibility =
            CompatibilityPolicy::Require(BTreeSet::from([Capability::LiveFork]));
        assert!(matches!(
            none.create(require).await,
            Err(ProviderError::Unsupported(_))
        ));
    }

    #[test]
    fn live_fork_takes_precedence_over_disk_fork() {
        assert_eq!(
            ForkFidelity::for_capabilities(&BTreeSet::from([
                Capability::DiskFork,
                Capability::LiveFork
            ])),
            Some(ForkFidelity::MemoryAndDisk)
        );
    }

    #[tokio::test]
    async fn operation_control_is_correlated_and_deterministic() {
        let provider = Arc::new(SimulatedMachines::default());
        let key = IdempotencyKey::parse("00000000-0000-0000-0000-00000000000c")
            .unwrap_or_else(|_| unreachable!());
        create_machine(provider.as_ref(), request(key))
            .await
            .unwrap_or_else(|_| unreachable!());
        let operation = provider.recover_operation(key)
            .await
            .unwrap_or_else(|_| unreachable!());
        let expected = OperationObservation {
            id: operation,
            phase: OperationPhase::Succeeded,
        };

        assert_eq!(provider.inspect_operation(operation).await, Ok(expected));
        assert_eq!(provider.cancel(operation).await, Ok(expected));
        let mut observations = provider.watch_operation(operation)
            .await
            .unwrap_or_else(|_| unreachable!());
        assert_eq!(observations.next().await, Some(Ok(expected)));
        assert_eq!(observations.next().await, None);

        let unknown = OperationId::parse("00000000-0000-0000-0000-00000000000d")
            .unwrap_or_else(|_| unreachable!());
        assert!(matches!(
            provider.inspect_operation(unknown).await,
            Err(ProviderError::NotFound(_))
        ));
        assert!(matches!(
            provider.cancel(unknown).await,
            Err(ProviderError::NotFound(_))
        ));
        assert!(matches!(
            provider.watch_operation(unknown).await,
            Err(ProviderError::NotFound(_))
        ));
        let unknown_key = IdempotencyKey::parse("00000000-0000-0000-0000-00000000000e")
            .unwrap_or_else(|_| unreachable!());
        assert!(matches!(
            provider.recover_operation(unknown_key).await,
            Err(ProviderError::NotFound(_))
        ));
    }

    #[test]
    fn derived_identities_are_version_five_and_never_nil() {
        assert_eq!(
            OperationId::from_digest([0; 32]).to_string(),
            "00000000-0000-5000-8000-000000000000"
        );
        assert_eq!(
            MachineId::from_digest([0xff; 32]).to_string(),
            "ffffffff-ffff-5fff-bfff-ffffffffffff"
        );
    }

    #[test]
    fn managed_oci_requires_an_immutable_digest() {
        assert!(Image::oci("ghcr.io/acme/agent:latest").is_err());
        assert!(Image::oci(format!("ghcr.io/acme/agent@sha256:{}", "0".repeat(64))).is_err());
        assert!(Image::oci(format!("ghcr.io/acme/agent@sha256:{}", "a".repeat(64))).is_ok());
    }

    #[test]
    #[allow(
        clippy::indexing_slicing,
        reason = "indexes a repo-local `compatibility/manifest.json` fixture bundled via include_str!; a missing key means the fixture itself is broken and the test should panic loudly"
    )]
    fn public_descriptor_is_pinned() {
        let digest: [u8; 32] = Sha256::digest(FILE_DESCRIPTOR_SET).into();
        let manifest: serde_json::Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../compatibility/manifest.json"
        )))
        .unwrap_or_else(|_| unreachable!());
        let expected = manifest["families"]["machines"]["descriptorDigest"]
            .as_str()
            .unwrap_or_else(|| unreachable!());
        assert_eq!(expected, format!("sha256:{}", hex::encode(digest)));
    }
}
