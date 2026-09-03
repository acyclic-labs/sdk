//! Public Machines contract, customer handles, and deterministic in-memory provider.
//!
//! The in-memory provider is a bounded process-local state machine. It provides no
//! operating-system, hypervisor, tenant-isolation, durability, or availability boundary.
#![allow(
    missing_docs,
    reason = "field-level wire semantics are canonical in proto/machines/v1/machines.proto"
)]

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    num::NonZeroU32,
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;
use uuid::Uuid;

mod grpc;
pub use grpc::Tls;

/// Generated revision-one public transport. Service implementations consume this module;
/// customer applications should use the checked types and handles in this crate.
#[doc(hidden)]
pub mod wire {
    #![allow(missing_docs, reason = "generated from the documented public schema")]
    #![allow(clippy::all, clippy::pedantic, reason = "generated protobuf bindings")]
    tonic::include_proto!("acyclic.machines.v1");
}

/// Canonical public descriptor set.
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("acyclic-machines-v1");
/// Current public protocol major.
pub const PROTOCOL_MAJOR: u32 = 1;
/// Current public protocol minor.
pub const PROTOCOL_MINOR: u32 = 0;
/// Maximum machines returned in one page.
pub const MAX_PAGE_SIZE: u32 = 256;
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
        let Some((name, digest)) = reference.rsplit_once("@sha256:") else {
            return Err(ProviderError::Invalid(
                "OCI image must contain @sha256:<digest>".into(),
            ));
        };
        if name.is_empty() || digest.len() != 64 {
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
}

/// Capability admission rule.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CompatibilityPolicy {
    /// Enable only the exact proven set.
    BestEffort,
    /// Reject unless every requested capability is proven.
    Require(BTreeSet<Capability>),
}

/// CPU grant and billing behavior.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Performance {
    Elastic,
    Dedicated,
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
pub struct CreateMachine {
    /// Caller-retained mutation identity.
    pub idempotency_key: IdempotencyKey,
    /// Immutable image.
    pub image: Image,
    /// Capability policy.
    pub compatibility: CompatibilityPolicy,
    /// CPU behavior.
    pub performance: Performance,
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
    /// Creates a best-effort Elastic request with 15-second idle suspension and no expiry.
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
            performance: Performance::Elastic,
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
    /// CPU behavior.
    pub performance: Performance,
    /// Suspension policy.
    pub suspension: SuspensionPolicy,
    /// Expiration policy.
    pub expiration: ExpirationPolicy,
    /// Network-policy commitment.
    pub network_policy_digest: [u8; 32],
    /// Cost and concurrency limits.
    pub budgets: Budgets,
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
    pub lineage_shared_bytes: u64,
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
    #[error("operation failed")]
    Failed,
    #[error("operation cancelled")]
    Cancelled,
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
        performance: Performance,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError>;
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
    async fn inspect_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationObservation, ProviderError>;
    async fn cancel(&self, operation: OperationId) -> Result<OperationObservation, ProviderError>;
}

/// Customer-level client over one provider.
#[derive(Clone)]
pub struct Machines {
    provider: Arc<dyn MachinesProvider>,
}

impl Machines {
    /// Binds a provider implementation.
    #[must_use]
    pub fn new(provider: Arc<dyn MachinesProvider>) -> Self {
        Self { provider }
    }
    /// Returns the provider's actual assurance class.
    #[must_use]
    pub fn assurance(&self) -> ProviderAssurance {
        self.provider.assurance()
    }
    pub async fn qualify_image(&self, image: Image) -> Result<ImageQualification, ProviderError> {
        self.provider.qualify_image(image).await
    }
    pub async fn create(&self, request: CreateMachine) -> Result<Machine, ProviderError> {
        match self.provider.create(request).await? {
            MutationOutcome::Created(value) => Ok(Machine::new(self.clone(), value.id)),
            _ => Err(ProviderError::Rejected(
                "provider returned the wrong create outcome".into(),
            )),
        }
    }
    pub async fn attach(&self, id: MachineId) -> Result<Machine, ProviderError> {
        self.provider.inspect_machine(id).await?;
        Ok(Machine::new(self.clone(), id))
    }
    pub async fn recover(&self, key: IdempotencyKey) -> Result<MutationOutcome, ProviderError> {
        self.provider.recover(key).await
    }
    pub async fn list(
        &self,
        after: Option<MachineId>,
        limit: u32,
    ) -> Result<MachinePage, ProviderError> {
        self.provider.list_machines(after, limit).await
    }
}

/// Stable machine handle.
#[derive(Clone)]
pub struct Machine {
    machines: Machines,
    id: MachineId,
}

impl Machine {
    fn new(machines: Machines, id: MachineId) -> Self {
        Self { machines, id }
    }
    #[must_use]
    pub fn id(&self) -> MachineId {
        self.id
    }
    pub async fn inspect(&self) -> Result<MachineObservation, ProviderError> {
        self.machines.provider.inspect_machine(self.id).await
    }
    pub async fn events(
        &self,
        after_sequence: Option<u64>,
        limit: u32,
    ) -> Result<EventPage, ProviderError> {
        self.machines
            .provider
            .events(self.id, after_sequence, limit)
            .await
    }
    pub async fn usage(
        &self,
        start_unix_ms: u64,
        end_unix_ms: u64,
    ) -> Result<UsageReceipt, ProviderError> {
        self.machines
            .provider
            .usage(self.id, start_unix_ms, end_unix_ms)
            .await
    }
    pub async fn checkpoint(&self, key: IdempotencyKey) -> Result<Checkpoint, ProviderError> {
        match self.machines.provider.checkpoint(self.id, key).await? {
            MutationOutcome::Checkpointed(value) => {
                Ok(Checkpoint::new(self.machines.clone(), value.id))
            }
            _ => Err(ProviderError::Rejected(
                "provider returned the wrong checkpoint outcome".into(),
            )),
        }
    }
    pub async fn suspend(&self, key: IdempotencyKey) -> Result<(), ProviderError> {
        terminal_machine(
            self.machines.provider.suspend(self.id, key).await?,
            self.id,
            MutationKind::Suspend,
        )
    }
    pub async fn wake(&self, key: IdempotencyKey) -> Result<(), ProviderError> {
        terminal_machine(
            self.machines.provider.wake(self.id, key).await?,
            self.id,
            MutationKind::Wake,
        )
    }
    pub async fn set_suspension_policy(
        &self,
        policy: SuspensionPolicy,
        key: IdempotencyKey,
    ) -> Result<(), ProviderError> {
        match self
            .machines
            .provider
            .set_suspension_policy(self.id, policy, key)
            .await?
        {
            MutationOutcome::SuspensionPolicySet(id, returned)
                if id == self.id && returned == policy =>
            {
                Ok(())
            }
            _ => Err(ProviderError::Rejected(
                "provider returned the wrong policy outcome".into(),
            )),
        }
    }
    pub async fn destroy(&self, key: IdempotencyKey) -> Result<(), ProviderError> {
        terminal_machine(
            self.machines.provider.destroy_machine(self.id, key).await?,
            self.id,
            MutationKind::Destroy,
        )
    }
}

enum MutationKind {
    Suspend,
    Wake,
    Destroy,
}
fn terminal_machine(
    outcome: MutationOutcome,
    expected: MachineId,
    kind: MutationKind,
) -> Result<(), ProviderError> {
    let matches = match (kind, outcome) {
        (MutationKind::Suspend, MutationOutcome::Suspended(id))
        | (MutationKind::Wake, MutationOutcome::Woken(id))
        | (MutationKind::Destroy, MutationOutcome::MachineDestroyed(id)) => id == expected,
        _ => false,
    };
    if matches {
        Ok(())
    } else {
        Err(ProviderError::Rejected(
            "provider returned the wrong mutation outcome".into(),
        ))
    }
}

/// Stable immutable checkpoint handle.
#[derive(Clone)]
pub struct Checkpoint {
    machines: Machines,
    id: CheckpointId,
}

impl Checkpoint {
    fn new(machines: Machines, id: CheckpointId) -> Self {
        Self { machines, id }
    }
    #[must_use]
    pub fn id(&self) -> CheckpointId {
        self.id
    }
    pub async fn inspect(&self) -> Result<CheckpointObservation, ProviderError> {
        self.machines.provider.inspect_checkpoint(self.id).await
    }
    pub async fn fork(
        &self,
        count: NonZeroU32,
        performance: Performance,
        key: IdempotencyKey,
    ) -> Result<Vec<Machine>, ProviderError> {
        match self
            .machines
            .provider
            .fork(self.id, count, performance, key)
            .await?
        {
            MutationOutcome::Forked(values) => Ok(values
                .into_iter()
                .map(|value| Machine::new(self.machines.clone(), value.id))
                .collect()),
            _ => Err(ProviderError::Rejected(
                "provider returned the wrong fork outcome".into(),
            )),
        }
    }
    pub async fn destroy(&self, key: IdempotencyKey) -> Result<(), ProviderError> {
        match self
            .machines
            .provider
            .destroy_checkpoint(self.id, key)
            .await?
        {
            MutationOutcome::CheckpointDestroyed(id) if id == self.id => Ok(()),
            _ => Err(ProviderError::Rejected(
                "provider returned the wrong checkpoint outcome".into(),
            )),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
enum MemoryIntent {
    Create(CreateMachine),
    Checkpoint(MachineId),
    Fork(CheckpointId, u32, Performance),
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
        }
    }
}

/// Deterministic bounded process-local state-machine provider.
#[derive(Clone, Default)]
pub struct SimulatedMachines {
    state: Arc<Mutex<MemoryState>>,
}

impl SimulatedMachines {
    fn all_capabilities() -> BTreeSet<Capability> {
        BTreeSet::from([
            Capability::ElasticCpu,
            Capability::ElasticMemory,
            Capability::LiveCheckpoint,
            Capability::LiveFork,
            Capability::SuspendResume,
            Capability::LiveMovement,
        ])
    }
    fn revision() -> [u8; 32] {
        Sha256::digest(b"acyclic-machines-memory-v1").into()
    }
    fn operation(key: IdempotencyKey) -> OperationId {
        OperationId(derived_uuid(b"operation", &key.as_bytes(), 0))
    }
    fn machine(key: IdempotencyKey, index: u32) -> MachineId {
        MachineId(derived_uuid(b"machine", &key.as_bytes(), index))
    }
    fn checkpoint_id(key: IdempotencyKey) -> CheckpointId {
        CheckpointId(derived_uuid(b"checkpoint", &key.as_bytes(), 0))
    }
    fn contract(request: &CreateMachine, capabilities: BTreeSet<Capability>) -> MachineContract {
        MachineContract {
            image: request.image.clone(),
            capabilities,
            compatibility: request.compatibility.clone(),
            compatibility_revision: Self::revision(),
            performance: request.performance,
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
        let encoded = serde_json::to_vec(&intent)
            .map_err(|_| ProviderError::Invalid("intent cannot be encoded".into()))?;
        let digest: [u8; 32] = Sha256::digest(encoded).into();
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

fn derived_uuid(domain: &[u8], key: &[u8; 16], index: u32) -> Uuid {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(key);
    hash.update(index.to_be_bytes());
    let digest = hash.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
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
            capabilities: Self::all_capabilities(),
            compatibility_revision: Self::revision(),
        })
    }
    async fn create(&self, request: CreateMachine) -> Result<MutationOutcome, ProviderError> {
        let key = request.idempotency_key;
        let intent = MemoryIntent::Create(request.clone());
        self.apply(key, intent, move |state| {
            let capabilities = Self::all_capabilities();
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
        performance: Performance,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        let count_value = count.get();
        if count_value > MAX_FORK_CHILDREN {
            return Err(ProviderError::Invalid("fork count exceeds 1024".into()));
        }
        self.apply(
            key,
            MemoryIntent::Fork(checkpoint, count_value, performance),
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
                    contract.performance = performance;
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
            let now = tick(state)?;
            let value = state
                .machines
                .get_mut(&machine)
                .ok_or_else(|| ProviderError::NotFound(machine.to_string()))?;
            if value.state == MachineState::Destroyed {
                return Err(ProviderError::Conflict(
                    "destroyed machine cannot change policy".into(),
                ));
            }
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
            lineage_shared_bytes: 0,
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
        changed.performance = Performance::Dedicated;
        assert!(matches!(
            provider.create(changed).await,
            Err(ProviderError::Conflict(_))
        ));
    }

    #[tokio::test]
    async fn checkpoint_fork_lifetimes_and_endpoints_are_independent() {
        let provider = Arc::new(SimulatedMachines::default());
        let machines = Machines::new(provider.clone());
        let create_key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000008")
            .unwrap_or_else(|_| unreachable!());
        let machine = machines
            .create(request(create_key))
            .await
            .unwrap_or_else(|_| unreachable!());
        let checkpoint_key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000009")
            .unwrap_or_else(|_| unreachable!());
        let checkpoint = machine
            .checkpoint(checkpoint_key)
            .await
            .unwrap_or_else(|_| unreachable!());
        let fork_key = IdempotencyKey::parse("00000000-0000-0000-0000-00000000000a")
            .unwrap_or_else(|_| unreachable!());
        let children = checkpoint
            .fork(
                NonZeroU32::new(2).unwrap_or(NonZeroU32::MIN),
                Performance::Elastic,
                fork_key,
            )
            .await
            .unwrap_or_else(|_| unreachable!());
        assert_eq!(children.len(), 2);
        assert_ne!(children[0].id(), children[1].id());
        assert_ne!(children[0].id(), machine.id());
        checkpoint
            .destroy(
                IdempotencyKey::parse("00000000-0000-0000-0000-00000000000b")
                    .unwrap_or_else(|_| unreachable!()),
            )
            .await
            .unwrap_or_else(|_| unreachable!());
        assert!(machine.inspect().await.is_ok());
        assert!(children[0].inspect().await.is_ok());
    }

    #[test]
    fn managed_oci_requires_an_immutable_digest() {
        assert!(Image::oci("ghcr.io/acme/agent:latest").is_err());
        assert!(Image::oci(format!("ghcr.io/acme/agent@sha256:{}", "a".repeat(64))).is_ok());
    }

    #[test]
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
