#![doc = include_str!("../README.md")]
#![doc = include_str!("../docs/guide.md")]
#![allow(
    missing_docs,
    reason = "field-level wire semantics and documentation are canonical in the Rust contract model"
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

mod grpc;
pub use grpc::Authentication;
#[cfg(not(target_arch = "wasm32"))]
pub use grpc::{Tls, service::Service};

/// Generated revision-one public transport. Service implementations consume this module;
/// customer applications should use the checked types and handles in this crate.
#[doc(hidden)]
pub mod wire {
    #![allow(missing_docs, reason = "generated from the documented public schema")]
    #![allow(clippy::all, clippy::pedantic, reason = "generated protobuf bindings")]
    include!(concat!(
        env!("OUT_DIR"),
        "/wire/messages/acyclic.machines.v1.rs"
    ));
    include!(concat!(
        env!("OUT_DIR"),
        "/wire/tonic/acyclic.machines.v1.rs"
    ));
}

/// Canonical public descriptor set.
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("generated/acyclic-machines-v1.bin");
/// Current public protocol major.
pub const PROTOCOL_MAJOR: u32 = 1;
/// Current public protocol minor.
pub const PROTOCOL_MINOR: u32 = 1;
/// Maximum machines returned in one page.
pub const MAX_PAGE_SIZE: u32 = acyclic_sdk_contract_wire::type_policy::MACHINE_PAGE_LIMIT_MAX;
/// Maximum children admitted by one fork request.
pub const MAX_FORK_CHILDREN: u32 = 1_024;
/// Maximum events returned in one page.
pub const MAX_EVENT_PAGE_SIZE: u32 = acyclic_sdk_contract_wire::type_policy::MACHINE_EVENT_PAGE_LIMIT_MAX;
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
    /// Allows the provider to change the machine's allocated CPU capacity.
    ElasticCpu,
    /// Allows the provider to change the machine's allocated memory capacity.
    ElasticMemory,
    /// Allows a checkpoint to capture a running machine without stopping it.
    LiveCheckpoint,
    /// Allows a running machine to be forked with memory and disk fidelity.
    LiveFork,
    /// Allows the machine to transition between running and suspended states.
    SuspendResume,
    /// Allows the provider to move the machine while preserving its contract.
    LiveMovement,
    /// [`MachinesProvider::fork_machine`] copies a running machine's persistent disk, but not
    /// its memory or processes. Which paths are persistent is provider-defined: a provider whose
    /// machines boot from an immutable image may copy only its declared data directory, the
    /// rest being the image both boot from. [`Capability::LiveFork`] is the memory-and-disk form and takes precedence
    /// when a contract declares both.
    DiskFork,
}

/// What the children of one [`MachinesProvider::fork_machine`] inherited from their source.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum ForkFidelity {
    /// Children resume from the source's memory, processes, and disk at the fork instant.
    MemoryAndDisk,
    /// Children boot fresh over a copy of the source's persistent disk (provider-defined; see
    /// [`Capability::DiskFork`]) taken at one consistent instant. No process or memory state is
    /// inherited; the caller restarts its workload in each child, for example from durable
    /// history.
    DiskOnly,
}

impl ForkFidelity {
    /// The fidelity a machine with `capabilities` forks at, or `None` when it cannot be
    /// forked live at all and the caller must fall back to checkpoint fork or a restart.
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
    /// Do not suspend automatically; suspension occurs only through an explicit mutation.
    Manual,
    /// Suspend after the machine has remained idle for the given duration.
    AfterIdle(Duration),
}

/// Automatic destruction policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ExpirationPolicy {
    /// Keep the machine until an explicit destroy mutation.
    Never,
    /// Destroy after the machine has existed for the given duration.
    MaxAge(Duration),
    /// Destroy at the given Unix timestamp in milliseconds.
    AtUnixMs(u64),
    /// Destroy after the machine has remained idle for the given duration.
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
    /// contract; `None` means live fork is unsupported for it.
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
    /// The provider is allocating and initializing the machine.
    Starting,
    /// The machine is available for normal operations.
    Running,
    /// A suspension transition has been admitted and is in progress.
    Suspending,
    /// The machine is suspended and can be woken when supported.
    Suspended,
    /// A wake transition has been admitted and is in progress.
    Waking,
    /// Destruction has been admitted and is in progress.
    Destroying,
    /// The machine has reached its terminal destroyed state.
    Destroyed,
    /// The provider observed a terminal failure for the machine.
    Failed,
    /// The provider cannot yet determine the final outcome of a mutation.
    Indeterminate,
}

/// Stable logical service endpoint.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Endpoint {
    /// Stable customer-visible endpoint name.
    pub name: String,
    /// URI used to reach the endpoint.
    pub uri: String,
}

/// Checked customer-visible machine observation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MachineObservation {
    /// Stable logical identity of the observed machine.
    pub id: MachineId,
    /// Lifecycle state observed by the provider.
    pub state: MachineState,
    /// Immutable contract under which the machine is running.
    pub contract: MachineContract,
    /// Endpoints currently published for the machine.
    pub endpoints: Vec<Endpoint>,
    /// Most recent checkpoint, when one exists.
    pub last_checkpoint: Option<CheckpointId>,
    /// Creation time as Unix milliseconds.
    pub created_at_unix_ms: u64,
    /// Last provider state change as Unix milliseconds.
    pub changed_at_unix_ms: u64,
}

/// Checked customer-visible checkpoint observation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CheckpointObservation {
    /// Stable immutable checkpoint identity.
    pub id: CheckpointId,
    /// Machine from which the checkpoint was captured.
    pub source: MachineId,
    /// Contract retained by the checkpoint.
    pub contract: MachineContract,
    /// Whether the provider can fork a new machine from this checkpoint.
    pub forkable: bool,
    /// Creation time as Unix milliseconds.
    pub created_at_unix_ms: u64,
}

/// Bounded stable-cursor machine page.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MachinePage {
    /// Machines in provider-defined stable order.
    pub machines: Vec<MachineObservation>,
    /// Cursor for the next page, or `None` when the snapshot is exhausted.
    pub next: Option<MachineId>,
}

/// Customer-visible capacity pressure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Pressure {
    /// The customer's configured spending budget limits capacity.
    CustomerBudget,
    /// The customer's configured concurrent-machine limit is reached.
    MachineLimit,
    /// The provider is temporarily saturated.
    ServiceSaturation,
}

/// Customer-visible event fact.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum EventFact {
    /// The machine entered the contained lifecycle state.
    State(MachineState),
    /// The provider observed the contained capacity pressure.
    Pressure(Pressure),
    /// The provider changed capacity relevant to this machine.
    CapacityChanged,
}

/// Ordered machine event.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MachineEvent {
    /// Machine to which this event belongs.
    pub machine: MachineId,
    /// Monotonic sequence used for exclusive event pagination.
    pub sequence: u64,
    /// Observation time as Unix milliseconds.
    pub observed_at_unix_ms: u64,
    /// Lifecycle or capacity fact reported by the provider.
    pub fact: EventFact,
}

/// Bounded event page.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventPage {
    /// Events in ascending sequence order.
    pub events: Vec<MachineEvent>,
    /// Exclusive cursor for the next page, or `None` when no later event exists.
    pub next_sequence: Option<u64>,
}

/// Immutable usage receipt over one half-open interval.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UsageReceipt {
    /// Machine whose usage was measured.
    pub machine: MachineId,
    /// Inclusive interval start as Unix milliseconds.
    pub start_unix_ms: u64,
    /// Exclusive interval end as Unix milliseconds.
    pub end_unix_ms: u64,
    /// Elastic CPU consumption in nanoseconds.
    pub elastic_cpu_ns: u64,
    /// Dedicated CPU consumption in nanoseconds.
    pub dedicated_cpu_ns: u64,
    /// Private resident-memory consumption in byte-seconds.
    pub private_resident_byte_seconds: u64,
    /// Durable private storage at the end of the interval, in bytes.
    pub durable_private_bytes: u64,
    /// Commitment to the separately signed account-lineage storage receipt.
    /// Zero is permitted only for process-local simulation.
    pub lineage_receipt_sha256: [u8; 32],
    /// Network egress measured during the interval, in bytes.
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
    /// A machine was created and its observation is returned.
    Created(MachineObservation),
    /// A checkpoint was created and its observation is returned.
    Checkpointed(CheckpointObservation),
    /// Checkpoint-based fork produced the listed machines.
    Forked(Vec<MachineObservation>),
    /// Outcome of [`MachinesProvider::fork_machine`]: the running source, the fidelity the
    /// children were forked at, and the children in index order.
    MachineForked {
        source: MachineId,
        fidelity: ForkFidelity,
        children: Vec<MachineObservation>,
    },
    /// A machine entered suspended state.
    Suspended(MachineId),
    /// A machine entered running state after waking.
    Woken(MachineId),
    /// A machine accepted the contained suspension policy.
    SuspensionPolicySet(MachineId, SuspensionPolicy),
    /// A machine reached its destroyed state.
    MachineDestroyed(MachineId),
    /// A checkpoint was destroyed.
    CheckpointDestroyed(CheckpointId),
}

/// Durable operation phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum OperationPhase {
    /// The provider admitted the operation but has not completed it.
    Pending,
    /// The operation completed successfully.
    Succeeded,
    /// The operation was cancelled before completion.
    Cancelled,
    /// The provider cannot determine the final remote outcome yet.
    Indeterminate,
    /// The operation completed with a provider failure.
    Failed,
}

/// Operation observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OperationObservation {
    /// Stable identity used to inspect, cancel, or watch the operation.
    pub id: OperationId,
    /// Latest provider-known phase.
    pub phase: OperationPhase,
}

/// Bounded-memory stream of correlated operation observations.
#[cfg(not(target_arch = "wasm32"))]
pub type OperationStream =
    Pin<Box<dyn Stream<Item = Result<OperationObservation, ProviderError>> + Send + 'static>>;
/// Browser operation stream using the platform's local executor.
#[cfg(target_arch = "wasm32")]
pub type OperationStream =
    Pin<Box<dyn Stream<Item = Result<OperationObservation, ProviderError>> + 'static>>;

/// Execution constraints selected automatically for the compilation target.
/// Native providers can be shared across threads; browser providers use local futures.
#[cfg(not(target_arch = "wasm32"))]
pub trait ProviderPlatform: Send + Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + Sync + ?Sized> ProviderPlatform for T {}
/// Execution constraints selected automatically for browser providers.
#[cfg(target_arch = "wasm32")]
pub trait ProviderPlatform {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> ProviderPlatform for T {}

/// Provider-boundary failure.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProviderError {
    /// The requested machine, checkpoint, or operation is absent.
    #[error("not found: {0}")]
    NotFound(String),
    /// The request conflicts with current state or a previously used identity.
    #[error("conflict: {0}")]
    Conflict(String),
    /// The provider or contract does not support the requested capability.
    #[error("unsupported: {0}")]
    Unsupported(String),
    /// Request fields or their relationships failed validation.
    #[error("invalid request: {0}")]
    Invalid(String),
    /// The provider rejected the request without changing the resource.
    #[error("rejected: {0}")]
    Rejected(String),
    /// The remote provider could not be reached or is not ready.
    #[error("Machines service is unavailable")]
    Unavailable,
    /// A mutation keyed by this idempotency identity has an unknown outcome.
    #[error("operation is indeterminate: {0}")]
    Indeterminate(IdempotencyKey),
    /// The operation result is unknown and must be inspected by this identity.
    #[error("operation observation is indeterminate; inspect operation {0}")]
    OperationIndeterminate(OperationId),
    /// The provider reported a terminal operation failure.
    #[error("operation failed")]
    Failed,
    /// The provider cancelled the operation before completion.
    #[error("operation cancelled")]
    Cancelled,
}

/// Public provider interface shared by deterministic and managed implementations.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait MachinesProvider: ProviderPlatform {
    /// Describes the provider's actual security/durability boundary.
    fn assurance(&self) -> ProviderAssurance;
    /// Resolves an immutable image into its provider-qualified capabilities.
    async fn qualify_image(&self, image: Image) -> Result<ImageQualification, ProviderError>;
    /// Admits a machine creation request and returns its terminal mutation outcome.
    async fn create(&self, request: CreateMachine) -> Result<MutationOutcome, ProviderError>;
    /// Reads the latest provider observation for one machine identity.
    async fn inspect_machine(
        &self,
        machine: MachineId,
    ) -> Result<MachineObservation, ProviderError>;
    /// Lists machines using the provider's stable machine cursor.
    async fn list_machines(
        &self,
        after: Option<MachineId>,
        limit: u32,
    ) -> Result<MachinePage, ProviderError>;
    /// Captures an immutable checkpoint using the caller-retained mutation identity.
    async fn checkpoint(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError>;
    /// Reads the latest observation for one immutable checkpoint.
    async fn inspect_checkpoint(
        &self,
        checkpoint: CheckpointId,
    ) -> Result<CheckpointObservation, ProviderError>;
    /// Forks a checkpoint into fresh machines using the caller-retained identity.
    async fn fork(
        &self,
        checkpoint: CheckpointId,
        count: NonZeroU32,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError>;
    /// Forks a running machine into `count` fresh children without an intermediate
    /// checkpoint, idempotently under `key`. The call may happen at any point in the source's
    /// life; nothing has to be prepared in advance.
    ///
    /// # Admission
    ///
    /// The source must be [`MachineState::Running`] (otherwise [`ProviderError::Conflict`])
    /// and its contract must declare [`Capability::LiveFork`] or [`Capability::DiskFork`]
    /// (otherwise [`ProviderError::Unsupported`], which is also this method's default). Check
    /// [`MachineContract::fork_fidelity`] before calling; on `Unsupported`, fall back to
    /// [`MachinesProvider::checkpoint`] plus [`MachinesProvider::fork`], or to a restart.
    /// `count` above [`MAX_FORK_CHILDREN`] is [`ProviderError::Invalid`].
    ///
    /// # What a child inherits
    ///
    /// - [`ForkFidelity::MemoryAndDisk`] (declared by `LiveFork`): the source's memory,
    ///   running processes, and disk at one instant. [`ForkFidelity::DiskOnly`] (declared by
    ///   `DiskFork`): a copy of the source's persistent disk only (see
    ///   [`Capability::DiskFork`]); the child boots fresh and the caller
    ///   restarts its workload. The outcome reports which one ran; it always equals the
    ///   source contract's [`MachineContract::fork_fidelity`].
    /// - The source's exact [`MachineContract`] (image, capabilities, policies,
    ///   budgets). `last_checkpoint` is `None`: no checkpoint was taken.
    /// - Anything in memory or on disk, including credentials and environment. A child that
    ///   needs its own identity must be re-provisioned after the fork.
    /// - A provider may fan out by forking earlier children of the same call (for example
    ///   when a machine can run only one fork at a time). A child forked from an earlier child
    ///   inherits whatever that child executed after its own fork, so the children are
    ///   memory-identical only if the workload does not advance while the fork is in progress,
    ///   for example because it waits for a signal after the fork point. Every child is still
    ///   reported as a child of `machine`.
    ///
    /// # Identity
    ///
    /// Children get fresh [`MachineId`]s distinct from the source and from each other, their
    /// own endpoints, and independent lifetimes. Replaying `key` with the same source and count
    /// returns the identical outcome; reusing it for another intent is
    /// [`ProviderError::Conflict`].
    ///
    /// # Network
    ///
    /// Open network connections are never guaranteed to survive in a child: a child may
    /// have a different address, and peers see at most one of the copies. Children must treat
    /// every socket open at the fork instant as broken and reconnect. The source keeps its
    /// connections, though it may be briefly quiesced while the fork is taken.
    ///
    /// # Joining
    ///
    /// Destroy children before their source. A provider may refuse to destroy a source while
    /// any of its live-fork children is not yet [`MachineState::Destroyed`]; it then returns
    /// [`ProviderError::Conflict`] without side effects, and the call may be retried once the
    /// children are gone. Providers that allow it leave the children running.
    ///
    /// # Partial failure
    ///
    /// Forking `count > 1` children is not atomic. A failed or indeterminate attempt may
    /// leave some children; replay `key` to finish it, or [`MachinesProvider::cancel`] its
    /// operation to remove them.
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
    /// Admits a suspension transition for one machine.
    async fn suspend(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError>;
    /// Admits a wake transition for one machine.
    async fn wake(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError>;
    /// Replaces one machine's automatic suspension policy.
    async fn set_suspension_policy(
        &self,
        machine: MachineId,
        policy: SuspensionPolicy,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError>;
    /// Admits destruction of one machine.
    async fn destroy_machine(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError>;
    /// Admits destruction of one immutable checkpoint.
    async fn destroy_checkpoint(
        &self,
        checkpoint: CheckpointId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError>;
    /// Reads an ordered event page after an exclusive sequence cursor.
    async fn events(
        &self,
        machine: MachineId,
        after_sequence: Option<u64>,
        limit: u32,
    ) -> Result<EventPage, ProviderError>;
    /// Produces an authoritative usage receipt for a half-open time interval.
    async fn usage(
        &self,
        machine: MachineId,
        start_unix_ms: u64,
        end_unix_ms: u64,
    ) -> Result<UsageReceipt, ProviderError>;
    /// Replays a mutation by its caller-retained idempotency identity.
    async fn recover(&self, key: IdempotencyKey) -> Result<MutationOutcome, ProviderError>;
    /// Resolves the operation admitted for an idempotency identity.
    async fn recover_operation(&self, key: IdempotencyKey) -> Result<OperationId, ProviderError>;
    /// Reads the latest state for an admitted operation.
    async fn inspect_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationObservation, ProviderError>;
    /// Requests cancellation of an admitted operation.
    async fn cancel(&self, operation: OperationId) -> Result<OperationObservation, ProviderError>;
    /// Streams correlated observations for an admitted operation.
    async fn watch_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationStream, ProviderError>;
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
    /// Returns a clone of the canonical provider boundary for native language bindings.
    ///
    /// Bindings use this boundary so request validation, operation identity, error mapping,
    /// and streaming semantics remain owned by the Rust provider implementation.
    #[must_use]
    pub fn provider(&self) -> Arc<dyn MachinesProvider> {
        Arc::clone(&self.provider)
    }
    /// Returns the provider's actual assurance class.
    #[must_use]
    pub fn assurance(&self) -> ProviderAssurance {
        self.provider.assurance()
    }
    /// Qualifies an immutable image against the provider's capability policy.
    pub async fn qualify_image(&self, image: Image) -> Result<ImageQualification, ProviderError> {
        self.provider.qualify_image(image).await
    }
    /// Creates a machine and returns a stable handle for its admitted identity.
    pub async fn create(&self, request: CreateMachine) -> Result<Machine, ProviderError> {
        match self.provider.create(request).await? {
            MutationOutcome::Created(value) => Ok(Machine::new(self.clone(), value.id)),
            _ => Err(ProviderError::Rejected(
                "provider returned the wrong create outcome".into(),
            )),
        }
    }
    /// Attaches a stable handle after confirming that the machine exists.
    pub async fn attach(&self, id: MachineId) -> Result<Machine, ProviderError> {
        self.provider.inspect_machine(id).await?;
        Ok(Machine::new(self.clone(), id))
    }
    /// Replays the mutation associated with an exact idempotency key.
    pub async fn recover(&self, key: IdempotencyKey) -> Result<MutationOutcome, ProviderError> {
        self.provider.recover(key).await
    }
    /// Resolves the provider operation admitted for an exact idempotency key.
    pub async fn operation_for(&self, key: IdempotencyKey) -> Result<OperationId, ProviderError> {
        self.provider.recover_operation(key).await
    }
    /// Reads the latest state of one exact admitted operation.
    pub async fn inspect_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationObservation, ProviderError> {
        self.provider.inspect_operation(operation).await
    }
    /// Requests cancellation of one exact admitted operation and returns its latest state.
    pub async fn cancel_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationObservation, ProviderError> {
        self.provider.cancel(operation).await
    }
    /// Watches correlated state changes for one exact admitted operation.
    pub async fn watch_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationStream, ProviderError> {
        self.provider.watch_operation(operation).await
    }
    /// Lists machines in provider order using an opaque cursor.
    ///
    /// Pass the `next` cursor from the returned [`MachinePage`] as `after` to
    /// continue the same snapshot. A `None` cursor starts at the first page;
    /// `limit` is bounded by [`MAX_PAGE_SIZE`] and providers may return fewer
    /// entries when the page or snapshot ends.
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
    /// Returns the stable logical identity represented by this handle.
    #[must_use]
    pub fn id(&self) -> MachineId {
        self.id
    }
    /// Reads the current observation for this machine handle.
    ///
    /// The observation is provider state at the time of the call and may
    /// advance immediately after it is returned. The handle remains valid
    /// after transient provider errors, while a missing or destroyed machine
    /// is reported through [`ProviderError`].
    pub async fn inspect(&self) -> Result<MachineObservation, ProviderError> {
        self.machines.provider.inspect_machine(self.id).await
    }
    /// Reads an ordered page of lifecycle and operation events for this machine.
    ///
    /// `after_sequence` is exclusive: pass the last sequence already consumed
    /// to resume without replaying it. Events are retained according to the
    /// provider's retention policy, and `limit` is bounded by
    /// [`MAX_EVENT_PAGE_SIZE`].
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
    /// Reads the provider's metering receipt for a half-open time interval.
    ///
    /// The interval is `[start_unix_ms, end_unix_ms)`. The provider validates
    /// the range and reports its authoritative measured units and accounting
    /// identity in [`UsageReceipt`].
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
    /// Creates an immutable checkpoint of this machine using `key` as the
    /// retained mutation identity.
    ///
    /// Replaying the same key returns the same checkpoint outcome. The
    /// resulting [`Checkpoint`] can be inspected, forked, or destroyed
    /// independently; it does not replace the live machine handle.
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
    /// Forks this running machine into `count` fresh children; see
    /// [`MachinesProvider::fork_machine`] for the exact semantics.
    pub async fn fork(
        &self,
        count: NonZeroU32,
        key: IdempotencyKey,
    ) -> Result<MachineFork, ProviderError> {
        match self
            .machines
            .provider
            .fork_machine(self.id, count, key)
            .await?
        {
            MutationOutcome::MachineForked {
                source,
                fidelity,
                children,
            } if source == self.id => Ok(MachineFork {
                fidelity,
                children: children
                    .into_iter()
                    .map(|value| Machine::new(self.machines.clone(), value.id))
                    .collect(),
            }),
            _ => Err(ProviderError::Rejected(
                "provider returned the wrong fork outcome".into(),
            )),
        }
    }
    /// Requests an idempotent transition of this machine into suspended state.
    ///
    /// The provider may reject the request when the machine state or declared
    /// capabilities do not permit suspension. Replaying `key` preserves the
    /// original mutation result and never creates a second transition.
    pub async fn suspend(&self, key: IdempotencyKey) -> Result<(), ProviderError> {
        terminal_machine(
            self.machines.provider.suspend(self.id, key).await?,
            self.id,
            MutationKind::Suspend,
        )
    }
    /// Requests an idempotent wake transition for this machine.
    ///
    /// A successful call returns after the provider admits the wake operation;
    /// callers that need completion details should observe the machine or use
    /// the correlated operation APIs. Reusing `key` is safe and returns the
    /// original mutation result.
    pub async fn wake(&self, key: IdempotencyKey) -> Result<(), ProviderError> {
        terminal_machine(
            self.machines.provider.wake(self.id, key).await?,
            self.id,
            MutationKind::Wake,
        )
    }
    /// Replaces the automatic suspension policy for this machine.
    ///
    /// The policy is applied only when the provider confirms the same machine
    /// identity and policy in its mutation outcome. `key` makes retries
    /// idempotent; provider validation errors leave the previous policy in
    /// force.
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
    /// Requests irreversible destruction of this machine using an idempotent
    /// mutation identity.
    ///
    /// Providers may require live fork children and related resources to be
    /// destroyed first. After successful destruction, this handle is retained
    /// only as an identity and machine reads return the provider's terminal
    /// state or error according to its retention policy.
    pub async fn destroy(&self, key: IdempotencyKey) -> Result<(), ProviderError> {
        terminal_machine(
            self.machines.provider.destroy_machine(self.id, key).await?,
            self.id,
            MutationKind::Destroy,
        )
    }
}

/// Children of one [`Machine::fork`] and the fidelity they were forked at.
#[derive(Clone)]
pub struct MachineFork {
    /// Fidelity at which the provider created the children.
    pub fidelity: ForkFidelity,
    /// Fresh child handles in provider-reported order.
    pub children: Vec<Machine>,
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
    /// Returns the stable immutable identity represented by this handle.
    #[must_use]
    pub fn id(&self) -> CheckpointId {
        self.id
    }
    /// Reads the provider's latest observation for this checkpoint.
    pub async fn inspect(&self) -> Result<CheckpointObservation, ProviderError> {
        self.machines.provider.inspect_checkpoint(self.id).await
    }
    /// Forks this checkpoint into `count` fresh machine handles.
    ///
    /// The caller-retained `key` makes retries idempotent; providers may reject
    /// the request when the checkpoint is not marked forkable.
    pub async fn fork(
        &self,
        count: NonZeroU32,
        key: IdempotencyKey,
    ) -> Result<Vec<Machine>, ProviderError> {
        match self.machines.provider.fork(self.id, count, key).await? {
            MutationOutcome::Forked(values) => Ok(values
                .into_iter()
                .map(|value| Machine::new(self.machines.clone(), value.id))
                .collect()),
            _ => Err(ProviderError::Rejected(
                "provider returned the wrong fork outcome".into(),
            )),
        }
    }
    /// Requests irreversible destruction of this checkpoint with an idempotent key.
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
///
/// It refuses to destroy a live-fork source while any of its children is not destroyed, the
/// strictest ordering [`MachinesProvider::fork_machine`] allows, so callers tested against it
/// join correctly on every provider.
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
    /// without [`Capability::LiveFork`] to exercise a caller's fallback path. Only admission
    /// and fork fidelity follow the set; other operations are simulated regardless.
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

#[allow(
    clippy::indexing_slicing,
    reason = "`digest` is a Sha256::finalize() output, always exactly 32 bytes, so slicing the first 16 is always in bounds"
)]
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
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
        let mut state = self.state.lock().await;
        let value = state
            .operations
            .get_mut(&operation)
            .ok_or_else(|| ProviderError::NotFound(operation.to_string()))?;
        if value.phase == OperationPhase::Pending {
            value.phase = OperationPhase::Cancelled;
        }
        Ok(*value)
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
            .fork(NonZeroU32::new(2).unwrap_or(NonZeroU32::MIN), fork_key)
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
        let machines = Machines::new(provider.clone());
        let parent = machines
            .create(request(key(1)))
            .await
            .unwrap_or_else(|_| unreachable!());
        let parent_view = parent.inspect().await.unwrap_or_else(|_| unreachable!());
        let first = provider.fork_machine(parent.id(), two(), key(2)).await;
        let Ok(MutationOutcome::MachineForked {
            source,
            fidelity,
            children,
        }) = first.clone()
        else {
            unreachable!("live fork must succeed: {first:?}")
        };
        assert_eq!(source, parent.id());
        assert_eq!(fidelity, ForkFidelity::MemoryAndDisk);
        assert_eq!(children.len(), 2);
        let ids = children
            .iter()
            .map(|child| child.id)
            .collect::<BTreeSet<_>>();
        assert_eq!(ids.len(), 2);
        assert!(!ids.contains(&parent.id()));
        for child in &children {
            assert_eq!(child.state, MachineState::Running);
            assert_eq!(child.contract, parent_view.contract);
            assert_eq!(child.last_checkpoint, None);
            assert_ne!(child.endpoints, parent_view.endpoints);
        }
        assert_eq!(
            provider.fork_machine(parent.id(), two(), key(2)).await,
            first
        );
        assert!(matches!(
            provider
                .fork_machine(parent.id(), NonZeroU32::MIN, key(2))
                .await,
            Err(ProviderError::Conflict(_))
        ));
        assert_eq!(
            parent.inspect().await.map(|value| value.state),
            Ok(MachineState::Running)
        );

        // Recursive fork from a child, through the customer handle.
        let child = machines
            .attach(children.first().map(|value| value.id).unwrap_or_default())
            .await
            .unwrap_or_else(|_| unreachable!());
        let grandchildren = child
            .fork(NonZeroU32::MIN, key(3))
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
        let machines = Machines::new(provider);
        let key = IdempotencyKey::parse("00000000-0000-0000-0000-00000000000c")
            .unwrap_or_else(|_| unreachable!());
        machines
            .create(request(key))
            .await
            .unwrap_or_else(|_| unreachable!());
        let operation = machines
            .operation_for(key)
            .await
            .unwrap_or_else(|_| unreachable!());
        let expected = OperationObservation {
            id: operation,
            phase: OperationPhase::Succeeded,
        };

        assert_eq!(machines.inspect_operation(operation).await, Ok(expected));
        assert_eq!(machines.cancel_operation(operation).await, Ok(expected));
        let mut observations = machines
            .watch_operation(operation)
            .await
            .unwrap_or_else(|_| unreachable!());
        assert_eq!(observations.next().await, Some(Ok(expected)));
        assert_eq!(observations.next().await, None);

        let unknown = OperationId::parse("00000000-0000-0000-0000-00000000000d")
            .unwrap_or_else(|_| unreachable!());
        assert!(matches!(
            machines.inspect_operation(unknown).await,
            Err(ProviderError::NotFound(_))
        ));
        assert!(matches!(
            machines.cancel_operation(unknown).await,
            Err(ProviderError::NotFound(_))
        ));
        assert!(matches!(
            machines.watch_operation(unknown).await,
            Err(ProviderError::NotFound(_))
        ));
        let unknown_key = IdempotencyKey::parse("00000000-0000-0000-0000-00000000000e")
            .unwrap_or_else(|_| unreachable!());
        assert!(matches!(
            machines.operation_for(unknown_key).await,
            Err(ProviderError::NotFound(_))
        ));
    }

    #[test]
    fn managed_oci_requires_an_immutable_digest() {
        assert!(Image::oci("ghcr.io/acme/agent:latest").is_err());
        assert!(Image::oci(format!("ghcr.io/acme/agent@sha256:{}", "0".repeat(64))).is_err());
        assert!(Image::oci(format!("ghcr.io/acme/agent@sha256:{}", "a".repeat(64))).is_ok());
    }

    #[test]
    fn public_descriptor_is_pinned() {
        let digest: [u8; 32] = Sha256::digest(FILE_DESCRIPTOR_SET).into();
        assert_eq!(
            "sha256:68feb507148fbf798a3e05236a4d93d36d216c260db0a6a339db5919c630e758",
            format!("sha256:{}", hex::encode(digest))
        );
    }
}
