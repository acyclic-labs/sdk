//! Raw provider primitives. Replay, ordering and resource ownership live in core.
use super::{Dispatch, NativeBinding};
use crate::{
    CheckpointId, CheckpointObservation, CreateMachine, IdempotencyKey, MachineContract, MachineId,
    MachineObservation, MachineState, ProviderError, SuspensionPolicy,
};
use serde::{Deserialize, Serialize};

/// Typed native addresses prevent checkpoint cleanup from deleting a machine.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum NativeResource {
    Machine(String),
    Checkpoint(String),
}

impl NativeResource {
    pub fn handle(&self) -> &str {
        match self {
            Self::Machine(value) | Self::Checkpoint(value) => value,
        }
    }
}
use async_trait::async_trait;

/// Minimal native resource custody. The canonical operation registry owns
/// cancellation, cleanup ordering, retained debt and driver affinity.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait NativeResources: acyclic_stream::ProviderPlatform {
    fn name(&self) -> &str;

    /// Delete one opaque resource once. A missing resource may return NotFound;
    /// a lost native reply remains Unavailable. Retry decisions belong to core.
    async fn raw_destroy(&self, resource: &NativeResource) -> Result<(), ProviderError>;
}

/// Raw lifecycle primitives, separate from the minimal resource custody used by
/// the operation registry. Neither boundary owns replay, events or cancellation.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait RawMachines: NativeResources {
    /// Dispatch one creation and retain its accepted address even when decoding
    /// the native observation fails. Admission/idempotency/readiness are in core.
    async fn raw_create(&self, request: &CreateMachine) -> Result<RawCreated, ProviderError>;

    /// Read once; `known` is the canonical observation, not a provider cache.
    /// Return the provider's actual readiness separately from its public state
    /// (a running VM can temporarily be busy snapshotting or forking).
    async fn raw_observe(
        &self,
        machine: MachineId,
        native: Option<&NativeBinding>,
        known: Option<&MachineObservation>,
    ) -> Result<RawObservation, ProviderError>;

    async fn raw_suspend(
        &self,
        machine: MachineId,
        native: Option<&NativeBinding>,
    ) -> Result<(), ProviderError>;
    async fn raw_wake(
        &self,
        machine: MachineId,
        native: Option<&NativeBinding>,
    ) -> Result<(), ProviderError>;
    async fn raw_set_suspension_policy(
        &self,
        machine: MachineId,
        native: Option<&NativeBinding>,
        policy: SuspensionPolicy,
    ) -> Result<(), ProviderError>;

    /// Start one atomic provider checkpoint. Return its native address only after
    /// explicit acceptance. A conflict or lost reply never proves ownership.
    async fn raw_checkpoint(
        &self,
        source: &MachineObservation,
        native: Option<&NativeBinding>,
        key: IdempotencyKey,
    ) -> Result<String, ProviderError>;

    /// Read accepted checkpoint readiness once. Missing/not yet visible is pending.
    async fn raw_observe_checkpoint(
        &self,
        handle: &str,
        source: MachineId,
        contract: &MachineContract,
    ) -> Result<Option<CheckpointObservation>, ProviderError>;

    /// Create one independent child from the admitted immutable checkpoint.
    /// Sequencing, child order, ownership and readiness remain in the core.
    async fn raw_fork(
        &self,
        checkpoint: CheckpointId,
        native: Option<&NativeBinding>,
        contract: &MachineContract,
        slot: ForkSlot,
        key: IdempotencyKey,
    ) -> Result<RawCreated, ProviderError>;

    /// Dispatch one native memory-and-disk fork. Only a definite state refusal
    /// may return Refused; ambiguous transport results remain errors.
    async fn raw_fork_machine(
        &self,
        parent: &MachineObservation,
        source: MachineId,
        native: Option<&NativeBinding>,
        slot: ForkSlot,
        key: IdempotencyKey,
    ) -> Result<Dispatch<RawCreated>, ProviderError>;

    /// Publish provider metadata only after canonical custody of the child.
    /// A provider without a separate metadata call returns Ok without native IO.
    async fn raw_finish_fork(
        &self,
        child: MachineId,
        native: Option<&NativeBinding>,
        parent: &MachineObservation,
        source: &str,
        slot: ForkSlot,
        key: IdempotencyKey,
    ) -> Result<(), ProviderError>;

    /// Read the native dependency states once. The core decides whether a live
    /// fork source may be deleted; native service refusals remain authoritative.
    async fn raw_fork_children(
        &self,
        machine: MachineId,
        native: Option<&NativeBinding>,
    ) -> Result<Vec<MachineState>, ProviderError>;
}

/// Native readiness is transport data; waiting and state history belong to core.
pub struct RawObservation {
    pub machine: MachineObservation,
    pub settled: bool,
}

/// A native acceptance and its decoding result. Metadata failure cannot erase an
/// already-created resource from the shared cleanup owner.
pub struct RawCreated {
    pub handle: String,
    pub endpoint: Option<String>,
    pub observation: Result<MachineObservation, ProviderError>,
    pub adopted: bool,
}

impl RawCreated {
    /// Admission validation shares the same retained-handle path as wire decoding.
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn checked(
        mut self,
        check: impl FnOnce(&MachineObservation) -> Result<(), ProviderError>,
    ) -> Self {
        self.observation = self.observation.and_then(|observation| {
            check(&observation)?;
            Ok(observation)
        });
        self
    }
}

/// Position of one child within an admitted fork.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ForkSlot {
    pub index: u32,
    pub count: u32,
}
