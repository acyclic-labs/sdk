//! Raw provider primitives. Replay, ordering and resource ownership live in core.
use super::NativeBinding;
use crate::{
    CheckpointObservation, IdempotencyKey, MachineId, MachineObservation, ProviderError,
    SuspensionPolicy,
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

/// Native machine adapter. The remaining lifecycle primitives are being migrated
/// into this same boundary; implementations must never add their own operation registry.
#[async_trait]
pub trait RawMachines: Send + Sync {
    fn name(&self) -> &str;
    /// Read once; `known` is the canonical observation, not a provider cache.
    /// Return the provider's actual readiness separately from its public state
    /// (a running VM can temporarily be busy snapshotting or forking).
    async fn raw_observe(
        &self,
        machine: MachineId,
        native: Option<&NativeBinding>,
        known: Option<&MachineObservation>,
    ) -> Result<RawObservation, ProviderError>;

    async fn raw_suspend(&self, machine: MachineId) -> Result<(), ProviderError>;
    async fn raw_wake(&self, machine: MachineId) -> Result<(), ProviderError>;
    async fn raw_set_suspension_policy(
        &self,
        machine: MachineId,
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
        source: &MachineObservation,
    ) -> Result<Option<CheckpointObservation>, ProviderError>;

    /// Delete one opaque resource once. A missing resource may return NotFound;
    /// a lost native reply remains Unavailable. Retry decisions belong to core.
    async fn raw_destroy(&self, resource: &NativeResource) -> Result<(), ProviderError>;
}

/// Native readiness is transport data; waiting and state history belong to core.
pub struct RawObservation {
    pub machine: MachineObservation,
    pub settled: bool,
}
