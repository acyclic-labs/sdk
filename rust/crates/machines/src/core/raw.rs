//! Raw provider primitives. Replay, ordering and resource ownership live in core.
use crate::{MachineId, MachineObservation, ProviderError, SuspensionPolicy};
use async_trait::async_trait;

/// Native machine adapter. The remaining lifecycle primitives are being migrated
/// into this same boundary; implementations must never add their own operation registry.
#[async_trait]
pub trait RawMachines: Send + Sync {
    /// Read once; `known` is the canonical observation, not a provider cache.
    /// Return the provider's actual readiness separately from its public state
    /// (a running VM can temporarily be busy snapshotting or forking).
    async fn raw_observe(
        &self,
        machine: MachineId,
        known: Option<&MachineObservation>,
    ) -> Result<RawObservation, ProviderError>;

    async fn raw_suspend(&self, machine: MachineId) -> Result<(), ProviderError>;
    async fn raw_wake(&self, machine: MachineId) -> Result<(), ProviderError>;
    async fn raw_set_suspension_policy(
        &self,
        machine: MachineId,
        policy: SuspensionPolicy,
    ) -> Result<(), ProviderError>;

    /// Delete one opaque resource once. A missing resource may return NotFound;
    /// a lost native reply remains Unavailable. Retry decisions belong to core.
    async fn destroy(&self, resource: &str) -> Result<(), ProviderError>;
}

/// Native readiness is transport data; waiting and state history belong to core.
pub struct RawObservation {
    pub machine: MachineObservation,
    pub settled: bool,
}
