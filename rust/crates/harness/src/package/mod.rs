//! Shared harness Actors hosting contract.
//!
//! The normative transaction and binding rules are in the packaged contract.
#![doc = include_str!("../../docs/actor-package.md")]

use crate::{
    AgentId, IdempotencyKey, OperationId, Result,
    workflow::{
        MachineCheckpoint, MachineIdentity, MachineStatus, MachineTransition, ResumableMachine,
        WorkflowCommand,
    },
};
use acyclic_stream::{BoxProviderFuture, ProviderPlatform};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

pub mod conformance;

/// Whether the volume can follow an actor to another node.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum VolumePlacement {
    /// The named volume is reachable from every eligible node.
    Portable,
    /// Recovery must wait for this node; it cannot substitute another volume.
    NodeLocal(String),
}

/// Explicit resources resolved by the runtime, with no credential values.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Bindings {
    /// Streams namespace containing the actors' inboxes and workflows.
    pub streams: String,
    /// Filesystem volume holding immutable command payloads and project state.
    pub volume: String,
    /// Placement restriction of that volume.
    pub placement: VolumePlacement,
    /// HTTPS origin admitted for model requests.
    pub model_https: String,
    /// Runtime secret binding resolved only by the HTTPS effect executor.
    pub model_secret: String,
    /// Runtime-managed compute executor binding.
    pub compute: String,
}

/// Deployable package metadata; code is pinned by the machine digest.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    /// Agent actor implementation and schema identity.
    pub agent: MachineIdentity,
    /// Exhaustive command kinds this package may emit to runtime effect bindings.
    pub effects: Vec<String>,
    /// Explicit platform bindings.
    pub bindings: Bindings,
}

impl Manifest {
    /// Validates the executable contract before a runtime installs any code.
    pub fn validate(&self) -> Result<()> {
        self.agent.validate(&mut None)?;
        let mut effects = std::collections::BTreeSet::new();
        for effect in &self.effects {
            if !crate::is_valid_component_label(effect) || !effects.insert(effect) {
                return Err(crate::Error::Invalid(
                    "invalid or duplicate package effect".into(),
                ));
            }
        }
        let bindings = &self.bindings;
        for name in [
            &bindings.streams,
            &bindings.volume,
            &bindings.model_secret,
            &bindings.compute,
        ] {
            if name.is_empty() || name.trim() != name || name.chars().any(char::is_control) {
                return Err(crate::Error::Invalid("invalid package binding name".into()));
            }
        }
        if let VolumePlacement::NodeLocal(node) = &bindings.placement
            && !crate::is_valid_component_label(node)
        {
            return Err(crate::Error::Invalid("invalid volume owner node".into()));
        }
        let origin = bindings
            .model_https
            .parse::<http::Uri>()
            .map_err(|_| crate::Error::Invalid("model binding requires an HTTPS origin".into()))?;
        if origin.scheme_str() != Some("https")
            || origin
                .authority()
                .is_none_or(|authority| authority.as_str().contains('@'))
            || origin.host().is_none_or(str::is_empty)
            || origin.path() != "/" && !origin.path().is_empty()
            || origin.query().is_some()
        {
            return Err(crate::Error::Invalid(
                "model binding requires an HTTPS origin".into(),
            ));
        }
        Ok(())
    }

    /// Rejects undeclared effects before committing a checkpoint or dispatching.
    pub fn validate_transition(&self, transition: &MachineTransition) -> Result<()> {
        for command in &transition.commands {
            command.validate(&mut None)?;
            if !self.effects.contains(&command.kind) {
                return Err(crate::Error::Invalid("undeclared package effect".into()));
            }
        }
        Ok(())
    }

    /// Inbox subscription relative to the Streams binding.
    #[must_use]
    pub fn inbox(actor: AgentId) -> String {
        format!("agents/{actor}/inbox")
    }

    /// Atomic checkpoint and outbox journal relative to the Streams binding.
    #[must_use]
    pub fn workflow(actor: AgentId) -> String {
        format!("agents/{actor}/workflow")
    }
}

/// The same immutable code artifact is installed by each runtime.
pub trait ActorPackage: ProviderPlatform {
    /// Resources, subscription layout and pinned actor identity.
    fn manifest(&self) -> &Manifest;
    /// Deterministic handler. Its identity must equal the manifest's agent.
    fn machine(&self) -> Arc<dyn ResumableMachine>;
}

/// Validates the manifest and pins the handler before installation. Runtimes
/// retain this returned handler rather than looking it up again after admission.
pub fn validate_package(package: &dyn ActorPackage) -> Result<Arc<dyn ResumableMachine>> {
    package.manifest().validate()?;
    let machine = package.machine();
    if machine.identity() != &package.manifest().agent {
        return Err(crate::Error::Conflict(
            "package handler identity differs from manifest".into(),
        ));
    }
    Ok(machine)
}

/// Runtime-owned fencing identity, checked on every commit and effect result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Placement {
    /// Agent receiving the event.
    pub actor: AgentId,
    /// Current owning node.
    pub node: String,
    /// Strictly increasing ownership generation.
    pub epoch: u64,
}

/// Immutable inbox admission. Authorization is supplied by the host, not input.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Event {
    /// Stable event operation identity.
    pub operation_id: OperationId,
    /// Retry key bound to the complete event.
    pub idempotency_key: IdempotencyKey,
    /// Machine-defined admitted input, including effect results.
    pub input: Value,
}

/// Durable observation used by both clients and the conformance driver.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    /// Current placement fence.
    pub placement: Placement,
    /// Complete durable machine state.
    pub checkpoint: MachineCheckpoint,
    /// Consumed inbox prefix, atomically advanced with the checkpoint.
    pub consumed: u64,
    /// Current machine status.
    pub status: MachineStatus,
    /// Committed commands still requiring reconciliation or execution.
    pub pending: Vec<WorkflowCommand>,
    /// True only after live effects have been fenced and drained.
    pub cancelled: bool,
}

/// Host execution boundary; implementations must obey the packaged contract.
pub trait ActorRuntime: ProviderPlatform {
    /// One deployment operation; rejects identity or binding mismatches.
    fn install(&self, package: Arc<dyn ActorPackage>) -> BoxProviderFuture<'_, Result<()>>;
    /// Admits an actor's initial checkpoint with explicit placement.
    fn place(&self, placement: Placement, input: Value) -> BoxProviderFuture<'_, Result<Snapshot>>;
    /// Admits and handles one event; conflicting retries and stale owners fail.
    fn deliver(
        &self,
        placement: Placement,
        event: Event,
    ) -> BoxProviderFuture<'_, Result<Snapshot>>;
    /// Reconstructs state and reconciles committed effects without a new input.
    fn recover(&self, placement: Placement) -> BoxProviderFuture<'_, Result<Snapshot>>;
    /// Durably fences and drains effects before cancellation settlement.
    fn cancel(&self, placement: Placement, event: Event)
    -> BoxProviderFuture<'_, Result<Snapshot>>;
}

/// Runtime effect boundary; handlers never hold raw secret values or do I/O.
pub trait EffectHost: ProviderPlatform {
    /// Reconciles by stable command ID before dispatch; streams each model chunk
    /// immediately and returns the final result for durable inbox admission.
    fn execute(
        &self,
        placement: Placement,
        command: WorkflowCommand,
    ) -> BoxProviderFuture<'_, Result<Event>>;
    /// Stops and drains all effects of this epoch before returning.
    fn fence(&self, placement: Placement) -> BoxProviderFuture<'_, Result<()>>;
}
