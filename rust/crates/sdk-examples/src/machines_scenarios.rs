//! Rust-owned Machines scenarios for the SDK examples bundle.
//!
//! The scenario exercises the public deterministic provider through the same
//! `Machines` client used by provider-backed applications. Its receipt is
//! explicitly process-local evidence: it does not imply isolation, durability,
//! or a hosted Machines endpoint.

use acyclic_machines::{
    CreateMachine, IdempotencyKey, Image, MachineState, Machines, ProviderAssurance,
    SimulatedMachines, wire,
};
use prost::Message;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{num::NonZeroU32, sync::Arc};

/// Stable source-bound scenario identity.
pub const SCENARIO_ID: &str = "machines-simulated-lifecycle";
/// Fully-qualified RPC identity from the Machines descriptor.
pub const OPERATION_ID: &str = "acyclic.machines.v1.MachinesService/Create";
/// Transport-neutral RPC selector; Machines has no HTTP route claim here.
pub const ROUTE: &str = "grpc:MachinesService/Create";
/// Rust source path recorded in generated receipts.
pub const SOURCE: &str = "rust/crates/sdk-examples/src/machines_scenarios.rs";
const IMAGE_DIGEST: [u8; 32] = [7; 32];
const NETWORK_POLICY_DIGEST: [u8; 32] = [8; 32];
const IDEMPOTENCY_KEY: [u8; 16] = [1; 16];

/// One canonical typed Machines create request and its encoded protobuf form.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MachinesFixture {
    /// Canonical `CreateMachineRequest` bytes.
    pub request: Vec<u8>,
    /// Stable semantic expectation for a local receipt.
    pub expected: Value,
}

/// Receipt emitted after a bounded process-local Machines lifecycle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MachinesReceipt {
    /// Stable scenario identity.
    pub scenario_id: &'static str,
    /// Local validation result.
    pub status: &'static str,
    /// Scope of evidence; no remote transport is implied.
    pub scope: &'static str,
    /// Provider assurance reported by the actual implementation.
    pub assurance: ProviderAssurance,
    /// SHA-256 of the exact encoded fixture request.
    pub fixture_sha256: Vec<u8>,
    /// State observed immediately after creation.
    pub machine_state: MachineState,
    /// Number of children produced by checkpoint fork.
    pub checkpoint_children: usize,
    /// Number of lifecycle events observed from the source machine.
    pub event_count: usize,
}

fn request() -> wire::CreateMachineRequest {
    wire::CreateMachineRequest {
        protocol: Some(wire::ProtocolVersion { major: 1, minor: 1 }),
        idempotency_key: Some(wire::IdempotencyKey {
            value: IDEMPOTENCY_KEY.to_vec(),
        }),
        image: Some(wire::Image {
            kind: wire::ImageKind::Custom as i32,
            immutable_reference: Some(wire::image::ImmutableReference::CustomDigest(
                IMAGE_DIGEST.to_vec(),
            )),
        }),
        compatibility: Some(wire::CompatibilityPolicy {
            mode: wire::CompatibilityMode::BestEffort as i32,
            required: Vec::new(),
        }),
        suspension: Some(wire::SuspensionPolicy {
            policy: Some(wire::suspension_policy::Policy::AfterIdleMs(15_000)),
        }),
        expiration: Some(wire::ExpirationPolicy {
            kind: wire::ExpirationKind::Never as i32,
            value_ms: 0,
        }),
        network_policy_digest: NETWORK_POLICY_DIGEST.to_vec(),
        budgets: Some(wire::Budgets {
            spend_micros: 0,
            concurrency: 0,
        }),
    }
}

/// Builds the canonical encoded fixture from generated Rust wire types.
#[must_use]
pub fn fixture() -> MachinesFixture {
    MachinesFixture {
        request: request().encode_to_vec(),
        expected: json!({
            "kind": "local-provider-result",
            "accepted": true,
            "operation": OPERATION_ID,
            "route": ROUTE,
            "state": "RUNNING",
            "assurance": "process-local-simulation",
            "checkpoint_children": 1,
        }),
    }
}

/// Executes create, inspect, checkpoint, and bounded checkpoint-fork operations.
pub async fn execute() -> Result<MachinesReceipt, Box<dyn std::error::Error + Send + Sync>> {
    let fixture = fixture();
    let provider = Arc::new(SimulatedMachines::default());
    let machines = Machines::new(provider);
    let assurance = machines.assurance();
    if assurance != ProviderAssurance::ProcessLocalSimulation {
        return Err("scenario requires process-local simulation assurance".into());
    }

    let image = Image::custom(IMAGE_DIGEST)?;
    let machine = machines
        .create(CreateMachine::new(
            IdempotencyKey::new(),
            image,
            NETWORK_POLICY_DIGEST,
        ))
        .await?;
    let observation = machine.inspect().await?;
    if observation.state != MachineState::Running {
        return Err(format!("unexpected machine state: {:?}", observation.state).into());
    }
    let checkpoint = machine.checkpoint(IdempotencyKey::new()).await?;
    let children = checkpoint
        .fork(
            NonZeroU32::new(1).ok_or("checkpoint child count must be nonzero")?,
            IdempotencyKey::new(),
        )
        .await?;
    let events = machine.events(None, 16).await?;
    if children.len() != 1 || events.events.is_empty() {
        return Err("bounded lifecycle did not produce the expected children and event".into());
    }

    Ok(MachinesReceipt {
        scenario_id: SCENARIO_ID,
        status: "qualified",
        scope: "rust-process-local-simulation",
        assurance,
        fixture_sha256: Sha256::digest(fixture.request).to_vec(),
        machine_state: observation.state,
        checkpoint_children: children.len(),
        event_count: events.events.len(),
    })
}

/// Rust snippet rendered into the source-bound examples bundle.
#[must_use]
pub fn rust_snippet() -> &'static str {
    r#"// capability: supported; scope: rust-process-local-simulation
use acyclic_machines::{CreateMachine, IdempotencyKey, Image, MachineState, Machines, SimulatedMachines};
use std::{num::NonZeroU32, sync::Arc};

let machines = Machines::new(Arc::new(SimulatedMachines::default()));
assert_eq!(machines.assurance(), acyclic_machines::ProviderAssurance::ProcessLocalSimulation);
let machine = machines.create(CreateMachine::new(
    IdempotencyKey::new(),
    Image::custom([7; 32])?,
    [8; 32],
)).await?;
assert_eq!(machine.inspect().await?.state, MachineState::Running);
let checkpoint = machine.checkpoint(IdempotencyKey::new()).await?;
let children = checkpoint.fork(NonZeroU32::new(1).unwrap(), IdempotencyKey::new()).await?;
assert_eq!(children.len(), 1);"#
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fixture_is_canonical_and_executes() {
        let fixture = fixture();
        let request = wire::CreateMachineRequest::decode(fixture.request.as_slice())
            .expect("create request fixture must be canonical protobuf");
        assert_eq!(
            request.protocol,
            Some(wire::ProtocolVersion { major: 1, minor: 1 })
        );
        assert_eq!(
            request.idempotency_key.unwrap().value,
            IDEMPOTENCY_KEY.to_vec()
        );
        assert_eq!(request.image.unwrap().kind, wire::ImageKind::Custom as i32);
        assert_eq!(fixture.expected["accepted"], true);
        let receipt = execute().await.expect("Machines lifecycle scenario");
        assert_eq!(receipt.scenario_id, SCENARIO_ID);
        assert_eq!(receipt.status, "qualified");
        assert_eq!(receipt.scope, "rust-process-local-simulation");
        assert_eq!(receipt.assurance, ProviderAssurance::ProcessLocalSimulation);
        assert_eq!(receipt.machine_state, MachineState::Running);
        assert_eq!(receipt.checkpoint_children, 1);
        assert!(!rust_snippet().contains("TODO"));
        assert!(!rust_snippet().contains("{{receipt"));
    }

    #[test]
    fn fixture_preserves_presence_and_wire_tags() {
        let request = wire::CreateMachineRequest::decode(fixture().request.as_slice())
            .expect("create request fixture must be canonical protobuf");
        assert!(request.protocol.is_some());
        assert!(request.idempotency_key.is_some());
        assert!(request.image.is_some());
        assert!(request.compatibility.is_some());
        assert!(request.suspension.is_some());
        assert!(request.expiration.is_some());
        assert!(request.budgets.is_some());
        assert_eq!(
            request.network_policy_digest,
            NETWORK_POLICY_DIGEST.to_vec()
        );
        assert_eq!(
            request.image.unwrap().immutable_reference,
            Some(wire::image::ImmutableReference::CustomDigest(
                IMAGE_DIGEST.to_vec()
            ))
        );
    }
}
