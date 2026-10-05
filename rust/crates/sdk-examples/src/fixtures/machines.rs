//! Rust-owned typed Machines RPC collector.
//!
//! This collector invokes the all-method Rust fixture directly. It records the
//! exact protobuf request and response frames plus the response fields that the
//! fixture populated. It is intentionally separate from `machines_scenarios`:
//! that scenario proves the process-local provider lifecycle, while this file
//! proves the generated wire surface for every Machines RPC.

use crate::tls_fixture::{AllRoutesMachinesFixture, MACHINES_RPC_METHODS};
use acyclic_machines::wire;
use futures::StreamExt;
use prost::Message;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tonic::Request;

/// One typed request/response observation from the Rust Machines fixture.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MachinesRpcObservation {
    /// Fully-qualified Rust descriptor RPC identity.
    pub rpc: &'static str,
    /// Fully-qualified protobuf request identity.
    pub request_type: &'static str,
    /// Fully-qualified protobuf response identity.
    pub response_type: &'static str,
    /// Protobuf method shape (`unary` or `server`).
    pub shape: &'static str,
    /// Rust-populated response field names in descriptor spelling.
    pub populated_fields: &'static [&'static str],
    /// Canonical encoded request bytes.
    pub request_bytes: Vec<u8>,
    /// Canonical encoded response frames in wire order.
    pub response_frames: Vec<Vec<u8>>,
}

fn sha256(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn encoded<M: Message>(message: &M) -> Vec<u8> {
    message.encode_to_vec()
}

fn unary<Req: Message, Res: Message>(
    rpc: &'static str,
    request_type: &'static str,
    response_type: &'static str,
    populated_fields: &'static [&'static str],
    request: &Req,
    response: &Res,
) -> MachinesRpcObservation {
    MachinesRpcObservation {
        rpc,
        request_type,
        response_type,
        shape: "unary",
        populated_fields,
        request_bytes: encoded(request),
        response_frames: vec![encoded(response)],
    }
}

fn protocol() -> wire::ProtocolVersion {
    wire::ProtocolVersion { major: 1, minor: 1 }
}

fn machine(value: u8) -> wire::MachineId {
    wire::MachineId {
        value: vec![value; 16],
    }
}

fn checkpoint(value: u8) -> wire::CheckpointId {
    wire::CheckpointId {
        value: vec![value; 16],
    }
}

fn operation(value: u8) -> wire::OperationId {
    wire::OperationId {
        value: vec![value; 16],
    }
}

fn idempotency(value: u8) -> wire::IdempotencyKey {
    wire::IdempotencyKey {
        value: vec![value; 16],
    }
}

fn image() -> wire::Image {
    wire::Image {
        kind: wire::ImageKind::Custom as i32,
        immutable_reference: Some(wire::image::ImmutableReference::CustomDigest(vec![7; 32])),
    }
}

fn create_request() -> wire::CreateMachineRequest {
    wire::CreateMachineRequest {
        protocol: Some(protocol()),
        idempotency_key: Some(idempotency(1)),
        image: Some(image()),
        compatibility: Some(wire::CompatibilityPolicy {
            mode: wire::CompatibilityMode::Require as i32,
            required: vec![wire::Capability::LiveCheckpoint as i32],
        }),
        suspension: Some(wire::SuspensionPolicy {
            policy: Some(wire::suspension_policy::Policy::AfterIdleMs(15_000)),
        }),
        expiration: Some(wire::ExpirationPolicy {
            kind: wire::ExpirationKind::Never as i32,
            value_ms: 0,
        }),
        network_policy_digest: vec![8; 32],
        budgets: Some(wire::Budgets {
            spend_micros: 0,
            concurrency: 0,
        }),
    }
}

/// Invoke each of the 19 Rust Machines methods and collect typed observations.
pub async fn collect() -> Result<Vec<MachinesRpcObservation>, String> {
    use wire::machines_service_server::MachinesService;

    let fixture = AllRoutesMachinesFixture::default();
    let mut observations = Vec::with_capacity(MACHINES_RPC_METHODS.len());

    let request = wire::QualifyImageRequest {
        protocol: Some(protocol()),
        image: Some(image()),
    };
    let response = fixture
        .qualify_image(Request::new(request.clone()))
        .await
        .map_err(|error| format!("Machines QualifyImage: {error}"))?
        .into_inner();
    observations.push(unary(
        "acyclic.machines.v1.MachinesService/QualifyImage",
        "acyclic.machines.v1.QualifyImageRequest",
        "acyclic.machines.v1.ImageQualification",
        &["capabilities", "compatibilityRevision"],
        &request,
        &response,
    ));

    let request = create_request();
    let response = fixture
        .create(Request::new(request.clone()))
        .await
        .map_err(|error| format!("Machines Create: {error}"))?
        .into_inner();
    observations.push(unary(
        "acyclic.machines.v1.MachinesService/Create",
        "acyclic.machines.v1.CreateMachineRequest",
        "acyclic.machines.v1.MachineAdmission",
        &["machine", "operation", "contract"],
        &request,
        &response,
    ));
    let created_machine = response
        .machine
        .clone()
        .ok_or_else(|| "Machines Create returned no machine identity".to_owned())?;
    let created_operation = response
        .operation
        .clone()
        .ok_or_else(|| "Machines Create returned no operation identity".to_owned())?;

    let request = wire::CheckpointMachineRequest {
        protocol: Some(protocol()),
        idempotency_key: Some(idempotency(2)),
        machine: Some(created_machine.clone()),
    };
    let response = fixture
        .checkpoint(Request::new(request.clone()))
        .await
        .map_err(|error| format!("Machines Checkpoint: {error}"))?
        .into_inner();
    observations.push(unary(
        "acyclic.machines.v1.MachinesService/Checkpoint",
        "acyclic.machines.v1.CheckpointMachineRequest",
        "acyclic.machines.v1.CheckpointAdmission",
        &["checkpoint", "source", "operation", "contract"],
        &request,
        &response,
    ));
    let created_checkpoint = response
        .checkpoint
        .clone()
        .ok_or_else(|| "Machines Checkpoint returned no checkpoint identity".to_owned())?;

    let request = wire::ForkCheckpointRequest {
        protocol: Some(protocol()),
        idempotency_key: Some(idempotency(3)),
        checkpoint: Some(created_checkpoint.clone()),
        count: 2,
    };
    let response = fixture
        .fork(Request::new(request.clone()))
        .await
        .map_err(|error| format!("Machines Fork: {error}"))?
        .into_inner();
    observations.push(unary(
        "acyclic.machines.v1.MachinesService/Fork",
        "acyclic.machines.v1.ForkCheckpointRequest",
        "acyclic.machines.v1.ForkAdmission",
        &["children", "operation", "contract"],
        &request,
        &response,
    ));

    let request = wire::ForkMachineRequest {
        protocol: Some(protocol()),
        idempotency_key: Some(idempotency(4)),
        machine: Some(created_machine.clone()),
        count: 2,
    };
    let response = fixture
        .fork_machine(Request::new(request.clone()))
        .await
        .map_err(|error| format!("Machines ForkMachine: {error}"))?
        .into_inner();
    observations.push(unary(
        "acyclic.machines.v1.MachinesService/ForkMachine",
        "acyclic.machines.v1.ForkMachineRequest",
        "acyclic.machines.v1.ForkMachineAdmission",
        &["children", "fidelity", "source", "operation", "contract"],
        &request,
        &response,
    ));
    let fork_child = response
        .children
        .first()
        .cloned()
        .ok_or_else(|| "Machines ForkMachine returned no child identity".to_owned())?;

    let request = wire::MachineMutationRequest {
        protocol: Some(protocol()),
        idempotency_key: Some(idempotency(5)),
        machine: Some(created_machine.clone()),
    };
    let response = fixture
        .suspend(Request::new(request.clone()))
        .await
        .map_err(|error| format!("Machines Suspend: {error}"))?
        .into_inner();
    observations.push(unary(
        "acyclic.machines.v1.MachinesService/Suspend",
        "acyclic.machines.v1.MachineMutationRequest",
        "acyclic.machines.v1.MutationAdmission",
        &["operation", "machine"],
        &request,
        &response,
    ));

    let request = wire::MachineMutationRequest {
        protocol: Some(protocol()),
        idempotency_key: Some(idempotency(6)),
        machine: Some(created_machine.clone()),
    };
    let response = fixture
        .wake(Request::new(request.clone()))
        .await
        .map_err(|error| format!("Machines Wake: {error}"))?
        .into_inner();
    observations.push(unary(
        "acyclic.machines.v1.MachinesService/Wake",
        "acyclic.machines.v1.MachineMutationRequest",
        "acyclic.machines.v1.MutationAdmission",
        &["operation", "machine"],
        &request,
        &response,
    ));

    let request = wire::SetSuspensionPolicyRequest {
        protocol: Some(protocol()),
        idempotency_key: Some(idempotency(7)),
        machine: Some(created_machine.clone()),
        policy: Some(wire::SuspensionPolicy {
            policy: Some(wire::suspension_policy::Policy::Manual(true)),
        }),
    };
    let response = fixture
        .set_suspension_policy(Request::new(request.clone()))
        .await
        .map_err(|error| format!("Machines SetSuspensionPolicy: {error}"))?
        .into_inner();
    observations.push(unary(
        "acyclic.machines.v1.MachinesService/SetSuspensionPolicy",
        "acyclic.machines.v1.SetSuspensionPolicyRequest",
        "acyclic.machines.v1.PolicyAdmission",
        &["machine", "operation"],
        &request,
        &response,
    ));

    let request = wire::MachineMutationRequest {
        protocol: Some(protocol()),
        idempotency_key: Some(idempotency(8)),
        machine: Some(fork_child),
    };
    let response = fixture
        .destroy_machine(Request::new(request.clone()))
        .await
        .map_err(|error| format!("Machines DestroyMachine: {error}"))?
        .into_inner();
    observations.push(unary(
        "acyclic.machines.v1.MachinesService/DestroyMachine",
        "acyclic.machines.v1.MachineMutationRequest",
        "acyclic.machines.v1.MutationAdmission",
        &["operation", "machine"],
        &request,
        &response,
    ));

    let request = wire::CheckpointMutationRequest {
        protocol: Some(protocol()),
        idempotency_key: Some(idempotency(9)),
        checkpoint: Some(created_checkpoint.clone()),
    };
    let response = fixture
        .destroy_checkpoint(Request::new(request.clone()))
        .await
        .map_err(|error| format!("Machines DestroyCheckpoint: {error}"))?
        .into_inner();
    observations.push(unary(
        "acyclic.machines.v1.MachinesService/DestroyCheckpoint",
        "acyclic.machines.v1.CheckpointMutationRequest",
        "acyclic.machines.v1.MutationAdmission",
        &["operation", "checkpoint"],
        &request,
        &response,
    ));

    let request = wire::RecoverRequest {
        protocol: Some(protocol()),
        idempotency_key: Some(idempotency(1)),
    };
    let response = fixture
        .recover(Request::new(request.clone()))
        .await
        .map_err(|error| format!("Machines Recover: {error}"))?
        .into_inner();
    observations.push(unary(
        "acyclic.machines.v1.MachinesService/Recover",
        "acyclic.machines.v1.RecoverRequest",
        "acyclic.machines.v1.RecoveredAdmission",
        &["result", "operation"],
        &request,
        &response,
    ));

    let request = wire::InspectMachineRequest {
        protocol: Some(protocol()),
        machine: Some(created_machine.clone()),
    };
    let response = fixture
        .inspect_machine(Request::new(request.clone()))
        .await
        .map_err(|error| format!("Machines InspectMachine: {error}"))?
        .into_inner();
    observations.push(unary(
        "acyclic.machines.v1.MachinesService/InspectMachine",
        "acyclic.machines.v1.InspectMachineRequest",
        "acyclic.machines.v1.MachineState",
        &[
            "status",
            "endpoints",
            "createdAtUnixMs",
            "changedAtUnixMs",
            "machine",
            "contract",
            "lastCheckpoint",
        ],
        &request,
        &response,
    ));

    let request = wire::InspectCheckpointRequest {
        protocol: Some(protocol()),
        checkpoint: Some(created_checkpoint.clone()),
    };
    let response = fixture
        .inspect_checkpoint(Request::new(request.clone()))
        .await
        .map_err(|error| format!("Machines InspectCheckpoint: {error}"))?
        .into_inner();
    observations.push(unary(
        "acyclic.machines.v1.MachinesService/InspectCheckpoint",
        "acyclic.machines.v1.InspectCheckpointRequest",
        "acyclic.machines.v1.CheckpointState",
        &[
            "forkable",
            "createdAtUnixMs",
            "checkpoint",
            "source",
            "contract",
        ],
        &request,
        &response,
    ));

    let request = wire::ListMachinesRequest {
        protocol: Some(protocol()),
        after: None,
        limit: 16,
    };
    let response = fixture
        .list_machines(Request::new(request.clone()))
        .await
        .map_err(|error| format!("Machines ListMachines: {error}"))?
        .into_inner();
    observations.push(unary(
        "acyclic.machines.v1.MachinesService/ListMachines",
        "acyclic.machines.v1.ListMachinesRequest",
        "acyclic.machines.v1.MachinePage",
        &["machines"],
        &request,
        &response,
    ));

    let request = wire::EventsRequest {
        protocol: Some(protocol()),
        machine: Some(created_machine.clone()),
        after_sequence: 0,
        limit: 16,
    };
    let response = fixture
        .events(Request::new(request.clone()))
        .await
        .map_err(|error| format!("Machines Events: {error}"))?
        .into_inner();
    observations.push(unary(
        "acyclic.machines.v1.MachinesService/Events",
        "acyclic.machines.v1.EventsRequest",
        "acyclic.machines.v1.EventPage",
        &["events", "nextSequence"],
        &request,
        &response,
    ));

    let request = wire::UsageRequest {
        protocol: Some(protocol()),
        machine: Some(created_machine.clone()),
        start_unix_ms: 0,
        end_unix_ms: 1,
    };
    let response = fixture
        .usage(Request::new(request.clone()))
        .await
        .map_err(|error| format!("Machines Usage: {error}"))?
        .into_inner();
    observations.push(unary(
        "acyclic.machines.v1.MachinesService/Usage",
        "acyclic.machines.v1.UsageRequest",
        "acyclic.machines.v1.UsageReceipt",
        &[
            "startUnixMs",
            "endUnixMs",
            "elasticCpuNs",
            "dedicatedCpuNs",
            "privateResidentByteSeconds",
            "durablePrivateBytes",
            "lineageReceiptSha256",
            "egressBytes",
            "receipt",
            "machine",
        ],
        &request,
        &response,
    ));

    let request = wire::OperationRequest {
        protocol: Some(protocol()),
        operation: Some(created_operation.clone()),
    };
    let response = fixture
        .cancel(Request::new(request.clone()))
        .await
        .map_err(|error| format!("Machines Cancel: {error}"))?
        .into_inner();
    observations.push(unary(
        "acyclic.machines.v1.MachinesService/Cancel",
        "acyclic.machines.v1.OperationRequest",
        "acyclic.machines.v1.OperationState",
        &["status", "operation"],
        &request,
        &response,
    ));

    let request = wire::OperationRequest {
        protocol: Some(protocol()),
        operation: Some(created_operation.clone()),
    };
    let response = fixture
        .inspect_operation(Request::new(request.clone()))
        .await
        .map_err(|error| format!("Machines InspectOperation: {error}"))?
        .into_inner();
    observations.push(unary(
        "acyclic.machines.v1.MachinesService/InspectOperation",
        "acyclic.machines.v1.OperationRequest",
        "acyclic.machines.v1.OperationState",
        &["status", "operation"],
        &request,
        &response,
    ));

    let request = wire::OperationRequest {
        protocol: Some(protocol()),
        operation: Some(created_operation.clone()),
    };
    let mut stream = fixture
        .watch_operation(Request::new(request.clone()))
        .await
        .map_err(|error| format!("Machines WatchOperation: {error}"))?
        .into_inner();
    let mut response_frames = Vec::new();
    while let Some(response) = stream.next().await {
        response_frames.push(encoded(
            &response.map_err(|error| format!("Machines WatchOperation frame: {error}"))?,
        ));
    }
    observations.push(MachinesRpcObservation {
        rpc: "acyclic.machines.v1.MachinesService/WatchOperation",
        request_type: "acyclic.machines.v1.OperationRequest",
        response_type: "acyclic.machines.v1.OperationState",
        shape: "server",
        populated_fields: &["status", "operation"],
        request_bytes: encoded(&request),
        response_frames,
    });

    if observations.len() != MACHINES_RPC_METHODS.len()
        || observations
            .iter()
            .map(|observation| observation.rpc)
            .collect::<std::collections::BTreeSet<_>>()
            != MACHINES_RPC_METHODS.iter().copied().collect()
    {
        return Err(
            "Machines typed collector did not cover the complete 19-RPC Rust fixture".into(),
        );
    }
    Ok(observations)
}

impl MachinesRpcObservation {
    /// Render one observation in the stable collector JSON shape.
    #[must_use]
    pub fn json(&self) -> Value {
        let frames = self
            .response_frames
            .iter()
            .enumerate()
            .map(|(index, bytes)| {
                json!({
                    "sequence": index,
                    "bytes_hex": hex::encode(bytes),
                    "byte_length": bytes.len(),
                    "sha256": sha256(bytes),
                })
            })
            .collect::<Vec<_>>();
        json!({
            "rpc": self.rpc,
            "shape": self.shape,
            "request": {
                "type": self.request_type,
                "bytes_hex": hex::encode(&self.request_bytes),
                "byte_length": self.request_bytes.len(),
                "sha256": sha256(&self.request_bytes),
            },
            "response": {
                "type": self.response_type,
                "frame_count": self.response_frames.len(),
                "frames": frames,
                "populated_fields": self.populated_fields,
            },
        })
    }
}

/// Render all 19 typed observations as a Rust-source-bound manifest.
pub async fn collect_json() -> Result<Value, String> {
    let observations = collect().await?;
    Ok(json!({
        "schema": "acyclic.sdk.machines-rust-typed-collector.v1",
        "authority": "rust",
        "family": "machines",
        "source": {
            "path": "rust/crates/sdk-examples",
            "sha256": option_env!("SDK_EXAMPLES_SOURCE_SHA256"),
            "build_target": option_env!("SDK_EXAMPLES_BUILD_TARGET"),
        },
        "rpc_count": observations.len(),
        "methods": observations.iter().map(MachinesRpcObservation::json).collect::<Vec<_>>(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn collector_executes_every_machines_rpc() {
        let observations = collect().await.expect("all Rust Machines fixture RPCs");
        assert_eq!(observations.len(), 19);
        assert_eq!(
            observations
                .iter()
                .map(|observation| observation.rpc)
                .collect::<std::collections::BTreeSet<_>>(),
            MACHINES_RPC_METHODS.iter().copied().collect()
        );
        assert!(
            observations
                .iter()
                .all(|observation| !observation.request_bytes.is_empty())
        );
        assert!(
            observations
                .iter()
                .all(|observation| !observation.response_frames.is_empty())
        );
        assert_eq!(
            observations
                .iter()
                .find(|observation| observation.rpc.ends_with("/WatchOperation"))
                .unwrap()
                .response_frames
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn collector_json_is_typed_and_hash_bound() {
        let manifest = collect_json().await.expect("collector JSON");
        assert_eq!(
            manifest["schema"],
            "acyclic.sdk.machines-rust-typed-collector.v1"
        );
        assert_eq!(manifest["rpc_count"], 19);
        for method in manifest["methods"].as_array().expect("methods") {
            assert!(method["request"]["type"].as_str().is_some());
            assert!(
                method["request"]["sha256"]
                    .as_str()
                    .unwrap()
                    .starts_with("sha256:")
            );
            assert!(method["response"]["type"].as_str().is_some());
            assert!(
                !method["response"]["populated_fields"]
                    .as_array()
                    .unwrap()
                    .is_empty()
            );
        }
    }
}
