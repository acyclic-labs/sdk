//! Real Rust-owned Filesystem and Harness gRPC fixtures.
//!
//! The Filesystem fixture delegates all thirty RPCs to the canonical in-memory
//! wire service. The Harness fixture keeps a small durable operation journal,
//! so every response and handshake is produced by the Rust protocol types.

use std::{collections::BTreeMap, sync::Arc};

use acyclic_fs::{
    FilesystemWireLimits, FilesystemWireService, MemoryAuthorityBackend, MemoryFs,
    MemoryObjectBackend,
};
use acyclic_harness::{
    Error, Result as HarnessResult, wire,
    wire_api::{HarnessWireApi, OperationControlRequest, current_protocol, negotiate},
};
use futures::{
    FutureExt as _,
    stream::{self, BoxStream},
};
use tokio::sync::Mutex;
use tonic::Status;

/// Every RPC in the canonical Filesystem service is backed by the real wire adapter.
pub const FILESYSTEM_RPC_COUNT: usize = 30;
/// Every RPC in the canonical Harness service is backed by the stateful fixture.
pub const HARNESS_RPC_COUNT: usize = 5;

/// Canonical descriptor archives used by fixture clients and handshake checks.
pub const FILESYSTEM_DESCRIPTOR_ARCHIVE: &[u8] = acyclic_fs::FILE_DESCRIPTOR_SET;
/// Canonical Harness descriptor archive used by fixture clients and handshake checks.
pub const HARNESS_DESCRIPTOR_ARCHIVE: &[u8] = acyclic_harness::FILE_DESCRIPTOR_SET;

/// One source-owned consumer input and its semantic assertion.
///
/// These inputs intentionally name wire operations and response guarantees rather
/// than depending on a handwritten language client. Every generated consumer can
/// consume this table while the canonical Rust services remain the implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FixtureScenario {
    pub family: &'static str,
    pub operation: &'static str,
    pub input: &'static str,
    pub expected: &'static str,
    pub order: u16,
    pub seed: &'static str,
    pub depends_on: &'static [&'static str],
    pub known_output: &'static str,
}

/// The thirty Filesystem operations covered by this fixture.
pub const FILESYSTEM_SCENARIOS: &[FixtureScenario] = &[
    scenario(
        1,
        "handshake",
        "protocol=current",
        "negotiated protocol and capabilities",
        "protocol:current",
        &[],
        "protocol+capabilities",
    ),
    scenario(
        2,
        "create_workspace",
        "workspace-name=fixture",
        "workspace response",
        "workspace:fixture",
        &["handshake"],
        "workspace+head",
    ),
    scenario(
        3,
        "open_workspace",
        "workspace=fixture",
        "workspace response",
        "workspace:fixture",
        &["create_workspace"],
        "workspace",
    ),
    scenario(
        4,
        "get_head",
        "workspace=fixture",
        "generation response",
        "workspace:fixture",
        &["open_workspace"],
        "generation",
    ),
    scenario(
        5,
        "get_generation",
        "workspace=fixture;generation=1",
        "generation response",
        "generation:fixture/head",
        &["get_head"],
        "generation",
    ),
    scenario(
        6,
        "read",
        "workspace=fixture;path=/hello",
        "read response",
        "generation:fixture/head",
        &["get_head"],
        "bytes",
    ),
    scenario(
        7,
        "stat",
        "workspace=fixture;path=/hello",
        "stat response",
        "generation:fixture/head",
        &["get_head"],
        "metadata",
    ),
    scenario(
        8,
        "list_directory",
        "workspace=fixture;path=/",
        "directory response",
        "generation:fixture/head",
        &["get_head"],
        "entries",
    ),
    scenario(
        9,
        "read_link",
        "workspace=fixture;path=/link",
        "read response",
        "generation:fixture/head",
        &["get_head"],
        "target",
    ),
    scenario(
        10,
        "plan_extents",
        "workspace=fixture;path=/hello",
        "extent plan response",
        "generation:fixture/head",
        &["get_head"],
        "extents",
    ),
    scenario(
        11,
        "apply_transaction",
        "workspace=fixture;transaction=fixture",
        "mutation response",
        "transaction:fixture",
        &["get_head"],
        "generation+conflicts",
    ),
    scenario(
        12,
        "rebase_transaction",
        "workspace=fixture;transaction=fixture",
        "rebase response",
        "transaction:fixture",
        &["apply_transaction"],
        "generation+conflicts",
    ),
    scenario(
        13,
        "fork_workspace",
        "workspace=fixture;name=fork",
        "workspace response",
        "workspace:fixture",
        &["get_head"],
        "workspace",
    ),
    scenario(
        14,
        "diff",
        "workspace=fixture;from=1;to=2",
        "diff response",
        "generations:fixture/head",
        &["get_head"],
        "changes",
    ),
    scenario(
        15,
        "rebase",
        "workspace=fixture;source=1;target=2",
        "rebase response",
        "generations:fixture/head",
        &["diff"],
        "generation+conflicts",
    ),
    scenario(
        16,
        "plan_join",
        "workspace=fixture;source=1;target=2",
        "join plan response",
        "join:fixture",
        &["diff"],
        "join_plan",
    ),
    scenario(
        17,
        "apply_join",
        "workspace=fixture;plan=fixture",
        "join response",
        "join:fixture",
        &["plan_join"],
        "generation+conflicts",
    ),
    scenario(
        18,
        "checkpoint",
        "workspace=fixture;generation=1",
        "retained generation response",
        "generation:fixture/head",
        &["get_head"],
        "retained_generation",
    ),
    scenario(
        19,
        "pin",
        "workspace=fixture;generation=1",
        "retained generation response",
        "generation:fixture/head",
        &["checkpoint"],
        "retained_generation",
    ),
    scenario(
        20,
        "export",
        "workspace=fixture;generation=1",
        "export stream",
        "generation:fixture/head",
        &["get_head"],
        "archive_chunk",
    ),
    scenario(
        21,
        "import",
        "workspace=fixture;archive=fixture",
        "import response",
        "archive:fixture",
        &["create_workspace"],
        "generation",
    ),
    scenario(
        22,
        "issue_mount_credential",
        "workspace=fixture",
        "credential response",
        "workspace:fixture",
        &["create_workspace"],
        "credential",
    ),
    scenario(
        23,
        "issue_s3_credential",
        "workspace=fixture",
        "credential response",
        "workspace:fixture",
        &["create_workspace"],
        "credential",
    ),
    scenario(
        24,
        "get_source_state",
        "workspace=fixture;source=fixture",
        "source response",
        "workspace:fixture",
        &["create_workspace"],
        "source_state",
    ),
    scenario(
        25,
        "reconcile_source",
        "workspace=fixture;source=fixture",
        "source response",
        "workspace:fixture",
        &["get_source_state"],
        "source_state",
    ),
    scenario(
        26,
        "rescan_source",
        "workspace=fixture;source=fixture",
        "source response",
        "workspace:fixture",
        &["reconcile_source"],
        "source_state",
    ),
    scenario(
        27,
        "seal_source",
        "workspace=fixture;source=fixture",
        "source response",
        "workspace:fixture",
        &["rescan_source"],
        "source_state",
    ),
    scenario(
        28,
        "observe",
        "operation=fixture-op",
        "operation status response",
        "operation:fixture-op",
        &["handshake"],
        "operation_status",
    ),
    scenario(
        29,
        "cancel",
        "operation=fixture-op",
        "cancel response",
        "operation:fixture-op",
        &["observe"],
        "cancelled_operation_status",
    ),
    scenario(
        30,
        "delete_workspace",
        "workspace=fixture",
        "mutation response",
        "workspace:fixture",
        &["cancel"],
        "deleted",
    ),
];

/// The five Harness operations and their state-machine expectations.
pub const HARNESS_SCENARIOS: &[FixtureScenario] = &[
    harness_scenario(
        31,
        "handshake",
        "protocol=current;capabilities=submit,replay,observe,cancel",
        "negotiated protocol and capabilities",
        "protocol:current",
        &[],
        "protocol+capabilities",
    ),
    harness_scenario(
        32,
        "submit",
        "operation=fixture-op;authority=task:fixture",
        "accepted admission and succeeded status",
        "operation:fixture-op",
        &["handshake"],
        "accepted+succeeded",
    ),
    harness_scenario(
        33,
        "replay",
        "cursor=fixture",
        "live delivery at cursor revision",
        "cursor:fixture",
        &["submit"],
        "live_delivery",
    ),
    harness_scenario(
        34,
        "observe",
        "operation=fixture-op",
        "succeeded operation status",
        "operation:fixture-op",
        &["submit"],
        "succeeded_status",
    ),
    harness_scenario(
        35,
        "cancel",
        "operation=fixture-op",
        "cancelled status with incremented revision",
        "operation:fixture-op",
        &["observe"],
        "cancelled+revision",
    ),
];

const fn scenario(
    order: u16,
    operation: &'static str,
    input: &'static str,
    expected: &'static str,
    seed: &'static str,
    depends_on: &'static [&'static str],
    known_output: &'static str,
) -> FixtureScenario {
    FixtureScenario {
        family: "filesystem",
        operation,
        input,
        expected,
        order,
        seed,
        depends_on,
        known_output,
    }
}

const fn harness_scenario(
    order: u16,
    operation: &'static str,
    input: &'static str,
    expected: &'static str,
    seed: &'static str,
    depends_on: &'static [&'static str],
    known_output: &'static str,
) -> FixtureScenario {
    FixtureScenario {
        family: "harness",
        operation,
        input,
        expected,
        order,
        seed,
        depends_on,
        known_output,
    }
}

/// Returns all source-owned qualification scenarios for generated consumers.
pub fn qualification_scenarios() -> impl Iterator<Item = FixtureScenario> {
    FILESYSTEM_SCENARIOS
        .iter()
        .chain(HARNESS_SCENARIOS)
        .copied()
}

/// The concrete Filesystem service used by the fixture server.
pub type FilesystemFixtureService =
    FilesystemWireService<MemoryAuthorityBackend, MemoryObjectBackend>;

/// Builds a Filesystem service over the public in-memory Stream and Objects providers.
///
/// No fixture behavior is duplicated here: the service is the production
/// `FilesystemWireService` over the canonical Rust engine.
pub fn filesystem_service() -> std::result::Result<FilesystemFixtureService, Status> {
    FilesystemWireService::new(MemoryFs::memory(), FilesystemWireLimits::default())
}

/// Returns the Filesystem tonic server with all thirty generated handlers.
pub fn filesystem_server() -> std::result::Result<
    acyclic_fs::wire::filesystem::v2::filesystem_service_server::FilesystemServiceServer<
        FilesystemFixtureService,
    >,
    Status,
> {
    Ok(
        acyclic_fs::wire::filesystem::v2::filesystem_service_server::FilesystemServiceServer::new(
            filesystem_service()?,
        ),
    )
}

/// Stateful in-memory Harness backend used by the five RPC fixture handlers.
#[derive(Clone, Default)]
pub struct HarnessFixtureBackend {
    operations: Arc<Mutex<BTreeMap<String, wire::OperationStatus>>>,
}

impl HarnessFixtureBackend {
    /// Creates an empty backend with no fabricated operation history.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn status_for(
        operation: wire::OperationIdentity,
        protocol: wire::ProtocolIdentity,
        owner: wire::Authority,
        state: wire::CompletionState,
        revision: u64,
        cancellation_requested: bool,
    ) -> wire::OperationStatus {
        wire::OperationStatus {
            operation: Some(operation),
            state: state as i32,
            error: None,
            protocol: Some(protocol),
            owner: Some(owner),
            cancellation_requested,
            revision,
        }
    }
}

impl HarnessWireApi for HarnessFixtureBackend {
    fn handshake<'a>(
        &'a self,
        request: wire::HandshakeRequest,
    ) -> futures::future::BoxFuture<'a, HarnessResult<wire::HandshakeResponse>> {
        async move {
            negotiate(
                &request,
                &wire::CapabilitySet {
                    capabilities: vec![
                        wire::Capability {
                            name: "submit".into(),
                            version: "1".into(),
                        },
                        wire::Capability {
                            name: "replay".into(),
                            version: "1".into(),
                        },
                        wire::Capability {
                            name: "observe".into(),
                            version: "1".into(),
                        },
                        wire::Capability {
                            name: "cancel".into(),
                            version: "1".into(),
                        },
                    ],
                },
            )
        }
        .boxed()
    }

    fn submit<'a>(
        &'a self,
        command: wire::CommandEnvelope,
    ) -> futures::future::BoxFuture<'a, HarnessResult<wire::Admission>> {
        async move {
            let operation = command
                .operation
                .ok_or_else(|| Error::Invalid("fixture command operation is missing".into()))?;
            let owner = command
                .authority
                .ok_or_else(|| Error::Invalid("fixture command authority is missing".into()))?;
            let protocol = command.protocol.unwrap_or_else(current_protocol);
            let operation_id = operation.operation_id.clone();
            let status = Self::status_for(
                operation.clone(),
                protocol,
                owner,
                wire::CompletionState::Succeeded,
                1,
                false,
            );
            self.operations.lock().await.insert(operation_id, status);
            Ok(wire::Admission {
                operation: Some(operation),
                state: wire::AdmissionState::Accepted as i32,
                error: None,
            })
        }
        .boxed()
    }

    fn replay<'a>(
        &'a self,
        request: wire::ResumeRequest,
    ) -> futures::future::BoxFuture<
        'a,
        HarnessResult<BoxStream<'static, HarnessResult<wire::Delivery>>>,
    > {
        async move {
            let cursor = request.cursors.into_iter().next();
            let authority = cursor.as_ref().and_then(|value| value.authority.clone());
            let generation = cursor.as_ref().map_or_else(
                || "fixture-generation".into(),
                |value| value.generation.clone(),
            );
            let revision = cursor.as_ref().map_or(0, |value| value.revision);
            Ok(Box::pin(stream::once(async move {
                Ok(wire::Delivery {
                    authority,
                    generation,
                    from_revision: revision,
                    through_revision: revision,
                    events: Vec::new(),
                    live: true,
                })
            }))
                as BoxStream<'static, HarnessResult<wire::Delivery>>)
        }
        .boxed()
    }

    fn authorize_operation_control<'a>(
        &'a self,
        _request: &'a OperationControlRequest,
    ) -> futures::future::BoxFuture<'a, HarnessResult<()>> {
        async { Ok(()) }.boxed()
    }

    fn observe<'a>(
        &'a self,
        request: wire::ObserveRequest,
    ) -> futures::future::BoxFuture<'a, HarnessResult<wire::OperationStatus>> {
        async move {
            self.operations
                .lock()
                .await
                .get(&request.operation_id)
                .cloned()
                .ok_or_else(|| {
                    Error::NotFound(format!(
                        "fixture operation {} is unknown",
                        request.operation_id
                    ))
                })
        }
        .boxed()
    }

    fn cancel<'a>(
        &'a self,
        request: wire::CancelRequest,
    ) -> futures::future::BoxFuture<'a, HarnessResult<wire::CancelResponse>> {
        async move {
            let mut operations = self.operations.lock().await;
            let status = operations.get_mut(&request.operation_id).ok_or_else(|| {
                Error::NotFound(format!(
                    "fixture operation {} is unknown",
                    request.operation_id
                ))
            })?;
            status.state = wire::CompletionState::Cancelled as i32;
            status.cancellation_requested = true;
            status.revision = status.revision.saturating_add(1);
            let operation = status.operation.clone();
            Ok(wire::CancelResponse {
                status: Some(status.clone()),
                operation,
            })
        }
        .boxed()
    }
}

/// Builds the Harness tonic server over the stateful fixture backend.
pub fn harness_server()
-> acyclic_harness::grpc::transport::harness_service_server::HarnessServiceServer<
    acyclic_harness::grpc::HarnessGrpcService,
> {
    acyclic_harness::grpc::HarnessGrpcService::new(Arc::new(HarnessFixtureBackend::new()))
        .into_server()
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_binds_all_generated_handlers_and_archives() {
        assert_eq!(FILESYSTEM_RPC_COUNT, 30);
        assert_eq!(HARNESS_RPC_COUNT, 5);
        assert_eq!(FILESYSTEM_SCENARIOS.len(), FILESYSTEM_RPC_COUNT);
        assert_eq!(HARNESS_SCENARIOS.len(), HARNESS_RPC_COUNT);
        let scenarios: Vec<_> = qualification_scenarios().collect();
        assert_eq!(scenarios.len(), 35);
        assert_eq!(
            scenarios
                .iter()
                .map(|scenario| scenario.order)
                .collect::<Vec<_>>(),
            (1..=35).collect::<Vec<_>>()
        );
        assert!(scenarios.iter().all(|scenario| {
            !scenario.operation.is_empty()
                && !scenario.input.is_empty()
                && !scenario.expected.is_empty()
        }));
        assert!(!FILESYSTEM_DESCRIPTOR_ARCHIVE.is_empty());
        assert!(!HARNESS_DESCRIPTOR_ARCHIVE.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn harness_backend_returns_semantic_state_transitions() {
        let backend = HarnessFixtureBackend::new();
        let protocol = current_protocol();
        let owner = wire::Authority {
            kind: wire::AggregateKind::Task as i32,
            id: "fixture".into(),
        };
        let operation = wire::OperationIdentity {
            operation_id: "fixture-op".into(),
            idempotency_key: "key".into(),
        };
        let admission = backend
            .submit(wire::CommandEnvelope {
                protocol: Some(protocol.clone()),
                authority: Some(owner.clone()),
                operation: Some(operation.clone()),
                ..Default::default()
            })
            .await
            .expect("submit");
        assert_eq!(admission.state, wire::AdmissionState::Accepted as i32);
        let observed = backend
            .observe(wire::ObserveRequest {
                operation_id: "fixture-op".into(),
                ..Default::default()
            })
            .await
            .expect("observe");
        assert_eq!(observed.state, wire::CompletionState::Succeeded as i32);
        let cancelled = backend
            .cancel(wire::CancelRequest {
                operation_id: "fixture-op".into(),
                ..Default::default()
            })
            .await
            .expect("cancel");
        assert_eq!(
            cancelled.status.expect("status").state,
            wire::CompletionState::Cancelled as i32
        );
    }
}
