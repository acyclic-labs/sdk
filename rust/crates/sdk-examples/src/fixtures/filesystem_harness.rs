//! Real Rust-owned Filesystem and Harness gRPC fixtures.
//!
//! The Filesystem fixture delegates all thirty RPCs to the canonical in-memory
//! wire service. The Harness fixture keeps a small durable operation journal,
//! so every response and handshake is produced by the Rust protocol types.

use std::{collections::BTreeMap, sync::Arc, task::{Context, Poll}};

use bytes::Bytes;

use acyclic_objects::v2::MemoryObjects;
use acyclic_stream::{MemoryLimits, MemoryStream, UnixMillisClock};
use acyclic_fs::{
    CancellationToken, FilesystemWireLimits, FilesystemWireService, MemoryAuthorityBackend,
    MemoryFs, MemoryObjectBackend, OperationId, WorkBudget,
    kernel::{LogicalName, NamespacePath},
    model::{AccessMode, CheckoutMode, ConsistencyMode, GenerationSelector, MutationMode},
};
use acyclic_fs::wire::filesystem::v2::filesystem_service_server::FilesystemService;
use acyclic_harness::{
    Error, Result as HarnessResult, wire,
    wire_api::{HarnessWireApi, OperationControlRequest, current_protocol, negotiate},
};
use futures::{
    FutureExt as _, StreamExt as _,
    stream::{self, BoxStream},
};
use tokio::sync::Mutex;
use tonic::codegen::{Body, BoxFuture, Service, StdError, http};
use tonic::{Request, Status};

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

/// Typed semantic result produced from the canonical fixture seed.
///
/// Consumers use these values as assertions after invoking the generated
/// operation. They are kept beside the Rust service implementation so a
/// language producer cannot replace them with a guessed response map.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScenarioExpectation {
    pub operation_id: &'static str,
    pub authority: &'static str,
    pub cursor: &'static str,
    pub status: &'static str,
    pub output: &'static str,
}

/// Returns the Rust-owned identity, state transition, and output expectation
/// for one ordered scenario.
#[must_use]
pub fn scenario_expectation(scenario: FixtureScenario) -> ScenarioExpectation {
    let expectation = match (scenario.family, scenario.operation) {
        ("filesystem", "handshake") => ("filesystem/handshake", "service:filesystem", "generation:fixture/head@0", "negotiated", "protocol=current;capabilities=30"),
        ("filesystem", "create_workspace") => ("workspace/create/fixture", "workspace:fixture", "generation:fixture/head@0", "created", "workspace=fixture;head=0"),
        ("filesystem", "open_workspace") => ("workspace/open/fixture", "workspace:fixture", "generation:fixture/head@0", "opened", "workspace=fixture"),
        ("filesystem", "get_head") => ("workspace/head/fixture", "workspace:fixture", "generation:fixture/head@0", "observed", "generation=fixture/head"),
        ("filesystem", "get_generation") => ("workspace/generation/fixture/1", "workspace:fixture", "generation:fixture/head@0", "observed", "generation=fixture/head"),
        ("filesystem", "read") => ("workspace/read/fixture/hello", "workspace:fixture", "generation:fixture/head@0", "ok", "path=/hello;bytes=rust-fixture"),
        ("filesystem", "stat") => ("workspace/stat/fixture/hello", "workspace:fixture", "generation:fixture/head@0", "ok", "path=/hello;size=12"),
        ("filesystem", "list_directory") => ("workspace/list/fixture/root", "workspace:fixture", "generation:fixture/head@0", "ok", "entries=[hello,link]"),
        ("filesystem", "read_link") => ("workspace/read-link/fixture/link", "workspace:fixture", "generation:fixture/head@0", "ok", "path=/link;target=hello"),
        ("filesystem", "plan_extents") => ("workspace/plan-extents/fixture/hello", "workspace:fixture", "generation:fixture/head@0", "ok", "path=/hello;extents=1"),
        ("filesystem", "apply_transaction") => ("workspace/apply/fixture", "workspace:fixture", "generation:fixture/head@0", "ok", "generation=fixture/head;conflicts=0"),
        ("filesystem", "rebase_transaction") => ("workspace/rebase-transaction/fixture", "workspace:fixture", "generation:fixture/head@0", "ok", "generation=fixture/head;conflicts=0"),
        ("filesystem", "fork_workspace") => ("workspace/fork/fixture", "workspace:fixture", "generation:fixture/head@0", "created", "workspace=fork;parent=fixture"),
        ("filesystem", "diff") => ("workspace/diff/fixture", "workspace:fixture", "generation:fixture/head@0", "ok", "changes=[]"),
        ("filesystem", "rebase") => ("workspace/rebase/fixture", "workspace:fixture", "generation:fixture/head@0", "ok", "generation=fixture/head;conflicts=0"),
        ("filesystem", "plan_join") => ("workspace/plan-join/fixture", "workspace:fixture", "generation:fixture/head@0", "ok", "join_plan=fixture"),
        ("filesystem", "apply_join") => ("workspace/apply-join/fixture", "workspace:fixture", "generation:fixture/head@0", "ok", "generation=fixture/head;conflicts=0"),
        ("filesystem", "checkpoint") => ("workspace/checkpoint/fixture", "workspace:fixture", "generation:fixture/head@0", "retained", "generation=fixture/head"),
        ("filesystem", "pin") => ("workspace/pin/fixture", "workspace:fixture", "generation:fixture/head@0", "retained", "generation=fixture/head"),
        ("filesystem", "export") => ("workspace/export/fixture", "workspace:fixture", "generation:fixture/head@0", "ok", "archive=fixture;bytes=rust-fixture"),
        ("filesystem", "import") => ("workspace/import/fixture", "workspace:fixture", "generation:fixture/head@0", "ok", "generation=fixture/head"),
        ("filesystem", "issue_mount_credential") => ("workspace/mount-credential/fixture", "workspace:fixture", "generation:fixture/head@0", "ok", "credential=fixture"),
        ("filesystem", "issue_s3_credential") => ("workspace/s3-credential/fixture", "workspace:fixture", "generation:fixture/head@0", "ok", "credential=fixture"),
        ("filesystem", "get_source_state") => ("workspace/source-state/fixture", "workspace:fixture", "generation:fixture/head@0", "ok", "source_state=fixture"),
        ("filesystem", "reconcile_source") => ("workspace/reconcile-source/fixture", "workspace:fixture", "generation:fixture/head@0", "ok", "source_state=fixture"),
        ("filesystem", "rescan_source") => ("workspace/rescan-source/fixture", "workspace:fixture", "generation:fixture/head@0", "ok", "source_state=fixture"),
        ("filesystem", "seal_source") => ("workspace/seal-source/fixture", "workspace:fixture", "generation:fixture/head@0", "ok", "source_state=fixture"),
        ("filesystem", "observe") => ("workspace/observe/fixture-op", "workspace:fixture", "generation:fixture/head@0", "error:not_found", "operation=fixture-op;code=not_found"),
        ("filesystem", "cancel") => ("workspace/cancel/fixture-op", "workspace:fixture", "generation:fixture/head@0", "error:not_found", "operation=fixture-op;code=not_found"),
        ("filesystem", "delete_workspace") => ("workspace/delete/fixture", "workspace:fixture", "generation:fixture/head@0", "deleted", "workspace=fixture"),
        ("harness", "handshake") => ("harness/handshake", "task:fixture", "task:fixture-generation@0", "negotiated", "protocol=current;capabilities=[submit,replay,observe,cancel]"),
        ("harness", "submit") => ("fixture-op", "task:fixture", "task:fixture-generation@0", "accepted;revision=1", "event=fixture.command.accepted;revision=1"),
        ("harness", "replay") => ("fixture-op", "task:fixture", "task:fixture-generation@0", "delivered;revision=1", "live=true;events=1;event=fixture.command.accepted"),
        ("harness", "observe") => ("fixture-op", "task:fixture", "task:fixture-generation@1", "succeeded;revision=1", "operation=fixture-op;state=succeeded;revision=1"),
        ("harness", "cancel") => ("fixture-op", "task:fixture", "task:fixture-generation@1", "cancelled;revision=2", "operation=fixture-op;state=cancelled;revision=2"),
        _ => (scenario.operation, "fixture", "fixture@0", scenario.expected, scenario.known_output),
    };
    ScenarioExpectation {
        operation_id: expectation.0,
        authority: expectation.1,
        cursor: expectation.2,
        status: expectation.3,
        output: expectation.4,
    }
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
        "cursor=authority:task:fixture;generation=fixture-generation;revision=0",
        "live delivery at cursor revision",
        "cursor:fixture",
        &["submit"],
        "live_delivery",
    ),
    harness_scenario(
        34,
        "observe",
        "operation=fixture-op;owner=task:fixture",
        "succeeded operation status",
        "operation:fixture-op",
        &["submit"],
        "succeeded_status",
    ),
    harness_scenario(
        35,
        "cancel",
        "operation=fixture-op;owner=task:fixture",
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

/// The production MemoryStream records the commit clock in generation identity.
/// Keep fixture instances on one Rust-owned clock so independently-created
/// collectors and loopback servers share the same wire identity rules.
#[derive(Debug, Clone, Copy)]
struct FixtureClock;

impl UnixMillisClock for FixtureClock {
    fn now_unix_millis(&self) -> u64 {
        1_700_000_000_000
    }
}

fn deterministic_memory_fs() -> MemoryFs {
    let stream = Arc::new(MemoryStream::new_with_clock(
        MemoryLimits::default(),
        Arc::new(FixtureClock),
    ));
    let (objects, bucket) = MemoryObjects::with_default_bucket_clock(super::fixture_clock::objects_clock());
    MemoryFs::from_memory_providers(stream, Arc::new(objects), bucket)
}

fn fixture_path(name: &str) -> Result<NamespacePath, Status> {
    let component = LogicalName::new(
        acyclic_fs::kernel::NameEncoding::Utf8,
        name.as_bytes().to_vec(),
        acyclic_fs::model::VolumeLimits::default().maximum_component_bytes,
    )
    .map_err(|error| Status::internal(format!("fixture path: {error}")))?;
    NamespacePath::new(vec![component], acyclic_fs::model::VolumeLimits::default())
        .map_err(|error| Status::internal(format!("fixture path: {error}")))
}

fn seeded_memory_fs() -> Result<MemoryFs, Status> {
    let fs = deterministic_memory_fs();
    let cancellation = CancellationToken::new();
    futures::executor::block_on(async {
        let workspace = fs
            .create_workspace("fixture")
            .await
            .map_err(|error| Status::internal(format!("fixture workspace: {error}")))?;
        let mut checkout = workspace
            .checkout(
                GenerationSelector::Head,
                CheckoutMode {
                    access: AccessMode::ReadWrite,
                    consistency: ConsistencyMode::TrackingSafe,
                    mutations: MutationMode::PrivateOverlay,
                },
            )
            .await
            .map_err(|error| Status::internal(format!("fixture checkout: {error}")))?;
        // Bind authored identities to one Rust-owned operation key. The
        // facade derives stable file identities from this key while normal
        // production mutations keep their fresh UUID defaults.
        checkout.bind_authored_operation(OperationId::from_bytes([0x42; 16]));
        checkout
            .create_file(
                fixture_path("hello")?,
                Bytes::from_static(b"rust-fixture"),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(|error| Status::internal(format!("fixture seed: {}", error.error)))?;
        checkout
            .create_symbolic_link(
                fixture_path("link")?,
                Bytes::from_static(b"hello"),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(|error| Status::internal(format!("fixture symlink: {}", error.error)))?;
        checkout
            // The fixture is part of the reproducible manifest, so its
            // operation identity must be stable across clean rebuilds. The
            // production engine still computes the workspace and generation
            // identities from this real commit; this is only the deterministic
            // seed input.
            .commit(
                OperationId::from_bytes([0x42; 16]),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(|error| Status::internal(format!("fixture commit: {}", error.error)))?;
        Ok(fs)
    })
}

/// Builds a Filesystem service over the public in-memory Stream and Objects providers.
///
/// No fixture behavior is duplicated here: the service is the production
/// `FilesystemWireService` over the canonical Rust engine.
pub fn filesystem_service() -> std::result::Result<FilesystemFixtureService, Status> {
    FilesystemWireService::new(seeded_memory_fs()?, FilesystemWireLimits::default())
}

/// Ensure the deterministic transfer source used by the Export/Import scenario
/// exists on every hosted fixture instance. The producer and hosted service
/// both create it through the generated wire adapter with the same idempotency
/// key, so they share one Rust-owned workspace identity.
pub async fn ensure_transfer_source(
    service: &FilesystemFixtureService,
) -> std::result::Result<acyclic_fs::wire::filesystem::v2::Workspace, Status> {
    use acyclic_fs::wire::filesystem::v2::{
        OpenWorkspaceRequest, open_workspace_request::Selector,
    };

    if let Ok(response) = service
        .open_workspace(Request::new(OpenWorkspaceRequest {
            selector: Some(Selector::Name("scenario-export".to_owned())),
        }))
        .await
    {
        return response
            .into_inner()
            .workspace
            .ok_or_else(|| Status::internal("transfer source omitted workspace"));
    }

    let response = service
        .create_workspace(Request::new(
            acyclic_fs::wire::filesystem::v2::CreateWorkspaceRequest {
                name: "scenario-export".to_owned(),
                profile: acyclic_fs::wire::filesystem::v2::FilesystemProfile::Portable as i32,
                operation: Some(acyclic_fs::wire::filesystem::v2::OperationOptions {
                    idempotency_key: vec![0x12; 16],
                }),
            },
        ))
        .await?;
    response
        .into_inner()
        .workspace
        .ok_or_else(|| Status::internal("transfer source omitted workspace"))
}

/// Builds an empty production Filesystem service for an imported volume.
pub fn empty_filesystem_service() -> std::result::Result<FilesystemFixtureService, Status> {
    FilesystemWireService::new(deterministic_memory_fs(), FilesystemWireLimits::default())
}

/// Routes the positive Import transfer into a fresh Rust Filesystem service.
///
/// Export and Import intentionally use separate production contexts: the
/// source service owns `scenario-export`, while the destination service starts
/// empty and authenticates the imported closure before creating its authority.
/// This preserves the product's duplicate-create behavior while making a
/// long-lived hosted replay observe the same semantics as the Rust scenario.
#[derive(Clone)]
pub struct FilesystemTransferRouter {
    source: acyclic_fs::wire::filesystem::v2::filesystem_service_server::FilesystemServiceServer<
        FilesystemFixtureService,
    >,
    destination: acyclic_fs::wire::filesystem::v2::filesystem_service_server::FilesystemServiceServer<
        FilesystemFixtureService,
    >,
}

impl FilesystemTransferRouter {
    fn new(source: FilesystemFixtureService, destination: FilesystemFixtureService) -> Self {
        Self {
            source: acyclic_fs::wire::filesystem::v2::filesystem_service_server::FilesystemServiceServer::new(source),
            destination: acyclic_fs::wire::filesystem::v2::filesystem_service_server::FilesystemServiceServer::new(destination),
        }
    }
}

impl<B> Service<http::Request<B>> for FilesystemTransferRouter
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Generated tonic servers are always ready and perform their own
        // bounded request admission in `call`.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        if request.uri().path().ends_with("/Import") {
            self.destination.call(request)
        } else {
            self.source.call(request)
        }
    }
}

impl tonic::server::NamedService for FilesystemTransferRouter {
    const NAME: &'static str = "acyclic.filesystem.v2.FilesystemService";
}

/// Returns the Filesystem tonic server with all thirty generated handlers.
pub fn filesystem_server() -> std::result::Result<FilesystemTransferRouter, Status> {
    let service = filesystem_service()?;
    futures::executor::block_on(ensure_transfer_source(&service))?;
    let destination = empty_filesystem_service()?;
    Ok(FilesystemTransferRouter::new(service, destination))
}

/// Stateful in-memory Harness backend used by the five RPC fixture handlers.
#[derive(Clone, Debug)]
struct OperationRecord {
    status: wire::OperationStatus,
    event: wire::EventEnvelope,
}

#[derive(Clone, Default)]
pub struct HarnessFixtureBackend {
    operations: Arc<Mutex<BTreeMap<String, OperationRecord>>>,
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

fn wire_authority(owner: &acyclic_harness::core::Authority) -> wire::Authority {
    let kind = match owner.kind {
        acyclic_harness::core::AggregateKind::Agent => wire::AggregateKind::Agent,
        acyclic_harness::core::AggregateKind::Conversation => wire::AggregateKind::Conversation,
        acyclic_harness::core::AggregateKind::Session => wire::AggregateKind::Session,
        acyclic_harness::core::AggregateKind::Turn => wire::AggregateKind::Turn,
        acyclic_harness::core::AggregateKind::Task => wire::AggregateKind::Task,
    };
    wire::Authority {
        kind: kind as i32,
        id: owner.id.clone(),
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
            let event = wire::EventEnvelope {
                protocol: Some(protocol.clone()),
                authority: Some(owner.clone()),
                revision: 1,
                operation_id: operation_id.clone(),
                intent_digest: command.intent_digest.clone(),
                scope: command.scope.as_ref().map(|scope| wire::RecordedScope {
                    id: scope.id.clone(),
                    capabilities: scope.capabilities.clone(),
                    issuer: scope.issuer.clone(),
                    agent_id: scope.agent_id.clone(),
                }),
                causal_parent: command.causal_parent.clone(),
                event_type: "fixture.command.accepted".into(),
                canonical_payload_json: br#"{"status":"accepted"}"#.to_vec(),
                attestation: vec![1; 32],
            };
            let status = Self::status_for(
                operation.clone(),
                protocol,
                owner.clone(),
                wire::CompletionState::Succeeded,
                1,
                false,
            );
            self.operations
                .lock()
                .await
                .insert(operation_id, OperationRecord { status, event });
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
            let record = self
                .operations
                .lock()
                .await
                .values()
                .find(|record| {
                    authority.as_ref().map_or(true, |expected| {
                        record.status.owner.as_ref() == Some(expected)
                    }) && record.event.revision > revision
                })
                .cloned();
            let Some(record) = record else {
                return Ok(
                    Box::pin(stream::empty()) as BoxStream<'static, HarnessResult<wire::Delivery>>
                );
            };
            let event_revision = record.event.revision;
            Ok(Box::pin(stream::once(async move {
                Ok(wire::Delivery {
                    authority: record.status.owner,
                    generation,
                    from_revision: event_revision,
                    through_revision: event_revision,
                    events: vec![record.event],
                    live: true,
                })
            }))
                as BoxStream<'static, HarnessResult<wire::Delivery>>)
        }
        .boxed()
    }

    fn authorize_operation_control<'a>(
        &'a self,
        request: &'a OperationControlRequest,
    ) -> futures::future::BoxFuture<'a, HarnessResult<()>> {
        let operation_id = request.operation_id.to_string();
        let owner = wire_authority(&request.owner);
        async move {
            let operations = self.operations.lock().await;
            let record = operations.get(&operation_id).ok_or_else(|| {
                Error::NotFound(format!("fixture operation {operation_id} is unknown"))
            })?;
            if record.status.owner.as_ref() != Some(&owner) {
                return Err(Error::Unauthorized(format!(
                    "fixture operation {operation_id} owner does not match"
                )));
            }
            Ok(())
        }
        .boxed()
    }

    fn observe<'a>(
        &'a self,
        request: wire::ObserveRequest,
    ) -> futures::future::BoxFuture<'a, HarnessResult<wire::OperationStatus>> {
        async move {
            let record = self
                .operations
                .lock()
                .await
                .get(&request.operation_id)
                .cloned()
                .ok_or_else(|| {
                    Error::NotFound(format!(
                        "fixture operation {} is unknown",
                        request.operation_id
                    ))
                })?;
            if request.owner.as_ref() != record.status.owner.as_ref() {
                return Err(Error::Unauthorized(format!(
                    "fixture operation {} owner does not match",
                    request.operation_id
                )));
            }
            Ok(record.status)
        }
        .boxed()
    }

    fn cancel<'a>(
        &'a self,
        request: wire::CancelRequest,
    ) -> futures::future::BoxFuture<'a, HarnessResult<wire::CancelResponse>> {
        async move {
            let mut operations = self.operations.lock().await;
            let record = operations.get_mut(&request.operation_id).ok_or_else(|| {
                Error::NotFound(format!(
                    "fixture operation {} is unknown",
                    request.operation_id
                ))
            })?;
            if request.owner.as_ref() != record.status.owner.as_ref() {
                return Err(Error::Unauthorized(format!(
                    "fixture operation {} owner does not match",
                    request.operation_id
                )));
            }
            record.status.state = wire::CompletionState::Cancelled as i32;
            record.status.cancellation_requested = true;
            record.status.revision = record.status.revision.saturating_add(1);
            let operation = record.status.operation.clone();
            Ok(wire::CancelResponse {
                status: Some(record.status.clone()),
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
    use acyclic_fs::wire::filesystem::v2::filesystem_service_server::FilesystemService;
    use tonic::Request;

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
        let expectations: Vec<_> = scenarios
            .iter()
            .copied()
            .map(scenario_expectation)
            .collect();
        let operation_ids: std::collections::BTreeSet<_> =
            expectations.iter().map(|value| value.operation_id).collect();
        assert_eq!(operation_ids.len(), 32);
        assert!(expectations
            .iter()
            .all(|value| !value.authority.is_empty() && !value.cursor.is_empty()));
        assert_eq!(
            scenario_expectation(FILESYSTEM_SCENARIOS[5]).output,
            "path=/hello;bytes=rust-fixture"
        );
        assert_eq!(
            scenario_expectation(HARNESS_SCENARIOS[4]).status,
            "cancelled;revision=2"
        );
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
        let mut replay = backend
            .replay(wire::ResumeRequest {
                cursors: vec![wire::ReplayCursor {
                    authority: Some(owner.clone()),
                    generation: "fixture-generation".into(),
                    revision: 0,
                }],
                ..Default::default()
            })
            .await
            .expect("replay");
        let delivery = replay
            .next()
            .await
            .expect("delivery")
            .expect("delivery result");
        assert!(delivery.live);
        assert_eq!(delivery.events.len(), 1);
        assert_eq!(delivery.events[0].event_type, "fixture.command.accepted");
        let observed = backend
            .observe(wire::ObserveRequest {
                operation_id: "fixture-op".into(),
                owner: Some(owner.clone()),
                ..Default::default()
            })
            .await
            .expect("observe");
        assert_eq!(observed.state, wire::CompletionState::Succeeded as i32);
        let cancelled = backend
            .cancel(wire::CancelRequest {
                operation_id: "fixture-op".into(),
                owner: Some(owner),
                ..Default::default()
            })
            .await
            .expect("cancel");
        assert_eq!(
            cancelled.status.as_ref().expect("status").state,
            wire::CompletionState::Cancelled as i32
        );
        assert_eq!(
            scenario_expectation(HARNESS_SCENARIOS[1]).status,
            "accepted;revision=1"
        );
        assert_eq!(
            scenario_expectation(HARNESS_SCENARIOS[2]).output,
            "live=true;events=1;event=fixture.command.accepted"
        );
        assert_eq!(
            scenario_expectation(HARNESS_SCENARIOS[3]).output,
            "operation=fixture-op;state=succeeded;revision=1"
        );
        assert_eq!(
            scenario_expectation(HARNESS_SCENARIOS[4]).output,
            "operation=fixture-op;state=cancelled;revision=2"
        );
        assert_eq!(cancelled.status.as_ref().expect("status").revision, 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn production_filesystem_wire_service_matches_seed_expectations() {
        let service = filesystem_service().expect("filesystem service");
        let workspace = service
            .filesystem()
            .open_workspace("fixture")
            .await
            .expect("seeded workspace");
        let workspace_ref = acyclic_fs::wire::filesystem::v2::WorkspaceRef {
            workspace_id: workspace.id().into_bytes().to_vec(),
            name: "fixture".into(),
        };
        let head = service
            .get_head(Request::new(
                acyclic_fs::wire::filesystem::v2::GetHeadRequest {
                    workspace: Some(workspace_ref.clone()),
                },
            ))
            .await
            .expect("head")
            .into_inner()
            .generation
            .expect("head generation");
        let read = service
            .read(Request::new(
                acyclic_fs::wire::filesystem::v2::ReadRequest {
                    generation: Some(head.clone()),
                    path: "/hello".into(),
                    range: None,
                    maximum_bytes: 1024,
                },
            ))
            .await
            .expect("read")
            .into_inner();
        assert_eq!(read.contents, b"rust-fixture");
        assert_eq!(
            scenario_expectation(FILESYSTEM_SCENARIOS[5]).output,
            format!("path=/hello;bytes={}", String::from_utf8_lossy(&read.contents))
        );
        let stat = service
            .stat(Request::new(
                acyclic_fs::wire::filesystem::v2::StatRequest {
                    generation: Some(head.clone()),
                    path: "/hello".into(),
                },
            ))
            .await
            .expect("stat")
            .into_inner()
            .stat
            .expect("file stat");
        let size = stat
            .logical_bytes
            .and_then(|value| value.value)
            .and_then(|value| match value {
                acyclic_fs::wire::filesystem::v2::optional_u64::Value::Present(value) => {
                    Some(value)
                }
                acyclic_fs::wire::filesystem::v2::optional_u64::Value::Unavailable(_) => None,
            })
            .expect("logical byte count");
        assert_eq!(size, 12);
        assert_eq!(
            scenario_expectation(FILESYSTEM_SCENARIOS[6]).output,
            "path=/hello;size=12"
        );
        let page = service
            .list_directory(Request::new(
                acyclic_fs::wire::filesystem::v2::ListDirectoryRequest {
                    generation: Some(head),
                    path: "/".into(),
                    page: Some(acyclic_fs::wire::filesystem::v2::PageOptions {
                        maximum_items: 32,
                        after: None,
                    }),
                },
            ))
            .await
            .expect("directory")
            .into_inner()
            .page
            .expect("directory page");
        let names: Vec<_> = page
            .entries
            .iter()
            .filter_map(|entry| entry.name.as_ref())
            .map(|name| String::from_utf8_lossy(&name.bytes).into_owned())
            .collect();
        assert_eq!(names, vec!["hello", "link"]);
        assert_eq!(
            scenario_expectation(FILESYSTEM_SCENARIOS[7]).output,
            "entries=[hello,link]"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn production_seed_wire_identities_are_reproducible() {
        async fn identity(service: &FilesystemFixtureService) -> (Vec<u8>, Vec<u8>) {
            let workspace = service
                .filesystem()
                .open_workspace("fixture")
                .await
                .expect("seeded workspace");
            let workspace_id = workspace.id().into_bytes().to_vec();
            let head = service
                .get_head(Request::new(
                    acyclic_fs::wire::filesystem::v2::GetHeadRequest {
                        workspace: Some(acyclic_fs::wire::filesystem::v2::WorkspaceRef {
                            workspace_id: workspace_id.clone(),
                            name: "fixture".into(),
                        }),
                    },
                ))
                .await
                .expect("head")
                .into_inner()
                .generation
                .expect("head generation");
            (workspace_id, head.generation_id)
        }

        // The seeded commit records the injected Rust-owned fixture clock in its immutable
        // envelope. Reopen the same production service to prove its wire identities remain stable;
        // the exporter binds dynamic values from each actual service response into later requests.
        let service = filesystem_service().expect("filesystem service");
        let first = identity(&service).await;
        let second = identity(&service).await;
        assert_eq!(first, second);
        assert!(!first.0.is_empty());
        assert!(!first.1.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn transfer_fixture_keeps_export_source_and_import_destination_separate() {
        let source = filesystem_service().expect("seeded filesystem service");
        let workspace = ensure_transfer_source(&source)
            .await
            .expect("deterministic transfer source");
        assert_eq!(workspace.name, "scenario-export");

        let destination = empty_filesystem_service().expect("fresh import service");
        let missing = destination
            .open_workspace(Request::new(
                acyclic_fs::wire::filesystem::v2::OpenWorkspaceRequest {
                    selector: Some(
                        acyclic_fs::wire::filesystem::v2::open_workspace_request::Selector::Name(
                            "scenario-export".to_owned(),
                        ),
                    ),
                },
            ))
            .await;
        assert!(
            missing.is_err(),
            "fresh import destination must not reuse export authority"
        );
    }

}
