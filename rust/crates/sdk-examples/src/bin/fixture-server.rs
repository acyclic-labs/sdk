//! Bounded Rust-owned loopback server for SDK transport qualification.
//!
//! The server accepts canonical protobuf request bytes and returns normalized
//! JSON receipts. It is deliberately process-local: it has a request budget,
//! binds only to loopback, and makes no hosted service-availability claim.

use acyclic_actors::{FILE_DESCRIPTOR_SET, validate_create, wire as actors_wire};
use acyclic_fs::wire::filesystem::v2 as fs_wire;
use acyclic_harness::{wire as harness_wire, wire_api::HarnessWireApi};
use acyclic_objects::wire as objects_wire;
use acyclic_sdk_contract_wire::{BEARER_NO_CRLF, BindingFamily, credential, transport_control};
use acyclic_sdk_examples::fixtures::objects_server::ObjectsFixture;
use acyclic_sdk_examples::tls_fixture::{
    AllRoutesMachinesFixture, InferenceMetadataFixture, InferenceRunsFixture,
    new_method_transcript_log,
};
use acyclic_sdk_examples::{
    fixtures::{filesystem_harness, fixture_clock, harness_backend},
    transport_fixtures,
};
use acyclic_stream::{
    AppendOutcome, AppendRequest, IdempotencyKey, MemoryStream, ReadRequest, StreamPath,
    StreamProvider, wire as stream_wire,
};
use acyclic_workers::{
    FILE_DESCRIPTOR_SET as WORKERS_FILE_DESCRIPTOR_SET, validate_publish, validate_select,
    validate_submit, wire as workers_wire,
};
use futures::{FutureExt, StreamExt, stream};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};
use serde_json::{Deserializer, Value, json};
use sha2::{Digest, Sha256};
use std::env;
use std::io;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

#[allow(
    missing_docs,
    clippy::pedantic,
    clippy::too_many_lines,
    clippy::large_enum_variant
)]
mod generated_control {
    pub mod acyclic {
        pub mod protocol {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/control/acyclic.protocol.v1.rs"));
            }
        }
        pub mod transport {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/control/acyclic.transport.v1.rs"));
            }
        }
    }
}

use generated_control::acyclic::{
    protocol::v1 as control_protocol,
    transport::v1::{
        protocol_service_client::ProtocolServiceClient,
        protocol_service_server::{ProtocolService, ProtocolServiceServer},
    },
};

const DEFAULT_MAX_REQUESTS: usize = 32;
const MAX_REQUEST_BUDGET: usize = 4_096;
const MAX_HEADER_BYTES: usize = 16 * 1024;
const MAX_BODY_BYTES: usize = 2 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Options {
    port: u16,
    grpc_port: u16,
    max_requests: usize,
}

#[derive(Debug)]
struct HttpRequest {
    method: String,
    path: String,
    authorization: Option<String>,
    content_type: String,
    body: Vec<u8>,
}

#[derive(Debug)]
struct HttpError {
    status: u16,
    message: String,
}

#[derive(Clone)]
struct App {
    stream: MemoryStream,
    requests: Arc<Mutex<usize>>,
    max_requests: usize,
    shutdown: Arc<Notify>,
}

#[derive(Clone)]
struct GrpcApp {
    stream: Arc<MemoryStream>,
    requests: Arc<AtomicUsize>,
    max_requests: usize,
    shutdown: Arc<Notify>,
}

#[derive(Clone)]
struct BudgetInterceptor {
    requests: Arc<AtomicUsize>,
    max_requests: usize,
    shutdown: Arc<Notify>,
}

#[derive(Clone, Copy)]
struct ProtocolFixture;

#[tonic::async_trait]
impl ProtocolService for ProtocolFixture {
    async fn handshake(
        &self,
        request: Request<control_protocol::HandshakeRequest>,
    ) -> Result<Response<control_protocol::HandshakeResponse>, Status> {
        let family = authorized_control_family(&request)?;
        let expected_version = transport_control::control_protocol_version(family);
        let protocol =
            request.get_ref().protocol.as_ref().ok_or_else(|| {
                Status::unauthenticated("handshake protocol identity is required")
            })?;
        if protocol.version != expected_version {
            return Err(Status::failed_precondition(format!(
                "protocol version does not match Rust-owned {} identity",
                family.name()
            )));
        }
        let expected_digest = transport_control::archived_descriptor_digest(family);
        if protocol.descriptor_digest != expected_digest {
            return Err(Status::failed_precondition(format!(
                "descriptor digest does not match archived {} identity",
                family.name()
            )));
        }
        let supported = control_protocol::CapabilitySet {
            capabilities: vec![control_protocol::Capability {
                name: family.name().to_owned(),
                version: expected_version.to_owned(),
            }],
        };
        if let Some(required) = request.get_ref().required.as_ref() {
            if required.capabilities.iter().any(|capability| {
                capability.name != family.name() || capability.version != expected_version
            }) {
                return Err(Status::failed_precondition(
                    "required capability is not supported by this Rust-owned family",
                ));
            }
        }
        Ok(Response::new(control_protocol::HandshakeResponse {
            protocol: Some(control_protocol::ProtocolIdentity {
                version: expected_version.to_owned(),
                descriptor_digest: expected_digest,
            }),
            supported: Some(supported),
        }))
    }
}

fn authorized_control_family<T>(request: &Request<T>) -> Result<BindingFamily, Status> {
    let authorization = request
        .metadata()
        .get("authorization")
        .ok_or_else(|| Status::unauthenticated("authorization metadata is required"))?
        .to_str()
        .map_err(|_| Status::unauthenticated("authorization metadata must be ASCII"))?;
    if !authorization
        .strip_prefix("Bearer ")
        .is_some_and(|token| !token.is_empty())
    {
        return Err(Status::unauthenticated(
            "authorization must be a bearer token",
        ));
    }
    let family = request
        .metadata()
        .get(transport_control::FAMILY_METADATA_KEY)
        .ok_or_else(|| Status::unauthenticated("acyclic-family metadata is required"))?
        .to_str()
        .map_err(|_| Status::unauthenticated("acyclic-family metadata must be ASCII"))?;
    let family = BindingFamily::ALL
        .iter()
        .copied()
        .find(|candidate| candidate.name() == family)
        .ok_or_else(|| Status::permission_denied("requested SDK family is not registered"))?;
    if !matches!(
        family,
        BindingFamily::Actors | BindingFamily::Workers | BindingFamily::Objects
    ) {
        return Err(Status::permission_denied(
            "fixture control endpoint authorizes registered remote SDK families only",
        ));
    }
    Ok(family)
}

impl tonic::service::Interceptor for BudgetInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let request_number = self.requests.fetch_add(1, Ordering::SeqCst);
        if request_number >= self.max_requests {
            self.shutdown.notify_waiters();
            return Err(Status::resource_exhausted(
                "fixture gRPC request budget exhausted",
            ));
        }
        if request_number + 1 == self.max_requests {
            self.shutdown.notify_waiters();
        }
        Ok(request)
    }
}

#[derive(Clone)]
struct ActorsFixture {}

impl ActorsFixture {
    fn new(_app: GrpcApp) -> Self {
        Self {}
    }
}

#[tonic::async_trait]
impl actors_wire::actors_service_server::ActorsService for ActorsFixture {
    async fn create_actor(
        &self,
        request: Request<actors_wire::CreateActorRequest>,
    ) -> Result<Response<actors_wire::CreateActorResponse>, Status> {
        let request = request.into_inner();
        validate_create(&request).map_err(|error| {
            Status::invalid_argument(format!("CreateActorRequest rejected: {error}"))
        })?;
        Ok(Response::new(actors_wire::CreateActorResponse {
            actor: Some(actors_wire::ActorObservation {
                actor_id: "fixture-actor".to_owned(),
                code_sha256: request.code_sha256,
                home_region: request.home_region,
                state: actors_wire::ActorState::Active as i32,
                subscriptions: request
                    .subscriptions
                    .into_iter()
                    .map(|subscription| actors_wire::SubscriptionObservation {
                        subscription_id: subscription.subscription_id,
                        stream_path: subscription.stream_path,
                        state: actors_wire::SubscriptionState::Active as i32,
                        delivered_cursor: 0,
                        completed_cursor: 0,
                        recoverable_cursor: 0,
                        placement_anchor: subscription.placement_anchor,
                        retry_count: 0,
                        failure_code: String::new(),
                        failed_cursor: None,
                    })
                    .collect(),
                checkpoint_unix_millis: None,
                checkpoint_epoch: 0,
                configuration_revision: 1,
            }),
        }))
    }

    async fn update_actor(
        &self,
        request: Request<actors_wire::UpdateActorRequest>,
    ) -> Result<Response<actors_wire::UpdateActorResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }

    async fn inspect_actor(
        &self,
        request: Request<actors_wire::InspectActorRequest>,
    ) -> Result<Response<actors_wire::InspectActorResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }

    async fn add_subscription(
        &self,
        request: Request<actors_wire::AddSubscriptionRequest>,
    ) -> Result<Response<actors_wire::AddSubscriptionResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }

    async fn remove_subscription(
        &self,
        request: Request<actors_wire::RemoveSubscriptionRequest>,
    ) -> Result<Response<actors_wire::RemoveSubscriptionResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }

    async fn resume_subscription(
        &self,
        request: Request<actors_wire::ResumeSubscriptionRequest>,
    ) -> Result<Response<actors_wire::ResumeSubscriptionResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }

    async fn checkpoint_actor(
        &self,
        request: Request<actors_wire::CheckpointActorRequest>,
    ) -> Result<Response<actors_wire::CheckpointActorResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }

    async fn invoke_actor(
        &self,
        request: Request<actors_wire::InvokeActorRequest>,
    ) -> Result<Response<actors_wire::InvokeActorResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
}

#[derive(Clone)]
struct FilesystemFixture;

#[tonic::async_trait]
impl fs_wire::filesystem_service_server::FilesystemService for FilesystemFixture {
    type ExportStream = stream::Iter<std::vec::IntoIter<Result<fs_wire::ExportChunk, Status>>>;

    async fn handshake(
        &self,
        request: Request<fs_wire::HandshakeRequest>,
    ) -> Result<Response<fs_wire::HandshakeResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
    async fn create_workspace(
        &self,
        request: Request<fs_wire::CreateWorkspaceRequest>,
    ) -> Result<Response<fs_wire::WorkspaceResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
    async fn open_workspace(
        &self,
        request: Request<fs_wire::OpenWorkspaceRequest>,
    ) -> Result<Response<fs_wire::WorkspaceResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
    async fn delete_workspace(
        &self,
        request: Request<fs_wire::DeleteWorkspaceRequest>,
    ) -> Result<Response<fs_wire::MutationResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
    async fn get_head(
        &self,
        request: Request<fs_wire::GetHeadRequest>,
    ) -> Result<Response<fs_wire::GenerationResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
    async fn get_generation(
        &self,
        request: Request<fs_wire::GetGenerationRequest>,
    ) -> Result<Response<fs_wire::GenerationResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
    async fn read(
        &self,
        request: Request<fs_wire::ReadRequest>,
    ) -> Result<Response<fs_wire::ReadResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
    async fn stat(
        &self,
        request: Request<fs_wire::StatRequest>,
    ) -> Result<Response<fs_wire::StatResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
    async fn list_directory(
        &self,
        request: Request<fs_wire::ListDirectoryRequest>,
    ) -> Result<Response<fs_wire::ListDirectoryResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
    async fn read_link(
        &self,
        request: Request<fs_wire::ReadLinkRequest>,
    ) -> Result<Response<fs_wire::ReadResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
    async fn plan_extents(
        &self,
        request: Request<fs_wire::PlanExtentsRequest>,
    ) -> Result<Response<fs_wire::PlanExtentsResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
    async fn apply_transaction(
        &self,
        request: Request<fs_wire::ApplyTransactionRequest>,
    ) -> Result<Response<fs_wire::MutationResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
    async fn rebase_transaction(
        &self,
        request: Request<fs_wire::RebaseTransactionRequest>,
    ) -> Result<Response<fs_wire::RebaseTransactionResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
    async fn fork_workspace(
        &self,
        request: Request<fs_wire::ForkWorkspaceRequest>,
    ) -> Result<Response<fs_wire::WorkspaceResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
    async fn diff(
        &self,
        request: Request<fs_wire::DiffRequest>,
    ) -> Result<Response<fs_wire::DiffResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
    async fn rebase(
        &self,
        request: Request<fs_wire::RebaseRequest>,
    ) -> Result<Response<fs_wire::RebaseResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
    async fn plan_join(
        &self,
        request: Request<fs_wire::PlanJoinRequest>,
    ) -> Result<Response<fs_wire::JoinPlan>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
    async fn apply_join(
        &self,
        request: Request<fs_wire::ApplyJoinRequest>,
    ) -> Result<Response<fs_wire::JoinResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
    async fn checkpoint(
        &self,
        request: Request<fs_wire::RetainGenerationRequest>,
    ) -> Result<Response<fs_wire::RetainGenerationResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
    async fn pin(
        &self,
        request: Request<fs_wire::RetainGenerationRequest>,
    ) -> Result<Response<fs_wire::RetainGenerationResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
    async fn export(
        &self,
        request: Request<fs_wire::ExportRequest>,
    ) -> Result<Response<Self::ExportStream>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(stream::iter(vec![Ok(
            fs_wire::ExportChunk {
                cursor: b"fixture-export-cursor-1".to_vec(),
                object_id: b"fixture-export-object-1".to_vec(),
                contents: b"rust-owned-filesystem-export".to_vec(),
                terminal: true,
            },
        )])))
    }
    async fn import(
        &self,
        request: Request<tonic::Streaming<fs_wire::ImportChunk>>,
    ) -> Result<Response<fs_wire::ImportResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
    async fn issue_mount_credential(
        &self,
        request: Request<fs_wire::CredentialRequest>,
    ) -> Result<Response<fs_wire::CredentialResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
    async fn issue_s3_credential(
        &self,
        request: Request<fs_wire::CredentialRequest>,
    ) -> Result<Response<fs_wire::CredentialResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
    async fn get_source_state(
        &self,
        request: Request<fs_wire::SourceStateRequest>,
    ) -> Result<Response<fs_wire::SourceResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
    async fn reconcile_source(
        &self,
        request: Request<fs_wire::SourceOperationRequest>,
    ) -> Result<Response<fs_wire::SourceResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
    async fn rescan_source(
        &self,
        request: Request<fs_wire::SourceOperationRequest>,
    ) -> Result<Response<fs_wire::SourceResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
    async fn seal_source(
        &self,
        request: Request<fs_wire::SourceOperationRequest>,
    ) -> Result<Response<fs_wire::SourceResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
    async fn observe(
        &self,
        request: Request<fs_wire::ObserveRequest>,
    ) -> Result<Response<fs_wire::ObserveResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
    async fn cancel(
        &self,
        request: Request<fs_wire::CancelRequest>,
    ) -> Result<Response<fs_wire::CancelResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
}

#[derive(Clone)]
struct HarnessFixtureApi {
    journal: Arc<Mutex<Option<harness_wire::CommandEnvelope>>>,
}

impl HarnessFixtureApi {
    fn new() -> Self {
        Self {
            journal: Arc::new(Mutex::new(None)),
        }
    }
}

impl HarnessWireApi for HarnessFixtureApi {
    fn authorize_operation_control<'a>(
        &'a self,
        _request: &'a acyclic_harness::wire_api::OperationControlRequest,
    ) -> futures::future::BoxFuture<'a, acyclic_harness::Result<()>> {
        async { Ok(()) }.boxed()
    }

    fn handshake<'a>(
        &'a self,
        _request: harness_wire::HandshakeRequest,
    ) -> futures::future::BoxFuture<'a, acyclic_harness::Result<harness_wire::HandshakeResponse>>
    {
        async {
            Ok(harness_wire::HandshakeResponse {
                protocol: Some(acyclic_harness::wire_api::current_protocol()),
                supported: Some(Default::default()),
            })
        }
        .boxed()
    }

    fn submit<'a>(
        &'a self,
        command: harness_wire::CommandEnvelope,
    ) -> futures::future::BoxFuture<'a, acyclic_harness::Result<harness_wire::Admission>> {
        let journal = Arc::clone(&self.journal);
        async move {
            *journal.lock().await = Some(command.clone());
            Ok(harness_wire::Admission {
                operation: command.operation,
                state: harness_wire::AdmissionState::Accepted as i32,
                error: None,
            })
        }
        .boxed()
    }

    fn replay<'a>(
        &'a self,
        _request: harness_wire::ResumeRequest,
    ) -> futures::future::BoxFuture<
        'a,
        acyclic_harness::Result<
            futures::stream::BoxStream<'static, acyclic_harness::Result<harness_wire::Delivery>>,
        >,
    > {
        let journal = Arc::clone(&self.journal);
        async move {
            let command = journal.lock().await.clone();
            let Some(command) = command else {
                return Ok(Box::pin(stream::empty()) as _);
            };
            let operation_id = command
                .operation
                .as_ref()
                .map(|operation| operation.operation_id.clone())
                .unwrap_or_default();
            let scope = command
                .scope
                .as_ref()
                .map(|scope| harness_wire::RecordedScope {
                    id: scope.id.clone(),
                    capabilities: scope.capabilities.clone(),
                    issuer: scope.issuer.clone(),
                    agent_id: scope.agent_id.clone(),
                });
            let event = harness_wire::EventEnvelope {
                protocol: command.protocol.clone(),
                authority: command.authority.clone(),
                revision: 1,
                operation_id,
                intent_digest: command.intent_digest.clone(),
                scope,
                causal_parent: command.causal_parent.clone(),
                event_type: "fixture.command.accepted".to_owned(),
                canonical_payload_json: br#"{"status":"accepted"}"#.to_vec(),
                attestation: vec![1; 32],
            };
            Ok(Box::pin(stream::once(async move {
                Ok(harness_wire::Delivery {
                    authority: command.authority,
                    generation: "rust-fixture-generation-v1".to_owned(),
                    from_revision: 1,
                    through_revision: 1,
                    events: vec![event],
                    live: false,
                })
            })) as _)
        }
        .boxed()
    }

    fn observe<'a>(
        &'a self,
        request: harness_wire::ObserveRequest,
    ) -> futures::future::BoxFuture<'a, acyclic_harness::Result<harness_wire::OperationStatus>>
    {
        async move {
            Ok(harness_wire::OperationStatus {
                operation: Some(harness_wire::OperationIdentity {
                    operation_id: request.operation_id,
                    idempotency_key: String::new(),
                }),
                state: harness_wire::CompletionState::Running as i32,
                error: None,
                protocol: request.protocol,
                owner: request.owner,
                cancellation_requested: false,
                revision: 0,
            })
        }
        .boxed()
    }

    fn cancel<'a>(
        &'a self,
        request: harness_wire::CancelRequest,
    ) -> futures::future::BoxFuture<'a, acyclic_harness::Result<harness_wire::CancelResponse>> {
        async move {
            Ok(harness_wire::CancelResponse {
                status: Some(harness_wire::OperationStatus {
                    operation: Some(harness_wire::OperationIdentity {
                        operation_id: request.operation_id.clone(),
                        idempotency_key: String::new(),
                    }),
                    state: harness_wire::CompletionState::Cancelled as i32,
                    error: None,
                    protocol: request.protocol.clone(),
                    owner: request.owner.clone(),
                    cancellation_requested: false,
                    revision: 1,
                }),
                operation: Some(harness_wire::OperationIdentity {
                    operation_id: request.operation_id,
                    idempotency_key: request.idempotency_key,
                }),
            })
        }
        .boxed()
    }
}
#[derive(Clone)]
struct WorkersFixture;

#[tonic::async_trait]
impl workers_wire::workers_service_server::WorkersService for WorkersFixture {
    async fn publish_version(
        &self,
        request: Request<workers_wire::PublishVersionRequest>,
    ) -> Result<Response<workers_wire::PublishVersionResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }

    async fn select_deployment(
        &self,
        request: Request<workers_wire::SelectDeploymentRequest>,
    ) -> Result<Response<workers_wire::SelectDeploymentResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }

    async fn submit_job(
        &self,
        request: Request<workers_wire::SubmitJobRequest>,
    ) -> Result<Response<workers_wire::SubmitJobResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }

    async fn inspect_job(
        &self,
        request: Request<workers_wire::InspectJobRequest>,
    ) -> Result<Response<workers_wire::InspectJobResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }

    async fn cancel_job(
        &self,
        request: Request<workers_wire::CancelJobRequest>,
    ) -> Result<Response<workers_wire::CancelJobResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }

    async fn invoke_version(
        &self,
        request: Request<workers_wire::InvokeVersionRequest>,
    ) -> Result<Response<workers_wire::InvokeResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }

    async fn invoke_deployment(
        &self,
        request: Request<workers_wire::InvokeDeploymentRequest>,
    ) -> Result<Response<workers_wire::InvokeResponse>, Status> {
        let _request = request.into_inner();
        Ok(Response::new(Default::default()))
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = parse_args(env::args().skip(1))?;
    let listener = TcpListener::bind(("127.0.0.1", options.port)).await?;
    let address = listener.local_addr()?;
    let grpc_listener = TcpListener::bind(("127.0.0.1", options.grpc_port)).await?;
    let grpc_address = grpc_listener.local_addr()?;
    let fixtures = transport_fixtures();
    let source_sha256 = source_sha256();
    let shutdown = Arc::new(Notify::new());
    let grpc_requests = Arc::new(AtomicUsize::new(0));
    let stream = Arc::new(MemoryStream::new_with_clock(
        acyclic_stream::MemoryLimits::default(),
        fixture_clock::stream_clock(),
    ));
    let app = App {
        stream: (*stream).clone(),
        requests: Arc::new(Mutex::new(0)),
        max_requests: options.max_requests,
        shutdown: Arc::clone(&shutdown),
    };
    let grpc_app = GrpcApp {
        stream,
        requests: grpc_requests,
        max_requests: options.max_requests,
        shutdown: Arc::clone(&shutdown),
    };
    let grpc_shutdown = Arc::clone(&shutdown);
    let filesystem_service = filesystem_harness::filesystem_server()
        .map_err(|error| format!("construct filesystem fixture service: {error}"))?;
    let harness_service = harness_backend::harness_server();
    let grpc_task = tokio::spawn(async move {
        let interceptor = BudgetInterceptor {
            requests: Arc::clone(&grpc_app.requests),
            max_requests: grpc_app.max_requests,
            shutdown: Arc::clone(&grpc_app.shutdown),
        };
        let actors = tonic::service::interceptor::InterceptedService::new(
            actors_wire::actors_service_server::ActorsServiceServer::new(ActorsFixture::new(
                grpc_app.clone(),
            ))
            .max_decoding_message_size(MAX_BODY_BYTES)
            .max_encoding_message_size(MAX_BODY_BYTES),
            interceptor.clone(),
        );
        let workers = tonic::service::interceptor::InterceptedService::new(
            workers_wire::workers_service_server::WorkersServiceServer::new(WorkersFixture)
                .max_decoding_message_size(MAX_BODY_BYTES)
                .max_encoding_message_size(MAX_BODY_BYTES),
            interceptor.clone(),
        );
        let control = tonic::service::interceptor::InterceptedService::new(
            ProtocolServiceServer::new(ProtocolFixture),
            interceptor.clone(),
        );
        let objects_fixture = ObjectsFixture::new();
        let objects_buckets = tonic::service::interceptor::InterceptedService::new(
            objects_wire::buckets_service_server::BucketsServiceServer::new(
                objects_fixture.clone(),
            ),
            interceptor.clone(),
        );
        let objects = tonic::service::interceptor::InterceptedService::new(
            objects_wire::objects_service_server::ObjectsServiceServer::new(
                objects_fixture.clone(),
            ),
            interceptor.clone(),
        );
        let objects_multipart = tonic::service::interceptor::InterceptedService::new(
            objects_wire::multipart_service_server::MultipartServiceServer::new(objects_fixture),
            interceptor.clone(),
        );
        let harness = tonic::service::interceptor::InterceptedService::new(
            harness_service,
            interceptor.clone(),
        );
        let filesystem = tonic::service::interceptor::InterceptedService::new(
            filesystem_service
                .max_decoding_message_size(MAX_BODY_BYTES)
                .max_encoding_message_size(MAX_BODY_BYTES),
            interceptor.clone(),
        );
        let streams = tonic::service::interceptor::InterceptedService::new(
            acyclic_stream::wire::stream_service_server::StreamServiceServer::new(
                acyclic_stream::grpc::Service::new(Arc::clone(&grpc_app.stream)),
            )
            .max_decoding_message_size(MAX_BODY_BYTES)
            .max_encoding_message_size(MAX_BODY_BYTES),
            interceptor.clone(),
        );
        let transcript = new_method_transcript_log();
        let machines = tonic::service::interceptor::InterceptedService::new(
            acyclic_machines::wire::machines_service_server::MachinesServiceServer::new(
                AllRoutesMachinesFixture::with_transcript(transcript.clone()),
            ),
            interceptor.clone(),
        );
        let models = tonic::service::interceptor::InterceptedService::new(
            acyclic_inference::wire::models_service_server::ModelsServiceServer::new(
                InferenceMetadataFixture::with_transcript(transcript.clone()),
            ),
            interceptor.clone(),
        );
        let contexts = tonic::service::interceptor::InterceptedService::new(
            acyclic_inference::wire::contexts_service_server::ContextsServiceServer::new(
                InferenceMetadataFixture::with_transcript(transcript.clone()),
            ),
            interceptor.clone(),
        );
        let warm_contexts = tonic::service::interceptor::InterceptedService::new(
            acyclic_inference::wire::warm_contexts_service_server::WarmContextsServiceServer::new(
                InferenceMetadataFixture::with_transcript(transcript.clone()),
            ),
            interceptor.clone(),
        );
        let evaluations = tonic::service::interceptor::InterceptedService::new(
            acyclic_inference::wire::evaluations_service_server::EvaluationsServiceServer::new(
                InferenceMetadataFixture::with_transcript(transcript.clone()),
            ),
            interceptor.clone(),
        );
        let runs = tonic::service::interceptor::InterceptedService::new(
            acyclic_inference::wire::runs_service_server::RunsServiceServer::new(
                InferenceRunsFixture::with_transcript(transcript),
            ),
            interceptor,
        );
        Server::builder()
            .add_service(control)
            .add_service(actors)
            .add_service(workers)
            .add_service(objects_buckets)
            .add_service(objects)
            .add_service(objects_multipart)
            .add_service(harness)
            .add_service(filesystem)
            .add_service(streams)
            .add_service(machines)
            .add_service(models)
            .add_service(contexts)
            .add_service(warm_contexts)
            .add_service(evaluations)
            .add_service(runs)
            .serve_with_incoming_shutdown(TcpListenerStream::new(grpc_listener), async move {
                grpc_shutdown.notified().await;
            })
            .await
    });
    println!(
        "{}",
        serde_json::to_string(&json!({
            "schema": "acyclic.sdk.fixture-server.v1",
            "address": format!("http://{address}"),
            "shutdown": format!("http://{address}/shutdown"),
            "grpc_address": format!("http://{grpc_address}"),
            "grpc_address_env": "FIXTURE_GRPC_ADDRESS",
            "max_requests": options.max_requests,
            "source": {
                "path": "rust/crates/sdk-examples/src/lib.rs",
                "sha256": source_sha256,
            },
            "fixtures": fixtures.iter().map(|fixture| fixture.id).collect::<Vec<_>>(),
            "request_content_types": ["application/json", "application/octet-stream"],
            "response_content_type": "application/json",
            "service_availability": "not_claimed",
        }))?
    );
    eprintln!("sdk fixture server listening on http://{address}");

    loop {
        tokio::select! {
            _ = shutdown.notified() => break,
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let app = app.clone();
                let keep_running = handle_connection(stream, app).await?;
                if !keep_running {
                    shutdown.notify_waiters();
                    break;
                }
            }
        }
    }
    shutdown.notify_waiters();
    grpc_task.await??;
    Ok(())
}

fn parse_args<I>(arguments: I) -> Result<Options, String>
where
    I: IntoIterator<Item = String>,
{
    let mut arguments = arguments.into_iter();
    let mut options = Options {
        port: 0,
        grpc_port: 0,
        max_requests: DEFAULT_MAX_REQUESTS,
    };
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--port" => {
                options.port = next_value(&mut arguments, "--port")?
                    .parse()
                    .map_err(|_| "--port must be a u16".to_owned())?;
            }
            "--grpc-port" => {
                options.grpc_port = next_value(&mut arguments, "--grpc-port")?
                    .parse()
                    .map_err(|_| "--grpc-port must be a u16".to_owned())?;
            }
            "--max-requests" => {
                options.max_requests = next_value(&mut arguments, "--max-requests")?
                    .parse()
                    .map_err(|_| "--max-requests must be a positive integer".to_owned())?;
                if options.max_requests == 0 || options.max_requests > MAX_REQUEST_BUDGET {
                    return Err(format!(
                        "--max-requests must be between 1 and {MAX_REQUEST_BUDGET}"
                    ));
                }
            }
            "--help" | "-h" => {
                println!("fixture-server [--port PORT] [--grpc-port PORT] [--max-requests N]");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument {other}")),
        }
    }
    Ok(options)
}

fn next_value<I>(arguments: &mut I, flag: &str) -> Result<String, String>
where
    I: Iterator<Item = String>,
{
    arguments
        .next()
        .ok_or_else(|| format!("{flag} requires a value"))
}

async fn handle_connection(mut stream: TcpStream, app: App) -> Result<bool, io::Error> {
    if !reserve_request(&app).await {
        write_json(
            &mut stream,
            429,
            &json!({
                "schema": "acyclic.sdk.fixture-response.v1",
                "status": "error",
                "message": "fixture request budget exhausted",
            }),
        )
        .await?;
        return Ok(false);
    }
    let request = match read_request(&mut stream).await {
        Ok(request) => request,
        Err(error) => {
            write_json(
                &mut stream,
                error.status,
                &json!({
                    "schema": "acyclic.sdk.fixture-response.v1",
                    "status": "error",
                    "message": error.message,
                }),
            )
            .await?;
            return Ok(true);
        }
    };
    let is_shutdown = request.path == "/shutdown";
    let is_control_handshake = request.method == "GET"
        && request.path.starts_with("/v1/sdk/")
        && request.path.ends_with("/handshake");
    let result = if request.path == "/health" && request.method == "GET" {
        Ok(json!({
            "schema": "acyclic.sdk.fixture-response.v1",
            "status": "ready",
            "service_availability": "not_claimed",
            "source": {
                "path": "rust/crates/sdk-examples/src/lib.rs",
                "sha256": source_sha256(),
            },
        }))
    } else if request.method != "POST" && !is_shutdown {
        Err(HttpError {
            status: 405,
            message: "fixture endpoints require POST".to_owned(),
        })
    } else if is_shutdown {
        Ok(json!({
            "schema": "acyclic.sdk.fixture-response.v1",
            "status": "shutdown",
        }))
    } else if is_control_handshake {
        control_handshake_http(&request.path, request.authorization.as_deref())
    } else {
        dispatch(&app, &request).await
    };
    match result {
        Ok(body) => write_json(&mut stream, 200, &body).await?,
        Err(error) => {
            write_json(
                &mut stream,
                error.status,
                &json!({
                    "schema": "acyclic.sdk.fixture-response.v1",
                    "status": "error",
                    "message": error.message,
                }),
            )
            .await?
        }
    }
    let keep_running = !is_shutdown && request_count(&app).await < app.max_requests;
    if !keep_running {
        app.shutdown.notify_waiters();
    }
    Ok(keep_running)
}

async fn reserve_request(app: &App) -> bool {
    let mut requests = app.requests.lock().await;
    if *requests >= app.max_requests {
        return false;
    }
    *requests += 1;
    true
}

async fn request_count(app: &App) -> usize {
    *app.requests.lock().await
}

async fn dispatch(app: &App, request: &HttpRequest) -> Result<Value, HttpError> {
    match request.path.as_str() {
        "/v1/actors/create" => actors_create(&request.content_type, &request.body),
        "/v1/workers/versions/publish" => workers_publish(&request.content_type, &request.body),
        "/v1/workers/deployments/select" => workers_select(&request.content_type, &request.body),
        "/v1/workers/jobs/submit" => workers_submit(&request.content_type, &request.body),
        "/v1/workers/jobs/inspect" => workers_inspect(&request.content_type, &request.body),
        "/v1/workers/jobs/cancel" => workers_cancel(&request.content_type, &request.body),
        "/v1/stream/append" => stream_append(app, &request.body).await,
        "/v1/stream/read" => stream_read(app, &request.body).await,
        "/health" => Ok(json!({
            "schema": "acyclic.sdk.fixture-response.v1",
            "status": "ready",
            "service_availability": "not_claimed",
        })),
        path if path.starts_with("/v1/workers/versions/") && path.ends_with("/invoke") => {
            workers_invoke_version(&request.content_type, &request.body)
        }
        path if path.starts_with("/v1/workers/deployments/") && path.ends_with("/invoke") => {
            workers_invoke_deployment(&request.content_type, &request.body)
        }
        path => Err(HttpError {
            status: 404,
            message: format!("unknown fixture route {path}"),
        }),
    }
}

fn control_handshake_http(path: &str, authorization: Option<&str>) -> Result<Value, HttpError> {
    if !authorization
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|token| credential::validate(BEARER_NO_CRLF, token))
    {
        return Err(HttpError {
            status: 401,
            message: "control handshake requires a valid bearer token".to_owned(),
        });
    }
    let family_name = path
        .strip_prefix("/v1/sdk/")
        .and_then(|path| path.strip_suffix("/handshake"))
        .filter(|family| transport_control::handshake_http_route(family).as_deref() == Some(path));
    let family = family_name
        .and_then(|name| {
            BindingFamily::ALL
                .iter()
                .copied()
                .find(|family| family.name() == name)
        })
        .filter(|family| {
            matches!(
                family,
                BindingFamily::Actors | BindingFamily::Workers | BindingFamily::Objects
            )
        })
        .ok_or_else(|| HttpError {
            status: 404,
            message: "unknown SDK control family".to_owned(),
        })?;
    let descriptor_digest = transport_control::archived_descriptor_digest(family);
    Ok(json!({
        "schema": "acyclic.sdk.handshake-response.v1",
        "rpc": transport_control::HANDSHAKE_RPC_PATH,
        "family": family.name(),
        "protocol": {
            "version": transport_control::control_protocol_version(family),
            "descriptor_digest": descriptor_digest,
        },
        "supported": {
            "capabilities": [{
                "name": family.name(),
                "version": transport_control::control_protocol_version(family),
            }]
        },
        "source": {
            "path": "rust/crates/sdk-contract-wire/src/family_registry.rs",
            "revision": env!("SDK_EXAMPLES_SOURCE_SHA256"),
        },
    }))
}

fn actors_create(content_type: &str, body: &[u8]) -> Result<Value, HttpError> {
    let request = decode_actor_request(content_type, body).map_err(|error| HttpError {
        status: 400,
        message: error,
    })?;
    validate_create(&request).map_err(|error| HttpError {
        status: 422,
        message: format!("CreateActorRequest rejected by Rust validation: {error}"),
    })?;
    let response = actors_wire::CreateActorResponse {
        actor: Some(actors_wire::ActorObservation {
            actor_id: "fixture-actor".to_owned(),
            code_sha256: request.code_sha256,
            home_region: request.home_region,
            state: actors_wire::ActorState::Active as i32,
            subscriptions: request
                .subscriptions
                .into_iter()
                .map(|subscription| actors_wire::SubscriptionObservation {
                    subscription_id: subscription.subscription_id,
                    stream_path: subscription.stream_path,
                    state: actors_wire::SubscriptionState::Active as i32,
                    delivered_cursor: 0,
                    completed_cursor: 0,
                    recoverable_cursor: 0,
                    placement_anchor: subscription.placement_anchor,
                    retry_count: 0,
                    failure_code: String::new(),
                    failed_cursor: None,
                })
                .collect(),
            checkpoint_unix_millis: None,
            checkpoint_epoch: 0,
            configuration_revision: 1,
        }),
    };
    encode_actor_response(&response).map_err(|error| HttpError {
        status: 500,
        message: error,
    })
}

fn decode_actor_request(
    content_type: &str,
    body: &[u8],
) -> Result<actors_wire::CreateActorRequest, String> {
    if content_type
        .split(';')
        .next()
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
        || body
            .iter()
            .copied()
            .find(|byte| !byte.is_ascii_whitespace())
            == Some(b'{')
    {
        decode_json_message(body, "acyclic.actors.v1.CreateActorRequest")
            .map_err(|error| format!("invalid CreateActorRequest Protobuf JSON: {error}"))
    } else {
        actors_wire::CreateActorRequest::decode(body)
            .map_err(|error| format!("invalid CreateActorRequest protobuf: {error}"))
    }
}

fn encode_actor_response(response: &actors_wire::CreateActorResponse) -> Result<Value, String> {
    let pool = DescriptorPool::decode(FILE_DESCRIPTOR_SET)
        .map_err(|error| format!("decode Actors descriptor: {error}"))?;
    let descriptor = pool
        .get_message_by_name("acyclic.actors.v1.CreateActorResponse")
        .ok_or_else(|| "CreateActorResponse descriptor is missing".to_owned())?;
    let message = DynamicMessage::decode(descriptor, response.encode_to_vec().as_slice())
        .map_err(|error| format!("encode CreateActorResponse: {error}"))?;
    serde_json::to_value(message).map_err(|error| format!("serialize CreateActorResponse: {error}"))
}

fn workers_publish(content_type: &str, body: &[u8]) -> Result<Value, HttpError> {
    let request = decode_workers_request::<workers_wire::PublishVersionRequest>(
        content_type,
        body,
        "acyclic.workers.v1.PublishVersionRequest",
    )?;
    validate_publish(&request).map_err(|error| HttpError {
        status: 422,
        message: format!("PublishVersionRequest rejected by Rust validation: {error}"),
    })?;
    encode_workers_response(
        &workers_wire::PublishVersionResponse {
            version: Some(workers_wire::CodeVersion {
                sha256: request.expected_sha256,
                size_bytes: request.javascript_module.len() as u64,
            }),
        },
        "acyclic.workers.v1.PublishVersionResponse",
    )
    .map_err(|error| HttpError {
        status: 500,
        message: error,
    })
}

fn workers_select(content_type: &str, body: &[u8]) -> Result<Value, HttpError> {
    let request = decode_workers_request::<workers_wire::SelectDeploymentRequest>(
        content_type,
        body,
        "acyclic.workers.v1.SelectDeploymentRequest",
    )?;
    validate_select(&request).map_err(|error| HttpError {
        status: 422,
        message: format!("SelectDeploymentRequest rejected by Rust validation: {error}"),
    })?;
    encode_workers_response(
        &workers_wire::SelectDeploymentResponse {
            deployment: Some(workers_wire::Deployment {
                alias: request.alias,
                version: Some(workers_wire::CodeVersion {
                    sha256: request.version_sha256,
                    size_bytes: 1,
                }),
                revision: 1,
            }),
        },
        "acyclic.workers.v1.SelectDeploymentResponse",
    )
    .map_err(|error| HttpError {
        status: 500,
        message: error,
    })
}

fn workers_submit(content_type: &str, body: &[u8]) -> Result<Value, HttpError> {
    let request = decode_workers_request::<workers_wire::SubmitJobRequest>(
        content_type,
        body,
        "acyclic.workers.v1.SubmitJobRequest",
    )?;
    validate_submit(&request).map_err(|error| HttpError {
        status: 422,
        message: format!("SubmitJobRequest rejected by Rust validation: {error}"),
    })?;
    let resolved_sha256 = match request
        .target
        .as_ref()
        .and_then(|target| target.target.as_ref())
    {
        Some(workers_wire::job_target::Target::VersionSha256(value)) => value.clone(),
        _ => vec![7; 32],
    };
    let input = request
        .input
        .and_then(|input| input.source)
        .and_then(|source| match source {
            workers_wire::payload::Source::InlineBytes(value) => Some(value),
            workers_wire::payload::Source::Object(_) => None,
        })
        .unwrap_or_default();
    encode_workers_response(
        &workers_wire::SubmitJobResponse {
            job: Some(workers_job(
                "fixture-job",
                workers_wire::JobState::Accepted,
                resolved_sha256,
                input,
                false,
            )),
        },
        "acyclic.workers.v1.SubmitJobResponse",
    )
    .map_err(|error| HttpError {
        status: 500,
        message: error,
    })
}

fn workers_inspect(content_type: &str, body: &[u8]) -> Result<Value, HttpError> {
    let request = decode_workers_request::<workers_wire::InspectJobRequest>(
        content_type,
        body,
        "acyclic.workers.v1.InspectJobRequest",
    )?;
    encode_workers_response(
        &workers_wire::InspectJobResponse {
            job: Some(workers_job(
                &request.job_id,
                workers_wire::JobState::Succeeded,
                vec![7; 32],
                b"fixture-result".to_vec(),
                false,
            )),
        },
        "acyclic.workers.v1.InspectJobResponse",
    )
    .map_err(|error| HttpError {
        status: 500,
        message: error,
    })
}

fn workers_cancel(content_type: &str, body: &[u8]) -> Result<Value, HttpError> {
    let request = decode_workers_request::<workers_wire::CancelJobRequest>(
        content_type,
        body,
        "acyclic.workers.v1.CancelJobRequest",
    )?;
    encode_workers_response(
        &workers_wire::CancelJobResponse {
            job: Some(workers_job(
                &request.job_id,
                workers_wire::JobState::Cancelled,
                vec![7; 32],
                Vec::new(),
                true,
            )),
        },
        "acyclic.workers.v1.CancelJobResponse",
    )
    .map_err(|error| HttpError {
        status: 500,
        message: error,
    })
}

fn workers_invoke_version(content_type: &str, body: &[u8]) -> Result<Value, HttpError> {
    let request = decode_workers_request::<workers_wire::InvokeVersionRequest>(
        content_type,
        body,
        "acyclic.workers.v1.InvokeVersionRequest",
    )?;
    encode_workers_response(
        &workers_wire::InvokeResponse {
            status: 200,
            headers: vec![workers_wire::Header {
                name: "x-acyclic-fixture".to_owned(),
                value: "workers".to_owned(),
            }],
            body: request.body,
            resolved_sha256: request.version_sha256,
            resolved_revision: None,
        },
        "acyclic.workers.v1.InvokeResponse",
    )
    .map_err(|error| HttpError {
        status: 500,
        message: error,
    })
}

fn workers_invoke_deployment(content_type: &str, body: &[u8]) -> Result<Value, HttpError> {
    let request = decode_workers_request::<workers_wire::InvokeDeploymentRequest>(
        content_type,
        body,
        "acyclic.workers.v1.InvokeDeploymentRequest",
    )?;
    encode_workers_response(
        &workers_wire::InvokeResponse {
            status: 200,
            headers: vec![workers_wire::Header {
                name: "x-acyclic-fixture".to_owned(),
                value: "workers".to_owned(),
            }],
            body: request.body,
            resolved_sha256: vec![7; 32],
            resolved_revision: Some(1),
        },
        "acyclic.workers.v1.InvokeResponse",
    )
    .map_err(|error| HttpError {
        status: 500,
        message: error,
    })
}

fn workers_job(
    job_id: &str,
    state: workers_wire::JobState,
    resolved_sha256: Vec<u8>,
    body: Vec<u8>,
    cancellation_requested: bool,
) -> workers_wire::JobObservation {
    workers_wire::JobObservation {
        job_id: job_id.to_owned(),
        state: state as i32,
        resolved_sha256,
        attempt: 1,
        result: Some(workers_wire::JobResult { body }),
        failure_code: String::new(),
        cancellation_requested,
    }
}

fn decode_json_message<M: Message + Default>(body: &[u8], name: &str) -> Result<M, String> {
    decode_json_message_from_descriptor(body, name, FILE_DESCRIPTOR_SET)
}

fn decode_workers_request<M: Message + Default>(
    content_type: &str,
    body: &[u8],
    name: &str,
) -> Result<M, HttpError> {
    if content_type
        .split(';')
        .next()
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
        || body
            .iter()
            .copied()
            .find(|byte| !byte.is_ascii_whitespace())
            == Some(b'{')
    {
        decode_json_message_from_descriptor(body, name, WORKERS_FILE_DESCRIPTOR_SET).map_err(
            |error| HttpError {
                status: 400,
                message: format!("invalid {name} Protobuf JSON: {error}"),
            },
        )
    } else {
        M::decode(body).map_err(|error| HttpError {
            status: 400,
            message: format!("invalid {name} protobuf: {error}"),
        })
    }
}

fn decode_json_message_from_descriptor<M: Message + Default>(
    body: &[u8],
    name: &str,
    descriptor_bytes: &[u8],
) -> Result<M, String> {
    let pool = DescriptorPool::decode(descriptor_bytes)
        .map_err(|error| format!("decode Actors descriptor: {error}"))?;
    let descriptor = pool
        .get_message_by_name(name)
        .ok_or_else(|| format!("descriptor is missing: {name}"))?;
    let mut json = Deserializer::from_slice(body);
    let message = DynamicMessage::deserialize(descriptor, &mut json)
        .map_err(|error| format!("decode JSON message: {error}"))?;
    json.end()
        .map_err(|error| format!("trailing JSON: {error}"))?;
    message
        .transcode_to()
        .map_err(|error| format!("transcode JSON message: {error}"))
}

fn encode_workers_response<M: Message>(response: &M, name: &str) -> Result<Value, String> {
    let pool = DescriptorPool::decode(WORKERS_FILE_DESCRIPTOR_SET)
        .map_err(|error| format!("decode Workers descriptor: {error}"))?;
    let descriptor = pool
        .get_message_by_name(name)
        .ok_or_else(|| format!("descriptor is missing: {name}"))?;
    let message = DynamicMessage::decode(descriptor, response.encode_to_vec().as_slice())
        .map_err(|error| format!("encode {name}: {error}"))?;
    serde_json::to_value(message).map_err(|error| format!("serialize {name}: {error}"))
}

async fn stream_append(app: &App, body: &[u8]) -> Result<Value, HttpError> {
    let wire = stream_wire::AppendRequest::decode(body).map_err(|error| HttpError {
        status: 400,
        message: format!("invalid AppendRequest protobuf: {error}"),
    })?;
    let path = StreamPath::new(wire.path.clone()).map_err(|error| HttpError {
        status: 422,
        message: format!("invalid Stream path: {error}"),
    })?;
    let idempotency_key = wire
        .idempotency_key
        .map(IdempotencyKey::new)
        .transpose()
        .map_err(|error| HttpError {
            status: 422,
            message: format!("invalid idempotency key: {error}"),
        })?;
    let outcome = app
        .stream
        .append(AppendRequest {
            path,
            records: wire.records,
            if_tail: wire.if_tail,
            idempotency_key,
        })
        .await
        .map_err(|error| HttpError {
            status: 422,
            message: format!("Rust MemoryStream rejected append: {error}"),
        })?;
    match outcome {
        AppendOutcome::Committed(receipt) => Ok(json!({
            "schema": "acyclic.sdk.fixture-response.v1",
            "fixture_id": "stream-append-read-v2",
            "operation_id": "acyclic.stream.v2.Stream/Append",
            "status": "committed",
            "request_sha256": digest(body),
            "start": receipt.start,
            "end": receipt.end,
            "tail": receipt.tail,
            "commit_id_hex": hex(receipt.commit_id.as_bytes()),
            "service_availability": "not_claimed",
        })),
        AppendOutcome::TailConflict { actual_tail } => Ok(json!({
            "schema": "acyclic.sdk.fixture-response.v1",
            "fixture_id": "stream-append-read-v2",
            "status": "tail_conflict",
            "request_sha256": digest(body),
            "actual_tail": actual_tail,
            "service_availability": "not_claimed",
        })),
    }
}

async fn stream_read(app: &App, body: &[u8]) -> Result<Value, HttpError> {
    let wire = stream_wire::ReadRequest::decode(body).map_err(|error| HttpError {
        status: 400,
        message: format!("invalid ReadRequest protobuf: {error}"),
    })?;
    let path = StreamPath::new(wire.path).map_err(|error| HttpError {
        status: 422,
        message: format!("invalid Stream path: {error}"),
    })?;
    let mut records = app
        .stream
        .read(ReadRequest {
            path,
            from: wire.from,
            limit: wire.limit,
        })
        .await
        .map_err(|error| HttpError {
            status: 422,
            message: format!("Rust MemoryStream rejected read: {error}"),
        })?;
    let mut values = Vec::new();
    while let Some(record) = records.next().await {
        let record = record.map_err(|error| HttpError {
            status: 422,
            message: format!("Rust MemoryStream read failed: {error}"),
        })?;
        values.push(json!({
            "sequence": record.sequence,
            "value_hex": hex(&record.value),
        }));
    }
    Ok(json!({
        "schema": "acyclic.sdk.fixture-response.v1",
        "fixture_id": "stream-append-read-v2",
        "operation_id": "acyclic.stream.v2.Stream/Read",
        "status": "ok",
        "request_sha256": digest(body),
        "records": values,
        "service_availability": "not_claimed",
    }))
}

async fn read_request(stream: &mut TcpStream) -> Result<HttpRequest, HttpError> {
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        if bytes.len() > MAX_HEADER_BYTES {
            return Err(HttpError {
                status: 431,
                message: "request headers exceed the fixture server bound".to_owned(),
            });
        }
        let read = stream.read(&mut chunk).await.map_err(io_error)?;
        if read == 0 {
            return Err(HttpError {
                status: 400,
                message: "connection closed before request headers".to_owned(),
            });
        }
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(index) = bytes.windows(4).position(|window| {
            window
                == b"\r
\r
"
        }) {
            break index + 4;
        }
    };
    let headers = std::str::from_utf8(&bytes[..header_end]).map_err(|_| HttpError {
        status: 400,
        message: "request headers are not UTF-8".to_owned(),
    })?;
    let mut lines = headers.split(
        "\r
",
    );
    let request_line = lines.next().ok_or_else(|| HttpError {
        status: 400,
        message: "request line is missing".to_owned(),
    })?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().unwrap_or_default().to_owned();
    let path = request_parts.next().unwrap_or_default().to_owned();
    if method.is_empty() || path.is_empty() {
        return Err(HttpError {
            status: 400,
            message: "request line is malformed".to_owned(),
        });
    }
    let header_lines = lines.collect::<Vec<_>>();
    let authorization = header_lines.iter().find_map(|line| {
        line.split_once(':')
            .filter(|(name, _)| name.eq_ignore_ascii_case("authorization"))
            .map(|(_, value)| value.trim().to_owned())
    });
    let content_type = header_lines
        .iter()
        .find_map(|line| {
            line.split_once(':')
                .filter(|(name, _)| name.eq_ignore_ascii_case("content-type"))
                .map(|(_, value)| value.trim().to_owned())
        })
        .unwrap_or_else(|| "application/octet-stream".to_owned());
    let content_length = header_lines
        .iter()
        .find_map(|line| {
            line.split_once(':')
                .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                .map(|(_, value)| value.trim())
        })
        .unwrap_or("0")
        .parse::<usize>()
        .map_err(|_| HttpError {
            status: 400,
            message: "Content-Length is invalid".to_owned(),
        })?;
    if content_length > MAX_BODY_BYTES {
        return Err(HttpError {
            status: 413,
            message: "request body exceeds the fixture server bound".to_owned(),
        });
    }
    while bytes.len() - header_end < content_length {
        let read = stream.read(&mut chunk).await.map_err(io_error)?;
        if read == 0 {
            return Err(HttpError {
                status: 400,
                message: "connection closed before request body".to_owned(),
            });
        }
        bytes.extend_from_slice(&chunk[..read]);
    }
    Ok(HttpRequest {
        method,
        path,
        authorization,
        content_type,
        body: bytes[header_end..header_end + content_length].to_vec(),
    })
}

async fn write_json(stream: &mut TcpStream, status: u16, body: &Value) -> io::Result<()> {
    let body = serde_json::to_vec(body).map_err(io::Error::other)?;
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        405 => "Method Not Allowed",
        413 => "Payload Too Large",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        422 => "Unprocessable Entity",
        431 => "Request Header Fields Too Large",
        _ => "Not Found",
    };
    let header = format!(
        "HTTP/1.1 {status} {reason}\r
Content-Type: application/json\r
Content-Length: {}\r
Connection: close\r
\r
",
        body.len()
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.shutdown().await
}

fn io_error(error: io::Error) -> HttpError {
    HttpError {
        status: 400,
        message: format!("read request: {error}"),
    }
}

fn digest(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn source_sha256() -> String {
    format!("sha256:{:x}", Sha256::digest(include_bytes!("../lib.rs")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_have_a_bounded_default() {
        let options = parse_args(Vec::<String>::new()).expect("default options");
        assert_eq!(options.port, 0);
        assert_eq!(options.max_requests, DEFAULT_MAX_REQUESTS);
    }

    #[test]
    fn options_reject_an_unbounded_budget() {
        assert!(parse_args(vec!["--max-requests".into(), "0".into()]).is_err());
        assert!(
            parse_args(vec![
                "--max-requests".into(),
                (MAX_REQUEST_BUDGET + 1).to_string(),
            ])
            .is_err()
        );
    }

    #[test]
    fn actor_json_roundtrip_uses_canonical_response_fields() {
        let body = br#"{
            "codeSha256":"AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=",
            "homeRegion":"eu",
            "limits":{"handlerTimeoutMillis":"1000","memoryBytes":"1048576","checkpointBytes":"4096"},
            "subscriptions":[{"subscriptionId":"events","streamPath":"agents/example/events","start":{"cursor":"0"}}],
            "idempotencyKey":"create-example"
        }"#;
        let response = actors_create("application/json", body).expect("canonical Actors response");
        assert_eq!(response["actor"]["actorId"], "fixture-actor");
        assert_eq!(response["actor"]["homeRegion"], "eu");
        assert_eq!(response["actor"]["state"], "ACTOR_STATE_ACTIVE");
    }

    #[tokio::test]
    async fn control_handshake_requires_family_metadata_and_returns_archived_identity() {
        let family = BindingFamily::Actors;
        let request = control_protocol::HandshakeRequest {
            protocol: Some(control_protocol::ProtocolIdentity {
                version: family.package().to_owned(),
                descriptor_digest: format!(
                    "{:x}",
                    Sha256::digest(family.archived_runtime_descriptor())
                ),
            }),
            required: Some(control_protocol::CapabilitySet {
                capabilities: vec![control_protocol::Capability {
                    name: family.name().to_owned(),
                    version: family.package().to_owned(),
                }],
            }),
        };
        let mut request = Request::new(request);
        request.metadata_mut().insert(
            transport_control::FAMILY_METADATA_KEY,
            family.name().parse().unwrap(),
        );
        request
            .metadata_mut()
            .insert("authorization", "Bearer fixture-token".parse().unwrap());
        let response = ProtocolFixture
            .handshake(request)
            .await
            .expect("authorized control handshake")
            .into_inner();
        assert_eq!(response.protocol.unwrap().version, family.package());
        assert_eq!(response.supported.unwrap().capabilities.len(), 1);

        let unauthorized = Request::new(control_protocol::HandshakeRequest::default());
        assert_eq!(
            ProtocolFixture
                .handshake(unauthorized)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
    }

    #[tokio::test]
    async fn registered_control_grpc_service_round_trips_over_loopback() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = Arc::new(Notify::new());
        let server_shutdown = Arc::clone(&shutdown);
        tokio::spawn(async move {
            Server::builder()
                .add_service(ProtocolServiceServer::new(ProtocolFixture))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    server_shutdown.notified().await
                })
                .await
                .unwrap();
        });

        let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let family = BindingFamily::Workers;
        let mut request = Request::new(control_protocol::HandshakeRequest {
            protocol: Some(control_protocol::ProtocolIdentity {
                version: family.package().to_owned(),
                descriptor_digest: format!(
                    "{:x}",
                    Sha256::digest(family.archived_runtime_descriptor())
                ),
            }),
            required: None,
        });
        request.metadata_mut().insert(
            transport_control::FAMILY_METADATA_KEY,
            family.name().parse().unwrap(),
        );
        request
            .metadata_mut()
            .insert("authorization", "Bearer fixture-token".parse().unwrap());
        let response = ProtocolServiceClient::new(channel)
            .handshake(request)
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.protocol.unwrap().version, family.package());
        assert_eq!(
            response.supported.unwrap().capabilities[0].name,
            family.name()
        );
        shutdown.notify_waiters();
    }

    #[test]
    fn http_control_handshake_uses_the_registered_family_route() {
        let response =
            control_handshake_http("/v1/sdk/workers/handshake", Some("Bearer fixture-token"))
                .expect("Workers control route");
        assert_eq!(response["family"], "workers");
        assert_eq!(response["rpc"], transport_control::HANDSHAKE_RPC_PATH);
        assert_eq!(
            response["protocol"]["version"],
            BindingFamily::Workers.package()
        );
        let response =
            control_handshake_http("/v1/sdk/objects/handshake", Some("Bearer fixture-token"))
                .expect("Objects control route");
        assert_eq!(response["family"], "objects");
        assert_eq!(
            response["protocol"]["version"],
            BindingFamily::Objects.package()
        );
    }

    #[test]
    fn http_control_handshake_requires_bearer_authorization() {
        let error = control_handshake_http("/v1/sdk/workers/handshake", None)
            .expect_err("unauthenticated control route");
        assert_eq!(error.status, 401);
        let error = control_handshake_http("/v1/sdk/workers/handshake", Some("Bearer \r\n"))
            .expect_err("malformed bearer control route");
        assert_eq!(error.status, 401);
    }
}
