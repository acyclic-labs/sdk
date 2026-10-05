//! Bounded Rust-owned loopback server for SDK transport qualification.
//!
//! The server accepts canonical protobuf request bytes and returns normalized
//! JSON receipts. It is deliberately process-local: it has a request budget,
//! binds only to loopback, and makes no hosted service-availability claim.

use acyclic_actors::{
    FILE_DESCRIPTOR_SET, validate_add_subscription, validate_create, validate_update,
    wire as actors_wire,
};
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
use std::collections::HashMap;
use std::io;
use std::net::IpAddr;
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
    bind_address: IpAddr,
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
    actors: ActorsFixture,
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

#[derive(Default)]
struct ActorsState {
    actor: Option<actors_wire::ActorObservation>,
    mutations: HashMap<String, Vec<u8>>,
}

#[derive(Clone)]
struct ActorsFixture {
    state: Arc<Mutex<ActorsState>>,
}

impl ActorsFixture {
    fn new(_app: GrpcApp) -> Self {
        Self::with_state(Arc::new(Mutex::new(ActorsState::default())))
    }

    fn with_state(state: Arc<Mutex<ActorsState>>) -> Self {
        Self {
            state,
        }
    }

    fn mutation_key(operation: &str, key: &str) -> String {
        format!("{operation}:{key}")
    }

    fn idempotency_error() -> Status {
        Status::failed_precondition("idempotency key was reused with a different request")
    }

    fn not_found_error() -> Status {
        Status::not_found("actor does not exist")
    }

    fn conflict_error(message: &'static str) -> Status {
        Status::failed_precondition(message)
    }

    fn observation_for_subscription(
        subscription: actors_wire::SubscriptionSpec,
    ) -> actors_wire::SubscriptionObservation {
        actors_wire::SubscriptionObservation {
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
        }
    }

    fn record_mutation(
        state: &mut ActorsState,
        operation: &str,
        key: &str,
        request: impl prost::Message,
    ) -> Result<bool, Status> {
        let request = request.encode_to_vec();
        let key = Self::mutation_key(operation, key);
        if let Some(previous) = state.mutations.get(&key) {
            if previous != &request {
                return Err(Self::idempotency_error());
            }
            return Ok(true);
        }
        state.mutations.insert(key, request);
        Ok(false)
    }

    fn mutation_replay(
        state: &ActorsState,
        operation: &str,
        key: &str,
        request: impl prost::Message,
    ) -> Result<bool, Status> {
        let request = request.encode_to_vec();
        let key = Self::mutation_key(operation, key);
        match state.mutations.get(&key) {
            Some(previous) if previous != &request => Err(Self::idempotency_error()),
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }

    fn actor_response(actor: &actors_wire::ActorObservation) -> actors_wire::ActorObservation {
        actor.clone()
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
        let mut state = self.state.lock().await;
        let actor = actors_wire::ActorObservation {
            actor_id: "fixture-actor".to_owned(),
            code_sha256: request.code_sha256.clone(),
            home_region: request.home_region.clone(),
            state: actors_wire::ActorState::Active as i32,
            subscriptions: request
                .subscriptions
                .iter()
                .cloned()
                .map(ActorsFixture::observation_for_subscription)
                .collect(),
            checkpoint_unix_millis: None,
            checkpoint_epoch: 0,
            configuration_revision: 1,
        };
        if state.actor.is_some() {
            Self::record_mutation(
                &mut state,
                "create",
                &request.idempotency_key,
                request.clone(),
            )?;
            return Ok(Response::new(actors_wire::CreateActorResponse {
                actor: state.actor.clone(),
            }));
        }
        Self::record_mutation(
            &mut state,
            "create",
            &request.idempotency_key,
            request.clone(),
        )?;
        state.actor = Some(actor.clone());
        Ok(Response::new(actors_wire::CreateActorResponse {
            actor: Some(actor),
        }))
    }

    async fn update_actor(
        &self,
        request: Request<actors_wire::UpdateActorRequest>,
    ) -> Result<Response<actors_wire::UpdateActorResponse>, Status> {
        let request = request.into_inner();
        validate_update(&request).map_err(|error| {
            Status::invalid_argument(format!("UpdateActorRequest rejected: {error}"))
        })?;
        let mut state = self.state.lock().await;
        {
            let current = state.actor.as_ref().ok_or_else(Self::not_found_error)?;
            if current.actor_id != request.actor_id {
                return Err(Self::not_found_error());
            }
        }
        let replay = Self::mutation_replay(
            &state,
            "update",
            &request.idempotency_key,
            request.clone(),
        )?;
        if replay {
            return Ok(Response::new(actors_wire::UpdateActorResponse {
                actor: state.actor.clone(),
            }));
        }
        let current = state.actor.as_ref().ok_or_else(Self::not_found_error)?;
        if request.expected_configuration_revision != current.configuration_revision {
            return Err(Self::conflict_error(
                "actor configuration revision does not match",
            ));
        }
        Self::record_mutation(
            &mut state,
            "update",
            &request.idempotency_key,
            request.clone(),
        )?;
        let current = state.actor.as_mut().ok_or_else(Self::not_found_error)?;
        current.code_sha256 = request.code_sha256;
        current.configuration_revision += 1;
        let actor = Self::actor_response(current);
        Ok(Response::new(actors_wire::UpdateActorResponse {
            actor: Some(actor),
        }))
    }

    async fn inspect_actor(
        &self,
        request: Request<actors_wire::InspectActorRequest>,
    ) -> Result<Response<actors_wire::InspectActorResponse>, Status> {
        let request = request.into_inner();
        let state = self.state.lock().await;
        let actor = state.actor.as_ref().ok_or_else(Self::not_found_error)?;
        if actor.actor_id != request.actor_id {
            return Err(Self::not_found_error());
        }
        Ok(Response::new(actors_wire::InspectActorResponse {
            actor: Some(actor.clone()),
        }))
    }

    async fn add_subscription(
        &self,
        request: Request<actors_wire::AddSubscriptionRequest>,
    ) -> Result<Response<actors_wire::AddSubscriptionResponse>, Status> {
        let request = request.into_inner();
        validate_add_subscription(&request).map_err(|error| {
            Status::invalid_argument(format!("AddSubscriptionRequest rejected: {error}"))
        })?;
        let mut state = self.state.lock().await;
        let subscription = request.subscription.clone().expect("validated subscription");
        {
            let current = state.actor.as_ref().ok_or_else(Self::not_found_error)?;
            if current.actor_id != request.actor_id {
                return Err(Self::not_found_error());
            }
            if current
                .subscriptions
                .iter()
                .any(|item| item.subscription_id == subscription.subscription_id)
            {
                return Err(Self::conflict_error("subscription already exists"));
            }
        }
        Self::record_mutation(
            &mut state,
            "add-subscription",
            &request.idempotency_key,
            request.clone(),
        )?;
        let current = state.actor.as_mut().ok_or_else(Self::not_found_error)?;
        current
            .subscriptions
            .push(ActorsFixture::observation_for_subscription(subscription));
        current.configuration_revision += 1;
        Ok(Response::new(actors_wire::AddSubscriptionResponse {
            actor: Some(current.clone()),
        }))
    }

    async fn remove_subscription(
        &self,
        request: Request<actors_wire::RemoveSubscriptionRequest>,
    ) -> Result<Response<actors_wire::RemoveSubscriptionResponse>, Status> {
        let request = request.into_inner();
        let mut state = self.state.lock().await;
        {
            let current = state.actor.as_ref().ok_or_else(Self::not_found_error)?;
            if current.actor_id != request.actor_id {
                return Err(Self::not_found_error());
            }
            if !current
                .subscriptions
                .iter()
                .any(|item| item.subscription_id == request.subscription_id)
            {
                return Err(Status::not_found("subscription does not exist"));
            }
        }
        Self::record_mutation(
            &mut state,
            "remove-subscription",
            &request.idempotency_key,
            request.clone(),
        )?;
        let current = state.actor.as_mut().ok_or_else(Self::not_found_error)?;
        current
            .subscriptions
            .retain(|item| item.subscription_id != request.subscription_id);
        current.configuration_revision += 1;
        Ok(Response::new(actors_wire::RemoveSubscriptionResponse {
            actor: Some(current.clone()),
        }))
    }

    async fn resume_subscription(
        &self,
        request: Request<actors_wire::ResumeSubscriptionRequest>,
    ) -> Result<Response<actors_wire::ResumeSubscriptionResponse>, Status> {
        let request = request.into_inner();
        let mut state = self.state.lock().await;
        {
            let current = state.actor.as_ref().ok_or_else(Self::not_found_error)?;
            if current.actor_id != request.actor_id {
                return Err(Self::not_found_error());
            }
            if !current
                .subscriptions
                .iter()
                .any(|item| item.subscription_id == request.subscription_id)
            {
                return Err(Status::not_found("subscription does not exist"));
            }
        }
        Self::record_mutation(
            &mut state,
            "resume-subscription",
            &request.idempotency_key,
            request.clone(),
        )?;
        let current = state.actor.as_mut().ok_or_else(Self::not_found_error)?;
        let subscription = current
            .subscriptions
            .iter_mut()
            .find(|item| item.subscription_id == request.subscription_id)
            .ok_or_else(|| Status::not_found("subscription does not exist"))?;
        subscription.state = actors_wire::SubscriptionState::Active as i32;
        current.configuration_revision += 1;
        Ok(Response::new(actors_wire::ResumeSubscriptionResponse {
            actor: Some(current.clone()),
        }))
    }

    async fn checkpoint_actor(
        &self,
        request: Request<actors_wire::CheckpointActorRequest>,
    ) -> Result<Response<actors_wire::CheckpointActorResponse>, Status> {
        let request = request.into_inner();
        let mut state = self.state.lock().await;
        {
            let current = state.actor.as_ref().ok_or_else(Self::not_found_error)?;
            if current.actor_id != request.actor_id {
                return Err(Self::not_found_error());
            }
        }
        Self::record_mutation(
            &mut state,
            "checkpoint",
            &request.idempotency_key,
            request.clone(),
        )?;
        let current = state.actor.as_mut().ok_or_else(Self::not_found_error)?;
        current.checkpoint_epoch += 1;
        current.checkpoint_unix_millis = Some(1_700_000_000_000 + current.checkpoint_epoch);
        Ok(Response::new(actors_wire::CheckpointActorResponse {
            actor: Some(current.clone()),
        }))
    }

    async fn invoke_actor(
        &self,
        request: Request<actors_wire::InvokeActorRequest>,
    ) -> Result<Response<actors_wire::InvokeActorResponse>, Status> {
        let request = request.into_inner();
        let state = self.state.lock().await;
        let actor = state.actor.as_ref().ok_or_else(Self::not_found_error)?;
        if actor.actor_id != request.actor_id {
            return Err(Self::not_found_error());
        }
        Ok(Response::new(actors_wire::InvokeActorResponse {
            status: 200,
            body: request.body,
            headers: request.headers,
        }))
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
#[derive(Clone, Default)]
struct WorkersFixture {
    state: Arc<Mutex<WorkersState>>,
}

#[derive(Default)]
struct WorkersState {
    versions: HashMap<Vec<u8>, workers_wire::CodeVersion>,
    modules: HashMap<Vec<u8>, Vec<u8>>,
    deployments: HashMap<String, workers_wire::Deployment>,
    jobs: HashMap<String, workers_wire::JobObservation>,
    idempotency: HashMap<String, String>,
    publish_idempotency: HashMap<String, Vec<u8>>,
}

impl WorkersFixture {
    fn invalid(error: impl std::fmt::Display) -> Status {
        Status::invalid_argument(format!("Workers fixture request: {error}"))
    }

    fn job_id(key: &str) -> String {
        format!("fixture-job-{}", hex::encode(Sha256::digest(key.as_bytes())))
    }

    fn invocation(
        body: Vec<u8>,
        sha256: Vec<u8>,
        revision: Option<u64>,
    ) -> workers_wire::InvokeResponse {
        workers_wire::InvokeResponse {
            status: 200,
            headers: vec![workers_wire::Header {
                name: "content-type".into(),
                value: "application/octet-stream".into(),
            }],
            body,
            resolved_sha256: sha256,
            resolved_revision: revision,
        }
    }
}

#[tonic::async_trait]
impl workers_wire::workers_service_server::WorkersService for WorkersFixture {
    async fn publish_version(
        &self,
        request: Request<workers_wire::PublishVersionRequest>,
    ) -> Result<Response<workers_wire::PublishVersionResponse>, Status> {
        let request = request.into_inner();
        validate_publish(&request).map_err(Self::invalid)?;
        let version = workers_wire::CodeVersion {
            sha256: request.expected_sha256.clone(),
            size_bytes: request.javascript_module.len() as u64,
        };
        let mut state = self.state.lock().await;
        if let Some(existing) = state.publish_idempotency.get(&request.idempotency_key) {
            if existing != &request.expected_sha256 {
                return Err(Status::already_exists("publish idempotency key is rebound"));
            }
        }
        state
            .versions
            .insert(request.expected_sha256.clone(), version.clone());
        state
            .modules
            .insert(request.expected_sha256.clone(), request.javascript_module);
        state
            .publish_idempotency
            .insert(request.idempotency_key, request.expected_sha256);
        Ok(Response::new(workers_wire::PublishVersionResponse {
            version: Some(version),
        }))
    }

    async fn select_deployment(
        &self,
        request: Request<workers_wire::SelectDeploymentRequest>,
    ) -> Result<Response<workers_wire::SelectDeploymentResponse>, Status> {
        let request = request.into_inner();
        validate_select(&request).map_err(Self::invalid)?;
        let mut state = self.state.lock().await;
        if !state.versions.contains_key(&request.version_sha256) {
            return Err(Status::not_found("Workers version is unknown"));
        }
        let current_revision = state
            .deployments
            .get(&request.alias)
            .map_or(0, |deployment| deployment.revision);
        if request
            .expected_revision
            .is_some_and(|expected| expected != current_revision)
        {
            return Err(Status::aborted("Workers deployment revision conflict"));
        }
        let deployment = workers_wire::Deployment {
            alias: request.alias.clone(),
            version: state.versions.get(&request.version_sha256).cloned(),
            revision: current_revision.saturating_add(1),
        };
        state.deployments.insert(request.alias, deployment.clone());
        Ok(Response::new(workers_wire::SelectDeploymentResponse {
            deployment: Some(deployment),
        }))
    }

    async fn submit_job(
        &self,
        request: Request<workers_wire::SubmitJobRequest>,
    ) -> Result<Response<workers_wire::SubmitJobResponse>, Status> {
        let request = request.into_inner();
        validate_submit(&request).map_err(Self::invalid)?;
        let mut state = self.state.lock().await;
        if let Some(existing) = state.idempotency.get(&request.idempotency_key) {
            if let Some(job) = state.jobs.get(existing).cloned() {
                return Ok(Response::new(workers_wire::SubmitJobResponse { job: Some(job) }));
            }
        }
        let sha256 = match request.target.as_ref().and_then(|target| target.target.as_ref()) {
            Some(workers_wire::job_target::Target::VersionSha256(value)) => value.clone(),
            Some(workers_wire::job_target::Target::DeploymentAlias(alias)) => state
                .deployments
                .get(alias)
                .and_then(|deployment| deployment.version.as_ref())
                .map(|version| version.sha256.clone())
                .ok_or_else(|| Status::not_found("Workers deployment is unknown"))?,
            None => return Err(Self::invalid("job target is required")),
        };
        if !state.versions.contains_key(&sha256) {
            return Err(Status::not_found("Workers version is unknown"));
        }
        let body = request
            .input
            .as_ref()
            .and_then(|input| input.source.as_ref())
            .and_then(|source| match source {
                workers_wire::payload::Source::InlineBytes(value) => Some(value.clone()),
                workers_wire::payload::Source::Object(_) => None,
            })
            .unwrap_or_default();
        let job_id = Self::job_id(&request.idempotency_key);
        let job = workers_wire::JobObservation {
            job_id: job_id.clone(),
            state: workers_wire::JobState::Succeeded as i32,
            resolved_sha256: sha256,
            attempt: 1,
            result: Some(workers_wire::JobResult { body }),
            failure_code: String::new(),
            cancellation_requested: false,
        };
        state.jobs.insert(job_id.clone(), job.clone());
        state.idempotency.insert(request.idempotency_key, job_id);
        Ok(Response::new(workers_wire::SubmitJobResponse { job: Some(job) }))
    }

    async fn inspect_job(
        &self,
        request: Request<workers_wire::InspectJobRequest>,
    ) -> Result<Response<workers_wire::InspectJobResponse>, Status> {
        let request = request.into_inner();
        let state = self.state.lock().await;
        let job = state
            .jobs
            .get(&request.job_id)
            .cloned()
            .ok_or_else(|| Status::not_found("Workers job is unknown"))?;
        Ok(Response::new(workers_wire::InspectJobResponse { job: Some(job) }))
    }

    async fn cancel_job(
        &self,
        request: Request<workers_wire::CancelJobRequest>,
    ) -> Result<Response<workers_wire::CancelJobResponse>, Status> {
        let request = request.into_inner();
        let mut state = self.state.lock().await;
        let job = state
            .jobs
            .get_mut(&request.job_id)
            .ok_or_else(|| Status::not_found("Workers job is unknown"))?;
        job.state = workers_wire::JobState::Cancelled as i32;
        job.cancellation_requested = true;
        Ok(Response::new(workers_wire::CancelJobResponse {
            job: Some(job.clone()),
        }))
    }

    async fn invoke_version(
        &self,
        request: Request<workers_wire::InvokeVersionRequest>,
    ) -> Result<Response<workers_wire::InvokeResponse>, Status> {
        let request = request.into_inner();
        let state = self.state.lock().await;
        if !state.versions.contains_key(&request.version_sha256) {
            return Err(Status::not_found("Workers version is unknown"));
        }
        Ok(Response::new(Self::invocation(
            request.body,
            request.version_sha256,
            None,
        )))
    }

    async fn invoke_deployment(
        &self,
        request: Request<workers_wire::InvokeDeploymentRequest>,
    ) -> Result<Response<workers_wire::InvokeResponse>, Status> {
        let request = request.into_inner();
        let state = self.state.lock().await;
        let deployment = state
            .deployments
            .get(&request.alias)
            .ok_or_else(|| Status::not_found("Workers deployment is unknown"))?;
        let version = deployment
            .version
            .as_ref()
            .ok_or_else(|| Status::internal("Workers deployment has no version"))?;
        Ok(Response::new(Self::invocation(
            request.body,
            version.sha256.clone(),
            Some(deployment.revision),
        )))
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = parse_args(env::args().skip(1))?;
    let listener = TcpListener::bind((options.bind_address, options.port)).await?;
    let address = listener.local_addr()?;
    let grpc_listener = TcpListener::bind((options.bind_address, options.grpc_port)).await?;
    let grpc_address = grpc_listener.local_addr()?;
    let fixtures = transport_fixtures();
    let source_sha256 = source_sha256();
    let shutdown = Arc::new(Notify::new());
    let grpc_requests = Arc::new(AtomicUsize::new(0));
    let stream = Arc::new(MemoryStream::new_with_clock(
        acyclic_stream::MemoryLimits::default(),
        fixture_clock::stream_clock(),
    ));
    let grpc_app = GrpcApp {
        stream: Arc::clone(&stream),
        requests: grpc_requests,
        max_requests: options.max_requests,
        shutdown: Arc::clone(&shutdown),
    };
    let actors_fixture = ActorsFixture::with_state(Arc::new(Mutex::new(ActorsState::default())));
    let http_actors = actors_fixture.clone();
    let grpc_actors = actors_fixture.clone();
    let app = App {
        stream: (*stream).clone(),
        actors: http_actors,
        requests: Arc::new(Mutex::new(0)),
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
            actors_wire::actors_service_server::ActorsServiceServer::new(grpc_actors)
            .max_decoding_message_size(MAX_BODY_BYTES)
            .max_encoding_message_size(MAX_BODY_BYTES),
            interceptor.clone(),
        );
        let workers = tonic::service::interceptor::InterceptedService::new(
            workers_wire::workers_service_server::WorkersServiceServer::new(WorkersFixture::default())
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
    io::Write::flush(&mut io::stdout())
        .map_err(|error| format!("flush fixture readiness: {error}"))?;
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
        bind_address: IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        port: 0,
        grpc_port: 0,
        max_requests: DEFAULT_MAX_REQUESTS,
    };
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--bind-address" => {
                options.bind_address = next_value(&mut arguments, "--bind-address")?
                    .parse()
                    .map_err(|_| "--bind-address must be an IP address".to_owned())?;
            }
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
                println!("fixture-server [--bind-address IP] [--port PORT] [--grpc-port PORT] [--max-requests N]");
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
        "/v1/actors/create" => {
            actors_http_create(&app.actors, &request.content_type, &request.body).await
        }
        "/v1/actors/update" => {
            actors_http_update(&app.actors, &request.content_type, &request.body).await
        }
        "/v1/actors/inspect" => {
            actors_http_inspect(&app.actors, &request.content_type, &request.body).await
        }
        "/v1/actors/subscriptions/add" => {
            actors_http_add_subscription(&app.actors, &request.content_type, &request.body).await
        }
        "/v1/actors/subscriptions/remove" => {
            actors_http_remove_subscription(&app.actors, &request.content_type, &request.body).await
        }
        "/v1/actors/subscriptions/resume" => {
            actors_http_resume_subscription(&app.actors, &request.content_type, &request.body).await
        }
        "/v1/actors/checkpoint" => {
            actors_http_checkpoint(&app.actors, &request.content_type, &request.body).await
        }
        "/v1/actors/invoke" => {
            actors_http_invoke(&app.actors, &request.content_type, &request.body).await
        }
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

fn actor_http_error(error: Status) -> HttpError {
    let status = match error.code() {
        tonic::Code::InvalidArgument => 400,
        tonic::Code::NotFound => 404,
        tonic::Code::AlreadyExists | tonic::Code::FailedPrecondition => 409,
        tonic::Code::Unauthenticated => 401,
        tonic::Code::PermissionDenied => 403,
        _ => 500,
    };
    HttpError {
        status,
        message: error.message().to_owned(),
    }
}

fn decode_actor_message<M: Message + Default>(
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
        decode_json_message(body, name).map_err(|error| HttpError {
            status: 400,
            message: format!("invalid {name} Protobuf JSON: {error}"),
        })
    } else {
        M::decode(body).map_err(|error| HttpError {
            status: 400,
            message: format!("invalid {name} protobuf: {error}"),
        })
    }
}

fn encode_actor_message<M: Message>(response: &M, name: &str) -> Result<Value, HttpError> {
    let pool = DescriptorPool::decode(FILE_DESCRIPTOR_SET).map_err(|error| HttpError {
        status: 500,
        message: format!("decode Actors descriptor: {error}"),
    })?;
    let descriptor = pool.get_message_by_name(name).ok_or_else(|| HttpError {
        status: 500,
        message: format!("descriptor is missing: {name}"),
    })?;
    let message = DynamicMessage::decode(descriptor, response.encode_to_vec().as_slice()).map_err(
        |error| HttpError {
            status: 500,
            message: format!("encode {name}: {error}"),
        },
    )?;
    serde_json::to_value(message).map_err(|error| HttpError {
        status: 500,
        message: format!("serialize {name}: {error}"),
    })
}

macro_rules! actor_http_unary {
    ($function:ident, $request:ty, $response:ty, $method:ident, $request_name:literal, $response_name:literal) => {
        async fn $function(
            fixture: &ActorsFixture,
            content_type: &str,
            body: &[u8],
        ) -> Result<Value, HttpError> {
            let request = decode_actor_message::<$request>(content_type, body, $request_name)?;
            let response = <ActorsFixture as actors_wire::actors_service_server::ActorsService>::$method(
                fixture,
                Request::new(request),
            )
            .await
            .map_err(actor_http_error)?
            .into_inner();
            encode_actor_message::<$response>(&response, $response_name)
        }
    };
}

actor_http_unary!(
    actors_http_create,
    actors_wire::CreateActorRequest,
    actors_wire::CreateActorResponse,
    create_actor,
    "acyclic.actors.v1.CreateActorRequest",
    "acyclic.actors.v1.CreateActorResponse"
);
actor_http_unary!(
    actors_http_update,
    actors_wire::UpdateActorRequest,
    actors_wire::UpdateActorResponse,
    update_actor,
    "acyclic.actors.v1.UpdateActorRequest",
    "acyclic.actors.v1.UpdateActorResponse"
);
actor_http_unary!(
    actors_http_inspect,
    actors_wire::InspectActorRequest,
    actors_wire::InspectActorResponse,
    inspect_actor,
    "acyclic.actors.v1.InspectActorRequest",
    "acyclic.actors.v1.InspectActorResponse"
);
actor_http_unary!(
    actors_http_add_subscription,
    actors_wire::AddSubscriptionRequest,
    actors_wire::AddSubscriptionResponse,
    add_subscription,
    "acyclic.actors.v1.AddSubscriptionRequest",
    "acyclic.actors.v1.AddSubscriptionResponse"
);
actor_http_unary!(
    actors_http_remove_subscription,
    actors_wire::RemoveSubscriptionRequest,
    actors_wire::RemoveSubscriptionResponse,
    remove_subscription,
    "acyclic.actors.v1.RemoveSubscriptionRequest",
    "acyclic.actors.v1.RemoveSubscriptionResponse"
);
actor_http_unary!(
    actors_http_resume_subscription,
    actors_wire::ResumeSubscriptionRequest,
    actors_wire::ResumeSubscriptionResponse,
    resume_subscription,
    "acyclic.actors.v1.ResumeSubscriptionRequest",
    "acyclic.actors.v1.ResumeSubscriptionResponse"
);
actor_http_unary!(
    actors_http_checkpoint,
    actors_wire::CheckpointActorRequest,
    actors_wire::CheckpointActorResponse,
    checkpoint_actor,
    "acyclic.actors.v1.CheckpointActorRequest",
    "acyclic.actors.v1.CheckpointActorResponse"
);
actor_http_unary!(
    actors_http_invoke,
    actors_wire::InvokeActorRequest,
    actors_wire::InvokeActorResponse,
    invoke_actor,
    "acyclic.actors.v1.InvokeActorRequest",
    "acyclic.actors.v1.InvokeActorResponse"
);

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
    async fn actors_fixture_update_is_stateful_and_idempotent() {
        let fixture = ActorsFixture::with_state(Arc::new(Mutex::new(ActorsState::default())));
        let create = actors_wire::CreateActorRequest {
            code_sha256: vec![1; 32],
            home_region: "eu".to_owned(),
            bindings: Vec::new(),
            limits: Some(actors_wire::ActorLimits {
                handler_timeout_millis: 1_000,
                memory_bytes: 1_024,
                checkpoint_bytes: 4_096,
            }),
            subscriptions: Vec::new(),
            idempotency_key: "create-actor".to_owned(),
        };
        <ActorsFixture as actors_wire::actors_service_server::ActorsService>::create_actor(
            &fixture,
            Request::new(create),
        )
        .await
        .expect("create actor");
        let update = actors_wire::UpdateActorRequest {
            actor_id: "fixture-actor".to_owned(),
            code_sha256: vec![2; 32],
            bindings: Vec::new(),
            limits: Some(actors_wire::ActorLimits {
                handler_timeout_millis: 2_000,
                memory_bytes: 2_048,
                checkpoint_bytes: 8_192,
            }),
            expected_configuration_revision: 1,
            idempotency_key: "update-actor".to_owned(),
        };
        let response = <ActorsFixture as actors_wire::actors_service_server::ActorsService>::update_actor(
            &fixture,
            Request::new(update.clone()),
        )
        .await
        .expect("update actor")
        .into_inner();
        let actor = response.actor.expect("updated actor observation");
        assert_eq!(actor.code_sha256, vec![2; 32]);
        assert_eq!(actor.configuration_revision, 2);
        let replay = <ActorsFixture as actors_wire::actors_service_server::ActorsService>::update_actor(
            &fixture,
            Request::new(update),
        )
        .await
        .expect("idempotent update replay")
        .into_inner()
        .actor
        .expect("replayed actor observation");
        assert_eq!(replay.configuration_revision, 2);
    }

    #[tokio::test]
    async fn workers_fixture_replays_inspects_cancels_and_rejects_invalid_requests() {
        let fixture = WorkersFixture::default();
        let module = b"export default 42".to_vec();
        let digest = Sha256::digest(&module).to_vec();
        let publish = workers_wire::PublishVersionRequest {
            javascript_module: module,
            expected_sha256: digest.clone(),
            idempotency_key: "publish-workers".to_owned(),
        };
        let published = <WorkersFixture as workers_wire::workers_service_server::WorkersService>::publish_version(
            &fixture,
            Request::new(publish.clone()),
        )
        .await
        .expect("publish Worker version")
        .into_inner()
        .version
        .expect("published version");
        assert_eq!(published.sha256, digest);
        let replayed_publish = <WorkersFixture as workers_wire::workers_service_server::WorkersService>::publish_version(
            &fixture,
            Request::new(publish),
        )
        .await
        .expect("replay Worker publication")
        .into_inner()
        .version
        .expect("replayed version");
        assert_eq!(replayed_publish, published);

        let deployment = <WorkersFixture as workers_wire::workers_service_server::WorkersService>::select_deployment(
            &fixture,
            Request::new(workers_wire::SelectDeploymentRequest {
                alias: "production".to_owned(),
                version_sha256: published.sha256.clone(),
                expected_revision: None,
                idempotency_key: "select-workers".to_owned(),
            }),
        )
        .await
        .expect("select Worker deployment")
        .into_inner()
        .deployment
        .expect("deployment");
        assert_eq!(deployment.revision, 1);

        let submit = workers_wire::SubmitJobRequest {
            target: Some(workers_wire::JobTarget {
                target: Some(workers_wire::job_target::Target::DeploymentAlias(
                    "production".to_owned(),
                )),
            }),
            input: Some(workers_wire::Payload {
                source: Some(workers_wire::payload::Source::InlineBytes(b"input".to_vec())),
            }),
            limits: Some(workers_wire::JobLimits {
                timeout_millis: 1_000,
                memory_bytes: 1_024,
                output_bytes: 1_024,
            }),
            retry: Some(workers_wire::RetryPolicy {
                max_attempts: 1,
                backoff_millis: 0,
            }),
            idempotency_key: "job-workers".to_owned(),
        };
        let accepted = <WorkersFixture as workers_wire::workers_service_server::WorkersService>::submit_job(
            &fixture,
            Request::new(submit.clone()),
        )
        .await
        .expect("submit Worker job")
        .into_inner()
        .job
        .expect("accepted job");
        assert_eq!(accepted.state, workers_wire::JobState::Succeeded as i32);
        assert_eq!(accepted.result.as_ref().unwrap().body, b"input");
        let replayed = <WorkersFixture as workers_wire::workers_service_server::WorkersService>::submit_job(
            &fixture,
            Request::new(submit),
        )
        .await
        .expect("replay Worker job")
        .into_inner()
        .job
        .expect("replayed job");
        assert_eq!(replayed, accepted);

        let cancelled = <WorkersFixture as workers_wire::workers_service_server::WorkersService>::cancel_job(
            &fixture,
            Request::new(workers_wire::CancelJobRequest {
                job_id: accepted.job_id.clone(),
                idempotency_key: "cancel-workers".to_owned(),
            }),
        )
        .await
        .expect("cancel Worker job")
        .into_inner()
        .job
        .expect("cancelled job");
        assert_eq!(cancelled.state, workers_wire::JobState::Cancelled as i32);
        assert!(cancelled.cancellation_requested);
        let inspected = <WorkersFixture as workers_wire::workers_service_server::WorkersService>::inspect_job(
            &fixture,
            Request::new(workers_wire::InspectJobRequest {
                job_id: accepted.job_id.clone(),
            }),
        )
        .await
        .expect("inspect cancelled Worker job")
        .into_inner()
        .job
        .expect("inspected job");
        assert_eq!(inspected, cancelled);
        let invalid = <WorkersFixture as workers_wire::workers_service_server::WorkersService>::publish_version(
            &fixture,
            Request::new(workers_wire::PublishVersionRequest::default()),
        )
        .await
        .expect_err("invalid publication must be rejected");
        assert_eq!(invalid.code(), tonic::Code::InvalidArgument);
        let missing = <WorkersFixture as workers_wire::workers_service_server::WorkersService>::inspect_job(
            &fixture,
            Request::new(workers_wire::InspectJobRequest {
                job_id: "missing-job".to_owned(),
            }),
        )
        .await
        .expect_err("unknown job must be rejected");
        assert_eq!(missing.code(), tonic::Code::NotFound);
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
