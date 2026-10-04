//! Stateful Rust-owned gRPC fixtures for Actors and Workers qualification.
//!
//! Every method returns a typed, non-empty response derived from the request
//! and fixture state. These handlers are intentionally kept separate from the
//! executable router so the router can install the same source-owned service
//! in every transport test.

use std::{collections::BTreeMap, sync::Arc, time::{SystemTime, UNIX_EPOCH}};

use acyclic_actors::wire as actors_wire;
use acyclic_workers::wire as workers_wire;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

fn fixture_digest() -> Vec<u8> {
    Sha256::digest(b"acyclic-rust-fixture").to_vec()
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

fn actor_observation() -> actors_wire::ActorObservation {
    actors_wire::ActorObservation {
        actor_id: "fixture-actor".into(),
        code_sha256: fixture_digest(),
        home_region: "local".into(),
        state: actors_wire::ActorState::Active as i32,
        subscriptions: Vec::new(),
        checkpoint_unix_millis: None,
        checkpoint_epoch: 0,
        configuration_revision: 1,
    }
}

fn subscription_observation(spec: actors_wire::SubscriptionSpec) -> actors_wire::SubscriptionObservation {
    actors_wire::SubscriptionObservation {
        subscription_id: if spec.subscription_id.is_empty() { "fixture-subscription".into() } else { spec.subscription_id },
        stream_path: if spec.stream_path.is_empty() { "/fixture/events".into() } else { spec.stream_path },
        state: actors_wire::SubscriptionState::Active as i32,
        delivered_cursor: 0,
        completed_cursor: 0,
        recoverable_cursor: 0,
        placement_anchor: spec.placement_anchor,
        retry_count: 0,
        failure_code: String::new(),
        failed_cursor: None,
    }
}

/// Shared stateful Actors implementation used by remote qualification.
#[derive(Clone)]
pub struct ActorsFixture {
    state: Arc<Mutex<actors_wire::ActorObservation>>,
}

impl Default for ActorsFixture {
    fn default() -> Self { Self::new() }
}

impl ActorsFixture {
    #[must_use]
    pub fn new() -> Self { Self { state: Arc::new(Mutex::new(actor_observation())) } }

    async fn current(&self) -> actors_wire::ActorObservation { self.state.lock().await.clone() }
}

#[tonic::async_trait]
impl actors_wire::actors_service_server::ActorsService for ActorsFixture {
    async fn create_actor(&self, request: Request<actors_wire::CreateActorRequest>) -> Result<Response<actors_wire::CreateActorResponse>, Status> {
        let request = request.into_inner();
        let mut state = self.state.lock().await;
        state.code_sha256 = if request.code_sha256.is_empty() { fixture_digest() } else { request.code_sha256 };
        if !request.home_region.is_empty() { state.home_region = request.home_region; }
        state.subscriptions = request.subscriptions.into_iter().map(subscription_observation).collect();
        state.state = actors_wire::ActorState::Active as i32;
        state.configuration_revision = 1;
        Ok(Response::new(actors_wire::CreateActorResponse { actor: Some(state.clone()) }))
    }

    async fn update_actor(&self, request: Request<actors_wire::UpdateActorRequest>) -> Result<Response<actors_wire::UpdateActorResponse>, Status> {
        let request = request.into_inner();
        let mut state = self.state.lock().await;
        if !request.actor_id.is_empty() && request.actor_id != state.actor_id { return Err(Status::not_found("actor not found")); }
        if request.expected_configuration_revision != 0 && request.expected_configuration_revision != state.configuration_revision { return Err(Status::aborted("configuration revision conflict")); }
        if !request.code_sha256.is_empty() { state.code_sha256 = request.code_sha256; }
        state.configuration_revision += 1;
        Ok(Response::new(actors_wire::UpdateActorResponse { actor: Some(state.clone()) }))
    }

    async fn inspect_actor(&self, request: Request<actors_wire::InspectActorRequest>) -> Result<Response<actors_wire::InspectActorResponse>, Status> {
        let request = request.into_inner();
        let state = self.current().await;
        if !request.actor_id.is_empty() && request.actor_id != state.actor_id { return Err(Status::not_found("actor not found")); }
        Ok(Response::new(actors_wire::InspectActorResponse { actor: Some(state) }))
    }

    async fn add_subscription(&self, request: Request<actors_wire::AddSubscriptionRequest>) -> Result<Response<actors_wire::AddSubscriptionResponse>, Status> {
        let request = request.into_inner();
        let mut state = self.state.lock().await;
        if !request.actor_id.is_empty() && request.actor_id != state.actor_id { return Err(Status::not_found("actor not found")); }
        let spec = request.subscription.unwrap_or(actors_wire::SubscriptionSpec { subscription_id: String::new(), stream_path: String::new(), start: None, placement_anchor: false });
        let subscription = subscription_observation(spec);
        state.subscriptions.retain(|item| item.subscription_id != subscription.subscription_id);
        state.subscriptions.push(subscription);
        state.configuration_revision += 1;
        Ok(Response::new(actors_wire::AddSubscriptionResponse { actor: Some(state.clone()) }))
    }

    async fn remove_subscription(&self, request: Request<actors_wire::RemoveSubscriptionRequest>) -> Result<Response<actors_wire::RemoveSubscriptionResponse>, Status> {
        let request = request.into_inner();
        let mut state = self.state.lock().await;
        if !request.actor_id.is_empty() && request.actor_id != state.actor_id { return Err(Status::not_found("actor not found")); }
        let id = if request.subscription_id.is_empty() { "fixture-subscription" } else { &request.subscription_id };
        state.subscriptions.retain(|item| item.subscription_id != id);
        state.configuration_revision += 1;
        Ok(Response::new(actors_wire::RemoveSubscriptionResponse { actor: Some(state.clone()) }))
    }

    async fn resume_subscription(&self, request: Request<actors_wire::ResumeSubscriptionRequest>) -> Result<Response<actors_wire::ResumeSubscriptionResponse>, Status> {
        let request = request.into_inner();
        let mut state = self.state.lock().await;
        if !request.actor_id.is_empty() && request.actor_id != state.actor_id { return Err(Status::not_found("actor not found")); }
        let id = if request.subscription_id.is_empty() { "fixture-subscription" } else { &request.subscription_id };
        for subscription in &mut state.subscriptions { if subscription.subscription_id == id { subscription.state = actors_wire::SubscriptionState::Active as i32; } }
        state.configuration_revision += 1;
        Ok(Response::new(actors_wire::ResumeSubscriptionResponse { actor: Some(state.clone()) }))
    }

    async fn checkpoint_actor(&self, request: Request<actors_wire::CheckpointActorRequest>) -> Result<Response<actors_wire::CheckpointActorResponse>, Status> {
        let request = request.into_inner();
        let mut state = self.state.lock().await;
        if !request.actor_id.is_empty() && request.actor_id != state.actor_id { return Err(Status::not_found("actor not found")); }
        state.checkpoint_epoch += 1;
        state.checkpoint_unix_millis = Some(now_millis());
        Ok(Response::new(actors_wire::CheckpointActorResponse { actor: Some(state.clone()) }))
    }

    async fn invoke_actor(&self, request: Request<actors_wire::InvokeActorRequest>) -> Result<Response<actors_wire::InvokeActorResponse>, Status> {
        let request = request.into_inner();
        let state = self.current().await;
        if !request.actor_id.is_empty() && request.actor_id != state.actor_id { return Err(Status::not_found("actor not found")); }
        Ok(Response::new(actors_wire::InvokeActorResponse { status: 200, body: request.body, headers: vec![actors_wire::Header { name: "content-type".into(), value: "application/octet-stream".into() }] }))
    }
}

#[derive(Clone, Default)]
struct WorkerState {
    version: Option<workers_wire::CodeVersion>,
    deployment: Option<workers_wire::Deployment>,
    jobs: BTreeMap<String, workers_wire::JobObservation>,
    next_job: u64,
}

/// Shared stateful Workers implementation used by remote qualification.
#[derive(Clone, Default)]
pub struct WorkersFixture { state: Arc<Mutex<WorkerState>> }

impl WorkersFixture { #[must_use] pub fn new() -> Self { Self::default() } }

fn default_module() -> Vec<u8> { b"export default { fetch() { return new Response('fixture') } }".to_vec() }

fn worker_version(bytes: &[u8]) -> workers_wire::CodeVersion { workers_wire::CodeVersion { sha256: Sha256::digest(bytes).to_vec(), size_bytes: bytes.len() as u64 } }

fn job_observation(id: String, version: Vec<u8>, body: Vec<u8>, state: workers_wire::JobState) -> workers_wire::JobObservation {
    workers_wire::JobObservation { job_id: id, state: state as i32, resolved_sha256: version, attempt: 1, result: Some(workers_wire::JobResult { body }), failure_code: String::new(), cancellation_requested: state == workers_wire::JobState::Cancelled }
}

#[tonic::async_trait]
impl workers_wire::workers_service_server::WorkersService for WorkersFixture {
    async fn publish_version(&self, request: Request<workers_wire::PublishVersionRequest>) -> Result<Response<workers_wire::PublishVersionResponse>, Status> {
        let request = request.into_inner();
        let bytes = if request.javascript_module.is_empty() { default_module() } else { request.javascript_module };
        let version = worker_version(&bytes);
        if !request.expected_sha256.is_empty() && request.expected_sha256 != version.sha256 { return Err(Status::invalid_argument("module digest mismatch")); }
        self.state.lock().await.version = Some(version.clone());
        Ok(Response::new(workers_wire::PublishVersionResponse { version: Some(version) }))
    }

    async fn select_deployment(&self, request: Request<workers_wire::SelectDeploymentRequest>) -> Result<Response<workers_wire::SelectDeploymentResponse>, Status> {
        let request = request.into_inner();
        let mut state = self.state.lock().await;
        let version = state.version.clone().unwrap_or_else(|| worker_version(&default_module()));
        if !request.version_sha256.is_empty() && request.version_sha256 != version.sha256 { return Err(Status::not_found("version not found")); }
        let current_revision = state.deployment.as_ref().map_or(0, |deployment| deployment.revision);
        if request.expected_revision.is_some_and(|revision| revision != current_revision) { return Err(Status::aborted("deployment revision conflict")); }
        let deployment = workers_wire::Deployment { alias: if request.alias.is_empty() { "production".into() } else { request.alias }, version: Some(version), revision: current_revision + 1 };
        state.deployment = Some(deployment.clone());
        Ok(Response::new(workers_wire::SelectDeploymentResponse { deployment: Some(deployment) }))
    }

    async fn submit_job(&self, request: Request<workers_wire::SubmitJobRequest>) -> Result<Response<workers_wire::SubmitJobResponse>, Status> {
        let request = request.into_inner();
        let mut state = self.state.lock().await;
        let version = state.version.clone().unwrap_or_else(|| worker_version(&default_module()));
        let body = match request.input.and_then(|input| input.source) { Some(workers_wire::payload::Source::InlineBytes(bytes)) => bytes, Some(workers_wire::payload::Source::Object(object)) => object.key.into_bytes(), None => b"fixture-job".to_vec() };
        state.next_job += 1;
        let id = format!("fixture-job-{}", state.next_job);
        let job = job_observation(id.clone(), version.sha256, body, workers_wire::JobState::Succeeded);
        state.jobs.insert(id, job.clone());
        Ok(Response::new(workers_wire::SubmitJobResponse { job: Some(job) }))
    }

    async fn inspect_job(&self, request: Request<workers_wire::InspectJobRequest>) -> Result<Response<workers_wire::InspectJobResponse>, Status> {
        let request = request.into_inner();
        let mut state = self.state.lock().await;
        let id = if request.job_id.is_empty() { "fixture-job-1".into() } else { request.job_id };
        let version = state.version.clone().unwrap_or_else(|| worker_version(&default_module()));
        let job = state.jobs.entry(id.clone()).or_insert_with(|| job_observation(id, version.sha256, b"fixture-job".to_vec(), workers_wire::JobState::Succeeded)).clone();
        Ok(Response::new(workers_wire::InspectJobResponse { job: Some(job) }))
    }

    async fn cancel_job(&self, request: Request<workers_wire::CancelJobRequest>) -> Result<Response<workers_wire::CancelJobResponse>, Status> {
        let request = request.into_inner();
        let mut state = self.state.lock().await;
        let id = if request.job_id.is_empty() { "fixture-job-1".into() } else { request.job_id };
        let version = state.version.clone().unwrap_or_else(|| worker_version(&default_module()));
        let job = state.jobs.entry(id.clone()).or_insert_with(|| job_observation(id, version.sha256, b"fixture-job".to_vec(), workers_wire::JobState::Accepted));
        job.state = workers_wire::JobState::Cancelled as i32;
        job.cancellation_requested = true;
        Ok(Response::new(workers_wire::CancelJobResponse { job: Some(job.clone()) }))
    }

    async fn invoke_version(&self, request: Request<workers_wire::InvokeVersionRequest>) -> Result<Response<workers_wire::InvokeResponse>, Status> {
        let request = request.into_inner();
        let state = self.state.lock().await;
        let version = state.version.clone().unwrap_or_else(|| worker_version(&default_module()));
        if !request.version_sha256.is_empty() && request.version_sha256 != version.sha256 { return Err(Status::not_found("version not found")); }
        Ok(Response::new(workers_wire::InvokeResponse { status: 200, headers: vec![workers_wire::Header { name: "content-type".into(), value: "application/octet-stream".into() }], body: if request.body.is_empty() { b"fixture-response".to_vec() } else { request.body }, resolved_sha256: version.sha256, resolved_revision: None }))
    }

    async fn invoke_deployment(&self, request: Request<workers_wire::InvokeDeploymentRequest>) -> Result<Response<workers_wire::InvokeResponse>, Status> {
        let request = request.into_inner();
        let state = self.state.lock().await;
        let deployment = state.deployment.clone().unwrap_or_else(|| workers_wire::Deployment { alias: "production".into(), version: Some(worker_version(&default_module())), revision: 1 });
        if !request.alias.is_empty() && request.alias != deployment.alias { return Err(Status::not_found("deployment not found")); }
        let version = deployment.version.unwrap_or_else(|| worker_version(&default_module()));
        Ok(Response::new(workers_wire::InvokeResponse { status: 200, headers: vec![workers_wire::Header { name: "content-type".into(), value: "application/octet-stream".into() }], body: if request.body.is_empty() { b"fixture-response".to_vec() } else { request.body }, resolved_sha256: version.sha256, resolved_revision: Some(deployment.revision) }))
    }
}
