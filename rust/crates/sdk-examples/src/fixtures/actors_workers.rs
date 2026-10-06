//! Stateful Rust-owned gRPC fixtures for Actors and Workers qualification.
//!
//! Every method returns a typed, non-empty response derived from the request
//! and fixture state. These handlers are intentionally kept separate from the
//! executable router so the router can install the same source-owned service
//! in every transport test.

use std::{collections::BTreeMap, sync::Arc};

use acyclic_actors::wire as actors_wire;
use acyclic_workers::{validate_publish, validate_select, validate_submit, wire as workers_wire};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

fn fixture_digest() -> Vec<u8> {
    Sha256::digest(b"acyclic-rust-fixture").to_vec()
}

const FIXTURE_CHECKPOINT_UNIX_MILLIS: u64 = 1_700_000_000_123;

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
        // A newly-created subscription has not retried delivery yet. Keep
        // this semantic invariant in the Rust provider so generated clients
        // do not inherit a stale manifest value.
        retry_count: 0,
        failure_code: String::new(),
        failed_cursor: None,
    }
}

/// Shared stateful Actors implementation used by remote qualification.
#[derive(Clone)]
pub struct ActorsFixture {
	state: Arc<Mutex<ActorsState>>,
}

#[derive(Clone, Default)]
struct ActorsState {
	actor: actors_wire::ActorObservation,
	mutations: BTreeMap<String, Vec<u8>>,
}

impl Default for ActorsFixture {
    fn default() -> Self { Self::new() }
}

impl ActorsFixture {
	#[must_use]
	pub fn new() -> Self {
		Self {
			state: Arc::new(Mutex::new(ActorsState { actor: actor_observation(), mutations: BTreeMap::new() })),
		}
	}

	fn mutation_key(operation: &str, key: &str) -> String { format!("{operation}:{key}") }

	fn record_mutation(
		state: &mut ActorsState,
		operation: &str,
		key: &str,
		request: &impl prost::Message,
	) -> Result<bool, Status> {
		let encoded = request.encode_to_vec();
		let mutation_key = Self::mutation_key(operation, key);
		match state.mutations.get(&mutation_key) {
			Some(previous) if previous != &encoded => Err(Status::failed_precondition("idempotency key was reused with a different request")),
			Some(_) => Ok(true),
			None => {
				state.mutations.insert(mutation_key, encoded);
				Ok(false)
			}
		}
	}

	async fn current(&self) -> actors_wire::ActorObservation { self.state.lock().await.actor.clone() }
}

#[tonic::async_trait]
impl actors_wire::actors_service_server::ActorsService for ActorsFixture {
	async fn create_actor(&self, request: Request<actors_wire::CreateActorRequest>) -> Result<Response<actors_wire::CreateActorResponse>, Status> {
		let request = request.into_inner();
		let mut state = self.state.lock().await;
		if Self::record_mutation(&mut state, "create", &request.idempotency_key, &request)? {
			return Ok(Response::new(actors_wire::CreateActorResponse { actor: Some(state.actor.clone()) }));
		}
		state.actor.code_sha256 = if request.code_sha256.is_empty() { fixture_digest() } else { request.code_sha256 };
		if !request.home_region.is_empty() { state.actor.home_region = request.home_region; }
		state.actor.subscriptions = request.subscriptions.into_iter().map(subscription_observation).collect();
		state.actor.state = actors_wire::ActorState::Active as i32;
		state.actor.configuration_revision = 1;
		Ok(Response::new(actors_wire::CreateActorResponse { actor: Some(state.actor.clone()) }))
    }

    async fn update_actor(&self, request: Request<actors_wire::UpdateActorRequest>) -> Result<Response<actors_wire::UpdateActorResponse>, Status> {
		let request = request.into_inner();
		let mut state = self.state.lock().await;
		if !request.actor_id.is_empty() && request.actor_id != state.actor.actor_id { return Err(Status::not_found("actor not found")); }
		if Self::record_mutation(&mut state, "update", &request.idempotency_key, &request)? {
			return Ok(Response::new(actors_wire::UpdateActorResponse { actor: Some(state.actor.clone()) }));
		}
		if request.expected_configuration_revision != 0 && request.expected_configuration_revision != state.actor.configuration_revision { return Err(Status::aborted("configuration revision conflict")); }
		if !request.code_sha256.is_empty() { state.actor.code_sha256 = request.code_sha256; }
		state.actor.configuration_revision += 1;
		Ok(Response::new(actors_wire::UpdateActorResponse { actor: Some(state.actor.clone()) }))
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
		if !request.actor_id.is_empty() && request.actor_id != state.actor.actor_id { return Err(Status::not_found("actor not found")); }
		let spec = request.subscription.clone().unwrap_or(actors_wire::SubscriptionSpec { subscription_id: String::new(), stream_path: String::new(), start: None, placement_anchor: false });
		let subscription = subscription_observation(spec);
		if Self::record_mutation(&mut state, "add-subscription", &request.idempotency_key, &request)? {
			return Ok(Response::new(actors_wire::AddSubscriptionResponse { actor: Some(state.actor.clone()) }));
		}
		if state.actor.subscriptions.iter().any(|item| item.subscription_id == subscription.subscription_id) { return Err(Status::failed_precondition("subscription already exists")); }
		state.actor.subscriptions.push(subscription);
		state.actor.configuration_revision += 1;
		Ok(Response::new(actors_wire::AddSubscriptionResponse { actor: Some(state.actor.clone()) }))
    }

    async fn remove_subscription(&self, request: Request<actors_wire::RemoveSubscriptionRequest>) -> Result<Response<actors_wire::RemoveSubscriptionResponse>, Status> {
		let request = request.into_inner();
		let mut state = self.state.lock().await;
		if !request.actor_id.is_empty() && request.actor_id != state.actor.actor_id { return Err(Status::not_found("actor not found")); }
		let id = if request.subscription_id.is_empty() { "fixture-subscription" } else { &request.subscription_id };
		if Self::record_mutation(&mut state, "remove-subscription", &request.idempotency_key, &request)? {
			return Ok(Response::new(actors_wire::RemoveSubscriptionResponse { actor: Some(state.actor.clone()) }));
		}
		if !state.actor.subscriptions.iter().any(|item| item.subscription_id == id) { return Err(Status::not_found("subscription does not exist")); }
		state.actor.subscriptions.retain(|item| item.subscription_id != id);
		state.actor.configuration_revision += 1;
		Ok(Response::new(actors_wire::RemoveSubscriptionResponse { actor: Some(state.actor.clone()) }))
    }

    async fn resume_subscription(&self, request: Request<actors_wire::ResumeSubscriptionRequest>) -> Result<Response<actors_wire::ResumeSubscriptionResponse>, Status> {
		let request = request.into_inner();
		let mut state = self.state.lock().await;
		if !request.actor_id.is_empty() && request.actor_id != state.actor.actor_id { return Err(Status::not_found("actor not found")); }
		let id = if request.subscription_id.is_empty() { "fixture-subscription" } else { &request.subscription_id };
		if Self::record_mutation(&mut state, "resume-subscription", &request.idempotency_key, &request)? {
			return Ok(Response::new(actors_wire::ResumeSubscriptionResponse { actor: Some(state.actor.clone()) }));
		}
		let subscription = state.actor.subscriptions.iter_mut().find(|subscription| subscription.subscription_id == id).ok_or_else(|| Status::not_found("subscription does not exist"))?;
		subscription.state = actors_wire::SubscriptionState::Active as i32;
		state.actor.configuration_revision += 1;
		Ok(Response::new(actors_wire::ResumeSubscriptionResponse { actor: Some(state.actor.clone()) }))
    }

    async fn checkpoint_actor(&self, request: Request<actors_wire::CheckpointActorRequest>) -> Result<Response<actors_wire::CheckpointActorResponse>, Status> {
		let request = request.into_inner();
		let mut state = self.state.lock().await;
		if !request.actor_id.is_empty() && request.actor_id != state.actor.actor_id { return Err(Status::not_found("actor not found")); }
		if Self::record_mutation(&mut state, "checkpoint", &request.idempotency_key, &request)? {
			return Ok(Response::new(actors_wire::CheckpointActorResponse { actor: Some(state.actor.clone()) }));
		}
		state.actor.checkpoint_epoch += 1;
		state.actor.checkpoint_unix_millis = Some(FIXTURE_CHECKPOINT_UNIX_MILLIS);
		Ok(Response::new(actors_wire::CheckpointActorResponse { actor: Some(state.actor.clone()) }))
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
    idempotency: BTreeMap<String, String>,
    publish_idempotency: BTreeMap<String, Vec<u8>>,
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
        validate_publish(&request).map_err(|error| Status::invalid_argument(error.to_string()))?;
        let version = worker_version(&request.javascript_module);
        let mut state = self.state.lock().await;
        if let Some(existing) = state.publish_idempotency.get(&request.idempotency_key) {
            if existing != &version.sha256 { return Err(Status::already_exists("publish idempotency key is rebound")); }
        }
        state.version = Some(version.clone());
        state.publish_idempotency.insert(request.idempotency_key, version.sha256.clone());
        Ok(Response::new(workers_wire::PublishVersionResponse { version: Some(version) }))
    }

    async fn select_deployment(&self, request: Request<workers_wire::SelectDeploymentRequest>) -> Result<Response<workers_wire::SelectDeploymentResponse>, Status> {
        let request = request.into_inner();
        validate_select(&request).map_err(|error| Status::invalid_argument(error.to_string()))?;
        let mut state = self.state.lock().await;
        let version = state.version.clone().ok_or_else(|| Status::not_found("version not found"))?;
        if request.version_sha256 != version.sha256 { return Err(Status::not_found("version not found")); }
        let current_revision = state.deployment.as_ref().map_or(0, |deployment| deployment.revision);
        if request.expected_revision.is_some_and(|revision| revision != current_revision) { return Err(Status::aborted("deployment revision conflict")); }
        let deployment = workers_wire::Deployment { alias: if request.alias.is_empty() { "production".into() } else { request.alias }, version: Some(version), revision: current_revision + 1 };
        state.deployment = Some(deployment.clone());
        Ok(Response::new(workers_wire::SelectDeploymentResponse { deployment: Some(deployment) }))
    }

    async fn submit_job(&self, request: Request<workers_wire::SubmitJobRequest>) -> Result<Response<workers_wire::SubmitJobResponse>, Status> {
        let request = request.into_inner();
        validate_submit(&request).map_err(|error| Status::invalid_argument(error.to_string()))?;
        let mut state = self.state.lock().await;
        if let Some(existing) = state.idempotency.get(&request.idempotency_key) {
            if let Some(job) = state.jobs.get(existing).cloned() {
                return Ok(Response::new(workers_wire::SubmitJobResponse { job: Some(job) }));
            }
        }
        let version = state.version.clone().ok_or_else(|| Status::not_found("version not found"))?;
        let target = request.target.as_ref().and_then(|target| target.target.as_ref());
        match target {
            Some(workers_wire::job_target::Target::VersionSha256(value)) if value == &version.sha256 => {}
            Some(workers_wire::job_target::Target::DeploymentAlias(alias))
                if state.deployment.as_ref().is_some_and(|deployment| deployment.alias == *alias) => {}
            _ => return Err(Status::not_found("Workers target is unknown")),
        }
        let body = match request.input.and_then(|input| input.source) { Some(workers_wire::payload::Source::InlineBytes(bytes)) => bytes, Some(workers_wire::payload::Source::Object(object)) => object.key.into_bytes(), None => b"fixture-job".to_vec() };
        let id = format!("fixture-job-{}", hex::encode(Sha256::digest(request.idempotency_key.as_bytes())));
        let job = job_observation(id.clone(), version.sha256, body, workers_wire::JobState::Succeeded);
        state.jobs.insert(id, job.clone());
        state.idempotency.insert(request.idempotency_key, job.job_id.clone());
        Ok(Response::new(workers_wire::SubmitJobResponse { job: Some(job) }))
    }

    async fn inspect_job(&self, request: Request<workers_wire::InspectJobRequest>) -> Result<Response<workers_wire::InspectJobResponse>, Status> {
        let request = request.into_inner();
        let state = self.state.lock().await;
        let job = state.jobs.get(&request.job_id).cloned().ok_or_else(|| Status::not_found("Workers job is unknown"))?;
        Ok(Response::new(workers_wire::InspectJobResponse { job: Some(job) }))
    }

    async fn cancel_job(&self, request: Request<workers_wire::CancelJobRequest>) -> Result<Response<workers_wire::CancelJobResponse>, Status> {
        let request = request.into_inner();
        let mut state = self.state.lock().await;
        let job = state.jobs.get_mut(&request.job_id).ok_or_else(|| Status::not_found("Workers job is unknown"))?;
        job.state = workers_wire::JobState::Cancelled as i32;
        job.cancellation_requested = true;
        Ok(Response::new(workers_wire::CancelJobResponse { job: Some(job.clone()) }))
    }

    async fn invoke_version(&self, request: Request<workers_wire::InvokeVersionRequest>) -> Result<Response<workers_wire::InvokeResponse>, Status> {
        let request = request.into_inner();
        let state = self.state.lock().await;
        let version = state.version.clone().ok_or_else(|| Status::not_found("version not found"))?;
        if request.version_sha256 != version.sha256 { return Err(Status::not_found("version not found")); }
        Ok(Response::new(workers_wire::InvokeResponse { status: 200, headers: vec![workers_wire::Header { name: "content-type".into(), value: "application/octet-stream".into() }], body: if request.body.is_empty() { b"fixture-response".to_vec() } else { request.body }, resolved_sha256: version.sha256, resolved_revision: None }))
    }

    async fn invoke_deployment(&self, request: Request<workers_wire::InvokeDeploymentRequest>) -> Result<Response<workers_wire::InvokeResponse>, Status> {
        let request = request.into_inner();
        let state = self.state.lock().await;
        let deployment = state.deployment.clone().ok_or_else(|| Status::not_found("deployment not found"))?;
        if request.alias != deployment.alias { return Err(Status::not_found("deployment not found")); }
        let version = deployment.version.unwrap_or_else(|| worker_version(&default_module()));
        Ok(Response::new(workers_wire::InvokeResponse { status: 200, headers: vec![workers_wire::Header { name: "content-type".into(), value: "application/octet-stream".into() }], body: if request.body.is_empty() { b"fixture-response".to_vec() } else { request.body }, resolved_sha256: version.sha256, resolved_revision: Some(deployment.revision) }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_actors::wire::actors_service_server::ActorsService;
    use acyclic_workers::wire::workers_service_server::WorkersService;

    #[tokio::test]
    async fn actors_cover_all_operations_and_preserve_state() {
        let fixture = ActorsFixture::new();
        let create = actors_wire::CreateActorRequest {
            code_sha256: vec![7, 8],
            home_region: "eu-west".into(),
            bindings: Vec::new(),
            limits: None,
            subscriptions: vec![actors_wire::SubscriptionSpec {
                subscription_id: "events".into(),
                stream_path: "/events".into(),
                start: None,
                placement_anchor: true,
            }],
            idempotency_key: "create-1".into(),
        };
        let created = fixture.create_actor(Request::new(create.clone())).await.unwrap().into_inner().actor.unwrap();
        assert_eq!(created.actor_id, "fixture-actor");
        assert_eq!(created.configuration_revision, 1);
        assert_eq!(created.state, actors_wire::ActorState::Active as i32);
        assert_eq!(created.home_region, "eu-west");
        assert_eq!(created.subscriptions.len(), 1);
        assert_eq!(created.subscriptions[0].retry_count, 0);

        let replayed = fixture.create_actor(Request::new(create.clone())).await.unwrap().into_inner().actor.unwrap();
        assert_eq!(replayed.configuration_revision, created.configuration_revision);
        let mut conflicting_create = create.clone();
        conflicting_create.home_region = "us-east".into();
        assert_eq!(fixture.create_actor(Request::new(conflicting_create)).await.unwrap_err().code(), tonic::Code::FailedPrecondition);

        let update = actors_wire::UpdateActorRequest {
            actor_id: created.actor_id.clone(),
            code_sha256: vec![9],
            bindings: Vec::new(),
            limits: None,
            expected_configuration_revision: created.configuration_revision,
            idempotency_key: "update-1".into(),
        };
        let updated = fixture.update_actor(Request::new(update.clone())).await.unwrap().into_inner().actor.unwrap();
        assert_eq!(updated.actor_id, "fixture-actor");
        assert_eq!(updated.configuration_revision, 2);
        assert_eq!(updated.state, actors_wire::ActorState::Active as i32);
        let replayed_update = fixture.update_actor(Request::new(update.clone())).await.unwrap().into_inner().actor.unwrap();
        assert_eq!(replayed_update.configuration_revision, updated.configuration_revision);
        let inspected = fixture.inspect_actor(Request::new(actors_wire::InspectActorRequest { actor_id: created.actor_id.clone() })).await.unwrap().into_inner().actor.unwrap();
        assert_eq!(inspected.actor_id, "fixture-actor");
        assert_eq!(inspected.configuration_revision, 2);
        assert_eq!(inspected.state, actors_wire::ActorState::Active as i32);
        assert_eq!(inspected.code_sha256, vec![9]);

        let added = fixture.add_subscription(Request::new(actors_wire::AddSubscriptionRequest {
            actor_id: created.actor_id.clone(),
            subscription: Some(actors_wire::SubscriptionSpec { subscription_id: "audit".into(), stream_path: "/audit".into(), start: None, placement_anchor: false }),
            idempotency_key: "add-1".into(),
        })).await.unwrap().into_inner().actor.unwrap();
        assert_eq!(added.actor_id, "fixture-actor");
        assert_eq!(added.configuration_revision, 3);
        assert_eq!(added.state, actors_wire::ActorState::Active as i32);
        assert_eq!(added.subscriptions.len(), 2);
        let removed = fixture.remove_subscription(Request::new(actors_wire::RemoveSubscriptionRequest { actor_id: created.actor_id.clone(), subscription_id: "audit".into(), idempotency_key: "remove-1".into() })).await.unwrap().into_inner().actor.unwrap();
        assert_eq!(removed.actor_id, "fixture-actor");
        assert_eq!(removed.configuration_revision, 4);
        assert_eq!(removed.state, actors_wire::ActorState::Active as i32);
        assert_eq!(removed.subscriptions.len(), 1);
        let resumed = fixture.resume_subscription(Request::new(actors_wire::ResumeSubscriptionRequest { actor_id: created.actor_id.clone(), subscription_id: "events".into(), idempotency_key: "resume-1".into() })).await.unwrap().into_inner().actor.unwrap();
        assert_eq!(resumed.actor_id, "fixture-actor");
        assert_eq!(resumed.configuration_revision, 5);
        assert_eq!(resumed.state, actors_wire::ActorState::Active as i32);
        assert_eq!(resumed.subscriptions[0].state, actors_wire::SubscriptionState::Active as i32);
        let checkpointed = fixture.checkpoint_actor(Request::new(actors_wire::CheckpointActorRequest { actor_id: created.actor_id.clone(), idempotency_key: "checkpoint-1".into() })).await.unwrap().into_inner().actor.unwrap();
        assert_eq!(checkpointed.actor_id, "fixture-actor");
        assert_eq!(checkpointed.configuration_revision, 5);
        assert_eq!(checkpointed.state, actors_wire::ActorState::Active as i32);
        assert_eq!(checkpointed.checkpoint_epoch, 1);
        let invocation = fixture.invoke_actor(Request::new(actors_wire::InvokeActorRequest { actor_id: created.actor_id, method: "POST".into(), url: "/echo".into(), body: b"payload".to_vec(), headers: Vec::new() })).await.unwrap().into_inner();
        assert_eq!(invocation.status, 200);
        assert_eq!(invocation.body, b"payload");
    }

    #[tokio::test]
    async fn workers_cover_all_operations_and_bind_outputs_to_version() {
        let fixture = WorkersFixture::new();
        let module = b"export default { fetch() { return new Response('ok') } }".to_vec();
        let published = fixture.publish_version(Request::new(workers_wire::PublishVersionRequest { javascript_module: module.clone(), expected_sha256: Sha256::digest(&module).to_vec(), idempotency_key: "publish-1".into() })).await.unwrap().into_inner().version.unwrap();
        assert_eq!(published.size_bytes, module.len() as u64);
        let selected = fixture.select_deployment(Request::new(workers_wire::SelectDeploymentRequest { alias: "production".into(), version_sha256: published.sha256.clone(), expected_revision: None, idempotency_key: "select-1".into() })).await.unwrap().into_inner().deployment.unwrap();
        assert_eq!(selected.alias, "production");
        assert_eq!(selected.revision, 1);
        assert_eq!(selected.version.as_ref().unwrap().sha256, published.sha256);
        let submitted = fixture.submit_job(Request::new(workers_wire::SubmitJobRequest {
            target: Some(workers_wire::JobTarget { target: Some(workers_wire::job_target::Target::VersionSha256(published.sha256.clone())) }),
            input: Some(workers_wire::Payload { source: Some(workers_wire::payload::Source::InlineBytes(b"job-input".to_vec())) }),
            limits: Some(workers_wire::JobLimits { timeout_millis: 10_000, memory_bytes: 64 * 1024 * 1024, output_bytes: 1024 }),
            retry: Some(workers_wire::RetryPolicy { max_attempts: 3, backoff_millis: 10 }),
            idempotency_key: "job-1".into(),
        })).await.unwrap().into_inner().job.unwrap();
        assert_eq!(submitted.state, workers_wire::JobState::Succeeded as i32);
        assert_eq!(submitted.job_id, format!("fixture-job-{}", hex::encode(Sha256::digest(b"job-1"))));
        assert_eq!(submitted.resolved_sha256, published.sha256);
        let inspected = fixture.inspect_job(Request::new(workers_wire::InspectJobRequest { job_id: submitted.job_id.clone() })).await.unwrap().into_inner().job.unwrap();
        assert_eq!(inspected.job_id, submitted.job_id);
        assert_eq!(inspected.state, workers_wire::JobState::Succeeded as i32);
        assert_eq!(inspected.result.unwrap().body, b"job-input");
        let cancelled = fixture.cancel_job(Request::new(workers_wire::CancelJobRequest { job_id: submitted.job_id.clone(), idempotency_key: "cancel-1".into() })).await.unwrap().into_inner().job.unwrap();
        assert_eq!(cancelled.job_id, submitted.job_id);
        assert_eq!(cancelled.state, workers_wire::JobState::Cancelled as i32);
        assert!(cancelled.cancellation_requested);
        let invoked = fixture.invoke_version(Request::new(workers_wire::InvokeVersionRequest { version_sha256: published.sha256.clone(), method: "POST".into(), url: "/run".into(), headers: Vec::new(), body: b"invoke-input".to_vec() })).await.unwrap().into_inner();
        assert_eq!(invoked.status, 200);
        assert_eq!(invoked.resolved_sha256, published.sha256);
        let deployment_invoked = fixture.invoke_deployment(Request::new(workers_wire::InvokeDeploymentRequest { alias: "production".into(), method: "GET".into(), url: "/".into(), headers: Vec::new(), body: Vec::new() })).await.unwrap().into_inner();
        assert_eq!(deployment_invoked.resolved_revision, Some(selected.revision));
        assert!(!deployment_invoked.body.is_empty());
    }
}
