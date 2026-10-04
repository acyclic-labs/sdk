//! Rust-owned typed request manifest for cross-language fixture consumers.
//!
//! The manifest is derived directly from the descriptor sets exported by each
//! Rust contract crate. It therefore records the exact RPC and input message
//! identity that the generated clients must use. The encoded payload is made
//! with the same `prost-reflect` implementation used by the Rust transport
//! adapters; no language-specific request inventory is an authoring input.

use prost::Message;
use futures::StreamExt;
use bytes::Bytes;
use prost_reflect::{DescriptorPool, DynamicMessage};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tonic::Request;
use acyclic_sdk_contract_wire::bindings::BindingFamily;

/// One encoded protobuf response frame observed from a Rust fixture stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResponseFrameRecord {
    /// Monotonic zero-based position in the response stream.
    pub sequence: usize,
    /// Fully-qualified protobuf response message identity.
    pub response_type: String,
    /// Canonically encoded response bytes as base64.
    pub response_base64: String,
    /// SHA-256 of the canonical response bytes.
    pub response_sha256: String,
}

/// One deterministic request record emitted for a Rust contract RPC.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedRequestRecord {
    /// Contract family owning the RPC.
    pub family: String,
    /// Fully-qualified protobuf service method identity.
    pub rpc: String,
    /// Fully-qualified protobuf input message identity.
    pub request_type: String,
    /// Canonically encoded protobuf request bytes as base64.
    pub request_base64: String,
    /// SHA-256 of the canonical request bytes, lower-case hexadecimal.
    pub request_sha256: String,
    /// Wire expectation shared by all generated consumers.
    pub expected_wire: String,
    /// Exact protobuf response bytes observed from a Rust fixture, when present.
    pub response_type: Option<String>,
    pub response_base64: Option<String>,
    pub response_sha256: Option<String>,
    /// All response frames for streaming RPCs, in wire order.
    pub response_frames: Vec<ResponseFrameRecord>,
    /// Status of this manifest record. This describes encoding evidence only.
    pub expected_status: &'static str,
}

const EXPECTED_STATUS: &str = "descriptor-only";
const EXPECTED_WIRE: &str = "descriptor-input-only";

/// Build all 106 request records from the Rust descriptor authorities.
///
/// The function does not claim that a remote service accepted these requests.
/// It proves that each RPC has a Rust-owned input identity and a reproducible
/// canonical protobuf payload. The fixture server is responsible for adding
/// semantic request values and recording actual response observations.
pub fn records() -> Result<Vec<TypedRequestRecord>, String> {
    let mut output = Vec::with_capacity(106);
    append_pool(
        &mut output,
        "actors",
        acyclic_actors::FILE_DESCRIPTOR_SET,
    )?;
    append_pool(
        &mut output,
        "workers",
        acyclic_workers::FILE_DESCRIPTOR_SET,
    )?;
    append_pool(
        &mut output,
        "objects",
        acyclic_objects::v2::FILE_DESCRIPTOR_SET,
    )?;
    append_pool(
        &mut output,
        "stream",
        acyclic_stream::FILE_DESCRIPTOR_SET,
    )?;
    append_pool(
        &mut output,
        "filesystem",
        acyclic_fs::FILE_DESCRIPTOR_SET,
    )?;
    append_pool(
        &mut output,
        "harness",
        acyclic_harness::FILE_DESCRIPTOR_SET,
    )?;
    append_pool(
        &mut output,
        "inference",
        &BindingFamily::Inference.model_descriptor(),
    )?;
    append_pool(
        &mut output,
        "machines",
        acyclic_machines::FILE_DESCRIPTOR_SET,
    )?;

    if output.len() != 106 {
        return Err(format!(
            "Rust descriptor authorities produced {} RPC records; expected 106",
            output.len()
        ));
    }
    Ok(output)
}

/// Serialize the manifest in the stable JSON shape consumed by qualification.
pub fn manifest_json() -> Result<Value, String> {
    let records = records()?;
    Ok(json!({
        "schema_version": 2,
        "source": "rust-descriptor-authorities",
        "record_count": records.len(),
        "records": records.iter().map(record_json).collect::<Vec<_>>(),
    }))
}

/// Serialize the manifest with a trailing newline for checked-in artifacts.
pub fn manifest_json_pretty() -> Result<String, String> {
    let value = manifest_json()?;
    serde_json::to_string_pretty(&value)
        .map(|json| format!("{json}\n"))
        .map_err(|error| format!("serialize typed request manifest: {error}"))
}

/// Convert producer observations into a strict 106-record manifest.
///
/// Each value must be an object emitted by a Rust fixture producer with a
/// nested `request` object containing `type`, `bytes_base64`, and `sha256`.
/// The response status is copied when the producer returned a status object;
/// successful responses are recorded as `ok`. This is the only path that may
/// produce a qualification manifest: descriptor inventory alone cannot be
/// promoted to execution evidence.
pub fn records_from_observations(observations: &[Value]) -> Result<Vec<TypedRequestRecord>, String> {
    let mut output = Vec::with_capacity(observations.len());
    for observation in observations {
        let object = observation
            .as_object()
            .ok_or_else(|| "fixture observation is not an object".to_owned())?;
        let family = object
            .get("family")
            .and_then(Value::as_str)
            .ok_or_else(|| "fixture observation family is missing".to_owned())?;
        let rpc = object
            .get("rpc")
            .or_else(|| object.get("operation"))
            .and_then(Value::as_str)
            .ok_or_else(|| "fixture observation rpc is missing".to_owned())?;
        let request = object
            .get("request")
            .and_then(Value::as_object)
            .ok_or_else(|| format!("{rpc} request observation is missing"))?;
        let request_type = request
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("{rpc} request type is missing"))?;
        let request_base64 = request
            .get("bytes_base64")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("{rpc} request bytes_base64 is missing"))?;
        let request_sha256 = request
            .get("sha256")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("{rpc} request sha256 is missing"))?;
        let expected_status = object
            .get("response")
            .and_then(Value::as_object)
            .and_then(|response| response.get("status"))
            .and_then(Value::as_str)
            .unwrap_or("ok");
        output.push(TypedRequestRecord {
            family: family.to_owned(),
            rpc: rpc.to_owned(),
            request_type: request_type.to_owned(),
            request_base64: request_base64.to_owned(),
            request_sha256: request_sha256.to_owned(),
            expected_wire: format!("{EXPECTED_WIRE};sha256={request_sha256}"),
            response_type: None,
            response_base64: None,
            response_sha256: None,
            response_frames: Vec::new(),
            expected_status: if expected_status == "ok" {
                "observed-ok"
            } else {
                "observed-status"
            },
        });
    }
    if output.len() != 106 {
        return Err(format!(
            "actual fixture observations produced {}; expected 106",
            output.len()
        ));
    }
    let mut identities = std::collections::BTreeSet::new();
    for record in &output {
        if !identities.insert(record.rpc.as_str()) {
            return Err(format!("duplicate actual fixture RPC {}", record.rpc));
        }
    }
    Ok(output)
}

/// Serialize a strict manifest from actual Rust fixture observations.
pub fn manifest_json_from_observations(observations: &[Value]) -> Result<Value, String> {
    let records = records_from_observations(observations)?;
    Ok(json!({
        "schema_version": 2,
        "source": "rust-fixture-observations",
        "record_count": records.len(),
        "records": records.iter().map(record_json).collect::<Vec<_>>(),
    }))
}

/// Collect request bytes produced by the executable Rust scenario families.
/// Missing RPCs are left missing; the result is never padded with defaults.
pub async fn actual_records() -> Result<Vec<TypedRequestRecord>, String> {
    let mut output = Vec::new();
    output.extend(actor_worker_records().await?);
    output.extend(stream_records().await?);
    crate::workers_scenarios::execute()
        .map_err(|error| format!("execute Workers Rust fixture: {error}"))?;
    crate::objects_scenarios::execute()
        .await
        .map_err(|error| format!("execute Objects Rust fixture: {error}"))?;
    crate::inference_scenarios::execute()
        .map_err(|error| format!("execute Inference Rust fixture: {error}"))?;
    crate::machines_scenarios::execute()
        .await
        .map_err(|error| format!("execute Machines Rust fixture: {error}"))?;
    for fixture in crate::transport_fixtures() {
        if fixture.family == "actors" || fixture.family == "stream" {
            continue;
        }
        for request in fixture.requests {
            output.push(record_from_bytes(
                fixture.family,
                fixture.operation_id,
                request.message,
                &request.bytes,
                "rust-fixture-defined",
            ));
        }
    }
    let objects = crate::objects_scenarios::fixture();
    for request in objects.requests {
        let operation = if request.message.ends_with("GetObjectRequest") {
            "acyclic.objects.v2.ObjectsService/GetObject"
        } else {
            "acyclic.objects.v2.ObjectsService/PutObject"
        };
        output.push(record_from_bytes(
            "objects",
            operation,
            request.message,
            &request.bytes,
            "rust-fixture-defined",
        ));
    }
    let machines = crate::machines_scenarios::fixture();
    output.push(record_from_bytes(
        "machines",
        crate::machines_scenarios::OPERATION_ID,
        "acyclic.machines.v1.CreateMachineRequest",
        &machines.request,
        "rust-fixture-defined",
    ));
    let fs_harness = super::filesystem_harness_scenarios::export().await?;
    output.extend(fs_harness.iter().map(observation_record).collect::<Result<Vec<_>, _>>()?);
    ensure_unique(&output)?;
    Ok(output)
}

async fn actor_worker_records() -> Result<Vec<TypedRequestRecord>, String> {
    use acyclic_actors::wire as actors_wire;
    use acyclic_actors::wire::actors_service_server::ActorsService;
    use acyclic_workers::wire as workers_wire;
    use acyclic_workers::wire::workers_service_server::WorkersService;
    use crate::fixtures::actors_workers::{ActorsFixture, WorkersFixture};

    let actors = ActorsFixture::new();
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
    let create_response = actors
        .create_actor(Request::new(create.clone()))
        .await
        .map_err(|error| format!("Actors CreateActor: {error}"))?
        .into_inner();
    let created = create_response.actor.as_ref()
        .ok_or_else(|| "Actors CreateActor omitted actor".to_owned())?
        .clone();
    let update = actors_wire::UpdateActorRequest {
        actor_id: created.actor_id.clone(),
        code_sha256: vec![9],
        bindings: Vec::new(),
        limits: None,
        expected_configuration_revision: created.configuration_revision,
        idempotency_key: "update-1".into(),
    };
    let update_response = actors
        .update_actor(Request::new(update.clone()))
        .await
        .map_err(|error| format!("Actors UpdateActor: {error}"))?
        .into_inner();
    let updated = update_response.actor.as_ref()
        .ok_or_else(|| "Actors UpdateActor omitted actor".to_owned())?
        .clone();
    let inspect = actors_wire::InspectActorRequest { actor_id: created.actor_id.clone() };
    let inspect_response = actors.inspect_actor(Request::new(inspect.clone())).await.map_err(|error| format!("Actors InspectActor: {error}"))?.into_inner();
    let add = actors_wire::AddSubscriptionRequest {
        actor_id: created.actor_id.clone(),
        subscription: Some(actors_wire::SubscriptionSpec { subscription_id: "audit".into(), stream_path: "/audit".into(), start: None, placement_anchor: false }),
        idempotency_key: "add-1".into(),
    };
    let add_response = actors.add_subscription(Request::new(add.clone())).await.map_err(|error| format!("Actors AddSubscription: {error}"))?.into_inner();
    let added = add_response.actor.as_ref().ok_or_else(|| "Actors AddSubscription omitted actor".to_owned())?.clone();
    let remove = actors_wire::RemoveSubscriptionRequest { actor_id: created.actor_id.clone(), subscription_id: "audit".into(), idempotency_key: "remove-1".into() };
    let remove_response = actors.remove_subscription(Request::new(remove.clone())).await.map_err(|error| format!("Actors RemoveSubscription: {error}"))?.into_inner();
    let resume = actors_wire::ResumeSubscriptionRequest { actor_id: created.actor_id.clone(), subscription_id: "events".into(), idempotency_key: "resume-1".into() };
    let resume_response = actors.resume_subscription(Request::new(resume.clone())).await.map_err(|error| format!("Actors ResumeSubscription: {error}"))?.into_inner();
    let checkpoint = actors_wire::CheckpointActorRequest { actor_id: created.actor_id.clone(), idempotency_key: "checkpoint-1".into() };
    let checkpoint_response = actors.checkpoint_actor(Request::new(checkpoint.clone())).await.map_err(|error| format!("Actors CheckpointActor: {error}"))?.into_inner();
    let invoke = actors_wire::InvokeActorRequest { actor_id: created.actor_id, method: "POST".into(), url: "/echo".into(), body: b"payload".to_vec(), headers: Vec::new() };
    let invoke_response = actors.invoke_actor(Request::new(invoke.clone())).await.map_err(|error| format!("Actors InvokeActor: {error}"))?.into_inner();

    let mut output = vec![
        with_response(record_from_bytes("actors", "acyclic.actors.v1.ActorsService/CreateActor", "acyclic.actors.v1.CreateActorRequest", &create.encode_to_vec(), "rust-fixture-executed"), "acyclic.actors.v1.CreateActorResponse", &create_response),
        with_response(record_from_bytes("actors", "acyclic.actors.v1.ActorsService/UpdateActor", "acyclic.actors.v1.UpdateActorRequest", &update.encode_to_vec(), "rust-fixture-executed"), "acyclic.actors.v1.UpdateActorResponse", &update_response),
        with_response(record_from_bytes("actors", "acyclic.actors.v1.ActorsService/InspectActor", "acyclic.actors.v1.InspectActorRequest", &inspect.encode_to_vec(), "rust-fixture-executed"), "acyclic.actors.v1.InspectActorResponse", &inspect_response),
        with_response(record_from_bytes("actors", "acyclic.actors.v1.ActorsService/AddSubscription", "acyclic.actors.v1.AddSubscriptionRequest", &add.encode_to_vec(), "rust-fixture-executed"), "acyclic.actors.v1.AddSubscriptionResponse", &add_response),
        with_response(record_from_bytes("actors", "acyclic.actors.v1.ActorsService/RemoveSubscription", "acyclic.actors.v1.RemoveSubscriptionRequest", &remove.encode_to_vec(), "rust-fixture-executed"), "acyclic.actors.v1.RemoveSubscriptionResponse", &remove_response),
        with_response(record_from_bytes("actors", "acyclic.actors.v1.ActorsService/ResumeSubscription", "acyclic.actors.v1.ResumeSubscriptionRequest", &resume.encode_to_vec(), "rust-fixture-executed"), "acyclic.actors.v1.ResumeSubscriptionResponse", &resume_response),
        with_response(record_from_bytes("actors", "acyclic.actors.v1.ActorsService/CheckpointActor", "acyclic.actors.v1.CheckpointActorRequest", &checkpoint.encode_to_vec(), "rust-fixture-executed"), "acyclic.actors.v1.CheckpointActorResponse", &checkpoint_response),
        with_response(record_from_bytes("actors", "acyclic.actors.v1.ActorsService/InvokeActor", "acyclic.actors.v1.InvokeActorRequest", &invoke.encode_to_vec(), "rust-fixture-executed"), "acyclic.actors.v1.InvokeActorResponse", &invoke_response),
    ];
    let workers = WorkersFixture::new();
    let module = b"export default { fetch() { return new Response('ok') } }".to_vec();
    let publish = workers_wire::PublishVersionRequest { javascript_module: module.clone(), expected_sha256: Sha256::digest(&module).to_vec(), idempotency_key: "publish-1".into() };
    let publish_response = workers.publish_version(Request::new(publish.clone())).await.map_err(|error| format!("Workers PublishVersion: {error}"))?.into_inner();
    let version = publish_response.version.clone().ok_or_else(|| "Workers PublishVersion omitted version".to_owned())?;
    let select = workers_wire::SelectDeploymentRequest { alias: "production".into(), version_sha256: version.sha256.clone(), expected_revision: None, idempotency_key: "select-1".into() };
    let select_response = workers.select_deployment(Request::new(select.clone())).await.map_err(|error| format!("Workers SelectDeployment: {error}"))?.into_inner();
    let deployment = select_response.deployment.clone().ok_or_else(|| "Workers SelectDeployment omitted deployment".to_owned())?;
    let submit = workers_wire::SubmitJobRequest { target: None, input: Some(workers_wire::Payload { source: Some(workers_wire::payload::Source::InlineBytes(b"job-input".to_vec())) }), limits: None, retry: None, idempotency_key: "job-1".into() };
    let submit_response = workers.submit_job(Request::new(submit.clone())).await.map_err(|error| format!("Workers SubmitJob: {error}"))?.into_inner();
    let job = submit_response.job.clone().ok_or_else(|| "Workers SubmitJob omitted job".to_owned())?;
    let inspect_job = workers_wire::InspectJobRequest { job_id: job.job_id.clone() };
    let inspect_job_response = workers.inspect_job(Request::new(inspect_job.clone())).await.map_err(|error| format!("Workers InspectJob: {error}"))?.into_inner();
    let cancel = workers_wire::CancelJobRequest { job_id: job.job_id, idempotency_key: "cancel-1".into() };
    let cancel_response = workers.cancel_job(Request::new(cancel.clone())).await.map_err(|error| format!("Workers CancelJob: {error}"))?.into_inner();
    let invoke_version = workers_wire::InvokeVersionRequest { version_sha256: version.sha256, method: "POST".into(), url: "/run".into(), headers: Vec::new(), body: b"invoke-input".to_vec() };
    let invoke_version_response = workers.invoke_version(Request::new(invoke_version.clone())).await.map_err(|error| format!("Workers InvokeVersion: {error}"))?.into_inner();
    let invoke_deployment = workers_wire::InvokeDeploymentRequest { alias: deployment.alias, method: "GET".into(), url: "/".into(), headers: Vec::new(), body: Vec::new() };
    let invoke_deployment_response = workers.invoke_deployment(Request::new(invoke_deployment.clone())).await.map_err(|error| format!("Workers InvokeDeployment: {error}"))?.into_inner();
    output.extend([
        with_response(record_from_bytes("workers", "acyclic.workers.v1.WorkersService/PublishVersion", "acyclic.workers.v1.PublishVersionRequest", &publish.encode_to_vec(), "rust-fixture-executed"), "acyclic.workers.v1.PublishVersionResponse", &publish_response),
        with_response(record_from_bytes("workers", "acyclic.workers.v1.WorkersService/SelectDeployment", "acyclic.workers.v1.SelectDeploymentRequest", &select.encode_to_vec(), "rust-fixture-executed"), "acyclic.workers.v1.SelectDeploymentResponse", &select_response),
        with_response(record_from_bytes("workers", "acyclic.workers.v1.WorkersService/SubmitJob", "acyclic.workers.v1.SubmitJobRequest", &submit.encode_to_vec(), "rust-fixture-executed"), "acyclic.workers.v1.SubmitJobResponse", &submit_response),
        with_response(record_from_bytes("workers", "acyclic.workers.v1.WorkersService/InspectJob", "acyclic.workers.v1.InspectJobRequest", &inspect_job.encode_to_vec(), "rust-fixture-executed"), "acyclic.workers.v1.InspectJobResponse", &inspect_job_response),
        with_response(record_from_bytes("workers", "acyclic.workers.v1.WorkersService/CancelJob", "acyclic.workers.v1.CancelJobRequest", &cancel.encode_to_vec(), "rust-fixture-executed"), "acyclic.workers.v1.CancelJobResponse", &cancel_response),
        with_response(record_from_bytes("workers", "acyclic.workers.v1.WorkersService/InvokeVersion", "acyclic.workers.v1.InvokeVersionRequest", &invoke_version.encode_to_vec(), "rust-fixture-executed"), "acyclic.workers.v1.InvokeResponse", &invoke_version_response),
        with_response(record_from_bytes("workers", "acyclic.workers.v1.WorkersService/InvokeDeployment", "acyclic.workers.v1.InvokeDeploymentRequest", &invoke_deployment.encode_to_vec(), "rust-fixture-executed"), "acyclic.workers.v1.InvokeResponse", &invoke_deployment_response),
    ]);
    let _ = updated;
    let _ = added;
    Ok(output)
}


async fn stream_records() -> Result<Vec<TypedRequestRecord>, String> {
    use acyclic_stream::grpc::Service;
    use acyclic_stream::wire;
    use acyclic_stream::wire::stream_service_server::StreamService;
    use std::sync::Arc;

    let service = Service::new(Arc::new(acyclic_stream::MemoryStream::default()));
    let path = "/typed/stream".to_owned();
    let records: Vec<Bytes> = vec![Bytes::from_static(b"alpha"), Bytes::from_static(b"beta")];
    let key = Bytes::from_static(b"typed-stream-append");

    let inspect = wire::InspectIdempotencyRequest {
        idempotency_key: key.clone(),
    };
    let inspect_response = service
        .inspect_idempotency(Request::new(inspect.clone()))
        .await
        .map_err(|e| format!("Stream InspectIdempotency: {e}"))?
        .into_inner();

    let append = wire::AppendRequest {
        path: path.clone(),
        records: records.clone(),
        if_tail: Some(0),
        idempotency_key: Some(key.clone()),
    };
    let append_response = service
        .append(Request::new(append.clone()))
        .await
        .map_err(|e| format!("Stream Append: {e}"))?
        .into_inner();
    let commit_id = match append_response.outcome.as_ref() {
        Some(wire::append_response::Outcome::Committed(receipt)) => receipt.commit_id.clone(),
        _ => Bytes::new(),
    };

    let tail = wire::TailRequest { path: path.clone() };
    let tail_response = service
        .tail(Request::new(tail.clone()))
        .await
        .map_err(|e| format!("Stream Tail: {e}"))?
        .into_inner();

    let fork = wire::ForkRequest {
        source: path.clone(),
        destination: "/typed/stream-fork".into(),
        at_tail: Some(2),
        idempotency_key: Some(Bytes::from_static(b"typed-stream-fork")),
    };
    let fork_response = service
        .fork(Request::new(fork.clone()))
        .await
        .map_err(|e| format!("Stream Fork: {e}"))?
        .into_inner();

    let read = wire::ReadRequest {
        path: path.clone(),
        from: 0,
        limit: 10,
    };
    let mut read_stream = service
        .read(Request::new(read.clone()))
        .await
        .map_err(|e| format!("Stream Read: {e}"))?
        .into_inner();
    let mut read_frames = Vec::new();
    while let Some(frame) = read_stream.next().await {
        read_frames.push(frame.map_err(|e| format!("Stream Read frame: {e}"))?);
    }

    let follow = wire::FollowRequest {
        path: path.clone(),
        from: 2,
    };
    let mut follow_stream = service
        .follow(Request::new(follow.clone()))
        .await
        .map_err(|e| format!("Stream Follow: {e}"))?
        .into_inner();
    let follow_status =
        match tokio::time::timeout(std::time::Duration::from_millis(10), follow_stream.next())
            .await
        {
            Ok(Some(Ok(_))) => "observed-frame",
            Ok(Some(Err(_))) => "observed-status",
            Ok(None) | Err(_) => "observed-status",
        };

    let children = wire::ChildrenRequest {
        parent: None,
        limit: 10,
    };
    let mut children_stream = service
        .children(Request::new(children.clone()))
        .await
        .map_err(|e| format!("Stream Children: {e}"))?
        .into_inner();
    let mut children_frames = Vec::new();
    while let Some(frame) = children_stream.next().await {
        children_frames.push(frame.map_err(|e| format!("Stream Children frame: {e}"))?);
    }

    let children_page = wire::ChildrenPageRequest {
        parent: None,
        after: None,
        hierarchy_version: None,
        limit: 10,
    };
    let children_page_response = service
        .children_page(Request::new(children_page.clone()))
        .await
        .map_err(|e| format!("Stream ChildrenPage: {e}"))?
        .into_inner();

    let commit = wire::CommitRequest {
        conditions: Vec::new(),
        mutations: vec![wire::CommitMutation {
            mutation: Some(wire::commit_mutation::Mutation::Append(
                wire::AppendMutation {
                    path: "/typed/commit".into(),
                    records: vec![Bytes::from_static(b"commit")],
                },
            )),
        }],
        idempotency_key: Bytes::from_static(b"typed-stream-commit"),
        deadline_unix_millis: None,
    };
    let commit_response = service
        .commit(Request::new(commit.clone()))
        .await
        .map_err(|e| format!("Stream Commit: {e}"))?
        .into_inner();

    let read_commit = wire::ReadCommitRequest {
        commit_id: commit_id.clone(),
    };
    let read_commit_response = service
        .read_commit(Request::new(read_commit.clone()))
        .await
        .map_err(|e| format!("Stream ReadCommit: {e}"))?
        .into_inner();

    let output = vec![
        with_response(
            record_from_bytes(
                "stream",
                "acyclic.stream.v2.StreamService/InspectIdempotency",
                "acyclic.stream.v2.InspectIdempotencyRequest",
                &inspect.encode_to_vec(),
                "rust-fixture-executed",
            ),
            "acyclic.stream.v2.InspectIdempotencyResponse",
            &inspect_response,
        ),
        with_response(
            record_from_bytes(
                "stream",
                "acyclic.stream.v2.StreamService/Append",
                "acyclic.stream.v2.AppendRequest",
                &append.encode_to_vec(),
                "rust-fixture-executed",
            ),
            "acyclic.stream.v2.AppendResponse",
            &append_response,
        ),
        with_response(
            record_from_bytes(
                "stream",
                "acyclic.stream.v2.StreamService/Tail",
                "acyclic.stream.v2.TailRequest",
                &tail.encode_to_vec(),
                "rust-fixture-executed",
            ),
            "acyclic.stream.v2.TailResponse",
            &tail_response,
        ),
        with_response(
            record_from_bytes(
                "stream",
                "acyclic.stream.v2.StreamService/Fork",
                "acyclic.stream.v2.ForkRequest",
                &fork.encode_to_vec(),
                "rust-fixture-executed",
            ),
            "acyclic.stream.v2.ForkReceipt",
            &fork_response,
        ),
        with_frames(
            record_from_bytes(
                "stream",
                "acyclic.stream.v2.StreamService/Read",
                "acyclic.stream.v2.ReadRequest",
                &read.encode_to_vec(),
                "rust-fixture-executed",
            ),
            "acyclic.stream.v2.ReadResponse",
            &read_frames,
        ),
        record_from_bytes(
            "stream",
            "acyclic.stream.v2.StreamService/Follow",
            "acyclic.stream.v2.FollowRequest",
            &follow.encode_to_vec(),
            follow_status,
        ),
        with_frames(
            record_from_bytes(
                "stream",
                "acyclic.stream.v2.StreamService/Children",
                "acyclic.stream.v2.ChildrenRequest",
                &children.encode_to_vec(),
                "rust-fixture-executed",
            ),
            "acyclic.stream.v2.ChildrenResponse",
            &children_frames,
        ),
        with_response(
            record_from_bytes(
                "stream",
                "acyclic.stream.v2.StreamService/ChildrenPage",
                "acyclic.stream.v2.ChildrenPageRequest",
                &children_page.encode_to_vec(),
                "rust-fixture-executed",
            ),
            "acyclic.stream.v2.ChildrenPageResponse",
            &children_page_response,
        ),
        with_response(
            record_from_bytes(
                "stream",
                "acyclic.stream.v2.StreamService/Commit",
                "acyclic.stream.v2.CommitRequest",
                &commit.encode_to_vec(),
                "rust-fixture-executed",
            ),
            "acyclic.stream.v2.CommitResponse",
            &commit_response,
        ),
        with_response(
            record_from_bytes(
                "stream",
                "acyclic.stream.v2.StreamService/ReadCommit",
                "acyclic.stream.v2.ReadCommitRequest",
                &read_commit.encode_to_vec(),
                "rust-fixture-executed",
            ),
            "acyclic.stream.v2.CommittedEnvelope",
            &read_commit_response,
        ),
    ];
    Ok(output)
}

/// Serialize executable evidence with an explicit completeness bit.
pub async fn actual_manifest_json() -> Result<Value, String> {
    let observations = actual_records().await?;
    validate_actual_records(&observations)?;
    let expected = self::records()?;
    let observed = observations
        .iter()
        .map(|record| record.rpc.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let missing = expected
        .iter()
        .filter(|record| !observed.contains(record.rpc.as_str()))
        .map(|record| record.rpc.clone())
        .collect::<Vec<_>>();
    Ok(json!({
        "schema_version": 2,
        "source": "rust-executable-fixtures",
        "record_count": observations.len(),
        "complete": missing.is_empty() && observations.len() == 106,
        "missing_rpcs": missing,
        "records": observations.iter().map(record_json).collect::<Vec<_>>(),
    }))
}

fn observation_record(observation: &Value) -> Result<TypedRequestRecord, String> {
    let object = observation
        .as_object()
        .ok_or_else(|| "fixture observation is not an object".to_owned())?;
    let family = object
        .get("family")
        .and_then(Value::as_str)
        .ok_or_else(|| "fixture observation family is missing".to_owned())?;
    let rpc = object
        .get("rpc")
        .or_else(|| object.get("operation"))
        .and_then(Value::as_str)
        .ok_or_else(|| "fixture observation rpc is missing".to_owned())?;
    let request = object
        .get("request")
        .and_then(Value::as_object)
        .ok_or_else(|| format!("{rpc} request observation is missing"))?;
    let request_type = request
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{rpc} request type is missing"))?;
    let request_base64 = request
        .get("bytes_base64")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{rpc} request bytes_base64 is missing"))?;
    let request_sha256 = request
        .get("sha256")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{rpc} request sha256 is missing"))?;
    let expected_status = if object
        .get("response")
        .and_then(Value::as_object)
        .and_then(|response| response.get("status"))
        .is_some()
    {
        "observed-status"
    } else {
        "observed-ok"
    };
    let response_object = object.get("response").and_then(Value::as_object);
    let response_type = response_object
        .and_then(|response| response.get("type"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let response_base64 = response_object
        .and_then(|response| response.get("bytes_base64"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let response_sha256 = response_object
        .and_then(|response| response.get("sha256"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let response_frames = match (&response_type, &response_base64, &response_sha256) {
        (Some(response_type), Some(response_base64), Some(response_sha256)) => {
            vec![ResponseFrameRecord {
                sequence: 0,
                response_type: response_type.clone(),
                response_base64: response_base64.clone(),
                response_sha256: response_sha256.clone(),
            }]
        }
        _ => Vec::new(),
    };
    Ok(TypedRequestRecord {
        family: family.to_owned(),
        rpc: rpc.to_owned(),
        request_type: request_type.to_owned(),
        request_base64: request_base64.to_owned(),
        request_sha256: request_sha256.to_owned(),
        expected_wire: format!("canonical-protobuf-v3;sha256={request_sha256}"),
        response_type,
        response_base64,
        response_sha256,
        response_frames,
        expected_status,
    })
}

fn record_from_bytes(
    family: &str,
    rpc: &str,
    request_type: &str,
    bytes: &[u8],
    status: &'static str,
) -> TypedRequestRecord {
    let request_sha256 = format!("sha256:{}", hex(&Sha256::digest(bytes)));
    TypedRequestRecord {
        family: family.to_owned(),
        rpc: rpc.to_owned(),
        request_type: request_type.to_owned(),
        request_base64: base64(bytes),
        request_sha256: request_sha256.clone(),
        expected_wire: format!("canonical-protobuf-v3;sha256={request_sha256}"),
        response_type: None,
        response_base64: None,
        response_sha256: None,
        response_frames: Vec::new(),
        expected_status: status,
    }
}

fn with_response<M: Message>(
    mut record: TypedRequestRecord,
    response_type: &str,
    response: &M,
) -> TypedRequestRecord {
    let bytes = response.encode_to_vec();
    record.response_type = Some(response_type.to_owned());
    record.response_base64 = Some(base64(&bytes));
    record.response_sha256 = Some(format!("sha256:{}", hex(&Sha256::digest(&bytes))));
    record.response_frames = vec![ResponseFrameRecord {
        sequence: 0,
        response_type: response_type.to_owned(),
        response_base64: base64(&bytes),
        response_sha256: format!("sha256:{}", hex(&Sha256::digest(&bytes))),
    }];
    record
}

fn validate_actual_records(records: &[TypedRequestRecord]) -> Result<(), String> {
    let mut identities = std::collections::BTreeSet::new();
    for record in records {
        if !identities.insert(record.rpc.as_str()) {
            return Err(format!("duplicate actual fixture RPC {}", record.rpc));
        }
        if !matches!(
            record.expected_status,
            "rust-fixture-executed"
                | "rust-fixture-defined"
                | "observed-ok"
                | "observed-status"
        ) {
            return Err(format!(
                "{} has an unrecognized observed status {}",
                record.rpc, record.expected_status
            ));
        }
        let Some(response_type) = record.response_type.as_deref() else {
            if !record.response_frames.is_empty() {
                return Err(format!(
                    "{} has response frames but no top-level response type",
                    record.rpc
                ));
            }
            continue;
        };
        let response_base64 = record.response_base64.as_deref().ok_or_else(|| {
            format!("{} has a response type but no response bytes", record.rpc)
        })?;
        let response_sha256 = record.response_sha256.as_deref().ok_or_else(|| {
            format!("{} has response bytes but no response digest", record.rpc)
        })?;
        let response_bytes = decode_base64(response_base64)
            .map_err(|error| format!("{} response bytes: {error}", record.rpc))?;
        let actual_digest = format!("sha256:{}", hex(&Sha256::digest(&response_bytes)));
        if response_sha256 != actual_digest {
            return Err(format!(
                "{} response digest {response_sha256} does not match decoded bytes {actual_digest}",
                record.rpc
            ));
        }
        let first = record.response_frames.first().ok_or_else(|| {
            format!("{} response is missing its observed response frame", record.rpc)
        })?;
        if first.sequence != 0
            || first.response_type != response_type
            || first.response_base64 != response_base64
            || first.response_sha256 != response_sha256
        {
            return Err(format!(
                "{} response frame 0 does not match the decoded response",
                record.rpc
            ));
        }
        for (index, frame) in record.response_frames.iter().enumerate() {
            if frame.sequence != index {
                return Err(format!(
                    "{} response frame order is invalid at index {} (sequence {})",
                    record.rpc, index, frame.sequence
                ));
            }
            let bytes = decode_base64(&frame.response_base64)
                .map_err(|error| format!("{} response frame {index}: {error}", record.rpc))?;
            let digest = format!("sha256:{}", hex(&Sha256::digest(&bytes)));
            if frame.response_sha256 != digest {
                return Err(format!(
                    "{} response frame {index} digest {} does not match decoded bytes {digest}",
                    record.rpc, frame.response_sha256
                ));
            }
            if frame.response_type.is_empty() {
                return Err(format!(
                    "{} response frame {index} has no response type",
                    record.rpc
                ));
            }
        }
    }
    Ok(())
}


fn with_frames<M: Message>(
    mut record: TypedRequestRecord,
    response_type: &str,
    responses: &[M],
) -> TypedRequestRecord {
    let frames = responses.iter().map(Message::encode_to_vec).collect::<Vec<_>>();
    record.response_type = Some(response_type.to_owned());
    record.response_base64 = frames.first().map(|bytes| base64(bytes));
    record.response_sha256 = frames
        .first()
        .map(|bytes| format!("sha256:{}", hex(&Sha256::digest(bytes))));
    record.response_frames = frames
        .into_iter()
        .enumerate().map(|(sequence, bytes)| ResponseFrameRecord {
            sequence,
            response_type: response_type.to_owned(),
            response_base64: base64(&bytes),
            response_sha256: format!("sha256:{}", hex(&Sha256::digest(&bytes))),
        })
        .collect();
    record
}

fn ensure_unique(records: &[TypedRequestRecord]) -> Result<(), String> {
    let mut identities = std::collections::BTreeSet::new();
    for record in records {
        if !identities.insert(record.rpc.as_str()) {
            return Err(format!("duplicate actual fixture RPC {}", record.rpc));
        }
    }
    Ok(())
}

fn append_pool(
    output: &mut Vec<TypedRequestRecord>,
    family: &str,
    descriptor_bytes: &[u8],
) -> Result<(), String> {
    let pool = DescriptorPool::decode(descriptor_bytes)
        .map_err(|error| format!("decode {family} descriptor set: {error}"))?;
    for file in pool.files() {
        for service in file.services() {
            for method in service.methods() {
                let input = method.input();
                let request = DynamicMessage::new(input.clone());
                let bytes = request.encode_to_vec();
                let digest = Sha256::digest(&bytes);
                let request_sha256 = hex(&digest);
                output.push(TypedRequestRecord {
                    family: family.to_owned(),
                    rpc: method.full_name().to_owned(),
                    request_type: input.full_name().to_owned(),
                    request_base64: base64(&bytes),
                    request_sha256: request_sha256.clone(),
                    expected_wire: format!("{EXPECTED_WIRE};sha256={request_sha256}"),
                    response_type: None,
                    response_base64: None,
                    response_sha256: None,
            response_frames: Vec::new(),
                    expected_status: EXPECTED_STATUS,
                });
            }
        }
    }
    Ok(())
}

fn record_json(record: &TypedRequestRecord) -> Value {
    json!({
        "family": record.family,
        "rpc": record.rpc,
        "request_type": record.request_type,
        "request_base64": record.request_base64,
        "request_sha256": record.request_sha256,
        "expected_wire": record.expected_wire,
        "response_type": record.response_type,
        "response_base64": record.response_base64,
        "response_sha256": record.response_sha256,
        "response_frames": record.response_frames.iter().map(|frame| json!({
            "sequence": frame.sequence,
            "response_type": frame.response_type,
            "response_base64": frame.response_base64,
            "response_sha256": frame.response_sha256,
        })).collect::<Vec<_>>(),
        "expected_status": record.expected_status,
    })
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        output.push(ALPHABET[(first >> 2) as usize] as char);
        output.push(ALPHABET[((first & 0x03) << 4 | second >> 4) as usize] as char);
        output.push(if chunk.len() > 1 {
            ALPHABET[((second & 0x0f) << 2 | third >> 6) as usize] as char
        } else {
            '='
        });
        output.push(if chunk.len() > 2 {
            ALPHABET[(third & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    output
}

fn decode_base64(value: &str) -> Result<Vec<u8>, String> {
    if value.len() % 4 != 0 {
        return Err("base64 length is not a multiple of four".to_owned());
    }
    fn sextet(byte: u8) -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let bytes = value.as_bytes();
    let mut output = Vec::with_capacity(bytes.len() / 4 * 3);
    for (index, chunk) in bytes.chunks_exact(4).enumerate() {
        let first = sextet(chunk[0]).ok_or_else(|| format!("invalid base64 at byte {}", index * 4))?;
        let second = sextet(chunk[1]).ok_or_else(|| format!("invalid base64 at byte {}", index * 4 + 1))?;
        output.push((first << 2) | (second >> 4));
        if chunk[2] != b'=' {
            let third = sextet(chunk[2]).ok_or_else(|| format!("invalid base64 at byte {}", index * 4 + 2))?;
            output.push((second << 4) | (third >> 2));
            if chunk[3] != b'=' {
                let fourth = sextet(chunk[3]).ok_or_else(|| format!("invalid base64 at byte {}", index * 4 + 3))?;
                output.push((third << 6) | fourth);
            }
        } else if chunk[3] != b'=' {
            return Err("invalid base64 padding".to_owned());
        }
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::{base64, records, validate_actual_records, ResponseFrameRecord, TypedRequestRecord};

    fn observed_record() -> TypedRequestRecord {
        let encoded = base64(&[0]);
        let digest = "sha256:6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d";
        TypedRequestRecord {
            family: "actors".to_owned(),
            rpc: "acyclic.actors.v1.ActorsService/CreateActor".to_owned(),
            request_type: "acyclic.actors.v1.CreateActorRequest".to_owned(),
            request_base64: encoded.clone(),
            request_sha256: digest.to_owned(),
            expected_wire: "canonical-protobuf-v3".to_owned(),
            response_type: Some("acyclic.actors.v1.CreateActorResponse".to_owned()),
            response_base64: Some(encoded.clone()),
            response_sha256: Some(digest.to_owned()),
            response_frames: vec![ResponseFrameRecord {
                sequence: 0,
                response_type: "acyclic.actors.v1.CreateActorResponse".to_owned(),
                response_base64: encoded,
                response_sha256: digest.to_owned(),
            }],
            expected_status: "rust-fixture-executed",
        }
    }

    #[test]
    fn base64_matches_the_empty_protobuf_payload_shape() {
        assert_eq!(base64(&[]), "");
        assert_eq!(base64(&[0]), "AA==");
        assert_eq!(base64(&[0, 1]), "AAE=");
        assert_eq!(base64(&[0, 1, 2]), "AAEC");
    }

    #[test]
    fn descriptor_authorities_cover_all_rpcs() {
        let manifest = records().expect("Rust descriptors decode");
        assert_eq!(manifest.len(), 106);
        assert!(manifest.iter().all(|record| {
            !record.rpc.is_empty()
                && !record.request_type.is_empty()
                && record.expected_status == "descriptor-only"
        }));
    }

    #[test]
    fn observed_frames_accept_canonical_order() {
        validate_actual_records(&[observed_record()]).expect("canonical frame is valid");
    }

    #[test]
    fn observed_frames_reject_altered_order() {
        let mut record = observed_record();
        record.response_frames[0].sequence = 1;
        let error = validate_actual_records(&[record]).expect_err("altered order must fail");
        assert!(error.contains("frame 0"));
    }

    #[test]
    fn observed_frames_reject_altered_type_and_status() {
        let mut wrong_type = observed_record();
        wrong_type.response_frames[0].response_type = "forged.Response".to_owned();
        let error = validate_actual_records(&[wrong_type]).expect_err("altered type must fail");
        assert!(error.contains("frame 0"));

        let mut wrong_status = observed_record();
        wrong_status.expected_status = "forged-status";
        let error = validate_actual_records(&[wrong_status]).expect_err("altered status must fail");
        assert!(error.contains("unrecognized observed status"));
    }
}


