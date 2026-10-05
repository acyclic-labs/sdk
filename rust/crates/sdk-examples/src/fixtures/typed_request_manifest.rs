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
use std::{env, fs};
use tonic::{Code, Request};
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

/// One encoded protobuf request frame emitted by a Rust fixture stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestFrameRecord {
    /// Monotonic zero-based position in the request stream.
    pub sequence: usize,
    /// Fully-qualified protobuf request message identity.
    pub request_type: String,
    /// Canonically encoded request bytes as base64.
    pub request_base64: String,
    /// SHA-256 of the canonical request bytes.
    pub request_sha256: String,
}

/// One deterministic request record emitted for a Rust contract RPC.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedRequestRecord {
    /// Contract family owning the RPC.
    pub family: String,
    /// Fully-qualified protobuf service method identity.
    pub rpc: String,
    /// Rust scenario identity for qualification-only steps. This is omitted
    /// for ordinary wire operations and never replaces `rpc`.
    pub scenario_id: Option<String>,
    /// Fully-qualified protobuf input message identity.
    pub request_type: String,
    /// Canonically encoded protobuf request bytes as base64.
    pub request_base64: String,
    /// SHA-256 of the canonical request bytes, lower-case hexadecimal.
    pub request_sha256: String,
    /// All request frames for client-streaming RPCs, in wire order.
    pub request_frames: Vec<RequestFrameRecord>,
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
    /// Structured Rust-owned outcome expected from the operation.
    pub expected_outcome: ExpectedOutcome,
}

/// The semantic result a generated consumer must observe for one execution
/// step.  The legacy `expected_status` field remains for compatibility, while
/// this value distinguishes success, protocol errors, and stream termination.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpectedOutcome {
    pub kind: String,
    pub grpc_code: Option<String>,
    pub detail: Option<String>,
    pub terminal: Option<String>,
}

impl ExpectedOutcome {
    fn descriptor() -> Self {
        Self {
            kind: "descriptor-only".to_owned(),
            grpc_code: None,
            detail: None,
            terminal: None,
        }
    }

    fn success() -> Self {
        Self {
            kind: "success".to_owned(),
            grpc_code: Some("OK".to_owned()),
            detail: None,
            terminal: None,
        }
    }

    fn stream_terminal(terminal: &str, grpc_code: Option<&str>) -> Self {
        Self {
            kind: "stream".to_owned(),
            grpc_code: grpc_code.map(str::to_owned).or_else(|| Some("OK".to_owned())),
            detail: None,
            terminal: Some(terminal.to_owned()),
        }
    }

    fn error(code: Option<&str>, detail: Option<&str>, terminal: Option<&str>) -> Self {
        Self {
            kind: "error".to_owned(),
            grpc_code: code.map(str::to_owned),
            detail: detail.map(str::to_owned),
            terminal: terminal.map(str::to_owned),
        }
    }
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
        let raw_rpc = object
            .get("rpc")
            .or_else(|| object.get("operation"))
            .and_then(Value::as_str)
            .ok_or_else(|| "fixture observation rpc is missing".to_owned())?;
        let (rpc, scenario_id) = canonical_rpc(raw_rpc);
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
            scenario_id,
            request_type: request_type.to_owned(),
            request_base64: request_base64.to_owned(),
            request_sha256: request_sha256.to_owned(),
            request_frames: parse_request_frames(observation, &rpc)?,
            expected_wire: format!("{EXPECTED_WIRE};sha256={request_sha256}"),
            response_type: descriptor_response_type(&rpc)?,
            response_base64: None,
            response_sha256: None,
            response_frames: Vec::new(),
            expected_status: if expected_status == "ok" {
                "observed-ok"
            } else {
                "observed-status"
            },
            expected_outcome: observation_outcome(object),
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

fn observation_outcome(object: &serde_json::Map<String, Value>) -> ExpectedOutcome {
    let response = object.get("response").and_then(Value::as_object);
    if response
        .and_then(|response| response.get("status"))
        .and_then(Value::as_str)
        == Some("error")
    {
        return ExpectedOutcome::error(
            response
                .and_then(|response| response.get("code"))
                .and_then(Value::as_str),
            response
                .and_then(|response| response.get("details"))
                .and_then(Value::as_str),
            Some("error"),
        );
    }
    if object
        .get("response_frames")
        .and_then(Value::as_array)
        .is_some_and(|frames| frames.len() > 1)
    {
        return ExpectedOutcome::stream_terminal("eof", None);
    }
    ExpectedOutcome::success()
}

fn canonical_rpc(raw_rpc: &str) -> (String, Option<String>) {
    const NEGATIVE_SUFFIX: &str = "/after-completion";
    raw_rpc
        .strip_suffix(NEGATIVE_SUFFIX)
        .map(|rpc| (rpc.to_owned(), Some(raw_rpc.to_owned())))
        .unwrap_or_else(|| (raw_rpc.to_owned(), None))
}

fn parse_request_frames(
    observation: &Value,
    rpc: &str,
) -> Result<Vec<RequestFrameRecord>, String> {
    let Some(frames) = observation.get("request_frames").and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    frames
        .iter()
        .enumerate()
        .map(|(sequence, frame)| {
            let frame = frame
                .as_object()
                .ok_or_else(|| format!("{rpc} request frame is not an object"))?;
            Ok(RequestFrameRecord {
                sequence: frame
                    .get("sequence")
                    .and_then(Value::as_u64)
                    .map_or(sequence, |value| value as usize),
                request_type: frame
                    .get("type")
                    .and_then(Value::as_str)
                    .ok_or_else(|| format!("{rpc} request frame type is missing"))?
                    .to_owned(),
                request_base64: frame
                    .get("bytes_base64")
                    .and_then(Value::as_str)
                    .ok_or_else(|| format!("{rpc} request frame bytes are missing"))?
                    .to_owned(),
                request_sha256: frame
                    .get("sha256")
                    .and_then(Value::as_str)
                    .ok_or_else(|| format!("{rpc} request frame digest is missing"))?
                    .to_owned(),
            })
        })
        .collect()
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

/// Collect the complete ordered execution plan produced by the executable Rust
/// scenario families.
///
/// This intentionally retains repeated setup calls and protocol-negative
/// steps.  A replay client needs those calls in order to obtain the runtime
/// identities consumed by later requests; the 106-record inventory is only a
/// projection of this plan.
pub async fn actual_execution_records() -> Result<Vec<TypedRequestRecord>, String> {
    let mut output = Vec::new();
    let inference_transcript = env::var_os("ACYCLIC_INFERENCE_TRANSCRIPT_FILE");
    output.extend(actor_worker_records().await?);
    output.extend(stream_records().await?);
    crate::workers_scenarios::execute()
        .map_err(|error| format!("execute Workers Rust fixture: {error}"))?;
    let objects_observations = crate::fixtures::objects_typed_scenarios::collect_network_qualification()
        .await
        .map_err(|error| format!("collect Objects Rust fixture: {error}"))?;
    output.extend(
        objects_observations
            .iter()
            .map(observation_record)
            .collect::<Result<Vec<_>, _>>()?,
    );
    crate::inference_scenarios::execute()
        .map_err(|error| format!("execute Inference Rust fixture: {error}"))?;
    crate::machines_scenarios::execute()
        .await
        .map_err(|error| format!("execute Machines Rust fixture: {error}"))?;
    if inference_transcript.is_none() {
        output.extend(inference_fixture_records().await?);
    }
    output.extend(
        crate::fixtures::machines::collect()
            .await?
            .into_iter()
            .map(machine_observation_record),
    );
    for fixture in crate::transport_fixtures() {
        if fixture.family == "actors"
            || fixture.family == "stream"
            || fixture.family == "inference"
            || fixture.family == "machines"
        {
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
    if let Some(path) = inference_transcript {
        let bytes = fs::read(&path).map_err(|error| {
            format!(
                "read Rust Inference transcript {}: {error}",
                path.to_string_lossy()
            )
        })?;
        let document: Value = serde_json::from_slice(&bytes)
            .map_err(|error| format!("decode Rust Inference transcript: {error}"))?;
        output.extend(inference_records_from_transcript(&document)?);
    }
    let fs_harness = super::filesystem_harness_scenarios::export().await?;
    output.extend(fs_harness.iter().map(observation_record).collect::<Result<Vec<_>, _>>()?);
    Ok(output)
}

/// Collect the unique 106-record inventory from the ordered Rust execution
/// plan.  Repeated setup calls and protocol-negative steps remain available
/// through [`actual_execution_records`].
pub async fn actual_records() -> Result<Vec<TypedRequestRecord>, String> {
    let execution = actual_execution_records().await?;
    let mut output = Vec::with_capacity(106);
    let mut identities = std::collections::BTreeSet::new();
    for record in execution {
        // These are executable protocol-validation steps, rather than
        // descriptor RPC identities, so they belong only to the plan.
        if record.rpc.ends_with("/after-completion") {
            continue;
        }
        if identities.insert(record.rpc.clone()) {
            output.push(record);
        }
    }
    ensure_unique(&output)?;
    if output.len() != 106 {
        return Err(format!(
            "Rust executable fixture inventory produced {}; expected 106",
            output.len()
        ));
    }
    Ok(output)
}

async fn inference_fixture_records() -> Result<Vec<TypedRequestRecord>, String> {
    use crate::tls_fixture::{
        InferenceMetadataFixture, InferenceRunsFixture, new_method_transcript_log,
    };
    use acyclic_inference::wire::{
        contexts_service_server::ContextsService,
        evaluations_service_server::EvaluationsService,
        models_service_server::ModelsService,
        runs_service_server::RunsService,
        warm_contexts_service_server::WarmContextsService,
    };

    let transcript = new_method_transcript_log();
    let metadata = InferenceMetadataFixture::with_transcript(transcript.clone());
    let runs = InferenceRunsFixture::with_transcript(transcript.clone());
    metadata.list(Request::new(acyclic_inference::wire::ListModelsRequest::default())).await
        .map_err(|error| format!("Inference Models/List: {error}"))?;
    ContextsService::create(&metadata, Request::new(acyclic_inference::wire::CreateContextRequest::default())).await
        .map_err(|error| format!("Inference Contexts/Create: {error}"))?;
    ContextsService::inspect(&metadata, Request::new(acyclic_inference::wire::InspectContextRequest::default())).await
        .map_err(|error| format!("Inference Contexts/Inspect: {error}"))?;
    metadata.mutate(Request::new(acyclic_inference::wire::MutateContextRequest::default())).await
        .map_err(|error| format!("Inference Contexts/Mutate: {error}"))?;
    metadata.retain(Request::new(acyclic_inference::wire::RetainWarmRequest::default())).await
        .map_err(|error| format!("Inference WarmContexts/Retain: {error}"))?;
    WarmContextsService::inspect(&metadata, Request::new(acyclic_inference::wire::InspectWarmRequest::default())).await
        .map_err(|error| format!("Inference WarmContexts/Inspect: {error}"))?;
    metadata.renew(Request::new(acyclic_inference::wire::RenewWarmRequest::default())).await
        .map_err(|error| format!("Inference WarmContexts/Renew: {error}"))?;
    metadata.release(Request::new(acyclic_inference::wire::ReleaseWarmRequest::default())).await
        .map_err(|error| format!("Inference WarmContexts/Release: {error}"))?;
    runs.generate(Request::new(acyclic_inference::wire::GenerateRunRequest::default())).await
        .map_err(|error| format!("Inference Runs/Generate: {error}"))?;
    runs.inspect(Request::new(acyclic_inference::wire::InspectRunRequest::default())).await
        .map_err(|error| format!("Inference Runs/Inspect: {error}"))?;
    let mut watch = runs.watch(Request::new(acyclic_inference::wire::WatchRunRequest::default())).await
        .map_err(|error| format!("Inference Runs/Watch: {error}"))?.into_inner();
    while let Some(event) = watch.next().await {
        event.map_err(|error| format!("Inference Runs/Watch frame: {error}"))?;
    }
    runs.cancel(Request::new(acyclic_inference::wire::InspectRunRequest::default())).await
        .map_err(|error| format!("Inference Runs/Cancel: {error}"))?;
    EvaluationsService::create(&metadata, Request::new(acyclic_inference::wire::CreateEvaluationRequest::default())).await
        .map_err(|error| format!("Inference Evaluations/Create: {error}"))?;
    EvaluationsService::inspect(&metadata, Request::new(acyclic_inference::wire::InspectEvaluationRequest::default())).await
        .map_err(|error| format!("Inference Evaluations/Inspect: {error}"))?;

    let methods = transcript.lock().map_err(|_| "Inference fixture transcript mutex poisoned".to_owned())?
        .iter().map(|entry| json!({
            "rpc": entry.rpc,
            "requestBytes": entry.request_bytes,
            "requestBase64": entry.request_base64,
            "requestSha256": entry.request_sha256,
            "responseBytes": entry.response_bytes,
            "responseBase64": entry.response_base64,
            "responseSha256": entry.response_sha256,
            "responseFrames": entry.response_frames.iter().map(|frame| json!({
                "bytesBase64": frame.response_base64,
                "sha256": frame.response_sha256,
            })).collect::<Vec<_>>(),
        })).collect::<Vec<_>>();
    let mut records = inference_records_from_transcript(&json!({
        "schema": "acyclic.sdk.inference-runs-rsa-fixture-transcript.v1",
        "complete": true,
        "methods": methods,
    }))?;
    // The fixture transcript stores a concatenated protobuf stream for Watch.
    // Re-express its frames from the same Rust-owned scenario bytes so the
    // manifest preserves stream framing and remains byte-identical between
    // processes. The service was still invoked above; this only makes the
    // frame boundary explicit in the cross-language artifact.
    if let Some(record) = records.iter_mut().find(|record| {
        record.rpc == "inference.customer.v1.RunsService/Watch"
    }) {
        let fixture = crate::inference_scenarios::fixture();
        record.response_type = Some("inference.customer.v1.RunEvent".to_owned());
        record.response_frames = fixture.events.into_iter().enumerate().map(|(sequence, bytes)| {
            ResponseFrameRecord {
                sequence,
                response_type: "inference.customer.v1.RunEvent".to_owned(),
                response_base64: base64(&bytes),
                response_sha256: format!("sha256:{}", hex(&Sha256::digest(&bytes))),
            }
        }).collect();
        if let Some(first) = record.response_frames.first() {
            record.response_base64 = Some(first.response_base64.clone());
            record.response_sha256 = Some(first.response_sha256.clone());
        }
    }
    Ok(records)
}

fn machine_observation_record(
    observation: crate::fixtures::machines::MachinesRpcObservation,
) -> TypedRequestRecord {
    let mut record = record_from_bytes(
        "machines", observation.rpc, observation.request_type,
        &observation.request_bytes, "rust-fixture-executed",
    );
    let response_type = descriptor_response_type(&record.rpc)
        .ok()
        .flatten()
        .unwrap_or_else(|| observation.response_type.to_owned());
    record.response_type = Some(response_type.clone());
    record.response_frames = observation.response_frames.into_iter().enumerate()
        .map(|(sequence, bytes)| ResponseFrameRecord {
            sequence,
            response_type: response_type.clone(),
            response_base64: base64(&bytes),
            response_sha256: format!("sha256:{}", hex(&Sha256::digest(&bytes))),
        }).collect();
    if let Some(first) = record.response_frames.first() {
        record.response_base64 = Some(first.response_base64.clone());
        record.response_sha256 = Some(first.response_sha256.clone());
    }
    record
}

fn inference_records_from_transcript(
    document: &Value,
) -> Result<Vec<TypedRequestRecord>, String> {
    if document.get("schema").and_then(Value::as_str)
        != Some("acyclic.sdk.inference-runs-rsa-fixture-transcript.v1")
    {
        return Err("Inference transcript schema is not Rust-owned".to_owned());
    }
    if document.get("complete").and_then(Value::as_bool) != Some(true) {
        return Err("Inference transcript is incomplete".to_owned());
    }
    let methods = document
        .get("methods")
        .and_then(Value::as_array)
        .ok_or("Inference transcript methods are missing")?;
    if methods.len() != crate::tls_fixture::INFERENCE_RUNS_RPC_METHODS.len() {
        return Err(format!(
            "Inference transcript contains {}; expected {} methods",
            methods.len(),
            crate::tls_fixture::INFERENCE_RUNS_RPC_METHODS.len()
        ));
    }
    let descriptor_records = records()?
        .into_iter()
        .filter(|record| record.family == "inference")
        .map(|record| (record.rpc.clone(), record))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut output = Vec::with_capacity(methods.len());
    let pool = DescriptorPool::decode(BindingFamily::Inference.model_descriptor().as_slice())
        .map_err(|error| format!("decode Inference descriptor set: {error}"))?;
    for method in methods {
        let object = method
            .as_object()
            .ok_or("Inference transcript method is not an object")?;
        let rpc = object
            .get("rpc")
            .and_then(Value::as_str)
            .ok_or("Inference transcript RPC is missing")?;
        let mut record = descriptor_records
            .get(rpc)
            .cloned()
            .ok_or_else(|| format!("Inference transcript RPC is absent from Rust descriptors: {rpc}"))?;
        let request_base64 = object
            .get("requestBase64")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("{rpc} requestBase64 is missing"))?;
        let request_sha256 = object
            .get("requestSha256")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("{rpc} requestSha256 is missing"))?;
        let response_base64 = object
            .get("responseBase64")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("{rpc} responseBase64 is missing"))?;
        let response_sha256 = object
            .get("responseSha256")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("{rpc} responseSha256 is missing"))?;
        let request_bytes = decode_base64(request_base64)
            .map_err(|error| format!("{rpc} requestBase64: {error}"))?;
        let response_bytes = decode_base64(response_base64)
            .map_err(|error| format!("{rpc} responseBase64: {error}"))?;
        if request_sha256 != format!("sha256:{}", hex(&Sha256::digest(&request_bytes))) {
            return Err(format!("{rpc} request digest does not match bytes"));
        }
        if response_sha256 != format!("sha256:{}", hex(&Sha256::digest(&response_bytes))) {
            return Err(format!("{rpc} response digest does not match bytes"));
        }
        let (service_name, method_name) = rpc
            .rsplit_once('/')
            .ok_or_else(|| format!("Inference transcript RPC has no method separator: {rpc}"))?;
        let method_descriptor = pool.services().find_map(|service| {
            (service.full_name() == service_name)
                .then(|| service.methods().find(|candidate| candidate.name() == method_name))
                .flatten()
        })
            .ok_or_else(|| format!("Inference descriptor method is missing: {rpc}"))?;
        let response_type = method_descriptor.output().full_name().to_owned();
        let response_frames = object
            .get("responseFrames")
            .or_else(|| object.get("response_frames"))
            .and_then(Value::as_array)
            .map(|frames| {
                frames
                    .iter()
                    .enumerate()
                    .map(|(sequence, frame)| {
                        let frame = frame
                            .as_object()
                            .ok_or_else(|| format!("{rpc} response frame is not an object"))?;
                        let frame_base64 = frame
                            .get("bytesBase64")
                            .or_else(|| frame.get("bytes_base64"))
                            .and_then(Value::as_str)
                            .ok_or_else(|| format!("{rpc} response frame bytes are missing"))?;
                        let frame_sha256 = frame
                            .get("sha256")
                            .and_then(Value::as_str)
                            .ok_or_else(|| format!("{rpc} response frame digest is missing"))?;
                        let frame_bytes = decode_base64(frame_base64)
                            .map_err(|error| format!("{rpc} response frame {sequence}: {error}"))?;
                        let expected_sha256 = format!("sha256:{}", hex(&Sha256::digest(&frame_bytes)));
                        if frame_sha256 != expected_sha256 {
                            return Err(format!(
                                "{rpc} response frame {sequence} digest does not match bytes"
                            ));
                        }
                        Ok(ResponseFrameRecord {
                            sequence,
                            response_type: response_type.clone(),
                            response_base64: frame_base64.to_owned(),
                            response_sha256: frame_sha256.to_owned(),
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()
            })
            .transpose()?
            .unwrap_or_else(|| vec![ResponseFrameRecord {
                sequence: 0,
                response_type: response_type.clone(),
                response_base64: response_base64.to_owned(),
                response_sha256: response_sha256.to_owned(),
            }]);
        if response_frames.is_empty() {
            return Err(format!("{rpc} response frames are empty"));
        }
        if response_frames[0].response_base64 != response_base64
            || response_frames[0].response_sha256 != response_sha256
        {
            return Err(format!("{rpc} top-level response does not match frame 0"));
        }
        record.request_base64 = request_base64.to_owned();
        record.request_sha256 = request_sha256.to_owned();
        record.response_type = Some(response_type);
        record.response_base64 = Some(response_base64.to_owned());
        record.response_sha256 = Some(response_sha256.to_owned());
        record.response_frames = response_frames;
        record.expected_status = "rust-fixture-executed";
        record.expected_outcome = if record.response_frames.len() > 1 {
            ExpectedOutcome::stream_terminal("eof", None)
        } else {
            ExpectedOutcome::success()
        };
        output.push(record);
    }
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
        code_sha256: vec![7; 32],
        home_region: "eu-west".into(),
        bindings: Vec::new(),
        limits: Some(actors_wire::ActorLimits {
            handler_timeout_millis: 1_000,
            memory_bytes: 1_024,
            checkpoint_bytes: 4_096,
        }),
        subscriptions: vec![actors_wire::SubscriptionSpec {
            subscription_id: "events".into(),
            stream_path: "/events".into(),
            start: Some(actors_wire::SubscriptionStart {
                start: Some(actors_wire::subscription_start::Start::CurrentHead(true)),
            }),
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
        code_sha256: vec![9; 32],
        bindings: Vec::new(),
        limits: Some(actors_wire::ActorLimits {
            handler_timeout_millis: 2_000,
            memory_bytes: 2_048,
            checkpoint_bytes: 8_192,
        }),
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
        subscription: Some(actors_wire::SubscriptionSpec {
            subscription_id: "audit".into(),
            stream_path: "/audit".into(),
            // The public Actors contract requires an explicit cursor policy
            // for newly-created subscriptions.  Keep this request valid in
            // the Rust producer and every generated consumer.
            start: Some(actors_wire::SubscriptionStart {
                start: Some(actors_wire::subscription_start::Start::CurrentHead(true)),
            }),
            placement_anchor: false,
        }),
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
    let submit = workers_wire::SubmitJobRequest {
        target: Some(workers_wire::JobTarget {
            target: Some(workers_wire::job_target::Target::DeploymentAlias(
                deployment.alias.clone(),
            )),
        }),
        input: Some(workers_wire::Payload {
            source: Some(workers_wire::payload::Source::InlineBytes(b"job-input".to_vec())),
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
        idempotency_key: "job-1".into(),
    };
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

    let service = Service::new(Arc::new(acyclic_stream::MemoryStream::new_with_clock(
        acyclic_stream::MemoryLimits::default(),
        crate::fixtures::fixture_clock::stream_clock(),
    )));
    let path = "typed/stream".to_owned();
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
    let _append_commit_id = match append_response.outcome.as_ref() {
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
        destination: "typed/stream-fork".into(),
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
    let follow_outcome =
        match tokio::time::timeout(std::time::Duration::from_millis(10), follow_stream.next())
            .await
        {
            Ok(Some(Ok(_))) => ExpectedOutcome::stream_terminal("frame", None),
            Ok(Some(Err(status))) => ExpectedOutcome::error(
                Some(grpc_code_name(status.code())),
                Some(status.message()),
                Some("error"),
            ),
            Ok(None) => ExpectedOutcome::stream_terminal("eof", None),
            Err(_) => ExpectedOutcome::stream_terminal("timeout", Some("DEADLINE_EXCEEDED")),
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
        conditions: vec![wire::CommitCondition {
            condition: Some(wire::commit_condition::Condition::Absent(
                wire::AbsentCondition {
                    path: "typed/commit".into(),
                },
            )),
        }],
        mutations: vec![wire::CommitMutation {
            mutation: Some(wire::commit_mutation::Mutation::Append(
                wire::AppendMutation {
                    path: "typed/commit".into(),
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

    let commit_id = match commit_response.outcome.as_ref() {
        Some(wire::commit_response::Outcome::Committed(envelope)) => envelope.commit_id.clone(),
        _ => Bytes::new(),
    };
    let read_commit = wire::ReadCommitRequest {
        commit_id: commit_id.clone(),
    };
    let read_commit_response = service
        .read_commit(Request::new(read_commit.clone()))
        .await
        .map_err(|e| format!("Stream ReadCommit: {e}"))?
        .into_inner();

    let mut follow_record = record_from_bytes(
        "stream",
        "acyclic.stream.v2.StreamService/Follow",
        "acyclic.stream.v2.FollowRequest",
        &follow.encode_to_vec(),
        "rust-fixture-executed",
    );
    follow_record.expected_outcome = follow_outcome;

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
        follow_record,
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
    let execution = actual_execution_records().await?;
    validate_actual_records(&execution)?;
    let observations = unique_inventory(&execution)?;
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
        "execution_plan_count": execution.len(),
        "execution_plan": execution_plan_json(&execution),
    }))
}

fn unique_inventory(
    execution: &[TypedRequestRecord],
) -> Result<Vec<TypedRequestRecord>, String> {
    let mut output = Vec::with_capacity(106);
    let mut identities = std::collections::BTreeSet::new();
    for record in execution {
        if record.rpc.ends_with("/after-completion") {
            continue;
        }
        if identities.insert(record.rpc.as_str()) {
            output.push(record.clone());
        }
    }
    ensure_unique(&output)?;
    Ok(output)
}

fn execution_plan_json(execution: &[TypedRequestRecord]) -> Vec<Value> {
    let mut occurrences = std::collections::BTreeMap::<String, usize>::new();
    execution
        .iter()
        .enumerate()
        .map(|(step, record)| {
            let occurrence = occurrences.entry(record.rpc.clone()).or_insert(0);
            let current_occurrence = *occurrence;
            *occurrence += 1;
            let mut value = record_json(record);
            if let Value::Object(object) = &mut value {
                object.insert("execution_step".to_owned(), json!(step));
                object.insert("rpc_occurrence".to_owned(), json!(current_occurrence));
            }
            value
        })
        .collect()
}

fn observation_record(observation: &Value) -> Result<TypedRequestRecord, String> {
    let object = observation
        .as_object()
        .ok_or_else(|| "fixture observation is not an object".to_owned())?;
    let family = object
        .get("family")
        .and_then(Value::as_str)
        .ok_or_else(|| "fixture observation family is missing".to_owned())?;
    let raw_rpc = object
        .get("rpc")
        .or_else(|| object.get("operation"))
        .and_then(Value::as_str)
        .ok_or_else(|| "fixture observation rpc is missing".to_owned())?;
    let (rpc, scenario_id) = canonical_rpc(raw_rpc);
    let request = object
        .get("request")
        .and_then(Value::as_object)
            .ok_or_else(|| format!("{raw_rpc} request observation is missing"))?;
    let request_type = request
        .get("type")
        .and_then(Value::as_str)
            .ok_or_else(|| format!("{raw_rpc} request type is missing"))?;
    let request_base64 = request
        .get("bytes_base64")
        .and_then(Value::as_str)
            .ok_or_else(|| format!("{raw_rpc} request bytes_base64 is missing"))?;
    let request_sha256 = request
        .get("sha256")
        .and_then(Value::as_str)
            .ok_or_else(|| format!("{raw_rpc} request sha256 is missing"))?;
    let request_frames = object
        .get("request_frames")
        .and_then(Value::as_array)
        .map(|frames| {
            frames
                .iter()
                .enumerate()
                .map(|(sequence, frame)| {
                    let frame = frame
                        .as_object()
                        .ok_or_else(|| format!("{rpc} request frame is not an object"))?;
                    Ok(RequestFrameRecord {
                        sequence: frame
                            .get("sequence")
                            .and_then(Value::as_u64)
                            .map_or(sequence, |value| value as usize),
                        request_type: frame
                            .get("type")
                            .and_then(Value::as_str)
                            .ok_or_else(|| format!("{rpc} request frame type is missing"))?
                            .to_owned(),
                        request_base64: frame
                            .get("bytes_base64")
                            .and_then(Value::as_str)
                            .ok_or_else(|| format!("{rpc} request frame bytes are missing"))?
                            .to_owned(),
                        request_sha256: frame
                            .get("sha256")
                            .and_then(Value::as_str)
                            .ok_or_else(|| format!("{rpc} request frame digest is missing"))?
                            .to_owned(),
                    })
                })
                .collect::<Result<Vec<_>, String>>()
        })
        .transpose()?
        .unwrap_or_default();
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
    let observed_response_type = response_object
        .and_then(|response| response.get("type"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let response_type = descriptor_response_type(&rpc)?.or(observed_response_type);
    let response_base64 = response_object
        .and_then(|response| response.get("bytes_base64"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let response_sha256 = response_object
        .and_then(|response| response.get("sha256"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let response_frames = if let Some(frames) = object
        .get("response_frames")
        .and_then(Value::as_array)
    {
        frames
            .iter()
            .enumerate()
            .map(|(sequence, frame)| {
                let frame = frame
                    .as_object()
                    .ok_or_else(|| format!("{rpc} response frame is not an object"))?;
                let frame_response_type = response_type.clone().or_else(|| {
                    frame
                        .get("type")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                }).ok_or_else(|| format!("{rpc} response frame type is missing"))?;
                Ok(ResponseFrameRecord {
                    sequence: frame
                        .get("sequence")
                        .and_then(Value::as_u64)
                        .map_or(sequence, |value| value as usize),
                    response_type: frame_response_type,
                    response_base64: frame
                        .get("bytes_base64")
                        .and_then(Value::as_str)
                        .ok_or_else(|| format!("{rpc} response frame bytes are missing"))?
                        .to_owned(),
                    response_sha256: frame
                        .get("sha256")
                        .and_then(Value::as_str)
                        .ok_or_else(|| format!("{rpc} response frame digest is missing"))?
                        .to_owned(),
                })
            })
            .collect::<Result<Vec<_>, String>>()?
    } else {
        match (&response_type, &response_base64, &response_sha256) {
            (Some(response_type), Some(response_base64), Some(response_sha256)) => {
                vec![ResponseFrameRecord {
                    sequence: 0,
                    response_type: response_type.clone(),
                    response_base64: response_base64.clone(),
                    response_sha256: response_sha256.clone(),
                }]
            }
            _ => Vec::new(),
        }
    };
    Ok(TypedRequestRecord {
            family: family.to_owned(),
            rpc: rpc.to_owned(),
            scenario_id,
        request_type: request_type.to_owned(),
        request_base64: request_base64.to_owned(),
        request_sha256: request_sha256.to_owned(),
        request_frames,
        expected_wire: format!("canonical-protobuf-v3;sha256={request_sha256}"),
        response_type,
        response_base64,
        response_sha256,
        response_frames,
        expected_status,
        expected_outcome: observation_outcome(object),
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
        scenario_id: None,
        request_type: request_type.to_owned(),
        request_base64: base64(bytes),
        request_sha256: request_sha256.clone(),
        request_frames: Vec::new(),
        expected_wire: format!("canonical-protobuf-v3;sha256={request_sha256}"),
        response_type: descriptor_response_type(rpc).ok().flatten(),
        response_base64: None,
        response_sha256: None,
        response_frames: Vec::new(),
        expected_status: status,
        expected_outcome: outcome_for_status(status),
    }
}

fn descriptor_response_type(rpc: &str) -> Result<Option<String>, String> {
    let descriptors: Vec<Vec<u8>> = vec![
        acyclic_actors::FILE_DESCRIPTOR_SET.to_vec(),
        acyclic_workers::FILE_DESCRIPTOR_SET.to_vec(),
        acyclic_objects::v2::FILE_DESCRIPTOR_SET.to_vec(),
        acyclic_stream::FILE_DESCRIPTOR_SET.to_vec(),
        acyclic_fs::FILE_DESCRIPTOR_SET.to_vec(),
        acyclic_harness::FILE_DESCRIPTOR_SET.to_vec(),
        BindingFamily::Inference.model_descriptor(),
        acyclic_machines::FILE_DESCRIPTOR_SET.to_vec(),
    ];
    for descriptor in &descriptors {
        let pool = DescriptorPool::decode(descriptor.as_slice())
            .map_err(|error| format!("decode descriptor set for {rpc}: {error}"))?;
        for service in pool.services() {
            for method in service.methods() {
                if format!("{}/{}", service.full_name(), method.name()) == rpc {
                    return Ok(Some(method.output().full_name().to_owned()));
                }
            }
        }
    }
    Ok(None)
}

fn outcome_for_status(status: &str) -> ExpectedOutcome {
    match status {
        "descriptor-only" => ExpectedOutcome::descriptor(),
        "observed-status" => ExpectedOutcome::error(None, None, Some("error")),
        _ => ExpectedOutcome::success(),
    }
}

fn grpc_code_name(code: Code) -> &'static str {
    match code {
        Code::Ok => "OK",
        Code::Cancelled => "CANCELLED",
        Code::Unknown => "UNKNOWN",
        Code::InvalidArgument => "INVALID_ARGUMENT",
        Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        Code::NotFound => "NOT_FOUND",
        Code::AlreadyExists => "ALREADY_EXISTS",
        Code::PermissionDenied => "PERMISSION_DENIED",
        Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        Code::FailedPrecondition => "FAILED_PRECONDITION",
        Code::Aborted => "ABORTED",
        Code::OutOfRange => "OUT_OF_RANGE",
        Code::Unimplemented => "UNIMPLEMENTED",
        Code::Internal => "INTERNAL",
        Code::Unavailable => "UNAVAILABLE",
        Code::DataLoss => "DATA_LOSS",
        Code::Unauthenticated => "UNAUTHENTICATED",
    }
}

fn with_response<M: Message>(
    mut record: TypedRequestRecord,
    response_type: &str,
    response: &M,
) -> TypedRequestRecord {
    let bytes = response.encode_to_vec();
    let resolved_response_type = descriptor_response_type(&record.rpc)
        .ok()
        .flatten()
        .unwrap_or_else(|| response_type.to_owned());
    record.response_type = Some(resolved_response_type.clone());
    record.response_base64 = Some(base64(&bytes));
    record.response_sha256 = Some(format!("sha256:{}", hex(&Sha256::digest(&bytes))));
    record.response_frames = vec![ResponseFrameRecord {
        sequence: 0,
        response_type: resolved_response_type,
        response_base64: base64(&bytes),
        response_sha256: format!("sha256:{}", hex(&Sha256::digest(&bytes))),
    }];
    record.expected_outcome = ExpectedOutcome::success();
    record
}

fn validate_actual_records(records: &[TypedRequestRecord]) -> Result<(), String> {
    for record in records {
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
        match record.expected_outcome.kind.as_str() {
            "success" => {
                if record.expected_outcome.grpc_code.as_deref() != Some("OK")
                    || record.expected_outcome.detail.is_some()
                    || record.expected_outcome.terminal.is_some()
                {
                    return Err(format!("{} has an invalid success outcome", record.rpc));
                }
            }
            "stream" => {
                if record.expected_outcome.grpc_code.is_none()
                    || record.expected_outcome.terminal.is_none()
                {
                    return Err(format!("{} has an incomplete stream outcome", record.rpc));
                }
            }
            "error" => {
                if record.expected_outcome.grpc_code.is_none()
                    || record.expected_outcome.terminal.as_deref() != Some("error")
                {
                    return Err(format!("{} has an incomplete error outcome", record.rpc));
                }
            }
            other => {
                return Err(format!("{} has an unrecognized outcome {other}", record.rpc));
            }
        }
        for (index, frame) in record.request_frames.iter().enumerate() {
            if frame.sequence != index {
                return Err(format!(
                    "{} request frame order is invalid at index {} (sequence {})",
                    record.rpc, index, frame.sequence
                ));
            }
            let bytes = decode_base64(&frame.request_base64)
                .map_err(|error| format!("{} request frame {index}: {error}", record.rpc))?;
            let digest = format!("sha256:{}", hex(&Sha256::digest(&bytes)));
            if frame.request_sha256 != digest || frame.request_type.is_empty() {
                return Err(format!(
                    "{} request frame {index} has invalid type or digest",
                    record.rpc
                ));
            }
        }
        if let Some(first) = record.request_frames.first() {
            if first.sequence != 0
                || first.request_base64 != record.request_base64
                || first.request_sha256 != record.request_sha256
            {
                return Err(format!(
                    "{} request frame 0 does not match the top-level request",
                    record.rpc
                ));
            }
        }
        let Some(response_type) = record.response_type.as_deref() else {
            // A streaming response has no single top-level protobuf value;
            // validate its ordered, typed frames directly.
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
                if frame.response_sha256 != digest || frame.response_type.is_empty() {
                    return Err(format!(
                        "{} response frame {index} has invalid type or digest",
                        record.rpc
                    ));
                }
            }
            continue;
        };
        let Some(response_base64) = record.response_base64.as_deref() else {
            if record.expected_outcome.kind == "error" && record.response_frames.is_empty() {
                continue;
            }
            return Err(format!("{} has a response type but no response bytes", record.rpc));
        };
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
    let resolved_response_type = descriptor_response_type(&record.rpc)
        .ok()
        .flatten()
        .unwrap_or_else(|| response_type.to_owned());
    record.response_type = Some(resolved_response_type.clone());
    record.response_base64 = frames.first().map(|bytes| base64(bytes));
    record.response_sha256 = frames
        .first()
        .map(|bytes| format!("sha256:{}", hex(&Sha256::digest(bytes))));
    record.response_frames = frames
        .into_iter()
        .enumerate().map(|(sequence, bytes)| ResponseFrameRecord {
            sequence,
            response_type: resolved_response_type.clone(),
            response_base64: base64(&bytes),
            response_sha256: format!("sha256:{}", hex(&Sha256::digest(&bytes))),
        })
        .collect();
    record.expected_outcome = ExpectedOutcome::stream_terminal("eof", None);
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
                    rpc: format!("{}/{}", service.full_name(), method.name()),
                    scenario_id: None,
                    request_type: input.full_name().to_owned(),
                    request_base64: base64(&bytes),
                    request_sha256: request_sha256.clone(),
                    request_frames: Vec::new(),
                    expected_wire: format!("{EXPECTED_WIRE};sha256={request_sha256}"),
                    response_type: None,
                    response_base64: None,
                    response_sha256: None,
            response_frames: Vec::new(),
                    expected_status: EXPECTED_STATUS,
                    expected_outcome: ExpectedOutcome::descriptor(),
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
        "scenario_id": record.scenario_id,
        "request_type": record.request_type,
        "request_base64": record.request_base64,
        "request_sha256": record.request_sha256,
        "request_frames": record.request_frames.iter().map(|frame| json!({
            "sequence": frame.sequence,
            "type": frame.request_type,
            "bytes_base64": frame.request_base64,
            "sha256": frame.request_sha256,
        })).collect::<Vec<_>>(),
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
        "expected_outcome": {
            "kind": record.expected_outcome.kind,
            "grpc_code": record.expected_outcome.grpc_code,
            "detail": record.expected_outcome.detail,
            "terminal": record.expected_outcome.terminal,
        },
    })
}

pub fn haskell_replay_source(manifest: &Value) -> Result<String, String> {
    let steps = manifest
        .get("execution_plan")
        .and_then(Value::as_array)
        .or_else(|| manifest.get("records").and_then(Value::as_array))
        .ok_or("Rust manifest has no execution plan or records")?;
    if steps.is_empty() {
        return Err("Rust manifest execution plan is empty".to_owned());
    }

    let mut methods = Vec::with_capacity(steps.len());
    for step in steps {
        let rpc = step
            .get("rpc")
            .and_then(Value::as_str)
            .ok_or("Rust manifest step has no rpc")?;
        methods.push((rpc.to_owned(), descriptor_method(rpc)?));
    }

    let mut imports = std::collections::BTreeMap::<String, String>::new();
    let mut services = std::collections::BTreeSet::<String>::new();
    for (_, method) in &methods {
        let module = haskell_module_for_service(method.0.as_str())?;
        let alias = haskell_module_alias(&module);
        imports.insert(module, alias.to_owned());
        services.insert(format!("{alias}.{}", haskell_last(method.0.as_str())));
        for message in [method.1.as_str(), method.2.as_str()] {
            if message.starts_with("acyclic.protocol.v1.") {
                imports.insert(
                    "Proto.Protocol.V1.Protocol".to_owned(),
                    "Protocol".to_owned(),
                );
            }
        }
    }

    let mut lines = vec![
        "{-# LANGUAGE DataKinds #-}".to_owned(),
        "{-# LANGUAGE ImportQualifiedPost #-}".to_owned(),
        "{-# LANGUAGE OverloadedStrings #-}".to_owned(),
        "{-# LANGUAGE ScopedTypeVariables #-}".to_owned(),
        "{-# LANGUAGE TypeApplications #-}".to_owned(),
        "{-# LANGUAGE TypeFamilies #-}".to_owned(),
        "{-# LANGUAGE FlexibleInstances #-}".to_owned(),
        "{-# LANGUAGE MultiParamTypeClasses #-}".to_owned(),
        "module Main where".to_owned(),
        String::new(),
        "-- Generated by the Rust typed-request-manifest authority; do not edit.".to_owned(),
        format!("-- Rust execution-plan steps: {}", steps.len()),
        "import qualified Data.ByteString as BS".to_owned(),
        "import Control.Exception (SomeException, try)".to_owned(),
        "import Control.Monad (when)".to_owned(),
        "import Data.ProtoLens.Encoding (decodeMessage, encodeMessage)".to_owned(),
        "import Network.GRPC.Client qualified as Client".to_owned(),
        "import Network.GRPC.Client.StreamType.IO qualified as Typed".to_owned(),
        "import Network.GRPC.Common".to_owned(),
        "import Network.GRPC.Common.Protobuf (Proto(..), Protobuf)".to_owned(),
    ];
    for (module, alias) in &imports {
        lines.push(format!("import qualified {module} as {alias}"));
    }
    lines.push(String::new());
    for service in &services {
        lines.push(format!("type instance RequestMetadata (Protobuf {service} meth) = NoMetadata"));
        lines.push(format!("type instance ResponseInitialMetadata (Protobuf {service} meth) = NoMetadata"));
        lines.push(format!("type instance ResponseTrailingMetadata (Protobuf {service} meth) = NoMetadata"));
    }
    lines.extend([
        String::new(),
        "collect :: Monad m => m (NextElem a) -> [a] -> m [a]".to_owned(),
        "collect next acc = do".to_owned(),
        "  item <- next".to_owned(),
        "  case item of".to_owned(),
        "    NoNextElem -> pure (reverse acc)".to_owned(),
        "    NextElem value -> collect next (value : acc)".to_owned(),
        String::new(),
        "requireCall :: String -> IO a -> IO a".to_owned(),
        "requireCall name action = do".to_owned(),
        "  result <- try action".to_owned(),
        "  case result of".to_owned(),
        "    Left (errorValue :: SomeException) -> fail (name ++ \" unexpected RPC failure: \" ++ show errorValue)".to_owned(),
        "    Right value -> pure value".to_owned(),
        String::new(),
        "expect :: String -> BS.ByteString -> BS.ByteString -> IO ()".to_owned(),
        "expect name wanted actual = when (wanted /= actual) (fail (name ++ \" response bytes differ\"))".to_owned(),
        String::new(),
        "expectFrames :: String -> [BS.ByteString] -> [BS.ByteString] -> IO ()".to_owned(),
        "expectFrames name wanted actual = when (wanted /= actual) (fail (name ++ \" response frames differ\"))".to_owned(),
        String::new(),
        "reconnectWait :: Int -> IO ()".to_owned(),
        "reconnectWait _ = pure ()".to_owned(),
    ]);
    for (index, (_, method)) in methods.iter().enumerate() {
        let module = haskell_module_for_service(method.0.as_str())?;
        let alias = haskell_module_alias(&module);
        let service = format!("{alias}.{}", haskell_last(method.0.as_str()));
        lines.push(format!("type Rpc{index} = Protobuf {service} \"{}\"", haskell_lower_first(haskell_last(method.3.as_str()))));
    }
    lines.extend([
        String::new(),
        "main :: IO ()".to_owned(),
        "main = do".to_owned(),
        "  let address = Client.Address \"127.0.0.1\" 50055 Nothing".to_owned(),
        "      server = Client.ServerInsecure address".to_owned(),
        "      params = def { Client.connReconnectPolicy = Client.exponentialBackoff reconnectWait 1.5 (0.05, 0.1) 3 }".to_owned(),
        "  Client.withConnection params server $ \\conn -> do".to_owned(),
    ]);

    for (index, step) in steps.iter().enumerate() {
        let rpc = step.get("rpc").and_then(Value::as_str).ok_or("Rust manifest step has no rpc")?;
        let (_, method) = &methods[index];
        let request_type = step.get("request_type").and_then(Value::as_str).unwrap_or(method.1.as_str());
        let request_alias = if request_type.starts_with("acyclic.protocol.v1.") { "Protocol" } else { haskell_module_alias(&haskell_module_for_service(method.0.as_str())?) };
        let request_name = haskell_last(request_type);
        let bytes = step.get("request_base64").and_then(Value::as_str).unwrap_or("");
        lines.push(format!("    putStrLn \"CALL:{rpc}\""));
        lines.push(format!("    request{index} <- either (fail . (\"{rpc} request decode: \" ++)) pure (decodeMessage {} :: Either String {request_alias}.{request_name})", haskell_bytes(bytes)?));

        let is_client = method.5;
        let is_server = method.6;
        let response_type = step.get("response_type").and_then(Value::as_str).unwrap_or(method.2.as_str());
        let response_alias = if response_type.starts_with("acyclic.protocol.v1.") { "Protocol" } else { haskell_module_alias(&haskell_module_for_service(method.0.as_str())?) };
        let expected_frames = step.get("response_frames").and_then(Value::as_array).cloned().unwrap_or_default();
        if is_client {
            let frames = step.get("request_frames").and_then(Value::as_array).cloned().unwrap_or_default();
            let mut sends = Vec::new();
            if frames.is_empty() {
                sends.push(format!("send (NextElem (Proto request{index}))"));
            } else {
                for (frame_index, frame) in frames.iter().enumerate() {
                    let frame_bytes = frame.get("bytes_base64").and_then(Value::as_str).unwrap_or("");
                    let frame_type = frame.get("type").and_then(Value::as_str).unwrap_or(request_type);
                    if frame_index == 0 {
                        sends.push(format!("send (NextElem (Proto request{index}))"));
                    } else {
                        let frame_name = format!("request{index}f{frame_index}");
                        lines.push(format!("    {frame_name} <- either (fail . (\"{rpc} frame decode: \" ++)) pure (decodeMessage {} :: Either String {request_alias}.{})", haskell_bytes(frame_bytes)?, haskell_last(frame_type)));
                        sends.push(format!("send (NextElem (Proto {frame_name}))"));
                    }
                }
            }
            sends.push("send NoNextElem".to_owned());
            lines.push(format!("    response{index} <- requireCall \"{rpc}\" (Typed.clientStreaming conn (Client.rpc @Rpc{index}) (\\send -> {} >> pure ()))", sends.join(" >> "))); 
            let expected = expected_frames.first().and_then(|frame| frame.get("response_base64")).and_then(Value::as_str).or_else(|| step.get("response_base64").and_then(Value::as_str)).unwrap_or("");
            lines.push(format!("    let actual{index} = case response{index} of (Proto value, _) -> encodeMessage value"));
            lines.push(format!("    expect \"{rpc}\" {} actual{index}", haskell_bytes(expected)?));
        } else if is_server {
            let expected = expected_frames.iter().map(|frame| frame.get("response_base64").and_then(Value::as_str).unwrap_or("")).map(haskell_bytes).collect::<Result<Vec<_>, _>>()?;
            lines.push(format!("    response{index} <- requireCall \"{rpc}\" (Typed.serverStreaming conn (Client.rpc @Rpc{index}) (Proto request{index}) (\\next -> collect next []))"));
            lines.push(format!("    let actual{index} = map (\\(Proto value) -> encodeMessage value) response{index}"));
            lines.push(format!("    expectFrames \"{rpc}\" [{}] actual{index}", expected.join(", ")));
        } else {
            lines.push(format!("    response{index} <- requireCall \"{rpc}\" (Typed.nonStreaming conn (Client.rpc @Rpc{index}) (Proto request{index}))"));
            lines.push(format!("    let actual{index} = case response{index} of Proto value -> encodeMessage value"));
            let expected = expected_frames.first().and_then(|frame| frame.get("response_base64")).and_then(Value::as_str).or_else(|| step.get("response_base64").and_then(Value::as_str)).unwrap_or("");
            lines.push(format!("    expect \"{rpc}\" {} actual{index}", haskell_bytes(expected)?));
        }
    }
    lines.push(format!("    putStrLn \"PASS:rust-canonical-runtime={}\"", steps.len()));
    Ok(lines.join("\n") + "\n")
}

// (module, request, response, service method, lower-case method, client-streaming, server-streaming)
type HaskellMethod = (String, String, String, String, String, bool, bool);

fn descriptor_method(rpc: &str) -> Result<HaskellMethod, String> {
    let (service_name, method_name) = rpc.rsplit_once('/').ok_or_else(|| format!("RPC has no method separator: {rpc}"))?;
    for (_, descriptor_bytes) in descriptor_sets() {
        let pool = DescriptorPool::decode(descriptor_bytes.as_slice()).map_err(|error| format!("decode descriptor set: {error}"))?;
        for service in pool.services() {
            if service.full_name() == service_name {
                if let Some(method) = service.methods().find(|candidate| candidate.name() == method_name) {
                    return Ok((service_name.to_owned(), method.input().full_name().to_owned(), method.output().full_name().to_owned(), method_name.to_owned(), haskell_lower_first(method_name), method.is_client_streaming(), method.is_server_streaming()));
                }
            }
        }
    }
    Err(format!("Rust descriptors do not contain {rpc}"))
}

fn descriptor_sets() -> Vec<(&'static str, Vec<u8>)> {
    vec![
        ("actors", acyclic_actors::FILE_DESCRIPTOR_SET.to_vec()),
        ("workers", acyclic_workers::FILE_DESCRIPTOR_SET.to_vec()),
        ("objects", acyclic_objects::v2::FILE_DESCRIPTOR_SET.to_vec()),
        ("stream", acyclic_stream::FILE_DESCRIPTOR_SET.to_vec()),
        ("filesystem", acyclic_fs::FILE_DESCRIPTOR_SET.to_vec()),
        ("harness", acyclic_harness::FILE_DESCRIPTOR_SET.to_vec()),
        ("inference", BindingFamily::Inference.model_descriptor()),
        ("machines", acyclic_machines::FILE_DESCRIPTOR_SET.to_vec()),
    ]
}

fn haskell_module_for_service(service: &str) -> Result<String, String> {
    let module = if service.starts_with("acyclic.actors.") { "Proto.Actors.V1.Actors" }
        else if service.starts_with("acyclic.filesystem.") { "Proto.Filesystem.V2.Filesystem" }
        else if service.starts_with("acyclic.harness.") { "Proto.Harness.V2.Harness" }
        else if service.starts_with("inference.customer.") { "Proto.Inference.V1.Inference" }
        else if service.starts_with("acyclic.machines.") { "Proto.Machines.V1.Machines" }
        else if service.starts_with("acyclic.objects.") { "Proto.Objects.V2.Objects" }
        else if service.starts_with("acyclic.stream.") { "Proto.Stream.V2.Stream" }
        else if service.starts_with("acyclic.workers.") { "Proto.Workers.V1.Workers" }
        else { return Err(format!("no Rust-owned Haskell module mapping for {service}")); };
    Ok(module.to_owned())
}

fn haskell_module_alias(module: &str) -> &'static str {
    match module {
        "Proto.Actors.V1.Actors" => "Actors",
        "Proto.Filesystem.V2.Filesystem" => "Filesystem",
        "Proto.Harness.V2.Harness" => "Harness",
        "Proto.Inference.V1.Inference" => "Inference",
        "Proto.Machines.V1.Machines" => "Machines",
        "Proto.Objects.V2.Objects" => "ObjectsV2",
        "Proto.Stream.V2.Stream" => "Stream",
        "Proto.Workers.V1.Workers" => "Workers",
        "Proto.Protocol.V1.Protocol" => "Protocol",
        _ => "Generated",
    }
}

fn haskell_last(value: &str) -> &str { value.rsplit('.').next().unwrap_or(value) }

fn haskell_lower_first(value: &str) -> String {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) => first.to_lowercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

fn haskell_bytes(encoded: &str) -> Result<String, String> {
    let bytes = decode_base64(encoded)?;
    Ok(format!("BS.pack [{}]", bytes.iter().map(|byte| byte.to_string()).collect::<Vec<_>>().join(",")))
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
    use super::{
        actual_execution_records, base64, records, unique_inventory, validate_actual_records,
        ExpectedOutcome, ResponseFrameRecord, TypedRequestRecord,
    };

    fn observed_record() -> TypedRequestRecord {
        let encoded = base64(&[0]);
        let digest = "sha256:6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d";
        TypedRequestRecord {
            family: "actors".to_owned(),
            rpc: "acyclic.actors.v1.ActorsService/CreateActor".to_owned(),
            scenario_id: None,
            request_type: "acyclic.actors.v1.CreateActorRequest".to_owned(),
            request_base64: encoded.clone(),
            request_sha256: digest.to_owned(),
            request_frames: Vec::new(),
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
            expected_outcome: ExpectedOutcome::success(),
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

    #[tokio::test]
    async fn actual_collector_is_complete_and_uses_put_object_envelope() {
        let execution = actual_execution_records()
            .await
            .expect("Rust actual fixture execution plan");
        assert!(execution.len() > 106);
        let records = unique_inventory(&execution).expect("unique Rust fixture inventory");
        assert_eq!(records.len(), 106);
        validate_actual_records(&records).expect("actual fixture records are canonical");
        let create_bucket_count = execution
            .iter()
            .filter(|record| record.rpc == "acyclic.objects.v2.BucketsService/CreateBucket")
            .count();
        let create_multipart_count = execution
            .iter()
            .filter(|record| record.rpc == "acyclic.objects.v2.MultipartService/CreateMultipart")
            .count();
        assert_eq!(create_bucket_count, 2);
        assert_eq!(create_multipart_count, 3);
        let put = records
            .iter()
            .find(|record| record.rpc == "acyclic.objects.v2.ObjectsService/PutObject")
            .expect("Objects PutObject record");
        assert_eq!(put.request_type, "acyclic.objects.v2.PutObjectRequest");
        assert_eq!(put.response_frames.len(), 1);
    }

    #[tokio::test]
    async fn ordered_execution_plan_is_replayable_by_protocol_shape() {
        let first = actual_execution_records()
            .await
            .expect("first Rust actual fixture execution plan");
        let second = actual_execution_records()
            .await
            .expect("second Rust actual fixture execution plan");
        let shape = |records: &[TypedRequestRecord]| {
            records
                .iter()
                .map(|record| {
                    format!(
                        "{}|{}|{}|{}|{}|{}|{}|{}|{}",
                        record.family,
                        record.rpc,
                        record.request_type,
                        record.request_frames.len(),
                        record.response_frames.len(),
                        record.expected_status,
                        record.expected_outcome.kind,
                        record.expected_outcome.grpc_code.as_deref().unwrap_or(""),
                        record.expected_outcome.terminal.as_deref().unwrap_or(""),
                    )
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(first.len(), 111);
        assert_eq!(second.len(), 111);
        assert_eq!(shape(&first), shape(&second));
        assert_eq!(
            first
                .iter()
                .filter(|record| record.rpc == "acyclic.objects.v2.BucketsService/CreateBucket")
                .count(),
            2
        );
        assert_eq!(
            first
                .iter()
                .filter(|record| record.rpc == "acyclic.objects.v2.MultipartService/CreateMultipart")
                .count(),
            3
        );
        let negative_steps = first
            .iter()
            .filter(|record| record.scenario_id.is_some())
            .collect::<Vec<_>>();
        assert_eq!(negative_steps.len(), 2);
        assert!(negative_steps.iter().all(|record| {
            record.rpc == "acyclic.objects.v2.ObjectsService/PutObject"
                || record.rpc == "acyclic.objects.v2.MultipartService/UploadPart"
        }));
        assert!(negative_steps.iter().all(|record| {
            record.expected_outcome.kind == "error"
                && record.expected_outcome.grpc_code.as_deref() == Some("INVALID_ARGUMENT")
                && record.expected_outcome.terminal.as_deref() == Some("error")
        }));
    }
}


