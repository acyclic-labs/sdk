// Verify generated SDK transport observations against the Rust typed manifest.
//
// SDK consumers only serialize requests and responses. This binary owns the
// comparison with Rust's ordered plan, expected outcomes, descriptors, and
// wire semantics.

use acyclic_sdk_contract_wire::{compare_family_rpc_message, RpcDirection};
use acyclic_sdk_contract_wire::resolved_rpc_methods;
use base64::Engine;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::{env, fs, path::{Path, PathBuf}};

fn usage() -> ! {
    eprintln!("usage: verify-observations --manifest PATH --observed PATH --output PATH --canonical-manifest PATH");
    std::process::exit(2);
}

fn args() -> (PathBuf, PathBuf, PathBuf, PathBuf) {
    let mut manifest = None;
    let mut observed = None;
    let mut output = None;
    let mut canonical = None;
    let mut it = env::args_os().skip(1);
    while let Some(arg) = it.next() {
        match arg.to_string_lossy().as_ref() {
            "--manifest" => manifest = it.next().map(PathBuf::from),
            "--observed" => observed = it.next().map(PathBuf::from),
            "--output" => output = it.next().map(PathBuf::from),
            "--canonical-manifest" => canonical = it.next().map(PathBuf::from),
            _ => usage(),
        }
    }
    match (manifest, observed, output, canonical) {
        (Some(manifest), Some(observed), Some(output), Some(canonical)) => (manifest, observed, output, canonical),
        _ => usage(),
    }
}

fn object<'a>(value: &'a Value, label: &str) -> Result<&'a Map<String, Value>, String> {
    value.as_object().ok_or_else(|| format!("{label} must be an object"))
}

fn text<'a>(object: &'a Map<String, Value>, key: &str, label: &str) -> Result<&'a str, String> {
    object.get(key).and_then(Value::as_str).ok_or_else(|| format!("{label}.{key} must be a string"))
}

fn bytes(value: &str, label: &str) -> Result<Vec<u8>, String> {
    base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|error| format!("{label} is not valid base64: {error}"))
}

fn digest(value: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(value))
}

fn required_digest(object: &Map<String, Value>, key: &str, label: &str, value: &[u8]) -> Result<(), String> {
    let expected = text(object, key, label)?;
    if expected != digest(value) {
        return Err(format!("{label}.{key} does not match decoded bytes"));
    }
    Ok(())
}

fn family(rpc: &str) -> Result<&str, String> {
    let package = rpc.split('.').next().ok_or_else(|| format!("invalid RPC {rpc}"))?;
    match package {
        "acyclic" => rpc.split('.').nth(1).ok_or_else(|| format!("invalid RPC {rpc}")),
        "actors" | "workers" | "objects" | "stream" | "inference" | "machines" | "filesystem" | "harness" => Ok(package),
        _ => Err(format!("RPC {rpc} is outside the Rust authority")),
    }
}

fn frame_bytes(frame: &Map<String, Value>, label: &str, prefix: &str) -> Result<Vec<u8>, String> {
    let encoded = text(frame, "bytes_base64", label)?;
    let value = bytes(encoded, &format!("{label}.bytes_base64"))?;
    required_digest(frame, "sha256", label, &value)?;
    if !prefix.is_empty() {
        let actual = text(frame, "type", label)?;
        if actual != prefix {
            return Err(format!("{label}.type is {actual}, expected {prefix}"));
        }
    }
    Ok(value)
}

fn expected_frames(record: &Map<String, Value>, request: bool) -> Result<Vec<(usize, String, Vec<u8>)>, String> {
    let array_key = if request { "request_frames" } else { "response_frames" };
    let type_key = if request { "request_type" } else { "response_type" };
    let bytes_key = if request { "request_base64" } else { "response_base64" };
    let frames = record.get(array_key).and_then(Value::as_array);
    if let Some(frames) = frames.filter(|frames| !frames.is_empty()) {
        return frames.iter().enumerate().map(|(index, raw)| {
            let frame = object(raw, &format!("{array_key}[{index}]"))?;
            let kind = if request { "type" } else { "response_type" };
            let label = format!("{array_key}[{index}]");
            let sequence = frame.get("sequence").and_then(Value::as_u64).ok_or_else(|| format!("{label}.sequence is missing"))? as usize;
            if sequence != index { return Err(format!("{label}.sequence {sequence} != {index}")); }
            let ty = text(frame, kind, &label)?.to_owned();
            let encoded = text(frame, if request { "bytes_base64" } else { "response_base64" }, &label)?;
            let value = bytes(encoded, &format!("{array_key}[{index}]"))?;
            required_digest(frame, if request { "sha256" } else { "response_sha256" }, &label, &value)?;
            Ok((sequence, ty, value))
        }).collect();
    }
    if !request && record.get("response_base64").and_then(Value::as_str).is_none() {
        return Ok(Vec::new());
    }
    let ty = text(record, type_key, "record")?.to_owned();
    let encoded = record.get(bytes_key).and_then(Value::as_str).ok_or_else(|| format!("record.{bytes_key} is missing"))?;
    let value = bytes(encoded, bytes_key)?;
    required_digest(record, if request { "request_sha256" } else { "response_sha256" }, "record", &value)?;
    Ok(vec![(0, ty, value)])
}

fn observed_frames<'a>(scenario: &'a Map<String, Value>, key: &str) -> Result<Vec<&'a Map<String, Value>>, String> {
    scenario.get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("scenario.{key} must be an array"))?
        .iter()
        .enumerate()
        .map(|(index, value)| object(value, &format!("scenario.{key}[{index}]")))
        .collect()
}

fn compare_frames(
    rpc: &str,
    direction: RpcDirection,
    expected: &[(usize, String, Vec<u8>)],
    observed: &[&Map<String, Value>],
    failures: &mut Vec<String>,
) {
    if expected.len() != observed.len() {
        failures.push(format!("{rpc}: {direction:?} frame count {} != {}", observed.len(), expected.len()));
        return;
    }
    let family = match family(rpc) {
        Ok(value) => value,
        Err(error) => { failures.push(error); return; }
    };
    for (index, ((expected_sequence, expected_type, expected_bytes), frame)) in expected.iter().zip(observed).enumerate() {
        let label = format!("{rpc} {direction:?} frame {index}");
        let observed_sequence = frame.get("sequence").and_then(Value::as_u64).map(|value| value as usize);
        if observed_sequence != Some(*expected_sequence) {
            failures.push(format!("{label}: sequence {observed_sequence:?} != Rust expected {expected_sequence}"));
        }
        let observed_bytes = match frame_bytes(frame, &label, expected_type) {
            Ok(value) => value,
            Err(error) => { failures.push(error); continue; }
        };
        if let Err(error) = compare_family_rpc_message(family, rpc, direction, expected_bytes, &observed_bytes) {
            failures.push(format!("{label}: {error}"));
        }
    }
}

fn verify_step(record: &Map<String, Value>, scenario: &Map<String, Value>, source_revision: &str) -> Vec<String> {
    let mut failures = Vec::new();
    let rpc = match text(record, "rpc", "record") {
        Ok(value) => value,
        Err(error) => return vec![error],
    };
    if scenario.get("execution_step").and_then(Value::as_u64).map(|value| value as usize) != record.get("execution_step").and_then(Value::as_u64).map(|value| value as usize) {
        failures.push(format!("{rpc}: execution_step does not match Rust plan"));
    }
    if scenario.get("rpc").and_then(Value::as_str) != Some(rpc) {
        failures.push(format!("step RPC differs from Rust plan: expected {rpc}"));
        return failures;
    }
    if scenario.get("schema").and_then(Value::as_str) != Some("acyclic.sdk.rpc-scenario-result.v2") {
        failures.push(format!("{rpc}: scenario schema is not acyclic.sdk.rpc-scenario-result.v2"));
    }
    if scenario.get("source_revision").and_then(Value::as_str) != Some(source_revision) {
        failures.push(format!("{rpc}: scenario source_revision does not match the receipt"));
    }
    if scenario.get("invoked").and_then(Value::as_bool) != Some(true) {
        failures.push(format!("{rpc}: scenario was not invoked"));
    }
    if scenario.get("execution_mode").and_then(Value::as_str) != Some("remote") || scenario.get("transport").and_then(Value::as_str) != Some("grpc") {
        failures.push(format!("{rpc}: scenario is not a remote gRPC observation"));
    }
    let status = scenario.get("status").and_then(Value::as_str).unwrap_or("");
    if !matches!(status, "observed" | "observed_error") {
        failures.push(format!("{rpc}: consumer emitted non-transport status {status:?}"));
    }
    let expected_requests = match expected_frames(record, true) {
        Ok(value) => value,
        Err(error) => { failures.push(error); Vec::new() }
    };
    match observed_frames(scenario, "request_frames") {
        Ok(frames) => compare_frames(rpc, RpcDirection::Request, &expected_requests, &frames, &mut failures),
        Err(error) => failures.push(error),
    }
    let outcome = record.get("expected_outcome").and_then(Value::as_object);
    let expected_kind = outcome.and_then(|value| value.get("kind")).and_then(Value::as_str).unwrap_or("");
    let expected_code = outcome.and_then(|value| value.get("grpc_code")).and_then(Value::as_str);
    if !matches!(expected_kind, "success" | "stream" | "error") { failures.push(format!("{rpc}: Rust expected_outcome.kind is missing or invalid")); }
    if expected_code.is_none() { failures.push(format!("{rpc}: Rust expected_outcome.grpc_code is missing")); }
    let terminal = scenario.get("terminal").and_then(Value::as_object);
    let observed_code = terminal.and_then(|value| value.get("code")).and_then(Value::as_str).unwrap_or("");
    if expected_code != Some(observed_code) {
        failures.push(format!("{rpc}: gRPC code {observed_code:?} != Rust expected {expected_code:?}"));
    }
    if expected_kind == "error" && status != "observed_error" {
        failures.push(format!("{rpc}: Rust expected an error but consumer did not report one"));
    }
    if expected_kind == "success" && status != "observed" {
        failures.push(format!("{rpc}: Rust expected success but consumer reported {status}"));
    }
    let expected_terminal = outcome.and_then(|value| value.get("terminal")).and_then(Value::as_str);
    if let Some(detail) = outcome.and_then(|value| value.get("detail")).and_then(Value::as_str) {
        let observed_detail = terminal.and_then(|value| value.get("details")).and_then(Value::as_str).unwrap_or("");
        if observed_detail != detail { failures.push(format!("{rpc}: error detail {observed_detail:?} != Rust detail {detail:?}")); }
    }
    let expected_terminal_kind = outcome.and_then(|value| value.get("terminal_kind")).and_then(Value::as_str);
    let observed_terminal_kind = scenario.get("terminal_kind").and_then(Value::as_str);
    if expected_terminal != expected_terminal_kind {
        failures.push(format!("{rpc}: Rust expected_outcome terminal and terminal_kind disagree"));
    }
    if expected_kind == "stream" {
        let expected_stream_status = if expected_terminal == Some("timeout") { "observed_error" } else { "observed" };
        if status != expected_stream_status { failures.push(format!("{rpc}: Rust stream terminal requires status {expected_stream_status}, got {status}")); }
    }
    if observed_terminal_kind != expected_terminal_kind {
        failures.push(format!("{rpc}: terminal kind {observed_terminal_kind:?} != Rust expected {expected_terminal_kind:?}"));
    }
    let expected_responses = match expected_frames(record, false) {
        Ok(value) => value,
        Err(error) => { failures.push(error); Vec::new() }
    };
    match observed_frames(scenario, "response_frames") {
        Ok(frames) => compare_frames(rpc, RpcDirection::Response, &expected_responses, &frames, &mut failures),
        Err(error) => failures.push(error),
    }
    if scenario.get("response_count").and_then(Value::as_u64) != Some(scenario.get("response_frames").and_then(Value::as_array).map_or(0, Vec::len) as u64) {
        failures.push(format!("{rpc}: response_count does not equal response_frames length"));
    }
    failures
}

pub fn verify_paths(manifest_path: &Path, observed_path: &Path) -> Result<Value, String> {
    let canonical = env::var_os("ACYCLIC_RUST_CANONICAL_TYPED_REQUEST_MANIFEST")
        .ok_or("ACYCLIC_RUST_CANONICAL_TYPED_REQUEST_MANIFEST is required for Rust authority verification")?;
    verify_paths_with_canonical(manifest_path, observed_path, Path::new(&canonical))
}

pub fn verify_paths_with_canonical(manifest_path: &Path, observed_path: &Path, canonical_path: &Path) -> Result<Value, String> {
    let manifest_bytes = fs::read(&manifest_path).map_err(|error| error.to_string())?;
    let observed_bytes = fs::read(&observed_path).map_err(|error| error.to_string())?;
    let manifest: Value = serde_json::from_slice(&manifest_bytes).map_err(|error| error.to_string())?;
    let observed: Value = serde_json::from_slice(&observed_bytes).map_err(|error| error.to_string())?;
    let manifest_root = object(&manifest, "manifest")?;
    let observed_root = object(&observed, "observed")?;
    if manifest_root.get("complete").and_then(Value::as_bool) != Some(true) { return Err("Rust typed manifest is not complete".to_owned()); }
    if manifest_root.get("schema_version").and_then(Value::as_u64) != Some(2) { return Err("Rust typed manifest schema_version must be 2".to_owned()); }
    if text(manifest_root, "source", "manifest")? != "rust-executable-fixtures" { return Err("manifest.source is not the Rust executable fixture producer".to_owned()); }
    if text(manifest_root, "producer", "manifest")? != "acyclic-sdk-examples::typed-request-manifest" { return Err("manifest.producer is not the Rust typed-request producer".to_owned()); }
    if observed_root.get("schema").and_then(Value::as_str) != Some("acyclic.sdk.rpc-scenario-log.v2") { return Err("observed schema must be acyclic.sdk.rpc-scenario-log.v2".to_owned()); }
    let observed_manifest_digest = observed_root.get("manifest_sha256").and_then(Value::as_str).ok_or("observed.manifest_sha256 is missing")?;
    if observed_manifest_digest != digest(&manifest_bytes) { return Err("observed.manifest_sha256 does not match the supplied Rust manifest".to_owned()); }
    let manifest_revision = text(manifest_root, "source_revision", "manifest")?;
    if manifest_revision.is_empty() { return Err("manifest.source_revision must not be empty".to_owned()); }
    let source_revision = text(observed_root, "source_revision", "observed")?;
    if source_revision.is_empty() { return Err("observed.source_revision must not be empty".to_owned()); }
    if source_revision != manifest_revision { return Err("observed.source_revision does not match the Rust manifest".to_owned()); }
    let plan = manifest_root.get("execution_plan").and_then(Value::as_array).ok_or("manifest.execution_plan is missing")?;
    let records = manifest_root.get("records").and_then(Value::as_array).ok_or("manifest.records is missing")?;
    let authority_methods = resolved_rpc_methods().map_err(|error| format!("Rust RPC registry failed to resolve: {error}"))?;
    let authority_registry = authority_methods.iter().map(|method| method.rpc.as_str()).collect::<std::collections::BTreeSet<_>>();
    if records.len() != authority_registry.len() { return Err(format!("Rust manifest registry contains {}; compiled Rust registry contains {}", records.len(), authority_registry.len())); }
    if manifest_root.get("record_count").and_then(Value::as_u64) != Some(records.len() as u64) { return Err("manifest.record_count does not match records".to_owned()); }
    if manifest_root.get("execution_plan_count").and_then(Value::as_u64) != Some(plan.len() as u64) { return Err("manifest.execution_plan_count does not match execution_plan".to_owned()); }
    if manifest_root.get("missing_rpcs").and_then(Value::as_array).is_none_or(|missing| !missing.is_empty()) { return Err("manifest.missing_rpcs is not empty".to_owned()); }
    let mut registry = std::collections::BTreeSet::new();
    for (index, raw) in records.iter().enumerate() {
        let record = object(raw, &format!("manifest.records[{index}]"))?;
        let rpc = text(record, "rpc", &format!("manifest.records[{index}]"))?;
        if !registry.insert(rpc) { return Err(format!("Rust manifest registry repeats RPC {rpc}")); }
    }
    let mut covered = std::collections::BTreeSet::new();
    for raw in plan {
        let record = object(raw, "manifest.execution_plan record")?;
        let rpc = text(record, "rpc", "manifest.execution_plan record")?;
        if !registry.contains(rpc) { return Err(format!("Rust execution plan RPC {rpc} is absent from records")); }
        covered.insert(rpc);
    }
    if registry != authority_registry { return Err("Rust manifest records do not exactly match the compiled Rust RPC registry".to_owned()); }
    if plan.len() < authority_registry.len() { return Err(format!("Rust execution plan contains {}; compiled Rust registry requires at least {} ordered steps", plan.len(), authority_registry.len())); }
    if covered != authority_registry { return Err(format!("Rust execution plan covers {} of {} compiled Rust RPCs", covered.len(), authority_registry.len())); }
    let plan_bytes = serde_json::to_vec(plan).map_err(|error| error.to_string())?;
    let plan_digest = digest(&plan_bytes);
    if text(manifest_root, "execution_plan_sha256", "manifest")? != plan_digest {
        return Err("manifest.execution_plan_sha256 does not match the ordered Rust execution plan".to_owned());
    }
    let canonical_bytes = fs::read(canonical_path).map_err(|error| error.to_string())?;
    let canonical: Value = serde_json::from_slice(&canonical_bytes).map_err(|error| error.to_string())?;
    let canonical_root = object(&canonical, "canonical manifest")?;
    if canonical_root.get("complete").and_then(Value::as_bool) != Some(true) {
        return Err("canonical manifest is not complete".to_owned());
    }
    if canonical_root.get("schema_version").and_then(Value::as_u64) != Some(2) {
        return Err("canonical manifest schema_version must be 2".to_owned());
    }
    if canonical_root.get("source").and_then(Value::as_str) != Some("rust-executable-fixtures") {
        return Err("canonical manifest is not the Rust executable fixture producer".to_owned());
    }
    if canonical_root.get("producer").and_then(Value::as_str) != Some("acyclic-sdk-examples::typed-request-manifest") {
        return Err("canonical manifest is not produced by the Rust typed-request producer".to_owned());
    }
    let canonical_revision = text(canonical_root, "source_revision", "canonical manifest")?;
    if canonical_revision.is_empty() {
        return Err("canonical manifest.source_revision must not be empty".to_owned());
    }
    if canonical_revision != manifest_revision || canonical_revision != source_revision {
        return Err("canonical, manifest, and observed source_revision values must match".to_owned());
    }
    if canonical_root.get("execution_plan").is_none() || canonical_root.get("records").is_none() {
        return Err("canonical manifest must contain records and execution_plan".to_owned());
    }
    if canonical_root.get("execution_plan") != manifest_root.get("execution_plan") {
        return Err("manifest execution plan differs from the independent Rust producer plan".to_owned());
    }
    if canonical_root.get("records") != manifest_root.get("records") {
        return Err("manifest RPC records differ from the independent Rust producer registry".to_owned());
    }
    let authority_payload = json!({"records": records, "execution_plan": plan});
    let authority_bytes = serde_json::to_vec(&authority_payload).map_err(|error| error.to_string())?;
    let authority_digest = text(manifest_root, "authority_sha256", "manifest")?;
    if authority_digest != digest(&authority_bytes) { return Err("manifest.authority_sha256 does not match the Rust registry and execution plan".to_owned()); }
    let scenarios = observed_root.get("scenarios").and_then(Value::as_array).ok_or("observed.scenarios is missing")?;
    let mut failures = Vec::new();
    if scenarios.len() != plan.len() { failures.push(format!("observed scenario count {} != Rust execution plan {}", scenarios.len(), plan.len())); }
    let mut rows = Vec::new();
    for (index, record) in plan.iter().enumerate() {
        let mut plan_failures = Vec::new();
        if record.get("execution_step").and_then(Value::as_u64) != Some(index as u64) {
            plan_failures.push(format!("Rust execution plan record has execution_step != {index}"));
        }
        let step_failures = scenarios.get(index).and_then(Value::as_object).map(|scenario| {
            let mut failures = verify_step(object(record, "plan record").unwrap_or(&Map::new()), scenario, source_revision);
            failures.splice(0..0, plan_failures.clone());
            failures
        }).unwrap_or_else(|| {
            plan_failures.push(format!("missing observed step {index}"));
            plan_failures
        });
        failures.extend(step_failures.iter().map(|failure| format!("step {index}: {failure}")));
        rows.push(json!({"execution_step": index, "rpc": record.get("rpc"), "status": if step_failures.is_empty() { "passed" } else { "failed" }, "failures": step_failures}));
    }
    let report = json!({"schema":"acyclic.sdk.rust-observation-verification.v1","status":if failures.is_empty(){"passed"}else{"failed"},"inputs":{"manifest_sha256":digest(&manifest_bytes),"observed_sha256":digest(&observed_bytes),"manifest_path":manifest_path,"observed_path":observed_path,"manifest_schema_version":manifest_root.get("schema_version"),"observed_schema":observed_root.get("schema"),"source_revision":source_revision},"execution_plan_count":plan.len(),"observed_count":scenarios.len(),"rows":rows,"failures":failures});
    Ok(report)
}

pub fn run_cli() -> Result<(), String> {
    let (manifest_path, observed_path, output_path, canonical_path) = args();
    let report = verify_paths_with_canonical(&manifest_path, &observed_path, &canonical_path)?;
    fs::write(&output_path, serde_json::to_vec_pretty(&report).map_err(|error| error.to_string())?).map_err(|error| error.to_string())?;
    if report["status"] == "passed" { Ok(()) } else { Err(format!("Rust observation verification failed: {} failures", report["failures"].as_array().map_or(0, Vec::len))) }
}

fn main() -> Result<(), String> {
    run_cli()
}
