//! Rust-owned runtime consumer rendering for the additional language lane.
//!
//! The authority manifest is the only input that contains RPC identity, shape,
//! and type information.  The language launchers deliberately know nothing
//! about protobuf syntax or target naming rules; they invoke this renderer.

use base64::Engine as _;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy)]
pub enum Language {
    Elixir,
    Erlang,
    CommonLisp,
}

#[derive(Debug, Clone)]
struct Call {
    package: String,
    service: String,
    method: String,
    request: String,
    response: String,
    shape: String,
    rpc: String,
    source: String,
    request_base64: String,
}

#[derive(Debug, Clone)]
struct TypedRecord {
    request_base64: String,
    request_frames: Vec<String>,
}

#[derive(Debug, Clone)]
struct ExecutionRecord {
    execution_step: usize,
    rpc: String,
    request_base64: String,
    request_frames: Vec<String>,
}

fn encoded_frames(record: &Value, rpc: &str) -> Result<Vec<String>, String> {
    record
        .get("request_frames")
        .and_then(Value::as_array)
        .map(|frames| {
            frames
                .iter()
                .map(|frame| {
                    frame
                        .get("request_base64")
                        .or_else(|| frame.get("bytes_base64"))
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                        .ok_or_else(|| format!("typed request frame for {rpc} is missing bytes_base64"))
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()
        .map(|frames| frames.unwrap_or_default())
}

fn typed_records(path: Option<&Path>) -> Result<std::collections::BTreeMap<String, TypedRecord>, String> {
    let Some(path) = path else {
        return Ok(std::collections::BTreeMap::new());
    };
    let bytes = fs::read(path)
        .map_err(|error| format!("read {}: {error}", path.display()))?;
    let document: Value = serde_json::from_slice(&bytes)
        .map_err(|error| format!("parse {}: {error}", path.display()))?;
    let records = document
        .get("records")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{} is missing records", path.display()))?;
    let mut output = std::collections::BTreeMap::new();
    for record in records {
        let rpc = required_string(record, "rpc")?.to_string();
        let request_base64 = record
            .get("request_base64")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("typed request record {rpc} is missing request_base64"))?
            .to_string();
        let request_frames = encoded_frames(record, &rpc)?;
        if output.insert(rpc.clone(), TypedRecord { request_base64, request_frames }).is_some() {
            return Err(format!("typed request manifest contains duplicate RPC {rpc}"));
        }
    }
    Ok(output)
}

fn execution_records(path: &Path) -> Result<Vec<ExecutionRecord>, String> {
    let bytes = fs::read(path).map_err(|error| format!("read {}: {error}", path.display()))?;
    let document: Value = serde_json::from_slice(&bytes)
        .map_err(|error| format!("parse {}: {error}", path.display()))?;
    let Some(records) = document.get("execution_plan").and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    let mut seen_steps = BTreeSet::new();
    records
        .iter()
        .enumerate()
        .map(|(index, record)| {
            let rpc = required_string(record, "rpc")?.to_owned();
            let execution_step = record
                .get("execution_step")
                .and_then(Value::as_u64)
                .map(|step| step as usize)
                .unwrap_or(index);
            let request_base64 = record
                .get("request_base64")
                .and_then(Value::as_str)
                .ok_or_else(|| format!("typed execution record {rpc} is missing request_base64"))?
                .to_owned();
            if !seen_steps.insert(execution_step) {
                return Err(format!("typed execution plan contains duplicate execution_step {execution_step}"));
            }
            Ok(ExecutionRecord {
                execution_step,
                rpc: rpc.clone(),
                request_base64,
                request_frames: encoded_frames(record, &rpc)?,
            })
        })
        .collect()
}

fn required_string<'a>(value: &'a Value, key: &str) -> Result<&'a str, String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("authority RPC is missing non-empty {key}"))
}

fn read_json(path: &Path) -> Result<Value, String> {
    let bytes = fs::read(path).map_err(|error| format!("read {}: {error}", path.display()))?;
    serde_json::from_slice(&bytes).map_err(|error| format!("parse {}: {error}", path.display()))
}

fn normalized_digest(value: &Value, label: &str) -> Result<String, String> {
    let raw = value
        .as_str()
        .ok_or_else(|| format!("{label} must be a SHA-256 digest"))?;
    let normalized = raw.strip_prefix("sha256:").unwrap_or(raw).to_ascii_lowercase();
    if normalized.len() != 64 || !normalized.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(format!("{label} must be a SHA-256 digest"));
    }
    Ok(normalized)
}

fn sha256_bytes(value: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(value))
}

fn authority_binding(inventory: &Value, receipt: &Value) -> Result<Value, String> {
    let expected = inventory
        .get("authority")
        .and_then(Value::as_object)
        .ok_or_else(|| "Rust RPD inventory authority is missing".to_string())?;
    let actual = receipt
        .get("authority")
        .and_then(Value::as_object)
        .ok_or_else(|| "runtime receipt authority is missing; use the Rust-owned receipt emitter".to_string())?;
    for field in ["source_git_sha", "model_digest", "source_file_hashes"] {
        if actual.get(field) != expected.get(field) {
            return Err(format!("runtime receipt authority {field} does not match the Rust producer authority"));
        }
    }
    let source_git_sha = expected
        .get("source_git_sha")
        .and_then(Value::as_str)
        .ok_or_else(|| "Rust authority source_git_sha is missing".to_string())?;
    if source_git_sha.len() != 40 || !source_git_sha.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("Rust authority source_git_sha must be a 40-digit Git revision".into());
    }
    let hashes = expected
        .get("source_file_hashes")
        .and_then(Value::as_object)
        .filter(|hashes| !hashes.is_empty())
        .ok_or_else(|| "Rust authority source_file_hashes must be a non-empty producer closure".to_string())?;
    for (path, value) in hashes {
        normalized_digest(value, &format!("Rust authority source_file_hashes[{path:?}]"))?;
    }
    if let Some(revision) = receipt.get("source_revision")
        && revision.as_str() != Some(source_git_sha)
    {
        return Err("runtime receipt source_revision does not match the Rust authority".into());
    }
    if let Some(manifest_digest) = receipt.get("rust_authority_manifest_sha256") {
        normalized_digest(manifest_digest, "runtime receipt rust_authority_manifest_sha256")?;
    }
    Ok(Value::Object(expected.clone()))
}

fn executed_package_binding(receipt: &Value, language: &str, authority: &Value) -> Result<Value, String> {
    let binding = ["executed_package", "package_binding", "package_provenance"]
        .iter()
        .find_map(|key| receipt.get(*key))
        .and_then(Value::as_object)
        .ok_or_else(|| format!("{language}: runtime receipt executed_package metadata is missing"))?;
    if let Some(declared) = binding.get("language").and_then(Value::as_str)
        && declared != language
    {
        return Err(format!("{language}: executed package language does not match receipt language"));
    }
    if binding.get("source_git_sha") != authority.get("source_git_sha") {
        return Err(format!("{language}: executed package source_git_sha does not match Rust authority"));
    }
    let package_model = binding.get("model_digest").or_else(|| binding.get("rust_model_digest"));
    if package_model != authority.get("model_digest") {
        return Err(format!("{language}: executed package model digest does not match Rust authority"));
    }
    let authority_closure = authority
        .get("source_file_hashes")
        .and_then(Value::as_object)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{language}: Rust authority source_file_hashes closure is missing"))?;
    let package_closure = binding
        .get("source_file_hashes")
        .and_then(Value::as_object)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{language}: executed package source_file_hashes closure is missing"))?;
    if package_closure != authority_closure {
        return Err(format!("{language}: executed package source_file_hashes closure does not match Rust authority"));
    }
    if let Some(producer) = binding.get("producer_source_file_hashes")
        && producer != &Value::Object(authority_closure.clone())
    {
        return Err(format!("{language}: executed package producer source closure does not match Rust authority"));
    }
    for field in ["source_file_hashes", "generated_source_file_hashes", "package_source_file_hashes"] {
        if let Some(values) = binding.get(field) {
            let values = values.as_object().filter(|values| !values.is_empty())
                .ok_or_else(|| format!("{language}: executed package {field} must be a non-empty hash map"))?;
            for (path, value) in values {
                normalized_digest(value, &format!("{language}: executed package {field}[{path:?}]"))?;
            }
        }
    }
    for field in ["artifact_sha256", "package_artifact_sha256", "provenance_sha256"] {
        if let Some(value) = binding.get(field) {
            normalized_digest(value, &format!("{language}: executed package {field}"))?;
        }
    }
    Ok(Value::Object(binding.clone()))
}

fn rpc_key(value: &Value) -> Result<String, String> {
    let key = value
        .get("rpc")
        .or_else(|| value.get("path"))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "manifest/observation method is missing rpc/path".to_string())?;
    Ok(key.trim_start_matches('/').to_string())
}

fn bytes_from_base64(value: &Value, label: &str) -> Result<(String, Vec<u8>), String> {
    let encoded = value
        .as_str()
        .ok_or_else(|| format!("{label} must be base64 text"))?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|error| format!("{label} is not valid base64: {error}"))?;
    Ok((bytes.iter().map(|byte| format!("{byte:02x}")).collect(), bytes))
}

fn hex_bytes(value: &str, label: &str) -> Result<Vec<u8>, String> {
    if value.len() % 2 != 0 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(format!("{label} is not hexadecimal"));
    }
    (0..value.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&value[index..index + 2], 16)
            .map_err(|error| format!("{label} is not hexadecimal: {error}")))
        .collect()
}

fn response_frames(observation: &Value, label: &str) -> Result<Vec<(String, Vec<u8>)>, String> {
    if let Some(frames) = observation.get("response_frame_base64").or_else(|| observation.get("response_frames_base64")) {
        let frames = frames.as_array().ok_or_else(|| format!("{label}.response_frame_base64 must be an array"))?;
        return frames.iter().enumerate().map(|(index, frame)| bytes_from_base64(frame, &format!("{label}.response_frame_base64[{index}]"))).collect();
    }
    let frames = observation.get("response_frames_hex")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{label}.response_frame_base64 or response_frames_hex must be an array"))?;
    frames.iter().enumerate().map(|(index, frame)| {
        let text = frame.as_str().ok_or_else(|| format!("{label}.response_frames_hex[{index}] must be hexadecimal text"))?;
        Ok((text.to_ascii_lowercase(), hex_bytes(text, &format!("{label}.response_frames_hex[{index}]"))?))
    }).collect()
}

fn terminal(observation: &Value, label: &str) -> Result<(String, i64), String> {
    let status = observation.get("terminal_status").and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{label}.terminal_status is missing"))?;
    let code = observation.get("terminal_code").and_then(Value::as_i64)
        .ok_or_else(|| format!("{label}.terminal_code must be a numeric gRPC status"))?;
    if !(0..=16).contains(&code) {
        return Err(format!("{label}.terminal_code must be in 0..16"));
    }
    Ok((if status == "cancelled" { "canceled".into() } else { status.into() }, code))
}

fn is_server_streaming(method: &Value) -> bool {
    method.get("server_streaming").and_then(Value::as_bool) == Some(true)
        || matches!(method.get("shape").and_then(Value::as_str), Some("server" | "server_stream" | "bidi"))
}

fn request_hex(observation: &Value, label: &str) -> Result<(String, Vec<u8>), String> {
    if let Some(value) = observation.get("request_base64") {
        return bytes_from_base64(value, &format!("{label}.request_base64"));
    }
    let value = observation.get("request_bytes_hex").and_then(Value::as_str)
        .ok_or_else(|| format!("{label}.request_base64 or request_bytes_hex is missing"))?;
    Ok((value.to_ascii_lowercase(), hex_bytes(value, &format!("{label}.request_bytes_hex"))?))
}

fn request_frames(value: &Value, label: &str) -> Result<Vec<String>, String> {
    let frames = value.as_array().ok_or_else(|| format!("{label} must be an array"))?;
    frames.iter().enumerate().map(|(index, frame)| {
        if let Some(text) = frame.as_str() {
            return Ok(text.to_ascii_lowercase());
        }
        let object = frame.as_object().ok_or_else(|| format!("{label}[{index}] must be an object or base64 text"))?;
        if let Some(hex) = object.get("serialized_hex").or_else(|| object.get("bytes_hex")).and_then(Value::as_str) {
            hex_bytes(hex, &format!("{label}[{index}]"))?;
            return Ok(hex.to_ascii_lowercase());
        }
        let encoded = object.get("request_base64").or_else(|| object.get("bytes_base64")).or_else(|| object.get("base64"))
            .ok_or_else(|| format!("{label}[{index}] is missing serialized bytes"))?;
        Ok(bytes_from_base64(encoded, &format!("{label}[{index}]"))?.0)
    }).collect()
}

fn observed_request_frames(observation: &Value, label: &str) -> Result<Option<Vec<String>>, String> {
    if let Some(value) = observation.get("request_frames_base64").or_else(|| observation.get("request_frame_base64")) {
        let values = value.as_array().ok_or_else(|| format!("{label}.request_frames_base64 must be an array"))?;
        return Ok(Some(values.iter().enumerate().map(|(index, frame)| bytes_from_base64(frame, &format!("{label}.request_frames_base64[{index}]" )).map(|(hex, _)| hex)).collect::<Result<Vec<_>, _>>()?));
    }
    if let Some(value) = observation.get("request_frames_hex") {
        let values = value.as_array().ok_or_else(|| format!("{label}.request_frames_hex must be an array"))?;
        return Ok(Some(values.iter().enumerate().map(|(index, frame)| {
            let text = frame.as_str().ok_or_else(|| format!("{label}.request_frames_hex[{index}] must be hexadecimal text"))?;
            hex_bytes(text, &format!("{label}.request_frames_hex[{index}]"))?;
            Ok(text.to_ascii_lowercase())
        }).collect::<Result<Vec<_>, String>>()?));
    }
    Ok(None)
}

fn normalize_receipt(
    inventory_path: &Path,
    receipt_path: &Path,
    language: &str,
    output_path: &Path,
) -> Result<(), String> {
    let inventory = read_json(inventory_path)?;
    let receipt = read_json(receipt_path)?;
    let authority = authority_binding(&inventory, &receipt)?;
    let package = executed_package_binding(&receipt, language, &authority)?;
    let methods = inventory.get("methods").and_then(Value::as_array)
        .ok_or_else(|| "Rust RPD inventory methods are missing".to_string())?;
    if methods.is_empty() {
        return Err("Rust RPD inventory methods must not be empty".into());
    }
    let mut method_by_rpc = BTreeMap::<String, &Value>::new();
    for method in methods {
        let rpc = rpc_key(method)?;
        if method_by_rpc.insert(rpc.clone(), method).is_some() {
            return Err(format!("Rust RPD inventory contains duplicate method {rpc}"));
        }
    }
    let plan = inventory.get("execution_plan").and_then(Value::as_array);
    let expected: Vec<&Value> = match plan {
        Some(plan) if !plan.is_empty() => plan.iter().collect(),
        _ => methods.iter().collect(),
    };
    let observations = receipt.get("observations").and_then(Value::as_array)
        .ok_or_else(|| "runtime receipt observations are missing".to_string())?;
    if observations.is_empty() {
        return Err("runtime receipt observations must not be empty".into());
    }
    let mut by_step = BTreeMap::<usize, &Value>::new();
    let mut by_rpc = BTreeMap::<String, Vec<&Value>>::new();
    for (index, observation) in observations.iter().enumerate() {
        let rpc = rpc_key(observation)?;
        by_rpc.entry(rpc).or_default().push(observation);
        if let Some(step) = observation.get("execution_step").and_then(Value::as_u64) {
            if by_step.insert(step as usize, observation).is_some() {
                return Err(format!("duplicate runtime execution_step {step}"));
            }
        } else if plan.is_some() {
            return Err(format!("observation {index} is missing execution_step for an ordered Rust execution plan"));
        }
    }
    let mut rpc_occurrence = BTreeMap::<String, usize>::new();
    let mut used_observations = BTreeSet::<usize>::new();
    let mut output_methods = Vec::with_capacity(expected.len());
    for (index, raw_plan) in expected.iter().enumerate() {
        let rpc = rpc_key(raw_plan)?;
        let method = method_by_rpc.get(&rpc).copied().unwrap_or(*raw_plan);
        let observation = if let Some(step) = raw_plan.get("execution_step").and_then(Value::as_u64) {
            by_step.get(&(step as usize)).copied()
        } else if plan.is_some() {
            by_step.get(&index).copied()
        } else {
            let occurrence = rpc_occurrence.entry(rpc.clone()).or_insert(0);
            let item = by_rpc.get(&rpc).and_then(|items| items.get(*occurrence)).copied();
            *occurrence += 1;
            item
        };
        let observation = observation.ok_or_else(|| format!("runtime receipt is missing execution step {index} ({rpc})"))?;
        let observation_index = observations.iter().position(|item| std::ptr::eq(item, observation)).unwrap_or(index);
        if !used_observations.insert(observation_index) {
            return Err(format!("runtime observation for {rpc} is used more than once"));
        }
        if rpc_key(observation)? != rpc {
            return Err(format!("execution step {index} RPC identity does not match Rust plan: expected {rpc}"));
        }
        let typed = method.get("typed_request").or_else(|| raw_plan.get("typed_request"))
            .and_then(Value::as_object)
            .ok_or_else(|| format!("{rpc}: inventory lacks canonical typed_request"))?;
        let canonical_frames = raw_plan.get("request_frames").or_else(|| typed.get("serialized_frames"))
            .map(|value| request_frames(value, &format!("{rpc}.request_frames")))
            .transpose()?;
        let canonical_hex = typed.get("serialized_hex")
            .or_else(|| typed.get("empty_serialized_hex"))
            .and_then(Value::as_str)
            .map(str::to_ascii_lowercase);
        let has_canonical_frames = canonical_frames.is_some();
        if let Some(canonical_frames) = canonical_frames {
            if let Some(actual_frames) = observed_request_frames(observation, &rpc)? {
                if actual_frames != canonical_frames {
                    return Err(format!("{rpc}: request frames do not match the Rust canonical typed request frames"));
                }
            } else {
                return Err(format!("{rpc}: canonical request frames are missing from the runtime receipt"));
            }
        }
        let (request_hex_value, request_bytes) = request_hex(observation, &rpc)?;
        if !has_canonical_frames {
            let canonical_hex = canonical_hex.ok_or_else(|| format!("{rpc}: inventory lacks canonical typed request bytes"))?;
            if request_hex_value != canonical_hex {
                return Err(format!("{rpc}: request bytes do not match the Rust canonical typed request"));
            }
        }
        if !has_canonical_frames {
            if let Some(expected_request) = raw_plan.get("request_base64") {
            let (expected_hex, _) = bytes_from_base64(expected_request, &format!("{rpc}.request_base64"))?;
            if expected_hex != request_hex_value {
                return Err(format!("{rpc}: request bytes do not match the ordered Rust execution plan"));
            }
            }
        }
        let request_digest = match observation.get("request_sha256") {
            Some(value) => value.as_str().ok_or_else(|| format!("{rpc}.request_sha256 must be a SHA-256 digest"))?.to_owned(),
            None => sha256_bytes(&request_bytes),
        };
        let actual_request_digest = sha256_bytes(&request_bytes);
        if request_digest.strip_prefix("sha256:").unwrap_or(&request_digest).to_ascii_lowercase()
            != actual_request_digest.strip_prefix("sha256:").unwrap_or(&actual_request_digest).to_ascii_lowercase()
        {
            return Err(format!("{rpc}: request digest does not match request bytes"));
        }
        let (status, code) = terminal(observation, &rpc)?;
        let frames = response_frames(observation, &rpc)?;
        if frames.is_empty() {
            return Err(format!("{rpc}: response frame list is empty"));
        }
        if let Some(value) = observation.get("response_sha256") {
            let digests = value.as_array().ok_or_else(|| format!("{rpc}.response_sha256 must be an array"))?;
            if digests.len() != frames.len() {
                return Err(format!("{rpc}: response digest count does not match response frame count"));
            }
            for (frame_index, (digest_value, (_, bytes))) in digests.iter().zip(frames.iter()).enumerate() {
                let actual = sha256_bytes(bytes);
                let expected = normalized_digest(digest_value, &format!("{rpc}.response_sha256[{frame_index}]"))?;
                if expected != actual.strip_prefix("sha256:").unwrap_or(&actual) {
                    return Err(format!("{rpc}: response frame {frame_index} digest does not match response bytes"));
                }
            }
        }
        let semantic_passed = observation.get("semantic_passed").and_then(Value::as_bool) == Some(true)
            || matches!(observation.get("semantic_status").and_then(Value::as_str), Some("passed" | "semantic_passed"));
        let mut output = method.clone();
        let object = output.as_object_mut().ok_or_else(|| format!("{rpc}: inventory method is not an object"))?;
        object.insert("request_sha256".into(), Value::String(request_digest));
        object.insert("status".into(), Value::String(if semantic_passed { "semantic_passed" } else if matches!(status.as_str(), "ok" | "canceled") { "transport_success_pending_semantics" } else { "error" }.into()));
        object.insert("terminal_status".into(), Value::String(status));
        object.insert("terminal_code".into(), Value::Number(code.into()));
        object.insert("request_bytes_hex".into(), Value::String(request_hex_value));
        object.insert("response_type_observed".into(), method.get("response_type").cloned().unwrap_or(Value::Null));
        object.insert("response_type_id_observed".into(), method.get("response_type").cloned().unwrap_or(Value::Null));
        if raw_plan.get("execution_step").is_some() || plan.is_some() {
            object.insert("execution_step".into(), raw_plan.get("execution_step").cloned().unwrap_or_else(|| Value::Number((index as u64).into())));
        }
        let frame_hexes: Vec<Value> = frames.iter().map(|(hex, _)| Value::String(hex.clone())).collect();
        let frame_digests: Vec<Value> = frames.iter().map(|(_, bytes)| Value::String(sha256_bytes(bytes))).collect();
        object.insert("response_frames_sha256".into(), Value::Array(frame_digests.clone()));
        if is_server_streaming(method) {
            let semantic = observation.get("response_frames").or_else(|| observation.get("decoded_response"))
                .and_then(Value::as_array)
                .ok_or_else(|| format!("{rpc}: semantic response_frames are missing or unordered"))?;
            if semantic.len() != frames.len() {
                return Err(format!("{rpc}: semantic response_frames count does not match wire frames"));
            }
            object.insert("response_frames_hex".into(), Value::Array(frame_hexes));
            object.insert("response_frames".into(), Value::Array(semantic.clone()));
            object.insert("response_frame_types".into(), Value::Array(vec![method.get("response_type").cloned().unwrap_or(Value::Null); frames.len()]));
            object.insert("response_frame_type_ids".into(), Value::Array(vec![method.get("response_type").cloned().unwrap_or(Value::Null); frames.len()]));
        } else {
            if frames.len() != 1 {
                return Err(format!("{rpc}: unary RPC must produce exactly one response frame"));
            }
            let decoded = observation.get("response").or_else(|| observation.get("decoded_response"))
                .cloned().ok_or_else(|| format!("{rpc}: semantic response value is missing"))?;
            object.insert("response_bytes_hex".into(), frame_hexes[0].clone());
            object.insert("response".into(), decoded);
            object.insert("response_sha256".into(), frame_digests[0].clone());
        }
        output_methods.push(output);
    }
    if used_observations.len() != observations.len() {
        return Err("runtime receipt contains observations outside the Rust ordered plan".into());
    }
    let output = serde_json::json!({
        "schema": format!("acyclic.sdk.rpd.{language}-live-receipt.v1"),
        "authority": authority,
        "method_count": output_methods.len(),
        "methods": output_methods,
        "executed_package": package,
    });
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent).map_err(|error| format!("create {}: {error}", parent.display()))?;
    }
    let mut bytes = serde_json::to_vec_pretty(&output).map_err(|error| format!("encode receipt: {error}"))?;
    bytes.push(b'\n');
    fs::write(output_path, bytes)
        .map_err(|error| format!("write {}: {error}", output_path.display()))
}

fn pascal(value: &str) -> String {
    value
        .trim_start_matches('.')
        .split('.')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(".")
}

fn snake(value: &str) -> String {
    let mut output = String::with_capacity(value.len() + 4);
    for (index, character) in value.chars().enumerate() {
        if character.is_ascii_uppercase() && index > 0 {
            output.push('_');
        }
        if character.is_ascii_alphanumeric() || character == '_' {
            output.push(character.to_ascii_lowercase());
        } else {
            output.push('_');
        }
    }
    output
}

fn lisp_name(value: &str) -> String {
    let mut output = String::with_capacity(value.len() + 4);
    let chars: Vec<char> = value.chars().collect();
    for (index, character) in chars.iter().copied().enumerate() {
        if index > 0
            && chars[index - 1].is_ascii_lowercase()
            && character.is_ascii_uppercase()
        {
            output.push('-');
        }
        if character.is_ascii_alphanumeric() {
            output.push(character.to_ascii_lowercase());
        } else {
            output.push('-');
        }
    }
    output.trim_matches('-').to_string()
}

fn method_parts(rpc: &str) -> Result<(String, String, String), String> {
    let (service_path, method) = rpc
        .split_once('/')
        .ok_or_else(|| format!("authority RPC has no method separator: {rpc}"))?;
    let split = service_path.rsplitn(2, '.').collect::<Vec<_>>();
    if split.len() != 2 || split[0].is_empty() || split[1].is_empty() || method.is_empty() {
        return Err(format!("authority RPC has invalid service identity: {rpc}"));
    }
    Ok((split[1].to_string(), split[0].to_string(), method.to_string()))
}

fn calls(authority: &Value, typed: &std::collections::BTreeMap<String, TypedRecord>) -> Result<Vec<Call>, String> {
    if authority.get("schema").and_then(Value::as_str)
        != Some("acyclic.sdk.rust-authority.v1")
    {
        return Err("runtime consumers require acyclic.sdk.rust-authority.v1".into());
    }
    let families = authority
        .get("families")
        .and_then(Value::as_array)
        .ok_or_else(|| "authority is missing families".to_string())?;
    let mut output = Vec::new();
    for family in families {
        let source = required_string(family, "source")?.to_string();
        let methods = family
            .get("rpc_methods")
            .and_then(Value::as_array)
            .ok_or_else(|| format!("authority family {source} is missing rpc_methods"))?;
        for method in methods {
            let rpc = required_string(method, "rpc")?.to_string();
            let (package, service, method_name) = method_parts(&rpc)?;
            let request = required_string(method, "request")?.to_string();
            let response = required_string(method, "response")?.to_string();
            let shape = match required_string(method, "shape")? {
                "server" => "server_stream".to_string(),
                "client" => "client_stream".to_string(),
                value => value.to_string(),
            };
            if !matches!(shape.as_str(), "unary" | "server_stream" | "client_stream" | "bidi") {
                return Err(format!("authority RPC {rpc} has unsupported shape {shape}"));
            }
            let request_base64 = typed
                .get(&rpc)
                .map(|record| record.request_base64.clone())
                .ok_or_else(|| format!("typed request manifest is missing {rpc}"))?;
            output.push(Call {
                package,
                service,
                method: method_name,
                request,
                response,
                shape,
                rpc,
                source: source.clone(),
                request_base64,
            });
        }
    }
    if output.is_empty() {
        return Err("authority contains no RPC methods".into());
    }
    Ok(output)
}

/// Write the ordered, Rust-owned streaming request projection consumed by
/// runtime language probes.  Every frame keeps its manifest sequence identity;
/// repeated messages are retained verbatim rather than deduplicated by RPC.
pub fn write_stream_scenarios(authority_path: &Path, typed_path: &Path, output: &Path) -> Result<(), String> {
    let authority: Value = serde_json::from_slice(
        &fs::read(authority_path)
            .map_err(|error| format!("read {}: {error}", authority_path.display()))?,
    )
    .map_err(|error| format!("parse {}: {error}", authority_path.display()))?;
    let typed = typed_records(Some(typed_path))?;
    let families = authority
        .get("families")
        .and_then(Value::as_array)
        .ok_or_else(|| "authority is missing families".to_string())?;
    let mut streaming_shapes = std::collections::BTreeMap::new();
    for family in families {
        let methods = family
            .get("rpc_methods")
            .and_then(Value::as_array)
            .ok_or_else(|| "authority family is missing rpc_methods".to_string())?;
        for method in methods {
            let rpc = required_string(method, "rpc")?.to_owned();
            let shape = required_string(method, "shape")?.to_owned();
            if matches!(shape.as_str(), "server" | "server_stream" | "client" | "client_stream" | "bidi") {
                streaming_shapes.insert(rpc, shape);
            }
        }
    }
    let mut lines = Vec::new();
    let execution = execution_records(typed_path)?;
    if !execution.is_empty() {
        for record in execution {
            if !streaming_shapes.contains_key(&record.rpc) {
                continue;
            }
            let frames = if record.request_frames.is_empty() {
                vec![record.request_base64]
            } else {
                record.request_frames
            };
            for (frame_sequence, frame) in frames.into_iter().enumerate() {
                // The execution step is part of the identity. Repeated calls
                // to one streaming RPC must remain distinct and ordered.
                // Empty Base64 is a valid canonical protobuf encoding for an
                // empty request message and must remain observable.
                lines.push(format!(
                    "{}#{}#{}\t{}",
                    record.rpc, record.execution_step, frame_sequence, frame
                ));
            }
        }
    } else {
        for (rpc, record) in typed {
            if !streaming_shapes.contains_key(&rpc) {
                continue;
            }
            let frames = if record.request_frames.is_empty() {
                vec![record.request_base64]
            } else {
                record.request_frames
            };
            for (sequence, frame) in frames.into_iter().enumerate() {
                lines.push(format!("{rpc}#{sequence}\t{frame}"));
            }
        }
    }
    if lines.is_empty() {
        return Err("authority contains no streaming request scenarios".into());
    }
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).map_err(|error| format!("create {}: {error}", parent.display()))?;
    }
    fs::write(output, format!("{}\n", lines.join("\n")))
        .map_err(|error| format!("write {}: {error}", output.display()))
}

fn shape_atom(shape: &str) -> &str {
    match shape {
        "server_stream" => "server_stream",
        "client_stream" => "client_stream",
        other => other,
    }
}

fn elixir_calls(calls: &[Call]) -> String {
    calls
        .iter()
        .map(|call| {
            let service_module = format!("{}.{}.Stub", pascal(&call.package), call.service);
            format!(
                "  {{{}, :{}, {}, {}, :{}, \"{}\", \"{}\", [{}]}},",
                service_module,
                snake(&call.method),
                pascal(&call.request),
                pascal(&call.response),
                shape_atom(&call.shape),
                call.rpc,
                call.request_base64,
                ""
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn proto_module(source: &str) -> String {
    let stem = Path::new(source)
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("contract");
    format!("{}_pb", snake(stem))
}

fn erlang_message_name(value: &str) -> String {
    let parts = value.trim_start_matches('.').split('.').collect::<Vec<_>>();
    let prefix = if parts.first() == Some(&"acyclic") {
        parts.get(1).copied().unwrap_or("message")
    } else {
        parts.first().copied().unwrap_or("message")
    };
    let message = parts.last().copied().unwrap_or("message");
    [erlang_name(prefix), erlang_name(message)]
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("_")
}
fn erlang_name(value: &str) -> String {
    let normalized = snake(value);
    let mut output = String::with_capacity(normalized.len() + 2);
    let mut previous: Option<char> = None;
    for character in normalized.chars() {
        if character.is_ascii_digit() && previous.is_some_and(|value| value.is_ascii_alphabetic()) {
            output.push('_');
        }
        output.push(character);
        previous = Some(character);
    }
    output
}

fn erlang_segment(value: &str) -> String {
    erlang_name(value)
}

fn erlang_calls(calls: &[Call]) -> String {
    calls
        .iter()
        .map(|call| {
            let package_prefix = call
                .package
                .split('.')
                .map(erlang_segment)
                .collect::<Vec<_>>()
                .join("_");
            let module = [package_prefix, erlang_name(&call.service), "client".into()]
                .into_iter()
                .filter(|part| !part.is_empty())
                .collect::<Vec<_>>()
                .join("_");
            let request = erlang_message_name(&call.request);
            let response = erlang_message_name(&call.response);
            format!(
                "    {{{}, {}, {}, {}, \"{}\", \"{}\", \"{}\", \"{}\", [{}]}}{}",
                module,
                erlang_name(&call.method),
                shape_atom(&call.shape),
                proto_module(&call.source),
                request,
                response,
                call.rpc,
                call.request_base64,
                "",
                if std::ptr::eq(call, calls.last().unwrap()) { "" } else { "," }
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn lisp_type(value: &str) -> (String, String) {
    let mut pieces = value.trim_start_matches('.').rsplitn(2, '.');
    let class = lisp_name(pieces.next().unwrap_or(value));
    let package = pieces.next().unwrap_or("").to_ascii_uppercase();
    (package, class)
}

fn lisp_calls(calls: &[Call]) -> String {
    calls
        .iter()
        .map(|call| {
            let request = lisp_type(&call.request);
            let response = lisp_type(&call.response);
            format!(
                "  (\"{}\" \"{}\" \"{}\" \"{}\" \"{}\" \"{}\" \"{}\" \"{}\" \"{}\" \"{}\" [{}])",
                call.package.to_ascii_uppercase(),
                lisp_name(&call.service),
                lisp_name(&call.method),
                if request.0.is_empty() { call.package.to_ascii_uppercase() } else { request.0 },
                request.1,
                if response.0.is_empty() { call.package.to_ascii_uppercase() } else { response.0 },
                response.1,
                call.shape.replace('_', "-"),
                call.rpc,
                call.request_base64,
                ""
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn render(language: Language, authority_path: &Path, typed_path: &Path, output: &Path) -> Result<(), String> {
    let bytes = fs::read(authority_path)
        .map_err(|error| format!("read {}: {error}", authority_path.display()))?;
    let authority: Value = serde_json::from_slice(&bytes)
        .map_err(|error| format!("parse {}: {error}", authority_path.display()))?;
    let typed = typed_records(Some(typed_path))?;
    let calls = calls(&authority, &typed)?;
    let (template, call_text) = match language {
        Language::Elixir => (include_str!("../templates/elixir.tmpl"), elixir_calls(&calls)),
        Language::Erlang => (include_str!("../templates/erlang.tmpl"), erlang_calls(&calls)),
        Language::CommonLisp => (
            include_str!("../templates/common-lisp.tmpl"),
            lisp_calls(&calls),
        ),
    };
    let rendered = template.replace("{{CALLS}}", &call_text);
    if rendered.contains("{{CALLS}}") {
        return Err("runtime-consumer template did not contain a single call slot".into());
    }
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).map_err(|error| format!("create {}: {error}", parent.display()))?;
    }
    fs::write(output, rendered).map_err(|error| format!("write {}: {error}", output.display()))
}


fn authority_methods(authority: &Value) -> Result<Vec<String>, String> {
    if authority.get("schema").and_then(Value::as_str) != Some("acyclic.sdk.rust-authority.v1") {
        return Err("runtime tools require acyclic.sdk.rust-authority.v1".into());
    }
    let families = authority.get("families").and_then(Value::as_array)
        .ok_or_else(|| "authority is missing families".to_string())?;
    let mut methods = Vec::new();
    for family in families {
        let source = required_string(family, "source")?;
        let rpc_methods = family.get("rpc_methods").and_then(Value::as_array)
            .ok_or_else(|| format!("authority family {source} is missing rpc_methods"))?;
        for method in rpc_methods {
            methods.push(required_string(method, "rpc")?.to_string());
        }
    }
    Ok(methods)
}

fn generated_source_files(root: &Path, output: &mut Vec<PathBuf>) -> Result<(), String> {
    for entry in fs::read_dir(root).map_err(|error| format!("read {}: {error}", root.display()))? {
        let entry = entry.map_err(|error| format!("read directory entry in {}: {error}", root.display()))?;
        let path = entry.path();
        if path.is_dir() {
            generated_source_files(&path, output)?;
            continue;
        }
        let extension = path.extension().and_then(|value| value.to_str()).unwrap_or_default();
        if matches!(extension, "ex" | "erl" | "ml" | "lisp" | "lsp") {
            output.push(path);
        }
    }
    Ok(())
}

fn identifier_boundary(source: &str, candidate: &str) -> bool {
    source.match_indices(candidate).any(|(index, _)| {
        let before = source[..index].chars().next_back();
        let after = source[index + candidate.len()..].chars().next();
        let allowed = |value: Option<char>| value.is_some_and(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-'));
        !allowed(before) && !allowed(after)
    })
}

fn callable_identifier(source: &str, extension: &str, candidate: &str) -> bool {
    let patterns = match extension {
        "ex" => vec![
            format!("def {candidate}"),
            format!("defp {candidate}"),
            format!("defmacro {candidate}"),
        ],
        "erl" => vec![format!("{candidate}(")],
        "lisp" | "lsp" => vec![
            format!("(defun {candidate}"),
            format!("(defmethod {candidate}"),
            format!("(defgeneric {candidate}"),
        ],
        "ml" => vec![format!("let {candidate}"), format!("val {candidate}")],
        _ => vec![format!("{candidate}(")],
    };
    patterns.iter().any(|pattern| source.match_indices(pattern).any(|(index, _)| {
        let after = source[index + pattern.len()..].chars().next();
        after.is_none_or(|character| character.is_whitespace() || matches!(character, '(' | ')' | ',' | ':'))
    }))
}

fn service_candidates(rpc: &str) -> Vec<String> {
    let service = rpc
        .split_once('/')
        .map(|(service, _)| service.rsplit_once('.').map(|(_, value)| value).unwrap_or(service))
        .unwrap_or(rpc);
    let snake_name = snake(service);
    vec![service.to_string(), snake_name.clone(), snake_name.replace('_', "-"), lisp_name(service)]
}

fn method_candidates(rpc: &str) -> Vec<String> {
    let method = rpc.rsplit_once('/').map(|(_, value)| value).unwrap_or(rpc);
    let snake_name = snake(method);
    vec![method.to_string(), snake_name.clone(), snake_name.replace('_', "-"), lisp_name(method)]
}

fn callable_service_method(source: &str, extension: &str, rpc: &str) -> bool {
    let code = source
        .lines()
        .filter(|line| {
            let trimmed = line.trim_start();
            !trimmed.starts_with('#')
                && !trimmed.starts_with('%')
                && !trimmed.starts_with(';')
                && !trimmed.starts_with("//")
        })
        .collect::<Vec<_>>()
        .join("\n");
    let service_present = service_candidates(rpc)
        .iter()
        .any(|candidate| identifier_boundary(&code, candidate));
    service_present && method_candidates(rpc)
        .iter()
        .any(|candidate| callable_identifier(&code, extension, candidate))
}

pub fn verify_generated_rpc_coverage(authority_path: &Path, generated_root: &Path) -> Result<usize, String> {
    let authority: Value = serde_json::from_slice(
        &fs::read(authority_path).map_err(|error| format!("read {}: {error}", authority_path.display()))?,
    ).map_err(|error| format!("parse {}: {error}", authority_path.display()))?;
    let methods = authority_methods(&authority)?;
    let mut files = Vec::new();
    generated_source_files(generated_root, &mut files)?;
    files.sort();
    let generated = files.iter().map(|path| {
        let extension = path.extension().and_then(|value| value.to_str()).unwrap_or_default().to_string();
        let source = fs::read_to_string(path).unwrap_or_else(|_| String::from_utf8_lossy(&fs::read(path).unwrap_or_default()).into_owned());
        (extension, source)
    }).collect::<Vec<_>>();
    let mut missing = Vec::new();
    let mut seen = BTreeSet::new();
    for rpc in &methods {
        if !seen.insert(rpc) {
            return Err(format!("Rust authority contains duplicate RPC {rpc}"));
        }
        let present = generated.iter().any(|(extension, source)| callable_service_method(source, extension, rpc));
        if !present {
            missing.push(rpc.to_string());
        }
    }
    if !missing.is_empty() {
        return Err(format!("Generated client surface is missing Rust RPCs: {}", missing.join(", ")));
    }
    Ok(methods.len())
}

fn observation_files(directory: &Path) -> Result<Vec<PathBuf>, String> {
    let mut files = fs::read_dir(directory)
        .map_err(|error| format!("read {}: {error}", directory.display()))?
        .map(|entry| entry.map(|value| value.path()).map_err(|error| format!("read observation entry: {error}")))
        .collect::<Result<Vec<_>, _>>()?;
    files.sort();
    Ok(files)
}

fn digest(path: &Path) -> Result<String, String> {
    let bytes = fs::read(path).map_err(|error| format!("read {}: {error}", path.display()))?;
    let digest = sha2::Sha256::digest(bytes);
    Ok(format!("sha256:{}", digest.iter().map(|byte| format!("{byte:02x}")).collect::<String>()))
}

fn base64_file(path: &Path) -> Result<String, String> {
    Ok(base64::engine::general_purpose::STANDARD.encode(
        fs::read(path).map_err(|error| format!("read {}: {error}", path.display()))?,
    ))
}

fn sexp_value(text: &str, key: &str) -> Option<String> {
    let marker = format!(":{key}");
    let start = text.find(&marker)? + marker.len();
    let rest = text[start..].trim_start().strip_prefix('"')?;
    let mut escaped = false;
    let mut value = String::new();
    for character in rest.chars() {
        if escaped {
            value.push(character);
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character == '"' {
            return Some(value);
        } else {
            value.push(character);
        }
    }
    None
}

fn sexp_status(text: &str) -> Option<i64> {
    let start = text.find(":status")? + ":status".len();
    let token = text[start..].trim_start().split(|character: char| character.is_whitespace() || character == ')').next()?;
    if token.eq_ignore_ascii_case("nil") { None } else { token.parse().ok() }
}

fn observation_metadata(path: &Path) -> Result<Map<String, Value>, String> {
    let text = fs::read_to_string(path).map_err(|error| format!("read {}: {error}", path.display()))?;
    let rpc = sexp_value(&text, "rpc");
    let shape = sexp_value(&text, "shape");
    let execution = sexp_value(&text, "execution");
    let status = sexp_status(&text);
    let terminal_status = if execution.as_deref() == Some("deferred-rust-scenario") {
        "deferred"
    } else if status == Some(0) {
        "ok"
    } else if status.is_some() {
        "error"
    } else {
        "unknown"
    };
    let mut value = Map::new();
    value.insert("rpc".into(), rpc.map(Value::from).unwrap_or(Value::Null));
    value.insert("shape".into(), shape.map(Value::from).unwrap_or(Value::Null));
    value.insert("execution".into(), execution.map(Value::from).unwrap_or(Value::Null));
    value.insert("status".into(), status.map(Value::from).unwrap_or(Value::Null));
    value.insert("terminal_status".into(), Value::from(terminal_status));
    value.insert("terminal_code".into(), status.map(Value::from).unwrap_or(Value::Null));
    Ok(value)
}

pub fn collect_runtime_observation_receipt(project: &Path, output: &Path, source_revision: &str, manifest: &str) -> Result<usize, String> {
    let observation_dir = project.join("runtime-observations");
    let files = observation_files(&observation_dir)?;
    let mut requests = files.iter().filter_map(|path| {
        let name = path.file_name()?.to_str()?;
        let prefix = name.strip_suffix(".request.bin")?;
        prefix.chars().all(|character| character.is_ascii_digit()).then(|| (prefix.to_string(), path.clone()))
    }).collect::<Vec<_>>();
    requests.sort_by_key(|(prefix, _)| prefix.parse::<u64>().unwrap_or(u64::MAX));
    for pair in requests.windows(2) {
        if pair[0].0.parse::<u64>().ok() == pair[1].0.parse::<u64>().ok() {
            return Err(format!("duplicate numeric runtime observation index: {} and {}", pair[0].0, pair[1].0));
        }
    }
    let request_count = requests.len();
    let mut observations = Vec::new();
    for (prefix, request) in requests {
        let request_name = request.file_name().and_then(|value| value.to_str()).ok_or_else(|| "request has no filename".to_string())?.to_string();
        let mut request_frames = vec![request.clone()];
        let request_marker = format!("{prefix}.request.");
        let mut extra_requests = Vec::new();
        for path in &files {
            let Some(name) = path.file_name().and_then(|value| value.to_str()) else { continue };
            if let Some(raw_sequence) = name.strip_prefix(&request_marker).and_then(|value| value.strip_suffix(".bin")) {
                let sequence = raw_sequence.parse::<u64>().map_err(|_| format!("request frame for {prefix} has non-numeric sequence {raw_sequence}"))?;
                extra_requests.push((sequence, path.clone()));
            }
        }
        extra_requests.sort_by_key(|(sequence, _)| *sequence);
        for (expected, (sequence, _)) in extra_requests.iter().enumerate() {
            let expected = expected as u64 + 1;
            if *sequence != expected {
                return Err(format!("request frame sequence for {prefix} is not contiguous at {sequence}, expected {expected}"));
            }
        }
        request_frames.extend(extra_requests.into_iter().map(|(_, path)| path));
        let response_marker = format!("{prefix}.response.");
        let mut response_files = Vec::new();
        for path in &files {
            let Some(name) = path.file_name().and_then(|value| value.to_str()) else { continue };
            if let Some(raw_sequence) = name.strip_prefix(&response_marker).and_then(|value| value.strip_suffix(".bin")) {
                let sequence = raw_sequence.parse::<u64>().map_err(|_| format!("response frame for {prefix} has non-numeric sequence {raw_sequence}"))?;
                response_files.push((sequence, path.clone()));
            }
        }
        response_files.sort_by_key(|(sequence, _)| *sequence);
        for (expected, (sequence, _)) in response_files.iter().enumerate() {
            if *sequence != expected as u64 {
                return Err(format!("response frame sequence for {prefix} is not contiguous at {sequence}, expected {expected}"));
            }
        }
        let response_files = response_files.into_iter().map(|(_, path)| path).collect::<Vec<_>>();
        let mut item = Map::new();
        item.insert("request_file".into(), Value::from(request_name));
        item.insert("request_files".into(), Value::from(request_frames.iter().map(|path| path.file_name().and_then(|value| value.to_str()).unwrap_or_default()).collect::<Vec<_>>()));
        item.insert("request_base64".into(), Value::from(base64_file(&request)?));
        item.insert("request_frames_base64".into(), Value::from(request_frames.iter().map(|path| base64_file(path)).collect::<Result<Vec<_>, _>>()?));
        item.insert("request_sha256".into(), Value::from(digest(&request)?));
        item.insert("response_files".into(), Value::from(response_files.iter().map(|path| path.file_name().and_then(|value| value.to_str()).unwrap_or_default()).collect::<Vec<_>>()));
        item.insert("response_frame_base64".into(), Value::from(response_files.iter().map(|path| base64_file(path)).collect::<Result<Vec<_>, _>>()?));
        item.insert("response_sha256".into(), Value::from(response_files.iter().map(|path| digest(path)).collect::<Result<Vec<_>, _>>()?));
        item.insert("response_frame_count".into(), Value::from(response_files.len()));
        let metadata = observation_dir.join(format!("{prefix}.meta.sexp"));
        if metadata.is_file() {
            item.extend(observation_metadata(&metadata)?);
        }
        if let Some(rpc) = item.get("rpc").and_then(Value::as_str).map(str::to_owned) {
            item.insert("family".into(), Value::from(rpc.split_once('/').map(|(service, _)| service).unwrap_or(rpc.as_str())));
        }
        observations.push(Value::Object(item));
    }
    let env_count = |name: &str, fallback: usize| std::env::var(name).ok().and_then(|value| value.parse().ok()).unwrap_or(fallback);
    let mut payload = Map::new();
    payload.insert("schema".into(), Value::from("acyclic.runtime-consumer-receipt.v1"));
    payload.insert("language".into(), Value::from("common-lisp"));
    payload.insert("source_revision".into(), Value::from(source_revision));
    payload.insert("rust_authority_manifest_sha256".into(), Value::from(manifest));
    payload.insert("observations".into(), Value::from(observations));
    payload.insert("rpc_count".into(), Value::from(request_count));
    payload.insert("current_rpc_count".into(), Value::from(env_count("ACYCLIC_RUST_CURRENT_RPC_COUNT", request_count)));
    payload.insert("archived_rpc_count".into(), Value::from(env_count("ACYCLIC_RUST_ARCHIVED_RPC_COUNT", 0)));
    payload.insert("all_rpc_count".into(), Value::from(env_count("ACYCLIC_RUST_ALL_RPC_COUNT", request_count)));
    payload.insert("inventory_scope".into(), Value::from("current-rust-authority"));
    payload.insert("byte_evidence".into(), Value::from("actual-ag-proto-serialized-request-and-response-files"));
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).map_err(|error| format!("create {}: {error}", parent.display()))?;
    }
    fs::write(output, serde_json::to_string_pretty(&Value::Object(payload)).map_err(|error| format!("serialize receipt: {error}"))? + "\n")
        .map_err(|error| format!("write {}: {error}", output.display()))?;
    Ok(request_count)
}

pub fn parse_language(value: &str) -> Result<Language, String> {
    match value {
        "elixir" => Ok(Language::Elixir),
        "erlang" => Ok(Language::Erlang),
        "common-lisp" | "lisp" => Ok(Language::CommonLisp),
        _ => Err(format!("unsupported runtime consumer language {value}")),
    }
}

pub fn run_from_args(args: &[String]) -> Result<(), String> {
    let mut language: Option<String> = None;
    let mut authority = None;
    let mut output = None;
    let mut typed = None;
    let mut stream_scenarios = None;
    let mut coverage_root = None;
    let mut receipt_project = None;
    let mut source_revision = None;
    let mut manifest_sha256 = None;
    let mut inventory = None;
    let mut receipt = None;
    let mut normalize_output = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--language" => { index += 1; language = args.get(index).cloned(); }
            "--authority" => { index += 1; authority = args.get(index).map(PathBuf::from); }
            "--output" => { index += 1; output = args.get(index).map(PathBuf::from); }
            "--typed-request-manifest" => { index += 1; typed = args.get(index).map(PathBuf::from); }
            "--stream-scenarios" => { index += 1; stream_scenarios = args.get(index).map(PathBuf::from); }
            "--verify-coverage" => { index += 1; coverage_root = args.get(index).map(PathBuf::from); }
            "--collect-receipt" => { index += 1; receipt_project = args.get(index).map(PathBuf::from); }
            "--source-revision" => { index += 1; source_revision = args.get(index).cloned(); }
            "--manifest-sha256" => { index += 1; manifest_sha256 = args.get(index).cloned(); }
            "--inventory" => { index += 1; inventory = args.get(index).map(PathBuf::from); }
            "--receipt" => { index += 1; receipt = args.get(index).map(PathBuf::from); }
            "--normalize-output" => { index += 1; normalize_output = args.get(index).map(PathBuf::from); }
            value => return Err(format!("unknown runtime-consumer argument {value}")),
        }
        index += 1;
    }
    if let Some(generated_root) = coverage_root {
        let authority = authority.ok_or_else(|| "--authority is required for --verify-coverage".to_string())?;
        let count = verify_generated_rpc_coverage(&authority, &generated_root)?;
        println!("verified {count} Rust RPCs in {}", generated_root.display());
        return Ok(());
    }
    if let Some(project) = receipt_project {
        let count = collect_runtime_observation_receipt(
            &project,
            &output.ok_or_else(|| "--output is required for --collect-receipt".to_string())?,
            &source_revision.ok_or_else(|| "--source-revision is required for --collect-receipt".to_string())?,
            &manifest_sha256.ok_or_else(|| "--manifest-sha256 is required for --collect-receipt".to_string())?,
        )?;
        println!("collected {count} runtime observations from {}", project.display());
        return Ok(());
    }
    if inventory.is_some() || receipt.is_some() || normalize_output.is_some() {
        let inventory = inventory.ok_or_else(|| "--inventory, --receipt, and --normalize-output must be supplied together".to_string())?;
        let receipt = receipt.ok_or_else(|| "--inventory, --receipt, and --normalize-output must be supplied together".to_string())?;
        let output = normalize_output.ok_or_else(|| "--inventory, --receipt, and --normalize-output must be supplied together".to_string())?;
        return normalize_receipt(
            &inventory,
            &receipt,
            language.as_deref().ok_or_else(|| "--language is required for receipt normalization".to_string())?,
            &output,
        );
    }
    let authority = authority.ok_or_else(|| "--authority is required".to_string())?;
    let typed = typed.ok_or_else(|| "--typed-request-manifest is required".to_string())?;
    if let Some(output) = stream_scenarios {
        return write_stream_scenarios(&authority, &typed, &output);
    }
    render(
        parse_language(language.as_deref().ok_or_else(|| "--language is required".to_string())?)?,
        &authority,
        &typed,
        &output.ok_or_else(|| "--output is required".to_string())?,
    )
}
