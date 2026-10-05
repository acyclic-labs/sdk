//! Standalone Rust semantic verifier for live Ruby/PHP/Dart observations.
use acyclic_sdk_contract_wire::{RpcDirection, compare_family_rpc_message, family_rpc_streaming};
use base64::Engine;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::{env, fs, path::PathBuf};

const EXPECTED_SCHEMA: &str = "acyclic.sdk.rpd.rust-authority-consumer-inventory.v1";
const GRPC_STATUS_CODES: std::ops::RangeInclusive<i64> = 0..=16;

fn usage() -> ! {
    eprintln!(
        "usage: verify-rpc-observations --expected PATH --observed PATH --source-git-sha SHA --verifier-sha256 SHA256 --output PATH [--allow-partial]"
    );
    std::process::exit(2);
}

fn valid_sha(value: &str, bytes: usize) -> bool {
    let value = value.strip_prefix("sha256:").unwrap_or(value);
    value.len() == bytes * 2 && value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit())
}

fn args() -> (PathBuf, PathBuf, String, PathBuf, String, bool) {
    let mut expected = None;
    let mut observed = None;
    let mut source = None;
    let mut output = None;
    let mut verifier_sha = None;
    let mut allow_partial = false;
    let mut it = env::args_os().skip(1);
    while let Some(arg) = it.next() {
        match arg.to_string_lossy().as_ref() {
            "--expected" => expected = it.next().map(PathBuf::from),
            "--observed" => observed = it.next().map(PathBuf::from),
            "--source-git-sha" => source = it.next().map(|v| v.to_string_lossy().into_owned()),
            "--output" => output = it.next().map(PathBuf::from),
            "--verifier-sha256" => {
                verifier_sha = it.next().map(|v| v.to_string_lossy().into_owned())
            }
            "--allow-partial" => allow_partial = true,
            _ => usage(),
        }
    }
    match (expected, observed, source, output, verifier_sha) {
        (Some(expected), Some(observed), Some(source), Some(output), Some(verifier_sha)) => {
            if !valid_sha(&verifier_sha, 32) {
                usage();
            }
            (
                expected,
                observed,
                source,
                output,
                verifier_sha,
                allow_partial,
            )
        }
        _ => usage(),
    }
}

fn current_executable_sha256() -> Result<String, String> {
    let path =
        env::current_exe().map_err(|error| format!("resolve verifier executable: {error}"))?;
    let bytes = fs::read(&path)
        .map_err(|error| format!("read verifier executable {}: {error}", path.display()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn object<'a>(value: &'a Value, label: &str) -> Result<&'a Map<String, Value>, String> {
    value
        .as_object()
        .ok_or_else(|| format!("{label} must be an object"))
}

fn string_field<'a>(
    object: &'a Map<String, Value>,
    key: &str,
    label: &str,
) -> Result<&'a str, String> {
    object
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{label}.{key} must be a string"))
}

fn method_key(object: &Map<String, Value>, label: &str) -> Result<String, String> {
    if let Some(rpc) = object.get("rpc").and_then(Value::as_str) {
        return Ok(rpc.trim_start_matches('/').to_owned());
    }
    let package = string_field(object, "package", label)?;
    let service = string_field(object, "service", label)?;
    let method = string_field(object, "method", label)?;
    Ok(format!("{package}.{service}/{method}"))
}

fn contract_family<'a>(entry: &'a Map<String, Value>, label: &str) -> Result<&'a str, String> {
    let family = string_field(entry, "family", label)?;
    let normalized = family.split('/').next().unwrap_or(family);
    match normalized {
        "actors" | "workers" | "objects" | "stream" | "inference" | "machines" | "filesystem"
        | "harness" => Ok(normalized),
        _ => Err(format!(
            "{label}.family {family:?} is not a registered Rust contract family"
        )),
    }
}

fn validate_producer_provenance(
    expected_root: &Map<String, Value>,
    observed_root: &Map<String, Value>,
    observed_schema: &str,
    expected_authority: &Map<String, Value>,
    observed_authority: &Map<String, Value>,
    failures: &mut Vec<String>,
) {
    let Some(language) = observed_schema
        .strip_prefix("acyclic.sdk.rpd.")
        .and_then(|value| value.strip_suffix("-live-receipt.v1"))
    else {
        failures.push("observed.schema does not identify an RPD language".to_owned());
        return;
    };
    let Some(packages) = expected_root.get("packages").and_then(Value::as_object) else {
        failures.push("expected.packages is missing; producer provenance is required".to_owned());
        return;
    };
    let Some(expected_package) = packages.get(language).and_then(Value::as_object) else {
        failures.push(format!("expected.packages.{language} is missing"));
        return;
    };
    let Some(expected_provenance) = expected_package
        .get("provenance")
        .and_then(Value::as_object)
    else {
        failures.push(format!(
            "expected.packages.{language}.provenance is missing"
        ));
        return;
    };
    let Some(observed_package) = observed_root
        .get("executed_package")
        .and_then(Value::as_object)
    else {
        failures.push(
            "observed.executed_package is missing; runtime package provenance is required"
                .to_owned(),
        );
        return;
    };
    let observed_provenance = observed_package
        .get("provenance")
        .and_then(Value::as_object)
        .unwrap_or(observed_package);
    for (field, label) in [
        ("source_git_sha", "source Git revision"),
        ("rust_model_digest", "Rust model digest"),
    ] {
        let expected_value = expected_provenance.get(field).and_then(Value::as_str);
        let observed_value = observed_package
            .get(field)
            .or_else(|| observed_provenance.get(field))
            .and_then(Value::as_str);
        if expected_value.is_none() || observed_value.is_none() {
            failures.push(format!(
                "{language}: executed package {label} provenance is missing"
            ));
        } else if expected_value != observed_value {
            failures.push(format!(
                "{language}: executed package {label} provenance differs from the Rust producer"
            ));
        }
    }
    if expected_provenance.get("source_git_sha") != expected_authority.get("source_git_sha") {
        failures.push(format!(
            "{language}: expected package producer source_git_sha differs from authority"
        ));
    }
    if expected_provenance.get("rust_model_digest") != expected_authority.get("model_digest") {
        failures.push(format!(
            "{language}: expected package producer rust_model_digest differs from authority"
        ));
    }
    if observed_authority.get("source_git_sha") != expected_authority.get("source_git_sha")
        || observed_authority.get("model_digest") != expected_authority.get("model_digest")
    {
        failures.push(format!(
            "{language}: observed authority is not the Rust producer authority"
        ));
    }
    let authority_closure = expected_authority
        .get("source_file_hashes")
        .and_then(Value::as_object);
    let observed_authority_closure = observed_authority
        .get("source_file_hashes")
        .and_then(Value::as_object);
    if authority_closure.is_none() || authority_closure.is_some_and(|closure| closure.is_empty()) {
        failures.push(format!(
            "{language}: expected Rust authority source_file_hashes closure is missing"
        ));
    }
    if observed_authority_closure.is_none()
        || observed_authority_closure.is_some_and(|closure| closure.is_empty())
    {
        failures.push(format!(
            "{language}: observed Rust authority source_file_hashes closure is missing"
        ));
    }
    if authority_closure != observed_authority_closure {
        failures.push(format!(
            "{language}: observed Rust authority source_file_hashes closure differs from expected"
        ));
    }
    let observed_package_closure = observed_provenance
        .get("source_file_hashes")
        .and_then(Value::as_object);
    if observed_package_closure.is_none()
        || observed_package_closure.is_some_and(|closure| closure.is_empty())
    {
        failures.push(format!(
            "{language}: executed package source_file_hashes closure is missing"
        ));
    }
    if observed_package_closure != observed_authority_closure {
        failures.push(format!(
            "{language}: executed package source_file_hashes closure differs from Rust authority"
        ));
    }
    let Some(expected_generator) = expected_provenance
        .get("generator")
        .and_then(Value::as_object)
    else {
        failures.push(format!(
            "{language}: expected producer generator identity is missing"
        ));
        return;
    };
    let Some(observed_generator) = observed_provenance
        .get("generator")
        .and_then(Value::as_object)
    else {
        failures.push(format!(
            "{language}: executed package generator identity is missing"
        ));
        return;
    };
    if expected_generator != observed_generator {
        failures.push(format!(
            "{language}: executed package generator identity differs from the Rust producer record"
        ));
    }
    for field in ["generator_lock_sha256", "schema_inputs_sha256"] {
        let expected_value = expected_provenance.get(field);
        let observed_value = observed_provenance.get(field);
        if expected_value.is_none() || observed_value.is_none() {
            failures.push(format!(
                "{language}: executed package {field} provenance is missing"
            ));
        } else if expected_value != observed_value {
            failures.push(format!(
                "{language}: executed package {field} provenance differs from the producer record"
            ));
        }
    }
    for field in [
        "generator_executable_sha256",
        "toolchain",
        "producer_source_file_hashes",
        "type_policy",
    ] {
        if let Some(expected_value) = expected_provenance.get(field) {
            if observed_provenance.get(field) != Some(expected_value) {
                failures.push(format!("{language}: executed package {field} provenance differs from the producer record"));
            }
        }
    }
}

fn hex_bytes(value: &str, label: &str) -> Result<Vec<u8>, String> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if value.len() % 2 != 0 {
        return Err(format!("{label} must contain an even number of hex digits"));
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    let chars: Vec<_> = value.as_bytes().chunks_exact(2).collect();
    for pair in chars {
        let text = std::str::from_utf8(pair).map_err(|_| format!("{label} is not hexadecimal"))?;
        bytes
            .push(u8::from_str_radix(text, 16).map_err(|_| format!("{label} is not hexadecimal"))?);
    }
    Ok(bytes)
}

fn base64_bytes(value: &str, label: &str) -> Result<Vec<u8>, String> {
    base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|e| format!("{label} is not valid base64: {e}"))
}

fn frame_hexes(value: &Value, label: &str) -> Result<Vec<Vec<u8>>, String> {
    let array = value
        .as_array()
        .ok_or_else(|| format!("{label} must be an array"))?;
    array
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let text = value
                .as_str()
                .ok_or_else(|| format!("{label}[{index}] must be a hex string"))?;
            hex_bytes(text, &format!("{label}[{index}]"))
        })
        .collect()
}

fn expected_request_frames(
    entry: &Map<String, Value>,
    client_streaming: bool,
    label: &str,
) -> Result<Vec<Vec<u8>>, String> {
    let typed = entry
        .get("typed_request")
        .ok_or_else(|| format!("{label}.typed_request is missing"))?;
    let typed = object(typed, &format!("{label}.typed_request"))?;
    if let Some(frames) = typed.get("serialized_frames") {
        let frames = frames
            .as_array()
            .ok_or_else(|| format!("{label}.serialized_frames must be an array"))?;
        if client_streaming {
            if frames.is_empty() {
                return Err(format!(
                    "{label}.serialized_frames must contain at least one frame for a client-streaming RPC"
                ));
            }
        } else if frames.len() != 1 {
            return Err(format!(
                "{label}.serialized_frames must contain exactly one frame for a unary RPC"
            ));
        }
        return frames
            .iter()
            .enumerate()
            .map(|(index, frame)| {
                let frame = object(frame, &format!("{label}.serialized_frames[{index}]"))?;
                let hex = string_field(
                    frame,
                    "serialized_hex",
                    &format!("{label}.serialized_frames[{index}]"),
                )?;
                hex_bytes(
                    hex,
                    &format!("{label}.serialized_frames[{index}].serialized_hex"),
                )
            })
            .collect();
    }
    if client_streaming {
        return Err(format!(
            "{label}.typed_request.serialized_frames is required for a client-streaming RPC"
        ));
    }
    let hex = typed
        .get("serialized_hex")
        .or_else(|| typed.get("empty_serialized_hex"))
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{label}.typed_request serialized bytes are missing"))?;
    Ok(vec![hex_bytes(
        hex,
        &format!("{label}.typed_request.serialized_hex"),
    )?])
}

fn expected_response_frames(
    entry: &Map<String, Value>,
    label: &str,
) -> Result<Vec<Vec<u8>>, String> {
    let typed = entry.get("typed_request").and_then(Value::as_object);
    let frames = typed
        .and_then(|o| o.get("response_frames"))
        .or_else(|| entry.get("rust_response_frames"))
        .or_else(|| entry.get("response_frames"));
    if let Some(frames) = frames {
        let frames = frames
            .as_array()
            .ok_or_else(|| format!("{label}.response_frames must be an array"))?;
        return frames
            .iter()
            .enumerate()
            .map(|(index, frame)| {
                let frame = object(frame, &format!("{label}.response_frames[{index}]"))?;
                let bytes = frame
                    .get("response_base64")
                    .or_else(|| frame.get("base64"))
                    .and_then(Value::as_str)
                    .ok_or_else(|| format!("{label}.response_frames[{index}] bytes are missing"))?;
                base64_bytes(bytes, &format!("{label}.response_frames[{index}]"))
            })
            .collect();
    }
    let bytes = typed
        .and_then(|o| o.get("response_base64"))
        .or_else(|| entry.get("rust_response_base64"))
        .or_else(|| entry.get("response_base64"))
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{label} Rust response bytes are missing"))?;
    Ok(vec![base64_bytes(
        bytes,
        &format!("{label}.response_base64"),
    )?])
}

fn observed_request_frames(
    entry: &Map<String, Value>,
    client_streaming: bool,
    label: &str,
) -> Result<Vec<Vec<u8>>, String> {
    if let Some(frames) = entry.get("request_frames_hex") {
        let parsed = frame_hexes(frames, &format!("{label}.request_frames_hex"))?;
        if client_streaming {
            if parsed.is_empty() {
                return Err(format!(
                    "{label}.request_frames_hex must contain at least one frame for a client-streaming RPC"
                ));
            }
        } else if parsed.len() != 1 {
            return Err(format!(
                "{label}.request_frames_hex must contain exactly one frame for a unary RPC"
            ));
        }
        return Ok(parsed);
    }
    if client_streaming {
        return Err(format!(
            "{label}.request_frames_hex is required for a client-streaming RPC"
        ));
    }
    for key in ["request_bytes_hex", "request_hex"] {
        if let Some(bytes) = entry.get(key).and_then(Value::as_str) {
            return Ok(vec![hex_bytes(bytes, &format!("{label}.{key}"))?]);
        }
    }
    Err(format!("{label} request wire bytes are missing"))
}

fn observed_response_frames(
    entry: &Map<String, Value>,
    streaming: bool,
    allow_empty: bool,
    label: &str,
) -> Result<Vec<Vec<u8>>, String> {
    if streaming {
        let Some(frames) = entry.get("response_frames_hex") else {
            if allow_empty {
                return Ok(Vec::new());
            }
            return Err(format!("{label}.response_frames_hex is missing"));
        };
        return frame_hexes(frames, &format!("{label}.response_frames_hex"));
    }
    let Some(bytes) = entry.get("response_bytes_hex").and_then(Value::as_str) else {
        if allow_empty {
            return Ok(Vec::new());
        }
        return Err(format!("{label}.response_bytes_hex is missing"));
    };
    Ok(vec![hex_bytes(
        bytes,
        &format!("{label}.response_bytes_hex"),
    )?])
}

fn bool_field(object: &Map<String, Value>, key: &str, label: &str) -> Result<bool, String> {
    object
        .get(key)
        .and_then(Value::as_bool)
        .ok_or_else(|| format!("{label}.{key} must be a boolean"))
}

fn required_terminal<'a>(
    entry: &'a Map<String, Value>,
    label: &str,
) -> Result<(&'a str, i64), String> {
    let status = string_field(entry, "terminal_status", label)?;
    let code = entry
        .get("terminal_code")
        .and_then(Value::as_i64)
        .ok_or_else(|| format!("{label}.terminal_code must be an integer"))?;
    if !GRPC_STATUS_CODES.contains(&code) {
        return Err(format!("{label}.terminal_code must be in 0..=16"));
    }
    let status_code_ok = match status {
        "ok" => code == 0,
        "canceled" | "cancelled" => code == 1,
        "error" => (2..=16).contains(&code),
        _ => false,
    };
    if !status_code_ok {
        return Err(format!(
            "{label}.terminal_status {status:?} conflicts with terminal_code {code}"
        ));
    }
    Ok((status, code))
}

fn canonical_status(status: &str) -> &str {
    if status == "cancelled" {
        "canceled"
    } else {
        status
    }
}

pub fn verify_paths(
    expected_path: &PathBuf,
    observed_path: &PathBuf,
    source_sha: &str,
    verifier_sha: &str,
    allow_partial: bool,
) -> Result<Value, String> {
    let actual_verifier_sha = current_executable_sha256()?;
    let declared_verifier_sha = verifier_sha.strip_prefix("sha256:").unwrap_or(verifier_sha);
    if declared_verifier_sha.to_ascii_lowercase() != actual_verifier_sha {
        return Err(format!(
            "--verifier-sha256 does not match the running executable: declared {verifier_sha}, actual {actual_verifier_sha}"
        ));
    }
    let expected_bytes = fs::read(&expected_path)
        .map_err(|error| format!("read {}: {error}", expected_path.display()))?;
    let observed_bytes = fs::read(&observed_path)
        .map_err(|error| format!("read {}: {error}", observed_path.display()))?;
    let expected_input_sha256 = format!("sha256:{:x}", Sha256::digest(&expected_bytes));
    let observed_input_sha256 = format!("sha256:{:x}", Sha256::digest(&observed_bytes));
    let expected = serde_json::from_slice(&expected_bytes)
        .map_err(|error| format!("decode {}: {error}", expected_path.display()))?;
    let observed = serde_json::from_slice(&observed_bytes)
        .map_err(|error| format!("decode {}: {error}", observed_path.display()))?;
    let expected_root = object(&expected, "expected")?;
    let observed_root = object(&observed, "observed")?;
    if expected_root.get("schema").and_then(Value::as_str) != Some(EXPECTED_SCHEMA) {
        return Err(format!("expected.schema must be {EXPECTED_SCHEMA}"));
    }
    let observed_schema = observed_root
        .get("schema")
        .and_then(Value::as_str)
        .ok_or("observed.schema is missing")?;
    if !observed_schema.starts_with("acyclic.sdk.rpd.")
        || !observed_schema.ends_with("-live-receipt.v1")
    {
        return Err(
            "observed.schema must be an acyclic.sdk.rpd.*-live-receipt.v1 receipt".to_owned(),
        );
    }
    let expected_complete = expected_root.get("complete").and_then(Value::as_bool) == Some(true);
    if !expected_complete && !allow_partial {
        return Err(
            "expected.complete must be true; partial inventories cannot qualify".to_owned(),
        );
    }
    let expected_count = expected_root
        .get("method_count")
        .and_then(Value::as_u64)
        .ok_or("expected.method_count is missing")? as usize;
    let expected_authority = object(
        expected_root
            .get("authority")
            .ok_or("expected.authority is missing")?,
        "expected.authority",
    )?;
    let observed_authority = object(
        observed_root
            .get("authority")
            .ok_or("observed.authority is missing")?,
        "observed.authority",
    )?;
    let expected_sha = string_field(expected_authority, "source_git_sha", "expected.authority")?;
    let observed_sha = string_field(observed_authority, "source_git_sha", "observed.authority")?;
    let mut failures = Vec::new();
    if !valid_sha(expected_sha, 20) {
        failures.push("expected authority source_git_sha is not a 40-digit SHA-1".to_owned());
    }
    if !valid_sha(observed_sha, 20) {
        failures.push("observed authority source_git_sha is not a 40-digit SHA-1".to_owned());
    }
    for (authority, label) in [
        (&expected_authority, "expected.authority"),
        (&observed_authority, "observed.authority"),
    ] {
        let hashes = authority
            .get("source_file_hashes")
            .and_then(Value::as_object);
        if hashes.map_or(true, |hashes| hashes.is_empty()) {
            failures.push(format!(
                "{label}.source_file_hashes must be a non-empty source closure"
            ));
        } else if let Some(hashes) = hashes {
            for (path, digest) in hashes {
                if digest
                    .as_str()
                    .map_or(true, |digest| !valid_sha(digest, 32))
                {
                    failures.push(format!(
                        "{label}.source_file_hashes[{path:?}] is not a SHA-256"
                    ));
                }
            }
        }
    }
    if expected_authority.get("model_digest") != observed_authority.get("model_digest") {
        failures.push("expected and observed authority model_digest differ".to_owned());
    }
    if expected_authority.get("source_file_hashes") != observed_authority.get("source_file_hashes")
    {
        failures.push("expected and observed source_file_hashes differ".to_owned());
    }
    if expected_sha != source_sha {
        failures.push(format!(
            "expected authority source_git_sha {expected_sha} differs from CLI {source_sha}"
        ));
    }
    if observed_sha != source_sha {
        failures.push(format!(
            "observed authority source_git_sha {observed_sha} differs from CLI {source_sha}"
        ));
    }
    validate_producer_provenance(
        expected_root,
        observed_root,
        observed_schema,
        expected_authority,
        observed_authority,
        &mut failures,
    );
    let expected_methods = expected_root
        .get("methods")
        .and_then(Value::as_array)
        .ok_or("expected.methods is missing")?;
    let expected_execution_plan = expected_root
        .get("execution_plan")
        .and_then(Value::as_array)
        .filter(|plan| !plan.is_empty());
    let expected_sequence = expected_execution_plan.unwrap_or(expected_methods);
    let ordered_execution = expected_execution_plan.is_some();
    let observed_methods = observed_root
        .get("methods")
        .and_then(Value::as_array)
        .ok_or("observed.methods is missing")?;
    if expected_count != expected_methods.len() {
        failures.push("expected.method_count differs from expected.methods length".to_owned());
    }
    if observed_root
        .get("method_count")
        .and_then(Value::as_u64)
        .map(|count| count as usize)
        != Some(observed_methods.len())
    {
        failures.push("observed.method_count differs from observed.methods length".to_owned());
    }
    if expected_sequence.is_empty() {
        failures.push("expected execution sequence must not be empty".to_owned());
    }
    if observed_methods.is_empty() {
        failures.push("observed.methods must not be empty".to_owned());
    }
    if observed_methods.len() != expected_sequence.len() {
        failures.push(format!(
            "observed execution count {} differs from Rust sequence {}",
            observed_methods.len(),
            expected_sequence.len()
        ));
    }
    let mut observed_by_key = std::collections::BTreeMap::new();
    if !ordered_execution {
        for (index, raw) in observed_methods.iter().enumerate() {
            let entry = object(raw, &format!("observed.methods[{index}]"))?;
            let key = method_key(entry, &format!("observed.methods[{index}]"))?;
            if observed_by_key.insert(key.clone(), entry).is_some() {
                failures.push(format!("duplicate observed RPC {key}"));
            }
        }
    }
    let mut comparisons = 0usize;
    let mut expected_by_key = std::collections::BTreeMap::new();
    for (index, raw) in expected_methods.iter().enumerate() {
        let entry = object(raw, &format!("expected.methods[{index}]"))?;
        let key = method_key(entry, &format!("expected.methods[{index}]"))?;
        if expected_by_key.insert(key.clone(), entry).is_some() {
            failures.push(format!("duplicate expected RPC {key}"));
        }
    }
    if !ordered_execution
        && expected_by_key
            .keys()
            .any(|key| !observed_by_key.contains_key(key))
    {
        for key in expected_by_key
            .keys()
            .filter(|key| !observed_by_key.contains_key(*key))
        {
            failures.push(format!("missing observed RPC {key}"));
        }
    }
    if !ordered_execution
        && observed_by_key
            .keys()
            .any(|key| !expected_by_key.contains_key(key))
    {
        for key in observed_by_key
            .keys()
            .filter(|key| !expected_by_key.contains_key(*key))
        {
            failures.push(format!("extra observed RPC {key}"));
        }
    }
    for (index, raw) in expected_sequence.iter().enumerate() {
        let sequence_entry = object(raw, &format!("expected.execution[{index}]"))?;
        let key = method_key(sequence_entry, &format!("expected.execution[{index}]"))?;
        let expected_entry = expected_by_key.get(&key).copied().unwrap_or(sequence_entry);
        let observed_entry = if ordered_execution {
            observed_methods
                .get(index)
                .map(|raw| object(raw, &format!("observed.methods[{index}]")))
                .transpose()?
        } else {
            observed_by_key.get(&key).copied()
        };
        let Some(observed_entry) = observed_entry else {
            failures.push(format!("missing observed execution step {index} ({key})"));
            continue;
        };
        if method_key(observed_entry, &format!("observed.methods[{index}]"))? != key {
            failures.push(format!(
                "execution step {index}: observed RPC does not match Rust sequence ({key})"
            ));
            continue;
        }
        let family = match contract_family(expected_entry, &format!("expected.methods[{index}]")) {
            Ok(value) => value,
            Err(error) => {
                failures.push(error);
                continue;
            }
        };
        let observed_family = match contract_family(observed_entry, &format!("observed.{key}")) {
            Ok(value) => value,
            Err(error) => {
                failures.push(error);
                continue;
            }
        };
        if observed_family != family {
            failures.push(format!(
                "{key}: observed family does not match Rust registry family"
            ));
        }
        let declared_client_streaming = match bool_field(expected_entry, "client_streaming", &key) {
            Ok(value) => value,
            Err(error) => {
                failures.push(error);
                continue;
            }
        };
        let declared_server_streaming = match bool_field(expected_entry, "server_streaming", &key) {
            Ok(value) => value,
            Err(error) => {
                failures.push(error);
                continue;
            }
        };
        let shape = match family_rpc_streaming(family, &key) {
            Ok(value) => value,
            Err(error) => {
                failures.push(format!(
                    "{key}: Rust descriptor streaming lookup failed: {error}"
                ));
                continue;
            }
        };
        if declared_client_streaming != shape.client_streaming {
            failures.push(format!(
                "{key}: manifest client_streaming disagrees with the Rust descriptor"
            ));
        }
        if declared_server_streaming != shape.server_streaming {
            failures.push(format!(
                "{key}: manifest server_streaming disagrees with the Rust descriptor"
            ));
        }
        let client_streaming = shape.client_streaming;
        let streaming = shape.server_streaming;
        let request_expected = match expected_request_frames(expected_entry, client_streaming, &key)
        {
            Ok(value) => value,
            Err(error) => {
                failures.push(error);
                continue;
            }
        };
        let request_observed = match observed_request_frames(observed_entry, client_streaming, &key)
        {
            Ok(value) => value,
            Err(error) => {
                failures.push(error);
                continue;
            }
        };
        if request_expected.len() != request_observed.len() {
            failures.push(format!("{key}: request frame count differs"));
        }
        for (frame_index, (expected_bytes, observed_bytes)) in
            request_expected.iter().zip(&request_observed).enumerate()
        {
            if let Err(error) = compare_family_rpc_message(
                family,
                &key,
                RpcDirection::Request,
                expected_bytes,
                observed_bytes,
            ) {
                failures.push(format!("{key} request frame {frame_index}: {error}"));
            }
            comparisons += 1;
        }
        if streaming && observed_entry.get("response_bytes_hex").is_some() {
            failures.push(format!(
                "{key}: server-streaming registry conflicts with unary response_bytes_hex"
            ));
        }
        if !streaming && observed_entry.get("response_frames_hex").is_some() {
            failures.push(format!(
                "{key}: unary registry conflicts with ordered response_frames_hex"
            ));
        }
        let expected_terminal = match required_terminal(expected_entry, &key) {
            Ok(value) => value,
            Err(error) => {
                failures.push(error);
                continue;
            }
        };
        let observed_terminal = match required_terminal(observed_entry, &key) {
            Ok(value) => value,
            Err(error) => {
                failures.push(error);
                continue;
            }
        };
        if canonical_status(expected_terminal.0) != canonical_status(observed_terminal.0)
            || expected_terminal.1 != observed_terminal.1
        {
            failures.push(format!(
                "{key}: terminal_status/terminal_code differs from Rust"
            ));
        }
        let expected_error = expected_terminal.0 == "error";
        let response_expected = match expected_response_frames(expected_entry, &key) {
            Ok(value) => value,
            Err(_error) if expected_error => Vec::new(),
            Err(error) => {
                failures.push(error);
                continue;
            }
        };
        let response_observed =
            match observed_response_frames(observed_entry, streaming, expected_error, &key) {
                Ok(value) => value,
                Err(error) => {
                    failures.push(error);
                    continue;
                }
            };
        if response_expected.len() != response_observed.len() {
            failures.push(format!("{key}: response frame count differs"));
        }
        for (frame_index, (expected_bytes, observed_bytes)) in
            response_expected.iter().zip(&response_observed).enumerate()
        {
            if let Err(error) = compare_family_rpc_message(
                family,
                &key,
                RpcDirection::Response,
                expected_bytes,
                observed_bytes,
            ) {
                failures.push(format!("{key} response frame {frame_index}: {error}"));
            }
            comparisons += 1;
        }
    }
    let status = if !failures.is_empty() {
        "failed"
    } else if !expected_complete {
        "partial"
    } else {
        "passed"
    };
    let qualification = match status {
        "passed" => "qualified",
        "partial" => "unqualified",
        _ => "rejected",
    };
    let result = json!({
        "schema":"acyclic.sdk.rpd.rust-semantic-verifier.v1",
        "status":status,
        "qualification":qualification,
        "source_git_sha":source_sha,
        "verifier_sha256":actual_verifier_sha,
        "expected_input_sha256":expected_input_sha256,
        "observed_input_sha256":observed_input_sha256,
        "method_count":expected_sequence.len(),
        "semantic_comparisons":comparisons,
        "failures":failures
    });
    if status == "failed" {
        return Err("Rust semantic verifier rejected observations".to_owned());
    }
    if status == "partial" {
        return Err("Rust semantic verifier produced an unqualified partial result".to_owned());
    }
    Ok(result)
}

fn main() -> Result<(), String> {
    let (expected_path, observed_path, source_sha, output_path, verifier_sha, allow_partial) =
        args();
    let result = verify_paths(
        &expected_path,
        &observed_path,
        &source_sha,
        &verifier_sha,
        allow_partial,
    )?;
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("create {}: {error}", parent.display()))?;
    }
    fs::write(
        &output_path,
        serde_json::to_vec_pretty(&result).map_err(|error| format!("encode result: {error}"))?,
    )
    .map_err(|error| format!("write {}: {error}", output_path.display()))?;
    Ok(())
}
