//! Verify captured SDK protobuf frames against the Rust-owned fixture manifest.
use acyclic_sdk_contract_wire::{RpcDirection, compare_family_rpc_message};
use base64::Engine;
use serde_json::{Value, json};
use std::{collections::BTreeMap, env, fs, path::PathBuf};

fn main() -> Result<(), String> {
    let (manifest, observed, source, output) = args()?;
    let manifest = read(&manifest)?;
    let observed = read(&observed)?;
    for (label, value) in [("manifest", &manifest), ("observed", &observed)] {
        if let Some(revision) = value.get("source_revision").and_then(Value::as_str) {
            if revision != source {
                return Err(format!(
                    "{label} source revision differs from --source-git-sha"
                ));
            }
        }
    }
    if manifest.get("complete").and_then(Value::as_bool) == Some(false) {
        return Err("Rust executable manifest is incomplete; streaming scenarios must be executed before qualification".into());
    }
    let expected = records(&manifest, "records")?;
    let actual = records(&observed, "observations")?;
    let mut expected_by_rpc = BTreeMap::new();
    let mut actual_by_rpc = BTreeMap::new();
    for record in expected {
        let rpc = string(&record, "rpc")?.to_owned();
        if expected_by_rpc.insert(rpc.clone(), record).is_some() {
            return Err(format!("duplicate Rust manifest RPC {rpc}"));
        }
    }
    for record in actual {
        let rpc = string(&record, "rpc")?.to_owned();
        if actual_by_rpc.insert(rpc.clone(), record).is_some() {
            return Err(format!("duplicate runtime RPC {rpc}"));
        }
    }
    let mut failures = Vec::new();
    let mut comparisons = 0usize;
    for (rpc, expected) in &expected_by_rpc {
        let Some(actual) = actual_by_rpc.get(rpc) else {
            failures.push(format!("missing runtime RPC {rpc}"));
            continue;
        };
        let family = string(expected, "family")?;
        if string(actual, "family")? != family {
            failures.push(format!(
                "{rpc}: runtime family does not match the Rust manifest"
            ));
        }
        for key in ["terminal_status", "terminal_code"] {
            if actual.get(key).is_none() {
                failures.push(format!("{rpc}: runtime receipt is missing {key}"));
            }
        }
        let expected_request = bytes(expected.get("request_base64"), rpc, "request_base64")?;
        let actual_request = bytes(actual.get("request_base64"), rpc, "request_base64")?;
        compare(
            family,
            rpc,
            "request",
            0,
            RpcDirection::Request,
            &expected_request,
            &actual_request,
            &mut failures,
        );
        comparisons += 1;
        let expected_frames = expected
            .get("response_frames")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let actual_frames = actual
            .get("response_frame_base64")
            .and_then(Value::as_array)
            .ok_or_else(|| format!("{rpc}: runtime receipt is missing response_frame_base64"))?;
        if expected_frames.len() != actual_frames.len() {
            failures.push(format!(
                "{rpc}: expected {} response frames, observed {}",
                expected_frames.len(),
                actual_frames.len()
            ));
            continue;
        }
        for (index, frame) in expected_frames.iter().enumerate() {
            let expected_bytes = bytes(frame.get("response_base64"), rpc, "response_base64")?;
            let actual_bytes = bytes(actual_frames.get(index), rpc, "response_frame_base64")?;
            compare(
                family,
                rpc,
                "response",
                index,
                RpcDirection::Response,
                &expected_bytes,
                &actual_bytes,
                &mut failures,
            );
            comparisons += 1;
        }
    }
    for rpc in actual_by_rpc.keys() {
        if !expected_by_rpc.contains_key(rpc) {
            failures.push(format!("unexpected runtime RPC {rpc}"));
        }
    }
    let status = if failures.is_empty() {
        "passed"
    } else {
        "failed"
    };
    let result = json!({"schema":"acyclic.sdk.rust-semantic-verifier.v2","status":status,"source_git_sha":source,"manifest_record_count":expected_by_rpc.len(),"observed_record_count":actual_by_rpc.len(),"semantic_comparisons":comparisons,"failures":failures});
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).map_err(|error| format!("create output directory: {error}"))?;
    }
    fs::write(
        &output,
        serde_json::to_vec_pretty(&result).map_err(|error| error.to_string())?,
    )
    .map_err(|error| format!("write {}: {error}", output.display()))?;
    if status == "failed" {
        return Err("Rust semantic verifier rejected observations".into());
    }
    Ok(())
}

fn args() -> Result<(PathBuf, PathBuf, String, PathBuf), String> {
    let mut manifest = None;
    let mut observed = None;
    let mut source = None;
    let mut output = None;
    let mut args = env::args_os().skip(1);
    while let Some(arg) = args.next() {
        let slot = match arg.to_string_lossy().as_ref() { "--manifest" => &mut manifest, "--observed" => &mut observed, "--source-git-sha" => &mut source, "--output" => &mut output, _ => return Err("usage: verify-observations --manifest PATH --observed PATH --source-git-sha SHA --output PATH".into()) };
        *slot = Some(arg_value(&mut args)?);
    }
    Ok((
        manifest.ok_or("--manifest is required")?,
        observed.ok_or("--observed is required")?,
        source
            .ok_or("--source-git-sha is required")?
            .to_string_lossy()
            .into_owned(),
        output.ok_or("--output is required")?,
    ))
}

fn arg_value(args: &mut impl Iterator<Item = std::ffi::OsString>) -> Result<PathBuf, String> {
    args.next()
        .map(PathBuf::from)
        .ok_or("argument requires a value".into())
}
fn read(path: &PathBuf) -> Result<Value, String> {
    serde_json::from_slice(
        &fs::read(path).map_err(|error| format!("read {}: {error}", path.display()))?,
    )
    .map_err(|error| format!("decode {}: {error}", path.display()))
}
fn records<'a>(value: &'a Value, key: &str) -> Result<&'a Vec<Value>, String> {
    value
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("JSON root is missing {key} array"))
}
fn string<'a>(value: &'a Value, key: &str) -> Result<&'a str, String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("record is missing {key}"))
}
fn bytes(value: Option<&Value>, rpc: &str, key: &str) -> Result<Vec<u8>, String> {
    let encoded = value
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{rpc} is missing {key}"))?;
    base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|error| format!("{rpc} {key}: {error}"))
}
fn compare(
    family: &str,
    rpc: &str,
    direction: &str,
    index: usize,
    kind: RpcDirection,
    expected: &[u8],
    actual: &[u8],
    failures: &mut Vec<String>,
) {
    if let Err(error) = compare_family_rpc_message(family, rpc, kind, expected, actual) {
        failures.push(format!("{rpc} {direction} frame {index}: {error}"));
    }
}
