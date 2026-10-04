//! Validate a Rust-built Stream N-API package and normalize its installed
//! consumer evidence for the central generation gate.
//!
//! The package builder and the JavaScript consumer own execution. This small
//! Rust tool owns the acceptance boundary: it binds the archive, loader,
//! provenance, and every public Stream RPC scenario to one source revision.

use flate2::read::GzDecoder;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use tar::Archive;

const PROVENANCE_SCHEMA: &str = "acyclic.sdk.stream.native.provenance.v1";
const BUILD_SCHEMA: &str = "acyclic.sdk.stream.native.build.v1";
const CONSUMER_SCHEMA: &str = "acyclic.sdk.stream.native-installed-consumer.v1";
const OUTPUT_SCHEMA: &str = "acyclic.sdk.stream.native.qualification.v1";
const REQUIRED_CHECKS: &[&str] = &[
    "package-install",
    "napi-loader",
    "tls",
    "serialization",
    "cancellation",
    "recovery",
];
const REQUIRED_RPCS: &[&str] = &[
    "acyclic.stream.v2.StreamService/Append",
    "acyclic.stream.v2.StreamService/Read",
    "acyclic.stream.v2.StreamService/InspectIdempotency",
    "acyclic.stream.v2.StreamService/Commit",
    "acyclic.stream.v2.StreamService/ReadCommit",
    "acyclic.stream.v2.StreamService/ChildrenPage",
    "acyclic.stream.v2.StreamService/Follow",
    "acyclic.stream.v2.StreamService/Tail",
];

fn main() {
    if let Err(error) = run() {
        eprintln!("sdk-stream-native-receipt: {error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), String> {
    let mut args = env::args().skip(1);
    if args.next().as_deref() != Some("verify") {
        return Err(usage());
    }
    let mut values = BTreeMap::new();
    while let Some(flag) = args.next() {
        let key = flag.strip_prefix("--").ok_or_else(usage)?.to_owned();
        let value = args.next().ok_or_else(usage)?;
        values.insert(key, value);
    }
    let provenance = PathBuf::from(required(&values, "provenance")?);
    let package_root = PathBuf::from(required(&values, "package-root")?);
    let scenario = PathBuf::from(required(&values, "scenario")?);
    let source_revision = required(&values, "source-revision")?;
    let archive = values.get("archive").map(PathBuf::from);
    let output = PathBuf::from(required(&values, "output")?);
    let receipt = verify(
        &provenance,
        &package_root,
        &scenario,
        archive.as_deref(),
        source_revision,
    )?;
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    fs::write(
        &output,
        serde_json::to_vec_pretty(&receipt).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    println!("{}", output.display());
    Ok(())
}

fn verify(
    provenance_path: &Path,
    package_root: &Path,
    scenario_path: &Path,
    archive_path: Option<&Path>,
    source_revision: &str,
) -> Result<Value, String> {
    let provenance = read_json(provenance_path)?;
    if provenance.get("schema").and_then(Value::as_str) != Some(PROVENANCE_SCHEMA) {
        return Err("native provenance schema mismatch".into());
    }
    expect_revision(&provenance, source_revision, "provenance")?;
    let package_root = package_root
        .canonicalize()
        .map_err(|error| format!("package root: {error}"))?;
    let package_json = read_json(&package_root.join("package.json"))?;
    let build = read_json(&package_root.join("BUILD.json"))?;
    if build.get("schema").and_then(Value::as_str) != Some(BUILD_SCHEMA) {
        return Err("native BUILD.json schema mismatch".into());
    }
    expect_revision(&build, source_revision, "BUILD.json")?;
    let package_name = string_field(&package_json, "name")?;
    let package_version = string_field(&package_json, "version")?;
    if provenance.get("package_manifest").and_then(Value::as_str) != Some(package_name.as_str()) {
        return Err("provenance package name differs from package.json".into());
    }
    if provenance.get("package_version").and_then(Value::as_str) != Some(package_version.as_str()) {
        return Err("provenance package version differs from package.json".into());
    }
    let package_record = build
        .get("package")
        .and_then(Value::as_object)
        .ok_or_else(|| "BUILD.json package record is missing".to_owned())?;
    if package_record.get("name").and_then(Value::as_str) != Some(package_name.as_str())
        || package_record.get("version").and_then(Value::as_str) != Some(package_version.as_str())
    {
        return Err("BUILD.json package identity differs from package.json".into());
    }
    let artifacts = build
        .get("artifacts")
        .and_then(Value::as_object)
        .ok_or_else(|| "BUILD.json artifact record is missing".to_owned())?;
    let build_hash = hash_file(&package_root.join("BUILD.json"))?;
    if provenance.get("build_sha256").and_then(Value::as_str) != Some(build_hash.as_str()) {
        return Err("provenance BUILD.json hash differs from installed BUILD.json".into());
    }
    let binary = verify_file_hash(&package_root, artifacts, "binary", "binary_sha256")?;
    let manifest = verify_file_hash(&package_root, artifacts, "manifest", "manifest_sha256")?;
    let loader = verify_file_hash(&package_root, artifacts, "loader", "loader_sha256")?;
    if fs::read(package_root.join("index.js"))
        .map_err(|error| error.to_string())?
        .windows(b"process.env".len())
        .any(|window| window == b"process.env")
    {
        return Err("native loader reads environment state".into());
    }
    let archive_record = if let Some(path) = archive_path {
        Some(verify_archive(path, &package_root, &package_name)?)
    } else {
        None
    };
    let consumer = read_json(scenario_path)?;
    if consumer.get("schema").and_then(Value::as_str) != Some(CONSUMER_SCHEMA) {
        return Err("installed consumer schema mismatch".into());
    }
    expect_revision(&consumer, source_revision, "installed consumer")?;
    if consumer.get("status").and_then(Value::as_str) != Some("passed")
        || consumer.get("invoked").and_then(Value::as_bool) != Some(true)
        || consumer.get("exit_code").and_then(Value::as_i64) != Some(0)
    {
        return Err("installed consumer did not execute successfully".into());
    }
    let checks = consumer
        .get("checks")
        .and_then(Value::as_array)
        .ok_or_else(|| "installed consumer checks are missing".to_owned())?;
    for required in REQUIRED_CHECKS {
        if !checks.iter().any(|value| value.as_str() == Some(required)) {
            return Err(format!("installed consumer is missing check {required}"));
        }
    }
    if consumer.get("package").and_then(Value::as_str) != Some(package_name.as_str()) {
        return Err("installed consumer package differs from package.json".into());
    }
    let scenarios = consumer
        .get("scenarios")
        .and_then(Value::as_array)
        .ok_or_else(|| "installed consumer scenarios are missing".to_owned())?;
    let mut observed = BTreeSet::new();
    let scenario_root = scenario_path
        .parent()
        .ok_or_else(|| "scenario path has no parent".to_owned())?;
    for item in scenarios {
        let object = item.as_object().ok_or_else(|| "scenario is not an object".to_owned())?;
        let rpc = object
            .get("rpc")
            .and_then(Value::as_str)
            .ok_or_else(|| "scenario RPC is missing".to_owned())?;
        if !REQUIRED_RPCS.contains(&rpc) || !observed.insert(rpc.to_owned()) {
            return Err(format!("unexpected or duplicate Stream RPC scenario: {rpc}"));
        }
        if object.get("family").and_then(Value::as_str) != Some("stream")
            || object.get("status").and_then(Value::as_str) != Some("passed")
        {
            return Err(format!("Stream RPC scenario did not pass: {rpc}"));
        }
        let result_path = object
            .get("output_path")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("scenario output is missing: {rpc}"))?;
        let result_path = scenario_root.join(result_path);
        let result_path = result_path
            .canonicalize()
            .map_err(|error| format!("scenario result path: {error}"))?;
        let scenario_root = scenario_root
            .canonicalize()
            .map_err(|error| format!("scenario root: {error}"))?;
        if result_path.strip_prefix(&scenario_root).is_err() {
            return Err(format!("scenario output escapes its evidence root: {rpc}"));
        }
        let result = read_json(&result_path)?;
        if result.get("schema").and_then(Value::as_str) != Some("acyclic.sdk.rpc-scenario-result.v1")
            || result.get("source_revision").and_then(Value::as_str) != Some(source_revision)
            || result.get("status").and_then(Value::as_str) != Some("passed")
            || result.get("invoked").and_then(Value::as_bool) != Some(true)
            || result.get("exit_code").and_then(Value::as_i64) != Some(0)
        {
            return Err(format!("scenario result is not a passed invocation: {rpc}"));
        }
        let expected_hash = object
            .get("output_sha256")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("scenario output hash is missing: {rpc}"))?;
        if hash_file(&result_path)? != expected_hash {
            return Err(format!("scenario output hash differs: {rpc}"));
        }
    }
    let required: BTreeSet<_> = REQUIRED_RPCS.iter().copied().collect();
    if observed != required {
        return Err(format!("Stream scenario inventory is incomplete: {observed:?}"));
    }
    Ok(json!({
        "schema": OUTPUT_SCHEMA,
        "status": "qualified",
        "capability": "remote-native-stream",
        "source_revision": source_revision,
        "package": {
            "name": package_name,
            "version": package_version,
            "root": package_root.to_string_lossy(),
            "binary_sha256": binary,
            "manifest_sha256": manifest,
            "loader_sha256": loader,
            "archive": archive_record,
        },
        "installed_consumer": {
            "path": scenario_path,
            "resolution": "node_modules package name",
            "checks": checks,
            "scenarios": observed,
        },
        "transport": "grpc",
        "consumer_feature_flags_required": false,
        "environment_transport_override": false,
    }))
}

fn verify_archive(path: &Path, package_root: &Path, package_name: &str) -> Result<Value, String> {
    let bytes = fs::read(path).map_err(|error| format!("archive: {error}"))?;
    let archive_sha256 = hash_bytes(&bytes);
    let file = File::open(path).map_err(|error| error.to_string())?;
    let decoder = GzDecoder::new(file);
    let mut archive = Archive::new(decoder);
    let mut entries = BTreeMap::new();
    for entry in archive.entries().map_err(|error| error.to_string())? {
        let mut entry = entry.map_err(|error| error.to_string())?;
        let name = entry
            .path()
            .map_err(|error| error.to_string())?
            .to_string_lossy()
            .replace('\\', "/");
        let mut content = Vec::new();
        std::io::Read::read_to_end(&mut entry, &mut content).map_err(|error| error.to_string())?;
        entries.insert(name, content);
    }
    let required = ["package.json", "index.js", "BUILD.json", "acyclic_stream_native.node"];
    let mut matched = BTreeSet::new();
    for (name, content) in &entries {
        for required_name in required {
            if name.ends_with(&format!("package/{required_name}")) {
                if content != &fs::read(package_root.join(required_name)).map_err(|error| error.to_string())? {
                    return Err(format!("archive entry differs from installed package: {required_name}"));
                }
                matched.insert(required_name);
            }
        }
    }
    if matched.len() != required.len() {
        return Err(format!("archive for {package_name} is missing package files: {matched:?}"));
    }
    Ok(json!({
        "path": path.to_string_lossy(),
        "sha256": archive_sha256,
        "entries": matched,
    }))
}

fn verify_file_hash(
    root: &Path,
    artifacts: &serde_json::Map<String, Value>,
    path_key: &str,
    hash_key: &str,
) -> Result<String, String> {
    let relative = artifacts
        .get(path_key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("BUILD.json is missing {path_key}"))?;
    let expected = artifacts
        .get(hash_key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("BUILD.json is missing {hash_key}"))?;
    let actual = hash_file(&root.join(relative))?;
    if actual != expected {
        return Err(format!("{relative} hash differs from BUILD.json"));
    }
    Ok(actual)
}

fn expect_revision(value: &Value, expected: &str, label: &str) -> Result<(), String> {
    if value.get("source_revision").and_then(Value::as_str) != Some(expected) {
        return Err(format!("{label} source revision differs from requested revision"));
    }
    Ok(())
}

fn read_json(path: &Path) -> Result<Value, String> {
    serde_json::from_slice(&fs::read(path).map_err(|error| format!("{}: {error}", path.display()))?)
        .map_err(|error| format!("{}: {error}", path.display()))
}

fn string_field(value: &Value, key: &str) -> Result<String, String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| format!("missing non-empty {key}"))
}

fn hash_file(path: &Path) -> Result<String, String> {
    Ok(hash_bytes(&fs::read(path).map_err(|error| format!("{}: {error}", path.display()))?))
}

fn hash_bytes(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    format!("sha256:{:x}", digest.finalize())
}

fn required<'a>(values: &'a BTreeMap<String, String>, key: &str) -> Result<&'a str, String> {
    values.get(key).map(String::as_str).filter(|value| !value.is_empty()).ok_or_else(usage)
}

fn usage() -> String {
    "usage: sdk-stream-native-receipt verify --provenance PATH --package-root PATH --scenario PATH --archive PATH --source-revision REV --output PATH".into()
}

#[cfg(test)]
mod tests {
    use super::{REQUIRED_CHECKS, REQUIRED_RPCS, hash_bytes};
    use std::collections::BTreeSet;

    #[test]
    fn native_gate_requires_the_complete_stream_surface() {
        assert_eq!(REQUIRED_RPCS.len(), 8);
        assert_eq!(
            REQUIRED_RPCS.iter().copied().collect::<BTreeSet<_>>().len(),
            REQUIRED_RPCS.len()
        );
        assert!(REQUIRED_RPCS.iter().any(|rpc| rpc.ends_with("/Follow")));
        assert!(REQUIRED_RPCS.iter().any(|rpc| rpc.ends_with("/InspectIdempotency")));
    }

    #[test]
    fn native_gate_requires_loader_and_recovery_checks() {
        for check in ["napi-loader", "cancellation", "recovery"] {
            assert!(REQUIRED_CHECKS.contains(&check));
        }
    }

    #[test]
    fn hashes_are_content_bound() {
        assert_eq!(
            hash_bytes(b"acyclic"),
            "sha256:319d362f0b301c05e8c303868f7dffc2e516dfcab087ba7d886ca7a9ac8b1aa4"
        );
        assert_ne!(hash_bytes(b"acyclic"), hash_bytes(b"acyclic-stream"));
    }
}
