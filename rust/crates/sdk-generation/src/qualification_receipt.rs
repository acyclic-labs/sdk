//! Rust-owned qualification receipt writer.
//!
//! Language producers may record their real consumer invocations in a small
//! scenario log and hand that log to this writer. The writer reads the
//! generated authority and generation manifest, verifies every referenced
//! byte and invocation result, and derives the family/RPC inventory itself.
//! It deliberately has no mode for supplying families or marking a scenario
//! passed by command line.

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const RECEIPT_SCHEMA: &str = "acyclic.sdk.qualification.receipt.v1";
const SCENARIO_LOG_SCHEMA: &str = "acyclic.sdk.rpc-scenario-log.v1";
const SCENARIO_RESULT_SCHEMA: &str = "acyclic.sdk.rpc-scenario-result.v1";
const SOURCE_REVISION_KIND: &str = "git-oid";

#[derive(Debug, Clone)]
pub struct Options {
    pub source_root: PathBuf,
    pub output: PathBuf,
    pub scenario_log: PathBuf,
    pub language: String,
    pub tool: String,
    pub suite: String,
}

#[derive(Debug)]
struct ScenarioEvidence {
    family: String,
    execution_mode: String,
    checks: BTreeSet<String>,
}

pub fn write(options: &Options) -> Result<PathBuf, String> {
    let source_root = canonical_dir(&options.source_root, "source root")?;
    let output = canonical_dir(&options.output, "generation output")?;
    let git_root = canonical_dir(&git_toplevel(&source_root)?, "Git source root")?;
    if git_root != source_root {
        return Err(format!(
            "Git source root {} does not match requested source root {}",
            git_root.display(),
            source_root.display()
        ));
    }
    let source_revision = git_head(&source_root)?;
    if !git_status(&source_root)?.is_empty() {
        return Err("source checkout is dirty; receipts require a clean Git source revision".into());
    }

    let generation = read_json(&output.join("sdk-generation-manifest.json"))?;
    if generation.get("schema").and_then(Value::as_str)
        != Some("acyclic.sdk.generation.manifest.v1")
    {
        return Err("generation manifest has an unexpected schema".into());
    }
    let source = generation
        .get("source")
        .and_then(Value::as_object)
        .ok_or_else(|| "generation manifest has no source identity".to_owned())?;
    let manifest_revision = nonempty_string(source, "revision")?;
    let contract_digest = nonempty_string(source, "digest")?;
    if !is_sha256(&contract_digest) {
        return Err("generation manifest source.digest is not a SHA-256 digest".into());
    }
    if manifest_revision != source_revision {
        return Err(format!(
            "generation manifest Git revision {manifest_revision} does not match HEAD {source_revision}"
        ));
    }
    let artifact_digest = string_field(&generation, "artifact_digest")?;
    if !is_sha256(&artifact_digest) {
        return Err("generation manifest artifact_digest is not a SHA-256 digest".into());
    }
    let artifacts = artifact_index(&generation)?;
    let authority = authority_inventory(&output.join("wire/rust-authority.json"))?;

    let log = read_json(&options.scenario_log)?;
    if log.get("schema").and_then(Value::as_str) != Some(SCENARIO_LOG_SCHEMA) {
        return Err("scenario log has an unexpected schema".into());
    }
    if log.get("source_revision").and_then(Value::as_str) != Some(source_revision.as_str()) {
        return Err("scenario log is bound to a different Git source revision".into());
    }
    let consumer = log
        .get("consumer")
        .and_then(Value::as_object)
        .ok_or_else(|| "scenario log has no consumer object".to_owned())?;
    let consumer_name = nonempty_string(consumer, "name")?;
    let consumer_version = nonempty_string(consumer, "version")?;
    let consumer_path = portable_consumer_path(nonempty_string(consumer, "artifact_path")?)?;
    let consumer_digest = nonempty_string(consumer, "artifact_sha256")?;
    if !is_sha256(&consumer_digest) {
        return Err("consumer artifact_sha256 is not a SHA-256 digest".into());
    }
    let mut seen_paths = BTreeSet::from([consumer_path.clone()]);
    verify_artifact(
        &output,
        &artifacts,
        &consumer_path,
        &consumer_digest,
        "consumer artifact",
    )?;

    let scenario_values = log
        .get("scenarios")
        .and_then(Value::as_array)
        .ok_or_else(|| "scenario log has no scenarios array".to_owned())?;
    if scenario_values.is_empty() {
        return Err("scenario log must contain at least one invocation".into());
    }
    let mut scenarios = Vec::with_capacity(scenario_values.len());
    let mut evidence = Vec::with_capacity(scenario_values.len());
    let mut observed = BTreeMap::<String, BTreeMap<String, String>>::new();
    let mut seen = BTreeSet::new();
    for entry in scenario_values {
        let output_path = portable_consumer_path(nonempty_string(entry, "output_path")?)?;
        let output_digest = nonempty_string(entry, "output_sha256")?;
        if !is_sha256(&output_digest) || !seen_paths.insert(output_path.clone()) {
            return Err(format!("scenario output {output_path} has an invalid or duplicate path/hash"));
        }
        verify_artifact(
            &output,
            &artifacts,
            &output_path,
            &output_digest,
            "scenario result",
        )?;
        let result = read_output_json(&output, &output_path, "scenario result")?;
        if result.get("schema").and_then(Value::as_str) != Some(SCENARIO_RESULT_SCHEMA)
            || result.get("source_revision").and_then(Value::as_str)
                != Some(source_revision.as_str())
            || result.get("status").and_then(Value::as_str) != Some("passed")
            || result.get("invoked").and_then(Value::as_bool) != Some(true)
            || result.get("exit_code").and_then(Value::as_i64) != Some(0)
        {
            return Err(format!("scenario result {} is not an invoked exit-0 pass", output_path));
        }
        let family = nonempty_string(&result, "family")?;
        let rpc = nonempty_string(&result, "rpc")?;
        let shape = nonempty_string(&result, "shape")?;
        if !matches!(shape.as_str(), "unary" | "client" | "server" | "bidi") {
            return Err(format!("scenario {family}/{rpc} has an invalid RPC shape"));
        }
        let authority_shape = authority
            .get(&family)
            .and_then(|methods| methods.get(&rpc))
            .ok_or_else(|| format!("scenario {family}/{rpc} is absent from Rust authority"))?;
        if authority_shape != &shape {
            return Err(format!("scenario {family}/{rpc} has shape {shape}, authority says {authority_shape}"));
        }
        let transport = nonempty_string(&result, "transport")?;
        if !matches!(transport.as_str(), "grpc" | "http" | "http-json" | "grpc-web") {
            return Err(format!("scenario {family}/{rpc} has an unknown transport"));
        }
        let execution_mode = nonempty_string(&result, "execution_mode")?;
        if !matches!(execution_mode.as_str(), "remote" | "in-process") {
            return Err(format!("scenario {family}/{rpc} has an unknown execution mode"));
        }
        let checks = result
            .get("checks")
            .and_then(Value::as_array)
            .ok_or_else(|| format!("scenario {family}/{rpc} has no checks"))?;
        let mut check_set = BTreeSet::new();
        for check in checks {
            let check = check
                .as_str()
                .ok_or_else(|| format!("scenario {family}/{rpc} has a non-string check"))?;
            if !matches!(
                check,
                "invocation" | "transport" | "serialization" | "cancellation" | "recovery"
            )
                || !check_set.insert(check.to_owned())
            {
                return Err(format!("scenario {family}/{rpc} has invalid or duplicate checks"));
            }
        }
        if !check_set.contains("invocation")
            || !check_set.contains("transport")
            || !check_set.contains("serialization")
        {
            return Err(format!("scenario {family}/{rpc} lacks invocation or transport evidence"));
        }
        if !seen.insert((family.clone(), rpc.clone())) {
            return Err(format!("scenario {family}/{rpc} is duplicated"));
        }
        observed
            .entry(family.clone())
            .or_default()
            .insert(rpc, shape);
        evidence.push(ScenarioEvidence {
            family: family.clone(),
            execution_mode,
            checks: check_set,
        });
        scenarios.push(json!({
            "family": family,
            "rpc": result.get("rpc"),
            "shape": result.get("shape"),
            "execution_mode": result.get("execution_mode"),
            "status": "passed",
            "output_path": output_path,
            "output_sha256": output_digest,
        }));
    }
    let service_authority = authority
        .iter()
        .filter(|(_, methods)| !methods.is_empty())
        .map(|(family, methods)| (family.clone(), methods.clone()))
        .collect::<BTreeMap<_, _>>();
    if observed != service_authority {
        return Err("scenario log does not cover exactly the Rust authority RPC inventory".into());
    }
    if evidence.iter().any(|scenario| scenario.execution_mode != "remote") {
        return Err("remote qualification requires execution_mode=remote for every scenario".into());
    }

    let families = authority
        .iter()
        .map(|(family, methods)| {
            let mut features = BTreeSet::from(["serialization".to_owned(), "transport".to_owned()]);
            for scenario in evidence.iter().filter(|scenario| scenario.family == *family) {
                for check in &scenario.checks {
                    if matches!(check.as_str(), "cancellation" | "recovery") {
                        features.insert(check.clone());
                    }
                }
            }
            Ok(json!({
                "family": family,
                "methods": methods.keys().collect::<Vec<_>>(),
                "features": features.into_iter().collect::<Vec<_>>(),
                "rpc_shapes": methods.values().collect::<Vec<_>>(),
            }))
        })
        .collect::<Result<Vec<_>, String>>()?;

    let tool = if options.tool.trim().is_empty() {
        return Err("tool must not be empty".into());
    } else {
        options.tool.clone()
    };
    let suite = if options.suite.trim().is_empty() {
        return Err("suite must not be empty".into());
    } else {
        options.suite.clone()
    };
    let receipt = json!({
        "schema": RECEIPT_SCHEMA,
        "tool": tool,
        "language": options.language,
        "capability": "remote",
        "source_revision": source_revision,
        "source_revision_kind": SOURCE_REVISION_KIND,
        "contract_digest": contract_digest,
        "artifact_digest": artifact_digest,
        "status": "passed",
        "exit_code": 0,
        "suite": suite,
        "assertions": scenarios.len(),
        "consumer": {
            "executed": true,
            "name": consumer_name,
            "version": consumer_version,
            "source_revision": source_revision,
            "artifact_path": consumer_path,
            "artifact_sha256": consumer_digest,
            "scenarios": scenarios,
        },
        "families": families,
    });
    let receipt_path = output.join("qualification/receipts").join(format!("{}.json", options.language));
    if !is_safe_language_id(&options.language) {
        return Err("language must be a portable identifier".into());
    }
    fs::create_dir_all(receipt_path.parent().expect("receipt parent"))
        .map_err(|error| error.to_string())?;
    let bytes = serde_json::to_vec_pretty(&receipt).map_err(|error| error.to_string())?;
    fs::write(&receipt_path, bytes).map_err(|error| error.to_string())?;
    Ok(receipt_path)
}

fn read_json(path: &Path) -> Result<Value, String> {
    let bytes = fs::read(path).map_err(|error| format!("{}: {error}", path.display()))?;
    serde_json::from_slice(&bytes).map_err(|error| format!("{}: {error}", path.display()))
}

fn string_field(value: &Value, field: &str) -> Result<String, String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| format!("missing non-empty string field {field}"))
}

fn nonempty_string(value: &impl JsonObject, field: &str) -> Result<String, String> {
    value.string_field(field)
}

trait JsonObject {
    fn string_field(&self, field: &str) -> Result<String, String>;
}

impl JsonObject for Value {
    fn string_field(&self, field: &str) -> Result<String, String> {
        string_field(self, field)
    }
}

impl JsonObject for serde_json::Map<String, Value> {
    fn string_field(&self, field: &str) -> Result<String, String> {
        self.get(field)
            .and_then(Value::as_str)
            .filter(|text| !text.trim().is_empty())
            .map(str::to_owned)
            .ok_or_else(|| format!("missing non-empty string field {field}"))
    }
}

fn artifact_index(manifest: &Value) -> Result<BTreeMap<String, (String, u64)>, String> {
    let mut index = BTreeMap::new();
    for artifact in manifest
        .get("artifacts")
        .and_then(Value::as_array)
        .ok_or_else(|| "generation manifest has no artifacts".to_owned())?
    {
        let path = string_field(artifact, "path")?;
        let digest = string_field(artifact, "sha256")?;
        if !is_sha256(&digest) {
            return Err(format!("artifact {path} has an invalid SHA-256 digest"));
        }
        let bytes = artifact
            .get("bytes")
            .and_then(Value::as_u64)
            .ok_or_else(|| format!("artifact {path} bytes is not an integer"))?;
        if index.insert(path.clone(), (digest, bytes)).is_some() {
            return Err(format!("generation manifest contains duplicate artifact {path}"));
        }
    }
    Ok(index)
}

fn verify_artifact(
    output: &Path,
    artifacts: &BTreeMap<String, (String, u64)>,
    path: &str,
    digest: &str,
    label: &str,
) -> Result<(), String> {
    let Some((manifest_digest, bytes)) = artifacts.get(path) else {
        return Err(format!("{label} {path} is absent from the generation manifest"));
    };
    if manifest_digest != digest {
        return Err(format!("{label} {path} digest does not match the generation manifest"));
    }
    let data = read_output_file(output, path, label)?;
    if data.len() as u64 != *bytes || sha256(&data) != digest {
        return Err(format!("{label} {path} has stale or forged bytes"));
    }
    Ok(())
}

fn read_output_json(output: &Path, relative: &str, label: &str) -> Result<Value, String> {
    let bytes = read_output_file(output, relative, label)?;
    serde_json::from_slice(&bytes).map_err(|error| format!("{label} {relative}: {error}"))
}

fn read_output_file(output: &Path, relative: &str, label: &str) -> Result<Vec<u8>, String> {
    let path = output.join(relative);
    let metadata = fs::symlink_metadata(&path)
        .map_err(|error| format!("{label} {}: {error}", path.display()))?;
    if !metadata.file_type().is_file() {
        return Err(format!("{label} {} is not a regular file", path.display()));
    }
    let canonical = fs::canonicalize(&path)
        .map_err(|error| format!("{label} {}: {error}", path.display()))?;
    let canonical = PathBuf::from(canonical.to_string_lossy().trim_start_matches("\\\\?\\"));
    if canonical.strip_prefix(output).is_err() {
        return Err(format!("{label} {} escapes the output tree", path.display()));
    }
    fs::read(&path).map_err(|error| format!("{label} {}: {error}", path.display()))
}

fn authority_inventory(path: &Path) -> Result<BTreeMap<String, BTreeMap<String, String>>, String> {
    let value = read_json(path)?;
    let mut inventory = BTreeMap::new();
    for family in value
        .get("families")
        .and_then(Value::as_array)
        .ok_or_else(|| "authority manifest has no families".to_owned())?
    {
        let source = string_field(family, "source")?;
        let family_name = source
            .split('/')
            .next()
            .ok_or_else(|| "authority family source has no family name".to_owned())?
            .to_owned();
        let mut methods = BTreeMap::new();
        for method in family
            .get("rpc_methods")
            .and_then(Value::as_array)
            .ok_or_else(|| format!("authority family {family_name} has no RPC methods"))?
        {
            let rpc = string_field(method, "rpc")?;
            let shape = string_field(method, "shape")?;
            if methods.insert(rpc.clone(), shape).is_some() {
                return Err(format!("authority family {family_name} contains duplicate RPC {rpc}"));
            }
        }
        if inventory.insert(family_name.clone(), methods).is_some() {
            return Err(format!("authority contains duplicate family {family_name}"));
        }
    }
    Ok(inventory)
}

fn portable_consumer_path(path: String) -> Result<String, String> {
    if !path.starts_with("qualification/consumers/")
        || path.contains('\\')
        || path.split('/').any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(format!("path {path} must be a portable consumer artifact path"));
    }
    Ok(path)
}

fn is_safe_language_id(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
}

fn canonical_dir(path: &Path, label: &str) -> Result<PathBuf, String> {
    let path = fs::canonicalize(path).map_err(|error| format!("{label}: {error}"))?;
    if !path.is_dir() {
        return Err(format!("{label} is not a directory: {}", path.display()));
    }
    Ok(PathBuf::from(path.to_string_lossy().trim_start_matches("\\\\?\\")))
}

fn git_head(root: &Path) -> Result<String, String> {
    let mut command = Command::new("git");
    clear_git_selection(&mut command);
    let output = command
        .args(["rev-parse", "HEAD"])
        .current_dir(root)
        .output()
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_owned());
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn git_toplevel(root: &Path) -> Result<PathBuf, String> {
    let mut command = Command::new("git");
    clear_git_selection(&mut command);
    let output = command
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(root)
        .output()
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_owned());
    }
    Ok(PathBuf::from(String::from_utf8_lossy(&output.stdout).trim()))
}

fn git_status(root: &Path) -> Result<String, String> {
    let mut command = Command::new("git");
    clear_git_selection(&mut command);
    let output = command
        .args(["status", "--porcelain"])
        .current_dir(root)
        .output()
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_owned());
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn clear_git_selection(command: &mut Command) {
    for name in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_COMMON_DIR",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    ] {
        command.env_remove(name);
    }
}

fn is_sha256(value: &str) -> bool {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return false;
    };
    hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn sha256(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn receipt_writer_derives_complete_rpc_inventory_from_results() {
        let (root, output, options) = fixture(false, false);
        let receipt_path = write(&options).expect("valid scenario log writes receipt");
        let receipt = read_json(&receipt_path).expect("read receipt");
        assert_eq!(receipt["schema"], RECEIPT_SCHEMA);
        assert_eq!(receipt["source_revision_kind"], SOURCE_REVISION_KIND);
        assert_eq!(receipt["families"][0]["methods"][0], "acyclic.actors.v1.ActorsService/CreateActor");
        assert_eq!(receipt["consumer"]["scenarios"].as_array().unwrap().len(), 1);
        cleanup(&root);
        cleanup(&output);
    }

    #[test]
    fn receipt_writer_rejects_missing_authority_rpc() {
        let (root, output, options) = fixture(true, false);
        let error = write(&options).expect_err("missing RPC must fail closed");
        assert!(error.contains("absent from Rust authority"));
        cleanup(&root);
        cleanup(&output);
    }

    #[test]
    fn receipt_writer_rejects_in_process_evidence_for_remote_qualification() {
        let (root, output, options) = fixture(false, true);
        let error = write(&options).expect_err("in-process evidence must not qualify remote");
        assert!(error.contains("execution_mode=remote"));
        cleanup(&root);
        cleanup(&output);
    }

    fn fixture(missing_rpc: bool, in_process: bool) -> (PathBuf, PathBuf, Options) {
        let root = env::temp_dir().join(format!(
            "acyclic-sdk-receipt-helper-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
        ));
        let output = root.join(".output");
        fs::create_dir_all(&output).expect("create output");
        fs::write(root.join(".gitignore"), b".output/\n").expect("write ignore");
        fs::write(root.join("source.txt"), b"rust authority").expect("write source");
        git(&root, &["init", "--initial-branch", "main"]);
        git(&root, &["config", "user.email", "receipt@example.invalid"]);
        git(&root, &["config", "user.name", "receipt-test"]);
        git(&root, &["add", "."]);
        git(&root, &["-c", "commit.gpgsign=false", "commit", "-m", "fixture"]);

        let consumer_bytes = b"consumer";
        let execution_mode = if in_process { "in-process" } else { "remote" };
        let scenario_bytes = br#"{"schema":"acyclic.sdk.rpc-scenario-result.v1","source_revision":"REVISION","status":"passed","invoked":true,"exit_code":0,"family":"actors","rpc":"acyclic.actors.v1.ActorsService/CreateActor","shape":"unary","transport":"grpc","execution_mode":"EXECUTION_MODE","checks":["invocation","transport","serialization"]}"#;
        let revision = git_head(&root).expect("fixture revision");
        let scenario_bytes = String::from_utf8(scenario_bytes.to_vec())
            .expect("fixture JSON")
            .replace("REVISION", &revision)
            .replace("EXECUTION_MODE", execution_mode)
            .into_bytes();
        let consumer_path = "qualification/consumers/consumer.bin";
        let scenario_path = "qualification/consumers/actors-create.json";
        fs::create_dir_all(output.join("qualification/consumers")).expect("consumer directory");
        fs::write(output.join(consumer_path), consumer_bytes).expect("consumer bytes");
        fs::write(output.join(scenario_path), &scenario_bytes).expect("scenario bytes");
        let manifest = json!({
            "schema": "acyclic.sdk.generation.manifest.v1",
            "source": {"revision": revision, "digest": sha256(b"contract")},
            "artifact_digest": sha256(b"artifacts"),
            "artifacts": [
                {"path": consumer_path, "sha256": sha256(consumer_bytes), "bytes": consumer_bytes.len()},
                {"path": scenario_path, "sha256": sha256(&scenario_bytes), "bytes": scenario_bytes.len()}
            ]
        });
        write_json(&output.join("sdk-generation-manifest.json"), &manifest);
        let actor_methods = if missing_rpc {
            json!([])
        } else {
            json!([{"rpc":"acyclic.actors.v1.ActorsService/CreateActor","shape":"unary"}])
        };
        let authority = json!({"schema":"acyclic.sdk.rust-authority.v1","families":[
            {"source":"actors/v1/actors.proto","rpc_methods":actor_methods},
            {"source":"protocol/v1/protocol.proto","rpc_methods":[]}
        ]});
        fs::create_dir_all(output.join("wire")).expect("wire directory");
        write_json(&output.join("wire/rust-authority.json"), &authority);
        let scenarios = vec![json!({
            "output_path": scenario_path,
            "output_sha256": sha256(&scenario_bytes)
        })];
        let log = json!({
            "schema": SCENARIO_LOG_SCHEMA,
            "source_revision": git_head(&root).expect("revision"),
            "consumer": {"name":"fixture-consumer","version":"1","artifact_path":consumer_path,"artifact_sha256":sha256(consumer_bytes)},
            "scenarios": scenarios
        });
        let log_path = output.join("scenario-log.json");
        write_json(&log_path, &log);
        (
            root.clone(),
            output.clone(),
            Options {
                source_root: root,
                output,
                scenario_log: log_path,
                language: "python".into(),
                tool: "fixture".into(),
                suite: "fixture".into(),
            },
        )
    }

    fn git(root: &Path, args: &[&str]) {
        let result = Command::new("git")
            .args(args)
            .current_dir(root)
            .output()
            .expect("git fixture command");
        assert!(result.status.success(), "git failed: {}", String::from_utf8_lossy(&result.stderr));
    }

    fn write_json(path: &Path, value: &Value) {
        fs::write(path, serde_json::to_vec_pretty(value).expect("encode JSON")).expect("write JSON");
    }

    fn cleanup(path: &Path) {
        let _ = fs::remove_dir_all(path);
    }
}
