//! Rust-owned receipt writer and verifier for generated native language packages.
//!
//! This records compilation and installed-consumer evidence. It intentionally
//! does not call a compile-only result an RPC qualification.

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const SCHEMA: &str = "acyclic.sdk.platform.qualification.v1";

fn main() {
    if let Err(error) = run() {
        eprintln!("sdk-platform-receipt: {error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let command = args.next().ok_or_else(usage)?;
    let mut values = std::collections::BTreeMap::new();
    while let Some(flag) = args.next() {
        let key = flag.strip_prefix("--").ok_or_else(usage)?.to_owned();
        let value = args.next().ok_or_else(usage)?;
        values.insert(key, value);
    }
    match command.as_str() {
        "write" => write_receipt(&values),
        "verify" => verify_receipt(&values),
        _ => Err(usage()),
    }
}

fn write_receipt(values: &std::collections::BTreeMap<String, String>) -> Result<(), String> {
    let source_root = required(values, "source-root")?;
    let generated = required(values, "generated")?;
    let consumer = required(values, "consumer")?;
    let tests = required(values, "tests")?;
    let output = required(values, "output")?;
    let language = required(values, "language")?;
    let toolchain = required(values, "toolchain")?;
    let tools = required(values, "tools")?
        .split(';')
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .collect::<Vec<_>>();
    if tools.is_empty() {
        return Err("at least one pinned generator tool is required".into());
    }
    let source_revision = git_head(Path::new(source_root))?;
    let generation = read_json(&Path::new(generated).join("generation-receipt.json"))?;
    if generation.get("language").and_then(Value::as_str) != Some(language) {
        return Err("generated receipt language does not match the requested package".into());
    }
    let tests_value = read_json(Path::new(tests))?;
    if tests_value.get("status").and_then(Value::as_str) != Some("passed") {
        return Err("platform test log is not a passed compile/install result".into());
    }
    let checks = tests_value
        .get("checks")
        .and_then(Value::as_array)
        .filter(|items| !items.is_empty())
        .ok_or_else(|| "platform test log has no checks".to_owned())?;
    validate_checks(checks)?;
    let generated_hash = tree_hash(Path::new(generated))?;
    let consumer_hash = artifact_hash(Path::new(consumer))?;
    let tests_hash = artifact_hash(Path::new(tests))?;
    let tool_evidence = tools
        .iter()
        .map(|path| {
            Ok(json!({
                "path": path,
                "sha256": artifact_hash(path)?,
            }))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let receipt = json!({
        "schema": SCHEMA,
        "language": language,
        "status": "compiled",
        "capability": "transport-compile",
        "rpc_scenario": false,
        "source_revision": source_revision,
        "toolchain": toolchain,
        "tools": tool_evidence,
        "generated": {
            "path": generated,
            "sha256": generated_hash,
            "generation_receipt": generation,
        },
        "consumer": {
            "path": consumer,
            "sha256": consumer_hash,
        },
        "tests": {
            "path": tests,
            "sha256": tests_hash,
            "status": "passed",
            "checks": checks,
            "log": tests_value,
        },
    });
    let output = PathBuf::from(output);
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

fn verify_receipt(values: &std::collections::BTreeMap<String, String>) -> Result<(), String> {
    let receipt = read_json(Path::new(required(values, "receipt")?))?;
    if receipt.get("schema").and_then(Value::as_str) != Some(SCHEMA) {
        return Err("platform receipt schema mismatch".into());
    }
    if receipt.get("language").and_then(Value::as_str) != Some(required(values, "language")?) {
        return Err("platform receipt language mismatch".into());
    }
    if receipt.get("source_revision").and_then(Value::as_str)
        != Some(required(values, "source-revision")?)
    {
        return Err("platform receipt source revision mismatch".into());
    }
    if receipt.get("status").and_then(Value::as_str) != Some("compiled")
        || receipt.get("rpc_scenario").and_then(Value::as_bool) != Some(false)
    {
        return Err("platform receipt is not a compile-only qualification".into());
    }
    for section in ["generated", "consumer", "tests"] {
        let object = receipt
            .get(section)
            .and_then(Value::as_object)
            .ok_or_else(|| format!("platform receipt has no {section} evidence"))?;
        let digest = object
            .get("sha256")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("{section} evidence has no digest"))?;
        if !is_sha256(digest) {
            return Err(format!("{section} evidence digest is invalid"));
        }
        let path = object
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("{section} evidence has no path"))?;
        let actual = artifact_hash(Path::new(path))?;
        if actual != digest {
            return Err(format!("{section} evidence bytes do not match the receipt"));
        }
    }
    if receipt["tests"]["status"].as_str() != Some("passed") {
        return Err("platform receipt has no passing checks".into());
    }
    let tools = receipt
        .get("tools")
        .and_then(Value::as_array)
        .filter(|items| !items.is_empty())
        .ok_or_else(|| "platform receipt has no generator tool evidence".to_owned())?;
    for tool in tools {
        let path = tool
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| "generator tool evidence has no path".to_owned())?;
        let expected = tool
            .get("sha256")
            .and_then(Value::as_str)
            .ok_or_else(|| "generator tool evidence has no digest".to_owned())?;
        if artifact_hash(Path::new(path))? != expected {
            return Err(format!(
                "generator tool bytes do not match the receipt: {path}"
            ));
        }
    }
    let checks = receipt["tests"]["checks"]
        .as_array()
        .filter(|items| !items.is_empty())
        .ok_or_else(|| "platform receipt has no passing checks".to_owned())?;
    validate_checks(checks)?;
    let tests_path = receipt["tests"]["path"]
        .as_str()
        .ok_or_else(|| "platform receipt has no test log".to_owned())?;
    let tests = read_json(Path::new(tests_path))?;
    if tests != receipt["tests"]["log"] {
        return Err("platform test log contents do not match the receipt".into());
    }
    Ok(())
}

fn validate_checks(checks: &[Value]) -> Result<(), String> {
    if checks.is_empty() {
        return Err("platform test log has no checks".into());
    }
    for check in checks {
        let object = check
            .as_object()
            .ok_or_else(|| "platform test checks must be objects".to_owned())?;
        if object
            .get("command")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .is_none()
        {
            return Err("platform test check has no command".into());
        }
        if object.get("exit_code").and_then(Value::as_i64) != Some(0) {
            return Err("platform test check did not exit successfully".into());
        }
        for output in ["stdout_sha256", "stderr_sha256"] {
            let digest = object
                .get(output)
                .and_then(Value::as_str)
                .ok_or_else(|| format!("platform test check has no {output}"))?;
            if !is_sha256(digest) {
                return Err(format!("platform test check {output} is invalid"));
            }
        }
    }
    Ok(())
}

fn required<'a>(
    values: &'a std::collections::BTreeMap<String, String>,
    key: &str,
) -> Result<&'a str, String> {
    values
        .get(key)
        .map(String::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(usage)
}

fn read_json(path: &Path) -> Result<Value, String> {
    serde_json::from_slice(&fs::read(path).map_err(|error| format!("{}: {error}", path.display()))?)
        .map_err(|error| format!("{}: {error}", path.display()))
}

fn git_head(root: &Path) -> Result<String, String> {
    let output = Command::new("git")
        .args(["-C", &root.to_string_lossy(), "rev-parse", "HEAD"])
        .output()
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err("cannot resolve source revision".into());
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn artifact_hash(path: &Path) -> Result<String, String> {
    if path.is_file() {
        return hash_bytes(&fs::read(path).map_err(|error| error.to_string())?);
    }
    tree_hash(path)
}

fn tree_hash(root: &Path) -> Result<String, String> {
    if !root.is_dir() {
        return Err(format!(
            "artifact directory does not exist: {}",
            root.display()
        ));
    }
    let mut files = Vec::new();
    collect_files(root, root, &mut files)?;
    files.sort_by(|left, right| left.0.cmp(&right.0));
    let mut digest = Sha256::new();
    for (relative, bytes) in files {
        digest.update(relative.as_bytes());
        digest.update([0]);
        digest.update(&bytes);
        digest.update([0]);
    }
    Ok(format!("sha256:{:x}", digest.finalize()))
}

fn collect_files(
    root: &Path,
    path: &Path,
    files: &mut Vec<(String, Vec<u8>)>,
) -> Result<(), String> {
    for entry in fs::read_dir(path).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let path = entry.path();
        if path.is_dir() {
            collect_files(root, &path, files)?;
        } else if path.is_file() {
            let relative = path
                .strip_prefix(root)
                .map_err(|error| error.to_string())?
                .to_string_lossy()
                .replace('\\', "/");
            files.push((
                relative,
                fs::read(&path).map_err(|error| error.to_string())?,
            ));
        }
    }
    Ok(())
}

fn hash_bytes(bytes: &[u8]) -> Result<String, String> {
    let mut digest = Sha256::new();
    digest.update(bytes);
    Ok(format!("sha256:{:x}", digest.finalize()))
}

fn is_sha256(value: &str) -> bool {
    value.len() == 71
        && value.starts_with("sha256:")
        && value[7..].bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn usage() -> String {
    "usage: sdk-platform-receipt <write|verify> --source-root PATH --generated PATH --consumer PATH --tests PATH --output PATH --language ID --toolchain PIN --tools PATH;PATH | verify --receipt PATH --language ID --source-revision REV".into()
}
