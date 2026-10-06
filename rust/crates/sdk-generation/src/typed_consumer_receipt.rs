//! Fail-closed validation for generated language typed-consumer receipts.
//!
//! A producer manifest proves that files came from the Rust generation run;
//! this receipt proves that a generated typed consumer was actually executed.
//! The two pieces are deliberately separate so a package containing only
//! protobuf JSON tables or generated files cannot satisfy qualification.

use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

pub const SCHEMA: &str = "acyclic.sdk.typed-consumer-receipt.v1";

/// Assertions required for a target whose Rust catalog declares full gRPC
/// output. HTTP/OpenAPI-only targets are intentionally excluded by the caller.
pub const REQUIRED_ASSERTIONS: &[&str] = &[
    "field-identities",
    "presence-oneof",
    "bytes",
    "uint64",
    "enums",
    "rpc-stream-signatures",
];

/// Return whether the target needs the generated typed-consumer gate.
///
/// The wire kind comes from the Rust-owned language catalog. A target that is
/// explicitly HTTP-only has no protobuf typed consumer to qualify here.
pub fn requires_typed_consumer(wire_kind: &str, remote_level: &str) -> bool {
    wire_kind == "protobuf-grpc" && remote_level == "full-grpc"
}

/// Validate one producer-owned typed-consumer receipt and all files it names.
///
/// The output root is the producer's staged output directory. The receipt must
/// bind its generated source, command output, and authority identities to the
/// exact output tree and generation inputs supplied by the Rust orchestrator.
pub fn verify_file(
    receipt_path: &Path,
    output_root: &Path,
    expected_target: &str,
    expected_source_revision: &str,
    expected_model_digest: &str,
) -> Result<Value, String> {
    let bytes = fs::read(receipt_path)
        .map_err(|error| format!("read typed-consumer receipt {}: {error}", receipt_path.display()))?;
    let value: Value = serde_json::from_slice(&bytes)
        .map_err(|error| format!("parse typed-consumer receipt {}: {error}", receipt_path.display()))?;
    verify_value(
        &value,
        output_root,
        expected_target,
        expected_source_revision,
        expected_model_digest,
    )?;
    Ok(value)
}

/// Execute the producer-declared typed consumer under Rust supervision.
/// The receipt supplies only argv; Rust captures the process itself into
/// fixed paths, so producer-provided execution flags and log files cannot
/// turn a declaration into runtime evidence.
pub fn supervise_file(
    receipt_path: &Path,
    output_root: &Path,
    expected_target: &str,
    expected_source_revision: &str,
    expected_model_digest: &str,
) -> Result<Value, String> {
    let bytes = fs::read(receipt_path)
        .map_err(|error| format!("read typed-consumer receipt {}: {error}", receipt_path.display()))?;
    let mut receipt: Value = serde_json::from_slice(&bytes)
        .map_err(|error| format!("parse typed-consumer receipt {}: {error}", receipt_path.display()))?;
    if receipt.get("schema").and_then(Value::as_str) != Some(SCHEMA)
        || receipt.get("target").and_then(Value::as_str) != Some(expected_target)
    {
        return Err("typed-consumer receipt schema or target is not Rust-bound".into());
    }
    require_identity(&receipt, "source_revision", expected_source_revision)?;
    require_identity(&receipt, "source_digest", expected_model_digest)?;
    require_identity(&receipt, "rust_model_digest", expected_model_digest)?;
    let generated = receipt
        .get("generated_consumer")
        .and_then(Value::as_object)
        .ok_or_else(|| "typed-consumer receipt has no generated_consumer object".to_owned())?;
    let generated_path = relative_file(generated, "path", "generated consumer")?;
    let generated_hash = required_sha256(generated, "sha256", "generated consumer")?.to_owned();
    require_object_identity(generated, "source_revision", expected_source_revision)?;
    require_object_identity(generated, "source_digest", expected_model_digest)?;
    verify_generated_source(output_root, &generated_path, &generated_hash)?;
    let command = receipt
        .get("command")
        .and_then(Value::as_object)
        .ok_or_else(|| "typed-consumer receipt has no command object".to_owned())?;
    require_object_identity(command, "source_revision", expected_source_revision)?;
    require_object_identity(command, "source_digest", expected_model_digest)?;
    let argv = command
        .get("argv")
        .and_then(Value::as_array)
        .ok_or_else(|| "typed-consumer command has no argv".to_owned())?;
    if argv.is_empty() || argv.iter().any(|part| part.as_str().is_none_or(str::is_empty)) {
        return Err("typed-consumer command argv is empty or contains a non-string".into());
    }
    let argv_program = argv[0]
        .as_str()
        .ok_or_else(|| "typed-consumer command runner is not a string".to_owned())?;
    let approved_program = super::resolve_typed_consumer_program(expected_target, argv_program)?;
    let recorded_program = command
        .get("tool_path")
        .and_then(Value::as_str)
        .ok_or_else(|| "typed-consumer command has no supervised tool path".to_owned())?;
    let recorded_program = fs::canonicalize(recorded_program)
        .map_err(|error| format!("canonicalize recorded typed-consumer runner: {error}"))?;
    if recorded_program != approved_program {
        return Err("typed-consumer receipt tool path differs from Rust-approved runner".into());
    }
    let tool_hash = required_sha256(command, "tool_sha256", "typed-consumer runner")?;
    let actual_tool_hash = format!("sha256:{:x}", Sha256::digest(fs::read(&approved_program).map_err(
        |error| format!("read approved typed-consumer runner: {error}"),
    )?));
    if tool_hash != actual_tool_hash {
        return Err("typed-consumer runner hash differs from supervised executable".into());
    }
    let tool_version = command
        .get("tool_version")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "typed-consumer command has no supervised tool version".to_owned())?;
    if let Some(expected_version) = super::typed_consumer_toolchain_version(expected_target) {
        if !tool_version.contains(expected_version) {
            return Err(format!("typed-consumer runner version is not pinned to {expected_version}"));
        }
    }
    let argv = argv
        .iter()
        .map(|part| part.as_str().expect("checked above").to_owned())
        .collect::<Vec<_>>();
    let approved_program = super::resolve_typed_consumer_program(expected_target, &argv[0])?;
    if argv.iter().skip(1).any(|part| {
        matches!(
            part.to_ascii_lowercase().as_str(),
            "-c" | "/c" | "-command" | "/command" | "-encodedcommand" | "/encodedcommand" | "-e"
        )
    }) {
        return Err("typed-consumer command may not use inline shell or evaluator flags".into());
    }
    let tool_version = if let Some(expected_version) =
        super::typed_consumer_toolchain_version(expected_target)
    {
        let version_args = vec!["version".to_owned()];
        let (status, stdout, _) = run_bounded_command(
            &approved_program,
            &version_args,
            output_root,
            &output_root.join("typed-consumer-version.stdout"),
            &output_root.join("typed-consumer-version.stderr"),
        )?;
        let version = String::from_utf8_lossy(&stdout).into_owned();
        let _ = fs::remove_file(output_root.join("typed-consumer-version.stdout"));
        let _ = fs::remove_file(output_root.join("typed-consumer-version.stderr"));
        if !status.success() || !version.contains(expected_version) {
            return Err(format!(
                "approved typed-consumer runner version is not pinned to {expected_version}"
            ));
        }
        version.trim().to_owned()
    } else {
        String::from("catalogue-approved")
    };
    let tool_sha256 = format!(
        "sha256:{:x}",
        Sha256::digest(
            &fs::read(&approved_program)
                .map_err(|error| format!("read approved typed-consumer runner: {error}"))?,
        )
    );
    if !super::typed_consumer_source_matches_target(expected_target, &generated_path) {
        return Err("generated consumer source type is not approved for the target".into());
    }
    let source_index = super::typed_consumer_source_argument_index(expected_target, &argv[1..])
        .ok_or_else(|| "typed-consumer command has no approved source invocation form".to_owned())?
        + 1;
    let source_argument = argv
        .get(source_index)
        .ok_or_else(|| "typed-consumer command is missing its generated source argument".to_owned())?;
    let source_argument = Path::new(source_argument);
    let source_argument = if source_argument.is_absolute() {
        source_argument.to_owned()
    } else {
        output_root.join(source_argument)
    };
    let expected_source_path = fs::canonicalize(output_root.join(&generated_path))
        .map_err(|error| format!("canonicalize generated source: {error}"))?;
    let actual_source_path = fs::canonicalize(&source_argument)
        .map_err(|error| format!("canonicalize typed-consumer source argument: {error}"))?;
    if actual_source_path != expected_source_path {
        return Err("typed-consumer command must directly invoke the declared generated source".into());
    }
    let stdout_path = PathBuf::from("typed-consumer-supervisor.stdout");
    let stderr_path = PathBuf::from("typed-consumer-supervisor.stderr");
    let (status, stdout, stderr) = run_bounded_command(
        &approved_program,
        &argv[1..],
        output_root,
        &output_root.join("typed-consumer-supervisor.stdout"),
        &output_root.join("typed-consumer-supervisor.stderr"),
    )?;
    for path in [&stdout_path, &stderr_path] {
        if let Ok(metadata) = fs::symlink_metadata(output_root.join(path)) {
            if metadata.file_type().is_symlink() {
                return Err(format!(
                    "refusing to overwrite symlinked supervised log {}",
                    path.display()
                ));
            }
        }
    }
    fs::write(output_root.join(&stdout_path), &stdout)
        .map_err(|error| format!("write supervised typed-consumer stdout: {error}"))?;
    fs::write(output_root.join(&stderr_path), &stderr)
        .map_err(|error| format!("write supervised typed-consumer stderr: {error}"))?;
    if !status.success() {
        return Err(format!(
            "typed-consumer command failed under Rust supervision with exit code {:?}",
            status.code()
        ));
    }
    verify_runtime_assertion_output(&stdout, &generated_hash)?;
    let stdout_sha256 = format!("sha256:{:x}", Sha256::digest(&stdout));
    let stderr_sha256 = format!("sha256:{:x}", Sha256::digest(&stderr));
    let command = receipt
        .get_mut("command")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| "typed-consumer receipt has no mutable command object".to_owned())?;
    command.insert("executed".into(), Value::Bool(true));
    command.insert("exit_code".into(), Value::from(0));
    command.insert("source_revision".into(), Value::String(expected_source_revision.to_owned()));
    command.insert("source_digest".into(), Value::String(expected_model_digest.to_owned()));
    command.insert("stdout_path".into(), Value::String(stdout_path.to_string_lossy().into_owned()));
    command.insert("stdout_sha256".into(), Value::String(stdout_sha256.clone()));
    command.insert("stderr_path".into(), Value::String(stderr_path.to_string_lossy().into_owned()));
    command.insert("stderr_sha256".into(), Value::String(stderr_sha256.clone()));
    command.insert("tool_path".into(), Value::String(approved_program.to_string_lossy().into_owned()));
    command.insert("tool_sha256".into(), Value::String(tool_sha256));
    command.insert("tool_version".into(), Value::String(tool_version));
    if let Some(assertions) = receipt.get_mut("assertions").and_then(Value::as_array_mut) {
        for assertion in assertions {
            if let Some(evidence) = assertion
                .get_mut("evidence")
                .and_then(Value::as_object_mut)
            {
                evidence.insert("runtime".into(), Value::Bool(true));
                evidence.insert(
                    "generated_consumer_path".into(),
                    Value::String(generated_path.to_string_lossy().replace('\\', "/")),
                );
                evidence.insert(
                    "stdout_path".into(),
                    Value::String(stdout_path.to_string_lossy().into_owned()),
                );
                evidence.insert(
                    "stderr_path".into(),
                    Value::String(stderr_path.to_string_lossy().into_owned()),
                );
                evidence.insert("generated_consumer_sha256".into(), Value::String(generated_hash.clone()));
                evidence.insert("stdout_sha256".into(), Value::String(stdout_sha256.clone()));
                evidence.insert("stderr_sha256".into(), Value::String(stderr_sha256.clone()));
            }
        }
    }
    let receipt_bytes = serde_json::to_vec_pretty(&receipt)
        .map_err(|error| format!("serialize supervised typed-consumer receipt: {error}"))?;
    fs::write(receipt_path, [receipt_bytes.as_slice(), b"\n"].concat())
        .map_err(|error| format!("write supervised typed-consumer receipt: {error}"))?;
    verify_file(
        receipt_path,
        output_root,
        expected_target,
        expected_source_revision,
        expected_model_digest,
    )
}

pub fn verify_value(
    receipt: &Value,
    output_root: &Path,
    expected_target: &str,
    expected_source_revision: &str,
    expected_model_digest: &str,
) -> Result<(), String> {
    if receipt.get("schema").and_then(Value::as_str) != Some(SCHEMA) {
        return Err("typed-consumer receipt has an unsupported schema".into());
    }
    if receipt.get("target").and_then(Value::as_str) != Some(expected_target) {
        return Err("typed-consumer receipt target differs from the producer target".into());
    }
    require_identity(receipt, "source_revision", expected_source_revision)?;
    require_identity(receipt, "source_digest", expected_model_digest)?;
    require_identity(receipt, "rust_model_digest", expected_model_digest)?;

    let generated = receipt
        .get("generated_consumer")
        .and_then(Value::as_object)
        .ok_or_else(|| "typed-consumer receipt has no generated_consumer object".to_owned())?;
    let generated_path = relative_file(generated, "path", "generated consumer")?;
    if generated_path
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            matches!(extension.to_ascii_lowercase().as_str(), "json" | "yaml" | "yml")
        })
    {
        return Err("generated_consumer.path points to a data projection, not typed source".into());
    }
    require_object_identity(generated, "source_revision", expected_source_revision)?;
    require_object_identity(generated, "source_digest", expected_model_digest)?;
    verify_generated_source(
        output_root,
        &generated_path,
        required_sha256(generated, "sha256", "generated consumer")?,
    )?;

    let command = receipt
        .get("command")
        .and_then(Value::as_object)
        .ok_or_else(|| "typed-consumer receipt has no command object".to_owned())?;
    if command.get("executed").and_then(Value::as_bool) != Some(true)
        || command.get("exit_code").and_then(Value::as_i64) != Some(0)
    {
        return Err("typed-consumer command did not execute successfully".into());
    }
    let argv = command
        .get("argv")
        .and_then(Value::as_array)
        .ok_or_else(|| "typed-consumer command has no argv".to_owned())?;
    if argv.is_empty() || argv.iter().any(|part| part.as_str().is_none_or(str::is_empty)) {
        return Err("typed-consumer command argv is empty or contains a non-string".into());
    }
    require_object_identity(command, "source_revision", expected_source_revision)?;
    require_object_identity(command, "source_digest", expected_model_digest)?;
    let stdout_path = relative_file(command, "stdout_path", "typed-consumer stdout")?;
    let stderr_path = relative_file(command, "stderr_path", "typed-consumer stderr")?;
    let stdout_hash = required_sha256(command, "stdout_sha256", "typed-consumer stdout")?;
    let stderr_hash = required_sha256(command, "stderr_sha256", "typed-consumer stderr")?;
    verify_file_hash(output_root, &stdout_path, stdout_hash, "typed-consumer stdout")?;
    verify_file_hash(output_root, &stderr_path, stderr_hash, "typed-consumer stderr")?;

    let assertions = receipt
        .get("assertions")
        .and_then(Value::as_array)
        .ok_or_else(|| "typed-consumer receipt has no assertions array".to_owned())?;
    let mut observed = BTreeSet::new();
    for assertion in assertions {
        let object = assertion
            .as_object()
            .ok_or_else(|| "typed-consumer assertion is not an object".to_owned())?;
        let id = object
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| "typed-consumer assertion has no id".to_owned())?;
        if !REQUIRED_ASSERTIONS.contains(&id) || !observed.insert(id) {
            return Err(format!("typed-consumer assertion is unknown or duplicated: {id}"));
        }
        if object.get("status").and_then(Value::as_str) != Some("passed")
            || object.get("executed").and_then(Value::as_bool) != Some(true)
            || object.get("source_bound").and_then(Value::as_bool) != Some(true)
        {
            return Err(format!("typed-consumer assertion did not pass: {id}"));
        }
        let evidence = object
            .get("evidence")
            .and_then(Value::as_object)
            .ok_or_else(|| format!("typed-consumer assertion has no evidence: {id}"))?;
        if evidence.get("runtime").and_then(Value::as_bool) != Some(true) {
            return Err(format!("typed-consumer assertion lacks runtime evidence: {id}"));
        }
        let evidence_generated_path = relative_file(
            evidence,
            "generated_consumer_path",
            "typed-consumer assertion evidence",
        )?;
        let evidence_stdout_path = relative_file(
            evidence,
            "stdout_path",
            "typed-consumer assertion evidence",
        )?;
        let evidence_stderr_path = relative_file(
            evidence,
            "stderr_path",
            "typed-consumer assertion evidence",
        )?;
        if evidence_generated_path != generated_path
            || evidence_stdout_path != stdout_path
            || evidence_stderr_path != stderr_path
        {
            return Err(format!(
                "typed-consumer assertion evidence is detached from generated source and command output: {id}"
            ));
        }
        if evidence.get("generated_consumer_sha256").and_then(Value::as_str)
            != Some(required_sha256(generated, "sha256", "generated consumer")?)
            || evidence.get("stdout_sha256").and_then(Value::as_str) != Some(stdout_hash)
            || evidence.get("stderr_sha256").and_then(Value::as_str) != Some(stderr_hash)
        {
            return Err(format!(
                "typed-consumer assertion evidence hashes are detached from supervised facts: {id}"
            ));
        }
    }
    let missing = REQUIRED_ASSERTIONS
        .iter()
        .copied()
        .filter(|id| !observed.contains(id))
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(format!("typed-consumer assertions are missing: {}", missing.join(", ")));
    }
    Ok(())
}

fn require_identity(value: &Value, key: &str, expected: &str) -> Result<(), String> {
    if value.get(key).and_then(Value::as_str) != Some(expected) {
        return Err(format!("typed-consumer receipt {key} is not bound to the Rust generation"));
    }
    Ok(())
}

fn require_object_identity(
    value: &serde_json::Map<String, Value>,
    key: &str,
    expected: &str,
) -> Result<(), String> {
    if value.get(key).and_then(Value::as_str) != Some(expected) {
        return Err(format!("typed-consumer receipt {key} is not bound to the Rust generation"));
    }
    Ok(())
}

fn relative_file(
    value: &serde_json::Map<String, Value>,
    key: &str,
    label: &str,
) -> Result<PathBuf, String> {
    let text = value
        .get(key)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(|| format!("{label} has no {key}"))?;
    let path = Path::new(text);
    if path.is_absolute()
        || text.starts_with('/')
        || text.starts_with('\\')
        || text.contains(':')
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(format!("{label} path is not a safe relative file: {text}"));
    }
    Ok(path.to_owned())
}

fn required_sha256<'a>(
    value: &'a serde_json::Map<String, Value>,
    key: &str,
    label: &str,
) -> Result<&'a str, String> {
    let digest = value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{label} has no {key}"))?;
    if !is_sha256(digest) {
        return Err(format!("{label} {key} is not a SHA-256 digest"));
    }
    Ok(digest)
}

const SUPERVISOR_TIMEOUT: Duration = Duration::from_secs(120);
const SUPERVISOR_OUTPUT_LIMIT: usize = 8 * 1024 * 1024;

fn run_bounded_command(
    program: &Path,
    args: &[String],
    current_dir: &Path,
    stdout_path: &Path,
    stderr_path: &Path,
) -> Result<(std::process::ExitStatus, Vec<u8>, Vec<u8>), String> {
    for path in [stdout_path, stderr_path] {
        if let Ok(metadata) = fs::symlink_metadata(path) {
            if metadata.file_type().is_symlink() {
                return Err(format!("refusing to overwrite symlinked log {}", path.display()));
            }
        }
    }
    let stdout_file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(stdout_path)
        .map_err(|error| format!("open typed-consumer stdout: {error}"))?;
    let stderr_file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(stderr_path)
        .map_err(|error| format!("open typed-consumer stderr: {error}"))?;
    let mut child = Command::new(program)
        .args(args)
        .current_dir(current_dir)
        .env("ACYCLIC_RUST_GENERATION_ENTRYPOINT", "1")
        .stdout(Stdio::from(stdout_file))
        .stderr(Stdio::from(stderr_file))
        .spawn()
        .map_err(|error| format!("start typed-consumer command: {error}"))?;
    let deadline = Instant::now() + SUPERVISOR_TIMEOUT;
    let status = loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("poll typed-consumer command: {error}"))?
        {
            break status;
        }
        if Instant::now() >= deadline {
            terminate_process_tree(&mut child);
            let _ = child.wait();
            return Err(format!(
                "typed-consumer command exceeded {} second deadline",
                SUPERVISOR_TIMEOUT.as_secs()
            ));
        }
        thread::sleep(Duration::from_millis(25));
    };
    let stdout = read_bounded_file(stdout_path)?;
    let stderr = read_bounded_file(stderr_path)?;
    Ok((status, stdout, stderr))
}

fn read_bounded_file(path: &Path) -> Result<Vec<u8>, String> {
    let bytes = fs::read(path).map_err(|error| format!("read typed-consumer output: {error}"))?;
    if bytes.len() > SUPERVISOR_OUTPUT_LIMIT {
        return Err(format!(
            "typed-consumer output exceeded {} byte limit",
            SUPERVISOR_OUTPUT_LIMIT
        ));
    }
    Ok(bytes)
}

fn terminate_process_tree(child: &mut std::process::Child) {
    let pid = child.id();
    {
        #[cfg(windows)]
        {
            let _ = Command::new("taskkill")
                .args(["/PID", &pid.to_string(), "/T", "/F"])
                .status();
        }
        #[cfg(unix)]
        {
            let _ = Command::new("pkill")
                .args(["-TERM", "-P", &pid.to_string()])
                .status();
        }
    }
    let _ = child.kill();
}

fn verify_file_hash(
    output_root: &Path,
    path: &Path,
    expected: &str,
    label: &str,
) -> Result<(), String> {
    let contained = canonical_contained_file(output_root, path, label)?;
    let bytes = fs::read(&contained)
        .map_err(|error| format!("read {label} {}: {error}", path.display()))?;
    let actual = format!("sha256:{:x}", Sha256::digest(bytes));
    if actual != expected {
        return Err(format!(
            "{label} hash differs: expected {expected}, got {actual}"
        ));
    }
    Ok(())
}

fn canonical_contained_file(root: &Path, relative: &Path, label: &str) -> Result<PathBuf, String> {
    let root = fs::canonicalize(root)
        .map_err(|error| format!("canonicalize {label} root {}: {error}", root.display()))?;
    let path = root.join(relative);
    let candidate = fs::canonicalize(&path)
        .map_err(|error| format!("canonicalize {label} {}: {error}", path.display()))?;
    if !candidate.starts_with(&root) {
        return Err(format!("{label} resolves outside its output root"));
    }
    Ok(candidate)
}

fn verify_generated_source(root: &Path, relative: &Path, expected: &str) -> Result<(), String> {
    let path = canonical_contained_file(root, relative, "generated consumer")?;
    let bytes = fs::read(&path)
        .map_err(|error| format!("read generated consumer {}: {error}", path.display()))?;
    let actual = format!("sha256:{:x}", Sha256::digest(&bytes));
    if actual != expected {
        return Err(format!(
            "generated consumer hash differs: expected {expected}, got {actual}"
        ));
    }
    let source = std::str::from_utf8(&bytes)
        .map_err(|_| "generated consumer is not UTF-8 source".to_owned())?;
    if source.trim().is_empty() {
        return Err("generated consumer source is empty".into());
    }
    if serde_json::from_str::<Value>(source).is_ok() {
        return Err("generated consumer is a JSON document, not typed source".into());
    }
    Ok(())
}

fn verify_runtime_assertion_output(bytes: &[u8], generated_hash: &str) -> Result<(), String> {
    let output: Value = serde_json::from_slice(bytes)
        .map_err(|error| format!("typed-consumer stdout is not machine-readable assertion output: {error}"))?;
    if output.get("schema").and_then(Value::as_str)
        != Some("acyclic.sdk.typed-consumer-runtime.v1")
        || output.get("generated_consumer_sha256").and_then(Value::as_str) != Some(generated_hash)
    {
        return Err("typed-consumer runtime output is not bound to the generated source hash".into());
    }
    let assertions = output
        .get("assertions")
        .and_then(Value::as_array)
        .ok_or_else(|| "typed-consumer runtime output has no assertions array".to_owned())?;
    let mut observed = BTreeSet::new();
    for assertion in assertions {
        let object = assertion
            .as_object()
            .ok_or_else(|| "typed-consumer runtime assertion is not an object".to_owned())?;
        let id = object
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| "typed-consumer runtime assertion has no id".to_owned())?;
        if !REQUIRED_ASSERTIONS.contains(&id)
            || object.get("status").and_then(Value::as_str) != Some("passed")
            || !observed.insert(id)
        {
            return Err(format!("typed-consumer runtime assertion failed or duplicated: {id}"));
        }
    }
    if observed.len() != REQUIRED_ASSERTIONS.len() {
        return Err("typed-consumer runtime output does not cover every required assertion".into());
    }
    Ok(())
}

fn is_sha256(value: &str) -> bool {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return false;
    };
    hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn http_targets_do_not_require_grpc_typed_consumers() {
        assert!(!requires_typed_consumer("json-http", "http-projection"));
        assert!(!requires_typed_consumer("none", "none"));
        assert!(requires_typed_consumer("protobuf-grpc", "full-grpc"));
    }

    #[test]
    fn required_assertions_are_unique_and_cover_the_gate() {
        let unique = REQUIRED_ASSERTIONS.iter().copied().collect::<BTreeSet<_>>();
        assert_eq!(unique.len(), REQUIRED_ASSERTIONS.len());
        for required in [
            "field-identities",
            "presence-oneof",
            "bytes",
            "uint64",
            "enums",
            "rpc-stream-signatures",
        ] {
            assert!(unique.contains(required));
        }
    }

    #[test]
    fn json_only_generated_consumer_is_rejected() {
        let receipt = json!({
            "schema": SCHEMA,
            "target": "fixture",
            "source_revision": "a".repeat(40),
            "source_digest": "b".repeat(64),
            "rust_model_digest": "b".repeat(64),
            "generated_consumer": {
                "path": "package.json",
                "sha256": "sha256:".to_owned() + &"c".repeat(64),
                "source_revision": "a".repeat(40),
                "source_digest": "b".repeat(64)
            }
        });
        let error = verify_value(
            &receipt,
            Path::new("."),
            "fixture",
            &"a".repeat(40),
            &"b".repeat(64),
        )
        .expect_err("JSON projections must not qualify as typed consumers");
        assert!(error.contains("data projection"));
    }
}
