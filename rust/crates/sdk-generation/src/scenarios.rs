//! Rust-owned executable scenarios used by the generated documentation data.
//!
//! This first registry is intentionally small. It records only examples whose
//! source and invocation are already present in this checkout. The launcher
//! can use [`validate`] before compiling or executing a scenario and can bind
//! the returned source digest to its generation receipt.

use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    env,
    path::{Path, PathBuf},
    process::Command,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ScenarioMode {
    /// Compile the Rust example with the pinned, locked toolchain.
    #[allow(dead_code)]
    Compile,
    /// Execute against the local, self-contained fixture.
    ExecuteLocal,
    /// Execute against a declared qualification endpoint during release or a
    /// manually dispatched run.
    ExecuteWithEndpoint,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ScenarioKind {
    ActorsUnary,
    ActorsTypescriptConsumer,
    StreamStreaming,
    StreamTypescriptConsumer,
    FilesystemEmbedded,
    MachinesTypescriptConsumer,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Scenario {
    pub id: &'static str,
    pub family: &'static str,
    pub package: &'static str,
    pub example: &'static str,
    pub source_path: &'static str,
    pub operation: &'static str,
    pub kind: ScenarioKind,
    pub mode: ScenarioMode,
    pub features: &'static [&'static str],
}

/// The first six source-backed scenarios. More examples require an explicit
/// registry entry and a matching receipt; Cargo example discovery alone is
/// deliberately insufficient for publishing user-facing snippets.
pub const SCENARIOS: &[Scenario] = &[
    Scenario {
        id: "actors/transport-conformance-unary",
        family: "actors",
        package: "acyclic-actors",
        example: "transport-conformance",
        source_path: "rust/crates/actors/examples/transport-conformance.rs",
        operation: "create_actor",
        kind: ScenarioKind::ActorsUnary,
        mode: ScenarioMode::ExecuteWithEndpoint,
        features: &[],
    },
    Scenario {
        id: "actors/typescript-consumer",
        family: "actors",
        package: "acyclic-actors",
        example: "actors-typescript-consumer",
        source_path: "rust/crates/actors/examples/actors-typescript-consumer.rs",
        operation: "encode-create-request",
        kind: ScenarioKind::ActorsTypescriptConsumer,
        mode: ScenarioMode::ExecuteLocal,
        features: &[],
    },
    Scenario {
        id: "stream/http-conformance-streaming",
        family: "stream",
        package: "acyclic-stream",
        example: "http-conformance",
        source_path: "rust/crates/stream/examples/http-conformance.rs",
        operation: "tail",
        kind: ScenarioKind::StreamStreaming,
        mode: ScenarioMode::ExecuteWithEndpoint,
        features: &[],
    },
    Scenario {
        id: "stream/typescript-consumer",
        family: "stream",
        package: "acyclic-stream",
        example: "stream-typescript-consumer",
        source_path: "rust/crates/stream/examples/stream-typescript-consumer.rs",
        operation: "memory-append-read",
        kind: ScenarioKind::StreamTypescriptConsumer,
        mode: ScenarioMode::ExecuteLocal,
        features: &[],
    },
    Scenario {
        id: "filesystem/embedded-workspace",
        family: "filesystem",
        package: "acyclic-fs",
        example: "embedded_workspace",
        source_path: "rust/crates/filesystem/examples/embedded_workspace.rs",
        operation: "mounted-view",
        kind: ScenarioKind::FilesystemEmbedded,
        mode: ScenarioMode::ExecuteLocal,
        features: &[],
    },
    Scenario {
        id: "machines/typescript-consumer",
        family: "machines",
        package: "acyclic-machines",
        example: "machines-typescript-consumer",
        source_path: "rust/crates/machines/examples/machines-typescript-consumer.rs",
        operation: "create-list-consumer",
        kind: ScenarioKind::MachinesTypescriptConsumer,
        mode: ScenarioMode::ExecuteLocal,
        features: &[],
    },
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScenarioSource {
    pub scenario: Scenario,
    pub source_sha256: String,
    pub source_files: Vec<PathBuf>,
}

/// The source-backed portion emitted in the generation output.
///
/// `source_files` is intentionally kept out of this record: it contains
/// checkout-specific absolute paths and is used only to extend the generator
/// source closure. The repository-relative source identity is already carried
/// by [`Scenario::source_path`] and the digest binds the record to its Rust
/// inputs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScenarioRecord {
    pub id: &'static str,
    pub family: &'static str,
    pub package: &'static str,
    pub example: &'static str,
    pub source_path: &'static str,
    pub operation: &'static str,
    pub kind: ScenarioKind,
    pub mode: ScenarioMode,
    pub features: &'static [&'static str],
    pub source_sha256: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScenarioCatalog {
    pub schema: &'static str,
    pub scenarios: Vec<ScenarioRecord>,
}

pub const CATALOG_SCHEMA: &str = "acyclic.sdk.scenarios.v1";

pub fn catalog(sources: &[ScenarioSource]) -> ScenarioCatalog {
    ScenarioCatalog {
        schema: CATALOG_SCHEMA,
        scenarios: sources
            .iter()
            .map(|source| ScenarioRecord {
                id: source.scenario.id,
                family: source.scenario.family,
                package: source.scenario.package,
                example: source.scenario.example,
                source_path: source.scenario.source_path,
                operation: source.scenario.operation,
                kind: source.scenario.kind,
                mode: source.scenario.mode,
                features: source.scenario.features,
                source_sha256: source.source_sha256.clone(),
            })
            .collect(),
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScenarioExecutionRecord {
    pub id: String,
    pub source_sha256: String,
    pub mode: ScenarioMode,
    pub status: &'static str,
    pub stdout_sha256: String,
    pub stderr_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TypeScriptProjectionRecord {
    pub scenario_id: &'static str,
    pub language: &'static str,
    pub package: &'static str,
    pub path: &'static str,
    pub source_sha256: String,
    pub rust_output_sha256: String,
    pub snippet_sha256: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TypeScriptProjectionCatalog {
    pub schema: &'static str,
    pub projections: Vec<TypeScriptProjectionRecord>,
}

pub const PROJECTION_CATALOG_SCHEMA: &str = "acyclic.sdk.scenario-projections.v1";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScenarioExecution {
    pub scenario: Scenario,
    pub source_sha256: String,
    pub stdout: String,
    pub stdout_sha256: String,
    pub stderr_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypeScriptSnippet {
    pub scenario_id: &'static str,
    pub package: &'static str,
    pub path: &'static str,
    pub source_sha256: String,
    pub rust_output_sha256: String,
    pub source: String,
}

pub fn execution_catalog(executions: &[ScenarioExecution]) -> Vec<ScenarioExecutionRecord> {
    executions
        .iter()
        .map(|execution| ScenarioExecutionRecord {
            id: execution.scenario.id.to_owned(),
            source_sha256: execution.source_sha256.clone(),
            mode: execution.scenario.mode,
            status: "passed",
            stdout_sha256: execution.stdout_sha256.clone(),
            stderr_sha256: execution.stderr_sha256.clone(),
        })
        .collect()
}

pub fn projection_catalog(
    snippets: &[TypeScriptSnippet],
    snippet_hashes: impl Fn(&TypeScriptSnippet) -> String,
) -> TypeScriptProjectionCatalog {
    TypeScriptProjectionCatalog {
        schema: PROJECTION_CATALOG_SCHEMA,
        projections: snippets
            .iter()
            .map(|snippet| TypeScriptProjectionRecord {
                scenario_id: snippet.scenario_id,
                language: "typescript",
                package: snippet.package,
                path: snippet.path,
                source_sha256: snippet.source_sha256.clone(),
                rust_output_sha256: snippet.rust_output_sha256.clone(),
                snippet_sha256: snippet_hashes(snippet),
            })
            .collect(),
    }
}

/// Execute local examples with a structured Cargo invocation. Endpoint-backed
/// examples deliberately remain receipt-only until their declared service is
/// available.
pub fn execute_local(
    root: &Path,
    sources: &[ScenarioSource],
    cargo_path: Option<&Path>,
) -> Result<Vec<ScenarioExecution>, Error> {
    let mut executions = Vec::new();
    for source in sources {
        if source.scenario.mode != ScenarioMode::ExecuteLocal {
            continue;
        }
        let manifest = root
            .join("rust/crates")
            .join(package_directory(&source.scenario))
            .join("Cargo.toml");
        let cargo = cargo_path.unwrap_or_else(|| Path::new("cargo"));
        let mut command = Command::new(cargo);
        command
            .current_dir(root)
            .args(["run", "--quiet", "--locked", "--manifest-path"])
            .arg(&manifest)
            .args(["--example", source.scenario.example]);
        if !source.scenario.features.is_empty() {
            command
                .arg("--features")
                .arg(source.scenario.features.join(","));
        }
        sanitize_compiler_environment(&mut command);
        let output = command
            .output()
            .map_err(|error| Error::Io(format!("{}: {error}", source.scenario.id)))?;
        if !output.status.success() {
            return Err(Error::Invalid(format!(
                "scenario {} failed: {}",
                source.scenario.id,
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        let stdout = String::from_utf8(output.stdout).map_err(|error| {
            Error::Invalid(format!(
                "scenario {} stdout is not UTF-8: {error}",
                source.scenario.id
            ))
        })?;
        executions.push(ScenarioExecution {
            scenario: source.scenario,
            source_sha256: source.source_sha256.clone(),
            stdout_sha256: digest_bytes(stdout.as_bytes()),
            stderr_sha256: digest_bytes(&output.stderr),
            stdout,
        });
    }
    Ok(executions)
}

fn sanitize_compiler_environment(command: &mut Command) {
    for (key, _) in env::vars_os() {
        let uppercase = key.to_string_lossy().to_ascii_uppercase();
        let remove = matches!(
            uppercase.as_str(),
            "RUSTC"
                | "RUSTDOC"
                | "RUSTFLAGS"
                | "RUSTDOCFLAGS"
                | "CARGO_ENCODED_RUSTFLAGS"
                | "CARGO_ENCODED_RUSTDOCFLAGS"
                | "RUSTC_WRAPPER"
                | "RUSTC_WORKSPACE_WRAPPER"
                | "CARGO_BUILD_TARGET"
                | "RUSTUP_TOOLCHAIN"
        ) || uppercase.starts_with("CARGO_CFG_")
            || (uppercase.starts_with("CARGO_BUILD_") && uppercase != "CARGO_BUILD_JOBS")
            || uppercase.starts_with("CARGO_TARGET_")
            || uppercase.starts_with("RUSTC_")
            || uppercase.starts_with("RUSTDOC_");
        if remove {
            command.env_remove(&key);
        }
    }
}

/// Render the TypeScript consumer from the JSON emitted by the Rust example.
/// The wrapper is deliberately small; every request value comes from the
/// executable output and the installed package remains the runtime authority.
pub fn render_typescript(
    executions: &[ScenarioExecution],
) -> Result<Vec<TypeScriptSnippet>, Error> {
    executions
        .iter()
        .filter(|execution| {
            matches!(
                execution.scenario.kind,
                ScenarioKind::MachinesTypescriptConsumer
                    | ScenarioKind::ActorsTypescriptConsumer
                    | ScenarioKind::StreamTypescriptConsumer
            )
        })
        .map(|execution| match execution.scenario.kind {
            ScenarioKind::MachinesTypescriptConsumer => render_machines_typescript(execution),
            ScenarioKind::ActorsTypescriptConsumer => render_actors_typescript(execution),
            ScenarioKind::StreamTypescriptConsumer => render_stream_typescript(execution),
            _ => unreachable!("filtered scenario kind must have a TypeScript renderer"),
        })
        .collect()
}

fn render_stream_typescript(execution: &ScenarioExecution) -> Result<TypeScriptSnippet, Error> {
    let value: serde_json::Value =
        serde_json::from_str(execution.stdout.trim()).map_err(|error| {
            Error::Invalid(format!(
                "scenario {} did not emit JSON: {error}",
                execution.scenario.id
            ))
        })?;
    let request = value
        .get("request")
        .ok_or_else(|| Error::Invalid("Stream scenario output has no request".into()))?;
    let path = string_field(request, "path")?;
    let values = required_field(request, "values")?;
    let value_arrays = values
        .as_array()
        .ok_or_else(|| Error::Invalid("Stream request values are not an array".into()))?;
    if value_arrays.is_empty() {
        return Err(Error::Invalid("Stream request values are empty".into()));
    }
    let values_literal = value_arrays
        .iter()
        .map(|value| {
            let _ = bytes_hex(value)?;
            serde_json::to_string(value).map_err(|error| Error::Invalid(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");
    let idempotency_key = required_field(request, "idempotency_key")?;
    let _ = bytes_hex(idempotency_key)?;
    let idempotency_literal = serde_json::to_string(idempotency_key)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let read_limit = number_field(request, "read_limit")?;
    let append = value
        .get("append")
        .ok_or_else(|| Error::Invalid("Stream scenario output has no append receipt".into()))?;
    let append_tail = number_field(append, "tail")?;
    let tail = number_field(&value, "tail")?;
    let records = value
        .get("records")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| Error::Invalid("Stream scenario output has no records".into()))?;
    if records.len() != value_arrays.len() {
        return Err(Error::Invalid(
            "Stream read record count differs from request".into(),
        ));
    }
    let record_values = records
        .iter()
        .map(|record| {
            let bytes = required_field(record, "value")?;
            let _ = bytes_hex(bytes)?;
            serde_json::to_string(bytes).map_err(|error| Error::Invalid(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");
    let source = format!(
        "// Generated from Rust scenario {}.\n// Rust output SHA256: {}\nimport {{ StreamClient, idempotencyKey }} from \"@acyclic-labs/stream\";\n\nconst path = {path:?};\nconst values = [{values_literal}].map(value => Uint8Array.from(value));\nconst retry = idempotencyKey(Uint8Array.from({idempotency_literal}));\nconst stream = StreamClient.memory().bytes(path);\nconst append = await stream.appendBatch(values, {{ idempotencyKey: retry }});\nif (!append.ok || append.tail !== BigInt(\"{append_tail}\")) throw new Error(\"Rust append receipt parity failed\");\nconst tail = await stream.tail();\nif (tail !== BigInt(\"{tail}\")) throw new Error(\"Rust tail parity failed\");\nconst records = [];\nfor await (const record of stream.read({{ from: 0n, limit: {read_limit} }})) records.push(record.value);\nconst expected = [{record_values}].map(value => Uint8Array.from(value));\nif (records.length !== expected.length || records.some((record, index) => record.some((byte, offset) => byte !== expected[index][offset]))) throw new Error(\"Rust read parity failed\");\nconsole.log(JSON.stringify({{ tail: tail.toString(), records: records.length }}));\n",
        execution.scenario.id, execution.stdout_sha256,
    );
    Ok(TypeScriptSnippet {
        scenario_id: execution.scenario.id,
        package: "@acyclic-labs/stream",
        path: "generated/scenarios/stream/typescript-consumer.ts",
        source_sha256: execution.source_sha256.clone(),
        rust_output_sha256: execution.stdout_sha256.clone(),
        source,
    })
}

fn render_actors_typescript(execution: &ScenarioExecution) -> Result<TypeScriptSnippet, Error> {
    let value: serde_json::Value =
        serde_json::from_str(execution.stdout.trim()).map_err(|error| {
            Error::Invalid(format!(
                "scenario {} did not emit JSON: {error}",
                execution.scenario.id
            ))
        })?;
    let request = value
        .get("request")
        .ok_or_else(|| Error::Invalid("Actors scenario output has no request".into()))?;
    let code_sha256 = required_field(request, "code_sha256")?;
    let _code_sha256_hex = bytes_hex(code_sha256)?;
    let home_region = string_field(request, "home_region")?;
    let idempotency_key = string_field(request, "idempotency_key")?;
    let limits = request
        .get("limits")
        .ok_or_else(|| Error::Invalid("Actors request has no limits".into()))?;
    let handler_timeout_millis = number_field(limits, "handler_timeout_millis")?;
    let memory_bytes = number_field(limits, "memory_bytes")?;
    let checkpoint_bytes = number_field(limits, "checkpoint_bytes")?;
    let bindings = required_field(request, "bindings")?;
    let subscriptions = required_field(request, "subscriptions")?;
    if value.get("validated") != Some(&serde_json::Value::Bool(true)) {
        return Err(Error::Invalid(
            "Actors scenario did not report canonical validation".into(),
        ));
    }
    let code_literal =
        serde_json::to_string(code_sha256).map_err(|error| Error::Invalid(error.to_string()))?;
    let region_literal =
        serde_json::to_string(&home_region).map_err(|error| Error::Invalid(error.to_string()))?;
    let idempotency_literal = serde_json::to_string(&idempotency_key)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let bindings_literal =
        serde_json::to_string(bindings).map_err(|error| Error::Invalid(error.to_string()))?;
    let subscriptions_literal =
        serde_json::to_string(subscriptions).map_err(|error| Error::Invalid(error.to_string()))?;
    let source = format!(
        "// Generated from Rust scenario {}.\n// Rust output SHA256: {}\nimport {{ CreateActorRequestSchema }} from \"@acyclic-labs/actors/proto\";\nimport {{ create, toBinary }} from \"@bufbuild/protobuf\";\n\nconst request = create(CreateActorRequestSchema, {{\n  codeSha256: Uint8Array.from({code_literal}),\n  homeRegion: {region_literal},\n  bindings: {bindings_literal},\n  limits: {{\n    handlerTimeoutMillis: BigInt(\"{handler_timeout_millis}\"),\n    memoryBytes: BigInt(\"{memory_bytes}\"),\n    checkpointBytes: BigInt(\"{checkpoint_bytes}\"),\n  }},\n  subscriptions: {subscriptions_literal},\n  idempotencyKey: {idempotency_literal},\n}});\nconst encoded = toBinary(CreateActorRequestSchema, request);\nif (encoded.length === 0) throw new Error(\"Rust Actors request encoded to an empty payload\");\nconsole.log(JSON.stringify({{ homeRegion: request.homeRegion, encodedBytes: encoded.length }}));\n",
        execution.scenario.id, execution.stdout_sha256,
    );
    Ok(TypeScriptSnippet {
        scenario_id: execution.scenario.id,
        package: "@acyclic-labs/actors",
        path: "generated/scenarios/actors/typescript-consumer.ts",
        source_sha256: execution.source_sha256.clone(),
        rust_output_sha256: execution.stdout_sha256.clone(),
        source,
    })
}

fn render_machines_typescript(execution: &ScenarioExecution) -> Result<TypeScriptSnippet, Error> {
    let value: serde_json::Value =
        serde_json::from_str(execution.stdout.trim()).map_err(|error| {
            Error::Invalid(format!(
                "scenario {} did not emit JSON: {error}",
                execution.scenario.id
            ))
        })?;
    let request = value
        .get("request")
        .ok_or_else(|| Error::Invalid("Machines scenario output has no request".into()))?;
    let idempotency_key = string_field(request, "idempotency_key")?;
    let compatibility = policy_kind(request, "compatibility", "BestEffort", "best-effort")?;
    let expiration = policy_kind(request, "expiration", "Never", "never")?;
    let image = request
        .get("image")
        .and_then(|image| image.get("ManagedOci"))
        .ok_or_else(|| Error::Invalid("Machines request has no ManagedOci image".into()))?;
    let image_hex = bytes_hex(image)?;
    let network_hex =
        bytes_hex(request.get("network_policy_digest").ok_or_else(|| {
            Error::Invalid("Machines request has no network policy digest".into())
        })?)?;
    let suspension_policy = request
        .get("suspension")
        .ok_or_else(|| Error::Invalid("Machines request has no suspension policy".into()))?;
    let suspension_kind = nested_policy_kind(suspension_policy, "AfterIdle", "after-idle")?;
    let suspension = suspension_policy
        .get("AfterIdle")
        .ok_or_else(|| Error::Invalid("Machines request has no AfterIdle policy".into()))?;
    let seconds = number_field(suspension, "secs")?;
    let nanos = number_field(suspension, "nanos")?;
    let milliseconds = seconds * 1_000 + nanos / 1_000_000;
    let budgets = request
        .get("budgets")
        .ok_or_else(|| Error::Invalid("Machines request has no budgets".into()))?;
    let spend_micros = number_field(budgets, "spend_micros")?;
    let concurrency = number_field(budgets, "concurrency")?;
    if value
        .get("outcome")
        .and_then(|outcome| outcome.get("Created"))
        .is_none()
    {
        return Err(Error::Invalid(
            "Machines scenario did not create a Created outcome".into(),
        ));
    }
    let page_machines = value
        .get("page")
        .and_then(|page| page.get("machines"))
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| Error::Invalid("Machines scenario has no page machines".into()))?;
    let page_limit = value
        .get("page_size")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| Error::Invalid("Machines scenario has no page size".into()))?;
    let page_size = page_machines.len();
    let page_suspension = page_machines
        .first()
        .and_then(|machine| machine.get("contract"))
        .and_then(|contract| contract.get("suspension"))
        .and_then(|suspension| suspension.get("AfterIdle"))
        .map(|_| "after-idle")
        .ok_or_else(|| Error::Invalid("Machines page has no AfterIdle policy".into()))?;
    let idempotency_literal = serde_json::to_string(&idempotency_key)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let image_literal =
        serde_json::to_string(&format!("registry.example/generated@sha256:{image_hex}"))
            .map_err(|error| Error::Invalid(error.to_string()))?;
    let source = format!(
        "// Generated from Rust scenario {}.\n// Rust output SHA256: {}\nimport {{ SimulatedMachines, idempotencyKey, managedOci, type CreateMachine }} from \"@acyclic-labs/machines\";\n\nconst request = {{\n  idempotencyKey: idempotencyKey({idempotency_literal}),\n  image: managedOci({image_literal}),\n  compatibility: {{ kind: \"{compatibility}\" }},\n  suspension: {{ kind: \"{suspension_kind}\", milliseconds: {milliseconds} }},\n  expiration: {{ kind: \"{expiration}\" }},\n  networkPolicyDigestHex: \"{network_hex}\",\n  budgets: {{ spendMicros: BigInt(\"{spend_micros}\"), concurrency: {concurrency} }},\n}} satisfies CreateMachine;\n\nconst provider = new SimulatedMachines();\nconst outcome = await provider.create(request);\nif (outcome.kind !== \"created\") throw new Error(`expected created outcome, received ${{outcome.kind}}`);\nconst page = await provider.listMachines(null, {page_limit});\nif (page.machines.length !== {page_size} || page.machines[0].contract.suspension.kind !== \"{page_suspension}\") throw new Error(\"Rust scenario parity failed\");\nconsole.log(JSON.stringify({{ kind: outcome.kind, pageSize: page.machines.length, suspension: page.machines[0].contract.suspension.kind }}));\n",
        execution.scenario.id, execution.stdout_sha256,
    );
    Ok(TypeScriptSnippet {
        scenario_id: execution.scenario.id,
        package: "@acyclic-labs/machines",
        path: "generated/scenarios/machines/typescript-consumer.ts",
        source_sha256: execution.source_sha256.clone(),
        rust_output_sha256: execution.stdout_sha256.clone(),
        source,
    })
}

fn nested_policy_kind(
    value: &serde_json::Value,
    rust_variant: &str,
    typescript_kind: &'static str,
) -> Result<&'static str, Error> {
    let object = value
        .as_object()
        .ok_or_else(|| Error::Invalid("Machines policy is not a Rust enum object".into()))?;
    match object.len() {
        1 if object.contains_key(rust_variant) => Ok(typescript_kind),
        _ => Err(Error::Invalid(format!(
            "unsupported Machines policy variant; expected {rust_variant}"
        ))),
    }
}

fn required_field<'a>(
    value: &'a serde_json::Value,
    name: &str,
) -> Result<&'a serde_json::Value, Error> {
    value
        .get(name)
        .ok_or_else(|| Error::Invalid(format!("scenario output has no {name} field")))
}

fn policy_kind(
    value: &serde_json::Value,
    field: &str,
    rust_variant: &str,
    typescript_kind: &'static str,
) -> Result<&'static str, Error> {
    match value.get(field).and_then(serde_json::Value::as_str) {
        Some(value) if value == rust_variant => Ok(typescript_kind),
        Some(value) => Err(Error::Invalid(format!(
            "unsupported Machines {field} policy variant {value}"
        ))),
        None => Err(Error::Invalid(format!(
            "Machines request field {field} is not a Rust enum variant"
        ))),
    }
}

fn string_field(value: &serde_json::Value, name: &str) -> Result<String, Error> {
    value
        .get(name)
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| Error::Invalid(format!("scenario request field {name} is not a string")))
}

fn number_field(value: &serde_json::Value, name: &str) -> Result<u64, Error> {
    value
        .get(name)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| Error::Invalid(format!("scenario request field {name} is not a u64")))
}

fn bytes_hex(value: &serde_json::Value) -> Result<String, Error> {
    let bytes = value
        .as_array()
        .ok_or_else(|| Error::Invalid("scenario byte field is not an array".into()))?;
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let byte = byte
            .as_u64()
            .filter(|byte| *byte <= u8::MAX as u64)
            .ok_or_else(|| {
                Error::Invalid("scenario byte field contains an invalid value".into())
            })?;
        result.push_str(&format!("{byte:02x}"));
    }
    Ok(result)
}

fn digest_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{:x}", hasher.finalize())
}

/// Return the repository-relative paths that must be present in the fixed
/// generator source closure for these scenarios.
pub fn source_paths(root: &Path, sources: &[ScenarioSource]) -> Result<Vec<PathBuf>, Error> {
    let mut paths = std::collections::BTreeSet::new();
    for source in sources {
        for path in &source.source_files {
            let relative = path.strip_prefix(root).map_err(|_| {
                Error::Invalid(format!("scenario source escapes root: {}", path.display()))
            })?;
            paths.insert(relative.to_owned());
        }
    }
    Ok(paths.into_iter().collect())
}

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    Io(String),
    Invalid(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "scenario I/O error: {error}"),
            Self::Invalid(error) => f.write_str(error),
        }
    }
}

impl std::error::Error for Error {}

/// Validate the concrete source closure for every registered scenario.
///
/// The digest covers the example, its package manifest, and the workspace
/// lockfile. A receipt tied to this digest cannot be reused after a source or
/// dependency change. The function does not execute a process or claim that
/// an endpoint scenario has passed.
pub fn validate(root: &Path) -> Result<Vec<ScenarioSource>, Error> {
    let mut seen = std::collections::BTreeSet::new();
    let mut result = Vec::with_capacity(SCENARIOS.len());
    for scenario in SCENARIOS {
        if !seen.insert(scenario.id) {
            return Err(Error::Invalid(format!(
                "duplicate scenario id: {}",
                scenario.id
            )));
        }
        if scenario.id.is_empty()
            || scenario.family.is_empty()
            || scenario.package.is_empty()
            || scenario.example.is_empty()
            || scenario.operation.is_empty()
        {
            return Err(Error::Invalid(format!(
                "scenario {} has an empty identity field",
                scenario.id
            )));
        }
        let source = root.join(scenario.source_path);
        let package_root = root.join("rust/crates").join(package_directory(scenario));
        let manifest = package_root.join("Cargo.toml");
        let lockfile = root.join("Cargo.lock");
        for path in [&source, &manifest, &lockfile] {
            if !path.is_file() {
                return Err(Error::Invalid(format!(
                    "scenario {} source closure is missing {}",
                    scenario.id,
                    path.display()
                )));
            }
        }
        if !manifest_declares_package(&manifest, scenario.package)? {
            return Err(Error::Invalid(format!(
                "scenario {} package {} does not match {}",
                scenario.id,
                scenario.package,
                manifest.display()
            )));
        }
        result.push(ScenarioSource {
            scenario: *scenario,
            source_sha256: digest_files(root, [&source, &manifest, &lockfile])?,
            source_files: vec![source, manifest, lockfile],
        });
    }
    Ok(result)
}

fn manifest_declares_package(manifest: &Path, expected: &str) -> Result<bool, Error> {
    let contents =
        std::fs::read_to_string(manifest).map_err(|error| Error::Io(error.to_string()))?;
    Ok(contents.lines().any(|line| {
        let line = line.trim();
        line.strip_prefix("name = ")
            .and_then(|value| value.strip_prefix('"'))
            .and_then(|value| value.strip_suffix('"'))
            == Some(expected)
    }))
}

fn package_directory(scenario: &Scenario) -> &'static str {
    match scenario.family {
        "actors" => "actors",
        "stream" => "stream",
        "filesystem" => "filesystem",
        family => family,
    }
}

fn digest_files<'a>(
    root: &Path,
    files: impl IntoIterator<Item = &'a PathBuf>,
) -> Result<String, Error> {
    let mut hasher = Sha256::new();
    for path in files {
        let relative = path.strip_prefix(root).map_err(|_| {
            Error::Invalid(format!("scenario source escapes root: {}", path.display()))
        })?;
        let bytes = std::fs::read(path).map_err(|error| Error::Io(error.to_string()))?;
        hasher.update(relative.to_string_lossy().replace('\\', "/").as_bytes());
        hasher.update([0]);
        hasher.update(&bytes);
        hasher.update([0]);
    }
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_contains_one_bounded_scenario_per_requested_family() {
        assert_eq!(SCENARIOS.len(), 6);
        assert_eq!(SCENARIOS[0].kind, ScenarioKind::ActorsUnary);
        assert_eq!(SCENARIOS[1].kind, ScenarioKind::ActorsTypescriptConsumer);
        assert_eq!(SCENARIOS[2].kind, ScenarioKind::StreamStreaming);
        assert_eq!(SCENARIOS[3].kind, ScenarioKind::StreamTypescriptConsumer);
        assert_eq!(SCENARIOS[4].kind, ScenarioKind::FilesystemEmbedded);
        assert_eq!(SCENARIOS[5].kind, ScenarioKind::MachinesTypescriptConsumer);
        assert!(SCENARIOS.iter().all(|scenario| !scenario.id.is_empty()));
    }

    #[test]
    fn current_source_closure_is_present_and_digested() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let sources = validate(&root).expect("registered scenario sources should exist");
        assert_eq!(sources.len(), SCENARIOS.len());
        assert!(
            sources
                .iter()
                .all(|source| source.source_sha256.starts_with("sha256:")
                    && source.source_files.len() == 3)
        );
    }

    #[test]
    fn machines_projection_is_rendered_from_rust_output() {
        let output = include_str!(
            "../../../../research/machines-typescript-scenario-20261007/machines-typescript-consumer.output.json"
        );
        let execution = ScenarioExecution {
            scenario: SCENARIOS[5],
            source_sha256: "sha256:test-source".into(),
            stdout_sha256: digest_bytes(output.as_bytes()),
            stderr_sha256: digest_bytes(&[]),
            stdout: output.into(),
        };
        let snippets = render_typescript(&[execution]).expect("Rust output should render");
        assert_eq!(snippets.len(), 1);
        assert!(snippets[0].source.contains("satisfies CreateMachine"));
        assert!(snippets[0].source.contains("milliseconds: 15000"));
        if let Some(path) = std::env::var_os("SCENARIO_SNIPPET_OUTPUT") {
            std::fs::write(path, snippets[0].source.as_bytes()).expect("write snippet receipt");
        }
    }

    #[test]
    fn actors_projection_is_rendered_from_rust_output() {
        let output = include_str!(
            "../../../../research/machines-typescript-scenario-20261007/actors-typescript-consumer.output.json"
        );
        let execution = ScenarioExecution {
            scenario: SCENARIOS[1],
            source_sha256: "sha256:test-source".into(),
            stdout_sha256: digest_bytes(output.as_bytes()),
            stderr_sha256: digest_bytes(&[]),
            stdout: output.into(),
        };
        let snippets = render_typescript(&[execution]).expect("Rust output should render");
        assert_eq!(snippets.len(), 1);
        assert!(snippets[0].source.contains("CreateActorRequestSchema"));
        assert!(snippets[0].source.contains("BigInt(\"1000\")"));
        if let Some(path) = std::env::var_os("ACTORS_SCENARIO_SNIPPET_OUTPUT") {
            std::fs::write(path, snippets[0].source.as_bytes()).expect("write snippet receipt");
        }
    }

    #[test]
    fn actors_projection_changes_when_rust_output_changes() {
        let output = include_str!(
            "../../../../research/machines-typescript-scenario-20261007/actors-typescript-consumer.output.json"
        );
        let mut changed: serde_json::Value =
            serde_json::from_str(output).expect("Rust output should be JSON");
        changed["request"]["home_region"] = serde_json::Value::String("us".into());
        let changed = serde_json::to_string(&changed).expect("changed Rust output should encode");
        let executions = [
            ScenarioExecution {
                scenario: SCENARIOS[1],
                source_sha256: "sha256:source-a".into(),
                stdout_sha256: digest_bytes(output.as_bytes()),
                stderr_sha256: digest_bytes(&[]),
                stdout: output.into(),
            },
            ScenarioExecution {
                scenario: SCENARIOS[1],
                source_sha256: "sha256:source-b".into(),
                stdout_sha256: digest_bytes(changed.as_bytes()),
                stderr_sha256: digest_bytes(&[]),
                stdout: changed,
            },
        ];
        let snippets = render_typescript(&executions).expect("both Rust outputs should render");
        assert_eq!(snippets.len(), 2);
        assert_ne!(snippets[0].source, snippets[1].source);
        assert!(snippets[1].source.contains("homeRegion: \"us\""));
    }

    #[test]
    fn stream_projection_is_rendered_from_rust_output() {
        let output = r#"{"request":{"path":"typescript/events","values":[[123,34,107,105,110,100,34,58,34,99,114,101,97,116,101,100,34,125],[123,34,107,105,110,100,34,58,34,114,101,97,100,121,34,125]],"idempotency_key":[116,121,112,101,115,99,114,105,112,116,45,115,116,114,101,97,109],"read_limit":2},"append":{"start":0,"end":2,"tail":2},"tail":2,"records":[{"sequence":0,"value":[123,34,107,105,110,100,34,58,34,99,114,101,97,116,101,100,34,125]},{"sequence":1,"value":[123,34,107,105,110,100,34,58,34,114,101,97,100,121,34,125]}]}"#;
        let execution = ScenarioExecution {
            scenario: SCENARIOS[3],
            source_sha256: "sha256:test-source".into(),
            stdout_sha256: digest_bytes(output.as_bytes()),
            stderr_sha256: digest_bytes(&[]),
            stdout: output.into(),
        };
        let snippets = render_typescript(&[execution]).expect("Rust output should render");
        assert_eq!(snippets.len(), 1);
        assert!(snippets[0].source.contains("StreamClient.memory().bytes"));
        assert!(snippets[0].source.contains("typescript/events"));
        assert!(snippets[0].source.contains("Rust read parity failed"));
        if let Some(path) = std::env::var_os("STREAM_SCENARIO_SNIPPET_OUTPUT") {
            std::fs::write(path, snippets[0].source.as_bytes()).expect("write snippet receipt");
        }
    }

    #[test]
    fn stream_projection_changes_when_rust_output_changes() {
        let output = r#"{"request":{"path":"typescript/events","values":[[1]],"idempotency_key":[116],"read_limit":1},"append":{"start":0,"end":1,"tail":1},"tail":1,"records":[{"sequence":0,"value":[1]}]}"#;
        let changed = output.replace("typescript/events", "typescript/other");
        let executions = [
            ScenarioExecution {
                scenario: SCENARIOS[3],
                source_sha256: "sha256:source-a".into(),
                stdout_sha256: digest_bytes(output.as_bytes()),
                stderr_sha256: digest_bytes(&[]),
                stdout: output.into(),
            },
            ScenarioExecution {
                scenario: SCENARIOS[3],
                source_sha256: "sha256:source-b".into(),
                stdout_sha256: digest_bytes(changed.as_bytes()),
                stderr_sha256: digest_bytes(&[]),
                stdout: changed,
            },
        ];
        let snippets = render_typescript(&executions).expect("both Rust outputs should render");
        assert_eq!(snippets.len(), 2);
        assert_ne!(snippets[0].source, snippets[1].source);
        assert!(snippets[1].source.contains("typescript/other"));
    }
}
