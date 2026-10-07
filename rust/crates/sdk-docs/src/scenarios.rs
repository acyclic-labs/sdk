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
    WorkersModuleContract,
    WorkersTypescriptConsumer,
    ObjectsTypescriptConsumer,
    InferenceContractDefaults,
    PluginCliHelp,
    NativeRuntimePositionalIo,
    HarnessAuthorityIdContract,
    HarnessChildPageContract,
    HarnessComponentLabelContract,
    HarnessConversationPageContract,
    HarnessCustomExecutor,
    HarnessLimitsContract,
    HarnessPrivateDirectoryPageContract,
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
    /// Optional repository-owned fixture runner for endpoint scenarios.
    pub fixture_script: Option<&'static str>,
}

/// Source-backed scenarios with explicit operation identities. Cargo example
/// discovery alone is deliberately insufficient for publishing user-facing
/// snippets or claiming ownership of a contract example.
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
        fixture_script: Some("typescript/packages/actors/test/grpc-conformance.mjs"),
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
        fixture_script: None,
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
        features: &["http"],
        fixture_script: Some("typescript/packages/stream/test/http-provider-conformance.mjs"),
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
        fixture_script: None,
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
        features: &["local"],
        fixture_script: None,
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
        fixture_script: None,
    },
    Scenario {
        id: "workers/module-contract",
        family: "workers",
        package: "acyclic-workers",
        example: "workers-module-contract",
        source_path: "rust/crates/workers/examples/workers-module-contract.rs",
        operation: "module-contract",
        kind: ScenarioKind::WorkersModuleContract,
        mode: ScenarioMode::ExecuteLocal,
        features: &[],
        fixture_script: None,
    },
    Scenario {
        id: "workers/typescript-consumer",
        family: "workers",
        package: "acyclic-workers",
        example: "workers-typescript-consumer",
        source_path: "rust/crates/workers/examples/workers-typescript-consumer.rs",
        operation: "publish-submit-consumer",
        kind: ScenarioKind::WorkersTypescriptConsumer,
        mode: ScenarioMode::ExecuteLocal,
        features: &[],
        fixture_script: None,
    },
    Scenario {
        id: "objects/typescript-consumer",
        family: "objects",
        package: "acyclic-objects",
        example: "objects-typescript-consumer",
        source_path: "rust/crates/objects/examples/objects-typescript-consumer.rs",
        operation: "memory-put-get-consumer",
        kind: ScenarioKind::ObjectsTypescriptConsumer,
        mode: ScenarioMode::ExecuteLocal,
        features: &[],
        fixture_script: None,
    },
    Scenario {
        id: "inference/typescript-consumer",
        family: "inference",
        package: "acyclic-inference",
        example: "inference-typescript-consumer",
        source_path: "rust/crates/inference/examples/inference-typescript-consumer.rs",
        operation: "contract-defaults",
        kind: ScenarioKind::InferenceContractDefaults,
        mode: ScenarioMode::ExecuteLocal,
        features: &[],
        fixture_script: None,
    },
    Scenario {
        id: "plugin/cli-help",
        family: "plugin",
        package: "acyclic-plugin",
        example: "acyclic",
        source_path: "plugin/src/main.rs",
        operation: "cli-help",
        kind: ScenarioKind::PluginCliHelp,
        mode: ScenarioMode::ExecuteLocal,
        features: &[],
        fixture_script: None,
    },
    Scenario {
        id: "native-runtime/positional-io",
        family: "native-runtime",
        package: "acyclic-native-runtime",
        example: "native-runtime-positional-io",
        source_path: "rust/crates/native-runtime/examples/native-runtime-positional-io.rs",
        operation: "positional-read-write",
        kind: ScenarioKind::NativeRuntimePositionalIo,
        mode: ScenarioMode::ExecuteLocal,
        features: &[],
        fixture_script: None,
    },
    Scenario {
        id: "harness/authority-id-contract",
        family: "harness",
        package: "acyclic-harness",
        example: "authority-id-contract",
        source_path: "rust/crates/harness/examples/authority-id-contract.rs",
        operation: "authority-id-policy",
        kind: ScenarioKind::HarnessAuthorityIdContract,
        mode: ScenarioMode::ExecuteLocal,
        features: &[],
        fixture_script: None,
    },
    Scenario {
        id: "harness/child-page-contract",
        family: "harness",
        package: "acyclic-harness",
        example: "child-page-contract",
        source_path: "rust/crates/harness/examples/child-page-contract.rs",
        operation: "child-page-policy",
        kind: ScenarioKind::HarnessChildPageContract,
        mode: ScenarioMode::ExecuteLocal,
        features: &[],
        fixture_script: None,
    },
    Scenario {
        id: "harness/component-label-contract",
        family: "harness",
        package: "acyclic-harness",
        example: "component-label-contract",
        source_path: "rust/crates/harness/examples/component-label-contract.rs",
        operation: "component-label-policy",
        kind: ScenarioKind::HarnessComponentLabelContract,
        mode: ScenarioMode::ExecuteLocal,
        features: &[],
        fixture_script: None,
    },
    Scenario {
        id: "harness/conversation-page-contract",
        family: "harness",
        package: "acyclic-harness",
        example: "conversation-page-contract",
        source_path: "rust/crates/harness/examples/conversation-page-contract.rs",
        operation: "conversation-page-policy",
        kind: ScenarioKind::HarnessConversationPageContract,
        mode: ScenarioMode::ExecuteLocal,
        features: &[],
        fixture_script: None,
    },
    Scenario {
        id: "harness/custom-executor",
        family: "harness",
        package: "acyclic-harness",
        example: "custom_executor",
        source_path: "rust/crates/harness/examples/custom_executor.rs",
        operation: "custom-executor",
        kind: ScenarioKind::HarnessCustomExecutor,
        mode: ScenarioMode::ExecuteLocal,
        features: &[],
        fixture_script: None,
    },
    Scenario {
        id: "harness/limits-contract",
        family: "harness",
        package: "acyclic-harness",
        example: "limits-contract",
        source_path: "rust/crates/harness/examples/limits-contract.rs",
        operation: "limits-policy",
        kind: ScenarioKind::HarnessLimitsContract,
        mode: ScenarioMode::ExecuteLocal,
        features: &[],
        fixture_script: None,
    },
    Scenario {
        id: "harness/private-directory-page-contract",
        family: "harness",
        package: "acyclic-harness",
        example: "private-directory-page-contract",
        source_path: "rust/crates/harness/examples/private-directory-page-contract.rs",
        operation: "private-directory-page-policy",
        kind: ScenarioKind::HarnessPrivateDirectoryPageContract,
        mode: ScenarioMode::ExecuteLocal,
        features: &[],
        fixture_script: None,
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
    pub fixture_script: Option<&'static str>,
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
                fixture_script: source.scenario.fixture_script,
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

/// Compile every registered scenario with Cargo without treating compiler output as
/// documentation data. This keeps Rust examples as the source of truth for the
/// registry, including examples that require an external endpoint to execute.
pub fn compile_all(
    root: &Path,
    sources: &[ScenarioSource],
    cargo_path: Option<&Path>,
) -> Result<(), Error> {
    let cargo = cargo_path.unwrap_or_else(|| Path::new("cargo"));
    for source in sources {
        let manifest = root
            .join("rust/crates")
            .join(package_directory(&source.scenario))
            .join("Cargo.toml");
        let mut command = Command::new(cargo);
        command
            .current_dir(root)
            .args(["check", "--quiet", "--locked", "--manifest-path"])
            .arg(&manifest);
        if source.scenario.family == "plugin" {
            command.args(["--bin", source.scenario.example]);
        } else {
            command.args(["--example", source.scenario.example]);
        }
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
                "scenario {} failed to compile: {}",
                source.scenario.id,
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
    }
    Ok(())
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
            .arg(&manifest);
        if source.scenario.family == "plugin" {
            command.args(["--bin", source.scenario.example, "--", "--help"]);
        } else {
            command.args(["--example", source.scenario.example]);
        }
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
            // Successful Cargo runs may emit compiler diagnostics only on the
            // first build. They are toolchain/cache noise rather than
            // scenario output, so keep the execution receipt deterministic.
            stderr_sha256: digest_bytes(&[]),
            stdout,
        });
    }
    Ok(executions)
}

/// Execute every registered scenario that has a release-gate invocation.
///
/// Local Cargo examples and endpoint-backed fixture scripts are both selected
/// from the Rust-owned registry. The latter are deliberately invoked through
/// the checked-in fixture path recorded on [`Scenario::fixture_script`], so a
/// generation run cannot silently report the 17 local examples as the full
/// 19-scenario release result.
pub fn execute_all(
    root: &Path,
    sources: &[ScenarioSource],
    cargo_path: Option<&Path>,
    bun_path: Option<&Path>,
) -> Result<Vec<ScenarioExecution>, Error> {
    let mut executions = execute_local(root, sources, cargo_path)?;
    let bun = bun_path.unwrap_or_else(|| Path::new("bun"));
    for source in sources {
        if source.scenario.mode != ScenarioMode::ExecuteWithEndpoint {
            continue;
        }
        let fixture_script = source.scenario.fixture_script.ok_or_else(|| {
            Error::Invalid(format!(
                "endpoint scenario {} has no fixture script",
                source.scenario.id
            ))
        })?;
        let fixture = root.join(fixture_script);
        if !fixture.is_file() {
            return Err(Error::Invalid(format!(
                "scenario {} fixture script is missing {}",
                source.scenario.id,
                fixture.display()
            )));
        }
        let mut command = Command::new(bun);
        command.current_dir(root).arg(&fixture);
        let output = command
            .output()
            .map_err(|error| Error::Io(format!("{}: {error}", source.scenario.id)))?;
        if !output.status.success() {
            return Err(Error::Invalid(format!(
                "scenario {} fixture failed: {}",
                source.scenario.id,
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        let stdout = String::from_utf8(output.stdout).map_err(|error| {
            Error::Invalid(format!(
                "scenario {} fixture stdout is not UTF-8: {error}",
                source.scenario.id
            ))
        })?;
        executions.push(ScenarioExecution {
            scenario: source.scenario,
            source_sha256: source.source_sha256.clone(),
            stdout_sha256: digest_bytes(stdout.as_bytes()),
            stderr_sha256: digest_bytes(&[]),
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
                    | ScenarioKind::WorkersTypescriptConsumer
                    | ScenarioKind::ObjectsTypescriptConsumer
                    | ScenarioKind::InferenceContractDefaults
            )
        })
        .map(|execution| match execution.scenario.kind {
            ScenarioKind::MachinesTypescriptConsumer => render_machines_typescript(execution),
            ScenarioKind::ActorsTypescriptConsumer => render_actors_typescript(execution),
            ScenarioKind::StreamTypescriptConsumer => render_stream_typescript(execution),
            ScenarioKind::WorkersTypescriptConsumer => render_workers_typescript(execution),
            ScenarioKind::ObjectsTypescriptConsumer => render_objects_typescript(execution),
            ScenarioKind::InferenceContractDefaults => render_inference_typescript(execution),
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
        "// Generated from Rust scenario {}.\n// Rust output SHA256: {}\nimport {{ StreamClient, idempotencyKey }} from \"@acyclic-labs/stream\";\n\nconst path = {path:?};\nconst values = [{values_literal}].map(value => Uint8Array.from(value));\nconst retry = idempotencyKey(Uint8Array.from({idempotency_literal}));\nconst stream = StreamClient.memory().bytes(path);\nconst append = await stream.appendBatch(values, {{ idempotencyKey: retry }});\nif (!append.ok || append.tail !== BigInt(\"{append_tail}\")) throw new Error(\"Rust append receipt parity failed\");\nconst tail = await stream.tail();\nif (tail !== BigInt(\"{tail}\")) throw new Error(\"Rust tail parity failed\");\nconst records = [];\nfor await (const record of stream.read({{ from: 0n, limit: {read_limit} }})) records.push(record.value);\nconst expected = [{record_values}].map(value => Uint8Array.from(value));\nconst recordsDiffer = records.some((record, index) => {{\n  const expectedRecord = expected[index];\n  return expectedRecord === undefined || record.length !== expectedRecord.length || record.some((byte, offset) => byte !== expectedRecord[offset]);\n}});\nif (records.length !== expected.length || recordsDiffer) throw new Error(\"Rust read parity failed\");\nconsole.log(JSON.stringify({{ tail: tail.toString(), records: records.length }}));\n",
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
    let bindings_literal = bindings
        .as_array()
        .ok_or_else(|| Error::Invalid("Actors bindings are not an array".into()))?
        .iter()
        .map(|binding| {
            let name = string_field(binding, "name")?;
            let capability = string_field(binding, "capability")?;
            let resource = string_field(binding, "resource")?;
            Ok(format!(
                "create(BindingSchema, {{ name: {name:?}, capability: {capability:?}, resource: {resource:?} }})"
            ))
        })
        .collect::<Result<Vec<_>, Error>>()?
        .join(", ");
    let subscriptions_literal = subscriptions
        .as_array()
        .ok_or_else(|| Error::Invalid("Actors subscriptions are not an array".into()))?
        .iter()
        .map(|subscription| {
            let subscription_id = string_field(subscription, "subscription_id")?;
            let stream_path = string_field(subscription, "stream_path")?;
            let placement_anchor = subscription
                .get("placement_anchor")
                .and_then(serde_json::Value::as_bool)
                .ok_or_else(|| Error::Invalid("Actor subscription anchor is not a bool".into()))?;
            let start = subscription
                .get("start")
                .and_then(|start| start.get("cursor"))
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| Error::Invalid("Actor subscription cursor is absent".into()))?;
            Ok(format!(
                "create(SubscriptionSpecSchema, {{ subscriptionId: {subscription_id:?}, streamPath: {stream_path:?}, start: create(SubscriptionStartSchema, {{ start: {{ case: \"cursor\", value: BigInt(\"{start}\") }} }}), placementAnchor: {placement_anchor} }})"
            ))
        })
        .collect::<Result<Vec<_>, Error>>()?
        .join(", ");
    let source = format!(
        "// Generated from Rust scenario {}.\n// Rust output SHA256: {}\nimport {{ ActorLimitsSchema, BindingSchema, CreateActorRequestSchema, SubscriptionSpecSchema, SubscriptionStartSchema }} from \"@acyclic-labs/actors/proto\";\nimport {{ create, toBinary }} from \"@bufbuild/protobuf\";\n\nconst request = create(CreateActorRequestSchema, {{\n  codeSha256: Uint8Array.from({code_literal}),\n  homeRegion: {region_literal},\n  bindings: [{bindings_literal}],\n  limits: create(ActorLimitsSchema, {{\n    handlerTimeoutMillis: BigInt(\"{handler_timeout_millis}\"),\n    memoryBytes: BigInt(\"{memory_bytes}\"),\n    checkpointBytes: BigInt(\"{checkpoint_bytes}\"),\n  }}),\n  subscriptions: [{subscriptions_literal}],\n  idempotencyKey: {idempotency_literal},\n}});\nconst encoded = toBinary(CreateActorRequestSchema, request);\nif (encoded.length === 0) throw new Error(\"Rust Actors request encoded to an empty payload\");\nconsole.log(JSON.stringify({{ homeRegion: request.homeRegion, encodedBytes: encoded.length }}));\n",
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
        "// Generated from Rust scenario {}.\n// Rust output SHA256: {}\nimport {{ SimulatedMachines, idempotencyKey, managedOci, type CreateMachine }} from \"@acyclic-labs/machines\";\n\nconst request = {{\n  idempotencyKey: idempotencyKey({idempotency_literal}),\n  image: managedOci({image_literal}),\n  compatibility: {{ kind: \"{compatibility}\" }},\n  suspension: {{ kind: \"{suspension_kind}\", milliseconds: {milliseconds} }},\n  expiration: {{ kind: \"{expiration}\" }},\n  networkPolicyDigestHex: \"{network_hex}\",\n  budgets: {{ spendMicros: BigInt(\"{spend_micros}\"), concurrency: {concurrency} }},\n}} satisfies CreateMachine;\n\nconst provider = new SimulatedMachines();\nconst outcome = await provider.create(request);\nif (outcome.kind !== \"created\") throw new Error(`expected created outcome, received ${{outcome.kind}}`);\nconst page = await provider.listMachines(null, {page_limit});\nconst firstMachine = page.machines[0];\nif (page.machines.length !== {page_size} || firstMachine === undefined || firstMachine.contract.suspension.kind !== \"{page_suspension}\") throw new Error(\"Rust scenario parity failed\");\nconsole.log(JSON.stringify({{ kind: outcome.kind, pageSize: page.machines.length, suspension: firstMachine.contract.suspension.kind }}));\n",
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

fn render_workers_typescript(execution: &ScenarioExecution) -> Result<TypeScriptSnippet, Error> {
    let value: serde_json::Value =
        serde_json::from_str(execution.stdout.trim()).map_err(|error| {
            Error::Invalid(format!(
                "scenario {} did not emit JSON: {error}",
                execution.scenario.id
            ))
        })?;
    if value.get("validated") != Some(&serde_json::Value::Bool(true)) {
        return Err(Error::Invalid(
            "Workers scenario did not report validation".into(),
        ));
    }
    let module = required_field(&value, "module")?;
    let digest = required_field(&value, "digest")?;
    let input = required_field(&value, "input")?;
    let module_literal = array_literal(module)?;
    let digest_literal = array_literal(digest)?;
    let input_literal = array_literal(input)?;
    let alias = string_field(&value, "alias")?;
    let publish_idempotency = string_field(&value, "publish_idempotency")?;
    let job_idempotency = string_field(&value, "job_idempotency")?;
    let max_attempts = number_field(&value, "max_attempts")?;
    let timeout = number_field(&value, "timeout_millis")?;
    let memory = number_field(&value, "memory_bytes")?;
    let output = number_field(&value, "output_bytes")?;
    let backoff = number_field(&value, "backoff_millis")?;
    let source = format!(
        "// Generated from Rust scenario {}.\n// Rust output SHA256: {}\nimport {{ create, toBinary }} from \"@bufbuild/protobuf\";\nimport {{ JobLimitsSchema, JobTargetSchema, PayloadSchema, PublishVersionRequestSchema, RetryPolicySchema, SubmitJobRequestSchema }} from \"@acyclic-labs/workers/proto\";\n\nconst module = Uint8Array.from({module_literal});\nconst digest = Uint8Array.from({digest_literal});\nconst publication = create(PublishVersionRequestSchema, {{ javascriptModule: module, expectedSha256: digest, idempotencyKey: {publish_idempotency:?} }});\nconst submission = create(SubmitJobRequestSchema, {{\n  target: create(JobTargetSchema, {{ target: {{ case: \"deploymentAlias\", value: {alias:?} }} }}),\n  input: create(PayloadSchema, {{ source: {{ case: \"inlineBytes\", value: Uint8Array.from({input_literal}) }} }}),\n  limits: create(JobLimitsSchema, {{ timeoutMillis: BigInt(\"{timeout}\"), memoryBytes: BigInt(\"{memory}\"), outputBytes: BigInt(\"{output}\") }}),\n  retry: create(RetryPolicySchema, {{ maxAttempts: {max_attempts}, backoffMillis: BigInt(\"{backoff}\") }}),\n  idempotencyKey: {job_idempotency:?},\n}});\nif (toBinary(PublishVersionRequestSchema, publication).length === 0 || toBinary(SubmitJobRequestSchema, submission).length === 0) throw new Error(\"Workers request encoded to an empty payload\");\nconsole.log(JSON.stringify({{ validated: true, moduleBytes: module.length, attempts: submission.retry?.maxAttempts }}));\n",
        execution.scenario.id, execution.stdout_sha256,
    );
    Ok(TypeScriptSnippet {
        scenario_id: execution.scenario.id,
        package: "@acyclic-labs/workers",
        path: "generated/scenarios/workers/typescript-consumer.ts",
        source_sha256: execution.source_sha256.clone(),
        rust_output_sha256: execution.stdout_sha256.clone(),
        source,
    })
}

fn render_objects_typescript(execution: &ScenarioExecution) -> Result<TypeScriptSnippet, Error> {
    let value: serde_json::Value =
        serde_json::from_str(execution.stdout.trim()).map_err(|error| {
            Error::Invalid(format!(
                "scenario {} did not emit JSON: {error}",
                execution.scenario.id
            ))
        })?;
    let bucket = string_field(&value, "bucket")?;
    let key = string_field(&value, "key")?;
    let body = string_field(&value, "body")?;
    let size = number_field(&value, "size")?;
    let source = format!(
        "// Generated from Rust scenario {}.\n// Rust output SHA256: {}\nimport {{ create }} from \"@bufbuild/protobuf\";\nimport {{ BucketRefSchema, CreateBucketRequestSchema, GetObjectRequestSchema, MemoryObjectsV2, PutObjectHeaderSchema }} from \"@acyclic-labs/objects/v2\";\n\nconst bucket = {bucket:?};\nconst key = {key:?};\nconst body = new TextEncoder().encode({body:?});\nconst provider = await MemoryObjectsV2.create();\nawait provider.createBucket(create(CreateBucketRequestSchema, {{ name: bucket }}));\nconst written = await provider.put(create(PutObjectHeaderSchema, {{ bucket: create(BucketRefSchema, {{ name: bucket }}), objectKey: key }}), body);\nif (written.size !== BigInt(\"{size}\")) throw new Error(\"Rust Objects size parity failed\");\nconst selected = await provider.get(create(GetObjectRequestSchema, {{ bucket: create(BucketRefSchema, {{ name: bucket }}), objectKey: key }}), 1024n);\nif (new TextDecoder().decode(selected.body) !== {body:?}) throw new Error(\"Rust Objects body parity failed\");\nconsole.log(JSON.stringify({{ bucket, key, size: Number(written.size) }}));\n",
        execution.scenario.id, execution.stdout_sha256,
    );
    Ok(TypeScriptSnippet {
        scenario_id: execution.scenario.id,
        package: "@acyclic-labs/objects",
        path: "generated/scenarios/objects/typescript-consumer.ts",
        source_sha256: execution.source_sha256.clone(),
        rust_output_sha256: execution.stdout_sha256.clone(),
        source,
    })
}

fn render_inference_typescript(execution: &ScenarioExecution) -> Result<TypeScriptSnippet, Error> {
    let value: serde_json::Value =
        serde_json::from_str(execution.stdout.trim()).map_err(|error| {
            Error::Invalid(format!(
                "scenario {} did not emit JSON: {error}",
                execution.scenario.id
            ))
        })?;
    let message = number_field(&value, "maximum_message_bytes")?;
    let http = number_field(&value, "maximum_http_json_bytes")?;
    let source = format!(
        "// Generated from Rust scenario {}.\n// Rust output SHA256: {}\nimport {{ create }} from \"@bufbuild/protobuf\";\nimport {{ RequestIdentitySchema }} from \"@acyclic-labs/inference/proto\";\n\nconst identity = create(RequestIdentitySchema);\nif (identity.requestId.byteLength !== 0) throw new Error(\"generated identity default changed\");\nconst maximumMessageBytes = {message};\nconst maximumHttpJsonBytes = {http};\nconsole.log(JSON.stringify({{ maximumMessageBytes, maximumHttpJsonBytes }}));\n",
        execution.scenario.id, execution.stdout_sha256,
    );
    Ok(TypeScriptSnippet {
        scenario_id: execution.scenario.id,
        package: "@acyclic-labs/inference",
        path: "generated/scenarios/inference/typescript-consumer.ts",
        source_sha256: execution.source_sha256.clone(),
        rust_output_sha256: execution.stdout_sha256.clone(),
        source,
    })
}

fn array_literal(value: &serde_json::Value) -> Result<String, Error> {
    value
        .as_array()
        .ok_or_else(|| Error::Invalid("scenario byte field is not an array".into()))?
        .iter()
        .map(|byte| {
            byte.as_u64()
                .filter(|value| *value <= u8::MAX as u64)
                .map(|value| value.to_string())
                .ok_or_else(|| {
                    Error::Invalid("scenario byte field contains an invalid value".into())
                })
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|values| format!("[{}]", values.join(", ")))
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
        let mut source_files = vec![source, manifest.clone(), lockfile];
        if let Some(fixture_script) = scenario.fixture_script {
            source_files.push(root.join(fixture_script));
        }
        for path in &source_files {
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
            source_sha256: digest_files(root, source_files.iter())?,
            source_files,
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
        "plugin" => "../../plugin",
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
    fn registry_contains_requested_consumer_families() {
        assert_eq!(SCENARIOS.len(), 19);
        assert!(SCENARIOS
            .iter()
            .any(|scenario| scenario.id == "actors/typescript-consumer"));
        assert!(SCENARIOS
            .iter()
            .any(|scenario| scenario.id == "stream/typescript-consumer"));
        assert!(SCENARIOS
            .iter()
            .any(|scenario| scenario.id == "machines/typescript-consumer"));
        assert!(SCENARIOS
            .iter()
            .any(|scenario| scenario.id == "workers/typescript-consumer"));
        assert!(SCENARIOS
            .iter()
            .any(|scenario| scenario.id == "objects/typescript-consumer"));
        assert!(SCENARIOS
            .iter()
            .any(|scenario| scenario.id == "inference/typescript-consumer"));
        assert!(SCENARIOS
            .iter()
            .any(|scenario| scenario.id == "plugin/cli-help"));
        assert!(SCENARIOS
            .iter()
            .any(|scenario| scenario.id == "native-runtime/positional-io"));
        assert_eq!(
            SCENARIOS
                .iter()
                .filter(|scenario| scenario.family == "harness")
                .count(),
            7
        );
        assert_eq!(
            SCENARIOS
                .iter()
                .filter(|scenario| scenario.mode == ScenarioMode::ExecuteWithEndpoint)
                .count(),
            2
        );
        assert!(SCENARIOS
            .iter()
            .filter(|scenario| scenario.mode == ScenarioMode::ExecuteWithEndpoint)
            .all(|scenario| scenario.fixture_script.is_some()));
        assert!(SCENARIOS
            .iter()
            .filter(|scenario| scenario.mode == ScenarioMode::ExecuteLocal)
            .all(|scenario| scenario.fixture_script.is_none()));
    }

    #[test]
    fn current_source_closure_is_present_and_digested() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let sources = validate(&root).expect("registered scenario sources should exist");
        assert_eq!(sources.len(), SCENARIOS.len());
        assert!(sources.iter().all(|source| {
            source.source_sha256.starts_with("sha256:")
                && source.source_files.len()
                    == if source.scenario.mode == ScenarioMode::ExecuteWithEndpoint {
                        4
                    } else {
                        3
                    }
        }));
    }

    #[test]
    fn projection_catalog_binds_source_and_output() {
        let execution = ScenarioExecution {
            scenario: SCENARIOS[1],
            source_sha256: "sha256:source".into(),
            stdout: "{}".into(),
            stdout_sha256: "sha256:output".into(),
            stderr_sha256: "sha256:empty".into(),
        };
        let catalog = execution_catalog(&[execution]);
        assert_eq!(catalog[0].id, "actors/typescript-consumer");
        assert_eq!(catalog[0].source_sha256, "sha256:source");
    }

    #[test]
    fn rendered_projection_changes_when_rust_scenario_output_changes() {
        let scenario = SCENARIOS
            .iter()
            .find(|scenario| scenario.id == "workers/typescript-consumer")
            .copied()
            .expect("Workers TypeScript scenario is registered");
        let stdout = r#"{"validated":true,"module":[1],"digest":[2],"alias":"current","publish_idempotency":"publish-example","job_idempotency":"job-example","input":[3],"max_attempts":2,"timeout_millis":1000,"memory_bytes":4194304,"output_bytes":4096,"backoff_millis":25}"#;
        let changed_stdout = stdout.replace("current", "canary");
        let execution = |stdout: String| ScenarioExecution {
            stdout_sha256: digest_bytes(stdout.as_bytes()),
            stdout,
            scenario,
            source_sha256: "sha256:source".into(),
            stderr_sha256: digest_bytes(&[]),
        };
        let original = render_typescript(&[execution(stdout.into())])
            .expect("Rust scenario output should render")
            .remove(0);
        let changed = render_typescript(&[execution(changed_stdout)])
            .expect("mutated Rust scenario output should render")
            .remove(0);
        assert_ne!(original.rust_output_sha256, changed.rust_output_sha256);
        assert_ne!(original.source, changed.source);
        assert!(changed.source.contains("canary"));
    }
}
