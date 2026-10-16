//! Rust-owned executable scenarios used by the generated documentation data.
//!
//! This first registry is intentionally small. It records only examples whose
//! source and invocation are already present in this checkout. The launcher
//! can use [`validate`] before compiling or executing a scenario and can bind
//! the returned source digest to its generation receipt.

use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
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

impl ScenarioKind {
    /// Eligibility shares the renderer's Rust-owned kind selection.
    fn has_typescript_projection(self) -> bool {
        matches!(
            self,
            Self::MachinesTypescriptConsumer
                | Self::ActorsTypescriptConsumer
                | Self::StreamTypescriptConsumer
                | Self::ObjectsTypescriptConsumer
                | Self::InferenceContractDefaults
        )
    }
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
    pub typescript_projection: bool,
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
                typescript_projection: source.scenario.kind.has_typescript_projection(),
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

mod execution;
pub use execution::{compile_all, execute_all, execute_local};
mod render;
use render::digest_bytes;
pub use render::render_typescript;

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
/// The digest covers the example, its package manifest, the workspace
/// lockfile, and any declared endpoint fixture script. A receipt tied to this
/// digest cannot be reused after a source, fixture, or dependency change. The
/// function does not execute a process or claim that an endpoint scenario has
/// passed.
pub fn validate(
    root: &Path,
    metadata: &cargo_metadata::Metadata,
) -> Result<Vec<ScenarioSource>, Error> {
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
        if !manifest_declares_package(metadata, &manifest, scenario.package)? {
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

fn manifest_declares_package(
    metadata: &cargo_metadata::Metadata,
    manifest: &Path,
    expected: &str,
) -> Result<bool, Error> {
    let manifest = manifest
        .canonicalize()
        .map_err(|error| Error::Io(error.to_string()))?;
    Ok(metadata.packages.iter().any(|package| {
        package.name.as_ref() == expected
            && package
                .manifest_path
                .as_std_path()
                .canonicalize()
                .is_ok_and(|path| path == manifest)
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
    fn catalog_exports_renderer_eligibility_from_rust_kinds() {
        let sources: Vec<_> = SCENARIOS
            .iter()
            .map(|scenario| ScenarioSource {
                scenario: *scenario,
                source_sha256: "sha256:fixture".into(),
                source_files: Vec::new(),
            })
            .collect();
        let records = catalog(&sources);
        assert_eq!(records.scenarios.len(), 17);
        assert_eq!(
            records
                .scenarios
                .iter()
                .filter(|record| record.typescript_projection)
                .count(),
            5
        );
        for record in records.scenarios {
            assert_eq!(
                record.typescript_projection,
                record.kind.has_typescript_projection()
            );
            assert_eq!(
                serde_json::to_value(&record).unwrap()["typescriptProjection"],
                record.typescript_projection
            );
        }
    }

    #[test]
    fn registry_contains_requested_consumer_families() {
        assert_eq!(SCENARIOS.len(), 17);
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
    fn cargo_package_owner_requires_the_actual_package_and_manifest() {
        let fixture = tempfile::tempdir().unwrap();
        let manifest = fixture.path().join("Cargo.toml");
        std::fs::create_dir(fixture.path().join("src")).unwrap();
        std::fs::write(fixture.path().join("src/lib.rs"), "pub struct Witness;").unwrap();
        // Cargo accepts compact, single-quoted TOML; metadata names are not package identities.
        std::fs::write(
            &manifest,
            r#"[package]
name='actual-owner'
version='0.1.0'
edition='2021'
[workspace]
[package.metadata]
name = "decoy-owner"
"#,
        )
        .unwrap();
        let metadata = cargo_metadata::MetadataCommand::new()
            .manifest_path(&manifest)
            .no_deps()
            .other_options(vec!["--offline".into()])
            .exec()
            .unwrap();
        assert!(manifest_declares_package(&metadata, &manifest, "actual-owner").unwrap());
        assert!(!manifest_declares_package(&metadata, &manifest, "decoy-owner").unwrap());
        let other = fixture.path().join("other.toml");
        std::fs::copy(&manifest, &other).unwrap();
        assert!(!manifest_declares_package(&metadata, &other, "actual-owner").unwrap());
    }

    #[test]
    fn current_source_closure_is_present_and_digested() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let metadata = crate::rustdoc_profiles::load_metadata(
            root.join("Cargo.toml"),
            crate::rustdoc_profiles::CargoExecutionContext::default(),
        )
        .expect("actual workspace Cargo metadata");
        let sources = validate(&root, &metadata).expect("registered scenario sources should exist");
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
}
