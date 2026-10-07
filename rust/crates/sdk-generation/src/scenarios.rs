//! Rust-owned executable scenarios used by the generated documentation data.
//!
//! This first registry is intentionally small. It records only examples whose
//! source and invocation are already present in this checkout. The launcher
//! can use [`validate`] before compiling or executing a scenario and can bind
//! the returned source digest to its generation receipt.

use serde::Serialize;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

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
    StreamStreaming,
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

/// The first four source-backed scenarios. More examples require an explicit
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
        assert_eq!(SCENARIOS.len(), 4);
        assert_eq!(SCENARIOS[0].kind, ScenarioKind::ActorsUnary);
        assert_eq!(SCENARIOS[1].kind, ScenarioKind::StreamStreaming);
        assert_eq!(SCENARIOS[2].kind, ScenarioKind::FilesystemEmbedded);
        assert_eq!(SCENARIOS[3].kind, ScenarioKind::MachinesTypescriptConsumer);
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
}
