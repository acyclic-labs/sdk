use cargo_metadata::Metadata;
use sdk_docs::{
    Channel,
    scenarios::{ScenarioMode, ScenarioSource},
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

pub const SCHEMA: &str = "sdk-generation-release-manifest.v1";
pub const FILE_NAME: &str = "sdk-generation-release-manifest.v1.json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Manifest {
    pub schema: String,
    pub version: String,
    pub channel: Channel,
    pub revision: String,
    pub source_state: String,
    pub source_sha256: String,
    pub source_files: BTreeMap<String, String>,
    pub scenarios: Vec<Scenario>,
    pub artifacts: Vec<Artifact>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Scenario {
    pub id: String,
    pub package: String,
    pub cargo_version: String,
    pub example: String,
    pub source_path: String,
    pub operation: String,
    pub mode: String,
    pub features: Vec<String>,
    pub fixture_script: Option<String>,
    pub source_sha256: String,
    pub source_files: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Artifact {
    pub kind: String,
    pub path: String,
    pub sha256: String,
}

pub fn output_path(output: &Path, channel: &Channel, version: &str) -> Result<PathBuf, String> {
    let channel_dir = match channel {
        Channel::Preview => "preview",
        Channel::Release => "releases",
    };
    Ok(output
        .join(channel_dir)
        .join(sdk_docs::safe_version(version).map_err(|error| error.to_string())?)
        .join(FILE_NAME))
}

pub fn build(
    root: &Path,
    output: &Path,
    version: &str,
    channel: Channel,
    revision: &str,
    source_state: &str,
    source_sha256: &str,
    source_files: &BTreeMap<String, String>,
    metadata: &Metadata,
    scenarios: Option<&[ScenarioSource]>,
    artifact_hashes: &BTreeMap<String, String>,
    scenario_artifacts: &[PathBuf],
) -> Result<Manifest, String> {
    let mut records = Vec::new();
    if let Some(scenarios) = scenarios {
        for source in scenarios {
            let package = metadata
                .packages
                .iter()
                .find(|package| package.name.as_ref() == source.scenario.package)
                .ok_or_else(|| {
                    format!(
                        "scenario {} package {} is absent from Cargo metadata",
                        source.scenario.id, source.scenario.package
                    )
                })?;
            let mut files = BTreeMap::new();
            for path in &source.source_files {
                let relative = path.strip_prefix(root).map_err(|_| {
                    format!(
                        "scenario {} source file escapes repository root: {}",
                        source.scenario.id,
                        path.display()
                    )
                })?;
                let relative = path_string(relative);
                let digest = match source_files.get(&relative) {
                    Some(digest) => digest.clone(),
                    None => super::sha256_file(path)
                        .map_err(|error| format!("scenario source file {relative}: {error}"))?,
                };
                files.insert(relative, digest);
            }
            records.push(Scenario {
                id: source.scenario.id.to_owned(),
                package: source.scenario.package.to_owned(),
                cargo_version: package.version.to_string(),
                example: source.scenario.example.to_owned(),
                source_path: source.scenario.source_path.to_owned(),
                operation: source.scenario.operation.to_owned(),
                mode: mode_name(source.scenario.mode).to_owned(),
                features: source
                    .scenario
                    .features
                    .iter()
                    .map(|feature| (*feature).to_owned())
                    .collect(),
                fixture_script: source.scenario.fixture_script.map(str::to_owned),
                source_sha256: source.source_sha256.clone(),
                source_files: files,
            });
        }
    }
    records.sort_by(|left, right| left.id.cmp(&right.id));

    let mut paths = BTreeSet::from(["sdk-docs-scenarios.v1.json".to_owned()]);
    if scenarios.is_some() {
        paths.insert("sdk-docs-scenario-executions.v1.json".to_owned());
        paths.insert("sdk-docs-scenario-projections.v1.json".to_owned());
    }
    for path in scenario_artifacts {
        let relative = path.strip_prefix(output).map_err(|_| {
            format!(
                "scenario artifact escapes output directory: {}",
                path.display()
            )
        })?;
        paths.insert(path_string(relative));
    }
    let artifacts = paths
        .into_iter()
        .map(|path| {
            let digest = artifact_hashes.get(&path).cloned().ok_or_else(|| {
                format!("release manifest artifact is absent from output: {path}")
            })?;
            let kind = if path == "sdk-docs-scenarios.v1.json" {
                "scenario-catalog"
            } else if path == "sdk-docs-scenario-executions.v1.json" {
                "scenario-executions"
            } else if path == "sdk-docs-scenario-projections.v1.json" {
                "scenario-projections"
            } else {
                "typescript-snippet"
            };
            Ok(Artifact {
                kind: kind.to_owned(),
                path,
                sha256: digest,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    if artifacts.is_empty() {
        return Err("release manifest has no scenario artifacts".into());
    }
    Ok(Manifest {
        schema: SCHEMA.into(),
        version: version.to_owned(),
        channel,
        revision: revision.to_owned(),
        source_state: source_state.to_owned(),
        source_sha256: source_sha256.to_owned(),
        source_files: source_files.clone(),
        scenarios: records,
        artifacts,
    })
}

pub fn validate(
    root: &Path,
    path: &Path,
    expected_version: &str,
    expected_channel: &Channel,
    expected_revision: &str,
    expected_source_state: &str,
    expected_source_sha256: &str,
    expected_source_files: &BTreeMap<String, String>,
    expected_artifacts: &BTreeMap<String, String>,
) -> Result<(), String> {
    let bytes =
        fs::read(path).map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    let manifest: Manifest = serde_json::from_slice(&bytes)
        .map_err(|error| format!("invalid release manifest {}: {error}", path.display()))?;
    if manifest.schema != SCHEMA
        || manifest.version != expected_version
        || &manifest.channel != expected_channel
        || manifest.revision != expected_revision
        || manifest.source_state != expected_source_state
        || manifest.source_sha256 != expected_source_sha256
        || manifest.source_files != *expected_source_files
    {
        return Err(format!(
            "release manifest provenance does not match {}",
            path.display()
        ));
    }
    for artifact in &manifest.artifacts {
        let expected = expected_artifacts.get(&artifact.path).ok_or_else(|| {
            format!(
                "release manifest artifact is absent from output: {}",
                artifact.path
            )
        })?;
        if expected != &artifact.sha256 {
            return Err(format!(
                "release manifest artifact hash changed: {}",
                artifact.path
            ));
        }
    }
    for scenario in &manifest.scenarios {
        for (relative, expected) in &scenario.source_files {
            let actual = super::sha256_file(&root.join(relative))
                .map_err(|error| format!("scenario source file {relative}: {error}"))?;
            if &actual != expected {
                return Err(format!(
                    "scenario source digest changed: {} ({relative})",
                    scenario.id
                ));
            }
        }
    }
    Ok(())
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn mode_name(mode: ScenarioMode) -> &'static str {
    match mode {
        ScenarioMode::Compile => "compile",
        ScenarioMode::ExecuteLocal => "executeLocal",
        ScenarioMode::ExecuteWithEndpoint => "executeWithEndpoint",
    }
}

