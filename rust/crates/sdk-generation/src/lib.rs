use cargo_metadata::{Metadata, Package, TargetKind};
use sdk_docs::rustdoc_profiles::{
    api_owner_for_package, extract_owned_api, observe_rustdoc, project_into_docs,
    validate_rustdoc_version, ApiOwnerKind, ProfileId, ProfileSpec,
};
use sdk_docs::CommandOutput;
use sdk_docs::{
    build_data, merge_profile_catalog, scenarios, write_bundle, write_bundle_files, BuildInput,
    Channel, GeneratedSource, PackageMetadata,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::OsStr;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

mod generation;
pub mod historical;
mod release_manifest;
use generation::*;
mod drift_check;
use drift_check::*;
mod parallel;
mod profiles;
use profiles::*;
mod toolchains;
use toolchains::*;
mod artifacts;
mod cache_record;
#[path = "../../sdk-docs/src/concrete_versions.rs"]
mod concrete_versions;
mod file_cache;
mod shared_cache;
mod source_selection;
pub use artifacts::materialize_generated_sources;
use artifacts::*;
use file_cache::rustdoc_digest;

#[derive(Debug)]
pub struct CliError(String);

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for CliError {}

#[derive(Debug, Clone)]
struct GenerateArgs {
    root: PathBuf,
    output: PathBuf,
    rustdoc_dir: Option<PathBuf>,
    version: String,
    channel: Channel,
    skip_scenarios: bool,
    execute_profiles: bool,
    historical_archives: Option<PathBuf>,
    historical_source: HistoricalSource,
    registry_frontier_repository: Option<PathBuf>,
    registry_frontier_revisions: BTreeSet<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum HistoricalSource {
    #[default]
    Git,
    RegistryArchives,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
struct GenerationManifest {
    scope: historical::Scope,
    registry_corpus: Option<historical::RegistryCorpus>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    declared_source_toolchain: String,
    schema: String,
    generator: String,
    rustdoc_tool: String,
    cargo_metadata: String,
    #[serde(default)]
    toolchain: String,
    version: String,
    channel: Channel,
    revision: String,
    source_state: String,
    source_sha256: String,
    source_files: BTreeMap<String, String>,
    rustdoc_root: String,
    rustdoc_files: BTreeMap<String, String>,
    profile_availability: String,
    scenarios: String,
    #[serde(default)]
    release_manifest: String,
    artifacts: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProfileReceipt {
    #[serde(skip_serializing_if = "Option::is_none")]
    rustdoc_target: Option<sdk_docs::rustdoc_profiles::RustdocTarget>,
    package: String,
    published_owner: String,
    owner_kind: String,
    target: String,
    default_features: bool,
    features: Vec<String>,
    profile: ProfileId,
    rustdoc_file: String,
    rustdoc_sha256: String,
    rustdoc_version: Option<String>,
    rustdoc_profiles_covered: bool,
    installed_runtime_qualified: bool,
}

#[derive(Debug, Clone)]
struct ExecutedProfile {
    target: sdk_docs::rustdoc_profiles::RustdocTarget,
    receipt: PathBuf,
    spec: ProfileSpec,
}

/// Runs the generator command with explicit arguments.
pub fn run(
    values: impl IntoIterator<Item = String>,
    stdout: &mut dyn Write,
) -> Result<(), CliError> {
    file_cache::with_run(|| sdk_docs::with_rustdoc_cache(|| run_inner(values, stdout)))
}

fn run_inner(
    values: impl IntoIterator<Item = String>,
    stdout: &mut dyn Write,
) -> Result<(), CliError> {
    let mut args = values.into_iter();
    match args.next().as_deref() {
        Some("generate") => generate(parse_generate(args.collect())?, stdout),
        Some("drift") => drift(parse_path_flags(args.collect())?, stdout),
        Some("help") | None => print_help(stdout),
        Some(command) => Err(CliError(format!(
            "unknown command `{command}` (try `help`)"
        ))),
    }
}

fn print_help(stdout: &mut dyn Write) -> Result<(), CliError> {
    writeln!(stdout,
        "sdk-generation generate --root ROOT --output DIR --version VERSION [--rustdoc-dir DIR | --execute-profiles] [--channel preview|release] [--skip-scenarios] [--historical-archives DIR] [--historical-source git|registry-archives] [--registry-frontier-repository REPO --registry-frontier-revisions SHA,...]\nHistorical archives require --channel release --execute-profiles.\nsdk-generation drift --root ROOT --output DIR --rustdoc-dir DIR"
    ).map_err(io_error)
}

fn command_var_os(name: &str) -> Option<std::ffi::OsString> {
    sdk_docs::command_environment()
        .into_iter()
        .find(|(key, _)| {
            key == name
                || cfg!(windows) && key.as_encoded_bytes().eq_ignore_ascii_case(name.as_bytes())
        })
        .map(|(_, value)| value)
}

fn parse_generate(values: Vec<String>) -> Result<GenerateArgs, CliError> {
    let flags = parse_flags(
        values,
        &[
            "root",
            "output",
            "version",
            "rustdoc-dir",
            "execute-profiles",
            "channel",
            "skip-scenarios",
            "historical-archives",
            "historical-source",
            "registry-frontier-repository",
            "registry-frontier-revisions",
        ],
    )?;
    let required = |name: &str| {
        flags
            .get(name)
            .cloned()
            .ok_or_else(|| CliError(format!("missing --{name}")))
    };
    let channel = match flags
        .get("channel")
        .map(String::as_str)
        .unwrap_or("preview")
    {
        "preview" => Channel::Preview,
        "release" => Channel::Release,
        value => return Err(CliError(format!("unknown channel `{value}`"))),
    };
    let rustdoc_dir = flags.get("rustdoc-dir").map(PathBuf::from);
    let execute_profiles = flags.contains_key("execute-profiles") || rustdoc_dir.is_none();
    if rustdoc_dir.is_some() && flags.contains_key("execute-profiles") {
        return Err(CliError(
            "--rustdoc-dir and --execute-profiles are mutually exclusive".into(),
        ));
    }
    if channel == Channel::Release && !execute_profiles {
        return Err(CliError(
            "release generation requires --execute-profiles; hand-supplied Rustdoc receipts are preview-only".into(),
        ));
    }
    let historical_archives = flags.get("historical-archives").map(PathBuf::from);
    let historical_source = match flags
        .get("historical-source")
        .map(String::as_str)
        .unwrap_or("git")
    {
        "git" => HistoricalSource::Git,
        "registry-archives" => HistoricalSource::RegistryArchives,
        value => return Err(CliError(format!("unknown historical source `{value}`"))),
    };
    if flags.contains_key("historical-source") && historical_archives.is_none() {
        return Err(CliError(
            "historical source requires --historical-archives".into(),
        ));
    }
    if historical_archives.is_some()
        && (channel != Channel::Release
            || !execute_profiles
            || flags.contains_key("skip-scenarios"))
    {
        return Err(CliError(
            "historical archives require executed release profiles and all actual Cargo examples"
                .into(),
        ));
    }
    let registry_frontier_repository = flags.get("registry-frontier-repository").map(PathBuf::from);
    let registry_frontier_revisions: BTreeSet<String> = flags
        .get("registry-frontier-revisions")
        .map(|value| value.split(',').map(str::to_owned).collect())
        .unwrap_or_default();
    if (registry_frontier_repository.is_some() || !registry_frontier_revisions.is_empty())
        && historical_source != HistoricalSource::RegistryArchives
    {
        return Err(CliError(
            "registry frontier inputs require registry-archives mode".into(),
        ));
    }
    if registry_frontier_repository.is_some() != !registry_frontier_revisions.is_empty() {
        return Err(CliError(
            "complete registry admission requires both source repository and history anchors"
                .into(),
        ));
    }
    if historical_source == HistoricalSource::RegistryArchives
        && registry_frontier_repository.is_none()
    {
        return Err(CliError(
            "new registry-archive generation requires a complete registry corpus".into(),
        ));
    }
    Ok(GenerateArgs {
        root: required("root")?.into(),
        output: required("output")?.into(),
        rustdoc_dir,
        version: required("version")?,
        channel,
        skip_scenarios: flags.contains_key("skip-scenarios"),
        execute_profiles,
        historical_archives,
        historical_source,
        registry_frontier_repository,
        registry_frontier_revisions,
    })
}

fn parse_path_flags(values: Vec<String>) -> Result<(PathBuf, PathBuf, PathBuf), CliError> {
    let flags = parse_flags(values, &["root", "output", "rustdoc-dir"])?;
    let root = flags
        .get("root")
        .cloned()
        .ok_or_else(|| CliError("missing --root".into()))?;
    let output = flags
        .get("output")
        .cloned()
        .ok_or_else(|| CliError("missing --output".into()))?;
    let rustdoc_dir = flags
        .get("rustdoc-dir")
        .cloned()
        .ok_or_else(|| CliError("missing --rustdoc-dir".into()))?;
    Ok((root.into(), output.into(), rustdoc_dir.into()))
}

fn parse_flags(
    values: Vec<String>,
    allowed: &[&str],
) -> Result<BTreeMap<String, String>, CliError> {
    let mut result = BTreeMap::new();
    let mut iter = values.into_iter();
    while let Some(value) = iter.next() {
        let Some(name) = value.strip_prefix("--") else {
            return Err(CliError(format!("unexpected argument `{value}`")));
        };
        if !allowed.contains(&name) {
            return Err(CliError(format!("unknown option `--{name}`")));
        }
        if matches!(name, "skip-scenarios" | "execute-profiles") {
            result.insert(name.to_owned(), String::new());
            continue;
        }
        let value = iter
            .next()
            .ok_or_else(|| CliError(format!("missing value for --{name}")))?;
        if value.starts_with("--") {
            return Err(CliError(format!("missing value for --{name}")));
        }
        result.insert(name.to_owned(), value);
    }
    Ok(result)
}

#[cfg(test)]
mod tests;
