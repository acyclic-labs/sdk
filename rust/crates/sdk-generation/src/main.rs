use cargo_metadata::{Metadata, Package, TargetKind};
use sdk_docs::rustdoc_profiles::{
    api_owner_for_package, extract_owned_api_for_crate, observe_rustdoc, project_into_docs,
    validate_rustdoc_version, ProfileId, ProfileSpec,
};
use sdk_docs::{
    build_data, scenarios, write_bundle, BuildInput, Channel, GeneratedSource, PackageMetadata,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::OsStr;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Debug)]
struct CliError(String);

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
    rustdoc_dir: PathBuf,
    version: String,
    channel: Channel,
    skip_scenarios: bool,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
struct GenerationManifest {
    schema: String,
    generator: String,
    rustdoc_tool: String,
    cargo_metadata: String,
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
    artifacts: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProfileReceipt {
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

fn main() {
    if let Err(error) = run() {
        eprintln!("sdk-generation: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), CliError> {
    let mut args = env::args().skip(1);
    match args.next().as_deref() {
        Some("generate") => generate(parse_generate(args.collect())?),
        Some("drift") => drift(parse_path_flags(args.collect())?),
        Some("help") | None => {
            print_help();
            Ok(())
        }
        Some(command) => Err(CliError(format!(
            "unknown command `{command}` (try `help`)"
        ))),
    }
}

fn print_help() {
    println!(
        "sdk-generation generate --root ROOT --output DIR --rustdoc-dir DIR --version VERSION [--channel preview|release] [--skip-scenarios]\nsdk-generation drift --root ROOT --output DIR --rustdoc-dir DIR"
    );
}

fn parse_generate(values: Vec<String>) -> Result<GenerateArgs, CliError> {
    let flags = parse_flags(values)?;
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
    Ok(GenerateArgs {
        root: required("root")?.into(),
        output: required("output")?.into(),
        rustdoc_dir: required("rustdoc-dir")?.into(),
        version: required("version")?,
        channel,
        skip_scenarios: flags.contains_key("skip-scenarios"),
    })
}

fn parse_path_flags(values: Vec<String>) -> Result<(PathBuf, PathBuf, PathBuf), CliError> {
    let flags = parse_flags(values)?;
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

fn parse_flags(values: Vec<String>) -> Result<BTreeMap<String, String>, CliError> {
    let mut result = BTreeMap::new();
    let mut iter = values.into_iter();
    while let Some(value) = iter.next() {
        let Some(name) = value.strip_prefix("--") else {
            return Err(CliError(format!("unexpected argument `{value}`")));
        };
        if name == "skip-scenarios" {
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

fn generate(args: GenerateArgs) -> Result<(), CliError> {
    let root = canonical(&args.root)?;
    let output = absolute(&args.output)?;
    let rustdoc_dir = canonical(&args.rustdoc_dir)?;
    let metadata = load_metadata(&root)?;
    let revision = git_revision(&root)?;
    let source_files = source_file_hashes(&root, &output)?;
    let source_sha256 = digest_map(&source_files);
    let source_state = if args.channel == Channel::Release {
        "captured-snapshot"
    } else {
        "working-tree"
    };
    if args.channel == Channel::Release && git_dirty(&root)? {
        return Err(CliError(
            "release generation requires a clean source checkout".into(),
        ));
    }
    if args.channel == Channel::Release && args.skip_scenarios {
        return Err(CliError(
            "release generation cannot skip the registered scenario source closure".into(),
        ));
    }

    let receipts = rustdoc_files(&rustdoc_dir)?;
    if receipts.is_empty() {
        return Err(CliError(format!(
            "no .json Rustdoc receipts found in {}",
            rustdoc_dir.display()
        )));
    }
    let mut rustdoc_files = Vec::new();
    let mut package_metadata = Vec::new();
    let mut profile_items = Vec::new();
    let mut profiles = BTreeMap::new();
    let mut receipt_manifest = BTreeMap::new();
    for receipt in receipts {
        let observation = observe_rustdoc(&receipt).map_err(profile_error)?;
        let source_attestation = receipt_source_attestation(&receipt)?;
        if source_attestation.as_deref() != Some(source_sha256.as_str()) {
            return Err(CliError(format!(
                "Rustdoc receipt {} is missing a matching source attestation (expected {})",
                receipt.display(),
                source_sha256
            )));
        }
        let package = package_for_crate(&metadata, &observation.crate_name)?;
        validate_rustdoc_version(&metadata, &package.name.to_string(), &observation)
            .map_err(profile_error)?;
        let owner =
            api_owner_for_package(&metadata, &package.name.to_string()).map_err(profile_error)?;
        let profile_spec = ProfileSpec {
            package: package.name.to_string(),
            target: observation.target.clone(),
            default_features: true,
            features: BTreeSet::new(),
        };
        let profile_id = profile_spec.id();
        let crate_name = observation.crate_name.clone();
        let items = extract_owned_api_for_crate(&receipt, &owner, profile_id.clone(), &crate_name)
            .map_err(profile_error)?;
        profile_items.extend(items);
        profiles.insert(profile_id.clone(), profile_spec.clone());
        let receipt_key = rustdoc_key(&rustdoc_dir, &receipt)?;
        receipt_manifest.insert(receipt_key, sha256_file(&receipt)?);
        rustdoc_files.push(receipt.clone());
        package_metadata.push(PackageMetadata {
            rustdoc_file: receipt,
            package_name: package.name.to_string(),
            crate_name,
            version: package.version.to_string(),
        });
    }

    let published = published_packages(&root)?;
    let found = package_metadata
        .iter()
        .map(|metadata| metadata.package_name.clone())
        .collect::<BTreeSet<_>>();
    for owner in published {
        if !found.contains(&owner) {
            return Err(CliError(format!(
                "published owner `{owner}` has no Rustdoc receipt"
            )));
        }
    }
    rustdoc_files.sort();
    package_metadata.sort_by(|left, right| left.package_name.cmp(&right.package_name));
    let generated_sources = materialize_generated_sources(&root, &rustdoc_files)?;
    let input = BuildInput {
        version: args.version.clone(),
        channel: args.channel.clone(),
        revision: revision.clone(),
        source_state: source_state.into(),
        source_sha256: Some(source_sha256.clone()),
        repository_root: root.clone(),
        rustdoc_files,
        package_metadata,
        generated_sources,
        mark_latest: args.channel == Channel::Release,
    };
    let data = build_data(&input).map_err(docs_error)?;
    let availability =
        project_into_docs(&data, &metadata, profile_items, &profiles).map_err(profile_error)?;
    fs::create_dir_all(&output).map_err(io_error)?;
    write_bundle(&data, &output, input.mark_latest).map_err(docs_error)?;
    let profile_path = output.join("sdk-docs-profile-availability.v1.json");
    let profile_bytes = json_bytes(&availability)?;
    write_immutable(&profile_path, &profile_bytes)?;
    let receipts_path = output.join("sdk-docs-rustdoc-profiles.v1.json");
    let mut receipt_rows = Vec::new();
    for (path, digest) in &receipt_manifest {
        let receipt = rustdoc_dir.join(path.strip_prefix("rustdoc/").unwrap_or(path));
        let observation = observe_rustdoc(&receipt).map_err(profile_error)?;
        let package = package_for_crate(&metadata, &observation.crate_name)?;
        let owner =
            api_owner_for_package(&metadata, &package.name.to_string()).map_err(profile_error)?;
        let spec = ProfileSpec {
            package: package.name.to_string(),
            target: observation.target.clone(),
            default_features: true,
            features: BTreeSet::new(),
        };
        receipt_rows.push(ProfileReceipt {
            package: package.name.to_string(),
            published_owner: owner.published_package,
            owner_kind: format!("{:?}", owner.kind),
            target: observation.target,
            default_features: true,
            features: Vec::new(),
            profile: spec.id(),
            rustdoc_file: path.clone(),
            rustdoc_sha256: digest.clone(),
            rustdoc_version: observation.crate_version,
            rustdoc_profiles_covered: true,
            installed_runtime_qualified: false,
        });
    }
    write_immutable(&receipts_path, &json_bytes(&receipt_rows)?)?;
    let scenario_path = output.join("sdk-docs-scenarios.v1.json");
    let mut scenario_artifacts = Vec::new();
    if !args.skip_scenarios {
        let sources = scenarios::validate(&root).map_err(scenario_error)?;
        scenarios::compile_all(&root, &sources, None).map_err(scenario_error)?;
        let executions =
            scenarios::execute_all(&root, &sources, None, None).map_err(scenario_error)?;
        let snippets = scenarios::render_typescript(&executions).map_err(scenario_error)?;
        write_immutable(&scenario_path, &json_bytes(&scenarios::catalog(&sources))?)?;
        let executions_path = output.join("sdk-docs-scenario-executions.v1.json");
        write_immutable(
            &executions_path,
            &json_bytes(&scenarios::execution_catalog(&executions))?,
        )?;
        let projections = scenarios::projection_catalog(&snippets, |snippet| {
            sha256_bytes(snippet.source.as_bytes())
        });
        let projections_path = output.join("sdk-docs-scenario-projections.v1.json");
        write_immutable(&projections_path, &json_bytes(&projections)?)?;
        for snippet in snippets {
            let path = output.join(&snippet.path);
            write_immutable(&path, snippet.source.as_bytes())?;
            scenario_artifacts.push(path);
        }
    } else {
        write_immutable(
            &scenario_path,
            br#"{"schema":"acyclic.sdk.scenarios.v1","scenarios":[],"skipped":true}"#,
        )?;
    }
    let artifacts = artifact_hashes(&output)?;
    let manifest = GenerationManifest {
        schema: "sdk-generation-manifest.v1".into(),
        generator: "sdk-generation/0.1.0".into(),
        rustdoc_tool: "rustdoc-types/0.60.0".into(),
        cargo_metadata: "cargo_metadata/0.23.1".into(),
        version: args.version,
        channel: args.channel,
        revision,
        source_state: source_state.into(),
        source_sha256,
        source_files,
        rustdoc_root: "rustdoc".into(),
        rustdoc_files: receipt_manifest,
        profile_availability: path_string(
            profile_path.strip_prefix(&output).unwrap_or(&profile_path),
        ),
        scenarios: path_string(
            scenario_path
                .strip_prefix(&output)
                .unwrap_or(&scenario_path),
        ),
        artifacts,
    };
    let manifest_path = output.join("generation-manifest.v1.json");
    let manifest_bytes = json_bytes(&manifest)?;
    write_immutable(&manifest_path, &manifest_bytes)?;
    write_immutable(
        &output.join("generation-manifest.v1.sha256"),
        sha256_bytes(&manifest_bytes).as_bytes(),
    )?;
    let _ = scenario_artifacts;
    println!("generated {}", output.display());
    Ok(())
}

fn drift((root, output, rustdoc_dir): (PathBuf, PathBuf, PathBuf)) -> Result<(), CliError> {
    let root = canonical(&root)?;
    let output = canonical(&output)?;
    let rustdoc_dir = canonical(&rustdoc_dir)?;
    let manifest_path = output.join("generation-manifest.v1.json");
    let manifest: GenerationManifest =
        serde_json::from_slice(&fs::read(&manifest_path).map_err(io_error)?)
            .map_err(|e| CliError(format!("invalid generation manifest: {e}")))?;
    let manifest_bytes = fs::read(&manifest_path).map_err(io_error)?;
    let expected_manifest_digest =
        fs::read_to_string(output.join("generation-manifest.v1.sha256")).map_err(io_error)?;
    if expected_manifest_digest.trim() != sha256_bytes(&manifest_bytes) {
        return Err(CliError(
            "generation manifest integrity check failed".into(),
        ));
    }
    if manifest.revision != git_revision(&root)? {
        return Err(CliError("source revision changed since generation".into()));
    }
    let expected_state = if manifest.channel == Channel::Release {
        "captured-snapshot"
    } else {
        "working-tree"
    };
    if manifest.source_state != expected_state {
        return Err(CliError(format!(
            "generation source state `{}` does not match channel `{expected_state}`",
            manifest.source_state
        )));
    }
    if manifest.channel == Channel::Release && git_dirty(&root)? {
        return Err(CliError("release checkout is no longer clean".into()));
    }
    let source_files = source_file_hashes(&root, &output)?;
    let digest = digest_map(&source_files);
    if source_files != manifest.source_files || digest != manifest.source_sha256 {
        let changed = manifest
            .source_files
            .keys()
            .chain(source_files.keys())
            .find(|path| manifest.source_files.get(*path) != source_files.get(*path))
            .cloned()
            .unwrap_or_else(|| "<source manifest>".into());
        return Err(CliError(format!(
            "source digest changed since generation at {changed} (expected {}, got {})",
            manifest.source_sha256, digest
        )));
    }
    for (path, expected) in &manifest.rustdoc_files {
        let relative = path.strip_prefix("rustdoc/").unwrap_or(path);
        let actual = sha256_file(&rustdoc_dir.join(relative))?;
        if &actual != expected {
            return Err(CliError(format!("Rustdoc input changed: {path}")));
        }
    }
    let actual = artifact_hashes(&output)?;
    if actual != manifest.artifacts {
        return Err(CliError(
            "generated artifact digest changed since generation".into(),
        ));
    }
    println!("drift check passed for {}", output.display());
    Ok(())
}

fn load_metadata(root: &Path) -> Result<Metadata, CliError> {
    sdk_docs::rustdoc_profiles::load_metadata_with_cargo(root.join("Cargo.toml"), None)
        .map_err(profile_error)
}

fn rustdoc_files(dir: &Path) -> Result<Vec<PathBuf>, CliError> {
    let mut result = fs::read_dir(dir)
        .map_err(io_error)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension() == Some(OsStr::new("json")))
        .collect::<Vec<_>>();
    result.sort();
    Ok(result)
}

fn package_for_crate<'a>(
    metadata: &'a Metadata,
    crate_name: &str,
) -> Result<&'a Package, CliError> {
    let matches = metadata
        .packages
        .iter()
        .filter(|package| {
            package.targets.iter().any(|target| {
                target.kind.iter().any(|kind| {
                    matches!(
                        kind,
                        TargetKind::Lib
                            | TargetKind::RLib
                            | TargetKind::CDyLib
                            | TargetKind::StaticLib
                            | TargetKind::DyLib
                    )
                }) && target.name.replace('-', "_") == crate_name
            })
        })
        .collect::<Vec<_>>();
    matches.into_iter().next().ok_or_else(|| {
        CliError(format!(
            "Rustdoc crate `{crate_name}` has no matching Cargo package target"
        ))
    })
}

fn published_packages(root: &Path) -> Result<Vec<String>, CliError> {
    let value: serde_json::Value = serde_json::from_slice(
        &fs::read(root.join("release/cargo-crates.json")).map_err(io_error)?,
    )
    .map_err(|e| CliError(format!("invalid release/cargo-crates.json: {e}")))?;
    value
        .as_array()
        .ok_or_else(|| CliError("release/cargo-crates.json must be an array".into()))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| CliError("release owner names must be strings".into()))
        })
        .collect()
}

fn canonical(path: &Path) -> Result<PathBuf, CliError> {
    path.canonicalize().map_err(io_error)
}
fn absolute(path: &Path) -> Result<PathBuf, CliError> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(env::current_dir().map_err(io_error)?.join(path))
    }
}
fn path_string(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}
fn profile_error(error: impl std::fmt::Display) -> CliError {
    CliError(format!("profile error: {error}"))
}
fn docs_error(error: impl std::fmt::Display) -> CliError {
    CliError(format!("docs error: {error}"))
}
fn scenario_error(error: impl std::fmt::Display) -> CliError {
    CliError(format!("scenario error: {error}"))
}
fn io_error(error: std::io::Error) -> CliError {
    CliError(format!("I/O error: {error}"))
}
fn json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, CliError> {
    serde_json::to_vec_pretty(value).map_err(|e| CliError(format!("JSON error: {e}")))
}
fn sha256_bytes(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}
fn sha256_file(path: &Path) -> Result<String, CliError> {
    Ok(sha256_bytes(&fs::read(path).map_err(io_error)?))
}

fn receipt_source_attestation(path: &Path) -> Result<Option<String>, CliError> {
    let sidecar = path.with_extension("source.sha256");
    match fs::read_to_string(&sidecar) {
        Ok(value) => Ok(Some(value.trim().to_owned())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_error(error)),
    }
}

fn rustdoc_key(root: &Path, path: &Path) -> Result<String, CliError> {
    let relative = path.strip_prefix(root).map_err(|_| {
        CliError(format!(
            "Rustdoc receipt {} is outside its input directory",
            path.display()
        ))
    })?;
    Ok(format!("rustdoc/{}", path_string(relative)))
}

/// Existing profile receipts may refer to generated files in the Cargo target
/// directory used by the producing invocation. Rebind those spans to the
/// current checkout only after locating a same-named generated source and
/// copying its exact bytes to the receipt's recorded path. Missing or
/// ambiguous files fail closed; the docs projection never silently drops a
/// source span.
fn materialize_generated_sources(
    root: &Path,
    rustdoc_files: &[PathBuf],
) -> Result<Vec<GeneratedSource>, CliError> {
    let mut filenames = BTreeSet::new();
    for receipt in rustdoc_files {
        let value: serde_json::Value =
            serde_json::from_slice(&fs::read(receipt).map_err(io_error)?).map_err(|e| {
                CliError(format!("invalid Rustdoc JSON {}: {e}", receipt.display()))
            })?;
        collect_span_filenames(&value, &mut filenames);
    }
    let mut candidates = BTreeMap::<String, Vec<PathBuf>>::new();
    collect_named_files(&root.join("target"), &mut candidates)?;
    collect_named_files(&root.join("rust"), &mut candidates)?;
    let mut result = Vec::new();
    for filename in filenames {
        let physical = PathBuf::from(&filename);
        if !physical.is_absolute() {
            continue;
        }
        let physical = if physical.is_file() {
            physical
        } else {
            let basename = physical
                .file_name()
                .and_then(OsStr::to_str)
                .ok_or_else(|| CliError(format!("Rustdoc span has invalid filename {filename}")))?;
            let matches = candidates.get(basename).cloned().unwrap_or_default();
            let Some(candidate) = matches.into_iter().next() else {
                return Err(CliError(format!(
                    "Rustdoc generated source is unavailable: {filename}"
                )));
            };
            if let Some(parent) = physical.parent() {
                fs::create_dir_all(parent).map_err(io_error)?;
            }
            fs::copy(&candidate, &physical).map_err(io_error)?;
            physical
        };
        let is_generated = !physical.starts_with(root)
            || physical
                .strip_prefix(root)
                .ok()
                .and_then(|relative| relative.components().next())
                .is_some_and(|component| component.as_os_str() == OsStr::new("target"));
        if !is_generated {
            continue;
        }
        let digest = sha256_file(&physical)?;
        let suffix = physical
            .extension()
            .and_then(OsStr::to_str)
            .map(|ext| format!(".{ext}"))
            .unwrap_or_default();
        let logical_path = PathBuf::from(format!(
            "generated/rustdoc/{}{}",
            sha256_bytes(path_string(&physical).as_bytes()).trim_start_matches("sha256:"),
            suffix
        ));
        result.push(GeneratedSource {
            physical_path: physical,
            logical_path,
            sha256: digest,
        });
    }
    Ok(result)
}

fn collect_span_filenames(value: &serde_json::Value, output: &mut BTreeSet<String>) {
    match value {
        serde_json::Value::Object(map) => {
            if let Some(serde_json::Value::String(filename)) = map.get("filename") {
                output.insert(filename.clone());
            }
            for child in map.values() {
                collect_span_filenames(child, output);
            }
        }
        serde_json::Value::Array(values) => {
            for child in values {
                collect_span_filenames(child, output);
            }
        }
        _ => {}
    }
}

fn collect_named_files(
    root: &Path,
    output: &mut BTreeMap<String, Vec<PathBuf>>,
) -> Result<(), CliError> {
    if !root.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(root).map_err(io_error)? {
        let path = entry.map_err(io_error)?.path();
        if path.is_dir() {
            collect_named_files(&path, output)?;
        } else if path.is_file() {
            if let Some(name) = path.file_name().and_then(OsStr::to_str) {
                output.entry(name.to_owned()).or_default().push(path);
            }
        }
    }
    Ok(())
}

fn write_immutable(path: &Path, bytes: &[u8]) -> Result<(), CliError> {
    if let Ok(existing) = fs::read(path) {
        if existing == bytes {
            return Ok(());
        }
        return Err(CliError(format!(
            "refusing to rewrite immutable artifact {}",
            path.display()
        )));
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(io_error)?;
    }
    let temp = path.with_extension(format!("tmp-{}", std::process::id()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp)
        .map_err(io_error)?;
    file.write_all(bytes).map_err(io_error)?;
    file.sync_all().map_err(io_error)?;
    drop(file);
    match fs::hard_link(&temp, path) {
        Ok(()) => {
            let _ = fs::remove_file(&temp);
            Ok(())
        }
        Err(error) => {
            let _ = fs::remove_file(&temp);
            if let Ok(existing) = fs::read(path) {
                if existing == bytes {
                    return Ok(());
                }
            }
            Err(io_error(error))
        }
    }
}

fn artifact_hashes(output: &Path) -> Result<BTreeMap<String, String>, CliError> {
    let mut files = Vec::new();
    collect_files(output, output, &mut files, true)?;
    files.sort();
    let mut result = BTreeMap::new();
    for path in files {
        if path.file_name() == Some(OsStr::new("generation-manifest.v1.json"))
            || path.file_name() == Some(OsStr::new("generation-manifest.v1.sha256"))
        {
            continue;
        }
        let relative = path.strip_prefix(output).unwrap_or(&path);
        result.insert(path_string(relative), sha256_file(&path)?);
    }
    Ok(result)
}

fn collect_files(
    root: &Path,
    current: &Path,
    files: &mut Vec<PathBuf>,
    include_all: bool,
) -> Result<(), CliError> {
    for entry in fs::read_dir(current).map_err(io_error)? {
        let path = entry.map_err(io_error)?.path();
        if path.is_dir() {
            collect_files(root, &path, files, include_all)?;
        } else if include_all {
            files.push(path);
        }
    }
    let _ = root;
    Ok(())
}

fn source_file_hashes(root: &Path, output: &Path) -> Result<BTreeMap<String, String>, CliError> {
    let mut files = Vec::new();
    collect_source_files(root, root, output, &mut files)?;
    files.sort();
    let mut result = BTreeMap::new();
    for path in files {
        let relative = path.strip_prefix(root).unwrap_or(&path);
        result.insert(path_string(relative), sha256_file(&path)?);
    }
    Ok(result)
}

fn digest_map(files: &BTreeMap<String, String>) -> String {
    let mut hasher = Sha256::new();
    for (path, digest) in files {
        hasher.update(path.as_bytes());
        hasher.update([0]);
        hasher.update(digest.as_bytes());
        hasher.update([0]);
    }
    format!("sha256:{:x}", hasher.finalize())
}

fn collect_source_files(
    root: &Path,
    current: &Path,
    output: &Path,
    files: &mut Vec<PathBuf>,
) -> Result<(), CliError> {
    for entry in fs::read_dir(current).map_err(io_error)? {
        let path = entry.map_err(io_error)?.path();
        if path == output || path.starts_with(output) {
            continue;
        }
        let name = path.file_name().and_then(OsStr::to_str).unwrap_or_default();
        if name == ".git" || name == "target" || name == "node_modules" {
            continue;
        }
        if path.is_dir() {
            collect_source_files(root, &path, output, files)?;
        } else if path.is_file() {
            let relative = path.strip_prefix(root).unwrap_or(&path);
            let components = relative.components().collect::<Vec<_>>();
            let under_crates = components.len() >= 3
                && components[0].as_os_str() == OsStr::new("rust")
                && components[1].as_os_str() == OsStr::new("crates");
            let include = (under_crates
                && relative
                    .extension()
                    .and_then(OsStr::to_str)
                    .is_some_and(|ext| matches!(ext, "rs" | "toml" | "lock" | "md")))
                || relative == Path::new("Cargo.toml")
                || relative == Path::new("Cargo.lock")
                || relative == Path::new("release/cargo-crates.json");
            if include {
                files.push(path);
            }
        }
    }
    let _ = root;
    Ok(())
}

fn git_revision(root: &Path) -> Result<String, CliError> {
    let output = Command::new("git")
        .current_dir(root)
        .args(["rev-parse", "HEAD"])
        .output()
        .map_err(io_error)?;
    if !output.status.success() {
        return Err(CliError("git rev-parse HEAD failed".into()));
    }
    let revision = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if revision.len() < 40 {
        return Err(CliError(format!("invalid git revision `{revision}`")));
    }
    Ok(revision)
}

fn git_dirty(root: &Path) -> Result<bool, CliError> {
    let output = Command::new("git")
        .current_dir(root)
        .args(["status", "--porcelain"])
        .output()
        .map_err(io_error)?;
    Ok(!output.status.success() || !output.stdout.is_empty())
}
