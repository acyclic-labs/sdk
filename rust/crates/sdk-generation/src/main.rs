use cargo_metadata::{Metadata, Package, TargetKind};
use sdk_docs::rustdoc_profiles::{
    api_owner_for_package, execute_profile_with_cargo, extract_owned_api_for_crate,
    observe_rustdoc, project_into_docs, validate_rustdoc_version, ApiOwnerKind, ProfileId,
    ProfileSpec,
};
use sdk_docs::{
    build_data, merge_profile_catalog, rustdoc_digest, scenarios, write_bundle, write_bundle_files,
    BuildInput, Channel, GeneratedSource, PackageMetadata,
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

mod release_manifest;

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
    rustdoc_dir: Option<PathBuf>,
    version: String,
    channel: Channel,
    skip_scenarios: bool,
    execute_profiles: bool,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
struct GenerationManifest {
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
    receipt: PathBuf,
    spec: ProfileSpec,
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
        "sdk-generation generate --root ROOT --output DIR --version VERSION [--rustdoc-dir DIR | --execute-profiles] [--channel preview|release] [--skip-scenarios]\nsdk-generation drift --root ROOT --output DIR --rustdoc-dir DIR"
    );
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
    Ok(GenerateArgs {
        root: required("root")?.into(),
        output: required("output")?.into(),
        rustdoc_dir,
        version: required("version")?,
        channel,
        skip_scenarios: flags.contains_key("skip-scenarios"),
        execute_profiles,
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

fn generate(args: GenerateArgs) -> Result<(), CliError> {
    let root = canonical(&args.root)?;
    let output = absolute(&args.output)?;
    let rustdoc_dir = match (&args.rustdoc_dir, args.execute_profiles) {
        (Some(path), false) => canonical(path)?,
        (None, true) => output
            .join(".rustdoc")
            .canonicalize()
            .unwrap_or_else(|_| output.join(".rustdoc")),
        _ => return Err(CliError("invalid Rustdoc input mode".into())),
    };
    let cargo_path = args
        .execute_profiles
        .then(|| pinned_cargo(&root))
        .transpose()?;
    let metadata = load_metadata(&root, cargo_path.as_deref())?;
    if args.channel == Channel::Release {
        let published = published_packages(&root)?;
        let versions = metadata
            .packages
            .iter()
            .filter(|package| published.iter().any(|name| name == package.name.as_ref()))
            .map(|package| package.version.to_string())
            .collect::<BTreeSet<_>>();
        validate_release_package_versions(&args.version, &versions)?;
    }
    let toolchain = cargo_path
        .as_deref()
        .map(toolchain_identity)
        .transpose()?
        .unwrap_or_default();
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
    let executed_profiles = if args.execute_profiles {
        execute_default_profiles(
            &root,
            &output,
            &rustdoc_dir,
            &metadata,
            &source_sha256,
            cargo_path
                .as_deref()
                .ok_or_else(|| CliError("profile execution has no pinned Cargo".into()))?,
        )?
    } else {
        Vec::new()
    };

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
    let mut receipt_specs = BTreeMap::<String, ProfileSpec>::new();
    for receipt in &receipts {
        let observation = observe_rustdoc(receipt).map_err(profile_error)?;
        let source_attestation = receipt_source_attestation(receipt)?;
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
        let profile_spec = executed_profiles
            .iter()
            .find(|executed| executed.receipt == *receipt)
            .map(|executed| executed.spec.clone())
            .unwrap_or(ProfileSpec {
                package: package.name.to_string(),
                target: observation.target.clone(),
                // A hand-supplied receipt has no producer profile metadata.
                // Keep its item coverage useful while refusing to claim that
                // Cargo default features were actually selected.
                default_features: false,
                features: BTreeSet::new(),
            });
        let profile_id = profile_spec.id();
        let crate_name = observation.crate_name.clone();
        let items = extract_owned_api_for_crate(receipt, &owner, profile_id.clone(), &crate_name)
            .map_err(profile_error)?;
        profile_items.extend(items);
        profiles.insert(profile_id.clone(), profile_spec.clone());
        if executed_profiles
            .iter()
            .any(|executed| executed.receipt == *receipt)
        {
            receipt_specs.insert(path_string(receipt), profile_spec.clone());
        }
        let receipt_key = rustdoc_key(&rustdoc_dir, receipt)?;
        receipt_manifest.insert(
            receipt_key,
            rustdoc_digest(receipt, &root).map_err(|error| CliError(error.to_string()))?,
        );
        rustdoc_files.push(receipt.clone());
        package_metadata.push(PackageMetadata {
            rustdoc_file: receipt.clone(),
            package_name: package.name.to_string(),
            crate_name,
            version: package.version.to_string(),
        });
    }

    // Additional feature and target receipts are profile evidence, not
    // additional docs families. Merge their exact signatures into the
    // existing catalog below.
    for executed in executed_profiles
        .iter()
        .filter(|executed| !receipts.iter().any(|receipt| receipt == &executed.receipt))
    {
        let receipt = &executed.receipt;
        let observation = observe_rustdoc(&receipt).map_err(profile_error)?;
        if receipt_source_attestation(&receipt)?.as_deref() != Some(source_sha256.as_str()) {
            return Err(CliError(format!(
                "WASM Rustdoc receipt {} has no matching source attestation",
                receipt.display()
            )));
        }
        let package = package_for_crate(&metadata, &observation.crate_name)?;
        validate_rustdoc_version(&metadata, &package.name.to_string(), &observation)
            .map_err(profile_error)?;
        let owner =
            api_owner_for_package(&metadata, &package.name.to_string()).map_err(profile_error)?;
        let profile = executed.spec.clone();
        let profile_id = profile.id();
        profile_items.extend(
            extract_owned_api_for_crate(
                &receipt,
                &owner,
                profile_id.clone(),
                &observation.crate_name,
            )
            .map_err(profile_error)?,
        );
        profiles.insert(profile_id, profile);
        receipt_specs.insert(path_string(receipt), executed.spec.clone());
        let receipt_key = rustdoc_key(&rustdoc_dir, &receipt)?;
        receipt_manifest.insert(
            receipt_key,
            rustdoc_digest(&receipt, &root).map_err(|error| CliError(error.to_string()))?,
        );
    }

    let published = published_packages(&root)?;
    let found = package_metadata
        .iter()
        .map(|metadata| metadata.package_name.clone())
        .collect::<BTreeSet<_>>();
    for owner in &published {
        if !found.contains(owner.as_str()) {
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
    let mut data = build_data(&input).map_err(docs_error)?;
    let mut generated_source_artifacts = input.generated_sources.clone();
    if args.channel == Channel::Release {
        validate_published_coverage(&published, &data)?;
    }
    // Feature and target receipts are additional views of the same crate
    // family. Build each through the maintained sdk-docs projection and union
    // its public items before availability is attached, so binding-only and
    // feature-gated APIs remain present in the actual docs catalog.
    for executed in executed_profiles
        .iter()
        .filter(|executed| !receipts.iter().any(|receipt| receipt == &executed.receipt))
    {
        let receipt = &executed.receipt;
        let observation = observe_rustdoc(receipt).map_err(profile_error)?;
        let package = package_for_crate(&metadata, &observation.crate_name)?;
        let variant_input = BuildInput {
            version: args.version.clone(),
            channel: args.channel.clone(),
            revision: revision.clone(),
            source_state: source_state.into(),
            source_sha256: Some(source_sha256.clone()),
            repository_root: root.clone(),
            rustdoc_files: vec![receipt.clone()],
            package_metadata: vec![PackageMetadata {
                rustdoc_file: receipt.clone(),
                package_name: package.name.to_string(),
                crate_name: observation.crate_name,
                version: package.version.to_string(),
            }],
            generated_sources: materialize_generated_sources(&root, std::slice::from_ref(receipt))?,
            mark_latest: false,
        };
        let variant_data = build_data(&variant_input).map_err(docs_error)?;
        generated_source_artifacts.extend(variant_input.generated_sources.clone());
        merge_profile_catalog(&mut data, variant_data).map_err(docs_error)?;
    }
    let availability =
        project_into_docs(&data, &metadata, profile_items, &profiles).map_err(profile_error)?;
    fs::create_dir_all(&output).map_err(io_error)?;
    write_bundle_files(&data, &output, input.mark_latest).map_err(docs_error)?;
    for source in generated_source_artifacts {
        let bytes = fs::read(&source.physical_path).map_err(io_error)?;
        if sha256_bytes(&bytes).trim_start_matches("sha256:")
            != source.sha256.trim_start_matches("sha256:")
        {
            return Err(CliError(format!(
                "generated source changed before bundling: {}",
                source.physical_path.display()
            )));
        }
        write_immutable(&output.join(&source.logical_path), &bytes)?;
    }
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
        let spec = receipt_specs
            .get(&path_string(&receipt))
            .cloned()
            .unwrap_or(ProfileSpec {
                package: package.name.to_string(),
                target: observation.target.clone(),
                default_features: true,
                features: BTreeSet::new(),
            });
        receipt_rows.push(ProfileReceipt {
            package: package.name.to_string(),
            published_owner: owner.published_package,
            owner_kind: format!("{:?}", owner.kind),
            target: observation.target,
            default_features: spec.default_features,
            features: spec.features.iter().cloned().collect(),
            profile: spec.id(),
            rustdoc_file: path.clone(),
            rustdoc_sha256: digest.clone(),
            rustdoc_version: observation.crate_version,
            rustdoc_profiles_covered: receipt_specs.contains_key(&path_string(&receipt)),
            installed_runtime_qualified: false,
        });
    }
    write_immutable(&receipts_path, &json_bytes(&receipt_rows)?)?;
    let scenario_path = output.join("sdk-docs-scenarios.v1.json");
    let mut scenario_artifacts = Vec::new();
    let scenario_sources = if !args.skip_scenarios {
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
        Some(sources)
    } else {
        write_immutable(
            &scenario_path,
            br#"{"schema":"acyclic.sdk.scenarios.v1","scenarios":[],"skipped":true}"#,
        )?;
        None
    };
    let pre_manifest_artifacts = artifact_hashes(&output)?;
    let release_manifest = release_manifest::build(
        &root,
        &output,
        &args.version,
        args.channel.clone(),
        &revision,
        source_state,
        &source_sha256,
        &source_files,
        &metadata,
        scenario_sources.as_deref(),
        &pre_manifest_artifacts,
        &scenario_artifacts,
    )
    .map_err(|error| CliError(format!("release manifest error: {error}")))?;
    let release_manifest_path =
        release_manifest::output_path(&output, &args.channel, &args.version)
            .map_err(|error| CliError(format!("release manifest path error: {error}")))?;
    let release_manifest_bytes = json_bytes(&release_manifest)?;
    write_immutable(&release_manifest_path, &release_manifest_bytes)?;
    write_immutable(
        &release_manifest_path.with_extension("sha256"),
        sha256_bytes(&release_manifest_bytes).as_bytes(),
    )?;
    let artifacts = artifact_hashes(&output)?;
    let manifest = GenerationManifest {
        schema: "sdk-generation-manifest.v1".into(),
        generator: "sdk-generation/0.1.0".into(),
        rustdoc_tool: "rustdoc-types/0.60.0".into(),
        cargo_metadata: "cargo_metadata/0.23.1".into(),
        toolchain,
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
        release_manifest: path_string(
            release_manifest_path
                .strip_prefix(&output)
                .unwrap_or(&release_manifest_path),
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
    // Re-read the source closure, revision, receipts and all immutable artifacts
    // after execution. Failed qualification leaves files unadmitted.
    drift((root.clone(), output.clone(), rustdoc_dir.clone()))?;
    write_bundle(&data, &output, input.mark_latest).map_err(docs_error)?;
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
    if !manifest.toolchain.is_empty() {
        let current_toolchain = toolchain_identity(&pinned_cargo(&root)?)?;
        if manifest.toolchain != current_toolchain {
            return Err(CliError(format!(
                "pinned Cargo toolchain changed since generation (expected {}, got {})",
                manifest.toolchain, current_toolchain
            )));
        }
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
        let actual = rustdoc_digest(&rustdoc_dir.join(relative), &root)
            .map_err(|error| CliError(error.to_string()))?;
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
    if !manifest.release_manifest.is_empty() {
        release_manifest::validate(
            &root,
            &output,
            &output.join(&manifest.release_manifest),
            &manifest.version,
            &manifest.channel,
            &manifest.revision,
            &manifest.source_state,
            &manifest.source_sha256,
            &manifest.source_files,
            &actual,
        )
        .map_err(|error| CliError(format!("release manifest drift: {error}")))?;
    }
    println!("drift check passed for {}", output.display());
    Ok(())
}

fn load_metadata(root: &Path, cargo_path: Option<&Path>) -> Result<Metadata, CliError> {
    sdk_docs::rustdoc_profiles::load_metadata_with_cargo(root.join("Cargo.toml"), cargo_path)
        .map_err(profile_error)
}

fn execute_default_profiles(
    root: &Path,
    output: &Path,
    rustdoc_dir: &Path,
    metadata: &Metadata,
    source_sha256: &str,
    cargo_path: &Path,
) -> Result<Vec<ExecutedProfile>, CliError> {
    let host = rustc_host()?;
    let targets = BTreeSet::from([host.clone()]);
    fs::create_dir_all(rustdoc_dir).map_err(io_error)?;
    let target_dir = output.join(".profile-build");
    let wasm_target = "wasm32-unknown-unknown";
    let installed_targets = rustup_targets()?;
    let published = published_packages(root)?
        .into_iter()
        .collect::<BTreeSet<_>>();
    let mut executed = Vec::new();
    for package in &metadata.packages {
        let owned_binding = api_owner_for_package(metadata, package.name.as_str())
            .ok()
            .is_some_and(|owner| owner.rustdoc_package == package.name.as_ref());
        if !published.contains(package.name.as_ref()) && !owned_binding {
            continue;
        }
        let Some(target) = package.targets.iter().find(|target| {
            target.kind.iter().any(|kind| {
                matches!(
                    kind,
                    TargetKind::Lib
                        | TargetKind::RLib
                        | TargetKind::CDyLib
                        | TargetKind::StaticLib
                        | TargetKind::DyLib
                )
            })
        }) else {
            continue;
        };
        let owner = api_owner_for_package(metadata, package.name.as_str()).ok();
        let wasm_binding = owner
            .as_ref()
            .is_some_and(|owner| owner.kind == ApiOwnerKind::WasmBinding);
        if !wasm_binding {
            let profiles = sdk_docs::rustdoc_profiles::profiles_for_package(
                metadata,
                package.name.as_str(),
                &host,
                &targets,
            )
            .map_err(profile_error)?;
            for profile in profiles {
                let receipt = if profile.default_features && profile.features.is_empty() {
                    rustdoc_dir.join(format!("{}.json", target.name.replace('-', "_")))
                } else {
                    rustdoc_dir
                        .join("profiles")
                        .join(format!("{}.json", profile.id().0.replace(';', "_")))
                };
                if let Some(parent) = receipt.parent() {
                    fs::create_dir_all(parent).map_err(io_error)?;
                }
                execute_profile_with_cargo(
                    root.join("Cargo.toml"),
                    metadata,
                    &profile,
                    &targets,
                    &target_dir,
                    &receipt,
                    Some(cargo_path),
                )
                .map_err(profile_error)?;
                retain_profile_generated_sources(root, &receipt)?;
                fs::write(receipt.with_extension("source.sha256"), source_sha256)
                    .map_err(io_error)?;
                executed.push(ExecutedProfile {
                    receipt,
                    spec: profile,
                });
            }
        }
        if installed_targets.contains(wasm_target) {
            if wasm_binding {
                let wasm_targets = BTreeSet::from([wasm_target.to_owned()]);
                let wasm_profiles = sdk_docs::rustdoc_profiles::profiles_for_package(
                    metadata,
                    package.name.as_str(),
                    wasm_target,
                    &wasm_targets,
                )
                .map_err(profile_error)?;
                for wasm_profile in wasm_profiles {
                    let wasm_receipt =
                        if wasm_profile.default_features && wasm_profile.features.is_empty() {
                            rustdoc_dir.join(format!("{}.json", target.name.replace('-', "_")))
                        } else {
                            rustdoc_dir.join("profiles").join(format!(
                                "wasm-{}.json",
                                wasm_profile.id().0.replace(';', "_")
                            ))
                        };
                    if let Some(parent) = wasm_receipt.parent() {
                        fs::create_dir_all(parent).map_err(io_error)?;
                    }
                    execute_profile_with_cargo(
                        root.join("Cargo.toml"),
                        metadata,
                        &wasm_profile,
                        &wasm_targets,
                        &target_dir,
                        &wasm_receipt,
                        Some(cargo_path),
                    )
                    .map_err(profile_error)?;
                    retain_profile_generated_sources(root, &wasm_receipt)?;
                    fs::write(wasm_receipt.with_extension("source.sha256"), source_sha256)
                        .map_err(io_error)?;
                    executed.push(ExecutedProfile {
                        receipt: wasm_receipt,
                        spec: wasm_profile,
                    });
                }
            }
        }
    }
    Ok(executed)
}

fn retain_profile_generated_sources(root: &Path, receipt: &Path) -> Result<(), CliError> {
    let value: serde_json::Value = serde_json::from_slice(&fs::read(receipt).map_err(io_error)?)
        .map_err(|error| {
            CliError(format!(
                "invalid Rustdoc JSON {}: {error}",
                receipt.display()
            ))
        })?;
    let mut filenames = BTreeSet::new();
    collect_span_filenames(&value, &mut filenames);
    let retained_root = root.join("target/sdk-generation-generated-sources/retained");
    let index_path = retained_root.join("aliases.json");
    let mut aliases = if index_path.is_file() {
        serde_json::from_slice::<BTreeMap<String, String>>(
            &fs::read(&index_path).map_err(io_error)?,
        )
        .map_err(|error| CliError(format!("invalid retained Rustdoc source index: {error}")))?
    } else {
        BTreeMap::new()
    };
    for filename in filenames {
        let source = PathBuf::from(&filename);
        if !source.is_absolute() || source.starts_with(root) || !source.is_file() {
            continue;
        }
        let basename = source.file_name().and_then(OsStr::to_str).ok_or_else(|| {
            CliError(format!(
                "Rustdoc generated source has invalid filename {filename}"
            ))
        })?;
        let bytes = fs::read(&source).map_err(io_error)?;
        let digest = sha256_bytes(&bytes);
        let retained = retained_root.join(format!(
            "{}-{basename}",
            digest.trim_start_matches("sha256:")
        ));
        if let Some(parent) = retained.parent() {
            fs::create_dir_all(parent).map_err(io_error)?;
        }
        if !retained.is_file() {
            fs::write(&retained, bytes).map_err(io_error)?;
        }
        let relative = retained
            .strip_prefix(root)
            .map(path_string)
            .map_err(|_| CliError("retained generated source escaped repository root".into()))?;
        aliases.insert(filename, relative);
    }
    fs::create_dir_all(&retained_root).map_err(io_error)?;
    fs::write(
        &index_path,
        serde_json::to_vec(&aliases).map_err(|error| {
            CliError(format!(
                "cannot serialize retained Rustdoc source index: {error}"
            ))
        })?,
    )
    .map_err(io_error)?;
    Ok(())
}

fn rustc_host() -> Result<String, CliError> {
    let output = Command::new("rustc")
        .args(["-vV"])
        .output()
        .map_err(io_error)?;
    if !output.status.success() {
        return Err(CliError("rustc -vV failed".into()));
    }
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .map(str::to_owned)
        .ok_or_else(|| CliError("rustc -vV did not report a host target".into()))
}

fn pinned_cargo(root: &Path) -> Result<PathBuf, CliError> {
    let toolchain = fs::read_to_string(root.join("rust-toolchain.toml"))
        .map_err(io_error)?
        .lines()
        .find_map(|line| {
            let (key, value) = line.split_once('=')?;
            (key.trim() == "channel").then(|| value.trim().trim_matches('"').to_owned())
        })
        .ok_or_else(|| CliError("rust-toolchain.toml has no pinned channel".into()))?;
    let output = Command::new("rustup")
        .args(["which", "cargo", "--toolchain", &toolchain])
        .output()
        .map_err(io_error)?;
    if !output.status.success() {
        return Err(CliError(format!(
            "pinned Cargo toolchain `{toolchain}` is unavailable: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let path = PathBuf::from(String::from_utf8_lossy(&output.stdout).trim());
    if !path.is_file() {
        return Err(CliError(format!(
            "rustup selected a missing Cargo executable {}",
            path.display()
        )));
    }
    Ok(path)
}

fn toolchain_identity(cargo: &Path) -> Result<String, CliError> {
    let output = Command::new(cargo)
        .args(["-V", "--verbose"])
        .output()
        .map_err(io_error)?;
    if !output.status.success() {
        return Err(CliError("pinned Cargo identity query failed".into()));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn rustup_targets() -> Result<BTreeSet<String>, CliError> {
    let output = Command::new("rustup")
        .args(["target", "list", "--installed"])
        .output()
        .map_err(io_error)?;
    if !output.status.success() {
        return Err(CliError("rustup target list --installed failed".into()));
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect())
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

fn validate_release_package_versions(
    requested: &str,
    package_versions: &BTreeSet<String>,
) -> Result<(), CliError> {
    if package_versions.len() != 1 {
        return Err(CliError(format!(
            "release package versions must agree before docs can be released: {package_versions:?}"
        )));
    }
    let expected = package_versions.first().expect("length checked above");
    if requested != expected {
        return Err(CliError(format!(
            "release docs version `{requested}` does not match SDK package version `{expected}`; use preview for candidate labels"
        )));
    }
    Ok(())
}

fn validate_published_coverage(
    published: &[String],
    data: &sdk_docs::DocsData,
) -> Result<(), CliError> {
    for package in published {
        let Some(entry) = data
            .packages
            .entries
            .iter()
            .find(|entry| entry.package_name == *package)
        else {
            return Err(CliError(format!(
                "published owner `{package}` has no generated docs family"
            )));
        };
        let family = data
            .families
            .iter()
            .find(|family| family.slug == entry.family_slug)
            .ok_or_else(|| {
                CliError(format!(
                    "published owner `{package}` has no bound generated family"
                ))
            })?;
        if family.items.is_empty() && family.guides.is_empty() {
            return Err(CliError(format!(
                "published owner `{package}` has empty public docs coverage"
            )));
        }
    }
    Ok(())
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
    let staging_root = root.join("target/sdk-generation-generated-sources");
    for matches in candidates.values_mut() {
        matches.retain(|candidate| !candidate.starts_with(&staging_root));
    }
    let retained_aliases =
        root.join("target/sdk-generation-generated-sources/retained/aliases.json");
    let retained_aliases = if retained_aliases.is_file() {
        serde_json::from_slice::<BTreeMap<String, String>>(
            &fs::read(&retained_aliases).map_err(io_error)?,
        )
        .map_err(|error| CliError(format!("invalid retained Rustdoc source index: {error}")))?
    } else {
        BTreeMap::new()
    };
    let mut result = BTreeMap::<PathBuf, GeneratedSource>::new();
    for filename in filenames {
        let physical = PathBuf::from(&filename);
        if !physical.is_absolute() {
            continue;
        }
        if !physical.is_file() {
            validate_restore_path(root, rustdoc_files, &physical)?;
            if !physical.starts_with(root) && !retained_aliases.contains_key(&filename) {
                return Err(CliError(format!(
                    "external Rustdoc restoration requires a captured source alias: {filename}"
                )));
            }
            let basename = physical
                .file_name()
                .and_then(OsStr::to_str)
                .ok_or_else(|| CliError(format!("Rustdoc span has invalid filename {filename}")))?;
            let matches = retained_aliases
                .get(&filename)
                .map(|relative| vec![root.join(relative)])
                .unwrap_or_else(|| candidates.get(basename).cloned().unwrap_or_default());
            let [candidate] = matches.as_slice() else {
                if matches.is_empty() {
                    return Err(CliError(format!(
                        "Rustdoc generated source is unavailable: {filename}"
                    )));
                }
                return Err(CliError(format!(
                    "Rustdoc generated source basename is ambiguous: {filename}"
                )));
            };
            if retained_aliases.contains_key(&filename) {
                validate_restore_path(root, &[], candidate)?;
                let captured = candidate.canonicalize().map_err(io_error)?;
                let retained = root.join("target/sdk-generation-generated-sources/retained");
                if !captured.starts_with(&retained) {
                    return Err(CliError(
                        "captured Rustdoc alias escaped retained sources".into(),
                    ));
                }
                let expected =
                    format!("{}-", sha256_file(&captured)?.trim_start_matches("sha256:"));
                if !captured
                    .file_name()
                    .and_then(OsStr::to_str)
                    .is_some_and(|name| name.starts_with(&expected))
                {
                    return Err(CliError("captured Rustdoc source digest changed".into()));
                }
            }
            if let Some(parent) = physical.parent() {
                fs::create_dir_all(parent).map_err(io_error)?;
            }
            fs::copy(candidate, &physical).map_err(io_error)?;
        }
        let physical = physical.canonicalize().map_err(io_error)?;
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
        // The logical identity must survive relocating the checkout.  The
        // physical path is intentionally excluded: rustdoc may point into a
        // target directory or a staged foreign source whose absolute path
        // differs on every producer host.  The stable source identity keeps
        // distinct basenames separate, while the content digest keeps
        // same-named sources with different bytes separate.
        let basename = physical
            .file_name()
            .and_then(OsStr::to_str)
            .ok_or_else(|| CliError("generated source has no filename".into()))?;
        let logical_identity = format!("{basename}\0{digest}");
        let logical_path = PathBuf::from(format!(
            "generated/rustdoc/{}{}",
            sha256_bytes(logical_identity.as_bytes()).trim_start_matches("sha256:"),
            suffix
        ));
        let source = GeneratedSource {
            physical_path: physical,
            logical_path,
            sha256: digest,
        };
        // Multiple Rustdoc profiles can spell the same canonical source with
        // equivalent paths (for example, a `./` segment). The publication
        // catalog uses one content identity, but ingestion keeps every
        // distinct physical alias so all profile source spans can resolve.
        result.entry(source.physical_path.clone()).or_insert(source);
    }
    Ok(result.into_values().collect())
}

fn validate_restore_path(root: &Path, receipts: &[PathBuf], path: &Path) -> Result<(), CliError> {
    if path
        .components()
        .any(|part| matches!(part, std::path::Component::ParentDir))
    {
        return Err(CliError(
            "Rustdoc restoration path contains parent traversal".into(),
        ));
    }
    let mut owned = vec![root.join("target")];
    for receipt in receipts {
        if let Some(rustdoc) = receipt
            .ancestors()
            .find(|ancestor| ancestor.file_name() == Some(OsStr::new(".rustdoc")))
        {
            if let Some(output) = rustdoc.parent() {
                owned.push(output.join(".profile-build"));
            }
        }
    }
    let comparable = |path: &Path| -> PathBuf {
        #[cfg(windows)]
        {
            let text = path
                .to_string_lossy()
                .replace('/', "\\")
                .to_ascii_lowercase();
            let text = if let Some(unc) = text.strip_prefix("\\\\?\\unc\\") {
                format!("\\\\{unc}")
            } else {
                text.strip_prefix("\\\\?\\").unwrap_or(&text).to_string()
            };
            PathBuf::from(text)
        }
        #[cfg(not(windows))]
        path.to_path_buf()
    };
    if !owned
        .iter()
        .any(|directory| comparable(path).starts_with(comparable(directory)))
    {
        return Err(CliError(format!(
            "Rustdoc restoration is outside owned build directories: {}",
            path.display()
        )));
    }
    for ancestor in path.ancestors() {
        match fs::symlink_metadata(ancestor) {
            Ok(metadata) => {
                #[cfg(windows)]
                let reparse = {
                    use std::os::windows::fs::MetadataExt;
                    metadata.file_attributes() & 0x400 != 0
                };
                #[cfg(not(windows))]
                let reparse = false;
                if metadata.file_type().is_symlink() || reparse {
                    return Err(CliError(
                        "Rustdoc restoration path contains a reparse point or symlink".into(),
                    ));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error(error)),
        }
    }
    Ok(())
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
            // The catalog changes as qualified versions are admitted; it is
            // not an immutable artifact belonging to one generation.
            || path == output.join("sdk-docs-versions.v1.json")
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
            // Cargo's private profile target is execution scratch space, not
            // a generated artifact. It is deliberately only the output-root
            // directory: a nested directory with this name can be a published
            // docs asset and must remain observable by drift checks.
            if path == root.join(".profile-build") {
                continue;
            }
            collect_files(root, &path, files, include_all)?;
        } else if include_all {
            // Atomic publication leaves a short-lived sibling while the
            // immutable artifact is linked into place. It is producer scratch
            // space and must never enter the published artifact inventory.
            if path
                .file_name()
                .and_then(OsStr::to_str)
                .is_some_and(|name| name.contains(".tmp-"))
            {
                continue;
            }
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
            // Research notes and build caches are inputs to neither rustdoc
            // nor the generated documentation contract. Keep the closure
            // precise so an unrelated note cannot invalidate a bundle.
            if matches!(
                name,
                "research" | ".cache" | ".profile-build" | "target" | "node_modules"
            ) {
                continue;
            }
            collect_source_files(root, &path, output, files)?;
        } else if path.is_file() {
            let relative = path.strip_prefix(root).unwrap_or(&path);
            let components = relative.components().collect::<Vec<_>>();
            let under_crates = components.len() >= 3
                && components[0].as_os_str() == OsStr::new("rust")
                && components[1].as_os_str() == OsStr::new("crates");
            let under_plugin = components
                .first()
                .is_some_and(|component| component.as_os_str() == OsStr::new("plugin"));
            let under_doc_sources = components.first().is_some_and(|component| {
                matches!(
                    component.as_os_str().to_str(),
                    Some("docs" | "examples" | "scripts")
                )
            });
            let under_proto = components
                .first()
                .is_some_and(|component| component.as_os_str() == OsStr::new("proto"));
            let under_crate_proto = under_crates
                && components
                    .iter()
                    .any(|component| component.as_os_str() == OsStr::new("proto"));
            let under_conformance = under_crates
                && components
                    .iter()
                    .any(|component| component.as_os_str() == OsStr::new("conformance"));
            let relative_string = path_string(relative);
            let descriptor_input = matches!(
                relative_string.as_str(),
                "rust/crates/actors/src/generated/acyclic-actors-v1.bin"
                    | "rust/crates/filesystem/src/generated/acyclic-filesystem-v2.bin"
                    | "rust/crates/inference/inference_descriptor.bin"
                    | "rust/crates/machines/src/generated/acyclic-machines-v1.bin"
                    | "rust/crates/objects/src/generated/acyclic-objects-v2.bin"
                    | "rust/crates/stream/proto/stream/v2/stream_descriptor.bin"
                    | "rust/crates/workers/src/generated/acyclic-workers-v1.bin"
            );
            let plugin_install_asset = matches!(
                relative_string.as_str(),
                "plugin/.agents/plugins/marketplace.json"
                    | "plugin/.codex-plugin/plugin.json"
                    | "plugin/.mcp.json"
                    | "plugin/bin/acyclic"
                    | "plugin/bin/targets.json"
                    | "plugin/hooks/hooks.json"
                    | "plugin/package.json"
                    | "plugin/plugin.json"
            ) || (under_plugin
                && components
                    .iter()
                    .any(|component| component.as_os_str() == OsStr::new("bin"))
                && path.extension().and_then(OsStr::to_str) == Some("js"))
                || (under_plugin
                    && components
                        .iter()
                        .any(|component| component.as_os_str() == OsStr::new("scripts"))
                    && path.extension().and_then(OsStr::to_str) == Some("mjs"));
            let include = ((under_crates || under_plugin || under_doc_sources)
                && relative
                    .extension()
                    .and_then(OsStr::to_str)
                    .is_some_and(|ext| matches!(ext, "rs" | "toml" | "lock" | "md")))
                || ((under_proto || under_crate_proto)
                    && path.extension().and_then(OsStr::to_str) == Some("proto"))
                || (under_conformance && path.extension().and_then(OsStr::to_str) == Some("json"))
                || descriptor_input
                || plugin_install_asset
                || relative
                    == Path::new(
                        "rust/crates/sdk-generation/scripts/qualify-typescript-snippets.mjs",
                    )
                || relative == Path::new("Cargo.toml")
                || relative == Path::new("Cargo.lock")
                || relative == Path::new("rust-toolchain.toml")
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn release_version_must_match_published_sdk_version() {
        let versions = BTreeSet::from(["0.2.0".to_owned()]);
        let error = validate_release_package_versions("0.1.0", &versions)
            .expect_err("a stale docs version must not be released");
        assert!(error
            .to_string()
            .contains("does not match SDK package version"));
        validate_release_package_versions("0.2.0", &versions)
            .expect("matching SDK and docs versions should be accepted");
    }

    fn test_root(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be available")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "sdk-generation-collector-{label}-{}-{nonce}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("test root should be creatable");
        root
    }

    fn write(root: &Path, relative: &str, bytes: &[u8]) {
        let path = root.join(relative);
        fs::create_dir_all(path.parent().expect("fixture path has a parent"))
            .expect("fixture parent should be creatable");
        fs::write(path, bytes).expect("fixture file should be writable");
    }

    #[test]
    fn internal_profile_build_cache_is_excluded_but_public_outputs_are_hashed() {
        let root = test_root("artifact-cache");
        let output = root.join("output");
        write(&output, "sdk-docs-data.v2.json", b"docs");
        write(&output, ".profile-build/debug/cache.bin", b"cache-v1");
        write(
            &output,
            "assets/.profile-build/qualified-example.txt",
            b"published",
        );
        let before = artifact_hashes(&output).expect("artifact inventory should build");
        assert!(before.contains_key("sdk-docs-data.v2.json"));
        assert!(!before
            .keys()
            .any(|path| path.starts_with(".profile-build/")));
        assert_eq!(
            before.get("assets/.profile-build/qualified-example.txt"),
            Some(&sha256_bytes(b"published")),
            "only the output-root profile scratch directory is private"
        );

        write(&output, ".profile-build/debug/cache.bin", b"cache-v2");
        let after = artifact_hashes(&output).expect("artifact inventory should rebuild");
        assert_eq!(
            before, after,
            "internal cache mutation must not alter artifacts"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn contract_inputs_are_source_attested_with_precise_non_rust_rules() {
        let root = test_root("source-inputs");
        let output = root.join("output");
        for (relative, bytes) in [
            ("proto/demo.proto", b"syntax = \"proto3\";".as_slice()),
            (
                "rust/crates/stream/proto/stream/v2/stream.proto",
                b"message Stream {}".as_slice(),
            ),
            (
                "rust/crates/stream/conformance/stream.json",
                b"{\"suite\":true}".as_slice(),
            ),
            (
                "rust/crates/stream/proto/stream/v2/stream_descriptor.bin",
                b"descriptor-v1".as_slice(),
            ),
            (
                "plugin/bin/install.js",
                b"console.log('install');".as_slice(),
            ),
            (
                "plugin/scripts/package.mjs",
                b"export default {};".as_slice(),
            ),
            ("plugin/package.json", b"{\"name\":\"plugin\"}".as_slice()),
            ("rust/crates/demo/src/ignored.json", b"{}".as_slice()),
        ] {
            write(&root, relative, bytes);
        }

        for (relative, bytes) in [
            ("docs/research/ignored.md", b"research note".as_slice()),
            (
                "rust/crates/demo/research/ignored.md",
                b"crate research note".as_slice(),
            ),
            (".cache/ignored.md", b"build cache".as_slice()),
        ] {
            write(&root, relative, bytes);
        }

        let before = source_file_hashes(&root, &output).expect("source inventory should build");
        for required in [
            "proto/demo.proto",
            "rust/crates/stream/proto/stream/v2/stream.proto",
            "rust/crates/stream/conformance/stream.json",
            "rust/crates/stream/proto/stream/v2/stream_descriptor.bin",
            "plugin/bin/install.js",
            "plugin/scripts/package.mjs",
            "plugin/package.json",
        ] {
            assert!(
                before.contains_key(required),
                "missing source input {required}"
            );
        }
        for excluded in [
            "rust/crates/demo/src/ignored.json",
            "docs/research/ignored.md",
            "rust/crates/demo/research/ignored.md",
            ".cache/ignored.md",
        ] {
            assert!(
                !before.contains_key(excluded),
                "unexpected source input {excluded}"
            );
        }

        for (relative, changed) in [
            (
                "proto/demo.proto",
                b"syntax = \"proto3\"; message Changed {}".as_slice(),
            ),
            (
                "rust/crates/stream/proto/stream/v2/stream_descriptor.bin",
                b"descriptor-v2".as_slice(),
            ),
            (
                "rust/crates/stream/conformance/stream.json",
                b"{\"suite\":false}".as_slice(),
            ),
            (
                "plugin/bin/install.js",
                b"console.log('changed install');".as_slice(),
            ),
        ] {
            let original = fs::read(root.join(relative)).expect("source fixture should exist");
            write(&root, relative, changed);
            let after = source_file_hashes(&root, &output)
                .expect("source inventory should rebuild after an input edit");
            assert_ne!(
                digest_map(&before),
                digest_map(&after),
                "editing {relative} must change the source attestation"
            );
            fs::write(root.join(relative), original).expect("source fixture should restore");
        }
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn release_generation_requires_executed_profiles() {
        let error = parse_generate(vec![
            "--root".into(),
            "root".into(),
            "--output".into(),
            "output".into(),
            "--rustdoc-dir".into(),
            "rustdoc".into(),
            "--version".into(),
            "1.0.0".into(),
            "--channel".into(),
            "release".into(),
        ])
        .expect_err("release hand-supplied receipts must be rejected");
        assert!(error.to_string().contains("requires --execute-profiles"));
    }

    #[test]
    fn missing_external_rustdoc_span_is_restored_at_rustdoc_path() {
        let root = test_root("generated-span-staging")
            .canonicalize()
            .expect("test root should canonicalize");
        let candidate = root.join("target/candidate/wire.rs");
        write(&root, "target/candidate/wire.rs", b"pub struct Wire;\n");
        let foreign = root.join("target/producer/wire.rs");
        let receipt = root.join("receipt.json");
        fs::write(
            &receipt,
            serde_json::to_vec(&serde_json::json!({
                "span": { "filename": foreign.to_string_lossy() }
            }))
            .expect("span fixture should serialize"),
        )
        .expect("receipt should be writable");
        let sources = materialize_generated_sources(&root, std::slice::from_ref(&receipt))
            .expect("matching generated source should be staged");
        assert_eq!(sources.len(), 1);
        assert_eq!(
            sources[0].physical_path,
            foreign
                .canonicalize()
                .expect("restored span should canonicalize")
        );
        assert_eq!(fs::read(&foreign).unwrap(), fs::read(&candidate).unwrap());
        assert_eq!(sources[0].sha256, sha256_file(&candidate).unwrap());
        assert!(candidate.exists());
        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(foreign.parent().unwrap());
    }

    #[test]
    fn equivalent_rustdoc_source_paths_are_deduplicated() {
        let root = test_root("deduplicate-generated-span");
        write(&root, "target/candidate/wire.rs", b"pub struct Wire;\n");
        let canonical = root.join("target/candidate/wire.rs");
        let equivalent = root.join("target/candidate/./wire.rs");
        let receipt = root.join("receipt.json");
        fs::write(
            &receipt,
            serde_json::to_vec(&serde_json::json!({
                "spans": [
                    { "filename": canonical.to_string_lossy() },
                    { "filename": equivalent.to_string_lossy() }
                ]
            }))
            .expect("span fixture should serialize"),
        )
        .expect("receipt should be writable");

        let sources = materialize_generated_sources(&root, std::slice::from_ref(&receipt))
            .expect("equivalent source paths should be accepted");
        assert_eq!(sources.len(), 1);
        assert_eq!(
            sources[0].sha256,
            sha256_file(&canonical).expect("source should hash")
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn existing_external_rustdoc_span_keeps_canonical_input_path() {
        let root = test_root("existing-external-generated-span");
        let external = std::env::temp_dir()
            .join(format!(
                "sdk-generation-existing-span-{}",
                std::process::id()
            ))
            .join("wire.rs");
        if let Some(parent) = external.parent() {
            fs::create_dir_all(parent).expect("external fixture directory should be writable");
        }
        fs::write(&external, b"pub struct Wire;\n").expect("external source should be writable");
        let receipt = root.join("receipt.json");
        fs::write(
            &receipt,
            serde_json::to_vec(&serde_json::json!({
                "span": { "filename": external.to_string_lossy() }
            }))
            .expect("span fixture should serialize"),
        )
        .expect("receipt should be writable");

        let sources = materialize_generated_sources(&root, std::slice::from_ref(&receipt))
            .expect("existing external source should be attested");
        assert_eq!(sources.len(), 1);
        assert_eq!(
            sources[0].physical_path,
            external
                .canonicalize()
                .expect("external source should canonicalize")
        );
        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(external.parent().unwrap());
    }

    #[test]
    fn ambiguous_external_rustdoc_span_is_rejected() {
        let root = test_root("ambiguous-generated-span");
        write(&root, "target/one/wire.rs", b"pub struct One;\n");
        write(&root, "rust/two/wire.rs", b"pub struct Two;\n");
        let foreign = root.join("target/producer/wire.rs");
        let receipt = root.join("receipt.json");
        fs::write(
            &receipt,
            serde_json::to_vec(&serde_json::json!({
                "span": { "filename": foreign.to_string_lossy() }
            }))
            .expect("span fixture should serialize"),
        )
        .expect("receipt should be writable");
        let error = materialize_generated_sources(&root, std::slice::from_ref(&receipt))
            .expect_err("ambiguous generated source must fail closed");
        assert!(error.to_string().contains("basename is ambiguous"));
        assert!(!foreign.exists());
        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(foreign.parent().unwrap());
    }

    #[test]
    fn restoration_rejects_arbitrary_paths_and_parent_traversal() {
        let root = test_root("restoration-boundary");
        for path in [
            std::env::temp_dir().join(".cargo/sdk-docs-unowned/wire.rs"),
            root.join("rust/wire.rs"),
            root.join("target/../unowned/wire.rs"),
        ] {
            let error = validate_restore_path(&root, &[], &path).unwrap_err();
            assert!(
                error.to_string().contains("outside owned")
                    || error.to_string().contains("parent traversal")
            );
        }
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn captured_external_restoration_is_owned_and_digest_bound() {
        let root = test_root("captured-restoration").canonicalize().unwrap();
        let output = test_root("captured-restoration-output")
            .canonicalize()
            .unwrap();
        let foreign = output.join(".profile-build/build/demo/out/wire.rs");
        let receipt = output.join(".rustdoc/demo.json");
        let bytes = b"pub struct Captured;\n";
        let digest = sha256_bytes(bytes);
        let relative = format!(
            "target/sdk-generation-generated-sources/retained/{}-wire.rs",
            digest.trim_start_matches("sha256:")
        );
        write(&root, &relative, bytes);
        write(
            &output,
            ".rustdoc/demo.json",
            &serde_json::to_vec(&serde_json::json!({
                "span": { "filename": foreign.to_string_lossy() }
            }))
            .unwrap(),
        );
        let aliases = serde_json::to_vec(&BTreeMap::from([(
            foreign.to_string_lossy().to_string(),
            relative.clone(),
        )]))
        .unwrap();
        write(
            &root,
            "target/sdk-generation-generated-sources/retained/aliases.json",
            &aliases,
        );
        let sources = materialize_generated_sources(&root, std::slice::from_ref(&receipt)).unwrap();
        assert_eq!(sources[0].sha256, digest);
        assert_eq!(fs::read(&foreign).unwrap(), bytes);
        fs::remove_file(&foreign).unwrap();
        fs::write(root.join(relative), b"changed capture").unwrap();
        let error =
            materialize_generated_sources(&root, std::slice::from_ref(&receipt)).unwrap_err();
        assert!(error
            .to_string()
            .contains("captured Rustdoc source digest changed"));
        assert!(!foreign.exists());
        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(output);
    }

    #[test]
    fn identical_profile_outputs_keep_each_physical_span_mapping() {
        let root = test_root("identical-profile-spans").canonicalize().unwrap();
        let output = test_root("identical-profile-spans-output")
            .canonicalize()
            .unwrap();
        let first = output.join(".profile-build/build/one/out/wire.rs");
        let second = output.join(".profile-build/build/two/out/wire.rs");
        write(
            &output,
            ".profile-build/build/one/out/wire.rs",
            b"pub struct Wire;\n",
        );
        write(
            &output,
            ".profile-build/build/two/out/wire.rs",
            b"pub struct Wire;\n",
        );
        let receipt = output.join(".rustdoc/demo.json");
        write(&output, ".rustdoc/demo.json", &serde_json::to_vec(&serde_json::json!({
            "spans": [{ "filename": first.to_string_lossy() }, { "filename": second.to_string_lossy() }]
        })).unwrap());
        let sources = materialize_generated_sources(&root, &[receipt]).unwrap();
        assert_eq!(sources.len(), 2);
        assert_eq!(sources[0].sha256, sources[1].sha256);
        assert_eq!(sources[0].logical_path, sources[1].logical_path);
        assert_ne!(sources[0].physical_path, sources[1].physical_path);
        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(output);
    }

    #[test]
    #[ignore = "requires SDK_GENERATION_CORPUS_ROOT and SDK_GENERATION_CORPUS_RUSTDOC"]
    fn real_rustdoc_corpus_projects_every_receipt_source_span() {
        let root = PathBuf::from(env::var("SDK_GENERATION_CORPUS_ROOT").unwrap())
            .canonicalize()
            .unwrap();
        let rustdoc = PathBuf::from(env::var("SDK_GENERATION_CORPUS_RUSTDOC").unwrap())
            .canonicalize()
            .unwrap();
        let mut receipts = rustdoc_files(&rustdoc).unwrap();
        receipts.extend(rustdoc_files(&rustdoc.join("profiles")).unwrap());
        receipts.sort();
        receipts.dedup();
        let metadata = load_metadata(&root, None).unwrap();
        let sources = materialize_generated_sources(&root, &receipts).unwrap();
        println!(
            "collector diagnostic: {} receipts, {} generated span mappings",
            receipts.len(),
            sources.len()
        );
        for (index, receipt) in receipts.iter().enumerate() {
            let observation = observe_rustdoc(receipt).unwrap();
            let package = package_for_crate(&metadata, &observation.crate_name).unwrap();
            println!(
                "projecting {}/{}: {}",
                index + 1,
                receipts.len(),
                receipt.display()
            );
            let data = build_data(&BuildInput {
                version: "corpus-diagnostic".into(),
                channel: Channel::Preview,
                revision: git_revision(&root).unwrap(),
                source_state: "working-tree".into(),
                source_sha256: None,
                repository_root: root.clone(),
                rustdoc_files: vec![receipt.clone()],
                package_metadata: vec![PackageMetadata {
                    rustdoc_file: receipt.clone(),
                    package_name: package.name.to_string(),
                    crate_name: observation.crate_name,
                    version: package.version.to_string(),
                }],
                generated_sources: sources.clone(),
                mark_latest: false,
            })
            .unwrap();
            println!(
                "projected {}/{}: {} families",
                index + 1,
                receipts.len(),
                data.families.len()
            );
        }
    }

    #[cfg(windows)]
    #[test]
    fn restoration_rejects_junction_ancestors_before_writing() {
        let root = test_root("restoration-junction");
        let external = test_root("restoration-junction-external");
        fs::create_dir_all(root.join("target")).unwrap();
        let junction = root.join("target/linked");
        assert!(Command::new("powershell.exe")
            .env("SDK_DOCS_RESTORE_JUNCTION", &junction)
            .env("SDK_DOCS_RESTORE_TARGET", &external)
            .args(["-NoProfile", "-NonInteractive", "-Command",
                "New-Item -ItemType Junction -Path $env:SDK_DOCS_RESTORE_JUNCTION -Target $env:SDK_DOCS_RESTORE_TARGET | Out-Null"])
            .status().unwrap().success());
        let error = validate_restore_path(&root, &[], &junction.join("wire.rs")).unwrap_err();
        assert!(error.to_string().contains("reparse point or symlink"));
        assert!(!external.join("wire.rs").exists());
        fs::remove_dir(junction).unwrap();
        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(external);
    }
}
