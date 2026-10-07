use depinfo::RustcDepInfo;
use sdk_docs::{
    BuildInput, Channel, DocsData, GENERATOR_VERSION, GeneratedSource, build_data, write_bundle,
};
use sdk_docs::rustdoc_profiles::{
    OwnedApiItem, ProfileAvailability, ProfileId, ProfileSpec, api_owner_for_package,
    extract_owned_api_for_crate, load_metadata, observe_rustdoc, project_into_docs,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    process::Command,
};

mod compiled_generator_inputs;
mod native_targets;

const MANIFEST: &str = "generation-manifest.json";
const PROFILE_AVAILABILITY: &str = "sdk-docs-profile-availability.v1.json";
const VERSION: &str = env!("CARGO_PKG_VERSION");
const ACTORS_CRATE: &str = "acyclic_actors";
const SOURCE_PATHS: &[&str] = &[
    "Cargo.toml",
    "release/cargo-crates.json",
    "Cargo.lock",
    "rust/crates/sdk-docs/Cargo.toml",
    "rust/crates/sdk-docs/Cargo.lock",
    "rust/crates/sdk-docs/src",
    "rust/crates/actors-napi/Cargo.toml",
    "rust/crates/actors-napi/build.rs",
    "rust/crates/actors-napi/src",
    "rust/crates/actors-napi/README.md",
    "rust/crates/actors-napi/qualification",
    "rust/crates/sdk-generation/Cargo.toml",
    "rust/crates/sdk-generation/Cargo.lock",
    "rust/crates/sdk-generation/build.rs",
    "rust/crates/sdk-generation/rust-toolchain.toml",
    "rust/crates/sdk-generation/src",
    "rust/crates/sdk-generation/tests",
    "rust-toolchain.toml",
];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct FileHash {
    path: String,
    sha256: String,
    bytes: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct ToolRecord {
    id: String,
    version: String,
    channel: String,
    tool_sha256: String,
    #[serde(default)]
    pinned_toolchain: Option<PinnedToolchainRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PinnedToolIdentity {
    version: String,
    sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PinnedToolchainRecord {
    cargo: PinnedToolIdentity,
    rustc: PinnedToolIdentity,
    rustdoc: PinnedToolIdentity,
}

#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    schema: String,
    generator_version: String,
    version: String,
    /// Kept as the Actors staging marker for the existing TypeScript stage.
    family: String,
    /// The complete ordered set of published Rust documentation families.
    families: Vec<String>,
    revision: String,
    source: Vec<FileHash>,
    source_sha256: String,
    rustdoc: Vec<FileHash>,
    rustdoc_sha256: String,
    tool: ToolRecord,
    artifacts: Vec<FileHash>,
    artifacts_sha256: String,
}

#[derive(Debug)]
struct Config {
    operation: String,
    root: PathBuf,
    rustdoc_json: Option<PathBuf>,
    output: PathBuf,
    version: String,
    channel: String,
}

struct PinnedToolchain {
    cargo: PathBuf,
    rustc: PathBuf,
    rustdoc: PathBuf,
}

struct RustdocInput {
    paths: Vec<PathBuf>,
    markdown_dependencies: Vec<PathBuf>,
    generated_sources: Vec<GeneratedSource>,
    _cache_lock: Option<RustdocCacheLock>,
    _staging_lock: Option<RustdocStagingLock>,
    toolchain: Option<PinnedToolchainRecord>,
}

struct RustdocCacheLock {
    _file: File,
}

struct RustdocStagingLock {
    root: PathBuf,
    _file: File,
}

#[derive(Debug, Deserialize)]
struct CargoMetadata {
    packages: Vec<CargoPackage>,
}

#[derive(Debug, Deserialize)]
struct CargoPackage {
    name: String,
    version: String,
    manifest_path: PathBuf,
    publish: Option<Vec<String>>,
    targets: Vec<CargoTarget>,
    #[serde(default)]
    metadata: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct CargoTarget {
    name: String,
    kind: Vec<String>,
}

#[derive(Debug, Clone)]
struct RustdocOwner {
    package: String,
    version: String,
    crate_name: String,
    target_kind: String,
    target_name: String,
    package_root: PathBuf,
}

const ACTORS_GENERATED_ROOT: &str = "generated/actors";
const WORKERS_GENERATED_ROOT: &str = "generated/workers";
const WORKERS_PROTO_HEADER: &str = "// Generated from Rust-owned Workers contract. Do not edit.\n";
const ACTORS_TYPESCRIPT_ROOT: &str = "generated/typescript";
const ACTORS_TYPESCRIPT_BARREL: &str = "types.ts";

fn compiled_generator_digest(root: &Path) -> io::Result<String> {
    let mut hasher = Sha256::new();
    for relative in compiled_generator_inputs::PATHS {
        let path = root.join(relative);
        hasher.update(relative.as_bytes());
        hasher.update([0]);
        let mut file = File::open(&path)?;
        let mut buffer = [0u8; 64 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        hasher.update([0]);
    }
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

fn verify_compiled_generator_source(root: &Path) -> io::Result<()> {
    let configured = compiled_generator_digest(root)?;
    let compiled = env!("SDK_GENERATION_COMPILED_SOURCE_SHA256");
    if configured != compiled {
        return Err(io::Error::other(format!(
            "compiled generator source digest {configured} does not match the binary dependency {compiled}"
        )));
    }
    Ok(())
}

fn cargo_metadata(root: &Path) -> io::Result<CargoMetadata> {
    let cargo = rustup_tool("cargo")?;
    verify_tool_version(&cargo, "cargo")?;
    let mut command = Command::new(cargo);
    command
        .current_dir(root)
        .args([
            "metadata",
            "--locked",
            "--no-deps",
            "--format-version",
            "1",
            "--manifest-path",
        ])
        .arg(root.join("Cargo.toml"));
    command.env_remove("RUSTUP_TOOLCHAIN");
    let output = command.output().map_err(|error| {
        io::Error::other(format!("failed to run pinned cargo metadata: {error}"))
    })?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "pinned cargo metadata exited with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    serde_json::from_slice(&output.stdout).map_err(io::Error::other)
}

fn published_package(publish: &Option<Vec<String>>) -> bool {
    publish
        .as_ref()
        .is_none_or(|targets| targets.iter().any(|target| target == "crates-io"))
}

fn is_library_target(target: &CargoTarget) -> bool {
    target.kind.iter().any(|kind| {
        matches!(
            kind.as_str(),
            "lib" | "rlib" | "cdylib" | "dylib" | "proc-macro"
        )
    })
}

fn load_rustdoc_owners(root: &Path, metadata: &CargoMetadata) -> io::Result<Vec<RustdocOwner>> {
    let declared: Vec<String> =
        serde_json::from_slice(&fs::read(root.join("release/cargo-crates.json"))?)
            .map_err(io::Error::other)?;
    if declared.is_empty() {
        return Err(io::Error::other(
            "release/cargo-crates.json must declare at least one package",
        ));
    }
    let mut declared_set = BTreeSet::new();
    for package in &declared {
        if package.is_empty() || !declared_set.insert(package) {
            return Err(io::Error::other(format!(
                "release/cargo-crates.json contains an empty or duplicate package: {package:?}"
            )));
        }
    }

    let root = canonical(root)?;
    let mut packages = BTreeMap::new();
    for package in &metadata.packages {
        if packages.insert(package.name.clone(), package).is_some() {
            return Err(io::Error::other(
                "cargo metadata contains duplicate packages",
            ));
        }
    }

    let publishable: BTreeSet<String> = packages
        .values()
        .filter(|package| published_package(&package.publish))
        .map(|package| package.name.clone())
        .collect();
    let declared_set: BTreeSet<String> = declared.iter().cloned().collect();
    if declared_set != publishable {
        return Err(io::Error::other(format!(
            "release/cargo-crates.json does not exactly match publishable cargo packages; declared={declared_set:?}, metadata={publishable:?}"
        )));
    }

    let mut owners = Vec::with_capacity(declared.len());
    let mut crates = BTreeSet::new();
    for package_name in declared {
        let package = packages.get(&package_name).ok_or_else(|| {
            io::Error::other(format!(
                "release/cargo-crates.json names missing cargo package: {package_name}"
            ))
        })?;
        reject_reparse_ancestors(&package.manifest_path, true)?;
        let manifest = canonical(&package.manifest_path)?;
        let package_root = manifest
            .parent()
            .ok_or_else(|| io::Error::other("cargo package manifest has no parent"))?;
        if !package_root.starts_with(&root) {
            return Err(io::Error::other(format!(
                "cargo package escapes checkout: {}",
                package_root.display()
            )));
        }
        let package_root = package_root
            .strip_prefix(&root)
            .map_err(|_| io::Error::other("cargo package escapes checkout"))?
            .to_owned();

        let libraries: Vec<&CargoTarget> = package
            .targets
            .iter()
            .filter(|target| is_library_target(target))
            .collect();
        let (target_kind, target) = if libraries.len() == 1 {
            ("lib", libraries[0])
        } else if libraries.len() > 1 {
            return Err(io::Error::other(format!(
                "published package has multiple library targets: {package_name}"
            )));
        } else {
            let binaries: Vec<&CargoTarget> = package
                .targets
                .iter()
                .filter(|target| target.kind.iter().any(|kind| kind == "bin"))
                .collect();
            if binaries.len() != 1 {
                return Err(io::Error::other(format!(
                    "published package must have one library or one binary target: {package_name}"
                )));
            }
            ("bin", binaries[0])
        };
        let crate_name = target.name.replace('-', "_");
        if !crates.insert(crate_name.clone()) {
            return Err(io::Error::other(format!(
                "published packages resolve to duplicate rustdoc crate: {}",
                crate_name
            )));
        }
        owners.push(RustdocOwner {
            package: package.name.clone(),
            version: package.version.clone(),
            crate_name,
            target_kind: target_kind.into(),
            target_name: target.name.clone(),
            package_root,
        });
    }
    if !crates.contains(ACTORS_CRATE) {
        return Err(io::Error::other(
            "release/cargo-crates.json must include the Actors crate for the existing staging contract",
        ));
    }
    Ok(owners)
}

fn actors_napi_package<'a>(metadata: &'a CargoMetadata) -> io::Result<&'a CargoPackage> {
    metadata
        .packages
        .iter()
        .find(|package| package.name == native_targets::ACTORS_NAPI_PACKAGE)
        .ok_or_else(|| {
            io::Error::other(format!(
                "cargo metadata is missing {}",
                native_targets::ACTORS_NAPI_PACKAGE
            ))
        })
}

fn expected_native_targets(
    config: &Config,
    metadata: &CargoMetadata,
    revision: &str,
    source_sha256: &str,
) -> io::Result<native_targets::NativeTargetsArtifact> {
    let package = actors_napi_package(metadata)?;
    if package.version != config.version {
        return Err(io::Error::other(format!(
            "{} has version {}, expected {}",
            native_targets::ACTORS_NAPI_PACKAGE,
            package.version,
            config.version
        )));
    }
    let targets = native_targets::parse_targets(package.metadata.as_ref())?;
    Ok(native_targets::artifact(
        config.version.clone(),
        revision.to_owned(),
        source_sha256.to_owned(),
        targets,
    ))
}

fn write_native_targets(
    config: &Config,
    metadata: &CargoMetadata,
    revision: &str,
    source_sha256: &str,
) -> io::Result<()> {
    let artifact = expected_native_targets(config, metadata, revision, source_sha256)?;
    let path = config.output.join("generated/native-targets.json");
    let mut encoded = serde_json::to_vec_pretty(&artifact).map_err(io::Error::other)?;
    encoded.push(b'\n');
    fs::write(path, encoded)
}

fn validate_native_targets(
    config: &Config,
    metadata: &CargoMetadata,
    revision: &str,
    source_sha256: &str,
) -> io::Result<()> {
    let expected = expected_native_targets(config, metadata, revision, source_sha256)?;
    let path = config.output.join("generated/native-targets.json");
    let actual: native_targets::NativeTargetsArtifact =
        serde_json::from_slice(&fs::read(&path)?).map_err(io::Error::other)?;
    if actual != expected {
        return Err(io::Error::other(
            "generated native target metadata does not match Rust source attestation",
        ));
    }
    Ok(())
}

fn validate_owner_versions(config: &Config, owners: &[RustdocOwner]) -> io::Result<()> {
    if config.channel == "release" {
        for owner in owners {
            if owner.version != config.version {
                return Err(io::Error::other(format!(
                    "published package {} has version {}, expected release {}",
                    owner.package, owner.version, config.version
                )));
            }
        }
    }
    Ok(())
}

fn owner_package_roots(owners: &[RustdocOwner]) -> Vec<PathBuf> {
    owners
        .iter()
        .map(|owner| owner.package_root.clone())
        .collect()
}

fn generate_actors_contract_artifacts(config: &Config) -> io::Result<()> {
    let stage = config.output.join(ACTORS_GENERATED_ROOT);
    let proto_root = stage.join("proto");
    acyclic_actors::contract::render_proto_files(&proto_root).map_err(io::Error::other)?;
    let descriptor = stage.join("acyclic-actors-v1.bin");
    fs::write(&descriptor, acyclic_actors::FILE_DESCRIPTOR_SET)?;
    if !descriptor.is_file() {
        return Err(io::Error::other(
            "Actors contract metadata did not produce a descriptor",
        ));
    }
    Ok(())
}

fn generate_workers_contract_artifacts(config: &Config) -> io::Result<()> {
    let stage = config.output.join(WORKERS_GENERATED_ROOT);
    let proto_root = stage.join("proto");
    acyclic_workers::contract::render_proto_files(&proto_root).map_err(io::Error::other)?;
    let proto = proto_root.join("workers/v1/workers.proto");
    let rendered = fs::read_to_string(&proto)?;
    fs::write(&proto, format!("{WORKERS_PROTO_HEADER}{rendered}"))?;
    let descriptor = stage.join("acyclic-workers-v1.bin");
    fs::write(&descriptor, acyclic_workers::FILE_DESCRIPTOR_SET)?;
    Ok(())
}

fn generate_actors_typescript_artifacts(config: &Config) -> io::Result<()> {
    let stage = config.output.join(ACTORS_TYPESCRIPT_ROOT);
    acyclic_actors::domain::export_typescript(&stage).map_err(io::Error::other)?;
    let actors = stage.join("actors");
    if !actors.is_dir() {
        return Err(io::Error::other(
            "Actors TypeScript export did not produce its actors directory",
        ));
    }
    let mut modules = BTreeSet::new();
    for entry in fs::read_dir(&actors)? {
        let path = entry?.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() || is_reparse_point(&metadata) {
            return Err(io::Error::other(format!(
                "Actors TypeScript export contains a symlink: {}",
                path.display()
            )));
        }
        if metadata.is_file() && path.extension().is_some_and(|extension| extension == "ts") {
            let stem = path
                .file_stem()
                .ok_or_else(|| io::Error::other("Actors TypeScript file has no module name"))?
                .to_string_lossy()
                .into_owned();
            if stem != "types" {
                modules.insert(stem);
            }
        }
    }
    if modules.is_empty() {
        return Err(io::Error::other(
            "Actors TypeScript export did not produce any declaration modules",
        ));
    }
    let mut barrel = String::from("// Generated from Rust-owned Actors semantic declarations.\n");
    for module in modules {
        barrel.push_str(&format!("export * from \"./{module}.js\";\n"));
    }
    fs::write(actors.join(ACTORS_TYPESCRIPT_BARREL), barrel)?;
    Ok(())
}

fn digest(bytes: &[u8]) -> String {
    let mut hash = Sha256::new();
    hash.update(bytes);
    format!("sha256:{:x}", hash.finalize())
}

fn is_reparse_point(metadata: &fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        metadata.file_attributes() & 0x400 != 0
    }
    #[cfg(not(windows))]
    {
        let _ = metadata;
        false
    }
}

fn tree_digest(files: &[FileHash]) -> String {
    let mut encoded = Vec::new();
    for file in files {
        encoded.extend_from_slice(file.path.as_bytes());
        encoded.push(0);
        encoded.extend_from_slice(file.sha256.as_bytes());
        encoded.push(0);
        encoded.extend_from_slice(file.bytes.to_string().as_bytes());
        encoded.push(b'\n');
    }
    digest(&encoded)
}

fn hash_file(path: &Path, relative: String) -> io::Result<FileHash> {
    let mut file = File::open(path)?;
    let mut hash = Sha256::new();
    let mut bytes = 0;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
        bytes += read as u64;
    }
    Ok(FileHash {
        path: relative,
        sha256: format!("sha256:{:x}", hash.finalize()),
        bytes,
    })
}

fn canonical(path: &Path) -> io::Result<PathBuf> {
    fs::canonicalize(path).map_err(|error| {
        io::Error::other(format!("cannot canonicalize {}: {error}", path.display()))
    })
}

fn absolute_without_resolving(path: &Path) -> io::Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_owned())
    } else {
        Ok(env::current_dir()?.join(path))
    }
}

fn reject_reparse_ancestors(path: &Path, require_leaf: bool) -> io::Result<()> {
    let absolute = absolute_without_resolving(path)?;
    let mut current = absolute.as_path();
    let mut checked_leaf = false;
    loop {
        match fs::symlink_metadata(current) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || is_reparse_point(&metadata) {
                    return Err(io::Error::other(format!(
                        "symlink or reparse point is not allowed: {}",
                        current.display()
                    )));
                }
                checked_leaf = true;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                if !checked_leaf && require_leaf {
                    return Err(io::Error::other(format!(
                        "required path does not exist: {}",
                        path.display()
                    )));
                }
            }
            Err(error) => return Err(error),
        }
        let Some(parent) = current.parent() else {
            break;
        };
        if parent == current {
            break;
        }
        current = parent;
    }
    Ok(())
}

fn collect_dir(
    root: &Path,
    current: &Path,
    files: &mut BTreeMap<String, FileHash>,
) -> io::Result<()> {
    for entry in fs::read_dir(current)? {
        let path = entry?.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() || is_reparse_point(&metadata) {
            return Err(io::Error::other(format!(
                "symlink is not allowed: {}",
                path.display()
            )));
        }
        let canonical_path = canonical(&path)?;
        if !canonical_path.starts_with(root) {
            return Err(io::Error::other(format!(
                "declared path escapes checkout: {}",
                path.display()
            )));
        }
        if metadata.is_dir() {
            collect_dir(root, &path, files)?;
        } else if metadata.is_file() {
            let relative = path
                .strip_prefix(root)
                .map_err(|_| io::Error::other("path escapes declared root"))?
                .to_string_lossy()
                .replace('\\', "/");
            if files
                .insert(relative.clone(), hash_file(&path, relative.clone())?)
                .is_some()
            {
                return Err(io::Error::other(format!(
                    "duplicate source path: {relative}"
                )));
            }
        }
    }
    Ok(())
}

fn collect_sources(
    root: &Path,
    markdown_dependencies: &[PathBuf],
    owner_roots: &[PathBuf],
) -> io::Result<Vec<FileHash>> {
    let root_metadata = fs::symlink_metadata(root)?;
    if root_metadata.file_type().is_symlink() || is_reparse_point(&root_metadata) {
        return Err(io::Error::other(
            "checkout root symlink or reparse point is not allowed",
        ));
    }
    let root = canonical(root)?;
    let mut files = BTreeMap::new();
    let declarations = SOURCE_PATHS
        .iter()
        .map(PathBuf::from)
        .chain(owner_roots.iter().cloned());
    for declaration in declarations {
        let declaration_path = root.join(&declaration);
        let declaration_metadata = fs::symlink_metadata(&declaration_path)?;
        if declaration_metadata.file_type().is_symlink() || is_reparse_point(&declaration_metadata)
        {
            return Err(io::Error::other(format!(
                "source symlink is not allowed: {}",
                declaration.display()
            )));
        }
        let path = canonical(&declaration_path)?;
        if !path.starts_with(&root) {
            return Err(io::Error::other("source declaration escapes checkout"));
        }
        if path.is_dir() {
            collect_dir(&root, &path, &mut files)?;
        } else if path.is_file() {
            let relative = path
                .strip_prefix(&root)
                .map_err(|_| io::Error::other("source declaration escapes checkout"))?
                .to_string_lossy()
                .replace('\\', "/");
            if files
                .insert(relative.clone(), hash_file(&path, relative.clone())?)
                .is_some()
            {
                return Err(io::Error::other(format!(
                    "duplicate source path: {relative}"
                )));
            }
        } else {
            return Err(io::Error::other(format!(
                "declared source does not exist: {}",
                declaration.display()
            )));
        }
    }
    for dependency in markdown_dependencies {
        let dependency_path = if dependency.is_absolute() {
            dependency.clone()
        } else {
            root.join(dependency)
        };
        let metadata = fs::symlink_metadata(&dependency_path)?;
        if metadata.file_type().is_symlink() || is_reparse_point(&metadata) {
            return Err(io::Error::other(format!(
                "source symlink is not allowed: {}",
                dependency_path.display()
            )));
        }
        let path = canonical(&dependency_path)?;
        if !path.starts_with(&root) {
            return Err(io::Error::other(format!(
                "source dependency is outside the checkout: {}",
                dependency_path.display()
            )));
        }
        let relative = path
            .strip_prefix(&root)
            .map_err(|_| io::Error::other("rustdoc dependency escapes checkout"))?
            .to_string_lossy()
            .replace('\\', "/");
        let file = hash_file(&path, relative.clone())?;
        if let Some(existing) = files.get(&relative) {
            if existing != &file {
                return Err(io::Error::other(format!(
                    "source path changed while collecting: {relative}"
                )));
            }
        } else {
            files.insert(relative, file);
        }
    }
    if files.is_empty() {
        return Err(io::Error::other("source closure is empty"));
    }
    Ok(files.into_values().collect())
}

fn active_cargo_config_paths(root: &Path) -> io::Result<Vec<PathBuf>> {
    let root = canonical(root)?;
    let mut current = root.clone();
    let mut paths = Vec::new();
    loop {
        for name in ["config.toml", "config"] {
            let path = current.join(".cargo").join(name);
            if path.is_file() {
                paths.push(path);
            }
        }
        if current == root {
            break;
        }
        current = current
            .parent()
            .ok_or_else(|| io::Error::other("source checkout has no parent"))?
            .to_owned();
    }
    paths.sort();
    Ok(paths)
}

fn baseline_source_extras(root: &Path) -> io::Result<Vec<PathBuf>> {
    let mut paths = tracked_markdown_paths(root)?;
    paths.extend(active_cargo_config_paths(root)?);
    Ok(paths)
}

fn tracked_markdown_paths(root: &Path) -> io::Result<Vec<PathBuf>> {
    let output = Command::new("git")
        .args(["ls-files", "-z", "--", "*.md"])
        .current_dir(root)
        .output()?;
    if !output.status.success() {
        return Err(io::Error::other("git ls-files for Markdown failed"));
    }
    output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .map(|path| {
            String::from_utf8(path.to_vec())
                .map(PathBuf::from)
                .map_err(io::Error::other)
        })
        .collect()
}

fn tracked_markdown_set(root: &Path) -> io::Result<BTreeSet<PathBuf>> {
    tracked_markdown_paths(root)?
        .into_iter()
        .map(|path| canonical(&root.join(path)))
        .collect()
}

fn collect_json(
    root: &Path,
    current: &Path,
    files: &mut BTreeMap<String, FileHash>,
) -> io::Result<()> {
    for entry in fs::read_dir(current)? {
        let path = entry?.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() || is_reparse_point(&metadata) {
            return Err(io::Error::other(format!(
                "rustdoc symlink is not allowed: {}",
                path.display()
            )));
        }
        let canonical_path = canonical(&path)?;
        if !canonical_path.starts_with(root) {
            return Err(io::Error::other("rustdoc path escapes input root"));
        }
        if metadata.is_dir() {
            collect_json(root, &path, files)?;
        } else if metadata.is_file()
            && path
                .extension()
                .is_some_and(|extension| extension == "json")
        {
            let relative = path
                .strip_prefix(root)
                .map_err(|_| io::Error::other("rustdoc path escapes input root"))?
                .to_string_lossy()
                .replace('\\', "/");
            if files
                .insert(relative.clone(), hash_file(&path, relative.clone())?)
                .is_some()
            {
                return Err(io::Error::other(format!(
                    "duplicate rustdoc path: {relative}"
                )));
            }
        }
    }
    Ok(())
}

fn collect_rustdoc(path: &Path) -> io::Result<(Vec<FileHash>, Vec<PathBuf>)> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || is_reparse_point(&metadata) {
        return Err(io::Error::other("rustdoc symlink is not allowed"));
    }
    let path = canonical(path)?;
    let mut files = BTreeMap::new();
    let mut paths = Vec::new();
    if path.is_file() {
        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();
        files.insert(name.clone(), hash_file(&path, name)?);
        paths.push(path);
    } else if path.is_dir() {
        collect_json(&path, &path, &mut files)?;
        paths = files.keys().map(|relative| path.join(relative)).collect();
    }
    if files.is_empty() {
        return Err(io::Error::other("rustdoc input contains no JSON files"));
    }
    Ok((files.into_values().collect(), paths))
}

fn collect_rustdoc_files(paths: &[PathBuf]) -> io::Result<Vec<FileHash>> {
    let mut files = BTreeMap::new();
    for path in paths {
        let metadata = fs::symlink_metadata(path)?;
        if metadata.file_type().is_symlink() || is_reparse_point(&metadata) {
            return Err(io::Error::other(format!(
                "rustdoc symlink or reparse point is not allowed: {}",
                path.display()
            )));
        }
        let path = canonical(path)?;
        if !path.is_file() {
            return Err(io::Error::other(format!(
                "rustdoc input is not a file: {}",
                path.display()
            )));
        }
        let name = path
            .file_name()
            .ok_or_else(|| io::Error::other("rustdoc input has no file name"))?
            .to_string_lossy()
            .into_owned();
        if files
            .insert(name.clone(), hash_file(&path, name.clone())?)
            .is_some()
        {
            return Err(io::Error::other(format!(
                "duplicate rustdoc crate file: {name}"
            )));
        }
    }
    if files.is_empty() {
        return Err(io::Error::other("rustdoc input contains no JSON files"));
    }
    Ok(files.into_values().collect())
}

fn rustdoc_markdown_dependencies(
    dep_info: &Path,
    checkout: &Path,
    dependency_root: &Path,
) -> io::Result<Vec<PathBuf>> {
    let checkout = canonical(checkout)?;
    let dependency_root = canonical(dependency_root)?;
    let tracked_markdown = tracked_markdown_set(&checkout)?;
    let mut paths = Vec::new();
    let dep_info = read_dep_info(dep_info)?;
    for dependency in dep_info.files {
        let path = resolve_dep_info_path(dependency, &dependency_root);
        if path.extension().is_none_or(|extension| extension != "md") {
            continue;
        }
        reject_unsupported_make_escapes(&path)?;
        if !path.is_file() {
            return Err(io::Error::other(format!(
                "rustdoc dep-info references missing Markdown: {}",
                path.display()
            )));
        }
        let path = canonical(&path)?;
        if !path.starts_with(&checkout) {
            return Err(io::Error::other(format!(
                "rustdoc dep-info references Markdown outside checkout: {}",
                path.display()
            )));
        }
        if !tracked_markdown.contains(&path) {
            return Err(io::Error::other(format!(
                "rustdoc dep-info references untracked Markdown: {}",
                path.display()
            )));
        }
        paths.push(path);
    }
    paths.sort();
    paths.dedup();
    Ok(paths)
}

fn resolve_dep_info_path(dependency: PathBuf, dependency_root: &Path) -> PathBuf {
    if dependency.is_absolute() {
        dependency
    } else {
        dependency_root.join(dependency)
    }
}

fn resolve_dep_info_path_with_roots(
    dependency: PathBuf,
    checkout_root: &Path,
    target_root: &Path,
) -> io::Result<PathBuf> {
    if dependency.is_absolute() {
        return Ok(dependency);
    }
    let checkout_path = checkout_root.join(&dependency);
    let target_path = target_root.join(&dependency);
    let checkout_exists = fs::symlink_metadata(&checkout_path).is_ok();
    let target_exists = fs::symlink_metadata(&target_path).is_ok();
    match (checkout_exists, target_exists) {
        (true, false) => Ok(checkout_path),
        (false, true) => Ok(target_path),
        (false, false) => Ok(checkout_path),
        (true, true) => {
            let checkout_canonical = canonical(&checkout_path)?;
            let target_canonical = canonical(&target_path)?;
            if checkout_canonical == target_canonical {
                Ok(checkout_path)
            } else {
                Err(io::Error::other(format!(
                    "rustdoc dep-info path is ambiguous between checkout and target: {}",
                    dependency.display()
                )))
            }
        }
    }
}

fn generated_out_dir(
    dep_info: &RustcDepInfo,
    dep_info_target: &Path,
    owner: &RustdocOwner,
) -> io::Result<Option<PathBuf>> {
    let values: Vec<&Option<String>> = dep_info
        .env
        .iter()
        .filter(|(name, _)| name == "OUT_DIR")
        .map(|(_, value)| value)
        .collect();
    if values.len() > 1 {
        return Err(io::Error::other(format!(
            "rustdoc dep-info contains multiple OUT_DIR values for {}",
            owner.package
        )));
    }
    let Some(value) = values.first().copied() else {
        return Ok(None);
    };
    let value = value.as_ref().ok_or_else(|| {
        io::Error::other(format!(
            "rustdoc dep-info contains an unset OUT_DIR for {}",
            owner.package
        ))
    })?;
    let target = canonical(dep_info_target)?;
    let raw_out_dir = Path::new(value);
    reject_reparse_ancestors(raw_out_dir, true)?;
    let out_dir = canonical(raw_out_dir)?;
    reject_reparse_ancestors(&out_dir, true)?;
    if !out_dir.is_dir() {
        return Err(io::Error::other(format!(
            "rustdoc OUT_DIR is not a directory: {}",
            out_dir.display()
        )));
    }
    let relative = out_dir.strip_prefix(&target).map_err(|_| {
        io::Error::other(format!(
            "rustdoc OUT_DIR escapes its dep-info target: {}",
            out_dir.display()
        ))
    })?;
    let components: Vec<String> = relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy().into_owned())
        .collect();
    if components.len() != 4
        || components[0] != "debug"
        || components[1] != "build"
        || components[3] != "out"
        || !components[2].starts_with(&format!("{}-", owner.package))
        || components[2].len() <= owner.package.len() + 1
    {
        return Err(io::Error::other(format!(
            "rustdoc OUT_DIR is not the declared build output for {}: {}",
            owner.package,
            out_dir.display()
        )));
    }
    Ok(Some(out_dir))
}

fn path_has_owner_out_dir(path: &Path, target: &Path, owner: &RustdocOwner) -> bool {
    let Ok(relative) = path.strip_prefix(target) else {
        return false;
    };
    let components: Vec<String> = relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy().into_owned())
        .collect();
    components.windows(4).any(|window| {
        window[0] == "debug"
            && window[1] == "build"
            && window[2].starts_with(&format!("{}-", owner.package))
            && window[3] == "out"
    })
}

fn generated_sources_for_owner(
    config: &Config,
    dep_info: &RustcDepInfo,
    dep_info_target: &Path,
    json_target: &Path,
    owner: &RustdocOwner,
) -> io::Result<Vec<GeneratedSource>> {
    let Some(dep_out) = generated_out_dir(dep_info, dep_info_target, owner)? else {
        let target = canonical(dep_info_target)?;
        for dependency in &dep_info.files {
            let path = resolve_dep_info_path_with_roots(
                dependency.clone(),
                &config.root,
                dep_info_target,
            )?;
            reject_reparse_ancestors(&path, true)?;
            let path = canonical(&path)?;
            if path_has_owner_out_dir(&path, &target, owner) {
                return Err(io::Error::other(format!(
                    "rustdoc dep-info references generated output without OUT_DIR for {}",
                    owner.package
                )));
            }
        }
        return Ok(Vec::new());
    };
    let dep_target = canonical(dep_info_target)?;
    let json_target = canonical(json_target)?;
    let relative_out = dep_out
        .strip_prefix(&dep_target)
        .map_err(|_| io::Error::other("validated rustdoc OUT_DIR is outside its target"))?;
    let json_out = json_target.join(relative_out);
    reject_reparse_ancestors(&json_out, true)?;
    let json_out = canonical(&json_out)?;
    if !json_out.starts_with(&json_target) || !json_out.is_dir() {
        return Err(io::Error::other(format!(
            "matching rustdoc JSON OUT_DIR is invalid: {}",
            json_out.display()
        )));
    }

    let mut declared = BTreeSet::new();
    let mut generated_files = Vec::new();
    for dependency in &dep_info.files {
        let path =
            resolve_dep_info_path_with_roots(dependency.clone(), &config.root, dep_info_target)?;
        reject_reparse_ancestors(&path, true)?;
        let path = canonical(&path)?;
        if !path_has_owner_out_dir(&path, &dep_target, owner) {
            continue;
        }
        if !path.starts_with(&dep_out) || path == dep_out {
            return Err(io::Error::other(format!(
                "rustdoc dep-info generated path escapes OUT_DIR: {}",
                path.display()
            )));
        }
        let relative = path
            .strip_prefix(&dep_out)
            .map_err(|_| io::Error::other("generated path escapes OUT_DIR"))?
            .to_string_lossy()
            .replace('\\', "/");
        if !declared.insert(relative.clone()) {
            return Err(io::Error::other(format!(
                "rustdoc dep-info declares generated path more than once for {}",
                owner.package
            )));
        }
        let relative_path = Path::new(&relative);
        let json_file = json_out.join(relative_path);
        reject_reparse_ancestors(&json_file, true)?;
        let json_file = canonical(&json_file)?;
        if !json_file.starts_with(&json_out) || !json_file.is_file() {
            return Err(io::Error::other(format!(
                "matching rustdoc JSON generated file is invalid: {}",
                json_file.display()
            )));
        }
        let dep_file = hash_file(&path, relative.clone())?;
        let json_hash = hash_file(&json_file, relative.clone())?;
        if dep_file.sha256 != json_hash.sha256 || dep_file.bytes != json_hash.bytes {
            return Err(io::Error::other(format!(
                "rustdoc generated file differs between lanes: {relative}"
            )));
        }
        generated_files.push((relative, path, json_file, dep_file));
    }
    generated_files.sort_by(|left, right| left.0.cmp(&right.0));

    let output_root = (config.operation == "generate")
        .then(|| canonical(&config.output))
        .transpose()?;
    let mut generated_sources = Vec::with_capacity(generated_files.len());
    for (relative, source, json_file, file) in generated_files {
        let logical_path = PathBuf::from(format!("generated/{}/{}", owner.crate_name, relative));
        if let Some(output_root) = &output_root {
            let destination = output_root.join(&logical_path);
            if !destination.starts_with(output_root) {
                return Err(io::Error::other("generated output escapes generation root"));
            }
            if let Some(parent) = destination.parent() {
                reject_reparse_ancestors(parent, false)?;
                fs::create_dir_all(parent)?;
                reject_reparse_ancestors(parent, true)?;
            }
            reject_reparse_ancestors(&destination, false)?;
            fs::copy(&source, &destination)?;
            let copied_hash = hash_file(&destination, relative.clone())?;
            if copied_hash.sha256 != file.sha256 || copied_hash.bytes != file.bytes {
                return Err(io::Error::other(format!(
                    "copied generated file changed while staging: {relative}"
                )));
            }
        }
        generated_sources.push(GeneratedSource {
            physical_path: json_file,
            logical_path,
            sha256: file.sha256,
        });
    }
    Ok(generated_sources)
}

/// Rustdoc records generated source spans using the compiler's physical OUT_DIR.
/// The directory name is content-addressed by Cargo and can change when the
/// same checkout is built in a different target directory.  Attest those
/// bytes first, then project the path through a stable sibling of the checkout
/// so the JSON consumed by sdk-docs contains a checkout-relative identity.
fn normalize_rustdoc_json(
    json: &Path,
    generated_sources: &[GeneratedSource],
    staging_root: &Path,
    json_target: &Path,
) -> io::Result<(PathBuf, Vec<GeneratedSource>)> {
    let staging_root = canonical(staging_root)?;

    let mut replacements = BTreeMap::new();
    let mut attested = Vec::with_capacity(generated_sources.len());
    for source in generated_sources {
        reject_reparse_ancestors(&source.physical_path, true)?;
        let physical = canonical(&source.physical_path)?;
        let bytes = fs::read(&physical)?;
        if digest(&bytes) != source.sha256 {
            return Err(io::Error::other(format!(
                "generated source digest changed before rustdoc normalization: {}",
                physical.display()
            )));
        }
        let destination = staging_root.join(&source.logical_path);
        if !destination.starts_with(&staging_root) {
            return Err(io::Error::other(
                "generated source staging escapes its root",
            ));
        }
        if let Some(parent) = destination.parent() {
            reject_reparse_ancestors(parent, false)?;
            fs::create_dir_all(parent)?;
            reject_reparse_ancestors(parent, true)?;
        }
        reject_reparse_ancestors(&destination, false)?;
        fs::write(&destination, &bytes)?;
        let destination_hash = hash_file(&destination, source.logical_path.display().to_string())?;
        if destination_hash.sha256 != source.sha256 || destination_hash.bytes != bytes.len() as u64
        {
            return Err(io::Error::other(format!(
                "generated source staging changed while normalizing: {}",
                destination.display()
            )));
        }
        let destination = canonical(&destination)?;
        if replacements
            .insert(physical, stable_generated_reference(&source.logical_path))
            .is_some()
        {
            return Err(io::Error::other(
                "generated source physical path is declared more than once",
            ));
        }
        attested.push(GeneratedSource {
            physical_path: destination,
            logical_path: source.logical_path.clone(),
            sha256: source.sha256.clone(),
        });
    }

    let mut value: serde_json::Value = serde_json::from_slice(&fs::read(json)?)
        .map_err(|error| io::Error::other(format!("invalid rustdoc JSON: {error}")))?;
    normalize_rustdoc_paths(&mut value, &replacements);
    let bytes = serde_json::to_vec(&value).map_err(io::Error::other)?;
    let normalized_root = canonical(json_target)?.join("normalized");
    reject_reparse_ancestors(&normalized_root, false)?;
    fs::create_dir_all(&normalized_root)?;
    reject_reparse_ancestors(&normalized_root, true)?;
    let normalized = normalized_root.join(
        json.file_name()
            .ok_or_else(|| io::Error::other("rustdoc JSON has no file name"))?,
    );
    reject_reparse_ancestors(&normalized, false)?;
    fs::write(&normalized, bytes)?;
    Ok((canonical(&normalized)?, attested))
}

fn stable_generated_reference(logical_path: &Path) -> String {
    PathBuf::from("..")
        .join(".sdk-docs-generated")
        .join(logical_path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn rustdoc_absolute_path(value: &str) -> Option<PathBuf> {
    let value = value.strip_prefix(r"\\?\").unwrap_or(value);
    let path = PathBuf::from(value);
    path.is_absolute().then(|| canonical(&path).ok()).flatten()
}

fn normalize_rustdoc_paths(
    value: &mut serde_json::Value,
    replacements: &BTreeMap<PathBuf, String>,
) {
    if let serde_json::Value::Object(values) = value {
        if let Some(serde_json::Value::Object(span)) = values.get_mut("span") {
            if let Some(serde_json::Value::String(filename)) = span.get_mut("filename") {
                if let Some(path) = rustdoc_absolute_path(filename) {
                    if let Some(reference) = replacements.get(&path) {
                        *filename = reference.clone();
                    }
                }
            }
        }
    }
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                normalize_rustdoc_paths(value, replacements);
            }
        }
        serde_json::Value::Object(values) => {
            for (key, value) in values {
                if key == "span" {
                    continue;
                }
                if key == "attrs" {
                    normalize_rustdoc_attrs(value, replacements);
                    continue;
                }
                normalize_rustdoc_paths(value, replacements);
            }
        }
        _ => {}
    }
}

fn normalize_rustdoc_attrs(
    value: &mut serde_json::Value,
    replacements: &BTreeMap<PathBuf, String>,
) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                normalize_rustdoc_attrs(value, replacements);
            }
        }
        serde_json::Value::Object(values) => {
            if let Some(serde_json::Value::String(other)) = values.get_mut("other") {
                // rustdoc serializes compiler-generated attribute spans inside
                // `attrs.other`. `Attribute::Other` can also be authored by a
                // crate, so require the exact compiler serialization markers
                // observed around these spans before rewriting paths.
                if is_compiler_generated_attribute_span(other) {
                    normalize_generated_attribute_paths(other, replacements);
                }
            }
            for value in values.values_mut() {
                normalize_rustdoc_attrs(value, replacements);
            }
        }
        _ => {}
    }
}

fn is_compiler_generated_attribute_span(value: &str) -> bool {
    value.contains("CfgTrace") && value.contains("span:") && value.contains("(#")
}

fn normalize_generated_attribute_paths(
    value: &mut String,
    replacements: &BTreeMap<PathBuf, String>,
) {
    for (physical, reference) in replacements {
        let display = physical.to_string_lossy();
        let slash_display = display.replace('\\', "/");
        let candidates = [
            display.to_string(),
            format!(r"\?\{}", display),
            format!(r"\\?\{}", display),
            slash_display.clone(),
            format!(r"\?\{}", slash_display),
            format!(r"\\?\{}", slash_display),
        ];
        for candidate in candidates {
            replace_exact_generated_candidate(value, &candidate, reference);
        }
        let path_for_matching = slash_display
            .strip_prefix(r"//?/")
            .or_else(|| slash_display.strip_prefix(r"/?/"))
            .unwrap_or(&slash_display);
        replace_generated_path_with_mixed_separators(value, path_for_matching, reference);
    }
}

fn replace_exact_generated_candidate(value: &mut String, candidate: &str, reference: &str) {
    let mut search_from = 0;
    while let Some(found) = value[search_from..].find(candidate) {
        let start = search_from + found;
        let end = start + candidate.len();
        if generated_path_start_boundary(value.as_bytes(), start)
            && generated_path_boundary(value.as_bytes(), end)
        {
            value.replace_range(start..end, reference);
            search_from = start + reference.len();
        } else {
            search_from = end;
        }
    }
}

fn generated_path_start_boundary(value: &[u8], index: usize) -> bool {
    if index >= 3 && value[..index].ends_with(br"\?\") {
        return true;
    }
    if index >= 4 && value[..index].ends_with(br"\\?\") {
        return true;
    }
    match index.checked_sub(1).and_then(|index| value.get(index)) {
        None => true,
        Some(byte) => {
            !byte.is_ascii_alphanumeric() && !matches!(*byte, b'.' | b'_' | b'-' | b'/' | b'\\')
        }
    }
}

fn generated_path_boundary(value: &[u8], index: usize) -> bool {
    match value.get(index) {
        None => true,
        Some(byte) => {
            !byte.is_ascii_alphanumeric() && !matches!(*byte, b'.' | b'_' | b'-' | b'/' | b'\\')
        }
    }
}

fn replace_generated_path_with_mixed_separators(
    value: &mut String,
    slash_path: &str,
    reference: &str,
) {
    let Some(prefix) = slash_path
        .as_bytes()
        .get(0..2)
        .filter(|prefix| prefix[1] == b':')
    else {
        return;
    };
    let prefix = String::from_utf8_lossy(prefix);
    let mut search_from = 0;
    while let Some(found) = value[search_from..].find(prefix.as_ref()) {
        let start = search_from + found;
        let mut value_index = start;
        let mut path_index = 0;
        while path_index < slash_path.len() {
            let Some(expected) = slash_path.as_bytes().get(path_index).copied() else {
                break;
            };
            if expected == b'/' {
                if !matches!(value.as_bytes().get(value_index), Some(b'/' | b'\\')) {
                    break;
                }
            } else if value.as_bytes().get(value_index).copied() != Some(expected) {
                break;
            }
            value_index += 1;
            path_index += 1;
        }
        if path_index == slash_path.len()
            && generated_path_start_boundary(value.as_bytes(), start)
            && generated_path_boundary(value.as_bytes(), value_index)
        {
            let mut replacement_start = start;
            let before = &value[..start];
            for prefix in [r"\\?\", r"\?\"] {
                if before.ends_with(prefix) {
                    replacement_start -= prefix.len();
                    break;
                }
            }
            value.replace_range(replacement_start..value_index, reference);
            search_from = replacement_start + reference.len();
        } else {
            search_from = start + prefix.len();
        }
    }
}

fn reject_unsupported_make_escapes(path: &Path) -> io::Result<()> {
    let display = path.to_string_lossy();
    if display.contains(r"\#") || display.contains(r"\:") {
        return Err(io::Error::other(format!(
            "rustdoc dep-info contains unsupported Make-escaped Markdown path: {}; rename the path or emit an unescaped path",
            path.display()
        )));
    }
    Ok(())
}

fn read_dep_info(path: &Path) -> io::Result<RustcDepInfo> {
    let contents = fs::read_to_string(path)?;
    parse_dep_info_contents(&contents)
}

fn parse_dep_info_contents(contents: &str) -> io::Result<RustcDepInfo> {
    // rustc may wrap a dependency list with a physical `\\` continuation. The
    // maintained parser handles escaped paths, but intentionally leaves line
    // joining to its caller, so normalize only the continuation boundary.
    let normalized = contents.replace("\\\r\n", " ").replace("\\\n", " ");
    let dep_info: RustcDepInfo = normalized.parse().map_err(io::Error::other)?;
    if dep_info.files.is_empty() {
        return Err(io::Error::other("rustdoc dep-info contains no files"));
    }
    Ok(dep_info)
}

fn git_revision(root: &Path) -> io::Result<String> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(root)
        .output()?;
    if !output.status.success() {
        return Err(io::Error::other("git rev-parse HEAD failed"));
    }
    let revision = String::from_utf8(output.stdout)
        .map_err(io::Error::other)?
        .trim()
        .to_owned();
    if !(40..=64).contains(&revision.len())
        || !revision.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(io::Error::other(
            "git HEAD is not a trusted hexadecimal revision",
        ));
    }
    Ok(revision)
}

fn require_clean_release(root: &Path, channel: &str) -> io::Result<()> {
    if channel != "release" {
        return Ok(());
    }
    let output = Command::new("git")
        .args(["status", "--porcelain=v1", "--untracked-files=all"])
        .current_dir(root)
        .output()?;
    if !output.status.success() {
        return Err(io::Error::other("git status failed"));
    }
    if !output.stdout.is_empty() {
        return Err(io::Error::other(
            "release generation requires a clean checkout matching HEAD",
        ));
    }
    Ok(())
}

fn docs_channel(channel: &str) -> Channel {
    if channel == "release" {
        Channel::Release
    } else {
        Channel::Preview
    }
}

fn validate_rustdoc_data(data: &DocsData, owners: &[RustdocOwner]) -> io::Result<()> {
    let mut expected: Vec<&str> = owners
        .iter()
        .map(|owner| owner.crate_name.as_str())
        .collect();
    expected.sort_unstable();
    let mut actual: Vec<&str> = data
        .families
        .iter()
        .map(|family| family.crate_name.as_str())
        .collect();
    actual.sort_unstable();
    if actual != expected {
        return Err(io::Error::other(format!(
            "rustdoc input families do not match release/cargo-crates.json: expected {expected:?}, got {actual:?}"
        )));
    }
    Ok(())
}

fn validate_generated_artifacts(
    artifacts: &[FileHash],
    generated_sources: &[GeneratedSource],
    owners: &[RustdocOwner],
) -> io::Result<()> {
    let mut expected = BTreeMap::new();
    for source in generated_sources {
        let logical = source.logical_path.to_string_lossy().replace('\\', "/");
        if expected
            .insert(logical.clone(), source.sha256.as_str())
            .is_some()
        {
            return Err(io::Error::other(format!(
                "generated source logical path is duplicated: {logical}"
            )));
        }
    }
    for (logical, sha256) in &expected {
        let artifact = artifacts.iter().find(|artifact| &artifact.path == logical);
        let Some(artifact) = artifact else {
            return Err(io::Error::other(format!(
                "generated source artifact is missing: {logical}"
            )));
        };
        if artifact.sha256 != *sha256 {
            return Err(io::Error::other(format!(
                "generated source artifact changed: {logical}"
            )));
        }
    }
    for owner in owners {
        let prefix = format!("generated/{}/", owner.crate_name);
        for artifact in artifacts {
            if artifact.path.starts_with(&prefix) && !expected.contains_key(&artifact.path) {
                return Err(io::Error::other(format!(
                    "generated source artifact is not declared by current rustdoc: {}",
                    artifact.path
                )));
            }
        }
    }
    Ok(())
}

fn current_tool_hash() -> io::Result<String> {
    let path = canonical(&env::current_exe()?)?;
    Ok(hash_file(&path, "sdk-generation".into())?.sha256)
}

fn rustup_tool(tool: &str) -> io::Result<PathBuf> {
    let output = Command::new("rustup")
        .args(["which", "--toolchain", "1.98.1", tool])
        .output()
        .map_err(|error| {
            io::Error::other(format!("failed to resolve {tool} with rustup: {error}"))
        })?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "rustup could not resolve pinned {tool}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let path: PathBuf = String::from_utf8(output.stdout)
        .map_err(io::Error::other)?
        .trim()
        .into();
    if !path.is_file() {
        return Err(io::Error::other(format!(
            "rustup resolved {tool} to a non-file path: {}",
            path.display()
        )));
    }
    canonical(&path)
}

fn verify_tool_version(path: &Path, tool: &str) -> io::Result<()> {
    let output = Command::new(path)
        .arg("--version")
        .output()
        .map_err(|error| io::Error::other(format!("failed to inspect {tool}: {error}")))?;
    let version = String::from_utf8_lossy(&output.stdout);
    if !output.status.success()
        || !version
            .lines()
            .next()
            .is_some_and(|line| line.split_whitespace().take(2).eq([tool, "1.98.1"]))
    {
        return Err(io::Error::other(format!(
            "resolved {tool} is not version 1.98.1: {}",
            version.trim()
        )));
    }
    Ok(())
}

fn pinned_toolchain() -> io::Result<PinnedToolchain> {
    let cargo = rustup_tool("cargo")?;
    let rustc = rustup_tool("rustc")?;
    let rustdoc = rustup_tool("rustdoc")?;
    verify_tool_version(&cargo, "cargo")?;
    verify_tool_version(&rustc, "rustc")?;
    verify_tool_version(&rustdoc, "rustdoc")?;
    Ok(PinnedToolchain {
        cargo,
        rustc,
        rustdoc,
    })
}

fn pinned_toolchain_record(tools: &PinnedToolchain) -> io::Result<PinnedToolchainRecord> {
    fn identity(path: &Path, tool: &str) -> io::Result<PinnedToolIdentity> {
        let output = Command::new(path)
            .arg("--version")
            .output()
            .map_err(|error| io::Error::other(format!("failed to inspect {tool}: {error}")))?;
        let version = String::from_utf8(output.stdout)
            .map_err(io::Error::other)?
            .lines()
            .next()
            .filter(|line| !line.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| io::Error::other(format!("{tool} returned no version")))?;
        if !output.status.success() {
            return Err(io::Error::other(format!(
                "{tool} --version failed: {version}"
            )));
        }
        Ok(PinnedToolIdentity {
            version,
            sha256: hash_file(path, format!("tool:{tool}"))?.sha256,
        })
    }

    Ok(PinnedToolchainRecord {
        cargo: identity(&tools.cargo, "cargo")?,
        rustc: identity(&tools.rustc, "rustc")?,
        rustdoc: identity(&tools.rustdoc, "rustdoc")?,
    })
}

fn sanitize_compiler_environment(command: &mut Command, tools: &PinnedToolchain) {
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
                | "CARGO_TARGET_DIR"
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
    command
        .env("RUSTC", &tools.rustc)
        .env("RUSTDOC", &tools.rustdoc)
        .env("RUSTC_BOOTSTRAP", "1");
}

fn rustdoc_cache_base(config: &Config) -> io::Result<PathBuf> {
    let parent = if let Some(configured) = env::var_os("CARGO_TARGET_DIR") {
        let configured = PathBuf::from(configured);
        if configured.is_absolute() {
            configured
        } else {
            config.root.join(configured)
        }
    } else {
        config
            .output
            .parent()
            .ok_or_else(|| io::Error::other("output has no parent directory"))?
            .to_owned()
    };
    reject_reparse_ancestors(&parent, false)?;
    fs::create_dir_all(&parent)?;
    reject_reparse_ancestors(&parent, true)?;
    let parent = canonical(&parent)?;
    disjoint(&config.root, &parent)?;
    if parent == config.output || parent.starts_with(&config.output) {
        return Err(io::Error::other(
            "Rustdoc cache must be disjoint from generation output",
        ));
    }
    Ok(parent)
}

impl RustdocCacheLock {
    fn acquire(config: &Config) -> io::Result<Self> {
        let path = rustdoc_cache_base(config)?.join("sdk-generation-rustdoc.lock");
        reject_reparse_ancestors(&path, false)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        file.lock()?;
        Ok(Self { _file: file })
    }
}

fn rustdoc_staging_root(config: &Config) -> io::Result<PathBuf> {
    let root = canonical(&config.root)?;
    let parent = root
        .parent()
        .ok_or_else(|| io::Error::other("source checkout has no parent"))?;
    Ok(parent.join(".sdk-docs-generated"))
}

impl RustdocStagingLock {
    fn acquire(config: &Config) -> io::Result<Self> {
        let root = rustdoc_staging_root(config)?;
        reject_reparse_ancestors(&root, false)?;
        fs::create_dir_all(&root)?;
        reject_reparse_ancestors(&root, true)?;
        let root = canonical(&root)?;
        let path = root.join(".lock");
        reject_reparse_ancestors(&path, false)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        file.lock()?;
        Ok(Self { root, _file: file })
    }
}

fn rustdoc_target_named(config: &Config, name: &str) -> io::Result<PathBuf> {
    let parent = rustdoc_cache_base(config)?;
    let target = parent.join(name);
    reject_reparse_ancestors(&target, false)?;
    fs::create_dir_all(&target)?;
    reject_reparse_ancestors(&target, true)?;
    let target = canonical(&target)?;
    disjoint(&config.root, &target)?;
    if target == config.output || target.starts_with(&config.output) {
        return Err(io::Error::other(
            "rustdoc target must be disjoint from generation output",
        ));
    }
    Ok(target)
}

fn run_pinned_rustdoc(
    tools: &PinnedToolchain,
    manifest: &Path,
    workspace_root: &Path,
    package: &str,
    target_kind: &str,
    target_name: &str,
    target: &Path,
    rustdoc_args: &[&str],
) -> io::Result<()> {
    let mut cargo = Command::new(&tools.cargo);
    cargo
        .current_dir(workspace_root)
        .args(["rustdoc", "--locked", "--manifest-path"])
        .arg(manifest)
        .args(["--package", package]);
    if target_kind == "lib" {
        cargo.arg("--lib");
    } else {
        cargo.args(["--bin", target_name]);
    }
    cargo
        .args(["--target-dir"])
        .arg(target)
        .arg("--")
        .args(rustdoc_args);
    sanitize_compiler_environment(&mut cargo, tools);
    let status = cargo
        .status()
        .map_err(|error| io::Error::other(format!("failed to run pinned rustdoc: {error}")))?;
    if !status.success() {
        return Err(io::Error::other(format!(
            "pinned rustdoc exited with {status}"
        )));
    }
    Ok(())
}

fn clear_rustdoc_output(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || is_reparse_point(&metadata) {
                return Err(io::Error::other(format!(
                    "rustdoc output symlink or reparse point is not allowed: {}",
                    path.display()
                )));
            }
            fs::remove_file(path)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn generate_rustdoc(config: &Config, owners: &[RustdocOwner]) -> io::Result<RustdocInput> {
    let cache_lock = RustdocCacheLock::acquire(config)?;
    let staging_lock = RustdocStagingLock::acquire(config)?;
    let json_target = rustdoc_target_named(config, "sdk-generation-rustdoc-target")?;
    let dep_info_target = rustdoc_target_named(config, "sdk-generation-rustdoc-dep-info-target")?;
    let manifest = config.root.join("Cargo.toml");
    let tools = pinned_toolchain()?;
    let mut paths = Vec::with_capacity(owners.len());
    let mut markdown_dependencies = Vec::new();
    let mut generated_sources = Vec::new();
    for owner in owners {
        let json = json_target
            .join("doc")
            .join(format!("{}.json", owner.crate_name));
        let dep_json = dep_info_target
            .join("doc")
            .join(format!("{}.json", owner.crate_name));
        let dep_info = dep_info_target
            .join("doc")
            .join(format!("{}.d", owner.crate_name));
        clear_rustdoc_output(&json)?;
        clear_rustdoc_output(&dep_json)?;
        clear_rustdoc_output(&dep_info)?;
        run_pinned_rustdoc(
            &tools,
            &manifest,
            &config.root,
            &owner.package,
            &owner.target_kind,
            &owner.target_name,
            &json_target,
            &["-Z", "unstable-options", "--output-format", "json"],
        )?;
        if !json.is_file() {
            return Err(io::Error::other(format!(
                "pinned rustdoc did not produce {}",
                json.display()
            )));
        }
        run_pinned_rustdoc(
            &tools,
            &manifest,
            &config.root,
            &owner.package,
            &owner.target_kind,
            &owner.target_name,
            &dep_info_target,
            &["-Z", "unstable-options", "--emit", "dep-info"],
        )?;
        if dep_json.exists() {
            return Err(io::Error::other(
                "pinned dep-info rustdoc unexpectedly produced JSON",
            ));
        }
        if !dep_info.is_file() {
            return Err(io::Error::other(format!(
                "pinned rustdoc did not produce {}",
                dep_info.display()
            )));
        }
        let parsed_dep_info = read_dep_info(&dep_info)?;
        let owner_generated_sources = generated_sources_for_owner(
            config,
            &parsed_dep_info,
            &dep_info_target,
            &json_target,
            owner,
        )?;
        let (normalized_json, normalized_sources) = normalize_rustdoc_json(
            &json,
            &owner_generated_sources,
            &staging_lock.root,
            &json_target,
        )?;
        paths.push(normalized_json);
        generated_sources.extend(normalized_sources);
        markdown_dependencies.extend(rustdoc_markdown_dependencies(
            &dep_info,
            &config.root,
            &config.root,
        )?);
    }
    markdown_dependencies.sort();
    markdown_dependencies.dedup();
    let toolchain = Some(pinned_toolchain_record(&tools)?);
    Ok(RustdocInput {
        paths,
        markdown_dependencies,
        generated_sources,
        _cache_lock: Some(cache_lock),
        _staging_lock: Some(staging_lock),
        toolchain,
    })
}

fn resolve_rustdoc(config: &Config, owners: &[RustdocOwner]) -> io::Result<RustdocInput> {
    match (&config.channel[..], &config.rustdoc_json) {
        ("release", Some(_)) => Err(io::Error::other(
            "release generation owns rustdoc input; omit --rustdoc-json",
        )),
        ("release", None) => generate_rustdoc(config, owners),
        (_, Some(path)) => {
            let (_, paths) = collect_rustdoc(path)?;
            Ok(RustdocInput {
                paths,
                markdown_dependencies: Vec::new(),
                generated_sources: Vec::new(),
                _cache_lock: None,
                _staging_lock: None,
                toolchain: None,
            })
        }
        (_, None) => Err(io::Error::other(
            "preview generation requires --rustdoc-json",
        )),
    }
}

fn profile_availability(
    root: &Path,
    data: &DocsData,
    rustdoc_paths: &[PathBuf],
) -> io::Result<ProfileAvailability> {
    let metadata = load_metadata(root.join("Cargo.toml")).map_err(io::Error::other)?;
    let mut profiles = BTreeMap::<ProfileId, ProfileSpec>::new();
    let mut items = Vec::<OwnedApiItem>::new();

    for path in rustdoc_paths {
        let observation = observe_rustdoc(path).map_err(io::Error::other)?;
        let package = metadata
            .packages
            .iter()
            .find(|package| {
                package.targets.iter().any(|target| {
                    target.name.replace('-', "_") == observation.crate_name
                })
            })
            .ok_or_else(|| {
                io::Error::other(format!(
                    "Rustdoc crate `{}` has no matching Cargo target",
                    observation.crate_name
                ))
            })?;
        let package_name = package.name.to_string();
        let owner = api_owner_for_package(&metadata, &package_name).map_err(io::Error::other)?;
        let profile = ProfileSpec {
            package: package_name,
            target: observation.target,
            default_features: true,
            features: BTreeSet::new(),
        };
        let profile_id = profile.id();
        let receipt_items = extract_owned_api_for_crate(
            path,
            &owner,
            profile_id.clone(),
            &observation.crate_name,
        )
        .map_err(io::Error::other)?;
        profiles.insert(profile_id, profile);
        items.extend(receipt_items);
    }

    project_into_docs(data, &metadata, items, &profiles).map_err(io::Error::other)
}

fn write_immutable_json(path: &Path, value: &impl Serialize) -> io::Result<()> {
    if path.exists() {
        return Err(io::Error::other(format!(
            "immutable output already exists: {}",
            path.display()
        )));
    }
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    let mut bytes = serde_json::to_vec_pretty(value).map_err(io::Error::other)?;
    bytes.push(b'\n');
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(temporary, path)
}

fn write_manifest(path: &Path, value: &impl Serialize) -> io::Result<()> {
    write_immutable_json(path, value)
}

fn collect_outputs(root: &Path) -> io::Result<Vec<FileHash>> {
    let root_metadata = fs::symlink_metadata(root)?;
    if root_metadata.file_type().is_symlink() || is_reparse_point(&root_metadata) {
        return Err(io::Error::other(
            "output root symlink or reparse point is not allowed",
        ));
    }
    let root = canonical(root)?;
    let mut files = BTreeMap::new();
    collect_dir(&root, &root, &mut files)?;
    files.remove(MANIFEST);
    Ok(files.into_values().collect())
}

fn disjoint(root: &Path, output: &Path) -> io::Result<()> {
    let root = canonical(root)?;
    let output = canonical(output)?;
    if root == output || root.starts_with(&output) || output.starts_with(&root) {
        return Err(io::Error::other("checkout and output must be disjoint"));
    }
    Ok(())
}

fn generate(config: &Config) -> io::Result<()> {
    fs::create_dir_all(&config.output)?;
    disjoint(&config.root, &config.output)?;
    if fs::read_dir(&config.output)?.next().transpose()?.is_some() {
        return Err(io::Error::other("generation output must be new or empty"));
    }
    let revision = git_revision(&config.root)?;
    require_clean_release(&config.root, &config.channel)?;
    verify_compiled_generator_source(&config.root)?;
    let metadata = cargo_metadata(&config.root)?;
    let owners = load_rustdoc_owners(&config.root, &metadata)?;
    validate_owner_versions(config, &owners)?;
    let owner_roots = owner_package_roots(&owners);
    let source_extras_before = baseline_source_extras(&config.root)?;
    let source_before_stage = collect_sources(&config.root, &source_extras_before, &owner_roots)?;
    generate_actors_contract_artifacts(config)?;
    generate_workers_contract_artifacts(config)?;
    generate_actors_typescript_artifacts(config)?;
    let rustdoc_input = resolve_rustdoc(config, &owners)?;
    let source_extras_after = baseline_source_extras(&config.root)?;
    if collect_sources(&config.root, &source_extras_after, &owner_roots)? != source_before_stage {
        return Err(io::Error::other(
            "source changed during pinned rustdoc generation",
        ));
    }
    let mut markdown_dependencies = source_extras_after;
    markdown_dependencies.extend(rustdoc_input.markdown_dependencies.iter().cloned());
    let source = collect_sources(&config.root, &markdown_dependencies, &owner_roots)?;
    let rustdoc = collect_rustdoc_files(&rustdoc_input.paths)?;
    let rustdoc_paths = rustdoc_input.paths.clone();
    let tool_sha256 = current_tool_hash()?;
    let source_sha256 = tree_digest(&source);
    let input = BuildInput {
        version: config.version.clone(),
        channel: docs_channel(&config.channel),
        revision: revision.clone(),
        source_state: if config.channel == "release" {
            "captured-snapshot".into()
        } else {
            "working-tree".into()
        },
        source_sha256: Some(source_sha256.clone()),
        repository_root: config.root.clone(),
        rustdoc_files: rustdoc_paths,
        generated_sources: rustdoc_input.generated_sources.clone(),
        mark_latest: false,
    };
    let data = build_data(&input).map_err(io::Error::other)?;
    validate_rustdoc_data(&data, &owners)?;
    if current_tool_hash()? != tool_sha256 {
        return Err(io::Error::other(
            "generation tool changed during generation",
        ));
    }
    if git_revision(&config.root)? != revision {
        return Err(io::Error::other(
            "source revision changed during documentation generation",
        ));
    }
    require_clean_release(&config.root, &config.channel)?;
    let mut markdown_after = baseline_source_extras(&config.root)?;
    markdown_after.extend(rustdoc_input.markdown_dependencies.iter().cloned());
    let source_after = collect_sources(&config.root, &markdown_after, &owner_roots)?;
    let rustdoc_after = collect_rustdoc_files(&rustdoc_input.paths)?;
    if source_after != source {
        return Err(io::Error::other(
            "source changed during documentation generation",
        ));
    }
    if rustdoc_after != rustdoc {
        return Err(io::Error::other(
            "rustdoc input changed during documentation generation",
        ));
    }
    let availability = profile_availability(&config.root, &data, &rustdoc_input.paths)?;
    write_immutable_json(
        &config.output.join(PROFILE_AVAILABILITY),
        &availability,
    )?;
    write_bundle(&data, &config.output, config.channel == "release").map_err(io::Error::other)?;
    write_native_targets(config, &metadata, &revision, &source_sha256)?;
    let artifacts = collect_outputs(&config.output)?;
    let manifest = Manifest {
        schema: "acyclic.sdk.generation.v1".into(),
        generator_version: VERSION.into(),
        version: config.version.clone(),
        family: ACTORS_CRATE.into(),
        families: owners
            .iter()
            .map(|owner| owner.crate_name.clone())
            .collect(),
        revision,
        source_sha256: tree_digest(&source),
        source,
        rustdoc_sha256: tree_digest(&rustdoc),
        rustdoc,
        tool: ToolRecord {
            id: "sdk-docs-library".into(),
            version: GENERATOR_VERSION.into(),
            channel: config.channel.clone(),
            tool_sha256,
            pinned_toolchain: rustdoc_input.toolchain.clone(),
        },
        artifacts_sha256: tree_digest(&artifacts),
        artifacts,
    };
    write_manifest(&config.output.join(MANIFEST), &manifest)
}

fn drift(config: &Config) -> io::Result<()> {
    verify_compiled_generator_source(&config.root)?;
    let metadata = cargo_metadata(&config.root)?;
    let owners = load_rustdoc_owners(&config.root, &metadata)?;
    validate_owner_versions(config, &owners)?;
    let owner_roots = owner_package_roots(&owners);
    let manifest: Manifest = serde_json::from_slice(&fs::read(config.output.join(MANIFEST))?)
        .map_err(io::Error::other)?;
    let revision = git_revision(&config.root)?;
    if manifest.schema != "acyclic.sdk.generation.v1"
        || manifest.generator_version != VERSION
        || manifest.version != config.version
        || manifest.family != ACTORS_CRATE
        || manifest.families
            != owners
                .iter()
                .map(|owner| owner.crate_name.clone())
                .collect::<Vec<_>>()
        || manifest.revision != revision
    {
        return Err(io::Error::other(
            "generation identity differs from manifest",
        ));
    }
    let rustdoc_input = resolve_rustdoc(config, &owners)?;
    let mut source_extras = baseline_source_extras(&config.root)?;
    source_extras.extend(rustdoc_input.markdown_dependencies.iter().cloned());
    let source = collect_sources(&config.root, &source_extras, &owner_roots)?;
    if manifest.source != source || manifest.source_sha256 != tree_digest(&source) {
        return Err(io::Error::other("source drift detected"));
    }
    validate_native_targets(config, &metadata, &revision, &manifest.source_sha256)?;
    let rustdoc = collect_rustdoc_files(&rustdoc_input.paths)?;
    if manifest.rustdoc != rustdoc || manifest.rustdoc_sha256 != tree_digest(&rustdoc) {
        return Err(io::Error::other("rustdoc input drift detected"));
    }
    if manifest.tool.id != "sdk-docs-library"
        || manifest.tool.version != GENERATOR_VERSION
        || manifest.tool.channel != config.channel
        || manifest.tool.pinned_toolchain != rustdoc_input.toolchain
        || current_tool_hash()? != manifest.tool.tool_sha256
    {
        return Err(io::Error::other("fixed documentation stage drift detected"));
    }
    let artifacts = collect_outputs(&config.output)?;
    validate_generated_artifacts(&artifacts, &rustdoc_input.generated_sources, &owners)?;
    if manifest.artifacts != artifacts || manifest.artifacts_sha256 != tree_digest(&artifacts) {
        return Err(io::Error::other("generated output drift detected"));
    }
    println!("{{\"schema\":\"acyclic.sdk.generation-drift.v1\",\"status\":\"passed\"}}");
    Ok(())
}

fn value(args: &[String], name: &str) -> io::Result<String> {
    let index = args
        .iter()
        .position(|arg| arg == name)
        .ok_or_else(|| io::Error::other(format!("missing {name}")))?;
    args.get(index + 1)
        .filter(|value| !value.starts_with("--"))
        .cloned()
        .ok_or_else(|| io::Error::other(format!("missing value after {name}")))
}

fn parse(args: &[String]) -> io::Result<Config> {
    let operation = args
        .first()
        .cloned()
        .ok_or_else(|| io::Error::other("operation is required"))?;
    let known = [
        "--root",
        "--rustdoc-json",
        "--output",
        "--version",
        "--channel",
    ];
    let mut index = 1;
    let mut seen = BTreeSet::new();
    while index < args.len() {
        if !known.contains(&args[index].as_str()) {
            return Err(io::Error::other(format!(
                "unknown argument {}",
                args[index]
            )));
        }
        if !seen.insert(args[index].clone()) {
            return Err(io::Error::other(format!(
                "duplicate argument {}",
                args[index]
            )));
        }
        index += 2;
    }
    let channel = value(args, "--channel")?;
    if channel != "release" && channel != "preview" {
        return Err(io::Error::other("channel must be release or preview"));
    }
    Ok(Config {
        operation,
        root: value(args, "--root")?.into(),
        rustdoc_json: args
            .iter()
            .position(|arg| arg == "--rustdoc-json")
            .map(|_| value(args, "--rustdoc-json"))
            .transpose()?
            .map(Into::into),
        output: value(args, "--output")?.into(),
        version: value(args, "--version")?,
        channel,
    })
}

fn normalize(mut config: Config) -> io::Result<Config> {
    reject_reparse_ancestors(&config.root, true)?;
    config.root = canonical(&config.root)?;
    if let Some(rustdoc_json) = &config.rustdoc_json {
        reject_reparse_ancestors(rustdoc_json, true)?;
        config.rustdoc_json = Some(canonical(rustdoc_json)?);
    }
    reject_reparse_ancestors(&config.output, false)?;
    fs::create_dir_all(&config.output)?;
    reject_reparse_ancestors(&config.output, true)?;
    config.output = canonical(&config.output)?;
    Ok(config)
}

fn main() -> io::Result<()> {
    let config = normalize(parse(&env::args().skip(1).collect::<Vec<_>>())?)?;
    let result = match config.operation.as_str() {
        "generate" => generate(&config),
        "drift" => drift(&config),
        _ => Err(io::Error::other(
            "usage: sdk-generation <generate|drift> ...",
        )),
    };
    if let Err(error) = result {
        eprintln!("sdk-generation: {error}");
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dep_info_continuations_are_joined_before_parsing() {
        let crlf = "target: src/lib.rs \\\r\n docs/objects-v2-http.md\n";
        let lf = "target: src/lib.rs \\\n docs/objects-v2-http.md\n";
        let continuation_without_spacing = "target: src/lib.rs\\\ndocs/objects-v2-http.md\n";
        let expected = vec![
            PathBuf::from("src/lib.rs"),
            PathBuf::from("docs/objects-v2-http.md"),
        ];

        assert_eq!(parse_dep_info_contents(crlf).unwrap().files, expected);
        assert_eq!(parse_dep_info_contents(lf).unwrap().files, expected);
        assert_eq!(
            parse_dep_info_contents(continuation_without_spacing)
                .unwrap()
                .files,
            expected
        );
    }

    #[test]
    fn dep_info_without_files_is_rejected() {
        let error = parse_dep_info_contents("target: \n").unwrap_err();
        assert_eq!(error.to_string(), "rustdoc dep-info contains no files");
    }

    #[test]
    fn make_escaped_markdown_path_is_rejected_with_actionable_diagnostic() {
        let error = reject_unsupported_make_escapes(Path::new(r"docs\guide\#name.md")).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unsupported Make-escaped Markdown path")
        );
        assert!(error.to_string().contains("#name.md"));
        assert!(error.to_string().contains("rename the path"));
    }

    #[test]
    fn relative_dep_info_paths_resolve_from_the_workspace_root() {
        let root = Path::new("workspace");
        assert_eq!(
            resolve_dep_info_path(PathBuf::from("rust/crates/actors/README.md"), root,),
            root.join("rust/crates/actors/README.md")
        );
    }

    #[test]
    fn generated_artifact_hash_must_match_current_rustdoc_source() {
        let owners = vec![RustdocOwner {
            package: "acyclic-actors".into(),
            version: "0.1.0".into(),
            crate_name: "acyclic_actors".into(),
            target_kind: "lib".into(),
            target_name: "acyclic_actors".into(),
            package_root: PathBuf::from("rust/crates/actors"),
        }];
        let generated = GeneratedSource {
            physical_path: PathBuf::from("json-target/rust/generated.rs"),
            logical_path: PathBuf::from("generated/acyclic_actors/rust/generated.rs"),
            sha256: "sha256:current".into(),
        };
        let unchanged = vec![FileHash {
            path: "generated/acyclic_actors/rust/generated.rs".into(),
            sha256: "sha256:current".into(),
            bytes: 7,
        }];
        assert!(validate_generated_artifacts(&unchanged, &[generated.clone()], &owners).is_ok());

        let changed = vec![FileHash {
            sha256: "sha256:stale".into(),
            ..unchanged[0].clone()
        }];
        let error = validate_generated_artifacts(&changed, &[generated], &owners).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("generated source artifact changed")
        );
    }

    #[test]
    fn generated_rustdoc_paths_use_stable_logical_references() {
        let suffix = std::process::id();
        let root = env::temp_dir().join(format!("sdk-generation-rustdoc-root-{suffix}"));
        let target = env::temp_dir().join(format!("sdk-generation-rustdoc-target-{suffix}"));
        let json = target.join("doc/acyclic_workers.json");
        let source = target.join("debug/build/acyclic-workers-random/out/rust/wire.rs");
        let _ = fs::remove_dir_all(&root);
        let _ = fs::remove_dir_all(&target);
        fs::create_dir_all(root.join("src")).unwrap();
        fs::create_dir_all(source.parent().unwrap()).unwrap();
        fs::create_dir_all(json.parent().unwrap()).unwrap();
        fs::write(&source, b"pub struct Wire;\n").unwrap();
        let source_string = source.to_string_lossy().into_owned();
        let mixed_path = source_string.replace('/', "\\");
        let raw_filename = format!(r"\\?\{}", source.display());
        fs::write(
            &json,
            serde_json::json!({
                "span": { "filename": raw_filename },
                "same": { "filename": source_string.clone() },
                "other": format!("authored text {}", format!(r"\?\{}", source.display())),
                "attrs": [
                    { "other": format!(
                        "#[attr = CfgTrace(span: {}:1:2 (#0), {}:3:4 (#0))]",
                        format!(r"\?\{}", source.display()),
                        format!(r"\?\{}", source.display())
                    ) },
                    { "other": format!("#[attr = CfgTrace(span: {}:5:6 (#0))]", mixed_path) },
                    { "other": format!("#[attr = CfgTrace(span: {}.bak:7:8 (#0))]", source.display()) },
                    { "other": format!("user supplied {}", source.display()) },
                ],
            })
            .to_string(),
        )
        .unwrap();
        let config = Config {
            operation: "drift".into(),
            root: root.clone(),
            rustdoc_json: None,
            output: root.join("bundle"),
            version: "0.2.0".into(),
            channel: "release".into(),
        };
        let logical_path = PathBuf::from("generated/acyclic_workers/rust/wire.rs");
        let source_hash = hash_file(&source, "wire.rs".into()).unwrap().sha256;
        let generated = GeneratedSource {
            physical_path: source.clone(),
            logical_path: logical_path.clone(),
            sha256: source_hash,
        };
        let staging_root = rustdoc_staging_root(&config).unwrap();
        fs::create_dir_all(&staging_root).unwrap();
        let (normalized, staged) =
            normalize_rustdoc_json(&json, &[generated], &staging_root, &target).unwrap();
        let value: serde_json::Value =
            serde_json::from_slice(&fs::read(normalized).unwrap()).unwrap();
        assert_eq!(
            value["span"]["filename"],
            "../.sdk-docs-generated/generated/acyclic_workers/rust/wire.rs"
        );
        assert_eq!(value["same"]["filename"], source_string);
        assert_eq!(
            value["other"],
            format!("authored text {}", format!(r"\?\{}", source.display()))
        );
        assert_eq!(
            value["attrs"][0]["other"],
            "#[attr = CfgTrace(span: ../.sdk-docs-generated/generated/acyclic_workers/rust/wire.rs:1:2 (#0), ../.sdk-docs-generated/generated/acyclic_workers/rust/wire.rs:3:4 (#0))]"
        );
        assert_eq!(
            value["attrs"][1]["other"],
            "#[attr = CfgTrace(span: ../.sdk-docs-generated/generated/acyclic_workers/rust/wire.rs:5:6 (#0))]"
        );
        assert_eq!(
            value["attrs"][2]["other"],
            format!(
                "#[attr = CfgTrace(span: {}.bak:7:8 (#0))]",
                source.display()
            )
        );
        assert_eq!(
            value["attrs"][3]["other"],
            format!("user supplied {}", source.display())
        );
        assert!(staged[0].physical_path.is_file());
        assert_eq!(staged[0].logical_path, logical_path);
        let _ = fs::remove_dir_all(&root);
        let _ = fs::remove_dir_all(&target);
    }

    #[test]
    fn path_normalization_is_alias_stable_but_preserves_generated_byte_digests() {
        let suffix = std::process::id();
        let root = env::temp_dir().join(format!("sdk-generation-rustdoc-alias-root-{suffix}"));
        let target = env::temp_dir().join(format!("sdk-generation-rustdoc-alias-target-{suffix}"));
        let json_a = target.join("doc/a.json");
        let json_b = target.join("doc/b.json");
        let source = target.join("debug/build/acyclic-workers-alias/out/rust/wire.rs");
        let _ = fs::remove_dir_all(&root);
        let _ = fs::remove_dir_all(&target);
        fs::create_dir_all(root.join("src")).unwrap();
        fs::create_dir_all(source.parent().unwrap()).unwrap();
        fs::create_dir_all(json_a.parent().unwrap()).unwrap();
        fs::write(&source, b"pub struct Wire;\n").unwrap();
        fs::write(
            &json_a,
            serde_json::json!({ "span": { "filename": source.to_string_lossy() } }).to_string(),
        )
        .unwrap();
        fs::write(
            &json_b,
            serde_json::json!({
                "span": { "filename": format!(r"\\?\{}", source.display()) }
            })
            .to_string(),
        )
        .unwrap();
        let config = Config {
            operation: "drift".into(),
            root: root.clone(),
            rustdoc_json: None,
            output: root.join("bundle"),
            version: "0.2.0".into(),
            channel: "release".into(),
        };
        let logical_path = PathBuf::from("generated/acyclic_workers/rust/wire.rs");
        let bytes_a = b"pub struct Wire;\n";
        let hash_a = digest(bytes_a);
        let source_a = GeneratedSource {
            physical_path: source.clone(),
            logical_path: logical_path.clone(),
            sha256: hash_a.clone(),
        };
        let staging_root = rustdoc_staging_root(&config).unwrap();
        fs::create_dir_all(&staging_root).unwrap();
        let (normalized_a, staged_a) =
            normalize_rustdoc_json(&json_a, &[source_a], &staging_root, &target).unwrap();
        let (normalized_b, staged_b) = normalize_rustdoc_json(
            &json_b,
            &[GeneratedSource {
                physical_path: source.clone(),
                logical_path: logical_path.clone(),
                sha256: hash_a,
            }],
            &staging_root,
            &target,
        )
        .unwrap();
        assert_eq!(
            fs::read(normalized_a).unwrap(),
            fs::read(normalized_b).unwrap(),
            "OUT_DIR spelling aliases must produce the same normalized snapshot"
        );
        assert_eq!(staged_a[0].sha256, staged_b[0].sha256);
        let duplicate = normalize_rustdoc_json(
            &json_a,
            &[
                GeneratedSource {
                    physical_path: source.clone(),
                    logical_path: PathBuf::from("generated/acyclic_workers/rust/one.rs"),
                    sha256: staged_a[0].sha256.clone(),
                },
                GeneratedSource {
                    physical_path: source.clone(),
                    logical_path: PathBuf::from("generated/acyclic_workers/rust/two.rs"),
                    sha256: staged_a[0].sha256.clone(),
                },
            ],
            &staging_root,
            &target,
        )
        .unwrap_err();
        assert!(
            duplicate
                .to_string()
                .contains("physical path is declared more than once")
        );

        let bytes_b = b"pub struct WireChanged;\n";
        fs::write(&source, bytes_b).unwrap();
        let staged_changed = normalize_rustdoc_json(
            &json_a,
            &[GeneratedSource {
                physical_path: source,
                logical_path,
                sha256: digest(bytes_b),
            }],
            &staging_root,
            &target,
        )
        .unwrap()
        .1;
        assert_ne!(
            staged_a[0].sha256, staged_changed[0].sha256,
            "normalization must not erase generated source byte changes"
        );
        let _ = fs::remove_dir_all(&root);
        let _ = fs::remove_dir_all(&target);
    }
}
