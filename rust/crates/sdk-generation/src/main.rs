use sdk_docs::{BuildInput, Channel, DocsData, build_data, write_bundle};
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

const MANIFEST: &str = "generation-manifest.json";
const VERSION: &str = env!("CARGO_PKG_VERSION");
const ACTORS_CRATE: &str = "acyclic_actors";
const ACTORS_PACKAGE: &str = "acyclic-actors";
const SOURCE_PATHS: &[&str] = &[
    "Cargo.toml",
    "docs/objects-v2-http.md",
    "docs/rust-source-generation.md",
    "Cargo.lock",
    "rust/crates/actors/Cargo.toml",
    "rust/crates/actors/README.md",
    "rust/crates/actors/examples",
    "rust/crates/actors/src",
    "rust/crates/sdk-docs/Cargo.toml",
    "rust/crates/sdk-docs/Cargo.lock",
    "rust/crates/sdk-docs/src",
    "rust/crates/sdk-generation/Cargo.toml",
    "rust/crates/sdk-generation/Cargo.lock",
    "rust/crates/sdk-generation/README.md",
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
}

#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    schema: String,
    generator_version: String,
    family: String,
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
    path: PathBuf,
    markdown_dependencies: Vec<PathBuf>,
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

fn collect_sources(root: &Path, markdown_dependencies: &[PathBuf]) -> io::Result<Vec<FileHash>> {
    let root_metadata = fs::symlink_metadata(root)?;
    if root_metadata.file_type().is_symlink() || is_reparse_point(&root_metadata) {
        return Err(io::Error::other(
            "checkout root symlink or reparse point is not allowed",
        ));
    }
    let root = canonical(root)?;
    let mut files = BTreeMap::new();
    for declaration in SOURCE_PATHS {
        let declaration_metadata = fs::symlink_metadata(root.join(declaration))?;
        if declaration_metadata.file_type().is_symlink() || is_reparse_point(&declaration_metadata)
        {
            return Err(io::Error::other(format!(
                "source symlink is not allowed: {declaration}"
            )));
        }
        let path = canonical(&root.join(declaration))?;
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
                "declared source does not exist: {declaration}"
            )));
        }
    }
    for dependency in markdown_dependencies {
        let metadata = fs::symlink_metadata(dependency)?;
        if metadata.file_type().is_symlink() || is_reparse_point(&metadata) {
            return Err(io::Error::other(format!(
                "source symlink is not allowed: {}",
                dependency.display()
            )));
        }
        let path = canonical(dependency)?;
        if !path.starts_with(&root) || path.extension().is_none_or(|extension| extension != "md") {
            return Err(io::Error::other(format!(
                "rustdoc dependency is outside the Markdown source closure: {}",
                dependency.display()
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

fn parse_dep_info(contents: &str) -> io::Result<Vec<PathBuf>> {
    let mut logical = String::new();
    for line in contents.lines() {
        if let Some(prefix) = line.strip_suffix('\\') {
            logical.push_str(prefix);
            logical.push(' ');
        } else {
            logical.push_str(line);
            logical.push(' ');
        }
    }
    let separator = logical
        .char_indices()
        .find(|(index, character)| {
            *character == ':'
                && logical[*index + character.len_utf8()..]
                    .chars()
                    .next()
                    .is_some_and(char::is_whitespace)
        })
        .map(|(index, _)| index)
        .ok_or_else(|| io::Error::other("rustdoc dep-info has no target separator"))?;
    let mut dependencies = Vec::new();
    let mut token = String::new();
    let mut escaped = false;
    let mut characters = logical[separator + 1..].chars().peekable();
    while let Some(character) = characters.next() {
        if escaped {
            token.push(character);
            escaped = false;
        } else if character == '\\' {
            if characters
                .peek()
                .is_some_and(|next| next.is_whitespace() || *next == '\\')
            {
                escaped = true;
            } else {
                token.push(character);
            }
        } else if character.is_whitespace() {
            if !token.is_empty() {
                dependencies.push(PathBuf::from(std::mem::take(&mut token)));
            }
        } else {
            token.push(character);
        }
    }
    if escaped {
        token.push('\\');
    }
    if !token.is_empty() {
        dependencies.push(PathBuf::from(token));
    }
    if dependencies.is_empty() {
        return Err(io::Error::other("rustdoc dep-info has no dependencies"));
    }
    Ok(dependencies)
}

fn rustdoc_markdown_dependencies(
    dep_info: &Path,
    checkout: &Path,
    dependency_root: &Path,
) -> io::Result<Vec<PathBuf>> {
    let checkout = canonical(checkout)?;
    let dependency_root = canonical(dependency_root)?;
    let contents = String::from_utf8(fs::read(dep_info)?).map_err(io::Error::other)?;
    let mut paths = Vec::new();
    for dependency in parse_dep_info(&contents)? {
        let path = if dependency.is_absolute() {
            dependency
        } else {
            dependency_root.join(dependency)
        };
        if path.extension().is_none_or(|extension| extension != "md") {
            continue;
        }
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
        paths.push(path);
    }
    paths.sort();
    paths.dedup();
    Ok(paths)
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

fn validate_actors_data(data: &DocsData) -> io::Result<()> {
    if data.families.len() != 1 || data.families[0].crate_name != ACTORS_CRATE {
        return Err(io::Error::other(format!(
            "rustdoc input must contain exactly the {ACTORS_CRATE} family"
        )));
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
            || uppercase.starts_with("CARGO_BUILD_")
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

fn rustdoc_target(config: &Config) -> io::Result<PathBuf> {
    let parent = config
        .output
        .parent()
        .ok_or_else(|| io::Error::other("output has no parent directory"))?;
    let target = parent.join("sdk-generation-rustdoc-target");
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

fn generate_rustdoc(config: &Config) -> io::Result<RustdocInput> {
    let target = rustdoc_target(config)?;
    let manifest = config.root.join("Cargo.toml");
    let tools = pinned_toolchain()?;
    let actors_root = config.root.join("rust/crates/actors");
    let mut cargo = Command::new(&tools.cargo);
    cargo
        .current_dir(&actors_root)
        .args(["rustdoc", "--locked", "--manifest-path"])
        .arg(&manifest)
        .args(["--package", ACTORS_PACKAGE, "--lib", "--target-dir"])
        .arg(&target)
        .args([
            "--",
            "-Z",
            "unstable-options",
            "--output-format",
            "json",
            "--emit",
            "dep-info",
        ]);
    sanitize_compiler_environment(&mut cargo, &tools);
    let status = cargo
        .status()
        .map_err(|error| io::Error::other(format!("failed to run pinned rustdoc: {error}")))?;
    if !status.success() {
        return Err(io::Error::other(format!(
            "pinned rustdoc exited with {status}"
        )));
    }
    let json = target.join("doc").join(format!("{ACTORS_CRATE}.json"));
    if !json.is_file() {
        return Err(io::Error::other(format!(
            "pinned rustdoc did not produce {}",
            json.display()
        )));
    }
    let json = canonical(&json)?;
    let dep_info = target.join("doc").join(format!("{ACTORS_CRATE}.d"));
    if !dep_info.is_file() {
        return Err(io::Error::other(format!(
            "pinned rustdoc did not produce {}",
            dep_info.display()
        )));
    }
    let markdown_dependencies =
        rustdoc_markdown_dependencies(&dep_info, &config.root, &actors_root)?;
    Ok(RustdocInput {
        path: json,
        markdown_dependencies,
    })
}

fn resolve_rustdoc(config: &Config) -> io::Result<RustdocInput> {
    match (&config.channel[..], &config.rustdoc_json) {
        ("release", Some(_)) => Err(io::Error::other(
            "release generation owns rustdoc input; omit --rustdoc-json",
        )),
        ("release", None) => generate_rustdoc(config),
        (_, Some(path)) => Ok(RustdocInput {
            path: path.clone(),
            markdown_dependencies: Vec::new(),
        }),
        (_, None) => Err(io::Error::other(
            "preview generation requires --rustdoc-json",
        )),
    }
}

fn write_manifest(path: &Path, value: &impl Serialize) -> io::Result<()> {
    if path.exists() {
        return Err(io::Error::other("generation manifest already exists"));
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
    let tracked_markdown_before = tracked_markdown_paths(&config.root)?;
    let source_before_stage = collect_sources(&config.root, &tracked_markdown_before)?;
    let rustdoc_input = resolve_rustdoc(config)?;
    let tracked_markdown_after = tracked_markdown_paths(&config.root)?;
    if collect_sources(&config.root, &tracked_markdown_after)? != source_before_stage {
        return Err(io::Error::other(
            "source changed during pinned rustdoc generation",
        ));
    }
    let mut markdown_dependencies = tracked_markdown_after;
    markdown_dependencies.extend(rustdoc_input.markdown_dependencies.iter().cloned());
    let source = collect_sources(&config.root, &markdown_dependencies)?;
    let (rustdoc, rustdoc_paths) = collect_rustdoc(&rustdoc_input.path)?;
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
        source_sha256: Some(source_sha256),
        repository_root: config.root.clone(),
        rustdoc_files: rustdoc_paths,
        mark_latest: false,
    };
    let data = build_data(&input).map_err(io::Error::other)?;
    validate_actors_data(&data)?;
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
    let mut markdown_after = tracked_markdown_paths(&config.root)?;
    markdown_after.extend(rustdoc_input.markdown_dependencies.iter().cloned());
    let source_after = collect_sources(&config.root, &markdown_after)?;
    let (rustdoc_after, _) = collect_rustdoc(&rustdoc_input.path)?;
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
    write_bundle(&data, &config.output, config.channel == "release").map_err(io::Error::other)?;
    let artifacts = collect_outputs(&config.output)?;
    let manifest = Manifest {
        schema: "acyclic.sdk.generation.v1".into(),
        generator_version: VERSION.into(),
        family: ACTORS_CRATE.into(),
        revision,
        source_sha256: tree_digest(&source),
        source,
        rustdoc_sha256: tree_digest(&rustdoc),
        rustdoc,
        tool: ToolRecord {
            id: "sdk-docs-library".into(),
            version: config.version.clone(),
            channel: config.channel.clone(),
            tool_sha256,
        },
        artifacts_sha256: tree_digest(&artifacts),
        artifacts,
    };
    write_manifest(&config.output.join(MANIFEST), &manifest)
}

fn drift(config: &Config) -> io::Result<()> {
    let manifest: Manifest = serde_json::from_slice(&fs::read(config.output.join(MANIFEST))?)
        .map_err(io::Error::other)?;
    let revision = git_revision(&config.root)?;
    if manifest.schema != "acyclic.sdk.generation.v1"
        || manifest.generator_version != VERSION
        || manifest.family != ACTORS_CRATE
        || manifest.revision != revision
    {
        return Err(io::Error::other(
            "generation identity differs from manifest",
        ));
    }
    let rustdoc_input = resolve_rustdoc(config)?;
    let source = collect_sources(&config.root, &rustdoc_input.markdown_dependencies)?;
    if manifest.source != source || manifest.source_sha256 != tree_digest(&source) {
        return Err(io::Error::other("source drift detected"));
    }
    let (rustdoc, _) = collect_rustdoc(&rustdoc_input.path)?;
    if manifest.rustdoc != rustdoc || manifest.rustdoc_sha256 != tree_digest(&rustdoc) {
        return Err(io::Error::other("rustdoc input drift detected"));
    }
    if manifest.tool.id != "sdk-docs-library"
        || manifest.tool.version != config.version
        || manifest.tool.channel != config.channel
        || current_tool_hash()? != manifest.tool.tool_sha256
    {
        return Err(io::Error::other("fixed documentation stage drift detected"));
    }
    let artifacts = collect_outputs(&config.output)?;
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
