use super::*;

/// SDK candidates belong to the original release authority and Cargo workspace.
/// The declared frontier is explicit; it does not prove unseen Git history.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct RegistryOwnerSource {
    pub revision: String,
    pub input_sha256: BTreeMap<String, String>,
    /// Original Git link blobs retained as opaque regular files, never followed.
    pub symlink_paths: BTreeSet<String>,
    pub discovery: CargoDiscoveryInputs,
    pub owners: BTreeSet<String>,
    pub outside_release_authority: BTreeSet<String>,
    pub uses_frontier_authority: bool,
    pub absent_authority_members: BTreeSet<String>,
}

/// Discovery transport/config inputs are producer evidence, not archive identity.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct CargoDiscoveryInputs {
    git_sha256: String,
    cargo_sha256: String,
    cargo_version: String,
    config_sha256: BTreeMap<String, Option<String>>,
    environment_sha256: BTreeMap<String, String>,
}

pub(super) fn frontier_git() -> Result<std::path::PathBuf, String> {
    crate::command_var_os("PATH")
        .and_then(|paths| {
            std::env::split_paths(&paths)
                .map(|path| path.join(if cfg!(windows) { "git.exe" } else { "git" }))
                .find(|path| path.is_file())
        })
        .ok_or("source frontier Git is absent from PATH".into())
}

// One maintained Git process reads exact ls-tree IDs without export-ignore,
// textconv, filters or replacement-object substitution.
pub(super) fn git_source_blobs(
    repository: &Path,
    listing: &[u8],
    consume: impl FnMut(&str, &[u8]) -> Result<(), String>,
) -> Result<(), String> {
    let mut child = sdk_docs::command(frontier_git()?)
        .arg("--no-replace-objects")
        .current_dir(repository)
        .args(["cat-file", "--batch"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .map_err(|e| e.to_string())?;
    read_git_source_blobs(&mut child, listing, consume)
}

pub(super) fn read_git_source_blobs(
    child: &mut std::process::Child,
    listing: &[u8],
    mut consume: impl FnMut(&str, &[u8]) -> Result<(), String>,
) -> Result<(), String> {
    let result: Result<(), String> = (|| {
        let mut input = child.stdin.take().ok_or("Git batch stdin is absent")?;
        let mut output = BufReader::new(child.stdout.take().ok_or("Git batch stdout is absent")?);
        for entry in listing
            .split(|byte| *byte == 0)
            .filter(|entry| !entry.is_empty())
        {
            let entry = std::str::from_utf8(entry).map_err(|e| e.to_string())?;
            let (metadata, path) = entry
                .split_once('\t')
                .ok_or("invalid original Git tree entry")?;
            let fields: Vec<_> = metadata.split_whitespace().collect();
            let [mode, kind, object] = fields.as_slice() else {
                return Err("invalid original Git tree metadata".into());
            };
            if !matches!(*mode, "100644" | "100755" | "120000")
                || *kind != "blob"
                || object.len() != 40
                || !object.bytes().all(|b| b.is_ascii_hexdigit())
            {
                return Err("original source frontier contains a linked or non-file member".into());
            }
            writeln!(input, "{object}").map_err(|e| e.to_string())?;
            input.flush().map_err(|e| e.to_string())?;
            let mut header = String::new();
            output.read_line(&mut header).map_err(|e| e.to_string())?;
            let fields: Vec<_> = header.split_whitespace().collect();
            let [actual, kind, size] = fields.as_slice() else {
                return Err("invalid Git batch object header".into());
            };
            if actual != object || *kind != "blob" {
                return Err("Git batch object identity differs from original tree".into());
            }
            let mut bytes = vec![0; size.parse::<usize>().map_err(|e| e.to_string())?];
            output.read_exact(&mut bytes).map_err(|e| e.to_string())?;
            let mut separator = [0];
            output
                .read_exact(&mut separator)
                .map_err(|e| e.to_string())?;
            if separator != *b"\n" {
                return Err("invalid Git batch object boundary".into());
            }
            consume(path, &bytes)?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = child.kill();
    }
    let status = child.wait().map_err(|e| e.to_string())?;
    result?;
    if !status.success() {
        return Err("Git batch source reader failed".into());
    }
    Ok(())
}

pub(super) fn discovery_environment_inputs(
    inputs: impl IntoIterator<Item = (std::ffi::OsString, std::ffi::OsString)>,
    windows: bool,
) -> Result<BTreeMap<String, String>, String> {
    // Retain digests, never raw credential-bearing environment values.
    let mut environment_sha256 = BTreeMap::new();
    for (key, value) in inputs {
        let mut key = key
            .into_string()
            .map_err(|_| "non-UTF8 discovery environment key")?;
        // Windows environment names are case-insensitive and commonly expose
        // PATH as `Path`; retain the same discovery authority on both hosts.
        if windows {
            key.make_ascii_uppercase();
        }
        if key == "PATH"
            || key.starts_with("GIT_")
            || key.starts_with("CARGO_")
            || key.starts_with("RUSTUP_")
            || key == "HOME"
            || key == "USERPROFILE"
        {
            let value = value
                .into_string()
                .map_err(|_| "non-UTF8 discovery environment value")?;
            let digest = crate::sha256_bytes(value.as_bytes());
            if environment_sha256
                .insert(key, digest.clone())
                .is_some_and(|previous| previous != digest)
            {
                return Err("conflicting case aliases in Cargo discovery environment".into());
            }
        }
    }
    Ok(environment_sha256)
}

pub(super) fn cargo_discovery_inputs(
    root: &Path,
    cargo: &Path,
) -> Result<CargoDiscoveryInputs, String> {
    let mut config_sha256 = BTreeMap::new();
    let home = crate::command_var_os("CARGO_HOME")
        .map(std::path::PathBuf::from)
        .or_else(|| {
            crate::command_var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
                .map(|home| std::path::PathBuf::from(home).join(".cargo"))
        })
        .ok_or("Cargo discovery home is absent")?;
    // Record both accepted spellings, including absence. A newly introduced
    // ancestor/home config must not silently change workspace discovery.
    for directory in root
        .ancestors()
        .map(|path| path.join(".cargo"))
        .chain([home])
    {
        for name in ["config", "config.toml"] {
            let path = directory.join(name);
            let digest = if path.try_exists().map_err(|e| e.to_string())? {
                let metadata = fs::symlink_metadata(&path).map_err(|e| e.to_string())?;
                if !metadata.is_file() || is_linked(&metadata) {
                    return Err("Cargo discovery config is redirected or non-file".into());
                }
                Some(crate::sha256_file(&path).map_err(|e| e.to_string())?)
            } else {
                None
            };
            config_sha256.insert(crate::path_string(&path), digest);
        }
    }
    let environment_sha256 =
        discovery_environment_inputs(sdk_docs::command_environment().into_iter(), cfg!(windows))?;
    let git_sha256 =
        crate::file_cache::tool_identity(&frontier_git()?).map_err(|e| e.to_string())?;
    let cargo_sha256 = crate::file_cache::tool_identity(cargo).map_err(|e| e.to_string())?;
    let cargo_version = crate::toolchains::cargo_version(cargo, root).map_err(|e| e.to_string())?;
    Ok(CargoDiscoveryInputs {
        git_sha256,
        cargo_sha256,
        cargo_version,
        config_sha256,
        environment_sha256,
    })
}

pub(super) fn registry_source(
    repository: &Path,
    root: &Path,
    revision: &str,
    capture: bool,
) -> Result<(RegistryOwnerSource, BTreeSet<String>), String> {
    if revision.len() != 40 || !revision.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("registry owner source requires an immutable Git revision".into());
    }
    let root = root.canonicalize().map_err(|e| e.to_string())?;
    let repository = repository.canonicalize().map_err(|e| e.to_string())?;
    let top = command_output(
        sdk_docs::command(frontier_git()?)
            .arg("--no-replace-objects")
            .current_dir(&repository)
            .args(["rev-parse", "--show-toplevel"]),
    )?;
    if Path::new(std::str::from_utf8(&top).map_err(|e| e.to_string())?.trim())
        .canonicalize()
        .map_err(|e| e.to_string())?
        != repository
    {
        return Err("registry owner discovery requires the complete source root".into());
    }
    let listing = command_output(
        sdk_docs::command(frontier_git()?)
            .arg("--no-replace-objects")
            .current_dir(&repository)
            .args(["ls-tree", "-r", "-z", revision]),
    )?;
    let mut source = RegistryOwnerSource {
        revision: revision.into(),
        input_sha256: BTreeMap::new(),
        symlink_paths: listing
            .split(|byte| *byte == 0)
            .filter_map(|entry| {
                let entry = std::str::from_utf8(entry).ok()?;
                let (metadata, path) = entry.split_once('\t')?;
                (metadata.split_whitespace().next() == Some("120000")).then(|| path.to_owned())
            })
            .collect(),
        discovery: CargoDiscoveryInputs::default(),
        owners: BTreeSet::new(),
        outside_release_authority: BTreeSet::new(),
        uses_frontier_authority: false,
        absent_authority_members: BTreeSet::new(),
    };
    // The complete tracked frontier includes configs, workspace manifests and
    // original release authority; no source-owned input is replaced by a name list.
    let mut authority = BTreeSet::new();
    git_source_blobs(&repository, &listing, |path, original| {
        if path == "release/cargo-crates.json" {
            authority.extend(decode_release_authority(original)?);
        }
        let input = root.join(path);
        validate_import_path(&root, &input)?;
        if capture {
            crate::write_immutable(&input, original).map_err(|e| e.to_string())?;
        }
        if fs::read(&input).map_err(|e| e.to_string())?.as_slice() != original {
            return Err("registry owner input differs from declared Git source".into());
        }
        source
            .input_sha256
            .insert(path.into(), crate::sha256_bytes(original));
        Ok(())
    })?;
    // Link blobs preserve the exact Git bytes and mode evidence without
    // constructing host links. They cannot stand in for a Cargo discovery input.
    for path in &source.symlink_paths {
        let path = Path::new(path);
        if path.parent().and_then(Path::file_name) == Some(std::ffi::OsStr::new(".cargo"))
            && matches!(
                path.file_name().and_then(|name| name.to_str()),
                Some("config" | "config.toml")
            )
        {
            return Err("original Git link is a Cargo discovery config".into());
        }
    }
    Ok((source, authority))
}

pub(super) fn discover_owner(
    root: &Path,
    cargo: &Path,
    mut source: RegistryOwnerSource,
    frontier_authority: Option<&BTreeSet<String>>,
) -> Result<RegistryOwnerSource, String> {
    let root = root.canonicalize().map_err(|error| error.to_string())?;
    source.discovery = cargo_discovery_inputs(&root, cargo)?;
    let authority_path = "release/cargo-crates.json";
    let authority = if source.input_sha256.contains_key(authority_path) {
        decode_release_authority(&fs::read(root.join(authority_path)).map_err(|e| e.to_string())?)?
    } else {
        source.uses_frontier_authority = true;
        frontier_authority
            .ok_or("frontier has no original SDK release authority")?
            .clone()
    };
    // Cargo's no-deps branch serializes validated workspace members without
    // build_resolve_graph. Reuse the maintained context, not a workspace glob parser.
    validate_archive_inventory(&root, &source.input_sha256.keys().cloned().collect())?;
    let mut discovery = cargo_metadata::MetadataCommand::new();
    sdk_docs::rustdoc_profiles::CargoExecutionContext {
        cargo_path: Some(cargo),
        lock_selection: None,
    }
    .configure_metadata(&mut discovery)
    .map_err(|e| e.to_string())?;
    discovery
        .cargo_path(cargo)
        .current_dir(&root)
        .manifest_path(root.join("Cargo.toml"))
        .env_remove("CARGO_RESOLVER_LOCKFILE_PATH")
        .other_options(vec!["--locked".into(), "--offline".into()])
        .no_deps();
    let metadata = owner_metadata(&root, cargo, &source, || {
        sdk_docs::execute_metadata(&discovery).map_err(|e| e.to_string())
    })?;
    if metadata
        .workspace_root
        .as_std_path()
        .canonicalize()
        .map_err(|e| e.to_string())?
        != root
    {
        return Err("Cargo owner inspection selected a different workspace".into());
    }
    let cargo_root = metadata.workspace_root.as_std_path();
    for package in &metadata.packages {
        for path in
            std::iter::once(package.manifest_path.as_std_path())
                .chain(
                    package
                        .targets
                        .iter()
                        .map(|target| target.src_path.as_std_path()),
                )
                .chain(package.dependencies.iter().filter_map(|dependency| {
                    dependency.path.as_ref().map(|path| path.as_std_path())
                }))
        {
            if let Ok(relative) = path.strip_prefix(cargo_root) {
                let mut normalized = std::path::PathBuf::new();
                for component in relative.components() {
                    match component {
                        std::path::Component::Normal(part) => normalized.push(part),
                        std::path::Component::ParentDir => {
                            normalized.pop();
                        }
                        std::path::Component::CurDir => {}
                        _ => {
                            return Err("Cargo discovery input has an invalid relative path".into())
                        }
                    }
                }
                if source
                    .symlink_paths
                    .iter()
                    .any(|link| normalized.starts_with(link))
                {
                    return Err(
                        "original Git link is a Cargo manifest, target, or dependency input".into(),
                    );
                }
            }
        }
    }
    let mut workspace_names = BTreeSet::new();
    for package in &metadata.packages {
        if !metadata.workspace_members.contains(&package.id) {
            continue;
        }
        let manifest = package
            .manifest_path
            .as_std_path()
            .canonicalize()
            .map_err(|e| e.to_string())?;
        validate_import_path(&root, &manifest)?;
        let relative = manifest.strip_prefix(&root).map_err(|e| e.to_string())?;
        if !source
            .input_sha256
            .contains_key(&crate::path_string(relative))
        {
            return Err("Cargo member has no original Git manifest".into());
        }
        let name = package.name.to_string();
        validate_name(&name)?;
        if !workspace_names.insert(name.clone()) {
            return Err("original workspace has ambiguous package identities".into());
        }
        if crates_io_eligible(package.publish.as_deref()) {
            if authority.contains(&name) {
                source.owners.insert(name);
            } else {
                source.outside_release_authority.insert(name);
            }
        }
    }
    source.absent_authority_members = authority.difference(&workspace_names).cloned().collect();
    validate_archive_inventory(&root, &source.input_sha256.keys().cloned().collect())?;
    if cargo_discovery_inputs(&root, cargo)? != source.discovery {
        return Err("Cargo discovery tool/config/environment changed during inspection".into());
    }
    for (path, expected) in &source.input_sha256 {
        if crate::sha256_file(&root.join(path)).map_err(|e| e.to_string())? != *expected {
            return Err("registry owner source changed during Cargo inspection".into());
        }
    }
    Ok(source)
}

fn owner_metadata(
    root: &Path,
    cargo: &Path,
    source: &RegistryOwnerSource,
    mut query: impl FnMut() -> Result<Metadata, String>,
) -> Result<Metadata, String> {
    // Opaque Cargo configuration may include further files. Leave it Cargo-owned.
    if !crate::file_cache::is_resolved_cargo(cargo)
        || source.discovery.config_sha256.values().any(Option::is_some)
    {
        return query();
    }
    let Ok(workspace) = workspace_inputs(root) else {
        return query();
    };
    let mut checked_query = || {
        let metadata = query()?;
        if workspace_inputs(root)? != workspace {
            return Err("Cargo workspace discovery inputs changed during inspection".into());
        }
        Ok(metadata)
    };
    // The caller checks the complete original inventory and discovery inputs
    // before AND after this lookup. Never reuse an external input closure.
    let context =
        owner_metadata_context(root, source, sdk_docs::command_environment(), &workspace)?;
    let mut queried = None;
    let encoded = crate::file_cache::owner_metadata(cargo, &context, || {
        let metadata = checked_query().map_err(crate::CliError)?;
        let encoded =
            serde_json::to_string(&metadata).map_err(|error| crate::CliError(error.to_string()))?;
        queried = Some(metadata);
        Ok(encoded)
    })
    .map_err(|error| error.to_string())?;
    let metadata = if let Some(metadata) = queried {
        metadata
    } else {
        let metadata: Metadata =
            serde_json::from_str(&encoded).map_err(|error| error.to_string())?;
        let closed = metadata.packages.iter().all(|package| {
            std::iter::once(package.manifest_path.as_std_path())
                .chain(
                    package
                        .targets
                        .iter()
                        .map(|target| target.src_path.as_std_path()),
                )
                .chain(package.dependencies.iter().filter_map(|dependency| {
                    dependency.path.as_ref().map(|path| path.as_std_path())
                }))
                .map(Path::to_owned)
                .chain(
                    [package.readme.as_ref(), package.license_file.as_ref()]
                        .into_iter()
                        .flatten()
                        .map(|path| {
                            package
                                .manifest_path
                                .parent()
                                .unwrap()
                                .join(path)
                                .into_std_path_buf()
                        }),
                )
                .all(|path| path.canonicalize().is_ok_and(|path| path.starts_with(root)))
        });
        if closed {
            metadata
        } else {
            checked_query()?
        }
    };
    if workspace_inputs(root)? != workspace {
        return Err("Cargo workspace discovery inputs changed during inspection".into());
    }
    Ok(metadata)
}

fn workspace_inputs(root: &Path) -> Result<BTreeMap<String, Option<String>>, String> {
    let mut inputs = BTreeMap::new();
    for directory in root.ancestors().skip(1) {
        for name in ["Cargo.toml", "Cargo.lock"] {
            let path = directory.join(name);
            let digest = match fs::symlink_metadata(&path) {
                Ok(metadata) if metadata.is_file() && !is_linked(&metadata) => {
                    Some(crate::sha256_file(&path).map_err(|error| error.to_string())?)
                }
                Ok(_) => return Err("workspace discovery input is redirected or non-file".into()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(error.to_string()),
            };
            inputs.insert(crate::path_string(&path), digest);
        }
    }
    Ok(inputs)
}

fn owner_metadata_context(
    root: &Path,
    source: &RegistryOwnerSource,
    environment: BTreeMap<std::ffi::OsString, std::ffi::OsString>,
    workspace: &BTreeMap<String, Option<String>>,
) -> Result<String, String> {
    let input = serde_json::to_vec(&(source, workspace)).map_err(|error| error.to_string())?;
    Ok(crate::toolchains::cargo_context(
        root,
        &format!("owner-metadata:{}", crate::sha256_bytes(&input)),
        environment,
    ))
}

#[cfg(test)]
mod metadata_tests;
