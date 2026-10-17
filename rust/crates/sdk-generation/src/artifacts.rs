use super::*;

pub(super) fn rustdoc_files(dir: &Path) -> Result<Vec<PathBuf>, CliError> {
    let mut result = fs::read_dir(dir)
        .map_err(io_error)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension() == Some(OsStr::new("json")))
        .collect::<Vec<_>>();
    result.sort();
    Ok(result)
}

pub(super) fn package_for_crate<'a>(
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
                        TargetKind::ProcMacro
                            | TargetKind::Lib
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

pub(super) fn published_packages(root: &Path) -> Result<Vec<String>, CliError> {
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

pub(super) fn validate_release_package_versions(
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

pub(super) fn validate_published_coverage(
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

pub(super) fn canonical(path: &Path) -> Result<PathBuf, CliError> {
    absolute(path)?.canonicalize().map_err(io_error)
}
pub(super) fn absolute(path: &Path) -> Result<PathBuf, CliError> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(sdk_docs::command_directory()
            .map(Ok)
            .unwrap_or_else(env::current_dir)
            .map_err(io_error)?
            .join(path))
    }
}
pub(super) fn path_string(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}
pub(super) fn profile_error(error: impl std::fmt::Display) -> CliError {
    CliError(format!("profile error: {error}"))
}
pub(super) fn docs_error(error: impl std::fmt::Display) -> CliError {
    CliError(format!("docs error: {error}"))
}
pub(super) fn scenario_error(error: impl std::fmt::Display) -> CliError {
    CliError(format!("scenario error: {error}"))
}
pub(super) fn io_error(error: std::io::Error) -> CliError {
    CliError(format!("I/O error: {error}"))
}
pub(super) fn json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, CliError> {
    serde_json::to_vec_pretty(value).map_err(|e| CliError(format!("JSON error: {e}")))
}
pub(super) fn sha256_bytes(bytes: &[u8]) -> String {
    format_digest(ring::digest::digest(&ring::digest::SHA256, bytes).as_ref())
}
fn format_digest(digest: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(7 + digest.len() * 2);
    result.push_str("sha256:");
    for &byte in digest {
        result.push(DIGITS[(byte >> 4) as usize] as char);
        result.push(DIGITS[(byte & 0xf) as usize] as char);
    }
    result
}
pub(super) fn sha256_file(path: &Path) -> Result<String, CliError> {
    let bytes = fs::read(path).map_err(io_error)?;
    #[cfg(test)]
    HASH_BYTES.set(HASH_BYTES.get() + bytes.len() as u64);
    Ok(sha256_bytes(&bytes))
}

#[cfg(test)]
thread_local! { pub(super) static HASH_BYTES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) }; }

pub(super) fn receipt_source_attestation(path: &Path) -> Result<Option<String>, CliError> {
    let sidecar = path.with_extension("source.sha256");
    match fs::read_to_string(&sidecar) {
        Ok(value) => Ok(Some(value.trim().to_owned())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_error(error)),
    }
}

pub(super) fn rustdoc_key(root: &Path, path: &Path) -> Result<String, CliError> {
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
pub fn materialize_generated_sources(
    root: &Path,
    rustdoc_files: &[PathBuf],
) -> Result<Vec<GeneratedSource>, CliError> {
    let root = root.canonicalize().map_err(io_error)?;
    let root = root.as_path();
    let mut filenames = BTreeSet::new();
    for receipt in rustdoc_files {
        let value = sdk_docs::read_rustdoc_json(receipt).map_err(docs_error)?;
        collect_span_filenames(&value, &mut filenames);
    }
    let mut candidates = BTreeMap::<String, Vec<PathBuf>>::new();
    // Current producers retain their exact physical sources. Search the build
    // tree only when a missing span actually needs the restoration boundary.
    if filenames.iter().any(|filename| {
        let path = Path::new(filename);
        path.is_absolute() && !path.is_file()
    }) {
        collect_named_files(&root.join("target"), &mut candidates)?;
        collect_named_files(&root.join("rust"), &mut candidates)?;
    }
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
            if !comparable_path(&physical).starts_with(comparable_path(root))
                && !retained_aliases.contains_key(&filename)
            {
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

pub(super) fn validate_restore_path(
    root: &Path,
    receipts: &[PathBuf],
    path: &Path,
) -> Result<(), CliError> {
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
    if !owned
        .iter()
        .any(|directory| comparable_path(path).starts_with(comparable_path(directory)))
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

fn comparable_path(path: &Path) -> PathBuf {
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
}

pub(super) fn collect_span_filenames(value: &serde_json::Value, output: &mut BTreeSet<String>) {
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

pub(super) fn collect_named_files(
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

pub(super) fn write_immutable(path: &Path, bytes: &[u8]) -> Result<(), CliError> {
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

pub(super) fn artifact_hashes(output: &Path) -> Result<BTreeMap<String, String>, CliError> {
    let mut files = Vec::new();
    collect_files(output, output, &mut files, true)?;
    files.sort();
    let mut result = BTreeMap::new();
    for path in files {
        if !is_immutable_artifact(output, &path) {
            continue;
        }
        let relative = path.strip_prefix(output).unwrap_or(&path);
        result.insert(
            path_string(relative),
            crate::file_cache::sha256_file(&path)?,
        );
    }
    Ok(result)
}

pub(super) fn is_immutable_artifact(output: &Path, path: &Path) -> bool {
    !matches!(path.file_name().and_then(OsStr::to_str),
        Some("generation-manifest.v1.json" | "generation-manifest.v1.sha256"))
        // The changing catalog and its coordination lock are not artifacts.
        // Windows enforces the lock on reads, including inventory hashing.
        && path != output.join("sdk-docs-versions.v1.json")
        && path != output.join(".sdk-docs-versions.v1.lock")
}

pub(super) fn artifact_hashes_with_scope(
    output: &Path,
    scope: &historical::Scope,
) -> Result<BTreeMap<String, String>, CliError> {
    let mut artifacts = artifact_hashes(output)?;
    if matches!(scope, historical::Scope::RegistryArchives { .. }) {
        // This exact root belongs to the existing compiler-span retention
        // engine; actual archived inputs live under package-version roots.
        artifacts.retain(|path, _| !path.starts_with("sources/target/"));
    }
    Ok(artifacts)
}

pub(super) fn declared_archive_toolchain(metadata: &Metadata) -> Result<String, CliError> {
    let declarations = metadata
        .packages
        .iter()
        .map(|package| {
            (
                package.name.to_string(),
                package.rust_version.as_ref().map(ToString::to_string),
            )
        })
        .collect::<BTreeMap<_, _>>();
    serde_json::to_string(&declarations).map_err(|e| CliError(e.to_string()))
}

pub(super) fn collect_files(
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

pub(super) fn source_file_hashes(
    root: &Path,
    output: &Path,
) -> Result<BTreeMap<String, String>, CliError> {
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

pub(super) fn imported_source_file_hashes(
    root: &Path,
    output: &Path,
) -> Result<BTreeMap<String, String>, CliError> {
    let mut files = Vec::new();
    collect_source_files_inner(root, root, output, &mut files, true)?;
    files.sort();
    files
        .into_iter()
        .map(|path| {
            Ok((
                path_string(
                    path.strip_prefix(root)
                        .map_err(|e| CliError(e.to_string()))?,
                ),
                sha256_file(&path)?,
            ))
        })
        .collect()
}

pub(super) fn digest_map(files: &BTreeMap<String, String>) -> String {
    let mut hasher = ring::digest::Context::new(&ring::digest::SHA256);
    for (path, digest) in files {
        hasher.update(path.as_bytes());
        hasher.update(&[0]);
        hasher.update(digest.as_bytes());
        hasher.update(&[0]);
    }
    format_digest(hasher.finish().as_ref())
}

pub(super) fn collect_source_files(
    root: &Path,
    current: &Path,
    output: &Path,
    files: &mut Vec<PathBuf>,
) -> Result<(), CliError> {
    collect_source_files_inner(root, current, output, files, false)
}

pub(super) fn collect_source_files_inner(
    root: &Path,
    current: &Path,
    output: &Path,
    files: &mut Vec<PathBuf>,
    all_archive_members: bool,
) -> Result<(), CliError> {
    for entry in fs::read_dir(current).map_err(io_error)? {
        let path = entry.map_err(io_error)?.path();
        if !all_archive_members && (path == output || path.starts_with(output)) {
            continue;
        }
        if all_archive_members {
            if path == root.join("target") {
                continue;
            }
            let metadata = fs::symlink_metadata(&path).map_err(io_error)?;
            #[cfg(windows)]
            let reparse = {
                use std::os::windows::fs::MetadataExt;
                metadata.file_attributes() & 0x400 != 0
            };
            #[cfg(not(windows))]
            let reparse = false;
            if metadata.file_type().is_symlink() || reparse || !canonical(&path)?.starts_with(root)
            {
                return Err(CliError(
                    "imported source contains a linked or escaping member".into(),
                ));
            }
            if metadata.is_dir() {
                collect_source_files_inner(root, &path, output, files, true)?;
            } else if metadata.is_file() {
                files.push(path);
            } else {
                return Err(CliError(
                    "imported source contains a non-file member".into(),
                ));
            }
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
            collect_source_files_inner(root, &path, output, files, false)?;
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
                "compatibility/descriptors/actors-v1.bin"
                    | "compatibility/descriptors/auth-v1.bin"
                    | "compatibility/descriptors/filesystem-v2.bin"
                    | "compatibility/descriptors/inference-v1.bin"
                    | "compatibility/descriptors/machines-v1.bin"
                    | "compatibility/descriptors/objects-v1.bin"
                    | "compatibility/descriptors/stream-v1.bin"
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
                || relative == Path::new("compatibility/Cargo.toml")
                || relative.starts_with("compatibility/src")
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

pub(super) fn git_revision(root: &Path) -> Result<String, CliError> {
    let output = sdk_docs::command("git")
        .current_dir(root)
        .args(["rev-parse", "HEAD"])
        .run_output()
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

pub(super) fn git_dirty(root: &Path) -> Result<bool, CliError> {
    let output = sdk_docs::command("git")
        .current_dir(root)
        .args(["status", "--porcelain"])
        .run_output()
        .map_err(io_error)?;
    Ok(!output.status.success() || !output.stdout.is_empty())
}
