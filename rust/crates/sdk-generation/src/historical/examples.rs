use super::*;

/// A source-owned example and its exact Cargo invocation inputs.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Example {
    pub package: String,
    pub version: String,
    pub target: String,
    pub source_path: String,
    pub required_features: Vec<String>,
    pub source_sha256: String,
}

pub fn examples(
    metadata: &Metadata,
    root: &Path,
    owners: &BTreeSet<String>,
) -> Result<Vec<Example>, String> {
    let root = root.canonicalize().map_err(|e| e.to_string())?;
    if owners.iter().any(|owner| {
        !metadata
            .packages
            .iter()
            .any(|package| package.name.as_ref() == owner)
    }) {
        return Err("historical example owner is absent from Cargo metadata".into());
    }
    let mut examples = Vec::new();
    for package in metadata
        .packages
        .iter()
        .filter(|package| owners.contains(package.name.as_ref()))
    {
        for target in package
            .targets
            .iter()
            .filter(|target| target.kind.contains(&TargetKind::Example))
        {
            let path = target
                .src_path
                .as_std_path()
                .canonicalize()
                .map_err(|e| e.to_string())?;
            let relative = path
                .strip_prefix(&root)
                .map_err(|_| "historical example source escapes source root")?;
            examples.push(Example {
                package: package.name.to_string(),
                version: package.version.to_string(),
                target: target.name.clone(),
                source_path: crate::path_string(relative),
                required_features: target.required_features.clone(),
                source_sha256: crate::sha256_file(&path).map_err(|e| e.to_string())?,
            });
        }
    }
    examples.sort_by(|a, b| (&a.package, &a.target).cmp(&(&b.package, &b.target)));
    Ok(examples)
}

/// Extend the maintained closure with source files verified from archive members.
pub fn source_file_hashes(
    root: &Path,
    output: &Path,
    archive_paths: &[String],
) -> Result<BTreeMap<String, String>, String> {
    let mut files = crate::source_file_hashes(root, output).map_err(|e| e.to_string())?;
    for relative in archive_paths {
        let path = Path::new(relative);
        if path.is_absolute()
            || path
                .components()
                .any(|part| matches!(part, Component::ParentDir | Component::Prefix(_)))
        {
            return Err("historical archive source path escapes source root".into());
        }
        let physical = root.join(path).canonicalize().map_err(|e| e.to_string())?;
        if !physical.starts_with(root.canonicalize().map_err(|e| e.to_string())?) {
            return Err("historical archive source escapes source root".into());
        }
        files.insert(
            relative.clone(),
            crate::sha256_file(&physical).map_err(|e| e.to_string())?,
        );
    }
    Ok(files)
}

pub fn scope_source_file_hashes(
    root: &Path,
    output: &Path,
    scope: &Scope,
) -> Result<BTreeMap<String, String>, String> {
    if matches!(scope, Scope::RegistryArchives { .. }) {
        crate::imported_source_file_hashes(root, output).map_err(|e| e.to_string())
    } else {
        source_file_hashes(root, output, scope.archive_source_paths())
    }
}

/// Execute each target with one shared Cargo cache and one source baseline.
pub fn execute_examples(
    root: &Path,
    cargo: &Path,
    target_dir: &Path,
    examples: &[Example],
    owner_metadata: Option<&BTreeMap<String, Metadata>>,
    captured: Option<&CapturedSource>,
) -> Result<Vec<ExampleExecution>, String> {
    let root = root.canonicalize().map_err(|e| e.to_string())?;
    let capture = || {
        if owner_metadata.is_some() {
            crate::imported_source_file_hashes(&root, target_dir)
        } else {
            crate::source_file_hashes(&root, target_dir)
        }
    };
    let source_files = capture().map_err(|e| e.to_string())?;
    let mut results = Vec::new();
    for example in examples {
        let source = root
            .join(&example.source_path)
            .canonicalize()
            .map_err(|e| e.to_string())?;
        if !source.starts_with(&root) {
            return Err("historical example source escapes source root".into());
        }
        if crate::sha256_file(&root.join(&example.source_path)).map_err(|e| e.to_string())?
            != example.source_sha256
        {
            return Err("historical example source changed before execution".into());
        }
        let mut command = sdk_docs::command(cargo);
        let manifest = if let Some(owners) = owner_metadata {
            let metadata = owners
                .get(&example.package)
                .ok_or("historical example owner metadata is absent")?;
            metadata
                .packages
                .iter()
                .find(|package| package.name.as_ref() == example.package)
                .ok_or("historical example Cargo owner is absent")?
                .manifest_path
                .clone()
                .into_std_path_buf()
        } else {
            root.join("Cargo.toml")
        };
        command
            .current_dir(if owner_metadata.is_some() {
                manifest
                    .parent()
                    .ok_or("archive example manifest has no parent")?
            } else {
                &root
            })
            .args(["run", "--locked", "--quiet", "--manifest-path"])
            .arg(&manifest)
            .args(["--package", &example.package, "--example", &example.target])
            .arg("--target-dir")
            .arg(target_dir);
        let config = producer_config(&root, captured, &example.package)?;
        let lock = manifest.with_file_name("Cargo.lock");
        sdk_docs::rustdoc_profiles::CargoExecutionContext {
            cargo_path: Some(cargo),
            lock_selection: producer_lock_selection(config.as_deref(), &lock, captured),
        }
        .configure(&mut command)
        .map_err(|e| e.to_string())?;
        if !example.required_features.is_empty() {
            command
                .arg("--features")
                .arg(example.required_features.join(","));
        }
        if let Some(directory) = cargo.parent() {
            command.env(
                "RUSTC",
                directory.join(if cfg!(windows) { "rustc.exe" } else { "rustc" }),
            );
        }
        command
            .env_remove("RUSTC_WRAPPER")
            .env_remove("RUSTC_WORKSPACE_WRAPPER");
        let output = command.run_output().map_err(|e| e.to_string())?;
        if !output.status.success() {
            return Err(format!(
                "historical example {}::{} failed: {}",
                example.package,
                example.target,
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        if crate::sha256_file(&root.join(&example.source_path)).map_err(|e| e.to_string())?
            != example.source_sha256
        {
            return Err("historical example source changed during execution".into());
        }
        if capture().map_err(|e| e.to_string())? != source_files {
            return Err("historical source closure changed during example execution".into());
        }
        results.push(ExampleExecution {
            example: example.clone(),
            stdout_sha256: crate::sha256_bytes(&output.stdout),
            stderr_sha256: crate::sha256_bytes(&output.stderr),
        });
    }
    Ok(results)
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExampleExecution {
    pub example: Example,
    pub stdout_sha256: String,
    pub stderr_sha256: String,
}
