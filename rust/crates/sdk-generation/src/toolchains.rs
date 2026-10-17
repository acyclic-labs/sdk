use super::*;

static VERSIONS: crate::concrete_versions::Versions<PathBuf, sdk_file_identity::Stamp> =
    crate::concrete_versions::Mutex::new(BTreeMap::new());

pub(super) fn rustc_host() -> Result<String, CliError> {
    sdk_docs::rustdoc_profiles::rustdoc_host().map_err(profile_error)
}

pub(super) fn pinned_cargo(root: &Path) -> Result<PathBuf, CliError> {
    let declaration = root.join("rust-toolchain.toml");
    select_cargo(
        root,
        &declaration,
        &fs::read_to_string(&declaration).map_err(io_error)?,
    )
}

pub(super) fn maintained_cargo(root: &Path) -> Result<PathBuf, CliError> {
    select_cargo(
        root,
        &std::env::current_exe().map_err(io_error)?,
        include_str!("../../../../rust-toolchain.toml"),
    )
}

fn select_cargo(root: &Path, declaration: &Path, source: &str) -> Result<PathBuf, CliError> {
    let toolchain = source
        .lines()
        .find_map(|line| {
            let (key, value) = line.split_once('=')?;
            (key.trim() == "channel").then(|| value.trim().trim_matches('"').to_owned())
        })
        .ok_or_else(|| CliError("rust-toolchain.toml has no pinned channel".into()))?;
    let context = format!(
        "rustup-which:{}",
        cargo_context(
            root,
            &toolchain,
            sdk_docs::command_environment().into_iter()
        )
    );
    let path = crate::file_cache::get(&declaration, &context, || {
        let output = sdk_docs::command("rustup")
            .args(["which", "cargo", "--toolchain", &toolchain])
            .run_output()
            .map_err(io_error)?;
        if !output.status.success() {
            return Err(CliError(format!(
                "pinned Cargo toolchain `{toolchain}` is unavailable: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    })?;
    let path = PathBuf::from(path);
    if !path.is_file() {
        return Err(CliError(format!(
            "rustup selected a missing Cargo executable {}",
            path.display()
        )));
    }
    let path = path.canonicalize().map_err(io_error)?;
    crate::file_cache::resolved_cargo(&path);
    Ok(path)
}

pub(super) fn toolchain_identity(cargo: &Path) -> Result<String, CliError> {
    Ok(cargo_version(
        cargo,
        &sdk_docs::command_directory()
            .map(Ok)
            .unwrap_or_else(std::env::current_dir)
            .map_err(io_error)?,
    )?
    .trim()
    .to_owned())
}

pub(super) fn cargo_version(cargo: &Path, root: &Path) -> Result<String, CliError> {
    let probe = || {
        let output = sdk_docs::command(cargo)
            .current_dir(root)
            .arg("-vV")
            .run_output()
            .map_err(io_error)?;
        if !output.status.success() {
            return Err(CliError("pinned Cargo identity query failed".into()));
        }
        String::from_utf8(output.stdout).map_err(|error| CliError(error.to_string()))
    };
    if crate::file_cache::is_resolved_cargo(cargo) {
        crate::file_cache::get(cargo, "cargo-vV", || {
            crate::concrete_versions::query(
                &VERSIONS,
                cargo.to_owned(),
                || sdk_file_identity::stamp(cargo).map_err(io_error),
                probe,
                || CliError("concrete Cargo changed during its version query".into()),
            )
        })
    } else {
        // A proxy's own stamp does not identify the toolchain it dispatches to.
        probe()
    }
}

pub(super) fn cargo_context(
    root: &Path,
    discovery: &str,
    environment: impl IntoIterator<Item = (std::ffi::OsString, std::ffi::OsString)>,
) -> String {
    // Tool selection can observe its working directory, declaration and environment.
    // Frame every environment byte, retaining only its digest in the cache key.
    let mut context = Vec::new();
    for bytes in [root.as_os_str().as_encoded_bytes(), discovery.as_bytes()] {
        context.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
        context.extend_from_slice(bytes);
    }
    for (key, value) in environment.into_iter().collect::<BTreeMap<_, _>>() {
        for part in [key, value] {
            let bytes = part.as_encoded_bytes();
            context.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
            context.extend_from_slice(bytes);
        }
    }
    format!("cargo-vV:{}", sha256_bytes(&context))
}

pub(super) fn rustup_targets() -> Result<BTreeSet<String>, CliError> {
    sdk_docs::rustdoc_profiles::rustdoc_targets(&["wasm32-unknown-unknown"]).map_err(profile_error)
}
