use super::*;

pub(super) fn rustc_host() -> Result<String, CliError> {
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

pub(super) fn pinned_cargo(root: &Path) -> Result<PathBuf, CliError> {
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

pub(super) fn toolchain_identity(cargo: &Path) -> Result<String, CliError> {
    let output = Command::new(cargo)
        .args(["-V", "--verbose"])
        .output()
        .map_err(io_error)?;
    if !output.status.success() {
        return Err(CliError("pinned Cargo identity query failed".into()));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

pub(super) fn rustup_targets() -> Result<BTreeSet<String>, CliError> {
    sdk_docs::rustdoc_profiles::rustdoc_targets(&["wasm32-unknown-unknown"]).map_err(profile_error)
}
