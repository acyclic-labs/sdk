//! Configuration file operations and locations.

use super::*;

pub(crate) fn read_json_object(path: &Path) -> Result<serde_json::Map<String, Value>, String> {
    if !path.exists() {
        return Ok(serde_json::Map::new());
    }
    serde_json::from_slice::<Value>(&fs::read(path).map_err(display)?)
        .map_err(display)?
        .as_object()
        .cloned()
        .ok_or_else(|| format!("{} must contain a JSON object", path.display()))
}

pub(crate) fn object_entry<'a>(
    document: &'a mut serde_json::Map<String, Value>,
    key: &str,
) -> Result<&'a mut serde_json::Map<String, Value>, String> {
    document
        .entry(key.to_owned())
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .ok_or_else(|| format!("configuration field '{key}' must be an object"))
}

pub(crate) fn write_json_with_backup(path: &Path, value: &Value) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(display)?;
    }
    let backup = path.with_extension(format!(
        "{}.acyclic-backup",
        path.extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or("json")
    ));
    if path.exists() && !backup.exists() {
        fs::copy(path, &backup).map_err(display)?;
    }
    let temporary = path.with_extension("acyclic-next");
    fs::write(
        &temporary,
        serde_json::to_vec_pretty(value).map_err(display)?,
    )
    .map_err(display)?;
    acyclic_native_runtime::durable_rename(
        &temporary,
        path,
        acyclic_native_runtime::RenameMode::Replace,
        None,
    )
    .map_err(display)
}

pub(crate) fn write_text_with_backup(path: &Path, value: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(display)?;
    }
    let backup = path.with_extension(format!(
        "{}.acyclic-backup",
        path.extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or("txt")
    ));
    if path.exists() && !backup.exists() {
        fs::copy(path, &backup).map_err(display)?;
    }
    let temporary = path.with_extension("acyclic-next");
    fs::write(&temporary, value.as_bytes()).map_err(display)?;
    acyclic_native_runtime::durable_rename(
        &temporary,
        path,
        acyclic_native_runtime::RenameMode::Replace,
        None,
    )
    .map_err(display)
}

pub(crate) fn executable_on_path(name: &str) -> bool {
    find_executable(name).is_some()
}

/// The command a shell would run for `name`: the first `PATH` directory
/// holding it, trying each `PATHEXT` extension on Windows. Package managers
/// install host CLIs there as `.cmd` launchers (npm's `codex.cmd`), which a
/// bare `Command::new(name)` never finds because it only appends `.exe`.
pub(crate) fn find_executable(name: &str) -> Option<PathBuf> {
    let paths = env::var_os("PATH")?;
    let extensions = env::var_os("PATHEXT");
    executable_in(
        env::split_paths(&paths),
        name,
        cfg!(windows).then(|| {
            extensions
                .as_deref()
                .unwrap_or(std::ffi::OsStr::new(".COM;.EXE;.BAT;.CMD"))
        }),
    )
}

pub(crate) fn executable_in(
    directories: impl IntoIterator<Item = PathBuf>,
    name: &str,
    extensions: Option<&std::ffi::OsStr>,
) -> Option<PathBuf> {
    let extensions: Vec<String> = extensions.map_or_else(Vec::new, |extensions| {
        extensions
            .to_string_lossy()
            .split(';')
            .filter(|extension| extension.starts_with('.'))
            // Windows matches names without regard to case; package managers
            // write their launchers in lower case.
            .map(str::to_ascii_lowercase)
            .collect()
    });
    directories.into_iter().find_map(|directory| {
        if extensions.is_empty() {
            let candidate = directory.join(name);
            return runnable(&candidate).then_some(candidate);
        }
        extensions.iter().find_map(|extension| {
            let candidate = directory.join(format!("{name}{extension}"));
            runnable(&candidate).then_some(candidate)
        })
    })
}

/// A file the operating system would execute: on Unix, as `execvp` does, a
/// file without execute permission does not hide a later `PATH` entry.
fn runnable(candidate: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::metadata(candidate)
            .is_ok_and(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
    }
    #[cfg(not(unix))]
    {
        candidate.is_file()
    }
}

/// A command for the host CLI `name` as installed on `PATH`.
pub(crate) fn host_command(name: &str) -> Result<std::process::Command, String> {
    find_executable(name)
        .map(std::process::Command::new)
        .ok_or_else(|| format!("cannot find the `{name}` command on PATH"))
}

pub(crate) fn home_directory() -> Result<PathBuf, String> {
    env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
        .map(PathBuf::from)
        .ok_or_else(|| "cannot locate the per-user configuration directory".to_owned())
}

pub(crate) fn config_directory() -> Result<PathBuf, String> {
    if cfg!(windows) {
        env::var_os("APPDATA")
            .map(PathBuf::from)
            .ok_or_else(|| "APPDATA is unavailable".to_owned())
    } else {
        Ok(env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .unwrap_or(home_directory()?.join(".config")))
    }
}

pub(crate) fn claude_desktop_config() -> Result<PathBuf, String> {
    if cfg!(target_os = "macos") {
        Ok(home_directory()?.join("Library/Application Support/Claude/claude_desktop_config.json"))
    } else {
        Ok(config_directory()?.join("Claude/claude_desktop_config.json"))
    }
}
