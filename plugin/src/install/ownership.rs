//! MCP configuration ownership.

use super::*;

#[derive(Deserialize, Serialize)]
pub(crate) struct ConfigOwnership {
    pub(crate) version: u32,
    pub(crate) config_path: PathBuf,
    pub(crate) section: String,
    pub(crate) prior: Option<Value>,
    pub(crate) installed: Value,
}

pub(crate) fn ownership_path(path: &Path, section: &str) -> PathBuf {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-mcp-config-ownership-v1\0");
    hasher.update(path.as_os_str().to_string_lossy().as_bytes());
    hasher.update(&[0]);
    hasher.update(section.as_bytes());
    #[cfg(test)]
    return path.with_extension(format!(
        "{}.acyclic-owner",
        hasher
            .finalize()
            .to_hex()
            .chars()
            .take(16)
            .collect::<String>()
    ));
    #[cfg(not(test))]
    default_data_directory()
        .join("install-ownership")
        .join(format!(
            "{}.json",
            hasher
                .finalize()
                .to_hex()
                .chars()
                .take(32)
                .collect::<String>()
        ))
}

pub(crate) fn read_ownership(path: &Path) -> Result<Option<ConfigOwnership>, String> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(display),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(display(error)),
    }
}

pub(crate) fn write_ownership(path: &Path, ownership: &ConfigOwnership) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(display)?;
    }
    let temporary = path.with_extension("acyclic-next");
    fs::write(
        &temporary,
        serde_json::to_vec_pretty(ownership).map_err(display)?,
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

pub(crate) fn read_optional_json(path: &Path, label: &str) -> Result<Option<Value>, String> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(|error| {
            format!(
                "refusing to overwrite invalid {label} at {}: {error}",
                path.display()
            )
        }),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(display(error)),
    }
}

pub(crate) fn install_owned_json(
    path: &Path,
    section: &str,
    label: &str,
    build: impl FnOnce(Option<&Value>) -> Result<Value, String>,
) -> Result<(), String> {
    install_owned_json_normalized(path, section, label, |prior| Ok(prior.cloned()), build)
}

pub(crate) fn install_owned_json_normalized(
    path: &Path,
    section: &str,
    label: &str,
    normalize: impl FnOnce(Option<&Value>) -> Result<Option<Value>, String>,
    build: impl FnOnce(Option<&Value>) -> Result<Value, String>,
) -> Result<(), String> {
    let ownership_path = ownership_path(path, section);
    let current = read_optional_json(path, label)?;
    let raw_prior = match read_ownership(&ownership_path)? {
        Some(ownership)
            if ownership.version == 1
                && ownership.config_path == path
                && ownership.section == section
                && (current == ownership.prior
                    || current.as_ref() == Some(&ownership.installed)) =>
        {
            ownership.prior
        }
        Some(_) => {
            return Err(format!(
                "{label} ownership at {} is inconsistent; refusing to overwrite it",
                path.display()
            ));
        }
        None => current,
    };
    let prior = normalize(raw_prior.as_ref())?;
    let installed = build(prior.as_ref())?;
    write_ownership(
        &ownership_path,
        &ConfigOwnership {
            version: 1,
            config_path: path.to_path_buf(),
            section: section.to_owned(),
            prior,
            installed: installed.clone(),
        },
    )?;
    write_json_with_backup(path, &installed)
}

pub(crate) fn remove_owned_json(path: &Path, section: &str, label: &str) -> Result<(), String> {
    let ownership_path = ownership_path(path, section);
    let current = read_optional_json(path, label)?;
    let Some(ownership) = read_ownership(&ownership_path)? else {
        if current.is_some() {
            return Err(format!(
                "refusing to remove {label} without an Acyclic ownership record from {}",
                path.display()
            ));
        }
        return Ok(());
    };
    if ownership.version != 1
        || ownership.config_path != path
        || ownership.section != section
        || (current != ownership.prior && current.as_ref() != Some(&ownership.installed))
    {
        return Err(format!(
            "the Acyclic {label} at {} were modified; leaving them untouched",
            path.display()
        ));
    }
    if current != ownership.prior {
        match &ownership.prior {
            Some(prior) => write_json_with_backup(path, prior)?,
            None => match fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(display(error)),
            },
        }
    }
    fs::remove_file(&ownership_path).map_err(display)
}
