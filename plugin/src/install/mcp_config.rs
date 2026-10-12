//! MCP configuration ownership.

use super::*;

#[derive(Clone, Copy)]
pub(crate) enum McpConfigShape {
    Standard,
    VsCode,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct McpConfigOwnership {
    pub(crate) version: u32,
    pub(crate) config_path: PathBuf,
    pub(crate) section: String,
    pub(crate) prior: Option<Value>,
    pub(crate) installed: Value,
}

pub(crate) fn mcp_ownership_path(path: &Path, section: &str) -> PathBuf {
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

pub(crate) fn read_mcp_ownership(path: &Path) -> Result<Option<McpConfigOwnership>, String> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(display),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(display(error)),
    }
}

pub(crate) fn write_mcp_ownership(
    path: &Path,
    ownership: &McpConfigOwnership,
) -> Result<(), String> {
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
    let ownership_path = mcp_ownership_path(path, section);
    let current = read_optional_json(path, label)?;
    let raw_prior = match read_mcp_ownership(&ownership_path)? {
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
    write_mcp_ownership(
        &ownership_path,
        &McpConfigOwnership {
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
    let ownership_path = mcp_ownership_path(path, section);
    let current = read_optional_json(path, label)?;
    let Some(ownership) = read_mcp_ownership(&ownership_path)? else {
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

pub(crate) fn host_config(host: &str, project: bool) -> Result<(PathBuf, McpConfigShape), String> {
    let cwd = env::current_dir().map_err(display)?;
    let home = home_directory()?;
    let result = match (host, project) {
        ("vscode", true) => (cwd.join(".mcp.json"), McpConfigShape::VsCode),
        ("vscode", false) => {
            return Err("VS Code installation is project-scoped; pass --project".to_owned());
        }
        ("copilot", false) => (
            home.join(".copilot/mcp-config.json"),
            McpConfigShape::Standard,
        ),
        ("claude-desktop", false) => (claude_desktop_config()?, McpConfigShape::Standard),
        ("copilot-cloud", _) => {
            return Err("Copilot cloud is job-local; install the binary in the job and invoke its MCP bridge explicitly".to_owned());
        }
        ("pydantic-ai", _) => {
            return Err("Pydantic AI requires explicit SDK integration and has no host configuration to mutate".to_owned());
        }
        _ => return Err(format!("unsupported Acyclic host '{host}'")),
    };
    Ok(result)
}

pub(crate) fn merge_mcp_config(
    path: &Path,
    shape: McpConfigShape,
    executable: &Path,
) -> Result<(), String> {
    let mut document = read_json_object(path)?;
    let command = executable.to_string_lossy().into_owned();
    let (section, installed) = match shape {
        McpConfigShape::Standard => (
            "mcpServers",
            json!({"command":command,"args":["__mcp-commandless"]}),
        ),
        McpConfigShape::VsCode => (
            "servers",
            json!({"type":"stdio","command":command,"args":["__mcp-commandless"]}),
        ),
    };
    let ownership_path = mcp_ownership_path(path, section);
    let entries = object_entry(&mut document, section)?;
    let current = entries.get("acyclic").cloned();
    let ownership = match read_mcp_ownership(&ownership_path)? {
        Some(ownership)
            if ownership.version == 1
                && ownership.config_path == path
                && ownership.section == section
                && ownership.installed == installed
                && (current == ownership.prior || current == Some(installed.clone())) =>
        {
            ownership
        }
        Some(_) => {
            return Err(format!(
                "Acyclic does not own the current '{}' entry in {}; resolve it manually",
                section,
                path.display()
            ));
        }
        None if current.is_none() => McpConfigOwnership {
            version: 1,
            config_path: path.to_path_buf(),
            section: section.to_owned(),
            prior: None,
            installed: installed.clone(),
        },
        None => {
            return Err(format!(
                "refusing to replace the unowned 'acyclic' entry in {}",
                path.display()
            ));
        }
    };
    write_mcp_ownership(&ownership_path, &ownership)?;
    entries.insert("acyclic".to_owned(), installed);
    if let Err(error) = write_json_with_backup(path, &Value::Object(document)) {
        return Err(format!("{error}; ownership intent retained for recovery"));
    }
    Ok(())
}

pub(crate) fn remove_mcp_config(path: &Path, shape: McpConfigShape) -> Result<(), String> {
    if !path.exists() {
        return Ok(());
    }
    let mut document = read_json_object(path)?;
    let key = match shape {
        McpConfigShape::Standard => "mcpServers",
        McpConfigShape::VsCode => "servers",
    };
    let ownership_path = mcp_ownership_path(path, key);
    let Some(ownership) = read_mcp_ownership(&ownership_path)? else {
        return Err(format!(
            "refusing to remove an Acyclic entry without an ownership record from {}",
            path.display()
        ));
    };
    if ownership.version != 1
        || ownership.config_path != path
        || ownership.section != key
        || document
            .get(key)
            .and_then(Value::as_object)
            .and_then(|entries| entries.get("acyclic"))
            != Some(&ownership.installed)
    {
        return Err(format!(
            "the Acyclic entry in {} was modified; leaving it untouched",
            path.display()
        ));
    }
    if let Some(Value::Object(entries)) = document.get_mut(key) {
        match ownership.prior {
            Some(prior) => {
                entries.insert("acyclic".to_owned(), prior);
            }
            None => {
                entries.remove("acyclic");
            }
        }
    }
    write_json_with_backup(path, &Value::Object(document))?;
    fs::remove_file(&ownership_path).map_err(display)
}
