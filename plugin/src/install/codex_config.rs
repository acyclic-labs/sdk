//! Codex configuration ownership.

use super::*;

#[derive(Deserialize, Serialize)]
pub(crate) struct CodexPluginOwnership {
    pub(crate) version: u32,
    pub(crate) marketplace_root: PathBuf,
    pub(crate) added_marketplace: bool,
    pub(crate) plugin_was_installed: bool,
    pub(crate) config_path: Option<PathBuf>,
    pub(crate) prior_default_permissions: Option<String>,
}

pub(crate) struct CodexConfigPlan {
    pub(crate) path: PathBuf,
    pub(crate) prior_default_permissions: Option<String>,
    pub(crate) installed: String,
}

pub(crate) fn codex_ownership_path() -> PathBuf {
    default_data_directory().join("install-ownership/codex-plugin.json")
}

pub(crate) fn read_codex_ownership(path: &Path) -> Result<Option<CodexPluginOwnership>, String> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(display),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(display(error)),
    }
}

pub(crate) fn write_codex_ownership(
    path: &Path,
    ownership: &CodexPluginOwnership,
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

pub(crate) fn codex_config_path() -> Result<PathBuf, String> {
    Ok(env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .unwrap_or(home_directory()?.join(".codex"))
        .join("config.toml"))
}

pub(crate) fn codex_plugin_configuration_matches(
    ownership: &CodexPluginOwnership,
    packaged_root: &Path,
) -> bool {
    let Some(config_path) = ownership.config_path.as_ref() else {
        return false;
    };
    let Ok(document) = fs::read_to_string(config_path)
        .map_err(display)
        .and_then(|text| text.parse::<toml_edit::DocumentMut>().map_err(display))
    else {
        return false;
    };
    let marketplace = document
        .get("marketplaces")
        .and_then(toml_edit::Item::as_table)
        .and_then(|marketplaces| marketplaces.get("acyclic"))
        .and_then(toml_edit::Item::as_table);
    let configured_root = marketplace
        .and_then(|marketplace| marketplace.get("source"))
        .and_then(toml_edit::Item::as_str)
        .map(Path::new);
    let source_is_local = marketplace
        .and_then(|marketplace| marketplace.get("source_type"))
        .and_then(toml_edit::Item::as_str)
        == Some("local");
    let enabled = document
        .get("plugins")
        .and_then(toml_edit::Item::as_table)
        .and_then(|plugins| plugins.get("acyclic@acyclic"))
        .and_then(toml_edit::Item::as_table)
        .and_then(|plugin| plugin.get("enabled"))
        .and_then(toml_edit::Item::as_bool)
        == Some(true);
    source_is_local
        && enabled
        && configured_root
            .and_then(|configured| configured.canonicalize().ok())
            .zip(packaged_root.canonicalize().ok())
            .is_some_and(|(configured, packaged)| configured == packaged)
}

pub(crate) fn read_optional_text(path: &Path) -> Result<Option<String>, String> {
    match fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(display(error)),
    }
}

pub(crate) fn toml_table<'a>(
    parent: &'a mut toml_edit::Table,
    key: &str,
) -> Result<&'a mut toml_edit::Table, String> {
    parent
        .entry(key)
        .or_insert_with(|| toml_edit::Item::Table(toml_edit::Table::new()))
        .as_table_mut()
        .ok_or_else(|| format!("Codex configuration field '{key}' must be a table"))
}

pub(crate) fn codex_permission_profile(
    data: &Path,
    base: &str,
) -> Result<toml_edit::Table, String> {
    let mut profile = toml_edit::Table::new();
    profile.insert("extends", toml_edit::value(base));
    let roots = toml_table(&mut profile, "workspace_roots")?;
    roots.insert(
        data.join("w").to_string_lossy().as_ref(),
        toml_edit::value(true),
    );
    #[cfg(unix)]
    {
        let runtime = unix_control_runtime_directory();
        roots.insert(runtime.to_string_lossy().as_ref(), toml_edit::value(true));
    }
    #[cfg(windows)]
    {
        let filesystem = toml_table(&mut profile, "filesystem")?;
        filesystem.insert(":minimal", toml_edit::value("read"));
        toml_table(filesystem, ":workspace_roots")?.insert(".", toml_edit::value("write"));
    }
    Ok(profile)
}

pub(crate) fn codex_profile_matches(
    profile: &toml_edit::Table,
    data: &Path,
    base_profile: &str,
) -> bool {
    let roots = profile
        .get("workspace_roots")
        .and_then(toml_edit::Item::as_table);
    #[cfg(unix)]
    {
        let runtime = unix_control_runtime_directory();
        profile.len() == 2
            && profile.get("extends").and_then(toml_edit::Item::as_str) == Some(base_profile)
            && roots.is_some_and(|roots| {
                roots.len() == 2
                    && roots
                        .get(data.join("w").to_string_lossy().as_ref())
                        .and_then(toml_edit::Item::as_bool)
                        == Some(true)
                    && roots
                        .get(runtime.to_string_lossy().as_ref())
                        .and_then(toml_edit::Item::as_bool)
                        == Some(true)
            })
    }
    #[cfg(not(unix))]
    {
        let filesystem = profile
            .get("filesystem")
            .and_then(toml_edit::Item::as_table);
        profile.len() == 3
            && profile.get("extends").and_then(toml_edit::Item::as_str) == Some(base_profile)
            && roots.is_some_and(|roots| {
                roots.len() == 1
                    && roots
                        .get(data.join("w").to_string_lossy().as_ref())
                        .and_then(toml_edit::Item::as_bool)
                        == Some(true)
            })
            && filesystem.is_some_and(|filesystem| {
                filesystem.len() == 2
                    && filesystem.get(":minimal").and_then(toml_edit::Item::as_str) == Some("read")
                    && filesystem
                        .get(":workspace_roots")
                        .and_then(toml_edit::Item::as_table)
                        .is_some_and(|roots| {
                            roots.len() == 1
                                && roots.get(".").and_then(toml_edit::Item::as_str) == Some("write")
                        })
            })
    }
}

pub(crate) fn plan_codex_config(
    ownership: &CodexPluginOwnership,
) -> Result<CodexConfigPlan, String> {
    let path = codex_config_path()?;
    let mut document = read_optional_text(&path)?
        .as_deref()
        .unwrap_or("")
        .parse::<toml_edit::DocumentMut>()
        .map_err(|error| format!("invalid Codex configuration at {}: {error}", path.display()))?;
    let root = document.as_table_mut();
    let base_profile = ownership
        .prior_default_permissions
        .as_deref()
        .unwrap_or(":workspace");
    if let Some(owned_path) = ownership.config_path.as_ref() {
        let installed_profile = root
            .get("permissions")
            .and_then(toml_edit::Item::as_table)
            .and_then(|permissions| permissions.get("acyclic"))
            .and_then(toml_edit::Item::as_table);
        let owned = owned_path == &path
            && root
                .get("default_permissions")
                .and_then(toml_edit::Item::as_str)
                == Some("acyclic")
            && installed_profile.is_some_and(|profile| {
                codex_profile_matches(profile, &default_data_directory(), base_profile)
            });
        if !owned {
            return Err(format!(
                "Codex configuration ownership at {} is inconsistent; refusing to overwrite it",
                path.display()
            ));
        }
        return Ok(CodexConfigPlan {
            path,
            prior_default_permissions: ownership.prior_default_permissions.clone(),
            installed: document.to_string(),
        });
    }
    if ownership.prior_default_permissions.is_some() {
        return Err("Codex configuration ownership record is incomplete".to_owned());
    }
    let prior_default_permissions = root
        .get("default_permissions")
        .map(|item| {
            item.as_str()
                .map(str::to_owned)
                .ok_or_else(|| "Codex default_permissions must be a string".to_owned())
        })
        .transpose()?;
    let expected_profile = codex_permission_profile(
        &default_data_directory(),
        prior_default_permissions.as_deref().unwrap_or(":workspace"),
    )?;
    if toml_table(root, "permissions")?.contains_key("acyclic") {
        return Err(format!(
            "refusing to replace the unowned 'acyclic' permission profile in {}",
            path.display()
        ));
    }
    root.insert("default_permissions", toml_edit::value("acyclic"));
    toml_table(root, "permissions")?.insert("acyclic", toml_edit::Item::Table(expected_profile));
    Ok(CodexConfigPlan {
        path,
        prior_default_permissions,
        installed: document.to_string(),
    })
}

pub(crate) fn apply_codex_config(plan: &CodexConfigPlan) -> Result<(), String> {
    write_text_with_backup(&plan.path, &plan.installed)
}

pub(crate) fn restore_codex_config(ownership: &CodexPluginOwnership) -> Result<(), String> {
    let Some(path) = ownership.config_path.as_ref() else {
        return Ok(());
    };
    let mut document = read_optional_text(path)?
        .as_deref()
        .unwrap_or("")
        .parse::<toml_edit::DocumentMut>()
        .map_err(|error| format!("invalid Codex configuration at {}: {error}", path.display()))?;
    let root = document.as_table_mut();
    let base_profile = ownership
        .prior_default_permissions
        .as_deref()
        .unwrap_or(":workspace");
    let owned = root
        .get("default_permissions")
        .and_then(toml_edit::Item::as_str)
        == Some("acyclic")
        && root
            .get("permissions")
            .and_then(toml_edit::Item::as_table)
            .and_then(|permissions| permissions.get("acyclic"))
            .and_then(toml_edit::Item::as_table)
            .is_some_and(|profile| {
                codex_profile_matches(profile, &default_data_directory(), base_profile)
            });
    if !owned {
        let restored_default = match ownership.prior_default_permissions.as_deref() {
            Some(prior) => {
                root.get("default_permissions")
                    .and_then(toml_edit::Item::as_str)
                    == Some(prior)
            }
            None => !root.contains_key("default_permissions"),
        };
        let profile_absent = root
            .get("permissions")
            .and_then(toml_edit::Item::as_table)
            .is_none_or(|permissions| !permissions.contains_key("acyclic"));
        if restored_default && profile_absent {
            return Ok(());
        }
        return Err(format!(
            "the Acyclic Codex permission profile at {} was modified; leaving it untouched",
            path.display()
        ));
    }
    match ownership.prior_default_permissions.as_deref() {
        Some(prior) => {
            root.insert("default_permissions", toml_edit::value(prior));
        }
        None => {
            root.remove("default_permissions");
        }
    }
    let permissions = toml_table(root, "permissions")?;
    permissions.remove("acyclic");
    write_text_with_backup(path, &document.to_string())
}
