//! Codex plugin installation and discovery.

use super::*;

pub(crate) fn install_codex_plugin() -> Result<(), String> {
    let manifest_root = plugin_root()?;
    let marketplace = manifest_root.join(".agents/plugins/marketplace.json");
    if !marketplace.is_file() {
        return Err(format!(
            "Codex marketplace manifest is missing at {}",
            marketplace.display()
        ));
    }
    let marketplaces = codex_json(&["plugin", "marketplace", "list", "--json"])?;
    let configured = marketplaces
        .get("marketplaces")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find(|entry| entry.get("name").and_then(Value::as_str) == Some("acyclic"));
    if let Some(configured) = configured {
        let configured_root = configured
            .get("root")
            .and_then(Value::as_str)
            .ok_or_else(|| "configured Acyclic marketplace has no root".to_owned())?;
        let same = Path::new(configured_root)
            .canonicalize()
            .ok()
            .zip(manifest_root.canonicalize().ok())
            .is_some_and(|(configured, package)| configured == package);
        if !same {
            return Err(format!(
                "refusing to replace the Acyclic marketplace owned by {configured_root}"
            ));
        }
    }
    let installed_before = installed_codex_plugin()?;
    let plugin_was_installed = installed_before.is_some();
    if plugin_was_installed && configured.is_none() {
        return Err(
            "Codex reports an installed Acyclic plugin without its marketplace; refusing mutation"
                .to_owned(),
        );
    }
    if let Some(installed) = installed_before.as_ref() {
        validate_existing_codex_plugin(installed)?;
    }
    let added_marketplace = configured.is_none();
    let ownership_path = codex_ownership_path();
    let existing_ownership = read_codex_ownership(&ownership_path)?;
    let mut ownership = if let Some(ownership) = existing_ownership.as_ref() {
        let same = ownership
            .marketplace_root
            .canonicalize()
            .ok()
            .zip(manifest_root.canonicalize().ok())
            .is_some_and(|(owned, package)| owned == package);
        if ownership.version != 1 || !same {
            return Err("existing Codex integration ownership is inconsistent".to_owned());
        }
        CodexPluginOwnership {
            version: ownership.version,
            marketplace_root: ownership.marketplace_root.clone(),
            added_marketplace: ownership.added_marketplace,
            plugin_was_installed: ownership.plugin_was_installed,
            config_path: ownership.config_path.clone(),
            prior_default_permissions: ownership.prior_default_permissions.clone(),
        }
    } else {
        CodexPluginOwnership {
            version: 1,
            marketplace_root: manifest_root.clone(),
            added_marketplace,
            plugin_was_installed,
            config_path: None,
            prior_default_permissions: None,
        }
    };
    write_codex_ownership(&ownership_path, &ownership)?;
    let install = (|| {
        if added_marketplace {
            run_codex(&[
                "plugin",
                "marketplace",
                "add",
                manifest_root.to_string_lossy().as_ref(),
            ])?;
        }
        if !plugin_was_installed {
            run_codex(&["plugin", "add", "acyclic@acyclic", "--json"])?;
        }
        let installed = codex_json(&["plugin", "list", "--marketplace", "acyclic", "--json"])?;
        if !installed
            .get("installed")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .any(|entry| {
                entry.get("pluginId").and_then(Value::as_str) == Some("acyclic@acyclic")
                    && entry.get("version").and_then(Value::as_str)
                        == Some(env!("CARGO_PKG_VERSION"))
                    && entry.get("installed").and_then(Value::as_bool) == Some(true)
                    && entry.get("enabled").and_then(Value::as_bool) == Some(true)
            })
        {
            return Err("Codex did not activate the expected Acyclic release".to_owned());
        }
        let plan = plan_codex_config(&ownership)?;
        ownership.config_path = Some(plan.path.clone());
        ownership
            .prior_default_permissions
            .clone_from(&plan.prior_default_permissions);
        write_codex_ownership(&ownership_path, &ownership)?;
        apply_codex_config(&plan)?;
        Ok(())
    })();
    if let Err(error) = install {
        let _ = restore_codex_config(&ownership);
        if !plugin_was_installed {
            let _ = run_codex(&["plugin", "remove", "acyclic@acyclic", "--json"]);
        }
        if added_marketplace {
            let _ = run_codex(&["plugin", "marketplace", "remove", "acyclic"]);
        }
        match existing_ownership.as_ref() {
            Some(existing) => {
                let _ = write_codex_ownership(&ownership_path, existing);
            }
            None => {
                let _ = fs::remove_file(&ownership_path);
            }
        }
        return Err(format!("Codex plugin installation rolled back: {error}"));
    }
    println!("installed Acyclic through the Codex plugin manager");
    Ok(())
}

pub(crate) fn run_codex(arguments: &[&str]) -> Result<(), String> {
    let status = acyclic_native_runtime::process_status(
        std::process::Command::new("codex").args(arguments),
        std::time::Duration::from_secs(120),
    )
    .map_err(|error| format!("cannot run Codex plugin manager: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("Codex plugin manager exited with {status}"))
    }
}

pub(crate) fn installed_codex_plugin() -> Result<Option<Value>, String> {
    let plugins = codex_json(&["plugin", "list", "--json"])?;
    let installed = plugins
        .get("installed")
        .and_then(Value::as_array)
        .ok_or_else(|| "Codex plugin list has no installed array".to_owned())?;
    let matches = installed
        .iter()
        .filter(|entry| entry.get("pluginId").and_then(Value::as_str) == Some("acyclic@acyclic"))
        .cloned()
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [] => Ok(None),
        [plugin] => Ok(Some(plugin.clone())),
        _ => Err("Codex reported duplicate installed Acyclic plugins".to_owned()),
    }
}

pub(crate) fn validate_existing_codex_plugin(installed: &Value) -> Result<(), String> {
    let version = installed
        .get("version")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let enabled = installed
        .get("enabled")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if version == env!("CARGO_PKG_VERSION") && enabled {
        Ok(())
    } else {
        Err(format!(
            "Codex already has Acyclic {version} (enabled={enabled}); refusing to replace prior plugin state. Update the local package and restart Codex, or explicitly remove the old plugin before retrying"
        ))
    }
}

pub(crate) fn plugin_root() -> Result<PathBuf, String> {
    let executable = current_executable()?;
    let installed = installed_plugin_root()?;
    discover_plugin_root(&executable, &installed)
        .ok_or_else(|| "cannot locate the installed Acyclic plugin root".to_owned())
}

pub(crate) fn discover_plugin_root(executable: &Path, installed: &Path) -> Option<PathBuf> {
    executable
        .ancestors()
        .take(4)
        .chain(std::iter::once(installed))
        .find(|root| {
            root.join("plugin.json").is_file()
                && root.join(".agents/plugins/marketplace.json").is_file()
        })
        .map(Path::to_path_buf)
}

pub(crate) fn installed_plugin_root() -> Result<PathBuf, String> {
    #[cfg(windows)]
    {
        env::var_os("LOCALAPPDATA")
            .map(PathBuf::from)
            .map(|path| path.join("Acyclic/plugin"))
            .ok_or_else(|| "LOCALAPPDATA is unavailable".to_owned())
    }
    #[cfg(not(windows))]
    {
        let data = env::var_os("XDG_DATA_HOME")
            .map(PathBuf::from)
            .unwrap_or(home_directory()?.join(".local/share"));
        Ok(data.join("acyclic/plugin"))
    }
}
