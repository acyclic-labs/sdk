//! Installation uses native host configuration and never changes its sandbox.
use super::*;
use std::{fs, process::Command};

pub(super) fn run(args: &[String]) -> Result<()> {
    let command = args
        .first()
        .map(String::as_str)
        .ok_or("missing installer command")?;
    let host = args
        .get(1)
        .map(String::as_str)
        .ok_or("specify codex, claude-code or pi")?;
    if !matches!(host, "codex" | "claude-code" | "pi") {
        return Err("unknown native host".into());
    }
    for pair in args.get(2..).unwrap_or_default().chunks(2) {
        if !matches!(pair, [name, _] if matches!(name.as_str(), "--plugin-root" | "--settings")) {
            return Err("installer options: --plugin-root PATH, --settings PATH".into());
        }
    }
    if host == "codex" {
        check_codex()?;
    }
    if command == "doctor" {
        println!(
            "{}",
            json!({"host":host,"minimum_codex":acyclic_harness::host_hooks::MINIMUM_CODEX_VERSION,"transport":"workspace files","writable_paths":[".acyclic-sdk/host-files/requests",".volumes/agents"],"additional_sandbox_roots":[]})
        );
        return Ok(());
    }
    let executable = env::current_exe()?.canonicalize()?;
    let package = option(args, "--plugin-root")
        .map_or_else(
            || {
                executable
                    .parent()
                    .and_then(Path::parent)
                    .map(Path::to_path_buf)
            },
            Some,
        )
        .ok_or("specify --plugin-root PATH")?
        .canonicalize()?;
    let install = command == "install";
    match host {
        "codex" => {
            if install {
                if !package.join(".agents/plugins/marketplace.json").is_file() {
                    return Err("plugin marketplace manifest is missing".into());
                }
                codex(&[
                    "plugin",
                    "marketplace",
                    "add",
                    package.to_str().ok_or("non-Unicode plugin path")?,
                ])?;
                codex(&["plugin", "add", "acyclic@acyclic", "--json"])?;
            } else {
                codex(&["plugin", "remove", "acyclic@acyclic", "--json"])?;
            }
        }
        "claude-code" => install_claude(args, &executable, install)?,
        "pi" => install_pi(args, &package, &executable, install)?,
        _ => unreachable!(),
    }
    println!(
        "{command} {host}: workspace-local files only; native permissions and sandbox retained"
    );
    Ok(())
}

fn install_claude(args: &[String], executable: &Path, install: bool) -> Result<()> {
    let path = option(args, "--settings").map_or_else(
        || {
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                env::var_os("CLAUDE_CONFIG_DIR")
                    .map(PathBuf::from)
                    .unwrap_or(home()?.join(".claude"))
                    .join("settings.json"),
            )
        },
        Ok,
    )?;
    let original = optional(&path)?;
    let mut document = original
        .as_deref()
        .map(serde_json::from_slice)
        .transpose()?
        .unwrap_or(json!({}));
    update_claude(&mut document, &executable.to_string_lossy(), install)?;
    write_config(
        &path,
        &serde_json::to_vec_pretty(&document)?,
        original.as_deref(),
    )?;
    Ok(())
}

fn install_pi(args: &[String], package: &Path, executable: &Path, install: bool) -> Result<()> {
    let path = option(args, "--settings").map_or_else(
        || {
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                home()?.join(".pi/agent/extensions/acyclic.ts"),
            )
        },
        Ok,
    )?;
    let original = optional(&path)?;
    if original
        .as_ref()
        .is_some_and(|bytes| !bytes.starts_with(b"// Acyclic native workspace shim"))
    {
        return Err("refusing to replace an extension not owned by Acyclic".into());
    }
    if install {
        let source = fs::read_to_string(package.join("pi/index.ts"))?;
        let source = source.replace(
            "\"__ACYCLIC_EXECUTABLE__\"",
            &serde_json::to_string(&executable)?,
        );
        write_config(&path, source.as_bytes(), original.as_deref())?;
    } else if original.is_some() {
        fs::remove_file(&path)?;
    }
    Ok(())
}

fn option(args: &[String], name: &str) -> Option<PathBuf> {
    args.windows(2).find_map(|pair| match pair {
        [option, path] if option == name => Some(PathBuf::from(path)),
        _ => None,
    })
}

fn home() -> Result<PathBuf> {
    env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
        .map(PathBuf::from)
        .ok_or_else(|| "host home is unset; provide --settings PATH".into())
}

fn check_codex() -> Result<()> {
    let output = Command::new("codex").arg("--version").output()?;
    if !output.status.success() {
        return Err("cannot identify Codex; requires Codex >= 0.164.0".into());
    }
    Ok(acyclic_harness::host_hooks::check_codex_version(
        std::str::from_utf8(&output.stdout)?,
    )?)
}

fn codex(args: &[&str]) -> Result<()> {
    let status = Command::new("codex").args(args).status()?;
    if !status.success() {
        return Err(format!("Codex plugin manager failed: {status}").into());
    }
    Ok(())
}

fn optional(path: &Path) -> Result<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn write_config(path: &Path, bytes: &[u8], original: Option<&[u8]>) -> Result<()> {
    if original == Some(bytes) {
        return Ok(());
    }
    if optional(path)?.as_deref() != original {
        return Err("native host configuration changed during installation; retry".into());
    }
    let parent = path.parent().ok_or("configuration path needs a parent")?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(
        ".acyclic-install-{}.next",
        acyclic_harness::OperationId::new()
    ));
    let result = (|| -> Result<()> {
        use std::io::Write as _;
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        acyclic_native_runtime::durable_rename(
            &temporary,
            path,
            acyclic_native_runtime::RenameMode::Replace,
            None,
        )?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

pub(super) fn update_claude(document: &mut Value, executable: &str, install: bool) -> Result<()> {
    let object = document
        .as_object_mut()
        .ok_or("Claude settings must be an object")?;
    let hooks = object
        .entry("hooks")
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .ok_or("Claude hooks must be an object")?;
    for event in acyclic_harness::host_hooks::CLAUDE_EVENTS {
        if let Some(groups) = hooks.get_mut(event) {
            let groups = groups
                .as_array_mut()
                .ok_or("Claude hook event must be an array")?;
            groups.retain(|group| {
                acyclic_harness::host_hooks::claude_hook_executable(event, group).is_none()
            });
        }
        if install {
            let groups = hooks
                .entry(event)
                .or_insert_with(|| json!([]))
                .as_array_mut()
                .ok_or("Claude hook event must be an array")?;
            groups.extend(
                acyclic_harness::host_hooks::claude_hooks(executable)
                    .remove(event)
                    .ok_or("missing shared Claude hook")?,
            );
        }
        if hooks
            .get(event)
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty)
        {
            hooks.remove(event);
        }
    }
    if hooks.is_empty() {
        object.remove("hooks");
    }
    Ok(())
}
