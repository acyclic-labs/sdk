//! Installation uses native host configuration and never changes its sandbox.
use super::*;
use acyclic_native_runtime::host_command;
use std::fs;

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
            json!({"host":host,"minimum_codex":acyclic_harness::host_hooks::MINIMUM_CODEX_VERSION,"transport":"workspace files","writable_paths":[".acyclic-sdk/host-files/requests",".acyclic-sdk/host-files/status",".acyclic-sdk/host-processes",".volumes/agents"],"additional_sandbox_roots":[]})
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
                if !codex_marketplace(&package)? {
                    codex(&[
                        "plugin",
                        "marketplace",
                        "add",
                        package.to_str().ok_or("non-Unicode plugin path")?,
                    ])?;
                }
                install_codex_plugin(&package)?;
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
                host_directory("CLAUDE_CONFIG_DIR", ".claude")?.join("settings.json"),
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
        "claude-code",
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
                host_directory("PI_CODING_AGENT_DIR", ".pi/agent")?.join("extensions/acyclic.ts"),
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
        let recovery = serde_json::to_string(&(
            executable,
            "install",
            "pi",
            "--plugin-root",
            package,
            "--settings",
            &path,
        ))?;
        let source = fs::read_to_string(package.join("pi/index.ts")).map_err(|error| {
            format!("step=admit Pi extension asset: {error}; recovery argv: {recovery}")
        })?;
        let client =
            fs::read_to_string(package.join("shared/mcp-client.mjs")).map_err(|error| {
                format!("step=admit Pi coordination asset: {error}; recovery argv: {recovery}")
            })?;
        let (before, after) = source
            .split_once("import { McpClient } from \"../shared/mcp-client.mjs\";")
            .ok_or_else(|| format!("step=compose Pi extension: missing coordination import; recovery argv: {recovery}"))?;
        // Pi loads one owned extension. Embed the canonical transport before
        // the original atomic publication, without installing another path.
        let source = format!("{before}{client}{after}");
        let source = source.replace(
            "\"__ACYCLIC_EXECUTABLE__\"",
            &serde_json::to_string(&executable)?,
        );
        write_config("pi", &path, source.as_bytes(), original.as_deref())?;
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

fn host_directory(variable: &str, default: &str) -> Result<PathBuf> {
    env::var_os(variable)
        .filter(|path| !path.is_empty())
        .map_or_else(|| Ok(home()?.join(default)), |path| Ok(path.into()))
}

fn check_codex() -> Result<()> {
    let output = codex_output(&["--version"])?;
    Ok(acyclic_harness::host_hooks::check_codex_version(
        std::str::from_utf8(&output.stdout)?,
    )?)
}

fn codex(args: &[&str]) -> Result<()> {
    use std::io::Write as _;
    let output = codex_output(args)?;
    io::stdout().write_all(&output.stdout)?;
    io::stderr().write_all(&output.stderr)?;
    Ok(())
}

fn codex_output(args: &[&str]) -> Result<std::process::Output> {
    use acyclic_native_runtime::{
        CaptureSchedule, ProcessCommand, ProcessInput, ProcessStdio as Stdio, ProcessTree,
    };
    let recovery = format!(
        "recovery argv: {}",
        serde_json::to_string(
            &std::iter::once("codex")
                .chain(args.iter().copied())
                .collect::<Vec<_>>()
        )?
    );
    let resolved = host_command("codex")?;
    let mut command = ProcessCommand::new(resolved.get_program());
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = ProcessTree::spawn(&mut command)
        .map_err(|error| format!("step=start Codex native command: {error}; {recovery}"))?;
    let output = child
        .wait_with_output(
            CaptureSchedule::Unbounded,
            usize::MAX,
            None,
            || Ok(()),
            |_| Ok(ProcessInput::default()),
            None,
            None,
            None,
        )
        .map_err(|failure| {
            format!(
                "step=complete Codex native command: {}; physical_cleanup_completed={}; {recovery}",
                failure.error, failure.cleanup_completed
            )
        })?;
    if !output.status.success() {
        return Err(format!(
            "step=Codex native command: {}; stdout={}; stderr={}; {recovery}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(output)
}

/// Discovery is read-only. Native marketplace add rewrites configuration even
/// for an existing source, so it cannot serve as the admission check.
fn codex_marketplace(package: &Path) -> Result<bool> {
    let recovery = "recovery=codex plugin marketplace list --json";
    let output = codex_output(&["plugin", "marketplace", "list", "--json"])?;
    let listed: Value = serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("step=decode Codex marketplaces: {error}; {recovery}"))?;
    let entries = listed
        .get("marketplaces")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            format!("step=admit Codex marketplaces: marketplace list absent; {recovery}")
        })?;
    let mut selected = None;
    for entry in entries {
        let name = entry.get("name").and_then(Value::as_str).ok_or_else(|| {
            format!("step=admit Codex marketplace: native marketplace name absent; {recovery}")
        })?;
        if name == "acyclic" && selected.replace(entry).is_some() {
            return Err(format!(
                "step=admit Codex marketplace: NATIVE_MARKETPLACE_AMBIGUOUS; {recovery}"
            )
            .into());
        }
    }
    let Some(entry) = selected else {
        return Ok(false);
    };
    let source = entry.get("marketplaceSource");
    let root = entry.get("root").and_then(Value::as_str);
    let original = source
        .and_then(|source| source.get("source"))
        .and_then(Value::as_str);
    let local = source
        .and_then(|source| source.get("sourceType"))
        .and_then(Value::as_str)
        == Some("local");
    let Some((root, original)) = root.zip(original).filter(|_| local) else {
        return Err(format!(
            "step=admit Codex marketplace: NATIVE_MARKETPLACE_SOURCE_UNAVAILABLE; {recovery}"
        )
        .into());
    };
    let original = if original == root {
        None
    } else {
        Some(fs::canonicalize(original).map_err(|error| {
            format!("step=resolve Codex marketplace source: {error}; {recovery}")
        })?)
    };
    let root = fs::canonicalize(root)
        .map_err(|error| format!("step=resolve Codex marketplace root: {error}; {recovery}"))?;
    if root != package || original.as_deref().is_some_and(|source| source != package) {
        let source = original.as_deref().unwrap_or(&root);
        return Err(format!("step=admit Codex marketplace: NATIVE_MARKETPLACE_SOURCE_MISMATCH; existing registration retained; recovery argv: {}", serde_json::to_string(&("acyclic", "install", "codex", "--plugin-root", source))?).into());
    }
    Ok(true)
}

/// The native install request names the admitted manifest itself, so a later
/// registry change cannot redirect installation to a namesake marketplace.
/// Install and hook trust use one initialized app-server and config writer.
fn install_codex_plugin(package: &Path) -> Result<()> {
    use acyclic_native_runtime::{
        CaptureInput, CaptureSchedule, ProcessCommand, ProcessInput, ProcessOutput,
        ProcessStdio as Stdio, ProcessTree,
    };
    let recovery = format!(
        "recovery argv: {}",
        serde_json::to_string(&("acyclic", "install", "codex", "--plugin-root", package))?
    );
    let resolved = host_command("codex")?;
    let mut command = ProcessCommand::new(resolved.get_program());
    command
        .arg("app-server")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let mut server = ProcessTree::spawn(&mut command)
        .map_err(|error| format!("step=start Codex app-server: {error}; {recovery}"))?;
    let initial = codex_frame(
        1,
        "initialize",
        json!({"clientInfo": {"name": "acyclic", "title": null, "version": env!("CARGO_PKG_VERSION")}, "capabilities": null}),
    )?;
    let mut expected = 1;
    let mut line = Vec::new();
    let mut committed = false;
    // The shared pump owns all delivery, output, root-exit and physical-close
    // events. A final acknowledgement ends containment; stdin EOF is not an
    // obligation for an external service to honour before we can reap it.
    let outcome = server.wait_with_output(
        CaptureSchedule::Unbounded, usize::MAX,
        Some(CaptureInput::Exchange {initial: &initial, maximum: usize::MAX}),
        || Ok(()),
        |output| {
            let ProcessOutput::Stdout(bytes) = output else {return Ok(ProcessInput::default());};
            let mut next = Vec::new();
            for fragment in bytes.split_inclusive(|byte| *byte == b'\n') {
                line.extend_from_slice(fragment);
                if fragment.last() != Some(&b'\n') {continue;}
                let message: Value = serde_json::from_slice(&line).map_err(io::Error::other)?;
                line.clear();
                let Some(id) = message.get("id") else {continue;};
                if id != &json!(expected) {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected Codex app-server response identity"));
                }
                if let Some(error) = message.get("error") {
                    return Err(io::Error::other(format!("Codex request {expected}: {error}")));
                }
                let result = message.get("result").ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Codex response lacks result"))?;
                match expected {
                    1 => {
                        next.extend_from_slice(b"{\"method\":\"initialized\"}\n");
                        next.extend(codex_frame(2, "plugin/install", json!({"pluginName": "acyclic", "marketplacePath": package.join(".agents/plugins/marketplace.json")})).map_err(io::Error::other)?);
                        expected = 2;
                    }
                    2 => {
                        if result.get("authPolicy").and_then(Value::as_str).is_none() || result.get("appsNeedingAuth").and_then(Value::as_array).is_none() {
                            return Err(io::Error::new(io::ErrorKind::InvalidData, "Codex plugin/install acknowledgement lacks its native authentication disposition"));
                        }
                        next.extend(codex_frame(3, "hooks/list", json!({})).map_err(io::Error::other)?);
                        expected = 3;
                    }
                    3 => {
                        let trust = codex_hook_trust(result).map_err(io::Error::other)?;
                        if trust.is_empty() {committed = true;} else {
                            next.extend(codex_frame(4, "config/batchWrite", json!({"edits": [{"keyPath": "hooks.state", "value": trust, "mergeStrategy": "upsert"}]})).map_err(io::Error::other)?);
                            expected = 4;
                        }
                    }
                    _ => committed = true,
                }
                if committed {break;}
            }
            Ok(ProcessInput {bytes: next, complete: committed})
        }, None, None, None,
    ).map_err(|failure| format!("step=Codex hook trust exchange: {}; physical_cleanup_completed={}; {recovery}", failure.error, failure.cleanup_completed))?;
    if !committed {
        return Err(format!("step=Codex hook trust exchange: root exited {} before request {expected} acknowledgement; {recovery}", outcome.status).into());
    }
    Ok(())
}

fn codex_frame(id: u64, method: &str, params: Value) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec(&json!({"id": id, "method": method, "params": params}))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn codex_hook_trust(listed: &Value) -> Result<serde_json::Map<String, Value>> {
    let data = listed
        .get("data")
        .and_then(Value::as_array)
        .ok_or("Codex hooks/list lacks data")?;
    let mut trust = serde_json::Map::new();
    let mut selected = false;
    for entry in data {
        for hook in entry
            .get("hooks")
            .and_then(Value::as_array)
            .ok_or("Codex hook entry lacks hooks")?
        {
            if hook.get("pluginId") != Some(&json!("acyclic@acyclic")) {
                continue;
            }
            selected = true;
            if hook.get("trustStatus") == Some(&json!("trusted")) {
                continue;
            }
            let key = hook
                .get("key")
                .and_then(Value::as_str)
                .ok_or("Codex hook lacks key")?;
            let hash = hook
                .get("currentHash")
                .and_then(Value::as_str)
                .ok_or("Codex hook lacks hash")?;
            trust.insert(key.to_owned(), json!({"trusted_hash": hash}));
        }
    }
    if !selected {
        return Err("Codex hooks/list lacks installed acyclic@acyclic hooks".into());
    }
    Ok(trust)
}
fn optional(path: &Path) -> Result<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub(super) fn write_config(
    host: &str,
    path: &Path,
    bytes: &[u8],
    original: Option<&[u8]>,
) -> Result<()> {
    if original == Some(bytes) {
        return Ok(());
    }
    let recovery = serde_json::to_string(&("acyclic", "install", host, "--settings", path))?;
    if optional(path)?.as_deref() != original {
        return Err(
            format!("install configuration: source changed; recovery argv: {recovery}").into(),
        );
    }
    let parent = path.parent().ok_or("configuration path needs a parent")?;
    let failure = |step: &str, error: io::Error| {
        format!("install configuration: {step}: {error}; recovery argv: {recovery}")
    };
    fs::create_dir_all(parent).map_err(|error| failure("create settings directory", error))?;
    use std::io::Write as _;
    let mut staged =
        acyclic_native_runtime::private_file::StagedFile::create(parent, ".acyclic-install-")
            .map_err(|error| failure("create private staging file", error))?;
    staged
        .as_file_mut()
        .write_all(bytes)
        .map_err(|error| failure("write staged settings", error))?;
    Ok(staged
        .publish(path, acyclic_native_runtime::RenameMode::Replace)
        .map_err(|error| failure("publish staged settings", error))?)
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
