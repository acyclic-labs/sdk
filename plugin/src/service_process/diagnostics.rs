//! Installed host and platform diagnostics.

use super::*;

pub(crate) fn doctor_check(name: &str, status: &str, detail: impl Into<String>) -> Value {
    json!({"name": name, "status": status, "detail": detail.into()})
}

pub(crate) fn valid_platform_receipt(receipt: &Value, executable_blake3: &str) -> bool {
    const COVERAGE: &[&str] = &[
        "create-read-write",
        "atomic-save",
        "rename-delete",
        "rename-before-hydration",
        "nested-paths",
        "large-directory-paging",
        "concurrent-handles",
        "watchers",
        "crash-detach-recovery",
        "mount-restoration",
        "hard-links",
        "symbolic-links-reparse-points",
        "metadata",
        "case-behavior",
        "escape-attempts",
        "root-checkout-untouched",
        "git-administration-untouched",
    ];
    const CASES: &[&str] = &[
        "real-mount-mutation-matrix",
        "crash-detach-recovery",
        "checkout-and-git-untouched",
    ];
    let (required_kind, provider_process_io_observable) = match env::consts::OS {
        "linux" => ("linux-fuse", true),
        "macos" => ("macos-nfs", true),
        "windows" => ("windows-projfs", false),
        _ => return false,
    };
    let Some(document) = receipt.as_object() else {
        return false;
    };
    let expected_keys = [
        "schema",
        "os",
        "arch",
        "coverage",
        "capability",
        "required_kind",
        "release_version",
        "executable_blake3",
        "passed",
        "cases",
    ];
    if document.len() != expected_keys.len()
        || expected_keys.iter().any(|key| !document.contains_key(*key))
        || receipt.get("schema").and_then(Value::as_str)
            != Some("acyclic-native-mount-qualification-v2")
        || receipt.get("os").and_then(Value::as_str) != Some(env::consts::OS)
        || receipt.get("arch").and_then(Value::as_str) != Some(env::consts::ARCH)
        || receipt.get("required_kind").and_then(Value::as_str) != Some(required_kind)
        || receipt.get("release_version").and_then(Value::as_str) != Some(env!("CARGO_PKG_VERSION"))
        || receipt.get("executable_blake3").and_then(Value::as_str) != Some(executable_blake3)
        || receipt.get("passed").and_then(Value::as_bool) != Some(true)
    {
        return false;
    }
    let coverage = receipt.get("coverage").and_then(Value::as_array);
    if coverage.is_none_or(|values| {
        let observed = values
            .iter()
            .map(Value::as_str)
            .collect::<Option<BTreeSet<_>>>();
        observed.is_none_or(|observed| {
            observed.len() != values.len()
                || COVERAGE.iter().any(|expected| !observed.contains(expected))
        })
    }) {
        return false;
    }
    let capability = receipt.get("capability");
    if capability
        .and_then(|value| value.get("kind"))
        .and_then(Value::as_str)
        != Some(required_kind)
        || capability
            .and_then(|value| value.get("available"))
            .and_then(Value::as_bool)
            != Some(true)
        || capability
            .and_then(|value| value.get("writable"))
            .and_then(Value::as_bool)
            != Some(true)
        || capability
            .and_then(|value| value.get("provider_process_io_observable"))
            .and_then(Value::as_bool)
            != Some(provider_process_io_observable)
        || capability
            .and_then(|value| value.get("session_isolation"))
            .and_then(Value::as_str)
            != Some("SharedProcess")
        || capability
            .and_then(|value| value.get("unavailable_reason"))
            .is_none_or(|value| !value.is_null())
    {
        return false;
    }
    receipt
        .get("cases")
        .and_then(Value::as_array)
        .is_some_and(|cases| {
            let observed = cases
                .iter()
                .filter_map(|case| case.get("name").and_then(Value::as_str))
                .collect::<BTreeSet<_>>();
            observed.len() == cases.len()
                && CASES.iter().all(|expected| observed.contains(expected))
                && cases.iter().all(|case| {
                    case.get("status").and_then(Value::as_str) == Some("passed")
                        && case.get("reason").is_some_and(Value::is_null)
                })
        })
}

pub(crate) fn codex_json(arguments: &[&str]) -> Result<Value, String> {
    let output = acyclic_native_runtime::process_output(
        host_command("codex")?.args(arguments),
        std::time::Duration::from_secs(120),
        8 * 1024 * 1024,
    )
    .map_err(|error| format!("cannot run Codex plugin manager: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "Codex plugin manager exited with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    serde_json::from_slice(&output.stdout).map_err(display)
}

#[allow(
    clippy::too_many_arguments,
    reason = "each argument is one independently gathered service fact"
)]
/// What the running service reports about itself to doctor.
pub(crate) struct ServiceSummary {
    pub(crate) identity: String,
    pub(crate) sessions: usize,
    pub(crate) routes: usize,
    pub(crate) leases: usize,
    pub(crate) pending_recovery: bool,
}

/// The doctor report. `service` is the running service's summary, or why no
/// service could answer: the report is still complete then, so doctor
/// diagnoses the installation exactly when the service cannot start.
pub(crate) fn doctor_report(
    data: &Path,
    service: Result<&ServiceSummary, &str>,
    executable_sha256: &str,
    executable_blake3: &str,
) -> Result<Value, String> {
    let routes = service.map_or(0, |service| service.routes);
    let executable = current_executable()?;
    let mut checks = Vec::new();
    let binary_identity_path = executable
        .parent()
        .unwrap_or(Path::new("."))
        .join("installed-binary.json");
    let binary_identity = fs::read(&binary_identity_path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
    match binary_identity {
        Some(ref installed)
            if installed.get("version").and_then(Value::as_str)
                == Some(env!("CARGO_PKG_VERSION"))
                && installed.get("sha256").and_then(Value::as_str)
                    == Some(executable_sha256) =>
        {
            checks.push(doctor_check(
                "binary",
                "pass",
                format!("{} sha256:{executable_sha256}", env!("CARGO_PKG_VERSION")),
            ));
        }
        Some(_) => checks.push(doctor_check(
            "binary",
            "fail",
            "installed binary bytes or version differ from installed-binary.json",
        )),
        None => checks.push(doctor_check(
            "binary",
            "warn",
            format!(
                "no packaged binary identity beside {}; development builds are not release-certified",
                executable.display()
            ),
        )),
    }

    let root = plugin_root();
    match &root {
        Ok(root) => {
            let package_version = fs::read(root.join("package.json"))
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                .and_then(|value| {
                    value
                        .get("version")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                });
            let status = if package_version.as_deref() == Some(env!("CARGO_PKG_VERSION")) {
                "pass"
            } else {
                "fail"
            };
            checks.push(doctor_check(
                "package-cache",
                status,
                format!(
                    "plugin={} binary={}",
                    package_version.unwrap_or_else(|| "unknown".to_owned()),
                    env!("CARGO_PKG_VERSION")
                ),
            ));
            let marketplace = root.join(".agents/plugins/marketplace.json");
            let marketplace_owned = fs::read(&marketplace)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                .is_some_and(|value| {
                    value.get("name").and_then(Value::as_str) == Some("acyclic")
                        && value
                            .get("plugins")
                            .and_then(Value::as_array)
                            .into_iter()
                            .flatten()
                            .any(|plugin| {
                                plugin.get("name").and_then(Value::as_str) == Some("acyclic")
                            })
                });
            checks.push(doctor_check(
                "marketplace",
                if marketplace_owned { "pass" } else { "fail" },
                marketplace.display().to_string(),
            ));
            let ownership = read_codex_ownership(&codex_ownership_path()).ok().flatten();
            // A service Codex started runs from Codex's copy of the plugin,
            // so the installation is judged by the owned marketplace.
            let configured_owned = ownership.as_ref().is_some_and(|ownership| {
                ownership.version == 1
                    && codex_home().is_ok_and(|codex_home| {
                        runs_owned_codex_plugin(ownership, root, &codex_home)
                    })
                    && plan_codex_config(ownership).is_ok()
                    && codex_plugin_configuration_matches(ownership, &ownership.marketplace_root)
            });
            checks.push(doctor_check(
                "codex-install",
                // Like every host, Codex is optional: only an installation
                // that exists can be broken.
                match (&ownership, configured_owned) {
                    (None, _) => "warn",
                    (Some(_), true) => "pass",
                    (Some(_), false) => "fail",
                },
                ownership.as_ref().map_or_else(
                    || "no owned Codex installation".to_owned(),
                    |ownership| {
                        format!(
                            "marketplace={} plugin-version={}",
                            ownership.marketplace_root.display(),
                            env!("CARGO_PKG_VERSION")
                        )
                    },
                ),
            ));
            let hooks = root.join("hooks/hooks.json");
            let mcp = root.join(".mcp.json");
            // The npm command links to `bin/acyclic`: the executable itself on
            // Unix, and on Windows a name that resolves to `acyclic.exe` once
            // the installer has removed the placeholder.
            let native = root.join(if cfg!(windows) {
                "bin/acyclic.exe"
            } else {
                "bin/acyclic"
            });
            let command_placeholder = cfg!(windows) && root.join("bin/acyclic").exists();
            let hooks_valid = fs::read(&hooks)
                .ok()
                .filter(|bytes| bytes.len() <= 1024 * 1024)
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                .is_some_and(|document| codex_hook_mcp::hook_manifest_is_current(&document));
            let server_valid = fs::read(&mcp)
                .ok()
                .filter(|bytes| bytes.len() <= 1024 * 1024)
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                .is_some_and(|document| document == codex_hook_mcp::server_declaration());
            let command_valid = native.is_file() && !command_placeholder && server_valid;
            checks.push(doctor_check(
                "hooks",
                if hooks_valid { "pass" } else { "fail" },
                hooks.display().to_string(),
            ));
            checks.push(doctor_check(
                "agent-command",
                if command_valid { "pass" } else { "fail" },
                if command_valid {
                    "npm command and Codex hook server run the native executable; no model-visible MCP bridge"
                } else {
                    "npm command does not reach the native executable, or the plugin MCP declaration is not the Codex hook server"
                },
            ));
        }
        Err(error) => {
            checks.push(doctor_check("package-cache", "fail", error.clone()));
            checks.push(doctor_check("marketplace", "fail", error.clone()));
            checks.push(doctor_check("codex-install", "fail", error.clone()));
            checks.push(doctor_check("hooks", "fail", error.clone()));
            checks.push(doctor_check("agent-command", "fail", error));
        }
    }

    let (service_check, state_check) = service_checks(service);
    checks.push(service_check);
    let native = acyclic_fs::probe_native_mount();
    checks.push(doctor_check(
        "mount-backend",
        if native.available() && routes > 0 {
            "pass"
        } else {
            "warn"
        },
        format!(
            "kind={:?} available={} writable={} provider-io-observable={} qualification={}{}",
            native.kind,
            native.available(),
            native.available(),
            native.provider_process_io_observable(),
            if routes > 0 {
                "runtime-observed"
            } else {
                "first-spawn-runtime-check"
            },
            native
                .unavailable_reason
                .map(|reason| format!(" reason={reason}"))
                .unwrap_or_default()
        ),
    ));
    checks.push(state_check);
    let cli_on_path = env::var_os("PATH").is_some_and(|paths| {
        env::split_paths(&paths).any(|directory| {
            ["acyclic", "acyclic.exe", "acyclic.cmd", "acyclic.ps1"]
                .iter()
                .any(|name| directory.join(name).is_file())
        })
    });
    checks.push(doctor_check(
        "cli-path",
        if cli_on_path { "pass" } else { "warn" },
        if cli_on_path {
            "acyclic shell launcher is on PATH"
        } else {
            "plugin MCP tool is available; install the npm package for shell PATH access"
        },
    ));
    let receipt_path = data.join("certification").join(format!(
        "native-mount-{}-{}.json",
        env::consts::OS,
        env::consts::ARCH
    ));
    let certified = fs::read(&receipt_path)
        .ok()
        .filter(|bytes| bytes.len() <= 1024 * 1024)
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
        .is_some_and(|receipt| valid_platform_receipt(&receipt, executable_blake3));
    checks.push(doctor_check(
        "platform-certification",
        if certified { "pass" } else { "fail" },
        if certified {
            receipt_path.display().to_string()
        } else {
            format!(
                "no passing live qualification receipt at {}",
                receipt_path.display()
            )
        },
    ));
    if let Some(filters) = obs::active_filters() {
        checks.push(doctor_check("tracing", "pass", filters));
    }
    checks.push(claude_code_installation_check());
    checks.push(pi_installation_check());
    let ok = checks
        .iter()
        .all(|check| check.get("status").and_then(Value::as_str) != Some("fail"));
    Ok(json!({
        "schemaVersion": 2,
        "ok": ok,
        "capabilities": {
            "nativeMount": {
                "available": native.available(),
                "writable": native.available(),
                "providerProcessIoObservable": native.provider_process_io_observable(),
                "sessionIsolation": format!("{:?}", native.kind),
                "qualifiedForThisBinary": certified,
            },
            "processConfinement": {
                "qualified": false,
                "reason": "requires a separate passing host/platform escape qualification"
            }
        },
        "version": env!("CARGO_PKG_VERSION"),
        "platform": {"os": env::consts::OS, "arch": env::consts::ARCH},
        "checks": checks,
    }))
}

/// The service and durable-state checks, from the service's own summary or
/// from the reason no service answered.
pub(crate) fn service_checks(service: Result<&ServiceSummary, &str>) -> (Value, Value) {
    match service {
        Ok(service) => (
            doctor_check(
                "service",
                "pass",
                format!(
                    "identity={} sessions={} routes={} leases={}",
                    service.identity, service.sessions, service.routes, service.leases
                ),
            ),
            if service.pending_recovery {
                doctor_check(
                    "persistent-state",
                    "warn",
                    "durable recovery work is pending",
                )
            } else {
                doctor_check(
                    "persistent-state",
                    "pass",
                    "durable state loaded with no pending adapter recovery",
                )
            },
        ),
        Err(error) => (
            doctor_check("service", "fail", error),
            doctor_check(
                "persistent-state",
                "warn",
                "durable state is not loaded while the service is unavailable",
            ),
        ),
    }
}

/// The user-scope Claude Code hooks: absent is a warning, and an owned
/// installation must route every event to an executable that exists.
pub(crate) fn claude_code_installation_check() -> Value {
    claude_settings_path(false).map_or_else(
        |error| doctor_check("claude-code-install", "fail", error),
        |path| claude_code_installation_check_at(&path),
    )
}

pub(crate) fn claude_code_installation_check_at(path: &Path) -> Value {
    let inspect = || -> Result<Option<PathBuf>, String> {
        let path = path.to_path_buf();
        if read_mcp_ownership(&mcp_ownership_path(&path, "claude-hooks"))?.is_none() {
            return Ok(None);
        }
        let settings = read_optional_json(&path, "Claude Code settings")?
            .ok_or_else(|| format!("Claude Code settings at {} are missing", path.display()))?;
        for event in CLAUDE_HOOK_EVENTS {
            let routed = settings
                .pointer(&format!("/hooks/{event}"))
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter(|group| is_acyclic_claude_hook_group(event, group))
                .filter_map(|group| group.pointer("/hooks/0/command").and_then(Value::as_str))
                .any(|command| Path::new(command).is_file());
            if !routed {
                return Err(format!(
                    "Claude Code hooks at {} do not route {event} to an installed Acyclic executable",
                    path.display()
                ));
            }
        }
        Ok(Some(path))
    };
    match inspect() {
        Ok(Some(path)) => doctor_check("claude-code-install", "pass", path.display().to_string()),
        Ok(None) => doctor_check(
            "claude-code-install",
            "warn",
            "no owned per-user Claude Code hook installation",
        ),
        Err(error) => doctor_check("claude-code-install", "fail", error),
    }
}
