//! Service identity, locking, lifecycle, doctor, and the control client.

use super::*;

pub(crate) fn default_data_directory() -> PathBuf {
    #[cfg(windows)]
    if let Some(root) = env::var_os("LOCALAPPDATA") {
        return PathBuf::from(root).join("Acyclic").join("state-v5");
    }
    #[cfg(not(windows))]
    if let Some(root) = env::var_os("XDG_STATE_HOME") {
        return PathBuf::from(root).join("acyclic").join("state-v5");
    }
    #[cfg(not(windows))]
    if let Some(root) = env::var_os("HOME") {
        return PathBuf::from(root)
            .join(".local")
            .join("state")
            .join("acyclic")
            .join("state-v5");
    }
    env::temp_dir().join("acyclic-state-v5")
}

/// The running executable's own path. macOS reports the path it was started
/// through, which for the npm-installed command is a link outside the package.
pub(crate) fn current_executable() -> Result<PathBuf, String> {
    let executable = env::current_exe().map_err(display)?;
    #[cfg(unix)]
    let executable = executable.canonicalize().map_err(display)?;
    Ok(executable)
}

pub(crate) fn service_identity(data: &Path) -> Result<String, String> {
    service_identity_for(data, &current_executable()?)
}

/// The identity of an executable's artifact bytes. Hashing tens of megabytes
/// on every session start is avoidable: the identity is cached under the
/// file's fingerprint, which every rewrite or replacement of the file changes.
pub(crate) fn service_identity_for(data: &Path, executable: &Path) -> Result<String, String> {
    let cache = data.join("executable-identity");
    let file = fs::File::open(executable).map_err(display)?;
    let (fingerprint, changed) = executable_fingerprint(&file)?;
    if let Some(identity) = fs::read(&cache)
        .ok()
        .and_then(|cached| cached_identity(&cached, &fingerprint))
    {
        return Ok(identity);
    }
    let mut hasher = blake3::Hasher::new();
    read_executable(&file, |bytes| {
        hasher.update(bytes);
    })?;
    let identity = service_identity_from_digest(&hasher.finalize().to_hex());
    // Cache only a hash of bytes that did not change while they were read and
    // whose last change is older than any timestamp granularity: a later write
    // in the same clock tick could otherwise keep the fingerprint (the racy
    // timestamp problem Git's index solves the same way). A cache that cannot
    // be written, or is lost, only costs the next caller one hash.
    let settled = std::time::SystemTime::now()
        .duration_since(changed)
        .is_ok_and(|age| age > SETTLED_EXECUTABLE_AGE);
    if settled && executable_fingerprint(&file)?.0 == fingerprint {
        let staged = data.join(format!("executable-identity.{}", std::process::id()));
        let entry = format!("{}\n{identity}", hex::encode(fingerprint));
        if fs::write(&staged, entry)
            .and_then(|()| fs::rename(&staged, &cache))
            .is_err()
        {
            let _ = fs::remove_file(&staged);
        }
    }
    Ok(identity)
}

pub(crate) fn cached_identity(cached: &[u8], fingerprint: &[u8; 32]) -> Option<String> {
    let (cached_fingerprint, identity) = std::str::from_utf8(cached).ok()?.split_once('\n')?;
    (cached_fingerprint == hex::encode(fingerprint)
        && identity.len() == 64
        && identity
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f')))
    .then(|| identity.to_owned())
}

/// An executable changed longer ago than this has timestamps that any later
/// write must advance.
pub(crate) const SETTLED_EXECUTABLE_AGE: std::time::Duration = std::time::Duration::from_secs(1);

/// Digest of the file's identity, size, and modification and change times,
/// with the change time. The change time cannot be set by callers, so no write
/// can preserve it.
#[cfg(unix)]
pub(crate) fn executable_fingerprint(
    file: &fs::File,
) -> Result<([u8; 32], std::time::SystemTime), String> {
    use std::os::unix::fs::MetadataExt as _;
    let metadata = file.metadata().map_err(display)?;
    let mut hasher = blake3::Hasher::new();
    for field in [
        metadata.dev(),
        metadata.ino(),
        metadata.size(),
        metadata.mtime().cast_unsigned(),
        metadata.mtime_nsec().cast_unsigned(),
        metadata.ctime().cast_unsigned(),
        metadata.ctime_nsec().cast_unsigned(),
    ] {
        hasher.update(&field.to_le_bytes());
    }
    let changed = std::time::UNIX_EPOCH
        .checked_add(std::time::Duration::new(
            metadata.ctime().try_into().unwrap_or_default(),
            metadata.ctime_nsec().try_into().unwrap_or_default(),
        ))
        .unwrap_or(std::time::UNIX_EPOCH);
    Ok((*hasher.finalize().as_bytes(), changed))
}

/// Digest of the file's volume and identity, size, and write and change
/// times, with the change time. The change time cannot be set by callers, so
/// no write can preserve it.
#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "GetFileInformationByHandleEx fills fixed-size structures for a live handle"
)]
pub(crate) fn executable_fingerprint(
    file: &fs::File,
) -> Result<([u8; 32], std::time::SystemTime), String> {
    use std::os::windows::io::AsRawHandle as _;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_BASIC_INFO, FILE_ID_INFO, FileBasicInfo, FileIdInfo, GetFileInformationByHandleEx,
    };
    fn query<T>(file: &fs::File, class: i32) -> Result<T, String> {
        let mut information = std::mem::MaybeUninit::<T>::zeroed();
        let size = u32::try_from(std::mem::size_of::<T>()).map_err(display)?;
        // SAFETY: the handle is live for the call and the buffer is exactly
        // `size` writable bytes of the structure this class returns.
        if unsafe {
            GetFileInformationByHandleEx(
                file.as_raw_handle(),
                class,
                information.as_mut_ptr().cast(),
                size,
            )
        } == 0
        {
            return Err(display(io::Error::last_os_error()));
        }
        // SAFETY: the call succeeded, so it initialized the structure.
        Ok(unsafe { information.assume_init() })
    }
    let identity = query::<FILE_ID_INFO>(file, FileIdInfo)?;
    let basic = query::<FILE_BASIC_INFO>(file, FileBasicInfo)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(&identity.VolumeSerialNumber.to_le_bytes());
    hasher.update(&identity.FileId.Identifier);
    hasher.update(&file.metadata().map_err(display)?.len().to_le_bytes());
    for time in [basic.CreationTime, basic.LastWriteTime, basic.ChangeTime] {
        hasher.update(&time.to_le_bytes());
    }
    // FILETIME counts 100 ns intervals from 1601; Unix time starts 11,644,473,600 s later.
    let changed = u64::try_from(basic.ChangeTime)
        .ok()
        .and_then(|ticks| ticks.checked_sub(116_444_736_000_000_000))
        .and_then(|ticks| {
            std::time::UNIX_EPOCH
                .checked_add(std::time::Duration::from_nanos(ticks.saturating_mul(100)))
        })
        .unwrap_or(std::time::UNIX_EPOCH);
    Ok((*hasher.finalize().as_bytes(), changed))
}

pub(crate) fn service_identity_from_digest(digest: &str) -> String {
    blake3::hash(format!("{}:{digest}", env!("CARGO_PKG_VERSION")).as_bytes())
        .to_hex()
        .to_string()
}

pub(crate) fn read_executable(
    mut file: &fs::File,
    mut update: impl FnMut(&[u8]),
) -> Result<(), String> {
    file.rewind().map_err(display)?;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(display)?;
        if read == 0 {
            break;
        }
        let chunk = buffer
            .get(..read)
            .ok_or_else(|| "binary hash read exceeded its buffer".to_owned())?;
        update(chunk);
    }
    Ok(())
}

pub(crate) struct ExecutableDigests {
    pub(crate) service_identity: String,
    pub(crate) sha256: String,
    pub(crate) blake3: String,
}

pub(crate) fn executable_digests() -> Result<ExecutableDigests, String> {
    executable_digests_for(&current_executable()?)
}

pub(crate) async fn executable_digests_async() -> Result<ExecutableDigests, String> {
    tokio::task::spawn_blocking(executable_digests)
        .await
        .map_err(display)?
}

pub(crate) fn executable_digests_for(executable: &Path) -> Result<ExecutableDigests, String> {
    // Launchers and plugin caches can expose the same signed artifact at different paths. The
    // service belongs to the artifact, not to one of those aliases; including the path caused
    // identical clients to continuously drain and replace each other's service.
    let mut sha256 = Sha256::new();
    let mut blake3 = blake3::Hasher::new();
    read_executable(&fs::File::open(executable).map_err(display)?, |chunk| {
        sha256.update(chunk);
        blake3.update(chunk);
    })?;
    let sha256 = hex::encode(sha256.finalize());
    let blake3 = blake3.finalize().to_hex().to_string();
    Ok(ExecutableDigests {
        service_identity: service_identity_from_digest(&blake3),
        sha256,
        blake3,
    })
}

pub(crate) fn blake3_file(path: &Path) -> Result<String, String> {
    let mut hasher = blake3::Hasher::new();
    read_executable(&fs::File::open(path).map_err(display)?, |bytes| {
        hasher.update(bytes);
    })?;
    Ok(hasher.finalize().to_hex().to_string())
}

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
    let output = std::process::Command::new("codex")
        .args(arguments)
        .output()
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

#[allow(clippy::too_many_arguments)]
pub(crate) fn doctor_report(
    data: &Path,
    identity: &str,
    executable_sha256: &str,
    executable_blake3: &str,
    sessions: usize,
    routes: usize,
    leases: usize,
    pending_recovery: bool,
) -> Result<Value, String> {
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
            let configured_owned = ownership.as_ref().is_some_and(|ownership| {
                ownership.version == 1
                    && ownership
                        .marketplace_root
                        .canonicalize()
                        .ok()
                        .zip(root.canonicalize().ok())
                        .is_some_and(|(configured, packaged)| configured == packaged)
                    && plan_codex_config(ownership).is_ok()
                    && codex_plugin_configuration_matches(ownership, root)
            });
            checks.push(doctor_check(
                "codex-install",
                if configured_owned { "pass" } else { "fail" },
                ownership.map_or_else(
                    || "Acyclic has no Codex installation ownership record".to_owned(),
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

    checks.push(doctor_check(
        "service",
        "pass",
        format!("identity={identity} sessions={sessions} routes={routes} leases={leases}"),
    ));
    let native = acyclic_fs::probe_native_mount();
    checks.push(doctor_check(
        "mount-backend",
        if native.available && native.writable && routes > 0 {
            "pass"
        } else {
            "warn"
        },
        format!(
            "kind={:?} available={} writable={} provider-io-observable={} qualification={}{}",
            native.kind,
            native.available,
            native.writable,
            native.provider_process_io_observable,
            if routes > 0 {
                "runtime-observed"
            } else {
                "first-spawn-runtime-check"
            },
            native
                .unavailable_reason
                .as_deref()
                .map(|reason| format!(" reason={reason}"))
                .unwrap_or_default()
        ),
    ));
    checks.push(doctor_check(
        "persistent-state",
        if pending_recovery { "warn" } else { "pass" },
        if pending_recovery {
            "durable recovery work is pending"
        } else {
            "durable state loaded with no pending adapter recovery"
        },
    ));
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
    let ok = checks
        .iter()
        .all(|check| check.get("status").and_then(Value::as_str) != Some("fail"));
    Ok(json!({
        "schemaVersion": 2,
        "ok": ok,
        "capabilities": {
            "nativeMount": {
                "available": native.available,
                "writable": native.writable,
                "providerProcessIoObservable": native.provider_process_io_observable,
                "sessionIsolation": format!("{:?}", native.session_isolation),
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

pub(crate) struct ServiceLock {
    pub(crate) lifecycle_file: fs::File,
    pub(crate) data_file: Option<fs::File>,
}

impl ServiceLock {
    pub(crate) fn prepare_purge(&mut self) {
        if let Some(file) = self.data_file.take() {
            let _ = file.unlock();
        }
    }
}

impl Drop for ServiceLock {
    fn drop(&mut self) {
        self.prepare_purge();
        let _ = self.lifecycle_file.unlock();
    }
}

/// How any Acyclic binary identifies and stops the service of any other.
///
/// The control protocol changes between releases, and a service rejects a
/// protocol it does not speak, so a new binary could neither identify nor
/// drain an old service through it. This contract never changes, so every
/// binary from this one on can hand off to any other:
///
/// - The service holds an exclusive lock on `service.lock` for its whole
///   life, so a free lock proves that no service runs.
/// - While its endpoint accepts requests it publishes `service.identity`,
///   exactly `acyclic-service-v1\n{instance id}\n{binary identity}\n`.
/// - A file `service-stop/{instance id}` holding a drain ID of at most 128
///   ASCII letters, digits and hyphens asks that instance to drain every
///   session and exit. It then writes `service-drain/{instance id}.json`,
///   version 1, with its instance ID as `identity`, the `drainId` of the
///   request it served and whether teardown succeeded, and releases its
///   lock. Every requester of that instance is answered by that record:
///   an instance drains every session once, whichever request it read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ServiceMarker {
    pub(crate) instance_id: String,
    pub(crate) binary_identity: String,
}

pub(crate) const SERVICE_MARKER_HEADER: &str = "acyclic-service-v1";
pub(crate) const SERVICE_STOP_DIRECTORY: &str = "service-stop";

impl ServiceMarker {
    pub(crate) fn path(data: &Path) -> PathBuf {
        data.join("service.identity")
    }

    pub(crate) fn encode(&self) -> String {
        format!(
            "{SERVICE_MARKER_HEADER}\n{}\n{}\n",
            self.instance_id, self.binary_identity
        )
    }

    /// The running service's marker, if one is published.
    pub(crate) fn read(data: &Path) -> Option<Self> {
        let text = fs::read_to_string(Self::path(data)).ok()?;
        let mut lines = text.strip_suffix('\n')?.split('\n');
        let (Some(SERVICE_MARKER_HEADER), Some(instance_id), Some(binary_identity), None) =
            (lines.next(), lines.next(), lines.next(), lines.next())
        else {
            return None;
        };
        is_handoff_id(instance_id).then(|| Self {
            instance_id: instance_id.to_owned(),
            binary_identity: binary_identity.to_owned(),
        })
    }
}

/// Instance and drain IDs name files, so they are short and plain.
pub(crate) fn is_handoff_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
}

/// Asks the service instance `instance_id` to drain; see [`ServiceMarker`].
pub(crate) fn request_service_stop(
    data: &Path,
    instance_id: &str,
    drain_id: &str,
) -> Result<(), String> {
    let directory = data.join(SERVICE_STOP_DIRECTORY);
    fs::create_dir_all(&directory).map_err(display)?;
    // Written aside and renamed, so the service never reads a partial ID.
    let staged = directory.join(format!("{instance_id}.{drain_id}.next"));
    fs::write(&staged, drain_id).map_err(display)?;
    fs::rename(&staged, directory.join(instance_id)).map_err(display)
}

/// The service's side of stop requests; see [`ServiceMarker`].
pub(crate) struct StopRequests {
    pub(crate) request: PathBuf,
    pub(crate) arrived: Arc<tokio::sync::Notify>,
    pub(crate) _watcher: acyclic_fs::watch::NativeEventWatcher,
}

impl StopRequests {
    pub(crate) fn open(data: &Path, instance_id: &str) -> Result<Self, String> {
        let directory = data.join(SERVICE_STOP_DIRECTORY);
        fs::create_dir_all(&directory).map_err(display)?;
        // Requests addressed to earlier instances can never be served, and
        // their drain records were read by requesters that held this lock.
        let drains = data.join(SERVICE_DRAIN_DIRECTORY);
        match fs::remove_dir_all(&drains) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(display(error)),
        }
        for entry in fs::read_dir(&directory).map_err(display)? {
            match fs::remove_file(entry.map_err(display)?.path()) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(display(error)),
            }
        }
        let arrived = Arc::new(tokio::sync::Notify::new());
        let notification = Arc::clone(&arrived);
        // Any event, or a watcher error, only prompts another look.
        let mut watcher = <acyclic_fs::watch::NativeEventWatcher as notify::Watcher>::new(
            move |_| notification.notify_one(),
            notify::Config::default(),
        )
        .map_err(display)?;
        notify::Watcher::watch(
            &mut watcher,
            &directory,
            notify::RecursiveMode::NonRecursive,
        )
        .map_err(display)?;
        Ok(Self {
            request: directory.join(instance_id),
            arrived,
            _watcher: watcher,
        })
    }

    /// Waits for a stop request and returns its drain ID.
    pub(crate) async fn next(&self) -> String {
        loop {
            if let Some(drain_id) = fs::read_to_string(&self.request)
                .ok()
                .filter(|drain_id| is_handoff_id(drain_id))
            {
                return drain_id;
            }
            self.arrived.notified().await;
        }
    }
}

/// The published marker, withdrawn when the service stops answering.
pub(crate) struct PublishedServiceMarker {
    pub(crate) path: PathBuf,
}

impl PublishedServiceMarker {
    /// Publishes the marker whole: a reader sees no marker or all of it.
    pub(crate) fn create(data: &Path, marker: &ServiceMarker) -> Result<Self, String> {
        let path = ServiceMarker::path(data);
        let staged = data.join(format!("service.identity.{}.next", marker.instance_id));
        fs::write(&staged, marker.encode()).map_err(display)?;
        if let Err(error) = fs::rename(&staged, &path) {
            let _ = fs::remove_file(&staged);
            return Err(display(error));
        }
        Ok(Self { path })
    }
}

impl Drop for PublishedServiceMarker {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub(crate) const SERVICE_DRAIN_DIRECTORY: &str = "service-drain";

/// Where the instance `identity` records its drain; see [`ServiceMarker`].
pub(crate) fn service_drain_completion_path(data: &Path, identity: &str) -> PathBuf {
    data.join(SERVICE_DRAIN_DIRECTORY)
        .join(format!("{identity}.json"))
}

pub(crate) fn write_service_drain_completion(
    data: &Path,
    identity: &str,
    drain_id: &str,
    result: &Result<(), String>,
) -> Result<(), String> {
    let path = service_drain_completion_path(data, identity);
    let next = path.with_extension("next");
    fs::create_dir_all(data.join(SERVICE_DRAIN_DIRECTORY)).map_err(display)?;
    let value = json!({
        "version": 1,
        "identity": identity,
        "drainId": drain_id,
        "ok": result.is_ok(),
        "error": result.as_ref().err(),
    });
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&next)
        .map_err(display)?;
    serde_json::to_writer_pretty(&mut file, &value).map_err(display)?;
    file.write_all(b"\n").map_err(display)?;
    file.sync_all().map_err(display)?;
    drop(file);
    durable_rename(&next, &path, RenameMode::Replace).map_err(display)
}

pub(crate) fn verify_service_drain_completion(data: &Path, identity: &str) -> Result<(), String> {
    let path = service_drain_completion_path(data, identity);
    let value: Value = serde_json::from_slice(&fs::read(&path).map_err(|error| {
        format!(
            "Acyclic service exited without durable drain confirmation at {}: {error}",
            path.display()
        )
    })?)
    .map_err(display)?;
    if value.get("version").and_then(Value::as_u64) != Some(1)
        || value.get("identity").and_then(Value::as_str) != Some(identity)
    {
        return Err("Acyclic service drain confirmation does not match this request".to_owned());
    }
    if value.get("ok").and_then(Value::as_bool) != Some(true) {
        return Err(format!(
            "Acyclic service teardown failed: {}",
            value
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("unknown teardown error")
        ));
    }
    Ok(())
}

pub(crate) fn acquire_service_lock(data: &Path) -> Result<Option<ServiceLock>, String> {
    let parent = data
        .parent()
        .ok_or_else(|| "service data path has no parent".to_owned())?;
    fs::create_dir_all(parent).map_err(display)?;
    let lifecycle_file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(parent.join(".acyclic-service-lifecycle.lock"))
        .map_err(display)?;
    match lifecycle_file.try_lock_exclusive() {
        Ok(()) => {}
        Err(error) if service_lock_is_contended(&error) => return Ok(None),
        Err(error) => return Err(display(error)),
    }
    fs::create_dir_all(data).map_err(display)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(data, fs::Permissions::from_mode(0o700)).map_err(display)?;
    }
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(data.join("service.lock"))
        .map_err(display)?;
    match file.try_lock_exclusive() {
        Ok(()) => Ok(Some(ServiceLock {
            lifecycle_file,
            data_file: Some(file),
        })),
        Err(error) if service_lock_is_contended(&error) => Ok(None),
        Err(error) => Err(display(error)),
    }
}

pub(crate) fn service_lock_is_contended(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::WouldBlock || cfg!(windows) && error.raw_os_error() == Some(33)
}

pub(crate) async fn run_service(data: PathBuf) -> Result<(), String> {
    // Taken first, so nothing the service prints can reach its starter.
    let ready = acyclic_native_runtime::take_service_ready_signal().map_err(display)?;
    run_service_with_identity(data, None, ready).await
}

pub(crate) async fn shutdown_service_endpoint(
    endpoint: ControlEndpoint,
    control: Arc<ConcurrentServiceControl>,
) -> Result<(), String> {
    let endpoint_result = endpoint.shutdown().await;
    let control_result = match Arc::try_unwrap(control) {
        Ok(control) => control.shutdown().await,
        Err(_) => Err("control service retained an active request during shutdown".to_owned()),
    };
    endpoint_result.and(control_result)
}

/// Runs the service. `ready`, when its starter waits on it, is signalled once
/// the service answers requests, and closes unsignalled if it exits first,
/// for instance because another service holds the lock.
pub(crate) async fn run_service_with_identity(
    data: PathBuf,
    identity_override: Option<String>,
    ready: Option<acyclic_native_runtime::ServiceReadySignal>,
) -> Result<(), String> {
    let Some(lock) = acquire_service_lock(&data)? else {
        return Ok(());
    };
    let result = run_locked_service(data, identity_override, ready).await;
    drop(lock);
    result
}

pub(crate) async fn run_locked_service(
    data: PathBuf,
    identity_override: Option<String>,
    ready: Option<acyclic_native_runtime::ServiceReadySignal>,
) -> Result<(), String> {
    let mut service = ConcurrentServiceControl::open(data.clone()).await?;
    if let Some(identity) = identity_override {
        service.resources.binary_identity = identity;
    }
    let instance_id = service.instance_id.clone();
    let binary_identity = service.binary_identity.clone();
    let control = Arc::new(service);
    let endpoint = match start_control_endpoint(Arc::clone(&control), &data).await {
        Ok(endpoint) => endpoint,
        Err(endpoint_error) => {
            let control_result = match Arc::try_unwrap(control) {
                Ok(control) => control.shutdown().await,
                Err(_) => Err("failed endpoint retained the control service".to_owned()),
            };
            return match control_result {
                Ok(()) => Err(endpoint_error),
                Err(shutdown_error) => Err(format!(
                    "{endpoint_error}; endpoint startup cleanup failed: {shutdown_error}"
                )),
            };
        }
    };
    let published = StopRequests::open(&data, &instance_id).and_then(|stops| {
        let marker = ServiceMarker {
            instance_id: instance_id.clone(),
            binary_identity,
        };
        PublishedServiceMarker::create(&data, &marker).map(|marker| (stops, marker))
    });
    let (stops, _marker) = match published {
        Ok(published) => published,
        Err(marker_error) => {
            let cleanup = shutdown_service_endpoint(endpoint, control).await;
            return match cleanup {
                Ok(()) => Err(marker_error),
                Err(cleanup_error) => Err(format!(
                    "{marker_error}; identity publication cleanup failed: {cleanup_error}"
                )),
            };
        }
    };
    if let Some(ready) = ready {
        // A starter that stopped waiting has nothing to be told.
        let _ = ready.signal();
    }
    let (service_result, requested_drain) = tokio::select! {
        signal = tokio::signal::ctrl_c() => (signal.map_err(display), None),
        drain_id = stops.next() => {
            control.begin_drain();
            (Ok(()), Some(drain_id))
        }
    };
    drop(stops);
    let result = service_result.and(shutdown_service_endpoint(endpoint, control).await);
    if let Some(drain_id) = requested_drain {
        write_service_drain_completion(&data, &instance_id, &drain_id, &result)?;
    }
    result
}

pub(crate) async fn service_is_ready_for_identity(
    data: &Path,
    identity: &str,
) -> Result<bool, String> {
    let ping = ping_request()?;
    let answer = send_control_request_once(data, &ping).await;
    if let Ok(active) = &answer
        && active.get("identity").and_then(Value::as_str) == Some(identity)
    {
        return Ok(true);
    }
    // Only a service that holds its lock can still open an endpoint; when
    // none does, nothing needs waiting for.
    if claim_stopped_service(data, None)?.is_some() {
        return Ok(false);
    }
    // A running service says what it is through its marker, whatever
    // protocol it speaks, and a service of another binary is stopped
    // through the same contract.
    match ServiceMarker::read(data) {
        Some(marker) if marker.binary_identity != identity => {
            // Only the service is replaced: its state belongs to the stores,
            // each of which refuses a format it does not know.
            drop(drain_service(data, Some(&marker.instance_id)).await?);
            Ok(false)
        }
        // This binary's service, or one still starting: it answers soon.
        _ => match answer {
            Ok(_) | Err(ControlRequestError::Unavailable(_)) => Ok(false),
            Err(error) => Err(format!(
                "cannot safely identify the Acyclic service: {error}"
            )),
        },
    }
}

pub(crate) async fn ensure_service(data: &Path) -> Result<(), String> {
    fs::create_dir_all(data).map_err(display)?;
    let identity = service_identity(data)?;
    if service_is_ready_for_identity(data, &identity).await? {
        return Ok(());
    }
    let ping = ping_request()?;
    let readiness = spawn_service_process(&current_executable()?)?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    // The service signals once it answers requests. A channel that closes
    // unsignalled means it exited, typically because another service holds
    // the lock and is starting; only then does this poll for that one.
    wait_for_service_readiness(readiness, deadline).await?;
    let last = loop {
        let last = match send_control_request_once(data, &ping).await {
            Ok(active)
                if active.get("identity").and_then(Value::as_str) == Some(identity.as_str()) =>
            {
                return Ok(());
            }
            Ok(_) => "the previous Acyclic service is still draining".to_owned(),
            Err(error) => error.to_string(),
        };
        if std::time::Instant::now() >= deadline {
            break last;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    Err(format!("Acyclic service did not become ready: {last}"))
}

pub(crate) fn spawn_service_process(
    executable: &Path,
) -> Result<acyclic_native_runtime::ServiceReadiness, String> {
    acyclic_native_runtime::spawn_service_process(executable).map_err(display)
}

/// Waits, until `deadline`, for a started service to signal readiness or
/// exit. The blocking read runs on its own thread, which the process may
/// leave behind when it exits.
pub(crate) async fn wait_for_service_readiness(
    readiness: acyclic_native_runtime::ServiceReadiness,
    deadline: std::time::Instant,
) -> Result<(), String> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("acyclic-service-readiness".to_owned())
        .spawn(move || {
            let _ = sender.send(readiness.wait());
        })
        .map_err(display)?;
    let wait = deadline.saturating_duration_since(std::time::Instant::now());
    let _ = tokio::time::timeout(wait, receiver).await;
    Ok(())
}

pub(crate) fn ping_request() -> Result<ControlRequest, String> {
    Ok(ControlRequest {
        version: 1,
        command: ControlCommand::Ping,
        cwd: env::current_dir().map_err(display)?,
        argv: Vec::new(),
        name: String::new(),
        arguments: Value::Null,
    })
}

pub(crate) async fn send_cli_control_request(
    data: &Path,
    request: &ControlRequest,
) -> Result<Value, String> {
    let identity = service_identity(data)?;
    // Sandboxed hosts may expose the already-running local endpoint while denying the client's
    // direct view of per-user state. Probe that endpoint before attempting a filesystem-backed
    // cold start. The published marker is an instance nonce, not a binary compatibility identity.
    let ping = ping_request()?;
    for attempt in 0..10 {
        match send_control_request(data, &ping).await {
            Ok(active)
                if active.get("identity").and_then(Value::as_str) == Some(identity.as_str()) =>
            {
                return send_control_request(data, request)
                    .await
                    .map_err(|error| error.to_string());
            }
            Err(ControlRequestError::Unavailable(_)) if attempt < 9 => {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            Ok(_) | Err(ControlRequestError::Unavailable(_)) => break,
            Err(error) => return Err(error.to_string()),
        }
    }
    ensure_service(data).await?;
    send_control_request(data, request)
        .await
        .map_err(|error| error.to_string())
}

pub(crate) fn control_request_from_argv(
    cwd: &Path,
    mut argv: Vec<String>,
) -> Result<ControlRequest, String> {
    let cwd = cwd.canonicalize().map_err(display)?;
    let mut selected_cwd = cwd.clone();
    if argv.first().is_some_and(|argument| argument == "-C") {
        if argv.len() < 2 {
            return Err("acyclic -C requires a path".to_owned());
        }
        selected_cwd = PathBuf::from(argv.remove(1))
            .canonicalize()
            .map_err(display)?;
        argv.remove(0);
    }
    let command = match argv.first().map(String::as_str) {
        Some("git") => ControlCommand::Git,
        Some("agents") => ControlCommand::Agents,
        Some("doctor") => ControlCommand::Doctor,
        Some("discard") => ControlCommand::Discard,
        _ => return Err("unsupported Acyclic command".to_owned()),
    };
    Ok(ControlRequest {
        version: 1,
        command,
        cwd,
        argv: argv.get(1..).unwrap_or_default().to_vec(),
        name: String::new(),
        arguments: cli_routing(selected_cwd),
    })
}

pub(crate) async fn run_rpc_proxy(
    data: &Path,
    mut reader: impl BufRead,
    mut writer: impl Write,
    commandless: bool,
) -> Result<(), String> {
    while let Some(line) = read_bounded_rpc_line(&mut reader)? {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let request: Value = serde_json::from_slice(&line).map_err(display)?;
        let Some(id) = request.get("id").cloned() else {
            continue;
        };
        let method = request.get("method").and_then(Value::as_str).unwrap_or("");
        let response = match method {
            "initialize" => {
                json!({"jsonrpc":"2.0","id":id,"result":{"protocolVersion":"2025-06-18","capabilities":{"tools":{"listChanged":false}},"serverInfo":{"name":"acyclic","version":env!("CARGO_PKG_VERSION")}}})
            }
            "ping" => json!({"jsonrpc":"2.0","id":id,"result":{}}),
            "tools/list" => {
                json!({"jsonrpc":"2.0","id":id,"result":{"tools":public_tools(commandless)}})
            }
            "tools/call" => {
                let params = request.get("params").cloned().unwrap_or(Value::Null);
                let name = params.get("name").and_then(Value::as_str).unwrap_or("");
                let arguments = params
                    .get("arguments")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                let forwarded = if commandless && name == "acyclic" {
                    let argv = arguments
                        .get("argv")
                        .and_then(Value::as_array)
                        .ok_or_else(|| "acyclic requires an argv string array".to_owned())?
                        .iter()
                        .map(|value| {
                            value
                                .as_str()
                                .map(str::to_owned)
                                .ok_or_else(|| "acyclic argv entries must be strings".to_owned())
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    control_request_from_argv(&env::current_dir().map_err(display)?, argv)?
                } else {
                    return Err("this Acyclic MCP endpoint does not expose that tool".to_owned());
                };
                match send_control_request(data, &forwarded).await {
                    Ok(result) => {
                        json!({"jsonrpc":"2.0","id":id,"result":{"content":[{"type":"text","text":serde_json::to_string(&result).map_err(display)?}]}})
                    }
                    Err(error) => {
                        json!({"jsonrpc":"2.0","id":id,"result":{"isError":true,"content":[{"type":"text","text":error.to_string()}]}})
                    }
                }
            }
            _ => {
                json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"method not found"}})
            }
        };
        serde_json::to_writer(&mut writer, &response).map_err(display)?;
        writer.write_all(b"\n").map_err(display)?;
        writer.flush().map_err(display)?;
    }
    Ok(())
}

pub(crate) fn read_bounded_rpc_line(reader: &mut impl BufRead) -> Result<Option<Vec<u8>>, String> {
    let mut line = Vec::new();
    let read = reader
        .take((MAXIMUM_CONTROL_MESSAGE_BYTES + 1) as u64)
        .read_until(b'\n', &mut line)
        .map_err(display)?;
    if read == 0 {
        return Ok(None);
    }
    if line.len() > MAXIMUM_CONTROL_MESSAGE_BYTES {
        return Err("Acyclic MCP request exceeds the maximum frame size".to_owned());
    }
    if line.last() == Some(&b'\n') {
        line.pop();
        if line.last() == Some(&b'\r') {
            line.pop();
        }
    }
    Ok(Some(line))
}

#[derive(Debug)]
pub(crate) enum ControlRequestError {
    Unavailable(String),
    Indeterminate(String),
    Response(String),
}

impl std::fmt::Display for ControlRequestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unavailable(message) | Self::Indeterminate(message) | Self::Response(message) => {
                formatter.write_str(message)
            }
        }
    }
}

pub(crate) async fn send_control_request(
    data: &Path,
    request: &ControlRequest,
) -> Result<Value, ControlRequestError> {
    send_control_envelope(data, &ControlEnvelope::new(request.clone())).await
}

pub(crate) async fn send_control_envelope(
    data: &Path,
    envelope: &ControlEnvelope<ControlRequest>,
) -> Result<Value, ControlRequestError> {
    send_control_envelope_with_attempts(data, envelope, 50, control_request_wait(&envelope.request))
        .await
}

pub(crate) fn control_request_wait(request: &ControlRequest) -> std::time::Duration {
    match request.command {
        ControlCommand::Ping => CONTROL_PROBE_WAIT,
        ControlCommand::Hook if request.name.ends_with(":SessionEnd") => CONTROL_PROBE_WAIT,
        ControlCommand::Hook => CONTROL_HOOK_WAIT,
        ControlCommand::Doctor
        | ControlCommand::Git
        | ControlCommand::Agents
        | ControlCommand::Discard => CONTROL_COMMAND_WAIT,
    }
}

pub(crate) async fn send_control_request_once(
    data: &Path,
    request: &ControlRequest,
) -> Result<Value, ControlRequestError> {
    send_control_envelope_once(data, &ControlEnvelope::new(request.clone())).await
}

pub(crate) async fn send_control_envelope_once(
    data: &Path,
    envelope: &ControlEnvelope<ControlRequest>,
) -> Result<Value, ControlRequestError> {
    send_control_envelope_with_attempts(data, envelope, 1, control_request_wait(&envelope.request))
        .await
}

pub(crate) async fn send_control_envelope_with_attempts(
    data: &Path,
    envelope: &ControlEnvelope<ControlRequest>,
    windows_connect_attempts: usize,
    response_wait: std::time::Duration,
) -> Result<Value, ControlRequestError> {
    let deadline = tokio::time::Instant::now() + response_wait;
    #[cfg(not(windows))]
    let _ = windows_connect_attempts;
    let mut encoded = serde_json::to_vec(envelope)
        .map_err(|error| ControlRequestError::Unavailable(error.to_string()))?;
    encoded.push(b'\n');
    #[cfg(target_os = "linux")]
    return send_linux_mailbox_request(
        data,
        &encoded,
        &envelope.request_id,
        remaining_control_wait(deadline)?,
    )
    .await;
    #[cfg(not(target_os = "linux"))]
    {
        #[cfg(all(unix, not(target_os = "linux")))]
        let socket_path = unix_control_socket_path(data);
        #[cfg(all(unix, not(target_os = "linux")))]
        let stream = tokio::time::timeout(
            CONTROL_PROBE_WAIT.min(remaining_control_wait(deadline)?),
            tokio::net::UnixStream::connect(socket_path),
        )
        .await
        .map_err(|_| {
            ControlRequestError::Unavailable(
                "Acyclic service connection exceeded the probe deadline".to_owned(),
            )
        })?
        .map_err(|error| {
            ControlRequestError::Unavailable(format!("Acyclic service is not running: {error}"))
        })?;
        // Only this user's own service may answer.
        #[cfg(all(unix, not(target_os = "linux")))]
        if stream
            .peer_cred()
            .map_err(|error| ControlRequestError::Unavailable(error.to_string()))?
            .uid()
            != rustix::process::getuid().as_raw()
        {
            return Err(ControlRequestError::Unavailable(
                "Acyclic service socket is served by another user".to_owned(),
            ));
        }
        #[cfg(windows)]
        let stream = connect_windows_control_pipe(data, windows_connect_attempts, deadline).await?;
        exchange_control_stream(
            stream,
            &encoded,
            &envelope.request_id,
            remaining_control_wait(deadline)?,
        )
        .await
    }
}

/// Connects to the service's pipe, trying `attempts` times while none
/// exists. A pipe whose every instance is connected is busy only until the
/// service creates the next, so that wait counts no attempt.
#[cfg(windows)]
#[allow(unsafe_code)]
pub(crate) async fn connect_windows_control_pipe(
    data: &Path,
    attempts: usize,
    deadline: tokio::time::Instant,
) -> Result<tokio::net::windows::named_pipe::NamedPipeClient, ControlRequestError> {
    use windows_sys::Win32::Foundation::ERROR_PIPE_BUSY;
    use windows_sys::Win32::System::Pipes::WaitNamedPipeW;

    let pipe = windows_control_pipe_path(data);
    let mut attempt = 1;
    loop {
        let remaining = remaining_control_wait(deadline)?;
        let error = match tokio::net::windows::named_pipe::ClientOptions::new().open(&pipe) {
            // Only this user's own service may answer.
            Ok(client) => {
                return match windows_pipe_server_is_this_user(&client) {
                    Ok(true) => Ok(client),
                    Ok(false) => Err(ControlRequestError::Unavailable(
                        "Acyclic service pipe is served by another user".to_owned(),
                    )),
                    Err(error) => Err(ControlRequestError::Unavailable(format!(
                        "cannot identify the Acyclic service pipe's server: {error}"
                    ))),
                };
            }
            Err(error) => error,
        };
        if error.raw_os_error() == i32::try_from(ERROR_PIPE_BUSY).ok() {
            let name = pipe.encode_utf16().chain([0]).collect::<Vec<_>>();
            let wait = u32::try_from(remaining.as_millis())
                .unwrap_or(u32::MAX)
                .max(1);
            // A failed wait (the pipe vanished) shows in the next open.
            // SAFETY: `name` is a live NUL-terminated UTF-16 string that the
            // task owns for the whole call.
            let _ =
                tokio::task::spawn_blocking(move || unsafe { WaitNamedPipeW(name.as_ptr(), wait) })
                    .await;
            continue;
        }
        if attempt >= attempts {
            return Err(ControlRequestError::Unavailable(format!(
                "Acyclic service is not running: {error}"
            )));
        }
        attempt += 1;
        tokio::time::sleep(std::time::Duration::from_millis(20).min(remaining)).await;
    }
}

/// Whether the process serving `client`'s pipe runs as this process's user.
#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "queries the pipe's server process and both processes' token users"
)]
pub(crate) fn windows_pipe_server_is_this_user(
    client: &tokio::net::windows::named_pipe::NamedPipeClient,
) -> io::Result<bool> {
    use std::os::windows::io::AsRawHandle as _;
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::System::Pipes::GetNamedPipeServerProcessId;
    use windows_sys::Win32::System::Threading::{
        GetCurrentProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };

    let mut server = 0_u32;
    // SAFETY: the pipe handle is live for the call and `server` is writable.
    if unsafe { GetNamedPipeServerProcessId(client.as_raw_handle() as HANDLE, &mut server) } == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: opening a process by id has no preconditions; a null handle is
    // an error.
    let process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, server) };
    if process.is_null() {
        return Err(io::Error::last_os_error());
    }
    let server_user = windows_token_user(process);
    // SAFETY: `process` was opened above and is closed once.
    unsafe { CloseHandle(process) };
    // SAFETY: the pseudo-handle of this process needs no closing.
    let own_user = windows_token_user(unsafe { GetCurrentProcess() })?;
    Ok(server_user? == own_user)
}

/// The security identifier of the user `process` runs as, as bytes.
#[cfg(windows)]
#[allow(unsafe_code, reason = "reads a process token's user")]
pub(crate) fn windows_token_user(
    process: windows_sys::Win32::Foundation::HANDLE,
) -> io::Result<Vec<u8>> {
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::Security::{
        GetLengthSid, GetTokenInformation, TOKEN_QUERY, TOKEN_USER, TokenUser,
    };
    use windows_sys::Win32::System::Threading::OpenProcessToken;

    let mut token: HANDLE = std::ptr::null_mut();
    // SAFETY: `process` is a live process handle and `token` is writable.
    if unsafe { OpenProcessToken(process, TOKEN_QUERY, &mut token) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let user = (|| {
        let mut length = 0_u32;
        // SAFETY: a null buffer of length zero asks only for the length.
        unsafe { GetTokenInformation(token, TokenUser, std::ptr::null_mut(), 0, &mut length) };
        let words = usize::try_from(length)
            .map_err(io::Error::other)?
            .div_ceil(8);
        let mut buffer = vec![0_u64; words.max(1)];
        // SAFETY: `buffer` holds `length` writable, 8-aligned bytes.
        if unsafe {
            GetTokenInformation(
                token,
                TokenUser,
                buffer.as_mut_ptr().cast(),
                length,
                &mut length,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the call filled a TOKEN_USER at the start of `buffer`,
        // whose SID points within it.
        let sid = unsafe { (*buffer.as_ptr().cast::<TOKEN_USER>()).User.Sid };
        // SAFETY: `sid` is a valid SID within `buffer`.
        let bytes = unsafe { GetLengthSid(sid) };
        let bytes = usize::try_from(bytes).map_err(io::Error::other)?;
        // SAFETY: the SID spans `bytes` readable bytes within `buffer`.
        Ok(unsafe { std::slice::from_raw_parts(sid.cast::<u8>(), bytes) }.to_vec())
    })();
    // SAFETY: `token` was opened above and is closed once.
    unsafe { CloseHandle(token) };
    user
}

#[cfg(windows)]
pub(crate) fn windows_control_pipe_path(data: &Path) -> String {
    format!(
        r"\\.\pipe\acyclic-{}",
        short_hash(data.as_os_str().to_string_lossy().as_bytes())
    )
}

pub(crate) fn remaining_control_wait(
    deadline: tokio::time::Instant,
) -> Result<std::time::Duration, ControlRequestError> {
    let now = tokio::time::Instant::now();
    if now >= deadline {
        return Err(ControlRequestError::Unavailable(
            "Acyclic service did not answer before the request deadline".to_owned(),
        ));
    }
    Ok(deadline - now)
}

#[cfg(not(target_os = "linux"))]
pub(crate) async fn exchange_control_stream(
    mut stream: impl AsyncRead + AsyncWrite + Unpin,
    encoded: &[u8],
    request_id: &control_protocol::RequestId,
    maximum_wait: std::time::Duration,
) -> Result<Value, ControlRequestError> {
    let exchange = async {
        stream
            .write_all(encoded)
            .await
            .map_err(|error| ControlRequestError::Indeterminate(error.to_string()))?;
        stream
            .flush()
            .await
            .map_err(|error| ControlRequestError::Indeterminate(error.to_string()))?;
        let mut response = Vec::new();
        let mut reader = BufReader::new(stream).take((MAXIMUM_CONTROL_MESSAGE_BYTES + 1) as u64);
        reader
            .read_until(b'\n', &mut response)
            .await
            .map_err(|error| ControlRequestError::Indeterminate(error.to_string()))?;
        Ok::<_, ControlRequestError>(response)
    };
    let mut response = tokio::time::timeout(maximum_wait, exchange)
        .await
        .map_err(|_| {
            ControlRequestError::Indeterminate(
                "Acyclic service did not answer before the request deadline".to_owned(),
            )
        })??;
    if response.len() > MAXIMUM_CONTROL_MESSAGE_BYTES || response.last() != Some(&b'\n') {
        return Err(ControlRequestError::Indeterminate(
            "invalid response from Acyclic service".to_owned(),
        ));
    }
    response.pop();
    decode_control_response(&response, request_id)
}

#[cfg(target_os = "linux")]
pub(crate) async fn send_linux_mailbox_request(
    data: &Path,
    encoded: &[u8],
    request_id: &control_protocol::RequestId,
    maximum_wait: std::time::Duration,
) -> Result<Value, ControlRequestError> {
    use std::time::{SystemTime, UNIX_EPOCH};

    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let mailbox = linux_control_mailbox_path(data);
    let nonce = format!(
        "{}:{}:{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    let name = short_hash(nonce.as_bytes());
    let unpublished = format!("{name}{LINUX_EXCHANGE_UNPUBLISHED}");
    let mailbox_directory = Arc::new(open_linux_directory(rustix::fs::CWD, &mailbox).map_err(
        |error| {
            ControlRequestError::Unavailable(format!("Acyclic service is not running: {error}"))
        },
    )?);
    let metadata = rustix::fs::fstat(&*mailbox_directory)
        .map_err(|error| ControlRequestError::Unavailable(error.to_string()))?;
    // Only this user's own service may answer: the mailbox must be private
    // to this user and owned by them.
    if metadata.st_mode & 0o777 != 0o700 || metadata.st_uid != rustix::process::getuid().as_raw() {
        return Err(ControlRequestError::Unavailable(
            "Acyclic service mailbox is not this user's private directory".to_owned(),
        ));
    }
    rustix::fs::mkdirat(&*mailbox_directory, &unpublished, rustix::fs::Mode::RWXU).map_err(
        |error| {
            ControlRequestError::Unavailable(format!(
                "cannot create Acyclic control exchange: {error}"
            ))
        },
    )?;
    let mut exchange = LinuxMailboxExchange {
        mailbox: Arc::clone(&mailbox_directory),
        exchange: None,
        name: unpublished.clone().into(),
    };
    let result = tokio::time::timeout(maximum_wait, async {
        let directory = open_linux_directory(&*mailbox_directory, &unpublished)
            .map_err(|error| ControlRequestError::Unavailable(error.to_string()))?;
        let directory = &*exchange.exchange.insert(directory);
        rustix::fs::mkfifoat(
            directory,
            LINUX_EXCHANGE_RESPONSE,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )
        .map_err(|error| ControlRequestError::Unavailable(error.to_string()))?;
        // Holding the write end as well means the response ends at its
        // newline, never at an end of file before the service opens it.
        let mut response_pipe = rustix::fs::openat(
            directory,
            LINUX_EXCHANGE_RESPONSE,
            rustix::fs::OFlags::RDWR
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::NONBLOCK
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(errno_to_io)
        .and_then(tokio::net::unix::pipe::Receiver::from_owned_fd)
        .map(|pipe| BufReader::new(pipe).take((MAXIMUM_CONTROL_MESSAGE_BYTES + 1) as u64))
        .map_err(|error| ControlRequestError::Unavailable(error.to_string()))?;
        let request_file = rustix::fs::openat(
            directory,
            LINUX_EXCHANGE_REQUEST,
            rustix::fs::OFlags::WRONLY
                | rustix::fs::OFlags::CREATE
                | rustix::fs::OFlags::EXCL
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )
        .map_err(|error| ControlRequestError::Unavailable(error.to_string()))?;
        std::fs::File::from(request_file)
            .write_all(encoded)
            .map_err(|error| ControlRequestError::Unavailable(error.to_string()))?;
        rustix::fs::renameat(
            &*mailbox_directory,
            &unpublished,
            &*mailbox_directory,
            &name,
        )
        .map_err(|error| ControlRequestError::Indeterminate(error.to_string()))?;
        exchange.name = name.into();
        let mut response = Vec::new();
        response_pipe
            .read_until(b'\n', &mut response)
            .await
            .map_err(|error| ControlRequestError::Indeterminate(error.to_string()))?;
        if response.len() > MAXIMUM_CONTROL_MESSAGE_BYTES || response.pop() != Some(b'\n') {
            return Err(ControlRequestError::Indeterminate(
                "invalid response from Acyclic service".to_owned(),
            ));
        }
        decode_control_response(&response, request_id)
    })
    .await
    .unwrap_or_else(|_| {
        Err(ControlRequestError::Indeterminate(
            "Acyclic service did not answer before the filesystem control deadline".to_owned(),
        ))
    });
    // The service removes every exchange that it answers.
    if result.is_err() {
        exchange.remove();
    }
    result
}

pub(crate) fn decode_control_response(
    response: &[u8],
    request_id: &control_protocol::RequestId,
) -> Result<Value, ControlRequestError> {
    let response: Value = serde_json::from_slice(response)
        .map_err(|error| ControlRequestError::Indeterminate(error.to_string()))?;
    if response.get("requestId").and_then(Value::as_str) != Some(request_id.as_str()) {
        return Err(ControlRequestError::Indeterminate(
            "Acyclic control response does not match the request identity".to_owned(),
        ));
    }
    if response.get("ok").and_then(Value::as_bool) == Some(true) {
        Ok(response.get("result").cloned().unwrap_or(Value::Null))
    } else {
        Err(ControlRequestError::Response(
            response
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("Acyclic service request failed")
                .to_owned(),
        ))
    }
}
