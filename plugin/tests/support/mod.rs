mod scripted_provider;

pub use scripted_provider::{ProviderProtocol, ScriptedProvider};

use acyclic_native_runtime::{ProcessTree, spawn_process_tree};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

pub struct PackagedPlugin {
    pub root: PathBuf,
    pub launcher: PathBuf,
}

pub struct ServiceGuard {
    launcher: PathBuf,
    home: PathBuf,
    identity: Option<String>,
    process_tree: Option<ProcessTree>,
    active: bool,
}

impl ServiceGuard {
    pub fn new(launcher: &Path, home: &Path) -> Self {
        Self {
            launcher: launcher.to_path_buf(),
            home: home.to_path_buf(),
            identity: None,
            process_tree: None,
            active: true,
        }
    }

    pub fn attach_process_tree(&mut self, process_tree: ProcessTree) {
        assert!(self.process_tree.replace(process_tree).is_none());
    }

    pub fn assert_hook_service_live(&mut self) -> String {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut last = String::new();
        while Instant::now() < deadline {
            match self.status() {
                Ok(status) => {
                    let marker = status.get("markerIdentity").and_then(Value::as_str);
                    let reachable = status.get("reachableIdentity").and_then(Value::as_str);
                    if let Some(marker) = marker
                        && Some(marker) == reachable
                        && status.get("lockAcquirable").and_then(Value::as_bool) == Some(false)
                    {
                        let identity = marker.to_owned();
                        self.identity = Some(identity.clone());
                        return identity;
                    }
                    last = status.to_string();
                }
                Err(error) => last = error,
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        panic!("installed hook did not establish a reachable locked service: {last}");
    }

    pub fn drain(mut self) {
        if let Err(error) = self.cleanup() {
            let fallback = self.force_cleanup();
            panic!("service drain failed: {error}; forced cleanup: {fallback:?}");
        }
    }

    pub fn assert_timeout_cleanup(mut self, process_tree: ProcessTree) {
        self.attach_process_tree(process_tree);
        let marker = service_data(&self.home).join("service.identity");
        let identity = fs::read_to_string(&marker).expect("timed-out host started hook service");
        assert!(!identity.is_empty(), "timed-out hook service identity");
        self.identity = Some(identity);
        if let Err(error) = self.cleanup() {
            self.force_cleanup().unwrap_or_else(|fallback| {
                panic!(
                    "timed-out host service was not cleaned: graceful drain: {error}; \
                     forced cleanup: {fallback}"
                )
            });
        }
    }

    fn cleanup(&mut self) -> Result<(), String> {
        let identity = self
            .identity
            .as_deref()
            .ok_or_else(|| "service liveness was not positively observed".to_owned())?
            .to_owned();
        let mut drain = command("node");
        drain.arg(&self.launcher).arg("__service-drain");
        isolated_state(&mut drain, &self.home);
        let BoundedOutput {
            output,
            expired,
            process_tree,
        } = try_output_with_timeout(&mut drain, Duration::from_secs(10))
            .map_err(|error| error.to_string())?;
        drop(process_tree);
        if expired || !output.status.success() {
            return Err(format!(
                "drain command expired={expired}: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        self.verify_drained(&identity)?;
        self.active = false;
        drop(self.process_tree.take());
        Ok(())
    }

    fn verify_drained(&self, identity: &str) -> Result<(), String> {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut last = String::new();
        while Instant::now() < deadline {
            match self.status() {
                Ok(status)
                    if status.get("markerIdentity").is_some_and(Value::is_null)
                        && status.get("reachableIdentity").is_some_and(Value::is_null)
                        && status.get("lockAcquirable").and_then(Value::as_bool) == Some(true) =>
                {
                    let drain: Value = serde_json::from_slice(
                        &fs::read(service_data(&self.home).join("service-drain.json"))
                            .map_err(|error| error.to_string())?,
                    )
                    .map_err(|error| error.to_string())?;
                    if drain.get("identity").and_then(Value::as_str) == Some(identity)
                        && drain.get("ok").and_then(Value::as_bool) == Some(true)
                    {
                        return Ok(());
                    }
                    return Err(format!("durable drain evidence is invalid: {drain}"));
                }
                Ok(status) => last = status.to_string(),
                Err(error) => last = error,
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        Err(format!("service remained reachable or locked: {last}"))
    }

    fn force_cleanup(&mut self) -> Result<(), String> {
        let tree = self
            .process_tree
            .as_mut()
            .ok_or_else(|| "host process tree was not attached".to_owned())?;
        tree.terminate().map_err(|error| error.to_string())?;
        let deadline = Instant::now() + Duration::from_secs(5);
        let marker = service_data(&self.home).join("service.identity");
        let mut last = String::new();
        while Instant::now() < deadline {
            match self.status() {
                Ok(status)
                    if status.get("reachableIdentity").is_some_and(Value::is_null)
                        && status.get("lockAcquirable").and_then(Value::as_bool) == Some(true) =>
                {
                    match fs::remove_file(&marker) {
                        Ok(()) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => return Err(error.to_string()),
                    }
                    self.active = false;
                    return assert_service_absent(&self.launcher, &self.home);
                }
                Ok(status) => last = status.to_string(),
                Err(error) => last = error,
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        Err(format!(
            "forced process-tree cleanup did not release service: {last}"
        ))
    }

    fn status(&self) -> Result<Value, String> {
        service_status(&self.launcher, &self.home)
    }
}

impl Drop for ServiceGuard {
    fn drop(&mut self) {
        if self.active {
            let cleanup = self.cleanup();
            if let Err(error) = cleanup {
                let fallback = self.force_cleanup();
                if std::thread::panicking() {
                    eprintln!(
                        "service cleanup failed during unwind: {error}; fallback: {fallback:?}"
                    );
                } else {
                    panic!("service cleanup failed: {error}; fallback: {fallback:?}");
                }
            }
        }
    }
}

pub fn assert_service_absent(launcher: &Path, home: &Path) -> Result<(), String> {
    let status = service_status(launcher, home)?;
    if status.get("markerIdentity").is_some_and(Value::is_null)
        && status.get("reachableIdentity").is_some_and(Value::is_null)
        && status.get("lockAcquirable").and_then(Value::as_bool) == Some(true)
    {
        Ok(())
    } else {
        Err(format!("unexpected hook service state: {status}"))
    }
}

fn service_status(launcher: &Path, home: &Path) -> Result<Value, String> {
    let mut status = command("node");
    status.arg(launcher).arg("__service-status");
    isolated_state(&mut status, home);
    let BoundedOutput {
        output,
        expired,
        process_tree,
    } = try_output_with_timeout(&mut status, Duration::from_secs(5))
        .map_err(|error| error.to_string())?;
    drop(process_tree);
    if expired || !output.status.success() {
        return Err(format!(
            "service status expired={expired}: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    serde_json::from_slice(&output.stdout).map_err(|error| error.to_string())
}

fn service_data(home: &Path) -> PathBuf {
    if cfg!(windows) {
        home.join("local").join("Acyclic").join("state-v2")
    } else {
        home.join("state").join("acyclic").join("state-v2")
    }
}

pub const ACYCLIC: &str = env!("CARGO_BIN_EXE_acyclic");

pub fn command(program: impl AsRef<OsStr>) -> Command {
    let mut command = Command::new(program);
    command.env_remove("ACYCLIC_INSTALL_TEST_CRASH");
    command.env_remove("ACYCLIC_INSTALL_TEST_FAIL");
    command
}

pub fn isolated_state(command: &mut Command, root: &Path) {
    for directory in ["config", "state", "data", "local", "codex"] {
        fs::create_dir_all(root.join(directory)).expect("isolated host directory");
    }
    command.env("HOME", root);
    command.env("USERPROFILE", root);
    command.env("XDG_CONFIG_HOME", root.join("config"));
    command.env("XDG_STATE_HOME", root.join("state"));
    command.env("XDG_DATA_HOME", root.join("data"));
    command.env("LOCALAPPDATA", root.join("local"));
    command.env("CODEX_HOME", root.join("codex"));
}

pub fn output_with_stdin(command: &mut Command, input: &[u8]) -> Output {
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn child process");
    child
        .stdin
        .take()
        .expect("child stdin")
        .write_all(input)
        .expect("write child stdin");
    child.wait_with_output().expect("wait for child process")
}

pub struct BoundedOutput {
    pub output: Output,
    pub expired: bool,
    pub process_tree: ProcessTree,
}

pub fn output_with_timeout(command: &mut Command, timeout: Duration) -> BoundedOutput {
    try_output_with_timeout(command, timeout).expect("spawn bounded process tree")
}

fn try_output_with_timeout(
    command: &mut Command,
    timeout: Duration,
) -> std::io::Result<BoundedOutput> {
    let mut process_tree = spawn_process_tree(
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped()),
    )?;
    let deadline = Instant::now() + timeout;
    loop {
        match process_tree.try_wait()? {
            Some(_) => {
                let output = process_tree.wait_with_output()?;
                return Ok(BoundedOutput {
                    output,
                    expired: false,
                    process_tree,
                });
            }
            None if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(10)),
            None => {
                process_tree.terminate_descendants()?;
                let output = process_tree.wait_with_output()?;
                return Ok(BoundedOutput {
                    output,
                    expired: true,
                    process_tree,
                });
            }
        }
    }
}

pub fn target_name() -> &'static str {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") if cfg!(target_env = "musl") => "linux-x64-musl",
        ("linux", "x86_64") => "linux-x64-gnu",
        ("linux", "aarch64") if cfg!(target_env = "musl") => "linux-arm64-musl",
        ("linux", "aarch64") => "linux-arm64-gnu",
        ("macos", "x86_64") => "darwin-x64",
        ("macos", "aarch64") => "darwin-arm64",
        ("windows", "x86_64") => "win32-x64",
        ("windows", "aarch64") => "win32-arm64",
        combination => panic!("unsupported E2E target: {combination:?}"),
    }
}

pub fn package_production_plugin(temporary: &Path) -> PackagedPlugin {
    let plugin = Path::new(env!("CARGO_MANIFEST_DIR"));
    let package_root = temporary.join("package");
    let package = command("node")
        .arg(plugin.join("scripts/package.mjs"))
        .args(["--binary", &format!("{}={ACYCLIC}", target_name()), "--out"])
        .arg(&package_root)
        .output()
        .expect("run package builder");
    assert!(
        package.status.success(),
        "{}",
        String::from_utf8_lossy(&package.stderr)
    );
    let root = package_root.join("plugin");
    let mut install = command("node");
    install.arg(root.join("bin/install.js"));
    install.env("NODE_ENV", "test");
    install.env("ACYCLIC_INSTALL_SKIP_DRAIN", "1");
    isolated_state(&mut install, temporary);
    let installed = install.output().expect("install packaged binary");
    assert!(
        installed.status.success(),
        "{}",
        String::from_utf8_lossy(&installed.stderr)
    );
    PackagedPlugin {
        launcher: root.join("bin/acyclic.js"),
        root,
    }
}

pub fn installed_host_binary(name: &str, override_name: &str) -> Option<PathBuf> {
    if let Some(path) = std::env::var_os(override_name) {
        return Some(PathBuf::from(path));
    }
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .flat_map(|directory| {
            #[cfg(windows)]
            let candidates = [
                format!("{name}.exe"),
                format!("{name}.cmd"),
                name.to_owned(),
            ];
            #[cfg(not(windows))]
            let candidates = [name.to_owned()];
            candidates.map(move |candidate| directory.join(candidate))
        })
        .find(|candidate| candidate.is_file())
}

pub fn tree_snapshot(root: &Path) -> BTreeMap<PathBuf, (u64, String)> {
    fn visit(root: &Path, current: &Path, snapshot: &mut BTreeMap<PathBuf, (u64, String)>) {
        let mut entries = fs::read_dir(current)
            .expect("read snapshot directory")
            .collect::<Result<Vec<_>, _>>()
            .expect("read snapshot entries");
        entries.sort_by_key(fs::DirEntry::file_name);
        for entry in entries {
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path).expect("snapshot metadata");
            if metadata.is_dir() {
                visit(root, &path, snapshot);
            } else if metadata.is_file() {
                let bytes = fs::read(&path).expect("snapshot file");
                snapshot.insert(
                    path.strip_prefix(root)
                        .expect("snapshot prefix")
                        .to_path_buf(),
                    (metadata.len(), hex::encode(Sha256::digest(bytes))),
                );
            }
        }
    }
    let mut snapshot = BTreeMap::new();
    visit(root, root, &mut snapshot);
    snapshot
}

pub fn write_qualification_receipt(host: &str, host_binary: &Path, invariants: &[&str]) {
    let Some(directory) = std::env::var_os("ACYCLIC_E2E_RECEIPT_DIR") else {
        return;
    };
    let version = command(host_binary)
        .arg("--version")
        .output()
        .expect("read host version");
    assert!(version.status.success(), "host --version failed");
    let package = qualification_environment("ACYCLIC_E2E_HOST_PACKAGE");
    let package_version = qualification_environment("ACYCLIC_E2E_HOST_PACKAGE_VERSION");
    let package_integrity = qualification_environment("ACYCLIC_E2E_HOST_PACKAGE_INTEGRITY");
    let platform_package = qualification_environment("ACYCLIC_E2E_HOST_PLATFORM_PACKAGE");
    let platform_version = qualification_environment("ACYCLIC_E2E_HOST_PLATFORM_VERSION");
    let platform_integrity = qualification_environment("ACYCLIC_E2E_HOST_PLATFORM_INTEGRITY");
    let lock_sha256 = qualification_environment("ACYCLIC_E2E_HOST_LOCK_SHA256");
    assert!(
        String::from_utf8_lossy(&version.stdout).contains(&package_version),
        "host version does not match the qualification lock"
    );
    let receipt = serde_json::json!({
        "schema": "acyclic-agent-qualification-v1",
        "acyclic": {
            "package": env!("CARGO_PKG_NAME"),
            "version": env!("CARGO_PKG_VERSION"),
            "executable_sha256": file_sha256(Path::new(ACYCLIC)),
        },
        "host": {
            "name": host,
            "version": String::from_utf8_lossy(&version.stdout).trim(),
            "executable_sha256": file_sha256(host_binary),
            "lock_sha256": lock_sha256,
            "package": {
                "name": package,
                "version": package_version,
                "integrity": package_integrity,
            },
            "platform_package": {
                "name": platform_package,
                "version": platform_version,
                "integrity": platform_integrity,
            },
        },
        "platform": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "backend": match std::env::consts::OS {
                "linux" => "linux-fuse",
                "macos" => "macos-nfs",
                "windows" => "windows-projfs",
                other => other,
            },
        },
        "scenario_corpus_sha256": tree_digest(&Path::new(env!("CARGO_MANIFEST_DIR")).join("tests")),
        "satisfied_invariants": invariants,
        "passed": true,
    });
    let directory = PathBuf::from(directory);
    fs::create_dir_all(&directory).expect("receipt directory");
    fs::write(
        directory.join(format!(
            "{host}-{}-{}.json",
            std::env::consts::OS,
            std::env::consts::ARCH
        )),
        format!(
            "{}\n",
            serde_json::to_string_pretty(&receipt).expect("serialize qualification receipt")
        ),
    )
    .expect("write qualification receipt");
}

fn qualification_environment(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is required for a qualification receipt"))
}

fn file_sha256(path: &Path) -> String {
    hex::encode(Sha256::digest(
        fs::read(path).expect("hash qualification file"),
    ))
}

fn tree_digest(root: &Path) -> String {
    let snapshot = tree_snapshot(root);
    let mut digest = Sha256::new();
    for (path, (length, hash)) in snapshot {
        digest.update(path.to_string_lossy().as_bytes());
        digest.update([0]);
        digest.update(length.to_le_bytes());
        digest.update(hash.as_bytes());
        digest.update([0]);
    }
    hex::encode(digest.finalize())
}

pub fn make_read_only(root: &Path) {
    fn visit(path: &Path) {
        let metadata = fs::symlink_metadata(path).expect("read-only metadata");
        if metadata.is_dir() {
            for entry in fs::read_dir(path).expect("read-only directory") {
                visit(&entry.expect("read-only entry").path());
            }
        }
        let mut permissions = metadata.permissions();
        permissions.set_readonly(true);
        fs::set_permissions(path, permissions).expect("mark package read-only");
    }
    visit(root);
}

pub fn make_writable(root: &Path) {
    fn visit(path: &Path) {
        let Ok(metadata) = fs::symlink_metadata(path) else {
            return;
        };
        let mut permissions = metadata.permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            permissions.set_mode(if metadata.is_dir() { 0o755 } else { 0o644 });
        }
        #[cfg(windows)]
        permissions.set_readonly(false);
        fs::set_permissions(path, permissions).expect("restore package permissions");
        if metadata.is_dir() {
            for entry in fs::read_dir(path).expect("writable directory") {
                visit(&entry.expect("writable entry").path());
            }
        }
    }
    visit(root);
}
