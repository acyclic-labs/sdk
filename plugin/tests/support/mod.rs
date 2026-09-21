mod scripted_provider;

pub use scripted_provider::{ProviderProtocol, ScriptedProvider};

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
    active: bool,
}

impl ServiceGuard {
    pub fn new(launcher: &Path, home: &Path) -> Self {
        Self {
            launcher: launcher.to_path_buf(),
            home: home.to_path_buf(),
            active: true,
        }
    }

    pub fn drain(mut self) {
        let (output, expired) = self.drain_output();
        self.active = false;
        assert!(
            output.status.success() && !expired,
            "service drain failed (expired={expired}): {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn drain_output(&self) -> (Output, bool) {
        let mut drain = command("node");
        drain.arg(&self.launcher).arg("__service-drain");
        isolated_state(&mut drain, &self.home);
        output_with_timeout(&mut drain, Duration::from_secs(10))
    }
}

impl Drop for ServiceGuard {
    fn drop(&mut self) {
        if self.active {
            let _ = self.drain_output();
        }
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

pub fn output_with_timeout(command: &mut Command, timeout: Duration) -> (Output, bool) {
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn bounded child process");
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait().expect("poll bounded child process") {
            Some(_) => {
                return (
                    child
                        .wait_with_output()
                        .expect("collect bounded child output"),
                    false,
                );
            }
            None if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(10)),
            None => {
                child.kill().expect("kill expired child process");
                return (
                    child
                        .wait_with_output()
                        .expect("collect expired child output"),
                    true,
                );
            }
        }
    }
}

pub fn target_name() -> &'static str {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => "linux-x64",
        ("linux", "aarch64") => "linux-arm64",
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
