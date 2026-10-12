use super::{ACYCLIC, command, isolated_state};
use std::{fs, path::{Path, PathBuf}, process::Command};

pub struct PackagedPlugin {
    pub root: PathBuf,
    pub native: PathBuf,
}

impl PackagedPlugin {
    /// Runs the package's `acyclic` command through a shell, as the npm-linked
    /// command does: `bin/acyclic` is the executable on Unix and resolves to
    /// `acyclic.exe` on Windows.
    pub fn command(&self, arguments: &[&str]) -> Command {
        let invocation = format!(
            "\"{}\" {}",
            self.root.join("bin").join("acyclic").display(),
            arguments.join(" ")
        );
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt as _;
            let mut shell = command("cmd");
            shell.raw_arg(format!("/D /S /C \"{invocation}\""));
            shell
        }
        #[cfg(not(windows))]
        {
            let mut shell = command("sh");
            shell.args(["-c", &invocation]);
            shell
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
    let package = acyclic_native_runtime::process_output(
        command("node")
            .arg(plugin.join("scripts/package.mjs"))
            .args(["--binary", &format!("{}={ACYCLIC}", target_name()), "--out"])
            .arg(&package_root),
        std::time::Duration::from_secs(120),
        8 * 1024 * 1024,
    )
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
    isolated_state(&mut install, temporary);
    let installed = acyclic_native_runtime::process_output(
        &mut install,
        std::time::Duration::from_secs(120),
        8 * 1024 * 1024,
    )
    .expect("install packaged binary");
    assert!(
        installed.status.success(),
        "{}",
        String::from_utf8_lossy(&installed.stderr)
    );
    PackagedPlugin {
        native: root.join(if cfg!(windows) {
            "bin/acyclic.exe"
        } else {
            "bin/acyclic"
        }),
        root,
    }
}

pub fn installed_host_binary(name: &str, override_name: &str) -> Option<PathBuf> {
    if let Some(path) = std::env::var_os(override_name) {
        return executable_host_path(name, PathBuf::from(path));
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
        .find_map(|candidate| executable_host_path(name, candidate))
}

fn executable_host_path(name: &str, candidate: PathBuf) -> Option<PathBuf> {
    if !candidate.is_file() {
        return None;
    }
    #[cfg(windows)]
    {
        if candidate
            .extension()
            .is_some_and(|extension| extension == "exe")
        {
            return Some(candidate);
        }
        if name != "codex" {
            return None;
        }
        let (package, target) = match std::env::consts::ARCH {
            "x86_64" => ("codex-win32-x64", "x86_64-pc-windows-msvc"),
            "aarch64" => ("codex-win32-arm64", "aarch64-pc-windows-msvc"),
            _ => return None,
        };
        let native = candidate
            .parent()?
            .join("node_modules")
            .join("@openai")
            .join("codex")
            .join("node_modules")
            .join("@openai")
            .join(package)
            .join("vendor")
            .join(target)
            .join("bin")
            .join("codex.exe");
        native.is_file().then_some(native)
    }
    #[cfg(not(windows))]
    {
        let _ = name;
        Some(candidate)
    }
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
        // Windows has only the read-only attribute; clearing it is exact.
        #[cfg(windows)]
        #[allow(
            clippy::permissions_set_readonly_false,
            reason = "Windows has only the read-only attribute, so clearing it is exact"
        )]
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
