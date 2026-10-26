//! Real native owner shutdown must retire mounts before its runtime exits.
use acyclic_harness::{
    TaskId,
    local::{host_files, host_owner::HostAction},
};
use std::{
    process::Command,
    time::{Duration, Instant},
};

struct Running(std::process::Child);
impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[cfg(target_os = "linux")]
fn mounts_in(root: &std::path::Path) -> std::io::Result<Vec<std::path::PathBuf>> {
    Ok(std::fs::read_to_string("/proc/self/mountinfo")?
        .lines()
        .filter_map(|line| line.split_whitespace().nth(4))
        .map(std::path::PathBuf::from)
        .filter(|path| path.starts_with(root))
        .collect())
}

#[test]
fn native_workspace_shutdown_retires_mounts_on_interrupt_and_terminate() {
    for signal in ["TERM", "INT"] {
        let fixture = tempfile::tempdir().unwrap();
        let checkout = fixture.path().join("workspace");
        std::fs::create_dir(&checkout).unwrap();
        let mut owner = Running(
            Command::new(super::build_metadata::EXECUTABLE)
                .env_clear()
                .env("PATH", "/usr/bin:/bin:/usr/sbin:/sbin")
                .env("HOME", fixture.path())
                .env("TMPDIR", fixture.path())
                .env("XDG_RUNTIME_DIR", fixture.path())
                .env("CODEX_HOME", fixture.path().join("fixture-codex-home"))
                .env("XDG_STATE_HOME", fixture.path().join("fixture-state"))
                .current_dir(&checkout)
                .args(["--host-workspace", "--host-state-directory"])
                .arg(fixture.path().join("owner-state"))
                .arg("--workspace")
                .arg(&checkout)
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(2);
        while !checkout
            .join(host_files::DIRECTORY)
            .join("owner.json")
            .is_file()
        {
            assert!(
                owner.0.try_wait().unwrap().is_none(),
                "owner exited before readiness"
            );
            assert!(
                Instant::now() < deadline,
                "owner readiness exceeded fixture bound"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        let root = host_files::request(
            &checkout,
            HostAction::Open {
                host: "pi".into(),
                session: "shutdown-fixture".into(),
            },
        )
        .unwrap();
        let task: TaskId = serde_json::from_value(root.get("task").unwrap().clone()).unwrap();
        let child = host_files::request(
            &checkout,
            HostAction::Fork {
                parent: task,
                reference: "child".into(),
            },
        )
        .unwrap();
        let view = std::path::PathBuf::from(child.get("cwd").unwrap().as_str().unwrap());
        std::fs::write(view.join("native-write.txt"), "captured before shutdown").unwrap();
        let child: TaskId = serde_json::from_value(child.get("task").unwrap().clone()).unwrap();
        host_files::request(&checkout, HostAction::Capture { task: child }).unwrap();
        #[cfg(target_os = "linux")]
        {
            let active = mounts_in(fixture.path()).unwrap();
            assert!(
                active.iter().any(|root| view.starts_with(root)),
                "shutdown qualification requires a child in a live shared mount: {view:?}; active={active:?}"
            );
        }
        assert!(
            Command::new("kill")
                .args(["-s", signal, &owner.0.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(status) = owner.0.try_wait().unwrap() {
                assert!(
                    status.success(),
                    "{signal} bypassed native owner cleanup: {status}"
                );
                break;
            }
            assert!(
                Instant::now() < deadline,
                "owner shutdown exceeded fixture bound"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        #[cfg(target_os = "linux")]
        {
            let remaining = mounts_in(fixture.path()).unwrap();
            assert!(
                remaining.is_empty(),
                "native workspace mount outlived its owner: {remaining:?}"
            );
        }
        // Check kernel mount ownership before traversing directories, including
        // root/private mounts as well as the child project mount.
        std::fs::remove_dir_all(&checkout).unwrap();
    }
}
