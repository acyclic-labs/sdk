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
        // This fails with ENOTCONN when process exit leaves a dead FUSE mount.
        // It also checks root/private mounts, not only the child project mount.
        std::fs::remove_dir_all(&checkout).unwrap();
    }
}
