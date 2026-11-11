//! Exercise native startup and retirement with hidden or read-only fusectl.

use super::*;
use nix::mount::{MsFlags, mount};
use std::process::Command;

const MODE: &str = "ACYCLIC_FS_ABORT_CONTROL_CASE";
const CONNECTIONS: &str = "/sys/fs/fuse/connections";
const STALE_CHILD: &str = "ACYCLIC_FS_STALE_MOUNT_TEST_CHILD";
const REQUIRE_DIRECT: &str = "ACYCLIC_FS_TEST_DIRECT_FUSE";

#[test]
fn linux_dead_mountpoint_is_reclaimed_without_fusermount_with_negative_control()
-> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::MetadataExt as _;
    if std::env::var_os(STALE_CHILD).is_none() {
        let temporary = tempfile::tempdir()?;
        let output = Command::new(std::env::current_exe()?)
            .args(["native_mount::fuse::tests::connection::linux_dead_mountpoint_is_reclaimed_without_fusermount_with_negative_control", "--exact", "--nocapture"])
            .current_dir(temporary.path())
            .env(STALE_CHILD, "1")
            .output()?;
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return Ok(());
    }
    crate::initialize_native_mount_services()?;
    // The child owns its cwd, so the empty-parent relative path is exercised
    // without changing the cwd of any concurrent test.
    let root = std::path::PathBuf::from("mount");
    std::fs::create_dir(&root)?;
    let before = std::fs::metadata(&root)?;
    let work = fuse_fixture_work();
    let mut mount = super::super::mount::Mount::default();
    let mut device = None;
    let fence = std::fs::File::open("/dev/null")?;
    mount.open_into(
        &root,
        true,
        || {
            assert!(
                std::env::var_os(REQUIRE_DIRECT).is_none(),
                "qualified direct mount must not discover fusermount"
            );
            super::super::lifecycle::fuse_recovery_helper(&work)
        },
        &mut device,
        &fence,
        &work,
    )?;
    // Closing the only device descriptor is the exact kernel disconnect event;
    // the mount remains in the namespace, but has no FUSE server.
    drop(device.take());
    let failed = std::fs::metadata(&root).unwrap_err();
    assert_eq!(
        failed.raw_os_error(),
        Some(libc::ENOTCONN),
        "old startup stat negative control"
    );
    // The first owner is gone. Restart through the public startup entry, so
    // recovery must happen before its first stat, not just inside a helper.
    mount.cancel_exit_action()?;
    let (source, _) = shared_sources(posix_config())?;
    let volume_id = source.volume_id()?;
    let mut session = crate::mount_native(
        crate::NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id,
            destination: root.clone(),
            writable: true,
            shared: false,
        },
        Arc::new(source),
    )?;
    std::fs::write(root.join("child"), b"restarted")?;
    assert_eq!(std::fs::read(root.join("child"))?, b"restarted");
    assert!(session.stop()?);
    let after = std::fs::metadata(&root)?;
    assert_eq!((after.dev(), after.ino()), (before.dev(), before.ino()));
    assert!(!crate::recover_disconnected_native_mount_destination(
        &root
    )?);
    Ok(())
}

#[test]
#[ignore = "requires a live Linux FUSE mount and mount namespace privileges"]
fn linux_mount_serves_without_abort_control_and_retains_available_control()
-> Result<(), Box<dyn std::error::Error>> {
    if let Ok(mode) = std::env::var(MODE) {
        return isolated_case(&mode);
    }
    for mode in ["missing", "read-only", "hidden-after-start"] {
        let output = Command::new("unshare")
            .args(["--mount", "--propagation", "private", "--"])
            .arg(std::env::current_exe()?)
            .args([
                "native_mount::fuse::tests::connection::linux_mount_serves_without_abort_control_and_retains_available_control",
                "--exact",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(MODE, mode)
            .output()?;
        assert!(
            output.status.success(),
            "{mode}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
    Ok(())
}

fn isolated_case(mode: &str) -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let hidden = temporary.path().join("hidden");
    std::fs::create_dir(&hidden)?;
    if mode == "read-only" {
        mount(
            Some(CONNECTIONS),
            CONNECTIONS,
            None::<&str>,
            MsFlags::MS_BIND,
            None::<&str>,
        )?;
        mount(
            None::<&str>,
            CONNECTIONS,
            None::<&str>,
            MsFlags::MS_BIND | MsFlags::MS_REMOUNT | MsFlags::MS_RDONLY,
            None::<&str>,
        )?;
    } else if mode == "missing" {
        hide_connections(&hidden)?;
    }
    for shared in [false, true] {
        let root = temporary.path().join(format!("views-{shared}"));
        std::fs::create_dir(&root)?;
        let destination = if shared {
            root.join("view")
        } else {
            root.clone()
        };
        let (source, _) = shared_sources(posix_config())?;
        let result = crate::mount_native(
            crate::NativeMountRequest {
                mount_id: crate::MountId::new(),
                volume_id: source.volume_id()?,
                destination,
                writable: true,
                shared,
            },
            Arc::new(source),
        );
        if mode == "hidden-after-start" {
            let mut session = result?;
            // Retain a kernel borrower and the notifier after all readers exit.
            let held = shared.then(|| std::fs::File::open(&root)).transpose()?;
            hide_connections(&hidden)?;
            assert!(session.stop()?);
            if let Some(held) = held {
                let error = rustix::fs::openat(
                    &held,
                    "unserved",
                    rustix::fs::OFlags::RDONLY,
                    rustix::fs::Mode::empty(),
                )
                .expect_err("the retained abort control ends the connection");
                assert_eq!(error.raw_os_error(), libc::ENOTCONN);
            }
            nix::mount::umount(CONNECTIONS)?;
        } else {
            let mut session = result?;
            std::fs::write(root.join("child"), b"without abort control")?;
            assert_eq!(std::fs::read(root.join("child"))?, b"without abort control");
            assert!(session.stop()?);
        }
        let table = std::fs::read_to_string("/proc/self/mountinfo")?;
        assert!(
            !table
                .lines()
                .filter_map(|line| line.split(' ').nth(4))
                .any(|point| Path::new(point) == root),
            "startup or retirement left a mount at {}",
            root.display(),
        );
    }
    Ok(())
}

fn hide_connections(hidden: &Path) -> Result<(), nix::errno::Errno> {
    mount(
        Some(hidden),
        CONNECTIONS,
        None::<&str>,
        MsFlags::MS_BIND,
        None::<&str>,
    )
}
