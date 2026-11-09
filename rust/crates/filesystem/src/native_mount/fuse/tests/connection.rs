//! Exercise abort authority against real, privately hidden or read-only fusectl.

use super::*;
use nix::mount::{MsFlags, mount};
use std::process::Command;

const MODE: &str = "ACYCLIC_FS_ABORT_CONTROL_CASE";
const CONNECTIONS: &str = "/sys/fs/fuse/connections";

#[test]
#[ignore = "requires a live Linux FUSE mount and mount namespace privileges"]
fn linux_abort_control_is_acquired_before_admission_and_retained()
-> Result<(), Box<dyn std::error::Error>> {
    if let Ok(mode) = std::env::var(MODE) {
        return isolated_case(&mode);
    }
    for mode in ["missing", "read-only", "hidden-after-start"] {
        let output = Command::new("unshare")
            .args(["--mount", "--propagation", "private", "--"])
            .arg(std::env::current_exe()?)
            .args([
                "native_mount::fuse::tests::connection::linux_abort_control_is_acquired_before_admission_and_retained",
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
            let Err(NativeMountError::StartupFailed(mut failure)) = result else {
                panic!("{mode} fusectl did not reject mount startup");
            };
            let NativeMountError::Io(error) = failure.error() else {
                panic!("unexpected startup failure: {failure}");
            };
            assert_eq!(
                error.raw_os_error(),
                Some(if mode == "missing" {
                    libc::ENOENT
                } else {
                    libc::EROFS
                }),
            );
            assert!(failure.owner_mut().stop()?);
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
