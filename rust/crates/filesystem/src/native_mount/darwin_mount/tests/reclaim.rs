use super::*;
use std::io::{BufRead as _, Write as _};
use std::process::Stdio;

const CHILD: &str = "native_mount::darwin_mount::tests::reclaim::crashed_owner";

fn request(
    destination: &Path,
    source: &MemorySource,
) -> Result<crate::NativeMountRequest, NativeMountError> {
    Ok(crate::NativeMountRequest {
        mount_id: crate::MountId::new(),
        volume_id: source.volume_id()?,
        destination: destination.to_path_buf(),
        writable: true,
    })
}

#[test]
#[ignore = "subprocess fixture for the live macOS crash recovery test"]
fn crashed_owner() -> TestResult {
    let Some(root) = std::env::var_os("ACYCLIC_RECLAIM_TEST_ROOT") else {
        return Ok(());
    };
    let root = PathBuf::from(root);
    let (source, _, work) = checkout_context(MountPublication::Manual)?;
    let _owned = crate::mount_native(request(&root.join("owned"), &source)?, source.clone())?;
    // Another user of the same NFS transport is not an SDK-owned destination.
    let (foreign, _, _) = checkout_context(MountPublication::Manual)?;
    let mut session = None;
    DarwinMountSession::start_into(
        &request(&root.join("foreign"), &foreign)?,
        foreign,
        &work,
        &mut session,
        &mut None,
        |_| Ok(()),
    )?;
    println!("RECLAIM_READY");
    std::io::stdout().flush()?;
    let mut command = String::new();
    std::io::stdin().read_line(&mut command)?;
    Err("crash fixture must be killed".into())
}

struct CrashRoots(tempfile::TempDir);

impl Drop for CrashRoots {
    fn drop(&mut self) {
        for name in ["owned", "foreign"] {
            if let Ok(path) = CString::new(self.0.path().join(name).as_os_str().as_bytes()) {
                // SAFETY: only the two mount destinations created by this fixture.
                unsafe {
                    libc::unmount(path.as_ptr(), libc::MNT_FORCE);
                }
            }
        }
    }
}

#[test]
#[ignore = "requires live macOS NFS mounts and SIGKILL"]
fn startup_reclaims_crashed_sdk_mounts_and_preserves_live_and_foreign_mounts() -> TestResult {
    let root = CrashRoots(tempfile::tempdir()?);
    let canonical = root.0.path().canonicalize()?;
    for name in ["owned", "foreign"] {
        std::fs::create_dir(canonical.join(name))?;
    }
    let (live_root, _live_source, mut live, _) = live_mount()?;
    let live_socket = mount_socket(live_root.path())?;
    let mut child = Command::new(std::env::current_exe()?)
        .args(["--exact", CHILD, "--ignored", "--nocapture"])
        .env("ACYCLIC_RECLAIM_TEST_ROOT", &canonical)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    let output = child.stdout.take().ok_or("missing child stdout")?;
    let ready = std::io::BufReader::new(output)
        .lines()
        .find_map(|line| match line {
            Ok(line) if line == "RECLAIM_READY" => Some(Ok(())),
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        })
        .ok_or("child exited before mounting")?;
    ready?;
    child.kill()?;
    child.wait()?;
    // Negative witness: SIGKILL itself leaves both kernel mounts behind.
    let mounted = |path: &Path| -> Result<bool, NativeMountError> {
        Ok(super::super::reclaim::mount_table()?.iter().any(|mount| {
            super::super::reclaim::field(&mount.f_mntonname)
                .is_ok_and(|name| name.to_bytes() == path.as_os_str().as_bytes())
        }))
    };
    assert!(mounted(&canonical.join("owned"))?);
    assert!(mounted(&canonical.join("foreign"))?);
    // A historical SDK fence alone must not authorize detaching a different
    // transport's mount subsequently placed at the same destination.
    let (foreign_fence, _) =
        crate::native_mount::fence::MountDestinationGuard::paths_for_canonical(
            &canonical.join("foreign"),
        )?;
    drop(crate::native_mount::fence::acquire_mount_lock(
        &foreign_fence,
        false,
    )?);
    crate::initialize_native_mount_services()?;
    assert!(
        !mounted(&canonical.join("owned"))?,
        "startup left the crashed SDK mount attached"
    );
    assert!(
        mounted(&canonical.join("foreign"))?,
        "startup reclaimed a foreign mount"
    );
    assert_eq!(mount_socket(live_root.path())?, live_socket);
    let (source, _, _) = checkout_context(MountPublication::Manual)?;
    let mut replacement = crate::mount_native(request(&canonical.join("owned"), &source)?, source)?;
    std::fs::write(canonical.join("owned/reconnected"), b"ready")?;
    assert_eq!(
        std::fs::read(canonical.join("owned/reconnected"))?,
        b"ready"
    );
    replacement.stop()?;
    live.stop()?;
    Ok(())
}
