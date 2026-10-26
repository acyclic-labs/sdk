use super::*;
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
        shared: false,
    })
}

#[test]
#[ignore = "subprocess fixture for the live macOS crash recovery test"]
fn crashed_owner() -> TestResult {
    let Some(root) = std::env::var_os("ACYCLIC_RECLAIM_TEST_ROOT") else {
        return Ok(());
    };
    let root = PathBuf::from(root);
    let began = Instant::now();
    let (source, _, work) = checkout_context(MountPublication::Manual)?;
    let _owned = crate::mount_native(request(&root.join("owned"), &source)?, source.clone())?;
    phase(began, "child-owned-mounted");
    // The ordinary case leaves a dead foreign transport too. The forced
    // retirement case keeps that independent transport alive in the parent,
    // so its separate dead-server teardown does not serialize after the
    // Disk Arbitration callback being proved here.
    let mut foreign_session = None;
    if std::env::var_os("ACYCLIC_RECLAIM_LIVE_FOREIGN").is_none() {
        let (foreign, _, _) = checkout_context(MountPublication::Manual)?;
        DarwinMountSession::start_into(
            &request(&root.join("foreign"), &foreign)?,
            foreign,
            &work,
            &mut foreign_session,
            &mut None,
            |_| Ok(()),
        )?;
    }
    phase(began, "child-foreign-ready");
    println!("RECLAIM_READY");
    std::io::stdout().flush()?;
    let mut command = String::new();
    std::io::stdin().read_line(&mut command)?;
    Err("crash fixture must be killed".into())
}

struct CrashRoots(Option<tempfile::TempDir>);

impl Drop for CrashRoots {
    fn drop(&mut self) {
        let Some(root) = self.0.take() else { return };
        let cleanup = (|| -> TestResult {
            let canonical = root.path().canonicalize()?;
            for mount in super::super::reclaim::mount_table()? {
                let destination = super::super::reclaim::field(&mount.f_mntonname)?.to_bytes();
                if ["owned", "foreign"]
                    .into_iter()
                    .any(|name| canonical.join(name).as_os_str().as_bytes() == destination)
                {
                    // The fixture owns both exact destinations, including its
                    // deliberately unfenced foreign transport.
                    super::super::reclaim::detach_stale_mount(&mount)?;
                }
            }
            Ok(())
        })();
        if let Err(error) = cleanup {
            // Never recursively remove a directory while a dead NFS mount may
            // cover it. Preserve the fixture for recovery and fail the proof.
            let retained = root.keep();
            eprintln!("crash fixture retirement failed at {retained:?}: {error}");
            assert!(std::thread::panicking(), "crash fixture retirement failed");
        }
    }
}

#[test]
#[ignore = "requires live macOS NFS mounts and SIGKILL"]
fn startup_reclaims_crashed_sdk_mounts_and_preserves_live_and_foreign_mounts() -> TestResult {
    check_startup_recovery(false, crate::initialize_native_mount_services)
}

#[test]
#[ignore = "requires live macOS NFS mounts and SIGKILL"]
fn permission_denied_startup_preserves_live_and_foreign_mounts() -> TestResult {
    check_startup_recovery(true, crate::initialize_native_mount_services)
}

#[test]
#[ignore = "requires live macOS NFS mounts and SIGKILL"]
fn negative_control_direct_only_startup_cannot_retire_after_eperm() -> TestResult {
    let attempted = std::cell::Cell::new(false);
    let result = check_startup_recovery(true, || {
        attempted.set(true);
        super::super::reclaim::reclaim_stale_mounts_direct_only()
    });
    assert!(
        attempted.get(),
        "fixture failed before exercising retirement"
    );
    let error = result.expect_err("direct-only startup unexpectedly recovered after EPERM");
    assert!(matches!(
        error.downcast_ref::<NativeMountError>(),
        Some(NativeMountError::Io(error)) if error.raw_os_error() == Some(libc::EPERM)
    ));
    println!("RECLAIM_DIRECT_ONLY_REJECTED errno=EPERM");
    Ok(())
}

fn check_startup_recovery(
    deny_direct_unmount: bool,
    startup: impl FnOnce() -> Result<(), NativeMountError>,
) -> TestResult {
    let began = Instant::now();
    let root = CrashRoots(Some(tempfile::tempdir()?));
    let canonical = root
        .0
        .as_ref()
        .ok_or("missing crash fixture")?
        .path()
        .canonicalize()?;
    for name in ["owned", "foreign"] {
        std::fs::create_dir(canonical.join(name))?;
    }
    let (live_root, _live_source, mut live, _) = live_mount()?;
    let live_socket = mount_socket(live_root.path())?;
    phase(began, "live-mounted");
    let mut foreign = if deny_direct_unmount {
        Some(ForeignMount::start(&canonical.join("foreign"))?)
    } else {
        None
    };
    let mut command = Command::new(std::env::current_exe()?);
    command
        .args(["--exact", CHILD, "--ignored", "--nocapture"])
        .env("ACYCLIC_RECLAIM_TEST_ROOT", &canonical)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped());
    if deny_direct_unmount {
        command.env("ACYCLIC_RECLAIM_LIVE_FOREIGN", "1");
    }
    let mut child = command.spawn()?;
    let output = child.stdout.take().ok_or("missing child stdout")?;
    acyclic_test_support::readiness::wait(&mut std::io::BufReader::new(output), "RECLAIM_READY")?;
    child.kill()?;
    child.wait()?;
    phase(began, "crash-witness-ready");
    // Negative witness: SIGKILL itself leaves both kernel mounts behind.
    let mounted = |path: &Path| -> Result<bool, NativeMountError> {
        Ok(super::super::reclaim::mount_table()?.iter().any(|mount| {
            super::super::reclaim::field(&mount.f_mntonname)
                .is_ok_and(|name| name.to_bytes() == path.as_os_str().as_bytes())
        }))
    };
    assert!(mounted(&canonical.join("owned"))?);
    assert!(mounted(&canonical.join("foreign"))?);
    println!("RECLAIM_CRASH_WITNESS owned=attached foreign=attached");
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
    if deny_direct_unmount {
        super::super::reclaim::with_direct_unmount_denied(startup)?;
    } else {
        startup()?;
    }
    phase(began, "startup-retired");
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
    phase(began, "replacement-mounted");
    std::fs::write(canonical.join("owned/reconnected"), b"ready")?;
    assert_eq!(
        std::fs::read(canonical.join("owned/reconnected"))?,
        b"ready"
    );
    phase(began, "replacement-writable");
    replacement.stop()?;
    phase(began, "replacement-stopped");
    live.stop()?;
    phase(began, "live-stopped");
    if let Some(foreign) = &mut foreign {
        foreign.stop()?;
    }
    phase(began, "foreign-stopped");
    println!(
        "RECLAIM_RECOVERY_VERIFIED owned=retired live=preserved foreign=preserved replacement=writable"
    );
    Ok(())
}

fn phase(began: Instant, name: &str) {
    eprintln!(
        "RECLAIM_PHASE name={name} seconds={:.6}",
        began.elapsed().as_secs_f64()
    );
}

// The intentionally unfenced provider stays alive until its namespace has
// retired, including the expected-error negative-control path. The ordinary
// fixture still covers a crashed, unfenced transport with a historical fence.
struct ForeignMount {
    session: DarwinMountSession,
    work: Work,
}

impl ForeignMount {
    fn start(destination: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let (source, _, work) = checkout_context(MountPublication::Manual)?;
        let mut session = None;
        DarwinMountSession::start_into(
            &request(destination, &source)?,
            source,
            &work,
            &mut session,
            &mut None,
            |_| Ok(()),
        )?;
        Ok(Self {
            session: session.ok_or("missing foreign session")?,
            work,
        })
    }

    fn stop(&mut self) -> Result<(), NativeMountError> {
        self.session
            .stop(&crate::native_mount::NativeSessionStopWork::Meter(
                &self.work,
            ))
    }
}

impl Drop for ForeignMount {
    fn drop(&mut self) {
        if let Err(error) = self.stop() {
            eprintln!("foreign fixture retirement failed: {error}");
            assert!(
                std::thread::panicking(),
                "foreign fixture retirement failed"
            );
        }
    }
}
