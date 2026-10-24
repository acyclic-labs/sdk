use super::*;
use crate::durable_host::tests::fixture::fixture;
use std::sync::atomic::AtomicUsize;

#[tokio::test]
async fn dispatch_cannot_use_a_cached_owner_after_invocation_cancellation() -> Result<()> {
    let fixture = fixture().await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let owner = Arc::new(
        fixture
            .running(1)
            .await?
            .with_invocation_cancellation(&cancellation),
    );
    let control = OwnerControl::open(owner).await?;
    control.check(|| Ok(())).map_err(io_error)?;
    cancellation.cancel();
    // No await gives the driver a turn between cancellation and dispatch.
    let mut verified = false;
    let result = control.check(|| {
        verified = true;
        Err(std::io::ErrorKind::Interrupted.into())
    });
    assert!(
        verified && result.is_err(),
        "cancelled dispatch reused its cached owner before the driver ran"
    );
    Ok(())
}

#[tokio::test]
async fn concurrent_hooks_share_an_in_flight_owner_rejection()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let fixture = fixture().await?;
    let owner = Arc::new(fixture.running(1).await?);
    let control = OwnerControl::open(owner).await?;
    let first = control.clone();
    let (entered, entry) = std::sync::mpsc::sync_channel(1);
    let (release, released) = std::sync::mpsc::sync_channel(1);
    let verifying = std::thread::spawn(move || {
        first.check(|| {
            entered.send(()).map_err(std::io::Error::other)?;
            released.recv().map_err(std::io::Error::other)?;
            Err(std::io::ErrorKind::PermissionDenied.into())
        })
    });
    entry.recv_timeout(Duration::from_secs(1))?;
    let second = control.clone();
    let (result, observed) = std::sync::mpsc::sync_channel(1);
    let attempting = std::thread::spawn(move || {
        let checked = second.check(|| Err(std::io::Error::other("duplicate owner verification")));
        let _ = result.send(checked);
    });
    let early = observed.recv_timeout(Duration::from_millis(50));
    release.send(())?;
    let rejected = match early {
        Ok(checked) => checked,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            observed.recv_timeout(Duration::from_secs(1))?
        }
        Err(cause) => return Err(cause.into()),
    };
    let first = verifying.join().map_err(|_| "owner verifier panicked")?;
    attempting.join().map_err(|_| "hook waiter panicked")?;
    assert!(first.is_err());
    assert!(
        rejected.is_err(),
        "a hook passed while the shared owner was being rejected"
    );
    assert!(
        control.check(|| Ok(())).is_err(),
        "an owner rejection must remain fenced"
    );
    Ok(())
}

#[tokio::test]
async fn a_quiet_owner_authenticates_once_instead_of_every_poll() -> Result<()> {
    let fixture = fixture().await?;
    let owner = Arc::new(fixture.running(1).await?);
    let control = OwnerControl::open(Arc::clone(&owner)).await?;
    owner.verify(false, &mut None).await?;
    let mut checks = 0;
    for _ in 0..9 {
        control
            .check(|| {
                checks += 1;
                Ok(())
            })
            .map_err(io_error)?;
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    eprintln!("QUIET_OWNER held_ms=200 checks={checks}");
    assert_eq!(
        checks, 1,
        "an unchanged owner must not be reauthenticated by a timer"
    );
    Ok(())
}

#[test]
fn owned_silent_child() {
    if crate::native_environment::variable("ACYCLIC_OWNED_SILENT_CHILD").is_some() {
        println!("OWNER_CONTROL_READY");
        std::thread::sleep(Duration::from_secs(2));
    }
}

#[tokio::test]
async fn invocation_cancellation_wakes_the_retained_capture_worker()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let fixture = fixture().await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let owner = Arc::new(
        fixture
            .running(1)
            .await?
            .with_invocation_cancellation(&cancellation),
    );
    let control = OwnerControl::open(Arc::clone(&owner)).await?;
    let wake = control.wake();
    let runtime = tokio::runtime::Handle::current();
    let checks = Arc::new(AtomicUsize::new(0));
    let checked = Arc::clone(&checks);
    let (ready, announced) = tokio::sync::oneshot::channel();
    let observed = Mutex::new((Vec::new(), Some(ready)));
    let observer: Arc<NativeProcessObserver> = Arc::new(move |chunk| {
        let acyclic_native_runtime::ProcessOutput::Stdout(bytes) = chunk else {
            return Ok(());
        };
        let mut observed = observed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        observed.0.extend_from_slice(bytes);
        if observed
            .0
            .windows(b"OWNER_CONTROL_READY".len())
            .any(|bytes| bytes == b"OWNER_CONTROL_READY")
            && let Some(ready) = observed.1.take()
        {
            let _ = ready.send(());
        }
        Ok(())
    });
    let root = tempfile::tempdir()?;
    let invocation = Invocation {
        executable: std::env::current_exe()?,
        argv: vec![
            "--exact".into(),
            "filesystem::native_process::capture::owner::tests::owned_silent_child".into(),
            "--nocapture".into(),
        ],
        cwd: root.path().into(),
        environment: [("ACYCLIC_OWNED_SILENT_CHILD".into(), "1".into())].into(),
        inherit_environment: true,
        timeout_ms: 1000,
        control_timeout_ms: 1000,
        maximum_output_bytes: 16 * 1024,
        standard_input: None,
        mcp_stdio: None,
        process_custody: None,
    };
    let capture = tokio::spawn(capture_checked(
        invocation,
        Some(observer),
        move || {
            control.check(|| {
                checked.fetch_add(1, Ordering::SeqCst);
                check_on_runtime(
                    &runtime,
                    Duration::from_secs(1),
                    owner.verify(false, &mut None),
                )
            })
        },
        Some(wake),
    ));
    tokio::time::timeout(Duration::from_secs(1), announced).await??;
    let started = std::time::Instant::now();
    cancellation.cancel();
    let output = tokio::time::timeout(Duration::from_secs(1), capture)
        .await
        .map_err(|_| std::io::Error::other("owner notification did not wake capture"))???
        .ok_or("capture worker was interrupted")?;
    let stop = output
        .stop
        .ok_or("cancelled owner did not stop the command")?;
    assert_eq!(stop.kind, NativeProcessStopKind::ControlStopped);
    assert!(stop.cleanup_completed);
    assert!(
        started.elapsed() < Duration::from_millis(750),
        "owner notification did not wake capture"
    );
    assert_eq!(checks.load(Ordering::SeqCst), 1);
    Ok(())
}
