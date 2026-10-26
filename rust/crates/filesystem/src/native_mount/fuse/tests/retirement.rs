use super::*;
use crate::native_mount::NativeSessionStopWork;
use std::sync::mpsc::{Receiver, Sender};

enum Event {
    Polling,
    Retired(Result<(), NativeMountError>),
}

#[derive(Clone, Copy)]
enum Retirement {
    Meter,
    AcceptedWait,
    LastOwner,
}

#[test]
fn failed_worker_retires_its_destination_before_reporting_both_causes()
-> Result<(), Box<dyn std::error::Error>> {
    for metered in [false, true] {
        terminal_worker_retires_destination(false, metered)?;
    }
    Ok(())
}

#[test]
fn panicked_worker_retires_its_destination_without_losing_its_payload()
-> Result<(), Box<dyn std::error::Error>> {
    for metered in [false, true] {
        terminal_worker_retires_destination(true, metered)?;
    }
    Ok(())
}

fn terminal_worker_retires_destination(
    panicked: bool,
    metered: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    use crate::native_mount::tests::retirement::{Fixture, retired};

    let Fixture {
        root,
        mut session,
        work,
        fence,
    } = Fixture::new()?;
    let missing = root.path().join("absent-worker-input");
    let worker = std::thread::spawn(move || {
        assert!(!panicked, "retained FUSE worker panic");
        std::fs::read(missing).map(drop)
    });
    let cleanup_error =
        std::fs::read(root.path()).expect_err("a directory cannot be read as source bytes");
    let cleanup_code = cleanup_error.raw_os_error();
    let (callbacks, _) = shared_sources(posix_config())?;
    let core = projection_core(
        Arc::new(callbacks),
        crate::WorkBudget::UNBOUNDED,
        work.cancellation(),
    )?;
    core.stop();
    let state = Arc::new(Mutex::new(FuseSessionState {
        path: super::super::FuseOwned::empty(vec![0]),
        before: None,
        mount: super::super::mount::Mount::default(),
        worker: Some(worker),
        device: None,
        notifier: None,
        identity: None,
        retirement: Some(crate::native_mount::NativeMountRetirement {
            retained_source: None,
            destination_guard: session.destination_guard.take(),
            startup_source_watch: None,
            terminal_source_failure: Some(cleanup_error.into()),
        }),
        cleanup: work.reserve_work(crate::WorkCounters {
            backend_read_operations: 1,
            ..crate::WorkCounters::UNCHARGED
        })?,
        failure: crate::native_mount::retirement::TerminalFailures::default(),
        shared: None,
        _allocation: work.claim(
            std::mem::size_of::<Mutex<FuseSessionState>>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?,
    }));
    let before = work.receipt();
    // The real physical owner must return after the failed worker and source
    // have exited. The old early-return path parks here with the fence held.
    run_fuse_session((Arc::clone(&core), Arc::clone(&state)));
    assert!(
        state
            .lock()
            .map_err(|_| "retirement state poisoned")?
            .is_retired()
    );
    retired(root.path(), &fence, before, &work)?;
    let mut facade = super::super::FuseSession {
        core,
        state,
        invalidator: None,
    };
    let wait = crate::CancellationToken::new();
    let stop = if metered {
        NativeSessionStopWork::Meter(&work)
    } else {
        NativeSessionStopWork::AcceptedWait(&wait)
    };
    let mut outcomes = facade.stop(&stop)?;
    let first = outcomes
        .take()
        .ok_or("worker error was lost during cleanup")?;
    if panicked {
        let NativeMountError::DriverPanicked { panic, .. } = first else {
            panic!("worker panic changed to {first:?}");
        };
        let payload = panic.into_inner().map_err(|_| "panic payload poisoned")?;
        assert_eq!(
            payload.downcast_ref::<&str>(),
            Some(&"retained FUSE worker panic")
        );
    } else {
        assert!(
            matches!(first, NativeMountError::Io(ref cause) if cause.kind() == std::io::ErrorKind::NotFound)
        );
    }
    let second = outcomes
        .take()
        .ok_or("source error was lost during cleanup")?;
    assert!(
        matches!(second, NativeMountError::Io(ref cause) if cause.raw_os_error() == cleanup_code)
    );
    assert!(outcomes.is_empty());
    assert!(facade.stop(&stop)?.is_empty());
    assert_eq!(
        work.receipt().allocation_operations,
        before.allocation_operations
    );
    assert_eq!(
        work.receipt().peak_allocation_bytes,
        before.peak_allocation_bytes
    );
    Ok(())
}

/// Runs the real physical loop after namespace/device retirement, retaining a
/// real caller-supplied source packet. No live kernel mount is needed to drive
/// the remaining source-join obligation.
pub(in crate::native_mount) fn retire_source_in_physical_loop(
    source: Arc<dyn MountFilesystem>,
) -> Result<bool, NativeMountError> {
    let (callbacks, _) = shared_sources(posix_config())
        .map_err(|cause| NativeMountError::Driver(cause.to_string()))?;
    let work = fuse_fixture_work();
    let core = projection_core(
        Arc::new(callbacks),
        crate::WorkBudget::UNBOUNDED,
        work.cancellation(),
    )?;
    core.stop();
    let state = Arc::new(Mutex::new(FuseSessionState {
        path: super::super::FuseOwned::empty(vec![0]),
        before: None,
        mount: super::super::mount::Mount::default(),
        worker: None,
        device: None,
        notifier: None,
        identity: None,
        retirement: Some(crate::native_mount::NativeMountRetirement {
            retained_source: Some(source),
            destination_guard: None,
            startup_source_watch: None,
            terminal_source_failure: None,
        }),
        cleanup: work.reserve_work(crate::WorkCounters::UNCHARGED)?,
        failure: crate::native_mount::retirement::TerminalFailures::default(),
        shared: None,
        _allocation: work.claim(
            std::mem::size_of::<Mutex<FuseSessionState>>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?,
    }));
    run_fuse_session((core, Arc::clone(&state)));
    let mut state = state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(error) = state.failure.take() {
        return Err(error);
    }
    Ok(state.is_retired())
}

fn waiting_mount(
    polling: Sender<Event>,
    acknowledged: Receiver<()>,
    mut interrupted: bool,
) -> std::io::Result<super::super::mount::Mount> {
    super::super::mount::Mount::accepted_unmount(move |timeout| {
        if timeout == 0 {
            // A probe observes the connection before the later acknowledgment.
            return Ok(false);
        }
        assert_eq!(timeout, -1);
        if std::mem::take(&mut interrupted) {
            return Err(std::io::ErrorKind::Interrupted.into());
        }
        polling
            .send(Event::Polling)
            .map_err(|_| std::io::ErrorKind::BrokenPipe)?;
        acknowledged
            .recv()
            .map_err(|_| std::io::ErrorKind::BrokenPipe)?;
        Ok(true)
    })
}

// The kernel accepts unmount before its last connection reference disappears.
// Drive those two events separately through the real session and its original
// finite cleanup reservation, with no scheduler timing or live FUSE dependency.
#[test]
fn accepted_unmount_waits_for_kernel_ack_using_birth_credit()
-> Result<(), Box<dyn std::error::Error>> {
    for interrupted in [false, true] {
        interleaving(Retirement::AcceptedWait, interrupted)?;
    }
    Ok(())
}

#[test]
fn accepted_unmount_waits_for_kernel_ack_on_meter() -> Result<(), Box<dyn std::error::Error>> {
    for interrupted in [false, true] {
        interleaving(Retirement::Meter, interrupted)?;
    }
    Ok(())
}

#[test]
fn last_session_drop_waits_for_kernel_ack_using_birth_credit()
-> Result<(), Box<dyn std::error::Error>> {
    for interrupted in [false, true] {
        interleaving(Retirement::LastOwner, interrupted)?;
    }
    Ok(())
}

fn interleaving(mode: Retirement, interrupted: bool) -> Result<(), Box<dyn std::error::Error>> {
    let meter = matches!(mode, Retirement::Meter);
    let (source, _) = shared_sources(posix_config())?;
    let core = projection_core(
        Arc::new(source),
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )?;
    let original = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            backend_read_operations: 1,
            backend_write_operations: 1,
            ..crate::WorkBudget::UNBOUNDED
        },
        &crate::CancellationToken::new(),
    )
    .map_err(|(cause, _)| cause)?;
    let cleanup = original.reserve_work(crate::WorkCounters {
        backend_read_operations: u64::from(!meter),
        backend_write_operations: 1,
        ..crate::WorkCounters::UNCHARGED
    })?;
    let allocation = original.claim(
        std::mem::size_of::<Mutex<FuseSessionState>>() as u64,
        1,
        crate::WorkCounters::UNCHARGED,
    )?;
    let (events, observed) = std::sync::mpsc::channel();
    let polling = events.clone();
    let (acknowledge, acknowledged) = std::sync::mpsc::channel();
    // Model a successful first unmount, with acknowledgment still withheld.
    cleanup.started_io(
        crate::WorkCounters {
            backend_write_operations: 1,
            ..crate::WorkCounters::UNCHARGED
        },
        None,
    )?;
    let mount = waiting_mount(polling, acknowledged, interrupted)?;
    core.stop();
    if !meter {
        // Losing the original caller cannot revoke already accepted cleanup.
        original.cancellation().cancel();
    }
    let state = Arc::new(Mutex::new(FuseSessionState {
        path: super::super::FuseOwned::empty(vec![0]),
        before: Some((0, 0)),
        mount,
        worker: None,
        device: Some(super::super::session::FuseDevice::Owned(
            std::fs::File::open("/dev/null")?.into(),
        )),
        notifier: None,
        identity: None,
        retirement: None,
        cleanup,
        failure: crate::native_mount::retirement::TerminalFailures::default(),
        shared: None,
        _allocation: allocation,
    }));
    let mut facade = super::super::FuseSession {
        core,
        state: Arc::clone(&state),
        invalidator: None,
    };
    let caller = original.clone();
    let worker = std::thread::spawn(move || {
        let result = match mode {
            Retirement::Meter => facade
                .stop(&NativeSessionStopWork::Meter(&caller))
                .map(|outcome| assert!(outcome.is_empty())),
            Retirement::AcceptedWait => {
                let wait = crate::CancellationToken::new();
                facade
                    .stop(&NativeSessionStopWork::AcceptedWait(&wait))
                    .map(|outcome| assert!(outcome.is_empty()))
            }
            Retirement::LastOwner => {
                let mut owner = crate::NativeMountSession::new(
                    crate::NativeMountRequest {
                        mount_id: crate::MountId::new(),
                        volume_id: crate::VolumeId::new(),
                        destination: Path::new("/not-mounted").to_path_buf(),
                        writable: false,
                        shared: false,
                    },
                    None,
                    None,
                );
                owner.driver = Some(facade);
                drop(owner);
                Ok(())
            }
        };
        events.send(Event::Retired(result)).unwrap();
    });
    match observed.recv()? {
        Event::Polling => {
            assert!(
                matches!(state.try_lock(), Err(std::sync::TryLockError::WouldBlock)),
                "session ownership escaped before the kernel acknowledgment"
            );
            acknowledge.send(())?;
        }
        Event::Retired(result) => {
            worker.join().map_err(|_| "retirement worker panicked")?;
            panic!("retirement returned before the kernel acknowledgment: {result:?}");
        }
    }
    let Event::Retired(result) = observed.recv()? else {
        panic!("retirement polled again instead of awaiting acknowledgment");
    };
    worker.join().map_err(|_| "retirement worker panicked")?;
    result?;
    assert!(
        state
            .lock()
            .map_err(|_| "retirement state poisoned")?
            .is_retired()
    );
    let receipt = original.receipt();
    assert_eq!(receipt.backend_write_operations, 1);
    assert_eq!(receipt.backend_read_operations, 1);
    Ok(())
}
