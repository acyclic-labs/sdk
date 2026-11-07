use super::*;
use crate::native_mount::NativeSessionStopWork;
use std::sync::mpsc::{Receiver, Sender};

enum Event {
    Polling,
    Released,
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
        for physical in [false, true] {
            terminal_worker_retires_destination(false, metered, physical)?;
        }
    }
    Ok(())
}

#[test]
fn panicked_worker_retires_its_destination_without_losing_its_payload()
-> Result<(), Box<dyn std::error::Error>> {
    for metered in [false, true] {
        for physical in [false, true] {
            terminal_worker_retires_destination(true, metered, physical)?;
        }
    }
    Ok(())
}

fn terminal_worker_retires_destination(
    panicked: bool,
    metered: bool,
    physical: bool,
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
        std::fs::read(missing)
            .map(drop)
            .map_err(NativeMountError::from)
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
            terminal_source_failure: Some(cleanup_error.into()),
        }),
        cleanup: work.reserve_work(crate::WorkCounters {
            backend_read_operations: 2,
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
    if physical {
        run_fuse_session((Arc::clone(&core), Arc::clone(&state)));
    }
    session.driver = Some(super::super::FuseSession {
        core,
        state: Arc::clone(&state),
        invalidator: Some(std::thread::spawn(|| {
            panic!("retained FUSE invalidator panic")
        })),
    });
    let wait = crate::CancellationToken::new();
    let stop = if metered {
        NativeSessionStopWork::Meter(&work)
    } else {
        NativeSessionStopWork::AcceptedWait(&wait)
    };
    let first = session
        .stop_with_context(&stop)
        .expect_err("worker error was lost during cleanup");
    assert!(session.driver.is_none(), "terminal cause kept the driver");
    assert!(session.cleanup_state().cleanup_complete());
    assert!(
        state
            .lock()
            .map_err(|_| "retirement state poisoned")?
            .is_retired()
    );
    retired(root.path(), &fence, before, &work)?;
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
    let second = session
        .stop_with_context(&stop)
        .expect_err("source error was lost during cleanup");
    assert!(
        matches!(second, NativeMountError::Io(ref cause) if cause.raw_os_error() == cleanup_code)
    );
    let third = session
        .stop_with_context(&stop)
        .expect_err("invalidator panic was lost during cleanup");
    let NativeMountError::DriverPanicked { operation, panic } = third else {
        panic!("invalidator panic changed to {third:?}");
    };
    assert_eq!(operation, "FUSE invalidator");
    let payload = panic.into_inner().map_err(|_| "panic payload poisoned")?;
    assert_eq!(
        payload.downcast_ref::<&str>(),
        Some(&"retained FUSE invalidator panic")
    );
    assert!(!session.stop_with_context(&stop)?);
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
            retained_source: Some(crate::native_mount::retirement::RetainedSource::Filesystem(
                source,
            )),
            destination_guard: None,
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
    physical: Option<Arc<std::sync::OnceLock<std::thread::ThreadId>>>,
    caller_waited: Arc<std::sync::atomic::AtomicBool>,
) -> std::io::Result<super::super::mount::Mount> {
    super::super::mount::Mount::accepted_unmount(move |timeout| {
        if timeout == 0 {
            // A probe observes the connection before the later acknowledgment.
            return Ok(false);
        }
        assert_eq!(timeout, -1);
        if physical
            .as_ref()
            .is_some_and(|owner| owner.get() != Some(&std::thread::current().id()))
        {
            // Refuse the observer's misplaced wait without hanging the
            // negative control. The accepted physical owner still drains it.
            caller_waited.store(true, std::sync::atomic::Ordering::Release);
            return Err(std::io::ErrorKind::WouldBlock.into());
        }
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
    use crate::native_mount::tests::retirement::{Fixture, retired};

    let Fixture {
        root,
        mut session,
        work,
        fence,
    } = Fixture::new()?;
    let before = work.receipt();
    let meter = matches!(mode, Retirement::Meter);
    let last_owner = matches!(mode, Retirement::LastOwner);
    let (callbacks, source) = shared_sources(posix_config())?;
    let retained_source: Arc<dyn MountFilesystem> = Arc::new(source);
    let source = Arc::downgrade(&retained_source);
    let core = projection_core(
        Arc::new(callbacks),
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )?;
    let original = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            backend_read_operations: 1 + u64::from(last_owner),
            backend_write_operations: 1,
            ..crate::WorkBudget::UNBOUNDED
        },
        &crate::CancellationToken::new(),
    )
    .map_err(|(cause, _)| cause)?;
    let cleanup = original.reserve_work(crate::WorkCounters {
        backend_read_operations: u64::from(!meter) + u64::from(last_owner),
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
    let physical = last_owner.then(|| Arc::new(std::sync::OnceLock::new()));
    let caller_waited = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mount = waiting_mount(
        polling,
        acknowledged,
        interrupted,
        physical.clone(),
        Arc::clone(&caller_waited),
    )?;
    core.stop();
    if !meter {
        // Losing the original caller cannot revoke already accepted cleanup.
        original.cancellation().cancel();
        work.cancellation().cancel();
    }
    let state = Arc::new(Mutex::new(FuseSessionState {
        path: super::super::FuseOwned::empty(vec![0]),
        before: Some((0, 0)),
        mount,
        worker: None,
        device: Some(Arc::new(std::fs::File::open("/dev/null")?)),
        notifier: None,
        identity: None,
        retirement: Some(crate::native_mount::NativeMountRetirement {
            retained_source: Some(crate::native_mount::retirement::RetainedSource::Filesystem(
                retained_source,
            )),
            destination_guard: session.destination_guard.take(),
            terminal_source_failure: None,
        }),
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
    let (release, accepted) = std::sync::mpsc::channel();
    if let Some(physical) = physical {
        let core = Arc::clone(&facade.core);
        let state = Arc::clone(&state);
        let events = events.clone();
        facade.invalidator = Some(std::thread::spawn(move || {
            physical.set(std::thread::current().id()).unwrap();
            // Preempt the already accepted physical owner until the observer
            // leaves. Its original loop, state and cleanup credit remain held.
            accepted.recv().unwrap();
            run_fuse_session((core, Arc::clone(&state)));
            let result = state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .failure
                .take()
                .map_or(Ok(()), Err);
            events.send(Event::Retired(result)).unwrap();
        }));
    }
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
                session.driver = Some(facade);
                drop(session);
                events.send(Event::Released).unwrap();
                return;
            }
        };
        events.send(Event::Retired(result)).unwrap();
    });
    let mut released = !last_owner;
    let mut polling = false;
    let mut observer_custody = true;
    while !released || !polling {
        match observed.recv()? {
            Event::Polling => polling = true,
            Event::Released => {
                // The physical owner is still preempted here: only its packet
                // can retain the source and destination after observer drop.
                observer_custody = source.upgrade().is_some()
                    && fence.is_file()
                    && root.path().join("retired-mount").is_dir();
                released = true;
                release.send(())?;
            }
            Event::Retired(result) => {
                worker.join().map_err(|_| "retirement worker panicked")?;
                panic!("retirement returned before the kernel acknowledgment: {result:?}");
            }
        }
    }
    let locked = matches!(state.try_lock(), Err(std::sync::TryLockError::WouldBlock));
    let source_held = source.upgrade().is_some();
    let fence_held = fence.is_file() && root.path().join("retired-mount").is_dir();
    // Drain the accepted owner before asserting, including negative controls
    // that retire a resource too early. A failed assertion must not strand it.
    acknowledge.send(())?;
    let Event::Retired(result) = observed.recv()? else {
        panic!("retirement polled again instead of awaiting acknowledgment");
    };
    worker.join().map_err(|_| "retirement worker panicked")?;
    result?;
    assert!(
        observer_custody,
        "observer drop released retirement resources"
    );
    assert!(
        locked,
        "session ownership escaped before the kernel acknowledgment"
    );
    assert!(
        source_held,
        "source retired before the kernel acknowledgment"
    );
    assert!(
        fence_held,
        "destination fence retired before the kernel acknowledgment"
    );
    assert!(source.upgrade().is_none(), "retired packet kept its source");
    retired(root.path(), &fence, before, &work)?;
    assert!(
        !caller_waited.load(std::sync::atomic::Ordering::Acquire),
        "observer drop entered the original native wait instead of its accepted physical owner"
    );
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
