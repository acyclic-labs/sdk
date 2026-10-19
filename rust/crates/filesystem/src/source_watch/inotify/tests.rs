use super::*;
use crate::cancellation::CancellationToken;
use crate::source_watch::HostChangeSink;

#[cfg(feature = "native-mount")]
const ORIGINAL_BUDGET: WorkBudget = WorkBudget {
    backend_read_operations: 128,
    backend_write_operations: 128,
    source_bytes_read: 1024 * 1024,
    bytes_copied: 1024 * 1024,
    items_examined: 4096,
    items_returned: 4096,
    allocation_operations: 128,
    peak_allocation_bytes: 1024 * 1024,
    ..WorkBudget::UNCHARGED
};

struct Ignored;
impl HostChangeSink for Ignored {
    fn logical_owned_bytes(&self) -> u64 {
        0
    }
    fn host_changed(&self, _: &[HostChange], _: &mut DemandWork) -> Result<(), DemandError> {
        Ok(())
    }
    fn host_changed_with_native_work(
        &self,
        _: &[HostChange],
        work: &mut crate::NativeMaterializationWork,
    ) -> Result<(), DemandError> {
        work.check()
    }
}

fn watcher(root: &Path) -> io::Result<NativeSourceWatch> {
    let mut work = crate::native_host::ordinary_work();
    let host = crate::native_host::HostRoot::open(root, &mut work)
        .map_err(crate::native_host::ordinary_error)?;
    let delivery =
        Delivery::new(Arc::new(Ignored), &mut work).map_err(crate::native_host::ordinary_error)?;
    NativeSourceWatch::start_driver(&host, Arc::new(delivery), &mut work)
        .map_err(crate::native_host::ordinary_error)
}

#[cfg(feature = "native-mount")]
#[test]
fn reused_watch_keeps_storage_on_its_owner_and_io_on_each_caller()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir_all(root.path().join("a/b/c"))?;
    let host =
        crate::native_host::HostRoot::open(root.path(), &mut crate::native_host::ordinary_work())
            .map_err(crate::native_host::ordinary_error)?;
    let original = Arc::new(crate::demand::NativeDemandReadMeter::new(DemandWork::new(
        ORIGINAL_BUDGET,
        &CancellationToken::new(),
    )));
    let mut birth = crate::NativeMaterializationWork::from_meter(original.clone());
    let delivery = Arc::new(Delivery::new(Arc::new(Ignored), &mut birth)?);
    let watch = NativeSourceWatch::start_driver(&host, delivery, &mut birth)?;
    let paused = watch.paused();
    let owner_before = original.lock().receipt();
    let retained_before = watch.shared.retained_variable_bytes.load(Ordering::Acquire);

    let stop = CancellationToken::new();
    let mut cancelled = crate::NativeMaterializationWork::new(ORIGINAL_BUDGET, &stop)
        .map_err(|(error, _)| error)?;
    stop.cancel();
    assert!(matches!(
        watch.admit_with_work(Path::new("a/b/c"), &mut cancelled),
        Err(DemandError::Cancelled)
    ));
    assert_eq!(original.lock().receipt(), owner_before);
    let mut denied = DemandWork::new(
        WorkBudget {
            backend_read_operations: 0,
            ..ORIGINAL_BUDGET
        },
        &CancellationToken::new(),
    );
    assert!(
        watch
            .admit_with_work(Path::new("a/b/c"), &mut denied)
            .is_err()
    );
    assert_eq!(original.lock().receipt(), owner_before);

    // Neither incoming ledger contains the watch owner's custody gauge.
    let incoming = Arc::new(crate::demand::NativeDemandReadMeter::new(DemandWork::new(
        ORIGINAL_BUDGET,
        &CancellationToken::new(),
    )));
    let mut caller = crate::NativeMaterializationWork::from_meter(incoming.clone());
    {
        let mut state = original.lock();
        state.budget.peak_allocation_bytes = state.live_allocation_bytes();
    }
    assert!(
        watch
            .admit_with_work(Path::new("a/b/c"), &mut caller)
            .is_err()
    );
    assert_eq!(
        watch.shared.retained_variable_bytes.load(Ordering::Acquire),
        retained_before
    );
    original.lock().budget = ORIGINAL_BUDGET;
    assert!(watch.admit_with_work(Path::new("a/b/c"), &mut caller)?);
    let owner_after = original.lock().receipt();
    let caller_after = incoming.lock().receipt();
    assert!(owner_after.allocation_operations > owner_before.allocation_operations);
    assert_eq!(
        owner_after.backend_read_operations,
        owner_before.backend_read_operations
    );
    assert_eq!(owner_after.bytes_copied, owner_before.bytes_copied);
    assert!(caller_after.backend_read_operations > 0);
    assert!(caller_after.bytes_copied > 0);
    assert_eq!(incoming.lock().live_allocation_bytes(), 0);
    assert!(watch.shared.retained_variable_bytes.load(Ordering::Acquire) > retained_before);
    watch.unadmit_with_work(Path::new("a"), &mut caller)?;

    let mut ordinary = DemandWork::new(ORIGINAL_BUDGET, &CancellationToken::new());
    assert!(watch.admit_with_work(Path::new("a/b/c"), &mut ordinary)?);
    assert!(ordinary.receipt().backend_read_operations > 0);
    assert!(ordinary.receipt().bytes_copied > 0);
    assert_eq!(ordinary.live_allocation_bytes(), 0);
    watch.unadmit_with_work(Path::new("a"), &mut ordinary)?;
    drop(paused);
    watch.stop_with_cleanup_wait(&CancellationToken::new())?;
    assert!(watch.shared.physically_cleared());
    Ok(())
}

#[test]
fn stop_closes_descriptors_without_closing_reused_numbers() -> Result<(), Box<dyn std::error::Error>>
{
    let root = tempfile::tempdir()?;
    std::fs::create_dir_all(root.path().join("a/b"))?;
    let host =
        crate::native_host::HostRoot::open(root.path(), &mut crate::native_host::ordinary_work())
            .map_err(crate::native_host::ordinary_error)?;
    let watch = watcher(root.path())?;
    assert!(watch.admit(Path::new("a/b")));
    let descriptors = [
        watch.shared.inotify.load(Ordering::Acquire),
        watch.shared.wake.load(Ordering::Acquire),
        watch.shared.root.load(Ordering::Acquire),
    ];
    let mut work = DemandWork::new(
        WorkBudget {
            backend_read_operations: 128,
            backend_write_operations: 128,
            items_examined: 4096,
            ..WorkBudget::default()
        },
        &crate::CancellationToken::new(),
    );
    watch.stop_with_work(&mut work)?;
    assert!(watch.is_stopped());
    assert!(!watch.delivery().is_exact());
    assert!(watch.shared.physically_cleared());
    watch.stop_with_work(&mut work)?;
    let mut replacements = Vec::new();
    for _ in descriptors {
        // SAFETY: the root is held and each duplicate gets one owner.
        replacements.push(owned(unsafe {
            libc::fcntl(host.directory_fd(), libc::F_DUPFD_CLOEXEC, 0)
        })?);
    }
    drop(watch);
    for replacement in replacements {
        // SAFETY: this fixture owns the duplicate.
        assert!(unsafe { libc::fcntl(replacement.as_raw_fd(), libc::F_GETFD) } >= 0);
    }
    Ok(())
}

#[cfg(feature = "native-mount")]
#[test]
fn foreign_fence_keeps_deferred_cleanup_storage_on_the_watch_owner()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("before"))?;
    let host =
        crate::native_host::HostRoot::open(root.path(), &mut crate::native_host::ordinary_work())
            .map_err(crate::native_host::ordinary_error)?;
    let original = Arc::new(crate::demand::NativeDemandReadMeter::new(DemandWork::new(
        ORIGINAL_BUDGET,
        &CancellationToken::new(),
    )));
    let mut birth = crate::NativeMaterializationWork::from_meter(original.clone());
    let delivery = Arc::new(Delivery::new(Arc::new(Ignored), &mut birth)?);
    let watch = NativeSourceWatch::start_driver(&host, delivery, &mut birth)?;
    assert!(watch.admit_with_work(Path::new("before"), &mut birth)?);
    // Block the background consumer before producing events. The public fence
    // must perform this drain with the unrelated invocation's work context.
    watch
        .shared
        .reading
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .blocked = true;
    let owner_before = original.lock().receipt();
    std::fs::rename(root.path().join("before"), root.path().join("after"))?;
    let incoming = Arc::new(crate::demand::NativeDemandReadMeter::new(DemandWork::new(
        ORIGINAL_BUDGET,
        &CancellationToken::new(),
    )));
    let mut caller = crate::NativeMaterializationWork::from_meter(incoming.clone());
    assert!(watch.fence_events_with_work(std::time::Duration::MAX, &mut caller)?);
    assert!(original.lock().receipt().allocation_operations > owner_before.allocation_operations);
    assert!(incoming.lock().receipt().source_bytes_read > 0);
    assert!(incoming.lock().receipt().bytes_copied > 0);
    assert_eq!(incoming.lock().live_allocation_bytes(), 0);
    assert!(
        watch
            .shared
            .reading
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .cleanup
            .is_empty()
    );
    watch.stop_with_cleanup_wait(&CancellationToken::new())?;
    assert!(watch.shared.physically_cleared());
    Ok(())
}

#[test]
fn finite_admission_preserves_the_first_watch_and_denies_the_later_native_call()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir_all(root.path().join("a/b"))?;
    let watch = watcher(root.path())?;
    let cancellation = CancellationToken::new();
    let mut work = DemandWork::new(
        WorkBudget {
            backend_read_operations: 3,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    );
    assert!(matches!(
        watch.admit_with_work(Path::new("a/b"), &mut work),
        Err(DemandError::Work(WorkError::BudgetExceeded { .. }))
    ));

    assert!(
        watch
            .shared
            .watches
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .any(|(path, _)| path == Path::new("a"))
    );
    assert!(
        !watch
            .shared
            .watches
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .any(|(path, _)| path == Path::new("a/b"))
    );
    assert!(watch.delivery().is_exact());

    let mut work = DemandWork::new(
        WorkBudget {
            backend_read_operations: 3,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    );
    assert!(watch.admit_with_work(Path::new("a/b"), &mut work)?);

    Ok(())
}

#[test]
fn component_admission_does_not_follow_a_directory_symlink_outside_the_held_root()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let outside = tempfile::tempdir()?;
    std::fs::create_dir(outside.path().join("child"))?;
    std::os::unix::fs::symlink(outside.path(), root.path().join("a"))?;
    let watch = watcher(root.path())?;
    let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
    assert!(!watch.admit_with_work(Path::new("a/child"), &mut work)?);

    assert!(
        !watch
            .shared
            .watches
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .any(|(path, _)| path == Path::new("a"))
    );
    assert!(
        !watch
            .shared
            .watches
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .any(|(path, _)| path == Path::new("a/child"))
    );
    Ok(())
}

#[test]
fn removal_admits_the_native_write_before_forgetting_the_live_registration()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("a"))?;
    let watch = watcher(root.path())?;
    watch.admit_with_work(
        Path::new("a"),
        &mut DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()),
    )?;
    let mut denied = DemandWork::new(
        WorkBudget {
            backend_write_operations: 0,
            peak_allocation_bytes: 0,
            ..WorkBudget::UNBOUNDED
        },
        &CancellationToken::new(),
    );
    assert!(matches!(
        watch.unadmit_with_work(Path::new("a"), &mut denied),
        Err(DemandError::Work(_))
    ));

    assert!(
        watch
            .shared
            .watches
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .any(|(path, _)| path == Path::new("a"))
    );
    let mut allowed = DemandWork::new(
        WorkBudget {
            backend_write_operations: 1,
            peak_allocation_bytes: 0,
            ..WorkBudget::UNBOUNDED
        },
        &CancellationToken::new(),
    );
    watch.unadmit_with_work(Path::new("a"), &mut allowed)?;

    assert!(
        !watch
            .shared
            .watches
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .any(|(path, _)| path == Path::new("a"))
    );
    Ok(())
}

#[cfg(feature = "native-mount")]
#[test]
fn original_cleanup_wait_precancel_and_contention_preserve_custody_for_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let host =
        crate::native_host::HostRoot::open(root.path(), &mut crate::native_host::ordinary_work())
            .map_err(crate::native_host::ordinary_error)?;
    let cancellation = CancellationToken::new();
    let original = Arc::new(crate::demand::NativeDemandReadMeter::new(DemandWork::new(
        WorkBudget {
            items_returned: 0,
            ..ORIGINAL_BUDGET
        },
        &cancellation,
    )));
    let mut work = crate::NativeMaterializationWork::from_meter(original.clone());
    let delivery = Arc::new(Delivery::new(Arc::new(Ignored), &mut work)?);
    let watch = NativeSourceWatch::start_driver(&host, delivery, &mut work)?;

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !watch.shared.polling.load(Ordering::Acquire) && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(watch.shared.polling.load(Ordering::Acquire));
    let paused = watch.paused();
    cancellation.cancel();
    let prefix = original.lock().receipt();

    let wait = CancellationToken::new();
    wait.cancel();
    assert!(matches!(
        watch.stop_with_cleanup_wait(&wait),
        Err(DemandError::Cancelled)
    ));
    assert!(matches!(
        watch.stop_with_cleanup_wait(&CancellationToken::new()),
        Err(DemandError::WorkerUnavailable)
    ));
    assert_eq!(original.lock().receipt(), prefix);
    assert!(!watch.shared.stopping.load(Ordering::Acquire));
    assert!(!watch.is_stopped());
    assert!(
        watch
            .thread
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
    );

    for descriptor in [
        &watch.shared.inotify,
        &watch.shared.wake,
        &watch.shared.root,
    ] {
        assert!(descriptor.load(Ordering::Acquire) >= 0);
    }
    drop(paused);
    // Withhold the already-owned join cursor, then restore that exact cursor. Neither denial nor retry creates additional native credit.
    let reads = {
        let mut cleanup = watch
            .shared
            .cleanup
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        std::mem::take(&mut cleanup.reads)
    };
    let denied = watch.stop_with_cleanup_wait(&CancellationToken::new());
    watch
        .shared
        .cleanup
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .reads = reads;
    assert!(matches!(denied, Err(DemandError::NativeWorkUnavailable)));
    assert_eq!(original.lock().receipt(), prefix);
    assert!(!watch.shared.stopping.load(Ordering::Acquire));
    assert!(!watch.is_stopped());

    let wait = CancellationToken::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match watch.stop_with_cleanup_wait(&wait) {
            Err(DemandError::WorkerUnavailable) if std::time::Instant::now() < deadline => {
                std::thread::yield_now();
            }
            result => {
                result?;
                break;
            }
        }
    }
    assert!(watch.is_stopped());
    assert!(watch.shared.physically_cleared());
    assert!(
        watch
            .thread
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_none()
    );
    let terminal = original.lock().receipt();

    watch.stop_with_cleanup_wait(&wait)?;
    assert_eq!(original.lock().receipt(), terminal);
    drop(watch);

    Ok(())
}

#[cfg(feature = "native-mount")]
#[test]
fn original_cleanup_wait_late_cancel_waits_for_the_actual_native_terminal()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let host =
        crate::native_host::HostRoot::open(root.path(), &mut crate::native_host::ordinary_work())
            .map_err(crate::native_host::ordinary_error)?;
    let original = Arc::new(crate::demand::NativeDemandReadMeter::new(DemandWork::new(
        WorkBudget {
            items_returned: 0,
            ..ORIGINAL_BUDGET
        },
        &CancellationToken::new(),
    )));
    let mut work = crate::NativeMaterializationWork::from_meter(original.clone());
    let delivery = Arc::new(Delivery::new(Arc::new(Ignored), &mut work)?);
    let watch = Arc::new(NativeSourceWatch::start_driver(&host, delivery, &mut work)?);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !watch.shared.polling.load(Ordering::Acquire) && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(watch.shared.polling.load(Ordering::Acquire));
    let wait = CancellationToken::new();
    // Holding only the test's accounting leaf delays recording a genuine wake/retirement, without inventing a mock native completion.
    let state = original.lock();

    let stopping = Arc::clone(&watch);
    let caller_wait = wait.clone();
    let (finished, received) = std::sync::mpsc::channel();
    let stopper = std::thread::spawn(move || {
        let result = stopping.stop_with_cleanup_wait(&caller_wait);
        assert!(finished.send(result).is_ok(), "live test controller");
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !watch.shared.stopping.load(Ordering::Acquire) && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    let started = watch.shared.stopping.load(Ordering::Acquire);
    wait.cancel();
    let premature = received.try_recv();
    let terminal_before_release = watch.is_stopped();
    drop(state);
    let result = match premature {
        Ok(result) => result,
        Err(std::sync::mpsc::TryRecvError::Empty) => received.recv()?,
        Err(error) => return Err(error.into()),
    };
    stopper
        .join()
        .map_err(|_| "actual native stop controller panicked")?;
    assert!(started);
    assert!(!terminal_before_release);
    assert!(matches!(result, Err(DemandError::Cancelled)));
    assert!(watch.is_stopped());
    assert!(watch.shared.physically_cleared());
    assert!(
        watch
            .thread
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_none()
    );

    Ok(())
}

#[cfg(feature = "native-mount")]
#[test]
fn original_cleanup_wait_retains_inflight_callback_and_retires_after_its_panic()
-> Result<(), Box<dyn std::error::Error>> {
    struct PanicsAfterRelease {
        entered: std::sync::mpsc::Sender<()>,
        release: Mutex<bool>,
        changed: Condvar,
    }
    impl HostChangeSink for PanicsAfterRelease {
        fn logical_owned_bytes(&self) -> u64 {
            0
        }
        fn host_changed(&self, _: &[HostChange], _: &mut DemandWork) -> Result<(), DemandError> {
            Err(DemandError::NativeWorkUnavailable)
        }
        fn host_changed_with_native_work(
            &self,
            _: &[HostChange],
            work: &mut crate::NativeMaterializationWork,
        ) -> Result<(), DemandError> {
            work.started_io(WorkCounters {
                items_returned: 1,
                ..WorkCounters::UNCHARGED
            })?;
            assert!(self.entered.send(()).is_ok(), "live callback controller");
            let mut release = self.release.lock().unwrap_or_else(PoisonError::into_inner);
            while !*release {
                release = self
                    .changed
                    .wait(release)
                    .unwrap_or_else(PoisonError::into_inner);
            }
            panic!("controlled native callback unwind");
        }
    }
    let root = tempfile::tempdir()?;
    let host =
        crate::native_host::HostRoot::open(root.path(), &mut crate::native_host::ordinary_work())
            .map_err(crate::native_host::ordinary_error)?;
    let cancellation = CancellationToken::new();
    let original = Arc::new(crate::demand::NativeDemandReadMeter::new(DemandWork::new(
        ORIGINAL_BUDGET,
        &cancellation,
    )));
    let (entered, receive) = std::sync::mpsc::channel();
    let sink = Arc::new(PanicsAfterRelease {
        entered,
        release: Mutex::new(false),
        changed: Condvar::new(),
    });
    let mut work = crate::NativeMaterializationWork::from_meter(original.clone());
    let delivery = Arc::new(Delivery::new(sink.clone(), &mut work)?);
    let watch = NativeSourceWatch::start_driver(&host, delivery, &mut work)?;
    let retained_control = original.lock().live_allocation_bytes()
        - watch.shared.retained_variable_bytes.load(Ordering::Acquire);
    std::fs::write(root.path().join("reported"), b"changed")?;
    receive.recv_timeout(std::time::Duration::from_secs(5))?;
    cancellation.cancel();

    let denied = watch.stop_with_cleanup_wait(&CancellationToken::new());
    let pending_terminal = watch.is_stopped();
    *sink.release.lock().unwrap_or_else(PoisonError::into_inner) = true;
    sink.changed.notify_all();
    assert!(matches!(denied, Err(DemandError::WorkerUnavailable)));
    assert!(!pending_terminal);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !watch.is_stopped() && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(
        watch.is_stopped(),
        "callback unwind is not itself terminal evidence"
    );
    watch.stop_with_cleanup_wait(&CancellationToken::new())?;
    assert!(watch.shared.physically_cleared());
    assert!(
        watch
            .thread
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_none()
    );
    assert!(!watch.delivery().is_exact());
    let mut state = original.lock();
    assert_eq!(
        state.receipt().items_returned,
        1,
        "the panicked sink is never re-entered"
    );
    assert_eq!(state.live_allocation_bytes(), retained_control);

    drop(state);
    drop(watch);

    Ok(())
}

#[cfg(feature = "native-mount")]
#[test]
fn original_late_callback_cancellation_retains_the_driver_until_admitted_quiescence()
-> Result<(), Box<dyn std::error::Error>> {
    struct Cancels {
        original: std::sync::Weak<crate::demand::NativeDemandReadMeter>,
        cancellation: CancellationToken,
        invoked: AtomicBool,
    }
    impl HostChangeSink for Cancels {
        fn logical_owned_bytes(&self) -> u64 {
            0
        }
        fn host_changed(&self, _: &[HostChange], _: &mut DemandWork) -> Result<(), DemandError> {
            Err(DemandError::NativeWorkUnavailable)
        }
        fn host_changed_with_native_work(
            &self,
            _: &[HostChange],
            work: &mut crate::NativeMaterializationWork,
        ) -> Result<(), DemandError> {
            let original = self.original.upgrade().expect("live original callback");
            assert!(original.0.try_lock().is_ok());
            assert!(
                work.shares_ledger(&crate::NativeMaterializationWork::from_meter(
                    original.clone(),
                ))
            );
            work.started_io(WorkCounters {
                items_returned: 1,
                ..WorkCounters::UNCHARGED
            })?;
            self.invoked.store(true, Ordering::Release);
            self.cancellation.cancel();
            work.check()
        }
    }
    let root = tempfile::tempdir()?;
    let host =
        crate::native_host::HostRoot::open(root.path(), &mut crate::native_host::ordinary_work())
            .map_err(crate::native_host::ordinary_error)?;
    let cancellation = CancellationToken::new();
    let original = Arc::new(crate::demand::NativeDemandReadMeter::new(DemandWork::new(
        ORIGINAL_BUDGET,
        &cancellation,
    )));
    let sink = Arc::new(Cancels {
        original: Arc::downgrade(&original),
        cancellation: cancellation.clone(),
        invoked: AtomicBool::new(false),
    });
    let mut work = crate::NativeMaterializationWork::from_meter(original.clone());
    let delivery = Arc::new(Delivery::new(sink.clone(), &mut work)?);
    let watch = NativeSourceWatch::start_driver(&host, delivery, &mut work)?;

    let paused = watch.paused();
    std::fs::write(root.path().join("reported"), b"changed")?;
    drop(paused);
    let _ = watch.fence_events_with_work(std::time::Duration::MAX, &mut work);
    {
        let reading = watch
            .shared
            .reading
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        assert!(reading.filled > reading.cursor);
        assert_eq!(reading.alias_cursor, 0);
    }
    assert!(sink.invoked.load(Ordering::Acquire));
    assert!(!watch.delivery().is_exact());
    assert!(!watch.is_stopped());
    assert!(matches!(
        watch.stop_with_work(&mut work),
        Err(DemandError::Cancelled)
    ));
    for descriptor in [
        &watch.shared.inotify,
        &watch.shared.wake,
        &watch.shared.root,
    ] {
        assert!(descriptor.load(Ordering::Acquire) >= 0);
    }
    let prefix = original.lock().receipt();
    assert!(matches!(
        watch.delivery().take_failure(&mut work),
        Ok(()) | Err(DemandError::Cancelled)
    ));
    assert_eq!(original.lock().receipt(), prefix);
    let wait = CancellationToken::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match watch.stop_with_cleanup_wait(&wait) {
            Err(DemandError::WorkerUnavailable) if std::time::Instant::now() < deadline => {
                std::thread::yield_now();
            }
            result => {
                result?;
                break;
            }
        }
    }
    assert!(watch.is_stopped());

    assert_eq!(original.lock().receipt().items_returned, 1);
    drop(watch);

    Ok(())
}

#[cfg(feature = "native-mount")]
#[test]
fn original_last_controller_drop_in_callback_retires_the_actual_reader()
-> Result<(), Box<dyn std::error::Error>> {
    struct Retires {
        watch: Mutex<Option<NativeSourceWatch>>,
        original: std::sync::Weak<crate::demand::NativeDemandReadMeter>,
        invoked: AtomicBool,
    }
    impl HostChangeSink for Retires {
        fn logical_owned_bytes(&self) -> u64 {
            0
        }
        fn host_changed(&self, _: &[HostChange], _: &mut DemandWork) -> Result<(), DemandError> {
            Err(DemandError::NativeWorkUnavailable)
        }
        fn host_changed_with_native_work(
            &self,
            _: &[HostChange],
            work: &mut crate::NativeMaterializationWork,
        ) -> Result<(), DemandError> {
            let original = self
                .original
                .upgrade()
                .expect("the actual reader retains its original");
            assert!(original.0.try_lock().is_ok());
            assert!(
                work.shares_ledger(&crate::NativeMaterializationWork::from_meter(
                    original.clone(),
                ))
            );
            let watch = self
                .watch
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            drop(watch);
            self.invoked.store(true, Ordering::Release);
            Ok(())
        }
    }
    let root = tempfile::tempdir()?;
    let host =
        crate::native_host::HostRoot::open(root.path(), &mut crate::native_host::ordinary_work())
            .map_err(crate::native_host::ordinary_error)?;
    let original = Arc::new(crate::demand::NativeDemandReadMeter::new(DemandWork::new(
        ORIGINAL_BUDGET,
        &CancellationToken::new(),
    )));
    let lifetime = Arc::downgrade(&original);
    let sink = Arc::new(Retires {
        watch: Mutex::new(None),
        original: lifetime.clone(),
        invoked: AtomicBool::new(false),
    });
    let mut work = crate::NativeMaterializationWork::from_meter(original.clone());
    let delivery = Arc::new(Delivery::new(sink.clone(), &mut work)?);
    let watch = NativeSourceWatch::start_driver(&host, delivery, &mut work)?;
    let reader = Arc::downgrade(&watch.shared);
    *sink.watch.lock().unwrap_or_else(PoisonError::into_inner) = Some(watch);
    drop(work);
    drop(original);
    assert!(lifetime.upgrade().is_some());
    std::fs::write(root.path().join("retire"), b"event")?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while reader.upgrade().is_some() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert!(sink.invoked.load(Ordering::Acquire));
    assert!(
        reader.upgrade().is_none(),
        "physical worker custody reaches its actual terminal"
    );
    assert!(
        lifetime.upgrade().is_none(),
        "the original keeper outlives physical payload, not vice versa"
    );
    Ok(())
}
