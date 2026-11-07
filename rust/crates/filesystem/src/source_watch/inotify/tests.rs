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
    watcher_with_sink(root, Arc::new(Ignored))
}

fn watcher_with_sink(root: &Path, sink: Arc<dyn HostChangeSink>) -> io::Result<NativeSourceWatch> {
    let mut work = crate::native_host::ordinary_work();
    let host = crate::native_host::HostRoot::open(root, &mut work)
        .map_err(crate::native_host::ordinary_error)?;
    let delivery = Delivery::new(sink, &mut work).map_err(crate::native_host::ordinary_error)?;
    NativeSourceWatch::start_driver(&host, Arc::new(delivery), &mut work)
        .map_err(crate::native_host::ordinary_error)
}

std::thread_local! {
    static BEFORE_SUBSCRIBE_LOCK: std::cell::RefCell<Option<Box<dyn FnOnce() -> io::Result<()>>>> =
        const { std::cell::RefCell::new(None) };
    static SUBSCRIBED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(super) fn before_subscribe_lock() -> io::Result<()> {
    BEFORE_SUBSCRIBE_LOCK
        .with(|hook| hook.borrow_mut().take())
        .map_or(Ok(()), |hook| hook())
}

pub(super) fn after_subscribe() {
    SUBSCRIBED.with(|subscribed| subscribed.set(true));
}

#[test]
fn collector_loss_rejects_subscriptions_and_preserves_owned_cleanup()
-> Result<(), Box<dyn std::error::Error>> {
    const CHILD: &str = "ACYCLIC_TEST_INOTIFY_COLLECTOR_LOSS";
    let Some(root) = std::env::var_os(CHILD) else {
        // Permanent loss of the process-wide hub must be isolated from other
        // tests. The parent removes the real roots after the child exits.
        let root = tempfile::tempdir()?;
        std::fs::create_dir_all(root.path().join("child/grandchild"))?;
        let current = std::thread::current();
        let test = current
            .name()
            .ok_or("collector-loss control has no test identity")?;
        let status = std::process::Command::new(std::env::current_exe()?)
            .args(["--exact", test, "--nocapture"])
            .env(CHILD, root.path())
            .status()?;
        assert!(
            status.success(),
            "isolated inotify collector-loss control failed"
        );
        return Ok(());
    };
    struct Observed(std::sync::mpsc::Sender<()>);
    impl HostChangeSink for Observed {
        fn logical_owned_bytes(&self) -> u64 {
            0
        }
        fn host_changed(&self, _: &[HostChange], work: &mut DemandWork) -> Result<(), DemandError> {
            work.check()
        }
        fn delivery_lost(&self) {
            let _ = self.0.send(());
        }
    }
    let root = PathBuf::from(root);
    let (lost, observations) = std::sync::mpsc::channel();
    let first = watcher_with_sink(&root, Arc::new(Observed(lost.clone())))?;
    let second = watcher_with_sink(&root.join("child/grandchild"), Arc::new(Observed(lost)))?;
    let first_cursor = first.paused();
    let second_cursor = second.paused();
    let retained = [
        first.shared.retained_variable_bytes.load(Ordering::Acquire),
        second
            .shared
            .retained_variable_bytes
            .load(Ordering::Acquire),
    ];

    let late = std::thread::scope(|scope| -> io::Result<io::Error> {
        // Both channels belong to this scope: an early failure drops its
        // sender before the scope joins, so the other wait returns an error.
        let (waiting, entered) = std::sync::mpsc::channel();
        let (release, resume) = std::sync::mpsc::channel();
        struct StopDuringReport {
            entered: std::sync::mpsc::Sender<bool>,
            resume: Mutex<std::sync::mpsc::Receiver<()>>,
        }
        impl HostChangeSink for StopDuringReport {
            fn logical_owned_bytes(&self) -> u64 {
                0
            }
            fn host_changed(
                &self,
                _: &[HostChange],
                work: &mut DemandWork,
            ) -> Result<(), DemandError> {
                work.check()
            }
            fn delivery_failed(&self, _: &DemandError) {
                let _ = self.entered.send(true);
                let _ = self.resume.lock().unwrap().recv();
            }
        }
        let (reporting, reported) = std::sync::mpsc::channel();
        let ended = reporting.clone();
        let (release_report, resume_report) = std::sync::mpsc::channel();
        let racing = watcher_with_sink(
            &root,
            Arc::new(StopDuringReport {
                entered: reporting,
                resume: Mutex::new(resume_report),
            }),
        )?;
        let root = &root;
        let subscribing = scope.spawn(move || {
            BEFORE_SUBSCRIBE_LOCK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    waiting.send(()).map_err(io::Error::other)?;
                    resume.recv().map_err(io::Error::other)
                }));
            });
            let result = watcher(root);
            (result, SUBSCRIBED.with(std::cell::Cell::get))
        });
        entered.recv().map_err(io::Error::other)?;
        // The subscription has started but has not acquired routes. A real
        // notification wakes the original collector, whose exit guard must
        // publish loss without either existing watch's read cursor.
        let ending = scope.spawn(move || {
            let result = hub::end_for_test(|| std::fs::write(root.join("collector-wake"), b""));
            let _ = ended.send(false);
            result
        });
        assert!(
            reported.recv().map_err(io::Error::other)?,
            "collector ended without publishing loss"
        );
        // The original collector is inside the sink's failure report, outside
        // all native locks. A simultaneous stop must already own its cause.
        let stopped = racing.stop_with_work(&mut crate::native_host::ordinary_work());
        assert!(
            matches!(stopped, Err(DemandError::Io(ref error))
                if error.kind() == io::ErrorKind::BrokenPipe),
            "cleanup erased collector loss during notification: {stopped:?}"
        );
        assert!(racing.is_stopped());
        release_report.send(()).map_err(io::Error::other)?;
        ending
            .join()
            .map_err(|_| io::Error::other("collector controller unwound"))??;
        racing
            .stop_with_work(&mut crate::native_host::ordinary_work())
            .map_err(crate::native_host::ordinary_error)?;
        release.send(()).map_err(io::Error::other)?;
        let (result, subscribed) = subscribing
            .join()
            .map_err(|_| io::Error::other("subscription control unwound"))?;
        // A later startup read can also refuse, but it cannot excuse an
        // insertion made after the collector detached its subscriber map.
        assert!(
            !subscribed,
            "collector loss admitted a subscription without a producer"
        );
        match result {
            Err(error) => Ok(error),
            Ok(_) => panic!("collector loss admitted a subscription without a producer"),
        }
    })?;
    assert_eq!(late.kind(), io::ErrorKind::BrokenPipe);
    let cause = late
        .get_ref()
        .and_then(|error| error.downcast_ref::<Arc<io::Error>>())
        .ok_or("late subscription lost the original collector cause")?;
    for (watch, bytes) in [&first, &second].into_iter().zip(retained) {
        observations.try_recv()?;
        assert!(!watch.delivery().is_exact());
        assert!(
            !watch.is_stopped(),
            "delivery loss claimed physical retirement"
        );
        assert!(!watch.shared.physically_cleared());
        assert_eq!(
            watch.shared.retained_variable_bytes.load(Ordering::Acquire),
            bytes
        );
    }
    assert!(observations.try_recv().is_err());
    let error = hub::hub()?
        .drain()
        .expect_err("failed collector accepted a fence");
    assert!(
        error
            .get_ref()
            .and_then(|error| error.downcast_ref::<Arc<io::Error>>())
            .is_some_and(|error| Arc::ptr_eq(error, cause))
    );
    drop(first_cursor);
    drop(second_cursor);
    for watch in [&first, &second] {
        let mut work = crate::native_host::ordinary_work();
        let error = watch
            .stop_with_work(&mut work)
            .expect_err("cleanup erased collector loss");
        assert!(matches!(error, DemandError::Io(error) if error.get_ref()
            .and_then(|error| error.downcast_ref::<Arc<io::Error>>())
            .is_some_and(|error| Arc::ptr_eq(error, cause))));
        assert!(watch.is_stopped());
        assert!(watch.shared.physically_cleared());
        watch.stop_with_work(&mut work)?;
    }
    Ok(())
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
    // Startup leaves a retired gauge in the owner's admission registry; its
    // compaction is owner housekeeping that the next owner charge settles.
    original.lock().reconcile_cancellation_registry()?;
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
        state.budget.allocation_operations = state.receipt().allocation_operations;
    }
    assert!(matches!(
        watch.admit_with_work(Path::new("a/b/c"), &mut caller),
        Err(DemandError::Work(WorkError::BudgetExceeded {
            counter: "allocation_operations",
            ..
        }))
    ));
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
    watch.stop_accepted_cleanup()?;
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
    let shared_queue = watch.shared.inotify.load(Ordering::Acquire);
    assert_eq!(shared_queue, hub::hub()?.raw_descriptor());
    let descriptors = [watch.shared.root.load(Ordering::Acquire)];
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
    // The process-wide queue outlives every watch.
    // SAFETY: the hub owns this descriptor for the life of the process.
    assert!(unsafe { libc::fcntl(shared_queue, libc::F_GETFD) } >= 0);
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
    watch.shared.reading().blocked = true;
    let owner_before = original.lock().receipt();
    std::fs::rename(root.path().join("before"), root.path().join("after"))?;
    let incoming = Arc::new(crate::demand::NativeDemandReadMeter::new(DemandWork::new(
        ORIGINAL_BUDGET,
        &CancellationToken::new(),
    )));
    let mut caller = crate::NativeMaterializationWork::from_meter(incoming.clone());
    assert!(watch.fence_events_with_work(&mut caller)?);
    assert!(original.lock().receipt().allocation_operations > owner_before.allocation_operations);
    assert!(incoming.lock().receipt().source_bytes_read > 0);
    assert!(incoming.lock().receipt().bytes_copied > 0);
    assert_eq!(incoming.lock().live_allocation_bytes(), 0);
    assert!(watch.shared.reading().cleanup.is_empty());
    watch.stop_accepted_cleanup()?;
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
    assert_eq!(work.live_allocation_bytes(), 0);

    assert!(
        watch
            .shared
            .watches
            .lock()
            .iter()
            .any(|(path, _)| path == Path::new("a"))
    );
    assert!(
        !watch
            .shared
            .watches
            .lock()
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
    assert_eq!(work.live_allocation_bytes(), 0);

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
            .iter()
            .any(|(path, _)| path == Path::new("a"))
    );
    assert!(
        !watch
            .shared
            .watches
            .lock()
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
            .iter()
            .any(|(path, _)| path == Path::new("a"))
    );
    Ok(())
}

#[cfg(feature = "native-mount")]
#[test]
fn original_cleanup_wait_precancel_and_contention_preserve_cleanup_custody()
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
    let watch = Arc::new(NativeSourceWatch::start_driver(&host, delivery, &mut work)?);

    let paused = watch.paused();
    cancellation.cancel();
    let prefix = original.lock().receipt();

    let wait = CancellationToken::new();
    wait.cancel();
    assert!(matches!(
        watch.stop_with_cleanup_wait(&wait),
        Err(DemandError::Cancelled)
    ));
    super::super::tests::cancel_contended_cleanup(&watch)?;
    // Give the queued cleanup its permit while it is between registration
    // and observation. The actual nonblocking service path then finds that
    // permit held. Cancelling its observer must hand the owed service back,
    // even though no ReadingGuard was ever constructed for that permit.
    super::super::tests::cancel_contended_cleanup_after(&watch, || {
        drop(paused);
        assert!(watch.shared.service_reading().is_none());
        assert!(watch.shared.reading.owed.load(Ordering::SeqCst));
    })?;
    assert!(
        !watch.shared.reading.owed.load(Ordering::SeqCst),
        "cancelled acquisition must hand back the owed service"
    );
    assert_eq!(original.lock().receipt(), prefix);
    assert!(!watch.shared.stopping.load(Ordering::Acquire));
    assert!(!watch.is_stopped());

    for descriptor in [&watch.shared.inotify, &watch.shared.root] {
        assert!(descriptor.load(Ordering::Acquire) >= 0);
    }
    // Withhold the already-owned withdrawal credit, then restore it.
    // Neither refusal nor retirement creates native credit.
    let writes = {
        let mut cleanup = watch.shared.cleanup.lock();
        std::mem::take(&mut cleanup.writes)
    };
    let denied = watch.stop_with_cleanup_wait(&CancellationToken::new());
    watch.shared.cleanup.lock().writes = writes;
    assert!(matches!(denied, Err(DemandError::NativeWorkUnavailable)));
    assert_eq!(original.lock().receipt(), prefix);
    assert!(!watch.shared.stopping.load(Ordering::Acquire));
    assert!(!watch.is_stopped());

    let wait = CancellationToken::new();
    let released = Arc::new(AtomicBool::new(false));
    let observer = Arc::downgrade(&watch);
    let observed = Arc::clone(&released);
    *watch.delivery().after_retirement.lock().unwrap() = Some(Box::new(move || {
        let observer = observer.upgrade().expect("live retirement controller");
        observed.store(
            observer.is_stopped()
                && observer.shared.physically_cleared()
                && observer.controller.try_lock().is_some()
                && observer.shared.reading.raw.try_lock().is_some()
                && observer.shared.retired.try_lock().is_ok()
                && observer.shared.cleanup.try_lock().is_some(),
            Ordering::Release,
        );
    }));
    watch.stop_accepted_cleanup()?;
    assert!(
        released.load(Ordering::Acquire),
        "physical retirement publication releases original driver guards"
    );
    assert!(watch.is_stopped());
    assert!(watch.shared.physically_cleared());
    let terminal = original.lock().receipt();
    assert_eq!(
        terminal.backend_read_operations, prefix.backend_read_operations,
        "retiring shared delivery performs no backend read"
    );

    watch.stop_with_cleanup_wait(&wait)?;
    assert_eq!(original.lock().receipt(), terminal);
    drop(watch);

    Ok(())
}

#[cfg(feature = "native-mount")]
#[test]
fn original_cleanup_wait_late_cancel_waits_for_the_actual_native_terminal()
-> Result<(), Box<dyn std::error::Error>> {
    enum StopEvent {
        Started,
        Finished(Result<(), DemandError>),
    }
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
    let wait = CancellationToken::new();
    // Holding only the test's accounting leaf delays recording a genuine wake/retirement, without inventing a mock native completion.
    let state = original.lock();

    let stopping = Arc::clone(&watch);
    let caller_wait = wait.clone();
    let (finished, received) = std::sync::mpsc::channel();
    let stopper = std::thread::spawn(move || {
        let started = finished.clone();
        BEFORE_UNSUBSCRIBE_RECORD.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                started
                    .send(StopEvent::Started)
                    .expect("live stop observer");
            }));
        });
        let result = stopping.stop_with_cleanup_wait(&caller_wait);
        assert!(
            finished.send(StopEvent::Finished(result)).is_ok(),
            "live test controller"
        );
    });
    let observed = received.recv();
    let started = matches!(&observed, Ok(StopEvent::Started));
    wait.cancel();
    let premature = received.try_recv();
    let terminal_before_release = watch.is_stopped();
    drop(state);
    let terminal = if started {
        match premature {
            Ok(terminal) => terminal,
            Err(std::sync::mpsc::TryRecvError::Empty) => received.recv()?,
            Err(error) => return Err(error.into()),
        }
    } else {
        observed?
    };
    let StopEvent::Finished(result) = terminal else {
        return Err("native stop repeated its admission event".into());
    };
    stopper
        .join()
        .map_err(|_| "actual native stop controller panicked")?;
    assert!(started);
    assert!(!terminal_before_release);
    assert!(matches!(result, Err(DemandError::Cancelled)));
    assert!(watch.is_stopped());
    assert!(watch.shared.physically_cleared());

    Ok(())
}

#[cfg(feature = "native-mount")]
#[test]
fn original_cleanup_wait_retains_inflight_callback_and_retires_after_its_panic()
-> Result<(), Box<dyn std::error::Error>> {
    for panics in [false, true] {
        #[derive(Debug, Eq, PartialEq)]
        enum CallbackEvent {
            Entered,
            Lost,
            Retired(bool),
        }
        struct FailsAfterRelease {
            events: std::sync::mpsc::Sender<CallbackEvent>,
            release: Mutex<bool>,
            changed: Condvar,
            panics: bool,
        }
        struct ReleaseCallback<'a>(&'a FailsAfterRelease);
        impl Drop for ReleaseCallback<'_> {
            fn drop(&mut self) {
                *self
                    .0
                    .release
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner) = true;
                self.0.changed.notify_all();
            }
        }
        impl HostChangeSink for FailsAfterRelease {
            fn logical_owned_bytes(&self) -> u64 {
                0
            }
            fn host_changed(
                &self,
                _: &[HostChange],
                _: &mut DemandWork,
            ) -> Result<(), DemandError> {
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
                self.events
                    .send(CallbackEvent::Entered)
                    .expect("live callback controller");
                let mut release = self.release.lock().unwrap_or_else(PoisonError::into_inner);
                while !*release {
                    release = self
                        .changed
                        .wait(release)
                        .unwrap_or_else(PoisonError::into_inner);
                }
                if self.panics {
                    panic!("controlled native callback unwind");
                }
                Err(DemandError::InvalidRequest)
            }
            fn delivery_lost(&self) {
                self.events
                    .send(CallbackEvent::Lost)
                    .expect("live loss observer");
                panic!("controlled terminal callback unwind");
            }
        }
        let root = tempfile::tempdir()?;
        let host = crate::native_host::HostRoot::open(
            root.path(),
            &mut crate::native_host::ordinary_work(),
        )
        .map_err(crate::native_host::ordinary_error)?;
        let cancellation = CancellationToken::new();
        let original = Arc::new(crate::demand::NativeDemandReadMeter::new(DemandWork::new(
            ORIGINAL_BUDGET,
            &cancellation,
        )));
        let (events, receive) = std::sync::mpsc::channel();
        let sink = Arc::new(FailsAfterRelease {
            events,
            release: Mutex::new(false),
            changed: Condvar::new(),
            panics,
        });
        let mut work = crate::NativeMaterializationWork::from_meter(original.clone());
        let delivery = Arc::new(Delivery::new(sink.clone(), &mut work)?);
        let watch = NativeSourceWatch::start_driver(&host, delivery, &mut work)?;
        let observer = Arc::downgrade(&watch.shared);
        let terminal = sink.events.clone();
        *watch.delivery().after_retirement.lock().unwrap() = Some(Box::new(move || {
            let observer = observer.upgrade().expect("live retirement observer");
            let released = observer.reading.raw.try_lock().is_some()
                && observer.retired.try_lock().is_ok()
                && observer.cleanup.try_lock().is_some();
            terminal.send(CallbackEvent::Retired(released)).unwrap();
        }));
        // Release the original callback before watch cleanup on every error path.
        let release = ReleaseCallback(&sink);
        let retained_control = {
            let mut state = original.lock();
            // Startup's temporary failure custody has retired. Reconcile its gauge
            // through the same budget projection used by the next native call.
            state.remaining()?;
            state.live_allocation_bytes()
                - watch.shared.retained_variable_bytes.load(Ordering::Acquire)
        };
        std::fs::write(root.path().join("reported"), b"changed")?;
        if receive.recv()? != CallbackEvent::Entered {
            return Err("native delivery ended before callback entry".into());
        }
        cancellation.cancel();

        super::super::tests::cancel_contended_cleanup(&watch)?;
        let pending_terminal = watch.is_stopped();
        drop(release);
        assert!(!pending_terminal);

        assert_eq!(receive.recv()?, CallbackEvent::Lost);
        // Loss precedes physical retirement. Observe the original publication,
        // after its read and retirement guards release, before taking over cleanup.
        assert_eq!(receive.recv()?, CallbackEvent::Retired(true));
        assert!(
            watch.is_stopped(),
            "the original service retires its resources"
        );
        let stopped = watch.stop_accepted_cleanup();
        if panics {
            stopped?;
        } else {
            assert!(
                matches!(stopped, Err(DemandError::InvalidRequest)),
                "cleanup must return the original callback refusal"
            );
        }
        let terminal_receipt = original.lock().receipt();
        watch.stop_accepted_cleanup()?;
        assert_eq!(
            original.lock().receipt(),
            terminal_receipt,
            "observing the saved failure cannot repeat native cleanup"
        );
        assert!(
            watch.is_stopped(),
            "callback unwind is not itself terminal evidence"
        );
        watch.delivery_lost();
        assert!(receive.try_recv().is_err(), "one terminal notification");
        assert!(watch.shared.physically_cleared());
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
    }

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

    // The foreground fence owns this callback. Keep the background reader
    // parked until the test explicitly admits terminal cleanup below.
    watch.shared.reading().blocked = true;
    std::fs::write(root.path().join("reported"), b"changed")?;
    let _ = watch.fence_events_with_work(&mut work);
    {
        let reading = watch.shared.reading();
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
    for descriptor in [&watch.shared.inotify, &watch.shared.root] {
        assert!(descriptor.load(Ordering::Acquire) >= 0);
    }
    let prefix = original.lock().receipt();
    assert!(matches!(
        watch.delivery().take_failure(&mut work),
        Ok(()) | Err(DemandError::Cancelled)
    ));
    assert_eq!(original.lock().receipt(), prefix);
    watch.stop_accepted_cleanup()?;
    assert!(watch.is_stopped());

    assert_eq!(original.lock().receipt().items_returned, 1);
    drop(watch);

    Ok(())
}

/// Holds the sink's destruction between Shared's weak-pointer expiry and
/// Delivery's later release of the original keeper. Disconnecting `release`
/// also releases this pause when the test unwinds.
#[cfg(feature = "native-mount")]
struct DropPause {
    entered: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

#[cfg(feature = "native-mount")]
impl Drop for DropPause {
    fn drop(&mut self) {
        if self.entered.send(()).is_ok() {
            let _ = self.release.recv();
        }
    }
}

#[cfg(feature = "native-mount")]
#[test]
fn original_last_controller_drop_in_callback_retires_the_actual_reader()
-> Result<(), Box<dyn std::error::Error>> {
    struct Retires {
        watch: Mutex<Option<NativeSourceWatch>>,
        original: std::sync::Weak<crate::demand::NativeDemandReadMeter>,
        invoked: Arc<AtomicBool>,
        drop_pause: Mutex<Option<DropPause>>,
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
    let invoked = Arc::new(AtomicBool::new(false));
    let sink = Arc::new(Retires {
        watch: Mutex::new(None),
        original: lifetime.clone(),
        invoked: invoked.clone(),
        drop_pause: Mutex::new(None),
    });
    let mut work = crate::NativeMaterializationWork::from_meter(original.clone());
    let delivery = Arc::new(Delivery::new(sink.clone(), &mut work)?);
    let watch = NativeSourceWatch::start_driver(&host, delivery, &mut work)?;
    let reader = Arc::downgrade(&watch.shared);
    let (complete, completed) = std::sync::mpsc::channel();
    watch.observe_data_drop(complete)?;
    let (entered, dropping) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel();
    *sink
        .drop_pause
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = Some(DropPause {
        entered,
        release: released,
    });
    *sink.watch.lock().unwrap_or_else(PoisonError::into_inner) = Some(watch);
    drop(sink);
    drop(work);
    drop(original);
    assert!(lifetime.upgrade().is_some());
    std::fs::write(root.path().join("retire"), b"event")?;
    dropping.recv()?;
    assert!(invoked.load(Ordering::Acquire));
    assert!(reader.upgrade().is_none(), "Shared has started destruction");
    // Negative control for the former inference: Shared's weak pointer is
    // already expired, but its Delivery field still owns the original here.
    assert!(lifetime.upgrade().is_some());
    assert!(
        matches!(
            completed.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ),
        "the data-drop event follows the retained physical payload"
    );
    release.send(())?;
    assert!(
        completed.recv().is_err(),
        "all Shared fields were destroyed"
    );
    assert!(
        lifetime.upgrade().is_none(),
        "the original keeper outlives physical payload, not vice versa"
    );
    Ok(())
}

/// Many watches share the process-wide queue, a fixed set of delivery
/// threads and one kernel registration per shared ancestor; a record about a
/// shared ancestor reaches only the watch whose root it names. Negative
/// control: registering ancestors with `Interest::All` (broadcast routing)
/// delivers the unrelated sibling's record to every watch and fails below.
#[test]
fn watches_share_the_queue_and_route_ancestor_records_by_name()
-> Result<(), Box<dyn std::error::Error>> {
    const WATCHES: usize = 32;
    let root = tempfile::tempdir()?;
    let mut watches = Vec::new();
    for index in 0..WATCHES {
        let path = root.path().join(format!("w{index}"));
        std::fs::create_dir(&path)?;
        watches.push(watcher(&path)?);
    }
    let hub = hub::hub()?;
    let parent = {
        let ancestors = watches
            .first()
            .ok_or("watches exist")?
            .shared
            .ancestors
            .lock();
        ancestors
            .iter()
            .find(|(_, name)| name == "w0")
            .map(|(descriptor, _)| *descriptor)
            .ok_or("the shared parent is an ancestor")?
    };
    for (index, watch) in watches.iter().enumerate() {
        assert_eq!(
            watch.shared.inotify.load(Ordering::Acquire),
            hub.raw_descriptor()
        );
        let ancestors = watch.shared.ancestors.lock();
        let name = format!("w{index}");
        assert!(
            ancestors
                .iter()
                .any(|(descriptor, on_the_way)| *descriptor == parent && *on_the_way == *name)
        );
    }
    assert_eq!(hub.interests(parent), (0, WATCHES));
    let threads = std::fs::read_dir("/proc/self/task")?
        .filter_map(Result::ok)
        .filter(|task| {
            std::fs::read_to_string(task.path().join("comm"))
                .is_ok_and(|name| name.starts_with("acyclic-watch"))
        })
        .count();
    assert_eq!(threads, 3, "delivery threads do not grow with watches");

    // Paused watches keep routed records in their inboxes for inspection;
    // records other tests' activity routed earlier stay out of the comparison.
    let mut paused = watches
        .iter()
        .map(|watch| watch.shared.reading())
        .collect::<Vec<_>>();
    let pending = || {
        watches
            .iter()
            .map(|watch| {
                watch
                    .shared
                    .inbox
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .pending()
            })
            .collect::<Vec<_>>()
    };
    hub.drain()?;
    let before = pending();
    std::fs::create_dir(root.path().join("unrelated"))?;
    std::fs::rename(root.path().join("w5"), root.path().join("moved"))?;
    hub.drain()?;
    for (index, (after, before)) in pending().into_iter().zip(before).enumerate() {
        assert_eq!(after != before, index == 5, "watch {index}");
    }
    // Watch 5 stops with its routed records still unread: stopping releases
    // them with the rest of its retained bytes. Negative control: without
    // the inbox reset in `clear_measured` they outlive the watch.
    paused.get_mut(5).ok_or("watch 5 exists")?.blocked = true;
    drop(paused);

    for watch in &watches {
        watch.stop_with_work(&mut DemandWork::new(
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        ))?;
        assert!(watch.shared.physically_cleared());
        assert_eq!(
            watch
                .shared
                .inbox
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .pending(),
            (0, false)
        );
    }
    assert_eq!(hub.interests(parent), (0, 0));
    Ok(())
}

/// A watch never receives records queued before it registered. A directory
/// other watches already share holds the record of this watch's root being
/// created; routed to the new watch's ancestor interest, it would report the
/// root rebound and leave the watch inexact. Negative control: registering
/// without first routing the queued records (`Hub::add` skipping
/// `drain_locked`) fails the exactness assertion below.
#[test]
fn a_registration_never_receives_records_that_predate_it() -> Result<(), Box<dyn std::error::Error>>
{
    let parent = tempfile::tempdir()?;
    std::fs::create_dir(parent.path().join("a"))?;
    // Registers `parent` for the child name `a`, so the kernel queues every
    // later creation in `parent` on the shared descriptor.
    let first = watcher(&parent.path().join("a"))?;
    let second = {
        // Keep the router from routing the creation of `b` before `b`'s watch
        // registers `parent` for the name `b`.
        let _paused = hub::ROUTER_PAUSED
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        std::fs::create_dir(parent.path().join("b"))?;
        watcher(&parent.path().join("b"))?
    };
    second.fence()?;
    assert!(second.shared.delivery.is_exact());
    first.fence()?;
    assert!(first.shared.delivery.is_exact());
    Ok(())
}

/// A watch whose cursor is held never stalls delivery to other watches.
/// Every watch shares the hub's service pool; a pass that waited for a held
/// cursor parked a pool thread, and once records had scheduled the held watch
/// once per pool thread, no other watch in the process was delivered until the
/// cursor was released. A long pause (`lost_changes_are_reported_as_everything`)
/// starved concurrent demand watches past their deadline.
/// The held watch is served as soon as its cursor is released. Negative
/// controls: a service pass taking `self.reading()` instead of
/// `service_reading()` fails the wait for the other watch; a guard that does
/// not reschedule its owed pass fails the wait for the held watch.
#[test]
fn a_held_cursor_never_stalls_delivery_to_other_watches() -> Result<(), Box<dyn std::error::Error>>
{
    let held_root = tempfile::tempdir()?;
    let other_root = tempfile::tempdir()?;
    let (held, held_changes) = crate::source_watch::tests::watch(held_root.path())?;
    let (_other, other_changes) = crate::source_watch::tests::watch(other_root.path())?;
    let hub = hub::hub()?;
    let paused = held.paused();
    // Each routed record schedules the held watch again once a pass started,
    // so it reaches as many pool threads as it receives records.
    for index in 0..hub::SERVICE_THREADS {
        let started = held
            .shared
            .inbox
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .next_service();
        std::fs::write(held_root.path().join(format!("held-{index}")), b"")?;
        hub.drain()?;
        started.recv()?;
    }
    std::fs::write(other_root.path().join("other"), b"")?;
    other_changes.wait_until(|changes| !changes.is_empty())?;
    assert!(held_changes.take().is_empty());
    drop(paused);
    held_changes.wait_until(|changes| !changes.is_empty())?;
    Ok(())
}

std::thread_local! {
    /// Runs once at the controller's unsubscribe, after its credit check.
    static BEFORE_UNSUBSCRIBE_RECORD: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

pub(super) fn before_unsubscribe_record() {
    if let Some(hook) = BEFORE_UNSUBSCRIBE_RECORD.with(|hook| hook.borrow_mut().take()) {
        hook();
    }
}

// A service pass that passed its stopping check before the controller began
// to stop fails on the cancelled root and retires the watch. Forced order: it
// retires between the controller's unsubscribe credit check and its record,
// unless the controller excludes it there. Both withdraw the same registration,
// so exactly one may spend that write.
#[cfg(feature = "native-mount")]
#[test]
fn original_stop_and_a_failed_service_retirement_spend_the_unsubscribe_write_once()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let host =
        crate::native_host::HostRoot::open(root.path(), &mut crate::native_host::ordinary_work())
            .map_err(crate::native_host::ordinary_error)?;
    let cancellation = CancellationToken::new();
    let original = Arc::new(crate::demand::NativeDemandReadMeter::new(DemandWork::new(
        ORIGINAL_BUDGET,
        &cancellation,
    )));
    let mut work = crate::NativeMaterializationWork::from_meter(original.clone());
    let delivery = Arc::new(Delivery::new(Arc::new(Ignored), &mut work)?);
    let watch = NativeSourceWatch::start_driver(&host, delivery, &mut work)?;
    cancellation.cancel();

    let shared = Arc::clone(&watch.shared);
    let (late_tx, late_rx) = std::sync::mpsc::channel();
    BEFORE_UNSUBSCRIBE_RECORD.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            let free = shared.retired.try_lock().is_ok();
            let retire = move || {
                let mut callback = shared.delivery.callback_work();
                shared
                    .delivery
                    .failed_with_work(DemandError::Cancelled, &mut callback);
                shared.retire();
            };
            if free {
                retire();
            } else {
                // The controller holds retirement: the pass retires after it.
                late_tx.send(std::thread::spawn(retire)).unwrap();
            }
        }));
    });
    let stopped = watch.stop_accepted_cleanup();
    BEFORE_UNSUBSCRIBE_RECORD.with(|hook| assert!(hook.borrow().is_none(), "hook not reached"));
    if let Ok(late) = late_rx.try_recv() {
        late.join().map_err(|_| "late retirement panicked")?;
    }
    assert!(watch.is_stopped());
    assert!(watch.shared.physically_cleared());
    let terminal = original.lock().receipt();
    assert!(
        matches!(
            (stopped, watch.stop_accepted_cleanup()),
            (Err(DemandError::Cancelled), Ok(())) | (Ok(()), Err(DemandError::Cancelled))
        ),
        "the service's original failure reaches exactly one cleanup observer"
    );
    watch.stop_accepted_cleanup()?;
    assert_eq!(original.lock().receipt(), terminal);
    drop(watch);
    Ok(())
}
