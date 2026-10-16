use super::super::HostChangeSink;
use super::*;
use crate::demand::DemandWork;
use crate::native_host::{HostRoot, ordinary_error, ordinary_work};
use crate::{CancellationToken, WorkBudget, WorkCounters};
use std::mem::size_of;
use std::sync::{OnceLock, Weak};
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn held() -> TestResult<(tempfile::TempDir, HostRoot)> {
    let root = tempfile::tempdir()?;
    let host = HostRoot::open(root.path(), &mut ordinary_work()).map_err(ordinary_error)?;
    Ok((root, host))
}
fn driver<W: SourceDemandReadWork>(
    held: &HostRoot,
    sink: Arc<dyn HostChangeSink>,
    work: &mut W,
) -> Result<NativeSourceWatch, DemandError> {
    let delivery = Arc::new(Delivery::new(sink, work)?);
    NativeSourceWatch::start_driver(held, delivery, work)
}

const ITEM_MODIFIED: u32 = 0x1000;
const ITEM_IS_FILE: u32 = 0x1_0000;
const ITEM_IS_DIR: u32 = 0x2_0000;

struct Ignored;
impl HostChangeSink for Ignored {
    fn logical_owned_bytes(&self) -> u64 {
        0
    }
    fn host_changed(&self, _: &[HostChange], _: &mut DemandWork) -> Result<(), DemandError> {
        Ok(())
    }
}

fn finite_budget() -> WorkBudget {
    WorkBudget {
        backend_read_operations: 32,
        backend_write_operations: 32,
        source_entries_visited: 2,
        source_path_components: 1024,
        items_examined: 1024,
        bytes_copied: 1024 * 1024,
        allocation_operations: 16,
        peak_allocation_bytes: 1024 * 1024,
        ..WorkBudget::default()
    }
}

fn stop_budget() -> WorkBudget {
    WorkBudget {
        backend_write_operations: 4,
        backend_read_operations: 1,
        ..WorkBudget::default()
    }
}

struct CallbackSink {
    calls: std::sync::atomic::AtomicUsize,
    cancel: Option<CancellationToken>,
}
impl HostChangeSink for CallbackSink {
    fn logical_owned_bytes(&self) -> u64 {
        0
    }
    fn host_changed(
        &self,
        changes: &[HostChange],
        work: &mut DemandWork,
    ) -> Result<(), DemandError> {
        if let Some(cancellation) = &self.cancel {
            cancellation.cancel();
            return work.check();
        }
        work.charge(WorkCounters {
            items_returned: changes.len() as u64,
            ..WorkCounters::UNCHARGED
        })?;
        assert!(matches!(&changes[0], HostChange::Rebound(path) if path == Path::new("a/b")));
        self.calls.fetch_add(changes.len(), Ordering::Relaxed);
        Ok(())
    }
}

#[test]
fn callback_admission_and_cancellation_preserve_work_and_retire_owned_paths() -> TestResult {
    for (copy_limit, precancel, cancel_in_sink) in [
        (0, false, false),
        (finite_budget().bytes_copied, false, false),
        (finite_budget().bytes_copied, true, false),
        (finite_budget().bytes_copied, false, true),
    ] {
        let mut budget = finite_budget();
        budget.bytes_copied = copy_limit;
        if !precancel && !cancel_in_sink {
            budget.items_returned = 1;
        }
        if cancel_in_sink {
            budget.items_examined = 2;
        }
        let cancellation = CancellationToken::new();
        let mut original = DemandWork::new(budget, &cancellation);
        let sink = Arc::new(CallbackSink {
            calls: std::sync::atomic::AtomicUsize::new(0),
            cancel: cancel_in_sink.then(|| cancellation.clone()),
        });
        let shared = Shared {
            root: PathBuf::from("/root"),
            held: (0, 0),
            stream_bytes: AtomicU64::new(0),
            delivery: Arc::new(Delivery::new(sink.clone(), &mut original)?),
            ownership: SourcePayloadOwnership::default(),
        };
        if precancel {
            cancellation.cancel();
        }
        let mut callback = shared.delivery.callback_work();
        let result = shared.events(
            std::iter::once((Path::new("/root/a/b"), ITEM_CREATED)),
            &mut callback,
        );
        if precancel || cancel_in_sink {
            assert!(matches!(result, Err(DemandError::Cancelled)));
            if precancel {
                assert_eq!(
                    callback.with_original_leaf(|work| work.work),
                    WorkCounters::UNCHARGED
                );
            }
        } else if copy_limit == 0 {
            let error = result.err().ok_or("callback copied a denied path")?;
            assert!(matches!(error, DemandError::Work(_)));
            assert_eq!(sink.calls.load(Ordering::Relaxed), 0);
            assert_eq!(
                callback.with_original_leaf(|work| work.work).bytes_copied,
                0
            );
            shared.delivery.failed_with_work(error, &mut callback);
            let mut next = DemandWork::new(finite_budget(), &cancellation);
            assert!(matches!(
                shared.delivery.take_failure(&mut next),
                Err(DemandError::Work(_))
            ));
            assert!(!shared.delivery.is_exact());
        } else {
            result?;
            assert_eq!(sink.calls.load(Ordering::Relaxed), 1);
            assert_eq!(
                callback.with_original_leaf(|work| work.work).items_returned,
                1
            );
        }
        assert_eq!(
            callback.with_original_leaf(|work| work.live_allocation_bytes()),
            0
        );
        assert_eq!(callback.with_original_leaf(|work| work.budget), budget);
    }
    Ok(())
}

#[test]
fn stop_denial_and_precancellation_preserve_the_original_stream_for_retry() -> TestResult {
    let (_root, directory) = held()?;
    let watch = driver(&directory, Arc::new(Ignored), &mut ordinary_work())?;
    let owned = watch.platform_owned_bytes();
    assert!(!watch.is_stopped());
    assert!(matches!(
        watch.stop_with_cleanup_wait(&CancellationToken::new()),
        Err(DemandError::NativeWorkUnavailable),
    ));
    assert!(matches!(
        watch.stop_accepted_cleanup(),
        Err(DemandError::NativeWorkUnavailable)
    ));
    assert_eq!(watch.platform_owned_bytes(), owned);
    for (writes, reads) in [(3, 1), (4, 0)] {
        let mut work = DemandWork::new(
            WorkBudget {
                backend_write_operations: writes,
                backend_read_operations: reads,
                ..WorkBudget::default()
            },
            &CancellationToken::new(),
        );
        assert!(matches!(
            watch.stop_with_work(&mut work),
            Err(DemandError::Work(_))
        ));
        assert_eq!(work.work, WorkCounters::default());
        assert!(!watch.is_stopped());
        assert!(watch.delivery().is_exact());
        assert_eq!(watch.platform_owned_bytes(), owned);
        assert!(
            watch
                .stream
                .lock()
                .map_err(|_| "poisoned stream")?
                .is_some()
        );
    }
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let stop_budget = WorkBudget {
        backend_write_operations: 4,
        backend_read_operations: 1,
        ..WorkBudget::default()
    };
    let mut cancelled = DemandWork::new(stop_budget, &cancellation);
    assert!(matches!(
        watch.stop_with_work(&mut cancelled),
        Err(DemandError::Cancelled)
    ));
    assert_eq!(cancelled.work, WorkCounters::default());
    assert!(!watch.is_stopped());
    assert!(watch.delivery().is_exact());
    assert_eq!(watch.platform_owned_bytes(), owned);

    let mut retry = DemandWork::new(stop_budget, &CancellationToken::new());
    watch.stop_with_work(&mut retry)?;
    assert!(watch.is_stopped());
    assert_eq!(retry.work.allocation_operations, 0);
    assert_eq!(retry.work.peak_allocation_bytes, 0);
    assert_eq!(retry.work.items_examined, 0);
    assert!(!watch.delivery().is_exact());
    assert!(watch.platform_owned_bytes() < owned);
    assert!(
        watch
            .stream
            .lock()
            .map_err(|_| "poisoned stream")?
            .is_none()
    );
    let mut again = DemandWork::new(WorkBudget::default(), &CancellationToken::new());
    watch.stop_with_work(&mut again)?;
    assert!(watch.is_stopped());
    assert_eq!(again.work, WorkCounters::default());
    drop(watch);
    Ok(())
}

#[cfg(feature = "native-mount")]
impl crate::fsevents::StreamTerminal for (std::sync::Arc<Shared>, CancellationToken) {
    fn stream_stopped(&self) {
        crate::fsevents::StreamTerminal::stream_stopped(self.0.as_ref());
        self.1.cancel();
    }
}

#[cfg(feature = "native-mount")]
#[test]
fn cleanup_wait_preserves_cancelled_birth_and_finishes_late_cancellation() -> TestResult {
    use crate::demand::{DemandWork, NativeDemandReadMeter};
    use crate::{CancellationToken, DemandError};
    use std::sync::Arc;

    crate::fsevents::EventStream::initialize_services()?;
    for late_wait in [false, true] {
        let (_root, held) = held()?;
        let root_cancellation = CancellationToken::new();
        let mut budget = finite_budget();
        budget.backend_read_operations = 128;
        budget.backend_write_operations = 128;
        budget.allocation_operations = 128;
        budget.items_examined = 4096;
        let birth = Arc::new(NativeDemandReadMeter::new(DemandWork::new(
            budget,
            &root_cancellation,
        )));
        let mut startup = crate::NativeMaterializationWork::from_meter((&birth).clone());
        let delivery = Arc::new(Delivery::new(Arc::new(Ignored), &mut startup)?);
        let watch = NativeSourceWatch::start_driver(&held, delivery, &mut startup)?;
        let owned = watch.platform_owned_bytes();
        let before = birth.with_original_leaf(|work| work.work);
        let held_budget = birth.with_original_leaf(|work| work.budget);
        let declined = CancellationToken::new();
        declined.cancel();
        assert!(matches!(
            watch.stop_with_cleanup_wait(&declined),
            Err(DemandError::Cancelled)
        ));
        assert!(!watch.is_stopped());
        assert!(watch.delivery().is_exact());
        assert_eq!(watch.platform_owned_bytes(), owned);
        assert_eq!(
            birth.with_original_leaf(|work| (work.work, work.budget)),
            (before, held_budget)
        );
        let wait = CancellationToken::new();
        let custody = watch
            .stream
            .lock()
            .map_err(|_| "poisoned actual stream custody")?;
        assert!(matches!(
            watch.stop_with_cleanup_wait(&wait),
            Err(DemandError::WorkerUnavailable)
        ));
        drop(custody);
        assert_eq!(birth.with_original_leaf(|work| work.work), before);
        root_cancellation.cancel();
        assert!(matches!(
            watch.stop_with_work(&mut crate::NativeMaterializationWork::from_meter(
                (&birth).clone()
            )),
            Err(DemandError::Cancelled)
        ));
        if late_wait {
            watch
                .stream
                .lock()
                .map_err(|_| "poisoned stream")?
                .as_mut()
                .ok_or("missing stream")?
                .set_terminal(Arc::new((watch.shared.clone(), wait.clone())));
            assert!(matches!(
                watch.stop_with_cleanup_wait(&wait),
                Err(DemandError::Cancelled)
            ));
            assert!(wait.is_cancelled());
        } else {
            watch.stop_with_cleanup_wait(&wait)?;
        }
        assert!(
            watch.is_stopped(),
            "terminal follows native serial queue drain and context release"
        );
        assert_eq!(watch.shared.stream_bytes.load(Ordering::Acquire), 0);
        assert!(
            watch
                .stream
                .lock()
                .map_err(|_| "poisoned retired stream custody")?
                .is_none()
        );
        assert!(watch.platform_owned_bytes() < owned);
        let stopped = birth.with_original_leaf(|work| {
            assert_eq!(
                work.work.allocation_operations,
                before.allocation_operations
            );
            assert_eq!(
                work.work.peak_allocation_bytes,
                before.peak_allocation_bytes
            );
            assert_eq!(work.budget, budget);
            assert!(work.cancellation().is_cancelled());
            assert!(matches!(work.check(), Err(DemandError::Cancelled)));
            work.work
        });
        watch.stop_with_cleanup_wait(&CancellationToken::new())?;
        watch.stop_accepted_cleanup()?;
        drop(watch);
        assert_eq!(birth.with_original_leaf(|work| work.work), stopped);
    }
    Ok(())
}

#[test]
fn startup_denial_cleans_the_actual_created_stream_and_queue() -> TestResult {
    let (_root, directory) = held()?;
    drop(driver(&directory, Arc::new(Ignored), &mut ordinary_work())?);
    for limit in [1, 7, 9, 10, 11, 12] {
        let mut budget = finite_budget();
        budget.backend_write_operations = limit;
        let mut work = DemandWork::new(budget, &CancellationToken::new());
        let error = driver(&directory, Arc::new(Ignored), &mut work)
            .err()
            .ok_or("startup used the reserved teardown allowance")?;
        assert!(matches!(error, DemandError::Work(_)));
        assert_eq!(work.budget, budget);
        assert_eq!(work.live_allocation_bytes(), 0);
    }
    Ok(())
}

#[test]
fn stop_allows_callback_payload_reads_and_rejects_callback_self_stop() -> TestResult {
    use std::sync::mpsc::{Receiver, Sender, channel};
    type Observed = (u64, bool, bool, Result<(), DemandError>, WorkCounters);
    struct Gate {
        entered: Sender<()>,
        release: Mutex<Option<Receiver<()>>>,
        observed: Sender<Observed>,
        watch: OnceLock<Weak<NativeSourceWatch>>,
        #[cfg(feature = "native-mount")]
        original: OnceLock<Arc<crate::demand::NativeDemandReadMeter>>,
    }
    impl HostChangeSink for Gate {
        fn logical_owned_bytes(&self) -> u64 {
            size_of::<Self>() as u64
        }
        fn host_changed(&self, _: &[HostChange], _: &mut DemandWork) -> Result<(), DemandError> {
            if let Some(release) = self
                .release
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
            {
                let _ = self.entered.send(());
                let _ = release.recv();
            }
            let watch = self
                .watch
                .get()
                .and_then(Weak::upgrade)
                .ok_or(DemandError::WorkerUnavailable)?;
            #[cfg(feature = "native-mount")]
            let unlocked = self
                .original
                .get()
                .is_some_and(|work| work.0.try_lock().is_ok());
            #[cfg(not(feature = "native-mount"))]
            let unlocked = true;
            let mut stop = DemandWork::new(stop_budget(), &CancellationToken::new());
            let bytes = watch.platform_owned_bytes();
            let stopped = watch.is_stopped();
            let result = watch.stop_with_work(&mut stop);
            let _ = self
                .observed
                .send((bytes, stopped, unlocked, result, stop.work));
            Ok(())
        }
    }
    struct Release(Option<Sender<()>>);
    impl Release {
        fn release(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }
    impl Drop for Release {
        fn drop(&mut self) {
            self.release();
        }
    }
    let (root, directory) = held()?;
    let (entered, arrival) = channel();
    let (release, receiver) = channel();
    let (observed, observation) = channel();
    let gate = Arc::new(Gate {
        entered,
        release: Mutex::new(Some(receiver)),
        observed,
        watch: OnceLock::new(),
        #[cfg(feature = "native-mount")]
        original: OnceLock::new(),
    });
    let watch = Arc::new(NativeSourceWatch::start(
        &directory,
        gate.clone(),
        &mut ordinary_work(),
    )?);
    let mut release = Release(Some(release));
    gate.watch
        .set(Arc::downgrade(&watch))
        .map_err(|_| "watch already installed")?;
    #[cfg(feature = "native-mount")]
    let original = Arc::new(crate::demand::NativeDemandReadMeter::new(DemandWork::new(
        stop_budget(),
        &CancellationToken::new(),
    )));
    #[cfg(feature = "native-mount")]
    gate.original
        .set(original.clone())
        .map_err(|_| "stop work already installed")?;
    assert!(!watch.is_stopped());
    let live_bytes = watch.platform_owned_bytes();
    std::fs::write(root.path().join("callback"), b"event")?;
    arrival.recv_timeout(Duration::from_secs(10))?;
    let (finished, completion) = channel();
    std::thread::scope(|scope| -> TestResult {
        let stop = scope.spawn(|| {
            #[cfg(feature = "native-mount")]
            let result = watch.stop_with_work(&mut crate::NativeMaterializationWork::from_meter(
                original.clone(),
            ));
            #[cfg(not(feature = "native-mount"))]
            let result = watch.stop_with_work(&mut DemandWork::new(
                stop_budget(),
                &CancellationToken::new(),
            ));
            let _ = finished.send(());
            result
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let stop_entered = loop {
            match watch.stream.try_lock() {
                Err(std::sync::TryLockError::WouldBlock) => break true,
                Err(std::sync::TryLockError::Poisoned(_)) => break false,
                Ok(_) => {}
            }
            if std::time::Instant::now() >= deadline {
                break false;
            }
            std::thread::yield_now();
        };
        let before_release = completion.recv_timeout(Duration::from_millis(100));
        let stopped_before_release = watch.is_stopped();
        release.release();
        let stopped = stop.join().map_err(|_| "stop worker panicked")?;
        assert!(stop_entered, "stop did not begin");
        assert!(matches!(
            before_release,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));
        assert!(!stopped_before_release);
        let (bytes, stopped_in_callback, original_unlocked, self_stop, work) =
            observation.recv_timeout(Duration::from_secs(10))?;
        assert_eq!(bytes, live_bytes);
        assert!(!stopped_in_callback);
        assert!(original_unlocked);
        assert!(matches!(self_stop, Err(DemandError::WorkerUnavailable)));
        assert_eq!(work, WorkCounters::UNCHARGED);
        stopped?;
        Ok(())
    })?;
    assert!(watch.is_stopped());
    assert!(!watch.delivery().is_exact());
    assert!(watch.platform_owned_bytes() < live_bytes);
    Ok(())
}

#[test]
fn denied_root_copy_never_reaches_metadata_or_the_framework() -> TestResult {
    let (_root, directory) = held()?;
    let mut budget = finite_budget();
    budget.bytes_copied = 0;
    let mut work = DemandWork::new(budget, &CancellationToken::new());
    let error = driver(&directory, Arc::new(Ignored), &mut work)
        .err()
        .ok_or("root conversion exceeded its admitted copy allowance")?;
    assert!(matches!(error, DemandError::Work(_)));
    assert_eq!(work.work.backend_write_operations, 0);
    assert_eq!(work.work.source_entries_visited, 0);
    assert_eq!(work.work.bytes_copied, 0);
    assert_eq!(work.live_allocation_bytes(), 0);
    Ok(())
}

#[test]
fn cancelled_start_admits_no_root_conversion() -> TestResult {
    let (_root, directory) = held()?;
    let cancellation = CancellationToken::new();
    let mut work = DemandWork::new(finite_budget(), &cancellation);
    let delivery = Delivery::new(Arc::new(Ignored), &mut work)?;
    cancellation.cancel();
    let error = NativeSourceWatch::start_driver(&directory, Arc::new(delivery), &mut work)
        .err()
        .ok_or("cancelled startup unexpectedly ran")?;
    assert!(matches!(error, DemandError::Cancelled));
    assert_eq!(work.work, WorkCounters::default());
    Ok(())
}

#[test]
fn measured_fence_rejects_denied_reads_and_abandons_a_replaced_root() -> TestResult {
    let root = tempfile::tempdir()?;
    let source = root.path().join("source");
    std::fs::create_dir(&source)?;
    let directory = HostRoot::open(&source, &mut ordinary_work()).map_err(ordinary_error)?;
    let watch = driver(&directory, Arc::new(Ignored), &mut ordinary_work())?;
    let cancellation = CancellationToken::new();
    let mut denied = DemandWork::new(WorkBudget::default(), &cancellation);
    assert!(matches!(
        watch.fence_with_work(Duration::ZERO, &mut denied),
        Err(DemandError::Work(_))
    ));
    assert_eq!(denied.work, WorkCounters::default());
    assert!(watch.delivery().is_exact());
    std::fs::rename(&source, root.path().join("moved"))?;
    std::fs::create_dir(&source)?;
    let mut work = DemandWork::new(
        WorkBudget {
            backend_read_operations: 1,
            items_examined: 1,
            ..WorkBudget::default()
        },
        &cancellation,
    );
    assert!(!watch.fence_with_work(Duration::ZERO, &mut work)?);
    assert!(!watch.delivery().is_exact());
    assert_eq!(work.work.source_entries_visited, 0);
    Ok(())
}

/// `FSEvents` proves no delivery order, so a fence reports everything.
#[test]
fn a_fence_reports_everything() -> TestResult {
    struct Recorded(Mutex<Vec<HostChange>>);
    impl HostChangeSink for Recorded {
        fn logical_owned_bytes(&self) -> u64 {
            0
        }
        fn host_changed(
            &self,
            changes: &[HostChange],
            work: &mut DemandWork,
        ) -> Result<(), DemandError> {
            work.check()?;
            if let Ok(mut recorded) = self.0.lock() {
                recorded.extend_from_slice(changes);
            }
            Ok(())
        }
    }
    let root = tempfile::tempdir()?;
    let recorded = Arc::new(Recorded(Mutex::new(Vec::new())));
    let watch = NativeSourceWatch::start_driver(
        &HostRoot::open(root.path(), &mut ordinary_work()).map_err(ordinary_error)?,
        Arc::new(Delivery::new(recorded.clone(), &mut ordinary_work())?),
        &mut ordinary_work(),
    )?;
    watch.fence()?;
    let recorded = recorded.0.lock().map_err(|_| "poisoned")?;
    assert!(recorded.contains(&HostChange::Unconfirmed));
    assert!(!recorded.contains(&HostChange::Everything));
    Ok(())
}

#[test]
fn original_callback_last_owner_drop_runs_the_same_queue_terminal_suffix() -> TestResult {
    use crate::demand::NativeDemandReadMeter;
    use crate::{CancellationToken, DemandError};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    struct Retires {
        watch: Mutex<Option<NativeSourceWatch>>,
        original: Weak<NativeDemandReadMeter>,
        cancellation: CancellationToken,
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
            if self.invoked.swap(true, Ordering::AcqRel) {
                return Ok(());
            }
            let original = self
                .original
                .upgrade()
                .ok_or(DemandError::WorkerUnavailable)?;
            assert!(
                original.0.try_lock().is_ok(),
                "callback runs outside the original admission lock"
            );
            assert!(
                work.shares_ledger(&crate::NativeMaterializationWork::from_meter(
                    original.clone()
                ))
            );
            self.cancellation.cancel();
            let watch = self
                .watch
                .lock()
                .map_err(|_| DemandError::WorkerUnavailable)?
                .take();
            drop(watch);
            Ok(())
        }
    }
    crate::fsevents::EventStream::initialize_services()?;
    let root = tempfile::tempdir()?;
    let held = HostRoot::open(root.path(), &mut ordinary_work()).map_err(ordinary_error)?;
    let cancellation = CancellationToken::new();
    let mut budget = finite_budget();
    budget.backend_read_operations = 128;
    budget.backend_write_operations = 128;
    budget.items_examined = 4096;
    budget.allocation_operations = 128;
    let original = Arc::new(NativeDemandReadMeter::new(DemandWork::new(
        budget,
        &cancellation,
    )));
    let lifetime = Arc::downgrade(&original);
    let sink = Arc::new(Retires {
        watch: Mutex::new(None),
        original: lifetime.clone(),
        cancellation,
        invoked: AtomicBool::new(false),
    });
    let mut work = crate::NativeMaterializationWork::from_meter((&original).clone());
    let delivery = Arc::new(Delivery::new(sink.clone(), &mut work)?);
    let watch = NativeSourceWatch::start_driver(&held, delivery, &mut work)?;
    let completion = Arc::clone(&watch.shared);
    let funded_live = original.with_original_leaf(|work| work.live_allocation_bytes());
    *sink.watch.lock().map_err(|_| "poisoned callback custody")? = Some(watch);
    drop(work);
    std::fs::write(root.path().join("retire"), b"native event")?;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while completion.stream_bytes.load(Ordering::Acquire) != 0
        && std::time::Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(sink.invoked.load(Ordering::Acquire));
    assert!(
        completion.stream_bytes.load(Ordering::Acquire) == 0,
        "terminal flag follows actual stream/context/queue release"
    );
    assert_eq!(completion.stream_bytes.load(Ordering::Acquire), 0);
    original.with_original_leaf(|work| {
        assert_eq!(
            work.budget.backend_write_operations,
            budget.backend_write_operations
        );
        assert_eq!(
            work.budget.backend_read_operations,
            budget.backend_read_operations
        );
    });
    drop(completion);
    original.with_original_leaf(|work| {
        work.reconcile_native_controls()?;
        // The original registry's own capacity remains legitimately live.
        assert!(work.live_allocation_bytes() < funded_live);
        Ok::<_, crate::WorkError>(())
    })?;
    drop(original);
    while lifetime.upgrade().is_some() && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(
        lifetime.upgrade().is_none(),
        "no context-slot or custody/root strong cycle remains"
    );
    Ok(())
}

/// An event that does not say which item it changed, or says its directory must be scanned, covers everything beneath its path.
#[test]
fn unspecific_events_cover_everything_beneath_their_path() {
    let root = PathBuf::new;
    let name = || PathBuf::from("d");
    assert_eq!(change(root(), 0), HostChange::Everything);
    assert_eq!(
        change(root(), MUST_SCAN_SUBDIRS | ITEM_IS_DIR),
        HostChange::Everything
    );
    assert_eq!(
        change(root(), ITEM_CREATED | ITEM_IS_DIR),
        HostChange::Altered(root())
    );
    assert_eq!(change(name(), 0), HostChange::Rebound(name()));
    assert_eq!(
        change(name(), MUST_SCAN_SUBDIRS),
        HostChange::Rebound(name())
    );
    for flag in [ITEM_CREATED, ITEM_REMOVED, ITEM_RENAMED] {
        assert_eq!(
            change(name(), flag | ITEM_MODIFIED | ITEM_IS_FILE),
            HostChange::Rebound(name())
        );
    }
    assert_eq!(
        change(name(), ITEM_MODIFIED | ITEM_IS_FILE),
        HostChange::Altered(name())
    );
}
