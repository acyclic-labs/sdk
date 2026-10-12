use super::*;
use crate::native_host::NativeHostWork as _;
use crate::demand::{DemandWork, NativeDemandReadMeter};
use crate::source_watch::{HostChangeSink, tests::Recorded};
use std::sync::{Condvar, atomic::AtomicBool};

const ITEM_MODIFIED: u32 = 0x1000;
const ITEM_IS_FILE: u32 = 0x1_0000;
const ITEM_IS_DIR: u32 = 0x2_0000;

fn finite_budget() -> crate::WorkBudget {
    crate::WorkBudget { backend_read_operations: 32, backend_write_operations: 32,
        source_entries_visited: 2, source_path_components: 1024, items_examined: 1024,
        bytes_copied: 1024 * 1024, allocation_operations: 16, peak_allocation_bytes: 1024 * 1024,
        ..crate::WorkBudget::default() }
}

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

#[test]
fn callback_copy_denial_and_cancellation_release_owned_changes() -> Result<(), Box<dyn std::error::Error>> {
    struct Cancel(Arc<Recorded>, CancellationToken);
    impl HostChangeSink for Cancel {
        fn logical_owned_bytes(&self) -> u64 { 0 }
        fn host_changed(&self, changes: &[HostChange], work: &mut crate::source_watch::CallbackWork<'_>) -> Result<(), DemandError> {
            self.0.host_changed(changes, work)?;
            self.1.cancel();
            work.check()
        }
    }
    for case in ["copy-denied", "cancel-before", "cancel-in-sink", "delivered"] {
        let cancellation = CancellationToken::new();
        let recorded = Arc::new(Recorded::default());
        let sink: Arc<dyn HostChangeSink> = if case == "cancel-in-sink" { Arc::new(Cancel(recorded.clone(), cancellation.clone())) } else { recorded.clone() };
        let mut budget = finite_budget();
        if case == "copy-denied" { budget.bytes_copied = 0; }
        if case == "cancel-in-sink" { budget.items_examined = 2; }
        let delivery = Arc::new(Delivery { budget, cancellation: cancellation.clone(), ..Delivery::new(sink) });
        let shared = Shared { root: PathBuf::from("/root"), held: (0, 0), delivery,
            stream_bytes: AtomicU64::new(0), completed: AtomicBool::new(false), ownership: SourcePayloadOwnership::default() };
        if case == "cancel-before" { cancellation.cancel(); }
        let mut ordinary = None;
        let mut work = shared.delivery.callback_work(&mut ordinary);
        let result = shared.events(std::iter::once((Path::new("/root/a/b"), ITEM_CREATED)), &mut work);
        match case {
            "copy-denied" => {
                let error = result.err().ok_or("denied copy reached the sink")?;
                assert!(matches!(error, DemandError::Work(_)));
                shared.delivery.failed_with_work(error, &mut work);
                assert!(matches!(shared.delivery.take_failure(&mut DemandWork::new(finite_budget(), &cancellation)), Err(DemandError::Work(_))));
                assert!(!shared.delivery.is_exact());
                assert!(recorded.take().is_empty());
            }
            "cancel-before" => { assert!(matches!(result, Err(DemandError::Cancelled))); assert!(recorded.take().is_empty()); }
            "cancel-in-sink" => { assert!(matches!(result, Err(DemandError::Cancelled))); assert_eq!(recorded.take(), [HostChange::Rebound(PathBuf::from("a/b"))]); }
            _ => { result?; assert_eq!(recorded.take(), [HostChange::Rebound(PathBuf::from("a/b"))]); }
        }
        assert_eq!(work.with_original_leaf(|work| work.live_allocation_bytes()), 0);
    }
    Ok(())
}

#[test]
fn denied_start_and_stop_keep_the_existing_subscription_retryable() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let held = crate::native_host::HostRoot::open(directory.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
    for precancelled in [false, true] {
        let cancellation = CancellationToken::new();
        let mut budget = finite_budget();
        if precancelled { cancellation.cancel(); } else { budget.bytes_copied = 0; }
        let mut work = DemandWork::new(budget, &cancellation);
        let sink = Arc::new(Recorded::default());
        let delivery = Arc::new(Delivery { budget, cancellation, ..Delivery::new(sink) });
        let result = NativeSourceWatch::start_driver(&held, delivery, &mut work);
        assert!(result.is_err());
        assert_eq!(work.work.backend_write_operations, 0);
    }
    let sink = Arc::new(Recorded::default());
    let watch = NativeSourceWatch::start(&held, sink.clone())?;
    let budget = crate::WorkBudget { backend_read_operations: 1, backend_write_operations: 3, ..crate::WorkBudget::default() };
    let cancellation = CancellationToken::new();
    assert!(matches!(watch.stop_with_work(&mut DemandWork::new(budget, &cancellation)), Err(DemandError::Work(_))));
    cancellation.cancel();
    assert!(matches!(watch.stop_with_work(&mut DemandWork::new(finite_budget(), &cancellation)), Err(DemandError::Cancelled)));
    assert!(!watch.is_stopped());
    watch.inject(&directory.path().join("after-denial"), ITEM_CREATED);
    assert_eq!(sink.take(), [HostChange::Rebound(PathBuf::from("after-denial"))]);
    watch.stop_with_work(&mut crate::native_host::ordinary_work())?;
    assert!(watch.is_stopped());
    Ok(())
}

#[test]
fn fence_rejects_denied_reads_and_abandons_a_replaced_root() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("source");
    std::fs::create_dir(&source)?;
    let held = crate::native_host::HostRoot::open(&source, &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
    let recorded = Arc::new(Recorded::default());
    let watch = NativeSourceWatch::start(&held, recorded.clone())?;
    let mut denied = DemandWork::new(crate::WorkBudget::default(), &CancellationToken::new());
    assert!(matches!(watch.fence_with_work(Duration::ZERO, &mut denied), Err(DemandError::Work(_))));
    watch.fence()?;
    assert_eq!(recorded.take(), [HostChange::Unconfirmed]);
    std::fs::rename(&source, directory.path().join("moved"))?;
    std::fs::create_dir(&source)?;
    watch.fence()?;
    assert!(!watch.is_exact());
    assert!(recorded.take().contains(&HostChange::Everything));
    Ok(())
}

#[test]
fn native_cleanup_waits_for_the_callback_after_late_cancel_or_last_owner_drop() -> Result<(), Box<dyn std::error::Error>> {
    struct Gate {
        entered: AtomicBool, release: Mutex<bool>, changed: Condvar,
        owner: Mutex<Option<Arc<NativeSourceWatch>>>, retire: bool,
    }
    impl HostChangeSink for Gate {
        fn logical_owned_bytes(&self) -> u64 { 0 }
        fn host_changed(&self, _: &[HostChange], work: &mut crate::source_watch::CallbackWork<'_>) -> Result<(), DemandError> {
            if !crate::demand::SourceDemandReadWork::is_native_original(work) { return Err(crate::demand::DemandError::NativeWorkUnavailable); }
            work.check()?;
            if self.retire { drop(self.owner.lock().unwrap_or_else(PoisonError::into_inner).take()); }
            self.entered.store(true, Ordering::Release);
            if !self.retire {
                let mut release = self.release.lock().unwrap_or_else(PoisonError::into_inner);
                while !*release { release = self.changed.wait(release).unwrap_or_else(PoisonError::into_inner); }
            }
            Ok(())
        }

    }
    EventStream::initialize_services()?;
    for retire in [false, true] {
        let directory = tempfile::tempdir()?;
        let held = crate::native_host::HostRoot::open(directory.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        let cancellation = CancellationToken::new();
        let mut budget = finite_budget();
        budget.backend_read_operations = 128;
        budget.backend_write_operations = 128;
        budget.allocation_operations = 128;
        budget.items_examined = 4096;
        let original = Arc::new(NativeDemandReadMeter(Mutex::new(DemandWork::new(budget, &cancellation))));
        let gate = Arc::new(Gate { entered: AtomicBool::new(false), release: Mutex::new(false), changed: Condvar::new(), owner: Mutex::new(None), retire });
        let mut work = NativeSourceWork::new(&original);
        let delivery = Arc::new(Delivery::new_with_work(gate.clone(), &mut work)?);
        let watch = Arc::new(NativeSourceWatch::start_driver(&held, delivery, &mut work)?);
        let completed = watch.shared.clone();
        if retire { *gate.owner.lock().unwrap_or_else(PoisonError::into_inner) = Some(watch.clone()); }
        let controlled = if retire { drop(watch); None } else { Some(watch) };
        std::fs::write(directory.path().join("callback"), b"native event")?;
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !gate.entered.load(Ordering::Acquire) {
            if std::time::Instant::now() >= deadline {
                *gate.release.lock().unwrap_or_else(PoisonError::into_inner) = true;
                gate.changed.notify_all();
                return Err("the actual callback did not run".into());
            }
            std::thread::yield_now();
        }
        if let Some(watch) = controlled {
            let before = original.with_original_leaf(|work| work.work.backend_write_operations);
            let wait = CancellationToken::new();
            let stop = { let watch = watch.clone(); let wait = wait.clone(); std::thread::spawn(move || watch.stop_with_cleanup_wait(&wait)) };
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while original.with_original_leaf(|work| work.work.backend_write_operations) == before {
                if std::time::Instant::now() >= deadline {
                    *gate.release.lock().unwrap_or_else(PoisonError::into_inner) = true;
                    gate.changed.notify_all();
                    stop.join().map_err(|_| "native stopper panicked")??;
                    return Err("native stop was not entered".into());
                }
                std::thread::yield_now();
            }
            wait.cancel();
            cancellation.cancel();
            assert!(!watch.is_stopped());
            *gate.release.lock().unwrap_or_else(PoisonError::into_inner) = true;
            gate.changed.notify_all();
            assert!(matches!(stop.join().map_err(|_| "native stopper panicked")?, Err(DemandError::Cancelled)));
            assert!(watch.is_stopped());
            watch.stop_accepted_cleanup()?;
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !completed.completed.load(Ordering::Acquire) {
            assert!(std::time::Instant::now() < deadline, "native callback resources did not retire");
            std::thread::yield_now();
        }
        assert_eq!(completed.stream_bytes.load(Ordering::Acquire), 0);
    }
    Ok(())
}
