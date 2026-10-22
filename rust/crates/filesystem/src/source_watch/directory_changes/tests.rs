use super::*;
use crate::native_host::NativeHostWork;
use crate::source_watch::HostChangeSink;
use crate::{CancellationToken, WorkBudget};
use std::os::windows::io::AsHandle;
type TestResult = Result<(), Box<dyn std::error::Error>>;

struct Quiet;
impl HostChangeSink for Quiet {
    fn logical_owned_bytes(&self) -> u64 {
        0
    }
    fn host_changed(&self, _: &[HostChange], work: &mut DemandWork) -> Result<(), DemandError> {
        work.check()
    }
    fn host_changed_wide(
        &self,
        _: &[HostChangeWide<'_>],
        work: &mut DemandWork,
    ) -> Result<(), DemandError> {
        work.check()
    }
    fn host_changed_wide_with_native_work(
        &self,
        _: &[HostChangeWide<'_>],
        work: &mut crate::NativeMaterializationWork,
    ) -> Result<(), DemandError> {
        NativeHostWork::check(work)
    }
}
fn held() -> Result<(tempfile::TempDir, cap_std::fs::Dir), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let dir = cap_std::fs::Dir::open_ambient_dir(root.path(), cap_std::ambient_authority())?;
    Ok((root, dir))
}
fn driver(
    dir: &cap_std::fs::Dir,
    work: &mut impl SourceDemandReadWork,
) -> Result<NativeSourceWatch, DemandError> {
    let delivery = Arc::new(Delivery::new(Arc::new(Quiet), work)?);
    NativeSourceWatch::open_watch(dir.as_handle(), delivery, false, Some(true), false, work)
}
fn assert_retired(shared: &Shared) {
    assert!(shared.retired(&shared.lock_reads()));
    assert_eq!(shared.buffer_bytes.load(Ordering::Acquire), 0);
}
fn wait_retired(shared: &Shared) -> Instant {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !shared.stop_completed.load(Ordering::Acquire) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(shared.stop_completed.load(Ordering::Acquire));
    assert_retired(shared);
    deadline
}
fn record(extended: bool, action: u32, file: u64, name: &str, last: bool) -> Vec<u8> {
    let name = name
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>();
    let mut bytes = vec![0; if extended { 84 } else { 12 }];
    bytes[4..8].copy_from_slice(&action.to_le_bytes());
    let at = if extended { 80 } else { 8 };
    bytes[at..at + 4].copy_from_slice(
        &(u32::try_from(name.len()).expect("fixture name fits the record")).to_le_bytes(),
    );
    if extended {
        bytes[64..72].copy_from_slice(&file.to_le_bytes());
    }
    bytes.extend(name);
    bytes.resize(bytes.len().next_multiple_of(8), 0);
    if !last {
        let length = u32::try_from(bytes.len()).expect("fixture record fits the header");
        bytes[..4].copy_from_slice(&length.to_le_bytes());
    }
    bytes
}
#[test]
fn reports_borrow_native_names_and_identity_in_either_layout() {
    for extended in [true, false] {
        let mut bytes = record(extended, 1, 7, r"d\created", false);
        bytes.extend(record(extended, 3, 9, "written", true));
        let (next, action, name, file) = notification(&bytes, extended).expect("created report");
        assert_eq!((action, file), (1, extended.then_some(7)));
        assert_eq!(name, r"d\created".encode_utf16().collect::<Vec<_>>());
        let (next, action, name, file) =
            notification(&bytes[next..], extended).expect("modified report");
        assert_eq!((next, action, file), (0, 3, extended.then_some(9)));
        assert_eq!(name, "written".encode_utf16().collect::<Vec<_>>());
    }
}
#[test]
fn flexible_utf16_notification_names_are_not_scalar_decoded() {
    for extended in [false, true] {
        let mut bytes = record(extended, 3, 0, "x", true);
        let at = if extended { 84 } else { 12 };
        bytes[at..at + 2].copy_from_slice(&0xD800_u16.to_le_bytes());
        let (_, action, name, _) = notification(&bytes, extended).expect("opaque report");
        assert_eq!((action, name), (3, &[0xD800][..]));
    }
}
#[test]
fn startup_denies_cleanup_before_native_io() -> TestResult {
    let (_root, held) = held()?;
    for budget in [
        WorkBudget {
            backend_read_operations: 1,
            ..WorkBudget::UNBOUNDED
        },
        WorkBudget {
            backend_write_operations: 0,
            ..WorkBudget::UNBOUNDED
        },
    ] {
        let mut work = DemandWork::new(budget, &CancellationToken::new());
        assert!(matches!(
            driver(&held, &mut work),
            Err(DemandError::Work(_))
        ));
        assert_eq!(
            (
                work.work.backend_read_operations,
                work.work.backend_write_operations
            ),
            (0, 0)
        );
    }
    Ok(())
}
#[test]
fn stop_denial_and_cancellation_retain_the_same_native_owner() -> TestResult {
    let (_root, held) = held()?;
    let mut startup = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
    let watch = driver(&held, &mut startup)?;
    assert!(!watch.shared.recursive);
    let directory = watch.shared.directory.load(Ordering::Acquire);
    let owned = watch.platform_owned_bytes();
    assert!(matches!(
        watch.stop_with_cleanup_wait(&CancellationToken::new()),
        Err(DemandError::NativeWorkUnavailable)
    ));
    assert!(matches!(
        watch.stop_accepted_cleanup(),
        Err(DemandError::NativeWorkUnavailable)
    ));
    let reads = watch.paused();
    let observed = std::thread::scope(|scope| {
        let (sender, receiver) = std::sync::mpsc::channel();
        let watch = &watch;
        scope.spawn(move || {
            let _ = sender.send(watch.platform_owned_bytes());
        });
        let observed = receiver.recv_timeout(Duration::from_secs(1));
        drop(reads);
        observed
    })?;
    assert_eq!(observed, owned);
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancelled);
    assert!(matches!(
        watch.stop_with_work(&mut work),
        Err(DemandError::Cancelled)
    ));
    assert_eq!(work.work, WorkCounters::UNCHARGED);
    // Stopping cancels the pending read, waits for it and closes the
    // directory: two writes.
    let mut denied = DemandWork::new(
        WorkBudget {
            backend_write_operations: 1,
            ..WorkBudget::UNBOUNDED
        },
        &CancellationToken::new(),
    );
    assert!(matches!(
        watch.stop_with_work(&mut denied),
        Err(DemandError::Work(_))
    ));
    assert_eq!(denied.work, WorkCounters::UNCHARGED);
    assert!(!watch.is_stopped());
    assert!(watch.delivery().is_exact());
    assert_eq!(watch.shared.directory.load(Ordering::Acquire), directory);
    assert_eq!(watch.platform_owned_bytes(), owned);
    let mut admitted = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
    watch.stop_with_work(&mut admitted)?;
    assert_eq!(admitted.work.allocation_operations, 0);
    assert_eq!(watch.platform_owned_bytes(), owned - INITIAL_BUFFER_BYTES);
    assert_retired(&watch.shared);
    assert!(!watch.delivery().is_exact());
    assert!(watch.is_stopped());
    Ok(())
}

#[cfg(feature = "native-mount")]
mod original {
    use super::*;
    use crate::demand::NativeDemandReadMeter;
    fn budget() -> WorkBudget {
        WorkBudget {
            backend_read_operations: 64,
            backend_write_operations: 64,
            items_examined: 4096,
            allocation_operations: 128,
            peak_allocation_bytes: 4 * 1024 * 1024,
            bytes_copied: 4 * 1024 * 1024,
            source_bytes_read: 4 * 1024 * 1024,
            ..WorkBudget::default()
        }
    }
    fn birth(budget: WorkBudget, cancellation: &CancellationToken) -> Arc<NativeDemandReadMeter> {
        Arc::new(NativeDemandReadMeter::new(DemandWork::new(
            budget,
            cancellation,
        )))
    }
    fn no_growth(birth: &NativeDemandReadMeter, before: WorkCounters, budget: WorkBudget) {
        birth.with_original_leaf(|work| {
            assert_eq!(
                (
                    work.work.allocation_operations,
                    work.work.peak_allocation_bytes,
                    work.budget
                ),
                (
                    before.allocation_operations,
                    before.peak_allocation_bytes,
                    budget
                )
            );
        });
    }
    #[test]
    fn child_reads_keep_root_authority_and_registration_keeps_its_owner() -> TestResult {
        for source_bytes_read in [0, 1024 * 1024] {
            for observer_bytes in [0, 4 * 1024 * 1024] {
                let (root, held) = held()?;
                let cancellation = CancellationToken::new();
                let producer = birth(
                    WorkBudget {
                        source_bytes_read,
                        ..budget()
                    },
                    &cancellation,
                );
                let observer = birth(
                    WorkBudget {
                        source_bytes_read: observer_bytes,
                        ..budget()
                    },
                    &cancellation,
                );
                let mut producer_work =
                    crate::NativeMaterializationWork::from_meter(producer.clone());
                let mut observer_work =
                    crate::NativeMaterializationWork::from_meter(observer.clone());
                let source = driver(&held, &mut producer_work)?;
                let child = source.start_directory_with_work(
                    &held,
                    Arc::new(Quiet),
                    true,
                    &mut observer_work,
                )?;
                assert!(
                    child
                        .delivery()
                        .original
                        .as_ref()
                        .unwrap()
                        .shares_ledger(&observer_work)
                );
                let callback = child.delivery().callback_work();
                assert!(
                    callback
                        .native_source_work()
                        .unwrap()
                        .shares_ledger(&producer_work)
                );
                let observer_prefix = observer_work.receipt();
                std::fs::write(root.path().join("payload"), b"changed")?;
                let result = child.fence_with_work(Duration::from_secs(1), &mut producer_work);
                if source_bytes_read == 0 {
                    assert!(matches!(result, Err(DemandError::Work(_))));
                    assert_eq!(producer_work.receipt().source_bytes_read, 0);
                } else {
                    assert!(result?);
                    assert!(producer_work.receipt().source_bytes_read > 0);
                }
                assert_eq!(
                    observer_work.receipt().source_bytes_read,
                    observer_prefix.source_bytes_read
                );
                // No caller cancellation is under test here. Retire accepted
                // resources directly; a cancellable try-lock may report a
                // transient WorkerUnavailable while a completion owns it.
                child.stop_accepted_cleanup()?;
                source.stop_accepted_cleanup()?;
            }
        }
        Ok(())
    }
    #[test]
    fn native_birth_denies_the_known_final_drop_plan_before_the_first_native_owner() -> TestResult {
        let (_root, held) = held()?;
        // One write short of the six that two retirement attempts reserve.
        let budget = WorkBudget {
            backend_write_operations: 5,
            ..budget()
        };
        let birth = birth(budget, &CancellationToken::new());
        let mut work = crate::NativeMaterializationWork::from_meter(birth.clone());
        let delivery = Arc::new(Delivery::new(Arc::new(Quiet), &mut work)?);
        let lifetime = Arc::downgrade(&delivery);
        assert!(matches!(
            NativeSourceWatch::open_watch(
                held.as_handle(),
                delivery,
                false,
                Some(true),
                false,
                &mut work
            ),
            Err(DemandError::Work(_))
        ));
        assert!(lifetime.upgrade().is_none());
        birth.with_original_leaf(|work| {
            assert_eq!(
                (
                    work.work.backend_read_operations,
                    work.work.backend_write_operations
                ),
                (0, 0)
            );
            assert_eq!(work.budget, budget);
        });
        Ok(())
    }
    #[test]
    fn cleanup_wait_finishes_late_cancellation_without_new_work() -> TestResult {
        let cancellation = CancellationToken::new();
        let budget = WorkBudget {
            source_bytes_read: 0,
            ..budget()
        };
        let (_root, held) = held()?;
        let birth = birth(budget, &cancellation);
        let mut work = crate::NativeMaterializationWork::from_meter(birth.clone());
        let watch = driver(&held, &mut work)?;
        let before = birth.with_original_leaf(|work| work.work);
        let wait = CancellationToken::new();
        let paused = watch.paused();
        assert!(matches!(
            watch.stop_with_cleanup_wait(&wait),
            Err(DemandError::WorkerUnavailable)
        ));
        drop(paused);
        assert_eq!(birth.with_original_leaf(|work| work.work), before);
        cancellation.cancel();
        *watch
            .shared
            .native_wait_cancellation
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(wait.clone());
        assert!(matches!(
            watch.stop_with_cleanup_wait(&wait),
            Err(DemandError::Cancelled)
        ));
        assert!(wait.is_cancelled());
        assert!(watch.is_stopped());
        assert_retired(&watch.shared);
        no_growth(&birth, before, budget);
        Ok(())
    }
    #[test]
    fn any_live_ledger_stops_a_shared_watch_from_its_birth_credit() -> TestResult {
        let budget = budget();
        let (_root, held) = held()?;
        let origin = birth(budget, &CancellationToken::new());
        let mut work = crate::NativeMaterializationWork::from_meter(origin.clone());
        let watch = driver(&held, &mut work)?;
        let before = origin.with_original_leaf(|work| work.work);
        // Negative control: a cancelled foreign caller is refused before any native step.
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let mut refused = crate::NativeMaterializationWork::from_meter(birth(budget, &cancelled));
        assert!(!watch.shared.delivery.birth_matches(&refused));
        assert!(matches!(
            watch.stop_with_work(&mut refused),
            Err(DemandError::Cancelled)
        ));
        assert!(!watch.is_stopped());
        assert!(watch.delivery().is_exact());
        // A live foreign ledger, such as a later request's, stops the same watch.
        let caller = birth(budget, &CancellationToken::new());
        let mut foreign = crate::NativeMaterializationWork::from_meter(caller.clone());
        assert!(!watch.shared.delivery.birth_matches(&foreign));
        watch.stop_with_work(&mut foreign)?;
        assert!(watch.is_stopped());
        assert_retired(&watch.shared);
        // The native suffix was paid from the birth's reserved cleanup credit.
        assert_eq!(foreign.receipt(), WorkCounters::UNCHARGED);
        no_growth(&origin, before, budget);
        origin.with_original_leaf(|work| assert!(work.take_native_io_failure().is_none()));
        Ok(())
    }
    #[test]
    fn poisoned_last_drop_uses_the_same_born_worker() -> TestResult {
        let cancellation = CancellationToken::new();
        let budget = budget();
        let (_root, held) = held()?;
        let birth = birth(budget, &cancellation);
        let mut work = crate::NativeMaterializationWork::from_meter(birth.clone());
        let watch = driver(&held, &mut work)?;
        let completion = Arc::clone(&watch.shared);
        let reads = completion.lock_reads();
        let before = birth.with_original_leaf(|work| work.work);
        let live = birth.with_original_leaf(|work| work.live_allocation_bytes());
        cancellation.cancel();
        birth.with_original_leaf(DemandWork::poison_native_original_work);
        drop(reads);
        drop(watch);
        wait_retired(&completion);
        no_growth(&birth, before, budget);
        birth.with_original_leaf(|work| {
            assert!(work.live_allocation_bytes() > 0);
            assert!(work.live_allocation_bytes() < live);
            assert!(matches!(
                work.take_native_io_failure(),
                Some(DemandError::OriginalWorkPoisoned)
            ));
        });
        Ok(())
    }
    #[test]
    fn actual_close_failure_parks_the_same_owner_and_preserves_the_original_cause() -> TestResult {
        use windows::Win32::Foundation::{
            HANDLE_FLAG_PROTECT_FROM_CLOSE, HANDLE_FLAGS, SetHandleInformation,
        };
        struct Protection(Arc<Shared>);
        impl Drop for Protection {
            fn drop(&mut self) {
                let _reads = self.0.lock_reads();
                // SAFETY: the fixture still owns this protected handle.
                unsafe {
                    SetHandleInformation(
                        HANDLE(self.0.directory.load(Ordering::Acquire)),
                        HANDLE_FLAG_PROTECT_FROM_CLOSE.0,
                        HANDLE_FLAGS(0),
                    )
                }
                .expect("remove close protection");
            }
        }
        let cancellation = CancellationToken::new();
        let budget = budget();
        let (_root, held) = held()?;
        let birth = birth(budget, &cancellation);
        let mut work = crate::NativeMaterializationWork::from_meter(birth.clone());
        let watch = driver(&held, &mut work)?;
        let completion = Arc::clone(&watch.shared);
        let owner = Arc::downgrade(&completion);
        let reads = completion.lock_reads();
        let directory = completion.directory.load(Ordering::Acquire);
        let cursor = reads.cursor;
        // SAFETY: protects the actual held directory for this fixture.
        unsafe {
            SetHandleInformation(
                HANDLE(directory),
                HANDLE_FLAG_PROTECT_FROM_CLOSE.0,
                HANDLE_FLAG_PROTECT_FROM_CLOSE,
            )
        }
        .map_err(io::Error::from)?;
        let protection = Protection(Arc::clone(&completion));
        let before = birth.with_original_leaf(|work| work.work);
        cancellation.cancel();
        drop(reads);
        let failure = watch
            .stop_with_cleanup_wait(&CancellationToken::new())
            .err()
            .ok_or("protected close succeeded")?;
        assert!(matches!(failure, DemandError::Io(error) if error.raw_os_error().is_some()));
        assert!(!watch.is_stopped());
        assert!(owner.upgrade().is_some());
        assert_eq!(completion.directory.load(Ordering::Acquire), directory);
        assert!(is_open(&completion.directory));
        assert_eq!(
            completion.buffer_bytes.load(Ordering::Acquire),
            INITIAL_BUFFER_BYTES
        );
        assert_eq!(completion.lock_reads().cursor, cursor);
        assert!(matches!(
            watch.stop_with_cleanup_wait(&CancellationToken::new()),
            Err(DemandError::NativeWorkUnavailable)
        ));
        assert!(!completion.parked.load(Ordering::Acquire));
        // The retry still owns its terminal reservation. Its capacity cannot
        // return to the original ledger until the protected handle retires.
        let parked_budget = birth.with_original_leaf(|work| work.budget);
        let cleanup = completion
            .cleanup
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        assert!(cleanup[0].is_none());
        assert!(cleanup[1].is_some());
        drop(cleanup);
        no_growth(&birth, before, parked_budget);
        birth.with_original_leaf(|work| {
            assert!(matches!(
                work.check(),
                Err(DemandError::OriginalWorkPoisoned)
            ));
        });
        drop(protection);
        drop(watch);
        let deadline = wait_retired(&completion);
        birth.with_original_leaf(|work| {
            assert_eq!(work.budget, budget);
            assert!(work.live_allocation_bytes() > 0);
        });
        drop(completion);
        while owner.upgrade().is_some() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(owner.upgrade().is_none());
        Ok(())
    }
}

#[test]
fn actual_callback_stop_keeps_the_owner_retryable_without_work() -> TestResult {
    use std::sync::{OnceLock, Weak};
    struct SelfStop {
        watch: OnceLock<Weak<NativeSourceWatch>>,
        result: std::sync::mpsc::SyncSender<(bool, WorkCounters)>,
    }
    impl SelfStop {
        fn attempt(&self, work: &mut DemandWork) -> Result<(), DemandError> {
            if let Some(watch) = self.watch.get().and_then(Weak::upgrade) {
                let mut stop = DemandWork::new(work.budget, work.cancellation());
                let _ = self.result.try_send((
                    matches!(
                        watch.stop_with_work(&mut stop),
                        Err(DemandError::WorkerUnavailable)
                    ),
                    stop.work,
                ));
            }
            Ok(())
        }
    }
    impl HostChangeSink for SelfStop {
        fn logical_owned_bytes(&self) -> u64 {
            0
        }
        fn host_changed(&self, _: &[HostChange], work: &mut DemandWork) -> Result<(), DemandError> {
            self.attempt(work)
        }
        fn host_changed_wide(
            &self,
            _: &[HostChangeWide<'_>],
            work: &mut DemandWork,
        ) -> Result<(), DemandError> {
            self.attempt(work)
        }
    }
    let (root, held) = held()?;
    let (result, received) = std::sync::mpsc::sync_channel(1);
    let sink = Arc::new(SelfStop {
        watch: OnceLock::new(),
        result,
    });
    let cancellation = CancellationToken::new();
    let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let delivery = Arc::new(Delivery::new(sink.clone(), &mut work)?);
    let watch = Arc::new(NativeSourceWatch::open_watch(
        held.as_handle(),
        delivery,
        false,
        Some(true),
        false,
        &mut work,
    )?);
    assert!(sink.watch.set(Arc::downgrade(&watch)).is_ok());
    std::fs::write(root.path().join("changed"), b"callback")?;
    assert_eq!(
        received.recv_timeout(Duration::from_secs(5))?,
        (true, WorkCounters::UNCHARGED)
    );
    assert!(!watch.is_stopped());
    assert!(watch.delivery().is_exact());
    watch.stop_with_work(&mut work)?;
    assert!(watch.is_stopped());
    let mut repeated = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
    watch.stop_with_work(&mut repeated)?;
    assert_eq!(repeated.work, WorkCounters::UNCHARGED);
    Ok(())
}
