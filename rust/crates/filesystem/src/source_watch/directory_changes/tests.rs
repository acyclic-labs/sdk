use super::*;
use crate::demand::{NativeDemandReadMeter, NativeSourceWork};
use crate::native_host::NativeHostWork as _;
use crate::source_watch::{CallbackWork, HostChange, HostChangeSink};
use crate::CancellationToken;
use std::sync::{OnceLock, Weak};

type TestResult = Result<(), Box<dyn std::error::Error>>;

struct Quiet;
impl HostChangeSink for Quiet {
    fn logical_owned_bytes(&self) -> u64 { 0 }
    fn host_changed(&self, _: &[HostChange], work: &mut CallbackWork<'_>) -> Result<(), DemandError> { work.check() }
    fn host_changed_wide(&self, _: &[HostChangeWide<'_>], work: &mut CallbackWork<'_>) -> Result<(), DemandError> { work.check() }
}

fn native_watch() -> Result<(tempfile::TempDir, Arc<NativeDemandReadMeter>, NativeSourceWatch), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let held = cap_std::fs::Dir::open_ambient_dir(root.path(), cap_std::ambient_authority())?;
    let budget = WorkBudget { backend_read_operations: 64, backend_write_operations: 64,
        items_examined: 4096, allocation_operations: 128, peak_allocation_bytes: 4 * 1024 * 1024,
        bytes_copied: 4 * 1024 * 1024, source_bytes_read: 4 * 1024 * 1024, ..WorkBudget::default() };
    let birth = Arc::new(NativeDemandReadMeter(Mutex::new(DemandWork::new(budget, &CancellationToken::new()))));
    let watch = NativeSourceWatch::start_directory_with_work(&held, Arc::new(Quiet), true, &mut NativeSourceWork::new(&birth))?;
    Ok((root, birth, watch))
}

fn retired(owner: &Weak<NativeDemandReadMeter>) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while owner.upgrade().is_some() && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(owner.upgrade().is_none());
}

#[test]
fn ordinary_io_denial_retains_a_usable_owner() -> TestResult {
    let root = tempfile::tempdir()?;
    let held = cap_std::fs::Dir::open_ambient_dir(root.path(), cap_std::ambient_authority())?;
    let cancellation = CancellationToken::new();
    let mut denied = DemandWork::new(WorkBudget { backend_write_operations: 0, ..WorkBudget::UNBOUNDED }, &cancellation);
    assert!(matches!(NativeSourceWatch::start_directory_with_work(&held, Arc::new(Quiet), true, &mut denied), Err(DemandError::Work(_))));
    assert_eq!(denied.work.backend_write_operations, 0);
    let watch = NativeSourceWatch::start_directory_with_work(&held, Arc::new(Quiet), true, &mut crate::native_host::ordinary_work())?;
    assert!(matches!(watch.stop_with_work(&mut denied), Err(DemandError::Work(_))));
    assert!(!watch.is_stopped());
    let reads = watch.shared.lock_reads();
    std::thread::scope(|scope| {
        let (send, receive) = std::sync::mpsc::channel();
        let watch = &watch;
        scope.spawn(move || { let _ = send.send(watch.platform_owned_bytes()); });
        let observed = receive.recv_timeout(Duration::from_secs(1));
        drop(reads);
        observed.map(|bytes| assert!(bytes > 0))
    })?;
    std::fs::write(root.path().join("still-watched"), b"notification")?;
    watch.fence()?;
    watch.stop_with_work(&mut crate::native_host::ordinary_work())?;
    assert!(watch.is_stopped());
    Ok(())
}

#[test]
fn late_wait_cancellation_finishes_accepted_cleanup() -> TestResult {
    let (_root, birth, watch) = native_watch()?;
    let wait = CancellationToken::new();
    birth.with_original_leaf(|work| work.cancellation().cancel());
    *watch.shared.native_wait_cancellation.lock().unwrap_or_else(PoisonError::into_inner) = Some(wait.clone());
    assert!(matches!(watch.stop_with_cleanup_wait(&wait), Err(DemandError::Cancelled)));
    assert!(wait.is_cancelled());
    assert!(watch.is_stopped());
    watch.stop_accepted_cleanup()?;
    Ok(())
}

#[test]
fn poisoned_controller_drop_retires_the_actual_owner() -> TestResult {
    let (_root, birth, watch) = native_watch()?;
    let owner = Arc::downgrade(&watch.shared);
    birth.with_original_leaf(|work| {
        work.cancellation().cancel();
        work.poison_native_original_work();
    });
    drop(watch);
    let meter = Arc::downgrade(&birth);
    drop(birth);
    retired(&meter);
    assert!(owner.upgrade().is_none());
    Ok(())
}

#[test]
fn failed_close_retains_the_owner_until_terminal_retry() -> TestResult {
    use windows::Win32::Foundation::{HANDLE_FLAGS, HANDLE_FLAG_PROTECT_FROM_CLOSE, SetHandleInformation};
    struct Protection(Arc<Shared>);
    impl Drop for Protection {
        fn drop(&mut self) {
            let _reads = self.0.lock_reads();
            // SAFETY: the failed close retained this same protected directory.
            unsafe { SetHandleInformation(HANDLE(self.0.directory.as_raw_handle()), HANDLE_FLAG_PROTECT_FROM_CLOSE.0, HANDLE_FLAGS(0)) }
                .expect("clear owned-directory protection");
        }
    }
    let (_root, birth, watch) = native_watch()?;
    let owner = Arc::downgrade(&watch.shared);
    // SAFETY: this fixture owns the handle and serializes protection with retirement.
    unsafe { SetHandleInformation(HANDLE(watch.shared.directory.as_raw_handle()), HANDLE_FLAG_PROTECT_FROM_CLOSE.0, HANDLE_FLAG_PROTECT_FROM_CLOSE) }
        .map_err(io::Error::from)?;
    let protection = Protection(watch.shared.clone());
    birth.with_original_leaf(|work| work.cancellation().cancel());
    assert!(matches!(watch.stop_with_cleanup_wait(&CancellationToken::new()), Err(DemandError::Io(_))));
    assert!(!watch.is_stopped());
    assert!(owner.upgrade().is_some());
    assert!(matches!(watch.stop_with_cleanup_wait(&CancellationToken::new()), Err(DemandError::NativeWorkUnavailable)));
    drop(protection);
    drop(watch);
    let meter = Arc::downgrade(&birth);
    drop(birth);
    retired(&meter);
    assert!(owner.upgrade().is_none());
    Ok(())
}

#[test]
fn callback_self_stop_keeps_the_watch_retryable() -> TestResult {
    struct SelfStop {
        watch: OnceLock<Weak<NativeSourceWatch>>,
        result: Mutex<Option<bool>>,
        ready: Condvar,
    }
    impl SelfStop {
        fn attempt(&self, work: &mut CallbackWork<'_>) -> Result<(), DemandError> {
            if self.result.lock().unwrap_or_else(PoisonError::into_inner).is_some() { return Ok(()); }
            let Some(watch) = self.watch.get().and_then(Weak::upgrade) else { return Ok(()); };
            let mut stop = work.with_original_leaf(|work| DemandWork::new(work.budget, work.cancellation()));
            *self.result.lock().unwrap_or_else(PoisonError::into_inner) = Some(matches!(watch.stop_with_work(&mut stop), Err(DemandError::WorkerUnavailable)));
            self.ready.notify_all();
            Ok(())
        }
    }
    impl HostChangeSink for SelfStop {
        fn logical_owned_bytes(&self) -> u64 { 0 }
        fn host_changed(&self, _: &[HostChange], work: &mut CallbackWork<'_>) -> Result<(), DemandError> { self.attempt(work) }
        fn host_changed_wide(&self, _: &[HostChangeWide<'_>], work: &mut CallbackWork<'_>) -> Result<(), DemandError> { self.attempt(work) }
    }
    let root = tempfile::tempdir()?;
    let held = cap_std::fs::Dir::open_ambient_dir(root.path(), cap_std::ambient_authority())?;
    let sink = Arc::new(SelfStop { watch: OnceLock::new(), result: Mutex::new(None), ready: Condvar::new() });
    let watch = Arc::new(NativeSourceWatch::start_directory_with_work(&held, sink.clone(), true, &mut crate::native_host::ordinary_work())?);
    assert!(sink.watch.set(Arc::downgrade(&watch)).is_ok());
    std::fs::write(root.path().join("changed"), b"callback")?;
    let result = sink.result.lock().unwrap_or_else(PoisonError::into_inner);
    let (result, _) = sink.ready.wait_timeout_while(result, Duration::from_secs(5), |result| result.is_none()).unwrap_or_else(PoisonError::into_inner);
    assert_eq!(*result, Some(true));
    drop(result);
    assert!(!watch.is_stopped());
    assert!(watch.is_exact());
    watch.stop_with_work(&mut crate::native_host::ordinary_work())?;
    assert!(watch.is_stopped());
    Ok(())
}
