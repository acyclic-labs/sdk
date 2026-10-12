use super::{HostChange, HostChangeSink, NativeSourceWatch};
use crate::native_host::HostRoot;
use crate::demand::{DemandError};
use crate::native_host::NativeHostWork;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Every change delivered, in order.
#[derive(Default)]
struct Recorded(Mutex<Vec<HostChange>>);

impl HostChangeSink for Recorded {
    fn logical_owned_bytes(&self) -> u64 { 0 }

    fn host_changed(&self, changes: &[HostChange], work: &mut crate::demand::DemandWork)
        -> Result<(), DemandError>
    {
        work.check()?;
        self.0.lock().unwrap_or_else(PoisonError::into_inner).extend_from_slice(changes);
        Ok(())
    }

    fn host_changed_with_native_work(&self, changes: &[HostChange], work: &mut crate::NativeMaterializationWork)
        -> Result<(), DemandError>
    {
        work.check()?;
        self.0.lock().unwrap_or_else(PoisonError::into_inner).extend_from_slice(changes);
        Ok(())
    }

    #[cfg(windows)]
    fn host_changed_wide(&self, changes: &[super::HostChangeWide<'_>], work: &mut crate::demand::DemandWork)
        -> Result<(), DemandError>
    {
        work.check()?;
        self.record_wide(changes);
        Ok(())
    }

    #[cfg(windows)]
    fn host_changed_wide_with_native_work(&self, changes: &[super::HostChangeWide<'_>], work: &mut crate::NativeMaterializationWork)
        -> Result<(), DemandError>
    {
        work.check()?;
        self.record_wide(changes);
        Ok(())
    }
}

impl Recorded {
    #[cfg(windows)]
    fn record_wide(&self, changes: &[super::HostChangeWide<'_>]) {
        use std::os::windows::ffi::OsStringExt as _;
        self.0.lock().unwrap_or_else(PoisonError::into_inner).extend(changes.iter().map(|change| match change {
            super::HostChangeWide::Path { action, path } => {
                let path = PathBuf::from(std::ffi::OsString::from_wide(path));
                if *action == 3 { HostChange::Altered(path) } else { HostChange::Rebound(path) }
            }
            super::HostChangeWide::File(file) => HostChange::File(*file),
            super::HostChangeWide::Everything => HostChange::Everything,
            super::HostChangeWide::Unconfirmed => HostChange::Unconfirmed,
        }));
    }

    fn take(&self) -> Vec<HostChange> {
        std::mem::take(&mut *self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }

    fn reported(changes: &[HostChange], path: &Path) -> bool {
        changes.iter().any(|change| match change {
            HostChange::Rebound(reported) | HostChange::Altered(reported) => reported == path,
            _ => false,
        })
    }

    fn rebound(changes: &[HostChange], path: &Path) -> bool {
        changes.contains(&HostChange::Rebound(path.to_path_buf()))
    }

    fn everything(changes: &[HostChange]) -> bool {
        changes.contains(&HostChange::Everything)
    }

    /// Whether a fence reported that changes may be undelivered, which it
    fn unconfirmed(changes: &[HostChange]) -> bool {
        #[cfg(any(target_os = "macos", windows))]
        return changes.contains(&HostChange::Unconfirmed);
        #[cfg(not(any(target_os = "macos", windows)))]
        {
            let _ = changes;
            false
        }
    }
}

/// Everything reported until `reported` holds of it, or ten seconds pass. A
fn reports_until(
    watch: &NativeSourceWatch,
    recorded: &Recorded,
    reported: impl Fn(&[HostChange]) -> bool,
) -> std::io::Result<Vec<HostChange>> {
    watch.fence()?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut changes = recorded.take();
    while !reported(&changes) && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(10));
        changes.extend(recorded.take());
    }
    Ok(changes)
}

fn watch(root: &Path) -> Result<(NativeSourceWatch, Arc<Recorded>), Box<dyn std::error::Error>> {
    let recorded = Arc::new(Recorded::default());
    let watch = NativeSourceWatch::start(&HostRoot::open(root, &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?, recorded.clone())?;
    Ok((watch, recorded))
}

/// Watches `directory` where the source would, before reading beneath it.
fn admit(watch: &NativeSourceWatch, directory: &str) {
    #[cfg(target_os = "linux")]
    watch.admit(Path::new(directory));
    #[cfg(not(target_os = "linux"))]
    let _ = (watch, directory);
}

/// Every kind of change beneath the root is reported under the names it
#[test]
fn changes_beneath_the_root_are_reported_by_name() -> TestResult {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("d"))?;
    std::fs::write(root.path().join("d").join("f"), b"one")?;
    std::fs::write(root.path().join("g"), b"one")?;
    let (watch, recorded) = watch(root.path())?;
    admit(&watch, "d");

    std::fs::write(root.path().join("d").join("f"), b"changed")?;
    std::fs::write(root.path().join("new"), b"new")?;
    std::fs::rename(root.path().join("g"), root.path().join("h"))?;
    std::fs::remove_file(root.path().join("new"))?;
    std::fs::create_dir(root.path().join("e"))?;
    let expected = ["d/f", "new", "g", "h", "e"];
    let changes = reports_until(&watch, &recorded, |changes| {
        expected
            .iter()
            .all(|path| Recorded::reported(changes, Path::new(path)))
    })?;
    for path in expected {
        assert!(
            Recorded::reported(&changes, &PathBuf::from(path)),
            "{path} was not reported: {changes:?}"
        );
    }
    // A write changes the entry in place (which `FSEvents`, coalescing a
    // recent creation into it, may report as the rebinding it could be);
    // every name operation rebinds.
    #[cfg(not(target_os = "macos"))]
    assert!(changes.contains(&HostChange::Altered(PathBuf::from("d").join("f"))));
    for path in ["new", "g", "h", "e"] {
        assert!(
            Recorded::rebound(&changes, Path::new(path)),
            "{path}: {changes:?}"
        );
    }
    // Nothing was lost.
    assert!(
        !Recorded::everything(&changes),
        "nothing was lost: {changes:?}"
    );
    assert!(watch.is_exact());
    Ok(())
}

/// A fence returns only once every change that completed before it has
#[test]
fn a_fence_delivers_every_change_completed_before_it() -> TestResult {
    let root = tempfile::tempdir()?;
    let (watch, recorded) = watch(root.path())?;
    for index in 0..500 {
        let created = format!("f{index}");
        std::fs::write(root.path().join(&created), b"x")?;
        let mut expected = vec![PathBuf::from(created)];
        if index >= 1 {
            let written = format!("f{}", index - 1);
            std::fs::write(root.path().join(&written), b"written")?;
            expected.push(PathBuf::from(written));
        }
        if index >= 2 {
            let (from, to) = (format!("f{}", index - 2), format!("g{}", index - 2));
            std::fs::rename(root.path().join(&from), root.path().join(&to))?;
            expected.extend([PathBuf::from(from), PathBuf::from(to)]);
        }
        watch.fence()?;
        let changes = recorded.take();
        for path in expected {
            assert!(
                Recorded::reported(&changes, &path) || Recorded::unconfirmed(&changes),
                "{path:?} changed before fence {index} but was not delivered by it: {changes:?}"
            );
        }
    }
    Ok(())
}

/// Changes the host could not queue are reported as everything, and the
#[cfg(any(target_os = "linux", windows))]
#[test]
fn lost_changes_are_reported_as_everything() -> TestResult {
    let root = tempfile::tempdir()?;
    let (watch, recorded) = watch(root.path())?;
    {
        let _paused = watch.paused();
        // More than the host queues: inotify's default queue holds 16384
        // events; a directory read buffer holds 64 KiB of reports.
        let flood = if cfg!(windows) { 3_000 } else { 20_000 };
        for index in 0..flood {
            std::fs::write(root.path().join(format!("flood-{index:05}")), b"")?;
        }
    }
    watch.fence()?;
    assert!(Recorded::everything(&recorded.take()));
    std::fs::write(root.path().join("after"), b"x")?;
    watch.fence()?;
    assert!(Recorded::reported(&recorded.take(), Path::new("after")));
    assert!(watch.is_exact());
    Ok(())
}

/// `FSEvents` reports lost events with flags, which report everything.
#[cfg(target_os = "macos")]
#[test]
fn lost_changes_are_reported_as_everything() -> TestResult {
    let root = tempfile::tempdir()?;
    let (watch, recorded) = watch(root.path())?;
    for flags in [0x02, 0x04, 0x40, 0x80] {
        watch.inject(root.path(), flags);
        assert!(Recorded::everything(&recorded.take()), "flags {flags:#x}");
    }
    assert!(watch.is_exact());
    Ok(())
}

/// A directory that moves is watched under the name it moved to once the
#[cfg(target_os = "linux")]
#[test]
fn a_moved_directory_reports_changes_under_its_new_name() -> TestResult {
    let root = tempfile::tempdir()?;
    std::fs::create_dir_all(root.path().join("a").join("b"))?;
    let (watch, recorded) = watch(root.path())?;
    admit(&watch, "a/b");
    std::fs::rename(root.path().join("a"), root.path().join("c"))?;
    watch.fence()?;
    let changes = recorded.take();
    assert!(Recorded::reported(&changes, Path::new("a")));
    assert!(Recorded::reported(&changes, Path::new("c")));
    admit(&watch, "c/b");
    std::fs::write(root.path().join("c").join("b").join("x"), b"x")?;
    watch.fence()?;
    let changes = recorded.take();
    assert!(
        Recorded::reported(&changes, Path::new("c/b/x")),
        "{changes:?}"
    );
    assert!(!Recorded::reported(&changes, Path::new("a/b/x")));
    Ok(())
}

/// A root that stops being reported makes the watch inexact for good,
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn a_removed_root_makes_the_watch_inexact() -> TestResult {
    let parent = tempfile::tempdir()?;
    let root = parent.path().join("root");
    std::fs::create_dir(&root)?;
    let (watch, recorded) = watch(&root)?;
    std::fs::remove_dir(&root)?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while watch.is_exact() {
        assert!(
            std::time::Instant::now() < deadline,
            "the watch stayed exact"
        );
        let _ = watch.fence();
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(Recorded::everything(&recorded.take()));
    Ok(())
}

/// While a Windows watch lives, its root's path keeps naming the directory
#[cfg(windows)]
#[test]
fn a_watched_root_keeps_its_name() -> TestResult {
    let parent = tempfile::tempdir()?;
    let root = parent.path().join("root");
    std::fs::create_dir(&root)?;
    let (watch, _recorded) = watch(&root)?;
    assert!(std::fs::rename(&root, parent.path().join("moved")).is_err());
    assert!(std::fs::remove_dir(&root).is_err());
    drop(watch);
    std::fs::rename(&root, parent.path().join("moved"))?;
    Ok(())
}

/// A root whose path stops naming it, because a directory on the way moved,
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn a_root_whose_path_moves_makes_the_watch_inexact() -> TestResult {
    let parent = tempfile::tempdir()?;
    let root = parent.path().join("a").join("root");
    std::fs::create_dir_all(&root)?;
    let (watch, recorded) = watch(&root)?;
    std::fs::write(root.join("before"), b"x")?;
    watch.fence()?;
    assert!(watch.is_exact());
    std::fs::rename(parent.path().join("a"), parent.path().join("b"))?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while watch.is_exact() {
        assert!(
            std::time::Instant::now() < deadline,
            "the watch stayed exact"
        );
        let _ = watch.fence();
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(Recorded::everything(&recorded.take()));
    Ok(())
}

fn original_watch_budget() -> Result<crate::WorkBudget, Box<dyn std::error::Error>> {
    let mut value = serde_json::to_value(crate::WorkBudget::UNBOUNDED)?;
    if let serde_json::Value::Object(fields) = &mut value {
        for allowance in fields.values_mut() { *allowance = serde_json::json!(1_000_000_u64); }
    }
    Ok(serde_json::from_value(value)?)
}

fn cleanup_original_watch(watch: &NativeSourceWatch, wait: &crate::CancellationToken) -> TestResult {
    // The OS must eventually complete its existing join/serial completion.
    // This deadline bounds test retries, not an admitted native shutdown.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        match watch.stop_with_cleanup_wait(wait) {
            Err(DemandError::WorkerUnavailable) if std::time::Instant::now() < deadline => {
                std::thread::yield_now();
            }
            result => { result?; return Ok(()); }
        }
    }
}

#[test]
fn cleanup_wait_ordinary_wrapper_rejects_before_stop_and_still_delivers() -> TestResult {
    let root = tempfile::tempdir()?;
    let (watch, recorded) = watch(root.path())?;
    let bytes = watch.logical_owned_bytes();
    assert!(matches!(watch.stop_with_cleanup_wait(&crate::CancellationToken::new()),
        Err(DemandError::NativeWorkUnavailable)));
    assert!(matches!(watch.stop_accepted_cleanup(), Err(DemandError::NativeWorkUnavailable)),
        "accepted cleanup cannot turn an ordinary subscription into original native credit");
    assert!(!watch.is_stopped());
    assert_eq!(watch.logical_owned_bytes(), bytes);
    std::fs::write(root.path().join("after-denial"), b"real notification")?;
    let changes = reports_until(&watch, &recorded, |changes| {
        Recorded::reported(changes, Path::new("after-denial"))
    })?;
    assert!(Recorded::reported(&changes, Path::new("after-denial")), "{changes:?}");
    let mut cleanup = crate::demand::DemandWork::new(
        crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
    );
    watch.stop_with_work(&mut cleanup)?;
    assert!(watch.is_stopped());
    Ok(())
}

#[test]
fn cleanup_wait_original_wrapper_preserves_birth_owner_through_root_cancellation() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    let directory = tempfile::tempdir()?;
    let host = HostRoot::open(directory.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
    let budget = original_watch_budget()?;
    let cancellation = crate::CancellationToken::new();
    let root = crate::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(error, _)| error)?;
    let scope = root.native_volume_work(budget)?;
    let original = scope.clone();
    let resource = Arc::downgrade(scope.resource.as_ref().ok_or("birth resource absent")?);
    let meter = Arc::downgrade(&original.state);
    let recorded = Arc::new(Recorded::default());
    let before = scope.receipt();
    let mut work = crate::NativeMaterializationWork::clone(&original);
    let watch = NativeSourceWatch::start_with_work(&host, recorded.clone(), &mut work)?;
    let started = scope.receipt();
    assert!(started.backend_read_operations > before.backend_read_operations);
    assert!(started.allocation_operations > before.allocation_operations);
    assert!(!watch.is_stopped());
    let delivery = watch.delivery();
    let birth = delivery.original.as_ref().ok_or("original delivery absent")?;
    assert!(Arc::ptr_eq(&birth.state, &original.state));
    assert!(Arc::ptr_eq(
        birth.resource.as_ref().ok_or("retained resource absent")?,
        scope.resource.as_ref().ok_or("birth resource absent")?,
    ));

    let wait = crate::CancellationToken::new();
    wait.cancel();
    let bytes = watch.logical_owned_bytes();
    let native_bytes = watch.platform_owned_bytes();
    original.with_original_leaf(|ledger| {
        let prefix = ledger.receipt();
        assert!(matches!(watch.stop_with_cleanup_wait(&wait), Err(DemandError::Cancelled)));
        assert_eq!(ledger.receipt(), prefix, "pre-cancel does not spend the birth cleanup suffix");
    });
    assert!(!watch.is_stopped());
    assert_eq!(watch.logical_owned_bytes(), bytes);

    // Pre-cancel has not stopped the actual producer or replaced its callback
    // context. Fence and callback work still borrow the very same original.
    std::fs::write(directory.path().join("after-precancel"), b"real notification")?;
    watch.fence_with_work(std::time::Duration::from_secs(10), &mut work)?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut changes = recorded.take();
    while !Recorded::reported(&changes, Path::new("after-precancel"))
        && std::time::Instant::now() < deadline
    {
        std::thread::sleep(std::time::Duration::from_millis(10));
        changes.extend(recorded.take());
    }
    assert!(Recorded::reported(&changes, Path::new("after-precancel")), "{changes:?}");
    cancellation.cancel();
    drop(work);
    drop(original);
    drop(scope);
    drop(root);
    assert!(meter.upgrade().is_some(), "the real native owner retains its birth ledger");
    assert!(resource.upgrade().is_some(), "the real native owner retains its birth resource");
    assert!(!watch.is_stopped(), "root cancellation is not physical watch retirement");

    cleanup_original_watch(&watch, &crate::CancellationToken::new())?;
    assert!(watch.is_stopped());
    assert!(!watch.is_exact());
    assert!(watch.platform_owned_bytes() < native_bytes,
        "terminal retirement releases native variable payload, not the borrowed control Arc");
    let terminal = birth.state.with_original_leaf(crate::demand::DemandWork::receipt);
    terminal.verify(budget)?;
    let callbacks = recorded.take();
    watch.stop_with_cleanup_wait(&crate::CancellationToken::new())?;
    assert_eq!(birth.state.with_original_leaf(crate::demand::DemandWork::receipt), terminal);
    std::fs::write(directory.path().join("after-terminal"), b"not watched")?;
    assert!(recorded.take().is_empty(), "a terminal watch cannot deliver another callback");
    drop(callbacks);
    drop(watch);
    // Managed-driver retirement need not attest native thread exit. The same
    // original ledger remains alive until its actual final SDK data alias drops.
    let data_drop_deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while meter.upgrade().is_some() && std::time::Instant::now() < data_drop_deadline {
        std::thread::yield_now();
    }
    assert!(meter.upgrade().is_none());
    assert!(resource.upgrade().is_none());
    // This proves watch callbacks/payload retirement, not SourceReader drain.
    Ok(())
}

#[test]
fn accepted_cleanup_wrapper_retires_original_watch_after_observer_and_root_cancel() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    let directory = tempfile::tempdir()?;
    let host = HostRoot::open(directory.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
    let budget = original_watch_budget()?;
    let cancellation = crate::CancellationToken::new();
    let root = crate::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(error, _)| error)?;
    let scope = root.native_volume_work(budget)?;
    let original = scope.clone();
    let resource = Arc::downgrade(scope.resource.as_ref().ok_or("birth resource absent")?);
    let meter = Arc::downgrade(&original.state);
    let watch = NativeSourceWatch::start_with_work(
        &host, Arc::new(Recorded::default()), &mut crate::NativeMaterializationWork::clone(&original),
    )?;
    let observer = crate::CancellationToken::new();
    observer.cancel();
    cancellation.cancel();
    let before = scope.receipt();
    assert!(matches!(watch.stop_with_cleanup_wait(&observer), Err(DemandError::Cancelled)));
    assert_eq!(scope.receipt(), before);
    assert!(!watch.is_stopped());
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        match watch.stop_accepted_cleanup() {
            Err(DemandError::WorkerUnavailable) if std::time::Instant::now() < deadline => {
                std::thread::yield_now();
            }
            result => { result?; break; }
        }
    }
    assert!(watch.is_stopped(), "real callback/native-resource terminal state, not kernel thread exit");
    let completed = scope.receipt();
    assert!(completed.backend_write_operations > before.backend_write_operations);
    completed.verify(budget)?;
    watch.stop_accepted_cleanup()?;
    assert_eq!(scope.receipt(), completed, "idempotence cannot repeat native close attempts");
    drop(original);
    drop(scope);
    drop(root);
    assert!(meter.upgrade().is_some(), "actual watch data still keeps its original ledger alive");
    drop(watch);
    let data_drop_deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while meter.upgrade().is_some() && std::time::Instant::now() < data_drop_deadline {
        std::thread::yield_now();
    }
    assert!(meter.upgrade().is_none());
    assert!(resource.upgrade().is_none());
    Ok(())
}

#[test]
fn cleanup_wait_original_start_precancel_and_finite_io_denial_are_prebirth() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    let directory = tempfile::tempdir()?;
    let host = HostRoot::open(directory.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
    let budget = original_watch_budget()?;
    let cancellation = crate::CancellationToken::new();
    let root = crate::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(error, _)| error)?;
    let scope = root.native_volume_work(budget)?;
    cancellation.cancel();
    let before = root.receipt();
    assert!(matches!(NativeSourceWatch::start_with_work(
        &host, Arc::new(Recorded::default()),
        &mut crate::NativeMaterializationWork::clone(&scope),
    ), Err(DemandError::Cancelled)));
    assert_eq!(root.receipt(), before);

    let cancellation = crate::CancellationToken::new();
    let root = crate::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(error, _)| error)?;
    let scope = root.native_volume_work(crate::WorkBudget {
        backend_read_operations: 0, backend_write_operations: 0, ..budget
    })?;
    let before = root.receipt();
    assert!(NativeSourceWatch::start_with_work(
        &host, Arc::new(Recorded::default()),
        &mut crate::NativeMaterializationWork::clone(&scope),
    ).is_err());
    let denied = root.receipt();
    assert_eq!(denied.backend_read_operations, before.backend_read_operations);
    assert_eq!(denied.backend_write_operations, before.backend_write_operations);
    Ok(())
}
