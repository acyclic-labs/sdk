use super::*;
use windows::Win32::Storage::ProjectedFileSystem::PRJ_NOTIFICATION_CB;

fn notify(runtime: &Runtime, kind: PRJ_NOTIFICATION) -> HRESULT {
    notify_path(runtime, kind, "read.txt")
}

fn notify_path(runtime: &Runtime, kind: PRJ_NOTIFICATION, path: &str) -> HRESULT {
    let name = HSTRING::from(path);
    let data = PRJ_CALLBACK_DATA {
        InstanceContext: std::ptr::from_ref(runtime).cast_mut().cast(),
        FilePathName: PCWSTR(name.as_ptr()),
        FileId: GUID::from_u128(1),
        ..Default::default()
    };
    // SAFETY: the callback data, name and runtime stay live for this call.
    unsafe {
        notification(
            &raw const data,
            false,
            kind,
            PCWSTR::null(),
            std::ptr::null_mut(),
        )
    }
}

#[tokio::test]
async fn native_access_projfs_cached_opens_require_current_list_authority()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::access::Action;
    use crate::native_mount::adapter::tests::access::{deny, publish_test_access};
    for path in ["read.txt", ""] {
        let work = setup_work()?;
        let source = windows_checkout_source().await?;
        source.create_file(
            &windows_path("read.txt"),
            FileMetadata::default(),
            None,
            &work,
        )?;
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join("read.txt"), b"cached host contents")?;
        let runtime = metadata_errors::callback_runtime(Arc::clone(&source), root.path(), &work)?;
        let stamp = source.view_stamp_with_meter(&work)?;
        publish_test_access(&source, vec![deny("no-disclosure", &[Action::List], path)])?;
        assert_eq!(source.view_stamp_with_meter(&work)?, stamp);
        let before = work.receipt().backend_read_operations;
        assert_eq!(
            notify_path(&runtime, PRJ_NOTIFICATION_FILE_OPENED, path),
            HR_ACCESS_DENIED
        );
        assert_eq!(
            work.receipt().backend_read_operations,
            before,
            "denial precedes host probes"
        );
        assert!(lock_recover(&runtime.metadata_baselines).is_empty());
    }
    Ok(())
}

fn close_read(runtime: &Runtime, work: &crate::NativeMaterializationWork) {
    // Persisted policy validation is storage I/O, distinct from the two native
    // metadata probes this regression checks. Measure the shared authorization
    // cost rather than hiding it in an increased host-probe allowance.
    let before = work.receipt().backend_read_operations;
    assert!(
        authorize_open(
            runtime.source.as_ref(),
            &windows_path("read.txt"),
            false,
            work
        )
        .is_ok()
    );
    let authorization_reads = work.receipt().backend_read_operations - before;
    let before = work.receipt().backend_read_operations;
    assert_eq!(notify(runtime, PRJ_NOTIFICATION_FILE_OPENED), HR_OK);
    assert_eq!(
        notify(runtime, PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION),
        HR_OK
    );
    assert_eq!(
        work.receipt().backend_read_operations - before - authorization_reads,
        2
    );
    assert!(lock_recover(&runtime.metadata_baselines).is_empty());
}

#[derive(Default)]
struct ReplayState {
    callback: PRJ_NOTIFICATION_CB,
    noop: bool,
    pending: Option<(Arc<Runtime>, bool)>,
}

thread_local! {
    static REPLAY: std::cell::RefCell<ReplayState> = Default::default();
}

pub(crate) fn registered_callbacks(mut callbacks: PRJ_CALLBACKS) -> PRJ_CALLBACKS {
    REPLAY.with_borrow_mut(|state| {
        if state.noop {
            callbacks.NotificationCallback = Some(noop_notification);
        }
        state.callback = callbacks.NotificationCallback;
    });
    callbacks
}

unsafe extern "system" fn noop_notification(
    _data: *const PRJ_CALLBACK_DATA,
    _directory: bool,
    _kind: PRJ_NOTIFICATION,
    _destination: PCWSTR,
    _parameters: *mut PRJ_NOTIFICATION_PARAMETERS,
) -> HRESULT {
    HR_OK
}

pub(crate) fn bypass_probe_registration() -> bool {
    REPLAY.with_borrow(|state| state.pending.as_ref().is_some_and(|(_, bypass)| *bypass))
}

pub(crate) struct FallbackReplay(Arc<Runtime>);

pub(crate) fn replay_fallback() -> Option<FallbackReplay> {
    // Release the thread-local borrow before invoking a callback: the real
    // entry point can itself probe metadata on the provider fiber.
    let pending = REPLAY.with_borrow_mut(|state| state.pending.take());
    pending.map(|(runtime, _)| {
        notify_registered(&runtime, PRJ_NOTIFICATION_FILE_OPENED);
        FallbackReplay(runtime)
    })
}

impl Drop for FallbackReplay {
    fn drop(&mut self) {
        notify_registered(&self.0, PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION);
    }
}

fn notify_registered(runtime: &Runtime, kind: PRJ_NOTIFICATION) {
    let callback = REPLAY.with_borrow(|state| state.callback.expect("mounted callback"));
    let name = HSTRING::from("nested\\read.txt");
    let data = PRJ_CALLBACK_DATA {
        InstanceContext: std::ptr::from_ref(runtime).cast_mut().cast(),
        FilePathName: PCWSTR(name.as_ptr()),
        FileId: GUID::from_u128(1),
        TriggeringProcessId: std::process::id(),
        ..Default::default()
    };
    // SAFETY: this is the callback table passed to PrjStartVirtualizing; the
    // mounted runtime, callback data and UTF-16 name remain live for the call.
    let result = unsafe {
        callback(
            &raw const data,
            false,
            kind,
            PCWSTR::null(),
            std::ptr::null_mut(),
        )
    };
    assert_eq!(result, HR_OK);
}

struct ResetReplay;

impl Drop for ResetReplay {
    fn drop(&mut self) {
        REPLAY.with_borrow_mut(|state| *state = ReplayState::default());
    }
}

async fn mounted_replay(noop: bool, bypass: bool) -> Result<[bool; 3], Box<dyn std::error::Error>> {
    let _reset = ResetReplay;
    let work = setup_work()?;
    let source = windows_checkout_source().await?;
    let parent = windows_path("nested");
    let path = parent.child(windows_name("read.txt"));
    source.create_directory(&parent, FileMetadata::default(), &work)?;
    source.create_file(&path, FileMetadata::default(), None, &work)?;
    source.write_range(&path, 0, Bytes::from_static(b"contents"))?;
    REPLAY.with_borrow_mut(|state| state.noop = noop);
    let (_root, destination, mut session) = mount_source(&source)?;
    assert_eq!(
        std::fs::read(destination.join("nested/read.txt"))?,
        b"contents"
    );
    session.flush_callbacks(&work)?;
    let runtime = Arc::clone(
        session
            .driver
            .as_ref()
            .and_then(|driver| driver.runtime.as_ref())
            .ok_or("mount has no runtime")?,
    );
    let mut observed = [false; 3];
    for (index, suppress) in [true, false, true].into_iter().enumerate() {
        lock_recover(&runtime.projection).remove(&path);
        REPLAY.with_borrow_mut(|state| {
            state.pending = Some((Arc::clone(&runtime), bypass || !suppress));
        });
        probe_host_windows_metadata(
            &runtime.metadata_root,
            &path,
            &runtime.metadata_probes,
            &work,
        )?;
        assert!(
            REPLAY.with_borrow(|state| state.pending.is_none()),
            "production fallback was not replayed"
        );
        runtime.callbacks.drain();
        // Only ordinary close inserts this exact leaf binding; parent callbacks
        // cannot satisfy this observation.
        observed[index] = lock_recover(&runtime.projection).contains_key(&path);
        assert!(!has_pending_capture(&runtime.post_operation_failure, &path));
        assert!(lock_recover(&runtime.metadata_probes).is_empty());
        assert!(lock_recover(&runtime.metadata_baselines).is_empty());
    }
    drop(runtime);
    session.stop()?;
    Ok(observed)
}

#[tokio::test]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn mounted_nested_fallback_negative_control_replays_self_notifications()
-> Result<(), Box<dyn std::error::Error>> {
    let expected = [false, true, false];
    assert_eq!(mounted_replay(false, false).await?, expected);
    // Both deliberately broken production seams must fail the same oracle.
    let callback_mutant = mounted_replay(true, false).await?;
    assert_eq!(callback_mutant, [false; 3]);
    assert_ne!(
        callback_mutant, expected,
        "no-op registered callback escaped detection"
    );
    let guard_mutant = mounted_replay(false, true).await?;
    assert_eq!(guard_mutant, [true; 3]);
    assert_ne!(
        guard_mutant, expected,
        "bypassed production registration escaped detection"
    );
    Ok(())
}

#[tokio::test]
async fn cached_read_close_rechecks_source_changes_and_keeps_both_host_probes()
-> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    let source = windows_checkout_source().await?;
    let path = windows_path("read.txt");
    source.create_file(&path, FileMetadata::default(), None, &work)?;
    source.write_range(&path, 0, Bytes::from_static(b"contents"))?;
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("read.txt"), b"contents")?;
    let runtime = metadata_errors::callback_runtime(Arc::clone(&source), root.path(), &work)?;
    close_read(&runtime, &work);
    let binding = lock_recover(&runtime.projection)
        .get(&path)
        .copied()
        .ok_or("read did not retain its source binding")?;
    for _ in 0..2 {
        close_read(&runtime, &work);
        assert_eq!(
            lock_recover(&runtime.projection)
                .get(&path)
                .map(|b| b.basis),
            Some(binding.basis)
        );
        assert!(!has_pending_capture(&runtime.post_operation_failure, &path));
    }
    source.create_file(&windows_path("other"), FileMetadata::default(), None, &work)?;
    assert_ne!(ReadBasis::sample(source.as_ref()), Some(binding.basis));
    close_read(&runtime, &work);
    assert!(!has_pending_capture(&runtime.post_operation_failure, &path));
    assert_eq!(
        lock_recover(&runtime.projection)
            .get(&path)
            .map(|b| b.basis),
        Some(binding.basis),
        "an unrelated source change should keep the original valid binding"
    );
    source.remove(&path, None, &work)?;
    close_read(&runtime, &work);
    assert!(!lock_recover(&runtime.projection).contains_key(&path));
    assert!(has_pending_capture(&runtime.post_operation_failure, &path));
    assert!(source.lookup(&path)?.is_none());
    Ok(())
}

#[tokio::test]
async fn a_current_source_binding_still_captures_a_host_metadata_edit()
-> Result<(), Box<dyn std::error::Error>> {
    use std::os::windows::fs::OpenOptionsExt as _;

    let work = setup_work()?;
    let source = windows_checkout_source().await?;
    let path = windows_path("read.txt");
    source.create_file(&path, FileMetadata::default(), None, &work)?;
    source.write_range(&path, 0, Bytes::from_static(b"contents"))?;
    let root = tempfile::tempdir()?;
    let host = root.path().join("read.txt");
    std::fs::write(&host, b"contents")?;
    let runtime = metadata_errors::callback_runtime(Arc::clone(&source), root.path(), &work)?;
    close_read(&runtime, &work);
    let basis = ReadBasis::sample(source.as_ref()).ok_or("source is unstable")?;
    assert_eq!(notify(&runtime, PRJ_NOTIFICATION_FILE_OPENED), HR_OK);
    std::fs::OpenOptions::new()
        .access_mode(windows::Win32::Storage::FileSystem::FILE_WRITE_ATTRIBUTES.0)
        .open(&host)?
        .set_modified(
            std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(978_307_200),
        )?;
    assert_eq!(ReadBasis::sample(source.as_ref()), Some(basis));
    assert_eq!(
        notify(
            &runtime,
            PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION
        ),
        HR_OK
    );
    let captured = source.lookup(&path)?.ok_or("source file disappeared")?;
    assert_eq!(
        captured.metadata.modified_ns,
        crate::kernel::MetadataField::Value(978_307_200_000_000_000)
    );
    assert_eq!(source.read_range(&path, 0, 8)?.as_ref(), b"contents");
    assert!(!has_pending_capture(&runtime.post_operation_failure, &path));
    Ok(())
}

#[tokio::test]
async fn cached_read_close_charges_source_validation_to_its_callback_meter()
-> Result<(), Box<dyn std::error::Error>> {
    use proptest::prelude::*;
    use proptest::test_runner::TestRunner;

    let setup = setup_work()?;
    let source = windows_checkout_source().await?;
    let path = windows_path("read.txt");
    source.create_file(&path, FileMetadata::default(), None, &setup)?;
    source.write_range(&path, 0, Bytes::from_static(b"contents"))?;
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("read.txt"), b"contents")?;
    let warm = metadata_errors::callback_runtime(Arc::clone(&source), root.path(), &setup)?;
    close_read(&warm, &setup);
    let binding = lock_recover(&warm.projection)
        .get(&path)
        .copied()
        .ok_or("no cached binding")?;
    let before = setup.receipt();
    host_windows_metadata(&warm.metadata_root, &path, &setup, None)?;
    let probe_items = setup.receipt().items_examined - before.items_examined;
    let limits = prop_oneof![Just(probe_items), 0_u64..128, any::<u64>()];
    let mut runner = TestRunner::new(ProptestConfig {
        cases: 32,
        source_file: Some(file!()),
        ..ProptestConfig::default()
    });
    runner.run(&limits, |items_examined| {
        let mut runtime =
            metadata_errors::callback_runtime(Arc::clone(&source), root.path(), &setup)
                .map_err(|e| TestCaseError::fail(e.to_string()))?;
        lock_recover(&runtime.projection).insert(path.clone(), binding);
        prop_assert_eq!(notify(&runtime, PRJ_NOTIFICATION_FILE_OPENED), HR_OK);
        let work = crate::NativeMaterializationWork::new(
            crate::WorkBudget {
                items_examined,
                backend_read_operations: 1,
                ..crate::WorkBudget::UNBOUNDED
            },
            &crate::CancellationToken::new(),
        )
        .map_err(|(e, _)| TestCaseError::fail(e.to_string()))?;
        runtime.work = Mutex::new(work.clone());
        let result = notify(
            &runtime,
            PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION,
        );
        let receipt = work.receipt();
        prop_assert!(receipt.items_examined <= items_examined);
        prop_assert!(receipt.backend_read_operations <= 1);
        if result.is_ok() {
            prop_assert_eq!(receipt.backend_read_operations, 1);
            prop_assert!(
                receipt.items_examined > probe_items,
                "a successful close must charge source validation after its host probe"
            );
        }
        if items_examined == probe_items {
            prop_assert!(
                result.is_err(),
                "the host-only budget cannot also fund source validation"
            );
        }
        prop_assert!(lock_recover(&runtime.metadata_baselines).is_empty());
        prop_assert!(!has_pending_capture(&runtime.post_operation_failure, &path));
        Ok(())
    })?;
    assert_eq!(source.read_range(&path, 0, 8)?.as_ref(), b"contents");
    Ok(())
}
