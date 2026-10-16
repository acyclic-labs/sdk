use super::*;

fn notify(runtime: &Runtime, kind: PRJ_NOTIFICATION) -> HRESULT {
    let name = HSTRING::from("read.txt");
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

fn close_read(runtime: &Runtime, work: &crate::NativeMaterializationWork) {
    let before = work.receipt().backend_read_operations;
    assert_eq!(notify(runtime, PRJ_NOTIFICATION_FILE_OPENED), HR_OK);
    assert_eq!(
        notify(runtime, PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION),
        HR_OK
    );
    assert_eq!(work.receipt().backend_read_operations - before, 2);
    assert!(lock_recover(&runtime.metadata_baselines).is_empty());
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
