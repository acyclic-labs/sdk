use super::*;

pub(super) fn callback_runtime(
    source: Arc<MemorySource>,
    root: &std::path::Path,
    work: &crate::NativeMaterializationWork,
) -> Result<Runtime, Box<dyn std::error::Error>> {
    Ok(Runtime {
        source,
        work: Mutex::new(work.clone()),
        active_work: Mutex::new(Default::default()),
        original_work: Mutex::new(None),
        callback_guard: None,
        root: root.to_path_buf(),
        metadata_root: HostRoot::open(root, &mut &*work)?,
        staging: Mutex::new(None),
        creating_name: RwLock::new((MountPath::root(), None)),
        writable: true,
        metadata_baselines: Mutex::new(HashMap::new()),
        projection: Mutex::new(HashMap::new()),
        projector: Arc::new(Projector::new()),
        metadata_probes: Mutex::new(HashMap::new()),
        metadata_edits: FileLocks::default(),
        post_operation_failure: Arc::new(Mutex::new(PostOperationFailures::default())),
        callbacks: CallbackGate::start()?,
        _allocation: work.claim(0, 0, crate::WorkCounters::UNCHARGED)?,
    })
}

#[tokio::test]
async fn read_close_after_rename_does_not_block_publication()
-> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    let source = windows_checkout_source().await?;
    let path = windows_path("read.txt");
    source.create_file(&path, FileMetadata::default(), None, &work)?;
    source.write_range(&path, 0, Bytes::from_static(b"source contents"))?;
    let directory = tempfile::tempdir()?;
    std::fs::write(directory.path().join("read.txt"), b"host contents")?;
    let runtime = callback_runtime(Arc::clone(&source), directory.path(), &work)?;
    let name = HSTRING::from("read.txt");
    let data = PRJ_CALLBACK_DATA {
        InstanceContext: std::ptr::from_ref(&runtime).cast_mut().cast(),
        FilePathName: PCWSTR(name.as_ptr()),
        FileId: GUID::from_u128(1),
        ..Default::default()
    };
    let notify = |kind| {
        // SAFETY: the callback data, name and runtime stay live throughout each call.
        unsafe {
            notification(
                &raw const data,
                false,
                kind,
                PCWSTR::null(),
                std::ptr::null_mut(),
            )
        }
    };
    assert_eq!(notify(PRJ_NOTIFICATION_FILE_OPENED), HR_OK);
    std::fs::rename(
        directory.path().join("read.txt"),
        directory.path().join("moved.txt"),
    )?;
    assert_eq!(
        notify(PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION),
        HR_IO_DEVICE
    );
    for _ in 0..2 {
        flush_callback_gate(
            &runtime.callbacks,
            &runtime.post_operation_failure,
            |paths| {
                assert!(
                    paths.is_empty(),
                    "a failed read probe must not recapture contents"
                );
                Ok(())
            },
        )?;
    }
    assert!(lock_recover(&runtime.metadata_baselines).is_empty());
    assert_eq!(
        source.read_range(&path, 0, 15)?.as_ref(),
        b"source contents"
    );
    Ok(())
}

#[test]
fn metadata_close_failure_blocks_publication_without_recapturing_contents()
-> Result<(), Box<dyn std::error::Error>> {
    let failures = Mutex::new(PostOperationFailures::default());
    let callbacks = CallbackGate::start()?;
    let path = windows_path("changed.txt");
    record_post_operation_failure(
        &failures,
        PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION,
        &path,
        None,
        &Err(MountSourceError::Stale),
    );
    flush_callback_gate(&callbacks, &failures, |paths| {
        assert!(paths.is_empty());
        Ok(())
    })?;
    record_post_operation_failure(
        &failures,
        PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION,
        &path,
        Some(&path),
        &Err(MountSourceError::Stale),
    );
    for _ in 0..2 {
        assert!(
            flush_callback_gate(&callbacks, &failures, |paths| {
                assert!(
                    paths.is_empty(),
                    "metadata errors must not recapture file contents"
                );
                Ok(())
            })
            .is_err()
        );
        assert!(lock_recover(&failures).unreplayable.is_some());
        assert!(lock_recover(&failures).pending_captures.is_empty());
    }
    Ok(())
}
