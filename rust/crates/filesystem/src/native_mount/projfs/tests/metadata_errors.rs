use super::*;

fn notify_handle(runtime: &Runtime, name: &str, kind: PRJ_NOTIFICATION) -> HRESULT {
    let name = HSTRING::from(name);
    let data = PRJ_CALLBACK_DATA {
        InstanceContext: std::ptr::from_ref(runtime).cast_mut().cast(),
        FilePathName: PCWSTR(name.as_ptr()),
        FileId: GUID::from_u128(1),
        ..Default::default()
    };
    // SAFETY: callback data, runtime and name remain live until the return.
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
async fn delete_close_cannot_remove_a_rebound_source() -> Result<(), Box<dyn std::error::Error>> {
    for (directory, opened_before_rebind) in
        [(false, true), (true, true), (false, false), (true, false)]
    {
        let work = setup_work()?;
        let source = windows_checkout_source().await?;
        let path = windows_path("held.txt");
        source.create_file(&path, FileMetadata::default(), None, &work)?;
        let original = source.lookup(&path)?.ok_or("original source missing")?;
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join("held.txt"), b"old host incarnation")?;
        let runtime = callback_runtime(Arc::clone(&source), root.path(), &work)?;
        let (lookup, pin) = source.lookup_pinned(&path)?.ok_or("original pin missing")?;
        let host = host_windows_metadata(&runtime.metadata_root, &path, &work, None)?;
        runtime.projector.written(
            path.clone(),
            EntryFacts { lookup, pin },
            Some(Owned::File(host)),
            ReadBasis::sample(source.as_ref()),
        );
        let name = HSTRING::from("held.txt");
        let data = PRJ_CALLBACK_DATA {
            InstanceContext: std::ptr::from_ref(&runtime).cast_mut().cast(),
            FilePathName: PCWSTR(name.as_ptr()),
            FileId: GUID::from_u128(1),
            ..Default::default()
        };
        let notify = |kind| {
            // SAFETY: the runtime and callback names remain live for this call.
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
        if opened_before_rebind {
            assert_eq!(notify(PRJ_NOTIFICATION_FILE_OPENED), HR_OK);
            assert_eq!(notify(PRJ_NOTIFICATION_PRE_DELETE), HR_OK);
        }
        source.remove(&path, Some(original.node.file_id), &work)?;
        if directory {
            source.create_directory(&path, FileMetadata::default(), &work)?;
        } else {
            source.create_file(&path, FileMetadata::default(), None, &work)?;
            source.write_range(&path, 0, Bytes::from_static(b"replacement"))?;
        }
        let replacement = source.lookup(&path)?.ok_or("replacement source missing")?;
        assert_ne!(original.node.file_id, replacement.node.file_id);
        if !opened_before_rebind {
            assert_eq!(notify(PRJ_NOTIFICATION_FILE_OPENED), HR_OK);
        }
        // A second handle must retain the first handle's incarnation, even
        // when the source now contains a different node at this name.
        assert_eq!(notify(PRJ_NOTIFICATION_FILE_OPENED), HR_OK);
        assert_eq!(notify(PRJ_NOTIFICATION_PRE_DELETE), HR_OK);
        defer_host_capture(runtime.post_operation_failure.as_ref(), path.clone());
        std::fs::remove_file(root.path().join("held.txt"))?;
        assert_eq!(
            notify(PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED),
            HR_OK
        );
        assert_eq!(
            notify(PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED),
            HR_OK
        );
        assert_eq!(
            source.lookup(&path)?.map(|entry| entry.node.file_id),
            Some(replacement.node.file_id),
            "the old host's close must retain the replacement, directory={directory}"
        );
        assert!(lock_recover(&runtime.metadata_baselines).is_empty());
        assert!(has_pending_capture(
            runtime.post_operation_failure.as_ref(),
            &path
        ));
        flush_callback_gate(
            &runtime.callbacks,
            &runtime.post_operation_failure,
            |paths| {
                assert_eq!(
                    paths,
                    &[(path.clone(), false)],
                    "the new capture retains its owner"
                );
                Ok(())
            },
        )?;
        assert!(!has_pending_capture(
            runtime.post_operation_failure.as_ref(),
            &path
        ));
    }
    Ok(())
}

#[tokio::test]
async fn delete_admission_releases_custody_on_every_close_kind()
-> Result<(), Box<dyn std::error::Error>> {
    for kind in [
        PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION,
        PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED,
    ] {
        let work = setup_work()?;
        let source = windows_checkout_source().await?;
        let path = windows_path("cancelled.txt");
        source.create_file(&path, FileMetadata::default(), None, &work)?;
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join("cancelled.txt"), b"retained")?;
        let runtime = callback_runtime(Arc::clone(&source), root.path(), &work)?;
        // Keep the original reader registry alive across every callback. An
        // unrelated unmetered lookup can replace that backing and refund its
        // old owner; that is independent of the handle admission being tested.
        let registry = source.acquire_binding_lease_with_meter(None, &work)?;
        let before = work.remaining()?.peak_allocation_bytes;
        for _ in 0..4 {
            assert_eq!(
                notify_handle(&runtime, "cancelled.txt", PRJ_NOTIFICATION_PRE_DELETE),
                HR_OK
            );
            assert!(
                work.remaining()?.peak_allocation_bytes < before,
                "admission owns retained custody"
            );
            // The host can cancel deletion and close either modified or clean.
            assert_eq!(notify_handle(&runtime, "cancelled.txt", kind), HR_OK);
            assert!(lock_recover(&runtime.metadata_baselines).is_empty());
            assert_eq!(
                work.remaining()?.peak_allocation_bytes,
                before,
                "{kind:?} must retire its admission"
            );
            assert!(source.lookup_with_meter(&path, &work)?.is_some());
        }
        drop(registry);
    }
    Ok(())
}

#[tokio::test]
async fn delete_of_an_external_replacement_uses_its_own_source_identity()
-> Result<(), Box<dyn std::error::Error>> {
    for opened in [false, true] {
        let work = setup_work()?;
        let source = windows_checkout_source().await?;
        let path = windows_path("replaced.txt");
        source.create_file(&path, FileMetadata::default(), None, &work)?;
        let root = tempfile::tempdir()?;
        let host_path = root.path().join("replaced.txt");
        std::fs::write(&host_path, b"original")?;
        let runtime = callback_runtime(Arc::clone(&source), root.path(), &work)?;
        let host = host_windows_metadata(&runtime.metadata_root, &path, &work, None)?;
        let (lookup, pin) = source.lookup_pinned(&path)?.ok_or("original missing")?;
        runtime.projector.written(
            path.clone(),
            EntryFacts { lookup, pin },
            Some(Owned::File(host)),
            ReadBasis::sample(source.as_ref()),
        );
        // Retain the old inode at another name so object-ID reuse cannot make
        // this a timing-dependent replacement test.
        std::fs::rename(&host_path, root.path().join("retained-original"))?;
        std::fs::write(&host_path, b"replacement")?;
        source.remove(&path, None, &work)?;
        source.create_file(&path, FileMetadata::default(), None, &work)?;
        let replacement = source.lookup(&path)?.ok_or("replacement missing")?;
        assert_ne!(replacement.node.file_id, lookup.node.file_id);
        assert!(
            host_windows_metadata(&runtime.metadata_root, &path, &work, None)?.identity
                != host.identity
        );
        if opened {
            assert_eq!(
                notify_handle(&runtime, "replaced.txt", PRJ_NOTIFICATION_FILE_OPENED),
                HR_OK
            );
        }
        assert_eq!(
            notify_handle(&runtime, "replaced.txt", PRJ_NOTIFICATION_PRE_DELETE),
            HR_OK
        );
        std::fs::remove_file(&host_path)?;
        assert_eq!(
            notify_handle(
                &runtime,
                "replaced.txt",
                PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED
            ),
            HR_OK
        );
        assert!(
            source.lookup(&path)?.is_none(),
            "the replacement's deletion must use its own binding; opened={opened}"
        );
        assert!(lock_recover(&runtime.metadata_baselines).is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn delete_close_removes_its_original_source_binding() -> Result<(), Box<dyn std::error::Error>>
{
    let work = setup_work()?;
    let source = windows_checkout_source().await?;
    let path = windows_path("removed.txt");
    source.create_file(&path, FileMetadata::default(), None, &work)?;
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("removed.txt"), b"host")?;
    let runtime = callback_runtime(Arc::clone(&source), root.path(), &work)?;
    let name = HSTRING::from("removed.txt");
    let data = PRJ_CALLBACK_DATA {
        InstanceContext: std::ptr::from_ref(&runtime).cast_mut().cast(),
        FilePathName: PCWSTR(name.as_ptr()),
        FileId: GUID::from_u128(1),
        ..Default::default()
    };
    for kind in [
        PRJ_NOTIFICATION_PRE_DELETE,
        PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED,
    ] {
        if kind == PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED {
            std::fs::remove_file(root.path().join("removed.txt"))?;
        }
        // SAFETY: the runtime and names outlive both synchronous callbacks.
        let status = unsafe {
            notification(
                &raw const data,
                false,
                kind,
                PCWSTR::null(),
                std::ptr::null_mut(),
            )
        };
        assert_eq!(status, HR_OK);
    }
    assert!(source.lookup(&path)?.is_none());
    assert!(lock_recover(&runtime.metadata_baselines).is_empty());
    Ok(())
}

#[tokio::test]
async fn delete_admission_without_open_fences_later_source_incarnations()
-> Result<(), Box<dyn std::error::Error>> {
    for initially_present in [false, true] {
        let work = setup_work()?;
        let source = windows_checkout_source().await?;
        let path = windows_path("admitted.txt");
        if initially_present {
            source.create_file(&path, FileMetadata::default(), None, &work)?;
        }
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join("admitted.txt"), b"old host")?;
        let runtime = callback_runtime(Arc::clone(&source), root.path(), &work)?;
        let name = HSTRING::from("admitted.txt");
        let data = PRJ_CALLBACK_DATA {
            InstanceContext: std::ptr::from_ref(&runtime).cast_mut().cast(),
            FilePathName: PCWSTR(name.as_ptr()),
            FileId: GUID::from_u128(1),
            ..Default::default()
        };
        let notify = |kind| {
            // SAFETY: the runtime, callback data and name outlive this call.
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
        assert_eq!(notify(PRJ_NOTIFICATION_PRE_DELETE), HR_OK);
        if initially_present {
            source.remove(&path, None, &work)?;
        }
        source.create_file(&path, FileMetadata::default(), None, &work)?;
        let replacement = source.lookup(&path)?.ok_or("replacement missing")?;
        // Repeated admission for the same outstanding operation cannot move
        // its token onto the replacement, even without an open notification.
        assert_eq!(notify(PRJ_NOTIFICATION_PRE_DELETE), HR_OK);
        std::fs::remove_file(root.path().join("admitted.txt"))?;
        assert_eq!(
            notify(PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED),
            HR_OK
        );
        assert_eq!(
            source.lookup(&path)?.map(|entry| entry.node.file_id),
            Some(replacement.node.file_id)
        );
        assert!(lock_recover(&runtime.metadata_baselines).is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn delete_close_without_an_open_binding_cannot_mutate_the_source()
-> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    let source = windows_checkout_source().await?;
    let path = windows_path("unbound.txt");
    source.create_file(&path, FileMetadata::default(), None, &work)?;
    let original = source.lookup(&path)?.ok_or("source missing")?;
    let root = tempfile::tempdir()?;
    let runtime = callback_runtime(Arc::clone(&source), root.path(), &work)?;
    let name = HSTRING::from("unbound.txt");
    let data = PRJ_CALLBACK_DATA {
        InstanceContext: std::ptr::from_ref(&runtime).cast_mut().cast(),
        FilePathName: PCWSTR(name.as_ptr()),
        FileId: GUID::from_u128(1),
        ..Default::default()
    };
    // SAFETY: the runtime and name remain live for this synchronous callback.
    let status = unsafe {
        notification(
            &raw const data,
            false,
            PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED,
            PCWSTR::null(),
            std::ptr::null_mut(),
        )
    };
    assert!(status.is_err());
    assert_eq!(
        source.lookup(&path)?.map(|entry| entry.node.file_id),
        Some(original.node.file_id)
    );
    assert!(
        flush_callback_gate(
            &runtime.callbacks,
            &runtime.post_operation_failure,
            |paths| {
                assert!(
                    paths.is_empty(),
                    "an unbound delete cannot be replayed against a newer entry"
                );
                Ok(())
            }
        )
        .is_err()
    );
    Ok(())
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
