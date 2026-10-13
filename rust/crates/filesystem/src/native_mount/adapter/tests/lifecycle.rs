#[test]
fn a_detached_write_that_cannot_stamp_changes_nothing() -> Result<(), Box<dyn std::error::Error>> {
    let objects = Arc::new(MetadataRefusingObjects::default());
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        Arc::clone(&objects),
        crate::EmbeddedCapabilities::MEMORY,
    );
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let checkout = runtime.block_on(async {
        let cancellation = CancellationToken::new();
        let volume = fs
            .create_volume(config, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value;
        volume
            .checkout(
                GenerationSelector::Head,
                CheckoutMode {
                    access: AccessMode::ReadWrite,
                    consistency: ConsistencyMode::Pinned,
                    mutations: MutationMode::PrivateOverlay,
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map(|receipt| receipt.value)
    })?;
    let source = CheckoutMountSource::new(Arc::new(SharedCheckout::new(checkout)), config)?;
    let path = native_test_path("stamped.bin");
    let timed = FileMetadata {
        modified_ns: crate::kernel::MetadataField::Value(1),
        changed_ns: crate::kernel::MetadataField::Value(1),
        ..metadata()
    };
    let work = capture_meter(boundary_budget(), source.cancellation())?;
    source.create_file(&path, timed, None, &work)?;
    source.write_range(&path, 0, Bytes::from_static(b"before"))?;
    let detached = source.detach_file(&path)?;
    let before = detached.lookup()?;

    objects.refuse_metadata.store(true, Ordering::Release);
    assert!(
        detached
            .write_range(0, Bytes::from_static(b"after!"))
            .is_err()
    );
    assert!(detached.resize(64).is_err());
    objects.refuse_metadata.store(false, Ordering::Release);

    // Content and stamp are one transition: neither happened, so no
    // cached fact about the file went stale without being invalidated.
    assert_eq!(detached.lookup()?, before);
    assert_eq!(detached.read_range(0, 6)?.as_ref(), b"before");
    detached.write_range(0, Bytes::from_static(b"after!"))?;
    let after = detached.lookup()?;
    assert_eq!(detached.read_range(0, 6)?.as_ref(), b"after!");
    assert_ne!(after.metadata, before.metadata);
    Ok(())
}

type MemorySource =
    CheckoutMountSource<crate::facade::MemoryAuthorityBackend, crate::facade::MemoryObjectBackend>;

#[test]
fn batched_capture_budget_is_cumulative_across_paths() -> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Portable)?;
    let directory = tempfile::tempdir()?;
    for name in ["first", "second"] {
        std::fs::write(directory.path().join(name), b"first")?;
    }
    let paths = ["first", "second"]
        .map(|name| source.path(&native_test_path(name)))
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    let options = CaptureOptions {
        source_root: directory.path().to_path_buf(),
        expected_root_identity: capture_root_identity(directory.path())?,
        maximum_paths: 2,
        maximum_extent_spans: 64,
    };
    let cancellation = CancellationToken::new();
    let mut single_bytes = 0;
    for count in [0, 1, 2] {
        let mut budget = WorkBudget::UNBOUNDED;
        if count == 0 {
            budget.source_entries_visited = 0;
        }
        if count == 2 {
            budget.source_bytes_read = single_bytes;
        }
        let work = capture_meter(budget, &cancellation)?;
        let mut candidate = source.runtime.wait(|| async {
            source
                .checkout
                .lock()
                .await
                .private_candidate(&work, &cancellation)
                .map_err(fs_error)
        })?;
        let result = source.runtime.block_on(
            || {
                capture_paths_batched(
                    &mut candidate,
                    &paths[..count.max(1)],
                    &options,
                    1,
                    &cancellation,
                    None,
                    &work,
                )
            },
            None,
        )?;
        if count == 1 {
            single_bytes = result?.work.source_bytes_read;
            assert!(single_bytes > 0);
        } else {
            let error = result.expect_err("capture ceiling ignored");
            assert!(matches!(
                error.error,
                crate::CaptureError::Kernel(
                    FsError::Work(_)
                        | FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(_))
                )
            ));
            assert!(!candidate.has_pending_mutations());
            if count == 0 {
                assert_eq!(work.receipt().source_entries_visited, 0);
            }
        }
    }
    Ok(())
}

#[test]
fn publication_policy_and_subtree_cases() -> Result<(), Box<dyn std::error::Error>> {
    let native = if cfg!(windows) {
        FilesystemProfile::Windows
    } else {
        FilesystemProfile::Posix
    };
    for (profile, policy, publishes) in [
        (native, MountPublication::Manual, false),
        (native, MountPublication::PerMutation, true),
        (
            FilesystemProfile::Portable,
            MountPublication::PerMutation,
            true,
        ),
    ] {
        let (source, _) = sources(profile, ConsistencyMode::Pinned, policy)?;
        let work = capture_meter(boundary_budget(), source.cancellation())?;
        let before = checkout_state(&source).0;
        let directory = native_test_path("src");
        if profile == native && publishes {
            source.create_file(&native_test_path("published.bin"), metadata(), None, &work)?;
        }
        let created = source.create_directory(&directory, metadata(), &work)?;
        assert_eq!(source.lookup(&directory)?, Some(created));
        source.flush(&work)?;
        assert_eq!(checkout_state(&source).0 != before, publishes);
        source.sync()?;
        assert!(!checkout_state(&source).1);
        if !publishes {
            let config = VolumeConfig {
                profile,
                ..VolumeConfig::portable(Lifecycle::Ephemeral)
            };
            let root = source.path(&directory)?;
            let subtree = CheckoutMountSource::new_at(source.checkout.clone(), config, root)?;
            let nested = native_test_path("nested.bin");
            subtree.create_file(&nested, metadata(), None, &work)?;
            assert!(source.lookup(&nested)?.is_none());
            assert!(subtree.lookup(&nested)?.is_some());
            assert_eq!(
                source.lookup(&directory)?.map(|entry| entry.node.kind),
                Some(MountNodeKind::Directory)
            );
        }
    }
    Ok(())
}

#[test]
fn refresh_preserves_overlapping_read_conflicts_and_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let profile = if cfg!(target_os = "windows") {
        FilesystemProfile::Windows
    } else {
        FilesystemProfile::Posix
    };
    let (reader, writer) = sources(
        profile,
        ConsistencyMode::TrackingSafe,
        MountPublication::CloseAndSync,
    )?;
    let path = native_test_path("observed.bin");
    let work = capture_meter(boundary_budget(), writer.cancellation())?;
    writer.create_file(&path, metadata(), None, &work)?;
    writer.write_range(&path, 0, Bytes::from_static(b"first"))?;
    writer.sync()?;
    let binding_before = reader.binding_epoch();
    reader.runtime.wait(|| reader.refresh_async())?;
    let binding_after = reader.binding_epoch();
    assert_ne!(binding_after, binding_before);
    assert_eq!(reader.read_range(&path, 0, 5)?.as_ref(), b"first");
    let observed_generation = checkout_state(&reader).0;

    writer.write_range(&path, 0, Bytes::from_static(b"other"))?;
    writer.sync()?;
    assert!(matches!(
        reader.runtime.wait(|| reader.refresh_async()),
        Err(MountSourceError::Stale)
    ));
    assert_eq!(checkout_state(&reader).0, observed_generation);
    assert_eq!(reader.binding_epoch(), binding_after);
    assert_eq!(reader.read_range(&path, 0, 5)?.as_ref(), b"first");
    Ok(())
}

#[test]
fn open_handle_identity_and_sparse_cases() -> Result<(), Box<dyn std::error::Error>> {
    let profile = if cfg!(windows) {
        FilesystemProfile::Windows
    } else {
        FilesystemProfile::Posix
    };
    for detached in [false, true] {
        let source = source(profile)?;
        let work = capture_meter(boundary_budget(), source.cancellation())?;
        let original = native_test_path("original");
        let alias = native_test_path("alias");
        let renamed = native_test_path("renamed");
        let created = source.create_file(&original, metadata(), None, &work)?;
        source.resize(&original, 64)?;
        source.write_range(&original, 8, Bytes::from_static(b"visible"))?;
        let file = if detached {
            source.detach_file(&original)?
        } else {
            source.open_file(&original)?
        };
        if !detached {
            source.hard_link(&original, &alias, &work)?;
        }
        source.remove(&original, Some(created.node.file_id), &work)?;
        assert!(source.lookup(&original)?.is_none());
        assert_eq!(file.lookup()?.node.link_count, if detached { 0 } else { 1 });
        assert_eq!(file.read_range(8, 7)?.as_ref(), b"visible");
        file.write_range(32, Bytes::from_static(b"open"))?;
        file.allocate_range(16, 8, MountRangeAllocation::Preallocate { keep_size: true })?;
        let changed = FileMetadata {
            modified_ns: crate::kernel::MetadataField::Value(42),
            ..metadata()
        };
        file.set_attributes(changed, Some(40), &work)?;
        assert_eq!(file.lookup()?.metadata, changed);
        assert_eq!(file.lookup()?.node.logical_bytes, 40);
        assert_eq!(file.read_range(32, 4)?.as_ref(), b"open");
        for offset in [40, u64::MAX] {
            assert!(file.read_up_to(offset, 8)?.is_empty());
        }
        assert_eq!(file.seek(0, MountSeekTarget::Data)?, Some(8));
        if !detached {
            assert_eq!(source.read_range(&alias, 32, 4)?.as_ref(), b"open");
            source.rename(&alias, &renamed, false, &work)?;
            file.write_range(0, Bytes::from_static(b"live"))?;
            assert!(source.lookup(&alias)?.is_none());
            assert_eq!(source.read_range(&renamed, 0, 4)?.as_ref(), b"live");
            assert_eq!(file.lookup()?.node.file_id, created.node.file_id);
        }
        #[cfg(unix)]
        {
            let name = b"user.acyclic.handle";
            file.write_attribute(
                name,
                crate::kernel::OwnedSourceChunk::claimed(Bytes::from_static(b"retained").into(), 0),
                MountAttributeWriteMode::Create,
                &work,
            )?;
            assert_eq!(
                file.read_attribute(name, &work)?
                    .map(|value| value.bytes)
                    .as_deref(),
                Some(b"retained".as_slice())
            );
            assert_eq!(file.list_attributes(None, 8)?.names, vec![name.to_vec()]);
            if !detached {
                assert_eq!(
                    source
                        .read_attribute(&renamed, name, &work)?
                        .map(|value| value.bytes)
                        .as_deref(),
                    Some(b"retained".as_slice())
                );
            }
            file.remove_attribute(name, &work)?;
            assert!(file.read_attribute(name, &work)?.is_none());
        }
        if detached {
            let from = native_test_path("sparse-source");
            let to = native_test_path("sparse-destination");
            source.create_file(&from, metadata(), None, &work)?;
            source.create_file(&to, metadata(), None, &work)?;
            source.resize(&from, 128)?;
            source.write_range(&from, 64, Bytes::from_static(b"payload"))?;
            source.write_range(&to, 0, Bytes::from_static(b"dense"))?;
            let from = source.detach_file(&from)?;
            let to = source.detach_file(&to)?;
            let range = from.read_sparse_range(0, 128, &work)?;
            to.write_sparse_range(0, range, &work)?;
            assert_eq!(to.lookup()?.node.logical_bytes, 128);
            for (offset, target, expected) in [
                (0, MountSeekTarget::Data, Some(64)),
                (64, MountSeekTarget::Hole, Some(71)),
            ] {
                assert_eq!(to.seek(offset, target)?, expected);
            }
            assert_eq!(to.read_range(64, 7)?.as_ref(), b"payload");
        }
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn host_capture_preserves_unobservable_named_metadata() -> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Posix)?;
    let path = native_test_path("metadata.bin");
    let work = capture_meter(boundary_budget(), source.cancellation())?;
    source.create_file(&path, metadata(), None, &work)?;
    source.write_attribute(
        &path,
        b"user.acyclic.retained",
        crate::kernel::OwnedSourceChunk::intrinsic(OwnedBytes::from_static(b"opaque"), 0),
        MountAttributeWriteMode::Create,
        &work,
    )?;
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("metadata.bin"), b"host")?;
    source.capture_host_path(root.path(), &path)?;
    assert_eq!(
        source
            .read_attribute(&path, b"user.acyclic.retained", &work)?
            .map(|value| value.bytes)
            .as_deref(),
        Some(b"opaque".as_slice())
    );
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn sparse_seek_and_special_nodes_preserve_canonical_native_semantics()
-> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Posix)?;
    let file = native_test_path("sparse");
    let work = capture_meter(boundary_budget(), source.cancellation())?;
    source.create_file(&file, metadata(), None, &work)?;
    source.resize(&file, 100)?;
    source.write_range(&file, 20, Bytes::from_static(b"data"))?;
    assert_eq!(source.seek(&file, 0, MountSeekTarget::Data)?, Some(20));
    assert_eq!(source.seek(&file, 20, MountSeekTarget::Hole)?, Some(24));
    assert_eq!(source.seek(&file, 24, MountSeekTarget::Data)?, None);
    assert_eq!(source.seek(&file, 100, MountSeekTarget::Hole)?, Some(100));

    let fifo = MountPath::root().child(b"fifo".to_vec());
    let created = source.create_special(&fifo, MountNodeKind::Fifo, None, metadata(), &work)?;
    assert_eq!(created.node.kind, MountNodeKind::Fifo);
    let device = MountPath::root().child(b"device".to_vec());
    let created = source.create_special(
        &device,
        MountNodeKind::CharacterDevice,
        Some((12, 34)),
        metadata(),
        &work,
    )?;
    assert_eq!(created.node.device, Some((12, 34)));
    assert_eq!(source.lookup(&device)?, Some(created));
    Ok(())
}

#[cfg(target_os = "windows")]
#[test]
#[ignore = "requires the live Windows ProjFS optional feature"]
fn writable_projfs_captures_closes_renames_links_and_deletes()
-> Result<(), Box<dyn std::error::Error>> {
    let source = Arc::new(source(FilesystemProfile::Windows)?);
    let work = capture_meter(boundary_budget(), source.cancellation())?;
    let temporary = tempfile::tempdir()?;
    let destination = temporary.path().join("projection");
    std::fs::create_dir(&destination)?;
    let mut session = mount_source(&source, &destination)?;

    let first_name = "written-α.bin";
    let second_name = "renamed-β.bin";
    let linked_name = "linked-γ.bin";
    let write_status = acyclic_native_runtime::process_status(std::process::Command::new("powershell.exe")
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "[IO.File]::WriteAllBytes($env:ACYCLIC_FS_TEST_PATH, [Text.Encoding]::UTF8.GetBytes('projected'))",
            ])
            .env("ACYCLIC_FS_TEST_PATH", destination.join(first_name)), std::time::Duration::from_secs(120))?;
    assert!(write_status.success());
    // Captures of external operations are deferred until callbacks flush.
    session.flush_callbacks(&work)?;
    let first = native_test_path(first_name);
    assert_eq!(source.read_range(&first, 0, 9)?.as_ref(), b"projected");

    let rename_status = acyclic_native_runtime::process_status(
        std::process::Command::new("powershell.exe")
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "[IO.File]::Move($env:ACYCLIC_FS_TEST_SOURCE, $env:ACYCLIC_FS_TEST_DESTINATION)",
            ])
            .env("ACYCLIC_FS_TEST_SOURCE", destination.join(first_name))
            .env("ACYCLIC_FS_TEST_DESTINATION", destination.join(second_name)),
        std::time::Duration::from_secs(120),
    )?;
    assert!(rename_status.success());
    session.flush_callbacks(&work)?;
    let second = native_test_path(second_name);
    assert_eq!(source.lookup(&first)?, None);
    assert!(source.lookup(&second)?.is_some());

    let link_status = acyclic_native_runtime::process_status(std::process::Command::new("powershell.exe")
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "New-Item -ItemType HardLink -Path $env:ACYCLIC_FS_TEST_DESTINATION -Target $env:ACYCLIC_FS_TEST_SOURCE | Out-Null",
            ])
            .env("ACYCLIC_FS_TEST_SOURCE", destination.join(second_name))
            .env("ACYCLIC_FS_TEST_DESTINATION", destination.join(linked_name)), std::time::Duration::from_secs(120))?;
    assert!(link_status.success());
    session.flush_callbacks(&work)?;
    let linked = native_test_path(linked_name);
    let second_id = source
        .lookup(&second)?
        .ok_or("renamed source absent")?
        .node
        .file_id;
    assert_eq!(
        source
            .lookup(&linked)?
            .ok_or("hard link absent")?
            .node
            .file_id,
        second_id
    );

    let delete_status = acyclic_native_runtime::process_status(
        std::process::Command::new("powershell.exe")
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "[IO.File]::Delete($env:ACYCLIC_FS_TEST_PATH)",
            ])
            .env("ACYCLIC_FS_TEST_PATH", destination.join(second_name)),
        std::time::Duration::from_secs(120),
    )?;
    assert!(delete_status.success());
    session.flush_callbacks(&work)?;
    assert_eq!(source.lookup(&second)?, None);
    assert!(source.lookup(&linked)?.is_some());
    assert!(session.stop()?);
    Ok(())
}
