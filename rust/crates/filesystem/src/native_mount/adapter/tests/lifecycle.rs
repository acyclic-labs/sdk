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
    source.create_file(&path, timed)?;
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
    std::fs::write(directory.path().join("first"), b"first")?;
    std::fs::write(directory.path().join("second"), b"other")?;
    let first = source.path(&native_test_path("first"))?;
    let second = source.path(&native_test_path("second"))?;
    let options = CaptureOptions {
        source_root: directory.path().to_path_buf(),
        expected_root_identity: capture_root_identity(directory.path())?,
        maximum_paths: 2,
        maximum_extent_spans: 64,
    };
    let cancellation = CancellationToken::new();
    let mut zero_candidate = source
        .runtime
        .block_on(|| async { source.checkout.lock().await.private_candidate() });
    let zero_work = crate::NativeMaterializationWork::new(WorkBudget::default(), &cancellation)
        .map_err(|(error, _)| error)?;
    let zero_result = source.runtime.block_on(|| {
        capture_paths_batched(
            &mut zero_candidate,
            std::slice::from_ref(&first),
            &options,
            1,
            &cancellation,
            None,
            &zero_work,
        )
    });
    let Err(zero_failure) = zero_result else {
        return Err(std::io::Error::other("zero budget admitted host observation").into());
    };
    assert_eq!(zero_failure.work.source_entries_visited, 0);
    assert!(!zero_candidate.has_pending_mutations());
    let mut candidate = source
        .runtime
        .block_on(|| async { source.checkout.lock().await.private_candidate() });
    let single_work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let single = source.runtime.block_on(|| {
        capture_paths_batched(
            &mut candidate,
            std::slice::from_ref(&first),
            &options,
            1,
            &cancellation,
            None,
            &single_work,
        )
    })?;
    assert!(single.work.source_bytes_read > 0);
    let mut budget = WorkBudget::UNBOUNDED;
    budget.source_bytes_read = single.work.source_bytes_read;
    let mut candidate = source
        .runtime
        .block_on(|| async { source.checkout.lock().await.private_candidate() });
    let paths = [first, second];
    let work = crate::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(error, _)| error)?;
    let result = source.runtime.block_on(|| {
        capture_paths_batched(
            &mut candidate,
            &paths,
            &options,
            1,
            &cancellation,
            None,
            &work,
        )
    });
    let Err(failure) = result else {
        return Err(std::io::Error::other("aggregate source budget was ignored").into());
    };
    assert!(failure.error.to_string().contains("source_bytes_read"));
    assert!(!candidate.has_pending_mutations());
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
        let work = capture_meter(boundary_budget(), source.cancellation())
            .map_err(|failure| failure.error)?;
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
            let root = NamespacePath::new(
                vec![source.logical_name(directory.components()[0].as_slice())?],
                config.limits,
            )?;
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
    let (reader, writer) = sources(profile, ConsistencyMode::TrackingSafe, MountPublication::CloseAndSync)?;
    let path = native_test_path("observed.bin");
    writer.create_file(&path, metadata())?;
    writer.write_range(&path, 0, Bytes::from_static(b"first"))?;
    writer.sync()?;
    let binding_before = reader.binding_epoch();
    reader.runtime.block_on(|| reader.refresh_async())?;
    let binding_after = reader.binding_epoch();
    assert_ne!(binding_after, binding_before);
    assert_eq!(reader.read_range(&path, 0, 5)?.as_ref(), b"first");
    let observed_generation = checkout_state(&reader).0;

    writer.write_range(&path, 0, Bytes::from_static(b"other"))?;
    writer.sync()?;
    assert!(matches!(
        reader.runtime.block_on(|| reader.refresh_async()),
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
        let work = capture_meter(boundary_budget(), source.cancellation())
            .map_err(|failure| failure.error)?;
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

#[test]
fn shared_checkout_fence_blocks_every_adapter_until_exact_resolution()
-> Result<(), Box<dyn std::error::Error>> {
    let profile = if cfg!(target_os = "windows") {
        FilesystemProfile::Windows
    } else {
        FilesystemProfile::Posix
    };
    let (first, second) = shared_sources(profile)?;
    let operation_id = OperationId::new();
    first.runtime.block_on(|| async {
        first
            .checkout
            .lock()
            .await
            .retain_operation_id(operation_id)
    })?;
    let path = native_test_path("fenced.bin");
    assert!(matches!(
        second.create_file(&path, metadata()),
        Err(MountSourceError::Stale)
    ));
    first.runtime.block_on(|| async {
        first
            .checkout
            .lock()
            .await
            .clear_retained_operation(operation_id);
    });
    second.create_file(&path, metadata())?;
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn host_capture_preserves_unobservable_named_metadata() -> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Posix)?;
    let path = native_test_path("metadata.bin");
    source.create_file(&path, metadata())?;
    source.write_attribute(
        &path,
        b"user.acyclic.retained",
        Bytes::from_static(b"opaque"),
        MountAttributeWriteMode::Create,
    )?;
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("metadata.bin"), b"host")?;
    source.capture_host_path(root.path(), &path)?;
    assert_eq!(
        source
            .read_attribute(&path, b"user.acyclic.retained")?
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
    let file = MountPath::root().child(b"sparse".to_vec());
    source.create_file(&file, metadata())?;
    source.resize(&file, 100)?;
    source.write_range(&file, 20, Bytes::from_static(b"data"))?;
    assert_eq!(source.seek(&file, 0, MountSeekTarget::Data)?, Some(20));
    assert_eq!(source.seek(&file, 20, MountSeekTarget::Hole)?, Some(24));
    assert_eq!(source.seek(&file, 24, MountSeekTarget::Data)?, None);
    assert_eq!(source.seek(&file, 100, MountSeekTarget::Hole)?, Some(100));

    let fifo = MountPath::root().child(b"fifo".to_vec());
    let created = source.create_special(&fifo, MountNodeKind::Fifo, None, metadata())?;
    assert_eq!(created.node.kind, MountNodeKind::Fifo);
    let device = MountPath::root().child(b"device".to_vec());
    let created = source.create_special(
        &device,
        MountNodeKind::CharacterDevice,
        Some((12, 34)),
        metadata(),
    )?;
    assert_eq!(created.node.device, Some((12, 34)));
    assert_eq!(source.lookup(&device)?, Some(created));
    Ok(())
}

#[cfg(target_os = "windows")]
#[test]
fn windows_mount_path_preserves_exact_utf16_components() -> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Windows)?;
    let (initial, pending) = checkout_state(&source);
    assert!(!pending);
    let name = "exact-Î±-ðŸ˜€"
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>();
    let path = MountPath::root().child(name.clone());
    let exact_metadata = FileMetadata {
        windows_attributes: crate::kernel::MetadataField::Value(0x20),
        created_ns: crate::kernel::MetadataField::Value(1_234_500),
        modified_ns: crate::kernel::MetadataField::Value(2_345_600),
        ..metadata()
    };
    let created = source.create_file(&path, exact_metadata)?;
    assert_eq!(source.lookup(&path)?, Some(created));
    let page = source.read_directory(&MountPath::root(), None, 8)?;
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].name, name);
    assert_eq!(page.entries[0].metadata, exact_metadata);
    assert!(checkout_state(&source).1);
    source.flush()?;
    source.flush()?;
    let (published, pending) = checkout_state(&source);
    assert_ne!(published, initial);
    assert!(!pending);

    let host = tempfile::tempdir()?;
    let captured_text = "captured-Î².bin";
    std::fs::write(host.path().join(captured_text), b"captured")?;
    let captured_name = captured_text
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>();
    let captured_path = MountPath::root().child(captured_name);
    source.capture_host_path(host.path(), &captured_path)?;
    assert_eq!(
        source.read_range(&captured_path, 0, 8)?.as_ref(),
        b"captured"
    );
    std::fs::remove_file(host.path().join(captured_text))?;
    source.capture_host_path(host.path(), &captured_path)?;
    assert_eq!(source.lookup(&captured_path)?, None);
    Ok(())
}

#[cfg(target_os = "windows")]
#[test]
#[ignore = "requires the live Windows ProjFS optional feature"]
fn writable_projfs_captures_closes_renames_links_and_deletes()
-> Result<(), Box<dyn std::error::Error>> {
    let source = Arc::new(source(FilesystemProfile::Windows)?);
    let temporary = tempfile::tempdir()?;
    let destination = temporary.path().join("projection");
    std::fs::create_dir(&destination)?;
    let mut session = mount_source(&source, &destination)?;

    let first_name = "written-Î±.bin";
    let second_name = "renamed-Î².bin";
    let linked_name = "linked-Î³.bin";
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
    session.flush_callbacks()?;
    let first = windows_path(first_name);
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
    session.flush_callbacks()?;
    let second = windows_path(second_name);
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
    session.flush_callbacks()?;
    let linked = windows_path(linked_name);
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
    session.flush_callbacks()?;
    assert_eq!(source.lookup(&second)?, None);
    assert!(source.lookup(&linked)?.is_some());
    assert!(session.stop()?);
    Ok(())
}

#[cfg(target_os = "windows")]
fn windows_path(value: &str) -> MountPath {
    MountPath::root().child(value.encode_utf16().flat_map(u16::to_le_bytes).collect())
}
