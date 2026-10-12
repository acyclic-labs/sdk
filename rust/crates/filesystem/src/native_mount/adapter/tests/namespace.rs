fn metadata() -> FileMetadata {
    FileMetadata {
        posix_mode: crate::kernel::MetadataField::Unavailable,
        posix_uid: crate::kernel::MetadataField::Unavailable,
        posix_gid: crate::kernel::MetadataField::Unavailable,
        posix_flags: crate::kernel::MetadataField::Unavailable,
        windows_attributes: crate::kernel::MetadataField::Unavailable,
        created_ns: crate::kernel::MetadataField::Unavailable,
        modified_ns: crate::kernel::MetadataField::Unavailable,
        accessed_ns: crate::kernel::MetadataField::Unavailable,
        changed_ns: crate::kernel::MetadataField::Unavailable,
        named_attributes: crate::kernel::MetadataField::Unavailable,
        acl: crate::kernel::MetadataField::Unavailable,
        security_descriptor: crate::kernel::MetadataField::Unavailable,
    }
}

fn checkout_state(source: &MemorySource) -> (crate::GenerationId, bool) {
    source.runtime.block_on(|| async {
        let checkout = source.checkout.lock().await;
        (checkout.generation_id(), checkout.has_pending_mutations())
    })
}

fn source(profile: FilesystemProfile) -> Result<MemorySource, Box<dyn std::error::Error>> {
    Ok(shared_sources(profile)?.0)
}

fn shared_sources(
    profile: FilesystemProfile,
) -> Result<(MemorySource, MemorySource), Box<dyn std::error::Error>> {
    shared_sources_with_publication(profile, MountPublication::CloseAndSync)
}

#[test]
fn creating_an_existing_path_fails() -> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Portable)?;
    let path = native_test_path("once");
    let stale = source.view_stamp().ok_or("ledger stamp")?;
    source.create_file(&path, metadata())?;
    assert!(matches!(
        source.create_file(&path, metadata()),
        Err(MountSourceError::AlreadyExists)
    ));
    assert!(matches!(
        source.create_absent_file(&path, metadata(), stale),
        Err(MountSourceError::AlreadyExists)
    ));
    source.sync()?;
    assert!(matches!(
        source.create_absent_file(&path, metadata(), stale),
        Err(MountSourceError::AlreadyExists)
    ));
    Ok(())
}

#[test]
fn created_handles_keep_identity_and_sparse_bytes() -> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Portable)?;
    let (held, read, large) = (
        native_test_path("held"),
        native_test_path("read"),
        native_test_path("large"),
    );

    let created = source.create_file(&held, metadata())?;
    let file = source.open_created(&held, &created)?;
    file.write_range(0, Bytes::from_static(b"first bytes"))?;
    assert_eq!(file.lookup()?.node.logical_bytes, 11);
    assert_eq!(file.read_up_to(0, 64)?.as_ref(), b"first bytes");
    file.settle()?;
    let applied = source.lookup(&held)?.ok_or("settled creation is absent")?;
    assert_eq!(applied.node.file_id, created.node.file_id);
    assert_eq!(applied.node.logical_bytes, 11);
    assert_eq!(applied.metadata, file.lookup()?.metadata);

    let created = source.create_file(&read, metadata())?;
    let file = source.open_created(&read, &created)?;
    file.write_range(4, Bytes::from_static(b"tail"))?;
    let reopened = source.open_file(&read)?;
    assert_eq!(reopened.read_up_to(0, 64)?.as_ref(), b"\0\0\0\0tail");
    file.write_range(0, Bytes::from_static(b"head"))?;
    assert_eq!(reopened.read_up_to(0, 64)?.as_ref(), b"headtail");

    let created = source.create_file(&large, metadata())?;
    let file = source.open_created(&large, &created)?;
    let past = (1 << 20) + 1;
    file.write_range(0, Bytes::from(vec![7_u8; past]))?;
    assert_eq!(
        source
            .lookup(&large)?
            .ok_or("large creation is absent")?
            .node
            .logical_bytes,
        u64::try_from(past)?
    );
    Ok(())
}

#[test]
fn changes_record_their_requesters_origin() -> Result<(), Box<dyn std::error::Error>> {
    struct Origins(StdMutex<Vec<ViewOrigin>>);
    impl ViewObserver for Origins {
        fn view_changed(&self, _position: ViewStamp, origin: ViewOrigin) {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(origin);
        }
    }

    let source = source(FilesystemProfile::Portable)?;
    let observer = Arc::new(Origins(StdMutex::new(Vec::new())));
    let weak: Weak<dyn ViewObserver> = Arc::<Origins>::downgrade(&observer);
    source.observe_view(weak);
    let (requester, applier) = (ViewOrigin::new(), ViewOrigin::new());
    let target = native_test_path("applied");
    source.create_file(&target, metadata())?;
    assert!(source.lookup(&target)?.is_some());
    {
        let _requester = requester.enter();
        source.runtime.wait(|| {
            source.checkout.apply_grouped(
                GroupedChange::CreateFile {
                    path: source.path(&native_test_path("queued"))?,
                    metadata: Box::new(metadata()),
                    file_id: FileId::new(),
                    bytes: acyclic_stream::OwnedBytes::new(),
                },
                &source.cancellation,
            )
        })?;
    }
    {
        let _applier = applier.enter();
        source
            .open_file(&target)?
            .write_range(0, Bytes::from_static(b"applied"))?;
    }
    let origins = observer.0.lock().map_err(|_| "poisoned origins")?.clone();
    assert_eq!(
        origins.get(origins.len().saturating_sub(2)..),
        Some(&[requester, applier][..])
    );
    Ok(())
}

#[test]
fn direct_checkout_view_lease_excludes_concurrent_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let source = Arc::new(source(FilesystemProfile::Portable)?);
    let lease = source.acquire_view_lease()?;
    let writer = Arc::clone(&source);
    let (completed, receive) = std::sync::mpsc::sync_channel(1);
    let task = std::thread::spawn(move || {
        let result =
            writer.create_directory(&MountPath::root().child(b"leased".to_vec()), metadata());
        assert!(completed.send(result).is_ok(), "mutation result receiver");
    });
    assert!(
        receive.recv_timeout(Duration::from_millis(50)).is_err(),
        "mutation crossed a retained native callback view"
    );
    drop(lease);
    receive.recv_timeout(Duration::from_secs(5))??;
    task.join()
        .map_err(|_| std::io::Error::other("mutation thread panicked"))?;
    Ok(())
}

#[test]
fn callback_reads_overlap_a_held_observation() -> Result<(), Box<dyn std::error::Error>> {
    let (source, _) = tracking_sources(FilesystemProfile::Portable)?;
    let source = Arc::new(source);
    let path = native_test_path("shared.bin");
    source.create_file(&path, metadata())?;
    source.write_range(&path, 0, Bytes::from_static(b"shared"))?;
    let owner = ViewGate::callback_owner();
    let held = source.runtime.wait(|| {
        source.checkout.observe(
            owner,
            &mut NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(
                boundary_budget(),
                &source.cancellation,
            )),
            &source.cancellation,
            None,
        )
    })?;
    let reader = Arc::clone(&source);
    let (completed, receive) = std::sync::mpsc::sync_channel(1);
    let task = std::thread::spawn(move || {
        let result = reader.read_range(&path, 0, 6);
        assert!(completed.send(result).is_ok(), "read result receiver");
    });
    let read = receive.recv_timeout(Duration::from_secs(5)).map_err(|_| {
        std::io::Error::other("a callback read waited for another callback's read")
    })??;
    assert_eq!(read.as_ref(), b"shared");
    drop(held);
    task.join()
        .map_err(|_| std::io::Error::other("read thread panicked"))?;
    Ok(())
}

#[test]
fn concurrent_observations_never_see_a_mutation_half_applied()
-> Result<(), Box<dyn std::error::Error>> {
    const ROUNDS: u8 = 32;
    let (source, _) = tracking_sources(FilesystemProfile::Portable)?;
    let source = Arc::new(source);
    let pair = [
        native_test_path("first.bin"),
        native_test_path("second.bin"),
    ];
    for path in &pair {
        source.create_file(path, metadata())?;
        source.write_range(path, 0, Bytes::from_static(&[0]))?;
    }
    let pair = [source.path(&pair[0])?, source.path(&pair[1])?];
    let done = Arc::new(AtomicBool::new(false));
    let readers = (0..4)
        .map(|_| {
            let source = Arc::clone(&source);
            let pair = pair.clone();
            let done = Arc::clone(&done);
            std::thread::spawn(move || -> Result<u32, MountSourceError> {
                let owner = ViewGate::callback_owner();
                let mut observed = 0;
                loop {
                    let finished = done.load(Ordering::Acquire);
                    let [first, second] = source.runtime.wait(|| async {
                        let observation = source
                            .checkout
                            .observe(
                                owner,
                                &mut NativeDemandCarrier::Borrowed(
                                    &mut crate::demand::DemandWork::new(
                                        boundary_budget(),
                                        &source.cancellation,
                                    ),
                                ),
                                &source.cancellation,
                                None,
                            )
                            .await?;
                        let mut checkout = observation
                            .observer(boundary_budget(), &source.cancellation)
                            .map_err(facade_error)?
                            .value;
                        let mut values = [0; 2];
                        for (value, path) in values.iter_mut().zip(&pair) {
                            let range = ByteRange {
                                offset: 0,
                                length: 1,
                            };
                            *value = checkout
                                .read_file_range(
                                    path,
                                    range,
                                    boundary_budget(),
                                    &source.cancellation,
                                )
                                .await
                                .map_err(engine_error)?
                                .value
                                .bytes[0];
                            tokio::task::yield_now().await;
                        }
                        Ok(values)
                    })?;
                    assert_eq!(first, second, "an observation crossed a mutation");
                    observed += 1;
                    if finished {
                        return Ok(observed);
                    }
                }
            })
        })
        .collect::<Vec<_>>();
    for round in 1..=ROUNDS {
        source.runtime.wait(|| {
            Box::pin(async {
                let mut checkout = source.checkout.lock().await;
                for path in &pair {
                    checkout
                        .write_file(
                            path.clone(),
                            0,
                            Bytes::from(vec![round]),
                            boundary_budget(),
                            &source.cancellation,
                        )
                        .await
                        .map_err(engine_error)?;
                    tokio::task::yield_now().await;
                }
                checkout
                    .publish_after_mutation(ViewChange::Everything, &source.cancellation)
                    .await
            })
        })?;
    }
    done.store(true, Ordering::Release);
    for reader in readers {
        let observed = reader
            .join()
            .map_err(|_| std::io::Error::other("observation thread panicked"))??;
        assert!(observed > 0);
    }
    Ok(())
}

#[test]
fn observers_cannot_mutate_or_advance_their_checkout() -> Result<(), Box<dyn std::error::Error>> {
    let (source, _) = tracking_sources(FilesystemProfile::Portable)?;
    let path = native_test_path("observed.bin");
    source.create_file(&path, metadata())?;
    let path = source.path(&path)?;
    let owner = ViewGate::callback_owner();
    let (write, refresh) = source.runtime.wait(|| async {
        let observation = source
            .checkout
            .observe(
                owner,
                &mut NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(
                    boundary_budget(),
                    &source.cancellation,
                )),
                &source.cancellation,
                None,
            )
            .await?;
        let mut observer = observation
            .observer(boundary_budget(), &source.cancellation)
            .map_err(facade_error)?
            .value;
        let write = observer
            .write_file(
                path,
                0,
                Bytes::from_static(b"lost"),
                boundary_budget(),
                &source.cancellation,
            )
            .await;
        let refresh = observer
            .refresh_head(boundary_budget(), &source.cancellation)
            .await;
        Ok::<_, MountSourceError>((write, refresh))
    })?;
    assert!(matches!(write, Err(failure) if matches!(failure.error, FsError::MutationNotAllowed)));
    assert!(matches!(refresh, Err(failure) if matches!(failure.error, FsError::RefreshNotAllowed)));
    Ok(())
}

#[test]
fn native_mutation_invalidates_cache_without_rebinding_the_mount()
-> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Portable)?;
    let binding = source.binding_epoch();
    let created = native_test_path("new-file");
    let stamp = source.view_stamp().ok_or("checkout has no view stamp")?;
    source.create_file(&created, metadata())?;
    assert_eq!(source.binding_epoch(), binding);
    assert!(!source.unchanged_since(&created, None, stamp));
    let _lease = source.acquire_binding_lease(binding)?;
    Ok(())
}

#[test]
fn view_changes_invalidate_exactly_the_lookups_they_change()
-> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Portable)?;
    let directory = native_test_path("d");
    let other = native_test_path("e");
    let child = |parent: &MountPath, name: &str| {
        let name = native_test_path(name);
        parent.child(name.components()[0].clone())
    };
    let (a, b, c, x) = (
        child(&directory, "a"),
        child(&directory, "b"),
        child(&directory, "c"),
        child(&other, "x"),
    );
    let directory_id = source
        .create_directory(&directory, metadata())?
        .node
        .file_id;
    let other_id = source.create_directory(&other, metadata())?.node.file_id;
    let a_id = source.create_file(&a, metadata())?.node.file_id;
    let b_id = source.create_file(&b, metadata())?.node.file_id;
    let x_id = source.create_file(&x, metadata())?.node.file_id;
    let current = |stamp| {
        [
            source.unchanged_since(&directory, Some(directory_id), stamp),
            source.unchanged_since(&a, Some(a_id), stamp),
            source.unchanged_since(&b, Some(b_id), stamp),
            source.unchanged_since(&c, None, stamp),
            source.unchanged_since(&other, Some(other_id), stamp),
            source.unchanged_since(&x, Some(x_id), stamp),
        ]
    };
    let stamp = source.view_stamp().ok_or("checkout has no view stamp")?;
    assert_eq!(current(stamp), [true; 6]);

    source.write_range(&a, 0, Bytes::from_static(b"a"))?;
    assert_eq!(current(stamp), [true, false, true, true, true, true]);

    let stamp = source.view_stamp().ok_or("checkout has no view stamp")?;
    source.create_file(&c, metadata())?;
    assert_eq!(current(stamp), [false, true, true, false, true, true]);

    let stamp = source.view_stamp().ok_or("checkout has no view stamp")?;
    source.remove(&b, Some(b_id))?;
    assert_eq!(current(stamp), [false, true, false, true, true, true]);
    Ok(())
}

#[test]
fn host_captures_invalidate_exactly_what_they_capture() -> Result<(), Box<dyn std::error::Error>> {
    let (source, _) =
        shared_sources_with_publication(FilesystemProfile::Portable, MountPublication::Manual)?;
    let host = tempfile::tempdir()?;
    for directory in ["captured", "untouched"] {
        std::fs::create_dir(host.path().join(directory))?;
        source.create_directory(&native_test_path(directory), metadata())?;
        for name in ["a", "b"] {
            std::fs::write(host.path().join(directory).join(name), b"host")?;
        }
    }
    let child = |directory: &str, name: &str| {
        native_test_path(directory).child(native_test_path(name).components()[0].clone())
    };
    let (a, b) = (child("captured", "a"), child("captured", "b"));
    let untouched = child("untouched", "a");
    source.capture_host_paths(host.path(), std::slice::from_ref(&a))?;
    let lookup = |path: &MountPath| -> Result<_, MountSourceError> {
        Ok(source.lookup(path)?.map(|lookup| lookup.node.file_id))
    };
    let current = |stamp| -> Result<[bool; 4], MountSourceError> {
        Ok([
            source.unchanged_since(&a, lookup(&a)?, stamp),
            source.unchanged_since(&b, lookup(&b)?, stamp),
            source.unchanged_since(&untouched, lookup(&untouched)?, stamp),
            source.unchanged_since(&native_test_path("untouched"), None, stamp),
        ])
    };

    // Recapturing a changed file invalidates it alone.
    let stamp = source.view_stamp().ok_or("checkout has no view stamp")?;
    std::fs::write(host.path().join("captured").join("a"), b"changed")?;
    source.capture_host_paths(host.path(), std::slice::from_ref(&a))?;
    assert_eq!(current(stamp)?, [false, true, true, true]);

    // A first capture binds a new name in its directory.
    let stamp = source.view_stamp().ok_or("checkout has no view stamp")?;
    source.capture_host_paths(host.path(), std::slice::from_ref(&b))?;
    assert_eq!(current(stamp)?, [true, false, true, true]);

    // A subtree capture invalidates beneath its root only.
    let stamp = source.view_stamp().ok_or("checkout has no view stamp")?;
    std::fs::write(host.path().join("captured").join("b"), b"changed")?;
    source.capture_host_subtree(host.path(), &native_test_path("captured"))?;
    assert_eq!(current(stamp)?[2..], [true, true]);
    assert!(!source.unchanged_since(&b, lookup(&b)?, stamp));
    Ok(())
}

#[test]
fn an_advance_records_exactly_what_it_changed() -> Result<(), Box<dyn std::error::Error>> {
    let (reader, writer) = tracking_sources(FilesystemProfile::Portable)?;
    let untouched = native_test_path("untouched");
    let advances: [&dyn Fn() -> Result<(), MountSourceError>; 2] = [
        &|| reader.runtime.block_on(|| reader.refresh_async()),
        &|| reader.runtime.block_on(|| reader.advance_to_head_async()),
    ];
    for (round, advance) in advances.into_iter().enumerate() {
        let (published, written) = (
            native_test_path(&format!("published-{round}")),
            native_test_path(&format!("written-{round}")),
        );
        let written_id = writer.create_file(&written, metadata())?.node.file_id;
        writer.sync()?;
        advance()?;

        let stamp = reader.view_stamp().ok_or("checkout has no view stamp")?;
        advance()?;
        assert_eq!(
            reader.view_stamp(),
            Some(stamp),
            "an advance to an unmoved head records nothing"
        );

        let root = reader
            .lookup(&MountPath::root())?
            .map(|lookup| lookup.node.file_id);
        writer.create_file(&published, metadata())?;
        writer.write_range(&written, 0, Bytes::from_static(b"changed"))?;
        writer.sync()?;
        advance()?;
        assert!(!reader.unchanged_since(&published, None, stamp));
        assert!(!reader.unchanged_since(&written, Some(written_id), stamp));
        assert!(!reader.unchanged_since(&MountPath::root(), root, stamp));
        assert!(
            reader.unchanged_since(&untouched, None, stamp),
            "a name the advance left alone stays cached"
        );
    }
    Ok(())
}

#[test]
fn a_write_through_one_name_invalidates_every_alias() -> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Portable)?;
    let (first, alias) = (native_test_path("first"), native_test_path("alias"));
    let file_id = source.create_file(&first, metadata())?.node.file_id;
    source.hard_link(&first, &alias)?;
    let stamp = source.view_stamp().ok_or("checkout has no view stamp")?;
    let attached = source.open_file(&first)?;
    attached.write_range(0, Bytes::from_static(b"shared"))?;
    assert!(!source.unchanged_since(&alias, Some(file_id), stamp));
    Ok(())
}

#[test]
fn an_unrecorded_checkout_change_invalidates_every_lookup() -> Result<(), Box<dyn std::error::Error>>
{
    let source = source(FilesystemProfile::Portable)?;
    let path = native_test_path("external.bin");
    let namespace = source.path(&path)?;
    let stamp = source.view_stamp().ok_or("checkout has no view stamp")?;
    let untouched = native_test_path("untouched");
    source.runtime.block_on(|| async {
        let mut checkout = source.checkout.lock().await;
        checkout
            .create_file(
                namespace,
                Bytes::new(),
                boundary_budget(),
                &source.cancellation,
            )
            .await
            .map(|_| ())
    })?;
    assert!(!source.unchanged_since(&untouched, None, stamp));
    Ok(())
}

#[test]
fn open_handle_read_up_to_clips_at_eof_without_a_separate_lookup()
-> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Portable)?;
    let path = native_test_path("clipped-read");
    source.create_file(&path, metadata())?;
    source.write_range(&path, 0, Bytes::from_static(b"abc"))?;
    let open = source.open_file(&path)?;
    assert_eq!(open.read_up_to(0, 128)?.as_ref(), b"abc");
    assert_eq!(open.read_up_to(1, 1)?.as_ref(), b"b");
    assert!(open.read_up_to(3, 128)?.is_empty());
    assert!(open.read_up_to(u64::MAX, 128)?.is_empty());
    source.write_range(&path, 0, Bytes::from_static(b"xyz"))?;
    assert_eq!(open.read_up_to(0, 128)?.as_ref(), b"xyz");
    Ok(())
}
