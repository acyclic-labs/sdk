fn metadata() -> FileMetadata {
    FileMetadata::default()
}

fn checkout_state(source: &MemorySource) -> (crate::GenerationId, bool) {
    source.runtime.block_on(|| async {
        let checkout = source.checkout.lock().await;
        (checkout.generation_id(), checkout.has_pending_mutations())
    }, None).expect("checkout state")
}

fn source(profile: FilesystemProfile) -> Result<MemorySource, Box<dyn std::error::Error>> {
    Ok(shared_sources(profile)?.0)
}

fn shared_sources(
    profile: FilesystemProfile,
) -> Result<(MemorySource, MemorySource), Box<dyn std::error::Error>> {
    sources(
        profile,
        ConsistencyMode::Pinned,
        MountPublication::CloseAndSync,
    )
}

#[test]
fn created_file_cases() -> Result<(), Box<dyn std::error::Error>> {
    for (name, offset, bytes) in [
        ("held", 0, Bytes::from_static(b"first bytes")),
        ("read", 4, Bytes::from_static(b"tail")),
        ("large", 0, Bytes::from(vec![7_u8; (1 << 20) + 1])),
    ] {
        let source = source(FilesystemProfile::Portable)?;
        let work = capture_meter(boundary_budget(), source.cancellation())?;
        let path = native_test_path(name);
        let created = source.create_file(&path, metadata(), None, &work)?;
        let file = source.open_created(&path, &created)?;
        file.write_range(offset, bytes.clone())?;
        assert_eq!(
            file.lookup()?.node.logical_bytes,
            offset + bytes.len() as u64
        );
        assert_eq!(
            source.lookup(&path)?.map(|entry| entry.node.file_id),
            Some(created.node.file_id)
        );
        assert!(matches!(
            source.create_file(&path, metadata(), None, &work),
            Err(MountSourceError::AlreadyExists)
        ));
        if name != "large" {
            let reopened = source.open_file(&path)?;
            let mut expected = vec![0; offset as usize];
            expected.extend_from_slice(&bytes);
            assert_eq!(reopened.read_up_to(0, 64)?.as_ref(), expected);
            if offset != 0 {
                file.write_range(0, Bytes::from_static(b"head"))?;
                assert_eq!(reopened.read_up_to(0, 64)?.as_ref(), b"headtail");
            }
            file.settle(&work)?;
            assert_eq!(
                source.lookup(&path)?.map(|entry| entry.metadata),
                Some(file.lookup()?.metadata)
            );
        }
        source.sync()?;
        assert!(matches!(
            source.create_file(&path, metadata(), None, &work),
            Err(MountSourceError::AlreadyExists)
        ));
    }
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
    let work =
        capture_meter(boundary_budget(), &source.cancellation)?;
    source.create_file(&target, metadata(), None, &work)?;
    assert!(source.lookup(&target)?.is_some());
    {
        let _requester = requester.enter();
        let queued = native_test_path("queued");
        source.runtime.wait(|| {
            source.create(&queued, &work, |path| AuthoredMutation::CreateFile {
                path,
                metadata: metadata(),
                bytes: acyclic_stream::OwnedBytes::new(),
            })
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
            writer.create_directory(&MountPath::root().child(b"leased".to_vec()), metadata(), &capture_meter(boundary_budget(), &CancellationToken::new())?);
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
    let (source, _) = sources(
        FilesystemProfile::Portable,
        ConsistencyMode::TrackingSafe,
        MountPublication::CloseAndSync,
    )?;
    let source = Arc::new(source);
    let path = native_test_path("shared.bin");
    source.create_file(&path, metadata(), None, &capture_meter(boundary_budget(), &CancellationToken::new())?)?;
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
    let (source, _) = sources(
        FilesystemProfile::Portable,
        ConsistencyMode::TrackingSafe,
        MountPublication::CloseAndSync,
    )?;
    let source = Arc::new(source);
    let pair = [
        native_test_path("first.bin"),
        native_test_path("second.bin"),
    ];
    for path in &pair {
        source.create_file(path, metadata(), None, &capture_meter(boundary_budget(), &CancellationToken::new())?)?;
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
                        .apply_authored_transaction(vec![crate::AuthoredMutation::Write { path: path.clone(), offset: 0, bytes: acyclic_stream::OwnedBytes::from_vec((Bytes::from(vec![round])).to_vec()) }], boundary_budget(), &source.cancellation)
                        .await
                        .map_err(engine_error)?;
                    tokio::task::yield_now().await;
                }
                Ok::<_, MountSourceError>(())
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
    let (source, _) = sources(
        FilesystemProfile::Portable,
        ConsistencyMode::TrackingSafe,
        MountPublication::CloseAndSync,
    )?;
    let path = native_test_path("observed.bin");
    source.create_file(&path, metadata(), None, &capture_meter(boundary_budget(), &CancellationToken::new())?)?;
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
            .apply_authored_transaction(vec![crate::AuthoredMutation::Write { path: path, offset: 0, bytes: acyclic_stream::OwnedBytes::from_vec((Bytes::from_static(b"lost")).to_vec()) }], boundary_budget(), &source.cancellation)
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
fn namespace_invalidation_cases() -> Result<(), Box<dyn std::error::Error>> {
    for (operation, expected) in [
        ("write", [true, false, true, true, true, true]),
        ("create", [false, true, true, false, true, true]),
        ("remove", [false, true, false, true, true, true]),
    ] {
        let source = source(FilesystemProfile::Portable)?;
        let work = capture_meter(boundary_budget(), source.cancellation())?;
        let d = native_test_path("d");
        let e = native_test_path("e");
        let a = d.child(native_test_path("a").components()[0].clone());
        let b = d.child(native_test_path("b").components()[0].clone());
        let c = d.child(native_test_path("c").components()[0].clone());
        let x = e.child(native_test_path("x").components()[0].clone());
        let alias = native_test_path("alias");
        let d_id = source.create_directory(&d, metadata(), &work)?.node.file_id;
        let e_id = source.create_directory(&e, metadata(), &work)?.node.file_id;
        let a_id = source
            .create_file(&a, metadata(), None, &work)?
            .node
            .file_id;
        let b_id = source
            .create_file(&b, metadata(), None, &work)?
            .node
            .file_id;
        let x_id = source
            .create_file(&x, metadata(), None, &work)?
            .node
            .file_id;
        source.hard_link(&a, &alias, &work)?;
        let checks = [
            (&d, Some(d_id)),
            (&a, Some(a_id)),
            (&b, Some(b_id)),
            (&c, None),
            (&e, Some(e_id)),
            (&x, Some(x_id)),
        ];
        let stamp = source.view_stamp().ok_or("view stamp")?;
        let binding = source.binding_epoch();
        assert!(
            checks
                .iter()
                .all(|(path, id)| source.unchanged_since(path, *id, stamp))
        );
        match operation {
            "write" => {
                source
                    .open_file(&a)?
                    .write_range(0, Bytes::from_static(b"a"))?;
                assert!(!source.unchanged_since(&alias, Some(a_id), stamp));
            }
            "create" => {
                source.create_file(&c, metadata(), None, &work)?;
            }
            "remove" => {
                source.remove(&b, Some(b_id), &work)?;
            }
            _ => unreachable!(),
        }
        for ((path, id), expected) in checks.into_iter().zip(expected) {
            assert_eq!(source.unchanged_since(path, id, stamp), expected);
        }
        assert_eq!(source.binding_epoch(), binding);
        let _lease = source.acquire_binding_lease(binding)?;
    }
    Ok(())
}

#[test]
fn host_captures_invalidate_exactly_what_they_capture() -> Result<(), Box<dyn std::error::Error>> {
    let (source, _) = sources(
        FilesystemProfile::Portable,
        ConsistencyMode::Pinned,
        MountPublication::Manual,
    )?;
    let host = tempfile::tempdir()?;
    for directory in ["captured", "untouched"] {
        std::fs::create_dir(host.path().join(directory))?;
        source.create_directory(&native_test_path(directory), metadata(), &capture_meter(boundary_budget(), &CancellationToken::new())?)?;
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
    let (reader, writer) = sources(
        FilesystemProfile::Portable,
        ConsistencyMode::TrackingSafe,
        MountPublication::CloseAndSync,
    )?;
    let untouched = native_test_path("untouched");
    let advances: [&dyn Fn() -> Result<(), MountSourceError>; 2] = [
        &|| reader.runtime.block_on(|| reader.refresh_async(), None).expect("callback future"),
        &|| reader.runtime.block_on(|| reader.advance_to_head_async(), None).expect("callback future"),
    ];
    for (round, advance) in advances.into_iter().enumerate() {
        let (published, written) = (
            native_test_path(&format!("published-{round}")),
            native_test_path(&format!("written-{round}")),
        );
        let written_id = writer.create_file(&written, metadata(), None, &capture_meter(boundary_budget(), &CancellationToken::new())?)?.node.file_id;
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
        writer.create_file(&published, metadata(), None, &capture_meter(boundary_budget(), &CancellationToken::new())?)?;
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
            .create(crate::AuthoredMutation::CreateFile { path: namespace, bytes: acyclic_stream::OwnedBytes::new(), metadata: FileMetadata::default() }, boundary_budget(), &source.cancellation)
            .await
            .map(|_| ())
    }, None).expect("callback future")?;
    assert!(!source.unchanged_since(&untouched, None, stamp));
    Ok(())
}

#[test]
fn open_handle_read_up_to_clips_at_eof_without_a_separate_lookup()
-> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Portable)?;
    let path = native_test_path("clipped-read");
    source.create_file(&path, metadata(), None, &capture_meter(boundary_budget(), &CancellationToken::new())?)?;
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
