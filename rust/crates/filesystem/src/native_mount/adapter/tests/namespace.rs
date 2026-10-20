fn metadata() -> FileMetadata {
    FileMetadata::default()
}

fn checkout_state(source: &MemorySource) -> (crate::GenerationId, bool) {
    source
        .runtime
        .block_on(
            || async {
                let checkout = source.checkout.lock().await;
                (checkout.generation_id(), checkout.has_pending_mutations())
            },
            None,
        )
        .expect("checkout state")
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
        let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
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
            let mut expected = vec![0; usize::try_from(offset)?];
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
    source.observe_view(weak)?;
    let (requester, applier) = (ViewOrigin::new(), ViewOrigin::new());
    let target = native_test_path("applied");
    let work = capture_meter(BOUNDARY_BUDGET, &source.cancellation)?;
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
        let result = boundary(writer.cancellation(), |work| {
            writer.create_directory(&native_test_path("leased"), metadata(), work)
        });
        assert!(completed.send(result).is_ok(), "mutation result receiver");
    });
    assert!(
        receive.recv_timeout(Duration::from_millis(50)).is_err(),
        "mutation crossed a retained native callback view"
    );
    drop(lease);
    receive.recv_timeout(Duration::from_secs(5))??;
    task.join().map_err(|_| "mutation thread panicked")?;
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
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    let path = native_test_path("shared.bin");
    source.create_file(&path, metadata(), None, &work)?;
    source.write_range(&path, 0, Bytes::from_static(b"shared"))?;
    let owner = ViewGate::callback_owner()?;
    let held = source.runtime.wait(|| async {
        source
            .checkout
            .observe(
                owner,
                &mut NativeDemandCarrier::Native(&work),
                &source.cancellation,
                None,
            )
            .await
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
    task.join().map_err(|_| "read thread panicked")?;
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
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    let pair = ["first.bin", "second.bin"].map(native_test_path);
    for path in &pair {
        source.create_file(path, metadata(), None, &work)?;
        source.write_range(path, 0, Bytes::from_static(&[0]))?;
    }
    let pair = [source.path(&pair[0])?, source.path(&pair[1])?];
    let done = Arc::new(AtomicBool::new(false));
    let readers = (0..4)
        .map(|_| {
            let source = Arc::clone(&source);
            let pair = pair.clone();
            let done = Arc::clone(&done);
            std::thread::spawn(move || -> Result<(), MountSourceError> {
                let owner = ViewGate::callback_owner()?;
                loop {
                    let finished = done.load(Ordering::Acquire);
                    let [first, second] = source.runtime.wait(|| async {
                        let work = capture_meter(BOUNDARY_BUDGET, &source.cancellation)?;
                        let observation = source
                            .checkout
                            .observe(
                                owner,
                                &mut NativeDemandCarrier::Native(&work),
                                work.cancellation(),
                                None,
                            )
                            .await?;
                        let mut checkout = observation.observer(
                            &mut crate::kernel::allocation::KernelAccounting::native(
                                &work,
                                work.cancellation(),
                            ),
                            work.cancellation(),
                        )?;
                        let mut values = [0; 2];
                        for (value, path) in values.iter_mut().zip(&pair) {
                            *value = checkout
                                .read_file_range(
                                    path,
                                    ByteRange {
                                        offset: 0,
                                        length: 1,
                                    },
                                    &work,
                                    work.cancellation(),
                                )
                                .await?
                                .bytes[0];
                            tokio::task::yield_now().await;
                        }
                        Ok(values)
                    })?;
                    assert_eq!(first, second, "an observation crossed a mutation");
                    if finished {
                        return Ok(());
                    }
                }
            })
        })
        .collect::<Vec<_>>();
    for round in 1..=ROUNDS {
        source.runtime.wait(|| {
            Box::pin(async {
                let mut candidate = source
                    .checkout
                    .candidate(
                        None,
                        &mut NativeDemandCarrier::Native(&work),
                        work.cancellation(),
                        Some(&source.cancellation),
                    )
                    .await?;
                for path in &pair {
                    candidate
                        .checkout
                        .change_content(
                            path.clone(),
                            ContentChange::Write {
                                offset: 0,
                                bytes: OwnedBytes::from(vec![round]),
                            },
                            ContentTimes::Preserve,
                            &work,
                            &source.cancellation,
                        )
                        .await
                        .map_err(engine_error)?;
                    tokio::task::yield_now().await;
                }
                source
                    .checkout
                    .install_change(
                        candidate,
                        ViewChange::Everything,
                        &work,
                        &source.cancellation,
                        source.native_callback_guard.clone(),
                    )
                    .await
            })
        })?;
    }
    done.store(true, Ordering::Release);
    for reader in readers {
        reader.join().map_err(|_| "observation thread panicked")??;
    }
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
        let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
        let paths = ["d", "d/a", "d/b", "d/c", "e", "e/x"].map(native_test_path);
        let mut ids = [None; 6];
        for index in [0, 4, 1, 2, 5] {
            ids[index] = Some(
                if index == 0 || index == 4 {
                    source.create_directory(&paths[index], metadata(), &work)?
                } else {
                    source.create_file(&paths[index], metadata(), None, &work)?
                }
                .node
                .file_id,
            );
        }
        let alias = native_test_path("alias");
        source.hard_link(&paths[1], &alias, &work)?;
        let stamp = source.view_stamp().ok_or("view stamp")?;
        let binding = source.binding_epoch();
        assert!(
            paths
                .iter()
                .zip(ids)
                .all(|(path, id)| source.unchanged_since(path, id, stamp))
        );
        match operation {
            "write" => {
                source
                    .open_file(&paths[1])?
                    .write_range(0, Bytes::from_static(b"a"))?;
                assert!(!source.unchanged_since(&alias, ids[1], stamp));
            }
            "create" => {
                source.create_file(&paths[3], metadata(), None, &work)?;
            }
            "remove" => {
                source.remove(&paths[2], ids[2], &work)?;
            }
            _ => unreachable!(),
        }
        for ((path, id), expected) in paths.iter().zip(ids).zip(expected) {
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
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    for directory in ["captured", "untouched"] {
        std::fs::create_dir(host.path().join(directory))?;
        source.create_directory(&native_test_path(directory), metadata(), &work)?;
        for name in ["a", "b"] {
            std::fs::write(host.path().join(directory).join(name), b"host")?;
        }
    }
    let [a, b, untouched] = ["captured/a", "captured/b", "untouched/a"].map(native_test_path);
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

    let stamp = source.view_stamp().ok_or("checkout has no view stamp")?;
    std::fs::write(host.path().join("captured").join("a"), b"changed")?;
    source.capture_host_paths(host.path(), std::slice::from_ref(&a))?;
    assert_eq!(current(stamp)?, [false, true, true, true]);

    let stamp = source.view_stamp().ok_or("checkout has no view stamp")?;
    source.capture_host_paths(host.path(), std::slice::from_ref(&b))?;
    assert_eq!(current(stamp)?, [true, false, true, true]);

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
    let work = capture_meter(BOUNDARY_BUDGET, writer.cancellation())?;
    let advances: [&dyn Fn() -> Result<(), MountSourceError>; 2] =
        [&|| reader.runtime.wait(|| reader.refresh_async()), &|| {
            reader.runtime.wait(|| reader.advance_to_head_async())
        }];
    for (round, advance) in advances.into_iter().enumerate() {
        let (published, written) = (
            native_test_path(&format!("published-{round}")),
            native_test_path(&format!("written-{round}")),
        );
        let written_id = writer
            .create_file(&written, metadata(), None, &work)?
            .node
            .file_id;
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
        writer.create_file(&published, metadata(), None, &work)?;
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
    let stamp = source.view_stamp().ok_or("checkout has no view stamp")?;
    let untouched = native_test_path("untouched");
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    let (namespace, _allocation) =
        source.namespace_path(&path, &mut NativeDemandCarrier::Native(&work))?;
    source.runtime.wait(|| async {
        let mut checkout = source.checkout.lock().await;
        checkout.checkout = checkout.checkout.private_candidate(
            &mut crate::kernel::KernelAccounting::native(&work, work.cancellation()),
            work.cancellation(),
        )?;
        apply_authored(
            &mut checkout.checkout,
            &AuthoredMutation::CreateFile {
                path: namespace,
                metadata: metadata(),
                bytes: OwnedBytes::new(),
            },
            &work,
        )
        .await
    })?;
    assert!(!source.unchanged_since(&untouched, None, stamp));
    Ok(())
}

/// A rename across kinds, over a directory that is not empty, over an
/// existing name without replacement or of an absent name, and the removal
/// of a directory that is not empty, fail with their own POSIX error, which
/// a native mount reports as is, not as an I/O error.
#[test]
fn kind_and_emptiness_refusals_keep_their_posix_errors() -> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Portable)?;
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    let (directory, file, full, absent) = (
        native_test_path("directory"),
        native_test_path("file"),
        native_test_path("full"),
        native_test_path("absent"),
    );
    source.create_directory(&directory, metadata(), &work)?;
    source.create_file(&file, metadata(), None, &work)?;
    source.create_directory(&full, metadata(), &work)?;
    source.create_file(&native_test_path("full/child"), metadata(), None, &work)?;

    let refusals = [
        (source.rename(&directory, &file, true, &work), "ENOTDIR"),
        (source.rename(&file, &directory, true, &work), "EISDIR"),
        (source.rename(&directory, &full, true, &work), "ENOTEMPTY"),
        (source.remove(&full, None, &work), "ENOTEMPTY"),
        (source.rename(&file, &directory, false, &work), "EEXIST"),
        (source.rename(&absent, &file, true, &work), "ENOENT"),
    ];
    for (refusal, expected) in refusals {
        let error = refusal.err().ok_or("the operation succeeded")?;
        let reported = match error.semantic_error() {
            MountSourceError::NotDirectory => "ENOTDIR",
            MountSourceError::IsDirectory => "EISDIR",
            MountSourceError::DirectoryNotEmpty => "ENOTEMPTY",
            MountSourceError::AlreadyExists => "EEXIST",
            MountSourceError::NotFound => "ENOENT",
            _ => "other",
        };
        assert_eq!(reported, expected, "{error:?}");
        #[cfg(target_os = "linux")]
        assert_eq!(
            crate::native_mount::errno(&error),
            match expected {
                "ENOTDIR" => libc::ENOTDIR,
                "EISDIR" => libc::EISDIR,
                "ENOTEMPTY" => libc::ENOTEMPTY,
                "EEXIST" => libc::EEXIST,
                _ => libc::ENOENT,
            }
        );
    }
    assert!(source.lookup(&directory)?.is_some());
    assert!(source.lookup(&file)?.is_some());
    Ok(())
}

/// The kernel checks a rename's kinds itself but leaves emptiness to the
/// file system, so a mount's own refusal must arrive as `ENOTEMPTY`.
#[cfg(target_os = "linux")]
#[test]
#[ignore = "mounts a live FUSE session"]
fn a_linux_mount_refuses_replacing_or_removing_a_full_directory_with_enotempty()
-> Result<(), Box<dyn std::error::Error>> {
    let source = Arc::new(source(FilesystemProfile::Posix)?);
    let temporary = tempfile::tempdir()?;
    let mut mount = mount_source(&source, temporary.path())?;

    let (empty, full) = (
        temporary.path().join("empty"),
        temporary.path().join("full"),
    );
    std::fs::create_dir(&empty)?;
    std::fs::create_dir(&full)?;
    std::fs::write(full.join("child"), b"child")?;
    let renamed = std::fs::rename(&empty, &full).err();
    let removed = std::fs::remove_dir(&full).err();
    assert_eq!(
        renamed.and_then(|error| error.raw_os_error()),
        Some(libc::ENOTEMPTY)
    );
    assert_eq!(
        removed.and_then(|error| error.raw_os_error()),
        Some(libc::ENOTEMPTY)
    );
    assert!(empty.is_dir() && full.join("child").is_file());
    assert!(mount.stop()?);
    Ok(())
}

#[test]
fn full_case_fold_native_comparison_keys_can_expand_past_name_limits()
-> Result<(), Box<dyn std::error::Error>> {
    let mut source = source(FilesystemProfile::Portable)?;
    source.folds_names = true;
    let text = "ΐ".repeat(100);
    let path = native_test_path(&text);
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    source.create_file(&path, metadata(), None, &work)?;
    let folded = source
        .fold_path(&path, &work)?
        .ok_or("missing comparison key")?;
    assert_eq!(
        folded.0.components()[0],
        native_test_path(&"ι\u{308}\u{301}".repeat(100)).components()[0]
    );
    assert!(source.lookup(&path)?.is_some());
    Ok(())
}
