use super::*;

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_facade_reopens_durable_volume_and_exact_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let volume_id = VolumeId::from_bytes([48; 16]);
    let cancellation = CancellationToken::new();
    let generation_id;
    let root_released;
    let mut durable = config();
    durable.lifecycle = Lifecycle::Durable;
    {
        let fs = Fs::local(LocalOptions::new(directory.path())).await?;
        root_released = fs
            .local_root_release_barrier()
            .ok_or("local root has no release barrier")?;
        let created = fs
            .create_volume_with_id(volume_id, durable, WorkBudget::UNBOUNDED, &cancellation)
            .await?;
        let checkout = created
            .value
            .checkout(
                GenerationSelector::Head,
                writable_pinned(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        generation_id = checkout.value.generation_id();
    }
    tokio::time::timeout(std::time::Duration::from_secs(10), root_released.released()).await?;
    for relative in ["stream/stream.lock", "objects/owner.lock"] {
        drop(acyclic_native_runtime::StoreClaim::acquire(
            &directory.path().join(relative),
        )?);
    }
    let reopened = Fs::local(LocalOptions::new(directory.path())).await?;
    let volume = reopened
        .open_volume(volume_id, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    let exact = volume
        .value
        .checkout(
            GenerationSelector::Exact(generation_id),
            pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    assert_eq!(exact.value.generation_id(), generation_id);
    assert_eq!(exact.value.volume_id(), volume_id);
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test]
async fn local_root_lifecycle_outlives_provider_destruction()
-> Result<(), Box<dyn std::error::Error>> {
    struct BlockingProviderDrop {
        entered: std::sync::mpsc::SyncSender<()>,
        release: std::sync::mpsc::Receiver<()>,
    }

    impl Drop for BlockingProviderDrop {
        fn drop(&mut self) {
            let _ = self.entered.send(());
            let _ = self.release.recv();
        }
    }

    let lifecycle = Arc::new(tokio::sync::Mutex::new(()));
    let ownership = Arc::clone(&lifecycle).try_lock_owned()?;
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(0);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
    let inner = FsInner {
        authority: BlockingProviderDrop {
            entered: entered_tx,
            release: release_rx,
        },
        objects: (),
        capabilities: EmbeddedCapabilities::MEMORY,
        workspace_namespace: [0; 16],
        generation_positions: super::super::generation_cache::GenerationPositions::default(),
        policy_cache: super::super::policy_cache::PolicyCache::default(),
        publication_proof: Default::default(),
        _local_root_lifecycle: Some(acyclic_native_runtime::OwnershipAnchor::new(ownership)),
    };
    let dropping = std::thread::spawn(move || drop(inner));

    entered_rx.recv()?;
    assert!(
        Arc::clone(&lifecycle).try_lock_owned().is_err(),
        "replacement root ownership must remain fenced while providers are dropping"
    );
    release_tx.send(())?;
    dropping
        .join()
        .map_err(|_| "provider drop thread panicked")?;
    Arc::clone(&lifecycle).try_lock_owned()?;
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn detached_provider_handles_retain_local_root_ownership()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let lifecycle = Arc::new(tokio::sync::Mutex::new(()));
    let ownership = Arc::clone(&lifecycle).lock_owned().await;
    let fs = Fs::open_local_unshared(
        LocalOptions::new(directory.path()),
        Some(acyclic_native_runtime::OwnershipAnchor::new(ownership)),
    )
    .await?;
    let stream = fs.inner.authority.provider();
    let objects = Arc::clone(fs.inner.objects.inner().inner().provider());
    drop(fs);

    assert!(
        Arc::clone(&lifecycle).try_lock_owned().is_err(),
        "a detached Stream or Objects handle must retain the canonical root gate"
    );
    drop(stream);
    assert!(
        Arc::clone(&lifecycle).try_lock_owned().is_err(),
        "the root gate must remain held until every detached provider is gone"
    );
    drop(objects);
    Arc::clone(&lifecycle).try_lock_owned()?;
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test]
async fn cancelled_local_open_keeps_the_canonical_lifecycle_gate()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let options = LocalOptions::new(directory.path());
    let first = Fs::local(options.clone()).await?;
    let retained = first
        .inner
        ._local_root_lifecycle
        .as_ref()
        .ok_or_else(|| std::io::Error::other("local root ownership"))?
        .clone();
    drop(first);

    let retry_options = options.clone();
    let opening = tokio::spawn(async move { Fs::local(retry_options).await });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(!opening.is_finished());
    let cancelled_options = options.clone();
    let cancelled = tokio::spawn(async move { Fs::local(cancelled_options).await });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(!cancelled.is_finished());
    cancelled.abort();
    assert!(cancelled.await.is_err_and(|error| error.is_cancelled()));

    drop(retained);
    let reopened = tokio::time::timeout(std::time::Duration::from_secs(2), opening).await???;
    let coalesced = Fs::local(options).await?;
    assert!(reopened.same_deployment(&coalesced));
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test]
async fn cancelled_local_initialization_keeps_root_owned_until_startup_stops()
-> Result<(), Box<dyn std::error::Error>> {
    let lifecycle = Arc::new(tokio::sync::Mutex::new(()));
    let ownership = Arc::clone(&lifecycle).lock_owned().await;
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let opening = tokio::spawn(run_local_initialization(
        ownership,
        move |ownership| async move {
            let _ownership = ownership;
            let _ = started_tx.send(());
            let _ = release_rx.await;
            drop(_ownership);
        },
    ));

    started_rx.await?;
    opening.abort();
    let cancelled = opening.await;
    assert!(
        cancelled.as_ref().is_err_and(|error| error.is_cancelled()),
        "opening caller must be cancelled: {cancelled:?}"
    );
    assert!(
        Arc::clone(&lifecycle).try_lock_owned().is_err(),
        "caller cancellation must not release ownership from an active startup worker"
    );
    let _ = release_tx.send(());
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        Arc::clone(&lifecycle).lock_owned(),
    )
    .await?;
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_facade_shares_bounded_object_acceleration_across_handles()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let mut durable = config();
    durable.lifecycle = Lifecycle::Durable;
    let fs = Fs::local(LocalOptions::new(directory.path())).await?;
    let volume = fs
        .create_volume(durable, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    let checkout = volume
        .checkout(
            GenerationSelector::Head,
            pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let root = ObjectId {
        storage: None,
        kind: ObjectKind::GenerationRoot,
        digest: checkout.generation_id().digest(),
    };

    fs.clear_object_cache()?;
    let baseline = fs.object_cache_stats()?;
    assert_eq!(baseline.resident_entries, 0);
    let first = fs
        .export_object(
            root,
            durable.limits.maximum_object_bytes,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    let cloned = fs.clone();
    let warm = cloned
        .export_object(
            root,
            durable.limits.maximum_object_bytes,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;

    assert_eq!(first.value.bytes, warm.value.bytes);
    assert_eq!(first.work.backend_read_operations, 1);
    assert_eq!(warm.work.backend_read_operations, 0);
    let stats = fs.object_cache_stats()?;
    assert_eq!(stats.misses - baseline.misses, 1);
    assert_eq!(stats.hits - baseline.hits, 1);
    assert_eq!(stats.resident_entries, 1);
    assert!(stats.resident_bytes > 0);
    Ok(())
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rejected_open_leaves_no_provider_opening_behind_it()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let options = LocalOptions::new(directory.path());
    for iteration in 0..24_u32 {
        let fs = Fs::local(options.clone()).await?;
        assert!(
            Fs::open_local_unshared(options.clone(), None)
                .await
                .is_err(),
            "a live engine excludes a second owner"
        );
        drop(fs);
        drop(
            acyclic_objects::local::LocalObjects::open(
                directory.path().join("objects"),
                acyclic_objects::local::LocalObjectsLimits::default(),
                None,
            )
            .await
            .map_err(|error| format!("objects still owned after iteration {iteration}: {error}"))?,
        );
        drop(
            acyclic_stream::LocalStream::open(
                directory.path().join("stream"),
                acyclic_stream::LocalStreamLimits::default(),
            )
            .await
            .map_err(|error| format!("stream still owned after iteration {iteration}: {error}"))?,
        );
    }
    Ok(())
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_garbage_collection_keeps_live_state_while_the_root_is_open()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let options = LocalOptions::new(directory.path());
    let cancellation = CancellationToken::new();
    let volume_id = VolumeId::from_bytes([49; 16]);
    let file = path("retained.txt")?;
    let mut durable = config();
    durable.lifecycle = Lifecycle::Durable;
    let fs = Fs::local(options.clone()).await?;
    let volume = fs
        .create_volume_with_id(volume_id, durable, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    let mut checkout = volume
        .checkout(
            GenerationSelector::Head,
            writable_pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    checkout
        .create(
            crate::AuthoredMutation::CreateFile {
                path: file.clone(),
                bytes: (bytes::Bytes::from_static(b"retained")).into(),
                metadata: crate::kernel::FileMetadata::default(),
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    checkout
        .commit(
            OperationId::from_bytes([49; 16]),
            None,
            None,
            None,
            crate::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &cancellation,
            allow::<FsError>,
        )
        .await?;
    let pending = path("pending.txt")?;
    checkout
        .create(
            crate::AuthoredMutation::CreateFile {
                path: pending.clone(),
                bytes: (bytes::Bytes::from(vec![b'p'; 256 * 1_024])).into(),
                metadata: crate::kernel::FileMetadata::default(),
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    fs.collect_local_garbage(None, &cancellation).await?;
    checkout
        .commit(
            OperationId::from_bytes([50; 16]),
            None,
            None,
            None,
            crate::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &cancellation,
            allow::<FsError>,
        )
        .await?;
    fs.collect_local_garbage(None, &cancellation).await?;
    drop(checkout);
    drop(volume);
    drop(fs);

    let orphan_bytes = OwnedBytes::from(vec![b'o'; 64 * 1_024 + 1]);
    let objects = acyclic_objects::local::LocalObjects::open(
        directory.path().join("objects"),
        acyclic_objects::local::LocalObjectsLimits::default(),
        None,
    )
    .await?;
    acyclic_objects::Blobs::put(&objects, orphan_bytes, Vec::new()).await?;
    drop(objects);
    let reopened = Fs::local(options).await?;
    let collected = reopened.collect_local_garbage(None, &cancellation).await?;
    assert!(collected.removed >= 1);

    let volume = reopened
        .open_volume(volume_id, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    let mut checkout = volume
        .value
        .checkout(
            GenerationSelector::Head,
            pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    let retained = checkout
        .value
        .read_file_range(
            &file,
            ByteRange {
                offset: 0,
                length: 8,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    assert_eq!(retained.value.bytes.as_ref(), b"retained");
    let pending = checkout
        .value
        .read_file_range(
            &pending,
            ByteRange {
                offset: 0,
                length: 256 * 1_024,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    assert_eq!(
        pending.value.bytes.as_ref(),
        vec![b'p'; 256 * 1_024].as_slice()
    );
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_facade_centralizes_same_process_owners_and_rejects_option_drift()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let options = LocalOptions::new(directory.path());
    let first = Fs::local(options.clone()).await?;
    let second = Fs::local(options.clone()).await?;
    assert!(first.same_deployment(&second));

    let mut conflicting = options;
    conflicting.object_cache.maximum_entries =
        conflicting.object_cache.maximum_entries.saturating_add(1);
    assert!(matches!(
        Fs::local(conflicting).await,
        Err(FsError::LocalOptionsConflict)
    ));
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_facade_waits_per_root_without_serializing_independent_roots()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let first_root = directory.path().join("first");
    let second_root = directory.path().join("second");
    let first = Fs::local(LocalOptions::new(&first_root)).await?;
    let retained = first
        .inner
        ._local_root_lifecycle
        .as_ref()
        .ok_or_else(|| std::io::Error::other("local root ownership"))?
        .clone();
    drop(first);

    let same_root = first_root.clone();
    let reopening = tokio::spawn(async move { Fs::local(LocalOptions::new(first_root)).await });
    let concurrent = tokio::spawn(async move { Fs::local(LocalOptions::new(same_root)).await });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        !reopening.is_finished(),
        "retained native owner must fence reopening"
    );
    assert!(!concurrent.is_finished());

    let second = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        Fs::local(LocalOptions::new(second_root)),
    )
    .await??;
    drop(second);
    drop(retained);
    let reopened = tokio::time::timeout(std::time::Duration::from_secs(2), reopening).await???;
    let shared = tokio::time::timeout(std::time::Duration::from_secs(2), concurrent).await???;
    assert!(reopened.same_deployment(&shared));
    drop(shared);
    drop(reopened);
    Ok(())
}

/// Counts one local commit of `files` new files: the ordering barriers and
/// the full device drains its durable trace issues.
#[cfg(all(feature = "local", any(unix, windows)))]
async fn local_commit_flushes(files: u8) -> Result<(usize, usize), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (schedule, mut events) = acyclic_native_runtime::testing::Schedule::new();
    let driver = tokio::spawn(async move {
        while let Some(boundary) = events.recv().await {
            boundary.release();
        }
    });
    let counted = schedule
        .scope(async {
            let cancellation = CancellationToken::new();
            let mut durable = config();
            durable.lifecycle = Lifecycle::Durable;
            let fs = Fs::local(LocalOptions::new(directory.path())).await?;
            let volume = fs
                .create_volume_with_id(
                    VolumeId::from_bytes([51; 16]),
                    durable,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await?
                .value;
            let mut checkout = volume
                .checkout(
                    GenerationSelector::Head,
                    writable_pinned(),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await?
                .value;
            for file in 0..files {
                checkout
                    .create(
                        crate::AuthoredMutation::CreateFile {
                            path: path(&format!("f{file}.txt"))?,
                            bytes: bytes::Bytes::from(vec![file; 4096]).into(),
                            metadata: crate::kernel::FileMetadata::default(),
                        },
                        WorkBudget::UNBOUNDED,
                        &cancellation,
                    )
                    .await?;
            }
            let before = schedule.durable_writes().len();
            checkout
                .commit(
                    OperationId::from_bytes([51; 16]),
                    None,
                    None,
                    None,
                    crate::PublicationPermit::Unrestricted,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                    allow::<FsError>,
                )
                .await?;
            let trace = schedule.durable_writes();
            let commit = &trace[before..];
            let barrier = |call: &acyclic_native_runtime::testing::DurableWrite| {
                call.operation.ends_with("_barrier")
            };
            let drain = |call: &acyclic_native_runtime::testing::DurableWrite| {
                matches!(
                    call.operation,
                    "file:sync" | "file:sync_data" | "file:sync_all" | "file:sync_parent"
                )
            };
            let counted = (
                commit.iter().filter(|call| barrier(call)).count(),
                commit.iter().filter(|call| drain(call)).count(),
            );
            assert!(
                commit.iter().rposition(barrier) < commit.iter().rposition(drain),
                "the acknowledging drain must follow every ordering barrier: {commit:?}"
            );
            drop(checkout);
            drop(volume);
            drop(fs);
            Ok::<_, Box<dyn std::error::Error>>(counted)
        })
        .await?;
    schedule.drained().await;
    driver.abort();
    let _ = driver.await;
    Ok(counted)
}

/// A commit's objects are only ordered ahead of its Stream frame, and the
/// frame's flush is the commit's one device drain, at any commit size.
#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test]
async fn a_local_commit_drains_the_device_once_at_any_size()
-> Result<(), Box<dyn std::error::Error>> {
    let small = local_commit_flushes(1).await?;
    let large = local_commit_flushes(64).await?;
    eprintln!("LOCAL_COMMIT_FLUSHES files=1 {small:?} files=64 {large:?}");
    assert_eq!(small, (1, 1), "unexpected flush counts for one file");
    assert_eq!(large, (1, 1), "unexpected flush counts for 64 files");
    Ok(())
}

/// One step of a commit against a volatile device cache, per device.
#[derive(Clone, Copy, PartialEq)]
enum Step {
    /// Write number `.0` to device `.1`.
    Write(usize, usize),
    /// Order the device's earlier writes ahead of its later ones.
    Barrier(usize),
    /// Make the device's earlier writes durable.
    Drain(usize),
    /// Tell the caller the commit is durable.
    Acknowledge,
}

/// Every crash state of `steps`: whether the acknowledgement was given, and
/// which writes survive. A crash keeps every write its device drained, and
/// otherwise any set of that device's writes that respects its barriers; a
/// barrier or drain on one device orders nothing on another.
fn crash_states(steps: &[Step]) -> Vec<(bool, Vec<bool>)> {
    let writes = steps
        .iter()
        .filter(|step| matches!(step, Step::Write(..)))
        .count();
    let mut states = Vec::new();
    for crash in 0..=steps.len() {
        let issued = &steps[..crash];
        let acknowledged = issued.contains(&Step::Acknowledge);
        // Per write: its number, device, ordering epoch and whether drained.
        let mut epoch = [0_usize; 2];
        let mut placed: Vec<(usize, usize, usize, bool)> = Vec::new();
        for step in issued {
            match *step {
                Step::Write(id, device) => placed.push((id, device, epoch[device], false)),
                Step::Barrier(device) => epoch[device] += 1,
                Step::Drain(device) => {
                    epoch[device] += 1;
                    for write in &mut placed {
                        write.3 |= write.1 == device;
                    }
                }
                Step::Acknowledge => {}
            }
        }
        for mask in 0_u32..(1 << placed.len()) {
            let kept = |index: usize| placed[index].3 || mask & (1 << index) != 0;
            let ordered = (0..placed.len()).all(|later| {
                !kept(later)
                    || (0..placed.len()).all(|earlier| {
                        placed[earlier].1 != placed[later].1
                            || placed[earlier].2 >= placed[later].2
                            || kept(earlier)
                    })
            });
            if ordered {
                let mut survived = vec![false; writes];
                for (index, write) in placed.iter().enumerate() {
                    survived[write.0] |= kept(index);
                }
                states.push((acknowledged, survived));
            }
        }
    }
    states
}

/// The local commit protocol: the objects (write 0) on `objects_device`
/// take `objects_flush`, then the frame (write 1) is written and drained on
/// device 0, then the commit is acknowledged. A surviving frame must find
/// its objects, and an acknowledged commit must survive whole.
fn commit_is_crash_consistent(objects_flush: Option<Step>, objects_device: usize) -> bool {
    let mut steps = vec![Step::Write(0, objects_device)];
    steps.extend(objects_flush);
    steps.extend([Step::Write(1, 0), Step::Drain(0), Step::Acknowledge]);
    crash_states(&steps)
        .into_iter()
        .all(|(acknowledged, survived)| {
            (!survived[1] || survived[0]) && (!acknowledged || (survived[0] && survived[1]))
        })
}

#[test]
fn an_ordering_barrier_suffices_for_objects_only_on_the_frame_device() {
    // The composition's choice: a barrier on the frame's device.
    assert!(commit_is_crash_consistent(Some(Step::Barrier(0)), 0));
    // On another device only that device's own drain keeps the contract.
    assert!(commit_is_crash_consistent(Some(Step::Drain(1)), 1));
    // Negative controls: without a flush a frame can outlive its objects, and
    // a barrier on another device neither orders the frame nor is drained by it.
    assert!(!commit_is_crash_consistent(None, 0));
    assert!(!commit_is_crash_consistent(Some(Step::Barrier(1)), 1));
}
