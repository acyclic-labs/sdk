use super::*;

#[cfg(all(not(target_arch = "wasm32"), any(unix, windows)))]
fn assert_local_open_pending(opening: std::pin::Pin<&mut impl std::future::Future>) {
    // Finish canonicalization inline, so Pending observes the real registry lock.
    let _inline = acyclic_native_runtime::InlineBlocking::enter();
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(std::future::Future::poll(opening, &mut context).is_pending());
}

#[cfg(all(not(target_arch = "wasm32"), any(unix, windows)))]
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

#[cfg(all(not(target_arch = "wasm32"), any(unix, windows)))]
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

#[cfg(all(not(target_arch = "wasm32"), any(unix, windows)))]
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

    let mut opening = std::pin::pin!(Fs::local(options.clone()));
    assert_local_open_pending(opening.as_mut());
    {
        let mut cancelled = std::pin::pin!(Fs::local(options.clone()));
        assert_local_open_pending(cancelled.as_mut());
    }
    assert_local_open_pending(opening.as_mut());

    drop(retained);
    let reopened = opening.await?;
    let coalesced = Fs::local(options).await?;
    assert!(reopened.same_deployment(&coalesced));
    Ok(())
}

#[cfg(all(not(target_arch = "wasm32"), any(unix, windows)))]
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
    release_tx
        .send(())
        .map_err(|_| "startup worker lost its release event")?;
    let _released = Arc::clone(&lifecycle).lock_owned().await;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
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

#[cfg(all(not(target_arch = "wasm32"), any(unix, windows)))]
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

#[cfg(all(not(target_arch = "wasm32"), any(unix, windows)))]
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

    let mut reopening = std::pin::pin!(Fs::local(LocalOptions::new(&first_root)));
    let mut concurrent = std::pin::pin!(Fs::local(LocalOptions::new(&first_root)));
    assert_local_open_pending(reopening.as_mut());
    assert_local_open_pending(concurrent.as_mut());

    let second = Fs::local(LocalOptions::new(second_root)).await?;
    assert_local_open_pending(reopening.as_mut());
    assert_local_open_pending(concurrent.as_mut());
    drop(second);
    drop(retained);
    let reopened = reopening.await?;
    let shared = concurrent.await?;
    assert!(reopened.same_deployment(&shared));
    drop(shared);
    drop(reopened);
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
