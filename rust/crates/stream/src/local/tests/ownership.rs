use super::*;

#[test]
fn cancelled_real_open_retains_ownership_until_journal_open_stops()
-> Result<(), Box<dyn std::error::Error>> {
    use tracing::instrument::WithSubscriber as _;
    use tracing_subscriber::layer::SubscriberExt as _;
    let captured = acyclic_grpc_observability::test_support::Capture::default();
    let dispatch =
        tracing::Dispatch::new(tracing_subscriber::Registry::default().with(captured.clone()));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()?;
    let (blocking_started_tx, blocking_started_rx) = std::sync::mpsc::sync_channel(0);
    let (release_blocker_tx, release_blocker_rx) = std::sync::mpsc::sync_channel(0);
    let blocker = runtime.spawn_blocking(move || {
        let _ = blocking_started_tx.send(());
        let _ = release_blocker_rx.recv();
    });
    blocking_started_rx.recv()?;

    runtime.block_on(async move {
        let directory = tempfile::tempdir()?;
        let lifecycle = Arc::new(tokio::sync::Mutex::new(()));
        let ownership = Arc::clone(&lifecycle).lock_owned().await;
        let anchor = OwnershipAnchor::new(ownership);
        let root = directory.path().to_path_buf();
        let (submitted_tx, submitted_rx) = tokio::sync::oneshot::channel();
        {
            let mut hook = JOURNAL_OPEN_SUBMITTED
                .lock()
                .map_err(|_| "journal-open test hook was poisoned")?;
            *hook = Some((root.clone(), submitted_tx));
        }
        let opening = tokio::spawn(
            async move {
                LocalStream::open_with_ownership_anchor(root, LocalStreamLimits::default(), anchor)
                    .await
            }
            .with_subscriber(dispatch),
        );

        submitted_rx.await?;
        opening.abort();
        let cancelled = opening.await;
        assert!(
            cancelled
                .as_ref()
                .is_err_and(tokio::task::JoinError::is_cancelled)
        );
        let ownership_retained = Arc::clone(&lifecycle).try_lock_owned().is_err();

        release_blocker_tx.send(())?;
        blocker.await?;
        tokio::task::spawn_blocking(|| ()).await?;
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            Arc::clone(&lifecycle).lock_owned(),
        )
        .await?;
        assert!(
            ownership_retained,
            "caller cancellation must not release ownership from active Journal::open"
        );
        let fields = captured.fields("acyclic.stream.open");
        assert_eq!(
            fields["outcome"], "ok",
            "detached work owns its successful completion"
        );
        assert!(!fields.contains_key("error.kind"));
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}

#[test]
fn cancelled_persist_retains_ownership_until_the_journal_task_stops()
-> Result<(), Box<dyn std::error::Error>> {
    let _serial = PERSIST_BLOCKER_TESTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    use tracing::instrument::WithSubscriber as _;
    use tracing_subscriber::layer::SubscriberExt as _;
    let captured = acyclic_grpc_observability::test_support::Capture::default();
    let dispatch =
        tracing::Dispatch::new(tracing_subscriber::Registry::default().with(captured.clone()));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()?;

    runtime.block_on(async move {
        let directory = tempfile::tempdir()?;
        let lifecycle = Arc::new(tokio::sync::Mutex::new(()));
        let ownership = Arc::clone(&lifecycle).lock_owned().await;
        let provider = LocalStream::open_with_ownership_anchor(
            directory.path(),
            LocalStreamLimits::default(),
            OwnershipAnchor::new(ownership),
        )
        .await?;

        let inner_identity = Arc::as_ptr(&provider.inner) as usize;
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        {
            let mut hook = JOURNAL_PERSIST_BLOCKER
                .lock()
                .map_err(|_| "journal-persist test hook was poisoned")?;
            *hook = Some((inner_identity, started_tx, release_rx));
        }
        let appending = tokio::spawn(
            {
                let provider = provider.clone();
                async move {
                    provider
                        .append(AppendRequest {
                            path: StreamPath::new("cancelled-persist")?,
                            records: vec![OwnedBytes::from_static(b"record")],
                            if_tail: Some(0),
                            idempotency_key: None,
                        })
                        .await
                }
            }
            .with_subscriber(dispatch),
        );

        started_rx.recv()?;
        appending.abort();
        assert!(appending.await.is_err_and(|error| error.is_cancelled()));
        drop(provider);
        assert!(
            Arc::clone(&lifecycle).try_lock_owned().is_err(),
            "queued journal persistence must retain local-root ownership after cancellation"
        );

        release_tx.send(())?;
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            Arc::clone(&lifecycle).lock_owned(),
        )
        .await?;
        let fields = captured.fields("acyclic.stream.append");
        assert_eq!(
            fields["outcome"], "ok",
            "detached work owns its successful completion"
        );
        assert!(!fields.contains_key("error.kind"));
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}

#[test]
fn native_caller_abort_keeps_sdk_request_and_accepted_flush_owned()
-> Result<(), Box<dyn std::error::Error>> {
    let _serial = PERSIST_BLOCKER_TESTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let request = keyed_commit("native-detached-input", 0)?;
        let key = request.idempotency_key.clone();
        let input_bytes = request_control_capacity(&request);
        let meter = Arc::new(Meter::local());
        let original = meter.original();
        let caller_controls = ReadAllocation::adopt(&original, input_bytes)?;
        let inner_identity = Arc::as_ptr(&provider.inner) as usize;
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        *JOURNAL_PERSIST_BLOCKER
            .lock()
            .map_err(|_| "journal-persist hook was poisoned")? =
            Some((inner_identity, started_tx, release_rx));
        let caller = tokio::spawn({
            let provider = provider.clone();
            async move {
                let _caller_controls = caller_controls;
                provider.native_commit(request, original).await
            }
        });
        tokio::task::spawn_blocking(move || {
            started_rx.recv_timeout(std::time::Duration::from_secs(5))
        })
        .await??;
        caller.abort();
        let cancelled = caller.await.is_err_and(|error| error.is_cancelled());
        let owned_during_drain = meter.held.load(Ordering::SeqCst);
        // Always unblock accepted cleanup before asserting the control.
        release_tx.send(())?;
        assert!(cancelled);
        assert!(owned_during_drain >= input_bytes as u64);
        assert_eq!(
            provider
                .tail(StreamPath::new("native-detached-input")?)
                .await?,
            1
        );
        let accepted = provider
            .inspect_idempotency(key.clone())
            .await?
            .ok_or("detached native operation lost its identity")?;
        let terminal = Arc::downgrade(&provider.inner);
        drop(provider);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while terminal.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let reopened = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        assert_eq!(reopened.inspect_idempotency(key).await?, Some(accepted));
        meter.assert_refunded();
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}

#[test]
fn a_cancelled_mutation_is_invisible_until_its_frame_is_durable()
-> Result<(), Box<dyn std::error::Error>> {
    let _serial = PERSIST_BLOCKER_TESTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let inner_identity = Arc::as_ptr(&provider.inner) as usize;
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        *JOURNAL_PERSIST_BLOCKER
            .lock()
            .map_err(|_| "journal-persist test hook was poisoned")? =
            Some((inner_identity, started_tx, release_rx));
        let path = StreamPath::new("cancelled-visibility")?;
        let appending = tokio::spawn({
            let (provider, path) = (provider.clone(), path.clone());
            async move {
                provider
                    .append(AppendRequest {
                        path,
                        records: vec![OwnedBytes::from_static(b"record")],
                        if_tail: Some(0),
                        idempotency_key: None,
                    })
                    .await
            }
        });
        tokio::task::spawn_blocking(move || started_rx.recv()).await??;
        appending.abort();
        assert!(appending.await.is_err_and(|error| error.is_cancelled()));

        let reading = tokio::spawn({
            let (provider, path) = (provider.clone(), path.clone());
            async move { provider.tail(path).await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(!reading.is_finished(), "the unsynced record is not visible");
        release_tx.send(())?;
        assert_eq!(reading.await??, 1, "the persisted record is visible");
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}

#[test]
fn mutations_on_other_paths_write_while_a_flush_is_under_way()
-> Result<(), Box<dyn std::error::Error>> {
    let _serial = PERSIST_BLOCKER_TESTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let inner_identity = Arc::as_ptr(&provider.inner) as usize;
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        *JOURNAL_PERSIST_BLOCKER
            .lock()
            .map_err(|_| "journal-persist test hook was poisoned")? =
            Some((inner_identity, started_tx, release_rx));
        let commit = |path: &'static str| {
            let provider = provider.clone();
            tokio::spawn(async move { provider.commit(keyed_commit(path, 0)?).await })
        };
        let first = commit("first");
        tokio::task::spawn_blocking(move || started_rx.recv()).await??;
        let second = commit("second");
        let appended = |provider: &LocalStream| {
            provider
                .inner
                .journal
                .lock()
                .map(|journal| journal.appended)
                .unwrap_or_default()
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while appended(&provider) < 2 {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await?;
        assert!(!first.is_finished() && !second.is_finished());
        release_tx.send(())?;
        for (outcome, path) in [(first.await??, "first"), (second.await??, "second")] {
            assert!(matches!(outcome, CommitOutcome::Committed(_)), "{path}");
            assert_eq!(provider.tail(StreamPath::new(path)?).await?, 1, "{path}");
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}

#[test]
fn native_guard_lease_survives_caller_abort_and_real_durability_terminal()
-> Result<(), Box<dyn std::error::Error>> {
    let _serial = PERSIST_BLOCKER_TESTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let directory = tempfile::tempdir()?;
        let limits = LocalStreamLimits::default();
        let provider = LocalStream::open(directory.path(), limits).await?;
        let request = keyed_commit("guard-owned-drain", 0)?;
        let key = request.idempotency_key.clone();
        let meter = Arc::new(Meter::local());
        let released = Arc::new(AtomicU64::new(0));
        let lease = Arc::new(GuardLeaseWitness(Arc::clone(&released)));
        let weak = Arc::downgrade(&lease);
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        *JOURNAL_PERSIST_BLOCKER
            .lock()
            .map_err(|_| "journal-persist hook was poisoned")? = Some((
            Arc::as_ptr(&provider.inner) as usize,
            started_tx,
            release_rx,
        ));
        let caller = tokio::spawn({
            let provider = provider.clone();
            let meter = Arc::clone(&meter);
            async move {
                provider
                    .native_commit_guarded(request, None, meter.original(), move || {
                        let lease = Arc::clone(&lease);
                        async move {
                            let _original_lease = lease;
                            Ok::<(), NativeGuardFailure>(())
                        }
                    })
                    .await
            }
        });
        tokio::task::spawn_blocking(move || {
            started_rx.recv_timeout(std::time::Duration::from_secs(5))
        })
        .await??;
        caller.abort();
        let cancelled = caller.await.is_err_and(|error| error.is_cancelled());
        let retained = weak.upgrade().is_some();
        let completed_bytes = meter.write_bytes.load(Ordering::SeqCst);
        let syncs_before_release = meter.durability_operations.load(Ordering::SeqCst);
        // Always release the accepted real cleanup before asserting.
        release_tx.send(())?;
        assert!(cancelled && retained);
        assert!(completed_bytes > 0);
        assert_eq!(syncs_before_release, 0);
        assert_eq!(
            provider.tail(StreamPath::new("guard-owned-drain")?).await?,
            1
        );
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while weak.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert_eq!(released.load(Ordering::SeqCst), 1);
        let accepted = provider
            .inspect_idempotency(key.clone())
            .await?
            .ok_or("owned guarded commit lost its original identity")?;
        drop(provider);
        let reopened = LocalStream::open(directory.path(), limits).await?;
        assert_eq!(reopened.inspect_idempotency(key).await?, Some(accepted));
        drop(reopened);
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
        meter.assert_refunded();
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}
