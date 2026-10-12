use super::*;

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
        let inner_identity = Arc::as_ptr(&provider.inner.journal) as usize;
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
            Arc::as_ptr(&provider.inner.journal) as usize,
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
