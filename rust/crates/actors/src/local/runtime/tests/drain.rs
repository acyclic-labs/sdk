use super::*;

/// The producer's receipt and the dispatcher's delayed follow are one event.
/// A fresh committed activation is the control that still permits recovery.
#[tokio::test]
async fn delayed_activation_cannot_redeliver_a_failed_attempt() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let handler = Arc::new(Gated::new()?);
    let runtime = Runtime::new(store.clone(), handler.clone(), Arc::new(SystemClock));
    let address = spawn_with(&store, "one-activation", &[b"emit"]).await?;
    runtime.track(address.clone())?;
    for recover in [false, true] {
        let mut request = CommitRequest::new(key(if recover { "recover" } else { "deliver" })?);
        request.effects.push(store.activation(&address)?);
        let outcome = provider.commit(request).await?;
        let acyclic_stream::CommitOutcome::Committed(receipt) = outcome else {
            return Err(format!("activation did not commit: {outcome:?}").into());
        };
        runtime.wake_committed(&address, &receipt)?;
        let work = runtime.ready.take().ok_or("activation did not wake")?;
        let mut delivery = Box::pin(runtime.execute(work));
        assert!(futures::poll!(delivery.as_mut()).is_pending());
        // Deliver the same original activation through the shared dispatcher
        // path while its handler is running. It must not queue a second attempt.
        let path = store.scheduling_path(store.scheduling_shard(&address))?;
        let cursor = receipt
            .appends
            .iter()
            .find(|append| append.path == path)
            .ok_or("activation receipt missing")?
            .tail;
        runtime.activate(&address, cursor);
        if recover {
            handler.gate.add_permits(1);
            delivery.await.map_err(|(_, error)| error)?;
        } else {
            drop(delivery);
            assert_eq!(
                store
                    .load(&address)
                    .await?
                    .state()
                    .and_then(|state| state.cursor("inbox")),
                Some(0)
            );
        }
        assert!(
            runtime.ready.take().is_none(),
            "duplicate activation queued another delivery"
        );
    }
    assert_eq!(records(&*provider, "out/one-activation").await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn failure_observers_survive_hibernation_and_unpolled_recovery() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let handler = Arc::new(Gated::new()?);
    let runtime = Runtime::new(store.clone(), handler.clone(), Arc::new(SystemClock));
    let address = spawn_with(&store, "failure-observed", &[b"emit"]).await?;
    let mut failed = runtime.delivery_failure(&address)?;
    assert_eq!(
        runtime.counts(),
        (0, 0),
        "observation registered an actor slot that hibernation could remove"
    );
    assert!(runtime.release(&address));
    assert_eq!(
        runtime.counts(),
        (0, 0),
        "observer retained actor residency"
    );
    assert!(
        futures::poll!(failed.as_mut()).is_pending(),
        "hibernation fabricated a failure"
    );

    runtime.host(address.clone())?;
    let mut delivery = Box::pin(runtime.drain_actor(&address));
    assert!(futures::poll!(delivery.as_mut()).is_pending());
    assert_eq!(handler.active.load(Ordering::SeqCst), 1);
    drop(delivery);
    assert_eq!(
        store
            .load(&address)
            .await?
            .state()
            .and_then(|state| state.cursor("inbox")),
        Some(0)
    );
    // Recover before the registered observer gets another poll. Recovery must
    // not erase its event, but a newly registered observer must remain healthy.
    assert!(runtime.release(&address));
    runtime.host(address.clone())?;
    handler.gate.add_permits(1);
    runtime.drain_actor(&address).await?;
    assert!(failed.await.to_string().contains("producer disappeared"));
    let mut next = runtime.delivery_failure(&address)?;
    assert!(
        futures::poll!(next.as_mut()).is_pending(),
        "recovery retained a stale active failure"
    );
    assert_eq!(records(&*provider, "out/failure-observed").await?.len(), 1);

    assert!(runtime.release(&address));
    assert_eq!(runtime.counts(), (0, 0));
    provider
        .append(AppendRequest {
            path: StreamPath::new("agents/failure-observed")?,
            records: vec![Bytes::from_static(b"emit").into()],
            if_tail: Some(1),
            idempotency_key: key("failure-observed/next")?,
        })
        .await?;
    runtime.host(address.clone())?;
    handler.gate.close();
    assert!(runtime.drain_actor(&address).await.is_err());
    assert!(
        next.await.to_string().contains("test gate closed"),
        "rehydration lost its original observer"
    );
    let mut stopping = runtime.delivery_failure(&actor("healthy-runtime-shutdown")?)?;
    assert!(futures::poll!(stopping.as_mut()).is_pending());
    drop(runtime);
    assert!(stopping.await.to_string().contains("runtime released"));
    Ok(())
}

/// Default admission runs beyond the retired 64 slots. The same real gate
/// under explicit credit is the deterministic negative control.
#[tokio::test]
async fn unbounded_default_admits_beyond_old_concurrency_with_explicit_credit_control() -> TestResult
{
    const ACTORS: usize = 65;
    for credit in [None, Some(64)] {
        let provider = Arc::new(MemoryStream::default());
        let store = Store::new(Arc::clone(&provider), root()?);
        let handler = Arc::new(Gated::new()?);
        let runtime = Runtime::new(
            crate::guest::Requests::default(),
            Store::new(Arc::clone(&provider), root()?),
            handler.clone(),
            Arc::new(SystemClock),
        );
        let runtime = match credit {
            Some(credit) => runtime.with_limits(RuntimeLimits::new(credit, 64)?),
            None => runtime,
        };
        let mut addresses = Vec::new();
        for index in 0..ACTORS {
            let address = spawn_with(&store, &format!("unbounded-{index}"), &[b"emit"]).await?;
            runtime.host(address.clone())?;
            addresses.push(address);
        }
        let mut entered = handler.entered.subscribe();
        let mut running = FuturesUnordered::new();
        for address in &addresses {
            running.push(runtime.drain_actor(address));
        }
        tokio::select! {
            observed = entered.wait_for(|active| *active == credit.unwrap_or(ACTORS)) => {
                assert_eq!(*observed?, credit.unwrap_or(ACTORS));
            }
            completed = running.next() => {
                return Err(format!("delivery completed before its gate opened: {completed:?}").into());
            }
        }
        assert_eq!(
            handler.active.load(Ordering::SeqCst),
            credit.unwrap_or(ACTORS)
        );
        handler.gate.add_permits(ACTORS);
        while let Some(result) = running.next().await {
            result?;
        }
        assert_eq!(handler.active.load(Ordering::SeqCst), 0);
        assert_eq!(
            handler.peak.load(Ordering::SeqCst),
            credit.unwrap_or(ACTORS)
        );
    }
    Ok(())
}

/// One admitted slot is the serial negative control. With two slots, the old
/// serial drain fails at the entry assertion, without waiting for a timeout.
#[tokio::test]
async fn finite_drain_shares_admission_and_retains_cancelled_inputs() -> TestResult {
    for active in [1, 2] {
        let provider = Arc::new(MemoryStream::default());
        let store = Store::new(Arc::clone(&provider), root()?);
        let handler = Arc::new(Gated::new()?);
        let runtime = Runtime::new(
            crate::guest::Requests::default(),
            Store::new(Arc::clone(&provider), root()?),
            handler.clone(),
            Arc::new(SystemClock),
        )
        .with_limits(RuntimeLimits::new(active, 64)?);
        let mut actors = Vec::new();
        for index in 0..6 {
            let address = spawn_with(&store, &format!("finite-{index}"), &[b"emit"]).await?;
            runtime.host(address.clone())?;
            actors.push(address);
        }
        {
            let mut drain = Box::pin(runtime.drain());
            assert!(futures::poll!(drain.as_mut()).is_pending());
            assert_eq!(handler.active.load(Ordering::SeqCst), active);
            assert_eq!(handler.peak.load(Ordering::SeqCst), active);
            assert_eq!(
                runtime
                    .admission
                    .as_ref()
                    .expect("explicit concurrency reservation")
                    .available_permits(),
                0
            );
            assert_eq!(runtime.counts(), (actors.len(), actors.len() - active));
            for address in &actors {
                assert_eq!(
                    store
                        .load(address)
                        .await?
                        .state()
                        .and_then(|state| state.cursor("inbox")),
                    Some(0),
                    "pending handlers cannot consume an input"
                );
            }
            // Dropping the original drain owns cancellation of every admitted
            // handler and returns its delivery locks and admission permits.
        }
        assert_eq!(handler.active.load(Ordering::SeqCst), 0);
        assert_eq!(
            runtime
                .admission
                .as_ref()
                .expect("explicit concurrency reservation")
                .available_permits(),
            active
        );
        handler.gate.add_permits(actors.len());
        for address in &actors {
            runtime.wake(address);
        }
        assert!(runtime.drain().await.is_empty());
        assert_eq!(runtime.counts(), (actors.len(), 0));
        for index in 0..actors.len() {
            assert_eq!(
                records(&*provider, &format!("out/finite-{index}")).await?,
                vec![Bytes::from_static(b"r0")],
                "cancelled delivery cannot duplicate its effect"
            );
        }
        assert_eq!(handler.peak.load(Ordering::SeqCst), active);

        // Losing the real handler gate's producer resolves all admitted waits
        // with its typed error; queued deliveries observe the same closed gate.
        for (index, address) in actors.iter().enumerate() {
            provider
                .append(AppendRequest {
                    path: StreamPath::new(format!("agents/finite-{index}"))?,
                    records: vec![Bytes::from_static(b"emit").into()],
                    if_tail: Some(1),
                    idempotency_key: key(&format!("finite/next-{index}"))?,
                })
                .await?;
            runtime.wake(address);
        }
        let mut drain = Box::pin(runtime.drain());
        assert!(futures::poll!(drain.as_mut()).is_pending());
        assert_eq!(handler.active.load(Ordering::SeqCst), active);
        handler.gate.close();
        let failures = drain.await;
        assert_eq!(failures.len(), actors.len());
        for (address, error) in failures {
            assert!(
                matches!(error, Error::Handler(crate::guest::GuestError::Handler(ref message)) if message == "test gate closed")
            );
            assert_eq!(
                store
                    .load(&address)
                    .await?
                    .state()
                    .and_then(|state| state.cursor("inbox")),
                Some(1),
                "failed delivery cannot consume the next input"
            );
        }
        assert_eq!(handler.active.load(Ordering::SeqCst), 0);
        assert_eq!(
            runtime
                .admission
                .as_ref()
                .expect("explicit concurrency reservation")
                .available_permits(),
            active
        );
    }
    Ok(())
}
