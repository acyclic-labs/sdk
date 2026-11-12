use super::*;

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
