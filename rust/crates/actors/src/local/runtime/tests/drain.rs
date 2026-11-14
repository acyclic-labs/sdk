use super::*;

struct OwnedGated {
    gated: Gated,
    owner: ActorReference,
    /// Actors whose next delivery fails definitively, making them unavailable.
    refused: std::sync::Mutex<Vec<ActorReference>>,
}
impl OwnedGated {
    fn refuse(&self, actor: &ActorReference, refused: bool) {
        let mut actors = self.refused.lock().unwrap();
        actors.retain(|candidate| candidate != actor);
        if refused {
            actors.push(actor.clone());
        }
    }
}
impl Handler for OwnedGated {
    fn manifest(&self) -> &Manifest {
        self.gated.manifest()
    }
    fn failure_owner(
        &self,
        actor: &ActorReference,
        _: Option<&Checkpoint>,
    ) -> Option<ActorReference> {
        actor
            .id()
            .starts_with("dependent-")
            .then(|| self.owner.clone())
    }
    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        cancellation: crate::guest::CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        if self.refused.lock().unwrap().contains(&delivery.actor) {
            return Box::pin(async {
                Err(crate::guest::GuestError::Handler("definitive refusal".into()))
            });
        }
        self.gated.on_record(delivery, checkpoint, cancellation)
    }
}

#[tokio::test]
async fn dependent_failures_survive_owner_success_and_clear_only_on_source_recovery() -> TestResult
{
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(provider.clone(), root()?);
    let owner = spawn_with(&store, "owner", &[b"emit"]).await?;
    let first = spawn_with(&store, "dependent-first", &[b"emit"]).await?;
    let second = spawn_with(&store, "dependent-second", &[b"emit"]).await?;
    let unrelated = spawn_with(&store, "unrelated", &[b"emit"]).await?;
    let handler = Arc::new(OwnedGated {
        gated: Gated::new()?,
        owner: owner.clone(),
        refused: std::sync::Mutex::new(Vec::new()),
    });
    let hosted = Arc::new(crate::guest::HostedHandler::new(
        handler.clone(),
        handler.clone(),
        |_: &ActorReference| true,
    ));
    let runtime = Runtime::new(
        crate::guest::Requests::default(),
        store,
        hosted,
        Arc::new(SystemClock),
    );
    let observed = runtime.unavailable(&owner)?;
    for actor in [&first, &second] {
        handler.refuse(actor, true);
        runtime.host(actor.clone())?;
        assert!(runtime.drain_actor(actor).await.is_err());
        handler.refuse(actor, false);
    }
    assert!(observed.await.to_string().contains("dependent-"));
    let unpolled = runtime.unavailable(&owner)?;
    // A healthy owner cannot erase either failed dependency.
    for actor in [&owner, &first] {
        runtime.host(actor.clone())?;
        handler.gated.gate.add_permits(1);
        runtime.drain_actor(actor).await?;
    }
    assert!(
        runtime
            .unavailable(&owner)?
            .await
            .to_string()
            .contains("dependent-second")
    );
    // Hibernation and recreation cannot lose the source-specific recovery key.
    assert!(runtime.release(&second));
    runtime.host(second.clone())?;
    handler.gated.gate.add_permits(1);
    runtime.drain_actor(&second).await?;
    assert!(unpolled.await.to_string().contains("dependent-"));
    let mut healthy = runtime.unavailable(&owner)?;
    assert!(futures::poll!(healthy.as_mut()).is_pending());
    let unrelated_failure = runtime.unavailable(&unrelated)?;
    handler.refuse(&unrelated, true);
    runtime.host(unrelated.clone())?;
    assert!(runtime.drain_actor(&unrelated).await.is_err());
    assert!(unrelated_failure.await.to_string().contains("unrelated"));
    assert!(futures::poll!(healthy.as_mut()).is_pending());
    Ok(())
}

/// The producer's receipt and the dispatcher's delayed follow are one event.
/// A fresh committed activation is the control that still permits recovery.
#[tokio::test]
async fn delayed_activation_cannot_redeliver_a_failed_attempt() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let handler = Arc::new(Gated::new()?);
    let runtime = Runtime::new(
        crate::guest::Requests::default(),
        store.clone(),
        handler.clone(),
        Arc::new(SystemClock),
    );
    let address = spawn_with(&store, "one-activation", &[b"emit"]).await?;
    runtime.track(address.clone())?;
    for recover in [false, true] {
        let request = CommitRequest {
            deadline_unix_millis: None,
            effects: vec![store.activation(&address)?],
            idempotency_key: key(if recover { "recover" } else { "deliver" })?,
        };
        let outcome = provider.commit(request).await?;
        let acyclic_stream::CommitOutcome::Committed(receipt) = outcome else {
            return Err(format!("activation did not commit: {outcome:?}").into());
        };
        runtime.wake_committed(&address, &receipt)?;
        let work = runtime.ready[0].take().ok_or("activation did not wake")?;
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
            runtime.ready[0].take().is_none(),
            "duplicate activation queued another delivery"
        );
    }
    assert_eq!(records(&*provider, "out/one-activation").await?.len(), 1);
    Ok(())
}

/// Availability is an actor event, not an attempt event: a cancelled attempt
/// keeps its input for the next one and resolves no observer, while a
/// definitive failure reaches observers registered before hibernation.
#[tokio::test]
async fn unavailability_observers_survive_hibernation_and_cancelled_attempts() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let handler = Arc::new(Gated::new()?);
    let runtime = Runtime::new(
        crate::guest::Requests::default(),
        store.clone(),
        handler.clone(),
        Arc::new(SystemClock),
    );
    let address = spawn_with(&store, "failure-observed", &[b"emit"]).await?;
    let mut failed = runtime.unavailable(&address)?;
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
    // The actor's next event retries the retained input. The cancelled
    // attempt made the actor unavailable to no observer, old or new.
    assert!(runtime.release(&address));
    runtime.host(address.clone())?;
    handler.gate.add_permits(1);
    runtime.drain_actor(&address).await?;
    assert!(
        futures::poll!(failed.as_mut()).is_pending(),
        "a cancelled attempt made its actor unavailable"
    );
    let mut next = runtime.unavailable(&address)?;
    assert!(
        futures::poll!(next.as_mut()).is_pending(),
        "recovery retained a stale unavailability"
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
    assert!(
        failed.await.to_string().contains("test gate closed"),
        "hibernation lost its earliest observer"
    );
    let mut stopping = runtime.unavailable(&actor("healthy-runtime-shutdown")?)?;
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
                runtime.admission[0]
                    .as_ref()
                    .expect("reserved work credit")
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
            runtime.admission[0]
                .as_ref()
                .expect("reserved work credit")
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
            runtime.admission[0]
                .as_ref()
                .expect("reserved work credit")
                .available_permits(),
            active
        );

        // Closed runtime credit is permanent; retrying cannot reopen it. The
        // original acquisition error settles availability without consuming input.
        let address = spawn_with(&store, "closed-admission", &[b"emit"]).await?;
        runtime.host(address.clone())?;
        let failed = runtime.unavailable(&address)?;
        runtime.admission[0]
            .as_ref()
            .expect("reserved work credit")
            .close();
        let error = runtime
            .drain_actor(&address)
            .await
            .expect_err("closed credit admitted work");
        assert!(matches!(error, Error::AdmissionClosed(_)));
        assert!(!error.retryable());
        assert!(
            std::error::Error::source(&error)
                .and_then(|source| source.downcast_ref::<tokio::sync::AcquireError>())
                .is_some()
        );
        assert!(
            failed
                .await
                .to_string()
                .contains("runtime admission is closed")
        );
        assert_eq!(
            store
                .load(&address)
                .await?
                .state()
                .and_then(|state| state.cursor("inbox")),
            Some(0),
            "closed admission consumed retained input"
        );
    }
    Ok(())
}
