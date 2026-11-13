use super::*;

async fn observers<P: StreamProvider>(
    provider: &Arc<P>,
    count: usize,
) -> Result<(Store<P>, Vec<ActorReference>, StreamPath), Box<dyn std::error::Error>> {
    let store = Store::new(Arc::clone(provider), root()?);
    let observed = StreamPath::new("shared/observed")?;
    let actors = (0..count)
        .map(|index| actor(&format!("observer-{index}")))
        .collect::<Result<Vec<_>, _>>()?;
    for (index, address) in actors.iter().enumerate() {
        let mut publication = PreparedPublish::foreign(key(&format!("create-observer-{index}"))?);
        publication.join(store.create(
            None,
            address,
            crate::local::Creation {
                subscriptions: BTreeMap::from([(
                    "observed".into(),
                    Subscription {
                        path: observed.clone(),
                        cursor: 0,
                    },
                )]),
                checkpoint: Some(ledger(&format!("out/observer-{index}"))?),
                escrow: None,
            },
        )?)?;
        publication.join(store.activation(address))?;
        store.submit(publication).await?;
    }
    Ok((store, actors, observed))
}

async fn shared_dynamic_interests<P: StreamProvider>(provider: P) -> TestResult {
    let provider = Arc::new(provider);
    let (store, actors, observed) = observers(&provider, 2).await?;
    let runtime = Runtime::new(store, Arc::new(Package::new()?), Arc::new(SystemClock));
    let retained = Arc::strong_count(&provider);
    for address in &actors {
        runtime.track(address.clone())?;
        runtime.drain_actor(address).await?;
    }
    assert_eq!(
        Arc::strong_count(&provider),
        retained + 1,
        "pending openings must share one provider hold"
    );
    for address in &actors {
        assert!(runtime.release(address));
    }
    assert_eq!(
        Arc::strong_count(&provider),
        retained,
        "the last release must drop its I/O without another dispatcher poll"
    );
    let mut serving = Box::pin(runtime.run(|_, error| panic!("dynamic delivery failed: {error}")));
    observe_while_running(serving.as_mut(), async {
        runtime.complete().await?;
        Ok(())
    })
    .await?;
    assert_eq!(
        runtime.subscriptions.lock().unwrap().paths.len(),
        1,
        "observers retain one shared event source, even before stream creation"
    );
    // No activation entry: the writer knows neither observer.
    provider
        .append(AppendRequest {
            path: observed.clone(),
            records: vec![Bytes::from_static(b"emit").into()],
            if_tail: None,
            idempotency_key: key("first-observed-record")?,
        })
        .await?;
    observe_while_running(serving.as_mut(), async {
        for address in &actors {
            state_after(runtime.store(), address, |state| {
                state.cursor("observed") == Some(1)
            })
            .await?;
        }
        runtime.complete().await?;
        Ok(())
    })
    .await?;
    assert!(runtime.release(&actors[0]));
    provider
        .append(AppendRequest {
            path: observed.clone(),
            records: vec![Bytes::from_static(b"emit").into()],
            if_tail: None,
            idempotency_key: key("second-observed-record")?,
        })
        .await?;
    observe_while_running(serving.as_mut(), async {
        state_after(runtime.store(), &actors[1], |state| {
            state.cursor("observed") == Some(2)
        })
        .await?;
        runtime.complete().await?;
        Ok(())
    })
    .await?;
    assert_eq!(records(&*provider, "out/observer-0").await?.len(), 1);
    assert_eq!(records(&*provider, "out/observer-1").await?.len(), 2);
    drop(serving);
    drop(runtime);
    // Restart reconstructs interests from their original durable cursors.
    let runtime = Runtime::new(
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    );
    let mut serving = Box::pin(runtime.run(|_, error| panic!("restored delivery failed: {error}")));
    observe_while_running(serving.as_mut(), async {
        runtime.complete().await?;
        Ok(())
    })
    .await?;
    for index in 0..2 {
        assert_eq!(
            records(&*provider, &format!("out/observer-{index}"))
                .await?
                .len(),
            2
        );
    }
    for address in &actors {
        assert!(runtime.release(address));
    }
    assert!(runtime.subscriptions.lock().unwrap().paths.is_empty());
    Ok(())
}

#[tokio::test]
async fn dynamic_interests_share_events_and_restore_original_cursors() -> TestResult {
    shared_dynamic_interests(MemoryStream::default()).await?;
    let directory = tempfile::tempdir()?;
    shared_dynamic_interests(LocalStream::open(directory.path(), Default::default()).await?).await
}

#[tokio::test]
async fn source_failure_and_fanout_remain_bounded_and_recover_without_stopping_other_actors()
-> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let (store, actors, observed) = observers(&provider, 5).await?;
    let runtime = Runtime::new(store, Arc::new(Package::new()?), Arc::new(SystemClock))
        .with_limits(RuntimeLimits::new(1, 1)?);
    let reported = Arc::new(std::sync::Mutex::new(std::collections::BTreeSet::new()));
    let (first, first_report) = tokio::sync::oneshot::channel();
    let first = std::sync::Mutex::new(Some(first));
    let errors = Arc::clone(&reported);
    let mut serving = Box::pin(runtime.run(move |actor, _| {
        errors.lock().unwrap().insert(actor.clone());
        if let Some(first) = first.lock().unwrap().take() {
            let _ = first.send(());
        }
    }));
    observe_while_running(serving.as_mut(), async {
        runtime.complete().await?;
        Ok(())
    })
    .await?;
    provider
        .append(AppendRequest {
            path: observed.clone(),
            records: vec![Bytes::from_static(b"emit").into()],
            if_tail: None,
            idempotency_key: key("bounded-source-record")?,
        })
        .await?;
    // Drive exactly the committed source event, without delivering the actors it wakes.
    poll_fn(|context| runtime.poll_subscriptions(context))
        .await
        .map_err(|_| "unexpected source failure")?;
    assert_eq!(
        runtime.counts().1,
        1,
        "one source step exceeded its work credit"
    );
    observe_while_running(serving.as_mut(), async {
        for address in &actors {
            state_after(runtime.store(), address, |state| {
                state.cursor("observed") == Some(1)
            })
            .await?;
        }
        runtime.complete().await?;
        Ok(())
    })
    .await?;
    {
        let mut subscriptions = runtime.subscriptions.lock().unwrap();
        let source = subscriptions
            .paths
            .get_mut(&observed)
            .ok_or("source missing")?;
        source.next = Some(Box::pin(async { Err(StreamError::Unavailable) }));
        source.waker.wake_by_ref();
    }
    observe_while_running(serving.as_mut(), async {
        first_report.await?;
        Ok(())
    })
    .await?;
    assert_eq!(
        reported.lock().unwrap().len(),
        1,
        "failure fanout exceeded its work credit"
    );
    observe_while_running(serving.as_mut(), async {
        assert!(matches!(
            runtime.complete().await,
            Err(Error::CompletionFailed(_))
        ));
        Ok(())
    })
    .await?;
    assert_eq!(reported.lock().unwrap().len(), actors.len());
    let unrelated = spawn_with(runtime.store(), "unrelated", &[b"emit"]).await?;
    observe_while_running(serving.as_mut(), async {
        state_after(runtime.store(), &unrelated, |state| {
            state.cursor("inbox") == Some(1)
        })
        .await?;
        Ok(())
    })
    .await?;
    assert_eq!(records(&*provider, "out/unrelated").await?.len(), 1);
    provider
        .append(AppendRequest {
            path: observed.clone(),
            records: vec![Bytes::from_static(b"emit").into()],
            if_tail: None,
            idempotency_key: key("record-while-source-failed")?,
        })
        .await?;
    runtime.wake(&actors[0]);
    observe_while_running(serving.as_mut(), async {
        for address in &actors {
            state_after(runtime.store(), address, |state| {
                state.cursor("observed") == Some(2)
            })
            .await?;
        }
        Ok(())
    })
    .await?;
    for index in 0..actors.len() {
        assert_eq!(
            records(&*provider, &format!("out/observer-{index}"))
                .await?
                .len(),
            2
        );
    }
    Ok(())
}
