use super::*;

async fn input_aliases<P: StreamProvider>(provider: P) -> TestResult {
    let provider = Arc::new(provider);
    let store = Store::new(provider.clone(), root()?);
    let address = actor("input-alias")?;
    let input = StreamPath::new("owned/input")?;
    let mut creation = PreparedPublish::foreign(key("create-input-alias")?);
    creation.join(
        store.create(
            None,
            &address,
            crate::local::Creation {
                checkpoint: Some(ledger("out/input-alias")?),
                subscriptions: ["inbox", "alias"]
                    .into_iter()
                    .map(|id| {
                        (
                            id.into(),
                            Subscription {
                                path: input.clone(),
                                cursor: 0,
                            },
                        )
                    })
                    .collect(),
                ..crate::local::Creation::default()
            },
        )?,
    )?;
    creation.join(store.activation(&address)?)?;
    store.submit(creation).await?;
    let runtime = Runtime::new(
        crate::guest::Requests::default(),
        store,
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    );
    runtime.track(address.clone())?;
    runtime.drain_actor(&address).await?;
    assert!(
        runtime.subscriptions.lock().unwrap().paths.is_empty(),
        "an inbox alias opened a redundant follow"
    );
    assert!(
        runtime
            .store()
            .interest_holders(runtime.store().scheduling_shard(&address), None)
            .await?
            .0
            .is_empty(),
        "an inbox alias created a restore marker"
    );

    // Negative control: a genuinely external interest still needs its source
    // follow, because its producer does not publish this actor's activation.
    let (_, external, observed) = observers(&provider, 1).await?;
    runtime.track(external[0].clone())?;
    runtime.drain_actor(&external[0]).await?;
    assert_eq!(runtime.subscriptions.lock().unwrap().paths.len(), 1);
    assert!(
        runtime
            .subscriptions
            .lock()
            .unwrap()
            .paths
            .contains_key(&observed)
    );

    let mut serving =
        Box::pin(runtime.run(|_, error| panic!("subscription delivery failed: {error}")));
    observe_while_running(serving.as_mut(), async {
        runtime.complete().await?;
        Ok(())
    })
    .await?;
    let mut publication = PreparedPublish::foreign(key("activated-input")?);
    publication.join(CommitEffect::hard(
        vec![CommitCondition::Tail {
            path: input.clone(),
            expected: 0,
        }],
        vec![CommitMutation::Append {
            path: input.clone(),
            records: vec!["emit".into()],
        }],
    ))?;
    publication.join(runtime.store().activation(&address)?)?;
    runtime.store().submit(publication).await?;
    observe_while_running(serving.as_mut(), async {
        state_after(runtime.store(), &address, |state| {
            state.cursor("inbox") == Some(1) && state.cursor("alias") == Some(1)
        })
        .await?;
        runtime.complete().await?;
        Ok(())
    })
    .await?;
    assert_eq!(
        records(provider.as_ref(), "out/input-alias").await?.len(),
        2,
        "the alias lost its independent cursor"
    );
    assert!(
        !runtime
            .subscriptions
            .lock()
            .unwrap()
            .paths
            .contains_key(&input)
    );
    provider
        .append(AppendRequest {
            path: observed,
            records: vec!["emit".into()],
            if_tail: None,
            idempotency_key: key("external-input-control")?,
        })
        .await?;
    observe_while_running(serving.as_mut(), async {
        state_after(runtime.store(), &external[0], |state| {
            state.cursor("observed") == Some(1)
        })
        .await?;
        runtime.complete().await?;
        Ok(())
    })
    .await?;
    assert_eq!(records(provider.as_ref(), "out/observer-0").await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn input_aliases_share_activation_without_follow_or_restore_work() -> TestResult {
    input_aliases(MemoryStream::default()).await?;
    let directory = tempfile::tempdir()?;
    input_aliases(LocalStream::open(directory.path(), Default::default()).await?).await
}

async fn observers<P: StreamProvider>(
    provider: &Arc<P>,
    count: usize,
) -> Result<(Store<P>, Vec<ActorReference>, StreamPath), Box<dyn std::error::Error>> {
    let actors = (0..count)
        .map(|index| actor(&format!("observer-{index}")))
        .collect::<Result<Vec<_>, _>>()?;
    observing(provider, actors).await
}

/// Create each of `actors` observing one shared stream it did not declare.
async fn observing<P: StreamProvider>(
    provider: &Arc<P>,
    actors: Vec<ActorReference>,
) -> Result<(Store<P>, Vec<ActorReference>, StreamPath), Box<dyn std::error::Error>> {
    let store = Store::new(Arc::clone(provider), root()?);
    let observed = StreamPath::new("shared/observed")?;
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
                        paused: false,
                        cursor: 0,
                    },
                )]),
                checkpoint: Some(ledger(&format!("out/observer-{index}"))?),
                escrow: None,
            },
        )?)?;
        publication.join(store.activation(address)?)?;
        store.submit(publication).await?;
    }
    Ok((store, actors, observed))
}

async fn shared_dynamic_interests<P: StreamProvider>(provider: P) -> TestResult {
    let provider = Arc::new(provider);
    let (store, actors, observed) = observers(&provider, 2).await?;
    let runtime = Runtime::new(
        crate::guest::Requests::default(),
        store,
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    );
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
        crate::guest::Requests::default(),
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    );
    let mut serving = Box::pin(runtime.run(|_, error| panic!("restored delivery failed: {error}")));
    observe_while_running(serving.as_mut(), async {
        runtime.restored().await?;
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
async fn idle_creation_with_interests_is_restored_after_its_activation_checkpoints() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let (store, actors, observed) = observers(&provider, 1).await?;
    let shard = store.scheduling_shard(&actors[0]);
    let runtime = Runtime::new(
        crate::guest::Requests::default(),
        store,
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    );
    let mut serving = Box::pin(runtime.run(|_, error| panic!("idle delivery failed: {error}")));
    observe_while_running(serving.as_mut(), async {
        runtime.complete().await?;
        Ok(())
    })
    .await?;
    // No publication ran: the idle pass alone made the interests restorable.
    assert_eq!(
        runtime.store().interest_holders(shard, None).await?.0,
        actors
    );
    drop(serving);
    drop(runtime);
    let runtime = Runtime::new(
        crate::guest::Requests::default(),
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    );
    let mut serving = Box::pin(runtime.run(|_, error| panic!("restored delivery failed: {error}")));
    observe_while_running(serving.as_mut(), async {
        runtime.restored().await?;
        runtime.complete().await?;
        Ok(())
    })
    .await?;
    // No activation entry: only the restored interest can wake the actor.
    provider
        .append(AppendRequest {
            path: observed,
            records: vec![Bytes::from_static(b"emit").into()],
            if_tail: None,
            idempotency_key: key("observed-after-restart")?,
        })
        .await?;
    observe_while_running(serving.as_mut(), async {
        state_after(runtime.store(), &actors[0], |state| {
            state.cursor("observed") == Some(1)
        })
        .await?;
        Ok(())
    })
    .await?;
    Ok(())
}

/// The idle marker write has a bounded key, so the longest valid address
/// (128-byte namespace and id) still becomes restorable.
#[tokio::test]
async fn idle_marker_of_the_longest_valid_address_is_written() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let long = ActorReference::new(b"account", &[b'n'; 128], &[b'i'; 128])?;
    let (store, actors, _) = observing(&provider, vec![long]).await?;
    let shard = store.scheduling_shard(&actors[0]);
    let runtime = Runtime::new(
        crate::guest::Requests::default(),
        store,
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    );
    let mut serving = Box::pin(runtime.run(|_, error| panic!("idle delivery failed: {error}")));
    observe_while_running(serving.as_mut(), async {
        runtime.complete().await?;
        Ok(())
    })
    .await?;
    assert_eq!(
        runtime.store().interest_holders(shard, None).await?.0,
        actors
    );
    Ok(())
}

/// Holds every delivery in flight forever: a continuously backlogged host.
struct Stalls(Package);
impl Handler for Stalls {
    fn manifest(&self) -> &Manifest {
        self.0.manifest()
    }
    fn on_record<'a>(
        &'a self,
        _delivery: Delivery,
        _checkpoint: Option<&'a Checkpoint>,
        _cancellation: crate::guest::CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        futures::future::pending().boxed()
    }
}

/// Restoration has its own credit: interest holders on later pages are
/// restored while earlier ones keep delivery work in flight. With paused
/// time, a restore gated on quiescence could finish only after the manifest
/// timeout fails a stalled delivery.
#[tokio::test(start_paused = true)]
async fn restoration_completes_while_restored_actors_stay_backlogged() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    // Two holders on different shards, so restoring them takes two pages.
    let first = actor("observer-0")?;
    let second = (1..)
        .map(|index| actor(&format!("observer-{index}")))
        .find(|candidate| {
            candidate.as_ref().is_ok_and(|candidate| {
                store.scheduling_shard(candidate) != store.scheduling_shard(&first)
            })
        })
        .ok_or("no second shard")??;
    let (store, actors, observed) = observing(&provider, vec![first, second]).await?;
    let runtime = Runtime::new(
        crate::guest::Requests::default(),
        store,
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    );
    let mut serving = Box::pin(runtime.run(|_, error| panic!("idle delivery failed: {error}")));
    observe_while_running(serving.as_mut(), async {
        runtime.complete().await?;
        Ok(())
    })
    .await?;
    for address in &actors {
        let shard = runtime.store().scheduling_shard(address);
        assert!(
            runtime
                .store()
                .interest_holders(shard, None)
                .await?
                .0
                .contains(address)
        );
    }
    drop(serving);
    drop(runtime);
    // No activation entry: each restored holder finds input and stalls on it.
    provider
        .append(AppendRequest {
            path: observed,
            records: vec![Bytes::from_static(b"emit").into()],
            if_tail: None,
            idempotency_key: key("backlog")?,
        })
        .await?;
    let runtime = Runtime::new(
        crate::guest::Requests::default(),
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Stalls(Package::new()?)),
        Arc::new(SystemClock),
    );
    let failed = std::sync::Mutex::new(Vec::new());
    let mut serving = Box::pin(runtime.run(|actor, _| failed.lock().unwrap().push(actor.clone())));
    observe_while_running(serving.as_mut(), async {
        runtime.restored().await?;
        Ok(())
    })
    .await?;
    assert!(
        failed.lock().unwrap().is_empty(),
        "restoration waited for a stalled delivery to fail"
    );
    for address in &actors {
        assert!(
            runtime.shard(address).lock().unwrap().contains_key(address),
            "every holder was restored"
        );
    }
    Ok(())
}

#[tokio::test]
async fn source_failure_and_fanout_remain_bounded_and_recover_without_stopping_other_actors()
-> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let (store, actors, observed) = observers(&provider, 5).await?;
    let runtime = Runtime::new(
        crate::guest::Requests::default(),
        store,
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    )
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
    let mut failures = actors
        .iter()
        .map(|actor| runtime.unavailable(actor))
        .collect::<Result<Vec<_>, _>>()?;
    let mut healthy = runtime.unavailable(&actor("unrelated")?)?;
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
    for failed in &mut failures {
        assert!(
            futures::poll!(failed.as_mut()).is_ready(),
            "a failed subscription left its actor's waiter pending"
        );
    }
    let mut retained = runtime.unavailable(&actors[0])?;
    assert!(
        futures::poll!(retained.as_mut()).is_ready(),
        "a resident actor lost its subscription failure"
    );
    assert!(
        futures::poll!(healthy.as_mut()).is_pending(),
        "a subscription failure notified an unrelated actor"
    );
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

use std::sync::atomic::AtomicUsize;

struct ApprovalPause(Package, AtomicUsize, bool, bool);
impl Handler for ApprovalPause {
    fn manifest(&self) -> &Manifest {
        self.0.manifest()
    }
    fn subscription_key(
        &self,
        id: &str,
        _: Option<&Checkpoint>,
    ) -> Result<Option<Bytes>, crate::guest::GuestError> {
        Ok((self.3 && id == "source").then(|| Bytes::from_static(b"selected")))
    }
    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        _: crate::guest::CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        Box::pin(async move {
            use crate::guest::SubscriptionChange;
            self.1.fetch_add(1, Ordering::SeqCst);
            let (Trigger::Record { subscription, .. } | Trigger::KeyedRecord { subscription, .. }) =
                &delivery.trigger
            else {
                panic!("unexpected alarm")
            };
            let (state, change, destination, bytes) = match (
                subscription.as_str(),
                checkpoint.map(|value| value.bytes.as_ref()),
            ) {
                ("source", None) => (
                    b"pending".as_slice(),
                    Some(SubscriptionChange::Pause {
                        id: "source".into(),
                    }),
                    "pause/requests",
                    Bytes::from_static(b"approval"),
                ),
                ("approval", Some(b"pending")) => (
                    b"approved".as_slice(),
                    (!self.2).then(|| SubscriptionChange::Resume {
                        id: "source".into(),
                    }),
                    "pause/approvals",
                    Bytes::from_static(b"approved"),
                ),
                ("source", Some(b"approved")) => (
                    b"complete".as_slice(),
                    None,
                    "pause/effects",
                    Bytes::copy_from_slice(&delivery.record.as_ref().expect("source record").value),
                ),
                _ => panic!("suspended input was delivered: {subscription}"),
            };
            Ok(Transition {
                pause: false,
                response: None,
                checkpoint: Checkpoint {
                    codec: self.manifest().codec.clone(),
                    bytes: Bytes::copy_from_slice(state),
                },
                alarm: AlarmChange::Keep,
                subscriptions: change.into_iter().collect(),
                effects: vec![acyclic_stream::CommitEffect::hard(
                    vec![],
                    vec![CommitMutation::Append {
                        path: StreamPath::new(destination).expect("valid destination"),
                        records: vec![bytes.into()],
                    }],
                )],
                participant: None,
            })
        })
    }
}

async fn pause_retains_input<P: StreamProvider + 'static>(
    provider: P,
    external_resume: bool,
    keyed: bool,
) -> TestResult {
    let provider = Arc::new(provider);
    let store = Store::new(Arc::clone(&provider), root()?);
    let address = actor("paused-source")?;
    let source = StreamPath::new("pause/source")?;
    let approval = StreamPath::new("pause/approval")?;
    let mut create = PreparedPublish::foreign(key("pause/create")?);
    create.join(store.create(
        None,
        &address,
        crate::local::Creation {
            subscriptions: BTreeMap::from([
                (
                    "source".into(),
                    Subscription {
                        path: source.clone(),
                        cursor: 0,
                        paused: false,
                    },
                ),
                (
                    "approval".into(),
                    Subscription {
                        path: approval.clone(),
                        cursor: 0,
                        paused: false,
                    },
                ),
            ]),
            checkpoint: None,
            escrow: None,
        },
    )?)?;
    create.join(store.activation(&address)?)?;
    create.join(acyclic_stream::CommitEffect::hard(
        vec![],
        vec![CommitMutation::Append {
            path: source.clone(),
            records: if keyed {
                vec![
                    Bytes::from_static(b"unrelated").into(),
                    acyclic_stream::AppendRecord {
                        value: Bytes::from_static(b"original").into(),
                        headers: acyclic_stream::RecordHeaders::new(
                            vec![Bytes::from_static(b"selected").into()],
                            vec![],
                        )?,
                    },
                ]
            } else {
                vec![Bytes::from_static(b"original").into()]
            },
        }],
    ))?;
    store.submit(create).await?;
    let handler = Arc::new(ApprovalPause(
        Package::new()?,
        AtomicUsize::new(0),
        external_resume,
        keyed,
    ));
    let runtime = Runtime::new(
        crate::guest::Requests::default(),
        store,
        handler.clone(),
        Arc::new(SystemClock),
    );
    runtime.track(address.clone())?;
    runtime.drain_actor(&address).await?;
    let paused = runtime.store().load(&address).await?;
    let state = paused.state().ok_or("missing paused state")?;
    assert_eq!(state.cursor("source"), Some(0));
    assert!(state.subscriptions()["source"].paused);
    assert_eq!(state.checkpoint().unwrap().bytes.as_ref(), b"pending");
    assert_eq!(
        records(&*provider, "pause/requests").await?,
        vec![Bytes::from_static(b"approval")]
    );
    assert!(records(&*provider, "pause/effects").await?.is_empty());
    assert!(
        !runtime
            .subscriptions
            .lock()
            .unwrap()
            .paths
            .contains_key(&source)
    );
    let child = actor("paused-child")?;
    let mut subscriptions = state.subscriptions().clone();
    subscriptions.get_mut("source").unwrap().paused = false;
    let mut fork = PreparedPublish::foreign(key("pause/fork")?);
    fork.join(
        runtime
            .store()
            .branch(&paused, &child, Branch::unfunded(subscriptions))?,
    )?;
    runtime.store().submit(fork).await?;
    let inherited = runtime.store().load(&child).await?;
    assert!(inherited.state().unwrap().subscriptions()["source"].paused);
    assert_eq!(inherited.state().unwrap().cursor("source"), Some(0));
    runtime.drain_actor(&address).await?;
    assert_eq!(handler.1.load(Ordering::SeqCst), 1);
    drop(runtime);
    let runtime = Runtime::new(
        crate::guest::Requests::default(),
        Store::new(Arc::clone(&provider), root()?),
        handler.clone(),
        Arc::new(SystemClock),
    );
    runtime.track(address.clone())?;
    runtime.drain_actor(&address).await?;
    assert_eq!(handler.1.load(Ordering::SeqCst), 1);
    provider
        .append(AppendRequest {
            path: approval,
            records: vec![Bytes::from_static(b"allow").into()],
            if_tail: None,
            idempotency_key: key("pause/allow")?,
        })
        .await?;
    runtime.drain_actor(&address).await?;
    if external_resume {
        let awaiting = runtime.store().load(&address).await?;
        let state = awaiting.state().ok_or("missing approved state")?;
        assert_eq!(state.cursor("source"), Some(0));
        assert!(state.subscriptions()["source"].paused);
        assert_eq!(state.checkpoint().unwrap().bytes.as_ref(), b"approved");
        assert!(records(&*provider, "pause/effects").await?.is_empty());
        let resume = crate::wire::ResumeSubscriptionRequest {
            namespace: Some(address.namespace().as_str().into()),
            account_id: address.account().as_bytes().to_vec(),
            actor_id: address.id().into(),
            subscription_id: "source".into(),
            idempotency_key: "resume-approved".into(),
        };
        runtime.store().resume_subscription(&[], &resume).await?;
        runtime.drain_actor(&address).await?;
        let completed = runtime.store().load(&address).await?;
        runtime.store().resume_subscription(&[], &resume).await?;
        assert_eq!(runtime.store().load(&address).await?.tail, completed.tail);
    }
    let settled = runtime.store().load(&address).await?;
    let state = settled.state().ok_or("missing resumed state")?;
    assert_eq!(
        (state.cursor("source"), state.cursor("approval")),
        (Some(if keyed { 2 } else { 1 }), Some(1))
    );
    assert!(!state.subscriptions()["source"].paused);
    assert_eq!(state.checkpoint().unwrap().bytes.as_ref(), b"complete");
    assert_eq!(
        records(&*provider, "pause/effects").await?,
        vec![Bytes::from_static(b"original")]
    );
    assert_eq!(handler.1.load(Ordering::SeqCst), 3);
    runtime.drain_actor(&address).await?;
    assert_eq!(handler.1.load(Ordering::SeqCst), 3);
    Ok(())
}

#[tokio::test]
async fn paused_inputs_keep_their_cursor_across_restart_and_resume_from_another_input() -> TestResult
{
    for (external, keyed) in [(false, false), (true, false), (false, true), (true, true)] {
        pause_retains_input(MemoryStream::default(), external, keyed).await?;
        let directory = tempfile::tempdir()?;
        pause_retains_input(
            LocalStream::open(directory.path(), Default::default()).await?,
            external,
            keyed,
        )
        .await?;
    }
    Ok(())
}
