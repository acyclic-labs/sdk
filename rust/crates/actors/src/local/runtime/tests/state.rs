use super::*;
use crate::guest::{
    ActorMetadataWrite, ActorStateAlarm, ActorStateCommit, ActorStateHost, GuestError, Requests,
};

struct Flushes {
    manifest: Manifest,
    requests: Requests,
    calls: AtomicU64,
    retained: std::sync::Mutex<Option<ActorStateHost>>,
    entered: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<crate::guest::RequestId>>>,
    gate: Option<Arc<tokio::sync::Semaphore>>,
}

impl Handler for Flushes {
    fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        _: Option<&'a Checkpoint>,
        _: crate::guest::CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        Box::pin(async move {
            assert_eq!(
                self.calls.fetch_add(1, Ordering::SeqCst),
                0,
                "state publication re-entered the handler"
            );
            if let Some(entered) = self.entered.lock().unwrap().take() {
                let _ = entered.send(delivery.request);
            }
            if let Some(gate) = &self.gate {
                gate.acquire()
                    .await
                    .map_err(|error| crate::guest::GuestError::Handler(error.to_string()))?
                    .forget();
            }
            let state = self.requests.state(delivery.request)?;
            *self.retained.lock().unwrap() = Some(state.clone());
            assert_eq!(state.seed().await?.journal_tail, "0");
            state
                .commit(ActorStateCommit {
                    writes: vec![ActorMetadataWrite {
                        key: "value".into(),
                        value: Bytes::from_static(b"first"),
                    }],
                    deletions: Vec::new(),
                    journal_from: "0".into(),
                    journal: vec![Bytes::from_static(b"one")],
                    alarm: ActorStateAlarm::Set {
                        deadline: "500".into(),
                    },
                })
                .await?;
            assert_eq!(
                state.read_journal(0, 10).await?,
                vec![Bytes::from_static(b"one")]
            );
            state
                .stage(ActorStateCommit {
                    writes: vec![ActorMetadataWrite {
                        key: "value".into(),
                        value: Bytes::from_static(b"second"),
                    }],
                    deletions: Vec::new(),
                    journal_from: "1".into(),
                    journal: vec![Bytes::from_static(b"two")],
                    alarm: ActorStateAlarm::Set {
                        deadline: "1000".into(),
                    },
                })
                .await?;
            Ok(Transition {
                response: None,
                checkpoint: Checkpoint {
                    codec: self.manifest.codec.clone(),
                    bytes: Bytes::from_static(b"finished"),
                },
                alarm: AlarmChange::Keep,
                pause: false,
                effects: Vec::new(),
                participant: None,
                subscriptions: Vec::new(),
            })
        })
    }
}

struct HoldFinal {
    inner: Flushes,
    entered: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    finish: tokio::sync::Semaphore,
}

impl Handler for HoldFinal {
    fn manifest(&self) -> &Manifest {
        self.inner.manifest()
    }

    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        cancellation: crate::guest::CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        Box::pin(async move {
            let transition = self
                .inner
                .on_record(delivery, checkpoint, cancellation)
                .await?;
            let _ = self.entered.lock().unwrap().take().unwrap().send(());
            self.finish.acquire().await.unwrap().forget();
            Ok(transition)
        })
    }
}

#[tokio::test]
async fn final_state_packet_shares_the_delivery_fence_and_preserves_flushed_prefix() -> TestResult {
    for deposed in [false, true] {
        let store = Store::new(Arc::new(MemoryStream::default()), root()?);
        let requests = Requests::default();
        let mut manifest = crate::guest::wire::decode_manifest(
            &crate::guest::wire::encode_manifest(Package::new()?.manifest()),
        )?;
        manifest
            .bindings
            .push(crate::domain::Binding::actor_state("STATE".into())?);
        let (entered, observing) = tokio::sync::oneshot::channel();
        let handler = Arc::new(HoldFinal {
            inner: Flushes {
                manifest,
                requests: requests.clone(),
                calls: AtomicU64::new(0),
                retained: std::sync::Mutex::new(None),
                entered: std::sync::Mutex::new(None),
                gate: None,
            },
            entered: std::sync::Mutex::new(Some(entered)),
            finish: tokio::sync::Semaphore::new(0),
        });
        let runtime = Runtime::new(
            requests,
            store.clone(),
            handler.clone(),
            Arc::new(Manual(AtomicU64::new(100))),
        );
        let address = spawn_with(&store, "joined-state", &[b"emit"]).await?;
        runtime.place(address.clone(), 1).await?;
        let delivery = runtime.drain_event(&address);
        futures::pin_mut!(delivery);
        match futures::future::select(delivery.as_mut(), observing).await {
            futures::future::Either::Right((entered, _)) => entered?,
            futures::future::Either::Left((result, _)) => {
                return Err(format!("ended before stage: {result:?}").into());
            }
        }
        let prefix = store.load(&address).await?;
        let state = prefix.state().ok_or("missing flushed prefix")?;
        assert_eq!(state.cursor("inbox"), Some(0));
        assert_eq!(state.journal_tail, 1);
        assert_eq!(
            state.metadata.get("value"),
            Some(&Bytes::from_static(b"first"))
        );
        assert!(state.checkpoint().is_none());
        assert_eq!(state.alarm().pending(), Some(500));
        if deposed {
            let successor = Runtime::new(
                Requests::default(),
                store.clone(),
                handler.clone(),
                Arc::new(Manual(AtomicU64::new(100))),
            );
            successor.place(address.clone(), 2).await?;
        }
        handler.finish.add_permits(1);
        let delivered = delivery.await;
        assert_eq!(delivered.is_err(), deposed, "{delivered:?}");
        let cut = store.load(&address).await?;
        let state = cut.state().ok_or("missing final cut")?;
        assert_eq!(state.cursor("inbox"), Some(u64::from(!deposed)));
        assert_eq!(state.journal_tail, if deposed { 1 } else { 2 });
        assert_eq!(
            state.metadata.get("value"),
            Some(&Bytes::from_static(if deposed {
                b"first"
            } else {
                b"second"
            }))
        );
        assert_eq!(state.checkpoint().is_some(), !deposed);
        assert_eq!(
            state.alarm().pending(),
            Some(if deposed { 500 } else { 1000 })
        );
    }
    Ok(())
}

#[tokio::test]
async fn stale_epoch_flush_is_a_typed_revocation_and_publishes_nothing() -> TestResult {
    let store = Store::new(Arc::new(MemoryStream::default()), root()?);
    let requests = Requests::default();
    let mut manifest = crate::guest::wire::decode_manifest(&crate::guest::wire::encode_manifest(
        Package::new()?.manifest(),
    ))?;
    manifest
        .bindings
        .push(crate::domain::Binding::actor_state("STATE".into())?);
    let (entered, observing) = tokio::sync::oneshot::channel();
    let handler = Arc::new(Flushes {
        manifest,
        requests: requests.clone(),
        calls: AtomicU64::new(0),
        retained: std::sync::Mutex::new(None),
        entered: std::sync::Mutex::new(Some(entered)),
        gate: Some(Arc::new(tokio::sync::Semaphore::new(0))),
    });
    let runtime = Runtime::new(
        requests.clone(),
        store.clone(),
        handler.clone(),
        Arc::new(SystemClock),
    );
    let address = spawn_with(&store, "stale-state", &[b"emit"]).await?;
    runtime.place(address.clone(), 1).await?;
    let delivery = runtime.drain_event(&address);
    futures::pin_mut!(delivery);
    let request = match futures::future::select(delivery.as_mut(), observing).await {
        futures::future::Either::Right((request, _)) => request?,
        futures::future::Either::Left((result, _)) => {
            return Err(format!("ended before admission: {result:?}").into());
        }
    };
    let owner = requests.state(request)?;
    let successor = Runtime::new(
        Requests::default(),
        store.clone(),
        handler,
        Arc::new(SystemClock),
    );
    successor.place(address.clone(), 2).await?;
    let flushing = owner.commit(ActorStateCommit {
        writes: vec![ActorMetadataWrite {
            key: "stale".into(),
            value: Bytes::from_static(b"forbidden"),
        }],
        deletions: Vec::new(),
        journal_from: "0".into(),
        journal: vec![Bytes::from_static(b"forbidden")],
        alarm: ActorStateAlarm::Set {
            deadline: "1000".into(),
        },
    });
    let (delivered, flushed) = futures::join!(delivery, flushing);
    assert!(delivered.is_err());
    assert_eq!(flushed, Err(GuestError::Revoked));
    let cut = store.load(&address).await?;
    let state = cut.state().ok_or("missing current owner cut")?;
    assert_eq!(state.epoch(), 2);
    assert_eq!(state.cursor("inbox"), Some(0));
    assert_eq!(state.journal_tail, 0);
    assert!(state.metadata.is_empty());
    assert!(state.alarm().pending().is_none());
    Ok(())
}

#[tokio::test]
async fn state_flush_advances_the_delivery_cut_without_reentering_the_handler() -> TestResult {
    for alarm_delivery in [false, true] {
        let provider = Arc::new(MemoryStream::default());
        let store = Store::new(provider.clone(), root()?);
        let address = actor("state-owner")?;
        let input = StreamPath::new("state/input")?;
        let requests = Requests::default();
        let mut manifest = crate::guest::wire::decode_manifest(
            &crate::guest::wire::encode_manifest(Package::new()?.manifest()),
        )?;
        manifest
            .bindings
            .push(crate::domain::Binding::actor_state("STATE".into())?);
        let subscription = manifest.subscriptions[0].clone();
        let handler = Arc::new(Flushes {
            manifest,
            requests: requests.clone(),
            calls: AtomicU64::new(0),
            retained: std::sync::Mutex::new(None),
            entered: std::sync::Mutex::new(None),
            gate: None,
        });
        let runtime = Runtime::new(
            requests,
            store.clone(),
            handler.clone(),
            Arc::new(Manual(AtomicU64::new(100))),
        );
        let mut create = PreparedPublish::foreign(key("state/create")?);
        create.join(
            store.create(
                None,
                &address,
                crate::local::Creation {
                    subscriptions: [(
                        subscription.clone(),
                        Subscription {
                            path: input.clone(),
                            cursor: 0,
                            paused: false,
                        },
                    )]
                    .into(),
                    ..crate::local::Creation::default()
                },
            )?,
        )?;
        create.join(store.activation(&address)?)?;
        if !alarm_delivery {
            create.join(CommitEffect::hard(
                Vec::new(),
                vec![CommitMutation::Append {
                    path: input,
                    records: vec![Bytes::from_static(b"input").into()],
                }],
            ))?;
        }
        store.submit(create).await?;
        runtime.place(address.clone(), 1).await?;
        if alarm_delivery {
            let snapshot = store.load(&address).await?;
            let mut binding = store.bind_state(snapshot, 1)?;
            binding.transaction(|state| state.alarm(Some(100)))?;
            binding.flush(key("state/alarm")?).await?;
            runtime.wake(&address);
        }
        runtime.track(address.clone())?;
        assert_eq!(runtime.drain_event(&address).await?, Some(1000));
        let snapshot = store.load(&address).await?;
        let state = snapshot.state().ok_or("missing committed state")?;
        assert_eq!(
            state.cursor(&subscription),
            Some(if alarm_delivery { 0 } else { 1 })
        );
        assert_eq!(
            state.metadata().collect::<Vec<_>>(),
            vec![("value", &Bytes::from_static(b"second"))]
        );
        assert_eq!(state.alarm().pending(), Some(1000));
        assert_eq!(
            state
                .checkpoint()
                .map(|checkpoint| checkpoint.bytes.as_ref()),
            Some(b"finished".as_slice())
        );
        assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
        let retained = handler
            .retained
            .lock()
            .unwrap()
            .take()
            .ok_or("missing retained capability")?;
        assert!(matches!(
            retained.seed().await,
            Err(crate::guest::GuestError::Revoked)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn unknown_state_flush_retains_its_publication_without_reentering_the_handler() -> TestResult
{
    let provider = Arc::new(super::pause::Provider::default());
    let store = Store::new(provider.clone(), root()?);
    let requests = Requests::default();
    let mut manifest = crate::guest::wire::decode_manifest(&crate::guest::wire::encode_manifest(
        Package::new()?.manifest(),
    ))?;
    manifest
        .bindings
        .push(crate::domain::Binding::actor_state("STATE".into())?);
    let (entered, observing) = tokio::sync::oneshot::channel();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let handler = Arc::new(Flushes {
        manifest,
        requests: requests.clone(),
        calls: AtomicU64::new(0),
        retained: std::sync::Mutex::new(None),
        entered: std::sync::Mutex::new(Some(entered)),
        gate: Some(gate.clone()),
    });
    let runtime = Runtime::new(
        requests,
        store.clone(),
        handler.clone(),
        Arc::new(SystemClock),
    );
    let address = spawn_with(&store, "uncertain-state", &[b"emit"]).await?;
    runtime.place(address.clone(), 1).await?;
    let delivery = runtime.drain_event(&address);
    let release = async {
        observing.await?;
        let (entered, release) = provider.arm(super::pause::Fault::Unknown);
        gate.add_permits(1);
        entered.await?;
        release
            .send(())
            .map_err(|_| "lost state publication producer")?;
        Ok::<_, Box<dyn std::error::Error>>(())
    };
    let (delivered, released) = futures::join!(delivery, release);
    released?;
    assert!(
        matches!(delivered, Err(Error::Stream(StreamError::Unavailable))),
        "{delivered:?}"
    );
    let slot = runtime
        .slot(&address)
        .ok_or("missing unsettled state owner")?;
    assert!(slot.inner.lock().await.pending.is_some());
    runtime.pause(&address, true).await?;
    assert!(slot.inner.lock().await.pending.is_none());
    let cut = store.load(&address).await?;
    let state = cut.state().ok_or("missing committed state")?;
    assert_eq!(state.cursor("inbox"), Some(0));
    assert_eq!(state.journal_tail, 1);
    assert_eq!(
        state.metadata.get("value"),
        Some(&Bytes::from_static(b"first"))
    );
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    let retained = handler
        .retained
        .lock()
        .unwrap()
        .take()
        .ok_or("missing state capability")?;
    assert!(matches!(
        retained.seed().await,
        Err(crate::guest::GuestError::Revoked)
    ));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn handler_deadline_cancels_reads_and_revokes_calls_but_retains_publication() -> TestResult {
    for journal_read in [false, true] {
        let provider = Arc::new(super::pause::Provider::default());
        let store = Store::new(provider.clone(), root()?);
        let requests = Requests::default();
        let mut manifest = crate::guest::wire::decode_manifest(
            &crate::guest::wire::encode_manifest(Package::new()?.manifest()),
        )?;
        manifest
            .bindings
            .push(crate::domain::Binding::actor_state("STATE".into())?);
        manifest.limits = crate::domain::ActorLimits::new(Some(10), None, None)?;
        let (entered, observing) = tokio::sync::oneshot::channel();
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let handler = Arc::new(Flushes {
            manifest,
            requests: requests.clone(),
            calls: AtomicU64::new(0),
            retained: std::sync::Mutex::new(None),
            entered: std::sync::Mutex::new(Some(entered)),
            gate: Some(gate.clone()),
        });
        let runtime = Runtime::new(
            requests.clone(),
            store.clone(),
            handler.clone(),
            Arc::new(SystemClock),
        );
        let address = spawn_with(&store, "deadline-state", &[b"emit"]).await?;
        runtime.place(address.clone(), 1).await?;
        let mut delivery = Box::pin(runtime.drain_event(&address));
        let request = match futures::future::select(delivery.as_mut(), observing).await {
            futures::future::Either::Right((request, _)) => request?,
            futures::future::Either::Left((result, _)) => {
                return Err(format!("delivery ended before handler admission: {result:?}").into());
            }
        };
        let (entered, release) = if journal_read {
            provider.hold_journal()
        } else {
            provider.arm(super::pause::Fault::Commit)
        };
        gate.add_permits(1);
        match futures::future::select(delivery.as_mut(), entered).await {
            futures::future::Either::Right((entered, _)) => entered?,
            futures::future::Either::Left((result, _)) => {
                return Err(format!("delivery ended before native publication: {result:?}").into());
            }
        }
        let retained = handler
            .retained
            .lock()
            .unwrap()
            .clone()
            .ok_or("missing state capability")?;
        let mut queued = Box::pin(retained.seed());
        assert!(futures::poll!(queued.as_mut()).is_pending());
        let mut backpressured = Box::pin(retained.read_journal(0, 1));
        assert!(futures::poll!(backpressured.as_mut()).is_pending());
        tokio::time::advance(std::time::Duration::from_millis(10)).await;
        // Drive the original timeout event once. The publication remains physically
        // held at its provider, while the request must already be revoked.
        let expired = futures::poll!(delivery.as_mut());
        if journal_read {
            assert!(matches!(
                expired,
                std::task::Poll::Ready(Err(Error::Invalid(
                    "handler exceeded its manifest timeout"
                )))
            ));
            assert!(
                release.is_canceled(),
                "deadline retained a read with no publication"
            );
        } else {
            assert!(
                expired.is_pending(),
                "deadline dropped an original publication"
            );
        }
        assert!(matches!(
            requests.admit(request),
            Err(crate::guest::GuestError::Revoked)
        ));
        assert!(matches!(
            futures::poll!(queued.as_mut()),
            std::task::Poll::Ready(Err(crate::guest::GuestError::Revoked))
        ));
        assert!(matches!(
            futures::poll!(backpressured.as_mut()),
            std::task::Poll::Ready(Err(crate::guest::GuestError::Revoked))
        ));
        let mut late = Box::pin(retained.seed());
        assert!(matches!(
            futures::poll!(late.as_mut()),
            std::task::Poll::Ready(Err(crate::guest::GuestError::Revoked))
        ));
        if !journal_read {
            release
                .send(())
                .map_err(|_| "state publication owner was dropped at its caller deadline")?;
            assert!(matches!(
                delivery.await,
                Err(Error::Invalid("handler exceeded its manifest timeout"))
            ));
        }
        let cut = store.load(&address).await?;
        let state = cut.state().ok_or("missing committed state")?;
        assert_eq!(state.cursor("inbox"), Some(0));
        assert_eq!(state.journal_tail, 1);
        assert_eq!(
            state.metadata.get("value"),
            Some(&Bytes::from_static(b"first"))
        );
        assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    }
    Ok(())
}
