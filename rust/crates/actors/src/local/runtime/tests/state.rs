use super::*;
use crate::guest::{
    ActorMetadataWrite, ActorStateAlarm, ActorStateCommit, ActorStateHost, Requests,
};

struct Flushes {
    manifest: Manifest,
    requests: Requests,
    calls: AtomicU64,
    retained: std::sync::Mutex<Option<ActorStateHost>>,
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
            self.calls.fetch_add(1, Ordering::SeqCst);
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
                    alarm: ActorStateAlarm::Keep,
                })
                .await?;
            assert_eq!(
                state.read_journal(0, 10).await?,
                vec![Bytes::from_static(b"one")]
            );
            state
                .commit(ActorStateCommit {
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
