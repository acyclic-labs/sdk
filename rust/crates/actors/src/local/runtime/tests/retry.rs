use super::*;

/// Refuses the first delivery of its actor's alarm with a retained failure,
/// then delegates; counts every alarm delivery it is handed.
struct RefusesFirstAlarm {
    package: Package,
    alarms: AtomicUsize,
}

impl Handler for RefusesFirstAlarm {
    fn manifest(&self) -> &Manifest {
        self.package.manifest()
    }

    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        cancellation: crate::guest::CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        if matches!(delivery.trigger, Trigger::Alarm { .. })
            && self.alarms.fetch_add(1, Ordering::SeqCst) == 0
        {
            return futures::future::ready(Err(crate::guest::GuestError::Handler(
                "refused alarm".into(),
            )))
            .boxed();
        }
        self.package.on_record(delivery, checkpoint, cancellation)
    }
}

/// A failed delivery stays queued for the actor's NEXT event. Its own due
/// alarm is not one: the continuous host must not redeliver it in a loop, and
/// reaches its idle boundary with the failure reported once.
///
/// Negative control: installing the alarm being delivered into the due index
/// before its handler runs made the shared timer fire it again as soon as the
/// attempt failed, so the host redelivered it forever without a new event.
#[tokio::test]
async fn a_failed_alarm_delivery_waits_for_its_next_event() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let address = spawn_with(&store, "refused-alarm", &[b"set 10"]).await?;
    let (clock, mut control) = ControlledClock::new(100);
    let handler = Arc::new(RefusesFirstAlarm {
        package: Package::new()?,
        alarms: AtomicUsize::new(0),
    });
    let runtime = Runtime::new(
        Store::new(Arc::clone(&provider), root()?),
        Arc::clone(&handler) as Arc<dyn Handler>,
        Arc::new(clock),
    );
    let reported = AtomicUsize::new(0);
    let mut serving = Box::pin(runtime.run(|actor, error| {
        assert_eq!(actor, &address);
        assert!(matches!(error, Error::Handler(_)), "{error}");
        reported.fetch_add(1, Ordering::SeqCst);
    }));
    observe_while_running(
        serving.as_mut(),
        state_after(&store, &address, |state| state.cursor("inbox") == Some(1)),
    )
    .await?;
    control.advance_to(200)?;
    let completed = observe_while_running(serving.as_mut(), async {
        Ok::<_, Box<dyn std::error::Error>>(runtime.complete().await)
    })
    .await?;
    assert!(
        matches!(completed, Err(Error::CompletionFailed(_))),
        "{completed:?}"
    );
    assert_eq!(
        handler.alarms.load(Ordering::SeqCst),
        1,
        "the failed alarm was redelivered without a new event"
    );
    assert_eq!(reported.load(Ordering::SeqCst), 1);
    assert!(records(&*provider, "out/refused-alarm").await?.is_empty());
    // The producer's wake is the next event: it delivers the retained alarm
    // before the new input.
    provider
        .append(AppendRequest {
            path: StreamPath::new("agents/refused-alarm")?,
            records: vec![Bytes::from_static(b"emit").into()],
            if_tail: Some(1),
            idempotency_key: key("refused-alarm-next")?,
        })
        .await?;
    runtime.wake(&address);
    observe_while_running(
        serving.as_mut(),
        state_after(&store, &address, |state| state.cursor("inbox") == Some(2)),
    )
    .await?;
    assert_eq!(handler.alarms.load(Ordering::SeqCst), 2);
    assert_eq!(
        records(&*provider, "out/refused-alarm").await?,
        vec![Bytes::from_static(b"a1"), Bytes::from_static(b"r1")]
    );
    Ok(())
}

/// How a composed participant refuses: a stale guard the commit rejects
/// identically each time, or a refusal while sealing that names no change.
#[derive(Clone, Copy)]
enum Refusal {
    StaleGuard,
    Sealing,
}

struct Guard {
    refusal: Refusal,
    seals: Arc<AtomicUsize>,
    path: StreamPath,
}

impl crate::guest::Composition for Guard {
    fn seal<'a>(&'a mut self, mut request: CommitRequest) -> GuestFuture<'a, CommitRequest> {
        Box::pin(async move {
            assert!(
                self.seals.fetch_add(1, Ordering::SeqCst) < 2,
                "unchanged guards caused a third storage attempt"
            );
            match self.refusal {
                // The guard stream holds one record: this can never hold.
                Refusal::StaleGuard => request.conditions.push(CommitCondition::Tail {
                    path: self.path.clone(),
                    expected: 0,
                }),
                Refusal::Sealing => return Err(crate::guest::GuestError::PreconditionChanged),
            }
            Ok(request)
        })
    }

    fn revalidate(&self, _: &[acyclic_stream::CommitConflict]) -> bool {
        true
    }

    fn settle<'a>(
        self: Box<Self>,
        _: Result<&'a acyclic_stream::CommitOutcome, &'a StreamError>,
    ) -> GuestFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

impl crate::guest::Composer for Guard {
    fn open(
        &self,
        _: &ActorReference,
        _: Option<&Checkpoint>,
        _: Option<&acyclic_stream::Record>,
        _: &Checkpoint,
        participant: &crate::guest::Participant,
    ) -> Result<Box<dyn crate::guest::Composition>, crate::guest::GuestError> {
        Ok(Box::new(Guard {
            refusal: self.refusal,
            seals: Arc::clone(&self.seals),
            path: StreamPath::new(format!(
                "{}/{}",
                self.path,
                std::str::from_utf8(&participant.plan)
                    .map_err(|error| crate::guest::GuestError::Decode(error.to_string()))?
            ))
            .map_err(|error| crate::guest::GuestError::Decode(error.to_string()))?,
        }))
    }
}

/// Joins a participant to every transition of the package.
struct Joined(Package);

impl Handler for Joined {
    fn manifest(&self) -> &Manifest {
        self.0.manifest()
    }

    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        cancellation: crate::guest::CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        Box::pin(async move {
            let plan = match &delivery.trigger {
                Trigger::Record { subscription, .. } => {
                    Bytes::copy_from_slice(subscription.as_bytes())
                }
                Trigger::Alarm { .. } => Bytes::new(),
            };
            let mut transition = self.0.on_record(delivery, checkpoint, cancellation).await?;
            transition.participant = Some(crate::guest::Participant {
                kind: "guard".into(),
                plan,
            });
            Ok(transition)
        })
    }
}

/// Every delivery attempt touches storage, so it must append to the journal
/// or wait on an event. A composed-guard refusal is retried only on evidence
/// of an external change; the identical refusal again settles as a named
/// failure, with the input unconsumed and nothing published.
///
/// Negative control: retrying every refusal recomputed the same delivery
/// against the same unchanged guard forever, a storage loop with no journal
/// progress that never reached the idle boundary. Two ready subscriptions have
/// different stale guards: advancing fairness before commit alternated those
/// refusals forever. The attempt counter catches that third attempt directly.
#[tokio::test]
async fn an_unchanged_guard_refusal_settles_instead_of_retrying_forever() -> TestResult {
    for refusal in [Refusal::StaleGuard, Refusal::Sealing] {
        let provider = Arc::new(MemoryStream::default());
        let store = Store::new(Arc::clone(&provider), root()?);
        let address = actor("guarded")?;
        let mut held = PreparedPublish::foreign(key("guard")?);
        let mut subscriptions = BTreeMap::new();
        for name in ["first", "second"] {
            let inbox = StreamPath::new(format!("agents/guarded/{name}"))?;
            subscriptions.insert(
                name.to_owned(),
                Subscription {
                    path: inbox.clone(),
                    cursor: 0,
                },
            );
            for (path, value) in [
                (inbox, Bytes::from_static(b"emit")),
                (
                    StreamPath::new(format!("guard/{name}"))?,
                    Bytes::from_static(b"held"),
                ),
            ] {
                held.join(Effect {
                    conditions: vec![CommitCondition::Absent { path: path.clone() }],
                    mutation: CommitMutation::Append {
                        path,
                        records: vec![value.into()],
                    },
                })?;
            }
        }
        for effect in store.create(&address, subscriptions, Some(ledger("out/guarded")?))? {
            held.join(effect)?;
        }
        store.submit(held).await?;
        let seals = Arc::new(AtomicUsize::new(0));
        let runtime = Runtime::new(
            Store::new(Arc::clone(&provider), root()?).with_composer(Arc::new(Guard {
                refusal,
                seals: Arc::clone(&seals),
                path: StreamPath::new("guard")?,
            })),
            Arc::new(Joined(Package::new()?)),
            Arc::new(SystemClock),
        );
        let reported = AtomicUsize::new(0);
        let mut serving = Box::pin(runtime.run(|actor, error| {
            assert_eq!(actor, &address);
            assert!(matches!(error, Error::PreconditionUnchanged), "{error}");
            reported.fetch_add(1, Ordering::SeqCst);
        }));
        let completed = observe_while_running(serving.as_mut(), async {
            Ok::<_, Box<dyn std::error::Error>>(runtime.complete().await)
        })
        .await?;
        assert!(
            matches!(completed, Err(Error::CompletionFailed(_))),
            "{completed:?}"
        );
        // The first refusal is retried once; its identical repeat is final.
        assert_eq!(seals.load(Ordering::SeqCst), 2);
        assert_eq!(reported.load(Ordering::SeqCst), 1);
        assert!(records(&*provider, "out/guarded").await?.is_empty());
        let state = store.load(&address).await?;
        for name in ["first", "second"] {
            assert_eq!(state.state().and_then(|state| state.cursor(name)), Some(0));
        }
    }
    Ok(())
}
