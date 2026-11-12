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
/// identically each time, or two real source changes while sealing.
#[derive(Clone, Copy)]
enum Refusal {
    StaleGuard,
    InterruptedStaleGuard,
    ChangingSource,
}

struct Guard {
    refusal: Refusal,
    seals: Arc<AtomicUsize>,
    path: StreamPath,
    provider: Arc<MemoryStream>,
}

impl crate::guest::Composition for Guard {
    fn seal<'a>(&'a mut self, mut request: CommitRequest) -> GuestFuture<'a, CommitRequest> {
        Box::pin(async move {
            let attempt = self.seals.fetch_add(1, Ordering::SeqCst);
            let source = match self.refusal {
                Refusal::ChangingSource if attempt < 2 => Some((
                    self.path.clone(),
                    u64::try_from(attempt).expect("attempt fits") + 1,
                )),
                Refusal::InterruptedStaleGuard if attempt == 1 => {
                    Some((StreamPath::new("guard/second").expect("valid path"), 1))
                }
                _ => None,
            };
            if let Some((path, tail)) = source {
                self.provider
                    .append(AppendRequest {
                        path,
                        records: vec![Bytes::from_static(b"changed").into()],
                        if_tail: Some(tail),
                        idempotency_key: key(&format!("source-change-{attempt}"))
                            .expect("valid key"),
                    })
                    .await
                    .expect("source changes before submission");
                return Err(crate::guest::GuestError::PreconditionChanged);
            }
            match self.refusal {
                // The guard stream holds one record: this can never hold.
                Refusal::StaleGuard | Refusal::InterruptedStaleGuard => {
                    let attempts = match self.refusal {
                        Refusal::StaleGuard => 2,
                        _ => 4,
                    };
                    assert!(
                        attempt < attempts,
                        "unchanged guards caused another storage attempt"
                    );
                    request.effects[0].conditions.push(CommitCondition::Tail {
                        path: self.path.clone(),
                        expected: 0,
                    });
                }
                Refusal::ChangingSource => {
                    assert!(attempt < 4, "changed sources prevented delivery progress");
                }
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
            provider: Arc::clone(&self.provider),
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

async fn guarded_store() -> Result<(Store<MemoryStream>, ActorReference), Box<dyn std::error::Error>>
{
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
            held.join(acyclic_stream::CommitEffect::hard(
                vec![CommitCondition::Absent { path: path.clone() }],
                vec![CommitMutation::Append {
                    path,
                    records: vec![value.into()],
                }],
            ))?;
        }
    }
    for effect in [
        store.create(
            None,
            &address,
            crate::local::Creation {
                subscriptions,
                checkpoint: Some(ledger("out/guarded")?),
                escrow: None,
            },
        )?,
        store.activation(&address)?,
    ] {
        held.join(effect)?;
    }
    store.submit(held).await?;
    Ok((store, address))
}

/// Every delivery attempt touches storage, so it must append to the journal
/// or wait on an event. A composed-guard refusal is retried only on evidence
/// of an external change; the identical refusal again settles as a named
/// failure, with the input unconsumed and nothing published. Consecutive source
/// changes during sealing must instead let the original input commit once.
///
/// Negative control: retrying every refusal recomputed the same delivery
/// against the same unchanged guard forever, a storage loop with no journal
/// progress that never reached the idle boundary. Two ready subscriptions have
/// different stale guards: advancing fairness before commit alternated those
/// refusals forever. The attempt counter catches that third attempt directly.
/// Treating empty sealing refusals as equal instead failed the changing source
/// case after its second real append, leaving both inputs unconsumed.
/// A source change also retires a prior refusal: only subsequent identical
/// guard observations can establish that recomputation made no progress.
#[tokio::test]
async fn guard_refusals_distinguish_unchanged_observations_from_source_changes() -> TestResult {
    for refusal in [
        Refusal::StaleGuard,
        Refusal::InterruptedStaleGuard,
        Refusal::ChangingSource,
    ] {
        let (store, address) = guarded_store().await?;
        let provider = store.provider();
        let seals = Arc::new(AtomicUsize::new(0));
        let runtime = Runtime::new(
            Store::new(Arc::clone(provider), root()?).with_composer(Arc::new(Guard {
                refusal,
                seals: Arc::clone(&seals),
                path: StreamPath::new("guard")?,
                provider: Arc::clone(provider),
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
        let cursor = match refusal {
            Refusal::StaleGuard | Refusal::InterruptedStaleGuard => {
                assert!(
                    matches!(completed, Err(Error::CompletionFailed(_))),
                    "{completed:?}"
                );
                let attempts = if let Refusal::StaleGuard = refusal {
                    2
                } else {
                    assert_eq!(records(provider.as_ref(), "guard/second").await?.len(), 2);
                    4
                };
                assert_eq!(seals.load(Ordering::SeqCst), attempts);
                assert_eq!(reported.load(Ordering::SeqCst), 1);
                assert!(records(provider.as_ref(), "out/guarded").await?.is_empty());
                0
            }
            Refusal::ChangingSource => {
                completed?;
                assert_eq!(seals.load(Ordering::SeqCst), 4);
                assert_eq!(reported.load(Ordering::SeqCst), 0);
                assert_eq!(records(provider.as_ref(), "guard/first").await?.len(), 3);
                assert_eq!(
                    records(provider.as_ref(), "out/guarded").await?,
                    vec![Bytes::from_static(b"r0"), Bytes::from_static(b"r0")]
                );
                1
            }
        };
        let state = store.load(&address).await?;
        for name in ["first", "second"] {
            assert_eq!(
                state.state().and_then(|state| state.cursor(name)),
                Some(cursor)
            );
        }
    }
    Ok(())
}

/// An ordinary failed record stays unconsumed, while the next explicit attempt
/// gives another ready subscription its turn through the same public runtime.
#[tokio::test]
async fn a_failed_record_does_not_starve_other_ready_subscriptions() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let address = actor("failed-input-fairness")?;
    let mut create = PreparedPublish::foreign(key("failed-input-fairness/create")?);
    let mut subscriptions = BTreeMap::new();
    for (name, value) in [
        ("inbox", b"unknown".as_slice()),
        ("mirror", b"emit".as_slice()),
    ] {
        let path = StreamPath::new(format!("fairness/{name}"))?;
        subscriptions.insert(
            name.to_owned(),
            Subscription {
                path: path.clone(),
                cursor: 0,
            },
        );
        create.join(acyclic_stream::CommitEffect::hard(
            vec![CommitCondition::Absent { path: path.clone() }],
            vec![CommitMutation::Append {
                path,
                records: vec![Bytes::copy_from_slice(value).into()],
            }],
        ))?;
    }
    for effect in [
        store.create(
            None,
            &address,
            crate::local::Creation {
                subscriptions,
                checkpoint: Some(ledger("out/fairness")?),
                escrow: None,
            },
        )?,
        store.activation(&address)?,
    ] {
        create.join(effect)?;
    }
    store.submit(create).await?;
    let runtime = Runtime::new(
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    );
    runtime.track(address.clone())?;
    assert!(matches!(
        runtime.drain_actor(&address).await,
        Err(Error::Handler(_))
    ));
    let failed = store.load(&address).await?;
    for name in ["inbox", "mirror"] {
        assert_eq!(failed.state().and_then(|state| state.cursor(name)), Some(0));
    }
    assert!(records(&*provider, "out/fairness").await?.is_empty());
    // This is an explicit caller attempt, not a readiness retry loop.
    assert!(matches!(
        runtime.drain_actor(&address).await,
        Err(Error::Handler(_))
    ));
    let resumed = store.load(&address).await?;
    assert_eq!(
        resumed.state().and_then(|state| state.cursor("inbox")),
        Some(0)
    );
    assert_eq!(
        resumed.state().and_then(|state| state.cursor("mirror")),
        Some(1),
        "failed input starved another ready subscription"
    );
    assert_eq!(
        records(&*provider, "out/fairness").await?,
        vec![Bytes::from_static(b"r0")]
    );
    Ok(())
}
