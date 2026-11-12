//! One real actor decision retains its participant through a lost acknowledgement.
use super::*;
use crate::guest::{Composer, Composition, GuestFuture};

fn key(value: &str) -> Result<IdempotencyKey, StreamError> {
    IdempotencyKey::new(value.as_bytes().to_vec())
}

#[derive(Default)]
struct Counts {
    seals: AtomicUsize,
    committed: AtomicUsize,
    rejected: AtomicUsize,
    refused: AtomicUsize,
    dropped: AtomicUsize,
    /// The delivered input bytes each opened participant was handed.
    delivered: std::sync::Mutex<Vec<Option<Vec<u8>>>>,
}

struct Participant {
    counts: Arc<Counts>,
    remove_actor_fence: bool,
    fork_reply_parent: bool,
}

impl Drop for Participant {
    fn drop(&mut self) {
        self.counts.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

impl Composition for Participant {
    fn seal<'a>(&'a mut self, mut request: CommitRequest) -> GuestFuture<'a, CommitRequest> {
        Box::pin(async move {
            self.counts.seals.fetch_add(1, Ordering::SeqCst);
            if self.remove_actor_fence {
                request.effects[0].conditions.clear();
            }
            if self.fork_reply_parent {
                let reply = request
                    .mutations()
                    .find_map(|mutation| match mutation {
                        CommitMutation::Append { path, .. } => path.as_str().rsplit_once("/calls/"),
                        _ => None,
                    })
                    .ok_or_else(|| GuestError::Decode("missing original reply".into()))?;
                let destination = StreamPath::new(format!("{}/calls", reply.0))
                    .map_err(|_| GuestError::Decode("invalid original reply parent".into()))?;
                let source = request
                    .mutations()
                    .find_map(|mutation| match mutation {
                        CommitMutation::Append { path, .. } => Some(path.clone()),
                        _ => None,
                    })
                    .ok_or_else(|| GuestError::Decode("missing original state".into()))?;
                request.effects[0].mutations.push(CommitMutation::Fork {
                    exclude: Vec::new(),
                    source,
                    destination,
                    at_tail: 0,
                    records: Vec::new(),
                });
            }
            Ok(request)
        })
    }

    fn settle<'a>(
        self: Box<Self>,
        outcome: Result<&'a CommitOutcome, &'a StreamError>,
    ) -> GuestFuture<'a, ()> {
        Box::pin(async move {
            let counter = match outcome {
                Ok(CommitOutcome::Committed(_)) => &self.counts.committed,
                Ok(CommitOutcome::Conflict(_)) => &self.counts.rejected,
                Err(_) => &self.counts.refused,
            };
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

/// This oracle also rejects the former all-errors-are-unknown implementation.
fn refusal_was_settled(result: &Result<(), Error>, counts: &Counts, refusal: &StreamError) -> bool {
    matches!(result, Err(Error::Stream(error)) if error == refusal)
        && counts.refused.load(Ordering::SeqCst) == 1
        && counts.committed.load(Ordering::SeqCst) == 0
        && counts.rejected.load(Ordering::SeqCst) == 0
        && counts.dropped.load(Ordering::SeqCst) == 1
}

#[tokio::test]
async fn definite_refusals_settle_and_leave_the_input_available() -> Result<(), Failure> {
    for refusal in StreamError::ALL {
        if matches!(
            refusal.category(),
            StreamError::Unavailable | StreamError::InvalidResponse
        ) {
            continue;
        }
        let provider = Arc::new(Faulty {
            inner: MemoryStream::default(),
            plan: vec![Fault::None, Fault::None, Fault::Refuse(refusal.clone())],
            calls: AtomicUsize::new(0),
        });
        let counts = Arc::new(Counts::default());
        let (store, actor) = placed(provider.clone(), counts.clone()).await?;
        let snapshot = store.load(&actor).await?;
        let trigger = Trigger::Record {
            subscription: "inbox".into(),
            sequence: 0,
        };
        let result = store
            .publish(
                &snapshot,
                1,
                &trigger,
                None,
                transition(false)?,
                key("refused")?,
            )
            .await;
        assert!(
            refusal_was_settled(&result, &counts, &refusal),
            "definite refusal must settle its original participant: {refusal:?}: {result:?}"
        );
        assert_eq!(
            store
                .load(&actor)
                .await?
                .state()
                .and_then(|state| state.cursor("inbox")),
            Some(0)
        );
        assert_eq!(
            provider.inner.tail(path("participant/child")?).await,
            Err(StreamError::NotFound)
        );
        // A new attempt can publish; it is not pinned to the refused request.
        store
            .publish(
                &snapshot,
                1,
                &trigger,
                None,
                transition(false)?,
                key("after-refusal")?,
            )
            .await?;
        assert_eq!(
            store
                .load(&actor)
                .await?
                .state()
                .and_then(|state| state.cursor("inbox")),
            Some(1)
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_retry_refusal_cannot_settle_an_earlier_lost_ack() -> Result<(), Failure> {
    let provider = Arc::new(Faulty {
        inner: MemoryStream::default(),
        plan: vec![
            Fault::None,
            Fault::None,
            Fault::LoseAck,
            Fault::Refuse(StreamError::AccessDenied),
        ],
        calls: AtomicUsize::new(0),
    });
    let counts = Arc::new(Counts::default());
    let (store, actor) = placed(provider.clone(), counts.clone()).await?;
    let result = store
        .publish(
            &store.load(&actor).await?,
            1,
            &Trigger::Record {
                subscription: "inbox".into(),
                sequence: 0,
            },
            None,
            transition(false)?,
            key("lost-then-refused")?,
        )
        .await;
    let Err(Error::Unknown { publication, .. }) = result else {
        return Err("lost acknowledgement did not retain custody".into());
    };
    let Err(Error::Unknown {
        publication,
        source: StreamError::AccessDenied,
    }) = store.submit(*publication).await
    else {
        return Err("retry refusal incorrectly settled an uncertain publication".into());
    };
    assert_eq!(counts.refused.load(Ordering::SeqCst), 0);
    assert_eq!(counts.dropped.load(Ordering::SeqCst), 0);
    store.submit(*publication).await?;
    assert_eq!(counts.seals.load(Ordering::SeqCst), 1);
    assert_eq!(counts.committed.load(Ordering::SeqCst), 1);
    assert_eq!(counts.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(provider.inner.tail(path("participant/child")?).await?, 1);
    Ok(())
}

/// Opens each test participant request with the test's counters.
struct Participants(Arc<Counts>);

/// Interrupt successive real custody settlements before permitting completion.
struct InterruptedRecovery(Arc<AtomicUsize>);

impl Composer for InterruptedRecovery {
    fn open(
        &self,
        _: &ActorReference,
        _: Option<&Checkpoint>,
        _: Option<&acyclic_stream::Record>,
        _: &Checkpoint,
        _: &crate::guest::Participant,
    ) -> Result<Box<dyn Composition>, GuestError> {
        Ok(Box::new(RecoveryAttempt(
            self.0.fetch_add(1, Ordering::SeqCst),
        )))
    }
}

struct RecoveryAttempt(usize);

impl Composition for RecoveryAttempt {
    fn seal<'a>(&'a mut self, request: CommitRequest) -> GuestFuture<'a, CommitRequest> {
        Box::pin(async move { Ok(request) })
    }

    fn settle<'a>(
        self: Box<Self>,
        outcome: Result<&'a CommitOutcome, &'a StreamError>,
    ) -> GuestFuture<'a, ()> {
        Box::pin(async move {
            assert!(matches!(outcome, Ok(CommitOutcome::Committed(_))));
            match self.0 {
                0 => Err(GuestError::Handler("interrupted custody recovery".into())),
                1 => futures::future::pending().await,
                _ => Ok(()),
            }
        })
    }
}

#[tokio::test]
async fn placement_replays_finish_failed_and_cancelled_custody_recovery() -> Result<(), Failure> {
    let provider = Arc::new(Faulty {
        inner: MemoryStream::default(),
        plan: vec![Fault::None, Fault::None, Fault::LoseAck],
        calls: AtomicUsize::new(0),
    });
    let counts = Arc::new(Counts::default());
    let (store, actor) = placed(provider.clone(), counts.clone()).await?;
    let result = store
        .publish(
            &store.load(&actor).await?,
            1,
            &Trigger::Record {
                subscription: "inbox".into(),
                sequence: 0,
            },
            None,
            transition(false)?,
            key("interrupted-owner")?,
        )
        .await;
    let Err(Error::Unknown { publication, .. }) = result else {
        return Err("publication must commit without settling custody".into());
    };
    drop(publication);
    assert_eq!(counts.committed.load(Ordering::SeqCst), 0);
    let attempts = Arc::new(AtomicUsize::new(0));
    let runtime = super::super::runtime::Runtime::new(
        store.with_composer(Arc::new(InterruptedRecovery(attempts.clone()))),
        Arc::new(crate::conformance::Package::new()?),
        Arc::new(super::super::runtime::SystemClock),
    )
    .with_external_placement();
    assert!(matches!(
        runtime.place(actor.clone(), 1).await,
        Err(Error::Handler(GuestError::Handler(message))) if message == "interrupted custody recovery"
    ));
    let committed = runtime.store().load(&actor).await?;
    assert_eq!(committed.state().map(State::epoch), Some(2));
    let mut retry = Box::pin(runtime.place(actor.clone(), 1));
    assert!(futures::poll!(retry.as_mut()).is_pending());
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    drop(retry);
    runtime.place(actor.clone(), 1).await?;
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    let admitted = runtime.store().load(&actor).await?;
    assert_eq!(admitted.tail, committed.tail);
    assert_eq!(admitted.anchor, committed.anchor);
    Ok(())
}

impl Composer for Participants {
    fn open(
        &self,
        _: &ActorReference,
        _: Option<&Checkpoint>,
        delivered: Option<&acyclic_stream::Record>,
        _: &Checkpoint,
        participant: &crate::guest::Participant,
    ) -> Result<Box<dyn Composition>, GuestError> {
        self.0
            .delivered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(delivered.map(|record| record.value.to_vec()));
        Ok(Box::new(Participant {
            counts: self.0.clone(),
            remove_actor_fence: participant.plan.as_ref() == [1],
            fork_reply_parent: participant.plan.as_ref() == [2],
        }))
    }
}

async fn placed(
    provider: Arc<Faulty>,
    counts: Arc<Counts>,
) -> Result<(Store<Faulty>, ActorReference), Failure> {
    let store = Store::new(provider, path("participant/actors")?)
        .with_composer(Arc::new(Participants(counts)));
    let actor = ActorReference::new(b"account", b"agents", b"participant")?;
    let mut create = PreparedPublish::foreign(key("create-participant")?);
    for effect in [
        store.create(
            None,
            &actor,
            crate::local::Creation {
                subscriptions: BTreeMap::from([(
                    "inbox".to_owned(),
                    Subscription {
                        path: path("mail/inbox")?,
                        cursor: 0,
                    },
                )]),
                checkpoint: None,
                escrow: None,
            },
        )?,
        store.activation(&actor)?,
    ] {
        create.join(effect)?;
    }
    store.submit(create).await?;
    store
        .claim(&store.load(&actor).await?, 1, key("place-participant")?)
        .await?;
    Ok((store, actor))
}

#[tokio::test]
async fn uncertain_invocation_admission_reopens_with_the_original_input_after_control_changes()
-> Result<(), Failure> {
    use crate::invocation::Invocation;
    // Drop before apply or lose the real provider's acknowledgement after apply.
    // No retained record or successful outcome is synthesized.
    for fault in [Fault::Drop, Fault::LoseAck] {
        let provider = Arc::new(Faulty {
            inner: MemoryStream::default(),
            plan: vec![
                Fault::None,
                Fault::None,
                fault.clone(),
                Fault::Refuse(StreamError::AccessDenied),
            ],
            calls: AtomicUsize::new(0),
        });
        let counts = Arc::new(Counts::default());
        let (store, actor) = placed(provider.clone(), counts.clone()).await?;
        let before = store.load(&actor).await?;
        let invocation = Invocation::new(
            acyclic_stream::StreamPath::new("invoke-control/caller")?,
            actor.clone(),
            "inbox".into(),
            Bytes::from_static(b"uncertain-admission"),
            Bytes::from_static(b"original-request"),
        )?;
        let operation = IdempotencyKey::new(invocation.identity(before.path.as_str()).to_vec())?;
        let input = before
            .state()
            .and_then(|state| state.subscriptions().get("inbox"))
            .ok_or("missing original subscription")?
            .path
            .clone();
        assert!(matches!(
            store.enqueue_invoke(&invocation).await,
            Err(Error::Stream(StreamError::Unavailable))
        ));
        let original = provider.inspect_idempotency(operation.clone()).await?;
        assert_eq!(original.is_some(), fault == Fault::LoseAck);
        let retained_input = if original.is_some() {
            Some(
                provider
                    .inner
                    .read(ReadRequest {
                        key: None,
                        path: input.clone(),
                        from: 0,
                        limit: 1,
                        at_least: Default::default(),
                    })
                    .await?
                    .try_next()
                    .await?
                    .ok_or("lost committed input")?,
            )
        } else {
            None
        };
        // A separate real owner pauses and replaces the placement while this
        // caller is down. Retry identity must not include that mutable cut.
        let controls = Store::new(Arc::new(provider.inner.clone()), store.root().clone());
        controls
            .set_paused(&before, 1, true, key("admission-pause")?)
            .await?;
        controls
            .claim(&controls.load(&actor).await?, 2, key("admission-replace")?)
            .await?;
        drop(store);
        let reopened = Store::new(Arc::clone(&provider), controls.root().clone())
            .with_composer(Arc::new(Participants(Arc::clone(&counts))));
        assert!(matches!(
            reopened.enqueue_invoke(&invocation).await,
            Err(Error::Stream(StreamError::AccessDenied))
        ));
        assert_eq!(
            provider.inspect_idempotency(operation.clone()).await?,
            original
        );
        let ticket = reopened.enqueue_invoke(&invocation).await?;
        if let Some(original) = retained_input {
            assert_eq!(ticket.record(), &original);
        }
        assert_eq!(provider.inner.tail(input.clone()).await?, 1);
        let accepted = provider
            .inspect_idempotency(operation.clone())
            .await?
            .ok_or("accepted input lost its retry identity")?;
        assert_eq!(accepted.idempotency_key, operation);
        let activation = reopened.activation(&actor)?;
        let mut mutations = vec![CommitMutation::Append {
            path: input,
            records: vec![invocation.record(&reopened.path(&actor)?)?],
        }];
        mutations.extend(activation.mutations);
        let mut expected = CommitRequest {
            deadline_unix_millis: None,
            effects: vec![acyclic_stream::CommitEffect::hard(
                activation.conditions,
                mutations,
            )],
            idempotency_key: operation,
        };
        assert_eq!(
            accepted.request_digest,
            acyclic_stream::request::commit_digest(&mut expected)?
        );
        let changed = Invocation::new(
            acyclic_stream::StreamPath::new("invoke-control/caller")?,
            actor.clone(),
            "inbox".into(),
            invocation.operation().clone(),
            Bytes::from_static(b"changed-request"),
        )?;
        assert!(matches!(
            reopened.enqueue_invoke(&changed).await,
            Err(Error::Stream(StreamError::IdempotencyMismatch))
        ));
        // Dropping a response waiter cannot consume a paused input or retract
        // its original admission, including after an earlier lost reply.
        let mut wait = Box::pin(reopened.wait_invocation(ticket.clone()));
        assert!(futures::poll!(wait.as_mut()).is_pending());
        drop(wait);
        let paused = controls.load(&actor).await?;
        assert_eq!(
            paused.state().and_then(|state| state.cursor("inbox")),
            Some(0)
        );
        controls
            .set_paused(&paused, 2, false, key("admission-resume")?)
            .await?;
        let snapshot = reopened.load(&actor).await?;
        let mut result = transition(false)?;
        result.response = Some(Bytes::from_static(b"original-response"));
        reopened
            .publish(
                &snapshot,
                2,
                &Trigger::Record {
                    subscription: "inbox".into(),
                    sequence: 0,
                },
                Some(ticket.record()),
                result,
                key("admission-response")?,
            )
            .await?;
        let committed = reopened.wait_invocation(ticket.clone()).await?;
        assert_eq!(committed.response().as_ref(), b"original-response");
        assert_eq!(reopened.invoke(&invocation).await?, committed);
        assert_eq!(reopened.enqueue_invoke(&invocation).await?, ticket);
        assert_eq!(provider.inner.tail(path("mail/inbox")?).await?, 1);
        assert_eq!(provider.inner.tail(path("participant/child")?).await?, 1);
        assert_eq!(counts.seals.load(Ordering::SeqCst), 1);
        assert_eq!(counts.committed.load(Ordering::SeqCst), 1);
        assert_eq!(counts.dropped.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[tokio::test]
async fn invocation_lost_ack_retains_the_original_reply_and_participant_through_a_retry_refusal()
-> Result<(), Failure> {
    use crate::invocation::Invocation;
    let provider = Arc::new(Faulty {
        inner: MemoryStream::default(),
        plan: vec![
            Fault::None,
            Fault::None,
            Fault::None,
            Fault::LoseAck,
            Fault::Refuse(StreamError::AccessDenied),
            Fault::None,
        ],
        calls: AtomicUsize::new(0),
    });
    let counts = Arc::new(Counts::default());
    let (store, actor) = placed(provider.clone(), counts.clone()).await?;
    let invocation = Invocation::new(
        acyclic_stream::StreamPath::new("invoke-control/caller")?,
        actor.clone(),
        "inbox".into(),
        Bytes::from_static(b"op"),
        Bytes::from_static(b"request"),
    )?;
    let ticket = store.enqueue_invoke(&invocation).await?;
    let snapshot = store.load(&actor).await?;
    let mut transition = transition(false)?;
    transition.response = Some(Bytes::from_static(b"response"));
    let trigger = Trigger::Record {
        subscription: "inbox".into(),
        sequence: ticket.record().sequence,
    };
    let result = store
        .publish(
            &snapshot,
            1,
            &trigger,
            Some(ticket.record()),
            transition,
            key("invocation-participant")?,
        )
        .await;
    let Err(Error::Unknown { publication, .. }) = result else {
        return Err("lost invocation acknowledgement did not retain custody".into());
    };
    let original = publication.request().clone();
    assert_eq!(counts.seals.load(Ordering::SeqCst), 1);
    assert_eq!(counts.committed.load(Ordering::SeqCst), 0);
    assert_eq!(counts.dropped.load(Ordering::SeqCst), 0);
    let Err(Error::Unknown {
        publication,
        source,
    }) = store.submit(*publication).await
    else {
        return Err("retry refusal settled an uncertain invocation".into());
    };
    assert_eq!(source, StreamError::AccessDenied);
    assert_eq!(publication.request(), &original);
    assert_eq!(counts.refused.load(Ordering::SeqCst), 0);
    assert_eq!(counts.dropped.load(Ordering::SeqCst), 0);
    store.submit(*publication).await?;
    let committed = store.wait_invocation(ticket).await?;
    assert_eq!(committed.response().as_ref(), b"response");
    assert_eq!(committed.receipt().revision(), snapshot.tail + 1);
    assert_eq!(counts.seals.load(Ordering::SeqCst), 1);
    assert_eq!(counts.committed.load(Ordering::SeqCst), 1);
    assert_eq!(counts.dropped.load(Ordering::SeqCst), 1);
    let result = store.invoke(&invocation).await?;
    assert_eq!(
        result, committed,
        "reconciliation must return the original reply"
    );
    assert_eq!(provider.tail(path("participant/child")?).await?, 1);
    assert_eq!(
        store
            .load(&actor)
            .await?
            .state()
            .and_then(|state| state.cursor("inbox")),
        Some(1)
    );
    Ok(())
}

#[tokio::test]
async fn composer_cannot_fork_over_the_original_invocation_reply() -> Result<(), Failure> {
    use crate::invocation::Invocation;
    let provider = Arc::new(Faulty {
        inner: MemoryStream::default(),
        plan: Vec::new(),
        calls: AtomicUsize::new(0),
    });
    let counts = Arc::new(Counts::default());
    let (store, actor) = placed(provider.clone(), counts.clone()).await?;
    let invocation = Invocation::new(
        acyclic_stream::StreamPath::new("invoke-control/caller")?,
        actor.clone(),
        "inbox".into(),
        Bytes::from_static(b"fork-control"),
        Bytes::new(),
    )?;
    let ticket = store.enqueue_invoke(&invocation).await?;
    let snapshot = store.load(&actor).await?;
    let mut transition = transition(false)?;
    transition.response = Some(Bytes::new());
    transition
        .participant
        .as_mut()
        .ok_or("missing participant request")?
        .plan = Bytes::from_static(&[2]);
    let before = provider.calls.load(Ordering::SeqCst);
    let result = store
        .publish(
            &snapshot,
            1,
            &Trigger::Record {
                subscription: "inbox".into(),
                sequence: ticket.record().sequence,
            },
            Some(ticket.record()),
            transition,
            key("fork-original-reply")?,
        )
        .await;
    assert!(matches!(
        result,
        Err(Error::Invalid("composition changed the actor publication"))
    ));
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        before,
        "a tampered publication must not reach the provider"
    );
    assert_eq!(counts.seals.load(Ordering::SeqCst), 1);
    assert_eq!(counts.committed.load(Ordering::SeqCst), 0);
    assert_eq!(counts.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(store.load(&actor).await?.tail, snapshot.tail);
    assert_eq!(
        provider.inner.tail(path("participant/child")?).await,
        Err(StreamError::NotFound)
    );
    Ok(())
}

fn transition(remove_actor_fence: bool) -> Result<Transition, StreamError> {
    Ok(Transition {
        response: None,
        checkpoint: checkpoint(1),
        alarm: AlarmChange::Keep,
        effects: vec![acyclic_stream::CommitEffect::hard(
            vec![CommitCondition::Absent {
                path: path("participant/child")?,
            }],
            vec![CommitMutation::Append {
                path: path("participant/child")?,
                records: vec![Bytes::from_static(b"joined volume").into()],
            }],
        )],
        participant: Some(crate::guest::Participant {
            kind: "test".into(),
            plan: Bytes::from(vec![u8::from(remove_actor_fence)]),
        }),
        subscriptions: Vec::new(),
    })
}

#[tokio::test]
async fn lost_ack_keeps_participant_and_replays_the_sealed_request_once() -> Result<(), Failure> {
    let provider = Arc::new(Faulty {
        inner: MemoryStream::default(),
        plan: vec![Fault::None, Fault::None, Fault::LoseAck],
        calls: AtomicUsize::new(0),
    });
    let counts = Arc::new(Counts::default());
    let (store, actor) = placed(provider.clone(), counts.clone()).await?;
    let snapshot = store.load(&actor).await?;
    let result = store
        .publish(
            &snapshot,
            1,
            &Trigger::Record {
                subscription: "inbox".into(),
                sequence: 0,
            },
            None,
            transition(false)?,
            key("joined-publication")?,
        )
        .await;
    let Err(Error::Unknown { publication, .. }) = result else {
        return Err("lost acknowledgement did not retain the publication".into());
    };
    assert_eq!(counts.seals.load(Ordering::SeqCst), 1);
    assert_eq!(counts.committed.load(Ordering::SeqCst), 0);
    assert_eq!(counts.dropped.load(Ordering::SeqCst), 0);
    assert_eq!(provider.inner.tail(path("participant/child")?).await?, 1);
    store.submit(*publication).await?;
    assert_eq!(counts.seals.load(Ordering::SeqCst), 1);
    assert_eq!(counts.committed.load(Ordering::SeqCst), 1);
    assert_eq!(counts.rejected.load(Ordering::SeqCst), 0);
    assert_eq!(counts.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(provider.inner.tail(path("participant/child")?).await?, 1);
    assert_eq!(
        store
            .load(&actor)
            .await?
            .state()
            .and_then(|state| state.cursor("inbox")),
        Some(1)
    );
    Ok(())
}

/// The owner dies after its composed publication applied but before it learned
/// so: the intent the publication itself carried lets the next owner settle
/// the participant exactly once, and a later plain publication clears it.
#[tokio::test]
async fn a_new_owner_settles_the_participant_a_dead_owner_left_unsettled() -> Result<(), Failure> {
    let provider = Arc::new(Faulty {
        inner: MemoryStream::default(),
        plan: vec![Fault::None, Fault::None, Fault::LoseAck],
        calls: AtomicUsize::new(0),
    });
    let counts = Arc::new(Counts::default());
    let (store, actor) = placed(provider.clone(), counts.clone()).await?;
    let record = |sequence| Trigger::Record {
        subscription: "inbox".into(),
        sequence,
    };
    let result = store
        .publish(
            &store.load(&actor).await?,
            1,
            &record(0),
            None,
            transition(false)?,
            key("dead-owner")?,
        )
        .await;
    let Err(Error::Unknown { publication, .. }) = result else {
        return Err("lost acknowledgement did not retain the publication".into());
    };
    drop(publication);
    assert_eq!(counts.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(counts.committed.load(Ordering::SeqCst), 0);
    let custody = StreamPath::new(format!("{}/custody", store.path(&actor)?))?;
    let claimed = store
        .claim(&store.load(&actor).await?, 2, key("new-owner")?)
        .await?;
    store.recover_custody(&claimed).await?;
    assert_eq!(counts.committed.load(Ordering::SeqCst), 1);
    assert_eq!(
        claimed.state().and_then(|state| state.cursor("inbox")),
        Some(1)
    );
    store
        .publish(
            &claimed,
            2,
            &record(1),
            None,
            Transition {
                participant: None,
                effects: Vec::new(),
                ..transition(false)?
            },
            key("plain")?,
        )
        .await?;
    let plain = store
        .claim(&store.load(&actor).await?, 3, key("third-owner")?)
        .await?;
    store.recover_custody(&plain).await?;
    assert_eq!(counts.committed.load(Ordering::SeqCst), 1);
    assert_eq!(provider.inner.tail(custody).await?, 1);
    let actors = std::slice::from_ref(&actor);
    assert!(
        super::super::tests::orphans(&store, actors)
            .await?
            .is_empty()
    );
    // Negative control: a window entry whose actor never existed.
    let ghost = ActorReference::new(b"account", b"agents", b"ghost")?;
    let mut orphaned = PreparedPublish::foreign(key("ghost")?);
    orphaned.join(store.deadline(&ghost, 60_000)?)?;
    PreparedPublish::settle(&provider.inner.commit(orphaned.into_request()?).await?)?;
    assert_eq!(
        super::super::tests::orphans(&store, actors).await?.len(),
        1,
        "the oracle must report an entry without a live owner"
    );
    Ok(())
}

#[tokio::test]
async fn participant_cannot_remove_the_actor_fence_before_submission() -> Result<(), Failure> {
    let provider = Arc::new(Faulty {
        inner: MemoryStream::default(),
        plan: Vec::new(),
        calls: AtomicUsize::new(0),
    });
    let counts = Arc::new(Counts::default());
    let (store, actor) = placed(provider.clone(), counts.clone()).await?;
    let snapshot = store.load(&actor).await?;
    let before = provider.calls.load(Ordering::SeqCst);
    let result = store
        .publish(
            &snapshot,
            1,
            &Trigger::Record {
                subscription: "inbox".into(),
                sequence: 0,
            },
            None,
            transition(true)?,
            key("broken-participant")?,
        )
        .await;
    assert!(matches!(
        result,
        Err(Error::Invalid("composition changed the actor publication"))
    ));
    assert_eq!(provider.calls.load(Ordering::SeqCst), before);
    assert_eq!(
        provider.inner.tail(path("participant/child")?).await,
        Err(StreamError::NotFound)
    );
    assert_eq!(
        store
            .load(&actor)
            .await?
            .state()
            .and_then(|state| state.cursor("inbox")),
        Some(0)
    );
    assert_eq!(counts.dropped.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn rejected_companion_settles_without_consuming_the_actor_input() -> Result<(), Failure> {
    let provider = Arc::new(Faulty {
        inner: MemoryStream::default(),
        plan: Vec::new(),
        calls: AtomicUsize::new(0),
    });
    let counts = Arc::new(Counts::default());
    let (store, actor) = placed(provider.clone(), counts.clone()).await?;
    let snapshot = store.load(&actor).await?;
    let mut competing = PreparedPublish::foreign(key("competing-child")?);
    competing.join(acyclic_stream::CommitEffect::hard(
        vec![CommitCondition::Absent {
            path: path("participant/child")?,
        }],
        vec![CommitMutation::Append {
            path: path("participant/child")?,
            records: vec![Bytes::from_static(b"other owner").into()],
        }],
    ))?;
    store.submit(competing).await?;
    let result = store
        .publish(
            &snapshot,
            1,
            &Trigger::Record {
                subscription: "inbox".into(),
                sequence: 0,
            },
            None,
            transition(false)?,
            key("rejected-participant")?,
        )
        .await;
    assert!(matches!(result, Err(Error::Conflict)));
    assert_eq!(counts.seals.load(Ordering::SeqCst), 1);
    assert_eq!(counts.committed.load(Ordering::SeqCst), 0);
    assert_eq!(counts.rejected.load(Ordering::SeqCst), 1);
    assert_eq!(counts.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(provider.inner.tail(path("participant/child")?).await?, 1);
    assert_eq!(
        store
            .load(&actor)
            .await?
            .state()
            .and_then(|state| state.cursor("inbox")),
        Some(0)
    );
    Ok(())
}

/// Proof that custody recovery reading by (path, seq) is exact: the owner
/// dies after its composed publication applied, every way to replace the
/// delivered input is attempted and refused by the stream primitive, and
/// recovery hands the composer the original bytes.
#[tokio::test]
async fn custody_recovery_never_reads_a_replaced_input() -> Result<(), Failure> {
    for replaced in [false, true] {
        let provider = Arc::new(Faulty {
            inner: MemoryStream::default(),
            plan: vec![Fault::None, Fault::None, Fault::LoseAck],
            calls: AtomicUsize::new(0),
        });
        let counts = Arc::new(Counts::default());
        let (store, actor) = placed(provider.clone(), counts.clone()).await?;
        let append = |stream: &str, value: &'static [u8], name: &str| {
            let mut input = PreparedPublish::foreign(key(name)?);
            input.join(CommitEffect::hard(
                Vec::new(),
                vec![CommitMutation::Append {
                    path: path(stream)?,
                    records: vec![bytes::Bytes::from_static(value).into()],
                }],
            ))?;
            Ok::<_, Failure>(input.into_request()?)
        };
        PreparedPublish::settle(
            &provider
                .inner
                .commit(append("mail/inbox", b"original", "input")?)
                .await?,
        )?;
        let delivered = {
            use futures::StreamExt as _;
            provider
                .inner
                .read(acyclic_stream::ReadRequest {
                    key: None,
                    path: path("mail/inbox")?,
                    from: 0,
                    limit: 1,
                    at_least: Default::default(),
                })
                .await?
                .next()
                .await
                .ok_or("the input was not committed")??
        };
        let result = store
            .publish(
                &store.load(&actor).await?,
                1,
                &Trigger::Record {
                    subscription: "inbox".into(),
                    sequence: 0,
                },
                Some(&delivered),
                transition(false)?,
                key("delivered")?,
            )
            .await;
        let Err(Error::Unknown { publication, .. }) = result else {
            return Err("publication must commit without settling custody".into());
        };
        drop(publication);
        let opened = counts
            .delivered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len();
        if replaced {
            // Try to replace the input: fork other bytes over the stream
            // itself and over its namespace parent. The primitive refuses
            // both, since a fork destination must be absent.
            for (stream, value) in [("forged", &b"seed"[..]), ("forged/inbox", &b"forged"[..])] {
                PreparedPublish::settle(
                    &provider
                        .inner
                        .commit(append(stream, value, stream)?)
                        .await?,
                )?;
            }
            for (source, destination) in [("forged/inbox", "mail/inbox"), ("forged", "mail")] {
                let mut fork = PreparedPublish::foreign(key(destination)?);
                fork.join(CommitEffect::hard(
                    vec![CommitCondition::Absent {
                        path: path(destination)?,
                    }],
                    vec![CommitMutation::Fork {
                        exclude: Vec::new(),
                        source: path(source)?,
                        destination: path(destination)?,
                        at_tail: 1,
                        records: Vec::new(),
                    }],
                ))?;
                let applied = match provider.inner.commit(fork.into_request()?).await {
                    Ok(outcome) => PreparedPublish::settle(&outcome).is_ok(),
                    Err(_) => false,
                };
                assert!(!applied, "the input at {destination} was replaced");
            }
        }
        let claimed = store
            .claim(&store.load(&actor).await?, 2, key("next-owner")?)
            .await?;
        let recovered = store.recover_custody(&claimed).await;
        let seen = counts
            .delivered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .split_off(opened);
        recovered?;
        assert_eq!(seen, [Some(b"original".to_vec())]);
    }
    Ok(())
}

/// Crash before settlement, then an activation replay with nothing to
/// deliver: the dead owner's publication consumed the final input, so the
/// replacement finds no input or alarm (or a paused actor) and never admits
/// placement. Its replay still settles the committed participant.
#[tokio::test]
async fn a_replay_with_nothing_to_deliver_settles_a_dead_owners_participant() -> Result<(), Failure>
{
    for paused in [false, true] {
        let provider = Arc::new(Faulty {
            inner: MemoryStream::default(),
            plan: vec![Fault::None, Fault::None, Fault::LoseAck],
            calls: AtomicUsize::new(0),
        });
        let counts = Arc::new(Counts::default());
        let (store, actor) = placed(provider.clone(), counts.clone()).await?;
        let result = store
            .publish(
                &store.load(&actor).await?,
                1,
                &Trigger::Record {
                    subscription: "inbox".into(),
                    sequence: 0,
                },
                None,
                transition(false)?,
                key("final-input")?,
            )
            .await;
        let Err(Error::Unknown { publication, .. }) = result else {
            return Err("publication must commit without settling custody".into());
        };
        // The owner dies before settlement.
        drop(publication);
        if paused {
            store
                .set_paused(&store.load(&actor).await?, 1, true, key("paused")?)
                .await?;
        }
        assert_eq!(counts.committed.load(Ordering::SeqCst), 0);
        let runtime = super::super::runtime::Runtime::new(
            store.clone(),
            Arc::new(crate::conformance::Package::new()?),
            Arc::new(super::super::runtime::SystemClock),
        );
        runtime.track(actor.clone())?;
        runtime.drain_actor(&actor).await?;
        assert_eq!(
            counts.committed.load(Ordering::SeqCst),
            1,
            "paused: {paused}"
        );
        assert_eq!(
            runtime
                .store()
                .load(&actor)
                .await?
                .state()
                .map(State::epoch),
            Some(1),
            "the replay admitted no placement"
        );
    }
    Ok(())
}
