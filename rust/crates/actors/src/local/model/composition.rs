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
                request.conditions.clear();
            }
            if self.fork_reply_parent {
                let reply = request
                    .mutations
                    .iter()
                    .find_map(|mutation| match mutation {
                        CommitMutation::Append { path, .. } => {
                            path.as_str().rsplit_once("/replies/")
                        }
                        _ => None,
                    })
                    .ok_or_else(|| GuestError::Decode("missing original reply".into()))?;
                let destination = StreamPath::new(format!("{}/replies", reply.0))
                    .map_err(|_| GuestError::Decode("invalid original reply parent".into()))?;
                let source = request
                    .mutations
                    .iter()
                    .find_map(|mutation| match mutation {
                        CommitMutation::Append { path, .. } => Some(path.clone()),
                        _ => None,
                    })
                    .ok_or_else(|| GuestError::Decode("missing original state".into()))?;
                request.mutations.push(CommitMutation::Fork {
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
            refusal,
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
            "{refusal:?}: {result:?}"
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
async fn negative_control_all_errors_unknown_fails_the_refusal_oracle() -> Result<(), Failure> {
    let provider = Arc::new(Faulty {
        inner: MemoryStream::default(),
        plan: vec![
            Fault::None,
            Fault::None,
            Fault::Refuse(StreamError::InvalidArgument),
        ],
        calls: AtomicUsize::new(0),
    });
    let counts = Arc::new(Counts::default());
    let (store, actor) = placed(provider.clone(), counts.clone()).await?;
    let snapshot = store.load(&actor).await?;
    let mut publication = store.prepare_publish(
        &snapshot,
        1,
        &Trigger::Record {
            subscription: "inbox".into(),
            sequence: 0,
        },
        None,
        transition(false)?,
        key("negative-refusal")?,
    )?;
    let participant = publication
        .composition
        .as_mut()
        .ok_or("missing participant")?;
    publication.request = participant.seal(publication.request.clone()).await?;
    publication.sealed = true;
    let source = provider
        .commit(publication.request.clone())
        .await
        .err()
        .ok_or("missing refusal")?;
    // The removed implementation retained even a definite refusal as unknown.
    let result = Err(Error::Unknown {
        publication: Box::new(publication),
        source,
    });
    assert!(!refusal_was_settled(
        &result,
        &counts,
        &StreamError::InvalidArgument
    ));
    assert_eq!(counts.refused.load(Ordering::SeqCst), 0);
    assert_eq!(counts.dropped.load(Ordering::SeqCst), 0);
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

impl Composer for Participants {
    fn open(
        &self,
        _: &ActorReference,
        _: Option<&Checkpoint>,
        _: Option<&acyclic_stream::Record>,
        _: &Checkpoint,
        participant: &crate::guest::Participant,
    ) -> Result<Box<dyn Composition>, GuestError> {
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
    for effect in store.create(
        &actor,
        BTreeMap::from([(
            "inbox".to_owned(),
            Subscription {
                path: path("participant/inbox")?,
                cursor: 0,
            },
        )]),
        None,
    )? {
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
        let mut expected = CommitRequest {
            deadline_unix_millis: None,
            conditions: Vec::new(),
            mutations: vec![
                CommitMutation::Append {
                    path: input,
                    records: vec![invocation.record(&reopened.path(&actor)?)?],
                },
                reopened.activation(&actor).mutation,
            ],
            idempotency_key: operation,
        };
        assert_eq!(
            accepted.request_digest,
            acyclic_stream::request::commit_digest(&mut expected)?
        );
        let changed = Invocation::new(
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
        assert_eq!(provider.inner.tail(path("participant/inbox")?).await?, 1);
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
        effects: vec![Effect {
            conditions: vec![CommitCondition::Absent {
                path: path("participant/child")?,
            }],
            mutation: CommitMutation::Append {
                path: path("participant/child")?,
                records: vec![Bytes::from_static(b"joined volume").into()],
            },
        }],
        participant: Some(crate::guest::Participant {
            kind: "test".into(),
            plan: Bytes::from(vec![u8::from(remove_actor_fence)]),
        }),
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
    competing.join(Effect {
        conditions: vec![CommitCondition::Absent {
            path: path("participant/child")?,
        }],
        mutation: CommitMutation::Append {
            path: path("participant/child")?,
            records: vec![Bytes::from_static(b"other owner").into()],
        },
    })?;
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
