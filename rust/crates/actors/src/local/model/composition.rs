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

fn transition(remove_actor_fence: bool) -> Result<Transition, StreamError> {
    Ok(Transition {
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
        conditions: Vec::new(),
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
