//! One real actor decision retains its participant through a lost acknowledgement.
use super::*;
use crate::guest::{Composition, GuestFuture};

fn key(value: &str) -> Result<IdempotencyKey, StreamError> {
    IdempotencyKey::new(value.as_bytes().to_vec())
}

#[derive(Default)]
struct Counts {
    seals: AtomicUsize,
    committed: AtomicUsize,
    rejected: AtomicUsize,
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

    fn settle<'a>(self: Box<Self>, outcome: &'a CommitOutcome) -> GuestFuture<'a, ()> {
        Box::pin(async move {
            let counter = match outcome {
                CommitOutcome::Committed(_) => &self.counts.committed,
                CommitOutcome::Conflict(_) => &self.counts.rejected,
            };
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

async fn placed(provider: Arc<Faulty>) -> Result<(Store<Faulty>, ActorReference), Failure> {
    let store = Store::new(provider, path("participant/actors")?);
    let actor = ActorReference::new(b"account", b"agents", b"participant")?;
    let mut create = PreparedPublish::foreign(key("create-participant")?);
    for effect in store.create(
        &actor,
        BTreeMap::from([(
            "inbox".to_owned(),
            Subscription { path: path("participant/inbox")?, cursor: 0 },
        )]),
        None,
    )? {
        create.join(effect)?;
    }
    store.submit(create).await?;
    store.claim(&store.load(&actor).await?, 1, key("place-participant")?).await?;
    Ok((store, actor))
}

fn transition(counts: Arc<Counts>, remove_actor_fence: bool) -> Result<Transition, StreamError> {
    Ok(Transition {
        checkpoint: checkpoint(1),
        alarm: AlarmChange::Keep,
        effects: vec![Effect {
            conditions: vec![CommitCondition::Absent { path: path("participant/child")? }],
            mutation: CommitMutation::Append {
                path: path("participant/child")?,
                records: vec![Bytes::from_static(b"joined volume").into()],
            },
        }],
        composition: Some(Box::new(Participant { counts, remove_actor_fence })),
    })
}

#[tokio::test]
async fn lost_ack_keeps_participant_and_replays_the_sealed_request_once() -> Result<(), Failure> {
    let provider = Arc::new(Faulty {
        inner: MemoryStream::default(),
        plan: vec![Fault::None, Fault::None, Fault::LoseAck],
        calls: AtomicUsize::new(0),
    });
    let (store, actor) = placed(provider.clone()).await?;
    let counts = Arc::new(Counts::default());
    let snapshot = store.load(&actor).await?;
    let result = store.publish(
        &snapshot, 1,
        &Trigger::Record { subscription: "inbox".into(), sequence: 0 },
        transition(counts.clone(), false)?, key("joined-publication")?,
    ).await;
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
    assert_eq!(store.load(&actor).await?.state().and_then(|state| state.cursor("inbox")), Some(1));
    Ok(())
}

#[tokio::test]
async fn participant_cannot_remove_the_actor_fence_before_submission() -> Result<(), Failure> {
    let provider = Arc::new(Faulty {
        inner: MemoryStream::default(), plan: Vec::new(), calls: AtomicUsize::new(0),
    });
    let (store, actor) = placed(provider.clone()).await?;
    let counts = Arc::new(Counts::default());
    let snapshot = store.load(&actor).await?;
    let before = provider.calls.load(Ordering::SeqCst);
    let result = store.publish(
        &snapshot, 1,
        &Trigger::Record { subscription: "inbox".into(), sequence: 0 },
        transition(counts.clone(), true)?, key("broken-participant")?,
    ).await;
    assert!(matches!(result, Err(Error::Invalid("composition changed the actor publication"))));
    assert_eq!(provider.calls.load(Ordering::SeqCst), before);
    assert_eq!(provider.inner.tail(path("participant/child")?).await, Err(StreamError::NotFound));
    assert_eq!(store.load(&actor).await?.state().and_then(|state| state.cursor("inbox")), Some(0));
    assert_eq!(counts.dropped.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn rejected_companion_settles_without_consuming_the_actor_input() -> Result<(), Failure> {
    let provider = Arc::new(Faulty {
        inner: MemoryStream::default(), plan: Vec::new(), calls: AtomicUsize::new(0),
    });
    let (store, actor) = placed(provider.clone()).await?;
    let counts = Arc::new(Counts::default());
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
    let result = store.publish(
        &snapshot, 1,
        &Trigger::Record { subscription: "inbox".into(), sequence: 0 },
        transition(counts.clone(), false)?, key("rejected-participant")?,
    ).await;
    assert!(matches!(result, Err(Error::Conflict)));
    assert_eq!(counts.seals.load(Ordering::SeqCst), 1);
    assert_eq!(counts.committed.load(Ordering::SeqCst), 0);
    assert_eq!(counts.rejected.load(Ordering::SeqCst), 1);
    assert_eq!(counts.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(provider.inner.tail(path("participant/child")?).await?, 1);
    assert_eq!(store.load(&actor).await?.state().and_then(|state| state.cursor("inbox")), Some(0));
    Ok(())
}
