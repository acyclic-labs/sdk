//! Original actor publications with counted IO and a cancellable source.
use super::super::cancel;
use super::*;
use acyclic_stream::{
    BoxProviderFuture, CommitId, CommittedEnvelope, FollowRequest, FollowStream, Heads,
    HeadsRequest, IdempotencyKey, IdempotencyObservation, ProviderContext, ReadRequest,
    RecordStream, StreamError, StreamProvider, StreamProviderExt as _, WriteOutcome, WriteRequest,
};
use futures::StreamExt as _;
use std::sync::Mutex;
use tokio::sync::oneshot;

#[derive(Default)]
struct Source {
    inner: MemoryStream,
    heads: AtomicUsize,
    reads: AtomicUsize,
    follows: AtomicUsize,
    stop: Mutex<Option<oneshot::Receiver<()>>>,
}

#[async_trait::async_trait]
impl StreamProvider for Source {
    fn context(&self) -> ProviderContext<'_> {
        self.inner.context()
    }
    async fn heads(&self, request: HeadsRequest) -> std::result::Result<Heads, StreamError> {
        self.heads.fetch_add(1, Ordering::SeqCst);
        self.inner.heads(request).await
    }
    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> std::result::Result<Option<IdempotencyObservation>, StreamError> {
        self.inner.inspect_idempotency(key).await
    }
    fn write(
        &self,
        request: WriteRequest,
    ) -> BoxProviderFuture<'_, std::result::Result<WriteOutcome, StreamError>> {
        self.inner.write(request)
    }
    async fn read(&self, request: ReadRequest) -> std::result::Result<RecordStream, StreamError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.read(request).await
    }
    async fn follow(
        &self,
        request: FollowRequest,
    ) -> std::result::Result<FollowStream, StreamError> {
        self.follows.fetch_add(1, Ordering::SeqCst);
        let stop = if request.path.as_str().starts_with("harness/spawn-results/") {
            self.stop
                .lock()
                .map_err(|_| StreamError::Unavailable)?
                .take()
        } else {
            None
        };
        let follow = self.inner.follow(request).await?;
        Ok(match stop {
            Some(stop) => Box::pin(follow.take_until(async move {
                let _ = stop.await;
            })),
            None => follow,
        })
    }
    async fn read_commit(
        &self,
        id: CommitId,
    ) -> std::result::Result<CommittedEnvelope, StreamError> {
        self.inner.read_commit(id).await
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl acyclic_stream::NativeStreamProvider for Source {}

#[tokio::test]
async fn outcomes_have_constant_read_cost_and_end_when_their_source_ends() -> Result {
    let provider = Arc::new(Source::default());
    let host = AgentHost::new(
        provider.clone(),
        Spawner(AtomicUsize::new(1)),
        ActorReference::new(b"account", b"agents", b"owner")?,
        ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
        Arc::new(SystemClock),
    )?;
    let root = host.create(&intent("create"), adopted(id(1))).await?;
    assert!(host.runtime().drain().await.is_empty());
    let request = InboxRecord::Spawn(SpawnRequest {
        history: None,
        by: user(),
        id: intent("spawn"),
        brief: Box::new(super::super::driver::file("task")?),
    });
    let Delivered::Committed { inbox } = host.send(&root, &user(), &request).await? else {
        return Err("spawn input was not committed".into());
    };
    let (name, child) = requested_child(&root, inbox)?;
    let (producer, stopped) = oneshot::channel();
    *provider.stop.lock().expect("source lock") = Some(stopped);
    let reply = host.spawned(&root, inbox, &name);
    futures::pin_mut!(reply);
    assert!(futures::poll!(reply.as_mut()).is_pending());
    // Dropping the original source producer must wake the same wait with a
    // typed error, even though the accepted input has not been consumed.
    drop(producer);
    assert!(matches!(
        futures::poll!(reply.as_mut()),
        std::task::Poll::Ready(Err(Error::Stream(StreamError::Unavailable)))
    ));
    assert!(host.runtime().drain().await.is_empty());
    // Later child closure changes the parent's current state. The result
    // still names the original successful fork, rather than a new refusal.
    host.archive(&child, &user(), &intent("close-child"))
        .await?;
    assert!(host.runtime().drain().await.is_empty());
    assert!(!host.state(&root).await?.open_children.contains(&child));
    for later in [0, 1, 32] {
        for n in 0..later {
            host.send(&root, &user(), &cancel(&format!("later-{later}-{n}")))
                .await?;
        }
        assert!(host.runtime().drain().await.is_empty());
        provider.heads.store(0, Ordering::SeqCst);
        provider.reads.store(0, Ordering::SeqCst);
        provider.follows.store(0, Ordering::SeqCst);
        assert_eq!(
            host.spawned(&root, inbox, &name).await?,
            SpawnOutcome::Spawned {
                child: child.clone()
            }
        );
        assert_eq!(provider.heads.load(Ordering::SeqCst), 0);
        assert_eq!(provider.reads.load(Ordering::SeqCst), 1);
        assert_eq!(provider.follows.load(Ordering::SeqCst), 1);
    }
    // A different child at the exact result address is still a refusal:
    // stream selection alone does not validate its payload's identity.
    let request = InboxRecord::Spawn(SpawnRequest {
        history: None,
        by: user(),
        id: intent("other-spawn"),
        brief: Box::new(super::super::driver::file("other task")?),
    });
    let Delivered::Committed { inbox } = host.send(&root, &user(), &request).await? else {
        return Err("spawn input was not committed".into());
    };
    provider
        .inner
        .append(acyclic_stream::AppendRequest {
            path: root.spawn_result(inbox)?,
            records: vec![SpawnOutcome::Spawned { child }.encode()?.into()],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    assert!(matches!(
        host.spawned(&root, inbox, &requested_child(&root, inbox)?.0)
            .await,
        Err(Error::Record(_))
    ));
    Ok(())
}

#[tokio::test]
async fn a_conflicting_result_prevents_the_child_fork_in_the_same_commit() -> Result {
    use acyclic_stream::{AppendRequest, CommitCondition};
    let provider = Arc::new(MemoryStream::default());
    let host = AgentHost::new(
        provider.clone(),
        Spawner(AtomicUsize::new(1)),
        ActorReference::new(b"account", b"agents", b"owner")?,
        ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
        Arc::new(SystemClock),
    )?;
    let root = host.create(&intent("create"), adopted(id(1))).await?;
    assert!(host.runtime().drain().await.is_empty());
    let request = SpawnRequest {
        history: None,
        by: user(),
        id: intent("spawn"),
        brief: Box::new(super::super::driver::file("task")?),
    };
    let Delivered::Committed { inbox } = host
        .send(&root, &user(), &InboxRecord::Spawn(request.clone()))
        .await?
    else {
        return Err("spawn input was not committed".into());
    };
    let step = step(
        &Spawner(AtomicUsize::new(1)),
        &root,
        Some(host.state(&root).await?),
        inbox + 1,
        Wake::Inbox {
            sequence: inbox,
            record: InboxRecord::Spawn(request),
        },
        acyclic_actors::guest::CancellationToken::new(),
    )
    .await?;
    let mut publication = lower(
        &step,
        inbox + 1,
        delivery_key(&step.state, "original-spawn")?,
    )?;
    let path = root.spawn_result(inbox)?;
    provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![
                SpawnOutcome::Refused {
                    reason: SpawnRefusal::Limit,
                }
                .encode()?
                .into(),
            ],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    let (_, child) = requested_child(&root, inbox)?;
    assert!(matches!(
        submit(&*provider, publication.clone()).await?,
        Submitted::Conflict(_)
    ));
    assert!(values(&provider, child.inbox()?).await?.is_empty());
    assert_eq!(provider.tail(path.clone()).await?, 1);
    // Remove only the result's absence condition: the original fork then
    // commits and a contradictory second result can escape.
    publication.conditions.retain(|condition| !matches!(condition, CommitCondition::Absent { path: guarded } if guarded == &path));
    assert!(matches!(
        submit(&*provider, publication).await?,
        Submitted::Committed(_)
    ));
    assert!(!values(&provider, child.inbox()?).await?.is_empty());
    assert_eq!(provider.tail(path).await?, 2);
    Ok(())
}
