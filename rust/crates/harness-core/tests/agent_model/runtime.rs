//! The agent host on the A4 runtime: create, wake on inbox append, spawn, archive.

use super::{Result, adopted, id, intent, user, values};
use acyclic_actors::{
    addressing::ActorReference,
    domain::ActorLimits,
    local::{Snapshot, Store, SystemClock},
};
use acyclic_harness_core::agent::*;
use acyclic_stream::{MemoryStream, StreamPath};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

/// Spawns child `c` on its first turn and idles afterwards.
#[derive(Default)]
struct Spawner(AtomicUsize);

impl TurnDriver for Spawner {
    fn step<'a>(
        &'a self,
        _agent: &'a AgentState,
        _cursor: u64,
        _input: Option<Queued>,
        _cancellation: acyclic_actors::guest::CancellationToken,
    ) -> TurnFuture<'a> {
        let first = self.0.fetch_add(1, Ordering::SeqCst) == 0;
        Box::pin(async move {
            Ok(TurnStep {
                reserved_children: 0,
                checkpoint: None,
                outs: if first {
                    vec![Out::Spawn {
                        intent: intent("c"),
                        brief: None,
                        volumes: Vec::new(),
                    }]
                } else {
                    Vec::new()
                },
                wait: Wait::Input,
                drops: None,
            })
        })
    }
}

fn store(provider: &Arc<MemoryStream>) -> Result<Store<MemoryStream>> {
    Ok(Store::new(provider.clone(), StreamPath::new(ACTORS)?))
}

#[tokio::test]
async fn a_foreign_actor_cannot_run_the_agents_committed_input() -> Result {
    use acyclic_actors::guest::{CancellationToken, Delivery, Handler, SubscriptionCuts, Trigger};
    use acyclic_stream::{ReadRequest, StreamProvider};
    use futures::TryStreamExt as _;

    struct Observed(Arc<AtomicUsize>, Spawner);
    impl TurnDriver for Observed {
        fn step<'a>(
            &'a self,
            agent: &'a AgentState,
            cursor: u64,
            input: Option<Queued>,
            cancellation: CancellationToken,
        ) -> TurnFuture<'a> {
            self.0.fetch_add(1, Ordering::SeqCst);
            self.1.step(agent, cursor, input, cancellation)
        }
    }

    let provider = Arc::new(MemoryStream::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let handler = Arc::new(AgentHandler::new(
        store(&provider)?,
        Observed(calls.clone(), Spawner(AtomicUsize::new(1))),
        ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
    )?);
    let host = AgentHost::hosting(
        provider.clone(),
        ActorReference::new(b"account", b"agents", b"owner")?,
        handler.clone(),
        Arc::new(SystemClock),
        None,
    )?;
    let root = host.create(&intent("create"), adopted(id(1))).await?;
    let failures = host.runtime().drain().await;
    assert!(failures.is_empty(), "{failures:?}");
    let actor = host.actor(&root)?;
    let snapshot = store(&provider)?.load(&actor).await?;
    let state = snapshot.state().ok_or("missing actor state")?;
    let checkpoint = state.checkpoint().ok_or("missing agent checkpoint")?;
    let input = InboxRecord::ApprovalRequested(ApprovalRequested {
        child: root.child(&id(2))?,
        id: intent("approval"),
    });
    let Delivered::Committed { inbox } = host.send(&root, &user(), &input).await? else {
        return Err("input was not committed".into());
    };
    let record = provider
        .read(ReadRequest {
            path: root.inbox()?,
            from: inbox,
            limit: 1,
            at_least: Default::default(),
        })
        .await?
        .try_next()
        .await?
        .ok_or("missing committed input")?;
    let anchor = provider
        .head(snapshot.path().clone())
        .await?
        .last
        .ok_or("missing state publication")?
        .commit_id;
    let delivery = Delivery {
        actor,
        epoch: state.epoch(),
        trigger: Trigger::Record {
            subscription: INBOX.into(),
            sequence: inbox,
        },
        now_unix_ms: 0,
        record: Some(record),
        subscriptions: SubscriptionCuts::new(anchor, state.subscriptions().clone()),
    };
    let mut foreign = delivery.clone();
    foreign.actor = ActorReference::new(b"account", b"agents", id(9).as_str().as_bytes())?;
    assert!(matches!(
        handler
            .on_record(foreign, Some(checkpoint), CancellationToken::new())
            .await,
        Err(acyclic_actors::guest::GuestError::Handler(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    handler
        .on_record(delivery, Some(checkpoint), CancellationToken::new())
        .await?;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn an_inbox_append_wakes_the_agent_and_its_spawn_creates_the_child_actor() -> Result {
    let provider = Arc::new(MemoryStream::default());
    let host = AgentHost::new(
        provider.clone(),
        Spawner::default(),
        ActorReference::new(b"account", b"agents", b"owner")?,
        ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
        Arc::new(SystemClock),
    )?;
    let root = host.create(&intent("create"), adopted(id(1))).await?;
    assert_eq!(host.create(&intent("create"), adopted(id(1))).await?, root);
    let initial = host.state(&root).await?;
    assert_eq!(initial.path, root);
    assert_eq!(initial.parent, None);
    assert!(matches!(
        host.state(&AgentPath::root(&id(9))).await,
        Err(Error::NoSuchAgent)
    ));
    let failures = host.runtime().drain().await;
    assert!(failures.is_empty(), "{failures:?}");
    let actors = store(&provider)?;
    let cursor = |state: &Snapshot| state.state().and_then(|s| s.cursor(INBOX));
    assert_eq!(cursor(&actors.load(&host.actor(&root)?).await?), Some(1));
    let child = root.child(&AgentId::derive(&root.id(), "c"))?;
    let record = InboxRecord::ApprovalRequested(ApprovalRequested {
        child: child.clone(),
        id: intent("q"),
    });
    assert!(matches!(
        host.send(&root, &user(), &record).await?,
        Delivered::Committed { .. }
    ));
    // The root's pending inbox follow holds its waker: the append alone wakes it.
    let failures = host.runtime().drain().await;
    assert!(failures.is_empty(), "{failures:?}");
    let state = actors.load(&host.actor(&child)?).await?;
    assert_eq!(cursor(&state), Some(2));
    assert_eq!(values(&provider, child.inbox()?).await?.len(), 3);
    // The child inherits the root's inbox prefix. Its unconsumed adoption is
    // at its actor subscription cursor, rather than record zero.
    let inherited = host.state(&child).await?;
    assert_eq!(inherited.path, child);
    assert_eq!(inherited.parent.as_ref(), Some(&root));
    assert!(host.state(&AgentPath::root(&child.id())).await.is_err());

    host.host(&child)?;
    let failures = host.runtime().drain().await;
    assert!(failures.is_empty(), "{failures:?}");
    let state = actors.load(&host.actor(&child)?).await?;
    assert_eq!(cursor(&state), Some(3));
    assert!(state.state().and_then(|s| s.checkpoint()).is_some());
    let observed = host.state(&child).await?;
    assert_eq!(observed.path, child);
    assert_eq!(observed.parent.as_ref(), Some(&root));

    let by = intent("archive");
    assert!(matches!(
        host.archive(&root, &user(), &by).await?,
        Delivered::Committed { .. }
    ));
    let late = InboxRecord::ApprovalRequested(ApprovalRequested {
        child,
        id: intent("late"),
    });
    assert_eq!(host.send(&root, &user(), &late).await?, Delivered::Closed);
    Ok(())
}

#[tokio::test]
async fn host_replays_exact_intents_and_rejects_changed_bytes_after_progress() -> Result {
    use acyclic_harness_core::{
        conversation::{VolumeClass, VolumeOwner, VolumeRef},
        resources::ProviderRef,
    };
    use acyclic_stream::StreamError;

    let provider = Arc::new(MemoryStream::default());
    let host = AgentHost::new(
        provider.clone(),
        Spawner::default(),
        ActorReference::new(b"account", b"agents", b"owner")?,
        ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
        Arc::new(SystemClock),
    )?;
    let root = host.create(&intent("create"), adopted(id(1))).await?;
    let done = InboxRecord::ChildDone(ChildDone {
        child: root.child(&id(2))?,
        outcome: AgentOutcome::Succeeded { result: None },
    });
    assert!(matches!(
        host.send(&root, &user(), &done).await?,
        Delivered::Committed { .. }
    ));
    let another = InboxRecord::Cancel(Control {
        by: user(),
        id: intent("next"),
    });
    assert!(matches!(
        host.send(&root, &user(), &another).await?,
        Delivered::Committed { .. }
    ));
    assert!(matches!(
        host.send(&root, &user(), &done).await?,
        Delivered::Committed { .. }
    ));
    let changed = InboxRecord::ChildDone(ChildDone {
        child: root.child(&id(2))?,
        outcome: AgentOutcome::Failed {
            message: "changed".into(),
        },
    });
    assert!(matches!(
        host.send(&root, &user(), &changed).await,
        Err(Error::Stream(StreamError::IdempotencyMismatch))
    ));
    assert_eq!(values(&provider, root.inbox()?).await?.len(), 3);

    // Recover an unobserved creation receipt after the actor has advanced.
    assert!(host.runtime().drain().await.is_empty());
    assert_eq!(host.create(&intent("create"), adopted(id(1))).await?, root);
    let mut changed = adopted(id(1));
    changed.volumes.push(VolumeBinding {
        name: "private".into(),
        volume: VolumeRef::new(
            ProviderRef::new("test".into(), "filesystem".into(), "1".into(), &mut None)?,
            "changed".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(acyclic_harness_core::AgentId::from_bytes(id(1).to_bytes())),
            &mut None,
        )?,
    });
    assert!(matches!(
        host.create(&intent("create"), changed).await,
        Err(Error::Stream(StreamError::IdempotencyMismatch))
    ));
    assert!(matches!(
        host.archive(&root, &user(), &intent("archive")).await?,
        Delivered::Committed { .. }
    ));
    assert!(matches!(
        host.send(&root, &user(), &done).await?,
        Delivered::Committed { .. }
    ));
    assert!(matches!(
        host.archive(&root, &user(), &intent("archive")).await?,
        Delivered::Committed { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn a_spawn_reply_is_the_outcome_the_parent_step_committed() -> Result {
    let provider = Arc::new(MemoryStream::default());
    let host = AgentHost::new(
        provider,
        Spawner(AtomicUsize::new(1)),
        ActorReference::new(b"account", b"agents", b"owner")?,
        ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
        Arc::new(SystemClock),
    )?;
    let root = host.create(&intent("create"), adopted(id(1))).await?;
    let failures = host.runtime().drain().await;
    assert!(failures.is_empty(), "{failures:?}");
    let request = InboxRecord::Spawn(SpawnRequest {
        by: user(),
        id: intent("s"),
        brief: "task".into(),
    });
    let Delivered::Committed { inbox } = host.send(&root, &user(), &request).await? else {
        return Err("spawn request was not committed".into());
    };
    let (spawn, child) = requested_child(&root, inbox)?;
    let (another, _) = requested_child(&root, inbox + 1)?;
    // Neither a caller's send id nor another inbox record's spawn intent
    // identifies this result. Refuse both before waiting for the step.
    for unrelated in [intent("s"), another] {
        let reply = host.spawned(&root, inbox, &unrelated);
        futures::pin_mut!(reply);
        assert!(matches!(
            futures::poll!(reply.as_mut()),
            std::task::Poll::Ready(Err(Error::Conflict(_)))
        ));
    }
    // Until the parent's step commits, the reply waits rather than guessing.
    let reply = host.spawned(&root, inbox, &spawn);
    futures::pin_mut!(reply);
    assert!(futures::poll!(reply.as_mut()).is_pending());
    let failures = host.runtime().drain().await;
    assert!(failures.is_empty(), "{failures:?}");
    assert_eq!(
        reply.await?,
        SpawnOutcome::Spawned {
            child: child.clone()
        }
    );
    assert_eq!(host.state(&child).await?.parent.as_ref(), Some(&root));
    assert!(matches!(
        host.spawned(&root, inbox, &intent("s")).await,
        Err(Error::Conflict(_))
    ));
    Ok(())
}

#[tokio::test]
async fn a_root_with_every_child_open_commits_and_refuses_one_more() -> Result {
    // Every open child is in the root's checkpoint, so a root at the limit
    // is the largest the child set makes it; that state must commit.
    let provider = Arc::new(MemoryStream::default());
    let host = AgentHost::new(
        provider,
        Spawner(AtomicUsize::new(1)),
        ActorReference::new(b"account", b"agents", b"owner")?,
        ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
        Arc::new(SystemClock),
    )?;
    let root = host.create(&intent("create"), adopted(id(1))).await?;
    let request = |n: usize| {
        InboxRecord::Spawn(SpawnRequest {
            by: user(),
            id: intent(&format!("s{n}")),
            brief: "task".into(),
        })
    };
    let mut last = 0;
    for n in 0..=MAX_OPEN_CHILDREN {
        let Delivered::Committed { inbox } = host.send(&root, &user(), &request(n)).await? else {
            return Err("spawn request was not committed".into());
        };
        last = inbox;
    }
    let failures = host.runtime().drain().await;
    assert!(failures.is_empty(), "{failures:?}");
    let state = host.state(&root).await?;
    assert_eq!(state.open_children.len(), MAX_OPEN_CHILDREN);
    let (spawn, _) = requested_child(&root, last)?;
    assert_eq!(
        host.spawned(&root, last, &spawn).await?,
        SpawnOutcome::Refused {
            reason: SpawnRefusal::Limit
        }
    );
    Ok(())
}

#[tokio::test]
async fn a_spawn_retry_after_later_steps_names_the_same_child() -> Result {
    let provider = Arc::new(MemoryStream::default());
    let host = AgentHost::new(
        provider,
        Spawner(AtomicUsize::new(1)),
        ActorReference::new(b"account", b"agents", b"owner")?,
        ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
        Arc::new(SystemClock),
    )?;
    let root = host.create(&intent("create"), adopted(id(1))).await?;
    let request = |n: usize| {
        InboxRecord::Spawn(SpawnRequest {
            by: user(),
            id: intent(&format!("s{n}")),
            brief: "task".into(),
        })
    };
    let mut sequences = Vec::new();
    for n in 0..3 {
        let Delivered::Committed { inbox } = host.send(&root, &user(), &request(n)).await? else {
            return Err("spawn request was not committed".into());
        };
        sequences.push(inbox);
    }
    let failures = host.runtime().drain().await;
    assert!(failures.is_empty(), "{failures:?}");
    let (first, child) = requested_child(&root, sequences[0])?;
    assert_eq!(host.state(&root).await?.open_children.len(), 3);
    // The exact retry replays its inbox record, and its outcome comes from
    // the step that consumed it: the same child, and no second one.
    let Delivered::Committed { inbox } = host.send(&root, &user(), &request(0)).await? else {
        return Err("the retried spawn request was not committed".into());
    };
    assert_eq!(inbox, sequences[0]);
    assert_eq!(
        host.spawned(&root, inbox, &first).await?,
        SpawnOutcome::Spawned { child }
    );
    let failures = host.runtime().drain().await;
    assert!(failures.is_empty(), "{failures:?}");
    assert_eq!(host.state(&root).await?.open_children.len(), 3);
    Ok(())
}
