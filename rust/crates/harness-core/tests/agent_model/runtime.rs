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
async fn committed_controls_interrupt_a_held_turn_without_interrupting_ordinary_input() -> Result {
    use acyclic_actors::guest::CancellationToken;
    use std::sync::Mutex;
    use tokio::sync::oneshot;

    struct Held(Mutex<Option<oneshot::Sender<CancellationToken>>>);
    impl TurnDriver for Held {
        fn step<'a>(
            &'a self,
            _agent: &'a AgentState,
            _cursor: u64,
            _input: Option<Queued>,
            cancellation: CancellationToken,
        ) -> TurnFuture<'a> {
            let started = self.0.lock().expect("held turn lock").take();
            Box::pin(async move {
                if let Some(started) = started {
                    started.send(cancellation.clone()).expect("turn observer");
                    cancellation.cancelled().await;
                }
                Ok(TurnStep {
                    reserved_children: 0,
                    checkpoint: None,
                    outs: Vec::new(),
                    wait: Wait::Input,
                    drops: None,
                })
            })
        }
    }

    for archive in [false, true] {
        let provider = Arc::new(MemoryStream::default());
        let (started, observed) = oneshot::channel();
        let host = AgentHost::new(
            provider,
            Held(Mutex::new(Some(started))),
            ActorReference::new(b"account", b"agents", b"owner")?,
            ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
            Arc::new(SystemClock),
        )?;
        let root = host.create(&intent("create"), adopted(id(1))).await?;
        assert!(host.runtime().drain().await.is_empty());
        let message = |name| -> Result<InboxRecord> {
            Ok(InboxRecord::Message(Message {
                from: user(),
                id: intent(name),
                body: Box::new(super::driver::file(name)?),
                attachments: None,
            }))
        };
        host.send(&root, &user(), &message("held")?).await?;
        let drain = host.runtime().drain();
        tokio::pin!(drain);
        let cancellation = tokio::select! {
            started = observed => started?,
            failures = &mut drain => panic!("held turn ended before control: {failures:?}"),
        };
        host.send(&root, &user(), &message("ordinary")?).await?;
        assert!(futures::poll!(&mut drain).is_pending());
        assert!(
            !cancellation.is_cancelled(),
            "ordinary input interrupted the turn"
        );

        let delivered = if archive {
            host.archive(&root, &user(), &intent("stop")).await?
        } else {
            host.send(
                &root,
                &user(),
                &InboxRecord::Cancel(Control {
                    by: user(),
                    id: intent("stop"),
                }),
            )
            .await?
        };
        assert!(matches!(delivered, Delivered::Committed { .. }));
        // MemoryStream has the committed control ready. One delivery poll must
        // signal the original token; an obsolete or overbroad prefix fails
        // directly, without waiting for the handler's deadline.
        let completed = futures::poll!(&mut drain);
        assert!(
            cancellation.is_cancelled(),
            "committed control missed the held turn"
        );
        let failures = match completed {
            std::task::Poll::Ready(failures) => failures,
            std::task::Poll::Pending => drain.await,
        };
        assert!(failures.is_empty(), "{failures:?}");
        let state = host.state(&root).await?;
        assert_eq!(state.closed, archive);
        if !archive {
            assert_eq!(state.phase, Phase::Idle);
        }
    }
    Ok(())
}

#[tokio::test]
async fn committed_checkpoints_carry_final_tails_even_when_the_turn_must_be_shed() -> Result {
    for oversized in [false, true] {
        let provider = Arc::new(MemoryStream::default());
        let driver = super::script(vec![TurnStep {
            reserved_children: 0,
            checkpoint: Some(vec![7; if oversized { MAX_CHECKPOINT_BYTES } else { 8 }]),
            outs: vec![
                Out::Conversation(b"first".to_vec()),
                Out::Conversation(b"second".to_vec()),
            ],
            wait: Wait::Approval(intent("decision")),
            drops: None,
        }]);
        let host = AgentHost::new(
            provider.clone(),
            driver,
            ActorReference::new(b"account", b"agents", b"owner")?,
            ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
            Arc::new(SystemClock),
        )?;
        let root = host.create(&intent("create"), adopted(id(1))).await?;
        assert!(host.runtime().drain().await.is_empty());
        let before = host.state(&root).await?;
        let input = InboxRecord::ApprovalRequested(ApprovalRequested {
            child: root.child(&id(2))?,
            id: intent("approval"),
        });
        assert!(matches!(
            host.send(&root, &user(), &input).await?,
            Delivered::Committed { .. }
        ));
        let failures = host.runtime().drain().await;
        assert!(failures.is_empty(), "{failures:?}");
        // Reopening decodes the actual checkpoint published by the handler.
        let committed = host.state(&root).await?;
        assert_eq!(committed.conversation_tail, before.conversation_tail + 2);
        assert_eq!(committed.status_tail, before.status_tail + 1);
        assert_eq!(
            values(&provider, root.conversation()?).await?,
            vec![b"first".to_vec(), b"second".to_vec()]
        );
        assert_eq!(
            values(&provider, root.status()?).await?.len() as u64,
            committed.status_tail
        );
        if oversized {
            assert_eq!(committed.turn, None);
            assert_eq!(
                committed.phase,
                Phase::Done {
                    outcome: AgentOutcome::Failed {
                        message: OVERSIZED_STATE.into(),
                    }
                }
            );
        } else {
            assert_eq!(committed.turn.map(|turn| turn.checkpoint), Some(vec![7; 8]));
        }
    }
    Ok(())
}

#[tokio::test]
async fn a_foreign_actor_cannot_run_the_agents_committed_input() -> Result {
    use acyclic_actors::guest::{CancellationToken, Delivery, Handler, SubscriptionCuts, Trigger};
    use acyclic_stream::{ReadRequest, StreamProvider, StreamProviderExt};
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
            key: None,
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
        subscriptions: SubscriptionCuts::new(
            snapshot.path().clone(),
            anchor,
            state.subscriptions().clone(),
        ),
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
async fn conversation_follow_waits_on_actual_status_publication_and_preserves_refusals() -> Result {
    use acyclic_stream::{
        CommitMutation, CommitOutcome, CommitRequest, FollowRequest, StreamError, StreamProvider,
        StreamProviderExt,
    };
    use futures::TryStreamExt;

    let provider = Arc::new(MemoryStream::default());
    let host = AgentHost::new(
        provider.clone(),
        Spawner(AtomicUsize::new(1)),
        ActorReference::new(b"account", b"agents", b"owner")?,
        ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
        Arc::new(SystemClock),
    )?;
    let root = host.create(&intent("create"), adopted(id(1))).await?;
    // The primitive's missing-path contract still refuses and creates nothing.
    assert!(matches!(
        provider
            .follow(FollowRequest::new(root.conversation()?, 0))
            .await,
        Err(StreamError::NotFound)
    ));
    assert!(matches!(
        host.follow_conversation(&root, 1).await,
        Err(Error::Stream(StreamError::NotFound))
    ));
    let mut cancelled = Box::pin(host.follow_conversation(&root, 0));
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(cancelled.as_mut().poll(cx).is_pending()))
            .await
    );
    drop(cancelled);
    assert!(matches!(
        provider.tail(root.conversation()?).await,
        Err(StreamError::NotFound)
    ));

    let mut following = Box::pin(host.follow_conversation(&root, 0));
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(following.as_mut().poll(cx).is_pending()))
            .await
    );
    let entry = ConversationEntry {
        inbox: 1,
        input: TurnInput::Message {
            from: user(),
            id: intent("message"),
            body: Box::new(super::driver::file("input")?),
            attachments: None,
        },
        output: super::driver::file("output")?,
        outcome: Default::default(),
    }
    .encode()?;
    assert!(matches!(
        provider
            .commit(CommitRequest {
                deadline_unix_millis: None,
                conditions: Vec::new(),
                mutations: vec![
                    CommitMutation::Append {
                        path: root.conversation()?,
                        records: vec![entry.clone().into()]
                    },
                    CommitMutation::Append {
                        path: root.status()?,
                        records: vec![
                            StatusRecord {
                                phase: Phase::Idle,
                                cursor: 2,
                                conversation: 1
                            }
                            .encode()?
                            .into()
                        ]
                    },
                ],
                idempotency_key: intent_key("conversation-follow", &["publish"])?,
            })
            .await?,
        CommitOutcome::Committed(_)
    ));
    let mut records = following.await?;
    let first = records
        .try_next()
        .await?
        .ok_or("conversation follow ended")?;
    assert_eq!(first.sequence, 0);
    assert_eq!(first.value.as_ref(), entry.as_slice());
    drop(records);
    assert!(matches!(
        host.follow_conversation(&root, 2).await,
        Err(Error::Stream(StreamError::OutOfRange))
    ));
    assert!(matches!(
        host.follow_conversation(&AgentPath::root(&id(9)), 0).await,
        Err(Error::Stream(StreamError::NotFound))
    ));

    let closed = host.create(&intent("closed"), adopted(id(2))).await?;
    host.archive(&closed, &user(), &intent("archive")).await?;
    assert!(host.runtime().drain().await.is_empty());
    assert!(matches!(
        host.state(&closed).await?.phase,
        Phase::Done { .. }
    ));
    assert!(matches!(
        host.follow_conversation(&closed, 0).await,
        Err(Error::Stream(StreamError::NotFound))
    ));
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
    // Confirmed delivery wakes the tracked root without an idle inbox follow.
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
        brief: Box::new(super::driver::file("task").expect("file")),
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
        match futures::poll!(reply.as_mut()) {
            std::task::Poll::Ready(result) => result?,
            std::task::Poll::Pending => panic!("committed spawn step left its reply pending"),
        },
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
            brief: Box::new(super::driver::file("task").expect("file")),
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
