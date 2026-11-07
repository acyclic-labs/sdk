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
                        profile: None,
                        history: None,
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
async fn same_phase_settlement_advances_the_actor_without_appending_status() -> Result {
    use acyclic_stream::StreamProvider as _;
    use futures::TryStreamExt as _;
    let provider = Arc::new(MemoryStream::default());
    let within = ActorReference::new(b"account", b"agents", b"owner")?;
    let host = AgentHost::new(
        provider.clone(),
        super::script(
            (0..3)
                .map(|_| TurnStep {
                    reserved_children: 0,
                    checkpoint: None,
                    outs: vec![Out::Conversation(b"answer".to_vec())],
                    wait: Wait::Input,
                    drops: None,
                })
                .collect(),
        ),
        within.clone(),
        ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
        Arc::new(SystemClock),
    )?;
    let root = host.create(&intent("create"), adopted(id(1))).await?;
    assert!(host.runtime().drain().await.is_empty());
    for n in 0..3 {
        let record = InboxRecord::ApprovalRequested(ApprovalRequested {
            child: root.child(&id(2))?,
            id: intent(&format!("request-{n}")),
        });
        let Delivered::Committed { inbox } = host.send(&root, &user(), &record).await? else {
            return Err("input was refused".into());
        };
        let (ready, cut) = host.input_progress(&root, inbox).await?;
        assert!(!ready);
        assert_eq!(settled(&provider, &within, &root).await?, None);
        let mut changes = acyclic_stream::records_only(provider.follow(cut).await?);
        assert!(host.runtime().drain().await.is_empty());
        assert!(changes.try_next().await?.is_some());
        assert!(host.input_progress(&root, inbox).await?.0);
        assert_eq!(settled(&provider, &within, &root).await?, Some(Phase::Idle));
        let statuses = values(provider.as_ref(), root.status()?).await?;
        assert_eq!(statuses.len(), 1);
        let stale = StatusRecord::decode(&statuses[0])?;
        assert!(
            stale.cursor <= inbox,
            "the former status-only observer cannot settle this input"
        );
    }
    assert_eq!(host.state(&root).await?.conversation_tail, 3);
    Ok(())
}

#[tokio::test]
async fn notice_delivery_wakes_once_and_quiet_delivery_preserves_the_paused_turn() -> Result {
    struct Calls {
        count: Arc<AtomicUsize>,
        pause: bool,
    }
    impl TurnDriver for Calls {
        fn step<'a>(
            &'a self,
            _: &'a AgentState,
            _: u64,
            input: Option<Queued>,
            _: acyclic_actors::guest::CancellationToken,
        ) -> TurnFuture<'a> {
            assert!(matches!(
                input,
                Some(Queued {
                    input: TurnInput::Notice { .. },
                    ..
                })
            ));
            self.count.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                Ok(TurnStep {
                    reserved_children: 0,
                    checkpoint: None,
                    outs: Vec::new(),
                    wait: if self.pause {
                        Wait::Approval {
                            id: intent("decision"),
                            binding: None,
                        }
                    } else {
                        Wait::Input
                    },
                    drops: None,
                })
            })
        }
    }

    for pause in [false, true] {
        let provider = Arc::new(MemoryStream::default());
        let count = Arc::new(AtomicUsize::new(0));
        let host = AgentHost::new(
            provider.clone(),
            Calls {
                count: count.clone(),
                pause,
            },
            ActorReference::new(b"account", b"agents", b"owner")?,
            ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
            Arc::new(SystemClock),
        )?;
        let root = host.create(&intent("create"), adopted(id(1))).await?;
        assert!(host.runtime().drain().await.is_empty());
        let staged = super::corpus::notice_file()?;
        let notice = |name: &str, wake| InboxRecord::Notice {
            id: intent(name),
            notice: if wake {
                NoticePayload::Waking(Box::new(staged.clone()))
            } else {
                NoticePayload::Quiet(acyclic_harness_core::conversation::wire::Notice {
                    source: "filesystem".into(),
                    kind: "arbitrary.open.tag".into(),
                    body: serde_json::json!({"paths": [name]}),
                    wake,
                    coalesce_key: Some("files".into()),
                })
            },
        };
        let quiet = notice("before", false);
        host.send(&root, &user(), &quiet).await?;
        assert!(host.runtime().drain().await.is_empty());
        assert_eq!(count.load(Ordering::SeqCst), 0);

        let waking = notice("waking", true);
        host.send(&root, &user(), &waking).await?;
        assert!(host.runtime().drain().await.is_empty());
        assert_eq!(count.load(Ordering::SeqCst), 1);
        let before = host.state(&root).await?;
        // The original append key owns delivery deduplication, including a
        // caller retry after losing the successful acknowledgement.
        host.send(&root, &user(), &waking).await?;
        assert!(host.runtime().drain().await.is_empty());
        assert_eq!(count.load(Ordering::SeqCst), 1);

        let after = notice("after", false);
        host.send(&root, &user(), &after).await?;
        assert!(host.runtime().drain().await.is_empty());
        assert_eq!(count.load(Ordering::SeqCst), 1);
        let reopened = host.state(&root).await?;
        assert_eq!(reopened.pending_notices, Some(1..4));
        assert_eq!(reopened.phase, before.phase);
        assert_eq!(reopened.turn, before.turn);
        let records = values(provider.as_ref(), root.inbox()?).await?;
        let notices = records
            .iter()
            .map(|record| InboxRecord::decode(record))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        assert_eq!(
            notices,
            vec![
                InboxRecord::ForkAdopted(Box::new(adopted(id(1)))),
                quiet,
                waking,
                after
            ]
        );
    }
    Ok(())
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

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn committed_checkpoints_carry_final_tails_even_when_the_turn_must_be_shed() -> Result {
    #[cfg(feature = "test-support")]
    return super::control_trace::checkpoint(checkpoint_tails()).await;
    #[cfg(not(feature = "test-support"))]
    checkpoint_tails().await.map(|_| ())
}

#[cfg(not(target_arch = "wasm32"))]
async fn checkpoint_tails() -> Result<Vec<AgentPath>> {
    use acyclic_stream::LocalStream;

    let mut agents = Vec::new();
    for oversized in [false, true] {
        let directory = tempfile::tempdir()?;
        let provider = Arc::new(LocalStream::open(directory.path(), Default::default()).await?);
        let idle = || TurnStep {
            reserved_children: 0,
            checkpoint: None,
            outs: Vec::new(),
            wait: Wait::Input,
            drops: None,
        };
        let driver = super::script(vec![
            TurnStep {
                outs: ["a", "b", "c"]
                    .into_iter()
                    .map(|name| Out::Spawn {
                        profile: None,
                        history: None,
                        intent: intent(name),
                        brief: None,
                        volumes: Vec::new(),
                    })
                    .collect(),
                ..idle()
            },
            TurnStep {
                reserved_children: 0,
                checkpoint: Some(vec![7; if oversized { MAX_CHECKPOINT_BYTES } else { 8 }]),
                outs: vec![
                    Out::Conversation(b"first".to_vec()),
                    Out::Conversation(b"second".to_vec()),
                ],
                wait: Wait::Approval {
                    id: intent("decision"),
                    binding: Some(acyclic_harness_core::interaction::ApprovalBinding {
                        operation_id: acyclic_harness_core::OperationId::from_bytes([8; 16]),
                        action_digest: [9; 32],
                    }),
                },
                drops: None,
            },
            idle(),
        ]);
        let within = ActorReference::new(b"account", b"agents", b"owner")?;
        let host = AgentHost::new(
            provider.clone(),
            driver,
            within.clone(),
            ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
            Arc::new(SystemClock),
        )?;
        let root = host.create(&intent("create"), adopted(id(1))).await?;
        host.drain(&root).await?;
        let input = InboxRecord::ApprovalRequested(ApprovalRequested {
            child: root.child(&id(2))?,
            id: intent("approval"),
        });
        assert!(matches!(
            host.send(&root, &user(), &input).await?,
            Delivered::Committed { .. }
        ));
        host.drain(&root).await?;
        let parent_before = host.state(&root).await?;
        assert_eq!(parent_before.open_children.len(), 3);
        agents.push(root.clone());
        agents.extend(parent_before.open_children.iter().cloned());
        for child in &parent_before.open_children {
            host.drain(child).await?;
        }
        let child = parent_before
            .open_children
            .first()
            .expect("spawned child")
            .clone();
        let before = host.state(&child).await?;
        assert_eq!(before.parent.as_ref(), Some(&root));
        let Delivered::Committed { inbox } = host.send(&child, &user(), &input).await? else {
            return Err("child input was refused".into());
        };
        host.drain(&child).await?;
        let parent_records = values(provider.as_ref(), root.inbox()?).await?;
        let reports = parent_records
            .iter()
            .map(|bytes| InboxRecord::decode(bytes))
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            .filter_map(|record| match record {
                InboxRecord::ChildDone(done) => Some(done),
                _ => None,
            })
            .collect::<Vec<_>>();
        let expected_reports = if oversized {
            vec![ChildDone {
                child: child.clone(),
                outcome: AgentOutcome::Failed {
                    message: OVERSIZED_STATE.into(),
                },
            }]
        } else {
            Vec::new()
        };
        assert_eq!(
            reports, expected_reports,
            "shed checkpoint lost its parent report"
        );
        host.drain(&root).await?;
        let mut expected_children = parent_before.open_children;
        if oversized {
            expected_children.remove(&child);
        }
        let parent_committed = host.state(&root).await?;
        assert_eq!(parent_committed.open_children, expected_children);
        // Reopening decodes the actual checkpoint published by the handler.
        let committed = host.state(&child).await?;
        assert_eq!(committed.conversation_tail, before.conversation_tail + 2);
        assert_eq!(committed.status_tail, before.status_tail + 1);
        assert_eq!(
            values(provider.as_ref(), child.conversation()?).await?,
            vec![b"first".to_vec(), b"second".to_vec()]
        );
        assert_eq!(
            values(provider.as_ref(), child.status()?).await?.len() as u64,
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
            let expected = Phase::WaitingApproval {
                id: intent("decision"),
                binding: Some(acyclic_harness_core::interaction::ApprovalBinding {
                    operation_id: acyclic_harness_core::OperationId::from_bytes([8; 16]),
                    action_digest: [9; 32],
                }),
            };
            assert_eq!(committed.phase, expected);
            let statuses = values(provider.as_ref(), child.status()?).await?;
            let status = StatusRecord::decode(&statuses[(committed.status_tail - 1) as usize])?;
            assert_eq!(
                status.phase, expected,
                "status and checkpoint lost original binding"
            );
            let mut invalid = status;
            let Phase::WaitingApproval {
                binding: Some(binding),
                ..
            } = &mut invalid.phase
            else {
                unreachable!()
            };
            binding.action_digest = [0; 32];
            assert!(
                invalid.encode().is_err(),
                "an unbound action became valid status"
            );
            assert_eq!(
                committed
                    .turn
                    .as_ref()
                    .map(|turn| turn.checkpoint.as_slice()),
                Some([7; 8].as_slice())
            );
        }
        let snapshot = host.runtime().store().load(&host.actor(&child)?).await?;
        let actor_state = snapshot.state().expect("committed child actor").clone();
        assert!(actor_state.alarm().pending().is_none());
        assert_eq!(actor_state.cursor(INBOX), Some(inbox + 1));
        drop(host);
        drop(provider);
        let provider = Arc::new(LocalStream::open(directory.path(), Default::default()).await?);
        let host = AgentHost::new(
            provider.clone(),
            super::script(Vec::new()),
            within,
            ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
            Arc::new(SystemClock),
        )?;
        assert_eq!(host.state(&child).await?, committed);
        assert_eq!(host.state(&root).await?, parent_committed);
        let reopened = host.runtime().store().load(&host.actor(&child)?).await?;
        assert_eq!(
            reopened.state(),
            Some(&actor_state),
            "reopen lost the committed checkpoint/cursor/alarm tuple"
        );
        if oversized {
            // Archive after restart must retain the completed owner's outcome.
            // The empty driver also refuses any accidental turn redispatch.
            let Delivered::Committed { inbox } = host
                .archive(&child, &user(), &intent("archive-after-reopen"))
                .await?
            else {
                return Err("terminal child archive was refused".into());
            };
            host.drain(&child).await?;
            assert_eq!(
                values(provider.as_ref(), root.inbox()?).await?,
                parent_records,
                "archive reported a completed child again"
            );
            let mut expected = committed;
            expected.closed = true;
            expected.dropped_before = expected.dropped_before.max(inbox);
            assert_eq!(host.state(&child).await?, expected);
            let archived = host.runtime().store().load(&host.actor(&child)?).await?;
            let archived = archived.state().expect("archived child actor");
            assert_eq!(archived.cursor(INBOX), Some(inbox + 1));
            assert!(archived.alarm().pending().is_none());
        }
    }
    Ok(agents)
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_reopen_preserves_and_replaces_deadlines_until_control_clears_them() -> Result {
    use acyclic_actors::local::ControlledClock;
    use acyclic_stream::LocalStream;

    for archive in [false, true] {
        let directory = tempfile::tempdir()?;
        let (clock, mut time) = ControlledClock::new(0);
        let clock = Arc::new(clock);
        let within = ActorReference::new(b"account", b"agents", b"owner")?;
        let waiting = |checkpoint: &[u8], timeout, outs| TurnStep {
            reserved_children: 0,
            checkpoint: Some(checkpoint.to_vec()),
            outs,
            wait: Wait::Until(Until::sleep(timeout)),
            drops: None,
        };
        let provider = Arc::new(LocalStream::open(directory.path(), Default::default()).await?);
        let host = AgentHost::new(
            provider.clone(),
            super::script(vec![waiting(
                b"first",
                10,
                ["a", "b", "c"]
                    .into_iter()
                    .map(|name| Out::Spawn {
                        profile: None,
                        history: None,
                        intent: intent(name),
                        brief: None,
                        volumes: Vec::new(),
                    })
                    .collect(),
            )]),
            within.clone(),
            ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
            clock.clone(),
        )?;
        let root = host.create(&intent("create"), adopted(id(1))).await?;
        host.drain(&root).await?;
        let input = InboxRecord::ApprovalRequested(ApprovalRequested {
            child: root.child(&id(2))?,
            id: intent("start"),
        });
        host.send(&root, &user(), &input).await?;
        assert_eq!(
            host.drain_event(&root).await?,
            Some(10),
            "pending deadline lost"
        );
        let first = host.state(&root).await?;
        assert_eq!(first.open_children.len(), 3);
        for child in &first.open_children {
            host.drain(child).await?;
        }
        let snapshot = host.runtime().store().load(&host.actor(&root)?).await?;
        let first_actor = snapshot.state().ok_or("missing waiting actor")?.clone();
        drop(host);
        drop(provider);

        let provider = Arc::new(LocalStream::open(directory.path(), Default::default()).await?);
        let host = AgentHost::new(
            provider.clone(),
            super::script(vec![waiting(b"second", 20, Vec::new())]),
            within.clone(),
            ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
            clock.clone(),
        )?;
        assert_eq!(host.drain_event(&root).await?, Some(10));
        assert_eq!(host.state(&root).await?, first, "deadline fired early");
        let snapshot = host.runtime().store().load(&host.actor(&root)?).await?;
        assert_eq!(snapshot.state(), Some(&first_actor));
        time.advance_to(10)?;
        assert_eq!(
            host.drain_event(&root).await?,
            Some(30),
            "delivered deadline was not replaced"
        );
        let second = host.state(&root).await?;
        assert_eq!(
            second.turn.as_ref().map(|turn| turn.checkpoint.as_slice()),
            Some(b"second".as_slice())
        );
        let snapshot = host.runtime().store().load(&host.actor(&root)?).await?;
        let second_actor = snapshot.state().ok_or("missing resumed actor")?.clone();
        assert!(second_actor.alarm().generation() > first_actor.alarm().generation());
        assert_eq!(second_actor.alarm().pending(), Some(30));
        drop(host);
        drop(provider);

        let provider = Arc::new(LocalStream::open(directory.path(), Default::default()).await?);
        let host = AgentHost::new(
            provider,
            super::script(Vec::new()),
            within.clone(),
            ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
            clock.clone(),
        )?;
        assert_eq!(host.drain_event(&root).await?, Some(30));
        let snapshot = host.runtime().store().load(&host.actor(&root)?).await?;
        assert_eq!(snapshot.state(), Some(&second_actor));
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
        assert_eq!(
            host.drain_event(&root).await?,
            None,
            "control retained deadline"
        );
        let stopped = host.state(&root).await?;
        assert_eq!(stopped.closed, archive);
        assert_eq!(stopped.turn, None);
        let snapshot = host.runtime().store().load(&host.actor(&root)?).await?;
        let stopped_actor = snapshot.state().ok_or("missing stopped actor")?.clone();
        assert!(stopped_actor.alarm().generation() > second_actor.alarm().generation());
        assert!(stopped_actor.alarm().pending().is_none());
        time.advance_to(30)?;
        // Finite delivery must leave the cleared generation untouched. The
        // empty driver refuses an early, duplicate or stale continuation.
        assert_eq!(host.drain_event(&root).await?, None);
        assert_eq!(host.state(&root).await?, stopped);
        let snapshot = host.runtime().store().load(&host.actor(&root)?).await?;
        assert_eq!(snapshot.state(), Some(&stopped_actor));
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_reopen_preserves_reserved_spawns_until_the_matching_approval() -> Result {
    use acyclic_stream::LocalStream;

    let directory = tempfile::tempdir()?;
    let provider = Arc::new(LocalStream::open(directory.path(), Default::default()).await?);
    let within = ActorReference::new(b"account", b"agents", b"owner")?;
    let spawn = |name: &str| Out::Spawn {
        profile: None,
        history: None,
        intent: intent(name),
        brief: None,
        volumes: Vec::new(),
    };
    let host = AgentHost::new(
        provider.clone(),
        super::script(vec![TurnStep {
            reserved_children: 3,
            checkpoint: Some(vec![7; 8]),
            outs: vec![spawn("existing-a"), spawn("existing-b")],
            wait: Wait::Approval {
                id: intent("decision"),
                binding: None,
            },
            drops: None,
        }]),
        within.clone(),
        ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
        Arc::new(SystemClock),
    )?;
    let root = host.create(&intent("create"), adopted(id(1))).await?;
    host.drain(&root).await?;
    host.send(
        &root,
        &user(),
        &InboxRecord::ApprovalRequested(ApprovalRequested {
            child: root.child(&id(2))?,
            id: intent("question"),
        }),
    )
    .await?;
    host.drain(&root).await?;
    let paused = host.state(&root).await?;
    assert_eq!(paused.open_children.len(), 2);
    assert_eq!(paused.reserved_children, 3);
    assert_eq!(
        paused.turn.as_ref().map(|turn| turn.checkpoint.as_slice()),
        Some([7; 8].as_slice())
    );
    // Neither the runtime nor the native provider survives the reopen.
    drop(host);
    drop(provider);
    let provider = Arc::new(LocalStream::open(directory.path(), Default::default()).await?);
    let host = AgentHost::new(
        provider,
        super::script(vec![
            TurnStep {
                reserved_children: 0,
                checkpoint: None,
                outs: vec![
                    spawn("recorded-a"),
                    spawn("recorded-b"),
                    spawn("recorded-c"),
                ],
                wait: Wait::Input,
                drops: None,
            },
            // Once that turn settles, the unrelated queued input can run.
            TurnStep {
                reserved_children: 0,
                checkpoint: None,
                outs: Vec::new(),
                wait: Wait::Input,
                drops: None,
            },
        ]),
        within,
        ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
        Arc::new(SystemClock),
    )?;
    assert_eq!(host.state(&root).await?, paused);
    for decision in ["unrelated", "decision"] {
        host.send(
            &root,
            &user(),
            &InboxRecord::Approval(Approval {
                id: intent(decision),
            }),
        )
        .await?;
        host.drain(&root).await?;
        let state = host.state(&root).await?;
        if decision == "unrelated" {
            assert_eq!(state.turn, paused.turn);
            assert_eq!(state.phase, paused.phase);
            assert_eq!(state.reserved_children, 3);
            assert_eq!(state.open_children, paused.open_children);
        } else {
            assert_eq!(state.phase, Phase::Idle);
            assert_eq!(state.turn, None);
            assert_eq!(state.reserved_children, 0);
            assert_eq!(state.open_children.len(), 5);
            for child in state.open_children {
                assert_eq!(host.state(&child).await?.parent.as_ref(), Some(&root));
            }
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
async fn an_inbox_append_wakes_the_agent_and_its_spawn_creates_the_child_actor() -> Result {
    #[cfg(feature = "test-support")]
    return super::control_trace::runtime(spawn_and_archive()).await;
    #[cfg(not(feature = "test-support"))]
    spawn_and_archive().await
}

async fn spawn_and_archive() -> Result {
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
    assert_eq!(values(provider.as_ref(), child.inbox()?).await?.len(), 3);
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
    let failures = host.runtime().drain().await;
    assert!(failures.is_empty(), "{failures:?}");
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
    assert_eq!(values(provider.as_ref(), root.inbox()?).await?.len(), 3);

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

async fn pending_request_host<P: acyclic_harness_core::store::JournalProvider + 'static>(
    provider: Arc<P>,
) -> Result<(AgentHost<P>, AgentPath, u64)> {
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
    let message = InboxRecord::Message(Message {
        from: user(),
        id: intent("message"),
        body: Box::new(super::driver::file("message")?),
        attachments: None,
    });
    let Delivered::Committed { inbox: message_at } = host.send(&root, &user(), &message).await?
    else {
        return Err("message was not committed".into());
    };
    assert_eq!(
        host.reply(&root, &intent("message"), message_at).await?,
        Reply::Pending
    );
    for (id, at) in [
        (intent("other"), message_at),
        (intent("message"), message_at + 1),
        (intent("message"), u64::MAX),
    ] {
        assert!(
            matches!(host.reply(&root, &id, at).await, Err(Error::Conflict(_))),
            "reply wait admitted an absent or mismatched original input"
        );
    }
    Ok((host, root, message_at))
}

#[tokio::test]
async fn reply_waits_require_original_committed_requests() -> Result {
    #[cfg(feature = "filesystem-local")]
    {
        let directory = Arc::new(tempfile::tempdir()?);
        let provider = acyclic_stream::LocalStream::open_with_ownership_anchor(
            directory.path(),
            Default::default(),
            acyclic_native_runtime::OwnershipAnchor::new(directory.clone()),
        )
        .await?;
        let _ = pending_request_host(Arc::new(provider)).await?;
    }
    let (host, root, message_at) = pending_request_host(Arc::new(MemoryStream::default())).await?;
    for at in [message_at, message_at + 1] {
        let (spawn, _) = requested_child(&root, at)?;
        let waiting = host.spawned(&root, at, &spawn);
        tokio::pin!(waiting);
        assert!(
            matches!(
                futures::poll!(&mut waiting),
                std::task::Poll::Ready(Err(Error::Conflict(_)))
            ),
            "spawn wait admitted an absent or non-spawn original input"
        );
    }
    let request = InboxRecord::Spawn(SpawnRequest {
        profile: acyclic_harness_core::agent::CreationConfig {
            context: acyclic_harness_core::agent::SpawnContext::Fork,
            template: None,
            peers: Default::default(),
        },
        history: None,
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
        let mut reply = core::pin::pin!(reply);
        assert!(matches!(
            futures::poll!(reply.as_mut()),
            std::task::Poll::Ready(Err(Error::Conflict(_)))
        ));
    }
    // Until the parent's step commits, the reply waits rather than guessing.
    let reply = host.spawned(&root, inbox, &spawn);
    let mut reply = core::pin::pin!(reply);
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
    let Delivered::Committed { inbox: child_inbox } = host.send(&child, &user(), &request).await?
    else {
        return Err("descendant spawn request was not committed".into());
    };
    let (descendant_spawn, descendant) = requested_child(&child, child_inbox)?;
    let waiting = host.spawned(&child, child_inbox, &descendant_spawn);
    tokio::pin!(waiting);
    assert!(futures::poll!(&mut waiting).is_pending());
    assert!(host.runtime().drain().await.is_empty());
    let outcome = match futures::poll!(&mut waiting) {
        std::task::Poll::Ready(result) => result?,
        std::task::Poll::Pending => {
            return Err("committed descendant spawn retained its wait".into());
        }
    };
    assert_eq!(
        outcome,
        SpawnOutcome::Spawned {
            child: descendant.clone()
        }
    );
    assert_eq!(host.state(&descendant).await?.parent.as_ref(), Some(&child));
    assert_eq!(
        host.reply(&root, &intent("message"), message_at).await?,
        Reply::Dropped
    );
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
            profile: acyclic_harness_core::agent::CreationConfig {
                context: acyclic_harness_core::agent::SpawnContext::Fork,
                template: None,
                peers: Default::default(),
            },
            history: None,
            by: user(),
            id: intent(&format!("s{n}")),
            brief: Box::new(super::driver::file("task").expect("file")),
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
            profile: acyclic_harness_core::agent::CreationConfig {
                context: acyclic_harness_core::agent::SpawnContext::Fork,
                template: None,
                peers: Default::default(),
            },
            history: None,
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
