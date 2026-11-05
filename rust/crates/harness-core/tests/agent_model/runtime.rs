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
async fn physical_driver_frames_settle_without_losing_the_terminal_commit() -> Result {
    use acyclic_harness_core::conversation::{FileDescriptor, FileRef};
    use acyclic_stream::{ReadRequest, StreamProvider as _};
    use futures::TryStreamExt as _;

    struct Frames(Vec<Out>, Option<(bool, usize)>);
    impl TurnDriver for Frames {
        fn step<'a>(
            &'a self,
            _: &'a AgentState,
            _: u64,
            _: Option<Queued>,
            _: acyclic_actors::guest::CancellationToken,
        ) -> TurnFuture<'a> {
            Box::pin(async move {
                let wait = if let Some((approval, status_bytes)) = self.1 {
                    let text = "x".repeat(status_bytes);
                    if approval {
                        Wait::Approval {
                            id: IntentId::try_from(text)?,
                            binding: None,
                        }
                    } else {
                        Wait::Host(text)
                    }
                } else {
                    Wait::Done(AgentOutcome::Succeeded { result: None })
                };
                Ok(TurnStep {
                    reserved_children: 0,
                    checkpoint: Some(vec![7]),
                    outs: self.0.clone(),
                    wait,
                    drops: None,
                })
            })
        }
    }

    let maximum = acyclic_stream::MAX_RECORD_BYTES;
    let done = |message: String| {
        InboxRecord::ChildDone(ChildDone {
            child: AgentPath::root(&id(3)),
            outcome: AgentOutcome::Failed { message },
        })
    };
    // Measure the actual long-string envelope; MessagePack's string header
    // grows with its payload, unlike the empty-string header.
    let envelope = done("x".repeat(maximum)).encode()?.len() - maximum;
    let exact = done("x".repeat(maximum - envelope));
    assert_eq!(exact.encode()?.len(), maximum);
    // Check the encoded envelope against the actual physical record boundary.
    let escaped = done("\u{0001}".repeat(maximum));
    assert!(escaped.encode()?.len() > maximum);
    let peer = AgentPath::root(&id(2));
    for (out, admitted, wait) in [
        (Out::Conversation(vec![b'x'; maximum]), true, None),
        (Out::Conversation(vec![b'x'; maximum + 1]), false, None),
        (
            Out::Send {
                to: peer.clone(),
                record: exact,
            },
            true,
            None,
        ),
        (
            Out::Send {
                to: peer.clone(),
                record: escaped,
            },
            false,
            None,
        ),
        // The wait is also retained inside the original checkpoint. Keep its
        // admitted control within that separately configured physical record.
        (
            Out::Conversation(vec![b'x']),
            true,
            Some((false, maximum / 2)),
        ),
        (
            Out::Conversation(vec![b'x']),
            false,
            Some((false, maximum + 1)),
        ),
        (
            Out::Conversation(vec![b'x']),
            true,
            Some((true, maximum / 2)),
        ),
        (
            Out::Conversation(vec![b'x']),
            false,
            Some((true, maximum + 1)),
        ),
    ] {
        let provider = Arc::new(MemoryStream::default());
        super::create(&provider, id(2)).await?;
        let conversation = matches!(out, Out::Conversation(_));
        let expected = out.clone();
        let host = AgentHost::new(
            provider.clone(),
            Frames(vec![out], wait),
            ActorReference::new(b"account", b"agents", b"owner")?,
            ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
            Arc::new(SystemClock),
        )?;
        let mut adoption = adopted(id(1));
        adoption.brief = Some(super::driver::file("frames")?);
        let root = host.create(&intent("create"), adoption).await?;
        host.drain(&root).await?;
        let state = host.state(&root).await?;
        if admitted && let Some((approval, ..)) = wait {
            assert!(if approval {
                matches!(state.phase, Phase::WaitingApproval { .. })
            } else {
                matches!(state.phase, Phase::WaitingHost { .. })
            });
            assert_eq!(
                state
                    .turn
                    .as_ref()
                    .ok_or("missing waiting checkpoint")?
                    .checkpoint,
                vec![7]
            );
        } else {
            assert_eq!(
                state.phase,
                Phase::Done {
                    outcome: if admitted {
                        AgentOutcome::Succeeded { result: None }
                    } else {
                        AgentOutcome::Failed {
                            message: INADMISSIBLE_EFFECT.into(),
                        }
                    }
                }
            );
            assert!(state.turn.is_none());
        }
        let status = values(provider.as_ref(), root.status()?).await?;
        assert_eq!(
            StatusRecord::decode(status.last().ok_or("missing terminal status")?)?.phase,
            state.phase
        );
        let records = values(provider.as_ref(), root.conversation()?).await?;
        if admitted && conversation {
            let Out::Conversation(bytes) = &expected else {
                unreachable!()
            };
            assert_eq!(records.as_slice(), std::slice::from_ref(bytes));
        } else {
            assert!(records.is_empty());
        }
        let sent = values(provider.as_ref(), peer.inbox()?).await?;
        assert_eq!(sent.len(), 1 + usize::from(admitted && !conversation));
        if admitted && !conversation {
            let Out::Send { record, .. } = &expected else {
                unreachable!()
            };
            assert_eq!(&InboxRecord::decode(&sent[1])?, record);
        }
    }
    fn result_file(field: usize, bytes: usize) -> acyclic_harness_core::Result<FileRef> {
        let original = super::driver::file("result")?;
        FileRef::new(
            original.volume().clone(),
            if field == 0 {
                "x".repeat(bytes)
            } else {
                "result".into()
            },
            if field == 3 {
                "x".repeat(bytes)
            } else {
                original.version().into()
            },
            FileDescriptor::new(
                [0; 32],
                0,
                if field == 2 {
                    format!("application/{}", "x".repeat(bytes))
                } else {
                    "application/x".into()
                },
            )?,
            if field == 1 {
                "x".repeat(bytes)
            } else {
                "result".into()
            },
            &mut None,
        )
    }

    struct Results {
        field: usize,
        bytes: usize,
        children: bool,
        parent_only: bool,
    }
    impl TurnDriver for Results {
        fn step<'a>(
            &'a self,
            agent: &'a AgentState,
            cursor: u64,
            input: Option<Queued>,
            _: acyclic_actors::guest::CancellationToken,
        ) -> TurnFuture<'a> {
            Box::pin(async move {
                if self.children && agent.parent.is_none() {
                    return Ok(TurnStep {
                        reserved_children: 0,
                        checkpoint: None,
                        outs: if matches!(
                            input,
                            Some(Queued {
                                input: TurnInput::Brief { .. },
                                ..
                            })
                        ) {
                            vec![Out::Spawn {
                                profile: None,
                                history: None,
                                intent: intent("result"),
                                brief: Some(
                                    super::driver::file("child")
                                        .map_err(|e| Error::Turn(Box::new(e)))?,
                                ),
                                volumes: Vec::new(),
                            }]
                        } else {
                            Vec::new()
                        },
                        wait: Wait::Input,
                        drops: None,
                    });
                }
                let outcome = |bytes| {
                    result_file(self.field, bytes)
                        .map(|file| AgentOutcome::Succeeded {
                            result: Some(Box::new(file)),
                        })
                        .map_err(|error| Error::Turn(Box::new(error)))
                };
                let bytes = if self.parent_only {
                    let envelope = InboxRecord::ChildDone(ChildDone {
                        child: agent.path.clone(),
                        outcome: outcome(1)?,
                    })
                    .encode()?
                    .len()
                        - 1;
                    acyclic_stream::MAX_RECORD_BYTES + 1 - envelope
                } else {
                    self.bytes
                };
                let outcome = outcome(bytes)?;
                if self.parent_only {
                    assert!(
                        StatusRecord {
                            phase: Phase::Done {
                                outcome: outcome.clone()
                            },
                            cursor,
                            conversation: agent.conversation_tail + 1
                        }
                        .encode()?
                        .len()
                            <= acyclic_stream::MAX_RECORD_BYTES
                    );
                    assert_eq!(
                        InboxRecord::ChildDone(ChildDone {
                            child: agent.path.clone(),
                            outcome: outcome.clone()
                        })
                        .encode()?
                        .len(),
                        acyclic_stream::MAX_RECORD_BYTES + 1
                    );
                }
                Ok(TurnStep {
                    reserved_children: 0,
                    checkpoint: Some(vec![7]),
                    outs: vec![Out::Conversation(vec![b'x'])],
                    wait: Wait::Done(outcome),
                    drops: None,
                })
            })
        }
    }

    // Both generated envelopes matter: a child's notification can overflow
    // while its own status fits. Exercise every unbounded FileRef text field.
    for (children, field, bytes, parent_only) in [
        (false, 0, maximum / 2, false),
        (false, 0, maximum, false),
        (false, 1, maximum / 2, false),
        (false, 1, maximum, false),
        (false, 2, maximum / 2, false),
        (false, 2, maximum, false),
        (false, 3, maximum / 2, false),
        (false, 3, maximum, false),
        (true, 0, maximum / 2, false),
        (true, 0, maximum, false),
        (true, 1, maximum / 2, false),
        (true, 1, maximum, false),
        (true, 2, maximum / 2, false),
        (true, 2, maximum, false),
        (true, 3, maximum / 2, false),
        (true, 3, maximum, false),
        (true, 2, 0, true),
    ] {
        let provider = Arc::new(MemoryStream::default());
        let host = AgentHost::new(
            provider.clone(),
            Results {
                field,
                bytes,
                children,
                parent_only,
            },
            ActorReference::new(b"account", b"agents", b"owner")?,
            ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
            Arc::new(SystemClock),
        )?;
        let mut adoption = adopted(id(1));
        adoption.brief = Some(super::driver::file("frames")?);
        let root = host.create(&intent("create"), adoption).await?;
        host.drain(&root).await?;
        let target = if children {
            root.child(&AgentId::derive(&root.id(), "result"))
        } else {
            root.clone()
        };
        if children {
            host.drain(&target).await?;
        }
        let admitted = bytes < maximum && !parent_only;
        let outcome = if admitted {
            AgentOutcome::Succeeded {
                result: Some(Box::new(result_file(field, bytes)?)),
            }
        } else {
            AgentOutcome::Failed {
                message: INADMISSIBLE_EFFECT.into(),
            }
        };
        let state = host.state(&target).await?;
        assert!(
            matches!(state.phase, Phase::Done { .. }),
            "completion missing: children={children} field={field} bytes={bytes} parent_only={parent_only}"
        );
        assert_eq!(
            state.phase,
            Phase::Done {
                outcome: outcome.clone()
            }
        );
        assert!(state.turn.is_none());
        assert_eq!(
            values(provider.as_ref(), target.conversation()?)
                .await?
                .len(),
            usize::from(admitted)
        );
        let status = provider
            .read(ReadRequest {
                path: target.status()?,
                from: state.status_tail - 1,
                limit: 1,
                key: None,
                at_least: Default::default(),
            })
            .await?
            .try_next()
            .await?
            .ok_or("missing result status")?;
        assert_eq!(StatusRecord::decode(&status.value)?.phase, state.phase);
        if children {
            let records = provider
                .read(ReadRequest {
                    path: root.inbox()?,
                    from: 1,
                    limit: 1,
                    key: None,
                    at_least: Default::default(),
                })
                .await?
                .try_next()
                .await?
                .ok_or("missing result notification")?;
            assert_eq!(records.commit_id, status.commit_id);
            assert_eq!(
                InboxRecord::decode(&records.value)?,
                InboxRecord::ChildDone(ChildDone {
                    child: target,
                    outcome
                })
            );
            host.drain(&root).await?;
            assert!(host.state(&root).await?.open_children.is_empty());
        }
    }
    Ok(())
}

#[tokio::test]
async fn large_failure_publishes_complete_diagnostic_with_original_child_settlement() -> Result {
    use acyclic_actors::guest::CancellationToken;
    use acyclic_stream::{AppendRequest, StreamProvider as _, StreamProviderExt as _};
    use futures::TryStreamExt as _;

    #[derive(Clone)]
    struct Failing(String);
    impl TurnDriver for Failing {
        fn step<'a>(
            &'a self,
            agent: &'a AgentState,
            _: u64,
            input: Option<Queued>,
            _: CancellationToken,
        ) -> TurnFuture<'a> {
            Box::pin(async move {
                if agent.parent.is_some() {
                    return Err(Error::Turn(Box::new(acyclic_harness_core::Error::Invalid(
                        self.0.clone(),
                    ))));
                }
                Ok(TurnStep {
                    reserved_children: 0,
                    checkpoint: None,
                    outs: if matches!(
                        input,
                        Some(Queued {
                            input: TurnInput::Brief { .. },
                            ..
                        })
                    ) {
                        vec![Out::Spawn {
                            profile: None,
                            history: None,
                            intent: intent("large-failure"),
                            brief: Some(
                                super::driver::file("child")
                                    .map_err(|error| Error::Turn(Box::new(error)))?,
                            ),
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

    // Controls and multibyte text exercise both JSON expansion and chunk edges.
    let driver = Failing(format!(
        "{}complete suffix",
        "\u{0001}\"ÃƒÂ©".repeat(acyclic_stream::MAX_RECORD_BYTES)
    ));
    let expected = Error::Turn(Box::new(acyclic_harness_core::Error::Invalid(
        driver.0.clone(),
    )))
    .to_string();
    let provider = Arc::new(MemoryStream::default());
    let host = AgentHost::new(
        provider.clone(),
        driver.clone(),
        ActorReference::new(b"account", b"agents", b"owner")?,
        ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
        Arc::new(SystemClock),
    )?;
    let mut adoption = adopted(id(1));
    adoption.brief = Some(super::driver::file("root")?);
    let root = host.create(&intent("create"), adoption).await?;
    // A fork inherits its ancestor's staged records. Even the same diagnostic
    // bytes must use the child's original identity rather than that inherited
    // slot; an interrupted ancestor publication must not prevent settlement.
    let ancestor = Diagnostic {
        agent: root.clone(),
        digest: *blake3::hash(expected.as_bytes()).as_bytes(),
        bytes: expected.len() as u64,
    };
    provider
        .append(AppendRequest {
            path: ancestor.path()?,
            records: vec![
                expected.as_bytes()[..acyclic_stream::MAX_RECORD_BYTES]
                    .to_vec()
                    .into(),
            ],
            if_tail: Some(0),
            idempotency_key: intent_key("ancestor-diagnostic", &["original"])?,
        })
        .await?;
    let failures = host.runtime().drain().await;
    assert!(failures.is_empty(), "{failures:?}");
    let child = root.child(&AgentId::derive(&root.id(), "large-failure"));
    let snapshot = store(&provider)?.load(&host.actor(&child)?).await?;
    let cursor = snapshot
        .state()
        .and_then(|state| state.cursor(INBOX))
        .ok_or("missing child cursor")?;
    let mut records = provider
        .read(acyclic_stream::ReadRequest {
            path: child.inbox()?,
            from: cursor,
            limit: 1,
            key: None,
            at_least: Default::default(),
        })
        .await?;
    let record = records.try_next().await?.ok_or("missing child adoption")?;
    let step = step(
        &driver,
        &child,
        None,
        (cursor + 1, 0),
        Wake::Inbox {
            sequence: cursor,
            record: InboxRecord::decode(&record.value)?,
        },
        CancellationToken::new(),
    )
    .await?;
    let Out::Diagnostic { diagnostic, text } = step
        .outs
        .iter()
        .find(|out| matches!(out, Out::Diagnostic { .. }))
        .ok_or("missing diagnostic staging")?
    else {
        return Err("unexpected staging effect".into());
    };
    assert_eq!(text, &expected);
    assert_eq!(diagnostic.bytes, expected.len() as u64);
    let before = values(provider.as_ref(), root.inbox()?).await?;
    // Neither an absent payload nor an incomplete prefix may publish the status
    // or notify the parent. A failed conditional leaves the same key unspent.
    let request = lower(
        &step,
        cursor + 1,
        intent_key("partial-diagnostic", &["one"])?,
    )?;
    for prefix in 0..2 {
        if prefix == 1 {
            let path = diagnostic.path()?;
            provider
                .append(AppendRequest {
                    idempotency_key: intent_key("diagnostic", &[path.as_str(), "0"])?,
                    path,
                    records: vec![
                        text.as_bytes()[..acyclic_stream::MAX_RECORD_BYTES]
                            .to_vec()
                            .into(),
                    ],
                    if_tail: Some(0),
                })
                .await?;
        }
        assert!(matches!(
            submit(provider.as_ref(), request.clone()).await?,
            Submitted::Conflict(_)
        ));
        assert_eq!(values(provider.as_ref(), root.inbox()?).await?, before);
    }
    host.host(&child)?;
    let failures = host.runtime().drain().await;
    assert!(failures.is_empty(), "{failures:?}");
    let state = host.state(&child).await?;
    assert_eq!(
        state.phase,
        Phase::Done {
            outcome: AgentOutcome::FailedDiagnostic {
                diagnostic: diagnostic.clone()
            }
        }
    );
    let mut complete = Vec::new();
    for from in 0..diagnostic.records() {
        let page: Vec<_> = diagnostic
            .page(provider.as_ref(), from, 1)
            .await?
            .try_collect()
            .await?;
        assert_eq!(page.len(), 1);
        complete.extend_from_slice(&page[0].value);
    }
    assert_eq!(complete, expected.as_bytes());
    assert_eq!(blake3::hash(&complete).as_bytes(), &diagnostic.digest);
    let child_status = provider
        .read(acyclic_stream::ReadRequest {
            path: child.status()?,
            from: state.status_tail - 1,
            limit: 1,
            key: None,
            at_least: Default::default(),
        })
        .await?
        .try_next()
        .await?
        .ok_or("missing terminal status")?;
    let parent_done = provider
        .read(acyclic_stream::ReadRequest {
            path: root.inbox()?,
            from: before.len() as u64,
            limit: 1,
            key: None,
            at_least: Default::default(),
        })
        .await?
        .try_next()
        .await?
        .ok_or("missing parent notification")?;
    assert_eq!(child_status.commit_id, parent_done.commit_id);
    assert_eq!(
        StatusRecord::decode(&child_status.value)?.phase,
        state.phase
    );
    assert_eq!(
        InboxRecord::decode(&parent_done.value)?,
        InboxRecord::ChildDone(ChildDone {
            child,
            outcome: AgentOutcome::FailedDiagnostic {
                diagnostic: diagnostic.clone()
            },
        })
    );
    // This finite host has no activation dispatcher running. Deliver the
    // parent's committed ChildDone through its existing event-owned drain.
    host.drain(&root).await?;
    assert!(host.state(&root).await?.open_children.is_empty());
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
async fn committed_checkpoints_carry_final_tails_after_native_reopen() -> Result {
    use acyclic_stream::LocalStream;

    for checkpoint_bytes in [8, 1024] {
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
                checkpoint: Some(vec![7; checkpoint_bytes]),
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
            child: root.child(&id(2)),
            id: intent("approval"),
        });
        assert!(matches!(
            host.send(&root, &user(), &input).await?,
            Delivered::Committed { .. }
        ));
        host.drain(&root).await?;
        let parent_before = host.state(&root).await?;
        assert_eq!(parent_before.open_children.len(), 3);
        for id in parent_before.open_children.page(None, 3).await? {
            host.drain(&root.child(&id)).await?;
        }
        let child = root.child(
            &parent_before
                .open_children
                .first()
                .await?
                .ok_or("spawned child")?,
        );
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
        let expected_reports = Vec::new();
        assert_eq!(
            reports, expected_reports,
            "shed checkpoint lost its parent report"
        );
        host.drain(&root).await?;
        let parent_committed = host.state(&root).await?;
        assert_eq!(parent_committed.open_children, parent_before.open_children);
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
            Some(vec![7; checkpoint_bytes].as_slice())
        );

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
    }
    Ok(())
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
            child: root.child(&id(2)),
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
        for id in first.open_children.page(None, 3).await? {
            host.drain(&root.child(&id)).await?;
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
            child: root.child(&id(2)),
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
            for id in state.open_children.page(None, 5).await? {
                assert_eq!(
                    host.state(&root.child(&id)).await?.parent.as_ref(),
                    Some(&root)
                );
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
        child: root.child(&id(2)),
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
async fn conversation_follow_waits_on_the_original_checkpoint_and_preserves_refusals() -> Result {
    use acyclic_stream::{FollowRequest, StreamError, StreamProvider, StreamProviderExt};
    use futures::TryStreamExt;

    let provider = Arc::new(MemoryStream::default());
    let entry = ConversationEntry {
        steering: None,
        inbox: 1,
        input: TurnInput::Message {
            from: user(),
            id: intent("message"),
            body: Box::new(super::driver::file("input")?),
            attachments: None,
        },
        output: super::driver::file("output")?,
        outcome: Default::default(),
        goal: None,
        targets: None,
        tool_results: None,
        notices: None,
    }
    .encode()?;
    let host = AgentHost::new(
        provider.clone(),
        super::script(vec![TurnStep {
            reserved_children: 0,
            checkpoint: None,
            outs: vec![Out::Conversation(entry.clone())],
            wait: Wait::Input,
            drops: None,
        }]),
        ActorReference::new(b"account", b"agents", b"owner")?,
        ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
        Arc::new(SystemClock),
    )?;
    let root = host.create(&intent("create"), adopted(id(1))).await?;
    // The primitive's missing-path contract still refuses and creates nothing.
    host.drain(&root).await?;
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
    host.send(
        &root,
        &user(),
        &InboxRecord::ApprovalRequested(ApprovalRequested {
            child: root.child(&id(2)),
            id: intent("publish"),
        }),
    )
    .await?;
    assert!(host.runtime().drain().await.is_empty());
    assert_eq!(
        provider.tail(root.status()?).await?,
        1,
        "same-phase publication adds no status"
    );
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
    let child = root.child(&AgentId::derive(&root.id(), "c"));
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
        child: root.child(&id(2)),
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
        child: root.child(&id(2)),
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
        profile: acyclic_harness_core::agent::CreationConfig {
            context: acyclic_harness_core::agent::SpawnContext::Fork,
            template: None,
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
            profile: acyclic_harness_core::agent::CreationConfig {
                context: acyclic_harness_core::agent::SpawnContext::Fork,
                template: None,
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
            child: root.child(&id(2)),
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
