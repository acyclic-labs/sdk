use super::*;
use crate::agent::{Cancellation, Message, Queued, Sender, TurnFuture, TurnStep, Wait};
use acyclic_actors::guest::{CancellationToken, SubscriptionCuts, wire};
use acyclic_stream::{
    AppendRequest, CommitOutcome, MemoryStream, ReadRequest, StreamProvider as _,
    StreamProviderExt as _,
};
use futures::TryStreamExt as _;
use tokio::sync::oneshot;

struct ReplayCapacity {
    inner: MemoryStream,
    replay: Mutex<Option<(acyclic_stream::StreamPath, u64)>>,
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl acyclic_stream::StreamProvider for ReplayCapacity {
    fn context(&self) -> acyclic_stream::ProviderContext<'_> {
        self.inner.context()
    }
    async fn heads(
        &self,
        request: acyclic_stream::HeadsRequest,
    ) -> std::result::Result<acyclic_stream::Heads, acyclic_stream::StreamError> {
        self.inner.heads(request).await
    }
    async fn inspect_idempotency(
        &self,
        key: acyclic_stream::IdempotencyKey,
    ) -> std::result::Result<
        Option<acyclic_stream::IdempotencyObservation>,
        acyclic_stream::StreamError,
    > {
        self.inner.inspect_idempotency(key).await
    }
    fn write(
        &self,
        request: acyclic_stream::WriteRequest,
    ) -> acyclic_stream::BoxProviderFuture<
        '_,
        std::result::Result<acyclic_stream::WriteOutcome, acyclic_stream::StreamError>,
    > {
        self.inner.write(request)
    }
    async fn read(
        &self,
        request: ReadRequest,
    ) -> std::result::Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> {
        let refused = {
            let mut replay = self.replay.lock().unwrap();
            if replay
                .as_ref()
                .is_some_and(|(path, from)| path == &request.path && *from == request.from)
            {
                replay.take();
                true
            } else {
                false
            }
        };
        if refused {
            return Err(acyclic_stream::StreamError::Capacity);
        }
        self.inner.read(request).await
    }
    async fn follow(
        &self,
        request: acyclic_stream::FollowRequest,
    ) -> std::result::Result<acyclic_stream::FollowStream, acyclic_stream::StreamError> {
        self.inner.follow(request).await
    }
    async fn read_commit(
        &self,
        id: acyclic_stream::CommitId,
    ) -> std::result::Result<acyclic_stream::CommittedEnvelope, acyclic_stream::StreamError> {
        self.inner.read_commit(id).await
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl acyclic_stream::NativeStreamProvider for ReplayCapacity {}

struct Opening(Mutex<Option<oneshot::Sender<u64>>>);

impl TurnDriver for Opening {
    fn step<'a>(
        &'a self,
        _: &'a AgentState,
        _: u64,
        input: Option<Queued>,
        cancellation: CancellationToken,
    ) -> TurnFuture<'a> {
        Box::pin(async move {
            let input = input.expect("refilled input").inbox;
            self.0.lock().unwrap().take().unwrap().send(input).unwrap();
            cancellation.cancelled().await;
            Ok(TurnStep {
                checkpoint: None,
                wait: Wait::Input,
                drops: None,
                outs: vec![Out::InputCancelled {
                    input,
                    conversation: 0,
                }],
            })
        })
    }
}

#[tokio::test]
async fn alarm_opening_from_overflow_accepts_only_its_selected_input_interrupt()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = Arc::new(ReplayCapacity {
        inner: MemoryStream::default(),
        replay: Mutex::new(None),
    });
    let id = AgentId::from_bytes([7; 16]);
    let adopted = ForkAdopted {
        agent: id.clone(),
        parent: None,
        profile: None,
        forked_at: 0,
        conversation_tail: 0,
        checkpoint: None,
        volumes: Vec::new(),
        brief: None,
        policy: None,
    };
    let created = crate::agent::create_root(&"create".try_into()?, adopted.clone())?;
    let path = created.child;
    let CommitOutcome::Committed(_) = provider.commit(created.request).await? else {
        panic!("creation refused");
    };
    let storage = crate::filesystem::MemoryHarnessStorage::new(
        crate::AgentId::from_bytes(id.to_bytes()),
        crate::conversation::Limits::default().file_bytes,
    )
    .await?;
    let file = storage
        .stage(
            crate::OperationId::new(),
            "message",
            b"input",
            "text/plain",
            "input",
        )
        .await?;
    provider
        .append(AppendRequest {
            path: path.inbox()?,
            records: vec![
                InboxRecord::Awaited.encode()?.into(),
                InboxRecord::Message(Message {
                    from: Sender::User {
                        principal: "owner".into(),
                    },
                    id: "request".try_into()?,
                    body: Box::new(file),
                    re: None,
                    attachments: None,
                })
                .encode()?
                .into(),
            ],
            if_tail: Some(1),
            idempotency_key: crate::agent::intent_key("overflow", &[])?,
        })
        .await?;
    let mut state = AgentState::adopted(path.clone(), &adopted)?;
    state.overflow = Some(1);
    let checkpoint =
        crate::agent::checkpoint::from_encoded(crate::agent::checkpoint::encode(&state)?)?;
    let actor = ActorReference::new(b"account", b"agents", id.as_str().as_bytes())?;
    let store = crate::agent::actor_store(&provider)?;
    let subscriptions = BTreeMap::from([(
        INBOX.into(),
        Subscription {
            path: path.inbox()?,
            paused: false,
            cursor: 3,
        },
    )]);
    let CommitOutcome::Committed(receipt) = provider
        .commit(acyclic_stream::CommitRequest {
            deadline_unix_millis: None,
            effects: vec![store.create(
                None,
                &actor,
                Creation {
                    checkpoint: Some(checkpoint.clone()),
                    subscriptions: subscriptions.clone(),
                    ..Creation::default()
                },
            )?],
            idempotency_key: crate::agent::intent_key("checkpoint", &[])?,
        })
        .await?
    else {
        panic!("actor checkpoint refused");
    };
    let current = crate::agent::settled(&provider, &actor, &path).await?;
    assert_eq!(current.input, Some(2));
    assert_eq!(current.phase, crate::agent::Phase::Running);
    assert!(!current.settled);
    let delivery = Delivery {
        request: acyclic_actors::guest::RequestId::new(1),
        actor: actor.clone(),
        epoch: 1,
        now_unix_ms: 100,
        trigger: Trigger::Alarm {
            generation: 1,
            now_unix_ms: 100,
        },
        record: None,
        subscriptions: SubscriptionCuts::new(store.path(&actor)?, receipt.commit_id, subscriptions),
    };
    let (started, starting) = oneshot::channel();
    let handler = AgentHandler::new(
        store,
        Opening(Mutex::new(Some(started))),
        ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
    )?;
    // Refuse the original overflow replay, not delivery acquisition. The same
    // accepted input must remain available to the subsequent real execution.
    *provider.replay.lock().unwrap() = Some((path.inbox()?, 1));
    assert!(!Error::Stream(acyclic_stream::StreamError::Capacity).uncertain());
    let refused = handler
        .on_record(
            delivery.clone(),
            Some(&checkpoint),
            CancellationToken::new(),
        )
        .await;
    assert!(
        matches!(refused, Err(GuestError::Pending(_))),
        "capacity must retain replay input"
    );
    assert!(
        provider.replay.lock().unwrap().is_none(),
        "capacity fault did not reach inbox replay"
    );
    let retained = crate::agent::actor_store(&provider)?.load(&actor).await?;
    let retained = retained.state().ok_or("missing retained actor")?;
    assert_eq!(retained.checkpoint(), Some(&checkpoint));
    assert_eq!(retained.cursor(INBOX), Some(3));
    let stop = CancellationToken::new();
    let running = handler.on_record(delivery.clone(), Some(&checkpoint), stop.clone());
    tokio::pin!(running);
    tokio::select! {
        input = starting => assert_eq!(input?, 2),
        result = &mut running => panic!("opening returned before cancellation: {:?}", result.map(|_| ())),
    }
    let cancel = InboxRecord::Cancel(Cancellation {
        by: Sender::User {
            principal: "owner".into(),
        },
        id: "cancel".try_into()?,
        input: 2,
        conversation: 0,
    });
    crate::agent::submit(
        provider.as_ref(),
        crate::agent::send(
            &path,
            &Sender::User {
                principal: "owner".into(),
            },
            &cancel,
        )?,
    )
    .await?;
    assert!(
        matches!(
            crate::agent::reply(provider.as_ref(), &mut state, 3, &"request".try_into()?, 2)
                .await?,
            crate::agent::Reply::Pending
        ),
        "an accepted cancellation must wait for the overflow-backed execution to retire"
    );
    let mut records = provider
        .read(ReadRequest {
            key: None,
            path: path.inbox()?,
            from: 3,
            limit: 1,
            at_least: Default::default(),
        })
        .await?;
    let mut stale = records.try_next().await?.expect("accepted control");
    // A delivered control must match the immutable accepted cancellation fact.
    // An unrelated conversation append is not completion of this input.
    stale.value = InboxRecord::Cancel(Cancellation {
        by: Sender::User {
            principal: "owner".into(),
        },
        id: "cancel".try_into()?,
        input: 2,
        conversation: 1,
    })
    .encode()?
    .into();
    assert!(
        !wire::accepts_interrupt(
            &handler,
            &wire::encode_interrupt(&delivery, Some(&checkpoint), INBOX, &stale)
        )
        .await?
    );
    let mut rejected = delivery.clone();
    rejected.trigger = Trigger::Record {
        subscription: INBOX.into(),
        sequence: stale.sequence,
    };
    rejected.record = Some(stale);
    let transition = handler
        .on_record(rejected, Some(&checkpoint), CancellationToken::new())
        .await?;
    let retained = crate::agent::checkpoint_head(&transition.checkpoint)?.state;
    assert_eq!(retained.queued.front().expect("retained input").inbox, 2);
    assert_eq!(retained.phase, crate::agent::Phase::Idle);
    assert!(!retained.taken.contains(&2));
    let mut records = provider
        .read(ReadRequest {
            key: None,
            path: path.inbox()?,
            from: 3,
            limit: 1,
            at_least: Default::default(),
        })
        .await?;
    let mut record = records.try_next().await?.expect("cancel control");
    let query = wire::encode_interrupt(&delivery, Some(&checkpoint), INBOX, &record);
    assert!(wire::accepts_interrupt(&handler, &query).await?);
    record.value = InboxRecord::Cancel(Cancellation {
        by: Sender::User {
            principal: "owner".into(),
        },
        id: "other".try_into()?,
        input: 3,
        conversation: 0,
    })
    .encode()?
    .into();
    assert!(
        !wire::accepts_interrupt(
            &handler,
            &wire::encode_interrupt(&delivery, Some(&checkpoint), INBOX, &record)
        )
        .await?
    );
    stop.cancel();
    let transition = running.await?;
    let state = crate::agent::checkpoint_head(&transition.checkpoint)?.state;
    assert!(state.turn.is_none());
    assert_eq!(state.phase, crate::agent::Phase::Idle);
    Ok(())
}

#[tokio::test]
async fn unrelated_turn_completion_preserves_a_queued_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::agent::{ConversationEntry, Message, SteeringTranscript, TurnOutcome};
    use acyclic_stream::{CommitCondition, CommitEffect, CommitMutation, CommitRequest};
    let provider = Arc::new(ReplayCapacity {
        inner: MemoryStream::default(),
        replay: Mutex::new(None),
    });
    let id = AgentId::from_bytes([17; 16]);
    let adopted = ForkAdopted {
        agent: id.clone(),
        parent: None,
        profile: None,
        forked_at: 0,
        conversation_tail: 0,
        checkpoint: None,
        volumes: Vec::new(),
        brief: None,
        policy: None,
    };
    let root = crate::agent::create_root(&"create".try_into()?, adopted.clone())?;
    let path = root.child;
    crate::agent::submit(provider.as_ref(), root.request).await?;
    let storage = crate::filesystem::MemoryHarnessStorage::new(
        crate::AgentId::from_bytes(id.to_bytes()),
        4096,
    )
    .await?;
    let file = storage
        .stage(
            crate::OperationId::new(),
            "message",
            b"input",
            "text/plain",
            "input",
        )
        .await?;
    let user = Sender::User {
        principal: "owner".into(),
    };
    let message = |name: &str| Message {
        from: user.clone(),
        id: name.try_into().unwrap(),
        body: Box::new(file.clone()),
        re: None,
        attachments: None,
    };
    let first = InboxRecord::Message(message("first"));
    provider
        .append(AppendRequest {
            path: path.inbox()?,
            if_tail: Some(1),
            records: vec![
                first.encode()?.into(),
                InboxRecord::FollowUp(message("queued")).encode()?.into(),
                InboxRecord::Message(message("steered")).encode()?.into(),
                InboxRecord::FollowUp(message("reserved")).encode()?.into(),
            ],
            idempotency_key: crate::agent::intent_key("inputs", &[])?,
        })
        .await?;
    let cancel = |input| {
        InboxRecord::Cancel(Cancellation {
            by: user.clone(),
            id: format!("cancel-{input}").try_into().unwrap(),
            input,
            conversation: 0,
        })
    };
    // Capture B's cancellation while A is still the current conversation.
    let pending_cancel = crate::agent::send(&path, &user, &cancel(2))?;
    // A long turn crosses the former per-mutation bound in one atomic settlement.
    let count = acyclic_stream::MAX_ITEMS + 1;
    let additional = (0..count)
        .map(|index| {
            InboxRecord::Message(message(&format!("bulk-{index}")))
                .encode()
                .map(Into::into)
        })
        .collect::<Result<Vec<acyclic_stream::AppendRecord>, _>>()?;
    for (index, records) in additional.chunks(acyclic_stream::MAX_ITEMS).enumerate() {
        provider
            .append(AppendRequest {
                path: path.inbox()?,
                if_tail: None,
                records: records.to_vec(),
                idempotency_key: crate::agent::intent_key("long-turn", &[&index.to_string()])?,
            })
            .await?;
    }
    let through = 5 + count as u64;
    let entry = ConversationEntry {
        inbox: 1,
        input: crate::agent::input::input(1, first).unwrap().input,
        output: file.clone(),
        outcome: TurnOutcome::Completed,
        goal: None,
        observed_through: through,
        announcements: Vec::new(),
        tool_results: None,
        steering: Some(SteeringTranscript {
            file: file.clone(),
            through,
        }),
        notices: None,
    };
    let encoded = entry.encode()?;
    let outs = vec![Out::Conversation(encoded.clone())];
    let mut completed = CommitRequest {
        deadline_unix_millis: None,
        effects: vec![CommitEffect::hard(
            vec![CommitCondition::Tail {
                path: path.conversation()?,
                expected: 0,
            }],
            vec![CommitMutation::Append {
                path: path.conversation()?,
                records: vec![encoded.into()],
            }],
        )],
        idempotency_key: crate::agent::intent_key("complete-first", &[])?,
    };
    crate::agent::cancellation::complete(provider.as_ref(), &path, &outs, None, 1, &mut completed)
        .await?;
    crate::agent::submit(provider.as_ref(), completed).await?;
    crate::agent::submit(provider.as_ref(), pending_cancel).await?;
    assert_eq!(
        crate::agent::cancellation::cancelled(provider.as_ref(), &path, 2).await?,
        Some(0)
    );
    // Own and steered inputs are complete; reserved follow-ups are independent.
    for input in [1, 3, 4, through - 1] {
        crate::agent::submit(
            provider.as_ref(),
            crate::agent::send(&path, &user, &cancel(input))?,
        )
        .await?;
        assert_eq!(
            crate::agent::cancellation::cancelled(provider.as_ref(), &path, input).await?,
            (input == 4).then_some(0)
        );
    }
    let mut state = AgentState::adopted(path.clone(), &adopted)?;
    state.conversation_tail = 1;
    let never_open = Opening(Mutex::new(None));
    let driver = crate::agent::cancellation::Driver {
        provider: provider.as_ref(),
        inner: &never_open,
    };
    let step = driver
        .step(
            &state,
            through,
            crate::agent::input::input(2, InboxRecord::FollowUp(message("queued"))),
            CancellationToken::new(),
        )
        .await?;
    assert!(matches!(
        step.outs.as_slice(),
        [Out::InputCancelled { input: 2, .. }]
    ));
    Ok(())
}

#[tokio::test]
async fn cancelling_a_goal_continuation_closes_only_its_run_without_an_entry()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::agent::{ConversationEntry, Phase, Progress, Reply, TurnOutcome, Wake};
    struct BoundaryRetirement(Mutex<Option<TurnStep>>);
    impl TurnDriver for BoundaryRetirement {
        fn step<'a>(
            &'a self,
            _: &'a AgentState,
            _: u64,
            _: Option<Queued>,
            _: CancellationToken,
        ) -> TurnFuture<'a> {
            Box::pin(async move { Ok(self.0.lock().unwrap().take().unwrap()) })
        }
    }
    for (parked, steered, racing) in [
        (None, false, false),
        (Some(false), false, false),
        (Some(true), false, false),
        (None, true, false),
        (Some(true), true, false),
        (None, true, true),
    ] {
        let provider = MemoryStream::default();
        let id = AgentId::from_bytes([29; 16]);
        let adopted = ForkAdopted {
            agent: id.clone(),
            parent: None,
            profile: None,
            forked_at: 0,
            conversation_tail: 0,
            checkpoint: None,
            volumes: Vec::new(),
            brief: None,
            policy: None,
        };
        let created = crate::agent::create_root(&"goal-create".try_into()?, adopted.clone())?;
        let path = created.child;
        crate::agent::submit(&provider, created.request).await?;
        let storage = crate::filesystem::MemoryHarnessStorage::new(
            crate::AgentId::from_bytes(id.to_bytes()),
            crate::conversation::Limits::default().file_bytes,
        )
        .await?;
        let file = storage
            .stage(
                crate::OperationId::new(),
                "goal",
                b"goal",
                "text/plain",
                "goal",
            )
            .await?;
        let user = Sender::User {
            principal: "owner".into(),
        };
        let message = |from, name: String| -> Result<InboxRecord, crate::agent::Error> {
            Ok(InboxRecord::Message(Message {
                from,
                id: name.try_into()?,
                re: None,
                body: Box::new(file.clone()),
                attachments: None,
            }))
        };
        let runs = [
            crate::OperationId::from_bytes([31; 16]),
            crate::OperationId::from_bytes([32; 16]),
        ];
        let originals = [
            message(user.clone(), "stopped-goal".into())?,
            message(user.clone(), "other-goal".into())?,
        ];
        let mut continuations = [
            message(
                Sender::Agent { path: path.clone() },
                format!("goal:{}:1", runs[0]),
            )?,
            message(
                Sender::Agent { path: path.clone() },
                format!("goal:{}:1", runs[1]),
            )?,
        ];
        for input in &originals {
            crate::agent::submit(&provider, crate::agent::send(&path, &user, input)?).await?;
        }
        let active = message(user.clone(), "unrelated-turn".into())?;
        if steered {
            crate::agent::submit(&provider, crate::agent::send(&path, &user, &active)?).await?;
            // This goal retains its independent turn after the cancelled
            // continuation is withdrawn from the unrelated turn's boundary.
            let InboxRecord::Message(other) = continuations[1].clone() else {
                unreachable!()
            };
            continuations[1] = InboxRecord::FollowUp(other);
        }
        for input in &continuations {
            crate::agent::submit(&provider, crate::agent::send(&path, &user, input)?).await?;
        }
        let cancelled_input = if steered { 4 } else { 3 };
        let other_input = cancelled_input + 1;
        let cancel_sequence = other_input + 1;
        let cursor = cancel_sequence + 1;
        let entries = originals
            .into_iter()
            .enumerate()
            .map(|(index, input)| {
                ConversationEntry {
                    inbox: index as u64 + 1,
                    input: crate::agent::turn_input(index as u64 + 1, input)
                        .unwrap()
                        .input,
                    output: file.clone(),
                    outcome: TurnOutcome::Completed,
                    goal: Some(runs[index]),
                    observed_through: 0,
                    announcements: Vec::new(),
                    tool_results: None,
                    steering: None,
                    notices: None,
                }
                .encode()
                .map(Into::into)
            })
            .collect::<Result<Vec<acyclic_stream::AppendRecord>, _>>()?;
        provider
            .append(AppendRequest {
                path: path.conversation()?,
                if_tail: Some(0),
                records: entries,
                idempotency_key: crate::agent::intent_key("goal-entries", &[])?,
            })
            .await?;
        let mut state = AgentState::adopted(path.clone(), &adopted)?;
        state.conversation_tail = 2;
        if steered {
            state.turn = Some(Progress {
                inbox: 3,
                checkpoint: Vec::new(),
            });
            state.phase = if parked.is_some() {
                Phase::Waiting {
                    deadline_unix_ms: None,
                    input: true,
                }
            } else {
                Phase::Running
            };
            state.queued.push_back(
                crate::agent::turn_input(cancelled_input, continuations[0].clone()).unwrap(),
            );
        } else if let Some(host) = parked {
            state.turn = Some(Progress {
                inbox: cancelled_input,
                checkpoint: Vec::new(),
            });
            state.phase = if host {
                Phase::WaitingHost {
                    host: "host".into(),
                }
            } else {
                Phase::WaitingApproval {
                    id: "approval".try_into()?,
                    binding: None,
                }
            };
        } else {
            state.queued.push_back(
                crate::agent::turn_input(cancelled_input, continuations[0].clone()).unwrap(),
            );
        }
        state
            .queued
            .push_back(crate::agent::turn_input(other_input, continuations[1].clone()).unwrap());
        let cancel = InboxRecord::Cancel(Cancellation {
            by: user.clone(),
            id: "stop-goal".try_into()?,
            input: cancelled_input,
            conversation: if steered { u64::MAX } else { 2 },
        });
        if !racing {
            crate::agent::submit(&provider, crate::agent::send(&path, &user, &cancel)?).await?;
        }
        let through = if racing { cancel_sequence } else { cursor };
        let mut stopped = if steered {
            state.steering_tail = through;
            let selection = crate::agent::select_boundary(
                &provider,
                &path,
                3,
                4,
                crate::agent::BoundaryEnd::Exact(through),
            )
            .await?;
            assert_eq!(selection.inputs.len(), usize::from(racing));
            let mut outs = vec![Out::Steered(selection.through)];
            if parked.is_none() {
                outs.push(Out::Conversation(
                    ConversationEntry {
                        inbox: 3,
                        input: crate::agent::turn_input(3, active).unwrap().input,
                        output: file.clone(),
                        outcome: TurnOutcome::Completed,
                        goal: None,
                        observed_through: selection.through,
                        announcements: Vec::new(),
                        tool_results: None,
                        steering: Some(crate::agent::SteeringTranscript {
                            through: selection.through,
                            file: file.clone(),
                        }),
                        notices: None,
                    }
                    .encode()?,
                ));
            }
            crate::agent::step(
                &BoundaryRetirement(Mutex::new(Some(TurnStep {
                    checkpoint: parked.map(|_| Vec::new()),
                    wait: if parked.is_some() {
                        Wait::Until(crate::agent::Until::input(None))
                    } else {
                        Wait::Input
                    },
                    drops: None,
                    outs,
                }))),
                &path,
                Some(state.clone()),
                (cancel_sequence, 0),
                Wake::Alarm,
                CancellationToken::new(),
            )
            .await?
        } else {
            crate::agent::step(
                &Opening(Mutex::new(None)),
                &path,
                Some(state.clone()),
                (cursor, 0),
                Wake::Inbox {
                    sequence: cancel_sequence,
                    record: cancel.clone(),
                },
                CancellationToken::new(),
            )
            .await?
        };
        let conversation_tail = if steered && parked.is_none() { 3 } else { 2 };
        assert_eq!(stopped.state.turn.is_some(), steered && parked.is_some());
        assert_eq!(stopped.state.conversation_tail, conversation_tail);
        assert_eq!(stopped.state.dropped_before, 0);
        assert_eq!(stopped.state.queued.front().unwrap().inbox, other_input);
        let mut publication = crate::agent::lower(
            &stopped,
            if steered { cancel_sequence } else { cursor },
            crate::agent::intent_key("goal-stop", &[])?,
        )?;
        let retired =
            (steered && parked.is_some()).then_some(crate::agent::BoundaryCut { from: 4, through });
        if steered && parked.is_some() {
            assert!(publication.effects.iter().all(|effect| effect.soft));
        }
        let guarded = crate::agent::cancellation::complete(
            &provider,
            &path,
            &stopped.outs,
            retired,
            stopped.state.conversation_tail,
            &mut publication,
        )
        .await?;
        assert_eq!(guarded, steered);
        if racing {
            crate::agent::submit(&provider, crate::agent::send(&path, &user, &cancel)?).await?;
            let CommitOutcome::Conflict(conflicts) = provider.commit(publication).await? else {
                panic!("a cancellation after preparation must refuse stale retirement");
            };
            use acyclic_actors::guest::Composer as _;
            let composer = crate::agent::AgentComposer::wrapping(Arc::new(provider.clone()), None);
            let participant = crate::agent::cancellation::retirement_participant(None)?;
            let next = super::super::checkpoint::from_encoded(super::super::checkpoint::encode(
                &stopped.state,
            )?)?;
            let actor = acyclic_actors::addressing::ActorReference::new(
                b"account",
                b"agents",
                path.id().as_str().as_bytes(),
            )?;
            let mut guard = composer.open(&actor, None, None, &next, &participant)?;
            assert!(guard.revalidate(&conflicts));
            publication = crate::agent::lower(
                &stopped,
                cancel_sequence,
                crate::agent::intent_key("goal-stop", &[])?,
            )?;
            crate::agent::cancellation::complete(
                &provider,
                &path,
                &stopped.outs,
                retired,
                stopped.state.conversation_tail,
                &mut publication,
            )
            .await?;
            publication = guard.seal(publication).await?;
        }
        // Preparation alone does not settle the run: the marker and stopped
        // checkpoint must publish together. No synthetic final entry is added.
        assert_eq!(
            crate::agent::reply(
                &provider,
                &mut state,
                cursor,
                &"stopped-goal".try_into()?,
                1
            )
            .await?,
            Reply::Continuing { run: runs[0] }
        );
        crate::agent::submit(&provider, publication.clone()).await?;
        crate::agent::submit(&provider, publication).await?;
        assert_eq!(
            crate::agent::reply(
                &provider,
                &mut stopped.state,
                cursor,
                &"stopped-goal".try_into()?,
                1
            )
            .await?,
            Reply::Dropped
        );
        assert_eq!(
            crate::agent::reply(
                &provider,
                &mut stopped.state,
                cursor,
                &"other-goal".try_into()?,
                2
            )
            .await?,
            Reply::Continuing { run: runs[1] }
        );
        assert_eq!(
            provider.head(path.conversation()?).await?.tail,
            conversation_tail
        );
        if steered {
            let late = crate::agent::step(
                &BoundaryRetirement(Mutex::new(Some(TurnStep {
                    checkpoint: Some(Vec::new()),
                    wait: Wait::Host("host".into()),
                    drops: None,
                    outs: Vec::new(),
                }))),
                &path,
                Some(stopped.state),
                (cursor, 0),
                Wake::Inbox {
                    sequence: cancel_sequence,
                    record: cancel,
                },
                CancellationToken::new(),
            )
            .await?;
            // A parked turn still owns its admitted window. Its late control
            // can retire that input again; the original run already settled
            // atomically above, independently of this delivery.
            assert_eq!(
                late.outs
                    .iter()
                    .any(|out| matches!(out, Out::InputCancelled { .. })),
                parked.is_some()
            );
        }
    }
    Ok(())
}
