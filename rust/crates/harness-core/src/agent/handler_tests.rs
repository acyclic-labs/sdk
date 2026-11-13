use super::*;
use crate::agent::{Cancellation, Message, Queued, Sender, TurnFuture, TurnStep, Wait};
use acyclic_actors::guest::{CancellationToken, SubscriptionCuts, wire};
use acyclic_stream::{
    AppendRequest, CommitOutcome, MemoryStream, ReadRequest, StreamProvider as _,
    StreamProviderExt as _,
};
use futures::TryStreamExt as _;
use tokio::sync::oneshot;

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
    let provider = Arc::new(MemoryStream::default());
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
    let provider = Arc::new(MemoryStream::default());
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
    crate::agent::cancellation::complete(provider.as_ref(), &path, &outs, &mut completed).await?;
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
    for parked in [None, Some(false), Some(true)] {
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
        let continuations = [
            message(
                Sender::Agent { path: path.clone() },
                format!("goal:{}:1", runs[0]),
            )?,
            message(
                Sender::Agent { path: path.clone() },
                format!("goal:{}:1", runs[1]),
            )?,
        ];
        for input in originals.iter().chain(&continuations) {
            crate::agent::submit(&provider, crate::agent::send(&path, &user, input)?).await?;
        }
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
        if let Some(host) = parked {
            state.turn = Some(Progress {
                inbox: 3,
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
            state
                .queued
                .push_back(crate::agent::turn_input(3, continuations[0].clone()).unwrap());
        }
        state
            .queued
            .push_back(crate::agent::turn_input(4, continuations[1].clone()).unwrap());
        let cancel = InboxRecord::Cancel(Cancellation {
            by: user.clone(),
            id: "stop-goal".try_into()?,
            input: 3,
            conversation: 2,
        });
        crate::agent::submit(&provider, crate::agent::send(&path, &user, &cancel)?).await?;
        let mut stopped = crate::agent::step(
            &Opening(Mutex::new(None)),
            &path,
            Some(state.clone()),
            (6, 0),
            Wake::Inbox {
                sequence: 5,
                record: cancel,
            },
            CancellationToken::new(),
        )
        .await?;
        assert!(stopped.state.turn.is_none());
        assert_eq!(stopped.state.conversation_tail, 2);
        assert_eq!(stopped.state.dropped_before, 0);
        assert_eq!(stopped.state.queued.front().unwrap().inbox, 4);
        let mut publication =
            crate::agent::lower(&stopped, 6, crate::agent::intent_key("goal-stop", &[])?)?;
        crate::agent::cancellation::complete(&provider, &path, &stopped.outs, &mut publication)
            .await?;
        // Preparation alone does not settle the run: the marker and stopped
        // checkpoint must publish together. No synthetic final entry is added.
        assert_eq!(
            crate::agent::reply(&provider, &mut state, 6, &"stopped-goal".try_into()?, 1).await?,
            Reply::Continuing { run: runs[0] }
        );
        crate::agent::submit(&provider, publication.clone()).await?;
        crate::agent::submit(&provider, publication).await?;
        assert_eq!(
            crate::agent::reply(
                &provider,
                &mut stopped.state,
                6,
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
                6,
                &"other-goal".try_into()?,
                2
            )
            .await?,
            Reply::Continuing { run: runs[1] }
        );
        assert_eq!(provider.head(path.conversation()?).await?.tail, 2);
    }
    Ok(())
}
