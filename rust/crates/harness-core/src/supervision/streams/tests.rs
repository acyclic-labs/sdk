//! Real Streams commits prove query atomicity, indexed files and fork isolation.
use super::*;
use crate::agent::{AgentId, NOT_INHERITED};
use acyclic_stream::{CommitOutcome, CommitRequest, IdempotencyKey, MemoryStream, ReadRequest};
use futures::TryStreamExt as _;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
async fn original_turn_spending_is_atomic_and_does_not_borrow_later_turn_usage() -> TestResult {
    let provider = MemoryStream::default();
    let agent = AgentPath::root(&AgentId::from_bytes([95; 16]));
    let run = OperationId::from_bytes([96; 16]);
    let queries = Queries::new(&provider, &agent, run);
    let policy = Policy {
        no_progress_turns: Some(10),
        ..Default::default()
    };
    let (spent, quiet_turn_fence) = queries.spent_in_turn(0).await?;
    assert!(!spent);
    let (_, spending_turn_fence) = queries.spent_in_turn(1).await?;
    let publish = |sequence, projection: Projection| -> TestResult<CommitRequest> {
        Ok(CommitRequest {
            deadline_unix_millis: None,
            idempotency_key: IdempotencyKey::unique(),
            effects: vec![
                CommitEffect::hard(
                    vec![if sequence == 0 {
                        CommitCondition::Absent {
                            path: agent.inbox()?,
                        }
                    } else {
                        CommitCondition::Tail {
                            path: agent.inbox()?,
                            expected: sequence,
                        }
                    }],
                    vec![CommitMutation::Append {
                        path: agent.inbox()?,
                        records: vec![b"original usage".as_slice().into()],
                    }],
                ),
                projection.effect,
            ],
        })
    };
    for (sequence, turn, tokens, spent_tokens) in [
        (0, 0, Some(0), false),
        (1, 1, Some(2), true),
        (2, 0, Some(0), false),
        (3, 2, None, false),
        (4, 3, None, true),
        (5, 1, Some(0), false),
    ] {
        let source = Position {
            stream: agent.inbox()?,
            sequence,
        };
        let usage = TurnUsage {
            turn,
            tokens,
            usd_nanos: None,
            spent_tokens,
        };
        let projection = queries.project_usage(&source, &usage, &policy).await?;
        let request = publish(sequence, projection)?;
        if sequence == 1 {
            let mut refused = request.clone();
            refused.idempotency_key = IdempotencyKey::unique();
            refused.effects[0].conditions[0] = CommitCondition::Tail {
                path: agent.inbox()?,
                expected: sequence + 1,
            };
            assert!(matches!(
                provider.commit(refused).await?,
                CommitOutcome::Conflict(_)
            ));
            assert!(
                !queries.spent_in_turn(turn).await?.0,
                "refused usage published its turn marker"
            );
        }
        let receipt = provider.commit(request.clone()).await?;
        assert!(matches!(receipt, CommitOutcome::Committed(_)));
        assert_eq!(provider.commit(request).await?, receipt);
    }
    for (turn, expected) in [(0, false), (1, true), (2, false), (3, true), (4, false)] {
        assert_eq!(queries.spent_in_turn(turn).await?.0, expected);
    }
    let decision = |fence, name: &str| -> TestResult<CommitRequest> {
        Ok(CommitRequest {
            deadline_unix_millis: None,
            idempotency_key: IdempotencyKey::unique(),
            effects: vec![CommitEffect::hard(
                vec![fence],
                vec![CommitMutation::Append {
                    path: StreamPath::new(format!("decisions/{name}"))?,
                    records: vec![b"settled".as_slice().into()],
                }],
            )],
        })
    };
    // Later turns do not invalidate an earlier quiet turn's point decision.
    assert!(matches!(
        provider
            .commit(decision(quiet_turn_fence, "quiet")?)
            .await?,
        CommitOutcome::Committed(_)
    ));
    assert!(matches!(
        provider
            .commit(decision(spending_turn_fence, "stale")?)
            .await?,
        CommitOutcome::Conflict(_)
    ));
    assert!(matches!(
        provider.tail(StreamPath::new("decisions/stale")?).await,
        Err(StreamError::NotFound)
    ));
    let checkpoint = queries
        .checkpoint()
        .await?
        .ok_or("usage checkpoint missing")?;
    assert_eq!(checkpoint.state.tokens.measured, 2);
    assert!(
        !checkpoint.state.tokens.complete,
        "unreported totals became measured zeroes"
    );
    let mut policy = Policy {
        no_progress_turns: Some(2),
        ..policy
    };
    policy
        .actions
        .insert(super::super::Kind::NoProgress, super::super::Action::Stop);
    // Settle real original usage publications, rather than asking the kernel
    // to trust a caller-supplied spending boolean. Progress and a quiet turn
    // both break the consecutive spending-turn run.
    for (index, (spent_tokens, progress, expected)) in [
        (true, false, 1),
        (true, true, 0),
        (true, false, 1),
        (false, false, 0),
        (true, false, 1),
        (true, false, 2),
    ]
    .into_iter()
    .enumerate()
    {
        let sequence = 6 + index as u64 * 3;
        let turn = 100 + index as u64;
        let source = Position {
            stream: agent.inbox()?,
            sequence,
        };
        let usage = queries
            .project_usage(
                &source,
                &TurnUsage {
                    turn,
                    tokens: Some(u64::from(spent_tokens)),
                    usd_nanos: None,
                    spent_tokens,
                },
                &policy,
            )
            .await?;
        assert!(matches!(
            provider.commit(publish(sequence, usage)?).await?,
            CommitOutcome::Committed(_)
        ));
        let source = Position {
            sequence: sequence + 1,
            ..source
        };
        let progress = if progress {
            queries.project_progress(&source, turn, &policy).await?
        } else {
            queries
                .project(&source, &Observation::Progress, &policy)
                .await?
        };
        assert!(matches!(
            provider.commit(publish(sequence + 1, progress)?).await?,
            CommitOutcome::Committed(_)
        ));
        let source = Position {
            sequence: sequence + 2,
            ..source
        };
        let settlement = queries
            .project_settlement(&source, turn, &policy)
            .await?
            .ok_or("settlement query missing")?;
        assert_eq!(settlement.checkpoint.state.unproductive_turns, expected);
        assert_eq!(
            settlement.checkpoint.action,
            if expected == 2 {
                super::super::Action::Stop
            } else {
                super::super::Action::Alert
            }
        );
        let request = publish(sequence + 2, settlement)?;
        let receipt = provider.commit(request.clone()).await?;
        assert!(matches!(receipt, CommitOutcome::Committed(_)));
        assert_eq!(provider.commit(request).await?, receipt);
    }
    assert!(
        queries
            .project_settlement(
                &Position {
                    stream: agent.inbox()?,
                    sequence: 24,
                },
                106,
                &Policy::default(),
            )
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn lifecycle_actions_keep_the_signal_cut_across_a_concurrent_append() -> TestResult {
    use crate::supervision::Action;
    let provider = MemoryStream::default();
    let agent = AgentPath::root(&AgentId::from_bytes([93; 16]));
    let run = OperationId::from_bytes([94; 16]);
    let queries = Queries::new(&provider, &agent, run);
    let source = Position {
        stream: agent.inbox()?,
        sequence: 0,
    };
    provider
        .append(acyclic_stream::AppendRequest {
            path: source.stream.clone(),
            records: vec![b"original job".as_slice().into()],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    let mut policy = Policy {
        quiet_ms: Some(10),
        ..Default::default()
    };
    policy.actions.insert(Kind::Stuck, Action::Pause);
    let observation = Observation::ToolAlarm {
        call: [1; 32],
        deadline_ms: 10,
        last_activity_ms: 0,
        in_flight: true,
        now_ms: 10,
    };
    let event = Event {
        version: 1,
        agent: agent.clone(),
        run: Some(run),
        source: source.clone(),
        event: Lifecycle::Paused {
            signal: Some(Kind::Stuck),
        },
    };
    let mut stale = queries.project(&source, &observation, &policy).await?;
    // The owner publishes after the signal captured absence, before its
    // lifecycle append is composed. Composition must retain that first cut.
    provider
        .append(acyclic_stream::AppendRequest {
            path: path(&agent)?,
            records: vec![
                serde_json::to_vec(&Event {
                    run: None,
                    event: Lifecycle::Resumed {},
                    ..event.clone()
                })?
                .into(),
            ],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    stale.append_event(&event)?;
    let publish = |effect| CommitRequest {
        deadline_unix_millis: None,
        effects: vec![effect],
        idempotency_key: IdempotencyKey::unique(),
    };
    assert!(matches!(
        provider.commit(publish(stale.effect)).await?,
        CommitOutcome::Conflict(_)
    ));
    assert!(queries.checkpoint().await?.is_none());
    assert!(queries.read_signal(Kind::Stuck).await?.is_none());
    let mut current = queries.project(&source, &observation, &policy).await?;
    assert!(
        current
            .append_event(&Event {
                run: Some(OperationId::new()),
                ..event.clone()
            })
            .is_err(),
        "foreign run joined the captured projection"
    );
    current.append_event(&event)?;
    let request = publish(current.effect);
    let receipt = provider.commit(request.clone()).await?;
    assert_eq!(provider.commit(request).await?, receipt);
    let CommitOutcome::Committed(receipt) = receipt else {
        return Err("fresh alarm refused".into());
    };
    let mut records = provider
        .read(ReadRequest {
            path: path(&agent)?,
            key: None,
            from: 1,
            limit: 2,
            at_least: Default::default(),
        })
        .await?;
    let signal = records.try_next().await?.ok_or("signal missing")?;
    let action = records.try_next().await?.ok_or("action missing")?;
    assert_eq!(signal.commit_id, receipt.commit_id);
    assert_eq!(action.commit_id, receipt.commit_id);
    assert!(matches!(
        serde_json::from_slice::<Event>(&signal.value)?.event,
        Lifecycle::Signal { .. }
    ));
    assert_eq!(serde_json::from_slice::<Event>(&action.value)?, event);
    assert!(records.try_next().await?.is_none());
    Ok(())
}

#[tokio::test]
async fn owner_resume_fences_the_original_pause_and_preserves_reservations_and_terminal_stop()
-> TestResult {
    use crate::supervision::{Action, Reservation};
    for action in [Action::Pause, Action::Stop] {
        let provider = MemoryStream::default();
        let agent = AgentPath::root(&AgentId::from_bytes([91; 16]));
        let run = OperationId::from_bytes([92; 16]);
        let queries = Queries::new(&provider, &agent, run);
        let mut policy = Policy {
            reservation: Reservation {
                tokens: Some(1),
                usd_micros: None,
            },
            ..Default::default()
        };
        policy.actions.insert(Kind::Spend, action);
        let source = |name: &str| -> TestResult<Position> {
            Ok(Position {
                stream: StreamPath::new(format!("sources/{name}"))?,
                sequence: 0,
            })
        };
        let original = source("usage")?;
        let owner = source("resume")?;
        assert!(queries.resume(&owner).await?.is_none());
        let publish = |source: &Position, effect: CommitEffect| -> TestResult<CommitRequest> {
            Ok(CommitRequest {
                deadline_unix_millis: None,
                idempotency_key: IdempotencyKey::unique(),
                effects: vec![
                    CommitEffect::hard(
                        vec![CommitCondition::Absent {
                            path: source.stream.clone(),
                        }],
                        vec![CommitMutation::Append {
                            path: source.stream.clone(),
                            records: vec![b"original".to_vec().into()],
                        }],
                    ),
                    effect,
                ],
            })
        };
        let paused = queries
            .project(
                &original,
                &Observation::Usage {
                    tokens: Some(1),
                    usd_nanos: None,
                },
                &policy,
            )
            .await?;
        assert!(matches!(
            provider.commit(publish(&original, paused.effect)?).await?,
            CommitOutcome::Committed(_)
        ));
        let pending = queries.resume(&owner).await?;
        assert_eq!(
            queries
                .admission(&policy)
                .await?
                .control
                .ok_or("missing original control")?
                .action,
            action
        );
        if action == Action::Stop {
            assert!(pending.is_none(), "owner resume reopened a terminal stop");
            continue;
        }
        let pending = pending.ok_or("pause cannot be resumed")?;
        let later = source("intervening")?;
        let progress = queries
            .project(
                &later,
                &Observation::ModelOutput {
                    turn: 0,
                    output: None,
                },
                &policy,
            )
            .await?;
        assert!(matches!(
            provider.commit(publish(&later, progress.effect)?).await?,
            CommitOutcome::Committed(_)
        ));
        assert!(matches!(
            provider.commit(publish(&owner, pending)?).await?,
            CommitOutcome::Conflict(_)
        ));
        assert!(matches!(
            provider.tail(owner.stream.clone()).await,
            Err(StreamError::NotFound)
        ));
        assert_eq!(
            queries
                .admission(&policy)
                .await?
                .control
                .ok_or("stale resume erased pause")?
                .source,
            original
        );
        let request = publish(
            &owner,
            queries
                .resume(&owner)
                .await?
                .ok_or("fresh resume missing")?,
        )?;
        let receipt = provider.commit(request.clone()).await?;
        assert!(matches!(receipt, CommitOutcome::Committed(_)));
        assert_eq!(provider.commit(request).await?, receipt);
        let exhausted = queries
            .admission(&policy)
            .await?
            .control
            .ok_or("resume granted unreserved dispatch capacity")?;
        assert_eq!(exhausted.source, owner);
        assert_eq!(exhausted.action, Action::Pause);
        assert_eq!(exhausted.signal, Some(Kind::Spend));
        assert!(
            queries
                .admission(&Policy::default())
                .await?
                .control
                .is_none(),
            "unbounded capacity retained a cleared decision"
        );
        let mut alerting = policy.clone();
        alerting.actions.insert(Kind::Spend, Action::Alert);
        assert!(queries.admission(&alerting).await?.control.is_none());
        let resumed = queries
            .checkpoint()
            .await?
            .ok_or("resume lost its checkpoint")?;
        assert_eq!(resumed.source, owner);
        assert_eq!(resumed.state.tokens.measured, 1);
        assert_eq!(
            queries
                .read_signal(Kind::Spend)
                .await?
                .ok_or("resume erased original signal")?
                .count,
            1
        );
        // An explicit resume is not additional reserved capacity. The normal
        // next-dispatch check must still refuse an exhausted reservation.
        let next = source("next-dispatch")?;
        let exhausted = queries
            .project(&next, &Observation::PolicyChanged, &policy)
            .await?;
        assert_eq!(exhausted.checkpoint.action, Action::Pause);
        assert_eq!(exhausted.checkpoint.state.tokens.measured, 1);
    }
    Ok(())
}

#[tokio::test]
async fn output_occurrences_count_distinct_turns_and_commit_markers_atomically() -> TestResult {
    for across_turns in [false, true] {
        let provider = MemoryStream::default();
        let agent = AgentPath::root(&AgentId::from_bytes([12; 16]));
        let run = OperationId::from_bytes([13; 16]);
        let queries = Queries::new(&provider, &agent, run);
        let source = StreamPath::new(format!("{agent}/execution"))?;
        let policy = Policy::default();
        // Nonadjacent responses in one turn are healthy. Only another turn
        // repeating the first response can contribute its second occurrence.
        for (sequence, turn, digest) in [
            (0, 0, [1; 32]),
            (1, 0, [1; 32]),
            (2, 0, [2; 32]),
            (3, 0, [1; 32]),
            (4, 1, [if across_turns { 1 } else { 3 }; 32]),
        ] {
            let position = Position {
                stream: source.clone(),
                sequence,
            };
            let projection = queries
                .project(
                    &position,
                    &Observation::ModelOutput {
                        turn,
                        output: Some(digest),
                    },
                    &policy,
                )
                .await?;
            let request = CommitRequest {
                deadline_unix_millis: None,
                idempotency_key: IdempotencyKey::new(format!("output-{sequence}").into_bytes())?,
                effects: vec![
                    CommitEffect::hard(
                        vec![CommitCondition::Tail {
                            path: source.clone(),
                            expected: sequence,
                        }],
                        vec![CommitMutation::Append {
                            path: source.clone(),
                            records: vec![serde_json::to_vec(&(turn, digest))?.into()],
                        }],
                    ),
                    projection.effect,
                ],
            };
            let outcome = provider.commit(request.clone()).await?;
            assert!(matches!(outcome, CommitOutcome::Committed(_)));
            assert_eq!(
                provider.commit(request).await?,
                outcome,
                "source replay changed its original occurrence"
            );
            let signal = queries.read_signal(Kind::LoopOutput).await?;
            if sequence == 4 && across_turns {
                assert_eq!(signal.as_ref().map(|signal| signal.count), Some(2));
            } else {
                assert!(
                    signal.is_none(),
                    "one turn acquired multiple output occurrences"
                );
            }
        }
        let cut = queries
            .checkpoint()
            .await?
            .ok_or("query checkpoint missing")?;
        assert_eq!(cut.state.through, 5);
        assert_eq!(
            cut.action,
            if across_turns {
                super::super::Action::Pause
            } else {
                super::super::Action::Alert
            }
        );
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_sources_fence_one_durable_query_and_retain_its_pause() -> TestResult {
    let provider = MemoryStream::default();
    let agent = AgentPath::root(&AgentId::from_bytes([9; 16]));
    let run = OperationId::from_bytes([10; 16]);
    let queries = Queries::new(&provider, &agent, run);
    let policy = Policy {
        reservation: super::super::Reservation {
            tokens: Some(2),
            usd_micros: None,
        },
        ..Default::default()
    };
    let usage = Observation::Usage {
        tokens: Some(1),
        usd_nanos: None,
    };
    let first = Position {
        stream: StreamPath::new("sources/first")?,
        sequence: 0,
    };
    let second = Position {
        stream: StreamPath::new("sources/second")?,
        sequence: 0,
    };
    let publish =
        |source: &Position, projection: Projection, key: &str| -> TestResult<CommitRequest> {
            Ok(CommitRequest {
                deadline_unix_millis: None,
                idempotency_key: IdempotencyKey::new(key.as_bytes().to_vec())?,
                effects: vec![
                    CommitEffect::hard(
                        vec![CommitCondition::Absent {
                            path: source.stream.clone(),
                        }],
                        vec![CommitMutation::Append {
                            path: source.stream.clone(),
                            records: vec![b"original".as_slice().into()],
                        }],
                    ),
                    projection.effect,
                ],
            })
        };
    let one = queries.project(&first, &usage, &policy).await?;
    let stale = queries.project(&second, &usage, &policy).await?;
    let request = publish(&first, one, "first")?;
    let receipt = provider.commit(request.clone()).await?;
    assert!(matches!(receipt, CommitOutcome::Committed(_)));
    assert_eq!(provider.commit(request).await?, receipt);
    assert!(matches!(
        provider.commit(publish(&second, stale, "stale")?).await?,
        CommitOutcome::Conflict(_)
    ));
    assert!(matches!(
        provider.tail(second.stream.clone()).await,
        Err(StreamError::NotFound)
    ));
    let first_cut = queries.checkpoint().await?.ok_or("missing original cut")?;
    assert_eq!(first_cut.source, first);
    assert_eq!(first_cut.state.tokens.measured, 1);
    assert_eq!(first_cut.action, super::super::Action::Alert);
    assert!(queries.read_signal(Kind::Spend).await?.is_none());

    let admitted_before_capacity = queries.admission(&policy).await?;
    assert!(admitted_before_capacity.control.is_none());
    let two = queries.project(&second, &usage, &policy).await?;
    assert_eq!(two.checkpoint.action, super::super::Action::Pause);
    let receipt = provider.commit(publish(&second, two, "second")?).await?;
    let CommitOutcome::Committed(receipt) = receipt else {
        return Err("second source refused".into());
    };
    let signal = provider
        .latest(
            path(&agent)?,
            signal_key(run, Kind::Spend).into(),
            Default::default(),
        )
        .await?
        .ok_or("missing signal")?;
    assert_eq!(signal.commit_id, receipt.commit_id);
    let cut = queries.checkpoint().await?.ok_or("missing capacity cut")?;
    assert_eq!(cut.state.tokens.measured, 2);
    assert_eq!(cut.signal, Some(Kind::Spend));
    let dispatch = StreamPath::new("sources/dispatch")?;
    let refused = provider
        .commit(CommitRequest {
            deadline_unix_millis: None,
            idempotency_key: IdempotencyKey::new(b"stale-dispatch".to_vec())?,
            effects: vec![CommitEffect::hard(
                vec![admitted_before_capacity.condition],
                vec![CommitMutation::Append {
                    path: dispatch.clone(),
                    records: vec![b"provider intent".as_slice().into()],
                }],
            )],
        })
        .await?;
    assert!(matches!(refused, CommitOutcome::Conflict(_)));
    assert!(matches!(
        provider.tail(dispatch).await,
        Err(StreamError::NotFound)
    ));
    let blocked = queries
        .admission(&policy)
        .await?
        .control
        .ok_or("new dispatch missed capacity")?;
    assert_eq!(blocked.source, second);
    let third = Position {
        stream: StreamPath::new("sources/third")?,
        sequence: 0,
    };
    let unrelated = queries
        .project(
            &third,
            &Observation::ModelOutput {
                turn: 0,
                output: None,
            },
            &policy,
        )
        .await?;
    assert_eq!(unrelated.checkpoint.action, super::super::Action::Pause);
    assert_eq!(unrelated.checkpoint.signal, Some(Kind::Spend));
    assert_eq!(unrelated.checkpoint.decision_source, Some(second.clone()));
    assert!(matches!(
        provider
            .commit(publish(&third, unrelated, "third")?)
            .await?,
        CommitOutcome::Committed(_)
    ));
    assert_eq!(
        queries
            .checkpoint()
            .await?
            .ok_or("lost durable pause")?
            .action,
        super::super::Action::Pause
    );
    let control = queries
        .admission(&policy)
        .await?
        .control
        .ok_or("later output erased pause")?;
    assert_eq!(control.source, second);
    assert!(matches!(queries.project(&third, &Observation::ToolStarted {
        turn: 0, command: "new check", check: true,
    }, &policy).await, Err(Error::ExecutionControlled(control)) if control.source == second));
    Ok(())
}

fn source_commit(agent: &AgentPath, prepared: &mut Prepared) -> TestResult<CommitRequest> {
    let through = prepared.transition.state.through;
    let source = agent.inbox()?;
    let mut effects = vec![CommitEffect::hard(
        vec![condition(
            source.clone(),
            (through > 1).then_some(through - 1),
        )],
        vec![CommitMutation::Append {
            path: source,
            records: vec![serde_json::to_vec(&prepared.transition.state)?.into()],
        }],
    )];
    effects.extend(prepared.effect.take());
    Ok(CommitRequest {
        deadline_unix_millis: None,
        effects,
        idempotency_key: IdempotencyKey::new(format!("supervision-source-{through}").into_bytes())?,
    })
}

#[tokio::test]
async fn signal_file_and_stream_share_one_atomic_source_record_through_crash_replay() -> TestResult
{
    let provider = MemoryStream::default();
    let agent = AgentPath::root(&AgentId::from_bytes([1; 16]));
    let queries = Queries::new(&provider, &agent, OperationId::from_bytes([7; 16]));
    let policy = Policy::default();
    let observation = Observation::ToolFinished {
        command: "check",
        error: Some(b"failed"),
    };
    let mut state = State::default();
    assert!(queries.read_signal(Kind::RepeatFailure).await?.is_none());
    for sequence in 0..3 {
        let source = Position {
            stream: agent.inbox()?,
            sequence,
        };
        let mut prepared = queries
            .prepare(&state, sequence, &source, &observation, &policy)
            .await?;
        let candidate = prepared.transition.state.clone();
        let request = source_commit(&agent, &mut prepared)?;
        // Force a stale source fence. Neither the signature count nor the
        // signal may commit independently of the source checkpoint.
        let mut rejected = request.clone();
        rejected.effects[0].conditions = vec![CommitCondition::Tail {
            path: agent.inbox()?,
            expected: 99,
        }];
        assert!(matches!(
            provider.commit(rejected).await?,
            CommitOutcome::Conflict(_)
        ));
        let rebuilt = queries
            .prepare(&state, sequence, &source, &observation, &policy)
            .await?;
        assert_eq!(rebuilt.transition.cell, prepared.transition.cell);
        let accepted = provider.commit(request.clone()).await?;
        assert!(matches!(accepted, CommitOutcome::Committed(_)));
        assert_eq!(
            provider.commit(request).await?,
            accepted,
            "exact retry retains its original receipt"
        );
        state = candidate;
    }
    let file = queries
        .read_signal(Kind::RepeatFailure)
        .await?
        .ok_or("signal absent")?;
    // Sparse and independent original streams do not share the fold ordinal.
    let original = Position {
        stream: StreamPath::new("other/original")?,
        sequence: 91,
    };
    let sparse = queries
        .prepare(&state, state.through, &original, &observation, &policy)
        .await?;
    assert!(!sparse.transition.signals.is_empty());
    assert!(
        sparse
            .transition
            .signals
            .iter()
            .all(|signal| signal.sequence == 91)
    );
    assert_eq!((file.sequence, file.count), (2, 3));
    assert_eq!(policy.action(&file), super::super::Action::Pause);
    let mut events = provider
        .read(ReadRequest {
            key: None,
            path: path(&agent)?,
            from: 0,
            limit: 10,
            at_least: Default::default(),
        })
        .await?;
    let mut signals = Vec::new();
    while let Some(record) = events.try_next().await? {
        let event: Event = serde_json::from_slice(&record.value)?;
        assert_eq!(event.agent, agent);
        assert_eq!(event.version, 1);
        assert_eq!(event.run, Some(OperationId::from_bytes([7; 16])));
        let Lifecycle::Signal { signal, action } = event.event else {
            return Err("not a signal event".into());
        };
        assert_eq!(
            event.source,
            Position {
                stream: agent.inbox()?,
                sequence: signal.sequence
            }
        );
        assert_eq!(action, policy.action(&signal));
        signals.push(signal);
    }
    assert_eq!(signals.len(), 2);
    assert_eq!(
        signals.last(),
        Some(&file),
        "file projection is the original stream signal"
    );
    let source = Position {
        stream: agent.inbox()?,
        sequence: 1,
    };
    let replay = queries
        .prepare(&state, 1, &source, &observation, &policy)
        .await?;
    assert!(replay.effect.is_none() && replay.transition.signals.is_empty());
    let independent = Queries::new(&provider, &agent, OperationId::from_bytes([8; 16]));
    let own = independent
        .prepare(&State::default(), 0, &source, &observation, &policy)
        .await?;
    assert_eq!(own.transition.cell.ok_or("new run cell absent")?.count, 1);
    assert!(
        independent
            .read_signal(Kind::RepeatFailure)
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn ordinary_fork_does_not_inherit_the_parents_supervision_or_spending() -> TestResult {
    let provider = MemoryStream::default();
    let parent = AgentPath::root(&AgentId::from_bytes([3; 16]));
    let child = parent.child(&AgentId::from_bytes([4; 16]))?;
    let parent_queries = Queries::new(&provider, &parent, OperationId::from_bytes([7; 16]));
    let child_queries = Queries::new(&provider, &child, OperationId::from_bytes([7; 16]));
    let policy = Policy::default();
    let observation = Observation::ToolFinished {
        command: "check",
        error: Some(b"failed"),
    };
    let mut prepared = parent_queries
        .prepare(
            &State::default(),
            0,
            &Position {
                stream: parent.inbox()?,
                sequence: 0,
            },
            &observation,
            &policy,
        )
        .await?;
    assert!(matches!(
        provider
            .commit(source_commit(&parent, &mut prepared)?)
            .await?,
        CommitOutcome::Committed(_)
    ));
    let exclude = NOT_INHERITED
        .iter()
        .map(|name| StreamPath::new(*name))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert!(matches!(
        provider
            .commit(CommitRequest {
                deadline_unix_millis: None,
                idempotency_key: IdempotencyKey::new(b"supervision-fork".to_vec())?,
                effects: vec![CommitEffect::hard(
                    vec![CommitCondition::Absent {
                        path: child.inbox()?
                    }],
                    vec![CommitMutation::Fork {
                        source: parent.inbox()?,
                        destination: child.inbox()?,
                        at_tail: 1,
                        exclude,
                        records: Vec::new()
                    }],
                )],
            })
            .await?,
        CommitOutcome::Committed(_)
    ));
    let own = child_queries
        .prepare(
            &State::default(),
            0,
            &Position {
                stream: child.inbox()?,
                sequence: 0,
            },
            &observation,
            &policy,
        )
        .await?;
    assert_eq!(own.transition.cell.ok_or("cell absent")?.count, 1);
    assert!(own.transition.signals.is_empty());
    assert!(
        child_queries
            .read_signal(Kind::RepeatFailure)
            .await?
            .is_none()
    );
    Ok(())
}
