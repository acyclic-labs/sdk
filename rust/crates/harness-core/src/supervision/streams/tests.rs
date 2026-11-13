//! Real Streams commits prove query atomicity, indexed files and fork isolation.
use super::*;
use crate::agent::{AgentId, NOT_INHERITED};
use acyclic_stream::{CommitOutcome, CommitRequest, IdempotencyKey, MemoryStream, ReadRequest};
use futures::TryStreamExt as _;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
async fn signal_files_follow_a_continuing_runs_pause_at_the_captured_cut() -> TestResult {
    use crate::client_files::FileView as _;
    let provider = MemoryStream::default();
    let agent = AgentPath::root(&AgentId::from_bytes([87; 16]));
    let a = OperationId::from_bytes([88; 16]);
    let b = OperationId::from_bytes([89; 16]);
    let events = path(&agent)?;
    let policy = Policy {
        thresholds: super::super::Thresholds {
            repeat_failure: Some(1),
            repeat_failure_pause: Some(1),
            ..Default::default()
        },
        ..Default::default()
    };
    let observation = Observation::ToolFinished {
        command: "check",
        error: Some(b"failure"),
    };
    let mut state = State::default();
    let mut b_cut = 0;
    for (sequence, run) in [(0, a), (1, b), (2, a)] {
        let source = Position {
            stream: agent.inbox()?,
            sequence,
        };
        let queries = Queries::new(&provider, &agent, run);
        let previous = if run == a { &state } else { &State::default() };
        let mut prepared = queries
            .prepare(previous, previous.through, &source, &observation, &policy)
            .await?;
        let mut records = Vec::new();
        if sequence < 2 {
            let mut started = lifecycle_record(&Event {
                version: 1,
                agent: agent.clone(),
                run: Some(run),
                source: source.clone(),
                event: Lifecycle::Started { input: sequence },
            })?;
            started.headers = acyclic_stream::RecordHeaders::new(
                vec![
                    Bytes::from_static(CURRENT_RUN_KEY).into(),
                    Bytes::from_static(super::super::RUN_KEY).into(),
                ],
                Vec::new(),
            )?;
            records.push(started);
        }
        // The continuation deliberately has no new Started record.
        if sequence == 2 {
            records.push(lifecycle_record(&Event {
                version: 1,
                agent: agent.clone(),
                run: Some(a),
                source: source.clone(),
                event: Lifecycle::Paused {
                    signal: Some(Kind::RepeatFailure),
                },
            })?);
        }
        let mut effect = prepared.effect.take().ok_or("signal effect absent")?;
        let appended = effect
            .mutations
            .iter_mut()
            .find_map(|mutation| match mutation {
                CommitMutation::Append { path, records } if path == &events => Some(records),
                _ => None,
            })
            .ok_or("signal records absent")?;
        // Start precedes its signal; pause follows it in the same commit.
        if sequence < 2 {
            records.append(appended);
            *appended = records;
        } else {
            appended.extend(records);
        }
        let request = CommitRequest {
            deadline_unix_millis: None,
            idempotency_key: IdempotencyKey::unique(),
            effects: vec![
                CommitEffect::hard(
                    vec![condition(
                        agent.inbox()?,
                        (sequence > 0).then_some(sequence),
                    )],
                    vec![CommitMutation::Append {
                        path: agent.inbox()?,
                        records: vec![b"actual source".as_slice().into()],
                    }],
                ),
                effect,
            ],
        };
        if sequence == 2 {
            let mut refused = request.clone();
            refused.idempotency_key = IdempotencyKey::unique();
            refused.effects[0].conditions = vec![CommitCondition::Tail {
                path: agent.inbox()?,
                expected: 99,
            }];
            assert!(matches!(
                provider.commit(refused).await?,
                CommitOutcome::Conflict(_)
            ));
            assert_eq!(
                current_run_before(&provider, &agent, u64::MAX, Default::default()).await?,
                Some(b)
            );
        }
        assert!(matches!(
            provider.commit(request).await?,
            CommitOutcome::Committed(_)
        ));
        if run == a {
            state = prepared.transition.state;
        }
        if sequence == 1 {
            b_cut = provider.tail(events.clone()).await?;
        }
    }
    assert_eq!(
        active_run_before(&provider, &agent, u64::MAX, Default::default()).await?,
        Some(b),
        "negative control: latest start still belongs to B"
    );
    let files = super::super::files::SignalFiles;
    let paths = ["status/supervision/repeat_failure.json"];
    for (cut, expected_sequence) in [(b_cut, 1), (u64::MAX, 2)] {
        let values = files
            .read(&provider, &agent, &paths, &events, cut, Default::default())
            .await?;
        let signal: Signal =
            serde_json::from_str(&values.first().ok_or("pause reason disappeared")?.text)?;
        assert_eq!(signal.sequence, expected_sequence);
        assert_eq!(values[0].input.stream, events);
        #[cfg(not(target_arch = "wasm32"))]
        {
            let cancellation = acyclic_fs::CancellationToken::new();
            let original = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::performance::work_budget(1_000_000, 1_000_000),
                &cancellation,
            )
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let baseline = original.live_allocation_bytes();
            let projected = native_signal_file(
                &provider,
                &agent,
                SignalFileRead {
                    source: events.clone(),
                    before: cut,
                    at_least: Default::default(),
                    kind: Kind::RepeatFailure,
                },
                &original,
            )
            .await?
            .ok_or("native pause reason disappeared")?;
            assert_eq!(
                serde_json::from_slice::<Signal>(&projected.bytes)?,
                signal,
                "native signal borrowed a later run or signal cut"
            );
            assert_eq!(projected.sequence, values[0].input.sequence);
            assert!(
                original.live_allocation_bytes() > baseline,
                "native signal output lost its original allocation owner"
            );
            drop(projected);
            assert_eq!(original.live_allocation_bytes(), baseline);
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let budget = acyclic_fs::performance::work_budget(1_000_000, 1_000_000);
        // Absence is distinct from successful measured data, decoder denial
        // and cancellation. None may silently use the portable entrance.
        for (kind, cut, decode, cancelled, absent) in [
            (Kind::RepeatFailure, 0, true, false, true),
            (Kind::Stuck, b_cut, true, false, true),
            (Kind::RepeatFailure, b_cut, false, false, false),
            (Kind::RepeatFailure, 0, true, true, false),
        ] {
            let cancellation = acyclic_fs::CancellationToken::new();
            let original = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::WorkBudget {
                    bytes_decoded: if decode { budget.bytes_decoded } else { 0 },
                    ..budget
                },
                &cancellation,
            )
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let baseline = original.live_allocation_bytes();
            if cancelled {
                cancellation.cancel();
            }
            let result = native_signal_file(
                &provider,
                &agent,
                SignalFileRead {
                    source: events.clone(),
                    before: cut,
                    at_least: Default::default(),
                    kind,
                },
                &original,
            )
            .await;
            if absent {
                assert!(result?.is_none(), "absent signal became a status file");
            } else {
                assert!(
                    result.is_err(),
                    "native signal query bypassed its original denial"
                );
                drop(result);
            }
            assert_eq!(original.live_allocation_bytes(), baseline);
        }
        let cancellation = acyclic_fs::CancellationToken::new();
        let original = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = original.live_allocation_bytes();
        let foreign = AgentPath::root(&AgentId::from_bytes([90; 16]));
        assert!(
            native_signal_file(
                &provider,
                &foreign,
                SignalFileRead {
                    source: events.clone(),
                    before: b_cut,
                    at_least: Default::default(),
                    kind: Kind::RepeatFailure,
                },
                &original,
            )
            .await
            .is_err(),
            "native signal query accepted another agent's source"
        );
        // An authentic indexed record with a mismatched signal payload is
        // rejected by the same validator on portable and native entrances.
        let invalid = Event {
            version: 1,
            agent: agent.clone(),
            run: Some(a),
            source: Position {
                stream: agent.inbox()?,
                sequence: 2,
            },
            event: Lifecycle::Signal {
                signal: Signal {
                    version: 1,
                    sequence: 2,
                    kind: Kind::ValidationLoop,
                    count: 1,
                    evidence: super::super::Evidence::Signature { key: [0; 32] },
                },
                action: super::super::Action::Alert,
            },
        };
        provider
            .append(acyclic_stream::AppendRequest {
                path: events.clone(),
                records: vec![AppendRecord {
                    value: serde_json::to_vec(&invalid)?.into(),
                    headers: acyclic_stream::RecordHeaders::new(
                        vec![
                            Bytes::from_static(CURRENT_RUN_KEY).into(),
                            signal_key(a, Kind::RepeatFailure).into(),
                        ],
                        Vec::new(),
                    )?,
                }],
                if_tail: None,
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?;
        assert!(
            Queries::new(&provider, &agent, a)
                .read_signal(Kind::RepeatFailure)
                .await
                .is_err()
        );
        assert!(
            native_signal_file(
                &provider,
                &agent,
                SignalFileRead {
                    source: events,
                    before: u64::MAX,
                    at_least: Default::default(),
                    kind: Kind::RepeatFailure,
                },
                &original,
            )
            .await
            .is_err(),
            "native signal accepted an indexed payload for another kind"
        );
        assert_eq!(original.live_allocation_bytes(), baseline);
    }
    Ok(())
}

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
        (true, Some(false), 1),
        (true, Some(true), 0),
        (true, Some(false), 1),
        // Missing original content coverage breaks an absence proof without
        // claiming positive progress. The next known unchanged turn counts.
        (true, None, 0),
        (true, Some(false), 1),
        (false, Some(false), 0),
        (true, Some(false), 1),
        (true, Some(false), 2),
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
        let measurement = queries
            .project_native_capture(
                &source,
                turn,
                OperationId::from_bytes([index as u8; 16]),
                NativeMeasurement::Complete(progress),
                &policy,
            )
            .await?;
        assert!(matches!(
            provider.commit(publish(sequence + 1, measurement)?).await?,
            CommitOutcome::Committed(_)
        ));
        assert_eq!(
            queries.progress_in_turn(turn).await?.0,
            progress == Some(true),
            "unavailable capture evidence became positive progress"
        );
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
                    sequence: 30,
                },
                108,
                &Policy::default(),
            )
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn native_pending_measurements_settle_per_call_without_bypassing_no_progress() -> TestResult {
    use crate::supervision::Action;
    use NativeMeasurement::{Complete, Pending};
    let a = OperationId::from_bytes([191; 16]);
    let b = OperationId::from_bytes([192; 16]);
    for (phases, expected) in [
        (vec![(a, Pending), (a, Complete(Some(false)))], 1),
        // One completion cannot clear another call's pending comparison.
        (
            vec![(a, Pending), (b, Pending), (a, Complete(Some(false)))],
            0,
        ),
        (
            vec![
                (a, Pending),
                (b, Pending),
                (a, Complete(Some(false))),
                (b, Complete(Some(false))),
            ],
            1,
        ),
        // Exact replay and a delayed start cannot resurrect pending credit.
        (
            vec![
                (a, Pending),
                (a, Pending),
                (a, Complete(Some(false))),
                (a, Complete(Some(false))),
                (a, Pending),
            ],
            1,
        ),
        (vec![(a, Complete(Some(false))), (a, Pending)], 1),
        (vec![(a, Pending), (a, Complete(None))], 0),
        (
            vec![(a, Pending), (b, Pending), (a, Complete(Some(true)))],
            0,
        ),
    ] {
        let provider = MemoryStream::default();
        let agent = AgentPath::root(&AgentId::from_bytes([193; 16]));
        let run = OperationId::from_bytes([194; 16]);
        let queries = Queries::new(&provider, &agent, run);
        let mut policy = Policy {
            no_progress_turns: Some(1),
            ..Default::default()
        };
        policy.actions.insert(Kind::NoProgress, Action::Stop);
        let publish = |sequence, projection: Projection| -> TestResult<CommitRequest> {
            native_source_request(
                &Position {
                    stream: agent.inbox()?,
                    sequence,
                },
                projection,
            )
        };
        let source = |sequence| -> TestResult<Position> {
            Ok(Position {
                stream: agent.inbox()?,
                sequence,
            })
        };
        let usage = queries
            .project_usage(
                &source(0)?,
                &TurnUsage {
                    turn: 0,
                    tokens: Some(1),
                    usd_nanos: None,
                    spent_tokens: true,
                },
                &policy,
            )
            .await?;
        assert!(matches!(
            provider.commit(publish(0, usage)?).await?,
            CommitOutcome::Committed(_)
        ));
        let mut sequence = 1;
        for (call, measurement) in phases {
            let projection = queries
                .project_native_capture(&source(sequence)?, 0, call, measurement, &policy)
                .await?;
            let request = publish(sequence, projection)?;
            let receipt = provider.commit(request.clone()).await?;
            assert!(matches!(receipt, CommitOutcome::Committed(_)));
            assert_eq!(provider.commit(request).await?, receipt);
            sequence += 1;
        }
        let settlement = queries
            .project_settlement(&source(sequence)?, 0, &policy)
            .await?
            .ok_or("native settlement missing")?;
        assert_eq!(
            settlement.checkpoint.state.unproductive_turns, expected,
            "completed native measurements did not settle their own pending calls"
        );
        assert_eq!(
            settlement.checkpoint.action,
            if expected == 1 {
                Action::Stop
            } else {
                Action::Alert
            }
        );
        assert!(matches!(
            provider.commit(publish(sequence, settlement)?).await?,
            CommitOutcome::Committed(_)
        ));
    }
    Ok(())
}

fn native_source_request(source: &Position, projection: Projection) -> TestResult<CommitRequest> {
    Ok(CommitRequest {
        deadline_unix_millis: None,
        idempotency_key: IdempotencyKey::unique(),
        effects: vec![
            CommitEffect::hard(
                vec![if source.sequence == 0 {
                    CommitCondition::Absent {
                        path: source.stream.clone(),
                    }
                } else {
                    CommitCondition::Tail {
                        path: source.stream.clone(),
                        expected: source.sequence,
                    }
                }],
                vec![CommitMutation::Append {
                    path: source.stream.clone(),
                    records: vec![b"original native producer".as_slice().into()],
                }],
            ),
            projection.effect,
        ],
    })
}

#[tokio::test]
async fn late_native_completions_keep_original_turns_and_latched_controls() -> TestResult {
    use crate::supervision::Action;
    use NativeMeasurement::{Complete, Pending};
    async fn publish(
        provider: &MemoryStream,
        source: &Position,
        projection: Projection,
    ) -> TestResult {
        assert!(matches!(
            provider
                .commit(native_source_request(source, projection)?)
                .await?,
            CommitOutcome::Committed(_)
        ));
        Ok(())
    }
    for (complete_before_next_settlement, changed) in
        [(false, false), (false, true), (true, false), (true, true)]
    {
        let provider = MemoryStream::default();
        let agent = AgentPath::root(&AgentId::from_bytes([195; 16]));
        let run = OperationId::from_bytes([196; 16]);
        let call = OperationId::from_bytes([197; 16]);
        let queries = Queries::new(&provider, &agent, run);
        let mut policy = Policy {
            no_progress_turns: Some(1),
            ..Default::default()
        };
        policy.actions.insert(Kind::NoProgress, Action::Stop);
        let source = |sequence| -> TestResult<Position> {
            Ok(Position {
                stream: agent.inbox()?,
                sequence,
            })
        };
        let usage = |turn| TurnUsage {
            turn,
            tokens: Some(1),
            usd_nanos: None,
            spent_tokens: true,
        };
        // Turn zero returns logically while its original physical measurement
        // is still pending. That unknown cannot prove absence or make progress.
        let spending = queries
            .project_usage(&source(0)?, &usage(0), &policy)
            .await?;
        publish(&provider, &source(0)?, spending).await?;
        let pending = queries
            .project_native_capture(&source(1)?, 0, call, Pending, &policy)
            .await?;
        publish(&provider, &source(1)?, pending).await?;
        let logical = queries
            .project_settlement(&source(2)?, 0, &policy)
            .await?
            .ok_or("logical settlement absent")?;
        assert_eq!(logical.checkpoint.state.unproductive_turns, 0);
        assert_eq!(logical.checkpoint.action, Action::Alert);
        publish(&provider, &source(2)?, logical).await?;
        let spending = queries
            .project_usage(&source(3)?, &usage(1), &policy)
            .await?;
        publish(&provider, &source(3)?, spending).await?;
        let mut sequence = 4;
        if complete_before_next_settlement {
            let completed = queries
                .project_native_capture(
                    &source(sequence)?,
                    0,
                    call,
                    Complete(Some(changed)),
                    &policy,
                )
                .await?;
            assert_eq!(
                completed.checkpoint.state.unproductive_turns, 0,
                "late native completion retroactively counted its logical turn"
            );
            publish(&provider, &source(sequence)?, completed).await?;
            sequence += 1;
        }
        // This later turn has reported spending and no pending producer of its
        // own. Another turn's pending count must not buy it another paid turn.
        let settled = queries
            .project_settlement(&source(sequence)?, 1, &policy)
            .await?
            .ok_or("later settlement absent")?;
        assert_eq!(
            settled.checkpoint.state.unproductive_turns, 1,
            "another turn's pending measurement bypassed no-progress settlement"
        );
        assert_eq!(settled.checkpoint.action, Action::Stop);
        let decision_source = settled.checkpoint.decision_source.clone();
        publish(&provider, &source(sequence)?, settled).await?;
        sequence += 1;
        if !complete_before_next_settlement {
            let completed = queries
                .project_native_capture(
                    &source(sequence)?,
                    0,
                    call,
                    Complete(Some(changed)),
                    &policy,
                )
                .await?;
            assert_eq!(
                completed.checkpoint.state.unproductive_turns, 1,
                "late native completion changed another turn's absence proof"
            );
            assert_eq!(
                completed.checkpoint.action,
                Action::Stop,
                "late native completion cleared a latched stop"
            );
            assert_eq!(completed.checkpoint.decision_source, decision_source);
            let request = native_source_request(&source(sequence)?, completed)?;
            let receipt = provider.commit(request.clone()).await?;
            assert!(matches!(receipt, CommitOutcome::Committed(_)));
            assert_eq!(provider.commit(request).await?, receipt);
            // A delayed start after physical completion cannot resurrect the
            // call's pending credit or rewrite the stop's original cause.
            sequence += 1;
            let delayed = queries
                .project_native_capture(&source(sequence)?, 0, call, Pending, &policy)
                .await?;
            assert_eq!(delayed.checkpoint.action, Action::Stop);
            assert_eq!(delayed.checkpoint.decision_source, decision_source);
            publish(&provider, &source(sequence)?, delayed).await?;
        }
        let admission = queries.admission(&policy).await?;
        assert_eq!(
            admission.control.as_ref().map(|control| control.action),
            Some(Action::Stop)
        );
        assert_eq!(
            admission.control.as_ref().map(|control| &control.source),
            decision_source.as_ref()
        );
        assert_eq!(queries.progress_in_turn(0).await?.0, changed);
        assert!(
            !queries.progress_in_turn(1).await?.0,
            "late native progress was rebound to the later turn"
        );
    }
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
