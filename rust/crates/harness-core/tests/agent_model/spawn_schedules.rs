//! Deterministic actor schedules: the real step, paused tool effects, client
//! arrivals and replay after losing the step response. No second reducer.
use super::*;

#[tokio::test]
async fn an_invalid_wait_fails_before_publishing_status_or_effects() -> Result {
    let invalid = [String::new(), "x".repeat(MAX_INTENT_BYTES + 1), "\0".into()]
        .into_iter()
        .map(Wait::Host)
        .chain([Wait::Approval {
            id: intent("invalid-action"),
            binding: Some(acyclic_harness_core::interaction::ApprovalBinding {
                operation_id: acyclic_harness_core::OperationId::from_bytes([1; 16]),
                action_digest: [0; 32],
            }),
        }]);
    for wait in invalid {
        let (root, mut state) = populated_parent()?;
        state.turn = Some(Progress {
            inbox: 0,
            checkpoint: Vec::new(),
        });
        state.phase = Phase::Running;
        let driver = script(vec![TurnStep {
            checkpoint: Some(vec![]),
            outs: vec![Out::Spawn {
                profile: None,
                history: None,
                intent: intent("spawn:invalid-host"),
                brief: None,
                volumes: vec![],
            }],
            wait,
            drops: None,
        }]);
        let result = step(
            &driver,
            &root,
            Some(state),
            1,
            Wake::Alarm,
            acyclic_actors::guest::CancellationToken::new(),
        )
        .await?;
        assert!(
            matches!(
                result.state.phase,
                Phase::Done {
                    outcome: AgentOutcome::Failed { .. }
                }
            ),
            "invalid wait must settle as a bounded failure"
        );
        assert!(
            spawns(&result.outs).is_empty(),
            "invalid wait published unchecked effects"
        );
        StatusRecord {
            phase: result.state.phase,
            cursor: u64::MAX,
            conversation: u64::MAX,
        }
        .encode()?;
    }
    Ok(())
}

#[tokio::test]
async fn a_child_finishing_during_pause_cannot_strand_a_recorded_replay() -> Result {
    let (root, mut state) = populated_parent()?;
    let recorded = intent("spawn:already-open");
    let child = TurnEffect::child(&root, &recorded)?;
    state.open_children.insert(child.clone());
    state.turn = Some(Progress {
        inbox: 0,
        checkpoint: Vec::new(),
    });
    state.phase = Phase::WaitingApproval {
        id: intent("approval"),
        binding: None,
    };
    let driver = script(vec![TurnStep {
        checkpoint: None,
        outs: vec![Out::Spawn {
            profile: None,
            history: None,
            intent: recorded.clone(),
            brief: None,
            volumes: vec![],
        }],
        wait: Wait::Input,
        drops: None,
    }]);
    let done = || Wake::Inbox {
        sequence: 1,
        record: InboxRecord::ChildDone(ChildDone {
            child: child.clone(),
            outcome: AgentOutcome::Archived,
        }),
    };
    state = step(
        &driver,
        &root,
        Some(state),
        2,
        done(),
        acyclic_actors::guest::CancellationToken::new(),
    )
    .await?
    .state;
    assert!(!state.open_children.contains(&child));
    state = step(
        &driver,
        &root,
        Some(state),
        3,
        done(),
        acyclic_actors::guest::CancellationToken::new(),
    )
    .await?
    .state;
    assert!(!state.open_children.contains(&child));
    let client = step(
        &driver,
        &root,
        Some(state),
        4,
        spawn_request(3),
        acyclic_actors::guest::CancellationToken::new(),
    )
    .await?;
    assert_eq!(spawns(&client.outs).len(), 1);
    let resumed = step(
        &driver,
        &root,
        Some(client.state),
        5,
        Wake::Inbox {
            sequence: 4,
            record: InboxRecord::Approval(Approval {
                id: intent("approval"),
            }),
        },
        acyclic_actors::guest::CancellationToken::new(),
    )
    .await?;
    assert_eq!(spawns(&resumed.outs), [recorded]);
    assert_eq!(resumed.state.open_children.len(), 302);
    Ok(())
}

#[tokio::test]
async fn spawn_schedules_preserve_all_recorded_spawns() -> Result {
    for recorded_maximum in 1..=4 {
        for recorded in 1..=recorded_maximum {
            // Place resume at every cut of five sequential client arrivals.
            for resume_at in 0..=5 {
                let (root, mut state) = populated_parent()?;
                state.phase = Phase::Running;
                state.turn = Some(Progress {
                    inbox: 0,
                    checkpoint: Vec::new(),
                });
                let named: Vec<_> = (0..recorded)
                    .map(|n| intent(&format!("spawn:recorded-{n}")))
                    .collect();
                let driver = script(vec![
                    TurnStep {
                        checkpoint: Some(vec![1]),
                        outs: vec![],
                        wait: Wait::Approval {
                            id: intent("approval"),
                            binding: None,
                        },
                        drops: None,
                    },
                    TurnStep {
                        checkpoint: None,
                        outs: named
                            .iter()
                            .map(|intent| Out::Spawn {
                                profile: None,
                                history: None,
                                intent: intent.clone(),
                                brief: None,
                                volumes: Vec::new(),
                            })
                            .collect(),
                        wait: Wait::Input,
                        drops: None,
                    },
                ]);
                state = step(
                    &driver,
                    &root,
                    Some(state),
                    1,
                    Wake::Alarm,
                    acyclic_actors::guest::CancellationToken::new(),
                )
                .await?
                .state;
                for cut in 0..=5 {
                    if cut == resume_at {
                        let resumed = step(
                            &driver,
                            &root,
                            Some(state),
                            10 + cut,
                            Wake::Inbox {
                                sequence: 9 + cut,
                                record: InboxRecord::Approval(Approval {
                                    id: intent("approval"),
                                }),
                            },
                            acyclic_actors::guest::CancellationToken::new(),
                        )
                        .await?;
                        assert_eq!(
                            spawns(&resumed.outs),
                            named,
                            "maximum={recorded_maximum}, cut={cut}"
                        );
                        state = resumed.state;
                    }
                    if cut < 5 {
                        let before = state.clone();
                        let sequence = 20 + cut;
                        let first = step(
                            &driver,
                            &root,
                            Some(before.clone()),
                            sequence + 1,
                            spawn_request(sequence),
                            acyclic_actors::guest::CancellationToken::new(),
                        )
                        .await?;
                        // A client-spawn step never runs the driver. Losing its
                        // reply and re-executing from its old checkpoint agrees.
                        let replay = step(
                            &driver,
                            &root,
                            Some(before),
                            sequence + 1,
                            spawn_request(sequence),
                            acyclic_actors::guest::CancellationToken::new(),
                        )
                        .await?;
                        assert_eq!(spawns(&first.outs).len(), 1);
                        assert_eq!(first.state, replay.state);
                        assert_eq!(spawns(&first.outs), spawns(&replay.outs));
                        state = first.state;
                    }
                }
                assert_eq!(state.open_children.len(), 305 + recorded);
                for intent in &named {
                    assert!(
                        matches!(state.spawn_outcome(intent)?, SpawnOutcome::Spawned { .. }),
                        "recorded spawn stranded: {intent:?}; maximum={recorded_maximum}, cut={resume_at}"
                    );
                }
            }
        }
    }
    Ok(())
}
