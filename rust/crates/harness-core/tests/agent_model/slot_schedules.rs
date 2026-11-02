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
        let (root, mut state) = one_below_the_child_limit()?;
        state.turn = Some(Progress {
            inbox: 0,
            checkpoint: Vec::new(),
        });
        state.phase = Phase::Running;
        let driver = script(vec![TurnStep {
            checkpoint: Some(vec![]),
            reserved_children: 0,
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
            (1, 0),
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
    let (root, mut state) = one_below_the_child_limit()?;
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
    state.reserved_children = 0; // Recorded spawn initially names an open child.
    let driver = script(vec![TurnStep {
        checkpoint: None,
        reserved_children: 0,
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
        (2, 0),
        done(),
        acyclic_actors::guest::CancellationToken::new(),
    )
    .await?
    .state;
    assert_eq!(
        state.reserved_children, 1,
        "vacated slot still belongs to the paused turn"
    );
    state = step(
        &driver,
        &root,
        Some(state),
        (3, 0),
        done(),
        acyclic_actors::guest::CancellationToken::new(),
    )
    .await?
    .state;
    assert_eq!(
        state.reserved_children, 1,
        "duplicate completion reserves no second slot"
    );
    let client = step(
        &driver,
        &root,
        Some(state),
        (4, 0),
        spawn_request(3),
        acyclic_actors::guest::CancellationToken::new(),
    )
    .await?;
    assert!(
        spawns(&client.outs).is_empty(),
        "client took a recorded replay's vacated slot"
    );
    let resumed = step(
        &driver,
        &root,
        Some(client.state),
        (5, 0),
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
    assert_eq!(resumed.state.reserved_children, 0);
    assert_eq!(resumed.state.open_children.len(), MAX_OPEN_CHILDREN);
    Ok(())
}

#[tokio::test]
async fn slot_schedules_preserve_all_recorded_spawns() -> Result {
    for room in 1..=4 {
        for recorded in 1..=room {
            // Place resume at every cut of five sequential client arrivals.
            for resume_at in 0..=5 {
                let (root, mut state) = one_below_the_child_limit()?;
                while state.open_children.len() > MAX_OPEN_CHILDREN - room {
                    state.open_children.pop_last();
                }
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
                        reserved_children: recorded,
                        outs: vec![],
                        wait: Wait::Approval {
                            id: intent("approval"),
                            binding: None,
                        },
                        drops: None,
                    },
                    TurnStep {
                        checkpoint: None,
                        reserved_children: 0,
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
                    (1, 0),
                    Wake::Alarm,
                    acyclic_actors::guest::CancellationToken::new(),
                )
                .await?
                .state;
                assert_eq!(usize::from(state.reserved_children), recorded);
                for cut in 0..=5 {
                    if cut == resume_at {
                        let resumed = step(
                            &driver,
                            &root,
                            Some(state),
                            (10 + cut, 0),
                            Wake::Inbox {
                                sequence: 9 + cut,
                                record: InboxRecord::Approval(Approval {
                                    id: intent("approval"),
                                }),
                            },
                            acyclic_actors::guest::CancellationToken::new(),
                        )
                        .await?;
                        assert_eq!(spawns(&resumed.outs), named, "room={room}, cut={cut}");
                        state = resumed.state;
                        assert_eq!(state.reserved_children, 0);
                    }
                    if cut < 5 {
                        let before = state.clone();
                        let sequence = 20 + cut;
                        let first = step(
                            &driver,
                            &root,
                            Some(before.clone()),
                            (sequence + 1, 0),
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
                            (sequence + 1, 0),
                            spawn_request(sequence),
                            acyclic_actors::guest::CancellationToken::new(),
                        )
                        .await?;
                        assert_eq!(first.state, replay.state);
                        assert_eq!(spawns(&first.outs), spawns(&replay.outs));
                        state = first.state;
                    }
                    assert!(state.open_children.len() <= MAX_OPEN_CHILDREN);
                    assert!(
                        state.open_children.len() + usize::from(state.reserved_children)
                            <= MAX_OPEN_CHILDREN
                    );
                }
                for intent in &named {
                    assert!(
                        matches!(state.spawn_outcome(intent), SpawnOutcome::Spawned { .. }),
                        "recorded spawn stranded: {intent:?}; room={room}, cut={resume_at}"
                    );
                }
            }
        }
    }
    Ok(())
}
