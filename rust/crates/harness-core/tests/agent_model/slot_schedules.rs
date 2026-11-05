//! Deterministic actor schedules: the real step, paused tool effects, client
//! arrivals and replay after losing the step response. No second reducer.
use super::*;

#[tokio::test]
async fn an_invalid_host_wait_fails_before_publishing_status_or_effects() -> Result {
    for host in [String::new(), "\0".into()] {
        let (root, mut state) = populated_parent(0).await?;
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
            wait: Wait::Host(host),
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
    let (root, mut state) = populated_parent(2).await?;
    let recorded = intent("spawn:already-open");
    let child = TurnEffect::child(&root, &recorded)?;
    state.open_children.insert(child.clone()).await?;
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
        (2, 0),
        done(),
        acyclic_actors::guest::CancellationToken::new(),
    )
    .await?
    .state;
    assert!(!state.open_children.contains(&child).await?);
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
    assert_eq!(state.open_children.len(), 2);
    let client = step(
        &driver,
        &root,
        Some(state),
        (4, 0),
        spawn_request(3),
        acyclic_actors::guest::CancellationToken::new(),
    )
    .await?;
    assert_eq!(spawns(&client.outs), [requested_child(&root, 3)?.0]);
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
    assert!(resumed.state.open_children.contains(&child).await?);
    assert_eq!(resumed.state.open_children.len(), 4);
    Ok(())
}

#[tokio::test]
async fn admission_schedules_preserve_every_spawn_and_replay() -> Result {
    for population in [0, 2] {
        for recorded in 1..=3 {
            // Place resume at every cut of five sequential client arrivals.
            for resume_at in 0..=5 {
                let (root, mut state) = populated_parent(population).await?;
                let mut expected: BTreeSet<_> = state
                    .open_children
                    .page(None, population)
                    .await?
                    .into_iter()
                    .collect();
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
                    (1, 0),
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
                        assert_eq!(
                            spawns(&resumed.outs),
                            named,
                            "population={population}, cut={cut}"
                        );
                        for intent in &named {
                            expected.insert(TurnEffect::child(&root, intent)?.id());
                        }
                        state = resumed.state;
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
                        let (intent, child) = requested_child(&root, sequence)?;
                        assert_eq!(spawns(&first.outs), [intent]);
                        assert!(expected.insert(child.id()));
                        state = first.state;
                    }
                    assert_eq!(state.open_children.len(), expected.len());
                    assert_eq!(
                        state.open_children.page(None, expected.len()).await?,
                        expected.iter().cloned().collect::<Vec<_>>()
                    );
                }
                for intent in &named {
                    assert!(
                        matches!(
                            state.spawn_outcome(intent).await?,
                            SpawnOutcome::Spawned { .. }
                        ),
                        "recorded spawn stranded: {intent:?}; population={population}, cut={resume_at}"
                    );
                }
            }
        }
    }
    Ok(())
}
