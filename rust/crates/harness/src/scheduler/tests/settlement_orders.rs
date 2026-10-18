use super::*;

fn settlement(scheduler: &Scheduler) -> Option<SchedulerEvent> {
    let (outcome, cancel, reduction_digest) =
        match scheduler.orchestration(id(1), &mut None).unwrap() {
            OrchestrationDecision::Wait => return None,
            OrchestrationDecision::Complete { outcome, cancel } => (outcome, cancel, None),
            OrchestrationDecision::Assemble {
                assembly,
                values,
                cancel,
            } => {
                assert_eq!(
                    values,
                    vec![
                        ("a".into(), result_ref(b"11").unwrap()),
                        ("b".into(), result_ref(b"22").unwrap())
                    ]
                );
                (
                    Outcome::Succeeded(result_ref(b"[11,22]").unwrap()),
                    cancel,
                    Some(assembly_invocation_digest(assembly, &values).unwrap()),
                )
            }
            OrchestrationDecision::Reduce { .. } => panic!("no custom reducer registered"),
        };
    Some(SchedulerEvent::Orchestrated {
        operation_id: id(1),
        expected_revision: scheduler.operation(id(1)).unwrap().revision,
        outcome,
        cancel,
        reducer: None,
        reduction_digest,
    })
}

fn orders(lengths: [usize; 2]) -> Vec<Vec<usize>> {
    fn visit(left: [usize; 2], prefix: &mut Vec<usize>, output: &mut Vec<Vec<usize>>) {
        if left == [0, 0] {
            output.push(prefix.clone());
            return;
        }
        for actor in 0..2 {
            if left[actor] != 0 {
                let mut next = left;
                next[actor] -= 1;
                prefix.push(actor);
                visit(next, prefix, output);
                prefix.pop();
            }
        }
    }
    let mut output = Vec::new();
    visit(lengths, &mut Vec::new(), &mut output);
    output
}

fn publish(scheduler: &mut Scheduler, event: Option<SchedulerEvent>) {
    if let Some(event) = event {
        let before = scheduler.clone();
        if scheduler.apply(&event, &mut None).is_err() {
            assert_eq!(*scheduler, before);
        }
    }
}

#[test]
fn concurrent_children_settle_join_once_in_slot_order() {
    // Each actor completes, plans, then conditionally publishes. All 20
    // interleavings preserve its program order, as in the former mutex model.
    let schedules = orders([3, 3]);
    assert_eq!(schedules.len(), 20);
    for schedule in schedules {
        let mut scheduler = two_slot_parent(Orchestration::Join).unwrap();
        let mut next = [0; 2];
        let mut plans = [None, None];
        for actor in schedule {
            match next[actor] {
                0 => complete(
                    &mut scheduler,
                    id(actor as u8 + 2),
                    Outcome::Succeeded(result_ref(if actor == 0 { b"11" } else { b"22" }).unwrap()),
                    Some(fence(0)),
                )
                .unwrap(),
                1 => plans[actor] = settlement(&scheduler),
                2 => publish(&mut scheduler, plans[actor].take()),
                _ => unreachable!(),
            }
            next[actor] += 1;
        }
        assert_eq!(
            scheduler.operation(id(1)).unwrap().phase,
            OperationPhase::Terminal
        );
        assert_eq!(
            scheduler
                .completion_order
                .iter()
                .filter(|op| **op == id(1))
                .count(),
            1
        );
        assert_eq!(
            scheduler.operation(id(1)).unwrap().outcome,
            Some(Outcome::Succeeded(result_ref(b"[11,22]").unwrap()))
        );
        let before = scheduler.clone();
        let expected_revision = scheduler.operation(id(1)).unwrap().revision;
        assert!(
            scheduler
                .apply(
                    &SchedulerEvent::Orchestrated {
                        operation_id: id(1),
                        expected_revision,
                        outcome: Outcome::Cancelled,
                        cancel: Vec::new(),
                        reducer: None,
                        reduction_digest: None,
                    },
                    &mut None
                )
                .is_err()
        );
        assert_eq!(scheduler, before);
    }
}

#[test]
fn concurrent_success_and_cancellation_preserve_race_winner() {
    for schedule in orders([3, 3]) {
        let mut scheduler = two_slot_parent(Orchestration::Race).unwrap();
        let mut next = [0; 2];
        let mut plans = [None, None];
        for actor in schedule {
            match next[actor] {
                0 if actor == 0 => complete(
                    &mut scheduler,
                    id(2),
                    Outcome::Succeeded(result_ref(b"11").unwrap()),
                    Some(fence(0)),
                )
                .unwrap(),
                0 => {
                    scheduler
                        .apply(
                            &SchedulerEvent::CancellationRequested {
                                operation_id: id(3),
                                recursive: false,
                            },
                            &mut None,
                        )
                        .unwrap();
                    complete(&mut scheduler, id(3), Outcome::Cancelled, Some(fence(0))).unwrap();
                }
                1 => plans[actor] = settlement(&scheduler),
                2 => publish(&mut scheduler, plans[actor].take()),
                _ => unreachable!(),
            }
            next[actor] += 1;
        }
        assert_eq!(
            scheduler.operation(id(1)).unwrap().outcome,
            Some(Outcome::Succeeded(result_ref(b"11").unwrap()))
        );
        assert_eq!(
            scheduler
                .completion_order
                .iter()
                .filter(|op| **op == id(1))
                .count(),
            1
        );
        assert!(
            scheduler
                .operations
                .values()
                .all(|state| state.reservation.is_none())
        );
    }
}

fn leaf() -> Scheduler {
    let mut scheduler = Scheduler::new();
    scheduler
        .apply(
            &scheduler
                .declare(spec(id(1), Orchestration::Leaf).unwrap())
                .unwrap(),
            &mut None,
        )
        .unwrap();
    scheduler
}

#[test]
fn cancellation_and_indeterminate_completion_retain_settlement_lease() {
    for schedule in orders([1, 1]) {
        let mut scheduler = leaf();
        start(&mut scheduler, id(1), reservation(0, 1)).unwrap();
        for actor in schedule {
            if actor == 0 {
                complete(
                    &mut scheduler,
                    id(1),
                    Outcome::Indeterminate {
                        operation_id: id(1),
                        cause: crate::IndeterminateCause::Unobserved,
                    },
                    Some(fence(0)),
                )
                .unwrap();
            } else {
                scheduler
                    .apply(
                        &SchedulerEvent::CancellationRequested {
                            operation_id: id(1),
                            recursive: false,
                        },
                        &mut None,
                    )
                    .unwrap();
            }
        }
        let state = scheduler.operation(id(1)).unwrap();
        assert_eq!(state.phase, OperationPhase::Reconciling);
        assert_eq!(state.reservation, Some(reservation(0, 1)));
        assert!(require_execution_owner(state, &fence(0), false, &mut None).is_err());
        assert!(require_execution_owner(state, &fence(0), true, &mut None).is_ok());
        assert!(require_execution_owner(state, &fence(1), true, &mut None).is_err());
        complete(&mut scheduler, id(1), Outcome::Cancelled, Some(fence(0))).unwrap();
        assert!(scheduler.operation(id(1)).unwrap().reservation.is_none());
        assert_eq!(scheduler.completion_order, vec![id(1)]);
    }
}

#[test]
fn competing_workers_cannot_share_or_release_a_lease() {
    for schedule in orders([2, 2]) {
        let mut scheduler = leaf();
        let mut next = [0; 2];
        let mut admitted = [false; 2];
        for actor in schedule {
            let lease = actor as u8;
            if next[actor] == 0 {
                let before = scheduler.clone();
                admitted[actor] = scheduler
                    .apply(
                        &SchedulerEvent::Admitted {
                            operation_id: id(1),
                            reservation: reservation(lease, 0),
                        },
                        &mut None,
                    )
                    .is_ok();
                if !admitted[actor] {
                    assert_eq!(scheduler, before);
                }
            } else if admitted[actor] {
                scheduler
                    .apply(
                        &SchedulerEvent::Started {
                            operation_id: id(1),
                            fence: fence(lease),
                        },
                        &mut None,
                    )
                    .unwrap();
            }
            next[actor] += 1;
        }
        let winners: Vec<_> = admitted
            .into_iter()
            .enumerate()
            .filter_map(|(lease, admitted)| admitted.then_some(lease as u8))
            .collect();
        assert_eq!(winners.len(), 1);
        let winner = winners[0];
        let before = scheduler.clone();
        assert!(
            scheduler
                .apply(
                    &SchedulerEvent::LeaseReleased {
                        operation_id: id(1),
                        fence: fence(1 - winner),
                    },
                    &mut None
                )
                .is_err()
        );
        assert_eq!(scheduler, before);
        assert_eq!(
            scheduler.operation(id(1)).unwrap().reservation,
            Some(reservation(winner, 0))
        );
        complete(
            &mut scheduler,
            id(1),
            Outcome::Cancelled,
            Some(fence(winner)),
        )
        .unwrap();
        assert!(scheduler.operation(id(1)).unwrap().reservation.is_none());
    }
}
