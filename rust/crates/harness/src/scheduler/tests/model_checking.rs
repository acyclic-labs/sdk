use super::*;
use loom::sync::{Arc, Mutex};
use loom::thread;

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

fn settle(scheduler: &Mutex<Scheduler>) {
    // Decision and conditional publication are distinct transactions.
    let event = settlement(&scheduler.lock().unwrap());
    if let Some(event) = event {
        let mut scheduler = scheduler.lock().unwrap();
        let before = scheduler.clone();
        if scheduler.apply(&event, &mut None).is_err() {
            assert_eq!(*scheduler, before);
        }
    }
}

#[test]
fn concurrent_children_settle_join_once_in_slot_order() {
    loom::model(|| {
        let scheduler = Arc::new(Mutex::new(two_slot_parent(Orchestration::Join).unwrap()));
        let children: Vec<_> = [2, 3]
            .into_iter()
            .map(|op| {
                let scheduler = scheduler.clone();
                thread::spawn(move || {
                    complete(
                        &mut scheduler.lock().unwrap(),
                        id(op),
                        Outcome::Succeeded(
                            result_ref(if op == 2 { b"11" } else { b"22" }).unwrap(),
                        ),
                        Some(fence(0)),
                    )
                    .unwrap();
                    // Both completions may plan settlement at the same parent revision.
                    settle(&scheduler);
                })
            })
            .collect();
        for child in children {
            child.join().unwrap();
        }
        let scheduler = scheduler.lock().unwrap();
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
        let before = scheduler.clone();
        assert_eq!(
            scheduler.operation(id(1)).unwrap().outcome,
            Some(Outcome::Succeeded(result_ref(b"[11,22]").unwrap()))
        );
        let mut scheduler = scheduler;
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
        assert_eq!(*scheduler, before);
    });
}

#[test]
fn concurrent_success_and_cancellation_preserve_race_winner() {
    loom::model(|| {
        let scheduler = Arc::new(Mutex::new(two_slot_parent(Orchestration::Race).unwrap()));
        let success = {
            let scheduler = scheduler.clone();
            thread::spawn(move || {
                complete(
                    &mut scheduler.lock().unwrap(),
                    id(2),
                    Outcome::Succeeded(result_ref(b"11").unwrap()),
                    Some(fence(0)),
                )
                .unwrap();
                settle(&scheduler);
            })
        };
        let cancel = {
            let scheduler = scheduler.clone();
            thread::spawn(move || {
                let mut state = scheduler.lock().unwrap();
                state
                    .apply(
                        &SchedulerEvent::CancellationRequested {
                            operation_id: id(3),
                            recursive: false,
                        },
                        &mut None,
                    )
                    .unwrap();
                complete(&mut state, id(3), Outcome::Cancelled, Some(fence(0))).unwrap();
                drop(state);
                settle(&scheduler);
            })
        };
        success.join().unwrap();
        cancel.join().unwrap();
        let scheduler = scheduler.lock().unwrap();
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
    });
}

#[test]
fn cancellation_and_indeterminate_completion_retain_settlement_lease() {
    loom::model(|| {
        let mut initial = Scheduler::new();
        initial
            .apply(
                &initial
                    .declare(spec(id(1), Orchestration::Leaf).unwrap())
                    .unwrap(),
                &mut None,
            )
            .unwrap();
        start(&mut initial, id(1), reservation(0, 1)).unwrap();
        let scheduler = Arc::new(Mutex::new(initial));
        let completion = {
            let scheduler = scheduler.clone();
            thread::spawn(move || {
                complete(
                    &mut scheduler.lock().unwrap(),
                    id(1),
                    Outcome::Indeterminate {
                        operation_id: id(1),
                    },
                    Some(fence(0)),
                )
                .unwrap();
            })
        };
        let cancellation = {
            let scheduler = scheduler.clone();
            thread::spawn(move || {
                scheduler
                    .lock()
                    .unwrap()
                    .apply(
                        &SchedulerEvent::CancellationRequested {
                            operation_id: id(1),
                            recursive: false,
                        },
                        &mut None,
                    )
                    .unwrap();
            })
        };
        completion.join().unwrap();
        cancellation.join().unwrap();
        let mut scheduler = scheduler.lock().unwrap();
        let state = scheduler.operation(id(1)).unwrap();
        assert_eq!(state.phase, OperationPhase::Reconciling);
        assert_eq!(state.reservation, Some(reservation(0, 1)));
        assert!(require_execution_owner(state, &fence(0), false, &mut None).is_err());
        assert!(require_execution_owner(state, &fence(0), true, &mut None).is_ok());
        assert!(require_execution_owner(state, &fence(1), true, &mut None).is_err());
        complete(&mut scheduler, id(1), Outcome::Cancelled, Some(fence(0))).unwrap();
        assert!(scheduler.operation(id(1)).unwrap().reservation.is_none());
        assert_eq!(scheduler.completion_order, vec![id(1)]);
    });
}

#[test]
fn competing_workers_cannot_share_or_release_a_lease() {
    loom::model(|| {
        let mut initial = Scheduler::new();
        initial
            .apply(
                &initial
                    .declare(spec(id(1), Orchestration::Leaf).unwrap())
                    .unwrap(),
                &mut None,
            )
            .unwrap();
        let scheduler = Arc::new(Mutex::new(initial));
        let workers: Vec<_> = [0, 1]
            .into_iter()
            .map(|lease| {
                let scheduler = scheduler.clone();
                thread::spawn(move || {
                    let admitted = {
                        let mut state = scheduler.lock().unwrap();
                        let before = state.clone();
                        let admitted = state
                            .apply(
                                &SchedulerEvent::Admitted {
                                    operation_id: id(1),
                                    reservation: reservation(lease, 0),
                                },
                                &mut None,
                            )
                            .is_ok();
                        if !admitted {
                            assert_eq!(*state, before);
                        }
                        admitted
                    };
                    if admitted {
                        scheduler
                            .lock()
                            .unwrap()
                            .apply(
                                &SchedulerEvent::Started {
                                    operation_id: id(1),
                                    fence: fence(lease),
                                },
                                &mut None,
                            )
                            .unwrap();
                    }
                    (lease, admitted)
                })
            })
            .collect();
        let winners: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .filter_map(|(lease, admitted)| admitted.then_some(lease))
            .collect();
        assert_eq!(winners.len(), 1);
        let winner = winners[0];
        let mut scheduler = scheduler.lock().unwrap();
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
        assert_eq!(*scheduler, before);
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
    });
}
