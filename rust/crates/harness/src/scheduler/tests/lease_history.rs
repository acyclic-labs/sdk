use super::*;
use proptest::prelude::*;

#[derive(Clone, Copy, Debug)]
enum Input {
    Admit(u8),
    Start(u8),
    Release(u8),
    Uncertain(u8),
    Resume(u8),
    Cancel,
    Finish(u8, u8),
    Retry(u8),
}

#[derive(Clone, Copy, PartialEq)]
enum Fault {
    None,
    IgnorePlacement,
    IgnoreReservation,
    DropUncertainLease,
    DuplicateTerminalRetry,
}

fn held(lease: u8) -> Reservation {
    let mut held = reservation(lease / 2, 0);
    held.placement = format!("worker-{}", lease % 2);
    held
}

fn lease_fence(lease: u8) -> LeaseFence {
    LeaseFence::from(&held(lease))
}

fn outcome(kind: u8) -> Outcome<FileRef> {
    match kind {
        0 => Outcome::Succeeded(result_ref(b"0").unwrap()),
        1 => Outcome::Failed {
            message: "failed".into(),
        },
        2 => Outcome::Cancelled,
        _ => Outcome::Indeterminate {
            operation_id: id(1),
        },
    }
}

fn event(input: Input) -> SchedulerEvent {
    let operation_id = id(1);
    match input {
        Input::Admit(lease) => SchedulerEvent::Admitted {
            operation_id,
            reservation: held(lease),
        },
        Input::Start(lease) => SchedulerEvent::Started {
            operation_id,
            fence: lease_fence(lease),
        },
        Input::Release(lease) => SchedulerEvent::LeaseReleased {
            operation_id,
            fence: lease_fence(lease),
        },
        Input::Resume(lease) => SchedulerEvent::ReconciliationResumed {
            operation_id,
            fence: lease_fence(lease),
        },
        Input::Cancel => SchedulerEvent::CancellationRequested {
            operation_id,
            recursive: false,
        },
        Input::Uncertain(lease) | Input::Finish(lease, _) => SchedulerEvent::Completed {
            operation_id,
            outcome: outcome(match input {
                Input::Finish(_, kind) => kind,
                _ => 3,
            }),
            fence: Some(lease_fence(lease)),
            execution_duration_ns: None,
        },
        Input::Retry(kind) => SchedulerEvent::Completed {
            operation_id,
            outcome: outcome(kind),
            fence: None,
            execution_duration_ns: None,
        },
    }
}

// Input-only reference state. It never calls Scheduler or reads its projection.
struct Expected {
    phase: OperationPhase,
    lease: Option<u8>,
    cancelled: bool,
    outcome: Option<Outcome<FileRef>>,
    revision: u64,
}

impl Expected {
    fn record(&mut self, input: Input) {
        match input {
            Input::Retry(_) => return,
            Input::Admit(lease) => {
                self.lease = Some(lease);
                self.phase = OperationPhase::Admitted;
            }
            Input::Start(_) => self.phase = OperationPhase::Running,
            Input::Release(_) => {
                self.lease = None;
                self.phase = if self.cancelled {
                    OperationPhase::Terminal
                } else {
                    OperationPhase::WaitingForCapacity
                };
                if self.cancelled {
                    self.outcome = Some(Outcome::Cancelled);
                }
            }
            Input::Uncertain(_) => {
                self.phase = OperationPhase::Reconciling;
                self.outcome = Some(outcome(3));
            }
            Input::Resume(_) => {
                self.phase = OperationPhase::Running;
                self.outcome = None;
            }
            Input::Cancel => {
                if self.phase == OperationPhase::Terminal {
                    return;
                }
                if self.phase == OperationPhase::Admitted || self.lease.is_none() {
                    self.phase = OperationPhase::Terminal;
                    self.lease = None;
                    self.outcome = Some(Outcome::Cancelled);
                } else {
                    self.cancelled = true;
                }
            }
            Input::Finish(_, kind) => {
                self.phase = OperationPhase::Terminal;
                self.lease = None;
                self.outcome = Some(outcome(kind));
            }
        }
        self.revision += 1;
    }
}

fn assert_expected(scheduler: &Scheduler, expected: &Expected) {
    let completed = if expected.phase == OperationPhase::Terminal {
        vec![id(1)]
    } else {
        vec![]
    };
    assert_eq!(
        scheduler.completion_order, completed,
        "terminal retry duplicated completion history"
    );
    let actual = scheduler.operation(id(1)).unwrap();
    assert_eq!(
        actual.reservation,
        expected.lease.map(held),
        "lease differs from independent history"
    );
    assert_eq!(actual.phase, expected.phase);
    assert_eq!(actual.cancellation_requested, expected.cancelled);
    assert_eq!(actual.outcome, expected.outcome);
    assert_eq!(
        actual.revision, expected.revision,
        "revision differs from independent history"
    );
    let current = expected.lease.unwrap_or(0);
    for candidate in [current, current ^ 1, current + 2] {
        for settlement in [false, true] {
            let owner = expected.lease == Some(candidate)
                && (expected.phase == OperationPhase::Running
                    || (settlement && expected.phase == OperationPhase::Reconciling))
                && (settlement || !expected.cancelled);
            assert_eq!(
                require_execution_owner(actual, &lease_fence(candidate), settlement, &mut None)
                    .is_ok(),
                owner,
                "execution ownership differs from independent history"
            );
        }
    }
}

fn accept(scheduler: &mut Scheduler, expected: &mut Expected, input: Input, fault: Fault) {
    expected.record(input);
    scheduler.apply(&event(input), &mut None).unwrap();
    if fault == Fault::DropUncertainLease && matches!(input, Input::Uncertain(_)) {
        scheduler.operations.get_mut(&id(1)).unwrap().reservation = None;
    }
    if fault == Fault::DuplicateTerminalRetry && matches!(input, Input::Retry(_)) {
        scheduler.completion_order.push(id(1));
    }
    assert_expected(scheduler, expected);
}

fn reject(scheduler: &mut Scheduler, expected: &Expected, input: Input, fault: Fault) {
    assert_expected(scheduler, expected);
    let before = scheduler.clone();
    if let Input::Release(lease) = input
        && let Some(current) = scheduler
            .operations
            .get_mut(&id(1))
            .unwrap()
            .reservation
            .as_mut()
    {
        match fault {
            Fault::IgnorePlacement => current.placement = held(lease).placement,
            Fault::IgnoreReservation => current.id = held(lease).id,
            _ => {}
        }
    }
    assert!(
        scheduler.apply(&event(input), &mut None).is_err(),
        "stale or invalid lease event was accepted: {input:?}"
    );
    assert_eq!(*scheduler, before, "rejected event changed the scheduler");
    assert_expected(scheduler, expected);
}

fn trace(
    reassignments: u8,
    uncertainty: u8,
    base: u8,
    run: bool,
    cancel: u8,
    finish: u8,
    fault: Fault,
) {
    let mut scheduler = Scheduler::new();
    scheduler
        .apply(
            &SchedulerEvent::Declared {
                spec: Box::new(spec(id(1), Orchestration::Leaf).unwrap()),
            },
            &mut None,
        )
        .unwrap();
    let mut expected = Expected {
        phase: OperationPhase::WaitingForDependencies,
        lease: None,
        cancelled: false,
        outcome: None,
        revision: 1,
    };
    assert_expected(&scheduler, &expected);
    let lease = base + reassignments;
    for current in base..=lease {
        accept(&mut scheduler, &mut expected, Input::Admit(current), fault);
        reject(&mut scheduler, &expected, Input::Admit(current + 1), fault);
        if current == lease && cancel == 3 {
            accept(&mut scheduler, &mut expected, Input::Cancel, fault);
            break;
        }
        if run || current == lease {
            accept(&mut scheduler, &mut expected, Input::Start(current), fault);
        }
        for old in base..current {
            reject(&mut scheduler, &expected, Input::Release(old), fault);
            reject(&mut scheduler, &expected, Input::Start(old), fault);
            reject(&mut scheduler, &expected, Input::Finish(old, 1), fault);
        }
        if current != lease {
            accept(
                &mut scheduler,
                &mut expected,
                Input::Release(current),
                fault,
            );
        }
    }
    if cancel != 3 {
        if cancel == 1 {
            accept(&mut scheduler, &mut expected, Input::Cancel, fault);
        }
        for round in 0..uncertainty {
            accept(
                &mut scheduler,
                &mut expected,
                Input::Uncertain(lease),
                fault,
            );
            reject(&mut scheduler, &expected, Input::Release(lease), fault);
            if cancel == 2 && round + 1 == uncertainty {
                accept(&mut scheduler, &mut expected, Input::Cancel, fault);
            }
            if !expected.cancelled {
                accept(&mut scheduler, &mut expected, Input::Resume(lease), fault);
            } else {
                reject(&mut scheduler, &expected, Input::Resume(lease), fault);
            }
        }
        if cancel == 2 && uncertainty == 0 {
            accept(&mut scheduler, &mut expected, Input::Cancel, fault);
        }
        reject(&mut scheduler, &expected, Input::Retry(finish), fault);
        accept(
            &mut scheduler,
            &mut expected,
            Input::Finish(lease, finish),
            fault,
        );
    }
    let finish = if cancel == 3 { 2 } else { finish };
    accept(&mut scheduler, &mut expected, Input::Retry(finish), fault);
    accept(&mut scheduler, &mut expected, Input::Cancel, fault);
    for input in [
        Input::Admit(lease + 1),
        Input::Start(lease),
        Input::Release(lease),
        Input::Finish(lease, finish),
    ] {
        reject(&mut scheduler, &expected, input, fault);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn lease_histories_match_an_independent_model(
        reassignments in 0..6_u8, uncertainty in 0..6_u8, base in 0..16_u8, run in any::<bool>(),
    ) {
        for cancel in 0..4 {
            for finish in 0..3 {
                trace(reassignments, uncertainty, base * 2, run, cancel, finish, Fault::None);
            }
        }
    }
}

#[test]
#[should_panic(expected = "stale or invalid lease event was accepted")]
fn negative_control_ignores_placement() {
    trace(1, 0, 0, true, 0, 1, Fault::IgnorePlacement);
}

#[test]
#[should_panic(expected = "stale or invalid lease event was accepted")]
fn negative_control_ignores_reservation_identity() {
    trace(2, 0, 0, true, 0, 1, Fault::IgnoreReservation);
}

#[test]
#[should_panic(expected = "lease differs from independent history")]
fn negative_control_releases_an_uncertain_lease() {
    trace(0, 1, 0, true, 0, 1, Fault::DropUncertainLease);
}

#[test]
#[should_panic(expected = "terminal retry duplicated completion history")]
fn negative_control_records_a_terminal_retry_twice() {
    trace(0, 0, 0, true, 0, 1, Fault::DuplicateTerminalRetry);
}
