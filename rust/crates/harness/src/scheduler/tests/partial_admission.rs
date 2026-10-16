use super::*;
use proptest::prelude::*;

#[derive(Clone, Copy, Debug, PartialEq)]
struct Allocation {
    lease: u8,
    amounts: [u8; 2],
    full: bool,
}

#[derive(Clone, Copy, PartialEq)]
enum Fault {
    None,
    Rewind,
    ForeignPlacement,
    ForeignReservation,
    Excess,
    Underfunded,
    IgnoreHeld,
    DuplicateHeld,
    ForeignFullPlacement,
    ForeignFullReservation,
    RejectExactFull,
}

fn resources(amounts: [u8; 2]) -> ResourceRequest {
    ResourceRequest(BTreeMap::from([
        ("cpu".into(), u64::from(amounts[0])),
        ("memory".into(), u64::from(amounts[1])),
    ]))
}

fn held(lease: u8, amounts: [u8; 2]) -> Reservation {
    Reservation {
        id: format!("partial-{}", lease / 2),
        placement: format!("worker-{}", lease % 2),
        admitted: resources(amounts),
    }
}

fn event(input: Allocation) -> SchedulerEvent {
    let reservation = held(input.lease, input.amounts);
    if input.full {
        SchedulerEvent::Admitted {
            operation_id: id(1),
            reservation,
        }
    } else {
        SchedulerEvent::PartiallyAdmitted {
            operation_id: id(1),
            reservation,
        }
    }
}

struct Expected {
    request: [u8; 2],
    held: Option<Allocation>,
    phase: OperationPhase,
    cancelled: bool,
    revision: u64,
    sibling: u8,
}

impl Expected {
    // Input-only arithmetic; no production readiness, coverage or lease helper.
    fn record(&mut self, input: Allocation) -> Option<&'static str> {
        if self.cancelled
            || !matches!(
                self.phase,
                OperationPhase::WaitingForDependencies | OperationPhase::WaitingForCapacity
            )
        {
            return Some("closed admission accepted an allocation");
        }
        if input.full {
            if (0..2).any(|n| input.amounts[n] < self.request[n]) {
                return Some("underfunded full reservation was accepted");
            }
        } else {
            if (0..2).any(|n| input.amounts[n] > self.request[n]) {
                return Some("excess partial reservation was accepted");
            }
            if input.amounts == self.request {
                return Some("complete reservation was accepted as partial");
            }
        }
        if let Some(prior) = self.held {
            if prior.lease != input.lease {
                return Some("foreign partial lease was accepted");
            }
            if !input.full && (0..2).any(|n| input.amounts[n] < prior.amounts[n]) {
                return Some("partial reservation rewound retained capacity");
            }
        }
        self.held = Some(input);
        self.phase = if input.full {
            OperationPhase::Admitted
        } else {
            OperationPhase::WaitingForCapacity
        };
        self.revision += 1;
        None
    }
}

fn assert_cpu_capacity(actual: u64, expected: u64) {
    assert_eq!(
        actual, expected,
        "available capacity differs from independent ledger"
    );
}

fn assert_expected(scheduler: &Scheduler, expected: &Expected, fault: Fault) {
    for placement in 0..3_u8 {
        for advertised in [
            [0, 0],
            expected.request,
            [expected.request[0] + 3, expected.request[1] + 3],
        ] {
            let mut available = advertised.map(u64::from);
            if placement == expected.sibling {
                available = [
                    available[0].saturating_sub(1),
                    available[1].saturating_sub(2),
                ];
            }
            if let Some(allocation) = expected.held
                && placement == allocation.lease % 2
            {
                for (value, amount) in available.iter_mut().zip(allocation.amounts) {
                    *value = value.saturating_sub(u64::from(amount));
                }
            }
            let actual = scheduler
                .available_for(
                    &format!("worker-{placement}"),
                    &resources(advertised),
                    &mut None,
                )
                .unwrap();
            assert_cpu_capacity(actual.0["cpu"], available[0]);
            assert_eq!(
                actual.0["memory"], available[1],
                "available capacity differs from independent ledger"
            );
        }
    }
    let mut query = scheduler.clone();
    if fault == Fault::IgnoreHeld
        && let Some(held) = query
            .operations
            .get_mut(&id(1))
            .unwrap()
            .reservation
            .as_mut()
    {
        held.admitted = resources([0, 0]);
    }
    let allocated = expected.held.map_or([0, 0], |held| held.amounts);
    let remaining = [
        expected.request[0].saturating_sub(allocated[0]),
        expected.request[1].saturating_sub(allocated[1]),
    ];
    for cpu in [
        remaining[0].saturating_sub(1),
        remaining[0],
        expected.request[0] + 1,
    ] {
        for memory in [
            remaining[1].saturating_sub(1),
            remaining[1],
            expected.request[1] + 1,
        ] {
            let ready = !expected.cancelled
                && matches!(
                    expected.phase,
                    OperationPhase::WaitingForDependencies | OperationPhase::WaitingForCapacity
                )
                && cpu >= remaining[0]
                && memory >= remaining[1];
            assert_eq!(
                query
                    .ready(&resources([cpu, memory]), &BTreeMap::new(), None)
                    .any(|op| op == id(1)),
                ready,
                "readiness ignored independently retained capacity"
            );
        }
    }
    let actual = scheduler.operation(id(1)).unwrap();
    assert_eq!(actual.spec.resources, resources(expected.request));
    assert_eq!(
        actual.reservation,
        expected
            .held
            .map(|held| self::held(held.lease, held.amounts))
    );
    assert_eq!(actual.phase, expected.phase);
    assert_eq!(actual.cancellation_requested, expected.cancelled);
    assert_eq!(actual.revision, expected.revision);
    assert_eq!(
        actual.outcome,
        (expected.phase == OperationPhase::Terminal).then_some(Outcome::Cancelled)
    );
}

fn apply(
    scheduler: &mut Scheduler,
    expected: &mut Expected,
    input: Allocation,
    fault: Fault,
) -> bool {
    assert_expected(scheduler, expected, fault);
    let refusal = expected.record(input);
    let before = scheduler.clone();
    let actual = scheduler.operations.get_mut(&id(1)).unwrap();
    match (fault, refusal) {
        (Fault::Rewind, Some("partial reservation rewound retained capacity")) => {
            actual.reservation.as_mut().unwrap().admitted = resources(input.amounts);
        }
        (
            Fault::ForeignPlacement | Fault::ForeignFullPlacement,
            Some("foreign partial lease was accepted"),
        ) if fault == Fault::ForeignPlacement || input.full => {
            let current = actual.reservation.as_mut().unwrap();
            current.placement = held(input.lease, input.amounts).placement;
        }
        (
            Fault::ForeignReservation | Fault::ForeignFullReservation,
            Some("foreign partial lease was accepted"),
        ) if fault == Fault::ForeignReservation || input.full => {
            actual.reservation.as_mut().unwrap().id = held(input.lease, input.amounts).id;
        }
        (Fault::Excess, Some("excess partial reservation was accepted"))
        | (Fault::Underfunded, Some("underfunded full reservation was accepted")) => {
            actual.spec.resources = resources(input.amounts);
            if !input.full {
                actual
                    .spec
                    .resources
                    .0
                    .insert("memory".into(), u64::from(input.amounts[1]) + 1);
            }
        }
        (Fault::RejectExactFull, None) if input.full && input.amounts == expected.request => {
            actual
                .spec
                .resources
                .0
                .insert("cpu".into(), u64::from(input.amounts[0]) + 1);
        }
        _ => {}
    }
    let substituted = *scheduler != before;
    let accepted = scheduler.apply(&event(input), &mut None).is_ok();
    assert_eq!(
        accepted,
        refusal.is_none(),
        "{}: {input:?}",
        refusal.unwrap_or("valid partial history was rejected")
    );
    // A mutant's acceptance assertion is its terminal observation. Production
    // histories still check rejection atomicity and the complete resource ledger.
    if substituted {
        return true;
    }
    if refusal.is_some() {
        assert_eq!(*scheduler, before, "rejected allocation changed state");
    }
    assert_expected(scheduler, expected, fault);
    false
}

fn trace(
    request: [u8; 2],
    proposals: &[Allocation],
    sibling: u8,
    full: Option<[u8; 2]>,
    fault: Fault,
) {
    let mut scheduler = Scheduler::new();
    for (op, amounts) in [(1, request), (2, [1, 2])] {
        let mut spec = spec(id(op), Orchestration::Leaf).unwrap();
        spec.resources = resources(amounts);
        scheduler
            .apply(
                &SchedulerEvent::Declared {
                    spec: Box::new(spec),
                },
                &mut None,
            )
            .unwrap();
    }
    start(&mut scheduler, id(2), held(sibling, [1, 2])).unwrap();
    if fault == Fault::DuplicateHeld {
        let mut duplicate = scheduler.operation(id(2)).unwrap().clone();
        duplicate.spec.operation_id = id(3);
        scheduler.operations.insert(id(3), duplicate);
    }
    let mut expected = Expected {
        request,
        held: None,
        phase: OperationPhase::WaitingForDependencies,
        cancelled: false,
        revision: 1,
        sibling,
    };
    if fault == Fault::DuplicateHeld {
        let advertised = [request[0] + 3, request[1] + 3];
        let actual = scheduler
            .available_for(
                &format!("worker-{sibling}"),
                &resources(advertised),
                &mut None,
            )
            .unwrap();
        // The input ledger contains exactly one sibling reservation of one CPU.
        // Observe only CPU here: removing this assertion must let the mutant finish.
        assert_cpu_capacity(actual.0["cpu"], u64::from(advertised[0]) - 1);
        return;
    }
    let first = Allocation {
        lease: 0,
        amounts: [0, 0],
        full: false,
    };
    if apply(&mut scheduler, &mut expected, first, fault) {
        return;
    }
    let first = Allocation {
        amounts: [1, 0],
        ..first
    };
    if apply(&mut scheduler, &mut expected, first, fault) {
        return;
    }
    for proposal in [
        Allocation {
            amounts: [0, 0],
            ..first
        },
        Allocation { lease: 1, ..first },
        Allocation { lease: 2, ..first },
        Allocation {
            amounts: [request[0] + 1, 0],
            ..first
        },
        Allocation {
            amounts: request,
            ..first
        },
        Allocation {
            amounts: [request[0] - 1, request[1]],
            full: true,
            ..first
        },
        Allocation {
            lease: 1,
            amounts: request,
            full: true,
        },
        Allocation {
            lease: 2,
            amounts: request,
            full: true,
        },
    ] {
        if apply(&mut scheduler, &mut expected, proposal, fault) {
            return;
        }
    }
    if let Some(amounts) = full {
        if apply(
            &mut scheduler,
            &mut expected,
            Allocation {
                amounts,
                full: true,
                ..first
            },
            fault,
        ) {
            return;
        }
    }
    for &proposal in proposals {
        if apply(&mut scheduler, &mut expected, proposal, fault) {
            return;
        }
    }
    let admitted = expected.phase == OperationPhase::Admitted;
    expected.revision += 1;
    if admitted {
        expected.held = None;
        expected.phase = OperationPhase::Terminal;
    } else {
        expected.cancelled = true;
    }
    scheduler
        .apply(
            &SchedulerEvent::CancellationRequested {
                operation_id: id(1),
                recursive: false,
            },
            &mut None,
        )
        .unwrap();
    assert_expected(&scheduler, &expected, fault);
    if apply(
        &mut scheduler,
        &mut expected,
        Allocation {
            amounts: request,
            full: true,
            ..first
        },
        fault,
    ) {
        return;
    }
    if !admitted {
        expected.revision += 1;
        expected.held = None;
        expected.phase = OperationPhase::Terminal;
        scheduler
            .apply(
                &SchedulerEvent::LeaseReleased {
                    operation_id: id(1),
                    fence: LeaseFence::from(&held(0, [0, 0])),
                },
                &mut None,
            )
            .unwrap();
        assert_expected(&scheduler, &expected, fault);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn partial_capacity_matches_an_independent_resource_ledger(
        cpu in 1..8_u8, memory in 1..8_u8,
        proposals in proptest::collection::vec((0..4_u8, 0..10_u8, 0..10_u8, any::<bool>()), 0..24),
    ) {
        let proposals: Vec<_> = proposals.into_iter().map(|(lease, cpu, memory, full)| Allocation { lease, amounts: [cpu, memory], full }).collect();
        for sibling in 0..2 {
            for full in [None, Some([cpu, memory]), Some([cpu + 1, memory + 1])] {
                trace([cpu, memory], &proposals, sibling, full, Fault::None);
            }
        }
    }
}

#[test]
fn partial_cancellation_keeps_capacity_until_fenced_release() {
    for sibling in 0..2 {
        trace([3, 2], &[], sibling, None, Fault::None);
    }
}

#[test]
#[should_panic(expected = "partial reservation rewound retained capacity")]
fn negative_control_rewinds_partial_capacity() {
    trace([3, 2], &[], 0, None, Fault::Rewind);
}

#[test]
#[should_panic(expected = "foreign partial lease was accepted")]
fn negative_control_changes_partial_placement() {
    trace([3, 2], &[], 0, None, Fault::ForeignPlacement);
}

#[test]
#[should_panic(expected = "foreign partial lease was accepted")]
fn negative_control_changes_partial_reservation_identity() {
    trace([3, 2], &[], 0, None, Fault::ForeignReservation);
}

#[test]
#[should_panic(expected = "excess partial reservation was accepted")]
fn negative_control_accepts_excess_partial_capacity() {
    trace([3, 2], &[], 0, None, Fault::Excess);
}

#[test]
#[should_panic(expected = "underfunded full reservation was accepted")]
fn negative_control_starts_underfunded_work() {
    trace([3, 2], &[], 0, None, Fault::Underfunded);
}

#[test]
#[should_panic(expected = "readiness ignored independently retained capacity")]
fn negative_control_ignores_partial_capacity_when_scheduling() {
    trace([3, 2], &[], 0, None, Fault::IgnoreHeld);
}

#[test]
#[should_panic(expected = "available capacity differs from independent ledger")]
fn negative_control_charges_a_reservation_twice() {
    trace([3, 2], &[], 0, None, Fault::DuplicateHeld);
}

#[test]
#[should_panic(expected = "foreign partial lease was accepted")]
fn negative_control_changes_full_admission_placement() {
    trace([3, 2], &[], 0, None, Fault::ForeignFullPlacement);
}

#[test]
#[should_panic(expected = "foreign partial lease was accepted")]
fn negative_control_changes_full_admission_identity() {
    trace([3, 2], &[], 0, None, Fault::ForeignFullReservation);
}

#[test]
#[should_panic(expected = "valid partial history was rejected")]
fn negative_control_rejects_an_exact_resource_full_match() {
    trace([3, 2], &[], 0, Some([3, 2]), Fault::RejectExactFull);
}
