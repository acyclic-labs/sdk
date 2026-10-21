// Partial admission against an input-only resource ledger, judged by the
// shared oracle; a targeted fault proves each invariant load bearing.
use super::*;
use acyclic_test_support::differential::oracle::{Observer, Oracle, Stop};
use proptest::prelude::*;

#[derive(Clone, Copy, Debug, PartialEq)]
struct Allocation {
    lease: u8,
    amounts: [u8; 2],
    full: bool,
}

#[derive(Debug)]
struct Case {
    request: [u8; 2],
    proposals: Vec<Allocation>,
    sibling: u8,
    full: Option<[u8; 2]>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Fault {
    /// Lowers the retained partial reservation to a rewinding proposal.
    Rewind,
    /// Moves the retained partial lease to a foreign placement.
    ForeignPlacement,
    /// Renames the retained partial lease to a foreign reservation.
    ForeignReservation,
    /// Shrinks the request so an excess partial reservation fits.
    Excess,
    /// Shrinks the request so an underfunded full reservation fits.
    Underfunded,
    /// Grows the request so a complete reservation looks partial.
    AcceptComplete,
    /// Readiness forgets the independently retained partial capacity.
    IgnoreHeld,
    /// Charges the sibling's reservation twice.
    DuplicateHeld,
    /// Moves the retained lease when a full admission arrives.
    ForeignFullPlacement,
    /// Renames the retained lease when a full admission arrives.
    ForeignFullReservation,
    /// Grows the request so an exact full admission is refused.
    RejectExactFull,
    /// A refused allocation still advances the operation revision.
    DirtyRejection,
    /// An accepted allocation leaves the operation revision behind.
    StaleRevision,
}

const CLOSED: &str = "closed admission accepted an allocation";
const UNDERFUNDED: &str = "underfunded full reservation was accepted";
const EXCESS: &str = "excess partial reservation was accepted";
const COMPLETE: &str = "complete reservation was accepted as partial";
const FOREIGN: &str = "foreign partial lease was accepted";
const REWOUND: &str = "partial reservation rewound retained capacity";

#[derive(Debug)]
enum Seen {
    Capacity {
        placement: u8,
        advertised: [u8; 2],
        actual: Option<[u64; 2]>,
        expected: [u64; 2],
    },
    Ready {
        offered: [u8; 2],
        actual: bool,
        expected: bool,
    },
    Operation {
        field: &'static str,
        actual: String,
        expected: String,
    },
    Decision {
        input: Allocation,
        refusal: Option<&'static str>,
        accepted: bool,
    },
    Rejected {
        input: Allocation,
        unchanged: bool,
    },
}

type See<'a, 'b> = &'a mut Observer<'b, Seen>;

// An accepted allocation the reference refused for one of `reasons`.
fn refused(seen: &Seen, reasons: &[&str]) -> std::result::Result<(), String> {
    match seen {
        Seen::Decision {
            input,
            refusal: Some(reason),
            accepted: true,
        } if reasons.contains(reason) => Err(format!("{reason}: {input:?}")),
        _ => Ok(()),
    }
}

fn oracle() -> Oracle<Case, Fault, Seen> {
    let base = || Case {
        request: [3, 2],
        proposals: Vec::new(),
        sibling: 0,
        full: None,
    };
    Oracle::declare(run)
        .invariant("capacity", |seen| match seen {
            Seen::Capacity {
                placement,
                advertised,
                actual,
                expected,
            } if *actual != Some(*expected) => Err(format!(
                "available capacity differs from independent ledger on worker-{placement} \
                 advertising {advertised:?}: {actual:?} != {expected:?}"
            )),
            _ => Ok(()),
        })
        .invariant("readiness", |seen| match seen {
            Seen::Ready {
                offered,
                actual,
                expected,
            } if actual != expected => Err(format!(
                "readiness ignored independently retained capacity at {offered:?}: {actual}"
            )),
            _ => Ok(()),
        })
        .invariant("operation", |seen| match seen {
            Seen::Operation {
                field,
                actual,
                expected,
            } if actual != expected => Err(format!("{field}: {actual} != {expected}")),
            _ => Ok(()),
        })
        .invariant("rewind", |seen| refused(seen, &[REWOUND]))
        .invariant("foreign", |seen| refused(seen, &[FOREIGN]))
        .invariant("excess", |seen| refused(seen, &[EXCESS]))
        .invariant("underfunded", |seen| refused(seen, &[UNDERFUNDED]))
        .invariant("refusal", |seen| refused(seen, &[COMPLETE, CLOSED]))
        .invariant("valid", |seen| match seen {
            Seen::Decision {
                input,
                refusal: None,
                accepted: false,
            } => Err(format!("valid partial history was rejected: {input:?}")),
            _ => Ok(()),
        })
        .invariant("atomicity", |seen| match seen {
            Seen::Rejected {
                input,
                unchanged: false,
            } => Err(format!("rejected allocation changed state: {input:?}")),
            _ => Ok(()),
        })
        .control("capacity", base(), Fault::DuplicateHeld)
        .control("readiness", base(), Fault::IgnoreHeld)
        .control("operation", base(), Fault::StaleRevision)
        .control("rewind", base(), Fault::Rewind)
        .control("foreign", base(), Fault::ForeignPlacement)
        .control("foreign", base(), Fault::ForeignReservation)
        .control("foreign", base(), Fault::ForeignFullPlacement)
        .control("foreign", base(), Fault::ForeignFullReservation)
        .control("excess", base(), Fault::Excess)
        .control("underfunded", base(), Fault::Underfunded)
        .control("refusal", base(), Fault::AcceptComplete)
        .control(
            "valid",
            Case {
                full: Some([3, 2]),
                ..base()
            },
            Fault::RejectExactFull,
        )
        .control("atomicity", base(), Fault::DirtyRejection)
        .build()
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
            return Some(CLOSED);
        }
        if input.full {
            if (0..2).any(|n| input.amounts[n] < self.request[n]) {
                return Some(UNDERFUNDED);
            }
        } else {
            if (0..2).any(|n| input.amounts[n] > self.request[n]) {
                return Some(EXCESS);
            }
            if input.amounts == self.request {
                return Some(COMPLETE);
            }
        }
        if let Some(prior) = self.held {
            if prior.lease != input.lease {
                return Some(FOREIGN);
            }
            if !input.full && (0..2).any(|n| input.amounts[n] < prior.amounts[n]) {
                return Some(REWOUND);
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

    // Available capacity on `placement` when it advertises `advertised`.
    fn available(&self, placement: u8, advertised: [u8; 2]) -> [u64; 2] {
        let mut available = advertised.map(u64::from);
        if placement == self.sibling {
            available = [
                available[0].saturating_sub(1),
                available[1].saturating_sub(2),
            ];
        }
        if let Some(allocation) = self.held
            && placement == allocation.lease % 2
        {
            for (value, amount) in available.iter_mut().zip(allocation.amounts) {
                *value = value.saturating_sub(u64::from(amount));
            }
        }
        available
    }
}

fn observe(
    see: See<'_, '_>,
    scheduler: &Scheduler,
    expected: &Expected,
    fault: Option<Fault>,
) -> std::result::Result<(), Stop> {
    for placement in 0..3_u8 {
        for advertised in [
            [0, 0],
            expected.request,
            [expected.request[0] + 3, expected.request[1] + 3],
        ] {
            see.compare(
                expected.available(placement, advertised),
                || {
                    scheduler
                        .available_for(
                            &format!("worker-{placement}"),
                            &resources(advertised),
                            &mut None,
                        )
                        .ok()
                        .map(|actual| [actual.0["cpu"], actual.0["memory"]])
                },
                |expected, &actual| Seen::Capacity {
                    placement,
                    advertised,
                    actual,
                    expected,
                },
            )?;
        }
    }
    let mut query = scheduler.clone();
    if fault == Some(Fault::IgnoreHeld)
        && let Some(held) = query
            .operations
            .get_mut(&id(1))
            .and_then(|operation| operation.reservation.as_mut())
    {
        held.admitted = resources([0, 0]);
    }
    let allocated = expected.held.map_or([0, 0], |held| held.amounts);
    let remaining = [
        expected.request[0].saturating_sub(allocated[0]),
        expected.request[1].saturating_sub(allocated[1]),
    ];
    let open = !expected.cancelled
        && matches!(
            expected.phase,
            OperationPhase::WaitingForDependencies | OperationPhase::WaitingForCapacity
        );
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
            see.compare(
                open && cpu >= remaining[0] && memory >= remaining[1],
                || {
                    query
                        .ready(&resources([cpu, memory]), &BTreeMap::new(), None)
                        .any(|op| op == id(1))
                },
                |expected, &actual| Seen::Ready {
                    offered: [cpu, memory],
                    actual,
                    expected,
                },
            )?;
        }
    }
    let actual = scheduler.operation(id(1));
    let fields = [
        (
            "resources",
            format!("{:?}", actual.map(|op| &op.spec.resources)),
            format!("{:?}", Some(&resources(expected.request))),
        ),
        (
            "reservation",
            format!("{:?}", actual.map(|op| &op.reservation)),
            format!(
                "{:?}",
                Some(
                    &expected
                        .held
                        .map(|held| self::held(held.lease, held.amounts))
                )
            ),
        ),
        (
            "phase",
            format!("{:?}", actual.map(|op| op.phase)),
            format!("{:?}", Some(expected.phase)),
        ),
        (
            "cancellation",
            format!("{:?}", actual.map(|op| op.cancellation_requested)),
            format!("{:?}", Some(expected.cancelled)),
        ),
        (
            "revision",
            format!("{:?}", actual.map(|op| op.revision)),
            format!("{:?}", Some(expected.revision)),
        ),
        (
            "outcome",
            format!("{:?}", actual.map(|op| &op.outcome)),
            format!(
                "{:?}",
                Some(
                    &(expected.phase == OperationPhase::Terminal)
                        .then_some(Outcome::<FileRef>::Cancelled)
                )
            ),
        ),
    ];
    for (field, actual, expected) in fields {
        see.see(Seen::Operation {
            field,
            actual,
            expected,
        })?;
    }
    Ok(())
}

// Rewrites production state so that the faulted scheduler judges `input`
// the way the named defect would.
fn substitute(
    scheduler: &mut Scheduler,
    expected: &Expected,
    input: Allocation,
    refusal: Option<&'static str>,
    fault: Option<Fault>,
) {
    let Some(fault) = fault else { return };
    let Some(actual) = scheduler.operations.get_mut(&id(1)) else {
        return;
    };
    match (fault, refusal) {
        (Fault::Rewind, Some(REWOUND)) => {
            if let Some(current) = actual.reservation.as_mut() {
                current.admitted = resources(input.amounts);
            }
        }
        (Fault::ForeignPlacement | Fault::ForeignFullPlacement, Some(FOREIGN))
            if fault == Fault::ForeignPlacement || input.full =>
        {
            if let Some(current) = actual.reservation.as_mut() {
                current.placement = held(input.lease, input.amounts).placement;
            }
        }
        (Fault::ForeignReservation | Fault::ForeignFullReservation, Some(FOREIGN))
            if fault == Fault::ForeignReservation || input.full =>
        {
            if let Some(current) = actual.reservation.as_mut() {
                current.id = held(input.lease, input.amounts).id;
            }
        }
        (Fault::Excess, Some(EXCESS))
        | (Fault::Underfunded, Some(UNDERFUNDED))
        | (Fault::AcceptComplete, Some(COMPLETE)) => {
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
}

fn step(
    see: See<'_, '_>,
    scheduler: &mut Scheduler,
    expected: &mut Expected,
    input: Allocation,
    fault: Option<Fault>,
) -> std::result::Result<(), Stop> {
    observe(see, scheduler, expected, fault)?;
    let refusal = expected.record(input);
    let before = scheduler.clone();
    substitute(scheduler, expected, input, refusal, fault);
    let accepted = see.compare(
        refusal,
        || scheduler.apply(&event(input), &mut None).is_ok(),
        |refusal, &accepted| Seen::Decision {
            input,
            refusal,
            accepted,
        },
    )?;
    let drift = match fault {
        Some(Fault::DirtyRejection) if !accepted => 1,
        Some(Fault::StaleRevision) if accepted => -1,
        _ => 0,
    };
    if drift != 0
        && let Some(actual) = scheduler.operations.get_mut(&id(1))
    {
        actual.revision = actual.revision.saturating_add_signed(drift);
    }
    if refusal.is_some() {
        see.see(Seen::Rejected {
            input,
            unchanged: *scheduler == before,
        })?;
    }
    observe(see, scheduler, expected, fault)
}

// A lifecycle event the reference expects production to accept.
fn lifecycle(
    see: See<'_, '_>,
    scheduler: &mut Scheduler,
    event: &SchedulerEvent,
    field: &'static str,
) -> std::result::Result<(), Stop> {
    see.compare(
        format!("{:?}", Ok::<(), ()>(())),
        || {
            format!(
                "{:?}",
                scheduler.apply(event, &mut None).map(drop).map_err(drop)
            )
        },
        |expected, actual| Seen::Operation {
            field,
            actual: actual.clone(),
            expected,
        },
    )
    .map(drop)
}

fn run(case: &Case, fault: Option<&Fault>, see: See<'_, '_>) -> std::result::Result<(), Stop> {
    let fault = fault.copied();
    let Case {
        request,
        ref proposals,
        sibling,
        full,
    } = *case;
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
    if fault == Some(Fault::DuplicateHeld) {
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
    let first = Allocation {
        lease: 0,
        amounts: [0, 0],
        full: false,
    };
    step(see, &mut scheduler, &mut expected, first, fault)?;
    let first = Allocation {
        amounts: [1, 0],
        ..first
    };
    step(see, &mut scheduler, &mut expected, first, fault)?;
    let fixed = [
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
    ];
    let full = full.map(|amounts| Allocation {
        amounts,
        full: true,
        ..first
    });
    for &proposal in fixed.iter().chain(&full).chain(proposals) {
        step(see, &mut scheduler, &mut expected, proposal, fault)?;
    }
    let admitted = expected.phase == OperationPhase::Admitted;
    expected.revision += 1;
    if admitted {
        expected.held = None;
        expected.phase = OperationPhase::Terminal;
    } else {
        expected.cancelled = true;
    }
    let cancellation = SchedulerEvent::CancellationRequested {
        operation_id: id(1),
        recursive: false,
    };
    lifecycle(see, &mut scheduler, &cancellation, "cancellation request")?;
    observe(see, &scheduler, &expected, fault)?;
    let closed = Allocation {
        amounts: request,
        full: true,
        ..first
    };
    step(see, &mut scheduler, &mut expected, closed, fault)?;
    if !admitted {
        expected.revision += 1;
        expected.held = None;
        expected.phase = OperationPhase::Terminal;
        let release = SchedulerEvent::LeaseReleased {
            operation_id: id(1),
            fence: LeaseFence::from(&held(0, [0, 0])),
        };
        lifecycle(see, &mut scheduler, &release, "fenced release")?;
        observe(see, &scheduler, &expected, fault)?;
    }
    Ok(())
}

#[test]
fn partial_capacity_matches_an_independent_resource_ledger() {
    let oracle = oracle();
    proptest!(ProptestConfig::with_cases(64), |(
        cpu in 1..8_u8, memory in 1..8_u8,
        proposals in proptest::collection::vec((0..4_u8, 0..10_u8, 0..10_u8, any::<bool>()), 0..24),
    )| {
        let proposals: Vec<_> = proposals.into_iter().map(|(lease, cpu, memory, full)| Allocation { lease, amounts: [cpu, memory], full }).collect();
        for sibling in 0..2 {
            for full in [None, Some([cpu, memory]), Some([cpu + 1, memory + 1])] {
                oracle.check(&Case { request: [cpu, memory], proposals: proposals.clone(), sibling, full });
            }
        }
    });
}

#[test]
fn partial_cancellation_keeps_capacity_until_fenced_release() {
    let oracle = oracle();
    for sibling in 0..2 {
        oracle.check(&Case {
            request: [3, 2],
            proposals: Vec::new(),
            sibling,
            full: None,
        });
    }
}
