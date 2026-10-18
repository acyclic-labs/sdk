// Lease histories against an input-only reference, judged by the shared
// oracle; a targeted fault proves each invariant load bearing.
use super::*;
use acyclic_test_support::differential::oracle::{Observer, Oracle, Stop};
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

#[derive(Debug)]
struct Trace {
    reassignments: u8,
    uncertainty: u8,
    base: u8,
    run: bool,
    cancel: u8,
    finish: u8,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Fault {
    /// Rewrites the live placement to match a stale release.
    IgnorePlacement,
    /// Rewrites the live reservation identity to match a stale release.
    IgnoreReservation,
    /// Drops the lease when execution becomes uncertain.
    DropUncertainLease,
    /// Records a terminal retry in the completion order again.
    DuplicateTerminalRetry,
}

/// Operation state as the reference predicts it or as the scheduler shows it.
#[derive(Debug, PartialEq)]
struct State {
    reservation: Option<Reservation>,
    phase: OperationPhase,
    cancelled: bool,
    outcome: Option<Outcome<FileRef>>,
    revision: u64,
}

#[derive(Debug)]
enum Seen {
    Completion {
        actual: Vec<OperationId>,
        expected: Vec<OperationId>,
    },
    State {
        actual: State,
        expected: State,
    },
    Owner {
        candidate: u8,
        settlement: bool,
        actual: bool,
        expected: bool,
    },
    Applied {
        input: Input,
        accepted: bool,
        expected: bool,
    },
    Rejected {
        input: Input,
        unchanged: bool,
    },
}

type See<'a, 'b> = &'a mut Observer<'b, Seen>;

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
            cause: crate::IndeterminateCause::Unobserved,
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

fn oracle() -> Oracle<Trace, Fault, Seen> {
    let trace = |reassignments, uncertainty| Trace {
        reassignments,
        uncertainty,
        base: 0,
        run: true,
        cancel: 0,
        finish: 1,
    };
    Oracle::declare(run)
        .invariant("completion", |seen| match seen {
            Seen::Completion { actual, expected } if actual != expected => Err(format!(
                "terminal retry duplicated completion history: {actual:?} != {expected:?}"
            )),
            _ => Ok(()),
        })
        .invariant("history", |seen| match seen {
            Seen::State { actual, expected } if actual != expected => Err(format!(
                "{actual:?} differs from independent history {expected:?}"
            )),
            Seen::Owner {
                candidate,
                settlement,
                actual,
                expected,
            } if actual != expected => Err(format!(
                "execution ownership of lease {candidate} (settlement {settlement}) differs \
                 from independent history"
            )),
            _ => Ok(()),
        })
        .invariant("acceptance", |seen| match seen {
            Seen::Applied {
                input,
                accepted,
                expected,
            } if accepted != expected => Err(format!(
                "{input:?} accepted {accepted}, expected {expected}"
            )),
            Seen::Rejected {
                input,
                unchanged: false,
            } => Err(format!("rejected {input:?} changed the scheduler")),
            _ => Ok(()),
        })
        .control("acceptance", trace(1, 0), Fault::IgnorePlacement)
        .control("acceptance", trace(2, 0), Fault::IgnoreReservation)
        .control("history", trace(0, 1), Fault::DropUncertainLease)
        .control("completion", trace(0, 0), Fault::DuplicateTerminalRetry)
        .build()
}

fn observe(
    see: See<'_, '_>,
    scheduler: &Scheduler,
    expected: &Expected,
) -> std::result::Result<(), Stop> {
    let terminal = expected.phase == OperationPhase::Terminal;
    see.see(Seen::Completion {
        actual: scheduler.completion_order.clone(),
        expected: if terminal { vec![id(1)] } else { vec![] },
    })?;
    let actual = scheduler.operation(id(1)).unwrap();
    see.see(Seen::State {
        actual: State {
            reservation: actual.reservation.clone(),
            phase: actual.phase,
            cancelled: actual.cancellation_requested,
            outcome: actual.outcome.clone(),
            revision: actual.revision,
        },
        expected: State {
            reservation: expected.lease.map(held),
            phase: expected.phase,
            cancelled: expected.cancelled,
            outcome: expected.outcome.clone(),
            revision: expected.revision,
        },
    })?;
    let current = expected.lease.unwrap_or(0);
    for candidate in [current, current ^ 1, current + 2] {
        for settlement in [false, true] {
            let owner = expected.lease == Some(candidate)
                && (expected.phase == OperationPhase::Running
                    || (settlement && expected.phase == OperationPhase::Reconciling))
                && (settlement || !expected.cancelled);
            let fence = lease_fence(candidate);
            see.see(Seen::Owner {
                candidate,
                settlement,
                actual: require_execution_owner(actual, &fence, settlement, &mut None).is_ok(),
                expected: owner,
            })?;
        }
    }
    Ok(())
}

struct Run<'a, 'b> {
    scheduler: Scheduler,
    expected: Expected,
    fault: Option<Fault>,
    see: See<'a, 'b>,
}

impl Run<'_, '_> {
    fn accept(&mut self, input: Input) -> std::result::Result<(), Stop> {
        self.expected.record(input);
        let accepted = self.scheduler.apply(&event(input), &mut None).is_ok();
        let operation = self.scheduler.operations.get_mut(&id(1)).unwrap();
        match (self.fault, input) {
            (Some(Fault::DropUncertainLease), Input::Uncertain(_)) => operation.reservation = None,
            (Some(Fault::DuplicateTerminalRetry), Input::Retry(_)) => {
                self.scheduler.completion_order.push(id(1))
            }
            _ => {}
        }
        self.see.see(Seen::Applied {
            input,
            accepted,
            expected: true,
        })?;
        observe(self.see, &self.scheduler, &self.expected)
    }

    fn reject(&mut self, input: Input) -> std::result::Result<(), Stop> {
        observe(self.see, &self.scheduler, &self.expected)?;
        let before = self.scheduler.clone();
        let operation = self.scheduler.operations.get_mut(&id(1)).unwrap();
        if let Input::Release(lease) = input
            && let Some(current) = operation.reservation.as_mut()
        {
            match self.fault {
                Some(Fault::IgnorePlacement) => current.placement = held(lease).placement,
                Some(Fault::IgnoreReservation) => current.id = held(lease).id,
                _ => {}
            }
        }
        let accepted = self.scheduler.apply(&event(input), &mut None).is_ok();
        self.see.see(Seen::Applied {
            input,
            accepted,
            expected: false,
        })?;
        self.see.see(Seen::Rejected {
            input,
            unchanged: self.scheduler == before,
        })?;
        observe(self.see, &self.scheduler, &self.expected)
    }
}

fn run(trace: &Trace, fault: Option<&Fault>, see: See<'_, '_>) -> std::result::Result<(), Stop> {
    let Trace {
        reassignments,
        uncertainty,
        base,
        run: running,
        cancel,
        finish,
    } = *trace;
    let mut scheduler = Scheduler::new();
    scheduler
        .apply(
            &SchedulerEvent::Declared {
                spec: Box::new(spec(id(1), Orchestration::Leaf).unwrap()),
            },
            &mut None,
        )
        .unwrap();
    let mut run = Run {
        scheduler,
        expected: Expected {
            phase: OperationPhase::WaitingForDependencies,
            lease: None,
            cancelled: false,
            outcome: None,
            revision: 1,
        },
        fault: fault.copied(),
        see,
    };
    observe(run.see, &run.scheduler, &run.expected)?;
    let lease = base + reassignments;
    for current in base..=lease {
        run.accept(Input::Admit(current))?;
        run.reject(Input::Admit(current + 1))?;
        if current == lease && cancel == 3 {
            run.accept(Input::Cancel)?;
            break;
        }
        if running || current == lease {
            run.accept(Input::Start(current))?;
        }
        for old in base..current {
            run.reject(Input::Release(old))?;
            run.reject(Input::Start(old))?;
            run.reject(Input::Finish(old, 1))?;
        }
        if current != lease {
            run.accept(Input::Release(current))?;
        }
    }
    if cancel != 3 {
        if cancel == 1 {
            run.accept(Input::Cancel)?;
        }
        for round in 0..uncertainty {
            run.accept(Input::Uncertain(lease))?;
            run.reject(Input::Release(lease))?;
            if cancel == 2 && round + 1 == uncertainty {
                run.accept(Input::Cancel)?;
            }
            if !run.expected.cancelled {
                run.accept(Input::Resume(lease))?;
            } else {
                run.reject(Input::Resume(lease))?;
            }
        }
        if cancel == 2 && uncertainty == 0 {
            run.accept(Input::Cancel)?;
        }
        run.reject(Input::Retry(finish))?;
        run.accept(Input::Finish(lease, finish))?;
    }
    let finish = if cancel == 3 { 2 } else { finish };
    run.accept(Input::Retry(finish))?;
    run.accept(Input::Cancel)?;
    for input in [
        Input::Admit(lease + 1),
        Input::Start(lease),
        Input::Release(lease),
        Input::Finish(lease, finish),
    ] {
        run.reject(input)?;
    }
    Ok(())
}

#[test]
fn lease_histories_match_an_independent_model() {
    let oracle = oracle();
    proptest!(ProptestConfig::with_cases(64), |(
        reassignments in 0..6_u8, uncertainty in 0..6_u8, base in 0..16_u8, run in any::<bool>()
    )| {
        for cancel in 0..4 {
            for finish in 0..3 {
                let base = base * 2;
                let trace = Trace { reassignments, uncertainty, base, run, cancel, finish };
                oracle.check(&trace);
            }
        }
    });
}
