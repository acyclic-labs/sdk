// Model dispatch claims against an input-only shared-budget ledger, judged by
// the shared oracle; a targeted fault proves each invariant load bearing.
use super::*;
use acyclic_test_support::differential::oracle::{Observer, Oracle, Stop};

#[derive(Clone, Copy, Debug)]
struct Claim {
    task: u8,
    attempt: u8,
    step: u8,
    digest: u8,
    ceiling: u8,
    lease: u8,
}

#[derive(Debug)]
struct Trace {
    ceiling: u8,
    inputs: Vec<Claim>,
    cancel: u8,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Fault {
    /// Charges an exact retry as a fresh claim.
    ChargeRetry,
    /// Drops the sibling's charges when the shared budget is full.
    ForgetSibling,
    /// Overwrites a retained claim with a changed request.
    ChangeRequest,
    /// Re-pins the task ceiling to the claim's ceiling.
    ChangeCeiling,
    /// Clears the owner's cancellation before its claim.
    IgnoreCancellation,
    /// Loses the replaced owner's charges with its lease.
    ForgetAfterLease,
    /// Corrupts state after a correctly rejected claim.
    MutateRejected,
}

type Claims = BTreeMap<OperationId, (u64, BTreeMap<OperationId, BTreeMap<u32, [u8; 32]>>, u64)>;

#[derive(Debug)]
enum Seen {
    Ledger {
        actual: Claims,
        expected: Claims,
    },
    Step {
        what: &'static str,
        ok: bool,
    },
    Admission {
        claim: Claim,
        refusal: Option<&'static str>,
        accepted: bool,
    },
    Rejected {
        unchanged: bool,
    },
}

type See<'a, 'b> = &'a mut Observer<'b, Seen>;

struct Ledger {
    entries: BTreeMap<(u8, u8, u8), u8>,
    ceilings: [Option<u8>; 2],
    leases: [u8; 2],
    cancelled: [bool; 2],
}

impl Ledger {
    // No production reducer, projection or event is used by the reference model.
    fn record(&mut self, claim: Claim) -> Option<&'static str> {
        let task = usize::from(claim.task);
        if self.cancelled[task] {
            return Some("cancelled owner claim was accepted");
        }
        if claim.lease != self.leases[task] {
            return Some("foreign owner claim was accepted");
        }
        if claim.digest == 0 || !(1..=2).contains(&claim.ceiling) {
            return Some("invalid model claim was accepted");
        }
        if self.ceilings[task].is_some_and(|ceiling| ceiling != claim.ceiling) {
            return Some("pinned model ceiling changed");
        }
        let key = (claim.task, claim.attempt, claim.step);
        if let Some(prior) = self.entries.get(&key) {
            return if *prior == claim.digest {
                None
            } else {
                Some("changed claim request was accepted")
            };
        }
        if self.entries.len() == 2 {
            return Some("shared model budget exceeded");
        }
        let used = self
            .entries
            .keys()
            .filter(|(owner, _, _)| *owner == claim.task)
            .count();
        if used >= usize::from(claim.ceiling) {
            return Some("task model ceiling exceeded");
        }
        self.entries.insert(key, claim.digest);
        self.ceilings[task] = Some(claim.ceiling);
        None
    }

    fn expected(&self) -> Claims {
        let mut expected = BTreeMap::new();
        for task in 0..2_u8 {
            let Some(ceiling) = self.ceilings[usize::from(task)] else {
                continue;
            };
            let mut attempts: BTreeMap<OperationId, BTreeMap<u32, [u8; 32]>> = BTreeMap::new();
            let mut used = 0_u64;
            for (&(owner, attempt, step), &digest) in &self.entries {
                if owner == task {
                    attempts
                        .entry(id(attempt + 20))
                        .or_default()
                        .insert(u32::from(step), [digest; 32]);
                    used += 1;
                }
            }
            expected.insert(id(task + 2), (u64::from(ceiling), attempts, used));
        }
        expected
    }
}

fn oracle() -> Oracle<Trace, Fault, Seen> {
    let trace = || Trace {
        ceiling: 2,
        inputs: Vec::new(),
        cancel: 0,
    };
    Oracle::declare(run)
        .invariant("ledger", |seen| match seen {
            Seen::Ledger { actual, expected } if actual != expected => Err(format!(
                "claim ledger {actual:?} differs from independent inputs {expected:?}"
            )),
            Seen::Step { what, ok: false } => Err(format!("{what} failed")),
            _ => Ok(()),
        })
        .invariant("admission", |seen| match seen {
            Seen::Admission {
                claim,
                refusal,
                accepted,
            } if *accepted != refusal.is_none() => Err(format!(
                "{}: {claim:?}",
                refusal.unwrap_or("valid claim was rejected")
            )),
            _ => Ok(()),
        })
        .invariant("rejection", |seen| match seen {
            Seen::Rejected { unchanged: false } => Err("rejected claim changed state".into()),
            _ => Ok(()),
        })
        .control("ledger", trace(), Fault::ChargeRetry)
        .control("ledger", trace(), Fault::ForgetAfterLease)
        .control("admission", trace(), Fault::ForgetSibling)
        .control("admission", trace(), Fault::ChangeRequest)
        .control("admission", trace(), Fault::ChangeCeiling)
        .control("admission", trace(), Fault::IgnoreCancellation)
        .control("rejection", trace(), Fault::MutateRejected)
        .build()
}

fn observe(
    see: See<'_, '_>,
    scheduler: &Scheduler,
    ledger: &Ledger,
) -> std::result::Result<(), Stop> {
    let actual = scheduler
        .model_claims
        .iter()
        .map(|(owner, claims)| {
            let claims = (claims.ceiling, claims.attempts.clone(), claims.used());
            (*owner, claims)
        })
        .collect();
    see.see(Seen::Ledger {
        actual,
        expected: ledger.expected(),
    })
}

fn step(
    see: See<'_, '_>,
    what: &'static str,
    result: Result<impl Sized>,
) -> std::result::Result<(), Stop> {
    let ok = result.is_ok();
    see.see(Seen::Step { what, ok })?;
    if ok { Ok(()) } else { Err(Stop) }
}

struct Run<'a, 'b> {
    scheduler: Scheduler,
    ledger: Ledger,
    fault: Option<Fault>,
    see: See<'a, 'b>,
}

impl Run<'_, '_> {
    fn apply(&mut self, input: Claim) -> std::result::Result<(), Stop> {
        let Self {
            scheduler,
            ledger,
            fault,
            see,
        } = self;
        observe(see, scheduler, ledger)?;
        let key = (input.task, input.attempt, input.step);
        let retained = ledger.entries.get(&key).copied();
        let pinned = ledger.ceilings[usize::from(input.task)];
        let refusal = ledger.record(input);
        let before = scheduler.clone();
        let op = id(input.task + 2);
        match fault {
            Some(Fault::ForgetSibling) if refusal == Some("shared model budget exceeded") => {
                scheduler.model_claims.remove(&id(3 - input.task));
            }
            Some(Fault::ChangeRequest) if refusal == Some("changed claim request was accepted") => {
                let claims = scheduler.model_claims.get_mut(&op).unwrap();
                let attempt = claims.attempts.get_mut(&id(input.attempt + 20)).unwrap();
                attempt.insert(u32::from(input.step), [input.digest; 32]);
            }
            Some(Fault::ChangeCeiling) if refusal == Some("pinned model ceiling changed") => {
                scheduler.model_claims.get_mut(&op).unwrap().ceiling = u64::from(input.ceiling);
            }
            Some(Fault::IgnoreCancellation)
                if refusal == Some("cancelled owner claim was accepted") =>
            {
                let operation = scheduler.operations.get_mut(&op).unwrap();
                operation.cancellation_requested = false;
            }
            _ => {}
        }
        let accepted = scheduler.apply(&event(input), &mut None).is_ok();
        see.see(Seen::Admission {
            claim: input,
            refusal,
            accepted,
        })?;
        if refusal.is_some() {
            if *fault == Some(Fault::MutateRejected) {
                // Preserve the real rejected result, but corrupt state after it.
                scheduler.operations.get_mut(&op).unwrap().revision += 1;
            }
            see.see(Seen::Rejected {
                unchanged: *scheduler == before,
            })?;
        }
        let duplicate = *fault == Some(Fault::ChargeRetry)
            && retained == Some(input.digest)
            && pinned == Some(input.ceiling)
            && refusal.is_none();
        if duplicate {
            let claims = scheduler.model_claims.get_mut(&op).unwrap();
            claims
                .attempts
                .entry(id(250))
                .or_default()
                .insert(0, [1; 32]);
        }
        observe(see, scheduler, ledger)
    }

    fn step(&mut self, what: &'static str, event: SchedulerEvent) -> std::result::Result<(), Stop> {
        step(self.see, what, self.scheduler.apply(&event, &mut None))?;
        observe(self.see, &self.scheduler, &self.ledger)
    }
}

fn event(input: Claim) -> SchedulerEvent {
    SchedulerEvent::ModelDispatchClaimed {
        operation_id: id(input.task + 2),
        attempt_id: id(input.attempt + 20),
        step: u32::from(input.step),
        request_digest: [input.digest; 32],
        fence: fence(input.lease),
        ceiling: u64::from(input.ceiling),
    }
}

fn fixture() -> Scheduler {
    let root = spec(id(1), Orchestration::Join).unwrap();
    let mut scheduler = session(
        root.clone(),
        SessionLimits {
            active_tasks: 2,
            total_tasks: 3,
            depth: 1,
            model_steps: 2,
        },
    )
    .unwrap();
    for task in 0..2_u8 {
        let mut spec = child(task + 2, 1, &task.to_string()).unwrap();
        spec.owner = root.owner.clone();
        scheduler
            .apply(
                &SchedulerEvent::Declared {
                    spec: Box::new(spec),
                },
                &mut None,
            )
            .unwrap();
        start(&mut scheduler, id(task + 2), reservation(task, 0)).unwrap();
    }
    scheduler
}

fn run(trace: &Trace, fault: Option<&Fault>, see: See<'_, '_>) -> std::result::Result<(), Stop> {
    let Trace {
        ceiling,
        ref inputs,
        cancel,
    } = *trace;
    let mut run = Run {
        scheduler: fixture(),
        ledger: Ledger {
            entries: BTreeMap::new(),
            ceilings: [None; 2],
            leases: [0, 1],
            cancelled: [false; 2],
        },
        fault: fault.copied(),
        see,
    };
    let first = Claim {
        task: 0,
        attempt: 0,
        step: 0,
        digest: 1,
        ceiling,
        lease: 0,
    };
    run.apply(first)?;
    run.apply(first)?;
    run.apply(Claim { digest: 2, ..first })?;
    run.apply(Claim {
        ceiling: 3 - ceiling,
        ..first
    })?;
    run.apply(Claim { lease: 1, ..first })?;
    for &input in inputs {
        run.apply(input)?;
    }
    let second = Claim {
        task: 1,
        ceiling: 2,
        lease: 1,
        ..first
    };
    run.apply(second)?;
    run.apply(Claim { step: 1, ..first })?;
    // Exact retries do not consume additional capacity, even at the shared ceiling.
    run.apply(first)?;

    run.ledger.cancelled[usize::from(cancel)] = true;
    run.step(
        "cancellation",
        SchedulerEvent::CancellationRequested {
            operation_id: id(cancel + 2),
            recursive: false,
        },
    )?;
    run.apply(if cancel == 0 { first } else { second })?;
    let replacement = 1 - cancel;
    run.step(
        "lease release",
        SchedulerEvent::LeaseReleased {
            operation_id: id(replacement + 2),
            fence: fence(replacement),
        },
    )?;
    let started = start(&mut run.scheduler, id(replacement + 2), reservation(2, 0));
    step(run.see, "replacement start", started)?;
    run.ledger.leases[usize::from(replacement)] = 2;
    if run.fault == Some(Fault::ForgetAfterLease) {
        run.scheduler.model_claims.remove(&id(replacement + 2));
    }
    observe(run.see, &run.scheduler, &run.ledger)?;
    let original = if replacement == 0 { first } else { second };
    run.apply(original)?;
    run.apply(Claim {
        lease: 2,
        ..original
    })?;
    let fresh = Claim {
        task: replacement,
        attempt: 9,
        step: 0,
        digest: 1,
        ceiling: run.ledger.ceilings[usize::from(replacement)].unwrap_or(2),
        lease: 2,
    };
    run.apply(fresh)?;
    run.step(
        "cancelled completion",
        SchedulerEvent::Completed {
            operation_id: id(cancel + 2),
            outcome: Outcome::Cancelled,
            fence: Some(fence(cancel)),
            execution_duration_ns: None,
        },
    )?;
    run.apply(fresh)
}

#[test]
fn model_claims_match_an_independent_shared_budget() {
    let oracle = oracle();
    let claim = (0..2_u8, 0..3_u8, 0..3_u8, 0..4_u8, 0..4_u8, 0..4_u8);
    proptest!(ProptestConfig::with_cases(64), |(
        inputs in proptest::collection::vec(claim, 0..32)
    )| {
        let inputs: Vec<_> = inputs
            .into_iter()
            .map(|(task, attempt, step, digest, ceiling, lease)| Claim {
                task, attempt, step, digest, ceiling, lease,
            })
            .collect();
        for ceiling in 1..=2 {
            for cancel in 0..2 {
                oracle.check(&Trace { ceiling, inputs: inputs.clone(), cancel });
            }
        }
    });
}
