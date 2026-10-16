use super::*;
use proptest::prelude::*;

#[derive(Clone, Copy, Debug)]
struct Claim {
    task: u8,
    attempt: u8,
    step: u8,
    digest: u8,
    ceiling: u8,
    lease: u8,
}

#[derive(Clone, Copy, PartialEq)]
enum Fault {
    None,
    ChargeRetry,
    ForgetSibling,
    ChangeRequest,
    ChangeCeiling,
    IgnoreCancellation,
}

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

fn assert_ledger(scheduler: &Scheduler, ledger: &Ledger) {
    assert_eq!(
        scheduler.model_claims.len(),
        ledger.ceilings.iter().flatten().count()
    );
    for task in 0..2_u8 {
        let mut attempts: BTreeMap<OperationId, BTreeMap<u32, [u8; 32]>> = BTreeMap::new();
        for (&(owner, attempt, step), &digest) in &ledger.entries {
            if owner == task {
                attempts
                    .entry(id(attempt + 20))
                    .or_default()
                    .insert(u32::from(step), [digest; 32]);
            }
        }
        match scheduler.model_claims.get(&id(task + 2)) {
            Some(actual) => {
                assert_eq!(
                    actual.attempts, attempts,
                    "claim ledger differs from independent inputs"
                );
                assert_eq!(
                    Some(actual.ceiling),
                    ledger.ceilings[usize::from(task)].map(u64::from)
                );
                assert_eq!(
                    actual.used(),
                    ledger
                        .entries
                        .keys()
                        .filter(|(owner, _, _)| *owner == task)
                        .count() as u64
                );
            }
            None => assert!(attempts.is_empty()),
        }
    }
    assert_eq!(
        scheduler
            .model_claims
            .values()
            .map(TaskModelClaims::used)
            .sum::<u64>(),
        ledger.entries.len() as u64
    );
}

fn apply(scheduler: &mut Scheduler, ledger: &mut Ledger, input: Claim, fault: Fault) {
    assert_ledger(scheduler, ledger);
    let key = (input.task, input.attempt, input.step);
    let retained = ledger.entries.get(&key).copied();
    let pinned = ledger.ceilings[usize::from(input.task)];
    let refusal = ledger.record(input);
    let before = scheduler.clone();
    let op = id(input.task + 2);
    match fault {
        Fault::ForgetSibling if refusal == Some("shared model budget exceeded") => {
            scheduler.model_claims.remove(&id(3 - input.task));
        }
        Fault::ChangeRequest if refusal == Some("changed claim request was accepted") => {
            scheduler
                .model_claims
                .get_mut(&op)
                .unwrap()
                .attempts
                .get_mut(&id(input.attempt + 20))
                .unwrap()
                .insert(u32::from(input.step), [input.digest; 32]);
        }
        Fault::ChangeCeiling if refusal == Some("pinned model ceiling changed") => {
            scheduler.model_claims.get_mut(&op).unwrap().ceiling = u64::from(input.ceiling);
        }
        Fault::IgnoreCancellation if refusal == Some("cancelled owner claim was accepted") => {
            scheduler
                .operations
                .get_mut(&op)
                .unwrap()
                .cancellation_requested = false;
        }
        _ => {}
    }
    assert_eq!(
        scheduler.apply(&event(input), &mut None).is_ok(),
        refusal.is_none(),
        "{}: {input:?}",
        refusal.unwrap_or("valid claim was rejected")
    );
    if refusal.is_some() {
        assert_eq!(*scheduler, before, "rejected claim changed state");
    }
    if fault == Fault::ChargeRetry
        && retained == Some(input.digest)
        && pinned == Some(input.ceiling)
        && refusal.is_none()
    {
        scheduler
            .model_claims
            .get_mut(&op)
            .unwrap()
            .attempts
            .entry(id(250))
            .or_default()
            .insert(0, [1; 32]);
    }
    assert_ledger(scheduler, ledger);
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

fn trace(ceiling: u8, inputs: &[Claim], cancel: u8, fault: Fault) {
    let mut scheduler = fixture();
    let mut ledger = Ledger {
        entries: BTreeMap::new(),
        ceilings: [None; 2],
        leases: [0, 1],
        cancelled: [false; 2],
    };
    let first = Claim {
        task: 0,
        attempt: 0,
        step: 0,
        digest: 1,
        ceiling,
        lease: 0,
    };
    apply(&mut scheduler, &mut ledger, first, fault);
    apply(&mut scheduler, &mut ledger, first, fault);
    apply(
        &mut scheduler,
        &mut ledger,
        Claim { digest: 2, ..first },
        fault,
    );
    apply(
        &mut scheduler,
        &mut ledger,
        Claim {
            ceiling: 3 - ceiling,
            ..first
        },
        fault,
    );
    apply(
        &mut scheduler,
        &mut ledger,
        Claim { lease: 1, ..first },
        fault,
    );
    for &input in inputs {
        apply(&mut scheduler, &mut ledger, input, fault);
    }
    let second = Claim {
        task: 1,
        ceiling: 2,
        lease: 1,
        ..first
    };
    apply(&mut scheduler, &mut ledger, second, fault);
    apply(
        &mut scheduler,
        &mut ledger,
        Claim { step: 1, ..first },
        fault,
    );
    // Exact retries do not consume additional capacity, even at the shared ceiling.
    apply(&mut scheduler, &mut ledger, first, fault);

    ledger.cancelled[usize::from(cancel)] = true;
    scheduler
        .apply(
            &SchedulerEvent::CancellationRequested {
                operation_id: id(cancel + 2),
                recursive: false,
            },
            &mut None,
        )
        .unwrap();
    assert_ledger(&scheduler, &ledger);
    apply(
        &mut scheduler,
        &mut ledger,
        if cancel == 0 { first } else { second },
        fault,
    );
    let replacement = 1 - cancel;
    scheduler
        .apply(
            &SchedulerEvent::LeaseReleased {
                operation_id: id(replacement + 2),
                fence: fence(replacement),
            },
            &mut None,
        )
        .unwrap();
    start(&mut scheduler, id(replacement + 2), reservation(2, 0)).unwrap();
    ledger.leases[usize::from(replacement)] = 2;
    assert_ledger(&scheduler, &ledger);
    let original = if replacement == 0 { first } else { second };
    apply(&mut scheduler, &mut ledger, original, fault);
    apply(
        &mut scheduler,
        &mut ledger,
        Claim {
            lease: 2,
            ..original
        },
        fault,
    );
    let fresh = Claim {
        task: replacement,
        attempt: 9,
        step: 0,
        digest: 1,
        ceiling: ledger.ceilings[usize::from(replacement)].unwrap_or(2),
        lease: 2,
    };
    apply(&mut scheduler, &mut ledger, fresh, fault);
    scheduler
        .apply(
            &SchedulerEvent::Completed {
                operation_id: id(cancel + 2),
                outcome: Outcome::Cancelled,
                fence: Some(fence(cancel)),
                execution_duration_ns: None,
            },
            &mut None,
        )
        .unwrap();
    assert_ledger(&scheduler, &ledger);
    apply(&mut scheduler, &mut ledger, fresh, fault);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn model_claims_match_an_independent_shared_budget(
        inputs in proptest::collection::vec((0..2_u8, 0..3_u8, 0..3_u8, 0..4_u8, 0..4_u8, 0..4_u8), 0..32),
    ) {
        let inputs: Vec<_> = inputs.into_iter().map(|(task, attempt, step, digest, ceiling, lease)|
            Claim { task, attempt, step, digest, ceiling, lease }).collect();
        for ceiling in 1..=2 {
            for cancel in 0..2 { trace(ceiling, &inputs, cancel, Fault::None); }
        }
    }
}

#[test]
#[should_panic(expected = "claim ledger differs from independent inputs")]
fn negative_control_charges_an_exact_retry_again() {
    trace(2, &[], 0, Fault::ChargeRetry);
}

#[test]
#[should_panic(expected = "shared model budget exceeded")]
fn negative_control_forgets_a_sibling_charge() {
    trace(2, &[], 0, Fault::ForgetSibling);
}

#[test]
#[should_panic(expected = "changed claim request was accepted")]
fn negative_control_accepts_a_changed_retry() {
    trace(2, &[], 0, Fault::ChangeRequest);
}

#[test]
#[should_panic(expected = "pinned model ceiling changed")]
fn negative_control_changes_the_pinned_task_ceiling() {
    trace(2, &[], 0, Fault::ChangeCeiling);
}

#[test]
#[should_panic(expected = "cancelled owner claim was accepted")]
fn negative_control_charges_cancelled_execution() {
    trace(2, &[], 0, Fault::IgnoreCancellation);
}
