use super::*;

fn orderings(scheduler: &Scheduler, actions: &mut [SchedulerEvent]) -> u64 {
    if actions.is_empty() {
        return 1;
    }
    let used = |state: &Scheduler| {
        state
            .model_claims
            .values()
            .map(TaskModelClaims::used)
            .sum::<u64>()
    };
    let mut leaves = 0;
    for index in 0..actions.len() {
        actions.swap(0, index);
        let event = &actions[0];
        let mut next = scheduler.clone();
        let accepted = next.apply(event, &mut None).is_ok();
        if !accepted {
            assert_eq!(&next, scheduler, "rejected event mutated state: {event:?}");
        }
        if matches!(event, SchedulerEvent::LeaseReleased { .. }) {
            assert!(!accepted, "stale fence released ownership: {event:?}");
        }
        if let SchedulerEvent::ModelDispatchClaimed {
            operation_id,
            attempt_id,
            step,
            request_digest,
            ..
        } = event
        {
            if scheduler
                .operation(*operation_id)
                .is_some_and(|op| op.cancellation_requested)
            {
                assert!(!accepted, "cancelled owner charged work: {event:?}");
            }
            let retained = scheduler
                .model_claims
                .get(operation_id)
                .and_then(|claims| claims.attempts.get(attempt_id))
                .and_then(|steps| steps.get(step));
            if retained.is_some_and(|digest| digest != request_digest) {
                assert!(!accepted, "changed request accepted as retry: {event:?}");
            }
            if accepted {
                assert_eq!(
                    used(&next),
                    used(scheduler) + u64::from(retained.is_none()),
                    "exact retry changed accounting: {event:?}"
                );
            }
        }
        assert!(used(&next) <= 2, "shared budget exceeded: {event:?}");
        assert!(
            claims_preserved(scheduler, &next),
            "retained claims changed: {event:?}"
        );
        for op in [id(2), id(3)] {
            assert_eq!(
                next.operation(op).and_then(|op| op.reservation.as_ref()),
                scheduler
                    .operation(op)
                    .and_then(|op| op.reservation.as_ref()),
                "uncertainty released ownership: {event:?}"
            );
            if next
                .operation(id(1))
                .is_some_and(|root| root.cancellation_requested)
            {
                assert!(next.operation(op).expect("child").cancellation_requested);
            }
        }
        leaves += orderings(&next, &mut actions[1..]);
        actions.swap(0, index);
    }
    leaves
}

#[test]
fn all_model_claim_cancellation_and_uncertainty_orders_preserve_accounting() -> Result<()> {
    let mut scheduler = Scheduler::new();
    let root = spec(id(1), Orchestration::Join)?;
    scheduler.apply(&scheduler.declare(root.clone())?, &mut None)?;
    scheduler.apply(
        &SchedulerEvent::SessionConfigured {
            operation_id: id(1),
            limits: SessionLimits {
                active_tasks: 2,
                total_tasks: 3,
                depth: 1,
                model_steps: 2,
            },
        },
        &mut None,
    )?;
    for (op, lease) in [(2, 0), (3, 1)] {
        let mut child = child(op, 1, &op.to_string())?;
        child.owner = root.owner.clone();
        scheduler.apply(&scheduler.declare(child)?, &mut None)?;
        start(&mut scheduler, id(op), reservation(lease, 0))?;
    }
    let mut actions = [
        claim(2, 0, 0, 0, 1),
        claim(2, 0, 0, 0, 1),
        claim(2, 0, 0, 0, 2),
        claim(3, 1, 0, 0, 1),
        claim(2, 0, 1, 0, 1),
        SchedulerEvent::CancellationRequested {
            operation_id: id(1),
            recursive: true,
        },
        SchedulerEvent::Completed {
            operation_id: id(2),
            outcome: Outcome::Indeterminate {
                operation_id: id(2),
            },
            fence: Some(fence(0)),
            execution_duration_ns: None,
        },
        SchedulerEvent::LeaseReleased {
            operation_id: id(2),
            fence: fence(1),
        },
    ];
    let expected = (1..=actions.len() as u64).product::<u64>();
    assert_eq!(orderings(&scheduler, &mut actions), expected);
    Ok(())
}
