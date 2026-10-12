use super::*;

fn publication(scheduler: &Scheduler, outcome: Outcome<FileRef>) -> SchedulerEvent {
    SchedulerEvent::Orchestrated {
        operation_id: id(1),
        expected_revision: scheduler.operation(id(1)).unwrap().revision,
        outcome,
        cancel: Vec::new(),
        reducer: None,
        reduction_digest: None,
    }
}

fn rejected(scheduler: &mut Scheduler, event: &SchedulerEvent) {
    let before = scheduler.clone();
    assert!(scheduler.apply(event, &mut None).is_err());
    assert_eq!(*scheduler, before);
}

#[test]
fn join_slot_order_survives_reversed_completion_and_rejects_swapped_digest() -> Result<()> {
    let mut scheduler = two_slot_parent(Orchestration::Join)?;
    let values = vec![
        ("a".into(), result_ref(b"11")?),
        ("b".into(), result_ref(b"22")?),
    ];
    for (op, value) in [(3, &values[1].1), (2, &values[0].1)] {
        complete(
            &mut scheduler,
            id(op),
            Outcome::Succeeded(value.clone()),
            Some(fence(0)),
        )?;
        if op == 3 {
            assert_eq!(
                scheduler.orchestration(id(1), &mut None)?,
                OrchestrationDecision::Wait
            );
        }
    }
    assert_eq!(
        scheduler.orchestration(id(1), &mut None)?,
        OrchestrationDecision::Assemble {
            assembly: AssemblyKind::Join,
            values: values.clone(),
            cancel: Vec::new(),
        }
    );
    let digest = assembly_invocation_digest(AssemblyKind::Join, &values)?;
    let swapped =
        assembly_invocation_digest(AssemblyKind::Join, &[values[1].clone(), values[0].clone()])?;
    let outcome = Outcome::Succeeded(result_ref(b"[11,22]")?);
    let mut event = publication(&scheduler, outcome.clone());
    if let SchedulerEvent::Orchestrated {
        reduction_digest, ..
    } = &mut event
    {
        *reduction_digest = Some(swapped);
        rejected(&mut scheduler, &event);
    }
    if let SchedulerEvent::Orchestrated {
        reduction_digest, ..
    } = &mut event
    {
        *reduction_digest = Some(digest);
    }
    scheduler.apply(&event, &mut None)?;
    assert_eq!(scheduler.operation(id(1)).unwrap().outcome, Some(outcome));
    Ok(())
}

#[test]
fn quorum_waits_for_running_child_then_fails_at_exact_parent_revision() -> Result<()> {
    let mut scheduler = two_slot_parent(Orchestration::Quorum { required: 2 })?;
    complete(
        &mut scheduler,
        id(2),
        Outcome::Succeeded(result_ref(b"11")?),
        Some(fence(0)),
    )?;
    assert_eq!(
        scheduler.orchestration(id(1), &mut None)?,
        OrchestrationDecision::Wait
    );
    let failure = Outcome::Failed {
        message: "quorum is no longer reachable".into(),
    };
    let event = publication(&scheduler, failure.clone());
    rejected(&mut scheduler, &event);
    complete(
        &mut scheduler,
        id(3),
        Outcome::Failed {
            message: "child failed".into(),
        },
        Some(fence(0)),
    )?;
    assert_eq!(
        scheduler.orchestration(id(1), &mut None)?,
        OrchestrationDecision::Complete {
            outcome: failure.clone(),
            cancel: Vec::new(),
        }
    );
    let mut stale = event.clone();
    if let SchedulerEvent::Orchestrated {
        expected_revision, ..
    } = &mut stale
    {
        *expected_revision -= 1;
    }
    rejected(&mut scheduler, &stale);
    scheduler.apply(&event, &mut None)?;
    assert_eq!(scheduler.operation(id(1)).unwrap().outcome, Some(failure));
    rejected(&mut scheduler, &event);
    Ok(())
}

#[test]
fn race_uses_first_observed_success_and_cancels_running_or_suspended_losers() -> Result<()> {
    let mut scheduler = two_slot_parent(Orchestration::Race)?;
    let winner = Outcome::Succeeded(result_ref(b"\"winner\"")?);
    complete(&mut scheduler, id(3), winner.clone(), Some(fence(0)))?;
    assert_eq!(
        scheduler.orchestration(id(1), &mut None)?,
        OrchestrationDecision::Complete {
            outcome: winner.clone(),
            cancel: vec![id(2)],
        }
    );
    for suspended in [false, true] {
        let mut scheduler = scheduler.clone();
        if suspended {
            scheduler.apply(
                &SchedulerEvent::WorkflowSuspended {
                    operation_id: id(2),
                    fence: fence(0),
                    workflow_revision: 1,
                    waiting_command: None,
                },
                &mut None,
            )?;
        }
        let mut event = publication(&scheduler, winner.clone());
        if let SchedulerEvent::Orchestrated { cancel, .. } = &mut event {
            cancel.push(id(2));
        }
        scheduler.apply(&event, &mut None)?;
        assert_eq!(
            scheduler.operation(id(1)).unwrap().outcome,
            Some(winner.clone())
        );
        let loser = scheduler.operation(id(2)).unwrap();
        if !suspended {
            assert!(loser.cancellation_requested);
        }
        assert_eq!(loser.outcome, suspended.then_some(Outcome::Cancelled));
    }
    Ok(())
}

#[test]
fn reduction_is_bound_to_the_exact_contract_and_inputs() -> Result<()> {
    let reducer = EntrypointRef {
        name: "example.reducer".into(),
        version: "1".into(),
        digest: [4; 32],
        result_schema: serde_json::json!({"type": "number"}),
    };
    let mut scheduler = Scheduler::new();
    scheduler.apply(
        &scheduler.declare(spec(
            id(1),
            Orchestration::Reduce {
                reducer: reducer.clone(),
            },
        )?)?,
        &mut None,
    )?;
    start(&mut scheduler, id(1), reservation(0, 0))?;
    scheduler.apply(
        &SchedulerEvent::WaitingForChildren {
            operation_id: id(1),
            fence: fence(0),
        },
        &mut None,
    )?;
    scheduler.apply(&scheduler.declare(child(2, 1, "value")?)?, &mut None)?;
    start(&mut scheduler, id(2), reservation(1, 0))?;
    complete(
        &mut scheduler,
        id(2),
        Outcome::Succeeded(result_ref(b"2")?),
        Some(fence(1)),
    )?;
    let OrchestrationDecision::Reduce { values, .. } = scheduler.orchestration(id(1), &mut None)?
    else {
        panic!("reducer must be ready");
    };
    let mut event = publication(&scheduler, Outcome::Succeeded(result_ref(b"2")?));
    if let SchedulerEvent::Orchestrated {
        reducer: selected,
        reduction_digest,
        ..
    } = &mut event
    {
        *selected = Some(reducer.clone());
        *reduction_digest = Some([0; 32]);
    }
    rejected(&mut scheduler, &event);
    if let SchedulerEvent::Orchestrated {
        reduction_digest, ..
    } = &mut event
    {
        *reduction_digest = Some(reduction_invocation_digest(&reducer, &values)?);
    }
    scheduler.apply(&event, &mut None)?;
    Ok(())
}
