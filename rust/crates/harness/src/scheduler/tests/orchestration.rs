use super::*;

#[test]
fn join_slot_order_survives_reversed_completion_and_rejects_swapped_digest() -> Result<()> {
    let mut scheduler = two_slot_parent(Orchestration::Join)?;
    let a = result_ref(b"11")?;
    let b = result_ref(b"22")?;
    for (op, value) in [(3, b.clone()), (2, a.clone())] {
        complete(
            &mut scheduler,
            id(op),
            Outcome::Succeeded(value),
            Some(fence(0)),
        )?;
        if op == 3 {
            assert_eq!(
                scheduler.orchestration(id(1), &mut None)?,
                OrchestrationDecision::Wait
            );
        }
    }
    let values = vec![("a".into(), a), ("b".into(), b)];
    assert_eq!(scheduler.completion_order, vec![id(3), id(2)]);
    assert_eq!(
        scheduler.orchestration(id(1), &mut None)?,
        OrchestrationDecision::Assemble {
            assembly: AssemblyKind::Join,
            values: values.clone(),
            cancel: Vec::new()
        }
    );
    let digest = assembly_invocation_digest(AssemblyKind::Join, &values)?;
    let swapped =
        assembly_invocation_digest(AssemblyKind::Join, &[values[1].clone(), values[0].clone()])?;
    assert_ne!(digest, swapped);
    let before = scheduler.clone();
    let mut publication = SchedulerEvent::Orchestrated {
        operation_id: id(1),
        expected_revision: scheduler
            .operation(id(1))
            .ok_or_else(|| Error::NotFound("parent".into()))?
            .revision,
        outcome: Outcome::Succeeded(result_ref(b"[11,22]")?),
        cancel: Vec::new(),
        reducer: None,
        reduction_digest: Some(swapped),
    };
    assert!(matches!(
        scheduler.apply(&publication.clone(), &mut None),
        Err(Error::Conflict(_))
    ));
    assert_eq!(scheduler, before);
    if let SchedulerEvent::Orchestrated {
        reduction_digest, ..
    } = &mut publication
    {
        *reduction_digest = Some(digest);
    }
    scheduler.apply(&publication, &mut None)?;
    let parent = scheduler
        .operation(id(1))
        .ok_or_else(|| Error::NotFound("parent".into()))?;
    assert_eq!(parent.phase, OperationPhase::Terminal);
    assert_eq!(
        parent.outcome,
        Some(Outcome::Succeeded(result_ref(b"[11,22]")?))
    );
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
        scheduler.operation(id(3)).map(|child| child.phase),
        Some(OperationPhase::Running)
    );
    assert_eq!(
        scheduler.orchestration(id(1), &mut None)?,
        OrchestrationDecision::Wait
    );
    let failure = Outcome::Failed {
        message: "quorum is no longer reachable".into(),
    };
    let revision = scheduler
        .operation(id(1))
        .ok_or_else(|| Error::NotFound("parent".into()))?
        .revision;
    let publication = |expected_revision| SchedulerEvent::Orchestrated {
        operation_id: id(1),
        expected_revision,
        outcome: failure.clone(),
        cancel: Vec::new(),
        reducer: None,
        reduction_digest: None,
    };
    let before = scheduler.clone();
    assert!(matches!(
        scheduler.apply(&publication(revision), &mut None),
        Err(Error::Conflict(_))
    ));
    assert_eq!(scheduler, before);
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
            cancel: Vec::new()
        }
    );
    assert_eq!(
        scheduler.operation(id(1)).map(|parent| parent.revision),
        Some(revision)
    );
    let before = scheduler.clone();
    assert!(matches!(
        scheduler.apply(&publication(revision - 1), &mut None),
        Err(Error::Conflict(_))
    ));
    assert_eq!(scheduler, before);
    scheduler.apply(&publication(revision), &mut None)?;
    let parent = scheduler
        .operation(id(1))
        .ok_or_else(|| Error::NotFound("parent".into()))?;
    assert_eq!(parent.phase, OperationPhase::Terminal);
    assert_eq!(parent.outcome, Some(failure.clone()));
    assert_eq!(parent.revision, revision + 1);
    assert!(matches!(
        scheduler.apply(&publication(revision), &mut None),
        Err(Error::Conflict(_))
    ));
    Ok(())
}

#[test]
fn race_uses_first_observed_success_and_cancels_losers() -> Result<()> {
    let mut scheduler = two_slot_parent(Orchestration::Race)?;
    complete(
        &mut scheduler,
        id(3),
        Outcome::Succeeded(result_ref(b"\"winner\"")?),
        Some(fence(0)),
    )?;
    assert_eq!(
        scheduler.orchestration(id(1), &mut None)?,
        OrchestrationDecision::Complete {
            outcome: Outcome::Succeeded(result_ref(b"\"winner\"")?),
            cancel: vec![id(2)],
        }
    );
    let expected_revision = scheduler
        .operation(id(1))
        .ok_or_else(|| Error::NotFound("parent".into()))?
        .revision;
    let mut suspended_loser = scheduler.clone();
    suspended_loser.apply(
        &SchedulerEvent::WorkflowSuspended {
            operation_id: id(2),
            fence: fence(0),
            workflow_revision: 1,
            waiting_command: None,
        },
        &mut None,
    )?;
    suspended_loser.apply(
        &SchedulerEvent::Orchestrated {
            operation_id: id(1),
            expected_revision,
            outcome: Outcome::Succeeded(result_ref(b"\"winner\"")?),
            cancel: vec![id(2)],
            reducer: None,
            reduction_digest: None,
        },
        &mut None,
    )?;
    assert!(
        suspended_loser
            .operation(id(2))
            .is_some_and(|state| state.phase == OperationPhase::Terminal
                && state.outcome == Some(Outcome::Cancelled))
    );
    scheduler.apply(
        &SchedulerEvent::Orchestrated {
            operation_id: id(1),
            expected_revision,
            outcome: Outcome::Succeeded(result_ref(b"\"winner\"")?),
            cancel: vec![id(2)],
            reducer: None,
            reduction_digest: None,
        },
        &mut None,
    )?;
    assert_eq!(
        scheduler.operation(id(1)).map(|state| state.phase),
        Some(OperationPhase::Terminal)
    );
    assert_eq!(
        scheduler
            .operation(id(2))
            .and_then(|state| state.outcome.as_ref()),
        None
    );
    assert!(
        scheduler
            .operation(id(2))
            .is_some_and(|state| state.cancellation_requested)
    );
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
            id(11),
            Orchestration::Reduce {
                reducer: reducer.clone(),
            },
        )?)?,
        &mut None,
    )?;
    let parent_reservation = Reservation {
        id: "reduce-parent".into(),
        placement: "worker".into(),
        admitted: ResourceRequest::default(),
    };
    let parent_fence = LeaseFence::from(&parent_reservation);
    start(&mut scheduler, id(11), parent_reservation)?;
    scheduler.apply(
        &SchedulerEvent::WaitingForChildren {
            operation_id: id(11),
            fence: parent_fence,
        },
        &mut None,
    )?;
    let mut child = spec(id(12), Orchestration::Leaf)?;
    child.parent = Some(ParentLink {
        operation_id: id(11),
        slot: "value".into(),
    });
    scheduler.apply(&scheduler.declare(child)?, &mut None)?;
    let child_reservation = Reservation {
        id: "reduce-child".into(),
        placement: "worker".into(),
        admitted: ResourceRequest::default(),
    };
    let child_fence = LeaseFence::from(&child_reservation);
    start(&mut scheduler, id(12), child_reservation)?;
    complete(
        &mut scheduler,
        id(12),
        Outcome::Succeeded(result_ref(b"2")?),
        Some(child_fence),
    )?;
    let OrchestrationDecision::Reduce { values, .. } =
        scheduler.orchestration(id(11), &mut None)?
    else {
        return Err(Error::Conflict("reducer not ready".into()));
    };
    let expected_revision = scheduler
        .operation(id(11))
        .ok_or_else(|| Error::NotFound("parent".into()))?
        .revision;
    assert!(
        scheduler
            .apply(
                &SchedulerEvent::Orchestrated {
                    operation_id: id(11),
                    expected_revision,
                    outcome: Outcome::Succeeded(result_ref(b"2")?),
                    cancel: Vec::new(),
                    reducer: Some(reducer.clone()),
                    reduction_digest: Some([0; 32]),
                },
                &mut None
            )
            .is_err()
    );
    scheduler.apply(
        &SchedulerEvent::Orchestrated {
            operation_id: id(11),
            expected_revision,
            outcome: Outcome::Succeeded(result_ref(b"2")?),
            cancel: Vec::new(),
            reducer: Some(reducer.clone()),
            reduction_digest: Some(reduction_invocation_digest(&reducer, &values)?),
        },
        &mut None,
    )?;
    Ok(())
}
