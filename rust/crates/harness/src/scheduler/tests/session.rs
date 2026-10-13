use super::*;

#[test]
fn workflow_suspension_releases_shared_capacity_and_fences_wakes() -> Result<()> {
    let root = spec(id(91), Orchestration::Leaf)?;
    let mut child = child(92, 91, "child")?;
    child.owner = root.owner.clone();
    let mut scheduler = session(
        root,
        SessionLimits {
            active_tasks: 1,
            total_tasks: 2,
            depth: 1,
            model_steps: 1,
        },
    )?;
    scheduler.apply(&scheduler.declare(child)?, &mut None)?;
    let input = result_ref(b"7")?;
    let suspend = |lease, workflow_revision| SchedulerEvent::WorkflowSuspended {
        operation_id: id(91),
        fence: fence(lease),
        workflow_revision,
        waiting_command: None,
    };
    let resume = |workflow_revision| SchedulerEvent::WorkflowResumed {
        operation_id: id(91),
        workflow_revision,
        input: input.clone(),
    };
    let ready = |scheduler: &Scheduler, operation| {
        scheduler
            .ready(&ResourceRequest::default(), &BTreeMap::new(), None)
            .any(|candidate| candidate == id(operation))
    };
    start(&mut scheduler, id(91), reservation(1, 0))?;
    assert!(!ready(&scheduler, 92));
    for event in [suspend(9, 1), suspend(1, 0)] {
        rejected(&mut scheduler, &event);
    }
    scheduler.apply(&suspend(1, 1), &mut None)?;
    let state = scheduler.operation(id(91)).unwrap();
    assert_eq!(state.phase, OperationPhase::Suspended);
    assert!(state.reservation.is_none());
    assert_eq!(
        state.workflow,
        Some(WorkflowSuspension {
            revision: 1,
            waiting_command: None,
            input: None,
        })
    );
    assert!(ready(&scheduler, 92));
    assert!(!ready(&scheduler, 91));
    rejected(&mut scheduler, &resume(2));
    scheduler.apply(&resume(1), &mut None)?;
    rejected(&mut scheduler, &resume(1));
    start(&mut scheduler, id(91), reservation(2, 0))?;
    rejected(&mut scheduler, &suspend(1, 2));
    for revision in [1, 2] {
        let mut polled = scheduler.clone();
        polled.apply(&suspend(2, revision), &mut None)?;
        let state = polled.operation(id(91)).unwrap().workflow.as_ref().unwrap();
        assert!(state.input.is_none());
        assert_eq!(state.revision, revision);
    }
    scheduler.apply(&suspend(2, 2), &mut None)?;
    scheduler.apply(
        &SchedulerEvent::CancellationRequested {
            operation_id: id(91),
            recursive: true,
        },
        &mut None,
    )?;
    assert_eq!(
        scheduler.operation(id(91)).unwrap().outcome,
        Some(Outcome::Cancelled)
    );
    rejected(&mut scheduler, &resume(2));
    Ok(())
}

#[test]
fn measured_completions_require_active_terminal_fences() -> Result<()> {
    let mut scheduler = Scheduler::new();
    scheduler.apply(
        &scheduler.declare(spec(id(1), Orchestration::Leaf)?)?,
        &mut None,
    )?;
    start(&mut scheduler, id(1), reservation(1, 0))?;
    let completion = |outcome, fence| SchedulerEvent::Completed {
        operation_id: id(1),
        outcome,
        fence,
        execution_duration_ns: Some(1_234_567_890),
    };
    for event in [
        completion(Outcome::Succeeded(result_ref(b"null")?), None),
        completion(
            Outcome::Indeterminate {
                operation_id: id(1),
            },
            Some(fence(1)),
        ),
    ] {
        rejected(&mut scheduler, &event);
    }
    let event = completion(Outcome::Succeeded(result_ref(b"null")?), Some(fence(1)));
    scheduler.apply(&event, &mut None)?;
    assert_eq!(
        scheduler.operation(id(1)).unwrap().phase,
        OperationPhase::Terminal
    );
    rejected(&mut scheduler, &event);
    Ok(())
}

#[test]
fn session_ceilings_and_model_claims_are_shared() -> Result<()> {
    let mut scheduler = session(
        spec(id(40), Orchestration::Join)?,
        SessionLimits {
            active_tasks: 1,
            total_tasks: 3,
            depth: 1,
            model_steps: 2,
        },
    )?;
    for op in [41, 42] {
        scheduler.apply(
            &scheduler.declare(child(op, 40, &op.to_string())?)?,
            &mut None,
        )?;
        if op == 41 {
            assert!(scheduler.declare(child(43, 41, "too-deep")?).is_err());
        }
    }
    assert!(scheduler.declare(child(43, 40, "excess")?).is_err());
    let mut root_reservation = reservation(1, 0);
    root_reservation.placement = "one".into();
    let root_fence = LeaseFence::from(&root_reservation);
    let mut child_reservation = reservation(2, 0);
    child_reservation.placement = "two".into();
    let child_fence = LeaseFence::from(&child_reservation);
    start(&mut scheduler, id(40), root_reservation)?;
    let ready = |scheduler: &Scheduler| {
        scheduler
            .ready(&ResourceRequest::default(), &BTreeMap::new(), None)
            .next()
            .is_some()
    };
    assert!(!ready(&scheduler));
    rejected(
        &mut scheduler,
        &SchedulerEvent::Admitted {
            operation_id: id(41),
            reservation: child_reservation.clone(),
        },
    );
    let claim =
        |operation_id, attempt_id, fence, request_digest| SchedulerEvent::ModelDispatchClaimed {
            operation_id,
            attempt_id,
            step: 0,
            request_digest,
            fence,
            ceiling: 2,
        };
    let event = claim(id(40), id(40), root_fence.clone(), [1; 32]);
    scheduler.apply(&event, &mut None)?;
    scheduler.apply(&event, &mut None)?;
    assert_eq!(scheduler.model_claims[&id(40)].used(), 1);
    rejected(
        &mut scheduler,
        &claim(id(40), id(40), root_fence.clone(), [2; 32]),
    );
    scheduler.apply(
        &SchedulerEvent::WaitingForChildren {
            operation_id: id(40),
            fence: root_fence,
        },
        &mut None,
    )?;
    start(&mut scheduler, id(41), child_reservation)?;
    scheduler.apply(
        &claim(id(41), id(41), child_fence.clone(), [2; 32]),
        &mut None,
    )?;
    rejected(
        &mut scheduler,
        &claim(id(41), id(42), child_fence.clone(), [3; 32]),
    );
    complete(
        &mut scheduler,
        id(41),
        Outcome::Indeterminate {
            operation_id: id(41),
        },
        Some(child_fence),
    )?;
    assert!(!ready(&scheduler));
    assert_eq!(
        scheduler
            .model_claims
            .values()
            .map(TaskModelClaims::used)
            .sum::<u64>(),
        2
    );
    Ok(())
}
