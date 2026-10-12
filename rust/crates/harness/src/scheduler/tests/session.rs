use super::*;

#[test]
fn workflow_suspension_releases_shared_capacity_and_fences_wakes() -> Result<()> {
    let mut scheduler = Scheduler::new();
    let root = id(91);
    let child_id = id(92);
    let declared = spec(root, Orchestration::Leaf)?;
    let mut child = spec(child_id, Orchestration::Leaf)?;
    child.owner = declared.owner.clone();
    child.parent = Some(ParentLink {
        operation_id: root,
        slot: "child".into(),
    });
    scheduler.apply(&scheduler.declare(declared)?, &mut None)?;
    scheduler.apply(
        &SchedulerEvent::SessionConfigured {
            operation_id: root,
            limits: SessionLimits {
                active_tasks: 1,
                total_tasks: 2,
                depth: 1,
                model_steps: 1,
            },
        },
        &mut None,
    )?;
    scheduler.apply(&scheduler.declare(child)?, &mut None)?;
    let first = fence(1);
    let second = fence(2);
    let input = result_ref(b"7")?;
    let suspend = |fence, workflow_revision| SchedulerEvent::WorkflowSuspended {
        operation_id: root,
        fence,
        workflow_revision,
        waiting_command: None,
    };
    let resume = |workflow_revision| SchedulerEvent::WorkflowResumed {
        operation_id: root,
        workflow_revision,
        input: input.clone(),
    };
    let ready = |scheduler: &Scheduler, operation| {
        scheduler
            .ready(&ResourceRequest::default(), &BTreeMap::new(), None)
            .any(|id| id == operation)
    };
    start(&mut scheduler, root, reservation(1, 0))?;
    assert!(!ready(&scheduler, child_id));
    for event in [suspend(fence(9), 1), suspend(first.clone(), 0)] {
        let before = scheduler.clone();
        assert!(scheduler.apply(&event, &mut None).is_err());
        assert_eq!(scheduler, before);
    }
    scheduler.apply(&suspend(first.clone(), 1), &mut None)?;
    let state = scheduler.operation(root).unwrap();
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
    assert!(ready(&scheduler, child_id));
    assert!(!ready(&scheduler, root));
    assert!(scheduler.apply(&resume(2), &mut None).is_err());
    scheduler.apply(&resume(1), &mut None)?;
    assert!(scheduler.apply(&resume(1), &mut None).is_err());
    start(&mut scheduler, root, reservation(2, 0))?;
    assert!(scheduler.apply(&suspend(first, 2), &mut None).is_err());
    let mut polled = scheduler.clone();
    polled.apply(&suspend(second.clone(), 1), &mut None)?;
    assert!(
        polled
            .operation(root)
            .unwrap()
            .workflow
            .as_ref()
            .unwrap()
            .input
            .is_none()
    );
    scheduler.apply(&suspend(second, 2), &mut None)?;
    let state = scheduler
        .operation(root)
        .unwrap()
        .workflow
        .as_ref()
        .unwrap();
    assert!(state.input.is_none());
    assert_eq!(state.revision, 2);
    scheduler.apply(
        &SchedulerEvent::CancellationRequested {
            operation_id: root,
            recursive: true,
        },
        &mut None,
    )?;
    assert_eq!(
        scheduler.operation(root).unwrap().outcome,
        Some(Outcome::Cancelled)
    );
    assert!(scheduler.apply(&resume(2), &mut None).is_err());
    Ok(())
}

#[test]
fn measured_completions_require_active_terminal_fences() -> Result<()> {
    let mut scheduler = Scheduler::new();
    let operation_id = id(1);
    scheduler.apply(
        &scheduler.declare(spec(operation_id, Orchestration::Leaf)?)?,
        &mut None,
    )?;
    start(&mut scheduler, operation_id, reservation(1, 0))?;
    let completion = |outcome, fence| SchedulerEvent::Completed {
        operation_id,
        outcome,
        fence,
        execution_duration_ns: Some(1_234_567_890),
    };
    for event in [
        completion(Outcome::Succeeded(result_ref(b"null")?), None),
        completion(Outcome::Indeterminate { operation_id }, Some(fence(1))),
    ] {
        let before = scheduler.clone();
        assert!(scheduler.apply(&event, &mut None).is_err());
        assert_eq!(scheduler, before);
    }
    let event = completion(Outcome::Succeeded(result_ref(b"null")?), Some(fence(1)));
    scheduler.apply(&event, &mut None)?;
    assert_eq!(
        scheduler.operation(operation_id).unwrap().phase,
        OperationPhase::Terminal
    );
    assert!(scheduler.apply(&event, &mut None).is_err());
    Ok(())
}

#[test]
fn session_ceilings_and_model_claims_are_shared() -> Result<()> {
    let mut scheduler = Scheduler::new();
    let root = id(40);
    scheduler.apply(
        &scheduler.declare(spec(root, Orchestration::Join)?)?,
        &mut None,
    )?;
    scheduler.apply(
        &SchedulerEvent::SessionConfigured {
            operation_id: root,
            limits: SessionLimits {
                active_tasks: 1,
                total_tasks: 3,
                depth: 1,
                model_steps: 2,
            },
        },
        &mut None,
    )?;
    let child = |operation, parent, slot: &str| -> Result<OperationSpec> {
        let mut spec = spec(id(operation), Orchestration::Leaf)?;
        spec.parent = Some(ParentLink {
            operation_id: parent,
            slot: slot.into(),
        });
        Ok(spec)
    };
    for index in [41, 42] {
        scheduler.apply(
            &scheduler.declare(child(index, root, &index.to_string())?)?,
            &mut None,
        )?;
        if index == 41 {
            assert!(scheduler.declare(child(43, id(41), "too-deep")?).is_err());
        }
    }
    assert!(scheduler.declare(child(43, root, "excess")?).is_err());
    let mut root_reservation = reservation(1, 0);
    root_reservation.placement = "one".into();
    let root_fence = LeaseFence::from(&root_reservation);
    let mut child_reservation = reservation(2, 0);
    child_reservation.placement = "two".into();
    let child_fence = LeaseFence::from(&child_reservation);
    start(&mut scheduler, root, root_reservation)?;
    assert!(
        scheduler
            .ready(&ResourceRequest::default(), &BTreeMap::new(), None)
            .next()
            .is_none()
    );
    assert!(
        scheduler
            .apply(
                &SchedulerEvent::Admitted {
                    operation_id: id(41),
                    reservation: child_reservation.clone(),
                },
                &mut None
            )
            .is_err()
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
    let event = claim(root, root, root_fence.clone(), [1; 32]);
    scheduler.apply(&event, &mut None)?;
    scheduler.apply(&event, &mut None)?;
    assert_eq!(scheduler.model_claims[&root].used(), 1);
    let before = scheduler.clone();
    assert!(
        scheduler
            .apply(&claim(root, root, root_fence.clone(), [2; 32]), &mut None)
            .is_err()
    );
    assert_eq!(scheduler, before);
    scheduler.apply(
        &SchedulerEvent::WaitingForChildren {
            operation_id: root,
            fence: root_fence,
        },
        &mut None,
    )?;
    start(&mut scheduler, id(41), child_reservation)?;
    scheduler.apply(
        &claim(id(41), id(41), child_fence.clone(), [2; 32]),
        &mut None,
    )?;
    let before = scheduler.clone();
    assert!(
        scheduler
            .apply(
                &claim(id(41), id(42), child_fence.clone(), [3; 32]),
                &mut None
            )
            .is_err()
    );
    assert_eq!(scheduler, before);
    complete(
        &mut scheduler,
        id(41),
        Outcome::Indeterminate {
            operation_id: id(41),
        },
        Some(child_fence),
    )?;
    assert!(
        scheduler
            .ready(&ResourceRequest::default(), &BTreeMap::new(), None)
            .next()
            .is_none()
    );
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
