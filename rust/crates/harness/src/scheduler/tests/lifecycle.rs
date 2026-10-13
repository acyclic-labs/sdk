use super::*;

#[test]
fn exhausted_child_revision_rejects_cancellation_atomically() -> Result<()> {
    let mut scheduler = Scheduler::new();
    let parent = spec(id(1), Orchestration::Join)?;
    let mut child = child(2, 1, "child")?;
    child.owner = parent.owner.clone();
    scheduler.apply(&scheduler.declare(parent)?, &mut None)?;
    scheduler.apply(&scheduler.declare(child)?, &mut None)?;
    scheduler.operations.get_mut(&id(2)).unwrap().revision = u64::MAX;
    rejected(
        &mut scheduler,
        &SchedulerEvent::CancellationRequested {
            operation_id: id(1),
            recursive: true,
        },
    );
    Ok(())
}

#[test]
fn cancellation_retains_owned_capacity_until_fenced_acknowledgement() -> Result<()> {
    for recursive in [false, true] {
        for partial in [false, true] {
            let mut scheduler = Scheduler::new();
            let mut parent = spec(id(30), Orchestration::Join)?;
            parent.resources = cpu(2);
            let owner = parent.owner.clone();
            scheduler.apply(&scheduler.declare(parent)?, &mut None)?;
            let target = if recursive {
                let mut child = child(31, 30, "child")?;
                child.owner = owner;
                child.resources = cpu(2);
                scheduler.apply(&scheduler.declare(child)?, &mut None)?;
                id(31)
            } else {
                id(30)
            };
            let reservation = reservation(1, if partial { 1 } else { 2 });
            let held = LeaseFence::from(&reservation);
            let admission = if partial {
                SchedulerEvent::PartiallyAdmitted {
                    operation_id: target,
                    reservation: reservation.clone(),
                }
            } else {
                SchedulerEvent::Admitted {
                    operation_id: target,
                    reservation: reservation.clone(),
                }
            };
            scheduler.apply(&admission, &mut None)?;
            scheduler.apply(
                &SchedulerEvent::CancellationRequested {
                    operation_id: id(30),
                    recursive,
                },
                &mut None,
            )?;
            let state = scheduler.operation(target).unwrap();
            assert_eq!(state.reservation.as_ref(), Some(&reservation));
            assert!(state.cancellation_requested);
            for event in [
                SchedulerEvent::Rejected {
                    operation_id: target,
                    reason: "cancel".into(),
                },
                SchedulerEvent::Started {
                    operation_id: target,
                    fence: held.clone(),
                },
            ] {
                rejected(&mut scheduler, &event);
            }
            assert!(complete(&mut scheduler, target, Outcome::Cancelled, None).is_err());
            let acknowledgement = if partial {
                SchedulerEvent::LeaseReleased {
                    operation_id: target,
                    fence: held,
                }
            } else {
                SchedulerEvent::Completed {
                    operation_id: target,
                    outcome: Outcome::Cancelled,
                    fence: Some(held),
                    execution_duration_ns: None,
                }
            };
            scheduler.apply(&acknowledgement, &mut None)?;
            let state = scheduler.operation(target).unwrap();
            assert_eq!(state.outcome, Some(Outcome::Cancelled));
            assert!(state.reservation.is_none());
        }
    }
    Ok(())
}

#[test]
fn single_pull_selection_preserves_partial_slots_and_advertised_resource_keys() -> Result<()> {
    let mut scheduler = session(
        spec(id(1), Orchestration::Join)?,
        SessionLimits {
            active_tasks: 1,
            total_tasks: 2,
            depth: 1,
            model_steps: 1,
        },
    )?;
    let mut child = child(2, 1, "child")?;
    child.resources = cpu(2);
    let labels = BTreeMap::from([("zone".into(), "a".into())]);
    child.placement = labels.clone();
    scheduler.apply(&scheduler.declare(child)?, &mut None)?;
    start(&mut scheduler, id(1), reservation(0, 0))?;
    scheduler.apply(
        &SchedulerEvent::WaitingForChildren {
            operation_id: id(1),
            fence: fence(0),
        },
        &mut None,
    )?;
    scheduler.apply(
        &SchedulerEvent::PartiallyAdmitted {
            operation_id: id(2),
            reservation: reservation(1, 1),
        },
        &mut None,
    )?;
    let mut other = spec(id(3), Orchestration::Leaf)?;
    other.resources = cpu(1);
    scheduler.apply(&scheduler.declare(other)?, &mut None)?;
    for op in 60..140 {
        let mut declared = spec(id(op), Orchestration::Leaf)?;
        declared.resources = ResourceRequest(BTreeMap::from([(format!("retired-{op}"), 1)]));
        let admitted = declared.resources.clone();
        scheduler.apply(&scheduler.declare(declared)?, &mut None)?;
        scheduler.apply(
            &SchedulerEvent::Admitted {
                operation_id: id(op),
                reservation: Reservation {
                    id: format!("held-{op}"),
                    placement: "worker".into(),
                    admitted,
                },
            },
            &mut None,
        )?;
    }
    let available = scheduler.available_for("worker", &cpu(2), &mut None)?;
    assert_eq!(available, cpu(1));
    assert_eq!(
        scheduler.ready(&available, &labels, None).next(),
        Some(id(2))
    );
    assert_eq!(
        scheduler
            .ready(&available, &labels, None)
            .collect::<Vec<_>>(),
        vec![id(2), id(3)]
    );
    assert_eq!(
        scheduler.ready(&available, &BTreeMap::new(), None).next(),
        Some(id(3))
    );
    assert_eq!(scheduler.ready(&cpu(0), &labels, None).next(), None);
    Ok(())
}

#[test]
fn uncertain_execution_retains_capacity_and_rejects_stale_owners() -> Result<()> {
    let mut scheduler = Scheduler::new();
    let mut declared = spec(id(32), Orchestration::Leaf)?;
    declared.resources = cpu(1);
    scheduler.apply(&scheduler.declare(declared)?, &mut None)?;
    let reservation = reservation(1, 1);
    let held = LeaseFence::from(&reservation);
    start(&mut scheduler, id(32), reservation.clone())?;
    complete(
        &mut scheduler,
        id(32),
        Outcome::Indeterminate {
            operation_id: id(32),
        },
        Some(held.clone()),
    )?;
    assert_eq!(
        scheduler.operation(id(32)).unwrap().reservation.as_ref(),
        Some(&reservation)
    );
    assert_eq!(
        scheduler.available_for("worker", &cpu(1), &mut None)?,
        cpu(0)
    );
    let replayed: Scheduler = serde_json::from_value(
        serde_json::to_value(&scheduler).map_err(|error| Error::Invalid(error.to_string()))?,
    )
    .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(replayed, scheduler);
    let reconcile = |fence| SchedulerEvent::ReconciliationResumed {
        operation_id: id(32),
        fence,
    };
    let mut resumed = scheduler.clone();
    rejected(&mut resumed, &reconcile(fence(9)));
    resumed.apply(&reconcile(held.clone()), &mut None)?;
    assert_eq!(resumed.available_for("worker", &cpu(1), &mut None)?, cpu(0));
    assert!(
        resumed
            .ready(&cpu(1), &BTreeMap::new(), None)
            .all(|op| op != id(32))
    );
    assert_eq!(
        resumed.operation(id(32)).unwrap().reservation.as_ref(),
        Some(&reservation)
    );
    scheduler.apply(
        &SchedulerEvent::CancellationRequested {
            operation_id: id(32),
            recursive: true,
        },
        &mut None,
    )?;
    rejected(&mut scheduler, &reconcile(held.clone()));
    for candidate in [None, Some(fence(9))] {
        rejected(
            &mut scheduler,
            &SchedulerEvent::Completed {
                operation_id: id(32),
                outcome: Outcome::Cancelled,
                fence: candidate,
                execution_duration_ns: None,
            },
        );
    }
    rejected(
        &mut scheduler,
        &SchedulerEvent::LeaseReleased {
            operation_id: id(32),
            fence: held.clone(),
        },
    );
    complete(
        &mut scheduler,
        id(32),
        Outcome::Cancelled,
        Some(held.clone()),
    )?;
    assert_eq!(
        scheduler.available_for("worker", &cpu(1), &mut None)?,
        cpu(1)
    );
    assert!(complete(&mut scheduler, id(32), Outcome::Cancelled, Some(held)).is_err());
    Ok(())
}

#[test]
fn task_inbox_is_gapless_and_idempotent() -> Result<()> {
    let task_id = TaskId::from_bytes([4; 16]);
    let item = InboxItem {
        task_id,
        sequence: 1,
        message_id: "message-1".into(),
        payload: state_ref()?,
    };
    let mut inbox = TaskInbox::new(task_id);
    inbox.apply(item.clone())?;
    inbox.apply(item)?;
    assert_eq!(inbox.after(0, 10).len(), 1);
    Ok(())
}

#[test]
fn recursive_cancellation_stops_at_owner_boundaries() -> Result<()> {
    let mut scheduler = Scheduler::new();
    let mut parent = spec(id(20), Orchestration::Join)?;
    let authority = parent.owner.authority().clone();
    parent.owner = DurableOwner::Attached {
        authority: authority.clone(),
    };
    scheduler.apply(&scheduler.declare(parent)?, &mut None)?;
    for (op, parent, slot, owner) in [
        (
            21,
            20,
            "owned",
            DurableOwner::Detached {
                authority: authority.clone(),
            },
        ),
        (
            23,
            21,
            "owned-grandchild",
            DurableOwner::Attached { authority },
        ),
        (22, 20, "foreign", spec(id(22), Orchestration::Leaf)?.owner),
    ] {
        let mut child = child(op, parent, slot)?;
        child.owner = owner;
        scheduler.apply(&scheduler.declare(child)?, &mut None)?;
    }
    scheduler.apply(
        &SchedulerEvent::CancellationRequested {
            operation_id: id(20),
            recursive: true,
        },
        &mut None,
    )?;
    for op in [21, 23] {
        assert_eq!(
            scheduler.operation(id(op)).unwrap().outcome,
            Some(Outcome::Cancelled)
        );
    }
    assert_eq!(
        scheduler.operation(id(22)).unwrap().phase,
        OperationPhase::WaitingForDependencies
    );
    Ok(())
}

#[test]
fn failed_dependencies_are_explicitly_rejectable() -> Result<()> {
    let mut scheduler = Scheduler::new();
    scheduler.apply(
        &scheduler.declare(spec(id(1), Orchestration::Leaf)?)?,
        &mut None,
    )?;
    start(&mut scheduler, id(1), reservation(1, 0))?;
    complete(
        &mut scheduler,
        id(1),
        Outcome::Failed {
            message: "failed".into(),
        },
        Some(fence(1)),
    )?;
    let mut dependent = spec(id(2), Orchestration::Leaf)?;
    dependent.dependencies.insert(id(1));
    scheduler.apply(&scheduler.declare(dependent)?, &mut None)?;
    assert_eq!(
        scheduler.blocked_by_dependencies(None).collect::<Vec<_>>(),
        vec![id(2)]
    );
    scheduler.apply(
        &SchedulerEvent::Rejected {
            operation_id: id(2),
            reason: "dependency failed".into(),
        },
        &mut None,
    )?;
    assert_eq!(
        scheduler.operation(id(2)).unwrap().outcome,
        Some(Outcome::Failed {
            message: "dependency failed".into()
        })
    );
    Ok(())
}

#[test]
fn partial_admission_advances_one_lease_and_only_requires_the_remainder() -> Result<()> {
    let mut scheduler = Scheduler::new();
    let mut declared = spec(id(5), Orchestration::Leaf)?;
    declared.resources = cpu(4);
    scheduler.apply(&scheduler.declare(declared)?, &mut None)?;
    let partial = |reservation| SchedulerEvent::PartiallyAdmitted {
        operation_id: id(5),
        reservation,
    };
    scheduler.apply(&partial(reservation(1, 2)), &mut None)?;
    assert_eq!(
        scheduler
            .ready(&cpu(2), &BTreeMap::new(), None)
            .collect::<Vec<_>>(),
        vec![id(5)]
    );
    assert!(matches!(
        scheduler.apply(&partial(reservation(2, 3)), &mut None),
        Err(Error::Conflict(_))
    ));
    Ok(())
}
