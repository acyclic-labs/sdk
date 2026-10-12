use super::*;
fn budget() -> acyclic_fs::WorkBudget {
    acyclic_fs::WorkBudget {
        items_examined: 1_048_576,
        bytes_copied: 16_777_216,
        bytes_encoded: 16_777_216,
        bytes_hashed: 16_777_216,
        allocation_operations: 65_536,
        peak_allocation_bytes: 16_777_216,
        ..Default::default()
    }
}
#[test]
fn phase_fence_capacity_quota_and_cancellation_are_atomic() -> Result<()> {
    let mut scheduler = Scheduler::new();
    scheduler.apply(
        &scheduler.declare(spec(id(82), Orchestration::Leaf)?)?,
        &mut None,
    )?;
    scheduler.apply(
        &SchedulerEvent::SessionConfigured {
            operation_id: id(82),
            limits: SessionLimits {
                active_tasks: 1,
                total_tasks: 2,
                depth: 1,
                model_steps: 2,
            },
        },
        &mut None,
    )?;
    let mut child = spec(id(83), Orchestration::Leaf)?;
    child.parent = Some(ParentLink {
        operation_id: id(82),
        slot: "capacity".into(),
    });
    scheduler.apply(&scheduler.declare(child)?, &mut None)?;
    scheduler.apply(
        &SchedulerEvent::Admitted {
            operation_id: id(82),
            reservation: reservation(4, 2),
        },
        &mut None,
    )?;
    scheduler.apply(
        &SchedulerEvent::Started {
            operation_id: id(82),
            fence: fence(4),
        },
        &mut None,
    )?;
    let before = scheduler.clone();
    let invalid = [
        SchedulerEvent::Started {
            operation_id: id(82),
            fence: fence(4),
        },
        SchedulerEvent::Checkpointed {
            operation_id: id(82),
            checkpoint: CheckpointRef::new(
                ProviderRef::new("example".into(), "machines".into(), "1".into(), &mut None)?,
                vec![1],
                None,
            )?,
            fence: fence(5),
        },
        SchedulerEvent::LeaseReleased {
            operation_id: id(82),
            fence: fence(5),
        },
        SchedulerEvent::Admitted {
            operation_id: id(83),
            reservation: reservation(6, 1),
        },
        SchedulerEvent::WorkflowSuspended {
            operation_id: id(82),
            fence: fence(4),
            workflow_revision: 0,
            waiting_command: None,
        },
        SchedulerEvent::Completed {
            operation_id: id(82),
            outcome: Outcome::Indeterminate {
                operation_id: id(83),
            },
            fence: Some(fence(4)),
            execution_duration_ns: None,
        },
    ];
    for event in invalid {
        assert!(before.clone().apply(&event, &mut None).is_err());
        let cancel = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(budget(), &cancel).expect("work");
        let mut io = crate::store::history::native_work_io(&work)?;
        let error = scheduler
            .apply(&event, &mut Some(&mut io))
            .expect_err("invalid event");
        assert!(error.is_native_failure());
        assert_ne!(error.code(), crate::wire::ErrorCode::Unsupported);
        assert_eq!(scheduler, before);
    }
    let event = SchedulerEvent::LeaseReleased {
        operation_id: id(82),
        fence: fence(4),
    };
    let cancel = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget {
            bytes_copied: 0,
            ..budget()
        },
        &cancel,
    )
    .expect("work");
    let mut io = crate::store::history::native_work_io(&work)?;
    assert!(
        scheduler
            .apply(&event, &mut Some(&mut io))
            .expect_err("copy limit")
            .is_native_failure()
    );
    assert_eq!(scheduler, before);
    let work = acyclic_fs::NativeMaterializationWork::new(budget(), &cancel).expect("work");
    let mut io = crate::store::history::native_work_io(&work)?;
    cancel.cancel();
    let receipt = work.receipt();
    assert!(
        scheduler
            .apply(&event, &mut Some(&mut io))
            .expect_err("cancelled")
            .is_native_failure()
    );
    assert_eq!(scheduler, before);
    assert_eq!(work.receipt(), receipt);
    Ok(())
}
#[test]
fn prospective_session_uses_immutable_root_policy() -> Result<()> {
    let limits = SessionLimits {
        active_tasks: 1,
        total_tasks: 2,
        depth: 1,
        model_steps: 2,
    };
    let mut scheduler = Scheduler::new();
    let root = spec(id(89), Orchestration::Leaf)?;
    scheduler.validate_session(&root, &limits, &mut None)?;
    scheduler.apply(&scheduler.declare(root.clone())?, &mut None)?;
    scheduler.apply(
        &SchedulerEvent::SessionConfigured {
            operation_id: root.operation_id,
            limits,
        },
        &mut None,
    )?;
    assert!(
        scheduler
            .validate_session(&root, &limits, &mut None)
            .is_err()
    );
    let mut child = spec(id(90), Orchestration::Leaf)?;
    child.parent = Some(ParentLink {
        operation_id: root.operation_id,
        slot: "child".into(),
    });
    assert!(
        scheduler
            .validate_session(&child, &limits, &mut None)
            .is_err()
    );
    scheduler.apply(&scheduler.declare(child)?, &mut None)?;
    assert!(
        scheduler
            .apply(
                &SchedulerEvent::SessionConfigured {
                    operation_id: id(90),
                    limits
                },
                &mut None
            )
            .is_err()
    );
    Ok(())
}
