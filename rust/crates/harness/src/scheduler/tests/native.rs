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

fn apply_native(
    scheduler: &mut Scheduler,
    event: &SchedulerEvent,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    let io = crate::filesystem::PreparationIo::from_native_work(work)?;
    scheduler.apply(event, &mut Some(&mut io.history_io()))
}

#[test]
fn phase_fence_capacity_quota_and_cancellation_are_atomic() -> Result<()> {
    let mut scheduler = session(
        spec(id(82), Orchestration::Leaf)?,
        SessionLimits {
            active_tasks: 1,
            total_tasks: 2,
            depth: 1,
            model_steps: 2,
        },
    )?;
    scheduler.apply(&scheduler.declare(child(83, 82, "capacity")?)?, &mut None)?;
    start(&mut scheduler, id(82), reservation(4, 2))?;
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
        rejected(&mut scheduler, &event);
        let cancellation = acyclic_fs::CancellationToken::new();
        let work =
            acyclic_fs::NativeMaterializationWork::new(budget(), &cancellation).expect("work");
        let error = apply_native(&mut scheduler, &event, &work).expect_err("invalid event");
        assert!(error.is_native_failure());
        assert_ne!(error.code(), crate::wire::ErrorCode::Unsupported);
        assert_eq!(scheduler, before);
    }
    let event = SchedulerEvent::LeaseReleased {
        operation_id: id(82),
        fence: fence(4),
    };
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget {
            bytes_copied: 0,
            ..budget()
        },
        &cancellation,
    )
    .expect("work");
    assert!(
        apply_native(&mut scheduler, &event, &work)
            .expect_err("copy limit")
            .is_native_failure()
    );
    assert_eq!(scheduler, before);
    let work = acyclic_fs::NativeMaterializationWork::new(budget(), &cancellation).expect("work");
    cancellation.cancel();
    let receipt = work.receipt();
    assert!(
        apply_native(&mut scheduler, &event, &work)
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
    let root = spec(id(89), Orchestration::Leaf)?;
    Scheduler::new().validate_session(&root, &limits, &mut None)?;
    let mut scheduler = session(root.clone(), limits)?;
    assert!(
        scheduler
            .validate_session(&root, &limits, &mut None)
            .is_err()
    );
    let child = child(90, 89, "child")?;
    assert!(
        scheduler
            .validate_session(&child, &limits, &mut None)
            .is_err()
    );
    scheduler.apply(&scheduler.declare(child)?, &mut None)?;
    rejected(
        &mut scheduler,
        &SchedulerEvent::SessionConfigured {
            operation_id: id(90),
            limits,
        },
    );
    Ok(())
}
