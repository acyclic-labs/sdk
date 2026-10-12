use super::*;

#[tokio::test]
async fn scheduling_is_bounded_typed_and_traceable() -> Result<(), Box<dyn std::error::Error>> {
    let simulation = Simulation::new(SimulationOptions {
        maximum_scheduled_faults: 1,
        maximum_trace_entries: 2,
        maximum_pending_objects: 1,
        maximum_pending_object_bytes: 8,
    })?;
    assert_eq!(
        simulation.schedule(ScheduledSimulationFault {
            operation: SimulationOperation::ObjectRead,
            occurrence: 0,
            fault: SimulationFault::RejectBefore,
        }),
        Err(SimulationError::ZeroOccurrence)
    );
    assert_eq!(
        simulation.schedule(ScheduledSimulationFault {
            operation: SimulationOperation::AuthorityHead,
            occurrence: 1,
            fault: SimulationFault::CorruptObjectRead,
        }),
        Err(SimulationError::IncompatibleFault)
    );
    simulation.schedule(ScheduledSimulationFault {
        operation: SimulationOperation::ObjectContains,
        occurrence: 1,
        fault: SimulationFault::RejectBefore,
    })?;
    assert_eq!(
        simulation.schedule(ScheduledSimulationFault {
            operation: SimulationOperation::ObjectRead,
            occurrence: 1,
            fault: SimulationFault::RejectBefore,
        }),
        Err(SimulationError::ScheduleFull)
    );
    let (_, objects) = simulation.stores();
    let cancellation = CancellationToken::new();
    let (object_id, _) = object(b"x");
    let rejected_result = (async {
        let mut __object_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let __object_result = objects.contains(object_id, &mut __object_work).await;
        __object_work.receipt(__object_result)
    })
    .await;
    let Err(rejected) = rejected_result else {
        return Err("scheduled contains rejection succeeded".into());
    };
    assert!(matches!(rejected.error, ObjectStoreError::Rejected(_)));
    assert_eq!(simulation.trace()?.len(), 1);
    assert_eq!(
        simulation.schedule(ScheduledSimulationFault {
            operation: SimulationOperation::ObjectContains,
            occurrence: 1,
            fault: SimulationFault::RejectBefore,
        }),
        Err(SimulationError::PastOccurrence)
    );
    let next = simulation.schedule_next(
        SimulationOperation::ObjectContains,
        SimulationFault::PartitionBefore,
    )?;
    assert_eq!(next.occurrence, 2);
    let second_result = (async {
        let mut __object_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let __object_result = objects.contains(object_id, &mut __object_work).await;
        __object_work.receipt(__object_result)
    })
    .await;
    let Err(second) = second_result else {
        return Err("scheduled next partition succeeded".into());
    };
    assert!(matches!(second.error, ObjectStoreError::Rejected(_)));
    assert_eq!(simulation.trace()?.len(), 2);
    Ok(())
}

#[tokio::test]
async fn every_operation_rejects_and_partitions_before_inner_work()
-> Result<(), Box<dyn std::error::Error>> {
    for fault in [
        SimulationFault::RejectBefore,
        SimulationFault::PartitionBefore,
    ] {
        exercise_authority_before_work(Some(fault)).await?;
        exercise_object_before_work(Some(fault)).await?;
    }
    Ok(())
}

#[tokio::test]
async fn every_operation_honours_precancellation_before_inner_work()
-> Result<(), Box<dyn std::error::Error>> {
    exercise_authority_before_work(None).await?;
    exercise_object_before_work(None).await?;
    Ok(())
}

#[tokio::test]
async fn simulator_bounds_duplicate_schedule_and_trace_exhaustion_are_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(matches!(
        Simulation::new(SimulationOptions {
            maximum_scheduled_faults: 0,
            ..SimulationOptions::default()
        }),
        Err(SimulationError::ZeroBound)
    ));
    let simulation = Simulation::new(SimulationOptions {
        maximum_scheduled_faults: 2,
        maximum_trace_entries: 1,
        maximum_pending_objects: 1,
        maximum_pending_object_bytes: 4,
    })?;
    let scheduled = ScheduledSimulationFault {
        operation: SimulationOperation::ObjectContains,
        occurrence: 1,
        fault: SimulationFault::RejectBefore,
    };
    simulation.schedule(scheduled)?;
    assert_eq!(
        simulation.schedule(scheduled),
        Err(SimulationError::DuplicateFault)
    );
    assert_eq!(
        simulation.schedule_next(
            SimulationOperation::AuthorityFence,
            SimulationFault::PartialObjectRead,
        ),
        Err(SimulationError::IncompatibleFault)
    );
    let (_, objects) = simulation.stores();
    let cancellation = CancellationToken::new();
    let (object_id, _) = object(b"trace");
    assert!(
        (async {
            let mut __object_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
            let __object_result = objects.contains(object_id, &mut __object_work).await;
            __object_work.receipt(__object_result)
        })
        .await
        .is_err()
    );
    let exhausted = (async {
        let mut __object_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let __object_result = objects.contains(object_id, &mut __object_work).await;
        __object_work.receipt(__object_result)
    })
    .await
    .err()
    .ok_or("trace exhaustion was ignored")?;
    assert!(matches!(exhausted.error, ObjectStoreError::Rejected(_)));
    assert_eq!(simulation.trace()?.len(), 1);
    Ok(())
}
