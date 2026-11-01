use super::*;

#[tokio::test]
async fn scheduling_is_typed_and_traceable() -> Result<(), Box<dyn std::error::Error>> {
    let simulation = Simulation::new();
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
    let (_, objects) = simulation.stores();
    let cancellation = CancellationToken::new();
    let (object_id, _) = object(b"x");
    let rejected_result = (work!(objects, contains(object_id), B, &cancellation)).await;
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
    let second_result = (work!(objects, contains(object_id), B, &cancellation)).await;
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
async fn duplicate_and_incompatible_schedules_are_refused() -> Result<(), Box<dyn std::error::Error>>
{
    let simulation = Simulation::new();
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
    Ok(())
}
