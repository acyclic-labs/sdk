use super::*;

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
#[should_panic(expected = "CorruptObjectRead cannot apply at authority.head")]
async fn a_fault_planned_where_it_cannot_apply_fails_the_call() {
    let (schedule, _events) = Schedule::new();
    let simulation = Simulation::new(&schedule);
    schedule.inject(sites::AUTHORITY_HEAD, 0, SimulationFault::CorruptObjectRead);
    let (authority, _) = simulation.stores();
    let cancellation = CancellationToken::new();
    let _ = work!(
        authority,
        head(AuthorityId::from_bytes([1; 16])),
        B,
        &cancellation
    )
    .await;
}
