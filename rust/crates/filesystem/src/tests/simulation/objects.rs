use super::*;

#[tokio::test]
async fn delayed_objects_are_invisible_until_flush() -> Result<(), Box<dyn std::error::Error>> {
    let (schedule, _events) = Schedule::new();
    let simulation = crate::Simulation::new(&schedule);
    schedule.inject(sites::OBJECT_PUT, 0, SimulationFault::DelayObjectVisibility);
    let (_, objects) = simulation.stores();
    let cancellation = CancellationToken::new();
    let (object_id, bytes) = object(b"delayed");
    (work!(objects, put(object_id, bytes.clone()), B, &cancellation)).await?;
    let missing_result = (work!(objects, read(object_id, 64), B, &cancellation)).await;
    let Err(missing) = missing_result else {
        return Err("delayed object became visible before flush".into());
    };
    assert!(matches!(missing.error, ObjectStoreError::Missing));
    let flushed = (work!(simulation, flush_object_visibility(), B, &cancellation)).await?;
    assert_eq!(flushed.value, 1);
    let visible = (work!(objects, read(object_id, 64), B, &cancellation)).await?;
    assert_eq!(visible.value.bytes, bytes);
    Ok(())
}

#[tokio::test]
async fn corrupt_and_partial_reads_are_exactly_scheduled() -> Result<(), Box<dyn std::error::Error>>
{
    let (schedule, _events) = Schedule::new();
    let simulation = crate::Simulation::new(&schedule);
    schedule.inject(sites::OBJECT_READ, 0, SimulationFault::CorruptObjectRead);
    schedule.inject(sites::OBJECT_READ, 1, SimulationFault::PartialObjectRead);
    let (_, objects) = simulation.stores();
    let cancellation = CancellationToken::new();
    let (object_id, bytes) = object(b"authenticated");
    (work!(objects, put(object_id, bytes), B, &cancellation)).await?;
    let corrupt = (work!(objects, read(object_id, 64), B, &cancellation)).await?;
    assert_ne!(object_digest(&corrupt.value.bytes), object_id.digest);
    let partial = (work!(objects, read(object_id, 64), B, &cancellation)).await?;
    assert_eq!(partial.value.bytes.len(), b"authenticated".len() - 1);
    Ok(())
}

#[tokio::test]
async fn delayed_object_admission_is_idempotent_and_digest_checked()
-> Result<(), Box<dyn std::error::Error>> {
    let (schedule, _events) = Schedule::new();
    let simulation = crate::Simulation::new(&schedule);
    for ordinal in 0..4 {
        schedule.inject(
            sites::OBJECT_PUT,
            ordinal,
            SimulationFault::DelayObjectVisibility,
        );
    }
    let (_, objects) = simulation.stores();
    let cancellation = CancellationToken::new();
    let (first_id, first_bytes) = object(b"four");
    let digest_failure = (work!(
        objects,
        put(first_id, OwnedBytes::from_static(b"bad!")),
        B,
        &cancellation
    ))
    .await
    .err()
    .ok_or("digest mismatch was admitted")?;
    assert!(matches!(
        digest_failure.error,
        ObjectStoreError::DigestMismatch
    ));
    (work!(
        objects,
        put(first_id, first_bytes.clone()),
        B,
        &cancellation
    ))
    .await?;
    (work!(objects, put(first_id, first_bytes), B, &cancellation)).await?;

    let (second_id, second_bytes) = object(b"x");
    (work!(
        objects,
        put(second_id, second_bytes.clone()),
        B,
        &cancellation
    ))
    .await?;
    let flushed = (work!(simulation, flush_object_visibility(), B, &cancellation)).await?;
    assert_eq!(flushed.value, 2);
    let visible = (work!(objects, read(second_id, 64), B, &cancellation)).await?;
    assert_eq!(visible.value.bytes, second_bytes);
    Ok(())
}

#[tokio::test]
async fn empty_corrupt_reads_and_authority_passthroughs_remain_typed()
-> Result<(), Box<dyn std::error::Error>> {
    let (schedule, _events) = Schedule::new();
    let simulation = crate::Simulation::new(&schedule);
    schedule.inject(sites::OBJECT_READ, 0, SimulationFault::CorruptObjectRead);
    let (authority, objects) = simulation.stores();
    let cancellation = CancellationToken::new();
    let (empty_id, empty_bytes) = object(b"");
    (work!(objects, put(empty_id, empty_bytes), B, &cancellation)).await?;
    let corrupt = (work!(objects, read(empty_id, 0), B, &cancellation))
        .await
        .err()
        .ok_or("empty corrupt read succeeded")?;
    assert!(matches!(corrupt.error, ObjectStoreError::Corrupt));

    let authority_id = AuthorityId::from_bytes([71; 16]);
    (work!(
        authority,
        create_authority(authority_id, Epoch::GENESIS),
        B,
        &cancellation
    ))
    .await?;
    let missing = (work!(
        authority,
        find_operation(authority_id, OperationId::from_bytes([72; 16])),
        B,
        &cancellation
    ))
    .await?;
    assert!(missing.value.is_none());
    let head = (work!(authority, head(authority_id), B, &cancellation))
        .await?
        .value;
    let fenced = (work!(authority, fence(authority_id, head), B, &cancellation)).await?;
    assert!(matches!(
        fenced.value,
        FenceOutcome::Advanced(advanced) if advanced.epoch.get() == 2
    ));
    Ok(())
}

#[tokio::test]
async fn corrupt_delayed_visibility_state_preserves_nested_failure_work()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();

    let (schedule, _events) = Schedule::new();

    let simulation = crate::Simulation::new(&schedule);
    let (object_id, _) = object(b"authentic");
    {
        let mut state = simulation
            .state
            .lock()
            .map_err(|_| "simulation state unexpectedly poisoned")?;
        state
            .pending_objects
            .insert(object_id, OwnedBytes::from_static(b"forged"));
        state.pending_object_bytes = 6;
    }
    let failure = (work!(simulation, flush_object_visibility(), B, &cancellation))
        .await
        .err()
        .ok_or("corrupt delayed object unexpectedly published")?;
    assert!(matches!(failure.error, ObjectStoreError::DigestMismatch));
    assert_eq!(failure.work.backend_write_operations, 0);

    let (schedule, _events) = Schedule::new();

    let simulation = crate::Simulation::new(&schedule);
    let (object_id, bytes) = object(b"underflow");
    {
        let mut state = simulation
            .state
            .lock()
            .map_err(|_| "simulation state unexpectedly poisoned")?;
        state.pending_objects.insert(object_id, bytes);
        state.pending_object_bytes = 0;
    }
    let failure = (work!(simulation, flush_object_visibility(), B, &cancellation))
        .await
        .err()
        .ok_or("underflowing delayed accounting unexpectedly succeeded")?;
    assert!(matches!(failure.error, ObjectStoreError::Corrupt));

    Ok(())
}
