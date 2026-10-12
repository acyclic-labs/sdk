use super::*;

#[tokio::test]
async fn delayed_objects_are_invisible_until_bounded_flush()
-> Result<(), Box<dyn std::error::Error>> {
    let simulation = Simulation::default();
    simulation.schedule(ScheduledSimulationFault {
        operation: SimulationOperation::ObjectPut,
        occurrence: 1,
        fault: SimulationFault::DelayObjectVisibility,
    })?;
    let (_, objects) = simulation.stores();
    let cancellation = CancellationToken::new();
    let (object_id, bytes) = object(b"delayed");
    (objects.put(
        object_id,
        bytes.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    let missing_result = (objects.read(object_id, 64, WorkBudget::UNBOUNDED, &cancellation)).await;
    let Err(missing) = missing_result else {
        return Err("delayed object became visible before flush".into());
    };
    assert!(matches!(missing.error, ObjectStoreError::Missing));
    let flushed =
        (simulation.flush_object_visibility(WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert_eq!(flushed.value, 1);
    let visible = (objects.read(object_id, 64, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert_eq!(visible.value.bytes, bytes);
    Ok(())
}

#[tokio::test]
async fn corrupt_and_partial_reads_are_exactly_scheduled() -> Result<(), Box<dyn std::error::Error>>
{
    let simulation = Simulation::default();
    simulation.schedule(ScheduledSimulationFault {
        operation: SimulationOperation::ObjectRead,
        occurrence: 1,
        fault: SimulationFault::CorruptObjectRead,
    })?;
    simulation.schedule(ScheduledSimulationFault {
        operation: SimulationOperation::ObjectRead,
        occurrence: 2,
        fault: SimulationFault::PartialObjectRead,
    })?;
    let (_, objects) = simulation.stores();
    let cancellation = CancellationToken::new();
    let (object_id, bytes) = object(b"authenticated");
    (objects.put(object_id, bytes, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let corrupt = (objects.read(object_id, 64, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert_ne!(
        object_digest(object_id.kind, &corrupt.value.bytes),
        object_id.digest
    );
    let partial = (objects.read(object_id, 64, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert_eq!(partial.value.bytes.len(), b"authenticated".len() - 1);
    Ok(())
}

#[tokio::test]
async fn delayed_object_admission_is_idempotent_bounded_and_digest_checked()
-> Result<(), Box<dyn std::error::Error>> {
    let simulation = Simulation::new(SimulationOptions {
        maximum_scheduled_faults: 4,
        maximum_trace_entries: 8,
        maximum_pending_objects: 1,
        maximum_pending_object_bytes: 4,
    })?;
    for occurrence in 1..=4 {
        simulation.schedule(ScheduledSimulationFault {
            operation: SimulationOperation::ObjectPut,
            occurrence,
            fault: SimulationFault::DelayObjectVisibility,
        })?;
    }
    let (_, objects) = simulation.stores();
    let cancellation = CancellationToken::new();
    let (first_id, first_bytes) = object(b"four");
    let digest_failure = (objects.put(
        first_id,
        OwnedBytes::from_static(b"bad!"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("digest mismatch was admitted")?;
    assert!(matches!(
        digest_failure.error,
        ObjectStoreError::DigestMismatch
    ));
    (objects.put(
        first_id,
        first_bytes.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    let repeated =
        (objects.put(first_id, first_bytes, WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert_eq!(repeated.work.backend_write_operations, 1);

    let (second_id, second_bytes) = object(b"x");
    let bounded = (objects.put(
        second_id,
        second_bytes,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("delayed-object bound was ignored")?;
    assert!(matches!(bounded.error, ObjectStoreError::Rejected(_)));
    let flushed =
        (simulation.flush_object_visibility(WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert_eq!(flushed.value, 1);
    Ok(())
}

#[tokio::test]
async fn empty_corrupt_reads_and_authority_passthroughs_remain_typed()
-> Result<(), Box<dyn std::error::Error>> {
    let simulation = Simulation::default();
    simulation.schedule(ScheduledSimulationFault {
        operation: SimulationOperation::ObjectRead,
        occurrence: 1,
        fault: SimulationFault::CorruptObjectRead,
    })?;
    let (authority, objects) = simulation.stores();
    let cancellation = CancellationToken::new();
    let (empty_id, empty_bytes) = object(b"");
    (objects.put(empty_id, empty_bytes, WorkBudget::UNBOUNDED, &cancellation)).await?;
    let corrupt = (objects.read(empty_id, 0, WorkBudget::UNBOUNDED, &cancellation))
        .await
        .err()
        .ok_or("empty corrupt read succeeded")?;
    assert!(matches!(corrupt.error, ObjectStoreError::Corrupt));

    let authority_id = AuthorityId::from_bytes([71; 16]);
    (async {
        let __authority_args = (
            authority_id,
            Epoch::GENESIS,
            WorkBudget::UNBOUNDED,
            &cancellation,
        );
        let mut __authority_work =
            crate::AuthorityWork::new(__authority_args.2, __authority_args.3);
        let __authority_result = authority
            .create_authority(
                __authority_args.0,
                __authority_args.1,
                &mut __authority_work,
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity));
        __authority_work.receipt(__authority_result)
    })
    .await?;
    let missing = (async {
        let __authority_args = (
            authority_id,
            OperationId::from_bytes([72; 16]),
            WorkBudget::UNBOUNDED,
            &cancellation,
        );
        let mut __authority_work =
            crate::AuthorityWork::new(__authority_args.2, __authority_args.3);
        let __authority_result = authority
            .find_operation(
                __authority_args.0,
                __authority_args.1,
                &mut __authority_work,
            )
            .await
            .map(|(value, _allocation)| value);
        __authority_work.receipt(__authority_result)
    })
    .await?;
    assert!(missing.value.is_none());
    let head = (async {
        let __authority_args = (authority_id, WorkBudget::UNBOUNDED, &cancellation);
        let mut __authority_work =
            crate::AuthorityWork::new(__authority_args.1, __authority_args.2);
        let __authority_result = authority
            .head(__authority_args.0, &mut __authority_work)
            .await;
        __authority_work.receipt(__authority_result)
    })
    .await?
    .value;
    let fenced = (async {
        let __authority_args = (authority_id, head, WorkBudget::UNBOUNDED, &cancellation);
        let mut __authority_work =
            crate::AuthorityWork::new(__authority_args.2, __authority_args.3);
        let __authority_result = authority
            .fence(
                __authority_args.0,
                __authority_args.1,
                &mut __authority_work,
            )
            .await;
        __authority_work.receipt(__authority_result)
    })
    .await?;
    assert!(matches!(
        fenced.value,
        FenceOutcome::Advanced(advanced) if advanced.epoch.get() == 2
    ));
    Ok(())
}

#[tokio::test]
async fn poisoned_simulation_authority_and_object_surfaces_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let simulation = Simulation::default();
    let state = Arc::clone(&simulation.state);
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let Ok(_guard) = state.lock() else {
            return;
        };
        std::panic::resume_unwind(Box::new("poison simulation state"));
    }));
    assert!(poisoned.is_err());
    assert_eq!(
        simulation.schedule(ScheduledSimulationFault {
            operation: SimulationOperation::ObjectRead,
            occurrence: 1,
            fault: SimulationFault::RejectBefore,
        }),
        Err(SimulationError::Poisoned)
    );

    let (authority, objects) = simulation.stores();
    let cancellation = CancellationToken::new();
    let authority_failure = (async {
        let __authority_args = (
            AuthorityId::from_bytes([80; 16]),
            WorkBudget::UNBOUNDED,
            &cancellation,
        );
        let mut __authority_work =
            crate::AuthorityWork::new(__authority_args.1, __authority_args.2);
        let __authority_result = authority
            .head(__authority_args.0, &mut __authority_work)
            .await;
        __authority_work.receipt(__authority_result)
    })
    .await
    .err()
    .ok_or("poisoned authority lookup unexpectedly succeeded")?;
    assert!(matches!(
        authority_failure.error,
        AuthorityStoreError::CorruptStatic(_)
    ));
    let object_failure =
        (objects.contains(object(b"poisoned").0, WorkBudget::UNBOUNDED, &cancellation))
            .await
            .err()
            .ok_or("poisoned object lookup unexpectedly succeeded")?;
    assert!(matches!(object_failure.error, ObjectStoreError::Corrupt));
    Ok(())
}

#[tokio::test]
async fn corrupt_delayed_visibility_state_preserves_nested_failure_work()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();

    let simulation = Simulation::default();
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
    let failure = (simulation.flush_object_visibility(WorkBudget::UNBOUNDED, &cancellation))
        .await
        .err()
        .ok_or("corrupt delayed object unexpectedly published")?;
    assert!(matches!(failure.error, ObjectStoreError::DigestMismatch));
    assert_eq!(failure.work.backend_write_operations, 0);

    let simulation = Simulation::default();
    let (object_id, bytes) = object(b"underflow");
    {
        let mut state = simulation
            .state
            .lock()
            .map_err(|_| "simulation state unexpectedly poisoned")?;
        state.pending_objects.insert(object_id, bytes);
        state.pending_object_bytes = 0;
    }
    let failure = (simulation.flush_object_visibility(WorkBudget::UNBOUNDED, &cancellation))
        .await
        .err()
        .ok_or("underflowing delayed accounting unexpectedly succeeded")?;
    assert!(matches!(failure.error, ObjectStoreError::Corrupt));
    assert_eq!(failure.work.backend_write_operations, 1);
    Ok(())
}
