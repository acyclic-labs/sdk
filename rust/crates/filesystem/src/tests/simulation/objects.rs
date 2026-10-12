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
    (async { let __object_args = (object_id, bytes.clone(), WorkBudget::UNBOUNDED, &cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3); let __object_result = objects.put(__object_args.0, __object_args.1, &mut __object_work).await; __object_work.receipt(__object_result) })
    .await?;
    let missing_result = (async { let __object_args = (object_id, 64, WorkBudget::UNBOUNDED, &cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3); let __object_result = objects.read(__object_args.0, __object_args.1, &mut __object_work).await; __object_work.receipt(__object_result) }).await;
    let Err(missing) = missing_result else {
        return Err("delayed object became visible before flush".into());
    };
    assert!(matches!(missing.error, ObjectStoreError::Missing));
    let flushed =
        (simulation.flush_object_visibility(WorkBudget::UNBOUNDED, &cancellation)).await?;
    assert_eq!(flushed.value, 1);
    let visible = (async { let __object_args = (object_id, 64, WorkBudget::UNBOUNDED, &cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3); let __object_result = objects.read(__object_args.0, __object_args.1, &mut __object_work).await; __object_work.receipt(__object_result) }).await?;
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
    (async { let __object_args = (object_id, bytes, WorkBudget::UNBOUNDED, &cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3); let __object_result = objects.put(__object_args.0, __object_args.1, &mut __object_work).await; __object_work.receipt(__object_result) }).await?;
    let corrupt = (async { let __object_args = (object_id, 64, WorkBudget::UNBOUNDED, &cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3); let __object_result = objects.read(__object_args.0, __object_args.1, &mut __object_work).await; __object_work.receipt(__object_result) }).await?;
    assert_ne!(
        object_digest(object_id.kind, &corrupt.value.bytes),
        object_id.digest
    );
    let partial = (async { let __object_args = (object_id, 64, WorkBudget::UNBOUNDED, &cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3); let __object_result = objects.read(__object_args.0, __object_args.1, &mut __object_work).await; __object_work.receipt(__object_result) }).await?;
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
    let digest_failure = (async { let __object_args = (first_id, OwnedBytes::from_static(b"bad!"), WorkBudget::UNBOUNDED, &cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3); let __object_result = objects.put(__object_args.0, __object_args.1, &mut __object_work).await; __object_work.receipt(__object_result) })
    .await
    .err()
    .ok_or("digest mismatch was admitted")?;
    assert!(matches!(
        digest_failure.error,
        ObjectStoreError::DigestMismatch
    ));
    (async { let __object_args = (first_id, first_bytes.clone(), WorkBudget::UNBOUNDED, &cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3); let __object_result = objects.put(__object_args.0, __object_args.1, &mut __object_work).await; __object_work.receipt(__object_result) })
    .await?;
    let repeated =
        (async { let __object_args = (first_id, first_bytes, WorkBudget::UNBOUNDED, &cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3); let __object_result = objects.put(__object_args.0, __object_args.1, &mut __object_work).await; __object_work.receipt(__object_result) }).await?;
    assert_eq!(repeated.work.backend_write_operations, 1);

    let (second_id, second_bytes) = object(b"x");
    let bounded = (async { let __object_args = (second_id, second_bytes, WorkBudget::UNBOUNDED, &cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3); let __object_result = objects.put(__object_args.0, __object_args.1, &mut __object_work).await; __object_work.receipt(__object_result) })
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
    (async { let __object_args = (empty_id, empty_bytes, WorkBudget::UNBOUNDED, &cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3); let __object_result = objects.put(__object_args.0, __object_args.1, &mut __object_work).await; __object_work.receipt(__object_result) }).await?;
    let corrupt = (async { let __object_args = (empty_id, 0, WorkBudget::UNBOUNDED, &cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3); let __object_result = objects.read(__object_args.0, __object_args.1, &mut __object_work).await; __object_work.receipt(__object_result) })
        .await
        .err()
        .ok_or("empty corrupt read succeeded")?;
    assert!(matches!(corrupt.error, ObjectStoreError::Corrupt));

    let authority_id = AuthorityId::from_bytes([71; 16]);
    (async {
        let mut __authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let __authority_result = authority
            .create_authority(
                authority_id,
                Epoch::GENESIS,
                &mut __authority_work,
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity));
        __authority_work.receipt(__authority_result)
    })
    .await?;
    let missing = (async {
        let authority_input_1 = OperationId::from_bytes([72; 16]);

        let mut __authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let __authority_result = authority
            .find_operation(authority_id, authority_input_1, &mut __authority_work)
            .await
            .map(|(value, _allocation)| value);
        __authority_work.receipt(__authority_result)
    })
    .await?;
    assert!(missing.value.is_none());
    let head = (async {
        let mut __authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let __authority_result = authority.head(authority_id, &mut __authority_work).await;
        __authority_work.receipt(__authority_result)
    })
    .await?
    .value;
    let fenced = (async {
        let mut __authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let __authority_result = authority
            .fence(authority_id, head, &mut __authority_work)
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
        let authority_input_0 = AuthorityId::from_bytes([80; 16]);

        let mut __authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let __authority_result = authority
            .head(authority_input_0, &mut __authority_work)
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
        (async { let __object_args = (object(b"poisoned").0, WorkBudget::UNBOUNDED, &cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2); let __object_result = objects.contains(__object_args.0, &mut __object_work).await; __object_work.receipt(__object_result) })
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
