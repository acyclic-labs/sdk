use super::*;
use crate::WorkError;
use crate::foundation::Digest;
use crate::storage::{FenceOutcome, ObjectKind};

fn object(bytes: &[u8]) -> (ObjectId, Bytes) {
    let bytes = Bytes::copy_from_slice(bytes);
    (
        ObjectId {
            kind: ObjectKind::BlobChunk,
            digest: object_digest(ObjectKind::BlobChunk, &bytes),
        },
        bytes,
    )
}

fn proposal(operation: u8, fingerprint: u8) -> ProposedCommit {
    ProposedCommit {
        operation_id: OperationId::from_bytes([operation; 16]),
        fingerprint: Digest::from_bytes([fingerprint; 32]),
        payload: Bytes::from(vec![operation, fingerprint]),
    }
}

#[test]
fn delayed_objects_are_invisible_until_bounded_flush() -> Result<(), Box<dyn std::error::Error>> {
    let simulation = Simulation::default();
    simulation.schedule(ScheduledSimulationFault {
        operation: SimulationOperation::ObjectPut,
        occurrence: 1,
        fault: SimulationFault::DelayObjectVisibility,
    })?;
    let (_, objects) = simulation.stores();
    let cancellation = CancellationToken::new();
    let (object_id, bytes) = object(b"delayed");
    crate::async_storage::poll_ready(objects.put(
        object_id,
        bytes.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("simulated put suspended")??;
    let missing_result = crate::async_storage::poll_ready(objects.read(
        object_id,
        64,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("simulated read suspended")?;
    let Err(missing) = missing_result else {
        return Err("delayed object became visible before flush".into());
    };
    assert!(matches!(missing.error, ObjectStoreError::Missing));
    let flushed = crate::async_storage::poll_ready(
        simulation.flush_object_visibility(WorkBudget::UNBOUNDED, &cancellation),
    )
    .ok_or("simulated visibility flush suspended")??;
    assert_eq!(flushed.value, 1);
    let visible = crate::async_storage::poll_ready(objects.read(
        object_id,
        64,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("simulated read suspended")??;
    assert_eq!(visible.value.bytes, bytes);
    Ok(())
}

#[test]
fn corrupt_and_partial_reads_are_exactly_scheduled() -> Result<(), Box<dyn std::error::Error>> {
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
    crate::async_storage::poll_ready(objects.put(
        object_id,
        bytes,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("simulated put suspended")??;
    let corrupt = crate::async_storage::poll_ready(objects.read(
        object_id,
        64,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("simulated read suspended")??;
    assert_ne!(
        object_digest(object_id.kind, &corrupt.value.bytes),
        object_id.digest
    );
    let partial = crate::async_storage::poll_ready(objects.read(
        object_id,
        64,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("simulated read suspended")??;
    assert_eq!(partial.value.bytes.len(), b"authenticated".len() - 1);
    Ok(())
}

#[test]
fn ambiguous_append_is_durable_and_retry_resolves_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let simulation = Simulation::default();
    simulation.schedule(ScheduledSimulationFault {
        operation: SimulationOperation::AuthorityAppend,
        occurrence: 1,
        fault: SimulationFault::AmbiguousAuthorityAppend,
    })?;
    let (authority, _) = simulation.stores();
    let cancellation = CancellationToken::new();
    let authority_id = AuthorityId::from_bytes([7; 16]);
    let created = crate::async_storage::poll_ready(authority.create_authority(
        authority_id,
        Epoch::GENESIS,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("simulated authority create suspended")??;
    let CreateAuthorityOutcome::Created(head) = created.value else {
        return Err("fresh simulated authority already existed".into());
    };
    let proposed = proposal(3, 4);
    let ambiguous_result = crate::async_storage::poll_ready(authority.compare_and_append(
        authority_id,
        Epoch::GENESIS,
        head,
        proposed.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("simulated append suspended")?;
    let Err(ambiguous) = ambiguous_result else {
        return Err("scheduled ambiguous append returned success".into());
    };
    assert!(matches!(
        ambiguous.error,
        AuthorityStoreError::Indeterminate { .. }
    ));
    let retry = crate::async_storage::poll_ready(authority.compare_and_append(
        authority_id,
        Epoch::GENESIS,
        head,
        proposed,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("simulated append retry suspended")??;
    assert!(matches!(retry.value, AppendOutcome::AlreadyCommitted(_)));
    Ok(())
}

#[test]
fn duplicate_replay_and_fence_change_are_observable_without_hidden_repair()
-> Result<(), Box<dyn std::error::Error>> {
    let simulation = Simulation::default();
    simulation.schedule(ScheduledSimulationFault {
        operation: SimulationOperation::AuthorityAppend,
        occurrence: 2,
        fault: SimulationFault::FenceBeforeAuthorityAppend,
    })?;
    simulation.schedule(ScheduledSimulationFault {
        operation: SimulationOperation::AuthorityReplay,
        occurrence: 1,
        fault: SimulationFault::DuplicateAuthorityReplay,
    })?;
    let (authority, _) = simulation.stores();
    let cancellation = CancellationToken::new();
    let authority_id = AuthorityId::from_bytes([8; 16]);
    let created = crate::async_storage::poll_ready(authority.create_authority(
        authority_id,
        Epoch::GENESIS,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("simulated authority create suspended")??;
    let CreateAuthorityOutcome::Created(head) = created.value else {
        return Err("fresh simulated authority already existed".into());
    };
    let committed = crate::async_storage::poll_ready(authority.compare_and_append(
        authority_id,
        Epoch::GENESIS,
        head,
        proposal(5, 6),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("simulated append suspended")??;
    let AppendOutcome::Committed(commit) = committed.value else {
        return Err("first simulated append did not commit".into());
    };
    let replay = crate::async_storage::poll_ready(authority.replay(
        authority_id,
        Sequence::GENESIS,
        ReplayLimit {
            records: 8,
            payload_bytes: 1_024,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("simulated replay suspended")??;
    assert_eq!(replay.value, vec![commit.clone(), commit.clone()]);
    let fenced = crate::async_storage::poll_ready(authority.compare_and_append(
        authority_id,
        Epoch::GENESIS,
        Head {
            epoch: commit.epoch,
            sequence: commit.sequence,
            digest: commit.digest,
        },
        proposal(7, 8),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("simulated fenced append suspended")??;
    assert!(matches!(fenced.value, AppendOutcome::Fenced { .. }));
    Ok(())
}

#[test]
fn fence_before_append_preserves_nested_budget_failure_work()
-> Result<(), Box<dyn std::error::Error>> {
    let simulation = Simulation::default();
    simulation.schedule(ScheduledSimulationFault {
        operation: SimulationOperation::AuthorityAppend,
        occurrence: 1,
        fault: SimulationFault::FenceBeforeAuthorityAppend,
    })?;
    let (authority, _) = simulation.stores();
    let cancellation = CancellationToken::new();
    let authority_id = AuthorityId::from_bytes([81; 16]);
    let created = crate::async_storage::poll_ready(authority.create_authority(
        authority_id,
        Epoch::GENESIS,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("simulated authority create suspended")??;
    let CreateAuthorityOutcome::Created(head) = created.value else {
        return Err("fresh simulated authority already existed".into());
    };
    let result = crate::async_storage::poll_ready(authority.compare_and_append(
        authority_id,
        Epoch::GENESIS,
        head,
        proposal(82, 83),
        WorkBudget {
            authority_records_appended: 1,
            backend_read_operations: 1,
            backend_write_operations: 1,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    ))
    .ok_or("simulated fenced append suspended")?;
    let failure = result
        .err()
        .ok_or("zero append budget unexpectedly published after fencing")?;
    assert!(
        matches!(
            failure.error,
            AuthorityStoreError::Work(WorkError::BudgetExceeded {
                counter: "backend_read_operations",
                maximum: 1,
                ..
            })
        ),
        "unexpected failure: {failure:?}"
    );
    assert_eq!(failure.work.backend_write_operations, 1);
    assert_eq!(failure.work.authority_records_appended, 1);
    Ok(())
}

#[test]
fn scheduling_is_bounded_typed_and_traceable() -> Result<(), Box<dyn std::error::Error>> {
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
    let rejected_result = crate::async_storage::poll_ready(objects.contains(
        object_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("simulated contains suspended")?;
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
    let second_result = crate::async_storage::poll_ready(objects.contains(
        object_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("second simulated contains suspended")?;
    let Err(second) = second_result else {
        return Err("scheduled next partition succeeded".into());
    };
    assert!(matches!(second.error, ObjectStoreError::Rejected(_)));
    assert_eq!(simulation.trace()?.len(), 2);
    Ok(())
}

fn assert_authority_before_work<T>(
    result: Option<AuthorityResult<T>>,
    cancelled: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let result = result.ok_or("simulated authority operation suspended")?;
    let Err(failure) = result else {
        return Err("simulated authority operation unexpectedly succeeded".into());
    };
    assert!(if cancelled {
        matches!(failure.error, AuthorityStoreError::Cancelled)
    } else {
        matches!(failure.error, AuthorityStoreError::Rejected(_))
    });
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

fn exercise_authority_before_work(
    fault: Option<SimulationFault>,
) -> Result<(), Box<dyn std::error::Error>> {
    let simulation = Simulation::default();
    if let Some(fault) = fault {
        for operation in [
            SimulationOperation::AuthorityCreate,
            SimulationOperation::AuthorityHead,
            SimulationOperation::AuthorityAppend,
            SimulationOperation::AuthorityReplay,
            SimulationOperation::AuthorityFence,
            SimulationOperation::AuthorityFindOperation,
        ] {
            simulation.schedule(ScheduledSimulationFault {
                operation,
                occurrence: 1,
                fault,
            })?;
        }
    }
    let (authority, _) = simulation.stores();
    let cancellation = CancellationToken::new();
    if fault.is_none() {
        cancellation.cancel();
    }
    let authority_id = AuthorityId::from_bytes([41; 16]);
    let operation_id = OperationId::from_bytes([42; 16]);
    let head = Head {
        epoch: Epoch::GENESIS,
        sequence: Sequence::GENESIS,
        digest: Digest::from_bytes([43; 32]),
    };
    let cancelled = fault.is_none();
    assert_authority_before_work(
        crate::async_storage::poll_ready(authority.create_authority(
            authority_id,
            Epoch::GENESIS,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )),
        cancelled,
    )?;
    assert_authority_before_work(
        crate::async_storage::poll_ready(authority.head(
            authority_id,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )),
        cancelled,
    )?;
    assert_authority_before_work(
        crate::async_storage::poll_ready(authority.compare_and_append(
            authority_id,
            Epoch::GENESIS,
            head,
            proposal(44, 45),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )),
        cancelled,
    )?;
    assert_authority_before_work(
        crate::async_storage::poll_ready(authority.replay(
            authority_id,
            Sequence::GENESIS,
            ReplayLimit {
                records: 1,
                payload_bytes: 64,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )),
        cancelled,
    )?;
    assert_authority_before_work(
        crate::async_storage::poll_ready(authority.fence(
            authority_id,
            head,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )),
        cancelled,
    )?;
    assert_authority_before_work(
        crate::async_storage::poll_ready(authority.find_operation(
            authority_id,
            operation_id,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )),
        cancelled,
    )?;
    assert!(simulation.trace()?.is_empty() == cancelled);
    Ok(())
}

fn assert_object_before_work<T>(
    result: Option<ObjectResult<T>>,
    cancelled: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let result = result.ok_or("simulated object operation suspended")?;
    let Err(failure) = result else {
        return Err("simulated object operation unexpectedly succeeded".into());
    };
    assert!(if cancelled {
        matches!(failure.error, ObjectStoreError::Cancelled)
    } else {
        matches!(failure.error, ObjectStoreError::Rejected(_))
    });
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

fn exercise_object_before_work(
    fault: Option<SimulationFault>,
) -> Result<(), Box<dyn std::error::Error>> {
    let simulation = Simulation::default();
    if let Some(fault) = fault {
        for operation in [
            SimulationOperation::ObjectPut,
            SimulationOperation::ObjectRead,
            SimulationOperation::ObjectReadMany,
            SimulationOperation::ObjectContains,
        ] {
            simulation.schedule(ScheduledSimulationFault {
                operation,
                occurrence: 1,
                fault,
            })?;
        }
    }
    let (_, objects) = simulation.stores();
    let cancellation = CancellationToken::new();
    if fault.is_none() {
        cancellation.cancel();
    }
    let (object_id, bytes) = object(b"fault-matrix");
    let requests = [ObjectReadRequest {
        object_id,
        maximum_bytes: 64,
    }];
    let cancelled = fault.is_none();
    assert_object_before_work(
        crate::async_storage::poll_ready(objects.put(
            object_id,
            bytes,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )),
        cancelled,
    )?;
    assert_object_before_work(
        crate::async_storage::poll_ready(objects.read(
            object_id,
            64,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )),
        cancelled,
    )?;
    assert_object_before_work(
        crate::async_storage::poll_ready(objects.read_many(
            &requests,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )),
        cancelled,
    )?;
    assert_object_before_work(
        crate::async_storage::poll_ready(objects.contains(
            object_id,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )),
        cancelled,
    )?;
    assert!(simulation.trace()?.is_empty() == cancelled);
    Ok(())
}

#[test]
fn every_operation_rejects_and_partitions_before_inner_work()
-> Result<(), Box<dyn std::error::Error>> {
    for fault in [
        SimulationFault::RejectBefore,
        SimulationFault::PartitionBefore,
    ] {
        exercise_authority_before_work(Some(fault))?;
        exercise_object_before_work(Some(fault))?;
    }
    Ok(())
}

#[test]
fn every_operation_honours_precancellation_before_inner_work()
-> Result<(), Box<dyn std::error::Error>> {
    exercise_authority_before_work(None)?;
    exercise_object_before_work(None)?;
    Ok(())
}

#[test]
fn stale_head_and_batch_read_faults_are_budgeted_and_exact()
-> Result<(), Box<dyn std::error::Error>> {
    let stale = Head {
        epoch: Epoch::GENESIS,
        sequence: Sequence::GENESIS,
        digest: Digest::from_bytes([51; 32]),
    };
    let simulation = Simulation::default();
    simulation.schedule(ScheduledSimulationFault {
        operation: SimulationOperation::AuthorityHead,
        occurrence: 1,
        fault: SimulationFault::StaleAuthorityHead(stale),
    })?;
    simulation.schedule(ScheduledSimulationFault {
        operation: SimulationOperation::AuthorityHead,
        occurrence: 2,
        fault: SimulationFault::StaleAuthorityHead(stale),
    })?;
    let (authority, objects) = simulation.stores();
    let cancellation = CancellationToken::new();
    let authority_id = AuthorityId::from_bytes([52; 16]);
    let observed = crate::async_storage::poll_ready(authority.head(
        authority_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("stale head suspended")??;
    assert_eq!(observed.value, stale);
    assert_eq!(observed.work.authority_records_read, 1);
    let too_small = crate::async_storage::poll_ready(authority.head(
        authority_id,
        WorkBudget::default(),
        &cancellation,
    ))
    .ok_or("bounded stale head suspended")?;
    assert!(matches!(
        too_small,
        Err(AuthorityFailure {
            error: AuthorityStoreError::Work(_),
            ..
        })
    ));

    let (object_id, bytes) = object(b"batch-fault");
    crate::async_storage::poll_ready(objects.put(
        object_id,
        bytes,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("object put suspended")??;
    let requests = [ObjectReadRequest {
        object_id,
        maximum_bytes: 64,
    }];
    for (occurrence, fault) in [
        (1, SimulationFault::CorruptObjectRead),
        (2, SimulationFault::PartialObjectRead),
    ] {
        simulation.schedule(ScheduledSimulationFault {
            operation: SimulationOperation::ObjectReadMany,
            occurrence,
            fault,
        })?;
    }
    let corrupt = crate::async_storage::poll_ready(objects.read_many(
        &requests,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("corrupt batch read suspended")??;
    assert_ne!(
        object_digest(object_id.kind, &corrupt.value[0].bytes),
        object_id.digest
    );
    let partial = crate::async_storage::poll_ready(objects.read_many(
        &requests,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("partial batch read suspended")??;
    assert_eq!(partial.value[0].bytes.len(), b"batch-fault".len() - 1);
    Ok(())
}

#[test]
fn simulator_bounds_duplicate_schedule_and_trace_exhaustion_are_fail_closed()
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
        crate::async_storage::poll_ready(objects.contains(
            object_id,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("scheduled trace operation suspended")?
        .is_err()
    );
    let exhausted = crate::async_storage::poll_ready(objects.contains(
        object_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("trace exhaustion suspended")?
    .err()
    .ok_or("trace exhaustion was ignored")?;
    assert!(matches!(exhausted.error, ObjectStoreError::Rejected(_)));
    assert_eq!(simulation.trace()?.len(), 1);
    Ok(())
}

#[test]
fn delayed_object_admission_is_idempotent_bounded_and_digest_checked()
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
    let digest_failure = crate::async_storage::poll_ready(objects.put(
        first_id,
        Bytes::from_static(b"bad!"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("digest mismatch put suspended")?
    .err()
    .ok_or("digest mismatch was admitted")?;
    assert!(matches!(
        digest_failure.error,
        ObjectStoreError::DigestMismatch
    ));
    crate::async_storage::poll_ready(objects.put(
        first_id,
        first_bytes.clone(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("delayed put suspended")??;
    let repeated = crate::async_storage::poll_ready(objects.put(
        first_id,
        first_bytes,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("idempotent delayed put suspended")??;
    assert_eq!(repeated.work.backend_write_operations, 1);

    let (second_id, second_bytes) = object(b"x");
    let bounded = crate::async_storage::poll_ready(objects.put(
        second_id,
        second_bytes,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("bounded delayed put suspended")?
    .err()
    .ok_or("delayed-object bound was ignored")?;
    assert!(matches!(bounded.error, ObjectStoreError::Rejected(_)));
    let flushed = crate::async_storage::poll_ready(
        simulation.flush_object_visibility(WorkBudget::UNBOUNDED, &cancellation),
    )
    .ok_or("delayed object flush suspended")??;
    assert_eq!(flushed.value, 1);
    Ok(())
}

#[test]
fn empty_corrupt_reads_and_authority_passthroughs_remain_typed()
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
    crate::async_storage::poll_ready(objects.put(
        empty_id,
        empty_bytes,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("empty object put suspended")??;
    let corrupt = crate::async_storage::poll_ready(objects.read(
        empty_id,
        0,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("empty corrupt read suspended")?
    .err()
    .ok_or("empty corrupt read succeeded")?;
    assert!(matches!(corrupt.error, ObjectStoreError::Corrupt));

    let authority_id = AuthorityId::from_bytes([71; 16]);
    crate::async_storage::poll_ready(authority.create_authority(
        authority_id,
        Epoch::GENESIS,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("authority creation suspended")??;
    let missing = crate::async_storage::poll_ready(authority.find_operation(
        authority_id,
        OperationId::from_bytes([72; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("operation lookup suspended")??;
    assert!(missing.value.is_none());
    let head = crate::async_storage::poll_ready(authority.head(
        authority_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("authority head suspended")??
    .value;
    let fenced = crate::async_storage::poll_ready(authority.fence(
        authority_id,
        head,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("authority fence suspended")??;
    assert!(matches!(
        fenced.value,
        FenceOutcome::Advanced(advanced) if advanced.epoch.get() == 2
    ));
    Ok(())
}

#[test]
fn poisoned_simulation_authority_and_object_surfaces_fail_closed()
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
    let authority_failure = crate::async_storage::poll_ready(authority.head(
        AuthorityId::from_bytes([80; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("poisoned authority lookup unexpectedly suspended")?
    .err()
    .ok_or("poisoned authority lookup unexpectedly succeeded")?;
    assert!(matches!(
        authority_failure.error,
        AuthorityStoreError::Corrupt(_)
    ));
    let object_failure = crate::async_storage::poll_ready(objects.contains(
        object(b"poisoned").0,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("poisoned object lookup unexpectedly suspended")?
    .err()
    .ok_or("poisoned object lookup unexpectedly succeeded")?;
    assert!(matches!(object_failure.error, ObjectStoreError::Corrupt));
    Ok(())
}

#[test]
fn corrupt_delayed_visibility_state_preserves_nested_failure_work()
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
            .insert(object_id, Bytes::from_static(b"forged"));
        state.pending_object_bytes = 6;
    }
    let failure = crate::async_storage::poll_ready(
        simulation.flush_object_visibility(WorkBudget::UNBOUNDED, &cancellation),
    )
    .ok_or("corrupt delayed flush unexpectedly suspended")?
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
    let failure = crate::async_storage::poll_ready(
        simulation.flush_object_visibility(WorkBudget::UNBOUNDED, &cancellation),
    )
    .ok_or("underflowing delayed flush unexpectedly suspended")?
    .err()
    .ok_or("underflowing delayed accounting unexpectedly succeeded")?;
    assert!(matches!(failure.error, ObjectStoreError::Corrupt));
    assert_eq!(failure.work.backend_write_operations, 1);
    Ok(())
}
