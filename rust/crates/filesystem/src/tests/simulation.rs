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
        matches!(failure.error, AuthorityStoreError::RejectedStatic(_))
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
        AuthorityStoreError::CorruptStatic(_)
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

#[tokio::test]
async fn reservation_lost_ack_and_readonly_query_faults_use_original_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    let simulation = Simulation::default();
    let (authority, _) = simulation.stores();
    let cancellation = CancellationToken::new();
    let authority_id = AuthorityId::from_bytes([0xc1; 16]);
    let operation_id = OperationId::from_bytes([0xc2; 16]);
    authority.create_authority(authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED, &cancellation).await?;
    let original = authority.head(authority_id, WorkBudget::UNBOUNDED, &cancellation).await?.value;
    simulation.schedule(ScheduledSimulationFault {
        operation: SimulationOperation::AuthorityReserve, occurrence: 1,
        fault: SimulationFault::AmbiguousAuthorityAppend,
    })?;
    let lost = authority.reserve_publication(
        authority_id, original, operation_id, WorkBudget::UNBOUNDED, &cancellation,
    ).await.expect_err("real acquisition committed before the scheduled acknowledgement loss");
    assert!(matches!(lost.error, AuthorityStoreError::Indeterminate { operation: "reserve-publication", .. }));
    assert_eq!(lost.work.backend_write_operations, 1);
    let reservation = authority.publication_reservation(
        authority_id, operation_id, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value.ok_or("original reservation evidence missing")?;
    assert_eq!(reservation.authority_id, authority_id);
    assert_eq!(reservation.operation_id, operation_id);
    assert_eq!(reservation.expected, original);
    simulation.schedule(ScheduledSimulationFault {
        operation: SimulationOperation::AuthorityReservation, occurrence: 2,
        fault: SimulationFault::PartitionBefore,
    })?;
    let partitioned = authority.publication_reservation(
        authority_id, operation_id, WorkBudget::UNBOUNDED, &cancellation,
    ).await.expect_err("scheduled actual readonly query is intercepted before provider I/O");
    assert!(matches!(partitioned.error, AuthorityStoreError::RejectedStatic(_)));
    assert_eq!(*partitioned.work, WorkCounters::UNCHARGED);
    simulation.schedule(ScheduledSimulationFault {
        operation: SimulationOperation::AuthorityRelease, occurrence: 1,
        fault: SimulationFault::AmbiguousAuthorityAppend,
    })?;
    let released = authority.release_publication(
        reservation, WorkBudget::UNBOUNDED, &cancellation,
    ).await.expect_err("real release committed before acknowledgement loss");
    assert!(matches!(released.error, AuthorityStoreError::Indeterminate { operation: "release-publication", .. }));
    authority.release_publication(reservation, WorkBudget::UNBOUNDED, &cancellation).await?;
    assert_eq!(authority.publication_reservation(
        authority_id, operation_id, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value, None);
    let trace = simulation.trace()?;
    assert_eq!(trace.iter().filter(|entry| entry.operation == SimulationOperation::AuthorityReserve).count(), 1);
    assert_eq!(trace.iter().filter(|entry| entry.operation == SimulationOperation::AuthorityRelease).count(), 2);
    assert!(trace.iter().any(|entry| entry.operation == SimulationOperation::AuthorityReservation
        && entry.fault == Some(SimulationFault::PartitionBefore)));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn original_authority_budget() -> WorkBudget {
    WorkBudget {
        authority_records_read: 1_024,
        authority_records_appended: 1_024,
        authority_bytes_read: 1_048_576,
        authority_bytes_written: 1_048_576,
        backend_read_operations: 1_024,
        backend_write_operations: 1_024,
        durability_operations: 1_024,
        bytes_hashed: 1_048_576,
        bytes_copied: 1_048_576,
        bytes_encoded: 1_048_576,
        items_examined: 1_024,
        items_returned: 1_024,
        allocation_operations: 1_024,
        peak_allocation_bytes: 1_048_576,
        ..WorkBudget::default()
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn exercise_original_authority<A: AsyncAuthorityStore>(
    backend: A,
) -> Result<(), Box<dyn std::error::Error>> {
    use crate::materializer::NativeMaterializationWork;
    let simulation = Simulation::wrap(backend, (), SimulationOptions::default())?;
    let (authority, _) = simulation.stores();
    let cancellation = CancellationToken::new();
    let budget = original_authority_budget();
    let id = AuthorityId::from_bytes([0xd1; 16]);
    authority.create_authority(id, Epoch::GENESIS, budget, &cancellation).await?;
    let initial = authority.head(id, budget, &cancellation).await?.value;
    let proposed = ProposedCommit {
        payload: Bytes::from(vec![0x5a; 8_192]),
        ..proposal(0xd2, 0xd3)
    };
    let AppendOutcome::Committed(commit) = authority.compare_and_append(
        id, Epoch::GENESIS, initial, proposed, budget, &cancellation,
    ).await?.value else { return Err("seed append did not commit".into()); };
    let expected = authority.head(id, budget, &cancellation).await?.value;
    let work = NativeMaterializationWork::new(budget, &cancellation).map_err(|(error, _)| error)?;
    work.charge(WorkCounters { items_examined: 3, ..WorkCounters::UNCHARGED })?;
    let before = work.receipt();
    assert_eq!(authority.head_with_native_work(id, &work, &cancellation).await?, expected);
    assert!(work.receipt().backend_read_operations > before.backend_read_operations);
    assert!(work.receipt().items_examined >= 3);
    let marker = work.claim(0)?;
    let (found, found_custody) = authority.find_operation_with_native_work(
        id, commit.operation_id, &work, &cancellation,
    ).await?;
    assert_eq!(found, Some(commit.clone()));
    assert!(found_custody.shares_ledger(&marker));
    drop(found);
    drop(found_custody);
    let limit = ReplayLimit { records: 1, payload_bytes: 16_384 };
    let (unfaulted, unfaulted_custody) = authority.replay_with_native_work(
        id, Sequence::GENESIS, limit, &work, &cancellation,
    ).await?;
    assert_eq!(unfaulted, vec![commit.clone()]);
    assert!(unfaulted_custody.shares_ledger(&marker));
    drop(unfaulted);
    drop(unfaulted_custody);
    let baseline_work = NativeMaterializationWork::new(budget, &cancellation).map_err(|(error, _)| error)?;
    let (baseline, baseline_custody) = simulation.authority.replay_with_native_work(
        id, Sequence::GENESIS, limit, &baseline_work, &cancellation,
    ).await?;
    assert_eq!(baseline.len(), 1);
    assert_eq!(baseline.capacity(), 1);
    let baseline_receipt = baseline_work.receipt();
    let baseline_bytes = baseline_custody.bytes();
    drop(baseline);
    drop(baseline_custody);
    simulation.schedule_next(SimulationOperation::AuthorityReplay, SimulationFault::DuplicateAuthorityReplay)?;
    let duplicate_work = NativeMaterializationWork::new(budget, &cancellation).map_err(|(error, _)| error)?;
    let (duplicated, duplicated_custody) = authority.replay_with_native_work(
        id, Sequence::GENESIS, limit, &duplicate_work, &cancellation,
    ).await?;
    assert_eq!(duplicated, vec![commit.clone(), commit.clone()]);
    assert_eq!(duplicated[0].payload.as_ptr(), duplicated[1].payload.as_ptr());
    let metadata_bytes = std::mem::size_of::<DurableCommit>() as u64;
    assert_eq!(duplicated_custody.bytes(), baseline_bytes + metadata_bytes);
    assert_eq!(duplicate_work.receipt().bytes_copied, baseline_receipt.bytes_copied + 2 * metadata_bytes);
    assert_eq!(duplicate_work.receipt().items_returned, baseline_receipt.items_returned + 1);
    assert_eq!(duplicate_work.receipt().backend_read_operations, baseline_receipt.backend_read_operations);
    assert_eq!(duplicate_work.receipt().authority_records_read, baseline_receipt.authority_records_read);
    assert_eq!(duplicate_work.receipt().allocation_operations, baseline_receipt.allocation_operations + 1);
    assert!(duplicate_work.receipt().peak_allocation_bytes >=
        NativeMaterializationWork::accounting_state_bytes() + baseline_bytes + 2 * metadata_bytes);
    let duplicate_marker = duplicate_work.claim(0)?;
    assert!(duplicated_custody.shares_ledger(&duplicate_marker));
    drop(duplicated);
    drop(duplicated_custody);

    // Backend production succeeds, but simulator metadata growth is denied.
    // The original meter retains the exact physical backend prefix.
    simulation.schedule_next(SimulationOperation::AuthorityReplay, SimulationFault::DuplicateAuthorityReplay)?;
    let denied_budget = WorkBudget {
        allocation_operations: baseline_receipt.allocation_operations,
        ..budget
    };
    let denied_work = NativeMaterializationWork::new(denied_budget, &cancellation).map_err(|(error, _)| error)?;
    let error = authority.replay_with_native_work(
        id, Sequence::GENESIS, limit, &denied_work, &cancellation,
    ).await.expect_err("duplicate metadata allocation must be admitted before production");
    assert!(matches!(error, AuthorityStoreError::NativeOriginal(
        crate::materializer::NativeTreeMaterializationError::Work(WorkError::BudgetExceeded { .. })
    )));
    assert_eq!(denied_work.receipt(), baseline_receipt);
    let (live, empty) = denied_work.with_owned_allocations(|ledger, _, _, _| {
        Ok::<_, crate::materializer::NativeTreeMaterializationError>(ledger.live_bytes())
    })?;
    assert_eq!(live, NativeMaterializationWork::accounting_state_bytes());
    drop(empty);

    let fence_before = work.receipt();
    let fenced = authority.fence_with_native_work(id, expected, &work, &cancellation).await?;
    let FenceOutcome::Advanced(advanced) = fenced else { return Err("original fence did not advance".into()); };
    assert!(work.receipt().backend_write_operations > fence_before.backend_write_operations);
    assert_eq!(authority.head(id, budget, &cancellation).await?.value, advanced);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_authority_family_forwards_memory_and_stream_with_exact_fault_custody()
-> Result<(), Box<dyn std::error::Error>> {
    exercise_original_authority(crate::MemoryAuthorityStore::default()).await?;
    exercise_original_authority(reference_backends().0).await?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_authority_synthetic_and_before_work_faults_do_not_invent_backend_reads()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::materializer::NativeMaterializationWork;
    let cancellation = CancellationToken::new();
    let budget = original_authority_budget();
    let id = AuthorityId::from_bytes([0xe1; 16]);
    let stale = Head { epoch: Epoch::GENESIS, sequence: Sequence::GENESIS, digest: Digest::from_bytes([0xe2; 32]) };
    let simulation = Simulation::default();
    let (authority, _) = simulation.stores();
    let work = NativeMaterializationWork::new(budget, &cancellation).map_err(|(error, _)| error)?;
    let before = work.receipt();
    simulation.schedule_next(SimulationOperation::AuthorityHead, SimulationFault::StaleAuthorityHead(stale))?;
    assert_eq!(authority.head_with_native_work(id, &work, &cancellation).await?, stale);
    assert_eq!(work.receipt(), before);
    for fault in [SimulationFault::RejectBefore, SimulationFault::PartitionBefore] {
        for operation in [SimulationOperation::AuthorityHead, SimulationOperation::AuthorityReplay,
            SimulationOperation::AuthorityFence, SimulationOperation::AuthorityFindOperation] {
            simulation.schedule_next(operation, fault)?;
        }
        assert!(matches!(authority.head_with_native_work(id, &work, &cancellation).await,
            Err(AuthorityStoreError::RejectedStatic(_))));
        assert!(matches!(authority.replay_with_native_work(id, Sequence::GENESIS,
            ReplayLimit { records: 1, payload_bytes: 64 }, &work, &cancellation).await,
            Err(AuthorityStoreError::RejectedStatic(_))));
        assert!(matches!(authority.fence_with_native_work(id, stale, &work, &cancellation).await,
            Err(AuthorityStoreError::RejectedStatic(_))));
        assert!(matches!(authority.find_operation_with_native_work(id, OperationId::from_bytes([0xe3; 16]),
            &work, &cancellation).await, Err(AuthorityStoreError::RejectedStatic(_))));
        assert_eq!(work.receipt(), before);
    }
    let trace_count = simulation.trace()?.len();
    cancellation.cancel();
    assert!(matches!(authority.head_with_native_work(id, &work, &cancellation).await,
        Err(AuthorityStoreError::Cancelled)));
    assert!(matches!(authority.replay_with_native_work(id, Sequence::GENESIS,
        ReplayLimit { records: 1, payload_bytes: 64 }, &work, &cancellation).await,
        Err(AuthorityStoreError::Cancelled)));
    assert!(matches!(authority.fence_with_native_work(id, stale, &work, &cancellation).await,
        Err(AuthorityStoreError::Cancelled)));
    assert!(matches!(authority.find_operation_with_native_work(id, OperationId::from_bytes([0xe3; 16]),
        &work, &cancellation).await, Err(AuthorityStoreError::Cancelled)));
    assert_eq!(simulation.trace()?.len(), trace_count);
    assert_eq!(work.receipt(), before);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
struct OpaqueOriginalAuthority {
    inner: crate::MemoryAuthorityStore,
    ordinary_calls: std::sync::atomic::AtomicUsize,
}

#[cfg(not(target_arch = "wasm32"))]
impl crate::storage::AuthorityStore for OpaqueOriginalAuthority {
    fn create_authority(&self, id: AuthorityId, epoch: Epoch, budget: WorkBudget) -> AuthorityResult<CreateAuthorityOutcome> {
        self.ordinary_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        crate::storage::AuthorityStore::create_authority(&self.inner, id, epoch, budget)
    }
    fn head(&self, id: AuthorityId, budget: WorkBudget) -> AuthorityResult<Head> {
        self.ordinary_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        crate::storage::AuthorityStore::head(&self.inner, id, budget)
    }
    fn compare_and_append(&self, id: AuthorityId, epoch: Epoch, expected: Head, commit: ProposedCommit, budget: WorkBudget) -> AuthorityResult<AppendOutcome> {
        self.ordinary_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        crate::storage::AuthorityStore::compare_and_append(&self.inner, id, epoch, expected, commit, budget)
    }
    fn replay(&self, id: AuthorityId, after: Sequence, limit: ReplayLimit, budget: WorkBudget) -> AuthorityResult<Vec<DurableCommit>> {
        self.ordinary_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        crate::storage::AuthorityStore::replay(&self.inner, id, after, limit, budget)
    }
    fn fence(&self, id: AuthorityId, expected: Head, budget: WorkBudget) -> AuthorityResult<FenceOutcome> {
        self.ordinary_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        crate::storage::AuthorityStore::fence(&self.inner, id, expected, budget)
    }
    fn find_operation(&self, id: AuthorityId, operation: OperationId, budget: WorkBudget) -> AuthorityResult<Option<DurableCommit>> {
        self.ordinary_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        crate::storage::AuthorityStore::find_operation(&self.inner, id, operation, budget)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl crate::async_storage::ImmediateAuthorityStore for OpaqueOriginalAuthority {}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_simulation_propagates_opaque_backend_rejection_before_ordinary_io()
-> Result<(), Box<dyn std::error::Error>> {
    let simulation = Simulation::wrap(OpaqueOriginalAuthority {
        inner: crate::MemoryAuthorityStore::default(),
        ordinary_calls: std::sync::atomic::AtomicUsize::new(0),
    }, (), SimulationOptions::default())?;
    let (authority, _) = simulation.stores();
    let cancellation = CancellationToken::new();
    let work = crate::materializer::NativeMaterializationWork::new(
        original_authority_budget(), &cancellation,
    ).map_err(|(error, _)| error)?;
    let before = work.receipt();
    let id = AuthorityId::from_bytes([0xf1; 16]);
    let expected = Head { epoch: Epoch::GENESIS, sequence: Sequence::GENESIS, digest: Digest::from_bytes([0xf2; 32]) };
    assert!(matches!(authority.head_with_native_work(id, &work, &cancellation).await,
        Err(AuthorityStoreError::NativeOriginalUnsupported)));
    assert!(matches!(authority.replay_with_native_work(id, Sequence::GENESIS,
        ReplayLimit { records: 1, payload_bytes: 64 }, &work, &cancellation).await,
        Err(AuthorityStoreError::NativeOriginalUnsupported)));
    assert!(matches!(authority.fence_with_native_work(id, expected, &work, &cancellation).await,
        Err(AuthorityStoreError::NativeOriginalUnsupported)));
    assert!(matches!(authority.find_operation_with_native_work(id, OperationId::from_bytes([0xf3; 16]),
        &work, &cancellation).await, Err(AuthorityStoreError::NativeOriginalUnsupported)));
    assert_eq!(simulation.authority.ordinary_calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert_eq!(work.receipt(), before);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn original_proposal(
    operation: u8,
    fingerprint: u8,
    original: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
) -> Result<ProposedCommit, Box<dyn std::error::Error>> {
    let mut allocation = original.claim(2)?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(2)?;
    original.charge(WorkCounters { bytes_copied: 2, ..WorkCounters::UNCHARGED })?;
    bytes.extend_from_slice(&[operation, fingerprint]);
    let object = crate::HashedObject::from_native_vec(
        ObjectKind::BlobChunk, &mut bytes, &mut allocation, original, cancellation,
    )?;
    let (_, payload) = object.into_parts();
    Ok(ProposedCommit {
        operation_id: OperationId::from_bytes([operation; 16]),
        fingerprint: Digest::from_bytes([fingerprint; 32]),
        payload,
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn original_append(id: AuthorityId, expected: Head, commit: ProposedCommit) -> crate::GuardedAppend {
    crate::GuardedAppend {
        authority_id: id,
        epoch: expected.epoch,
        expected,
        commit,
        permit: crate::PublicationPermit::Unrestricted,
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
enum SimulationOwnerFailure {
    Authority(AuthorityStoreError),
    Stopped(u64),
}

#[cfg(not(target_arch = "wasm32"))]
impl From<AuthorityStoreError> for SimulationOwnerFailure {
    fn from(error: AuthorityStoreError) -> Self { Self::Authority(error) }
}

#[cfg(not(target_arch = "wasm32"))]
async fn exercise_original_simulation_mutations<A: AsyncAuthorityStore>(
    backend: A,
) -> Result<(), Box<dyn std::error::Error>> {
    use crate::NativeMaterializationWork;
    let simulation = Simulation::wrap(backend, (), SimulationOptions::default())?;
    let (authority, _) = simulation.stores();
    let cancellation = CancellationToken::new();
    let budget = original_authority_budget();
    let original = NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(error, _)| error)?;
    original.charge(WorkCounters { items_examined: 7, ..WorkCounters::UNCHARGED })?;
    let marker = original.claim(0)?;
    let id = AuthorityId::from_bytes([0xb1; 16]);
    let first = original_proposal(0xb2, 0xb3, &original, &cancellation)?;
    simulation.schedule_next(
        SimulationOperation::AuthorityCreate, SimulationFault::AmbiguousAuthorityAppend,
    )?;
    let before_creation = original.receipt();
    let lost_creation = authority.create_authority_with_first_record_with_native_work(
        id, first.clone(), &original, &cancellation,
    ).await.expect_err("the first record is atomic before its acknowledgement is lost");
    assert!(matches!(lost_creation, AuthorityStoreError::Indeterminate {
        operation: "create-authority-with-first-record", source,
    } if source.kind() == std::io::ErrorKind::ConnectionReset));
    assert!(original.receipt().authority_records_appended > before_creation.authority_records_appended);
    assert!(original.receipt().items_examined >= 7);
    let initial = authority.head_with_native_work(id, &original, &cancellation).await?;
    let (found, found_custody) = authority.find_operation_with_native_work(
        id, first.operation_id, &original, &cancellation,
    ).await?;
    assert!(marker.shares_ledger(&found_custody));
    assert_eq!(found.as_ref().map(|record| &record.payload), Some(&first.payload));
    simulation.schedule_next(
        SimulationOperation::AuthorityCreate, SimulationFault::AmbiguousAuthorityAppend,
    )?;
    assert!(matches!(authority.create_authority_with_first_record_with_native_work(
        id, first.clone(), &original, &cancellation,
    ).await, Err(AuthorityStoreError::Indeterminate { .. })));
    assert!(authority.create_authority_with_first_record_with_native_work(
        id, first.clone(), &original, &cancellation,
    ).await?);
    assert!(authority.create_authority_with_first_record(
        id, first, budget, &cancellation,
    ).await?.value);
    assert_eq!(authority.head(id, budget, &cancellation).await?.value, initial);
    let trace = simulation.trace()?;
    assert_eq!(trace.iter().filter(|entry| entry.operation == SimulationOperation::AuthorityCreate).count(), 4);
    assert!(!trace.iter().any(|entry| entry.operation == SimulationOperation::AuthorityAppend));
    let different_first = original_proposal(0xbd, 0xbe, &original, &cancellation)?;
    simulation.schedule_next(
        SimulationOperation::AuthorityCreate, SimulationFault::AmbiguousAuthorityAppend,
    )?;
    assert!(!authority.create_authority_with_first_record_with_native_work(
        id, different_first, &original, &cancellation,
    ).await?, "a mismatched record is not an accepted acknowledgement to lose");

    let second = original_proposal(0xb4, 0xb5, &original, &cancellation)?;
    simulation.schedule_next(
        SimulationOperation::AuthorityAppend, SimulationFault::AmbiguousAuthorityAppend,
    )?;
    let before_append = original.receipt();
    let lost_append = authority.compare_and_append_guarded_with_native_work(
        original_append(id, initial, second.clone()), &original, &cancellation,
    ).await.expect_err("a real guarded append precedes acknowledgement loss");
    assert!(matches!(lost_append, AuthorityStoreError::Indeterminate {
        operation: "compare-and-append", source,
    } if source.kind() == std::io::ErrorKind::ConnectionReset));
    assert!(original.receipt().authority_records_appended > before_append.authority_records_appended);
    let appended = authority.head_with_native_work(id, &original, &cancellation).await?;
    simulation.schedule_next(
        SimulationOperation::AuthorityAppend, SimulationFault::AmbiguousAuthorityAppend,
    )?;
    assert!(matches!(authority.compare_and_append_guarded(
        original_append(id, initial, second.clone()), budget, &cancellation,
    ).await.expect_err("the accepted idempotent attempt can lose its acknowledgement").error,
        AuthorityStoreError::Indeterminate { .. }));
    let ordinary_retry = authority.compare_and_append_guarded(
        original_append(id, initial, second.clone()), budget, &cancellation,
    ).await?;
    assert!(matches!(ordinary_retry.value, AppendOutcome::AlreadyCommitted(_)));
    simulation.schedule_next(
        SimulationOperation::AuthorityAppend, SimulationFault::AmbiguousAuthorityAppend,
    )?;
    assert!(matches!(authority.compare_and_append_guarded_with_native_work(
        original_append(id, initial, second.clone()), &original, &cancellation,
    ).await, Err(AuthorityStoreError::Indeterminate { .. })));
    let (native_retry, retry_custody) = authority.compare_and_append_guarded_with_native_work(
        original_append(id, initial, second), &original, &cancellation,
    ).await?;
    assert!(matches!(native_retry, AppendOutcome::AlreadyCommitted(_)));
    assert!(marker.shares_ledger(&retry_custody));

    let third = original_proposal(0xb6, 0xb7, &original, &cancellation)?;
    let before_guard = original.receipt();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let callback_calls = Arc::clone(&calls);
    let callback_work = marker.work().clone();
    let callback_state = Arc::clone(&simulation.state);
    let callback_authority = Arc::clone(&simulation.authority);
    let callback_cancellation = cancellation.clone();
    let mut denied_guard = move |root: &NativeMaterializationWork| {
        callback_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        assert!(root.shares_ledger(&callback_work));
        assert!(root.receipt().backend_read_operations > before_guard.backend_read_operations);
        drop(callback_state.try_lock().expect("the simulator lock is not held at sealing"));
        let inner = Arc::clone(&callback_authority);
        let cancellation = callback_cancellation.clone();
        let root = root.clone();
        async move {
            assert_eq!(inner.head_with_native_work(id, &root, &cancellation).await?, appended);
            Err::<(), _>(SimulationOwnerFailure::Stopped(0x1234))
        }
    };
    let denied = authority.compare_and_append_guarded_with_native_owner(
        original_append(id, appended, third.clone()), &original, &cancellation, &mut denied_guard,
    ).await.expect_err("the actual producer preserves the owner's error");
    assert!(matches!(denied, SimulationOwnerFailure::Stopped(0x1234)));
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(original.receipt().backend_write_operations, before_guard.backend_write_operations);
    assert_eq!(authority.head_with_native_work(id, &original, &cancellation).await?, appended);
    let callback_work = marker.work().clone();
    let mut accepted_guard = move |root: &NativeMaterializationWork| {
        assert!(root.shares_ledger(&callback_work));
        std::future::ready(Ok::<(), AuthorityStoreError>(()))
    };
    let (published, published_custody) = authority.compare_and_append_guarded_with_native_owner(
        original_append(id, appended, third), &original, &cancellation, &mut accepted_guard,
    ).await?;
    let AppendOutcome::Committed(published) = published else {
        return Err("the accepted original owner did not publish".into());
    };
    assert!(marker.shares_ledger(&published_custody));
    let expected = Head { epoch: published.epoch, sequence: published.sequence, digest: published.digest };
    simulation.schedule_next(
        SimulationOperation::AuthorityAppend, SimulationFault::FenceBeforeAuthorityAppend,
    )?;
    let fourth = original_proposal(0xb8, 0xb9, &original, &cancellation)?;
    let (fenced, fenced_custody) = authority.compare_and_append_guarded_with_native_owner(
        original_append(id, expected, fourth), &original, &cancellation, &mut accepted_guard,
    ).await?;
    assert!(matches!(fenced, AppendOutcome::Fenced { .. }));
    assert!(marker.shares_ledger(&fenced_custody));
    let current = authority.head_with_native_work(id, &original, &cancellation).await?;
    let mismatched = Head { digest: Digest::from_bytes([0xbf; 32]), ..current };
    let conflict = original_proposal(0xc5, 0xc6, &original, &cancellation)?;
    simulation.schedule_next(
        SimulationOperation::AuthorityAppend, SimulationFault::AmbiguousAuthorityAppend,
    )?;
    let (conflict, conflict_custody) = authority.compare_and_append_guarded_with_native_owner(
        original_append(id, mismatched, conflict), &original, &cancellation, &mut accepted_guard,
    ).await?;
    assert!(matches!(conflict, AppendOutcome::Conflict { .. }));
    assert!(marker.shares_ledger(&conflict_custody));

    let mut denied_budget = budget;
    denied_budget.authority_records_appended = 0;
    let denied_original = NativeMaterializationWork::new(denied_budget, &cancellation)
        .map_err(|(error, _)| error)?;
    let denied_id = AuthorityId::from_bytes([0xba; 16]);
    let denied_first = original_proposal(0xbb, 0xbc, &denied_original, &cancellation)?;
    assert!(authority.create_authority_with_first_record_with_native_work(
        denied_id, denied_first, &denied_original, &cancellation,
    ).await.is_err());
    assert_eq!(denied_original.receipt().backend_write_operations, 0);
    assert!(matches!(simulation.authority.head(
        denied_id, budget, &cancellation,
    ).await.expect_err("denied atomic creation leaves no genesis authority").error,
        AuthorityStoreError::Missing));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_simulation_creation_append_and_owner_use_both_canonical_stock_engines()
-> Result<(), Box<dyn std::error::Error>> {
    exercise_original_simulation_mutations(crate::MemoryAuthorityStore::default()).await?;
    exercise_original_simulation_mutations(reference_backends().0).await?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_simulation_mutation_faults_and_unsupported_providers_never_call_ordinary_io()
-> Result<(), Box<dyn std::error::Error>> {
    let simulation = Simulation::wrap(OpaqueOriginalAuthority {
        inner: crate::MemoryAuthorityStore::default(),
        ordinary_calls: std::sync::atomic::AtomicUsize::new(0),
    }, (), SimulationOptions::default())?;
    let (authority, _) = simulation.stores();
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(original_authority_budget(), &cancellation)
        .map_err(|(error, _)| error)?;
    let id = AuthorityId::from_bytes([0xc1; 16]);
    let expected = Head {
        epoch: Epoch::GENESIS, sequence: Sequence::GENESIS, digest: Digest::from_bytes([0xc2; 32]),
    };
    let proposed = original_proposal(0xc3, 0xc4, &original, &cancellation)?;
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let callback_calls = Arc::clone(&calls);
    let mut guard = move |_root: &crate::NativeMaterializationWork| {
        callback_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::future::ready(Ok::<(), SimulationOwnerFailure>(()))
    };
    for fault in [SimulationFault::RejectBefore, SimulationFault::PartitionBefore] {
        simulation.schedule_next(SimulationOperation::AuthorityCreate, fault)?;
        assert!(matches!(authority.create_authority_with_first_record_with_native_work(
            id, proposed.clone(), &original, &cancellation,
        ).await, Err(AuthorityStoreError::RejectedStatic(_))));
        simulation.schedule_next(SimulationOperation::AuthorityAppend, fault)?;
        assert!(matches!(authority.compare_and_append_guarded_with_native_work(
            original_append(id, expected, proposed.clone()), &original, &cancellation,
        ).await, Err(AuthorityStoreError::RejectedStatic(_))));
        simulation.schedule_next(SimulationOperation::AuthorityAppend, fault)?;
        assert!(matches!(authority.compare_and_append_guarded_with_native_owner(
            original_append(id, expected, proposed.clone()), &original, &cancellation, &mut guard,
        ).await, Err(SimulationOwnerFailure::Authority(AuthorityStoreError::RejectedStatic(_)))));
    }
    assert!(matches!(authority.create_authority_with_first_record_with_native_work(
        id, proposed.clone(), &original, &cancellation,
    ).await, Err(AuthorityStoreError::NativeOriginalUnsupported)));
    assert!(matches!(authority.compare_and_append_guarded_with_native_work(
        original_append(id, expected, proposed.clone()), &original, &cancellation,
    ).await, Err(AuthorityStoreError::NativeOriginalUnsupported)));
    assert!(matches!(authority.compare_and_append_guarded_with_native_owner(
        original_append(id, expected, proposed), &original, &cancellation, &mut guard,
    ).await, Err(SimulationOwnerFailure::Authority(AuthorityStoreError::NativeOriginalUnsupported))));
    assert_eq!(simulation.authority.ordinary_calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert_eq!(original.receipt().backend_read_operations, 0);
    assert_eq!(original.receipt().backend_write_operations, 0);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_simulation_trace_denial_and_original_cancellation_precede_mutation_dispatch()
-> Result<(), Box<dyn std::error::Error>> {
    let simulation = Simulation::wrap(crate::MemoryAuthorityStore::default(), (), SimulationOptions::default())?;
    let (authority, _) = simulation.stores();
    let cancellation = CancellationToken::new();
    let mut budget = original_authority_budget();
    budget.allocation_operations = 1;
    let original = crate::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(error, _)| error)?;
    let id = AuthorityId::from_bytes([0xca; 16]);
    let proposed = ProposedCommit {
        operation_id: OperationId::from_bytes([0xcb; 16]),
        fingerprint: Digest::from_bytes([0xcc; 32]),
        payload: Bytes::from_static(b"root-borrowed"),
    };
    simulation.schedule_next(SimulationOperation::AuthorityCreate, SimulationFault::AmbiguousAuthorityAppend)?;
    let before = original.receipt();
    assert!(authority.create_authority_with_first_record_with_native_work(
        id, proposed.clone(), &original, &cancellation,
    ).await.is_err());
    assert_eq!(original.receipt(), before);
    assert!(simulation.trace()?.is_empty());
    assert_eq!(simulation.state.lock().expect("state").faults.len(), 1);
    let expected = Head {
        epoch: Epoch::GENESIS, sequence: Sequence::GENESIS, digest: Digest::from_bytes([0xcd; 32]),
    };
    assert!(authority.compare_and_append_guarded_with_native_work(
        original_append(id, expected, proposed.clone()), &original, &cancellation,
    ).await.is_err());
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let callback_calls = Arc::clone(&calls);
    let mut guard = move |_root: &crate::NativeMaterializationWork| {
        callback_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::future::ready(Ok::<(), AuthorityStoreError>(()))
    };
    assert!(authority.compare_and_append_guarded_with_native_owner(
        original_append(id, expected, proposed.clone()), &original, &cancellation, &mut guard,
    ).await.is_err());
    assert_eq!(original.receipt(), before);
    assert!(simulation.trace()?.is_empty());

    let original = crate::NativeMaterializationWork::new(original_authority_budget(), &cancellation)
        .map_err(|(error, _)| error)?;
    cancellation.cancel();
    let unrelated_argument = CancellationToken::new();
    let before = original.receipt();
    assert!(matches!(authority.create_authority_with_first_record_with_native_work(
        id, proposed.clone(), &original, &unrelated_argument,
    ).await, Err(AuthorityStoreError::NativeOriginal(crate::NativeTreeMaterializationError::Cancelled))));
    assert!(authority.compare_and_append_guarded_with_native_work(
        original_append(id, expected, proposed.clone()), &original, &unrelated_argument,
    ).await.is_err());
    assert!(authority.compare_and_append_guarded_with_native_owner(
        original_append(id, expected, proposed), &original, &unrelated_argument, &mut guard,
    ).await.is_err());
    assert_eq!(original.receipt(), before);
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert!(simulation.trace()?.is_empty());
    Ok(())
}
