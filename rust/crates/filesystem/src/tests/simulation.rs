use super::*;
use crate::WorkError;
use crate::foundation::Digest;
use crate::storage::{FenceOutcome, ObjectKind};
use acyclic_stream::OwnedBytes;

fn object(bytes: &[u8]) -> (ObjectId, OwnedBytes) {
    let bytes = OwnedBytes::copy_from_slice(bytes);
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
        payload: bytes::Bytes::from(vec![operation, fingerprint]),
    }
}

#[tokio::test]
async fn delayed_objects_are_invisible_until_bounded_flush() -> Result<(), Box<dyn std::error::Error>> {
    let simulation = Simulation::default();
    simulation.schedule(ScheduledSimulationFault {
        operation: SimulationOperation::ObjectPut,
        occurrence: 1,
        fault: SimulationFault::DelayObjectVisibility,
    })?;
    let (_, objects) = simulation.stores();
    let cancellation = CancellationToken::new();
    let (object_id, bytes) = object(b"delayed");
    (async {
        let __object_args = (
            object_id,
            bytes.clone(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = objects
            .put(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }).await?;
    let missing_result = (async {
        let __object_args = (object_id, 64, WorkBudget::UNBOUNDED, &cancellation);
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = objects
            .read(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }).await;
    let Err(missing) = missing_result else {
        return Err("delayed object became visible before flush".into());
    };
    assert!(matches!(missing.error, ObjectStoreError::Missing));
    let flushed = (
        simulation.flush_object_visibility(WorkBudget::UNBOUNDED, &cancellation)
    ).await?;
    assert_eq!(flushed.value, 1);
    let visible = (async {
        let __object_args = (object_id, 64, WorkBudget::UNBOUNDED, &cancellation);
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = objects
            .read(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }).await?;
    assert_eq!(visible.value.bytes, bytes);
    Ok(())
}

#[tokio::test]
async fn corrupt_and_partial_reads_are_exactly_scheduled() -> Result<(), Box<dyn std::error::Error>> {
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
    (async {
        let __object_args = (object_id, bytes, WorkBudget::UNBOUNDED, &cancellation);
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = objects
            .put(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }).await?;
    let corrupt = (async {
        let __object_args = (object_id, 64, WorkBudget::UNBOUNDED, &cancellation);
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = objects
            .read(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }).await?;
    assert_ne!(
        object_digest(object_id.kind, &corrupt.value.bytes),
        object_id.digest
    );
    let partial = (async {
        let __object_args = (object_id, 64, WorkBudget::UNBOUNDED, &cancellation);
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = objects
            .read(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }).await?;
    assert_eq!(partial.value.bytes.len(), b"authenticated".len() - 1);
    Ok(())
}

#[tokio::test]
async fn ambiguous_append_is_durable_and_retry_resolves_identity()
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
    let created = (async { let __authority_args = (authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = authority.create_authority(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) }).await?;
    let CreateAuthorityOutcome::Created(head) = created.value else {
        return Err("fresh simulated authority already existed".into());
    };
    let proposed = proposal(3, 4);
    let ambiguous_result = (async { let __authority_args = (authority_id, Epoch::GENESIS, head, proposed.clone(), WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result = authority.compare_and_append(crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }).await;
    let Err(ambiguous) = ambiguous_result else {
        return Err("scheduled ambiguous append returned success".into());
    };
    assert!(matches!(
        ambiguous.error,
        AuthorityStoreError::Indeterminate { .. }
    ));
    let retry = (async { let __authority_args = (authority_id, Epoch::GENESIS, head, proposed, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result = authority.compare_and_append(crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }).await?;
    assert!(matches!(retry.value, AppendOutcome::AlreadyCommitted(_)));
    Ok(())
}

#[tokio::test]
async fn duplicate_replay_and_fence_change_are_observable_without_hidden_repair()
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
    let created = (async { let __authority_args = (authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = authority.create_authority(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) }).await?;
    let CreateAuthorityOutcome::Created(head) = created.value else {
        return Err("fresh simulated authority already existed".into());
    };
    let committed = (async { let __authority_args = (authority_id, Epoch::GENESIS, head, proposal(5, 6), WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result = authority.compare_and_append(crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }).await?;
    let AppendOutcome::Committed(commit) = committed.value else {
        return Err("first simulated append did not commit".into());
    };
    let replay = (async { let __authority_args = (authority_id, Sequence::GENESIS, ReplayLimit {
            records: 8,
            payload_bytes: 1_024,
        }, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result = authority.replay(__authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }).await?;
    assert_eq!(replay.value, vec![commit.clone(), commit.clone()]);
    let fenced = (async { let __authority_args = (authority_id, Epoch::GENESIS, Head {
            epoch: commit.epoch,
            sequence: commit.sequence,
            digest: commit.digest,
        }, proposal(7, 8), WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result = authority.compare_and_append(crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }).await?;
    assert!(matches!(fenced.value, AppendOutcome::Fenced { .. }));
    Ok(())
}

#[tokio::test]
async fn fence_before_append_preserves_nested_budget_failure_work()
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
    let created = (async { let __authority_args = (authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = authority.create_authority(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) }).await?;
    let CreateAuthorityOutcome::Created(head) = created.value else {
        return Err("fresh simulated authority already existed".into());
    };
    let result = (async { let __authority_args = (authority_id, Epoch::GENESIS, head, proposal(82, 83), WorkBudget {
            authority_records_appended: 1,
            backend_read_operations: 1,
            backend_write_operations: 1,
            ..WorkBudget::UNBOUNDED
        }, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result = authority.compare_and_append(crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }).await;
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
        let __object_args = (object_id, WorkBudget::UNBOUNDED, &cancellation);
        let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result = objects.contains(__object_args.0, &mut __object_work).await;
        __object_work.receipt(__object_result)
    }).await;
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
        let __object_args = (object_id, WorkBudget::UNBOUNDED, &cancellation);
        let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result = objects.contains(__object_args.0, &mut __object_work).await;
        __object_work.receipt(__object_result)
    }).await;
    let Err(second) = second_result else {
        return Err("scheduled next partition succeeded".into());
    };
    assert!(matches!(second.error, ObjectStoreError::Rejected(_)));
    assert_eq!(simulation.trace()?.len(), 2);
    Ok(())
}

fn assert_authority_before_work<T>(
    result: AuthorityResult<T>,
    cancelled: bool,
) -> Result<(), Box<dyn std::error::Error>> {
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

async fn exercise_authority_before_work(
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
        (async { let __authority_args = (authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = authority.create_authority(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) }).await,
        cancelled,
    )?;
    assert_authority_before_work(
        (async { let __authority_args = (authority_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = authority.head(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).await,
        cancelled,
    )?;
    assert_authority_before_work(
        (async { let __authority_args = (authority_id, Epoch::GENESIS, head, proposal(44, 45), WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result = authority.compare_and_append(crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }).await,
        cancelled,
    )?;
    assert_authority_before_work(
        (async { let __authority_args = (authority_id, Sequence::GENESIS, ReplayLimit {
                records: 1,
                payload_bytes: 64,
            }, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result = authority.replay(__authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }).await,
        cancelled,
    )?;
    assert_authority_before_work(
        (async { let __authority_args = (authority_id, head, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = authority.fence(__authority_args.0, __authority_args.1, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).await,
        cancelled,
    )?;
    assert_authority_before_work(
        (async { let __authority_args = (authority_id, operation_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = authority.find_operation(__authority_args.0, __authority_args.1, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }).await,
        cancelled,
    )?;
    assert!(simulation.trace()?.is_empty() == cancelled);
    Ok(())
}

fn assert_object_before_work<T>(
    result: ObjectResult<T>,
    cancelled: bool,
) -> Result<(), Box<dyn std::error::Error>> {
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

async fn exercise_object_before_work(
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
        (async {
            let __object_args = (object_id, bytes, WorkBudget::UNBOUNDED, &cancellation);
            let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
            let __object_result = objects
                .put(__object_args.0, __object_args.1, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        })
        .await,
        cancelled,
    )?;
    assert_object_before_work(
        (async {
            let __object_args = (object_id, 64,
            WorkBudget::UNBOUNDED,
            &cancellation,
        );
            let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
            let __object_result = objects
                .read(__object_args.0, __object_args.1, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        })
        .await,
        cancelled,
    )?;
    assert_object_before_work(
        (async {
            let __object_args = (&requests, WorkBudget::UNBOUNDED, &cancellation);
            let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
            let __object_result = objects
                .read_many(__object_args.0, &mut __object_work)
                .await
                .map(|(values, _allocation)| values);
            __object_work.receipt(__object_result)
        })
        .await,
        cancelled,
    )?;
    assert_object_before_work(
        (async {
            let __object_args = (object_id, WorkBudget::UNBOUNDED,
            &cancellation,
        );
            let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
            let __object_result = objects.contains(__object_args.0, &mut __object_work).await;
            __object_work.receipt(__object_result)
        }).await,
        cancelled,
    )?;
    assert!(simulation.trace()?.is_empty() == cancelled);
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
async fn stale_head_and_batch_read_faults_are_budgeted_and_exact()
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
    let observed = (async { let __authority_args = (authority_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = authority.head(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).await?;
    assert_eq!(observed.value, stale);
    assert_eq!(observed.work.authority_records_read, 1);
    let too_small = (async { let __authority_args = (authority_id, WorkBudget::default(), &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = authority.head(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).await;
    assert!(matches!(
        too_small,
        Err(AuthorityFailure {
            error: AuthorityStoreError::Work(_),
            ..
        })
    ));

    let (object_id, bytes) = object(b"batch-fault");
    (async {
        let __object_args = (object_id, bytes, WorkBudget::UNBOUNDED, &cancellation);
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = objects
            .put(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }).await?;
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
    let corrupt = (async {
        let __object_args = (&requests, WorkBudget::UNBOUNDED, &cancellation);
        let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result = objects
            .read_many(__object_args.0, &mut __object_work)
            .await
            .map(|(values, _allocation)| values);
        __object_work.receipt(__object_result)
    }).await?;
    assert_ne!(
        object_digest(object_id.kind, &corrupt.value[0].bytes),
        object_id.digest
    );
    let partial = (async {
        let __object_args = (&requests, WorkBudget::UNBOUNDED, &cancellation);
        let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result = objects
            .read_many(__object_args.0, &mut __object_work)
            .await
            .map(|(values, _allocation)| values);
        __object_work.receipt(__object_result)
    }).await?;
    assert_eq!(partial.value[0].bytes.len(), b"batch-fault".len() - 1);
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
            let __object_args = (object_id, WorkBudget::UNBOUNDED, &cancellation);
            let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
            let __object_result = objects.contains(__object_args.0, &mut __object_work).await;
            __object_work.receipt(__object_result)
        })
        .await
        .is_err()
    );
    let exhausted = (async {
        let __object_args = (object_id,
            WorkBudget::UNBOUNDED,
            &cancellation,
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result = objects.contains(__object_args.0, &mut __object_work).await;
        __object_work.receipt(__object_result)
    }).await
    .err()
    .ok_or("trace exhaustion was ignored")?;
    assert!(matches!(exhausted.error, ObjectStoreError::Rejected(_)));
    assert_eq!(simulation.trace()?.len(), 1);
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
    let digest_failure = (async {
        let __object_args = (
            first_id,
            OwnedBytes::from_static(b"bad!"),
            WorkBudget::UNBOUNDED,
            &cancellation,
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = objects
            .put(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }).await
    .err()
    .ok_or("digest mismatch was admitted")?;
    assert!(matches!(
        digest_failure.error,
        ObjectStoreError::DigestMismatch
    ));
    (async {
        let __object_args = (
            first_id,
            first_bytes.clone(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = objects
            .put(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }).await?;
    let repeated = (async {
        let __object_args = (first_id, first_bytes, WorkBudget::UNBOUNDED, &cancellation);
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = objects
            .put(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }).await?;
    assert_eq!(repeated.work.backend_write_operations, 1);

    let (second_id, second_bytes) = object(b"x");
    let bounded = (async {
        let __object_args = (
            second_id,
            second_bytes,
            WorkBudget::UNBOUNDED,
            &cancellation,
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = objects
            .put(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }).await
    .err()
    .ok_or("delayed-object bound was ignored")?;
    assert!(matches!(bounded.error, ObjectStoreError::Rejected(_)));
    let flushed = (
        simulation.flush_object_visibility(WorkBudget::UNBOUNDED, &cancellation)
    ).await?;
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
    (async {
        let __object_args = (empty_id, empty_bytes, WorkBudget::UNBOUNDED, &cancellation);
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = objects
            .put(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }).await?;
    let corrupt = (async {
        let __object_args = (empty_id, 0, WorkBudget::UNBOUNDED, &cancellation);
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = objects
            .read(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }).await
    .err()
    .ok_or("empty corrupt read succeeded")?;
    assert!(matches!(corrupt.error, ObjectStoreError::Corrupt));

    let authority_id = AuthorityId::from_bytes([71; 16]);
    (async { let __authority_args = (authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = authority.create_authority(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) }).await?;
    let missing = (async { let __authority_args = (authority_id, OperationId::from_bytes([72; 16]), WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = authority.find_operation(__authority_args.0, __authority_args.1, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }).await?;
    assert!(missing.value.is_none());
    let head = (async { let __authority_args = (authority_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = authority.head(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).await?
    .value;
    let fenced = (async { let __authority_args = (authority_id, head, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = authority.fence(__authority_args.0, __authority_args.1, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).await?;
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
    let authority_failure = (async { let __authority_args = (AuthorityId::from_bytes([80; 16]), WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = authority.head(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).await
    .err()
    .ok_or("poisoned authority lookup unexpectedly succeeded")?;
    assert!(matches!(
        authority_failure.error,
        AuthorityStoreError::CorruptStatic(_)
    ));
    let object_failure = (async {
        let __object_args = (object(b"poisoned").0, WorkBudget::UNBOUNDED, &cancellation);
        let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result = objects.contains(__object_args.0, &mut __object_work).await;
        __object_work.receipt(__object_result)
    }).await
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
    let failure = (
        simulation.flush_object_visibility(WorkBudget::UNBOUNDED, &cancellation)
    ).await
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
    let failure = (
        simulation.flush_object_visibility(WorkBudget::UNBOUNDED, &cancellation)
    ).await
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
    ({ let __authority_args = (authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = authority.create_authority(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) })?;
    let original = ({ let __authority_args = (authority_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = authority.head(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?.value;
    simulation.schedule(ScheduledSimulationFault {
        operation: SimulationOperation::AuthorityReserve, occurrence: 1,
        fault: SimulationFault::AmbiguousAuthorityAppend,
    })?;
    let lost = ({ let __authority_args = (authority_id, original, operation_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result = authority.reserve_publication(__authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).expect_err("real acquisition committed before the scheduled acknowledgement loss");
    assert!(matches!(lost.error, AuthorityStoreError::Indeterminate { operation: "reserve-publication", .. }));
    assert_eq!(lost.work.backend_write_operations, 1);
    let reservation = ({ let __authority_args = (authority_id, operation_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = authority.publication_reservation(__authority_args.0, __authority_args.1, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?.value.ok_or("original reservation evidence missing")?;
    assert_eq!(reservation.authority_id, authority_id);
    assert_eq!(reservation.operation_id, operation_id);
    assert_eq!(reservation.expected, original);
    simulation.schedule(ScheduledSimulationFault {
        operation: SimulationOperation::AuthorityReservation, occurrence: 2,
        fault: SimulationFault::PartitionBefore,
    })?;
    let partitioned = ({ let __authority_args = (authority_id, operation_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = authority.publication_reservation(__authority_args.0, __authority_args.1, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).expect_err("scheduled actual readonly query is intercepted before provider I/O");
    assert!(matches!(partitioned.error, AuthorityStoreError::RejectedStatic(_)));
    assert_eq!(*partitioned.work, WorkCounters::UNCHARGED);
    simulation.schedule(ScheduledSimulationFault {
        operation: SimulationOperation::AuthorityRelease, occurrence: 1,
        fault: SimulationFault::AmbiguousAuthorityAppend,
    })?;
    let released = ({ let __authority_args = (reservation, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = authority.release_publication(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).expect_err("real release committed before acknowledgement loss");
    assert!(matches!(released.error, AuthorityStoreError::Indeterminate { operation: "release-publication", .. }));
    ({ let __authority_args = (reservation, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = authority.release_publication(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?;
    assert_eq!(({ let __authority_args = (authority_id, operation_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = authority.publication_reservation(__authority_args.0, __authority_args.1, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?.value, None);
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
struct OpaqueOriginalAuthority {
    inner: crate::MemoryAuthorityStore,
    ordinary_calls: std::sync::atomic::AtomicUsize,
}

#[cfg(not(target_arch = "wasm32"))]
impl crate::AsyncAuthorityStore for OpaqueOriginalAuthority {
async fn create_authority<G: crate::AuthorityGuard>(&self, authority: crate::AuthorityId, epoch: crate::Epoch, work: &mut crate::AuthorityWork<'_>, mut guard: Option<&mut G>) -> Result<crate::CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
crate::async_storage::seal(guard.as_deref_mut(), work).await?;
work.cancellation.check().map_err(|_| crate::AuthorityStoreError::Cancelled)?;
self.ordinary_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
let value = crate::AsyncAuthorityStore::create_authority(&self.inner, authority, epoch, work, None::<&mut ()>).await.map_err(|error| {
            crate::AuthorityError::Authority(error.into_error(std::convert::identity))
        })?;
if matches!(value, crate::CreateAuthorityOutcome::Existing(_)) { crate::async_storage::seal(guard.as_deref_mut(), work).await?; }
Ok(value)
}
async fn compare_and_append<G: crate::AuthorityGuard>(&self, request: crate::GuardedAppend, work: &mut crate::AuthorityWork<'_>, mut guard: Option<&mut G>) -> Result<(crate::AppendOutcome, crate::AuthorityAllocation), crate::AuthorityError<G::Error>> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
if request.permit != crate::PublicationPermit::Unrestricted { return Err(crate::AuthorityStoreError::Rejected("authority backend cannot atomically evaluate operation leases".to_owned()).into()); }
crate::async_storage::seal(guard.as_deref_mut(), work).await?;
work.cancellation.check().map_err(|_| crate::AuthorityStoreError::Cancelled)?;
self.ordinary_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
crate::AsyncAuthorityStore::compare_and_append(&self.inner, request, work, None::<&mut ()>).await.map_err(|error| {
                crate::AuthorityError::Authority(error.into_error(std::convert::identity))
            })
}
async fn head(&self, authority: crate::AuthorityId, work: &mut crate::AuthorityWork<'_>) -> Result<crate::Head, crate::AuthorityStoreError> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
work.cancellation.check().map_err(|_| crate::AuthorityStoreError::Cancelled)?;
self.ordinary_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
crate::AsyncAuthorityStore::head(&self.inner, authority, work).await
}
async fn replay(&self, authority: crate::AuthorityId, after: crate::Sequence, limit: crate::ReplayLimit, work: &mut crate::AuthorityWork<'_>) -> Result<(Vec<crate::DurableCommit>, crate::AuthorityAllocation), crate::AuthorityStoreError> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
work.cancellation.check().map_err(|_| crate::AuthorityStoreError::Cancelled)?;
self.ordinary_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
crate::AsyncAuthorityStore::replay(&self.inner, authority, after, limit, work).await
}
async fn fence(&self, authority: crate::AuthorityId, expected: crate::Head, work: &mut crate::AuthorityWork<'_>) -> Result<crate::FenceOutcome, crate::AuthorityStoreError> {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
work.cancellation.check().map_err(|_| crate::AuthorityStoreError::Cancelled)?;
self.ordinary_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
crate::AsyncAuthorityStore::fence(&self.inner, authority, expected, work).await
}
async fn find_operation(&self, authority: crate::AuthorityId, operation: crate::OperationId, work: &mut crate::AuthorityWork<'_>) -> Result<(Option<crate::DurableCommit>, crate::AuthorityAllocation), crate::AuthorityStoreError,
    > {
#[cfg(not(target_arch = "wasm32"))]
if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
work.cancellation.check().map_err(|_| crate::AuthorityStoreError::Cancelled)?;
self.ordinary_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
crate::AsyncAuthorityStore::find_operation(&self.inner, authority, operation, work).await
}
}

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
    assert!(matches!(({ let __authority_args = (id, &work, &cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.1, __authority_args.2); let __authority_result = authority.head(__authority_args.0, &mut __authority_work).await; __authority_result }),
        Err(AuthorityStoreError::NativeOriginalUnsupported)));
    assert!(matches!(({ let __authority_args = (id, Sequence::GENESIS, ReplayLimit { records: 1, payload_bytes: 64 }, &work, &cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.3, __authority_args.4); let __authority_result = authority.replay(__authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await.map(|(value, allocation)| {
                    (value, allocation.expect("native authority allocation"))
                }); __authority_result }),
        Err(AuthorityStoreError::NativeOriginalUnsupported)));
    assert!(matches!(({ let __authority_args = (id, expected, &work, &cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.2, __authority_args.3); let __authority_result = authority.fence(__authority_args.0, __authority_args.1, &mut __authority_work).await; __authority_result }),
        Err(AuthorityStoreError::NativeOriginalUnsupported)));
    assert!(matches!(({ let __authority_args = (id, OperationId::from_bytes([0xf3; 16]), &work, &cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.2, __authority_args.3); let __authority_result = authority.find_operation(__authority_args.0, __authority_args.1, &mut __authority_work).await.map(|(value, allocation)| {
                    (value, allocation.expect("native authority allocation"))
                }); __authority_result }), Err(AuthorityStoreError::NativeOriginalUnsupported)));
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
    let mut allocation = original.claim(2, 1, crate::WorkCounters::UNCHARGED)?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(2)?;
    original.charge(WorkCounters { bytes_copied: 2, ..WorkCounters::UNCHARGED })?;
    bytes.extend_from_slice(&[operation, fingerprint]);
    cancellation.check()?;
    allocation.reconcile_capacity(bytes.capacity() as u64)?;
    let hash = WorkCounters {
        bytes_hashed: bytes.len() as u64 + crate::storage::OBJECT_DIGEST_ENVELOPE_BYTES,
        ..WorkCounters::UNCHARGED
    };
    original.admit(hash)?;
    let _digest = crate::storage::object_digest(ObjectKind::BlobChunk, &bytes);
    original.record_admitted(hash)?;
    let control_bytes = crate::kernel::allocation::bytes_owner_control_bytes::<acyclic_stream::OwnedBytes>()?;
    let control = original.claim(control_bytes, u64::from(control_bytes != 0), WorkCounters::UNCHARGED)?;
    let payload = acyclic_stream::OwnedBytes::from_vec_with_guard(
        bytes, (allocation, control), original.runtime_meter(),
    ).map_err(|failure| match failure.into_error() {
        acyclic_stream::PreparedOwnedBytesError::Admission(error) => original.runtime_error(error),
        acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => {
            crate::NativeTreeMaterializationError::InvalidLayout
        }
    })?;
    let payload = bytes::Bytes::from_owner(payload);
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
        assert!(matches!(({ let __authority_args = (id, proposed.clone(), &original, &cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.2, __authority_args.3); let __authority_result = authority.create_first_record(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_result }), Err(AuthorityStoreError::RejectedStatic(_))));
        simulation.schedule_next(SimulationOperation::AuthorityAppend, fault)?;
        assert!(matches!(({ let __authority_args = (original_append(id, expected, proposed.clone()), &original, &cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.1, __authority_args.2); let __authority_result = authority.compare_and_append(__authority_args.0, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, allocation)| {
                        (value, allocation.expect("native authority allocation"))
                    }); __authority_result }), Err(AuthorityStoreError::RejectedStatic(_))));
        simulation.schedule_next(SimulationOperation::AuthorityAppend, fault)?;
        assert!(matches!(({ let __authority_args = (original_append(id, expected, proposed.clone()), &original, &cancellation, &mut guard,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.1, __authority_args.2); let __authority_result = authority.compare_and_append(__authority_args.0, &mut __authority_work, Some(&mut crate::NativeAuthorityGuard((__authority_args.3).clone()))).await.map_err(|error| error.into_error(std::convert::Into::into)).map(|(value, allocation)| {
                        (value, allocation.expect("native authority allocation"))
                    }); __authority_result }), Err(SimulationOwnerFailure::Authority(AuthorityStoreError::RejectedStatic(_)))));
    }
    assert!(matches!(({ let __authority_args = (id, proposed.clone(), &original, &cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.2, __authority_args.3); let __authority_result = authority.create_first_record(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_result }), Err(AuthorityStoreError::NativeOriginalUnsupported)));
    assert!(matches!(({ let __authority_args = (original_append(id, expected, proposed.clone()), &original, &cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.1, __authority_args.2); let __authority_result = authority.compare_and_append(__authority_args.0, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, allocation)| {
                    (value, allocation.expect("native authority allocation"))
                }); __authority_result }), Err(AuthorityStoreError::NativeOriginalUnsupported)));
    assert!(matches!(({ let __authority_args = (original_append(id, expected, proposed), &original, &cancellation, &mut guard,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.1, __authority_args.2); let __authority_result = authority.compare_and_append(__authority_args.0, &mut __authority_work, Some(&mut crate::NativeAuthorityGuard((__authority_args.3).clone()))).await.map_err(|error| error.into_error(std::convert::Into::into)).map(|(value, allocation)| {
                    (value, allocation.expect("native authority allocation"))
                }); __authority_result }), Err(SimulationOwnerFailure::Authority(AuthorityStoreError::NativeOriginalUnsupported))));
    assert_eq!(simulation.authority.ordinary_calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert_eq!(original.receipt().backend_read_operations, 0);
    assert_eq!(original.receipt().backend_write_operations, 0);
    Ok(())
}
