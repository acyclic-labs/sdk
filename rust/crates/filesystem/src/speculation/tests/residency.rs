use super::*;
use crate::foundation::Digest;
use crate::memory::{MemoryAuthorityStore, MemoryObjectStore};
use crate::storage::{ObjectId, ObjectKind, ObjectStore, object_digest};
use crate::{CachedObjectStore, EmbeddedCapabilities, Fs, ObjectCacheOptions, WorkCounters};
use bytes::Bytes;
use std::sync::Arc;

fn generation(byte: u8) -> GenerationId {
    GenerationId::new(Digest::from_bytes([byte; 32]))
}

fn object(store: &MemoryObjectStore, byte: u8) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let bytes = Bytes::from(vec![byte; 64]);
    let object_id = ObjectId {
        kind: ObjectKind::Blob,
        digest: object_digest(ObjectKind::Blob, &bytes),
    };
    ObjectStore::put(store, object_id, bytes, WorkBudget::UNBOUNDED)?;
    Ok(object_id)
}

fn candidate(
    operation_byte: u8,
    volume_id: VolumeId,
    generation_id: GenerationId,
    object_id: ObjectId,
) -> ResidencyCandidate {
    ResidencyCandidate {
        operation_id: OperationId::from_bytes([operation_byte; 16]),
        volume_id,
        generation_id,
        request: ObjectReadRequest {
            object_id,
            maximum_bytes: 64,
        },
        reason: ResidencyReason::SequentialRange,
    }
}

#[test]
fn option_validation_and_all_rejection_classes_are_total() -> Result<(), Box<dyn std::error::Error>>
{
    let defaults = ResidencySpeculatorOptions::default();
    for invalid in [
        ResidencySpeculatorOptions {
            maximum_active_operations: 0,
            ..defaults
        },
        ResidencySpeculatorOptions {
            maximum_active_bytes: 0,
            ..defaults
        },
        ResidencySpeculatorOptions {
            outcome_window: 0,
            ..defaults
        },
        ResidencySpeculatorOptions {
            traffic_window: 0,
            ..defaults
        },
        ResidencySpeculatorOptions {
            minimum_usefulness_samples: 0,
            ..defaults
        },
    ] {
        assert_eq!(invalid.validate(), Err(ResidencySpeculatorError::ZeroBound));
    }
    assert_eq!(
        ResidencySpeculatorOptions {
            outcome_window: 1,
            minimum_usefulness_samples: 2,
            ..defaults
        }
        .validate(),
        Err(ResidencySpeculatorError::UsefulnessWindowTooSmall)
    );
    for invalid in [
        ResidencySpeculatorOptions {
            speculative_cost_basis_points: 10_001,
            ..defaults
        },
        ResidencySpeculatorOptions {
            minimum_usefulness_basis_points: 10_001,
            ..defaults
        },
    ] {
        assert_eq!(
            invalid.validate(),
            Err(ResidencySpeculatorError::InvalidBasisPoints)
        );
    }

    let store = MemoryObjectStore::default();
    let object_id = object(&store, 40)?;
    let volume_id = VolumeId::from_bytes([41; 16]);
    let generation_id = generation(42);
    let mut engine = ResidencySpeculator::new(
        ResidencySpeculatorOptions {
            maximum_active_bytes: 32,
            speculative_cost_basis_points: 10_000,
            ..defaults
        },
        volume_id,
        generation_id,
    )?;
    assert_eq!(
        engine.admit(candidate(
            1,
            VolumeId::from_bytes([99; 16]),
            generation_id,
            object_id
        ))?,
        ResidencyAdmission::Rejected(ResidencyRejection::WrongVolume)
    );
    let mut invalid_request = candidate(2, volume_id, generation_id, object_id);
    invalid_request.request.maximum_bytes = 0;
    assert_eq!(
        engine.admit(invalid_request)?,
        ResidencyAdmission::Rejected(ResidencyRejection::InvalidRequest)
    );
    assert_eq!(
        engine.admit(candidate(3, volume_id, generation_id, object_id))?,
        ResidencyAdmission::Rejected(ResidencyRejection::ByteCapacity)
    );

    let mut cost_limited = ResidencySpeculator::new(defaults, volume_id, generation_id)?;
    assert_eq!(
        cost_limited.admit(candidate(4, volume_id, generation_id, object_id))?,
        ResidencyAdmission::Rejected(ResidencyRejection::CostBudget)
    );
    assert_eq!(cost_limited.metrics().rejected_cost, 1);
    Ok(())
}

#[test]
fn bounded_feedback_windows_discard_only_oldest_samples() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let first = object(&store, 50)?;
    let second = object(&store, 51)?;
    let volume_id = VolumeId::from_bytes([52; 16]);
    let generation_id = generation(53);
    let mut engine = ResidencySpeculator::new(
        ResidencySpeculatorOptions {
            maximum_active_operations: 2,
            maximum_active_bytes: 128,
            outcome_window: 1,
            traffic_window: 1,
            speculative_cost_basis_points: 10_000,
            minimum_usefulness_samples: 1,
            minimum_usefulness_basis_points: 0,
        },
        volume_id,
        generation_id,
    )?;
    engine.record_foreground(128)?;
    let ResidencyAdmission::Admitted(first_permit) =
        engine.admit(candidate(1, volume_id, generation_id, first))?
    else {
        return Err("first candidate was not admitted".into());
    };
    engine.finish(first_permit.candidate().operation_id, true)?;
    engine.record_foreground(128)?;
    let ResidencyAdmission::Admitted(second_permit) =
        engine.admit(candidate(2, volume_id, generation_id, second))?
    else {
        return Err("second candidate was not admitted".into());
    };
    engine.finish(second_permit.candidate().operation_id, false)?;
    assert_eq!(engine.outcomes.len(), 1);
    assert_eq!(engine.traffic.len(), 1);
    assert_eq!(engine.metrics().useful, 1);
    assert_eq!(engine.metrics().wasted, 1);
    Ok(())
}

#[test]
fn admission_is_generation_fenced_bounded_adaptive_and_exact()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let first = object(&store, 1)?;
    let second = object(&store, 2)?;
    let volume_id = VolumeId::from_bytes([3; 16]);
    let current = generation(4);
    let options = ResidencySpeculatorOptions {
        maximum_active_operations: 1,
        maximum_active_bytes: 64,
        outcome_window: 2,
        traffic_window: 8,
        speculative_cost_basis_points: 10_000,
        minimum_usefulness_samples: 2,
        minimum_usefulness_basis_points: 5_000,
    };
    let mut engine = ResidencySpeculator::new(options, volume_id, current)?;
    engine.record_foreground(1_000)?;

    let stale = candidate(1, volume_id, generation(9), first);
    assert_eq!(
        engine.admit(stale)?,
        ResidencyAdmission::Rejected(ResidencyRejection::StaleGeneration)
    );
    let admitted = candidate(2, volume_id, current, first);
    let ResidencyAdmission::Admitted(permit) = engine.admit(admitted)? else {
        return Err("candidate was not admitted".into());
    };
    assert_eq!(permit.candidate(), admitted);
    assert_eq!(
        engine.admit(candidate(3, volume_id, current, first))?,
        ResidencyAdmission::Rejected(ResidencyRejection::DuplicateObject)
    );
    let mut duplicate_operation = candidate(2, volume_id, current, second);
    duplicate_operation.operation_id = admitted.operation_id;
    assert_eq!(
        engine.admit(duplicate_operation)?,
        ResidencyAdmission::Rejected(ResidencyRejection::DuplicateOperation)
    );
    assert_eq!(
        engine.admit(candidate(4, volume_id, current, second))?,
        ResidencyAdmission::Rejected(ResidencyRejection::OperationCapacity)
    );
    engine.finish(admitted.operation_id, false)?;

    let ResidencyAdmission::Admitted(_) = engine.admit(candidate(5, volume_id, current, second))?
    else {
        return Err("second candidate was not admitted".into());
    };
    engine.finish(OperationId::from_bytes([5; 16]), false)?;
    assert_eq!(
        engine.admit(candidate(6, volume_id, current, first))?,
        ResidencyAdmission::Rejected(ResidencyRejection::LowUsefulness)
    );
    assert_eq!(
        engine.metrics(),
        ResidencyMetrics {
            candidates: 7,
            admitted: 2,
            active: 0,
            active_bytes: 0,
            useful: 0,
            wasted: 2,
            rejected_fence: 1,
            rejected_duplicate: 2,
            rejected_capacity: 1,
            rejected_cost: 0,
            rejected_usefulness: 1,
        }
    );
    assert_eq!(
        engine.finish(admitted.operation_id, true),
        Err(ResidencySpeculatorError::UnknownPermit)
    );
    Ok(())
}

#[test]
fn foreground_preemption_and_generation_replacement_terminalize_every_permit()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let first = object(&store, 1)?;
    let second = object(&store, 2)?;
    let volume_id = VolumeId::from_bytes([3; 16]);
    let current = generation(4);
    let mut engine = ResidencySpeculator::new(
        ResidencySpeculatorOptions {
            maximum_active_operations: 2,
            maximum_active_bytes: 128,
            speculative_cost_basis_points: 10_000,
            ..ResidencySpeculatorOptions::default()
        },
        volume_id,
        current,
    )?;
    engine.record_foreground(128)?;
    assert!(matches!(
        engine.admit(candidate(1, volume_id, current, first))?,
        ResidencyAdmission::Admitted(_)
    ));
    assert!(matches!(
        engine.admit(candidate(2, volume_id, current, second))?,
        ResidencyAdmission::Admitted(_)
    ));
    assert_eq!(
        engine.preempt_for_foreground()?,
        [
            OperationId::from_bytes([1; 16]),
            OperationId::from_bytes([2; 16])
        ]
    );
    assert_eq!(engine.metrics().wasted, 2);
    assert!(engine.replace_generation(generation(8))?.is_empty());
    assert_eq!(
        engine.admit(candidate(3, volume_id, current, first))?,
        ResidencyAdmission::Rejected(ResidencyRejection::StaleGeneration)
    );
    Ok(())
}

#[test]
fn execution_warms_the_same_authenticated_cache_without_retaining_a_second_body()
-> Result<(), Box<dyn std::error::Error>> {
    let backend = MemoryObjectStore::default();
    let object_id = object(&backend, 7)?;
    let store = CachedObjectStore::new(
        backend,
        ObjectCacheOptions {
            maximum_entries: 4,
            maximum_bytes: 1024,
            maximum_in_flight: 1,
            maximum_waiters_per_object: 1,
        },
    )?;
    let volume_id = VolumeId::from_bytes([3; 16]);
    let generation_id = generation(4);
    let mut engine = ResidencySpeculator::new(
        ResidencySpeculatorOptions {
            speculative_cost_basis_points: 10_000,
            ..ResidencySpeculatorOptions::default()
        },
        volume_id,
        generation_id,
    )?;
    engine.record_foreground(64)?;
    let ResidencyAdmission::Admitted(permit) =
        engine.admit(candidate(1, volume_id, generation_id, object_id))?
    else {
        return Err("candidate was not admitted".into());
    };
    let cancellation = CancellationToken::new();
    let first = crate::async_storage::poll_ready(execute_residency(
        &engine,
        &store,
        permit,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("memory speculation suspended")??;
    assert_eq!(first.value, 64);
    assert_eq!(first.work.backend_read_operations, 1);
    engine.finish(permit.candidate().operation_id, true)?;

    let stale = crate::async_storage::poll_ready(execute_residency(
        &engine,
        &store,
        permit,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("stale speculation suspended")?
    .err()
    .ok_or("stale speculation executed")?;
    assert!(matches!(
        stale.error,
        crate::storage::ObjectStoreError::Rejected(_)
    ));
    assert_eq!(*stale.work, WorkCounters::default());

    let warm = crate::async_storage::poll_ready(store.read(
        object_id,
        64,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("warm read suspended")??;
    assert_eq!(warm.work.backend_read_operations, 0);
    assert_eq!(warm.work.object_bytes_read, 0);
    assert_eq!(store.stats()?.hits, 1);
    assert_eq!(engine.metrics().useful, 1);
    assert_eq!(WorkCounters::default().backend_read_operations, 0);
    Ok(())
}

#[test]
fn every_accounting_failure_rolls_back_the_complete_residency_transition()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let object_id = object(&store, 11)?;
    let volume_id = VolumeId::from_bytes([3; 16]);
    let generation_id = generation(4);
    let mut engine = ResidencySpeculator::new(
        ResidencySpeculatorOptions {
            speculative_cost_basis_points: 10_000,
            ..ResidencySpeculatorOptions::default()
        },
        volume_id,
        generation_id,
    )?;
    engine.metrics.candidates = u64::MAX;
    let before_admit = engine.metrics();
    assert_eq!(
        engine.admit(candidate(1, volume_id, generation_id, object_id)),
        Err(ResidencySpeculatorError::Overflow)
    );
    assert_eq!(engine.metrics(), before_admit);
    assert!(engine.active.is_empty());

    engine.metrics.candidates = 0;
    engine.record_foreground(64)?;
    let ResidencyAdmission::Admitted(permit) =
        engine.admit(candidate(2, volume_id, generation_id, object_id))?
    else {
        return Err("rollback fixture was not admitted".into());
    };
    engine.metrics.wasted = u64::MAX;
    let before_replace = engine.metrics();
    assert_eq!(
        engine.replace_generation(generation(9)),
        Err(ResidencySpeculatorError::Overflow)
    );
    assert_eq!(engine.metrics(), before_replace);
    assert_eq!(engine.generation_id, generation_id);
    assert_eq!(
        engine.active.get(&permit.candidate().operation_id),
        Some(&permit.candidate())
    );

    engine.traffic.clear();
    engine.traffic.push_back(TrafficSample {
        speculative: false,
        bytes: u64::MAX,
    });
    let traffic_before = engine.traffic.len();
    assert_eq!(
        engine.record_foreground(1),
        Err(ResidencySpeculatorError::Overflow)
    );
    assert_eq!(engine.traffic.len(), traffic_before);
    assert_eq!(
        engine.traffic.front().map(|sample| sample.bytes),
        Some(u64::MAX)
    );
    Ok(())
}

#[test]
fn cancelled_residency_execution_is_inert_until_explicit_terminal_feedback()
-> Result<(), Box<dyn std::error::Error>> {
    let backend = MemoryObjectStore::default();
    let object_id = object(&backend, 12)?;
    let store = CachedObjectStore::new(backend, ObjectCacheOptions::default())?;
    let volume_id = VolumeId::from_bytes([3; 16]);
    let generation_id = generation(4);
    let mut engine = ResidencySpeculator::new(
        ResidencySpeculatorOptions {
            speculative_cost_basis_points: 10_000,
            ..ResidencySpeculatorOptions::default()
        },
        volume_id,
        generation_id,
    )?;
    engine.record_foreground(64)?;
    let ResidencyAdmission::Admitted(permit) =
        engine.admit(candidate(3, volume_id, generation_id, object_id))?
    else {
        return Err("cancellation fixture was not admitted".into());
    };
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let failure = crate::async_storage::poll_ready(execute_residency(
        &engine,
        &store,
        permit,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("cancelled residency suspended")?
    .err()
    .ok_or("cancelled residency executed")?;
    assert!(matches!(
        failure.error,
        crate::storage::ObjectStoreError::Cancelled
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    assert_eq!(engine.metrics().active, 1);
    assert_eq!(store.stats()?.resident_entries, 0);
    engine.finish(permit.candidate().operation_id, false)?;
    assert_eq!(engine.metrics().wasted, 1);
    Ok(())
}

#[test]
fn embedded_fs_executes_residency_against_its_one_shared_object_backend()
-> Result<(), Box<dyn std::error::Error>> {
    let backend = Arc::new(MemoryObjectStore::default());
    let object_id = object(&backend, 13)?;
    let store = CachedObjectStore::new(backend, ObjectCacheOptions::default())?;
    let fs = Fs::new(
        MemoryAuthorityStore::default(),
        store.clone(),
        EmbeddedCapabilities::MEMORY,
    );
    let volume_id = VolumeId::from_bytes([3; 16]);
    let generation_id = generation(4);
    let mut engine = ResidencySpeculator::new(
        ResidencySpeculatorOptions {
            speculative_cost_basis_points: 10_000,
            ..ResidencySpeculatorOptions::default()
        },
        volume_id,
        generation_id,
    )?;
    engine.record_foreground(64)?;
    let ResidencyAdmission::Admitted(permit) =
        engine.admit(candidate(4, volume_id, generation_id, object_id))?
    else {
        return Err("embedded fixture was not admitted".into());
    };
    assert_eq!(
        engine.active_permit(permit.candidate().operation_id),
        Some(permit)
    );
    let cancellation = CancellationToken::new();
    let receipt = crate::async_storage::poll_ready(fs.execute_residency(
        &engine,
        permit,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("embedded residency suspended")??;
    assert_eq!(receipt.value, 64);
    assert_eq!(store.stats()?.resident_entries, 1);
    Ok(())
}
