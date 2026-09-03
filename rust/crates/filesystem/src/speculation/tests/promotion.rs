use super::*;
use crate::foundation::Digest;
use crate::memory::{MemoryAuthorityStore, MemoryObjectStore};
use crate::simulation::{Simulation, SimulationFault, SimulationOperation, SimulationOptions};
use crate::storage::{
    ObjectKind, ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectReceipt, ObjectStore,
    ObjectStoreError, object_digest,
};
use crate::{AsyncObjectStore, CancellationToken, WorkCounters};
use bytes::Bytes;
use std::cell::Cell;
use std::future::Future;

struct CountingExecutor {
    calls: Cell<u32>,
}

struct UnderreportedOwnedStore {
    inner: MemoryObjectStore,
}

impl AsyncObjectStore for UnderreportedOwnedStore {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        AsyncObjectStore::put(&self.inner, object_id, bytes, budget, cancellation).await
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        let mut receipt =
            AsyncObjectStore::read(&self.inner, object_id, maximum_bytes, budget, cancellation)
                .await?;
        receipt.value.retention = ObjectReadRetention::Owned { logical_bytes: 65 };
        Ok(receipt)
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        AsyncObjectStore::read_many(&self.inner, requests, budget, cancellation).await
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        AsyncObjectStore::contains(&self.inner, object_id, budget, cancellation).await
    }
}

impl PromotionExecutor for CountingExecutor {
    fn promote(
        &self,
        _plan: &PromotionPlan,
        _budget: WorkBudget,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<()>> {
        self.calls.set(self.calls.get() + 1);
        std::future::ready(Ok(ObjectReceipt {
            value: (),
            work: WorkCounters::default(),
        }))
    }
}
use crate::{ResidencyAdmission, ResidencySpeculator};

fn generation(byte: u8) -> GenerationId {
    GenerationId::new(Digest::from_bytes([byte; 32]))
}

fn object(byte: u8) -> ObjectId {
    ObjectId {
        kind: ObjectKind::Blob,
        digest: object_digest(ObjectKind::Blob, &[byte; 64]),
    }
}

fn candidate(
    operation_byte: u8,
    volume_id: VolumeId,
    generation_id: GenerationId,
    object_id: ObjectId,
) -> PromotionCandidate {
    PromotionCandidate {
        operation_id: OperationId::from_bytes([operation_byte; 16]),
        volume_id,
        generation_id,
        request: ObjectReadRequest {
            object_id,
            maximum_bytes: 64,
        },
        accepted_tiers: vec![StorageTier::NodeLocal, StorageTier::SharedCache],
        reason: ResidencyReason::SequentialRange,
    }
}

fn fact(object_id: ObjectId, location_byte: u8, tier: StorageTier) -> ObjectResidency {
    ObjectResidency {
        object_id,
        location_id: StorageLocationId::from_bytes([location_byte; 16]),
        tier,
        source_priority: u16::from(location_byte),
    }
}

fn destination(location_byte: u8, tier: StorageTier, priority: u16) -> PromotionDestination {
    PromotionDestination {
        location_id: StorageLocationId::from_bytes([location_byte; 16]),
        tier,
        writable: true,
        maximum_object_bytes: 1024,
        priority,
        cost_units_per_byte: 2,
    }
}

fn planned(
    engine: &mut PromotionSpeculator,
    operation_byte: u8,
    volume_id: VolumeId,
    generation_id: GenerationId,
    object_id: ObjectId,
) -> Result<PromotionPlan, Box<dyn std::error::Error>> {
    let source = fact(object_id, 4, StorageTier::DurableOrigin);
    let destination = destination(5, StorageTier::NodeLocal, 0);
    let PromotionAdmission::Planned(plan) = engine.plan(
        candidate(operation_byte, volume_id, generation_id, object_id),
        &[source],
        &[destination],
    )?
    else {
        return Err("promotion was not planned".into());
    };
    Ok(plan)
}

#[test]
fn option_validation_location_identity_and_executor_accessors_are_total()
-> Result<(), Box<dyn std::error::Error>> {
    let defaults = PromotionSpeculatorOptions::default();
    for invalid in [
        PromotionSpeculatorOptions {
            maximum_active_operations: 0,
            ..defaults
        },
        PromotionSpeculatorOptions {
            maximum_active_bytes: 0,
            ..defaults
        },
        PromotionSpeculatorOptions {
            maximum_active_cost_units: 0,
            ..defaults
        },
        PromotionSpeculatorOptions {
            maximum_residency_facts: 0,
            ..defaults
        },
        PromotionSpeculatorOptions {
            maximum_destinations: 0,
            ..defaults
        },
        PromotionSpeculatorOptions {
            maximum_accepted_tiers: 0,
            ..defaults
        },
        PromotionSpeculatorOptions {
            outcome_window: 0,
            ..defaults
        },
        PromotionSpeculatorOptions {
            minimum_usefulness_samples: 0,
            ..defaults
        },
    ] {
        assert_eq!(invalid.validate(), Err(PromotionSpeculatorError::ZeroBound));
    }
    assert_eq!(
        PromotionSpeculatorOptions {
            outcome_window: 1,
            minimum_usefulness_samples: 2,
            ..defaults
        }
        .validate(),
        Err(PromotionSpeculatorError::UsefulnessWindowTooSmall)
    );
    assert_eq!(
        PromotionSpeculatorOptions {
            minimum_usefulness_basis_points: 10_001,
            ..defaults
        }
        .validate(),
        Err(PromotionSpeculatorError::InvalidBasisPoints)
    );

    let source_id = StorageLocationId::from_bytes([61; 16]);
    let destination_id = StorageLocationId::from_bytes([62; 16]);
    assert_eq!(source_id.into_bytes(), [61; 16]);
    let same = StorePromotionExecutor::new(
        source_id,
        MemoryObjectStore::default(),
        source_id,
        MemoryObjectStore::default(),
    )
    .err()
    .ok_or("same-location executor was admitted")?;
    assert_eq!(same, StorePromotionExecutorError::SameLocation);
    let executor = StorePromotionExecutor::new(
        source_id,
        MemoryObjectStore::default(),
        destination_id,
        MemoryObjectStore::default(),
    )?;
    assert!(!std::ptr::eq(executor.source(), executor.destination()));
    Ok(())
}

#[test]
fn input_and_active_capacity_rejections_are_independent() -> Result<(), Box<dyn std::error::Error>>
{
    let volume_id = VolumeId::from_bytes([71; 16]);
    let generation_id = generation(72);
    let object_id = object(73);
    let source = fact(object_id, 74, StorageTier::DurableOrigin);
    let writable = destination(75, StorageTier::NodeLocal, 0);
    let defaults = PromotionSpeculatorOptions::default();

    let mut engine = PromotionSpeculator::new(defaults, volume_id, generation_id)?;
    assert_eq!(
        engine.plan(
            candidate(1, VolumeId::from_bytes([99; 16]), generation_id, object_id),
            &[source],
            &[writable],
        )?,
        PromotionAdmission::Rejected(PromotionRejection::WrongVolume)
    );

    let mut input_limited = PromotionSpeculator::new(
        PromotionSpeculatorOptions {
            maximum_residency_facts: 1,
            ..defaults
        },
        volume_id,
        generation_id,
    )?;
    assert_eq!(
        input_limited.plan(
            candidate(2, volume_id, generation_id, object_id),
            &[source, fact(object_id, 76, StorageTier::SharedCache),],
            &[writable],
        )?,
        PromotionAdmission::Rejected(PromotionRejection::InputCapacity)
    );

    let mut active_limited = PromotionSpeculator::new(
        PromotionSpeculatorOptions {
            maximum_active_bytes: 63,
            ..defaults
        },
        volume_id,
        generation_id,
    )?;
    assert_eq!(
        active_limited.plan(
            candidate(3, volume_id, generation_id, object_id),
            &[source],
            &[writable],
        )?,
        PromotionAdmission::Rejected(PromotionRejection::ActiveCapacity)
    );
    Ok(())
}

#[test]
fn bounded_outcome_window_and_failure_overflow_are_exact() -> Result<(), Box<dyn std::error::Error>>
{
    let volume_id = VolumeId::from_bytes([81; 16]);
    let generation_id = generation(82);
    let mut engine = PromotionSpeculator::new(
        PromotionSpeculatorOptions {
            outcome_window: 1,
            minimum_usefulness_samples: 1,
            minimum_usefulness_basis_points: 0,
            ..PromotionSpeculatorOptions::default()
        },
        volume_id,
        generation_id,
    )?;
    for (operation, useful) in [(1, true), (2, false)] {
        let object_id = object(operation);
        let plan = planned(&mut engine, operation, volume_id, generation_id, object_id)?;
        engine.finish(plan.candidate.operation_id, useful)?;
    }
    assert_eq!(engine.outcomes.len(), 1);
    assert_eq!(engine.metrics().useful, 1);
    assert_eq!(engine.metrics().wasted, 1);

    let prior = WorkCounters {
        object_probes: u64::MAX,
        ..WorkCounters::default()
    };
    let nested = ObjectFailure::new(
        ObjectStoreError::Missing,
        WorkCounters {
            object_probes: 1,
            ..WorkCounters::default()
        },
    );
    let overflow = merge_promotion_failure(prior, 0, nested);
    assert!(matches!(
        overflow.error,
        ObjectStoreError::Work(WorkError::Overflow)
    ));
    assert_eq!(*overflow.work, prior);
    Ok(())
}

#[test]
fn planning_is_deterministic_bounded_generation_fenced_and_idempotent()
-> Result<(), Box<dyn std::error::Error>> {
    let volume_id = VolumeId::from_bytes([1; 16]);
    let generation_id = generation(2);
    let object_id = object(3);
    let source = fact(object_id, 4, StorageTier::DurableOrigin);
    let destinations = [
        destination(8, StorageTier::SharedCache, 0),
        destination(7, StorageTier::NodeLocal, 10),
        destination(6, StorageTier::NodeLocal, 1),
    ];
    let mut engine = PromotionSpeculator::new(
        PromotionSpeculatorOptions {
            maximum_active_operations: 1,
            maximum_active_bytes: 64,
            maximum_active_cost_units: 128,
            ..PromotionSpeculatorOptions::default()
        },
        volume_id,
        generation_id,
    )?;
    assert_eq!(
        engine.plan(
            candidate(1, volume_id, generation(9), object_id),
            &[source],
            &destinations,
        )?,
        PromotionAdmission::Rejected(PromotionRejection::StaleGeneration)
    );
    let requested = candidate(2, volume_id, generation_id, object_id);
    let PromotionAdmission::Planned(plan) =
        engine.plan(requested.clone(), &[source], &destinations)?
    else {
        return Err("promotion was not planned".into());
    };
    assert_eq!(plan.source, source);
    assert_eq!(
        plan.destination.location_id,
        StorageLocationId::from_bytes([6; 16])
    );
    assert_eq!(plan.estimated_cost_units, 128);
    assert_eq!(
        engine.plan(
            candidate(3, volume_id, generation_id, object_id),
            &[source],
            &destinations
        )?,
        PromotionAdmission::Rejected(PromotionRejection::DuplicateObject)
    );
    let second_object = object(9);
    let duplicate_operation = candidate(2, volume_id, generation_id, second_object);
    assert_eq!(
        engine.plan(
            duplicate_operation,
            &[fact(second_object, 4, StorageTier::DurableOrigin)],
            &destinations,
        )?,
        PromotionAdmission::Rejected(PromotionRejection::DuplicateOperation)
    );
    engine.finish(requested.operation_id, true)?;
    assert_eq!(engine.metrics().useful, 1);
    let executor = CountingExecutor {
        calls: Cell::new(0),
    };
    let cancellation = CancellationToken::new();
    let stale = crate::async_storage::poll_ready(execute_promotion(
        &engine,
        &executor,
        &plan,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("stale promotion suspended")?
    .err()
    .ok_or("stale promotion executed")?;
    assert!(matches!(stale.error, ObjectStoreError::Rejected(_)));
    assert_eq!(*stale.work, WorkCounters::default());
    assert_eq!(executor.calls.get(), 0);

    let local = fact(object_id, 6, StorageTier::NodeLocal);
    assert_eq!(
        engine.plan(
            candidate(4, volume_id, generation_id, object_id),
            &[source, local],
            &destinations
        )?,
        PromotionAdmission::Satisfied(local)
    );
    assert_eq!(engine.metrics().active, 0);
    assert_eq!(engine.metrics().active_bytes, 0);
    Ok(())
}

#[test]
fn residency_handoff_preemption_and_adaptive_feedback_are_exact()
-> Result<(), Box<dyn std::error::Error>> {
    let volume_id = VolumeId::from_bytes([1; 16]);
    let generation_id = generation(2);
    let first = object(3);
    let second = object(4);
    let mut residency = ResidencySpeculator::new(
        super::super::ResidencySpeculatorOptions {
            speculative_cost_basis_points: 10_000,
            ..super::super::ResidencySpeculatorOptions::default()
        },
        volume_id,
        generation_id,
    )?;
    residency.record_foreground(128)?;
    let ResidencyAdmission::Admitted(permit) =
        residency.admit(super::super::ResidencyCandidate {
            operation_id: OperationId::from_bytes([5; 16]),
            volume_id,
            generation_id,
            request: ObjectReadRequest {
                object_id: first,
                maximum_bytes: 64,
            },
            reason: ResidencyReason::DirectorySuccessor,
        })?
    else {
        return Err("residency was not admitted".into());
    };
    let promotion = PromotionCandidate::from_residency(
        permit,
        vec![StorageTier::NodeLocal, StorageTier::SharedCache],
    );
    assert_eq!(promotion.reason, ResidencyReason::DirectorySuccessor);

    let options = PromotionSpeculatorOptions {
        maximum_active_operations: 1,
        maximum_active_bytes: 64,
        maximum_active_cost_units: 128,
        outcome_window: 2,
        minimum_usefulness_samples: 2,
        minimum_usefulness_basis_points: 5_000,
        ..PromotionSpeculatorOptions::default()
    };
    let mut engine = PromotionSpeculator::new(options, volume_id, generation_id)?;
    let source_first = fact(first, 10, StorageTier::DurableOrigin);
    let node = destination(11, StorageTier::NodeLocal, 0);
    assert!(matches!(
        engine.plan(promotion, &[source_first], &[node])?,
        PromotionAdmission::Planned(_)
    ));
    assert_eq!(
        engine.preempt_for_foreground()?,
        [OperationId::from_bytes([5; 16])]
    );

    let source_second = fact(second, 10, StorageTier::DurableOrigin);
    let second_candidate = candidate(6, volume_id, generation_id, second);
    assert!(matches!(
        engine.plan(second_candidate.clone(), &[source_second], &[node])?,
        PromotionAdmission::Planned(_)
    ));
    engine.finish(second_candidate.operation_id, false)?;
    assert_eq!(
        engine.plan(
            candidate(7, volume_id, generation_id, first),
            &[source_first],
            &[node]
        )?,
        PromotionAdmission::Rejected(PromotionRejection::LowUsefulness)
    );
    assert!(engine.replace_generation(generation(9))?.is_empty());
    assert_eq!(engine.metrics().wasted, 2);
    Ok(())
}

#[test]
fn malformed_and_unavailable_inputs_reject_before_a_plan_exists()
-> Result<(), Box<dyn std::error::Error>> {
    let volume_id = VolumeId::from_bytes([1; 16]);
    let generation_id = generation(2);
    let object_id = object(3);
    let mut engine = PromotionSpeculator::new(
        PromotionSpeculatorOptions {
            maximum_residency_facts: 1,
            maximum_destinations: 1,
            ..PromotionSpeculatorOptions::default()
        },
        volume_id,
        generation_id,
    )?;
    let mut invalid = candidate(1, volume_id, generation_id, object_id);
    invalid.accepted_tiers = vec![StorageTier::NodeLocal, StorageTier::NodeLocal];
    assert_eq!(
        engine.plan(invalid, &[], &[])?,
        PromotionAdmission::Rejected(PromotionRejection::InvalidRequest)
    );
    assert_eq!(
        engine.plan(candidate(2, volume_id, generation_id, object_id), &[], &[])?,
        PromotionAdmission::Rejected(PromotionRejection::MissingSource)
    );
    let source = fact(object_id, 4, StorageTier::DurableOrigin);
    let mut unavailable = destination(5, StorageTier::NodeLocal, 0);
    unavailable.writable = false;
    assert_eq!(
        engine.plan(
            candidate(3, volume_id, generation_id, object_id),
            &[source],
            &[unavailable]
        )?,
        PromotionAdmission::Rejected(PromotionRejection::NoDestination)
    );
    assert_eq!(engine.metrics().active, 0);
    Ok(())
}

#[test]
fn canonical_store_executor_authenticates_moves_and_retries_without_a_second_body()
-> Result<(), Box<dyn std::error::Error>> {
    let volume_id = VolumeId::from_bytes([1; 16]);
    let generation_id = generation(2);
    let bytes = Bytes::from(vec![3; 64]);
    let object_id = object(3);
    let source = MemoryObjectStore::default();
    ObjectStore::put(&source, object_id, bytes.clone(), WorkBudget::UNBOUNDED)?;
    let destination = MemoryObjectStore::default();
    let mut engine = PromotionSpeculator::new(
        PromotionSpeculatorOptions::default(),
        volume_id,
        generation_id,
    )?;
    let plan = planned(&mut engine, 8, volume_id, generation_id, object_id)?;
    let executor = StorePromotionExecutor::new(
        plan.source.location_id,
        source,
        plan.destination.location_id,
        destination,
    )?;
    let cancellation = CancellationToken::new();
    for _ in 0..2 {
        let receipt = crate::async_storage::poll_ready(execute_promotion(
            &engine,
            &executor,
            &plan,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("immediate promotion suspended")??;
        assert_eq!(receipt.work.backend_read_operations, 1);
        assert_eq!(receipt.work.backend_write_operations, 1);
        assert_eq!(receipt.work.object_bytes_read, 64);
        assert_eq!(receipt.work.object_bytes_written, 64);
    }
    let admitted = ObjectStore::read(executor.destination(), object_id, 64, WorkBudget::UNBOUNDED)?;
    assert_eq!(admitted.value.bytes, bytes);

    let wrong = StorePromotionExecutor::new(
        StorageLocationId::from_bytes([7; 16]),
        MemoryObjectStore::default(),
        StorageLocationId::from_bytes([8; 16]),
        MemoryObjectStore::default(),
    )?;
    let failure = crate::async_storage::poll_ready(execute_promotion(
        &engine,
        &wrong,
        &plan,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("mismatched promotion suspended")?
    .err()
    .ok_or("mismatched promotion executed")?;
    assert!(matches!(failure.error, ObjectStoreError::Rejected(_)));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn promotion_rejects_owned_retention_that_exceeds_the_admitted_live_peak()
-> Result<(), Box<dyn std::error::Error>> {
    let volume_id = VolumeId::from_bytes([31; 16]);
    let generation_id = generation(32);
    let object_id = object(33);
    let source = UnderreportedOwnedStore {
        inner: MemoryObjectStore::default(),
    };
    ObjectStore::put(
        &source.inner,
        object_id,
        Bytes::from(vec![33; 64]),
        WorkBudget::UNBOUNDED,
    )?;
    let mut engine = PromotionSpeculator::new(
        PromotionSpeculatorOptions::default(),
        volume_id,
        generation_id,
    )?;
    let plan = planned(&mut engine, 34, volume_id, generation_id, object_id)?;
    let executor = StorePromotionExecutor::new(
        plan.source.location_id,
        source,
        plan.destination.location_id,
        MemoryObjectStore::default(),
    )?;
    let failure = crate::async_storage::poll_ready(execute_promotion(
        &engine,
        &executor,
        &plan,
        WorkBudget {
            peak_allocation_bytes: 64,
            ..WorkBudget::UNBOUNDED
        },
        &CancellationToken::new(),
    ))
    .ok_or("owned-retention promotion blocked")?
    .err()
    .ok_or("underreported owned retention was accepted")?;
    assert!(matches!(
        failure.error,
        ObjectStoreError::Work(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            observed: 65,
            maximum: 64,
        })
    ));
    assert_eq!(failure.work.backend_write_operations, 0);
    Ok(())
}

#[test]
fn simulated_remote_promotion_fails_exactly_and_retries_idempotently()
-> Result<(), Box<dyn std::error::Error>> {
    let volume_id = VolumeId::from_bytes([1; 16]);
    let generation_id = generation(2);
    let object_id = object(3);
    let source_inner = MemoryObjectStore::default();
    ObjectStore::put(
        &source_inner,
        object_id,
        Bytes::from(vec![3; 64]),
        WorkBudget::UNBOUNDED,
    )?;
    let source_simulation = Simulation::wrap(
        MemoryAuthorityStore::default(),
        source_inner,
        SimulationOptions::default(),
    )?;
    let destination_simulation = Simulation::default();
    destination_simulation.schedule_next(
        SimulationOperation::ObjectPut,
        SimulationFault::PartitionBefore,
    )?;
    let (_, source) = source_simulation.stores();
    let (_, destination) = destination_simulation.stores();
    let mut engine = PromotionSpeculator::new(
        PromotionSpeculatorOptions::default(),
        volume_id,
        generation_id,
    )?;
    let plan = planned(&mut engine, 9, volume_id, generation_id, object_id)?;
    let executor = StorePromotionExecutor::new(
        plan.source.location_id,
        source,
        plan.destination.location_id,
        destination,
    )?;
    let cancellation = CancellationToken::new();
    let failed = crate::async_storage::poll_ready(execute_promotion(
        &engine,
        &executor,
        &plan,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("simulated failed promotion suspended")?
    .err()
    .ok_or("scheduled partition did not fail")?;
    assert!(matches!(failed.error, ObjectStoreError::Rejected(_)));
    assert_eq!(failed.work.backend_read_operations, 1);
    assert_eq!(failed.work.backend_write_operations, 0);

    let retried = crate::async_storage::poll_ready(execute_promotion(
        &engine,
        &executor,
        &plan,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("simulated retry suspended")??;
    assert_eq!(retried.work.backend_read_operations, 1);
    assert_eq!(retried.work.backend_write_operations, 1);
    let contains = crate::async_storage::poll_ready(executor.destination().contains(
        object_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("simulated destination probe suspended")??;
    assert!(contains.value);
    assert_eq!(source_simulation.trace()?.len(), 2);
    assert_eq!(destination_simulation.trace()?.len(), 3);
    Ok(())
}

#[test]
fn every_accounting_failure_rolls_back_the_complete_promotion_transition()
-> Result<(), Box<dyn std::error::Error>> {
    let volume_id = VolumeId::from_bytes([1; 16]);
    let generation_id = generation(2);
    let object_id = object(3);
    let source = fact(object_id, 4, StorageTier::DurableOrigin);
    let destination = destination(5, StorageTier::NodeLocal, 0);
    let mut engine = PromotionSpeculator::new(
        PromotionSpeculatorOptions::default(),
        volume_id,
        generation_id,
    )?;
    engine.metrics.candidates = u64::MAX;
    let before_plan = engine.metrics();
    assert_eq!(
        engine.plan(
            candidate(1, volume_id, generation_id, object_id),
            &[source],
            &[destination],
        ),
        Err(PromotionSpeculatorError::Overflow)
    );
    assert_eq!(engine.metrics(), before_plan);
    assert!(engine.active.is_empty());

    engine.metrics.candidates = 0;
    let plan = planned(&mut engine, 2, volume_id, generation_id, object_id)?;
    engine.metrics.wasted = u64::MAX;
    let before_replace = engine.metrics();
    assert_eq!(
        engine.replace_generation(generation(9)),
        Err(PromotionSpeculatorError::Overflow)
    );
    assert_eq!(engine.metrics(), before_replace);
    assert_eq!(engine.generation_id, generation_id);
    assert_eq!(engine.active.get(&plan.candidate.operation_id), Some(&plan));
    Ok(())
}

#[test]
fn cancelled_promotion_execution_writes_nothing_until_explicit_feedback()
-> Result<(), Box<dyn std::error::Error>> {
    let volume_id = VolumeId::from_bytes([1; 16]);
    let generation_id = generation(2);
    let object_id = object(3);
    let source = MemoryObjectStore::default();
    ObjectStore::put(
        &source,
        object_id,
        Bytes::from(vec![3; 64]),
        WorkBudget::UNBOUNDED,
    )?;
    let destination = MemoryObjectStore::default();
    let mut engine = PromotionSpeculator::new(
        PromotionSpeculatorOptions::default(),
        volume_id,
        generation_id,
    )?;
    let plan = planned(&mut engine, 10, volume_id, generation_id, object_id)?;
    let executor = StorePromotionExecutor::new(
        plan.source.location_id,
        source,
        plan.destination.location_id,
        destination,
    )?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let failure = crate::async_storage::poll_ready(execute_promotion(
        &engine,
        &executor,
        &plan,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("cancelled promotion suspended")?
    .err()
    .ok_or("cancelled promotion executed")?;
    assert!(matches!(failure.error, ObjectStoreError::Cancelled));
    assert_eq!(*failure.work, WorkCounters::default());
    assert!(
        !ObjectStore::contains(executor.destination(), object_id, WorkBudget::UNBOUNDED,)?.value
    );
    assert_eq!(engine.metrics().active, 1);
    engine.finish(plan.candidate.operation_id, false)?;
    assert_eq!(engine.metrics().wasted, 1);
    Ok(())
}
