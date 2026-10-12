use super::*;
use crate::foundation::Digest;
use crate::storage::ObjectKind;

fn object(suffix: u8) -> ObjectId {
    let mut digest = [0_u8; 32];
    digest[8] = suffix;
    ObjectId {
        kind: ObjectKind::ExtentPage,
        digest: Digest::from_bytes(digest),
    }
}

#[test]
fn simultaneous_claims_produce_one_operation_wide_peak() -> Result<(), AllocationError> {
    let mut ledger = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let first = ledger.claim_elements::<u64>(8, &mut work, WorkBudget::UNBOUNDED)?;
    let second = ledger.claim_elements::<u32>(8, &mut work, WorkBudget::UNBOUNDED)?;
    assert_eq!(work.peak_allocation_bytes, 96);
    assert_eq!(work.allocation_operations, 2);
    ledger.release(first)?;
    ledger.release(second)?;
    assert_eq!(ledger.live_bytes(), 0);
    assert_eq!(work.peak_allocation_bytes, 96);
    Ok(())
}

#[test]
fn one_byte_short_budget_rejects_before_claim() -> Result<(), AllocationError> {
    let mut ledger = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let error = ledger
        .claim_elements::<u64>(
            8,
            &mut work,
            WorkBudget {
                peak_allocation_bytes: 63,
                allocation_operations: 1,
                ..WorkBudget::UNBOUNDED
            },
        )
        .err()
        .ok_or(AllocationError::AllocationFailed)?;
    assert!(matches!(
        error,
        AllocationError::Work(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            observed: 64,
            maximum: 63,
        })
    ));
    assert_eq!(ledger.live_bytes(), 0);
    assert_eq!(work, WorkCounters::default());
    Ok(())
}

#[test]
fn vector_capacity_and_ledger_corruption_guards_are_total() -> Result<(), AllocationError> {
    let mut capacity = LogicalVecCapacity::default();
    let mut values = Vec::<u64>::new();
    let mut ledger = AllocationLedger::default();
    let mut work = WorkCounters::default();
    capacity.ensure_for_push(
        &mut values,
        1,
        &mut ledger,
        &mut work,
        WorkBudget::UNBOUNDED,
    )?;
    let admitted = ledger.live_bytes();
    capacity.ensure_for_push(
        &mut values,
        1,
        &mut ledger,
        &mut work,
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(ledger.live_bytes(), admitted);
    values.push(1);
    assert_eq!(
        capacity.ensure_for_push(
            &mut values,
            1,
            &mut ledger,
            &mut work,
            WorkBudget::UNBOUNDED,
        ),
        Err(AllocationError::CapacityExceeded)
    );
    assert_eq!(
        ledger.release(admitted + 1),
        Err(AllocationError::ReleaseInvariant)
    );
    ledger.release(admitted)?;

    let mut overflow = AllocationLedger::default();
    overflow.claim_bytes(u64::MAX, 0, &mut work, WorkBudget::UNBOUNDED)?;
    assert_eq!(
        overflow.claim_bytes(1, 0, &mut work, WorkBudget::UNBOUNDED),
        Err(AllocationError::Overflow)
    );
    assert_eq!(
        VisitedObjectSet::slot_count(usize::MAX),
        Err(AllocationError::Overflow)
    );
    Ok(())
}

#[test]
fn visited_set_is_deterministic_bounded_and_detects_aliases() -> Result<(), crate::FsError> {
    let cancellation = crate::CancellationToken::new();
    let mut ledger = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let mut visited = VisitedObjectSet::new_with_accounting(2, &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, WorkBudget::UNBOUNDED, &cancellation))?;
    let first = VisitedObjectSet::insert_with_accounting(&mut visited, object(1), &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, WorkBudget::UNBOUNDED, &cancellation))?;
    let collision = VisitedObjectSet::insert_with_accounting(&mut visited, object(2), &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, WorkBudget::UNBOUNDED, &cancellation))?;
    let alias = VisitedObjectSet::insert_with_accounting(&mut visited, object(1), &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, WorkBudget::UNBOUNDED, &cancellation))?;
    assert!(first.inserted);
    assert!(collision.inserted);
    assert!(!alias.inserted);
    assert!(matches!(
        VisitedObjectSet::insert_with_accounting(&mut visited, object(3), &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, WorkBudget::UNBOUNDED, &cancellation)),
        Err(crate::FsError::PendingMutationAllocationFailed)
    ));
    visited.retire(&mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, WorkBudget::UNBOUNDED, &cancellation))?;
    assert_eq!(ledger.live_bytes(), 0);

    let mut scalable_ledger = AllocationLedger::default();
    let mut scalable_work = WorkCounters::default();
    let mut scalable = VisitedObjectSet::new_with_accounting(1_024, &mut KernelAccounting::borrowed_standalone(&mut scalable_ledger, &mut scalable_work, WorkBudget::UNBOUNDED, &cancellation))?;
    let initial_peak = scalable_work.peak_allocation_bytes;
    for suffix in 0..32 {
        assert!(
            VisitedObjectSet::insert_with_accounting(&mut scalable, object(suffix), &mut KernelAccounting::borrowed_standalone(&mut scalable_ledger, &mut scalable_work, WorkBudget::UNBOUNDED, &cancellation))?
                .inserted
        );
    }
    let eager_maximum_bytes = 2_u64
        .checked_mul(1_024)
        .and_then(|slots| slots.checked_mul(u64::try_from(size_of::<Option<ObjectId>>()).ok()?))
        .ok_or(WorkError::Overflow)?;
    assert!(initial_peak < eager_maximum_bytes);
    assert!(scalable_work.peak_allocation_bytes < eager_maximum_bytes);
    assert!(scalable_work.allocation_operations > 1);
    assert!(scalable_work.bytes_copied > 0);
    scalable.retire(&mut KernelAccounting::borrowed_standalone(&mut scalable_ledger, &mut scalable_work, WorkBudget::UNBOUNDED, &cancellation))?;
    assert_eq!(scalable_ledger.live_bytes(), 0);
    Ok(())
}

#[test]
fn visited_growth_failures_release_candidate_storage_and_preserve_the_old_table()
-> Result<(), crate::FsError> {
    let cancellation = crate::CancellationToken::new();
    let mut empty_ledger = AllocationLedger::default();
    let mut empty_work = WorkCounters::default();
    assert!(matches!(
        VisitedObjectSet::new_with_accounting(0, &mut KernelAccounting::borrowed_standalone(&mut empty_ledger, &mut empty_work, WorkBudget::UNBOUNDED, &cancellation)),
        Err(crate::FsError::PendingMutationAllocationFailed)
    ));
    assert_eq!(empty_ledger.live_bytes(), 0);

    for fail_on_copy in [false, true] {
        let mut ledger = AllocationLedger::default();
        let mut work = WorkCounters::default();
        let mut visited = VisitedObjectSet::new_with_accounting(16, &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, WorkBudget::UNBOUNDED, &cancellation))?;
        for suffix in 0..8 {
            assert!(
                VisitedObjectSet::insert_with_accounting(&mut visited, object(suffix), &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, WorkBudget::UNBOUNDED, &cancellation))?
                    .inserted
            );
        }
        let stable_bytes = ledger.live_bytes();
        let mut budget = WorkBudget::UNBOUNDED;
        if fail_on_copy {
            budget.bytes_copied = work.bytes_copied;
        } else {
            budget.items_examined = work.items_examined.saturating_add(1);
        }
        let failure = VisitedObjectSet::insert_with_accounting(&mut visited, object(9), &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, budget, &cancellation))
            .err()
            .ok_or(crate::FsError::PendingMutationAllocationFailed)?;
        assert!(matches!(failure, crate::FsError::Work(_)));
        assert_eq!(ledger.live_bytes(), stable_bytes);
        assert!(
            !VisitedObjectSet::insert_with_accounting(&mut visited, object(0), &mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, WorkBudget::UNBOUNDED, &cancellation))?
                .inserted
        );
        visited.retire(&mut KernelAccounting::borrowed_standalone(&mut ledger, &mut work, WorkBudget::UNBOUNDED, &cancellation))?;
        assert_eq!(ledger.live_bytes(), 0);
    }
    Ok(())
}

#[test]
fn vector_completed_growth_survives_a_late_budget_error() -> Result<(), AllocationError> {
    let mut values = Vec::<u64>::new();
    let mut capacity = LogicalVecCapacity::default();
    let mut ledger = AllocationLedger::default();
    let mut work = WorkCounters::UNCHARGED;
    let result = capacity.ensure_for_push_using(&mut values, 1, |error| error, |step| {
        match step {
            LogicalVecAllocationStep::Claim { bytes } =>
                ledger.claim_bytes(bytes, 1, &mut work, WorkBudget::UNBOUNDED),
            LogicalVecAllocationStep::ReserveFailed { bytes, .. } => {
                ledger.release(bytes)?;
                Err(AllocationError::AllocationFailed)
            }
            LogicalVecAllocationStep::Capacity { requested, actual } => {
                ledger.record_owned_bytes(actual - requested, &mut work)?;
                work.verify(WorkBudget { peak_allocation_bytes: 0, ..WorkBudget::UNBOUNDED })?;
                Ok(())
            }
        }
    });
    assert!(matches!(result, Err(AllocationError::Work(WorkError::BudgetExceeded { .. }))));
    let actual_bytes = usize_to_u64(values.capacity() * size_of::<u64>());
    assert_eq!(capacity.elements, 1);
    assert_eq!(capacity.logical_bytes(), actual_bytes);
    assert_eq!(ledger.live_bytes(), actual_bytes);
    assert_eq!(work.peak_allocation_bytes, actual_bytes);
    assert_eq!(work.allocation_operations, 1);
    drop(values);
    ledger.release(actual_bytes)?;
    Ok(())
}
