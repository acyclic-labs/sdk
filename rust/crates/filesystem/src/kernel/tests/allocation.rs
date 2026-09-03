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
fn visited_set_is_deterministic_bounded_and_detects_aliases() -> Result<(), AllocationError> {
    let mut ledger = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let mut visited = VisitedObjectSet::new(2, &mut ledger, &mut work, WorkBudget::UNBOUNDED)?;
    let first = visited.insert(object(1), &mut ledger, &mut work, WorkBudget::UNBOUNDED)?;
    let collision = visited.insert(object(2), &mut ledger, &mut work, WorkBudget::UNBOUNDED)?;
    let alias = visited.insert(object(1), &mut ledger, &mut work, WorkBudget::UNBOUNDED)?;
    assert_eq!(first.probes, 1);
    assert_eq!(collision.probes, 2);
    assert!(!alias.inserted);
    assert_eq!(alias.probes, 1);
    assert_eq!(
        visited.insert(object(3), &mut ledger, &mut work, WorkBudget::UNBOUNDED,),
        Err(AllocationError::CapacityExceeded)
    );
    visited.release(&mut ledger)?;
    assert_eq!(ledger.live_bytes(), 0);

    let mut scalable_ledger = AllocationLedger::default();
    let mut scalable_work = WorkCounters::default();
    let mut scalable = VisitedObjectSet::new(
        1_024,
        &mut scalable_ledger,
        &mut scalable_work,
        WorkBudget::UNBOUNDED,
    )?;
    let initial_peak = scalable_work.peak_allocation_bytes;
    for suffix in 0..32 {
        assert!(
            scalable
                .insert(
                    object(suffix),
                    &mut scalable_ledger,
                    &mut scalable_work,
                    WorkBudget::UNBOUNDED,
                )?
                .inserted
        );
    }
    let eager_maximum_bytes = 2_u64
        .checked_mul(1_024)
        .and_then(|slots| slots.checked_mul(u64::try_from(size_of::<Option<ObjectId>>()).ok()?))
        .ok_or(AllocationError::Overflow)?;
    assert!(initial_peak < eager_maximum_bytes);
    assert!(scalable_work.peak_allocation_bytes < eager_maximum_bytes);
    assert!(scalable_work.allocation_operations > 1);
    assert!(scalable_work.bytes_copied > 0);
    scalable.release(&mut scalable_ledger)?;
    assert_eq!(scalable_ledger.live_bytes(), 0);
    Ok(())
}

#[test]
fn visited_growth_failures_release_candidate_storage_and_preserve_the_old_table()
-> Result<(), AllocationError> {
    let mut empty_ledger = AllocationLedger::default();
    let mut empty_work = WorkCounters::default();
    assert!(matches!(
        VisitedObjectSet::new(0, &mut empty_ledger, &mut empty_work, WorkBudget::UNBOUNDED,),
        Err(AllocationError::InvalidCapacity)
    ));
    assert_eq!(empty_ledger.live_bytes(), 0);

    for fail_on_copy in [false, true] {
        let mut ledger = AllocationLedger::default();
        let mut work = WorkCounters::default();
        let mut visited = VisitedObjectSet::new(16, &mut ledger, &mut work, WorkBudget::UNBOUNDED)?;
        for suffix in 0..8 {
            assert!(
                visited
                    .insert(
                        object(suffix),
                        &mut ledger,
                        &mut work,
                        WorkBudget::UNBOUNDED,
                    )?
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
        let failure = visited
            .insert(object(9), &mut ledger, &mut work, budget)
            .err()
            .ok_or(AllocationError::AllocationFailed)?;
        assert!(matches!(failure, AllocationError::Work(_)));
        assert_eq!(ledger.live_bytes(), stable_bytes);
        assert!(
            !visited
                .insert(object(0), &mut ledger, &mut work, WorkBudget::UNBOUNDED,)?
                .inserted
        );
        visited.release(&mut ledger)?;
        assert_eq!(ledger.live_bytes(), 0);
    }
    Ok(())
}
