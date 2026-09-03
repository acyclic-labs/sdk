use super::*;
use crate::foundation::Digest;
use crate::kernel::attribute_mutation::AttributeFormat;
use crate::kernel::{AttributeClass, AttributeEntry, AttributeName, CanonicalDecodeError};
use crate::performance::WorkError;
use crate::storage::{ObjectKind, ObjectStoreError};

fn name(byte: u8) -> Result<AttributeName, crate::kernel::AttributeError> {
    AttributeName::new(AttributeClass::PosixXattr, vec![byte], 1)
}

fn entry(byte: u8) -> Result<AttributeEntry, crate::kernel::AttributeError> {
    Ok(AttributeEntry {
        name: name(byte)?,
        value_bytes: u64::from(byte),
        value: ObjectId {
            kind: ObjectKind::Blob,
            digest: Digest::from_bytes([byte; 32]),
        },
    })
}

fn child(byte: u8) -> Result<Child<AttributeName>, crate::kernel::AttributeError> {
    Ok(Child {
        first: name(byte)?,
        page: ObjectId {
            kind: ObjectKind::AttributePage,
            digest: Digest::from_bytes([byte; 32]),
        },
    })
}

fn exact_internal_helpers_are_bounded() -> Result<(), Box<dyn std::error::Error>> {
    assert!(admit_height(1, 1).is_ok());
    assert!(matches!(admit_height(2, 1), Err(Error::HeightExceeded)));
    assert_eq!(next_index(0)?, 1);
    assert_eq!(next_index(7)?, 8);
    assert!(matches!(next_index(usize::MAX), Err(Error::InvalidRouting)));
    assert_eq!(sort_admission_bound(2)?, 12);
    assert_eq!(sort_admission_bound(3)?, 18);
    assert_eq!(sort_admission_bound(4)?, 36);
    assert_eq!(heap_parent_count(0), 0);
    assert_eq!(heap_parent_count(1), 0);
    assert_eq!(heap_parent_count(2), 1);
    assert_eq!(heap_parent_count(5), 2);
    assert_eq!(
        charge_work(WorkCounters::default(), 3, WorkBudget::UNBOUNDED)?,
        WorkCounters {
            items_examined: 3,
            ..WorkCounters::default()
        }
    );
    assert!(matches!(
        charge_work(
            WorkCounters::default(),
            1,
            WorkBudget {
                items_examined: 0,
                ..WorkBudget::UNBOUNDED
            }
        ),
        Err(WorkError::BudgetExceeded {
            counter: "items_examined",
            observed: 1,
            maximum: 0
        })
    ));

    Ok(())
}

#[test]
fn stable_borrowed_sort_preserves_duplicate_ordinals_and_charges_exact_work()
-> Result<(), Box<dyn std::error::Error>> {
    exact_internal_helpers_are_bounded()?;
    let keys = [name(3)?, name(1)?, name(2)?, name(1)?];
    let mut indexed = keys
        .iter()
        .enumerate()
        .map(|(ordinal, key)| IndexedKey { ordinal, key })
        .collect::<Vec<_>>();
    let mut work = WorkCounters::default();
    sort_indexed(&mut indexed, &mut work, WorkBudget::UNBOUNDED)?;
    assert_eq!(
        indexed
            .iter()
            .map(|value| (value.key.as_bytes()[0], value.ordinal))
            .collect::<Vec<_>>(),
        vec![(1, 1), (1, 3), (2, 2), (3, 0)]
    );
    assert_eq!(work.items_examined, 7);

    let mut budgeted = keys
        .iter()
        .enumerate()
        .map(|(ordinal, key)| IndexedKey { ordinal, key })
        .collect::<Vec<_>>();
    sort_indexed(
        &mut budgeted,
        &mut WorkCounters::default(),
        WorkBudget {
            items_examined: 37,
            ..WorkBudget::UNBOUNDED
        },
    )?;
    let mut under_admitted = keys
        .iter()
        .enumerate()
        .map(|(ordinal, key)| IndexedKey { ordinal, key })
        .collect::<Vec<_>>();
    assert!(matches!(
        sort_indexed(
            &mut under_admitted,
            &mut WorkCounters::default(),
            WorkBudget {
                items_examined: 20,
                ..WorkBudget::UNBOUNDED
            }
        ),
        Err(Error::Work(WorkError::BudgetExceeded { .. }))
    ));

    let mut already_ordered = indexed;
    let mut ordered_work = WorkCounters::default();
    sort_indexed(
        &mut already_ordered,
        &mut ordered_work,
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(ordered_work.items_examined, 3);
    let mut ordered_budgeted = already_ordered;
    sort_indexed(
        &mut ordered_budgeted,
        &mut WorkCounters::default(),
        WorkBudget {
            items_examined: 3,
            ..WorkBudget::UNBOUNDED
        },
    )?;

    let mut singleton = [IndexedKey {
        ordinal: 0,
        key: &keys[0],
    }];
    let mut singleton_work = WorkCounters::default();
    sort_indexed(&mut singleton, &mut singleton_work, WorkBudget::UNBOUNDED)?;
    assert_eq!(singleton_work, WorkCounters::default());

    let mut rejected = [
        IndexedKey {
            ordinal: 0,
            key: &keys[1],
        },
        IndexedKey {
            ordinal: 1,
            key: &keys[0],
        },
    ];
    let mut rejected_work = WorkCounters::default();
    assert!(matches!(
        sort_indexed(&mut rejected, &mut rejected_work, WorkBudget::default()),
        Err(Error::Work(WorkError::BudgetExceeded {
            counter: "items_examined",
            observed: 1,
            maximum: 0
        }))
    ));
    assert_eq!(rejected_work, WorkCounters::default());
    Ok(())
}

#[test]
fn grouping_search_and_child_routing_cover_every_boundary() -> Result<(), Box<dyn std::error::Error>>
{
    let keys = [name(1)?, name(1)?, name(3)?, name(5)?];
    let indexed = keys
        .iter()
        .enumerate()
        .map(|(ordinal, key)| IndexedKey { ordinal, key })
        .collect::<Vec<_>>();
    assert_eq!(equal_group(&indexed, 0, indexed.len()), (2, 2));
    assert_eq!(equal_group(&indexed, 2, indexed.len()), (3, 1));
    assert_eq!(equal_group(&indexed, 3, indexed.len()), (4, 0));

    let values = [entry(2)?, entry(4)?, entry(6)?];
    assert_eq!(search::<AttributeFormat>(&values, &name(2)?), (Ok(0), 2));
    assert_eq!(search::<AttributeFormat>(&values, &name(4)?), (Ok(1), 1));
    assert_eq!(search::<AttributeFormat>(&values, &name(6)?), (Ok(2), 2));
    assert_eq!(search::<AttributeFormat>(&values, &name(1)?), (Err(0), 2));
    assert_eq!(search::<AttributeFormat>(&values, &name(3)?), (Err(1), 2));
    assert_eq!(search::<AttributeFormat>(&values, &name(7)?), (Err(3), 2));

    let children = [child(2)?, child(4)?, child(6)?];
    assert_eq!(route(&children, &name(1)?), (0, 2));
    assert_eq!(route(&children, &name(2)?), (0, 2));
    assert_eq!(route(&children, &name(5)?), (1, 2));
    assert_eq!(route(&children, &name(6)?), (2, 2));
    assert_eq!(route(&children, &name(9)?), (2, 2));
    assert_eq!(route::<AttributeName>(&[], &name(1)?), (0, 0));
    Ok(())
}

#[test]
fn page_bounds_are_exact_for_leaf_and_internal_frontiers() -> Result<(), Box<dyn std::error::Error>>
{
    let values = [entry(1)?, entry(3)?];
    assert!(validate_values::<AttributeFormat>(&values, None, None).is_ok());
    assert!(validate_values::<AttributeFormat>(&values, Some(&name(1)?), Some(&name(4)?)).is_ok());
    assert!(matches!(
        validate_values::<AttributeFormat>(&values, Some(&name(2)?), None),
        Err(Error::ChildBoundsMismatch)
    ));
    assert!(matches!(
        validate_values::<AttributeFormat>(&values, None, Some(&name(3)?)),
        Err(Error::ChildBoundsMismatch)
    ));
    assert!(validate_values::<AttributeFormat>(&[], None, Some(&name(1)?)).is_ok());

    let children = [child(1)?, child(3)?];
    assert!(validate_children::<AttributeFormat>(&children, None, None).is_ok());
    assert!(
        validate_children::<AttributeFormat>(&children, Some(&name(1)?), Some(&name(4)?)).is_ok()
    );
    assert!(matches!(
        validate_children::<AttributeFormat>(&children, Some(&name(2)?), None),
        Err(Error::ChildBoundsMismatch)
    ));
    assert!(matches!(
        validate_children::<AttributeFormat>(&children, None, Some(&name(3)?)),
        Err(Error::ChildBoundsMismatch)
    ));
    assert!(validate_children::<AttributeFormat>(&[], None, Some(&name(1)?)).is_ok());
    Ok(())
}

#[test]
fn allocation_and_page_io_error_translation_is_total() {
    for allocation in [
        AllocationError::Overflow,
        AllocationError::ReleaseInvariant,
        AllocationError::InvalidCapacity,
        AllocationError::CapacityExceeded,
        AllocationError::AllocationFailed,
    ] {
        assert!(matches!(
            map_allocation(allocation),
            Error::AllocationFailed
        ));
    }
    assert!(matches!(
        map_allocation(AllocationError::Work(WorkError::Overflow)),
        Error::Work(WorkError::Overflow)
    ));

    assert!(matches!(
        map_io(persistent_io::Error::AllocationFailed),
        Error::AllocationFailed
    ));
    assert!(matches!(
        map_io(persistent_io::Error::Allocation(
            AllocationError::CapacityExceeded
        )),
        Error::AllocationFailed
    ));
    assert!(matches!(
        map_io(persistent_io::Error::Storage(ObjectStoreError::Missing)),
        Error::Storage(ObjectStoreError::Missing)
    ));
    assert!(matches!(
        map_io(persistent_io::Error::Decode(
            CanonicalDecodeError::Truncated
        )),
        Error::Decode(CanonicalDecodeError::Truncated)
    ));
    assert!(matches!(
        map_io(persistent_io::Error::Work(WorkError::Overflow)),
        Error::Work(WorkError::Overflow)
    ));
}

#[test]
#[allow(clippy::too_many_lines)]
fn machine_admission_cycles_and_corrupt_cleanup_are_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let key = name(1)?;
    let keys = [key];
    let root = ObjectId {
        kind: ObjectKind::AttributePage,
        digest: Digest::from_bytes([1; 32]),
    };
    let wrong = ObjectId {
        kind: ObjectKind::Blob,
        digest: Digest::from_bytes([2; 32]),
    };
    assert!(matches!(
        Machine::<AttributeFormat>::new(
            wrong,
            &keys,
            1,
            DecodeLimits::default(),
            WorkBudget::UNBOUNDED,
        ),
        Err(OperationFailure {
            error: Error::WrongRootKind,
            ..
        })
    ));
    let invalid = DecodeLimits {
        maximum_page_height: 0,
        ..DecodeLimits::default()
    };
    assert!(matches!(
        Machine::<AttributeFormat>::new(root, &keys, 1, invalid, WorkBudget::UNBOUNDED),
        Err(OperationFailure {
            error: Error::InvalidLimits,
            ..
        })
    ));

    let mut cycle = Machine::<AttributeFormat>::new(
        root,
        &keys,
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    cycle.visit(root)?;
    assert!(matches!(
        cycle.visit(root),
        Err(OperationFailure {
            error: Error::CycleOrAlias,
            ..
        })
    ));

    let mut overflow = Machine::<AttributeFormat>::new(
        root,
        &keys,
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    overflow.requests[0].nested_bytes = u64::MAX;
    overflow.requests.push(Request {
        page: root,
        lower: None,
        upper: None,
        queries: 0..1,
        height: 1,
        nested_bytes: 1,
    });
    assert!(matches!(
        overflow.abort_cleanup(),
        Err(OperationFailure {
            error: Error::AllocationFailed,
            ..
        })
    ));

    let mut dirty_abort = Machine::<AttributeFormat>::new(
        root,
        &keys,
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    dirty_abort
        .allocations
        .claim_bytes(1, 0, &mut dirty_abort.work, WorkBudget::UNBOUNDED)?;
    assert!(matches!(
        dirty_abort.abort_cleanup(),
        Err(OperationFailure {
            error: Error::InvalidRouting,
            ..
        })
    ));

    let mut dirty_finish = Machine::<AttributeFormat>::new(
        root,
        &keys,
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    dirty_finish.requests.clear();
    dirty_finish
        .allocations
        .claim_bytes(1, 0, &mut dirty_finish.work, WorkBudget::UNBOUNDED)?;
    assert!(matches!(
        dirty_finish.finish(),
        Err(OperationFailure {
            error: Error::InvalidRouting,
            ..
        })
    ));
    Ok(())
}
