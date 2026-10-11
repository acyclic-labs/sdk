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
    assert_eq!(equal_group(&indexed, 0, indexed.len(), |_| Ok::<_, crate::FsError>(()))?, (2, 2));
    assert_eq!(equal_group(&indexed, 2, indexed.len(), |_| Ok::<_, crate::FsError>(()))?, (3, 1));
    assert_eq!(equal_group(&indexed, 3, indexed.len(), |_| Ok::<_, crate::FsError>(()))?, (4, 0));

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
    assert!(leaf_within::<AttributeFormat>(&values, None, None));
    assert!(leaf_within::<AttributeFormat>(
        &values,
        Some(&name(1)?),
        Some(&name(4)?)
    ));
    assert!(!leaf_within::<AttributeFormat>(
        &values,
        Some(&name(2)?),
        None
    ));
    assert!(!leaf_within::<AttributeFormat>(
        &values,
        None,
        Some(&name(3)?)
    ));
    assert!(leaf_within::<AttributeFormat>(&[], None, Some(&name(1)?)));

    let children = [child(1)?, child(3)?];
    assert!(children_within(&children, None, None));
    assert!(children_within(&children, Some(&name(1)?), Some(&name(4)?)));
    assert!(!children_within(&children, Some(&name(2)?), None));
    assert!(!children_within(&children, None, Some(&name(3)?)));
    assert!(children_within(&[], None, Some(&name(1)?)));
    Ok(())
}

#[test]
fn allocation_and_page_io_error_translation_is_total() {
    assert!(matches!(
        project_failure(crate::FsError::PendingMutationAllocationFailed),
        Error::AllocationFailed
    ));

    assert!(matches!(
        project_failure(crate::FsError::Decode(CanonicalDecodeError::AllocationFailed)),
        Error::AllocationFailed
    ));
    assert!(matches!(
        project_failure(query_error(Error::AllocationFailed)),
        Error::AllocationFailed
    ));
    assert!(matches!(
        project_failure(crate::FsError::Object(ObjectStoreError::Missing)),
        Error::Storage(ObjectStoreError::Missing)
    ));
    assert!(matches!(
        project_failure(crate::FsError::Decode(CanonicalDecodeError::Truncated)),
        Error::Decode(CanonicalDecodeError::Truncated)
    ));
    assert!(matches!(
        project_failure(crate::FsError::Work(WorkError::Overflow)),
        Error::Work(WorkError::Overflow)
    ));
}

#[test]
fn machine_admission_cycles_and_corrupt_cleanup_are_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let keys = [name(1)?];
    let root = ObjectId {
        kind: ObjectKind::AttributePage,
        digest: Digest::from_bytes([1; 32]),
    };
    let wrong = ObjectId { kind: ObjectKind::Blob, digest: Digest::from_bytes([2; 32]) };
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    assert!(matches!(
        Machine::<AttributeFormat>::new(wrong, &keys, 1, DecodeLimits::default(), &mut accounting),
        Err(crate::FsError::FileRecord(crate::kernel::FileRecordReadError::WrongRootKind))
    ));
    let invalid = DecodeLimits { maximum_page_height: 0, ..DecodeLimits::default() };
    assert!(matches!(
        Machine::<AttributeFormat>::new(root, &keys, 1, invalid, &mut accounting),
        Err(crate::FsError::FileRecord(crate::kernel::FileRecordReadError::InvalidHeightLimit))
    ));
    let mut cycle = Machine::<AttributeFormat>::new(root, &keys, 1, DecodeLimits::default(), &mut accounting)?;
    cycle.visit(root, &mut accounting)?;
    assert!(matches!(
        cycle.visit(root, &mut accounting),
        Err(crate::FsError::FileRecord(crate::kernel::FileRecordReadError::Cycle))
    ));
    cycle.abort_cleanup(&mut accounting)?;
    assert_eq!(accounting.standalone_parts().ok_or(WorkError::Overflow)?.0.live_bytes(), 0);

    let mut corrupt = Machine::<AttributeFormat>::new(root, &keys, 1, DecodeLimits::default(), &mut accounting)?;
    corrupt.requests.value.get_mut(0).ok_or(Error::InvalidRouting)?.request.nested_bytes = u64::MAX;
    assert!(matches!(
        corrupt.abort_cleanup(&mut accounting),
        Err(crate::FsError::FileRecord(crate::kernel::FileRecordReadError::InvalidRouting))
    ));
    Ok(())
}

#[test]
fn machine_cleanup_preserves_the_callers_existing_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let keys = [name(1)?];
    let root = ObjectId { kind: ObjectKind::AttributePage, digest: Digest::from_bytes([1; 32]) };
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let existing = Owned::<Vec<u8>>::allocate(1, &mut accounting)?;
    let machine = Machine::<AttributeFormat>::new(root, &keys, 1, DecodeLimits::default(), &mut accounting)?;
    machine.abort_cleanup(&mut accounting)?;
    assert_eq!(accounting.standalone_parts().ok_or(WorkError::Overflow)?.0.live_bytes(), 1);
    let machine = Machine::<AttributeFormat>::new(root, &keys, 1, DecodeLimits::default(), &mut accounting)?;
    let (receipt, allocation) = machine.finish(&mut accounting)?;
    let bytes = usize_to_u64(receipt.values.capacity() * std::mem::size_of::<Option<crate::kernel::AttributeEntry>>());
    Owned::new(receipt.values, allocation, bytes).retire(&mut accounting)?;
    assert_eq!(accounting.standalone_parts().ok_or(WorkError::Overflow)?.0.live_bytes(), 1);
    existing.retire(&mut accounting)?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_batch_denial_keeps_its_actual_constructor_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let keys = [name(1)?, name(2)?];
    let root = ObjectId { kind: ObjectKind::AttributePage, digest: Digest::from_bytes([1; 32]) };
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        WorkBudget { allocation_operations: 2, ..WorkBudget::UNBOUNDED }, &cancellation,
    ).map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    assert!(Machine::<AttributeFormat>::new(root, &keys, 2, DecodeLimits::default(), &mut accounting).is_err());
    assert_eq!(original.receipt().allocation_operations, 2);
    assert_eq!(original.receipt().items_examined, 1);
    assert_eq!(original.receipt().backend_read_operations, 0);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}
