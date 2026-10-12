use super::*;
use crate::foundation::Digest;
use crate::kernel::attribute_mutation::AttributeFormat;
use crate::kernel::{AttributeClass, AttributeEntry, AttributeName};
use crate::performance::WorkError;
use crate::storage::ObjectKind;

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
