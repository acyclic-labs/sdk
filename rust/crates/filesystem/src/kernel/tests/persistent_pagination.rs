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

fn root(kind: ObjectKind) -> ObjectId {
    ObjectId {
        kind,
        digest: Digest::from_bytes([9; 32]),
    }
}

#[test]
fn machine_admission_frontier_height_and_alias_guards_are_exact()
-> Result<(), Box<dyn std::error::Error>> {
    let limits = DecodeLimits::default();
    let wrong = Machine::<AttributeFormat>::new(
        root(ObjectKind::TreePage),
        None,
        1,
        limits,
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("wrong pagination root kind was admitted")?;
    assert!(matches!(wrong.error, Error::WrongRootKind));

    let zero = Machine::<AttributeFormat>::new(
        root(ObjectKind::AttributePage),
        None,
        0,
        limits,
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("zero pagination limit was admitted")?;
    assert!(matches!(zero.error, Error::ZeroLimit));

    let mut invalid_limits = limits;
    invalid_limits.maximum_page_height = 0;
    let invalid = Machine::<AttributeFormat>::new(
        root(ObjectKind::AttributePage),
        None,
        1,
        invalid_limits,
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("invalid pagination limits were admitted")?;
    assert!(matches!(invalid.error, Error::InvalidLimits));

    let budget = Machine::<AttributeFormat>::new(
        root(ObjectKind::AttributePage),
        None,
        1,
        limits,
        WorkBudget::default(),
    )
    .err()
    .ok_or("zero work budget admitted pagination output")?;
    assert!(matches!(
        budget.error,
        Error::Work(WorkError::BudgetExceeded { .. })
    ));

    let mut empty = Machine::<AttributeFormat>::new(
        root(ObjectKind::AttributePage),
        None,
        1,
        limits,
        WorkBudget::UNBOUNDED,
    )?;
    let mut pending = empty.pop_pending()?;
    let absent = empty
        .pop_pending()
        .err()
        .ok_or("empty pagination frontier produced a request")?;
    assert!(matches!(absent.error, Error::TraversalState));
    pending.height = limits.maximum_page_height.saturating_add(1);
    empty.push_pending(pending)?;
    let excessive = empty
        .pop_pending()
        .err()
        .ok_or("overheight pagination frontier was accepted")?;
    assert!(matches!(excessive.error, Error::HeightExceeded));

    let mut alias = Machine::<AttributeFormat>::new(
        root(ObjectKind::AttributePage),
        None,
        1,
        limits,
        WorkBudget::UNBOUNDED,
    )?;
    let page = root(ObjectKind::AttributePage);
    alias.visit(page)?;
    let repeated = alias
        .visit(page)
        .err()
        .ok_or("repeated pagination page was accepted")?;
    assert!(matches!(repeated.error, Error::CycleOrAlias));
    Ok(())
}

#[test]
fn cursor_search_and_page_bounds_cover_every_edge() -> Result<(), Box<dyn std::error::Error>> {
    let values = [entry(2)?, entry(4)?, entry(6)?];
    assert_eq!(
        upper_bound_values::<AttributeFormat>(&values, &name(1)?).0,
        0
    );
    assert_eq!(
        upper_bound_values::<AttributeFormat>(&values, &name(2)?).0,
        1
    );
    assert_eq!(
        upper_bound_values::<AttributeFormat>(&values, &name(5)?).0,
        2
    );
    assert_eq!(
        upper_bound_values::<AttributeFormat>(&values, &name(9)?).0,
        3
    );
    assert!(validate_values::<AttributeFormat>(&values, None, None).is_ok());
    assert!(validate_values::<AttributeFormat>(&values, Some(&name(2)?), Some(&name(7)?)).is_ok());
    assert!(matches!(
        validate_values::<AttributeFormat>(&values, Some(&name(3)?), None),
        Err(Error::ChildBoundsMismatch)
    ));
    assert!(matches!(
        validate_values::<AttributeFormat>(&values, None, Some(&name(6)?)),
        Err(Error::ChildBoundsMismatch)
    ));

    let children = [child(2)?, child(4)?, child(6)?];
    assert_eq!(upper_bound_children(&children, &name(1)?).0, 0);
    assert_eq!(upper_bound_children(&children, &name(2)?).0, 0);
    assert_eq!(upper_bound_children(&children, &name(5)?).0, 1);
    assert_eq!(upper_bound_children(&children, &name(9)?).0, 2);
    assert!(validate_children::<AttributeFormat>(&children, None, None).is_ok());
    assert!(
        validate_children::<AttributeFormat>(&children, Some(&name(2)?), Some(&name(7)?)).is_ok()
    );
    assert!(matches!(
        validate_children::<AttributeFormat>(&children, Some(&name(3)?), None),
        Err(Error::ChildBoundsMismatch)
    ));
    assert!(matches!(
        validate_children::<AttributeFormat>(&children, None, Some(&name(6)?)),
        Err(Error::ChildBoundsMismatch)
    ));
    Ok(())
}

#[test]
fn bounded_leaf_and_internal_frontiers_stop_exactly_and_reject_corrupt_accounting()
-> Result<(), Box<dyn std::error::Error>> {
    let values = [entry(2)?, entry(4)?, entry(6)?];
    let mut leaf = Machine::<AttributeFormat>::new(
        root(ObjectKind::AttributePage),
        None,
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    let pending = leaf.pop_pending()?;
    leaf.accept_shared_leaf(&values, &pending)?;
    assert_eq!(leaf.values, values[..2]);

    let children = [child(2)?, child(4)?, child(6)?, child(8)?];
    let mut internal = Machine::<AttributeFormat>::new(
        root(ObjectKind::AttributePage),
        None,
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    let mut pending = internal.pop_pending()?;
    internal.accept_shared_internal(&children, &mut pending)?;
    assert_eq!(internal.successor_page, Some(children[3].page));

    let shallow_limits = DecodeLimits {
        maximum_page_height: 1,
        ..DecodeLimits::default()
    };
    let mut shallow = Machine::<AttributeFormat>::new(
        root(ObjectKind::AttributePage),
        None,
        1,
        shallow_limits,
        WorkBudget::UNBOUNDED,
    )?;
    let mut pending = shallow.pop_pending()?;
    assert!(matches!(
        shallow.accept_shared_internal(&children, &mut pending),
        Err(OperationFailure {
            error: Error::HeightExceeded,
            ..
        })
    ));

    let corrupt_pending = || -> Result<Pending<AttributeName>, crate::kernel::AttributeError> {
        Ok(Pending {
            page: root(ObjectKind::AttributePage),
            lower: Some(name(2)?),
            upper: Some(name(9)?),
            height: 1,
            nested_bytes: 0,
        })
    };
    let bounded_children = [child(2)?, child(4)?];
    let mut shared = Machine::<AttributeFormat>::new(
        root(ObjectKind::AttributePage),
        None,
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    let mut pending = corrupt_pending()?;
    assert!(matches!(
        shared.accept_shared_internal(&bounded_children, &mut pending),
        Err(OperationFailure {
            error: Error::TraversalState,
            ..
        })
    ));

    let mut owned = Machine::<AttributeFormat>::new(
        root(ObjectKind::AttributePage),
        None,
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    let mut pending = corrupt_pending()?;
    assert!(matches!(
        owned.accept_internal(bounded_children.to_vec(), 2, &mut pending),
        Err(OperationFailure {
            error: Error::TraversalState,
            ..
        })
    ));
    Ok(())
}

#[test]
fn item_charging_and_lower_layer_error_translation_are_total()
-> Result<(), Box<dyn std::error::Error>> {
    let mut work = WorkCounters::default();
    charge_items(&mut work, 2, WorkBudget::UNBOUNDED)?;
    assert_eq!(work.items_examined, 2);
    let exact_budget = work;
    let rejected = charge_items(&mut work, 1, exact_budget)
        .err()
        .ok_or("over-budget item charge succeeded")?;
    assert!(matches!(
        rejected.error,
        Error::Work(WorkError::BudgetExceeded { .. })
    ));

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
    Ok(())
}
