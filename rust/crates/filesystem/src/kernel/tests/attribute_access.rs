use super::*;
use crate::foundation::Digest;
use crate::kernel::{AttributeClass, attribute_page_id, encode_attribute_page};
use crate::memory::MemoryObjectStore;
use crate::storage::{ObjectStore, object_digest};
use crate::test_support::OwnedReadObjectStore;
use bytes::Bytes;

fn name(byte: u8) -> Result<AttributeName, Box<dyn std::error::Error>> {
    Ok(AttributeName::new(
        AttributeClass::PosixXattr,
        vec![byte],
        8,
    )?)
}

fn entry(byte: u8) -> Result<AttributeEntry, Box<dyn std::error::Error>> {
    Ok(AttributeEntry {
        name: name(byte)?,
        value_bytes: u64::from(byte),
        value: ObjectId {
            kind: ObjectKind::Blob,
            digest: Digest::from_bytes([byte; 32]),
        },
    })
}

fn put(
    store: &MemoryObjectStore,
    page: &AttributePage,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let id = attribute_page_id(page, 8)?;
    ObjectStore::put(
        store,
        id,
        Bytes::from(encode_attribute_page(page, 8)?),
        WorkBudget::UNBOUNDED,
    )?;
    Ok(id)
}

fn two_level_tree(store: &MemoryObjectStore) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let first = put(store, &AttributePage::Leaf(vec![entry(1)?]))?;
    let second = put(store, &AttributePage::Leaf(vec![entry(3)?, entry(4)?]))?;
    put(
        store,
        &AttributePage::Internal(vec![
            AttributeChild {
                first_name: name(1)?,
                page: first,
            },
            AttributeChild {
                first_name: name(3)?,
                page: second,
            },
        ]),
    )
}

#[test]
fn lookup_reads_one_frontier_and_authenticates_absence() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = two_level_tree(&store)?;
    let found = lookup_attribute(
        &store,
        root,
        &name(3)?,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(found.entry, Some(entry(3)?));
    assert_eq!(found.work.page_reads, 2);
    let absent = lookup_attribute(
        &store,
        root,
        &name(5)?,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(absent.entry, None);
    assert_eq!(absent.work.page_reads, 2);

    let names = vec![name(4)?, name(1)?, name(4)?, name(5)?];
    let batch = lookup_attributes(
        &store,
        root,
        &names,
        4,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(
        batch.entries,
        vec![Some(entry(4)?), Some(entry(1)?), Some(entry(4)?), None]
    );
    assert_eq!(batch.work.page_reads, 3);
    Ok(())
}

#[test]
fn owned_attribute_pages_are_admitted_and_released_across_the_frontier()
-> Result<(), Box<dyn std::error::Error>> {
    let store = OwnedReadObjectStore::default();
    let root = two_level_tree(&store.inner)?;
    let lookup = crate::async_storage::poll_ready(lookup_attribute_async(
        &store,
        root,
        &name(4)?,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("owned attribute lookup blocked")??;
    assert_eq!(lookup.entry, Some(entry(4)?));
    assert_eq!(lookup.work.page_reads, 2);
    assert!(lookup.work.bytes_copied > 0);
    assert!(lookup.work.peak_allocation_bytes > 0);
    Ok(())
}

#[test]
fn sync_and_async_lookup_are_physically_identical() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = two_level_tree(&store)?;
    let requested = name(4)?;
    let synchronous = lookup_attribute(
        &store,
        root,
        &requested,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    let asynchronous = crate::async_storage::poll_ready(lookup_attribute_async(
        &store,
        root,
        &requested,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("memory-backed async attribute lookup unexpectedly blocked")??;
    assert_eq!(asynchronous, synchronous);
    Ok(())
}

#[test]
fn async_batch_is_identical_and_precancellation_is_inert() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let root = two_level_tree(&store)?;
    let names = vec![name(4)?, name(1)?, name(4)?, name(5)?];
    let synchronous = lookup_attributes(
        &store,
        root,
        &names,
        4,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    let asynchronous = crate::async_storage::poll_ready(lookup_attributes_async(
        &store,
        root,
        &names,
        4,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("memory-backed async attribute batch blocked")??;
    assert_eq!(asynchronous, synchronous);

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let failure = crate::async_storage::poll_ready(lookup_attributes_async(
        &store,
        root,
        &names,
        4,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("pre-cancelled async attribute batch blocked")?
    .err()
    .ok_or("pre-cancelled async attribute batch succeeded")?;
    assert!(matches!(failure.error, AttributeLookupError::Cancelled));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn missing_malformed_and_oversized_pages_preserve_exact_storage_boundaries()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let missing = ObjectId {
        kind: ObjectKind::AttributePage,
        digest: Digest::from_bytes([88; 32]),
    };
    let failure = lookup_attribute(
        &store,
        missing,
        &name(1)?,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("missing attribute page resolved")?;
    assert!(matches!(
        failure.error,
        AttributeLookupError::Storage(ObjectStoreError::Missing)
    ));
    assert_eq!(failure.work.page_reads, 1);

    let malformed = Bytes::from_static(b"not a canonical attribute page");
    let malformed_id = ObjectId {
        kind: ObjectKind::AttributePage,
        digest: object_digest(ObjectKind::AttributePage, &malformed),
    };
    ObjectStore::put(&store, malformed_id, malformed, WorkBudget::UNBOUNDED)?;
    let failure = lookup_attribute(
        &store,
        malformed_id,
        &name(1)?,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("malformed attribute page decoded")?;
    assert!(matches!(failure.error, AttributeLookupError::Decode(_)));
    assert_eq!(failure.work.page_reads, 1);
    assert_eq!(failure.work.backend_read_operations, 1);

    let valid = put(&store, &AttributePage::Leaf(vec![entry(1)?]))?;
    let limits = DecodeLimits {
        maximum_object_bytes: 1,
        maximum_page_bytes: 1,
        ..DecodeLimits::default()
    };
    let failure = lookup_attribute(&store, valid, &name(1)?, limits, WorkBudget::UNBOUNDED)
        .err()
        .ok_or("oversized attribute page read succeeded")?;
    assert!(matches!(
        failure.error,
        AttributeLookupError::Storage(ObjectStoreError::TooLarge { .. })
    ));
    assert_eq!(failure.work.page_reads, 1);
    Ok(())
}

#[test]
fn decoded_attribute_page_is_admitted_before_allocation() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let root = two_level_tree(&store)?;
    let baseline = lookup_attribute(
        &store,
        root,
        &name(4)?,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    let mut budget = WorkBudget::UNBOUNDED;
    budget.peak_allocation_bytes = baseline
        .work
        .peak_allocation_bytes
        .checked_sub(1)
        .ok_or("baseline peak is unexpectedly zero")?;
    let failure = lookup_attribute(&store, root, &name(4)?, DecodeLimits::default(), budget)
        .err()
        .ok_or("one-byte-short attribute allocation unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        AttributeLookupError::Work(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            ..
        })
    ));
    assert_eq!(failure.work.backend_write_operations, 0);
    Ok(())
}

#[test]
fn cancellation_and_forged_bounds_fail_before_unsafe_progress()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let leaf = put(&store, &AttributePage::Leaf(vec![entry(1)?]))?;
    let forged = put(
        &store,
        &AttributePage::Internal(vec![AttributeChild {
            first_name: name(2)?,
            page: leaf,
        }]),
    )?;
    let failure = lookup_attribute(
        &store,
        forged,
        &name(2)?,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("forged lower bound unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        AttributeLookupError::ChildBoundsMismatch
    ));
    assert_eq!(failure.work.page_reads, 2);

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let cancelled = crate::async_storage::poll_ready(lookup_attribute_async(
        &store,
        forged,
        &name(2)?,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("memory-backed async attribute lookup unexpectedly blocked")?
    .err()
    .ok_or("cancelled attribute lookup unexpectedly succeeded")?;
    assert!(matches!(cancelled.error, AttributeLookupError::Cancelled));
    assert_eq!(*cancelled.work, WorkCounters::default());
    Ok(())
}

#[test]
fn validation_and_boundary_queries_are_fail_closed_and_exact()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = two_level_tree(&store)?;
    let wrong_root = ObjectId {
        kind: ObjectKind::Blob,
        digest: Digest::from_bytes([77; 32]),
    };
    let wrong = lookup_attribute(
        &store,
        wrong_root,
        &name(1)?,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("wrong attribute root kind unexpectedly succeeded")?;
    assert!(matches!(wrong.error, AttributeLookupError::WrongRootKind));
    assert_eq!(*wrong.work, WorkCounters::default());

    let invalid_limits = DecodeLimits {
        maximum_page_height: 0,
        ..DecodeLimits::default()
    };
    let invalid = lookup_attribute(
        &store,
        root,
        &name(1)?,
        invalid_limits,
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("invalid attribute limits unexpectedly succeeded")?;
    assert!(matches!(invalid.error, AttributeLookupError::InvalidLimits));
    assert_eq!(*invalid.work, WorkCounters::default());

    let empty = lookup_attributes(
        &store,
        root,
        &[],
        4,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("empty attribute batch unexpectedly succeeded")?;
    assert!(matches!(empty.error, AttributeLookupError::EmptyBatch));
    assert_eq!(*empty.work, WorkCounters::default());
    let excessive = lookup_attributes(
        &store,
        root,
        &[name(1)?, name(3)?],
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("excessive attribute batch unexpectedly succeeded")?;
    assert!(matches!(
        excessive.error,
        AttributeLookupError::TooManyQueries
    ));
    assert_eq!(*excessive.work, WorkCounters::default());

    for (requested, expected) in [
        (1, Some(entry(1)?)),
        (2, None),
        (4, Some(entry(4)?)),
        (u8::MAX, None),
    ] {
        let lookup = lookup_attribute(
            &store,
            root,
            &name(requested)?,
            DecodeLimits::default(),
            WorkBudget::UNBOUNDED,
        )?;
        assert_eq!(lookup.entry, expected);
        assert_eq!(lookup.work.page_reads, 2);
    }

    let height_one = DecodeLimits {
        maximum_page_height: 1,
        ..DecodeLimits::default()
    };
    let height = lookup_attribute(&store, root, &name(1)?, height_one, WorkBudget::UNBOUNDED)
        .err()
        .ok_or("attribute height limit unexpectedly succeeded")?;
    assert!(matches!(height.error, AttributeLookupError::HeightExceeded));
    assert_eq!(height.work.page_reads, 1);
    Ok(())
}

#[test]
fn inherited_attribute_upper_bounds_reject_forged_leaf_contents()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let first = put(&store, &AttributePage::Leaf(vec![entry(1)?]))?;
    let forged_middle = put(
        &store,
        &AttributePage::Leaf(vec![entry(3)?, entry(4)?, entry(5)?]),
    )?;
    let last = put(&store, &AttributePage::Leaf(vec![entry(5)?]))?;
    let root = put(
        &store,
        &AttributePage::Internal(vec![
            AttributeChild {
                first_name: name(1)?,
                page: first,
            },
            AttributeChild {
                first_name: name(3)?,
                page: forged_middle,
            },
            AttributeChild {
                first_name: name(5)?,
                page: last,
            },
        ]),
    )?;
    let failure = lookup_attribute(
        &store,
        root,
        &name(4)?,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("forged attribute upper bound unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        AttributeLookupError::ChildBoundsMismatch
    ));
    assert_eq!(failure.work.page_reads, 2);
    Ok(())
}

#[test]
fn batch_allocation_storage_and_routing_error_maps_are_total()
-> Result<(), Box<dyn std::error::Error>> {
    let mapped = [
        map_batch_error(persistent_batch::Error::Cancelled),
        map_batch_error(persistent_batch::Error::Empty),
        map_batch_error(persistent_batch::Error::TooManyQueries),
        map_batch_error(persistent_batch::Error::WrongRootKind),
        map_batch_error(persistent_batch::Error::InvalidLimits),
        map_batch_error(persistent_batch::Error::HeightExceeded),
        map_batch_error(persistent_batch::Error::CycleOrAlias),
        map_batch_error(persistent_batch::Error::ChildBoundsMismatch),
        map_batch_error(persistent_batch::Error::InvalidRouting),
        map_batch_error(persistent_batch::Error::AllocationFailed),
        map_batch_error(persistent_batch::Error::Storage(
            ObjectStoreError::Cancelled,
        )),
        map_batch_error(persistent_batch::Error::Storage(ObjectStoreError::Corrupt)),
        map_batch_error(persistent_batch::Error::Decode(
            CanonicalDecodeError::TrailingBytes,
        )),
        map_batch_error(persistent_batch::Error::Work(WorkError::Overflow)),
    ];
    assert!(matches!(mapped[0], AttributeLookupError::Cancelled));
    assert!(matches!(mapped[1], AttributeLookupError::EmptyBatch));
    assert!(matches!(mapped[2], AttributeLookupError::TooManyQueries));
    assert!(matches!(mapped[3], AttributeLookupError::WrongRootKind));
    assert!(matches!(mapped[4], AttributeLookupError::InvalidLimits));
    assert!(matches!(mapped[5], AttributeLookupError::HeightExceeded));
    assert!(matches!(mapped[6], AttributeLookupError::CycleOrAlias));
    assert!(matches!(
        mapped[7],
        AttributeLookupError::ChildBoundsMismatch
    ));
    assert!(matches!(mapped[8], AttributeLookupError::InvalidRouting));
    assert!(matches!(mapped[9], AttributeLookupError::AllocationFailed));
    assert!(matches!(mapped[10], AttributeLookupError::Cancelled));
    assert!(matches!(mapped[11], AttributeLookupError::Storage(_)));
    assert!(matches!(mapped[12], AttributeLookupError::Decode(_)));
    assert!(matches!(mapped[13], AttributeLookupError::Work(_)));

    for allocation in [
        AllocationError::Overflow,
        AllocationError::ReleaseInvariant,
        AllocationError::InvalidCapacity,
        AllocationError::CapacityExceeded,
        AllocationError::AllocationFailed,
    ] {
        assert!(matches!(
            map_allocation(allocation),
            AttributeLookupError::AllocationFailed
        ));
    }
    assert!(matches!(
        map_allocation(AllocationError::Work(WorkError::Overflow)),
        AttributeLookupError::Work(WorkError::Overflow)
    ));
    assert!(matches!(
        map_storage(ObjectStoreError::Cancelled),
        AttributeLookupError::Cancelled
    ));

    let child = AttributeChild {
        first_name: name(3)?,
        page: ObjectId {
            kind: ObjectKind::AttributePage,
            digest: Digest::from_bytes([91; 32]),
        },
    };
    assert!(matches!(
        validate_children(std::slice::from_ref(&child), Some(&name(2)?), None),
        Err(AttributeLookupError::ChildBoundsMismatch)
    ));
    assert!(validate_children(std::slice::from_ref(&child), None, Some(&name(4)?)).is_ok());
    assert!(matches!(
        validate_children(std::slice::from_ref(&child), None, Some(&name(2)?)),
        Err(AttributeLookupError::ChildBoundsMismatch)
    ));
    Ok(())
}
