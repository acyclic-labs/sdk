use super::*;
use crate::foundation::Digest;
use crate::kernel::{
    AttributeChild, AttributeClass, AttributePage, attribute_page_id, encode_attribute_page,
};
use crate::memory::MemoryObjectStore;
use crate::storage::{ObjectKind, ObjectStore};
use crate::{CachedObjectStore, ObjectCacheOptions};
use bytes::Bytes;

fn name(value: &str) -> Result<AttributeName, Box<dyn std::error::Error>> {
    Ok(AttributeName::new(
        AttributeClass::PosixXattr,
        value.as_bytes().to_vec(),
        255,
    )?)
}

fn entry(value: &str, byte: u8) -> Result<AttributeEntry, Box<dyn std::error::Error>> {
    Ok(AttributeEntry {
        name: name(value)?,
        value_bytes: 1,
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

fn tree(store: &MemoryObjectStore) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let a = put(
        store,
        &AttributePage::Leaf(vec![entry("a", 1)?, entry("b", 2)?]),
    )?;
    let c = put(
        store,
        &AttributePage::Leaf(vec![entry("c", 3)?, entry("d", 4)?]),
    )?;
    let e = put(
        store,
        &AttributePage::Leaf(vec![entry("e", 5)?, entry("f", 6)?]),
    )?;
    put(
        store,
        &AttributePage::Internal(vec![
            AttributeChild {
                first_name: name("a")?,
                page: a,
            },
            AttributeChild {
                first_name: name("c")?,
                page: c,
            },
            AttributeChild {
                first_name: name("e")?,
                page: e,
            },
        ]),
    )
}

fn four_leaf_tree(
    store: &MemoryObjectStore,
) -> Result<(ObjectId, ObjectId), Box<dyn std::error::Error>> {
    let mut children = Vec::new();
    let mut third = None;
    for (index, value) in ["a", "b", "c", "d"].into_iter().enumerate() {
        let page = put(
            store,
            &AttributePage::Leaf(vec![entry(
                value,
                u8::try_from(index + 1).map_err(|_| "fixture index overflow")?,
            )?]),
        )?;
        if index == 2 {
            third = Some(page);
        }
        children.push(AttributeChild {
            first_name: name(value)?,
            page,
        });
    }
    Ok((
        put(store, &AttributePage::Internal(children))?,
        third.ok_or("missing third fixture leaf")?,
    ))
}

#[test]
fn bounded_attribute_listing_exposes_one_authenticated_metadata_successor()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, expected) = four_leaf_tree(&store)?;
    let limits = DecodeLimits::default();
    let page = list_attributes(&store, root, None, 1, limits, WorkBudget::UNBOUNDED)?;
    assert_eq!(
        page.entries
            .iter()
            .map(|entry| entry.name.as_bytes())
            .collect::<Vec<_>>(),
        [b"a".as_slice()]
    );
    assert!(page.has_more);
    assert_eq!(
        page.next_residency,
        Some(ResidencyHint {
            request: crate::ObjectReadRequest {
                object_id: expected,
                maximum_bytes: limits.maximum_page_object_bytes(),
            },
            reason: ResidencyReason::MetadataSuccessor,
        })
    );
    assert_eq!(page.work.page_reads, 3);
    Ok(())
}

#[test]
fn attributes_share_the_bounded_cursor_engine() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = tree(&store)?;
    let page = list_attributes(
        &store,
        root,
        Some(&name("c")?),
        2,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(
        page.entries
            .iter()
            .map(|entry| entry.name.as_bytes())
            .collect::<Vec<_>>(),
        vec![b"d".as_slice(), b"e".as_slice()]
    );
    assert!(page.has_more);
    assert_eq!(page.work.page_reads, 3);
    Ok(())
}

#[test]
fn cached_attribute_pagination_reuses_shared_internal_and_leaf_pages()
-> Result<(), Box<dyn std::error::Error>> {
    let backend = MemoryObjectStore::new(1024 * 1024)?;
    let root = tree(&backend)?;
    let store = CachedObjectStore::new(
        backend,
        ObjectCacheOptions {
            maximum_entries: 16,
            maximum_bytes: 1024 * 1024,
            maximum_in_flight: 4,
            maximum_waiters_per_object: 4,
        },
    )?;
    let cursor = name("b")?;
    let cancellation = CancellationToken::new();
    let cold = crate::async_storage::poll_ready(list_attributes_async(
        &store,
        root,
        Some(&cursor),
        4,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("cache-backed cold attribute pagination suspended")??;
    assert_eq!(
        cold.entries
            .iter()
            .map(|entry| entry.name.as_bytes())
            .collect::<Vec<_>>(),
        [
            b"c".as_slice(),
            b"d".as_slice(),
            b"e".as_slice(),
            b"f".as_slice()
        ]
    );
    assert!(!cold.has_more);
    assert_eq!(cold.work.backend_read_operations, 4);

    let warm = crate::async_storage::poll_ready(list_attributes_async(
        &store,
        root,
        Some(&cursor),
        4,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("cache-backed warm attribute pagination suspended")??;
    assert_eq!(warm.entries, cold.entries);
    assert_eq!(warm.work.backend_read_operations, 0);
    assert_eq!(warm.work.object_bytes_read, 0);
    assert_eq!(store.stats()?.decoded_hits, 4);
    Ok(())
}

#[test]
fn pre_cancel_and_output_admission_do_no_backend_work() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = tree(&store)?;
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failure = crate::async_storage::poll_ready(list_attributes_async(
        &store,
        root,
        None,
        2,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancelled,
    ))
    .ok_or("memory future remained pending")?
    .err()
    .ok_or("cancelled listing succeeded")?;
    assert!(matches!(failure.error, AttributeListError::Cancelled));
    assert_eq!(*failure.work, WorkCounters::default());

    let mut budget = WorkBudget::UNBOUNDED;
    budget.items_returned = 1;
    let failure = list_attributes(&store, root, None, 2, DecodeLimits::default(), budget)
        .err()
        .ok_or("unadmitted listing succeeded")?;
    assert!(matches!(
        failure.error,
        AttributeListError::Work(WorkError::BudgetExceeded {
            counter: "items_returned",
            ..
        })
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn forged_child_bounds_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let leaf = put(
        &store,
        &AttributePage::Leaf(vec![entry("c", 3)?, entry("d", 4)?]),
    )?;
    let root = put(
        &store,
        &AttributePage::Internal(vec![AttributeChild {
            first_name: name("a")?,
            page: leaf,
        }]),
    )?;
    let failure = list_attributes(
        &store,
        root,
        None,
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("forged child bound unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        AttributeListError::ChildBoundsMismatch
    ));
    assert_eq!(failure.work.page_reads, 2);
    Ok(())
}

#[test]
fn one_byte_short_peak_budget_rejects_the_same_plan() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = tree(&store)?;
    let cursor = name("c")?;
    let baseline = list_attributes(
        &store,
        root,
        Some(&cursor),
        2,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    let mut budget = WorkBudget::UNBOUNDED;
    budget.peak_allocation_bytes = baseline
        .work
        .peak_allocation_bytes
        .checked_sub(1)
        .ok_or("baseline peak was zero")?;
    let failure = list_attributes(
        &store,
        root,
        Some(&cursor),
        2,
        DecodeLimits::default(),
        budget,
    )
    .err()
    .ok_or("one-byte-short peak budget unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        AttributeListError::Work(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            ..
        })
    ));
    Ok(())
}

#[test]
fn cursors_before_inside_and_after_the_tree_are_exact_and_bounded()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = tree(&store)?;
    for (after, maximum, expected, has_more) in [
        (None, 8, vec!["a", "b", "c", "d", "e", "f"], false),
        (Some(name("0")?), 2, vec!["a", "b"], true),
        (Some(name("c")?), 2, vec!["d", "e"], true),
        (Some(name("z")?), 2, Vec::new(), false),
    ] {
        let page = list_attributes(
            &store,
            root,
            after.as_ref(),
            maximum,
            DecodeLimits::default(),
            WorkBudget::UNBOUNDED,
        )?;
        assert_eq!(
            page.entries
                .iter()
                .map(|entry| std::str::from_utf8(entry.name.as_bytes()))
                .collect::<Result<Vec<_>, _>>()?,
            expected
        );
        assert_eq!(page.has_more, has_more);
        assert_eq!(page.work.items_returned, u64::try_from(page.entries.len())?);
    }
    Ok(())
}

#[test]
fn pagination_admission_height_and_alias_guards_are_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = tree(&store)?;
    let wrong_root = ObjectId {
        kind: ObjectKind::Blob,
        digest: root.digest,
    };
    let failures = [
        list_attributes(
            &store,
            wrong_root,
            None,
            1,
            DecodeLimits::default(),
            WorkBudget::UNBOUNDED,
        )
        .err()
        .ok_or("wrong attribute root kind succeeded")?,
        list_attributes(
            &store,
            root,
            None,
            0,
            DecodeLimits::default(),
            WorkBudget::UNBOUNDED,
        )
        .err()
        .ok_or("zero attribute page limit succeeded")?,
        list_attributes(
            &store,
            root,
            None,
            1,
            DecodeLimits {
                maximum_page_height: 0,
                ..DecodeLimits::default()
            },
            WorkBudget::UNBOUNDED,
        )
        .err()
        .ok_or("invalid attribute decode limits succeeded")?,
    ];
    assert!(matches!(
        failures[0].error,
        AttributeListError::WrongRootKind
    ));
    assert!(matches!(failures[1].error, AttributeListError::ZeroLimit));
    assert!(matches!(
        failures[2].error,
        AttributeListError::InvalidLimits
    ));
    for failure in failures {
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let height = list_attributes(
        &store,
        root,
        None,
        1,
        DecodeLimits {
            maximum_page_height: 1,
            ..DecodeLimits::default()
        },
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("height-limited attribute traversal succeeded")?;
    assert!(matches!(height.error, AttributeListError::HeightExceeded));
    assert_eq!(height.work.page_reads, 1);

    let leaf = put(
        &store,
        &AttributePage::Leaf(vec![entry("a", 1)?, entry("b", 2)?]),
    )?;
    let alias_root = put(
        &store,
        &AttributePage::Internal(vec![
            AttributeChild {
                first_name: name("a")?,
                page: leaf,
            },
            AttributeChild {
                first_name: name("c")?,
                page: leaf,
            },
        ]),
    )?;
    let alias = list_attributes(
        &store,
        alias_root,
        None,
        4,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("aliased attribute page succeeded")?;
    assert!(matches!(alias.error, AttributeListError::CycleOrAlias));
    assert_eq!(alias.work.page_reads, 2);
    Ok(())
}

#[test]
fn pagination_error_translation_is_total() {
    use persistent_pagination::Error as PaginationError;

    assert!(matches!(
        map_error(PaginationError::Cancelled),
        AttributeListError::Cancelled
    ));
    assert!(matches!(
        map_error(PaginationError::ZeroLimit),
        AttributeListError::ZeroLimit
    ));
    assert!(matches!(
        map_error(PaginationError::LimitOverflow),
        AttributeListError::LimitOverflow
    ));
    assert!(matches!(
        map_error(PaginationError::WrongRootKind),
        AttributeListError::WrongRootKind
    ));
    assert!(matches!(
        map_error(PaginationError::InvalidLimits),
        AttributeListError::InvalidLimits
    ));
    assert!(matches!(
        map_error(PaginationError::HeightExceeded),
        AttributeListError::HeightExceeded
    ));
    assert!(matches!(
        map_error(PaginationError::CycleOrAlias),
        AttributeListError::CycleOrAlias
    ));
    assert!(matches!(
        map_error(PaginationError::ChildBoundsMismatch),
        AttributeListError::ChildBoundsMismatch
    ));
    assert!(matches!(
        map_error(PaginationError::TraversalState),
        AttributeListError::TraversalState
    ));
    assert!(matches!(
        map_error(PaginationError::AllocationFailed),
        AttributeListError::AllocationFailed
    ));
    assert!(matches!(
        map_error(PaginationError::Storage(ObjectStoreError::Missing)),
        AttributeListError::Storage(ObjectStoreError::Missing)
    ));
    assert!(matches!(
        map_error(PaginationError::Decode(
            CanonicalDecodeError::LengthOverflow
        )),
        AttributeListError::Decode(CanonicalDecodeError::LengthOverflow)
    ));
    assert!(matches!(
        map_error(PaginationError::Work(WorkError::Overflow)),
        AttributeListError::Work(WorkError::Overflow)
    ));
}
