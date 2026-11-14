use super::*;
use crate::foundation::Digest;
use crate::kernel::{AttributeClass, AttributePage, DirectoryReadError};
use crate::memory::MemoryObjectStore;
use crate::storage::ObjectKind;
use crate::{CachedObjectStore, ObjectCacheOptions};

use crate::kernel::Child;
fn name(value: &str) -> Result<AttributeName, Box<dyn std::error::Error>> {
    Ok(AttributeName::new(
        AttributeClass::PosixXattr,
        value.as_bytes().to_vec(),
    )?)
}

fn entry(value: &str, byte: u8) -> Result<AttributeEntry, Box<dyn std::error::Error>> {
    Ok(AttributeEntry {
        name: name(value)?,
        value_bytes: 1,
        value: ObjectId {
            storage: None,
            kind: ObjectKind::Blob,
            digest: Digest::from_bytes([byte; 32]),
        },
    })
}

use crate::test_support::put_attribute_page as put;

async fn tree(store: &MemoryObjectStore) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let a = put(
        store,
        &AttributePage::Leaf(vec![entry("a", 1)?, entry("b", 2)?]),
        8,
    )
    .await?;
    let c = put(
        store,
        &AttributePage::Leaf(vec![entry("c", 3)?, entry("d", 4)?]),
        8,
    )
    .await?;
    let e = put(
        store,
        &AttributePage::Leaf(vec![entry("e", 5)?, entry("f", 6)?]),
        8,
    )
    .await?;
    put(
        store,
        &AttributePage::Internal(vec![
            Child {
                first: name("a")?,
                page: a,
            },
            Child {
                first: name("c")?,
                page: c,
            },
            Child {
                first: name("e")?,
                page: e,
            },
        ]),
        8,
    )
    .await
}

async fn four_leaf_tree(
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
            8,
        )
        .await?;
        if index == 2 {
            third = Some(page);
        }
        children.push(Child {
            first: name(value)?,
            page,
        });
    }
    Ok((
        put(store, &AttributePage::Internal(children), 8).await?,
        third.ok_or("missing third fixture leaf")?,
    ))
}

#[tokio::test]
async fn bounded_attribute_listing_exposes_one_authenticated_metadata_successor()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, expected) = four_leaf_tree(&store).await?;
    let limits = DecodeLimits::default();
    let page = list_attributes(
        &store,
        root,
        None,
        1,
        limits,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
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
                maximum_bytes: limits.maximum_object_bytes,
            },
            reason: ResidencyReason::MetadataSuccessor,
        })
    );
    assert_eq!(page.work.page_reads, 3);
    Ok(())
}

#[tokio::test]
async fn attributes_share_the_bounded_cursor_engine() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = tree(&store).await?;
    let page = list_attributes(
        &store,
        root,
        Some(&name("c")?),
        2,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
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

#[tokio::test]
async fn cached_attribute_pagination_reuses_shared_internal_and_leaf_pages()
-> Result<(), Box<dyn std::error::Error>> {
    let backend = MemoryObjectStore::new(1024 * 1024)?;
    let root = tree(&backend).await?;
    let store = CachedObjectStore::new(
        backend,
        ObjectCacheOptions {
            maximum_entries: 16,
            maximum_bytes: 1024 * 1024,
        },
    )?;
    let cursor = name("b")?;
    let cancellation = CancellationToken::new();
    let cold = list_attributes(
        &store,
        root,
        Some(&cursor),
        4,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
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

    let warm = list_attributes(
        &store,
        root,
        Some(&cursor),
        4,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    assert_eq!(warm.entries, cold.entries);
    assert_eq!(warm.work.backend_read_operations, 0);
    assert_eq!(warm.work.object_bytes_read, 0);
    assert_eq!(store.stats()?.decoded_hits, 4);
    Ok(())
}

#[tokio::test]
async fn pre_cancel_and_output_admission_do_no_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = tree(&store).await?;
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failure = list_attributes(
        &store,
        root,
        None,
        2,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancelled,
    )
    .await
    .err()
    .ok_or("cancelled listing succeeded")?;
    assert!(matches!(failure.error, DirectoryReadError::Cancelled));
    assert_eq!(*failure.work, WorkCounters::default());

    let mut budget = WorkBudget::UNBOUNDED;
    budget.items_returned = 1;
    let failure = list_attributes(
        &store,
        root,
        None,
        2,
        DecodeLimits::default(),
        budget,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("unadmitted listing succeeded")?;
    assert!(matches!(
        failure.error,
        DirectoryReadError::Work(crate::performance::WorkError::BudgetExceeded {
            counter: "items_returned",
            ..
        })
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn forged_child_bounds_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let leaf = put(
        &store,
        &AttributePage::Leaf(vec![entry("c", 3)?, entry("d", 4)?]),
        8,
    )
    .await?;
    let root = put(
        &store,
        &AttributePage::Internal(vec![Child {
            first: name("a")?,
            page: leaf,
        }]),
        8,
    )
    .await?;
    let failure = list_attributes(
        &store,
        root,
        None,
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("forged child bound unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        DirectoryReadError::ChildBoundsMismatch
    ));
    assert_eq!(failure.work.page_reads, 2);
    Ok(())
}

#[tokio::test]
async fn one_byte_short_peak_budget_rejects_the_same_plan() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let root = tree(&store).await?;
    let cursor = name("c")?;
    let baseline = list_attributes(
        &store,
        root,
        Some(&cursor),
        2,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
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
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("one-byte-short peak budget unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        DirectoryReadError::Work(crate::performance::WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn cursors_before_inside_and_after_the_tree_are_exact_and_bounded()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = tree(&store).await?;
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
            &CancellationToken::new(),
        )
        .await?;
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

#[tokio::test]
async fn pagination_admission_height_and_alias_guards_are_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = tree(&store).await?;
    let wrong_root = ObjectId {
        storage: None,
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
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("wrong attribute root kind succeeded")?,
        list_attributes(
            &store,
            root,
            None,
            0,
            DecodeLimits::default(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
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
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("invalid attribute decode limits succeeded")?,
    ];
    assert!(matches!(
        failures[0].error,
        DirectoryReadError::WrongRootKind
    ));
    assert!(matches!(failures[1].error, DirectoryReadError::ZeroLimit));
    assert!(matches!(
        failures[2].error,
        DirectoryReadError::InvalidLimits
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
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("height-limited attribute traversal succeeded")?;
    assert!(matches!(height.error, DirectoryReadError::HeightExceeded));
    assert_eq!(height.work.page_reads, 1);

    let leaf = put(
        &store,
        &AttributePage::Leaf(vec![entry("a", 1)?, entry("b", 2)?]),
        8,
    )
    .await?;
    let alias_root = put(
        &store,
        &AttributePage::Internal(vec![
            Child {
                first: name("a")?,
                page: leaf,
            },
            Child {
                first: name("c")?,
                page: leaf,
            },
        ]),
        8,
    )
    .await?;
    let alias = list_attributes(
        &store,
        alias_root,
        None,
        4,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("aliased attribute page succeeded")?;
    assert!(matches!(alias.error, DirectoryReadError::Cycle));
    assert_eq!(alias.work.page_reads, 2);
    Ok(())
}

#[tokio::test]
async fn attribute_class_bound_skips_annotation_only_subtrees()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let mut children = Vec::new();
    let classes = [
        AttributeClass::PosixXattr,
        AttributeClass::WindowsStream,
        AttributeClass::MacResourceFork,
    ];
    for (index, class) in classes.into_iter().enumerate() {
        let mut value = entry("native", u8::try_from(index + 1)?)?;
        value.name = AttributeName::new(class, b"native".to_vec())?;
        children.push(Child {
            first: value.name.clone(),
            page: put(&store, &AttributePage::Leaf(vec![value]), 8).await?,
        });
    }
    // This authenticated branch has no backing object: a native read must never open it.
    children.push(Child {
        first: AttributeName::new(AttributeClass::Annotation, b"application".to_vec())?,
        page: ObjectId {
            storage: None,
            kind: ObjectKind::AttributePage,
            digest: Digest::from_bytes([99; 32]),
        },
    });
    let root = put(&store, &AttributePage::Internal(children), 8).await?;
    let cancellation = CancellationToken::new();
    for (index, class) in classes.into_iter().enumerate() {
        let mut accounting = super::super::allocation::KernelAccounting::standalone(
            WorkBudget::UNBOUNDED,
            &cancellation,
        );
        let (page, _) = list_attributes_async_with_accounting(
            &store,
            root,
            None,
            256,
            Some(class),
            DecodeLimits::default(),
            &mut accounting,
            &cancellation,
            None,
        )
        .await?;
        assert_eq!(page.entries.len(), index + 1);
        assert!(page.entries.iter().all(|entry| entry.name.class() <= class));
        assert!(!page.has_more);
        assert!(page.next_residency.is_none());
        assert_eq!(page.work.page_reads, u64::try_from(index + 2)?);
    }
    assert!(
        list_attributes(
            &store,
            root,
            None,
            256,
            DecodeLimits::default(),
            WorkBudget::UNBOUNDED,
            &cancellation
        )
        .await
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn attribute_ranges_stop_at_both_bounds_in_either_direction()
-> Result<(), Box<dyn std::error::Error>> {
    async fn check<S: AsyncObjectStore>(
        store: &S,
        root: ObjectId,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let lower = name("b")?;
        let upper = name("e")?;
        for direction in [
            acyclic_collections::page::Direction::Ascending,
            acyclic_collections::page::Direction::Descending,
        ] {
            for inclusive in [false, true] {
                for maximum in [1, 2, 8] {
                    let (start, end) = match direction {
                        acyclic_collections::page::Direction::Ascending => (
                            Bound::Included(&lower),
                            if inclusive {
                                Bound::Included(&upper)
                            } else {
                                Bound::Excluded(&upper)
                            },
                        ),
                        acyclic_collections::page::Direction::Descending => (
                            Bound::Included(&upper),
                            if inclusive {
                                Bound::Included(&lower)
                            } else {
                                Bound::Excluded(&lower)
                            },
                        ),
                    };
                    let expected: Vec<&[u8]> = match (direction, inclusive) {
                        (acyclic_collections::page::Direction::Ascending, true) => {
                            vec![b"b", b"c", b"d", b"e"]
                        }
                        (acyclic_collections::page::Direction::Ascending, false) => {
                            vec![b"b", b"c", b"d"]
                        }
                        (acyclic_collections::page::Direction::Descending, true) => {
                            vec![b"e", b"d", b"c", b"b"]
                        }
                        (acyclic_collections::page::Direction::Descending, false) => {
                            vec![b"e", b"d", b"c"]
                        }
                    };
                    let mut accounting = super::super::allocation::KernelAccounting::standalone(
                        WorkBudget::UNBOUNDED,
                        &cancellation,
                    );
                    let (page, _) =
                        persistent_pagination::paginate_range_with_accounting::<AttributeFormat>(
                            store,
                            (root, start, end, maximum, direction, false),
                            DecodeLimits::default(),
                            &mut accounting,
                            &cancellation,
                            None,
                        )
                        .await?;
                    let count = usize::try_from(maximum)?.min(expected.len());
                    assert_eq!(
                        page.values
                            .iter()
                            .map(|entry| entry.name.as_bytes())
                            .collect::<Vec<_>>(),
                        expected[..count]
                    );
                    assert_eq!(page.has_more, expected.len() > count);
                }
            }
        }
        Ok(())
    }
    let backend = MemoryObjectStore::default();
    let root = tree(&backend).await?;
    check(&backend, root).await?;
    let cached = CachedObjectStore::new(
        backend,
        ObjectCacheOptions {
            maximum_entries: 16,
            maximum_bytes: 1024 * 1024,
        },
    )?;
    check(&cached, root).await?;
    check(&cached, root).await?;
    Ok(())
}
