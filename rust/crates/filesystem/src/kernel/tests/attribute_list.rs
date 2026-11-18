use super::*;
use crate::foundation::Digest;
use crate::kernel::{AttributeClass, AttributePage};
use crate::memory::MemoryObjectStore;
use crate::storage::ObjectKind;
use crate::{CachedObjectStore, ObjectCacheOptions};

use crate::kernel::Child;

#[path = "../../../tests/support/attribute_entries.rs"]
mod attribute_entries;
use attribute_entries::{entry, name};

use crate::test_support::put_attribute_page as put;

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
        let mut value = entry("native", 1, u8::try_from(index + 1)?)?;
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
