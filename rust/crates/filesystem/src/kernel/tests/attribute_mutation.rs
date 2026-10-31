use super::*;
use crate::CancellationToken;
use crate::async_storage::AsyncObjectStore;
use crate::foundation::Digest;
use crate::kernel::AttributePage;
use crate::kernel::persistent_btree::Mutation;
use crate::kernel::{decode_attribute_page, encode_attribute_page};
use crate::memory::MemoryObjectStore;

use crate::kernel::Child;
fn name(byte: u8, length: usize) -> Result<AttributeName, Box<dyn std::error::Error>> {
    Ok(AttributeName::new(
        super::super::AttributeClass::PosixXattr,
        vec![byte; length],
        u32::try_from(length)?,
    )?)
}

fn entry(
    byte: u8,
    name_length: usize,
    value_byte: u8,
) -> Result<AttributeEntry, Box<dyn std::error::Error>> {
    Ok(AttributeEntry {
        name: name(byte, name_length)?,
        value_bytes: u64::from(value_byte),
        value: ObjectId {
            storage: None,
            kind: ObjectKind::Blob,
            digest: Digest::from_bytes([value_byte; 32]),
        },
    })
}

use crate::test_support::put_attribute_page as put;

/// Two-item pages: the item limit closes each page at its second item, so
/// canonical pages are pairs, whatever the keys hash to.
fn pair_limits() -> DecodeLimits {
    DecodeLimits {
        maximum_page_items: 2,
        ..DecodeLimits::default()
    }
}

/// The canonical tree of attributes 1 3 4 6: leaves `[1 3]` and `[4 6]`.
async fn two_leaf_tree(
    store: &MemoryObjectStore,
) -> Result<(ObjectId, [ObjectId; 2]), Box<dyn std::error::Error>> {
    let empty = put(store, &AttributePage::Leaf(Vec::new()), 8).await?;
    let root = apply_attribute_mutations(
        store,
        empty,
        vec![
            AttributeMutation::Insert(entry(1, 1, 1)?),
            AttributeMutation::Insert(entry(3, 1, 3)?),
            AttributeMutation::Insert(entry(4, 1, 4)?),
            AttributeMutation::Insert(entry(6, 1, 6)?),
        ],
        4,
        pair_limits(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?
    .value;
    let bytes = store
        .read(
            root,
            u64::MAX,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new()),
        )
        .await?;
    let AttributePage::Internal(children) =
        decode_attribute_page(&bytes, pair_limits(), &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })?
    else {
        return Err("two-leaf attribute root is not internal".into());
    };
    let [left, right] = children.as_slice() else {
        return Err("two-leaf attribute root does not hold two leaves".into());
    };
    Ok((root, [left.page, right.page]))
}

async fn three_leaf_tree(
    store: &MemoryObjectStore,
) -> Result<(ObjectId, [ObjectId; 3]), Box<dyn std::error::Error>> {
    let leaves = [
        put(store, &AttributePage::Leaf(vec![entry(1, 1, 1)?]), 8).await?,
        put(
            store,
            &AttributePage::Leaf(vec![entry(3, 1, 3)?, entry(4, 1, 4)?]),
            8,
        )
        .await?,
        put(store, &AttributePage::Leaf(vec![entry(6, 1, 6)?]), 8).await?,
    ];
    let root = put(
        store,
        &AttributePage::Internal(vec![
            Child {
                first: name(1, 1)?,
                page: leaves[0],
            },
            Child {
                first: name(3, 1)?,
                page: leaves[1],
            },
            Child {
                first: name(6, 1)?,
                page: leaves[2],
            },
        ]),
        8,
    )
    .await?;
    Ok((root, leaves))
}

#[tokio::test]
async fn sparse_attribute_changes_rewrite_only_one_leaf_and_root()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, leaves) = two_leaf_tree(&store).await?;
    let expected = entry(4, 1, 4)?;
    let receipt = apply_attribute_mutations(
        &store,
        root,
        vec![AttributeMutation::Replace {
            expected,
            replacement: entry(4, 1, 9)?,
        }],
        8,
        pair_limits(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(receipt.work.page_reads, 2);
    assert_eq!(receipt.work.page_writes, 2);
    let bytes = store
        .read(
            receipt.value,
            u64::MAX,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new()),
        )
        .await?;
    let AttributePage::Internal(children) =
        decode_attribute_page(&bytes, pair_limits(), &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })?
    else {
        return Err("candidate attribute root is not internal".into());
    };
    assert_eq!(children.len(), 2);
    assert_eq!(children[0].page, leaves[0]);
    assert_ne!(children[1].page, leaves[1]);
    Ok(())
}

#[tokio::test]
async fn unordered_attribute_mutations_are_sorted_once_and_persist_canonically()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put(&store, &AttributePage::Leaf(Vec::new()), 8).await?;
    let receipt = apply_attribute_mutations(
        &store,
        root,
        vec![
            AttributeMutation::Insert(entry(3, 1, 3)?),
            AttributeMutation::Insert(entry(1, 1, 1)?),
            AttributeMutation::Insert(entry(4, 1, 4)?),
            AttributeMutation::Insert(entry(2, 1, 2)?),
        ],
        4,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    let bytes = store
        .read(
            receipt.value,
            u64::MAX,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new()),
        )
        .await?;
    let AttributePage::Leaf(entries) =
        decode_attribute_page(&bytes, DecodeLimits::default(), &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })?
    else {
        return Err("unordered attribute result is not a leaf".into());
    };
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.name.as_bytes()[0])
            .collect::<Vec<_>>(),
        [1_u8, 2, 3, 4]
    );
    Ok(())
}

#[tokio::test]
async fn no_op_attribute_replace_preserves_root_without_writes()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, _) = three_leaf_tree(&store).await?;
    let same = entry(3, 1, 3)?;
    let receipt = apply_attribute_mutations(
        &store,
        root,
        vec![AttributeMutation::Replace {
            expected: same.clone(),
            replacement: same,
        }],
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(receipt.value, root);
    assert_eq!(receipt.work.page_reads, 2);
    assert_eq!(receipt.work.page_writes, 0);
    assert_eq!(receipt.work.backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn pre_cancelled_attribute_mutation_accesses_no_backend()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put(&store, &AttributePage::Leaf(vec![entry(1, 1, 1)?]), 8).await?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let failure = apply_attribute_mutations(
        &store,
        root,
        vec![AttributeMutation::Insert(entry(2, 1, 2)?)],
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await
    .err()
    .ok_or("cancelled attribute mutation unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        AttributeMutationError::Storage(ObjectStoreError::Cancelled)
    ));
    assert_eq!(failure.work.backend_read_operations, 0);
    assert_eq!(failure.work.backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn attribute_pages_split_on_bytes_before_item_count() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let first = AttributePage::Leaf(vec![entry(1, 40, 1)?]);
    let root = put(&store, &first, 8).await?;
    // Twice one item's page: each item fits in half, and two pass half, so
    // the byte bound closes pages at two items, below the item bound.
    let single = encode_attribute_page(
        &first,
        8,
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?
    .len();
    let bound = u32::try_from(2 * single)?;
    let limits = DecodeLimits {
        maximum_page_bytes: bound,
        maximum_page_items: 8,
        maximum_name_bytes: 40,
        ..DecodeLimits::default()
    };
    let receipt = apply_attribute_mutations(
        &store,
        root,
        vec![
            AttributeMutation::Insert(entry(2, 40, 2)?),
            AttributeMutation::Insert(entry(3, 40, 3)?),
        ],
        2,
        limits,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(receipt.work.page_writes, 3);
    let root_bytes = store
        .read(
            receipt.value,
            u64::MAX,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new()),
        )
        .await?;
    assert!(root_bytes.len() <= 2 * single);
    let AttributePage::Internal(children) =
        decode_attribute_page(&root_bytes, limits, &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })?
    else {
        return Err("byte-bounded candidate attribute root is not internal".into());
    };
    assert_eq!(children.len(), 2);
    let mut sizes = Vec::new();
    for child in children {
        let bytes = store
            .read(
                child.page,
                u64::MAX,
                &mut crate::AuthorityWork::new(
                    WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new(),
                ),
            )
            .await?;
        assert!(bytes.len() <= 2 * single);
        let AttributePage::Leaf(entries) = decode_attribute_page(&bytes, limits, &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })?
        else {
            return Err("byte-bounded attribute child is not a leaf".into());
        };
        sizes.push(entries.len());
    }
    assert_eq!(sizes, [2, 1]);
    Ok(())
}

#[tokio::test]
async fn oversized_attribute_item_takes_a_page_and_only_the_object_bound_refuses()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put(&store, &AttributePage::Leaf(Vec::new()), 8).await?;
    let mut limits = DecodeLimits {
        maximum_page_bytes: 99,
        maximum_name_bytes: 40,
        ..DecodeLimits::default()
    };
    let inserted = apply_attribute_mutations(
        &store,
        root,
        vec![
            AttributeMutation::Insert(entry(1, 1, 1)?),
            AttributeMutation::Insert(entry(2, 40, 2)?),
            AttributeMutation::Insert(entry(3, 1, 3)?),
        ],
        3,
        limits,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    // Every item here passes half the capacity and closes the leaf it joins:
    // three leaves, a pair and a single above them, and the root.
    assert_eq!(inserted.work.page_writes, 6);
    limits.maximum_object_bytes = 99;
    let failure = apply_attribute_mutations(
        &store,
        root,
        vec![AttributeMutation::Insert(entry(1, 40, 1)?)],
        1,
        limits,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("item past the object bound unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        AttributeMutationError::PageItemTooLarge
    ));
    assert_eq!(failure.work.backend_write_operations, 0);
    Ok(())
}

#[test]
fn mutation_validation_and_current_state_matrix_is_total() -> Result<(), Box<dyn std::error::Error>>
{
    let invalid = AttributeEntry {
        value: ObjectId {
            storage: None,
            kind: ObjectKind::TreePage,
            digest: Digest::from_bytes([1; 32]),
        },
        ..entry(1, 1, 1)?
    };
    assert!(matches!(
        AttributeMutation::Insert(invalid).validate(),
        Err(AttributeMutationError::InvalidEntry)
    ));
    assert!(matches!(
        AttributeMutation::Remove {
            name: name(2, 1)?,
            expected: Some(entry(1, 1, 1)?),
        }
        .validate(),
        Err(AttributeMutationError::NameMismatch)
    ));
    assert!(matches!(
        AttributeMutation::Replace {
            expected: entry(1, 1, 1)?,
            replacement: entry(2, 1, 2)?,
        }
        .validate(),
        Err(AttributeMutationError::NameMismatch)
    ));
    assert!(
        AttributeMutation::Remove {
            name: name(1, 1)?,
            expected: None,
        }
        .validate()
        .is_ok()
    );

    let insert = AttributeMutation::Insert(entry(1, 1, 1)?);
    assert_eq!(insert.key(), &name(1, 1)?);
    assert!(insert.changes_cardinality());
    let mut current = Some(entry(1, 1, 1)?);
    assert!(matches!(
        insert.replacement(current.as_ref()),
        Err(AttributeMutationError::AlreadyExists)
    ));
    current = None;
    assert!(insert.replacement(current.as_ref()).is_ok());

    let remove = AttributeMutation::Remove {
        name: name(1, 1)?,
        expected: Some(entry(1, 1, 2)?),
    };
    current = None;
    assert!(matches!(
        remove.replacement(current.as_ref()),
        Err(AttributeMutationError::Missing)
    ));
    current = Some(entry(1, 1, 1)?);
    assert!(matches!(
        remove.replacement(current.as_ref()),
        Err(AttributeMutationError::StateConflict)
    ));
    current = Some(entry(1, 1, 2)?);
    assert!(remove.replacement(current.as_ref())?.is_none());

    let replace = AttributeMutation::Replace {
        expected: entry(1, 1, 1)?,
        replacement: entry(1, 1, 2)?,
    };
    assert!(!replace.changes_cardinality());
    current = None;
    assert!(matches!(
        replace.replacement(current.as_ref()),
        Err(AttributeMutationError::Missing)
    ));
    current = Some(entry(1, 1, 3)?);
    assert!(matches!(
        replace.replacement(current.as_ref()),
        Err(AttributeMutationError::StateConflict)
    ));
    current = Some(entry(1, 1, 1)?);
    assert_eq!(
        replace.replacement(current.as_ref())?.cloned(),
        Some(entry(1, 1, 2)?)
    );
    Ok(())
}
