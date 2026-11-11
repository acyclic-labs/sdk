use super::*;
use crate::async_storage::AsyncObjectStore;
use crate::foundation::Digest;
use crate::kernel::persistent_btree::Mutation;
use crate::kernel::{FileKind, FilePayload, FileTablePage};
use crate::kernel::{decode_file_table_page, encode_file_table_page};
use crate::memory::MemoryObjectStore;

use crate::kernel::Child;
#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_table_encoding_denial_precedes_native_payload_and_backend_write()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put(&store, &FileTablePage::Leaf(vec![record(1, 1)]), 8).await?;
    let mutations = [FileTableMutation::Replace {
        expected: record(1, 1),
        replacement: record(1, 2),
    }];
    let cancellation = crate::CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(
        WorkBudget {
            bytes_encoded: 0,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    let live = work.live_allocation_bytes();
    let mut accounting = super::super::allocation::KernelAccounting::native(&work, &cancellation);
    let error = apply_file_table_mutations_with_accounting(
        &store,
        root,
        &mutations,
        DecodeLimits::default(),
        &mut accounting,
        &cancellation,
    )
    .await
    .expect_err("original canonical encoder must refuse before producing payload bytes");
    assert!(matches!(
        error,
        crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(_))
            | crate::FsError::Work(_)
    ));
    assert_eq!(work.receipt().bytes_encoded, 0);
    assert_eq!(work.receipt().page_writes, 0);
    assert_eq!(work.receipt().backend_write_operations, 0);
    assert_eq!(work.live_allocation_bytes(), live);
    Ok(())
}
fn record(byte: u8, metadata_byte: u8) -> FileRecord {
    FileRecord {
        bindings: None,
        file_id: FileId::from_bytes([byte; 16]),
        kind: FileKind::Fifo,
        link_count: 1,
        metadata: ObjectId {
            storage: None,
            kind: ObjectKind::Metadata,
            digest: Digest::from_bytes([metadata_byte; 32]),
        },
        payload: FilePayload::Empty,
    }
}

use crate::test_support::put_file_table_page as put;

/// Two-item pages: the item limit closes each page at its second item, so
/// canonical pages are pairs, whatever the keys hash to.
fn pair_limits() -> DecodeLimits {
    DecodeLimits {
        maximum_page_items: 2,
        ..DecodeLimits::default()
    }
}

/// The canonical table of records 1 3 4 6: leaves `[1 3]` and `[4 6]`.
async fn two_leaf_table(
    store: &MemoryObjectStore,
) -> Result<(ObjectId, [ObjectId; 2]), Box<dyn std::error::Error>> {
    let empty = put(store, &FileTablePage::Leaf(Vec::new()), 8).await?;
    let root = apply_file_table_mutations(
        store,
        empty,
        [1, 3, 4, 6]
            .into_iter()
            .map(|byte| FileTableMutation::Insert(record(byte, byte)))
            .collect(),
        pair_limits(),
        WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
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
    let FileTablePage::Internal(children) =
        decode_file_table_page(&bytes, pair_limits(), &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })?
    else {
        return Err("two-leaf table root is not internal".into());
    };
    let [left, right] = children.as_slice() else {
        return Err("two-leaf table root does not hold two leaves".into());
    };
    Ok((root, [left.page, right.page]))
}

async fn three_leaf_table(
    store: &MemoryObjectStore,
) -> Result<(ObjectId, [ObjectId; 3]), Box<dyn std::error::Error>> {
    let leaves = [
        put(store, &FileTablePage::Leaf(vec![record(1, 1)]), 8).await?,
        put(
            store,
            &FileTablePage::Leaf(vec![record(3, 3), record(4, 4)]),
            8,
        )
        .await?,
        put(store, &FileTablePage::Leaf(vec![record(6, 6)]), 8).await?,
    ];
    let root = put(
        store,
        &FileTablePage::Internal(vec![
            Child {
                first: FileId::from_bytes([1; 16]),
                page: leaves[0],
            },
            Child {
                first: FileId::from_bytes([3; 16]),
                page: leaves[1],
            },
            Child {
                first: FileId::from_bytes([6; 16]),
                page: leaves[2],
            },
        ]),
        8,
    )
    .await?;
    Ok((root, leaves))
}

#[tokio::test]
async fn multiple_record_changes_rewrite_one_leaf_and_shared_root()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, leaves) = two_leaf_table(&store).await?;
    let receipt = apply_file_table_mutations(
        &store,
        root,
        vec![
            FileTableMutation::Replace {
                expected: record(4, 4),
                replacement: record(4, 8),
            },
            FileTableMutation::Replace {
                expected: record(6, 6),
                replacement: record(6, 9),
            },
        ],
        pair_limits(),
        WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
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
    let FileTablePage::Internal(children) =
        decode_file_table_page(&bytes, pair_limits(), &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })?
    else {
        return Err("candidate file table is not internal".into());
    };
    assert_eq!(children.len(), 2);
    assert_eq!(children[0].page, leaves[0]);
    assert_ne!(children[1].page, leaves[1]);
    Ok(())
}

#[tokio::test]
async fn unordered_file_table_mutations_are_sorted_once_and_persist_canonically()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put(&store, &FileTablePage::Leaf(Vec::new()), 8).await?;
    let receipt = apply_file_table_mutations(
        &store,
        root,
        vec![
            FileTableMutation::Insert(record(3, 3)),
            FileTableMutation::Insert(record(1, 1)),
            FileTableMutation::Insert(record(4, 4)),
            FileTableMutation::Insert(record(2, 2)),
        ],
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await?;
    let bytes = store
        .read(
            receipt.value,
            u64::MAX,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new()),
        )
        .await?;
    let FileTablePage::Leaf(records) =
        decode_file_table_page(&bytes, DecodeLimits::default(), &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })?
    else {
        return Err("unordered file-table result is not a leaf".into());
    };
    assert_eq!(
        records
            .iter()
            .map(|record| record.file_id)
            .collect::<Vec<_>>(),
        [1_u8, 2, 3, 4].map(|byte| FileId::from_bytes([byte; 16]))
    );
    Ok(())
}

#[tokio::test]
async fn record_noop_preserves_root_with_zero_writes() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, _) = three_leaf_table(&store).await?;
    let receipt = apply_file_table_mutations(
        &store,
        root,
        vec![FileTableMutation::Replace {
            expected: record(3, 3),
            replacement: record(3, 3),
        }],
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await?;
    assert_eq!(receipt.value, root);
    assert_eq!(receipt.work.page_reads, 2);
    assert_eq!(receipt.work.page_writes, 0);
    assert_eq!(receipt.work.backend_write_operations, 0);
    Ok(())
}

#[test]
fn remove_validation_requires_the_exact_file_identity() {
    let expected = record(1, 1);
    assert!(matches!(
        FileTableMutation::Remove {
            file_id: FileId::from_bytes([2; 16]),
            expected: Some(expected),
        }
        .validate(),
        Err(FileTableMutationError::IdentityMismatch)
    ));
    assert!(
        FileTableMutation::Remove {
            file_id: expected.file_id,
            expected: Some(expected),
        }
        .validate()
        .is_ok()
    );
}

#[tokio::test]
async fn stale_record_precondition_writes_nothing() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, _) = three_leaf_table(&store).await?;
    let failure = apply_file_table_mutations(
        &store,
        root,
        vec![FileTableMutation::Replace {
            expected: record(3, 7),
            replacement: record(3, 8),
        }],
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("stale record unexpectedly replaced")?;
    assert!(matches!(
        failure.error,
        FileTableMutationError::StateConflict
    ));
    assert_eq!(failure.work.page_reads, 2);
    assert_eq!(failure.work.backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn page_byte_bound_splits_before_item_bound() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let first = FileTablePage::Leaf(vec![record(1, 1)]);
    let root = put(&store, &first, 8).await?;
    // Twice one record's page: each record fits in half, and two pass half,
    // so the byte bound closes pages at two records, below the item bound.
    let single = encode_file_table_page(
        &first,
        8,
        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
    )?
    .len();
    let limits = DecodeLimits {
        maximum_page_bytes: u32::try_from(2 * single)?,
        maximum_page_items: 8,
        ..DecodeLimits::default()
    };
    let receipt = apply_file_table_mutations(
        &store,
        root,
        vec![
            FileTableMutation::Insert(record(2, 2)),
            FileTableMutation::Insert(record(3, 3)),
        ],
        limits,
        WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
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
    let FileTablePage::Internal(children) =
        decode_file_table_page(&root_bytes, limits, &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })?
    else {
        return Err("byte-bounded candidate root is not internal".into());
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
        let FileTablePage::Leaf(records) = decode_file_table_page(&bytes, limits, &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })?
        else {
            return Err("byte-bounded table child is not a leaf".into());
        };
        sizes.push(records.len());
    }
    assert_eq!(sizes, [2, 1]);
    Ok(())
}

#[tokio::test]
async fn item_larger_than_page_bound_takes_a_page_and_only_the_object_bound_refuses()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put(&store, &FileTablePage::Leaf(Vec::new()), 8).await?;
    let mut limits = DecodeLimits {
        maximum_page_bytes: 72,
        ..DecodeLimits::default()
    };
    let inserted = apply_file_table_mutations(
        &store,
        root,
        vec![FileTableMutation::Insert(record(1, 1))],
        limits,
        WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await?;
    assert_eq!(inserted.work.page_writes, 1);
    let bytes = store
        .read(
            inserted.value,
            u64::MAX,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new()),
        )
        .await?;
    assert!(bytes.len() > 72);
    limits.maximum_object_bytes = 72;
    let failure = apply_file_table_mutations(
        &store,
        root,
        vec![FileTableMutation::Insert(record(1, 1))],
        limits,
        WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("oversized file-table item unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        FileTableMutationError::PageItemTooLarge
    ));
    assert_eq!(failure.work.backend_write_operations, 0);
    Ok(())
}

#[test]
fn mutation_validation_and_current_state_matrix_is_total() -> Result<(), Box<dyn std::error::Error>>
{
    let invalid = FileRecord {
        link_count: 0,
        ..record(1, 1)
    };
    assert!(matches!(
        FileTableMutation::Insert(invalid).validate(),
        Err(FileTableMutationError::InvalidRecord)
    ));
    assert!(matches!(
        FileTableMutation::Remove {
            file_id: FileId::from_bytes([2; 16]),
            expected: Some(record(1, 1)),
        }
        .validate(),
        Err(FileTableMutationError::IdentityMismatch)
    ));
    assert!(matches!(
        FileTableMutation::Replace {
            expected: record(1, 1),
            replacement: record(2, 2),
        }
        .validate(),
        Err(FileTableMutationError::IdentityMismatch)
    ));
    assert!(
        FileTableMutation::Remove {
            file_id: FileId::from_bytes([1; 16]),
            expected: None,
        }
        .validate()
        .is_ok()
    );

    let insert = FileTableMutation::Insert(record(1, 1));
    assert_eq!(insert.key(), &FileId::from_bytes([1; 16]));
    assert!(insert.changes_cardinality());
    let mut current = Some(record(1, 1));
    assert!(matches!(
        insert.replacement(current.as_ref()),
        Err(FileTableMutationError::AlreadyExists)
    ));
    current = None;
    assert!(insert.replacement(current.as_ref()).is_ok());
    assert_eq!(
        insert.replacement(current.as_ref())?.cloned(),
        Some(record(1, 1))
    );

    let remove = FileTableMutation::Remove {
        file_id: FileId::from_bytes([1; 16]),
        expected: Some(record(1, 2)),
    };
    current = None;
    assert!(matches!(
        remove.replacement(current.as_ref()),
        Err(FileTableMutationError::Missing)
    ));
    current = Some(record(1, 1));
    assert!(matches!(
        remove.replacement(current.as_ref()),
        Err(FileTableMutationError::StateConflict)
    ));
    current = Some(record(1, 2));
    assert!(remove.replacement(current.as_ref())?.is_none());

    let replace = FileTableMutation::Replace {
        expected: record(1, 1),
        replacement: record(1, 2),
    };
    assert!(!replace.changes_cardinality());
    current = None;
    assert!(matches!(
        replace.replacement(current.as_ref()),
        Err(FileTableMutationError::Missing)
    ));
    current = Some(record(1, 3));
    assert!(matches!(
        replace.replacement(current.as_ref()),
        Err(FileTableMutationError::StateConflict)
    ));
    current = Some(record(1, 1));
    assert_eq!(
        replace.replacement(current.as_ref())?.cloned(),
        Some(record(1, 2))
    );
    Ok(())
}
