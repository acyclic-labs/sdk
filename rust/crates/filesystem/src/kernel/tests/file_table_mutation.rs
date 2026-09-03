use super::*;
use crate::foundation::Digest;
use crate::kernel::persistent_btree::Mutation;
use crate::kernel::{
    FileKind, FilePayload, FileTableChild, encode_file_table_page, file_table_page_id,
};
use crate::memory::MemoryObjectStore;
use crate::storage::ObjectStore;
use bytes::Bytes;

fn record(byte: u8, metadata_byte: u8) -> FileRecord {
    FileRecord {
        file_id: FileId::from_bytes([byte; 16]),
        kind: FileKind::Fifo,
        link_count: 1,
        metadata: ObjectId {
            kind: ObjectKind::Metadata,
            digest: Digest::from_bytes([metadata_byte; 32]),
        },
        payload: FilePayload::Empty,
    }
}

fn put(
    store: &MemoryObjectStore,
    page: &FileTablePage,
    maximum_items: u32,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let id = file_table_page_id(page, maximum_items)?;
    store.put(
        id,
        Bytes::from(encode_file_table_page(page, maximum_items)?),
        WorkBudget::UNBOUNDED,
    )?;
    Ok(id)
}

fn three_leaf_table(
    store: &MemoryObjectStore,
) -> Result<(ObjectId, [ObjectId; 3]), Box<dyn std::error::Error>> {
    let leaves = [
        put(store, &FileTablePage::Leaf(vec![record(1, 1)]), 8)?,
        put(
            store,
            &FileTablePage::Leaf(vec![record(3, 3), record(4, 4)]),
            8,
        )?,
        put(store, &FileTablePage::Leaf(vec![record(6, 6)]), 8)?,
    ];
    let root = put(
        store,
        &FileTablePage::Internal(vec![
            FileTableChild {
                first_file_id: FileId::from_bytes([1; 16]),
                page: leaves[0],
            },
            FileTableChild {
                first_file_id: FileId::from_bytes([3; 16]),
                page: leaves[1],
            },
            FileTableChild {
                first_file_id: FileId::from_bytes([6; 16]),
                page: leaves[2],
            },
        ]),
        8,
    )?;
    Ok((root, leaves))
}

#[test]
fn multiple_record_changes_rewrite_one_leaf_and_shared_root()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, leaves) = three_leaf_table(&store)?;
    let receipt = apply_file_table_mutations(
        &store,
        root,
        vec![
            FileTableMutation::Replace {
                expected: record(3, 3),
                replacement: record(3, 8),
            },
            FileTableMutation::Replace {
                expected: record(4, 4),
                replacement: record(4, 9),
            },
        ],
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(receipt.work.page_reads, 2);
    assert_eq!(receipt.work.page_writes, 2);
    let bytes = store
        .read(receipt.root, u64::MAX, WorkBudget::UNBOUNDED)?
        .value;
    let FileTablePage::Internal(children) =
        decode_file_table_page(&bytes, DecodeLimits::default())?
    else {
        return Err("candidate file table is not internal".into());
    };
    assert_eq!(children[0].page, leaves[0]);
    assert_ne!(children[1].page, leaves[1]);
    assert_eq!(children[2].page, leaves[2]);
    Ok(())
}

#[test]
fn unordered_file_table_mutations_are_sorted_once_and_persist_canonically()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put(&store, &FileTablePage::Leaf(Vec::new()), 8)?;
    let receipt = apply_file_table_mutations(
        &store,
        root,
        vec![
            FileTableMutation::Insert(record(3, 3)),
            FileTableMutation::Insert(record(1, 1)),
            FileTableMutation::Insert(record(4, 4)),
            FileTableMutation::Insert(record(2, 2)),
        ],
        4,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    let bytes = store
        .read(receipt.root, u64::MAX, WorkBudget::UNBOUNDED)?
        .value;
    let FileTablePage::Leaf(records) = decode_file_table_page(&bytes, DecodeLimits::default())?
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

#[test]
fn record_noop_preserves_root_with_zero_writes() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, _) = three_leaf_table(&store)?;
    let receipt = apply_file_table_mutations(
        &store,
        root,
        vec![FileTableMutation::Replace {
            expected: record(3, 3),
            replacement: record(3, 3),
        }],
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(receipt.root, root);
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
        Err(FileTableSemanticError::IdentityMismatch)
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

#[test]
fn async_and_sync_file_table_mutation_share_exact_results_and_work()
-> Result<(), Box<dyn std::error::Error>> {
    let sync_store = MemoryObjectStore::default();
    let async_store = MemoryObjectStore::default();
    let (sync_root, _) = three_leaf_table(&sync_store)?;
    let (async_root, _) = three_leaf_table(&async_store)?;
    assert_eq!(sync_root, async_root);
    let mutations = vec![FileTableMutation::Replace {
        expected: record(3, 3),
        replacement: record(3, 8),
    }];
    let synchronous = apply_file_table_mutations(
        &sync_store,
        sync_root,
        mutations.clone(),
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    let asynchronous = crate::async_storage::poll_ready(apply_file_table_mutations_async(
        &async_store,
        async_root,
        mutations,
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    ))
    .ok_or("memory-backed asynchronous file-table mutation unexpectedly blocked")??;
    assert_eq!(asynchronous, synchronous);
    Ok(())
}

#[test]
fn stale_record_precondition_writes_nothing() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, _) = three_leaf_table(&store)?;
    let failure = apply_file_table_mutations(
        &store,
        root,
        vec![FileTableMutation::Replace {
            expected: record(3, 7),
            replacement: record(3, 8),
        }],
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )
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

#[test]
fn page_byte_bound_splits_before_item_bound() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put(&store, &FileTablePage::Leaf(vec![record(1, 1)]), 8)?;
    let limits = DecodeLimits {
        maximum_page_bytes: 120,
        maximum_page_items: 8,
        ..DecodeLimits::default()
    };
    let receipt = apply_file_table_mutations(
        &store,
        root,
        vec![FileTableMutation::Insert(record(2, 2))],
        1,
        limits,
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(receipt.work.page_writes, 3);
    let root_bytes = store
        .read(receipt.root, u64::MAX, WorkBudget::UNBOUNDED)?
        .value;
    assert!(root_bytes.len() <= 120);
    let FileTablePage::Internal(children) = decode_file_table_page(&root_bytes, limits)? else {
        return Err("byte-bounded candidate root is not internal".into());
    };
    assert_eq!(children.len(), 2);
    for child in children {
        let bytes = store
            .read(child.page, u64::MAX, WorkBudget::UNBOUNDED)?
            .value;
        assert!(bytes.len() <= 120);
        assert!(matches!(
            decode_file_table_page(&bytes, limits)?,
            FileTablePage::Leaf(records) if records.len() == 1
        ));
    }
    Ok(())
}

#[test]
fn item_larger_than_page_bound_rejects_before_writes() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put(&store, &FileTablePage::Leaf(Vec::new()), 8)?;
    let failure = apply_file_table_mutations(
        &store,
        root,
        vec![FileTableMutation::Insert(record(1, 1))],
        1,
        DecodeLimits {
            maximum_page_bytes: 72,
            ..DecodeLimits::default()
        },
        WorkBudget::UNBOUNDED,
    )
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
fn mutation_validation_and_current_state_matrix_is_total() {
    let invalid = FileRecord {
        link_count: 0,
        ..record(1, 1)
    };
    assert_eq!(
        FileTableMutation::Insert(invalid).validate(),
        Err(FileTableSemanticError::InvalidRecord)
    );
    assert_eq!(
        FileTableMutation::Remove {
            file_id: FileId::from_bytes([2; 16]),
            expected: Some(record(1, 1)),
        }
        .validate(),
        Err(FileTableSemanticError::IdentityMismatch)
    );
    assert_eq!(
        FileTableMutation::Replace {
            expected: record(1, 1),
            replacement: record(2, 2),
        }
        .validate(),
        Err(FileTableSemanticError::IdentityMismatch)
    );
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
    assert_eq!(
        insert.apply_current(&mut current),
        Err(FileTableSemanticError::AlreadyExists)
    );
    current = None;
    assert!(insert.apply_current(&mut current).is_ok());
    assert_eq!(current, Some(record(1, 1)));

    let remove = FileTableMutation::Remove {
        file_id: FileId::from_bytes([1; 16]),
        expected: Some(record(1, 2)),
    };
    current = None;
    assert_eq!(
        remove.apply_current(&mut current),
        Err(FileTableSemanticError::Missing)
    );
    current = Some(record(1, 1));
    assert_eq!(
        remove.apply_current(&mut current),
        Err(FileTableSemanticError::StateConflict)
    );
    current = Some(record(1, 2));
    assert!(remove.apply_current(&mut current).is_ok());
    assert_eq!(current, None);

    let replace = FileTableMutation::Replace {
        expected: record(1, 1),
        replacement: record(1, 2),
    };
    assert!(!replace.changes_cardinality());
    current = None;
    assert_eq!(
        replace.apply_current(&mut current),
        Err(FileTableSemanticError::Missing)
    );
    current = Some(record(1, 3));
    assert_eq!(
        replace.apply_current(&mut current),
        Err(FileTableSemanticError::StateConflict)
    );
    current = Some(record(1, 1));
    assert!(replace.apply_current(&mut current).is_ok());
    assert_eq!(current, Some(record(1, 2)));
}

#[test]
fn persistent_error_translation_is_total() {
    assert!(matches!(
        map_error(persistent_btree::Error::Empty),
        FileTableMutationError::Empty
    ));
    assert!(matches!(
        map_error(persistent_btree::Error::TooManyMutations),
        FileTableMutationError::TooManyMutations
    ));
    assert!(matches!(
        map_error(persistent_btree::Error::WrongRootKind),
        FileTableMutationError::WrongRootKind
    ));
    assert!(matches!(
        map_error(persistent_btree::Error::InvalidLimits),
        FileTableMutationError::InvalidLimits
    ));
    assert!(matches!(
        map_error(persistent_btree::Error::PageItemTooLarge),
        FileTableMutationError::PageItemTooLarge
    ));
    assert!(matches!(
        map_error(persistent_btree::Error::HeightExceeded),
        FileTableMutationError::HeightExceeded
    ));
    assert!(matches!(
        map_error(persistent_btree::Error::CycleOrAlias),
        FileTableMutationError::CycleOrAlias
    ));
    assert!(matches!(
        map_error(persistent_btree::Error::ChildBoundsMismatch),
        FileTableMutationError::ChildBoundsMismatch
    ));
    assert!(matches!(
        map_error(persistent_btree::Error::MutationContract),
        FileTableMutationError::MutationContract
    ));
    assert!(matches!(
        map_error(persistent_btree::Error::AllocationFailed),
        FileTableMutationError::AllocationFailed
    ));
    assert!(matches!(
        map_error(persistent_btree::Error::Allocation(
            AllocationError::CapacityExceeded
        )),
        FileTableMutationError::AllocationFailed
    ));
    assert!(matches!(
        map_error(persistent_btree::Error::Allocation(AllocationError::Work(
            WorkError::Overflow
        ))),
        FileTableMutationError::Work(WorkError::Overflow)
    ));
    assert!(matches!(
        map_error(persistent_btree::Error::Semantic(
            FileTableSemanticError::IdentityMismatch
        )),
        FileTableMutationError::IdentityMismatch
    ));
    assert!(matches!(
        map_error(persistent_btree::Error::Storage(ObjectStoreError::Missing)),
        FileTableMutationError::Storage(ObjectStoreError::Missing)
    ));
    assert!(matches!(
        map_error(persistent_btree::Error::Decode(
            CanonicalDecodeError::LengthOverflow
        )),
        FileTableMutationError::Decode(CanonicalDecodeError::LengthOverflow)
    ));
    assert!(matches!(
        map_error(persistent_btree::Error::Work(WorkError::Overflow)),
        FileTableMutationError::Work(WorkError::Overflow)
    ));
    for semantic in [
        FileTableSemanticError::InvalidRecord,
        FileTableSemanticError::AlreadyExists,
        FileTableSemanticError::Missing,
        FileTableSemanticError::StateConflict,
    ] {
        let mapped = FileTableMutationError::from(semantic);
        assert!(matches!(
            (semantic, mapped),
            (
                FileTableSemanticError::InvalidRecord,
                FileTableMutationError::InvalidRecord
            ) | (
                FileTableSemanticError::AlreadyExists,
                FileTableMutationError::AlreadyExists
            ) | (
                FileTableSemanticError::Missing,
                FileTableMutationError::Missing
            ) | (
                FileTableSemanticError::StateConflict,
                FileTableMutationError::StateConflict
            )
        ));
    }
}
