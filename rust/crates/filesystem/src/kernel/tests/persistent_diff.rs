use super::*;
use crate::async_storage::AsyncObjectStore;
use crate::foundation::{Digest, FileId};
use crate::kernel::{FileKind, NameEncoding, TreeChild, TreePage, encode_tree_page, tree_page_id};
use crate::kernel::{
    FilePayload, FileRecord, FileTableChild, FileTablePage, InlineFileData, encode_file_table_page,
    file_table_page_id,
};
use crate::memory::MemoryObjectStore;
use crate::storage::{ObjectKind, ObjectStore};
use crate::test_support::OwnedReadObjectStore;
use crate::{CachedObjectStore, ObjectCacheOptions};
use bytes::Bytes;
use std::mem::size_of;

fn name(value: &str) -> Result<LogicalName, Box<dyn std::error::Error>> {
    Ok(LogicalName::new(
        NameEncoding::Utf8,
        value.as_bytes().to_vec(),
        255,
    )?)
}

fn entry(value: &str, id: u8) -> Result<TreeEntry, Box<dyn std::error::Error>> {
    Ok(TreeEntry {
        name: name(value)?,
        file_id: FileId::from_bytes([id; 16]),
        kind: FileKind::Regular,
    })
}

fn put(store: &MemoryObjectStore, page: &TreePage) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let id = tree_page_id(page, 8)?;
    ObjectStore::put(
        store,
        id,
        Bytes::from(encode_tree_page(page, 8)?),
        WorkBudget::UNBOUNDED,
    )?;
    Ok(id)
}

fn file_record(id: u8) -> Result<FileRecord, Box<dyn std::error::Error>> {
    Ok(FileRecord {
        file_id: FileId::from_bytes([id; 16]),
        kind: FileKind::Regular,
        link_count: 1,
        metadata: ObjectId {
            kind: ObjectKind::Metadata,
            digest: Digest::from_bytes([id; 32]),
        },
        payload: FilePayload::InlineRegular(InlineFileData::new(&[])?),
    })
}

fn put_file_table(
    store: &MemoryObjectStore,
    page: &FileTablePage,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let id = file_table_page_id(page, 8)?;
    ObjectStore::put(
        store,
        id,
        Bytes::from(encode_file_table_page(page, 8)?),
        WorkBudget::UNBOUNDED,
    )?;
    Ok(id)
}

fn diff<S: AsyncObjectStore>(
    store: &S,
    before: Option<ObjectId>,
    after: Option<ObjectId>,
    maximum_changes: u32,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Option<Result<Diff<LogicalName, TreeEntry>, OperationFailure<DiffError>>> {
    crate::async_storage::poll_ready(diff_tree_entries_async(
        store,
        before,
        after,
        maximum_changes,
        DecodeLimits::default(),
        budget,
        cancellation,
    ))
}

#[test]
fn diff_validates_inputs_and_keeps_noop_paths_at_zero_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put(&store, &TreePage::Leaf(vec![entry("a", 1)?]))?;
    let cancellation = CancellationToken::new();
    for (before, after) in [(None, None), (Some(root), Some(root))] {
        let result = diff(
            &store,
            before,
            after,
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .ok_or("memory diff blocked")??;
        assert!(result.changes.is_empty());
        assert!(!result.truncated);
        assert_eq!(result.work, WorkCounters::default());
    }
    for (before, after) in [(None, Some(root)), (Some(root), None)] {
        let failure = diff(
            &store,
            before,
            after,
            0,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .ok_or("memory diff blocked")?
        .err()
        .ok_or("zero diff limit unexpectedly succeeded")?;
        assert!(matches!(failure.error, DiffError::InvalidLimit));
        assert_eq!(*failure.work, WorkCounters::default());
    }
    let wrong = ObjectId {
        kind: ObjectKind::BlobChunk,
        digest: Digest::from_bytes([7; 32]),
    };
    let failure = diff(
        &store,
        Some(wrong),
        Some(root),
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .ok_or("memory diff blocked")?
    .err()
    .ok_or("wrong diff root kind unexpectedly succeeded")?;
    assert!(matches!(failure.error, DiffError::WrongRootKind));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn leaf_diff_reports_sorted_add_delete_modify_and_exact_truncation()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let before = put(
        &store,
        &TreePage::Leaf(vec![entry("a", 1)?, entry("b", 2)?]),
    )?;
    let after = put(
        &store,
        &TreePage::Leaf(vec![entry("a", 3)?, entry("c", 4)?]),
    )?;
    let cancellation = CancellationToken::new();
    let complete = diff(
        &store,
        Some(before),
        Some(after),
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .ok_or("memory diff blocked")??;
    assert_eq!(
        complete
            .changes
            .iter()
            .map(|change| change.key.as_bytes())
            .collect::<Vec<_>>(),
        [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]
    );
    assert!(complete.changes[0].before.is_some());
    assert!(complete.changes[0].after.is_some());
    assert!(complete.changes[1].before.is_some());
    assert!(complete.changes[1].after.is_none());
    assert!(complete.changes[2].before.is_none());
    assert!(complete.changes[2].after.is_some());
    assert!(!complete.truncated);
    assert_eq!(complete.work.page_reads, 2);

    let bounded = diff(
        &store,
        Some(before),
        Some(after),
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .ok_or("memory diff blocked")??;
    assert_eq!(bounded.changes.len(), 1);
    assert!(bounded.truncated);

    let one_before = put(&store, &TreePage::Leaf(vec![entry("z", 5)?]))?;
    let one_after = put(&store, &TreePage::Leaf(vec![entry("z", 6)?]))?;
    let exact = diff(
        &store,
        Some(one_before),
        Some(one_after),
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .ok_or("memory diff blocked")??;
    assert_eq!(exact.changes.len(), 1);
    assert!(!exact.truncated);
    Ok(())
}

#[test]
fn diff_cancellation_and_budget_fail_before_unadmitted_reads()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let before = put(&store, &TreePage::Leaf(vec![entry("a", 1)?]))?;
    let after = put(&store, &TreePage::Leaf(vec![entry("a", 2)?]))?;
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failure = diff(
        &store,
        Some(before),
        Some(after),
        1,
        WorkBudget::UNBOUNDED,
        &cancelled,
    )
    .ok_or("memory diff blocked")?
    .err()
    .ok_or("cancelled diff unexpectedly succeeded")?;
    assert!(matches!(failure.error, DiffError::Cancelled));
    assert_eq!(*failure.work, WorkCounters::default());

    let failure = diff(
        &store,
        Some(before),
        Some(after),
        1,
        WorkBudget {
            page_reads: 0,
            ..WorkBudget::UNBOUNDED
        },
        &CancellationToken::new(),
    )
    .ok_or("memory diff blocked")?
    .err()
    .ok_or("unadmitted diff read unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        DiffError::Work(WorkError::BudgetExceeded {
            counter: "page_reads",
            observed: 1,
            maximum: 0,
        })
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn internal_diff_skips_shared_merkle_children() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let a = put(&store, &TreePage::Leaf(vec![entry("a", 1)?]))?;
    let c_before = put(&store, &TreePage::Leaf(vec![entry("c", 2)?]))?;
    let c_after = put(&store, &TreePage::Leaf(vec![entry("c", 3)?]))?;
    let e = put(&store, &TreePage::Leaf(vec![entry("e", 4)?]))?;
    let a_name = name("a")?;
    let c_name = name("c")?;
    let e_name = name("e")?;
    let root = |middle| {
        TreePage::Internal(vec![
            TreeChild {
                first_name: a_name.clone(),
                page: a,
            },
            TreeChild {
                first_name: c_name.clone(),
                page: middle,
            },
            TreeChild {
                first_name: e_name.clone(),
                page: e,
            },
        ])
    };
    let before = put(&store, &root(c_before))?;
    let after = put(&store, &root(c_after))?;
    let cancellation = CancellationToken::new();
    let diff = crate::async_storage::poll_ready(diff_tree_entries_async(
        &store,
        Some(before),
        Some(after),
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("diff blocked")??;
    assert_eq!(diff.changes.len(), 1);
    assert_eq!(diff.changes[0].key.as_bytes(), b"c");
    assert_eq!(diff.work.page_reads, 4);
    Ok(())
}

#[test]
fn internal_file_table_diff_aligns_shared_and_one_sided_children()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let shared = put_file_table(&store, &FileTablePage::Leaf(vec![file_record(1)?]))?;
    let removed = put_file_table(&store, &FileTablePage::Leaf(vec![file_record(3)?]))?;
    let added_left = put_file_table(&store, &FileTablePage::Leaf(vec![file_record(2)?]))?;
    let added_right = put_file_table(&store, &FileTablePage::Leaf(vec![file_record(4)?]))?;
    let before = put_file_table(
        &store,
        &FileTablePage::Internal(vec![
            FileTableChild {
                first_file_id: FileId::from_bytes([1; 16]),
                page: shared,
            },
            FileTableChild {
                first_file_id: FileId::from_bytes([3; 16]),
                page: removed,
            },
        ]),
    )?;
    let after = put_file_table(
        &store,
        &FileTablePage::Internal(vec![
            FileTableChild {
                first_file_id: FileId::from_bytes([1; 16]),
                page: shared,
            },
            FileTableChild {
                first_file_id: FileId::from_bytes([2; 16]),
                page: added_left,
            },
            FileTableChild {
                first_file_id: FileId::from_bytes([4; 16]),
                page: added_right,
            },
        ]),
    )?;
    let result = crate::async_storage::poll_ready(diff_file_records_async(
        &store,
        Some(before),
        Some(after),
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("memory file-table diff blocked")??;
    assert_eq!(
        result
            .changes
            .iter()
            .map(|change| change.key)
            .collect::<Vec<_>>(),
        [
            FileId::from_bytes([2; 16]),
            FileId::from_bytes([3; 16]),
            FileId::from_bytes([4; 16]),
        ]
    );
    assert_eq!(result.work.page_reads, 5);
    Ok(())
}

#[test]
fn one_sided_and_mixed_shape_diffs_walk_complete_subtrees() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let a = put(&store, &TreePage::Leaf(vec![entry("a", 1)?]))?;
    let c = put(&store, &TreePage::Leaf(vec![entry("c", 2)?]))?;
    let internal = put(
        &store,
        &TreePage::Internal(vec![
            TreeChild {
                first_name: name("a")?,
                page: a,
            },
            TreeChild {
                first_name: name("c")?,
                page: c,
            },
        ]),
    )?;
    let cancellation = CancellationToken::new();
    let added = diff(
        &store,
        None,
        Some(internal),
        2,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .ok_or("memory diff blocked")??;
    assert_eq!(added.changes.len(), 2);
    assert!(
        added
            .changes
            .iter()
            .all(|change| change.before.is_none() && change.after.is_some())
    );
    assert!(!added.truncated);

    let removed = diff(
        &store,
        Some(internal),
        None,
        2,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .ok_or("memory diff blocked")??;
    assert_eq!(removed.changes.len(), 2);
    assert!(
        removed
            .changes
            .iter()
            .all(|change| change.before.is_some() && change.after.is_none())
    );

    let flattened = put(
        &store,
        &TreePage::Leaf(vec![entry("a", 1)?, entry("c", 2)?]),
    )?;
    let mixed = diff(
        &store,
        Some(internal),
        Some(flattened),
        2,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .ok_or("memory diff blocked")??;
    assert!(mixed.changes.is_empty());
    assert!(!mixed.truncated);

    let one_child = put(
        &store,
        &TreePage::Internal(vec![TreeChild {
            first_name: name("a")?,
            page: flattened,
        }]),
    )?;
    let realigned = diff(
        &store,
        Some(internal),
        Some(one_child),
        2,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .ok_or("memory diff blocked")??;
    assert!(realigned.changes.is_empty());
    assert!(!realigned.truncated);
    Ok(())
}

#[test]
fn owned_one_sided_internal_diff_stops_at_the_exact_outer_bound()
-> Result<(), Box<dyn std::error::Error>> {
    let store = OwnedReadObjectStore::default();
    let first = put(&store.inner, &TreePage::Leaf(vec![entry("a", 1)?]))?;
    let second = put(&store.inner, &TreePage::Leaf(vec![entry("z", 2)?]))?;
    let root = put(
        &store.inner,
        &TreePage::Internal(vec![
            TreeChild {
                first_name: name("a")?,
                page: first,
            },
            TreeChild {
                first_name: name("z")?,
                page: second,
            },
        ]),
    )?;
    let result = diff(
        &store,
        None,
        Some(root),
        1,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .ok_or("owned one-sided diff blocked")??;
    assert_eq!(result.changes.len(), 1);
    assert!(result.truncated);
    assert!(result.work.bytes_copied > 0);
    Ok(())
}

#[test]
fn diff_preserves_storage_and_decode_failures_after_admitted_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let valid = put(&store, &TreePage::Leaf(vec![entry("a", 1)?]))?;
    let missing = ObjectId {
        kind: ObjectKind::TreePage,
        digest: Digest::from_bytes([99; 32]),
    };
    let failure = diff(
        &store,
        Some(valid),
        Some(missing),
        2,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .ok_or("memory diff blocked")?
    .err()
    .ok_or("missing page unexpectedly diffed")?;
    assert!(matches!(
        failure.error,
        DiffError::Storage(ObjectStoreError::Missing)
    ));
    assert_eq!(failure.work.page_reads, 2);
    assert!(failure.work.backend_read_operations >= 1);

    let malformed_bytes = Bytes::from_static(b"not a canonical tree page");
    let malformed = ObjectId {
        kind: ObjectKind::TreePage,
        digest: crate::storage::object_digest(ObjectKind::TreePage, &malformed_bytes),
    };
    ObjectStore::put(&store, malformed, malformed_bytes, WorkBudget::UNBOUNDED)?;
    let failure = diff(
        &store,
        Some(valid),
        Some(malformed),
        2,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .ok_or("memory diff blocked")?
    .err()
    .ok_or("malformed page unexpectedly diffed")?;
    assert!(matches!(failure.error, DiffError::Decode(_)));
    assert!(failure.work.page_reads >= 2);
    Ok(())
}

#[test]
fn repeated_diff_reuses_authenticated_decoded_pages_without_backend_reads()
-> Result<(), Box<dyn std::error::Error>> {
    let backend = MemoryObjectStore::new(1024 * 1024)?;
    let before = put(
        &backend,
        &TreePage::Leaf(vec![entry("a", 1)?, entry("b", 2)?]),
    )?;
    let after = put(
        &backend,
        &TreePage::Leaf(vec![entry("a", 3)?, entry("c", 4)?]),
    )?;
    let store = CachedObjectStore::new(
        backend,
        ObjectCacheOptions {
            maximum_entries: 8,
            maximum_bytes: 1024 * 1024,
            maximum_in_flight: 2,
            maximum_waiters_per_object: 2,
        },
    )?;
    let cancellation = CancellationToken::new();
    let first = diff(
        &store,
        Some(before),
        Some(after),
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .ok_or("cached diff blocked")??;
    assert_eq!(first.work.backend_read_operations, 2);
    assert_eq!(store.stats()?.resident_decoded_pages, 2);

    let second = diff(
        &store,
        Some(before),
        Some(after),
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .ok_or("warm cached diff blocked")??;
    assert_eq!(second.changes, first.changes);
    assert_eq!(second.work.page_reads, 2);
    assert_eq!(second.work.backend_read_operations, 0);
    assert_eq!(second.work.object_bytes_read, 0);
    assert!(second.work.bytes_copied > 0);
    assert_eq!(store.stats()?.decoded_hits, 2);
    Ok(())
}

#[test]
fn warm_diff_borrows_wide_internal_pages_and_copies_only_changed_leaf_values()
-> Result<(), Box<dyn std::error::Error>> {
    let backend = MemoryObjectStore::new(1024 * 1024)?;
    let mut before_children = Vec::new();
    let mut after_children = Vec::new();
    for index in 0_u8..7 {
        let child_name = name(&format!("shared-{index:02}"))?;
        let child = put(
            &backend,
            &TreePage::Leaf(vec![TreeEntry {
                name: child_name.clone(),
                file_id: FileId::from_bytes([index; 16]),
                kind: FileKind::Regular,
            }]),
        )?;
        let descriptor = TreeChild {
            first_name: child_name,
            page: child,
        };
        before_children.push(descriptor.clone());
        after_children.push(descriptor);
    }
    let changed_name = name("z")?;
    let before_leaf = put(&backend, &TreePage::Leaf(vec![entry("z", 90)?]))?;
    let after_leaf = put(&backend, &TreePage::Leaf(vec![entry("z", 91)?]))?;
    before_children.push(TreeChild {
        first_name: changed_name.clone(),
        page: before_leaf,
    });
    after_children.push(TreeChild {
        first_name: changed_name,
        page: after_leaf,
    });
    let before = put(&backend, &TreePage::Internal(before_children))?;
    let after = put(&backend, &TreePage::Internal(after_children))?;
    let store = CachedObjectStore::new(
        backend,
        ObjectCacheOptions {
            maximum_entries: 8,
            maximum_bytes: 1024 * 1024,
            maximum_in_flight: 2,
            maximum_waiters_per_object: 2,
        },
    )?;
    let cancellation = CancellationToken::new();
    let cold = diff(
        &store,
        Some(before),
        Some(after),
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .ok_or("cold wide diff blocked")??;
    assert_eq!(cold.changes.len(), 1);

    let warm = diff(
        &store,
        Some(before),
        Some(after),
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .ok_or("warm wide diff blocked")??;
    assert_eq!(warm.changes, cold.changes);
    assert_eq!(warm.work.backend_read_operations, 0);
    assert_eq!(warm.work.object_bytes_read, 0);
    assert_eq!(warm.work.page_reads, 4);
    let one_leaf_bytes = u64::try_from(size_of::<TreeEntry>())? + 1;
    assert_eq!(warm.work.bytes_copied, one_leaf_bytes * 2);
    assert_eq!(store.stats()?.decoded_hits, 4);
    Ok(())
}
