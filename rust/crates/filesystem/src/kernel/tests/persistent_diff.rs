use crate::{OperationFailure, WorkBudget};
use crate::storage::ObjectStoreError;
use super::*;
use crate::async_storage::AsyncObjectStore;
use crate::foundation::{Digest, FileId};
use crate::kernel::{FileKind, NameEncoding, TreePage, encode_tree_page, tree_page_id};
use crate::kernel::{
    FilePayload, FileRecord, FileTablePage, InlineFileData, encode_file_table_page,
    file_table_page_id,
};
use crate::memory::MemoryObjectStore;
use crate::storage::{ObjectKind, };
use crate::test_support::OwnedReadObjectStore;
use crate::{CachedObjectStore, ObjectCacheOptions};
use acyclic_stream::OwnedBytes;

use crate::kernel::Child;
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

async fn put(store: &MemoryObjectStore, page: &TreePage) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let id = tree_page_id(page, 8)?;
    AsyncObjectStore::put(
        store,
        id,
        OwnedBytes::from(encode_tree_page(page, 8, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?),
        WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await?;
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

async fn put_file_table(
    store: &MemoryObjectStore,
    page: &FileTablePage,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let id = file_table_page_id(page, 8)?;
    AsyncObjectStore::put(
        store,
        id,
        OwnedBytes::from(encode_file_table_page(page, 8, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?),
        WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await?;
    Ok(id)
}

async fn diff<S: AsyncObjectStore>(
    store: &S,
    before: Option<ObjectId>,
    after: Option<ObjectId>,
    maximum_changes: u32,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<Diff<LogicalName, TreeEntry>, OperationFailure<FsError>> {
    (diff_accounted::<_, TreeFormat>(
        store,
        before,
        after,
        maximum_changes,
        DecodeLimits::default(),
        budget,
        cancellation,
    )).await
}

#[tokio::test]
async fn diff_validates_inputs_and_keeps_noop_paths_at_zero_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put(&store, &TreePage::Leaf(vec![entry("a", 1)?])).await?;
    let cancellation = CancellationToken::new();
    for (before, after) in [(None, None), (Some(root), Some(root))] {
        let result = diff(
            &store,
            before,
            after,
            1,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ).await?;
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
        ).await
        .err()
        .ok_or("zero diff limit unexpectedly succeeded")?;
        assert!(matches!(failure.error, FsError::InvalidDiff));
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
    ).await
    .err()
    .ok_or("wrong diff root kind unexpectedly succeeded")?;
    assert!(matches!(failure.error, FsError::InvalidDiff));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn leaf_diff_reports_sorted_add_delete_modify_and_exact_truncation()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let before = put(
        &store,
        &TreePage::Leaf(vec![entry("a", 1)?, entry("b", 2)?]),
    ).await?;
    let after = put(
        &store,
        &TreePage::Leaf(vec![entry("a", 3)?, entry("c", 4)?]),
    ).await?;
    let cancellation = CancellationToken::new();
    let complete = diff(
        &store,
        Some(before),
        Some(after),
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ).await?;
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
    ).await?;
    assert_eq!(bounded.changes.len(), 1);
    assert!(bounded.truncated);

    let one_before = put(&store, &TreePage::Leaf(vec![entry("z", 5)?])).await?;
    let one_after = put(&store, &TreePage::Leaf(vec![entry("z", 6)?])).await?;
    let exact = diff(
        &store,
        Some(one_before),
        Some(one_after),
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ).await?;
    assert_eq!(exact.changes.len(), 1);
    assert!(!exact.truncated);
    Ok(())
}

#[tokio::test]
async fn diff_cancellation_and_budget_fail_before_unadmitted_reads()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let before = put(&store, &TreePage::Leaf(vec![entry("a", 1)?])).await?;
    let after = put(&store, &TreePage::Leaf(vec![entry("a", 2)?])).await?;
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failure = diff(
        &store,
        Some(before),
        Some(after),
        1,
        WorkBudget::UNBOUNDED,
        &cancelled,
    ).await
    .err()
    .ok_or("cancelled diff unexpectedly succeeded")?;
    assert!(matches!(failure.error, FsError::Cancelled(_)));
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
    ).await
    .err()
    .ok_or("unadmitted diff read unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        FsError::Work(WorkError::BudgetExceeded {
            counter: "page_reads",
            observed: 2,
            maximum: 0,
        })
    ));
    assert_eq!(failure.work.page_reads, 0);
    assert_eq!(failure.work.backend_read_operations, 0);
    assert!(failure.work.allocation_operations > 0, "the guarded frontier precedes provider admission");
    Ok(())
}

#[tokio::test]
async fn internal_diff_skips_shared_merkle_children() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let a = put(&store, &TreePage::Leaf(vec![entry("a", 1)?])).await?;
    let c_before = put(&store, &TreePage::Leaf(vec![entry("c", 2)?])).await?;
    let c_after = put(&store, &TreePage::Leaf(vec![entry("c", 3)?])).await?;
    let e = put(&store, &TreePage::Leaf(vec![entry("e", 4)?])).await?;
    let a_name = name("a")?;
    let c_name = name("c")?;
    let e_name = name("e")?;
    let root = |middle| {
        TreePage::Internal(vec![
            Child {
                first: a_name.clone(),
                page: a,
            },
            Child {
                first: c_name.clone(),
                page: middle,
            },
            Child {
                first: e_name.clone(),
                page: e,
            },
        ])
    };
    let before = put(&store, &root(c_before)).await?;
    let after = put(&store, &root(c_after)).await?;
    let cancellation = CancellationToken::new();
    let diff = (diff_accounted::<_, TreeFormat>(
        &store,
        Some(before),
        Some(after),
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(diff.changes.len(), 1);
    assert_eq!(diff.changes[0].key.as_bytes(), b"c");
    assert_eq!(diff.work.page_reads, 4);
    Ok(())
}

#[tokio::test]
async fn internal_file_table_diff_aligns_shared_and_one_sided_children()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let shared = put_file_table(&store, &FileTablePage::Leaf(vec![file_record(1)?])).await?;
    let removed = put_file_table(&store, &FileTablePage::Leaf(vec![file_record(3)?])).await?;
    let added_left = put_file_table(&store, &FileTablePage::Leaf(vec![file_record(2)?])).await?;
    let added_right = put_file_table(&store, &FileTablePage::Leaf(vec![file_record(4)?])).await?;
    let before = put_file_table(
        &store,
        &FileTablePage::Internal(vec![
            Child {
                first: FileId::from_bytes([1; 16]),
                page: shared,
            },
            Child {
                first: FileId::from_bytes([3; 16]),
                page: removed,
            },
        ]),
    ).await?;
    let after = put_file_table(
        &store,
        &FileTablePage::Internal(vec![
            Child {
                first: FileId::from_bytes([1; 16]),
                page: shared,
            },
            Child {
                first: FileId::from_bytes([2; 16]),
                page: added_left,
            },
            Child {
                first: FileId::from_bytes([4; 16]),
                page: added_right,
            },
        ]),
    ).await?;
    let result = (diff_accounted::<_, FileTableFormat>(
        &store,
        Some(before),
        Some(after),
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )).await?;
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

#[tokio::test]
async fn one_sided_and_mixed_shape_diffs_walk_complete_subtrees() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let a = put(&store, &TreePage::Leaf(vec![entry("a", 1)?])).await?;
    let c = put(&store, &TreePage::Leaf(vec![entry("c", 2)?])).await?;
    let internal = put(
        &store,
        &TreePage::Internal(vec![
            Child {
                first: name("a")?,
                page: a,
            },
            Child {
                first: name("c")?,
                page: c,
            },
        ]),
    ).await?;
    let cancellation = CancellationToken::new();
    let added = diff(
        &store,
        None,
        Some(internal),
        2,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ).await?;
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
    ).await?;
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
    ).await?;
    let mixed = diff(
        &store,
        Some(internal),
        Some(flattened),
        2,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ).await?;
    assert!(mixed.changes.is_empty());
    assert!(!mixed.truncated);

    let one_child = put(
        &store,
        &TreePage::Internal(vec![Child {
            first: name("a")?,
            page: flattened,
        }]),
    ).await?;
    let realigned = diff(
        &store,
        Some(internal),
        Some(one_child),
        2,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ).await?;
    assert!(realigned.changes.is_empty());
    assert!(!realigned.truncated);
    Ok(())
}

#[tokio::test]
async fn owned_one_sided_internal_diff_stops_at_the_exact_outer_bound()
-> Result<(), Box<dyn std::error::Error>> {
    let store = OwnedReadObjectStore::default();
    let first = put(&store.inner, &TreePage::Leaf(vec![entry("a", 1)?])).await?;
    let second = put(&store.inner, &TreePage::Leaf(vec![entry("z", 2)?])).await?;
    let root = put(
        &store.inner,
        &TreePage::Internal(vec![
            Child {
                first: name("a")?,
                page: first,
            },
            Child {
                first: name("z")?,
                page: second,
            },
        ]),
    ).await?;
    let result = diff(
        &store,
        None,
        Some(root),
        1,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ).await?;
    assert_eq!(result.changes.len(), 1);
    assert!(result.truncated);
    assert!(result.work.bytes_copied > 0);
    Ok(())
}

#[tokio::test]
async fn diff_preserves_storage_and_decode_failures_after_admitted_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let valid = put(&store, &TreePage::Leaf(vec![entry("a", 1)?])).await?;
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
    ).await
    .err()
    .ok_or("missing page unexpectedly diffed")?;
    assert!(matches!(
        failure.error,
        FsError::Object(ObjectStoreError::Missing)
    ));
    assert_eq!(failure.work.page_reads, 2);
    assert!(failure.work.backend_read_operations >= 1);

    let malformed_bytes = OwnedBytes::from_static(b"not a canonical tree page");
    let malformed = ObjectId {
        kind: ObjectKind::TreePage,
        digest: crate::storage::object_digest(ObjectKind::TreePage, &malformed_bytes),
    };
    AsyncObjectStore::put(&store, malformed, malformed_bytes, WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await?;
    let failure = diff(
        &store,
        Some(valid),
        Some(malformed),
        2,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ).await
    .err()
    .ok_or("malformed page unexpectedly diffed")?;
    assert!(matches!(failure.error, FsError::Decode(_)));
    assert!(failure.work.page_reads >= 2);
    Ok(())
}

#[tokio::test]
async fn warm_diff_borrows_wide_internal_pages_and_copies_only_changed_leaf_values()
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
        ).await?;
        let descriptor = Child {
            first: child_name,
            page: child,
        };
        before_children.push(descriptor.clone());
        after_children.push(descriptor);
    }
    let changed_name = name("z")?;
    let before_leaf = put(&backend, &TreePage::Leaf(vec![entry("z", 90)?])).await?;
    let after_leaf = put(&backend, &TreePage::Leaf(vec![entry("z", 91)?])).await?;
    before_children.push(Child {
        first: changed_name.clone(),
        page: before_leaf,
    });
    after_children.push(Child {
        first: changed_name,
        page: after_leaf,
    });
    let before = put(&backend, &TreePage::Internal(before_children)).await?;
    let after = put(&backend, &TreePage::Internal(after_children)).await?;
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
    ).await?;
    assert_eq!(cold.changes.len(), 1);

    let warm = diff(
        &store,
        Some(before),
        Some(after),
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ).await?;
    assert_eq!(warm.changes, cold.changes);
    assert_eq!(warm.work.backend_read_operations, 0);
    assert_eq!(warm.work.object_bytes_read, 0);
    assert_eq!(warm.work.page_reads, 4);
    // Only the two result names and their sorted-map routing key are copied.
    assert_eq!(warm.work.bytes_copied, 3);
    assert_eq!(store.stats()?.decoded_hits, 4);
    Ok(())
}

mod model {
    use super::{diff, entry, name, put};
    use crate::cancellation::CancellationToken;
    use crate::foundation::FileId;
    use crate::kernel::persistent_pagination::paginate_async;
    use crate::kernel::tree_mutation::{TreeFormat, TreeMutation, apply_tree_mutations};
    use crate::kernel::{DecodeLimits, TreeEntry, TreePage};
    use crate::memory::MemoryObjectStore;
    use crate::performance::WorkBudget;
    use crate::storage::ObjectId;
    use proptest::prelude::*;
    use std::collections::{BTreeMap, BTreeSet};

    type Model = BTreeMap<u8, u8>;
    type Outcome = Result<(), Box<dyn std::error::Error>>;

    const LIMITS: DecodeLimits = DecodeLimits {
        maximum_object_bytes: 64 * 1024,
        maximum_name_bytes: 255,
        maximum_page_items: 4,
        maximum_page_bytes: 64 * 1024,
        maximum_page_height: 16,
        maximum_visited_pages: 4_096,
    };

    fn key(byte: u8) -> String {
        format!("k{byte:03}")
    }

    fn entries(model: &Model) -> Result<Vec<TreeEntry>, Box<dyn std::error::Error>> {
        model
            .iter()
            .map(|(byte, id)| entry(&key(*byte), *id))
            .collect()
    }

    async fn rewrite(
        store: &MemoryObjectStore,
        root: ObjectId,
        from: &Model,
        to: &Model,
    ) -> Result<ObjectId, Box<dyn std::error::Error>> {
        let mut mutations = Vec::new();
        for byte in from.keys().chain(to.keys()).collect::<BTreeSet<_>>() {
            mutations.push(match (from.get(byte), to.get(byte)) {
                (None, Some(id)) => TreeMutation::Insert(entry(&key(*byte), *id)?),
                (Some(_), None) => TreeMutation::Remove {
                    name: name(&key(*byte))?,
                    expected_file_id: None,
                },
                (Some(old), Some(id)) if old != id => TreeMutation::Replace {
                    entry: entry(&key(*byte), *id)?,
                    expected_file_id: FileId::from_bytes([*old; 16]),
                },
                _ => continue,
            });
        }
        if mutations.is_empty() {
            return Ok(root);
        }
        let receipt = apply_tree_mutations(
            store, root, mutations, 256, LIMITS, WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        ).await?;
        Ok(receipt.value)
    }

    async fn paginate_all(
        store: &MemoryObjectStore,
        root: ObjectId,
        page: u32,
    ) -> Result<Vec<TreeEntry>, Box<dyn std::error::Error>> {
        let mut values = Vec::new();
        let mut after = None;
        for _ in 0..=256 {
            let receipt = paginate_async::<_, TreeFormat>(
                store,
                root,
                after.as_ref(),
                page,
                LIMITS,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
                None,
            ).await?;
            if receipt.values.len() > usize::try_from(page)? {
                return Err("page exceeded its bound".into());
            }
            after = receipt.values.last().map(|entry| entry.name.clone());
            values.extend(receipt.values);
            if !receipt.has_more {
                return Ok(values);
            }
        }
        Err("pagination did not terminate".into())
    }

    async fn check(before: &Model, after: &Model, page: u32) -> Outcome {
        let store = MemoryObjectStore::new(1 << 20)?;
        let empty = put(&store, &TreePage::Leaf(Vec::new())).await?;
        let old = rewrite(&store, empty, &Model::new(), before).await?;
        let new = rewrite(&store, old, before, after).await?;
        assert_eq!(paginate_all(&store, old, page).await?, entries(before)?);
        assert_eq!(paginate_all(&store, new, page).await?, entries(after)?);

        let changes = diff(
            &store,
            Some(old),
            Some(new),
            1_024,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        ).await?
        .changes
        .into_iter()
        .map(|change| (change.key, change.before, change.after))
        .collect::<Vec<_>>();
        let mut expected = Vec::new();
        for byte in before.keys().chain(after.keys()).collect::<BTreeSet<_>>() {
            let side = |model: &Model| {
                model
                    .get(byte)
                    .map(|id| entry(&key(*byte), *id))
                    .transpose()
            };
            let (old, new) = (side(before)?, side(after)?);
            if old != new {
                expected.push((name(&key(*byte))?, old, new));
            }
        }
        assert_eq!(changes, expected);
        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(48))]

        #[test]
        fn pagination_and_diff_match_a_sorted_map(
            before in prop::collection::btree_map(0_u8..48, 1_u8..4, 0..32),
            after in prop::collection::btree_map(0_u8..48, 1_u8..4, 0..32),
            page in 1_u32..6,
        ) {
            tokio::runtime::Builder::new_current_thread().build()
                .map_err(|error| TestCaseError::fail(error.to_string()))?
                .block_on(check(&before, &after, page))
                .map_err(|error| TestCaseError::fail(error.to_string()))?;
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_diff_denies_frontier_allocation_before_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put(&store, &TreePage::Leaf(vec![entry("alpha", 1)?])).await?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        WorkBudget { allocation_operations: 1, ..WorkBudget::UNBOUNDED }, &cancellation,
    ).map_err(|(error, _)| error)?;
    let before = original.receipt();
    let baseline = original.allocations(|ledger, _, _, _| Ok::<_, FsError>(ledger.live_bytes())).map(|(value, _allocation)| value)?;
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let result = diff_tree_entries_with_accounting(
        &store, None, Some(root), 1, DecodeLimits::default(), &mut accounting, &cancellation,
    ).await;
    assert!(matches!(result, Err(FsError::Work(WorkError::BudgetExceeded { counter: "allocation_operations", .. }))));
    assert_eq!(original.receipt(), before);
    assert_eq!(original.receipt().backend_read_operations, 0);
    assert_eq!(original.allocations(|ledger, _, _, _| Ok::<_, FsError>(ledger.live_bytes())).map(|(value, _allocation)| value)?, baseline);
    Ok(())
}

async fn diff_accounted<S: AsyncObjectStore, F: Format>(
    store: &S, before: Option<ObjectId>, after: Option<ObjectId>, maximum_changes: u32,
    limits: DecodeLimits, budget: WorkBudget, cancellation: &CancellationToken,
) -> Result<Diff<F::Key, F::Value>, OperationFailure<FsError>> {
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    let result = diff_with_accounting::<S, F>(store, before, after, maximum_changes, limits, &mut accounting, cancellation).await;
    let work = accounting.spent();
    result.map(|(mut diff, _)| { diff.work = work; diff })
        .map_err(|error| OperationFailure::new(error, work))
}
