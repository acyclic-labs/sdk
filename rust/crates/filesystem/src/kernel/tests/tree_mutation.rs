use crate::async_storage::AsyncObjectStore;
use crate::kernel::decode_tree_page;
use super::*;
use crate::kernel::{FileKind, NameEncoding, TreePage, encode_tree_page, tree_page_id};
use crate::memory::MemoryObjectStore;

use acyclic_stream::OwnedBytes;
use std::collections::BTreeMap;

use crate::kernel::Child;
#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_mutation_refuses_index_copy_before_allocating_or_reading()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let original = entry("a", 1, FileKind::Regular)?;
    let root = put(&store, &TreePage::Leaf(vec![original.clone()]), 8).await?;
    let mutations = [TreeMutation::Replace { expected_file_id: original.file_id, entry: original }];
    let cancellation = crate::CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(
        WorkBudget { bytes_copied: 0, ..WorkBudget::UNBOUNDED }, &cancellation,
    ).map_err(|(error, _)| error)?;
    let before = work.receipt();
    let live = work.live_allocation_bytes();
    let mut accounting = super::super::allocation::KernelAccounting::native(&work, &cancellation);
    let error = apply_tree_mutations_with_accounting(
        &store, root, &mutations, 1, DecodeLimits::default(), &mut accounting, &cancellation,
    ).await.expect_err("index copy must be admitted before allocating");
    assert!(matches!(error, crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(_)) | crate::FsError::Work(_)));
    assert_eq!(work.receipt(), before);
    assert_eq!(work.live_allocation_bytes(), live);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_mutation_write_denial_preserves_paid_orphan_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, _) = three_leaf_tree(&store).await?;
    let first = entry("a", 1, FileKind::Fifo)?;
    let first_page = tree_page_id(&TreePage::Leaf(vec![first.clone()]), 8)?;
    let mutations = [
        TreeMutation::Replace { entry: first, expected_file_id: FileId::from_bytes([1; 16]) },
        TreeMutation::Replace { entry: entry("x", 4, FileKind::Fifo)?, expected_file_id: FileId::from_bytes([4; 16]) },
    ];
    let cancellation = crate::CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(
        WorkBudget { page_writes: 1, ..WorkBudget::UNBOUNDED }, &cancellation,
    ).map_err(|(error, _)| error)?;
    let mut accounting = super::super::allocation::KernelAccounting::native(&work, &cancellation);
    let error = apply_tree_mutations_with_accounting(
        &store, root, &mutations, 2, DecodeLimits::default(), &mut accounting, &cancellation,
    ).await.expect_err("second changed leaf must refuse on original page-write limit");
    assert!(matches!(error, crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(_)) | crate::FsError::Work(_)));
    assert_eq!(work.receipt().page_writes, 1);
    assert_eq!(work.receipt().backend_write_operations, 1);
    assert!(store.read(first_page, u64::MAX, WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.is_ok());
    let first_page_bytes = encode_tree_page(&TreePage::Leaf(vec![entry("a", 1, FileKind::Fifo)?]), 8, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?;
    assert_eq!(work.receipt().bytes_encoded, first_page_bytes.len() as u64);
    drop(accounting);
    drop(store);
    assert_eq!(work.live_allocation_bytes(), crate::NativeMaterializationWork::accounting_state_bytes());
    Ok(())
}
fn name(value: &str) -> Result<LogicalName, Box<dyn std::error::Error>> {
    Ok(LogicalName::new(
        NameEncoding::Utf8,
        value.as_bytes().to_vec(),
        255,
    )?)
}

fn entry(value: &str, byte: u8, kind: FileKind) -> Result<TreeEntry, Box<dyn std::error::Error>> {
    Ok(TreeEntry {
        name: name(value)?,
        file_id: FileId::from_bytes([byte; 16]),
        kind,
    })
}

async fn put(
    store: &MemoryObjectStore,
    page: &TreePage,
    maximum_items: u32,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let id = tree_page_id(page, maximum_items)?;
    store.put(
        id,
        OwnedBytes::from(encode_tree_page(page, maximum_items, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?),
        WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await?;
    Ok(id)
}

async fn three_leaf_tree(
    store: &MemoryObjectStore,
) -> Result<(ObjectId, [ObjectId; 3]), Box<dyn std::error::Error>> {
    let leaves = [
        put(
            store,
            &TreePage::Leaf(vec![entry("a", 1, FileKind::Regular)?]),
            8,
        ).await?,
        put(
            store,
            &TreePage::Leaf(vec![
                entry("m", 2, FileKind::Regular)?,
                entry("n", 3, FileKind::Regular)?,
            ]),
            8,
        ).await?,
        put(
            store,
            &TreePage::Leaf(vec![entry("x", 4, FileKind::Regular)?]),
            8,
        ).await?,
    ];
    let root = put(
        store,
        &TreePage::Internal(vec![
            Child {
                first: name("a")?,
                page: leaves[0],
            },
            Child {
                first: name("m")?,
                page: leaves[1],
            },
            Child {
                first: name("x")?,
                page: leaves[2],
            },
        ]),
        8,
    ).await?;
    Ok((root, leaves))
}

#[tokio::test]
async fn touched_leaf_and_shared_root_are_rewritten_once() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, leaves) = three_leaf_tree(&store).await?;
    let receipt = apply_tree_mutations(
        &store,
        root,
        vec![
            TreeMutation::Replace {
                entry: entry("m", 2, FileKind::Fifo)?,
                expected_file_id: FileId::from_bytes([2; 16]),
            },
            TreeMutation::Replace {
                entry: entry("n", 3, FileKind::Fifo)?,
                expected_file_id: FileId::from_bytes([3; 16]),
            },
        ],
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    ).await?;
    assert_eq!(receipt.work.page_reads, 2);
    assert_eq!(receipt.work.page_writes, 2);
    let root_bytes = store
        .read(receipt.value, u64::MAX, WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await?
        .value;
    let TreePage::Internal(children) = decode_tree_page(&root_bytes, DecodeLimits::default(), &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?
    else {
        return Err("candidate root is not internal".into());
    };
    assert_eq!(children[0].page, leaves[0]);
    assert_ne!(children[1].page, leaves[1]);
    assert_eq!(children[2].page, leaves[2]);
    Ok(())
}

#[tokio::test]
async fn semantic_noop_preserves_root_and_writes_nothing() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, _) = three_leaf_tree(&store).await?;
    let receipt = apply_tree_mutations(
        &store,
        root,
        vec![TreeMutation::Replace {
            entry: entry("m", 2, FileKind::Regular)?,
            expected_file_id: FileId::from_bytes([2; 16]),
        }],
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    ).await?;
    assert_eq!(receipt.value, root);
    assert_eq!(receipt.work.page_reads, 2);
    assert_eq!(receipt.work.page_writes, 0);
    assert_eq!(receipt.work.backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn overflowing_leaf_splits_and_builds_one_new_root() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put(
        &store,
        &TreePage::Leaf(vec![
            entry("a", 1, FileKind::Regular)?,
            entry("c", 3, FileKind::Regular)?,
        ]),
        2,
    ).await?;
    let limits = DecodeLimits {
        maximum_page_items: 2,
        ..DecodeLimits::default()
    };
    let receipt = apply_tree_mutations(
        &store,
        root,
        vec![TreeMutation::Insert(entry("b", 2, FileKind::Regular)?)],
        2,
        limits,
        WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    ).await?;
    assert_eq!(receipt.work.page_reads, 1);
    assert_eq!(receipt.work.page_writes, 3);
    let bytes = store
        .read(receipt.value, u64::MAX, WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await?
        .value;
    assert!(matches!(
        decode_tree_page(&bytes, limits, &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?,
        TreePage::Internal(children) if children.len() == 2
    ));
    Ok(())
}

#[tokio::test]
async fn failed_identity_precondition_writes_no_objects() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, _) = three_leaf_tree(&store).await?;
    let failure = apply_tree_mutations(
        &store,
        root,
        vec![TreeMutation::Remove {
            name: name("m")?,
            expected_file_id: Some(FileId::from_bytes([9; 16])),
        }],
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("identity conflict unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        TreeMutationError::FileIdentityConflict
    ));
    assert_eq!(failure.work.page_reads, 2);
    assert_eq!(failure.work.backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn every_tree_semantic_failure_is_exact_and_non_mutating() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let existing = entry("m", 2, FileKind::Regular)?;
    let root = put(&store, &TreePage::Leaf(vec![existing.clone()]), 8).await?;
    let missing = name("z")?;
    let wrong_identity = FileId::from_bytes([9; 16]);
    let cases = [
        (TreeMutation::Insert(existing.clone()), "already-exists"),
        (
            TreeMutation::Remove {
                name: missing.clone(),
                expected_file_id: None,
            },
            "missing",
        ),
        (
            TreeMutation::Replace {
                entry: TreeEntry {
                    name: missing,
                    file_id: wrong_identity,
                    kind: FileKind::Regular,
                },
                expected_file_id: wrong_identity,
            },
            "missing",
        ),
        (
            TreeMutation::Replace {
                entry: existing,
                expected_file_id: wrong_identity,
            },
            "identity",
        ),
    ];
    for (mutation, expected) in cases {
        let failure = apply_tree_mutations(
            &store,
            root,
            vec![mutation],
            8,
            DecodeLimits::default(),
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("invalid tree mutation unexpectedly succeeded")?;
        assert!(match expected {
            "already-exists" => matches!(failure.error, TreeMutationError::AlreadyExists),
            "missing" => matches!(failure.error, TreeMutationError::Missing),
            "identity" => matches!(failure.error, TreeMutationError::FileIdentityConflict),
            _ => false,
        });
        assert_eq!(failure.work.backend_write_operations, 0);
    }
    Ok(())
}

fn wide_entry(value: u16, kind: FileKind) -> Result<TreeEntry, Box<dyn std::error::Error>> {
    entry(&format!("wide-{value:04}"), value.to_le_bytes()[0], kind)
}

#[tokio::test]
async fn point_noop_uses_logarithmic_leaf_search_without_rebuild()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let entries = (0..1_024_u16)
        .map(|value| wide_entry(value, FileKind::Regular))
        .collect::<Result<Vec<_>, _>>()?;
    let root = put(&store, &TreePage::Leaf(entries), 1_024).await?;
    let replacement = wide_entry(777, FileKind::Regular)?;
    let receipt = apply_tree_mutations(
        &store,
        root,
        vec![TreeMutation::Replace {
            expected_file_id: replacement.file_id,
            entry: replacement,
        }],
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    ).await?;
    // The allocation-free canonical shape pass validates all 1,024 bounded
    // records before allocation; the point lookup itself adds at most 11
    // binary-search comparisons.
    assert!(receipt.work.items_examined <= 1_024 + 11);
    assert_eq!(receipt.value, root);
    assert_eq!(receipt.work.page_writes, 0);
    Ok(())
}

#[tokio::test]
async fn visited_capacity_is_admitted_before_tree_access() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let existing = wide_entry(1, FileKind::Regular)?;
    let root = put(&store, &TreePage::Leaf(vec![existing.clone()]), 8).await?;
    let mutation = TreeMutation::Replace {
        entry: existing.clone(),
        expected_file_id: existing.file_id,
    };
    let failure = apply_tree_mutations(
        &store,
        root,
        vec![mutation],
        1,
        DecodeLimits::default(),
        WorkBudget {
            peak_allocation_bytes: 0,
            ..WorkBudget::UNBOUNDED
        },
        &crate::CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("underbudgeted visited set unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        TreeMutationError::Work(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            observed,
            maximum: observed_maximum,
        }) if observed > 0 && observed_maximum == 0
    ));
    assert_eq!(failure.work.backend_read_operations, 0);
    assert_eq!(failure.work.backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn decoded_page_allocation_is_admitted_after_shape_read_before_decode()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let existing = wide_entry(1, FileKind::Regular)?;
    let root = put(&store, &TreePage::Leaf(vec![existing.clone()]), 8).await?;
    let mutation = TreeMutation::Replace {
        entry: existing.clone(),
        expected_file_id: existing.file_id,
    };
    let baseline = apply_tree_mutations(
        &store,
        root,
        vec![mutation.clone()],
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    ).await?;
    let maximum = baseline.work.peak_allocation_bytes - 1;
    let failure = apply_tree_mutations(
        &store,
        root,
        vec![mutation],
        1,
        DecodeLimits::default(),
        WorkBudget {
            peak_allocation_bytes: maximum,
            ..WorkBudget::UNBOUNDED
        },
        &crate::CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("underbudgeted decoded page unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        TreeMutationError::Work(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            observed,
            maximum: observed_maximum,
        }) if observed == baseline.work.peak_allocation_bytes && observed_maximum == maximum
    ));
    assert_eq!(failure.work.backend_read_operations, 1);
    assert_eq!(failure.work.backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn structural_batch_rebuilds_wide_leaf_in_linear_work() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let initial = (0..1_024_u16)
        .step_by(2)
        .map(|value| wide_entry(value, FileKind::Regular))
        .collect::<Result<Vec<_>, _>>()?;
    let root = put(&store, &TreePage::Leaf(initial), 1_024).await?;
    let mutations = (1..1_024_u16)
        .step_by(2)
        .map(|value| wide_entry(value, FileKind::Regular).map(TreeMutation::Insert))
        .collect::<Result<Vec<_>, _>>()?;
    let receipt = apply_tree_mutations(
        &store,
        root,
        mutations,
        512,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    ).await?;
    // Merge, balanced page sizing, and page cutting each visit an item once.
    assert!(
        receipt.work.items_examined <= 5 * 1_024,
        "linear structural plan examined {} items",
        receipt.work.items_examined
    );
    assert_eq!(flatten_tree(&store, receipt.value).await?.len(), 1_024);
    Ok(())
}

#[tokio::test]
async fn unordered_batch_is_deterministic_and_preserves_equal_name_order()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put(&store, &TreePage::Leaf(Vec::new()), 8).await?;
    let first_b = entry("b", 1, FileKind::Regular)?;
    let a = entry("a", 2, FileKind::Regular)?;
    let receipt = apply_tree_mutations(
        &store,
        root,
        vec![
            TreeMutation::Insert(first_b.clone()),
            TreeMutation::Insert(a.clone()),
            TreeMutation::Remove {
                name: first_b.name,
                expected_file_id: Some(first_b.file_id),
            },
        ],
        3,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    ).await?;
    assert_eq!(flatten_tree(&store, receipt.value).await?, vec![a]);
    Ok(())
}

#[tokio::test]
async fn unordered_sort_work_is_admitted_before_tree_access() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put(&store, &TreePage::Leaf(Vec::new()), 8).await?;
    let failure = apply_tree_mutations(
        &store,
        root,
        vec![
            TreeMutation::Insert(entry("b", 1, FileKind::Regular)?),
            TreeMutation::Insert(entry("a", 2, FileKind::Regular)?),
        ],
        2,
        DecodeLimits::default(),
        WorkBudget {
            items_examined: 0,
            ..WorkBudget::UNBOUNDED
        },
        &crate::CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("underbudgeted unordered batch unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        TreeMutationError::Work(WorkError::BudgetExceeded {
            counter: "items_examined",
            observed: 1,
            maximum: 0,
        })
    ));
    assert_eq!(failure.work.backend_read_operations, 0);
    assert_eq!(failure.work.backend_write_operations, 0);
    Ok(())
}

fn numbered_entry(value: u8, kind: FileKind) -> Result<TreeEntry, Box<dyn std::error::Error>> {
    entry(&format!("n{value:03}"), value.saturating_add(1), kind)
}

async fn build_tree(
    store: &MemoryObjectStore,
    entries: &[TreeEntry],
    width: usize,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    if entries.is_empty() {
        return put(store, &TreePage::Leaf(Vec::new()), u32::try_from(width)?).await;
    }
    let mut level = Vec::new();
    for chunk in entries.chunks(width) {
        level.push((
            chunk[0].name.clone(),
            put(
                store,
                &TreePage::Leaf(chunk.to_vec()),
                u32::try_from(width)?,
            ).await?,
        ));
    }
    while level.len() > 1 {
        let mut parent = Vec::new();
        for chunk in level.chunks(width) {
            let children = chunk
                .iter()
                .map(|(first, page)| Child {
                    first: first.clone(),
                    page: *page,
                })
                .collect();
            parent.push((
                chunk[0].0.clone(),
                put(store, &TreePage::Internal(children), u32::try_from(width)?).await?,
            ));
        }
        level = parent;
    }
    Ok(level[0].1)
}

async fn flatten_tree(
    store: &MemoryObjectStore,
    root: ObjectId,
) -> Result<Vec<TreeEntry>, Box<dyn std::error::Error>> {
    let bytes = store.read(root, u64::MAX, WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await?.value;
    Ok(match decode_tree_page(&bytes, DecodeLimits::default(), &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))? {
        TreePage::Leaf(entries) => entries,
        TreePage::Internal(children) => {
            let mut entries = Vec::new();
            for child in children {
                entries.extend(Box::pin(flatten_tree(store, child.page).await).await?);
            }
            entries
        }
    })
}

fn next(seed: &mut u64) -> u64 {
    *seed = seed
        .wrapping_mul(2_862_933_555_777_941_757)
        .wrapping_add(3_037_000_493);
    *seed
}

#[tokio::test]
async fn generated_directory_histories_match_independent_ordered_map()
-> Result<(), Box<dyn std::error::Error>> {
    for history in 0..256_u64 {
        let store = MemoryObjectStore::default();
        let mut seed = history.saturating_add(1);
        let mut expected = BTreeMap::new();
        for value in 0..24_u8 {
            if next(&mut seed).is_multiple_of(3) {
                expected.insert(value, numbered_entry(value, FileKind::Regular)?);
            }
        }
        let initial: Vec<_> = expected.values().cloned().collect();
        let root = build_tree(&store, &initial, 4).await?;
        let count = usize::try_from(next(&mut seed) % 16 + 1)?;
        let mut mutations = Vec::with_capacity(count);
        for _ in 0..count {
            let selected = u8::try_from(next(&mut seed) % 24)?;
            if let Some(existing) = expected.get(&selected).cloned() {
                if next(&mut seed).is_multiple_of(2) {
                    mutations.push(TreeMutation::Remove {
                        name: existing.name.clone(),
                        expected_file_id: Some(existing.file_id),
                    });
                    expected.remove(&selected);
                } else {
                    let replacement = numbered_entry(selected, FileKind::Fifo)?;
                    mutations.push(TreeMutation::Replace {
                        entry: replacement.clone(),
                        expected_file_id: existing.file_id,
                    });
                    expected.insert(selected, replacement);
                }
            } else {
                let inserted = numbered_entry(selected, FileKind::Regular)?;
                mutations.push(TreeMutation::Insert(inserted.clone()));
                expected.insert(selected, inserted);
            }
        }
        let receipt = apply_tree_mutations(
            &store,
            root,
            mutations,
            16,
            DecodeLimits {
                maximum_page_items: 4,
                ..DecodeLimits::default()
            },
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        ).await?;
        assert_eq!(
            flatten_tree(&store, receipt.value).await?,
            expected.into_values().collect::<Vec<_>>(),
            "history {history} diverged"
        );
    }
    Ok(())
}

#[tokio::test]
async fn point_inserts_rewrite_bounded_pages_however_large_the_directory()
-> Result<(), Box<dyn std::error::Error>> {
    const ENTRIES: u32 = 3_000;
    let store = MemoryObjectStore::default();
    let mut root = put(&store, &TreePage::Leaf(Vec::new()), 1_024).await?;
    let mut largest_late_rewrite = 0;
    for step in 0..ENTRIES {
        // A fixed permutation spreads inserts across every leaf.
        let ordinal = step * 7_919 % ENTRIES;
        let receipt = apply_tree_mutations(
            &store,
            root,
            vec![TreeMutation::Insert(TreeEntry {
                name: name(&format!("entry-{ordinal:06}"))?,
                file_id: FileId::from_bytes([u8::try_from(ordinal % 251)?; 16]),
                kind: FileKind::Regular,
            })],
            1,
            DecodeLimits::default(),
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        ).await?;
        root = receipt.value;
        if step >= ENTRIES / 2 {
            largest_late_rewrite = largest_late_rewrite.max(receipt.work.bytes_encoded);
        }
    }
    // Only the touched leaf and its ancestors are re-encoded, each near the
    // soft target, however many entries the directory already holds.
    let page_bound = u64::try_from(crate::kernel::persistent_btree::TARGET_PAGE_BYTES + 512)?;
    assert!(
        largest_late_rewrite <= 3 * page_bound,
        "one insert re-encoded {largest_late_rewrite} bytes"
    );
    let listing = crate::kernel::list_tree_entries(
        &store,
        root,
        None,
        ENTRIES,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    ).await?;
    let names = listing
        .entries
        .iter()
        .map(|entry| entry.name.as_bytes().to_vec())
        .collect::<Vec<_>>();
    let expected = (0..ENTRIES)
        .map(|ordinal| format!("entry-{ordinal:06}").into_bytes())
        .collect::<Vec<_>>();
    assert_eq!(names, expected);
    Ok(())
}
