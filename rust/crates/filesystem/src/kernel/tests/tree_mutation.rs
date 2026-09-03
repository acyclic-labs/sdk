use super::*;
use crate::kernel::{FileKind, NameEncoding, TreeChild, encode_tree_page, tree_page_id};
use crate::memory::MemoryObjectStore;
use crate::storage::ObjectStore;
use bytes::Bytes;
use std::collections::BTreeMap;

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

fn put(
    store: &MemoryObjectStore,
    page: &TreePage,
    maximum_items: u32,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let id = tree_page_id(page, maximum_items)?;
    store.put(
        id,
        Bytes::from(encode_tree_page(page, maximum_items)?),
        WorkBudget::UNBOUNDED,
    )?;
    Ok(id)
}

fn three_leaf_tree(
    store: &MemoryObjectStore,
) -> Result<(ObjectId, [ObjectId; 3]), Box<dyn std::error::Error>> {
    let leaves = [
        put(
            store,
            &TreePage::Leaf(vec![entry("a", 1, FileKind::Regular)?]),
            8,
        )?,
        put(
            store,
            &TreePage::Leaf(vec![
                entry("m", 2, FileKind::Regular)?,
                entry("n", 3, FileKind::Regular)?,
            ]),
            8,
        )?,
        put(
            store,
            &TreePage::Leaf(vec![entry("x", 4, FileKind::Regular)?]),
            8,
        )?,
    ];
    let root = put(
        store,
        &TreePage::Internal(vec![
            TreeChild {
                first_name: name("a")?,
                page: leaves[0],
            },
            TreeChild {
                first_name: name("m")?,
                page: leaves[1],
            },
            TreeChild {
                first_name: name("x")?,
                page: leaves[2],
            },
        ]),
        8,
    )?;
    Ok((root, leaves))
}

#[test]
fn touched_leaf_and_shared_root_are_rewritten_once() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, leaves) = three_leaf_tree(&store)?;
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
    )?;
    assert_eq!(receipt.work.page_reads, 2);
    assert_eq!(receipt.work.page_writes, 2);
    let root_bytes = store
        .read(receipt.root, u64::MAX, WorkBudget::UNBOUNDED)?
        .value;
    let TreePage::Internal(children) = decode_tree_page(&root_bytes, DecodeLimits::default())?
    else {
        return Err("candidate root is not internal".into());
    };
    assert_eq!(children[0].page, leaves[0]);
    assert_ne!(children[1].page, leaves[1]);
    assert_eq!(children[2].page, leaves[2]);
    Ok(())
}

#[test]
fn semantic_noop_preserves_root_and_writes_nothing() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, _) = three_leaf_tree(&store)?;
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
    )?;
    assert_eq!(receipt.root, root);
    assert_eq!(receipt.work.page_reads, 2);
    assert_eq!(receipt.work.page_writes, 0);
    assert_eq!(receipt.work.backend_write_operations, 0);
    Ok(())
}

#[test]
fn async_and_sync_tree_mutation_share_exact_results_and_work()
-> Result<(), Box<dyn std::error::Error>> {
    let sync_store = MemoryObjectStore::default();
    let async_store = MemoryObjectStore::default();
    let (sync_root, _) = three_leaf_tree(&sync_store)?;
    let (async_root, _) = three_leaf_tree(&async_store)?;
    assert_eq!(sync_root, async_root);
    let mutations = vec![TreeMutation::Replace {
        entry: entry("m", 2, FileKind::Fifo)?,
        expected_file_id: FileId::from_bytes([2; 16]),
    }];
    let synchronous = apply_tree_mutations(
        &sync_store,
        sync_root,
        mutations.clone(),
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    let asynchronous = crate::async_storage::poll_ready(apply_tree_mutations_async(
        &async_store,
        async_root,
        mutations,
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    ))
    .ok_or("memory-backed asynchronous tree mutation unexpectedly blocked")??;
    assert_eq!(asynchronous, synchronous);
    Ok(())
}

#[test]
fn overflowing_leaf_splits_and_builds_one_new_root() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put(
        &store,
        &TreePage::Leaf(vec![
            entry("a", 1, FileKind::Regular)?,
            entry("c", 3, FileKind::Regular)?,
        ]),
        2,
    )?;
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
    )?;
    assert_eq!(receipt.work.page_reads, 1);
    assert_eq!(receipt.work.page_writes, 3);
    let bytes = store
        .read(receipt.root, u64::MAX, WorkBudget::UNBOUNDED)?
        .value;
    assert!(matches!(
        decode_tree_page(&bytes, limits)?,
        TreePage::Internal(children) if children.len() == 2
    ));
    Ok(())
}

#[test]
fn failed_identity_precondition_writes_no_objects() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (root, _) = three_leaf_tree(&store)?;
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
    )
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

#[test]
fn every_tree_semantic_failure_is_exact_and_non_mutating() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let existing = entry("m", 2, FileKind::Regular)?;
    let root = put(&store, &TreePage::Leaf(vec![existing.clone()]), 8)?;
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
        )
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
    assert!(matches!(
        TreeMutationError::from(TreeSemanticError::AlreadyExists),
        TreeMutationError::AlreadyExists
    ));
    assert!(matches!(
        TreeMutationError::from(TreeSemanticError::FileIdentityConflict),
        TreeMutationError::FileIdentityConflict
    ));
    Ok(())
}

fn wide_entry(value: u16, kind: FileKind) -> Result<TreeEntry, Box<dyn std::error::Error>> {
    entry(&format!("wide-{value:04}"), value.to_le_bytes()[0], kind)
}

#[test]
fn point_noop_uses_logarithmic_leaf_search_without_rebuild()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let entries = (0..1_024_u16)
        .map(|value| wide_entry(value, FileKind::Regular))
        .collect::<Result<Vec<_>, _>>()?;
    let root = put(&store, &TreePage::Leaf(entries), 1_024)?;
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
    )?;
    // The allocation-free canonical shape pass validates all 1,024 bounded
    // records before allocation; the point lookup itself adds at most 11
    // binary-search comparisons.
    assert!(receipt.work.items_examined <= 1_024 + 11);
    assert_eq!(receipt.root, root);
    assert_eq!(receipt.work.page_writes, 0);
    Ok(())
}

#[test]
fn visited_capacity_is_admitted_before_tree_access() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let existing = wide_entry(1, FileKind::Regular)?;
    let root = put(&store, &TreePage::Leaf(vec![existing.clone()]), 8)?;
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
    )
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

#[test]
fn decoded_page_allocation_is_admitted_after_shape_read_before_decode()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let existing = wide_entry(1, FileKind::Regular)?;
    let root = put(&store, &TreePage::Leaf(vec![existing.clone()]), 8)?;
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
    )?;
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
    )
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

#[test]
fn structural_batch_rebuilds_wide_leaf_in_linear_work() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let initial = (0..1_024_u16)
        .step_by(2)
        .map(|value| wide_entry(value, FileKind::Regular))
        .collect::<Result<Vec<_>, _>>()?;
    let root = put(&store, &TreePage::Leaf(initial), 1_024)?;
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
    )?;
    assert!(
        receipt.work.items_examined <= 4 * 1_024,
        "linear structural plan examined {} items",
        receipt.work.items_examined
    );
    assert_eq!(flatten_tree(&store, receipt.root)?.len(), 1_024);
    Ok(())
}

#[test]
fn unordered_batch_is_deterministic_and_preserves_equal_name_order()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put(&store, &TreePage::Leaf(Vec::new()), 8)?;
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
    )?;
    assert_eq!(flatten_tree(&store, receipt.root)?, vec![a]);
    Ok(())
}

#[test]
fn unordered_sort_work_is_admitted_before_tree_access() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put(&store, &TreePage::Leaf(Vec::new()), 8)?;
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
    )
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

fn build_tree(
    store: &MemoryObjectStore,
    entries: &[TreeEntry],
    width: usize,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    if entries.is_empty() {
        return put(store, &TreePage::Leaf(Vec::new()), u32::try_from(width)?);
    }
    let mut level = Vec::new();
    for chunk in entries.chunks(width) {
        level.push((
            chunk[0].name.clone(),
            put(
                store,
                &TreePage::Leaf(chunk.to_vec()),
                u32::try_from(width)?,
            )?,
        ));
    }
    while level.len() > 1 {
        let mut parent = Vec::new();
        for chunk in level.chunks(width) {
            let children = chunk
                .iter()
                .map(|(first_name, page)| TreeChild {
                    first_name: first_name.clone(),
                    page: *page,
                })
                .collect();
            parent.push((
                chunk[0].0.clone(),
                put(store, &TreePage::Internal(children), u32::try_from(width)?)?,
            ));
        }
        level = parent;
    }
    Ok(level[0].1)
}

fn flatten_tree(
    store: &MemoryObjectStore,
    root: ObjectId,
) -> Result<Vec<TreeEntry>, Box<dyn std::error::Error>> {
    let bytes = store.read(root, u64::MAX, WorkBudget::UNBOUNDED)?.value;
    Ok(match decode_tree_page(&bytes, DecodeLimits::default())? {
        TreePage::Leaf(entries) => entries,
        TreePage::Internal(children) => {
            let mut entries = Vec::new();
            for child in children {
                entries.extend(flatten_tree(store, child.page)?);
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

#[test]
fn generated_directory_histories_match_independent_ordered_map()
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
        let root = build_tree(&store, &initial, 4)?;
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
        )?;
        assert_eq!(
            flatten_tree(&store, receipt.root)?,
            expected.into_values().collect::<Vec<_>>(),
            "history {history} diverged"
        );
    }
    Ok(())
}

#[test]
fn persistent_error_translation_is_total() {
    use persistent_btree::Error as PersistentError;

    assert!(matches!(
        map_error(PersistentError::Empty),
        TreeMutationError::Empty
    ));
    assert!(matches!(
        map_error(PersistentError::TooManyMutations),
        TreeMutationError::TooManyMutations
    ));
    assert!(matches!(
        map_error(PersistentError::WrongRootKind),
        TreeMutationError::WrongRootKind
    ));
    assert!(matches!(
        map_error(PersistentError::InvalidLimits),
        TreeMutationError::InvalidLimits
    ));
    assert!(matches!(
        map_error(PersistentError::PageItemTooLarge),
        TreeMutationError::PageItemTooLarge
    ));
    assert!(matches!(
        map_error(PersistentError::HeightExceeded),
        TreeMutationError::HeightExceeded
    ));
    assert!(matches!(
        map_error(PersistentError::CycleOrAlias),
        TreeMutationError::CycleOrAlias
    ));
    assert!(matches!(
        map_error(PersistentError::ChildBoundsMismatch),
        TreeMutationError::ChildBoundsMismatch
    ));
    assert!(matches!(
        map_error(PersistentError::MutationContract),
        TreeMutationError::MutationContract
    ));
    assert!(matches!(
        map_error(PersistentError::AllocationFailed),
        TreeMutationError::AllocationFailed
    ));
    assert!(matches!(
        map_error(PersistentError::Allocation(
            AllocationError::CapacityExceeded
        )),
        TreeMutationError::AllocationFailed
    ));
    assert!(matches!(
        map_error(PersistentError::Allocation(AllocationError::Work(
            WorkError::Overflow
        ))),
        TreeMutationError::Work(WorkError::Overflow)
    ));
    assert!(matches!(
        map_error(PersistentError::Semantic(TreeSemanticError::Missing)),
        TreeMutationError::Missing
    ));
    assert!(matches!(
        map_error(PersistentError::Storage(ObjectStoreError::Missing)),
        TreeMutationError::Storage(ObjectStoreError::Missing)
    ));
    assert!(matches!(
        map_error(PersistentError::Decode(
            CanonicalDecodeError::LengthOverflow
        )),
        TreeMutationError::Decode(CanonicalDecodeError::LengthOverflow)
    ));
    assert!(matches!(
        map_error(PersistentError::Work(WorkError::Overflow)),
        TreeMutationError::Work(WorkError::Overflow)
    ));
}
