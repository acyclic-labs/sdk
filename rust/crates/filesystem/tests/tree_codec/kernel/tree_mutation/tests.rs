use acyclic_fs::kernel::{
    CanonicalDecodeError, Child, DecodeLimits, FileKind, TreeEntry, TreeMutation,
    TreeMutationError, TreePage, apply_tree_mutations, decode_tree_page,
};
use acyclic_fs::{AsyncObjectStore, AuthorityWork, CancellationToken, MemoryObjectStore};
use acyclic_fs_format::{identity::FileId, object_reference::ObjectId};
use acyclic_fs_work::performance::{WorkBudget, WorkError};

use std::collections::BTreeMap;

use super::super::memory::memory_objects;
use super::super::tree_entries::{entry, name, wide_entry};
use super::super::tree_objects::put_tree_page;

#[path = "../../../support/two_leaf_tree.rs"]
mod pair;
use pair::{pair_limits, two_leaf_tree};

#[tokio::test]
async fn overflowing_leaf_splits_and_builds_one_new_root() -> Result<(), Box<dyn std::error::Error>>
{
    let store = memory_objects();
    let root = put_tree_page(
        &store,
        &TreePage::Leaf(vec![
            entry("a", 1, FileKind::Regular)?,
            entry("c", 3, FileKind::Regular)?,
        ]),
        2,
    )
    .await?;
    let limits = DecodeLimits {
        maximum_page_items: 2,
        ..DecodeLimits::default()
    };
    let receipt = apply_tree_mutations(
        &store,
        root,
        vec![TreeMutation::Insert(entry("b", 2, FileKind::Regular)?)],
        limits,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(receipt.work.page_reads, 1);
    assert_eq!(receipt.work.page_writes, 3);
    let bytes = store
        .read(
            receipt.value,
            u64::MAX,
            &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()),
        )
        .await?;
    assert!(matches!(
        decode_tree_page(&bytes, limits, &mut |_| Ok::<_, CanonicalDecodeError>(()))?,
        TreePage::Internal(children) if children.len() == 2
    ));
    Ok(())
}

#[tokio::test]
async fn every_tree_semantic_failure_is_exact_and_non_mutating()
-> Result<(), Box<dyn std::error::Error>> {
    let store = memory_objects();
    let existing = entry("m", 2, FileKind::Regular)?;
    let root = put_tree_page(&store, &TreePage::Leaf(vec![existing.clone()]), 8).await?;
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
            DecodeLimits::default(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
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
        assert_eq!(failure.work()?.backend_write_operations, 0);
    }
    Ok(())
}

#[tokio::test]
async fn unordered_sort_work_is_admitted_before_tree_access()
-> Result<(), Box<dyn std::error::Error>> {
    let store = memory_objects();
    let root = put_tree_page(&store, &TreePage::Leaf(Vec::new()), 8).await?;
    let failure = apply_tree_mutations(
        &store,
        root,
        vec![
            TreeMutation::Insert(entry("b", 1, FileKind::Regular)?),
            TreeMutation::Insert(entry("a", 2, FileKind::Regular)?),
        ],
        DecodeLimits::default(),
        WorkBudget {
            items_examined: 0,
            ..WorkBudget::UNBOUNDED
        },
        &CancellationToken::new(),
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
    assert_eq!(failure.work()?.backend_read_operations, 0);
    assert_eq!(failure.work()?.backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn structural_batch_rebuilds_wide_leaf_in_order() -> Result<(), Box<dyn std::error::Error>> {
    let store = memory_objects();
    let initial = (0..1_024_u16)
        .step_by(2)
        .map(|value| wide_entry(value, FileKind::Regular))
        .collect::<Result<Vec<_>, _>>()?;
    let root = put_tree_page(&store, &TreePage::Leaf(initial), 1_024).await?;
    let mutations = (1..1_024_u16)
        .step_by(2)
        .map(|value| wide_entry(value, FileKind::Regular).map(TreeMutation::Insert))
        .collect::<Result<Vec<_>, _>>()?;
    let receipt = apply_tree_mutations(
        &store,
        root,
        mutations,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    let expected = (0..1_024_u16)
        .map(|value| wide_entry(value, FileKind::Regular))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(flatten_tree(&store, receipt.value).await?, expected);
    Ok(())
}

#[tokio::test]
async fn unordered_batch_is_deterministic_and_preserves_equal_name_order()
-> Result<(), Box<dyn std::error::Error>> {
    let store = memory_objects();
    let root = put_tree_page(&store, &TreePage::Leaf(Vec::new()), 8).await?;
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
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(flatten_tree(&store, receipt.value).await?, vec![a]);
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
        return put_tree_page(store, &TreePage::Leaf(Vec::new()), u32::try_from(width)?).await;
    }
    let mut level = Vec::new();
    for chunk in entries.chunks(width) {
        level.push((
            chunk
                .first()
                .ok_or("missing leaf fixture entry")?
                .name
                .clone(),
            put_tree_page(
                store,
                &TreePage::Leaf(chunk.to_vec()),
                u32::try_from(width)?,
            )
            .await?,
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
                chunk
                    .first()
                    .ok_or("missing internal fixture entry")?
                    .0
                    .clone(),
                put_tree_page(store, &TreePage::Internal(children), u32::try_from(width)?).await?,
            ));
        }
        level = parent;
    }
    Ok(level.first().ok_or("missing fixture root")?.1)
}

async fn flatten_tree(
    store: &MemoryObjectStore,
    root: ObjectId,
) -> Result<Vec<TreeEntry>, Box<dyn std::error::Error>> {
    let bytes = store
        .read(
            root,
            u64::MAX,
            &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()),
        )
        .await?;
    Ok(
        match decode_tree_page(&bytes, DecodeLimits::default(), &mut |_| {
            Ok::<_, CanonicalDecodeError>(())
        })? {
            TreePage::Leaf(entries) => entries,
            TreePage::Internal(children) => {
                let mut entries = Vec::new();
                for child in children {
                    entries.extend(Box::pin(flatten_tree(store, child.page)).await?);
                }
                entries
            }
        },
    )
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
        let store = memory_objects();
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
            DecodeLimits {
                maximum_page_items: 4,
                ..DecodeLimits::default()
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
        assert_eq!(
            flatten_tree(&store, receipt.value).await?,
            expected.into_values().collect::<Vec<_>>(),
            "history {history} diverged"
        );
    }
    Ok(())
}

#[tokio::test]
async fn point_noop_uses_logarithmic_leaf_search_without_rebuild()
-> Result<(), Box<dyn std::error::Error>> {
    let store = memory_objects();
    let entries = (0..1_024_u16)
        .map(|value| wide_entry(value, FileKind::Regular))
        .collect::<Result<Vec<_>, _>>()?;
    let root = put_tree_page(&store, &TreePage::Leaf(entries), 1_024).await?;
    for (selected, comparisons) in [(777, 10), (0, 11)] {
        let replacement = wide_entry(selected, FileKind::Regular)?;
        let receipt = apply_tree_mutations(
            &store,
            root,
            vec![TreeMutation::Replace {
                expected_file_id: replacement.file_id,
                entry: replacement,
            }],
            DecodeLimits::default(),
            WorkBudget {
                page_writes: 0,
                ..WorkBudget::UNBOUNDED
            },
            &CancellationToken::new(),
        )
        .await?;
        // The one-object backend visits one index node and compares its full
        // object key. Then validate the bounded page, classify one mutation and
        // admit one visited-page probe. Only the leaf search varies by name.
        let backend_lookup = 1 + ObjectId::ENCODED_LEN as u64;
        assert_eq!(
            receipt.work.items_examined,
            backend_lookup + 1_024 + 1 + 1 + comparisons
        );
        assert_eq!(receipt.value, root);
        assert_eq!(receipt.work.page_reads, 1);
        assert_eq!(receipt.work.backend_read_operations, 1);
        assert_eq!(receipt.work.page_writes, 0);
        assert_eq!(receipt.work.backend_write_operations, 0);
        assert_eq!(receipt.work.bytes_encoded, 0);
    }
    Ok(())
}

#[tokio::test]
async fn visited_capacity_is_admitted_before_tree_access() -> Result<(), Box<dyn std::error::Error>>
{
    let store = memory_objects();
    let existing = wide_entry(1, FileKind::Regular)?;
    let root = put_tree_page(&store, &TreePage::Leaf(vec![existing.clone()]), 8).await?;
    let mutation = TreeMutation::Replace {
        entry: existing.clone(),
        expected_file_id: existing.file_id,
    };
    let failure = apply_tree_mutations(
        &store,
        root,
        vec![mutation],
        DecodeLimits::default(),
        WorkBudget {
            peak_allocation_bytes: 0,
            ..WorkBudget::UNBOUNDED
        },
        &CancellationToken::new(),
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
    assert_eq!(failure.work()?.backend_read_operations, 0);
    assert_eq!(failure.work()?.backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn mutation_peak_budget_refuses_before_writes() -> Result<(), Box<dyn std::error::Error>> {
    let store = memory_objects();
    let existing = wide_entry(1, FileKind::Regular)?;
    let root = put_tree_page(&store, &TreePage::Leaf(vec![existing.clone()]), 8).await?;
    let mutation = TreeMutation::Replace {
        entry: existing.clone(),
        expected_file_id: existing.file_id,
    };
    let baseline = apply_tree_mutations(
        &store,
        root,
        vec![mutation.clone()],
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    let maximum = baseline.work.peak_allocation_bytes - 1;
    let failure = apply_tree_mutations(
        &store,
        root,
        vec![mutation],
        DecodeLimits::default(),
        WorkBudget {
            peak_allocation_bytes: maximum,
            ..WorkBudget::UNBOUNDED
        },
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("underbudgeted decoded page unexpectedly succeeded")?;
    assert!(
        matches!(
            failure.error,
            TreeMutationError::Work(WorkError::BudgetExceeded {
                counter: "peak_allocation_bytes",
                observed,
                maximum: observed_maximum,
            }) if observed > observed_maximum && observed_maximum <= maximum
        ),
        "{failure:?}"
    );
    assert!(failure.work()?.peak_allocation_bytes <= maximum);
    assert_eq!(failure.work()?.backend_write_operations, 0);
    assert_eq!(failure.work()?.object_bytes_written, 0);
    Ok(())
}

#[tokio::test]
async fn touched_leaf_and_shared_root_are_rewritten_once() -> Result<(), Box<dyn std::error::Error>>
{
    let store = memory_objects();
    let (root, leaves) = two_leaf_tree(&store).await?;
    let receipt = apply_tree_mutations(
        &store,
        root,
        vec![
            TreeMutation::Replace {
                entry: entry("n", 3, FileKind::Fifo)?,
                expected_file_id: FileId::from_bytes([3; 16]),
            },
            TreeMutation::Replace {
                entry: entry("x", 4, FileKind::Fifo)?,
                expected_file_id: FileId::from_bytes([4; 16]),
            },
        ],
        pair_limits(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(receipt.work.page_reads, 2);
    assert_eq!(receipt.work.page_writes, 2);
    let root_bytes = store
        .read(
            receipt.value,
            u64::MAX,
            &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()),
        )
        .await?;
    let TreePage::Internal(children) = decode_tree_page(&root_bytes, pair_limits(), &mut |_| {
        Ok::<_, CanonicalDecodeError>(())
    })?
    else {
        return Err("candidate root is not internal".into());
    };
    let [left, right] = children.as_slice() else {
        return Err("candidate root does not hold two leaves".into());
    };
    let [previous_left, previous_right] = leaves;
    assert_eq!(left.page, previous_left);
    assert_ne!(right.page, previous_right);
    Ok(())
}

#[tokio::test]
async fn semantic_noop_preserves_root_and_writes_nothing() -> Result<(), Box<dyn std::error::Error>>
{
    let store = memory_objects();
    let (root, _) = two_leaf_tree(&store).await?;
    let receipt = apply_tree_mutations(
        &store,
        root,
        vec![TreeMutation::Replace {
            entry: entry("m", 2, FileKind::Regular)?,
            expected_file_id: FileId::from_bytes([2; 16]),
        }],
        pair_limits(),
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
async fn failed_identity_precondition_writes_no_objects() -> Result<(), Box<dyn std::error::Error>>
{
    let store = memory_objects();
    let (root, _) = two_leaf_tree(&store).await?;
    let failure = apply_tree_mutations(
        &store,
        root,
        vec![TreeMutation::Remove {
            name: name("n")?,
            expected_file_id: Some(FileId::from_bytes([9; 16])),
        }],
        pair_limits(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("identity conflict unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        TreeMutationError::FileIdentityConflict
    ));
    // Removing the following leaf's first name retains its predecessor tail
    // before checking the selected identity: root, predecessor, selected leaf.
    assert_eq!(failure.work()?.page_reads, 3);
    assert_eq!(failure.work()?.backend_write_operations, 0);
    assert_eq!(failure.work()?.page_writes, 0);
    assert_eq!(failure.work()?.bytes_encoded, 0);
    Ok(())
}
