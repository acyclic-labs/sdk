use acyclic_fs::kernel::{
    CanonicalDecodeError, Child, DecodeLimits, FileKind, TreeEntry, TreeMutation,
    TreeMutationError, TreePage, apply_tree_mutations, decode_tree_page,
};
use acyclic_fs::{AsyncObjectStore, AuthorityWork, CancellationToken, MemoryObjectBackend};
use acyclic_fs_format::{identity::FileId, object_reference::ObjectId};
use acyclic_fs_work::performance::{WorkBudget, WorkError};

use std::collections::BTreeMap;

use super::super::memory::memory_objects;
use super::super::tree_entries::{entry, name, wide_entry};
use super::super::tree_objects::put_tree_page;

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
    store: &MemoryObjectBackend,
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
    store: &MemoryObjectBackend,
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
