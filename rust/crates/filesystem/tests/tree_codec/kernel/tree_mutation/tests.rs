use acyclic_fs::kernel::{
    CanonicalDecodeError, DecodeLimits, FileKind, TreeEntry, TreeMutation, TreeMutationError,
    TreePage, apply_tree_mutations, decode_tree_page,
};
use acyclic_fs::{AsyncObjectStore, AuthorityWork, CancellationToken};
use acyclic_fs_format::identity::FileId;
use acyclic_fs_work::performance::{WorkBudget, WorkError};

use super::super::memory::memory_objects;
use super::super::tree_entries::{entry, name};
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
