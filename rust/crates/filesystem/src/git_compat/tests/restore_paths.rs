//! A whole-tree restore from `git_restore_paths` rewrites every changed shape.
use super::*;

type MemoryWorkspace = Workspace<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend>;
type MemoryGeneration = Generation<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend>;

/// Publishes a saved tree, then a live tree where `/hot` has the other shape:
/// a file replacing the saved directory, or a directory replacing the saved
/// file.
async fn replaced(directory_saved: bool) -> (MemoryWorkspace, MemoryGeneration, MemoryGeneration) {
    let cancel = CancellationToken::new();
    let workspace = memory_workspace("git-restore-replaced").await;
    let mut saved = begin_fixture(&workspace, IdempotencyKey::from_bytes([0x81; 16])).await;
    if directory_saved {
        saved
            .create_dir_all("/hot", WorkBudget::UNBOUNDED, &cancel)
            .await
            .expect("directory");
        saved.write_text("/hot/value", "saved").await.expect("file");
    } else {
        saved.write_text("/hot", "saved").await.expect("file");
    }
    let source = publish_fixture(saved).await;
    let mut live = begin_fixture(&workspace, IdempotencyKey::from_bytes([0x82; 16])).await;
    if directory_saved {
        for path in ["/hot/value", "/hot"] {
            live.remove(path, WorkBudget::UNBOUNDED, &cancel)
                .await
                .expect("remove");
        }
        live.write_text("/hot", "live").await.expect("file");
    } else {
        live.remove("/hot", WorkBudget::UNBOUNDED, &cancel)
            .await
            .expect("remove");
        live.create_dir_all("/hot/nested", WorkBudget::UNBOUNDED, &cancel)
            .await
            .expect("directory");
        live.write_text("/hot/nested/value", "live")
            .await
            .expect("file");
    }
    let current = publish_fixture(live).await;
    (workspace, source, current)
}

/// Whether restoring `paths` from `source` over `current` committed.
async fn restored(
    workspace: &MemoryWorkspace,
    source: &MemoryGeneration,
    current: &MemoryGeneration,
    paths: &[String],
) -> bool {
    workspace
        .restore_paths_from(
            source,
            paths,
            current.id(),
            IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            |_| {
                std::future::ready(Ok::<_, crate::OperationFailure<crate::FsError>>(
                    crate::OperationReceipt {
                        value: (),
                        work: crate::WorkCounters::UNCHARGED,
                    },
                ))
            },
        )
        .await
        .is_ok_and(|receipt| matches!(receipt.value, TransactionCommit::Committed(_)))
}

#[tokio::test]
async fn a_whole_tree_restore_rewrites_a_replaced_shape_either_way() {
    let cancel = CancellationToken::new();
    for directory_saved in [true, false] {
        let (workspace, source, current) = replaced(directory_saved).await;
        let paths = git_restore_paths(
            &current,
            &source,
            &GitIgnorePolicy::default(),
            u32::MAX,
            &cancel,
        )
        .await
        .expect("restore paths");
        if directory_saved {
            // Negative control: children before their replaced parent cannot
            // restore the saved directory.
            let reversed: Vec<_> = paths.iter().rev().cloned().collect();
            assert!(!restored(&workspace, &source, &current, &reversed).await);
        }
        assert!(
            restored(&workspace, &source, &current, &paths).await,
            "{paths:?}"
        );
        let head = workspace
            .head(WorkBudget::UNBOUNDED, &cancel)
            .await
            .expect("head")
            .value;
        let saved = if directory_saved {
            "/hot/value"
        } else {
            "/hot"
        };
        let bytes = head
            .read(saved, 64, WorkBudget::UNBOUNDED, &cancel)
            .await
            .expect("saved content")
            .value;
        assert_eq!(bytes.as_ref(), b"saved");
        if !directory_saved {
            assert!(
                head.stat("/hot/nested", WorkBudget::UNBOUNDED, &cancel)
                    .await
                    .is_err()
            );
        }
    }
}
