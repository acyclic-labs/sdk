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

/// A whole-tree restore to a tree without `/.volumes` keeps `/.volumes`,
/// the directory holding the reserved `.volumes/scratch`, and still removes
/// its unreserved child.
#[tokio::test]
async fn a_whole_tree_restore_keeps_the_directory_of_a_reserved_path() {
    let cancel = CancellationToken::new();
    let workspace = memory_workspace("git-restore-reserved-parent").await;
    let mut saved = begin_fixture(&workspace, IdempotencyKey::from_bytes([0x83; 16])).await;
    saved.write_text("/base", "saved").await.expect("file");
    let source = publish_fixture(saved).await;
    let mut live = begin_fixture(&workspace, IdempotencyKey::from_bytes([0x84; 16])).await;
    live.create_dir_all("/.volumes/scratch", WorkBudget::UNBOUNDED, &cancel)
        .await
        .expect("directory");
    live.write_text("/.volumes/scratch/live", "host")
        .await
        .expect("reserved file");
    live.write_text("/.volumes/other", "user")
        .await
        .expect("file");
    let current = publish_fixture(live).await;
    let policy = GitIgnorePolicy::default().reserving([".volumes/scratch"]);
    let paths = git_restore_paths(&current, &source, &policy, u32::MAX, &cancel)
        .await
        .expect("restore paths");
    assert_eq!(paths, [".volumes/other"]);
    // Negative control: restoring the directory itself would remove it
    // with its reserved child, which the restore refuses.
    let mut with_parent = vec![".volumes".to_owned()];
    with_parent.extend(paths.iter().cloned());
    assert!(!restored(&workspace, &source, &current, &with_parent).await);
    assert!(restored(&workspace, &source, &current, &paths).await);
    let head = workspace
        .head(WorkBudget::UNBOUNDED, &cancel)
        .await
        .expect("head")
        .value;
    let kept = head
        .read("/.volumes/scratch/live", 64, WorkBudget::UNBOUNDED, &cancel)
        .await
        .expect("reserved content")
        .value;
    assert_eq!(kept.as_ref(), b"host");
    assert!(
        head.stat("/.volumes/other", WorkBudget::UNBOUNDED, &cancel)
            .await
            .is_err()
    );
}

/// A user file where a reserved path's parent would be holds no reserved
/// path, so restoring it proceeds: the lookup of the reserved path through
/// that file is absence, not a failure.
#[tokio::test]
async fn a_file_over_a_reserved_parent_restores() {
    let cancel = CancellationToken::new();
    let workspace = memory_workspace("git-restore-reserved-file-parent").await;
    let mut saved = begin_fixture(&workspace, IdempotencyKey::from_bytes([0x85; 16])).await;
    saved.write_text("/.volumes", "saved").await.expect("file");
    let source = publish_fixture(saved).await;
    let mut live = begin_fixture(&workspace, IdempotencyKey::from_bytes([0x86; 16])).await;
    live.remove("/.volumes", WorkBudget::UNBOUNDED, &cancel)
        .await
        .expect("remove");
    live.write_text("/.volumes", "live").await.expect("file");
    let current = publish_fixture(live).await;
    let policy = GitIgnorePolicy::default().reserving([".volumes/scratch"]);
    let paths = git_paths_preserving_reserved(
        &current,
        Some(&source),
        &policy,
        [".volumes".to_owned()],
        &cancel,
    )
    .await
    .expect("a lookup through a file is absence");
    assert_eq!(paths, [".volumes"]);
    assert!(restored(&workspace, &source, &current, &paths).await);
    let head = workspace
        .head(WorkBudget::UNBOUNDED, &cancel)
        .await
        .expect("head")
        .value;
    let bytes = head
        .read("/.volumes", 64, WorkBudget::UNBOUNDED, &cancel)
        .await
        .expect("restored file")
        .value;
    assert_eq!(bytes.as_ref(), b"saved");
}
