//! A whole-tree restore from `git_restore_paths` rewrites every changed shape.
use acyclic_fs::{
    CancellationToken, Fs, FsError, Generation, IdempotencyKey, MemoryAuthorityBackend,
    MemoryObjectBackend, PublicationPermit, TransactionCommit, Workspace, git_restore_paths,
};
use acyclic_fs_work::performance::WorkBudget;

use crate::transaction::{allow, commit};

type MemoryWorkspace = Workspace<MemoryAuthorityBackend, MemoryObjectBackend>;
type MemoryGeneration = Generation<MemoryAuthorityBackend, MemoryObjectBackend>;

/// Publishes a saved tree, then a live tree where `/hot` has the other shape:
/// a file replacing the saved directory, or a directory replacing the saved
/// file.
async fn replaced(
    directory_saved: bool,
) -> Result<(MemoryWorkspace, MemoryGeneration, MemoryGeneration), Box<dyn std::error::Error>> {
    let cancel = CancellationToken::new();
    let workspace = Fs::memory()
        .create_workspace("git-restore-replaced", WorkBudget::UNBOUNDED, &cancel)
        .await?
        .value;
    let mut saved = workspace
        .begin_transaction(
            IdempotencyKey::from_bytes([0x81; 16]),
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await?
        .value;
    if directory_saved {
        saved
            .create_dir_all("/hot", WorkBudget::UNBOUNDED, &cancel)
            .await?;
        saved.write_text("/hot/value", "saved").await?;
    } else {
        saved.write_text("/hot", "saved").await?;
    }
    let TransactionCommit::Committed(source) = commit(&mut saved).await? else {
        return Err("saved fixture did not commit".into());
    };
    let mut live = workspace
        .begin_transaction(
            IdempotencyKey::from_bytes([0x82; 16]),
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await?
        .value;
    if directory_saved {
        for path in ["/hot/value", "/hot"] {
            live.remove(path, WorkBudget::UNBOUNDED, &cancel).await?;
        }
        live.write_text("/hot", "live").await?;
    } else {
        live.remove("/hot", WorkBudget::UNBOUNDED, &cancel).await?;
        live.create_dir_all("/hot/nested", WorkBudget::UNBOUNDED, &cancel)
            .await?;
        live.write_text("/hot/nested/value", "live").await?;
    }
    let TransactionCommit::Committed(current) = commit(&mut live).await? else {
        return Err("live fixture did not commit".into());
    };
    Ok((workspace, source, current))
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
            PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            allow::<FsError>,
        )
        .await
        .is_ok_and(|receipt| matches!(receipt.value, TransactionCommit::Committed(_)))
}

#[tokio::test]
async fn a_whole_tree_restore_rewrites_a_replaced_shape_either_way()
-> Result<(), Box<dyn std::error::Error>> {
    let cancel = CancellationToken::new();
    for directory_saved in [true, false] {
        let (workspace, source, current) = replaced(directory_saved).await?;
        let paths = git_restore_paths(&current, &source, u32::MAX, &cancel).await?;
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
        let head = workspace.head(WorkBudget::UNBOUNDED, &cancel).await?.value;
        let saved = if directory_saved {
            "/hot/value"
        } else {
            "/hot"
        };
        let bytes = head
            .read(saved, 64, WorkBudget::UNBOUNDED, &cancel)
            .await?
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
    Ok(())
}
