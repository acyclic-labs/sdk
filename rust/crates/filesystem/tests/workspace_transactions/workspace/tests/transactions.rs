use acyclic_fs::kernel::{FileKind, FileMetadata};
use acyclic_fs::{
    ByteRange, FileId, ForkOptions, Fs, IdempotencyKey, TransactionCommit, TransactionRebase,
    WorkspaceError,
};
use acyclic_stream::OwnedBytes;
use std::error::Error;

use crate::fixtures::{commit, fork_workspace};

#[tokio::test]
async fn conditional_remove_preserves_typed_stale_identity() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("conditional-remove"))?;
    let mut create = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    create.write_text("/file.txt", "current").await?;
    assert!(matches!(
        commit(&mut create).await?,
        TransactionCommit::Committed(_)
    ));
    let actual = unbounded!(workspace, stat("/file.txt"))?.file_id;
    let mut stale = actual.into_bytes();
    stale[0] ^= 1;
    let stale = FileId::from_bytes(stale);
    let mut remove = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    assert!(matches!(
        unbounded!(remove, remove_if("/file.txt", stale)),
        Err(WorkspaceError::StaleIdentity)
    ));
    assert_eq!(
        unbounded!(workspace, read("/file.txt", 64))?.as_ref(),
        b"current"
    );
    Ok(())
}

#[tokio::test]
async fn transaction_is_atomic_idempotent_and_visible_to_forks() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("repo"))?;
    let key = IdempotencyKey::new();
    let mut transaction = unbounded!(workspace, begin_transaction(key))?;
    unbounded!(transaction, create_dir_all("/output/nested"))?;
    transaction
        .write_text("/output/nested/status", "ready")
        .await?;
    let TransactionCommit::Committed(generation) = commit(&mut transaction).await? else {
        return Err("first publication did not commit".into());
    };
    assert_eq!(
        unbounded!(workspace, read("/output/nested/status", 64))?,
        OwnedBytes::from_static(b"ready")
    );
    let reopened = unbounded!(workspace, generation(generation.id()))?;
    assert_eq!(
        unbounded!(reopened, read("/output/nested/status", 64))?,
        OwnedBytes::from_static(b"ready")
    );
    let fork = fork_workspace(
        &workspace,
        "copy",
        ForkOptions::from_generation(generation.clone(), IdempotencyKey::new()),
    )
    .await?;
    assert_eq!(
        unbounded!(fork, read("/output/nested/status", 64))?,
        OwnedBytes::from_static(b"ready")
    );

    // Retry the original candidate and publication head, rather than rebuilding
    // the same file bytes against a different authenticated base.
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::AlreadyCommitted(retried) if retried.id() == generation.id()
    ));

    let mut changed_base = unbounded!(workspace, begin_transaction(key))?;
    unbounded!(changed_base, create_dir_all("/output/nested"))?;
    changed_base
        .write_text("/output/nested/status", "ready")
        .await?;
    assert!(matches!(
        commit(&mut changed_base).await?,
        TransactionCommit::IdempotencyConflict
    ));

    let mut conflicting_retry = unbounded!(workspace, begin_transaction(key))?;
    conflicting_retry
        .write_text("/output/nested/status", "different")
        .await?;
    assert!(matches!(
        commit(&mut conflicting_retry).await?,
        TransactionCommit::IdempotencyConflict
    ));
    Ok(())
}

#[tokio::test]
async fn retained_transaction_rebases_disjoint_work_and_rejects_overlap()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("transaction-rebase"))?;

    let mut first = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    let mut disjoint = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    first.write_text("/first", "one").await?;
    disjoint.write_text("/second", "two").await?;
    assert!(matches!(
        commit(&mut first).await?,
        TransactionCommit::Committed(_)
    ));
    assert!(matches!(
        commit(&mut disjoint).await?,
        TransactionCommit::Conflict { .. }
    ));
    assert!(matches!(
        unbounded!(disjoint, rebase(16))?,
        TransactionRebase::Rebased(_)
    ));
    assert!(matches!(
        commit(&mut disjoint).await?,
        TransactionCommit::Committed(_)
    ));
    assert_eq!(
        unbounded!(workspace, read("/first", 8))?,
        OwnedBytes::from_static(b"one")
    );
    assert_eq!(
        unbounded!(workspace, read("/second", 8))?,
        OwnedBytes::from_static(b"two")
    );

    let mut winner = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    let mut overlap = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    winner.write_text("/first", "winner").await?;
    overlap.write_text("/first", "loser").await?;
    assert!(matches!(
        commit(&mut winner).await?,
        TransactionCommit::Committed(_)
    ));
    assert!(matches!(
        commit(&mut overlap).await?,
        TransactionCommit::Conflict { .. }
    ));
    let TransactionRebase::Conflicted {
        conflicts,
        truncated,
    } = unbounded!(overlap, rebase(16))?
    else {
        return Err("overlapping mutation rebased unsafely".into());
    };
    assert!(!conflicts.is_empty());
    assert!(!truncated);
    assert_eq!(
        unbounded!(workspace, read("/first", 8))?,
        OwnedBytes::from_static(b"winner")
    );
    Ok(())
}

#[tokio::test]
async fn concurrent_directory_creations_fold_and_equal_new_names_still_conflict()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("directory-fold"))?;

    // Each side creates the same missing directories for its own file.
    let mut first = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    let mut second = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    for (transaction, name) in [(&mut first, "one"), (&mut second, "two")] {
        unbounded!(transaction, create_dir_all("/inputs/messages"))?;
        transaction
            .write_text(&format!("/inputs/messages/{name}"), name)
            .await?;
    }
    assert!(matches!(
        commit(&mut first).await?,
        TransactionCommit::Committed(_)
    ));
    assert!(matches!(
        commit(&mut second).await?,
        TransactionCommit::Conflict { .. }
    ));
    assert!(matches!(
        unbounded!(second, rebase(16))?,
        TransactionRebase::Rebased(_)
    ));
    assert!(matches!(
        commit(&mut second).await?,
        TransactionCommit::Committed(_)
    ));
    for name in ["one", "two"] {
        assert_eq!(
            unbounded!(workspace, read(&format!("/inputs/messages/{name}"), 8))?.as_ref(),
            name.as_bytes()
        );
    }

    // The same new name with different bytes in both new directories is a
    // real conflict: folding never chooses one side's file.
    let mut winner = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    let mut loser = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    for (transaction, text) in [(&mut winner, "winner"), (&mut loser, "loser")] {
        unbounded!(transaction, create_dir_all("/outputs"))?;
        transaction.write_text("/outputs/report", text).await?;
    }
    assert!(matches!(
        commit(&mut winner).await?,
        TransactionCommit::Committed(_)
    ));
    assert!(matches!(
        commit(&mut loser).await?,
        TransactionCommit::Conflict { .. }
    ));
    assert!(matches!(
        unbounded!(loser, rebase(16))?,
        TransactionRebase::Conflicted { .. }
    ));
    assert_eq!(
        unbounded!(workspace, read("/outputs/report", 8))?.as_ref(),
        b"winner"
    );
    Ok(())
}

#[tokio::test]
async fn exact_generation_transaction_is_stateless_and_rebases_safely() -> Result<(), Box<dyn Error>>
{
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("exact-transaction"))?;
    let base = unbounded!(workspace, head())?;

    workspace.write_text("/upstream", "one").await?;
    let mut transaction = unbounded!(
        workspace,
        begin_transaction_at(&base, IdempotencyKey::new())
    )?;
    transaction.write_text("/local", "two").await?;
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::Conflict { .. }
    ));
    assert!(matches!(
        unbounded!(transaction, rebase(16))?,
        TransactionRebase::Rebased(_)
    ));
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::Committed(_)
    ));
    assert_eq!(
        unbounded!(workspace, read("/upstream", 8))?,
        OwnedBytes::from_static(b"one")
    );
    assert_eq!(
        unbounded!(workspace, read("/local", 8))?,
        OwnedBytes::from_static(b"two")
    );

    let other = unbounded!(fs, create_workspace("other-exact-transaction"))?;
    let foreign = unbounded!(other, head())?;
    assert!(matches!(
        unbounded!(
            workspace,
            begin_transaction_at(&foreign, IdempotencyKey::new())
        ),
        Err(WorkspaceError::ForeignGeneration)
    ));
    Ok(())
}

#[tokio::test]
async fn transaction_preserves_links_sparse_ranges_metadata_and_cow_clones()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("shapes"))?;
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(transaction, create_directory("/tree"))?;
    unbounded!(
        transaction,
        write("/tree/source", OwnedBytes::from_static(b"abcdef"))
    )?;
    unbounded!(
        transaction,
        write("/tree/destination", OwnedBytes::from_static(b"......"))
    )?;
    unbounded!(
        transaction,
        create_symbolic_link("/tree/symlink", OwnedBytes::from_static(b"source"))
    )?;
    unbounded!(transaction, hard_link("/tree/source", "/tree/hard-link"))?;
    unbounded!(
        transaction,
        write_range("/tree/source", 1, OwnedBytes::from_static(b"Z"))
    )?;
    unbounded!(
        transaction,
        zero_range(
            "/tree/source",
            ByteRange {
                offset: 2,
                length: 2,
            },
            false,
            false
        )
    )?;
    unbounded!(
        transaction,
        preallocate(
            "/tree/source",
            ByteRange {
                offset: 0,
                length: 6,
            },
            true
        )
    )?;
    unbounded!(
        transaction,
        clone_range("/tree/source", 0, "/tree/destination", 1, 5)
    )?;
    unbounded!(
        transaction,
        set_metadata("/tree/source", FileMetadata::default())
    )?;
    unbounded!(transaction, resize("/tree/destination", 8))?;
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::Committed(_)
    ));
    let source = OwnedBytes::from_static(b"aZ\0\0ef");
    assert_eq!(unbounded!(workspace, read("/tree/source", 32))?, source);
    assert_eq!(unbounded!(workspace, read("/tree/hard-link", 32))?, source);
    assert_eq!(
        unbounded!(workspace, read("/tree/destination", 32))?,
        OwnedBytes::from_static(b".aZ\0\0e\0\0")
    );
    Ok(())
}

#[tokio::test]
async fn transaction_rename_follows_posix_kind_rules() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("rename-kinds"))?;
    let mut setup = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(setup, create_dir_all("/a"))?;
    unbounded!(setup, create_dir_all("/e"))?;
    unbounded!(setup, create_dir_all("/full"))?;
    unbounded!(setup, write("/full/x", OwnedBytes::from_static(b"x")))?;
    unbounded!(
        setup,
        create_symbolic_link("/d", OwnedBytes::from_static(b"/\x08"))
    )?;
    unbounded!(setup, write("/f", OwnedBytes::from_static(b"file")))?;
    let TransactionCommit::Committed(_) = commit(&mut setup).await? else {
        return Err("setup did not commit".into());
    };
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    assert!(matches!(
        unbounded!(transaction, rename("/a", "/d", true)),
        Err(WorkspaceError::NotDirectory)
    ));
    assert!(matches!(
        unbounded!(transaction, rename("/a", "/f", true)),
        Err(WorkspaceError::NotDirectory)
    ));
    assert!(matches!(
        unbounded!(transaction, rename("/d", "/a", true)),
        Err(WorkspaceError::IsDirectory)
    ));
    assert!(matches!(
        unbounded!(transaction, rename("/f", "/a", true)),
        Err(WorkspaceError::IsDirectory)
    ));
    // Nor does anything replace a directory that is not empty (`ENOTEMPTY`).
    assert!(matches!(
        unbounded!(transaction, rename("/e", "/full", true)),
        Err(WorkspaceError::DirectoryNotEmpty)
    ));
    unbounded!(transaction, rename("/a", "/a", true))?;
    unbounded!(transaction, rename("/a", "/a", false))
        .err()
        .ok_or("RENAME_NOREPLACE onto itself succeeded")?;
    drop(transaction);
    let end = unbounded!(workspace, head())?;
    assert_eq!(unbounded!(end, stat("/a"))?.kind, FileKind::Directory);
    assert_eq!(unbounded!(end, stat("/d"))?.kind, FileKind::SymbolicLink);
    assert_eq!(unbounded!(end, stat("/f"))?.kind, FileKind::Regular);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn directory_batches_still_observe_replaced_file_contents() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("batch-file-observation"))?;
    let mut seed = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(seed, create_dir_all("/shared"))?;
    seed.write_text("/shared/file", "base").await?;
    commit(&mut seed).await?;
    let mut pending = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(
        pending,
        write_batch(
            &["/shared"],
            &[("/shared/file", OwnedBytes::from_static(b"replacement"))]
        )
    )?;
    workspace.write_text("/shared/file", "concurrent").await?;
    assert!(matches!(
        commit(&mut pending).await?,
        TransactionCommit::Conflict { .. }
    ));
    assert!(matches!(
        unbounded!(pending, rebase(16))?,
        TransactionRebase::Conflicted { .. }
    ));
    assert_eq!(
        unbounded!(workspace, read("/shared/file", 16))?.as_ref(),
        b"concurrent"
    );
    Ok(())
}
