use super::*;
use crate::model::{Lifecycle, VolumeConfig};
use crate::{CancellationToken, Fs, WorkBudget};
use std::error::Error;
#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
use std::path::Path;
use std::sync::Arc;

#[test]
fn names_are_canonical_bounded_and_path_independent() -> Result<(), Box<dyn Error>> {
    let composed = WorkspaceName::new("caf\u{e9}")?;
    let decomposed = WorkspaceName::new("cafe\u{301}")?;
    assert_eq!(composed, decomposed);
    assert_eq!(composed.as_str(), "caf\u{e9}");
    assert_eq!(WorkspaceName::new(""), Err(WorkspaceNameError::Empty));
    assert_eq!(WorkspaceName::new(".."), Err(WorkspaceNameError::Reserved));
    assert_eq!(
        WorkspaceName::new("parent/child"),
        Err(WorkspaceNameError::InvalidCharacter)
    );
    Ok(())
}

#[test]
fn identity_is_deterministic_and_namespace_scoped() -> Result<(), Box<dyn Error>> {
    let name = WorkspaceName::new("repo")?;
    let first = WorkspaceId::derive([1; 16], &name);
    assert_eq!(first, WorkspaceId::derive([1; 16], &name));
    assert_ne!(first, WorkspaceId::derive([2; 16], &name));
    assert_ne!(
        first,
        WorkspaceId::derive([1; 16], &WorkspaceName::new("other")?)
    );
    Ok(())
}

#[tokio::test]
async fn named_workspace_opens_and_forks_one_exact_generation() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("repo").await?;
    let base = main.head().await?;
    let fork_key = IdempotencyKey::from_bytes([0x42; 16]);
    let fork = main
        .fork(
            "agent-42",
            ForkOptions::from_generation(base.clone(), fork_key),
        )
        .await?;
    let retried = main
        .fork(
            "agent-42",
            ForkOptions::from_generation(base.clone(), fork_key),
        )
        .await?;
    assert_eq!(fs.open_workspace("repo").await?.id(), main.id());
    assert_ne!(fork.id(), main.id());
    assert_ne!(fork.head().await?.id(), base.id());
    assert_eq!(retried.id(), fork.id());
    assert_eq!(retried.head().await?.id(), fork.head().await?.id());
    assert!(
        main.fork(
            "agent-42",
            ForkOptions::from_generation(base, IdempotencyKey::from_bytes([0x43; 16])),
        )
        .await
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn string_workspace_paths_match_native_namespace_names() -> Result<(), Box<dyn Error>> {
    for profile in [
        crate::model::FilesystemProfile::Posix,
        crate::model::FilesystemProfile::Windows,
    ] {
        let fs = Fs::memory();
        let config = VolumeConfig {
            profile,
            ..VolumeConfig::portable(Lifecycle::Ephemeral)
        };
        let workspace = fs
            .create_workspace_with_config("native-path", config)
            .await?;
        workspace.write_text("/é.txt", "native").await?;
        let mut checkout = workspace
            .engine_checkout(GenerationSelector::Head, CheckoutMode::read_only_pinned())
            .await?;
        let path = NamespacePath::from_portable_in_profile(
            &PortablePath::parse("/é.txt", config.limits)?,
            profile,
            config.limits,
        )?;
        assert!(
            checkout
                .lookup_no_follow(&path, WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await?
                .value
                .record
                .is_some()
        );
    }
    Ok(())
}

#[tokio::test]
async fn conditional_remove_preserves_typed_stale_identity() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("conditional-remove").await?;
    let mut create = workspace.begin_transaction(IdempotencyKey::new()).await?;
    create.write_text("/file.txt", "current").await?;
    assert!(matches!(
        create.commit().await?,
        TransactionCommit::Committed(_)
    ));
    let actual = workspace.stat("/file.txt").await?.file_id;
    let mut stale = FileId::new();
    while stale == actual {
        stale = FileId::new();
    }
    let mut remove = workspace.begin_transaction(IdempotencyKey::new()).await?;
    assert!(matches!(
        remove.remove_if("/file.txt", stale).await,
        Err(WorkspaceError::StaleIdentity)
    ));
    assert_eq!(workspace.read("/file.txt", 64).await?.as_ref(), b"current");
    Ok(())
}

#[tokio::test]
async fn exact_generation_restore_is_fenced_and_idempotent() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("restore-exact").await?;
    workspace.write_text("/state", "first").await?;
    let first = workspace.head().await?;
    workspace.write_text("/state", "second").await?;
    let second = workspace.head().await?;
    let key = IdempotencyKey::from_bytes([0x51; 16]);

    assert!(matches!(
        workspace.restore_generation(&first, second.id(), key).await?,
        WorkspaceRestore::Restored(ref generation) if generation.id() == first.id()
    ));
    assert!(matches!(
        workspace.restore_generation(&first, second.id(), key).await?,
        WorkspaceRestore::AlreadyRestored(ref generation) if generation.id() == first.id()
    ));
    assert!(matches!(
        workspace
            .restore_generation(&second, first.id(), key)
            .await?,
        WorkspaceRestore::IdempotencyConflict
    ));
    assert!(matches!(
        workspace
            .restore_generation(&second, second.id(), IdempotencyKey::new())
            .await?,
        WorkspaceRestore::Stale(ref generation) if generation.id() == first.id()
    ));
    assert_eq!(workspace.head().await?.id(), first.id());
    Ok(())
}

#[tokio::test]
async fn path_restore_reuses_exact_records_and_preserves_unselected_state()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("restore-paths").await?;
    let mut transaction = workspace
        .begin_transaction(IdempotencyKey::from_bytes([0x60; 16]))
        .await?;
    transaction.create_dir_all("/tree").await?;
    transaction
        .create_file(
            "/tree/tracked",
            Bytes::from_static(b"committed"),
            FileMetadata::default(),
        )
        .await?;
    transaction
        .hard_link("/tree/tracked", "/tree/ignored-link")
        .await?;
    assert!(matches!(
        transaction.commit().await?,
        TransactionCommit::Committed(_)
    ));
    workspace.write_text("/ignored", "live").await?;
    let committed = workspace.head().await?;
    let snapshot = workspace
        .fork(
            "restore-paths-snapshot",
            ForkOptions::from_generation(committed, IdempotencyKey::from_bytes([0x61; 16])),
        )
        .await?;
    snapshot.remove("/ignored").await?;
    let source = snapshot.head().await?;

    workspace.write_text("/tree/tracked", "dirty").await?;
    workspace.write_text("/ignored", "still-live").await?;
    let dirty = workspace.head().await?;
    let outcome = workspace
        .restore_paths_from(
            &source,
            &["tree/tracked".to_owned()],
            dirty.id(),
            IdempotencyKey::from_bytes([0x62; 16]),
        )
        .await?;
    assert!(matches!(outcome, TransactionCommit::Committed(_)));
    assert_eq!(
        workspace.read("/tree/tracked", 64).await?,
        Bytes::from_static(b"committed")
    );
    assert_eq!(
        workspace.read("/tree/ignored-link", 64).await?,
        Bytes::from_static(b"committed")
    );
    assert_eq!(
        workspace.read("/ignored", 64).await?,
        Bytes::from_static(b"still-live")
    );
    let tracked = workspace.stat("/tree/tracked").await?;
    let linked = workspace.stat("/tree/ignored-link").await?;
    assert_eq!(tracked.file_id, linked.file_id);
    assert_eq!(tracked.link_count, 2);
    Ok(())
}

#[tokio::test]
async fn path_apply_is_three_way_conflict_checked() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("apply-paths").await?;
    workspace.write_text("/file", "base").await?;
    let base = workspace.head().await?;
    let source_workspace = workspace
        .fork(
            "apply-paths-source",
            ForkOptions::from_generation(base.clone(), IdempotencyKey::from_bytes([0x63; 16])),
        )
        .await?;
    source_workspace.write_text("/file", "source").await?;
    let source = source_workspace.head().await?;
    let current = workspace.head().await?;
    assert!(matches!(
        workspace
            .apply_paths_from(
                Some(&base),
                Some(&source),
                &["file".to_owned()],
                current.id(),
                IdempotencyKey::from_bytes([0x64; 16]),
            )
            .await?,
        WorkspacePathApply::Applied(_)
    ));
    assert_eq!(
        workspace.read("/file", 64).await?,
        Bytes::from_static(b"source")
    );

    let conflicting = fs.create_workspace("apply-paths-conflict").await?;
    conflicting.write_text("/file", "base").await?;
    let conflict_base = conflicting.head().await?;
    let conflict_source = conflicting
        .fork(
            "apply-paths-conflict-source",
            ForkOptions::from_generation(
                conflict_base.clone(),
                IdempotencyKey::from_bytes([0x65; 16]),
            ),
        )
        .await?;
    conflict_source.write_text("/file", "source").await?;
    let conflict_source = conflict_source.head().await?;
    conflicting.write_text("/file", "ours").await?;
    let conflict_current = conflicting.head().await?;
    let WorkspacePathApply::Conflicted(conflicts) = conflicting
        .apply_paths_from(
            Some(&conflict_base),
            Some(&conflict_source),
            &["file".to_owned()],
            conflict_current.id(),
            IdempotencyKey::from_bytes([0x66; 16]),
        )
        .await?
    else {
        return Err("overlapping path application did not conflict".into());
    };
    assert_eq!(conflicts.len(), 1);
    assert_eq!(conflicts[0].path, "file");
    assert_eq!(conflicts[0].kind, ConflictKind::Binary);
    Ok(())
}

#[tokio::test]
async fn repeated_join_preserves_prior_incremental_publications() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let parent = fs.create_workspace("incremental-parent").await?;
    let parent_base = parent.head().await?;
    let child = parent
        .fork(
            "incremental-child",
            ForkOptions::from_generation(
                parent_base.clone(),
                IdempotencyKey::from_bytes([0x67; 16]),
            ),
        )
        .await?;
    child.write_text("/first.txt", "first").await?;
    let first = child.join_into(&parent).plan().await?;
    assert!(matches!(
        first
            .apply(ApplyOptions {
                if_target: first.target_head(),
                idempotency_key: IdempotencyKey::from_bytes([0x68; 16]),
            })
            .await?,
        JoinOutcome::Applied(_)
    ));
    child.write_text("/second.txt", "second").await?;
    let second = child.join_into(&parent).plan().await?;
    assert!(matches!(
        second
            .apply(ApplyOptions {
                if_target: second.target_head(),
                idempotency_key: IdempotencyKey::from_bytes([0x69; 16]),
            })
            .await?,
        JoinOutcome::Applied(_)
    ));
    assert_eq!(parent.read("/first.txt", 64).await?.as_ref(), b"first");
    assert_eq!(parent.read("/second.txt", 64).await?.as_ref(), b"second");
    Ok(())
}

#[tokio::test]
async fn public_workspace_checkout_opens_pinned_and_live_private_views()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("public-checkout").await?;
    let head = workspace.head().await?;
    let pinned = workspace
        .checkout(GenerationSelector::Head, CheckoutMode::read_only_pinned())
        .await?;
    assert_eq!(pinned.generation_id(), head.id());
    let live_private = workspace
        .checkout(
            GenerationSelector::Head,
            CheckoutMode {
                access: crate::model::AccessMode::ReadWrite,
                consistency: crate::model::ConsistencyMode::Live,
                mutations: crate::model::MutationMode::PrivateOverlay,
            },
        )
        .await?;
    assert_eq!(live_private.generation_id(), head.id());
    Ok(())
}

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
#[tokio::test]
async fn public_generation_materialize_path_is_a_complete_consumer_flow()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("materialize-consumer").await?;
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
    transaction.create_dir_all("/output/nested").await?;
    transaction
        .write_text("/output/nested/status", "ready")
        .await?;
    #[cfg(unix)]
    transaction
        .set_metadata(
            "/output/nested",
            FileMetadata {
                posix_mode: crate::kernel::MetadataField::Value(0o700),
                ..FileMetadata::default()
            },
        )
        .await?;
    let TransactionCommit::Committed(generation) = transaction.commit().await? else {
        return Err("materialize fixture did not commit".into());
    };
    let destination = tempfile::tempdir()?;
    let receipt = generation
        .materialize_path(
            "/output/nested/status",
            &crate::MaterializeOptions {
                destination: destination.path().to_path_buf(),
                maximum_directory_entries: 32,
                maximum_extent_spans: 32,
                transfer_bytes: 4096,
            },
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?;
    assert_eq!(receipt.value.files, 1);
    assert_eq!(
        std::fs::read(destination.path().join("output/nested/status"))?,
        b"ready"
    );
    let directory_destination = tempfile::tempdir()?;
    generation
        .materialize_path(
            "/output/nested",
            &crate::MaterializeOptions {
                destination: directory_destination.path().to_path_buf(),
                maximum_directory_entries: 32,
                maximum_extent_spans: 32,
                transfer_bytes: 4096,
            },
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        std::fs::read(directory_destination.path().join("output/nested/status"))?,
        b"ready"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(directory_destination.path().join("output/nested"))?
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o700);
    }
    Ok(())
}

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
#[tokio::test]
async fn public_generation_materialize_paths_shares_parents_and_hard_link_identity()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("materialize-paths-consumer").await?;
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
    transaction.create_dir_all("/nested").await?;
    transaction.write_text("/nested/a", "linked").await?;
    transaction.hard_link("/nested/a", "/nested/b").await?;
    transaction.write_text("/nested/c", "sibling").await?;
    let TransactionCommit::Committed(generation) = transaction.commit().await? else {
        return Err("materialize fixture did not commit".into());
    };
    let destination = tempfile::tempdir()?;
    let receipt = generation
        .materialize_paths(
            &[
                "/nested/a".to_owned(),
                "/nested/b".to_owned(),
                "/nested/c".to_owned(),
            ],
            &crate::MaterializeOptions::native(destination.path()),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?;
    assert_eq!(receipt.value.files, 3);
    assert_eq!(
        std::fs::read(destination.path().join("nested/c"))?,
        b"sibling"
    );
    std::fs::write(destination.path().join("nested/a"), b"changed")?;
    assert_eq!(
        std::fs::read(destination.path().join("nested/b"))?,
        b"changed"
    );
    Ok(())
}

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
#[tokio::test]
async fn public_generation_restore_host_path_replaces_and_removes_exactly_one_path()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("restore-host-consumer").await?;
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
    transaction.create_dir_all("/nested").await?;
    transaction
        .write_text("/nested/file", "authenticated")
        .await?;
    let TransactionCommit::Committed(generation) = transaction.commit().await? else {
        return Err("restore fixture did not commit".into());
    };
    let destination = tempfile::tempdir()?;
    std::fs::create_dir(destination.path().join("nested"))?;
    std::fs::write(destination.path().join("nested/file"), b"stale")?;
    std::fs::write(destination.path().join("sibling"), b"untouched")?;
    let options = crate::MaterializeOptions {
        destination: destination.path().to_path_buf(),
        maximum_directory_entries: 32,
        maximum_extent_spans: 32,
        transfer_bytes: 4096,
    };
    let restored = generation
        .restore_host_path(
            Path::new("nested/file"),
            crate::HostPathReplacement::Atomic,
            &options,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?;
    assert_eq!(restored.value, crate::HostPathRestore::Restored);
    assert_eq!(
        std::fs::read(destination.path().join("nested/file"))?,
        b"authenticated"
    );
    assert_eq!(
        std::fs::read(destination.path().join("sibling"))?,
        b"untouched"
    );

    std::fs::write(destination.path().join("missing"), b"remove me")?;
    let removed = generation
        .restore_host_path(
            Path::new("missing"),
            crate::HostPathReplacement::Atomic,
            &options,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?;
    assert_eq!(removed.value, crate::HostPathRestore::Removed);
    assert!(!destination.path().join("missing").exists());
    for invalid in [Path::new(""), Path::new("../escape"), destination.path()] {
        assert!(
            generation
                .restore_host_path(
                    invalid,
                    crate::HostPathReplacement::Atomic,
                    &options,
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new(),
                )
                .await
                .is_err()
        );
    }
    Ok(())
}

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32"), unix))]
#[tokio::test]
async fn public_generation_restore_host_path_rejects_symlinked_parent() -> Result<(), Box<dyn Error>>
{
    let fs = Fs::memory();
    let workspace = fs.create_workspace("restore-host-containment").await?;
    let generation = workspace.head().await?;
    let destination = tempfile::tempdir()?;
    let outside = tempfile::tempdir()?;
    std::os::unix::fs::symlink(outside.path(), destination.path().join("escape"))?;
    let result = generation
        .restore_host_path(
            Path::new("escape/file"),
            crate::HostPathReplacement::Atomic,
            &crate::MaterializeOptions {
                destination: destination.path().to_path_buf(),
                maximum_directory_entries: 32,
                maximum_extent_spans: 32,
                transfer_bytes: 4096,
            },
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await;
    assert!(result.is_err());
    assert!(!outside.path().join("file").exists());
    Ok(())
}

#[cfg(all(feature = "native-mount", windows))]
#[tokio::test]
async fn public_generation_restore_host_path_rejects_reparse_parent_when_available()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("restore-host-containment-windows")
        .await?;
    let generation = workspace.head().await?;
    let destination = tempfile::tempdir()?;
    let outside = tempfile::tempdir()?;
    if let Err(error) =
        std::os::windows::fs::symlink_dir(outside.path(), destination.path().join("escape"))
    {
        if error.kind() == std::io::ErrorKind::PermissionDenied {
            return Ok(());
        }
        return Err(error.into());
    }
    let result = generation
        .restore_host_path(
            Path::new("escape/file"),
            crate::HostPathReplacement::Atomic,
            &crate::MaterializeOptions {
                destination: destination.path().to_path_buf(),
                maximum_directory_entries: 32,
                maximum_extent_spans: 32,
                transfer_bytes: 4096,
            },
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await;
    assert!(result.is_err());
    assert!(!outside.path().join("file").exists());
    Ok(())
}

#[tokio::test]
async fn workspace_forks_an_unpublished_checkpoint_without_mutating_its_source()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let source = fs.create_workspace("checkpoint-source").await?;
    let mut transaction = source.begin_transaction(IdempotencyKey::new()).await?;
    transaction
        .write_text("/candidate-only", "candidate")
        .await?;
    let checkpoint = transaction
        .checkout
        .checkpoint(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let unpublished = source.generation(checkpoint).await?;
    let child = source
        .fork(
            "checkpoint-child",
            ForkOptions::from_generation(unpublished, IdempotencyKey::from_bytes([0x44; 16])),
        )
        .await?;

    assert_eq!(
        child.read("/candidate-only", 64).await?,
        Bytes::from_static(b"candidate")
    );
    assert!(source.read("/candidate-only", 64).await.is_err());
    Ok(())
}

#[tokio::test]
async fn transaction_is_atomic_idempotent_and_visible_to_forks() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("repo").await?;
    let key = IdempotencyKey::new();
    let mut transaction = workspace.begin_transaction(key).await?;
    transaction.create_dir_all("/output/nested").await?;
    transaction
        .write_text("/output/nested/status", "ready")
        .await?;
    let TransactionCommit::Committed(generation) = transaction.commit().await? else {
        return Err("first publication did not commit".into());
    };
    assert_eq!(
        workspace.read("/output/nested/status", 64).await?,
        Bytes::from_static(b"ready")
    );
    let reopened = workspace.generation(generation.id()).await?;
    assert_eq!(
        reopened.read("/output/nested/status", 64).await?,
        Bytes::from_static(b"ready")
    );
    let fork = workspace
        .fork(
            "copy",
            ForkOptions::from_generation(generation.clone(), IdempotencyKey::new()),
        )
        .await?;
    assert_eq!(
        fork.read("/output/nested/status", 64).await?,
        Bytes::from_static(b"ready")
    );

    let mut exact_retry = workspace.begin_transaction(key).await?;
    exact_retry.create_dir_all("/output/nested").await?;
    exact_retry
        .write_text("/output/nested/status", "ready")
        .await?;
    assert!(matches!(
        exact_retry.commit().await?,
        TransactionCommit::AlreadyCommitted(retried) if retried.id() == generation.id()
    ));

    let mut conflicting_retry = workspace.begin_transaction(key).await?;
    conflicting_retry
        .write_text("/output/nested/status", "different")
        .await?;
    assert!(matches!(
        conflicting_retry.commit().await?,
        TransactionCommit::IdempotencyConflict
    ));
    Ok(())
}

#[tokio::test]
async fn retained_transaction_rebases_disjoint_work_and_rejects_overlap()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("transaction-rebase").await?;

    let mut first = workspace.begin_transaction(IdempotencyKey::new()).await?;
    let mut disjoint = workspace.begin_transaction(IdempotencyKey::new()).await?;
    first.write_text("/first", "one").await?;
    disjoint.write_text("/second", "two").await?;
    assert!(matches!(
        first.commit().await?,
        TransactionCommit::Committed(_)
    ));
    assert!(matches!(
        disjoint.commit().await?,
        TransactionCommit::Conflict { .. }
    ));
    assert!(matches!(
        disjoint.rebase(16).await?,
        TransactionRebase::Rebased(_)
    ));
    assert!(matches!(
        disjoint.commit().await?,
        TransactionCommit::Committed(_)
    ));
    assert_eq!(
        workspace.read("/first", 8).await?,
        Bytes::from_static(b"one")
    );
    assert_eq!(
        workspace.read("/second", 8).await?,
        Bytes::from_static(b"two")
    );

    let mut winner = workspace.begin_transaction(IdempotencyKey::new()).await?;
    let mut overlap = workspace.begin_transaction(IdempotencyKey::new()).await?;
    winner.write_text("/first", "winner").await?;
    overlap.write_text("/first", "loser").await?;
    assert!(matches!(
        winner.commit().await?,
        TransactionCommit::Committed(_)
    ));
    assert!(matches!(
        overlap.commit().await?,
        TransactionCommit::Conflict { .. }
    ));
    let TransactionRebase::Conflicted {
        conflicts,
        truncated,
    } = overlap.rebase(16).await?
    else {
        return Err("overlapping mutation rebased unsafely".into());
    };
    assert!(!conflicts.is_empty());
    assert!(!truncated);
    assert_eq!(
        workspace.read("/first", 8).await?,
        Bytes::from_static(b"winner")
    );
    Ok(())
}

#[tokio::test]
async fn exact_generation_transaction_is_stateless_and_rebases_safely() -> Result<(), Box<dyn Error>>
{
    let fs = Fs::memory();
    let workspace = fs.create_workspace("exact-transaction").await?;
    let base = workspace.head().await?;

    workspace.write_text("/upstream", "one").await?;
    let mut transaction = workspace
        .begin_transaction_at(&base, IdempotencyKey::new())
        .await?;
    transaction.write_text("/local", "two").await?;
    assert!(matches!(
        transaction.commit().await?,
        TransactionCommit::Conflict { .. }
    ));
    assert!(matches!(
        transaction.rebase(16).await?,
        TransactionRebase::Rebased(_)
    ));
    assert!(matches!(
        transaction.commit().await?,
        TransactionCommit::Committed(_)
    ));
    assert_eq!(
        workspace.read("/upstream", 8).await?,
        Bytes::from_static(b"one")
    );
    assert_eq!(
        workspace.read("/local", 8).await?,
        Bytes::from_static(b"two")
    );

    let other = fs.create_workspace("other-exact-transaction").await?;
    let foreign = other.head().await?;
    assert!(matches!(
        workspace
            .begin_transaction_at(&foreign, IdempotencyKey::new())
            .await,
        Err(WorkspaceError::ForeignGeneration)
    ));
    Ok(())
}

#[tokio::test]
async fn transaction_preserves_links_sparse_ranges_metadata_and_cow_clones()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("shapes").await?;
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
    transaction.create_directory("/tree").await?;
    transaction
        .write("/tree/source", Bytes::from_static(b"abcdef"))
        .await?;
    transaction
        .write("/tree/destination", Bytes::from_static(b"......"))
        .await?;
    transaction
        .create_symbolic_link("/tree/symlink", Bytes::from_static(b"source"))
        .await?;
    transaction
        .hard_link("/tree/source", "/tree/hard-link")
        .await?;
    transaction
        .write_range("/tree/source", 1, Bytes::from_static(b"Z"))
        .await?;
    transaction
        .zero_range(
            "/tree/source",
            crate::ByteRange {
                offset: 2,
                length: 2,
            },
            false,
            false,
        )
        .await?;
    transaction
        .preallocate(
            "/tree/source",
            crate::ByteRange {
                offset: 0,
                length: 6,
            },
            true,
        )
        .await?;
    transaction
        .clone_range("/tree/source", 0, "/tree/destination", 1, 5)
        .await?;
    transaction
        .set_metadata("/tree/source", FileMetadata::default())
        .await?;
    transaction.resize("/tree/destination", 8).await?;
    assert!(matches!(
        transaction.commit().await?,
        TransactionCommit::Committed(_)
    ));
    let source = Bytes::from_static(b"aZ\0\0ef");
    assert_eq!(workspace.read("/tree/source", 32).await?, source);
    assert_eq!(workspace.read("/tree/hard-link", 32).await?, source);
    assert_eq!(
        workspace.read("/tree/destination", 32).await?,
        Bytes::from_static(b".aZ\0\0e\0\0")
    );
    Ok(())
}

#[tokio::test]
async fn customer_reads_are_bounded_sparse_link_aware_and_generation_exact()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("read-shapes").await?;
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
    transaction.create_directory("/tree").await?;
    transaction
        .write("/tree/a", Bytes::from_static(b"abcdef"))
        .await?;
    transaction
        .write("/tree/sparse", Bytes::from(vec![1; 300]))
        .await?;
    transaction.hard_link("/tree/a", "/tree/b").await?;
    transaction
        .create_symbolic_link("/tree/link", Bytes::from_static(b"a"))
        .await?;
    transaction
        .zero_range(
            "/tree/a",
            crate::ByteRange {
                offset: 2,
                length: 2,
            },
            false,
            false,
        )
        .await?;
    transaction.resize("/tree/a", 8).await?;
    transaction
        .zero_range(
            "/tree/sparse",
            crate::ByteRange {
                offset: 100,
                length: 20,
            },
            true,
            false,
        )
        .await?;
    transaction.resize("/tree/sparse", 400).await?;
    let TransactionCommit::Committed(exact) = transaction.commit().await? else {
        return Err("shape publication did not commit".into());
    };

    let file_id = workspace.stat("/tree/a").await?.file_id;
    let directory_id = workspace.stat("/tree").await?.file_id;
    let missing_id = FileId::from_bytes([0xff; 16]);
    let paths = exact
        .paths_for_file_ids([file_id, directory_id, missing_id], 32)
        .await?;
    assert_eq!(
        paths.get(&file_id),
        Some(&vec!["/tree/a".to_owned(), "/tree/b".to_owned()])
    );
    assert_eq!(paths.get(&directory_id), Some(&vec!["/tree".to_owned()]));
    assert!(!paths.contains_key(&missing_id));

    assert_eq!(
        workspace.read_range("/tree/a", 1, 5).await?,
        Bytes::from_static(b"b\0\0ef")
    );
    let a = workspace.stat("/tree/a").await?;
    let b = workspace.stat("/tree/b").await?;
    assert_eq!(a.file_id, b.file_id);
    assert_eq!(a.link_count, 2);
    assert_eq!(a.logical_bytes, Some(8));
    assert_eq!(a.kind, FileKind::Regular);
    assert_eq!(
        workspace.read_symbolic_link("/tree/link").await?,
        Bytes::from_static(b"a")
    );

    let first = workspace.list_directory("/tree", None, 1).await?;
    assert_eq!(first.entries.len(), 1);
    assert!(first.has_more);
    let second = workspace
        .list_directory("/tree", Some(&first.entries[0].name), 16)
        .await?;
    assert_eq!(second.entries.len(), 3);
    assert!(!second.has_more);

    let extents = workspace.plan_extents("/tree/sparse", 0, 400, 16).await?;
    assert!(
        extents
            .spans
            .iter()
            .any(|span| span.kind == WorkspaceExtentKind::AllocatedZero)
    );
    assert!(
        extents
            .spans
            .iter()
            .any(|span| span.kind == WorkspaceExtentKind::Content)
    );

    workspace.write_text("/tree/a", "changed").await?;
    assert_eq!(
        exact.read("/tree/a", 16).await?,
        Bytes::from_static(b"ab\0\0ef\0\0")
    );
    assert_eq!(
        workspace.read("/tree/a", 16).await?,
        Bytes::from_static(b"changed")
    );
    Ok(())
}

#[tokio::test]
async fn checkpoint_retains_the_exact_current_generation() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("repo").await?;
    let checkpoint = workspace.checkpoint("baseline").await?;
    assert_eq!(checkpoint.label().as_str(), "baseline");
    assert_eq!(checkpoint.generation().id(), workspace.head().await?.id());
    let retried = workspace.checkpoint("baseline").await?;
    assert_eq!(retried.generation().id(), checkpoint.generation().id());
    Ok(())
}

#[tokio::test]
async fn explicit_pin_is_exact_and_conflicting_reuse_fails_closed() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("repo").await?;
    let first = workspace.head().await?;
    let pin = first.pin("deployment").await?;
    assert_eq!(pin.identity().as_str(), "deployment");
    assert_eq!(pin.generation().id(), first.id());
    assert_eq!(first.pin("deployment").await?.generation().id(), first.id());

    let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
    transaction.write_text("/changed", "yes").await?;
    let TransactionCommit::Committed(second) = transaction.commit().await? else {
        return Err("second generation did not commit".into());
    };
    assert!(matches!(
        second.pin("deployment").await,
        Err(WorkspaceError::RetentionConflict)
    ));
    Ok(())
}

#[tokio::test]
async fn deletion_is_terminal_and_does_not_invalidate_a_fork() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("repo").await?;
    let generation = workspace.head().await?;
    let fork = workspace
        .fork(
            "survivor",
            ForkOptions::from_generation(generation, IdempotencyKey::new()),
        )
        .await?;
    let key = IdempotencyKey::new();
    assert_eq!(workspace.delete(key).await?, WorkspaceDelete::Deleted);
    assert_eq!(
        workspace.delete(key).await?,
        WorkspaceDelete::AlreadyDeleted
    );
    assert_eq!(
        fs.delete_workspace("repo", key).await?,
        WorkspaceDelete::AlreadyDeleted
    );
    assert!(fs.open_workspace("repo").await.is_err());
    assert!(fs.create_workspace("repo").await.is_err());
    assert_eq!(fork.head().await?.workspace_id(), fork.id());
    Ok(())
}

#[tokio::test]
async fn side_effect_free_join_combines_independent_fork_and_target_changes()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("main").await?;
    main.write_text("/base", "base").await?;
    let base = main.head().await?;
    let agent = main
        .fork(
            "agent",
            ForkOptions::from_generation(base, IdempotencyKey::new()),
        )
        .await?;
    agent.write_text("/agent", "agent").await?;
    main.write_text("/main", "main").await?;

    let plan = agent.join_into(&main).plan().await?;
    let expected = plan.target_head();
    let idempotency_key = IdempotencyKey::new();
    assert!(main.read("/agent", 16).await.is_err());
    let outcome = plan
        .apply(ApplyOptions {
            if_target: expected,
            idempotency_key,
        })
        .await?;
    let JoinOutcome::Applied(joined) = outcome else {
        return Err("join did not publish".into());
    };
    assert_eq!(
        main.operation_generation(idempotency_key)
            .await?
            .ok_or("join operation was not recoverable")?
            .id(),
        joined.id()
    );
    assert!(
        main.operation_generation(IdempotencyKey::new())
            .await?
            .is_none()
    );
    assert_eq!(joined.read("/base", 16).await?, Bytes::from_static(b"base"));
    assert_eq!(main.read("/agent", 16).await?, Bytes::from_static(b"agent"));
    assert_eq!(main.read("/main", 16).await?, Bytes::from_static(b"main"));
    assert_eq!(
        agent.read("/agent", 16).await?,
        Bytes::from_static(b"agent")
    );
    assert!(matches!(
        plan.apply(ApplyOptions {
            if_target: expected,
            idempotency_key,
        })
        .await?,
        JoinOutcome::AlreadyApplied(_)
    ));
    Ok(())
}

#[tokio::test]
async fn bounded_common_ancestor_discovery_is_direction_independent_at_the_frontier()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("lineage-main").await?;
    let base = main.head().await?;
    let left = main
        .fork(
            "lineage-left",
            ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
        )
        .await?;
    let right = main
        .fork(
            "lineage-right",
            ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
        )
        .await?;

    assert_eq!(
        main.join_into(&left)
            .bounds(1, 32, 32)
            .plan()
            .await?
            .common_ancestor(),
        base.id()
    );
    assert_eq!(
        left.join_into(&main)
            .bounds(1, 32, 32)
            .plan()
            .await?
            .common_ancestor(),
        base.id()
    );
    assert!(matches!(
        left.join_into(&right).bounds(1, 32, 32).plan().await,
        Err(WorkspaceError::LineageLimit)
    ));
    assert!(matches!(
        right.join_into(&left).bounds(1, 32, 32).plan().await,
        Err(WorkspaceError::LineageLimit)
    ));

    let unrelated = fs.create_workspace("lineage-unrelated").await?;
    assert!(matches!(
        main.join_into(&unrelated).bounds(1, 32, 32).plan().await,
        Err(WorkspaceError::NoCommonAncestor)
    ));
    assert!(matches!(
        unrelated.join_into(&main).bounds(1, 32, 32).plan().await,
        Err(WorkspaceError::NoCommonAncestor)
    ));
    assert!(matches!(
        main.join_into(&left).bounds(0, 32, 32).plan().await,
        Err(WorkspaceError::JoinLimit)
    ));
    Ok(())
}

#[tokio::test]
async fn live_rebase_advances_fork_lineage_and_preserves_independent_changes()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("main-rebase").await?;
    main.write_text("/base", "base").await?;
    let agent = main
        .fork(
            "agent-rebase",
            ForkOptions::from_generation(main.head().await?, IdempotencyKey::new()),
        )
        .await?;
    agent.write_text("/agent", "local").await?;
    main.write_text("/upstream", "first").await?;

    let first_key = IdempotencyKey::new();
    let WorkspaceRebase::Rebased(first) = agent.live_rebase(first_key, 128, 128, 32).await? else {
        return Err("first live rebase did not publish".into());
    };
    assert_eq!(
        first.read("/agent", 16).await?,
        Bytes::from_static(b"local")
    );
    assert_eq!(
        first.read("/upstream", 16).await?,
        Bytes::from_static(b"first")
    );
    assert!(matches!(
        agent.live_rebase(first_key, 128, 128, 32).await?,
        WorkspaceRebase::Current(_)
    ));

    main.write_text("/later", "second").await?;
    assert!(matches!(
        agent
            .live_rebase(IdempotencyKey::new(), 128, 128, 32)
            .await?,
        WorkspaceRebase::Rebased(_)
    ));
    assert_eq!(
        agent.read("/later", 16).await?,
        Bytes::from_static(b"second")
    );
    assert_eq!(
        agent.read("/agent", 16).await?,
        Bytes::from_static(b"local")
    );
    Ok(())
}

#[tokio::test]
async fn live_rebase_conflict_and_non_fork_leave_heads_unchanged() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("main-conflict").await?;
    main.write_text("/shared", "base").await?;
    let agent = main
        .fork(
            "agent-conflict",
            ForkOptions::from_generation(main.head().await?, IdempotencyKey::new()),
        )
        .await?;
    agent.write_text("/shared", "local").await?;
    let before = agent.head().await?.id();
    main.write_text("/shared", "upstream").await?;
    assert!(matches!(
        agent
            .live_rebase(IdempotencyKey::new(), 128, 128, 32)
            .await?,
        WorkspaceRebase::Conflicted {
            truncated: false,
            ..
        }
    ));
    assert_eq!(agent.head().await?.id(), before);
    assert_eq!(
        agent.read("/shared", 16).await?,
        Bytes::from_static(b"local")
    );
    assert!(matches!(
        main.live_rebase(IdempotencyKey::new(), 128, 128, 32).await,
        Err(WorkspaceError::NotFork)
    ));
    Ok(())
}

#[tokio::test]
async fn live_rebase_merges_disjoint_sparse_ranges_without_materializing_file()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("main-ranges").await?;
    main.write("/shared", Bytes::from(vec![b'a'; 128])).await?;
    let agent = main
        .fork(
            "agent-ranges",
            ForkOptions::from_generation(main.head().await?, IdempotencyKey::new()),
        )
        .await?;
    let mut local = agent.begin_transaction(IdempotencyKey::new()).await?;
    local
        .write_range("/shared", 1, Bytes::from_static(b"XY"))
        .await?;
    assert!(matches!(
        local.commit().await?,
        TransactionCommit::Committed(_)
    ));
    let mut upstream = main.begin_transaction(IdempotencyKey::new()).await?;
    upstream
        .write_range("/shared", 64, Bytes::from_static(b"UV"))
        .await?;
    assert!(matches!(
        upstream.commit().await?,
        TransactionCommit::Committed(_)
    ));
    assert!(matches!(
        agent
            .live_rebase(IdempotencyKey::new(), 128, 128, 32)
            .await?,
        WorkspaceRebase::Rebased(_)
    ));
    let merged = agent.read("/shared", 128).await?;
    assert_eq!(&merged[1..3], b"XY");
    assert_eq!(&merged[64..66], b"UV");
    assert!(
        merged[..1]
            .iter()
            .chain(&merged[3..64])
            .all(|byte| *byte == b'a')
    );
    assert!(merged[66..].iter().all(|byte| *byte == b'a'));
    Ok(())
}

#[tokio::test]
async fn join_rejects_a_stale_target_without_mutation() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("main").await?;
    let base = main.head().await?;
    let agent = main
        .fork(
            "agent",
            ForkOptions::from_generation(base, IdempotencyKey::new()),
        )
        .await?;
    agent.write_text("/agent", "agent").await?;
    let plan = agent.join_into(&main).plan().await?;
    let expected = plan.target_head();
    main.write_text("/raced", "winner").await?;
    let outcome = plan
        .apply(ApplyOptions {
            if_target: expected,
            idempotency_key: IdempotencyKey::new(),
        })
        .await?;
    assert!(matches!(outcome, JoinOutcome::StaleTarget(_)));
    assert!(main.read("/agent", 16).await.is_err());
    assert_eq!(
        main.read("/raced", 16).await?,
        Bytes::from_static(b"winner")
    );
    Ok(())
}

#[tokio::test]
async fn semantic_change_set_and_overlap_conflict_are_exact() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("main").await?;
    main.write_text("/shared", "base").await?;
    let base = main.head().await?;
    let agent = main
        .fork(
            "agent",
            ForkOptions::from_generation(base, IdempotencyKey::new()),
        )
        .await?;
    let agent_base = agent.head().await?;
    agent.write_text("/shared", "agent").await?;
    let agent_head = agent.head().await?;
    let changes = agent.diff(&agent_base, &agent_head, 32).await?;
    assert_eq!(changes.from().id(), agent_base.id());
    assert_eq!(changes.to().id(), agent_head.id());
    assert!(!changes.changes().files.is_empty());

    main.write_text("/shared", "main").await?;
    let plan = agent.join_into(&main).plan().await?;
    assert!(matches!(
        plan.apply(ApplyOptions {
            if_target: plan.target_head(),
            idempotency_key: IdempotencyKey::new(),
        })
        .await?,
        JoinOutcome::Conflicted {
            truncated: false,
            ..
        }
    ));
    assert_eq!(main.read("/shared", 16).await?, Bytes::from_static(b"main"));
    Ok(())
}

struct SelectTheirsDriver;

impl crate::MergeDriver for SelectTheirsDriver {
    fn fingerprint(&self) -> &[u8] {
        b"workspace-select-theirs-v1"
    }

    fn mode(&self) -> crate::MergeDriverMode {
        crate::MergeDriverMode::Deterministic
    }

    fn resolve(
        &self,
        conflict: &crate::ConflictView,
    ) -> Result<crate::MergeResolution, crate::DriverError> {
        assert_eq!(conflict.kind, crate::ConflictKind::Text);
        assert_eq!(conflict.base, crate::ConflictValue::Text("base".into()));
        assert_eq!(conflict.ours, crate::ConflictValue::Text("main".into()));
        assert_eq!(conflict.theirs, crate::ConflictValue::Text("agent".into()));
        Ok(crate::MergeResolution::Select(crate::ConflictSide::Theirs))
    }
}

#[tokio::test]
async fn merge_drivers_resolve_and_publish_real_workspace_joins() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("driver-main").await?;
    main.write_text("/shared", "base").await?;
    let agent = main
        .fork(
            "driver-agent",
            ForkOptions::from_generation(main.head().await?, IdempotencyKey::new()),
        )
        .await?;
    agent.write_text("/shared", "agent").await?;
    main.write_text("/shared", "main").await?;

    let plan = agent.join_into(&main).plan().await?;
    let mut registry = crate::MergeDriverRegistry::new();
    registry.register("theirs", Arc::new(SelectTheirsDriver))?;
    registry.set_default("theirs")?;
    let mut cache = crate::MemoryMergeResolutionCache::default();
    let outcome = plan
        .apply_with_drivers(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new(),
            },
            &registry,
            &mut cache,
            false,
        )
        .await?;
    assert!(matches!(outcome, JoinOutcome::Applied(_)));
    assert_eq!(
        main.read("/shared", 16).await?,
        Bytes::from_static(b"agent")
    );
    Ok(())
}

#[tokio::test]
async fn default_text_driver_projects_markers_and_publishes_them() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs.create_workspace("text-driver-main").await?;
    main.write_text("/shared", "base\n").await?;
    let agent = main
        .fork(
            "text-driver-agent",
            ForkOptions::from_generation(main.head().await?, IdempotencyKey::new()),
        )
        .await?;
    agent.write_text("/shared", "agent\n").await?;
    main.write_text("/shared", "main\n").await?;

    let plan = agent.join_into(&main).plan().await?;
    let JoinOutcome::Conflicted {
        conflicts,
        truncated,
    } = plan
        .apply(ApplyOptions {
            if_target: plan.target_head(),
            idempotency_key: IdempotencyKey::new(),
        })
        .await?
    else {
        return Err("text join did not expose its conflict".into());
    };
    let described = plan.describe_conflicts(&conflicts, truncated).await?;
    assert_eq!(described.conflicts.len(), 1);
    assert_eq!(described.conflicts[0].path.as_deref(), Some("/shared"));
    assert_eq!(described.conflicts[0].kind, crate::ConflictKind::Text);
    assert!(matches!(
        described.conflicts[0].ours,
        crate::ConflictValue::Text(ref text) if text == "main\n"
    ));
    let mut registry = crate::MergeDriverRegistry::new();
    registry.register("text", Arc::new(crate::DefaultTextMergeDriver))?;
    registry.set_default("text")?;
    let mut cache = crate::MemoryMergeResolutionCache::default();
    let outcome = plan
        .apply_with_drivers(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new(),
            },
            &registry,
            &mut cache,
            false,
        )
        .await?;
    assert!(matches!(outcome, JoinOutcome::Applied(_)));
    let merged = String::from_utf8(main.read("/shared", 1024).await?.to_vec())?;
    assert!(merged.contains("<<<<<<< ours"));
    assert!(merged.contains("main"));
    assert!(merged.contains("agent"));
    Ok(())
}

#[tokio::test]
async fn contiguous_change_sets_compose_by_outer_semantics() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("compose").await?;
    let base = workspace.head().await?;
    workspace.write_text("/before", "body").await?;
    let middle = workspace.head().await?;
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
    transaction.rename("/before", "/after").await?;
    let TransactionCommit::Committed(end) = transaction.commit().await? else {
        return Err("rename did not commit".into());
    };

    let first = workspace.diff(&base, &middle, 32).await?;
    let second = workspace.diff(&middle, &end, 32).await?;
    let composed = first.compose(&second, 32).await?;
    let direct = workspace.diff(&base, &end, 32).await?;
    assert_eq!(composed.from().id(), base.id());
    assert_eq!(composed.to().id(), end.id());
    assert_eq!(composed.changes(), direct.changes());
    assert!(matches!(
        second.compose(&first, 32).await,
        Err(WorkspaceError::ChangeSetContinuity)
    ));
    Ok(())
}

#[tokio::test]
async fn change_set_resolves_only_changed_bindings_to_portable_paths() -> Result<(), Box<dyn Error>>
{
    let fs = Fs::memory();
    let workspace = fs.create_workspace("changed-paths").await?;
    let mut initial = workspace.begin_transaction(IdempotencyKey::new()).await?;
    initial.create_dir_all("/nested/deep").await?;
    initial
        .write("/rename", Bytes::from_static(b"same"))
        .await?;
    initial
        .write("/remove", Bytes::from_static(b"gone"))
        .await?;
    initial
        .write("/linked", Bytes::from_static(b"links"))
        .await?;
    initial
        .write("/modified", Bytes::from_static(b"before"))
        .await?;
    assert!(matches!(
        initial.commit().await?,
        TransactionCommit::Committed(_)
    ));
    let before = workspace.head().await?;

    let mut changed = workspace.begin_transaction(IdempotencyKey::new()).await?;
    changed.rename("/rename", "/renamed").await?;
    changed.remove("/remove").await?;
    changed.hard_link("/linked", "/nested/link").await?;
    changed
        .write("/nested/deep/added", Bytes::from_static(b"new"))
        .await?;
    changed
        .write("/modified", Bytes::from_static(b"after"))
        .await?;
    let TransactionCommit::Committed(after) = changed.commit().await? else {
        return Err("changed paths did not commit".into());
    };

    let paths = before.diff_to(&after, 64).await?.changed_paths(64).await?;
    let names: Vec<_> = paths
        .iter()
        .map(|change| super::namespace_path_text(&change.path))
        .collect::<Result<_, _>>()?;
    assert_eq!(
        names,
        [
            "/modified",
            "/nested/deep/added",
            "/nested/link",
            "/remove",
            "/rename",
            "/renamed"
        ]
    );
    assert!(paths.iter().any(|change| {
        super::namespace_path_text(&change.path).is_ok_and(|path| path == "/remove")
            && change.before.is_some()
            && change.after.is_none()
    }));
    assert!(paths.iter().any(|change| {
        super::namespace_path_text(&change.path).is_ok_and(|path| path == "/nested/deep/added")
            && change.before.is_none()
            && change.after.is_some()
    }));
    assert!(paths.iter().any(|change| {
        super::namespace_path_text(&change.path).is_ok_and(|path| path == "/modified")
            && change.before.is_some()
            && change.after.is_some()
            && change.before != change.after
    }));
    assert!(!paths.iter().any(|change| {
        super::namespace_path_text(&change.path).is_ok_and(|path| path == "/linked")
    }));
    assert!(matches!(
        before.diff_to(&after, 64).await?.changed_paths(1).await,
        Err(WorkspaceError::ChangedPathLimit)
    ));
    let exact = before.diff_to(&after, 64).await?;
    let mut truncated_changes = exact.changes().clone();
    truncated_changes.truncated = true;
    let truncated = ChangeSet {
        from: before.clone(),
        to: after.clone(),
        changes: truncated_changes,
        work: exact.work(),
    };
    assert!(matches!(
        truncated.changed_paths(64).await,
        Err(WorkspaceError::ChangedPathLimit)
    ));
    assert!(
        after
            .diff_to(&after, 1)
            .await?
            .changed_paths(0)
            .await?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn changed_path_queries_reuse_the_sdk_generation_index() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("changed-path-index").await?;
    for batch in 0..8 {
        let mut initial = workspace.begin_transaction(IdempotencyKey::new()).await?;
        for offset in 0..16 {
            let index = batch * 16 + offset;
            initial
                .write(
                    &format!("/unrelated-{index:04}"),
                    Bytes::from_static(b"unchanged"),
                )
                .await?;
        }
        assert!(matches!(
            initial.commit().await?,
            TransactionCommit::Committed(_)
        ));
    }
    let mut initial = workspace.begin_transaction(IdempotencyKey::new()).await?;
    initial
        .write("/selected", Bytes::from_static(b"before"))
        .await?;
    assert!(matches!(
        initial.commit().await?,
        TransactionCommit::Committed(_)
    ));
    let before = workspace.head().await?;
    workspace.write_text("/selected", "after").await?;
    let after = workspace.head().await?;
    let changes = workspace.diff(&before, &after, 1_024).await?;
    let cancellation = crate::CancellationToken::new();
    let cold = changes
        .changed_paths_bounded(1_024, crate::WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    let warm = changes
        .changed_paths_bounded(1_024, crate::WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    assert_eq!(warm.value, cold.value);
    assert!(warm.work.page_reads < cold.work.page_reads);
    assert!(warm.work.items_examined < cold.work.items_examined);
    Ok(())
}

#[tokio::test]
async fn generation_lookup_paths_preserves_order_absence_and_duplicates()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("lookup-paths").await?;
    workspace.write_text("/present", "body").await?;
    let generation = workspace.head().await?;
    let config = crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral);
    let present = customer_path("/present", config)?;
    let absent = customer_path("/absent", config)?;
    let records = generation
        .lookup_paths(
            &[present.clone(), absent, present.clone()],
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
    assert_eq!(records.value.len(), 3);
    assert!(records.value[0].is_some());
    assert!(records.value[1].is_none());
    assert_eq!(records.value[0], records.value[2]);

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let cancelled_failure = generation
        .lookup_paths(
            std::slice::from_ref(&present),
            WorkBudget::UNBOUNDED,
            &cancelled,
        )
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("pre-cancelled lookup must fail"))?;
    assert_eq!(*cancelled_failure.work, crate::WorkCounters::default());
    let budget_failure = generation
        .lookup_paths(&[present], WorkBudget::default(), &CancellationToken::new())
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("zero-budget checkout must fail"))?;
    assert_ne!(*budget_failure.work, crate::WorkCounters::default());
    assert!(budget_failure.work.verify(WorkBudget::default()).is_err());
    Ok(())
}

#[tokio::test]
async fn bounded_path_index_hit_never_substitutes_a_false_empty_result()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("bounded-path-index").await?;
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
    transaction.write_text("/a", "body").await?;
    transaction.hard_link("/a", "/b").await?;
    assert!(matches!(
        transaction.commit().await?,
        TransactionCommit::Committed(_)
    ));
    let generation = workspace.head().await?;
    let file_id = generation.stat("/a").await?.file_id;

    let complete = generation
        .namespace_records_for_file_ids([file_id], 16)
        .await?;
    assert!(complete.complete);
    assert_eq!(complete.records.get(&file_id).map(Vec::len), Some(2));

    let bounded = generation
        .namespace_records_for_file_ids([file_id], 1)
        .await?;
    assert!(!bounded.complete);
    assert_eq!(bounded.records.get(&file_id).map(Vec::len), Some(1));
    Ok(())
}

#[tokio::test]
async fn generation_reader_resolves_many_paths_from_one_pinned_root() -> Result<(), Box<dyn Error>>
{
    let fs = Fs::memory();
    let workspace = fs.create_workspace("generation-reader").await?;
    workspace.write_text("/present", "body").await?;
    let generation = workspace.head().await?;
    let config = crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral);
    let present = customer_path("/present", config)?;
    let absent = customer_path("/absent", config)?;

    let reader = generation.reader().await?;
    let descriptions = reader
        .describe_files(
            &[present, absent],
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;

    assert_eq!(descriptions.len(), 2);
    assert_eq!(
        descriptions[0].as_ref().map(|file| file.logical_bytes),
        Some(4)
    );
    assert!(descriptions[1].is_none());
    Ok(())
}

/// Where a fork's provider fault lands: before a call reaches the provider,
/// or after a commit applied but before its caller learned so.
#[derive(Clone, Copy, Debug)]
enum ForkCut {
    Before,
    AfterCommit,
}

/// A Stream provider that fails its `fail_at`-th call, and counts commits.
struct CutStream {
    inner: Arc<acyclic_stream::MemoryStream>,
    calls: std::sync::atomic::AtomicUsize,
    commits: std::sync::atomic::AtomicUsize,
    fail_at: std::sync::atomic::AtomicUsize,
    cut: std::sync::Mutex<ForkCut>,
}

impl CutStream {
    fn new(inner: Arc<acyclic_stream::MemoryStream>) -> Self {
        Self {
            inner,
            calls: std::sync::atomic::AtomicUsize::new(0),
            commits: std::sync::atomic::AtomicUsize::new(0),
            fail_at: std::sync::atomic::AtomicUsize::new(usize::MAX),
            cut: std::sync::Mutex::new(ForkCut::Before),
        }
    }

    fn arm(&self, fail_at: usize, cut: ForkCut) {
        use std::sync::atomic::Ordering;
        self.calls.store(0, Ordering::SeqCst);
        self.commits.store(0, Ordering::SeqCst);
        if let Ok(mut current) = self.cut.lock() {
            *current = cut;
        }
        self.fail_at.store(fail_at, Ordering::SeqCst);
    }

    fn disarm(&self) -> bool {
        use std::sync::atomic::Ordering;
        let fired = self.calls.load(Ordering::SeqCst) >= self.fail_at.load(Ordering::SeqCst);
        self.fail_at.store(usize::MAX, Ordering::SeqCst);
        fired
    }

    fn commits(&self) -> usize {
        self.commits.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn fails_now(&self) -> bool {
        use std::sync::atomic::Ordering;
        self.calls.fetch_add(1, Ordering::SeqCst) + 1 == self.fail_at.load(Ordering::SeqCst)
    }

    fn cut(&self) -> ForkCut {
        self.cut.lock().map_or(ForkCut::Before, |cut| *cut)
    }
}

#[async_trait::async_trait]
impl acyclic_stream::StreamProvider for CutStream {
    async fn inspect_idempotency(
        &self,
        key: acyclic_stream::IdempotencyKey,
    ) -> Result<Option<acyclic_stream::IdempotencyObservation>, acyclic_stream::StreamError> {
        if self.fails_now() {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.inspect_idempotency(key).await
    }

    async fn tail(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> Result<u64, acyclic_stream::StreamError> {
        if self.fails_now() {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.tail(path).await
    }

    async fn append(
        &self,
        request: acyclic_stream::AppendRequest,
    ) -> Result<acyclic_stream::AppendOutcome, acyclic_stream::StreamError> {
        if self.fails_now() {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.append(request).await
    }

    async fn fork(
        &self,
        request: acyclic_stream::ForkRequest,
    ) -> Result<acyclic_stream::ForkReceipt, acyclic_stream::StreamError> {
        if self.fails_now() {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.fork(request).await
    }

    async fn trim(
        &self,
        path: acyclic_stream::StreamPath,
        before: u64,
        key: acyclic_stream::IdempotencyKey,
    ) -> Result<acyclic_stream::TrimReceipt, acyclic_stream::StreamError> {
        if self.fails_now() {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.trim(path, before, key).await
    }

    async fn delete(
        &self,
        path: acyclic_stream::StreamPath,
        key: acyclic_stream::IdempotencyKey,
    ) -> Result<acyclic_stream::DeleteReceipt, acyclic_stream::StreamError> {
        if self.fails_now() {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.delete(path, key).await
    }

    async fn read(
        &self,
        request: acyclic_stream::ReadRequest,
    ) -> Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> {
        if self.fails_now() {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.read(request).await
    }

    async fn follow(
        &self,
        path: acyclic_stream::StreamPath,
        from: u64,
    ) -> Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> {
        if self.fails_now() {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.follow(path, from).await
    }

    async fn children(
        &self,
        request: acyclic_stream::ChildrenRequest,
    ) -> Result<acyclic_stream::ChildStream, acyclic_stream::StreamError> {
        if self.fails_now() {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.children(request).await
    }

    async fn commit(
        &self,
        request: acyclic_stream::CommitRequest,
    ) -> Result<acyclic_stream::CommitOutcome, acyclic_stream::StreamError> {
        let fails = self.fails_now();
        if fails && matches!(self.cut(), ForkCut::Before) {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        let outcome = self.inner.commit(request).await?;
        if matches!(outcome, acyclic_stream::CommitOutcome::Committed(_)) {
            self.commits
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        if fails {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        Ok(outcome)
    }

    async fn read_commit(
        &self,
        commit_id: acyclic_stream::CommitId,
    ) -> Result<acyclic_stream::CommittedEnvelope, acyclic_stream::StreamError> {
        if self.fails_now() {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.read_commit(commit_id).await
    }
}

type CutFs = Fs<
    crate::distributed::StreamAuthorityStore<CutStream>,
    crate::distributed::ProviderObjectStore<acyclic_objects::MemoryObjects>,
>;

/// The first record of one authority, if it has one.
async fn first_fork_record(
    stream: &Arc<acyclic_stream::MemoryStream>,
    authority: crate::AuthorityId,
) -> Result<Option<crate::foundation::DurableCommit>, Box<dyn Error>> {
    use crate::AsyncAuthorityStore as _;
    let store = crate::distributed::StreamAuthorityStore::new(Arc::clone(stream));
    match store
        .replay(
            authority,
            crate::foundation::Sequence::GENESIS,
            crate::storage::ReplayLimit {
                records: 1,
                payload_bytes: 4 * 1024,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
    {
        Ok(records) => Ok(records.value.into_iter().next()),
        Err(failure) if matches!(failure.error, crate::storage::AuthorityStoreError::Missing) => {
            Ok(None)
        }
        Err(failure) => Err(failure.error.into()),
    }
}

/// Checks a fork's durable authority state after any interruption: the
/// workspace exists only with its source retained, and a record that
/// exists is exactly the fork's.
async fn assert_fork_state_exact(
    stream: &Arc<acyclic_stream::MemoryStream>,
    source: &crate::Generation<
        crate::distributed::StreamAuthorityStore<CutStream>,
        crate::distributed::ProviderObjectStore<acyclic_objects::MemoryObjects>,
    >,
    source_volume: crate::foundation::VolumeId,
    destination: WorkspaceId,
) -> Result<bool, Box<dyn Error>> {
    let retention = crate::kernel::retention_authority_id(
        source_volume,
        crate::kernel::RetentionKind::ForkBase,
        &hex::encode(destination.into_bytes()),
    );
    let retained = first_fork_record(stream, retention).await?;
    if let Some(retained) = &retained {
        let record = crate::kernel::decode_retention_created(&retained.payload, 4 * 1024)?;
        assert_eq!(record.generation_root.digest, source.id().digest());
    }
    let created = first_fork_record(
        stream,
        crate::kernel::volume_authority_id(destination.volume_id()),
    )
    .await?;
    if let Some(created) = &created {
        let record = crate::kernel::decode_volume_created(&created.payload, 4 * 1024)?;
        assert_eq!(record.volume_id, destination.volume_id());
        assert!(
            retained.is_some(),
            "a workspace became durable before its source generation was retained"
        );
    }
    Ok(created.is_some())
}

/// Forks one generation through every provider cut, before calls and after
/// commits, checking the durable state after each interruption and that a
/// retry with the same key completes the same workspace. Returns the commits
/// an uninterrupted fork made.
async fn fork_through_every_cut(advance_source: bool) -> Result<usize, Box<dyn Error>> {
    let mut uninterrupted_commits = None;
    for cut in [ForkCut::Before, ForkCut::AfterCommit] {
        for fail_at in 1.. {
            let stream = Arc::new(acyclic_stream::MemoryStream::default());
            let cutting = Arc::new(CutStream::new(Arc::clone(&stream)));
            let (objects, bucket) = acyclic_objects::MemoryObjects::with_default_bucket();
            let fs: CutFs = Fs::new(
                crate::distributed::StreamAuthorityStore::new(Arc::clone(&cutting)),
                crate::distributed::ProviderObjectStore::new(Arc::new(objects), bucket),
                crate::EmbeddedCapabilities::MEMORY,
            );
            let main = fs.create_workspace("repo").await?;
            main.write_text("/base.txt", "base").await?;
            let source = main.head().await?;
            if advance_source {
                main.write_text("/later.txt", "later").await?;
            }
            let key = IdempotencyKey::from_bytes([0x51; 16]);
            cutting.arm(fail_at, cut);
            let attempt = main
                .fork("agent", ForkOptions::from_generation(source.clone(), key))
                .await;
            let fired = cutting.disarm();
            let committed = cutting.commits();
            let destination = fs.workspace_id("agent")?;
            let complete =
                assert_fork_state_exact(&stream, &source, main.id().volume_id(), destination)
                    .await?;
            if !fired {
                attempt?;
                assert!(complete);
                uninterrupted_commits.get_or_insert(committed);
                break;
            }
            assert!(
                attempt.is_err(),
                "a fault at call {fail_at} was not reported"
            );
            let retried = main
                .fork("agent", ForkOptions::from_generation(source.clone(), key))
                .await?;
            assert_eq!(retried.id(), destination);
            assert_eq!(
                retried.read("/base.txt", 16).await?,
                Bytes::from_static(b"base")
            );
            assert!(
                assert_fork_state_exact(&stream, &source, main.id().volume_id(), destination)
                    .await?
            );
            let again = main
                .fork("agent", ForkOptions::from_generation(source.clone(), key))
                .await?;
            assert_eq!(again.head().await?.id(), retried.head().await?.id());
        }
    }
    uninterrupted_commits.ok_or_else(|| "the fork never completed".into())
}

#[tokio::test]
async fn a_fork_of_the_head_is_exact_at_every_provider_cut() -> Result<(), Box<dyn Error>> {
    // The fork of a published head shares its lineage, so its creation
    // record follows the commit that creates both authorities.
    assert_eq!(fork_through_every_cut(false).await?, 2);
    Ok(())
}

#[tokio::test]
async fn an_independent_fork_is_one_commit_and_exact_at_every_provider_cut()
-> Result<(), Box<dyn Error>> {
    assert_eq!(fork_through_every_cut(true).await?, 1);
    Ok(())
}

/// Truncates a local root's Stream journal to `length` bytes, as a power
/// loss before the rest was synchronized would leave it.
#[cfg(all(feature = "local", any(unix, windows)))]
fn cut_journal(root: &Path, journal: &[u8], length: usize) -> std::io::Result<()> {
    std::fs::write(
        root.join("stream").join("stream.journal"),
        journal.get(..length).unwrap_or(journal),
    )
}

#[cfg(all(feature = "local", any(unix, windows)))]
async fn close_local(
    fs: crate::Fs<crate::LocalAuthorityBackend, crate::LocalObjectBackend>,
) -> Result<(), Box<dyn Error>> {
    let released = fs
        .local_root_release_barrier()
        .ok_or("local root has no release barrier")?;
    drop(fs);
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !released.is_released() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await?;
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_local_fork_survives_power_loss_at_every_journal_cut() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let root = directory.path();
    let journal_path = root.join("stream").join("stream.journal");
    let key = IdempotencyKey::from_bytes([0x61; 16]);
    let source_id;
    {
        let fs = Fs::local(crate::LocalOptions::new(root)).await?;
        let main = fs.create_workspace("repo").await?;
        main.write_text("/base.txt", "base").await?;
        source_id = main.head().await?.id();
        drop(main);
        close_local(fs).await?;
    }
    let before = std::fs::metadata(&journal_path)?.len();
    let forked_head;
    {
        let fs = Fs::local(crate::LocalOptions::new(root)).await?;
        let main = fs.open_workspace("repo").await?;
        let source = main.head().await?;
        assert_eq!(source.id(), source_id);
        let fork = main
            .fork("agent", ForkOptions::from_generation(source, key))
            .await?;
        forked_head = fork.head().await?.id();
        drop((main, fork));
        close_local(fs).await?;
    }
    let journal = std::fs::read(&journal_path)?;
    let before = usize::try_from(before)?;
    // Each frame the fork appended ends at a power-loss point, and a frame
    // torn anywhere, in its length, command, or checksum, is another. A
    // frame is a 4-byte length, the command, and a 32-byte checksum.
    let mut cuts = vec![before];
    let mut frame = before;
    while frame < journal.len() {
        let length = journal
            .get(frame..frame + 4)
            .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
            .map(u32::from_le_bytes)
            .ok_or("fork journal frame is truncated")?;
        let end = frame + 4 + usize::try_from(length)? + 32;
        cuts.extend([
            frame + 2,
            frame + 4 + usize::try_from(length)? / 2,
            end - 1,
            end,
        ]);
        frame = end;
    }
    assert_eq!(frame, journal.len(), "fork frames do not end the journal");
    // The fork of the published head is two commits: both authorities,
    // then the creation record.
    assert_eq!(cuts.len(), 1 + 4 * 2, "the fork did not append two frames");
    for length in cuts {
        cut_journal(root, &journal, length)?;
        let fs = Fs::local(crate::LocalOptions::new(root)).await?;
        let main = fs.open_workspace("repo").await?;
        let source = main.head().await?;
        assert_eq!(source.id(), source_id);
        match fs.open_workspace("agent").await {
            Ok(agent) => {
                assert_eq!(agent.head().await?.id(), forked_head);
                assert_eq!(
                    agent.read("/base.txt", 16).await?,
                    Bytes::from_static(b"base")
                );
            }
            Err(_) => {}
        }
        let retried = main
            .fork("agent", ForkOptions::from_generation(source, key))
            .await?;
        assert_eq!(retried.head().await?.id(), forked_head);
        assert_eq!(
            retried.read("/base.txt", 16).await?,
            Bytes::from_static(b"base")
        );
        drop((main, retried));
        close_local(fs).await?;
    }
    Ok(())
}
