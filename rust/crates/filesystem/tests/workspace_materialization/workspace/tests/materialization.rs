#[cfg(unix)]
use acyclic_fs::kernel::{FileMetadata, MetadataField};
use acyclic_fs::{
    CancellationToken, Fs, HostPathReplacement, HostPathRestore, IdempotencyKey,
    MaterializeOptions, TransactionCommit,
};
use acyclic_fs_work::performance::WorkBudget;
use std::{error::Error, path::Path};

use crate::selected_budget::selected_transaction_budget;
use crate::transaction::commit;

#[tokio::test]
async fn public_generation_materialize_path_is_a_complete_consumer_flow()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("materialize-consumer"))?;
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(transaction, create_dir_all("/output/nested"))?;
    transaction
        .write_text("/output/nested/status", "ready")
        .await?;
    #[cfg(unix)]
    unbounded!(
        transaction,
        set_metadata(
            "/output/nested",
            FileMetadata {
                posix_mode: MetadataField::Value(0o700),
                ..FileMetadata::default()
            }
        )
    )?;
    let TransactionCommit::Committed(generation) = commit(&mut transaction).await? else {
        return Err("materialize fixture did not commit".into());
    };
    let destination = tempfile::tempdir()?;
    let receipt = generation
        .materialize(
            std::iter::once("/output/nested/status"),
            &MaterializeOptions {
                materialized_view: None,
                destination: destination.path().to_path_buf(),
                expected_root_identity: None,
                maximum_directory_entries: 32,
                maximum_extent_spans: Some(32),
                transfer_bytes: 4096,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
    assert_eq!(receipt.value.files, 1);
    assert_eq!(
        std::fs::read(destination.path().join("output/nested/status"))?,
        b"ready"
    );
    let directory_destination = tempfile::tempdir()?;
    generation
        .materialize(
            std::iter::once("/output/nested"),
            &MaterializeOptions {
                materialized_view: None,
                destination: directory_destination.path().to_path_buf(),
                expected_root_identity: None,
                maximum_directory_entries: 32,
                maximum_extent_spans: Some(32),
                transfer_bytes: 4096,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
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

#[tokio::test]
async fn public_generation_materialize_paths_shares_parents_and_hard_link_identity()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("materialize-paths-consumer"))?;
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(transaction, create_dir_all("/nested"))?;
    transaction.write_text("/nested/a", "linked").await?;
    unbounded!(transaction, hard_link("/nested/a", "/nested/b"))?;
    transaction.write_text("/nested/c", "sibling").await?;
    let TransactionCommit::Committed(generation) = commit(&mut transaction).await? else {
        return Err("materialize fixture did not commit".into());
    };
    let destination = tempfile::tempdir()?;
    let receipt = generation
        .materialize(
            ["/nested/a", "/nested/b", "/nested/c"].into_iter(),
            &MaterializeOptions::native(destination.path()),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
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

#[tokio::test]
async fn public_generation_restore_host_path_replaces_and_removes_exactly_one_path()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("restore-host-consumer"))?;
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(transaction, create_dir_all("/nested"))?;
    transaction
        .write_text("/nested/file", "authenticated")
        .await?;
    let TransactionCommit::Committed(generation) = commit(&mut transaction).await? else {
        return Err("restore fixture did not commit".into());
    };
    let destination = tempfile::tempdir()?;
    std::fs::create_dir(destination.path().join("nested"))?;
    std::fs::write(destination.path().join("nested/file"), b"stale")?;
    std::fs::write(destination.path().join("sibling"), b"untouched")?;
    let options = MaterializeOptions {
        materialized_view: None,
        destination: destination.path().to_path_buf(),
        expected_root_identity: None,
        maximum_directory_entries: 32,
        maximum_extent_spans: Some(32),
        transfer_bytes: 4096,
    };
    let restored = generation
        .restore_host_path(
            Path::new("nested/file"),
            HostPathReplacement::Atomic,
            &options,
            selected_transaction_budget(),
            &CancellationToken::new(),
        )
        .await?;
    assert_eq!(restored.value, HostPathRestore::Restored);
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
            HostPathReplacement::Atomic,
            &options,
            selected_transaction_budget(),
            &CancellationToken::new(),
        )
        .await?;
    assert_eq!(removed.value, HostPathRestore::Removed);
    assert!(!destination.path().join("missing").exists());
    for invalid in [Path::new(""), Path::new("../escape"), destination.path()] {
        assert!(
            generation
                .restore_host_path(
                    invalid,
                    HostPathReplacement::Atomic,
                    &options,
                    selected_transaction_budget(),
                    &CancellationToken::new(),
                )
                .await
                .is_err()
        );
    }
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn public_generation_restore_host_path_rejects_symlinked_parent() -> Result<(), Box<dyn Error>>
{
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("restore-host-containment"))?;
    let generation = unbounded!(workspace, head())?;
    let destination = tempfile::tempdir()?;
    let outside = tempfile::tempdir()?;
    std::os::unix::fs::symlink(outside.path(), destination.path().join("escape"))?;
    let result = generation
        .restore_host_path(
            Path::new("escape/file"),
            HostPathReplacement::Atomic,
            &MaterializeOptions {
                materialized_view: None,
                destination: destination.path().to_path_buf(),
                expected_root_identity: None,
                maximum_directory_entries: 32,
                maximum_extent_spans: Some(32),
                transfer_bytes: 4096,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await;
    assert!(result.is_err());
    assert!(!outside.path().join("file").exists());
    Ok(())
}

#[cfg(windows)]
#[tokio::test]
async fn public_generation_restore_host_path_rejects_reparse_parent_when_available()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("restore-host-containment-windows"))?;
    let generation = unbounded!(workspace, head())?;
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
            HostPathReplacement::Atomic,
            &MaterializeOptions {
                materialized_view: None,
                destination: destination.path().to_path_buf(),
                expected_root_identity: None,
                maximum_directory_entries: 32,
                maximum_extent_spans: Some(32),
                transfer_bytes: 4096,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await;
    assert!(result.is_err());
    assert!(!outside.path().join("file").exists());
    Ok(())
}
