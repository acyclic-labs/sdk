use super::*;

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
#[tokio::test]
async fn public_generation_materialize_path_is_a_complete_consumer_flow()
-> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("materialize-consumer", B, &cancel)
        .await?
        .value;
    let mut transaction = workspace
        .begin_transaction(IdempotencyKey::new(), B, &cancel)
        .await?
        .value;
    transaction
        .create_dir_all("/output/nested", B, &cancel)
        .await?
        .value;
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
            B,
            &cancel,
        )
        .await?
        .value;
    let TransactionCommit::Committed(generation) = transaction
        .commit(
            None,
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            allow::<WorkspaceError>,
        )
        .await?
        .value
    else {
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
            B,
            &cancel,
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
            B,
            &cancel,
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
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("materialize-paths-consumer", B, &cancel)
        .await?
        .value;
    let mut transaction = workspace
        .begin_transaction(IdempotencyKey::new(), B, &cancel)
        .await?
        .value;
    transaction
        .create_dir_all("/nested", B, &cancel)
        .await?
        .value;
    transaction.write_text("/nested/a", "linked").await?;
    transaction
        .hard_link("/nested/a", "/nested/b", B, &cancel)
        .await?
        .value;
    transaction.write_text("/nested/c", "sibling").await?;
    let TransactionCommit::Committed(generation) = transaction
        .commit(
            None,
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            allow::<WorkspaceError>,
        )
        .await?
        .value
    else {
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
            B,
            &cancel,
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
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("restore-host-consumer", B, &cancel)
        .await?
        .value;
    let mut transaction = workspace
        .begin_transaction(IdempotencyKey::new(), B, &cancel)
        .await?
        .value;
    transaction
        .create_dir_all("/nested", B, &cancel)
        .await?
        .value;
    transaction
        .write_text("/nested/file", "authenticated")
        .await?;
    let TransactionCommit::Committed(generation) = transaction
        .commit(
            None,
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            allow::<WorkspaceError>,
        )
        .await?
        .value
    else {
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
            B,
            &cancel,
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
            B,
            &cancel,
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
                    B,
                    &cancel,
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
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("restore-host-containment", B, &cancel)
        .await?
        .value;
    let generation = workspace.head(B, &cancel).await?.value;
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
            B,
            &cancel,
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
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("restore-host-containment-windows", B, &cancel)
        .await?
        .value;
    let generation = workspace.head(B, &cancel).await?.value;
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
            B,
            &cancel,
        )
        .await;
    assert!(result.is_err());
    assert!(!outside.path().join("file").exists());
    Ok(())
}
