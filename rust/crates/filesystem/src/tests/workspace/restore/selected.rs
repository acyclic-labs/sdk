use super::*;

#[tokio::test]
async fn path_restore_reuses_exact_records_and_preserves_unselected_state()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("restore-paths"))?;
    let mut transaction = unbounded!(
        workspace,
        begin_transaction(IdempotencyKey::from_bytes([0x60; 16]))
    )?;
    unbounded!(transaction, create_dir_all("/tree"))?;
    unbounded!(
        transaction,
        create_file(
            "/tree/tracked",
            OwnedBytes::from_static(b"committed"),
            FileMetadata::default()
        )
    )?;
    unbounded!(
        transaction,
        hard_link("/tree/tracked", "/tree/ignored-link")
    )?;
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::Committed(_)
    ));
    workspace.write_text("/ignored", "live").await?;
    let committed = unbounded!(workspace, head())?;
    let snapshot = fork_workspace(
        &workspace,
        "restore-paths-snapshot",
        ForkOptions::from_generation(committed, IdempotencyKey::from_bytes([0x61; 16])),
    )
    .await?;
    unbounded!(snapshot, remove("/ignored"))?;
    let source = unbounded!(snapshot, head())?;

    workspace.write_text("/tree/tracked", "dirty").await?;
    workspace.write_text("/ignored", "still-live").await?;
    let dirty = unbounded!(workspace, head())?;
    let outcome = workspace
        .restore_paths_from(
            &source,
            &["tree/tracked".to_owned()],
            dirty.id(),
            IdempotencyKey::from_bytes([0x62; 16]),
            crate::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            allow::<FsError>,
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    assert!(matches!(outcome, TransactionCommit::Committed(_)));
    assert_eq!(
        unbounded!(workspace, read("/tree/tracked", 64))?,
        OwnedBytes::from_static(b"committed")
    );
    assert_eq!(
        unbounded!(workspace, read("/tree/ignored-link", 64))?,
        OwnedBytes::from_static(b"committed")
    );
    assert_eq!(
        unbounded!(workspace, read("/ignored", 64))?,
        OwnedBytes::from_static(b"still-live")
    );
    let tracked = unbounded!(workspace, stat("/tree/tracked"))?;
    let linked = unbounded!(workspace, stat("/tree/ignored-link"))?;
    assert_eq!(tracked.file_id, linked.file_id);
    assert_eq!(tracked.link_count, 2);
    Ok(())
}

#[tokio::test]
async fn path_apply_is_three_way_conflict_checked() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("apply-paths"))?;
    workspace.write_text("/file", "base").await?;
    let base = unbounded!(workspace, head())?;
    let source_workspace = fork_workspace(
        &workspace,
        "apply-paths-source",
        ForkOptions::from_generation(base.clone(), IdempotencyKey::from_bytes([0x63; 16])),
    )
    .await?;
    source_workspace.write_text("/file", "source").await?;
    let source = unbounded!(source_workspace, head())?;
    let current = unbounded!(workspace, head())?;
    assert!(matches!(
        workspace
            .apply_paths_from(
                Some(&base),
                Some(&source),
                &["file".to_owned()],
                current.id(),
                IdempotencyKey::from_bytes([0x64; 16]),
                crate::PublicationPermit::Unrestricted
            )
            .await?,
        WorkspacePathApply::Applied(_)
    ));
    assert_eq!(
        unbounded!(workspace, read("/file", 64))?,
        OwnedBytes::from_static(b"source")
    );

    let conflicting = unbounded!(fs, create_workspace("apply-paths-conflict"))?;
    conflicting.write_text("/file", "base").await?;
    let conflict_base = unbounded!(conflicting, head())?;
    let conflict_source = fork_workspace(
        &conflicting,
        "apply-paths-conflict-source",
        ForkOptions::from_generation(
            conflict_base.clone(),
            IdempotencyKey::from_bytes([0x65; 16]),
        ),
    )
    .await?;
    conflict_source.write_text("/file", "source").await?;
    let conflict_source = unbounded!(conflict_source, head())?;
    conflicting.write_text("/file", "ours").await?;
    let conflict_current = unbounded!(conflicting, head())?;
    let WorkspacePathApply::Conflicted(conflicts) = conflicting
        .apply_paths_from(
            Some(&conflict_base),
            Some(&conflict_source),
            &["file".to_owned()],
            conflict_current.id(),
            IdempotencyKey::from_bytes([0x66; 16]),
            crate::PublicationPermit::Unrestricted,
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
async fn directory_record_restore_preserves_unselected_descendants() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("directory-record"))?;
    let mode = |value| FileMetadata {
        posix_mode: MetadataField::Value(value),
        ..FileMetadata::default()
    };
    let mut seed = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(seed, create_dir_all("/tree"))?;
    unbounded!(seed, set_metadata("/tree", mode(0o700)))?;
    seed.write_text("/tree/tracked", "base").await?;
    commit(&mut seed).await?;
    let source = unbounded!(workspace, head())?;
    let mut live = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(live, set_metadata("/tree", mode(0o755)))?;
    live.write_text("/tree/tracked", "dirty").await?;
    live.write_text("/tree/ignored", "live ignored").await?;
    commit(&mut live).await?;
    let current = unbounded!(workspace, head())?;
    let restored = workspace
        .restore_paths_from(
            &source,
            &["tree".to_owned()],
            current.id(),
            IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted,
            selected_transaction_budget(),
            &CancellationToken::new(),
            allow::<FsError>,
        )
        .await?;
    restored.work.verify(selected_transaction_budget())?;
    assert!(matches!(restored.value, TransactionCommit::Committed(_)));
    assert_eq!(
        unbounded!(workspace, stat("/tree"))?.metadata.posix_mode,
        Some(0o700)
    );
    for (path, text) in [
        ("/tree/tracked", "dirty"),
        ("/tree/ignored", "live ignored"),
    ] {
        assert_eq!(
            unbounded!(workspace, read(path, 32))?.as_ref(),
            text.as_bytes()
        );
    }
    Ok(())
}
