use super::*;

async fn fixture() -> Result<(
    tempfile::TempDir,
    tempfile::TempDir,
    LocalHarness,
    CheckoutPublisher,
)> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let harness = open_publication_fixture(directory.path(), state.path()).await?;
    let publisher = publication_fixture_publisher(&harness).await?;
    Ok((directory, state, harness, publisher))
}

async fn open_publication_fixture(directory: &Path, state: &Path) -> Result<LocalHarness> {
    let model = Model::new(
        "scripted".into(),
        "publication".into(),
        "1".into(),
        serde_json::Value::Null,
    )?;
    let provider = Arc::new(crate::test_support::ScriptedModelProvider::new([]));
    LocalHarness::open(
        crate::test_support::local_harness_config(directory, (model, provider))?
            .state_directory(state),
    )
    .await
}

async fn publication_fixture_publisher(harness: &LocalHarness) -> Result<CheckoutPublisher> {
    let tools = Arc::new(
        CheckoutTools::new(
            harness
                .workspace
                .as_ref()
                .ok_or_else(|| Error::NotFound("checkout".into()))?
                .clone(),
            &harness.project,
            Arc::new(acyclic_stream::SystemUnixMillisClock),
            &harness.signed,
            Arc::clone(&harness.task_files),
        )
        .await?,
    );
    let publisher = CheckoutPublisher::new(
        tools,
        harness.files.clone(),
        &harness.project,
        &harness.verifier,
        harness.limits.file_bytes,
    );
    Ok(publisher)
}

#[tokio::test]
async fn materialized_writes_keep_pinned_edits_valid_and_capture_external_changes() -> Result<()> {
    for target in ["exact.txt", "nested/deep/exact.txt"] {
        materialized_write_sequence(target).await?;
    }
    Ok(())
}

async fn materialized_write_sequence(target: &str) -> Result<()> {
    let (directory, state, harness, publisher) = fixture().await?;
    let (original, _) = publisher
        .stage_once(
            OperationId::new(),
            target,
            b"original",
            "text/plain",
            "exact.txt",
            crate::conversation::ContentAccess {
                ceiling: None,
                io: None,
            },
        )?
        .await?;
    // Ordinary reads emit the same Linux notification as an explicit access
    // timestamp edit. Capture must normalize the forward stamp without reading
    // the payload again or invalidating this pinned generation.
    assert_eq!(
        std::fs::read(directory.path().join(target)).map_err(storage)?,
        b"original"
    );
    let (edited, _) = publisher
        .stage_at(OperationId::new(), &original, b"edited", None, None)?
        .await?;
    assert_eq!(
        std::fs::read(directory.path().join(target)).map_err(storage)?,
        b"edited"
    );
    // Release the composition and retire its mounts before reopening. Only
    // durable publication state may preserve the pinned generation.
    drop(publisher);
    harness.close().await?;
    let harness = open_publication_fixture(directory.path(), state.path()).await?;
    assert_eq!(edited.volume(), &harness.project);
    let reopened = publication_fixture_publisher(&harness).await?;
    let (latest, _) = reopened
        .stage_at(OperationId::new(), &edited, b"reopened", None, None)?
        .await?;
    assert_eq!(
        std::fs::read(directory.path().join(target)).map_err(storage)?,
        b"reopened"
    );
    // Same-length external bytes are an authored mutation, never our staged
    // postimage. The original strict generation fence must reject this edit.
    std::fs::write(directory.path().join(target), b"external").map_err(storage)?;
    assert!(
        reopened
            .stage_at(OperationId::new(), &latest, b"forbidden", None, None)?
            .await
            .is_err()
    );
    assert_eq!(
        std::fs::read(directory.path().join(target)).map_err(storage)?,
        b"external"
    );
    drop(reopened);
    harness.close().await?;
    Ok(())
}

#[tokio::test]
async fn materialized_write_attribution_does_not_hide_replacement_or_backdated_access() -> Result<()>
{
    for replace in [false, true] {
        let (directory, _state, harness, publisher) = fixture().await?;
        let (file, _) = publisher
            .stage_once(
                OperationId::new(),
                "identity.txt",
                b"same",
                "text/plain",
                "identity.txt",
                crate::conversation::ContentAccess {
                    ceiling: None,
                    io: None,
                },
            )?
            .await?;
        let path = directory.path().join("identity.txt");
        if replace {
            let replacement = directory.path().join("replacement.txt");
            std::fs::write(&replacement, b"same").map_err(storage)?;
            std::fs::remove_file(&path).map_err(storage)?;
            std::fs::rename(&replacement, &path).map_err(storage)?;
        } else {
            std::fs::File::options()
                .write(true)
                .open(&path)
                .map_err(storage)?
                .set_times(std::fs::FileTimes::new().set_accessed(std::time::UNIX_EPOCH))
                .map_err(storage)?;
        }
        assert!(
            publisher
                .stage_at(OperationId::new(), &file, b"forbidden", None, None)?
                .await
                .is_err(),
            "external identity or access mutation was lost: replace={replace}"
        );
        assert_eq!(std::fs::read(&path).map_err(storage)?, b"same");
        drop(publisher);
        harness.close().await?;
    }
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn materialized_write_attribution_does_not_hide_user_chmod() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let (directory, _state, harness, publisher) = fixture().await?;
    let (file, _) = publisher
        .stage_once(
            OperationId::new(),
            "mode.txt",
            b"same",
            "text/plain",
            "mode.txt",
            crate::conversation::ContentAccess {
                ceiling: None,
                io: None,
            },
        )?
        .await?;
    let path = directory.path().join("mode.txt");
    let mut permissions = std::fs::metadata(&path).map_err(storage)?.permissions();
    permissions.set_mode(permissions.mode() ^ 0o100);
    std::fs::set_permissions(&path, permissions).map_err(storage)?;
    assert!(
        publisher
            .stage_at(OperationId::new(), &file, b"forbidden", None, None)?
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&path).map_err(storage)?, b"same");
    drop(publisher);
    harness.close().await
}
