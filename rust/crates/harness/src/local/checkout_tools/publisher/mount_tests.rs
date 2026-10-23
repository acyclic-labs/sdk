use super::*;

async fn native_fixture() -> Result<(
    tempfile::TempDir,
    tempfile::TempDir,
    LocalHarness,
    CheckoutPublisher,
)> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let model = Model::new(
        "scripted".into(),
        "publication".into(),
        "1".into(),
        serde_json::Value::Null,
    )?;
    let provider = Arc::new(crate::test_support::ScriptedModelProvider::new([]));
    let harness = LocalHarness::open(
        crate::test_support::local_harness_config(directory.path(), (model, provider))?
            .state_directory(state.path()),
    )
    .await?;
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
    Ok((directory, state, harness, publisher))
}

#[tokio::test]
async fn file_publication_materializes_its_exact_committed_bytes() -> Result<()> {
    let (directory, _state, harness, publisher) = native_fixture().await?;
    let (file, _) = publisher
        .stage_once(
            OperationId::new(),
            "published.txt",
            b"exact bytes",
            "text/plain",
            "published.txt",
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
    assert_eq!(file.volume(), &harness.project);
    assert_eq!(
        std::fs::read(directory.path().join("published.txt")).map_err(storage)?,
        b"exact bytes"
    );
    assert!(!directory.path().join(".system").exists());
    // Private metadata stores the publication operation, whereas the returned
    // reference stores its immutable generation. Full reads authenticate bytes
    // and select that generation without assuming a literal staging version.
    let private = Publisher::new(
        harness.files.clone(),
        &harness.journal,
        &harness.verifier,
        &harness.signed,
        harness.limits.file_bytes,
        None,
    )?;
    let (published, _) = private
        .stage_once(
            OperationId::new(),
            "private.txt",
            b"private bytes",
            "text/plain",
            "private.txt",
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
    let grant = crate::conversation::ContentGrant::verify_directory_read(
        &harness.verifier,
        &harness.signed,
        &harness.journal,
        "",
        &mut None,
    )?;
    let (selected, bytes) = harness
        .files
        .read_private_path(
            &harness.journal,
            &grant,
            "private.txt",
            None,
            64,
            crate::conversation::ContentAccess::default(),
        )
        .await?;
    assert_eq!(selected, published);
    assert_eq!(bytes.as_ref(), b"private bytes");
    Ok(())
}

#[tokio::test]
async fn mounted_publication_retries_and_replacements_keep_their_volume_with_negative_control()
-> Result<()> {
    let (directory, _state, harness, publisher) = native_fixture().await?;
    let operation = OperationId::new();
    let (file, _) = publisher
        .stage_once(
            operation,
            ".volumes/scratch/note",
            b"first",
            "text/plain",
            "note",
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
    assert_eq!(file.volume(), harness.scratch());
    assert_eq!(file.path(), "note");
    let project_only = Capabilities::new([
        harness
            .project
            .capability(VolumeOperation::Read, &mut None)?,
        harness
            .project
            .capability(VolumeOperation::Write, &mut None)?,
    ]);
    assert!(matches!(
        publisher
            .stage_once(
                OperationId::new(),
                ".volumes/scratch/denied",
                b"denied",
                "text/plain",
                "denied",
                crate::conversation::ContentAccess {
                    ceiling: Some(&project_only),
                    io: None
                }
            )?
            .await,
        Err(Error::FilesystemAccessConfiguration(
            "local checkout publication"
        ))
    ));
    assert!(!directory.path().join(".volumes/scratch/denied").exists());
    assert_eq!(
        publisher
            .observe_staged_once(operation, 64, crate::conversation::ContentAccess::default())?
            .await?
            .0,
        Some(file.clone())
    );
    assert_eq!(
        publisher
            .stage_once(
                operation,
                ".volumes/scratch/note",
                b"first",
                "text/plain",
                "note",
                crate::conversation::ContentAccess::default()
            )?
            .await?
            .0,
        file
    );
    let (edited, _) = publisher
        .stage_at(OperationId::new(), &file, b"edited", None, None)?
        .await?;
    assert_eq!(edited.volume(), harness.scratch());
    // Auxiliary volumes exist physically only while a native command owns its
    // projection. Publication must persist their bytes without leaving a mount.
    let mounts = &harness
        .workspace
        .as_ref()
        .ok_or_else(|| Error::NotFound("checkout".into()))?
        .mounts;
    assert!(!directory.path().join(".volumes/scratch").exists());
    crate::local::workspace::volumes::native_scope(mounts, async |claims| {
        assert_eq!(
            std::fs::read(directory.path().join(".volumes/scratch/note")).map_err(storage)?,
            b"edited"
        );
        Ok(((), claims))
    })
    .await?;
    assert!(!directory.path().join(".volumes/scratch").exists());
    let view = harness.workspace.as_ref().unwrap().view();
    let stop = acyclic_fs::CancellationToken::new();
    let primary_absent = async |path: &str| -> Result<bool> {
        let head = view
            .workspace()
            .head(acyclic_fs::WorkBudget::UNBOUNDED, &stop)
            .await
            .map_err(storage)?
            .value;
        Ok(matches!(
            head.stat(path, acyclic_fs::WorkBudget::UNBOUNDED, &stop)
                .await,
            Err(acyclic_fs::OperationFailure {
                error: acyclic_fs::WorkspaceError::NotFound,
                ..
            })
        ))
    };
    assert!(primary_absent("/.volumes/scratch/note").await?);
    assert!(primary_absent("/note").await?);
    // The former implementation selected the project writer. This deliberately
    // bypasses routing and must fail the same history-attribution oracle.
    publisher
        .writer_for(&harness.project, view.workspace())?
        .stage_once(
            OperationId::new(),
            ".volumes/scratch/control",
            b"wrong history",
            "text/plain",
            "control",
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
    assert!(
        !primary_absent("/.volumes/scratch/control").await?,
        "negative control failed to expose the project-history leak"
    );
    assert!(
        publisher
            .destination(Some(&harness.journal), "nope", None)
            .is_err()
    );
    assert!(
        publisher
            .destination(Some(&harness.project), ".volumes/scratch/note", None)
            .is_err()
    );
    Ok(())
}
