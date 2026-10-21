use super::*;

async fn fixture() -> Result<(
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
            Arc::clone(&harness.task_files),
        )
        .await?,
    );
    let publisher = CheckoutPublisher::new(
        tools,
        harness.files.clone(),
        &harness.project,
        &harness.verifier,
        &harness.signed,
        harness.limits.file_bytes,
    );
    Ok((directory, state, harness, publisher))
}

#[tokio::test]
async fn file_publication_materializes_its_exact_committed_bytes() -> Result<()> {
    let (directory, _state, harness, publisher) = fixture().await?;
    let (file, _) = publisher
        .stage_once(
            OperationId::new(),
            "published.txt",
            b"exact bytes",
            "text/plain",
            "published.txt",
            None,
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
            None,
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
        .read_private_path(&harness.journal, &grant, "private.txt", None, 64, None)
        .await?;
    assert_eq!(selected, published);
    assert_eq!(bytes.as_ref(), b"private bytes");
    Ok(())
}

#[tokio::test]
async fn executor_bindings_share_one_initial_checkout_selection() -> Result<()> {
    let (_directory, _state, harness, publisher) = fixture().await?;
    let workspace = publisher.tools.workspace.clone();
    let before = workspace
        .selection_reads
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(before, 1, "initial binding must select the checkout once");
    CheckoutTools::new(
        workspace.clone(),
        &harness.project,
        Arc::new(acyclic_stream::SystemUnixMillisClock),
        Arc::clone(harness.task_files()),
    )
    .await?;
    assert_eq!(
        workspace
            .selection_reads
            .load(std::sync::atomic::Ordering::Relaxed),
        before,
        "another binding reloaded the already selected checkout"
    );
    Ok(())
}

#[tokio::test]
async fn committed_uploads_recover_checkout_before_capture_and_never_reapply_completed_edits()
-> Result<()> {
    let (directory, _state, harness, publisher) = fixture().await?;
    let view = harness.workspace.as_ref().unwrap().view();
    let writer = publisher.writer_for(&harness.project, view.workspace())?;
    let operation = OperationId::new();
    // Crash boundary: the actual volume write commits, but no native
    // preparation journal or checkout edit has happened.
    let (original, _) = writer
        .stage_once(
            operation,
            "once.txt",
            b"once",
            "text/plain",
            "once.txt",
            None,
        )?
        .await?;
    assert!(!directory.path().join("once.txt").exists());
    // Recovery normally enters through observe_staged_once, before executing
    // the tool again. It must repair the checkout as well as return its ref.
    assert_eq!(
        publisher
            .observe_staged_once(operation, 1024, None)?
            .await?
            .0
            .as_ref(),
        Some(&original)
    );
    assert_eq!(
        std::fs::read(directory.path().join("once.txt")).map_err(storage)?,
        b"once"
    );
    assert_eq!(
        publisher
            .stage_once(
                operation,
                "once.txt",
                b"once",
                "text/plain",
                "once.txt",
                None
            )?
            .await?
            .0,
        original
    );

    let operation = OperationId::new();
    let (original, _) = writer
        .stage(
            operation,
            "path.txt",
            b"path",
            "text/plain",
            "path.txt",
            None,
        )?
        .await?;
    assert!(!directory.path().join("path.txt").exists());
    assert_eq!(
        publisher
            .stage(
                operation,
                "path.txt",
                b"path",
                "text/plain",
                "path.txt",
                None
            )?
            .await?
            .0,
        original
    );
    assert_eq!(
        std::fs::read(directory.path().join("path.txt")).map_err(storage)?,
        b"path"
    );

    let operation = OperationId::new();
    let (replacement, _) = writer
        .stage_at(operation, &original, b"replacement", None)?
        .await?;
    assert_eq!(
        std::fs::read(directory.path().join("path.txt")).map_err(storage)?,
        b"path"
    );
    assert_eq!(
        publisher
            .stage_at(operation, &original, b"replacement", None)?
            .await?
            .0,
        replacement
    );
    assert_eq!(
        std::fs::read(directory.path().join("path.txt")).map_err(storage)?,
        b"replacement"
    );
    let cancellation = acyclic_fs::CancellationToken::new();
    let before = view
        .workspace()
        .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value
        .id();
    std::fs::write(directory.path().join("path.txt"), b"later user edit").map_err(storage)?;
    assert_eq!(
        publisher
            .stage_at(operation, &original, b"replacement", None)?
            .await?
            .0,
        replacement
    );
    assert_eq!(
        std::fs::read(directory.path().join("path.txt")).map_err(storage)?,
        b"later user edit"
    );
    // Git recovery may enter with the already-committed target as both
    // heads. It must use the saved source and retain later checkout edits too.
    let head = view
        .workspace()
        .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value;
    crate::tools::git::publish_checkout_transition(
        directory.path(),
        publisher.tools.workspace.identity,
        &publisher.tools.workspace.store,
        publication_operation(operation, original.path()),
        head.clone(),
        head,
        &|| std::future::ready(Ok(())),
    )
    .await?;
    assert_eq!(
        std::fs::read(directory.path().join("path.txt")).map_err(storage)?,
        b"later user edit"
    );
    let after = view
        .workspace()
        .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value
        .id();
    assert_eq!(
        before, after,
        "retry captured the missing native write or authored another upload"
    );
    Ok(())
}

#[tokio::test]
async fn one_operation_publishes_each_path_and_retries_without_overwriting_edits() -> Result<()> {
    let (directory, _state, _harness, publisher) = fixture().await?;
    let operation = OperationId::new();
    let mut files = Vec::new();
    for (path, bytes) in [
        ("first.txt", b"first".as_slice()),
        ("second.txt", b"second".as_slice()),
    ] {
        let (file, _) = publisher
            .stage(operation, path, bytes, "text/plain", path, None)?
            .await?;
        assert_eq!(
            std::fs::read(directory.path().join(path)).map_err(storage)?,
            bytes
        );
        files.push((file, bytes));
    }
    for (file, bytes) in files {
        std::fs::write(directory.path().join(file.path()), "later edit").map_err(storage)?;
        let (retried, _) = publisher
            .stage(
                operation,
                file.path(),
                bytes,
                "text/plain",
                file.display_name(),
                None,
            )?
            .await?;
        assert_eq!(retried, file);
        assert_eq!(
            std::fs::read(directory.path().join(file.path())).map_err(storage)?,
            b"later edit"
        );
    }
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(64))]
    #[test]
    fn publication_identity_binds_operation_and_path(
        operation in proptest::prelude::any::<[u8;16]>(),
        path in "[a-z]{1,16}(/[a-z]{1,16}){0,3}",
    ) {
        let original = OperationId::from_bytes(operation);
        let first = publication_operation(original, &path);
        proptest::prop_assert_eq!(first, publication_operation(original, &path));
        proptest::prop_assert_ne!(first, publication_operation(original, &format!("{path}x")));
        let mut other = operation; other[0] ^= 1;
        proptest::prop_assert_ne!(first, publication_operation(OperationId::from_bytes(other), &path));
    }
}

#[tokio::test]
async fn mounted_publication_retries_and_replacements_keep_their_volume_with_negative_control()
-> Result<()> {
    let (directory, _state, harness, publisher) = fixture().await?;
    let operation = OperationId::new();
    let (file, _) = publisher
        .stage_once(
            operation,
            ".volumes/scratch/note",
            b"first",
            "text/plain",
            "note",
            None,
        )?
        .await?;
    assert_eq!(file.volume(), harness.scratch());
    assert_eq!(file.path(), "note");
    assert_eq!(
        publisher.observe_staged_once(operation, 64, None)?.await?.0,
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
                None
            )?
            .await?
            .0,
        file
    );
    let (edited, _) = publisher
        .stage_at(OperationId::new(), &file, b"edited", None)?
        .await?;
    assert_eq!(edited.volume(), harness.scratch());
    // Publication selects the bound scratch volume without starting its
    // native projection; the bytes live in that volume's head.
    let stop = acyclic_fs::CancellationToken::new();
    let scratch = harness
        .workspace
        .as_ref()
        .unwrap()
        .mounts
        .named(Some("scratch"))?
        .1
        .view
        .source
        .clone()
        .ok_or_else(|| Error::NotFound("scratch source".into()))?;
    assert!(
        !scratch.destination.exists(),
        "publication projected scratch"
    );
    assert!(!directory.path().join(".volumes/scratch/note").exists());
    assert_eq!(
        scratch
            .workspace
            .workspace()
            .head(acyclic_fs::WorkBudget::UNBOUNDED, &stop)
            .await
            .map_err(storage)?
            .value
            .read("/note", 64, acyclic_fs::WorkBudget::UNBOUNDED, &stop)
            .await
            .map_err(storage)?
            .value
            .as_ref(),
        b"edited"
    );
    let view = harness.workspace.as_ref().unwrap().view();
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
            None,
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

#[tokio::test]
async fn a_written_file_ref_is_immediately_editable() -> Result<()> {
    let (directory, _state, _harness, publisher) = fixture().await?;
    let (written, _) = publisher
        .stage_once(
            OperationId::new(),
            "result.txt",
            b"before edit
",
            "text/plain",
            "result.txt",
            None,
        )?
        .await?;
    // Before the fix the returned version named the staged generation, and
    // capturing the write's own host metadata made that generation stale.
    let (edited, _) = publisher
        .stage_at(
            OperationId::new(),
            &written,
            b"after edit
",
            None,
        )?
        .await?;
    assert_eq!(edited.path(), "result.txt");
    assert_eq!(
        std::fs::read(directory.path().join("result.txt")).map_err(storage)?,
        b"after edit
"
    );
    // The edit's own reference is just as current.
    publisher
        .stage_at(
            OperationId::new(),
            &edited,
            b"third
",
            None,
        )?
        .await?;
    assert_eq!(
        std::fs::read(directory.path().join("result.txt")).map_err(storage)?,
        b"third
"
    );
    Ok(())
}

// Negative control: rebinding never hides a foreign change. A user edit made
// after the write still makes the returned reference stale.
#[tokio::test]
async fn a_foreign_change_after_a_write_still_conflicts() -> Result<()> {
    let (directory, _state, _harness, publisher) = fixture().await?;
    let (written, _) = publisher
        .stage_once(
            OperationId::new(),
            "result.txt",
            b"agent bytes
",
            "text/plain",
            "result.txt",
            None,
        )?
        .await?;
    let path = directory.path().join("result.txt");
    std::fs::write(
        &path,
        b"user bytes
",
    )
    .map_err(storage)?;
    let conflict = match publisher.stage_at(
        OperationId::new(),
        &written,
        b"overwrite
",
        None,
    ) {
        Ok(staged) => staged.await.err(),
        Err(error) => Some(error),
    };
    let conflict = conflict.ok_or_else(|| Error::Invalid("stale reference was accepted".into()))?;
    assert_eq!(
        conflict.code(),
        crate::wire::ErrorCode::Conflict,
        "{conflict}"
    );
    assert_eq!(
        std::fs::read(&path).map_err(storage)?,
        b"user bytes
"
    );
    Ok(())
}
