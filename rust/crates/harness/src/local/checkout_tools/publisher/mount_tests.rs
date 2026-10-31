use super::*;

async fn native_fixture() -> Result<(
    tempfile::TempDir,
    tempfile::TempDir,
    LocalHarness,
    CheckoutPublisher,
)> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let model = Model::new("scripted".into(), "publication".into(), "1".into())?;
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
            &harness.verifier,
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
async fn selected_branch_publication_and_recovery_advance_the_selected_mount() -> Result<()> {
    let (_directory, _state, harness, initial) = native_fixture().await?;
    let task = Arc::new(crate::runtime::test_context::agent_context(
        RuntimeScope::new(harness.signed.capabilities().clone(), Default::default())?,
        None,
    )?);
    let root = &initial.tools.workspace;
    let stop = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget::UNBOUNDED, &stop)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let view = root
        .view()
        .open_related(
            root.view().workspace().clone(),
            &mut acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                work: &work,
                cancellation: &stop,
            },
        )
        .await
        .map_err(storage)?;
    let mounted = root
        .mount_task_view(
            view,
            &*task,
            None,
            &crate::filesystem::NativeMountAuthority::admitted(
                harness.verifier.clone(),
                harness.signed.copy(&mut None)?,
            ),
        )
        .await?;
    let tools = Arc::new(
        CheckoutTools::new(
            Arc::clone(&mounted),
            &harness.project,
            Arc::new(acyclic_stream::SystemUnixMillisClock),
            &harness.signed,
            &harness.verifier,
        )
        .await?,
    );
    let publisher = CheckoutPublisher::new(
        Arc::clone(&tools),
        Arc::clone(&harness.files),
        &harness.project,
        &harness.verifier,
        harness.limits.file_bytes,
    );
    publisher
        .stage_once(
            OperationId::new(),
            "branch.txt",
            b"original",
            "text/plain",
            "branch.txt",
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
    let context = ToolContext::new(task, OperationId::new(), "select-branch")?;
    tools
        .execute(
            ToolInvocation {
                operation_id: context.operation_id(),
                call_id: context.call_id().into(),
                name: "fs".into(),
                arguments: serde_json::json!({"command": "checkout -b topic"}),
            },
            Some(context),
        )
        .await?;
    let selected = {
        let state = lock_owned(StateArc::clone(&tools.state)).await;
        publisher.selected_workspace(&state)?
    };
    assert_ne!(selected.path, mounted.path);
    for broken in [false, true] {
        let operation = OperationId::new();
        let bytes = if broken {
            b"counterfactual".as_slice()
        } else {
            b"selected".as_slice()
        };
        super::tests::PUBLISH_TO_INITIAL_MOUNT.set(broken);
        let staged = publisher
            .stage_once(
                operation,
                "branch.txt",
                bytes,
                "text/plain",
                "branch.txt",
                crate::conversation::ContentAccess::default(),
            )?
            .await;
        super::tests::PUBLISH_TO_INITIAL_MOUNT.set(false);
        let (file, _) = staged?;
        let visible = std::fs::read(selected.path.join("branch.txt")).map_err(storage)?;
        assert_eq!(
            visible == bytes,
            !broken,
            "publication advanced the wrong branch mount"
        );
        super::tests::PUBLISH_TO_INITIAL_MOUNT.set(broken);
        let recovered = publisher
            .observe_staged_once(operation, 64, crate::conversation::ContentAccess::default())?
            .await;
        super::tests::PUBLISH_TO_INITIAL_MOUNT.set(false);
        assert_eq!(recovered?.0, Some(file.clone()));
        let visible = std::fs::read(selected.path.join("branch.txt")).map_err(storage)?;
        assert_eq!(
            visible == bytes,
            !broken,
            "recovery advanced the wrong branch mount"
        );
        // The original observation repairs the deliberately unadvanced branch
        // without another write or generation, and retains the immutable receipt.
        assert_eq!(
            publisher
                .observe_staged_once(operation, 64, crate::conversation::ContentAccess::default(),)?
                .await?
                .0,
            Some(file)
        );
        assert_eq!(
            std::fs::read(selected.path.join("branch.txt")).map_err(storage)?,
            bytes
        );
        assert_eq!(
            std::fs::read(mounted.path.join("branch.txt")).map_err(storage)?,
            b"original"
        );
    }
    Ok(())
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

// A caller with project-only grants recovers its own project write: a
// publication that never committed observes as absent rather than failing
// on the scratch mount it may not reach, and a committed one is returned.
// Negative control: requiring access to every mount refuses the caller.
#[tokio::test]
async fn project_only_callers_recover_their_own_operations() -> Result<()> {
    let (_directory, _state, harness, publisher) = native_fixture().await?;
    let project_only = Capabilities::new([
        harness
            .project
            .capability(VolumeOperation::Read, &mut None)?,
        harness
            .project
            .capability(VolumeOperation::Write, &mut None)?,
    ]);
    let (publisher, project_only) = (&publisher, &project_only);
    let access = move || crate::conversation::ContentAccess {
        ceiling: Some(project_only),
        io: None,
    };
    let observe = move |operation| async move {
        publisher
            .observe_staged_once(operation, 1024, access())?
            .await
            .map(|(file, _)| file)
    };
    let operation = OperationId::new();
    let (written, _) = publisher
        .stage_once(
            operation,
            "result.txt",
            b"agent bytes",
            "text/plain",
            "result.txt",
            access(),
        )?
        .await?;
    for strict in [false, true] {
        super::tests::OBSERVE_EVERY_MOUNT.set(strict);
        let missing = observe(OperationId::new()).await;
        let recovered = observe(operation).await;
        super::tests::OBSERVE_EVERY_MOUNT.set(false);
        if strict {
            assert!(matches!(
                missing,
                Err(Error::FilesystemAccessConfiguration(_) | Error::Unauthorized(_))
            ));
        } else {
            assert!(missing?.is_none());
            assert_eq!(
                recovered?.map(|file| file.version().to_owned()),
                Some(written.version().to_owned())
            );
        }
    }
    // A caller that may reach no mount is still refused.
    let none = Capabilities::new(Vec::<String>::new());
    assert!(matches!(
        publisher
            .observe_staged_once(
                OperationId::new(),
                1024,
                crate::conversation::ContentAccess {
                    ceiling: Some(&none),
                    io: None
                },
            )?
            .await,
        Err(Error::FilesystemAccessConfiguration(_) | Error::Unauthorized(_))
    ));
    Ok(())
}
