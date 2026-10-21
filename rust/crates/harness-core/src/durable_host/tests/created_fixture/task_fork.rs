use super::*;

#[tokio::test]
async fn task_fork_allocations_admit_only_the_original_child_and_isolate_its_files() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_task_filesystem_child().await?;
    assert_eq!(created.seed.parent.kind, AggregateKind::Task);
    assert_eq!(created.seed.child.kind, AggregateKind::Task);
    assert!(created.seed.inherited_context.is_empty());
    assert!(created.seed.history_witness.is_none());
    assert!(
        created.seed.resources.iter().all(|capture| {
            !matches!(capture.source, crate::fork::ResourceRevision::History(_))
        })
    );
    let mut unproven = created.request.child.clone();
    unproven.resource_proofs.clear();
    assert!(
        fixture
            .host
            .admit_owned(unproven, (&fixture.root_lease.reservation).into())
            .await
            .is_err()
    );
    let task = TaskId::from_bytes([84; 16]);
    assert!(
        matches!(fixture.admit_child(created.request.child.clone()).await?, Admission::Accepted(id) if id == task)
    );
    let reservation = fixture.observation(task).await?.reservation.unwrap();
    let mut context = fixture
        .runtime
        .task_context(task, (&reservation).into())
        .await?;
    let foreign = Arc::new(
        fixture
            .host
            .journal_owner(
                fixture.root,
                &(&fixture.root_lease.reservation).into(),
                None,
            )
            .await?,
    );
    assert!(matches!(
        context.bind_ordinary_created_owner(foreign),
        Err(Error::Unauthorized(_))
    ));
    for volume in [&created.project, &created.private] {
        let file = context
            .stage_file(
                Some(volume),
                OperationId::new(),
                "child.txt",
                b"child only",
                "text/plain",
                "child.txt",
            )
            .await?;
        assert_eq!(file.volume(), volume);
        assert_eq!(
            context.read_file(&file, None).await?.as_ref(),
            b"child only"
        );
    }
    for selection in &created.report.request.selections {
        let parent = match &selection.revision {
            crate::fork::ResourceRevision::Project { volume, .. }
            | crate::fork::ResourceRevision::PrivateVolume { volume, .. } => volume,
            _ => continue,
        };
        let workspace = crate::filesystem::workspace_ref(
            parent.provider().clone(),
            &parent.storage_name(&mut None)?,
        )?;
        assert!(matches!(
            fixture
                .filesystem
                .stat(&workspace, None, "/child.txt")
                .await,
            Err(Error::NotFound(_))
        ));
        assert!(matches!(
            context
                .stage_file(
                    Some(parent),
                    OperationId::new(),
                    "escape.txt",
                    b"forbidden",
                    "text/plain",
                    "escape.txt"
                )
                .await,
            Err(Error::Unauthorized(_) | Error::Unsupported(_))
        ));
        assert!(matches!(
            fixture
                .filesystem
                .stat(&workspace, None, "/escape.txt")
                .await,
            Err(Error::NotFound(_))
        ));
    }
    use crate::runtime::TaskStateProvider as _;
    fixture.host.cancel(task).await?;
    assert!(
        context
            .stage_file(
                Some(&created.project),
                OperationId::new(),
                "late.txt",
                b"late",
                "text/plain",
                "late.txt"
            )
            .await
            .is_err()
    );
    let workspace = crate::filesystem::workspace_ref(
        created.project.provider().clone(),
        &created.project.storage_name(&mut None)?,
    )?;
    assert!(matches!(
        fixture.filesystem.stat(&workspace, None, "/late.txt").await,
        Err(Error::NotFound(_))
    ));
    Ok(())
}
