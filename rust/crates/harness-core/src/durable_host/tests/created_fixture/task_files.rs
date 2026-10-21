use super::*;
use crate::task_volume::{FileKey, FileValue, Query, TaskFiles};

#[tokio::test]
async fn child_task_files_use_created_private_authority_and_the_original_owner() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_task_filesystem_child().await?;
    let task = TaskId::from_bytes([84; 16]);
    assert!(matches!(
        fixture.admit_child(created.request.child.clone()).await?,
        Admission::Accepted(id) if id == task
    ));
    let reservation = fixture.observation(task).await?.reservation.unwrap();
    let context = fixture
        .runtime
        .task_context(task, (&reservation).into())
        .await?;
    #[cfg(not(target_arch = "wasm32"))]
    {
        let owner = context.ordinary_journal_owner::<MemoryStream>()?;
        let authenticated = owner.created.as_ref().unwrap();
        let issued = context.scope().created_access.as_ref().unwrap();
        assert_eq!(issued.content.len(), authenticated.content.len());
        assert!(!issued.content.is_empty());
        for (issued, authenticated) in issued.content.iter().zip(&authenticated.content) {
            assert_eq!(issued.volume, authenticated.volume);
            assert!(Arc::ptr_eq(
                &issued.binding.reader,
                &authenticated.binding.reader
            ));
            match (&issued.binding.writer, &authenticated.binding.writer) {
                (Some(issued), Some(authenticated)) => assert!(Arc::ptr_eq(issued, authenticated)),
                (None, None) => {}
                _ => panic!("issued content changed its authenticated writer"),
            }
        }
        assert!(issued.publication_lease.is_some());
        assert!(authenticated.publication_lease.is_none());
    }
    let agent = crate::AgentId::from_bytes([80; 16]);
    let root = VolumeRef::new(
        fixture.filesystem_provider.clone(),
        "root-task-files".into(),
        crate::conversation::VolumeClass::AgentPrivate,
        crate::conversation::VolumeOwner::Agent(agent),
        &mut None,
    )?;
    let issuer = crate::core::AuthorityIssuer::new(
        "created-owner",
        [80; 32],
        created.parent_verifier.audience().clone(),
    );
    let mut grants = Vec::new();
    for operation in [VolumeOperation::Read, VolumeOperation::Write] {
        grants.push(root.capability(operation, &mut None)?);
    }
    let signed = issuer.issue(
        "task-files",
        &Capabilities::new(grants),
        Some(agent),
        None,
        &mut None,
    )?;
    let root = TaskFiles::open(
        fixture.stream.clone(),
        &root,
        &issuer.verifier(&mut None)?,
        &signed,
        context.scope().limits().file_bytes,
    )
    .await?;
    let files = root
        .for_created_task(&context, context.scope())
        .await?
        .unwrap();
    assert_eq!(files.volume(), &created.private);
    assert_ne!(files.volume(), root.volume());
    for operation in [VolumeOperation::Read, VolumeOperation::Write] {
        let omitted = created.private.capability(operation, &mut None)?;
        let narrowed = context.scoped(
            Capabilities::new(
                context
                    .scope()
                    .grants()
                    .iter()
                    .filter(|grant| *grant != omitted),
            ),
            context.scope().limits(),
        )?;
        assert!(
            root.for_created_task(&context, narrowed.scope())
                .await
                .is_err()
        );
    }
    let owner = context.owned_mailbox.as_ref().unwrap();
    let request = FileValue::new(
        FileKey::parse("agents/nested/request.md").unwrap(),
        b"nested".to_vec(),
    );
    let before = files.inner.snapshot(&Query::Children, 8).await.unwrap();
    let (condition, deadline) = owner.file_publication_condition().await?;
    files
        .write_if(before.generation, &request, task, condition, deadline)
        .await
        .unwrap();
    assert!(
        root.inner
            .snapshot(&Query::Children, 8)
            .await
            .unwrap()
            .files
            .is_empty()
    );
    let reopened = root
        .for_created_task(&context, context.scope())
        .await?
        .unwrap();
    let snapshot = reopened.inner.snapshot(&Query::Children, 8).await.unwrap();
    assert_eq!(snapshot.files.len(), 1);
    assert_eq!(snapshot.files[0].bytes.as_ref(), b"nested");
    assert_eq!(snapshot.files[0].requester(), Some(task));
    let (condition, deadline) = owner.file_publication_condition().await?;
    use crate::runtime::TaskStateProvider as _;
    fixture.host.cancel(task).await?;
    assert!(
        root.for_created_task(&context, context.scope())
            .await
            .is_err()
    );
    let late = FileValue::new(
        FileKey::parse("agents/late/request.md").unwrap(),
        b"late".to_vec(),
    );
    assert!(matches!(
        files
            .write_if(snapshot.generation, &late, task, condition, deadline)
            .await,
        Err(crate::task_volume::Error::FenceConflict)
    ));
    assert_eq!(
        files
            .inner
            .snapshot(&Query::Children, 8)
            .await
            .unwrap()
            .files,
        snapshot.files
    );
    Ok(())
}
