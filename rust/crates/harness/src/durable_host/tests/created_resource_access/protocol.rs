use super::*;
use crate::resource_preparation::ResourceCreationProvider;

#[tokio::test]
async fn created_context_reopens_and_attenuates_without_root_authority() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    let task = TaskId::from_bytes([84; 16]);
    fixture.admit_child(created.request.child.clone()).await?;
    let read = created
        .private
        .capability(VolumeOperation::Read, &mut None)?;
    let write = created
        .private
        .capability(VolumeOperation::Write, &mut None)?;
    let context = fixture
        .harness
        .durable_context(task, created.request.child.operation_id)
        .await?;
    assert_eq!(
        context
            .scope()
            .current_created_content(&mut None)?
            .unwrap()
            .volume,
        created.private
    );
    let file = context
        .stage_file_once(
            OperationId::from_bytes([93; 16]),
            "created/content.txt",
            b"created private bytes",
            "text/plain",
            "content.txt",
        )
        .await?;
    assert_eq!(
        context.read_file(&file).await?.as_ref(),
        b"created private bytes"
    );
    let reopened = NativeCustomerRuntime::open(
        fixture.stream.clone(),
        fixture.filesystem.clone(),
        &VolumeRef::new(
            fixture.filesystem_provider.clone(),
            "scheduler".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([80; 16])),
            &mut None,
        )?,
        &fixture.host.verifier,
        &fixture.host.owner_scope,
        &fixture.root_scope,
        &fixture.host.tasks,
        &fixture.host.machines,
        &crate::tool::ToolRegistry::default(),
        fixture.host.session_limits.unwrap(),
        16,
        fixture.root_scope.limits().file_bytes,
        fixture.host.clock.clone(),
        None,
        &[fixture.filesystem_creator.clone()],
        &crate::durable_tool::ResumableToolRegistry::default(),
        None,
    )
    .await?;
    let reservation = fixture.host.observe_task(task).await?.reservation.unwrap();
    let reopened = reopened.task_context(task, (&reservation).into()).await?;
    assert_eq!(
        reopened.read_file(&file).await?.as_ref(),
        b"created private bytes"
    );
    let root = fixture
        .harness
        .durable_context(fixture.root, fixture.root_lease.operation.operation_id)
        .await?;
    assert!(!root.scope().grants().contains(&read));
    assert!(root.read_file(&file).await.is_err());
    let narrowed = context.scoped(Capabilities::new([read.as_str()]), context.scope().limits())?;
    assert_eq!(
        narrowed.read_file(&file).await?.as_ref(),
        b"created private bytes"
    );
    assert!(
        narrowed
            .stage_file(
                OperationId::from_bytes([94; 16]),
                "denied.txt",
                b"denied",
                "text/plain",
                "denied.txt"
            )
            .await
            .is_err()
    );
    assert!(
        narrowed
            .scoped(
                Capabilities::new([write.as_str()]),
                narrowed.scope().limits()
            )
            .is_err()
    );
    let mut descendant = fixture.admission(97, serde_json::json!({}));
    descendant.parent = Some(task);
    descendant.grants = Capabilities::new([read.as_str()]);
    descendant.resource_proofs = created.request.child.resource_proofs.clone();
    descendant.limits.file_bytes = 1024;
    fixture
        .host
        .admit_owned(descendant.clone(), (&reservation).into())
        .await?;
    fixture.start_next(TaskId::from_bytes([97; 16])).await?;
    let inherited = fixture
        .harness
        .durable_context(TaskId::from_bytes([97; 16]), descendant.operation_id)
        .await?;
    assert_eq!(
        inherited.read_file(&file).await?.as_ref(),
        b"created private bytes"
    );
    assert!(!inherited.scope().grants().contains(&write));
    Ok(())
}

#[tokio::test]
async fn genuine_allocation_corruption_denies_created_context() -> Result<()> {
    for (path, value) in [
        (
            vec!["task_origin", "owner", "id"],
            serde_json::json!("foreign-owner"),
        ),
        (
            vec!["task_origin", "preparation_task"],
            serde_json::to_value(TaskId::from_bytes([85; 16])).unwrap(),
        ),
        (
            vec!["task_origin", "intended_child"],
            serde_json::to_value(OperationId::from_bytes([85; 16])).unwrap(),
        ),
        (
            vec!["task_origin", "request_digest"],
            serde_json::to_value([0u8; 32]).unwrap(),
        ),
        (
            vec!["task_origin", "admission_digest"],
            serde_json::to_value([0u8; 32]).unwrap(),
        ),
    ] {
        let fixture = CreationFixture::new().await?;
        let created = fixture.create_filesystem_child().await?;
        fixture.admit_child(created.request.child.clone()).await?;
        let claim = workspace_ref(
            fixture.filesystem_provider.clone(),
            &format!(
                "fork-allocation-{}",
                created.private.storage_name(&mut None)?
            ),
        )?;
        let bytes = fixture
            .filesystem
            .read(&claim, None, "/claim.json", 65_536)
            .await?;
        let mut record: Value = crate::contract::json_from_slice(&bytes)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let mut target = &mut record;
        for field in path {
            target = &mut target[field];
        }
        *target = value;
        fixture
            .filesystem
            .apply(
                &claim,
                None,
                &[WorkspaceMutation::PutFile {
                    path: "/claim.json".into(),
                    bytes: crate::contract::canonical_json_bytes(&record)?.into(),
                }],
                &IdempotencyKey::new("corrupt-created-allocation")?,
            )
            .await?;
        assert!(
            fixture
                .harness
                .durable_context(
                    TaskId::from_bytes([84; 16]),
                    created.request.child.operation_id
                )
                .await
                .is_err()
        );
        assert!(
            fixture
                .host
                .observe_admission(fixture.root)
                .await?
                .resource_proofs
                .is_empty()
        );
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_creator_denies_quota_and_cancel_before_io() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    for mode in ["read", "allocation", "cancel"] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let mut budget = acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024);
        match mode {
            "read" => budget.backend_read_operations = 0,
            "allocation" => budget.allocation_operations = 1,
            _ => {}
        }
        let work = test_work(budget, &cancellation)?;
        if mode == "cancel" {
            cancellation.cancel();
        }
        let result = async {
            fixture
                .filesystem_creator
                .verify_with_native_work(
                    &created.request,
                    &created.request.child.resource_proofs[0],
                    &work,
                )?
                .await
        }
        .await;
        assert!(matches!(result, Err(Error::NativeWork { .. })));
        assert_eq!(work.receipt().backend_read_operations, 0);
        assert_eq!(work.receipt().backend_write_operations, 0);
    }
    Ok(())
}
