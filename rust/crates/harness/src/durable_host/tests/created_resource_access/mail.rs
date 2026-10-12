use super::*;
use crate::filesystem::MailReceiveTaskCommand;
use crate::scheduler::LeaseFence;
use crate::workflow::WorkflowCommand;

async fn receive(
    runtime: &NativeCustomerRuntime,
    task: TaskId,
    fence: LeaseFence,
    byte: u8,
) -> Result<OperationId> {
    let context = runtime.task_context(task, fence.clone()).await?;
    let operation = OperationId::from_bytes([byte; 16]);
    let payload = context
        .stage_file_once(
            OperationId::from_bytes([byte + 1; 16]),
            &format!("mailbox/selection-{byte}.json"),
            &crate::contract::canonical_json_bytes(&MailReceiveTaskCommand { after: 0 })?,
            "application/json",
            "selection.json",
        )
        .await?;
    runtime.open_task(task, fence).await?.step(OperationId::from_bytes([byte+2;16]), IdempotencyKey::new(format!("parcel-receive-{byte}"))?, serde_json::json!({"created_command": WorkflowCommand { operation_id:operation, kind:crate::filesystem::MAIL_RECEIVE_TASK_COMMAND_KIND.into(), payload }})).await?;
    Ok(operation)
}

#[tokio::test]
async fn received_created_parcel_never_imports_sender_file_authority() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    fixture.admit_child(created.request.child.clone()).await?;
    let child = TaskId::from_bytes([84; 16]);
    let runtime = fixture.runtime(crate::tool::ToolRegistry::default())?;
    let reservation = fixture.host.observe_task(child).await?.reservation.unwrap();
    let child_fence = LeaseFence::from(&reservation);
    let parent_fence = LeaseFence::from(&fixture.root_lease.reservation);
    let parent = runtime
        .task_context(fixture.root, parent_fence.clone())
        .await?;
    let receiver = runtime.task_context(child, child_fence.clone()).await?;
    let file = parent
        .stage_file_once(
            OperationId::from_bytes([160; 16]),
            "mailbox/parent.txt",
            b"parent steering",
            "text/plain",
            "parent.txt",
        )
        .await?;
    assert!(receiver.read_file(&file).await.is_err());
    let operation = receive(&runtime, child, child_fence.clone(), 164).await?;
    let send = OperationId::from_bytes([161; 16]);
    assert_eq!(parent.send(child, send, file.clone()).await?, file);
    let item = receiver.inbox(0, 1).await?.remove(0);
    assert_eq!(
        receiver
            .read_received_mail(operation, &item, &mut None)
            .await?
            .as_ref(),
        b"parent steering"
    );
    runtime.open_task(child, child_fence.clone()).await?.step(OperationId::from_bytes([167;16]), IdempotencyKey::new("parcel-consume")?, serde_json::json!({"commands":[{"operation_id":operation,"value":Outcome::Succeeded(item.clone())}]})).await?;
    assert_eq!(
        receiver
            .read_received_mail(operation, &item, &mut None)
            .await?
            .as_ref(),
        b"parent steering"
    );
    let copy = receiver
        .stage_file_once(
            OperationId::from_bytes([168; 16]),
            "mailbox/copied.txt",
            &receiver
                .read_received_mail(operation, &item, &mut None)
                .await?,
            "text/plain",
            "copied.txt",
        )
        .await?;
    assert_eq!(copy.volume(), &created.private);
    assert!(receiver.read_file(&file).await.is_err());
    let changed = parent
        .stage_file_once(
            OperationId::from_bytes([175; 16]),
            "mailbox/changed.txt",
            b"changed",
            "text/plain",
            "changed.txt",
        )
        .await?;
    assert!(parent.send(child, send, changed.clone()).await.is_err());
    let owner = runtime
        .task_host()
        .journal_owner(child, child_fence.clone())
        .await?;
    for field in 0..4 {
        let mut forged = item.clone();
        match field {
            0 => forged.task_id = fixture.root,
            1 => forged.message_id = OperationId::from_bytes([176; 16]).to_string(),
            2 => forged.payload = changed.clone(),
            _ => forged.sequence += 1,
        }
        assert!(
            runtime
                .task_host()
                .read_received_mail_owned(&owner, operation, &forged, None)
                .await
                .is_err()
        );
    }
    assert!(
        runtime
            .task_host()
            .read_received_mail_owned(&owner, OperationId::from_bytes([170; 16]), &item, None)
            .await
            .is_err()
    );
    drop((parent, receiver, owner, runtime));
    let reopened = fixture.runtime(crate::tool::ToolRegistry::default())?;
    let parent = reopened.task_context(fixture.root, parent_fence).await?;
    let receiver = reopened.task_context(child, child_fence).await?;
    assert_eq!(parent.send(child, send, file.clone()).await?, file);
    assert_eq!(receiver.inbox(0, 8).await?.len(), 1);
    assert_eq!(
        receiver
            .read_received_mail(operation, &item, &mut None)
            .await?
            .as_ref(),
        b"parent steering"
    );
    assert_eq!(
        receiver
            .observe_staged_file_once(OperationId::from_bytes([168; 16]), None)
            .await?,
        Some(copy)
    );
    assert!(receiver.read_file(&file).await.is_err());
    assert!(
        !receiver
            .scope()
            .grants()
            .contains(&file.volume().capability(VolumeOperation::Read, &mut None)?)
    );
    assert!(
        !parent.scope().grants().contains(
            &created
                .private
                .capability(VolumeOperation::Read, &mut None)?
        )
    );
    Ok(())
}
