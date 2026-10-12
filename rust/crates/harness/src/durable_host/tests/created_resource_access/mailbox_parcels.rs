//! Default transport crosses genuine created agent namespaces without file grants.

use super::*;
use crate::runtime::TaskStateProvider;
use crate::{filesystem::{FilesystemTaskRuntime, MailReceiveTaskCommand}, scheduler::{InboxItem, LeaseFence}, workflow::WorkflowCommand};

type ParcelRuntime = FilesystemTaskRuntime<MemoryStream, MemoryAuthorityBackend, MemoryObjectBackend>;

async fn open_runtime(fixture: &CreationFixture) -> Result<ParcelRuntime> {
    let volume = VolumeRef::new(
        fixture.filesystem_provider.clone(), "scheduler", VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::from_bytes([80; 16])),
    )?;
    FilesystemTaskRuntime::open_with_creation_providers(
        fixture.host.stream.clone(), fixture.filesystem.clone(), volume,
        fixture.host.verifier.clone(), fixture.host.owner_scope.clone(), fixture.root_scope.clone(),
        fixture.host.tasks.clone(), fixture.host.machines.clone(), crate::tool::ToolRegistry::default(),
        crate::scheduler::SessionLimits { active_tasks:16, total_tasks:32, depth:4, model_steps:4 },
        16, 65_536, Arc::new(SystemUnixMillisClock), None, fixture.host.resource_creation.clone(),
        crate::durable_tool::ResumableToolRegistry::default(),
    ).await
}

async fn receive_checkpoint(
    runtime: &ParcelRuntime, task: TaskId, fence: LeaseFence, byte: u8,
) -> Result<OperationId> {
    let context = runtime.task_context(task, fence.clone()).await?;
    let operation = OperationId::from_bytes([byte; 16]);
    let payload = context.stage_file_once(
        OperationId::from_bytes([byte + 1; 16]), &format!("mailbox/selection-{byte}.json"),
        &crate::contract::canonical_json_bytes(&MailReceiveTaskCommand { after:0 })?,
        "application/json", "selection.json",
    ).await?;
    runtime.open_task(task, fence).await?.step(
        OperationId::from_bytes([byte + 2; 16]), IdempotencyKey::new(format!("parcel-receive-{byte}"))?,
        serde_json::json!({"created_command":WorkflowCommand {
            operation_id:operation, kind:crate::filesystem::MAIL_RECEIVE_TASK_COMMAND_KIND.into(), payload,
        }}),
    ).await?;
    Ok(operation)
}

async fn consume_checkpoint(
    runtime: &ParcelRuntime, task: TaskId, fence: LeaseFence, operation: OperationId, item: &InboxItem, byte:u8,
) -> Result<()> {
    runtime.open_task(task, fence).await?.step(
        OperationId::from_bytes([byte; 16]), IdempotencyKey::new(format!("parcel-consume-{byte}"))?,
        serde_json::json!({"commands":[{"operation_id":operation,"value":Outcome::Succeeded(item.clone())}]}),
    ).await?;
    Ok(())
}

#[tokio::test]
async fn default_created_private_parcels_restart_without_importing_donor_file_authority() -> Result<()> {
    let fixture = CreationFixture::new().await?;
    let created = fixture.create_filesystem_child().await?;
    let child = crate::runtime::task_id_for_admission(created.request.child.operation_id);
    assert!(matches!(fixture.admit_child(created.request.child.clone()).await?, Admission::Accepted(id) if id == child));
    let runtime = open_runtime(&fixture).await?;
    let parent_fence = LeaseFence::from(&fixture.root_lease.reservation);
    let child_state = runtime.task_host().observe_task(child).await?;
    let child_fence = LeaseFence::from(child_state.reservation.as_ref()
        .ok_or_else(|| Error::NotFound("parcel child lease".into()))?);
    let parent = runtime.task_context(fixture.root, parent_fence.clone()).await?;
    let receiver = runtime.task_context(child, child_fence.clone()).await?;
    assert!(!receiver.scope().grants().contains("mail:read"));
    assert!(!receiver.scope().grants().contains("mail:send"));
    let parent_file = parent.stage_file_once(
        OperationId::from_bytes([160;16]), "mailbox/parent-after-child.txt", b"parent steering after child admission",
        "text/plain", "parent.txt",
    ).await?;
    assert_ne!(parent_file.volume(), &created.private);
    assert!(!crate::runtime::read_granted(receiver.scope().grants(), &parent_file)?);
    assert!(matches!(receiver.read_file(&parent_file).await, Err(Error::Unauthorized(_))));
    let child_receive = receive_checkpoint(&runtime, child, child_fence.clone(), 164).await?;
    let send = OperationId::from_bytes([161;16]);
    let ack = parent.send(child, send, parent_file.clone()).await?;
    assert_eq!(ack, parent_file);
    let item = receiver.inbox(0, 1).await?.remove(0);
    assert_eq!(receiver.read_received_mail(child_receive, &item, &mut None).await?.as_ref(), b"parent steering after child admission".as_ref());
    consume_checkpoint(&runtime, child, child_fence.clone(), child_receive, &item, 167).await?;
    assert_eq!(receiver.read_received_mail(child_receive, &item, &mut None).await?.as_ref(), b"parent steering after child admission".as_ref());
    let copy_operation = OperationId::from_bytes([168;16]);
    let copy = receiver.stage_file_once(
        copy_operation, "mailbox/received-parent.txt", &receiver.read_received_mail(child_receive, &item, &mut None).await?,
        parent_file.descriptor().media_type(), parent_file.display_name(),
    ).await?;
    assert_eq!(copy.volume(), &created.private);
    assert_eq!(receiver.read_file(&copy).await?.as_ref(), b"parent steering after child admission".as_ref());
    assert!(matches!(receiver.read_file(&parent_file).await, Err(Error::Unauthorized(_))));
    let child_file = receiver.stage_file_once(
        OperationId::from_bytes([169;16]), "mailbox/child-reply.txt", b"child answer from its private namespace",
        "text/plain", "child.txt",
    ).await?;
    assert!(!crate::runtime::read_granted(parent.scope().grants(), &child_file)?);
    assert!(matches!(parent.read_file(&child_file).await, Err(Error::Unauthorized(_))));
    let parent_receive = receive_checkpoint(&runtime, fixture.root, parent_fence.clone(), 170).await?;
    receiver.send(fixture.root, OperationId::from_bytes([173;16]), child_file.clone()).await?;
    let reply = parent.inbox(0,1).await?.remove(0);
    assert_eq!(parent.read_received_mail(parent_receive, &reply, &mut None).await?.as_ref(), b"child answer from its private namespace".as_ref());
    consume_checkpoint(&runtime, fixture.root, parent_fence.clone(), parent_receive, &reply, 174).await?;
    let changed = parent.stage_file_once(
        OperationId::from_bytes([175;16]), "mailbox/changed.txt", b"changed", "text/plain", "changed.txt",
    ).await?;
    assert!(matches!(parent.send(child, send, changed.clone()).await, Err(Error::Conflict(_))));
    let owner = runtime.task_host().journal_owner(child, child_fence.clone()).await?;
    for mutation in 0..4 {
        let mut forged = item.clone();
        match mutation {
            0 => forged.task_id = fixture.root,
            1 => forged.message_id = OperationId::from_bytes([176;16]).to_string(),
            2 => forged.payload = changed.clone(),
            _ => forged.sequence += 1,
        }
        assert!(runtime.task_host().read_received_mail_owned(&owner, child_receive, &forged, None).await.is_err());
    }
    assert!(runtime.task_host().read_received_mail_owned(&owner, parent_receive, &item, None).await.is_err());
    let mut stale = child_fence.clone();
    stale.reservation_id.push_str("-stale");
    assert!(async {
        let owner = runtime.task_host().journal_owner(child, stale).await?;
        runtime.task_host().read_received_mail_owned(&owner, child_receive, &item, None).await
    }.await.is_err());
    drop(receiver);
    drop(parent);
    drop(runtime);
    let reopened = open_runtime(&fixture).await?;
    let parent = reopened.task_context(fixture.root, parent_fence).await?;
    assert_eq!(parent.send(child, send, parent_file.clone()).await?, ack);
    let receiver = reopened.task_context(child, child_fence).await?;
    assert_eq!(receiver.inbox(0, 8).await?.len(), 1);
    assert_eq!(receiver.read_received_mail(child_receive, &item, &mut None).await?.as_ref(), b"parent steering after child admission".as_ref());
    assert_eq!(receiver.observe_staged_file_once(copy_operation).await?, Some(copy));
    assert!(matches!(receiver.read_file(&parent_file).await, Err(Error::Unauthorized(_))));
    assert!(!receiver.scope().grants().contains(&parent_file.volume().capability(VolumeOperation::Read)?));
    assert!(!parent.scope().grants().contains(&created.private.capability(VolumeOperation::Read)?));
    Ok(())
}
