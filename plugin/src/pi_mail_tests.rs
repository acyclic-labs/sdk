use super::*;
use acyclic_harness::runtime::TaskStateProvider;
use crate::pi_fork::tests::work;
use crate::pi_fork::tests::Passive;
use acyclic_harness::{Admission, AgentId, Capabilities,
    conversation::{ContentGrant, FileRef, Limits, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer, Scope},
    distributed::{WorkLease, WorkPull, Worker},
    resources::ProviderRef,
    runtime::{DurableTaskHost, RuntimeScope, TaskDefinition, TaskRegistry}, scheduler::{ResourceSnapshot, SessionLimits},
    tool::ToolRegistry, workflow::{MachineIdentity, MachineRegistry}};
use acyclic_fs::{CancellationToken, Fs, MemoryAuthorityBackend, MemoryObjectBackend};
use acyclic_stream::{MemoryStream};
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc};

type Runtime = FilesystemTaskRuntime<MemoryStream, MemoryAuthorityBackend, MemoryObjectBackend>;
struct Fixture {
    stream: std::sync::Arc<MemoryStream>, host: Arc<FilesystemHost<MemoryAuthorityBackend, MemoryObjectBackend>>,
    volume: VolumeRef, issuer: AuthorityIssuer, signed: Scope, scope: RuntimeScope,
    tasks: TaskRegistry, machines: MachineRegistry,
}
impl Fixture {
    async fn new() -> Result<Self> {
        let provider = ProviderRef::new("pi-mail".into(), "filesystem".into(), "2".into(), &mut None)?;
        let host = Arc::new(FilesystemHost::new(Fs::memory(), provider.clone())?);
        let agent = AgentId::from_bytes([71;16]);
        let volume = VolumeRef::new(provider, "journal", VolumeClass::AgentPrivate, VolumeOwner::Agent(agent))?;
        host.create_volume(&volume).await?;
        let issuer = AuthorityIssuer::new("pi-mail", [72;32], Authority { kind: AggregateKind::Task, id: "mail-original".into() });
        let grants = Capabilities::new([
            "operation:declare", "operation:observe", "operation:cancel",
            "task:spawn:pi.mail.test@1", "mail:send", "mail:read",
        ].into_iter().map(str::to_owned).chain([volume.directory_read_capability("pi-inputs", &mut None)?]));
        let signed = issuer.issue("owner", &Capabilities::new(grants.iter().map(str::to_owned).chain([
            volume.capability(VolumeOperation::Read, &mut None)?, volume.capability(VolumeOperation::Write, &mut None)?,
        ])), Some(agent), None, &mut None).expect("scope proof");
        let scope = RuntimeScope::new(grants, Limits::default())?;
        let machine = Arc::new(Passive { identity: MachineIdentity { name: "pi.mail.test".into(), version: "1".into(), digest: [73; 32] }, schema: json!({"type":"object"}) });
        let mut tasks = TaskRegistry::default();
        tasks.register(TaskDefinition::<Value, Value>::resumable(machine.clone(), json!({"type":"object"}), json!({}))?)?;
        let mut machines = MachineRegistry::default(); machines.register(machine, &mut None)?;
        Ok(Self { stream: Arc::new(MemoryStream::default()), host, volume, issuer, signed, scope, tasks, machines })
    }
    async fn runtime(&self) -> Result<Arc<Runtime>> {
        Ok(Arc::new(Runtime::open(self.stream.clone(), self.host.clone(), self.volume.clone(), self.issuer.verifier(&mut None).unwrap(),
            self.signed.clone(), self.scope.clone(), self.tasks.clone(), self.machines.clone(), ToolRegistry::default(),
            SessionLimits { active_tasks:8, total_tasks:8, depth:4, model_steps:8 }, 8, 65_536).await?))
    }
    async fn file(&self, operation: OperationId, path: &str, bytes: &[u8]) -> Result<FileRef> {
        let grant = ContentGrant::verify(&self.issuer.verifier(&mut None).unwrap(), &self.signed, &self.volume, VolumeOperation::Write)?;
        self.host.put_content(&self.volume, &grant, &format!("pi-inputs/{path}"), bytes,
            "application/json", path, 65_536, &acyclic_harness::IdempotencyKey::new(format!("mail:{operation}:{path}"))?).await
    }
    async fn pull(&self, runtime: &Runtime) -> Result<WorkLease> {
        match runtime.task_host().pull_work(&Worker { id:"pi-mail-worker".into(), available:ResourceSnapshot::default(), labels:BTreeMap::new() }).await? {
            WorkPull::Claimed(lease) => { runtime.task_host().start_task(&lease, None).await?; Ok(lease) }
            _ => Err(Error::NotFound("mail lease".into())),
        }
    }
    async fn admit(&self, runtime: &Runtime) -> Result<(WorkLease, TaskId)> {
        let definition = self.tasks.get_version::<Value, Value>("pi.mail.test", "1", None)?;
        let operation = OperationId::from_bytes([73;16]);
        assert!(matches!(runtime.harness().admit(operation, &definition, json!({}), None).await?, Admission::Accepted(_)));
        Ok((self.pull(runtime).await?, TaskId::from_bytes(operation.into_bytes())))
    }
    async fn child(&self, runtime: &Runtime, parent: TaskId, fence: LeaseFence, byte: u8) -> Result<TaskId> {
        let request = runtime.harness().registered_child_admission(OperationId::from_bytes([byte;16]), "pi.mail.test", "1", json!({}), parent, &self.scope)?;
        match runtime.task_host().admit_owned(request, fence).await? { Admission::Accepted(child) => Ok(child), _ => Err(Error::Invalid("mail child admission".into())) }
    }
}

#[tokio::test]
async fn actual_mailbox_lost_ack_exact_sequence_and_original_sender() -> Result<()> {
    let fixture = Fixture::new().await?;
    let work = work();
    let runtime = fixture.runtime().await?;
    let (lease, parent) = fixture.admit(&runtime).await?;
    let fence = LeaseFence::from(&lease.reservation);
    let child = fixture.child(&runtime, parent, fence.clone(), 74).await?;
    let other = fixture.child(&runtime, parent, fence.clone(), 75).await?;
    let operation = OperationId::from_bytes([76;16]);
    let envelope = PiMailEnvelope { kind:PiMailKind::Steer, message:"real steering".into() };
    let payload = fixture.file(operation, "envelope.json", &serde_json::to_vec(&PiMailPayload { sender: parent, envelope: envelope.clone() }).unwrap()).await?;
    let command_payload = fixture.file(operation, "send.json", &serde_json::to_vec(&MailSendTaskCommand { recipient:child, payload:payload.clone() }).unwrap()).await?;
    let command = WorkflowCommand { operation_id:operation, kind:MAIL_SEND_TASK_COMMAND_KIND.into(), payload:command_payload };
    let context = runtime.task_context_with_native_work(parent, &fence, &work).await?;
    let owner = runtime.task_host().journal_owner_with_native_work(parent, &fence, &work).await?;
    let sent = execute_send(&runtime, context.clone(), &owner, fence.clone(), command.clone(), &work).await?;
    assert!(matches!(sent, TaskCommandProgress::ReadyWithNativeCustody { .. }));
    drop(sent);
    drop(runtime);
    let reopened = fixture.runtime().await?;
    let replay = execute_send(&reopened, reopened.task_context_with_native_work(parent, &fence, &work).await?, &owner,
        fence.clone(), command, &work).await?;
    assert!(matches!(replay, TaskCommandProgress::ReadyWithNativeCustody { .. }));
    drop(replay);
    assert_eq!(reopened.task_host().inbox(child, 0, 2).await?.len(), 1);
    assert!(reopened.task_host().send_owned(parent, fence.clone(), TaskId::from_bytes([99;16]), OperationId::from_bytes([77;16]), payload.clone()).await.is_err());
    let child_lease = fixture.pull(&reopened).await?;
    assert_eq!(child_lease.operation.operation_id.into_bytes(), child.into_bytes());
    let child_fence = LeaseFence::from(&child_lease.reservation);
    let child_context = reopened.task_context(child, child_fence.clone()).await?;
    let child_owner = reopened.task_host().journal_owner(child, child_fence.clone()).await?;
    assert!(authenticate(context.context(), &child_owner, &work).await.is_err());
    let binding = crate::PiTaskBinding { native_session_id:"actual-child".into(), task_id:child,
        agent_id:AgentId::from_bytes([71;16]), project_volumes:BTreeMap::new(),
        results: None,
        scratch:crate::PiScratchBinding { volume:fixture.volume.clone(), allocation:crate::PiScratchAllocation::Preparing } };
    let baseline = work.live_allocation_bytes();
    let (received, received_custody) = receive(&reopened, &binding, 0, 1, &work).await?;
    let received = received.unwrap();
    assert_eq!(received.item.sequence, 1);
    assert_eq!(received.envelope, envelope);
    assert_eq!(received.sender, parent);
    assert!(received_custody.work().shares_ledger(&work));
    assert!(work.live_allocation_bytes() > baseline);
    let (repeated, repeated_custody) = receive(&reopened, &binding, 0, 1, &work).await?;
    assert_eq!(repeated.unwrap().item, received.item);
    drop(repeated_custody);
    let (empty, empty_custody) = receive(&reopened, &binding, 1, 1, &work).await?;
    assert!(empty.is_none());
    drop(empty);
    drop(empty_custody);
    assert!(receive(&reopened, &binding, 0, 2, &work).await.is_err());
    let (observed, observed_custody) = wait(&reopened, &binding, parent, 0, |caller, selected| {
        assert_eq!(caller, child); assert_eq!(selected, parent); Ok(())
    }, &work).await?;
    assert!(matches!(observed.state, PiWaitState::Pending));
    assert_eq!(observed.task, parent);
    assert_eq!(observed.mail.as_ref().unwrap().item.sequence, 1);
    drop(observed);
    drop(observed_custody);
    assert!(wait(&reopened, &binding, TaskId::from_bytes([98;16]), 0, |_, _| Ok(()), &work).await.is_err());
    drop(received);
    drop(received_custody);
    assert!(child_context.send(other, OperationId::from_bytes([80;16]), payload.clone()).await.is_err());
    child_context.send(parent, OperationId::from_bytes([81;16]), payload.clone()).await?;
    let mut stale = child_fence.clone(); stale.reservation_id.push_str("-replaced");
    assert!(reopened.task_host().send_owned(child, stale, parent, OperationId::from_bytes([82;16]), payload).await.is_err());
    reopened.task_host().cancel_child_owned(parent, &fence, child, OperationId::from_bytes([83;16]), &mut None).await?;
    assert!(child_owner.verify_dispatch().await.is_err());
    child_owner.verify_settlement().await?;
    assert!(authenticate(&child_context, &child_owner, &work).await.is_err());
    Ok(())
}

#[tokio::test]
async fn actual_passive_checkpoint_owner_replacement_rejects_original_mail_dispatch() -> Result<()> {
    let fixture = Fixture::new().await?;
    let work = work();
    let runtime = fixture.runtime().await?;
    let (parent_lease, parent) = fixture.admit(&runtime).await?;
    let child = fixture.child(&runtime, parent, LeaseFence::from(&parent_lease.reservation), 84).await?;
    let lease = fixture.pull(&runtime).await?;
    let original_fence = LeaseFence::from(&lease.reservation);
    let owner = runtime.task_host().journal_owner(child, original_fence.clone()).await?;
    let original_context = runtime.task_context(child, original_fence.clone()).await?;
    assert!(matches!(runtime.resume_task(lease, &runtime.commands(), 1).await,
        acyclic_harness::filesystem::TaskWorkerAttempt::Progress(
            acyclic_harness::filesystem::TaskWorkerOutcome::Suspended { .. })));
    let suspension = runtime.task_host().workflow_suspension(child).await?
        .ok_or_else(|| Error::NotFound("actual child suspension".into()))?;
    let operation = OperationId::from_bytes([85;16]);
    let wake = fixture.file(operation, "replacement-wake.json", b"{}").await?;
    runtime.task_host().resume_workflow(child, suspension.revision, wake, acyclic_harness::IdempotencyKey::new("pi-mail-original-checkpoint-wake")?, None).await?;
    let replacement = fixture.pull(&runtime).await?;
    assert_eq!(replacement.operation.operation_id.into_bytes(), child.into_bytes());
    let replacement_fence = LeaseFence::from(&replacement.reservation);
    assert_ne!(original_fence, replacement_fence);
    assert!(authenticate(&original_context, &owner, &work).await.is_err());
    let payload = fixture.file(operation, "replacement-envelope.json",
        &serde_json::to_vec(&PiMailPayload { sender:child, envelope:PiMailEnvelope { kind:PiMailKind::Message, message:"original lease cannot send".into() } }).unwrap()).await?;
    assert!(runtime.task_host().send_owned(child, original_fence, parent, operation, payload.clone()).await.is_err());
    runtime.task_context(child, replacement_fence).await?.send(parent, operation, payload).await?;
    assert_eq!(runtime.task_host().inbox(parent, 0, 1).await?.len(), 1);
    Ok(())
}

#[test]
fn bounded_envelope_rejects_unknown_fields_and_oversized_bodies() {
    let work = work();
    assert!(decode::<PiMailEnvelope>(br#"{"kind":"steer","message":"x","sender":"root"}"#, &work).is_err());
    assert!(decode::<PiMailEnvelope>(&vec![b' '; MAXIMUM_PI_MAIL_BYTES as usize + 1], &work).is_err());
    assert!(validate_envelope(&PiMailEnvelope { kind:PiMailKind::Message, message:"x".repeat(MAXIMUM_PI_MAIL_BYTES as usize) }).is_err());
}

#[tokio::test]
async fn native_inbox_bounds_and_cancellation_reject_before_io() -> Result<()> {
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime().await?;
    let (_, task) = fixture.admit(&runtime).await?;
    let binding = crate::PiTaskBinding { native_session_id:"actual-parent".into(), task_id:task,
        agent_id:AgentId::from_bytes([71;16]), project_volumes:BTreeMap::new(), results:None,
        scratch:crate::PiScratchBinding { volume:fixture.volume.clone(), allocation:crate::PiScratchAllocation::Preparing } };
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(crate::pi_root_provider::allocation_budget(), &cancellation).unwrap();
    let before = work.receipt();
    let live = work.live_allocation_bytes();
    cancellation.cancel();
    let failure = receive(&runtime, &binding, 0, 1, &work).await.err().unwrap();
    assert!(matches!(&failure, Error::NativeWork { cause:NativeTreeMaterializationError::Cancelled, .. }));
    drop(failure);
    assert_eq!(work.receipt(), before);
    assert_eq!(work.live_allocation_bytes(), live);
    let failure = receive(&runtime, &binding, 0, 2, &work).await.err().unwrap();
    assert!(matches!(failure, Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Invalid,
        ..
    }));
    assert_eq!(work.receipt(), before);
    assert_eq!(work.live_allocation_bytes(), live);
    Ok(())
}
