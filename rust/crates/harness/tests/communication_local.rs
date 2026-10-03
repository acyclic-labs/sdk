//! Production communication recovery against LocalStream and LocalFs.
#![cfg(feature = "filesystem-local")]
#![allow(clippy::too_many_lines)]

use acyclic_fs::{LocalAuthorityBackend, LocalFs, LocalObjectBackend, LocalOptions};
use acyclic_harness::communication::DurableWaitStore;
use acyclic_harness::{
    Admission, AgentId, Capabilities, Error, IdempotencyKey, OperationId, Result, TaskId,
    communication::{
        DurableCommunication, MessageRequest, MessageTarget, StreamWaitStore, WaitCompletion,
        WaitRequest, WaitTarget,
    },
    conversation::{FileRef, Limits, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer},
    distributed::DistributedCoordinator,
    durable_host::CoordinatorTaskHost,
    filesystem::{FilesystemContentVerifier, FilesystemHost, FilesystemSchedulerPayloadStore},
    resources::ProviderRef,
    runtime::{DurableTaskHost, RuntimeScope, TaskAdmissionRecord, TaskRegistry},
    workflow::{
        MachineIdentity, MachineRegistry, MachineStatus, MachineTransition, ResumableMachine,
    },
};
use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient};
use serde_json::Value;
use std::{path::Path, sync::Arc};

type FsHost = FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>;
type Stream = acyclic_stream::LocalStream;
type Host = CoordinatorTaskHost<Stream>;

struct Fixture {
    stream: StreamClient<Stream>,
    fs: Arc<FsHost>,
    volume: VolumeRef,
    scope: acyclic_harness::core::Scope,
    runtime_scope: RuntimeScope,
    issuer: AuthorityIssuer,
    owner: Authority,
    task: acyclic_harness::registry::ComponentIdentity,
    machine: MachineIdentity,
    tasks: TaskRegistry,
    machines: MachineRegistry,
}

struct SuspendedMachine(MachineIdentity);
impl ResumableMachine for SuspendedMachine {
    fn identity(&self) -> &MachineIdentity {
        &self.0
    }
    fn state_schema(&self) -> &Value {
        static SCHEMA: std::sync::OnceLock<Value> = std::sync::OnceLock::new();
        SCHEMA.get_or_init(|| serde_json::json!({"type":"object"}))
    }
    fn initialize(&self, _input: &Value) -> Result<Value> {
        Ok(serde_json::json!({}))
    }
    fn transition(&self, state: &Value, _input: &Value) -> Result<MachineTransition> {
        Ok(MachineTransition {
            state: state.clone(),
            commands: Vec::new(),
            status: MachineStatus::Suspended,
        })
    }
}

impl Fixture {
    async fn open(root: &Path) -> Result<Self> {
        let provider = ProviderRef::new("communication-local", "filesystem", "2")?;
        let agent = AgentId::from_bytes([6; 16]);
        let volume = VolumeRef::new(
            provider.clone(),
            "messages",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
        )?;
        let owner = Authority {
            kind: AggregateKind::Task,
            id: "communication-local-owner".into(),
        };
        let issuer = AuthorityIssuer::new("communication-local", [8; 32], owner.clone());
        let fs = Arc::new(FilesystemHost::new(
            LocalFs::local(LocalOptions::new(root.join("filesystem")))
                .await
                .map_err(|e| Error::Storage(e.to_string()))?,
            provider.clone(),
        )?);
        fs.create_volume(&volume).await?;
        let mut grants = vec![
            "operation:declare".into(),
            "operation:observe".into(),
            "operation:cancel".into(),
            "mail:send".into(),
            "mail:read".into(),
            "timer:wait".into(),
            "task:spawn:communication.test@1".into(),
        ];
        grants.push(volume.capability(VolumeOperation::Read)?);
        grants.push(volume.capability(VolumeOperation::Write)?);
        let scope = issuer.root_for_agent(agent, "owner", Capabilities::new(grants));
        let limits = Limits::default();
        let runtime_scope = RuntimeScope::new(scope.capabilities().clone(), limits)?;
        let machine = MachineIdentity {
            name: "communication.test".into(),
            version: "1".into(),
            digest: [4; 32],
        };
        let implementation: Arc<dyn ResumableMachine> = Arc::new(SuspendedMachine(machine.clone()));
        let mut machines = MachineRegistry::default();
        machines.register(implementation.clone())?;
        let definition = acyclic_harness::runtime::TaskDefinition::<Value, Value>::resumable(
            implementation,
            serde_json::json!({"type":"object"}),
            serde_json::json!({"type":"object"}),
        )?;
        let task = definition.identity().clone();
        let mut tasks = TaskRegistry::default();
        tasks.register(definition)?;
        let stream = StreamClient::new(Arc::new(
            LocalStream::open(root.join("history"), LocalStreamLimits::default())
                .await
                .map_err(|e| Error::Storage(e.to_string()))?,
        ));
        Ok(Self {
            stream,
            fs,
            volume,
            scope,
            runtime_scope,
            issuer,
            owner,
            task,
            machine,
            tasks,
            machines,
        })
    }

    async fn host(&self) -> Result<Arc<Host>> {
        let verifier = Arc::new(FilesystemContentVerifier::new(
            self.fs.clone(),
            self.issuer.verifier(),
            self.scope.clone(),
            self.runtime_scope.limits().file_bytes,
        )?);
        let stager = Arc::new(FilesystemSchedulerPayloadStore::new(
            self.fs.clone(),
            self.volume.clone(),
            &self.issuer.verifier(),
            &self.scope,
            self.runtime_scope.limits().file_bytes,
        )?);
        let coordinator = DistributedCoordinator::open(&self.stream, verifier.clone())
            .await?
            .with_payload_store(stager.clone());
        Ok(Arc::new(CoordinatorTaskHost::new(
            coordinator,
            self.stream.clone(),
            stager,
            verifier,
            self.owner.clone(),
            self.scope.clone(),
            self.issuer.verifier(),
            self.runtime_scope.clone(),
            self.tasks.clone(),
            self.machines.clone(),
            Arc::new(acyclic_stream::SystemUnixMillisClock),
        )?))
    }

    fn admission(
        &self,
        operation: OperationId,
        parent: Option<TaskId>,
    ) -> Result<TaskAdmissionRecord> {
        TaskAdmissionRecord::from_parts(
            operation,
            &self.task.name,
            &self.task.version,
            serde_json::json!({}),
            serde_json::json!({"type":"object"}),
            serde_json::json!({"type":"object"}),
            &std::collections::BTreeSet::new(),
            &self.machine.digest,
            parent,
            self.runtime_scope.grants().clone(),
            self.runtime_scope.limits(),
            self.runtime_scope.run_limits(),
            None,
            None,
            None,
        )
    }
}

async fn payload(fixture: &Fixture, _operation: OperationId) -> Result<FileRef> {
    let grant = acyclic_harness::conversation::ContentGrant::verify(
        &fixture.issuer.verifier(),
        &fixture.scope,
        &fixture.volume,
        VolumeOperation::Write,
    )?;
    Ok(fixture
        .fs
        .put_content(
            &fixture.volume,
            &grant,
            "messages/body.json",
            b"{}",
            "application/json",
            "body.json",
            fixture.runtime_scope.limits().file_bytes,
            &IdempotencyKey::new("message-payload")?,
        )
        .await?)
}

async fn alternate_payload(fixture: &Fixture) -> Result<FileRef> {
    let grant = acyclic_harness::conversation::ContentGrant::verify(
        &fixture.issuer.verifier(),
        &fixture.scope,
        &fixture.volume,
        VolumeOperation::Write,
    )?;
    Ok(fixture
        .fs
        .put_content(
            &fixture.volume,
            &grant,
            "messages/alternate.json",
            b"{\"alternate\":true}",
            "application/json",
            "alternate.json",
            fixture.runtime_scope.limits().file_bytes,
            &IdempotencyKey::new("alternate-message-payload")?,
        )
        .await?)
}

#[tokio::test]
async fn local_stream_and_filesystem_mail_reopens_idempotently() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|e| Error::Storage(e.to_string()))?;
    let fixture = Fixture::open(directory.path()).await?;
    let host = fixture.host().await?;
    let parent = TaskId::from_bytes([1; 16]);
    let child = TaskId::from_bytes([2; 16]);
    let sibling = TaskId::from_bytes([4; 16]);
    assert!(
        matches!(host.admit(fixture.admission(OperationId::from_bytes(parent.into_bytes()), None)?).await?, Admission::Accepted(id) if id == parent)
    );
    assert!(
        matches!(host.admit(fixture.admission(OperationId::from_bytes(child.into_bytes()), Some(parent))?).await?, Admission::Accepted(id) if id == child)
    );
    assert!(
        matches!(host.admit(fixture.admission(OperationId::from_bytes(sibling.into_bytes()), Some(parent))?).await?, Admission::Accepted(id) if id == sibling)
    );
    let body = payload(&fixture, OperationId::from_bytes([3; 16])).await?;
    let request = MessageRequest {
        sender: parent,
        recipient: child,
        message_id: OperationId::from_bytes([3; 16]),
        target: MessageTarget::Child,
        payload: body,
    };
    let communication = DurableCommunication::new(host.clone());
    communication.send(request.clone()).await?;
    communication.send(request.clone()).await?;
    let mut conflicting = request.clone();
    conflicting.payload = alternate_payload(&fixture).await?;
    assert!(matches!(
        communication.send(conflicting).await,
        Err(Error::Conflict(_))
    ));
    // The same caller message ID is valid on another endpoint pair. The
    // durable stream key must include both endpoints rather than collapsing
    // these two independently idempotent deliveries.
    communication
        .send(MessageRequest {
            sender: child,
            recipient: parent,
            message_id: request.message_id,
            target: MessageTarget::Parent,
            payload: body.clone(),
        })
        .await?;
    // A low-level host caller still cannot bypass the direct parent/child
    // relationship enforced by the typed communication adapter.
    assert!(matches!(
        host.send(child, sibling, request.message_id, body.clone()).await,
        Err(Error::Unauthorized(_))
    ));
    drop(communication);
    drop(host);
    drop(fixture);
    let reopened_fixture = Fixture::open(directory.path()).await?;
    let reopened = reopened_fixture.host().await?;
    let items = DurableCommunication::new(reopened.clone())
        .inbox(child, 0, 8)
        .await?;
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].message_id, request.message_id.to_string());
    let parent_items = DurableCommunication::new(reopened)
        .inbox(parent, 0, 8)
        .await?;
    assert_eq!(parent_items.len(), 1);
    assert_eq!(parent_items[0].message_id, request.message_id.to_string());
    Ok(())
}

#[tokio::test]
async fn local_wait_timeout_and_cancellation_are_typed() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|e| Error::Storage(e.to_string()))?;
    let fixture = Fixture::open(directory.path()).await?;
    let host = fixture.host().await?;
    let wait_store = Arc::new(StreamWaitStore::new(fixture.stream.clone()));
    let parent = TaskId::from_bytes([11; 16]);
    assert!(
        matches!(host.admit(fixture.admission(OperationId::from_bytes(parent.into_bytes()), None)?).await?, Admission::Accepted(id) if id == parent)
    );
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| Error::Invalid(e.to_string()))?
        .as_millis() as u64;
    let timeout_request = WaitRequest {
        operation_id: OperationId::from_bytes([12; 16]),
        waiter: parent,
        target: WaitTarget::Messages {
            task_id: parent,
            after: 0,
            limit: 8,
        },
        timeout_epoch_ms: Some(now + 50),
        cancellation_id: None,
    };
    let timed = DurableCommunication::new(host.clone())
        .with_wait_store(wait_store.clone())
        .wait(timeout_request.clone(), None)
        .await?;
    assert!(matches!(timed, WaitCompletion::TimedOut));
    let (sender, receiver) = tokio::sync::watch::channel(false);
    let cancel_request = WaitRequest {
        operation_id: OperationId::from_bytes([13; 16]),
        waiter: parent,
        target: WaitTarget::Messages {
            task_id: parent,
            after: 0,
            limit: 8,
        },
        timeout_epoch_ms: None,
        cancellation_id: None,
    };
    let task_wait_store = wait_store.clone();
    let task_cancel_request = cancel_request.clone();
    let waiting = tokio::spawn(async move {
        DurableCommunication::new(host)
            .with_wait_store(task_wait_store)
            .wait(task_cancel_request, Some(receiver))
            .await
    });
    sender
        .send(true)
        .map_err(|e| Error::Storage(e.to_string()))?;
    assert!(matches!(
        waiting.await.map_err(|e| Error::Storage(e.to_string()))??,
        WaitCompletion::Cancelled
    ));
    drop(wait_store);
    drop(fixture);
    let reopened_fixture = Fixture::open(directory.path()).await?;
    let reopened_store = StreamWaitStore::new(reopened_fixture.stream.clone());
    assert_eq!(
        reopened_store.open(timeout_request).await?,
        Some(WaitCompletion::TimedOut)
    );
    assert_eq!(
        reopened_store.open(cancel_request).await?,
        Some(WaitCompletion::Cancelled)
    );
    Ok(())
}
