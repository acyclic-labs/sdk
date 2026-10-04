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
    communication_tools::{MessageToolInput, MessageToolOutput, WaitToolInput, WaitToolOutput},
    conversation::{
        ContentGrant, FileRef, Limits, MessageKind, VolumeClass, VolumeOperation, VolumeOwner,
        VolumeRef,
    },
    core::{AggregateKind, Authority, AuthorityIssuer},
    distributed::DistributedCoordinator,
    durable_host::CoordinatorTaskHost,
    filesystem::{
        FilesystemContentVerifier, FilesystemHost, FilesystemSchedulerPayloadStore,
        LocalSwarmBindings, PersistentLocalHarness, PersistentLocalSwarm,
    },
    model::{Model, ModelAttempt, ModelEvent, ModelProvider},
    resources::ProviderRef,
    runtime::{DurableTaskHost, RuntimeScope, TaskAdmissionRecord, TaskRegistry},
    tool::{ToolInvocation, ToolResult},
    workflow::{
        MachineIdentity, MachineRegistry, MachineStatus, MachineTransition, ResumableMachine,
    },
};
use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient};
use bytes::Bytes;
use futures::{TryStreamExt, future::BoxFuture, stream::BoxStream};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

type FsHost = FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>;
type Stream = acyclic_stream::LocalStream;
type Host = CoordinatorTaskHost<Stream>;

fn canonical_json_bytes(value: &Value) -> Result<Vec<u8>> {
    fn canonical(value: &Value) -> Value {
        match value {
            Value::Array(items) => Value::Array(items.iter().map(canonical).collect()),
            Value::Object(fields) => Value::Object(
                fields
                    .iter()
                    .map(|(key, value)| (key.clone(), canonical(value)))
                    .collect::<BTreeMap<_, _>>()
                    .into_iter()
                    .collect(),
            ),
            value => value.clone(),
        }
    }
    serde_json::to_vec(&canonical(value)).map_err(|error| Error::Invalid(error.to_string()))
}

struct CoordinatorCommunicationModel {
    payload: FileRef,
    recipient: TaskId,
    target: &'static str,
    deadline_epoch_ms: u64,
    completion_metadata: Value,
    calls: AtomicUsize,
    captured:
        Arc<std::sync::Mutex<Vec<(Vec<u8>, acyclic_harness::model_input::ModelInputManifest)>>>,
}

impl ModelProvider for CoordinatorCommunicationModel {
    fn generate<'a>(
        &'a self,
        prepared: acyclic_harness::model_input::PreparedModelInput,
    ) -> BoxStream<'a, Result<ModelEvent>> {
        if self.calls.fetch_add(1, Ordering::SeqCst) > 0 {
            return Box::pin(futures::stream::iter([Ok(ModelEvent::Completed {
                metadata: self.completion_metadata.clone(),
            })]));
        }
        self.captured
            .lock()
            .expect("model input capture lock")
            .push((prepared.bytes().to_vec(), prepared.manifest().clone()));
        Box::pin(futures::stream::iter([
            Ok(ModelEvent::ToolCall {
                call_id: "message-call".into(),
                name: "swarm.message".into(),
                arguments: json!({
                    "recipient": self.recipient.to_string(),
                    "target": self.target,
                    "payload": self.payload,
                }),
            }),
            Ok(ModelEvent::ToolCall {
                call_id: "wait-call".into(),
                name: "swarm.wait".into(),
                arguments: json!({
                    "kind": "deadline",
                    "deadline_epoch_ms": self.deadline_epoch_ms,
                }),
            }),
            Ok(ModelEvent::Completed {
                metadata: self.completion_metadata.clone(),
            }),
        ]))
    }

    fn reconcile<'a>(
        &'a self,
        _attempt: ModelAttempt,
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async { Ok(None) })
    }
}

struct CompletionMetadataModel;

impl ModelProvider for CompletionMetadataModel {
    fn generate<'a>(
        &'a self,
        _prepared: acyclic_harness::model_input::PreparedModelInput,
    ) -> BoxStream<'a, Result<ModelEvent>> {
        Box::pin(futures::stream::iter([Ok(ModelEvent::Completed {
            metadata: json!({"ui_state": "ui-only-secret"}),
        })]))
    }

    fn reconcile<'a>(
        &'a self,
        _attempt: ModelAttempt,
    ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async { Ok(None) })
    }
}

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

async fn marker_payload(fixture: &Fixture, path: &str, bytes: &[u8], key: &str) -> Result<FileRef> {
    let grant = ContentGrant::verify(
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
            path,
            bytes,
            "text/plain",
            path.rsplit('/').next().unwrap_or(path),
            fixture.runtime_scope.limits().file_bytes,
            &IdempotencyKey::new(key)?,
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
    let second_child = TaskId::from_bytes([5; 16]);
    assert!(
        matches!(host.admit(fixture.admission(OperationId::from_bytes(parent.into_bytes()), None)?).await?, Admission::Accepted(id) if id == parent)
    );
    assert!(
        matches!(host.admit(fixture.admission(OperationId::from_bytes(child.into_bytes()), Some(parent))?).await?, Admission::Accepted(id) if id == child)
    );
    assert!(
        matches!(host.admit(fixture.admission(OperationId::from_bytes(sibling.into_bytes()), Some(parent))?).await?, Admission::Accepted(id) if id == sibling)
    );
    assert!(
        matches!(host.admit(fixture.admission(OperationId::from_bytes(second_child.into_bytes()), Some(parent))?).await?, Admission::Accepted(id) if id == second_child)
    );
    let body = payload(&fixture, OperationId::from_bytes([3; 16])).await?;
    let request = MessageRequest {
        sender: parent,
        recipient: child,
        message_id: OperationId::from_bytes([3; 16]),
        target: MessageTarget::Child,
        payload: body.clone(),
    };
    let communication = DurableCommunication::new(host.clone());
    communication.send(request.clone()).await?;
    communication.send(request.clone()).await?;
    // The same message ID is independently idempotent for each child
    // endpoint.  A durable key that omitted the recipient would collapse
    // this delivery into the first child.
    communication
        .send(MessageRequest {
            sender: parent,
            recipient: second_child,
            message_id: request.message_id,
            target: MessageTarget::Child,
            payload: body.clone(),
        })
        .await?;
    let alternate = alternate_payload(&fixture).await?;
    let mut conflicting = request.clone();
    conflicting.payload = alternate.clone();
    assert!(matches!(
        communication.send(conflicting).await,
        Err(Error::Conflict(_))
    ));
    communication
        .send(MessageRequest {
            sender: parent,
            recipient: child,
            message_id: OperationId::from_bytes([4; 16]),
            target: MessageTarget::Child,
            payload: alternate,
        })
        .await?;
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
    communication
        .send(MessageRequest {
            sender: second_child,
            recipient: parent,
            message_id: request.message_id,
            target: MessageTarget::Parent,
            payload: body.clone(),
        })
        .await?;
    // A low-level host caller still cannot bypass the direct parent/child
    // relationship enforced by the typed communication adapter.
    assert!(matches!(
        host.send(child, sibling, request.message_id, body.clone())
            .await,
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
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].sequence, 1);
    assert_eq!(items[1].sequence, 2);
    assert_eq!(items[0].message_id, request.message_id.to_string());
    assert_eq!(
        items[1].message_id,
        OperationId::from_bytes([4; 16]).to_string()
    );
    assert_eq!(items[0].sender, parent);
    assert_eq!(items[1].sender, parent);
    assert!(items[0].delivered_at_epoch_ms > 0);
    assert!(items[1].delivered_at_epoch_ms > 0);
    let second_child_items = DurableCommunication::new(reopened.clone())
        .inbox(second_child, 0, 8)
        .await?;
    assert_eq!(second_child_items.len(), 1);
    assert_eq!(second_child_items[0].sender, parent);
    assert!(second_child_items[0].delivered_at_epoch_ms > 0);
    assert_eq!(
        second_child_items[0].message_id,
        request.message_id.to_string()
    );
    let read = ContentGrant::verify_read(
        &reopened_fixture.issuer.verifier(),
        &reopened_fixture.scope,
        &items[0].payload,
    )?;
    assert_eq!(
        reopened_fixture
            .fs
            .read_content(
                &items[0].payload,
                &read,
                reopened_fixture.runtime_scope.limits().file_bytes,
            )
            .await?
            .as_ref(),
        b"{}"
    );
    let alternate_read = ContentGrant::verify_read(
        &reopened_fixture.issuer.verifier(),
        &reopened_fixture.scope,
        &items[1].payload,
    )?;
    assert_eq!(
        reopened_fixture
            .fs
            .read_content(
                &items[1].payload,
                &alternate_read,
                reopened_fixture.runtime_scope.limits().file_bytes,
            )
            .await?
            .as_ref(),
        b"{\"alternate\":true}"
    );
    let parent_items = DurableCommunication::new(reopened.clone())
        .inbox(parent, 0, 8)
        .await?;
    assert_eq!(parent_items.len(), 2);
    assert_eq!(parent_items[0].message_id, request.message_id.to_string());
    assert_eq!(parent_items[1].message_id, request.message_id.to_string());
    assert!(
        DurableCommunication::new(reopened)
            .inbox(sibling, 0, 8)
            .await?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn model_selected_mail_and_wait_use_the_coordinator_host_after_reopen() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|e| Error::Storage(e.to_string()))?;
    let fixture = Fixture::open(directory.path()).await?;
    let host = fixture.host().await?;
    let body = payload(&fixture, OperationId::from_bytes([63; 16])).await?;
    let child = TaskId::from_bytes([62; 16]);
    let sibling = TaskId::from_bytes([64; 16]);
    let sibling_history = marker_payload(
        &fixture,
        "markers/sibling-history-secret.txt",
        b"sibling-history-secret",
        "sibling-history-marker",
    )
    .await?;
    let workspace_private_content = marker_payload(
        &fixture,
        "workspace/private-content.txt",
        b"mailbox-private-secret",
        "workspace-private-content-marker",
    )
    .await?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| Error::Invalid(e.to_string()))?
        .as_millis() as u64;
    let deadline_epoch_ms = now + 1_000;
    let captures = Arc::new(std::sync::Mutex::new(Vec::new()));
    let model = Arc::new(CoordinatorCommunicationModel {
        payload: body.clone(),
        recipient: child,
        target: "child",
        deadline_epoch_ms,
        completion_metadata: json!({"ui_state": "ui-only-secret"}),
        captured: captures.clone(),
        calls: AtomicUsize::new(0),
    });
    let waits = Arc::new(StreamWaitStore::new(fixture.stream.clone()));
    let swarm_root = directory.path().join("model-swarm");
    let swarm = PersistentLocalSwarm::open_with_model_and_bindings(
        &swarm_root,
        Model::new("mock", "communication", "1", json!({}))?,
        model.clone(),
        Limits::default(),
        LocalSwarmBindings::communication(host.clone(), Some(waits), None),
    )
    .await?;
    let root_task = swarm.root_task().await?;
    assert!(matches!(
        host.admit(fixture.admission(OperationId::from_bytes(root_task.into_bytes()), None)?).await?,
        Admission::Accepted(id) if id == root_task
    ));
    assert!(matches!(
        host.admit(fixture.admission(OperationId::from_bytes(child.into_bytes()), Some(root_task))?).await?,
        Admission::Accepted(id) if id == child
    ));
    assert!(matches!(
        host.admit(fixture.admission(OperationId::from_bytes(sibling.into_bytes()), Some(root_task))?).await?,
        Admission::Accepted(id) if id == sibling
    ));
    host.send(
        root_task,
        sibling,
        OperationId::from_bytes([65; 16]),
        sibling_history.clone(),
    )
    .await?;
    // Keep a second sibling mailbox item unread. The first item represents
    // explicit sibling history that the coordinator is allowed to deliver;
    // this one remains private mailbox state outside the model input.
    host.send(
        root_task,
        sibling,
        OperationId::from_bytes([66; 16]),
        workspace_private_content.clone(),
    )
    .await?;
    let sibling_mailbox = fixture
        .stream
        .stream(format!("harness/v2/mail/{sibling}"))
        .map_err(|error| Error::Storage(error.to_string()))?;
    assert_eq!(sibling_mailbox.bounds().await?.tail, 2);
    drop(sibling_mailbox);
    let private_read = ContentGrant::verify_read(
        &fixture.issuer.verifier(),
        &fixture.scope,
        &workspace_private_content,
    )?;
    assert_eq!(
        fixture
            .fs
            .read_content(
                &workspace_private_content,
                &private_read,
                fixture.runtime_scope.limits().file_bytes,
            )
            .await?
            .as_ref(),
        b"mailbox-private-secret"
    );

    // Seed provider completion metadata through the same durable local
    // session before the communication-enabled swarm performs its operation.
    // This keeps the marker genuinely persisted while leaving the swarm task
    // in Ready state for the subsequent model operation under test.
    drop(swarm);
    drop(model);
    let metadata_session = PersistentLocalHarness::open(
        swarm_root.join("tasks").join(root_task.to_string()),
        Model::new("mock", "communication", "1", json!({}))?,
        Arc::new(CompletionMetadataModel),
        Limits::default(),
    )
    .await?;
    let metadata_operation = OperationId::from_bytes([59; 16]);
    let metadata_output = metadata_session
        .run(metadata_operation, "persist completion metadata")
        .await?;
    assert_eq!(
        metadata_output.metadata,
        json!({"ui_state": "ui-only-secret"})
    );
    drop(metadata_session);
    // Reopen the metadata-only session and read the persisted extension
    // through its owner-authenticated file reference. This makes the
    // exclusion assertion below causal: the marker is durable private
    // completion state, rather than a value that only existed in the test
    // provider's in-memory return value.
    let metadata_reopened = PersistentLocalHarness::open(
        swarm_root.join("tasks").join(root_task.to_string()),
        Model::new("mock", "communication", "1", json!({}))?,
        Arc::new(CompletionMetadataModel),
        Limits::default(),
    )
    .await?;
    let metadata_state = metadata_reopened
        .conversation_state(Limits::default())
        .await?;
    let metadata_reference = metadata_state
        .messages
        .iter()
        .find_map(|message| message.extensions.get("acyclic.model.metadata"))
        .ok_or_else(|| Error::Storage("persisted completion metadata is missing".into()))?;
    let (_, persisted_metadata) = metadata_reopened
        .read_private_path(metadata_reference.path(), None)
        .await?;
    assert_eq!(
        serde_json::from_slice::<Value>(&persisted_metadata)
            .map_err(|error| Error::Invalid(error.to_string()))?,
        json!({"ui_state": "ui-only-secret"})
    );
    drop(metadata_reopened);
    // Rebase the scripted deadline after the metadata-only recovery work so
    // the live wait always publishes its timer record before it expires.
    let deadline_epoch_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| Error::Invalid(e.to_string()))?
        .as_millis() as u64
        + 1_000;
    let model = Arc::new(CoordinatorCommunicationModel {
        payload: body.clone(),
        recipient: child,
        target: "child",
        deadline_epoch_ms,
        completion_metadata: json!({"ui_state": "ui-only-secret"}),
        captured: captures.clone(),
        calls: AtomicUsize::new(0),
    });
    let waits = Arc::new(StreamWaitStore::new(fixture.stream.clone()));
    let swarm = PersistentLocalSwarm::open_with_model_and_bindings(
        &swarm_root,
        Model::new("mock", "communication", "1", json!({}))?,
        model.clone(),
        Limits::default(),
        LocalSwarmBindings::communication(host.clone(), Some(waits), None),
    )
    .await?;
    let operation = OperationId::from_bytes([61; 16]);
    let first_output = swarm
        .run_root(operation, "send the staged message and wait")
        .await?;
    assert_eq!(first_output.metadata, json!({"ui_state": "ui-only-secret"}));
    let first_inbox = host.inbox(child, 0, 8).await?;
    assert_eq!(first_inbox.len(), 1);
    assert_eq!(first_inbox[0].payload, body);
    let messages = swarm.read_messages(root_task, 0, 64).await?;
    let tool_calls = messages
        .iter()
        .filter(|message| message.kind == MessageKind::ToolCall)
        .map(|message| message.tool_call_id.clone())
        .collect::<Vec<_>>();
    let tool_results = messages
        .iter()
        .filter(|message| message.kind == MessageKind::ToolResult)
        .map(|message| message.tool_call_id.clone())
        .collect::<Vec<_>>();
    assert_eq!(tool_calls, tool_results);
    assert_eq!(tool_calls.len(), 2);
    let mut decoded_calls = Vec::new();
    let mut decoded_results = Vec::new();
    let mut persisted_call_bytes = Vec::new();
    let mut persisted_result_bytes = Vec::new();
    for message in messages.iter().filter(|message| {
        matches!(
            message.kind,
            MessageKind::ToolCall | MessageKind::ToolResult
        )
    }) {
        let (_, bytes) = swarm
            .read_file(root_task, message.content.path(), None)
            .await?;
        if message.kind == MessageKind::ToolCall {
            persisted_call_bytes.push(bytes.clone());
            decoded_calls.push(
                serde_json::from_slice::<ToolInvocation>(&bytes)
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            );
        } else {
            persisted_result_bytes.push(bytes.clone());
            decoded_results.push(
                serde_json::from_slice::<ToolResult>(&bytes)
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            );
        }
    }
    assert_eq!(decoded_calls.len(), 2);
    assert_eq!(decoded_results.len(), 2);
    assert_eq!(decoded_calls[0].name, "swarm.message");
    let message_input: MessageToolInput =
        serde_json::from_value(decoded_calls[0].arguments.clone())
            .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(message_input.recipient, child.to_string());
    assert_eq!(
        message_input.target,
        acyclic_harness::communication_tools::MessageToolTarget::Child
    );
    let message_output: MessageToolOutput =
        serde_json::from_value(decoded_results[0].value.clone())
            .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(message_output.delivered);
    assert_eq!(message_output.recipient, child.to_string());
    assert_eq!(decoded_calls[1].name, "swarm.wait");
    let wait_input: WaitToolInput = serde_json::from_value(decoded_calls[1].arguments.clone())
        .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(matches!(
        wait_input,
        WaitToolInput::Deadline {
            deadline_epoch_ms: deadline,
            ..
        } if deadline == deadline_epoch_ms
    ));
    let wait_output: WaitToolOutput = serde_json::from_value(decoded_results[1].value.clone())
        .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(matches!(wait_output, WaitToolOutput::Deadline));
    let first_timer_tail = fixture
        .stream
        .stream(format!("harness/v2/timers/{root_task}"))
        .map_err(|error| Error::Storage(error.to_string()))?
        .bounds()
        .await
        .map_err(|error| Error::Storage(error.to_string()))?
        .tail;
    let first_wait_tail = fixture
        .stream
        .stream(format!("harness/v2/waits/{root_task}"))
        .map_err(|error| Error::Storage(error.to_string()))?
        .bounds()
        .await
        .map_err(|error| Error::Storage(error.to_string()))?
        .tail;
    assert!(first_wait_tail >= 2);
    let timer_records = fixture
        .stream
        .stream(format!("harness/v2/timers/{root_task}"))
        .map_err(|error| Error::Storage(error.to_string()))?
        .read(0, first_timer_tail as u32)
        .await
        .map_err(|error| Error::Storage(error.to_string()))?
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    assert_eq!(timer_records.len(), 1);
    let timer: Value = serde_json::from_slice(&timer_records[0].value)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(timer["task_id"], root_task.to_string());
    assert_eq!(timer["deadline_unix_ms"], deadline_epoch_ms);
    let wait_records = fixture
        .stream
        .stream(format!("harness/v2/waits/{root_task}"))
        .map_err(|error| Error::Storage(error.to_string()))?
        .read(0, first_wait_tail as u32)
        .await
        .map_err(|error| Error::Storage(error.to_string()))?
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let persisted_deadline: Value = serde_json::from_slice(
        &wait_records
            .last()
            .ok_or_else(|| Error::Storage("wait completion record is missing".into()))?
            .value,
    )
    .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(persisted_deadline["completion"]["kind"], "deadline");
    assert_eq!(
        persisted_deadline["request"]["target"]["deadline_epoch_ms"],
        deadline_epoch_ms
    );
    let completed_at = persisted_deadline["completed_at_epoch_ms"]
        .as_u64()
        .ok_or_else(|| Error::Storage("wait completion timestamp is missing".into()))?;
    assert!(completed_at >= deadline_epoch_ms);
    assert!(completed_at.saturating_sub(now) >= deadline_epoch_ms.saturating_sub(now));
    drop(swarm);
    drop(model);
    drop(host);
    drop(fixture);

    let reopened_fixture = Fixture::open(directory.path()).await?;
    let reopened_host = reopened_fixture.host().await?;
    let reopened_waits = Arc::new(StreamWaitStore::new(reopened_fixture.stream.clone()));
    let reopened_model = Arc::new(CoordinatorCommunicationModel {
        payload: body.clone(),
        recipient: child,
        target: "child",
        deadline_epoch_ms,
        completion_metadata: json!({"ui_state": "ui-only-secret"}),
        captured: captures.clone(),
        calls: AtomicUsize::new(0),
    });
    let reopened = PersistentLocalSwarm::open_with_model_and_bindings(
        &swarm_root,
        Model::new("mock", "communication", "1", json!({}))?,
        reopened_model,
        Limits::default(),
        LocalSwarmBindings::communication(reopened_host.clone(), Some(reopened_waits), None),
    )
    .await?;
    let captured_before_replay = captures.lock().expect("model input capture lock").len();
    assert_eq!(
        reopened
            .run_root(operation, "send the staged message and wait")
            .await?,
        first_output
    );
    assert_eq!(
        captures.lock().expect("model input capture lock").len(),
        captured_before_replay,
        "cold replay of a completed operation must not dispatch the provider"
    );
    assert_eq!(reopened_host.inbox(child, 0, 8).await?.len(), 1);
    assert_eq!(reopened_host.inbox(sibling, 0, 8).await?.len(), 2);
    let replayed_messages = reopened.read_messages(root_task, 0, 64).await?;
    let mut replayed_call_bytes = Vec::new();
    let mut replayed_result_bytes = Vec::new();
    for message in replayed_messages
        .iter()
        .filter(|message| {
            matches!(
                message.kind,
                MessageKind::ToolCall | MessageKind::ToolResult
            )
        })
        .take(persisted_call_bytes.len() + persisted_result_bytes.len())
    {
        let (_, bytes) = reopened
            .read_file(root_task, message.content.path(), None)
            .await?;
        if message.kind == MessageKind::ToolCall {
            replayed_call_bytes.push(bytes);
        } else {
            replayed_result_bytes.push(bytes);
        }
    }
    assert_eq!(replayed_call_bytes, persisted_call_bytes);
    assert_eq!(replayed_result_bytes, persisted_result_bytes);
    let replayed_results = replayed_result_bytes
        .iter()
        .map(|bytes| {
            serde_json::from_slice::<ToolResult>(bytes)
                .map_err(|error| Error::Invalid(error.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(replayed_results, decoded_results);
    assert_eq!(
        reopened_fixture
            .stream
            .stream(format!("harness/v2/timers/{root_task}"))
            .map_err(|error| Error::Storage(error.to_string()))?
            .bounds()
            .await
            .map_err(|error| Error::Storage(error.to_string()))?
            .tail,
        first_timer_tail
    );
    assert_eq!(
        reopened_fixture
            .stream
            .stream(format!("harness/v2/waits/{root_task}"))
            .map_err(|error| Error::Storage(error.to_string()))?
            .bounds()
            .await
            .map_err(|error| Error::Storage(error.to_string()))?
            .tail,
        first_wait_tail
    );
    let captured = captures.lock().expect("model input capture lock");
    assert_eq!(captured.len(), 1);
    let (provider_bytes, manifest) = &captured[0];
    assert_eq!(
        *blake3::hash(provider_bytes).as_bytes(),
        manifest.request_digest
    );
    let provider_value: Value = serde_json::from_slice(provider_bytes)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(provider_bytes, &canonical_json_bytes(&provider_value)?);
    for marker in [
        "sibling-history-secret",
        "ui-only-secret",
        "mailbox-private-secret",
    ] {
        assert!(
            !provider_bytes
                .windows(marker.len())
                .any(|window| window == marker.as_bytes())
        );
    }
    Ok(())
}

#[tokio::test]
async fn model_selected_forged_sibling_and_ancestor_targets_are_rejected() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|e| Error::Storage(e.to_string()))?;
    let cases = [
        (
            "sibling",
            TaskId::from_bytes([72; 16]),
            "child",
            Some(TaskId::from_bytes([71; 16])),
        ),
        ("ancestor", TaskId::from_bytes([74; 16]), "parent", None),
    ];
    for (name, recipient, target, recipient_parent) in cases {
        let case_root = directory.path().join(name);
        tokio::fs::create_dir_all(&case_root)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let fixture = Fixture::open(&case_root).await?;
        let host = fixture.host().await?;
        let body = payload(&fixture, OperationId::from_bytes([75; 16])).await?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| Error::Invalid(e.to_string()))?
            .as_millis() as u64;
        let model = Arc::new(CoordinatorCommunicationModel {
            payload: body,
            recipient,
            target,
            deadline_epoch_ms: now + 100,
            completion_metadata: Value::Null,
            captured: Arc::new(std::sync::Mutex::new(Vec::new())),
            calls: AtomicUsize::new(0),
        });
        let swarm = PersistentLocalSwarm::open_with_model_and_bindings(
            case_root.join("model-swarm"),
            Model::new("mock", "communication", "1", json!({}))?,
            model,
            Limits::default(),
            LocalSwarmBindings::communication(host.clone(), None, None),
        )
        .await?;
        let root_task = swarm.root_task().await?;
        assert!(matches!(
            host.admit(fixture.admission(OperationId::from_bytes(root_task.into_bytes()), None)?).await?,
            Admission::Accepted(id) if id == root_task
        ));
        if let Some(parent) = recipient_parent {
            assert!(matches!(
                host.admit(fixture.admission(OperationId::from_bytes(parent.into_bytes()), None)?).await?,
                Admission::Accepted(id) if id == parent
            ));
            assert!(matches!(
                host.admit(fixture.admission(OperationId::from_bytes(recipient.into_bytes()), Some(parent))?).await?,
                Admission::Accepted(id) if id == recipient
            ));
        } else {
            assert!(matches!(
                host.admit(fixture.admission(OperationId::from_bytes(recipient.into_bytes()), None)?).await?,
                Admission::Accepted(id) if id == recipient
            ));
        }
        let forged = swarm
            .run_root(OperationId::from_bytes([76; 16]), "send the forged message")
            .await;
        assert!(matches!(
            forged,
            Err(Error::Unauthorized(_)) | Err(Error::Invalid(_))
        ));
        assert!(host.inbox(recipient, 0, 8).await?.is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn local_wait_timeout_and_cancellation_are_typed() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|e| Error::Storage(e.to_string()))?;
    let fixture = Fixture::open(directory.path()).await?;
    let host = fixture.host().await?;
    let wait_store = Arc::new(StreamWaitStore::new(fixture.stream.clone()));
    let ancestor = TaskId::from_bytes([10; 16]);
    let parent = TaskId::from_bytes([11; 16]);
    let child = TaskId::from_bytes([12; 16]);
    let sibling = TaskId::from_bytes([13; 16]);
    assert!(
        matches!(host.admit(fixture.admission(OperationId::from_bytes(parent.into_bytes()), None)?).await?, Admission::Accepted(id) if id == parent)
    );
    assert!(
        matches!(host.admit(fixture.admission(OperationId::from_bytes(ancestor.into_bytes()), None)?).await?, Admission::Accepted(id) if id == ancestor)
    );
    assert!(
        matches!(host.admit(fixture.admission(OperationId::from_bytes(child.into_bytes()), Some(parent))?).await?, Admission::Accepted(id) if id == child)
    );
    assert!(
        matches!(host.admit(fixture.admission(OperationId::from_bytes(sibling.into_bytes()), Some(parent))?).await?, Admission::Accepted(id) if id == sibling)
    );
    let invalid_target = DurableCommunication::new(host.clone())
        .wait(
            WaitRequest {
                operation_id: OperationId::from_bytes([16; 16]),
                waiter: child,
                target: WaitTarget::Tasks {
                    task_ids: vec![sibling],
                },
                timeout_epoch_ms: None,
                cancellation_id: None,
            },
            None,
        )
        .await;
    assert!(matches!(invalid_target, Err(Error::Unauthorized(_))));
    let invalid_ancestor = DurableCommunication::new(host.clone())
        .wait(
            WaitRequest {
                operation_id: OperationId::from_bytes([17; 16]),
                waiter: child,
                target: WaitTarget::Tasks {
                    task_ids: vec![ancestor],
                },
                timeout_epoch_ms: None,
                cancellation_id: None,
            },
            None,
        )
        .await;
    assert!(matches!(invalid_ancestor, Err(Error::Unauthorized(_))));
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
    let pre_cancel_request = WaitRequest {
        operation_id: OperationId::from_bytes([14; 16]),
        waiter: parent,
        target: WaitTarget::Messages {
            task_id: parent,
            after: 0,
            limit: 8,
        },
        timeout_epoch_ms: None,
        cancellation_id: Some(OperationId::from_bytes([15; 16])),
    };
    assert_eq!(wait_store.open(pre_cancel_request.clone()).await?, None);
    assert_eq!(
        DurableCommunication::new(host.clone())
            .with_wait_store(wait_store.clone())
            .cancel(pre_cancel_request.clone())
            .await?,
        WaitCompletion::Cancelled
    );
    assert_eq!(
        DurableCommunication::new(host.clone())
            .with_wait_store(wait_store.clone())
            .wait(pre_cancel_request.clone(), None)
            .await?,
        WaitCompletion::Cancelled
    );
    let unknown_cancel_request = WaitRequest {
        operation_id: OperationId::from_bytes([18; 16]),
        waiter: parent,
        target: WaitTarget::Messages {
            task_id: parent,
            after: 0,
            limit: 8,
        },
        timeout_epoch_ms: None,
        cancellation_id: Some(OperationId::from_bytes([19; 16])),
    };
    assert!(matches!(
        DurableCommunication::new(host.clone())
            .with_wait_store(wait_store.clone())
            .cancel(unknown_cancel_request)
            .await,
        Err(Error::Conflict(message)) if message.contains("no retained admission")
    ));
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
        cancellation_id: Some(OperationId::from_bytes([16; 16])),
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
    assert_eq!(
        reopened_store.open(pre_cancel_request).await?,
        Some(WaitCompletion::Cancelled)
    );
    Ok(())
}

#[tokio::test]
async fn local_stream_durable_wait_rejects_a_forged_early_timeout_after_restart() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|e| Error::Storage(e.to_string()))?;
    let fixture = Fixture::open(directory.path()).await?;
    let host = fixture.host().await?;
    let waiter = TaskId::from_bytes([31; 16]);
    assert!(matches!(
        host.admit(fixture.admission(OperationId::from_bytes(waiter.into_bytes()), None)?).await?,
        Admission::Accepted(id) if id == waiter
    ));
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| Error::Invalid(e.to_string()))?
        .as_millis() as u64;
    let request = WaitRequest {
        operation_id: OperationId::from_bytes([32; 16]),
        waiter,
        target: WaitTarget::Messages {
            task_id: waiter,
            after: 0,
            limit: 1,
        },
        timeout_epoch_ms: Some(now.saturating_sub(1_000)),
        cancellation_id: None,
    };
    let stream = fixture
        .stream
        .stream(format!("harness/v2/waits/{waiter}"))
        .map_err(|error| Error::Storage(error.to_string()))?;
    let admission = serde_json::json!({
        "kind": "admission",
        "contract": "harness.wait-event.v2",
        "request": request.clone(),
    });
    stream
        .append_batch(
            vec![Bytes::from(canonical_json_bytes(&admission)?)],
            None,
            None,
        )
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let completion = serde_json::json!({
        "kind": "completion",
        "contract": "harness.wait-event.v2",
        "request": request.clone(),
        "completion": {"kind": "timed_out"},
        "completed_at_epoch_ms": 1,
    });
    stream
        .append_batch(
            vec![Bytes::from(canonical_json_bytes(&completion)?)],
            Some(1),
            None,
        )
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    drop(stream);
    // Reopen both the coordinator and the wait store before attempting the
    // replay.  This keeps the forged record test on the cold recovery path;
    // it cannot pass through a process-local completion cache.
    drop(host);
    drop(fixture);
    let reopened_fixture = Fixture::open(directory.path()).await?;
    let reopened_host = reopened_fixture.host().await?;
    let waits = Arc::new(StreamWaitStore::new(reopened_fixture.stream.clone()));
    assert!(matches!(
        DurableCommunication::new(reopened_host)
            .with_wait_store(waits)
            .wait(request, None)
            .await,
        Err(Error::Conflict(message)) if message.contains("before")
    ));
    Ok(())
}
