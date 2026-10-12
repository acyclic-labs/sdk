//! G controls use real loopback HTTP, the production protocol boundary and disk-backed execution/interaction owners.
use super::{journal, request, LocalJournal};
use acyclic_harness::{Capabilities, Error, InteractionId, OperationId, Outcome, Result,
    context::{ContextPipeline, ModelContextCapacity, ModelTokenCount},
    executor::{ExecutionJournal, Executor, StockExecutor, TurnInput},
    interaction::{Interaction, InteractionOutcome, InteractionResponse},
    mcp::{McpToolTransport, callbacks::{McpCallbackAdmission, McpCallbackPolicy, McpCallbackRequest, McpRuntimeCallbacks}, http::{HttpMcpTransport, NativeMcpHttpProvider}, stdio::{McpStdioMethod, McpStdioRequest}},
    model::{Model, ModelAttempt, ModelDispatch, ModelEvent, ModelProvider, PreparedModelRequest},
    runtime::{AgentHarness, InteractionRouter, RuntimeScope, TaskContext, TaskDefinition, TaskRegistry, ToolContext},
    tool::ToolRegistry,
    conversation::Limits,
    core::AuthorityIssuer,
};
use acyclic_stream::{BoxProviderFuture, BoxProviderStream};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering}};
use tokio::io::AsyncWriteExt as _;

struct ByteModel { dispatches: AtomicUsize, pause: bool }
impl ModelProvider for ByteModel {
    fn context_capacity(&self, _: &Model) -> Result<ModelContextCapacity> {
        Ok(ModelContextCapacity { context_tokens: 65_536, output_tokens: 1024 })
    }
    fn count_tokens(&self, request: &PreparedModelRequest) -> Result<ModelTokenCount> {
        Ok(ModelTokenCount { request_digest: request.manifest().request_digest,
            fixed_tokens: u32::try_from(request.bytes().len()).map_err(|_| Error::Invalid("byte-model request overflow".into()))?,
            message_tokens: vec![0; request.request().messages.len()] })
    }
    fn generate<'a>(&'a self, request: PreparedModelRequest, _: ModelDispatch) -> BoxProviderStream<'a, Result<ModelEvent>> {
        self.dispatches.fetch_add(1, Ordering::SeqCst);
        assert!(request.request().tools.is_empty(), "unadvertised sampling tools leaked from the supplied executor");
        if self.pause { return Box::pin(futures::stream::pending()); }
        let last = request.request().messages.last().map(|message| &message.content);
        let text = format!("recorded:{last:?}");
        Box::pin(futures::stream::iter([Ok(ModelEvent::Content { delta: text }), Ok(ModelEvent::Completed { metadata: json!({"finish":"stop"}) })]))
    }
    fn reconcile<'a>(&'a self, _: ModelAttempt) -> BoxProviderFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async { Ok(None) })
    }
}

struct Participant { pub(super) journal: Arc<LocalJournal>, pub(super) issuer: AuthorityIssuer }
impl InteractionRouter for Participant {
    fn route<'a>(&'a self, operation: OperationId, interaction: Interaction) -> BoxProviderFuture<'a, Result<InteractionOutcome>> {
        Box::pin(async move {
            let id = InteractionId::from_bytes(operation.into_bytes());
            self.journal.open_interaction(id, interaction.clone()).await?;
            if let Some(outcome) = self.journal.interaction_outcome(id).await? { return Ok(outcome); }
            let response = match interaction {
                Interaction::Form { .. } => InteractionResponse::Form { value: json!({"answer":"participant"}) },
                Interaction::Approval { .. } => InteractionResponse::Approval { approved: true, reason: None },
                _ => return Err(Error::Unsupported("participant control is form/approval only".into())),
            };
            let scope = self.issuer.root_for_agent(acyclic_harness::AgentId::from_bytes([41;16]), "participant", Capabilities::new(["interaction:resolve".into(), format!("interaction:respond:{id}")]));
            self.journal.resolve_interaction(id, response, &scope).await?;
            self.journal.interaction_outcome(id).await?.ok_or_else(|| Error::NotFound("recorded participant decision".into()))
        })
    }
}
pub(super) async fn admitted_context(scope: RuntimeScope, router: Arc<dyn InteractionRouter>, content: Option<acyclic_harness::runtime::ContentBindings>) -> Result<TaskContext> {
    let result = Arc::new(Mutex::new(None));
    let sink = Arc::clone(&result);
    let mut tasks = TaskRegistry::default();
    tasks.register(TaskDefinition::live("mcp-context", "1", move |context, _: ()| {
        let sink = Arc::clone(&sink);
        async move {
            *sink.lock().map_err(|error| Error::Storage(error.to_string()))? = Some(context);
            Ok(())
        }
    })?)?;
    let runtime = AgentHarness::with_content(tasks, ToolRegistry::new(), scope, 1, None, Some(router), content)?;
    let definition = runtime.task::<(), ()>("mcp-context@1")?;
    assert!(matches!(runtime.spawn(&definition, ()).await?.result().await?, Outcome::Succeeded(())));
    let context = result.lock().map_err(|error| Error::Storage(error.to_string()))?.take().ok_or_else(|| Error::NotFound("admitted context".into()))?;
    Ok(context)
}
fn sampling_policy() -> acyclic_harness::mcp::callbacks::McpSamplingPolicy {
    acyclic_harness::mcp::callbacks::McpSamplingPolicy {
        continuation: acyclic_harness::model::ModelContent::Text("Host-selected continuation request".into()),
        image: None, audio: None,
    }
}
fn policy() -> McpCallbackPolicy {
    McpCallbackPolicy { server: "fixture".into(), revision: "g1".into(), sampling: true, elicitation: true, maximum_requests: 4 }
}
fn sampling(parent: OperationId) -> McpCallbackRequest {
    McpCallbackRequest { parent, policy: policy(), message: json!({"jsonrpc":"2.0","id":"sample","method":"sampling/createMessage","params":{"messages":[{"role":"user","content":{"type":"text","text":"callback"}}],"maxTokens":128}}) }
}
fn executor(model: Arc<ByteModel>, scope: RuntimeScope) -> Result<StockExecutor> {
    StockExecutor::new(Model::new("byte-model", "deterministic", "1", json!({}))?, model, ContextPipeline::default(), ToolRegistry::new()).with_tool_authority(scope, None)
}

#[tokio::test]
async fn endpoint_sampling_lost_ack_reopens_without_model_redispatch_and_pins_request() -> Result<()> {
    for lose_ack in [false, true] {
        let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let (disk, issuer, _) = journal(root.path(), true).await?;
        let scope = RuntimeScope::new(Capabilities::new(["model:generate", "context:build", "interaction:route"]), Limits::default())?;
        let context = admitted_context(scope.clone(), Arc::new(Participant { journal: Arc::clone(&disk), issuer }), None).await?;
        let operation = OperationId::from_bytes([81;16]);
        let tool = ToolContext::new(context, operation, "remote")?;
        let model = Arc::new(ByteModel { dispatches: AtomicUsize::new(0), pause: false });
        let callback: Arc<dyn McpCallbackAdmission> = Arc::new(McpRuntimeCallbacks::new(executor(Arc::clone(&model), scope.clone())?, disk.clone(), "deterministic".into(), sampling_policy())?);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.map_err(|error| Error::Storage(error.to_string()))?;
        let address = listener.local_addr().map_err(|error| Error::Storage(error.to_string()))?;
        let callback_message = sampling(operation).message;
        let server = tokio::spawn(async move {
            let (mut call, _) = listener.accept().await.map_err(|error| Error::Storage(error.to_string()))?;
            let input = request(&mut call).await?;
            assert_eq!(input.get("method"), Some(&json!("tools/call")));
            let final_result = json!({"jsonrpc":"2.0","id":operation.to_string(),"result":{"content":[{"type":"text","text":"complete"}]}});
            let body = format!("data: {callback_message}\n\ndata: {final_result}\n\n");
            let reply = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            call.write_all(reply.as_bytes()).await.map_err(|error| Error::Storage(error.to_string()))?;
            let (mut acknowledgement, _) = listener.accept().await.map_err(|error| Error::Storage(error.to_string()))?;
            let result = request(&mut acknowledgement).await?;
            assert_eq!(result.get("id"), Some(&json!("sample")));
            assert_eq!(result.pointer("/result/role"), Some(&json!("assistant")));
            assert!(result.pointer("/result/content/text").and_then(Value::as_str).is_some_and(|text| text.contains("callback")));
            if !lose_ack { acknowledgement.write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.map_err(|error| Error::Storage(error.to_string()))?; }
            drop(acknowledgement);
            drop(call);
            assert!(tokio::time::timeout(std::time::Duration::from_millis(100), listener.accept()).await.is_err());
            Ok::<_, Error>(())
        });
        let transport: Arc<dyn McpToolTransport> = Arc::new(HttpMcpTransport::new(Arc::new(NativeMcpHttpProvider::new(reqwest::header::HeaderMap::new())?), format!("http://{address}/mcp"), None, 8192, 2000)?.with_callbacks(policy(), Arc::clone(&callback))?);
        let mut installed = super::catalog();
        let mut registry = ToolRegistry::new();
        let projection: Arc<dyn acyclic_harness::tool::ToolProjection> = Arc::new(super::Projection);
        assert!(matches!(installed.install(&mut registry, None, &transport, &projection, 8, 8192), Err(Error::Conflict(_))));
        assert!(registry.definitions()?.is_empty());
        installed.revision = "g1".into();
        installed.install(&mut registry, None, &transport, &projection, 8, 8192)?;
        let observed = transport.call_with_context(tool.clone(), "echo", json!({})).await;
        if lose_ack { assert!(matches!(observed, Err(Error::Indeterminate(_)))); }
        else { assert_eq!(observed?.content.first().and_then(|content| content.get("text")), Some(&json!("complete"))); }
        assert!(transport.reconcile(operation).await?.is_none());
        server.await.map_err(|error| Error::Storage(error.to_string()))??;
        assert_eq!(model.dispatches.load(Ordering::SeqCst), 1);
        drop(transport); drop(callback); drop(disk); drop(tool);
        let (disk, issuer, _) = journal(root.path(), false).await?;
        let context = admitted_context(scope.clone(), Arc::new(Participant { journal: Arc::clone(&disk), issuer }), None).await?;
        let tool = ToolContext::new(context, operation, "remote")?;
        let reopened = McpRuntimeCallbacks::new(executor(Arc::clone(&model), scope)?, disk, "deterministic".into(), sampling_policy())?;
        let result = reopened.admit(tool.clone(), &sampling(operation)).await?;
        assert_eq!(result.pointer("/result/role"), Some(&json!("assistant")));
        assert_eq!(model.dispatches.load(Ordering::SeqCst), 1);
        let mut changed = sampling(operation);
        changed.message.get_mut("params").and_then(Value::as_object_mut).ok_or_else(|| Error::Invalid("params absent".into()))?.insert("maxTokens".into(), json!(127));
        assert!(matches!(reopened.admit(tool, &changed).await, Err(Error::Conflict(_))));
        assert_eq!(model.dispatches.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[tokio::test]
async fn form_callback_replays_owner_answer_and_rejects_changed_schema() -> Result<()> {
    let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let (disk, issuer, _) = journal(root.path(), true).await?;
    let scope = RuntimeScope::new(Capabilities::new(["model:generate", "context:build", "interaction:route"]), Limits::default())?;
    let context = admitted_context(scope.clone(), Arc::new(Participant { journal: Arc::clone(&disk), issuer }), None).await?;
    let operation = OperationId::from_bytes([82;16]);
    let tool = ToolContext::new(context, operation, "form")?;
    let model = Arc::new(ByteModel { dispatches: AtomicUsize::new(0), pause: false });
    let callbacks = McpRuntimeCallbacks::new(executor(Arc::clone(&model), scope)?, disk, "deterministic".into(), sampling_policy())?;
    let mut form = McpCallbackRequest { parent: operation, policy: policy(), message: json!({"jsonrpc":"2.0","id":7,"method":"elicitation/create","params":{"mode":"form","message":"Question","requestedSchema":{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"]}}}) };
    let first = callbacks.admit(tool.clone(), &form).await?;
    assert_eq!(first.pointer("/result/content/answer"), Some(&json!("participant")));
    assert_eq!(callbacks.admit(tool.clone(), &form).await?, first);
    form.message.get_mut("params").and_then(Value::as_object_mut).ok_or_else(|| Error::Invalid("params absent".into()))?.insert("message".into(), json!("changed"));
    assert!(matches!(callbacks.admit(tool, &form).await, Err(Error::Conflict(_))));
    assert_eq!(model.dispatches.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn real_resource_prompt_endpoint_and_schema_negative_controls() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.map_err(|error| Error::Storage(error.to_string()))?;
    let address = listener.local_addr().map_err(|error| Error::Storage(error.to_string()))?;
    let server = tokio::spawn(async move {
        for expected in ["resources/list", "resources/templates/list", "resources/read", "prompts/list", "prompts/get"] {
            let (mut socket, _) = listener.accept().await.map_err(|error| Error::Storage(error.to_string()))?;
            let input = request(&mut socket).await?;
            assert_eq!(input.get("method"), Some(&json!(expected)));
            let result = match expected {
                "resources/list" => json!({"resources":[{"name":"resource","uri":"test://resource"}],"nextCursor":"next"}),
                "resources/templates/list" => json!({"resourceTemplates":[{"name":"template","uriTemplate":"test://{name}"}]}),
                "resources/read" => { assert_eq!(input.pointer("/params/uri"), Some(&json!("test://resource"))); json!({"contents":[{"uri":"test://resource","text":"content"}]}) },
                "prompts/list" => json!({"prompts":[{"name":"prompt"}]}),
                _ => { assert_eq!(input.pointer("/params/arguments/topic"), Some(&json!("chosen"))); json!({"messages":[{"role":"user","content":{"type":"text","text":"external"}}]}) },
            };
            let body = json!({"jsonrpc":"2.0","id":input.get("id"),"result":result}).to_string();
            let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            socket.write_all(response.as_bytes()).await.map_err(|error| Error::Storage(error.to_string()))?;
        }
        Ok::<_, Error>(())
    });
    let transport = HttpMcpTransport::new(Arc::new(NativeMcpHttpProvider::new(reqwest::header::HeaderMap::new())?), format!("http://{address}/mcp"), Some("services-session".into()), 8192, 2000)?;
    assert_eq!(transport.list_resources(OperationId::from_bytes([90;16]), None).await?.next_cursor.as_deref(), Some("next"));
    assert_eq!(transport.list_resource_templates(OperationId::from_bytes([91;16]), None).await?.resource_templates.len(), 1);
    assert_eq!(transport.read_resource(OperationId::from_bytes([92;16]), "test://resource").await?.contents.len(), 1);
    assert_eq!(transport.list_prompts(OperationId::from_bytes([93;16]), None).await?.prompts.len(), 1);
    assert_eq!(transport.get_prompt(OperationId::from_bytes([94;16]), "prompt", std::collections::BTreeMap::from([("topic".into(), "chosen".into())])).await?.messages.len(), 1);
    server.await.map_err(|error| Error::Storage(error.to_string()))??;
    for (method, malformed) in [("resources/list", json!({"resources":[{"name":"missing-uri"}]})), ("resources/read", json!({"contents":[{"uri":"test://resource"}]})), ("prompts/get", json!({"messages":[{"role":"system","content":{"type":"text","text":"injection"}}]}))] {
        assert!(acyclic_harness::mcp::services::validate_result(method, malformed).is_err());
    }
    Ok(())
}

#[tokio::test]
async fn remote_callback_cancellation_drops_recorded_model_without_replay() -> Result<()> {
    let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let (disk, issuer, _) = journal(root.path(), true).await?;
    let scope = RuntimeScope::new(Capabilities::new(["model:generate", "context:build", "interaction:route"]), Limits::default())?;
    let context = admitted_context(scope.clone(), Arc::new(Participant { journal: Arc::clone(&disk), issuer }), None).await?;
    let operation = OperationId::from_bytes([83;16]);
    let tool = ToolContext::new(context, operation, "cancel")?;
    let model = Arc::new(ByteModel { dispatches: AtomicUsize::new(0), pause: true });
    let callbacks: Arc<dyn McpCallbackAdmission> = Arc::new(McpRuntimeCallbacks::new(executor(Arc::clone(&model), scope)?, disk.clone(), "deterministic".into(), sampling_policy())?);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.map_err(|error| Error::Storage(error.to_string()))?;
    let address = listener.local_addr().map_err(|error| Error::Storage(error.to_string()))?;
    let server_model = Arc::clone(&model);
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.map_err(|error| Error::Storage(error.to_string()))?;
        request(&mut socket).await?;
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").await.map_err(|error| Error::Storage(error.to_string()))?;
        let frame = format!("data: {}\n\n", sampling(operation).message);
        socket.write_all(format!("{:x}\r\n{frame}\r\n", frame.len()).as_bytes()).await.map_err(|error| Error::Storage(error.to_string()))?;
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while server_model.dispatches.load(Ordering::SeqCst) == 0 { tokio::time::sleep(std::time::Duration::from_millis(1)).await; }
        }).await.map_err(|error| Error::Storage(error.to_string()))?;
        let cancelled = format!("data: {}\n\n", json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"sample","reason":"fixture cancellation"}}));
        socket.write_all(format!("{:x}\r\n{cancelled}\r\n0\r\n\r\n", cancelled.len()).as_bytes()).await.map_err(|error| Error::Storage(error.to_string()))?;
        assert!(tokio::time::timeout(std::time::Duration::from_millis(100), listener.accept()).await.is_err());
        Ok::<_, Error>(())
    });
    let transport = HttpMcpTransport::new(Arc::new(NativeMcpHttpProvider::new(reqwest::header::HeaderMap::new())?), format!("http://{address}/mcp"), None, 8192, 2000)?.with_callbacks(policy(), Arc::clone(&callbacks))?;
    assert!(matches!(transport.call_with_context(tool.clone(), "echo", json!({})).await, Err(Error::Indeterminate(id)) if id == operation));
    let callback = sampling(operation);
    let records = disk.replay(callback.operation()?, 0, 64).await?;
    assert!(records.iter().any(|record| matches!(record.event, acyclic_harness::executor::ExecutionEvent::ModelStarted { .. })));
    assert!(matches!(callbacks.admit(tool, &callback).await, Err(Error::Indeterminate(_))));
    assert_eq!(model.dispatches.load(Ordering::SeqCst), 1);
    server.await.map_err(|error| Error::Storage(error.to_string()))??;
    Ok(())
}

#[tokio::test]
async fn sampling_media_uses_scoped_filesystem_publication_and_native_policy() -> Result<()> {
    use acyclic_harness::{conversation::{VolumeRef, VolumeClass, VolumeOwner, VolumeOperation}, resources::ProviderRef,
        filesystem::{FilesystemHost, FilesystemContentPublisher, FilesystemContentVerifier}, runtime::ContentBindings,
        model::{ImageDetail, NativeMediaIntent, NativeMediaPolicy}};
    for granted in [false, true] {
        let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let (disk, issuer, signed) = journal(root.path(), true).await?;
        let provider = ProviderRef::new("mcp-fault", "filesystem", "2")?;
        let host = Arc::new(FilesystemHost::new(acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(root.path().join("fs"))).await.map_err(|error| Error::Storage(error.to_string()))?, provider.clone())?);
        let volume = VolumeRef::new(provider, "scratch", VolumeClass::AgentPrivate, VolumeOwner::Agent(acyclic_harness::AgentId::from_bytes([41;16])))?;
        let mut grants = vec!["model:generate".into(), "context:build".into(), "interaction:route".into()];
        if granted { grants.extend([volume.capability(VolumeOperation::Read)?, volume.capability(VolumeOperation::Write)?]); }
        let scope = RuntimeScope::new(Capabilities::new(grants), Limits::default())?;
        let content = ContentBindings {
            reader: Arc::new(FilesystemContentVerifier::new(Arc::clone(&host), issuer.verifier(), signed.clone(), 8192)?),
            writer: Some(Arc::new(FilesystemContentPublisher::new(host, volume, &issuer.verifier(), &signed, 8192)?)),
        };
        let context = admitted_context(scope.clone(), Arc::new(Participant { journal: Arc::clone(&disk), issuer }), Some(content)).await?;
        let operation = OperationId::from_bytes([86;16]);
        let tool = ToolContext::new(context.clone(), operation, "media")?;
        let model = Arc::new(ByteModel { dispatches: AtomicUsize::new(0), pause: false });
        let mut selection = sampling_policy();
        selection.image = Some(NativeMediaPolicy { intent: NativeMediaIntent::Image { detail: ImageDetail::Low }, maximum_bytes: 8, maximum_work: 16, configuration: None });
        selection.audio = Some(NativeMediaPolicy { intent: NativeMediaIntent::Audio { maximum_duration_ms: 1000 }, maximum_bytes: 8, maximum_work: 16, configuration: None });
        let callbacks = McpRuntimeCallbacks::new(executor(Arc::clone(&model), scope)?, disk.clone(), "deterministic".into(), selection)?;
        let request = McpCallbackRequest { parent: operation, policy: policy(), message: json!({"jsonrpc":"2.0","id":"media","method":"sampling/createMessage","params":{"messages":[{"role":"user","content":[{"type":"image","data":"bWVkaWE=","mimeType":"image/png"},{"type":"audio","data":"YXVkaW8=","mimeType":"audio/wav"}]}],"maxTokens":128}}) };
        let result = callbacks.admit(tool.clone(), &request).await;
        assert_eq!(result.is_ok(), granted);
        assert_eq!(model.dispatches.load(Ordering::SeqCst), usize::from(granted));
        if granted {
            let records = disk.replay(request.operation()?, 0, 64).await?;
            let file = records.iter().find_map(|record| match &record.event {
                acyclic_harness::executor::ExecutionEvent::ModelStarted { request, .. } => Some(request), _ => None,
            }).ok_or_else(|| Error::NotFound("media model request".into()))?;
            let prepared: acyclic_harness::model::ModelRequest = serde_json::from_slice(&disk.load(file).await?).map_err(|error| Error::Invalid(error.to_string()))?;
            let files = prepared.messages.last().ok_or_else(|| Error::NotFound("media input".into()))?.content.file_refs().collect::<Vec<_>>();
            assert_eq!(files.len(), 2);
            let mut bytes = Vec::new();
            for file in files { bytes.push(context.read_file(file).await?); }
            assert_eq!(bytes.len(), 2);
            assert_eq!(bytes[0].as_ref(), b"media");
            assert_eq!(bytes[1].as_ref(), b"audio");
            callbacks.admit(tool, &request).await?;
            assert_eq!(model.dispatches.load(Ordering::SeqCst), 1);
        }
    }
    Ok(())
}

