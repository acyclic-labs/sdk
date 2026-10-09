//! Actual MCP HTTP and disk-backed execution journals across provider reopen.
#![cfg(all(feature = "filesystem-local", feature = "mcp-http"))]

use acyclic_fs::{Fs, LocalOptions};
use acyclic_harness::{
    AgentId, Capabilities, Error, IdempotencyKey, InteractionId, OperationId, Outcome, Result,
    TaskId,
    conversation::{FileRef, Limits, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{Action, AggregateKind, Authority, AuthorityIssuer, Command, SchemaRegistry, Scope},
    durable_tool::DurableToolRunner,
    executor::{ExecutionEvent, ExecutionJournal, ExecutionRecord},
    filesystem::{FilesystemExecutionJournal, FilesystemHost},
    interaction::{Interaction, InteractionOutcome, InteractionResponse},
    mcp::{
        McpCatalog, McpCatalogInstallation, McpDiscoveryPolicy, McpSchemaExposure,
        McpToolDefinition, McpToolResult, McpToolTransport,
        http::{HttpMcpTransport, NativeMcpHttpProvider},
    },
    resources::ProviderRef,
    runtime::{Bindings, RuntimeScope, TaskAdmissionRecord, TaskStateProvider, ToolContext},
    store::StreamAggregate,
    tool::{ToolInvocation, ToolProjection, ToolRegistry, ToolResult},
};
use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient};
use futures::future::BoxFuture;
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    None,
    ClaimBefore,
    ClaimAfter,
    ResponseLost,
    ResultStage,
    TerminalBefore,
    TerminalAfter,
}

struct FaultJournal {
    inner: Arc<dyn ExecutionJournal>,
    fault: Fault,
    fired: AtomicBool,
}
impl ExecutionJournal for FaultJournal {
    fn replay<'a>(
        &'a self,
        op: OperationId,
        after: u64,
        maximum: u32,
    ) -> BoxFuture<'a, Result<Vec<ExecutionRecord>>> {
        self.inner.replay(op, after, maximum)
    }
    fn append<'a>(
        &'a self,
        op: OperationId,
        key: String,
        event: ExecutionEvent,
    ) -> BoxFuture<'a, Result<()>> {
        self.inner.append(op, key, event)
    }
    fn append_if_tail<'a>(
        &'a self,
        op: OperationId,
        tail: u64,
        key: String,
        event: ExecutionEvent,
    ) -> BoxFuture<'a, Result<bool>> {
        Box::pin(async move {
            let boundary = match event {
                ExecutionEvent::ToolStarted { .. } => {
                    matches!(self.fault, Fault::ClaimBefore | Fault::ClaimAfter)
                }
                ExecutionEvent::ToolCompleted { .. } => {
                    matches!(self.fault, Fault::TerminalBefore | Fault::TerminalAfter)
                }
                _ => false,
            };
            let inject = boundary && !self.fired.swap(true, Ordering::SeqCst);
            if inject && matches!(self.fault, Fault::ClaimBefore | Fault::TerminalBefore) {
                return Err(Error::Indeterminate(op));
            }
            let committed = self.inner.append_if_tail(op, tail, key, event).await?;
            if inject {
                return Err(Error::Indeterminate(op));
            }
            Ok(committed)
        })
    }
    fn stage<'a>(
        &'a self,
        op: OperationId,
        key: String,
        bytes: Vec<u8>,
        media: &'static str,
    ) -> BoxFuture<'a, Result<FileRef>> {
        Box::pin(async move {
            if self.fault == Fault::ResultStage
                && key.starts_with("tool:result:")
                && !self.fired.swap(true, Ordering::SeqCst)
            {
                return Err(Error::Storage(
                    "injected after remote result before local stage".into(),
                ));
            }
            self.inner.stage(op, key, bytes, media).await
        })
    }
    fn load<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>> {
        self.inner.load(file)
    }
    fn open_interaction<'a>(
        &'a self,
        id: InteractionId,
        interaction: Interaction,
    ) -> BoxFuture<'a, Result<()>> {
        self.inner.open_interaction(id, interaction)
    }
    fn interaction_outcome<'a>(
        &'a self,
        id: InteractionId,
    ) -> BoxFuture<'a, Result<Option<InteractionOutcome>>> {
        self.inner.interaction_outcome(id)
    }
}

// The fixture supplies an already admitted task scope. The production runner,
// journal authority, filesystem and HTTP adapter remain real implementations.
struct AdmittedTask(RuntimeScope);
impl TaskStateProvider for AdmittedTask {
    fn policy_identity(&self) -> Option<acyclic_harness::registry::ComponentIdentity> {
        None
    }
    fn observe_admission<'a>(&'a self, _: TaskId) -> BoxFuture<'a, Result<TaskAdmissionRecord>> {
        Box::pin(async { Err(Error::Unsupported("fixture scope already admitted".into())) })
    }
    fn resume_scope<'a>(
        &'a self,
        _: TaskId,
        _: OperationId,
    ) -> BoxFuture<'a, Result<RuntimeScope>> {
        Box::pin(async { Ok(self.0.clone()) })
    }
    fn outcome<'a>(&'a self, _: TaskId) -> BoxFuture<'a, Result<Option<Outcome<Value>>>> {
        Box::pin(async { Ok(None) })
    }
    fn cancel<'a>(&'a self, _: TaskId) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}
struct Projection;
impl ToolProjection for Projection {
    fn project(&self, _: &ToolInvocation, result: &ToolResult) -> Result<Value> {
        Ok(json!({"kind":"json","value":result.value}))
    }
}

// Require the actual durable runner's context at both transport boundaries.
// The underlying HTTP adapter still owns all network I/O and reconciliation.
struct ScopedHttp(HttpMcpTransport);

fn verify_transport_context(context: &ToolContext) -> Result<()> {
    let operation = OperationId::from_bytes([43; 16]);
    if context.task().durable_task_id() != Some(TaskId::from_bytes([42; 16]))
        || context.task().id() != OperationId::from_bytes([44; 16])
        || context.operation_id() != operation
        || context.call_id() != operation.to_string()
        || !context.task().scope().grants().contains("mcp:call:fixture")
    {
        return Err(Error::Unauthorized("durable MCP context changed".into()));
    }
    Ok(())
}

impl McpToolTransport for ScopedHttp {
    fn call<'a>(
        &'a self,
        _: OperationId,
        _: &'a str,
        _: Value,
    ) -> BoxFuture<'a, Result<McpToolResult>> {
        Box::pin(async { Err(Error::Unsupported("durable task context required".into())) })
    }
    fn reconcile<'a>(&'a self, _: OperationId) -> BoxFuture<'a, Result<Option<McpToolResult>>> {
        Box::pin(async { Err(Error::Unsupported("durable task context required".into())) })
    }
    fn call_with_context<'a>(
        &'a self,
        context: ToolContext,
        name: &'a str,
        arguments: Value,
    ) -> BoxFuture<'a, Result<McpToolResult>> {
        Box::pin(async move {
            verify_transport_context(&context)?;
            self.0.call(context.operation_id(), name, arguments).await
        })
    }
    fn reconcile_with_context<'a>(
        &'a self,
        context: ToolContext,
    ) -> BoxFuture<'a, Result<Option<McpToolResult>>> {
        Box::pin(async move {
            verify_transport_context(&context)?;
            self.0.reconcile(context.operation_id()).await
        })
    }
}

fn catalog() -> McpCatalog {
    McpCatalog {
        server: "fixture".into(),
        revision: "1".into(),
        schema_exposure: McpSchemaExposure::Eager,
        projection_schema: acyclic_harness::tool::json_projection_schema(json!({})),
        discovery: McpDiscoveryPolicy::Search,
        tools: vec![McpToolDefinition {
            name: "echo".into(),
            title: None,
            description: "fixture".into(),
            input_schema: json!({"type":"object"}),
            output_schema: None,
        }],
    }
}

type LocalJournal = FilesystemExecutionJournal<
    LocalStream,
    acyclic_fs::LocalAuthorityBackend,
    acyclic_fs::LocalObjectBackend,
>;

async fn bind_conversation(
    stream: &StreamClient<LocalStream>,
    issuer: &AuthorityIssuer,
    agent: AgentId,
) -> Result<()> {
    let mut aggregate = StreamAggregate::open(
        stream,
        Authority {
            kind: AggregateKind::Conversation,
            id: "mcp-fault".into(),
        },
        issuer.verifier(),
        SchemaRegistry::new(),
    )
    .await?;
    aggregate
        .execute(Command {
            operation_id: OperationId::from_bytes([45; 16]),
            idempotency_key: IdempotencyKey::new("bind:mcp-fault")?,
            expected_revision: 0,
            scope: issuer.root("owner", Capabilities::new(["conversation:bind"])),
            causal_parent: None,
            action: Action::BindConversation { agent },
        })
        .await?;
    Ok(())
}

async fn journal(root: &Path, create: bool) -> Result<(Arc<LocalJournal>, AuthorityIssuer, Scope)> {
    let provider = ProviderRef::new("mcp-fault", "filesystem", "2")?;
    let host = Arc::new(FilesystemHost::new(
        Fs::local(LocalOptions::new(root.join("fs")))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
        provider.clone(),
    )?);
    let agent = AgentId::from_bytes([41; 16]);
    let private = VolumeRef::new(
        provider,
        "scratch",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
    )?;
    if create {
        host.create_volume(&private).await?;
    }
    let issuer = AuthorityIssuer::new(
        "mcp-fault",
        [41; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "mcp-fault".into(),
        },
    );
    let signed = issuer.root_for_agent(
        agent,
        "agent",
        Capabilities::new([
            private.capability(VolumeOperation::Read)?,
            private.capability(VolumeOperation::Write)?,
            "mcp:install:fixture".into(),
            "interaction:open".into(),
            "interaction:resolve".into(),
            format!(
                "interaction:respond:{}",
                InteractionId::from_bytes([47; 16])
            ),
        ]),
    );
    let stream = StreamClient::new(Arc::new(
        LocalStream::open(root.join("stream"), LocalStreamLimits::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
    ));
    if create {
        bind_conversation(&stream, &issuer, agent).await?;
    }
    let journal = Arc::new(FilesystemExecutionJournal::new(
        stream,
        host,
        private,
        issuer.verifier(),
        signed.clone(),
        8192,
    )?);
    if create {
        let operation = OperationId::from_bytes([46; 16]);
        let interaction = InteractionId::from_bytes([47; 16]);
        journal
            .open_interaction(
                interaction,
                Interaction::approval(
                    "Install this exact MCP catalog",
                    operation,
                    catalog().installation_digest(None, 1, 8192)?,
                )?,
            )
            .await?;
        journal
            .resolve_interaction(
                interaction,
                InteractionResponse::Approval {
                    approved: true,
                    reason: None,
                },
                &signed,
            )
            .await?;
    }
    Ok((journal, issuer, signed))
}

async fn request(socket: &mut tokio::net::TcpStream) -> Result<Value> {
    let mut input = Vec::new();
    let mut buffer = [0_u8; 512];
    loop {
        let count = socket
            .read(&mut buffer)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        if count == 0 {
            return Err(Error::Invalid("fixture disconnected".into()));
        }
        input.extend(buffer.iter().take(count));
        if input.len() > 8192 {
            return Err(Error::Invalid("fixture request overflow".into()));
        }
        if let Some(end) = input.windows(4).position(|part| part == b"\r\n\r\n") {
            let header = std::str::from_utf8(
                input
                    .get(..end)
                    .ok_or_else(|| Error::Invalid("fixture header".into()))?,
            )
            .map_err(|error| Error::Invalid(error.to_string()))?;
            let length = header
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .and_then(|value| value.trim().parse::<usize>().ok())
                })
                .ok_or_else(|| Error::Invalid("fixture length".into()))?;
            if input.len() >= end + 4 + length {
                return serde_json::from_slice(
                    input
                        .get(end + 4..end + 4 + length)
                        .ok_or_else(|| Error::Invalid("fixture body".into()))?,
                )
                .map_err(|error| Error::Invalid(error.to_string()));
            }
        }
    }
}

async fn serve(
    listener: tokio::net::TcpListener,
    fault: Fault,
    applied: Arc<AtomicUsize>,
    stop: tokio::sync::oneshot::Receiver<()>,
) -> Result<()> {
    tokio::pin!(stop);
    loop {
        let (mut socket, _) = tokio::select! {
            _ = &mut stop => return Ok(()),
            accepted = listener.accept() => accepted.map_err(|error| Error::Storage(error.to_string()))?,
        };
        let message = tokio::time::timeout(Duration::from_secs(2), request(&mut socket))
            .await
            .map_err(|error| Error::Storage(error.to_string()))??;
        assert_eq!(message.get("method"), Some(&json!("tools/call")));
        applied.fetch_add(1, Ordering::SeqCst);
        if fault == Fault::ResponseLost {
            continue;
        }
        let body = json!({"jsonrpc":"2.0","id":message.get("id"),"result":{"content":[{"type":"text","text":"applied"}]}}).to_string();
        let reply = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        socket
            .write_all(reply.as_bytes())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        socket
            .shutdown()
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
    }
}

async fn phase(
    root: &Path,
    endpoint: &str,
    fault: Fault,
    reopened: bool,
) -> Result<Outcome<Value>> {
    let (approval, issuer, signed) = journal(root, !reopened).await?;
    let journal: Arc<dyn ExecutionJournal> = if reopened {
        approval.clone()
    } else {
        Arc::new(FaultJournal {
            inner: approval.clone(),
            fault,
            fired: AtomicBool::new(false),
        })
    };
    let transport: Arc<dyn McpToolTransport> = Arc::new(ScopedHttp(HttpMcpTransport::new(
        Arc::new(NativeMcpHttpProvider::new(
            reqwest::header::HeaderMap::new(),
        )?),
        endpoint.into(),
        Some("session".into()),
        8192,
        2000,
    )?));
    let projection: Arc<dyn ToolProjection> = Arc::new(Projection);
    let mut tools = ToolRegistry::new();
    catalog()
        .install_scoped(
            &mut tools,
            None,
            &transport,
            &projection,
            McpCatalogInstallation {
                operation: OperationId::from_bytes([46; 16]),
                interaction: InteractionId::from_bytes([47; 16]),
                scope: &signed,
                verifier: &issuer.verifier(),
                approval: approval.as_ref(),
                maximum_tools: 1,
                maximum_bytes: 8192,
            },
        )
        .await?;
    let definition = tools
        .get("mcp.fixture.echo")
        .ok_or_else(|| Error::NotFound("fixture tool".into()))?
        .definition
        .clone();
    let scope = RuntimeScope::new(
        Capabilities::new(["tool:call:mcp.fixture.echo", "mcp:call:fixture"]),
        Limits::default(),
    )?;
    let mut bindings = Bindings::local();
    bindings.scope = scope.clone();
    bindings.state = Some(Arc::new(AdmittedTask(scope)));
    let runtime = bindings.build()?;
    let task = TaskId::from_bytes([42; 16]);
    let op = OperationId::from_bytes([43; 16]);
    let context = runtime
        .durable_context(task, OperationId::from_bytes([44; 16]))
        .await?;
    let runner = DurableToolRunner::new(tools, journal);
    runner
        .run_with_context(
            task,
            op,
            definition,
            json!({}),
            ToolContext::new(context, op, op.to_string())?,
        )
        .await
}

#[tokio::test]
async fn disk_reopen_never_reposts_a_claimed_mcp_call() -> Result<()> {
    for fault in [
        Fault::None,
        Fault::ClaimBefore,
        Fault::ClaimAfter,
        Fault::ResponseLost,
        Fault::ResultStage,
        Fault::TerminalBefore,
        Fault::TerminalAfter,
    ] {
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let endpoint = format!(
            "http://{}/mcp",
            listener
                .local_addr()
                .map_err(|error| Error::Storage(error.to_string()))?
        );
        let applied = Arc::new(AtomicUsize::new(0));
        let (sender, stop) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(serve(listener, fault, applied.clone(), stop));
        let first = Box::pin(phase(directory.path(), &endpoint, fault, false)).await?;
        assert_eq!(
            matches!(first, Outcome::Succeeded(_)),
            fault == Fault::None,
            "{fault:?}"
        );
        // Each phase returns and drops every client/provider/journal/runtime
        // handle. The second phase reconstructs them from disk and the same
        // exact catalog/session binding, with no fault wrapper or cached result.
        let second = Box::pin(phase(directory.path(), &endpoint, fault, true)).await?;
        let succeeds = matches!(
            fault,
            Fault::None | Fault::ClaimBefore | Fault::TerminalAfter
        );
        assert_eq!(
            matches!(second, Outcome::Succeeded(_)),
            succeeds,
            "{fault:?}"
        );
        if !succeeds {
            assert!(matches!(second, Outcome::Indeterminate { .. }), "{fault:?}");
        }
        assert_eq!(
            applied.load(Ordering::SeqCst),
            usize::from(fault != Fault::ClaimAfter),
            "{fault:?}"
        );
        let _ = sender.send(());
        server
            .await
            .map_err(|error| Error::Storage(error.to_string()))??;
    }
    Ok(())
}
