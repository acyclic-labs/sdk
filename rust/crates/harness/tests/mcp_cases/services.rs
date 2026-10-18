//! G controls use real loopback HTTP, the production protocol boundary and disk-backed execution/interaction owners.
use super::{LocalJournal, hold_io_clock, journal, no_more_requests, request, workspace};
use acyclic_harness::{
    Capabilities, Error, InteractionId, OperationId, Outcome, Result,
    context::{ContextPipeline, ModelContextCapacity, ModelTokenCount},
    conversation::Limits,
    core::AuthorityIssuer,
    executor::{ExecutionJournal, StockExecutor},
    interaction::{Interaction, InteractionOutcome, InteractionResponse},
    mcp::{
        McpToolTransport,
        callbacks::{McpCallbackPolicy, McpCallbackRequest, McpRuntimeCallbacks},
        http::{HttpMcpTransport, NativeMcpHttpProvider},
    },
    model::{Model, ModelContent, ModelContentPart, ModelEvent},
    runtime::{
        InteractionRouter, RuntimeScope, TaskContext, TaskDefinition, TaskRegistry, ToolContext,
    },
    test_support::{ScriptedEvent, ScriptedModelProvider, ScriptedReconciliation, ScriptedTurn},
    tool::ToolRegistry,
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt as _;

fn sampling_model(turns: impl IntoIterator<Item = ScriptedTurn>) -> ScriptedModelProvider {
    ScriptedModelProvider::new(turns)
        .with_context_capacity(ModelContextCapacity {
            context_tokens: 65_536,
            output_tokens: Some(1024),
        })
        .with_token_counter(|request| {
            Ok(ModelTokenCount::from_upper_bounds(
                request.request_digest(),
                u32::try_from(request.bytes().len())
                    .map_err(|_| Error::Invalid("byte-model request overflow".into()))?,
                vec![0; request.request().messages.len()],
            ))
        })
}

fn sampling_turn() -> ScriptedTurn {
    ScriptedTurn::Events(vec![
        ScriptedEvent::Event(ModelEvent::Content {
            delta: "recorded:callback".into(),
        }),
        ScriptedEvent::Event(ModelEvent::Completed {
            metadata: json!({"finish":"stop"}),
        }),
    ])
}

fn assert_sampling_text(model: &ScriptedModelProvider) -> Result<()> {
    let requests = model.requests();
    assert_eq!(requests.len(), 1);
    let request = requests
        .first()
        .ok_or_else(|| Error::NotFound("sampling request".into()))?;
    assert!(
        request.tools.is_empty(),
        "unadvertised sampling tools leaked from the supplied executor"
    );
    assert_eq!(
        request.messages.last().map(|message| &message.content),
        Some(&ModelContent::Parts(vec![ModelContentPart::Text {
            text: "callback".into()
        }]))
    );
    assert_eq!(model.remaining(), 0);
    Ok(())
}

pub(super) struct Participant {
    pub(super) journal: std::sync::Weak<LocalJournal>,
    pub(super) issuer: AuthorityIssuer,
}
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl InteractionRouter for Participant {
    async fn route(
        &self,
        operation: OperationId,
        interaction: Interaction,
    ) -> Result<InteractionOutcome> {
        let journal = self
            .journal
            .upgrade()
            .ok_or_else(|| Error::NotFound("interaction journal is closed".into()))?;
        let id = InteractionId::from_bytes(operation.into_bytes());
        journal.open_interaction(id, &interaction, None)?.await?;
        if let Some(outcome) = journal.interaction_outcome(id, None)?.await? {
            return Ok(outcome);
        }
        let response = match interaction {
            Interaction::Form { .. } => InteractionResponse::Form {
                value: json!({"answer":"participant"}),
            },
            Interaction::Approval { .. } => InteractionResponse::Approval {
                approved: true,
                reason: None,
            },
            _ => {
                return Err(Error::Unsupported(
                    "participant control is form/approval only".into(),
                ));
            }
        };
        let scope = self
            .issuer
            .issue(
                "participant",
                &Capabilities::new([
                    "interaction:resolve".into(),
                    format!("interaction:respond:{id}"),
                ]),
                Some(acyclic_harness::AgentId::from_bytes([41; 16])),
                None,
                &mut None,
            )
            .expect("scope proof");
        journal.resolve_interaction(id, response, &scope).await?;
        journal
            .interaction_outcome(id, None)?
            .await?
            .ok_or_else(|| Error::NotFound("recorded participant decision".into()))
    }
}
pub(super) async fn admitted_context(
    scope: RuntimeScope,
    router: Arc<dyn InteractionRouter>,
    content: Option<acyclic_harness::runtime::ContentBindings>,
) -> Result<TaskContext> {
    let result = Arc::new(Mutex::new(None));
    let sink = Arc::clone(&result);
    let mut tasks = TaskRegistry::default();
    tasks.register(TaskDefinition::live(
        "mcp-context",
        "1",
        move |context, _: ()| {
            let sink = Arc::clone(&sink);
            async move {
                *sink
                    .lock()
                    .map_err(|error| Error::Storage(error.to_string()))? = Some(context);
                Ok(())
            }
        },
    )?)?;
    let runtime = acyclic_harness::runtime::Bindings {
        tasks,
        tools: ToolRegistry::default(),
        scope,
        concurrency: 1,
        durable_host: None,
        interactions: Some(router),
        content,
        ..acyclic_harness::runtime::Bindings::local()
    }
    .build(None)?;
    let definition = runtime.task::<(), ()>("mcp-context@1")?;
    assert!(matches!(
        runtime.spawn(&definition, ()).await?.result().await?,
        Outcome::Succeeded(())
    ));
    let context = result
        .lock()
        .map_err(|error| Error::Storage(error.to_string()))?
        .take()
        .ok_or_else(|| Error::NotFound("admitted context".into()))?;
    Ok(context)
}
fn policy() -> McpCallbackPolicy {
    McpCallbackPolicy {
        server: "fixture".into(),
        revision: "g1".into(),
        sampling: true,
        elicitation: true,
        maximum_requests: 4,
    }
}
fn sampling(parent: OperationId) -> McpCallbackRequest {
    McpCallbackRequest {
        parent,
        policy: policy(),
        message: json!({"jsonrpc":"2.0","id":"sample","method":"sampling/createMessage","params":{"messages":[{"role":"user","content":{"type":"text","text":"callback"}}],"maxTokens":128}}),
    }
}
fn executor(model: Arc<ScriptedModelProvider>, scope: RuntimeScope) -> Result<StockExecutor> {
    StockExecutor::new(
        Model::new(
            "byte-model".into(),
            "deterministic".into(),
            "1".into(),
            json!({}),
        )?,
        model,
        ContextPipeline::default(),
        ToolRegistry::default(),
    )
    .with_tool_authority(scope, None)
}

#[tokio::test(start_paused = true)]
async fn endpoint_sampling_lost_ack_reopens_without_model_redispatch_and_pins_request() -> Result<()>
{
    let _clock = hold_io_clock();
    for lose_ack in [false, true] {
        let root = workspace()?;
        let (disk, issuer, _) = journal(root.path(), true).await?;
        let scope = RuntimeScope::new(
            Capabilities::new(["model:generate", "context:build", "interaction:route"]),
            Limits::default(),
        )?;
        let context = admitted_context(
            scope.clone(),
            Arc::new(Participant {
                journal: Arc::downgrade(&disk),
                issuer,
            }),
            None,
        )
        .await?;
        let operation = OperationId::from_bytes([81; 16]);
        let context = Arc::new(context);
        let tool = ToolContext::new(Arc::clone(&context), operation, "remote", None)?;
        let model = Arc::new(sampling_model([sampling_turn()]));
        let callback: Arc<McpRuntimeCallbacks> = Arc::new(McpRuntimeCallbacks::new(
            executor(Arc::clone(&model), scope.clone())?,
            disk.clone(),
            "deterministic".into(),
            acyclic_harness::model::ModelContent::Text("Host-selected continuation request".into()),
            None,
            None,
        )?);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let address = listener
            .local_addr()
            .map_err(|error| Error::Storage(error.to_string()))?;
        let callback_message = sampling(operation).message;
        let (settled, completion) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut call, _) = listener
                .accept()
                .await
                .map_err(|error| Error::Storage(error.to_string()))?;
            let input = request(&mut call).await?;
            assert_eq!(input.get("method"), Some(&json!("tools/call")));
            let final_result = json!({"jsonrpc":"2.0","id":operation.to_string(),"result":{"content":[{"type":"text","text":"complete"}]}});
            let body = format!("data: {callback_message}\n\ndata: {final_result}\n\n");
            let reply = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            call.write_all(reply.as_bytes())
                .await
                .map_err(|error| Error::Storage(error.to_string()))?;
            let (mut acknowledgement, _) = listener
                .accept()
                .await
                .map_err(|error| Error::Storage(error.to_string()))?;
            let result = request(&mut acknowledgement).await?;
            assert_eq!(result.get("id"), Some(&json!("sample")));
            assert_eq!(result.pointer("/result/role"), Some(&json!("assistant")));
            assert!(
                result
                    .pointer("/result/content/text")
                    .and_then(Value::as_str)
                    .is_some_and(|text| text.contains("callback"))
            );
            if !lose_ack {
                acknowledgement
                    .write_all(
                        b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?;
            }
            drop(acknowledgement);
            drop(call);
            no_more_requests(listener, completion).await
        });
        let transport: Arc<dyn McpToolTransport> = Arc::new(HttpMcpTransport::new(
            Arc::new(NativeMcpHttpProvider::new(
                reqwest::header::HeaderMap::new(),
            )?),
            format!("http://{address}/mcp"),
            None,
            8192,
            2000,
            Some((policy(), Arc::clone(&callback))),
        )?);
        let mut installed = super::catalog();
        let mut registry = ToolRegistry::default();
        let projection: Arc<dyn acyclic_harness::tool::ToolProjection> =
            Arc::new(super::Projection);
        assert!(matches!(
            installed.install(&mut registry, None, &transport, &projection, 8, 8192),
            Err(Error::Conflict(_))
        ));
        assert!(registry.definitions(None)?.is_empty());
        installed.revision = "g1".into();
        installed.install(&mut registry, None, &transport, &projection, 8, 8192)?;
        let observed = transport
            .call(tool.operation_id(), "echo", json!({}), Some(tool.clone()))
            .await;
        if lose_ack {
            assert!(matches!(observed, Err(Error::Indeterminate(_))));
        } else {
            let observed = serde_json::to_value(observed?)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            assert_eq!(
                observed["content"]
                    .as_array()
                    .and_then(|content| content.first())
                    .and_then(|content| content.get("text")),
                Some(&json!("complete"))
            );
        }
        assert!(transport.reconcile(operation, None).await?.is_none());
        drop(registry);
        drop(transport);
        let _ = settled.send(());
        server
            .await
            .map_err(|error| Error::Storage(error.to_string()))??;
        assert_eq!(model.dispatches().len(), 1);
        assert_sampling_text(&model)?;
        drop(callback);
        drop(disk);
        drop(tool);
        let (disk, _, _) = journal(root.path(), false).await?;
        let tool = ToolContext::new(context, operation, "remote", None)?;
        let reopened = McpRuntimeCallbacks::new(
            executor(Arc::clone(&model), scope)?,
            disk,
            "deterministic".into(),
            acyclic_harness::model::ModelContent::Text("Host-selected continuation request".into()),
            None,
            None,
        )?;
        let (_, result) = reopened.admit(tool.clone(), &sampling(operation)).await?;
        assert_eq!(result.pointer("/result/role"), Some(&json!("assistant")));
        assert_eq!(model.dispatches().len(), 1);
        let mut changed = sampling(operation);
        changed
            .message
            .get_mut("params")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| Error::Invalid("params absent".into()))?
            .insert("maxTokens".into(), json!(127));
        assert!(matches!(
            reopened.admit(tool, &changed).await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(model.dispatches().len(), 1);
    }
    Ok(())
}

#[tokio::test]
async fn form_callback_replays_owner_answer_and_rejects_changed_schema() -> Result<()> {
    let root = workspace()?;
    let (disk, issuer, _) = journal(root.path(), true).await?;
    let scope = RuntimeScope::new(
        Capabilities::new(["model:generate", "context:build", "interaction:route"]),
        Limits::default(),
    )?;
    let context = admitted_context(
        scope.clone(),
        Arc::new(Participant {
            journal: Arc::downgrade(&disk),
            issuer,
        }),
        None,
    )
    .await?;
    let operation = OperationId::from_bytes([82; 16]);
    let tool = ToolContext::new(Arc::new(context), operation, "form", None)?;
    let model = Arc::new(sampling_model([]));
    let callbacks = McpRuntimeCallbacks::new(
        executor(Arc::clone(&model), scope)?,
        disk,
        "deterministic".into(),
        acyclic_harness::model::ModelContent::Text("Host-selected continuation request".into()),
        None,
        None,
    )?;
    let mut form = McpCallbackRequest {
        parent: operation,
        policy: policy(),
        message: json!({"jsonrpc":"2.0","id":7,"method":"elicitation/create","params":{"mode":"form","message":"Question","requestedSchema":{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"]}}}),
    };
    let (_, first) = callbacks.admit(tool.clone(), &form).await?;
    assert_eq!(
        first.pointer("/result/content/answer"),
        Some(&json!("participant"))
    );
    assert_eq!(callbacks.admit(tool.clone(), &form).await?.1, first);
    form.message
        .get_mut("params")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| Error::Invalid("params absent".into()))?
        .insert("message".into(), json!("changed"));
    assert!(matches!(
        callbacks.admit(tool, &form).await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(model.dispatches().len(), 0);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn real_resource_prompt_endpoint_and_schema_negative_controls() -> Result<()> {
    let _clock = hold_io_clock();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let address = listener
        .local_addr()
        .map_err(|error| Error::Storage(error.to_string()))?;
    let server = tokio::spawn(async move {
        for expected in [
            "resources/list",
            "resources/templates/list",
            "resources/read",
            "prompts/list",
            "prompts/get",
        ] {
            let (mut socket, _) = listener
                .accept()
                .await
                .map_err(|error| Error::Storage(error.to_string()))?;
            let input = request(&mut socket).await?;
            assert_eq!(input.get("method"), Some(&json!(expected)));
            let result = match expected {
                "resources/list" => {
                    json!({"resources":[{"name":"resource","uri":"test://resource"}],"nextCursor":"next"})
                }
                "resources/templates/list" => {
                    json!({"resourceTemplates":[{"name":"template","uriTemplate":"test://{name}"}]})
                }
                "resources/read" => {
                    assert_eq!(
                        input.pointer("/params/uri"),
                        Some(&json!("test://resource"))
                    );
                    json!({"contents":[{"uri":"test://resource","text":"content"}]})
                }
                "prompts/list" => json!({"prompts":[{"name":"prompt"}]}),
                _ => {
                    assert_eq!(
                        input.pointer("/params/arguments/topic"),
                        Some(&json!("chosen"))
                    );
                    json!({"messages":[{"role":"user","content":{"type":"text","text":"external"}}]})
                }
            };
            let body = json!({"jsonrpc":"2.0","id":input.get("id"),"result":result}).to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket
                .write_all(response.as_bytes())
                .await
                .map_err(|error| Error::Storage(error.to_string()))?;
        }
        Ok::<_, Error>(())
    });
    let transport = HttpMcpTransport::new(
        Arc::new(NativeMcpHttpProvider::new(
            reqwest::header::HeaderMap::new(),
        )?),
        format!("http://{address}/mcp"),
        Some("services-session".into()),
        8192,
        2000,
        None,
    )?;
    assert_eq!(
        transport
            .rpc(
                OperationId::from_bytes([90; 16]),
                "resources/list",
                json!({}),
                None
            )
            .await?["nextCursor"],
        json!("next")
    );
    assert_eq!(
        transport
            .rpc(
                OperationId::from_bytes([91; 16]),
                "resources/templates/list",
                json!({}),
                None
            )
            .await?["resourceTemplates"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        transport
            .rpc(
                OperationId::from_bytes([92; 16]),
                "resources/read",
                json!({"uri":"test://resource"}),
                None
            )
            .await?["contents"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        transport
            .rpc(
                OperationId::from_bytes([93; 16]),
                "prompts/list",
                json!({}),
                None
            )
            .await?["prompts"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        transport
            .rpc(
                OperationId::from_bytes([94; 16]),
                "prompts/get",
                json!({"name":"prompt","arguments":{"topic":"chosen"}}),
                None
            )
            .await?["messages"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    server
        .await
        .map_err(|error| Error::Storage(error.to_string()))??;
    for (method, malformed) in [
        (
            "resources/list",
            json!({"resources":[{"name":"missing-uri"}]}),
        ),
        (
            "resources/read",
            json!({"contents":[{"uri":"test://resource"}]}),
        ),
        (
            "prompts/get",
            json!({"messages":[{"role":"system","content":{"type":"text","text":"injection"}}]}),
        ),
    ] {
        assert!(acyclic_harness::mcp::services::validate_result(method, malformed, None).is_err());
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn remote_callback_cancellation_drops_recorded_model_without_replay() -> Result<()> {
    let _clock = hold_io_clock();
    let root = workspace()?;
    let (disk, issuer, _) = journal(root.path(), true).await?;
    let scope = RuntimeScope::new(
        Capabilities::new(["model:generate", "context:build", "interaction:route"]),
        Limits::default(),
    )?;
    let context = admitted_context(
        scope.clone(),
        Arc::new(Participant {
            journal: Arc::downgrade(&disk),
            issuer,
        }),
        None,
    )
    .await?;
    let operation = OperationId::from_bytes([83; 16]);
    let tool = ToolContext::new(Arc::new(context), operation, "cancel", None)?;
    let dispatched = Arc::new(tokio::sync::Notify::new());
    let signal = Arc::clone(&dispatched);
    let model = Arc::new(
        sampling_model([ScriptedTurn::Events(vec![ScriptedEvent::Pending {}])])
            .with_reconciliations([ScriptedReconciliation {
                orphan: false,
                observed: vec![],
                continuation: None,
            }])
            .with_dispatch_observer(move |_| signal.notify_one()),
    );
    let callbacks: Arc<McpRuntimeCallbacks> = Arc::new(McpRuntimeCallbacks::new(
        executor(Arc::clone(&model), scope)?,
        disk.clone(),
        "deterministic".into(),
        acyclic_harness::model::ModelContent::Text("Host-selected continuation request".into()),
        None,
        None,
    )?);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let address = listener
        .local_addr()
        .map_err(|error| Error::Storage(error.to_string()))?;
    let (settled, mut completion) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener
            .accept()
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        request(&mut socket).await?;
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").await.map_err(|error| Error::Storage(error.to_string()))?;
        let frame = format!("data: {}\n\n", sampling(operation).message);
        socket
            .write_all(format!("{:x}\r\n{frame}\r\n", frame.len()).as_bytes())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        tokio::select! {
            _ = dispatched.notified() => {},
            _ = &mut completion => return Err(Error::Invalid("transport completed before model dispatch".into())),
        }
        let cancelled = format!(
            "data: {}\n\n",
            json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"sample","reason":"fixture cancellation"}})
        );
        socket
            .write_all(format!("{:x}\r\n{cancelled}\r\n0\r\n\r\n", cancelled.len()).as_bytes())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        no_more_requests(listener, completion).await
    });
    let transport = HttpMcpTransport::new(
        Arc::new(NativeMcpHttpProvider::new(
            reqwest::header::HeaderMap::new(),
        )?),
        format!("http://{address}/mcp"),
        None,
        8192,
        2000,
        Some((policy(), Arc::clone(&callbacks))),
    )?;
    let observed = transport
        .call(tool.operation_id(), "echo", json!({}), Some(tool.clone()))
        .await;
    drop(transport);
    let _ = settled.send(());
    server
        .await
        .map_err(|error| Error::Storage(error.to_string()))??;
    assert!(matches!(observed, Err(Error::Indeterminate(id)) if id == operation));
    let callback = sampling(operation);
    let records = disk.replay(callback.validate()?.0, 0, 64, None)?.await?;
    assert!(records.iter().any(|record| matches!(
        record.event,
        acyclic_harness::executor::ExecutionEvent::ModelStarted { .. }
    )));
    assert!(matches!(
        callbacks.admit(tool, &callback).await,
        Err(Error::Indeterminate(_))
    ));
    assert_eq!(model.dispatches().len(), 1);
    assert_sampling_text(&model)?;
    assert_eq!(model.reconciliations().len(), 1);
    assert!(model.recoveries().is_empty());
    Ok(())
}

#[tokio::test]
async fn sampling_media_uses_scoped_filesystem_publication_and_native_policy() -> Result<()> {
    use acyclic_harness::{
        conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
        filesystem::{FilesystemContentPublisher, FilesystemContentVerifier, FilesystemHost},
        model::{ImageDetail, NativeMediaIntent, NativeMediaPolicy},
        resources::ProviderRef,
        runtime::ContentBindings,
    };
    for granted in [false, true] {
        let root = workspace()?;
        let (disk, issuer, signed) = journal(root.path(), true).await?;
        let provider = ProviderRef::new(
            "mcp-fault".into(),
            "filesystem".into(),
            "2".into(),
            &mut None,
        )?;
        let host = FilesystemHost::new(
            acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(root.path().join("fs")))
                .await
                .map_err(|error| Error::Storage(error.to_string()))?,
            &provider,
            None,
        )?;
        let volume = VolumeRef::new(
            provider,
            "scratch".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(acyclic_harness::AgentId::from_bytes([41; 16])),
            &mut None,
        )?;
        let mut grants = vec![
            "model:generate".into(),
            "context:build".into(),
            "interaction:route".into(),
        ];
        if granted {
            grants.extend([
                volume.capability(VolumeOperation::Read, &mut None)?,
                volume.capability(VolumeOperation::Write, &mut None)?,
            ]);
        }
        let scope = RuntimeScope::new(Capabilities::new(grants), Limits::default())?;
        let content = ContentBindings {
            reader: FilesystemContentVerifier::new(
                Arc::clone(&host),
                &issuer.verifier(&mut None).expect("verifier copy"),
                &signed,
                8192,
                None,
            )?,
            writer: Some(FilesystemContentPublisher::new(
                host,
                &volume,
                &issuer.verifier(&mut None).expect("verifier copy"),
                &signed,
                8192,
                None,
            )?),
        };
        let context = admitted_context(
            scope.clone(),
            Arc::new(Participant {
                journal: Arc::downgrade(&disk),
                issuer,
            }),
            Some(content),
        )
        .await?;
        let operation = OperationId::from_bytes([86; 16]);
        let tool = ToolContext::new(
            std::sync::Arc::new(context.clone()),
            operation,
            "media",
            None,
        )?;
        let model = Arc::new(sampling_model(granted.then(sampling_turn)));
        let callbacks = McpRuntimeCallbacks::new(
            executor(Arc::clone(&model), scope)?,
            disk.clone(),
            "deterministic".into(),
            acyclic_harness::model::ModelContent::Text("Host-selected continuation request".into()),
            Some(NativeMediaPolicy {
                intent: NativeMediaIntent::Image {
                    detail: ImageDetail::Low,
                },
                maximum_bytes: 8,
                maximum_work: 16,
                configuration: None,
            }),
            Some(NativeMediaPolicy {
                intent: NativeMediaIntent::Audio {
                    maximum_duration_ms: 1000,
                },
                maximum_bytes: 8,
                maximum_work: 16,
                configuration: None,
            }),
        )?;
        let request = McpCallbackRequest {
            parent: operation,
            policy: policy(),
            message: json!({"jsonrpc":"2.0","id":"media","method":"sampling/createMessage","params":{"messages":[{"role":"user","content":[{"type":"image","data":"bWVkaWE=","mimeType":"image/png"},{"type":"audio","data":"YXVkaW8=","mimeType":"audio/wav"}]}],"maxTokens":128}}),
        };
        let result = callbacks.admit(tool.clone(), &request).await;
        assert_eq!(result.is_ok(), granted);
        assert_eq!(model.dispatches().len(), usize::from(granted));
        if granted {
            let records = disk.replay(request.validate()?.0, 0, 64, None)?.await?;
            let file = records
                .iter()
                .find_map(|record| match &record.event {
                    acyclic_harness::executor::ExecutionEvent::ModelStarted { request, .. } => {
                        Some(request)
                    }
                    _ => None,
                })
                .ok_or_else(|| Error::NotFound("media model request".into()))?;
            let prepared: acyclic_harness::model::ModelRequest =
                serde_json::from_slice(disk.load(file, None)?.await?.as_ref())
                    .map_err(|error| Error::Invalid(error.to_string()))?;
            assert_eq!(model.requests().as_slice(), std::slice::from_ref(&prepared));
            assert!(
                prepared.tools.is_empty(),
                "unadvertised sampling tools leaked from the supplied executor"
            );
            let files = prepared
                .messages
                .last()
                .ok_or_else(|| Error::NotFound("media input".into()))?
                .content
                .file_refs()
                .collect::<Vec<_>>();
            assert_eq!(files.len(), 2);
            let mut bytes = Vec::new();
            for file in files {
                bytes.push(context.read_file(file, None).await?);
            }
            assert_eq!(bytes.len(), 2);
            assert_eq!(bytes[0].as_ref(), b"media");
            assert_eq!(bytes[1].as_ref(), b"audio");
            callbacks.admit(tool, &request).await?;
            assert_eq!(model.dispatches().len(), 1);
        }
        assert_eq!(model.remaining(), 0);
    }
    Ok(())
}
