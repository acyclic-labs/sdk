use acyclic_harness::{
    OperationId, Result,
    context::{Context, ContextInput, ContextPlacement, ContextStage, DiscoveredContext},
    conversation::{
        BoundedContentFuture, ContentIo, ContentResidencyVerifier, FileDescriptor, FileRef, Limits,
    },
    model::{
        ModelContent, ModelContentPart, ModelDispatch, ModelMessage, ModelProvider, ModelRequest,
        ModelRole, PreparedModelRequest, ToolCall, ToolResultContent,
    },
};
use acyclic_harness_providers::{Credentials, Registry};
use axum::{Json, Router, routing::post};
use futures::TryStreamExt;
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

const BODY: &str = "Project marker: Î»\nverbatim <literal>\n";

fn source(path: &str, bytes: &[u8]) -> FileRef {
    FileRef::new(
        serde_json::from_value(json!({
            "provider":{"namespace":"test","family":"filesystem","version":"2"},
            "id":"project","class":"project","owner":{"kind":"project","id":"project"}
        }))
        .unwrap(),
        path.into(),
        "1".into(),
        FileDescriptor::from_bytes(bytes, "text/markdown", &mut None).unwrap(),
        "rules".into(),
        &mut None,
    )
    .unwrap()
}

struct Reader(AtomicUsize);
impl ContentResidencyVerifier for Reader {
    fn read<'a>(
        &'a self,
        file: &'a FileRef,
        maximum: u64,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        assert_eq!(maximum, file.descriptor().byte_length());
        BoundedContentFuture::new(io, || async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(BODY.as_bytes().to_vec().into())
        })
    }
}

async fn discovered(files: Vec<FileRef>) -> Context {
    projected(
        files.into_iter().map(Into::into).collect(),
        Context::default(),
    )
    .await
}

async fn projected(
    files: Vec<acyclic_harness::model::ProjectInstruction>,
    context: Context,
) -> Context {
    let stage = DiscoveredContext {
        instructions: files,
        skills: vec![],
    }
    .stage(ContextPlacement::Prepend)
    .unwrap();
    let input = ContextInput {
        input: ModelContent::Text("hello".into()),
        selected_context: None,
        step: 0,
        prior_messages: vec![],
    };
    stage
        .apply(&input, context, Limits::default(), None)
        .unwrap()
        .await
        .unwrap()
}

// Actual HTTP, registry, request preparation and public stage path. Identity and
// changed-body controls exercise the same path rather than a stand-alone matcher.
#[tokio::test]
async fn instruction_review_loopback_scope_budget_and_read_deduplication() {
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let received = Arc::new(Mutex::new(Vec::<Value>::new()));
        let observed = received.clone();
        let app = Router::new().route("/responses", post(move |Json(body): Json<Value>| {
            observed.lock().unwrap().push(body);
            async { ([("content-type", "text/event-stream")], super::ANSWER) }
        }));
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let reader = Arc::new(Reader(AtomicUsize::new(0)));
        let mut registry = Registry::new(Credentials::explicit(Default::default())).unwrap()
            .with_content(reader.clone(), 4096);
        registry.register("fake".into(), serde_json::from_value(json!({
            "adapter":"responses", "authentication":"optional", "base_url":format!("http://{address}"),
            "models":{"tiny":{"capacity":{"context_tokens":8192,"output_tokens":1024},"features":{"tools":true,"images":false}}},
            "max_event_bytes":4096
        })).unwrap()).unwrap();
        let provider = registry.resolve("fake/tiny").unwrap();
        let agents = source("nested/AGENTS.md", BODY.as_bytes());
        let claude = source("CLAUDE.md", BODY.as_bytes());
        let context = discovered(vec![agents.clone(), agents.clone(), claude.clone()]).await;
        assert_eq!(context.messages.len(), 2, "same source must be projected once");
        for (read_file, returned, repeated) in [
            (agents.clone(), BODY, true),
            (source("elsewhere/AGENTS.md", BODY.as_bytes()), BODY, false),
            (agents.clone(), "changed after capture", false),
            (source("nested/AGENTS.md", b"changed after capture"), "changed after capture", false),
        ] {
            let mut messages = Vec::new();
            messages.push(ModelMessage { role: ModelRole::Assistant, content: ModelContent::Part(
                ModelContentPart::ToolCall(ToolCall::new("call".into(), "read".into(), json!({"file":read_file})))
            ) });
            messages.push(ModelMessage { role: ModelRole::Tool, content: ModelContent::Part(
                ModelContentPart::ToolResult { call_id: "call".into(), name: "read".into(),
                    content: ToolResultContent::Json { value: json!(returned) } }
            ) });
            let captured = acyclic_harness::model::ProjectInstruction::Captured {
                file: source("inputs/context/private-copy", BODY.as_bytes()),
                source: acyclic_harness::model::InstructionIdentity::new(agents.volume().clone(), agents.path().into()).unwrap(),
            };
            let context = projected(vec![captured, claude.clone().into()], Context { messages, ..Context::default() }).await;
            let request = PreparedModelRequest::prepare(ModelRequest { model: provider.model().clone(),
                messages: context.messages, tools: vec![acyclic_harness::tool::ToolDefinition {
                    name: "read".into(), revision: "1".into(), description: "Read".into(),
                    input_schema: json!({}), output_schema: json!({}), projection_schema: json!({}),
                }], max_output_tokens: Some(64) }, Limits::default(), None).unwrap();
            let dispatch = ModelDispatch { operation_id: OperationId::new(), step: 0, request_digest: request.request_digest() };
            provider.generate(request, dispatch).try_collect::<Vec<_>>().await.unwrap();
            let body = received.lock().unwrap().last().unwrap().clone();
            assert_eq!(body["input"][0], json!({"type":"message","role":"user","content":[{
                "type":"input_text","text":format!("# AGENTS.md instructions for nested\n\n<INSTRUCTIONS>\n{BODY}\n</INSTRUCTIONS>")
            }]}));
            assert_eq!(body["input"][1]["content"][0]["text"], format!("# CLAUDE.md instructions for .\n\n<INSTRUCTIONS>\n{BODY}\n</INSTRUCTIONS>"));
            assert_eq!(body["input"][2]["type"], "function_call");
            assert_eq!(body["input"][3]["type"], "function_call_output");
            let output: String = serde_json::from_str(body["input"][3]["output"].as_str().unwrap()).unwrap();
            assert_eq!(output.contains(returned), !repeated);
            assert_eq!(output.contains("instructions already included"), repeated);
        }
        // Individually valid bodies exceed the combined bound. No reader or HTTP
        // call may run, unlike a per-file-only (deliberately broken) admission.
        let bound = acyclic_harness::model::InstructionSource::new(&agents).framed_bytes().unwrap();
        assert!(acyclic_harness::model::InstructionSource::new(&claude).framed_bytes().unwrap() <= bound);
        let before = reader.0.load(Ordering::SeqCst);
        let limited = provider.bind_content(reader.clone(), bound).unwrap();
        let request = PreparedModelRequest::prepare(ModelRequest { model: provider.model().clone(),
            messages: context.messages, tools: vec![], max_output_tokens: Some(64) }, Limits::default(), None).unwrap();
        let dispatch = ModelDispatch { operation_id: OperationId::new(), step: 0, request_digest: request.request_digest() };
        assert!(limited.generate(request, dispatch).try_collect::<Vec<_>>().await.is_err());
        assert_eq!(reader.0.load(Ordering::SeqCst), before);
        assert_eq!(received.lock().unwrap().len(), 4);
        server.abort();
    }).await.expect("loopback watchdog");
}
