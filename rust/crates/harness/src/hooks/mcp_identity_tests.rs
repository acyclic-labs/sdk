//! Adapter output must retain one identity through hooks and approval admission.
use super::*;
use crate::{
    OperationId, TaskId,
    registry::ComponentIdentity,
    tool::{ToolDefinition, ToolInvocation},
};
use acyclic_harness::model::{
    ModelContent, ModelDispatch, ModelEvent, ModelMessage, ModelProvider, ModelRequest, ModelRole,
    PreparedModelRequest,
};
use acyclic_harness_providers::{Credentials, Registry};
use futures::{FutureExt as _, TryStreamExt as _};
use serde_json::json;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

thread_local! {
    pub(crate) static SUBSTITUTE_HOOK_NAME: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    pub(crate) static SUBSTITUTE_APPROVAL_NAME: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

const CANONICAL: &str = "mcp.docs.search";
const WIRE: &str = "mcp__docs__search";

async fn endpoint(adapter: &str) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let reply = match adapter {
        "responses" => vec![
            json!({"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","call_id":"call","name":WIRE,"arguments":"{}"}}),
            json!({"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":1,"output_tokens":1}}}),
        ],
        "chat" => vec![
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call","function":{"name":WIRE,"arguments":"{}"}}]},"finish_reason":"tool_calls"}]}),
            json!({"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1}}),
        ],
        "anthropic" => vec![
            json!({"type":"message_start","message":{"id":"run","usage":{"input_tokens":1,"output_tokens":0}}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call","name":WIRE,"input":{}}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{}"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":1}}),
            json!({"type":"message_stop"}),
        ],
        "gemini" => vec![
            json!({"candidates":[{"content":{"parts":[{"functionCall":{"id":"call","name":WIRE,"args":{}}}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1}}),
        ],
        _ => unreachable!(),
    };
    let mut body: String = reply
        .iter()
        .map(|item| format!("data: {item}\n\n"))
        .collect();
    if adapter == "chat" {
        body.push_str("data: [DONE]\n\n");
    }
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut input = Vec::new();
        let mut buffer = [0; 1024];
        let request = loop {
            let read = socket.read(&mut buffer).await.unwrap();
            assert!(read != 0);
            input.extend_from_slice(&buffer[..read]);
            if let Some(end) = input.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&input[..end]).to_ascii_lowercase();
                let length: usize = headers
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length:"))
                    .unwrap()
                    .trim()
                    .parse()
                    .unwrap();
                if input.len() >= end + 4 + length {
                    break serde_json::from_slice::<Value>(&input[end + 4..end + 4 + length])
                        .unwrap();
                }
            }
        };
        assert!(request.to_string().contains(WIRE));
        assert!(!request.to_string().contains(CANONICAL));
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
    });
    (url, server)
}

async fn canonical_consumers(adapter: &str) {
    let (url, server) = endpoint(adapter).await;
    let mut registry = Registry::new(Credentials::explicit(Default::default())).unwrap();
    registry.register("fixture".into(), serde_json::from_value(json!({
        "adapter":adapter,"authentication":"optional","base_url":url,
        "models":{"tiny":{"capacity":{"context_tokens":8192,"output_tokens":1024},"features":{"tools":true,"images":false}}},"max_event_bytes":4096,
        "token_counter":{"path":"count","adapter":adapter,"response_field":"input_tokens","framing_tokens":0}
    })).unwrap()).unwrap();
    let provider = registry.resolve("fixture/tiny").unwrap();
    let definition = ToolDefinition {
        name: CANONICAL.into(),
        revision: "1".into(),
        description: "Search documents".into(),
        input_schema: json!({"type":"object"}),
        output_schema: json!({}),
        projection_schema: json!({}),
    };
    let request = PreparedModelRequest::prepare(
        ModelRequest {
            model: provider.model().clone(),
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("search".into()),
            }],
            tools: vec![
                serde_json::from_value(serde_json::to_value(&definition).unwrap()).unwrap(),
            ],
            max_output_tokens: Some(64),
        },
        acyclic_harness::conversation::Limits::default(),
        None,
    )
    .unwrap();
    let dispatch = ModelDispatch {
        operation_id: acyclic_harness::OperationId::from_bytes([1; 16]),
        step: 0,
        request_digest: request.request_digest(),
    };
    let events: Vec<_> = provider
        .generate(request, dispatch)
        .try_collect()
        .await
        .unwrap();
    server.await.unwrap();
    let calls: Vec<_> = events
        .into_iter()
        .filter_map(|event| match event {
            ModelEvent::ToolCall(call) => Some(call),
            _ => None,
        })
        .collect();
    assert_eq!(calls.len(), 1);
    let call = &calls[0];
    assert_eq!(call.name, CANONICAL);
    let invocation = ToolInvocation::for_model_call(
        OperationId::from_bytes([1; 16]),
        0,
        call.call_id.clone(),
        call.name.clone(),
        call.arguments.clone(),
        None,
    )
    .unwrap();
    let mut hooks = Hooks::default();
    for event in [HookEvent::PreToolUse, HookEvent::PostToolUse] {
        hooks
            .add_rules([HookRule {
                on: event,
                matcher: r"^mcp\.docs\.search$".into(),
                hooks: vec![HookSpec {
                    run: HookRun::Executable {
                        executable: "guard".into(),
                        argv: vec![],
                    },
                    timeout_ms: 1000,
                    failure: HookFailure::Block,
                }],
            }])
            .unwrap();
        let output = hooks.run_commands(event, &json!({"tool_name":invocation.name}), &ToolRegistry::default(), 4096, |_,_| async {
            Ok((Some(0), br#"{"hookSpecificOutput":{"permissionDecision":"ask","permissionDecisionReason":"canonical guard","additionalContext":"canonical guard"}}"#.to_vec(), Vec::new()))
        }).await.unwrap();
        match event {
            HookEvent::PreToolUse => assert_eq!(
                output.approval.as_deref(),
                Some("canonical guard"),
                "canonical hook identity"
            ),
            HookEvent::PostToolUse => assert_eq!(
                output.additional_context,
                ["canonical guard"],
                "canonical hook identity"
            ),
            _ => unreachable!(),
        }
    }
    let task = TaskId::from_bytes([2; 16]);
    let policy = ComponentIdentity {
        name: "test.policy".into(),
        version: "1".into(),
        digest: [3; 32],
    };
    let expected =
        crate::executor::preparation_digest(&(task, &policy, &definition, &invocation), None)
            .unwrap();
    let ((_, approval), _) = crate::runtime::tool_approval_request(
        task,
        &policy,
        &definition,
        &invocation,
        ("canonical guard".into(), None),
        None,
    )
    .unwrap();
    let crate::interaction::Interaction::Approval {
        operation_id,
        action_digest,
        ..
    } = approval
    else {
        panic!("approval kind")
    };
    assert_eq!(operation_id, invocation.operation_id);
    assert_eq!(action_digest, expected, "canonical approval identity");
    let mut substituted = invocation.clone();
    substituted.name = WIRE.into();
    let substituted =
        crate::executor::preparation_digest(&(task, &policy, &definition, &substituted), None)
            .unwrap();
    assert_ne!(action_digest, substituted);
}

#[tokio::test]
async fn all_http_adapters_bind_canonical_hooks_and_approvals() {
    for adapter in ["responses", "chat", "anthropic", "gemini"] {
        canonical_consumers(adapter).await;
    }
}

async fn detects_substitution(
    flag: &'static std::thread::LocalKey<std::cell::Cell<bool>>,
    message: &str,
) {
    flag.with(|flag| flag.set(true));
    let result = std::panic::AssertUnwindSafe(canonical_consumers("responses"))
        .catch_unwind()
        .await;
    flag.with(|flag| flag.set(false));
    let panic = result.expect_err("consumer mutation must fail the same identity oracle");
    let text = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(text.contains(message), "{text}");
}

#[tokio::test]
async fn negative_control_detects_hook_identity_substitution() {
    detects_substitution(&SUBSTITUTE_HOOK_NAME, "canonical hook identity").await;
}

#[tokio::test]
async fn negative_control_detects_approval_identity_substitution() {
    detects_substitution(&SUBSTITUTE_APPROVAL_NAME, "canonical approval identity").await;
}
