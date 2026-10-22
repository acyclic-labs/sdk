//! Adapter output must retain one identity through hooks and approval admission.
use super::TestResult;
use acyclic_harness::model::{
    ModelContent, ModelDispatch, ModelEvent, ModelMessage, ModelProvider, ModelRequest, ModelRole,
    PreparedModelRequest,
};
use acyclic_harness::tool::ToolDefinition;
use acyclic_harness_providers::{Credentials, Registry};
use futures::TryStreamExt as _;
use serde_json::Value;
use serde_json::json;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

const CANONICAL: &str = "mcp.docs.search";
const WIRE: &str = "mcp__docs__search";

/// Reads one request's headers and declared JSON body.
async fn read_request(socket: &mut tokio::net::TcpStream) -> TestResult<Value> {
    let mut input = Vec::new();
    loop {
        if socket.read_buf(&mut input).await? == 0 {
            return Err("request ended before its body".into());
        }
        let Some(end) = input.windows(4).position(|bytes| bytes == b"\r\n\r\n") else {
            continue;
        };
        let headers =
            String::from_utf8_lossy(input.get(..end).unwrap_or_default()).to_ascii_lowercase();
        let length: usize = headers
            .lines()
            .find_map(|line| line.strip_prefix("content-length:"))
            .ok_or("request has no content length")?
            .trim()
            .parse()?;
        if let Some(body) = input.get(end + 4..end + 4 + length) {
            return Ok(serde_json::from_slice(body)?);
        }
    }
}

async fn endpoint(adapter: &str) -> TestResult<(String, tokio::task::JoinHandle<TestResult>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
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
        _ => return Err(format!("unknown adapter {adapter}").into()),
    };
    let mut body: String = reply
        .iter()
        .map(|item| format!("data: {item}\n\n"))
        .collect();
    if adapter == "chat" {
        body.push_str("data: [DONE]\n\n");
    }
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        let request = read_request(&mut socket).await?.to_string();
        if !request.contains(WIRE) || request.contains(CANONICAL) {
            return Err(format!("request lost the wire tool name: {request}").into());
        }
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await?;
        Ok(())
    });
    Ok((url, server))
}

async fn canonical_consumers(adapter: &str) -> TestResult {
    let (url, server) = endpoint(adapter).await?;
    let mut registry = Registry::new(Credentials::explicit(Default::default()))?;
    registry.register("fixture".into(), serde_json::from_value(json!({
        "adapter":adapter,"authentication":"optional","base_url":url,
        "models":{"tiny":{"capacity":{"context_tokens":8192,"output_tokens":1024},"features":{"tools":true,"images":false}}},"max_event_bytes":4096,
        "token_counter":{"path":"count","adapter":adapter,"response_field":"input_tokens","framing_tokens":0}
    }))?)?;
    let provider = registry.resolve("fixture/tiny")?;
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
            tools: vec![serde_json::from_value(serde_json::to_value(&definition)?)?],
            max_output_tokens: Some(64),
        },
        acyclic_harness::conversation::Limits::default(),
        None,
    )?;
    let dispatch = ModelDispatch {
        operation_id: acyclic_harness::OperationId::from_bytes([1; 16]),
        step: 0,
        request_digest: request.request_digest(),
    };
    let events: Vec<_> = provider.generate(request, dispatch).try_collect().await?;
    server.await??;
    let calls: Vec<_> = events
        .into_iter()
        .filter_map(|event| match event {
            ModelEvent::ToolCall(call) => Some(call),
            _ => None,
        })
        .collect();
    let [call] = calls.as_slice() else {
        return Err(format!("expected one tool call, got {}", calls.len()).into());
    };
    assert_eq!(call.name, CANONICAL);
    acyclic_harness::test_support::assert_canonical_tool_consumers(call).await?;
    Ok(())
}
#[tokio::test]
async fn all_http_adapters_bind_canonical_hooks_and_approvals() -> TestResult {
    for adapter in ["responses", "chat", "anthropic", "gemini"] {
        canonical_consumers(adapter).await?;
    }
    Ok(())
}
