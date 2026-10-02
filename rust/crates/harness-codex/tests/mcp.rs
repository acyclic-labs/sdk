//! A3 acceptance: the MCP endpoint over the tool registry.

#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "test assertions"
)]

mod support;

use acyclic_harness_codex::mcp::McpEndpoint;
use serde_json::{Value, json};

const PENDING: &str = "A3: the MCP endpoint is not built yet";

async fn endpoint() -> McpEndpoint {
    McpEndpoint::start(support::registry(), support::scope(), None)
        .await
        .expect(PENDING)
}

/// One JSON-RPC call; accepts either a JSON or an SSE (streamable HTTP) reply.
async fn rpc(
    endpoint: &McpEndpoint,
    token: Option<&str>,
    method: &str,
    params: Value,
) -> (u16, Value) {
    let mut request = reqwest::Client::new()
        .post(endpoint.url())
        .header("accept", "application/json, text/event-stream")
        .json(&json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}));
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let response = request.send().await.expect("endpoint reachable");
    let status = response.status().as_u16();
    let text = response.text().await.expect("body");
    let json = text
        .lines()
        .find_map(|line| line.strip_prefix("data:").map(str::trim))
        .unwrap_or(&text);
    (status, serde_json::from_str(json).unwrap_or(Value::Null))
}

async fn call(endpoint: &McpEndpoint, name: &str, arguments: Value) -> Value {
    let (status, reply) = rpc(
        endpoint,
        Some(endpoint.token()),
        "tools/call",
        json!({"name": name, "arguments": arguments}),
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        reply.get("error").is_none(),
        "tool problems are results, not JSON-RPC errors: {reply}"
    );
    reply["result"].clone()
}

#[tokio::test]
#[ignore = "A3: the MCP endpoint is not built yet"]
async fn calls_without_the_turn_token_are_refused() {
    let endpoint = endpoint().await;
    assert!(endpoint.url().starts_with("http://127.0.0.1:") && endpoint.url().ends_with("/mcp"));
    assert_eq!(rpc(&endpoint, None, "tools/list", json!({})).await.0, 401);
    assert_eq!(
        rpc(&endpoint, Some("wrong"), "tools/list", json!({}))
            .await
            .0,
        401
    );
}

#[tokio::test]
#[ignore = "A3: the MCP endpoint is not built yet"]
async fn initialize_offers_tools_on_the_codex_protocol_version() {
    let endpoint = endpoint().await;
    let (status, reply) = rpc(
        &endpoint,
        Some(endpoint.token()),
        "initialize",
        json!({"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "codex-mcp-client", "version": "0.155.1"}}),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(reply["result"]["protocolVersion"], "2025-06-18");
    assert!(reply["result"]["capabilities"]["tools"].is_object());
}

#[tokio::test]
#[ignore = "A3: the MCP endpoint is not built yet"]
async fn the_list_is_exactly_the_granted_tools() {
    let endpoint = endpoint().await;
    let (_, reply) = rpc(&endpoint, Some(endpoint.token()), "tools/list", json!({})).await;
    let names: Vec<&str> = reply["result"]["tools"]
        .as_array()
        .expect("tools")
        .iter()
        .filter_map(|tool| tool["name"].as_str())
        .collect();
    assert_eq!(
        names,
        ["acyclic_echo"],
        "acyclic.secret is registered but not granted"
    );
    let echo = &reply["result"]["tools"][0];
    assert_eq!(
        echo["inputSchema"]["required"],
        json!(["text"]),
        "the registry schema is passed through"
    );
}

#[tokio::test]
#[ignore = "A3: the MCP endpoint is not built yet"]
async fn a_call_runs_the_registry_tool() {
    let endpoint = endpoint().await;
    let result = call(&endpoint, "acyclic_echo", json!({"text": "ping"})).await;
    assert_ne!(result["isError"], true);
    assert_eq!(result["structuredContent"], json!({"echo": "ping"}));
    assert!(
        result["content"][0]["text"]
            .as_str()
            .is_some_and(|text| text.contains("ping"))
    );
}

#[tokio::test]
#[ignore = "A3: the MCP endpoint is not built yet"]
async fn refusals_and_failures_are_error_results() {
    let endpoint = endpoint().await;
    for (name, arguments, why) in [
        ("acyclic_secret", json!({"text": "x"}), "not granted"),
        ("acyclic_missing", json!({"text": "x"}), "not registered"),
        (
            "acyclic_echo",
            json!({"fail": true}),
            "schema violation: text is required",
        ),
        (
            "acyclic_echo",
            json!({"text": "x", "fail": true}),
            "the tool itself failed",
        ),
    ] {
        let result = call(&endpoint, name, arguments).await;
        assert_eq!(result["isError"], true, "{why}");
        assert!(
            result["content"][0]["text"]
                .as_str()
                .is_some_and(|text| !text.is_empty()),
            "{why}: the model is told why"
        );
    }
}
