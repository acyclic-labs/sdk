//! The MCP endpoint that exposes the consumer's tool registry to Codex (task A3).
//!
//! Contract, fixed by `tests/mcp.rs`:
//! - Streamable-HTTP MCP at `/mcp` on `127.0.0.1:0`, protocol `2025-06-18`,
//!   handling `initialize`, `notifications/initialized`, `tools/list`, `tools/call`
//!   with plain JSON responses.
//! - Requests without the per-turn bearer token get a 401.
//! - `tools/list` holds exactly the registry tools the scope grants
//!   (`tool:call:<name>`), with dots mapped to underscores (`acyclic.web` → `acyclic_web`).
//! - `tools/call` applies the stock loop's checks in the same order (grant,
//!   `authorize`, input schema, policy), then runs the registry's executor.
//!   Every refusal or failure is an `isError` result the model can read, never
//!   a transport failure.

use acyclic_harness::{
    Error, OperationId, Result,
    runtime::{RuntimeScope, ToolPolicy, ToolPolicyDecision},
    tool::{Tool, ToolInvocation, ToolRegistry},
};
use axum::{
    Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::post,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::task::JoinHandle;

/// The MCP protocol revision Codex 0.155.1 speaks.
pub const PROTOCOL_VERSION: &str = "2025-06-18";

/// Maps a registry tool name to the MCP name Codex sees.
#[must_use]
pub fn mcp_name(tool: &str) -> String {
    tool.replace('.', "_")
}

struct Shared {
    /// MCP name → registry tool, granted tools only.
    tools: BTreeMap<String, Tool>,
    scope: RuntimeScope,
    policy: Option<Arc<dyn ToolPolicy>>,
    token: String,
    parent: OperationId,
    calls: AtomicU64,
}

/// A running MCP endpoint for one turn. Dropping it stops the server.
pub struct McpEndpoint {
    url: String,
    token: String,
    names: BTreeMap<String, String>,
    task: JoinHandle<()>,
}

impl std::fmt::Debug for McpEndpoint {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpEndpoint")
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

impl McpEndpoint {
    /// Starts the endpoint over `tools`, limited to what `scope` grants.
    ///
    /// # Errors
    /// When the listener cannot bind, the registry is inconsistent, or two
    /// granted tools map to the same MCP name.
    pub async fn start(
        tools: ToolRegistry,
        scope: RuntimeScope,
        policy: Option<Arc<dyn ToolPolicy>>,
    ) -> Result<Self> {
        let mut granted = BTreeMap::new();
        let mut names = BTreeMap::new();
        for definition in tools.definitions()? {
            if !scope
                .grants()
                .contains(&format!("tool:call:{}", definition.name))
            {
                continue;
            }
            let tool = tools.get(&definition.name).ok_or_else(|| {
                Error::Invalid(format!("tool {} is not registered", definition.name))
            })?;
            let name = mcp_name(&definition.name);
            if let Some(clash) = names.insert(name.clone(), definition.name.clone()) {
                return Err(Error::Invalid(format!(
                    "tools {clash} and {} both map to MCP name {name}",
                    definition.name
                )));
            }
            granted.insert(name, tool.clone());
        }
        let token = uuid::Uuid::new_v4().simple().to_string();
        let shared = Arc::new(Shared {
            tools: granted,
            scope,
            policy,
            token: token.clone(),
            parent: OperationId::new(),
            calls: AtomicU64::new(0),
        });
        let app = Router::new()
            .route("/mcp", post(rpc).get(not_streaming).delete(not_streaming))
            .layer(DefaultBodyLimit::max(16 * 1024 * 1024))
            .with_state(shared);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| Error::Unsupported(format!("codex MCP bind: {error}")))?;
        let address = listener
            .local_addr()
            .map_err(|error| Error::Unsupported(format!("codex MCP address: {error}")))?;
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self {
            url: format!("http://{address}/mcp"),
            token,
            names,
            task,
        })
    }

    /// The URL Codex is configured with.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// The bearer token Codex must present.
    #[must_use]
    pub fn token(&self) -> &str {
        &self.token
    }

    /// The registry name behind an MCP tool name, for traces.
    #[must_use]
    pub fn registry_name(&self, mcp_name: &str) -> Option<&str> {
        self.names.get(mcp_name).map(String::as_str)
    }

    /// Whether no tool is offered at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }
}

impl Drop for McpEndpoint {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn not_streaming() -> Response {
    // Streamable HTTP lets a server decline the optional GET event stream.
    StatusCode::METHOD_NOT_ALLOWED.into_response()
}

fn reply(id: &Value, outcome: std::result::Result<Value, (i64, String)>) -> Response {
    let body = match outcome {
        Ok(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
        Err((code, message)) => {
            json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
        }
    };
    (StatusCode::OK, axum::Json(body)).into_response()
}

async fn rpc(
    State(shared): State<Arc<Shared>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let authorized = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|token| token == shared.token);
    if !authorized {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Ok(request) = serde_json::from_slice::<Value>(&body) else {
        return reply(&Value::Null, Err((-32700, "parse error".into())));
    };
    let Some(id) = request.get("id").cloned() else {
        // A notification, such as notifications/initialized.
        return StatusCode::ACCEPTED.into_response();
    };
    let params = request.get("params").cloned().unwrap_or_else(|| json!({}));
    match request.get("method").and_then(Value::as_str) {
        Some("initialize") => {
            let requested = params
                .get("protocolVersion")
                .and_then(Value::as_str)
                .unwrap_or(PROTOCOL_VERSION);
            reply(
                &id,
                Ok(json!({
                    "protocolVersion": requested,
                    "capabilities": {"tools": {"listChanged": false}},
                    "serverInfo": {"name": "acyclic", "version": env!("CARGO_PKG_VERSION")},
                })),
            )
        }
        Some("ping") => reply(&id, Ok(json!({}))),
        Some("tools/list") => reply(&id, Ok(json!({"tools": list(&shared)}))),
        Some("tools/call") => reply(&id, Ok(call(&shared, &params).await)),
        Some(other) => reply(
            &id,
            Err((-32601, format!("method {other} is not supported"))),
        ),
        None => reply(&id, Err((-32600, "request has no method".into()))),
    }
}

fn list(shared: &Shared) -> Vec<Value> {
    shared
        .tools
        .iter()
        .map(|(name, tool)| {
            json!({
                "name": name,
                "description": tool.definition.description,
                "inputSchema": tool.definition.input_schema,
            })
        })
        .collect()
}

fn error_result(message: impl Into<String>) -> Value {
    json!({"content": [{"type": "text", "text": message.into()}], "isError": true})
}

async fn call(shared: &Shared, params: &Value) -> Value {
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let arguments = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    // Ungranted tools are never listed, so to the model they do not exist:
    // one message for both, which reveals nothing about ungranted contracts.
    let Some(tool) = shared.tools.get(name) else {
        return error_result(format!("tool {name} is not available"));
    };
    let call_id = format!(
        "mcp-{}",
        shared
            .calls
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1)
    );
    let invocation = ToolInvocation::for_model_call(
        shared.parent,
        0,
        call_id,
        tool.definition.name.clone(),
        arguments,
    );
    if let Err(error) = tool.executor.authorize(Some(&shared.scope), &invocation) {
        return error_result(format!("tool {name} refused this call: {error}"));
    }
    if let Err(message) = validate(&tool.definition.input_schema, &invocation.arguments) {
        return error_result(format!("invalid arguments for {name}: {message}"));
    }
    if let Some(policy) = &shared.policy {
        match policy.evaluate(&invocation, &shared.scope).await {
            Ok(ToolPolicyDecision::Allow) => {}
            Ok(ToolPolicyDecision::Deny { reason }) => {
                return error_result(format!("policy denied {name}: {reason}"));
            }
            Ok(ToolPolicyDecision::RequireApproval { .. }) => {
                return error_result(format!(
                    "{name} needs human approval, which a Codex turn cannot ask for"
                ));
            }
            Err(error) => return error_result(format!("policy failed for {name}: {error}")),
        }
    }
    let result = match tool.executor.execute(invocation.clone()).await {
        Ok(result) => result,
        Err(error) => return error_result(format!("{name} failed: {error}")),
    };
    let projection = match tool.projection.project(&invocation, &result) {
        Ok(projection) => projection,
        Err(error) => return error_result(format!("{name} result could not be shown: {error}")),
    };
    let mut text = projection.to_string();
    let limit = usize::try_from(shared.scope.limits().render_bytes).unwrap_or(usize::MAX);
    if text.len() > limit {
        let mut cut = limit;
        while !text.is_char_boundary(cut) {
            cut -= 1;
        }
        text.truncate(cut);
        text.push_str("…[truncated]");
    }
    let structured = if result.value.is_object() {
        result.value
    } else {
        json!({"value": result.value})
    };
    json!({
        "content": [{"type": "text", "text": text}],
        "structuredContent": structured,
        "isError": false,
    })
}

fn validate(schema: &Value, value: &Value) -> std::result::Result<(), String> {
    let validator = jsonschema::validator_for(schema).map_err(|error| error.to_string())?;
    let errors: Vec<String> = validator
        .iter_errors(value)
        .map(|error| error.to_string())
        .collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn tool_names_map_dots_to_underscores() {
        assert_eq!(super::mcp_name("acyclic.web"), "acyclic_web");
        assert_eq!(super::mcp_name("acyclic.skills"), "acyclic_skills");
    }
}
