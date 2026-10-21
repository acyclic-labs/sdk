//! # Shared deterministic model server
//!
//! Decision: a standalone test-support crate, independent of Harness internals, shared
//! by the GraphCoder and plugin e2e lanes. Build before timing the suite:
//!
//! ```sh
//! cargo build -p harness-mock-model
//! harness-mock-model --script script.json --record requests.jsonl
//! ```
//!
//! The first flushed stdout line is `{"event":"ready","port":12345}`. Bind is always
//! `127.0.0.1:0`. Subsequent stdout lines are request notifications containing
//! `event`, `sequence`, `case`, `path`, `step`. Each notification follows a flushed
//! JSONL record with those fields plus `method`, `body`, `status`. Wait on these
//! lines; there is no readiness polling or timing delay. Kill/reap the process at
//! suite end. No credentials or network service are needed.
//!
//! ```json
//! {"cases":{"example":{"input_tokens":64,"steps":[
//!   {"when":{"user":"hello"},"text":["Hello ","**world**"],"tools":[
//!     {"id":"write_1","name":"write","arguments":{"path":"hello.txt","text":"file contents","media_type":"text/plain","display_name":"hello.txt"}}
//!   ]},
//!   {"when":{"tool_result":"file"},"text":["done"]}
//! ]}}}
//! ```
//!
//! Catalog `base_url`: `http://127.0.0.1:PORT/example/v1`. Adapters `responses`,
//! `chat`, `anthropic` select `/responses`, `/chat/completions`, `/messages`.
//! All emit native streaming SSE, including function argument deltas. Configure
//! `token_counter` with `path: "count"`, `adapter` matching the provider adapter,
//! `response_field: "input_tokens"`, `framing_tokens: 0`. Token requests are
//! recorded but do not consume steps. Native token-count paths work too.
//! `count_bytes: true` adds serialized request bytes to both count responses and generation usage for compaction
//! cases. A step with explicit `when.path: "count"` overrides automatic counting;
//! combine `json` and `hold` to suspend preparation until the process is killed.
//!
//! Each namespace independently consumes the first unused matching step, exactly
//! once. Optional `when` guards are ANDed: `path` is an exact endpoint suffix;
//! `user` and `tool_result` are substrings of the last corresponding content;
//! `contains` is a substring of the serialized request body. Mismatch/exhaustion
//! returns HTTP 409 and consumes nothing. Unknown namespaces fail. Guards never
//! match older user messages or tool definitions by accident.
//!
//! Optional step fields: `status` (HTTP code), `json` (ordinary JSON response for
//! errors or scripted search/MCP endpoints), `hold` (emit chunks but leave SSE open
//! until the client cancels). Text and tools default to empty. Scripts reject
//! unknown fields. Parallel requests share a local executor; consume, append,
//! flush and notification are one synchronous operation with no shared locks.
//!
//! Tool argument leaves `{"$tool_result":"/file"}` copy a JSON pointer from the
//! last actual tool result. Missing pointers fail. Use this for write → edit → read
//! with runtime-generated file references; no fabricated authority enters GraphCoder.
//!
//! Responses function calls accept `namespace`, e.g.
//! `{"id":"spawn","namespace":"collaboration","name":"spawn_agent","arguments":{}}`.
//! Freeform calls use `custom_tools`, e.g.
//! `{"id":"patch","name":"apply_patch","input":"*** Begin Patch\n..."}`.
//! These Responses-only features fail on other adapters rather than being dropped.
//! Completion usage includes exact input, output and total tokens; overflow fails.
//! JSON leaves `{"$request":"/input/0/output"}` copy request values. String
//! templates `{{request:/pointer}}` and `{{tool_result:/pointer}}` work in text,
//! arguments, freeform input and JSON replies; strings insert raw text, other
//! values compact JSON. Pointers follow RFC 6901. Missing/malformed templates
//! fail with HTTP 500; copied data is never evaluated as another template.
mod bindings;
mod wire;
pub use bindings::bind_result;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// Independent cases served by one process. URL prefixes select the case.
#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Script {
    /// Namespace to script. Each namespace has its own consumption cursor.
    pub cases: BTreeMap<String, Case>,
}

/// A finite model conversation, independent of token counting requests.
#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Case {
    /// Fixed count returned by the catalog's token counter endpoint.
    #[serde(default = "tokens")]
    pub input_tokens: u64,
    /// Add serialized request bytes to the fixed count, to force real compaction.
    #[serde(default)]
    pub count_bytes: bool,
    /// First matching unused step wins. Unmatched requests fail with HTTP 409.
    pub steps: Vec<Step>,
}
fn tokens() -> u64 {
    64
}

/// Optional guards are ANDed, and match actual request content.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Match {
    /// Exact endpoint suffix, e.g. `responses` or `search`.
    pub path: Option<String>,
    /// Substring of the last user message (never historical messages).
    pub user: Option<String>,
    /// Substring of the last tool result (never the tool definition).
    pub tool_result: Option<String>,
    /// Substring of the complete serialized request body.
    pub contains: Option<String>,
}

/// One response, consumed exactly once by a matching request.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Step {
    /// Request guards.
    pub when: Match,
    /// Text chunks, encoded as separate SSE events.
    pub text: Vec<String>,
    /// Function calls, encoded in the selected provider's native format.
    pub tools: Vec<Tool>,
    /// Responses freeform calls, emitted after function calls.
    pub custom_tools: Vec<CustomTool>,
    /// An explicit HTTP status; absent means 200.
    pub status: Option<u16>,
    /// Return ordinary JSON instead of SSE (search/MCP/error fixtures).
    pub json: Option<Value>,
    /// Leave the stream open after the chunks, for cancellation/crash cases.
    pub hold: bool,
}

/// A provider-independent function call.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tool {
    /// Stable call identity, unique within the conversation.
    pub id: String,
    /// The actual advertised tool name.
    pub name: String,
    /// Responses function namespace, for example `collaboration`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// JSON arguments, encoded as a string on APIs requiring it.
    pub arguments: Value,
}

/// A Responses freeform call, whose input is not JSON encoded.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CustomTool {
    /// Stable call identity.
    pub id: String,
    /// Advertised freeform tool name, for example `apply_patch`.
    pub name: String,
    /// Exact freeform input; request templates are supported.
    pub input: String,
    /// Optional Responses tool namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Last user/tool content, consistently across the three supported APIs.
pub fn last_content(body: &Value, tool: bool) -> String {
    let Some(messages) = body
        .get("input")
        .or_else(|| body.get("messages"))
        .and_then(Value::as_array)
    else {
        return String::new();
    };
    for message in messages.iter().rev() {
        if tool
            && (message["type"] == "function_call_output"
                || message["type"] == "custom_tool_call_output")
        {
            return content_text(&message["output"]);
        }
        if let Some(parts) = message["content"].as_array() {
            if tool {
                if let Some(result) = parts
                    .iter()
                    .rev()
                    .find(|part| part["type"] == "tool_result")
                {
                    return content_text(&result["content"]);
                }
            } else if message["role"] == "user" {
                let text: Vec<_> = parts
                    .iter()
                    .filter(|part| part["type"] != "tool_result")
                    .collect();
                if text.is_empty() {
                    continue;
                }
                return text
                    .into_iter()
                    .map(content_text)
                    .collect::<Vec<_>>()
                    .join("\n");
            }
        }
        if message["role"] == if tool { "tool" } else { "user" } {
            return content_text(&message["content"]);
        }
    }
    String::new()
}

fn content_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Array(parts) => parts
            .iter()
            .map(content_text)
            .collect::<Vec<_>>()
            .join("\n"),
        Value::Object(part) => part.get("text").map(content_text).unwrap_or_default(),
        _ => String::new(),
    }
}

/// Pure consumption state. The HTTP host owns it on a single local executor;
/// selecting, consuming and recording a request has no suspension point or lock.
pub struct Engine {
    script: Script,
    used: BTreeMap<String, Vec<bool>>,
}
impl Engine {
    /// Start independent unused scripts.
    pub fn new(script: Script) -> Self {
        let used = script
            .cases
            .iter()
            .map(|(name, case)| (name.clone(), vec![false; case.steps.len()]))
            .collect();
        Self { script, used }
    }

    /// Fixed token count for a known case.
    pub fn input_tokens(&self, namespace: &str, body: &Value) -> Option<u64> {
        self.script.cases.get(namespace).map(|case| {
            case.input_tokens.saturating_add(if case.count_bytes {
                body.to_string().len() as u64
            } else {
                0
            })
        })
    }

    /// Consume the first unused matching response. Mismatches consume nothing.
    pub fn take(&mut self, namespace: &str, path: &str, body: &Value) -> Option<(usize, &Step)> {
        let case = self.script.cases.get(namespace)?;
        let used = self.used.get_mut(namespace)?;
        let user = last_content(body, false);
        let tool = last_content(body, true);
        let encoded = body.to_string();
        for (index, (step, consumed)) in case.steps.iter().zip(used).enumerate() {
            let guard = &step.when;
            if matches!(
                path,
                "count" | "responses/input_tokens" | "messages/count_tokens"
            ) && guard.path.is_none()
            {
                continue;
            }
            if !*consumed
                && guard.path.as_deref().is_none_or(|s| s == path)
                && guard.user.as_deref().is_none_or(|s| user.contains(s))
                && guard
                    .tool_result
                    .as_deref()
                    .is_none_or(|s| tool.contains(s))
                && guard
                    .contains
                    .as_deref()
                    .is_none_or(|s| encoded.contains(s))
            {
                *consumed = true;
                return Some((index, step));
            }
        }
        None
    }
}

/// Generate provider-native SSE chunks; no sleeps, clocks or randomized IDs.
pub fn events(path: &str, step: &Step) -> Option<Vec<String>> {
    wire::events(path, step, 64)
}

/// Generate the same events with the request's deterministic counted usage.
/// The HTTP host uses this for both fixed and byte-counted namespaces.
pub fn events_with_usage(path: &str, step: &Step, input_tokens: u64) -> Option<Vec<String>> {
    wire::events(path, step, input_tokens)
}
