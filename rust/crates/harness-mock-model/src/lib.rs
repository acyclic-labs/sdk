//! Deterministic, namespaced scripts for real HTTP model qualification.
mod wire;

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
    /// JSON arguments, encoded as a string on APIs requiring it.
    pub arguments: Value,
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
        if tool && message["type"] == "function_call_output" {
            return content_text(&message["output"]);
        }
        if let Some(parts) = message["content"].as_array() {
            let results: Vec<_> = parts
                .iter()
                .filter(|part| part["type"] == "tool_result")
                .collect();
            if !results.is_empty() {
                if tool {
                    return results
                        .iter()
                        .map(|part| content_text(&part["content"]))
                        .collect::<Vec<_>>()
                        .join("\n");
                }
                continue;
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
    wire::events(path, step)
}

/// Resolve a `{"$tool_result":"/json/pointer"}` argument leaf from the last
/// real tool result. This lets scripts use runtime-generated immutable file
/// references without manufacturing or bypassing the application's authority.
pub fn bind_result(step: &Step, body: &Value) -> Result<Step, String> {
    fn bind(value: &mut Value, result: &Value) -> Result<(), String> {
        match value {
            Value::Object(fields) if fields.len() == 1 && fields.contains_key("$tool_result") => {
                let pointer = fields["$tool_result"]
                    .as_str()
                    .ok_or("tool result pointer must be a string")?;
                *value = result
                    .pointer(pointer)
                    .ok_or_else(|| format!("missing tool result pointer {pointer}"))?
                    .clone();
            }
            Value::Object(fields) => {
                for child in fields.values_mut() {
                    bind(child, result)?;
                }
            }
            Value::Array(items) => {
                for child in items {
                    bind(child, result)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    let result = serde_json::from_str(&last_content(body, true)).unwrap_or(Value::Null);
    let mut step = step.clone();
    for tool in &mut step.tools {
        bind(&mut tool.arguments, &result)?;
    }
    Ok(step)
}
