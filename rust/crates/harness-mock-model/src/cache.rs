//! Provider prompt-cache emulation. A request reads the longest prefix that an
//! earlier request wrote, at a boundary the provider would cache, and only when
//! the request asks in that provider's form:
//! - `messages` (Anthropic): a `cache_control` on the system block caches the
//!   tools and system; the top-level automatic one caches the conversation,
//!   looking back at most 20 content blocks for an earlier entry.
//! - `responses` and `chat/completions`: every message boundary, scoped by
//!   `prompt_cache_key`; without a key nothing is reused. A routed Chat request
//!   that also carries `cache_control` reports its writes.
//! - Gemini: implicit at every message boundary, scoped by the endpoint.
//!
//! Markers never count as prefix bytes, so a moving breakpoint is not a miss.
//! Tokens are canonical prefix bytes divided by the configured width.
use serde_json::Value;
use std::{
    collections::BTreeSet,
    hash::{DefaultHasher, Hash, Hasher},
};

const LOOKBACK: usize = 20;
/// The longest `prompt_cache_key` OpenAI accepts.
const MAX_KEY: usize = 64;

/// Cache tokens of one request, already included in its input count.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Cached {
    /// Tokens served from an earlier write.
    pub read: u64,
    /// Tokens this request wrote beyond what it read.
    pub write: u64,
}

/// Prefixes written so far, by scope and canonical bytes.
#[derive(Debug, Default)]
pub struct PromptCache {
    written: BTreeSet<u64>,
}

/// One cacheable position: its prefix identity and token length.
#[derive(Clone, Copy)]
struct Boundary {
    id: u64,
    tokens: u64,
}

fn strip(value: &mut Value) {
    match value {
        Value::Object(map) => {
            map.remove("cache_control");
            map.values_mut().for_each(strip);
        }
        Value::Array(items) => items.iter_mut().for_each(strip),
        _ => {}
    }
}

/// Keep message metadata in the prefix while exposing each content-block boundary.
fn content_blocks(messages: Vec<Value>) -> Vec<Value> {
    let mut blocks = Vec::new();
    for mut message in messages {
        strip(&mut message);
        let content = message
            .as_object_mut()
            .and_then(|map| map.remove("content"));
        let content = match content {
            Some(Value::Array(blocks)) => blocks,
            Some(content) => vec![serde_json::json!({"type":"text","text":content})],
            None => Vec::new(),
        };
        for (index, block) in content.into_iter().enumerate() {
            blocks.push(if index == 0 {
                serde_json::json!({"message":message,"block":block})
            } else {
                serde_json::json!({"block":block})
            });
        }
    }
    blocks
}

/// Boundaries after each prefix of `items`, following `head`, in order.
fn boundaries(scope: &str, head: &Value, items: &[Value], width: u64) -> Vec<Boundary> {
    let mut hasher = DefaultHasher::new();
    scope.hash(&mut hasher);
    let head = head.to_string();
    head.hash(&mut hasher);
    let mut bytes = head.len() as u64;
    let mut out = vec![Boundary {
        id: hasher.finish(),
        tokens: bytes.div_ceil(width),
    }];
    for item in items {
        let item = item.to_string();
        item.hash(&mut hasher);
        bytes += item.len() as u64;
        out.push(Boundary {
            id: hasher.finish(),
            tokens: bytes.div_ceil(width),
        });
    }
    out
}

impl PromptCache {
    /// Observes one generation request and records what it writes.
    pub fn observe(&mut self, path: &str, body: &Value, width: u64) -> Cached {
        self.observe_if(path, body, width, Some).unwrap_or_default()
    }

    /// A rejected reply cannot commit any of its prospective cache writes.
    pub(crate) fn observe_if<T>(
        &mut self,
        path: &str,
        body: &Value,
        width: u64,
        accept: impl FnOnce(Cached) -> Option<T>,
    ) -> Option<T> {
        let width = width.max(1);
        let field = |name: &str| body.get(name).cloned().unwrap_or(Value::Null);
        let items = |name: &str| {
            body.get(name)
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
        };
        // (boundaries, readable indexes, written indexes, whether writes are reported)
        let (all, readable, writes, reported): (Vec<Boundary>, Vec<usize>, Vec<usize>, bool) =
            match path {
                "messages" => {
                    let marked_system = body["system"]
                        .as_array()
                        .and_then(|blocks| blocks.last())
                        .is_some_and(|block| block.get("cache_control").is_some());
                    let automatic = body.get("cache_control").is_some();
                    let mut head = serde_json::json!({
                        "model": field("model"), "tools": field("tools"), "system": field("system"),
                    });
                    strip(&mut head);
                    let blocks = content_blocks(items("messages"));
                    let all = boundaries("messages", &head, &blocks, width);
                    let end = blocks.len();
                    let mut points = Vec::new();
                    if marked_system {
                        points.push(0);
                    }
                    let mut readable = points.clone();
                    if automatic {
                        readable.extend(end.saturating_sub(LOOKBACK - 1)..=end);
                        points.push(end);
                    }
                    (all, readable, points, true)
                }
                "responses" | "chat/completions" => {
                    // OpenAI refuses longer keys; the request is never cached.
                    let Some(key) = body["prompt_cache_key"]
                        .as_str()
                        .filter(|key| key.len() <= MAX_KEY)
                    else {
                        return accept(Cached::default());
                    };
                    let (name, head) = if path == "responses" {
                        (
                            "input",
                            serde_json::json!({"model": field("model"), "tools": field("tools"), "instructions": field("instructions")}),
                        )
                    } else {
                        (
                            "messages",
                            serde_json::json!({"model": field("model"), "tools": field("tools")}),
                        )
                    };
                    let messages = items(name);
                    let all = boundaries(key, &head, &messages, width);
                    let end = messages.len();
                    let routed = body.get("cache_control").is_some();
                    (all, (0..=end).collect(), (0..=end).collect(), routed)
                }
                path if path.starts_with("models/") => {
                    let head = serde_json::json!({
                        "systemInstruction": field("systemInstruction"), "tools": field("tools"),
                    });
                    let contents = items("contents");
                    let all = boundaries(path, &head, &contents, width);
                    let end = contents.len();
                    (all, (0..=end).collect(), (0..=end).collect(), false)
                }
                _ => return accept(Cached::default()),
            };
        let read = readable
            .iter()
            .filter(|&&index| self.written.contains(&all[index].id))
            .map(|&index| all[index].tokens)
            .max()
            .unwrap_or(0);
        let mut furthest = read;
        for &index in &writes {
            furthest = furthest.max(all[index].tokens);
        }
        let result = accept(Cached {
            read,
            write: if reported { furthest - read } else { 0 },
        })?;
        self.written
            .extend(writes.into_iter().map(|index| all[index].id));
        Some(result)
    }
}
