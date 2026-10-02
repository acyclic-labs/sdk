//! Typed `codex exec --json` events, pinned to the recorded 0.155.1 fixtures.
//!
//! Every line is first read as a JSON value, so duplicate keys (Codex writes
//! `id` twice on `web_search` items) and new fields never fail a turn. Event or
//! item types this version does not know are kept as [`CodexEvent::Other`] and
//! [`ItemKind::Other`] for the observer.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One line of `codex exec --json` output.
#[derive(Clone, Debug, PartialEq)]
pub enum CodexEvent {
    /// The thread id; emitted again, unchanged, by `codex exec resume`.
    ThreadStarted {
        /// Codex thread id, the handle `resume` takes.
        thread_id: String,
    },
    /// A turn began.
    TurnStarted,
    /// The turn ended normally.
    TurnCompleted {
        /// Cumulative usage for the whole thread, not just this turn.
        usage: CodexUsage,
    },
    /// The turn ended in an error. With the exit code, this is authoritative.
    TurnFailed {
        /// Codex's user-facing message.
        message: String,
    },
    /// An item began. `agent_message` and `reasoning` never send this.
    ItemStarted(CodexItem),
    /// An item changed (`todo_list` progress, for example).
    ItemUpdated(CodexItem),
    /// An item finished.
    ItemCompleted(CodexItem),
    /// A top-level error notice. Not terminal: retries are reported this way.
    Error {
        /// The notice text, such as `Reconnecting... 1/5 (...)`.
        message: String,
    },
    /// An event type this version does not know, kept verbatim.
    Other(Value),
}

/// Cumulative token usage as Codex reports it on `turn.completed`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodexUsage {
    /// Prompt tokens, including cached ones.
    #[serde(default)]
    pub input_tokens: u64,
    /// Prompt tokens served from cache.
    #[serde(default)]
    pub cached_input_tokens: u64,
    /// Prompt tokens written to cache.
    #[serde(default)]
    pub cache_write_input_tokens: u64,
    /// Generated tokens, including reasoning.
    #[serde(default)]
    pub output_tokens: u64,
    /// Generated reasoning tokens.
    #[serde(default)]
    pub reasoning_output_tokens: u64,
}

impl CodexUsage {
    /// Usage added since `earlier`, for turns that resume a thread.
    #[must_use]
    pub const fn since(self, earlier: Self) -> Self {
        Self {
            input_tokens: self.input_tokens.saturating_sub(earlier.input_tokens),
            cached_input_tokens: self
                .cached_input_tokens
                .saturating_sub(earlier.cached_input_tokens),
            cache_write_input_tokens: self
                .cache_write_input_tokens
                .saturating_sub(earlier.cache_write_input_tokens),
            output_tokens: self.output_tokens.saturating_sub(earlier.output_tokens),
            reasoning_output_tokens: self
                .reasoning_output_tokens
                .saturating_sub(earlier.reasoning_output_tokens),
        }
    }
}

/// An item with its Codex id (`item_N`) and typed body.
#[derive(Clone, Debug, PartialEq)]
pub struct CodexItem {
    /// Codex's per-thread item id.
    pub id: String,
    /// What the item is.
    pub kind: ItemKind,
}

/// Lifecycle state of a tool-like item.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ItemStatus {
    /// Still running.
    InProgress,
    /// Finished successfully.
    Completed,
    /// Finished with an error.
    Failed,
    /// Refused by approval policy.
    Declined,
    /// A status this version does not know.
    #[serde(other)]
    Unknown,
}

/// One path touched by a `file_change` item.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileUpdate {
    /// Absolute path inside the workspace.
    pub path: String,
    /// `add`, `delete` or `update`.
    pub kind: String,
}

/// A `todo_list` entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TodoEntry {
    /// The entry text.
    pub text: String,
    /// Whether Codex marked it done.
    pub completed: bool,
}

/// The typed body of an item.
#[derive(Clone, Debug, PartialEq)]
pub enum ItemKind {
    /// Assistant text. The last one is the turn's answer.
    AgentMessage {
        /// Message text.
        text: String,
    },
    /// Reasoning summary text.
    Reasoning {
        /// Summary text.
        text: String,
    },
    /// A shell command Codex ran itself.
    CommandExecution {
        /// The command as Codex wrapped it (`<shell> -lc '…'`).
        command: String,
        /// Combined stdout and stderr.
        aggregated_output: String,
        /// Exit code once finished.
        exit_code: Option<i64>,
        /// Lifecycle state.
        status: ItemStatus,
    },
    /// Files Codex edited with `apply_patch`.
    FileChange {
        /// The paths touched.
        changes: Vec<FileUpdate>,
        /// Lifecycle state.
        status: ItemStatus,
    },
    /// A call to one of our tools through the MCP endpoint.
    McpToolCall {
        /// MCP server name from `config.toml`.
        server: String,
        /// Tool name as exposed over MCP.
        tool: String,
        /// Call arguments.
        arguments: Value,
        /// MCP result once finished.
        result: Option<Value>,
        /// Error message when the call failed.
        error: Option<String>,
        /// Lifecycle state.
        status: ItemStatus,
    },
    /// A Codex subagent operation (`spawn_agent`, `send_input`, `wait`, `close_agent`).
    CollabToolCall {
        /// The subagent operation.
        tool: String,
        /// The full item, kept for traces.
        raw: Value,
    },
    /// A built-in web search (disabled in our config, parsed for completeness).
    WebSearch {
        /// The query text.
        query: String,
    },
    /// Codex's plan.
    TodoList {
        /// The entries.
        items: Vec<TodoEntry>,
    },
    /// A non-fatal warning, such as missing model metadata.
    Error {
        /// Warning text.
        message: String,
    },
    /// An item type this version does not know, kept verbatim.
    Other(Value),
}

/// A line that is not a JSON object with a string `type`.
#[derive(Debug, thiserror::Error)]
#[error("codex event is malformed: {0}")]
pub struct ParseError(String);

/// Parses one `codex exec --json` line.
///
/// # Errors
/// Only when the line is not JSON or has no string `type`; unknown types and
/// unexpected shapes of known types become `Other`.
pub fn parse_line(line: &str) -> Result<CodexEvent, ParseError> {
    let value: Value = serde_json::from_str(line).map_err(|error| ParseError(error.to_string()))?;
    let kind = value
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| ParseError("missing string `type`".into()))?;
    Ok(match kind {
        "thread.started" => str_field(&value, "thread_id").map_or_else(
            || CodexEvent::Other(value.clone()),
            |thread_id| CodexEvent::ThreadStarted { thread_id },
        ),
        "turn.started" => CodexEvent::TurnStarted,
        "turn.completed" => value
            .get("usage")
            .and_then(|usage| serde_json::from_value(usage.clone()).ok())
            .map_or_else(
                || CodexEvent::Other(value.clone()),
                |usage| CodexEvent::TurnCompleted { usage },
            ),
        "turn.failed" => CodexEvent::TurnFailed {
            message: value
                .get("error")
                .and_then(|error| str_field(error, "message"))
                .unwrap_or_default(),
        },
        "item.started" | "item.updated" | "item.completed" => match value.get("item").map(item) {
            Some(Some(item)) if kind == "item.started" => CodexEvent::ItemStarted(item),
            Some(Some(item)) if kind == "item.updated" => CodexEvent::ItemUpdated(item),
            Some(Some(item)) => CodexEvent::ItemCompleted(item),
            _ => CodexEvent::Other(value),
        },
        "error" => CodexEvent::Error {
            message: str_field(&value, "message").unwrap_or_default(),
        },
        _ => CodexEvent::Other(value),
    })
}

fn str_field(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(str::to_owned)
}

fn status(value: &Value) -> ItemStatus {
    value
        .get("status")
        .and_then(|status| serde_json::from_value(status.clone()).ok())
        .unwrap_or(ItemStatus::Unknown)
}

fn item(value: &Value) -> Option<CodexItem> {
    let id = str_field(value, "id")?;
    let text = || str_field(value, "text").unwrap_or_default();
    let kind = match value.get("type").and_then(Value::as_str)? {
        "agent_message" => ItemKind::AgentMessage { text: text() },
        "reasoning" => ItemKind::Reasoning { text: text() },
        "command_execution" => ItemKind::CommandExecution {
            command: str_field(value, "command").unwrap_or_default(),
            aggregated_output: str_field(value, "aggregated_output").unwrap_or_default(),
            exit_code: value.get("exit_code").and_then(Value::as_i64),
            status: status(value),
        },
        "file_change" => ItemKind::FileChange {
            changes: value
                .get("changes")
                .and_then(|changes| serde_json::from_value(changes.clone()).ok())
                .unwrap_or_default(),
            status: status(value),
        },
        "mcp_tool_call" => ItemKind::McpToolCall {
            server: str_field(value, "server").unwrap_or_default(),
            tool: str_field(value, "tool").unwrap_or_default(),
            arguments: value.get("arguments").cloned().unwrap_or(Value::Null),
            result: value
                .get("result")
                .filter(|result| !result.is_null())
                .cloned(),
            error: value
                .get("error")
                .and_then(|error| str_field(error, "message")),
            status: status(value),
        },
        "collab_tool_call" => ItemKind::CollabToolCall {
            tool: str_field(value, "tool").unwrap_or_default(),
            raw: value.clone(),
        },
        "web_search" => ItemKind::WebSearch {
            query: str_field(value, "query").unwrap_or_default(),
        },
        "todo_list" => ItemKind::TodoList {
            items: value
                .get("items")
                .and_then(|items| serde_json::from_value(items.clone()).ok())
                .unwrap_or_default(),
        },
        "error" => ItemKind::Error {
            message: str_field(value, "message").unwrap_or_default(),
        },
        _ => ItemKind::Other(value.clone()),
    };
    Some(CodexItem { id, kind })
}

/// What a finished `codex exec` run amounted to, folded from its events.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Transcript {
    /// The thread id from `thread.started`.
    pub thread_id: Option<String>,
    /// The last `agent_message`, which is the turn's answer.
    pub final_message: Option<String>,
    /// Cumulative thread usage from `turn.completed`.
    pub usage: Option<CodexUsage>,
    /// The `turn.failed` message, if the turn failed.
    pub failure: Option<String>,
    /// Completed items that did work (commands, edits, tool calls, messages).
    pub actions: u32,
    /// Non-fatal warnings (`error` items and `error` notices), in order.
    pub warnings: Vec<String>,
}

impl Transcript {
    /// Folds one event into the transcript.
    pub fn observe(&mut self, event: &CodexEvent) {
        match event {
            CodexEvent::ThreadStarted { thread_id } => self.thread_id = Some(thread_id.clone()),
            CodexEvent::TurnCompleted { usage } => self.usage = Some(*usage),
            CodexEvent::TurnFailed { message } => self.failure = Some(message.clone()),
            CodexEvent::Error { message } => self.warnings.push(message.clone()),
            CodexEvent::ItemCompleted(CodexItem { kind, .. }) => match kind {
                ItemKind::AgentMessage { text } => {
                    self.final_message = Some(text.clone());
                    self.actions = self.actions.saturating_add(1);
                }
                ItemKind::Error { message } => self.warnings.push(message.clone()),
                ItemKind::CommandExecution { .. }
                | ItemKind::FileChange { .. }
                | ItemKind::McpToolCall { .. }
                | ItemKind::CollabToolCall { .. }
                | ItemKind::WebSearch { .. } => self.actions = self.actions.saturating_add(1),
                ItemKind::Reasoning { .. } | ItemKind::TodoList { .. } | ItemKind::Other(_) => {}
            },
            CodexEvent::TurnStarted
            | CodexEvent::ItemStarted(_)
            | CodexEvent::ItemUpdated(_)
            | CodexEvent::Other(_) => {}
        }
    }

    /// Folds every line of a JSONL transcript.
    ///
    /// # Errors
    /// When a line is not a Codex event at all.
    pub fn from_jsonl(jsonl: &str) -> Result<Self, ParseError> {
        let mut transcript = Self::default();
        for line in jsonl.lines().filter(|line| !line.trim().is_empty()) {
            transcript.observe(&parse_line(line)?);
        }
        Ok(transcript)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! fixture {
        ($name:literal) => {
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/fixtures/codex-0.155.1/",
                $name,
                ".stdout.jsonl"
            ))
        };
    }

    #[test]
    fn a_plain_turn_yields_the_message_thread_and_usage() -> Result<(), ParseError> {
        let transcript = Transcript::from_jsonl(fixture!("ok"))?;
        assert_eq!(transcript.final_message.as_deref(), Some("hi from fake"));
        assert!(transcript.thread_id.is_some());
        assert!(transcript.failure.is_none());
        assert_eq!(transcript.usage.map(|usage| usage.output_tokens), Some(7));
        assert_eq!(transcript.actions, 1);
        // The unknown-model notice is a warning, not a failure.
        assert_eq!(transcript.warnings.len(), 1);
        Ok(())
    }

    #[test]
    fn every_recorded_line_parses_without_falling_back() -> Result<(), ParseError> {
        let all = [
            fixture!("ok"),
            fixture!("ok-gpt55"),
            fixture!("shell"),
            fixture!("patch"),
            fixture!("reasoning"),
            fixture!("resume"),
            fixture!("402"),
            fixture!("500"),
            fixture!("quota429"),
            fixture!("quotafailed"),
            fixture!("response-failed"),
            fixture!("400"),
            fixture!("missing-envkey"),
            fixture!("mcp-404"),
        ];
        for line in all.iter().flat_map(|jsonl| jsonl.lines()) {
            let event = parse_line(line)?;
            let fell_back = matches!(&event, CodexEvent::Other(_))
                || matches!(
                    &event,
                    CodexEvent::ItemStarted(item) | CodexEvent::ItemUpdated(item) | CodexEvent::ItemCompleted(item)
                        if matches!(item.kind, ItemKind::Other(_))
                );
            assert!(
                !fell_back,
                "fixture line did not map to a typed event: {line}"
            );
        }
        Ok(())
    }

    #[test]
    fn shell_and_patch_items_are_typed() -> Result<(), ParseError> {
        let shell = Transcript::from_jsonl(fixture!("shell"))?;
        assert_eq!(shell.actions, 2, "one command and the answer");
        let completed = fixture!("shell")
            .lines()
            .filter_map(|line| parse_line(line).ok())
            .find_map(|event| match event {
                CodexEvent::ItemCompleted(CodexItem {
                    kind:
                        ItemKind::CommandExecution {
                            exit_code,
                            status,
                            aggregated_output,
                            ..
                        },
                    ..
                }) => Some((exit_code, status, aggregated_output)),
                _ => None,
            });
        assert_eq!(
            completed,
            Some((Some(0), ItemStatus::Completed, "probe-output\n".to_owned()))
        );
        let patch = fixture!("patch")
            .lines()
            .filter_map(|line| parse_line(line).ok())
            .any(|event| {
                matches!(event, CodexEvent::ItemCompleted(CodexItem {
                    kind: ItemKind::FileChange { ref changes, status: ItemStatus::Completed }, ..
                }) if changes.iter().any(|change| change.kind == "add" && change.path.ends_with("hello.txt")))
            });
        assert!(patch);
        Ok(())
    }

    #[test]
    fn usage_is_cumulative_across_a_resumed_thread() -> Result<(), ParseError> {
        let first = Transcript::from_jsonl(fixture!("ok"))?
            .usage
            .unwrap_or_default();
        let resumed = Transcript::from_jsonl(fixture!("resume"))?
            .usage
            .unwrap_or_default();
        assert_eq!(
            resumed.since(first),
            first,
            "the resumed turn added one turn's worth"
        );
        Ok(())
    }

    #[test]
    fn retries_are_notices_and_only_turn_failed_ends_the_turn() -> Result<(), ParseError> {
        let retried = Transcript::from_jsonl(fixture!("402"))?;
        assert!(
            retried
                .failure
                .as_deref()
                .is_some_and(|failure| failure.contains("402"))
        );
        assert_eq!(
            retried
                .warnings
                .iter()
                .filter(|warning| warning.starts_with("Reconnecting"))
                .count(),
            5
        );
        let quota = Transcript::from_jsonl(fixture!("quota429"))?;
        assert_eq!(
            quota.failure.as_deref(),
            Some("Quota exceeded. Check your plan and billing details.")
        );
        let missing_key = Transcript::from_jsonl(fixture!("missing-envkey"))?;
        assert!(
            missing_key
                .failure
                .is_some_and(|failure| failure.contains("Missing environment variable"))
        );
        Ok(())
    }

    #[test]
    fn unknown_types_and_duplicate_keys_survive() -> Result<(), ParseError> {
        assert!(matches!(
            parse_line(r#"{"type":"turn.paused","x":1}"#)?,
            CodexEvent::Other(_)
        ));
        let search = parse_line(
            r#"{"type":"item.completed","item":{"id":"item_3","type":"web_search","id":"ws_1","query":"q","action":{}}}"#,
        )?;
        assert!(matches!(
            search,
            CodexEvent::ItemCompleted(CodexItem {
                kind: ItemKind::WebSearch { .. },
                ..
            })
        ));
        let future =
            parse_line(r#"{"type":"item.completed","item":{"id":"item_4","type":"hologram"}}"#)?;
        assert!(matches!(
            future,
            CodexEvent::ItemCompleted(CodexItem {
                kind: ItemKind::Other(_),
                ..
            })
        ));
        assert!(parse_line("not json").is_err());
        assert!(parse_line(r#"{"no_type":true}"#).is_err());
        Ok(())
    }
}
