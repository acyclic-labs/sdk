//! A settled turn's tool calls as a view shows them: one collapsed card per
//! call, projected from the turn's own transcript, so every client renders
//! the same cards from the one record and nothing else is stored.
use super::TurnOutput;
use crate::model::{ModelContentPart, ToolResultContent};
use serde_json::Value;

/// One tool call of a settled turn, collapsed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolCard {
    /// The registered tool name.
    pub name: String,
    /// The argument that says what the call acted on (its command, path,
    /// pattern, query or URL), or the arguments as JSON.
    pub summary: String,
    /// The call failed or was refused, or the turn ended before its result.
    pub failed: bool,
}

impl ToolCard {
    /// The card's text: the name, then the summary.
    #[must_use]
    pub fn line(&self) -> String {
        if self.summary.is_empty() {
            self.name.clone()
        } else {
            format!("{}  {}", self.name, self.summary)
        }
    }
}

/// The arguments that name what a call acts on, in preference order.
const SUBJECTS: [&str; 7] = [
    "command",
    "path",
    "file_path",
    "pattern",
    "query",
    "url",
    "name",
];

impl TurnOutput {
    /// The turn's tool calls in request order, each with its paired
    /// result's outcome.
    #[must_use]
    pub fn tool_cards(&self) -> Vec<ToolCard> {
        let mut cards: Vec<(&str, ToolCard)> = Vec::new();
        for part in self
            .transcript
            .iter()
            .flat_map(|message| message.content.parts())
        {
            match part {
                ModelContentPart::ToolCall(call) => cards.push((
                    &call.call_id,
                    ToolCard {
                        name: call.name.clone(),
                        summary: summary(&call.arguments),
                        failed: true,
                    },
                )),
                ModelContentPart::ToolResult {
                    call_id, content, ..
                } => {
                    if let Some((_, card)) =
                        cards.iter_mut().find(|(id, _)| *id == call_id.as_str())
                    {
                        card.failed = failed(content);
                    }
                }
                _ => {}
            }
        }
        cards.into_iter().map(|(_, card)| card).collect()
    }
}

fn summary(arguments: &Value) -> String {
    SUBJECTS
        .iter()
        .find_map(|key| arguments.get(key).and_then(Value::as_str))
        .map_or_else(
            || match arguments {
                Value::Null => String::new(),
                Value::Object(fields) if fields.is_empty() => String::new(),
                other => other.to_string(),
            },
            str::to_owned,
        )
}

/// A result reports failure structurally: an error result, or a JSON
/// result whose `success` is false or whose `error` is set.
fn failed(content: &ToolResultContent) -> bool {
    match content {
        ToolResultContent::Error { .. } => true,
        ToolResultContent::Json { value } => {
            value.get("success").and_then(Value::as_bool) == Some(false)
                || value.get("error").is_some_and(|error| !error.is_null())
        }
        ToolResultContent::Parts { .. } => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ModelContent, ModelMessage, ModelRole, ToolCall};
    use serde_json::json;

    fn result(call_id: &str, content: ToolResultContent) -> ModelMessage {
        ModelMessage {
            role: ModelRole::Tool,
            content: ModelContent::Part(ModelContentPart::ToolResult {
                call_id: call_id.into(),
                name: "shell".into(),
                content,
            }),
        }
    }

    #[test]
    fn cards_pair_each_call_with_its_own_result() {
        let calls = ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Parts(vec![
                ModelContentPart::text("checking".into()),
                ModelContentPart::ToolCall(ToolCall::new(
                    "a".into(),
                    "shell".into(),
                    json!({"command": "cargo test", "cwd": "x"}),
                )),
                ModelContentPart::ToolCall(ToolCall::new(
                    "b".into(),
                    "read".into(),
                    json!({"path": "missing.txt"}),
                )),
                ModelContentPart::ToolCall(ToolCall::new("c".into(), "plan".into(), json!({}))),
                ModelContentPart::ToolCall(ToolCall::new(
                    "d".into(),
                    "custom".into(),
                    json!({"n": 1}),
                )),
            ]),
        };
        let output = TurnOutput {
            #[cfg(feature = "research")]
            journal_cut: None,
            text: "done".into(),
            attachments: Vec::new(),
            metadata: Value::Null,
            steps: 2,
            transcript: vec![
                calls,
                // Results arrive out of call order; each pairs by identity.
                result(
                    "b",
                    ToolResultContent::Error {
                        message: "tool execution failed: absent".into(),
                    },
                ),
                result(
                    "a",
                    ToolResultContent::Json {
                        value: json!({"success": true, "exit_code": 0}),
                    },
                ),
                result(
                    "d",
                    ToolResultContent::Json {
                        value: json!({"success": false}),
                    },
                ),
            ],
        };
        let cards = output.tool_cards();
        let lines = cards.iter().map(ToolCard::line).collect::<Vec<_>>();
        assert_eq!(
            lines,
            [
                "shell  cargo test",
                "read  missing.txt",
                "plan",
                r#"custom  {"n":1}"#
            ]
        );
        // The unanswered call ("c") counts as failed: the turn ended first.
        assert_eq!(
            cards.iter().map(|card| card.failed).collect::<Vec<_>>(),
            [false, true, true, true]
        );
        // Negative control: a turn without calls has no cards.
        let plain = TurnOutput {
            transcript: Vec::new(),
            ..output
        };
        assert!(plain.tool_cards().is_empty());
    }
}
