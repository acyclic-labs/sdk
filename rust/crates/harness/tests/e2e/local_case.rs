use acyclic_harness::test_support::ScriptedTurn;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LocalCase {
    #[serde(default)]
    pub seed: BTreeMap<String, String>,
    /// Files in the fixture-owned user home, such as `.claude/settings.json`.
    /// A JSON value other than a string is written as its JSON encoding.
    #[serde(default)]
    pub home: BTreeMap<String, Value>,
    pub prompt: String,
    pub turns: Vec<ScriptedTurn>,
    pub expect: Expectations,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Expectations {
    pub text: Option<String>,
    #[serde(default)]
    pub outcome: ExpectedOutcome,
    /// Required failure substring when specified; permits a pre-dispatch failure fixture.
    pub error: Option<String>,
    pub wire: Vec<Value>,
    pub frames: Vec<Value>,
    pub statuses: Vec<Value>,
    pub files: BTreeMap<String, Option<String>>,
    pub diffs: Option<BTreeMap<String, Value>>,
    pub results: Option<BTreeMap<String, Value>>,
    /// Effective arguments of calls a PreToolUse hook rewrote, by call id.
    /// The published tool request carries these instead of the model's.
    #[serde(default)]
    pub arguments: BTreeMap<String, Value>,
    /// Ordered subsets of the model requests actually dispatched.
    pub requests: Option<Vec<Value>>,
    /// Subsets no dispatched model request may contain.
    #[serde(default)]
    pub not_requests: Vec<Value>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpectedOutcome {
    #[default]
    Succeeded,
    Failed,
    Indeterminate,
}

impl LocalCase {
    pub(super) fn validate(&self) -> Result<(), &'static str> {
        let failed = self.expect.outcome == ExpectedOutcome::Failed;
        if self.prompt.is_empty()
            || (self.turns.is_empty() && !(failed && self.expect.error.is_some()))
            || self.expect.error.as_ref().is_some_and(String::is_empty)
            || (self.expect.error.is_some() && !failed)
            || self.expect.files.is_empty()
            || self.expect.frames.is_empty()
            || self.expect.statuses.len() < 2
            || self.expect.statuses.iter().any(|status| {
                status["state"].as_str().is_none() || !status["attention"].is_boolean()
            })
            || self
                .expect
                .frames
                .iter()
                .any(|frame| frame.as_object().is_none_or(|value| value.is_empty()))
            || (self.expect.text.is_some() != (self.expect.outcome == ExpectedOutcome::Succeeded))
            || (self.expect.diffs.is_none()
                && self.expect.results.is_none()
                && self.expect.outcome == ExpectedOutcome::Succeeded)
        {
            return Err("local cases require a prompt, turns, frames and final files");
        }
        Ok(())
    }
}

// String file arguments are references to this case's seeded immutable files.
// Bind before the provider runs; production receives an ordinary FileRef DTO.
pub(super) fn bind_seed_files(
    mut turns: Vec<ScriptedTurn>,
    references: &BTreeMap<String, Value>,
) -> Result<Vec<ScriptedTurn>, String> {
    use acyclic_harness::{model::ModelEvent, test_support::ScriptedEvent};
    for turn in &mut turns {
        if let ScriptedTurn::Events(events) = turn {
            for event in events {
                if let ScriptedEvent::Event(ModelEvent::ToolCall(call)) = event
                    && matches!(call.name.split('@').next(), Some("read" | "edit"))
                    && let Some(path) = call.arguments.get("file").and_then(Value::as_str)
                {
                    call.arguments["file"] = references
                        .get(path)
                        .ok_or_else(|| format!("unknown seed file: {path}"))?
                        .clone();
                }
            }
        }
    }
    Ok(turns)
}
