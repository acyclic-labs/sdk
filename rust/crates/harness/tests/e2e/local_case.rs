use acyclic_harness::test_support::ScriptedTurn;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LocalCase {
    #[serde(default)]
    pub seed: BTreeMap<String, String>,
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
    pub wire: Vec<Value>,
    pub frames: Vec<Value>,
    pub statuses: Vec<Value>,
    pub files: BTreeMap<String, Option<String>>,
    pub diffs: Option<BTreeMap<String, Value>>,
    pub results: Option<BTreeMap<String, Value>>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ExpectedOutcome {
    #[default]
    Succeeded,
    Indeterminate,
}

impl LocalCase {
    pub(super) fn validate(&self) -> Result<(), &'static str> {
        if self.prompt.is_empty()
            || self.turns.is_empty()
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
