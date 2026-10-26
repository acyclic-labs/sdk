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
    pub files: BTreeMap<String, Option<String>>,
    pub diffs: Option<BTreeMap<String, Value>>,
    pub results: Option<BTreeMap<String, Value>>,
    /// Ordered subsets of the model requests actually dispatched.
    pub requests: Option<Vec<Value>>,
    /// Subsets no dispatched model request may contain.
    #[serde(default)]
    pub not_requests: Vec<Value>,
}

pub use super::expected_outcome::ExpectedOutcome;

impl LocalCase {
    pub(super) fn validate(&self) -> Result<(), &'static str> {
        let failed = self.expect.outcome != ExpectedOutcome::Succeeded;
        if self.prompt.is_empty()
            || (self.turns.is_empty() && !(failed && self.expect.error.is_some()))
            || self.expect.error.as_ref().is_some_and(String::is_empty)
            || (self.expect.error.is_some() && !failed)
            || self.expect.files.is_empty()
            || (self.expect.text.is_some() != !failed)
            || (self.expect.diffs.is_none() && self.expect.results.is_none() && !failed)
        {
            return Err("local cases require a prompt, turns and final files");
        }
        Ok(())
    }
}
