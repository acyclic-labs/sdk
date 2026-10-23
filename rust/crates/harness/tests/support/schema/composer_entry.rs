use acyclic_harness::test_support::ScriptedTurn;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ComposerEntry {
    /// Plain checkout files, including project `.claude/commands`.
    #[serde(default)]
    seed: BTreeMap<String, String>,
    /// Plain files in the fixture-owned user home, such as `.claude/commands`.
    #[serde(default)]
    home: BTreeMap<String, String>,
    prompt: String,
    #[serde(default)]
    turns: Vec<ScriptedTurn>,
    expect: Expectations,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Expectations {
    /// Text parts of the user message in the first model request, joined.
    user_text: Option<String>,
    /// Exact bytes of each file part in that user message, in order.
    #[serde(default)]
    attachments: Vec<String>,
    /// Substrings no dispatched model request may contain.
    #[serde(default)]
    absent: Vec<String>,
    /// Required refusal substring; refusal must precede every model request.
    error: Option<String>,
}

impl ComposerEntry {
    pub(super) fn parse(value: Value) -> Result<Self, String> {
        let case: Self = serde_json::from_value(value).map_err(|error| error.to_string())?;
        let refused = case.expect.error.is_some();
        if case.prompt.is_empty()
            || refused == case.expect.user_text.is_some()
            || refused != case.turns.is_empty()
            || (refused && !case.expect.attachments.is_empty())
            || case.expect.error.as_ref().is_some_and(String::is_empty)
        {
            return Err(
                "composer entry cases expect either a model turn with user text or a refusal"
                    .into(),
            );
        }
        Ok(case)
    }
}
