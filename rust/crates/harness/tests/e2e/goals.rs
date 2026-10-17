use acyclic_harness::{goal::GoalContinuationPolicy, test_support::ScriptedTurn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct GoalCase {
    #[serde(default)]
    pub seed: BTreeMap<String, String>,
    #[serde(default)]
    pub project_seed: BTreeMap<String, String>,
    pub prompt: String,
    pub turns: Vec<ScriptedTurn>,
    #[serde(default = "one_step")]
    pub max_steps: u32,
    #[serde(default = "automatic")]
    pub policy: Option<GoalContinuationPolicy>,
    pub expect: Expectations,
}

fn automatic() -> Option<GoalContinuationPolicy> {
    Some(Default::default())
}

fn one_step() -> u32 {
    1
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Expectations {
    pub text: String,
    pub runtime_error: Option<String>,
    pub request_prompt: String,
    pub result: Value,
    pub view: Option<Value>,
    pub view_error: Option<String>,
    pub frames: Vec<Value>,
    pub statuses: Vec<Value>,
    pub files: BTreeMap<String, Option<String>>,
    #[serde(default)]
    pub project_files: BTreeMap<String, Option<String>>,
    pub requests: Option<Vec<Value>>,
}

impl GoalCase {
    pub(super) fn parse(value: Value) -> Result<Self, String> {
        let case: Self = serde_json::from_value(value).map_err(|error| error.to_string())?;
        if case.expect.view.is_some() == case.expect.view_error.is_some() {
            return Err("expect exactly one of goal view or error".into());
        }
        if [&case.expect.view_error, &case.expect.runtime_error]
            .into_iter()
            .any(|error| error.as_ref().is_some_and(String::is_empty))
        {
            return Err("goal error must not be empty".into());
        }
        for value in std::iter::once(&case.expect.result).chain(case.expect.view.as_ref()) {
            if value.as_object().is_none_or(|object| object.is_empty()) {
                return Err("goal assertions must be nonempty objects".into());
            }
        }
        if !case.expect.files.contains_key("GOAL.md") {
            return Err("assert final GOAL.md bytes or absence".into());
        }
        if case
            .project_seed
            .keys()
            .any(|path| !case.expect.project_files.contains_key(path))
        {
            return Err("assert every seeded project file's final bytes or absence".into());
        }
        if case.max_steps == 0
            || case.expect.statuses.len() < 2
            || case
                .expect
                .statuses
                .iter()
                .any(|status| status.get("state").and_then(Value::as_str).is_none())
            || case.expect.frames.is_empty()
            || case
                .expect
                .frames
                .iter()
                .any(|frame| frame.as_object().is_none_or(|object| object.is_empty()))
        {
            return Err("goal cases require model steps and frame assertions".into());
        }
        Ok(case)
    }
}
