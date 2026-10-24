use super::TestResult;
use acyclic_harness::{task_volume::Control, test_support::ScriptedTurn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Case {
    prompt: String,
    #[serde(default)]
    seed: BTreeMap<String, String>,
    /// Commit the seed into a host Git repository before the harness opens.
    #[serde(default)]
    repository: bool,
    models: BTreeMap<String, Vec<ScriptedTurn>>,
    #[serde(default)]
    routes: BTreeMap<String, String>,
    #[serde(default)]
    actions: Vec<Action>,
    #[serde(default)]
    reader: Reader,
    expect: Expected,
}

/// Whose authority a model request's file parts are read under.
#[derive(Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Reader {
    /// The executing task's own reader, as the harness binds it.
    #[default]
    Task,
    /// The first reader bound, reused for every task: the former
    /// composition-wide binding, kept only as a negative control.
    Composition,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Expected {
    text: Option<String>,
    #[serde(default)]
    indeterminate: bool,
    requests: BTreeMap<String, usize>,
    task_files: BTreeMap<String, Value>,
    #[serde(default)]
    model_requests: BTreeMap<String, Vec<Value>>,
    #[serde(default)]
    files: BTreeMap<String, Option<String>>,
    #[serde(default)]
    active: Option<Vec<String>>,
    #[serde(default)]
    toolsets: BTreeMap<String, Vec<String>>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Action {
    model: String,
    request: usize,
    agent: String,
    control: Option<Control>,
    message: Option<String>,
}

impl Case {
    pub(super) fn validate(&self) -> TestResult {
        if self.expect.indeterminate != self.expect.text.is_none() {
            return Err("scenario must select either an answer or an indeterminate outcome".into());
        }
        if self.models.keys().ne(self.expect.requests.keys()) {
            return Err("every scripted model needs an exact request count".into());
        }
        for (name, turns) in &self.models {
            if turns.len()
                != *self
                    .expect
                    .requests
                    .get(name)
                    .ok_or("missing request count")?
            {
                return Err(
                    format!("model {name}: script must contain exactly the expected work").into(),
                );
            }
        }
        Ok(())
    }
}
