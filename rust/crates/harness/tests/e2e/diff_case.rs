use acyclic_harness::test_support::ScriptedTurn;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DiffCase {
    #[serde(default)]
    pub seed: BTreeMap<String, String>,
    pub prompt: String,
    pub turns: Vec<ScriptedTurn>,
    pub expect: Expectations,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Expectations {
    pub text: String,
    pub wire: Vec<Value>,
    pub frames: Vec<Value>,
    pub files: BTreeMap<String, String>,
    pub diffs: BTreeMap<String, Value>,
}
