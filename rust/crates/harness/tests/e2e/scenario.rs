use super::expectations::Expectations;
use acyclic_harness::test_support::{ScriptedReconciliation, ScriptedTurn};
use acyclic_harness::{
    context::{CompactionPolicy, ModelContextCapacity},
    model::ModelMessage,
};
use serde::Deserialize;
use serde_json::Value;
use std::{collections::BTreeMap, path::Path};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    #[serde(default)]
    pub seed: BTreeMap<String, String>,
    #[serde(default)]
    pub kind: Kind,
    pub prompt: Option<String>,
    pub instruction: Option<String>,
    pub turns: Vec<ScriptedTurn>,
    #[serde(default)]
    pub context: Vec<ModelMessage>,
    pub discovery: Option<super::discovery::Discovery>,
    pub capacity: Option<ModelContextCapacity>,
    pub compaction: Option<CompactionPolicy>,
    pub max_output_tokens: Option<u32>,
    pub message_tokens: Option<u32>,
    #[serde(default)]
    pub recoveries: Vec<ScriptedReconciliation>,
    #[serde(default)]
    pub resume: Vec<String>,
    #[serde(default)]
    pub limits: BTreeMap<String, Value>,
    pub max_steps: Option<u32>,
    #[serde(default)]
    pub replay: bool,
    pub expect: Expectations,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    #[default]
    Turn,
    Summary,
}

impl Scenario {
    pub fn take_instruction(&mut self) -> Result<String, String> {
        match self.kind {
            Kind::Turn => self.prompt.take(),
            Kind::Summary => self.instruction.take(),
        }
        .ok_or_else(|| "missing scenario instruction".into())
    }

    pub fn load(root: &Path, path: &Path) -> Result<Self, String> {
        Self::from_value(load_value(root, path)?)
    }

    pub fn execution_limits(&self) -> Result<acyclic_harness::conversation::Limits, String> {
        let defaults = acyclic_harness::conversation::Limits::default();
        if self.limits.is_empty() {
            return Ok(defaults);
        }
        let mut value = serde_json::to_value(defaults).map_err(|e| e.to_string())?;
        for (key, override_value) in &self.limits {
            let slot = value
                .get_mut(key)
                .ok_or_else(|| format!("unknown limit: {key}"))?;
            *slot = override_value.clone();
        }
        serde_json::from_value(value).map_err(|e| e.to_string())
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, String> {
        let value: Value = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
        Self::from_value(value)
    }

    pub(super) fn from_value(value: Value) -> Result<Self, String> {
        let forbidden = match value.get("kind").and_then(Value::as_str) {
            Some("summary") => &["prompt", "max_steps", "discovery"][..],
            _ => &["instruction"][..],
        };
        for key in forbidden {
            if value.get(key).is_some() {
                return Err(format!("{key} does not apply to this scenario kind"));
            }
        }
        let scenario: Self = serde_json::from_value(value).map_err(|e| e.to_string())?;
        let instruction = match scenario.kind {
            Kind::Summary => &scenario.instruction,
            Kind::Turn => &scenario.prompt,
        };
        if instruction.is_none() {
            return Err(format!("missing instruction for {:?}", scenario.kind));
        }
        if scenario.expect.text.is_some() == scenario.expect.error.is_some() {
            return Err("expect exactly one of text or error".into());
        }
        if scenario.expect.error.as_ref().is_some_and(String::is_empty) {
            return Err("error expectation must not be empty".into());
        }
        if scenario.resume.iter().any(String::is_empty) {
            return Err("resume error expectation must not be empty".into());
        }
        if scenario.expect.error.is_some() && (scenario.expect.result.is_some() || scenario.replay)
        {
            return Err("result and replay require a successful turn".into());
        }
        Ok(scenario)
    }
}

pub(super) use super::scenario_data::load_value;
