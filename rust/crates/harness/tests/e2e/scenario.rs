use super::input_files::InputFile;

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
    pub input_files: Vec<InputFile>,
    #[serde(default)]
    pub after_capture: BTreeMap<String, String>,
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
            Some("summary") => &[
                "prompt",
                "max_steps",
                "discovery",
                "input_files",
                "after_capture",
            ][..],
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

#[test]
fn scenarios_reject_typos_and_vacuous_results() {
    for input in [
        r#"{"prompt":"x","turns":[],"expect":{}}"#,
        r#"{"prompt":"x","turns":[],"expect":{"text":"x","error":"x"}}"#,
        r#"{"prompt":"x","turns":[],"expect":{"text":"x","typo":1}}"#,
        r#"{"prompt":"x","turns":[],"typo":1,"expect":{"text":"x"}}"#,
        r#"{"prompt":"x","turns":[],"expect":{"error":""}}"#,
        r#"{"prompt":"x","turns":[],"replay":true,"expect":{"error":"x"}}"#,
        r#"{"prompt":"x","turns":[],"expect":{"error":"x","result":{}}}"#,
        r#"{"prompt":"x","turns":[],"resume":[""],"expect":{"text":"x"}}"#,
        r#"{"prompt":"x","turns":[],"capacity":{"context_tokens":60,"output_tokens":10,"typo":1},"expect":{"text":"x"}}"#,
        r#"{"prompt":"x","turns":[],"compaction":{"kind":"typo"},"expect":{"text":"x"}}"#,
        r#"{"prompt":"x","turns":[],"message_tokens":-1,"expect":{"text":"x"}}"#,
        r#"{"summary":true,"prompt":"x","turns":[],"expect":{"text":"x"}}"#,
        r#"{"kind":"summary","prompt":"x","instruction":"x","turns":[],"expect":{"text":"x"}}"#,
        r#"{"kind":"summary","instruction":"x","max_steps":16,"turns":[],"expect":{"text":"x"}}"#,
        r#"{"kind":"summary","instruction":"x","max_steps":null,"turns":[],"expect":{"text":"x"}}"#,
        r#"{"kind":"summary","instruction":"x","discovery":null,"turns":[],"expect":{"text":"x"}}"#,
        r#"{"prompt":"x","discovery":{"typo":true},"turns":[],"expect":{"text":"x"}}"#,
        r#"{"prompt":"x","discovery":{"instructions":[{"typo":true}]},"turns":[],"expect":{"text":"x"}}"#,
        r#"{"prompt":"x","discovery":{"reload":"typo"},"turns":[],"expect":{"text":"x"}}"#,
        r#"{"prompt":"x","discovery":{"limits":{"entries":1,"typo":true}},"turns":[],"expect":{"text":"x"}}"#,
        r#"{"kind":"summary","turns":[],"expect":{"text":"x"}}"#,
        r#"{"kind":"turn","prompt":"x","instruction":"x","turns":[],"expect":{"text":"x"}}"#,
        r#"{"kind":"unknown","prompt":"x","turns":[],"expect":{"text":"x"}}"#,
    ] {
        assert!(
            Scenario::parse(input.as_bytes()).is_err(),
            "accepted {input}"
        );
    }
}

#[test]
fn limit_overrides_preserve_defaults_and_reject_unknown_keys() -> Result<(), String> {
    let case = Scenario::parse(
        br#"{"prompt":"x","turns":[],"limits":{"model_events_per_step":2},"expect":{"text":"x"}}"#,
    )?;
    let limits = case.execution_limits()?;
    assert_eq!(limits.model_events_per_step, 2);
    assert_eq!(
        limits.file_bytes,
        acyclic_harness::conversation::Limits::default().file_bytes
    );
    let typo =
        Scenario::parse(br#"{"prompt":"x","turns":[],"limits":{"typo":2},"expect":{"text":"x"}}"#)?;
    assert!(typo.execution_limits().is_err());
    Ok(())
}

#[test]
fn defaults_merge_from_root_to_family_and_keep_case_overrides() -> super::TestResult {
    let temp = tempfile::tempdir()?;
    let family = temp.path().join("family");
    std::fs::create_dir(&family)?;
    std::fs::write(
        temp.path().join("_defaults.json"),
        r#"{
        "prompt":"root", "turns":["root"], "capacity":{"context_tokens":60,"output_tokens":10},
        "max_output_tokens":10, "expect":{"text":"root","request_count":1}
    }"#,
    )?;
    std::fs::write(
        family.join("_defaults.json"),
        r#"{
        "prompt":"family", "expect":{"text":"family"}, "capacity":{"context_tokens":80}
    }"#,
    )?;
    let path = family.join("case.json");
    std::fs::write(
        &path,
        r#"{"turns":[],"max_output_tokens":null,"expect":{"text":"case"}}"#,
    )?;
    let case = Scenario::load(temp.path(), &path)?;
    assert_eq!(case.prompt.as_deref(), Some("family"));
    assert!(case.turns.is_empty());
    assert_eq!(case.max_output_tokens, None);
    assert_eq!(case.expect.text.as_deref(), Some("case"));
    assert_eq!(case.expect.request_count, Some(1));
    let capacity = case.capacity.ok_or("missing capacity")?;
    assert_eq!(capacity.context_tokens, 80);
    assert_eq!(capacity.output_tokens, 10);
    std::fs::write(&path, r#"{"typo":1}"#)?;
    assert!(Scenario::load(temp.path(), &path).is_err());
    std::fs::write(family.join("_defaults.json"), "{")?;
    assert!(Scenario::load(temp.path(), &path).is_err());
    Ok(())
}

#[test]
fn recovery_defaults_preserve_nested_overrides_arrays_and_nulls() -> super::TestResult {
    let temp = tempfile::tempdir()?;
    let family = temp.path().join("family");
    std::fs::create_dir(&family)?;
    std::fs::write(
        temp.path().join("_defaults.json"),
        r#"{
        "prompt":"root", "turns":["root"], "seed":{"a":"root","b":"kept"},
        "expect":{"text":"root","request_count":1}
    }"#,
    )?;
    std::fs::write(
        family.join("_defaults.json"),
        r#"{
        "prompt":"family", "seed":{"a":"family"}, "expect":{"text":"family"}
    }"#,
    )?;
    let path = family.join("case.json");
    std::fs::write(
        &path,
        r#"{"turns":[],"expect":{"text":"case","request_count":null}}"#,
    )?;
    let case = Scenario::load(temp.path(), &path)?;
    assert_eq!(case.prompt.as_deref(), Some("family"));
    assert!(case.turns.is_empty());
    assert_eq!(case.expect.request_count, None);
    assert_eq!(case.expect.text.as_deref(), Some("case"));
    assert_eq!(case.seed.get("a").map(String::as_str), Some("family"));
    assert_eq!(case.seed.get("b").map(String::as_str), Some("kept"));
    std::fs::write(&path, r#"{"typo":1}"#)?;
    assert!(Scenario::load(temp.path(), &path).is_err());
    std::fs::write(family.join("_defaults.json"), "{")?;
    assert!(Scenario::load(temp.path(), &path).is_err());
    Ok(())
}
