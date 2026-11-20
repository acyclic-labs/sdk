use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Expectations {
    pub text: Option<String>,
    pub error: Option<String>,
    pub result: Option<Value>,
    pub request_count: Option<usize>,
    pub reconciliation_count: Option<usize>,
    pub events: Option<Vec<Value>>,
    pub model_events: Option<Vec<Value>>,
    #[serde(default)]
    pub not_events: Vec<Value>,
    pub requests: Option<Vec<Value>>,
    #[serde(default)]
    pub not_requests: Vec<Value>,
    pub request_files: Option<Vec<Vec<String>>>,
    #[serde(default)]
    pub files: BTreeMap<String, Option<String>>,
}

impl Expectations {
    pub fn matches_model_events(&self, events: &[Value]) -> bool {
        self.model_events.as_ref().is_none_or(|expected| {
            expected.iter().eq(events
                .iter()
                .filter(|event| event["kind"] == "model")
                .map(|event| &event["event"]))
        })
    }
}
