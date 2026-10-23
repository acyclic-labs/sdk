use acyclic_harness::task_volume::Channel;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskQueryCase {
    #[serde(default)]
    pub folder: String,
    pub writes: Vec<File>,
    pub expect: Expectations,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct File {
    pub path: String,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<Channel>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Expectations {
    pub files: BTreeMap<String, String>,
    pub changes: Vec<Option<File>>,
    #[serde(default)]
    pub errors: Vec<Failure>,
    pub order: Option<Vec<String>>,
    pub heads: Option<BTreeMap<String, String>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Failure {
    Immutable,
    Sequence,
    InvalidPath,
    InvalidSegment,
}
