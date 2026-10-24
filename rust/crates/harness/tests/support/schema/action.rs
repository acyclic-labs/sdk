//! One action record shared by scenario admission and the native driver.
use acyclic_harness::task_volume::Control;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Action {
    pub model: String,
    pub request: usize,
    pub agent: String,
    pub control: Option<Control>,
    pub message: Option<String>,
}
