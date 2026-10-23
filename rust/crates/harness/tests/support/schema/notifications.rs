use acyclic_harness::client::{
    AttentionNotification, NotificationMode, TerminalNotificationProtocol,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Notifications {
    records: Vec<AttentionNotification>,
    /// Record indices identifying files removed before presentation.
    #[serde(default)]
    remove: Vec<usize>,
    #[serde(default)]
    mode: NotificationMode,
    protocol: TerminalNotificationProtocol,
    #[serde(default)]
    focused: bool,
    expect: Expectations,
}
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Expectations {
    output: String,
    presented: usize,
    view: Option<Value>,
}
impl Notifications {
    pub(super) fn parse(value: Value) -> Result<Self, String> {
        let case: Self = serde_json::from_value(value).map_err(|error| error.to_string())?;
        if case.remove.iter().any(|index| *index >= case.records.len()) {
            return Err("removal must reference an existing record".into());
        }
        if case
            .expect
            .view
            .as_ref()
            .is_some_and(|value| !value.is_array())
        {
            return Err("view expectation must be an array of current records".into());
        }
        Ok(case)
    }
}
