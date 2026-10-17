use super::{
    TestResult, matchers,
    notification_history::{self, Projection},
};
use acyclic_harness::client::{
    AttentionNotification, NotificationMode, TerminalNotificationProtocol,
    write_terminal_notifications,
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
    pub(super) fn verify_history(&self) -> Vec<u8> {
        notification_history::verify(
            self.records
                .iter()
                .cloned()
                .map(|record| (record, false))
                .chain(
                    self.remove
                        .iter()
                        .map(|index| (self.records[*index].clone(), true)),
                ),
            (self.mode, self.protocol, self.focused),
            false,
        )
    }
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
    pub(super) fn run(self) -> TestResult {
        let mut projection = Projection::new();
        for record in &self.records {
            projection.apply(record, false);
        }
        for index in self.remove {
            projection.apply(&self.records[index], true);
        }
        let mut output = Vec::new();
        let presented = write_terminal_notifications(
            projection.view.notifications(),
            &mut output,
            self.mode,
            self.protocol,
            self.focused,
        )?;
        if output != self.expect.output.as_bytes() {
            return Err(format!(
                "output: expected {:?}, got {:?}",
                self.expect.output, output
            )
            .into());
        }
        if presented != self.expect.presented {
            return Err(format!(
                "presented: expected {}, got {presented}",
                self.expect.presented
            )
            .into());
        }
        if let Some(expected) = self.expect.view {
            let actual = serde_json::to_value(projection.records())?;
            if !matchers::subset(&expected, &actual) {
                return Err(format!("view: expected {expected}, got {actual}").into());
            }
        }
        Ok(())
    }
}
#[cfg(test)]
#[path = "notifications_tests.rs"]
mod tests;
