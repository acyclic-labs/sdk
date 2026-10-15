use super::{TestResult, matchers};
use acyclic_harness::client::{
    AttentionNotification, NotificationMode, NotificationView, TerminalNotificationProtocol,
    write_terminal_notifications,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Notifications {
    records: Vec<AttentionNotification>,
    #[serde(default)]
    mode: NotificationMode,
    protocol: TerminalNotificationProtocol,
    #[serde(default)]
    focused: bool,
    maximum_tasks: usize,
    maximum_pending: usize,
    maximum_text_bytes: usize,
    expect: Expectations,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Expectations {
    applied: Vec<Value>,
    output: Option<String>,
    error: Option<String>,
    presented: Option<usize>,
    view: Option<Value>,
}

impl Notifications {
    pub(super) fn verify_history(&self) -> Vec<u8> {
        super::notification_history::verify(
            self.records.iter().cloned().map(|record| (record, false)),
            (
                self.maximum_tasks,
                self.maximum_pending,
                self.maximum_text_bytes,
            ),
            (self.mode, self.protocol, self.focused),
        )
    }

    pub(super) fn parse(value: Value) -> Result<Self, String> {
        let case: Self = serde_json::from_value(value).map_err(|error| error.to_string())?;
        if case.expect.output.is_some() == case.expect.error.is_some() {
            return Err("expect exactly one of output or error".into());
        }
        if case.expect.error.as_ref().is_some_and(String::is_empty)
            || case
                .expect
                .applied
                .iter()
                .any(|value| !value.is_boolean() && value.as_str().is_none_or(str::is_empty))
        {
            return Err("apply expectations must be booleans or nonempty errors".into());
        }
        if case.records.len() != case.expect.applied.len() {
            return Err("every notification requires an apply expectation".into());
        }
        if case
            .expect
            .view
            .as_ref()
            .is_some_and(|value| value.as_object().is_none_or(|object| object.is_empty()))
        {
            return Err("view expectation must be a nonempty object".into());
        }
        if case.expect.error.is_some()
            && (!case.records.is_empty()
                || case.expect.presented.is_some()
                || case.expect.view.is_some())
        {
            return Err("constructor errors cannot have notification expectations".into());
        }
        Ok(case)
    }

    pub(super) fn run(self) -> TestResult {
        let mut view = match NotificationView::new(
            self.maximum_tasks,
            self.maximum_pending,
            self.maximum_text_bytes,
        ) {
            Err(error)
                if self
                    .expect
                    .error
                    .as_ref()
                    .is_some_and(|expected| error.to_string().contains(expected)) =>
            {
                return Ok(());
            }
            result if self.expect.error.is_some() => {
                return Err(format!(
                    "constructor: expected {:?}, got {result:?}",
                    self.expect.error
                )
                .into());
            }
            result => result?,
        };
        for (index, (record, expected)) in self
            .records
            .into_iter()
            .zip(self.expect.applied)
            .enumerate()
        {
            match (view.apply(record), expected) {
                (Ok(actual), Value::Bool(expected)) if actual == expected => {}
                (Err(actual), Value::String(expected))
                    if actual.to_string().contains(&expected) => {}
                (actual, expected) => {
                    return Err(
                        format!("apply[{index}]: expected {expected}, got {actual:?}").into(),
                    );
                }
            }
        }
        let mut output = Vec::new();
        let presented = write_terminal_notifications(
            &mut view,
            &mut output,
            self.mode,
            self.protocol,
            self.focused,
        )?;
        let output = String::from_utf8(output)?;
        if self.expect.output.as_ref() != Some(&output) {
            return Err(
                format!("output: expected {:?}, got {output:?}", self.expect.output).into(),
            );
        }
        if self
            .expect
            .presented
            .is_some_and(|expected| expected != presented)
        {
            return Err(format!(
                "presented: expected {:?}, got {presented}",
                self.expect.presented
            )
            .into());
        }
        if !view.is_empty() {
            return Err("presentation left notifications pending".into());
        }
        if let Some(expected) = self.expect.view {
            let actual = serde_json::to_value(view)?;
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
