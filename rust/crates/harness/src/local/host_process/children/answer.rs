//! Completion from the native host's original turn terminal.
use super::{Outcome, Value};

#[cfg(any(test, feature = "test-support"))]
pub mod validation;

/// A lost frame and malformed JSON share the same omission boundary.
pub(super) fn decode(line: std::io::Result<String>) -> Option<(String, Value)> {
    line.ok().and_then(|line| {
        serde_json::from_str::<Value>(&line)
            .ok()
            .map(|event| (line, event))
    })
}

/// A preview is not a completed reply. Native hosts can also report a failed
/// turn before exiting successfully; their original terminal owns the outcome.
#[derive(Default)]
pub(super) struct NativeAnswer {
    pub(super) pending: Option<String>,
    pub(super) outcome: Option<Outcome<String>>,
    pub(super) omitted: bool,
    pub(super) last_part: Option<String>,
}

impl NativeAnswer {
    /// Framing loss invalidates a preview, but is not a native terminal.
    /// A later complete reply can recover; a native refusal stays authoritative.
    pub(super) fn omit(&mut self) {
        self.pending = None;
        self.last_part = None;
        if matches!(self.outcome, Some(Outcome::Succeeded(_))) {
            self.outcome = None;
        }
        self.omitted = true;
    }

    pub(super) fn record(&mut self, event: &Value) {
        let kind = event.get("type").and_then(Value::as_str);
        // A native refusal owns this attempt across every host format.
        // Only an original new-turn boundary can release that terminal.
        if !matches!(kind, Some("turn.started" | "step_start"))
            && matches!(
                self.outcome,
                Some(Outcome::Failed { .. } | Outcome::Cancelled | Outcome::Indeterminate { .. })
            )
        {
            return;
        }
        if event.get("event").and_then(Value::as_str) == Some("result") {
            self.pending = None;
            let result = &event["result"];
            self.outcome = match result.get("status").and_then(Value::as_str) {
                Some("SUCCESS")
                    if result.get("finished_reason").is_none()
                        && result.get("error").is_none() =>
                {
                    result
                        .get("response")
                        .and_then(Value::as_str)
                        .map(|text| Outcome::Succeeded(text.into()))
                }
                Some("CANCELED" | "INTERRUPTED") => Some(Outcome::Cancelled),
                _ => Some(Outcome::Failed {
                    message: result
                        .get("error")
                        .and_then(Value::as_str)
                        .unwrap_or("step=read native reply: native turn did not complete; recovery=acyclic agents")
                        .into(),
                }),
            };
            return;
        }
        match kind {
            Some("turn.started" | "step_start") => *self = Self::default(),
            Some("item.completed")
                if event.pointer("/item/type").and_then(Value::as_str) == Some("agent_message")
                    && self.outcome.is_none() =>
            {
                self.pending = event
                    .pointer("/item/text")
                    .and_then(Value::as_str)
                    .map(String::from);
            }
            Some("turn.completed") if self.outcome.is_none() => {
                self.outcome = self.pending.take().map(Outcome::Succeeded);
            }
            Some("text")
                if event
                    .pointer("/part/time/end")
                    .and_then(Value::as_u64)
                    .is_some_and(|end| end > 0)
                    && self.outcome.is_none()
                    && !self.omitted =>
            {
                if let (Some(id), Some(text)) = (
                    event
                        .pointer("/part/id")
                        .and_then(Value::as_str)
                        .filter(|id| !id.is_empty()),
                    event.pointer("/part/text").and_then(Value::as_str),
                ) && self.last_part.as_deref() != Some(id)
                {
                    self.pending.get_or_insert_with(String::new).push_str(text);
                    self.last_part = Some(id.into());
                }
            }
            Some("step_finish")
                if event.pointer("/part/reason").and_then(Value::as_str) == Some("stop")
                    && self.outcome.is_none()
                    && !self.omitted =>
            {
                self.outcome = self.pending.take().map(Outcome::Succeeded);
            }
            Some("turn.failed" | "error") => {
                self.pending = None;
                self.outcome = Some(Outcome::Failed {
                    message: event
                        .pointer("/error/message")
                        .or_else(|| event.pointer("/error/data/message"))
                        .or_else(|| event.get("message"))
                        .and_then(Value::as_str)
                        .unwrap_or("step=read native reply: native model turn failed; recovery=acyclic agents")
                        .into(),
                });
            }
            Some("result") => {
                self.outcome = if event.get("subtype").and_then(Value::as_str) == Some("success")
                    && event.get("is_error").and_then(Value::as_bool) == Some(false)
                {
                    event
                        .get("result")
                        .and_then(Value::as_str)
                        .map(|text| Outcome::Succeeded(text.into()))
                } else {
                    Some(Outcome::Failed {
                        message: event
                            .get("result")
                            .and_then(Value::as_str)
                            .unwrap_or("step=read native reply: native model turn failed; recovery=acyclic agents")
                            .into(),
                    })
                };
            }
            Some("message_end")
                if event.pointer("/message/role").and_then(Value::as_str) == Some("assistant") =>
            {
                self.outcome = match event.pointer("/message/stopReason").and_then(Value::as_str) {
                    Some("stop") => event
                        .pointer("/message/content")
                        .and_then(Value::as_array)
                        .map(|parts| {
                            Outcome::Succeeded(
                                parts
                                    .iter()
                                    .filter_map(|part| part.get("text").and_then(Value::as_str))
                                    .collect(),
                            )
                        }),
                    Some("error") => Some(Outcome::Failed {
                        message: event
                            .pointer("/message/errorMessage")
                            .and_then(Value::as_str)
                            .unwrap_or("step=read native reply: native assistant turn failed; recovery=acyclic agents")
                            .into(),
                    }),
                    Some("aborted") => Some(Outcome::Cancelled),
                    _ => None,
                };
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_replies_require_their_original_successful_terminal() {
        for captured in [
            include_str!("../fixtures/codex-completed.jsonl"),
            include_str!("../fixtures/claude-code-completed.jsonl"),
        ] {
            let mut events = captured.lines().peekable();
            let mut answer = NativeAnswer::default();
            while let Some(line) = events.next() {
                let event: Value = serde_json::from_str(line).expect("captured native JSON");
                // Removing the original terminal leaves no completed reply,
                // even when a native agent-message preview was received.
                assert!(answer.outcome.is_none());
                if event.get("type").and_then(Value::as_str) != Some("turn.started")
                    && answer.pending.is_none()
                {
                    // Derived framing loss before the captured reply must not
                    // become a sticky native refusal.
                    answer.omit();
                }
                answer.record(&event);
                if events.peek().is_none() {
                    assert_eq!(
                        answer.outcome,
                        Some(Outcome::Succeeded("PAIRED_NATIVE_DONE".into()))
                    );
                    if event.get("type").and_then(Value::as_str) == Some("result") {
                        // Derived negative: the captured success text cannot
                        // override the native terminal's failure flag.
                        let mut refused = event.clone();
                        refused["is_error"] = Value::Bool(true);
                        answer.record(&refused);
                        answer.omit();
                        answer.record(&event);
                        assert!(matches!(answer.outcome, Some(Outcome::Failed { .. })));
                    } else {
                        // Derived negative: a late completion cannot erase a
                        // failed turn. Only the captured next turn-start can
                        // release it for another attempt.
                        let mut refused = event.clone();
                        refused["type"] = Value::String("turn.failed".into());
                        answer.record(&refused);
                        answer.omit();
                        answer.record(&event);
                        assert!(matches!(answer.outcome, Some(Outcome::Failed { .. })));
                        for line in captured.lines() {
                            answer
                                .record(&serde_json::from_str(line).expect("captured native JSON"));
                        }
                        assert_eq!(
                            answer.outcome,
                            Some(Outcome::Succeeded("PAIRED_NATIVE_DONE".into()))
                        );
                    }
                }
            }
        }
        let mut refused = NativeAnswer::default();
        for line in include_str!("../fixtures/pi-assistant-error.jsonl").lines() {
            refused.record(&serde_json::from_str(line).expect("captured Pi JSON"));
        }
        let original = refused.outcome.clone();
        let line = include_str!("../fixtures/pi-assistant-error.jsonl")
            .lines()
            .next()
            .expect("captured Pi assistant terminal");
        let mut late: Value = serde_json::from_str(line).expect("captured Pi JSON");
        // Derived negative: a later assistant completion, or even an
        // unrecognized stop reason, cannot replace the original failure.
        for reason in ["stop", "unrecognized"] {
            late["message"]["stopReason"] = Value::String(reason.into());
            refused.record(&late);
            assert_eq!(refused.outcome, original);
        }
        assert!(
            matches!(refused.outcome, Some(Outcome::Failed { ref message }) if message == "No API key for provider: fixture")
        );
    }
}
