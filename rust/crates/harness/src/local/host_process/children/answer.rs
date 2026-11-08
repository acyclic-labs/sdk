//! Completion from the native host's original turn terminal.
use super::{Outcome, Value};

/// Byte bound of a retained answer.
pub(super) const ANSWER_LIMIT: usize = 4096;
/// Byte bound of one native event line and of a retained answer's source.
pub(super) const EVENT_LIMIT: usize = 65_536;

/// A preview is not a completed reply. Native hosts can also report a failed
/// turn before exiting successfully; their original terminal owns the outcome.
#[derive(Default)]
pub(super) struct NativeAnswer {
    pub(super) pending: Option<String>,
    pub(super) outcome: Option<Outcome<String>>,
    pub(super) omitted: bool,
    pub(super) last_part: Option<String>,
    pub(super) truncated: bool,
}

impl NativeAnswer {
    /// Framing loss invalidates a preview, but is not a native terminal.
    /// A later complete reply can recover; a native refusal stays authoritative.
    pub(super) fn omit(&mut self) {
        self.pending = None;
        self.last_part = None;
        self.truncated = false;
        if matches!(self.outcome, Some(Outcome::Succeeded(_))) {
            self.outcome = None;
        }
        self.omitted = true;
    }

    pub(super) fn record(&mut self, event: &Value) {
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
                        .map(|text| Outcome::Succeeded(bounded([text])))
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
        match event.get("type").and_then(Value::as_str) {
            Some("turn.started" | "step_start") => *self = Self::default(),
            Some("item.completed")
                if event.pointer("/item/type").and_then(Value::as_str) == Some("agent_message")
                    && self.outcome.is_none() =>
            {
                self.pending = event
                    .pointer("/item/text")
                    .and_then(Value::as_str)
                    .map(|text| bounded([text]));
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
                    && !self.truncated =>
            {
                if let (Some(id), Some(text)) = (
                    event
                        .pointer("/part/id")
                        .and_then(Value::as_str)
                        .filter(|id| !id.is_empty()),
                    event.pointer("/part/text").and_then(Value::as_str),
                ) && self.last_part.as_deref() != Some(id)
                {
                    let source_len = self.pending.as_ref().map_or(0, String::len) + text.len();
                    let retained = bounded(self.pending.as_deref().into_iter().chain([text]));
                    self.truncated = retained.len() < source_len;
                    self.pending = Some(retained);
                    self.last_part = Some(id.into());
                }
            }
            Some("step_finish")
                if event.pointer("/part/reason").and_then(Value::as_str) == Some("stop")
                    && self.outcome.is_none() =>
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
                        .map(|text| Outcome::Succeeded(bounded([text])))
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
                            Outcome::Succeeded(bounded(
                                parts
                                    .iter()
                                    .filter_map(|part| part.get("text").and_then(Value::as_str)),
                            ))
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

pub(super) fn bounded<'a>(parts: impl IntoIterator<Item = &'a str>) -> String {
    parts
        .into_iter()
        .flat_map(str::chars)
        .scan(0, |bytes, ch| {
            *bytes += ch.len_utf8();
            (*bytes <= ANSWER_LIMIT).then_some(ch)
        })
        .collect()
}
