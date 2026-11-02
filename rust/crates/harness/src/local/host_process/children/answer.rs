//! Native completion retaining the host's original tool and turn refusals.
use super::{Outcome, Value};

/// Byte bound of one native event line and of a retained answer's source.
pub(super) const EVENT_LIMIT: usize = 65_536;

/// Byte bound of a retained answer.
pub(super) const ANSWER_LIMIT: usize = 4096;

/// A lost frame and malformed JSON share the same omission boundary.
pub(super) fn decode(line: std::io::Result<String>) -> Option<(String, Value)> {
    line.ok().and_then(|line| {
        serde_json::from_str::<Value>(&line)
            .ok()
            .map(|event| (line, event))
    })
}

/// Last valid native resume identity, folded while publishing the original log.
/// The same bounded framer serves live replies and closed-log replay.
pub(crate) struct SessionId {
    frames: acyclic_native_runtime::lines::LineBuffer,
    pub(crate) id: Option<String>,
}

impl Default for SessionId {
    fn default() -> Self {
        Self {
            frames: acyclic_native_runtime::lines::LineBuffer::new(EVENT_LIMIT),
            id: None,
        }
    }
}

impl SessionId {
    pub(crate) fn push(&mut self, bytes: &[u8]) {
        for line in self.frames.push(bytes) {
            let Ok(line) = line else { continue };
            let Some((_, event)) = decode(Ok(line)) else {
                continue;
            };
            let id = event
                .get("thread_id")
                .or_else(|| event.get("session_id"))
                .or_else(|| {
                    (event.get("type").and_then(Value::as_str) == Some("session"))
                        .then(|| event.get("id"))
                        .flatten()
                })
                .and_then(Value::as_str)
                .filter(|id| {
                    !id.is_empty() && id.len() <= 256 && !id.chars().any(char::is_control)
                });
            if let Some(id) = id {
                // Most events repeat the same id. Retain its one allocation.
                if self.id.as_deref() != Some(id) {
                    self.id = Some(id.into());
                }
            }
        }
    }

    pub(crate) fn finish(&mut self) {
        // A crash can leave a complete JSON frame without its final delimiter.
        // This delimiter belongs only to parsing; publication keeps original bytes.
        self.push(b"\n");
    }
}

/// A preview is not a completed reply. Native hosts can also report a failed
/// tool or turn before exiting successfully; that refusal owns the outcome.
#[derive(Default)]
pub(super) struct NativeAnswer {
    pub(super) pending: Option<String>,
    pub(super) outcome: Option<Outcome<String>>,
    pub(super) omitted: bool,
}

impl NativeAnswer {
    fn refuse<'a>(&mut self, parts: impl IntoIterator<Item = &'a str>) {
        self.pending = None;
        self.outcome = Some(Outcome::Failed {
            message: bounded(parts),
        });
    }

    /// Framing loss invalidates a preview, but is not a native terminal.
    /// A later complete reply can recover; a native refusal stays authoritative.
    pub(super) fn omit(&mut self) {
        self.pending = None;
        if matches!(self.outcome, Some(Outcome::Succeeded(_))) {
            self.outcome = None;
        }
        self.omitted = true;
    }

    pub(super) fn record(&mut self, event: &Value) {
        let kind = event.get("type").and_then(Value::as_str);
        // A native refusal owns this attempt across every host format.
        // Only an original new-turn boundary can release that terminal.
        if kind != Some("turn.started")
            && matches!(
                self.outcome,
                Some(Outcome::Failed { .. } | Outcome::Cancelled | Outcome::Indeterminate { .. })
            )
        {
            return;
        }
        match kind {
            Some("turn.started") => *self = Self::default(),
            Some("item.completed")
                if matches!(
                    event.pointer("/item/type").and_then(Value::as_str),
                    Some("command_execution" | "mcp_tool_call" | "file_change")
                ) =>
            {
                let item = &event["item"];
                let status = item.get("status").and_then(Value::as_str);
                let error = item.get("error").filter(|error| !error.is_null());
                let command = item.get("type").and_then(Value::as_str) == Some("command_execution");
                if status != Some("completed")
                    || error.is_some()
                    || (command && item.get("exit_code").and_then(Value::as_i64) != Some(0))
                {
                    let exit = item
                        .get("exit_code")
                        .and_then(Value::as_i64)
                        .map(|code| code.to_string());
                    self.refuse([
                        "step=read native tool result; recovery=acyclic agents; native tool ",
                        item.get("id").and_then(Value::as_str).unwrap_or("unknown"),
                        ": status=",
                        status.unwrap_or("absent"),
                        ", exit_code=",
                        exit.as_deref().unwrap_or("absent"),
                        "; ",
                        error
                            .and_then(|error| {
                                error
                                    .as_str()
                                    .or_else(|| error.get("message").and_then(Value::as_str))
                            })
                            .or_else(|| item.get("aggregated_output").and_then(Value::as_str))
                            .unwrap_or("native tool did not complete successfully"),
                    ]);
                }
            }
            Some("user") => {
                if let Some(part) = event
                    .pointer("/message/content")
                    .and_then(Value::as_array)
                    .and_then(|parts| {
                        parts.iter().find(|part| {
                            part.get("type").and_then(Value::as_str) == Some("tool_result")
                                && part.get("is_error").and_then(Value::as_bool) == Some(true)
                        })
                    })
                {
                    self.refuse([
                        "step=read native tool result; recovery=acyclic agents; native tool ",
                        part.get("tool_use_id")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown"),
                        ": ",
                        part.get("content")
                            .and_then(Value::as_str)
                            .unwrap_or("native tool result reported an error; see original events"),
                    ]);
                }
            }
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
            Some("turn.failed" | "error") => {
                self.refuse([event
                    .pointer("/error/message")
                    .or_else(|| event.get("message"))
                    .and_then(Value::as_str)
                    .unwrap_or(
                        "step=read native reply: native model turn failed; recovery=acyclic agents",
                    )]);
            }
            Some("result") => {
                if event.get("subtype").and_then(Value::as_str) == Some("success")
                    && event.get("is_error").and_then(Value::as_bool) == Some(false)
                {
                    self.outcome = event
                        .get("result")
                        .and_then(Value::as_str)
                        .map(|text| Outcome::Succeeded(bounded([text])));
                } else {
                    self.refuse([event.get("result").and_then(Value::as_str).unwrap_or(
                        "step=read native reply: native model turn failed; recovery=acyclic agents",
                    )]);
                }
            }
            Some("message_end")
                if event.pointer("/message/role").and_then(Value::as_str) == Some("assistant") =>
            {
                match event.pointer("/message/stopReason").and_then(Value::as_str) {
                    Some("stop") => self.outcome = event
                        .pointer("/message/content")
                        .and_then(Value::as_array)
                        .map(|parts| {
                            Outcome::Succeeded(bounded(
                                parts
                                    .iter()
                                    .filter_map(|part| part.get("text").and_then(Value::as_str)),
                            ))
                        }),
                    Some("error") => self.refuse([event
                            .pointer("/message/errorMessage")
                            .and_then(Value::as_str)
                            .unwrap_or("step=read native reply: native assistant turn failed; recovery=acyclic agents")]),
                    Some("aborted") => self.outcome = Some(Outcome::Cancelled),
                    _ => self.outcome = None,
                }
            }
            _ => {}
        }
    }
}

fn bounded<'a>(parts: impl IntoIterator<Item = &'a str>) -> String {
    parts
        .into_iter()
        .flat_map(str::chars)
        .scan(0, |bytes, ch| {
            *bytes += ch.len_utf8();
            (*bytes <= ANSWER_LIMIT).then_some(ch)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recorded_resume_identity_survives_stream_boundaries() {
        let captured = include_str!("../fixtures/claude-code-completed.jsonl");
        // Derived representation control: JSON escapes preserve the original
        // member and value. This is not another captured provider response.
        let escaped = captured.replace("\"session_id\"", r#""sess\u0069on\u005fid""#);
        assert_eq!(
            serde_json::from_str::<Value>(captured).expect("original terminal"),
            serde_json::from_str::<Value>(&escaped).expect("equivalent terminal")
        );
        for wire in [captured, &escaped] {
            for chunk in [1, 17, 8192] {
                let mut session = SessionId::default();
                // A complete final JSON value need not have its newline on disk.
                for bytes in wire.as_bytes().trim_ascii_end().chunks(chunk) {
                    session.push(bytes);
                    assert!(session.frames.buffered_bytes() <= EVENT_LIMIT + 8192);
                }
                session.finish();
                assert_eq!(
                    session.id.as_deref(),
                    Some("a14f2590-4397-48a1-a076-1f2dac2d5349"),
                    "native resume decoding ignored an equivalent JSON member"
                );
            }
        }
    }

    #[test]
    fn recorded_native_tool_refusals_survive_successful_parent_terminals() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../../../../../../plugin/tests/hosts/fixtures/native-claude-workflow-refusal.json"
        ))
        .expect("recorded Claude workflow");
        let events = fixture["events"].as_array().expect("recorded events");
        let terminal = events.last().expect("original parent terminal");
        for event in &events[..events.len() - 1] {
            let part = &event["message"]["content"][0];
            let mut reply = NativeAnswer::default();
            reply.record(event);
            reply.record(terminal);
            let Some(Outcome::Failed { message }) = reply.outcome else {
                panic!("recorded tool refusal was masked by successful parent terminal");
            };
            assert!(message.contains(part["tool_use_id"].as_str().expect("original tool id")));
            assert!(message.contains(part["content"].as_str().expect("original refusal reason")));
        }
        let fixture: Value = serde_json::from_str(include_str!(
            "../../../../../../../plugin/tests/hosts/fixtures/native-codex-tool-terminals.json"
        ))
        .expect("recorded Codex tool terminals");
        let events = fixture["events"].as_array().expect("recorded events");
        let mut reply = NativeAnswer::default();
        for event in events {
            reply.record(event);
        }
        let Some(Outcome::Failed { message }) = reply.outcome else {
            panic!("recorded command failure was masked by successful parent terminal");
        };
        let item = events
            .iter()
            .map(|event| &event["item"])
            .find(|item| item["status"] == "failed")
            .expect("original failed command");
        assert!(message.contains(item["id"].as_str().expect("original command id")));
        assert!(message.contains(&item["exit_code"].to_string()));
    }

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
        // Bound across fragments without collecting the whole native message.
        // The non-ASCII boundary must not split a codepoint or inspect the tail.
        let prefix = "a".repeat(ANSWER_LIMIT - 1);
        let mut visits = 0;
        let text = bounded(
            [prefix.as_str(), "😀"]
                .into_iter()
                .chain(std::iter::repeat_n("unread", 100_000))
                .inspect(|_| visits += 1),
        );
        assert_eq!(text, prefix);
        assert_eq!(visits, 2);
    }
}
