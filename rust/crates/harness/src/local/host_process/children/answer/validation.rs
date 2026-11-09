//! Semantic properties of the deployed native reply reducer, without an alternate model.
use crate::Outcome;
use acyclic_native_runtime::lines::LineBuffer;
use serde_json::Value;

use super::{ANSWER_LIMIT, EVENT_LIMIT, NativeAnswer};

fn copy(answer: &NativeAnswer) -> NativeAnswer {
    NativeAnswer {
        pending: answer.pending.clone(),
        outcome: answer.outcome.clone(),
        omitted: answer.omitted,
        last_part: answer.last_part.clone(),
        truncated: answer.truncated,
    }
}

fn same(left: &NativeAnswer, right: &NativeAnswer) {
    assert_eq!(left.pending, right.pending);
    assert_eq!(left.outcome, right.outcome);
    assert_eq!(left.omitted, right.omitted);
    assert_eq!(left.last_part, right.last_part);
    assert_eq!(left.truncated, right.truncated);
}

fn prefix(original: &str, retained: &str) {
    assert!(original.starts_with(retained));
    assert!(retained.len() <= ANSWER_LIMIT);
    if let Some(next) = original
        .get(retained.len()..)
        .and_then(|tail| tail.chars().next())
    {
        assert!(retained.len() + next.len_utf8() > ANSWER_LIMIT);
    }
}

/// Original pipe bytes pass through the deployed framer and reducer. Vary read
/// boundaries, never add an EOF delimiter: incomplete live frames stay pending.
///
/// # Panics
/// Panics if event semantics, framing independence or loss invariants fail.
pub fn check(bytes: &[u8]) -> usize {
    let (fragmented, accepted) = fold(
        bytes,
        bytes.first().map_or(1, |byte| usize::from(*byte) + 1),
    );
    let (pages, page_count) = fold(bytes, 8192);
    same(&fragmented, &pages);
    assert_eq!(accepted, page_count);
    accepted
}

fn fold(bytes: &[u8], chunk_size: usize) -> (NativeAnswer, usize) {
    let mut frames = LineBuffer::new(EVENT_LIMIT);
    let mut current = NativeAnswer::default();
    let mut accepted = 0;
    for line in bytes
        .chunks(chunk_size)
        .flat_map(|chunk| frames.push(chunk))
    {
        let Ok(line) = line else {
            current.omit();
            continue;
        };
        let Ok(event) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        accepted += 1;
        let before = copy(&current);
        current.record(&event);
        let after = copy(&current);
        // Re-delivery of an event cannot turn a preview into a terminal or
        // consume the same terminal twice.
        current.record(&event);
        same(&current, &after);

        check_event(&event, &current, before);
        if let Some(Outcome::Succeeded(text)) = &current.outcome {
            assert!(text.len() <= ANSWER_LIMIT);
        }

        // Apply loss to a copy: later JSONL events still exercise the original
        // sequence. Loss revokes success/preview, retains failure/cancellation,
        // and is idempotent. It cannot fabricate a successful terminal.
        let mut lost = copy(&current);
        lost.omit();
        assert!(lost.pending.is_none());
        assert!(lost.omitted);
        match &current.outcome {
            Some(Outcome::Succeeded(_)) => assert!(lost.outcome.is_none()),
            other => assert_eq!(&lost.outcome, other),
        }
        let once = copy(&lost);
        lost.omit();
        same(&once, &lost);
    }
    (current, accepted)
}

/// Preserve the original unique cost control alongside the wider corpus property.
///
/// # Panics
/// Panics if bounded truncation reads beyond the retained prefix.
pub fn check_prefix_work() {
    let original = "a".repeat(ANSWER_LIMIT - 1);
    let mut visits = 0;
    let retained = super::bounded(
        [original.as_str(), "😀"]
            .into_iter()
            .chain(std::iter::repeat_n("unread", 100_000))
            .inspect(|_| visits += 1),
    );
    assert_eq!(retained, original);
    assert_eq!(visits, 2, "truncation visited the unread tail");
}

fn successful_prefix(text: &str, current: &NativeAnswer, message: &str) {
    assert!(
        matches!(&current.outcome, Some(Outcome::Succeeded(_))),
        "{message}"
    );
    if let Some(Outcome::Succeeded(retained)) = &current.outcome {
        prefix(text, retained);
    }
}

fn check_event(event: &Value, current: &NativeAnswer, before: NativeAnswer) {
    if event.get("event").and_then(Value::as_str) == Some("result") {
        check_agy_result(&event["result"], current);
        return;
    }
    match event.get("type").and_then(Value::as_str) {
        Some("turn.started" | "step_start") => same(current, &NativeAnswer::default()),
        Some("item.completed") => {
            assert_eq!(current.outcome, before.outcome, "preview completed a turn");
            if before.outcome.is_none()
                && event.pointer("/item/type").and_then(Value::as_str) == Some("agent_message")
            {
                match event.pointer("/item/text").and_then(Value::as_str) {
                    Some(text) => {
                        assert!(current.pending.is_some(), "lost preview");
                        if let Some(retained) = current.pending.as_deref() {
                            prefix(text, retained);
                        }
                    }
                    None => assert!(current.pending.is_none()),
                }
            }
        }
        Some("turn.completed") => {
            assert_eq!(
                current.outcome,
                before
                    .outcome
                    .or_else(|| before.pending.map(Outcome::Succeeded))
            );
        }
        Some("text") => check_text(event, current, &before),
        Some("step_finish") => {
            if before.omitted && before.outcome.is_none() {
                assert!(
                    !matches!(current.outcome, Some(Outcome::Succeeded(_))),
                    "incremental native reply succeeded after framing loss"
                );
            }
            if before.outcome.is_none()
                && !before.omitted
                && event.pointer("/part/reason").and_then(Value::as_str) == Some("stop")
            {
                assert_eq!(
                    current.outcome,
                    before.pending.map(Outcome::Succeeded),
                    "native OpenCode terminal lost"
                );
                assert!(current.pending.is_none());
            } else {
                same(current, &before);
            }
        }
        Some("turn.failed" | "error") => {
            assert!(matches!(current.outcome, Some(Outcome::Failed { .. })));
            assert!(current.pending.is_none());
            if let Some(message) = event
                .pointer("/error/message")
                .or_else(|| event.pointer("/error/data/message"))
                .or_else(|| event.get("message"))
                .and_then(Value::as_str)
            {
                assert_eq!(
                    current.outcome,
                    Some(Outcome::Failed {
                        message: message.into()
                    })
                );
            }
        }
        Some("result") => check_result(event, current),
        Some("message_end")
            if event.pointer("/message/role").and_then(Value::as_str) == Some("assistant") =>
        {
            check_message(event, current);
        }
        _ => same(current, &before),
    }
}

fn check_text(event: &Value, current: &NativeAnswer, before: &NativeAnswer) {
    if before.omitted {
        assert_eq!(
            current.pending, before.pending,
            "incremental native text retained after framing loss"
        );
    }
    assert_eq!(
        current.outcome, before.outcome,
        "text completed a native turn"
    );
    let id = event
        .pointer("/part/id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty());
    let text = event.pointer("/part/text").and_then(Value::as_str);
    if before.outcome.is_none()
        && !before.omitted
        && !before.truncated
        && event
            .pointer("/part/time/end")
            .and_then(Value::as_u64)
            .is_some_and(|end| end > 0)
        && let (Some(id), Some(text)) = (id, text)
        && before.last_part.as_deref() != Some(id)
    {
        let joined: String = before
            .pending
            .as_deref()
            .into_iter()
            .chain([text])
            .collect();
        assert!(current.pending.is_some(), "lost native text");
        if let Some(retained) = current.pending.as_deref() {
            prefix(&joined, retained);
            assert_eq!(current.truncated, joined.len() > retained.len());
        }
        assert_eq!(current.last_part.as_deref(), Some(id));
    } else {
        same(current, before);
    }
}

fn check_agy_result(result: &Value, current: &NativeAnswer) {
    assert!(current.pending.is_none());
    match result.get("status").and_then(Value::as_str) {
        Some("SUCCESS")
            if result.get("error").is_none() && result.get("finished_reason").is_none() =>
        {
            match result.get("response").and_then(Value::as_str) {
                Some(text) => successful_prefix(text, current, "successful native result lost"),
                None => assert!(current.outcome.is_none()),
            }
        }
        Some("CANCELED" | "INTERRUPTED") => assert_eq!(current.outcome, Some(Outcome::Cancelled)),
        _ => {
            assert!(
                matches!(current.outcome, Some(Outcome::Failed { .. })),
                "native refusal became success"
            );
            if let Some(message) = result.get("error").and_then(Value::as_str) {
                assert_eq!(
                    current.outcome,
                    Some(Outcome::Failed {
                        message: message.into()
                    })
                );
            }
        }
    }
}

fn check_result(event: &Value, current: &NativeAnswer) {
    if event.get("subtype").and_then(Value::as_str) == Some("success")
        && event.get("is_error").and_then(Value::as_bool) == Some(false)
    {
        match event.get("result").and_then(Value::as_str) {
            Some(text) => successful_prefix(text, current, "successful native result lost"),
            None => assert!(current.outcome.is_none()),
        }
    } else {
        assert!(matches!(current.outcome, Some(Outcome::Failed { .. })));
        if let Some(message) = event.get("result").and_then(Value::as_str) {
            assert_eq!(
                current.outcome,
                Some(Outcome::Failed {
                    message: message.into()
                })
            );
        }
    }
}

fn check_message(event: &Value, current: &NativeAnswer) {
    match event.pointer("/message/stopReason").and_then(Value::as_str) {
        Some("stop") => {
            if let Some(parts) = event.pointer("/message/content").and_then(Value::as_array) {
                let text: String = parts
                    .iter()
                    .filter_map(|part| part.get("text").and_then(Value::as_str))
                    .collect();
                successful_prefix(&text, current, "successful native message lost");
            } else {
                assert!(current.outcome.is_none());
            }
        }
        Some("error") => {
            assert!(matches!(current.outcome, Some(Outcome::Failed { .. })));
            if let Some(message) = event
                .pointer("/message/errorMessage")
                .and_then(Value::as_str)
            {
                assert_eq!(
                    current.outcome,
                    Some(Outcome::Failed {
                        message: message.into()
                    })
                );
            }
        }
        Some("aborted") => assert_eq!(current.outcome, Some(Outcome::Cancelled)),
        _ => assert!(current.outcome.is_none()),
    }
}
