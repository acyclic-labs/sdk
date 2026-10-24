use super::{State, finish, parse};
use crate::Adapter;
use acyclic_harness::model::ModelEvent;
use serde_json::{Value, json};

fn done(position: u64, parts: Value) -> Value {
    let item = serde_json::Map::from_iter([
        ("type".into(), "message".into()),
        ("role".into(), "assistant".into()),
        ("content".into(), parts),
    ]);
    json!({"type":"response.output_item.done", "output_index":position, "item":item})
}

fn message(text: &str) -> Value {
    json!([{"type":"output_text", "text":text}])
}

fn completed(values: impl IntoIterator<Item = Value>) -> crate::Result<Vec<ModelEvent>> {
    let mut state = State::new(4096);
    let mut events = Vec::new();
    for value in values {
        events.extend(parse(&mut state, Adapter::Responses, &value.to_string())?);
    }
    state.terminal = true;
    events.extend(finish(
        state,
        Adapter::Responses,
        acyclic_harness::model::ModelDispatch {
            operation_id: acyclic_harness::OperationId::new(),
            step: 0,
            request_digest: [0; 32],
        },
    )?);
    Ok(events)
}

fn answer(events: &[ModelEvent]) -> crate::Result<&str> {
    let answers: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            ModelEvent::MessageCompleted { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(answers.len(), 1);
    answers
        .first()
        .copied()
        .ok_or_else(|| crate::Error::Invalid("missing completed message".into()))
}

#[test]
fn last_output_position_wins_independently_of_delivery_order() -> crate::Result<()> {
    for reverse in [false, true] {
        for final_text in ["", "  final λ\nanswer\n"] {
            let mut values = vec![
                done(0, message("older")),
                done(9007199254740991, message(final_text)),
            ];
            if reverse {
                values.reverse();
            }
            assert_eq!(answer(&completed(values)?)?, final_text);
        }
    }
    Ok(())
}

#[test]
fn terminal_output_is_authoritative_over_item_notifications() -> crate::Result<()> {
    for text in ["", "terminal"] {
        let events = completed([
            done(999, message("stale item")),
            json!({"type":"response.completed", "response":{"status":"completed", "output":[
                {"type":"message", "role":"assistant", "content":message("earlier")},
                {"type":"message", "role":"assistant", "content":message(text)}
            ]}}),
        ])?;
        assert_eq!(answer(&events)?, text);
    }
    assert_eq!(
        answer(&completed([
            done(0, message("stale")),
            json!({"type":"response.completed", "response":{"output":[]}}),
        ])?)?,
        ""
    );
    Ok(())
}

#[test]
fn content_parts_and_refusal_preserve_exact_text() -> crate::Result<()> {
    assert_eq!(
        answer(&completed([done(
            0,
            json!([
                {"type":"output_text", "text":" λ "},
                {"type":"refusal", "refusal":"refused\n"},
                {"type":"output_text", "text":"end"}
            ])
        )])?)?,
        " λ refused\nend"
    );
    Ok(())
}

#[test]
fn malformed_or_unfunded_message_is_rejected() {
    for value in [
        done(9007199254740992, message("x")),
        done(0, json!([{"type":"output_text", "text":null}])),
        done(0, json!([{"type":"image", "text":"x"}])),
        done(0, message(&"x".repeat(65))),
    ] {
        let mut state = State::new(64);
        assert!(parse(&mut state, Adapter::Responses, &value.to_string()).is_err());
    }
}

#[test]
fn delta_only_responses_keep_the_streaming_fallback() -> crate::Result<()> {
    let events = completed([json!({"type":"response.output_text.delta", "delta":"fallback"})])?;
    assert!(
        events
            .iter()
            .any(|event| matches!(event, ModelEvent::Content { delta } if delta == "fallback"))
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, ModelEvent::MessageCompleted { .. }))
    );
    Ok(())
}
