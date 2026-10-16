//! Every declared shape is exercised in every declared content position.
use super::*;
use crate::protocol::{
    content,
    shape::{self, Constraint, Shape},
};

#[derive(Clone, Copy, Debug)]
enum Position {
    Chat(&'static str),
    Responses(&'static str),
    Output,
    ChatCall,
    ResponsesCall,
    Native(Adapter),
}

fn entries() -> Vec<(Position, &'static Shape)> {
    let mut entries = Vec::new();
    for (roles, position) in [
        (
            content::CHAT,
            Position::Chat as fn(&'static str) -> Position,
        ),
        (
            content::RESPONSES,
            Position::Responses as fn(&'static str) -> Position,
        ),
    ] {
        for (role, content) in roles {
            entries.extend(content.parts.iter().map(|shape| (position(role), *shape)));
        }
    }
    entries.extend(
        content::RESPONSE_TOOL_OUTPUT
            .parts
            .iter()
            .map(|shape| (Position::Output, *shape)),
    );
    entries.extend([
        (Position::ChatCall, &shape::CHAT_CALL),
        (Position::ResponsesCall, &shape::RESPONSE_CALL),
        (Position::Native(Adapter::Anthropic), &shape::THINKING),
        (Position::Native(Adapter::Anthropic), &shape::REDACTED),
        (Position::Native(Adapter::Responses), &shape::REASONING),
    ]);
    entries
}

fn insert(value: &mut Value, pointer: &str, field: Value) {
    let mut path = pointer.trim_start_matches('/').split('/').peekable();
    let mut node = value;
    while let Some(key) = path.next() {
        if path.peek().is_none() {
            node.as_object_mut().unwrap().insert(key.into(), field);
            return;
        }
        node = node
            .as_object_mut()
            .unwrap()
            .entry(key)
            .or_insert_with(|| json!({}));
    }
}

fn example(shape: &Shape, text: &str) -> Value {
    let mut value = json!({shape.discriminator.0: shape.discriminator.1});
    for (path, constraint) in shape.fields {
        let field = match constraint {
            Constraint::String { nonempty } => json!(if *nonempty {
                format!("x{text}")
            } else {
                text.into()
            }),
            Constraint::Name(_) => json!("call_1"),
            Constraint::ObjectJson => json!("{}"),
            Constraint::Array => {
                json!([{"type":"summary_text","text":text,"nested":[{"key":text}]}])
            }
        };
        insert(&mut value, path, field);
    }
    value
}

fn admitted(position: Position, part: &Value) -> bool {
    let (adapter, body) = match position {
        Position::Chat("tool") => {
            let mut body = fixtures()[2].1.clone();
            body["messages"][2]["content"] = json!([part]);
            (Adapter::Chat, body)
        }
        Position::Chat(role) => (
            Adapter::Chat,
            json!({"messages":[{"role":role,"content":[part]}]}),
        ),
        Position::Responses(role) => (
            Adapter::Responses,
            json!({"input":[{"type":"message","role":role,"content":[part]}]}),
        ),
        Position::Output => {
            let mut body = fixtures()[1].1.clone();
            body["input"][2]["output"] = json!([part]);
            (Adapter::Responses, body)
        }
        Position::ChatCall => {
            let mut body = fixtures()[2].1.clone();
            body["messages"][1]["tool_calls"] = json!([part]);
            (Adapter::Chat, body)
        }
        Position::ResponsesCall => {
            let mut body = fixtures()[1].1.clone();
            body["input"][1] = part.clone();
            (Adapter::Responses, body)
        }
        Position::Native(adapter) => {
            return crate::projection::native_reasoning::validated(
                adapter,
                &serde_json::to_vec(part).unwrap(),
            )
            .is_some();
        }
    };
    validate_input(adapter, &body).is_empty()
}

#[derive(Clone, Copy, PartialEq)]
enum Control {
    None,
    ShapeFieldsOmitted,
    ValidatorMutationIgnored,
    ShapeAcceptsNumbers,
}

fn assert_entry(position: Position, shape: &Shape, text: &str, control: Control) {
    let value = example(shape, text);
    assert!(shape.accepts(&value), "bad shape fixture: {value}");
    assert!(
        admitted(position, &value),
        "bad position fixture: {position:?} {value}"
    );
    let replay: Value = serde_json::from_slice(&serde_json::to_vec(&value).unwrap()).unwrap();
    assert!(admitted(position, &replay));
    let rule = Shape {
        discriminator: shape.discriminator,
        fields: if control == Control::ShapeFieldsOmitted {
            &[]
        } else {
            shape.fields
        },
    };
    for (path, constraint) in std::iter::once((format!("/{}", shape.discriminator.0), None)).chain(
        shape
            .fields
            .iter()
            .map(|(path, constraint)| ((*path).into(), Some(constraint))),
    ) {
        let mut missing = value.clone();
        let (parent, key) = path.rsplit_once('/').unwrap();
        missing
            .pointer_mut(parent)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove(key);
        assert!(
            !rule.accepts(&missing),
            "shape accepted missing {path}: {position:?}"
        );
        // Independent control: a validator which ignores the mutation reads the
        // valid original. The shape checker still rejects the actual mutation.
        let input = if control == Control::ValidatorMutationIgnored {
            &value
        } else {
            &missing
        };
        assert!(
            !admitted(position, input),
            "validator accepted missing {path}: {position:?}"
        );
        for wrong in [json!(null), json!(true), json!(42), json!({}), json!([])] {
            // Expected types come from the declaration, never from acceptance.
            // [] is the only correctly typed replacement in this mutation set.
            if matches!(constraint, Some(Constraint::Array)) && wrong.is_array() {
                continue;
            }
            let mut broken = value.clone();
            *broken.pointer_mut(&path).unwrap() = wrong;
            let accepts_number = control == Control::ShapeAcceptsNumbers
                && matches!(constraint, Some(Constraint::String { .. }))
                && broken.pointer(&path).unwrap().is_number();
            assert!(
                !rule.accepts(&broken) && !accepts_number,
                "shape accepted wrong type {path}: {position:?}"
            );
            let input = if control == Control::ValidatorMutationIgnored {
                &value
            } else {
                &broken
            };
            assert!(
                !admitted(position, input),
                "validator accepted wrong type {path}: {position:?}"
            );
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn every_shape_rejects_each_missing_or_wrongly_typed_required_field(
        chars in prop::collection::vec(any::<char>(), 0..32),
    ) {
        let text: String = chars.into_iter().collect();
        for (position, shape) in entries() {
            assert_entry(position, shape, &text, Control::None);
        }
    }
}

#[test]
fn omitting_shape_requirements_fails_independently_of_the_validator() {
    for (position, shape) in entries() {
        assert!(
            std::panic::catch_unwind(|| assert_entry(
                position,
                shape,
                "line\nquoted\"",
                Control::ShapeFieldsOmitted
            ))
            .is_err()
        );
    }
}

#[test]
fn ignoring_validator_mutations_fails_independently_of_the_shape_checker() {
    for (position, shape) in entries() {
        assert!(
            std::panic::catch_unwind(|| assert_entry(
                position,
                shape,
                "line\nquoted\"",
                Control::ValidatorMutationIgnored
            ))
            .is_err()
        );
    }
}

#[test]
fn accepting_numbers_as_strings_fails_the_same_mutation_oracle() {
    assert!(
        std::panic::catch_unwind(|| assert_entry(
            Position::Chat("user"),
            &shape::IMAGE_URL,
            "image",
            Control::ShapeAcceptsNumbers,
        ))
        .is_err()
    );
}
