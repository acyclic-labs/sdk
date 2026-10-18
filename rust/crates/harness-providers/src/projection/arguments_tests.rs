// Argument normalization and projection through the shared oracle: fixtures
// record observations, the shared judge's invariants plus projection ones
// rule, and a targeted fault proves each invariant load bearing.
use super::*;
use crate::projection::arguments::{
    ADAPTERS, CODEC, Codec, ENVELOPE, INVARIANTS, Seen, call, decoded, encode, normalization, slot,
};
use acyclic_harness::model::{ModelContentPart, ToolCall, ToolResultContent};
use acyclic_test_support::differential::oracle::{Observer, Oracle, Stop};
use proptest::prelude::*;

#[derive(Debug)]
enum Case {
    /// Direct and per-adapter call normalization.
    Normalized(Value),
    /// Normalization plus a full request projection in every protocol.
    Projected(Value),
}

/// Normalization observations from the shared judge, plus projection ones.
#[derive(Debug)]
enum Event {
    Normalization(Seen),
    /// Whether the request validator admitted the projected body.
    Admitted {
        adapter: Adapter,
        admitted: bool,
    },
    /// Whether the wire call borrowed the canonical arguments.
    Borrowed {
        adapter: Adapter,
        same: bool,
    },
    /// Whether projection left the canonical history unchanged.
    History {
        adapter: Adapter,
        unchanged: bool,
    },
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Fault {
    /// Serializes non-object arguments without the `_raw` wrapper.
    Unwrapped,
    /// Wraps every value, objects and already wrapped ones included.
    AlwaysWrap,
    /// Wraps non-object arguments twice.
    DoubleWrap,
    /// Encodes null arguments as an empty object.
    DropNull,
    /// Decodes without the generated envelope levels.
    NoEnvelope,
    /// Decodes with one generated envelope level.
    OneEnvelope,
    /// The adapter's call serializes its arguments without the `_raw` wrapper.
    CallUnwrapped(Adapter),
    /// The adapter's projected body carries the unwrapped arguments.
    UnwrapWire(Adapter),
    /// The adapter's wire call references a copy of the arguments.
    Materialize(Adapter),
    /// Projecting for the adapter rewrites the canonical history with
    /// normalized arguments.
    NormalizeHistory(Adapter),
}

impl Fault {
    fn codec(fault: Option<Self>) -> Codec {
        Codec {
            envelope: match fault {
                Some(Self::NoEnvelope) => 0,
                Some(Self::OneEnvelope) => 1,
                _ => ENVELOPE,
            },
            encode: Self::encode(fault),
            call: match fault {
                Some(Self::CallUnwrapped(adapter)) => match adapter {
                    Adapter::Anthropic => unwrapped::<0>,
                    Adapter::Responses => unwrapped::<1>,
                    Adapter::Chat => unwrapped::<2>,
                    Adapter::Gemini => unwrapped::<3>,
                },
                _ => CODEC.call,
            },
        }
    }

    fn encode(fault: Option<Self>) -> fn(&Value) -> Vec<u8> {
        match fault {
            Some(Self::Unwrapped) => |value| serde_json::to_vec(value).unwrap_or_default(),
            Some(Self::AlwaysWrap) => {
                |value| serde_json::to_vec(&json!({"_raw": value})).unwrap_or_default()
            }
            Some(Self::DoubleWrap) => |value| match value {
                Value::Object(_) => encode(value),
                _ => serde_json::to_vec(&json!({"_raw": {"_raw": value}})).unwrap_or_default(),
            },
            Some(Self::DropNull) => |value| match value {
                Value::Null => b"{}".to_vec(),
                _ => encode(value),
            },
            _ => CODEC.encode,
        }
    }
}

/// Arguments as an adapter would carry them without the `_raw` wrapper.
fn bare(adapter: Adapter, value: &Value) -> Value {
    match adapter {
        Adapter::Anthropic | Adapter::Gemini => value.clone(),
        Adapter::Responses | Adapter::Chat => Value::String(value.to_string()),
    }
}

/// `ADAPTERS[N]`'s call serialization with its arguments unwrapped; every
/// other adapter's is production.
fn unwrapped<const N: usize>(adapter: Adapter, value: &Value) -> Vec<u8> {
    let bytes = call(adapter, value);
    if adapter != ADAPTERS[N] {
        return bytes;
    }
    let Ok(mut wire) = crate::projection::arguments::literal(&bytes) else {
        return bytes;
    };
    *slot(adapter, &mut wire) = bare(adapter, value);
    serde_json::to_vec(&wire).unwrap_or_default()
}

/// Where the projected body for `adapter` carries the fixture's arguments.
fn projected_slot(adapter: Adapter, wire: &mut Value) -> &mut Value {
    match adapter {
        Adapter::Anthropic => &mut wire["messages"][1]["content"][0]["input"],
        Adapter::Gemini => &mut wire["contents"][1]["parts"][0]["functionCall"]["args"],
        Adapter::Chat => &mut wire["messages"][2]["tool_calls"][0]["function"]["arguments"],
        Adapter::Responses => &mut wire["input"][2]["arguments"],
    }
}

fn values() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(|value| json!(value)),
        prop::collection::vec(any::<char>(), 0..32)
            .prop_map(|chars| Value::String(chars.into_iter().collect()))
    ]
    .prop_recursive(4, 32, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            prop::collection::btree_map(".{0,32}", inner, 0..4)
                .prop_map(|values| Value::Object(values.into_iter().collect()))
        ]
    })
}

fn nested(depth: usize) -> Value {
    (0..depth).fold(json!(0), |inner, _| Value::Array(vec![inner]))
}

/// The shared judge's invariant `N`, applied to normalization events.
fn lift<const N: usize>(event: &Event) -> std::result::Result<(), String> {
    match event {
        Event::Normalization(seen) => (INVARIANTS[N].1)(seen),
        _ => Ok(()),
    }
}

fn oracle() -> Oracle<Case, Fault, Event> {
    let mut oracle = Oracle::declare(run)
        .invariant(INVARIANTS[0].0, lift::<0>)
        .invariant(INVARIANTS[1].0, lift::<1>)
        .invariant(INVARIANTS[2].0, lift::<2>)
        .invariant(INVARIANTS[3].0, lift::<3>)
        .invariant(INVARIANTS[4].0, lift::<4>)
        .invariant("admitted", |event| match event {
            Event::Admitted {
                adapter,
                admitted: false,
            } => Err(format!("{adapter:?}: rejected projected arguments")),
            _ => Ok(()),
        })
        .invariant("borrowed", |event| match event {
            Event::Borrowed {
                adapter,
                same: false,
            } => Err(format!(
                "{adapter:?}: wire calls must borrow canonical arguments"
            )),
            _ => Ok(()),
        })
        .invariant("history", |event| match event {
            Event::History {
                adapter,
                unchanged: false,
            } => Err(format!(
                "{adapter:?}: projection changed the canonical history"
            )),
            _ => Ok(()),
        })
        .control("object", Case::Normalized(json!([1])), Fault::Unwrapped)
        .control(
            "identity",
            Case::Normalized(json!({"a": 1})),
            Fault::AlwaysWrap,
        )
        .control("raw", Case::Normalized(json!([1])), Fault::DoubleWrap)
        .control("raw", Case::Normalized(Value::Null), Fault::DropNull)
        // The input bound alone cannot decode the wrapped boundary value, nor
        // can one envelope level decode its Gemini call.
        .control("decodes", Case::Normalized(nested(128)), Fault::NoEnvelope)
        .control("decodes", Case::Normalized(nested(128)), Fault::OneEnvelope)
        .control(
            "idempotent",
            Case::Normalized(json!([1])),
            Fault::AlwaysWrap,
        );
    // Each adapter serializes and projects separately, so each carries its
    // own controls: dropping one adapter's observations fails its controls.
    for adapter in ADAPTERS {
        let carried = match adapter {
            Adapter::Anthropic | Adapter::Gemini => "object",
            Adapter::Responses | Adapter::Chat => "admitted",
        };
        oracle = oracle
            .control(
                "object",
                Case::Normalized(json!([1])),
                Fault::CallUnwrapped(adapter),
            )
            .control(
                carried,
                Case::Projected(json!([1])),
                Fault::UnwrapWire(adapter),
            )
            .control(
                "borrowed",
                Case::Projected(json!({"nested": [1, 2, 3]})),
                Fault::Materialize(adapter),
            )
            .control(
                "history",
                Case::Projected(json!([1])),
                Fault::NormalizeHistory(adapter),
            );
    }
    oracle.build()
}

fn run(
    case: &Case,
    fault: Option<&Fault>,
    observer: &mut Observer<'_, Event>,
) -> std::result::Result<(), Stop> {
    let fault = fault.copied();
    let (Case::Normalized(value) | Case::Projected(value)) = case;
    normalization(value, &Fault::codec(fault), &mut |seen| {
        observer.see(Event::Normalization(seen))
    })?;
    match case {
        Case::Normalized(_) => Ok(()),
        Case::Projected(value) => projection(value, fault, observer),
    }
}

fn projection(
    value: &Value,
    fault: Option<Fault>,
    observer: &mut Observer<'_, Event>,
) -> std::result::Result<(), Stop> {
    let call = || ToolCall::new("call_0".into(), "echo".into(), value.clone());
    for adapter in ADAPTERS {
        let (provider, _) = provider(adapter, 200);
        let (prepared, _) = request(&provider).unwrap();
        let mut request = prepared.request().clone();
        request.messages.extend([
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Part(ModelContentPart::ToolCall(call())),
            },
            ModelMessage {
                role: ModelRole::Tool,
                content: ModelContent::Part(ModelContentPart::ToolResult {
                    call_id: "call_0".into(),
                    name: "echo".into(),
                    content: ToolResultContent::Json {
                        value: json!("done"),
                    },
                }),
            },
        ]);
        let ModelContent::Part(ModelContentPart::ToolCall(source)) = &request.messages[2].content
        else {
            unreachable!("the fixture appended a tool call")
        };
        let copy = source.arguments.clone();
        let names = crate::tool_names::Names::new(&request);
        let origin = provider.reasoning_origin(provider.key.as_deref(), None);
        let projected = crate::projection::body_named(
            &provider,
            &request,
            &names,
            &origin,
            &mut crate::files::FileText::default(),
        );
        let Ok(mut projected) = projected else {
            observer.see(Event::Admitted {
                adapter,
                admitted: false,
            })?;
            return Err(Stop);
        };
        if fault == Some(Fault::Materialize(adapter)) {
            for arguments in projected.arguments() {
                *arguments = &copy;
            }
        }
        let borrowed: Vec<&Value> = projected
            .arguments()
            .into_iter()
            .map(|arguments| *arguments)
            .collect();
        observer.see(Event::Borrowed {
            adapter,
            same: borrowed.len() == 1 && std::ptr::eq(&source.arguments, borrowed[0]),
        })?;
        let mut see = |seen| observer.see(Event::Normalization(seen));
        let bytes = serde_json::to_vec(&projected).unwrap_or_default();
        let site = format!("{adapter:?} projection");
        let Some(mut wire) = decoded(&mut see, &site, &bytes, 0)? else {
            return Ok(());
        };
        if fault == Some(Fault::UnwrapWire(adapter)) {
            *projected_slot(adapter, &mut wire) = bare(adapter, value);
        }
        if matches!(adapter, Adapter::Chat | Adapter::Responses) {
            observer.see(Event::Admitted {
                adapter,
                admitted: crate::validate_input(adapter, &wire).is_empty(),
            })?;
        }
        let arguments = projected_slot(adapter, &mut wire).take();
        let arguments = match adapter {
            Adapter::Anthropic | Adapter::Gemini => arguments,
            Adapter::Chat | Adapter::Responses => {
                let raw = arguments.as_str().unwrap_or_default().as_bytes();
                let mut see = |seen| observer.see(Event::Normalization(seen));
                let Some(arguments) = decoded(&mut see, &site, raw, 0)? else {
                    return Ok(());
                };
                arguments
            }
        };
        drop(projected);
        if fault == Some(Fault::NormalizeHistory(adapter)) {
            request.messages[2].content = ModelContent::Part(ModelContentPart::ToolCall(
                ToolCall::new("call_0".into(), "echo".into(), arguments.clone()),
            ));
        }
        observer.see(Event::Normalization(Seen::Normalized {
            site,
            value: value.clone(),
            normalized: arguments,
        }))?;
        observer.see(Event::History {
            adapter,
            unchanged: request.messages[2].content.parts()[0] == ModelContentPart::ToolCall(call()),
        })?;
    }
    Ok(())
}

#[test]
fn arbitrary_history_arguments_are_lossless_idempotent_objects_in_every_protocol() {
    let oracle = oracle();
    proptest!(ProptestConfig::with_cases(64), |(value in values())| {
        crate::fuzzing::assert_model_json_round_trip(&value);
        oracle.check(&Case::Projected(value));
    });
}

#[test]
fn raw_marker_names_and_non_object_fixtures_are_preserved() {
    let oracle = oracle();
    for value in [
        json!(null),
        json!([1]),
        json!({"_raw":[1]}),
        json!(["line\n", 0, false]),
        json!("{}"),
        json!({"_raw":null}),
        json!({"$serde_json::private::RawValue":"null","nested":[]}),
    ] {
        crate::fuzzing::assert_model_json_round_trip(&value);
        oracle.check(&Case::Projected(value));
    }
}

#[test]
fn wire_wrapping_collision_leaves_distinct_canonical_arguments_unchanged() {
    // Wire envelopes have no inverse: replay always reads the canonical transcript.
    let array = json!([1]);
    let object = json!({"_raw":[1]});
    assert_eq!(encode(&array), encode(&object));
    assert_ne!(array, object);
    let oracle = oracle();
    oracle.check(&Case::Normalized(array));
    oracle.check(&Case::Normalized(object));
}

#[test]
fn negative_control_default_value_decode_misinterprets_literal_marker_keys() {
    let original = json!({"$serde_json::private::RawValue":"null","nested":[]});
    let bytes = serde_json::to_vec(&original).unwrap();
    assert!(serde_json::from_slice::<Value>(&bytes).is_err());
    assert!(serde_json::from_value::<Value>(original.clone()).is_err());
    assert_eq!(
        crate::projection::arguments::literal(&bytes).unwrap(),
        original
    );
}

#[test]
fn object_argument_validation_checks_literals_without_marker_interpretation() {
    for raw in [
        r#"{"$serde_json::private::RawValue":"null","nested":[]}"#,
        r#" {"text":"quote\" newline\n slash\\"} "#,
    ] {
        assert!(crate::protocol::strict::object(raw));
    }
    for raw in [
        "null",
        "[]",
        r#"{"text":"\ud800"}"#,
        "{} trailing",
        r#"{"x":}"#,
    ] {
        assert!(!crate::protocol::strict::object(raw), "accepted {raw}");
    }
}

#[test]
fn generated_envelopes_preserve_every_admitted_argument_depth() {
    let oracle = oracle();
    proptest!(ProptestConfig::with_cases(32), |(depth in 0usize..=128)| {
        let value = nested(depth);
        crate::fuzzing::assert_model_json_round_trip(&value);
        oracle.check(&Case::Normalized(value));
    });
}

#[test]
fn checked_in_boundary_depth_runs_the_full_codec_fuzz_oracle() {
    let seed = include_bytes!("../../fuzz/corpus/provider_stream/arguments-depth-boundary.json");
    assert_eq!(seed.len(), 258);
    assert_eq!(seed.iter().filter(|byte| **byte == b'[').count(), 128);
    crate::projection::arguments::literal(&seed[1..]).unwrap();
    crate::fuzzing::assert_stream_invariants(seed);
    let too_deep = format!("{}0{}", "[".repeat(129), "]".repeat(129));
    assert!(crate::projection::arguments::literal(too_deep.as_bytes()).is_err());
}

#[test]
fn literal_model_payloads_reject_depth_beyond_the_existing_record_bound() {
    let arguments = (0..129).fold(json!(0), |inner, _| json!([inner]));
    let event = acyclic_harness::model::ModelEvent::ToolCall(ToolCall::new(
        "call".into(),
        "echo".into(),
        arguments,
    ));
    let value = serde_json::to_value(&event).unwrap();
    assert!(serde_json::from_value::<acyclic_harness::model::ModelEvent>(value).is_err());
}
