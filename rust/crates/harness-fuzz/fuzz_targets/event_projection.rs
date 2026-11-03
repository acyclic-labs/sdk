#![no_main]

use acyclic_harness_client::{AgentId, AgentPath, EventCursor, EventProjection, ExecutionPosition};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::Value;

fn check(bytes: &[u8]) {
    if let Ok(cursor) = serde_json::from_slice::<EventCursor>(bytes) {
        assert_eq!(
            serde_json::to_value(&cursor).unwrap(),
            serde_json::from_slice::<Value>(bytes).unwrap(),
            "cursor decoder dropped original metadata"
        );
        match (cursor.wire(), STANDARD.decode(&cursor.opaque)) {
            (Ok(wire), Ok(opaque)) => {
                assert_eq!(wire.next, cursor.next);
                assert_eq!(&wire.opaque[..], opaque.as_slice());
            }
            (Err(_), Err(_)) => {}
            _ => panic!("opaque cursor admission changed"),
        }
    }
    let Ok(mut state) = serde_json::from_slice::<EventProjection>(bytes) else {
        return;
    };
    let mut value: Value = serde_json::from_slice(bytes).unwrap();
    value
        .as_object_mut()
        .unwrap()
        .entry("execution")
        .or_insert(Value::Null);
    assert_eq!(
        serde_json::to_value(&state).unwrap(),
        value,
        "projection decoder dropped original metadata"
    );
    let resources = value["resources"].as_object().unwrap();
    let mut valid = value["version"] == 1 && value["operation"] == "oracle";
    for (resource, cursor) in resources {
        let cursor: EventCursor = serde_json::from_value(cursor.clone()).unwrap();
        valid &= cursor.wire().is_ok();
        assert_eq!(state.cursor(resource), cursor);
    }
    assert_eq!(
        state.validate("oracle").is_ok(),
        valid,
        "cursor admission must retain operation, version and opaque validity"
    );
    let names: Vec<_> = resources.keys().cloned().collect();
    assert!(state.validate_sources(&names).is_ok());
    assert_eq!(state.validate_sources(&[]).is_ok(), resources.is_empty());
    let selected = &value["execution"];
    let fallback = AgentPath::root(&AgentId::from_bytes([13; 16]));
    let (agent, previous) = if selected.is_null() {
        (fallback, ExecutionPosition::default())
    } else {
        let agent: AgentPath = serde_json::from_value(selected["agent"].clone()).unwrap();
        let position = serde_json::from_value(selected["position"].clone()).unwrap();
        assert_eq!(state.execution_position(&agent).unwrap(), position);
        let other = AgentPath::root(&AgentId::from_bytes(if agent == fallback {
            [14; 16]
        } else {
            [13; 16]
        }));
        assert!(state.execution_position(&other).is_err());
        assert!(state.accepted_execution(&other, position).is_err());
        assert_eq!(serde_json::to_value(&state).unwrap(), value);
        (agent, position)
    };
    let mut number = [0; 8];
    for (to, from) in number.iter_mut().zip(bytes) {
        *to = *from;
    }
    let next = ExecutionPosition {
        input: u64::from_le_bytes(number),
        after: bytes.len() as u64,
    };
    let expected = (next.input, next.after) >= (previous.input, previous.after);
    match state.accepted_execution(&agent, next) {
        Ok(changed) => {
            assert!(expected, "execution cursor moved backwards");
            assert_eq!(changed, selected.is_null() || next != previous);
            assert_eq!(state.execution_position(&agent).unwrap(), next);
            assert!(!state.accepted_execution(&agent, next).unwrap());
        }
        Err(_) => {
            assert!(!expected, "a forward execution cursor was refused");
            assert_eq!(serde_json::to_value(&state).unwrap(), value);
        }
    }
    // Decode the actual persisted writer, preserving every admitted field.
    let encoded = serde_json::to_vec(&state).unwrap();
    let restored: EventProjection = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(
        serde_json::to_value(&restored).unwrap(),
        serde_json::to_value(&state).unwrap()
    );
}

fn writer_corpus() -> Vec<(&'static str, Vec<u8>)> {
    let encode = |state: &EventProjection| serde_json::to_vec(state).unwrap();
    let mut state = EventProjection::new("oracle".into());
    let mut seeds = vec![
        ("empty-state", encode(&state)),
        (
            "wrong-operation",
            encode(&EventProjection::new("other".into())),
        ),
    ];
    let cursor = EventCursor {
        next: u64::MAX,
        opaque: STANDARD.encode([0, 1, 255]),
    };
    seeds.push(("opaque-cursor", serde_json::to_vec(&cursor).unwrap()));
    let agent = AgentPath::root(&AgentId::from_bytes([13; 16]));
    let resource = acyclic_stream::wire_codec::session::stream_resource(
        agent.conversation().unwrap().as_str(),
    );
    assert!(state.accepted(&resource, Some(cursor)));
    seeds.push(("resource-state", encode(&state)));
    state
        .accepted_execution(&agent, ExecutionPosition { input: 7, after: 3 })
        .unwrap();
    seeds.push(("execution-state", encode(&state)));
    state
        .accepted_execution(
            &agent,
            ExecutionPosition {
                input: u64::MAX,
                after: u64::MAX,
            },
        )
        .unwrap();
    seeds.push(("maximum-position", encode(&state)));
    // These are explicit corruptions of the original writer's bytes.
    let mut value = serde_json::to_value(&state).unwrap();
    value["version"] = Value::from(2);
    seeds.push(("wrong-version", serde_json::to_vec(&value).unwrap()));
    value["version"] = Value::from(1);
    value["resources"][&resource]["opaque"] = Value::from("!");
    seeds.push(("invalid-opaque", serde_json::to_vec(&value).unwrap()));
    seeds
}

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    static ORIGINALS: std::sync::Once = std::sync::Once::new();
    ORIGINALS.call_once(|| {
        let directory = std::env::var_os("ACYCLIC_FUZZ_CORPUS");
        for (name, original) in writer_corpus() {
            // Reject-everything is a defect too: the original writer decodes.
            if name == "opaque-cursor" {
                serde_json::from_slice::<EventCursor>(&original).unwrap();
            } else {
                serde_json::from_slice::<EventProjection>(&original).unwrap();
            }
            check(&original);
            if let Some(directory) = &directory {
                std::fs::write(std::path::Path::new(directory).join(name), &original).unwrap();
            }
        }
    });
    check(bytes);
});
