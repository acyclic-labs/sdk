use super::*;
use crate::{AgentId, TaskId};
use proptest::prelude::*;

fn states() -> impl Strategy<Value = TaskState> {
    prop_oneof![
        Just(TaskState::Pending),
        Just(TaskState::Completed),
        Just(TaskState::Cancelled),
        Just(TaskState::Failed),
        Just(TaskState::Indeterminate),
        Just(TaskState::Deleted),
    ]
}

fn statuses() -> impl Strategy<Value = TaskStatus> {
    (
        any::<[u8; 16]>(),
        proptest::option::of(any::<[u8; 16]>()),
        states(),
        any::<bool>(),
        proptest::option::of(any::<[u8; 16]>()),
    )
        .prop_map(|(task, parent, state, attention, agent)| TaskStatus {
            task: TaskId::from_bytes(task),
            parent: parent.map(TaskId::from_bytes),
            state,
            attention,
            owner_agent: agent.map(AgentId::from_bytes),
        })
}

/// `encode` is the production encoder; the control variant drops `parent`.
fn round_trip<const LOSSY: bool>(status: TaskStatus) {
    let mut bytes = status.encode().unwrap();
    if LOSSY {
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        value.as_object_mut().unwrap().remove("parent");
        bytes = serde_json::to_vec(&value).unwrap();
    }
    assert_eq!(
        TaskStatus::parse(&bytes).ok(),
        Some(status),
        "status round trip"
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]
    #[test]
    fn every_status_round_trips_through_the_production_codec(status in statuses()) {
        round_trip::<false>(status);
    }

    /// Whatever untrusted bytes decode re-encode to the same status.
    #[test]
    fn decoded_bytes_re_encode_canonically(
        bytes in prop_oneof![
            any::<Vec<u8>>(),
            statuses().prop_map(|status| status.encode().unwrap()),
            (statuses(), any::<usize>(), any::<u8>()).prop_map(|(status, at, byte)| {
                let mut bytes = status.encode().unwrap();
                let at = at % bytes.len();
                bytes[at] = byte;
                bytes
            }),
        ]
    ) {
        if let Ok(status) = TaskStatus::parse(&bytes) {
            round_trip::<false>(status);
        }
    }
}

#[test]
#[should_panic(expected = "status round trip")]
fn a_lossy_encoder_is_detected_negative_control() {
    round_trip::<true>(TaskStatus {
        task: TaskId::from_bytes([1; 16]),
        parent: Some(TaskId::from_bytes([2; 16])),
        state: TaskState::Completed,
        attention: true,
        owner_agent: None,
    });
}

#[test]
fn unknown_states_fields_and_shapes_are_rejected() {
    let valid = TaskStatus {
        task: TaskId::from_bytes([1; 16]),
        parent: None,
        state: TaskState::Pending,
        attention: false,
        owner_agent: None,
    };
    let mut value: serde_json::Value = serde_json::from_slice(&valid.encode().unwrap()).unwrap();
    assert_eq!(
        TaskStatus::parse(&serde_json::to_vec(&value).unwrap()).unwrap(),
        valid
    );
    for (field, bad) in [
        ("state", serde_json::json!("running")),
        ("task", serde_json::json!("not-a-task")),
        ("attention", serde_json::json!("yes")),
        ("extra", serde_json::json!(1)),
    ] {
        let mut changed = value.clone();
        changed[field] = bad;
        assert!(
            TaskStatus::parse(&serde_json::to_vec(&changed).unwrap()).is_err(),
            "{field}"
        );
    }
    value.as_object_mut().unwrap().remove("state");
    assert!(TaskStatus::parse(&serde_json::to_vec(&value).unwrap()).is_err());
    assert!(TaskStatus::parse(b"[]").is_err());
}
