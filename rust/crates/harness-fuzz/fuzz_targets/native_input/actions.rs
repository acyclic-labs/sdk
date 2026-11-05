//! Original native request unions; no owner, process or session is executed.
use acyclic_harness::{
    IndeterminateCause, OperationId, Outcome,
    agent::{AgentId, AgentPath, IntentId},
    executor::TurnOutput,
    local::host_owner::{Control, HostAction, SessionAction, TurnClaim},
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

pub(super) fn admission<T: Serialize + DeserializeOwned>(
    original: &T,
    label: &str,
    optional: &[&str],
) {
    let encoded = serde_json::to_value(original).unwrap();
    let decoded: T = serde_json::from_value(encoded.clone()).unwrap();
    assert_eq!(
        serde_json::to_value(decoded).unwrap(),
        encoded,
        "{label} must retain the original writer's fields"
    );
    let fields = encoded.as_object().unwrap();
    let mut unknown = fields.clone();
    unknown.insert("unrecognized_action_field".into(), Value::Null);
    assert!(
        serde_json::from_value::<T>(Value::Object(unknown)).is_err(),
        "{label} unknown fields must be refused"
    );
    for field in fields
        .keys()
        .filter(|field| !optional.contains(&field.as_str()))
    {
        let mut partial = fields.clone();
        partial.remove(field);
        assert!(
            serde_json::from_value::<T>(Value::Object(partial)).is_err(),
            "{label} required field {field} must be retained"
        );
    }
    let mut foreign = fields.clone();
    foreign.insert("kind".into(), json!("unrecognized_action"));
    assert!(
        serde_json::from_value::<T>(Value::Object(foreign)).is_err(),
        "{label} unknown kind must be refused"
    );
}

fn action(action: &HostAction) {
    // This measurement variant is absent from the shipping native binary.
    #[cfg(feature = "runtime")]
    if matches!(action, HostAction::ResourceProbe { .. }) {
        return;
    }
    admission(action, "native action", &["next"]);
    if let HostAction::Session { action, .. } = action {
        admission(action, "native session action", &[]);
    }
}

pub(super) fn check(bytes: &[u8]) {
    if let Ok(value) = serde_json::from_slice::<HostAction>(bytes) {
        action(&value);
    }
    if let Ok(value) = serde_json::from_slice::<SessionAction>(bytes) {
        admission(&value, "native session action", &[]);
    }
    let text = String::from_utf8_lossy(bytes).into_owned();
    // Caller-authored strings and values are not external host output fixtures.
    action(&HostAction::Hook {
        host: text.clone(),
        event: text.clone(),
        input: json!(text),
    });
    admission(
        &SessionAction::Send { prompt: text },
        "native session action",
        &[],
    );

    static WRITERS: std::sync::Once = std::sync::Once::new();
    WRITERS.call_once(|| {
        let task = AgentPath::root(&AgentId::from_bytes([13; 16]));
        let operation = OperationId::from_bytes([19; 16]);
        let completion = HostAction::Session {
            parent: task.clone(),
            reference: "near-bound-completion".into(),
            next: false,
            action: SessionAction::Complete {
                claim: TurnClaim {
                    agent: task.clone(),
                    operation,
                    turn: 1,
                },
                outcome: Outcome::Succeeded(TurnOutput::new("x".repeat(65_336), Value::Null, 1)),
            },
        };
        let encoded = serde_json::to_vec(&completion).unwrap();
        assert!(
            encoded.len() > 65_536,
            "workspace envelope crosses the hook frame bound"
        );
        let admitted = super::read_pipe(&encoded, None)
            .expect("workspace envelope must retain its complete JSON");
        assert_eq!(admitted, serde_json::to_value(&completion).unwrap());
        let mut sessions = vec![
            SessionAction::Spawn {
                prompt: "caller input".into(),
            },
            SessionAction::Send {
                prompt: "caller input".into(),
            },
            SessionAction::Next {},
            SessionAction::Observe {},
            SessionAction::Reply {
                id: IntentId::try_from("native:caller").unwrap(),
                inbox: u64::MAX,
            },
            SessionAction::Recover {},
        ];
        for control in [
            Control::Resume,
            Control::Interrupt,
            Control::Cancel,
            Control::Archive,
        ] {
            sessions.push(SessionAction::Control { control });
        }
        let output: TurnOutput = serde_json::from_value(json!({
            "text": "caller output", "metadata": {"caller": true}, "steps": u32::MAX,
        }))
        .unwrap();
        for outcome in [
            Outcome::Succeeded(output),
            Outcome::Failed {
                message: "caller failure".into(),
            },
            Outcome::Cancelled,
            Outcome::Indeterminate {
                operation_id: operation,
                cause: IndeterminateCause::Unobserved,
            },
        ] {
            sessions.push(SessionAction::Complete {
                claim: TurnClaim {
                    agent: task.clone(),
                    operation,
                    turn: u64::MAX,
                },
                outcome,
            });
        }
        let mut actions = vec![
            HostAction::Sessions {
                parent: task.clone(),
            },
            HostAction::Hook {
                host: "caller".into(),
                event: "caller".into(),
                input: json!({"opaque": null}),
            },
            HostAction::Locate {
                cwd: "caller workspace".into(),
            },
            HostAction::Open {
                host: "caller".into(),
                session: "caller session".into(),
            },
            HostAction::Reopen { task: task.clone() },
            HostAction::Capture { task: task.clone() },
            HostAction::Fs {
                task: task.clone(),
                command: "caller command".into(),
            },
            HostAction::Agents { task: task.clone() },
        ];
        for session in sessions {
            for next in [false, true] {
                actions.push(HostAction::Session {
                    parent: task.clone(),
                    reference: "caller child".into(),
                    action: session.clone(),
                    next,
                });
            }
        }
        actions.push(completion);
        let capture = std::env::var_os("ACYCLIC_FUZZ_CORPUS").map(std::path::PathBuf::from);
        for (index, original) in actions.into_iter().enumerate() {
            action(&original);
            if let HostAction::Session { next: false, .. } = &original {
                let mut omitted = serde_json::to_value(&original).unwrap();
                omitted.as_object_mut().unwrap().remove("next");
                let decoded: HostAction = serde_json::from_value(omitted).unwrap();
                assert!(
                    matches!(decoded, HostAction::Session { next: false, .. }),
                    "omitted next must remain false"
                );
            }
            if let Some(directory) = &capture {
                std::fs::write(
                    directory.join(format!("action-{index}")),
                    serde_json::to_vec(&original).unwrap(),
                )
                .unwrap();
            }
        }
    });
}
