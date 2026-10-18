use super::*;
use crate::executor::{ModelPurpose, ToolFailureKind};
use proptest::prelude::*;

fn accept(summary: &mut ExecutionSummary, event: ExecutionEvent) -> Result<()> {
    let sequence = summary.tail + 1;
    summary.accept(
        &ExecutionRecord {
            operation_id: OperationId::from_bytes([3; 16]),
            sequence,
            idempotency_key: format!("event-{sequence}"),
            event,
        },
        None,
        Limits::default(),
        None,
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn stopped_producer_settles_only_its_admitted_attempt_and_never_an_active_tool(
        step in any::<u32>(), summary in any::<bool>(), tool in any::<bool>(),
    ) {
        let purpose = if summary { ModelPurpose::Summary } else { ModelPurpose::Response };
        let stopped = ExecutionEvent::ModelStopped { step, purpose };
        let encoded = serde_json::to_vec(&stopped).unwrap();
        prop_assert_eq!(serde_json::from_slice::<ExecutionEvent>(&encoded).unwrap(), stopped.clone());
        let seed: serde_json::Value = serde_json::from_slice(include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/../harness-fuzz/corpus/child_control/child-agent-turn"))).unwrap();
        let request: FileRef = serde_json::from_value(seed["initial"]["model"]["payload"].clone()).unwrap();
        let mut state = ExecutionSummary::default();
        prop_assert!(state.require_next(&stopped, None).is_err());
        accept(&mut state, ExecutionEvent::Started { request_digest: [1;32] }).unwrap();
        accept(&mut state, ExecutionEvent::ModelStarted { step, purpose, request_digest: [2;32], request: request.clone() }).unwrap();
        prop_assert!(!state.quiescent(None).unwrap());
        let other = ExecutionEvent::ModelStopped { step: step ^ 1, purpose };
        prop_assert!(state.require_next(&other, None).is_err());
        if tool {
            accept(&mut state, ExecutionEvent::ToolStarted { step, call_id: "tool".into(), invocation: request }).unwrap();
        }
        prop_assert!(state.require_next(&stopped, None).is_ok());
        accept(&mut state, stopped.clone()).unwrap();
        prop_assert!(state.require_next(&stopped, None).is_err());
        prop_assert_eq!(state.quiescent(None).unwrap(), !tool);
        if tool {
            accept(&mut state, ExecutionEvent::ToolFailed { step, call_id: "tool".into(), reason: ToolFailureKind::ExecutorRejected }).unwrap();
            prop_assert!(state.quiescent(None).unwrap());
        }
    }
}
