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
    fn refusal_admission_requires_an_operation_and_an_unstarted_call(step in any::<u32>(), call in "[a-z]{1,32}") {
        for initialized in [false, true] {
            for started in [false, true] {
                for empty in [false, true] {
                    for reason in [ToolFailureKind::Unavailable, ToolFailureKind::InvalidInput,
                        ToolFailureKind::Refused, ToolFailureKind::ExecutorRejected,
                        ToolFailureKind::InvalidOutput, ToolFailureKind::ProjectionRejected,
                        ToolFailureKind::PublicationRejected] {
                        let call_id = if empty { "" } else { &call };
                        let mut state = ExecutionSummary { tail: u64::from(initialized), ..ExecutionSummary::default() };
                        if started { state.tools.insert((step, *blake3::hash(call_id.as_bytes()).as_bytes()), false); }
                        let failure = ExecutionEvent::ToolFailed {
                            step, call_id: call_id.into(), reason,
                            cause: crate::executor::ToolFailureCause {
                                class: crate::executor::ToolErrorClass::Failed, message: "declined".into(),
                            },
                        };
                        prop_assert_eq!(state.require_next(&failure, None).is_ok(),
                            initialized && !empty && (reason.precedes_dispatch() != started),
                            "{:?}, initialized={}, started={}, empty={}", reason, initialized, started, empty);
                    }
                }
            }
        }
    }

    // Each newly retained map entry is funded once, whether it starts execution or settles a
    // pre-admission refusal. Completion of a started call reuses its existing slot.
    #[test]
    fn refused_tools_fund_their_summary_entries(count in 1..24usize, step in any::<u32>()) {
        let io = PreparationIo::from_budget(
            acyclic_fs::performance::work_budget(1_000_000, 16_000_000),
            &acyclic_fs::CancellationToken::new(),
        ).unwrap();
        let mut state = ExecutionSummary::default();
        accept(&mut state, ExecutionEvent::Started { request_digest: [1; 32] }).unwrap();
        let before = io.owned_bytes().unwrap();
        for index in 0..count {
            let sequence = state.tail + 1;
            let record = ExecutionRecord {
                operation_id: OperationId::from_bytes([3; 16]), sequence,
                idempotency_key: format!("refusal-{index}"),
                event: ExecutionEvent::ToolFailed {
                    step, call_id: format!("call-{index}"), reason: ToolFailureKind::Unavailable,
                    cause: crate::executor::ToolFailureCause {
                        class: crate::executor::ToolErrorClass::NotFound, message: "unavailable".into(),
                    },
                },
            };
            state.accept(&record, None, Limits::default(), Some(&io)).unwrap();
        }
        let expected = count * (std::mem::size_of::<([u8; 32], (u64, [u8; 32]))>()
            + std::mem::size_of::<((u32, [u8; 32]), bool)>());
        prop_assert_eq!(io.owned_bytes().unwrap() - before, expected as u64);
        prop_assert_eq!(state.tools.len(), count);
    }

    #[test]
    fn stopped_producer_settles_only_its_admitted_attempt_and_never_an_active_tool(
        step in any::<u32>(), summary in any::<bool>(), tool in any::<bool>(),
    ) {
        let purpose = if summary { ModelPurpose::Summary } else { ModelPurpose::Response };
        let stopped = ExecutionEvent::ModelStopped { step, purpose };
        let encoded = serde_json::to_vec(&stopped).unwrap();
        prop_assert_eq!(serde_json::from_slice::<ExecutionEvent>(&encoded).unwrap(), stopped.clone());
        let seed: serde_json::Value = serde_json::from_slice(include_bytes!("../../../../../harness-fuzz/corpus/child_control/child-agent-turn")).unwrap();
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
            accept(&mut state, ExecutionEvent::ToolFailed { step, call_id: "tool".into(), reason: ToolFailureKind::ExecutorRejected, cause: crate::executor::ToolFailureCause { class: crate::executor::ToolErrorClass::Failed, message: "tool failed".into() } }).unwrap();
            prop_assert!(state.quiescent(None).unwrap());
        }
    }
}
