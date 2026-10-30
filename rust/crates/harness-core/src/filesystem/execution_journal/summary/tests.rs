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
    fn tool_failure_settlement_preserves_admission_and_dispatch_custody(step in any::<u32>(), call in "[a-z]{1,32}") {
        for initialized in [false, true] {
            for started in [false, true] {
                for empty in [false, true] {
                    for reason in [ToolFailureKind::Unavailable, ToolFailureKind::InvalidInput,
                        ToolFailureKind::ExecutorRejected,
                        ToolFailureKind::InvalidOutput, ToolFailureKind::ProjectionRejected,
                        ToolFailureKind::PublicationRejected] {
                        let call_id = if empty { "" } else { &call };
                        let mut state = ExecutionSummary { tail: u64::from(initialized), ..ExecutionSummary::default() };
                        if started {
                            state.tools.insert((step, *blake3::hash(call_id.as_bytes()).as_bytes()), false, None)
                                .map_err(|error| TestCaseError::fail(error.to_string()))?;
                            state.pending_dispatches = 1;
                        }
                        let failure = ExecutionEvent::ToolFailed {
                            step, call_id: call_id.into(), reason,
                            cause: crate::executor::ToolFailureCause {
                                class: crate::executor::ToolErrorClass::Failed, message: "declined".into(),
                            },
                        };
                        let valid = initialized && !empty && (reason == ToolFailureKind::ExecutorRejected || reason.precedes_dispatch() != started);
                        prop_assert_eq!(state.require_next(&failure, None).is_ok(), valid,
                            "{:?}, initialized={}, started={}, empty={}", reason, initialized, started, empty);
                        if valid {
                            accept(&mut state, failure.clone()).map_err(|error| TestCaseError::fail(error.to_string()))?;
                            prop_assert!(state.quiescent(None).map_err(|error| TestCaseError::fail(error.to_string()))?);
                            prop_assert!(state.require_next(&failure, None).is_err(), "a settled failure cannot retire dispatch twice");
                        }
                    }
                }
            }
        }
    }

    // Compare retained physical storage with ledger changes, including radix branches
    // and spare vector capacity rather than only the logical key/value sizes.
    #[test]
    fn refused_tools_fund_their_summary_entries(count in 1..24usize, step in any::<u32>()) {
        let io = PreparationIo::from_budget(
            acyclic_fs::performance::work_budget(1_000_000, 16_000_000),
            &acyclic_fs::CancellationToken::new(),
        ).unwrap();
        let mut state = ExecutionSummary::default();
        accept(&mut state, ExecutionEvent::Started { request_digest: [1; 32] }).unwrap();
        let before = io.owned_bytes().unwrap();
        let mut retained = 0i64;
        for index in 0..count {
            let sequence = state.tail + 1;
            let record = ExecutionRecord {
                operation_id: OperationId::from_bytes([3; 16]), sequence,
                idempotency_key: format!("refusal-{index}"),
                event: ExecutionEvent::ToolFailed {
                    step, call_id: format!("call-{index}"),
                    reason: if index % 2 == 0 { ToolFailureKind::Unavailable } else { ToolFailureKind::ExecutorRejected },
                    cause: crate::executor::ToolFailureCause {
                        class: crate::executor::ToolErrorClass::NotFound, message: "unavailable".into(),
                    },
                },
            };
            let mut result = None;
            let allocation = acyclic_test_support::measure(|| {
                result = Some(state.accept(&record, None, Limits::default(), Some(&io)));
            });
            result.ok_or_else(|| TestCaseError::fail("summary acceptance did not run"))?
                .map_err(|error| TestCaseError::fail(error.to_string()))?;
            retained += allocation.bytes_current;
        }
        let retained = u64::try_from(retained)
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        prop_assert_eq!(io.owned_bytes().unwrap() - before, retained);
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

#[cfg(not(target_arch = "wasm32"))]
#[path = "turn_cost.rs"]
pub(in crate::filesystem::execution_journal) mod turn_cost;

#[cfg(not(target_arch = "wasm32"))]
#[path = "recovery_cost.rs"]
mod recovery_cost;

#[cfg(not(target_arch = "wasm32"))]
#[path = "dispatch_admission.rs"]
mod dispatch_admission;

#[cfg(not(target_arch = "wasm32"))]
#[path = "cache_eviction.rs"]
mod cache_eviction;
