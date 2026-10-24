#![no_main]
#[cfg(test)]
acyclic_test_support::counting_allocator!();

#[cfg(test)]
use acyclic_harness::OperationId;
use acyclic_harness::{Error, ProcessError, Result};
use acyclic_harness::{TaskId, tool};
use libfuzzer_sys::fuzz_target;
use serde_json::Value;

// Include the production pure decoder and normalized types. No native runner,
// model provider, process, or public testing API is needed by this target.
#[path = "../../harness-core/src/hooks/spec.rs"]
mod spec;
use spec::{HookEvent, HookFailure};
#[path = "../../harness-core/src/hooks/output.rs"]
mod output;
use output::{HookDiagnostic, HookOutput};
#[path = "../../harness-core/src/hooks/budget.rs"]
mod budget;
#[path = "../../harness-core/src/hooks/plan.rs"]
mod plan;

// This target is also compiled by ordinary Cargo qualification without cfg(test)
// or cfg(fuzzing). Exercise only the same unconditional ledger entry points as
// production; do not depend on included modules' test-only convenience methods.
fn apply(
    output: &mut HookOutput,
    event: HookEvent,
    policy: HookFailure,
    result: Result<(Option<i32>, Vec<u8>, Vec<u8>)>,
) {
    let mut ledger = budget::Budget::existing(65_536, output).unwrap();
    ledger.apply(output, event, policy, result);
    ledger.finish(output);
}
#[path = "../../harness-core/src/hooks/input.rs"]
mod input;
use input::HookInput;

fuzz_target!(|bytes: &[u8]| {
    if bytes.len() > 4096 {
        return;
    }
    if let Ok(fields) = serde_json::from_slice::<Value>(bytes) {
        let invocation = tool::ToolInvocation {
            operation_id: acyclic_harness::OperationId::from_bytes([1; 16]),
            call_id: "fuzz-call".into(),
            name: "fuzz-tool".into(),
            arguments: fields.clone(),
        };
        let response = tool::ToolResult {
            value: fields.clone(),
        };
        for post in [false, true] {
            let event = if post {
                HookEvent::PostToolUse
            } else {
                HookEvent::PreToolUse
            };
            let input = HookInput::Tool {
                invocation: &invocation,
                response: post.then_some(&response),
                session_id: None,
            };
            let encoded = input
                .encode(event, std::path::Path::new("workspace"))
                .unwrap();
            let decoded: Value = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(&decoded["tool_input"], &fields);
            assert_eq!(decoded.get("tool_response"), post.then_some(&fields));
            assert_eq!(decoded["hook_event_name"], event.name());
            assert_eq!(decoded["cwd"], "workspace");
        }
        let encoded = HookInput::Fields(&fields)
            .encode(HookEvent::PreToolUse, std::path::Path::new("workspace"));
        if fields.is_object() {
            let decoded: Value = serde_json::from_slice(&encoded.unwrap()).unwrap();
            assert_eq!(decoded["cwd"], "workspace");
            assert_eq!(decoded["hook_event_name"], "PreToolUse");
            for (key, value) in fields.as_object().unwrap() {
                if !matches!(key.as_str(), "cwd" | "hook_event_name") {
                    assert_eq!(&decoded[key], value);
                }
            }
        } else {
            assert!(encoded.is_err());
        }
    }
    if let Ok(output) = serde_json::from_slice::<HookOutput>(bytes) {
        let encoded = serde_json::to_vec(&output).unwrap();
        let decoded: HookOutput = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(output, decoded);
    }
    for event in HookEvent::ALL {
        // Each event is fired by exactly one site, and a single-event site names it.
        assert_eq!(
            HookEvent::at(event.site())
                .filter(|fired| *fired == event)
                .count(),
            1
        );
        if let Ok(fired) = HookEvent::only(event.site()) {
            assert_eq!(fired, event);
        }
        let mut reports = HookOutput::default();
        apply(
            &mut reports,
            event,
            HookFailure::Report,
            Ok((Some(0), bytes.to_vec(), vec![])),
        );
        let mut merged = HookOutput {
            approval: Some("earlier approval".into()),
            ..HookOutput::default()
        };
        apply(
            &mut merged,
            event,
            HookFailure::Report,
            Ok((Some(0), bytes.to_vec(), vec![])),
        );
        assert_eq!(merged.approval.as_deref(), Some("earlier approval"));
        assert_eq!(merged.blocked, reports.blocked);
        let mut blocks = HookOutput::default();
        apply(
            &mut blocks,
            event,
            HookFailure::Block,
            Ok((Some(0), bytes.to_vec(), vec![])),
        );
        assert_eq!(reports.diagnostics.len(), blocks.diagnostics.len());
        for (report, block) in reports.diagnostics.iter().zip(&blocks.diagnostics) {
            assert_eq!(report.cause, block.cause);
            assert_eq!(block.blocking, event != HookEvent::Stop);
        }
        if reports.diagnostics.is_empty() || event == HookEvent::Stop {
            assert_eq!(reports, blocks);
        } else {
            assert!(blocks.blocks());
            assert!(reports.blocked.is_none());
            assert!(reports.additional_context.is_empty());
            assert!(reports.updated_input.is_none());
            assert!(reports.approval.is_none());
        }
        if let Ok(value) = serde_json::from_slice::<Value>(bytes) {
            let canonical = serde_json::to_vec(&value).unwrap();
            let mut round_trip = HookOutput::default();
            apply(
                &mut round_trip,
                event,
                HookFailure::Report,
                Ok((Some(0), canonical, vec![])),
            );
            // Syntax diagnostics contain byte positions; compare semantics
            // only when the structured input was accepted.
            if reports.diagnostics.is_empty() {
                if event == HookEvent::PreToolUse
                    && value
                        .pointer("/hookSpecificOutput/permissionDecision")
                        .and_then(Value::as_str)
                        == Some("ask")
                    && let Some(reason) = value
                        .pointer("/hookSpecificOutput/permissionDecisionReason")
                        .and_then(Value::as_str)
                {
                    assert_eq!(reports.approval.as_deref(), Some(reason));
                }
                assert_eq!(reports, round_trip);
            }
        }
    }
    // All event-specific deltas, including replacement arguments, must obey
    // the retained file bound and keep folding a following explicit denial.
    for event in HookEvent::ALL {
        // Exercise the production ledger with arbitrary output followed by a guard.
        // This must stay bounded and cannot abandon a deny after exhaustion.
        let mut ledger = budget::Budget::new(4096).unwrap();
        let mut outcome = HookOutput::default();
        ledger.apply(
            &mut outcome,
            event,
            HookFailure::Report,
            Ok((Some(0), bytes.to_vec(), vec![])),
        );
        ledger.apply(
            &mut outcome,
            event,
            HookFailure::Report,
            Ok((Some(0), br#"{"continue":false}"#.to_vec(), vec![])),
        );
        ledger.finish(&mut outcome);
        assert!(outcome.blocks());
        assert!(serde_json::to_vec(&outcome).unwrap().len() <= 4096);
    }
    if let Ok(rule) = serde_json::from_slice::<spec::HookRule>(bytes) {
        let encoded = serde_json::to_vec(&rule).unwrap();
        let decoded: spec::HookRule = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(rule, decoded);
        for (original, restored) in rule.hooks.iter().zip(&decoded.hooks) {
            assert_eq!(original.validate().is_ok(), restored.validate().is_ok());
        }
    }
});
