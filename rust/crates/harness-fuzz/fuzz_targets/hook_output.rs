#![no_main]

use acyclic_harness::{Error, ProcessError, Result};
use libfuzzer_sys::fuzz_target;
use serde_json::Value;

// Include the production pure decoder and normalized types. No native runner,
// model provider, process, or public testing API is needed by this target.
#[path = "../../harness/src/hooks/spec.rs"]
mod spec;
use spec::{HookEvent, HookFailure};
#[path = "../../harness/src/hooks/output.rs"]
mod output;
use output::{HookDiagnostic, HookOutput};
#[path = "../../harness/src/hooks/budget.rs"]
mod budget;
#[path = "../../harness/src/hooks/plan.rs"]
mod plan;

fuzz_target!(|bytes: &[u8]| {
    if bytes.len() > 4096 {
        return;
    }
    if let Ok(output) = serde_json::from_slice::<HookOutput>(bytes) {
        let encoded = serde_json::to_vec(&output).unwrap();
        let decoded: HookOutput = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(output, decoded);
    }
    for event in [
        HookEvent::SessionStart,
        HookEvent::PreToolUse,
        HookEvent::PostToolUse,
        HookEvent::Stop,
    ] {
        let mut reports = HookOutput::default();
        reports.apply(
            event,
            HookFailure::Report,
            Ok((Some(0), bytes.to_vec(), vec![])),
        );
        let mut merged = HookOutput {
            approval: Some("earlier approval".into()),
            ..HookOutput::default()
        };
        merged.apply(
            event,
            HookFailure::Report,
            Ok((Some(0), bytes.to_vec(), vec![])),
        );
        assert_eq!(merged.approval.as_deref(), Some("earlier approval"));
        assert_eq!(merged.blocked, reports.blocked);
        let mut blocks = HookOutput::default();
        blocks.apply(
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
            round_trip.apply(event, HookFailure::Report, Ok((Some(0), canonical, vec![])));
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
    // Exercise the production ledger with arbitrary output followed by a guard.
    // This must stay bounded and cannot abandon a deny after exhaustion.
    let mut ledger = budget::Budget::new(4096).unwrap();
    let mut outcome = HookOutput::default();
    ledger.apply(
        &mut outcome,
        HookEvent::SessionStart,
        HookFailure::Report,
        Ok((Some(0), bytes.to_vec(), vec![])),
    );
    ledger.apply(
        &mut outcome,
        HookEvent::SessionStart,
        HookFailure::Report,
        Ok((Some(0), br#"{"continue":false}"#.to_vec(), vec![])),
    );
    ledger.finish(&mut outcome);
    assert!(outcome.blocks());
    assert!(serde_json::to_vec(&outcome).unwrap().len() <= 4096);
    if let Ok(rule) = serde_json::from_slice::<spec::HookRule>(bytes) {
        let encoded = serde_json::to_vec(&rule).unwrap();
        let decoded: spec::HookRule = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(rule, decoded);
        for (original, restored) in rule.hooks.iter().zip(&decoded.hooks) {
            assert_eq!(original.validate().is_ok(), restored.validate().is_ok());
        }
    }
});
