//! Rust-owned Inference scenarios for the SDK examples bundle.
//!
//! The scenario validates an encoded Run view and consumes a finite, ordered
//! event stream through the public Inference contract helpers. It proves local
//! wire validation only; it does not connect to a model or claim a hosted
//! endpoint.

use acyclic_inference::{validate_customer_wire, watch_run_start_state_wire, wire};
use prost::Message;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// Stable source-bound scenario identity.
pub const SCENARIO_ID: &str = "inference-run-watch-roundtrip";
/// Fully-qualified RPC identity from the Inference descriptor.
pub const OPERATION_ID: &str = "inference.customer.v1.RunsService/Watch";
/// Descriptor-derived route relative to `/v1/inference/`.
pub const ROUTE: &str = "runs/watch";
/// Rust source path recorded in generated receipts.
pub const SOURCE: &str = "rust/crates/sdk-examples/src/inference_scenarios.rs";
const RUN_ID: [u8; 16] = [2; 16];
const INPUT_DIGEST: [u8; 32] = [3; 32];
const MODEL: &str = "model.example.v1";

/// One canonical Run view and its finite event sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InferenceFixture {
    /// Encoded `RunView` admission state.
    pub view: Vec<u8>,
    /// Encoded events consumed in sequence order.
    pub events: Vec<Vec<u8>>,
    /// Stable semantic expectation for a local receipt.
    pub expected: Value,
}

/// Receipt emitted after local wire validation and event-state execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InferenceReceipt {
    /// Stable scenario identity.
    pub scenario_id: &'static str,
    /// Local validation result.
    pub status: &'static str,
    /// Scope of evidence; no remote transport is implied.
    pub scope: &'static str,
    /// SHA-256 of the exact encoded fixture bytes.
    pub fixture_sha256: Vec<u8>,
    /// Number of events accepted by the bounded watcher.
    pub event_count: usize,
    /// Whether the terminal event was observed.
    pub terminal: bool,
}

fn view() -> wire::RunView {
    wire::RunView {
        run_id: RUN_ID.to_vec(),
        input: INPUT_DIGEST.to_vec(),
        model: MODEL.to_owned(),
        last_sequence: 0,
        cancellation_requested: false,
        result: None,
    }
}

fn events() -> Vec<wire::RunEvent> {
    vec![
        wire::RunEvent {
            sequence: 0,
            event: Some(wire::run_event::Event::Progress(wire::RunProgress {
                kind: "queued".to_owned(),
            })),
        },
        wire::RunEvent {
            sequence: 1,
            event: Some(wire::run_event::Event::Terminal(
                wire::RunTerminal::Completed.into(),
            )),
        },
    ]
}

/// Builds the canonical encoded fixture from generated Rust wire types.
#[must_use]
pub fn fixture() -> InferenceFixture {
    let view = view().encode_to_vec();
    let events = events()
        .into_iter()
        .map(|event| event.encode_to_vec())
        .collect::<Vec<_>>();
    InferenceFixture {
        view,
        events,
        expected: json!({
            "kind": "local-wire-validation",
            "accepted": true,
            "message": "inference.customer.v1.RunView",
            "route": ROUTE,
            "event_count": 2,
            "terminal": true,
        }),
    }
}

fn fixture_bytes(fixture: &InferenceFixture) -> Vec<u8> {
    fixture
        .view
        .iter()
        .chain(fixture.events.iter().flatten())
        .copied()
        .collect()
}

/// Executes the bounded Run validation and ordered terminal-event scenario.
pub fn execute() -> Result<InferenceReceipt, Box<dyn std::error::Error + Send + Sync>> {
    let fixture = fixture();
    validate_customer_wire("run_view", &fixture.view, &RUN_ID, &[])?;
    let mut state = watch_run_start_state_wire(&fixture.view, &RUN_ID, "0")?;
    for bytes in &fixture.events {
        validate_customer_wire("run_event", bytes, &[], &[])?;
        state.advance_wire(bytes)?;
    }
    state.finish()?;
    let bytes = fixture_bytes(&fixture);
    Ok(InferenceReceipt {
        scenario_id: SCENARIO_ID,
        status: "qualified",
        scope: "rust-wire-validation",
        fixture_sha256: Sha256::digest(bytes).to_vec(),
        event_count: fixture.events.len(),
        terminal: state.is_terminal(),
    })
}

/// Rust snippet rendered into the source-bound examples bundle.
#[must_use]
pub fn rust_snippet() -> &'static str {
    r#"// capability: supported; scope: rust-wire-validation
use acyclic_inference::{WatchRunState, validate_customer_wire, watch_run_start_state_wire, wire};
use prost::Message;

let view = wire::RunView { run_id: vec![2; 16], input: vec![3; 32], model: "model.example.v1".into(), last_sequence: 0, ..Default::default() };
validate_customer_wire("run_view", &view.encode_to_vec(), &[2; 16], &[])?;
let mut state: WatchRunState = watch_run_start_state_wire(&view.encode_to_vec(), &[2; 16], "0")?;
for (sequence, event) in [(0, wire::run_event::Event::Progress(wire::RunProgress { kind: "queued".into() })), (1, wire::run_event::Event::Terminal(wire::RunTerminal::Completed.into()))] {
    let event = wire::RunEvent { sequence, event: Some(event) };
    validate_customer_wire("run_event", &event.encode_to_vec(), &[], &[])?;
    state.advance_wire(&event.encode_to_vec())?;
}
state.finish()?;"#
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_is_canonical_and_executes() {
        let fixture = fixture();
        let view = wire::RunView::decode(fixture.view.as_slice())
            .expect("Run view fixture must be canonical protobuf");
        assert_eq!(view.run_id, RUN_ID.to_vec());
        assert_eq!(fixture.events.len(), 2);
        for event in &fixture.events {
            wire::RunEvent::decode(event.as_slice())
                .expect("Run event fixture must be canonical protobuf");
        }
        let receipt = execute().expect("Inference watch scenario");
        assert_eq!(receipt.scenario_id, SCENARIO_ID);
        assert_eq!(receipt.status, "qualified");
        assert_eq!(receipt.scope, "rust-wire-validation");
        assert_eq!(receipt.event_count, 2);
        assert!(receipt.terminal);
        assert_eq!(fixture.expected["accepted"], true);
        assert!(!rust_snippet().contains("TODO"));
        assert!(!rust_snippet().contains("{{receipt"));
    }

    #[test]
    fn out_of_order_events_are_rejected() {
        let fixture = fixture();
        let mut state = watch_run_start_state_wire(&fixture.view, &RUN_ID, "0")
            .expect("valid Run view fixture");
        let event = wire::RunEvent {
            sequence: 1,
            event: Some(wire::run_event::Event::Progress(wire::RunProgress {
                kind: "wrong-order".into(),
            })),
        };
        assert_eq!(
            state.advance_wire(&event.encode_to_vec()),
            Err("run event order or shape differs")
        );
    }
}
