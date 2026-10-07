#![forbid(unsafe_code)]
//! Transport-independent customer messages and validation.

use zeroize::Zeroize;

/// Generated customer protobuf messages. This is the only wire type universe
/// shared by native inference and the WebAssembly boundary.
#[allow(missing_docs, unused_qualifications, clippy::all, clippy::pedantic)]
pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/inference.customer.v1.rs"));
}

/// The archived customer descriptor used by reflection-based adapters.
pub const DESCRIPTOR: &[u8] = include_bytes!("../inference_descriptor.bin");

impl Drop for wire::Item {
    fn drop(&mut self) {
        self.payload.zeroize();
    }
}

impl Drop for wire::Replace {
    fn drop(&mut self) {
        self.payload.zeroize();
    }
}

impl Drop for wire::RunEvent {
    fn drop(&mut self) {
        scrub_run_event(self);
    }
}

impl Drop for wire::RunResult {
    fn drop(&mut self) {
        scrub_run_result(self);
    }
}

/// Zeroize a run event's output bytes before releasing it.
pub fn scrub_run_event(event: &mut wire::RunEvent) {
    if let Some(wire::run_event::Event::Output(output)) = event.event.as_mut() {
        output.zeroize();
    }
}

/// Zeroize a run result's output bytes before releasing it.
pub fn scrub_run_result(result: &mut wire::RunResult) {
    result.output.zeroize();
}

#[allow(missing_docs)]
mod validation;

/// Backwards-compatible name for callers that used the contract crate's
/// previous validation error type.
pub type ValidationError = validation::Error;

pub use validation::{
    Error, MAXIMUM_EVALUATION_CANDIDATES, MAXIMUM_EVALUATION_CASES, MAXIMUM_EVALUATION_METRICS,
    MAXIMUM_EVALUATION_RESULTS, MAXIMUM_HTTP_JSON_BYTES, MAXIMUM_MESSAGE_BYTES, WatchRunState,
    evaluation_observation_binding, fixed, nonzero, validate_context_view, validate_customer_wire,
    validate_evaluation_admission, validate_evaluation_spec, validate_evaluation_view,
    validate_generated_run_view, validate_model_capabilities, validate_receipt,
    validate_renew_request, validate_retain_request, validate_run_view, validate_warm_view,
    watch_run_event, watch_run_start, watch_run_start_state_wire, watch_run_start_wire,
};
