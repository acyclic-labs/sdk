#![forbid(unsafe_code)]
//! Protobuf byte boundary for the canonical inference contract.

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

/// Validate the exact generated message shape and caller-bound identity.
/// Byte arrays remain byte arrays across this boundary; no JSON or JS number
/// conversion is involved.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub fn validate_customer_wire(
    kind: &str,
    message: &[u8],
    expected: &[u8],
    related: &[u8],
) -> Result<(), JsValue> {
    acyclic_inference::validate_customer_wire(kind, message, expected, related)
        .map_err(|error| JsValue::from_str(error))
}

/// Decide from a validated Run view whether watching at this cursor is already complete.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub fn watch_run_start_wire(
    message: &[u8],
    expected: &[u8],
    from_sequence: &str,
) -> Result<bool, JsValue> {
    acyclic_inference::watch_run_start_wire(message, expected, from_sequence)
        .map_err(JsValue::from_str)
}
