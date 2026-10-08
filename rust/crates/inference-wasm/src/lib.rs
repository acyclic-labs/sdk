#![forbid(unsafe_code)]
//! Protobuf byte boundary for the canonical inference contract.

mod schema;

/// Return the current Rust-owned Run terminal metadata for code generators.
pub fn run_terminal_metadata_native() -> Result<String, &'static str> {
    schema::terminal_metadata()
}

/// Return canonical protobuf fixed-byte field metadata for code generators.
pub fn fixed_width_metadata_native() -> Result<String, &'static str> {
    schema::fixed_width_metadata()
}

/// Return Rust-owned client width names and their descriptor-derived values.
pub fn client_widths_native() -> Result<String, &'static str> {
    schema::client_widths()
}

use wasm_bindgen::prelude::*;

/// Builds every error thrown across the JavaScript boundary: an `Error` whose
/// string `code` names a stable failure category, `invalid` for a contract
/// violation or `unavailable` when the embedded descriptor cannot load.
fn js_error(code: &str, message: &str) -> JsValue {
    let error = js_sys::Error::new(message);
    // Defining a data property on a fresh, unfrozen `Error` cannot fail.
    let _ = js_sys::Reflect::set(&error, &JsValue::from_str("code"), &JsValue::from_str(code));
    error.into()
}

fn invalid(message: &str) -> JsValue {
    js_error("invalid", message)
}

/// Opaque Rust-owned state for an ordered Run watch.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub struct WatchRunState {
    inner: acyclic_inference::WatchRunState,
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
impl WatchRunState {
    /// Whether a terminal event has already been consumed.
    #[wasm_bindgen(getter)]
    pub fn terminal(&self) -> bool {
        self.inner.is_terminal()
    }

    /// Validate one protobuf Run event and advance this state in place.
    #[wasm_bindgen]
    pub fn advance(&mut self, event: &[u8]) -> Result<(), JsValue> {
        self.inner.advance_wire(event).map_err(invalid)
    }

    /// Confirm that the stream ended after a terminal event.
    #[wasm_bindgen]
    pub fn finish(&self) -> Result<(), JsValue> {
        self.inner.finish().map_err(invalid)
    }
}

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
    acyclic_inference::validate_customer_wire(kind, message, expected, related).map_err(invalid)
}

/// Start Rust-owned state for a validated Run watch.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub fn watch_run_start_state_wire(
    message: &[u8],
    expected: &[u8],
    from_sequence: &str,
) -> Result<WatchRunState, JsValue> {
    acyclic_inference::watch_run_start_state_wire(message, expected, from_sequence)
        .map(|inner| WatchRunState { inner })
        .map_err(invalid)
}
