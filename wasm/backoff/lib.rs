#![forbid(unsafe_code)]

use acyclic_auth_operation::Backoff;
use wasm_bindgen::prelude::wasm_bindgen;

/// Compute spacing from the caller's existing attempt and entropy.
#[wasm_bindgen(js_name = retryDelayMs)]
pub fn retry_delay_ms(base_ms: f64, maximum_ms: f64, doublings: u32, unit_sample: f64) -> f64 {
    Backoff::fractional_delay_ms_at(base_ms, maximum_ms, doublings, unit_sample)
}
