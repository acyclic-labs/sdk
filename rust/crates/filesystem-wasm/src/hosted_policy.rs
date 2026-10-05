//! Rust-owned hosted Filesystem request-policy exports.
//!
//! The remote TypeScript facade may use these functions for early rejection,
//! while the hosted Rust service remains the authoritative validator. Keeping
//! both calls on the same contract helpers prevents browser and native clients
//! from drifting in their interpretation of negotiated limits.

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

#[cfg(target_arch = "wasm32")]
fn invalid(error: &'static str) -> JsValue {
    JsValue::from_str(error)
}

/// Validate one positive page-sized request bound.
pub fn validate_hosted_page_bound(value: u32, maximum: u32) -> Result<(), &'static str> {
    acyclic_fs::hosted_contract::validate_page_bound(value, maximum)
}

/// Validate mutation and conflict limits shared by transaction requests.
pub fn validate_hosted_transaction_bounds(
    mutation_count: u32,
    maximum_mutations: u32,
    maximum_conflicts: u32,
    maximum_page_items: u32,
) -> Result<(), &'static str> {
    acyclic_fs::hosted_contract::validate_transaction_bounds(
        mutation_count as usize,
        maximum_mutations,
        maximum_conflicts,
        maximum_page_items,
    )
}

/// Validate generation, diff, and conflict limits shared by rebase and join.
pub fn validate_hosted_generation_bounds(
    maximum_generations: u32,
    maximum_changes: u32,
    maximum_conflicts: u32,
    maximum_page_items: u32,
) -> Result<(), &'static str> {
    acyclic_fs::hosted_contract::validate_generation_bounds(
        maximum_generations,
        maximum_changes,
        maximum_conflicts,
        maximum_page_items,
    )
}

/// Convert a JavaScript number to a Rust `u32` without allowing wasm-bindgen's
/// numeric coercion to wrap negative, fractional, non-finite, or overflowing
/// inputs before the Rust-owned policy runs.
fn js_u32(value: f64) -> Result<u32, &'static str> {
    if !value.is_finite() || value.fract() != 0.0 || !(0.0..=u32::MAX as f64).contains(&value) {
        return Err("value must be a finite integer in the u32 range");
    }
    Ok(value as u32)
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = validateHostedPageBound)]
pub fn validate_hosted_page_bound_js(value: f64, maximum: f64) -> Result<(), JsValue> {
    let value = js_u32(value).map_err(invalid)?;
    let maximum = js_u32(maximum).map_err(invalid)?;
    validate_hosted_page_bound(value, maximum).map_err(invalid)
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = validateHostedTransactionBounds)]
pub fn validate_hosted_transaction_bounds_js(
    mutation_count: f64,
    maximum_mutations: f64,
    maximum_conflicts: f64,
    maximum_page_items: f64,
) -> Result<(), JsValue> {
    let mutation_count = js_u32(mutation_count).map_err(invalid)?;
    let maximum_mutations = js_u32(maximum_mutations).map_err(invalid)?;
    let maximum_conflicts = js_u32(maximum_conflicts).map_err(invalid)?;
    let maximum_page_items = js_u32(maximum_page_items).map_err(invalid)?;
    validate_hosted_transaction_bounds(
        mutation_count,
        maximum_mutations,
        maximum_conflicts,
        maximum_page_items,
    )
    .map_err(invalid)
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = validateHostedGenerationBounds)]
pub fn validate_hosted_generation_bounds_js(
    maximum_generations: f64,
    maximum_changes: f64,
    maximum_conflicts: f64,
    maximum_page_items: f64,
) -> Result<(), JsValue> {
    let maximum_generations = js_u32(maximum_generations).map_err(invalid)?;
    let maximum_changes = js_u32(maximum_changes).map_err(invalid)?;
    let maximum_conflicts = js_u32(maximum_conflicts).map_err(invalid)?;
    let maximum_page_items = js_u32(maximum_page_items).map_err(invalid)?;
    validate_hosted_generation_bounds(
        maximum_generations,
        maximum_changes,
        maximum_conflicts,
        maximum_page_items,
    )
    .map_err(invalid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_bound_accepts_positive_value_and_rejects_zero_or_overflow() {
        assert!(validate_hosted_page_bound(1, 2).is_ok());
        assert!(validate_hosted_page_bound(0, 2).is_err());
        assert!(validate_hosted_page_bound(3, 2).is_err());
    }

    #[test]
    fn transaction_bound_accepts_valid_request_and_rejects_zero_or_overflow() {
        assert!(validate_hosted_transaction_bounds(1, 2, 1, 2).is_ok());
        assert!(validate_hosted_transaction_bounds(0, 2, 1, 2).is_err());
        assert!(validate_hosted_transaction_bounds(3, 2, 1, 2).is_err());
        assert!(validate_hosted_transaction_bounds(1, 2, 3, 2).is_err());
    }

    #[test]
    fn generation_bound_accepts_valid_request_and_rejects_zero_or_overflow() {
        assert!(validate_hosted_generation_bounds(1, 1, 1, 1).is_ok());
        assert!(validate_hosted_generation_bounds(0, 1, 1, 1).is_err());
        assert!(validate_hosted_generation_bounds(1, 2, 1, 1).is_err());
    }

    #[test]
    fn javascript_numbers_never_wrap_into_valid_u32_values() {
        for value in [-1.0, 1.5, f64::NAN, f64::INFINITY, (u32::MAX as f64) + 1.0] {
            assert!(js_u32(value).is_err(), "unexpectedly accepted {value:?}");
        }
        assert_eq!(js_u32(0.0), Ok(0));
        assert_eq!(js_u32(u32::MAX as f64), Ok(u32::MAX));
    }
}
