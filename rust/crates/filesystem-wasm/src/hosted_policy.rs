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

/// Validate the Rust-owned source lifecycle invariants before projection.
pub fn validate_hosted_source_state(
    state: u32,
    reason: u32,
    has_generation: bool,
) -> Result<(), &'static str> {
    if !(1..=5).contains(&state) || reason > 7 {
        return Err("source state or invalidation reason is invalid");
    }
    let needs_reason = state == 3;
    if needs_reason != (reason != 0) {
        return Err("source state and invalidation reason do not match");
    }
    let needs_generation = state == 1 || state == 5;
    if needs_generation != has_generation {
        return Err("source generation does not match its state");
    }
    Ok(())
}

/// Project a wire source lifecycle response into the public hosted API tags.
/// The wire enum values and public strings remain Rust-owned contract data.
pub fn project_hosted_source_state(
    state: u32,
    reason: u32,
    has_generation: bool,
) -> Result<(&'static str, Option<&'static str>), &'static str> {
    validate_hosted_source_state(state, reason, has_generation)?;
    let status = match state {
        1 => "clean",
        2 => "pending-capture",
        3 => "needs-rescan",
        4 => "conflict",
        5 => "sealed",
        _ => return Err("source state is invalid"),
    };
    let reason = match reason {
        0 => None,
        1 => Some("initial-snapshot-required"),
        2 => Some("queue-overflow"),
        3 => Some("native-rescan-required"),
        4 => Some("backend-error"),
        5 => Some("unrepresentable-path"),
        6 => Some("ambiguous-rename"),
        7 => Some("root-changed"),
        _ => return Err("source invalidation reason is invalid"),
    };
    Ok((status, reason))
}

/// Validate negotiated positive capability limits before the generated
/// hosted facade consumes them.
pub fn validate_hosted_advertised_limits(
    maximum_transaction_mutations: u32,
    maximum_page_items: u32,
) -> Result<(), &'static str> {
    if maximum_transaction_mutations == 0 || maximum_page_items == 0 {
        return Err("advertised hosted limits must be positive");
    }
    Ok(())
}

/// Validate caller-selected response and handshake bounds in Rust.
pub fn validate_hosted_response_bytes(
    maximum_response_bytes: f64,
    minimum_handshake_response_bytes: f64,
) -> Result<(), &'static str> {
    for value in [maximum_response_bytes, minimum_handshake_response_bytes] {
        if !value.is_finite()
            || value.fract() != 0.0
            || value <= 0.0
            || value > 9_007_199_254_740_991.0
        {
            return Err("response byte bounds must be positive safe integers");
        }
    }
    if maximum_response_bytes < minimum_handshake_response_bytes {
        return Err("maximum response bytes is below the handshake minimum");
    }
    Ok(())
}

#[cfg(target_arch = "wasm32")]
#[derive(serde::Serialize)]
struct HostedSourceProjection<'a> {
    status: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'a str>,
}

/// Validate a generation identity and its owning workspace identity.
pub fn validate_hosted_generation_identity(
    generation_id: &[u8],
    owner_workspace_id: &[u8],
    expected_workspace_id: &[u8],
) -> Result<(), &'static str> {
    if generation_id.len() != 32 {
        return Err("generation identity has the wrong length");
    }
    if owner_workspace_id.len() != 16 || expected_workspace_id.len() != 16 {
        return Err("workspace identity has the wrong length");
    }
    if owner_workspace_id != expected_workspace_id {
        return Err("generation belongs to another workspace");
    }
    Ok(())
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

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = validateHostedSourceState)]
pub fn validate_hosted_source_state_js(
    state: f64,
    reason: f64,
    has_generation: bool,
) -> Result<(), JsValue> {
    let state = js_u32(state).map_err(invalid)?;
    let reason = js_u32(reason).map_err(invalid)?;
    validate_hosted_source_state(state, reason, has_generation).map_err(invalid)
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = projectHostedSourceState)]
pub fn project_hosted_source_state_js(
    state: f64,
    reason: f64,
    has_generation: bool,
) -> Result<JsValue, JsValue> {
    let state = js_u32(state).map_err(invalid)?;
    let reason = js_u32(reason).map_err(invalid)?;
    let (status, reason) =
        project_hosted_source_state(state, reason, has_generation).map_err(invalid)?;
    let projection = HostedSourceProjection { status, reason };
    serde_wasm_bindgen::to_value(&projection)
        .map_err(|_| invalid("failed to encode hosted source projection"))
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = validateHostedAdvertisedLimits)]
pub fn validate_hosted_advertised_limits_js(
    maximum_transaction_mutations: f64,
    maximum_page_items: f64,
) -> Result<(), JsValue> {
    let maximum_transaction_mutations = js_u32(maximum_transaction_mutations).map_err(invalid)?;
    let maximum_page_items = js_u32(maximum_page_items).map_err(invalid)?;
    validate_hosted_advertised_limits(maximum_transaction_mutations, maximum_page_items)
        .map_err(invalid)
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = validateHostedResponseBytes)]
pub fn validate_hosted_response_bytes_js(
    maximum_response_bytes: f64,
    minimum_handshake_response_bytes: f64,
) -> Result<(), JsValue> {
    validate_hosted_response_bytes(maximum_response_bytes, minimum_handshake_response_bytes)
        .map_err(invalid)
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = validateHostedGenerationIdentity)]
pub fn validate_hosted_generation_identity_js(
    generation_id: &[u8],
    owner_workspace_id: &[u8],
    expected_workspace_id: &[u8],
) -> Result<(), JsValue> {
    validate_hosted_generation_identity(generation_id, owner_workspace_id, expected_workspace_id)
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

    #[test]
    fn source_state_requires_matching_reason_and_generation() {
        assert!(validate_hosted_source_state(1, 0, true).is_ok());
        assert!(validate_hosted_source_state(3, 2, false).is_ok());
        assert!(validate_hosted_source_state(3, 0, false).is_err());
        assert!(validate_hosted_source_state(1, 0, false).is_err());
        assert!(validate_hosted_source_state(2, 1, false).is_err());
    }

    #[test]
    fn source_projection_uses_public_contract_tags() {
        assert_eq!(project_hosted_source_state(1, 0, true), Ok(("clean", None)));
        assert_eq!(
            project_hosted_source_state(3, 2, false),
            Ok(("needs-rescan", Some("queue-overflow")))
        );
        assert!(project_hosted_source_state(2, 1, false).is_err());
    }

    #[test]
    fn advertised_and_response_limits_are_rust_owned() {
        assert!(validate_hosted_advertised_limits(1, 1).is_ok());
        assert!(validate_hosted_advertised_limits(0, 1).is_err());
        assert!(validate_hosted_response_bytes(1_024.0, 512.0).is_ok());
        assert!(validate_hosted_response_bytes(511.0, 512.0).is_err());
        assert!(validate_hosted_response_bytes(1.5, 1.0).is_err());
    }

    #[test]
    fn generation_identity_requires_exact_owner() {
        assert!(validate_hosted_generation_identity(&[7; 32], &[1; 16], &[1; 16]).is_ok());
        assert!(validate_hosted_generation_identity(&[7; 31], &[1; 16], &[1; 16]).is_err());
        assert!(validate_hosted_generation_identity(&[7; 32], &[1; 16], &[2; 16]).is_err());
    }
}
