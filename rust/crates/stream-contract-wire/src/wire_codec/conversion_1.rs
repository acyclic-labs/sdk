/// Projects an idempotency observation into its wire response.
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub fn observation_to_wire(
    value: acyclic_stream_contract_core::IdempotencyObservation,
) -> wire::InspectIdempotencyResponse {
    wire::InspectIdempotencyResponse {
        observation: Some(observation_wire(value)),
    }
}
