#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub(crate) fn observation_to_wire(
    value: crate::IdempotencyObservation,
) -> wire::InspectIdempotencyResponse {
    wire::InspectIdempotencyResponse {
        observation: Some(observation_wire(value)),
    }
}
