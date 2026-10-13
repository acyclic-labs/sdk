#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub(crate) fn observation_to_wire(
    value: crate::IdempotencyObservation,
) -> wire::InspectIdempotencyResponse {
    let outcome = match value.outcome {
        crate::IdempotencyOutcome::Append(value) => {
            wire::idempotency_observation::Outcome::Append(append_outcome_to_wire(value))
        }
        crate::IdempotencyOutcome::Fork(value) => {
            wire::idempotency_observation::Outcome::Fork(fork_receipt_to_wire(&value))
        }
        crate::IdempotencyOutcome::Commit(value) => {
            wire::idempotency_observation::Outcome::Commit(commit_outcome_to_wire(value))
        }
    };
    wire::InspectIdempotencyResponse {
        observation: Some(wire::IdempotencyObservation {
            idempotency_key: bytes::Bytes::copy_from_slice(value.idempotency_key.as_bytes()),
            request_digest: bytes::Bytes::copy_from_slice(&value.request_digest),
            outcome: Some(outcome),
        }),
    }
}
