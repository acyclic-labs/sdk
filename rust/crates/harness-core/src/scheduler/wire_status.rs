//! The scheduler projection's canonical wire status.
use super::{OperationPhase, OperationState};
use crate::{Outcome, wire, wire_codec::encode_authority, wire_validation::current_protocol};

/// Converts the authoritative scheduler projection to its canonical wire status.
#[must_use]
pub(crate) fn operation_status(state: &OperationState) -> wire::OperationStatus {
    let owner = state.spec.owner.authority();
    let (completion, error) = match state.outcome.as_ref() {
        Some(Outcome::Succeeded(_)) => (wire::CompletionState::Succeeded, None),
        Some(Outcome::Failed { message }) => (
            wire::CompletionState::Failed,
            Some(wire::Error {
                code: wire::ErrorCode::Unspecified as i32,
                message: message.clone(),
                operation_id: state.spec.operation_id.to_string(),
            }),
        ),
        Some(Outcome::Cancelled) => (wire::CompletionState::Cancelled, None),
        Some(Outcome::Indeterminate {
            operation_id,
            cause,
        }) => (
            wire::CompletionState::Indeterminate,
            Some(wire::Error {
                code: wire::ErrorCode::Indeterminate as i32,
                message: crate::contract::indeterminate_display(
                    *operation_id,
                    match cause {
                        crate::IndeterminateCause::Unobserved => None,
                        crate::IndeterminateCause::Command(cause) => Some(cause),
                    },
                )
                .to_string(),
                operation_id: operation_id.to_string(),
            }),
        ),
        None if state.phase == OperationPhase::Terminal => (
            wire::CompletionState::Indeterminate,
            Some(wire::Error {
                code: wire::ErrorCode::Indeterminate as i32,
                message: "terminal operation outcome is missing".into(),
                operation_id: state.spec.operation_id.to_string(),
            }),
        ),
        None => (wire::CompletionState::Running, None),
    };
    wire::OperationStatus {
        operation: Some(wire::OperationIdentity {
            operation_id: state.spec.operation_id.to_string(),
            idempotency_key: String::new(),
        }),
        state: completion as i32,
        error,
        protocol: Some(current_protocol()),
        owner: Some(encode_authority(owner)),
        cancellation_requested: state.cancellation_requested,
        revision: state.revision,
        replay_through: None,
    }
}
