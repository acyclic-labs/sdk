use super::*;

pub(super) fn checkout_commit_outcome(
    receipt: &crate::kernel::PublicationReceipt,
) -> LivePublicationObservation {
    let generation_id = receipt.proof.generation_id;
    match &receipt.outcome {
        AppendOutcome::Committed(commit) => LivePublicationObservation::Committed {
            generation_id,
            head: durable_head(commit),
        },
        AppendOutcome::AlreadyCommitted(commit) => LivePublicationObservation::AlreadyCommitted {
            generation_id,
            head: durable_head(commit),
        },
        AppendOutcome::Conflict { actual } => {
            LivePublicationObservation::Conflict { actual: *actual }
        }
        AppendOutcome::Fenced { actual_epoch } => LivePublicationObservation::Fenced {
            actual_epoch: *actual_epoch,
        },
        AppendOutcome::IdempotencyConflict {
            committed_fingerprint,
        } => LivePublicationObservation::IdempotencyConflict {
            committed_fingerprint: *committed_fingerprint,
        },
    }
}
