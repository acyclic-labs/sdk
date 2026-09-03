use super::*;
use crate::foundation::{Digest, Epoch, GenerationId, Head, Sequence};

fn head(value: u64) -> Result<Head, crate::foundation::IdentityError> {
    Ok(Head {
        epoch: Epoch::new(1)?,
        sequence: Sequence::new(value),
        digest: Digest::from_bytes([u8::try_from(value).unwrap_or(u8::MAX); 32]),
    })
}

#[test]
fn every_publication_terminal_is_total_and_final() -> Result<(), Box<dyn std::error::Error>> {
    let observations = [
        LivePublicationObservation::Committed {
            generation_id: GenerationId::new(Digest::from_bytes([1; 32])),
            head: head(1)?,
        },
        LivePublicationObservation::AlreadyCommitted {
            generation_id: GenerationId::new(Digest::from_bytes([2; 32])),
            head: head(2)?,
        },
        LivePublicationObservation::Fenced {
            actual_epoch: Epoch::new(2)?,
        },
        LivePublicationObservation::IdempotencyConflict {
            committed_fingerprint: Digest::from_bytes([3; 32]),
        },
    ];
    for observation in observations {
        let mut state = LiveRetryState::new(3)?;
        assert!(matches!(
            state.observe_publication(observation)?,
            LiveRetryAction::Complete(_)
        ));
        assert_eq!(
            state.observe_publication(observation),
            Err(LiveRetryError::InvalidTransition)
        );
    }
    Ok(())
}

#[test]
fn conflict_rebase_retry_and_exhaustion_have_one_transition_order()
-> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(LiveRetryState::new(0), Err(LiveRetryError::ZeroAttempts));
    let conflict = LivePublicationObservation::Conflict { actual: head(1)? };
    let mut state = LiveRetryState::new(2)?;
    assert_eq!(
        state.observe_publication(conflict)?,
        LiveRetryAction::Rebase
    );
    assert_eq!(
        state.observe_publication(conflict),
        Err(LiveRetryError::InvalidTransition)
    );
    assert_eq!(
        state.observe_rebase(RebaseDecision::Safe {
            generation: GenerationId::new(Digest::from_bytes([5; 32])),
        })?,
        LiveRetryAction::Publish
    );
    assert!(matches!(
        state.observe_publication(LivePublicationObservation::Conflict { actual: head(2)? })?,
        LiveRetryAction::Complete(LiveMutationOutcome::RetryLimit { actual }) if actual == head(2)?
    ));
    Ok(())
}

#[test]
fn rebase_observations_are_rejected_outside_the_rebase_phase()
-> Result<(), Box<dyn std::error::Error>> {
    let mut initial = LiveRetryState::new(1)?;
    assert_eq!(
        initial.observe_rebase(RebaseDecision::Safe {
            generation: GenerationId::new(Digest::from_bytes([9; 32])),
        }),
        Err(LiveRetryError::InvalidTransition)
    );

    assert!(matches!(
        initial.observe_publication(LivePublicationObservation::Fenced {
            actual_epoch: Epoch::new(2)?,
        })?,
        LiveRetryAction::Complete(_)
    ));
    assert_eq!(
        initial.observe_rebase(RebaseDecision::Safe {
            generation: GenerationId::new(Digest::from_bytes([9; 32])),
        }),
        Err(LiveRetryError::InvalidTransition)
    );
    Ok(())
}
