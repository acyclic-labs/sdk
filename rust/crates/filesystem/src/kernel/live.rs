//! Total direct-live publication and safe-rebase retry state machine.

use super::{RebaseConflict, RebaseDecision};
use crate::foundation::{Digest, Epoch, GenerationId, Head};
use thiserror::Error;

/// One authority publication observation consumed by live retry orchestration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LivePublicationObservation {
    /// This attempt durably published the candidate.
    Committed {
        /// Published generation.
        generation_id: GenerationId,
        /// Durable authority head.
        head: Head,
    },
    /// The same operation and candidate were already durable.
    AlreadyCommitted {
        /// Previously published generation.
        generation_id: GenerationId,
        /// Original durable authority head.
        head: Head,
    },
    /// Another operation advanced authority first.
    Conflict {
        /// Latest linearizable authority head.
        actual: Head,
    },
    /// The writer epoch is stale.
    Fenced {
        /// Active authority epoch.
        actual_epoch: Epoch,
    },
    /// The operation identity belongs to another candidate fingerprint.
    IdempotencyConflict {
        /// Existing durable fingerprint.
        committed_fingerprint: Digest,
    },
}

/// Terminal outcome of one direct-live mutation publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LiveMutationOutcome {
    /// This call durably published the candidate generation.
    Committed {
        /// Published content-addressed generation.
        generation_id: GenerationId,
        /// Authority head produced by the durable append.
        head: Head,
    },
    /// The same operation identity and fingerprint was already durable.
    AlreadyCommitted {
        /// Previously published content-addressed generation.
        generation_id: GenerationId,
        /// Authority head at the original durable append.
        head: Head,
    },
    /// An exact observed or mutated region changed concurrently.
    Conflicted {
        /// Bounded region-specific conflicts.
        conflicts: Vec<RebaseConflict>,
        /// Whether additional conflicts were omitted by the caller's bound.
        truncated: bool,
    },
    /// Safe retries were exhausted because the authority kept advancing.
    RetryLimit {
        /// Latest linearizable authority head observed by publication.
        actual: Head,
    },
    /// The checkout's writer epoch is stale.
    Fenced {
        /// Active authority epoch.
        actual_epoch: Epoch,
    },
    /// The operation identity was already bound to another fingerprint.
    IdempotencyConflict {
        /// Fingerprint durably bound to the reused operation identity.
        committed_fingerprint: Digest,
    },
}

/// Next exact action selected by live retry orchestration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LiveRetryAction {
    /// Rebase the retained candidate against the current authority head.
    Rebase,
    /// Publish the safely rebased candidate with the same operation identity.
    Publish,
    /// Stop with one typed terminal outcome.
    Complete(LiveMutationOutcome),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Publication,
    Rebase,
    Complete,
}

/// Operation-local total retry controller.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LiveRetryState {
    attempts_remaining: u32,
    phase: Phase,
}

impl LiveRetryState {
    /// Starts before the first publication attempt.
    ///
    /// # Errors
    ///
    /// Rejects a zero-attempt controller.
    pub const fn new(maximum_attempts: u32) -> Result<Self, LiveRetryError> {
        if maximum_attempts == 0 {
            return Err(LiveRetryError::ZeroAttempts);
        }
        Ok(Self {
            attempts_remaining: maximum_attempts,
            phase: Phase::Publication,
        })
    }

    /// Consumes one authority publication observation.
    ///
    /// # Errors
    ///
    /// Rejects observations outside the publication phase.
    pub fn observe_publication(
        &mut self,
        observation: LivePublicationObservation,
    ) -> Result<LiveRetryAction, LiveRetryError> {
        if self.phase != Phase::Publication {
            return Err(LiveRetryError::InvalidTransition);
        }
        let terminal = match observation {
            LivePublicationObservation::Committed {
                generation_id,
                head,
            } => Some(LiveMutationOutcome::Committed {
                generation_id,
                head,
            }),
            LivePublicationObservation::AlreadyCommitted {
                generation_id,
                head,
            } => Some(LiveMutationOutcome::AlreadyCommitted {
                generation_id,
                head,
            }),
            LivePublicationObservation::Fenced { actual_epoch } => {
                Some(LiveMutationOutcome::Fenced { actual_epoch })
            }
            LivePublicationObservation::IdempotencyConflict {
                committed_fingerprint,
            } => Some(LiveMutationOutcome::IdempotencyConflict {
                committed_fingerprint,
            }),
            LivePublicationObservation::Conflict { actual } => {
                self.attempts_remaining -= 1;
                if self.attempts_remaining == 0 {
                    Some(LiveMutationOutcome::RetryLimit { actual })
                } else {
                    self.phase = Phase::Rebase;
                    return Ok(LiveRetryAction::Rebase);
                }
            }
        };
        self.phase = Phase::Complete;
        Ok(LiveRetryAction::Complete(
            terminal.ok_or(LiveRetryError::InvalidTransition)?,
        ))
    }

    /// Consumes one exact rebase classification.
    ///
    /// # Errors
    ///
    /// Rejects classifications outside the rebase phase.
    pub fn observe_rebase(
        &mut self,
        decision: RebaseDecision,
    ) -> Result<LiveRetryAction, LiveRetryError> {
        if self.phase != Phase::Rebase {
            return Err(LiveRetryError::InvalidTransition);
        }
        match decision {
            RebaseDecision::Safe { .. } => {
                self.phase = Phase::Publication;
                Ok(LiveRetryAction::Publish)
            }
            RebaseDecision::Conflicted {
                conflicts,
                truncated,
            } => {
                self.phase = Phase::Complete;
                Ok(LiveRetryAction::Complete(LiveMutationOutcome::Conflicted {
                    conflicts,
                    truncated,
                }))
            }
        }
    }
}

/// Invalid retry-controller construction or event order.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LiveRetryError {
    /// No first publication attempt was admitted.
    #[error("live mutation retry bound must be positive")]
    ZeroAttempts,
    /// Driver supplied an observation outside its required phase.
    #[error("live mutation retry transition is invalid")]
    InvalidTransition,
}

#[cfg(test)]
#[path = "tests/live.rs"]
mod tests;
