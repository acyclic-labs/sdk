//! Original conditional publication results shared by private and direct-live mutations.

use crate::foundation::{Digest, Epoch, GenerationId, Head};

/// One original authority publication result.
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

/// A direct-live mutation returns the original conditional publication result.
/// Rebase is an explicit caller operation after a conflict.
pub type LiveMutationOutcome = LivePublicationObservation;
