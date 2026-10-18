use super::*;
use crate::foundation::{DurableCommit, Epoch, OperationId};

pub(super) fn outcome(sequence: u64, retry: bool) -> AppendOutcome {
    let commit = DurableCommit {
        epoch: Epoch::GENESIS,
        sequence: Sequence::new(sequence),
        operation_id: OperationId::from_bytes([sequence as u8; 16]),
        fingerprint: crate::Digest::ZERO,
        previous_digest: crate::Digest::ZERO,
        digest: crate::Digest::ZERO,
        payload: bytes::Bytes::new(),
        settled_at_micros: None,
    };
    if retry {
        AppendOutcome::AlreadyCommitted(commit)
    } else {
        AppendOutcome::Committed(commit)
    }
}
