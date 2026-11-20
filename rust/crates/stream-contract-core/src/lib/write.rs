/// One canonical publication request. Families retain their own retry digest
/// and outcome: an append or fork is never translated into a different commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteRequest {
    /// Append a contiguous batch, optionally conditioned on the exact tail.
    Append(AppendRequest),
    /// Fork an immutable prefix, selecting the current tail atomically if omitted.
    Fork(ForkRequest),
    /// Publish one all-or-nothing optimistic commit.
    Commit(CommitRequest),
    /// The same keyed commit, returning its original accepted envelope as well
    /// as the compact receipt. Selection does not change the retry identity.
    CommitAccepted(CommitRequest),
}

/// Terminal outcome of the corresponding canonical publication family.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteOutcome {
    /// Append receipt or tail conflict.
    Append(AppendOutcome),
    /// Immutable-prefix fork receipt.
    Fork(ForkReceipt),
    /// Successful commit or condition conflict.
    Commit(CommitOutcome),
    /// The original accepted or replayed fact, without a subsequent read.
    CommitAccepted(AcceptedCommitOutcome),
}

/// A committed fact selected at the original publication boundary. This is
/// observation data, not authorization to any path or resource. Both projections
/// describe the same installed publication. Record values share their original
/// immutable byte owners; no payload is reconstructed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcceptedCommit {
    /// Compact assigned ranges and authority revision.
    pub receipt: CommitReceipt,
    /// Original accepted records and fork cuts.
    pub envelope: CommittedEnvelope,
}

/// Successful original acceptance, or conditions refused without publishing or
/// spending the retry identity.
pub type AcceptedCommitOutcome = Result<AcceptedCommit, Vec<CommitConflict>>;

const _: () = assert!(core::mem::size_of::<WriteOutcome>() <= 272);

impl WriteOutcome {
    /// The receipt of a committed write, if this outcome published one.
    pub fn commit_receipt(&self) -> Option<&CommitReceipt> {
        match self {
            Self::Commit(CommitOutcome::Committed(receipt))
            | Self::CommitAccepted(Ok(AcceptedCommit { receipt, .. })) => Some(receipt),
            _ => None,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// The commit outcome, refusing any other write outcome.
    pub fn into_commit(self) -> Result<CommitOutcome, StreamError> {
        match self {
            Self::Commit(outcome) => Ok(outcome),
            _ => Err(StreamError::InvalidResponse),
        }
    }
}
