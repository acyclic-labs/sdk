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
/// observation data, not authorization to any path or resource.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AcceptedCommitOutcome {
    /// Both projections describe the same installed publication. Record values
    /// share their original immutable byte owners; no payload is reconstructed.
    Committed {
        /// Compact assigned ranges and authority revision.
        receipt: CommitReceipt,
        /// Original accepted records and fork cuts.
        envelope: CommittedEnvelope,
    },
    /// Conditions refused without publishing or spending the retry identity.
    Conflict(Vec<CommitConflict>),
}

impl WriteOutcome {
    #[cfg(any(feature = "grpc", all(feature = "wasm", target_arch = "wasm32")))]
    pub(crate) fn commit_receipt(&self) -> Option<&CommitReceipt> {
        match self {
            Self::Commit(CommitOutcome::Committed(receipt))
            | Self::CommitAccepted(AcceptedCommitOutcome::Committed { receipt, .. }) => {
                Some(receipt)
            }
            _ => None,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn into_commit(self) -> Result<CommitOutcome, StreamError> {
        match self {
            Self::Commit(outcome) => Ok(outcome),
            _ => Err(StreamError::InvalidResponse),
        }
    }
}
