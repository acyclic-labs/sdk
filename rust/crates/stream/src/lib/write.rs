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
}
