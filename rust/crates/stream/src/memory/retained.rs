/// One indefinitely retained snapshot entry.
struct Retained {
    replay: Option<Bytes>,
    replay_left: Option<usize>,
    replay_right: Option<usize>,
    replay_height: u8,
    commit_branch: Option<CommitBranch>,
    digest: [u8; 32],
    /// The publication's revision index.
    index: u64,
    result: IdempotencyOutcome,
    envelope: Option<CommittedEnvelope>,
    _allocation: Allocation,
}

impl Retained {
    fn envelope(&self) -> Option<&CommittedEnvelope> {
        match &self.result {
            IdempotencyOutcome::Commit(envelope) => Some(envelope),
            _ => self.envelope.as_ref(),
        }
    }
}
