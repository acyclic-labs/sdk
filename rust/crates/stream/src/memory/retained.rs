/// One indefinitely retained snapshot entry.
struct Retained {
    replay: Option<Bytes>,
    digest: [u8; 32],
    result: IdempotencyOutcome,
    envelope: Option<CommittedEnvelope>,
    _allocation: Allocation,
}

impl Retained {
    fn envelope(&self) -> Option<&CommittedEnvelope> {
        match &self.result {
            IdempotencyOutcome::Commit(CommitOutcome::Committed(envelope)) => Some(envelope),
            _ => self.envelope.as_ref(),
        }
    }
}
