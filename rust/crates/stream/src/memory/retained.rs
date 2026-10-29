/// One indefinitely retained snapshot entry.
struct Retained {
    fact: Arc<RetainedFact>,
    replay_left: Option<usize>,
    replay_right: Option<usize>,
    replay_height: u8,
    commit_branch: Option<CommitBranch>,
}

/// The original immutable fact and its allocation custody. Derived indexes
/// remain in the retained inventory; a snapshot pins only this ownership chain.
struct RetainedFact {
    previous: Option<Arc<Self>>,
    next: std::sync::OnceLock<RetainedLink>,
    replay: Option<Bytes>,
    digest: [u8; 32],
    /// The publication's revision index.
    index: u64,
    result: IdempotencyOutcome,
    envelope: Option<CommittedEnvelope>,
    _allocation: Allocation,
    _header: Allocation,
}

/// A successor retains its predecessor, so this link outlives every strong
/// successor owner. Drop the weak reference before releasing its original
/// allocation claim: a dead successor's Arc storage still belongs to the link.
struct RetainedLink {
    fact: std::sync::Weak<RetainedFact>,
    _allocation: Allocation,
}

impl RetainedFact {
    fn reserved(header: Allocation) -> Arc<Self> {
        Arc::new(Self {
            previous: None,
            next: std::sync::OnceLock::new(),
            replay: None,
            digest: [0; 32],
            index: 0,
            result: IdempotencyOutcome::Append(AppendOutcome::TailConflict { actual_tail: 0 }),
            envelope: None,
            _allocation: None,
            _header: header,
        })
    }

    fn envelope(&self) -> Option<&CommittedEnvelope> {
        match &self.result {
            IdempotencyOutcome::Commit(envelope) => Some(envelope),
            _ => self.envelope.as_ref(),
        }
    }
}

impl Drop for RetainedFact {
    fn drop(&mut self) {
        let mut previous = self.previous.take();
        while let Some(node) = previous {
            let Ok(mut node) = Arc::try_unwrap(node) else {
                break;
            };
            previous = node.previous.take();
        }
    }
}

impl std::ops::Deref for Retained {
    type Target = RetainedFact;

    fn deref(&self) -> &Self::Target {
        &self.fact
    }
}

impl Retained {
    fn envelope(&self) -> Option<&CommittedEnvelope> {
        self.fact.envelope()
    }
}
