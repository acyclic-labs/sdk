/// The affected range of one append, without echoing its record values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendRange {
    /// Destination path.
    pub path: StreamPath,
    /// First sequence assigned by this commit.
    pub start: u64,
    /// Exclusive resulting tail.
    pub tail: u64,
}

/// Compact acknowledgment of one atomic publication. Full immutable records
/// remain available through `StreamProvider::read_commit` and keyed inspection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitReceipt {
    /// Identity shared by every record and fork in this publication.
    pub commit_id: CommitId,
    /// Canonical publication time, including publications with no new records.
    pub committed_at_micros: u64,
    /// Assigned append ranges, in request order.
    pub appends: Vec<AppendRange>,
    /// Captured fork cuts, in request order.
    pub forks: Vec<ForkReceipt>,
    /// Committed authority revision, shared by every nested fork receipt.
    pub revision: Revision,
}

impl CommitReceipt {
    /// Projects retained facts into empty, caller-funded metadata buffers.
    /// Never grows either buffer. `visit(bytes, copied)` runs before each copy
    /// with `copied = false`, then after it with `copied = true`. Both failures
    /// retain the caller's original cause and allocation custody.
    pub fn project<E: From<StreamError>>(
        envelope: &CommittedEnvelope,
        revision: Revision,
        mut appends: Vec<AppendRange>,
        mut forks: Vec<ForkReceipt>,
        mut visit: impl FnMut(usize, bool) -> Result<(), E>,
    ) -> Result<Self, E> {
        if !appends.is_empty() || !forks.is_empty() {
            return Err(StreamError::InvalidArgument.into());
        }
        for mutation in &envelope.mutations {
            match mutation {
                CommittedMutation::Append(append) => {
                    if appends.len() == appends.capacity() {
                        return Err(StreamError::LimitExceeded.into());
                    }
                    let bytes = std::mem::size_of::<AppendRange>();
                    visit(bytes, false)?;
                    appends.push(AppendRange {
                        path: append.path.clone(),
                        start: append.start,
                        tail: append.tail,
                    });
                    visit(bytes, true)?;
                }
                CommittedMutation::Fork(fork) => {
                    if forks.len() == forks.capacity() {
                        return Err(StreamError::LimitExceeded.into());
                    }
                    let bytes = std::mem::size_of::<ForkReceipt>();
                    visit(bytes, false)?;
                    forks.push(ForkReceipt {
                        source: fork.source.clone(),
                        destination: fork.destination.clone(),
                        forked_at: fork.forked_at,
                        tail: fork.tail,
                        commit_id: envelope.commit_id,
                        revision: revision.clone(),
                    });
                    visit(bytes, true)?;
                }
            }
        }
        Ok(Self {
            commit_id: envelope.commit_id,
            committed_at_micros: envelope.committed_at_micros,
            appends,
            forks,
            revision,
        })
    }

    pub(crate) fn matches_envelope(&self, envelope: &CommittedEnvelope) -> bool {
        self.commit_id == envelope.commit_id
            && self.committed_at_micros == envelope.committed_at_micros
            && self
                .appends
                .iter()
                .map(|a| (&a.path, a.start, a.tail))
                .eq(envelope
                    .mutations
                    .iter()
                    .filter_map(|mutation| match mutation {
                        CommittedMutation::Append(a) => Some((&a.path, a.start, a.tail)),
                        CommittedMutation::Fork(_) => None,
                    }))
            && self
                .forks
                .iter()
                .map(|f| (&f.source, &f.destination, f.forked_at, f.tail, f.commit_id))
                .eq(envelope
                    .mutations
                    .iter()
                    .filter_map(|mutation| match mutation {
                        CommittedMutation::Fork(f) => Some((
                            &f.source,
                            &f.destination,
                            f.forked_at,
                            f.tail,
                            envelope.commit_id,
                        )),
                        CommittedMutation::Append(_) => None,
                    }))
    }

    pub(crate) fn validate(&self) -> Result<(), StreamError> {
        let count = self
            .appends
            .len()
            .checked_add(self.forks.len())
            .ok_or(StreamError::InvalidArgument)?;
        if count == 0
            || count > MAX_ITEMS
            || self
                .appends
                .windows(2)
                .any(|pair| pair[0].path >= pair[1].path)
            || self
                .forks
                .windows(2)
                .any(|pair| pair[0].destination >= pair[1].destination)
            || self.appends.iter().any(|a| {
                a.start >= a.tail
                    || a.tail - a.start > MAX_ITEMS as u64
                    || self
                        .forks
                        .binary_search_by(|f| f.destination.cmp(&a.path))
                        .is_ok()
            })
            || self.forks.iter().any(|f| {
                f.source == f.destination
                    || f.forked_at > f.tail
                    || f.tail - f.forked_at > MAX_ITEMS as u64
                    || f.commit_id != self.commit_id
                    || f.revision != self.revision
            })
        {
            return Err(StreamError::InvalidArgument);
        }
        Ok(())
    }
}
