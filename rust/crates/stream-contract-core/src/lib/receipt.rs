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
    /// Assigned append ranges, in canonical path order.
    pub appends: Vec<AppendRange>,
    /// Captured fork cuts, in canonical destination order.
    pub forks: Vec<ForkReceipt>,
    /// Committed authority revision, shared by every nested fork receipt.
    pub revision: Revision,
    /// Indices of the soft effects the commit skipped, ascending. A commit
    /// may skip every effect and still commit.
    pub skipped: Vec<u32>,
}

impl CommitReceipt {
    /// Validates the complete compact membership of an original request already
    /// admitted by [`request::validate_commit`]. Borrows both owners, examines
    /// only their control vectors, and performs no allocation, payload read,
    /// hash or I/O. Work is linear in this bounded request, not stream history,
    /// up to one binary search per mutation.
    ///
    /// Every mutation of every effect not listed in `skipped` must appear
    /// exactly once, with its exact record count, and no mutation of a skipped
    /// effect may appear; only soft effects may be skipped. Append starts are
    /// checked when the effect's own condition on the destination supplies a
    /// tail; an unconditioned or `Present` append supplies no independent start
    /// anchor. Fork cuts and nested receipt identities must match. This does not
    /// authenticate payloads, the request key, a provider revision, durability
    /// or device coverage. Noncanonical request controls return
    /// `InvalidArgument`; inconsistent receipt members return `InvalidResponse`.
    /// Neither owner is changed.
    pub fn validate_request(&self, request: &CommitRequest) -> Result<(), StreamError> {
        use crate::request::effect_conditions;
        if !crate::preparation::shaped(request)
            || request.mutations().count() > MAX_ITEMS
            || request.conditions().count() > MAX_ITEMS
            || request.effects.iter().any(|effect| {
                !effect.conditions.is_sorted_by(|left, right| {
                    crate::CommitCondition::path(left) < crate::CommitCondition::path(right)
                }) || !effect.mutations.is_sorted_by(|left, right| {
                    crate::CommitMutation::path(left) < crate::CommitMutation::path(right)
                })
            })
        {
            return Err(StreamError::InvalidArgument);
        }
        if !self.skipped.is_sorted_by(|left, right| left < right)
            || self.skipped.iter().any(|index| {
                usize::try_from(*index)
                    .ok()
                    .and_then(|index| request.effects.get(index))
                    .is_none_or(|effect| !effect.soft)
            })
        {
            return Err(StreamError::InvalidResponse);
        }
        let mut published = 0_usize;
        for (skipped, effect) in crate::preparation::settled_effects(request, &self.skipped) {
            let mut condition_for = effect_conditions(effect);
            for mutation in &effect.mutations {
                let path = crate::CommitMutation::path(mutation);
                let append = self
                    .appends
                    .binary_search_by(|append| append.path.cmp(path))
                    .ok()
                    .and_then(|index| self.appends.get(index));
                let fork = self
                    .forks
                    .binary_search_by(|fork| fork.destination.cmp(path))
                    .ok()
                    .and_then(|index| self.forks.get(index));
                let condition = condition_for(path);
                let matches = match mutation {
                    CommitMutation::Append { records, .. } => {
                        if records.is_empty() || records.len() > MAX_ITEMS {
                            return Err(StreamError::InvalidArgument);
                        }
                        let start = match condition {
                            Some(CommitCondition::Head { expected: 0, .. }) => {
                                return Err(StreamError::InvalidArgument);
                            }
                            Some(CommitCondition::Head { expected, .. })
                            | Some(CommitCondition::Tail { expected, .. }) => Some(*expected),
                            Some(CommitCondition::Absent { .. }) => Some(0),
                            Some(CommitCondition::Present { .. }) | None => None,
                        };
                        fork.is_none()
                            && match (skipped, append) {
                                (true, None) => true,
                                (false, Some(append)) => {
                                    start.is_none_or(|expected| append.start == expected)
                                        && append.start.checked_add(records.len() as u64)
                                            == Some(append.tail)
                                }
                                _ => false,
                            }
                    }
                    CommitMutation::Fork {
                        source,
                        at_tail,
                        records,
                        ..
                    } => {
                        if source == path
                            || records.len() > MAX_ITEMS
                            || !matches!(condition, Some(CommitCondition::Absent { .. }))
                        {
                            return Err(StreamError::InvalidArgument);
                        }
                        append.is_none()
                            && match (skipped, fork) {
                                (true, None) => true,
                                (false, Some(fork)) => {
                                    fork.source == *source
                                        && fork.forked_at == *at_tail
                                        && at_tail.checked_add(records.len() as u64)
                                            == Some(fork.tail)
                                        && fork.commit_id == self.commit_id
                                        && fork.revision == self.revision
                                }
                                _ => false,
                            }
                    }
                };
                if !matches {
                    return Err(StreamError::InvalidResponse);
                }
                published += usize::from(!skipped);
            }
        }
        // Mutation paths are unique, so matching counts leave no surplus member.
        if self.appends.len().checked_add(self.forks.len()) != Some(published) {
            return Err(StreamError::InvalidResponse);
        }
        Ok(())
    }

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
            skipped: Vec::new(),
        })
    }

    /// Whether this receipt names the same commit and effects as `envelope`.
    pub fn matches_envelope(&self, envelope: &CommittedEnvelope) -> bool {
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

    /// Admits the receipt's effect count and per-effect ranges.
    pub fn validate(&self) -> Result<(), StreamError> {
        let count = self
            .appends
            .len()
            .checked_add(self.forks.len())
            .ok_or(StreamError::InvalidArgument)?;
        // A commit that published nothing skipped every effect it had.
        if (count == 0 && self.skipped.is_empty())
            || count > MAX_ITEMS
            || self.skipped.len() > MAX_ITEMS
            || !self.skipped.is_sorted_by(|left, right| left < right)
            || !self
                .appends
                .is_sorted_by(|left, right| left.path < right.path)
            || !self
                .forks
                .is_sorted_by(|left, right| left.destination < right.destination)
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
