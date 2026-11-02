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
        use crate::request::effect_condition;
        if request.effects.is_empty()
            || request.mutations().count() > MAX_ITEMS
            || request.conditions().count() > MAX_ITEMS
            || request.effects.iter().any(|effect| {
                effect.mutations.is_empty()
                    || effect.conditions.windows(2).any(|pair| {
                        crate::CommitCondition::path(&pair[0])
                            >= crate::CommitCondition::path(&pair[1])
                    })
                    || effect.mutations.windows(2).any(|pair| {
                        crate::CommitMutation::path(&pair[0])
                            >= crate::CommitMutation::path(&pair[1])
                    })
            })
        {
            return Err(StreamError::InvalidArgument);
        }
        if self.skipped.windows(2).any(|pair| pair[0] >= pair[1])
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
        for (index, effect) in request.effects.iter().enumerate() {
            let skipped =
                u32::try_from(index).is_ok_and(|index| self.skipped.binary_search(&index).is_ok());
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
                let condition = effect_condition(effect, path);
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
        // A commit that published nothing skipped every effect it had.
        if (count == 0 && self.skipped.is_empty())
            || count > MAX_ITEMS
            || self.skipped.len() > MAX_ITEMS
            || self.skipped.windows(2).any(|pair| pair[0] >= pair[1])
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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod receipt_membership_tests {
    use super::*;

    #[tokio::test]
    async fn compact_membership_borrows_the_original_request_and_rejects_corrupt_ranges()
    -> Result<(), StreamError> {
        let provider = MemoryStream::default();
        let paths = [
            "b-head",
            "f-tail",
            "h-present",
            "j-unconditioned",
            "z-source",
        ]
        .map(StreamPath::new)
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
        let base = provider
            .commit(CommitRequest {
                deadline_unix_millis: None,
                effects: vec![crate::CommitEffect::hard(
                    Vec::new(),
                    paths
                        .iter()
                        .map(|path| CommitMutation::Append {
                            path: path.clone(),
                            records: vec![Bytes::from_static(b"base").into()],
                        })
                        .collect(),
                )],
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?;
        let CommitOutcome::Committed(base) = base else {
            return Err(StreamError::Unavailable);
        };
        let mut request = CommitRequest {
            deadline_unix_millis: None,
            effects: vec![crate::CommitEffect::hard(
                vec![
                    CommitCondition::Absent {
                        path: StreamPath::new("x-new")?,
                    },
                    CommitCondition::Present {
                        path: paths[2].clone(),
                    },
                    CommitCondition::Tail {
                        path: paths[1].clone(),
                        expected: 1,
                    },
                    CommitCondition::Head {
                        path: paths[0].clone(),
                        expected: 1,
                        commit_id: base.commit_id,
                    },
                ],
                paths[..4]
                    .iter()
                    .map(|path| CommitMutation::Append {
                        path: path.clone(),
                        records: vec![Bytes::from_static(b"next").into()],
                    })
                    .collect(),
            )],
            idempotency_key: IdempotencyKey::unique(),
        };
        request.effects[0].mutations.push(CommitMutation::Append {
            path: StreamPath::new("x-new")?,
            records: vec![Bytes::from_static(b"new").into()],
        });
        for (name, at_tail, records) in [
            ("d-fork", 1, vec![Bytes::from_static(b"forked").into()]),
            ("g-empty-fork", 0, Vec::new()),
        ] {
            let destination = StreamPath::new(name)?;
            request.effects[0].conditions.push(CommitCondition::Absent {
                path: destination.clone(),
            });
            request.effects[0].mutations.push(CommitMutation::Fork {
                source: paths[4].clone(),
                destination,
                at_tail,
                records,
                exclude: Vec::new(),
            });
        }
        request::validate_commit(&mut request)?;
        let CommitOutcome::Committed(receipt) = provider.commit(request.clone()).await? else {
            return Err(StreamError::Unavailable);
        };
        let mut validation = None;
        let allocations = acyclic_test_support::measure(|| {
            validation = Some(receipt.validate_request(&request));
        });
        validation.expect("validation ran")?;
        assert_eq!(allocations.count_total, 0);
        for index in 0..receipt.appends.len() {
            let mut missing = receipt.clone();
            missing.appends.remove(index);
            assert_eq!(
                missing.validate_request(&request),
                Err(StreamError::InvalidResponse)
            );
            let mut wrong_range = receipt.clone();
            wrong_range.appends[index].tail += 1;
            assert_eq!(
                wrong_range.validate_request(&request),
                Err(StreamError::InvalidResponse)
            );
        }
        for index in 0..receipt.forks.len() {
            let mut missing = receipt.clone();
            missing.forks.remove(index);
            assert_eq!(
                missing.validate_request(&request),
                Err(StreamError::InvalidResponse)
            );
            for change in 0..6 {
                let mut corrupt = receipt.clone();
                let fork = &mut corrupt.forks[index];
                match change {
                    0 => fork.source = paths[0].clone(),
                    1 => fork.destination = paths[0].clone(),
                    2 => fork.forked_at += 1,
                    3 => fork.tail += 1,
                    4 => fork.commit_id = base.commit_id,
                    _ => fork.revision = base.revision.clone(),
                }
                assert_eq!(
                    corrupt.validate_request(&request),
                    Err(StreamError::InvalidResponse)
                );
            }
        }
        let mut reordered = receipt.clone();
        reordered.appends.swap(0, 1);
        assert_eq!(
            reordered.validate_request(&request),
            Err(StreamError::InvalidResponse)
        );
        let mut reordered = receipt.clone();
        reordered.forks.swap(0, 1);
        assert_eq!(
            reordered.validate_request(&request),
            Err(StreamError::InvalidResponse)
        );
        let mut surplus = receipt.clone();
        surplus.appends.push(receipt.appends[0].clone());
        assert_eq!(
            surplus.validate_request(&request),
            Err(StreamError::InvalidResponse)
        );
        // A consistent but shifted range cannot override an original anchor.
        for index in [0, 1, 4] {
            let mut shifted = receipt.clone();
            shifted.appends[index].start += 1;
            shifted.appends[index].tail += 1;
            assert_eq!(
                shifted.validate_request(&request),
                Err(StreamError::InvalidResponse)
            );
        }
        // Present/unconditioned starts have no independent anchor in this
        // request. Structural membership must not fabricate such a proof.
        for index in [2, 3] {
            let mut shifted = receipt.clone();
            shifted.appends[index].start += 1;
            shifted.appends[index].tail += 1;
            shifted.validate_request(&request)?;
        }
        let mut overflow = receipt.clone();
        overflow.appends[3].start = u64::MAX;
        overflow.appends[3].tail = 0;
        assert_eq!(
            overflow.validate_request(&request),
            Err(StreamError::InvalidResponse)
        );
        let mut noncanonical = request.clone();
        noncanonical.effects[0].conditions.swap(0, 1);
        assert_eq!(
            receipt.validate_request(&noncanonical),
            Err(StreamError::InvalidArgument)
        );
        noncanonical = request.clone();
        noncanonical.effects[0].mutations.swap(0, 1);
        assert_eq!(
            receipt.validate_request(&noncanonical),
            Err(StreamError::InvalidArgument)
        );
        assert_eq!(
            provider.commit(request.clone()).await?,
            CommitOutcome::Committed(receipt.clone())
        );
        receipt.validate_request(&request)?;
        Ok(())
    }
}
