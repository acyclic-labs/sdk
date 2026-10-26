use super::*;

/// A prepared filesystem head has no Streams identity of its own. Only the
/// authenticated durable anchor can supply the first transaction's Head fence.
#[derive(Clone, Copy)]
pub(in crate::distributed) enum AppendCursor {
    Durable(AuthoritySnapshot),
    Prepared {
        anchor: AuthoritySnapshot,
        head: Head,
    },
}

impl AppendCursor {
    pub(super) fn head(self) -> Head {
        match self {
            Self::Durable(snapshot) => snapshot.head,
            Self::Prepared { head, .. } => head,
        }
    }

    pub(super) fn anchor(self) -> AuthoritySnapshot {
        match self {
            Self::Durable(snapshot)
            | Self::Prepared {
                anchor: snapshot, ..
            } => snapshot,
        }
    }

    pub(super) fn advance(self, head: Head) -> Result<Self, AuthorityStoreError> {
        let before = self.head();
        if head.epoch != before.epoch || before.sequence.checked_next().ok() != Some(head.sequence)
        {
            return Err(AuthorityStoreError::RejectedStatic(
                "prepared cursor does not extend its predecessor",
            ));
        }
        Ok(Self::Prepared {
            anchor: self.anchor(),
            head,
        })
    }

    pub(super) fn record_condition(
        self,
        view: &AppendView<'_>,
        records: &StreamPath,
        work: &mut AuthorityWork<'_>,
    ) -> Result<CommitCondition, AuthorityStoreError> {
        let staged = view.staged(records, work)?;
        match (self, staged) {
            (Self::Durable(snapshot), None) => Ok(CommitCondition::Head {
                path: records.clone(),
                expected: snapshot.head.sequence.get() + 1,
                commit_id: snapshot.record_commit_id,
            }),
            (Self::Prepared { anchor, head }, Some((start, rows))) => {
                let tail = start
                    .checked_add(crate::foundation::usize_to_u64(rows.len()))
                    .ok_or(crate::WorkError::Overflow)?;
                if start != anchor.head.sequence.get() + 1 || tail != head.sequence.get() + 1 {
                    return Err(AuthorityStoreError::RejectedStatic(
                        "prepared cursor disagrees with its staged records",
                    ));
                }
                Ok(CommitCondition::Tail {
                    path: records.clone(),
                    expected: tail,
                })
            }
            _ => Err(AuthorityStoreError::RejectedStatic(
                "prepared cursor has no matching durability prefix",
            )),
        }
    }
}

/// The already prepared prefix of one atomic authority transaction. Reads
/// outside that prefix still use the canonical provider I/O implementation.
#[derive(Default)]
pub(in crate::distributed) struct AppendView<'a> {
    pub(in crate::distributed) cursor: Option<AppendCursor>,
    pub(in crate::distributed) projection: Option<&'a super::super::actor::Projection>,
    pub(in crate::distributed) transaction: Option<&'a CommitRequest>,
}

impl AppendView<'_> {
    pub(super) async fn snapshot<P: crate::distributed::AuthorityProvider + ?Sized>(
        &self,
        store: &StreamAuthorityStore<P>,
        authority: AuthorityId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<AppendCursor, AuthorityStoreError> {
        match self.cursor {
            Some(cursor) => {
                work.check()?;
                Ok(cursor)
            }
            None => store
                .fork_snapshot(authority, work)
                .await
                .map(AppendCursor::Durable),
        }
    }

    pub(in crate::distributed) fn staged<'a>(
        &'a self,
        path: &StreamPath,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Option<(u64, &'a [acyclic_stream::AppendRecord])>, AuthorityStoreError> {
        let Some(transaction) = self.transaction else {
            return Ok(None);
        };
        for mutation in &transaction.mutations {
            visit(work)?;
            if let CommitMutation::Append {
                path: destination,
                records,
            } = mutation
                && destination == path
            {
                for condition in &transaction.conditions {
                    visit(work)?;
                    match condition {
                        CommitCondition::Absent { path: participant } if participant == path => {
                            return Ok(Some((0, records)));
                        }
                        CommitCondition::Tail {
                            path: participant,
                            expected,
                        }
                        | CommitCondition::Head {
                            path: participant,
                            expected,
                            ..
                        } if participant == path => return Ok(Some((*expected, records))),
                        _ => {}
                    }
                }
                return Err(AuthorityStoreError::RejectedStatic(
                    "staged append has no original fence",
                ));
            }
        }
        Ok(None)
    }

    pub(super) async fn tail<P: crate::distributed::AuthorityProvider + ?Sized>(
        &self,
        store: &StreamAuthorityStore<P>,
        path: StreamPath,
        work: &mut AuthorityWork<'_>,
    ) -> Result<u64, AuthorityStoreError> {
        if let Some((start, records)) = self.staged(&path, work)? {
            return start
                .checked_add(crate::foundation::usize_to_u64(records.len()))
                .ok_or_else(|| crate::WorkError::Overflow.into());
        }
        store.tail(path, work).await
    }

    pub(super) async fn value<P: crate::distributed::AuthorityProvider + ?Sized>(
        &self,
        store: &StreamAuthorityStore<P>,
        path: StreamPath,
        sequence: u64,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(acyclic_stream::OwnedBytes, ProviderAllocation), AuthorityStoreError> {
        if let Some((start, records)) = self.staged(&path, work)?
            && let Some(index) = sequence.checked_sub(start)
        {
            let index = usize::try_from(index).map_err(|_| crate::WorkError::Overflow)?;
            let record = records
                .get(index)
                .ok_or(AuthorityStoreError::RejectedStatic(
                    "staged read exceeds the prepared prefix",
                ))?;
            return Ok((record.value.clone(), None));
        }
        let (record, allocation) = store.record(path, sequence, work).await?;
        Ok((record.value, allocation))
    }
}

pub(super) fn visit(work: &mut AuthorityWork<'_>) -> Result<(), AuthorityStoreError> {
    let item = WorkCounters {
        items_examined: 1,
        ..WorkCounters::UNCHARGED
    };
    work.admit(item)?;
    work.charge(item)
}

#[cfg(test)]
mod cursor_tests {
    use super::*;

    #[test]
    fn prepared_heads_require_the_exact_staged_prefix_and_cannot_supply_a_head_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let token = CancellationToken::new();
        let mut work = AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &token);
        let records = StreamPath::new("fs/authorities/cursor/records")?;
        let anchor = AuthoritySnapshot {
            head: Head::genesis(Epoch::GENESIS),
            epoch_tail: 1,
            record_commit_id: acyclic_stream::CommitId::from_bytes([0x6b; 32]),
        };
        let durable = AppendCursor::Durable(anchor);
        let condition = durable.record_condition(&AppendView::default(), &records, &mut work)?;
        assert!(
            matches!(condition, CommitCondition::Head { expected: 1, commit_id, .. }
            if commit_id == anchor.record_commit_id)
        );
        assert!(durable.advance(anchor.head).is_err());
        assert!(
            durable
                .advance(Head {
                    sequence: Sequence::new(2),
                    ..anchor.head
                })
                .is_err()
        );
        for length in 1..=4 {
            let mut cursor = durable;
            for sequence in 1..=length {
                cursor = cursor.advance(Head {
                    sequence: Sequence::new(sequence),
                    ..anchor.head
                })?;
            }
            assert!(
                cursor
                    .record_condition(&AppendView::default(), &records, &mut work)
                    .is_err(),
                "prepared head acquired a fence without a durability prefix"
            );
            for start in 0..=2 {
                for rows in 0..=5 {
                    let transaction = CommitRequest {
                        conditions: vec![CommitCondition::Head {
                            path: records.clone(),
                            expected: start,
                            commit_id: anchor.record_commit_id,
                        }],
                        mutations: vec![CommitMutation::Append {
                            path: records.clone(),
                            records: (0..rows)
                                .map(|_| {
                                    acyclic_stream::OwnedBytes::from_static(b"prepared").into()
                                })
                                .collect(),
                        }],
                        idempotency_key: acyclic_stream::IdempotencyKey::new(Bytes::from_static(
                            b"cursor-proof",
                        ))?,
                    };
                    let view = AppendView {
                        cursor: Some(cursor),
                        transaction: Some(&transaction),
                        projection: None,
                    };
                    assert!(
                        durable
                            .record_condition(&view, &records, &mut work)
                            .is_err(),
                        "durable cursor accepted an unacknowledged prefix"
                    );
                    let result = cursor.record_condition(&view, &records, &mut work);
                    if start == 1 && rows == length {
                        assert!(matches!(result, Ok(CommitCondition::Tail { expected, .. })
                            if expected == length + 1));
                    } else {
                        assert!(result.is_err(), "prepared cursor accepted another prefix");
                    }
                }
            }
        }
        Ok(())
    }
}
