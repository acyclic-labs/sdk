//! Recover the rejected transaction's observation, rather than probing a newer
//! head. Every condition is evaluated at the same Streams commit point.
use super::*;

struct ConflictCursor {
    head: Head,
    records: acyclic_stream::StreamPath,
    record_tail: u64,
    epoch: Epoch,
}

fn recover_tail(
    known: u64,
    expected: u64,
    actual: Option<u64>,
    recovered: &mut u64,
    seen: &mut bool,
    work: &mut AuthorityWork<'_>,
) -> Result<(), AuthorityStoreError> {
    let actual = actual
        .ok_or_else(|| authority_corrupt("conflict removed an existing authority stream", work))?;
    if *seen || expected != known || actual <= known {
        return Err(authority_corrupt("invalid authority tail conflict", work));
    }
    *seen = true;
    *recovered = actual;
    Ok(())
}

impl<P: crate::distributed::AuthorityProvider + ?Sized> StreamAuthorityStore<P> {
    pub(in crate::distributed) async fn recover_conflict_head(
        &self,
        authority: AuthorityId,
        anchor: AuthoritySnapshot,
        conflicts: &[acyclic_stream::CommitConflict],
        work: &mut AuthorityWork<'_>,
        mut observe: impl FnMut(
            crate::streams_record::BorrowedStreamsDurableRecord<'_>,
            &acyclic_stream::Record,
            &mut AuthorityWork<'_>,
        ) -> Result<(), AuthorityStoreError>,
    ) -> Result<Head, AuthorityStoreError> {
        let records = work.path(&self.placement, authority, "/records", None)?;
        let epochs = work.path(&self.placement, authority, "/epochs", None)?;
        let retirement = work.path(&self.placement, authority, "/retirement", None)?;
        let expected_tail = anchor
            .head
            .sequence
            .get()
            .checked_add(1)
            .ok_or(crate::WorkError::Overflow)?;
        let mut record_tail = expected_tail;
        let mut epoch_tail = anchor.epoch_tail;
        let mut saw_records = false;
        let mut saw_epochs = false;
        for conflict in conflicts {
            super::view::visit(work)?;
            match conflict {
                acyclic_stream::CommitConflict::Head { path, .. } if path == &records => {
                    // Streams emits Head only when the tail still matches.
                    // An append-only authority cannot replace that immutable
                    // record. Never reuse a cursor whose identity was lost.
                    return Err(authority_corrupt(
                        "authority record identity changed without advancing its tail",
                        work,
                    ));
                }
                acyclic_stream::CommitConflict::Tail {
                    path,
                    expected,
                    actual,
                } if path == &records || path == &epochs => {
                    let (known, recovered, seen) = if path == &records {
                        (expected_tail, &mut record_tail, &mut saw_records)
                    } else {
                        (anchor.epoch_tail, &mut epoch_tail, &mut saw_epochs)
                    };
                    recover_tail(known, *expected, *actual, recovered, seen, work)?;
                }
                acyclic_stream::CommitConflict::Exists { path } if path == &retirement => {
                    self.read_at(authority, "/retirement", 0, work, |record, work| {
                        if record.value.as_ref() == AUTHORITY_RETIRED {
                            Err::<(), _>(AuthorityStoreError::Retired)
                        } else {
                            Err(authority_corrupt(
                                "filesystem retirement marker is invalid",
                                work,
                            ))
                        }
                    })
                    .await?;
                }
                _ => {}
            }
        }
        let epoch = if epoch_tail == anchor.epoch_tail {
            anchor.head.epoch
        } else {
            self.read_at(
                authority,
                "/epochs",
                epoch_tail - 1,
                work,
                |record, work| work.decode_epoch(&record.value, EPOCH_DOMAIN),
            )
            .await?
        };
        if epoch < anchor.head.epoch {
            return Err(authority_corrupt(
                "authority conflict regressed its writer epoch",
                work,
            ));
        }
        self.read_conflict_suffix(
            authority,
            ConflictCursor {
                head: anchor.head,
                records,
                record_tail,
                epoch,
            },
            work,
            &mut observe,
        )
        .await
    }

    async fn read_conflict_suffix(
        &self,
        authority: AuthorityId,
        cursor: ConflictCursor,
        work: &mut AuthorityWork<'_>,
        observe: &mut impl FnMut(
            crate::streams_record::BorrowedStreamsDurableRecord<'_>,
            &acyclic_stream::Record,
            &mut AuthorityWork<'_>,
        ) -> Result<(), AuthorityStoreError>,
    ) -> Result<Head, AuthorityStoreError> {
        let ConflictCursor {
            mut head,
            records,
            record_tail,
            epoch,
        } = cursor;
        let mut from = head
            .sequence
            .get()
            .checked_add(1)
            .ok_or(crate::WorkError::Overflow)?;
        // A bounded read cursor starts immediately after the verified base.
        // No Tail/GetHead is needed, and a later publication cannot extend the
        // prefix described by the failed transaction's reply.
        while from < record_tail {
            work.check()?;
            let count = (record_tail - from).min(acyclic_stream::MAX_ITEMS as u64);
            let count = u32::try_from(count).map_err(|_| crate::WorkError::Overflow)?;
            if !work.is_native() {
                work.admit(WorkCounters {
                    authority_records_read: u64::from(count),
                    authority_bytes_read: STREAM_RECORD_LIMIT.saturating_mul(u64::from(count)),
                    backend_read_operations: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                work.charge(WorkCounters {
                    backend_read_operations: 1,
                    ..WorkCounters::UNCHARGED
                })?;
            }
            let mut suffix = self
                .authority_read(
                    acyclic_stream::ReadRequest {
                        key: None,
                        path: records.clone(),
                        from,
                        limit: count,
                        at_least: Default::default(),
                    },
                    work,
                )
                .await?;
            for _ in 0..count {
                work.check()?;
                let record = suffix.next(work).await?.ok_or_else(|| {
                    authority_corrupt("authority conflict tail has a missing suffix", work)
                })?;
                let durable = authority_decode_borrowed(authority, &record.0.value, work)?;
                if record.0.sequence != from
                    || durable.sequence.get() != from
                    || durable.previous_digest != head.digest
                    || durable.epoch < head.epoch
                    || durable.epoch > epoch
                {
                    return Err(authority_corrupt(
                        "authority conflict suffix breaks its verified cursor",
                        work,
                    ));
                }
                observe(durable, &record.0, work)?;
                head = Head {
                    epoch: durable.epoch,
                    sequence: durable.sequence,
                    digest: durable.digest,
                };
                from = from.checked_add(1).ok_or(crate::WorkError::Overflow)?;
            }
        }
        work.check()?;
        Ok(Head { epoch, ..head })
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
